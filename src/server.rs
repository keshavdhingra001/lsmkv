//! A Redis-protocol server over a `Db` (DESIGN.md D27), so `redis-cli`,
//! `valkey-cli` and `redis-benchmark` can talk to lsmkv.
//!
//! One thread per connection, using only `std::net`. The engine is already
//! thread-safe (group commit for writes, lock-free reads), so connections
//! share one `Arc<Db>` and need no coordination of their own.
//!
//! Commands (case-insensitive):
//!
//! | command | what it does here |
//! |---|---|
//! | `PING [msg]`, `ECHO msg` | liveness |
//! | `GET k`, `SET k v`, `DEL k..`, `EXISTS k..` | single-key reads and writes |
//! | `MGET k..` | reads every key at ONE snapshot (Redis gives no such promise) |
//! | `MSET k v ..` | one atomic batch (D25) |
//! | `INCR k`, `INCRBY k n`, `DECR k` | a transaction with retries (D26) |
//! | `MULTI` .. `EXEC` / `DISCARD` | the queued commands run in one transaction |
//! | `WATCH k..`, `UNWATCH` | the next `EXEC` fails (nil) if a watched key changed since `WATCH` |
//! | `SCAN cursor [MATCH p] [COUNT n]`, `KEYS p` | key iteration (a range scan) |
//! | `RANGE start end [LIMIT n]` | lsmkv's own: key/value pairs in `[start, end)` |
//! | `DBSIZE`, `INFO` | key count, engine stats |
//!
//! No auth, so the server binds to localhost by default.

use std::io::{BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::db::{Db, Transaction, WriteBatch};
use crate::error::Error;
use crate::resp::{self, Reply};

/// Connections beyond this are refused, so a flood can't create unbounded threads.
pub const MAX_CONNECTIONS: usize = 1024;

/// Accepts connections forever, one thread each.
pub fn serve(db: Arc<Db>, listener: TcpListener) -> std::io::Result<()> {
    let live = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept: {e}");
                continue;
            }
        };
        if live.fetch_add(1, Ordering::AcqRel) >= MAX_CONNECTIONS {
            live.fetch_sub(1, Ordering::AcqRel);
            let mut out = Vec::new();
            Reply::err("max number of clients reached").encode(&mut out);
            let _ = stream.write_all(&out);
            continue;
        }
        let (db, live) = (Arc::clone(&db), Arc::clone(&live));
        std::thread::spawn(move || {
            let _ = handle(&db, stream);
            live.fetch_sub(1, Ordering::AcqRel);
        });
    }
    Ok(())
}

/// Per-connection state: an open `MULTI` block and `WATCH`ed keys.
struct Session<'a> {
    db: &'a Db,
    /// Commands queued since `MULTI`, or `None` outside one.
    multi: Option<Vec<Vec<Vec<u8>>>>,
    /// Started by `WATCH`: reads at the moment of the `WATCH`, with the
    /// watched keys in its conflict check.
    watch: Option<Transaction<'a>>,
    /// A command queued inside `MULTI` was malformed: `EXEC` must refuse.
    multi_failed: bool,
}

/// Serves one connection until it closes. Reads whatever has arrived,
/// answers every complete command in it (clients may pipeline), and writes
/// the replies back in one go.
fn handle(db: &Db, stream: TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = stream.try_clone()?;
    let mut writer = BufWriter::new(stream);
    let mut session = Session {
        db,
        multi: None,
        watch: None,
        multi_failed: false,
    };
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; 64 << 10];
    let mut out = Vec::new();
    loop {
        let n = reader.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        let mut used = 0;
        loop {
            match resp::parse(&buf[used..]) {
                Ok(Some((args, len))) => {
                    used += len;
                    if args.is_empty() {
                        continue;
                    }
                    if args[0].eq_ignore_ascii_case(b"QUIT") {
                        Reply::ok().encode(&mut out);
                        writer.write_all(&out)?;
                        return writer.flush();
                    }
                    session.run(args).encode(&mut out);
                }
                Ok(None) => break,
                Err(e) => {
                    Reply::Error(format!("ERR {e}")).encode(&mut out);
                    writer.write_all(&out)?;
                    return writer.flush();
                }
            }
        }
        buf.drain(..used);
        writer.write_all(&out)?;
        writer.flush()?;
        out.clear();
    }
}

fn arity(name: &str) -> Reply {
    Reply::err(format!("wrong number of arguments for '{name}' command"))
}

fn engine_error(e: Error) -> Reply {
    Reply::err(e.to_string())
}

fn int_arg(arg: &[u8]) -> Option<i64> {
    std::str::from_utf8(arg).ok()?.parse().ok()
}

impl<'a> Session<'a> {
    fn run(&mut self, args: Vec<Vec<u8>>) -> Reply {
        let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        if self.multi.is_some() {
            return match name.as_str() {
                "EXEC" => self.exec(),
                "DISCARD" => {
                    self.multi = None;
                    self.watch = None;
                    Reply::ok()
                }
                "MULTI" => Reply::err("MULTI calls can not be nested"),
                "WATCH" => Reply::err("WATCH inside MULTI is not allowed"),
                _ if !queueable(&name) => {
                    self.multi_failed = true;
                    Reply::err(format!("'{name}' can't be used inside MULTI"))
                }
                _ => {
                    self.multi.as_mut().expect("in MULTI").push(args);
                    Reply::Simple("QUEUED".into())
                }
            };
        }
        match name.as_str() {
            "MULTI" => {
                self.multi = Some(Vec::new());
                self.multi_failed = false;
                Reply::ok()
            }
            "EXEC" => Reply::err("EXEC without MULTI"),
            "DISCARD" => Reply::err("DISCARD without MULTI"),
            "WATCH" if args.len() >= 2 => {
                let tx = self.watch.get_or_insert_with(|| self.db.transaction());
                for key in &args[1..] {
                    if let Err(e) = tx.get_for_update(key) {
                        return engine_error(e);
                    }
                }
                Reply::ok()
            }
            "WATCH" => arity("watch"),
            "UNWATCH" => {
                self.watch = None;
                Reply::ok()
            }
            _ => {
                // Outside MULTI, each command is its own transaction (or plain read/write).
                let mut tx = None;
                execute(self.db, &mut tx, &name, &args)
            }
        }
    }

    /// Runs the queued commands in one transaction: in the `WATCH`'s if there
    /// is one (so a change to a watched key aborts it), otherwise a new one.
    fn exec(&mut self) -> Reply {
        let queued = self.multi.take().expect("in MULTI");
        let watch = self.watch.take();
        if std::mem::take(&mut self.multi_failed) {
            return Reply::Error(
                "EXECABORT Transaction discarded because of previous errors.".into(),
            );
        }
        let mut tx = Some(watch.unwrap_or_else(|| self.db.transaction()));
        let replies: Vec<Reply> = queued
            .iter()
            .map(|args| {
                let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
                execute(self.db, &mut tx, &name, args)
            })
            .collect();
        match tx.expect("still open").commit() {
            Ok(()) => Reply::Array(replies),
            // Like Redis when a watched key changed: a null reply, nothing applied.
            Err(Error::Conflict(_)) => Reply::NilArray,
            Err(e) => engine_error(e),
        }
    }
}

/// Commands that may be queued inside `MULTI`.
fn queueable(name: &str) -> bool {
    matches!(
        name,
        "GET"
            | "SET"
            | "DEL"
            | "EXISTS"
            | "MGET"
            | "MSET"
            | "INCR"
            | "INCRBY"
            | "DECR"
            | "PING"
            | "ECHO"
    )
}

/// Runs one command. `tx` is `Some` inside `EXEC`: reads and writes then go
/// through that transaction, and commit with it.
fn execute(db: &Db, tx: &mut Option<Transaction<'_>>, name: &str, args: &[Vec<u8>]) -> Reply {
    let a = &args[1..];
    let result: Result<Reply, Error> = (|| {
        Ok(match (name, a) {
            ("PING", []) => Reply::Simple("PONG".into()),
            ("PING", [msg]) | ("ECHO", [msg]) => Reply::Bulk(msg.clone()),
            ("GET", [k]) => match read(db, tx, k)? {
                Some(v) => Reply::Bulk(v),
                None => Reply::Nil,
            },
            ("SET", [k, v]) => {
                write(db, tx, k, Some(v))?;
                Reply::ok()
            }
            ("SET", [_, _, ..]) => Reply::err("SET options (EX, NX, ...) are not supported"),
            ("DEL", ks) | ("EXISTS", ks) if !ks.is_empty() => {
                let mut n = 0;
                for k in ks {
                    if read(db, tx, k)?.is_some() {
                        n += 1;
                    }
                }
                if name == "DEL" {
                    match tx {
                        Some(tx) => ks.iter().for_each(|k| tx.delete(k)),
                        None => {
                            let mut b = WriteBatch::new();
                            ks.iter().for_each(|k| {
                                b.delete(k);
                            });
                            db.write(b)?;
                        }
                    }
                }
                Reply::Int(n)
            }
            ("MGET", ks) if !ks.is_empty() => {
                let values: Vec<Option<Vec<u8>>> = match tx {
                    Some(tx) => ks.iter().map(|k| tx.get(k)).collect::<Result<_, _>>()?,
                    None => {
                        let snap = db.snapshot();
                        ks.iter().map(|k| snap.get(k)).collect::<Result<_, _>>()?
                    }
                };
                Reply::Array(
                    values
                        .into_iter()
                        .map(|v| v.map_or(Reply::Nil, Reply::Bulk))
                        .collect(),
                )
            }
            ("MSET", kvs) if !kvs.is_empty() && kvs.len() % 2 == 0 => {
                match tx {
                    Some(tx) => kvs.chunks(2).for_each(|kv| tx.put(&kv[0], &kv[1])),
                    None => {
                        let mut b = WriteBatch::new();
                        for kv in kvs.chunks(2) {
                            b.put(&kv[0], &kv[1]);
                        }
                        db.write(b)?;
                    }
                }
                Reply::ok()
            }
            ("INCR", [k]) => incr(db, tx, k, 1)?,
            ("DECR", [k]) => incr(db, tx, k, -1)?,
            ("INCRBY", [k, by]) => match int_arg(by) {
                Some(by) => incr(db, tx, k, by)?,
                None => Reply::err("value is not an integer or out of range"),
            },
            ("DBSIZE", []) => Reply::Int(db.iter()?.count() as i64),
            ("KEYS", [pattern]) => {
                let mut keys = Vec::new();
                for item in db.iter()? {
                    let (k, _) = item?;
                    if glob(pattern, &k) {
                        keys.push(Reply::Bulk(k));
                    }
                }
                Reply::Array(keys)
            }
            ("SCAN", [cursor, opts @ ..]) => scan(db, cursor, opts)?,
            ("RANGE", [start, end, opts @ ..]) => {
                let limit = match opts {
                    [] => usize::MAX,
                    [l, n] if l.eq_ignore_ascii_case(b"LIMIT") => match int_arg(n) {
                        Some(n) if n >= 0 => n as usize,
                        _ => return Ok(Reply::err("LIMIT must be a non-negative integer")),
                    },
                    _ => return Ok(Reply::err("syntax error")),
                };
                let mut items = Vec::new();
                for item in db.scan(start.as_slice()..end.as_slice())?.take(limit) {
                    let (k, v) = item?;
                    items.push(Reply::Bulk(k));
                    items.push(Reply::Bulk(v));
                }
                Reply::Array(items)
            }
            ("INFO", _) => Reply::Bulk(info(db).into_bytes()),
            (
                "PING" | "ECHO" | "GET" | "SET" | "DEL" | "EXISTS" | "MGET" | "MSET" | "INCR"
                | "DECR" | "INCRBY" | "DBSIZE" | "KEYS" | "SCAN" | "RANGE",
                _,
            ) => arity(&name.to_ascii_lowercase()),
            _ => Reply::err(format!(
                "unknown command '{}'",
                String::from_utf8_lossy(&args[0])
            )),
        })
    })();
    result.unwrap_or_else(engine_error)
}

/// Inside a transaction, reads see its own writes; outside, the latest data.
fn read(db: &Db, tx: &mut Option<Transaction<'_>>, k: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    match tx {
        Some(tx) => tx.get(k),
        None => db.get(k),
    }
}

fn write(
    db: &Db,
    tx: &mut Option<Transaction<'_>>,
    k: &[u8],
    v: Option<&Vec<u8>>,
) -> Result<(), Error> {
    match (tx, v) {
        (Some(tx), Some(v)) => tx.put(k, v),
        (Some(tx), None) => tx.delete(k),
        (None, Some(v)) => db.put(k, v)?,
        (None, None) => db.delete(k)?,
    }
    Ok(())
}

/// `INCRBY`: read, add, write. Outside `MULTI` it's its own transaction,
/// retried on conflict, so concurrent increments never lose one.
fn incr(db: &Db, tx: &mut Option<Transaction<'_>>, k: &[u8], by: i64) -> Result<Reply, Error> {
    fn step(tx: &mut Transaction<'_>, k: &[u8], by: i64) -> Result<Result<i64, Reply>, Error> {
        // `get_for_update`, so two increments from the same snapshot conflict.
        let current = match tx.get_for_update(k)? {
            None => 0,
            Some(v) => match int_arg(&v) {
                Some(n) => n,
                None => return Ok(Err(Reply::err("value is not an integer or out of range"))),
            },
        };
        let Some(next) = current.checked_add(by) else {
            return Ok(Err(Reply::err("increment or decrement would overflow")));
        };
        tx.put(k, next.to_string().as_bytes());
        Ok(Ok(next))
    }
    if let Some(tx) = tx {
        return Ok(match step(tx, k, by)? {
            Ok(n) => Reply::Int(n),
            Err(reply) => reply,
        });
    }
    loop {
        let mut tx = db.transaction();
        let n = match step(&mut tx, k, by)? {
            Ok(n) => n,
            Err(reply) => return Ok(reply),
        };
        match tx.commit() {
            Ok(()) => return Ok(Reply::Int(n)),
            Err(Error::Conflict(_)) => continue,
            Err(e) => return Err(e),
        }
    }
}

/// `SCAN cursor [MATCH p] [COUNT n]`. The cursor is the number of keys
/// already returned (Redis cursors are opaque, and `redis-cli --scan`
/// treats them as numbers), so each call re-scans from the start: O(cursor).
/// Fine at this scale; a real fix would encode the last key in the cursor.
fn scan(db: &Db, cursor: &[u8], opts: &[Vec<u8>]) -> Result<Reply, Error> {
    let Some(skip) = int_arg(cursor).filter(|&n| n >= 0) else {
        return Ok(Reply::err("invalid cursor"));
    };
    let (mut pattern, mut count): (&[u8], usize) = (b"*", 10);
    let mut rest = opts;
    while let [opt, value, tail @ ..] = rest {
        if opt.eq_ignore_ascii_case(b"MATCH") {
            pattern = value;
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            match int_arg(value) {
                Some(n) if n > 0 => count = n as usize,
                _ => return Ok(Reply::err("value is not an integer or out of range")),
            }
        } else {
            return Ok(Reply::err("syntax error"));
        }
        rest = tail;
    }
    if !rest.is_empty() {
        return Ok(Reply::err("syntax error"));
    }
    let mut keys = Vec::new();
    let mut seen = 0usize;
    let mut more = false;
    for item in db.iter()?.skip(skip as usize) {
        if seen == count {
            more = true;
            break;
        }
        let (k, _) = item?;
        seen += 1;
        if glob(pattern, &k) {
            keys.push(Reply::Bulk(k));
        }
    }
    let next = if more { skip as usize + seen } else { 0 };
    Ok(Reply::Array(vec![
        Reply::Bulk(next.to_string().into_bytes()),
        Reply::Array(keys),
    ]))
}

/// Redis glob patterns: `*` any run, `?` any one byte, `\\x` a literal x.
/// (No `[...]` classes.)
///
/// Iterative, with one backtrack point: on a mismatch, retry from just after
/// the last `*` with it swallowing one more byte. That's O(pattern × input)
/// at worst. The obvious recursive version is exponential on patterns like
/// `*a*a*a*a*b`, so one `KEYS` command could pin a thread (Redis had this bug).
pub fn glob(pattern: &[u8], s: &[u8]) -> bool {
    #[derive(Clone, Copy, PartialEq)]
    enum Tok {
        Byte(u8),
        One,
        Star,
    }
    let mut toks = Vec::with_capacity(pattern.len());
    let mut i = 0;
    while i < pattern.len() {
        toks.push(match pattern[i] {
            b'*' => Tok::Star,
            b'?' => Tok::One,
            b'\\' if i + 1 < pattern.len() => {
                i += 1;
                Tok::Byte(pattern[i])
            }
            b => Tok::Byte(b),
        });
        i += 1;
    }
    let (mut p, mut si) = (0, 0);
    // (token after the last star, input position that star swallowed up to)
    let mut backtrack: Option<(usize, usize)> = None;
    while si < s.len() {
        match toks.get(p) {
            Some(Tok::Star) => {
                p += 1;
                backtrack = Some((p, si));
            }
            Some(Tok::One) => {
                p += 1;
                si += 1;
            }
            Some(Tok::Byte(b)) if *b == s[si] => {
                p += 1;
                si += 1;
            }
            _ => match backtrack {
                Some((bp, bs)) => {
                    p = bp;
                    si = bs + 1;
                    backtrack = Some((bp, bs + 1));
                }
                None => return false,
            },
        }
    }
    toks[p..].iter().all(|t| *t == Tok::Star)
}

fn info(db: &Db) -> String {
    let s = db.stats();
    let mut out = String::from("# lsmkv\r\n");
    let mut field = |k: &str, v: String| out.push_str(&format!("{k}:{v}\r\n"));
    field("last_sequence", s.last_sequence.to_string());
    field("memtable_versions", s.memtable_entries.to_string());
    field("tables", s.tables.to_string());
    field("level_files", format!("{:?}", s.level_files));
    field("writes", s.writes.to_string());
    field("write_groups", s.write_groups.to_string());
    field("wal_syncs", s.wal_syncs.to_string());
    field(
        "write_amplification",
        format!("{:.2}", s.write_amplification()),
    );
    field("block_cache_hits", s.cache_hits.to_string());
    field("block_reads", s.block_reads.to_string());
    field("bloom_negatives", s.filter_negatives.to_string());
    field("snapshots", s.snapshots.to_string());
    out
}

#[cfg(test)]
mod tests {
    use super::glob;

    #[test]
    fn glob_matches_like_redis() {
        let yes = [
            ("*", ""),
            ("*", "abc"),
            ("user:*", "user:1"),
            ("u?er", "user"),
            ("a*c", "abbbc"),
            ("a\\*", "a*"),
            ("", ""),
        ];
        let no = [
            ("user:*", "use"),
            ("u?er", "uer"),
            ("a*c", "ab"),
            ("a\\*", "ab"),
            ("", "x"),
        ];
        for (p, s) in yes {
            assert!(glob(p.as_bytes(), s.as_bytes()), "{p} should match {s}");
        }
        for (p, s) in no {
            assert!(
                !glob(p.as_bytes(), s.as_bytes()),
                "{p} should not match {s}"
            );
        }
        assert!(glob(b"*a*b?", b"xxaxxbz"));
        assert!(!glob(b"*a*b?", b"xxaxxb"));
    }

    /// Exponential for a naive recursive matcher; near-linear here.
    #[test]
    fn glob_has_no_exponential_case() {
        let pattern = b"*a*a*a*a*a*a*a*a*a*a*a*a*b";
        let input = vec![b'a'; 200];
        let t = std::time::Instant::now();
        assert!(!glob(pattern, &input));
        assert!(
            t.elapsed() < std::time::Duration::from_millis(100),
            "{:?}",
            t.elapsed()
        );
    }
}
