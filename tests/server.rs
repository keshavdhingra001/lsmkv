//! The Redis-protocol server over real TCP sockets (DESIGN.md D27).

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use lsmkv::Db;

/// A parsed reply.
#[derive(Debug, Clone, PartialEq, Eq)]
enum R {
    Simple(String),
    Err(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    NilArray,
    Array(Vec<R>),
}

fn bulk(s: &str) -> R {
    R::Bulk(s.as_bytes().to_vec())
}

fn ok() -> R {
    R::Simple("OK".into())
}

/// A minimal RESP client.
struct Client {
    out: TcpStream,
    input: BufReader<TcpStream>,
}

impl Client {
    fn connect(port: u16) -> Self {
        let out = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let input = BufReader::new(out.try_clone().unwrap());
        Self { out, input }
    }

    fn send(&mut self, args: &[&[u8]]) {
        self.out.write_all(&encode(args)).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> R {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.send(&args);
        self.read()
    }

    fn read(&mut self) -> R {
        let mut line = String::new();
        self.input.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => R::Simple(rest.into()),
            "-" => R::Err(rest.into()),
            ":" => R::Int(rest.parse().unwrap()),
            "$" if rest == "-1" => R::Nil,
            "$" => {
                let mut buf = vec![0; rest.parse::<usize>().unwrap() + 2];
                self.input.read_exact(&mut buf).unwrap();
                buf.truncate(buf.len() - 2);
                R::Bulk(buf)
            }
            "*" if rest == "-1" => R::NilArray,
            "*" => R::Array(
                (0..rest.parse::<usize>().unwrap())
                    .map(|_| self.read())
                    .collect(),
            ),
            _ => panic!("bad reply line {line:?}"),
        }
    }
}

fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Starts a server on a free port over a fresh database.
fn start() -> (u16, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || lsmkv::server::serve(db, listener));
    (port, dir)
}

fn is_err(r: &R, starts: &str) -> bool {
    matches!(r, R::Err(e) if e.starts_with(starts))
}

#[test]
fn basic_commands() {
    let (port, _dir) = start();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["PING"]), R::Simple("PONG".into()));
    assert_eq!(
        c.cmd(&["ping", "hi"]),
        bulk("hi"),
        "commands are case-insensitive"
    );
    assert_eq!(c.cmd(&["GET", "k"]), R::Nil);
    assert_eq!(c.cmd(&["SET", "k", "v"]), ok());
    assert_eq!(c.cmd(&["GET", "k"]), bulk("v"));
    assert_eq!(c.cmd(&["MSET", "a", "1", "b", "2"]), ok());
    assert_eq!(
        c.cmd(&["MGET", "a", "nope", "b"]),
        R::Array(vec![bulk("1"), R::Nil, bulk("2")])
    );
    assert_eq!(c.cmd(&["EXISTS", "a", "nope", "k"]), R::Int(2));
    assert_eq!(c.cmd(&["DEL", "a", "nope"]), R::Int(1));
    assert_eq!(c.cmd(&["GET", "a"]), R::Nil);
    assert_eq!(c.cmd(&["INCR", "n"]), R::Int(1));
    assert_eq!(c.cmd(&["INCRBY", "n", "41"]), R::Int(42));
    assert_eq!(c.cmd(&["DECR", "n"]), R::Int(41));
    assert!(is_err(
        &c.cmd(&["INCR", "k"]),
        "ERR value is not an integer"
    ));
    assert!(is_err(&c.cmd(&["GET"]), "ERR wrong number of arguments"));
    assert!(is_err(&c.cmd(&["MSET", "a"]), "ERR wrong number"));
    assert!(is_err(
        &c.cmd(&["SET", "k", "v", "EX", "10"]),
        "ERR SET options"
    ));
    assert!(is_err(&c.cmd(&["FLY"]), "ERR unknown command"));
    assert_eq!(c.cmd(&["DBSIZE"]), R::Int(3));
    match c.cmd(&["INFO"]) {
        R::Bulk(b) => assert!(String::from_utf8(b).unwrap().contains("last_sequence:")),
        other => panic!("{other:?}"),
    }
    // Values are binary-safe.
    c.send(&[b"SET", b"bin", b"\r\n\0\xff"]);
    assert_eq!(c.read(), ok());
    assert_eq!(c.cmd(&["GET", "bin"]), R::Bulk(b"\r\n\0\xff".to_vec()));
    assert_eq!(c.cmd(&["QUIT"]), ok());
}

#[test]
fn inline_commands_and_pipelining() {
    let (port, _dir) = start();
    let mut c = Client::connect(port);
    c.out
        .write_all(b"PING\r\nSET x  1\r\n\r\nGET x\r\n")
        .unwrap();
    assert_eq!(c.read(), R::Simple("PONG".into()));
    assert_eq!(c.read(), ok());
    assert_eq!(c.read(), bulk("1"));

    // 5,000 commands (about 400 KB) in one write, replies read afterwards,
    // in order. Far bigger than one server read (64 KiB), so commands are
    // cut across reads, and the cut-off bytes must be kept for the next one.
    let value = |i: usize| format!("{i:0>100}");
    let mut burst = Vec::new();
    for i in 0..2500 {
        burst.extend(encode(&[
            b"SET",
            format!("p{i}").as_bytes(),
            value(i).as_bytes(),
        ]));
    }
    for i in 0..2500 {
        burst.extend(encode(&[b"GET", format!("p{i}").as_bytes()]));
    }
    assert!(burst.len() > 256 << 10);
    // Written from another thread: the replies have to be read meanwhile,
    // or both sides fill their socket buffers and stall.
    let mut out = c.out.try_clone().unwrap();
    let writer = std::thread::spawn(move || out.write_all(&burst).unwrap());
    for _ in 0..2500 {
        assert_eq!(c.read(), ok());
    }
    for i in 0..2500 {
        assert_eq!(c.read(), bulk(&value(i)));
    }
    writer.join().unwrap();
}

#[test]
fn multi_exec_runs_as_one_transaction() {
    let (port, _dir) = start();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["MULTI"]), ok());
    assert_eq!(c.cmd(&["SET", "a", "1"]), R::Simple("QUEUED".into()));
    assert_eq!(c.cmd(&["INCR", "a"]), R::Simple("QUEUED".into()));
    assert_eq!(c.cmd(&["GET", "a"]), R::Simple("QUEUED".into()));
    assert_eq!(
        c.cmd(&["EXEC"]),
        R::Array(vec![ok(), R::Int(2), bulk("2")]),
        "reads see earlier queued writes"
    );
    assert_eq!(c.cmd(&["GET", "a"]), bulk("2"));

    assert_eq!(c.cmd(&["MULTI"]), ok());
    c.cmd(&["SET", "a", "discarded"]);
    assert_eq!(c.cmd(&["DISCARD"]), ok());
    assert_eq!(c.cmd(&["GET", "a"]), bulk("2"));

    assert!(is_err(&c.cmd(&["EXEC"]), "ERR EXEC without MULTI"));
    assert_eq!(c.cmd(&["MULTI"]), ok());
    assert!(is_err(
        &c.cmd(&["MULTI"]),
        "ERR MULTI calls can not be nested"
    ));
    assert!(is_err(&c.cmd(&["SCAN", "0"]), "ERR"));
    c.cmd(&["SET", "a", "never"]);
    assert!(is_err(&c.cmd(&["EXEC"]), "EXECABORT"));
    assert_eq!(c.cmd(&["GET", "a"]), bulk("2"));
}

#[test]
fn watch_aborts_exec_when_a_watched_key_changes() {
    let (port, _dir) = start();
    let mut a = Client::connect(port);
    let mut b = Client::connect(port);
    a.cmd(&["SET", "balance", "10"]);

    assert_eq!(a.cmd(&["WATCH", "balance"]), ok());
    assert_eq!(a.cmd(&["GET", "balance"]), bulk("10"));
    assert_eq!(
        b.cmd(&["SET", "balance", "0"]),
        ok(),
        "another client changes it"
    );
    a.cmd(&["MULTI"]);
    a.cmd(&["SET", "balance", "9"]);
    assert_eq!(a.cmd(&["EXEC"]), R::NilArray, "aborted: nothing applied");
    assert_eq!(a.cmd(&["GET", "balance"]), bulk("0"));

    // Unchanged: EXEC goes through. WATCH is cleared by EXEC.
    a.cmd(&["WATCH", "balance"]);
    a.cmd(&["MULTI"]);
    a.cmd(&["SET", "balance", "5"]);
    assert_eq!(a.cmd(&["EXEC"]), R::Array(vec![ok()]));
    b.cmd(&["SET", "balance", "7"]);
    a.cmd(&["MULTI"]);
    a.cmd(&["SET", "other", "x"]);
    assert_eq!(a.cmd(&["EXEC"]), R::Array(vec![ok()]), "no WATCH left over");
}

#[test]
fn concurrent_increments_are_never_lost() {
    let (port, _dir) = start();
    std::thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(move || {
                let mut c = Client::connect(port);
                for _ in 0..200 {
                    assert!(matches!(c.cmd(&["INCR", "counter"]), R::Int(_)));
                }
            });
        }
    });
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["GET", "counter"]), bulk("1600"));
}

#[test]
fn scan_keys_and_range() {
    let (port, _dir) = start();
    let mut c = Client::connect(port);
    for i in 0..57 {
        c.cmd(&["SET", &format!("user:{i:03}"), &i.to_string()]);
    }
    c.cmd(&["SET", "other", "x"]);

    // SCAN with a small COUNT returns every key exactly once.
    let mut seen = BTreeSet::new();
    let mut cursor = "0".to_string();
    loop {
        let R::Array(reply) = c.cmd(&["SCAN", &cursor, "MATCH", "user:*", "COUNT", "10"]) else {
            panic!()
        };
        let [R::Bulk(next), R::Array(keys)] = reply.as_slice() else {
            panic!("{reply:?}")
        };
        for k in keys {
            let R::Bulk(k) = k else { panic!() };
            assert!(seen.insert(k.clone()), "a key came back twice");
        }
        cursor = String::from_utf8(next.clone()).unwrap();
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(seen.len(), 57);

    let R::Array(keys) = c.cmd(&["KEYS", "user:00?"]) else {
        panic!()
    };
    assert_eq!(keys.len(), 10);
    assert_eq!(
        c.cmd(&["RANGE", "user:010", "user:013"]),
        R::Array(vec![
            bulk("user:010"),
            bulk("10"),
            bulk("user:011"),
            bulk("11"),
            bulk("user:012"),
            bulk("12")
        ])
    );
    assert_eq!(
        c.cmd(&["RANGE", "a", "z", "LIMIT", "1"]),
        R::Array(vec![bulk("other"), bulk("x")])
    );
}

#[test]
fn a_protocol_error_closes_the_connection() {
    let (port, _dir) = start();
    let mut c = Client::connect(port);
    c.out.write_all(b"*1\r\n+GET\r\n").unwrap();
    assert!(is_err(&c.read(), "ERR Protocol error"));
    let mut rest = Vec::new();
    c.input.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty(), "closed after the error");
    // The server keeps serving everyone else.
    assert_eq!(
        Client::connect(port).cmd(&["PING"]),
        R::Simple("PONG".into())
    );
}

/// The real `redis-cli`, if it's installed (CI installs it): runs `script`
/// over one connection and returns its output lines.
fn redis_cli(port: u16, script: &str) -> Option<Vec<String>> {
    let mut child = std::process::Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .ok()?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    Some(
        String::from_utf8(out.stdout)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect(),
    )
}

#[test]
fn the_real_redis_cli_works() {
    let (port, _dir) = start();
    let script = "PING\nSET k hello\nGET k\nMGET k nope\nINCRBY n 41\nINCR n\n\
                  WATCH n\nMULTI\nINCRBY n -2\nGET n\nEXEC\nDEL k\nGET k\n";
    let Some(lines) = redis_cli(port, script) else {
        eprintln!("redis-cli not installed: skipped");
        return;
    };
    let expected = [
        "PONG", "OK", "hello", "hello", "", "41", "42", "OK", "OK", "QUEUED", "QUEUED", "40", "40",
        "1", "",
    ];
    assert_eq!(lines, expected);

    // A write to a watched key between WATCH and EXEC aborts the transaction:
    // EXEC replies with a null array (an empty line in redis-cli).
    let mut other = Client::connect(port);
    let mut tx = Client::connect(port);
    assert_eq!(tx.cmd(&["WATCH", "n"]), ok());
    assert_eq!(other.cmd(&["SET", "n", "7"]), ok());
    let lines = redis_cli(port, "GET n\n").unwrap();
    assert_eq!(lines, ["7"]);
    assert_eq!(tx.cmd(&["MULTI"]), ok());
    assert_eq!(tx.cmd(&["INCR", "n"]), R::Simple("QUEUED".into()));
    assert_eq!(tx.cmd(&["EXEC"]), R::NilArray);
}
