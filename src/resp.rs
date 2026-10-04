//! RESP2, the Redis wire protocol (DESIGN.md D27): just enough for
//! `redis-cli`, `valkey-cli` and `redis-benchmark` to talk to lsmkv.
//!
//! A request is an array of bulk strings, `*2\r\n$3\r\nGET\r\n$1\r\nk\r\n`,
//! or an "inline" line of space-separated words (`PING\r\n`, what you type
//! into telnet). Replies are one of:
//!
//! ```text
//! +OK\r\n              simple string        :42\r\n             integer
//! -ERR message\r\n     error                $5\r\nhello\r\n      bulk string
//! $-1\r\n              null bulk (no key)   *2\r\n...           array
//! ```
//!
//! The parser is incremental: given the bytes received so far, it returns a
//! complete command and how many bytes it used, or "need more". Clients may
//! pipeline (send many commands before reading any reply), so one read can
//! hold several commands, and one command can span several reads.
//!
//! Everything in a request is untrusted: lengths and counts are checked
//! against limits before anything is allocated (DESIGN.md D4 again).

use std::fmt;

/// Largest bulk string accepted (a key or a value). Redis allows 512 MiB.
pub const MAX_BULK: usize = 64 << 20;
/// Most arguments in one command.
pub const MAX_ARGS: usize = 1 << 20;
/// Longest inline command or header line.
pub const MAX_LINE: usize = 64 << 10;

/// A command: its name and arguments, as raw bytes.
pub type Command = Vec<Vec<u8>>;

/// What `parse` returns: a command and the bytes it used, `None` if more
/// bytes are needed, or an error.
pub type Parsed = Result<Option<(Command, usize)>, ProtocolError>;

/// A request the client got wrong. The connection is closed after replying,
/// since there's no reliable way to find where the next command starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError(pub String);

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Protocol error: {}", self.0)
    }
}

fn bad(msg: impl Into<String>) -> ProtocolError {
    ProtocolError(msg.into())
}

/// Parses one command from the front of `buf`: `Ok(Some((args, used)))`,
/// `Ok(None)` if `buf` doesn't hold a whole command yet, or an error. An
/// empty inline line is skipped (`args` is empty).
pub fn parse(buf: &[u8]) -> Parsed {
    match buf.first() {
        None => Ok(None),
        Some(b'*') => parse_array(buf),
        Some(_) => parse_inline(buf),
    }
}

/// The line starting at `buf[from]`, without its `\r\n`, and the offset just
/// past it. `None` if the line isn't complete yet.
fn line(buf: &[u8], from: usize) -> Result<Option<(&[u8], usize)>, ProtocolError> {
    let rest = &buf[from..];
    match rest.iter().position(|&b| b == b'\n') {
        Some(nl) => {
            if nl > MAX_LINE {
                return Err(bad("line too long"));
            }
            let text = rest[..nl].strip_suffix(b"\r").unwrap_or(&rest[..nl]);
            Ok(Some((text, from + nl + 1)))
        }
        None if rest.len() > MAX_LINE => Err(bad("line too long")),
        None => Ok(None),
    }
}

/// A non-negative decimal number from a header line, at most `max`.
fn number(text: &[u8], max: usize, what: &str) -> Result<usize, ProtocolError> {
    let s = std::str::from_utf8(text).map_err(|_| bad(format!("invalid {what}")))?;
    let n: usize = s
        .parse()
        .map_err(|_| bad(format!("invalid {what} '{s}'")))?;
    if n > max {
        return Err(bad(format!("{what} {n} is over the limit of {max}")));
    }
    Ok(n)
}

fn parse_array(buf: &[u8]) -> Parsed {
    let Some((header, mut pos)) = line(buf, 0)? else {
        return Ok(None);
    };
    let count = number(&header[1..], MAX_ARGS, "multibulk length")?;
    // Never trust `count` for an allocation: cap it by what the bytes so far
    // could possibly hold (each argument takes at least "$0\r\n\r\n").
    let mut args = Vec::with_capacity(count.min(buf.len() / 6));
    for _ in 0..count {
        let Some((head, after)) = line(buf, pos)? else {
            return Ok(None);
        };
        if head.first() != Some(&b'$') {
            return Err(bad(format!(
                "expected '$', got '{}'",
                String::from_utf8_lossy(&head[..head.len().min(1)])
            )));
        }
        let len = number(&head[1..], MAX_BULK, "bulk length")?;
        let end = after + len;
        if buf.len() < end + 2 {
            return Ok(None);
        }
        if &buf[end..end + 2] != b"\r\n" {
            return Err(bad("bulk string not followed by CRLF"));
        }
        args.push(buf[after..end].to_vec());
        pos = end + 2;
    }
    Ok(Some((args, pos)))
}

fn parse_inline(buf: &[u8]) -> Parsed {
    let Some((text, used)) = line(buf, 0)? else {
        return Ok(None);
    };
    let args = text
        .split(|b| b.is_ascii_whitespace())
        .filter(|w| !w.is_empty())
        .map(<[u8]>::to_vec)
        .collect();
    Ok(Some((args, used)))
}

/// A reply, encoded with `encode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    /// A null array: what `EXEC` returns when its transaction was aborted.
    NilArray,
    Array(Vec<Reply>),
}

impl Reply {
    pub fn ok() -> Self {
        Reply::Simple("OK".into())
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Reply::Error(format!("ERR {}", msg.into()))
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            // Simple strings and errors can't contain CR or LF.
            Reply::Simple(s) => line_reply(out, b'+', s),
            Reply::Error(s) => line_reply(out, b'-', s),
            Reply::Int(n) => {
                out.push(b':');
                out.extend_from_slice(n.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Reply::Bulk(b) => {
                out.push(b'$');
                out.extend_from_slice(b.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
            }
            Reply::Nil => out.extend_from_slice(b"$-1\r\n"),
            Reply::NilArray => out.extend_from_slice(b"*-1\r\n"),
            Reply::Array(items) => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode(out);
                }
            }
        }
    }
}

fn line_reply(out: &mut Vec<u8>, tag: u8, s: &str) {
    out.push(tag);
    out.extend(
        s.bytes()
            .map(|b| if b == b'\r' || b == b'\n' { b' ' } else { b }),
    );
    out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<Vec<u8>> {
        words.iter().map(|w| w.as_bytes().to_vec()).collect()
    }

    fn encode_request(words: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", words.len()).into_bytes();
        for w in words {
            out.extend_from_slice(format!("${}\r\n", w.len()).as_bytes());
            out.extend_from_slice(w);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    #[test]
    fn parses_arrays_and_inline_commands() {
        let req = encode_request(&[b"SET", b"key", b"a\r\nbinary\0value"]);
        let (got, used) = parse(&req).unwrap().unwrap();
        assert_eq!(
            got,
            vec![
                b"SET".to_vec(),
                b"key".to_vec(),
                b"a\r\nbinary\0value".to_vec()
            ]
        );
        assert_eq!(used, req.len());

        assert_eq!(parse(b"PING\r\n").unwrap(), Some((args(&["PING"]), 6)));
        assert_eq!(
            parse(b"  get   k \n").unwrap(),
            Some((args(&["get", "k"]), 11))
        );
        assert_eq!(parse(b"\r\n").unwrap(), Some((vec![], 2)), "blank line");
        assert_eq!(parse(b"*0\r\n").unwrap(), Some((vec![], 4)));
        assert_eq!(parse(b"").unwrap(), None);
    }

    /// Any prefix of a valid request is "need more", and the full request
    /// parses: what pipelined requests split across reads look like.
    #[test]
    fn every_prefix_needs_more() {
        let one = encode_request(&[b"MSET", b"a", b"1", b"bb", b""]);
        let mut two = one.clone();
        two.extend_from_slice(b"PING\r\n");
        for cut in 0..one.len() {
            assert_eq!(parse(&one[..cut]).unwrap(), None, "cut at {cut}");
        }
        let (first, used) = parse(&two).unwrap().unwrap();
        assert_eq!(first.len(), 5);
        assert_eq!(parse(&two[used..]).unwrap(), Some((args(&["PING"]), 6)));
    }

    #[test]
    fn malformed_and_oversized_requests_are_errors() {
        let cases: [&[u8]; 7] = [
            b"*x\r\n",
            b"*-1\r\n",
            b"*1\r\n+GET\r\n",
            b"*1\r\n$3\r\nGETxx",
            b"*1\r\n$abc\r\n",
            b"*99999999999\r\n",
            b"*1\r\n$999999999999\r\n",
        ];
        for case in cases {
            assert!(parse(case).is_err(), "{:?}", String::from_utf8_lossy(case));
        }
        // A huge but legal count with no data yet: need more, nothing allocated.
        assert_eq!(parse(b"*1000000\r\n").unwrap(), None);
        // An endless line is cut off instead of buffered forever.
        assert!(parse(&vec![b'a'; MAX_LINE + 1]).is_err());
    }

    #[test]
    fn replies_encode_like_redis() {
        let mut out = Vec::new();
        let reply = Reply::Array(vec![
            Reply::ok(),
            Reply::err("bad\r\nthing"),
            Reply::Int(-7),
            Reply::Bulk(b"hi".to_vec()),
            Reply::Nil,
            Reply::NilArray,
        ]);
        reply.encode(&mut out);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "*6\r\n+OK\r\n-ERR bad  thing\r\n:-7\r\n$2\r\nhi\r\n$-1\r\n*-1\r\n"
        );
    }

    mod fuzz {
        use super::*;
        use proptest::prelude::*;

        /// Parses everything in `stream` the way the server does: append
        /// each chunk, then take every complete command off the front.
        fn feed(chunks: &[&[u8]]) -> Result<Vec<Command>, ProtocolError> {
            let mut buf = Vec::new();
            let mut cmds = Vec::new();
            for chunk in chunks {
                buf.extend_from_slice(chunk);
                while let Some((cmd, used)) = parse(&buf)? {
                    buf.drain(..used);
                    cmds.push(cmd);
                }
            }
            Ok(cmds)
        }

        proptest! {
            /// Garbage never panics or hangs: it's a command, an error, or "need more".
            #[test]
            fn random_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
                let _ = parse(&bytes);
                let _ = feed(&[&bytes]);
            }

            /// Pipelined commands, cut into reads at random points, parse back exactly.
            #[test]
            fn split_pipelines_roundtrip(
                cmds in prop::collection::vec(
                    prop::collection::vec(prop::collection::vec(any::<u8>(), 0..40), 1..5),
                    1..8,
                ),
                cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..6),
            ) {
                let mut stream = Vec::new();
                for cmd in &cmds {
                    let args: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
                    stream.extend(encode_request(&args));
                }
                let mut points: Vec<usize> = cuts.iter().map(|i| i.index(stream.len() + 1)).collect();
                points.sort_unstable();
                let mut chunks = Vec::new();
                let mut last = 0;
                for p in points.into_iter().chain([stream.len()]) {
                    chunks.push(&stream[last..p]);
                    last = p;
                }
                prop_assert_eq!(feed(&chunks).unwrap(), cmds);
            }
        }
    }
}
