//! Load test for `lsmkv-server`, in the style of `redis-benchmark`:
//! `clients` connections, each sending SETs (then GETs) in pipelined batches
//! of `pipeline` commands.
//!
//! Usage: `cargo run --release --example server_bench -- [port] [requests]`
//! against a running server (default port 6380, 200,000 requests per row).
//! Start one with `cargo run --release --bin lsmkv-server -- --dir target/bench-server`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Instant;

fn encode(args: &[&[u8]], out: &mut Vec<u8>) {
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

/// Reads one reply and checks it isn't an error (bulk payloads skipped).
fn read_reply(r: &mut BufReader<TcpStream>, line: &mut String) {
    line.clear();
    r.read_line(line).unwrap();
    match line.as_bytes()[0] {
        b'-' => panic!("server error: {line}"),
        b'$' if !line.starts_with("$-1") => {
            let n: usize = line[1..].trim().parse().unwrap();
            let mut buf = vec![0; n + 2];
            r.read_exact(&mut buf).unwrap();
        }
        _ => {}
    }
}

fn run(port: u16, op: &str, clients: usize, pipeline: usize, requests: usize) -> f64 {
    let per_client = requests / clients;
    let value = [b'x'; 100];
    let t = Instant::now();
    std::thread::scope(|s| {
        for c in 0..clients {
            s.spawn(move || {
                let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
                stream.set_nodelay(true).unwrap();
                let mut w = stream.try_clone().unwrap();
                let mut r = BufReader::new(stream);
                let mut out = Vec::new();
                let mut line = String::new();
                let mut done = 0;
                while done < per_client {
                    out.clear();
                    let n = pipeline.min(per_client - done);
                    for i in done..done + n {
                        let key = format!("key:{c}:{i:08}");
                        match op {
                            "SET" => encode(&[b"SET", key.as_bytes(), &value], &mut out),
                            _ => encode(&[b"GET", key.as_bytes()], &mut out),
                        }
                    }
                    w.write_all(&out).unwrap();
                    for _ in 0..n {
                        read_reply(&mut r, &mut line);
                    }
                    done += n;
                }
            });
        }
    });
    (per_client * clients) as f64 / t.elapsed().as_secs_f64()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().map_or(6380, |p| p.parse().expect("port"));
    let requests: usize = args
        .next()
        .map_or(200_000, |n| n.parse().expect("requests"));
    println!("{requests} requests per row, 100-byte values\n");
    println!("| clients | pipeline | SET/s | GET/s |");
    println!("|---:|---:|---:|---:|");
    for (clients, pipeline) in [(1, 1), (8, 1), (50, 1), (8, 16), (50, 16)] {
        let set = run(port, "SET", clients, pipeline, requests);
        let get = run(port, "GET", clients, pipeline, requests);
        println!(
            "| {clients} | {pipeline} | {:.0}k | {:.0}k |",
            set / 1e3,
            get / 1e3
        );
    }
}
