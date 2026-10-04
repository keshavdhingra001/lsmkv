//! A Redis-protocol server for lsmkv (DESIGN.md D27).
//!
//! Usage: `lsmkv-server [--dir DIR] [--port PORT] [--bind ADDR] [--sync always|periodic]`
//! (defaults: `./data`, 6380, 127.0.0.1, periodic). Then: `redis-cli -p 6380`.
//!
//! Stop it with Ctrl-C: nothing is lost, since every acknowledged write is
//! already in the WAL (with `--sync always`, on the disk itself).

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use lsmkv::{Db, Options, SyncMode};

fn main() {
    let mut dir = String::from("./data");
    let mut port: u16 = 6380;
    let mut bind = String::from("127.0.0.1");
    let mut sync = SyncMode::Periodic(Duration::from_millis(100));
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .unwrap_or_else(|| usage(&format!("{flag} needs a value")))
        };
        match flag.as_str() {
            "--dir" => dir = value(),
            "--port" => port = value().parse().unwrap_or_else(|_| usage("bad port")),
            "--bind" => bind = value(),
            "--sync" => {
                sync = match value().as_str() {
                    "always" => SyncMode::Always,
                    "periodic" => SyncMode::Periodic(Duration::from_millis(100)),
                    other => usage(&format!("unknown sync mode {other}")),
                }
            }
            "-h" | "--help" => usage(""),
            other => usage(&format!("unknown flag {other}")),
        }
    }
    let opts = Options {
        sync_mode: sync,
        ..Options::default()
    };
    let db = match Db::open_with(&dir, opts) {
        Ok(db) => Arc::new(db),
        Err(e) => {
            eprintln!("can't open {dir}: {e}");
            std::process::exit(1);
        }
    };
    let listener = match TcpListener::bind((bind.as_str(), port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("can't listen on {bind}:{port}: {e}");
            std::process::exit(1);
        }
    };
    println!("lsmkv-server: {dir} on {bind}:{port} ({sync:?}); try `redis-cli -p {port}`");
    if let Err(e) = lsmkv::server::serve(db, listener) {
        eprintln!("server stopped: {e}");
        std::process::exit(1);
    }
}

fn usage(problem: &str) -> ! {
    if !problem.is_empty() {
        eprintln!("{problem}");
    }
    eprintln!(
        "usage: lsmkv-server [--dir DIR] [--port PORT] [--bind ADDR] [--sync always|periodic]"
    );
    std::process::exit(2);
}
