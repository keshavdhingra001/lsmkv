//! Tiny REPL for poking at the engine by hand.
//! Usage: cargo run -- [data_dir]

use std::io::{self, BufRead, Write};

fn main() -> lsmkv::Result<()> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "./data".into());
    let mut db = lsmkv::Db::open(&dir)?;
    println!("lsmkv @ {dir}  (put <k> <v> | get <k> | del <k> | flush | stats | quit)");

    let stdin = io::stdin();
    loop {
        print!("> ");
        io::stdout().flush()?;
        let mut line = String::new();
        if stdin.lock().read_line(&mut line)? == 0 {
            break;
        }
        let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
        match parts.as_slice() {
            ["put", k, v] => {
                db.put(k.as_bytes(), v.as_bytes())?;
                println!("OK");
            }
            ["get", k] => match db.get(k.as_bytes())? {
                Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                None => println!("(nil)"),
            },
            ["del", k] => {
                db.delete(k.as_bytes())?;
                println!("OK");
            }
            ["flush"] => {
                db.flush()?;
                println!("OK");
            }
            ["stats"] => {
                let s = db.stats();
                println!(
                    "memtable: {} entries, ~{} bytes | log: {:06}.log",
                    s.memtable_entries, s.memtable_bytes, s.log_number
                );
                for (level, (files, bytes)) in s.level_files.iter().zip(&s.level_bytes).enumerate()
                {
                    if *files > 0 {
                        println!("  L{level}: {files} tables, {bytes} bytes");
                    }
                }
            }
            ["quit"] | ["exit"] => break,
            [""] => {}
            _ => println!("unknown command"),
        }
    }
    Ok(())
}
