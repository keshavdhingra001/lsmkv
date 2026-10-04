//! Tiny REPL for poking at the engine by hand.
//! Usage: cargo run -- [data_dir]

use std::io::{self, BufRead, Write};

fn main() -> lsmkv::Result<()> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "./data".into());
    let db = lsmkv::Db::open(&dir)?;
    println!("lsmkv @ {dir}  (put <k> <v> | get <k> | del <k> | flush | compact | stats | quit)");

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
            ["compact"] => {
                db.compact_all()?;
                println!("OK");
            }
            ["stats"] => {
                let s = db.stats();
                println!(
                    "memtable: {} entries, ~{} bytes | log: {:06}.log",
                    s.memtable_entries, s.memtable_bytes, s.log_number
                );
                println!(
                    "  writes: {} in {} groups, {} WAL fsyncs",
                    s.writes, s.write_groups, s.wal_syncs
                );
                println!(
                    "  write amplification: {:.2} ({} user bytes -> {} flushed + {} compacted)",
                    s.write_amplification(),
                    s.user_bytes,
                    s.flush_bytes,
                    s.compaction_bytes
                );
                println!(
                    "  reads: {} blocks from disk, {} cache hits ({} bytes cached) | bloom: {} skipped, {} false positives",
                    s.block_reads,
                    s.cache_hits,
                    s.cache_bytes,
                    s.filter_negatives,
                    s.filter_false_positives
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
