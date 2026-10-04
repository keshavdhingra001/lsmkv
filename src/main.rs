//! Tiny REPL for poking at the engine by hand.
//! Usage: cargo run -- [data_dir]

use std::io::{self, BufRead, Write};

fn main() -> lsmkv::Result<()> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "./data".into());
    let db = lsmkv::Db::open(&dir)?;
    println!(
        "lsmkv @ {dir}  (put <k> <v> | get <k> | del <k> | scan [<from> [<to>]] | snap \
         | sget <k> | sscan | unsnap | flush | compact | stats | quit)"
    );
    // At most one snapshot, for trying point-in-time reads by hand.
    let mut snap: Option<lsmkv::Snapshot> = None;

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
            ["snap"] => {
                let s = db.snapshot();
                println!("snapshot at seq {}", s.sequence());
                snap = Some(s);
            }
            ["sget", k] => match &snap {
                Some(s) => match s.get(k.as_bytes())? {
                    Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                    None => println!("(nil)"),
                },
                None => println!("no snapshot (use snap)"),
            },
            ["scan"] => print_scan(db.iter()?)?,
            ["scan", from] => print_scan(db.scan(*from..)?)?,
            ["scan", from, to] => print_scan(db.scan(*from..*to)?)?,
            ["sscan"] => match &snap {
                Some(s) => print_scan(s.iter()?)?,
                None => println!("no snapshot (use snap)"),
            },
            ["unsnap"] => {
                snap = None;
                println!("OK");
            }
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
                    "memtable: {} versions, ~{} bytes | log: {:06}.log | last seq: {}",
                    s.memtable_entries, s.memtable_bytes, s.log_number, s.last_sequence
                );
                println!(
                    "  writes: {} in {} groups, {} WAL fsyncs | flushing: {} versions",
                    s.writes, s.write_groups, s.wal_syncs, s.immutable_entries
                );
                if let Some(oldest) = s.oldest_snapshot {
                    println!("  snapshots: {} live, oldest at seq {oldest}", s.snapshots);
                }
                println!(
                    "  backpressure: {} slowdowns, {} stalls ({} ms waiting)",
                    s.write_slowdowns,
                    s.write_stalls,
                    s.stall_micros / 1000
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

/// Prints up to 50 pairs of a scan, then how many there were in all.
fn print_scan(scan: lsmkv::DbIter) -> lsmkv::Result<()> {
    let mut n = 0;
    for item in scan {
        let (k, v) = item?;
        if n < 50 {
            println!(
                "{} = {}",
                String::from_utf8_lossy(&k),
                String::from_utf8_lossy(&v)
            );
        }
        n += 1;
    }
    println!("({n} keys)");
    Ok(())
}
