//! The filesystem, behind a trait (DESIGN.md D28), like RocksDB's
//! `FileSystem` / LevelDB's `Env`.
//!
//! Every file operation the engine does goes through `Fs`: `RealFs` in
//! production, `SimFs` in simulation tests. `SimFs` is an in-memory disk that
//! knows the difference between what a program wrote and what is actually
//! durable, so a test can cut the power at any I/O and see exactly what a
//! real machine might have kept.
//!
//! The durability rules `SimFs` enforces (POSIX's, at their weakest):
//! - **File contents** are durable only up to the last successful `sync` of
//!   that file. Writes after it live in the "page cache" and may be lost.
//! - **Directory entries** (a create, a rename, a remove) are durable only
//!   after a `sync_dir` of the directory. Syncing a file does not make its
//!   name durable.
//! - **A power cut** keeps, for each file, its synced bytes plus a random
//!   prefix (often none) of the unsynced bytes after them: writeback may have
//!   flushed some of it, and a write may be torn anywhere. Each file is cut
//!   independently, so writes to different files can survive out of order.
//!   Names roll back to the last directory sync.
//! - **A failed `sync`** returns an error, and the unsynced data is gone
//!   (Linux drops dirty pages it failed to write: "fsyncgate", D7).

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

/// Every filesystem operation the engine uses.
pub trait Fs: Send + Sync + fmt::Debug {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;
    /// Paths of the entries directly in `dir`.
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>>;
    /// The whole file. `NotFound` if it doesn't exist.
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    fn exists(&self, path: &Path) -> bool;
    fn remove(&self, path: &Path) -> io::Result<()>;
    /// Atomically replaces `to` (if it exists) with `from`.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Makes creations, renames and removals in `dir` durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// Opens for appending, creating the file if it's missing.
    fn open_append(&self, path: &Path) -> io::Result<Box<dyn WritableFile>>;
    /// Creates the file, or empties it if it exists.
    fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>>;
    fn open_read(&self, path: &Path) -> io::Result<Box<dyn ReadableFile>>;
}

/// A file open for writing. Writes go to the end.
pub trait WritableFile: Write + Send + fmt::Debug {
    /// Makes everything written so far durable (fdatasync).
    fn sync(&mut self) -> io::Result<()>;
    fn set_len(&mut self, len: u64) -> io::Result<()>;
    /// A handle that can `sync` this file from another thread, without
    /// whatever lock guards the writer (D11's periodic sync).
    fn sync_handle(&self) -> io::Result<Box<dyn SyncHandle>>;
}

pub trait SyncHandle: Send + fmt::Debug {
    fn sync(&self) -> io::Result<()>;
}

/// A file open for positional reads (tables).
pub trait ReadableFile: Send + Sync + fmt::Debug {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn size(&self) -> io::Result<u64>;
}

/// The real filesystem.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealFs;

impl RealFs {
    pub fn shared() -> Arc<dyn Fs> {
        Arc::new(RealFs)
    }
}

impl Fs for RealFs {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        fs::create_dir_all(dir)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        fs::read_dir(dir)?.map(|e| Ok(e?.path())).collect()
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        fs::read(path)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        File::open(dir)?.sync_all()
    }

    fn open_append(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Box::new(RealFile(file)))
    }

    fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Box::new(RealFile(file)))
    }

    fn open_read(&self, path: &Path) -> io::Result<Box<dyn ReadableFile>> {
        Ok(Box::new(RealFile(File::open(path)?)))
    }
}

#[derive(Debug)]
struct RealFile(File);

impl Write for RealFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl WritableFile for RealFile {
    fn sync(&mut self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    fn sync_handle(&self) -> io::Result<Box<dyn SyncHandle>> {
        // fsync acts on the file, not the descriptor, so a duplicate works.
        Ok(Box::new(RealFile(self.0.try_clone()?)))
    }
}

impl SyncHandle for RealFile {
    fn sync(&self) -> io::Result<()> {
        self.0.sync_data()
    }
}

impl ReadableFile for RealFile {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }

    fn size(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
}

/// An in-memory disk with power cuts and fsync failures (see the module
/// docs for its rules). Cheap to clone: clones share the same disk.
///
/// Deterministic: given the same seed and the same sequence of operations,
/// it makes the same choices, so a failing simulation replays exactly.
#[derive(Clone)]
pub struct SimFs {
    state: Arc<Mutex<SimState>>,
}

struct SimFile {
    /// What reads see: the page cache.
    data: Vec<u8>,
    /// What survives a power cut, for sure.
    durable: Vec<u8>,
}

struct SimState {
    rng: u64,
    files: BTreeMap<u64, SimFile>,
    next_id: u64,
    /// Path -> file, as the running program sees it.
    names: BTreeMap<PathBuf, u64>,
    /// Path -> file, as of each directory's last `sync_dir`.
    durable_names: BTreeMap<PathBuf, u64>,
    /// Operations that change the disk, counted, for `crash_at`.
    ops: u64,
    /// When `ops` reaches this, the machine dies: that operation and every
    /// later one (reads too) fails, until `power_cut`.
    crash_at: Option<u64>,
    crashed: bool,
    syncs: u64,
    /// The sync numbered this fails with EIO, and its unsynced data is lost.
    fail_sync_at: Option<u64>,
    /// A running hash of every operation, to check runs are deterministic.
    trace: u64,
}

/// What a test can see about the simulated disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimStats {
    /// Disk-changing operations so far.
    pub ops: u64,
    /// File syncs so far.
    pub syncs: u64,
    pub crashed: bool,
    /// A hash of every operation and its arguments, in order.
    pub trace: u64,
}

impl fmt::Debug for SimFs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimFs({:?})", self.stats())
    }
}

fn dead() -> io::Error {
    io::Error::other("simulated power loss: the machine is down")
}

fn not_found(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("{} not found", path.display()),
    )
}

impl SimState {
    fn next(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }

    fn record(&mut self, op: &str, path: &Path, n: u64) {
        // FNV-1a over the operation, its path and a number.
        for b in op
            .bytes()
            .chain(path.as_os_str().as_encoded_bytes().iter().copied())
        {
            self.trace = (self.trace ^ b as u64).wrapping_mul(0x100_0000_01b3);
        }
        self.trace = (self.trace ^ n).wrapping_mul(0x100_0000_01b3);
    }

    /// Every disk-changing operation goes through here first: it may be the
    /// one the machine dies on.
    fn mutate(&mut self, op: &str, path: &Path, n: u64) -> io::Result<()> {
        if self.crashed {
            return Err(dead());
        }
        self.ops += 1;
        self.record(op, path, n);
        if self.crash_at.is_some_and(|at| self.ops >= at) {
            self.crashed = true;
            return Err(dead());
        }
        Ok(())
    }

    fn alive(&self) -> io::Result<()> {
        if self.crashed {
            Err(dead())
        } else {
            Ok(())
        }
    }

    fn file(&mut self, id: u64) -> &mut SimFile {
        self.files.get_mut(&id).expect("open files stay in the map")
    }

    fn sync_file(&mut self, id: u64) -> io::Result<()> {
        self.mutate("sync", Path::new(""), id)?;
        self.syncs += 1;
        if self.fail_sync_at == Some(self.syncs) {
            // The kernel couldn't write the dirty pages, and dropped them.
            let f = self.file(id);
            f.data = f.durable.clone();
            return Err(io::Error::other("simulated EIO on fsync"));
        }
        let f = self.file(id);
        f.durable = f.data.clone();
        Ok(())
    }
}

impl SimFs {
    pub fn new(seed: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(SimState {
                rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
                files: BTreeMap::new(),
                next_id: 1,
                names: BTreeMap::new(),
                durable_names: BTreeMap::new(),
                ops: 0,
                crash_at: None,
                crashed: false,
                syncs: 0,
                fail_sync_at: None,
                trace: 0xcbf2_9ce4_8422_2325,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, SimState> {
        self.state.lock().expect("sim lock poisoned")
    }

    pub fn stats(&self) -> SimStats {
        let s = self.lock();
        SimStats {
            ops: s.ops,
            syncs: s.syncs,
            crashed: s.crashed,
            trace: s.trace,
        }
    }

    /// The machine dies when `after` more disk-changing operations have
    /// started: the last of them, and everything after it, fails.
    pub fn crash_after(&self, after: u64) {
        let mut s = self.lock();
        s.crash_at = Some(s.ops + after.max(1));
    }

    /// The machine dies now.
    pub fn crash_now(&self) {
        self.lock().crashed = true;
    }

    /// The `n`th file sync from now fails, losing that file's unsynced data.
    pub fn fail_sync_after(&self, n: u64) {
        let mut s = self.lock();
        s.fail_sync_at = Some(s.syncs + n.max(1));
    }

    /// Power comes back after a cut (see the module docs). Call it with
    /// nothing open: whatever was running died with the machine.
    pub fn power_cut(&self) {
        let mut s = self.lock();
        s.names = s.durable_names.clone();
        let ids: Vec<u64> = s.files.keys().copied().collect();
        for id in ids {
            // Half the time nothing unsynced survives; otherwise a random
            // prefix of it (a torn write).
            let coin = s.next();
            let cut = s.next();
            let f = s.file(id);
            let extra = f.data.len().saturating_sub(f.durable.len());
            let appended = f.data.starts_with(&f.durable);
            let keep = if appended && coin.is_multiple_of(2) && extra > 0 {
                (cut % (extra as u64 + 1)) as usize
            } else {
                0
            };
            let mut data = f.durable.clone();
            if keep > 0 {
                data.extend_from_slice(&f.data[f.durable.len()..f.durable.len() + keep]);
            }
            f.durable = data.clone();
            f.data = data;
        }
        // Files no name points to are gone.
        let live: std::collections::BTreeSet<u64> = s.names.values().copied().collect();
        s.files.retain(|id, _| live.contains(id));
        s.crashed = false;
        s.crash_at = None;
        s.fail_sync_at = None;
        s.record("power_cut", Path::new(""), 0);
    }

    /// The process dies but the machine doesn't (a `kill -9`): everything
    /// keeps working again, and the page cache, unsynced data included, is
    /// still there. Call it with nothing open, after `crash_now`.
    pub fn process_restart(&self) {
        let mut s = self.lock();
        s.crashed = false;
        s.crash_at = None;
        s.fail_sync_at = None;
        s.record("process_restart", Path::new(""), 0);
    }

    /// Cancels any armed crash or fsync failure that hasn't fired yet, as
    /// `power_cut` and `process_restart` do. For a clean close, so a fault
    /// armed for one run can't fire in the next one's recovery.
    pub fn disarm(&self) {
        let mut s = self.lock();
        s.crash_at = None;
        s.fail_sync_at = None;
    }

    /// Paths and sizes of every file, as the program sees them.
    pub fn files(&self) -> Vec<(PathBuf, usize)> {
        let s = self.lock();
        s.names
            .iter()
            .map(|(p, id)| (p.clone(), s.files[id].data.len()))
            .collect()
    }
}

impl Fs for SimFs {
    fn create_dir_all(&self, _dir: &Path) -> io::Result<()> {
        // Directories are implicit: a path's parent is its directory.
        self.lock().alive()
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let s = self.lock();
        s.alive()?;
        Ok(s.names
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .cloned()
            .collect())
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let s = self.lock();
        s.alive()?;
        let id = s.names.get(path).ok_or_else(|| not_found(path))?;
        Ok(s.files[id].data.clone())
    }

    fn exists(&self, path: &Path) -> bool {
        self.lock().names.contains_key(path)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        let mut s = self.lock();
        s.mutate("remove", path, 0)?;
        // The data stays while open handles read it (POSIX unlink).
        s.names
            .remove(path)
            .map(drop)
            .ok_or_else(|| not_found(path))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut s = self.lock();
        s.mutate("rename", from, 0)?;
        s.record("rename_to", to, 0);
        let id = s.names.remove(from).ok_or_else(|| not_found(from))?;
        s.names.insert(to.to_path_buf(), id);
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut s = self.lock();
        s.mutate("sync_dir", dir, 0)?;
        let in_dir = |p: &PathBuf| p.parent() == Some(dir);
        s.durable_names.retain(|p, _| !in_dir(p));
        let now: Vec<(PathBuf, u64)> = s
            .names
            .iter()
            .filter(|(p, _)| in_dir(p))
            .map(|(p, id)| (p.clone(), *id))
            .collect();
        s.durable_names.extend(now);
        Ok(())
    }

    fn open_append(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
        let mut s = self.lock();
        let id = match s.names.get(path) {
            Some(&id) => {
                s.alive()?;
                id
            }
            None => {
                s.mutate("create", path, 0)?;
                let id = s.next_id;
                s.next_id += 1;
                s.files.insert(
                    id,
                    SimFile {
                        data: Vec::new(),
                        durable: Vec::new(),
                    },
                );
                s.names.insert(path.to_path_buf(), id);
                id
            }
        };
        Ok(Box::new(SimHandle {
            fs: self.clone(),
            id,
        }))
    }

    fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
        let file = self.open_append(path)?;
        let mut s = self.lock();
        let id = s.names[path];
        s.mutate("truncate", path, 0)?;
        s.file(id).data.clear();
        drop(s);
        Ok(file)
    }

    fn open_read(&self, path: &Path) -> io::Result<Box<dyn ReadableFile>> {
        let s = self.lock();
        s.alive()?;
        let id = *s.names.get(path).ok_or_else(|| not_found(path))?;
        Ok(Box::new(SimHandle {
            fs: self.clone(),
            id,
        }))
    }
}

/// An open file on a `SimFs`: just its id. It keeps working after the name
/// is removed or renamed, like a POSIX descriptor.
#[derive(Debug)]
struct SimHandle {
    fs: SimFs,
    id: u64,
}

impl Write for SimHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut s = self.fs.lock();
        s.mutate("write", Path::new(""), buf.len() as u64)?;
        s.file(self.id).data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl WritableFile for SimHandle {
    fn sync(&mut self) -> io::Result<()> {
        self.fs.lock().sync_file(self.id)
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        let mut s = self.fs.lock();
        s.mutate("set_len", Path::new(""), len)?;
        s.file(self.id).data.resize(len as usize, 0);
        Ok(())
    }

    fn sync_handle(&self) -> io::Result<Box<dyn SyncHandle>> {
        Ok(Box::new(SimHandle {
            fs: self.fs.clone(),
            id: self.id,
        }))
    }
}

impl SyncHandle for SimHandle {
    fn sync(&self) -> io::Result<()> {
        self.fs.lock().sync_file(self.id)
    }
}

impl ReadableFile for SimHandle {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let s = self.fs.lock();
        s.alive()?;
        let data = &s.files[&self.id].data;
        let start = offset as usize;
        let end = start
            .checked_add(buf.len())
            .filter(|&e| e <= data.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "read past end of file"))?;
        buf.copy_from_slice(&data[start..end]);
        Ok(())
    }

    fn size(&self) -> io::Result<u64> {
        let s = self.fs.lock();
        s.alive()?;
        Ok(s.files[&self.id].data.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn unsynced_data_and_names_can_vanish() {
        // Over many seeds: synced data always survives, a name that was never
        // dir-synced never does, and unsynced bytes survive only as a prefix.
        let mut saw_torn = false;
        for seed in 0..50 {
            let fs = SimFs::new(seed);
            let mut a = fs.open_append(&p("d/a")).unwrap();
            fs.sync_dir(&p("d")).unwrap();
            a.write_all(b"synced").unwrap();
            a.sync().unwrap();
            a.write_all(b"-unsynced").unwrap();
            let mut b = fs.open_append(&p("d/b")).unwrap();
            b.write_all(b"data").unwrap();
            b.sync().unwrap(); // contents synced, name never
            drop((a, b));
            fs.power_cut();

            let a = fs.read(&p("d/a")).unwrap();
            assert!(a.starts_with(b"synced"), "{a:?}");
            assert!(b"synced-unsynced".starts_with(&a), "only a prefix: {a:?}");
            saw_torn |= a.len() > 6 && a.len() < 15;
            assert!(!fs.exists(&p("d/b")), "an un-dir-synced name survived");
        }
        assert!(saw_torn, "no seed tore a write");
    }

    #[test]
    fn rename_is_durable_only_after_a_dir_sync() {
        let fs = SimFs::new(1);
        let mut f = fs.create(&p("d/x.tmp")).unwrap();
        f.write_all(b"table").unwrap();
        f.sync().unwrap();
        fs.sync_dir(&p("d")).unwrap();
        fs.rename(&p("d/x.tmp"), &p("d/x")).unwrap();
        drop(f);
        let before = fs.clone();
        before.power_cut();
        assert!(
            fs.exists(&p("d/x.tmp")) && !fs.exists(&p("d/x")),
            "rename rolled back"
        );

        fs.rename(&p("d/x.tmp"), &p("d/x")).unwrap();
        fs.sync_dir(&p("d")).unwrap();
        fs.power_cut();
        assert_eq!(fs.read(&p("d/x")).unwrap(), b"table");
    }

    #[test]
    fn crashes_and_failed_syncs() {
        let fs = SimFs::new(7);
        let mut f = fs.open_append(&p("d/log")).unwrap();
        fs.crash_after(3);
        f.write_all(b"1").unwrap(); // op 1
        f.write_all(b"2").unwrap(); // op 2
        assert!(f.write_all(b"3").is_err(), "op 3: the machine dies");
        assert!(fs.read(&p("d/log")).is_err(), "nothing works after");
        assert!(fs.stats().crashed);
        drop(f);
        fs.power_cut();
        assert!(!fs.exists(&p("d/log")), "never dir-synced");

        let mut f = fs.open_append(&p("d/log")).unwrap();
        fs.sync_dir(&p("d")).unwrap();
        f.write_all(b"kept").unwrap();
        f.sync().unwrap();
        f.write_all(b"lost").unwrap();
        fs.fail_sync_after(1);
        assert!(f.sync().is_err());
        assert_eq!(
            fs.read(&p("d/log")).unwrap(),
            b"kept",
            "dirty pages dropped"
        );
        f.write_all(b"!").unwrap();
        f.sync().unwrap();
        assert_eq!(fs.read(&p("d/log")).unwrap(), b"kept!");
    }

    #[test]
    fn same_seed_same_choices() {
        let run = |seed| {
            let fs = SimFs::new(seed);
            let mut f = fs.open_append(&p("d/f")).unwrap();
            fs.sync_dir(&p("d")).unwrap();
            f.write_all(&[7; 100]).unwrap();
            drop(f);
            fs.power_cut();
            (fs.read(&p("d/f")).unwrap().len(), fs.stats().trace)
        };
        assert_eq!(run(3), run(3));
        let lens: std::collections::BTreeSet<usize> = (0..20).map(|s| run(s).0).collect();
        assert!(lens.len() > 2, "seeds should differ: {lens:?}");
    }
}
