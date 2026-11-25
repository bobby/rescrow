//! Cooperative same-user Linux authority: a checkpoint of live state in
//! `live.json` and an append-only `log` of checksummed records. A synchronized
//! append is the commit point. Path manipulation and network filesystems are
//! outside this adapter's contract. The lock inode is never replaced or removed.
use rescrow_application::{
    Loaded, LogStore, Publication, StoreError,
    dto::{Checkpoint, Entry, Record, State as TxState},
};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};

/// Ceiling for `live.json` and for each encoded log record.
pub const MAX_RECORD_BYTES: u64 = 16 * 1024 * 1024;
const LIVE: &str = "live.json";
const LOG: &str = "log";
/// The schema-1 snapshot file. Its presence means the store needs migration.
const LEGACY: &str = "state.json";
const HEADER: usize = 8;
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

/// Publication checkpoints are exposed only with a caller-supplied hook, for fault
/// qualification. The production constructor uses a no-op hook; no environment
/// variable can enable a crash in ordinary CLI builds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Appending a log record, before any byte is written.
    BeforeAppend,
    /// Appending a log record, after half of it is written.
    DuringAppend,
    /// Appending a log record, after it is written and synchronized.
    AppendSynced,
    /// Replacing `live.json`, before the candidate is written.
    BeforeWrite,
    DuringWrite,
    FileSynced,
    Renamed,
    DirectorySync,
    /// After any durable change, before reporting success.
    BeforeResponse,
    /// Synchronizing what a read observed, before returning it.
    ReadSync,
}
type Hook = fn(Stage) -> std::io::Result<()>;
fn no_fault(_: Stage) -> std::io::Result<()> {
    Ok(())
}

/// Adapter-owned envelope: the checkpoint and the byte offset of the first
/// log record it does not include.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveFile {
    log_offset: u64,
    checkpoint: Checkpoint,
}

/// Records decoded from one region of the log.
struct Scan {
    /// Each entry with the offset just past its record.
    entries: Vec<(Entry, u64)>,
    /// End of the last valid record; anything after it is an unacknowledged torn append.
    valid_end: u64,
    file_len: u64,
}

pub struct FileStore {
    directory: PathBuf,
    timeout: Duration,
    hook: Hook,
    max_bytes: u64,
}
impl FileStore {
    /// A missing store may be selected, but only initialize will create it.
    /// Its parent directory must already exist (so parent durability is explicit).
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let directory = match fs::canonicalize(path) {
            Ok(p) => p,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let parent = path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."));
                let name = path
                    .file_name()
                    .ok_or_else(|| StoreError::Io("store must name a directory".into()))?;
                fs::canonicalize(parent).map_err(io)?.join(name)
            }
            Err(e) => return Err(io(e)),
        };
        Ok(Self {
            directory,
            timeout: Duration::from_secs(2),
            hook: no_fault,
            max_bytes: MAX_RECORD_BYTES,
        })
    }
    /// A smaller explicit ceiling for `live.json` and each log record. Cooperating
    /// callers should select the same limit for a store.
    pub fn with_size_limit(mut self, bytes: u64) -> Result<Self, StoreError> {
        if bytes == 0 || bytes > MAX_RECORD_BYTES {
            return Err(StoreError::Io("record limit must be 1..=16 MiB".into()));
        }
        self.max_bytes = bytes;
        Ok(self)
    }
    pub fn with_lock_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    pub fn with_fault_hook(mut self, hook: Hook) -> Self {
        self.hook = hook;
        self
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    fn directory_file(&self) -> Result<File, StoreError> {
        let dir = File::open(&self.directory).map_err(missing)?;
        if !dir.metadata().map_err(io)?.is_dir() {
            return Err(StoreError::Io("store is not a directory".into()));
        }
        #[cfg(unix)]
        if dir.metadata().map_err(io)?.permissions().mode() & 0o077 != 0 {
            return Err(StoreError::Io(
                "store directory must have owner-only permissions (chmod 700)".into(),
            ));
        }
        Ok(dir)
    }
    fn lock(&self, exclusive: bool, initialize: bool) -> Result<(File, bool), StoreError> {
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        let path = self.directory.join("state.lock");
        let (file, created) = if initialize {
            match options.create_new(true).open(&path) {
                Ok(file) => (file, true),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (
                    options.create_new(false).open(&path).map_err(missing)?,
                    false,
                ),
                Err(e) => return Err(io(e)),
            }
        } else {
            (options.open(&path).map_err(missing)?, false)
        };
        check_file(&file)?;
        let start = Instant::now();
        loop {
            let result = if exclusive {
                file.try_lock()
            } else {
                file.try_lock_shared()
            };
            match result {
                Ok(()) => return Ok((file, created)),
                Err(std::fs::TryLockError::WouldBlock) if start.elapsed() < self.timeout => {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(std::fs::TryLockError::WouldBlock) => return Err(StoreError::Contention),
                Err(std::fs::TryLockError::Error(e)) => return Err(io(e)),
            }
        }
    }
    fn read_live(&self) -> Result<(LiveFile, File), StoreError> {
        let mut file = match File::open(self.directory.join(LIVE)) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(if self.directory.join(LEGACY).exists() {
                    StoreError::Unsupported(1)
                } else {
                    StoreError::Missing
                });
            }
            Err(e) => return Err(io(e)),
        };
        check_file(&file)?;
        let bytes = self.read_bounded(&mut file)?;
        let live: LiveFile =
            serde_json::from_slice(&bytes).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        Ok((live, file))
    }
    fn read_bounded(&self, file: &mut File) -> Result<Vec<u8>, StoreError> {
        if file.metadata().map_err(io)?.len() > self.max_bytes {
            return Err(StoreError::TooLarge);
        }
        let mut bytes = Vec::new();
        file.take(self.max_bytes + 1)
            .read_to_end(&mut bytes)
            .map_err(io)?;
        if bytes.len() as u64 > self.max_bytes {
            return Err(StoreError::TooLarge);
        }
        Ok(bytes)
    }
    fn open_log(&self, write: bool) -> Result<File, StoreError> {
        let file = OpenOptions::new()
            .read(true)
            .write(write)
            .open(self.directory.join(LOG))
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => {
                    StoreError::Corrupt("log is missing beside live.json".into())
                }
                _ => io(e),
            })?;
        check_file(&file)?;
        Ok(file)
    }
    /// Decode records from `offset` to the end of the log. An invalid final
    /// record, or an all-zero remainder, is an unacknowledged torn append.
    /// Anything else invalid fails closed.
    fn scan(&self, log: &mut File, offset: u64, sequence: u64) -> Result<Scan, StoreError> {
        let file_len = log.metadata().map_err(io)?.len();
        if offset > file_len {
            return Err(StoreError::Corrupt(
                "checkpoint refers past the end of the log".into(),
            ));
        }
        log.seek(SeekFrom::Start(offset)).map_err(io)?;
        let mut bytes = Vec::new();
        log.read_to_end(&mut bytes).map_err(io)?;
        let mut scan = Scan {
            entries: Vec::new(),
            valid_end: offset,
            file_len,
        };
        let mut at = 0;
        while at < bytes.len() {
            let rest = &bytes[at..];
            let torn = || rest.iter().all(|b| *b == 0);
            let Some(header) = rest.get(..HEADER) else {
                break; // incomplete header: torn
            };
            let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(header[4..].try_into().unwrap());
            if len == 0 || len as u64 > self.max_bytes {
                if torn() {
                    break;
                }
                return Err(StoreError::Corrupt(format!(
                    "invalid log record length at byte {}",
                    offset + at as u64
                )));
            }
            let Some(payload) = rest.get(HEADER..HEADER + len) else {
                break; // record extends past the end of the file: torn
            };
            let last = HEADER + len == rest.len();
            let decoded = (crc32(payload) == crc)
                .then(|| serde_json::from_slice::<Entry>(payload).ok())
                .flatten();
            let Some(entry) = decoded else {
                if last || torn() {
                    break;
                }
                return Err(StoreError::Corrupt(format!(
                    "invalid log record at byte {}",
                    offset + at as u64
                )));
            };
            let expected = sequence + scan.entries.len() as u64;
            if entry.sequence != expected {
                return Err(StoreError::Corrupt(format!(
                    "log record {} found where {expected} was expected",
                    entry.sequence
                )));
            }
            at += HEADER + len;
            scan.valid_end = offset + at as u64;
            scan.entries.push((entry, scan.valid_end));
        }
        Ok(scan)
    }
    /// Complete a previous writer's potentially uncertain append or checkpoint
    /// before confirming even a read or idempotent retry. Shared locks exclude
    /// every writer.
    fn sync_read(&self, files: &[&File]) -> Result<(), StoreError> {
        (self.hook)(Stage::ReadSync).map_err(unknown)?;
        for file in files {
            file.sync_all().map_err(unknown)?;
        }
        Ok(())
    }
    /// Read the checkpoint and the entries after it under a lock the caller holds.
    fn read(&self, directory: &File) -> Result<(LiveFile, File, Scan), StoreError> {
        let (live, live_file) = self.read_live()?;
        let mut log = self.open_log(false)?;
        let scan = self.scan(&mut log, live.log_offset, live.checkpoint.position)?;
        self.sync_read(&[&log, &live_file, directory])?;
        Ok((live, log, scan))
    }
    /// Replace `live.json` atomically: write, synchronize, rename, synchronize the directory.
    fn publish(&self, live: &LiveFile, directory: &File) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec(live).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        if bytes.len() as u64 > self.max_bytes {
            return Err(StoreError::TooLarge);
        }
        let (path, mut file) = loop {
            let seq = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = self
                .directory
                .join(format!(".live-{}-{seq}.tmp", std::process::id()));
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            match options.open(&path) {
                Ok(file) => break (path, file),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(io(e)),
            }
        };
        // Drop removes our candidate on ordinary pre-rename errors. A killed writer
        // may leave an orphan; no reader ever considers candidates authoritative.
        let candidate = Candidate(path);
        (self.hook)(Stage::BeforeWrite).map_err(io)?;
        let mid = bytes.len() / 2;
        file.write_all(&bytes[..mid]).map_err(io)?;
        (self.hook)(Stage::DuringWrite).map_err(io)?;
        file.write_all(&bytes[mid..]).map_err(io)?;
        file.flush().map_err(io)?;
        file.sync_all().map_err(io)?;
        (self.hook)(Stage::FileSynced).map_err(io)?;
        fs::rename(&candidate.0, self.directory.join(LIVE)).map_err(io)?;
        (self.hook)(Stage::Renamed).map_err(unknown)?;
        (self.hook)(Stage::DirectorySync).map_err(unknown)?;
        directory.sync_all().map_err(unknown)?;
        (self.hook)(Stage::BeforeResponse).map_err(unknown)?;
        Ok(())
    }
}
impl LogStore for FileStore {
    fn initialize(&self, empty: &Checkpoint) -> Result<(), StoreError> {
        if empty.position != 0 || !empty.resources.is_empty() || !empty.prepared.is_empty() {
            return Err(StoreError::Corrupt(
                "initial checkpoint must be empty at position zero".into(),
            ));
        }
        let mut builder = DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        match builder.create(&self.directory) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(io(e)),
        }
        let directory = self.directory_file()?;
        let (_lock, created_lock) = self.lock(true, true)?;
        // Sync the parent on every initialization attempt, including after another
        // initializer died creating the directory. The parent must preexist.
        File::open(
            self.directory
                .parent()
                .ok_or_else(|| StoreError::Io("store has no parent".into()))?,
        )
        .map_err(io)?
        .sync_all()
        .map_err(io)?;
        match self.read(&directory) {
            Ok(_) => return Err(StoreError::Exists),
            Err(StoreError::Missing) => {}
            Err(e) => return Err(e),
        }
        // An existing coordination file with no live state could represent lost
        // authority, not an empty budget. Even explicit init must fail closed.
        if !created_lock {
            return Err(StoreError::Missing);
        }
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        options.mode(0o600);
        options
            .open(self.directory.join(LOG))
            .map_err(io)?
            .sync_all()
            .map_err(io)?;
        self.publish(
            &LiveFile {
                log_offset: 0,
                checkpoint: empty.clone(),
            },
            &directory,
        )
    }
    fn load(&self) -> Result<Loaded, StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(false, false)?;
        let (live, _, scan) = self.read(&directory)?;
        Ok(Loaded {
            checkpoint: live.checkpoint,
            tail: scan.entries.into_iter().map(|(entry, _)| entry).collect(),
        })
    }
    fn append(&self, entry: &Entry) -> Result<Publication, StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(true, false)?;
        let (live, _, scan) = self.read(&directory)?;
        if entry.sequence != live.checkpoint.position + scan.entries.len() as u64 {
            return Ok(Publication::Conflict);
        }
        let payload = serde_json::to_vec(entry).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        if payload.len() as u64 > self.max_bytes {
            return Err(StoreError::TooLarge);
        }
        let mut frame = Vec::with_capacity(HEADER + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);
        let mut log = self.open_log(true)?;
        // Remove an earlier writer's unacknowledged torn append before writing after it.
        if scan.file_len > scan.valid_end {
            log.set_len(scan.valid_end).map_err(io)?;
            log.sync_all().map_err(io)?;
        }
        (self.hook)(Stage::BeforeAppend).map_err(io)?;
        log.seek(SeekFrom::Start(scan.valid_end)).map_err(io)?;
        let mid = frame.len() / 2;
        log.write_all(&frame[..mid]).map_err(io)?;
        (self.hook)(Stage::DuringAppend).map_err(io)?;
        log.write_all(&frame[mid..]).map_err(io)?;
        // Once every byte is written, a failed synchronization may still have persisted it.
        log.sync_all().map_err(unknown)?;
        (self.hook)(Stage::AppendSynced).map_err(unknown)?;
        (self.hook)(Stage::BeforeResponse).map_err(unknown)?;
        Ok(Publication::Applied)
    }
    fn checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(true, false)?;
        let (live, _, scan) = self.read(&directory)?;
        let Some(covered) = checkpoint.position.checked_sub(live.checkpoint.position) else {
            return Ok(());
        };
        if covered == 0 {
            return Ok(());
        }
        let (_, log_offset) = *scan
            .entries
            .get(covered as usize - 1)
            .ok_or_else(|| StoreError::Corrupt("checkpoint is past the end of the log".into()))?;
        self.publish(
            &LiveFile {
                log_offset,
                checkpoint: checkpoint.clone(),
            },
            &directory,
        )
    }
    fn find(&self, transaction_id: &str) -> Result<Option<Entry>, StoreError> {
        Ok(self.history()?.into_iter().rev().find(|entry| {
            matches!(&entry.record, Record::Transaction(tx)
                if tx.request.transaction_id == transaction_id
                    && !matches!(tx.state, TxState::Prepared {}))
        }))
    }
    fn history(&self) -> Result<Vec<Entry>, StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(false, false)?;
        let (live, live_file) = self.read_live()?;
        let mut log = self.open_log(false)?;
        let scan = self.scan(&mut log, 0, 0)?;
        if scan.valid_end < live.log_offset {
            return Err(StoreError::Corrupt(
                "checkpoint refers past the end of the log".into(),
            ));
        }
        self.sync_read(&[&log, &live_file, &directory])?;
        Ok(scan.entries.into_iter().map(|(entry, _)| entry).collect())
    }
}
struct Candidate(PathBuf);
impl Drop for Candidate {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
/// CRC-32 (IEEE 802.3), bitwise. Detects torn and corrupted records; it is not
/// a cryptographic integrity check.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (crc & 1).wrapping_neg());
        }
    }
    !crc
}
fn check_file(file: &File) -> Result<(), StoreError> {
    let metadata = file.metadata().map_err(io)?;
    if !metadata.is_file() {
        return Err(StoreError::Corrupt("expected regular file".into()));
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(StoreError::Io(
            "store files must have owner-only permissions (chmod 600)".into(),
        ));
    }
    Ok(())
}
fn io(e: std::io::Error) -> StoreError {
    StoreError::Io(e.to_string())
}
fn unknown(e: std::io::Error) -> StoreError {
    StoreError::Unknown(e.to_string())
}
fn missing(e: std::io::Error) -> StoreError {
    if e.kind() == std::io::ErrorKind::NotFound {
        StoreError::Missing
    } else {
        io(e)
    }
}
