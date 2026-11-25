//! Cooperative same-user Linux authority: a checkpoint of live state in
//! `live.json` and an append-only log of checksummed records, split into
//! segments. A synchronized append is the commit point. A full segment is
//! sealed by writing an immutable index of its finished transactions, so
//! lookups of old decisions read indexes rather than the whole log. Path
//! manipulation and network filesystems are outside this adapter's contract.
//! The lock inode is never replaced or removed.
use rescrow_application::{
    Loaded, LogStore, Publication, StoreError,
    dto::{Checkpoint, Entry, Record, Snapshot, State as TxState},
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
/// Default size after which the active segment is sealed and a new one started.
pub const SEGMENT_BYTES: u64 = 4 * 1024 * 1024;
const LIVE: &str = "live.json";
const LOG: &str = "log";
/// The schema-1 snapshot file. Its presence means the store needs migration.
const LEGACY: &str = "state.json";
/// A migrated schema-1 snapshot, kept for inspection and never read again.
const RETIRED: &str = "state.json.migrated";
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
    /// Sealing a full segment, after its index is written but before the
    /// next segment exists.
    Sealed,
    /// Synchronizing what a read observed, before returning it.
    ReadSync,
}
type Hook = fn(Stage) -> std::io::Result<()>;
fn no_fault(_: Stage) -> std::io::Result<()> {
    Ok(())
}

/// Adapter-owned envelope: the checkpoint and the position in the log of the
/// first record it does not include.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveFile {
    /// First sequence number of the segment holding that position.
    segment: u64,
    /// Byte offset within that segment.
    offset: u64,
    checkpoint: Checkpoint,
}

/// Immutable index of a sealed segment's finished transactions, sorted by ID.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Index {
    segment: u64,
    /// The segment's length when sealed; it never changes afterwards.
    length: u64,
    entries: Vec<IndexEntry>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexEntry {
    transaction_id: String,
    sequence: u64,
    offset: u64,
}

/// Records decoded from one segment.
struct Scan {
    /// Each entry with its start and end offsets.
    entries: Vec<(Entry, u64, u64)>,
    /// End of the last valid record; anything after it is an unacknowledged torn append.
    valid_end: u64,
    file_len: u64,
}
/// Records after a checkpoint, across segments, plus the state of the active segment.
struct Tail {
    /// Each entry with its segment and end offset.
    entries: Vec<(Entry, u64, u64)>,
    active: u64,
    sealed: bool,
    valid_end: u64,
    file_len: u64,
    files: Vec<File>,
}

pub struct FileStore {
    directory: PathBuf,
    timeout: Duration,
    hook: Hook,
    max_bytes: u64,
    segment_bytes: u64,
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
            segment_bytes: SEGMENT_BYTES,
        })
    }
    /// A smaller explicit ceiling for `live.json`, each log record and each
    /// index. Cooperating callers should select the same limit for a store.
    pub fn with_size_limit(mut self, bytes: u64) -> Result<Self, StoreError> {
        if bytes == 0 || bytes > MAX_RECORD_BYTES {
            return Err(StoreError::Io("record limit must be 1..=16 MiB".into()));
        }
        self.max_bytes = bytes;
        Ok(self)
    }
    /// Seal the active segment once it reaches this size. Mainly for tests;
    /// a store may be reopened with a different value.
    pub fn with_segment_size(mut self, bytes: u64) -> Self {
        self.segment_bytes = bytes.max(1);
        self
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
        open_directory(&self.directory)
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
    fn log(&self) -> PathBuf {
        self.directory.join(LOG)
    }
    fn segment_path(&self, first: u64) -> PathBuf {
        self.log().join(format!("{first:020}.seg"))
    }
    fn index_path(&self, first: u64) -> PathBuf {
        self.log().join(format!("{first:020}.idx"))
    }
    /// First sequence numbers of every segment, in order.
    fn segments(&self) -> Result<Vec<u64>, StoreError> {
        let log = self.log();
        open_directory(&log).map_err(|e| match e {
            StoreError::Missing => StoreError::Corrupt("log is missing beside live.json".into()),
            other => other,
        })?;
        let mut segments = Vec::new();
        for item in fs::read_dir(&log).map_err(io)? {
            let name = item.map_err(io)?.file_name();
            let name = name.to_string_lossy();
            if let Some(first) = name.strip_suffix(".seg") {
                segments.push(
                    first
                        .parse()
                        .map_err(|_| StoreError::Corrupt(format!("unexpected log file {name}")))?,
                );
            }
        }
        segments.sort_unstable();
        if segments.is_empty() {
            return Err(StoreError::Corrupt("log has no segments".into()));
        }
        Ok(segments)
    }
    fn open_segment(&self, first: u64, write: bool) -> Result<File, StoreError> {
        let file = OpenOptions::new()
            .read(true)
            .write(write)
            .open(self.segment_path(first))
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => {
                    StoreError::Corrupt(format!("log segment {first} is missing"))
                }
                _ => io(e),
            })?;
        check_file(&file)?;
        Ok(file)
    }
    /// The sealed segment's index, or `None` while the segment is active.
    fn read_index(&self, first: u64) -> Result<Option<Index>, StoreError> {
        let mut file = match File::open(self.index_path(first)) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io(e)),
        };
        check_file(&file)?;
        let bytes = self.read_bounded(&mut file)?;
        let index: Index = decode_frame(&bytes, self.max_bytes)
            .ok_or_else(|| StoreError::Corrupt(format!("invalid index for segment {first}")))?;
        let sorted = index
            .entries
            .windows(2)
            .all(|w| w[0].transaction_id < w[1].transaction_id);
        if index.segment != first || !sorted {
            return Err(StoreError::Corrupt(format!(
                "invalid index for segment {first}"
            )));
        }
        Ok(Some(index))
    }
    /// Decode records from `offset` to the end of a segment. In the active
    /// segment, an invalid final record or an all-zero remainder is an
    /// unacknowledged torn append. Anything else invalid fails closed.
    fn scan(
        &self,
        segment: &mut File,
        offset: u64,
        sequence: u64,
        active: bool,
    ) -> Result<Scan, StoreError> {
        let file_len = segment.metadata().map_err(io)?.len();
        if offset > file_len {
            return Err(StoreError::Corrupt(
                "checkpoint refers past the end of the log".into(),
            ));
        }
        segment.seek(SeekFrom::Start(offset)).map_err(io)?;
        let mut bytes = Vec::new();
        segment.read_to_end(&mut bytes).map_err(io)?;
        let mut scan = Scan {
            entries: Vec::new(),
            valid_end: offset,
            file_len,
        };
        let mut at = 0;
        while at < bytes.len() {
            let rest = &bytes[at..];
            let start = offset + at as u64;
            let len = rest
                .get(..4)
                .map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize);
            let complete = len.is_some_and(|len| rest.len() >= HEADER + len);
            let decoded = len
                .filter(|len| complete && *len > 0 && *len as u64 <= self.max_bytes)
                .and_then(|len| decode_frame::<Entry>(&rest[..HEADER + len], self.max_bytes));
            let Some(entry) = decoded else {
                // Only the final record of the active segment can be a torn append,
                // and only if its length is one a writer could have produced.
                let plausible = len.is_none_or(|len| len > 0 && len as u64 <= self.max_bytes);
                let last =
                    plausible && (!complete || len.is_some_and(|len| HEADER + len == rest.len()));
                if active && (last || rest.iter().all(|b| *b == 0)) {
                    break;
                }
                return Err(StoreError::Corrupt(format!(
                    "invalid log record at byte {start}"
                )));
            };
            let expected = sequence + scan.entries.len() as u64;
            if entry.sequence != expected {
                return Err(StoreError::Corrupt(format!(
                    "log record {} found where {expected} was expected",
                    entry.sequence
                )));
            }
            at += HEADER + len.unwrap_or_default();
            scan.valid_end = offset + at as u64;
            scan.entries.push((entry, start, scan.valid_end));
        }
        Ok(scan)
    }
    /// Every record from a position to the end of the log, under a lock the caller holds.
    fn tail(&self, segment: u64, offset: u64, sequence: u64) -> Result<Tail, StoreError> {
        let segments = self.segments()?;
        let start = segments
            .iter()
            .position(|first| *first == segment)
            .ok_or_else(|| StoreError::Corrupt(format!("log segment {segment} is missing")))?;
        let mut tail = Tail {
            entries: Vec::new(),
            active: segment,
            sealed: false,
            valid_end: 0,
            file_len: 0,
            files: Vec::new(),
        };
        let mut next = sequence;
        for (position, first) in segments.iter().enumerate().skip(start) {
            let active = position + 1 == segments.len();
            let index = self.read_index(*first)?;
            if !active && index.is_none() {
                return Err(StoreError::Corrupt(format!(
                    "segment {first} is followed by another but was never sealed"
                )));
            }
            let from = if position == start { offset } else { 0 };
            if position != start && *first != next {
                return Err(StoreError::Corrupt(format!(
                    "segment {first} found where {next} was expected"
                )));
            }
            let mut file = self.open_segment(*first, false)?;
            let scan = self.scan(&mut file, from, next, active && index.is_none())?;
            if let Some(index) = &index
                && index.length != scan.file_len
            {
                return Err(StoreError::Corrupt(format!(
                    "sealed segment {first} changed length"
                )));
            }
            next += scan.entries.len() as u64;
            tail.entries.extend(
                scan.entries
                    .into_iter()
                    .map(|(entry, _, end)| (entry, *first, end)),
            );
            if active {
                tail.active = *first;
                tail.sealed = index.is_some();
                tail.valid_end = scan.valid_end;
                tail.file_len = scan.file_len;
            }
            tail.files.push(file);
        }
        Ok(tail)
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
    fn read(&self, directory: &File) -> Result<(LiveFile, Tail), StoreError> {
        let (live, live_file) = self.read_live()?;
        let tail = self.tail(live.segment, live.offset, live.checkpoint.position)?;
        let log = open_directory(&self.log())?;
        let mut files: Vec<&File> = tail.files.iter().collect();
        files.extend([&live_file, &log, directory]);
        self.sync_read(&files)?;
        Ok((live, tail))
    }
    /// Write a new file atomically: write, synchronize, rename, synchronize the directory.
    fn publish(&self, path: &Path, bytes: &[u8], directory: &File) -> Result<(), StoreError> {
        if bytes.len() as u64 > self.max_bytes {
            return Err(StoreError::TooLarge);
        }
        let parent = path.parent().unwrap_or(&self.directory);
        let (temp, mut file) = loop {
            let seq = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let temp = parent.join(format!(".publish-{}-{seq}.tmp", std::process::id()));
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            match options.open(&temp) {
                Ok(file) => break (temp, file),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(io(e)),
            }
        };
        // Drop removes our candidate on ordinary pre-rename errors. A killed writer
        // may leave an orphan; no reader ever considers candidates authoritative.
        let candidate = Candidate(temp);
        (self.hook)(Stage::BeforeWrite).map_err(io)?;
        let mid = bytes.len() / 2;
        file.write_all(&bytes[..mid]).map_err(io)?;
        (self.hook)(Stage::DuringWrite).map_err(io)?;
        file.write_all(&bytes[mid..]).map_err(io)?;
        file.flush().map_err(io)?;
        file.sync_all().map_err(io)?;
        (self.hook)(Stage::FileSynced).map_err(io)?;
        fs::rename(&candidate.0, path).map_err(io)?;
        (self.hook)(Stage::Renamed).map_err(unknown)?;
        (self.hook)(Stage::DirectorySync).map_err(unknown)?;
        directory.sync_all().map_err(unknown)?;
        (self.hook)(Stage::BeforeResponse).map_err(unknown)?;
        Ok(())
    }
    fn publish_live(&self, live: &LiveFile, directory: &File) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec(live).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        self.publish(&self.directory.join(LIVE), &bytes, directory)
    }
    /// Seal the active segment with an index of its finished transactions and
    /// start the next one. Nothing is appended, so failures here are plain I/O
    /// errors: the log's contents are unchanged.
    fn roll(&self, tail: &Tail, next: u64) -> Result<(), StoreError> {
        let log = open_directory(&self.log())?;
        if !tail.sealed {
            let mut file = self.open_segment(tail.active, false)?;
            let scan = self.scan(&mut file, 0, tail.active, true)?;
            let mut entries: Vec<IndexEntry> = scan
                .entries
                .into_iter()
                .filter_map(|(entry, start, _)| match entry.record {
                    Record::Transaction(tx) if !matches!(tx.state, TxState::Prepared {}) => {
                        Some(IndexEntry {
                            transaction_id: tx.request.transaction_id,
                            sequence: entry.sequence,
                            offset: start,
                        })
                    }
                    _ => None,
                })
                .collect();
            entries.sort_by(|a, b| a.transaction_id.cmp(&b.transaction_id));
            let index = Index {
                segment: tail.active,
                length: scan.valid_end,
                entries,
            };
            self.publish(&self.index_path(tail.active), &encode_frame(&index)?, &log)
                .map_err(|e| match e {
                    StoreError::Unknown(detail) => StoreError::Io(detail),
                    other => other,
                })?;
        }
        (self.hook)(Stage::Sealed).map_err(io)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        options
            .open(self.segment_path(next))
            .map_err(io)?
            .sync_all()
            .map_err(io)?;
        log.sync_all().map_err(io)
    }
    /// Read a schema-1 store's snapshot for migration. Decoding is not validation;
    /// the Application validates it during import.
    pub fn legacy(&self) -> Result<Snapshot, StoreError> {
        self.directory_file()?;
        let (_lock, _) = self.lock(false, false)?;
        let mut file = match File::open(self.directory.join(LEGACY)) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(match self.read_live() {
                    Ok(_) => StoreError::Exists,
                    Err(e) => e,
                });
            }
            Err(e) => return Err(io(e)),
        };
        check_file(&file)?;
        let bytes = self.read_bounded(&mut file)?;
        serde_json::from_slice(&bytes).map_err(|e| StoreError::Corrupt(e.to_string()))
    }
    /// Keep a migrated snapshot for inspection under a name nothing reads, so it
    /// can never be imported again over newer state.
    fn retire_legacy(&self, directory: &File) -> Result<(), StoreError> {
        match fs::rename(self.directory.join(LEGACY), self.directory.join(RETIRED)) {
            Ok(()) => directory.sync_all().map_err(unknown),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(unknown(e)),
        }
    }
    fn read_record(&self, first: u64, offset: u64) -> Result<Entry, StoreError> {
        let mut file = self.open_segment(first, false)?;
        file.seek(SeekFrom::Start(offset)).map_err(io)?;
        let mut header = [0; HEADER];
        file.read_exact(&mut header).map_err(io)?;
        let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as u64;
        if len == 0 || len > self.max_bytes {
            return Err(StoreError::Corrupt(format!(
                "indexed record in segment {first} is invalid"
            )));
        }
        let mut frame = header.to_vec();
        file.take(len).read_to_end(&mut frame).map_err(io)?;
        decode_frame(&frame, self.max_bytes).ok_or_else(|| {
            StoreError::Corrupt(format!("indexed record in segment {first} is invalid"))
        })
    }
}
impl LogStore for FileStore {
    fn initialize(&self, empty: &Checkpoint) -> Result<(), StoreError> {
        if empty.position != 0
            || empty.recorded_at_ms != 0
            || !empty.resources.is_empty()
            || !empty.prepared.is_empty()
        {
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
        // An existing coordination file or log with no live state could represent
        // lost authority, not an empty budget. Even explicit init must fail closed.
        if !created_lock {
            return Err(StoreError::Missing);
        }
        match builder.create(self.log()) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(StoreError::Missing);
            }
            Err(e) => return Err(io(e)),
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        options
            .open(self.segment_path(0))
            .map_err(io)?
            .sync_all()
            .map_err(io)?;
        self.directory_file()?.sync_all().map_err(io)?;
        open_directory(&self.log())?.sync_all().map_err(io)?;
        self.publish_live(
            &LiveFile {
                segment: 0,
                offset: 0,
                checkpoint: empty.clone(),
            },
            &directory,
        )
    }
    /// Migration writes one segment holding every entry, then publishes
    /// `live.json` as its commit point, then retires `state.json`. A log left by
    /// an interrupted migration is discarded and rebuilt only while `state.json`
    /// is still the authority.
    fn import(&self, checkpoint: &Checkpoint, entries: &[Entry]) -> Result<(), StoreError> {
        let ordered = entries.iter().zip(0..).all(|(e, n)| e.sequence == n);
        if !ordered || checkpoint.position != entries.len() as u64 {
            return Err(StoreError::Corrupt(
                "imported entries must be numbered from zero, with the checkpoint at their end"
                    .into(),
            ));
        }
        let mut segment = Vec::new();
        for entry in entries {
            let frame = encode_frame(entry)?;
            if (frame.len() - HEADER) as u64 > self.max_bytes {
                return Err(StoreError::TooLarge);
            }
            segment.extend_from_slice(&frame);
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
        match self.read_live() {
            Ok(_) => {
                // Finish retiring a snapshot left by a migration that published
                // live state but stopped before renaming it.
                self.retire_legacy(&directory)?;
                return Err(StoreError::Exists);
            }
            Err(StoreError::Missing | StoreError::Unsupported(1)) => {}
            Err(e) => return Err(e),
        }
        let legacy = self.directory.join(LEGACY).exists();
        if !created_lock && !legacy {
            return Err(StoreError::Missing);
        }
        match fs::symlink_metadata(self.log()) {
            Ok(_) if legacy => {
                fs::remove_dir_all(self.log()).map_err(io)?;
                directory.sync_all().map_err(io)?;
            }
            Ok(_) => return Err(StoreError::Missing),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io(e)),
        }
        builder.create(self.log()).map_err(io)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(self.segment_path(0)).map_err(io)?;
        file.write_all(&segment).map_err(io)?;
        file.sync_all().map_err(io)?;
        open_directory(&self.log())?.sync_all().map_err(io)?;
        directory.sync_all().map_err(io)?;
        self.publish_live(
            &LiveFile {
                segment: 0,
                offset: segment.len() as u64,
                checkpoint: checkpoint.clone(),
            },
            &directory,
        )?;
        self.retire_legacy(&directory)
    }
    fn load(&self) -> Result<Loaded, StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(false, false)?;
        let (live, tail) = self.read(&directory)?;
        Ok(Loaded {
            checkpoint: live.checkpoint,
            tail: tail.entries.into_iter().map(|(entry, ..)| entry).collect(),
        })
    }
    fn append(&self, entry: &Entry) -> Result<Publication, StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(true, false)?;
        let (live, tail) = self.read(&directory)?;
        if entry.sequence != live.checkpoint.position + tail.entries.len() as u64 {
            return Ok(Publication::Conflict);
        }
        let frame = encode_frame(entry)?;
        // Remove an earlier writer's unacknowledged torn append before writing after it.
        if !tail.sealed && tail.file_len > tail.valid_end {
            let segment = self.open_segment(tail.active, true)?;
            segment.set_len(tail.valid_end).map_err(io)?;
            segment.sync_all().map_err(io)?;
        }
        let (active, offset) = if tail.sealed || tail.valid_end >= self.segment_bytes {
            self.roll(&tail, entry.sequence)?;
            (entry.sequence, 0)
        } else {
            (tail.active, tail.valid_end)
        };
        let mut segment = self.open_segment(active, true)?;
        (self.hook)(Stage::BeforeAppend).map_err(io)?;
        segment.seek(SeekFrom::Start(offset)).map_err(io)?;
        let mid = frame.len() / 2;
        segment.write_all(&frame[..mid]).map_err(io)?;
        (self.hook)(Stage::DuringAppend).map_err(io)?;
        segment.write_all(&frame[mid..]).map_err(io)?;
        // Once every byte is written, a failed synchronization may still have persisted it.
        segment.sync_all().map_err(unknown)?;
        (self.hook)(Stage::AppendSynced).map_err(unknown)?;
        (self.hook)(Stage::BeforeResponse).map_err(unknown)?;
        Ok(Publication::Applied)
    }
    fn checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(true, false)?;
        let (live, tail) = self.read(&directory)?;
        let Some(covered) = checkpoint.position.checked_sub(live.checkpoint.position) else {
            return Ok(());
        };
        if covered == 0 {
            return Ok(());
        }
        let (_, segment, offset) = *tail
            .entries
            .get(covered as usize - 1)
            .ok_or_else(|| StoreError::Corrupt("checkpoint is past the end of the log".into()))?;
        self.publish_live(
            &LiveFile {
                segment,
                offset,
                checkpoint: checkpoint.clone(),
            },
            &directory,
        )
    }
    /// Search the active segment, then sealed indexes from newest to oldest.
    /// Records are in time order, so a segment is skipped, with every older
    /// one, once the segment after it starts before `since_ms`.
    fn find(&self, transaction_id: &str, since_ms: u64) -> Result<Option<Entry>, StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(false, false)?;
        self.read_live()?;
        let segments = self.segments()?;
        let terminal = |entry: &Entry| {
            matches!(&entry.record, Record::Transaction(tx)
                if tx.request.transaction_id == transaction_id
                    && !matches!(tx.state, TxState::Prepared {}))
        };
        let mut newer_starts_ms = u64::MAX;
        for (position, first) in segments.iter().enumerate().rev() {
            if newer_starts_ms < since_ms {
                return Ok(None);
            }
            let Some(index) = self.read_index(*first)? else {
                if position + 1 != segments.len() {
                    return Err(StoreError::Corrupt(format!(
                        "segment {first} is followed by another but was never sealed"
                    )));
                }
                let mut file = self.open_segment(*first, false)?;
                let scan = self.scan(&mut file, 0, *first, true)?;
                self.sync_read(&[&file, &open_directory(&self.log())?, &directory])?;
                if let Some((entry, ..)) = scan
                    .entries
                    .iter()
                    .rev()
                    .take_while(|(entry, ..)| entry.recorded_at_ms >= since_ms)
                    .find(|(entry, ..)| terminal(entry))
                {
                    return Ok(Some(entry.clone()));
                }
                newer_starts_ms = scan
                    .entries
                    .first()
                    .map_or(newer_starts_ms, |(entry, ..)| entry.recorded_at_ms);
                continue;
            };
            if index.length > 0 {
                newer_starts_ms = self.read_record(*first, 0)?.recorded_at_ms;
            }
            let Ok(at) = index
                .entries
                .binary_search_by(|e| e.transaction_id.as_str().cmp(transaction_id))
            else {
                continue;
            };
            let indexed = &index.entries[at];
            let entry = self.read_record(*first, indexed.offset)?;
            if entry.sequence != indexed.sequence || !terminal(&entry) {
                return Err(StoreError::Corrupt(format!(
                    "index for segment {first} disagrees with its records"
                )));
            }
            return Ok((entry.recorded_at_ms >= since_ms).then_some(entry));
        }
        Ok(None)
    }
    fn history(&self) -> Result<Vec<Entry>, StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(false, false)?;
        let (live, live_file) = self.read_live()?;
        let tail = self.tail(0, 0, 0)?;
        let reaches = (live.segment, live.offset) == (0, 0)
            || tail
                .entries
                .iter()
                .any(|(_, segment, end)| (*segment, *end) == (live.segment, live.offset));
        if !reaches {
            return Err(StoreError::Corrupt(
                "checkpoint refers past the end of the log".into(),
            ));
        }
        let log = open_directory(&self.log())?;
        let mut files: Vec<&File> = tail.files.iter().collect();
        files.extend([&live_file, &log, &directory]);
        self.sync_read(&files)?;
        Ok(tail.entries.into_iter().map(|(entry, ..)| entry).collect())
    }
}
fn encode_frame(value: &impl Serialize) -> Result<Vec<u8>, StoreError> {
    let payload = serde_json::to_vec(value).map_err(|e| StoreError::Corrupt(e.to_string()))?;
    let len = u32::try_from(payload.len()).map_err(|_| StoreError::TooLarge)?;
    let mut frame = Vec::with_capacity(HEADER + payload.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&crc32(&payload).to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}
/// Decode exactly one length- and checksum-framed value.
fn decode_frame<T: serde::de::DeserializeOwned>(frame: &[u8], max_bytes: u64) -> Option<T> {
    let header = frame.get(..HEADER)?;
    let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(header[4..].try_into().unwrap());
    let payload = frame.get(HEADER..)?;
    (len > 0 && len as u64 <= max_bytes && payload.len() == len && crc32(payload) == crc)
        .then(|| serde_json::from_slice(payload).ok())
        .flatten()
}
fn open_directory(path: &Path) -> Result<File, StoreError> {
    let dir = File::open(path).map_err(missing)?;
    if !dir.metadata().map_err(io)?.is_dir() {
        return Err(StoreError::Io(format!(
            "{} is not a directory",
            path.display()
        )));
    }
    #[cfg(unix)]
    if dir.metadata().map_err(io)?.permissions().mode() & 0o077 != 0 {
        return Err(StoreError::Io(
            "store directories must have owner-only permissions (chmod 700)".into(),
        ));
    }
    Ok(dir)
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
