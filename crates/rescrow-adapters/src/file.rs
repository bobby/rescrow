//! Cooperative same-user Linux authority. Path manipulation and network filesystems
//! are outside this adapter's contract. The lock inode is never replaced or removed.
use rescrow_application::{
    Publication, SnapshotStore, StoreError, ValidatedSnapshot, dto::Snapshot,
};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};

pub const MAX_SNAPSHOT_BYTES: u64 = 16 * 1024 * 1024;
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

/// Publication checkpoints are exposed only with a caller-supplied hook, for fault
/// qualification. The production constructor uses a no-op hook; no environment
/// variable can enable a crash in ordinary CLI builds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    BeforeWrite,
    DuringWrite,
    FileSynced,
    Renamed,
    DirectorySync,
    BeforeResponse,
    ReadSync,
}
type Hook = fn(Stage) -> std::io::Result<()>;
fn no_fault(_: Stage) -> std::io::Result<()> {
    Ok(())
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
            max_bytes: MAX_SNAPSHOT_BYTES,
        })
    }
    /// A smaller explicit ceiling is useful for embedded budgets and qualification.
    /// It applies to reads and to publication including reserved settlement space.
    pub fn with_size_limit(mut self, bytes: u64) -> Result<Self, StoreError> {
        if bytes == 0 || bytes > MAX_SNAPSHOT_BYTES {
            return Err(StoreError::Io("snapshot limit must be 1..=16 MiB".into()));
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
    fn read(&self, directory: &File) -> Result<ValidatedSnapshot, StoreError> {
        let mut file = File::open(self.directory.join("state.json")).map_err(missing)?;
        check_file(&file)?;
        if file.metadata().map_err(io)?.len() > self.max_bytes {
            return Err(StoreError::TooLarge);
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take(self.max_bytes + 1)
            .read_to_end(&mut bytes)
            .map_err(io)?;
        if bytes.len() as u64 > self.max_bytes {
            return Err(StoreError::TooLarge);
        }
        // Decode only the version first so unsupported schemas have a distinct error.
        #[derive(serde::Deserialize)]
        struct Version {
            schema_version: u32,
        }
        let version: Version =
            serde_json::from_slice(&bytes).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        if version.schema_version != 1 {
            return Err(StoreError::Unsupported(version.schema_version));
        }
        let snapshot: Snapshot =
            serde_json::from_slice(&bytes).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        let snapshot = ValidatedSnapshot::try_from(snapshot)?;
        // Complete a previous writer's potentially uncertain rename before confirming
        // even a read or idempotent retry. Shared locks exclude every publisher.
        (self.hook)(Stage::ReadSync).map_err(unknown)?;
        file.sync_all()
            .map_err(|e| StoreError::Unknown(e.to_string()))?;
        directory
            .sync_all()
            .map_err(|e| StoreError::Unknown(e.to_string()))?;
        Ok(snapshot)
    }
    fn publish(&self, snapshot: &ValidatedSnapshot, directory: &File) -> Result<(), StoreError> {
        let encoded = crate::encoding::encode(snapshot)?;
        if encoded.settlement_bytes > self.max_bytes {
            return Err(StoreError::TooLarge);
        }
        let bytes = encoded.bytes;
        let (path, mut file) = loop {
            let seq = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = self
                .directory
                .join(format!(".state-{}-{seq}.tmp", std::process::id()));
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
        fs::rename(&candidate.0, self.directory.join("state.json")).map_err(io)?;
        (self.hook)(Stage::Renamed).map_err(unknown)?;
        (self.hook)(Stage::DirectorySync).map_err(unknown)?;
        directory.sync_all().map_err(unknown)?;
        (self.hook)(Stage::BeforeResponse).map_err(unknown)?;
        Ok(())
    }
}
impl SnapshotStore for FileStore {
    fn initialize(&self, empty: &ValidatedSnapshot) -> Result<(), StoreError> {
        if empty.revision() != 0
            || !empty.ledger().resources().is_empty()
            || !empty.ledger().transactions().is_empty()
        {
            return Err(StoreError::Corrupt(
                "initial snapshot must be empty at revision zero".into(),
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
        match fs::symlink_metadata(self.directory.join("state.json")) {
            Ok(_) => {
                self.read(&directory)?;
                return Err(StoreError::Exists);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io(e)),
        }
        // An existing coordination file with no live state could represent lost
        // authority, not an empty budget. Even explicit init must fail closed.
        if !created_lock {
            return Err(StoreError::Missing);
        }
        self.publish(empty, &directory)
    }
    fn load(&self) -> Result<ValidatedSnapshot, StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(false, false)?;
        self.read(&directory)
    }
    fn replace(
        &self,
        expected: u64,
        candidate: &ValidatedSnapshot,
    ) -> Result<Publication, StoreError> {
        let directory = self.directory_file()?;
        let (_lock, _) = self.lock(true, false)?;
        let current = self.read(&directory)?;
        if current.revision() != expected {
            return Ok(Publication::Conflict);
        }
        if expected.checked_add(1) != Some(candidate.revision()) {
            return Err(StoreError::Corrupt(
                "candidate revision must advance exactly once".into(),
            ));
        }
        self.publish(candidate, &directory)?;
        Ok(Publication::Applied)
    }
}
struct Candidate(PathBuf);
impl Drop for Candidate {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
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
