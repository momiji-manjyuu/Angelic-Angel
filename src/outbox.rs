//! Crash-durable, single-process notification outbox.
//!
//! A successful enqueue means a private temporary file was synced, atomically renamed,
//! and its directory synced. Use a local filesystem supporting those operations; this
//! is not a network-filesystem/distributed queue. One process owns `.lock`. After a
//! crash, an operator must verify the old process is gone before removing that lock.
//! Never remove a live lock. Existing directory permissions are never changed.
//! On Unix, the target directory and every committed record must deny all group/other
//! permissions. Group/other-writable ancestors require the sticky bit (such as `/tmp`).
//! Readable but non-writable ordinary ancestors are fine.
//!
//! Delivered records discard their payload but retain deduplication metadata forever.
//! Dead letters retain their payload. There is no automatic deletion: arrange disk
//! monitoring, backup/access controls, and an explicit retention policy. Deleting a
//! terminal record also deletes its deduplication protection. Crash-left `.tmp` files
//! are ignored and may be removed only while the queue is stopped. The record limit
//! fails closed rather than silently discarding a notification. At the default limits,
//! pending/dead-letter payloads alone can occupy about 2.5 GiB, plus metadata/temporary
//! rewrite space. Disk exhaustion must prevent upstream ACKs.
//!
//! Delivery is at least once, not exactly once. A crash after a receiver accepts a
//! request but before the delivered record is synced can cause redelivery.
//! A versioned, salted SHA-256 binding fixes the complete canonical endpoint without
//! persisting its URL. Bind an empty queue before starting the source. An endpoint
//! mismatch, or missing binding on a nonempty queue, requires explicit operator action;
//! neither condition silently retargets existing notifications. Use a fresh directory
//! for a different destination unless migration has been explicitly authorized.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

pub type SharedOutbox = Arc<Mutex<Outbox>>;
pub type Result<T> = std::result::Result<T, OutboxError>;

/// Errors deliberately contain no payload, URL, JSON parser text, or credentials.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error("outbox I/O failed ({0:?})")]
    Io(std::io::ErrorKind),
    #[error("outbox directory or record is unsafe or invalid")]
    InvalidStorage,
    #[error("outbox is locked; verify no process is running before stale-lock recovery")]
    Locked,
    #[error("outbox limits are invalid")]
    InvalidLimits,
    #[error("notification exceeds outbox payload or identifier limits")]
    TooLarge,
    #[error("outbox record limit reached; operator retention action required")]
    Full,
    #[error("outbox write outcome is uncertain; stop and reopen before continuing")]
    Unavailable,
    #[error("outbox record is missing or no longer pending")]
    NotPending,
    #[error("another delivery worker already owns this outbox")]
    WorkerActive,
    #[error("outbox blocking operation did not complete")]
    WorkerFailed,
    #[error("system clock is before the Unix epoch")]
    Clock,
    #[error("outbox destination differs from its immutable binding; use a new queue or explicitly authorized migration")]
    DestinationMismatch,
    #[error("outbox has no destination binding; bind before enqueue, or explicitly migrate existing records")]
    DestinationUnbound,
    #[error("outbox destination binding randomness is unavailable")]
    Entropy,
}

impl From<std::io::Error> for OutboxError {
    fn from(error: std::io::Error) -> Self { Self::Io(error.kind()) }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_payload_bytes: usize,
    pub max_records: usize,
}

impl Default for Limits {
    fn default() -> Self { Self { max_payload_bytes: 256 * 1024, max_records: 10_000 } }
}

const MAX_IDENTIFIER_BYTES: usize = 1024;
const RECORD_OVERHEAD_BYTES: usize = 32 * 1024;
const SCHEMA_VERSION: u32 = 1;
const BINDING_FILE: &str = ".destination.json";
const MAX_BINDING_BYTES: u64 = 2048;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DestinationBinding {
    schema_version: u32,
    algorithm: String,
    salt: [u8; 32],
    fingerprint: [u8; 32],
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureReason { PermanentHttp, AttemptsExhausted }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum State {
    Pending { next_attempt_unix_ms: u64 },
    Delivered { delivered_unix_ms: u64 },
    DeadLetter { dead_letter_unix_ms: u64, reason: FailureReason },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u32,
    id: String,
    channel_id: String,
    version: String,
    created_unix_ms: u64,
    attempts: u32,
    last_http_status: Option<u16>,
    state: State,
    payload: Value,
}

#[derive(Clone)]
struct Head {
    channel_id: String,
    version: String,
    attempts: u32,
    state: State,
}

impl From<&Record> for Head {
    fn from(record: &Record) -> Self {
        Self { channel_id: record.channel_id.clone(), version: record.version.clone(),
            attempts: record.attempts, state: record.state.clone() }
    }
}

#[derive(Debug, Clone)]
pub struct EnqueueResult { pub id: String, pub duplicate: bool }

/// Counts are computed from recovered/committed records, not a volatile work channel.
#[derive(Debug, Clone, Serialize)]
pub struct HealthSnapshot {
    pub pending: usize,
    pub delivered: usize,
    pub dead_letter: usize,
    pub next_attempt_unix_ms: Option<u64>,
    pub total_attempts: u64,
    pub storage_healthy: bool,
}

/// Intentionally does not implement Debug: the payload can contain private data.
pub(crate) struct DeliveryAttempt {
    pub id: String,
    pub payload: Value,
    pub attempts: u32,
}

pub struct Outbox {
    directory: PathBuf,
    limits: Limits,
    records: BTreeMap<String, Head>,
    dedup: HashMap<(String, String), String>,
    poisoned: bool,
    destination_binding: Option<DestinationBinding>,
    worker_active: Arc<AtomicBool>,
    // Hold the lock file open for the complete queue lifetime.
    lock_file: File,
}

pub(crate) struct WorkerGuard(Arc<AtomicBool>);
impl Drop for WorkerGuard {
    fn drop(&mut self) { self.0.store(false, Ordering::Release); }
}

impl Outbox {
    /// Opens/recover records and acquires the exclusive process lock. This is blocking
    /// filesystem work; call before starting async tasks or inside spawn_blocking.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_limits(path, Limits::default())
    }

    pub fn open_with_limits(path: impl AsRef<Path>, limits: Limits) -> Result<Self> {
        if limits.max_payload_bytes == 0 || limits.max_records == 0
            || limits.max_payload_bytes > usize::MAX - RECORD_OVERHEAD_BYTES - 1 {
            return Err(OutboxError::InvalidLimits);
        }
        let directory = absolute_directory(path.as_ref())?;
        create_private_directory(&directory)?;
        require_private_permissions(&fs::symlink_metadata(&directory)?)?;
        let lock_path = directory.join(".lock");
        let mut lock_file = match private_new_file(&lock_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Err(OutboxError::Locked),
            Err(error) => return Err(error.into()),
        };
        // If this fails, deliberately retain the lock: recovery is an operator action.
        writeln!(lock_file, "pid={}", std::process::id())?;
        lock_file.sync_all()?;
        sync_directory(&directory)?;
        let mut queue = Self { directory, limits, records: BTreeMap::new(),
            dedup: HashMap::new(), poisoned: false, destination_binding: None,
            worker_active: Arc::new(AtomicBool::new(false)), lock_file };
        if let Err(error) = queue.recover() {
            // Drop releases an ordinary open failure; corrupt data is never overwritten.
            queue.poisoned = true;
            return Err(error);
        }
        Ok(queue)
    }

    /// On success, including duplicate detection, the queue has a durable record for
    /// this exact channel/version pair. No upstream ACK is safe on any error.
    pub fn enqueue(&mut self, channel_id: &str, version: &str, payload: Value) -> Result<EnqueueResult> {
        self.ensure_healthy()?;
        if self.destination_binding.is_none() { return Err(OutboxError::DestinationUnbound); }
        validate_identifiers(channel_id, version)?;
        let key = (channel_id.to_owned(), version.to_owned());
        if let Some(id) = self.dedup.get(&key).cloned() {
            // Fail closed if an operator or storage failure removed/corrupted a
            // committed file while the process was running.
            self.load_record(&id)?;
            return Ok(EnqueueResult { id, duplicate: true });
        }
        validate_payload(&payload, self.limits)?;
        if self.records.len() >= self.limits.max_records { return Err(OutboxError::Full); }
        let now = unix_ms()?;
        let id = loop {
            let candidate = Uuid::new_v4().to_string();
            if !self.records.contains_key(&candidate) { break candidate; }
        };
        let record = Record { schema_version: SCHEMA_VERSION, id,
            channel_id: channel_id.to_owned(), version: version.to_owned(), created_unix_ms: now,
            attempts: 0, last_http_status: None,
            state: State::Pending { next_attempt_unix_ms: now }, payload };
        self.commit(&record)?;
        self.dedup.insert(key, record.id.clone());
        self.records.insert(record.id.clone(), Head::from(&record));
        Ok(EnqueueResult { id: record.id, duplicate: false })
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        let mut result = HealthSnapshot { pending: 0, delivered: 0, dead_letter: 0,
            next_attempt_unix_ms: None, total_attempts: 0, storage_healthy: !self.poisoned };
        for head in self.records.values() {
            result.total_attempts = result.total_attempts.saturating_add(u64::from(head.attempts));
            match head.state {
                State::Pending { next_attempt_unix_ms } => {
                    result.pending += 1;
                    result.next_attempt_unix_ms = Some(result.next_attempt_unix_ms
                        .map_or(next_attempt_unix_ms, |old| old.min(next_attempt_unix_ms)));
                }
                State::Delivered { .. } => result.delivered += 1,
                State::DeadLetter { .. } => result.dead_letter += 1,
            }
        }
        result
    }

    pub(crate) fn claim_worker(&self) -> Result<WorkerGuard> {
        self.ensure_healthy()?;
        self.worker_active.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| OutboxError::WorkerActive)?;
        Ok(WorkerGuard(self.worker_active.clone()))
    }

    /// Called only with the worker's parsed/canonical full URL. The URL is held in
    /// memory for hashing only; no error or serialized structure includes it.
    pub(crate) fn bind_destination(&mut self, canonical_endpoint: &str) -> Result<()> {
        self.ensure_healthy()?;
        let current = match self.read_binding() {
            Ok(binding) => binding,
            Err(error) => { self.poisoned = true; return Err(error); },
        };
        if current != self.destination_binding {
            self.poisoned = true;
            return Err(OutboxError::InvalidStorage);
        }
        if let Some(binding) = current {
            if endpoint_fingerprint(&binding.salt, canonical_endpoint) != binding.fingerprint {
                return Err(OutboxError::DestinationMismatch);
            }
            return Ok(());
        }
        if !self.records.is_empty() { return Err(OutboxError::DestinationUnbound); }
        let mut salt = [0u8; 32];
        rand::rngs::OsRng.try_fill_bytes(&mut salt).map_err(|_| OutboxError::Entropy)?;
        let binding = DestinationBinding { schema_version: 1, algorithm: "sha256".to_owned(),
            fingerprint: endpoint_fingerprint(&salt, canonical_endpoint), salt };
        let bytes = serde_json::to_vec(&binding).map_err(|_| OutboxError::InvalidStorage)?;
        if bytes.len() as u64 > MAX_BINDING_BYTES { return Err(OutboxError::InvalidStorage); }
        self.write_atomic(BINDING_FILE, &bytes)?;
        self.destination_binding = Some(binding);
        Ok(())
    }

    pub(crate) fn prepare_due(&mut self, now: u64) -> Result<Option<DeliveryAttempt>> {
        self.ensure_healthy()?;
        let id = self.records.iter().filter_map(|(id, head)| match head.state {
            State::Pending { next_attempt_unix_ms } if next_attempt_unix_ms <= now => Some((next_attempt_unix_ms, id)),
            _ => None,
        }).min().map(|(_, id)| id.clone());
        let Some(id) = id else { return Ok(None); };
        let mut record = self.load_record(&id)?;
        record.attempts = record.attempts.checked_add(1).ok_or(OutboxError::InvalidStorage)?;
        let payload = record.payload.clone();
        self.commit(&record)?;
        self.records.insert(id.clone(), Head::from(&record));
        Ok(Some(DeliveryAttempt { id, payload, attempts: record.attempts }))
    }

    pub(crate) fn delivered(&mut self, id: &str, now: u64, status: u16) -> Result<()> {
        self.update_pending(id, State::Delivered { delivered_unix_ms: now }, Some(status), true)
    }

    pub(crate) fn retry(&mut self, id: &str, next: u64, status: Option<u16>) -> Result<()> {
        self.update_pending(id, State::Pending { next_attempt_unix_ms: next }, status, false)
    }

    pub(crate) fn dead_letter(&mut self, id: &str, now: u64, status: Option<u16>, reason: FailureReason) -> Result<()> {
        self.update_pending(id, State::DeadLetter { dead_letter_unix_ms: now, reason }, status, false)
    }

    fn update_pending(&mut self, id: &str, state: State, status: Option<u16>, discard_payload: bool) -> Result<()> {
        self.ensure_healthy()?;
        let Some(head) = self.records.get(id) else { return Err(OutboxError::NotPending); };
        if !matches!(head.state, State::Pending { .. }) { return Err(OutboxError::NotPending); }
        let mut record = self.load_record(id)?;
        record.state = state;
        record.last_http_status = status;
        if discard_payload { record.payload = Value::Null; }
        self.commit(&record)?;
        self.records.insert(id.to_owned(), Head::from(&record));
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned { Err(OutboxError::Unavailable) } else { Ok(()) }
    }

    fn recover(&mut self) -> Result<()> {
        self.destination_binding = self.read_binding()?;
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if !file_type.is_file() { return Err(OutboxError::InvalidStorage); }
            let name = entry.file_name().into_string().map_err(|_| OutboxError::InvalidStorage)?;
            if name == ".lock" { continue; }
            // The exact binding filename is validated above, never treated as a
            // notification or silently generalized to arbitrary dotfiles.
            if name == BINDING_FILE { continue; }
            if is_temporary_name(&name) { continue; }
            let id = name.strip_suffix(".json").ok_or(OutboxError::InvalidStorage)?;
            if !canonical_uuid(id) { return Err(OutboxError::InvalidStorage); }
            let record = self.read_record(id)?;
            let key = (record.channel_id.clone(), record.version.clone());
            if self.dedup.insert(key, id.to_owned()).is_some() { return Err(OutboxError::InvalidStorage); }
            self.records.insert(id.to_owned(), Head::from(&record));
            if self.records.len() > self.limits.max_records { return Err(OutboxError::Full); }
        }
        if !self.records.is_empty() && self.destination_binding.is_none() {
            return Err(OutboxError::DestinationUnbound);
        }
        Ok(())
    }

    fn read_binding(&self) -> Result<Option<DestinationBinding>> {
        let path = self.directory.join(BINDING_FILE);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_file() || metadata.len() > MAX_BINDING_BYTES { return Err(OutboxError::InvalidStorage); }
        require_private_permissions(&metadata)?;
        let mut bytes = Vec::new();
        File::open(path)?.take(MAX_BINDING_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_BINDING_BYTES { return Err(OutboxError::InvalidStorage); }
        let binding: DestinationBinding = serde_json::from_slice(&bytes).map_err(|_| OutboxError::InvalidStorage)?;
        if binding.schema_version != 1 || binding.algorithm != "sha256" { return Err(OutboxError::InvalidStorage); }
        Ok(Some(binding))
    }

    fn read_record(&self, id: &str) -> Result<Record> {
        if !canonical_uuid(id) { return Err(OutboxError::InvalidStorage); }
        let path = self.directory.join(format!("{id}.json"));
        let metadata = fs::symlink_metadata(&path)?;
        let max_record_bytes = self.limits.max_payload_bytes + RECORD_OVERHEAD_BYTES;
        if !metadata.is_file() || metadata.len() > max_record_bytes as u64 { return Err(OutboxError::InvalidStorage); }
        require_private_permissions(&metadata)?;
        // A second bound protects against accidental concurrent growth after metadata.
        let mut bytes = Vec::new();
        File::open(path)?.take(max_record_bytes as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > max_record_bytes { return Err(OutboxError::InvalidStorage); }
        let record: Record = serde_json::from_slice(&bytes).map_err(|_| OutboxError::InvalidStorage)?;
        if record.id != id || record.schema_version != SCHEMA_VERSION { return Err(OutboxError::InvalidStorage); }
        validate_identifiers(&record.channel_id, &record.version).map_err(|_| OutboxError::InvalidStorage)?;
        match &record.state {
            State::Delivered { .. } if record.payload.is_null() => {},
            State::Pending { .. } | State::DeadLetter { .. } => {
                validate_payload(&record.payload, self.limits).map_err(|_| OutboxError::InvalidStorage)?;
            },
            _ => return Err(OutboxError::InvalidStorage),
        }
        // Metadata cannot change under the process lock. Detect accidental/manual edits
        // while live rather than delivering a different identity or stale terminal state.
        if let Some(head) = self.records.get(id) {
            if head.channel_id != record.channel_id || head.version != record.version
                || head.attempts != record.attempts
                || head.state != record.state {
                return Err(OutboxError::InvalidStorage);
            }
        }
        Ok(record)
    }

    fn load_record(&mut self, id: &str) -> Result<Record> {
        let result = self.read_record(id);
        if result.is_err() { self.poisoned = true; }
        result
    }

    fn commit(&mut self, record: &Record) -> Result<()> {
        self.ensure_healthy()?;
        let bytes = serde_json::to_vec(record).map_err(|_| OutboxError::InvalidStorage)?;
        if bytes.len() > self.limits.max_payload_bytes + RECORD_OVERHEAD_BYTES { return Err(OutboxError::TooLarge); }
        // The record envelope consumes JSON nesting depth too. Never acknowledge a
        // payload that our bounded/default-depth recovery parser cannot read back.
        let _: Record = serde_json::from_slice(&bytes).map_err(|_| OutboxError::TooLarge)?;
        self.write_atomic(&format!("{}.json", record.id), &bytes)
    }

    fn write_atomic(&mut self, filename: &str, bytes: &[u8]) -> Result<()> {
        self.ensure_healthy()?;
        let temporary = self.directory.join(format!(".{}.{}.tmp", Uuid::new_v4(), Uuid::new_v4()));
        let destination = self.directory.join(filename);
        let outcome = (|| -> Result<()> {
            let mut file = private_new_file(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, &destination)?;
            sync_directory(&self.directory)?;
            Ok(())
        })();
        if outcome.is_err() {
            // Rename may already have succeeded. Never return a duplicate success or
            // continue delivery against stale in-memory state after an uncertain write.
            self.poisoned = true;
            let _ = fs::remove_file(&temporary);
        }
        outcome
    }
}

impl Drop for Outbox {
    fn drop(&mut self) {
        // On ordinary shutdown release the lock. A killed process leaves it for manual
        // recovery. Never delete it through some other process while this object lives.
        let _ = self.lock_file.sync_all();
        if fs::remove_file(self.directory.join(".lock")).is_ok() {
            let _ = sync_directory(&self.directory);
        }
    }
}

/// Async entry point: does not block Tokio's executor threads. Cancellation of the
/// awaiting caller does not interrupt the durable operation; absence of its result
/// must be treated as unknown and the source must redeliver (dedup makes that safe).
pub async fn enqueue_durable(queue: SharedOutbox, channel_id: String, version: String, payload: Value) -> Result<EnqueueResult> {
    tokio::task::spawn_blocking(move || queue.blocking_lock().enqueue(&channel_id, &version, payload))
        .await.map_err(|_| OutboxError::WorkerFailed)?
}

pub(crate) fn unix_ms() -> Result<u64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| OutboxError::Clock)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| OutboxError::Clock)
}

fn endpoint_fingerprint(salt: &[u8; 32], canonical_endpoint: &str) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"angelic-angel-webhook-binding-v1\0");
    hash.update(salt);
    hash.update(canonical_endpoint.as_bytes());
    hash.finalize().into()
}

fn validate_identifiers(channel: &str, version: &str) -> Result<()> {
    if channel.is_empty() || version.is_empty() || channel.len() > MAX_IDENTIFIER_BYTES || version.len() > MAX_IDENTIFIER_BYTES {
        Err(OutboxError::TooLarge)
    } else { Ok(()) }
}

fn validate_payload(payload: &Value, limits: Limits) -> Result<()> {
    let bytes = serde_json::to_vec(payload).map_err(|_| OutboxError::TooLarge)?;
    if bytes.len() > limits.max_payload_bytes { Err(OutboxError::TooLarge) } else { Ok(()) }
}

fn canonical_uuid(value: &str) -> bool {
    Uuid::parse_str(value).map(|id| id.to_string() == value).unwrap_or(false)
}

fn is_temporary_name(name: &str) -> bool {
    let Some(inner) = name.strip_prefix('.').and_then(|s| s.strip_suffix(".tmp")) else { return false; };
    let Some((record, nonce)) = inner.split_once('.') else { return false; };
    canonical_uuid(record) && canonical_uuid(nonce)
}

fn absolute_directory(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() { return Err(OutboxError::InvalidStorage); }
    let path = if path.is_absolute() { path.to_owned() } else { std::env::current_dir()?.join(path) };
    if path.components().any(|part| matches!(part, Component::ParentDir)) { return Err(OutboxError::InvalidStorage); }
    Ok(path)
}

fn create_private_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() || metadata.file_type().is_symlink() { return Err(OutboxError::InvalidStorage); }
            #[cfg(unix)] {
                use std::os::unix::fs::PermissionsExt;
                let mode = metadata.permissions().mode();
                if mode & 0o022 != 0 && mode & 0o1000 == 0 { return Err(OutboxError::InvalidStorage); }
            }
            // Check every ancestor even when the final directory already exists.
            if let Some(parent) = path.parent() { create_private_directory(parent)?; }
            Ok(())
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path.parent().ok_or(OutboxError::InvalidStorage)?;
            create_private_directory(parent)?;
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)] {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(path) {
                Ok(()) => { sync_directory(path)?; sync_directory(parent)?; Ok(()) },
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => create_private_directory(path),
                Err(error) => Err(error.into()),
            }
        },
        Err(error) => Err(error.into()),
    }
}

fn private_new_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)] {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn require_private_permissions(metadata: &fs::Metadata) -> Result<()> {
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 { return Err(OutboxError::InvalidStorage); }
    }
    #[cfg(not(unix))]
    let _ = metadata;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> { File::open(path)?.sync_all().map_err(Into::into) }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct TestDirectory(PathBuf);
    impl TestDirectory {
        fn new() -> Self { Self(std::env::temp_dir().join(format!("angelic-outbox-test-{}", Uuid::new_v4()))) }
    }
    impl Drop for TestDirectory {
        fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
    }

    const TEST_ENDPOINT: &str = "https://example.invalid/webhook?token=synthetic-fixture";

    fn open_bound(path: &Path) -> Outbox {
        let mut queue = Outbox::open(path).unwrap();
        queue.bind_destination(TEST_ENDPOINT).unwrap();
        queue
    }

    #[test]
    fn enqueue_recovers_and_deduplicates_terminal_records() {
        let directory = TestDirectory::new();
        let id = {
            let mut queue = open_bound(&directory.0);
            let first = queue.enqueue("channel", "1", json!({"message": "hello"})).unwrap();
            assert!(!first.duplicate);
            let duplicate = queue.enqueue("channel", "1", json!({"message": "different"})).unwrap();
            assert!(duplicate.duplicate);
            assert_eq!(first.id, duplicate.id);
            queue.delivered(&first.id, unix_ms().unwrap(), 204).unwrap();
            first.id
        };
        let mut recovered = open_bound(&directory.0);
        assert_eq!(recovered.snapshot().delivered, 1);
        let duplicate = recovered.enqueue("channel", "1", json!({})).unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.id, id);
        let record = recovered.read_record(&id).unwrap();
        assert!(record.payload.is_null());
    }

    #[test]
    fn exclusive_lock_and_limits_fail_closed() {
        let directory = TestDirectory::new();
        let mut queue = Outbox::open_with_limits(&directory.0, Limits { max_payload_bytes: 10, max_records: 1 }).unwrap();
        queue.bind_destination(TEST_ENDPOINT).unwrap();
        assert!(matches!(Outbox::open(&directory.0), Err(OutboxError::Locked)));
        assert!(matches!(queue.enqueue("c", "1", json!("01234567890")), Err(OutboxError::TooLarge)));
        queue.enqueue("c", "1", json!(1)).unwrap();
        assert!(matches!(queue.enqueue("c", "2", json!(2)), Err(OutboxError::Full)));
        assert!(queue.enqueue("c", "1", json!(3)).unwrap().duplicate);
    }

    #[test]
    fn retry_and_dead_letter_survive_reopen() {
        let directory = TestDirectory::new();
        let id = {
            let mut queue = open_bound(&directory.0);
            let id = queue.enqueue("c", "v", json!({"x": true})).unwrap().id;
            let attempt = queue.prepare_due(u64::MAX).unwrap().unwrap();
            assert_eq!(attempt.attempts, 1);
            queue.retry(&id, u64::MAX, Some(429)).unwrap();
            assert!(queue.prepare_due(unix_ms().unwrap()).unwrap().is_none());
            id
        };
        {
            let mut queue = open_bound(&directory.0);
            assert_eq!(queue.snapshot().next_attempt_unix_ms, Some(u64::MAX));
            queue.dead_letter(&id, unix_ms().unwrap(), Some(400), FailureReason::PermanentHttp).unwrap();
        }
        let queue = open_bound(&directory.0);
        assert_eq!(queue.snapshot().dead_letter, 1);
        assert_eq!(queue.snapshot().total_attempts, 1);
    }

    #[test]
    fn corrupt_record_prevents_open() {
        let directory = TestDirectory::new();
        let id = { open_bound(&directory.0).enqueue("c", "v", json!({})).unwrap().id };
        fs::write(directory.0.join(format!("{id}.json")), b"not json").unwrap();
        assert!(matches!(Outbox::open(&directory.0), Err(OutboxError::InvalidStorage)));
    }

    #[test]
    fn failed_commit_never_acknowledges_and_poison_prevents_false_duplicates() {
        let directory = TestDirectory::new();
        let moved = TestDirectory::new();
        let mut queue = open_bound(&directory.0);
        let first = queue.enqueue("c", "1", json!({})).unwrap();
        // Simulate storage becoming unavailable, without permission changes or an
        // injectable production bypass. Both directories belong exclusively to this test.
        fs::rename(&directory.0, &moved.0).unwrap();
        assert!(queue.enqueue("c", "2", json!({})).is_err());
        assert!(!queue.snapshot().storage_healthy);
        assert!(matches!(queue.enqueue("c", "1", json!({})), Err(OutboxError::Unavailable)));
        fs::rename(&moved.0, &directory.0).unwrap();
        drop(queue);
        let mut reopened = open_bound(&directory.0);
        assert_eq!(reopened.enqueue("c", "1", json!({})).unwrap().id, first.id);
        assert!(!reopened.enqueue("c", "2", json!({})).unwrap().duplicate);
    }

    #[test]
    fn null_payload_and_uncommitted_temporary_file_recover_safely() {
        let directory = TestDirectory::new();
        {
            let mut queue = open_bound(&directory.0);
            queue.enqueue("c", "1", Value::Null).unwrap();
            let temporary = directory.0.join(format!(".{}.{}.tmp", Uuid::new_v4(), Uuid::new_v4()));
            private_new_file(&temporary).unwrap().write_all(b"partial").unwrap();
        }
        let mut queue = open_bound(&directory.0);
        assert_eq!(queue.snapshot().pending, 1);
        assert!(queue.prepare_due(u64::MAX).unwrap().unwrap().payload.is_null());
    }

    #[cfg(unix)]
    #[test]
    fn created_paths_are_private_and_symlinks_rejected() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let directory = TestDirectory::new();
        let mut queue = open_bound(&directory.0);
        let id = queue.enqueue("c", "v", json!({})).unwrap().id;
        assert_eq!(fs::metadata(&directory.0).unwrap().permissions().mode() & 0o777, 0o700);
        assert_eq!(fs::metadata(directory.0.join(format!("{id}.json"))).unwrap().permissions().mode() & 0o777, 0o600);
        let link = TestDirectory::new();
        symlink(&directory.0, &link.0).unwrap();
        assert!(matches!(Outbox::open(&link.0), Err(OutboxError::InvalidStorage)));
        fs::remove_file(&link.0).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn nonprivate_existing_target_is_rejected_without_chmod() {
        use std::os::unix::fs::PermissionsExt;
        let directory = TestDirectory::new();
        create_private_directory(&directory.0).unwrap();
        fs::set_permissions(&directory.0, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(Outbox::open(&directory.0), Err(OutboxError::InvalidStorage)));
        assert_eq!(fs::metadata(&directory.0).unwrap().permissions().mode() & 0o777, 0o755);
        assert!(!directory.0.join(".lock").exists());
    }

    #[cfg(unix)]
    #[test]
    fn nonprivate_record_is_rejected_without_chmod() {
        use std::os::unix::fs::PermissionsExt;
        let directory = TestDirectory::new();
        let id = {
            let mut queue = open_bound(&directory.0);
            queue.enqueue("c", "v", json!({})).unwrap().id
        };
        let path = directory.0.join(format!("{id}.json"));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(matches!(Outbox::open(&directory.0), Err(OutboxError::InvalidStorage)));
        assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o640);
    }

    #[cfg(unix)]
    #[test]
    fn ordinary_ancestor_permissions_are_preserved() {
        use std::os::unix::fs::PermissionsExt;
        let ancestor = TestDirectory::new();
        create_private_directory(&ancestor.0).unwrap();
        fs::set_permissions(&ancestor.0, fs::Permissions::from_mode(0o755)).unwrap();
        let directory = ancestor.0.join("private-outbox");
        let queue = Outbox::open(&directory).unwrap();
        assert_eq!(fs::metadata(&ancestor.0).unwrap().permissions().mode() & 0o777, 0o755);
        assert_eq!(fs::metadata(&directory).unwrap().permissions().mode() & 0o777, 0o700);
        drop(queue);
    }

    #[cfg(unix)]
    #[test]
    fn nonsticky_writable_ancestor_is_rejected_without_chmod() {
        use std::os::unix::fs::PermissionsExt;
        let ancestor = TestDirectory::new();
        create_private_directory(&ancestor.0).unwrap();
        fs::set_permissions(&ancestor.0, fs::Permissions::from_mode(0o777)).unwrap();
        let directory = ancestor.0.join("private-outbox");
        assert!(matches!(Outbox::open(&directory), Err(OutboxError::InvalidStorage)));
        assert!(!directory.exists());
        assert_eq!(fs::metadata(&ancestor.0).unwrap().permissions().mode() & 0o7777, 0o777);
    }

    #[test]
    fn endpoint_binding_survives_reopen_without_storing_url() {
        let directory = TestDirectory::new();
        let original = {
            let mut queue = open_bound(&directory.0);
            queue.enqueue("c", "1", json!({})).unwrap();
            fs::read(directory.0.join(BINDING_FILE)).unwrap()
        };
        let mut queue = Outbox::open(&directory.0).unwrap();
        queue.bind_destination(TEST_ENDPOINT).unwrap();
        assert_eq!(fs::read(directory.0.join(BINDING_FILE)).unwrap(), original);
        let serialized = String::from_utf8(original).unwrap();
        assert!(!serialized.contains(TEST_ENDPOINT));
        assert!(!serialized.contains("synthetic-fixture"));
        assert!(!serialized.contains("example.invalid"));
        #[cfg(unix)] {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(directory.0.join(BINDING_FILE)).unwrap().permissions().mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn changed_full_endpoint_is_rejected_without_rebinding() {
        let directory = TestDirectory::new();
        let mut queue = open_bound(&directory.0);
        let original = fs::read(directory.0.join(BINDING_FILE)).unwrap();
        for changed in [
            "https://example.invalid/other-path?token=synthetic-fixture",
            "https://example.invalid/webhook?token=different-fixture",
            "https://other.invalid/webhook?token=synthetic-fixture",
            "https://example.invalid:8443/webhook?token=synthetic-fixture",
        ] {
            assert!(matches!(queue.bind_destination(changed), Err(OutboxError::DestinationMismatch)));
            assert_eq!(fs::read(directory.0.join(BINDING_FILE)).unwrap(), original);
        }
        queue.bind_destination(TEST_ENDPOINT).unwrap();
    }

    #[test]
    fn missing_binding_fails_before_enqueue_and_with_existing_records() {
        let directory = TestDirectory::new();
        {
            let mut queue = Outbox::open(&directory.0).unwrap();
            assert!(matches!(queue.enqueue("c", "1", json!({})), Err(OutboxError::DestinationUnbound)));
            queue.bind_destination(TEST_ENDPOINT).unwrap();
            queue.enqueue("c", "1", json!({})).unwrap();
        }
        fs::remove_file(directory.0.join(BINDING_FILE)).unwrap();
        assert!(matches!(Outbox::open(&directory.0), Err(OutboxError::DestinationUnbound)));
        assert!(!directory.0.join(BINDING_FILE).exists());
    }

    #[test]
    fn corrupt_or_oversized_binding_fails_closed() {
        for contents in [b"invalid".to_vec(), vec![b' '; MAX_BINDING_BYTES as usize + 1]] {
            let directory = TestDirectory::new();
            drop(open_bound(&directory.0));
            fs::write(directory.0.join(BINDING_FILE), contents).unwrap();
            assert!(matches!(Outbox::open(&directory.0), Err(OutboxError::InvalidStorage)));
        }
    }

    #[cfg(unix)]
    #[test]
    fn binding_rejects_nonprivate_mode() {
        use std::os::unix::fs::PermissionsExt;
        let directory = TestDirectory::new();
        drop(open_bound(&directory.0));
        let path = directory.0.join(BINDING_FILE);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(Outbox::open(&directory.0), Err(OutboxError::InvalidStorage)));
        assert_eq!(fs::metadata(path).unwrap().permissions().mode() & 0o777, 0o644);
    }
}
