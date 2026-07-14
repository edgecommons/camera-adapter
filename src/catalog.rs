//! Durable SQLite catalog for capture jobs, command idempotency, recovery, and outbox delivery.
//!
//! All SQLite access runs on a fixed set of dedicated operating-system threads. Async callers use a
//! bounded channel, so catalog pressure applies backpressure without ever executing blocking SQLite
//! work on a Tokio worker. The state-directory lock is retained by the worker threads until every
//! connection has closed.

#[cfg(not(windows))]
use std::fs;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use edgecommons::facades::PreparedAppMessage;
use edgecommons::messaging::Message;
use fs4::fs_std::FileExt;
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Row, Transaction, TransactionBehavior, params,
};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};

use crate::idempotency::{RequestHash, validate_request_id};
use crate::model::JobState;
use crate::{CameraError, Result};

const DATABASE_NAME: &str = "camera-adapter.sqlite3";

/// The one integrity failure that means the FILE is bad rather than the environment.
///
/// It is a constant because `open_verified` matches on it to decide whether to quarantine, and
/// deciding that by string comparison against a literal typed in two places is how the wrong file
/// gets deleted.
const INTEGRITY_CHECK_FAILED: &str = "SQLite integrity_check did not return ok";
const LOCK_NAME: &str = "camera-adapter.lock";
const SCHEMA_VERSION: i64 = 4;
const DEFAULT_WORKERS: usize = 2;
const MAX_WORKERS: usize = 16;
const DEFAULT_QUEUE_CAPACITY: usize = 128;
const BUSY_TIMEOUT_MS: i64 = 5_000;

/// Ledger verb of the reconnect command.
///
/// Reconnect is the one mutating verb that performs no physical actuation: it asks the runtime to
/// drop and re-establish a session, which is idempotent and safe to redo.
pub const RECONNECT_VERB: &str = "sb/reconnect";

type Work = Box<dyn FnOnce(&mut Connection) + Send + 'static>;

/// Bounded catalog runtime configuration.
#[derive(Debug, Clone)]
pub struct CatalogOptions {
    /// Directory containing the lock and SQLite files.
    pub state_directory: PathBuf,
    /// Number of dedicated SQLite connections and worker threads.
    pub worker_count: usize,
    /// Maximum number of waiting operations before async senders apply backpressure.
    pub queue_capacity: usize,
}

impl CatalogOptions {
    /// Creates production defaults for a state directory.
    #[must_use]
    pub fn new(state_directory: impl Into<PathBuf>) -> Self {
        Self {
            state_directory: state_directory.into(),
            worker_count: DEFAULT_WORKERS,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

/// Durable command-ledger key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LedgerKey {
    /// Camera instance, or the component-scoped group instance.
    pub instance: String,
    /// Stable command verb.
    pub verb: String,
    /// Caller-provided durable operation identifier.
    pub request_id: String,
}

impl LedgerKey {
    /// Creates a key after validating all non-empty components and the request-id contract.
    pub fn new(
        instance: impl Into<String>,
        verb: impl Into<String>,
        request_id: impl Into<String>,
    ) -> Result<Self> {
        let key = Self {
            instance: instance.into(),
            verb: verb.into(),
            request_id: request_id.into(),
        };
        validate_ledger_key(&key)?;
        Ok(key)
    }
}

/// A group schedule's durable recovery cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupScheduleCursor {
    /// Latest unjittered intended-fire instant this schedule has consumed.
    pub intended_fire_time_ms: i64,
    /// The group admitted for the most recent admitted occurrence, if any.
    pub last_group_id: Option<String>,
}

/// Absolute durable deadlines for one capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobDeadlines {
    /// Overall terminal deadline in Unix epoch milliseconds.
    pub terminal_at_ms: i64,
    /// Optional queue-admission deadline in Unix epoch milliseconds.
    pub queue_at_ms: Option<i64>,
    /// Acquisition-stage deadline in Unix epoch milliseconds.
    pub capture_at_ms: i64,
    /// Encoding-stage deadline in Unix epoch milliseconds.
    pub encode_at_ms: i64,
    /// Persistence-stage deadline in Unix epoch milliseconds.
    pub persist_at_ms: i64,
}

/// Complete immutable material for accepting a capture job.
#[derive(Debug, Clone)]
pub struct NewJob {
    /// Globally unique, sortable capture identifier.
    pub capture_id: String,
    /// Camera instance.
    pub instance: String,
    /// Capture command ledger key. Scheduled work may omit it.
    pub ledger_key: Option<LedgerKey>,
    /// Original canonical request, retained exactly as a JSON value.
    pub canonical_request: Value,
    /// Immutable-argument hash.
    pub request_hash: RequestHash,
    /// Effective resolved capture profile.
    pub effective_profile: Value,
    /// Durable absolute deadlines.
    pub deadlines: JobDeadlines,
    /// Trigger metadata, including schedule context where applicable.
    pub trigger: Value,
    /// Correlation identifier belonging only to the operation creator.
    pub origin_correlation_id: Option<String>,
    /// Intended output/path metadata resolved before queue admission.
    pub intended_output: Value,
    /// Acceptance timestamp in Unix epoch milliseconds.
    pub accepted_at_ms: i64,
    /// Optional owning group identifier.
    pub group_id: Option<String>,
}

/// Complete immutable material for atomically accepting a capture group and its member jobs.
#[derive(Debug, Clone)]
pub struct NewGroup {
    /// Globally unique group identifier.
    pub group_id: String,
    /// Component-scoped capture-group ledger key.
    pub ledger_key: LedgerKey,
    /// Original canonical group request.
    pub canonical_request: Value,
    /// Immutable group request hash.
    pub request_hash: RequestHash,
    /// Creator correlation identifier.
    pub origin_correlation_id: Option<String>,
    /// Acceptance timestamp in Unix epoch milliseconds.
    pub accepted_at_ms: i64,
    /// Fully resolved member jobs, in caller-visible result order.
    pub members: Vec<NewJob>,
}

/// Durable job projection returned to schedulers, commands, and recovery logic.
#[derive(Debug, Clone, PartialEq)]
pub struct JobRecord {
    /// Capture identifier.
    pub capture_id: String,
    /// Camera instance.
    pub instance: String,
    /// Optional command verb.
    pub verb: Option<String>,
    /// Optional durable request identifier.
    pub request_id: Option<String>,
    /// Current durable state.
    pub state: JobState,
    /// Original canonical request.
    pub canonical_request: Value,
    /// Immutable request hash.
    pub request_hash: RequestHash,
    /// Effective profile snapshot.
    pub effective_profile: Value,
    /// Durable deadlines.
    pub deadlines: JobDeadlines,
    /// Trigger metadata.
    pub trigger: Value,
    /// Creator correlation identifier.
    pub origin_correlation_id: Option<String>,
    /// Intended output metadata.
    pub intended_output: Value,
    /// Atomic-write partial path, once persistence begins.
    pub partial_path: Option<String>,
    /// Final installation path, once persistence begins.
    pub final_path: Option<String>,
    /// Prepared image checksum durably expected before installation starts.
    pub expected_sha256: Option<String>,
    /// Prepared image byte count durably expected before installation starts.
    pub expected_bytes: Option<u64>,
    /// Verified lower-case SHA-256 of the installed artifact.
    pub installed_sha256: Option<String>,
    /// Verified installed byte count.
    pub installed_bytes: Option<u64>,
    /// Acceptance timestamp.
    pub accepted_at_ms: i64,
    /// Queue transition timestamp.
    pub queued_at_ms: Option<i64>,
    /// Terminal timestamp.
    pub terminal_at_ms: Option<i64>,
    /// Owning group.
    pub group_id: Option<String>,
    /// Whether the irrevocable install step has won its CAS.
    pub install_started: bool,
    /// Terminal success/error material.
    pub terminal_result: Option<Value>,
    /// Stable terminal error code.
    pub error_code: Option<String>,
    /// Sanitized terminal error message.
    pub error_message: Option<String>,
    /// Exact success terminal write staged durably before sidecar/install arbitration.
    pub pending_success: Option<TerminalWrite>,
}

/// Result of a durable acceptance attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum AcceptJobOutcome {
    /// A new ACCEPTED record was committed.
    Inserted(JobRecord),
    /// An exact retry found the existing operation.
    Existing(JobRecord),
    /// The ledger key already belongs to different immutable arguments.
    Conflict,
}

/// Durable group projection.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupRecord {
    /// Group identifier.
    pub group_id: String,
    /// Durable request identifier.
    pub request_id: String,
    /// Durable state shared by the group acceptance/queue phases.
    pub state: JobState,
    /// Original canonical request.
    pub canonical_request: Value,
    /// Immutable request hash.
    pub request_hash: RequestHash,
    /// Creator correlation identifier.
    pub origin_correlation_id: Option<String>,
    /// Acceptance timestamp.
    pub accepted_at_ms: i64,
    /// Queue timestamp.
    pub queued_at_ms: Option<i64>,
    /// Terminal timestamp.
    pub terminal_at_ms: Option<i64>,
    /// Retained aggregate result/error body.
    pub terminal_result: Option<Value>,
    /// Stable retained group error code.
    pub error_code: Option<String>,
    /// Sanitized retained group error message.
    pub error_message: Option<String>,
    /// Member jobs in stable result order.
    pub members: Vec<JobRecord>,
}

/// Result of a durable group acceptance attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum AcceptGroupOutcome {
    /// A new group and all ACCEPTED member jobs were committed atomically.
    Inserted(GroupRecord),
    /// An exact retry found the existing group.
    Existing(GroupRecord),
    /// The ledger key already belongs to different immutable arguments.
    Conflict,
}

/// Outcome of an ordinary state compare-and-set.
#[derive(Debug, Clone, PartialEq)]
pub enum StateCasOutcome {
    /// This caller changed the state.
    Changed(JobRecord),
    /// The stored state did not match the expected state.
    NotChanged(JobRecord),
}

/// Exact terminal outbox message. The encoded bytes are never regenerated by the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOutboxMessage {
    event_key: String,
    message_kind: String,
    header_name: String,
    topic: String,
    envelope_uuid: String,
    encoded_envelope: Vec<u8>,
    created_at_ms: i64,
    available_at_ms: i64,
}

impl NewOutboxMessage {
    /// Copies the exact topic, UUID, and encoded bytes from a core prepared application message.
    #[must_use]
    pub fn from_prepared(
        event_key: impl Into<String>,
        message_kind: impl Into<String>,
        prepared: &PreparedAppMessage,
        created_at_ms: i64,
        available_at_ms: i64,
    ) -> Self {
        Self {
            event_key: event_key.into(),
            message_kind: message_kind.into(),
            header_name: prepared.message().header.name.clone(),
            topic: prepared.topic().to_owned(),
            envelope_uuid: prepared.message().header.uuid.clone(),
            encoded_envelope: prepared.encoded().to_vec(),
            created_at_ms,
            available_at_ms,
        }
    }

    /// Builds an exact outbox record from an already stamped EdgeCommons envelope.
    ///
    /// This is primarily the seam for deterministic envelope encoders and tests; production
    /// application messages normally use [`Self::from_prepared`]. The catalog still decodes and
    /// validates the bytes again in the terminal transaction.
    pub fn from_message(
        event_key: impl Into<String>,
        message_kind: impl Into<String>,
        topic: impl Into<String>,
        message: &Message,
        created_at_ms: i64,
        available_at_ms: i64,
    ) -> Result<Self> {
        let encoded_envelope = message
            .to_vec()
            .map_err(|error| CameraError::Messaging(error.to_string()))?;
        Ok(Self {
            event_key: event_key.into(),
            message_kind: message_kind.into(),
            header_name: message.header.name.clone(),
            topic: topic.into(),
            envelope_uuid: message.header.uuid.clone(),
            encoded_envelope,
            created_at_ms,
            available_at_ms,
        })
    }
}

/// Terminal state plus exact message bytes committed in one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalWrite {
    /// Winning terminal state.
    pub state: JobState,
    /// Terminal result/error body retained for command retries.
    pub result: Value,
    /// Optional stable public error code.
    pub error_code: Option<String>,
    /// Optional sanitized error detail.
    pub error_message: Option<String>,
    /// Terminal timestamp in Unix epoch milliseconds.
    pub terminal_at_ms: i64,
    /// Exact terminal outbox message.
    pub outbox: NewOutboxMessage,
}

/// Complete immutable facts staged when a job enters `PERSISTING`.
///
/// Keeping these values together prevents callers from advancing durable state without also
/// recording everything required to finish the exact success after a process crash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingInstall {
    /// Adapter-owned partial image path.
    pub partial_path: String,
    /// Final no-clobber installation path.
    pub final_path: String,
    /// SHA-256 expected from either the partial or already-installed image.
    pub expected_sha256: String,
    /// Exact encoded byte count expected from the image.
    pub expected_bytes: u64,
    /// Exact success result and outbox envelope to reuse during recovery.
    pub success: TerminalWrite,
    /// State-transition timestamp in Unix epoch milliseconds.
    pub changed_at_ms: i64,
}

/// Winner of terminal-state arbitration.
#[derive(Debug, Clone, PartialEq)]
pub enum TerminalOutcome {
    /// This caller won and committed exactly one outbox row.
    Won(JobRecord),
    /// Another terminal outcome had already won.
    AlreadyTerminal(JobRecord),
    /// Irrevocable file installation already won, so cancellation observed PERSISTING.
    InstallationWon(JobRecord),
}

/// Winner of the persistence-installation CAS.
#[derive(Debug, Clone, PartialEq)]
pub enum InstallOutcome {
    /// This caller changed `install_started` from false to true.
    Started(JobRecord),
    /// Installation had already started.
    AlreadyStarted(JobRecord),
    /// The job was no longer in PERSISTING.
    WrongState(JobRecord),
}

/// Durable command-ledger state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerState {
    /// Operation is in progress.
    InProgress,
    /// Operation completed successfully.
    Succeeded,
    /// Operation completed with a known failure.
    Failed,
    /// A crash made the physical side effect unknowable.
    OutcomeUnknown,
}

/// Stored command-ledger projection.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandLedgerRecord {
    /// Durable key.
    pub key: LedgerKey,
    /// Immutable request hash.
    pub request_hash: RequestHash,
    /// Original canonical request.
    pub canonical_request: Value,
    /// Operation state.
    pub state: LedgerState,
    /// Creation timestamp.
    pub created_at_ms: i64,
    /// Last-update timestamp.
    pub updated_at_ms: i64,
    /// Associated capture, when any.
    pub capture_id: Option<String>,
    /// Associated group, when any.
    pub group_id: Option<String>,
    /// Retained reply result/error body.
    pub reply: Option<Value>,
    /// Stable retained error code.
    pub error_code: Option<String>,
    /// Sanitized retained error message.
    pub error_message: Option<String>,
}

/// Result of starting any mutating command.
#[derive(Debug, Clone, PartialEq)]
pub enum BeginCommandOutcome {
    /// New IN_PROGRESS ledger row was committed.
    Started(CommandLedgerRecord),
    /// Exact duplicate found a retained row.
    Existing(CommandLedgerRecord),
    /// Same key was reused with different immutable arguments.
    Conflict,
}

/// Deferred waiter metadata. Transport routing material remains in the core runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaiterRecord {
    /// Opaque waiter identifier.
    pub waiter_id: String,
    /// Capture being observed.
    pub capture_id: String,
    /// Correlation identifier for this retrying waiter.
    pub correlation_id: String,
    /// Optional core request UUID.
    pub request_uuid: Option<String>,
    /// Expiry timestamp.
    pub expires_at_ms: i64,
    /// Creation timestamp.
    pub created_at_ms: i64,
}

/// Exact stored outbox record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRecord {
    /// The camera whose capture produced this message, when it had one.
    pub instance: Option<String>,
    /// SQLite row identifier.
    pub id: i64,
    /// Stable semantic event key.
    pub event_key: String,
    /// Associated capture.
    pub capture_id: Option<String>,
    /// Associated group.
    pub group_id: Option<String>,
    /// Message kind.
    pub message_kind: String,
    /// Destination topic.
    pub topic: String,
    /// Stable envelope UUID.
    pub envelope_uuid: String,
    /// Exact encoded envelope bytes.
    pub encoded_envelope: Vec<u8>,
    /// Creation timestamp.
    pub created_at_ms: i64,
    /// Earliest next delivery timestamp.
    pub available_at_ms: i64,
    /// Attempt count.
    pub attempts: i64,
    /// Last attempt timestamp.
    pub last_attempt_at_ms: Option<i64>,
    /// Delivery timestamp.
    pub delivered_at_ms: Option<i64>,
    /// Sanitized last delivery error.
    pub last_error: Option<String>,
}

/// Outbox pressure counters used for health and metrics without deleting work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxStats {
    /// Number of undelivered records.
    pub undelivered: u64,
    /// Age of the oldest undelivered record, clamped to zero.
    pub oldest_age_ms: u64,
    /// Largest attempt count among undelivered records.
    pub max_attempts: u64,
}

/// Verified connection and schema state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogHealth {
    /// Current monotonic schema version.
    pub schema_version: i64,
    /// Whether foreign-key enforcement is active on the serving connection.
    pub foreign_keys: bool,
    /// Active journal mode.
    pub journal_mode: String,
    /// Active synchronous setting.
    pub synchronous: i64,
    /// Busy timeout in milliseconds.
    pub busy_timeout_ms: i64,
    /// Integrity-check result.
    pub integrity_ok: bool,
}

/// Whether the catalog worker pool completed the latest SQLite operation successfully.
///
/// This is an operational availability signal, not a record-count or output-filesystem signal.
/// It becomes unavailable when a catalog worker cannot complete a read or write and recovers only
/// after a later SQLite operation succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogAvailability {
    /// Whether the durable state catalog is currently completing operations.
    pub state_capacity_available: bool,
    /// Whether the last proven durable-state failure was SQLite's disk-full condition.
    pub disk_full: bool,
}

impl Default for CatalogAvailability {
    fn default() -> Self {
        Self {
            state_capacity_available: true,
            disk_full: false,
        }
    }
}

/// Durable catalog handle. Clones share the same bounded worker pool and state-directory lock.
#[derive(Clone)]
pub struct Catalog {
    inner: Arc<CatalogInner>,
}

struct CatalogInner {
    sender: mpsc::Sender<Work>,
    availability_tx: watch::Sender<CatalogAvailability>,
    database_path: PathBuf,
    _state_directory: Arc<StateDirectoryGuard>,
    _state_lock: Arc<StateLock>,
    _workers: Vec<thread::JoinHandle<()>>,
}

/// Keeps the validated state directory stable for the lifetime of the catalog.
///
/// On Windows the final directory handle denies delete sharing. Together with the protected DACL
/// applied during creation this prevents a post-validation rename/reparse substitution of that
/// adapter-owned state directory before SQLite's path-based VFS opens its files.
struct StateDirectoryGuard {
    #[cfg(windows)]
    directory: cap_std::fs::Dir,
}

impl StateDirectoryGuard {
    #[cfg(not(windows))]
    const fn new() -> Self {
        Self {}
    }

    #[cfg(windows)]
    fn validate_existing_entries(&self) -> Result<()> {
        use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
        use cap_std::fs::OpenOptions;

        // SQLite's VFS still takes paths, rather than capability handles. Validate every entry it
        // can open before any raw-path operation. The retained directory handle and protected DACL
        // then prevent an untrusted principal from substituting one after this check.
        for name in [
            DATABASE_NAME,
            LOCK_NAME,
            "camera-adapter.sqlite3-wal",
            "camera-adapter.sqlite3-shm",
            "camera-adapter.sqlite3-journal",
        ] {
            let mut options = OpenOptions::new();
            options.read(true).follow(FollowSymlinks::No);
            match self.directory.open_with(name, &options) {
                Ok(file) => {
                    if !file.metadata()?.is_file() {
                        return Err(CameraError::Catalog(format!(
                            "state entry '{name}' is not a regular file"
                        )));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(CameraError::Catalog(format!(
                        "state entry '{name}' is a reparse point or cannot be safely opened: {error}"
                    )));
                }
            }
        }
        Ok(())
    }
}

struct StateLock {
    _file: File,
}

impl Catalog {
    /// Opens, verifies, and migrates the catalog without blocking a Tokio worker thread.
    pub async fn open(options: CatalogOptions) -> Result<Self> {
        validate_options(&options)?;
        let (sender, receiver) = oneshot::channel();
        thread::Builder::new()
            .name("camera-catalog-open".to_owned())
            .spawn(move || {
                let _ = sender.send(open_blocking(options));
            })
            .map_err(|error| {
                CameraError::Catalog(format!("failed to start catalog opener: {error}"))
            })?;
        receiver.await.map_err(|_| {
            CameraError::Catalog("catalog opener exited without a result".to_owned())
        })?
    }

    /// Returns the SQLite path for diagnostics and backup tooling.
    #[must_use]
    pub fn database_path(&self) -> &Path {
        &self.inner.database_path
    }

    /// Subscribes to durable-state worker availability changes.
    #[must_use]
    pub fn availability(&self) -> watch::Receiver<CatalogAvailability> {
        self.inner.availability_tx.subscribe()
    }

    async fn execute<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let work = Box::new(move |connection: &mut Connection| {
            let _ = sender.send(operation(connection));
        });
        if self.inner.sender.send(work).await.is_err() {
            self.set_availability(false, false);
            return Err(CameraError::Catalog(
                "catalog worker pool is closed".to_owned(),
            ));
        }
        match receiver.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                if let Some(disk_full) = durable_state_failure(&error) {
                    self.set_availability(false, disk_full);
                }
                Err(error)
            }
            Err(_) => {
                self.set_availability(false, false);
                Err(CameraError::Catalog(
                    "catalog worker exited before replying".to_owned(),
                ))
            }
        }
    }

    /// Performs a bounded, committed write solely to verify recovery of durable state storage.
    ///
    /// Ordinary successful reads never clear an unavailable catalog condition: they do not prove
    /// that SQLite can acquire a write transaction and commit it durably.
    pub async fn probe_commit(&self) -> Result<()> {
        let result = self
            .execute(|connection| {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let changed = transaction.execute(
                    "UPDATE catalog_availability_probe SET generation = generation + 1 WHERE singleton = 1",
                    [],
                )?;
                if changed != 1 {
                    return Err(CameraError::Catalog(
                        "catalog availability probe row is missing".to_owned(),
                    ));
                }
                transaction.commit()?;
                Ok(())
            })
            .await;
        if result.is_ok() {
            self.set_availability(true, false);
        }
        result
    }

    fn set_availability(&self, state_capacity_available: bool, disk_full: bool) {
        self.inner.availability_tx.send_if_modified(|availability| {
            let next = CatalogAvailability {
                state_capacity_available,
                disk_full,
            };
            if *availability == next {
                false
            } else {
                *availability = next;
                true
            }
        });
    }

    /// Returns the currently verified SQLite settings.
    pub async fn health(&self) -> Result<CatalogHealth> {
        self.execute(|connection| read_health(connection)).await
    }

    /// Atomically inserts a command ledger row and one ACCEPTED job.
    pub async fn accept_job(&self, job: NewJob) -> Result<AcceptJobOutcome> {
        validate_new_job(&job, false)?;
        if job.ledger_key.is_none() {
            return Err(CameraError::Catalog(
                "command jobs require a ledger key; use accept_scheduled_job for schedules"
                    .to_owned(),
            ));
        }
        self.execute(move |connection| accept_job_blocking(connection, &job))
            .await
    }

    /// Atomically inserts a scheduled-occurrence dedupe key and its ACCEPTED job.
    ///
    /// An exact duplicate occurrence returns the original job even if configuration changed after
    /// the first acceptance. This closes the crash window that would exist between separate job and
    /// occurrence commits.
    pub async fn accept_scheduled_job(
        &self,
        job: NewJob,
        schedule_id: impl Into<String>,
        intended_fire_time_ms: i64,
    ) -> Result<AcceptJobOutcome> {
        validate_new_job(&job, false)?;
        if job.ledger_key.is_some() {
            return Err(CameraError::Catalog(
                "scheduled jobs must use the schedule occurrence key, not a command ledger"
                    .to_owned(),
            ));
        }
        let schedule_id = schedule_id.into();
        require_nonempty("schedule_id", &schedule_id)?;
        self.execute(move |connection| {
            accept_scheduled_job_blocking(connection, &job, &schedule_id, intended_fire_time_ms)
        })
        .await
    }

    /// Returns one group schedule's recovery cursor: how far it has consumed, and its last group.
    ///
    /// This is a *hint*, not the authority. Exactly-once admission is owned by the command ledger:
    /// a scheduled group is submitted under a request id derived from the schedule and the intended
    /// fire time, so re-submitting an occurrence returns the group already accepted for it. A crash
    /// between submitting and recording the cursor therefore costs a redundant submission, not a
    /// duplicate group.
    ///
    /// What the cursor genuinely owns is the misfire window: without it, a restart cannot tell an
    /// occurrence it missed from one that never came due.
    pub async fn group_schedule_cursor(
        &self,
        schedule_id: impl Into<String>,
    ) -> Result<Option<GroupScheduleCursor>> {
        let schedule_id = schedule_id.into();
        require_nonempty("schedule_id", &schedule_id)?;
        self.execute(move |connection| {
            connection
                .query_row(
                    "SELECT intended_fire_time_ms, last_group_id FROM group_schedule_cursors \
                     WHERE schedule_id=?1",
                    params![schedule_id],
                    |row| {
                        Ok(GroupScheduleCursor {
                            intended_fire_time_ms: row.get(0)?,
                            last_group_id: row.get(1)?,
                        })
                    },
                )
                .optional()
                .map_err(CameraError::from)
        })
        .await
    }

    /// Records a consumed group-schedule occurrence.
    ///
    /// `group_id` is `Some` only when the occurrence was admitted. A skipped occurrence still moves
    /// the cursor -- it was consumed -- but must not forget the group it skipped *for*, or the very
    /// next tick would see no overlap and admit the occurrence it just declined.
    pub async fn record_group_schedule_occurrence(
        &self,
        schedule_id: impl Into<String>,
        intended_fire_time_ms: i64,
        group_id: Option<String>,
        now_ms: i64,
    ) -> Result<()> {
        let schedule_id = schedule_id.into();
        require_nonempty("schedule_id", &schedule_id)?;
        self.execute(move |connection| {
            connection.execute(
                "INSERT INTO group_schedule_cursors\
                 (schedule_id,intended_fire_time_ms,last_group_id,updated_at_ms) \
                 VALUES(?1,?2,?3,?4) \
                 ON CONFLICT(schedule_id) DO UPDATE SET \
                   intended_fire_time_ms=excluded.intended_fire_time_ms, \
                   last_group_id=COALESCE(excluded.last_group_id,last_group_id), \
                   updated_at_ms=excluded.updated_at_ms",
                params![schedule_id, intended_fire_time_ms, group_id, now_ms],
            )?;
            Ok(())
        })
        .await
    }

    /// Returns the latest unjittered intended-fire instant durably consumed for one schedule.
    ///
    /// The value is the scheduler's recovery cursor, not a wall-clock observation.  Keeping it
    /// in the same table that deduplicates acceptance means a restart cannot re-admit an already
    /// accepted occurrence or silently skip a coalescible outage window.
    pub async fn latest_schedule_occurrence(
        &self,
        instance: impl Into<String>,
        schedule_id: impl Into<String>,
    ) -> Result<Option<i64>> {
        let instance = instance.into();
        let schedule_id = schedule_id.into();
        require_nonempty("instance", &instance)?;
        require_nonempty("schedule_id", &schedule_id)?;
        self.execute(move |connection| {
            connection
                .query_row(
                    "SELECT intended_fire_time_ms FROM schedule_occurrences \
                     WHERE instance=?1 AND schedule_id=?2 \
                     ORDER BY intended_fire_time_ms DESC LIMIT 1",
                    params![instance, schedule_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(CameraError::from)
        })
        .await
    }

    /// Performs the second durable acceptance phase, making one ACCEPTED job QUEUED.
    /// Rebases a QUEUED capture's deadlines onto the moment it is admitted to a camera.
    ///
    /// Deadlines were write-once acceptance facts, and that is precisely what broke oversized
    /// groups. A member's 30-second capture clock started when the GROUP was accepted, so anything
    /// the component could not dispatch immediately spent its whole budget sitting in a queue and
    /// then died of CAPTURE_TIMEOUT the moment a camera was free to serve it. Sequencing work into
    /// waves is pointless if the later waves are already dead on arrival.
    ///
    /// So a capture's stage clocks now start when a camera actually takes it. How long it may WAIT
    /// is a separate bound (`queue_at_ms`, from `profile.queueExpiryMs`) and is deliberately not
    /// rebased -- otherwise a starved capture would wait forever, one rebase at a time.
    ///
    /// Only a QUEUED row may be rebased. A capture that has begun acquiring owns its clocks, and a
    /// terminal one is finished; moving either one's deadline would be rewriting history.
    pub async fn reschedule_deadlines(
        &self,
        capture_id: impl Into<String>,
        deadlines: JobDeadlines,
        rebased_at_ms: i64,
    ) -> Result<JobRecord> {
        let capture_id = capture_id.into();
        require_nonempty("capture_id", &capture_id)?;
        if deadlines.capture_at_ms > deadlines.terminal_at_ms
            || deadlines.encode_at_ms > deadlines.terminal_at_ms
            || deadlines.persist_at_ms > deadlines.terminal_at_ms
            || deadlines
                .queue_at_ms
                .is_some_and(|deadline| deadline > deadlines.terminal_at_ms)
        {
            return Err(CameraError::Catalog(
                "stage deadlines must not exceed the terminal deadline".to_owned(),
            ));
        }
        self.execute(move |connection| {
            let transaction = connection.transaction()?;
            let state: String = transaction
                .query_row(
                    "SELECT state FROM jobs WHERE capture_id=?1",
                    rusqlite::params![capture_id],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| {
                    CameraError::rejected(
                        crate::ErrorCode::CaptureNotFound,
                        "capture was not found",
                    )
                })?;
            if parse_job_state(&state)? != JobState::Queued {
                return Err(CameraError::Catalog(format!(
                    "only a QUEUED capture may be rebased onto its admission; found {state}"
                )));
            }
            transaction.execute(
                "UPDATE jobs SET terminal_deadline_ms=?2,queue_deadline_ms=?3,capture_deadline_ms=?4,                 encode_deadline_ms=?5,persist_deadline_ms=?6,updated_at_ms=?7 WHERE capture_id=?1",
                rusqlite::params![
                    capture_id,
                    deadlines.terminal_at_ms,
                    deadlines.queue_at_ms,
                    deadlines.capture_at_ms,
                    deadlines.encode_at_ms,
                    deadlines.persist_at_ms,
                    rebased_at_ms,
                ],
            )?;
            let record = load_job(&transaction, &capture_id)?.ok_or_else(|| {
                CameraError::Catalog("capture disappeared while being rebased".to_owned())
            })?;
            transaction.commit()?;
            Ok(record)
        })
        .await
    }

    /// Performs the second durable acceptance phase, making one ACCEPTED job QUEUED.
    pub async fn queue_job(
        &self,
        capture_id: impl Into<String>,
        queued_at_ms: i64,
    ) -> Result<StateCasOutcome> {
        let capture_id = capture_id.into();
        require_nonempty("capture_id", &capture_id)?;
        self.execute(move |connection| {
            cas_state_blocking(
                connection,
                &capture_id,
                JobState::Accepted,
                JobState::Queued,
                queued_at_ms,
            )
        })
        .await
    }

    /// Atomically inserts the group ledger, group row, and all ACCEPTED member jobs.
    pub async fn accept_group(&self, group: NewGroup) -> Result<AcceptGroupOutcome> {
        validate_new_group(&group)?;
        self.execute(move |connection| accept_group_blocking(connection, &group))
            .await
    }

    /// Performs the second group phase: every member and the group become QUEUED in one transaction.
    pub async fn queue_group(
        &self,
        group_id: impl Into<String>,
        queued_at_ms: i64,
    ) -> Result<GroupRecord> {
        let group_id = group_id.into();
        require_nonempty("group_id", &group_id)?;
        self.execute(move |connection| queue_group_blocking(connection, &group_id, queued_at_ms))
            .await
    }

    /// Commits the aggregate group result after every member has reached a terminal state.
    ///
    /// Member application messages remain individual job outbox records. This transaction retains
    /// the group command reply in both the group row and its idempotency ledger.
    pub async fn complete_group(
        &self,
        group_id: impl Into<String>,
        state: JobState,
        result: Value,
        error_code: Option<String>,
        error_message: Option<String>,
        terminal_at_ms: i64,
    ) -> Result<GroupRecord> {
        if !state.is_terminal() {
            return Err(CameraError::Catalog(
                "group completion requires a terminal state".to_owned(),
            ));
        }
        let group_id = group_id.into();
        self.execute(move |connection| {
            complete_group_blocking(
                connection,
                &group_id,
                state,
                &result,
                error_code.as_deref(),
                error_message.as_deref(),
                terminal_at_ms,
            )
        })
        .await
    }

    /// Reads a job by durable identifier.
    pub async fn job(&self, capture_id: impl Into<String>) -> Result<Option<JobRecord>> {
        let capture_id = capture_id.into();
        self.execute(move |connection| load_job(connection, &capture_id))
            .await
    }

    /// Reads one complete capture group, including member jobs in original request order.
    pub async fn group(&self, group_id: impl Into<String>) -> Result<Option<GroupRecord>> {
        let group_id = group_id.into();
        self.execute(move |connection| load_group(connection, &group_id))
            .await
    }

    /// Resolves a camera-scoped idempotency key to its capture, if it still exists.
    ///
    /// The lookup is deliberately verb-specific. Callers must never search every camera's
    /// request IDs because the idempotency namespace is `(instance, verb, requestId)`.
    pub async fn job_by_ledger(&self, key: LedgerKey) -> Result<Option<JobRecord>> {
        self.execute(move |connection| {
            let Some(ledger) = load_ledger(connection, &key)? else {
                return Ok(None);
            };
            match ledger.capture_id {
                Some(capture_id) => load_job(connection, &capture_id),
                None => Ok(None),
            }
        })
        .await
    }

    /// Resolves a component-scoped group idempotency key to its durable group, if retained.
    pub async fn group_by_ledger(&self, key: LedgerKey) -> Result<Option<GroupRecord>> {
        self.execute(move |connection| {
            let Some(ledger) = load_ledger(connection, &key)? else {
                return Ok(None);
            };
            match ledger.group_id {
                Some(group_id) => load_group(connection, &group_id),
                None => Ok(None),
            }
        })
        .await
    }

    /// Counts durable jobs per state, optionally for one camera.
    ///
    /// The backlog an operator cares about is the durable one -- the rows that will still be there
    /// after a restart -- and it is the one thing an in-memory queue depth cannot tell them. Counting
    /// in SQL rather than paging rows keeps a break-glass question from becoming a scan of the whole
    /// catalog at exactly the moment the catalog is already the thing under strain.
    pub async fn count_jobs_by_state(
        &self,
        instance: Option<String>,
        states: Vec<JobState>,
    ) -> Result<std::collections::BTreeMap<String, u64>> {
        self.execute(move |connection| {
            let mut sql = "SELECT state,COUNT(*) FROM jobs WHERE 1=1".to_owned();
            let mut parameters = Vec::<rusqlite::types::Value>::new();
            if let Some(instance) = instance {
                sql.push_str(" AND instance=?");
                parameters.push(instance.into());
            }
            if !states.is_empty() {
                sql.push_str(" AND state IN (");
                for (index, state) in states.iter().enumerate() {
                    if index != 0 {
                        sql.push(',');
                    }
                    sql.push('?');
                    parameters.push(job_state_token(*state).to_owned().into());
                }
                sql.push(')');
            }
            sql.push_str(" GROUP BY state");
            let mut statement = connection.prepare(&sql)?;
            let counted = statement
                .query_map(rusqlite::params_from_iter(parameters), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut totals = std::collections::BTreeMap::new();
            for (token, count) in counted {
                // Parsed to reject a durable row whose state token is not one we know, then keyed by
                // the token itself: the caller is answering an operator's question over the wire, and
                // the durable token IS the name that question is asked in.
                parse_job_state(&token)?;
                totals.insert(token, u64::try_from(count).unwrap_or(0));
            }
            Ok(totals)
        })
        .await
    }

    /// Returns one stable, bounded page of jobs in descending `(acceptedAt,captureId)` order.
    ///
    /// `before` is the final tuple returned by the previous page. It is intentionally a typed
    /// cursor seam: the command layer binds it to its complete query before serializing its opaque
    /// continuation token, so a cursor cannot be replayed with different filters.
    pub async fn jobs_page(
        &self,
        instance: Option<String>,
        states: Vec<JobState>,
        before: Option<(i64, String)>,
        limit: usize,
    ) -> Result<Vec<JobRecord>> {
        if !(1..=1_000).contains(&limit) {
            return Err(CameraError::Catalog(
                "job page limit must be between 1 and 1000".to_owned(),
            ));
        }
        self.execute(move |connection| {
            let mut sql = format!("{JOB_SELECT} WHERE 1=1");
            let mut parameters = Vec::<rusqlite::types::Value>::new();
            if let Some(instance) = instance {
                sql.push_str(" AND instance=?");
                parameters.push(instance.into());
            }
            if !states.is_empty() {
                sql.push_str(" AND state IN (");
                for (index, state) in states.iter().enumerate() {
                    if index != 0 {
                        sql.push(',');
                    }
                    sql.push('?');
                    parameters.push(job_state_token(*state).to_owned().into());
                }
                sql.push(')');
            }
            if let Some((accepted_at_ms, capture_id)) = before {
                sql.push_str(
                    " AND (accepted_at_ms < ? OR (accepted_at_ms = ? AND capture_id < ?))",
                );
                parameters.push(accepted_at_ms.into());
                parameters.push(accepted_at_ms.into());
                parameters.push(capture_id.into());
            }
            sql.push_str(" ORDER BY accepted_at_ms DESC,capture_id DESC LIMIT ?");
            parameters.push(
                i64::try_from(limit)
                    .map_err(|_| {
                        CameraError::Catalog("job page limit conversion overflowed".to_owned())
                    })?
                    .into(),
            );
            let mut statement = connection.prepare(&sql)?;
            statement
                .query_map(rusqlite::params_from_iter(parameters), raw_job_from_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
                .into_iter()
                .map(RawJob::into_record)
                .collect()
        })
        .await
    }

    /// Advances a non-terminal job with an allowed compare-and-set transition.
    pub async fn compare_and_set_state(
        &self,
        capture_id: impl Into<String>,
        expected: JobState,
        next: JobState,
        changed_at_ms: i64,
    ) -> Result<StateCasOutcome> {
        if expected.is_terminal()
            || next.is_terminal()
            || !expected.can_transition_to(next)
            || next == JobState::Persisting
        {
            return Err(CameraError::Catalog(format!(
                "invalid ordinary transition {expected:?} -> {next:?}; PERSISTING requires begin_persisting"
            )));
        }
        let capture_id = capture_id.into();
        self.execute(move |connection| {
            cas_state_blocking(connection, &capture_id, expected, next, changed_at_ms)
        })
        .await
    }

    /// Atomically enters PERSISTING while staging every fact needed for exact crash recovery.
    pub async fn begin_persisting(
        &self,
        capture_id: impl Into<String>,
        pending: PendingInstall,
    ) -> Result<StateCasOutcome> {
        let capture_id = capture_id.into();
        let PendingInstall {
            partial_path,
            final_path,
            expected_sha256,
            expected_bytes,
            success: pending_success,
            changed_at_ms,
        } = pending;
        require_nonempty("partial_path", &partial_path)?;
        require_nonempty("final_path", &final_path)?;
        if partial_path == final_path {
            return Err(CameraError::Catalog(
                "partial and final persistence paths must differ".to_owned(),
            ));
        }
        validate_sha256("expected sha256", &expected_sha256)?;
        let expected_bytes = i64::try_from(expected_bytes).map_err(|_| {
            CameraError::Catalog("expected byte count exceeds SQLite INTEGER".to_owned())
        })?;
        if pending_success.state != JobState::Succeeded {
            return Err(CameraError::Catalog(
                "pending persistence terminal write must be SUCCEEDED".to_owned(),
            ));
        }
        validate_terminal_write(&pending_success)?;
        self.execute(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let before = load_job_required(&transaction, &capture_id)?;
            validate_terminal_for_job(&pending_success, &before)?;
            let changed = transaction.execute(
                "UPDATE jobs SET state='PERSISTING',updated_at_ms=?6,partial_path=?2,final_path=?3, \
                 expected_sha256=?4,expected_bytes=?5 \
                 WHERE capture_id=?1 AND state IN ('ACQUIRING','ENCODING')",
                params![
                    capture_id,
                    partial_path,
                    final_path,
                    expected_sha256,
                    expected_bytes,
                    changed_at_ms
                ],
            )?;
            if changed == 1 {
                insert_pending_success(&transaction, &capture_id, &pending_success)?;
            }
            let record = load_job_required(&transaction, &capture_id)?;
            transaction.commit()?;
            Ok(if changed == 1 {
                StateCasOutcome::Changed(record)
            } else {
                StateCasOutcome::NotChanged(record)
            })
        })
        .await
    }

    /// Records verified installed-artifact facts after installation has won and before terminal success.
    pub async fn record_installed_artifact(
        &self,
        capture_id: impl Into<String>,
        sha256: impl Into<String>,
        bytes: u64,
        changed_at_ms: i64,
    ) -> Result<JobRecord> {
        let capture_id = capture_id.into();
        let sha256 = sha256.into();
        validate_sha256("installed sha256", &sha256)?;
        let bytes = i64::try_from(bytes).map_err(|_| {
            CameraError::Catalog("installed byte count exceeds SQLite INTEGER".to_owned())
        })?;
        self.execute(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let before = load_job_required(&transaction, &capture_id)?;
            if before.expected_sha256.as_deref() != Some(sha256.as_str())
                || before.expected_bytes != Some(bytes as u64)
                || before.pending_success.is_none()
            {
                return Err(CameraError::Catalog(
                    "installed artifact does not match the durably prepared recovery facts"
                        .to_owned(),
                ));
            }
            let changed = transaction.execute(
                "UPDATE jobs SET installed_sha256=?2,installed_bytes=?3,updated_at_ms=?4 \
                 WHERE capture_id=?1 AND state='PERSISTING' AND install_started=1",
                params![capture_id, sha256, bytes, changed_at_ms],
            )?;
            let record = load_job_required(&transaction, &capture_id)?;
            if changed != 1 {
                return Err(CameraError::Catalog(
                    "installed artifact can be recorded only after installation wins in PERSISTING"
                        .to_owned(),
                ));
            }
            transaction.commit()?;
            Ok(record)
        })
        .await
    }

    /// Starts a generic mutating operation after durable idempotency arbitration.
    pub async fn begin_command(
        &self,
        key: LedgerKey,
        request_hash: RequestHash,
        canonical_request: Value,
        created_at_ms: i64,
    ) -> Result<BeginCommandOutcome> {
        validate_ledger_key(&key)?;
        require_object("canonical_request", &canonical_request)?;
        self.execute(move |connection| {
            begin_command_blocking(
                connection,
                &key,
                request_hash,
                &canonical_request,
                created_at_ms,
            )
        })
        .await
    }

    /// Persists the exact immediate acceptance reply while the physical command remains in
    /// progress.  This lets an idempotent retry return the original operation identifier without
    /// replaying a reconnect or other hazardous side effect.
    pub async fn record_command_acceptance(
        &self,
        key: LedgerKey,
        reply: Value,
        updated_at_ms: i64,
    ) -> Result<CommandLedgerRecord> {
        validate_ledger_key(&key)?;
        self.execute(move |connection| {
            record_command_acceptance_blocking(connection, &key, &reply, updated_at_ms)
        })
        .await
    }

    /// Completes a generic command with retained result/error material.
    pub async fn complete_command(
        &self,
        key: LedgerKey,
        state: LedgerState,
        reply: Value,
        error_code: Option<String>,
        error_message: Option<String>,
        updated_at_ms: i64,
    ) -> Result<CommandLedgerRecord> {
        if !matches!(state, LedgerState::Succeeded | LedgerState::Failed) {
            return Err(CameraError::Catalog(
                "command completion must be SUCCEEDED or FAILED".to_owned(),
            ));
        }
        validate_ledger_key(&key)?;
        self.execute(move |connection| {
            complete_command_blocking(
                connection,
                &key,
                state,
                &reply,
                error_code.as_deref(),
                error_message.as_deref(),
                updated_at_ms,
            )
        })
        .await
    }

    /// Settles every reconnect ledger left unresolved by a crash, so recovery never fences one as
    /// hazardous.
    ///
    /// A restart re-establishes every camera session by definition, which is exactly what the
    /// reconnect request asked for; the operation performs no physical actuation that could have
    /// half-happened. Settling the row keeps it reclaimable by
    /// [`Self::prune_completed_command_ledgers`] — an `IN_PROGRESS` or `OUTCOME_UNKNOWN` row
    /// matches no DELETE in this catalog and would live forever — and stops a retried reconnect
    /// from answering `PREVIOUS_OUTCOME_UNKNOWN` permanently.  `OUTCOME_UNKNOWN` rows written by
    /// an earlier build are settled here too.
    pub async fn settle_interrupted_reconnects(&self, updated_at_ms: i64) -> Result<u64> {
        self.execute(move |connection| {
            let changed = connection.execute(
                "UPDATE command_ledger SET operation_state='SUCCEEDED',updated_at_ms=?1 \
                 WHERE verb=?2 AND capture_id IS NULL AND group_id IS NULL \
                 AND operation_state IN ('IN_PROGRESS','OUTCOME_UNKNOWN')",
                params![updated_at_ms, RECONNECT_VERB],
            )?;
            Ok(changed as u64)
        })
        .await
    }

    /// Marks standalone in-progress commands OUTCOME_UNKNOWN after a crash.
    ///
    /// Capture/group ledgers are excluded because job recovery owns their known outcome.
    pub async fn mark_hazardous_commands_outcome_unknown(&self, updated_at_ms: i64) -> Result<u64> {
        self.execute(move |connection| {
            let changed = connection.execute(
                "UPDATE command_ledger SET operation_state='OUTCOME_UNKNOWN', updated_at_ms=?1 \
                 WHERE operation_state='IN_PROGRESS' AND capture_id IS NULL AND group_id IS NULL",
                params![updated_at_ms],
            )?;
            Ok(changed as u64)
        })
        .await
    }

    /// Adds durable waiter metadata without changing the creator's origin correlation.
    pub async fn add_waiter(&self, waiter: WaiterRecord) -> Result<bool> {
        validate_waiter(&waiter)?;
        self.execute(move |connection| {
            let changed = connection.execute(
                "INSERT OR IGNORE INTO deferred_waiters \
                 (waiter_id,capture_id,correlation_id,request_uuid,expires_at_ms,created_at_ms) \
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    waiter.waiter_id,
                    waiter.capture_id,
                    waiter.correlation_id,
                    waiter.request_uuid,
                    waiter.expires_at_ms,
                    waiter.created_at_ms
                ],
            )?;
            Ok(changed == 1)
        })
        .await
    }

    /// Lists unexpired waiter metadata for a capture.
    pub async fn waiters(
        &self,
        capture_id: impl Into<String>,
        now_ms: i64,
    ) -> Result<Vec<WaiterRecord>> {
        let capture_id = capture_id.into();
        self.execute(move |connection| {
            let mut statement = connection.prepare(
                "SELECT waiter_id,capture_id,correlation_id,request_uuid,expires_at_ms,created_at_ms \
                 FROM deferred_waiters WHERE capture_id=?1 AND expires_at_ms>?2 ORDER BY created_at_ms,waiter_id",
            )?;
            let records = statement
                .query_map(params![capture_id, now_ms], waiter_from_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(records)
        })
        .await
    }

    /// Removes one waiter after a terminal deferred reply or expiry.
    pub async fn remove_waiter(&self, waiter_id: impl Into<String>) -> Result<bool> {
        let waiter_id = waiter_id.into();
        self.execute(move |connection| {
            Ok(connection.execute(
                "DELETE FROM deferred_waiters WHERE waiter_id=?1",
                params![waiter_id],
            )? == 1)
        })
        .await
    }

    /// Attempts the irrevocable persistence-installation CAS while the job is PERSISTING.
    pub async fn try_begin_install(
        &self,
        capture_id: impl Into<String>,
        changed_at_ms: i64,
    ) -> Result<InstallOutcome> {
        let capture_id = capture_id.into();
        self.execute(move |connection| {
            begin_install_blocking(connection, &capture_id, changed_at_ms)
        })
        .await
    }

    /// Commits a non-cancellation terminal state and exactly one terminal outbox row.
    pub async fn commit_terminal(
        &self,
        capture_id: impl Into<String>,
        write: TerminalWrite,
    ) -> Result<TerminalOutcome> {
        if write.state == JobState::Cancelled {
            return Err(CameraError::Catalog(
                "use cancel_job for cancellation arbitration".to_owned(),
            ));
        }
        validate_terminal_write(&write)?;
        let capture_id = capture_id.into();
        self.execute(move |connection| {
            terminal_blocking(connection, &capture_id, &write, false, None)
        })
        .await
    }

    /// Lets cancellation race installation transactionally; the winner is authoritative.
    pub async fn cancel_job(
        &self,
        capture_id: impl Into<String>,
        write: TerminalWrite,
    ) -> Result<TerminalOutcome> {
        if write.state != JobState::Cancelled {
            return Err(CameraError::Catalog(
                "cancel_job requires the CANCELLED terminal state".to_owned(),
            ));
        }
        validate_terminal_write(&write)?;
        let capture_id = capture_id.into();
        self.execute(move |connection| {
            terminal_blocking(connection, &capture_id, &write, true, None)
        })
        .await
    }

    /// Returns non-terminal records requiring startup reconciliation.
    ///
    /// This method deliberately returns stored material only. Recovery must build an exact terminal
    /// envelope elsewhere and pass it to [`Catalog::interrupt_recovered`].
    pub async fn recovery_jobs(&self) -> Result<Vec<JobRecord>> {
        self.execute(move |connection| {
            let mut statement = connection.prepare(&format!(
                "{} WHERE state IN ('ACCEPTED','QUEUED','ACQUIRING','ENCODING','PERSISTING') ORDER BY accepted_at_ms,capture_id",
                JOB_SELECT
            ))?;
            let raw = statement.query_map([], raw_job_from_row)?.collect::<std::result::Result<Vec<_>, _>>()?;
            raw.into_iter().map(RawJob::into_record).collect()
        })
        .await
    }

    /// Commits recovery interruption only if the job is still in one of the supplied crash states.
    /// Exact envelope bytes must be supplied by the messaging layer; the catalog never fabricates them.
    pub async fn interrupt_recovered(
        &self,
        capture_id: impl Into<String>,
        expected_states: Vec<JobState>,
        write: TerminalWrite,
    ) -> Result<TerminalOutcome> {
        if write.state != JobState::Interrupted
            || expected_states.is_empty()
            || expected_states.iter().any(|state| state.is_terminal())
        {
            return Err(CameraError::Catalog(
                "recovery interruption requires INTERRUPTED and non-terminal expected states"
                    .to_owned(),
            ));
        }
        validate_terminal_write(&write)?;
        let capture_id = capture_id.into();
        self.execute(move |connection| {
            terminal_blocking(
                connection,
                &capture_id,
                &write,
                false,
                Some(&expected_states),
            )
        })
        .await
    }

    /// Returns deliverable undelivered outbox records in stable order.
    pub async fn pending_outbox(&self, now_ms: i64, limit: usize) -> Result<Vec<OutboxRecord>> {
        if !(1..=1_000).contains(&limit) {
            return Err(CameraError::Catalog(
                "outbox limit must be between 1 and 1000".to_owned(),
            ));
        }
        self.execute(move |connection| {
            let mut statement = connection.prepare(
                // The camera is joined through rather than stored again: a terminal job is retained
                // until its own messages are gone, so the row is always there while the message is
                // pending. The alternative -- a column and a migration -- stores what the catalog
                // already knows, and reading it per delivery would add work to the outbox, which is
                // a single serial worker with no headroom to give.
                "SELECT o.id,o.event_key,o.capture_id,o.group_id,o.message_kind,o.topic,\
                 o.envelope_uuid,o.encoded_envelope,o.created_at_ms,o.available_at_ms,o.attempts,\
                 o.last_attempt_at_ms,o.delivered_at_ms,o.last_error,j.instance \
                 FROM outbox o LEFT JOIN jobs j ON j.capture_id=o.capture_id \
                 WHERE o.delivered_at_ms IS NULL AND o.available_at_ms<=?1 ORDER BY o.id LIMIT ?2",
            )?;
            let records = statement
                .query_map(params![now_ms, limit as i64], outbox_from_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(records)
        })
        .await
    }

    /// Records a failed attempt without mutating exact encoded envelope bytes.
    pub async fn record_outbox_attempt(
        &self,
        id: i64,
        attempted_at_ms: i64,
        next_available_at_ms: i64,
        last_error: impl Into<String>,
    ) -> Result<bool> {
        let last_error = bounded_error(last_error.into());
        self.execute(move |connection| {
            Ok(connection.execute(
                "UPDATE outbox SET attempts=attempts+1,last_attempt_at_ms=?2,available_at_ms=?3,last_error=?4 \
                 WHERE id=?1 AND delivered_at_ms IS NULL",
                params![id, attempted_at_ms, next_available_at_ms, last_error],
            )? == 1)
        })
        .await
    }

    /// Marks an outbox row delivered. Already-delivered/missing rows return `false`.
    pub async fn mark_outbox_delivered(&self, id: i64, delivered_at_ms: i64) -> Result<bool> {
        self.execute(move |connection| {
            Ok(connection.execute(
                "UPDATE outbox SET delivered_at_ms=?2,last_error=NULL WHERE id=?1 AND delivered_at_ms IS NULL",
                params![id, delivered_at_ms],
            )? == 1)
        })
        .await
    }

    /// Returns outbox pressure without deleting or rewriting records.
    pub async fn outbox_stats(&self, now_ms: i64) -> Result<OutboxStats> {
        self.execute(move |connection| {
            let (count, oldest, attempts): (i64, Option<i64>, Option<i64>) = connection.query_row(
                "SELECT COUNT(*),MIN(created_at_ms),MAX(attempts) FROM outbox WHERE delivered_at_ms IS NULL",
                [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            Ok(OutboxStats {
                undelivered: count.max(0) as u64,
                oldest_age_ms: oldest.map_or(0, |value| now_ms.saturating_sub(value).max(0) as u64),
                max_attempts: attempts.unwrap_or(0).max(0) as u64,
            })
        })
        .await
    }

    /// Prunes only delivered outbox rows older than the cutoff, up to a bounded batch size.
    pub async fn prune_delivered_outbox(
        &self,
        delivered_before_ms: i64,
        limit: usize,
    ) -> Result<u64> {
        if !(1..=10_000).contains(&limit) {
            return Err(CameraError::Catalog(
                "prune limit must be between 1 and 10000".to_owned(),
            ));
        }
        self.execute(move |connection| {
            let changed = connection.execute(
                "DELETE FROM outbox WHERE id IN (SELECT id FROM outbox WHERE delivered_at_ms IS NOT NULL \
                 AND delivered_at_ms<?1 ORDER BY delivered_at_ms,id LIMIT ?2)",
                params![delivered_before_ms, limit as i64],
            )?;
            Ok(changed as u64)
        })
        .await
    }

    /// Prunes terminal jobs only after all associated outbox rows have themselves been retained and removed.
    /// Non-terminal jobs, OUTCOME_UNKNOWN ledgers, and jobs with any outbox row are never selected.
    pub async fn prune_terminal_jobs(&self, terminal_before_ms: i64, limit: usize) -> Result<u64> {
        if !(1..=10_000).contains(&limit) {
            return Err(CameraError::Catalog(
                "prune limit must be between 1 and 10000".to_owned(),
            ));
        }
        self.execute(move |connection| {
            prune_terminal_jobs_blocking(connection, terminal_before_ms, limit)
        })
        .await
    }

    /// Prunes a terminal group, its member jobs, and its ledger only after every member outbox row
    /// has completed its own delivered-message retention.
    pub async fn prune_terminal_groups(
        &self,
        terminal_before_ms: i64,
        limit: usize,
    ) -> Result<u64> {
        if !(1..=10_000).contains(&limit) {
            return Err(CameraError::Catalog(
                "prune limit must be between 1 and 10000".to_owned(),
            ));
        }
        self.execute(move |connection| {
            prune_terminal_groups_blocking(connection, terminal_before_ms, limit)
        })
        .await
    }

    /// Prunes retained standalone command outcomes after their result-retention cutoff.
    /// IN_PROGRESS and OUTCOME_UNKNOWN operations are never eligible.
    pub async fn prune_completed_command_ledgers(
        &self,
        completed_before_ms: i64,
        limit: usize,
    ) -> Result<u64> {
        if !(1..=10_000).contains(&limit) {
            return Err(CameraError::Catalog(
                "prune limit must be between 1 and 10000".to_owned(),
            ));
        }
        self.execute(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let keys = {
                let mut statement = transaction.prepare(
                    "SELECT instance,verb,request_id FROM command_ledger \
                     WHERE capture_id IS NULL AND group_id IS NULL \
                     AND operation_state IN ('SUCCEEDED','FAILED') AND updated_at_ms<?1 \
                     ORDER BY updated_at_ms,instance,verb,request_id LIMIT ?2",
                )?;
                statement
                    .query_map(params![completed_before_ms, limit as i64], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?
            };
            for (instance, verb, request_id) in &keys {
                transaction.execute(
                    "DELETE FROM command_ledger WHERE instance=?1 AND verb=?2 AND request_id=?3 \
                     AND operation_state IN ('SUCCEEDED','FAILED')",
                    params![instance, verb, request_id],
                )?;
            }
            transaction.commit()?;
            Ok(keys.len() as u64)
        })
        .await
    }

    /// Enforces `maxResultRecords` by pruning the oldest eligible terminal jobs first.
    ///
    /// Jobs with any retained outbox row, non-terminal jobs, group members, and
    /// OUTCOME_UNKNOWN operations remain ineligible even while the maximum is exceeded.
    pub async fn enforce_result_record_limit(
        &self,
        max_result_records: u64,
        limit: usize,
    ) -> Result<u64> {
        if !(1..=10_000).contains(&limit) {
            return Err(CameraError::Catalog(
                "prune limit must be between 1 and 10000".to_owned(),
            ));
        }
        let max_result_records = i64::try_from(max_result_records).map_err(|_| {
            CameraError::Catalog("max_result_records exceeds SQLite INTEGER".to_owned())
        })?;
        self.execute(move |connection| {
            enforce_result_record_limit_blocking(connection, max_result_records, limit)
        })
        .await
    }
}

fn durable_state_failure(error: &CameraError) -> Option<bool> {
    match error {
        CameraError::Sqlite(error) => {
            Some(error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull))
        }
        CameraError::Io(_) => Some(false),
        _ => None,
    }
}

fn validate_options(options: &CatalogOptions) -> Result<()> {
    if options.state_directory.as_os_str().is_empty() {
        return Err(CameraError::Catalog(
            "state directory must not be empty".to_owned(),
        ));
    }
    if !(1..=MAX_WORKERS).contains(&options.worker_count) {
        return Err(CameraError::Catalog(format!(
            "worker_count must be between 1 and {MAX_WORKERS}"
        )));
    }
    if options.queue_capacity == 0 || options.queue_capacity > 65_536 {
        return Err(CameraError::Catalog(
            "queue_capacity must be between 1 and 65536".to_owned(),
        ));
    }
    Ok(())
}

/// Why a catalog file could not be opened.
///
/// The distinction is the whole point. **Corruption is recoverable and a downgrade is not** -- and
/// treating them alike in either direction destroys something. A database written by a NEWER component
/// is not damaged: its data is intact and the version that wrote it can still read every row. Deleting
/// it to work around a rollback would throw away good, unpublished results to solve a problem nobody
/// has. So a future schema version stays fail-closed, and only a file that is genuinely unreadable is
/// quarantined.
enum OpenFailure {
    /// The file is not a usable database: unreadable, or failing its own integrity check.
    Corrupt(String),
    /// Anything else, including a schema version from the future. Not something a restart can fix.
    Fatal(CameraError),
}

impl From<OpenFailure> for CameraError {
    fn from(failure: OpenFailure) -> Self {
        match failure {
            OpenFailure::Corrupt(reason) => Self::Catalog(reason),
            OpenFailure::Fatal(error) => error,
        }
    }
}

/// Whether a SQLite error means the bytes on disk are not a database we can use.
fn is_corruption(error: &CameraError) -> bool {
    let CameraError::Sqlite(rusqlite::Error::SqliteFailure(failure, _)) = error else {
        return false;
    };
    matches!(
        failure.code,
        rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
    )
}

/// Opens every worker connection, migrates, and verifies -- classifying the failure if it fails.
fn open_verified(
    database_path: &Path,
    worker_count: usize,
) -> std::result::Result<Vec<Connection>, OpenFailure> {
    let mut connections = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        match open_connection(database_path) {
            Ok(connection) => connections.push(connection),
            Err(error) if is_corruption(&error) => {
                return Err(OpenFailure::Corrupt(error.to_string()));
            }
            Err(error) => return Err(OpenFailure::Fatal(error)),
        }
    }
    match migrate(&mut connections[0]) {
        Ok(()) => {}
        // A migration that trips over damaged pages is corruption. A migration that refuses to
        // DOWNGRADE is not -- and that is the case this must not swallow.
        Err(error) if is_corruption(&error) => {
            return Err(OpenFailure::Corrupt(error.to_string()));
        }
        Err(error) => return Err(OpenFailure::Fatal(error)),
    }
    for connection in &connections {
        match verify_connection(connection) {
            Ok(()) => {}
            Err(error) if is_corruption(&error) => {
                return Err(OpenFailure::Corrupt(error.to_string()));
            }
            Err(CameraError::Catalog(reason)) if reason == INTEGRITY_CHECK_FAILED => {
                return Err(OpenFailure::Corrupt(reason));
            }
            Err(error) => return Err(OpenFailure::Fatal(error)),
        }
    }
    Ok(connections)
}

/// Moves a corrupt catalog aside so the component can start on a clean one.
///
/// The WAL and shared-memory sidecars move WITH it. Leaving them beside the new database would be
/// worse than useless: SQLite would try to recover the new file from the old file's write-ahead log.
///
/// One quarantine slot, overwritten each time. Evidence is worth keeping; evidence that accumulates
/// without bound on an edge box is just a second way to fill the disk, which is the failure this
/// component already has a whole retention subsystem to avoid.
fn quarantine_corrupt_catalog(database_path: &Path, reason: &str) -> Result<()> {
    let mut quarantine = database_path.as_os_str().to_owned();
    quarantine.push(".corrupt");
    let quarantine = PathBuf::from(quarantine);

    for (from, to) in [
        (database_path.to_path_buf(), quarantine.clone()),
        (sidecar(database_path, "-wal"), sidecar(&quarantine, "-wal")),
        (sidecar(database_path, "-shm"), sidecar(&quarantine, "-shm")),
    ] {
        if !from.exists() {
            continue;
        }
        std::fs::rename(&from, &to).map_err(|error| {
            CameraError::Catalog(format!(
                "the catalog at {} is corrupt and could not be moved aside: {error}",
                from.display()
            ))
        })?;
    }

    tracing::error!(
        catalog = %database_path.display(),
        quarantined_to = %quarantine.display(),
        reason = %reason,
        "the catalog was corrupt and has been quarantined; the component is starting on a new, empty \
         catalog. Any capture results it had not yet published are in the quarantined file and are \
         NOT recoverable by this component."
    );
    Ok(())
}

/// The path of a SQLite sidecar (`-wal`, `-shm`) beside a database.
fn sidecar(database_path: &Path, suffix: &str) -> PathBuf {
    let mut path = database_path.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

fn open_blocking(options: CatalogOptions) -> Result<Catalog> {
    let state_directory = Arc::new(create_secure_directory(&options.state_directory)?);
    #[cfg(windows)]
    state_directory.validate_existing_entries()?;
    let state_lock = Arc::new(acquire_state_lock(&options.state_directory)?);
    let database_path = options.state_directory.join(DATABASE_NAME);
    create_secure_file(&database_path)?;

    // A corrupt catalog used to be a PERMANENT UNATTENDED CRASH-LOOP: fail closed, preserve the
    // evidence, and never run again until a human is dispatched. That is a fine trade on a machine
    // someone can walk up to. On a Greengrass edge box it means a camera adapter that has stopped
    // capturing, forever, because of a bad flush on a power cut -- and the evidence it so carefully
    // preserved is of no use to anyone who is not standing next to it.
    //
    // Availability wins. The corrupt file is moved aside, not deleted, and the component starts on a
    // clean catalog. What that costs is stated plainly rather than hidden: any results in the corrupt
    // file that had not yet been published are gone as far as this component is concerned.
    let connections = match open_verified(&database_path, options.worker_count) {
        Ok(connections) => connections,
        Err(OpenFailure::Fatal(error)) => return Err(error),
        Err(OpenFailure::Corrupt(reason)) => {
            quarantine_corrupt_catalog(&database_path, &reason)?;
            create_secure_file(&database_path)?;
            // A freshly created file that is STILL unusable is not corruption -- it is an environment
            // that cannot host a catalog at all, and starting over again would be a loop.
            open_verified(&database_path, options.worker_count).map_err(CameraError::from)?
        }
    };
    secure_sqlite_sidecars(&database_path)?;

    let (sender, receiver) = mpsc::channel::<Work>(options.queue_capacity);
    let (availability_tx, _) = watch::channel(CatalogAvailability::default());
    let receiver = Arc::new(Mutex::new(receiver));
    let mut workers = Vec::with_capacity(options.worker_count);
    for (index, mut connection) in connections.into_iter().enumerate() {
        let receiver = Arc::clone(&receiver);
        let worker_lock = Arc::clone(&state_lock);
        let handle = thread::Builder::new()
            .name(format!("camera-catalog-{index}"))
            .spawn(move || {
                let _lock_lifetime = worker_lock;
                loop {
                    let work = match receiver.lock() {
                        Ok(mut receiver) => receiver.blocking_recv(),
                        Err(_) => None,
                    };
                    match work {
                        Some(work) => work(&mut connection),
                        None => break,
                    }
                }
            })
            .map_err(|error| {
                CameraError::Catalog(format!("failed to start catalog worker {index}: {error}"))
            })?;
        workers.push(handle);
    }

    Ok(Catalog {
        inner: Arc::new(CatalogInner {
            sender,
            availability_tx,
            database_path,
            _state_directory: state_directory,
            _state_lock: state_lock,
            _workers: workers,
        }),
    })
}

fn acquire_state_lock(directory: &Path) -> Result<StateLock> {
    let path = directory.join(LOCK_NAME);
    let file = open_secure_read_write(&path)?;
    set_secure_file_mode(&path)?;
    if !file.try_lock_exclusive()? {
        return Err(CameraError::Catalog(format!(
            "state directory is already locked: {}",
            directory.display()
        )));
    }
    Ok(StateLock { _file: file })
}

fn open_connection(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS as u64))?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(CameraError::Catalog(format!(
            "SQLite refused WAL journal mode: {mode}"
        )));
    }
    connection.pragma_update(None, "synchronous", "FULL")?;
    verify_connection(&connection)?;
    Ok(connection)
}

fn verify_connection(connection: &Connection) -> Result<()> {
    let health = read_health(connection)?;
    if !health.foreign_keys {
        return Err(CameraError::Catalog(
            "SQLite foreign_keys verification failed".to_owned(),
        ));
    }
    if !health.journal_mode.eq_ignore_ascii_case("wal") {
        return Err(CameraError::Catalog(format!(
            "SQLite journal_mode is {}, not WAL",
            health.journal_mode
        )));
    }
    if health.synchronous != 2 {
        return Err(CameraError::Catalog(format!(
            "SQLite synchronous is {}, not FULL (2)",
            health.synchronous
        )));
    }
    if health.busy_timeout_ms < BUSY_TIMEOUT_MS {
        return Err(CameraError::Catalog(format!(
            "SQLite busy_timeout is {}ms, below {}ms",
            health.busy_timeout_ms, BUSY_TIMEOUT_MS
        )));
    }
    if !health.integrity_ok {
        return Err(CameraError::Catalog(INTEGRITY_CHECK_FAILED.to_owned()));
    }
    Ok(())
}

fn read_health(connection: &Connection) -> Result<CatalogHealth> {
    let schema_version = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let foreign_keys: i64 = connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    let journal_mode = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    let synchronous = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    let busy_timeout_ms = connection.query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
    let mut statement = connection.prepare("PRAGMA integrity_check")?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let integrity_ok = rows.len() == 1 && rows[0].eq_ignore_ascii_case("ok");
    Ok(CatalogHealth {
        schema_version,
        foreign_keys: foreign_keys == 1,
        journal_mode,
        synchronous,
        busy_timeout_ms,
        integrity_ok,
    })
}

fn migrate(connection: &mut Connection) -> Result<()> {
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(CameraError::Catalog(format!(
            "catalog schema version {version} is newer than supported version {SCHEMA_VERSION}"
        )));
    }
    if version == SCHEMA_VERSION {
        return Ok(());
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    match version {
        0 => transaction.execute_batch(SCHEMA_V1)?,
        1 => {
            transaction.execute_batch(MIGRATION_V2)?;
            transaction.execute_batch(MIGRATION_V3)?;
            transaction.execute_batch(MIGRATION_V4)?;
        }
        2 => {
            transaction.execute_batch(MIGRATION_V3)?;
            transaction.execute_batch(MIGRATION_V4)?;
        }
        3 => transaction.execute_batch(MIGRATION_V4)?,
        _ => {
            return Err(CameraError::Catalog(format!(
                "no monotonic migration from schema version {version}"
            )));
        }
    }
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

const SCHEMA_V1: &str = r#"
CREATE TABLE command_ledger (
    instance TEXT NOT NULL,
    verb TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_hash BLOB NOT NULL CHECK(length(request_hash)=32),
    canonical_request_json TEXT NOT NULL CHECK(json_valid(canonical_request_json)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    operation_state TEXT NOT NULL CHECK(operation_state IN ('IN_PROGRESS','SUCCEEDED','FAILED','OUTCOME_UNKNOWN')),
    capture_id TEXT,
    group_id TEXT,
    reply_json TEXT CHECK(reply_json IS NULL OR json_valid(reply_json)),
    error_code TEXT,
    error_message TEXT,
    PRIMARY KEY(instance,verb,request_id),
    CHECK(capture_id IS NULL OR group_id IS NULL)
) STRICT, WITHOUT ROWID;

CREATE TABLE capture_groups (
    group_id TEXT PRIMARY KEY,
    ledger_instance TEXT NOT NULL,
    ledger_verb TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_hash BLOB NOT NULL CHECK(length(request_hash)=32),
    canonical_request_json TEXT NOT NULL CHECK(json_valid(canonical_request_json)),
    state TEXT NOT NULL CHECK(state IN ('ACCEPTED','QUEUED','SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')),
    origin_correlation_id TEXT,
    accepted_at_ms INTEGER NOT NULL,
    queued_at_ms INTEGER,
    terminal_at_ms INTEGER,
    terminal_result_json TEXT CHECK(terminal_result_json IS NULL OR json_valid(terminal_result_json)),
    error_code TEXT,
    error_message TEXT,
    UNIQUE(ledger_instance,ledger_verb,request_id),
    FOREIGN KEY(ledger_instance,ledger_verb,request_id)
      REFERENCES command_ledger(instance,verb,request_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE jobs (
    capture_id TEXT PRIMARY KEY,
    instance TEXT NOT NULL,
    ledger_instance TEXT,
    verb TEXT,
    request_id TEXT,
    request_hash BLOB NOT NULL CHECK(length(request_hash)=32),
    canonical_request_json TEXT NOT NULL CHECK(json_valid(canonical_request_json)),
    effective_profile_json TEXT NOT NULL CHECK(json_valid(effective_profile_json)),
    state TEXT NOT NULL CHECK(state IN ('ACCEPTED','QUEUED','ACQUIRING','ENCODING','PERSISTING','SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')),
    accepted_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    queued_at_ms INTEGER,
    terminal_at_ms INTEGER,
    terminal_deadline_ms INTEGER NOT NULL,
    queue_deadline_ms INTEGER,
    capture_deadline_ms INTEGER NOT NULL,
    encode_deadline_ms INTEGER NOT NULL,
    persist_deadline_ms INTEGER NOT NULL,
    trigger_json TEXT NOT NULL CHECK(json_valid(trigger_json)),
    origin_correlation_id TEXT,
    intended_output_json TEXT NOT NULL CHECK(json_valid(intended_output_json)),
    partial_path TEXT,
    final_path TEXT,
    expected_sha256 TEXT CHECK(expected_sha256 IS NULL OR
      (length(expected_sha256)=64 AND expected_sha256 NOT GLOB '*[^0-9a-f]*')),
    expected_bytes INTEGER CHECK(expected_bytes IS NULL OR expected_bytes>=0),
    installed_sha256 TEXT CHECK(installed_sha256 IS NULL OR
      (length(installed_sha256)=64 AND installed_sha256 NOT GLOB '*[^0-9a-f]*')),
    installed_bytes INTEGER CHECK(installed_bytes IS NULL OR installed_bytes>=0),
    group_id TEXT,
    install_started INTEGER NOT NULL DEFAULT 0 CHECK(install_started IN (0,1)),
    terminal_result_json TEXT CHECK(terminal_result_json IS NULL OR json_valid(terminal_result_json)),
    error_code TEXT,
    error_message TEXT,
    UNIQUE(instance,request_id),
    CHECK((request_id IS NULL AND verb IS NULL AND ledger_instance IS NULL) OR
          (request_id IS NOT NULL AND verb IS NOT NULL AND ledger_instance IS NOT NULL)),
    FOREIGN KEY(ledger_instance,verb,request_id)
      REFERENCES command_ledger(instance,verb,request_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(group_id) REFERENCES capture_groups(group_id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE pending_success (
    capture_id TEXT PRIMARY KEY,
    terminal_result_json TEXT NOT NULL CHECK(json_valid(terminal_result_json)),
    terminal_at_ms INTEGER NOT NULL,
    event_key TEXT NOT NULL UNIQUE,
    message_kind TEXT NOT NULL,
    header_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    envelope_uuid TEXT NOT NULL UNIQUE,
    encoded_envelope BLOB NOT NULL CHECK(length(encoded_envelope)>0),
    created_at_ms INTEGER NOT NULL,
    available_at_ms INTEGER NOT NULL,
    FOREIGN KEY(capture_id) REFERENCES jobs(capture_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE group_members (
    group_id TEXT NOT NULL,
    capture_id TEXT NOT NULL UNIQUE,
    result_index INTEGER NOT NULL CHECK(result_index>=0),
    PRIMARY KEY(group_id,result_index),
    FOREIGN KEY(group_id) REFERENCES capture_groups(group_id) ON DELETE RESTRICT,
    FOREIGN KEY(capture_id) REFERENCES jobs(capture_id) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE schedule_occurrences (
    instance TEXT NOT NULL,
    schedule_id TEXT NOT NULL,
    intended_fire_time_ms INTEGER NOT NULL,
    capture_id TEXT NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY(instance,schedule_id,intended_fire_time_ms),
    FOREIGN KEY(capture_id) REFERENCES jobs(capture_id) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE deferred_waiters (
    waiter_id TEXT PRIMARY KEY,
    capture_id TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    request_uuid TEXT,
    expires_at_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    FOREIGN KEY(capture_id) REFERENCES jobs(capture_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT NOT NULL UNIQUE,
    capture_id TEXT,
    group_id TEXT,
    message_kind TEXT NOT NULL,
    topic TEXT NOT NULL,
    envelope_uuid TEXT NOT NULL UNIQUE,
    encoded_envelope BLOB NOT NULL CHECK(length(encoded_envelope)>0),
    created_at_ms INTEGER NOT NULL,
    available_at_ms INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
    last_attempt_at_ms INTEGER,
    delivered_at_ms INTEGER,
    last_error TEXT,
    UNIQUE(capture_id,message_kind),
    UNIQUE(group_id,message_kind),
    CHECK((capture_id IS NOT NULL AND group_id IS NULL) OR (capture_id IS NULL AND group_id IS NOT NULL)),
    FOREIGN KEY(capture_id) REFERENCES jobs(capture_id) ON DELETE RESTRICT,
    FOREIGN KEY(group_id) REFERENCES capture_groups(group_id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX jobs_recovery_idx ON jobs(state,accepted_at_ms,capture_id);
CREATE INDEX jobs_terminal_idx ON jobs(terminal_at_ms,capture_id) WHERE terminal_at_ms IS NOT NULL;
CREATE INDEX waiters_capture_idx ON deferred_waiters(capture_id,expires_at_ms);
CREATE INDEX outbox_pending_idx ON outbox(delivered_at_ms,available_at_ms,id);

CREATE TABLE group_schedule_cursors (
    schedule_id TEXT PRIMARY KEY,
    intended_fire_time_ms INTEGER NOT NULL,
    last_group_id TEXT,
    updated_at_ms INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE catalog_availability_probe (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    generation INTEGER NOT NULL CHECK(generation>=0)
) STRICT;
INSERT INTO catalog_availability_probe(singleton,generation) VALUES(1,0);
"#;

const MIGRATION_V2: &str = r#"
ALTER TABLE jobs ADD COLUMN expected_sha256 TEXT CHECK(expected_sha256 IS NULL OR
  (length(expected_sha256)=64 AND expected_sha256 NOT GLOB '*[^0-9a-f]*'));
ALTER TABLE jobs ADD COLUMN expected_bytes INTEGER CHECK(expected_bytes IS NULL OR expected_bytes>=0);
CREATE TABLE pending_success (
    capture_id TEXT PRIMARY KEY,
    terminal_result_json TEXT NOT NULL CHECK(json_valid(terminal_result_json)),
    terminal_at_ms INTEGER NOT NULL,
    event_key TEXT NOT NULL UNIQUE,
    message_kind TEXT NOT NULL,
    header_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    envelope_uuid TEXT NOT NULL UNIQUE,
    encoded_envelope BLOB NOT NULL CHECK(length(encoded_envelope)>0),
    created_at_ms INTEGER NOT NULL,
    available_at_ms INTEGER NOT NULL,
    FOREIGN KEY(capture_id) REFERENCES jobs(capture_id) ON DELETE CASCADE
) STRICT;
"#;

const MIGRATION_V4: &str = r#"
CREATE TABLE group_schedule_cursors (
    schedule_id TEXT PRIMARY KEY,
    intended_fire_time_ms INTEGER NOT NULL,
    last_group_id TEXT,
    updated_at_ms INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
"#;

const MIGRATION_V3: &str = r#"
CREATE TABLE catalog_availability_probe (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    generation INTEGER NOT NULL CHECK(generation>=0)
) STRICT;
INSERT INTO catalog_availability_probe(singleton,generation) VALUES(1,0);
"#;

#[cfg(unix)]
fn create_secure_directory(path: &Path) -> Result<StateDirectoryGuard> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700).create(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(StateDirectoryGuard::new())
}

#[cfg(not(any(unix, windows)))]
fn create_secure_directory(path: &Path) -> Result<StateDirectoryGuard> {
    fs::create_dir_all(path)?;
    Ok(StateDirectoryGuard::new())
}

#[cfg(windows)]
fn create_secure_directory(path: &Path) -> Result<StateDirectoryGuard> {
    use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
    use cap_std::{
        ambient_authority,
        fs::{Dir, OpenOptions, OpenOptionsExt},
    };

    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(CameraError::Catalog(
            "state directory must be an absolute Windows path without traversal".to_owned(),
        ));
    }

    // State-directory configuration is operator-owned, unlike an image output path derived from
    // request data. Opening every ancestor without following reparse points is both outside the
    // state-directory contract and incompatible with a service token that can create a directory
    // below a user profile but cannot enumerate that profile's ancestors. Create/open the declared
    // final directory, then retain a no-follow handle and replace its inherited DACL. Once this
    // succeeds, the handle's sharing mode and protected DACL protect the adapter-owned state root.
    std::fs::create_dir_all(path)
        .map_err(|error| CameraError::Catalog(format!("cannot create state directory: {error}")))?;

    // `Dir::open_ambient_dir` deliberately omits WRITE_DAC. Reopen `.` through the final
    // directory handle rather than reopening the ambient path, so the ACL operation itself cannot
    // be redirected after final-directory validation.
    const GENERIC_READ_WRITE_WITH_DACL: u32 = 0xC004_0000;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    let directory = Dir::open_ambient_dir(path, ambient_authority()).map_err(|error| {
        CameraError::Catalog(format!("cannot open declared state directory: {error}"))
    })?;
    let mut acl_options = OpenOptions::new();
    acl_options
        .read(true)
        .write(true)
        .access_mode(GENERIC_READ_WRITE_WITH_DACL)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .follow(FollowSymlinks::No);
    let mut acl_handle = directory.open_with(".", &acl_options).map_err(|error| {
        CameraError::Catalog(format!(
            "cannot reopen final state directory with DACL access: {error}"
        ))
    })?;
    crate::windows_security::restrict_state_handle(&mut acl_handle).map_err(|error| {
        CameraError::Catalog(format!("cannot restrict state-directory DACL: {error}"))
    })?;
    Ok(StateDirectoryGuard { directory })
}

#[cfg(unix)]
fn open_secure_read_write(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)?)
}

#[cfg(not(any(unix, windows)))]
fn open_secure_read_write(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?)
}

#[cfg(windows)]
fn open_secure_read_write(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .access_mode(0xC004_0000) // GENERIC_READ | GENERIC_WRITE | WRITE_DAC
        .open(path)?;
    crate::windows_security::restrict_state_handle(&mut file).map_err(|error| {
        CameraError::Catalog(format!("cannot restrict state-file DACL: {error}"))
    })?;
    Ok(file)
}

fn create_secure_file(path: &Path) -> Result<()> {
    drop(open_secure_read_write(path)?);
    set_secure_file_mode(path)
}

#[cfg(unix)]
fn set_secure_file_mode(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn set_secure_file_mode(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(windows)]
fn set_secure_file_mode(path: &Path) -> Result<()> {
    let mut file = open_secure_read_write(path)?;
    crate::windows_security::restrict_state_handle(&mut file)
        .map_err(|error| CameraError::Catalog(format!("cannot restrict state-file DACL: {error}")))
}

fn secure_sqlite_sidecars(database_path: &Path) -> Result<()> {
    set_secure_file_mode(database_path)?;
    let base = database_path.as_os_str().to_string_lossy();
    for suffix in ["-wal", "-shm"] {
        let path = PathBuf::from(format!("{base}{suffix}"));
        if path.exists() {
            set_secure_file_mode(&path)?;
        }
    }
    Ok(())
}

fn validate_ledger_key(key: &LedgerKey) -> Result<()> {
    require_nonempty("ledger instance", &key.instance)?;
    require_nonempty("ledger verb", &key.verb)?;
    validate_request_id(&key.request_id)
}

fn validate_new_job(job: &NewJob, group_member: bool) -> Result<()> {
    require_nonempty("capture_id", &job.capture_id)?;
    require_nonempty("instance", &job.instance)?;
    require_object("canonical_request", &job.canonical_request)?;
    require_object("effective_profile", &job.effective_profile)?;
    require_object("trigger", &job.trigger)?;
    require_object("intended_output", &job.intended_output)?;
    if job.deadlines.capture_at_ms > job.deadlines.terminal_at_ms
        || job.deadlines.encode_at_ms > job.deadlines.terminal_at_ms
        || job.deadlines.persist_at_ms > job.deadlines.terminal_at_ms
        || job
            .deadlines
            .queue_at_ms
            .is_some_and(|deadline| deadline > job.deadlines.terminal_at_ms)
    {
        return Err(CameraError::Catalog(
            "stage deadlines must not exceed the terminal deadline".to_owned(),
        ));
    }
    if let Some(key) = &job.ledger_key {
        validate_ledger_key(key)?;
        if key.instance != job.instance {
            return Err(CameraError::Catalog(
                "job and ledger instances must match".to_owned(),
            ));
        }
    }
    if group_member {
        if job.group_id.is_none() {
            return Err(CameraError::Catalog(
                "group member must name its group".to_owned(),
            ));
        }
        if job.ledger_key.is_some() {
            return Err(CameraError::Catalog(
                "group members are owned by the group ledger".to_owned(),
            ));
        }
    } else if job.group_id.is_some() {
        return Err(CameraError::Catalog(
            "standalone acceptance cannot attach an existing group".to_owned(),
        ));
    }
    Ok(())
}

fn validate_new_group(group: &NewGroup) -> Result<()> {
    require_nonempty("group_id", &group.group_id)?;
    validate_ledger_key(&group.ledger_key)?;
    require_object("canonical_request", &group.canonical_request)?;
    if group.members.len() < 2 {
        return Err(CameraError::Catalog(
            "capture group must contain at least two members".to_owned(),
        ));
    }
    let mut captures = std::collections::HashSet::new();
    let mut instances = std::collections::HashSet::new();
    for member in &group.members {
        validate_new_job(member, true)?;
        if member.group_id.as_deref() != Some(group.group_id.as_str()) {
            return Err(CameraError::Catalog(
                "member group_id does not match the owning group".to_owned(),
            ));
        }
        if !captures.insert(member.capture_id.as_str())
            || !instances.insert(member.instance.as_str())
        {
            return Err(CameraError::Catalog(
                "group capture identifiers and camera instances must be unique".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_waiter(waiter: &WaiterRecord) -> Result<()> {
    require_nonempty("waiter_id", &waiter.waiter_id)?;
    require_nonempty("capture_id", &waiter.capture_id)?;
    require_nonempty("correlation_id", &waiter.correlation_id)?;
    if waiter.expires_at_ms <= waiter.created_at_ms {
        return Err(CameraError::Catalog(
            "waiter expiry must follow creation".to_owned(),
        ));
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CameraError::Catalog(format!(
            "{label} must be 64 lower-case hexadecimal characters"
        )));
    }
    Ok(())
}

fn validate_terminal_write(write: &TerminalWrite) -> Result<()> {
    if !write.state.is_terminal() {
        return Err(CameraError::Catalog(
            "terminal write requires a terminal job state".to_owned(),
        ));
    }
    require_nonempty("outbox event_key", &write.outbox.event_key)?;
    require_nonempty("outbox message_kind", &write.outbox.message_kind)?;
    require_nonempty("outbox topic", &write.outbox.topic)?;
    require_nonempty("outbox envelope_uuid", &write.outbox.envelope_uuid)?;
    if write.outbox.encoded_envelope.is_empty() {
        return Err(CameraError::Catalog(
            "outbox encoded envelope must not be empty".to_owned(),
        ));
    }
    if write.outbox.available_at_ms < write.outbox.created_at_ms {
        return Err(CameraError::Catalog(
            "outbox availability must not precede creation".to_owned(),
        ));
    }
    if write.outbox.message_kind != "terminal" {
        return Err(CameraError::Catalog(
            "terminal outbox message_kind must be terminal".to_owned(),
        ));
    }
    let expected_header = match write.state {
        JobState::Succeeded => "ImageCaptured",
        JobState::Cancelled => "ImageCaptureCancelled",
        JobState::Failed | JobState::Interrupted => "ImageCaptureFailed",
        _ => unreachable!("terminal state was checked above"),
    };
    if write.outbox.header_name != expected_header {
        return Err(CameraError::Catalog(format!(
            "terminal state {:?} requires {expected_header}, not {}",
            write.state, write.outbox.header_name
        )));
    }
    require_object("terminal result", &write.result)?;
    match write.state {
        JobState::Succeeded if write.error_code.is_some() || write.error_message.is_some() => {
            return Err(CameraError::Catalog(
                "SUCCEEDED terminal writes must not retain an error".to_owned(),
            ));
        }
        JobState::Failed | JobState::Interrupted if write.error_code.is_none() => {
            return Err(CameraError::Catalog(
                "failed/interrupted terminal writes require a stable error code".to_owned(),
            ));
        }
        _ => {}
    }

    let envelope = Message::from_slice(&write.outbox.encoded_envelope).map_err(|error| {
        CameraError::Catalog(format!(
            "terminal outbox is not a valid EdgeCommons envelope: {error}"
        ))
    })?;
    if envelope.header.name != write.outbox.header_name
        || envelope.header.uuid != write.outbox.envelope_uuid
        || envelope.header.version != "1.0"
    {
        return Err(CameraError::Catalog(
            "terminal outbox metadata does not match its exact encoded envelope".to_owned(),
        ));
    }
    let expected_channel = match write.state {
        JobState::Succeeded => "/app/image/captured",
        JobState::Cancelled => "/app/image/cancelled",
        JobState::Failed | JobState::Interrupted => "/app/image/failed",
        _ => unreachable!("terminal state was checked above"),
    };
    if !write.outbox.topic.ends_with(expected_channel) {
        return Err(CameraError::Catalog(format!(
            "terminal outbox topic must end with {expected_channel}"
        )));
    }
    let body = envelope.body.as_object().ok_or_else(|| {
        CameraError::Catalog("terminal envelope body must be a JSON object".to_owned())
    })?;
    if body.get("schemaVersion").and_then(Value::as_u64) != Some(1)
        || body.get("eventId").and_then(Value::as_str) != Some(write.outbox.event_key.as_str())
        || body.get("correlationId").and_then(Value::as_str)
            != Some(envelope.header.correlation_id.as_str())
    {
        return Err(CameraError::Catalog(
            "terminal body schemaVersion, eventId, or correlationId does not match the envelope"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_terminal_for_job(write: &TerminalWrite, job: &JobRecord) -> Result<()> {
    let envelope = Message::from_slice(&write.outbox.encoded_envelope).map_err(|error| {
        CameraError::Catalog(format!(
            "terminal outbox envelope cannot be decoded: {error}"
        ))
    })?;
    let body = envelope.body.as_object().ok_or_else(|| {
        CameraError::Catalog("terminal envelope body must be a JSON object".to_owned())
    })?;
    if body.get("captureId").and_then(Value::as_str) != Some(job.capture_id.as_str())
        || body.get("cameraId").and_then(Value::as_str) != Some(job.instance.as_str())
    {
        return Err(CameraError::Catalog(
            "terminal envelope captureId/cameraId does not match the durable job".to_owned(),
        ));
    }
    if let Some(origin) = &job.origin_correlation_id {
        if envelope.header.correlation_id != *origin {
            return Err(CameraError::Catalog(
                "terminal command envelope correlation does not match the creating request"
                    .to_owned(),
            ));
        }
    }
    match (
        &job.group_id,
        body.get("captureGroupId").and_then(Value::as_str),
    ) {
        (Some(expected), Some(actual)) if expected == actual => {}
        (None, None) => {}
        _ => {
            return Err(CameraError::Catalog(
                "terminal envelope group identity does not match the durable job".to_owned(),
            ));
        }
    }
    Ok(())
}

fn require_nonempty(field: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        Err(CameraError::Catalog(format!("{field} must not be empty")))
    } else {
        Ok(())
    }
}

fn require_object(field: &str, value: &Value) -> Result<()> {
    if value.is_object() {
        Ok(())
    } else {
        Err(CameraError::Catalog(format!(
            "{field} must be a JSON object"
        )))
    }
}

fn bounded_error(mut value: String) -> String {
    const MAX: usize = 1_024;
    if value.len() <= MAX {
        return value;
    }
    let mut end = MAX;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

fn accept_job_blocking(connection: &mut Connection, job: &NewJob) -> Result<AcceptJobOutcome> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(key) = &job.ledger_key {
        if let Some(existing) = load_ledger(&transaction, key)? {
            let outcome = if existing.request_hash == job.request_hash {
                let capture_id = existing.capture_id.ok_or_else(|| {
                    CameraError::Catalog("capture ledger exists without its capture_id".to_owned())
                })?;
                AcceptJobOutcome::Existing(load_job_required(&transaction, &capture_id)?)
            } else {
                AcceptJobOutcome::Conflict
            };
            transaction.commit()?;
            return Ok(outcome);
        }
        // Capture idempotency is intentionally scoped to (camera, requestId), not to the
        // synchronous-vs-submitted verb. A retry through the other capture verb must resolve the
        // same physical job (or conflict), never fall through to the jobs UNIQUE constraint as a
        // raw SQLite error.
        let existing_capture: Option<String> = transaction
            .query_row(
                "SELECT capture_id FROM jobs WHERE instance=?1 AND request_id=?2",
                params![job.instance, key.request_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(capture_id) = existing_capture {
            let existing = load_job_required(&transaction, &capture_id)?;
            if existing.request_hash != job.request_hash {
                transaction.commit()?;
                return Ok(AcceptJobOutcome::Conflict);
            }
            insert_ledger(
                &transaction,
                key,
                job.request_hash,
                &job.canonical_request,
                job.accepted_at_ms,
                Some(&capture_id),
                None,
            )?;
            transaction.commit()?;
            return Ok(AcceptJobOutcome::Existing(existing));
        }
        insert_ledger(
            &transaction,
            key,
            job.request_hash,
            &job.canonical_request,
            job.accepted_at_ms,
            Some(&job.capture_id),
            None,
        )?;
    } else if let Some(existing) = load_job(&transaction, &job.capture_id)? {
        let outcome = if existing.request_hash == job.request_hash {
            AcceptJobOutcome::Existing(existing)
        } else {
            AcceptJobOutcome::Conflict
        };
        transaction.commit()?;
        return Ok(outcome);
    }
    insert_job(&transaction, job)?;
    let record = load_job_required(&transaction, &job.capture_id)?;
    transaction.commit()?;
    Ok(AcceptJobOutcome::Inserted(record))
}

fn accept_scheduled_job_blocking(
    connection: &mut Connection,
    job: &NewJob,
    schedule_id: &str,
    intended_fire_time_ms: i64,
) -> Result<AcceptJobOutcome> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let existing_capture: Option<String> = transaction
        .query_row(
            "SELECT capture_id FROM schedule_occurrences WHERE instance=?1 AND schedule_id=?2 AND intended_fire_time_ms=?3",
            params![job.instance, schedule_id, intended_fire_time_ms],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(capture_id) = existing_capture {
        let existing = load_job_required(&transaction, &capture_id)?;
        transaction.commit()?;
        return Ok(AcceptJobOutcome::Existing(existing));
    }
    if load_job(&transaction, &job.capture_id)?.is_some() {
        transaction.commit()?;
        return Ok(AcceptJobOutcome::Conflict);
    }
    insert_job(&transaction, job)?;
    transaction.execute(
        "INSERT INTO schedule_occurrences(instance,schedule_id,intended_fire_time_ms,capture_id,created_at_ms) \
         VALUES (?1,?2,?3,?4,?5)",
        params![
            job.instance,
            schedule_id,
            intended_fire_time_ms,
            job.capture_id,
            job.accepted_at_ms,
        ],
    )?;
    let record = load_job_required(&transaction, &job.capture_id)?;
    transaction.commit()?;
    Ok(AcceptJobOutcome::Inserted(record))
}

fn accept_group_blocking(
    connection: &mut Connection,
    group: &NewGroup,
) -> Result<AcceptGroupOutcome> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(existing) = load_ledger(&transaction, &group.ledger_key)? {
        let outcome = if existing.request_hash == group.request_hash {
            let group_id = existing.group_id.ok_or_else(|| {
                CameraError::Catalog("group ledger exists without its group_id".to_owned())
            })?;
            AcceptGroupOutcome::Existing(load_group_required(&transaction, &group_id)?)
        } else {
            AcceptGroupOutcome::Conflict
        };
        transaction.commit()?;
        return Ok(outcome);
    }

    insert_ledger(
        &transaction,
        &group.ledger_key,
        group.request_hash,
        &group.canonical_request,
        group.accepted_at_ms,
        None,
        Some(&group.group_id),
    )?;
    transaction.execute(
        "INSERT INTO capture_groups \
         (group_id,ledger_instance,ledger_verb,request_id,request_hash,canonical_request_json,state,\
          origin_correlation_id,accepted_at_ms) VALUES (?1,?2,?3,?4,?5,?6,'ACCEPTED',?7,?8)",
        params![
            group.group_id,
            group.ledger_key.instance,
            group.ledger_key.verb,
            group.ledger_key.request_id,
            group.request_hash.as_bytes().as_slice(),
            serde_json::to_string(&group.canonical_request)?,
            group.origin_correlation_id,
            group.accepted_at_ms,
        ],
    )?;
    for (index, member) in group.members.iter().enumerate() {
        insert_job(&transaction, member)?;
        transaction.execute(
            "INSERT INTO group_members(group_id,capture_id,result_index) VALUES (?1,?2,?3)",
            params![group.group_id, member.capture_id, index as i64],
        )?;
    }
    let record = load_group_required(&transaction, &group.group_id)?;
    transaction.commit()?;
    Ok(AcceptGroupOutcome::Inserted(record))
}

fn queue_group_blocking(
    connection: &mut Connection,
    group_id: &str,
    queued_at_ms: i64,
) -> Result<GroupRecord> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let group = load_group_required(&transaction, group_id)?;
    if group.state == JobState::Queued {
        transaction.commit()?;
        return Ok(group);
    }
    if group.state != JobState::Accepted {
        return Err(CameraError::Catalog(format!(
            "group {group_id} cannot queue from {:?}",
            group.state
        )));
    }
    if group
        .members
        .iter()
        .any(|member| member.state != JobState::Accepted)
    {
        return Err(CameraError::Catalog(
            "group members are not uniformly ACCEPTED".to_owned(),
        ));
    }
    let changed = transaction.execute(
        "UPDATE jobs SET state='QUEUED',queued_at_ms=?2,updated_at_ms=?2 \
         WHERE capture_id IN (SELECT capture_id FROM group_members WHERE group_id=?1) AND state='ACCEPTED'",
        params![group_id, queued_at_ms],
    )?;
    if changed != group.members.len() {
        return Err(CameraError::Catalog(
            "group queue CAS changed an unexpected number of members".to_owned(),
        ));
    }
    if transaction.execute(
        "UPDATE capture_groups SET state='QUEUED',queued_at_ms=?2 WHERE group_id=?1 AND state='ACCEPTED'",
        params![group_id, queued_at_ms],
    )? != 1
    {
        return Err(CameraError::Catalog("group queue CAS lost after member update".to_owned()));
    }
    let queued = load_group_required(&transaction, group_id)?;
    transaction.commit()?;
    Ok(queued)
}

fn complete_group_blocking(
    connection: &mut Connection,
    group_id: &str,
    state: JobState,
    result: &Value,
    error_code: Option<&str>,
    error_message: Option<&str>,
    terminal_at_ms: i64,
) -> Result<GroupRecord> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let before = load_group_required(&transaction, group_id)?;
    if before.state.is_terminal() {
        transaction.commit()?;
        return Ok(before);
    }
    if before
        .members
        .iter()
        .any(|member| !member.state.is_terminal())
    {
        return Err(CameraError::Catalog(
            "group cannot complete while a member is non-terminal".to_owned(),
        ));
    }
    let error_message = error_message.map(|value| bounded_error(value.to_owned()));
    if transaction.execute(
        "UPDATE capture_groups SET state=?2,terminal_at_ms=?3,terminal_result_json=?4,error_code=?5,error_message=?6 \
         WHERE group_id=?1 AND state IN ('ACCEPTED','QUEUED')",
        params![
            group_id,
            job_state_token(state),
            terminal_at_ms,
            serde_json::to_string(result)?,
            error_code,
            error_message,
        ],
    )? != 1
    {
        return Err(CameraError::Catalog(
            "group terminal CAS failed unexpectedly".to_owned(),
        ));
    }
    transaction.execute(
        "UPDATE command_ledger SET operation_state=?2,updated_at_ms=?3,reply_json=?4,error_code=?5,error_message=?6 \
         WHERE group_id=?1 AND operation_state='IN_PROGRESS'",
        params![
            group_id,
            ledger_state_token(if state == JobState::Succeeded {
                LedgerState::Succeeded
            } else {
                LedgerState::Failed
            }),
            terminal_at_ms,
            serde_json::to_string(result)?,
            error_code,
            error_message,
        ],
    )?;
    let record = load_group_required(&transaction, group_id)?;
    transaction.commit()?;
    Ok(record)
}

fn begin_command_blocking(
    connection: &mut Connection,
    key: &LedgerKey,
    request_hash: RequestHash,
    canonical_request: &Value,
    created_at_ms: i64,
) -> Result<BeginCommandOutcome> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(existing) = load_ledger(&transaction, key)? {
        let outcome = if existing.request_hash == request_hash {
            BeginCommandOutcome::Existing(existing)
        } else {
            BeginCommandOutcome::Conflict
        };
        transaction.commit()?;
        return Ok(outcome);
    }
    insert_ledger(
        &transaction,
        key,
        request_hash,
        canonical_request,
        created_at_ms,
        None,
        None,
    )?;
    let record = load_ledger(&transaction, key)?
        .ok_or_else(|| CameraError::Catalog("inserted ledger disappeared".to_owned()))?;
    transaction.commit()?;
    Ok(BeginCommandOutcome::Started(record))
}

fn complete_command_blocking(
    connection: &mut Connection,
    key: &LedgerKey,
    state: LedgerState,
    reply: &Value,
    error_code: Option<&str>,
    error_message: Option<&str>,
    updated_at_ms: i64,
) -> Result<CommandLedgerRecord> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed = transaction.execute(
        "UPDATE command_ledger SET operation_state=?4,reply_json=?5,error_code=?6,error_message=?7,updated_at_ms=?8 \
         WHERE instance=?1 AND verb=?2 AND request_id=?3 AND operation_state='IN_PROGRESS'",
        params![
            key.instance,
            key.verb,
            key.request_id,
            ledger_state_token(state),
            serde_json::to_string(reply)?,
            error_code,
            error_message.map(|value| bounded_error(value.to_owned())),
            updated_at_ms,
        ],
    )?;
    let record = load_ledger(&transaction, key)?
        .ok_or_else(|| CameraError::Catalog("command ledger does not exist".to_owned()))?;
    if changed == 0 && record.state == LedgerState::InProgress {
        return Err(CameraError::Catalog(
            "command completion CAS failed unexpectedly".to_owned(),
        ));
    }
    transaction.commit()?;
    Ok(record)
}

fn record_command_acceptance_blocking(
    connection: &mut Connection,
    key: &LedgerKey,
    reply: &Value,
    updated_at_ms: i64,
) -> Result<CommandLedgerRecord> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed = transaction.execute(
        "UPDATE command_ledger SET reply_json=?4,updated_at_ms=?5 \
         WHERE instance=?1 AND verb=?2 AND request_id=?3 AND operation_state='IN_PROGRESS'",
        params![
            key.instance,
            key.verb,
            key.request_id,
            serde_json::to_string(reply)?,
            updated_at_ms,
        ],
    )?;
    let record = load_ledger(&transaction, key)?
        .ok_or_else(|| CameraError::Catalog("command ledger does not exist".to_owned()))?;
    if changed == 0 && record.state == LedgerState::InProgress {
        return Err(CameraError::Catalog(
            "command acceptance reply CAS failed unexpectedly".to_owned(),
        ));
    }
    transaction.commit()?;
    Ok(record)
}

fn insert_ledger(
    transaction: &Transaction<'_>,
    key: &LedgerKey,
    request_hash: RequestHash,
    canonical_request: &Value,
    created_at_ms: i64,
    capture_id: Option<&str>,
    group_id: Option<&str>,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO command_ledger \
         (instance,verb,request_id,request_hash,canonical_request_json,created_at_ms,updated_at_ms,\
          operation_state,capture_id,group_id) VALUES (?1,?2,?3,?4,?5,?6,?6,'IN_PROGRESS',?7,?8)",
        params![
            key.instance,
            key.verb,
            key.request_id,
            request_hash.as_bytes().as_slice(),
            serde_json::to_string(canonical_request)?,
            created_at_ms,
            capture_id,
            group_id,
        ],
    )?;
    Ok(())
}

fn insert_job(transaction: &Transaction<'_>, job: &NewJob) -> Result<()> {
    let (ledger_instance, verb, request_id) =
        job.ledger_key.as_ref().map_or((None, None, None), |key| {
            (
                Some(key.instance.as_str()),
                Some(key.verb.as_str()),
                Some(key.request_id.as_str()),
            )
        });
    transaction.execute(
        "INSERT INTO jobs \
         (capture_id,instance,ledger_instance,verb,request_id,request_hash,canonical_request_json,\
          effective_profile_json,state,accepted_at_ms,updated_at_ms,terminal_deadline_ms,queue_deadline_ms,\
          capture_deadline_ms,encode_deadline_ms,persist_deadline_ms,trigger_json,origin_correlation_id,\
          intended_output_json,group_id) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'ACCEPTED',?9,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
        params![
            job.capture_id,
            job.instance,
            ledger_instance,
            verb,
            request_id,
            job.request_hash.as_bytes().as_slice(),
            serde_json::to_string(&job.canonical_request)?,
            serde_json::to_string(&job.effective_profile)?,
            job.accepted_at_ms,
            job.deadlines.terminal_at_ms,
            job.deadlines.queue_at_ms,
            job.deadlines.capture_at_ms,
            job.deadlines.encode_at_ms,
            job.deadlines.persist_at_ms,
            serde_json::to_string(&job.trigger)?,
            job.origin_correlation_id,
            serde_json::to_string(&job.intended_output)?,
            job.group_id,
        ],
    )?;
    Ok(())
}

fn cas_state_blocking(
    connection: &mut Connection,
    capture_id: &str,
    expected: JobState,
    next: JobState,
    changed_at_ms: i64,
) -> Result<StateCasOutcome> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed = transaction.execute(
        "UPDATE jobs SET state=?3,updated_at_ms=?4,queued_at_ms=CASE WHEN ?3='QUEUED' THEN ?4 ELSE queued_at_ms END \
         WHERE capture_id=?1 AND state=?2",
        params![capture_id, job_state_token(expected), job_state_token(next), changed_at_ms],
    )?;
    let record = load_job_required(&transaction, capture_id)?;
    transaction.commit()?;
    Ok(if changed == 1 {
        StateCasOutcome::Changed(record)
    } else {
        StateCasOutcome::NotChanged(record)
    })
}

fn begin_install_blocking(
    connection: &mut Connection,
    capture_id: &str,
    changed_at_ms: i64,
) -> Result<InstallOutcome> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed = transaction.execute(
        "UPDATE jobs SET install_started=1,updated_at_ms=?2 WHERE capture_id=?1 AND state='PERSISTING' AND install_started=0",
        params![capture_id, changed_at_ms],
    )?;
    let record = load_job_required(&transaction, capture_id)?;
    transaction.commit()?;
    Ok(if changed == 1 {
        InstallOutcome::Started(record)
    } else if record.state == JobState::Persisting && record.install_started {
        InstallOutcome::AlreadyStarted(record)
    } else {
        InstallOutcome::WrongState(record)
    })
}

fn terminal_blocking(
    connection: &mut Connection,
    capture_id: &str,
    write: &TerminalWrite,
    cancellation: bool,
    expected_states: Option<&[JobState]>,
) -> Result<TerminalOutcome> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let before = load_job_required(&transaction, capture_id)?;
    if before.state.is_terminal() {
        transaction.commit()?;
        return Ok(TerminalOutcome::AlreadyTerminal(before));
    }
    if let Some(expected) = expected_states {
        if !expected.contains(&before.state) {
            return Err(CameraError::Catalog(format!(
                "recovery CAS expected {expected:?}, found {:?}",
                before.state
            )));
        }
    }
    validate_terminal_for_job(write, &before)?;
    if cancellation && before.install_started {
        transaction.commit()?;
        return Ok(TerminalOutcome::InstallationWon(before));
    }
    if !before.state.can_transition_to(write.state) {
        return Err(CameraError::Catalog(format!(
            "invalid terminal transition {:?} -> {:?}",
            before.state, write.state
        )));
    }
    if write.state == JobState::Succeeded && before.pending_success.as_ref() != Some(write) {
        return Err(CameraError::Catalog(
            "success terminal write does not exactly match the durably staged write".to_owned(),
        ));
    }
    if write.state == JobState::Succeeded
        && (before.state != JobState::Persisting
            || !before.install_started
            || before.partial_path.is_none()
            || before.final_path.is_none()
            || before.expected_sha256.is_none()
            || before.expected_bytes.is_none()
            || before.installed_sha256.is_none()
            || before.installed_bytes.is_none())
    {
        return Err(CameraError::Catalog(
            "SUCCEEDED requires verified installed-artifact facts after the installation CAS"
                .to_owned(),
        ));
    }

    let changed = transaction.execute(
        "UPDATE jobs SET state=?2,updated_at_ms=?3,terminal_at_ms=?3,terminal_result_json=?4,\
         error_code=?5,error_message=?6 WHERE capture_id=?1 \
         AND state IN ('ACCEPTED','QUEUED','ACQUIRING','ENCODING','PERSISTING') \
         AND (?7=0 OR install_started=0)",
        params![
            capture_id,
            job_state_token(write.state),
            write.terminal_at_ms,
            serde_json::to_string(&write.result)?,
            write.error_code,
            write
                .error_message
                .as_ref()
                .map(|value| bounded_error(value.clone())),
            i64::from(cancellation),
        ],
    )?;
    if changed != 1 {
        let current = load_job_required(&transaction, capture_id)?;
        transaction.commit()?;
        return Ok(
            if cancellation && current.install_started && !current.state.is_terminal() {
                TerminalOutcome::InstallationWon(current)
            } else {
                TerminalOutcome::AlreadyTerminal(current)
            },
        );
    }
    insert_outbox(&transaction, capture_id, write)?;
    transaction.execute(
        "DELETE FROM pending_success WHERE capture_id=?1",
        params![capture_id],
    )?;
    if write.state != JobState::Succeeded {
        transaction.execute(
            "UPDATE jobs SET expected_sha256=NULL,expected_bytes=NULL WHERE capture_id=?1",
            params![capture_id],
        )?;
    }
    let ledger_state = if write.state == JobState::Succeeded {
        LedgerState::Succeeded
    } else {
        LedgerState::Failed
    };
    transaction.execute(
        "UPDATE command_ledger SET operation_state=?2,updated_at_ms=?3,reply_json=?4,error_code=?5,error_message=?6 \
         WHERE capture_id=?1 AND operation_state='IN_PROGRESS'",
        params![
            capture_id,
            ledger_state_token(ledger_state),
            write.terminal_at_ms,
            serde_json::to_string(&write.result)?,
            write.error_code,
            write.error_message.as_ref().map(|value| bounded_error(value.clone())),
        ],
    )?;
    let record = load_job_required(&transaction, capture_id)?;
    transaction.commit()?;
    Ok(TerminalOutcome::Won(record))
}

fn insert_outbox(
    transaction: &Transaction<'_>,
    capture_id: &str,
    write: &TerminalWrite,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO outbox \
         (event_key,capture_id,message_kind,topic,envelope_uuid,encoded_envelope,created_at_ms,available_at_ms) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            write.outbox.event_key,
            capture_id,
            write.outbox.message_kind,
            write.outbox.topic,
            write.outbox.envelope_uuid,
            write.outbox.encoded_envelope,
            write.outbox.created_at_ms,
            write.outbox.available_at_ms,
        ],
    )?;
    Ok(())
}

fn insert_pending_success(
    transaction: &Transaction<'_>,
    capture_id: &str,
    write: &TerminalWrite,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO pending_success \
         (capture_id,terminal_result_json,terminal_at_ms,event_key,message_kind,header_name,topic,\
          envelope_uuid,encoded_envelope,created_at_ms,available_at_ms) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            capture_id,
            serde_json::to_string(&write.result)?,
            write.terminal_at_ms,
            write.outbox.event_key,
            write.outbox.message_kind,
            write.outbox.header_name,
            write.outbox.topic,
            write.outbox.envelope_uuid,
            write.outbox.encoded_envelope,
            write.outbox.created_at_ms,
            write.outbox.available_at_ms,
        ],
    )?;
    Ok(())
}

fn prune_terminal_jobs_blocking(
    connection: &mut Connection,
    terminal_before_ms: i64,
    limit: usize,
) -> Result<u64> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let capture_ids = {
        let mut statement = transaction.prepare(
            "SELECT j.capture_id FROM jobs j WHERE j.group_id IS NULL AND j.terminal_at_ms IS NOT NULL \
             AND j.terminal_at_ms<?1 AND j.state IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED') \
             AND NOT EXISTS(SELECT 1 FROM outbox o WHERE o.capture_id=j.capture_id) \
             AND NOT EXISTS(SELECT 1 FROM command_ledger l WHERE l.capture_id=j.capture_id AND l.operation_state='OUTCOME_UNKNOWN') \
             ORDER BY j.terminal_at_ms,j.capture_id LIMIT ?2",
        )?;
        statement
            .query_map(params![terminal_before_ms, limit as i64], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    delete_terminal_jobs(&transaction, &capture_ids)?;
    transaction.commit()?;
    Ok(capture_ids.len() as u64)
}

fn prune_terminal_groups_blocking(
    connection: &mut Connection,
    terminal_before_ms: i64,
    limit: usize,
) -> Result<u64> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let group_ids = {
        let mut statement = transaction.prepare(
            "SELECT g.group_id FROM capture_groups g \
             WHERE g.terminal_at_ms IS NOT NULL AND g.terminal_at_ms<?1 \
             AND g.state IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED') \
             AND NOT EXISTS(SELECT 1 FROM group_members gm JOIN jobs j ON j.capture_id=gm.capture_id \
                            WHERE gm.group_id=g.group_id AND j.state NOT IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')) \
             AND NOT EXISTS(SELECT 1 FROM group_members gm JOIN outbox o ON o.capture_id=gm.capture_id \
                            WHERE gm.group_id=g.group_id) \
             AND NOT EXISTS(SELECT 1 FROM outbox o WHERE o.group_id=g.group_id) \
             AND NOT EXISTS(SELECT 1 FROM command_ledger l WHERE l.group_id=g.group_id AND l.operation_state='OUTCOME_UNKNOWN') \
             ORDER BY g.terminal_at_ms,g.group_id LIMIT ?2",
        )?;
        statement
            .query_map(params![terminal_before_ms, limit as i64], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for group_id in &group_ids {
        let captures = {
            let mut statement = transaction.prepare(
                "SELECT capture_id FROM group_members WHERE group_id=?1 ORDER BY result_index",
            )?;
            statement
                .query_map(params![group_id], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        transaction.execute(
            "DELETE FROM group_members WHERE group_id=?1",
            params![group_id],
        )?;
        for capture_id in &captures {
            transaction.execute(
                "DELETE FROM schedule_occurrences WHERE capture_id=?1",
                params![capture_id],
            )?;
            transaction.execute("DELETE FROM jobs WHERE capture_id=?1", params![capture_id])?;
        }
        transaction.execute(
            "DELETE FROM capture_groups WHERE group_id=?1",
            params![group_id],
        )?;
        transaction.execute(
            "DELETE FROM command_ledger WHERE group_id=?1 AND operation_state!='OUTCOME_UNKNOWN'",
            params![group_id],
        )?;
    }
    transaction.commit()?;
    Ok(group_ids.len() as u64)
}

fn enforce_result_record_limit_blocking(
    connection: &mut Connection,
    max_result_records: i64,
    limit: usize,
) -> Result<u64> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let terminal_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM jobs WHERE state IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')",
        [],
        |row| row.get(0),
    )?;
    // Below the cap the excess is negative, and SQLite reads a negative LIMIT as *no* limit: the
    // selection below would then match every eligible terminal record and delete the entire
    // retained result set.  Nothing may be reclaimed until the count is actually exceeded.
    let excess = terminal_count.saturating_sub(max_result_records);
    if excess <= 0 {
        transaction.commit()?;
        return Ok(0);
    }
    let selection_limit = excess.min(limit as i64);
    let capture_ids = {
        let mut statement = transaction.prepare(
            "SELECT j.capture_id FROM jobs j WHERE j.group_id IS NULL \
             AND j.state IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED') \
             AND NOT EXISTS(SELECT 1 FROM outbox o WHERE o.capture_id=j.capture_id) \
             AND NOT EXISTS(SELECT 1 FROM command_ledger l WHERE l.capture_id=j.capture_id AND l.operation_state='OUTCOME_UNKNOWN') \
             ORDER BY j.terminal_at_ms,j.capture_id LIMIT ?1",
        )?;
        statement
            .query_map(params![selection_limit], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    delete_terminal_jobs(&transaction, &capture_ids)?;
    transaction.commit()?;
    Ok(capture_ids.len() as u64)
}

fn delete_terminal_jobs(transaction: &Transaction<'_>, capture_ids: &[String]) -> Result<()> {
    for capture_id in capture_ids {
        transaction.execute(
            "DELETE FROM schedule_occurrences WHERE capture_id=?1",
            params![capture_id],
        )?;
        transaction.execute("DELETE FROM jobs WHERE capture_id=?1", params![capture_id])?;
        transaction.execute(
            "DELETE FROM command_ledger WHERE capture_id=?1 AND operation_state!='OUTCOME_UNKNOWN'",
            params![capture_id],
        )?;
    }
    Ok(())
}

const JOB_SELECT: &str = "SELECT capture_id,instance,verb,request_id,state,canonical_request_json,request_hash,\
 effective_profile_json,accepted_at_ms,queued_at_ms,terminal_at_ms,terminal_deadline_ms,queue_deadline_ms,\
 capture_deadline_ms,encode_deadline_ms,persist_deadline_ms,trigger_json,origin_correlation_id,\
 intended_output_json,partial_path,final_path,expected_sha256,expected_bytes,installed_sha256,installed_bytes,\
 group_id,install_started,terminal_result_json,error_code,error_message,\
 (SELECT terminal_result_json FROM pending_success p WHERE p.capture_id=jobs.capture_id),\
 (SELECT terminal_at_ms FROM pending_success p WHERE p.capture_id=jobs.capture_id),\
 (SELECT event_key FROM pending_success p WHERE p.capture_id=jobs.capture_id),\
 (SELECT message_kind FROM pending_success p WHERE p.capture_id=jobs.capture_id),\
 (SELECT header_name FROM pending_success p WHERE p.capture_id=jobs.capture_id),\
 (SELECT topic FROM pending_success p WHERE p.capture_id=jobs.capture_id),\
 (SELECT envelope_uuid FROM pending_success p WHERE p.capture_id=jobs.capture_id),\
 (SELECT encoded_envelope FROM pending_success p WHERE p.capture_id=jobs.capture_id),\
 (SELECT created_at_ms FROM pending_success p WHERE p.capture_id=jobs.capture_id),\
 (SELECT available_at_ms FROM pending_success p WHERE p.capture_id=jobs.capture_id) FROM jobs";

struct RawJob {
    capture_id: String,
    instance: String,
    verb: Option<String>,
    request_id: Option<String>,
    state: String,
    canonical_request: String,
    request_hash: Vec<u8>,
    effective_profile: String,
    accepted_at_ms: i64,
    queued_at_ms: Option<i64>,
    terminal_at_ms: Option<i64>,
    deadlines: JobDeadlines,
    trigger: String,
    origin_correlation_id: Option<String>,
    intended_output: String,
    partial_path: Option<String>,
    final_path: Option<String>,
    expected_sha256: Option<String>,
    expected_bytes: Option<i64>,
    installed_sha256: Option<String>,
    installed_bytes: Option<i64>,
    group_id: Option<String>,
    install_started: bool,
    terminal_result: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    pending_result: Option<String>,
    pending_terminal_at_ms: Option<i64>,
    pending_event_key: Option<String>,
    pending_message_kind: Option<String>,
    pending_header_name: Option<String>,
    pending_topic: Option<String>,
    pending_envelope_uuid: Option<String>,
    pending_encoded_envelope: Option<Vec<u8>>,
    pending_created_at_ms: Option<i64>,
    pending_available_at_ms: Option<i64>,
}

fn required_pending<T>(field: &str, value: Option<T>) -> Result<T> {
    value.ok_or_else(|| {
        CameraError::Catalog(format!(
            "pending success row is missing its required {field}"
        ))
    })
}

impl RawJob {
    fn into_record(self) -> Result<JobRecord> {
        let pending_success = self
            .pending_result
            .map(|result| -> Result<TerminalWrite> {
                Ok(TerminalWrite {
                    state: JobState::Succeeded,
                    result: serde_json::from_str(&result)?,
                    error_code: None,
                    error_message: None,
                    terminal_at_ms: required_pending(
                        "terminal_at_ms",
                        self.pending_terminal_at_ms,
                    )?,
                    outbox: NewOutboxMessage {
                        event_key: required_pending("event_key", self.pending_event_key)?,
                        message_kind: required_pending("message_kind", self.pending_message_kind)?,
                        header_name: required_pending("header_name", self.pending_header_name)?,
                        topic: required_pending("topic", self.pending_topic)?,
                        envelope_uuid: required_pending(
                            "envelope_uuid",
                            self.pending_envelope_uuid,
                        )?,
                        encoded_envelope: required_pending(
                            "encoded_envelope",
                            self.pending_encoded_envelope,
                        )?,
                        created_at_ms: required_pending(
                            "created_at_ms",
                            self.pending_created_at_ms,
                        )?,
                        available_at_ms: required_pending(
                            "available_at_ms",
                            self.pending_available_at_ms,
                        )?,
                    },
                })
            })
            .transpose()?;
        Ok(JobRecord {
            capture_id: self.capture_id,
            instance: self.instance,
            verb: self.verb,
            request_id: self.request_id,
            state: parse_job_state(&self.state)?,
            canonical_request: serde_json::from_str(&self.canonical_request)?,
            request_hash: parse_request_hash(&self.request_hash)?,
            effective_profile: serde_json::from_str(&self.effective_profile)?,
            deadlines: self.deadlines,
            trigger: serde_json::from_str(&self.trigger)?,
            origin_correlation_id: self.origin_correlation_id,
            intended_output: serde_json::from_str(&self.intended_output)?,
            partial_path: self.partial_path,
            final_path: self.final_path,
            expected_sha256: self.expected_sha256,
            expected_bytes: self.expected_bytes.map(|value| value as u64),
            installed_sha256: self.installed_sha256,
            installed_bytes: self.installed_bytes.map(|value| value as u64),
            accepted_at_ms: self.accepted_at_ms,
            queued_at_ms: self.queued_at_ms,
            terminal_at_ms: self.terminal_at_ms,
            group_id: self.group_id,
            install_started: self.install_started,
            terminal_result: self
                .terminal_result
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
            error_code: self.error_code,
            error_message: self.error_message,
            pending_success,
        })
    }
}

fn raw_job_from_row(row: &Row<'_>) -> rusqlite::Result<RawJob> {
    Ok(RawJob {
        capture_id: row.get(0)?,
        instance: row.get(1)?,
        verb: row.get(2)?,
        request_id: row.get(3)?,
        state: row.get(4)?,
        canonical_request: row.get(5)?,
        request_hash: row.get(6)?,
        effective_profile: row.get(7)?,
        accepted_at_ms: row.get(8)?,
        queued_at_ms: row.get(9)?,
        terminal_at_ms: row.get(10)?,
        deadlines: JobDeadlines {
            terminal_at_ms: row.get(11)?,
            queue_at_ms: row.get(12)?,
            capture_at_ms: row.get(13)?,
            encode_at_ms: row.get(14)?,
            persist_at_ms: row.get(15)?,
        },
        trigger: row.get(16)?,
        origin_correlation_id: row.get(17)?,
        intended_output: row.get(18)?,
        partial_path: row.get(19)?,
        final_path: row.get(20)?,
        expected_sha256: row.get(21)?,
        expected_bytes: row.get(22)?,
        installed_sha256: row.get(23)?,
        installed_bytes: row.get(24)?,
        group_id: row.get(25)?,
        install_started: row.get::<_, i64>(26)? == 1,
        terminal_result: row.get(27)?,
        error_code: row.get(28)?,
        error_message: row.get(29)?,
        pending_result: row.get(30)?,
        pending_terminal_at_ms: row.get(31)?,
        pending_event_key: row.get(32)?,
        pending_message_kind: row.get(33)?,
        pending_header_name: row.get(34)?,
        pending_topic: row.get(35)?,
        pending_envelope_uuid: row.get(36)?,
        pending_encoded_envelope: row.get(37)?,
        pending_created_at_ms: row.get(38)?,
        pending_available_at_ms: row.get(39)?,
    })
}

fn load_job(connection: &Connection, capture_id: &str) -> Result<Option<JobRecord>> {
    let sql = format!("{JOB_SELECT} WHERE capture_id=?1");
    let raw = connection
        .query_row(&sql, params![capture_id], raw_job_from_row)
        .optional()?;
    raw.map(RawJob::into_record).transpose()
}

fn load_job_required(connection: &Connection, capture_id: &str) -> Result<JobRecord> {
    load_job(connection, capture_id)?
        .ok_or_else(|| CameraError::Catalog(format!("capture {capture_id} does not exist")))
}

fn load_group(connection: &Connection, group_id: &str) -> Result<Option<GroupRecord>> {
    type RawGroup = (
        String,
        String,
        String,
        Vec<u8>,
        Option<String>,
        i64,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let raw: Option<RawGroup> = connection.query_row(
        "SELECT request_id,state,canonical_request_json,request_hash,origin_correlation_id,accepted_at_ms,queued_at_ms,\
         terminal_at_ms,terminal_result_json,error_code,error_message \
         FROM capture_groups WHERE group_id=?1",
        params![group_id],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,
                   row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?)),
    ).optional()?;
    let Some((
        request_id,
        state,
        canonical_request,
        request_hash,
        origin_correlation_id,
        accepted_at_ms,
        queued_at_ms,
        terminal_at_ms,
        terminal_result,
        error_code,
        error_message,
    )) = raw
    else {
        return Ok(None);
    };
    let mut statement = connection.prepare(&format!(
        "{JOB_SELECT} WHERE capture_id IN (SELECT capture_id FROM group_members WHERE group_id=?1) \
         ORDER BY (SELECT result_index FROM group_members WHERE group_id=?1 AND capture_id=jobs.capture_id)"
    ))?;
    let members = statement
        .query_map(params![group_id], raw_job_from_row)?
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .map(RawJob::into_record)
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(GroupRecord {
        group_id: group_id.to_owned(),
        request_id,
        state: parse_job_state(&state)?,
        canonical_request: serde_json::from_str(&canonical_request)?,
        request_hash: parse_request_hash(&request_hash)?,
        origin_correlation_id,
        accepted_at_ms,
        queued_at_ms,
        terminal_at_ms,
        terminal_result: terminal_result
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
        error_code,
        error_message,
        members,
    }))
}

fn load_group_required(connection: &Connection, group_id: &str) -> Result<GroupRecord> {
    load_group(connection, group_id)?
        .ok_or_else(|| CameraError::Catalog(format!("capture group {group_id} does not exist")))
}

fn load_ledger(connection: &Connection, key: &LedgerKey) -> Result<Option<CommandLedgerRecord>> {
    type RawLedger = (
        Vec<u8>,
        String,
        String,
        i64,
        i64,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let raw: Option<RawLedger> = connection.query_row(
        "SELECT request_hash,canonical_request_json,operation_state,created_at_ms,updated_at_ms,capture_id,group_id,reply_json,error_code,error_message \
         FROM command_ledger WHERE instance=?1 AND verb=?2 AND request_id=?3",
        params![key.instance,key.verb,key.request_id],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?)),
    ).optional()?;
    let Some((
        request_hash,
        canonical_request,
        state,
        created_at_ms,
        updated_at_ms,
        capture_id,
        group_id,
        reply,
        error_code,
        error_message,
    )) = raw
    else {
        return Ok(None);
    };
    Ok(Some(CommandLedgerRecord {
        key: key.clone(),
        request_hash: parse_request_hash(&request_hash)?,
        canonical_request: serde_json::from_str(&canonical_request)?,
        state: parse_ledger_state(&state)?,
        created_at_ms,
        updated_at_ms,
        capture_id,
        group_id,
        reply: reply
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
        error_code,
        error_message,
    }))
}

fn waiter_from_row(row: &Row<'_>) -> rusqlite::Result<WaiterRecord> {
    Ok(WaiterRecord {
        waiter_id: row.get(0)?,
        capture_id: row.get(1)?,
        correlation_id: row.get(2)?,
        request_uuid: row.get(3)?,
        expires_at_ms: row.get(4)?,
        created_at_ms: row.get(5)?,
    })
}

fn outbox_from_row(row: &Row<'_>) -> rusqlite::Result<OutboxRecord> {
    Ok(OutboxRecord {
        id: row.get(0)?,
        event_key: row.get(1)?,
        capture_id: row.get(2)?,
        group_id: row.get(3)?,
        message_kind: row.get(4)?,
        topic: row.get(5)?,
        envelope_uuid: row.get(6)?,
        encoded_envelope: row.get(7)?,
        created_at_ms: row.get(8)?,
        available_at_ms: row.get(9)?,
        attempts: row.get(10)?,
        last_attempt_at_ms: row.get(11)?,
        delivered_at_ms: row.get(12)?,
        last_error: row.get(13)?,
        instance: row.get(14)?,
    })
}

fn parse_request_hash(bytes: &[u8]) -> Result<RequestHash> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| CameraError::Catalog("stored request hash is not 32 bytes".to_owned()))?;
    Ok(RequestHash::from_bytes(bytes))
}

fn parse_job_state(value: &str) -> Result<JobState> {
    match value {
        "ACCEPTED" => Ok(JobState::Accepted),
        "QUEUED" => Ok(JobState::Queued),
        "ACQUIRING" => Ok(JobState::Acquiring),
        "ENCODING" => Ok(JobState::Encoding),
        "PERSISTING" => Ok(JobState::Persisting),
        "SUCCEEDED" => Ok(JobState::Succeeded),
        "FAILED" => Ok(JobState::Failed),
        "CANCELLED" => Ok(JobState::Cancelled),
        "INTERRUPTED" => Ok(JobState::Interrupted),
        other => Err(CameraError::Catalog(format!(
            "unknown stored job state {other}"
        ))),
    }
}

pub(crate) const fn job_state_token(state: JobState) -> &'static str {
    match state {
        JobState::Accepted => "ACCEPTED",
        JobState::Queued => "QUEUED",
        JobState::Acquiring => "ACQUIRING",
        JobState::Encoding => "ENCODING",
        JobState::Persisting => "PERSISTING",
        JobState::Succeeded => "SUCCEEDED",
        JobState::Failed => "FAILED",
        JobState::Cancelled => "CANCELLED",
        JobState::Interrupted => "INTERRUPTED",
    }
}

fn parse_ledger_state(value: &str) -> Result<LedgerState> {
    match value {
        "IN_PROGRESS" => Ok(LedgerState::InProgress),
        "SUCCEEDED" => Ok(LedgerState::Succeeded),
        "FAILED" => Ok(LedgerState::Failed),
        "OUTCOME_UNKNOWN" => Ok(LedgerState::OutcomeUnknown),
        other => Err(CameraError::Catalog(format!(
            "unknown stored ledger state {other}"
        ))),
    }
}

const fn ledger_state_token(state: LedgerState) -> &'static str {
    match state {
        LedgerState::InProgress => "IN_PROGRESS",
        LedgerState::Succeeded => "SUCCEEDED",
        LedgerState::Failed => "FAILED",
        LedgerState::OutcomeUnknown => "OUTCOME_UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;
    use std::time::Duration;

    use edgecommons::messaging::MessageBuilder;
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::sync::Barrier;

    use super::*;
    use crate::idempotency::canonical_request_hash;

    fn options(directory: &TempDir) -> CatalogOptions {
        CatalogOptions {
            state_directory: directory.path().join("state"),
            worker_count: 4,
            queue_capacity: 16,
        }
    }

    async fn open(directory: &TempDir) -> Catalog {
        Catalog::open(options(directory)).await.unwrap()
    }

    #[tokio::test]
    async fn availability_ignores_semantic_errors_and_recovers_only_after_a_committed_probe() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        let mut availability = catalog.availability();

        let semantic_error: Result<()> = catalog
            .execute(|_| {
                Err(CameraError::Catalog(
                    "simulated SQLite worker failure".to_string(),
                ))
            })
            .await;
        assert!(semantic_error.is_err());
        assert!(availability.borrow().state_capacity_available);
        assert!(
            !availability.has_changed().unwrap(),
            "semantic catalog errors must not be mistaken for state-storage loss"
        );

        let failed: Result<()> = catalog
            .execute(|connection| {
                connection.execute_batch("not valid SQL")?;
                Ok(())
            })
            .await;
        assert!(matches!(failed, Err(CameraError::Sqlite(_))));
        availability.changed().await.unwrap();
        assert!(!availability.borrow().state_capacity_available);

        catalog.health().await.unwrap();
        assert!(
            !availability.has_changed().unwrap(),
            "a successful read cannot prove a durable write has recovered"
        );
        assert!(!availability.borrow().state_capacity_available);

        catalog.probe_commit().await.unwrap();
        availability.changed().await.unwrap();
        assert!(availability.borrow().state_capacity_available);
    }

    #[test]
    fn disk_full_is_the_only_sqlite_error_classified_for_storage_pressure_alarm() {
        let disk_full = CameraError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            None,
        ));
        let other = CameraError::Sqlite(rusqlite::Error::InvalidQuery);
        assert_eq!(durable_state_failure(&disk_full), Some(true));
        assert_eq!(durable_state_failure(&other), Some(false));
        assert_eq!(
            durable_state_failure(&CameraError::Catalog("semantic mismatch".to_string())),
            None
        );
    }

    fn direct_job(capture_id: &str, request_id: &str) -> NewJob {
        let canonical_request = json!({
            "requestId": request_id,
            "profile": "inspection",
            "metadata": {"lot": 7}
        });
        NewJob {
            capture_id: capture_id.to_owned(),
            instance: "camera-a".to_owned(),
            ledger_key: Some(LedgerKey::new("camera-a", "sb/capture", request_id).unwrap()),
            request_hash: canonical_request_hash(&canonical_request, false).unwrap(),
            canonical_request,
            effective_profile: json!({"name":"inspection","encoding":"jpeg","quality":91}),
            deadlines: JobDeadlines {
                terminal_at_ms: 10_000,
                queue_at_ms: Some(2_000),
                capture_at_ms: 4_000,
                encode_at_ms: 7_000,
                persist_at_ms: 9_000,
            },
            trigger: json!({"type":"command","requestId":request_id}),
            origin_correlation_id: Some(format!("corr-{capture_id}")),
            intended_output: json!({"relativePath":format!("{capture_id}.jpg")}),
            accepted_at_ms: 1_000,
            group_id: None,
        }
    }

    fn group_member(capture_id: &str, instance: &str, group_id: &str) -> NewJob {
        let mut job = direct_job(capture_id, "unused-member-key");
        job.instance = instance.to_owned();
        job.ledger_key = None;
        job.group_id = Some(group_id.to_owned());
        job.trigger = json!({"type":"group-command","captureGroupId":group_id});
        job
    }

    fn scheduled_job(capture_id: &str, profile: &str) -> NewJob {
        let mut job = direct_job(capture_id, "unused-schedule-key");
        job.ledger_key = None;
        job.canonical_request = json!({"scheduleId":"every-minute","profile":profile});
        job.request_hash = canonical_request_hash(&job.canonical_request, false).unwrap();
        job.effective_profile = json!({"name":profile,"encoding":"jpeg"});
        job.trigger =
            json!({"type":"schedule","scheduleId":"every-minute","intendedFireTime":2000});
        job.origin_correlation_id = None;
        job
    }

    fn terminal_write(capture_id: &str, state: JobState, sequence: u8) -> TerminalWrite {
        terminal_write_for(capture_id, "camera-a", None, state, sequence)
    }

    fn terminal_write_for(
        capture_id: &str,
        instance: &str,
        group_id: Option<&str>,
        state: JobState,
        sequence: u8,
    ) -> TerminalWrite {
        let header_name = match state {
            JobState::Succeeded => "ImageCaptured",
            JobState::Cancelled => "ImageCaptureCancelled",
            _ => "ImageCaptureFailed",
        };
        let channel = match state {
            JobState::Succeeded => "captured",
            JobState::Cancelled => "cancelled",
            _ => "failed",
        };
        let event_key = format!("terminal-event-{sequence}");
        let mut body = json!({
            "schemaVersion": 1,
            "eventId": event_key.clone(),
            "captureId": capture_id,
            "cameraId": instance,
            "correlationId": format!("corr-{capture_id}"),
        });
        if let Some(group_id) = group_id {
            body["captureGroupId"] = json!(group_id);
        }
        let message = MessageBuilder::new(header_name, "1.0")
            .correlation_id(format!("corr-{capture_id}"))
            .payload(body)
            .build();
        let encoded_envelope = message.to_vec().unwrap();
        TerminalWrite {
            state,
            result: json!({"winner": sequence, "state": job_state_token(state)}),
            error_code: match state {
                JobState::Succeeded => None,
                JobState::Cancelled => Some("CAPTURE_CANCELLED".to_owned()),
                _ => Some("PROCESS_INTERRUPTED".to_owned()),
            },
            error_message: (state != JobState::Succeeded).then(|| "bounded failure".to_owned()),
            terminal_at_ms: 20_000 + i64::from(sequence),
            outbox: NewOutboxMessage {
                event_key,
                message_kind: "terminal".to_owned(),
                header_name: header_name.to_owned(),
                topic: format!("ecv1/device/camera-adapter/{instance}/app/image/{channel}"),
                envelope_uuid: message.header.uuid,
                encoded_envelope,
                created_at_ms: 20_000 + i64::from(sequence),
                available_at_ms: 20_000 + i64::from(sequence),
            },
        }
    }

    async fn move_to_persisting(
        catalog: &Catalog,
        capture_id: &str,
        expected_sha256: String,
        expected_bytes: u64,
        pending_success: TerminalWrite,
    ) {
        assert!(matches!(
            catalog.queue_job(capture_id, 2_000).await.unwrap(),
            StateCasOutcome::Changed(_)
        ));
        for (expected, next, at) in [
            (JobState::Queued, JobState::Acquiring, 3_000),
            (JobState::Acquiring, JobState::Encoding, 4_000),
        ] {
            assert!(matches!(
                catalog
                    .compare_and_set_state(capture_id, expected, next, at)
                    .await
                    .unwrap(),
                StateCasOutcome::Changed(_)
            ));
        }
        assert!(matches!(
            catalog
                .begin_persisting(
                    capture_id,
                    PendingInstall {
                        partial_path: format!("C:/captures/{capture_id}.partial"),
                        final_path: format!("C:/captures/{capture_id}.jpg"),
                        expected_sha256: expected_sha256.to_owned(),
                        expected_bytes,
                        success: pending_success,
                        changed_at_ms: 5_000,
                    },
                )
                .await
                .unwrap(),
            StateCasOutcome::Changed(_)
        ));
    }

    async fn reopen_after_workers_exit(options: CatalogOptions) -> Catalog {
        let mut last_error = None;
        for _ in 0..100 {
            match Catalog::open(options.clone()).await {
                Ok(catalog) => return catalog,
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
        panic!("catalog lock was not released: {last_error:?}");
    }

    #[tokio::test]
    async fn open_verifies_schema_pragmas_and_rejects_a_second_owner() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        let health = catalog.health().await.unwrap();
        assert_eq!(health.schema_version, SCHEMA_VERSION);
        assert!(health.foreign_keys);
        assert_eq!(health.journal_mode.to_ascii_lowercase(), "wal");
        assert_eq!(health.synchronous, 2);
        assert!(health.busy_timeout_ms >= BUSY_TIMEOUT_MS);
        assert!(health.integrity_ok);

        let tables: HashSet<String> = catalog
            .execute(|connection| {
                let mut statement = connection.prepare(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                )?;
                Ok(statement.query_map([], |row| row.get(0))?
                    .collect::<std::result::Result<_, _>>()?)
            })
            .await
            .unwrap();
        for required in [
            "jobs",
            "capture_groups",
            "group_members",
            "command_ledger",
            "schedule_occurrences",
            "deferred_waiters",
            "outbox",
        ] {
            assert!(tables.contains(required), "missing table {required}");
        }

        let error = Catalog::open(options(&directory)).await.err().unwrap();
        assert!(error.to_string().contains("already locked"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_sqlite_work_never_blocks_the_tokio_runtime() {
        let directory = TempDir::new().unwrap();
        let mut config = options(&directory);
        config.worker_count = 1;
        config.queue_capacity = 1;
        let catalog = Catalog::open(config).await.unwrap();
        let (started_sender, started_receiver) = oneshot::channel();
        let slow_catalog = catalog.clone();
        let slow = tokio::spawn(async move {
            slow_catalog
                .execute(move |_| {
                    let _ = started_sender.send(());
                    thread::sleep(Duration::from_millis(100));
                    Ok(())
                })
                .await
        });
        started_receiver.await.unwrap();
        tokio::time::timeout(
            Duration::from_millis(30),
            tokio::time::sleep(Duration::from_millis(2)),
        )
        .await
        .expect("Tokio timer was blocked by SQLite");
        assert!(!slow.is_finished());
        slow.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn concurrent_acceptance_has_one_creator_then_exact_existing_and_conflict() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        let mut tasks = Vec::new();
        for index in 0..24 {
            let catalog = catalog.clone();
            tasks.push(tokio::spawn(async move {
                catalog
                    .accept_job(direct_job(&format!("cap-{index}"), "request-1"))
                    .await
                    .unwrap()
            }));
        }
        let mut inserted = Vec::new();
        let mut existing = Vec::new();
        for task in tasks {
            match task.await.unwrap() {
                AcceptJobOutcome::Inserted(record) => inserted.push(record),
                AcceptJobOutcome::Existing(record) => existing.push(record),
                AcceptJobOutcome::Conflict => panic!("equal hashes conflicted"),
            }
        }
        assert_eq!(inserted.len(), 1);
        assert_eq!(existing.len(), 23);
        assert!(
            existing
                .iter()
                .all(|record| record.capture_id == inserted[0].capture_id)
        );
        assert!(
            existing
                .iter()
                .all(|record| record.origin_correlation_id == inserted[0].origin_correlation_id)
        );
        assert_eq!(inserted[0].state, JobState::Accepted);

        let mut cross_verb = direct_job("cap-must-not-be-created", "request-1");
        cross_verb.ledger_key =
            Some(LedgerKey::new("camera-a", "sb/capture-submit", "request-1").unwrap());
        match catalog.accept_job(cross_verb).await.unwrap() {
            AcceptJobOutcome::Existing(record) => {
                assert_eq!(record.capture_id, inserted[0].capture_id);
            }
            other => panic!("cross-verb capture retry did not resolve existing job: {other:?}"),
        }

        let mut changed = direct_job("cap-conflict", "request-1");
        changed.canonical_request["profile"] = json!("different");
        changed.request_hash = canonical_request_hash(&changed.canonical_request, false).unwrap();
        assert_eq!(
            catalog.accept_job(changed).await.unwrap(),
            AcceptJobOutcome::Conflict
        );

        let winner = &inserted[0].capture_id;
        assert!(matches!(
            catalog.queue_job(winner, 2_000).await.unwrap(),
            StateCasOutcome::Changed(_)
        ));
        assert!(matches!(
            catalog.queue_job(winner, 2_001).await.unwrap(),
            StateCasOutcome::NotChanged(_)
        ));

        let by_ledger = catalog
            .job_by_ledger(LedgerKey::new("camera-a", "sb/capture", "request-1").unwrap())
            .await
            .unwrap()
            .expect("accepted request must remain queryable by its exact ledger key");
        assert_eq!(by_ledger.capture_id, *winner);
        let page = catalog
            .jobs_page(
                Some("camera-a".to_owned()),
                vec![JobState::Queued],
                None,
                10,
            )
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].capture_id, *winner);
    }

    #[tokio::test]
    async fn group_acceptance_and_queue_are_atomic_and_rollback_on_member_collision() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        let canonical = json!({"requestId":"group-request","instances":["camera-a","camera-b"]});
        let group = NewGroup {
            group_id: "group-1".to_owned(),
            ledger_key: LedgerKey::new("main", "sb/capture-group", "group-request").unwrap(),
            request_hash: canonical_request_hash(&canonical, true).unwrap(),
            canonical_request: canonical,
            origin_correlation_id: Some("group-correlation".to_owned()),
            accepted_at_ms: 1_000,
            members: vec![
                group_member("group-cap-a", "camera-a", "group-1"),
                group_member("group-cap-b", "camera-b", "group-1"),
            ],
        };
        let accepted = catalog.accept_group(group.clone()).await.unwrap();
        let AcceptGroupOutcome::Inserted(accepted) = accepted else {
            panic!("new group was not inserted")
        };
        assert_eq!(accepted.members.len(), 2);
        assert!(
            accepted
                .members
                .iter()
                .all(|member| member.state == JobState::Accepted)
        );
        let queued = catalog.queue_group("group-1", 2_000).await.unwrap();
        assert_eq!(queued.state, JobState::Queued);
        assert!(
            queued
                .members
                .iter()
                .all(|member| member.state == JobState::Queued)
        );
        assert!(matches!(
            catalog.accept_group(group.clone()).await.unwrap(),
            AcceptGroupOutcome::Existing(_)
        ));
        let by_group_key = catalog
            .group_by_ledger(LedgerKey::new("main", "sb/capture-group", "group-request").unwrap())
            .await
            .unwrap()
            .expect("group request must resolve through its component-scoped ledger key");
        assert_eq!(by_group_key.group_id, "group-1");
        assert_eq!(by_group_key.members.len(), 2);
        catalog
            .commit_terminal(
                "group-cap-a",
                terminal_write_for(
                    "group-cap-a",
                    "camera-a",
                    Some("group-1"),
                    JobState::Failed,
                    12,
                ),
            )
            .await
            .unwrap();
        catalog
            .commit_terminal(
                "group-cap-b",
                terminal_write_for(
                    "group-cap-b",
                    "camera-b",
                    Some("group-1"),
                    JobState::Failed,
                    13,
                ),
            )
            .await
            .unwrap();
        let completed = catalog
            .complete_group(
                "group-1",
                JobState::Failed,
                json!({"succeeded":0,"failed":2}),
                Some("BACKEND_ERROR".to_owned()),
                Some("members failed".to_owned()),
                30_000,
            )
            .await
            .unwrap();
        assert_eq!(completed.state, JobState::Failed);
        assert_eq!(
            completed.terminal_result,
            Some(json!({"succeeded":0,"failed":2}))
        );
        let group_outbox = catalog.pending_outbox(i64::MAX, 10).await.unwrap();
        assert_eq!(group_outbox.len(), 2);
        for record in group_outbox {
            catalog
                .mark_outbox_delivered(record.id, 35_000)
                .await
                .unwrap();
        }
        assert_eq!(catalog.prune_delivered_outbox(40_000, 10).await.unwrap(), 2);
        assert_eq!(catalog.prune_terminal_groups(40_000, 10).await.unwrap(), 1);
        assert!(catalog.job("group-cap-a").await.unwrap().is_none());
        assert!(matches!(
            catalog.accept_group(group).await.unwrap(),
            AcceptGroupOutcome::Inserted(_)
        ));

        catalog
            .accept_job(direct_job("collision", "standalone-collision"))
            .await
            .unwrap();
        let bad_canonical = json!({"requestId":"group-bad","instances":["camera-c","camera-d"]});
        let bad = NewGroup {
            group_id: "group-bad".to_owned(),
            ledger_key: LedgerKey::new("main", "sb/capture-group", "group-bad").unwrap(),
            request_hash: canonical_request_hash(&bad_canonical, true).unwrap(),
            canonical_request: bad_canonical,
            origin_correlation_id: None,
            accepted_at_ms: 1_100,
            members: vec![
                group_member("collision", "camera-c", "group-bad"),
                group_member("fresh", "camera-d", "group-bad"),
            ],
        };
        assert!(catalog.accept_group(bad.clone()).await.is_err());
        let mut corrected = bad;
        corrected.members[0].capture_id = "corrected".to_owned();
        assert!(matches!(
            catalog.accept_group(corrected).await.unwrap(),
            AcceptGroupOutcome::Inserted(_)
        ));
    }

    #[tokio::test]
    async fn schedule_occurrence_and_job_are_one_commit_and_waiters_do_not_replace_origin() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        let inserted = catalog
            .accept_scheduled_job(
                scheduled_job("scheduled-original", "night"),
                "every-minute",
                2_000,
            )
            .await
            .unwrap();
        assert!(matches!(inserted, AcceptJobOutcome::Inserted(_)));
        let duplicate = catalog
            .accept_scheduled_job(
                scheduled_job("scheduled-new", "changed"),
                "every-minute",
                2_000,
            )
            .await
            .unwrap();
        let AcceptJobOutcome::Existing(existing) = duplicate else {
            panic!("occurrence was duplicated")
        };
        assert_eq!(existing.capture_id, "scheduled-original");
        assert_eq!(existing.effective_profile["name"], "night");
        assert!(existing.origin_correlation_id.is_none());

        let waiter = WaiterRecord {
            waiter_id: "waiter-1".to_owned(),
            capture_id: existing.capture_id.clone(),
            correlation_id: "retry-correlation".to_owned(),
            request_uuid: Some("request-uuid".to_owned()),
            expires_at_ms: 9_000,
            created_at_ms: 3_000,
        };
        assert!(catalog.add_waiter(waiter.clone()).await.unwrap());
        assert!(!catalog.add_waiter(waiter.clone()).await.unwrap());
        assert_eq!(
            catalog.waiters(&existing.capture_id, 4_000).await.unwrap(),
            vec![waiter]
        );
        assert!(
            catalog
                .job(&existing.capture_id)
                .await
                .unwrap()
                .unwrap()
                .origin_correlation_id
                .is_none()
        );
    }

    /// Rebasing a capture's clocks is a durable write, and it refuses everything it should.
    ///
    /// The scheduler rebases a capture the moment a camera takes it, so this write sits on the hot
    /// path of every capture that ever waits. Its guards are what keep a queue from corrupting the
    /// durable record: only a QUEUED row may be rebased (a terminal one has already been decided),
    /// the stage clocks must fit inside the terminal one, and a capture that is not there is not
    /// silently invented.
    #[tokio::test]
    async fn only_a_queued_capture_with_coherent_clocks_may_be_rebased() {
        let directory = TempDir::new().unwrap();
        let catalog = Catalog::open(CatalogOptions::new(directory.path().join("state")))
            .await
            .unwrap();
        let job = direct_job("cap_rebase", "rebase-request");
        let deadlines = job.deadlines.clone();
        let accepted = catalog.accept_job(job).await.unwrap();
        let capture = match accepted {
            AcceptJobOutcome::Inserted(record) | AcceptJobOutcome::Existing(record) => {
                record.capture_id
            }
            AcceptJobOutcome::Conflict => panic!("unexpected conflict"),
        };

        // A capture that does not exist is not invented.
        let missing = catalog
            .reschedule_deadlines("cap_absent", deadlines.clone(), 1)
            .await
            .unwrap_err();
        assert_eq!(missing.code(), crate::ErrorCode::CaptureNotFound);

        // A stage clock may not outlive the terminal clock it is supposed to sit inside.
        let mut incoherent = deadlines.clone();
        incoherent.capture_at_ms = incoherent.terminal_at_ms + 1;
        assert!(
            catalog
                .reschedule_deadlines(capture.clone(), incoherent, 1)
                .await
                .is_err(),
            "a stage deadline past the terminal deadline is a clock that can never be met"
        );

        // ACCEPTED is not QUEUED: a capture is only rebased when it is actually waiting.
        assert!(
            catalog
                .reschedule_deadlines(capture.clone(), deadlines.clone(), 1)
                .await
                .is_err(),
            "only a QUEUED capture may be rebased"
        );

        catalog
            .queue_job(capture.clone(), 1)
            .await
            .expect("the capture reaches the queue");
        let rebased = catalog
            .reschedule_deadlines(capture.clone(), deadlines.clone(), 4_242)
            .await
            .expect("a QUEUED capture rebases onto the moment a camera took it");
        assert_eq!(rebased.capture_id, capture);
    }

    #[tokio::test]
    async fn latest_schedule_occurrence_uses_the_durable_unjittered_key() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        assert_eq!(
            catalog
                .latest_schedule_occurrence("camera-a", "every-minute")
                .await
                .unwrap(),
            None
        );
        catalog
            .accept_scheduled_job(scheduled_job("first", "main"), "every-minute", 1_000)
            .await
            .unwrap();
        catalog
            .accept_scheduled_job(scheduled_job("second", "main"), "every-minute", 3_000)
            .await
            .unwrap();
        catalog
            .accept_scheduled_job(scheduled_job("other", "main"), "other", 9_000)
            .await
            .unwrap();
        assert_eq!(
            catalog
                .latest_schedule_occurrence("camera-a", "every-minute")
                .await
                .unwrap(),
            Some(3_000)
        );
    }

    #[tokio::test]
    async fn hazardous_command_recovery_is_sticky_and_cannot_be_overwritten() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        let key = LedgerKey::new("camera-a", "sb/ptz/absolute", "ptz-1").unwrap();
        let request = json!({"requestId":"ptz-1","pan":0.2,"tilt":0.3});
        let hash = canonical_request_hash(&request, false).unwrap();
        assert!(matches!(
            catalog
                .begin_command(key.clone(), hash, request.clone(), 1_000)
                .await
                .unwrap(),
            BeginCommandOutcome::Started(_)
        ));
        assert_eq!(
            catalog
                .mark_hazardous_commands_outcome_unknown(2_000)
                .await
                .unwrap(),
            1
        );
        let existing = catalog
            .begin_command(key.clone(), hash, request, 3_000)
            .await
            .unwrap();
        let BeginCommandOutcome::Existing(existing) = existing else {
            panic!("unknown result was not retained")
        };
        assert_eq!(existing.state, LedgerState::OutcomeUnknown);
        let still_unknown = catalog
            .complete_command(
                key,
                LedgerState::Succeeded,
                json!({"ok":true}),
                None,
                None,
                4_000,
            )
            .await
            .unwrap();
        assert_eq!(still_unknown.state, LedgerState::OutcomeUnknown);

        let completed_key = LedgerKey::new("camera-a", "sb/ptz/home", "ptz-complete").unwrap();
        let completed_request = json!({"requestId":"ptz-complete"});
        let completed_hash = canonical_request_hash(&completed_request, false).unwrap();
        catalog
            .begin_command(
                completed_key.clone(),
                completed_hash,
                completed_request,
                1_100,
            )
            .await
            .unwrap();
        catalog
            .complete_command(
                completed_key,
                LedgerState::Succeeded,
                json!({"ok":true}),
                None,
                None,
                1_200,
            )
            .await
            .unwrap();
        assert_eq!(
            catalog
                .prune_completed_command_ledgers(5_000, 10)
                .await
                .unwrap(),
            1
        );
        let unknown_again = catalog
            .begin_command(
                LedgerKey::new("camera-a", "sb/ptz/absolute", "ptz-1").unwrap(),
                hash,
                json!({"requestId":"ptz-1","pan":0.2,"tilt":0.3}),
                6_000,
            )
            .await
            .unwrap();
        assert!(matches!(
            unknown_again,
            BeginCommandOutcome::Existing(CommandLedgerRecord {
                state: LedgerState::OutcomeUnknown,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn interrupted_reconnect_ledgers_are_settled_and_become_reclaimable() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        let interrupted = LedgerKey::new("camera-a", RECONNECT_VERB, "reconnect-crash").unwrap();
        let fenced = LedgerKey::new("camera-b", RECONNECT_VERB, "reconnect-legacy").unwrap();
        let request = json!({"requestId":"reconnect","instance":"camera-a"});
        let hash = canonical_request_hash(&request, false).unwrap();
        for key in [interrupted.clone(), fenced.clone()] {
            catalog
                .begin_command(key, hash, request.clone(), 1_000)
                .await
                .unwrap();
        }
        // A state database written by an earlier build already carries a fenced reconnect row.
        assert_eq!(
            catalog
                .mark_hazardous_commands_outcome_unknown(1_500)
                .await
                .unwrap(),
            2
        );

        assert_eq!(
            catalog.settle_interrupted_reconnects(2_000).await.unwrap(),
            2
        );
        for key in [interrupted, fenced] {
            let BeginCommandOutcome::Existing(settled) = catalog
                .begin_command(key, hash, request.clone(), 2_100)
                .await
                .unwrap()
            else {
                panic!("a settled reconnect must remain the same durable operation");
            };
            assert_eq!(
                settled.state,
                LedgerState::Succeeded,
                "a settled reconnect must never answer PREVIOUS_OUTCOME_UNKNOWN"
            );
        }
        // The whole point: an IN_PROGRESS/OUTCOME_UNKNOWN row matches no DELETE in this catalog.
        assert_eq!(
            catalog
                .prune_completed_command_ledgers(3_000, 10)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            catalog.settle_interrupted_reconnects(4_000).await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn in_progress_command_retains_exact_acceptance_reply_for_idempotent_retries() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        let key = LedgerKey::new("camera-a", "sb/reconnect", "reconnect-1").unwrap();
        let request = json!({"requestId":"reconnect-1","instance":"camera-a"});
        let hash = canonical_request_hash(&request, false).unwrap();
        assert!(matches!(
            catalog
                .begin_command(key.clone(), hash, request.clone(), 1_000)
                .await
                .unwrap(),
            BeginCommandOutcome::Started(_)
        ));
        let reply = json!({"operationId":"op_durable","state":"ACCEPTED"});
        let retained = catalog
            .record_command_acceptance(key.clone(), reply.clone(), 1_001)
            .await
            .unwrap();
        assert_eq!(retained.state, LedgerState::InProgress);
        assert_eq!(retained.reply, Some(reply.clone()));
        let BeginCommandOutcome::Existing(retry) = catalog
            .begin_command(key, hash, request, 1_002)
            .await
            .unwrap()
        else {
            panic!("exact retry must return the existing command ledger");
        };
        assert_eq!(retry.reply, Some(reply));
    }

    #[tokio::test]
    async fn waiter_recovery_and_outbox_lifecycle_keep_durable_boundaries() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        let job = direct_job("recovery-outbox", "recovery-outbox");
        let key = job.ledger_key.clone().unwrap();
        assert!(matches!(
            catalog.accept_job(job).await.unwrap(),
            AcceptJobOutcome::Inserted(_)
        ));
        assert!(catalog.job_by_ledger(key).await.unwrap().is_some());
        let waiter = WaiterRecord {
            waiter_id: "waiter-recovery".to_string(),
            capture_id: "recovery-outbox".to_string(),
            correlation_id: "corr-waiter".to_string(),
            request_uuid: Some("uuid-waiter".to_string()),
            expires_at_ms: 2_000,
            created_at_ms: 1_000,
        };
        assert!(catalog.add_waiter(waiter.clone()).await.unwrap());
        assert_eq!(
            catalog.waiters("recovery-outbox", 1_999).await.unwrap(),
            vec![waiter]
        );
        assert!(
            catalog
                .waiters("recovery-outbox", 2_000)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(catalog.remove_waiter("waiter-recovery").await.unwrap());
        assert!(!catalog.remove_waiter("waiter-recovery").await.unwrap());

        assert!(matches!(
            catalog.queue_job("recovery-outbox", 1_100).await.unwrap(),
            StateCasOutcome::Changed(_)
        ));
        assert_eq!(catalog.recovery_jobs().await.unwrap().len(), 1);
        assert!(matches!(
            catalog
                .interrupt_recovered(
                    "recovery-outbox",
                    vec![JobState::Queued],
                    terminal_write("recovery-outbox", JobState::Interrupted, 9),
                )
                .await
                .unwrap(),
            TerminalOutcome::Won(_)
        ));
        assert!(catalog.recovery_jobs().await.unwrap().is_empty());
        assert!(
            catalog
                .interrupt_recovered(
                    "recovery-outbox",
                    vec![JobState::Succeeded],
                    terminal_write("recovery-outbox", JobState::Interrupted, 10),
                )
                .await
                .is_err()
        );
        assert!(catalog.pending_outbox(20_009, 0).await.is_err());
        let outbox = catalog.pending_outbox(20_009, 1).await.unwrap();
        assert_eq!(outbox.len(), 1);
        let message = &outbox[0];
        assert!(
            catalog
                .record_outbox_attempt(message.id, 20_010, 20_020, "broker unavailable")
                .await
                .unwrap()
        );
        assert!(catalog.pending_outbox(20_019, 1).await.unwrap().is_empty());
        let stats = catalog.outbox_stats(20_030).await.unwrap();
        assert_eq!(stats.undelivered, 1);
        assert_eq!(stats.max_attempts, 1);
        assert!(
            catalog
                .mark_outbox_delivered(message.id, 20_030)
                .await
                .unwrap()
        );
        assert!(
            !catalog
                .mark_outbox_delivered(message.id, 20_031)
                .await
                .unwrap()
        );
        assert_eq!(catalog.prune_delivered_outbox(20_031, 1).await.unwrap(), 1);
        assert!(catalog.prune_delivered_outbox(20_031, 0).await.is_err());
    }

    #[tokio::test]
    async fn concurrent_terminal_writers_commit_one_state_and_one_exact_envelope() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        catalog
            .accept_job(direct_job("terminal-race", "terminal-race"))
            .await
            .unwrap();
        let exact_write = terminal_write("terminal-race", JobState::Succeeded, 0);
        move_to_persisting(
            &catalog,
            "terminal-race",
            "cd".repeat(32),
            4_096,
            exact_write.clone(),
        )
        .await;
        catalog
            .try_begin_install("terminal-race", 6_000)
            .await
            .unwrap();
        catalog
            .record_installed_artifact("terminal-race", "cd".repeat(32), 4_096, 7_000)
            .await
            .unwrap();
        let mut tasks = Vec::new();
        for sequence in 0..32_u8 {
            let catalog = catalog.clone();
            let write = exact_write.clone();
            tasks.push(tokio::spawn(async move {
                let expected_bytes = write.outbox.encoded_envelope.clone();
                let expected_uuid = write.outbox.envelope_uuid.clone();
                (
                    sequence,
                    expected_bytes,
                    expected_uuid,
                    catalog
                        .commit_terminal("terminal-race", write)
                        .await
                        .unwrap(),
                )
            }));
        }
        let mut winner = None;
        for task in tasks {
            let (sequence, expected_bytes, expected_uuid, outcome) = task.await.unwrap();
            match outcome {
                TerminalOutcome::Won(_) => {
                    assert!(
                        winner
                            .replace((sequence, expected_bytes, expected_uuid))
                            .is_none()
                    )
                }
                TerminalOutcome::AlreadyTerminal(record) => {
                    assert_eq!(record.state, JobState::Succeeded)
                }
                TerminalOutcome::InstallationWon(_) => {
                    panic!("installation does not arbitrate success")
                }
            }
        }
        let (_winner, expected_bytes, expected_uuid) = winner.expect("no terminal writer won");
        let pending = catalog.pending_outbox(i64::MAX, 10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].encoded_envelope, expected_bytes);
        assert_eq!(pending[0].envelope_uuid, expected_uuid);
        assert_eq!(pending[0].event_key, "terminal-event-0");
        assert_eq!(catalog.outbox_stats(30_000).await.unwrap().undelivered, 1);
    }

    #[tokio::test]
    async fn cancellation_and_installation_use_one_transactionally_ordered_cas() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        for sequence in 40..56_u8 {
            let capture_id = format!("install-race-{sequence}");
            let request_id = format!("install-request-{sequence}");
            catalog
                .accept_job(direct_job(&capture_id, &request_id))
                .await
                .unwrap();
            move_to_persisting(
                &catalog,
                &capture_id,
                "00".repeat(32),
                3,
                terminal_write(&capture_id, JobState::Succeeded, sequence),
            )
            .await;
            let barrier = Arc::new(Barrier::new(3));
            let install_catalog = catalog.clone();
            let install_id = capture_id.clone();
            let install_barrier = Arc::clone(&barrier);
            let install = tokio::spawn(async move {
                install_barrier.wait().await;
                install_catalog
                    .try_begin_install(install_id, 6_000)
                    .await
                    .unwrap()
            });
            let cancel_catalog = catalog.clone();
            let cancel_id = capture_id.clone();
            let cancel_barrier = Arc::clone(&barrier);
            let cancel = tokio::spawn(async move {
                cancel_barrier.wait().await;
                cancel_catalog
                    .cancel_job(
                        cancel_id.clone(),
                        terminal_write(&cancel_id, JobState::Cancelled, sequence),
                    )
                    .await
                    .unwrap()
            });
            barrier.wait().await;
            let install = install.await.unwrap();
            let cancel = cancel.await.unwrap();
            match (install, cancel) {
                (InstallOutcome::Started(record), TerminalOutcome::InstallationWon(observed)) => {
                    assert_eq!(record.state, JobState::Persisting);
                    assert!(record.install_started);
                    assert!(record.pending_success.is_some());
                    assert_eq!(observed.state, JobState::Persisting);
                }
                (InstallOutcome::WrongState(record), TerminalOutcome::Won(cancelled)) => {
                    assert_eq!(record.state, JobState::Cancelled);
                    assert_eq!(cancelled.state, JobState::Cancelled);
                    assert!(!cancelled.install_started);
                    assert!(cancelled.pending_success.is_none());
                    assert!(cancelled.expected_sha256.is_none());
                    assert!(cancelled.expected_bytes.is_none());
                }
                other => panic!("invalid install/cancel arbitration: {other:?}"),
            }
        }
        let terminal_count = catalog.pending_outbox(i64::MAX, 100).await.unwrap().len();
        let cancelled_count = catalog
            .execute(|connection| {
                Ok(connection.query_row(
                    "SELECT COUNT(*) FROM jobs WHERE state='CANCELLED'",
                    [],
                    |row| row.get::<_, i64>(0),
                )? as usize)
            })
            .await
            .unwrap();
        assert_eq!(terminal_count, cancelled_count);
    }

    #[tokio::test]
    async fn persisting_recovery_has_exact_paths_and_verified_installed_facts() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        catalog
            .accept_job(direct_job("persist-paths", "persist-paths"))
            .await
            .unwrap();
        let pending_write = terminal_write("persist-paths", JobState::Succeeded, 78);
        move_to_persisting(
            &catalog,
            "persist-paths",
            "ab".repeat(32),
            12_345,
            pending_write.clone(),
        )
        .await;
        let recovery = catalog.recovery_jobs().await.unwrap();
        let record = recovery
            .iter()
            .find(|record| record.capture_id == "persist-paths")
            .unwrap();
        assert_eq!(
            record.partial_path.as_deref(),
            Some("C:/captures/persist-paths.partial")
        );
        assert_eq!(
            record.final_path.as_deref(),
            Some("C:/captures/persist-paths.jpg")
        );
        assert_eq!(
            record.expected_sha256.as_deref(),
            Some("ab".repeat(32).as_str())
        );
        assert_eq!(record.expected_bytes, Some(12_345));
        assert_eq!(record.pending_success.as_ref(), Some(&pending_write));
        assert!(matches!(
            catalog
                .try_begin_install("persist-paths", 6_000)
                .await
                .unwrap(),
            InstallOutcome::Started(_)
        ));
        let installed = catalog
            .record_installed_artifact("persist-paths", "ab".repeat(32), 12_345, 7_000)
            .await
            .unwrap();
        assert_eq!(
            installed.installed_sha256.as_deref(),
            Some("ab".repeat(32).as_str())
        );
        assert_eq!(installed.installed_bytes, Some(12_345));
        assert_eq!(installed.pending_success.as_ref(), Some(&pending_write));
        assert!(matches!(
            catalog
                .commit_terminal("persist-paths", pending_write,)
                .await
                .unwrap(),
            TerminalOutcome::Won(_)
        ));
        let terminal = catalog.job("persist-paths").await.unwrap().unwrap();
        assert!(terminal.pending_success.is_none());
    }

    /// Every supported starting version must reach the current one, with the tables it owes.
    ///
    /// The 1 -> N path was the only one covered, which is the path a brand-new migration arm is
    /// least likely to break: a catalog already on 2 or 3 takes a different arm entirely, and an arm
    /// that forgets to chain the newest migration ships a database missing a table nobody notices
    /// until a schedule tries to write to it.
    #[test]
    fn every_supported_schema_version_migrates_to_the_current_one() {
        for start in 1..SCHEMA_VERSION {
            let directory = TempDir::new().unwrap();
            let path = directory.path().join(format!("v{start}.sqlite3"));
            let mut connection = Connection::open(&path).unwrap();
            connection
                .execute_batch("CREATE TABLE jobs(capture_id TEXT PRIMARY KEY) STRICT;")
                .unwrap();
            connection
                .pragma_update(None, "user_version", start)
                .unwrap();
            migrate(&mut connection).unwrap();
            let version: i64 = connection
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, SCHEMA_VERSION, "migrating from v{start}");
            let cursors: String = connection
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type='table' \
                     AND name='group_schedule_cursors'",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or_else(|_| panic!("v{start} did not gain group_schedule_cursors"));
            assert_eq!(cursors, "group_schedule_cursors");
        }
    }

    #[test]
    fn schema_v3_migration_is_monotonic_and_transactional() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("v1.sqlite3");
        let mut connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE jobs(capture_id TEXT PRIMARY KEY) STRICT; PRAGMA user_version=1;",
            )
            .unwrap();
        migrate(&mut connection).unwrap();
        let version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let columns = connection
            .prepare("PRAGMA table_info(jobs)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(columns.contains(&"expected_sha256".to_string()));
        assert!(columns.contains(&"expected_bytes".to_string()));
        let pending_table: String = connection
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='pending_success'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pending_table, "pending_success");
        let probe_table: String = connection
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='catalog_availability_probe'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(probe_table, "catalog_availability_probe");

        let broken_path = directory.path().join("broken-v1.sqlite3");
        let mut broken = Connection::open(broken_path).unwrap();
        broken.pragma_update(None, "user_version", 1).unwrap();
        assert!(migrate(&mut broken).is_err());
        let version: i64 = broken
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 1, "failed migration must not advance user_version");
    }

    #[tokio::test]
    async fn recovery_requires_supplied_exact_bytes_and_retries_them_after_restart() {
        let directory = TempDir::new().unwrap();
        let config = options(&directory);
        let catalog = Catalog::open(config.clone()).await.unwrap();
        catalog
            .accept_job(direct_job("crash-accepted", "crash-accepted"))
            .await
            .unwrap();
        let recovery = catalog.recovery_jobs().await.unwrap();
        assert_eq!(recovery.len(), 1);
        assert_eq!(recovery[0].state, JobState::Accepted);
        assert!(
            catalog
                .pending_outbox(i64::MAX, 10)
                .await
                .unwrap()
                .is_empty()
        );

        let exact = terminal_write("crash-accepted", JobState::Interrupted, 77);
        assert!(matches!(
            catalog
                .interrupt_recovered("crash-accepted", vec![JobState::Accepted], exact.clone())
                .await
                .unwrap(),
            TerminalOutcome::Won(_)
        ));
        let pending = catalog.pending_outbox(i64::MAX, 10).await.unwrap();
        assert_eq!(pending[0].encoded_envelope, exact.outbox.encoded_envelope);
        catalog
            .record_outbox_attempt(pending[0].id, 30_000, 31_000, "temporary publish failure")
            .await
            .unwrap();
        drop(pending);
        drop(catalog);

        let reopened = reopen_after_workers_exit(config).await;
        let pending = reopened.pending_outbox(i64::MAX, 10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].encoded_envelope, exact.outbox.encoded_envelope);
        assert_eq!(pending[0].attempts, 1);
        assert!(reopened.recovery_jobs().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn retention_never_prunes_undelivered_outbox_or_its_terminal_job() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        for (capture, request, sequence) in
            [("delivered", "delivered", 81), ("pending", "pending", 82)]
        {
            catalog
                .accept_job(direct_job(capture, request))
                .await
                .unwrap();
            catalog.queue_job(capture, 2_000).await.unwrap();
            catalog
                .commit_terminal(capture, terminal_write(capture, JobState::Failed, sequence))
                .await
                .unwrap();
        }
        let pending = catalog.pending_outbox(i64::MAX, 10).await.unwrap();
        let delivered_id = pending
            .iter()
            .find(|record| record.capture_id.as_deref() == Some("delivered"))
            .unwrap()
            .id;
        catalog
            .mark_outbox_delivered(delivered_id, 30_000)
            .await
            .unwrap();
        assert_eq!(catalog.prune_delivered_outbox(40_000, 10).await.unwrap(), 1);
        assert_eq!(catalog.prune_terminal_jobs(40_000, 10).await.unwrap(), 1);
        assert!(catalog.job("delivered").await.unwrap().is_none());
        assert!(catalog.job("pending").await.unwrap().is_some());
        assert_eq!(
            catalog.prune_delivered_outbox(i64::MAX, 10).await.unwrap(),
            0
        );
        let remaining = catalog.pending_outbox(i64::MAX, 10).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].capture_id.as_deref(), Some("pending"));
    }

    #[tokio::test]
    async fn count_retention_deletes_only_the_oldest_eligible_terminal_records() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        for (capture, sequence) in [("oldest", 90), ("middle", 91), ("newest", 92)] {
            catalog
                .accept_job(direct_job(capture, capture))
                .await
                .unwrap();
            catalog.queue_job(capture, 2_000).await.unwrap();
            catalog
                .commit_terminal(capture, terminal_write(capture, JobState::Failed, sequence))
                .await
                .unwrap();
        }
        let outbox = catalog.pending_outbox(i64::MAX, 10).await.unwrap();
        for record in outbox {
            catalog
                .mark_outbox_delivered(record.id, 40_000)
                .await
                .unwrap();
        }
        assert_eq!(
            catalog.prune_delivered_outbox(i64::MAX, 10).await.unwrap(),
            3
        );
        assert_eq!(catalog.enforce_result_record_limit(1, 10).await.unwrap(), 2);
        assert!(catalog.job("oldest").await.unwrap().is_none());
        assert!(catalog.job("middle").await.unwrap().is_none());
        assert!(catalog.job("newest").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn count_retention_below_the_maximum_reclaims_nothing() {
        let directory = TempDir::new().unwrap();
        let catalog = open(&directory).await;
        for (capture, sequence) in [("kept-a", 95), ("kept-b", 96)] {
            catalog
                .accept_job(direct_job(capture, capture))
                .await
                .unwrap();
            catalog.queue_job(capture, 2_000).await.unwrap();
            catalog
                .commit_terminal(capture, terminal_write(capture, JobState::Failed, sequence))
                .await
                .unwrap();
        }
        for record in catalog.pending_outbox(i64::MAX, 10).await.unwrap() {
            catalog
                .mark_outbox_delivered(record.id, 40_000)
                .await
                .unwrap();
        }
        assert_eq!(
            catalog.prune_delivered_outbox(i64::MAX, 10).await.unwrap(),
            2
        );

        // A negative excess must never be handed to SQLite as a LIMIT: it reads a negative limit
        // as unbounded, which would delete every retained terminal record instead of none.
        assert_eq!(
            catalog
                .enforce_result_record_limit(100_000, 10)
                .await
                .unwrap(),
            0
        );
        assert!(catalog.job("kept-a").await.unwrap().is_some());
        assert!(catalog.job("kept-b").await.unwrap().is_some());
        assert_eq!(catalog.enforce_result_record_limit(2, 10).await.unwrap(), 0);
        assert!(catalog.job("kept-a").await.unwrap().is_some());
    }

    /// The write-ahead log and shared-memory file move WITH the database they belong to.
    ///
    /// Leaving a stale `-wal` beside the fresh database would be worse than useless: SQLite would try
    /// to recover the NEW file from the OLD file's log. (In practice SQLite usually reclaims its own log
    /// when the failed connection closes, which is exactly why this is tested here, directly, rather
    /// than left to whether that happened to occur.)
    #[test]
    fn quarantine_takes_the_sidecars_with_it() {
        let directory = TempDir::new().unwrap();
        let database = directory.path().join(DATABASE_NAME);
        fs::write(&database, b"corrupt").unwrap();
        fs::write(directory.path().join("camera-adapter.sqlite3-wal"), b"log").unwrap();
        fs::write(directory.path().join("camera-adapter.sqlite3-shm"), b"shm").unwrap();

        quarantine_corrupt_catalog(&database, "not a database").unwrap();

        assert!(!database.exists(), "the corrupt database is moved aside");
        assert!(
            !directory.path().join("camera-adapter.sqlite3-wal").exists(),
            "a stale log left behind would be recovered INTO the new database"
        );
        assert!(!directory.path().join("camera-adapter.sqlite3-shm").exists());

        assert_eq!(
            fs::read(directory.path().join("camera-adapter.sqlite3.corrupt")).unwrap(),
            b"corrupt"
        );
        assert_eq!(
            fs::read(directory.path().join("camera-adapter.sqlite3.corrupt-wal")).unwrap(),
            b"log"
        );
        assert_eq!(
            fs::read(directory.path().join("camera-adapter.sqlite3.corrupt-shm")).unwrap(),
            b"shm"
        );
    }

    /// A corrupt catalog is moved aside and the component runs. A DOWNGRADE is not touched.
    ///
    /// These two used to be one behaviour -- fail closed, preserve the evidence, never run again -- and
    /// on a Greengrass edge box that made a bad flush on a power cut into a camera adapter that stopped
    /// capturing FOREVER, until a human was dispatched. The evidence it so carefully preserved was of no
    /// use to anyone not standing next to it. Availability wins: the file is quarantined, not deleted,
    /// and the component starts on a clean catalog.
    ///
    /// But a database written by a NEWER component is not damaged. Its rows are intact and the version
    /// that wrote them can still read them. Quarantining THAT would throw away good, unpublished results
    /// to work around a rollback -- so it stays fail-closed, and this test is what keeps the two apart.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn a_corrupt_catalog_is_quarantined_and_a_downgrade_is_still_refused() {
        let corrupt_directory = TempDir::new().unwrap();
        let corrupt_options = options(&corrupt_directory);
        fs::create_dir_all(&corrupt_options.state_directory).unwrap();
        let corrupt_path = corrupt_options.state_directory.join(DATABASE_NAME);
        let corrupt_bytes = b"not a sqlite database; this is the evidence";
        fs::write(&corrupt_path, corrupt_bytes).unwrap();

        let catalog = Catalog::open(corrupt_options)
            .await
            .expect("a corrupt catalog must not stop the component from running");

        // It runs, and it is usable.
        assert!(catalog.job("nothing-here").await.unwrap().is_none());

        // The evidence is beside it, not destroyed.
        let quarantined = corrupt_directory
            .path()
            .join("state")
            .join("camera-adapter.sqlite3.corrupt");
        assert_eq!(
            fs::read(&quarantined).unwrap(),
            corrupt_bytes,
            "the corrupt file must be preserved for whoever eventually looks"
        );

        // A schema version from the FUTURE is a downgrade, not corruption. Hands off.
        let future_directory = TempDir::new().unwrap();
        let future_options = options(&future_directory);
        fs::create_dir_all(&future_options.state_directory).unwrap();
        let future_path = future_options.state_directory.join(DATABASE_NAME);
        let connection = Connection::open(&future_path).unwrap();
        connection.pragma_update(None, "user_version", 999).unwrap();
        drop(connection);
        assert!(
            Catalog::open(future_options).await.is_err(),
            "a database written by a newer component is intact; refusing to run is the right answer"
        );
        let connection = Connection::open(&future_path).unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            999,
            "and it must be left exactly as it was found"
        );
        assert!(
            !future_path.with_extension("sqlite3.corrupt").exists(),
            "a downgrade must never be quarantined -- that would delete good rows to work around a \
             rollback"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn protected_dacl_rejects_corrupt_and_future_catalog_before_use() {
        let corrupt_directory = TempDir::new().unwrap();
        let corrupt_options = options(&corrupt_directory);
        fs::create_dir_all(&corrupt_options.state_directory).unwrap();
        fs::write(
            corrupt_options.state_directory.join(DATABASE_NAME),
            b"not a sqlite database; retain this evidence",
        )
        .unwrap();
        assert!(Catalog::open(corrupt_options).await.is_err());

        let future_directory = TempDir::new().unwrap();
        let future_options = options(&future_directory);
        fs::create_dir_all(&future_options.state_directory).unwrap();
        let future_path = future_options.state_directory.join(DATABASE_NAME);
        let connection = Connection::open(&future_path).unwrap();
        connection.pragma_update(None, "user_version", 999).unwrap();
        drop(connection);
        assert!(Catalog::open(future_options).await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_state_and_database_modes_are_private() {
        use std::os::unix::fs::PermissionsExt;
        let directory = TempDir::new().unwrap();
        let config = options(&directory);
        let catalog = Catalog::open(config.clone()).await.unwrap();
        assert_eq!(
            fs::metadata(&config.state_directory)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(catalog.database_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(config.state_directory.join(LOCK_NAME))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
