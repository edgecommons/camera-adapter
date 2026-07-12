//! Protocol-neutral durable capture-job execution.
//!
//! Acceptance, actor dispatch, admission, backend acquisition, bounded encoding/persistence,
//! installation arbitration, terminal outbox commit, waiter settlement, and group aggregation are
//! joined here without depending on a concrete camera protocol or runtime supervisor.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use edgecommons::facades::AppFacade;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::admission::{
    AdmissionController, CapturePriority, CaptureResourceRequest, EncoderPermit, ProcessingLease,
    WriterPermit,
};
use crate::backend::{CameraSession, CaptureRequest};
use crate::catalog::{
    AcceptJobOutcome, Catalog, JobDeadlines, JobRecord, NewJob, NewOutboxMessage, PendingInstall,
    StateCasOutcome, TerminalOutcome, TerminalWrite,
};
use crate::config::{CaptureInterlock, CaptureProfile, OfflinePolicy};
use crate::encoding::EncodingRequest;
use crate::messages::{
    CameraSummary, CaptureDurations, CaptureTimestamps, CaptureTrigger, FailureSummary,
    FrameSummary, ImageArtifact, TERMINAL_SCHEMA_VERSION, TerminalBody, TerminalKind,
    TerminalMessage,
};
use crate::model::{
    CaptureFrame, CaptureMode, FrameTimestampQuality, JobState, PixelFormat, PtzRequest, PtzResult,
};
use crate::storage::{
    InstallGate, PrepareCapture, RecoveryOutcome, RecoveryRequest, RelativeOutputPath,
    StorageReservation, StorageRoot,
};
use crate::{CameraError, ErrorCode, Result};

const MAX_TERMINAL_DETAIL_BYTES: usize = 1_024;

/// Fully resolved capture profile stored immutably with an accepted job.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobProfileSnapshot {
    /// Caller-visible configured profile name.
    pub name: String,
    /// Backend-facing immutable profile.
    pub capture: CaptureProfile,
    /// Resolved offline behavior; no runtime default lookup is permitted after acceptance.
    pub offline_policy: OfflinePolicy,
    /// Resolved source/output reservation ceiling.
    pub maximum_frame_bytes: u64,
    /// Resolved acquisition mode used for pre-frame failure messages.
    pub capture_mode: CaptureMode,
    /// Immutable capture/PTZ interlock resolved at acceptance.
    pub capture_interlock: CaptureInterlock,
    /// Immutable settle delay for `stopAndSettle`.
    pub settle_ms: u64,
}

/// Immutable runtime material paired with one durable [`NewJob`].
#[derive(Debug, Clone)]
pub struct CaptureJobSpec {
    /// Durable capture ID.
    pub capture_id: String,
    /// Camera instance.
    pub instance: String,
    /// Immutable resolved profile.
    pub profile: JobProfileSnapshot,
    /// Optional shared transport admission group.
    pub resource_group: Option<String>,
    /// Validated output-root-relative final path.
    pub relative_path: RelativeOutputPath,
    /// Absolute durable stage deadlines.
    pub deadlines: JobDeadlines,
    /// Durable acceptance timestamp.
    pub accepted_at_ms: i64,
    /// Durable trigger.
    pub trigger: CaptureTrigger,
    /// Creator correlation, or the generated schedule correlation.
    pub correlation_id: String,
    /// Bounded caller metadata.
    pub metadata: Map<String, Value>,
    /// Immutable camera/capability summary available at acceptance.
    pub camera: CameraSummary,
    /// Group size when this is a group member.
    pub group_size: Option<usize>,
}

/// New durable job plus its actor priority and immutable execution material.
pub struct JobSubmission {
    /// Catalog acceptance material.
    pub job: NewJob,
    /// Runtime snapshot that must exactly match the catalog material.
    pub spec: CaptureJobSpec,
    /// Admission priority determined by the originating verb.
    pub priority: CapturePriority,
}

/// Reservation for one actor descriptor slot.
pub trait DispatchReservation: Send {
    /// Makes a durably queued descriptor visible to the actor and returns a bounded, one-based
    /// queue-position snapshot taken at commitment.
    fn commit(self: Box<Self>, descriptor: CaptureDescriptor) -> Result<usize>;
}

/// Bounded actor-dispatch seam used before the first catalog commit.
pub trait CaptureDispatcher: Send + Sync {
    /// Reserves capacity without exposing any descriptor to an actor.
    fn reserve(&self) -> Result<Box<dyn DispatchReservation>>;
}

/// Exact EdgeCommons terminal-envelope encoder.
pub trait TerminalEnvelopeEncoder: Send + Sync {
    /// Stamps and serializes one terminal message for durable outbox insertion.
    fn encode(&self, message: &TerminalMessage, created_at_ms: i64) -> Result<NewOutboxMessage>;
}

/// Production encoder backed by the guarded EdgeCommons `app()` facade.
pub struct AppTerminalEnvelopeEncoder {
    app: Arc<AppFacade>,
}

impl AppTerminalEnvelopeEncoder {
    /// Binds terminal encoding to one camera-instance application facade.
    #[must_use]
    pub fn new(app: Arc<AppFacade>) -> Self {
        Self { app }
    }
}

impl TerminalEnvelopeEncoder for AppTerminalEnvelopeEncoder {
    fn encode(&self, message: &TerminalMessage, created_at_ms: i64) -> Result<NewOutboxMessage> {
        let prepared = message.prepare(&self.app)?;
        Ok(NewOutboxMessage::from_prepared(
            message.body().event_id.clone(),
            "terminal",
            &prepared,
            created_at_ms,
            created_at_ms,
        ))
    }
}

/// Post-commit integration hooks. Implementations must be idempotent by waiter/member identity.
#[async_trait]
pub trait JobHooks: Send + Sync {
    /// Observes a capture after durable queueing and dispatcher commitment both succeeded.
    ///
    /// Implementations are diagnostic only: an observation failure must never alter the accepted
    /// capture's durable state or prevent its actor from running.
    async fn capture_queued(
        &self,
        _record: &JobRecord,
        _spec: &CaptureJobSpec,
        _queue_position: usize,
    ) {
    }

    /// Observes a capture after it durably enters `ACQUIRING`.
    ///
    /// Implementations are diagnostic only: an observation failure must never alter the durable
    /// capture outcome.
    async fn capture_started(&self, _spec: &CaptureJobSpec) {}

    /// Settles active deferred waiters after the durable terminal transaction wins.
    async fn settle_waiters(&self, _record: &JobRecord, _terminal_body: &Value) {}

    /// Offers a terminal group member to the aggregate-completion coordinator.
    async fn group_member_terminal(&self, _record: &JobRecord, _terminal_body: &Value) {}
}

/// Runs after durable `ACCEPTED` insertion and before the record can become `QUEUED` or visible
/// to a camera actor.  It is the only safe seam for attaching a deferred command waiter.
#[async_trait]
pub trait AcceptanceHook: Send + Sync {
    /// Persists/activates any command-specific acceptance material for this newly created job.
    async fn accepted_before_queue(&self, record: &JobRecord) -> Result<()>;
}

/// Default acceptance hook for submitted and scheduled work.
#[derive(Debug, Default)]
pub struct NoopAcceptanceHook;

#[async_trait]
impl AcceptanceHook for NoopAcceptanceHook {
    async fn accepted_before_queue(&self, _record: &JobRecord) -> Result<()> {
        Ok(())
    }
}

/// No-op hooks for submitted/scheduled-only runtimes and focused tests.
#[derive(Debug, Default)]
pub struct NoopJobHooks;

#[async_trait]
impl JobHooks for NoopJobHooks {}

/// Supervisor-owned camera-availability policy seam.
#[async_trait]
pub trait AvailabilityGate: Send + Sync {
    /// Waits or rejects according to the immutable offline policy and deadlines.
    async fn wait_until_ready(
        &self,
        instance: &str,
        policy: OfflinePolicy,
        queue_deadline_ms: Option<i64>,
        terminal_deadline_ms: i64,
        cancellation: &CancellationToken,
    ) -> Result<()>;
}

/// Availability gate for an actor that already owns a connected session.
#[derive(Debug, Default)]
pub struct ConnectedAvailability;

#[async_trait]
impl AvailabilityGate for ConnectedAvailability {
    async fn wait_until_ready(
        &self,
        _instance: &str,
        _policy: OfflinePolicy,
        _queue_deadline_ms: Option<i64>,
        _terminal_deadline_ms: i64,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if cancellation.is_cancelled() {
            Err(cancelled_error("while waiting for camera availability"))
        } else {
            Ok(())
        }
    }
}

/// Result of a durable cancellation attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct CancelResult {
    /// Whether this request won cancellation arbitration.
    pub cancelled: bool,
    /// Authoritative observed durable state.
    pub state: JobState,
    /// Whether a best-effort backend cancellation may still be unwinding.
    pub cancellation_in_progress: bool,
}

/// Actor-visible descriptor. Construction is restricted to [`JobEngine`].
#[derive(Clone)]
pub struct CaptureDescriptor {
    runtime: Arc<JobRuntime>,
    priority: CapturePriority,
}

impl CaptureDescriptor {
    /// Durable capture ID.
    #[must_use]
    pub fn capture_id(&self) -> &str {
        &self.runtime.spec.capture_id
    }

    /// Target camera instance.
    #[must_use]
    pub fn instance(&self) -> &str {
        &self.runtime.spec.instance
    }

    /// Actor priority.
    #[must_use]
    pub const fn priority(&self) -> CapturePriority {
        self.priority
    }

    /// Absolute terminal deadline converted to the local monotonic clock.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        instant_for_epoch(self.runtime.spec.deadlines.terminal_at_ms)
    }

    /// Cancellation watched by queue admission and backend work.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.runtime.cancellation.clone()
    }
}

struct JobRuntime {
    spec: Arc<CaptureJobSpec>,
    cancellation: CancellationToken,
    done: CancellationToken,
    trace: Mutex<ExecutionTrace>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobStage {
    Queued,
    Acquiring,
    Encoding,
    Persisting,
}

impl JobStage {
    const fn token(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Acquiring => "ACQUIRING",
            Self::Encoding => "ENCODING",
            Self::Persisting => "PERSISTING",
        }
    }
}

#[derive(Clone)]
struct FrameFacts {
    summary: FrameSummary,
    capture_mode: CaptureMode,
    source_timestamp: Option<DateTime<Utc>>,
    timestamp_quality: FrameTimestampQuality,
    backend_metadata: BTreeMap<String, Value>,
}

#[derive(Clone)]
struct ExecutionTrace {
    stage: JobStage,
    acquisition_started_at_ms: Option<i64>,
    frame_received_at_ms: Option<i64>,
    persisting_at_ms: Option<i64>,
    persisted_at_ms: Option<i64>,
    frame: Option<FrameFacts>,
    camera: CameraSummary,
}

struct SuccessFacts {
    image: ImageArtifact,
}

#[derive(Clone)]
struct ActiveJob {
    runtime: Arc<JobRuntime>,
}

/// Durable protocol-neutral capture engine.
#[derive(Clone)]
pub struct JobEngine {
    catalog: Catalog,
    admission: AdmissionController,
    storage: StorageRoot,
    envelopes: Arc<dyn TerminalEnvelopeEncoder>,
    hooks: Arc<dyn JobHooks>,
    availability: Arc<dyn AvailabilityGate>,
    install_gate: Arc<dyn InstallGate>,
    acceptance_hook: Arc<dyn AcceptanceHook>,
    active: Arc<Mutex<HashMap<String, ActiveJob>>>,
}

impl JobEngine {
    /// Creates an engine with connected-session availability and catalog installation arbitration.
    #[must_use]
    pub fn new(
        catalog: Catalog,
        admission: AdmissionController,
        storage: StorageRoot,
        envelopes: Arc<dyn TerminalEnvelopeEncoder>,
        hooks: Arc<dyn JobHooks>,
    ) -> Self {
        Self {
            install_gate: Arc::new(catalog.clone()),
            catalog,
            admission,
            storage,
            envelopes,
            hooks,
            availability: Arc::new(ConnectedAvailability),
            acceptance_hook: Arc::new(NoopAcceptanceHook),
            active: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Replaces the supervisor availability seam.
    #[must_use]
    pub fn with_availability(mut self, availability: Arc<dyn AvailabilityGate>) -> Self {
        self.availability = availability;
        self
    }

    /// Replaces installation arbitration, primarily for deterministic crash/race testing.
    #[must_use]
    pub fn with_install_gate(mut self, install_gate: Arc<dyn InstallGate>) -> Self {
        self.install_gate = install_gate;
        self
    }

    /// Installs the one pre-queue acceptance hook used by deferred command routing.
    #[must_use]
    pub fn with_acceptance_hook(mut self, acceptance_hook: Arc<dyn AcceptanceHook>) -> Self {
        self.acceptance_hook = acceptance_hook;
        self
    }

    /// Reserves actor capacity, commits `ACCEPTED`, commits `QUEUED`, then exposes the descriptor.
    pub async fn accept_and_queue(
        &self,
        dispatcher: &dyn CaptureDispatcher,
        submission: JobSubmission,
    ) -> Result<AcceptJobOutcome> {
        validate_submission(&submission)?;
        let reservation = dispatcher.reserve()?;
        let outcome = self.catalog.accept_job(submission.job.clone()).await?;
        let AcceptJobOutcome::Inserted(_) = outcome else {
            return Ok(outcome);
        };

        let accepted = self
            .catalog
            .job(&submission.job.capture_id)
            .await?
            .ok_or_else(|| {
                CameraError::Catalog("accepted capture disappeared before queueing".to_string())
            })?;
        self.acceptance_hook
            .accepted_before_queue(&accepted)
            .await?;

        let queued = match self
            .catalog
            .queue_job(&submission.job.capture_id, now_ms())
            .await?
        {
            StateCasOutcome::Changed(record) => record,
            StateCasOutcome::NotChanged(record) => return Ok(AcceptJobOutcome::Existing(record)),
        };
        let initial_camera = submission.spec.camera.clone();
        let runtime = Arc::new(JobRuntime {
            spec: Arc::new(submission.spec),
            cancellation: CancellationToken::new(),
            done: CancellationToken::new(),
            trace: Mutex::new(ExecutionTrace {
                stage: JobStage::Queued,
                acquisition_started_at_ms: None,
                frame_received_at_ms: None,
                persisting_at_ms: None,
                persisted_at_ms: None,
                frame: None,
                camera: initial_camera,
            }),
        });
        {
            let mut active = lock(&self.active);
            if active
                .insert(
                    queued.capture_id.clone(),
                    ActiveJob {
                        runtime: Arc::clone(&runtime),
                    },
                )
                .is_some()
            {
                return Err(CameraError::Catalog(
                    "capture runtime was registered more than once".to_string(),
                ));
            }
        }
        let descriptor = CaptureDescriptor {
            runtime: Arc::clone(&runtime),
            priority: submission.priority,
        };
        let queue_position = match reservation.commit(descriptor) {
            Ok(position) => position,
            Err(error) => {
                let _ = self
                    .finish_failure(
                        &runtime,
                        ErrorCode::QueueFull,
                        "durably queued job could not enter its reserved actor slot",
                    )
                    .await;
                return Err(error);
            }
        };
        self.spawn_terminal_deadline(Arc::clone(&runtime));
        self.hooks
            .capture_queued(&queued, runtime.spec.as_ref(), queue_position)
            .await;
        Ok(AcceptJobOutcome::Inserted(queued))
    }

    /// Queues a member that was already accepted as part of an atomically committed capture
    /// group.  Group acceptance is intentionally owned by the catalog so no member can become
    /// visible while another member fails its durable insert; this method performs only the second
    /// (queue/descriptor) phase for one of those accepted rows.
    pub async fn queue_preaccepted(
        &self,
        dispatcher: &dyn CaptureDispatcher,
        submission: JobSubmission,
    ) -> Result<JobRecord> {
        validate_submission(&submission)?;
        let reservation = dispatcher.reserve()?;
        let accepted = self
            .catalog
            .job(&submission.job.capture_id)
            .await?
            .ok_or_else(|| {
                CameraError::Catalog(
                    "accepted group capture disappeared before queueing".to_string(),
                )
            })?;
        if !matches!(accepted.state, JobState::Accepted | JobState::Queued) {
            return Err(CameraError::Catalog(
                "group member must be ACCEPTED or QUEUED before its queue phase".to_string(),
            ));
        }
        let queued = if accepted.state == JobState::Queued {
            accepted
        } else {
            match self
                .catalog
                .queue_job(&submission.job.capture_id, now_ms())
                .await?
            {
                StateCasOutcome::Changed(record) => record,
                StateCasOutcome::NotChanged(record) => return Ok(record),
            }
        };
        let initial_camera = submission.spec.camera.clone();
        let runtime = Arc::new(JobRuntime {
            spec: Arc::new(submission.spec),
            cancellation: CancellationToken::new(),
            done: CancellationToken::new(),
            trace: Mutex::new(ExecutionTrace {
                stage: JobStage::Queued,
                acquisition_started_at_ms: None,
                frame_received_at_ms: None,
                persisting_at_ms: None,
                persisted_at_ms: None,
                frame: None,
                camera: initial_camera,
            }),
        });
        {
            let mut active = lock(&self.active);
            if active
                .insert(
                    queued.capture_id.clone(),
                    ActiveJob {
                        runtime: Arc::clone(&runtime),
                    },
                )
                .is_some()
            {
                return Err(CameraError::Catalog(
                    "group capture runtime was registered more than once".to_string(),
                ));
            }
        }
        let descriptor = CaptureDescriptor {
            runtime: Arc::clone(&runtime),
            priority: submission.priority,
        };
        let queue_position = match reservation.commit(descriptor) {
            Ok(position) => position,
            Err(error) => {
                let _ = self
                    .finish_failure(
                        &runtime,
                        ErrorCode::QueueFull,
                        "durably queued group job could not enter its reserved actor slot",
                    )
                    .await;
                return Err(error);
            }
        };
        self.spawn_terminal_deadline(Arc::clone(&runtime));
        self.hooks
            .capture_queued(&queued, runtime.spec.as_ref(), queue_position)
            .await;
        Ok(queued)
    }

    /// Lets cancellation race installation through the catalog CAS.
    pub async fn cancel_active(
        &self,
        capture_id: &str,
        reason: impl Into<String>,
    ) -> Result<CancelResult> {
        let runtime = lock(&self.active)
            .get(capture_id)
            .map(|active| Arc::clone(&active.runtime));
        let Some(runtime) = runtime else {
            let record = self.catalog.job(capture_id).await?.ok_or_else(|| {
                CameraError::rejected(ErrorCode::CaptureNotFound, "capture does not exist")
            })?;
            if record.state.is_terminal() {
                return Ok(CancelResult {
                    cancelled: false,
                    state: record.state,
                    cancellation_in_progress: false,
                });
            }
            return Err(CameraError::rejected(
                ErrorCode::CameraUnavailable,
                "capture is owned by recovery or an unavailable actor",
            ));
        };
        let reason = bounded_detail(reason.into());
        let stage = lock(&runtime.trace).stage;
        let (write, body) = self.make_terminal_write(
            &runtime,
            JobState::Cancelled,
            None,
            Some((ErrorCode::CaptureCancelled, reason.clone())),
            now_ms(),
            None,
        )?;
        match self.catalog.cancel_job(capture_id, write).await? {
            TerminalOutcome::Won(record) => {
                runtime.cancellation.cancel();
                self.after_terminal(&runtime, &record, &body).await;
                Ok(CancelResult {
                    cancelled: true,
                    state: record.state,
                    cancellation_in_progress: stage == JobStage::Acquiring,
                })
            }
            TerminalOutcome::AlreadyTerminal(record) => {
                self.complete_runtime(&runtime);
                Ok(CancelResult {
                    cancelled: false,
                    state: record.state,
                    cancellation_in_progress: false,
                })
            }
            TerminalOutcome::InstallationWon(record) => Ok(CancelResult {
                cancelled: false,
                state: record.state,
                cancellation_in_progress: false,
            }),
        }
    }

    /// Completes one `PERSISTING/install_started=true` record from its durable expected artifact
    /// facts and exact staged success envelope.
    ///
    /// Startup orchestration remains responsible for enumerating recovery records and for building
    /// a failure envelope when no valid artifact remains.
    pub async fn recover_install_started(
        &self,
        record: JobRecord,
        cancellation: &CancellationToken,
    ) -> Result<JobRecord> {
        if record.state != JobState::Persisting || !record.install_started {
            return Err(CameraError::Catalog(
                "install recovery requires PERSISTING with install_started=true".to_string(),
            ));
        }
        let expected_sha256 = record.expected_sha256.clone().ok_or_else(|| {
            CameraError::Catalog("recovery record lacks expected sha256".to_string())
        })?;
        let expected_bytes = record.expected_bytes.ok_or_else(|| {
            CameraError::Catalog("recovery record lacks expected byte count".to_string())
        })?;
        let pending = record.pending_success.clone().ok_or_else(|| {
            CameraError::Catalog(
                "recovery record lacks its exact pending success write".to_string(),
            )
        })?;
        let relative_wire = record
            .intended_output
            .get("relativePath")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                CameraError::Catalog("recovery record lacks relativePath".to_string())
            })?;
        let relative_path = RelativeOutputPath::from_stored(relative_wire)?;
        let sidecar_body = pending
            .result
            .get("image")
            .and_then(|image| image.get("metadataSidecarRelativePath"))
            .is_some_and(|value| !value.is_null())
            .then(|| pending.result.clone());
        let request = RecoveryRequest {
            capture_id: record.capture_id.clone(),
            relative_path,
            expected_bytes,
            expected_sha256: expected_sha256.clone(),
            sidecar_body,
        };
        let writer = self
            .admission
            .acquire_writer(Instant::now() + Duration::from_secs(30), cancellation)
            .await?;
        let storage = self.storage.clone();
        let recovery_cancellation = cancellation.clone();
        let recovery = tokio::task::spawn_blocking(move || {
            let _writer = writer;
            storage.reconcile_install_started(request, &recovery_cancellation)
        })
        .await
        .map_err(|error| CameraError::Storage(format!("recovery worker failed: {error}")))??;
        match recovery {
            RecoveryOutcome::AlreadyInstalled | RecoveryOutcome::InstalledFromPartial => {}
            RecoveryOutcome::MissingArtifactsCleaned => {
                return Err(CameraError::Storage(
                    "no verified artifact remained; recovery must commit a failure envelope"
                        .to_string(),
                ));
            }
        }
        let recorded = self
            .catalog
            .record_installed_artifact(
                &record.capture_id,
                expected_sha256,
                expected_bytes,
                now_ms(),
            )
            .await?;
        let exact = recorded.pending_success.ok_or_else(|| {
            CameraError::Catalog("recovery lost its staged terminal write".to_string())
        })?;
        if exact != pending {
            return Err(CameraError::Catalog(
                "reloaded staged terminal write changed during recovery".to_string(),
            ));
        }
        let body = exact.result.clone();
        let outcome = self
            .catalog
            .commit_terminal(&record.capture_id, exact)
            .await?;
        let terminal = match outcome {
            TerminalOutcome::Won(record) | TerminalOutcome::AlreadyTerminal(record) => record,
            TerminalOutcome::InstallationWon(_) => {
                return Err(CameraError::Catalog(
                    "success recovery unexpectedly observed cancellation arbitration".to_string(),
                ));
            }
        };
        self.hooks.settle_waiters(&terminal, &body).await;
        if terminal.group_id.is_some() {
            self.hooks.group_member_terminal(&terminal, &body).await;
        }
        Ok(terminal)
    }

    /// Turns a non-terminal record left by a previous process into one exact durable
    /// `PROCESS_INTERRUPTED` terminal result.  This is deliberately a catalog CAS rather than
    /// a best-effort log entry: restart must never silently abandon accepted work.
    pub async fn interrupt_recovered(&self, record: JobRecord) -> Result<JobRecord> {
        self.interrupt_nonterminal(record, "capture was interrupted by process restart")
            .await
    }

    /// Turns a queued record made incompatible by an accepted configuration replacement into
    /// one exact durable `PROCESS_INTERRUPTED` terminal result.  It never replays or silently
    /// drops camera work from the old backend/profile contract.
    pub async fn interrupt_for_reload(&self, record: JobRecord) -> Result<JobRecord> {
        self.interrupt_nonterminal(record, "capture was interrupted by configuration reload")
            .await
    }

    async fn interrupt_nonterminal(
        &self,
        record: JobRecord,
        message: &'static str,
    ) -> Result<JobRecord> {
        if record.state.is_terminal() || record.install_started {
            return Err(CameraError::Catalog(
                "interruption recovery requires a non-terminal, non-install-owned job".to_string(),
            ));
        }
        let profile: JobProfileSnapshot = serde_json::from_value(record.effective_profile.clone())
            .map_err(|_| {
                CameraError::Catalog("recovery record has an invalid effective profile".to_string())
            })?;
        let trigger: CaptureTrigger =
            serde_json::from_value(record.trigger.clone()).map_err(|_| {
                CameraError::Catalog("recovery record has an invalid capture trigger".to_string())
            })?;
        let metadata = record
            .canonical_request
            .get("metadata")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| {
                CameraError::Catalog("recovery record has invalid capture metadata".to_string())
            })?
            .unwrap_or_default();
        let backend = record
            .intended_output
            .get("backend")
            .and_then(Value::as_str)
            .and_then(|value| serde_json::from_value(Value::String(value.to_owned())).ok())
            .ok_or_else(|| {
                CameraError::Catalog("recovery record lacks a valid backend kind".to_string())
            })?;
        let relative_path = record
            .intended_output
            .get("relativePath")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                CameraError::Catalog("recovery record lacks relativePath".to_string())
            })?;
        let group_size = record.group_id.as_ref().map(|_| 1_usize);
        let spec = CaptureJobSpec {
            capture_id: record.capture_id.clone(),
            instance: record.instance.clone(),
            profile,
            resource_group: None,
            relative_path: RelativeOutputPath::from_stored(relative_path)?,
            deadlines: record.deadlines.clone(),
            accepted_at_ms: record.accepted_at_ms,
            trigger,
            correlation_id: record
                .origin_correlation_id
                .clone()
                .unwrap_or_else(|| record.capture_id.clone()),
            metadata,
            camera: CameraSummary {
                backend,
                vendor: None,
                model: None,
                firmware: None,
                serial: None,
            },
            group_size,
        };
        let runtime = Arc::new(JobRuntime {
            spec: Arc::new(spec),
            cancellation: CancellationToken::new(),
            done: CancellationToken::new(),
            trace: Mutex::new(ExecutionTrace {
                stage: JobStage::Queued,
                acquisition_started_at_ms: None,
                frame_received_at_ms: None,
                persisting_at_ms: None,
                persisted_at_ms: None,
                frame: None,
                camera: CameraSummary {
                    backend,
                    vendor: None,
                    model: None,
                    firmware: None,
                    serial: None,
                },
            }),
        });
        let terminal_at_ms = now_ms();
        let (write, body) = self.make_terminal_write(
            &runtime,
            JobState::Interrupted,
            None,
            Some((ErrorCode::ProcessInterrupted, message.to_string())),
            terminal_at_ms,
            None,
        )?;
        let expected = vec![record.state];
        match self
            .catalog
            .interrupt_recovered(&record.capture_id, expected, write)
            .await?
        {
            TerminalOutcome::Won(terminal) | TerminalOutcome::AlreadyTerminal(terminal) => {
                self.hooks.settle_waiters(&terminal, &body).await;
                if terminal.group_id.is_some() {
                    self.hooks.group_member_terminal(&terminal, &body).await;
                }
                Ok(terminal)
            }
            TerminalOutcome::InstallationWon(_) => Err(CameraError::Catalog(
                "interruption recovery lost to installation arbitration".to_string(),
            )),
        }
    }

    /// Executes one descriptor against the actor-owned session.
    pub(crate) async fn execute(
        &self,
        session: &mut dyn CameraSession,
        descriptor: CaptureDescriptor,
    ) -> Result<JobRecord> {
        let runtime = descriptor.runtime;
        self.update_camera_from_session(&runtime, session);

        if let Err(error) = self
            .await_with_deadline(
                runtime.spec.deadlines.terminal_at_ms,
                &runtime.cancellation,
                self.availability.wait_until_ready(
                    &runtime.spec.instance,
                    runtime.spec.profile.offline_policy,
                    runtime.spec.deadlines.queue_at_ms,
                    runtime.spec.deadlines.terminal_at_ms,
                    &runtime.cancellation,
                ),
                "camera availability",
            )
            .await
        {
            return self.finish_error(&runtime, error).await;
        }

        if let Err(error) = self.enforce_capture_interlock(session, &runtime).await {
            return self.finish_error(&runtime, error).await;
        }

        let acquisition = match self
            .admission
            .acquire_capture(
                CaptureResourceRequest {
                    resource_group: runtime.spec.resource_group.clone(),
                    maximum_frame_bytes: runtime.spec.profile.maximum_frame_bytes,
                    deadline: instant_for_epoch(runtime.spec.deadlines.capture_at_ms),
                },
                &runtime.cancellation,
            )
            .await
        {
            Ok(lease) => lease,
            Err(error) => return self.finish_error(&runtime, error).await,
        };
        match self
            .catalog
            .compare_and_set_state(
                &runtime.spec.capture_id,
                JobState::Queued,
                JobState::Acquiring,
                now_ms(),
            )
            .await?
        {
            StateCasOutcome::Changed(_) => {
                {
                    let mut trace = lock(&runtime.trace);
                    trace.stage = JobStage::Acquiring;
                    trace.acquisition_started_at_ms = Some(now_ms());
                }
                self.hooks.capture_started(runtime.spec.as_ref()).await;
            }
            StateCasOutcome::NotChanged(record) if record.state.is_terminal() => {
                self.complete_runtime(&runtime);
                return Ok(record);
            }
            StateCasOutcome::NotChanged(record) => {
                return Err(CameraError::Catalog(format!(
                    "capture actor expected QUEUED, found {:?}",
                    record.state
                )));
            }
        }

        let capture = session.capture(CaptureRequest {
            capture_id: runtime.spec.capture_id.clone(),
            profile: runtime.spec.profile.capture.clone(),
            maximum_frame_bytes: runtime.spec.profile.maximum_frame_bytes,
            timeout: remaining_duration(runtime.spec.deadlines.capture_at_ms),
            cancellation: runtime.cancellation.clone(),
        });
        let frame = match self
            .await_with_deadline(
                runtime.spec.deadlines.capture_at_ms,
                &runtime.cancellation,
                capture,
                "backend acquisition",
            )
            .await
        {
            Ok(frame) => frame,
            Err(error) => return self.finish_error(&runtime, error).await,
        };
        let received_at_ms = now_ms();
        let frame_facts = frame_facts(&frame);
        {
            let mut trace = lock(&runtime.trace);
            trace.frame_received_at_ms = Some(received_at_ms);
            trace.frame = Some(frame_facts);
        }
        let processing = match acquisition.finish_acquisition(frame.bytes.len() as u64) {
            Ok(lease) => lease,
            Err(error) => return self.finish_error(&runtime, error).await,
        };

        let requires_encoding = !matches!(
            runtime.spec.profile.capture.output.encoding,
            crate::model::OutputEncoding::Raw | crate::model::OutputEncoding::Passthrough
        );
        let encoder = if requires_encoding {
            match self
                .catalog
                .compare_and_set_state(
                    &runtime.spec.capture_id,
                    JobState::Acquiring,
                    JobState::Encoding,
                    now_ms(),
                )
                .await?
            {
                StateCasOutcome::Changed(_) => lock(&runtime.trace).stage = JobStage::Encoding,
                StateCasOutcome::NotChanged(record) if record.state.is_terminal() => {
                    self.complete_runtime(&runtime);
                    return Ok(record);
                }
                StateCasOutcome::NotChanged(record) => {
                    return Err(CameraError::Catalog(format!(
                        "capture actor expected ACQUIRING, found {:?}",
                        record.state
                    )));
                }
            }
            match self
                .admission
                .acquire_encoder(
                    instant_for_epoch(runtime.spec.deadlines.encode_at_ms),
                    &runtime.cancellation,
                )
                .await
            {
                Ok(permit) => Some(permit),
                Err(error) => return self.finish_error(&runtime, error).await,
            }
        } else {
            None
        };
        let writer = match self
            .admission
            .acquire_writer(
                instant_for_epoch(runtime.spec.deadlines.persist_at_ms),
                &runtime.cancellation,
            )
            .await
        {
            Ok(permit) => permit,
            Err(error) => return self.finish_error(&runtime, error).await,
        };

        let work_deadline = if requires_encoding {
            runtime
                .spec
                .deadlines
                .encode_at_ms
                .min(runtime.spec.deadlines.persist_at_ms)
        } else {
            runtime.spec.deadlines.persist_at_ms
        };
        let prepared = match self
            .prepare_blocking(&runtime, frame, processing, encoder, writer, work_deadline)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => return self.finish_error(&runtime, error).await,
        };

        {
            let mut trace = lock(&runtime.trace);
            trace.stage = JobStage::Persisting;
            trace.persisting_at_ms = Some(now_ms());
        }

        let success = SuccessFacts {
            image: prepared.artifact().clone(),
        };
        // Sidecar-first visibility requires its body before the final image can be linked. Sample
        // one logical installation-transaction timestamp immediately at that boundary and reuse
        // its exact body for sidecar and outbox. The live trace is not marked persisted until the
        // install, parent sync, verification, and installed-artifact catalog write all succeed.
        let terminal_at_ms = now_ms();
        let (write, body) = self.make_terminal_write(
            &runtime,
            JobState::Succeeded,
            Some(&success),
            None,
            terminal_at_ms,
            Some(terminal_at_ms),
        )?;
        match self
            .catalog
            .begin_persisting(
                &runtime.spec.capture_id,
                PendingInstall {
                    partial_path: prepared.partial_path().to_owned(),
                    final_path: prepared.artifact().absolute_path.clone(),
                    expected_sha256: prepared.artifact().sha256.clone(),
                    expected_bytes: prepared.artifact().bytes,
                    success: write,
                    changed_at_ms: now_ms(),
                },
            )
            .await?
        {
            StateCasOutcome::Changed(_) => {}
            StateCasOutcome::NotChanged(record) if record.state.is_terminal() => {
                self.complete_runtime(&runtime);
                return Ok(record);
            }
            StateCasOutcome::NotChanged(record) => {
                return Err(CameraError::Catalog(format!(
                    "capture actor could not enter PERSISTING from {:?}",
                    record.state
                )));
            }
        }
        let sidecar = success
            .image
            .metadata_sidecar_relative_path
            .as_ref()
            .map(|_| &body);
        let installed = match prepared
            .install(
                self.install_gate.as_ref(),
                terminal_at_ms,
                sidecar,
                &runtime.cancellation,
            )
            .await
        {
            Ok(artifact) => artifact,
            Err(error) => return self.finish_error(&runtime, error).await,
        };
        let recorded = self
            .catalog
            .record_installed_artifact(
                &runtime.spec.capture_id,
                installed.sha256.clone(),
                installed.bytes,
                now_ms(),
            )
            .await?;
        lock(&runtime.trace).persisted_at_ms = Some(terminal_at_ms);
        let pending = recorded.pending_success.ok_or_else(|| {
            CameraError::Catalog(
                "installed artifact lost its durably staged success write".to_string(),
            )
        })?;
        let committed_body = pending.result.clone();
        let outcome = self
            .catalog
            .commit_terminal(&runtime.spec.capture_id, pending)
            .await?;
        self.finish_terminal_outcome(&runtime, outcome, &committed_body)
            .await
    }

    async fn enforce_capture_interlock(
        &self,
        session: &mut dyn CameraSession,
        runtime: &Arc<JobRuntime>,
    ) -> Result<()> {
        let policy = runtime.spec.profile.capture_interlock;
        if policy == CaptureInterlock::Allow {
            return Ok(());
        }
        let mut status_available = true;
        let moving = match self
            .capture_interlock_ptz(session, runtime, PtzRequest::Status)
            .await
        {
            Ok(PtzResult::Status(status)) => status.moving.unwrap_or(false),
            Ok(_) => {
                return Err(CameraError::rejected(
                    ErrorCode::CameraMoving,
                    "PTZ status response was invalid",
                ));
            }
            Err(error) if error.code() == ErrorCode::UnsupportedCapability => {
                status_available = false;
                false
            }
            Err(error) => return Err(error),
        };
        if policy == CaptureInterlock::Reject {
            return if moving {
                Err(CameraError::rejected(
                    ErrorCode::CameraMoving,
                    "camera is moving",
                ))
            } else {
                Ok(())
            };
        }
        if !moving && status_available {
            return Ok(());
        }
        match self
            .capture_interlock_ptz(
                session,
                runtime,
                PtzRequest::Stop {
                    pan: true,
                    tilt: true,
                    zoom: true,
                },
            )
            .await?
        {
            PtzResult::Commanded => {}
            _ => {
                return Err(CameraError::rejected(
                    ErrorCode::CameraMoving,
                    "camera did not acknowledge PTZ stop",
                ));
            }
        }
        let deadline = instant_for_epoch(runtime.spec.deadlines.capture_at_ms);
        if status_available {
            loop {
                if Instant::now() >= deadline {
                    return Err(CameraError::rejected(
                        ErrorCode::CameraMoving,
                        "camera did not become idle before capture deadline",
                    ));
                }
                match self
                    .capture_interlock_ptz(session, runtime, PtzRequest::Status)
                    .await?
                {
                    PtzResult::Status(status) if status.moving != Some(true) => break,
                    PtzResult::Status(_) => {
                        self.await_with_deadline(
                            runtime.spec.deadlines.capture_at_ms,
                            &runtime.cancellation,
                            async {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                Ok(())
                            },
                            "PTZ interlock",
                        )
                        .await?;
                    }
                    _ => {
                        return Err(CameraError::rejected(
                            ErrorCode::CameraMoving,
                            "PTZ status response was invalid",
                        ));
                    }
                }
            }
        }
        self.await_with_deadline(
            runtime.spec.deadlines.capture_at_ms,
            &runtime.cancellation,
            async {
                tokio::time::sleep(Duration::from_millis(runtime.spec.profile.settle_ms)).await;
                Ok(())
            },
            "PTZ settle",
        )
        .await
    }

    async fn capture_interlock_ptz(
        &self,
        session: &mut dyn CameraSession,
        runtime: &Arc<JobRuntime>,
        request: PtzRequest,
    ) -> Result<PtzResult> {
        let deadline = instant_for_epoch(runtime.spec.deadlines.capture_at_ms);
        match session
            .ptz_bounded(request, deadline, &runtime.cancellation)
            .await
        {
            Err(error) if error.code() == ErrorCode::PtzTimeout => Err(CameraError::rejected(
                ErrorCode::CameraMoving,
                "PTZ operation did not complete before the capture deadline",
            )),
            result => result,
        }
    }

    /// Converts a caught actor/session panic into one durable terminal failure.
    pub(crate) async fn fail_panic(&self, descriptor: &CaptureDescriptor) -> Result<JobRecord> {
        self.finish_failure(
            &descriptor.runtime,
            ErrorCode::BackendError,
            "camera actor isolated a backend panic",
        )
        .await
    }

    async fn prepare_blocking(
        &self,
        runtime: &Arc<JobRuntime>,
        frame: CaptureFrame,
        processing: ProcessingLease,
        encoder: Option<EncoderPermit>,
        writer: WriterPermit,
        deadline_ms: i64,
    ) -> Result<crate::storage::PreparedInstall> {
        let current = processing.reserved_disk_bytes();
        let other = self
            .admission
            .outstanding_disk_bytes()
            .saturating_sub(current);
        let request = PrepareCapture {
            capture_id: runtime.spec.capture_id.clone(),
            relative_path: runtime.spec.relative_path.clone(),
            frame,
            encoding: EncodingRequest {
                encoding: runtime.spec.profile.capture.output.encoding,
                jpeg_quality: runtime.spec.profile.capture.output.jpeg_quality,
                maximum_output_bytes: runtime.spec.profile.maximum_frame_bytes,
            },
            reservation: StorageReservation {
                current_bytes: current,
                other_bytes: other,
            },
        };
        let storage = self.storage.clone();
        let cancellation = runtime.cancellation.clone();
        let task = tokio::task::spawn_blocking(move || -> Result<_> {
            let _encoder = encoder;
            let _writer = writer;
            let mut processing = processing;
            let prepared = storage.prepare_capture(request, &cancellation)?;
            processing.release_memory();
            processing.shrink_disk(prepared.artifact().bytes)?;
            Ok((prepared, processing))
        });
        tokio::pin!(task);
        tokio::select! {
            biased;
            _ = runtime.cancellation.cancelled() => Err(cancelled_error("during encoding/persistence")),
            _ = tokio::time::sleep_until(instant_for_epoch(deadline_ms)) => {
                runtime.cancellation.cancel();
                Err(timeout_error("encoding/persistence"))
            }
            result = &mut task => {
                let (prepared, _processing) = result
                    .map_err(|error| CameraError::Storage(format!("bounded persistence worker failed: {error}")))??;
                Ok(prepared)
            }
        }
    }

    async fn finish_error(
        &self,
        runtime: &Arc<JobRuntime>,
        error: CameraError,
    ) -> Result<JobRecord> {
        if error.code() == ErrorCode::CaptureCancelled {
            return self
                .cancel_runtime(runtime, bounded_detail(error.to_string()))
                .await;
        }
        self.finish_failure(runtime, error.code(), bounded_detail(error.to_string()))
            .await
    }

    async fn finish_failure(
        &self,
        runtime: &Arc<JobRuntime>,
        code: ErrorCode,
        message: impl Into<String>,
    ) -> Result<JobRecord> {
        if let Some(record) = self.catalog.job(&runtime.spec.capture_id).await? {
            if record.state == JobState::Persisting && record.install_started {
                // Once installation wins, a generic worker/deadline/panic path must not guess the
                // terminal artifact outcome. Preserve the durable record for targeted recovery.
                self.complete_runtime(runtime);
                return Ok(record);
            }
        }
        let detail = bounded_detail(message.into());
        let (write, body) = self.make_terminal_write(
            runtime,
            JobState::Failed,
            None,
            Some((code, detail)),
            now_ms(),
            None,
        )?;
        let outcome = self
            .catalog
            .commit_terminal(&runtime.spec.capture_id, write)
            .await?;
        let record = self
            .finish_terminal_outcome(runtime, outcome, &body)
            .await?;
        runtime.cancellation.cancel();
        Ok(record)
    }

    async fn cancel_runtime(&self, runtime: &Arc<JobRuntime>, reason: String) -> Result<JobRecord> {
        let (write, body) = self.make_terminal_write(
            runtime,
            JobState::Cancelled,
            None,
            Some((ErrorCode::CaptureCancelled, reason)),
            now_ms(),
            None,
        )?;
        match self
            .catalog
            .cancel_job(&runtime.spec.capture_id, write)
            .await?
        {
            TerminalOutcome::Won(record) => {
                runtime.cancellation.cancel();
                self.after_terminal(runtime, &record, &body).await;
                Ok(record)
            }
            TerminalOutcome::AlreadyTerminal(record) => {
                self.complete_runtime(runtime);
                Ok(record)
            }
            TerminalOutcome::InstallationWon(record) => Ok(record),
        }
    }

    async fn finish_terminal_outcome(
        &self,
        runtime: &Arc<JobRuntime>,
        outcome: TerminalOutcome,
        body: &Value,
    ) -> Result<JobRecord> {
        match outcome {
            TerminalOutcome::Won(record) => {
                self.after_terminal(runtime, &record, body).await;
                Ok(record)
            }
            TerminalOutcome::AlreadyTerminal(record) => {
                self.complete_runtime(runtime);
                Ok(record)
            }
            TerminalOutcome::InstallationWon(record) => Ok(record),
        }
    }

    async fn after_terminal(&self, runtime: &Arc<JobRuntime>, record: &JobRecord, body: &Value) {
        self.complete_runtime(runtime);
        self.hooks.settle_waiters(record, body).await;
        if record.group_id.is_some() {
            self.hooks.group_member_terminal(record, body).await;
        }
    }

    fn complete_runtime(&self, runtime: &Arc<JobRuntime>) {
        runtime.done.cancel();
        lock(&self.active).remove(&runtime.spec.capture_id);
    }

    fn spawn_terminal_deadline(&self, runtime: Arc<JobRuntime>) {
        let engine = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = runtime.done.cancelled() => {}
                _ = tokio::time::sleep_until(instant_for_epoch(runtime.spec.deadlines.terminal_at_ms)) => {
                    let _ = engine.finish_failure(
                        &runtime,
                        ErrorCode::CaptureTimeout,
                        "capture exceeded its terminal deadline",
                    ).await;
                }
            }
        });
    }

    fn update_camera_from_session(&self, runtime: &Arc<JobRuntime>, session: &dyn CameraSession) {
        let capabilities = session.capabilities();
        let mut trace = lock(&runtime.trace);
        trace.camera.vendor.clone_from(&capabilities.vendor);
        trace.camera.model.clone_from(&capabilities.model);
        trace.camera.firmware.clone_from(&capabilities.firmware);
        trace.camera.serial.clone_from(&capabilities.serial);
    }

    fn make_terminal_write(
        &self,
        runtime: &Arc<JobRuntime>,
        state: JobState,
        success: Option<&SuccessFacts>,
        error: Option<(ErrorCode, String)>,
        terminal_at_ms: i64,
        persisted_at_override_ms: Option<i64>,
    ) -> Result<(TerminalWrite, Value)> {
        let trace = lock(&runtime.trace).clone();
        let frame = trace.frame.as_ref();
        let failure = if state == JobState::Failed || state == JobState::Interrupted {
            let (code, message) = error.clone().ok_or_else(|| {
                CameraError::Catalog("failed terminal result lacks an error".to_string())
            })?;
            Some(FailureSummary {
                code,
                stage: trace.stage.token().to_string(),
                retriable: is_retriable(code),
                message,
            })
        } else {
            None
        };
        let (capture_group_id, group_size) = match &runtime.spec.trigger {
            CaptureTrigger::GroupCommand {
                capture_group_id, ..
            } => (Some(capture_group_id.clone()), runtime.spec.group_size),
            _ => (None, None),
        };
        let body = TerminalBody {
            schema_version: TERMINAL_SCHEMA_VERSION,
            event_id: TerminalMessage::new_event_id(),
            capture_id: runtime.spec.capture_id.clone(),
            camera_id: runtime.spec.instance.clone(),
            correlation_id: runtime.spec.correlation_id.clone(),
            trigger: runtime.spec.trigger.clone(),
            capture_profile: runtime.spec.profile.name.clone(),
            capture_mode: frame.map_or(runtime.spec.profile.capture_mode, |facts| {
                facts.capture_mode
            }),
            timestamps: CaptureTimestamps {
                requested_at: datetime(runtime.spec.accepted_at_ms)?,
                acquisition_started_at: optional_datetime(trace.acquisition_started_at_ms)?,
                camera_frame_at: frame.and_then(|facts| facts.source_timestamp),
                frame_received_at: optional_datetime(trace.frame_received_at_ms)?,
                persisted_at: optional_datetime(
                    persisted_at_override_ms.or(trace.persisted_at_ms),
                )?,
                camera_frame_timestamp_quality: frame
                    .map_or(FrameTimestampQuality::Unknown, |facts| {
                        facts.timestamp_quality
                    }),
            },
            durations_ms: durations(&runtime.spec, &trace, terminal_at_ms),
            image: success.map(|facts| facts.image.clone()),
            frame: frame.map(|facts| facts.summary.clone()),
            camera: trace.camera,
            metadata: runtime.spec.metadata.clone(),
            failure,
            capture_group_id,
            group_size,
            backend_metadata: frame
                .map_or_else(BTreeMap::new, |facts| facts.backend_metadata.clone()),
        };
        let kind = match state {
            JobState::Succeeded => TerminalKind::Captured,
            JobState::Cancelled => TerminalKind::Cancelled,
            JobState::Failed | JobState::Interrupted => TerminalKind::Failed,
            _ => {
                return Err(CameraError::Catalog(
                    "terminal message requested for nonterminal state".to_string(),
                ));
            }
        };
        let message = TerminalMessage::new(kind, body)?;
        let value = message.body_value()?;
        let outbox = self.envelopes.encode(&message, terminal_at_ms)?;
        let retained_error = error.map(|(code, message)| (code.as_str().to_string(), message));
        Ok((
            TerminalWrite {
                state,
                result: value.clone(),
                error_code: retained_error.as_ref().map(|(code, _)| code.clone()),
                error_message: retained_error.map(|(_, message)| message),
                terminal_at_ms,
                outbox,
            },
            value,
        ))
    }

    async fn await_with_deadline<T, F>(
        &self,
        deadline_ms: i64,
        cancellation: &CancellationToken,
        future: F,
        stage: &'static str,
    ) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        tokio::pin!(future);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(cancelled_error(stage)),
            _ = tokio::time::sleep_until(instant_for_epoch(deadline_ms)) => Err(timeout_error(stage)),
            result = &mut future => result,
        }
    }
}

fn validate_submission(submission: &JobSubmission) -> Result<()> {
    let job = &submission.job;
    let spec = &submission.spec;
    if job.capture_id != spec.capture_id
        || job.instance != spec.instance
        || job.deadlines != spec.deadlines
        || job.accepted_at_ms != spec.accepted_at_ms
    {
        return Err(CameraError::Catalog(
            "runtime job identity/deadlines differ from durable acceptance material".to_string(),
        ));
    }
    if spec.capture_id.is_empty()
        || spec.instance.is_empty()
        || spec.profile.name.is_empty()
        || spec.correlation_id.is_empty()
        || spec.profile.maximum_frame_bytes == 0
    {
        return Err(CameraError::rejected(
            ErrorCode::InvalidRequest,
            "immutable capture snapshot contains an empty/zero required field",
        ));
    }
    if serde_json::to_value(&spec.profile)? != job.effective_profile
        || serde_json::to_value(&spec.trigger)? != job.trigger
    {
        return Err(CameraError::Catalog(
            "runtime profile/trigger differs from the immutable catalog snapshot".to_string(),
        ));
    }
    if job
        .origin_correlation_id
        .as_deref()
        .is_some_and(|correlation| correlation != spec.correlation_id)
    {
        return Err(CameraError::Catalog(
            "runtime creator correlation differs from the durable job".to_string(),
        ));
    }
    if job
        .intended_output
        .get("relativePath")
        .and_then(Value::as_str)
        != Some(spec.relative_path.as_wire_path())
    {
        return Err(CameraError::Catalog(
            "runtime output path differs from the durable intended output".to_string(),
        ));
    }
    match (&spec.trigger, &job.group_id, spec.group_size) {
        (
            CaptureTrigger::GroupCommand {
                capture_group_id, ..
            },
            Some(group_id),
            Some(size),
        ) if capture_group_id == group_id && size >= 2 => {}
        (CaptureTrigger::GroupCommand { .. }, _, _) => {
            return Err(CameraError::Catalog(
                "group runtime snapshot is inconsistent".to_string(),
            ));
        }
        (_, None, None) => {}
        _ => {
            return Err(CameraError::Catalog(
                "non-group runtime snapshot carries group material".to_string(),
            ));
        }
    }
    Ok(())
}

fn frame_facts(frame: &CaptureFrame) -> FrameFacts {
    FrameFacts {
        summary: FrameSummary {
            width: frame.width,
            height: frame.height,
            pixel_format: frame.pixel_format,
            source_encoding: match frame.pixel_format {
                PixelFormat::Jpeg => "jpeg",
                PixelFormat::Mono8 | PixelFormat::Rgb8 | PixelFormat::Bgr8 => "raw",
            }
            .to_string(),
        },
        capture_mode: frame.capture_mode,
        source_timestamp: frame.source_timestamp,
        timestamp_quality: frame.timestamp_quality,
        backend_metadata: frame.backend_metadata.clone(),
    }
}

fn durations(
    spec: &CaptureJobSpec,
    trace: &ExecutionTrace,
    terminal_at_ms: i64,
) -> CaptureDurations {
    CaptureDurations {
        queue: trace
            .acquisition_started_at_ms
            .map(|time| elapsed_ms(spec.accepted_at_ms, time)),
        acquisition: trace
            .acquisition_started_at_ms
            .zip(trace.frame_received_at_ms)
            .map(|(start, end)| elapsed_ms(start, end)),
        encoding: trace
            .frame_received_at_ms
            .zip(trace.persisting_at_ms)
            .map(|(start, end)| elapsed_ms(start, end)),
        persistence: trace
            .persisting_at_ms
            .map(|start| elapsed_ms(start, terminal_at_ms)),
        total: elapsed_ms(spec.accepted_at_ms, terminal_at_ms),
    }
}

fn elapsed_ms(start: i64, end: i64) -> u64 {
    u64::try_from(end.saturating_sub(start)).unwrap_or(0)
}

fn datetime(milliseconds: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(milliseconds).ok_or_else(|| {
        CameraError::Catalog(format!(
            "timestamp {milliseconds} is outside chrono's domain"
        ))
    })
}

fn optional_datetime(milliseconds: Option<i64>) -> Result<Option<DateTime<Utc>>> {
    milliseconds.map(datetime).transpose()
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn instant_for_epoch(deadline_ms: i64) -> Instant {
    Instant::now() + remaining_duration(deadline_ms)
}

fn remaining_duration(deadline_ms: i64) -> Duration {
    Duration::from_millis(u64::try_from(deadline_ms.saturating_sub(now_ms())).unwrap_or(0))
}

fn timeout_error(stage: &'static str) -> CameraError {
    CameraError::rejected(
        ErrorCode::CaptureTimeout,
        format!("capture timed out during {stage}"),
    )
}

fn cancelled_error(stage: &'static str) -> CameraError {
    CameraError::rejected(
        ErrorCode::CaptureCancelled,
        format!("capture cancelled {stage}"),
    )
}

fn is_retriable(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::CameraUnavailable
            | ErrorCode::QueueFull
            | ErrorCode::ResourceLimit
            | ErrorCode::CaptureTimeout
            | ErrorCode::StoragePressure
            | ErrorCode::PersistenceFailed
            | ErrorCode::BackendError
    )
}

fn bounded_detail(value: String) -> String {
    let mut output = String::with_capacity(value.len().min(MAX_TERMINAL_DETAIL_BYTES));
    for character in value.chars() {
        let character = if character.is_control() {
            ' '
        } else {
            character
        };
        if output.len() + character.len_utf8() > MAX_TERMINAL_DETAIL_BYTES {
            break;
        }
        output.push(character);
    }
    output
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use edgecommons::messaging::MessageBuilder;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::admission::FilesystemSpaceProbe;
    use crate::backend::{CameraSession, CameraStatus, CaptureRequest};
    use crate::catalog::{CatalogOptions, LedgerKey};
    use crate::config::ProfileOutputConfig;
    use crate::idempotency::RequestHash;
    use crate::messages::TERMINAL_ENVELOPE_VERSION;
    use crate::model::OutputEncoding;
    use crate::storage::StorageRoot;

    fn test_capabilities() -> crate::model::CameraCapabilities {
        crate::model::CameraCapabilities {
            capture_modes: vec![CaptureMode::Simulated],
            pixel_formats: vec![PixelFormat::Rgb8],
            software_trigger: false,
            snapshot_uri: false,
            rtsp: false,
            ptz: true,
            ptz_status: true,
            presets: false,
            preset_mutation: false,
            vendor: None,
            model: None,
            firmware: None,
            serial: None,
            warnings: Vec::new(),
        }
    }

    struct HungPtzSession {
        capabilities: crate::model::CameraCapabilities,
        calls: Arc<Mutex<Vec<PtzRequest>>>,
    }

    #[async_trait]
    impl CameraSession for HungPtzSession {
        fn capabilities(&self) -> &crate::model::CameraCapabilities {
            &self.capabilities
        }

        async fn status(&mut self) -> Result<CameraStatus> {
            unreachable!("the interlock issues PTZ status directly")
        }

        async fn capture(&mut self, _request: CaptureRequest) -> Result<CaptureFrame> {
            unreachable!("the interlock must finish before capture")
        }

        async fn ptz(&mut self, request: PtzRequest) -> Result<PtzResult> {
            lock(&self.calls).push(request);
            std::future::pending().await
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct StopAcknowledgementSession {
        capabilities: crate::model::CameraCapabilities,
        calls: Arc<Mutex<Vec<PtzRequest>>>,
        stop_seen: Arc<AtomicBool>,
    }

    #[async_trait]
    impl CameraSession for StopAcknowledgementSession {
        fn capabilities(&self) -> &crate::model::CameraCapabilities {
            &self.capabilities
        }

        async fn status(&mut self) -> Result<CameraStatus> {
            unreachable!("the interlock issues PTZ status directly")
        }

        async fn capture(&mut self, _request: CaptureRequest) -> Result<CaptureFrame> {
            unreachable!("the interlock must finish before capture")
        }

        async fn ptz(&mut self, request: PtzRequest) -> Result<PtzResult> {
            lock(&self.calls).push(request.clone());
            match request {
                PtzRequest::Status => Err(CameraError::rejected(
                    ErrorCode::UnsupportedCapability,
                    "test camera has no PTZ status capability",
                )),
                PtzRequest::Stop { .. } => {
                    self.stop_seen.store(true, Ordering::Release);
                    Ok(PtzResult::Commanded)
                }
                _ => unreachable!("the interlock only reads status and stops"),
            }
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn profile() -> CaptureProfile {
        CaptureProfile {
            capture_mode: Some(CaptureMode::Simulated),
            offline_policy: Some(OfflinePolicy::FailFast),
            queue_expiry_ms: None,
            timeout_ms: Some(5_000),
            maximum_frame_bytes: Some(4_096),
            pixel_format: None,
            width: None,
            height: None,
            offset_x: None,
            offset_y: None,
            exposure_micros: None,
            gain: None,
            output: ProfileOutputConfig {
                encoding: OutputEncoding::Jpeg,
                jpeg_quality: 90,
            },
            capture_interlock: None,
        }
    }

    fn submission() -> JobSubmission {
        let relative_path = RelativeOutputPath::from_stored("camera-a/cap_1.jpg").unwrap();
        let capture = profile();
        let profile = JobProfileSnapshot {
            name: "inspection".to_string(),
            capture,
            offline_policy: OfflinePolicy::FailFast,
            maximum_frame_bytes: 4_096,
            capture_mode: CaptureMode::Simulated,
            capture_interlock: CaptureInterlock::Allow,
            settle_ms: 0,
        };
        let trigger = CaptureTrigger::Command {
            request_id: "request-1".to_string(),
        };
        let deadlines = JobDeadlines {
            terminal_at_ms: 5_000,
            queue_at_ms: Some(2_000),
            capture_at_ms: 3_000,
            encode_at_ms: 4_000,
            persist_at_ms: 4_500,
        };
        let job = NewJob {
            capture_id: "cap_1".to_string(),
            instance: "camera-a".to_string(),
            ledger_key: None,
            canonical_request: json!({"requestId": "request-1"}),
            request_hash: RequestHash::from_bytes([7; 32]),
            effective_profile: serde_json::to_value(&profile).unwrap(),
            deadlines: deadlines.clone(),
            trigger: serde_json::to_value(&trigger).unwrap(),
            origin_correlation_id: Some("corr-1".to_string()),
            intended_output: json!({"relativePath": relative_path.as_wire_path()}),
            accepted_at_ms: 1_000,
            group_id: None,
        };
        JobSubmission {
            job,
            spec: CaptureJobSpec {
                capture_id: "cap_1".to_string(),
                instance: "camera-a".to_string(),
                profile,
                resource_group: None,
                relative_path,
                deadlines,
                accepted_at_ms: 1_000,
                trigger,
                correlation_id: "corr-1".to_string(),
                metadata: Map::new(),
                camera: CameraSummary {
                    backend: crate::model::BackendKind::Sim,
                    vendor: None,
                    model: None,
                    firmware: None,
                    serial: Some("sim-a".to_string()),
                },
                group_size: None,
            },
            priority: CapturePriority::Submitted,
        }
    }

    struct TestTerminalEncoder;

    impl TerminalEnvelopeEncoder for TestTerminalEncoder {
        fn encode(
            &self,
            message: &TerminalMessage,
            created_at_ms: i64,
        ) -> Result<NewOutboxMessage> {
            let envelope = MessageBuilder::new(message.header_name(), TERMINAL_ENVELOPE_VERSION)
                .correlation_id(message.correlation_id())
                .structured_payload(message.body_value()?)
                .build();
            NewOutboxMessage::from_message(
                message.body().event_id.clone(),
                "terminal",
                format!(
                    "ecv1/test/camera-adapter/camera-a/app/{}",
                    message.channel()
                ),
                &envelope,
                created_at_ms,
                created_at_ms,
            )
        }
    }

    #[derive(Default)]
    struct RecordingDispatcher {
        descriptors: Arc<Mutex<Vec<CaptureDescriptor>>>,
        fail_commit: bool,
    }

    struct RecordingReservation {
        descriptors: Arc<Mutex<Vec<CaptureDescriptor>>>,
        fail_commit: bool,
    }

    impl DispatchReservation for RecordingReservation {
        fn commit(self: Box<Self>, descriptor: CaptureDescriptor) -> Result<usize> {
            if self.fail_commit {
                return Err(CameraError::rejected(
                    ErrorCode::QueueFull,
                    "test actor queue rejected descriptor",
                ));
            }
            let mut descriptors = lock(&self.descriptors);
            descriptors.push(descriptor);
            Ok(descriptors.len())
        }
    }

    impl CaptureDispatcher for RecordingDispatcher {
        fn reserve(&self) -> Result<Box<dyn DispatchReservation>> {
            Ok(Box::new(RecordingReservation {
                descriptors: Arc::clone(&self.descriptors),
                fail_commit: self.fail_commit,
            }))
        }
    }

    fn output_config(root: &std::path::Path) -> crate::config::OutputConfig {
        crate::config::OutputConfig {
            root_directory: root.to_string_lossy().into_owned(),
            camera_directory_template: "{cameraId}".to_string(),
            file_name_template: "{captureId}.{extension}".to_string(),
            write_metadata_sidecar: true,
            minimum_free_bytes: 0,
            minimum_free_percent: 0,
            directory_mode: "0750".to_string(),
            file_mode: "0640".to_string(),
        }
    }

    async fn engine() -> (JobEngine, TempDir) {
        let directory = TempDir::new().unwrap();
        let output = directory.path().join("output");
        let state = directory.path().join("state");
        std::fs::create_dir_all(&output).unwrap();
        std::fs::create_dir_all(&state).unwrap();
        let output = output_config(&output);
        let catalog = Catalog::open(CatalogOptions::new(state)).await.unwrap();
        let admission = AdmissionController::new(
            &crate::config::LimitsConfig::default(),
            &output,
            Arc::new(FilesystemSpaceProbe::default()),
        )
        .unwrap();
        (
            JobEngine::new(
                catalog,
                admission,
                StorageRoot::open(&output).unwrap(),
                Arc::new(TestTerminalEncoder),
                Arc::new(NoopJobHooks),
            ),
            directory,
        )
    }

    fn command_submission(capture_id: &str, now: i64) -> JobSubmission {
        let mut submission = submission();
        let relative_path =
            RelativeOutputPath::from_stored(&format!("camera-a/{capture_id}.jpg")).unwrap();
        let canonical_request = json!({
            "requestId": "request-1",
            "metadata": {"source": "job-engine-test"}
        });
        let deadlines = JobDeadlines {
            terminal_at_ms: now + 60_000,
            queue_at_ms: Some(now + 20_000),
            capture_at_ms: now + 30_000,
            encode_at_ms: now + 40_000,
            persist_at_ms: now + 50_000,
        };
        submission.job.capture_id = capture_id.to_string();
        submission.job.ledger_key =
            Some(LedgerKey::new("camera-a", "sb/capture", "request-1").expect("valid ledger key"));
        submission.job.canonical_request = canonical_request.clone();
        submission.job.request_hash =
            crate::idempotency::canonical_request_hash(&canonical_request, false).unwrap();
        submission.job.deadlines = deadlines.clone();
        submission.job.origin_correlation_id = Some("corr-1".to_string());
        submission.job.intended_output = json!({
            "relativePath": relative_path.as_wire_path(),
            "backend": "sim"
        });
        submission.job.accepted_at_ms = now;
        submission.spec.capture_id = capture_id.to_string();
        submission.spec.relative_path = relative_path;
        submission.spec.deadlines = deadlines;
        submission.spec.accepted_at_ms = now;
        submission.spec.metadata =
            serde_json::from_value(canonical_request["metadata"].clone()).unwrap();
        submission
    }

    fn runtime_from(spec: CaptureJobSpec) -> Arc<JobRuntime> {
        let camera = spec.camera.clone();
        Arc::new(JobRuntime {
            spec: Arc::new(spec),
            cancellation: CancellationToken::new(),
            done: CancellationToken::new(),
            trace: Mutex::new(ExecutionTrace {
                stage: JobStage::Queued,
                acquisition_started_at_ms: None,
                frame_received_at_ms: None,
                persisting_at_ms: None,
                persisted_at_ms: None,
                frame: None,
                camera,
            }),
        })
    }

    fn stop_and_settle_runtime(
        capture_deadline_after: Duration,
        settle_ms: u64,
    ) -> Arc<JobRuntime> {
        let now = now_ms();
        let mut submission = command_submission("cap-interlock", now);
        submission.spec.profile.capture_interlock = CaptureInterlock::StopAndSettle;
        submission.spec.profile.settle_ms = settle_ms;
        submission.spec.deadlines.capture_at_ms = now
            + i64::try_from(capture_deadline_after.as_millis()).expect("test deadline fits i64");
        runtime_from(submission.spec)
    }

    fn artifact(capture_id: &str) -> ImageArtifact {
        ImageArtifact {
            absolute_path: format!("/captures/camera-a/{capture_id}.jpg"),
            relative_path: format!("camera-a/{capture_id}.jpg"),
            file_uri: format!("file:///captures/camera-a/{capture_id}.jpg"),
            content_type: "image/jpeg".to_string(),
            encoding: OutputEncoding::Jpeg,
            bytes: 123,
            sha256: "ab".repeat(32),
            metadata_sidecar_relative_path: Some(format!("camera-a/{capture_id}.jpg.json")),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stop_and_settle_bounds_a_hung_ptz_status_by_the_capture_deadline() {
        let (engine, _directory) = engine().await;
        let runtime = stop_and_settle_runtime(Duration::from_millis(100), 0);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut session = HungPtzSession {
            capabilities: test_capabilities(),
            calls: Arc::clone(&calls),
        };
        let task = tokio::spawn({
            let engine = engine.clone();
            let runtime = Arc::clone(&runtime);
            async move {
                engine
                    .enforce_capture_interlock(&mut session, &runtime)
                    .await
            }
        });

        tokio::task::yield_now().await;
        assert!(matches!(lock(&calls).as_slice(), [PtzRequest::Status]));
        tokio::time::advance(Duration::from_millis(101)).await;
        let error = task
            .await
            .expect("interlock task must not panic")
            .expect_err("hung PTZ status must not outlive the capture deadline");
        assert_eq!(error.code(), ErrorCode::CameraMoving);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_and_settle_cancels_a_hung_ptz_status_without_waiting_for_its_deadline() {
        let (engine, _directory) = engine().await;
        let runtime = stop_and_settle_runtime(Duration::from_secs(10), 0);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut session = HungPtzSession {
            capabilities: test_capabilities(),
            calls: Arc::clone(&calls),
        };
        let task = tokio::spawn({
            let engine = engine.clone();
            let runtime = Arc::clone(&runtime);
            async move {
                engine
                    .enforce_capture_interlock(&mut session, &runtime)
                    .await
            }
        });

        tokio::task::yield_now().await;
        assert!(matches!(lock(&calls).as_slice(), [PtzRequest::Status]));
        runtime.cancellation.cancel();
        let error = task
            .await
            .expect("interlock task must not panic")
            .expect_err("cancelled interlock must stop waiting for PTZ status");
        assert_eq!(error.code(), ErrorCode::CaptureCancelled);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_and_settle_bounds_the_post_stop_settle_delay_by_the_capture_deadline() {
        let (engine, _directory) = engine().await;
        let runtime = stop_and_settle_runtime(Duration::from_millis(100), 200);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let stop_seen = Arc::new(AtomicBool::new(false));
        let mut session = StopAcknowledgementSession {
            capabilities: test_capabilities(),
            calls: Arc::clone(&calls),
            stop_seen: Arc::clone(&stop_seen),
        };
        let task = tokio::spawn({
            let engine = engine.clone();
            let runtime = Arc::clone(&runtime);
            async move {
                engine
                    .enforce_capture_interlock(&mut session, &runtime)
                    .await
            }
        });

        while !stop_seen.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(101)).await;
        let error = task
            .await
            .expect("interlock task must not panic")
            .expect_err("settle delay must not outlive the capture deadline");
        assert_eq!(error.code(), ErrorCode::CaptureTimeout);
        assert!(matches!(
            lock(&calls).as_slice(),
            [PtzRequest::Status, PtzRequest::Stop { .. }]
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_helper_preserves_results_and_reports_cancellation_or_timeout() {
        let (engine, _directory) = engine().await;
        let cancellation = CancellationToken::new();
        assert_eq!(
            engine
                .await_with_deadline(
                    now_ms() + 1_000,
                    &cancellation,
                    async { Ok::<_, CameraError>("complete") },
                    "test completion",
                )
                .await
                .unwrap(),
            "complete"
        );

        cancellation.cancel();
        let cancelled = engine
            .await_with_deadline(
                now_ms() + 1_000,
                &cancellation,
                std::future::pending::<Result<()>>(),
                "test cancellation",
            )
            .await
            .expect_err("a cancelled runtime must win over an unfinished operation");
        assert_eq!(cancelled.code(), ErrorCode::CaptureCancelled);

        let timeout_cancellation = CancellationToken::new();
        let deadline = now_ms() + 100;
        let task = tokio::spawn({
            let engine = engine.clone();
            let timeout_cancellation = timeout_cancellation.clone();
            async move {
                engine
                    .await_with_deadline(
                        deadline,
                        &timeout_cancellation,
                        std::future::pending::<Result<()>>(),
                        "test timeout",
                    )
                    .await
            }
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(101)).await;
        let timed_out = task
            .await
            .expect("deadline helper task must not panic")
            .expect_err("an unfinished operation must time out at its absolute deadline");
        assert_eq!(timed_out.code(), ErrorCode::CaptureTimeout);
    }

    #[tokio::test]
    async fn terminal_writes_preserve_profile_trigger_frame_and_failure_contracts() {
        let (engine, _directory) = engine().await;
        let now = Utc::now().timestamp_millis();
        let mut submission = command_submission("cap-terminal", now);
        submission.spec.trigger = CaptureTrigger::GroupCommand {
            request_id: "request-1".to_string(),
            capture_group_id: "grp-terminal".to_string(),
        };
        submission.job.trigger = serde_json::to_value(&submission.spec.trigger).unwrap();
        submission.job.group_id = Some("grp-terminal".to_string());
        submission.spec.group_size = Some(2);
        let runtime = runtime_from(submission.spec);
        {
            let mut trace = lock(&runtime.trace);
            trace.stage = JobStage::Encoding;
            trace.acquisition_started_at_ms = Some(now + 10);
            trace.frame_received_at_ms = Some(now + 20);
            trace.persisting_at_ms = Some(now + 30);
            trace.frame = Some(frame_facts(&CaptureFrame {
                bytes: Bytes::from_static(&[1, 2, 3]),
                width: 1,
                height: 1,
                pixel_format: PixelFormat::Rgb8,
                capture_mode: CaptureMode::Simulated,
                source_timestamp: Some(datetime(now + 15).unwrap()),
                timestamp_quality: FrameTimestampQuality::Camera,
                backend_metadata: BTreeMap::from([("exposureUs".to_string(), json!(1200))]),
            }));
        }

        let success = SuccessFacts {
            image: artifact("cap-terminal"),
        };
        let (succeeded, success_body) = engine
            .make_terminal_write(
                &runtime,
                JobState::Succeeded,
                Some(&success),
                None,
                now + 40,
                Some(now + 40),
            )
            .unwrap();
        assert_eq!(succeeded.state, JobState::Succeeded);
        assert_eq!(succeeded.error_code, None);
        assert_eq!(success_body["captureMode"], "simulated");
        assert_eq!(success_body["captureGroupId"], "grp-terminal");
        assert_eq!(success_body["groupSize"], 2);
        assert_eq!(success_body["image"]["bytes"], 123);
        assert_eq!(success_body["frame"]["sourceEncoding"], "raw");
        assert_eq!(success_body["backendMetadata"]["exposureUs"], 1200);
        assert_eq!(success_body["durationsMs"]["queue"], 10);
        assert_eq!(success_body["durationsMs"]["acquisition"], 10);
        assert_eq!(success_body["durationsMs"]["encoding"], 10);
        assert_eq!(success_body["durationsMs"]["persistence"], 10);

        let (failed, failure_body) = engine
            .make_terminal_write(
                &runtime,
                JobState::Failed,
                None,
                Some((ErrorCode::BackendError, "backend disconnected".to_string())),
                now + 50,
                None,
            )
            .unwrap();
        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.error_code.as_deref(), Some("BACKEND_ERROR"));
        assert_eq!(failure_body["failure"]["stage"], "ENCODING");
        assert_eq!(failure_body["failure"]["retriable"], true);
        assert!(failure_body.get("image").is_none());

        let (cancelled, cancelled_body) = engine
            .make_terminal_write(
                &runtime,
                JobState::Cancelled,
                None,
                Some((ErrorCode::CaptureCancelled, "operator request".to_string())),
                now + 60,
                None,
            )
            .unwrap();
        assert_eq!(cancelled.state, JobState::Cancelled);
        assert_eq!(cancelled.error_code.as_deref(), Some("CAPTURE_CANCELLED"));
        assert!(cancelled_body.get("failure").is_none());
        assert!(cancelled_body.get("image").is_none());

        assert!(
            engine
                .make_terminal_write(&runtime, JobState::Succeeded, None, None, now + 70, None)
                .is_err()
        );
        assert!(
            engine
                .make_terminal_write(&runtime, JobState::Failed, None, None, now + 70, None)
                .is_err()
        );
        assert!(
            engine
                .make_terminal_write(&runtime, JobState::Queued, None, None, now + 70, None)
                .is_err()
        );
    }

    #[tokio::test]
    async fn command_acceptance_is_idempotent_and_active_cancellation_is_durable() {
        let (engine, _directory) = engine().await;
        let now = Utc::now().timestamp_millis();
        let dispatcher = RecordingDispatcher::default();
        let inserted = engine
            .accept_and_queue(&dispatcher, command_submission("cap-command", now))
            .await
            .unwrap();
        let queued = match inserted {
            AcceptJobOutcome::Inserted(record) => record,
            other => panic!("expected initial insert, got {other:?}"),
        };
        assert_eq!(queued.state, JobState::Queued);
        {
            let descriptors = lock(&dispatcher.descriptors);
            assert_eq!(descriptors.len(), 1);
            assert_eq!(descriptors[0].capture_id(), "cap-command");
            assert_eq!(descriptors[0].instance(), "camera-a");
            assert_eq!(descriptors[0].priority(), CapturePriority::Submitted);
        }

        let duplicate = engine
            .accept_and_queue(&dispatcher, command_submission("cap-command", now))
            .await
            .unwrap();
        assert!(matches!(duplicate, AcceptJobOutcome::Existing(_)));
        assert_eq!(lock(&dispatcher.descriptors).len(), 1);

        let cancelled = engine
            .cancel_active("cap-command", "operator\nrequested cancellation")
            .await
            .unwrap();
        assert!(cancelled.cancelled);
        assert_eq!(cancelled.state, JobState::Cancelled);
        let durable = engine.catalog.job("cap-command").await.unwrap().unwrap();
        assert_eq!(durable.state, JobState::Cancelled);
        assert_eq!(durable.error_code.as_deref(), Some("CAPTURE_CANCELLED"));
        assert_eq!(
            durable.terminal_result.as_ref().unwrap()["failure"],
            serde_json::Value::Null
        );
    }

    #[tokio::test]
    async fn reserved_dispatch_failure_publishes_one_terminal_and_later_cancel_is_idempotent() {
        let (engine, _directory) = engine().await;
        let now = Utc::now().timestamp_millis();
        let dispatcher = RecordingDispatcher {
            descriptors: Arc::new(Mutex::new(Vec::new())),
            fail_commit: true,
        };

        let error = engine
            .accept_and_queue(&dispatcher, command_submission("cap-dispatch-failure", now))
            .await
            .expect_err("a failed reserved actor slot must fail acceptance");
        assert_eq!(error.code(), ErrorCode::QueueFull);
        assert!(lock(&dispatcher.descriptors).is_empty());

        let terminal = engine
            .catalog
            .job("cap-dispatch-failure")
            .await
            .unwrap()
            .expect("the durably queued job must be terminalized");
        assert_eq!(terminal.state, JobState::Failed);
        assert_eq!(terminal.error_code.as_deref(), Some("QUEUE_FULL"));
        assert_eq!(
            engine
                .catalog
                .pending_outbox(Utc::now().timestamp_millis() + 1_000, 10)
                .await
                .unwrap()
                .len(),
            1,
            "the failure result must be published exactly once"
        );

        let repeated_cancel = engine
            .cancel_active("cap-dispatch-failure", "retrying a terminal cancellation")
            .await
            .unwrap();
        assert!(!repeated_cancel.cancelled);
        assert_eq!(repeated_cancel.state, JobState::Failed);
    }

    #[tokio::test]
    async fn preaccepted_queue_phase_uses_the_durable_row_and_cancellation_remains_durable() {
        let (engine, _directory) = engine().await;
        let now = Utc::now().timestamp_millis();
        let submission = command_submission("cap-preaccepted", now);
        assert!(matches!(
            engine
                .catalog
                .accept_job(submission.job.clone())
                .await
                .unwrap(),
            AcceptJobOutcome::Inserted(_)
        ));

        let dispatcher = RecordingDispatcher::default();
        let queued = engine
            .queue_preaccepted(&dispatcher, submission)
            .await
            .unwrap();
        assert_eq!(queued.state, JobState::Queued);
        assert_eq!(lock(&dispatcher.descriptors).len(), 1);

        let cancellation = engine
            .cancel_active(
                "cap-preaccepted",
                "operator cancelled before actor execution",
            )
            .await
            .unwrap();
        assert!(cancellation.cancelled);
        assert_eq!(cancellation.state, JobState::Cancelled);
        let durable = engine
            .catalog
            .job("cap-preaccepted")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable.state, JobState::Cancelled);
        assert_eq!(
            engine
                .catalog
                .pending_outbox(Utc::now().timestamp_millis() + 1_000, 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn interrupted_recovery_rehydrates_command_profile_trigger_and_metadata() {
        let (engine, _directory) = engine().await;
        let now = Utc::now().timestamp_millis();
        let submission = command_submission("cap-recovery", now);
        engine.catalog.accept_job(submission.job).await.unwrap();
        let accepted = engine.catalog.job("cap-recovery").await.unwrap().unwrap();
        assert_eq!(accepted.state, JobState::Accepted);

        let interrupted = engine.interrupt_recovered(accepted).await.unwrap();
        assert_eq!(interrupted.state, JobState::Interrupted);
        assert_eq!(
            interrupted.error_code.as_deref(),
            Some("PROCESS_INTERRUPTED")
        );
        let body = interrupted.terminal_result.unwrap();
        assert_eq!(
            body["trigger"],
            json!({"type":"command", "requestId":"request-1"})
        );
        assert_eq!(body["captureProfile"], "inspection");
        assert_eq!(body["captureMode"], "simulated");
        assert_eq!(body["metadata"], json!({"source":"job-engine-test"}));
        assert_eq!(body["camera"]["backend"], "sim");
        assert_eq!(body["failure"]["code"], "PROCESS_INTERRUPTED");
    }

    #[test]
    fn submission_validation_rejects_each_durable_snapshot_mismatch() {
        assert!(validate_submission(&submission()).is_ok());

        let mut identity = submission();
        identity.spec.capture_id = "cap_other".to_string();
        assert!(matches!(
            validate_submission(&identity),
            Err(CameraError::Catalog(_))
        ));

        let mut empty = submission();
        empty.job.capture_id.clear();
        empty.spec.capture_id.clear();
        assert_eq!(
            validate_submission(&empty).unwrap_err().code(),
            ErrorCode::InvalidRequest
        );

        let mut effective_profile = submission();
        effective_profile.spec.profile.maximum_frame_bytes = 1;
        assert!(matches!(
            validate_submission(&effective_profile),
            Err(CameraError::Catalog(_))
        ));

        let mut origin_correlation = submission();
        origin_correlation.job.origin_correlation_id = Some("different".to_string());
        assert!(matches!(
            validate_submission(&origin_correlation),
            Err(CameraError::Catalog(_))
        ));

        let mut output_path = submission();
        output_path.job.intended_output = json!({"relativePath": "other.jpg"});
        assert!(matches!(
            validate_submission(&output_path),
            Err(CameraError::Catalog(_))
        ));
    }

    #[test]
    fn submission_validation_enforces_group_snapshot_pairing() {
        let mut group = submission();
        group.spec.trigger = CaptureTrigger::GroupCommand {
            request_id: "request-1".to_string(),
            capture_group_id: "grp_1".to_string(),
        };
        group.job.trigger = serde_json::to_value(&group.spec.trigger).unwrap();
        group.job.group_id = Some("grp_1".to_string());
        group.spec.group_size = Some(2);
        assert!(validate_submission(&group).is_ok());

        group.spec.group_size = Some(1);
        assert!(matches!(
            validate_submission(&group),
            Err(CameraError::Catalog(_))
        ));

        let mut non_group = submission();
        non_group.job.group_id = Some("grp_1".to_string());
        assert!(matches!(
            validate_submission(&non_group),
            Err(CameraError::Catalog(_))
        ));
    }

    #[test]
    fn stage_frame_and_time_helpers_preserve_safe_observable_facts() {
        assert_eq!(JobStage::Queued.token(), "QUEUED");
        assert_eq!(JobStage::Acquiring.token(), "ACQUIRING");
        assert_eq!(JobStage::Encoding.token(), "ENCODING");
        assert_eq!(JobStage::Persisting.token(), "PERSISTING");

        let mut metadata = BTreeMap::new();
        metadata.insert("exposureUs".to_string(), json!(1200));
        let frame = CaptureFrame {
            bytes: Bytes::from_static(b"frame"),
            width: 640,
            height: 480,
            pixel_format: PixelFormat::Jpeg,
            capture_mode: CaptureMode::SoftwareTrigger,
            source_timestamp: Some(datetime(1_000).unwrap()),
            timestamp_quality: FrameTimestampQuality::Stream,
            backend_metadata: metadata.clone(),
        };
        let facts = frame_facts(&frame);
        assert_eq!(facts.summary.source_encoding, "jpeg");
        assert_eq!(facts.summary.width, 640);
        assert_eq!(facts.backend_metadata, metadata);

        for pixel_format in [PixelFormat::Mono8, PixelFormat::Rgb8, PixelFormat::Bgr8] {
            let mut raw = frame.clone();
            raw.pixel_format = pixel_format;
            assert_eq!(frame_facts(&raw).summary.source_encoding, "raw");
        }

        assert_eq!(elapsed_ms(9, 4), 0);
        assert_eq!(elapsed_ms(4, 9), 5);
        assert_eq!(datetime(0).unwrap().timestamp(), 0);
        assert!(datetime(i64::MAX).is_err());
        assert_eq!(optional_datetime(None).unwrap(), None);
        assert_eq!(optional_datetime(Some(0)).unwrap().unwrap().timestamp(), 0);
        assert!(remaining_duration(0).is_zero());
    }

    #[tokio::test]
    async fn cancellation_and_error_helpers_are_explicit_and_bounded() {
        let cancellation = CancellationToken::new();
        assert!(
            ConnectedAvailability
                .wait_until_ready("camera-a", OfflinePolicy::FailFast, None, 1, &cancellation)
                .await
                .is_ok()
        );
        cancellation.cancel();
        assert_eq!(
            ConnectedAvailability
                .wait_until_ready("camera-a", OfflinePolicy::FailFast, None, 1, &cancellation)
                .await
                .unwrap_err()
                .code(),
            ErrorCode::CaptureCancelled
        );
        assert_eq!(timeout_error("encoding").code(), ErrorCode::CaptureTimeout);
        assert_eq!(
            cancelled_error("persisting").code(),
            ErrorCode::CaptureCancelled
        );

        for code in [
            ErrorCode::CameraUnavailable,
            ErrorCode::QueueFull,
            ErrorCode::ResourceLimit,
            ErrorCode::CaptureTimeout,
            ErrorCode::StoragePressure,
            ErrorCode::PersistenceFailed,
            ErrorCode::BackendError,
        ] {
            assert!(is_retriable(code), "{code}");
        }
        assert!(!is_retriable(ErrorCode::InvalidRequest));

        assert_eq!(bounded_detail("one\ntwo\u{7f}".to_string()), "one two ");
        let bounded = bounded_detail("é".repeat(600));
        assert!(bounded.len() <= MAX_TERMINAL_DETAIL_BYTES);
        assert!(bounded.is_char_boundary(bounded.len()));
    }
}
