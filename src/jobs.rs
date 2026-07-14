//! Protocol-neutral durable capture-job execution.
//!
//! Acceptance, actor dispatch, admission, backend acquisition, bounded encoding/persistence,
//! installation arbitration, the terminal commit, the best-effort terminal announcement, waiter
//! settlement, and group aggregation are joined here without depending on a concrete camera protocol
//! or runtime supervisor.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::Notify;

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
    AcceptJobOutcome, Catalog, JobDeadlines, JobRecord, NewJob, PendingInstall, StateCasOutcome,
    TerminalOutcome, TerminalWrite,
};
use crate::config::{CaptureInterlock, CaptureProfile, OfflinePolicy};
use crate::encoding::EncodingRequest;
use crate::messages::{
    CameraSummary, CaptureDurations, CaptureTimestamps, CaptureTrigger, FailureSummary,
    FrameSummary, ImageArtifact, TERMINAL_SCHEMA_VERSION, TerminalBody, TerminalKind,
    TerminalMessage, Thumbnail,
};
use crate::model::{
    CaptureFrame, CaptureMode, FrameTimestampQuality, JobState, PixelFormat, PtzRequest, PtzResult,
};
use crate::storage::{
    InstallGate, PrepareCapture, RecoveryOutcome, RecoveryRequest, RelativeOutputPath,
    StorageReservation, StorageRoot,
};
use crate::thumbnail::{ThumbnailOutcome, ThumbnailPolicy};
use crate::{CameraError, ErrorCode, Result};

/// How long the terminal reaper waits before trying a durable store that just refused it.
const TERMINAL_REAP_RETRY: Duration = Duration::from_secs(1);
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
    /// Reserves capacity for one camera without exposing any descriptor to an actor.
    ///
    /// The camera is now explicit. A per-camera dispatcher knew it implicitly; a fleet-wide queue
    /// cannot, and it needs it to enforce the per-camera bound that keeps one busy camera from
    /// eating the whole component's backlog.
    fn reserve(&self, camera_id: &str) -> Result<Box<dyn DispatchReservation>>;
}

/// Publishes the terminal announcement, best-effort.
///
/// The announcement is **volatile**. It is sent after the terminal is already durable, it is never
/// retried, and a failure to send it is a degradation of observability -- not of the capture, which
/// is on disk and in the catalog either way. An implementation must therefore not wait for a
/// broker acknowledgement, and must not hold a capture up while it publishes.
#[async_trait]
pub trait TerminalAnnouncer: Send + Sync {
    /// Announces one terminal message. Fire-and-forget: no delivery acknowledgement is awaited.
    async fn announce(&self, message: &TerminalMessage) -> Result<()>;
}

/// Production announcer backed by the guarded EdgeCommons `app()` facade.
pub struct AppTerminalAnnouncer {
    app: Arc<AppFacade>,
}

impl AppTerminalAnnouncer {
    /// Binds terminal announcement to one camera-instance application facade.
    #[must_use]
    pub fn new(app: Arc<AppFacade>) -> Self {
        Self { app }
    }
}

#[async_trait]
impl TerminalAnnouncer for AppTerminalAnnouncer {
    async fn announce(&self, message: &TerminalMessage) -> Result<()> {
        let prepared = message.prepare(&self.app)?;
        // `publish_prepared`, deliberately -- NOT `publish_prepared_confirmed`. Waiting for a broker
        // acknowledgement is what the durable outbox existed to make safe, and there is no longer
        // anything to make safe: nothing is retained, nothing is retried, and nothing downstream
        // requires delivery.
        self.app
            .publish_prepared(&prepared)
            .await
            .map_err(|error| CameraError::Messaging(error.to_string()))
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

    /// Observes one terminal announcement that reached the transport, and how long that took.
    ///
    /// This is the only source of `publishLatencyMs` in `southbound_health` (SOUTHBOUND §5).
    async fn terminal_announced(&self, _record: &JobRecord, _latency: Duration) {}

    /// Observes one terminal announcement that could not be published.
    ///
    /// The capture is already durable and already SUCCEEDED/FAILED/CANCELLED. This is where the
    /// component counts the loss and marks messaging degraded; it must never fail the capture.
    async fn terminal_announcement_failed(&self, _record: &JobRecord, _error: &CameraError) {}

    /// Observes a configured thumbnail that could not be rendered or encoded.
    ///
    /// The capture is unaffected: it succeeds, it is announced, and it simply carries no preview.
    /// Which capture it was is already in the WARN the engine logged; the measure is deliberately
    /// undimensioned, because a per-camera dimension on a 256-camera fleet is 256 metric streams.
    async fn thumbnail_failed(&self) {}

    /// Observes a configured thumbnail that rendered but exceeded the announcement's byte ceiling.
    async fn thumbnail_dropped(&self) {}
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
        instant_for_epoch(self.runtime.deadlines().terminal_at_ms)
    }

    /// The queue-expiry instant, when the profile sets one.
    ///
    /// This bounds how long the capture may WAIT for a camera, and is deliberately separate from the
    /// deadlines that bound how long it may RUN.
    #[must_use]
    pub fn queue_expiry(&self) -> Option<Instant> {
        self.runtime.deadlines().queue_at_ms.map(instant_for_epoch)
    }

    /// Cancellation watched by queue admission and backend work.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.runtime.cancellation.clone()
    }
}

struct JobRuntime {
    spec: Arc<CaptureJobSpec>,
    /// The deadlines currently in force, which are NOT `spec.deadlines`.
    ///
    /// `spec.deadlines` is the acceptance-time record and stays exactly as accepted. These are the
    /// EFFECTIVE clocks, and they are rebased onto the moment a camera actually takes the capture.
    /// A capture's 30-second budget used to start when it was ACCEPTED, so anything that could not
    /// be dispatched at once spent its whole budget queueing and then died of CAPTURE_TIMEOUT the
    /// instant a camera was free to serve it -- which is exactly why an oversized group degraded
    /// into "most of your members failed" instead of taking longer.
    deadlines: RwLock<JobDeadlines>,
    /// Raised when the deadlines above are rebased, so the terminal timer stops sleeping on a clock
    /// that no longer exists.
    rebased: Notify,
    cancellation: CancellationToken,
    done: CancellationToken,
    trace: Mutex<ExecutionTrace>,
}

impl JobRuntime {
    /// The deadlines currently in force.
    fn deadlines(&self) -> JobDeadlines {
        self.deadlines
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
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
    announcer: Arc<dyn TerminalAnnouncer>,
    hooks: Arc<dyn JobHooks>,
    availability: Arc<dyn AvailabilityGate>,
    install_gate: Arc<dyn InstallGate>,
    acceptance_hook: Arc<dyn AcceptanceHook>,
    active: Arc<Mutex<HashMap<String, ActiveJob>>>,
    /// What the RESOLVED transport can carry. Required, not defaulted: a permissive default is
    /// exactly the assumption that cost the lab 90 announcements.
    thumbnail_policy: ThumbnailPolicy,
}

impl JobEngine {
    /// Creates an engine with connected-session availability and catalog installation arbitration.
    ///
    /// `thumbnail_policy` is derived from the transport the component actually started with. It is a
    /// required argument rather than an overridable default because every previous version of this
    /// question -- "what can the wire take?" -- was answered by assumption, and the assumption was
    /// wrong on the one transport that matters most.
    #[must_use]
    pub fn new(
        catalog: Catalog,
        admission: AdmissionController,
        storage: StorageRoot,
        announcer: Arc<dyn TerminalAnnouncer>,
        hooks: Arc<dyn JobHooks>,
        thumbnail_policy: ThumbnailPolicy,
    ) -> Self {
        Self {
            install_gate: Arc::new(catalog.clone()),
            catalog,
            admission,
            storage,
            announcer,
            hooks,
            availability: Arc::new(ConnectedAvailability),
            acceptance_hook: Arc::new(NoopAcceptanceHook),
            active: Arc::new(Mutex::new(HashMap::new())),
            thumbnail_policy,
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
        let reservation = dispatcher.reserve(&submission.spec.instance)?;
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
            deadlines: RwLock::new(submission.spec.deadlines.clone()),
            rebased: Notify::new(),
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
        let reservation = dispatcher.reserve(&submission.spec.instance)?;
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
            deadlines: RwLock::new(submission.spec.deadlines.clone()),
            rebased: Notify::new(),
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
                self.after_terminal(&runtime, &record, &body, None).await;
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
            // Only the winner announces. The staged success this recovery is finishing belongs to a
            // process that died before it could tell anyone; if a terminal had ALREADY been committed
            // for this capture, whoever committed it announced it, and announcing it twice would
            // publish the same event id again.
            TerminalOutcome::Won(record) => {
                self.announce_terminal(&record, &body, None).await;
                record
            }
            TerminalOutcome::AlreadyTerminal(record) => record,
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

    /// Puts a capture that was still waiting when the process died back on the queue.
    ///
    /// The durable record is the whole contract: the resolved profile, the intended output path, the
    /// trigger, the deadlines and the correlation are all committed before a capture is ever exposed
    /// to a camera, so a waiting capture can be rebuilt exactly rather than re-resolved against a
    /// configuration that may have moved underneath it.
    ///
    /// Only a capture that has not yet touched a camera may come back this way. Anything that reached
    /// `ACQUIRING` or beyond has side effects behind it and is interrupted instead -- a replay is not
    /// a recovery.
    pub async fn requeue_recovered(
        &self,
        dispatcher: &dyn CaptureDispatcher,
        record: JobRecord,
        resource_group: Option<String>,
        group_size: Option<usize>,
        priority: CapturePriority,
    ) -> Result<JobRecord> {
        if record.state != JobState::Queued || record.install_started {
            return Err(CameraError::Catalog(
                "only a QUEUED capture may be requeued after a restart".to_string(),
            ));
        }
        let spec = spec_from_record(&record, resource_group, group_size)?;
        let job = NewJob {
            capture_id: record.capture_id.clone(),
            instance: record.instance.clone(),
            // The ledger row already exists and is not re-inserted; `queue_preaccepted` reads the
            // durable record by capture id and never re-accepts it.
            ledger_key: None,
            canonical_request: record.canonical_request.clone(),
            request_hash: record.request_hash,
            effective_profile: record.effective_profile.clone(),
            deadlines: record.deadlines.clone(),
            trigger: record.trigger.clone(),
            origin_correlation_id: record.origin_correlation_id.clone(),
            intended_output: record.intended_output.clone(),
            accepted_at_ms: record.accepted_at_ms,
            group_id: record.group_id.clone(),
        };
        self.queue_preaccepted(
            dispatcher,
            JobSubmission {
                job,
                spec,
                priority,
            },
        )
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
        let spec = spec_from_record(&record, None, record.group_id.as_ref().map(|_| 1_usize))?;
        let backend = spec.camera.backend;
        let runtime = Arc::new(JobRuntime {
            deadlines: RwLock::new(spec.deadlines.clone()),
            rebased: Notify::new(),
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
            // Only the winner announces: a record that was already terminal was already announced by
            // whoever committed it, and announcing it again would republish that event id.
            TerminalOutcome::Won(terminal) => {
                self.announce_terminal(&terminal, &body, None).await;
                self.hooks.settle_waiters(&terminal, &body).await;
                if terminal.group_id.is_some() {
                    self.hooks.group_member_terminal(&terminal, &body).await;
                }
                Ok(terminal)
            }
            TerminalOutcome::AlreadyTerminal(terminal) => {
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
                runtime.deadlines().terminal_at_ms,
                &runtime.cancellation,
                self.availability.wait_until_ready(
                    &runtime.spec.instance,
                    runtime.spec.profile.offline_policy,
                    runtime.deadlines().queue_at_ms,
                    runtime.deadlines().terminal_at_ms,
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
                    deadline: instant_for_epoch(runtime.deadlines().capture_at_ms),
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
            timeout: remaining_duration(runtime.deadlines().capture_at_ms),
            cancellation: runtime.cancellation.clone(),
        });
        let frame = match self
            .await_with_deadline(
                runtime.deadlines().capture_at_ms,
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
                    instant_for_epoch(runtime.deadlines().encode_at_ms),
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
                instant_for_epoch(runtime.deadlines().persist_at_ms),
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
                .min(runtime.deadlines().persist_at_ms)
        } else {
            runtime.deadlines().persist_at_ms
        };
        let (prepared, thumbnail) = match self
            .prepare_blocking(&runtime, frame, processing, encoder, writer, work_deadline)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => return self.finish_error(&runtime, error).await,
        };
        let thumbnail = self.settle_thumbnail(&runtime, thumbnail).await;

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
        // its exact body for the sidecar, the durable terminal, and the announcement. The live trace
        // is not marked persisted until the install, parent sync, verification, and installed-artifact
        // catalog write all succeed.
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
        // The thumbnail rides ONLY on the announcement, never into `committed_body` -- which is what
        // the catalog holds, what the sidecar is written from, and what a deferred/group reply
        // carries. See the `messages` module docs.
        self.finish_terminal_outcome(&runtime, outcome, &committed_body, thumbnail.as_ref())
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
        let deadline = instant_for_epoch(runtime.deadlines().capture_at_ms);
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
                            runtime.deadlines().capture_at_ms,
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
            runtime.deadlines().capture_at_ms,
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
        let deadline = instant_for_epoch(runtime.deadlines().capture_at_ms);
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

    /// Rebases a queued capture's clocks onto the moment a camera takes it.
    ///
    /// This is what makes sequencing work at all. A capture's stage deadlines used to start when it
    /// was ACCEPTED, so a member of an oversized group spent its entire 30-second budget waiting for
    /// a free camera and then died of CAPTURE_TIMEOUT the instant one appeared. The work was not too
    /// slow; it was declared late before it was allowed to begin.
    ///
    /// The durable row and the in-memory clocks move together, and the durable write goes FIRST: if
    /// the process dies between them, the row carries the deadline the capture will actually run to,
    /// which is the direction that cannot lie to an operator reading the catalog.
    ///
    /// `queue_at_ms` is deliberately NOT rebased. It bounds how long a capture may WAIT, and a bound
    /// that moved every time the capture was passed over would never expire -- a starved capture
    /// would queue forever, one rebase at a time.
    pub(crate) async fn rebase_onto_admission(
        &self,
        descriptor: &CaptureDescriptor,
        timeouts: &crate::config::TimeoutsConfig,
    ) -> Result<()> {
        let runtime = &descriptor.runtime;
        let accepted = runtime.spec.deadlines.clone();
        let admitted_at_ms = now_ms();
        let terminal_budget_ms = accepted
            .terminal_at_ms
            .saturating_sub(runtime.spec.accepted_at_ms)
            .max(0);

        let rebased = JobDeadlines {
            terminal_at_ms: admitted_at_ms.saturating_add(terminal_budget_ms),
            // NOT rebased: the wait bound is measured from acceptance, on purpose.
            queue_at_ms: accepted.queue_at_ms,
            capture_at_ms: admitted_at_ms
                .saturating_add(i64::try_from(timeouts.capture_ms).unwrap_or(i64::MAX)),
            encode_at_ms: admitted_at_ms
                .saturating_add(i64::try_from(timeouts.encode_ms).unwrap_or(i64::MAX)),
            persist_at_ms: admitted_at_ms
                .saturating_add(i64::try_from(timeouts.persist_ms).unwrap_or(i64::MAX)),
        };

        self.catalog
            .reschedule_deadlines(&runtime.spec.capture_id, rebased.clone(), admitted_at_ms)
            .await?;
        {
            let mut effective = runtime
                .deadlines
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *effective = rebased;
        }
        // The terminal timer is asleep on the OLD clock. Wake it, or it fires on a deadline that no
        // longer exists -- while the capture it is watching has only just started.
        runtime.rebased.notify_waiters();
        Ok(())
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

    /// Retires a capture whose failure came from the durable store, keeping the camera session.
    ///
    /// Best effort by construction: the store is the thing that just failed, so writing the terminal
    /// row may fail too. Either way the runtime MUST be completed. It is normally retired inside
    /// `finish_failure`, but an error escaping `execute` skips that -- and the actor now keeps
    /// serving this camera instead of being torn down and rebuilt, so nothing else would ever clean
    /// up: the `active` entry would be held forever and its terminal-deadline task would outlive the
    /// job it belongs to.
    pub(crate) async fn fail_durable_store(
        &self,
        descriptor: &CaptureDescriptor,
        error: &CameraError,
    ) -> Result<JobRecord> {
        let outcome = self
            .finish_failure(
                &descriptor.runtime,
                error.code(),
                bounded_detail(error.operator_detail().into_owned()),
            )
            .await;
        if outcome.is_err() {
            self.complete_runtime(&descriptor.runtime);
        }
        outcome
    }

    /// Encodes and stages the artifact, and -- when the profile asks for one -- the thumbnail.
    ///
    /// Both are CPU-bound, both work from the same in-hand frame, and both belong inside the SAME
    /// `spawn_blocking`: it already holds the encoder and writer permits that bound how much of this
    /// work the component does at once, and it is already off the reactor. The thumbnail is rendered
    /// from `request.frame` BEFORE the request is moved into `prepare_capture`, so it is made from
    /// the camera's frame -- the same bytes the artifact is derived from -- and not from the file.
    ///
    /// The thumbnail's outcome is RETURNED rather than acted on here: a failed or dropped preview is
    /// a fact to log and count on the async side, never an error that could reach this function's
    /// `Result` and become the capture's.
    async fn prepare_blocking(
        &self,
        runtime: &Arc<JobRuntime>,
        frame: CaptureFrame,
        processing: ProcessingLease,
        encoder: Option<EncoderPermit>,
        writer: WriterPermit,
        deadline_ms: i64,
    ) -> Result<(crate::storage::PreparedInstall, Option<ThumbnailOutcome>)> {
        let current = processing.reserved_disk_bytes();
        let other = self
            .admission
            .outstanding_disk_bytes()
            .saturating_sub(current);
        let thumbnail = runtime.spec.profile.capture.thumbnail;
        let policy = self.thumbnail_policy;
        // The picture the thumbnail may decode is bounded by the same ceiling the capture was
        // ADMITTED with. A JPEG frame's decoded size is not its file size, and this is the only code
        // in the component that decodes one -- see `thumbnail::render`.
        let maximum_frame_bytes = runtime.spec.profile.maximum_frame_bytes;
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
            let thumbnail = thumbnail.map(|thumbnail| {
                crate::thumbnail::render(
                    &request.frame,
                    thumbnail.size,
                    policy,
                    maximum_frame_bytes,
                )
            });
            let prepared = storage.prepare_capture(request, &cancellation)?;
            processing.release_memory();
            processing.shrink_disk(prepared.artifact().bytes)?;
            Ok((prepared, thumbnail, processing))
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
                let (prepared, thumbnail, _processing) = result
                    .map_err(|error| CameraError::Storage(format!("bounded persistence worker failed: {error}")))??;
                Ok((prepared, thumbnail))
            }
        }
    }

    /// Turns one thumbnail outcome into the preview to announce, plus its log line and its measure.
    ///
    /// Everything that is not a rendered thumbnail resolves to `None`: the capture is already on
    /// disk, the announcement is already owed, and a preview is a convenience that never gets to
    /// change either.
    async fn settle_thumbnail(
        &self,
        runtime: &Arc<JobRuntime>,
        outcome: Option<ThumbnailOutcome>,
    ) -> Option<Thumbnail> {
        match outcome? {
            ThumbnailOutcome::Rendered(thumbnail) => Some(thumbnail),
            ThumbnailOutcome::Dropped { bytes } => {
                tracing::warn!(
                    capture = %runtime.spec.capture_id,
                    instance = %runtime.spec.instance,
                    bytes,
                    budget = self.thumbnail_policy.budget_bytes(),
                    transport = ?self.thumbnail_policy.transport(),
                    "the thumbnail did not fit what this transport can carry, even at the lowest \
                     quality, and was dropped; the capture and its announcement are unaffected"
                );
                self.hooks.thumbnail_dropped().await;
                None
            }
            ThumbnailOutcome::Failed { reason } => {
                tracing::warn!(
                    capture = %runtime.spec.capture_id,
                    instance = %runtime.spec.instance,
                    reason = %reason,
                    "the thumbnail could not be rendered; the capture and its announcement are \
                     unaffected"
                );
                self.hooks.thumbnail_failed().await;
                None
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
                .cancel_runtime(
                    runtime,
                    bounded_detail(error.operator_detail().into_owned()),
                )
                .await;
        }
        self.finish_failure(
            runtime,
            error.code(),
            bounded_detail(error.operator_detail().into_owned()),
        )
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
            .finish_terminal_outcome(runtime, outcome, &body, None)
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
                self.after_terminal(runtime, &record, &body, None).await;
                Ok(record)
            }
            TerminalOutcome::AlreadyTerminal(record) => {
                self.complete_runtime(runtime);
                Ok(record)
            }
            // The install path won the terminal and has already published it, so this caller must not
            // publish it again -- but the capture is over, and the runtime it is holding (a whole
            // `CaptureJobSpec`) is not free. Releasing it is not the same act as terminalizing it.
            TerminalOutcome::InstallationWon(record) => {
                self.complete_runtime(runtime);
                Ok(record)
            }
        }
    }

    async fn finish_terminal_outcome(
        &self,
        runtime: &Arc<JobRuntime>,
        outcome: TerminalOutcome,
        body: &Value,
        thumbnail: Option<&Thumbnail>,
    ) -> Result<JobRecord> {
        match outcome {
            TerminalOutcome::Won(record) => {
                self.after_terminal(runtime, &record, body, thumbnail).await;
                Ok(record)
            }
            TerminalOutcome::AlreadyTerminal(record) => {
                self.complete_runtime(runtime);
                Ok(record)
            }
            TerminalOutcome::InstallationWon(record) => {
                self.complete_runtime(runtime);
                Ok(record)
            }
        }
    }

    /// Everything that happens once a terminal has WON its durable commit.
    ///
    /// `body` is the committed document, and it is what the waiters and the group coordinator are
    /// settled with -- unchanged, thumbnail-free. Only the announcement gets the preview.
    async fn after_terminal(
        &self,
        runtime: &Arc<JobRuntime>,
        record: &JobRecord,
        body: &Value,
        thumbnail: Option<&Thumbnail>,
    ) {
        self.complete_runtime(runtime);
        self.announce_terminal(record, body, thumbnail).await;
        self.hooks.settle_waiters(record, body).await;
        if record.group_id.is_some() {
            self.hooks.group_member_terminal(record, body).await;
        }
    }

    /// Announces one terminal that has just WON its durable commit.
    ///
    /// The order is the whole point: the capture is on disk and the catalog says so BEFORE anyone is
    /// told about it. The announcement itself is volatile -- it is attempted exactly once, it is
    /// never retried, and every way it can fail ends here, at WARN, with a counted metric and
    /// messaging marked degraded. A broker that is down loses announcements; it does not lose
    /// captures, does not reject captures, and does not stop the component.
    async fn announce_terminal(
        &self,
        record: &JobRecord,
        body: &Value,
        thumbnail: Option<&Thumbnail>,
    ) {
        let started = Instant::now();
        let base = match terminal_kind(record.state)
            .and_then(|kind| TerminalMessage::from_committed_body(kind, body.clone()))
        {
            Ok(message) => message,
            Err(error) => {
                self.hooks.terminal_announcement_failed(record, &error).await;
                tracing::warn!(
                    capture = %record.capture_id,
                    instance = %record.instance,
                    error = %error,
                    "the terminal capture result is durable but no announcement could be built for it"
                );
                return;
            }
        };

        // The one thing the wire carries that the durable record does not: a volatile, derived preview,
        // attached to the message and to nothing else. An announcement REBUILT from the durable body in
        // a later process -- the recovery paths -- passes `None` here and simply has no preview, because
        // the frame is long gone.
        //
        // A PREVIEW MAY BE THE VERY THING THAT MAKES THE MESSAGE UNDELIVERABLE, and only the transport
        // knows. The Greengrass IPC client this component links encodes the whole eventstream packet
        // into a 10,000-byte STATIC buffer (`GG_IPC_MAX_MSG_LEN` in the component SDK) and answers NOMEM
        // above it -- before a single byte reaches the nucleus. A local MQTT broker takes a megabyte
        // without blinking. The component cannot know which it is talking to, and must not have to.
        //
        // So the preview is shed the instant it costs anything: if the announcement carrying it does not
        // go out, the RESULT is announced again without it. A result nobody was told about is a real
        // loss; a missing preview is an inconvenience. The preview never outranks the result.
        let announcement = match thumbnail {
            None => self.announcer.announce(&base).await,
            Some(thumbnail) => {
                let previewed = base.clone().with_thumbnail(thumbnail);
                match previewed {
                    Ok(previewed) => {
                        let first = self.announcer.announce(&previewed).await;
                        match first {
                            Ok(()) => Ok(()),
                            Err(error) => {
                                tracing::warn!(
                                    capture = %record.capture_id,
                                    instance = %record.instance,
                                    error = %error,
                                    "the capture result could not be announced with its thumbnail;                                      retrying without the preview so the result itself is not lost"
                                );
                                self.hooks.thumbnail_dropped().await;
                                self.announcer.announce(&base).await
                            }
                        }
                    }
                    Err(_) => {
                        self.hooks.thumbnail_dropped().await;
                        self.announcer.announce(&base).await
                    }
                }
            }
        };
        match announcement {
            Ok(()) => {
                self.hooks
                    .terminal_announced(record, started.elapsed())
                    .await;
            }
            Err(error) => {
                tracing::warn!(
                    capture = %record.capture_id,
                    instance = %record.instance,
                    state = ?record.state,
                    error = %error,
                    "the terminal capture result is durable but could not be announced; \
                     the announcement is dropped and will not be retried"
                );
                self.hooks.terminal_announcement_failed(record, &error).await;
            }
        }
    }

    /// Whether this engine is still holding a capture's runtime.
    ///
    /// Every entry here is a live `CaptureJobSpec`, so "how many is it holding" is a real question
    /// about the component's memory, not only a test's.
    #[must_use]
    pub fn is_active_for_test(&self, capture_id: &str) -> bool {
        lock(&self.active).contains_key(capture_id)
    }

    fn complete_runtime(&self, runtime: &Arc<JobRuntime>) {
        runtime.done.cancel();
        lock(&self.active).remove(&runtime.spec.capture_id);
    }

    /// Fails a capture that outlives its terminal deadline -- the one that is in force NOW.
    ///
    /// The timer re-reads the deadline instead of capturing it once, because a queued capture's
    /// clocks are rebased when a camera finally takes it. A timer that had latched the
    /// acceptance-time deadline would fire while the capture it was watching had only just begun.
    fn spawn_terminal_deadline(&self, runtime: Arc<JobRuntime>) {
        let engine = self.clone();
        tokio::spawn(async move {
            loop {
                let terminal_at_ms = runtime.deadlines().terminal_at_ms;
                let rebased = runtime.rebased.notified();
                tokio::select! {
                    biased;
                    _ = runtime.done.cancelled() => return,
                    () = rebased => continue,
                    _ = tokio::time::sleep_until(instant_for_epoch(terminal_at_ms)) => {
                        // The deadline may have moved while this slept; only fire on the one that
                        // is still in force.
                        if runtime.deadlines().terminal_at_ms > terminal_at_ms {
                            continue;
                        }
                        // This is the reaper. It is the last thing standing between a capture that
                        // went wrong and a capture that is never heard from again -- and it used to
                        // throw its own error away and exit. Under the one condition it exists to
                        // survive, a durable store that is briefly refusing writes, it therefore did
                        // nothing at all: the capture kept no terminal, and the runtime it was
                        // holding -- a whole `CaptureJobSpec` -- was never released, for the life of
                        // the process. The store being unwell is not a reason to stop reaping.
                        loop {
                            match engine.finish_failure(
                                &runtime,
                                ErrorCode::CaptureTimeout,
                                "capture exceeded its terminal deadline",
                            ).await {
                                Ok(_) => return,
                                Err(error) if error.is_durable_store_failure() => {
                                    tracing::warn!(
                                        capture = %runtime.spec.capture_id,
                                        error = %error,
                                        "the durable store could not retire an expired capture; retrying"
                                    );
                                    tokio::select! {
                                        biased;
                                        // Somebody else terminalized it while the store was unwell,
                                        // and released the runtime with it. Nothing left to do.
                                        _ = runtime.done.cancelled() => return,
                                        () = tokio::time::sleep(TERMINAL_REAP_RETRY) => {}
                                    }
                                }
                                Err(error) => {
                                    // Not the store: the capture is no longer in a state this can
                                    // retire. Whatever decided that owns it now -- but the runtime is
                                    // this task's to let go of, and holding it changes nothing.
                                    tracing::warn!(
                                        capture = %runtime.spec.capture_id,
                                        error = %error,
                                        "an expired capture could not be retired and is released"
                                    );
                                    engine.complete_runtime(&runtime);
                                    return;
                                }
                            }
                        }
                    }
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
        let message = TerminalMessage::new(terminal_kind(state)?, body)?;
        let value = message.body().clone();
        let retained_error = error.map(|(code, message)| (code.as_str().to_string(), message));
        Ok((
            TerminalWrite {
                state,
                result: value.clone(),
                error_code: retained_error.as_ref().map(|(code, _)| code.clone()),
                error_message: retained_error.map(|(_, message)| message),
                terminal_at_ms,
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

/// The announcer the tests use, shared by every module that builds a [`JobEngine`].
///
/// It is here rather than in one test module because "what did the component announce, and did it
/// keep working when it could not" is asked by the job tests, the actor tests, and the runtime
/// tests alike -- and a broker being down has to look the same to all three.
#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;

    use super::{TerminalAnnouncer, TerminalMessage};
    use crate::{CameraError, Result};

    /// One announcement, as a test can see it: exactly what would have gone on the wire.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct Announcement {
        /// EdgeCommons envelope header name (`ImageCaptured`, ...).
        pub header_name: &'static str,
        /// `app/` channel (`image/captured`, ...).
        pub channel: &'static str,
        /// Body `eventId`.
        pub event_id: String,
        /// Body `captureId`.
        pub capture_id: String,
        /// Envelope correlation.
        pub correlation_id: String,
        /// The validated schema-v1 body document exactly as it would be published.
        pub body: serde_json::Value,
    }

    impl Announcement {
        /// Exactly what this message would have put on the wire.
        fn of(message: &TerminalMessage) -> Self {
            Self {
                header_name: message.header_name(),
                channel: message.channel(),
                event_id: message.event_id().to_string(),
                capture_id: message.body()["captureId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                correlation_id: message.correlation_id().to_string(),
                body: message.body().clone(),
            }
        }

        /// Whether this announcement carried a preview.
        pub fn carries_thumbnail(&self) -> bool {
            self.body.get("thumbnail").is_some()
        }
    }

    /// Records every announcement, and fails them all while `fail` is set -- which is what a broker
    /// that is not there looks like from inside the component.
    #[derive(Debug, Default)]
    pub(crate) struct RecordingAnnouncer {
        announced: Mutex<Vec<Announcement>>,
        fail: AtomicBool,
        refuse_previews: AtomicBool,
    }

    impl RecordingAnnouncer {
        /// An announcer whose every publish fails, for as long as the test wants.
        pub fn failing() -> Self {
            Self {
                announced: Mutex::new(Vec::new()),
                fail: AtomicBool::new(true),
                refuse_previews: AtomicBool::new(false),
            }
        }

        /// A transport with a size ceiling: it refuses any message carrying a preview and takes the
        /// same message without one. This is the Greengrass IPC client, whose static send buffer
        /// answers NOMEM above 10,000 bytes -- observed on lab-5950x, where every `medium` and `large`
        /// announcement was lost and every `small` one landed.
        pub fn refusing_previews() -> Self {
            Self {
                announced: Mutex::new(Vec::new()),
                fail: AtomicBool::new(false),
                refuse_previews: AtomicBool::new(true),
            }
        }

        /// Starts or stops failing publishes.
        pub fn set_failing(&self, failing: bool) {
            self.fail.store(failing, Ordering::SeqCst);
        }

        /// Every announcement attempted so far, in order.
        pub fn announcements(&self) -> Vec<Announcement> {
            self.announced
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        /// The announcements attempted for one capture.
        pub fn for_capture(&self, capture_id: &str) -> Vec<Announcement> {
            self.announcements()
                .into_iter()
                .filter(|announcement| announcement.capture_id == capture_id)
                .collect()
        }
    }

    #[async_trait]
    impl TerminalAnnouncer for RecordingAnnouncer {
        async fn announce(&self, message: &TerminalMessage) -> Result<()> {
            // Every ATTEMPT is recorded, including the ones that fail. What the component tried to
            // put on the wire, and what it fell back to, is the whole question these doubles answer.
            let announcement = Announcement::of(message);
            let carries_preview = announcement.carries_thumbnail();
            self.announced
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(announcement);
            if carries_preview && self.refuse_previews.load(Ordering::SeqCst) {
                // The lab's failure, exactly: our own client's send buffer, not the broker, and only
                // for the message that carries the picture.
                return Err(CameraError::Messaging(
                    "greengrass IPC error: NOMEM".to_string(),
                ));
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(CameraError::Messaging(
                    "the local broker is not connected".to_string(),
                ));
            }
            Ok(())
        }
    }
}

/// The terminal message kind one durable terminal state announces itself as.
fn terminal_kind(state: JobState) -> Result<TerminalKind> {
    match state {
        JobState::Succeeded => Ok(TerminalKind::Captured),
        JobState::Cancelled => Ok(TerminalKind::Cancelled),
        JobState::Failed | JobState::Interrupted => Ok(TerminalKind::Failed),
        _ => Err(CameraError::Catalog(
            "terminal message requested for nonterminal state".to_string(),
        )),
    }
}

/// Rebuilds a capture's immutable execution snapshot from its durable record.
///
/// Everything a capture needs to run is committed before it is exposed to a camera, so a record is
/// sufficient to reconstruct it. Nothing here consults the live configuration: a capture runs under
/// the contract it was accepted with, or it does not run at all.
fn spec_from_record(
    record: &JobRecord,
    resource_group: Option<String>,
    group_size: Option<usize>,
) -> Result<CaptureJobSpec> {
    let profile: JobProfileSnapshot = serde_json::from_value(record.effective_profile.clone())
        .map_err(|_| {
            CameraError::Catalog("recovery record has an invalid effective profile".to_string())
        })?;
    let trigger: CaptureTrigger = serde_json::from_value(record.trigger.clone()).map_err(|_| {
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
        .ok_or_else(|| CameraError::Catalog("recovery record lacks relativePath".to_string()))?;
    Ok(CaptureJobSpec {
        capture_id: record.capture_id.clone(),
        instance: record.instance.clone(),
        profile,
        resource_group,
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
    })
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use serde_json::json;
    use tempfile::TempDir;

    use super::testing::RecordingAnnouncer;
    use super::*;
    use crate::admission::FilesystemSpaceProbe;
    use crate::backend::{CameraSession, CameraStatus, CaptureRequest};
    use crate::catalog::{CatalogOptions, LedgerKey};
    use crate::config::ProfileOutputConfig;
    use crate::idempotency::RequestHash;
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

        async fn ptz_bounded(
            &mut self,
            request: PtzRequest,
            deadline: Instant,
            cancellation: &CancellationToken,
        ) -> Result<PtzResult> {
            // Hangs forever on purpose. The bound is what must end it -- which is the whole reason the
            // trait no longer lets a backend skip one.
            let operation = async move {
                lock(&self.calls).push(request);
                std::future::pending().await
            };
            crate::backend::bounded_ptz(operation, deadline, cancellation).await
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

        async fn ptz_bounded(
            &mut self,
            request: PtzRequest,
            _deadline: Instant,
            _cancellation: &CancellationToken,
        ) -> Result<PtzResult> {
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
            thumbnail: None,
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

    /// Counts what the engine told the runtime about each announcement.
    #[derive(Default)]
    struct AnnouncementHooks {
        announced: AtomicUsize,
        failed: AtomicUsize,
    }

    #[async_trait]
    impl JobHooks for AnnouncementHooks {
        async fn terminal_announced(&self, _record: &JobRecord, _latency: Duration) {
            self.announced.fetch_add(1, Ordering::SeqCst);
        }

        async fn terminal_announcement_failed(&self, _record: &JobRecord, _error: &CameraError) {
            self.failed.fetch_add(1, Ordering::SeqCst);
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
        fn reserve(&self, _camera_id: &str) -> Result<Box<dyn DispatchReservation>> {
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

    async fn engine() -> (JobEngine, TempDir, Arc<RecordingAnnouncer>) {
        engine_with_announcer(
            Arc::new(RecordingAnnouncer::default()),
            Arc::new(NoopJobHooks),
            mqtt(),
        )
        .await
    }

    /// An engine whose announcements a test can see, and whose hooks it can count.
    /// An engine on a chosen transport, whose announcements a test can see and whose hooks it counts.
    async fn engine_with_announcer(
        announcer: Arc<RecordingAnnouncer>,
        hooks: Arc<dyn JobHooks>,
        policy: ThumbnailPolicy,
    ) -> (JobEngine, TempDir, Arc<RecordingAnnouncer>) {
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
                Arc::clone(&announcer) as Arc<dyn TerminalAnnouncer>,
                hooks,
                policy,
            ),
            directory,
            announcer,
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
            deadlines: RwLock::new(spec.deadlines.clone()),
            rebased: Notify::new(),
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
        let (engine, _directory, _announcer) = engine().await;
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
        let (engine, _directory, _announcer) = engine().await;
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
        let (engine, _directory, _announcer) = engine().await;
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
        let (engine, _directory, _announcer) = engine().await;
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
        let (engine, _directory, _announcer) = engine().await;
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

    /// B6: a capture the durable store could not run is retired WITHOUT killing the camera session.
    ///
    /// The actor used to treat any error escaping `execute` as proof the protocol session was dead --
    /// including a `SQLITE_BUSY` from a contended connection pool. It now fails just that capture and
    /// keeps serving, which means nothing else is going to tear the actor down and clean up after it:
    /// the runtime must be retired here, or its `active` entry and its terminal-deadline task outlive
    /// the job they belong to.
    #[tokio::test]
    async fn a_capture_the_store_could_not_run_is_retired_without_killing_the_session() {
        let (engine, _directory, _announcer) = engine().await;
        let now = Utc::now().timestamp_millis();
        let dispatcher = RecordingDispatcher::default();
        let accepted = engine
            .accept_and_queue(&dispatcher, command_submission("cap-busy", now))
            .await
            .unwrap();
        let AcceptJobOutcome::Inserted(queued) = accepted else {
            panic!("expected an initial insert");
        };
        assert_eq!(queued.state, JobState::Queued);
        let descriptor = dispatcher
            .descriptors
            .lock()
            .unwrap()
            .first()
            .cloned()
            .expect("the descriptor must have been committed");

        let busy = CameraError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(5), // SQLITE_BUSY
            Some("database is locked".to_owned()),
        ));
        assert!(
            busy.is_durable_store_failure(),
            "the premise of this test: a busy pool is the store, not the camera"
        );

        let record = engine
            .fail_durable_store(&descriptor, &busy)
            .await
            .expect("retiring the capture must succeed");

        assert_eq!(
            record.state,
            JobState::Failed,
            "the capture the store could not run must reach a terminal state, not linger as QUEUED"
        );
        assert!(
            !lock(&engine.active).contains_key(&queued.capture_id),
            "and its runtime must be retired -- nothing else is coming to clean up after it now that              the actor survives"
        );
    }

    #[tokio::test]
    async fn command_acceptance_is_idempotent_and_active_cancellation_is_durable() {
        let (engine, _directory, _announcer) = engine().await;
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
        let (engine, _directory, _announcer) = engine().await;
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
            _announcer.for_capture("cap-dispatch-failure").len(),
            1,
            "the failure result must be announced exactly once"
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
        let (engine, _directory, _announcer) = engine().await;
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
            _announcer.for_capture("cap-preaccepted").len(),
            1,
            "one won terminal, one announcement"
        );
    }

    #[tokio::test]
    async fn interrupted_recovery_rehydrates_command_profile_trigger_and_metadata() {
        let (engine, _directory, _announcer) = engine().await;
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

    /// A capture that is already over must not be terminalized a second time.
    ///
    /// Startup recovery walks every non-terminal row it finds, and a terminal message is published as
    /// part of retiring one. Interrupting a job that has already reached a terminal state would put a
    /// SECOND terminal for one capture on the bus -- and a waiter that has already been settled by the
    /// first would be settled again, with a different outcome. A capture the installer owns is refused
    /// for the mirror-image reason: only the targeted install recovery can know whether the file
    /// actually landed, so a generic "interrupted" would be a guess published as a fact.
    #[tokio::test]
    async fn interruption_recovery_refuses_a_capture_that_is_over_or_owned_by_the_installer() {
        let (engine, _directory, _announcer) = engine().await;
        let now = Utc::now().timestamp_millis();
        engine
            .catalog
            .accept_job(command_submission("cap-twice", now).job)
            .await
            .unwrap();
        let accepted = engine.catalog.job("cap-twice").await.unwrap().unwrap();

        let interrupted = engine
            .interrupt_recovered(accepted.clone())
            .await
            .expect("a non-terminal capture is exactly what interruption recovery is for");
        assert_eq!(interrupted.state, JobState::Interrupted);
        assert!(interrupted.state.is_terminal());

        let twice = engine
            .interrupt_recovered(interrupted)
            .await
            .expect_err("a capture that has already been retired must not be retired again");
        assert!(matches!(twice, CameraError::Catalog(_)));
        assert!(
            twice.to_string().contains("non-terminal"),
            "the refusal must name the invariant it is protecting: {twice}"
        );

        let mut installing = accepted;
        installing.state = JobState::Persisting;
        installing.install_started = true;
        let owned = engine
            .interrupt_recovered(installing)
            .await
            .expect_err("only install recovery can decide the outcome of an installed artifact");
        assert!(matches!(owned, CameraError::Catalog(_)));
        assert!(owned.to_string().contains("install-owned"));

        // Exactly one terminal was announced, which is the invariant all of the above protects.
        assert_eq!(_announcer.announcements().len(), 1);
    }

    /// A durable row the component cannot read is a catalog error, never a panic.
    ///
    /// Recovery rehydrates a `CaptureJobSpec` from JSON that was written by a possibly-older build of
    /// this component and has since been sitting on disk. Every one of these fields is therefore
    /// *untrusted input* at read time, however it got there -- a schema that moved, a disk that lied,
    /// a row an operator edited. Unwrapping any of them would turn one unreadable row into a crash
    /// loop at startup, which is the one failure an adapter cannot recover from on its own: it would
    /// never reach the point of quarantining the row that is killing it.
    #[tokio::test]
    async fn a_recovery_record_the_component_cannot_read_is_refused_field_by_field() {
        let (engine, _directory, _announcer) = engine().await;
        let now = Utc::now().timestamp_millis();
        engine
            .catalog
            .accept_job(command_submission("cap-corrupt", now).job)
            .await
            .unwrap();
        let accepted = engine.catalog.job("cap-corrupt").await.unwrap().unwrap();

        spec_from_record(&accepted, None, None)
            .expect("a row this component wrote itself must rehydrate");

        /// One way a durable row can be unreadable by the time recovery reaches it.
        type Corruption = fn(&mut JobRecord);

        let corruptions: [(Corruption, &str); 4] = [
            (
                |record| record.effective_profile = json!("not a profile"),
                "invalid effective profile",
            ),
            (
                |record| record.trigger = json!({"type": "telepathy"}),
                "invalid capture trigger",
            ),
            (
                |record| record.canonical_request = json!({"metadata": 7}),
                "invalid capture metadata",
            ),
            (
                |record| record.intended_output = json!({"relativePath": "camera-a/cap.jpg"}),
                "valid backend kind",
            ),
        ];

        for (corrupt, expected) in corruptions {
            let mut record = accepted.clone();
            corrupt(&mut record);
            let error = spec_from_record(&record, None, None)
                .expect_err("an unreadable durable field must not be unwrapped");
            assert!(
                matches!(error, CameraError::Catalog(_)),
                "an unreadable row is a catalog fault, not a rejection an operator can retry: \
                 {error}"
            );
            assert!(
                error.to_string().contains(expected),
                "the refusal must name the field that could not be read; got: {error}"
            );
        }
    }

    /// An announcement that cannot be published is attempted EXACTLY ONCE, and never retried.
    ///
    /// This is the whole trade the durable outbox used to make in the other direction. There is no
    /// queue behind the announcement any more, no backoff, and nothing that comes back for it later:
    /// one attempt per terminal, and a failure is a WARN, a metric, and a shrug. A retry loop
    /// creeping back in here would rebuild the outbox by accident -- in memory, unbounded.
    #[tokio::test]
    async fn a_failed_announcement_is_attempted_once_and_never_retried() {
        let hooks = Arc::new(AnnouncementHooks::default());
        let (engine, _directory, announcer) = engine_with_announcer(
            Arc::new(RecordingAnnouncer::failing()),
            Arc::clone(&hooks) as Arc<dyn JobHooks>,
            mqtt(),
        )
        .await;
        let now = Utc::now().timestamp_millis();
        let dispatcher = RecordingDispatcher::default();
        engine
            .accept_and_queue(&dispatcher, command_submission("cap-no-retry", now))
            .await
            .unwrap();

        let cancelled = engine
            .cancel_active("cap-no-retry", "operator cancelled")
            .await
            .unwrap();
        assert!(cancelled.cancelled);

        // Give any retry the engine might be hiding every chance to happen.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let attempts = announcer.for_capture("cap-no-retry");
        assert_eq!(
            attempts.len(),
            1,
            "one terminal is one announcement attempt, however badly it went: {attempts:?}"
        );
        assert_eq!(attempts[0].header_name, "ImageCaptureCancelled");
        assert_eq!(
            hooks.failed.load(Ordering::SeqCst),
            1,
            "the failure must be reported once, so it can be counted once"
        );
        assert_eq!(
            hooks.announced.load(Ordering::SeqCst),
            0,
            "a publish that failed must never be reported as published"
        );
        // The capture is terminal and durable regardless of what the broker did.
        let durable = engine.catalog.job("cap-no-retry").await.unwrap().unwrap();
        assert_eq!(durable.state, JobState::Cancelled);
        assert!(durable.terminal_result.is_some());
    }

    /// A publish failure never reaches the caller: the terminal commit succeeds, and so does the call.
    ///
    /// The engine's terminal paths return `Result`, and the announcement happens inside them. If the
    /// announcement's error were ever propagated, a broker outage would turn every capture's terminal
    /// into an error the runtime would then try to "handle" -- re-terminalizing a capture that is
    /// already durably terminal.
    #[tokio::test]
    async fn a_publish_failure_never_becomes_the_captures_failure() {
        let hooks = Arc::new(AnnouncementHooks::default());
        let (engine, _directory, announcer) = engine_with_announcer(
            Arc::new(RecordingAnnouncer::failing()),
            Arc::clone(&hooks) as Arc<dyn JobHooks>,
            mqtt(),
        )
        .await;
        let now = Utc::now().timestamp_millis();
        let dispatcher = RecordingDispatcher::default();
        engine
            .accept_and_queue(&dispatcher, command_submission("cap-broker-down", now))
            .await
            .unwrap();

        let record = engine
            .interrupt_for_reload(
                engine
                    .catalog
                    .job("cap-broker-down")
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .await
            .expect("a broker that cannot be published to must not fail the terminal commit");

        assert_eq!(record.state, JobState::Interrupted);
        assert_eq!(record.error_code.as_deref(), Some("PROCESS_INTERRUPTED"));
        assert_eq!(announcer.for_capture("cap-broker-down").len(), 1);
        assert_eq!(hooks.failed.load(Ordering::SeqCst), 1);
    }

    /// A published announcement is reported as published, with the latency it took.
    ///
    /// `publishLatencyMs` is a contract measure of `southbound_health` (SOUTHBOUND §5) and this hook
    /// is now its only source -- the confirmed-publish observer that used to feed it went with the
    /// outbox.
    #[tokio::test]
    async fn a_published_announcement_is_reported_with_its_latency() {
        let hooks = Arc::new(AnnouncementHooks::default());
        let (engine, _directory, announcer) = engine_with_announcer(
            Arc::new(RecordingAnnouncer::default()),
            Arc::clone(&hooks) as Arc<dyn JobHooks>,
            mqtt(),
        )
        .await;
        let now = Utc::now().timestamp_millis();
        let dispatcher = RecordingDispatcher::default();
        engine
            .accept_and_queue(&dispatcher, command_submission("cap-announced", now))
            .await
            .unwrap();

        engine
            .cancel_active("cap-announced", "operator cancelled")
            .await
            .unwrap();

        let announced = announcer.for_capture("cap-announced");
        assert_eq!(announced.len(), 1);
        assert_eq!(announced[0].channel, "image/cancelled");
        assert_eq!(
            announced[0].correlation_id, "corr-1",
            "the announcement is correlated to the request that asked for the capture"
        );
        assert_eq!(hooks.announced.load(Ordering::SeqCst), 1);
        assert_eq!(hooks.failed.load(Ordering::SeqCst), 0);
    }

    // ============================ the opt-in thumbnail ============================
    //
    // The property under all of these: a thumbnail is a convenience, and a convenience may not cost
    // a capture anything. Whatever the preview does -- absent, unrenderable, too big -- the capture
    // reaches SUCCEEDED, the artifact is installed, and the terminal is announced.

    /// A camera that hands the engine exactly the frame the test chose.
    struct FrameSession {
        capabilities: crate::model::CameraCapabilities,
        frame: CaptureFrame,
    }

    #[async_trait]
    impl CameraSession for FrameSession {
        fn capabilities(&self) -> &crate::model::CameraCapabilities {
            &self.capabilities
        }

        async fn status(&mut self) -> Result<CameraStatus> {
            unreachable!("a capture does not read camera status")
        }

        async fn capture(&mut self, _request: CaptureRequest) -> Result<CaptureFrame> {
            Ok(self.frame.clone())
        }

        async fn ptz_bounded(
            &mut self,
            _request: PtzRequest,
            _deadline: Instant,
            _cancellation: &CancellationToken,
        ) -> Result<PtzResult> {
            unreachable!("the interlock is Allow in these fixtures")
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// Counts what the engine told the runtime about each thumbnail, and about each announcement.
    #[derive(Default)]
    struct ThumbnailHooks {
        announced: AtomicUsize,
        announcement_failed: AtomicUsize,
        thumbnail_failed: AtomicUsize,
        thumbnail_dropped: AtomicUsize,
    }

    #[async_trait]
    impl JobHooks for ThumbnailHooks {
        async fn terminal_announced(&self, _record: &JobRecord, _latency: Duration) {
            self.announced.fetch_add(1, Ordering::SeqCst);
        }

        async fn terminal_announcement_failed(&self, _record: &JobRecord, _error: &CameraError) {
            self.announcement_failed.fetch_add(1, Ordering::SeqCst);
        }

        async fn thumbnail_failed(&self) {
            self.thumbnail_failed.fetch_add(1, Ordering::SeqCst);
        }

        async fn thumbnail_dropped(&self) {
            self.thumbnail_dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A capture whose profile asks for `size` (or, when `None`, does not mention thumbnails at all).
    fn thumbnail_submission(
        capture_id: &str,
        size: Option<crate::config::ThumbnailSize>,
        encoding: OutputEncoding,
    ) -> JobSubmission {
        let mut submission = command_submission(capture_id, Utc::now().timestamp_millis());
        submission.spec.profile.capture.output.encoding = encoding;
        submission.spec.profile.capture.thumbnail =
            size.map(|size| crate::config::ThumbnailConfig { size });
        submission.spec.profile.capture.maximum_frame_bytes = Some(8 * 1024 * 1024);
        submission.spec.profile.maximum_frame_bytes = 8 * 1024 * 1024;
        // The durable snapshot and the runtime snapshot are the same profile, and the engine checks
        // that they are.
        submission.job.effective_profile = serde_json::to_value(&submission.spec.profile).unwrap();
        submission
    }

    fn test_frame(format: PixelFormat, bytes: Vec<u8>, width: u32, height: u32) -> CaptureFrame {
        CaptureFrame {
            bytes: Bytes::from(bytes),
            width,
            height,
            pixel_format: format,
            capture_mode: CaptureMode::Simulated,
            source_timestamp: None,
            timestamp_quality: FrameTimestampQuality::Camera,
            backend_metadata: BTreeMap::new(),
        }
    }

    /// An RGB8 frame whose pixels vary with position.
    fn gradient_frame(width: u32, height: u32) -> CaptureFrame {
        let mut bytes = Vec::with_capacity((width * height * 3) as usize);
        for y in 0..height {
            for x in 0..width {
                bytes.extend_from_slice(&[(x % 256) as u8, (y % 256) as u8, ((x + y) % 256) as u8]);
            }
        }
        test_frame(PixelFormat::Rgb8, bytes, width, height)
    }

    /// Runs one capture end to end against a chosen frame, and reports what was announced.
    async fn capture_frame(
        submission: JobSubmission,
        frame: CaptureFrame,
        hooks: &Arc<ThumbnailHooks>,
    ) -> (JobRecord, Vec<super::testing::Announcement>, TempDir) {
        capture_frame_on(submission, frame, hooks, mqtt(), Arc::new(RecordingAnnouncer::default()))
            .await
    }

    /// The permissive transport: an MQTT broker.
    fn mqtt() -> ThumbnailPolicy {
        ThumbnailPolicy::for_transport(edgecommons::platform::Transport::Mqtt)
    }

    /// The strict one: Greengrass IPC, whose client cannot put more than 10,000 bytes on the wire.
    fn ipc() -> ThumbnailPolicy {
        ThumbnailPolicy::for_transport(edgecommons::platform::Transport::Ipc)
    }

    /// Runs one capture end to end against a chosen frame, transport, and announcer.
    async fn capture_frame_on(
        submission: JobSubmission,
        frame: CaptureFrame,
        hooks: &Arc<ThumbnailHooks>,
        policy: ThumbnailPolicy,
        announcer: Arc<RecordingAnnouncer>,
    ) -> (JobRecord, Vec<super::testing::Announcement>, TempDir) {
        let capture_id = submission.spec.capture_id.clone();
        let (engine, directory, announcer) =
            engine_with_announcer(announcer, Arc::clone(hooks) as Arc<dyn JobHooks>, policy).await;
        let dispatcher = RecordingDispatcher::default();
        engine
            .accept_and_queue(&dispatcher, submission)
            .await
            .expect("the capture must be accepted");
        let descriptor = lock(&dispatcher.descriptors)
            .pop()
            .expect("acceptance must have committed a descriptor to the actor");
        let mut session = FrameSession {
            capabilities: test_capabilities(),
            frame,
        };
        let record = engine
            .execute(&mut session, descriptor)
            .await
            .expect("the capture must reach a terminal state");
        (record, announcer.for_capture(&capture_id), directory)
    }

    /// Absent `thumbnail` in the profile means no thumbnail, and no `thumbnail` key on the wire.
    ///
    /// Off by default is the whole opt-in surface. A key carrying `null`, or an empty object, would
    /// be a consumer's problem to disambiguate forever after; the field simply is not there.
    #[tokio::test]
    async fn a_profile_that_asks_for_no_thumbnail_announces_no_thumbnail_key_at_all() {
        let hooks = Arc::new(ThumbnailHooks::default());
        let (record, announced, _directory) = capture_frame(
            thumbnail_submission("cap-thumb-off", None, OutputEncoding::Jpeg),
            gradient_frame(320, 240),
            &hooks,
        )
        .await;

        assert_eq!(record.state, JobState::Succeeded);
        assert_eq!(announced.len(), 1, "one terminal, one announcement");
        assert!(
            announced[0].body.get("thumbnail").is_none(),
            "a profile that never mentioned thumbnails must not announce one: {}",
            announced[0].body
        );
        assert!(
            announced[0].body.get("image").is_some(),
            "the artifact itself is unaffected"
        );
        assert_eq!(hooks.thumbnail_failed.load(Ordering::SeqCst), 0);
        assert_eq!(hooks.thumbnail_dropped.load(Ordering::SeqCst), 0);
    }

    /// A configured thumbnail is announced beside the artifact, as bytes, with no digest of its own.
    ///
    /// Three claims, and each is load-bearing:
    /// * the picture is carried through the library's binary marker, so it lands on the wire as a
    ///   native protobuf `bytes_value` rather than as a base64 string inside the JSON body;
    /// * the announced `width`/`height`/`bytes` describe the JPEG actually carried; and
    /// * there is NO `sha256`. The thumbnail is a lossy re-encode, so a digest of it could never be
    ///   checked against the artifact whose digest sits three keys away -- it could only invite a
    ///   consumer to believe it could be.
    #[tokio::test]
    async fn a_configured_thumbnail_is_announced_as_bytes_beside_the_artifact_and_carries_no_digest()
    {
        let hooks = Arc::new(ThumbnailHooks::default());
        let (record, announced, _directory) = capture_frame(
            thumbnail_submission(
                "cap-thumb-on",
                Some(crate::config::ThumbnailSize::Medium),
                OutputEncoding::Jpeg,
            ),
            gradient_frame(1024, 768),
            &hooks,
        )
        .await;

        assert_eq!(record.state, JobState::Succeeded);
        assert_eq!(announced.len(), 1);
        let thumbnail = &announced[0].body["thumbnail"];
        assert_eq!(thumbnail["encoding"], "jpeg", "a thumbnail is always a JPEG");
        assert_eq!(thumbnail["width"], 320, "medium bounds the longest edge");
        assert_eq!(thumbnail["height"], 240, "and the aspect ratio survives");
        assert!(
            thumbnail.get("sha256").is_none(),
            "a lossy re-encode must not be handed a digest that invites verification: {thumbnail}"
        );
        assert!(
            thumbnail["data"]["_edgecommonsBinary"].is_object(),
            "the picture must go through the library's binary marker, not into the JSON as base64"
        );

        // The bytes announced are a JPEG of exactly the announced size.
        let carried: crate::messages::Thumbnail = serde_json::from_value(thumbnail.clone())
            .expect("a consumer must be able to read the announced thumbnail back");
        let jpeg = carried
            .data_bytes()
            .expect("the marker must carry decodable bytes");
        assert_eq!(
            thumbnail["bytes"].as_u64(),
            Some(jpeg.len() as u64),
            "the announced byte count must be the size of the bytes announced"
        );
        assert!(jpeg.starts_with(&[0xff, 0xd8]) && jpeg.ends_with(&[0xff, 0xd9]));

        // ...and the DURABLE record does not carry it. See the test below.
        let durable = record.terminal_result.as_ref().unwrap();
        assert!(
            durable.get("thumbnail").is_none(),
            "the preview is volatile: it belongs on the wire and nowhere else"
        );
        assert_eq!(
            durable["image"], announced[0].body["image"],
            "and the announcement is otherwise the committed body, unchanged"
        );
        assert_eq!(hooks.thumbnail_failed.load(Ordering::SeqCst), 0);
        assert_eq!(hooks.thumbnail_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(hooks.announced.load(Ordering::SeqCst), 1);
    }

    /// On IPC, a `large` profile announces a `small` picture -- clamped, and still announced.
    ///
    /// The lab ran this exact configuration and lost 45 of 45 announcements to NOMEM. The capture
    /// pipeline does not ask the transport whether it will take the picture; it asks the POLICY what
    /// the transport can carry, and produces that. Nothing is rejected and nothing fails: the profile
    /// asked for 640 px, the wire can take 160 px, and 160 px is what goes out.
    #[tokio::test]
    async fn a_large_profile_on_the_ipc_transport_announces_a_small_picture() {
        let hooks = Arc::new(ThumbnailHooks::default());
        let (record, announced, _directory) = capture_frame_on(
            thumbnail_submission(
                "cap-thumb-ipc",
                Some(crate::config::ThumbnailSize::Large),
                OutputEncoding::Jpeg,
            ),
            gradient_frame(1024, 768),
            &hooks,
            ipc(),
            Arc::new(RecordingAnnouncer::default()),
        )
        .await;

        assert_eq!(record.state, JobState::Succeeded);
        assert_eq!(
            announced.len(),
            1,
            "one announcement, and it went out -- which is the entire point"
        );
        let thumbnail = &announced[0].body["thumbnail"];
        assert_eq!(
            (thumbnail["width"].as_u64(), thumbnail["height"].as_u64()),
            (Some(160), Some(120)),
            "a `large` profile on IPC must be clamped to `small`, not sent and lost: {thumbnail}"
        );
        assert!(
            thumbnail["bytes"].as_u64().unwrap() <= ipc().budget_bytes() as u64,
            "and it must fit inside what the IPC client can actually put on the wire"
        );
        assert_eq!(
            hooks.thumbnail_dropped.load(Ordering::SeqCst),
            0,
            "a clamped preview is not a dropped one: it was produced, carried, and delivered"
        );
        assert_eq!(hooks.thumbnail_failed.load(Ordering::SeqCst), 0);
    }

    /// A transport that refuses the previewed message still gets the RESULT.
    ///
    /// The safety net beneath the policy, and the thing the lab did not have: if the wire will not
    /// take the message that carries the picture, the picture is shed and the message is sent again
    /// without it. A result nobody was told about is a real loss; a missing preview is an
    /// inconvenience. The preview never outranks the result -- and `announcementFailed` must NOT be
    /// counted, because nothing was ultimately lost.
    #[tokio::test]
    async fn a_transport_that_refuses_the_preview_still_gets_the_result_announced() {
        let hooks = Arc::new(ThumbnailHooks::default());
        let (record, announced, _directory) = capture_frame_on(
            thumbnail_submission(
                "cap-thumb-nomem",
                Some(crate::config::ThumbnailSize::Small),
                OutputEncoding::Jpeg,
            ),
            gradient_frame(1024, 768),
            &hooks,
            // A permissive POLICY against a transport that in fact refuses the preview: the exact
            // shape of a limit the component has mis-modelled, which is what this net is for.
            mqtt(),
            Arc::new(RecordingAnnouncer::refusing_previews()),
        )
        .await;

        assert_eq!(
            record.state,
            JobState::Succeeded,
            "the capture is on disk whatever the wire did"
        );
        assert_eq!(
            announced.len(),
            2,
            "two attempts: the one carrying the picture, and the one that had to give it up"
        );
        assert!(
            announced[0].carries_thumbnail(),
            "the first attempt is the one with the preview -- the one the transport refused"
        );
        assert!(
            !announced[1].carries_thumbnail(),
            "and the second is the RESULT, shorn of the preview so that it can actually go out"
        );
        assert_eq!(
            announced[1].body["image"], announced[0].body["image"],
            "it is the same result, not a rebuilt or degraded one"
        );
        assert_eq!(
            hooks.thumbnail_dropped.load(Ordering::SeqCst),
            1,
            "the shed preview is counted -- it is how an operator learns the limit was mis-modelled"
        );
        assert_eq!(
            hooks.announced.load(Ordering::SeqCst),
            1,
            "and the announcement, on its second attempt, SUCCEEDED"
        );
        assert_eq!(
            hooks.announcement_failed.load(Ordering::SeqCst),
            0,
            "nothing was ultimately lost, so nothing may be counted as lost: an operator watching \
             announcementFailed must not be paged for a preview that was merely shed"
        );
    }

    /// NOTHING DURABLE CARRIES THE THUMBNAIL -- not the catalog, not the sidecar, not a reply.
    ///
    /// This is the whole point of the preview being volatile, and it is the reason the durable
    /// outbox was deleted in the first place: a capture must not pay to STORE an envelope. A 60 KiB
    /// preview in the terminal body would put ~80 KB of base64 into the catalog's `terminal_result`
    /// for every capture, the same again into the on-disk metadata sidecar beside the very image it
    /// is a thumbnail OF, and N times over into a group reply.
    ///
    /// So this asserts the negative, on a capture whose thumbnail was configured, rendered, and
    /// successfully announced: the announced body has it, and every durable/derived artifact of the
    /// same capture -- the catalog record, the sidecar FILE on disk, and the body the deferred
    /// caller is settled with -- does not.
    #[tokio::test]
    async fn the_thumbnail_is_announced_and_is_in_nothing_durable() {
        /// A waiter/group coordinator that keeps the exact body it was settled with.
        #[derive(Default)]
        struct SettledBodies {
            settled: Mutex<Vec<Value>>,
        }

        #[async_trait]
        impl JobHooks for SettledBodies {
            async fn settle_waiters(&self, _record: &JobRecord, terminal_body: &Value) {
                lock(&self.settled).push(terminal_body.clone());
            }

            async fn group_member_terminal(&self, _record: &JobRecord, terminal_body: &Value) {
                lock(&self.settled).push(terminal_body.clone());
            }
        }

        let hooks = Arc::new(SettledBodies::default());
        let capture_id = "cap-thumb-volatile";
        let (engine, _directory, announcer) = engine_with_announcer(
            Arc::new(RecordingAnnouncer::default()),
            Arc::clone(&hooks) as Arc<dyn JobHooks>,
            mqtt(),
        )
        .await;
        let dispatcher = RecordingDispatcher::default();
        engine
            .accept_and_queue(
                &dispatcher,
                thumbnail_submission(
                    capture_id,
                    Some(crate::config::ThumbnailSize::Medium),
                    OutputEncoding::Jpeg,
                ),
            )
            .await
            .unwrap();
        let descriptor = lock(&dispatcher.descriptors).pop().unwrap();
        let mut session = FrameSession {
            capabilities: test_capabilities(),
            frame: gradient_frame(1024, 768),
        };
        let record = engine.execute(&mut session, descriptor).await.unwrap();
        assert_eq!(record.state, JobState::Succeeded);

        // It WAS announced -- otherwise the negatives below would be vacuously true.
        let announced = announcer.for_capture(capture_id);
        assert_eq!(announced.len(), 1);
        assert!(
            announced[0].body["thumbnail"]["data"]["_edgecommonsBinary"].is_object(),
            "the fixture must actually have produced and announced a thumbnail: {}",
            announced[0].body
        );

        // 1. The durable catalog record.
        let durable = record
            .terminal_result
            .as_ref()
            .expect("a succeeded capture commits a terminal body");
        assert!(
            durable.get("thumbnail").is_none(),
            "the CATALOG must not store a lossy preview for every capture ever taken: {durable}"
        );

        // 2. The metadata sidecar FILE on disk -- which is the durable body, verbatim.
        let image = durable["image"]["absolutePath"].as_str().unwrap();
        let sidecar: Value =
            serde_json::from_slice(&std::fs::read(format!("{image}.json")).unwrap()).unwrap();
        assert_eq!(
            sidecar, *durable,
            "the sidecar is the committed body verbatim, and that invariant is untouched"
        );
        assert!(
            sidecar.get("thumbnail").is_none(),
            "a base64 preview of an image has no business sitting in a file NEXT to that image"
        );

        // 3. The body every deferred caller and group coordinator is settled with.
        let settled = lock(&hooks.settled).clone();
        assert!(!settled.is_empty(), "the waiter hook must have been called");
        for body in settled {
            assert!(
                body.get("thumbnail").is_none(),
                "a reply -- and a group reply carries one body PER MEMBER -- must not carry \
                 previews: {body}"
            );
        }
    }

    /// A thumbnail that will not fit the ceiling is dropped -- the capture and the announcement are not.
    ///
    /// This is the case the whole ladder exists for. Over 64 KiB the messaging library refuses the
    /// binary value, which means the ANNOUNCEMENT ITSELF would fail to build and the capture's
    /// terminal message would be lost. So the thumbnail is what gives way, and it is counted.
    #[tokio::test]
    async fn a_thumbnail_over_the_ceiling_is_dropped_while_the_capture_succeeds_and_is_announced() {
        // Incompressible per-pixel noise: at 640px even quality 50 cannot get under the budget.
        let (width, height) = (640_u32, 640_u32);
        let mut bytes = Vec::with_capacity((width * height * 3) as usize);
        let mut state = 0x2545_F491_4F6C_DD1D_u64;
        for _ in 0..width * height * 3 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            bytes.push(state as u8);
        }

        let hooks = Arc::new(ThumbnailHooks::default());
        let (record, announced, _directory) = capture_frame(
            thumbnail_submission(
                "cap-thumb-huge",
                Some(crate::config::ThumbnailSize::Large),
                OutputEncoding::Jpeg,
            ),
            test_frame(PixelFormat::Rgb8, bytes, width, height),
            &hooks,
        )
        .await;

        assert_eq!(
            record.state,
            JobState::Succeeded,
            "a preview that would not fit is not a capture that failed"
        );
        assert!(
            std::path::Path::new(
                record.terminal_result.as_ref().unwrap()["image"]["absolutePath"]
                    .as_str()
                    .unwrap()
            )
            .exists(),
            "the artifact is on disk, at full size, exactly as it would have been"
        );
        assert_eq!(announced.len(), 1, "and it was still announced");
        assert!(
            announced[0].body.get("thumbnail").is_none(),
            "an announcement that could not carry the picture carries no thumbnail key: {}",
            announced[0].body
        );
        assert_eq!(
            hooks.thumbnail_dropped.load(Ordering::SeqCst),
            1,
            "the drop is counted -- it is the only way an operator learns the preview never comes"
        );
        assert_eq!(
            hooks.thumbnail_failed.load(Ordering::SeqCst),
            0,
            "the picture rendered fine; it was the BUDGET that did not fit, and the two measures \
             call for different responses"
        );
    }

    /// A frame whose thumbnail cannot be rendered still succeeds, and is still announced.
    ///
    /// The frame is a real one a camera can send: a small, structurally valid JPEG whose SOF header
    /// declares 20000x20000. `encoding::validate_frame` reads only the HEADER of a declared JPEG, so
    /// the frame passes validation and a `passthrough` profile persists its bytes exactly as sent --
    /// the CAPTURE SUCCEEDS. The thumbnail, which is the only thing in this component that would
    /// actually DECODE a camera's JPEG, refuses it rather than allocating the 1.2 GB the header asks
    /// for. That refusal ends where every thumbnail failure ends: WARN, count, announce without it.
    #[tokio::test]
    async fn a_frame_whose_thumbnail_cannot_be_rendered_still_succeeds_and_is_announced_without_one()
    {
        use image::ExtendedColorType;
        use image::codecs::jpeg::JpegEncoder;

        const CLAIMED: u16 = 20_000;
        let mut jpeg = Vec::new();
        JpegEncoder::new_with_quality(std::io::Cursor::new(&mut jpeg), 90)
            .encode(&[128_u8; 32 * 32 * 3], 32, 32, ExtendedColorType::Rgb8)
            .expect("fixture JPEG");
        let sof = jpeg
            .windows(2)
            .position(|marker| marker == [0xff, 0xc0])
            .expect("a baseline JPEG has an SOF0 marker");
        jpeg[sof + 5..sof + 7].copy_from_slice(&CLAIMED.to_be_bytes());
        jpeg[sof + 7..sof + 9].copy_from_slice(&CLAIMED.to_be_bytes());

        let hooks = Arc::new(ThumbnailHooks::default());
        let (record, announced, _directory) = capture_frame(
            thumbnail_submission(
                "cap-thumb-broken",
                Some(crate::config::ThumbnailSize::Medium),
                OutputEncoding::Passthrough,
            ),
            test_frame(
                PixelFormat::Jpeg,
                jpeg.clone(),
                u32::from(CLAIMED),
                u32::from(CLAIMED),
            ),
            &hooks,
        )
        .await;

        assert_eq!(
            record.state,
            JobState::Succeeded,
            "the frame passed the encoder's header check and was persisted; the capture SUCCEEDED, \
             and an unrenderable preview may not retroactively fail it"
        );
        assert_eq!(
            record.installed_bytes,
            Some(jpeg.len() as u64),
            "passthrough installed exactly the bytes the camera sent"
        );
        assert_eq!(announced.len(), 1, "and the terminal was announced");
        assert!(
            announced[0].body.get("thumbnail").is_none(),
            "an announcement whose picture could not be rendered carries no thumbnail key: {}",
            announced[0].body
        );
        assert_eq!(
            hooks.thumbnail_failed.load(Ordering::SeqCst),
            1,
            "the failure is counted, not merely logged"
        );
        assert_eq!(hooks.thumbnail_dropped.load(Ordering::SeqCst), 0);
    }
}
