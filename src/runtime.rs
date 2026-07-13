//! Runtime composition and command-plane hand-off.
//!
//! This module deliberately separates inbox registration from construction of the durable
//! runtime. The core command inbox is subscribed during [`edgecommons::EdgeCommonsBuilder`]
//! construction, while the adapter cannot safely accept camera work until catalog recovery,
//! storage probing, and supervisor creation have completed. [`RuntimeCommandRouter`] closes that
//! short interval without a command-registration race.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use edgecommons::commands::{
    CommandError, CommandInbox, CommandOutcome, DeferredReplyRegistry, DeferredReplyToken,
    outcome_handler,
};
use edgecommons::config::{
    Config, ConfigurationApplicationError, ConfigurationApplicationResult,
    ConfigurationApplyListener, ConfigurationValidationPhase, ConfigurationValidationResult,
    PreparedConfigurationApply,
};
use edgecommons::facades::{AppFacade, EventsFacade, Severity};
use edgecommons::messaging::Message;
use edgecommons::platform::Platform;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[cfg(all(
    test,
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
use std::sync::atomic::AtomicU64;

use crate::{
    COMPONENT_NAME, Result,
    actor::{CameraActor, CameraActorHandle},
    admission::{AdmissionController, FilesystemSpaceProbe},
    backend::{BackendRuntimeContext, ConnectRequest, DiscoveryCandidate},
    catalog::{Catalog, CatalogOptions},
    config::{AdapterConfig, GlobalConfig},
    jobs::{AcceptanceHook, AppTerminalEnvelopeEncoder, CaptureJobSpec, JobEngine, JobHooks},
    outbox::{EdgeCommonsConfirmedPublisher, OutboxDurability, OutboxPressure, OutboxPublisher},
    registry::{CameraConnectionState, CameraRegistry, CameraStatusError},
    scheduler::{ScheduleDecision, ScheduleOccurrence, SchedulePlan},
    state_path::resolve_state_directory,
    storage::{StorageReservation, StorageRoot},
    storage_pressure::{RootPressure, StoragePressureMonitor, StoragePressureSnapshot},
};

use crate::commands::{
    self, CancelRequest, CaptureRequest, CaptureStatusMode, CaptureStatusRequest, DiscoverRequest,
    GroupCaptureRequest, ListRequest, PtzCommandRequest, PtzPresetsRequest, ReconnectRequest,
    StatusRequest,
};

// Continuations are intentionally process-local capability tokens.  They contain no request
// data and a caller cannot manufacture a valid one by guessing an offset.  A short retention
// window also bounds memory when an untrusted client deliberately abandons pages.
const CURSOR_TTL: Duration = Duration::from_secs(300);
const MAX_RETAINED_CURSORS: usize = 256;
const MAX_RETAINED_SNAPSHOT_VALUES: usize = 10_000;
const SCHEDULER_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// How often the component's queue levels are sampled into `camera_queue`.
///
/// Levels, not events: a capture that starts and finishes between two samples is still counted,
/// because counts ride the job hooks instead. So this only has to be fast enough to show a backlog
/// building, and slow enough that watching the component costs nothing -- the sample takes one
/// grouped COUNT against a catalog that the capture path is also using.
const METRIC_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
/// Ceiling on the cameras reported in one keepalive's `instances[]`.
///
/// The keepalive is published every few seconds forever, so its body must stay bounded even if a
/// configuration arrives with far more cameras than the design contemplates.
const MAX_CONNECTIVITY_INSTANCES: usize = 512;
const SCHEDULER_MISFIRE_GRACE: Duration = Duration::from_secs(5);

/// How long the capture scheduler waits before retrying a capture the durable store could not admit.
///
/// Only reached when the catalog itself is failing. Without it, a store outage turns the scheduler
/// into a spin: pop, fail, requeue, pop again, at whatever rate the CPU allows.
const SCHEDULER_RETRY_BACKOFF: Duration = Duration::from_millis(100);
// Lifecycle events are diagnostic-only. Keep their detached work bounded so a stalled broker
// cannot delay a durable acceptance, physical acquisition, or consume unbounded task memory.
const MAX_LIFECYCLE_EVENT_PUBLISHES: usize = 64;
const LIFECYCLE_EVENT_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
// Retention windows are configured in hours, so an hourly reclaim is ample and never needs to
// poll.  The catalog runs a two-worker pool that also carries the capture hot path: a sweep is
// therefore issued in small batches, paced between them, so a large backlog is reclaimed over
// several bounded round trips instead of saturating the pool and failing live camera sessions.
const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(3_600);
const RETENTION_BATCH: usize = 500;
const RETENTION_MAX_BATCHES: usize = 40;
const RETENTION_BATCH_PAUSE: Duration = Duration::from_millis(50);
const MILLIS_PER_HOUR: i64 = 3_600_000;

type ReadySetter = dyn Fn(bool) + Send + Sync;

#[derive(Clone, Copy)]
struct RuntimeReadinessState {
    startup_complete: bool,
    catalog_available: bool,
    outbox_available: bool,
    state_storage_available: bool,
    stopping: bool,
}

impl RuntimeReadinessState {
    fn is_ready(self) -> bool {
        self.startup_complete
            && self.catalog_available
            && self.outbox_available
            && self.state_storage_available
            && !self.stopping
    }
}

/// Combines the one-way startup gate with independently observed durable-state availability.
///
/// A recovered outbox catalog pass cannot make the component ready before every startup gate has
/// completed, and it cannot re-enable readiness after shutdown begins. This keeps the core's
/// single boolean readiness flag honest without treating ordinary broker pressure as a storage
/// failure. State changes and the external publication are serialized so a delayed older callback
/// cannot overwrite a newer unavailable state with stale readiness.
#[derive(Clone)]
pub struct RuntimeReadiness {
    state: Arc<Mutex<RuntimeReadinessState>>,
    set_ready: Arc<ReadySetter>,
}

impl RuntimeReadiness {
    /// Creates a readiness bridge in its initial not-ready state.
    #[must_use]
    pub fn new(set_ready: Arc<ReadySetter>) -> Self {
        Self {
            state: Arc::new(Mutex::new(RuntimeReadinessState {
                startup_complete: false,
                catalog_available: true,
                outbox_available: true,
                state_storage_available: true,
                stopping: false,
            })),
            set_ready,
        }
    }

    /// Opens readiness after command routing and all runtime startup gates have completed.
    pub fn complete_startup(&self) {
        self.transition(|state| !std::mem::replace(&mut state.startup_complete, true));
    }

    /// Permanently closes readiness for ordered shutdown.
    pub fn begin_shutdown(&self) {
        self.transition(|state| !std::mem::replace(&mut state.stopping, true));
    }

    fn set_catalog_available(&self, available: bool) {
        self.transition(|state| {
            std::mem::replace(&mut state.catalog_available, available) != available
        });
    }

    fn set_outbox_available(&self, available: bool) {
        self.transition(|state| {
            std::mem::replace(&mut state.outbox_available, available) != available
        });
    }

    fn set_state_storage_available(&self, available: bool) {
        self.transition(|state| {
            std::mem::replace(&mut state.state_storage_available, available) != available
        });
    }

    fn transition(&self, transition: impl FnOnce(&mut RuntimeReadinessState) -> bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if transition(&mut state) {
            // Hold the lock while publishing. This defines the linearization point: no newer
            // availability transition can publish `false` and then be overwritten by this older
            // transition's delayed `true` callback.
            (self.set_ready)(state.is_ready());
        }
    }

    #[cfg(test)]
    fn noop() -> Self {
        Self::new(Arc::new(|_| {}))
    }
}

#[derive(Debug, Clone, PartialEq)]
enum OutboxAlarmTransition {
    Raise(serde_json::Value),
    Clear(serde_json::Value),
}

struct OutboxHealthWatchers {
    pressure: watch::Receiver<OutboxPressure>,
    durability: watch::Receiver<OutboxDurability>,
    catalog_availability: watch::Receiver<crate::catalog::CatalogAvailability>,
}

#[derive(Default)]
struct OutboxAlarmState {
    delayed: bool,
}

impl OutboxAlarmState {
    fn transition(&mut self, pressure: &OutboxPressure) -> Option<OutboxAlarmTransition> {
        if self.delayed == pressure.delayed {
            return None;
        }
        self.delayed = pressure.delayed;
        let context = outbox_pressure_context(pressure);
        if pressure.delayed {
            Some(OutboxAlarmTransition::Raise(context))
        } else {
            Some(OutboxAlarmTransition::Clear(context))
        }
    }
}

fn outbox_pressure_context(pressure: &OutboxPressure) -> serde_json::Value {
    let mut context = serde_json::Map::new();
    context.insert("pending".to_string(), pressure.pending.into());
    context.insert("oldestAgeMs".to_string(), pressure.oldest_age_ms.into());
    context.insert("maxAttempts".to_string(), pressure.max_attempts.into());
    if let Some(error) = &pressure.last_error {
        context.insert(
            "lastError".to_string(),
            serde_json::Value::String(error.clone()),
        );
    }
    serde_json::Value::Object(context)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StorageAlarmContext {
    root: String,
    free_bytes: Option<u64>,
    free_percent: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StorageAlarmTransition {
    Raise(StorageAlarmContext),
    Clear(StorageAlarmContext),
}

#[derive(Default)]
struct StorageAlarmState {
    active: Option<StorageAlarmContext>,
}

impl StorageAlarmState {
    fn transition(&mut self, snapshot: &StoragePressureSnapshot) -> Option<StorageAlarmTransition> {
        let next = snapshot.alarm_root().map(storage_alarm_context);
        match (self.active.as_ref(), next) {
            (None, None) => None,
            (Some(previous), None) => {
                let previous = previous.clone();
                self.active = None;
                Some(StorageAlarmTransition::Clear(previous))
            }
            (Some(previous), Some(next)) if previous == &next => None,
            (_, Some(next)) => {
                self.active = Some(next.clone());
                Some(StorageAlarmTransition::Raise(next))
            }
        }
    }
}

fn storage_alarm_context(root: &RootPressure) -> StorageAlarmContext {
    StorageAlarmContext {
        root: root.root.to_string_lossy().into_owned(),
        free_bytes: root.free_bytes,
        free_percent: root.free_percent,
    }
}

fn storage_alarm_json(context: StorageAlarmContext) -> serde_json::Value {
    serde_json::json!({
        "root": context.root,
        "freeBytes": context.free_bytes,
        "freePercent": context.free_percent,
    })
}

async fn publish_storage_alarm(
    events: Option<EventsFacade>,
    alarms: &Mutex<StorageAlarmState>,
    snapshot: &StoragePressureSnapshot,
) {
    let transition = alarms
        .lock()
        .ok()
        .and_then(|mut state| state.transition(snapshot));
    let (Some(events), Some(transition)) = (events, transition) else {
        return;
    };
    let result = match transition {
        StorageAlarmTransition::Raise(context) => {
            events
                .raise_alarm(
                    Severity::Critical,
                    "storage-low",
                    Some("a configured storage root cannot safely admit capture work".to_string()),
                    Some(storage_alarm_json(context)),
                )
                .await
        }
        StorageAlarmTransition::Clear(context) => {
            events
                .clear_alarm(
                    Severity::Critical,
                    "storage-low",
                    Some(storage_alarm_json(context)),
                )
                .await
        }
    };
    if let Err(error) = result {
        tracing::warn!(error = %error, "failed to publish storage-pressure alarm");
    }
}

#[derive(Debug, Clone)]
enum CursorPayload {
    Snapshot {
        values: Vec<serde_json::Value>,
        offset: usize,
        completed_at: Option<serde_json::Value>,
    },
    Jobs {
        before: Option<(i64, String)>,
    },
    List {
        cameras: Vec<serde_json::Value>,
        unconfigured: Vec<serde_json::Value>,
        offset: usize,
    },
}

#[derive(Debug, Clone)]
struct CursorEntry {
    kind: &'static str,
    query_hash: String,
    payload: CursorPayload,
    expires_at: std::time::Instant,
}

#[derive(Default)]
struct CursorStoreInner {
    entries: HashMap<String, CursorEntry>,
    insertion_order: VecDeque<String>,
}

/// Bounded retained pagination state.  All entries are verified against a canonical query hash
/// before use, which makes a cursor unusable with a different camera filter, capability view, or
/// backend selection.
#[derive(Default)]
struct CursorStore {
    inner: Mutex<CursorStoreInner>,
}

impl CursorStore {
    fn list_page(
        &self,
        query: &serde_json::Value,
        cursor: Option<&str>,
        initial: Option<(Vec<serde_json::Value>, Vec<serde_json::Value>)>,
        limit: usize,
    ) -> Result<(
        Vec<serde_json::Value>,
        Vec<serde_json::Value>,
        Option<String>,
    )> {
        let query_hash = cursor_query_hash(query)?;
        let mut inner = self.lock()?;
        Self::prune(&mut inner);
        let (cameras, unconfigured, offset) = match cursor {
            Some(cursor) => {
                let entry = inner.entries.get(cursor).ok_or_else(cursor_rejected)?;
                if entry.kind != "list" || entry.query_hash != query_hash {
                    return Err(cursor_rejected());
                }
                let CursorPayload::List {
                    cameras,
                    unconfigured,
                    offset,
                } = &entry.payload
                else {
                    return Err(cursor_rejected());
                };
                (cameras.clone(), unconfigured.clone(), *offset)
            }
            None => {
                let (cameras, unconfigured) = initial.ok_or_else(cursor_rejected)?;
                if cameras.len().saturating_add(unconfigured.len()) > MAX_RETAINED_SNAPSHOT_VALUES {
                    return Err(crate::CameraError::rejected(
                        crate::ErrorCode::InvalidRequest,
                        "result exceeds the retained snapshot bound",
                    ));
                }
                (cameras, unconfigured, 0)
            }
        };
        let total = cameras.len().saturating_add(unconfigured.len());
        let end = offset.saturating_add(limit).min(total);
        let camera_start = offset.min(cameras.len());
        let camera_end = end.min(cameras.len());
        let page_cameras = cameras[camera_start..camera_end].to_vec();
        let unconfigured_start = offset.saturating_sub(cameras.len()).min(unconfigured.len());
        let unconfigured_end = end.saturating_sub(cameras.len()).min(unconfigured.len());
        let page_unconfigured = unconfigured[unconfigured_start..unconfigured_end].to_vec();
        let next = if end < total {
            Some(Self::insert(
                &mut inner,
                "list",
                query_hash,
                CursorPayload::List {
                    cameras,
                    unconfigured,
                    offset: end,
                },
            ))
        } else {
            None
        };
        Ok((page_cameras, page_unconfigured, next))
    }

    fn snapshot_page(
        &self,
        kind: &'static str,
        query: &serde_json::Value,
        cursor: Option<&str>,
        initial: Option<Vec<serde_json::Value>>,
        initial_completed_at: Option<serde_json::Value>,
        limit: usize,
    ) -> Result<(
        Vec<serde_json::Value>,
        Option<String>,
        Option<serde_json::Value>,
    )> {
        let query_hash = cursor_query_hash(query)?;
        let mut inner = self.lock()?;
        Self::prune(&mut inner);
        let (values, offset, completed_at) = match cursor {
            Some(cursor) => {
                let entry = inner.entries.get(cursor).ok_or_else(cursor_rejected)?;
                if entry.kind != kind || entry.query_hash != query_hash {
                    return Err(cursor_rejected());
                }
                let CursorPayload::Snapshot {
                    values,
                    offset,
                    completed_at,
                } = &entry.payload
                else {
                    return Err(cursor_rejected());
                };
                (values.clone(), *offset, completed_at.clone())
            }
            None => {
                let values = initial.ok_or_else(cursor_rejected)?;
                if values.len() > MAX_RETAINED_SNAPSHOT_VALUES {
                    return Err(crate::CameraError::rejected(
                        crate::ErrorCode::InvalidRequest,
                        "result exceeds the retained snapshot bound",
                    ));
                }
                (values, 0, initial_completed_at)
            }
        };
        let end = offset.saturating_add(limit).min(values.len());
        let page = values[offset..end].to_vec();
        let next = if end < values.len() {
            Some(Self::insert(
                &mut inner,
                kind,
                query_hash,
                CursorPayload::Snapshot {
                    values,
                    offset: end,
                    completed_at: completed_at.clone(),
                },
            ))
        } else {
            None
        };
        Ok((page, next, completed_at))
    }

    fn job_before(
        &self,
        query: &serde_json::Value,
        cursor: Option<&str>,
    ) -> Result<Option<(i64, String)>> {
        let query_hash = cursor_query_hash(query)?;
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        let mut inner = self.lock()?;
        Self::prune(&mut inner);
        let entry = inner.entries.get(cursor).ok_or_else(cursor_rejected)?;
        if entry.kind != "capture-status-list" || entry.query_hash != query_hash {
            return Err(cursor_rejected());
        }
        let CursorPayload::Jobs { before } = &entry.payload else {
            return Err(cursor_rejected());
        };
        Ok(before.clone())
    }

    fn next_job_cursor(&self, query: &serde_json::Value, before: (i64, String)) -> Result<String> {
        let query_hash = cursor_query_hash(query)?;
        let mut inner = self.lock()?;
        Self::prune(&mut inner);
        Ok(Self::insert(
            &mut inner,
            "capture-status-list",
            query_hash,
            CursorPayload::Jobs {
                before: Some(before),
            },
        ))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, CursorStoreInner>> {
        self.inner.lock().map_err(|_| {
            crate::CameraError::Catalog("retained cursor store is unavailable".to_string())
        })
    }

    fn insert(
        inner: &mut CursorStoreInner,
        kind: &'static str,
        query_hash: String,
        payload: CursorPayload,
    ) -> String {
        Self::prune(inner);
        while inner.entries.len() >= MAX_RETAINED_CURSORS {
            let Some(oldest) = inner.insertion_order.pop_front() else {
                break;
            };
            inner.entries.remove(&oldest);
        }
        let token = format!("cur_{}", uuid::Uuid::now_v7());
        inner.insertion_order.push_back(token.clone());
        inner.entries.insert(
            token.clone(),
            CursorEntry {
                kind,
                query_hash,
                payload,
                expires_at: std::time::Instant::now() + CURSOR_TTL,
            },
        );
        token
    }

    fn prune(inner: &mut CursorStoreInner) {
        let now = std::time::Instant::now();
        inner.entries.retain(|_, entry| entry.expires_at > now);
        inner
            .insertion_order
            .retain(|token| inner.entries.contains_key(token));
    }
}

fn cursor_query_hash(query: &serde_json::Value) -> Result<String> {
    crate::idempotency::canonical_request_hash(query, false).map(|hash| hash.to_hex())
}

fn cursor_rejected() -> crate::CameraError {
    crate::CameraError::rejected(
        crate::ErrorCode::InvalidRequest,
        "cursor is unknown, expired, or does not match this query",
    )
}

fn candidate_is_configured(
    candidate: &DiscoveryCandidate,
    instances: &[crate::config::CameraConfig],
) -> bool {
    instances.iter().any(|camera| {
        if camera.backend.kind() != candidate.backend {
            return false;
        }
        match &camera.backend {
            crate::config::BackendConfig::GenicamAravis(config) => {
                let selector = &config.selector;
                selector_value_matches(&candidate.selector, "serial", selector.serial.as_deref())
                    || selector_value_matches(&candidate.selector, "mac", selector.mac.as_deref())
                    || selector_value_matches(
                        &candidate.selector,
                        "deviceId",
                        selector.device_id.as_deref(),
                    )
                    || selector_value_matches(&candidate.selector, "ip", selector.ip.as_deref())
            }
            crate::config::BackendConfig::OnvifRtsp(config) => {
                selector_value_matches(
                    &candidate.selector,
                    "endpointReference",
                    config
                        .selector
                        .as_ref()
                        .map(|selector| selector.endpoint_reference.as_str()),
                ) || selector_value_matches(
                    &candidate.selector,
                    "deviceServiceUrl",
                    config.device_service_url.as_deref(),
                )
            }
            crate::config::BackendConfig::Sim(_) => false,
        }
    })
}

fn selector_value_matches(
    selector: &serde_json::Value,
    field: &str,
    configured: Option<&str>,
) -> bool {
    configured.is_some_and(|configured| {
        selector
            .get(field)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|observed| observed == configured)
    })
}

/// Most recent bounded, credential-free discovery observation.  The cache is intentionally
/// in-memory: it is an operator aid, never a configuration or camera-claim mechanism.
#[derive(Default)]
struct DiscoveryCache {
    candidates: Vec<DiscoveryCandidate>,
}

/// Live composition of the protocol-neutral durable engine and per-camera protocol actors.
///
/// The object owns no global singleton camera state.  Each supervisor receives a fresh backend
/// factory/session on every retry and retains only the compact registry snapshot and actor handle.
/// This is important for reload/shutdown correctness: a stale actor can never acquire work after
/// its supervisor cancellation token is observed.
pub struct CameraRuntime {
    config: RwLock<AdapterConfig>,
    backend_context: BackendRuntimeContext,
    catalog: Catalog,
    admission: AdmissionController,
    storage: StorageRoot,
    registry: Arc<CameraRegistry>,
    engines: RwLock<BTreeMap<String, JobEngine>>,
    events: RwLock<BTreeMap<String, EventsFacade>>,
    outbox_events: Option<EventsFacade>,
    storage_pressure: Option<StoragePressureMonitor>,
    storage_alarm: Arc<Mutex<StorageAlarmState>>,
    readiness: RuntimeReadiness,
    /// Capture counters and sampled queue levels. See `observability::CaptureMetrics`.
    metrics: Arc<crate::observability::CaptureMetrics>,
    actors: Arc<RwLock<HashMap<String, CameraActorHandle>>>,
    /// Per-supervisor shutdown tokens.  A child token also observes process shutdown, while a
    /// reload can retire one connecting/backing-off supervisor without stopping the whole runtime.
    supervisor_cancellations: Arc<RwLock<HashMap<String, CancellationToken>>>,
    /// Completion signals paired with [`Self::supervisor_cancellations`].  A replacement waits
    /// for the old loop to retire before publishing a new actor for the same camera ID.
    supervisor_finished: Arc<RwLock<HashMap<String, CancellationToken>>>,
    session_cancellations: Arc<RwLock<HashMap<String, CancellationToken>>>,
    scheduler_cancellations: RwLock<HashMap<(String, String), CancellationToken>>,
    discovery_cancellation: RwLock<Option<CancellationToken>>,
    discovery_cache: Mutex<DiscoveryCache>,
    /// The fleet-wide capture queue. One object, replacing N per-camera dispatch caches.
    scheduler: crate::dispatch::CaptureScheduler,
    cancellation: CancellationToken,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    connect_gate: Arc<Semaphore>,
    waiters: Arc<RuntimeJobHooks>,
    cursors: CursorStore,
    reload_gate: tokio::sync::Mutex<()>,
    reloading: AtomicBool,
    self_reference: OnceLock<Weak<Self>>,
}

/// Clears the command/schedule reload fence even if an async reload future is cancelled while it
/// awaits a supervisor.  Core can then invoke the prepared transaction's rollback rather than
/// leaving every subsequent command permanently rejected as "reloading".
struct ReloadInProgressGuard<'a>(&'a AtomicBool);

impl Drop for ReloadInProgressGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// The mutable runtime state needed to restore a prior committed camera generation after a
/// prepared replacement fails.  Durable job records are deliberately not cloned: a failed
/// transition never reaches the incompatible-queue interruption stage, and a successful
/// transition is never rolled back after Core publishes its new snapshot.
#[derive(Clone)]
struct RuntimeReloadCheckpoint {
    config: AdapterConfig,
    engines: BTreeMap<String, JobEngine>,
    events: BTreeMap<String, EventsFacade>,
}

/// Process-local core deferred tokens paired with durable waiter records. The durable row makes
/// acceptance auditable; only the opaque token stays in memory, because routing remains owned by
/// the EdgeCommons command inbox.
struct RuntimeJobHooks {
    catalog: Catalog,
    runtime: Mutex<Weak<CameraRuntime>>,
    lifecycle_event_slots: Arc<Semaphore>,
    tokens: Mutex<HashMap<String, Vec<(String, DeferredReplyToken)>>>,
    group_tokens: Mutex<HashMap<String, Vec<DeferredReplyToken>>>,
    pending: Mutex<HashMap<(String, String), PendingDeferredWaiter>>,
}

type PendingDeferredWaiter = (String, DeferredReplyToken, String, String);

impl RuntimeJobHooks {
    fn new(catalog: Catalog) -> Self {
        Self {
            catalog,
            runtime: Mutex::new(Weak::new()),
            lifecycle_event_slots: Arc::new(Semaphore::new(MAX_LIFECYCLE_EVENT_PUBLISHES)),
            tokens: Mutex::new(HashMap::new()),
            group_tokens: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
        }
    }

    fn attach_runtime(&self, runtime: Weak<CameraRuntime>) {
        match self.runtime.lock() {
            Ok(mut attached) => *attached = runtime,
            Err(_) => tracing::warn!("could not attach lifecycle-event routing to runtime hooks"),
        }
    }

    fn runtime(&self) -> Option<Arc<CameraRuntime>> {
        self.runtime.lock().ok()?.upgrade()
    }

    fn register(
        &self,
        capture_id: String,
        waiter_id: String,
        token: DeferredReplyToken,
    ) -> Result<()> {
        self.tokens
            .lock()
            .map_err(|_| {
                crate::CameraError::Catalog("deferred waiter registry is unavailable".to_string())
            })?
            .entry(capture_id)
            .or_default()
            .push((waiter_id, token));
        Ok(())
    }

    fn register_group(&self, group_id: String, token: DeferredReplyToken) -> Result<()> {
        self.group_tokens
            .lock()
            .map_err(|_| {
                crate::CameraError::Catalog(
                    "deferred group waiter registry is unavailable".to_string(),
                )
            })?
            .entry(group_id)
            .or_default()
            .push(token);
        Ok(())
    }

    async fn settle_group_waiters(&self, group: &crate::catalog::GroupRecord) {
        let tokens = match self.group_tokens.lock() {
            Ok(mut tokens) => tokens.remove(&group.group_id).unwrap_or_default(),
            Err(_) => return,
        };
        let body = group_terminal_json(group);
        for token in tokens {
            let _ = token.settle_success(Some(body.clone())).await;
        }
    }

    fn prepare(
        &self,
        instance: String,
        request_id: String,
        waiter_id: String,
        token: DeferredReplyToken,
        correlation_id: String,
        request_uuid: String,
    ) -> Result<()> {
        let mut pending = self.pending.lock().map_err(|_| {
            crate::CameraError::Catalog("deferred waiter registry is unavailable".to_string())
        })?;
        if pending
            .insert(
                (instance, request_id),
                (waiter_id, token, correlation_id, request_uuid),
            )
            .is_some()
        {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::IdempotencyConflict,
                "a deferred request is already being accepted",
            ));
        }
        Ok(())
    }

    fn take_pending(
        &self,
        instance: &str,
        request_id: &str,
    ) -> Option<(String, DeferredReplyToken, String, String)> {
        self.pending
            .lock()
            .ok()?
            .remove(&(instance.to_owned(), request_id.to_owned()))
    }

    async fn complete_group_if_terminal(&self, group_id: &str) {
        let Ok(Some(group)) = self.catalog.group(group_id).await else {
            return;
        };
        if group.state.is_terminal()
            || group
                .members
                .iter()
                .any(|member| !member.state.is_terminal())
        {
            return;
        }
        let succeeded = group
            .members
            .iter()
            .filter(|member| member.state == crate::model::JobState::Succeeded)
            .count();
        // The durable catalog uses the shared job-state enum. `PARTIAL` is a public aggregate
        // presentation state, while mixed groups retain FAILED durably with their complete member
        // result vector. This preserves strict SQLite state validation and exact member evidence.
        let state = if succeeded == group.members.len() {
            crate::model::JobState::Succeeded
        } else {
            crate::model::JobState::Failed
        };
        let completed = self
            .catalog
            .complete_group(
                group.group_id.clone(),
                state,
                group_terminal_json(&group),
                None,
                None,
                chrono::Utc::now().timestamp_millis(),
            )
            .await;
        if let Ok(group) = completed {
            self.settle_group_waiters(&group).await;
        }
    }
}

#[async_trait]
impl JobHooks for RuntimeJobHooks {
    async fn capture_queued(
        &self,
        record: &crate::catalog::JobRecord,
        spec: &CaptureJobSpec,
        queue_position: usize,
    ) {
        // Counted HERE, and awaited, rather than inside the spawned publish below.
        //
        // The publish is deliberately best-effort: it takes a permit from a bounded pool of
        // lifecycle-event slots and gives up if none is free, because a slow event consumer must
        // never be able to stall a capture. That bound is right for EVENTS. It is catastrophic for a
        // COUNTER: a metric that silently stops counting exactly when the component is busiest is
        // worse than no metric, and it would have under-reported precisely the overload an operator
        // was looking at. It also made the count race the capture's own terminal state -- CI caught
        // that, on the same commit that passed on another runner.
        if let Some(runtime) = self.runtime() {
            runtime.metrics.count("queued").await;
        }
        let Ok(permit) = Arc::clone(&self.lifecycle_event_slots).try_acquire_owned() else {
            return;
        };
        let Some(runtime) = self.runtime() else {
            return;
        };
        let record = record.clone();
        let spec = spec.clone();
        tokio::spawn(async move {
            let _permit = permit;
            runtime
                .emit_capture_queued(record, spec, queue_position)
                .await;
        });
    }

    async fn capture_started(&self, spec: &CaptureJobSpec) {
        // Counted before the best-effort publish gate, for the reasons in `capture_queued`.
        if let Some(runtime) = self.runtime() {
            runtime.metrics.count("started").await;
        }
        let Ok(permit) = Arc::clone(&self.lifecycle_event_slots).try_acquire_owned() else {
            return;
        };
        let Some(runtime) = self.runtime() else {
            return;
        };
        let spec = spec.clone();
        tokio::spawn(async move {
            let _permit = permit;
            runtime.emit_capture_started(spec).await;
        });
    }

    async fn settle_waiters(
        &self,
        record: &crate::catalog::JobRecord,
        terminal_body: &serde_json::Value,
    ) {
        // Counted here because `after_terminal` calls this for EVERY terminal, whatever produced
        // it -- success, failure, cancellation, a deadline, an isolated panic. A capture that ends
        // must be counted once, and there is exactly one place that sees all of them.
        if let (Some(runtime), Some(measure)) = (
            self.runtime(),
            crate::observability::terminal_measure(record.state),
        ) {
            runtime.metrics.count(measure).await;
        }
        // The execution slot the scheduler was holding on this capture's behalf, released on the one
        // path every capture takes exactly once. Releasing it only on success would be a component
        // that stops scheduling after its first failure.
        if let Some(runtime) = self.runtime() {
            runtime.scheduler.capture_finished(&record.capture_id);
        }
        let tokens = match self.tokens.lock() {
            Ok(mut tokens) => tokens.remove(&record.capture_id).unwrap_or_default(),
            Err(_) => return,
        };
        for (waiter_id, token) in tokens {
            if token
                .settle_success(Some(terminal_body.clone()))
                .await
                .is_ok()
            {
                let _ = self.catalog.remove_waiter(waiter_id).await;
            }
        }
    }

    async fn group_member_terminal(
        &self,
        record: &crate::catalog::JobRecord,
        _terminal_body: &serde_json::Value,
    ) {
        if let Some(group_id) = record.group_id.as_deref() {
            self.complete_group_if_terminal(group_id).await;
        }
    }
}

#[async_trait]
impl AcceptanceHook for RuntimeJobHooks {
    async fn accepted_before_queue(&self, record: &crate::catalog::JobRecord) -> Result<()> {
        let (Some(verb), Some(request_id)) = (record.verb.as_deref(), record.request_id.as_deref())
        else {
            return Ok(());
        };
        if verb != "sb/capture" {
            return Ok(());
        }
        let Some((waiter_id, token, correlation_id, request_uuid)) =
            self.take_pending(&record.instance, request_id)
        else {
            return Ok(());
        };
        let now = chrono::Utc::now().timestamp_millis();
        self.catalog
            .add_waiter(crate::catalog::WaiterRecord {
                waiter_id: waiter_id.clone(),
                capture_id: record.capture_id.clone(),
                correlation_id,
                request_uuid: Some(request_uuid),
                expires_at_ms: record.deadlines.terminal_at_ms,
                created_at_ms: now,
            })
            .await?;
        self.register(record.capture_id.clone(), waiter_id, token)?;
        Ok(())
    }
}

/// Runtime-owned facades and platform services supplied after durable startup resources exist.
///
/// Grouping these dependencies makes the boundary explicit: protocol construction, per-camera
/// terminal/event publication, component outbox alarms, readiness, and confirmed messaging are
/// process-owned services rather than configuration values.
pub struct RuntimeServices {
    /// Per-camera application facades used to encode terminal results.
    pub apps: BTreeMap<String, Arc<AppFacade>>,
    /// Per-camera event facades used by schedules and camera lifecycle events.
    pub events: BTreeMap<String, EventsFacade>,
    /// Component-main event facade used for component-wide outbox delivery alarms.
    pub outbox_events: EventsFacade,
    /// Combined startup/durable-state readiness bridge.
    pub readiness: RuntimeReadiness,
    /// Credential/discovery/security services required to construct a backend.
    pub backend_context: BackendRuntimeContext,
    /// Confirmed local publisher for durable terminal envelopes.
    pub messaging: Arc<dyn edgecommons::messaging::MessagingService>,
    /// Component metric service. Capture counts ride the job hooks; queue levels are sampled.
    pub metrics: Arc<dyn edgecommons::metrics::MetricService>,
}

fn capture_trigger_type(trigger: &crate::messages::CaptureTrigger) -> &'static str {
    match trigger {
        crate::messages::CaptureTrigger::Command { .. } => "command",
        crate::messages::CaptureTrigger::GroupCommand { .. } => "group-command",
        crate::messages::CaptureTrigger::Schedule { .. } => "schedule",
    }
}

fn capture_mode_type(mode: crate::model::CaptureMode) -> &'static str {
    match mode {
        crate::model::CaptureMode::Simulated => "simulated",
        crate::model::CaptureMode::SoftwareTrigger => "software-trigger",
        crate::model::CaptureMode::SnapshotUri => "snapshot-uri",
        crate::model::CaptureMode::RtspFrame => "rtsp-frame",
    }
}

impl CameraRuntime {
    fn config_snapshot(&self) -> Result<AdapterConfig> {
        self.config
            .read()
            .map(|config| config.clone())
            .map_err(|_| {
                crate::CameraError::Catalog("runtime configuration lock is unavailable".to_string())
            })
    }

    /// Takes the small, shared portion of the active configuration for long-lived supervisor
    /// work.  A supervisor already reads its camera-specific configuration from the registry;
    /// cloning the complete camera roster here would retain one full copy per connection attempt
    /// while the bounded connection gate is saturated.
    fn global_config_snapshot(&self) -> Result<GlobalConfig> {
        self.config
            .read()
            .map(|config| config.global.clone())
            .map_err(|_| {
                crate::CameraError::Catalog("runtime configuration lock is unavailable".to_string())
            })
    }

    fn lifecycle_events(&self, instance: &str) -> Option<EventsFacade> {
        let config = match self.config_snapshot() {
            Ok(config) => config,
            Err(error) => {
                tracing::warn!(
                    instance,
                    error = %error,
                    "could not read capture lifecycle-event policy"
                );
                return None;
            }
        };
        if !config.global.operator_events.capture_lifecycle {
            return None;
        }
        match self.events.read() {
            Ok(events) => match events.get(instance).cloned() {
                Some(events) => Some(events),
                None => {
                    tracing::warn!(
                        instance,
                        "capture lifecycle events are enabled but no event facade is installed"
                    );
                    None
                }
            },
            Err(_) => {
                tracing::warn!(instance, "could not access capture lifecycle-event facade");
                None
            }
        }
    }

    async fn emit_capture_queued(
        &self,
        record: crate::catalog::JobRecord,
        spec: CaptureJobSpec,
        queue_position: usize,
    ) {
        let Some(events) = self.lifecycle_events(&spec.instance) else {
            return;
        };
        let context = serde_json::json!({
            "captureId": record.capture_id,
            "trigger": capture_trigger_type(&spec.trigger),
            "captureProfile": spec.profile.name,
            "queuePosition": queue_position,
        });
        match tokio::time::timeout(
            LIFECYCLE_EVENT_PUBLISH_TIMEOUT,
            events.emit(Severity::Debug, "capture-queued", None, Some(context)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    instance = %spec.instance,
                    capture_id = %spec.capture_id,
                    error = %error,
                    "could not publish best-effort capture-queued lifecycle event"
                );
            }
            Err(_) => {
                tracing::warn!(
                    instance = %spec.instance,
                    capture_id = %spec.capture_id,
                    timeout_ms = LIFECYCLE_EVENT_PUBLISH_TIMEOUT.as_millis(),
                    "best-effort capture-queued lifecycle event timed out"
                );
            }
        }
    }

    async fn emit_capture_started(&self, spec: CaptureJobSpec) {
        let Some(events) = self.lifecycle_events(&spec.instance) else {
            return;
        };
        let context = serde_json::json!({
            "captureId": spec.capture_id,
            "trigger": capture_trigger_type(&spec.trigger),
            "captureProfile": spec.profile.name,
            "captureMode": capture_mode_type(spec.profile.capture_mode),
        });
        match tokio::time::timeout(
            LIFECYCLE_EVENT_PUBLISH_TIMEOUT,
            events.emit(Severity::Info, "capture-started", None, Some(context)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    instance = %spec.instance,
                    capture_id = %spec.capture_id,
                    error = %error,
                    "could not publish best-effort capture-started lifecycle event"
                );
            }
            Err(_) => {
                tracing::warn!(
                    instance = %spec.instance,
                    capture_id = %spec.capture_id,
                    timeout_ms = LIFECYCLE_EVENT_PUBLISH_TIMEOUT.as_millis(),
                    "best-effort capture-started lifecycle event timed out"
                );
            }
        }
    }

    fn new_engine(&self, app: Arc<AppFacade>) -> JobEngine {
        JobEngine::new(
            self.catalog.clone(),
            self.admission.clone(),
            self.storage.clone(),
            Arc::new(AppTerminalEnvelopeEncoder::new(app)),
            Arc::clone(&self.waiters) as Arc<dyn JobHooks>,
        )
        .with_acceptance_hook(Arc::clone(&self.waiters) as Arc<dyn AcceptanceHook>)
    }

    /// Builds the durable runtime, recovers install-owned records, starts outbox publication and
    /// creates one lightweight supervisor for every enabled camera.
    ///
    /// The caller must have registered the command router but must not make the component ready
    /// until this method succeeds.  Camera connection failure is intentionally not a startup
    /// failure; it is represented by that camera's registry state and reconnect loop.
    pub async fn start(
        config: AdapterConfig,
        resources: StartupResources,
        services: RuntimeServices,
    ) -> Result<Arc<Self>> {
        let RuntimeServices {
            apps,
            events,
            outbox_events,
            readiness,
            backend_context,
            messaging,
            metrics,
        } = services;
        let metrics = Arc::new(crate::observability::CaptureMetrics::new(metrics));
        backend_context.validate_config(&config)?;
        let max_connection_attempts = config.global.limits.max_concurrent_connects;
        let storage_pressure = StoragePressureMonitor::new(
            resources.storage.canonical_root(),
            &resources.state_directory,
            &config.global.output,
            Arc::new(FilesystemSpaceProbe::default()),
        );
        let waiters = Arc::new(RuntimeJobHooks::new(resources.catalog.clone()));
        let mut engines = BTreeMap::new();
        // One queue for the whole fleet. There is no longer a dispatch cache per camera, so a camera
        // that is added by a reload needs nothing built for it: it simply becomes a camera the
        // scheduler can hand work to.
        let scheduler = crate::dispatch::CaptureScheduler::new(&config.global.limits)?;
        for camera in &config.instances {
            let app = apps.get(&camera.id).ok_or_else(|| {
                crate::CameraError::Catalog(format!(
                    "missing instance application facade for camera '{}'",
                    camera.id
                ))
            })?;
            if !events.contains_key(&camera.id) {
                return Err(crate::CameraError::Catalog(format!(
                    "missing instance events facade for camera '{}'",
                    camera.id
                )));
            }
            engines.insert(
                camera.id.clone(),
                JobEngine::new(
                    resources.catalog.clone(),
                    resources.admission.clone(),
                    resources.storage.clone(),
                    Arc::new(AppTerminalEnvelopeEncoder::new(Arc::clone(app))),
                    Arc::clone(&waiters) as Arc<dyn JobHooks>,
                )
                .with_acceptance_hook(Arc::clone(&waiters) as Arc<dyn AcceptanceHook>),
            );
        }

        let runtime = Arc::new(Self {
            config: RwLock::new(config),
            backend_context,
            catalog: resources.catalog,
            admission: resources.admission,
            storage: resources.storage,
            registry: resources.registry,
            engines: RwLock::new(engines),
            events: RwLock::new(events),
            outbox_events: Some(outbox_events),
            storage_pressure: Some(storage_pressure),
            storage_alarm: Arc::new(Mutex::new(StorageAlarmState::default())),
            readiness,
            metrics,
            actors: Arc::new(RwLock::new(HashMap::new())),
            supervisor_cancellations: Arc::new(RwLock::new(HashMap::new())),
            supervisor_finished: Arc::new(RwLock::new(HashMap::new())),
            session_cancellations: Arc::new(RwLock::new(HashMap::new())),
            scheduler_cancellations: RwLock::new(HashMap::new()),
            discovery_cancellation: RwLock::new(None),
            discovery_cache: Mutex::new(DiscoveryCache::default()),
            scheduler,
            cancellation: CancellationToken::new(),
            tasks: Mutex::new(Vec::new()),
            connect_gate: Arc::new(Semaphore::new(max_connection_attempts)),
            waiters: Arc::clone(&waiters),
            cursors: CursorStore::default(),
            reload_gate: tokio::sync::Mutex::new(()),
            reloading: AtomicBool::new(false),
            self_reference: OnceLock::new(),
        });
        let _ = runtime.self_reference.set(Arc::downgrade(&runtime));
        waiters.attach_runtime(Arc::downgrade(&runtime));

        runtime.refresh_storage_pressure().await;
        runtime.start_capture_scheduler()?;
        runtime.start_storage_pressure_monitor()?;
        runtime.start_metric_sampler()?;
        runtime.recover_install_owned().await?;
        runtime.start_outbox(messaging)?;
        runtime.start_supervisors()?;
        runtime.start_schedulers()?;
        runtime.start_periodic_discovery()?;
        runtime.start_retention()?;
        Ok(runtime)
    }

    /// The compact configured registry used by command/status code.
    #[must_use]
    pub fn registry(&self) -> Arc<CameraRegistry> {
        Arc::clone(&self.registry)
    }

    /// Returns the currently connected actor.  Offline submission is not allowed to fabricate a
    /// queue entry: the caller must decide according to the immutable profile's offline policy.
    pub fn actor(&self, instance: &str) -> Result<CameraActorHandle> {
        self.actors
            .read()
            .map_err(|_| {
                crate::CameraError::Catalog("camera actor map is unavailable".to_string())
            })?
            .get(instance)
            .cloned()
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::CameraUnavailable,
                    format!("camera instance '{instance}' is not connected"),
                )
            })
    }

    /// Returns the durable job engine for one configured camera.
    pub fn engine(&self, instance: &str) -> Result<JobEngine> {
        self.engines
            .read()
            .map_err(|_| {
                crate::CameraError::Catalog("camera engine map is unavailable".to_string())
            })?
            .get(instance)
            .cloned()
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::UnknownInstance,
                    format!("camera instance '{instance}' is not configured"),
                )
            })
    }

    /// The fleet capture queue, checked against a camera that is actually configured.
    ///
    /// This used to hand back that camera's own dispatch cache. There is one queue now, and the
    /// instance check is what the map lookup used to give for free: a capture for a camera that does
    /// not exist must be rejected before anything durable is written for it.
    pub fn dispatcher(&self, instance: &str) -> Result<crate::dispatch::CaptureScheduler> {
        self.registry.snapshot(instance)?;
        Ok(self.scheduler.clone())
    }

    /// Resolves and durably accepts one single-camera capture before exposing it to the persistent
    /// supervisor queue.  The returned catalog outcome is safe to use for both direct and
    /// submitted command semantics; duplicate keys never re-resolve a changed profile.
    #[allow(clippy::too_many_arguments)] // Stable command fields are intentionally explicit at this boundary.
    pub async fn submit_capture(
        &self,
        instance: String,
        request_id: String,
        requested_profile: Option<String>,
        timeout_ms: Option<u64>,
        metadata: serde_json::Map<String, serde_json::Value>,
        correlation_id: String,
        verb: &str,
        priority: crate::admission::CapturePriority,
    ) -> Result<crate::catalog::AcceptJobOutcome> {
        // Idempotency is deliberately based on caller-owned immutable arguments, before any
        // config defaults, generated identifiers, deadlines, or output paths are resolved.
        // An exact retry after a reload must return the original durable job rather than compare
        // a new resolution against its first acceptance.
        let mut canonical_arguments = serde_json::Map::new();
        canonical_arguments.insert(
            "instance".to_string(),
            serde_json::Value::String(instance.clone()),
        );
        canonical_arguments.insert(
            "requestId".to_string(),
            serde_json::Value::String(request_id.clone()),
        );
        if let Some(profile) = requested_profile.as_ref() {
            canonical_arguments.insert(
                "captureProfile".to_string(),
                serde_json::Value::String(profile.clone()),
            );
        }
        if let Some(timeout_ms) = timeout_ms {
            canonical_arguments.insert("timeoutMs".to_string(), timeout_ms.into());
        }
        canonical_arguments.insert(
            "metadata".to_string(),
            serde_json::Value::Object(metadata.clone()),
        );
        let canonical = serde_json::Value::Object(canonical_arguments);
        let request_hash = crate::idempotency::canonical_request_hash(&canonical, false)?;
        let ledger_key =
            crate::catalog::LedgerKey::new(instance.clone(), verb, request_id.clone())?;
        if let Some(existing) = self.catalog.job_by_ledger(ledger_key.clone()).await? {
            return Ok(if existing.request_hash == request_hash {
                crate::catalog::AcceptJobOutcome::Existing(existing)
            } else {
                crate::catalog::AcceptJobOutcome::Conflict
            });
        }

        let config = self.config_snapshot()?;
        let camera = self.registry.camera_config(&instance)?;
        let profile_name =
            requested_profile.unwrap_or_else(|| camera.default_capture_profile.clone());
        let profile = camera
            .capture_profiles
            .get(&profile_name)
            .cloned()
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::UnknownCaptureProfile,
                    "capture profile is not configured",
                )
            })?;
        let accepted_at_ms = chrono::Utc::now().timestamp_millis();
        let terminal_ms = timeout_ms
            .or(profile.timeout_ms)
            .unwrap_or(config.global.timeouts.job_terminal_ms);
        let capture_mode = profile
            .capture_mode
            .unwrap_or_else(|| match &camera.backend {
                crate::config::BackendConfig::Sim(_) => crate::model::CaptureMode::Simulated,
                crate::config::BackendConfig::GenicamAravis(_) => {
                    crate::model::CaptureMode::SoftwareTrigger
                }
                crate::config::BackendConfig::OnvifRtsp(config) => config.capture_mode,
            });
        let snapshot = self.registry.snapshot(&instance)?;
        let camera_summary = crate::messages::CameraSummary {
            backend: snapshot.backend,
            vendor: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.vendor.clone()),
            model: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.model.clone()),
            firmware: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.firmware.clone()),
            serial: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.serial.clone()),
        };
        let capture_id = format!("cap_{}", uuid::Uuid::now_v7());
        let deadlines = crate::catalog::JobDeadlines {
            terminal_at_ms: accepted_at_ms
                .saturating_add(i64::try_from(terminal_ms).unwrap_or(i64::MAX)),
            queue_at_ms: profile.queue_expiry_ms.map(|duration| {
                accepted_at_ms.saturating_add(i64::try_from(duration).unwrap_or(i64::MAX))
            }),
            capture_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.capture_ms).unwrap_or(i64::MAX),
            ),
            encode_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.encode_ms).unwrap_or(i64::MAX),
            ),
            persist_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.persist_ms).unwrap_or(i64::MAX),
            ),
        };
        let relative_path = crate::storage::render_output_path(
            &config.global.output,
            crate::storage::OutputPathVariables {
                camera_id: &instance,
                capture_id: &capture_id,
                timestamp: chrono::Utc::now(),
            },
            profile.output.encoding,
        )?;
        let offline_policy = profile
            .offline_policy
            .unwrap_or(crate::config::OfflinePolicy::WaitUntilDeadline);
        let profile_snapshot = crate::jobs::JobProfileSnapshot {
            name: profile_name.clone(),
            capture: profile.clone(),
            offline_policy,
            maximum_frame_bytes: profile
                .maximum_frame_bytes
                .unwrap_or(config.global.limits.max_frame_bytes_per_camera),
            capture_mode,
            capture_interlock: profile
                .capture_interlock
                .unwrap_or(camera.ptz.capture_interlock),
            settle_ms: camera.ptz.settle_ms,
        };
        let trigger = crate::messages::CaptureTrigger::Command {
            request_id: request_id.clone(),
        };
        if let Err(error) = self.ensure_storage_capacity().await {
            if error.code() == crate::ErrorCode::StoragePressure {
                if let Some(existing) = self.catalog.job_by_ledger(ledger_key.clone()).await? {
                    return Ok(if existing.request_hash == request_hash {
                        crate::catalog::AcceptJobOutcome::Existing(existing)
                    } else {
                        crate::catalog::AcceptJobOutcome::Conflict
                    });
                }
            }
            return Err(error);
        }
        let job = crate::catalog::NewJob {
            capture_id: capture_id.clone(),
            instance: instance.clone(),
            ledger_key: Some(ledger_key),
            request_hash,
            canonical_request: canonical,
            effective_profile: serde_json::to_value(&profile_snapshot)?,
            deadlines: deadlines.clone(),
            trigger: serde_json::to_value(&trigger)?,
            origin_correlation_id: Some(correlation_id.clone()),
            intended_output: serde_json::json!({ "relativePath": relative_path.as_wire_path(), "backend": snapshot.backend.as_str() }),
            accepted_at_ms,
            group_id: None,
        };
        let submission = crate::jobs::JobSubmission {
            job,
            spec: crate::jobs::CaptureJobSpec {
                capture_id,
                instance: instance.clone(),
                profile: profile_snapshot,
                resource_group: camera.resource_group.clone(),
                relative_path,
                deadlines,
                accepted_at_ms,
                trigger,
                correlation_id,
                metadata,
                camera: camera_summary,
                group_size: None,
            },
            priority,
        };
        let dispatcher = self.dispatcher(&instance)?;
        self.engine(&instance)?
            .accept_and_queue(&dispatcher, submission)
            .await
    }

    #[allow(clippy::too_many_arguments)] // The group builder preserves each immutable acceptance fact.
    async fn build_group_submission(
        &self,
        instance: &str,
        request_id: &str,
        capture_group_id: &str,
        requested_profile: Option<&str>,
        timeout_ms: Option<u64>,
        metadata: serde_json::Map<String, serde_json::Value>,
        correlation_id: String,
    ) -> Result<crate::jobs::JobSubmission> {
        let config = self.config_snapshot()?;
        let camera = self.registry.camera_config(instance)?;
        if !camera.enabled {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::CameraDisabled,
                "camera is disabled",
            ));
        }
        let profile_name = requested_profile
            .map(str::to_owned)
            .unwrap_or_else(|| camera.default_capture_profile.clone());
        let profile = camera
            .capture_profiles
            .get(&profile_name)
            .cloned()
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::UnknownCaptureProfile,
                    "capture profile is not configured",
                )
            })?;
        let accepted_at_ms = chrono::Utc::now().timestamp_millis();
        let terminal_ms = timeout_ms
            .or(profile.timeout_ms)
            .unwrap_or(config.global.timeouts.job_terminal_ms);
        let capture_mode = profile
            .capture_mode
            .unwrap_or_else(|| match &camera.backend {
                crate::config::BackendConfig::Sim(_) => crate::model::CaptureMode::Simulated,
                crate::config::BackendConfig::GenicamAravis(_) => {
                    crate::model::CaptureMode::SoftwareTrigger
                }
                crate::config::BackendConfig::OnvifRtsp(config) => config.capture_mode,
            });
        let snapshot = self.registry.snapshot(instance)?;
        let camera_summary = crate::messages::CameraSummary {
            backend: snapshot.backend,
            vendor: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.vendor.clone()),
            model: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.model.clone()),
            firmware: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.firmware.clone()),
            serial: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.serial.clone()),
        };
        let capture_id = format!("cap_{}", uuid::Uuid::now_v7());
        let deadlines = crate::catalog::JobDeadlines {
            terminal_at_ms: accepted_at_ms
                .saturating_add(i64::try_from(terminal_ms).unwrap_or(i64::MAX)),
            queue_at_ms: profile.queue_expiry_ms.map(|duration| {
                accepted_at_ms.saturating_add(i64::try_from(duration).unwrap_or(i64::MAX))
            }),
            capture_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.capture_ms).unwrap_or(i64::MAX),
            ),
            encode_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.encode_ms).unwrap_or(i64::MAX),
            ),
            persist_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.persist_ms).unwrap_or(i64::MAX),
            ),
        };
        let relative_path = crate::storage::render_output_path(
            &config.global.output,
            crate::storage::OutputPathVariables {
                camera_id: instance,
                capture_id: &capture_id,
                timestamp: chrono::Utc::now(),
            },
            profile.output.encoding,
        )?;
        let profile_snapshot = crate::jobs::JobProfileSnapshot {
            name: profile_name.clone(),
            capture: profile.clone(),
            offline_policy: profile
                .offline_policy
                .unwrap_or(crate::config::OfflinePolicy::WaitUntilDeadline),
            maximum_frame_bytes: profile
                .maximum_frame_bytes
                .unwrap_or(config.global.limits.max_frame_bytes_per_camera),
            capture_mode,
            capture_interlock: profile
                .capture_interlock
                .unwrap_or(camera.ptz.capture_interlock),
            settle_ms: camera.ptz.settle_ms,
        };
        let trigger = crate::messages::CaptureTrigger::GroupCommand {
            request_id: request_id.to_owned(),
            capture_group_id: capture_group_id.to_owned(),
        };
        let canonical = serde_json::json!({
            "requestId": request_id,
            "captureGroupId": capture_group_id,
            "instance": instance,
            "captureProfile": profile_name,
            "timeoutMs": terminal_ms,
            "metadata": metadata,
            "effectiveProfile": profile_snapshot,
            "deadlines": {
                "terminalAtMs": deadlines.terminal_at_ms,
                "queueAtMs": deadlines.queue_at_ms,
                "captureAtMs": deadlines.capture_at_ms,
                "encodeAtMs": deadlines.encode_at_ms,
                "persistAtMs": deadlines.persist_at_ms,
            },
            "intendedOutput": { "relativePath": relative_path.as_wire_path(), "backend": snapshot.backend.as_str() },
        });
        let job = crate::catalog::NewJob {
            capture_id: capture_id.clone(),
            instance: instance.to_owned(),
            ledger_key: None,
            request_hash: crate::idempotency::canonical_request_hash(&canonical, false)?,
            canonical_request: canonical,
            effective_profile: serde_json::to_value(&profile_snapshot)?,
            deadlines: deadlines.clone(),
            trigger: serde_json::to_value(&trigger)?,
            origin_correlation_id: Some(correlation_id.clone()),
            intended_output: serde_json::json!({ "relativePath": relative_path.as_wire_path(), "backend": snapshot.backend.as_str() }),
            accepted_at_ms,
            group_id: Some(capture_group_id.to_owned()),
        };
        Ok(crate::jobs::JobSubmission {
            job,
            spec: crate::jobs::CaptureJobSpec {
                capture_id,
                instance: instance.to_owned(),
                profile: profile_snapshot,
                resource_group: camera.resource_group.clone(),
                relative_path,
                deadlines,
                accepted_at_ms,
                trigger,
                correlation_id,
                metadata,
                camera: camera_summary,
                group_size: Some(2), // replaced by the caller with the exact validated member count.
            },
            priority: crate::admission::CapturePriority::Direct,
        })
    }

    async fn submit_group(
        &self,
        body: GroupCaptureRequest,
        correlation_id: String,
        priority: crate::admission::CapturePriority,
        deferred_token: Option<DeferredReplyToken>,
    ) -> Result<crate::catalog::GroupRecord> {
        // Preserve only caller-owned group arguments in the idempotency record.  Member capture
        // IDs, group IDs, default profiles, deadlines, and output paths are acceptance facts;
        // including them here would turn an exact retry into a conflict.
        let mut canonical_arguments = serde_json::Map::new();
        canonical_arguments.insert(
            "requestId".to_string(),
            serde_json::Value::String(body.request_id.clone()),
        );
        canonical_arguments.insert(
            "instances".to_string(),
            serde_json::to_value(&body.instances)?,
        );
        if let Some(profile) = body.capture_profile.as_ref() {
            canonical_arguments.insert(
                "captureProfile".to_string(),
                serde_json::Value::String(profile.clone()),
            );
        }
        canonical_arguments.insert(
            "profileOverrides".to_string(),
            serde_json::to_value(&body.profile_overrides)?,
        );
        if let Some(timeout_ms) = body.timeout_ms {
            canonical_arguments.insert("timeoutMs".to_string(), timeout_ms.into());
        }
        canonical_arguments.insert(
            "metadata".to_string(),
            serde_json::Value::Object(body.metadata.clone()),
        );
        let canonical = serde_json::Value::Object(canonical_arguments);
        let request_hash = crate::idempotency::canonical_request_hash(&canonical, true)?;
        let ledger_key =
            crate::catalog::LedgerKey::new("main", "sb/capture-group", body.request_id.clone())?;
        if let Some(group) = self.catalog.group_by_ledger(ledger_key.clone()).await? {
            if group.request_hash != request_hash {
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::IdempotencyConflict,
                    "requestId was already used with different immutable group arguments",
                ));
            }
            if let Some(token) = deferred_token {
                if group.state.is_terminal() {
                    token
                        .settle_success(Some(group_terminal_json(&group)))
                        .await
                        .map_err(|_| {
                            crate::CameraError::rejected(
                                crate::ErrorCode::BackendError,
                                "deferred group reply could not be settled",
                            )
                        })?;
                } else {
                    self.waiters.register_group(group.group_id.clone(), token)?;
                }
            }
            return Ok(group);
        }

        let config = self.config_snapshot()?;
        body.validate(
            config.global.limits.max_cameras_per_group,
            config.global.limits.max_metadata_bytes,
        )?;
        // Resolve every member before creating any durable row. This gives the all-or-nothing
        // error surface required by the group contract.
        for instance in &body.instances {
            let _ = self.registry.resolve_actuation_instance(Some(instance))?;
        }
        let group_id = format!("grp_{}", uuid::Uuid::now_v7());
        let mut submissions = Vec::with_capacity(body.instances.len());
        for instance in &body.instances {
            let selected = body
                .profile_overrides
                .get(instance)
                .map(String::as_str)
                .or(body.capture_profile.as_deref());
            let mut submission = self
                .build_group_submission(
                    instance,
                    &body.request_id,
                    &group_id,
                    selected,
                    body.timeout_ms,
                    body.metadata.clone(),
                    correlation_id.clone(),
                )
                .await?;
            submission.priority = priority;
            submission.spec.group_size = Some(body.instances.len());
            submissions.push(submission);
        }
        let new_group = crate::catalog::NewGroup {
            group_id: group_id.clone(),
            ledger_key: ledger_key.clone(),
            request_hash,
            canonical_request: canonical,
            origin_correlation_id: Some(correlation_id),
            accepted_at_ms: chrono::Utc::now().timestamp_millis(),
            members: submissions
                .iter()
                .map(|submission| submission.job.clone())
                .collect(),
        };
        if let Err(error) = self.ensure_storage_capacity().await {
            if error.code() == crate::ErrorCode::StoragePressure {
                if let Some(group) = self.catalog.group_by_ledger(ledger_key.clone()).await? {
                    if group.request_hash != request_hash {
                        return Err(crate::CameraError::rejected(
                            crate::ErrorCode::IdempotencyConflict,
                            "requestId was already used with different immutable group arguments",
                        ));
                    }
                    if let Some(token) = deferred_token {
                        if group.state.is_terminal() {
                            token
                                .settle_success(Some(group_terminal_json(&group)))
                                .await
                                .map_err(|_| {
                                    crate::CameraError::rejected(
                                        crate::ErrorCode::BackendError,
                                        "deferred group reply could not be settled",
                                    )
                                })?;
                        } else {
                            self.waiters.register_group(group.group_id.clone(), token)?;
                        }
                    }
                    return Ok(group);
                }
            }
            return Err(error);
        }
        let outcome = self.catalog.accept_group(new_group).await?;
        let group = match outcome {
            crate::catalog::AcceptGroupOutcome::Inserted(group) => group,
            crate::catalog::AcceptGroupOutcome::Existing(group) => {
                if let Some(token) = deferred_token {
                    if group.state.is_terminal() {
                        token
                            .settle_success(Some(group_terminal_json(&group)))
                            .await
                            .map_err(|_| {
                                crate::CameraError::rejected(
                                    crate::ErrorCode::BackendError,
                                    "deferred group reply could not be settled",
                                )
                            })?;
                    } else {
                        self.waiters.register_group(group.group_id.clone(), token)?;
                    }
                }
                return Ok(group);
            }
            crate::catalog::AcceptGroupOutcome::Conflict => {
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::IdempotencyConflict,
                    "requestId was already used with different immutable group arguments",
                ));
            }
        };
        if let Some(token) = deferred_token {
            self.waiters.register_group(group.group_id.clone(), token)?;
        }
        // Group ACCEPTED and QUEUED are separate durable commits. Queue every member in one
        // catalog transaction before exposing any descriptor, then hand those already-queued
        // records to their independent camera supervisors.
        self.catalog
            .queue_group(
                group.group_id.clone(),
                chrono::Utc::now().timestamp_millis(),
            )
            .await?;
        for submission in submissions {
            let dispatcher = self.dispatcher(&submission.spec.instance)?;
            self.engine(&submission.spec.instance)?
                .queue_preaccepted(&dispatcher, submission)
                .await?;
        }
        self.waiters
            .complete_group_if_terminal(&group.group_id)
            .await;
        self.catalog
            .group(group.group_id.clone())
            .await?
            .ok_or_else(|| {
                crate::CameraError::Catalog("accepted capture group disappeared".to_string())
            })
    }

    async fn discover(&self, body: DiscoverRequest) -> Result<serde_json::Value> {
        body.validate()?;
        let config = self.config_snapshot()?;
        if !config.global.discovery.enabled {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::UnsupportedCapability,
                "camera discovery is disabled by configuration",
            ));
        }
        let query = serde_json::json!({ "backends": body.backends });
        if body.cursor.is_some() {
            let (candidates, next_cursor, completed_at) = self.cursors.snapshot_page(
                "discover",
                &query,
                body.cursor.as_deref(),
                None,
                None,
                usize::from(body.limit),
            )?;
            return Ok(serde_json::json!({
                "candidates": candidates,
                "nextCursor": next_cursor,
                // A continuation is a view of the original retained result, not a second probe.
                "completedAt": completed_at,
            }));
        }
        let wanted = if body.backends.is_empty() {
            None
        } else {
            Some(body.backends.clone())
        };
        let candidates = self
            .discover_candidates(
                &config,
                wanted.as_deref(),
                Duration::from_millis(body.timeout_ms),
                self.cancellation.child_token(),
            )
            .await?
            .into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let completed_at = serde_json::to_value(chrono::Utc::now())?;
        let (candidates, next_cursor, completed_at) = self.cursors.snapshot_page(
            "discover",
            &query,
            None,
            Some(candidates),
            Some(completed_at),
            usize::from(body.limit),
        )?;
        Ok(serde_json::json!({
            "candidates": candidates,
            "nextCursor": next_cursor,
            "completedAt": completed_at,
        }))
    }

    /// Executes one credential-free discovery pass, bounded across all distinct configured
    /// backend kinds.  The page size never affects the underlying snapshot: continuations may
    /// safely page through every retained discovery result up to the configured hard maximum.
    async fn discover_candidates(
        &self,
        config: &AdapterConfig,
        wanted: Option<&[crate::model::BackendKind]>,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Vec<DiscoveryCandidate>> {
        let mut candidates = Vec::new();
        let mut attempted = Vec::new();
        for camera in &config.instances {
            let kind = camera.backend.kind();
            if kind == crate::model::BackendKind::Sim
                || wanted.is_some_and(|wanted| !wanted.contains(&kind))
                || attempted.contains(&kind)
            {
                continue;
            }
            let remaining = config
                .global
                .discovery
                .max_results
                .saturating_sub(candidates.len());
            if remaining == 0 {
                break;
            }
            attempted.push(kind);
            let factory = self
                .backend_context
                .factory_for(&camera.backend, &config.global)?;
            let discovered = factory
                .discover(crate::backend::DiscoveryRequest {
                    eligible_interfaces: config.global.discovery.eligible_interfaces.clone(),
                    timeout,
                    max_results: remaining,
                    cancellation: cancellation.child_token(),
                })
                .await?;
            for candidate in discovered {
                if candidates.len() == config.global.discovery.max_results {
                    break;
                }
                if !candidates.contains(&candidate) {
                    candidates.push(candidate);
                }
            }
        }
        Ok(candidates)
    }

    /// Returns a fresh view of retained discovery observations after excluding cameras already
    /// represented by a stable configured selector.  This is read-only and never opens a session.
    fn unconfigured_discoveries(&self, config: &AdapterConfig) -> Result<Vec<serde_json::Value>> {
        let cache = self.discovery_cache.lock().map_err(|_| {
            crate::CameraError::Catalog("discovery cache is unavailable".to_string())
        })?;
        cache
            .candidates
            .iter()
            .filter(|candidate| !candidate_is_configured(candidate, &config.instances))
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::CameraError::from)
    }

    /// Samples every camera's reachability for the heartbeat's per-instance connectivity surface.
    ///
    /// Q5: camera presence used to be PULL-ONLY. A camera's state lived in `CameraRegistry` and could
    /// be learned only by asking -- `sb/list`, `sb/status` -- so a consumer wanting to know that a
    /// camera had dropped had to poll for it, and nothing was ever published. The assumption that
    /// camera connectivity was already reaching the standard health surface did not hold: nothing was
    /// registered against it.
    ///
    /// EdgeCommons ships exactly the mechanism this needs. The `main` state keepalive carries an
    /// `instances[]` array, fed by a provider, precisely so a multi-instance adapter can report each
    /// connection's health without minting a UNS instance per camera. This is that provider.
    ///
    /// Each optional member of the element carries what it was designed to carry, and the difference
    /// matters to whoever reads it:
    ///
    /// * `connected` -- the normalized flag every consumer can act on without knowing what a camera is.
    /// * `state` -- this component's own richer condition token. `BACKOFF` and `CONNECTING` are both
    ///   `connected: false`, and an operator deciding whether to intervene needs to know which.
    /// * `detail` -- why it is down, in the camera's own words, when it has given us any.
    /// * `attributes` -- the open bag, for what only a camera adapter understands: the backend, the
    ///   connection generation, and the stable code of the error that put it there.
    ///
    /// The same element shape answers core's built-in `status` verb, so one sampler serves both the
    /// push and the pull.
    #[must_use]
    pub fn camera_connectivity(&self) -> Vec<edgecommons::heartbeat::InstanceConnectivity> {
        let Ok(snapshots) = self.registry.snapshots(MAX_CONNECTIVITY_INSTANCES) else {
            return Vec::new();
        };
        snapshots
            .into_iter()
            .map(|snapshot| {
                let connected = snapshot.state == CameraConnectionState::Online;
                let mut attributes = serde_json::Map::new();
                attributes.insert(
                    "backend".to_owned(),
                    serde_json::to_value(snapshot.backend).unwrap_or(serde_json::Value::Null),
                );
                attributes.insert(
                    "generation".to_owned(),
                    serde_json::Value::from(snapshot.generation),
                );
                if let Some(error) = snapshot.last_error.as_ref() {
                    attributes.insert(
                        "lastErrorCode".to_owned(),
                        serde_json::Value::from(error.code.clone()),
                    );
                }
                let state = serde_json::to_value(snapshot.state)
                    .ok()
                    .and_then(|token| token.as_str().map(str::to_owned));
                let detail = snapshot
                    .last_error
                    .as_ref()
                    .filter(|_| !connected)
                    .map(|error| error.message.clone());

                let sample = edgecommons::heartbeat::InstanceConnectivity::new(
                    snapshot.instance,
                    connected,
                    detail,
                )
                .with_attributes(attributes);
                match state {
                    Some(state) => sample.with_state(state),
                    None => sample,
                }
            })
            .collect()
    }

    /// Publishes a camera state transition, and says so when it does not take.
    ///
    /// D5. `CameraRegistry::update` has two failure channels and every supervisor call site
    /// discarded both with `let _ =`:
    ///
    /// * `Err` is a poisoned registry lock. The component's camera state is now unreadable, every
    ///   subsequent transition will be lost, and nothing said a word.
    /// * `Ok(false)` is the generation fence doing its job -- this supervisor has been superseded by
    ///   a newer generation (or its camera is gone), and its update was deliberately dropped. That is
    ///   correct, and it is also exactly what an operator staring at a camera stuck in the wrong
    ///   state needs to be told, because the alternative explanation is a bug.
    ///
    /// Neither changes control flow: a superseded supervisor is already on its way out, and a
    /// poisoned lock is not something a camera actor can do anything about. What changes is that
    /// both are now visible.
    fn publish_camera_state(
        &self,
        instance: &str,
        generation: u64,
        state: CameraConnectionState,
        capabilities: Option<crate::model::CameraCapabilities>,
        last_error: Option<CameraStatusError>,
        observed_at: chrono::DateTime<chrono::Utc>,
    ) {
        match self.registry.update(
            instance,
            generation,
            state,
            capabilities,
            last_error,
            observed_at,
        ) {
            Ok(true) => {}
            Ok(false) => tracing::debug!(
                instance,
                generation,
                ?state,
                "camera state update was dropped: a newer generation owns this camera, or it is no longer configured"
            ),
            Err(error) => tracing::error!(
                instance,
                generation,
                ?state,
                error = %error,
                "camera registry is unavailable; this camera's state can no longer be published"
            ),
        }
    }

    /// Builds one `camera_queue` sample.
    ///
    /// Separated from the timer loop that calls it because a loop that fires every 30 seconds cannot
    /// be asserted on in a unit test, and "the numbers the operator sees are the numbers the
    /// component holds" is exactly the part worth asserting.
    async fn sample_queue_metric(&self) -> Result<std::collections::HashMap<String, f64>> {
        let status = self.queue_status(None).await?;
        let configured = status.cameras.len();
        let online = self
            .registry
            .snapshots(configured.max(1))
            .map(|snapshots| {
                snapshots
                    .into_iter()
                    .filter(|snapshot| snapshot.state == CameraConnectionState::Online)
                    .count()
            })
            .unwrap_or_default();
        let mut values = std::collections::HashMap::new();
        #[allow(clippy::cast_precision_loss)]
        // Counts and byte budgets; f64 is the metric wire type.
        {
            let mut put = |name: &str, value: f64| {
                values.insert(name.to_owned(), value);
            };
            put("dispatchQueued", status.dispatch_queued as f64);
            put("durableBacklog", status.durable_backlog as f64);
            put("durableInFlight", status.durable_in_flight as f64);
            put(
                "availableAcquisitions",
                status.admission.available_acquisitions as f64,
            );
            put(
                "availableEncoders",
                status.admission.available_encoders as f64,
            );
            put(
                "availableWriters",
                status.admission.available_writers as f64,
            );
            put(
                "availableMemoryBytes",
                status.admission.available_memory_bytes as f64,
            );
            put(
                "outstandingDiskBytes",
                status.admission.outstanding_disk_bytes as f64,
            );
            put("camerasOnline", online as f64);
            put("camerasConfigured", configured as f64);
        }
        Ok(values)
    }

    /// Answers `sb/queue-status`.
    ///
    /// Read-only and cheap: the admission and dispatcher numbers are atomics, and the durable
    /// counts are one grouped COUNT rather than a page of rows -- which matters, because the moment
    /// an operator asks this question is exactly the moment the catalog is least able to afford a
    /// scan.
    pub async fn queue_status(&self, instance: Option<String>) -> Result<QueueStatus> {
        if let Some(instance) = instance.as_deref() {
            // Rejects an unknown camera the same way every other targeted verb does.
            self.registry.snapshot(instance)?;
        }
        let config = self.config_snapshot()?;
        // The fleet queue evicts cancelled and expired entries itself -- each carries a watcher task
        // -- so there is no longer a sweep to run before the numbers can be trusted.
        let cameras = config
            .instances
            .iter()
            .filter(|camera| {
                instance
                    .as_deref()
                    .is_none_or(|target| target == camera.id.as_str())
            })
            .map(|camera| CameraQueueDepth {
                instance: camera.id.clone(),
                queued: self.scheduler.pending_for(&camera.id),
                capacity: self.scheduler.capacity_per_camera(),
            })
            .collect::<Vec<_>>();
        let dispatch_queued = self.scheduler.pending();

        let durable = self
            .catalog
            .count_jobs_by_state(instance, NON_TERMINAL_JOB_STATES.to_vec())
            .await?;
        let total_for = |states: &[crate::model::JobState]| -> u64 {
            states
                .iter()
                .filter_map(|state| durable.get(crate::catalog::job_state_token(*state)))
                .sum()
        };
        let durable_backlog = total_for(&BACKLOG_JOB_STATES);
        let durable_in_flight = total_for(&NON_TERMINAL_JOB_STATES) - durable_backlog;

        Ok(QueueStatus {
            admission: self.admission.snapshot(),
            limits: QueueLimits {
                max_concurrent_captures: config.global.limits.max_concurrent_captures,
                max_in_flight_bytes: config.global.limits.max_in_flight_bytes,
                max_queued_captures_per_camera: config.global.limits.max_queued_captures_per_camera,
                max_pending_captures: self.scheduler.capacity(),
            },
            cameras,
            dispatch_queued,
            durable,
            durable_backlog,
            durable_in_flight,
        })
    }

    /// Answers `sb/queue-clear` -- the break-glass drain.
    ///
    /// Cancels the durable backlog (and, only if asked, work already in flight) through the same
    /// `cancel_active` path a single `sb/capture-cancel` uses, so a drained capture reaches the same
    /// terminal state, publishes the same terminal message, and releases the same admission capacity
    /// as one cancelled by hand. There is no second cancellation mechanism to keep correct.
    ///
    /// It pages, because the whole point is that it is reached for when the backlog has run away,
    /// and a drain that tried to hold a runaway backlog in memory would fail exactly when it was
    /// needed. It reports what it could not cancel rather than claiming a clean sweep.
    async fn clear_queue(
        &self,
        instance: Option<String>,
        include_in_flight: bool,
        reason: String,
    ) -> Result<QueueClearOutcome> {
        if let Some(instance) = instance.as_deref() {
            self.registry.snapshot(instance)?;
        }
        let states = if include_in_flight {
            NON_TERMINAL_JOB_STATES.to_vec()
        } else {
            BACKLOG_JOB_STATES.to_vec()
        };
        let mut outcome = QueueClearOutcome {
            cancelled: 0,
            already_terminal: 0,
            failed: Vec::new(),
        };
        // Cancelling moves a row out of the queried states, so each page is drawn fresh from the
        // head rather than walked with a cursor: the set shrinks under us by design.
        loop {
            let page = self
                .catalog
                .jobs_page(instance.clone(), states.clone(), None, 1_000)
                .await?;
            if page.is_empty() {
                return Ok(outcome);
            }
            let drained = page.len();
            for job in page {
                match self.engine(&job.instance) {
                    Ok(engine) => match engine.cancel_active(&job.capture_id, reason.clone()).await
                    {
                        Ok(result) if result.cancelled => outcome.cancelled += 1,
                        Ok(_) => outcome.already_terminal += 1,
                        Err(error) => outcome.failed.push(QueueClearFailure {
                            capture_id: job.capture_id,
                            error: error.to_string(),
                        }),
                    },
                    Err(error) => outcome.failed.push(QueueClearFailure {
                        capture_id: job.capture_id,
                        error: error.to_string(),
                    }),
                }
            }
            // Every row in the page resisted the drain. Another pass would fetch the same rows and
            // fail on them again, forever, so stop and say so.
            if outcome.failed.len() >= drained && outcome.cancelled == 0 {
                return Ok(outcome);
            }
        }
    }

    /// Answers `sb/queue-status` for the command layer.
    async fn queue_status_command(
        &self,
        body: commands::QueueStatusRequest,
    ) -> Result<serde_json::Value> {
        body.validate()?;
        Ok(serde_json::to_value(
            self.queue_status(body.instance).await?,
        )?)
    }

    /// Answers `sb/queue-clear` for the command layer.
    async fn queue_clear_command(
        &self,
        body: commands::QueueClearRequest,
    ) -> Result<serde_json::Value> {
        body.validate()?;
        let commands::QueueClearRequest {
            request_id,
            instance,
            all_cameras: _,
            include_in_flight,
            reason,
        } = body;
        let canonical_reason = reason.clone();
        let reason = reason.unwrap_or_else(|| "operator queue drain".to_string());
        let canonical = serde_json::json!({
            "requestId": &request_id,
            "instance": &instance,
            "includeInFlight": include_in_flight,
            "reason": canonical_reason,
        });
        // Ledgered like every other mutating verb: a retried drain returns the original outcome
        // instead of cancelling a second wave of work the operator never saw.
        let key = crate::catalog::LedgerKey::new(
            instance.clone().unwrap_or_else(|| "main".to_string()),
            "sb/queue-clear",
            request_id,
        )?;
        self.cancel_with_ledger(
            key,
            canonical,
            serde_json::json!({
                "cancelled": 0,
                "alreadyTerminal": 0,
                "failed": [],
            }),
            async {
                let outcome = self
                    .clear_queue(instance, include_in_flight, reason)
                    .await?;
                Ok(serde_json::to_value(outcome)?)
            },
        )
        .await
    }

    async fn cancel_capture(&self, body: CancelRequest) -> Result<serde_json::Value> {
        body.validate()?;
        let CancelRequest {
            request_id,
            capture_id,
            capture_group_id,
            reason,
        } = body;
        let canonical_reason = reason.clone();
        let reason = reason.unwrap_or_else(|| "operator cancellation".to_string());
        if let Some(capture_id) = capture_id {
            let job = self.catalog.job(&capture_id).await?.ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::CaptureNotFound,
                    "capture was not found",
                )
            })?;
            let canonical = serde_json::json!({
                "requestId": &request_id,
                "target": { "kind": "capture", "captureId": &capture_id },
                "reason": canonical_reason,
            });
            let key = crate::catalog::LedgerKey::new(
                job.instance.clone(),
                "sb/capture-cancel",
                request_id,
            )?;
            return self
                .cancel_with_ledger(
                    key,
                    canonical,
                    serde_json::json!({
                        "captureId": capture_id,
                        "cancelled": false,
                        "state": job.state,
                        "cancellationInProgress": false,
                    }),
                    async {
                        let outcome = self
                            .engine(&job.instance)?
                            .cancel_active(&capture_id, reason)
                            .await?;
                        Ok(serde_json::json!({
                            "captureId": capture_id,
                            "cancelled": outcome.cancelled,
                            "state": outcome.state,
                            "cancellationInProgress": outcome.cancellation_in_progress,
                        }))
                    },
                )
                .await;
        }

        let capture_group_id = capture_group_id.ok_or_else(|| {
            crate::CameraError::rejected(
                crate::ErrorCode::InvalidRequest,
                "captureGroupId is required",
            )
        })?;
        let group = self
            .catalog
            .group(&capture_group_id)
            .await?
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::CaptureNotFound,
                    "capture group was not found",
                )
            })?;
        let canonical = serde_json::json!({
            "requestId": &request_id,
            "target": { "kind": "capture-group", "captureGroupId": &capture_group_id },
            "reason": canonical_reason,
        });
        let key = crate::catalog::LedgerKey::new("main", "sb/capture-cancel", request_id)?;
        self.cancel_with_ledger(
            key,
            canonical,
            serde_json::json!({
                "captureGroupId": capture_group_id,
                "cancelledMembers": 0,
                "unchangedMembers": group.members.len(),
                "members": group.members.iter().map(|member| serde_json::json!({
                    "captureId": member.capture_id,
                    "instance": member.instance,
                    "cancelled": false,
                    "state": member.state,
                    "cancellationInProgress": false,
                })).collect::<Vec<_>>(),
            }),
            async {
                let mut cancelled_members = 0_u64;
                let mut unchanged_members = 0_u64;
                let mut members = Vec::with_capacity(group.members.len());
                for member in group.members {
                    let outcome = if member.state.is_terminal() {
                        unchanged_members = unchanged_members.saturating_add(1);
                        crate::jobs::CancelResult {
                            cancelled: false,
                            state: member.state,
                            cancellation_in_progress: false,
                        }
                    } else {
                        let outcome = self
                            .engine(&member.instance)?
                            .cancel_active(&member.capture_id, reason.clone())
                            .await?;
                        if outcome.cancelled {
                            cancelled_members = cancelled_members.saturating_add(1);
                        } else {
                            unchanged_members = unchanged_members.saturating_add(1);
                        }
                        outcome
                    };
                    members.push(serde_json::json!({
                        "captureId": member.capture_id,
                        "instance": member.instance,
                        "cancelled": outcome.cancelled,
                        "state": outcome.state,
                        "cancellationInProgress": outcome.cancellation_in_progress,
                    }));
                }
                Ok(serde_json::json!({
                    "captureGroupId": capture_group_id,
                    "cancelledMembers": cancelled_members,
                    "unchangedMembers": unchanged_members,
                    "members": members,
                }))
            },
        )
        .await
    }

    async fn cancel_with_ledger<F>(
        &self,
        key: crate::catalog::LedgerKey,
        canonical: serde_json::Value,
        in_progress: serde_json::Value,
        operation: F,
    ) -> Result<serde_json::Value>
    where
        F: std::future::Future<Output = Result<serde_json::Value>>,
    {
        match self
            .catalog
            .begin_command(
                key.clone(),
                crate::idempotency::canonical_request_hash(&canonical, false)?,
                canonical,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?
        {
            crate::catalog::BeginCommandOutcome::Conflict => Err(crate::CameraError::rejected(
                crate::ErrorCode::IdempotencyConflict,
                "requestId was already used with different cancellation arguments",
            )),
            crate::catalog::BeginCommandOutcome::Existing(record) => match record.state {
                crate::catalog::LedgerState::OutcomeUnknown => Err(crate::CameraError::rejected(
                    crate::ErrorCode::PreviousOutcomeUnknown,
                    "the prior cancellation outcome is unknown after restart",
                )),
                _ => Ok(record.reply.unwrap_or(in_progress)),
            },
            crate::catalog::BeginCommandOutcome::Started(_) => {
                self.catalog
                    .record_command_acceptance(
                        key.clone(),
                        in_progress,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?;
                match operation.await {
                    Ok(response) => {
                        self.catalog
                            .complete_command(
                                key,
                                crate::catalog::LedgerState::Succeeded,
                                response.clone(),
                                None,
                                None,
                                chrono::Utc::now().timestamp_millis(),
                            )
                            .await?;
                        Ok(response)
                    }
                    Err(error) => {
                        let reply = serde_json::json!({
                            "errorCode": error.code().as_str(),
                            "errorMessage": command_error(&error).message,
                        });
                        let _ = self
                            .catalog
                            .complete_command(
                                key,
                                crate::catalog::LedgerState::Failed,
                                reply,
                                Some(error.code().as_str().to_string()),
                                Some(command_error(&error).message),
                                chrono::Utc::now().timestamp_millis(),
                            )
                            .await;
                        Err(error)
                    }
                }
            }
        }
    }

    async fn reconnect(&self, body: ReconnectRequest) -> Result<serde_json::Value> {
        body.validate()?;
        let instance = self
            .registry
            .resolve_actuation_instance(body.instance.as_deref())?;
        let canonical = serde_json::json!({ "instance": instance, "requestId": body.request_id, "reason": body.reason });
        let key = crate::catalog::LedgerKey::new(
            instance.clone(),
            crate::catalog::RECONNECT_VERB,
            body.request_id,
        )?;
        match self
            .catalog
            .begin_command(
                key.clone(),
                crate::idempotency::canonical_request_hash(&canonical, false)?,
                canonical,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?
        {
            crate::catalog::BeginCommandOutcome::Conflict => Err(crate::CameraError::rejected(
                crate::ErrorCode::IdempotencyConflict,
                "requestId was already used with different reconnect arguments",
            )),
            crate::catalog::BeginCommandOutcome::Existing(record) => match record.state {
                crate::catalog::LedgerState::OutcomeUnknown => Err(crate::CameraError::rejected(
                    crate::ErrorCode::PreviousOutcomeUnknown,
                    "the prior reconnect outcome is unknown after restart",
                )),
                _ => Ok(record.reply.unwrap_or_else(|| {
                    serde_json::json!({
                        "operationId": format!("op_{}", record.key.request_id),
                        "instance": instance,
                        "state": "ACCEPTED",
                    })
                })),
            },
            crate::catalog::BeginCommandOutcome::Started(_) => {
                let operation = serde_json::json!({
                    "operationId": format!("op_{}", uuid::Uuid::now_v7()),
                    "instance": instance,
                    "state": "ACCEPTED",
                });
                self.catalog
                    .record_command_acceptance(
                        key.clone(),
                        operation.clone(),
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?;
                if let Ok(sessions) = self.session_cancellations.read() {
                    if let Some(cancellation) = sessions.get(&instance) {
                        cancellation.cancel();
                    }
                }
                // Signalling the session cancellation completes this operation: reconnect is a
                // bounded, idempotent request to re-establish a session and performs no physical
                // actuation that could half-happen, so nothing hazardous is left in flight. The
                // ledger is therefore settled here rather than left IN_PROGRESS forever — such a
                // row is fenced to OUTCOME_UNKNOWN on the next start, which no retention DELETE
                // can ever match, and which would make every retry answer
                // PREVIOUS_OUTCOME_UNKNOWN for the life of the state database.
                self.catalog
                    .complete_command(
                        key,
                        crate::catalog::LedgerState::Succeeded,
                        operation.clone(),
                        None,
                        None,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?;
                Ok(operation)
            }
        }
    }

    async fn perform_ptz(&self, request: PtzCommandRequest) -> Result<serde_json::Value> {
        let config = self.config_snapshot()?;
        request.validate(60_000)?;
        let (instance, request_id, operation, physical, arguments) = match request {
            PtzCommandRequest::Continuous {
                instance,
                request_id,
                velocity,
                timeout_ms,
            } => (
                self.registry
                    .resolve_actuation_instance(instance.as_deref())?,
                Some(request_id),
                "continuous",
                Some(crate::model::PtzRequest::Continuous {
                    velocity,
                    timeout: Duration::from_millis(timeout_ms),
                }),
                serde_json::json!({ "velocity": velocity, "timeoutMs": timeout_ms }),
            ),
            PtzCommandRequest::Absolute {
                instance,
                request_id,
                position,
                speed,
            } => {
                let physical_speed = speed.map(|speed| crate::model::PtzVector {
                    pan: speed.pan,
                    tilt: speed.tilt,
                    zoom: speed.zoom,
                });
                (
                    self.registry
                        .resolve_actuation_instance(instance.as_deref())?,
                    Some(request_id),
                    "absolute",
                    Some(crate::model::PtzRequest::Absolute {
                        position,
                        speed: physical_speed,
                    }),
                    serde_json::json!({
                        "position": position,
                        "speed": speed.map(|speed| serde_json::json!({
                            "pan": speed.pan,
                            "tilt": speed.tilt,
                            "zoom": speed.zoom,
                        })),
                    }),
                )
            }
            PtzCommandRequest::Relative {
                instance,
                request_id,
                translation,
                speed,
            } => {
                let physical_speed = speed.map(|speed| crate::model::PtzVector {
                    pan: speed.pan,
                    tilt: speed.tilt,
                    zoom: speed.zoom,
                });
                (
                    self.registry
                        .resolve_actuation_instance(instance.as_deref())?,
                    Some(request_id),
                    "relative",
                    Some(crate::model::PtzRequest::Relative {
                        translation,
                        speed: physical_speed,
                    }),
                    serde_json::json!({
                        "translation": translation,
                        "speed": speed.map(|speed| serde_json::json!({
                            "pan": speed.pan,
                            "tilt": speed.tilt,
                            "zoom": speed.zoom,
                        })),
                    }),
                )
            }
            PtzCommandRequest::Stop {
                instance,
                request_id,
                axes,
            } => {
                let pan = axes.contains(&crate::commands::PtzAxis::Pan);
                let tilt = axes.contains(&crate::commands::PtzAxis::Tilt);
                let zoom = axes.contains(&crate::commands::PtzAxis::Zoom);
                (
                    self.registry
                        .resolve_actuation_instance(instance.as_deref())?,
                    Some(request_id),
                    "stop",
                    Some(crate::model::PtzRequest::Stop { pan, tilt, zoom }),
                    serde_json::json!({ "pan": pan, "tilt": tilt, "zoom": zoom }),
                )
            }
            PtzCommandRequest::Home {
                instance,
                request_id,
            } => (
                self.registry
                    .resolve_actuation_instance(instance.as_deref())?,
                Some(request_id),
                "home",
                Some(crate::model::PtzRequest::Home),
                serde_json::json!({}),
            ),
            PtzCommandRequest::Status { instance } => (
                self.registry
                    .resolve_actuation_instance(instance.as_deref())?,
                None,
                "status",
                Some(crate::model::PtzRequest::Status),
                serde_json::Value::Null,
            ),
        };
        let camera = self.registry.camera_config(&instance)?;
        if !camera.ptz.enabled {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::PtzDisabled,
                "PTZ is disabled by configuration",
            ));
        }
        let actor = self.actor(&instance)?;
        let physical = physical.ok_or_else(|| {
            crate::CameraError::rejected(
                crate::ErrorCode::UnsupportedCapability,
                "PTZ operation has no backend request",
            )
        })?;
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(config.global.timeouts.ptz_ms);
        if let Some(request_id) = request_id {
            let canonical = serde_json::json!({
                "instance": &instance,
                "requestId": &request_id,
                "operation": operation,
                "arguments": arguments,
            });
            let key = crate::catalog::LedgerKey::new(
                instance.clone(),
                format!("sb/ptz/{operation}"),
                request_id,
            )?;
            match self
                .catalog
                .begin_command(
                    key.clone(),
                    crate::idempotency::canonical_request_hash(&canonical, false)?,
                    canonical,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?
            {
                crate::catalog::BeginCommandOutcome::Conflict => {
                    return Err(crate::CameraError::rejected(
                        crate::ErrorCode::IdempotencyConflict,
                        "requestId was already used with different PTZ arguments",
                    ));
                }
                crate::catalog::BeginCommandOutcome::Existing(record) => {
                    match record.state {
                        crate::catalog::LedgerState::OutcomeUnknown => {
                            return Err(crate::CameraError::rejected(
                                crate::ErrorCode::PreviousOutcomeUnknown,
                                "the prior PTZ outcome is unknown after restart",
                            ));
                        }
                        _ => return Ok(record.reply.unwrap_or_else(
                            || serde_json::json!({ "operation": operation, "state": "COMMANDED" }),
                        )),
                    }
                }
                crate::catalog::BeginCommandOutcome::Started(_) => {}
            }
            let result = actor.ptz(physical, deadline, &self.cancellation).await;
            let response = match result {
                Ok(crate::model::PtzResult::Commanded) => {
                    serde_json::json!({ "operation": operation, "state": "COMMANDED", "acceptedAt": chrono::Utc::now(), "stopDeadline": if operation == "continuous" { serde_json::json!(chrono::Utc::now() + chrono::Duration::milliseconds(i64::try_from(camera.ptz.maximum_continuous_move_ms).unwrap_or(i64::MAX))) } else { serde_json::Value::Null } })
                }
                Ok(crate::model::PtzResult::PresetToken(token)) => {
                    serde_json::json!({ "operation": operation, "token": token })
                }
                Ok(crate::model::PtzResult::Removed) => {
                    serde_json::json!({ "operation": operation, "removed": true })
                }
                Ok(_) => {
                    return Err(crate::CameraError::rejected(
                        crate::ErrorCode::UnsupportedCapability,
                        "camera returned an unexpected PTZ response",
                    ));
                }
                Err(error) => {
                    let _ = self
                        .catalog
                        .complete_command(
                            key,
                            crate::catalog::LedgerState::Failed,
                            serde_json::json!({ "operation": operation }),
                            Some(error.code().as_str().to_string()),
                            Some(command_error(&error).message),
                            chrono::Utc::now().timestamp_millis(),
                        )
                        .await;
                    return Err(error);
                }
            };
            self.catalog
                .complete_command(
                    key,
                    crate::catalog::LedgerState::Succeeded,
                    response.clone(),
                    None,
                    None,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
            Ok(response)
        } else {
            match actor.ptz(physical, deadline, &self.cancellation).await? {
                crate::model::PtzResult::Status(status) => Ok(
                    serde_json::json!({ "position": status.position, "moving": status.moving, "available": true, "observedAt": status.observed_at }),
                ),
                _ => Err(crate::CameraError::rejected(
                    crate::ErrorCode::UnsupportedCapability,
                    "camera returned an unexpected PTZ status response",
                )),
            }
        }
    }

    async fn perform_presets(&self, request: PtzPresetsRequest) -> Result<serde_json::Value> {
        let config = self.config_snapshot()?;
        request.validate()?;
        match request {
            PtzPresetsRequest::List {
                instance,
                limit,
                cursor,
            } => {
                let instance = self
                    .registry
                    .resolve_actuation_instance(instance.as_deref())?;
                let camera = self.registry.camera_config(&instance)?;
                if !camera.ptz.enabled {
                    return Err(crate::CameraError::rejected(
                        crate::ErrorCode::PtzDisabled,
                        "PTZ is disabled by configuration",
                    ));
                }
                let query = serde_json::json!({ "instance": instance });
                let initial = if cursor.is_none() {
                    let deadline = tokio::time::Instant::now()
                        + Duration::from_millis(config.global.timeouts.ptz_ms);
                    match self
                        .actor(&instance)?
                        .ptz(
                            crate::model::PtzRequest::ListPresets,
                            deadline,
                            &self.cancellation,
                        )
                        .await?
                    {
                        crate::model::PtzResult::Presets(presets) => Some(
                            presets
                                .into_iter()
                                .map(serde_json::to_value)
                                .collect::<std::result::Result<Vec<_>, _>>()?,
                        ),
                        _ => {
                            return Err(crate::CameraError::rejected(
                                crate::ErrorCode::UnsupportedCapability,
                                "camera returned an unexpected preset-list response",
                            ));
                        }
                    }
                } else {
                    None
                };
                let (presets, next_cursor, _) = self.cursors.snapshot_page(
                    "ptz-presets",
                    &query,
                    cursor.as_deref(),
                    initial,
                    None,
                    usize::from(limit),
                )?;
                Ok(serde_json::json!({
                    "presets": presets,
                    "nextCursor": next_cursor,
                }))
            }
            PtzPresetsRequest::Goto {
                instance,
                request_id,
                token,
            } => {
                self.perform_preset_mutation(
                    instance,
                    request_id,
                    "goto",
                    crate::model::PtzRequest::GotoPreset(token.clone()),
                    serde_json::json!({ "token": token }),
                    false,
                )
                .await
            }
            PtzPresetsRequest::Set {
                instance,
                request_id,
                name,
            } => {
                self.perform_preset_mutation(
                    instance,
                    request_id,
                    "set",
                    crate::model::PtzRequest::SetPreset(name.clone()),
                    serde_json::json!({ "name": name }),
                    true,
                )
                .await
            }
            PtzPresetsRequest::Remove {
                instance,
                request_id,
                token,
            } => {
                self.perform_preset_mutation(
                    instance,
                    request_id,
                    "remove",
                    crate::model::PtzRequest::RemovePreset(token.clone()),
                    serde_json::json!({ "token": token }),
                    true,
                )
                .await
            }
        }
    }

    async fn perform_preset_mutation(
        &self,
        requested_instance: Option<String>,
        request_id: String,
        operation: &'static str,
        physical: crate::model::PtzRequest,
        arguments: serde_json::Value,
        requires_mutation_permission: bool,
    ) -> Result<serde_json::Value> {
        let config = self.config_snapshot()?;
        let instance = self
            .registry
            .resolve_actuation_instance(requested_instance.as_deref())?;
        let camera = self.registry.camera_config(&instance)?;
        if !camera.ptz.enabled {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::PtzDisabled,
                "PTZ is disabled by configuration",
            ));
        }
        if requires_mutation_permission && !camera.ptz.allow_preset_mutation {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::UnsupportedCapability,
                "preset mutation is disabled by configuration",
            ));
        }
        let canonical = serde_json::json!({
            "instance": &instance,
            "requestId": &request_id,
            "operation": operation,
            "arguments": arguments,
        });
        let key = crate::catalog::LedgerKey::new(
            instance.clone(),
            format!("sb/ptz-presets/{operation}"),
            request_id,
        )?;
        match self
            .catalog
            .begin_command(
                key.clone(),
                crate::idempotency::canonical_request_hash(&canonical, false)?,
                canonical,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?
        {
            crate::catalog::BeginCommandOutcome::Conflict => {
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::IdempotencyConflict,
                    "requestId was already used with different preset arguments",
                ));
            }
            crate::catalog::BeginCommandOutcome::Existing(record) => {
                return match record.state {
                    crate::catalog::LedgerState::OutcomeUnknown => {
                        Err(crate::CameraError::rejected(
                            crate::ErrorCode::PreviousOutcomeUnknown,
                            "the prior preset outcome is unknown after restart",
                        ))
                    }
                    _ => Ok(record.reply.unwrap_or_else(
                        || serde_json::json!({ "operation": operation, "state": "COMMANDED" }),
                    )),
                };
            }
            crate::catalog::BeginCommandOutcome::Started(_) => {}
        }
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(config.global.timeouts.ptz_ms);
        let response = match self
            .actor(&instance)?
            .ptz(physical, deadline, &self.cancellation)
            .await
        {
            Ok(crate::model::PtzResult::Commanded) => {
                serde_json::json!({ "operation": operation, "state": "COMMANDED" })
            }
            Ok(crate::model::PtzResult::PresetToken(token)) => {
                serde_json::json!({ "operation": operation, "token": token })
            }
            Ok(crate::model::PtzResult::Removed) => {
                serde_json::json!({ "operation": operation, "removed": true })
            }
            Ok(_) => {
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::UnsupportedCapability,
                    "camera returned an unexpected preset response",
                ));
            }
            Err(error) => {
                let _ = self
                    .catalog
                    .complete_command(
                        key,
                        crate::catalog::LedgerState::Failed,
                        serde_json::json!({ "operation": operation }),
                        Some(error.code().as_str().to_string()),
                        Some(command_error(&error).message),
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await;
                return Err(error);
            }
        };
        self.catalog
            .complete_command(
                key,
                crate::catalog::LedgerState::Succeeded,
                response.clone(),
                None,
                None,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?;
        Ok(response)
    }

    fn group_status_page(
        &self,
        group: crate::catalog::GroupRecord,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<serde_json::Value> {
        let query = serde_json::json!({ "captureGroupId": group.group_id });
        let initial = if cursor.is_none() {
            Some(
                group
                    .members
                    .iter()
                    .map(job_status_json)
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        let (members, next_cursor, _) = self.cursors.snapshot_page(
            "capture-status-group",
            &query,
            cursor,
            initial,
            None,
            limit,
        )?;
        Ok(serde_json::json!({
            "group": {
                "captureGroupId": group.group_id,
                "requestId": group.request_id,
                "state": group.state,
                "acceptedAtMs": group.accepted_at_ms,
                "terminalAtMs": group.terminal_at_ms,
                "errorCode": group.error_code,
                "errorMessage": group.error_message,
                "result": group.terminal_result,
            },
            "members": members,
            "nextCursor": next_cursor,
        }))
    }

    async fn jobs_status_page(&self, body: &CaptureStatusRequest) -> Result<serde_json::Value> {
        let query = serde_json::json!({
            "instance": body.instance,
            "states": body.states,
        });
        let before = self.cursors.job_before(&query, body.cursor.as_deref())?;
        let requested = usize::from(body.limit);
        // Read one additional durable row to decide whether a stable continuation exists.  The
        // catalog's descending (acceptedAt,captureId) tuple keeps rows inserted after page one
        // out of every continuation without retaining an unbounded process-local job snapshot.
        let mut jobs = self
            .catalog
            .jobs_page(
                body.instance.clone(),
                body.states.clone(),
                before,
                requested.saturating_add(1),
            )
            .await?;
        let has_next = jobs.len() > requested;
        if has_next {
            jobs.truncate(requested);
        }
        let next_cursor = if has_next {
            let last = jobs.last().ok_or_else(|| {
                crate::CameraError::Catalog(
                    "paged capture-status query reported a continuation without a row".to_string(),
                )
            })?;
            Some(
                self.cursors
                    .next_job_cursor(&query, (last.accepted_at_ms, last.capture_id.clone()))?,
            )
        } else {
            None
        };
        Ok(serde_json::json!({
            "jobs": jobs.iter().map(job_status_json).collect::<Vec<_>>(),
            "nextCursor": next_cursor,
        }))
    }

    /// Applies one already pre-commit-validated configuration generation without exposing a
    /// mixed roster.  All fallible preparation happens before the registry/config swap.  Existing
    /// compatible dispatchers retain their durable queued work; removal, disablement, or backend
    /// replacement terminalizes queued work with the exact reload-interruption envelope.
    pub async fn apply_reloaded_config(
        self: &Arc<Self>,
        replacement: AdapterConfig,
        apps: BTreeMap<String, Arc<AppFacade>>,
        events: BTreeMap<String, EventsFacade>,
    ) -> Result<crate::registry::RegistryDiff> {
        if self.reloading.swap(true, Ordering::AcqRel) {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::CameraUnavailable,
                "a configuration replacement is already draining camera work",
            ));
        }
        let _reloading = ReloadInProgressGuard(&self.reloading);
        self.apply_reloaded_config_inner(replacement, apps, events)
            .await
    }

    /// Performs the candidate-only half of a reload without altering any live camera generation.
    ///
    /// Core invokes this from its pre-commit application coordinator.  This method deliberately
    /// excludes supervisor cancellation, catalog changes, registry replacement, schedule changes,
    /// and readiness publication: a rejected candidate must leave the complete prior service able
    /// to keep accepting and completing captures (R-04).  The corresponding live transition is
    /// performed only from the post-commit configuration listener.
    fn preflight_reloaded_config(
        &self,
        replacement: &AdapterConfig,
        apps: &BTreeMap<String, Arc<AppFacade>>,
        events: &BTreeMap<String, EventsFacade>,
    ) -> Result<()> {
        self.backend_context.validate_config(replacement)?;
        let previous = self.config_snapshot()?;
        if previous.global.state.directory != replacement.global.state.directory
            || previous.global.output.root_directory != replacement.global.output.root_directory
            || previous.global.output.directory_mode != replacement.global.output.directory_mode
            || previous.global.output.file_mode != replacement.global.output.file_mode
        {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::InvalidRequest,
                "state/output root security settings require component restart",
            ));
        }

        // Constructibility of every new runtime dependency is part of candidate validation.  Do
        // not retain these temporary values: the committed transition constructs its own objects
        // after Core has atomically advanced the configuration snapshot.
        let existing_engine_ids = self
            .engines
            .read()
            .map_err(|_| {
                crate::CameraError::Catalog("camera engine map is unavailable".to_string())
            })?
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        for camera in &replacement.instances {
            if existing_engine_ids.contains(&camera.id) {
                continue;
            }
            if !apps.contains_key(&camera.id) {
                return Err(crate::CameraError::Catalog(format!(
                    "missing application facade for reloaded camera '{}'",
                    camera.id
                )));
            }
            if !events.contains_key(&camera.id) {
                return Err(crate::CameraError::Catalog(format!(
                    "missing events facade for reloaded camera '{}'",
                    camera.id
                )));
            }
        }
        Ok(())
    }

    /// Captures only in-memory generation state before a prepared transaction begins its live
    /// transition. All locks are released before any await in the commit/rollback path.
    fn reload_checkpoint(&self) -> Result<RuntimeReloadCheckpoint> {
        let config = self.config_snapshot()?;
        let engines = self
            .engines
            .read()
            .map_err(|_| {
                crate::CameraError::Catalog("camera engine map is unavailable".to_string())
            })?
            .clone();
        let events = self
            .events
            .read()
            .map_err(|_| {
                crate::CameraError::Catalog("camera events map is unavailable".to_string())
            })?
            .clone();
        Ok(RuntimeReloadCheckpoint {
            config,
            engines,
            events,
        })
    }

    /// Restores a checkpoint after a prepared candidate fails before Core publishes it.
    ///
    /// Every currently-live supervisor is first retired and confirmed stopped. Reinstalling the
    /// prior maps/configuration before starting fresh prior-generation supervisors avoids a stale
    /// actor controlling a camera concurrently with a rollback actor. The method is idempotent:
    /// Core may call it after a commit error even when the transition did not reach a destructive
    /// stage.
    async fn restore_reload_checkpoint(
        self: &Arc<Self>,
        checkpoint: RuntimeReloadCheckpoint,
    ) -> Result<()> {
        if self.reloading.swap(true, Ordering::AcqRel) {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::CameraUnavailable,
                "a configuration replacement is still active while restoring the prior generation",
            ));
        }
        let _reloading = ReloadInProgressGuard(&self.reloading);
        let _reload = self.reload_gate.lock().await;
        let current = self
            .config_snapshot()
            .unwrap_or_else(|_| checkpoint.config.clone());
        let mut instances = current
            .instances
            .iter()
            .map(|camera| camera.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        instances.extend(
            checkpoint
                .config
                .instances
                .iter()
                .map(|camera| camera.id.clone()),
        );
        let instances = instances.into_iter().collect::<Vec<_>>();
        let timeout =
            Duration::from_millis(checkpoint.config.global.timeouts.reload_drain_timeout_ms);
        self.replace_supervisors(&instances, timeout).await?;

        // All direct map writes happen after supervisor retirement. No lock is held across the
        // preceding await, and a poisoned map is reported so Core retains the prior snapshot.
        self.registry.apply_validated_config(&checkpoint.config)?;
        *self.engines.write().map_err(|_| {
            crate::CameraError::Catalog(
                "camera engine map is unavailable during rollback".to_string(),
            )
        })? = checkpoint.engines;
        *self.events.write().map_err(|_| {
            crate::CameraError::Catalog(
                "camera events map is unavailable during rollback".to_string(),
            )
        })? = checkpoint.events;
        *self.config.write().map_err(|_| {
            crate::CameraError::Catalog(
                "runtime configuration lock is unavailable during rollback".to_string(),
            )
        })? = checkpoint.config.clone();

        self.restart_schedulers()?;
        self.restart_periodic_discovery()?;
        for camera in checkpoint
            .config
            .instances
            .iter()
            .filter(|camera| camera.enabled)
        {
            self.start_supervisor(camera.id.clone(), self.engine(&camera.id)?)?;
        }
        Ok(())
    }

    async fn apply_reloaded_config_inner(
        self: &Arc<Self>,
        replacement: AdapterConfig,
        apps: BTreeMap<String, Arc<AppFacade>>,
        events: BTreeMap<String, EventsFacade>,
    ) -> Result<crate::registry::RegistryDiff> {
        let _reload = self.reload_gate.lock().await;
        self.preflight_reloaded_config(&replacement, &apps, &events)?;
        let previous = self.config_snapshot()?;

        let replacement_by_id = replacement
            .instances
            .iter()
            .map(|camera| (camera.id.as_str(), camera))
            .collect::<BTreeMap<_, _>>();
        // Queued work is compatible only when its backend *kind* remains the same.  A changed
        // endpoint/selector/credential reference still requires a new live session, but the
        // immutable accepted job profile remains executable by the same backend contract.
        let incompatible = previous
            .instances
            .iter()
            .filter_map(|old| match replacement_by_id.get(old.id.as_str()) {
                Some(new)
                    if new.enabled && old.enabled && old.backend.kind() == new.backend.kind() =>
                {
                    None
                }
                _ => Some(old.id.clone()),
            })
            .collect::<Vec<_>>();
        // ONVIF protocol clients retain the global network and HTTP/XML policy that existed when
        // their session was constructed.  A policy reload therefore retires otherwise unchanged
        // ONVIF sessions so the next connection cannot keep probing on an old interface set or
        // applying stale security limits.  Sim and GenICam sessions have no such global policy
        // dependency and remain live when their backend settings are unchanged.
        let onvif_runtime_policy_changed = previous.global.discovery.eligible_interfaces
            != replacement.global.discovery.eligible_interfaces
            || previous.global.security.max_header_bytes
                != replacement.global.security.max_header_bytes
            || previous.global.security.max_decompression_ratio
                != replacement.global.security.max_decompression_ratio
            || previous.global.security.allow_basic_over_plaintext
                != replacement.global.security.allow_basic_over_plaintext;
        let restarting = previous
            .instances
            .iter()
            .filter_map(|old| match replacement_by_id.get(old.id.as_str()) {
                Some(new)
                    if old.enabled
                        && new.enabled
                        && old.backend == new.backend
                        && !(onvif_runtime_policy_changed
                            && old.backend.kind() == crate::model::BackendKind::OnvifRtsp) =>
                {
                    None
                }
                _ => Some(old.id.clone()),
            })
            .collect::<Vec<_>>();

        // Build every new runtime object before changing the published registry.  An absent
        // facade is a real initialization failure, not permission to install a partial roster.
        let existing_engine_ids = self
            .engines
            .read()
            .map_err(|_| {
                crate::CameraError::Catalog("camera engine map is unavailable".to_string())
            })?
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut added_engines = Vec::new();
        let mut added_events = Vec::new();
        for camera in &replacement.instances {
            if existing_engine_ids.contains(&camera.id) {
                continue;
            }
            let app = apps.get(&camera.id).cloned().ok_or_else(|| {
                crate::CameraError::Catalog(format!(
                    "missing application facade for reloaded camera '{}'",
                    camera.id
                ))
            })?;
            let event = events.get(&camera.id).cloned().ok_or_else(|| {
                crate::CameraError::Catalog(format!(
                    "missing events facade for reloaded camera '{}'",
                    camera.id
                ))
            })?;
            added_engines.push((camera.id.clone(), self.new_engine(app)));
            added_events.push((camera.id.clone(), event));
        }
        // Core calls this method from its pre-commit application gate. Retire every old
        // supervisor before touching any published runtime generation: a timeout must veto the
        // candidate while Core and the runtime still expose the same previous configuration.
        // Cancellation itself may leave an affected camera unavailable until the old generation
        // exits and the configuration source retries, but it must never permit two generations to
        // control one camera concurrently.
        let drain_timeout =
            Duration::from_millis(replacement.global.timeouts.reload_drain_timeout_ms);
        self.wait_for_active_jobs(&restarting, drain_timeout)
            .await?;
        self.replace_supervisors(&restarting, drain_timeout).await?;

        // The retirement barrier above has confirmed that no old generation can mutate a camera
        // after this point. All remaining fallible preparation has completed, so the registry and
        // runtime configuration can now advance as one candidate generation.
        let diff = self.registry.apply_validated_config(&replacement)?;
        {
            match self.engines.write() {
                Ok(mut engines) => {
                    for (instance, engine) in added_engines {
                        engines.insert(instance, engine);
                    }
                }
                Err(_) => {
                    tracing::error!("camera engine map became unavailable while committing reload");
                }
            }
        }
        {
            match self.events.write() {
                Ok(mut runtime_events) => {
                    // The listener supplies fresh facades for all retained instances so their core
                    // configuration snapshot stays current; tests and internal callers may omit
                    // retained entries, in which case the established facade remains valid. Newly
                    // added cameras were required above and are therefore never installed without
                    // an event publishing path.
                    for (instance, event) in events {
                        if replacement_by_id.contains_key(instance.as_str()) {
                            runtime_events.insert(instance, event);
                        }
                    }
                    for (instance, event) in added_events {
                        runtime_events.insert(instance, event);
                    }
                }
                Err(_) => {
                    tracing::error!(
                        "camera events facade map became unavailable while committing reload"
                    );
                }
            }
        }
        {
            match self.config.write() {
                Ok(mut config) => *config = replacement.clone(),
                Err(_) => {
                    tracing::error!(
                        "runtime configuration lock became unavailable while committing reload"
                    );
                }
            }
        }

        for instance in &incompatible {
            if let Err(error) = self.interrupt_reload_queued(instance).await {
                tracing::error!(instance, error = %error, "could not terminalize incompatible queued jobs during reload");
            }
        }

        // Schedule plans are immutable.  Canceling the prior generation before constructing the
        // new plans prevents a schedule-only reload from admitting an old cron/profile after the
        // registry generation has changed.
        if let Err(error) = self.restart_schedulers() {
            tracing::error!(error = %error, "could not restart schedules after committed reload");
        }
        if let Err(error) = self.restart_periodic_discovery() {
            tracing::error!(error = %error, "could not restart periodic discovery after committed reload");
        }

        // The pre-commit retirement barrier confirmed every old supervisor exit before the
        // registry/configuration swap. New supervisors therefore cannot overlap a stale camera
        // generation or let a stale cleanup path remove their actor entry.
        for instance in &diff.added {
            if let Ok(camera) = self.registry.camera_config(instance) {
                if camera.enabled {
                    match self.engine(instance) {
                        Ok(engine) => {
                            if let Err(error) = self.start_supervisor(instance.clone(), engine) {
                                tracing::error!(instance, error = %error, "could not start added camera supervisor after committed reload");
                            }
                        }
                        Err(error) => {
                            tracing::error!(instance, error = %error, "added camera has no runtime engine after committed reload");
                        }
                    }
                }
            }
        }
        for instance in restarting.iter().filter(|instance| {
            !diff.removed.contains(instance)
                && self
                    .registry
                    .camera_config(instance)
                    .is_ok_and(|camera| camera.enabled)
        }) {
            match self.engine(instance) {
                Ok(engine) => {
                    if let Err(error) = self.start_supervisor(instance.clone(), engine) {
                        tracing::error!(instance, error = %error, "could not restart camera supervisor after committed reload");
                    }
                }
                Err(error) => {
                    tracing::error!(instance, error = %error, "restarted camera has no runtime engine after committed reload");
                }
            }
        }
        if !diff.removed.is_empty() {
            if let Ok(mut engines) = self.engines.write() {
                engines.retain(|instance, _| !diff.removed.contains(instance));
            }
            // A removed camera needs nothing torn down in the queue: it is deregistered, its work
            // is never admissible again, and each entry expires on its own wait deadline. The queue
            // outliving one camera is the same property that lets it outlive a reconnect.
            for instance in &diff.removed {
                self.scheduler.camera_offline(instance);
            }
            if let Ok(mut events) = self.events.write() {
                events.retain(|instance, _| !diff.removed.contains(instance));
            }
            if let Ok(mut cancellations) = self.supervisor_cancellations.write() {
                cancellations.retain(|instance, _| !diff.removed.contains(instance));
            }
            if let Ok(mut finished) = self.supervisor_finished.write() {
                finished.retain(|instance, _| !diff.removed.contains(instance));
            }
            if let Ok(mut sessions) = self.session_cancellations.write() {
                sessions.retain(|instance, _| !diff.removed.contains(instance));
            }
        }
        Ok(diff)
    }

    async fn wait_for_active_jobs(&self, instances: &[String], timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let mut any_active = false;
            for instance in instances {
                if self.has_active_job(instance).await? {
                    any_active = true;
                    break;
                }
            }
            if !any_active || tokio::time::Instant::now() >= deadline {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Cancels complete supervisor generations, not merely live actors.  This covers a reload
    /// arriving while a backend is connecting or sleeping in exponential backoff.
    async fn replace_supervisors(&self, instances: &[String], timeout: Duration) -> Result<()> {
        let cancellations = self
            .supervisor_cancellations
            .read()
            .map_err(|_| {
                crate::CameraError::Catalog(
                    "supervisor cancellation map is unavailable".to_string(),
                )
            })?
            .iter()
            .filter(|(instance, _)| instances.contains(*instance))
            .map(|(_, cancellation)| cancellation.clone())
            .collect::<Vec<_>>();
        for cancellation in cancellations {
            cancellation.cancel();
        }
        // A live actor receives the supervisor child token above.  Retaining the direct signal is
        // useful for an already-dispatched control operation which owns its own child token.
        if let Ok(sessions) = self.session_cancellations.read() {
            for (instance, cancellation) in sessions.iter() {
                if instances.contains(instance) {
                    cancellation.cancel();
                }
            }
        }

        let completed = self
            .supervisor_finished
            .read()
            .map_err(|_| {
                crate::CameraError::Catalog("supervisor completion map is unavailable".to_string())
            })?
            .iter()
            .filter(|(instance, _)| instances.contains(*instance))
            .map(|(_, finished)| finished.clone())
            .collect::<Vec<_>>();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if completed.iter().all(CancellationToken::is_cancelled) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::CameraUnavailable,
                    "camera supervisor did not stop within reloadDrainTimeoutMs",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn has_active_job(&self, instance: &str) -> Result<bool> {
        let states = vec![
            crate::model::JobState::Acquiring,
            crate::model::JobState::Encoding,
            crate::model::JobState::Persisting,
        ];
        Ok(!self
            .catalog
            .jobs_page(Some(instance.to_owned()), states, None, 1)
            .await?
            .is_empty())
    }

    async fn interrupt_reload_queued(&self, instance: &str) -> Result<()> {
        let mut before = None;
        loop {
            let page = self
                .catalog
                .jobs_page(
                    Some(instance.to_owned()),
                    vec![
                        crate::model::JobState::Accepted,
                        crate::model::JobState::Queued,
                    ],
                    before.clone(),
                    1_000,
                )
                .await?;
            let Some(last) = page.last() else {
                return Ok(());
            };
            for record in &page {
                self.engine(instance)?
                    .interrupt_for_reload(record.clone())
                    .await?;
            }
            if page.len() < 1_000 {
                return Ok(());
            }
            before = Some((last.accepted_at_ms, last.capture_id.clone()));
        }
    }

    /// Starts cooperative shutdown.  Pending outbox rows remain durable; tasks are joined only
    /// within the configured grace period so the process cannot hang behind a native backend.
    pub async fn shutdown(&self) {
        self.readiness.begin_shutdown();
        self.cancellation.cancel();
        let grace = match self.config_snapshot() {
            Ok(config) => Duration::from_millis(config.global.timeouts.shutdown_grace_ms),
            Err(_) => Duration::from_secs(30),
        };
        let tasks = self
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks));
        let Ok(tasks) = tasks else {
            return;
        };
        let join = async move {
            for task in tasks {
                let _ = task.await;
            }
        };
        let _ = tokio::time::timeout(grace, join).await;
    }

    async fn refresh_storage_pressure(&self) -> Option<StoragePressureSnapshot> {
        let monitor = self.storage_pressure.clone()?;
        let snapshot = monitor.assess().await;
        self.readiness
            .set_state_storage_available(snapshot.state_available());
        publish_storage_alarm(
            self.outbox_events.clone(),
            self.storage_alarm.as_ref(),
            &snapshot,
        )
        .await;
        Some(snapshot)
    }

    async fn ensure_storage_capacity(&self) -> Result<()> {
        let Some(snapshot) = self.refresh_storage_pressure().await else {
            return Ok(());
        };
        if snapshot.rejects_new_captures() {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::StoragePressure,
                "configured output or state storage cannot safely admit a new capture",
            ));
        }
        Ok(())
    }

    /// Samples what the component is holding into the `camera_queue` metric.
    ///
    /// The counts in `camera_captures` say what has happened; this says what is happening. An
    /// operator needs both: a fleet with a healthy success rate and a backlog that only grows is
    /// failing, and the counters alone cannot show it.
    fn start_metric_sampler(self: &Arc<Self>) -> Result<()> {
        let runtime = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => return,
                    () = tokio::time::sleep(METRIC_SAMPLE_INTERVAL) => {}
                }
                let values = match runtime.sample_queue_metric().await {
                    Ok(values) => values,
                    Err(error) => {
                        tracing::warn!(error = %error, "camera queue metrics could not be sampled");
                        continue;
                    }
                };
                runtime.metrics.sample_queue(values).await;
            }
        })
    }

    /// Runs the fleet capture queue: pull the best admissible capture, and give it to its camera.
    ///
    /// This is the component's only capture consumer, and it is the whole of Q1. It replaces N
    /// per-camera drain loops that each polled at 100 Hz and could see only their own camera's work.
    /// The ordering it applies is fleet-wide -- a `Direct` capture on a connected camera can no
    /// longer wait behind a `Scheduled` one that a busy camera happens to hold.
    ///
    /// It is also, without any further code, the fix for an oversized group. A wave is simply "as
    /// many as capacity allows"; the members beyond that wait here, and each one's clocks start when
    /// a camera actually takes it. "More work than I can do at once" became "this takes longer"
    /// instead of "most of your members failed".
    fn start_capture_scheduler(self: &Arc<Self>) -> Result<()> {
        let runtime = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            loop {
                let Some((queued, slot)) = runtime.scheduler.next_admissible(&cancellation).await
                else {
                    return;
                };
                let instance = queued.camera_id.clone();
                let descriptor = queued.payload.into_descriptor();
                let capture_id = descriptor.capture_id().to_owned();

                // The capture's clocks start NOW, not when it was accepted. Without this the whole
                // queue is a way of making captures die tidily: a member that waited its turn would
                // arrive at a free camera with its entire budget already spent.
                // Everything from here to the hand-off can fail, and a descriptor dropped on any of
                // those paths is a durable row left QUEUED with nothing left alive to drive it --
                // B5, rebuilt. So the rule for this whole block is: a capture only leaves the queue
                // when it is dispatched, or when it is provably no longer owed a run.
                let timeouts = match runtime.config_snapshot() {
                    Ok(config) => config.global.timeouts.clone(),
                    Err(error) => {
                        tracing::error!(error = %error, "capture scheduler cannot read its configuration");
                        runtime.return_to_queue(descriptor);
                        tokio::time::sleep(SCHEDULER_RETRY_BACKOFF).await;
                        continue;
                    }
                };
                let engine = match runtime.engine(&instance) {
                    Ok(engine) => engine,
                    Err(error) => {
                        // The camera is gone -- a reload retired it. There is nothing to put this
                        // back for, and requeueing would spin on it forever.
                        tracing::warn!(
                            instance = %instance,
                            capture = %descriptor.capture_id(),
                            error = %error,
                            "capture scheduler has no engine for this camera; the capture is dropped"
                        );
                        continue;
                    }
                };
                if let Err(error) = engine.rebase_onto_admission(&descriptor, &timeouts).await {
                    if error.is_durable_store_failure() {
                        // The STORE hiccuped -- it did not say this capture is finished. It is still
                        // QUEUED and still owed a run, so it goes back on the queue. Dropping it here
                        // is what stranded a real capture: under load the catalog returns SQLITE_BUSY,
                        // the rebase fails transiently, and treating that as "already retired"
                        // destroys a capture that nothing else will ever pick up. The back-off keeps a
                        // sustained store outage from turning this into a spin.
                        tracing::warn!(
                            instance = %instance,
                            capture = %descriptor.capture_id(),
                            error = %error,
                            "the catalog could not rebase this capture; it stays queued and will be retried"
                        );
                        runtime.return_to_queue(descriptor);
                        tokio::time::sleep(SCHEDULER_RETRY_BACKOFF).await;
                    } else {
                        // The row is no longer QUEUED: already terminal, cancelled, or expired. Its
                        // own machinery has retired it, and there is nothing to put back.
                        tracing::debug!(
                            instance = %instance,
                            capture = %descriptor.capture_id(),
                            error = %error,
                            "capture was retired before it could be dispatched"
                        );
                    }
                    continue;
                }

                // Held from here until the capture is terminal. The slot was taken BEFORE the pop,
                // so the queue never hands a camera work the component has no capacity to run: a
                // capture that would have waited a second time -- inside `execute`, invisibly, on a
                // clock already started for it -- waits in the queue instead, where waiting is free.
                runtime.scheduler.hold_execution_slot(&capture_id, slot);

                if let Err(descriptor) = runtime.scheduler.dispatch(&instance, descriptor) {
                    runtime.scheduler.capture_finished(&capture_id);
                    // The camera went offline between the pop and the hand-off. The capture has been
                    // durably promised, so it goes back in the queue rather than being dropped on the
                    // floor -- it will be admitted when the camera returns, or expire waiting.
                    tracing::debug!(
                        instance = %instance,
                        capture = %descriptor.capture_id(),
                        "camera went offline during dispatch; the capture stays queued"
                    );
                    runtime.return_to_queue(descriptor);
                }
            }
        })
    }

    /// Puts a descriptor back on the fleet queue after a failed hand-off.
    fn requeue(&self, descriptor: crate::jobs::CaptureDescriptor) -> Result<()> {
        let instance = descriptor.instance().to_owned();
        let reservation = crate::jobs::CaptureDispatcher::reserve(&self.scheduler, &instance)?;
        reservation.commit(descriptor)?;
        Ok(())
    }

    /// Returns a capture to the queue, and says so loudly if it cannot.
    ///
    /// A capture that can be neither dispatched nor requeued is a durable row that will sit QUEUED
    /// until its deadline retires it. That is recoverable -- the deadline task does terminalize it --
    /// but it is never routine, and it must not be silent.
    fn return_to_queue(&self, descriptor: crate::jobs::CaptureDescriptor) {
        let instance = descriptor.instance().to_owned();
        let capture = descriptor.capture_id().to_owned();
        if let Err(error) = self.requeue(descriptor) {
            tracing::error!(
                instance = %instance,
                capture = %capture,
                error = %error,
                "a capture could not be returned to the queue and will wait for its deadline"
            );
        }
    }

    fn start_storage_pressure_monitor(self: &Arc<Self>) -> Result<()> {
        let runtime = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                let _ = runtime.refresh_storage_pressure().await;
            }
        })
    }

    /// Starts the periodic retention sweep on the runtime's own task/shutdown machinery.
    fn start_retention(self: &Arc<Self>) -> Result<()> {
        let runtime = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            runtime
                .run_retention(cancellation, RETENTION_SWEEP_INTERVAL, RETENTION_BATCH)
                .await;
        })
    }

    /// Sweeps retained durable state on `interval` until the runtime is cancelled.
    ///
    /// The interval and batch size are parameters rather than constants read inside the loop, so
    /// the loop is directly drivable.  A failed sweep is never fatal: retention is a background
    /// reclaim, and the next interval retries it.
    async fn run_retention(
        self: Arc<Self>,
        cancellation: CancellationToken,
        interval: Duration,
        batch: usize,
    ) {
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
            match self
                .retention_sweep(chrono::Utc::now().timestamp_millis(), batch, &cancellation)
                .await
            {
                Ok(sweep) if sweep.reclaimed() > 0 => tracing::info!(
                    delivered_outbox = sweep.delivered_outbox,
                    terminal_jobs = sweep.terminal_jobs,
                    terminal_groups = sweep.terminal_groups,
                    command_ledgers = sweep.command_ledgers,
                    over_limit_jobs = sweep.over_limit_jobs,
                    "camera retention reclaimed durable state"
                ),
                Ok(_) => {
                    tracing::debug!("camera retention found no durable state past its windows");
                }
                Err(error) => {
                    tracing::warn!(error = %error, "camera retention sweep failed; retrying on the next interval");
                }
            }
        }
    }

    /// Runs one full retention pass and reports what it reclaimed.
    ///
    /// Delivered outbox rows are reclaimed first because a terminal job or group only becomes
    /// eligible once its own retained messages are gone.
    async fn retention_sweep(
        &self,
        now_ms: i64,
        batch: usize,
        cancellation: &CancellationToken,
    ) -> Result<RetentionSweep> {
        let config = self.config_snapshot()?;
        let state = &config.global.state;
        let outbox_before_ms =
            now_ms.saturating_sub(i64::from(state.outbox_retention_hours) * MILLIS_PER_HOUR);
        let terminal_before_ms =
            now_ms.saturating_sub(i64::from(state.result_retention_hours) * MILLIS_PER_HOUR);
        let catalog = &self.catalog;
        Ok(RetentionSweep {
            delivered_outbox: prune_in_batches(cancellation, batch, |limit| {
                catalog.prune_delivered_outbox(outbox_before_ms, limit)
            })
            .await?,
            terminal_jobs: prune_in_batches(cancellation, batch, |limit| {
                catalog.prune_terminal_jobs(terminal_before_ms, limit)
            })
            .await?,
            terminal_groups: prune_in_batches(cancellation, batch, |limit| {
                catalog.prune_terminal_groups(terminal_before_ms, limit)
            })
            .await?,
            command_ledgers: prune_in_batches(cancellation, batch, |limit| {
                catalog.prune_completed_command_ledgers(terminal_before_ms, limit)
            })
            .await?,
            over_limit_jobs: prune_in_batches(cancellation, batch, |limit| {
                catalog.enforce_result_record_limit(state.max_result_records, limit)
            })
            .await?,
        })
    }

    fn start_outbox(
        self: &Arc<Self>,
        messaging: Arc<dyn edgecommons::messaging::MessagingService>,
    ) -> Result<()> {
        let config = self.config_snapshot()?;
        let publisher = OutboxPublisher::new(
            Arc::new(self.catalog.clone()),
            Arc::new(EdgeCommonsConfirmedPublisher::new(messaging)),
            Duration::from_secs(10),
            Duration::from_millis(250),
            config.global.state.max_result_records,
        )?;
        let events = self.outbox_events.clone().ok_or_else(|| {
            crate::CameraError::Catalog(
                "missing component events facade for outbox health".to_string(),
            )
        })?;
        let readiness = self.readiness.clone();
        let watchers = OutboxHealthWatchers {
            pressure: publisher.pressure(),
            durability: publisher.durability(),
            catalog_availability: self.catalog.availability(),
        };
        let catalog = self.catalog.clone();
        let storage_pressure = self.storage_pressure.clone();
        let storage_alarm = Arc::clone(&self.storage_alarm);
        let observer_cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            Self::observe_outbox_health(
                watchers,
                catalog,
                events,
                storage_pressure,
                storage_alarm,
                readiness,
                observer_cancellation,
            )
            .await;
        })?;

        let cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            if let Err(error) = publisher.run(cancellation).await {
                tracing::error!(error = %error, "camera outbox worker stopped unexpectedly");
            }
        })
    }

    async fn observe_outbox_health(
        mut watchers: OutboxHealthWatchers,
        catalog: Catalog,
        events: EventsFacade,
        storage_pressure: Option<StoragePressureMonitor>,
        storage_alarm: Arc<Mutex<StorageAlarmState>>,
        readiness: RuntimeReadiness,
        cancellation: CancellationToken,
    ) {
        let mut alarms = OutboxAlarmState::default();
        let mut catalog_unavailable = !watchers
            .catalog_availability
            .borrow()
            .state_capacity_available;
        let mut recovery_probe = tokio::time::interval(Duration::from_secs(1));
        recovery_probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                changed = watchers.pressure.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let current = watchers.pressure.borrow_and_update().clone();
                    if let Some(transition) = alarms.transition(&current) {
                        let result = match transition {
                            OutboxAlarmTransition::Raise(context) => events.raise_alarm(
                                Severity::Warning,
                                "message-delivery-delayed",
                                Some("durable terminal-message delivery is delayed".to_string()),
                                Some(context),
                            ).await,
                            OutboxAlarmTransition::Clear(context) => events.clear_alarm(
                                Severity::Warning,
                                "message-delivery-delayed",
                                Some(context),
                            ).await,
                        };
                        if let Err(error) = result {
                            tracing::warn!(error = %error, "failed to publish outbox delivery-health alarm");
                        }
                    }
                }
                changed = watchers.durability.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let current = *watchers.durability.borrow_and_update();
                    readiness.set_outbox_available(current.state_capacity_available);
                }
                changed = watchers.catalog_availability.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let current = *watchers.catalog_availability.borrow_and_update();
                    catalog_unavailable = !current.state_capacity_available;
                    readiness.set_catalog_available(current.state_capacity_available);
                    if current.disk_full {
                        if let Some(monitor) = storage_pressure.as_ref() {
                            let snapshot = monitor.assess().await;
                            readiness.set_state_storage_available(snapshot.state_available());
                            publish_storage_alarm(
                                Some(events.clone()),
                                storage_alarm.as_ref(),
                                &snapshot,
                            )
                            .await;
                        }
                    }
                }
                _ = recovery_probe.tick(), if catalog_unavailable => {
                    if catalog.probe_commit().await.is_err() {
                        tracing::warn!("catalog durable-state recovery probe did not commit");
                    }
                }
            }
        }
    }

    fn start_supervisors(self: &Arc<Self>) -> Result<()> {
        for camera in self.config_snapshot()?.instances {
            if !camera.enabled {
                continue;
            }
            let engine = self.engine(&camera.id)?;
            self.start_supervisor(camera.id, engine)?;
        }
        Ok(())
    }

    /// Starts one isolated supervisor generation.  The child cancellation token propagates the
    /// process shutdown token but can also retire this generation during a per-camera reload.
    fn start_supervisor(self: &Arc<Self>, instance: String, engine: JobEngine) -> Result<()> {
        let cancellation = self.cancellation.child_token();
        let finished = CancellationToken::new();
        let previous = self
            .supervisor_cancellations
            .write()
            .map_err(|_| {
                crate::CameraError::Catalog(
                    "supervisor cancellation map is unavailable".to_string(),
                )
            })?
            .insert(instance.clone(), cancellation.clone());
        if let Some(previous) = previous {
            previous.cancel();
        }
        self.supervisor_finished
            .write()
            .map_err(|_| {
                crate::CameraError::Catalog("supervisor completion map is unavailable".to_string())
            })?
            .insert(instance.clone(), finished.clone());
        let runtime = Arc::clone(self);
        self.spawn_task(async move {
            runtime
                .run_supervisor(instance, engine, cancellation, finished)
                .await;
        })
    }

    fn start_schedulers(self: &Arc<Self>) -> Result<()> {
        let config = self.config_snapshot()?;
        for camera in &config.instances {
            if !camera.enabled {
                continue;
            }
            for schedule in camera.schedules.iter().filter(|schedule| schedule.enabled) {
                let plan = SchedulePlan::compile(camera.id.clone(), schedule)?;
                self.start_schedule_plan(plan)?;
            }
        }
        // One task per group schedule -- not one per (camera, schedule) pair. A group fires once,
        // as one thing; N member tasks racing the same cron would be N groups, or one group and
        // N-1 duplicate submissions.
        for schedule in config
            .global
            .capture_group_schedules
            .iter()
            .filter(|schedule| schedule.enabled)
        {
            let plan = SchedulePlan::compile_group(schedule)?;
            self.start_schedule_plan(plan)?;
        }
        Ok(())
    }

    fn start_schedule_plan(self: &Arc<Self>, plan: SchedulePlan) -> Result<()> {
        let key = plan.key_parts();
        let cancellation = CancellationToken::new();
        let previous = self
            .scheduler_cancellations
            .write()
            .map_err(|_| {
                crate::CameraError::Catalog("schedule task map is unavailable".to_string())
            })?
            .insert(key, cancellation.clone());
        if let Some(previous) = previous {
            previous.cancel();
        }
        let runtime = Arc::clone(self);
        self.spawn_task(async move {
            match plan.scope() {
                crate::scheduler::ScheduleScope::Camera(_) => {
                    runtime.run_schedule(plan, cancellation).await;
                }
                crate::scheduler::ScheduleScope::Group(_) => {
                    runtime.run_group_schedule(plan, cancellation).await;
                }
            }
        })
    }

    fn restart_schedulers(self: &Arc<Self>) -> Result<()> {
        let cancellations = self
            .scheduler_cancellations
            .write()
            .map_err(|_| {
                crate::CameraError::Catalog("schedule task map is unavailable".to_string())
            })?
            .drain()
            .map(|(_, cancellation)| cancellation)
            .collect::<Vec<_>>();
        for cancellation in cancellations {
            cancellation.cancel();
        }
        self.start_schedulers()
    }

    fn start_periodic_discovery(self: &Arc<Self>) -> Result<()> {
        let config = self.config_snapshot()?;
        if !config.global.discovery.enabled {
            return Ok(());
        }
        let cancellation = self.cancellation.child_token();
        let previous = self
            .discovery_cancellation
            .write()
            .map_err(|_| {
                crate::CameraError::Catalog("discovery cancellation is unavailable".to_string())
            })?
            .replace(cancellation.clone());
        if let Some(previous) = previous {
            previous.cancel();
        }
        let runtime = Arc::clone(self);
        self.spawn_task(async move {
            runtime.run_periodic_discovery(cancellation).await;
        })
    }

    /// Cancels the previous discovery generation even when reporting is disabled: stale network
    /// observations must not survive a policy disable/re-enable boundary.
    fn restart_periodic_discovery(self: &Arc<Self>) -> Result<()> {
        let previous = self
            .discovery_cancellation
            .write()
            .map_err(|_| {
                crate::CameraError::Catalog("discovery cancellation is unavailable".to_string())
            })?
            .take();
        if let Some(previous) = previous {
            previous.cancel();
        }
        if let Ok(mut cache) = self.discovery_cache.lock() {
            cache.candidates.clear();
        }
        self.start_periodic_discovery()
    }

    async fn run_periodic_discovery(self: Arc<Self>, cancellation: CancellationToken) {
        loop {
            if cancellation.is_cancelled() || self.reloading.load(Ordering::Acquire) {
                if cancellation.is_cancelled() {
                    return;
                }
            } else {
                let config = match self.config_snapshot() {
                    Ok(config) => config,
                    Err(error) => {
                        tracing::error!(error = %error, "periodic discovery lost runtime configuration");
                        return;
                    }
                };
                if !config.global.discovery.enabled {
                    return;
                }
                match self
                    .discover_candidates(
                        &config,
                        None,
                        Duration::from_millis(config.global.timeouts.connect_ms),
                        cancellation.child_token(),
                    )
                    .await
                {
                    Ok(candidates) if !cancellation.is_cancelled() => {
                        if let Ok(mut cache) = self.discovery_cache.lock() {
                            cache.candidates = candidates;
                        }
                    }
                    Ok(_) => return,
                    Err(error) => {
                        tracing::warn!(error = %error, "bounded periodic camera discovery failed");
                    }
                }
            }
            let interval = match self.config_snapshot() {
                Ok(config) => Duration::from_secs(config.global.discovery.interval_seconds),
                Err(_) => return,
            };
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = self.cancellation.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }

    async fn run_schedule(
        self: Arc<Self>,
        plan: SchedulePlan,
        schedule_cancellation: CancellationToken,
    ) {
        let (instance, schedule_id) = plan.key_parts();
        let now = chrono::Utc::now();
        let mut last_consumed = match self
            .catalog
            .latest_schedule_occurrence(instance.clone(), schedule_id.clone())
            .await
        {
            Ok(Some(milliseconds)) => chrono::DateTime::from_timestamp_millis(milliseconds)
                // A corrupt-but-schema-valid out-of-range timestamp must not turn into an
                // unbounded cron search.  Start cleanly and leave the corrupt row unavailable
                // for re-admission because the catalog dedupe key still owns it.
                .unwrap_or_else(|| now - chrono::Duration::seconds(1)),
            Ok(None) => now - chrono::Duration::seconds(1),
            Err(error) => {
                tracing::error!(
                    instance = %instance,
                    schedule_id = %schedule_id,
                    error = %error,
                    "camera schedule could not load its durable recovery cursor"
                );
                return;
            }
        };
        loop {
            if self.cancellation.is_cancelled() || schedule_cancellation.is_cancelled() {
                return;
            }
            if self.reloading.load(Ordering::Acquire) {
                tokio::select! {
                    _ = self.cancellation.cancelled() => return,
                    _ = schedule_cancellation.cancelled() => return,
                    _ = tokio::time::sleep(SCHEDULER_POLL_INTERVAL) => continue,
                }
            }
            let now = chrono::Utc::now();
            // Decide first, then ask the catalog -- and only if the answer can still change
            // anything.
            //
            // This loop used to open with `has_schedule_overlap`, a `jobs_page(.., 1_000)` that
            // rebuilds and re-prepares its SQL, on every 200 ms tick of every schedule, before
            // anything had established that an occurrence was even due. At 256 cameras that is
            // ~1,280 catalog reads a second, funnelled through the same two connections that carry
            // the capture path's fsync-per-write transactions. Nothing was due on virtually all of
            // those ticks, and an overlap observation cannot make a not-due schedule due: it is read
            // in exactly one branch of `evaluate`, and only ever turns an `Admit` into a
            // `SkippedOverlap`. So the entire read volume bought one thing -- contention.
            //
            // Evaluating with `false` first is therefore exact, not an approximation: the only
            // decision an overlap can alter is `Admit`, so that is the only one worth asking about.
            let mut decision = plan.evaluate(last_consumed, now, SCHEDULER_MISFIRE_GRACE, false);
            let admitted = match &decision {
                Ok(ScheduleDecision::Admit {
                    occurrence,
                    consumed,
                }) if plan.skips_on_overlap() => Some((occurrence.clone(), *consumed)),
                _ => None,
            };
            if let Some((occurrence, consumed)) = admitted {
                let overlap = match self.has_schedule_overlap(&instance, &schedule_id).await {
                    Ok(overlap) => overlap,
                    Err(error) => {
                        tracing::warn!(
                            instance = %instance,
                            schedule_id = %schedule_id,
                            error = %error,
                            "camera schedule could not evaluate overlap"
                        );
                        false
                    }
                };
                if overlap {
                    decision = Ok(ScheduleDecision::SkippedOverlap {
                        occurrence,
                        consumed,
                    });
                }
            }
            match decision {
                Ok(ScheduleDecision::NotDue) => {}
                Ok(ScheduleDecision::SkippedMisfire { latest, consumed }) => {
                    last_consumed = latest.intended_fire_time;
                    tracing::info!(
                        instance = %instance,
                        schedule_id = %schedule_id,
                        intended_fire_time = %latest.intended_fire_time,
                        consumed,
                        "camera schedule skipped a misfire"
                    );
                }
                Ok(ScheduleDecision::SkippedOverlap {
                    occurrence,
                    consumed,
                }) => {
                    last_consumed = occurrence.intended_fire_time;
                    tracing::info!(
                        instance = %instance,
                        schedule_id = %schedule_id,
                        intended_fire_time = %occurrence.intended_fire_time,
                        consumed,
                        "camera schedule skipped an overlapping occurrence"
                    );
                }
                Ok(ScheduleDecision::Admit {
                    occurrence,
                    consumed,
                }) => {
                    last_consumed = occurrence.intended_fire_time;
                    if let Err(error) = self.submit_scheduled(&occurrence).await {
                        // The occurrence is consumed even when capacity or the backend policy
                        // rejects it. Repeating it would violate the scheduler's one-occurrence
                        // guarantee; a new cron occurrence will be evaluated normally.
                        tracing::warn!(
                            instance = %instance,
                            schedule_id = %schedule_id,
                            intended_fire_time = %occurrence.intended_fire_time,
                            consumed,
                            error = %error,
                            "camera schedule occurrence was not admitted"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(
                        instance = %instance,
                        schedule_id = %schedule_id,
                        error = %error,
                        "camera schedule evaluation failed"
                    );
                }
            }
            tokio::select! {
                _ = self.cancellation.cancelled() => return,
                _ = schedule_cancellation.cancelled() => return,
                _ = tokio::time::sleep(SCHEDULER_POLL_INTERVAL) => {}
            }
        }
    }

    /// Runs one group schedule: fire a synchronised capture across several cameras on a cron.
    ///
    /// The occurrence is submitted through [`Self::submit_group`] -- the same path the
    /// `sb/capture-group` command takes -- so a scheduled group is indistinguishable from a
    /// commanded one: one durable group row, all-or-nothing acceptance, one collated terminal
    /// notification. What makes that safe to do from a scheduler is that `submit_group` is already
    /// idempotent on its request id, and this loop derives that id from the schedule and the
    /// intended fire time. An occurrence is therefore admitted exactly once even if the component
    /// crashes between submitting it and recording that it did.
    ///
    /// This shipped only once the fleet queue existed. On the old fire-all-and-hope dispatch, a
    /// scheduled group larger than the effective capacity would have timed out its surplus members
    /// on every tick, forever, writing a durable failure row each time.
    async fn run_group_schedule(
        self: Arc<Self>,
        plan: SchedulePlan,
        schedule_cancellation: CancellationToken,
    ) {
        let (_, schedule_id) = plan.key_parts();
        let now = chrono::Utc::now();
        let cursor = match self
            .catalog
            .group_schedule_cursor(schedule_id.clone())
            .await
        {
            Ok(cursor) => cursor,
            Err(error) => {
                tracing::error!(
                    schedule_id = %schedule_id,
                    error = %error,
                    "group schedule could not load its durable recovery cursor"
                );
                return;
            }
        };
        let mut last_consumed = cursor
            .as_ref()
            .and_then(|cursor| {
                chrono::DateTime::from_timestamp_millis(cursor.intended_fire_time_ms)
            })
            .unwrap_or_else(|| now - chrono::Duration::seconds(1));
        let mut last_group_id = cursor.and_then(|cursor| cursor.last_group_id);
        loop {
            if self.cancellation.is_cancelled() || schedule_cancellation.is_cancelled() {
                return;
            }
            if self.reloading.load(Ordering::Acquire) {
                tokio::select! {
                    _ = self.cancellation.cancelled() => return,
                    _ = schedule_cancellation.cancelled() => return,
                    _ = tokio::time::sleep(SCHEDULER_POLL_INTERVAL) => continue,
                }
            }
            let now = chrono::Utc::now();
            // Decide first, and only ask about overlap when the answer can still change something
            // -- the same rule the camera loop learned the hard way in B6. An overlap observation
            // can only ever turn an `Admit` into a `SkippedOverlap`.
            let mut decision = plan.evaluate(last_consumed, now, SCHEDULER_MISFIRE_GRACE, false);
            let admitted = match &decision {
                Ok(ScheduleDecision::Admit {
                    occurrence,
                    consumed,
                }) if plan.skips_on_overlap() => Some((occurrence.clone(), *consumed)),
                _ => None,
            };
            if let Some((occurrence, consumed)) = admitted {
                // Evaluated against the GROUP, not its members: the previous occurrence is
                // outstanding until every camera in it is terminal.
                if self.group_schedule_overlaps(last_group_id.as_deref()).await {
                    decision = Ok(ScheduleDecision::SkippedOverlap {
                        occurrence,
                        consumed,
                    });
                }
            }
            match decision {
                Ok(ScheduleDecision::NotDue) => {}
                Ok(ScheduleDecision::SkippedMisfire { latest, consumed }) => {
                    last_consumed = latest.intended_fire_time;
                    self.record_group_occurrence(&schedule_id, latest.intended_fire_time, None)
                        .await;
                    tracing::info!(
                        schedule_id = %schedule_id,
                        intended_fire_time = %latest.intended_fire_time,
                        consumed,
                        "group schedule skipped a misfire"
                    );
                }
                Ok(ScheduleDecision::SkippedOverlap {
                    occurrence,
                    consumed,
                }) => {
                    last_consumed = occurrence.intended_fire_time;
                    self.record_group_occurrence(&schedule_id, occurrence.intended_fire_time, None)
                        .await;
                    tracing::info!(
                        schedule_id = %schedule_id,
                        intended_fire_time = %occurrence.intended_fire_time,
                        consumed,
                        "group schedule skipped an occurrence whose previous group is still running"
                    );
                }
                Ok(ScheduleDecision::Admit {
                    occurrence,
                    consumed,
                }) => {
                    last_consumed = occurrence.intended_fire_time;
                    match self.submit_scheduled_group(&occurrence).await {
                        Ok(group_id) => {
                            last_group_id = Some(group_id.clone());
                            self.record_group_occurrence(
                                &schedule_id,
                                occurrence.intended_fire_time,
                                Some(group_id),
                            )
                            .await;
                        }
                        Err(error) => {
                            // The occurrence is consumed even when it could not be admitted, exactly
                            // as a camera schedule consumes one: repeating it would violate the
                            // one-occurrence guarantee. The next cron occurrence is evaluated
                            // normally.
                            self.record_group_occurrence(
                                &schedule_id,
                                occurrence.intended_fire_time,
                                None,
                            )
                            .await;
                            tracing::warn!(
                                schedule_id = %schedule_id,
                                intended_fire_time = %occurrence.intended_fire_time,
                                consumed,
                                error = %error,
                                "group schedule occurrence was not admitted"
                            );
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(
                        schedule_id = %schedule_id,
                        error = %error,
                        "group schedule evaluation failed"
                    );
                }
            }
            tokio::select! {
                _ = self.cancellation.cancelled() => return,
                _ = schedule_cancellation.cancelled() => return,
                _ = tokio::time::sleep(SCHEDULER_POLL_INTERVAL) => {}
            }
        }
    }

    /// Whether this schedule's previous group is still running.
    ///
    /// One primary-key lookup, and only when an occurrence is actually due. A group that has been
    /// pruned by retention is long terminal, so a missing row is not an overlap.
    async fn group_schedule_overlaps(&self, last_group_id: Option<&str>) -> bool {
        let Some(group_id) = last_group_id else {
            return false;
        };
        match self.catalog.group(group_id.to_owned()).await {
            Ok(Some(group)) => !group.state.is_terminal(),
            Ok(None) => false,
            Err(error) => {
                // Fail closed: a catalog we cannot read is not a licence to pile a second group on
                // top of one that may still be running.
                tracing::warn!(
                    group_id = %group_id,
                    error = %error,
                    "group schedule could not evaluate overlap and skipped the occurrence"
                );
                true
            }
        }
    }

    async fn record_group_occurrence(
        &self,
        schedule_id: &str,
        intended_fire_time: chrono::DateTime<chrono::Utc>,
        group_id: Option<String>,
    ) {
        if let Err(error) = self
            .catalog
            .record_group_schedule_occurrence(
                schedule_id.to_owned(),
                intended_fire_time.timestamp_millis(),
                group_id,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
        {
            // The cursor is a recovery hint, not the authority -- the command ledger is. Losing this
            // write costs a redundant submission after a restart, which the ledger absorbs.
            tracing::warn!(
                schedule_id = %schedule_id,
                error = %error,
                "group schedule could not record its recovery cursor"
            );
        }
    }

    /// Submits one group-schedule occurrence as an ordinary capture group.
    async fn submit_scheduled_group(&self, occurrence: &ScheduleOccurrence) -> Result<String> {
        let config = self.config_snapshot()?;
        let schedule = config
            .global
            .capture_group_schedules
            .iter()
            .find(|schedule| schedule.id == occurrence.schedule_id && schedule.enabled)
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::InvalidRequest,
                    "group schedule is no longer enabled",
                )
            })?;
        // Derived, not random: this is what makes the occurrence exactly-once. The same occurrence
        // always produces the same request id, and `submit_group` answers a repeat with the group it
        // already accepted rather than a second one.
        let request_id = format!(
            "schedule:{}:{}",
            schedule.id,
            occurrence.intended_fire_time.timestamp_millis()
        );
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "scheduleId".to_string(),
            serde_json::Value::String(schedule.id.clone()),
        );
        metadata.insert(
            "intendedFireTime".to_string(),
            serde_json::Value::String(
                occurrence
                    .intended_fire_time
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            ),
        );
        let body = crate::commands::GroupCaptureRequest {
            request_id,
            instances: schedule.instances.clone(),
            capture_profile: schedule.capture_profile.clone(),
            profile_overrides: schedule.profile_overrides.clone(),
            timeout_ms: schedule.timeout_ms,
            metadata,
        };
        let correlation_id = format!("sched_{}", uuid::Uuid::now_v7());
        let group = self
            .submit_group(
                body,
                correlation_id,
                crate::admission::CapturePriority::Scheduled,
                None,
            )
            .await?;
        tracing::info!(
            schedule_id = %schedule.id,
            group_id = %group.group_id,
            members = schedule.instances.len(),
            intended_fire_time = %occurrence.intended_fire_time,
            "group schedule admitted a synchronised capture"
        );
        Ok(group.group_id)
    }

    async fn has_schedule_overlap(&self, instance: &str, schedule_id: &str) -> Result<bool> {
        let states = vec![
            crate::model::JobState::Accepted,
            crate::model::JobState::Queued,
            crate::model::JobState::Acquiring,
            crate::model::JobState::Encoding,
            crate::model::JobState::Persisting,
        ];
        let mut before = None;
        loop {
            let page = self
                .catalog
                .jobs_page(
                    Some(instance.to_owned()),
                    states.clone(),
                    before.clone(),
                    1_000,
                )
                .await?;
            if page.iter().any(|record| {
                record.trigger.get("type")
                    == Some(&serde_json::Value::String("schedule".to_string()))
                    && record.trigger.get("scheduleId")
                        == Some(&serde_json::Value::String(schedule_id.to_owned()))
            }) {
                return Ok(true);
            }
            let Some(last) = page.last() else {
                return Ok(false);
            };
            if page.len() < 1_000 {
                return Ok(false);
            }
            before = Some((last.accepted_at_ms, last.capture_id.clone()));
        }
    }

    async fn emit_schedule_skipped(&self, occurrence: &ScheduleOccurrence) {
        let Some(instance) = occurrence.scope.camera() else {
            return;
        };
        let event = self
            .events
            .read()
            .ok()
            .and_then(|events| events.get(instance).cloned());
        if let Some(event) = event {
            let _ = event
                .emit(
                    Severity::Warning,
                    "schedule-skipped",
                    Some("scheduled capture skipped because camera is moving".to_string()),
                    Some(serde_json::json!({
                        "scheduleId": occurrence.schedule_id,
                        "intendedFireTime": occurrence.intended_fire_time,
                        "code": "CAMERA_MOVING",
                    })),
                )
                .await;
        }
    }

    async fn submit_scheduled(&self, occurrence: &ScheduleOccurrence) -> Result<()> {
        let instance = occurrence.scope.camera().ok_or_else(|| {
            crate::CameraError::Catalog(
                "a group-schedule occurrence cannot be submitted as a single camera capture"
                    .to_string(),
            )
        })?;
        let config = self.config_snapshot()?;
        let camera = self.registry.camera_config(instance)?;
        if !camera.enabled {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::CameraDisabled,
                "camera was disabled before scheduled admission",
            ));
        }
        let schedule = camera
            .schedules
            .iter()
            .find(|schedule| schedule.id == occurrence.schedule_id && schedule.enabled)
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::InvalidRequest,
                    "schedule is no longer enabled for this camera",
                )
            })?;
        let profile = camera
            .capture_profiles
            .get(&schedule.capture_profile)
            .cloned()
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::UnknownCaptureProfile,
                    "scheduled capture profile is not configured",
                )
            })?;
        if profile
            .capture_interlock
            .unwrap_or(camera.ptz.capture_interlock)
            == crate::config::CaptureInterlock::Reject
        {
            if let Ok(actor) = self.actor(instance) {
                if matches!(
                    actor
                        .ptz(
                            crate::model::PtzRequest::Status,
                            tokio::time::Instant::now()
                                + Duration::from_millis(config.global.timeouts.ptz_ms),
                            &self.cancellation,
                        )
                        .await,
                    Ok(crate::model::PtzResult::Status(status)) if status.moving == Some(true)
                ) {
                    self.emit_schedule_skipped(occurrence).await;
                    return Ok(());
                }
            }
        }
        let accepted_at_ms = chrono::Utc::now().timestamp_millis();
        let terminal_ms = profile
            .timeout_ms
            .unwrap_or(config.global.timeouts.job_terminal_ms);
        let capture_mode = profile
            .capture_mode
            .unwrap_or_else(|| match &camera.backend {
                crate::config::BackendConfig::Sim(_) => crate::model::CaptureMode::Simulated,
                crate::config::BackendConfig::GenicamAravis(_) => {
                    crate::model::CaptureMode::SoftwareTrigger
                }
                crate::config::BackendConfig::OnvifRtsp(config) => config.capture_mode,
            });
        let capture_id = format!("cap_{}", uuid::Uuid::now_v7());
        let deadlines = crate::catalog::JobDeadlines {
            terminal_at_ms: accepted_at_ms
                .saturating_add(i64::try_from(terminal_ms).unwrap_or(i64::MAX)),
            queue_at_ms: profile.queue_expiry_ms.map(|duration| {
                accepted_at_ms.saturating_add(i64::try_from(duration).unwrap_or(i64::MAX))
            }),
            capture_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.capture_ms).unwrap_or(i64::MAX),
            ),
            encode_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.encode_ms).unwrap_or(i64::MAX),
            ),
            persist_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.persist_ms).unwrap_or(i64::MAX),
            ),
        };
        let relative_path = crate::storage::render_output_path(
            &config.global.output,
            crate::storage::OutputPathVariables {
                camera_id: instance,
                capture_id: &capture_id,
                timestamp: chrono::Utc::now(),
            },
            profile.output.encoding,
        )?;
        let snapshot = self.registry.snapshot(instance)?;
        let camera_summary = crate::messages::CameraSummary {
            backend: snapshot.backend,
            vendor: snapshot
                .capabilities
                .as_ref()
                .and_then(|capabilities| capabilities.vendor.clone()),
            model: snapshot
                .capabilities
                .as_ref()
                .and_then(|capabilities| capabilities.model.clone()),
            firmware: snapshot
                .capabilities
                .as_ref()
                .and_then(|capabilities| capabilities.firmware.clone()),
            serial: snapshot
                .capabilities
                .as_ref()
                .and_then(|capabilities| capabilities.serial.clone()),
        };
        let profile_snapshot = crate::jobs::JobProfileSnapshot {
            name: schedule.capture_profile.clone(),
            capture: profile.clone(),
            // The binding deliberately gives schedules a fail-fast default even when direct
            // capture defaults to wait-until-deadline.
            offline_policy: profile
                .offline_policy
                .unwrap_or(crate::config::OfflinePolicy::FailFast),
            maximum_frame_bytes: profile
                .maximum_frame_bytes
                .unwrap_or(config.global.limits.max_frame_bytes_per_camera),
            capture_mode,
            capture_interlock: profile
                .capture_interlock
                .unwrap_or(camera.ptz.capture_interlock),
            settle_ms: camera.ptz.settle_ms,
        };
        let trigger = crate::messages::CaptureTrigger::Schedule {
            schedule_id: occurrence.schedule_id.clone(),
            intended_fire_time: occurrence.intended_fire_time,
        };
        let correlation_id = uuid::Uuid::now_v7().to_string();
        let canonical = serde_json::json!({
            "scheduleId": occurrence.schedule_id,
            "intendedFireTime": occurrence.intended_fire_time,
            "captureProfile": schedule.capture_profile,
            "effectiveProfile": profile_snapshot,
            "deadlines": {
                "terminalAtMs": deadlines.terminal_at_ms,
                "queueAtMs": deadlines.queue_at_ms,
                "captureAtMs": deadlines.capture_at_ms,
                "encodeAtMs": deadlines.encode_at_ms,
                "persistAtMs": deadlines.persist_at_ms,
            },
            "intendedOutput": {
                "relativePath": relative_path.as_wire_path(),
                "backend": snapshot.backend.as_str(),
            },
        });
        let submission = crate::jobs::JobSubmission {
            job: crate::catalog::NewJob {
                capture_id: capture_id.clone(),
                instance: instance.to_owned(),
                ledger_key: None,
                request_hash: crate::idempotency::canonical_request_hash(&canonical, false)?,
                canonical_request: canonical,
                effective_profile: serde_json::to_value(&profile_snapshot)?,
                deadlines: deadlines.clone(),
                trigger: serde_json::to_value(&trigger)?,
                origin_correlation_id: None,
                intended_output: serde_json::json!({
                    "relativePath": relative_path.as_wire_path(),
                    "backend": snapshot.backend.as_str(),
                }),
                accepted_at_ms,
                group_id: None,
            },
            spec: crate::jobs::CaptureJobSpec {
                capture_id: capture_id.clone(),
                instance: instance.to_owned(),
                profile: profile_snapshot,
                resource_group: camera.resource_group.clone(),
                relative_path,
                deadlines,
                accepted_at_ms,
                trigger,
                correlation_id,
                metadata: serde_json::Map::new(),
                camera: camera_summary,
                group_size: None,
            },
            priority: crate::admission::CapturePriority::Scheduled,
        };
        self.ensure_storage_capacity().await?;
        let outcome = self
            .catalog
            .accept_scheduled_job(
                submission.job.clone(),
                occurrence.schedule_id.clone(),
                occurrence.intended_fire_time.timestamp_millis(),
            )
            .await?;
        if matches!(outcome, crate::catalog::AcceptJobOutcome::Inserted(_)) {
            let dispatcher = self.dispatcher(instance)?;
            self.engine(instance)?
                .queue_preaccepted(&dispatcher, submission)
                .await?;
        }
        Ok(())
    }

    fn spawn_task(
        &self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        let handle = tokio::spawn(task);
        self.tasks
            .lock()
            .map_err(|_| {
                crate::CameraError::Catalog("runtime task registry is unavailable".to_string())
            })?
            .push(handle);
        Ok(())
    }

    async fn recover_install_owned(&self) -> Result<()> {
        // A restart re-establishes every session, which is exactly what a reconnect asked for, so
        // an interrupted reconnect is settled rather than fenced. This must run before the
        // hazardous fence below: an OUTCOME_UNKNOWN reconnect row is unreclaimable by every
        // retention statement in the catalog and answers PREVIOUS_OUTCOME_UNKNOWN forever.
        let settled = self
            .catalog
            .settle_interrupted_reconnects(chrono::Utc::now().timestamp_millis())
            .await?;
        if settled > 0 {
            tracing::info!(
                settled,
                "settled reconnect commands interrupted by the previous run"
            );
        }
        // Generic PTZ/preset commands may have crossed a physical side-effect boundary before the
        // process died. They are never replayed automatically; exact retries receive the durable
        // PREVIOUS_OUTCOME_UNKNOWN result instead.
        self.catalog
            .mark_hazardous_commands_outcome_unknown(chrono::Utc::now().timestamp_millis())
            .await?;
        // A PERSISTING record whose install CAS won has a fully staged success envelope and can
        // be reconciled without reconnecting any camera.  Other active states need a fresh
        // command/runtime recovery policy; never quietly drop them during startup.
        for record in self.catalog.recovery_jobs().await? {
            let engine = self.engine(&record.instance)?;
            if record.install_started {
                let cancellation = CancellationToken::new();
                engine
                    .recover_install_started(record, &cancellation)
                    .await?;
            } else {
                engine.interrupt_recovered(record).await?;
            }
        }
        Ok(())
    }

    async fn run_supervisor(
        self: Arc<Self>,
        instance: String,
        engine: JobEngine,
        cancellation: CancellationToken,
        finished: CancellationToken,
    ) {
        self.run_supervisor_loop(instance, engine, cancellation)
            .await;
        finished.cancel();
    }

    async fn run_supervisor_loop(
        self: Arc<Self>,
        instance: String,
        engine: JobEngine,
        cancellation: CancellationToken,
    ) {
        let mut attempt = 0_u32;
        // A reload retains the registry/watch entry but explicitly advances its generation to
        // fence stale callbacks.  A replacement supervisor must continue from that fence rather
        // than restart at zero, or every one of its observations would be discarded as stale.
        let mut generation = self
            .registry
            .snapshot(&instance)
            .map_or(0, |snapshot| snapshot.generation);
        loop {
            let global_config = match self.global_config_snapshot() {
                Ok(config) => config,
                Err(error) => {
                    tracing::error!(instance = %instance, error = %error, "camera supervisor lost runtime configuration");
                    return;
                }
            };
            let camera = match self.registry.camera_config(&instance) {
                Ok(camera) if camera.enabled => camera,
                Ok(_) | Err(_) => return,
            };
            let factory = match self
                .backend_context
                .factory_for(&camera.backend, &global_config)
            {
                Ok(factory) => factory,
                Err(error) => {
                    self.publish_camera_state(
                        &instance,
                        generation,
                        CameraConnectionState::Backoff,
                        None,
                        Some(status_error(&error)),
                        chrono::Utc::now(),
                    );
                    return;
                }
            };
            if cancellation.is_cancelled() {
                self.publish_camera_state(
                    &camera.id,
                    generation,
                    CameraConnectionState::Stopping,
                    None,
                    None,
                    chrono::Utc::now(),
                );
                return;
            }
            generation = generation.saturating_add(1);
            self.publish_camera_state(
                &camera.id,
                generation,
                CameraConnectionState::Connecting,
                None,
                None,
                chrono::Utc::now(),
            );
            let permit = tokio::select! {
                _ = cancellation.cancelled() => return,
                permit = self.connect_gate.clone().acquire_owned() => match permit { Ok(permit) => permit, Err(_) => return },
            };
            let request = ConnectRequest {
                instance_id: camera.id.clone(),
                backend: camera.backend.clone(),
                timeout: Duration::from_millis(global_config.timeouts.connect_ms),
                cancellation: cancellation.child_token(),
            };
            let connected =
                crate::supervisor::isolate_backend_panic(factory.connect(request)).await;
            drop(permit);
            let retry_class = match &connected {
                Err(crate::CameraError::Config { .. }) => crate::supervisor::RetryClass::Permanent,
                _ => crate::supervisor::RetryClass::Transient,
            };
            match connected {
                Ok(session) => {
                    attempt = 0;
                    let capabilities = session.capabilities().clone();
                    let (actor, handle) = match CameraActor::new(
                        camera.id.clone(),
                        session,
                        engine.clone(),
                        global_config.limits.max_queued_captures_per_camera,
                        global_config.limits.max_queued_controls_per_camera,
                        self.scheduler.capacity_signal(),
                    ) {
                        Ok(pair) => pair,
                        Err(error) => {
                            self.publish_camera_state(
                                &camera.id,
                                generation,
                                CameraConnectionState::Backoff,
                                None,
                                Some(status_error(&error)),
                                chrono::Utc::now(),
                            );
                            self.sleep_backoff(
                                &camera.id,
                                attempt,
                                crate::supervisor::RetryClass::Permanent,
                                &cancellation,
                            )
                            .await;
                            attempt = attempt.saturating_add(1);
                            continue;
                        }
                    };
                    if let Ok(mut actors) = self.actors.write() {
                        actors.insert(camera.id.clone(), handle.clone());
                    }
                    let actor_cancellation = cancellation.child_token();
                    if let Ok(mut sessions) = self.session_cancellations.write() {
                        sessions.insert(camera.id.clone(), actor_cancellation.clone());
                    }
                    self.publish_camera_state(
                        &camera.id,
                        generation,
                        CameraConnectionState::Online,
                        Some(capabilities),
                        None,
                        chrono::Utc::now(),
                    );
                    let mut actor_task = tokio::spawn(actor.run(actor_cancellation));
                    // The camera is online: tell the fleet queue it can take work. There is no
                    // per-camera cache to drain any more, and therefore no loop here that drains one
                    // -- the scheduler pulls, it is not pushed to. This supervisor's only remaining
                    // job while connected is to wait for its actor to finish or be cancelled.
                    self.scheduler.camera_online(&camera.id, handle.clone());
                    let result = tokio::select! {
                        joined = &mut actor_task => joined.map_err(|error| crate::CameraError::Backend {
                            backend: "actor",
                            message: format!("actor task failed: {error}"),
                        }).and_then(|result| result),
                        _ = cancellation.cancelled() => {
                            // The actor holds a child of this token and is already winding down, so
                            // it must be awaited, not dropped: its teardown is what delivers the
                            // shutdown safety stop and closes the session. The budget is the smaller
                            // of the two deadlines that already bound this path — the shutdown grace
                            // and the reload drain timeout — so a hung backend can defeat neither.
                            let grace = Duration::from_millis(
                                global_config
                                    .timeouts
                                    .shutdown_grace_ms
                                    .min(global_config.timeouts.reload_drain_timeout_ms),
                            );
                            if !join_actor_within_grace(&mut actor_task, grace).await {
                                tracing::warn!(
                                    instance = %camera.id,
                                    grace_ms = grace.as_millis(),
                                    "camera actor did not complete its shutdown teardown within the grace budget; aborting"
                                );
                            }
                            Ok(())
                        }
                    };
                    // Its queued work stays queued. That is the entire point of a queue that outlives
                    // the session: a camera that drops does not lose the captures promised to it.
                    self.scheduler.camera_offline(&camera.id);
                    if let Ok(mut actors) = self.actors.write() {
                        actors.remove(&camera.id);
                    }
                    if let Ok(mut sessions) = self.session_cancellations.write() {
                        sessions.remove(&camera.id);
                    }
                    if cancellation.is_cancelled() {
                        return;
                    }
                    if let Err(error) = result {
                        self.publish_camera_state(
                            &camera.id,
                            generation,
                            CameraConnectionState::Backoff,
                            None,
                            Some(status_error(&error)),
                            chrono::Utc::now(),
                        );
                    }
                }
                Err(error) => {
                    self.publish_camera_state(
                        &camera.id,
                        generation,
                        CameraConnectionState::Backoff,
                        None,
                        Some(status_error(&error)),
                        chrono::Utc::now(),
                    );
                }
            }
            self.sleep_backoff(&camera.id, attempt, retry_class, &cancellation)
                .await;
            attempt = attempt.saturating_add(1);
        }
    }

    async fn sleep_backoff(
        &self,
        instance: &str,
        attempt: u32,
        retry_class: crate::supervisor::RetryClass,
        cancellation: &CancellationToken,
    ) {
        let config = match self.config_snapshot() {
            Ok(config) => config,
            Err(error) => {
                tracing::error!(instance, error = %error, "camera supervisor cannot load reconnect policy");
                return;
            }
        };
        let policy = match crate::supervisor::BackoffPolicy::new(
            Duration::from_millis(config.global.timeouts.reconnect_backoff_min_ms),
            Duration::from_millis(config.global.timeouts.reconnect_backoff_max_ms),
        ) {
            Ok(policy) => policy,
            Err(error) => {
                tracing::error!(error = %error, "validated reconnect policy became invalid");
                return;
            }
        };
        let delay = policy.delay(instance, 1, retry_class, attempt);
        tokio::select! {
            _ = cancellation.cancelled() => {}
            _ = tokio::time::sleep(delay) => {}
        }
    }

    async fn handle_deferred_capture(
        &self,
        request: Message,
        deferred: DeferredReplyRegistry,
    ) -> CommandOutcome {
        let config = match self.config_snapshot() {
            Ok(config) => config,
            Err(error) => return CommandOutcome::ImmediateError(command_error(&error)),
        };
        let body: Result<CaptureRequest> = commands::parse_closed(request.body.clone());
        let body = match body.and_then(|body| {
            body.validate(config.global.limits.max_metadata_bytes)?;
            Ok(body)
        }) {
            Ok(body) => body,
            Err(error) => return CommandOutcome::ImmediateError(command_error(&error)),
        };
        let token = match deferred.defer(
            &request,
            Duration::from_millis(config.global.timeouts.max_deferred_reply_lifetime_ms),
        ) {
            Ok(token) => token,
            Err(error) => {
                return CommandOutcome::ImmediateError(CommandError::new(
                    crate::ErrorCode::ReplyRequired.as_str(),
                    error.message,
                ));
            }
        };
        if token.activate().is_err() {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::BackendError.as_str(),
                "deferred reply could not be activated",
            ));
        }
        let Some(runtime) = self.self_reference.get().and_then(Weak::upgrade) else {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::ComponentStopping.as_str(),
                "camera runtime is not available",
            ));
        };
        let correlation_id = request.header.correlation_id.clone();
        let request_uuid = request.header.uuid.clone();
        let continuation_token = token.clone();
        CommandOutcome::deferred_with_continuation(token, async move {
            runtime
                .accept_deferred_capture(body, correlation_id, request_uuid, continuation_token)
                .await
                .map_err(|error| command_error(&error))
        })
    }

    async fn accept_deferred_capture(
        &self,
        body: CaptureRequest,
        correlation_id: String,
        request_uuid: String,
        token: DeferredReplyToken,
    ) -> Result<()> {
        let config = self.config_snapshot()?;
        let instance = self
            .registry
            .resolve_actuation_instance(body.instance.as_deref())?;
        let request_id = body.request_id.clone();
        let waiter_id = format!("wait_{}", uuid::Uuid::now_v7());
        self.waiters.prepare(
            instance.clone(),
            request_id.clone(),
            waiter_id.clone(),
            token.clone(),
            correlation_id.clone(),
            request_uuid.clone(),
        )?;
        let accepted = self
            .submit_capture(
                instance.clone(),
                request_id.clone(),
                body.capture_profile,
                body.timeout_ms,
                body.metadata,
                correlation_id.clone(),
                "sb/capture",
                crate::admission::CapturePriority::Direct,
            )
            .await?;
        let record = match accepted {
            crate::catalog::AcceptJobOutcome::Inserted(_) => return Ok(()),
            crate::catalog::AcceptJobOutcome::Existing(record) => record,
            crate::catalog::AcceptJobOutcome::Conflict => {
                let _ = self.waiters.take_pending(&instance, &request_id);
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::IdempotencyConflict,
                    "requestId was already used with different immutable capture arguments",
                ));
            }
        };
        let _ = self.waiters.take_pending(
            &record.instance,
            record.request_id.as_deref().unwrap_or_default(),
        );
        if let Some(terminal) = record.terminal_result {
            token.settle_success(Some(terminal)).await.map_err(|_| {
                crate::CameraError::rejected(
                    crate::ErrorCode::BackendError,
                    "deferred reply could not be settled",
                )
            })?;
            return Ok(());
        }
        let now = chrono::Utc::now().timestamp_millis();
        self.catalog
            .add_waiter(crate::catalog::WaiterRecord {
                waiter_id: waiter_id.clone(),
                capture_id: record.capture_id.clone(),
                correlation_id,
                request_uuid: Some(request_uuid),
                expires_at_ms: now.saturating_add(
                    i64::try_from(config.global.timeouts.max_deferred_reply_lifetime_ms)
                        .unwrap_or(i64::MAX),
                ),
                created_at_ms: now,
            })
            .await?;
        self.waiters
            .register(record.capture_id.clone(), waiter_id, token.clone())?;
        if let Some(terminal) = self
            .catalog
            .job(record.capture_id)
            .await?
            .and_then(|job| job.terminal_result)
        {
            token.settle_success(Some(terminal)).await.map_err(|_| {
                crate::CameraError::rejected(
                    crate::ErrorCode::BackendError,
                    "deferred reply could not be settled",
                )
            })?;
        }
        Ok(())
    }

    async fn handle_deferred_group_capture(
        &self,
        request: Message,
        deferred: DeferredReplyRegistry,
    ) -> CommandOutcome {
        let config = match self.config_snapshot() {
            Ok(config) => config,
            Err(error) => return CommandOutcome::ImmediateError(command_error(&error)),
        };
        let body: Result<GroupCaptureRequest> = commands::parse_closed(request.body.clone());
        let body = match body.and_then(|body| {
            body.validate(
                config.global.limits.max_cameras_per_group,
                config.global.limits.max_metadata_bytes,
            )?;
            Ok(body)
        }) {
            Ok(body) => body,
            Err(error) => return CommandOutcome::ImmediateError(command_error(&error)),
        };
        let token = match deferred.defer(
            &request,
            Duration::from_millis(config.global.timeouts.max_deferred_reply_lifetime_ms),
        ) {
            Ok(token) => token,
            Err(error) => {
                return CommandOutcome::ImmediateError(CommandError::new(
                    crate::ErrorCode::ReplyRequired.as_str(),
                    error.message,
                ));
            }
        };
        if token.activate().is_err() {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::BackendError.as_str(),
                "deferred reply could not be activated",
            ));
        }
        let Some(runtime) = self.self_reference.get().and_then(Weak::upgrade) else {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::ComponentStopping.as_str(),
                "camera runtime is not available",
            ));
        };
        let correlation_id = request.header.correlation_id.clone();
        let continuation_token = token.clone();
        CommandOutcome::deferred_with_continuation(token, async move {
            runtime
                .submit_group(
                    body,
                    correlation_id,
                    crate::admission::CapturePriority::Direct,
                    Some(continuation_token),
                )
                .await
                .map(|_| ())
                .map_err(|error| command_error(&error))
        })
    }
}

#[async_trait]
impl CameraCommandService for CameraRuntime {
    async fn handle_camera_command(
        &self,
        verb: &'static str,
        request: Message,
        deferred: DeferredReplyRegistry,
    ) -> CommandOutcome {
        if self.reloading.load(Ordering::Acquire) {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::CameraUnavailable.as_str(),
                "the camera adapter is draining a configuration replacement",
            ));
        }
        if verb == "sb/capture" {
            return self.handle_deferred_capture(request, deferred).await;
        }
        if verb == "sb/capture-group" {
            return self.handle_deferred_group_capture(request, deferred).await;
        }
        let config = match self.config_snapshot() {
            Ok(config) => config,
            Err(error) => return CommandOutcome::ImmediateError(command_error(&error)),
        };
        let outcome: Result<serde_json::Value> = async {
            match verb {
                "sb/list" => {
                    let body: ListRequest = commands::parse_closed(request.body.clone())?;
                    body.validate()?;
                    let query = serde_json::json!({
                        "includeCapabilities": body.include_capabilities,
                        "includeUnconfigured": body.include_unconfigured,
                    });
                    let initial = if body.cursor.is_none() {
                        let cameras =
                            self.registry
                                .snapshots(4_096)?
                                .into_iter()
                                .map(|snapshot| {
                                    if body.include_capabilities {
                                        serde_json::to_value(snapshot).map_err(crate::CameraError::from)
                                    } else {
                                        Ok(serde_json::json!({
                                            "instance": snapshot.instance,
                                            "enabled": snapshot.enabled,
                                            "state": snapshot.state,
                                            "backend": snapshot.backend,
                                        }))
                                    }
                                })
                                .collect::<Result<Vec<_>>>()?;
                        let unconfigured = if body.include_unconfigured
                            && config.global.discovery.report_unconfigured
                        {
                            self.unconfigured_discoveries(&config)?
                        } else {
                            Vec::new()
                        };
                        Some((cameras, unconfigured))
                    } else {
                        None
                    };
                    let (cameras, unconfigured, next_cursor) = self.cursors.list_page(
                        &query,
                        body.cursor.as_deref(),
                        initial,
                        usize::from(body.limit),
                    )?;
                    Ok(serde_json::json!({
                        "cameras": cameras,
                        "unconfigured": unconfigured,
                        "nextCursor": next_cursor,
                    }))
                }
                "sb/discover" => {
                    let body: DiscoverRequest = commands::parse_closed(request.body.clone())?;
                    self.discover(body).await
                }
                "sb/status" => {
                    let body: StatusRequest = commands::parse_closed(request.body.clone())?;
                    body.validate()?;
                    match body.instance {
                        Some(instance) => Ok(serde_json::to_value(self.registry.snapshot(&instance)?)?),
                        None => Ok(serde_json::json!({ "cameras": self.registry.snapshots(1_000)? })),
                    }
                }
                "sb/capture-submit" => {
                    let body: CaptureRequest = commands::parse_closed(request.body.clone())?;
                    body.validate(config.global.limits.max_metadata_bytes)?;
                    let instance = self.registry.resolve_actuation_instance(body.instance.as_deref())?;
                    let accepted = self.submit_capture(
                        instance,
                        body.request_id,
                        body.capture_profile,
                        body.timeout_ms,
                        body.metadata,
                        request.header.correlation_id.clone(),
                        verb,
                        crate::admission::CapturePriority::Submitted,
                    ).await?;
                    let record = match accepted {
                        crate::catalog::AcceptJobOutcome::Inserted(record)
                        | crate::catalog::AcceptJobOutcome::Existing(record) => record,
                        crate::catalog::AcceptJobOutcome::Conflict => return Err(crate::CameraError::rejected(
                            crate::ErrorCode::IdempotencyConflict,
                            "requestId was already used with different immutable capture arguments",
                        )),
                    };
                    Ok(serde_json::json!({
                        "captureId": record.capture_id,
                        "state": record.state,
                        "acceptedAt": chrono::DateTime::from_timestamp_millis(record.accepted_at_ms),
                        "statusVerb": "sb/capture-status",
                    }))
                }
                "sb/capture-group-submit" => {
                    let body: GroupCaptureRequest = commands::parse_closed(request.body.clone())?;
                    let group = self.submit_group(
                        body,
                        request.header.correlation_id.clone(),
                        crate::admission::CapturePriority::Submitted,
                        None,
                    ).await?;
                    Ok(serde_json::json!({
                        "captureGroupId": group.group_id,
                        "state": group.state,
                        "members": group.members.iter().map(|member| serde_json::json!({
                            "instance": member.instance,
                            "captureId": member.capture_id,
                            "state": member.state,
                        })).collect::<Vec<_>>(),
                    }))
                }
                "sb/capture-status" => {
                    let body: CaptureStatusRequest = commands::parse_closed(request.body.clone())?;
                    let limit = body.limit;
                    let cursor = body.cursor.clone();
                    match body.validate()? {
                        CaptureStatusMode::Capture => {
                            let id = body.capture_id.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::InvalidRequest, "captureId is required"))?;
                            let job = self.catalog.job(id).await?.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::CaptureNotFound, "capture was not found"))?;
                            Ok(job_status_json(&job))
                        }
                        CaptureStatusMode::Group => {
                            let id = body.capture_group_id.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::InvalidRequest, "captureGroupId is required"))?;
                            let group = self.catalog.group(id).await?.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::CaptureNotFound, "capture group was not found"))?;
                            self.group_status_page(group, usize::from(limit), cursor.as_deref())
                        }
                        CaptureStatusMode::CameraRequest => {
                            let instance = body.instance.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::InvalidRequest, "instance is required"))?;
                            let request_id = body.request_id.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::InvalidRequest, "requestId is required"))?;
                            let job = self.catalog.job_by_ledger(crate::catalog::LedgerKey::new(instance, "sb/capture", request_id)?).await?
                                .ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::CaptureNotFound, "capture was not found"))?;
                            Ok(job_status_json(&job))
                        }
                        CaptureStatusMode::GroupRequest => {
                            let request_id = body.request_id.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::InvalidRequest, "requestId is required"))?;
                            let group = self.catalog.group_by_ledger(crate::catalog::LedgerKey::new("main", "sb/capture-group", request_id)?).await?
                                .ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::CaptureNotFound, "capture group was not found"))?;
                            self.group_status_page(group, usize::from(limit), cursor.as_deref())
                        }
                        CaptureStatusMode::List => {
                            self.jobs_status_page(&body).await
                        }
                    }
                }
                "sb/capture-cancel" => {
                    let body: CancelRequest = commands::parse_closed(request.body.clone())?;
                    self.cancel_capture(body).await
                }
                "sb/queue-status" => {
                    let body: commands::QueueStatusRequest =
                        commands::parse_closed(request.body.clone())?;
                    self.queue_status_command(body).await
                }
                "sb/queue-clear" => {
                    let body: commands::QueueClearRequest =
                        commands::parse_closed(request.body.clone())?;
                    self.queue_clear_command(body).await
                }
                "sb/reconnect" => {
                    let body: ReconnectRequest = commands::parse_closed(request.body.clone())?;
                    self.reconnect(body).await
                }
                "sb/ptz" => {
                    let body: PtzCommandRequest = commands::parse_closed(request.body.clone())?;
                    self.perform_ptz(body).await
                }
                "sb/ptz-presets" => {
                    let body: PtzPresetsRequest = commands::parse_closed(request.body.clone())?;
                    self.perform_presets(body).await
                }
                _ => Err(crate::CameraError::rejected(
                    crate::ErrorCode::UnsupportedCapability,
                    "unsupported camera command verb",
                )),
            }
        }.await;
        match outcome {
            Ok(value) => CommandOutcome::ImmediateSuccess(Some(value)),
            Err(error) => CommandOutcome::ImmediateError(command_error(&error)),
        }
    }
}

fn status_error(error: &crate::CameraError) -> CameraStatusError {
    CameraStatusError {
        code: error.code().as_str().to_string(),
        message: command_error(error).message,
        observed_at: chrono::Utc::now(),
    }
}

fn job_status_json(record: &crate::catalog::JobRecord) -> serde_json::Value {
    serde_json::json!({
        "captureId": record.capture_id,
        "instance": record.instance,
        "state": record.state,
        "acceptedAtMs": record.accepted_at_ms,
        "terminalAtMs": record.terminal_at_ms,
        "captureGroupId": record.group_id,
        "errorCode": record.error_code,
        "errorMessage": record.error_message,
        "result": record.terminal_result,
    })
}

/// The public aggregate keeps the design's `COMPLETED`/`PARTIAL` distinction even though the
/// durable catalog deliberately stores only the shared job terminal state vocabulary. Member
/// terminal bodies are reused verbatim so a direct group reply cannot diverge from durable status.
fn group_terminal_json(record: &crate::catalog::GroupRecord) -> serde_json::Value {
    let succeeded = record
        .members
        .iter()
        .filter(|member| member.state == crate::model::JobState::Succeeded)
        .count();
    let state = if succeeded == record.members.len() {
        "COMPLETED"
    } else if succeeded == 0 {
        "FAILED"
    } else {
        "PARTIAL"
    };
    let members = record
        .members
        .iter()
        .map(|member| {
            member
                .terminal_result
                .clone()
                .unwrap_or_else(|| job_status_json(member))
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "captureGroupId": record.group_id,
        "requestId": record.request_id,
        "state": state,
        "members": members,
    })
}

/// All application command verbs required by the binding design.
pub const CAMERA_COMMAND_VERBS: [&str; 14] = [
    "sb/list",
    "sb/discover",
    "sb/status",
    "sb/capture",
    "sb/capture-submit",
    "sb/capture-group",
    "sb/capture-group-submit",
    "sb/capture-status",
    "sb/capture-cancel",
    "sb/queue-status",
    "sb/queue-clear",
    "sb/reconnect",
    "sb/ptz",
    "sb/ptz-presets",
];

/// Durable states in which a capture still owes the operator an outcome.
const NON_TERMINAL_JOB_STATES: [crate::model::JobState; 5] = [
    crate::model::JobState::Accepted,
    crate::model::JobState::Queued,
    crate::model::JobState::Acquiring,
    crate::model::JobState::Encoding,
    crate::model::JobState::Persisting,
];

/// Durable states in which a capture has been promised but no physical work has begun.
const BACKLOG_JOB_STATES: [crate::model::JobState; 2] = [
    crate::model::JobState::Accepted,
    crate::model::JobState::Queued,
];

/// What one camera is holding.
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CameraQueueDepth {
    /// Camera instance token.
    pub instance: String,
    /// Descriptors queued plus reservations taken, i.e. what counts against the camera's ceiling.
    pub queued: usize,
    /// The ceiling itself. A camera at `queued == capacity` is answering QUEUE_FULL.
    pub capacity: usize,
}

/// The live answer to "is the component coping, and if not, where is it stuck?"
///
/// It is assembled from three places on purpose, because no one of them can answer it alone:
/// admission says what capacity is left, the per-camera dispatchers say what is waiting to be
/// handed to a camera, and the catalog says what the component still owes -- the only one of the
/// three that survives a restart.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueStatus {
    /// Live admission capacity: permits, unreserved frame memory, outstanding disk bytes.
    pub admission: crate::admission::AdmissionSnapshot,
    /// The configured ceilings the numbers above should be read against.
    pub limits: QueueLimits,
    /// Per-camera dispatcher depth.
    pub cameras: Vec<CameraQueueDepth>,
    /// Total descriptors held across every camera's dispatcher.
    pub dispatch_queued: usize,
    /// Durable non-terminal counts, keyed by state token.
    pub durable: BTreeMap<String, u64>,
    /// Durable captures promised but not started (ACCEPTED + QUEUED).
    pub durable_backlog: u64,
    /// Durable captures already doing physical work (ACQUIRING + ENCODING + PERSISTING).
    pub durable_in_flight: u64,
}

/// The ceilings a [`QueueStatus`] should be read against.
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueueLimits {
    /// Global acquisition permits.
    pub max_concurrent_captures: usize,
    /// Frame-memory budget.
    pub max_in_flight_bytes: u64,
    /// Per-camera dispatcher ceiling.
    pub max_queued_captures_per_camera: usize,
    /// The component's fleet-wide pending ceiling.
    pub max_pending_captures: usize,
}

/// What a break-glass drain actually did.
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueueClearOutcome {
    /// Captures cancelled by this call.
    pub cancelled: usize,
    /// Captures that reached a terminal state on their own before the drain got to them.
    pub already_terminal: usize,
    /// Captures the drain could not cancel, with the reason. Bounded; a drain reports what it could
    /// not do rather than claiming a clean sweep.
    pub failed: Vec<QueueClearFailure>,
}

/// One capture a drain could not cancel.
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueueClearFailure {
    /// The capture that survived the drain.
    pub capture_id: String,
    /// Operator-safe reason.
    pub error: String,
}

/// Performs the side-effect-free adapter half of core candidate validation.
///
/// The core has already parsed the candidate into a JSON document before this callback runs.
/// This second parse applies the camera adapter's closed backend schemas and its deliberately
/// different initial/reload policy without resolving credentials, opening files, or touching
/// sessions. Diagnostics intentionally remain generic: candidate documents can contain secret
/// references and a validator error is surfaced outside the adapter's redaction boundary.
pub fn validate_configuration_candidate(
    candidate: serde_json::Value,
    redacted_current: Option<serde_json::Value>,
    phase: ConfigurationValidationPhase,
) -> edgecommons::Result<ConfigurationValidationResult> {
    validate_configuration_candidate_with_credentials(
        candidate,
        redacted_current,
        phase,
        // This compatibility entry point cannot observe the live component services. The process
        // entry point uses the credential-aware variant below once the initial core generation
        // has constructed its immutable credential service.
        true,
    )
}

/// Performs candidate validation with the availability of the immutable credential service.
///
/// A core credential service is constructed only for the initial generation. A reload must
/// therefore reject an ONVIF secret reference when that service was absent at startup, rather
/// than accepting the generation and discovering the failure after core configuration committed.
pub fn validate_configuration_candidate_with_credentials(
    candidate: serde_json::Value,
    redacted_current: Option<serde_json::Value>,
    phase: ConfigurationValidationPhase,
    credential_service_available: bool,
) -> edgecommons::Result<ConfigurationValidationResult> {
    let core = match Config::from_value(COMPONENT_NAME, "candidate", candidate) {
        Ok(config) => config,
        Err(_) => {
            return Ok(ConfigurationValidationResult::reject(
                "CAMERA_CONFIG_INVALID",
                "camera adapter configuration is invalid",
            ));
        }
    };
    let result = match phase {
        ConfigurationValidationPhase::Initial => {
            AdapterConfig::from_core_initial(&core).map(|_| ())
        }
        ConfigurationValidationPhase::Reload => (|| -> Result<()> {
            let replacement = AdapterConfig::from_core_reload(&core)?;
            if !credential_service_available
                && replacement.instances.iter().any(|camera| {
                    let crate::config::BackendConfig::OnvifRtsp(onvif) = &camera.backend else {
                        return false;
                    };
                    onvif.credentials.is_some() || onvif.tls.ca.is_some()
                })
            {
                return Err(crate::CameraError::Config {
                    path: "component.instances[].backend".to_string(),
                    message: "ONVIF secret references require credentials configured at component startup"
                        .to_string(),
                });
            }
            if let Some(current) = redacted_current {
                let current =
                    Config::from_value(COMPONENT_NAME, "current", current).map_err(|_| {
                        crate::CameraError::Config {
                            path: "component".to_string(),
                            message: "current configuration could not be compared safely"
                                .to_string(),
                        }
                    })?;
                let current = AdapterConfig::from_core_reload(&current)?;
                if current.global.state.directory != replacement.global.state.directory
                    || current.global.output.root_directory
                        != replacement.global.output.root_directory
                    || current.global.output.directory_mode
                        != replacement.global.output.directory_mode
                    || current.global.output.file_mode != replacement.global.output.file_mode
                {
                    return Err(crate::CameraError::Config {
                        path: "component.global".to_string(),
                        message: "camera state/output root security settings require restart"
                            .to_string(),
                    });
                }
            }
            Ok(())
        })(),
    };
    match result {
        Ok(()) => Ok(ConfigurationValidationResult::accept()),
        Err(_) => Ok(ConfigurationValidationResult::reject(
            "CAMERA_CONFIG_INVALID",
            "camera adapter configuration is invalid",
        )),
    }
}

/// Converts one adapter error into the core command-reply shape without creating an alternate
/// error vocabulary at the command boundary.
#[must_use]
pub fn command_error(error: &crate::CameraError) -> CommandError {
    let message: String = error
        .to_string()
        .chars()
        .filter(|character| !character.is_control())
        .take(256)
        .collect();
    CommandError::new(error.code().as_str(), message)
}

/// Durable, protocol-neutral objects which must be valid before camera supervisor creation.
///
/// The resources are intentionally assembled before any backend connects. A connection failure
/// is an ordinary per-camera lifecycle event; a state, catalog, or output failure is a startup
/// failure and must keep component readiness false.
pub struct StartupResources {
    /// Deterministically resolved durable state directory.
    pub state_directory: PathBuf,
    /// Verified SQLite catalog and exclusive state-directory lock.
    pub catalog: Catalog,
    /// Capability-scoped output root.
    pub storage: StorageRoot,
    /// Bounded global capture/encoder/writer admission controls.
    pub admission: AdmissionController,
    /// Compact configured roster, including disabled cameras.
    pub registry: Arc<CameraRegistry>,
}

/// Resolves and validates all startup resources that are independent of live camera sessions.
///
/// This function is deliberately all-or-nothing. It makes the adapter's initial-ready boundary
/// testable and prevents an actor from being created before exclusive state ownership is known.
pub async fn prepare_startup_resources(
    config: &AdapterConfig,
    platform: Platform,
) -> Result<StartupResources> {
    let state_directory =
        resolve_state_directory(platform, config.global.state.directory.as_deref())?;
    create_state_directory(&state_directory)?;

    let storage = StorageRoot::open(&config.global.output)?;
    storage.check_storage_pressure(StorageReservation {
        current_bytes: config.global.limits.max_frame_bytes_per_camera,
        other_bytes: 0,
    })?;

    let catalog = Catalog::open(CatalogOptions::new(state_directory.clone())).await?;
    let health = catalog.health().await?;
    if !health.integrity_ok
        || !health.foreign_keys
        || !health.journal_mode.eq_ignore_ascii_case("wal")
    {
        return Err(crate::CameraError::Catalog(
            "catalog startup verification did not confirm required durability settings".to_string(),
        ));
    }
    let admission = AdmissionController::new(
        &config.global.limits,
        &config.global.output,
        Arc::new(FilesystemSpaceProbe::default()),
    )?;
    let registry = Arc::new(CameraRegistry::new(config)?);
    Ok(StartupResources {
        state_directory,
        catalog,
        storage,
        admission,
        registry,
    })
}

fn create_state_directory(directory: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(directory).map_err(|error| {
        crate::CameraError::Storage(format!(
            "failed to create configured durable state directory: {error}"
        ))
    })?;
    let metadata = std::fs::metadata(directory)?;
    if !metadata.is_dir() {
        return Err(crate::CameraError::Storage(
            "configured durable state path is not a directory".to_string(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).map_err(
            |error| {
                crate::CameraError::Storage(format!(
                    "failed to restrict durable state directory permissions: {error}"
                ))
            },
        )?;
    }
    Ok(())
}

/// Durable rows reclaimed by one retention sweep, reported so an operator can tell the subsystem
/// is alive and how much state it is holding back.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RetentionSweep {
    delivered_outbox: u64,
    terminal_jobs: u64,
    terminal_groups: u64,
    command_ledgers: u64,
    over_limit_jobs: u64,
}

impl RetentionSweep {
    fn reclaimed(self) -> u64 {
        self.delivered_outbox
            .saturating_add(self.terminal_jobs)
            .saturating_add(self.terminal_groups)
            .saturating_add(self.command_ledgers)
            .saturating_add(self.over_limit_jobs)
    }
}

/// Repeats one bounded catalog prune until it stops reclaiming rows.
///
/// The batch size and the pause between batches keep a large backlog off the capture hot path:
/// the shared two-worker catalog pool must never be saturated by a reclaim, because the actor
/// treats any catalog error as a fatal session failure.  Cancellation is observed between
/// batches so shutdown is never delayed by a sweep in flight.
async fn prune_in_batches<F, Fut>(
    cancellation: &CancellationToken,
    batch: usize,
    mut prune: F,
) -> Result<u64>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<u64>>,
{
    let mut reclaimed = 0_u64;
    for round in 0..RETENTION_MAX_BATCHES {
        if cancellation.is_cancelled() {
            break;
        }
        if round > 0 {
            tokio::time::sleep(RETENTION_BATCH_PAUSE).await;
        }
        let removed = prune(batch).await?;
        reclaimed = reclaimed.saturating_add(removed);
        if removed < batch as u64 {
            break;
        }
    }
    Ok(reclaimed)
}

/// Awaits the graceful teardown a cancelled actor is already running, and aborts only when the
/// grace expires.  Returns `true` when the actor stopped itself within the budget.
///
/// The actor owns a child of the supervisor's cancellation token, so by the time the supervisor
/// observes cancellation the actor is winding down: it delivers its queued shutdown safety stop
/// (on a fresh token, so a tripped component token cannot suppress it), drains queued captures,
/// and closes the protocol session.  Aborting immediately drops that future at its first await
/// point — deterministically, since the actor task has not been polled yet — which leaves a
/// panning camera moving after exit and leaks every RTSP/ONVIF session server-side.
async fn join_actor_within_grace(actor_task: &mut JoinHandle<Result<()>>, grace: Duration) -> bool {
    if tokio::time::timeout(grace, &mut *actor_task).await.is_ok() {
        return true;
    }
    actor_task.abort();
    let _ = actor_task.await;
    false
}

/// Runtime command implementation installed only after every startup gate has passed.
#[async_trait]
pub trait CameraCommandService: Send + Sync + 'static {
    /// Handles one full core command envelope and selects immediate or deferred settlement.
    async fn handle_camera_command(
        &self,
        verb: &'static str,
        request: Message,
        deferred: DeferredReplyRegistry,
    ) -> CommandOutcome;
}

type AppFacadeFactory =
    dyn Fn(&str, Arc<Config>) -> edgecommons::Result<Arc<AppFacade>> + Send + Sync;
type EventsFacadeFactory =
    dyn Fn(&str, Arc<Config>) -> edgecommons::Result<EventsFacade> + Send + Sync;

/// A fully preflighted runtime reload held while Core retains the prior configuration snapshot.
/// `commit` either completes the adapter generation transition or restores its captured prior
/// service before Core rejects the candidate; only a successful return permits Core to swap its
/// own snapshot.
struct RuntimeReloadTransaction {
    runtime: Arc<CameraRuntime>,
    replacement: AdapterConfig,
    apps: BTreeMap<String, Arc<AppFacade>>,
    events: BTreeMap<String, EventsFacade>,
    checkpoint: Option<RuntimeReloadCheckpoint>,
}

impl RuntimeReloadTransaction {
    fn application_error(error: &crate::CameraError) -> ConfigurationApplicationError {
        ConfigurationApplicationError::new(error.code().as_str(), error.to_string())
    }

    async fn restore(&mut self) -> ConfigurationApplicationResult<()> {
        let Some(checkpoint) = self.checkpoint.take() else {
            return Ok(());
        };
        self.runtime
            .restore_reload_checkpoint(checkpoint)
            .await
            .map_err(|error| Self::application_error(&error))
    }
}

#[async_trait]
impl PreparedConfigurationApply for RuntimeReloadTransaction {
    async fn commit(&mut self) -> ConfigurationApplicationResult<()> {
        match self
            .runtime
            .apply_reloaded_config(
                self.replacement.clone(),
                self.apps.clone(),
                self.events.clone(),
            )
            .await
        {
            Ok(_) => {
                // A successful adapter transition is now waiting only for Core's infallible
                // ArcSwap store. Rollback is no longer permitted after this point.
                self.checkpoint = None;
                Ok(())
            }
            Err(error) => {
                let application_error = Self::application_error(&error);
                if let Err(rollback_error) = self.restore().await {
                    return Err(ConfigurationApplicationError::new(
                        "CONFIG_APPLICATION_ROLLBACK_FAILED",
                        format!(
                            "candidate transition failed [{}]; prior runtime restoration failed [{}]: {}",
                            application_error.code, rollback_error.code, rollback_error.message
                        ),
                    ));
                }
                Err(application_error)
            }
        }
    }

    async fn rollback(&mut self) -> ConfigurationApplicationResult<()> {
        self.restore().await
    }
}

/// Bridges Core's prepared configuration-application coordinator to the runtime transaction.
///
/// Candidate validation remains side-effect-free in [`validate_configuration_candidate`]. The
/// pre-commit hook constructs candidate-scoped facades, validates the complete runtime plan, and
/// captures the prior in-memory generation. Core invokes the returned transaction while retaining
/// its prior snapshot, so an unsuccessful live transition cannot expose a mixed generation.
pub struct RuntimeConfigListener {
    runtime: Weak<CameraRuntime>,
    app_factory: Arc<AppFacadeFactory>,
    events_factory: Arc<EventsFacadeFactory>,
}

impl RuntimeConfigListener {
    /// Creates a listener whose factories obtain candidate-scoped facades for cameras added by a
    /// later reload.
    #[must_use]
    pub fn new(
        runtime: Weak<CameraRuntime>,
        app_factory: Arc<AppFacadeFactory>,
        events_factory: Arc<EventsFacadeFactory>,
    ) -> Self {
        Self {
            runtime,
            app_factory,
            events_factory,
        }
    }
}

#[async_trait]
impl ConfigurationApplyListener for RuntimeConfigListener {
    async fn prepare_configuration_apply(
        &self,
        config: Arc<Config>,
    ) -> ConfigurationApplicationResult<Box<dyn PreparedConfigurationApply>> {
        let Some(runtime) = self.runtime.upgrade() else {
            return Err(ConfigurationApplicationError::new(
                "CONFIG_APPLICATION_UNAVAILABLE",
                "camera runtime is no longer available",
            ));
        };
        let replacement = match AdapterConfig::from_core_reload(&config) {
            Ok(config) => config,
            Err(error) => {
                tracing::error!(error = %error, "accepted core configuration was invalid for camera runtime");
                return Err(ConfigurationApplicationError::new(
                    error.code().as_str(),
                    error.to_string(),
                ));
            }
        };
        let mut apps = BTreeMap::new();
        let mut events = BTreeMap::new();
        for camera in &replacement.instances {
            match (self.app_factory)(&camera.id, Arc::clone(&config)) {
                Ok(app) => {
                    apps.insert(camera.id.clone(), app);
                }
                Err(error) => {
                    tracing::error!(instance = %camera.id, error = %error, "could not construct application facade for reloaded camera");
                    return Err(ConfigurationApplicationError::new(
                        "CONFIG_APPLICATION_PREPARE_FAILED",
                        error.to_string(),
                    ));
                }
            }
            match (self.events_factory)(&camera.id, Arc::clone(&config)) {
                Ok(event) => {
                    events.insert(camera.id.clone(), event);
                }
                Err(error) => {
                    tracing::error!(instance = %camera.id, error = %error, "could not construct events facade for reloaded camera");
                    return Err(ConfigurationApplicationError::new(
                        "CONFIG_APPLICATION_PREPARE_FAILED",
                        error.to_string(),
                    ));
                }
            }
        }
        if let Err(error) = runtime.preflight_reloaded_config(&replacement, &apps, &events) {
            tracing::error!(error = %error, "camera runtime rejected configuration candidate during non-destructive preflight");
            return Err(ConfigurationApplicationError::new(
                error.code().as_str(),
                error.to_string(),
            ));
        }
        let checkpoint = runtime.reload_checkpoint().map_err(|error| {
            ConfigurationApplicationError::new(error.code().as_str(), error.to_string())
        })?;
        Ok(Box::new(RuntimeReloadTransaction {
            runtime,
            replacement,
            apps,
            events,
            checkpoint: Some(checkpoint),
        }))
    }
}

/// A pre-registered, swap-once command router.
///
/// The router is installed into the core inbox before its acknowledged subscription begins. It
/// then receives its runtime delegate exactly once. This ensures an early request can never
/// bypass durable-startup gates or observe a partially initialized map of camera actors.
pub struct RuntimeCommandRouter {
    service: RwLock<Option<Arc<dyn CameraCommandService>>>,
    stopping: AtomicBool,
}

impl RuntimeCommandRouter {
    /// Creates an empty router that rejects work until a complete runtime is installed.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            service: RwLock::new(None),
            stopping: AtomicBool::new(false),
        })
    }

    /// Registers every required adapter verb before the core subscribes to the command filter.
    ///
    /// A registration failure is fatal to component construction; a partial command surface is
    /// never exposed as active.
    pub fn register(self: &Arc<Self>, inbox: &CommandInbox) -> edgecommons::Result<()> {
        for verb in CAMERA_COMMAND_VERBS {
            let router = Arc::clone(self);
            inbox.register_outcome(
                verb,
                outcome_handler(move |request, deferred| {
                    let router = Arc::clone(&router);
                    async move { router.dispatch(verb, request, deferred).await }
                }),
            )?;
        }
        Ok(())
    }

    /// Makes one fully initialized runtime visible to the already-active command plane.
    ///
    /// Replacing a live delegate would make a reload command race the old/new durable state;
    /// reload is coordinated inside the delegate instead.
    pub fn install(&self, service: Arc<dyn CameraCommandService>) -> Result<()> {
        let mut slot = self.service.write().map_err(|_| {
            crate::CameraError::Catalog("command router lock is unavailable".to_string())
        })?;
        if slot.is_some() {
            return Err(crate::CameraError::Catalog(
                "camera command runtime was installed more than once".to_string(),
            ));
        }
        if self.stopping.load(Ordering::Acquire) {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::ComponentStopping,
                "component shutdown began before command runtime installation",
            ));
        }
        *slot = Some(service);
        Ok(())
    }

    /// Permanently stops new command delegation before runtime shutdown starts.
    pub fn begin_shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
    }

    async fn dispatch(
        &self,
        verb: &'static str,
        request: Message,
        deferred: DeferredReplyRegistry,
    ) -> CommandOutcome {
        if self.stopping.load(Ordering::Acquire) {
            return CommandOutcome::ImmediateError(CommandError::new(
                "COMPONENT_STOPPING",
                "the camera adapter is shutting down",
            ));
        }
        let service = match self.service.read() {
            Ok(slot) => slot.clone(),
            Err(_) => {
                return CommandOutcome::ImmediateError(CommandError::new(
                    "BACKEND_ERROR",
                    "the camera command router is unavailable",
                ));
            }
        };
        match service {
            Some(service) => service.handle_camera_command(verb, request, deferred).await,
            None => CommandOutcome::ImmediateError(CommandError::new(
                "CAMERA_UNAVAILABLE",
                "the camera adapter is still starting",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;

    use super::*;
    use crate::jobs::CaptureDispatcher;

    struct CountingService(AtomicUsize);

    #[async_trait]
    impl CameraCommandService for CountingService {
        async fn handle_camera_command(
            &self,
            verb: &'static str,
            _request: Message,
            _deferred: DeferredReplyRegistry,
        ) -> CommandOutcome {
            self.0.fetch_add(1, Ordering::AcqRel);
            CommandOutcome::ImmediateSuccess(Some(json!({ "verb": verb })))
        }
    }

    // Full inbox activation is covered by the core contract suite. This focused test locks the
    // router's startup/shutdown hand-off without needing a broker-backed Message fixture.
    #[test]
    fn install_is_single_assignment_and_shutdown_is_latched() {
        let router = RuntimeCommandRouter::new();
        router
            .install(Arc::new(CountingService(AtomicUsize::new(0))))
            .unwrap();
        assert!(
            router
                .install(Arc::new(CountingService(AtomicUsize::new(0))))
                .is_err()
        );
        router.begin_shutdown();
        assert!(
            router
                .install(Arc::new(CountingService(AtomicUsize::new(0))))
                .is_err()
        );
    }

    #[test]
    fn command_errors_keep_the_adapter_catalog_code_and_bound_the_detail() {
        let error = crate::CameraError::rejected(
            crate::ErrorCode::QueueFull,
            format!("queue unavailable\n{}", "x".repeat(512)),
        );
        let command = command_error(&error);
        assert_eq!(command.code, "QUEUE_FULL");
        assert!(!command.message.contains('\n'));
        assert!(command.message.len() <= 256);
    }

    #[test]
    fn retained_snapshot_cursor_is_opaque_query_bound_and_stable() {
        let cursors = CursorStore::default();
        let query = json!({ "includeCapabilities": true, "includeUnconfigured": false });
        let values = vec![json!({ "instance": "a" }), json!({ "instance": "b" })];
        let (first, cursor, _) = cursors
            .snapshot_page("list", &query, None, Some(values), None, 1)
            .unwrap();
        assert_eq!(first, vec![json!({ "instance": "a" })]);
        let cursor = cursor.expect("a second page must retain a cursor");
        assert!(cursor.starts_with("cur_"));

        let (second, final_cursor, _) = cursors
            .snapshot_page("list", &query, Some(&cursor), None, None, 1)
            .unwrap();
        assert_eq!(second, vec![json!({ "instance": "b" })]);
        assert!(final_cursor.is_none());

        let changed_query = json!({ "includeCapabilities": false, "includeUnconfigured": false });
        assert_eq!(
            cursors
                .snapshot_page("list", &changed_query, Some(&cursor), None, None, 1)
                .unwrap_err()
                .code(),
            crate::ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn list_cursor_retains_configured_and_unconfigured_pages_together() {
        let cursors = CursorStore::default();
        let query = json!({ "includeCapabilities": false, "includeUnconfigured": true });
        let (cameras, unconfigured, next) = cursors
            .list_page(
                &query,
                None,
                Some((
                    vec![json!({ "instance": "camera-a" }), json!({ "instance": "camera-b" })],
                    vec![json!({ "backend": "onvif-rtsp", "selector": { "endpointReference": "urn:camera:new" } })],
                )),
                2,
            )
            .unwrap();
        assert_eq!(cameras.len(), 2);
        assert!(unconfigured.is_empty());
        let next = next.expect("unconfigured observation must remain in the same snapshot");

        let (cameras, unconfigured, final_cursor) =
            cursors.list_page(&query, Some(&next), None, 2).unwrap();
        assert!(cameras.is_empty());
        assert_eq!(unconfigured.len(), 1);
        assert!(final_cursor.is_none());
        assert!(
            cursors
                .list_page(
                    &json!({ "includeCapabilities": false, "includeUnconfigured": false }),
                    Some(&next),
                    None,
                    2,
                )
                .is_err()
        );
    }

    #[test]
    fn retained_discovery_hides_only_matching_stable_configured_selectors() {
        let camera = crate::config::CameraConfig {
            id: "camera-a".to_string(),
            enabled: true,
            resource_group: None,
            backend: crate::config::BackendConfig::GenicamAravis(
                crate::config::GenicamBackendConfig {
                    selector: crate::config::GenicamSelector {
                        serial: Some("SN-42".to_string()),
                        mac: None,
                        device_id: None,
                        ip: None,
                    },
                    transport: crate::config::GenicamTransport::Auto,
                    interface: None,
                    packet_size: None,
                    packet_delay_ns: None,
                    buffer_count: None,
                    feature_overrides: BTreeMap::new(),
                },
            ),
            default_capture_profile: "main".to_string(),
            capture_profiles: BTreeMap::new(),
            schedules: Vec::new(),
            ptz: crate::config::PtzConfig::default(),
        };
        let matching = DiscoveryCandidate {
            backend: crate::model::BackendKind::GenicamAravis,
            selector: json!({ "serial": "SN-42" }),
            vendor: None,
            model: None,
            capabilities: json!({}),
        };
        let other = DiscoveryCandidate {
            selector: json!({ "serial": "SN-43" }),
            ..matching.clone()
        };
        assert!(candidate_is_configured(
            &matching,
            std::slice::from_ref(&camera)
        ));
        assert!(!candidate_is_configured(&other, &[camera]));
    }

    #[test]
    fn retained_cursor_expiry_and_kind_confusion_fail_closed() {
        let cursors = CursorStore::default();
        let query = json!({ "instance": "camera-a" });
        let (_, cursor, _) = cursors
            .snapshot_page(
                "ptz-presets",
                &query,
                None,
                Some(vec![json!({ "token": "one" }), json!({ "token": "two" })]),
                None,
                1,
            )
            .unwrap();
        let cursor = cursor.expect("a second page must retain a cursor");
        assert!(
            cursors
                .snapshot_page("list", &query, Some(&cursor), None, None, 1)
                .is_err()
        );
        {
            let mut entries = cursors.lock().unwrap();
            let entry = entries.entries.get_mut(&cursor).unwrap();
            entry.expires_at = std::time::Instant::now() - Duration::from_secs(1);
        }
        assert!(
            cursors
                .snapshot_page("ptz-presets", &query, Some(&cursor), None, None, 1)
                .is_err()
        );
    }

    #[test]
    fn job_cursor_binds_filters_and_preserves_typed_tuple() {
        let cursors = CursorStore::default();
        let query = json!({ "instance": "camera-a", "states": ["FAILED"] });
        let cursor = cursors
            .next_job_cursor(&query, (1234, "cap_1".to_string()))
            .unwrap();
        assert_eq!(
            cursors.job_before(&query, Some(&cursor)).unwrap(),
            Some((1234, "cap_1".to_string()))
        );
        assert!(
            cursors
                .job_before(
                    &json!({ "instance": "camera-b", "states": ["FAILED"] }),
                    Some(&cursor)
                )
                .is_err()
        );
    }

    #[test]
    fn delayed_outbox_alarm_transitions_once_and_keeps_context_bounded() {
        let mut state = OutboxAlarmState::default();
        let delayed = OutboxPressure {
            pending: 101,
            oldest_age_ms: 61_000,
            max_attempts: 4,
            delayed: true,
            escalated: false,
            last_error: Some("transport confirmation failed or remained ambiguous".to_string()),
        };
        let Some(OutboxAlarmTransition::Raise(context)) = state.transition(&delayed) else {
            panic!("a delayed outbox must raise exactly one alarm");
        };
        assert_eq!(context["pending"], 101);
        assert_eq!(context["oldestAgeMs"], 61_000);
        assert_eq!(context["maxAttempts"], 4);
        assert_eq!(
            context["lastError"],
            "transport confirmation failed or remained ambiguous"
        );
        assert!(state.transition(&delayed).is_none());

        let cleared = OutboxPressure::default();
        let Some(OutboxAlarmTransition::Clear(context)) = state.transition(&cleared) else {
            panic!("a recovered outbox must clear its stateful alarm");
        };
        assert_eq!(
            context,
            json!({
                "pending": 0,
                "oldestAgeMs": 0,
                "maxAttempts": 0,
            })
        );
        assert!(state.transition(&cleared).is_none());
    }

    #[test]
    fn storage_low_alarm_is_deduplicated_and_clears_only_after_every_root_recovers() {
        let healthy = StoragePressureSnapshot {
            output: RootPressure {
                root: PathBuf::from("/configured/output"),
                free_bytes: Some(900),
                free_percent: Some(90),
                pressured: false,
                readable: true,
            },
            state: RootPressure {
                root: PathBuf::from("/configured/state"),
                free_bytes: Some(900),
                free_percent: Some(90),
                pressured: false,
                readable: true,
            },
        };
        let mut alarms = StorageAlarmState::default();
        assert!(alarms.transition(&healthy).is_none());

        let pressured_output = StoragePressureSnapshot {
            output: RootPressure {
                root: PathBuf::from("/configured/output"),
                free_bytes: Some(99),
                free_percent: Some(9),
                pressured: true,
                readable: true,
            },
            state: RootPressure {
                root: PathBuf::from("/configured/state"),
                free_bytes: Some(900),
                free_percent: Some(90),
                pressured: false,
                readable: true,
            },
        };
        let Some(StorageAlarmTransition::Raise(context)) = alarms.transition(&pressured_output)
        else {
            panic!("a configured output floor violation must raise storage-low");
        };
        assert_eq!(context.root, "/configured/output");
        assert_eq!(context.free_bytes, Some(99));
        assert_eq!(context.free_percent, Some(9));
        assert!(alarms.transition(&pressured_output).is_none());

        let pressured_state = StoragePressureSnapshot {
            output: pressured_output.output.clone(),
            state: RootPressure {
                root: PathBuf::from("/configured/state"),
                free_bytes: Some(50),
                free_percent: Some(5),
                pressured: true,
                readable: true,
            },
        };
        let Some(StorageAlarmTransition::Raise(context)) = alarms.transition(&pressured_state)
        else {
            panic!("a new pressured root must replace the active storage-low context");
        };
        assert_eq!(context.root, "/configured/state");
        assert_eq!(context.free_bytes, Some(50));

        let recovered = StoragePressureSnapshot {
            output: RootPressure {
                free_bytes: Some(900),
                free_percent: Some(90),
                pressured: false,
                ..pressured_output.output.clone()
            },
            state: RootPressure {
                free_bytes: Some(900),
                free_percent: Some(90),
                pressured: false,
                ..pressured_state.state
            },
        };
        let Some(StorageAlarmTransition::Clear(context)) = alarms.transition(&recovered) else {
            panic!("the stateful storage-low condition must clear after both roots recover");
        };
        assert_eq!(context.root, "/configured/state");
        assert!(alarms.transition(&recovered).is_none());
    }

    #[test]
    fn capture_lifecycle_labels_cover_every_supported_trigger_and_mode() {
        assert_eq!(
            capture_trigger_type(&crate::messages::CaptureTrigger::Command {
                request_id: "request".to_string(),
            }),
            "command"
        );
        assert_eq!(
            capture_trigger_type(&crate::messages::CaptureTrigger::GroupCommand {
                request_id: "request".to_string(),
                capture_group_id: "group".to_string(),
            }),
            "group-command"
        );
        assert_eq!(
            capture_trigger_type(&crate::messages::CaptureTrigger::Schedule {
                schedule_id: "nightly".to_string(),
                intended_fire_time: chrono::Utc::now(),
            }),
            "schedule"
        );
        assert_eq!(
            capture_mode_type(crate::model::CaptureMode::Simulated),
            "simulated"
        );
        assert_eq!(
            capture_mode_type(crate::model::CaptureMode::SoftwareTrigger),
            "software-trigger"
        );
        assert_eq!(
            capture_mode_type(crate::model::CaptureMode::SnapshotUri),
            "snapshot-uri"
        );
        assert_eq!(
            capture_mode_type(crate::model::CaptureMode::RtspFrame),
            "rtsp-frame"
        );
    }

    #[test]
    fn outbox_recovery_cannot_make_readiness_true_before_startup_or_after_shutdown() {
        let updates = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&updates);
        let readiness = RuntimeReadiness::new(Arc::new(move |ready| {
            recorded.lock().unwrap().push(ready);
        }));

        readiness.set_outbox_available(false);
        readiness.set_outbox_available(true);
        assert_eq!(*updates.lock().unwrap(), vec![false, false]);

        readiness.complete_startup();
        readiness.begin_shutdown();
        readiness.set_outbox_available(false);
        readiness.set_outbox_available(true);
        assert_eq!(
            *updates.lock().unwrap(),
            vec![false, false, true, false, false, false]
        );
    }

    #[test]
    fn catalog_and_outbox_durability_are_independent_readiness_gates() {
        let updates = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&updates);
        let readiness = RuntimeReadiness::new(Arc::new(move |ready| {
            recorded.lock().unwrap().push(ready);
        }));

        readiness.complete_startup();
        readiness.set_catalog_available(false);
        readiness.set_outbox_available(false);
        readiness.set_catalog_available(true);
        readiness.set_outbox_available(true);

        assert_eq!(
            *updates.lock().unwrap(),
            vec![true, false, false, false, true]
        );
    }

    #[test]
    fn readiness_publication_is_linearizable_across_concurrent_availability_changes() {
        use std::sync::mpsc;

        let updates = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&updates);
        let (true_entered_tx, true_entered_rx) = mpsc::channel();
        let (release_true_tx, release_true_rx) = mpsc::channel();
        let release_true_rx = Arc::new(Mutex::new(release_true_rx));
        let (false_published_tx, false_published_rx) = mpsc::channel();
        let block_next_true = Arc::new(AtomicBool::new(false));
        let observe_false = Arc::new(AtomicBool::new(false));
        let readiness = RuntimeReadiness::new(Arc::new({
            let block_next_true = Arc::clone(&block_next_true);
            let observe_false = Arc::clone(&observe_false);
            move |ready| {
                if ready && block_next_true.swap(false, Ordering::AcqRel) {
                    true_entered_tx
                        .send(())
                        .expect("the test must await the blocked ready publication");
                    release_true_rx
                        .lock()
                        .unwrap()
                        .recv()
                        .expect("the test must release the blocked ready publication");
                }
                if !ready && observe_false.load(Ordering::Acquire) {
                    let _ = false_published_tx.send(());
                }
                recorded.lock().unwrap().push(ready);
            }
        }));

        readiness.set_catalog_available(false);
        readiness.complete_startup();
        updates.lock().unwrap().clear();
        block_next_true.store(true, Ordering::Release);
        observe_false.store(true, Ordering::Release);

        let recovering = readiness.clone();
        let recovering_thread = std::thread::spawn(move || {
            recovering.set_catalog_available(true);
        });
        true_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("ready publication did not enter its controlled delay");

        let becoming_unavailable = readiness.clone();
        let unavailable_thread = std::thread::spawn(move || {
            becoming_unavailable.set_outbox_available(false);
        });
        assert!(
            false_published_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "a newer unavailable transition must not publish before the older ready callback completes"
        );

        release_true_tx
            .send(())
            .expect("the ready publication must still be waiting for release");
        recovering_thread
            .join()
            .expect("the recovering readiness transition must finish");
        unavailable_thread
            .join()
            .expect("the unavailable readiness transition must finish");
        false_published_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the unavailable transition must publish after the ready callback");
        assert_eq!(*updates.lock().unwrap(), vec![true, false]);
    }

    fn reload_config(root_directory: &str) -> serde_json::Value {
        json!({
            "component": {
                "global": { "output": { "rootDirectory": root_directory } },
                "instances": [{
                    "id": "camera-a",
                    "backend": { "type": "sim" },
                    "defaultCaptureProfile": "main",
                    "captureProfiles": { "main": { "output": { "encoding": "jpeg" } } }
                }]
            }
        })
    }

    #[test]
    fn reload_validator_rejects_restart_only_output_root_without_touching_runtime() {
        let current = reload_config("C:/captures-a");
        let candidate = reload_config("C:/captures-b");
        let result = validate_configuration_candidate(
            candidate,
            Some(current),
            ConfigurationValidationPhase::Reload,
        )
        .unwrap();
        assert!(matches!(
            result,
            ConfigurationValidationResult::Reject { code, .. } if code == "CAMERA_CONFIG_INVALID"
        ));
    }

    #[test]
    fn reload_validator_accepts_schedule_only_generation() {
        let current = reload_config("C:/captures-a");
        let mut candidate = current.clone();
        candidate["component"]["instances"][0]["schedules"] = json!([{
            "id": "minute",
            "cron": "0 * * * * *",
            "timezone": "UTC",
            "captureProfile": "main"
        }]);
        assert_eq!(
            validate_configuration_candidate(
                candidate,
                Some(current),
                ConfigurationValidationPhase::Reload,
            )
            .unwrap(),
            ConfigurationValidationResult::Accept
        );
    }

    #[test]
    fn reload_validator_vetoes_onvif_secret_when_startup_has_no_credential_service() {
        let current = reload_config("C:/captures-a");
        let mut candidate = current.clone();
        candidate["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "deviceServiceUrl": "https://camera.example.test/onvif/device_service",
            "credentials": { "$secret": "camera/login" },
            "mediaProfile": "primary"
        });

        let result = validate_configuration_candidate_with_credentials(
            candidate,
            Some(current),
            ConfigurationValidationPhase::Reload,
            false,
        )
        .unwrap();
        assert!(matches!(
            result,
            ConfigurationValidationResult::Reject { code, .. } if code == "CAMERA_CONFIG_INVALID"
        ));
    }

    #[tokio::test]
    async fn runtime_config_listener_rejects_when_its_runtime_is_gone_before_factory_use() {
        let listener = RuntimeConfigListener::new(
            Weak::new(),
            Arc::new(
                |_instance, _config| -> edgecommons::Result<Arc<AppFacade>> {
                    unreachable!("unavailable runtimes must not construct application facades")
                },
            ),
            Arc::new(|_instance, _config| -> edgecommons::Result<EventsFacade> {
                unreachable!("unavailable runtimes must not construct event facades")
            }),
        );
        let candidate = Arc::new(
            Config::from_value(COMPONENT_NAME, "gw-01", reload_config("C:/captures-a"))
                .expect("the core fixture must be structurally valid"),
        );
        let error = match listener.prepare_configuration_apply(candidate).await {
            Ok(_) => panic!("a dropped runtime must veto the candidate before preparation"),
            Err(error) => error,
        };
        assert_eq!(error.code, "CONFIG_APPLICATION_UNAVAILABLE");
    }

    #[cfg(test)]
    mod simulator_runtime {
        use std::{
            path::Path,
            sync::{Arc, Mutex},
        };

        use edgecommons::{messaging::MessageBuilder, prelude::EdgeCommonsBuilder};
        use tempfile::TempDir;

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        use serde::Serialize;
        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        use serde_json::Value;
        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        use std::{fs, path::PathBuf, time::Instant};

        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
        };

        use super::*;

        type RecordedMqttPublishes = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

        /// Records what the component emits, so a test can assert that it emits anything at all.
        ///
        /// The camera adapter shipped with zero call sites for the metric subsystem, so the useful
        /// assertion is not "the numbers are right" but "the wiring exists" -- a metric nobody emits
        /// is indistinguishable from a metric that does not exist.
        #[derive(Default)]
        struct RecordingMetrics {
            defined: Mutex<Vec<String>>,
            emitted: Mutex<Vec<(String, std::collections::HashMap<String, f64>)>>,
        }

        impl RecordingMetrics {
            fn counts(&self, metric: &str, measure: &str) -> f64 {
                self.emitted
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(name, _)| name == metric)
                    .filter_map(|(_, values)| values.get(measure))
                    .sum()
            }
        }

        #[async_trait::async_trait]
        impl edgecommons::metrics::MetricService for RecordingMetrics {
            fn define_metric(&self, metric: edgecommons::metrics::Metric) {
                self.defined
                    .lock()
                    .unwrap()
                    .push(metric.get_name().to_owned());
            }

            fn is_metric_defined(&self, name: &str) -> bool {
                self.defined.lock().unwrap().iter().any(|held| held == name)
            }

            async fn emit_metric(
                &self,
                name: &str,
                values: std::collections::HashMap<String, f64>,
            ) -> edgecommons::Result<()> {
                self.emitted.lock().unwrap().push((name.to_owned(), values));
                Ok(())
            }

            async fn emit_metric_now(
                &self,
                name: &str,
                values: std::collections::HashMap<String, f64>,
            ) -> edgecommons::Result<()> {
                self.emit_metric(name, values).await
            }

            async fn flush_metrics(&self) -> edgecommons::Result<()> {
                Ok(())
            }

            async fn shutdown(&self) {}
        }

        struct TestTerminalEncoder;

        impl crate::jobs::TerminalEnvelopeEncoder for TestTerminalEncoder {
            fn encode(
                &self,
                terminal: &crate::messages::TerminalMessage,
                created_at_ms: i64,
            ) -> Result<crate::catalog::NewOutboxMessage> {
                let message =
                    edgecommons::messaging::MessageBuilder::new(terminal.header_name(), "1.0")
                        .correlation_id(terminal.correlation_id())
                        .structured_payload(terminal.body_value()?)
                        .build();
                crate::catalog::NewOutboxMessage::from_message(
                    terminal.body().event_id.clone(),
                    "terminal",
                    format!("ecv1/test/camera-adapter/main/app/{}", terminal.channel()),
                    &message,
                    created_at_ms,
                    created_at_ms,
                )
            }
        }

        fn core_config_value(root: &Path, cameras: &[&str], schedules: bool) -> serde_json::Value {
            let root = root.to_string_lossy();
            let instances = cameras
                .iter()
                .map(|id| {
                    let mut camera = json!({
                        "id": id,
                        "backend": { "type": "sim" },
                        "defaultCaptureProfile": "main",
                        "captureProfiles": { "main": { "output": { "encoding": "jpeg" } } }
                    });
                    if schedules && *id == "camera-a" {
                        camera["schedules"] = json!([{
                            "id": "minute",
                            "cron": "0 * * * * *",
                            "timezone": "UTC",
                            "captureProfile": "main"
                        }]);
                    }
                    camera
                })
                .collect::<Vec<_>>();
            let raw = json!({
                "component": {
                    "global": { "output": { "rootDirectory": root.as_ref() } },
                    "instances": instances,
                }
            });
            raw
        }

        fn core_config(root: &Path, cameras: &[&str], schedules: bool) -> Config {
            Config::from_value(
                COMPONENT_NAME,
                "gw-01",
                core_config_value(root, cameras, schedules),
            )
            .unwrap()
        }

        fn config(root: &Path, cameras: &[&str], schedules: bool) -> AdapterConfig {
            AdapterConfig::from_core_reload(&core_config(root, cameras, schedules)).unwrap()
        }

        async fn runtime(config: AdapterConfig, directory: &TempDir) -> Arc<CameraRuntime> {
            runtime_with_storage_pressure(config, directory, None).await
        }

        /// Builds the test runtime and hands back the metric recorder wired into it.
        async fn runtime_with_metrics(
            config: AdapterConfig,
            directory: &TempDir,
        ) -> (Arc<CameraRuntime>, Arc<RecordingMetrics>) {
            let recorder = Arc::new(RecordingMetrics::default());
            let runtime = runtime_with_storage_pressure_and_metrics(
                config,
                directory,
                None,
                Arc::clone(&recorder) as Arc<dyn edgecommons::metrics::MetricService>,
            )
            .await;
            (runtime, recorder)
        }

        async fn runtime_with_storage_pressure(
            config: AdapterConfig,
            directory: &TempDir,
            storage_pressure: Option<StoragePressureMonitor>,
        ) -> Arc<CameraRuntime> {
            runtime_with_storage_pressure_and_metrics(
                config,
                directory,
                storage_pressure,
                Arc::new(RecordingMetrics::default()),
            )
            .await
        }

        async fn runtime_with_storage_pressure_and_metrics(
            config: AdapterConfig,
            directory: &TempDir,
            storage_pressure: Option<StoragePressureMonitor>,
            metrics: Arc<dyn edgecommons::metrics::MetricService>,
        ) -> Arc<CameraRuntime> {
            let state = directory.path().join("state");
            std::fs::create_dir_all(&state).unwrap();
            let storage = StorageRoot::open(&config.global.output).unwrap();
            let catalog = Catalog::open(CatalogOptions::new(state)).await.unwrap();
            let admission = AdmissionController::new(
                &config.global.limits,
                &config.global.output,
                Arc::new(FilesystemSpaceProbe::default()),
            )
            .unwrap();
            let registry = Arc::new(CameraRegistry::new(&config).unwrap());
            let waiters = Arc::new(RuntimeJobHooks::new(catalog.clone()));
            let mut engines = BTreeMap::new();
            for camera in &config.instances {
                engines.insert(
                    camera.id.clone(),
                    JobEngine::new(
                        catalog.clone(),
                        admission.clone(),
                        storage.clone(),
                        Arc::new(TestTerminalEncoder),
                        Arc::clone(&waiters) as Arc<dyn JobHooks>,
                    )
                    .with_acceptance_hook(Arc::clone(&waiters) as Arc<dyn AcceptanceHook>),
                );
            }
            let scheduler = crate::dispatch::CaptureScheduler::new(&config.global.limits).unwrap();
            let runtime = Arc::new(CameraRuntime {
                config: RwLock::new(config),
                backend_context: BackendRuntimeContext::new(
                    None,
                    &crate::config::LimitsConfig::default(),
                ),
                catalog,
                admission,
                storage,
                registry,
                engines: RwLock::new(engines),
                events: RwLock::new(BTreeMap::new()),
                outbox_events: None,
                storage_pressure,
                storage_alarm: Arc::new(Mutex::new(StorageAlarmState::default())),
                readiness: RuntimeReadiness::noop(),
                metrics: Arc::new(crate::observability::CaptureMetrics::new(metrics)),
                actors: Arc::new(RwLock::new(HashMap::new())),
                supervisor_cancellations: Arc::new(RwLock::new(HashMap::new())),
                supervisor_finished: Arc::new(RwLock::new(HashMap::new())),
                session_cancellations: Arc::new(RwLock::new(HashMap::new())),
                scheduler_cancellations: RwLock::new(HashMap::new()),
                discovery_cancellation: RwLock::new(None),
                discovery_cache: Mutex::new(DiscoveryCache::default()),
                scheduler,
                cancellation: CancellationToken::new(),
                tasks: Mutex::new(Vec::new()),
                connect_gate: Arc::new(Semaphore::new(1)),
                waiters: Arc::clone(&waiters),
                cursors: CursorStore::default(),
                reload_gate: tokio::sync::Mutex::new(()),
                reloading: AtomicBool::new(false),
                self_reference: OnceLock::new(),
            });
            let _ = runtime.self_reference.set(Arc::downgrade(&runtime));
            waiters.attach_runtime(Arc::downgrade(&runtime));
            // The fleet queue needs its consumer. Without it a capture is durably accepted, queued,
            // and then waits forever -- which is exactly what these tests would have shown.
            runtime
                .start_capture_scheduler()
                .expect("the capture scheduler must start");
            runtime
        }

        struct LowSpaceProbe;

        #[async_trait::async_trait]
        impl crate::admission::DiskSpaceProbe for LowSpaceProbe {
            async fn space(&self, _path: &std::path::Path) -> Result<crate::admission::DiskSpace> {
                Ok(crate::admission::DiskSpace {
                    available_bytes: 0,
                    total_bytes: 1_000,
                })
            }
        }

        struct ToggleSpaceProbe {
            pressured: Arc<std::sync::atomic::AtomicBool>,
        }

        #[async_trait::async_trait]
        impl crate::admission::DiskSpaceProbe for ToggleSpaceProbe {
            async fn space(&self, _path: &std::path::Path) -> Result<crate::admission::DiskSpace> {
                let available_bytes = if self.pressured.load(std::sync::atomic::Ordering::Acquire) {
                    0
                } else {
                    20_000_000_000
                };
                Ok(crate::admission::DiskSpace {
                    available_bytes,
                    total_bytes: 40_000_000_000,
                })
            }
        }

        #[tokio::test]
        async fn storage_pressure_rejects_fresh_capture_before_a_ledger_or_job_is_committed() {
            let directory = TempDir::new().unwrap();
            let configuration = config(directory.path(), &["camera-a"], false);
            let monitor = StoragePressureMonitor::new(
                configuration.global.output.root_directory.clone(),
                directory.path().join("state"),
                &configuration.global.output,
                Arc::new(LowSpaceProbe),
            );
            let runtime =
                runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;

            let error = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "storage-pressure-fresh".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "storage-pressure-test".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), crate::ErrorCode::StoragePressure);
            assert!(
                runtime
                    .catalog
                    .job_by_ledger(
                        crate::catalog::LedgerKey::new(
                            "camera-a",
                            "sb/capture-submit",
                            "storage-pressure-fresh",
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap()
                    .is_none(),
                "storage pressure must reject before creating an idempotency ledger or job"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn storage_pressure_rejects_fresh_groups_but_exact_group_retries_remain_replayable() {
            let directory = TempDir::new().unwrap();
            let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
            let pressured = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let monitor = StoragePressureMonitor::new(
                configuration.global.output.root_directory.clone(),
                directory.path().join("state"),
                &configuration.global.output,
                Arc::new(ToggleSpaceProbe {
                    pressured: Arc::clone(&pressured),
                }),
            );
            let runtime =
                runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;
            let request = GroupCaptureRequest {
                request_id: "storage-group-existing".to_string(),
                instances: vec!["camera-a".to_string(), "camera-b".to_string()],
                capture_profile: None,
                profile_overrides: BTreeMap::new(),
                timeout_ms: None,
                metadata: serde_json::Map::new(),
            };
            let accepted = runtime
                .submit_group(
                    request.clone(),
                    "storage-group-original-correlation".to_string(),
                    crate::admission::CapturePriority::Submitted,
                    None,
                )
                .await
                .unwrap();
            pressured.store(true, std::sync::atomic::Ordering::Release);

            let replay = runtime
                .submit_group(
                    request.clone(),
                    "storage-group-retry-correlation".to_string(),
                    crate::admission::CapturePriority::Submitted,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(replay.group_id, accepted.group_id);
            assert_eq!(
                replay.origin_correlation_id.as_deref(),
                Some("storage-group-original-correlation"),
                "pressure must not erase the durable result of an exact group retry"
            );

            let mut changed = request.clone();
            changed.timeout_ms = Some(30_000);
            assert_eq!(
                runtime
                    .submit_group(
                        changed,
                        "storage-group-conflict-correlation".to_string(),
                        crate::admission::CapturePriority::Submitted,
                        None,
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::IdempotencyConflict
            );
            let fresh = GroupCaptureRequest {
                request_id: "storage-group-fresh".to_string(),
                ..request
            };
            assert_eq!(
                runtime
                    .submit_group(
                        fresh,
                        "storage-group-fresh-correlation".to_string(),
                        crate::admission::CapturePriority::Submitted,
                        None,
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::StoragePressure
            );
            assert!(
                runtime
                    .catalog
                    .group_by_ledger(
                        crate::catalog::LedgerKey::new(
                            "main",
                            "sb/capture-group",
                            "storage-group-fresh",
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap()
                    .is_none(),
                "storage pressure must reject a fresh group before durable acceptance"
            );
            runtime.shutdown().await;
        }

        async fn spawn_recording_mqtt_broker() -> (u16, RecordedMqttPublishes) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let publishes = Arc::new(Mutex::new(Vec::new()));
            let recorder = Arc::clone(&publishes);
            tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            let recorder = Arc::clone(&recorder);
                            tokio::spawn(async move {
                                record_mqtt_connection(stream, recorder).await;
                            });
                        }
                        Err(_) => return,
                    }
                }
            });
            (port, publishes)
        }

        async fn record_mqtt_connection(mut stream: TcpStream, publishes: RecordedMqttPublishes) {
            while let Some((header, payload)) = read_mqtt_packet(&mut stream).await {
                match header >> 4 {
                    1 => {
                        if stream.write_all(&[0x20, 0x02, 0x00, 0x00]).await.is_err() {
                            return;
                        }
                    }
                    8 => {
                        if payload.len() < 2
                            || stream
                                .write_all(&[0x90, 0x03, payload[0], payload[1], 0x00])
                                .await
                                .is_err()
                        {
                            return;
                        }
                    }
                    3 => {
                        let Some((topic, bytes, packet_id)) =
                            mqtt_publish_payload(header, &payload)
                        else {
                            return;
                        };
                        publishes.lock().unwrap().push((topic, bytes));
                        if let Some(packet_id) = packet_id {
                            if stream
                                .write_all(&[0x40, 0x02, packet_id[0], packet_id[1]])
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                    12 => {
                        if stream.write_all(&[0xD0, 0x00]).await.is_err() {
                            return;
                        }
                    }
                    14 => return,
                    _ => {}
                }
            }
        }

        async fn read_mqtt_packet(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
            let mut header = [0_u8; 1];
            stream.read_exact(&mut header).await.ok()?;
            let mut remaining = 0_usize;
            let mut multiplier = 1_usize;
            loop {
                let mut byte = [0_u8; 1];
                stream.read_exact(&mut byte).await.ok()?;
                remaining = remaining.checked_add(usize::from(byte[0] & 0x7f) * multiplier)?;
                if byte[0] & 0x80 == 0 {
                    break;
                }
                multiplier = multiplier.checked_mul(128)?;
                if multiplier > 128_usize.pow(4) {
                    return None;
                }
            }
            let mut payload = vec![0_u8; remaining];
            stream.read_exact(&mut payload).await.ok()?;
            Some((header[0], payload))
        }

        fn mqtt_publish_payload(
            header: u8,
            payload: &[u8],
        ) -> Option<(String, Vec<u8>, Option<[u8; 2]>)> {
            let topic_length = usize::from(u16::from_be_bytes(payload.get(..2)?.try_into().ok()?));
            let topic_end = 2_usize.checked_add(topic_length)?;
            let topic = std::str::from_utf8(payload.get(2..topic_end)?)
                .ok()?
                .to_string();
            let qos = (header >> 1) & 0b11;
            let (body_start, packet_id) = if qos == 0 {
                (topic_end, None)
            } else {
                let packet_id = payload
                    .get(topic_end..topic_end.checked_add(2)?)?
                    .try_into()
                    .ok()?;
                (topic_end.checked_add(2)?, Some(packet_id))
            };
            Some((topic, payload.get(body_start..)?.to_vec(), packet_id))
        }

        #[cfg(all(feature = "standalone", feature = "onvif"))]
        async fn facade_core(directory: &TempDir, port: u16) -> Arc<edgecommons::EdgeCommons> {
            facade_core_with_router(directory, port, None).await
        }

        /// Builds the loopback core fixture, optionally configuring the adapter router before
        /// the core command inbox can subscribe. This keeps the startup race test faithful to
        /// the binary's construction order rather than registering test handlers after startup.
        async fn facade_core_with_router(
            directory: &TempDir,
            port: u16,
            router: Option<Arc<RuntimeCommandRouter>>,
        ) -> Arc<edgecommons::EdgeCommons> {
            let instances = ["camera-a", "camera-b", "camera-c"]
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            facade_core_with_router_instances(directory, port, router, &instances).await
        }

        /// Builds the loopback Core fixture with the same instance roster used by a runtime test.
        ///
        /// Core permits dynamic facade handles, but capacity validation must not use that escape
        /// hatch: the configured Core roster and the adapter roster need to agree so facade
        /// construction, command registration, and instance identity all follow the production
        /// path.
        async fn facade_core_with_router_instances(
            directory: &TempDir,
            port: u16,
            router: Option<Arc<RuntimeCommandRouter>>,
            instances: &[String],
        ) -> Arc<edgecommons::EdgeCommons> {
            let component_config = directory.path().join("facade-core-config.json");
            let messaging_config = directory.path().join("facade-core-messaging.json");
            std::fs::write(
                &component_config,
                serde_json::to_vec(&json!({
                    "component": {
                        "instances": instances.iter().map(|id| json!({ "id": id })).collect::<Vec<_>>()
                    }
                }))
                .unwrap(),
            )
            .unwrap();
            std::fs::write(
                &messaging_config,
                serde_json::to_vec(&json!({
                    "messaging": {
                        "local": {
                            "host": "127.0.0.1",
                            "port": port,
                            "clientId": format!("camera-adapter-runtime-events-{}", uuid::Uuid::now_v7())
                        }
                    }
                }))
                .unwrap(),
            )
            .unwrap();
            let builder = EdgeCommonsBuilder::new(COMPONENT_NAME)
                .args(vec![
                    "camera-adapter-runtime-events".into(),
                    "--platform".into(),
                    "HOST".into(),
                    "--transport".into(),
                    "MQTT".into(),
                    messaging_config.into_os_string(),
                    "--config".into(),
                    "FILE".into(),
                    component_config.into_os_string(),
                    "--thing".into(),
                    "camera-adapter-runtime-events".into(),
                ])
                .initial_ready(false);
            let builder = match router {
                Some(router) => builder.configure_commands(move |inbox| router.register(inbox)),
                None => builder,
            };
            Arc::new(
                builder
                    .build()
                    .await
                    .expect("in-process MQTT fixture must build EdgeCommons facades"),
            )
        }

        async fn command_deferred_registry(
            directory: &TempDir,
            port: u16,
        ) -> (Arc<edgecommons::EdgeCommons>, DeferredReplyRegistry) {
            let component_config = directory.path().join("command-e2e-config.json");
            let messaging_config = directory.path().join("command-e2e-messaging.json");
            std::fs::write(&component_config, br#"{"component":{}}"#).unwrap();
            let client_id = format!("camera-adapter-command-e2e-{}", uuid::Uuid::now_v7());
            std::fs::write(
                &messaging_config,
                serde_json::to_vec(&json!({
                    "messaging": {
                        "local": {
                            "host": "127.0.0.1",
                            "port": port,
                            "clientId": client_id
                        }
                    }
                }))
                .unwrap(),
            )
            .unwrap();
            let args = vec![
                "camera-adapter-command-e2e".into(),
                "--platform".into(),
                "HOST".into(),
                "--transport".into(),
                "MQTT".into(),
                messaging_config.into_os_string(),
                "--config".into(),
                "FILE".into(),
                component_config.into_os_string(),
                "--thing".into(),
                "camera-adapter-command-e2e".into(),
            ];
            let app = Arc::new(
                EdgeCommonsBuilder::new(COMPONENT_NAME)
                    .args(args)
                    .initial_ready(false)
                    .build()
                    .await
                    .expect("loopback EMQX command-inbox fixture must start"),
            );
            let deferred = app
                .commands()
                .expect("MQTT transport creates the command inbox")
                .deferred_replies();
            (app, deferred)
        }

        fn command_message(verb: &str, suffix: &str, body: serde_json::Value) -> Message {
            MessageBuilder::new(verb, "1.0")
                .correlation_id(format!("command-e2e-correlation-{suffix}"))
                .reply_to("camera-adapter-command-e2e/replies")
                .structured_payload(body)
                .build()
        }

        fn immediate_success(outcome: CommandOutcome) -> serde_json::Value {
            match outcome {
                CommandOutcome::ImmediateSuccess(Some(value)) => value,
                other => panic!("expected immediate command success, got {other:?}"),
            }
        }

        fn queued_job(config: &AdapterConfig, capture_id: &str) -> crate::catalog::NewJob {
            let camera = config
                .instances
                .iter()
                .find(|camera| camera.id == "camera-b")
                .unwrap();
            let profile = camera.capture_profiles.get("main").unwrap().clone();
            let profile = crate::jobs::JobProfileSnapshot {
                name: "main".to_string(),
                capture: profile,
                offline_policy: crate::config::OfflinePolicy::WaitUntilDeadline,
                maximum_frame_bytes: config.global.limits.max_frame_bytes_per_camera,
                capture_mode: crate::model::CaptureMode::Simulated,
                capture_interlock: camera.ptz.capture_interlock,
                settle_ms: camera.ptz.settle_ms,
            };
            let now = chrono::Utc::now().timestamp_millis();
            let trigger = crate::messages::CaptureTrigger::Command {
                request_id: "reload-queued".to_string(),
            };
            let canonical = json!({ "requestId": "reload-queued", "metadata": {} });
            crate::catalog::NewJob {
                capture_id: capture_id.to_string(),
                instance: "camera-b".to_string(),
                ledger_key: Some(
                    crate::catalog::LedgerKey::new("camera-b", "sb/capture", "reload-queued")
                        .unwrap(),
                ),
                request_hash: crate::idempotency::canonical_request_hash(&canonical, false)
                    .unwrap(),
                canonical_request: canonical,
                effective_profile: serde_json::to_value(profile).unwrap(),
                deadlines: crate::catalog::JobDeadlines {
                    terminal_at_ms: now + 60_000,
                    queue_at_ms: None,
                    capture_at_ms: now + 30_000,
                    encode_at_ms: now + 30_000,
                    persist_at_ms: now + 30_000,
                },
                trigger: serde_json::to_value(trigger).unwrap(),
                origin_correlation_id: Some("correlation".to_string()),
                intended_output: json!({ "relativePath": "camera-b/cap.jpg", "backend": "sim" }),
                accepted_at_ms: now,
                group_id: None,
            }
        }

        async fn wait_for_online(runtime: &CameraRuntime, instance: &str) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            loop {
                if runtime
                    .registry
                    .snapshot(instance)
                    .is_ok_and(|snapshot| snapshot.state == CameraConnectionState::Online)
                {
                    return;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "simulator supervisor did not reach ONLINE"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        async fn wait_for_terminal(
            runtime: &CameraRuntime,
            capture_id: &str,
        ) -> crate::catalog::JobRecord {
            wait_for_terminal_within(runtime, capture_id, Duration::from_secs(5)).await
        }

        async fn wait_for_terminal_within(
            runtime: &CameraRuntime,
            capture_id: &str,
            timeout: Duration,
        ) -> crate::catalog::JobRecord {
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                if let Some(record) = runtime.catalog.job(capture_id).await.unwrap() {
                    if record.state.is_terminal() {
                        return record;
                    }
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "simulator capture did not reach a terminal state"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        async fn wait_for_group_terminal(
            runtime: &CameraRuntime,
            group_id: &str,
        ) -> crate::catalog::GroupRecord {
            wait_for_group_terminal_within(runtime, group_id, Duration::from_secs(5)).await
        }

        async fn wait_for_group_terminal_within(
            runtime: &CameraRuntime,
            group_id: &str,
            timeout: Duration,
        ) -> crate::catalog::GroupRecord {
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                if let Some(record) = runtime.catalog.group(group_id).await.unwrap() {
                    if record.state.is_terminal() {
                        return record;
                    }
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "simulator capture group did not reach a terminal state"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        async fn wait_for_recorded_reply(
            publishes: &RecordedMqttPublishes,
            first_index: usize,
        ) -> Message {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let reply = publishes
                    .lock()
                    .unwrap()
                    .iter()
                    .skip(first_index)
                    .find(|(topic, _)| topic == "camera-adapter-command-e2e/replies")
                    .map(|(_, bytes)| Message::from_slice(bytes).unwrap());
                if let Some(reply) = reply {
                    return reply;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "deferred command did not publish a reply to its guarded reply topic"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        #[tokio::test]
        async fn simulator_runtime_exercises_capture_status_ptz_and_reconnect_idempotency() {
            let directory = TempDir::new().unwrap();
            let mut config = config(directory.path(), &["camera-a"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut config.instances[0].backend else {
                panic!("test fixture must use the simulator backend");
            };
            sim.capture_delay_ms = 1;
            sim.ptz.supported = true;
            config.instances[0].ptz.enabled = true;
            let runtime = runtime(config, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "runtime-e2e-capture".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "runtime-e2e-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let capture_id = match accepted {
                crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
                other => panic!("expected newly accepted capture, got {other:?}"),
            };
            let terminal = wait_for_terminal(&runtime, &capture_id).await;
            assert_eq!(terminal.state, crate::model::JobState::Succeeded);
            assert!(terminal.terminal_result.is_some());

            let status = runtime
                .jobs_status_page(&CaptureStatusRequest {
                    capture_id: None,
                    capture_group_id: None,
                    instance: Some("camera-a".to_string()),
                    request_id: None,
                    states: vec![crate::model::JobState::Succeeded],
                    limit: 1,
                    cursor: None,
                })
                .await
                .unwrap();
            assert_eq!(status["jobs"].as_array().unwrap().len(), 1);
            assert_eq!(status["jobs"][0]["captureId"], capture_id);

            let ptz: PtzCommandRequest = commands::parse_closed(json!({
                "operation": "relative",
                "instance": "camera-a",
                "requestId": "runtime-e2e-ptz",
                "translation": { "pan": 0.1, "tilt": 0.0, "zoom": 0.0 }
            }))
            .unwrap();
            let first_ptz = runtime.perform_ptz(ptz.clone()).await.unwrap();
            assert_eq!(first_ptz["state"], "COMMANDED");
            assert_eq!(runtime.perform_ptz(ptz).await.unwrap(), first_ptz);

            let first_reconnect = runtime
                .reconnect(ReconnectRequest {
                    instance: Some("camera-a".to_string()),
                    request_id: "runtime-e2e-reconnect".to_string(),
                    reason: Some("test reconnect".to_string()),
                })
                .await
                .unwrap();
            let duplicate_reconnect = runtime
                .reconnect(ReconnectRequest {
                    instance: Some("camera-a".to_string()),
                    request_id: "runtime-e2e-reconnect".to_string(),
                    reason: Some("test reconnect".to_string()),
                })
                .await
                .unwrap();
            assert_eq!(first_reconnect, duplicate_reconnect);
            wait_for_online(&runtime, "camera-a").await;
            runtime.shutdown().await;
        }

        /// A capture whose durable state has moved on is refused BEFORE it can reach a camera.
        ///
        /// This test used to prove the opposite half of the contract: it corrupted a queued capture's
        /// row (QUEUED -> ACQUIRING, simulating a crash/race) and asserted that the engine's fatal
        /// error propagated all the way out through the actor, retiring it and forcing a reconnect.
        /// That was the best the component could do when a per-camera cache pushed descriptors at an
        /// actor with nothing in between.
        ///
        /// The fleet scheduler rebases a capture onto its admission before handing it over, and only
        /// a QUEUED row may be rebased -- so a capture whose durable state has moved on never reaches
        /// the camera at all. The camera keeps serving. Tearing down a healthy session because a
        /// DURABLE row was inconsistent was never a good trade, and now it is not made.
        ///
        /// The supervisor-recovery path it used to cover is not lost: an actor that dies still retires
        /// and advances its generation, which
        /// `cancelled_actor_runs_its_safety_stop_and_session_close_within_the_grace` and the panic
        /// isolation path both exercise.
        #[tokio::test]
        async fn a_capture_whose_durable_state_moved_on_never_reaches_the_camera() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            // Keep BACKOFF observable rather than relying on a scheduling race, while preserving
            // the same bounded reconnect policy used in production.
            configuration.global.timeouts.reconnect_backoff_min_ms = 200;
            configuration.global.timeouts.reconnect_backoff_max_ms = 200;
            let runtime = runtime(configuration, &directory).await;

            // Accept and dispatch before a session exists, then simulate the durable invariant a
            // crash/race could expose: the descriptor still says QUEUED but its record advanced.
            // `CameraActor` must surface that fatal engine error to the supervisor, which must
            // retire the actor, publish BACKOFF, and create a fresh generation rather than leave
            // the persistent dispatcher wedged behind the failed session.
            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "fatal-actor-recovery".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "fatal-actor-recovery-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let capture_id = match accepted {
                crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
                other => panic!("expected newly accepted capture, got {other:?}"),
            };
            assert!(matches!(
                runtime
                    .catalog
                    .compare_and_set_state(
                        &capture_id,
                        crate::model::JobState::Queued,
                        crate::model::JobState::Acquiring,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await
                    .unwrap(),
                crate::catalog::StateCasOutcome::Changed(_)
            ));

            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            // The camera comes up and STAYS up. The scheduler refuses to rebase a row that is no
            // longer QUEUED, so the inconsistent capture is never handed to it.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < deadline {
                let snapshot = runtime.registry.snapshot("camera-a").unwrap();
                assert_ne!(
                    snapshot.state,
                    CameraConnectionState::Backoff,
                    "a camera must not be torn down because a DURABLE row was inconsistent -- the                      session was healthy and the capture was the thing that was wrong"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert_eq!(
                runtime.registry.snapshot("camera-a").unwrap().state,
                CameraConnectionState::Online,
                "the camera keeps serving"
            );

            // And the camera still works: a well-formed capture succeeds on the same session.
            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "healthy-after-the-bad-row".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "healthy-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let healthy_id = match accepted {
                crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
                other => panic!("expected a newly accepted capture, got {other:?}"),
            };
            let terminal = wait_for_terminal(&runtime, &healthy_id).await;
            assert_eq!(
                terminal.state,
                crate::model::JobState::Succeeded,
                "the session was never broken, so it must still be capturing"
            );

            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn startup_recovery_interrupts_pending_jobs_and_fences_hazardous_commands() {
            let directory = TempDir::new().unwrap();
            let configuration = config(directory.path(), &["camera-b"], false);
            let runtime = runtime(configuration.clone(), &directory).await;

            let pending = queued_job(&configuration, "cap-startup-recovery");
            runtime.catalog.accept_job(pending).await.unwrap();
            assert!(matches!(
                runtime
                    .catalog
                    .queue_job(
                        "cap-startup-recovery",
                        chrono::Utc::now().timestamp_millis()
                    )
                    .await
                    .unwrap(),
                crate::catalog::StateCasOutcome::Changed(_)
            ));

            let key = crate::catalog::LedgerKey::new(
                "camera-b",
                "sb/ptz/absolute",
                "startup-recovery-ptz",
            )
            .unwrap();
            let canonical = serde_json::json!({
                "requestId": "startup-recovery-ptz",
                "pan": 0.2,
                "tilt": -0.1,
            });
            let request_hash =
                crate::idempotency::canonical_request_hash(&canonical, false).unwrap();
            assert!(matches!(
                runtime
                    .catalog
                    .begin_command(
                        key.clone(),
                        request_hash,
                        canonical.clone(),
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await
                    .unwrap(),
                crate::catalog::BeginCommandOutcome::Started(_)
            ));

            runtime.recover_install_owned().await.unwrap();

            let recovered = runtime
                .catalog
                .job("cap-startup-recovery")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(recovered.state, crate::model::JobState::Interrupted);
            assert_eq!(
                recovered.error_code.as_deref(),
                Some(crate::ErrorCode::ProcessInterrupted.as_str())
            );
            assert!(recovered.terminal_result.is_some());
            assert!(runtime.catalog.recovery_jobs().await.unwrap().is_empty());

            let replay = runtime
                .catalog
                .begin_command(
                    key,
                    request_hash,
                    canonical,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await
                .unwrap();
            assert!(matches!(
                replay,
                crate::catalog::BeginCommandOutcome::Existing(record)
                    if record.state == crate::catalog::LedgerState::OutcomeUnknown
                        && record.reply.is_none()
            ));
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn simulator_runtime_presets_page_and_reject_reused_keys_with_changed_arguments() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            sim.ptz.supported = true;
            sim.ptz.presets_supported = true;
            configuration.instances[0].ptz.enabled = true;
            configuration.instances[0].ptz.allow_preset_mutation = true;
            let runtime = runtime(configuration, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let set: PtzPresetsRequest = commands::parse_closed(json!({
                "operation": "set",
                "instance": "camera-a",
                "requestId": "preset-set-1",
                "name": "loading-bay"
            }))
            .unwrap();
            let first = runtime.perform_presets(set.clone()).await.unwrap();
            let first_token = first["token"].as_str().unwrap().to_string();
            assert_eq!(runtime.perform_presets(set).await.unwrap(), first);
            assert_eq!(
                runtime
                    .perform_presets(
                        commands::parse_closed(json!({
                            "operation": "set",
                            "instance": "camera-a",
                            "requestId": "preset-set-1",
                            "name": "packing-line"
                        }))
                        .unwrap()
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::IdempotencyConflict,
                "the same request id must never authorize a changed preset mutation"
            );

            let ptz: PtzCommandRequest = commands::parse_closed(json!({
                "operation": "relative",
                "instance": "camera-a",
                "requestId": "ptz-relative-1",
                "translation": { "pan": 0.1, "tilt": 0.0, "zoom": 0.0 }
            }))
            .unwrap();
            assert_eq!(
                runtime.perform_ptz(ptz.clone()).await.unwrap()["state"],
                "COMMANDED"
            );
            assert_eq!(
                runtime
                    .perform_ptz(
                        commands::parse_closed(json!({
                            "operation": "relative",
                            "instance": "camera-a",
                            "requestId": "ptz-relative-1",
                            "translation": { "pan": 0.2, "tilt": 0.0, "zoom": 0.0 }
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::IdempotencyConflict,
                "the same request id must never authorize changed PTZ motion"
            );

            let second = runtime
                .perform_presets(
                    commands::parse_closed(json!({
                        "operation": "set",
                        "instance": "camera-a",
                        "requestId": "preset-set-2",
                        "name": "packing-line"
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
            let second_token = second["token"].as_str().unwrap().to_string();
            let first_page = runtime
                .perform_presets(
                    commands::parse_closed(json!({
                        "operation": "list",
                        "instance": "camera-a",
                        "limit": 1
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(first_page["presets"].as_array().unwrap().len(), 1);
            let cursor = first_page["nextCursor"].as_str().unwrap().to_string();
            let second_page = runtime
                .perform_presets(
                    commands::parse_closed(json!({
                        "operation": "list",
                        "instance": "camera-a",
                        "limit": 1,
                        "cursor": cursor
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(second_page["presets"].as_array().unwrap().len(), 1);
            assert!(second_page["nextCursor"].is_null());

            assert_eq!(
                runtime
                    .perform_presets(
                        commands::parse_closed(json!({
                            "operation": "goto",
                            "instance": "camera-a",
                            "requestId": "preset-goto-1",
                            "token": first_token
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap()["state"],
                "COMMANDED"
            );
            assert_eq!(
                runtime
                    .perform_presets(
                        commands::parse_closed(json!({
                            "operation": "remove",
                            "instance": "camera-a",
                            "requestId": "preset-remove-1",
                            "token": second_token
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap()["removed"],
                true
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn simulator_runtime_exercises_the_full_ptz_and_preset_command_matrix() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            sim.ptz.supported = true;
            sim.ptz.status_supported = true;
            sim.ptz.presets_supported = true;
            configuration.instances[0].ptz.enabled = true;
            configuration.instances[0].ptz.allow_preset_mutation = true;
            let runtime = runtime(configuration, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let absolute: PtzCommandRequest = commands::parse_closed(json!({
                "operation": "absolute",
                "instance": "camera-a",
                "requestId": "ptz-absolute-matrix",
                "position": { "pan": 0.2, "tilt": -0.1, "zoom": 0.3 },
                "speed": { "pan": 0.5, "tilt": 0.4, "zoom": 0.3 }
            }))
            .unwrap();
            let absolute_reply = runtime.perform_ptz(absolute.clone()).await.unwrap();
            assert_eq!(absolute_reply["operation"], "absolute");
            assert_eq!(absolute_reply["state"], "COMMANDED");
            assert_eq!(
                runtime.perform_ptz(absolute).await.unwrap(),
                absolute_reply,
                "an exact absolute PTZ retry must replay its retained result"
            );
            assert_eq!(
                runtime
                    .perform_ptz(
                        commands::parse_closed(json!({
                            "operation": "absolute",
                            "instance": "camera-a",
                            "requestId": "ptz-absolute-matrix",
                            "position": { "pan": 0.3, "tilt": -0.1, "zoom": 0.3 }
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::IdempotencyConflict
            );

            let relative: PtzCommandRequest = commands::parse_closed(json!({
                "operation": "relative",
                "instance": "camera-a",
                "requestId": "ptz-relative-matrix",
                "translation": { "pan": 0.1, "tilt": 0.2, "zoom": -0.1 },
                "speed": { "pan": 0.6, "tilt": 0.5, "zoom": 0.4 }
            }))
            .unwrap();
            assert_eq!(
                runtime.perform_ptz(relative).await.unwrap()["state"],
                "COMMANDED"
            );

            let continuous: PtzCommandRequest = commands::parse_closed(json!({
                "operation": "continuous",
                "instance": "camera-a",
                "requestId": "ptz-continuous-matrix",
                "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
                "timeoutMs": 100
            }))
            .unwrap();
            assert_eq!(
                runtime.perform_ptz(continuous).await.unwrap()["state"],
                "COMMANDED"
            );
            let moving = runtime
                .perform_ptz(
                    commands::parse_closed(json!({
                        "operation": "status",
                        "instance": "camera-a"
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(moving["available"], true);
            assert_eq!(moving["moving"], true);

            let stop: PtzCommandRequest = commands::parse_closed(json!({
                "operation": "stop",
                "instance": "camera-a",
                "requestId": "ptz-stop-matrix",
                "axes": ["pan", "zoom"]
            }))
            .unwrap();
            assert_eq!(
                runtime.perform_ptz(stop).await.unwrap()["state"],
                "COMMANDED"
            );
            let stopped = runtime
                .perform_ptz(
                    commands::parse_closed(json!({
                        "operation": "status",
                        "instance": "camera-a"
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(stopped["moving"], false);

            let home: PtzCommandRequest = commands::parse_closed(json!({
                "operation": "home",
                "instance": "camera-a",
                "requestId": "ptz-home-matrix"
            }))
            .unwrap();
            assert_eq!(
                runtime.perform_ptz(home).await.unwrap()["state"],
                "COMMANDED"
            );

            let set: PtzPresetsRequest = commands::parse_closed(json!({
                "operation": "set",
                "instance": "camera-a",
                "requestId": "preset-matrix-set",
                "name": "matrix-home"
            }))
            .unwrap();
            let set_reply = runtime.perform_presets(set.clone()).await.unwrap();
            let token = set_reply["token"].as_str().unwrap().to_string();
            assert_eq!(runtime.perform_presets(set).await.unwrap(), set_reply);
            assert_eq!(
                runtime
                    .perform_presets(
                        commands::parse_closed(json!({
                            "operation": "set",
                            "instance": "camera-a",
                            "requestId": "preset-matrix-set",
                            "name": "matrix-other"
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::IdempotencyConflict
            );
            assert_eq!(
                runtime
                    .perform_presets(
                        commands::parse_closed(json!({
                            "operation": "goto",
                            "instance": "camera-a",
                            "requestId": "preset-matrix-goto",
                            "token": token
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap()["state"],
                "COMMANDED"
            );
            let listed = runtime
                .perform_presets(
                    commands::parse_closed(json!({
                        "operation": "list",
                        "instance": "camera-a",
                        "limit": 10
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(listed["presets"].as_array().unwrap().len(), 1);
            let remove: PtzPresetsRequest = commands::parse_closed(json!({
                "operation": "remove",
                "instance": "camera-a",
                "requestId": "preset-matrix-remove",
                "token": token
            }))
            .unwrap();
            let removed = runtime.perform_presets(remove.clone()).await.unwrap();
            assert_eq!(removed["removed"], true);
            assert_eq!(runtime.perform_presets(remove).await.unwrap(), removed);
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn simulator_runtime_discovery_and_capture_status_use_stable_bounded_pages() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            configuration.global.discovery.enabled = true;
            let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            sim.capture_delay_ms = 1;
            let runtime = runtime(configuration, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let discovery = runtime
                .discover(DiscoverRequest {
                    backends: Vec::new(),
                    timeout_ms: 100,
                    limit: 1,
                    cursor: None,
                })
                .await
                .unwrap();
            assert!(discovery["candidates"].as_array().unwrap().is_empty());
            assert!(discovery["completedAt"].is_string());
            assert!(discovery["nextCursor"].is_null());

            for request_id in ["status-page-a", "status-page-b"] {
                let accepted = runtime
                    .submit_capture(
                        "camera-a".to_string(),
                        request_id.to_string(),
                        None,
                        None,
                        serde_json::Map::new(),
                        format!("status-page-correlation-{request_id}"),
                        "sb/capture-submit",
                        crate::admission::CapturePriority::Submitted,
                    )
                    .await
                    .unwrap();
                let capture_id = match accepted {
                    crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
                    other => panic!("expected new status-page job, got {other:?}"),
                };
                assert_eq!(
                    wait_for_terminal(&runtime, &capture_id).await.state,
                    crate::model::JobState::Succeeded
                );
            }

            let first = runtime
                .jobs_status_page(&CaptureStatusRequest {
                    capture_id: None,
                    capture_group_id: None,
                    instance: Some("camera-a".to_string()),
                    request_id: None,
                    states: vec![crate::model::JobState::Succeeded],
                    limit: 1,
                    cursor: None,
                })
                .await
                .unwrap();
            assert_eq!(first["jobs"].as_array().unwrap().len(), 1);
            let cursor = first["nextCursor"].as_str().unwrap().to_string();
            let second = runtime
                .jobs_status_page(&CaptureStatusRequest {
                    capture_id: None,
                    capture_group_id: None,
                    instance: Some("camera-a".to_string()),
                    request_id: None,
                    states: vec![crate::model::JobState::Succeeded],
                    limit: 1,
                    cursor: Some(cursor.clone()),
                })
                .await
                .unwrap();
            assert_eq!(second["jobs"].as_array().unwrap().len(), 1);
            assert!(second["nextCursor"].is_null());
            assert_eq!(
                runtime
                    .jobs_status_page(&CaptureStatusRequest {
                        capture_id: None,
                        capture_group_id: None,
                        instance: Some("camera-a".to_string()),
                        request_id: None,
                        states: vec![crate::model::JobState::Failed],
                        limit: 1,
                        cursor: Some(cursor),
                    })
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::InvalidRequest,
                "a cursor must be bound to its original state filter"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn simulator_runtime_cancel_ledger_replays_exact_results_and_conflicts_on_changes() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
            for camera in &mut configuration.instances {
                let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
                    panic!("test fixture must use the simulator backend");
                };
                sim.capture_delay_ms = 1_000;
            }
            let runtime = runtime(configuration, &directory).await;
            for instance in ["camera-a", "camera-b"] {
                runtime
                    .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
                    .unwrap();
                wait_for_online(&runtime, instance).await;
            }

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "cancel-single-target".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "cancel-single-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let capture_id = match accepted {
                crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
                other => panic!("expected newly accepted cancellation target, got {other:?}"),
            };
            let single_request = CancelRequest {
                request_id: "cancel-single-ledger".to_string(),
                capture_id: Some(capture_id.clone()),
                capture_group_id: None,
                reason: Some("operator stopped this capture".to_string()),
            };
            let single = runtime
                .cancel_capture(single_request.clone())
                .await
                .unwrap();
            assert_eq!(single["captureId"], capture_id);
            assert_eq!(single["cancelled"], true);
            assert_eq!(single["state"], "CANCELLED");
            assert!(
                single["cancellationInProgress"].is_boolean(),
                "the durable cancellation CAS may precede an acquiring backend's unwind"
            );
            assert_eq!(
                runtime
                    .cancel_capture(single_request.clone())
                    .await
                    .unwrap(),
                single,
                "an exact retry must return the stored direct-cancel result"
            );
            let mut changed_single = single_request;
            changed_single.reason = Some("different cancellation reason".to_string());
            assert_eq!(
                runtime
                    .cancel_capture(changed_single)
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::IdempotencyConflict
            );

            let group = runtime
                .submit_group(
                    GroupCaptureRequest {
                        request_id: "cancel-group-target".to_string(),
                        instances: vec!["camera-a".to_string(), "camera-b".to_string()],
                        capture_profile: None,
                        profile_overrides: BTreeMap::new(),
                        timeout_ms: None,
                        metadata: serde_json::Map::new(),
                    },
                    "cancel-group-correlation".to_string(),
                    crate::admission::CapturePriority::Submitted,
                    None,
                )
                .await
                .unwrap();
            let group_request = CancelRequest {
                request_id: "cancel-group-ledger".to_string(),
                capture_id: None,
                capture_group_id: Some(group.group_id.clone()),
                reason: Some("operator cancelled the capture group".to_string()),
            };
            let group_result = runtime.cancel_capture(group_request.clone()).await.unwrap();
            assert_eq!(group_result["captureGroupId"], group.group_id);
            assert_eq!(group_result["cancelledMembers"], 2);
            assert_eq!(group_result["unchangedMembers"], 0);
            let members = group_result["members"].as_array().unwrap();
            assert_eq!(members.len(), 2);
            for member in members {
                assert!(member["captureId"].is_string());
                assert!(member["instance"].is_string());
                assert_eq!(member["cancelled"], true);
                assert_eq!(member["state"], "CANCELLED");
                assert!(
                    member["cancellationInProgress"].is_boolean(),
                    "an acquiring backend may still be unwinding after the durable cancellation CAS"
                );
            }
            assert_eq!(
                runtime.cancel_capture(group_request.clone()).await.unwrap(),
                group_result,
                "the component-scoped group ledger must replay its exact complete result"
            );
            let canonical_group_cancel = json!({
                "requestId": "cancel-group-ledger",
                "target": { "kind": "capture-group", "captureGroupId": group.group_id },
                "reason": "operator cancelled the capture group",
            });
            assert!(matches!(
                runtime
                    .catalog
                    .begin_command(
                        crate::catalog::LedgerKey::new(
                            "main",
                            "sb/capture-cancel",
                            "cancel-group-ledger",
                        )
                        .unwrap(),
                        crate::idempotency::canonical_request_hash(&canonical_group_cancel, false)
                            .unwrap(),
                        canonical_group_cancel,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await
                    .unwrap(),
                crate::catalog::BeginCommandOutcome::Existing(record)
                    if record.state == crate::catalog::LedgerState::Succeeded
                        && record.reply.as_ref() == Some(&group_result)
            ));
            let mut changed_group = group_request;
            changed_group.reason = Some("different group cancellation reason".to_string());
            assert_eq!(
                runtime
                    .cancel_capture(changed_group)
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::IdempotencyConflict
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn command_service_dispatches_immediate_verbs_with_a_real_inbox_registry() {
            let (port, _) = spawn_recording_mqtt_broker().await;
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            // Keep acquisition active long enough for the immediate cancellation verb to win.
            sim.capture_delay_ms = 1_000;
            sim.ptz.supported = true;
            configuration.instances[0].ptz.enabled = true;
            let runtime = runtime(configuration, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;
            let (app, deferred) = command_deferred_registry(&directory, port).await;

            let list = immediate_success(
                runtime
                    .handle_camera_command(
                        "sb/list",
                        command_message("sb/list", "list", json!({"limit": 10})),
                        deferred.clone(),
                    )
                    .await,
            );
            assert_eq!(list["cameras"].as_array().unwrap().len(), 1);
            assert_eq!(list["cameras"][0]["instance"], "camera-a");

            let status = immediate_success(
                runtime
                    .handle_camera_command(
                        "sb/status",
                        command_message("sb/status", "status", json!({"instance": "camera-a"})),
                        deferred.clone(),
                    )
                    .await,
            );
            assert_eq!(status["instance"], "camera-a");
            assert_eq!(status["state"], "ONLINE");

            let accepted = immediate_success(
                runtime
                    .handle_camera_command(
                        "sb/capture-submit",
                        command_message(
                            "sb/capture-submit",
                            "capture-submit",
                            json!({"instance": "camera-a", "requestId": "command-e2e-capture"}),
                        ),
                        deferred.clone(),
                    )
                    .await,
            );
            let capture_id = accepted["captureId"]
                .as_str()
                .expect("capture-submit returns a durable capture ID")
                .to_string();
            assert_eq!(accepted["state"], "QUEUED");

            let status_before_cancel = immediate_success(
                runtime
                    .handle_camera_command(
                        "sb/capture-status",
                        command_message(
                            "sb/capture-status",
                            "capture-status-before-cancel",
                            json!({"captureId": capture_id}),
                        ),
                        deferred.clone(),
                    )
                    .await,
            );
            assert_eq!(status_before_cancel["captureId"], capture_id);

            let cancelled = immediate_success(
                runtime
                    .handle_camera_command(
                        "sb/capture-cancel",
                        command_message(
                            "sb/capture-cancel",
                            "capture-cancel",
                            json!({
                                "requestId": "command-e2e-cancel",
                                "captureId": capture_id,
                                "reason": "operator cancelled command-dispatch coverage fixture"
                            }),
                        ),
                        deferred.clone(),
                    )
                    .await,
            );
            assert_eq!(cancelled["captureId"], capture_id);
            assert_eq!(cancelled["state"], "CANCELLED");
            assert_eq!(cancelled["cancelled"], true);

            let status_after_cancel = immediate_success(
                runtime
                    .handle_camera_command(
                        "sb/capture-status",
                        command_message(
                            "sb/capture-status",
                            "capture-status-after-cancel",
                            json!({"captureId": capture_id}),
                        ),
                        deferred.clone(),
                    )
                    .await,
            );
            assert_eq!(status_after_cancel["state"], "CANCELLED");

            let ptz = immediate_success(
                runtime
                    .handle_camera_command(
                        "sb/ptz",
                        command_message(
                            "sb/ptz",
                            "ptz-status",
                            json!({"operation": "status", "instance": "camera-a"}),
                        ),
                        deferred.clone(),
                    )
                    .await,
            );
            assert_eq!(ptz["available"], true);
            assert!(ptz.get("position").is_some());

            match runtime
                .handle_camera_command(
                    "sb/list",
                    command_message("sb/list", "invalid-list", json!({"limit": 0})),
                    deferred,
                )
                .await
            {
                CommandOutcome::ImmediateError(error) => {
                    assert_eq!(error.code, crate::ErrorCode::InvalidRequest.as_str());
                }
                other => panic!("invalid request must return an immediate error, got {other:?}"),
            }

            app.commands().unwrap().stop().await;
            runtime.shutdown().await;
            drop(app);
        }

        #[tokio::test]
        async fn simulator_runtime_executes_group_capture_and_paged_group_status() {
            let directory = TempDir::new().unwrap();
            let mut config = config(directory.path(), &["camera-a", "camera-b"], false);
            for camera in &mut config.instances {
                let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
                    panic!("test fixture must use the simulator backend");
                };
                sim.capture_delay_ms = 1;
            }
            let runtime = runtime(config, &directory).await;
            for instance in ["camera-a", "camera-b"] {
                runtime
                    .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
                    .unwrap();
                wait_for_online(&runtime, instance).await;
            }

            let group = runtime
                .submit_group(
                    GroupCaptureRequest {
                        request_id: "runtime-e2e-group".to_string(),
                        instances: vec!["camera-a".to_string(), "camera-b".to_string()],
                        capture_profile: None,
                        profile_overrides: BTreeMap::new(),
                        timeout_ms: None,
                        metadata: serde_json::Map::new(),
                    },
                    "runtime-e2e-group-correlation".to_string(),
                    crate::admission::CapturePriority::Submitted,
                    None,
                )
                .await
                .unwrap();
            for member in &group.members {
                assert_eq!(
                    wait_for_terminal(&runtime, &member.capture_id).await.state,
                    crate::model::JobState::Succeeded
                );
            }
            let completed_group = runtime
                .catalog
                .group(group.group_id.clone())
                .await
                .unwrap()
                .unwrap();
            let first = runtime.group_status_page(completed_group, 1, None).unwrap();
            assert_eq!(first["members"].as_array().unwrap().len(), 1);
            let cursor = first["nextCursor"]
                .as_str()
                .expect("two-member group must page at a limit of one");
            let second = runtime
                .group_status_page(
                    runtime
                        .catalog
                        .group(group.group_id)
                        .await
                        .unwrap()
                        .unwrap(),
                    1,
                    Some(cursor),
                )
                .unwrap();
            assert_eq!(second["members"].as_array().unwrap().len(), 1);
            assert!(second["nextCursor"].is_null());
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn live_same_backend_reload_replaces_session_without_stale_generation() {
            let directory = TempDir::new().unwrap();
            let mut initial = config(directory.path(), &["camera-a"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
                panic!("test fixture must use the simulator backend");
            };
            sim.capture_delay_ms = 1;
            let runtime = runtime(initial, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;
            let previous_generation = runtime.registry.snapshot("camera-a").unwrap().generation;

            let mut replacement = config(directory.path(), &["camera-a"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            sim.seed = Some(991);
            let diff = runtime
                .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
                .await
                .unwrap();
            assert_eq!(diff.lifecycle_changed, vec!["camera-a".to_string()]);
            wait_for_online(&runtime, "camera-a").await;
            assert!(
                runtime.registry.snapshot("camera-a").unwrap().generation > previous_generation,
                "replacement supervisor must advance the lifecycle generation"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn reload_timeout_preserves_prior_generation_until_a_safe_retry() {
            let directory = TempDir::new().unwrap();
            let initial = config(directory.path(), &["camera-a"], false);
            let runtime = runtime(initial, &directory).await;
            let generation_before = runtime.registry.snapshot("camera-a").unwrap().generation;

            // Model an old supervisor that has accepted cancellation but cannot yet prove that
            // every backend/session task has exited. A zero drain budget makes the timeout
            // deterministic without depending on executor scheduling.
            let old_cancellation = CancellationToken::new();
            let old_finished = CancellationToken::new();
            runtime
                .supervisor_cancellations
                .write()
                .unwrap()
                .insert("camera-a".to_string(), old_cancellation.clone());
            runtime
                .supervisor_finished
                .write()
                .unwrap()
                .insert("camera-a".to_string(), old_finished.clone());

            let mut replacement = config(directory.path(), &["camera-a"], false);
            replacement.global.timeouts.reload_drain_timeout_ms = 0;
            let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            sim.seed = Some(991);

            let error = runtime
                .apply_reloaded_config(replacement.clone(), BTreeMap::new(), BTreeMap::new())
                .await
                .expect_err("a candidate must be vetoed while an old supervisor is unconfirmed");
            assert_eq!(error.code(), crate::ErrorCode::CameraUnavailable);
            assert!(old_cancellation.is_cancelled());
            assert!(
                runtime
                    .supervisor_cancellations
                    .read()
                    .unwrap()
                    .get("camera-a")
                    .is_some_and(CancellationToken::is_cancelled),
                "timeout must not install a replacement supervisor token"
            );
            assert_eq!(
                runtime.registry.snapshot("camera-a").unwrap().generation,
                generation_before,
                "a rejected pre-commit candidate must not advance the runtime registry"
            );
            let crate::config::BackendConfig::Sim(sim) =
                &runtime.config_snapshot().unwrap().instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            assert_eq!(
                sim.seed, None,
                "runtime configuration must remain on Core's prior generation"
            );

            // Once termination is confirmed, a retry may atomically advance the candidate and
            // only then install its replacement generation.
            old_finished.cancel();
            let diff = runtime
                .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
                .await
                .expect("a confirmed old-supervisor exit must permit retry");
            assert_eq!(diff.lifecycle_changed, vec!["camera-a".to_string()]);
            assert!(
                !runtime
                    .supervisor_cancellations
                    .read()
                    .unwrap()
                    .get("camera-a")
                    .is_some_and(CancellationToken::is_cancelled),
                "the replacement supervisor starts only after the old completion signal"
            );
            let crate::config::BackendConfig::Sim(sim) =
                &runtime.config_snapshot().unwrap().instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            assert_eq!(sim.seed, Some(991));
            runtime.shutdown().await;
        }

        #[cfg(not(feature = "genicam"))]
        #[tokio::test]
        async fn supervisor_reports_unavailable_genicam_as_backoff_without_creating_an_actor() {
            let directory = TempDir::new().unwrap();
            let mut raw = core_config_value(directory.path(), &["camera-a"], false);
            raw["component"]["instances"][0]["backend"] = json!({
                "type": "genicam-aravis",
                "selector": { "serial": "genicam-feature-gate-test" }
            });
            let configuration = AdapterConfig::from_core_reload(
                &Config::from_value(COMPONENT_NAME, "gw-01", raw)
                    .expect("core carries a valid adapter-specific GenICam shape"),
            )
            .expect("GenICam configuration itself is valid even when its native feature is absent");
            let runtime = runtime(configuration, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();

            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                let snapshot = runtime.registry.snapshot("camera-a").unwrap();
                if snapshot.state == CameraConnectionState::Backoff {
                    assert_eq!(
                        snapshot
                            .last_error
                            .as_ref()
                            .map(|error| error.code.as_str()),
                        Some(crate::ErrorCode::UnsupportedCapability.as_str())
                    );
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "a missing native GenICam feature must be surfaced as BACKOFF"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            match runtime.actor("camera-a") {
                Err(error) => assert_eq!(
                    error.code(),
                    crate::ErrorCode::CameraUnavailable,
                    "a rejected backend factory must never leave a live actor handle"
                ),
                Ok(_) => panic!("a rejected backend factory must never leave a live actor handle"),
            }
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn runtime_command_paths_reject_disabled_ptz_and_preserve_reconnect_conflicts() {
            let directory = TempDir::new().unwrap();
            let initial = config(directory.path(), &["camera-a"], false);
            let runtime = runtime(initial, &directory).await;
            let ptz: PtzCommandRequest = commands::parse_closed(json!({
                "operation": "status", "instance": "camera-a"
            }))
            .unwrap();
            assert_eq!(
                runtime.perform_ptz(ptz).await.unwrap_err().code(),
                crate::ErrorCode::PtzDisabled
            );
            runtime
                .reconnect(ReconnectRequest {
                    instance: Some("camera-a".to_string()),
                    request_id: "reconnect-conflict".to_string(),
                    reason: None,
                })
                .await
                .unwrap();
            assert_eq!(
                runtime
                    .reconnect(ReconnectRequest {
                        instance: Some("camera-a".to_string()),
                        request_id: "reconnect-conflict".to_string(),
                        reason: Some("changed".to_string()),
                    })
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::IdempotencyConflict
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn reload_schedule_only_restarts_plan_without_replacing_roster() {
            let directory = TempDir::new().unwrap();
            let initial = config(directory.path(), &["camera-a"], false);
            let runtime = runtime(initial, &directory).await;
            let replacement = config(directory.path(), &["camera-a"], true);
            let diff = runtime
                .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
                .await
                .unwrap();
            assert_eq!(diff.updated, vec!["camera-a".to_string()]);
            assert_eq!(
                runtime.config_snapshot().unwrap().instances[0]
                    .schedules
                    .len(),
                1
            );
            assert_eq!(runtime.scheduler_cancellations.read().unwrap().len(), 1);
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn reload_cancels_periodic_discovery_and_clears_retained_observations() {
            let directory = TempDir::new().unwrap();
            let mut initial = config(directory.path(), &["camera-a"], false);
            initial.global.discovery.enabled = true;
            initial.global.discovery.report_unconfigured = true;
            initial.global.discovery.interval_seconds = 5;
            let runtime = runtime(initial, &directory).await;
            runtime.start_periodic_discovery().unwrap();
            let previous = runtime
                .discovery_cancellation
                .read()
                .unwrap()
                .clone()
                .expect("enabled discovery starts a cancellable generation");
            runtime
                .discovery_cache
                .lock()
                .unwrap()
                .candidates
                .push(DiscoveryCandidate {
                    backend: crate::model::BackendKind::GenicamAravis,
                    selector: json!({ "serial": "unconfigured" }),
                    vendor: None,
                    model: None,
                    capabilities: json!({}),
                });
            assert_eq!(
                runtime
                    .unconfigured_discoveries(&runtime.config_snapshot().unwrap())
                    .unwrap()
                    .len(),
                1
            );

            let replacement = config(directory.path(), &["camera-a"], false);
            runtime
                .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
                .await
                .unwrap();
            assert!(previous.is_cancelled());
            assert!(runtime.discovery_cancellation.read().unwrap().is_none());
            assert!(
                runtime
                    .discovery_cache
                    .lock()
                    .unwrap()
                    .candidates
                    .is_empty()
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn periodic_discovery_replaces_stale_cache_and_retires_each_generation() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            configuration.global.discovery.enabled = true;
            configuration.global.discovery.report_unconfigured = true;
            configuration.global.discovery.interval_seconds = 60;
            let runtime = runtime(configuration, &directory).await;
            runtime
                .discovery_cache
                .lock()
                .unwrap()
                .candidates
                .push(DiscoveryCandidate {
                    backend: crate::model::BackendKind::GenicamAravis,
                    selector: json!({ "serial": "stale-discovery" }),
                    vendor: Some("stale".to_string()),
                    model: None,
                    capabilities: json!({}),
                });

            runtime.start_periodic_discovery().unwrap();
            let first = runtime
                .discovery_cancellation
                .read()
                .unwrap()
                .clone()
                .expect("enabled discovery must retain its cancellation generation");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                if runtime
                    .discovery_cache
                    .lock()
                    .unwrap()
                    .candidates
                    .is_empty()
                {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the simulator discovery pass did not replace stale retained observations"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            runtime.restart_periodic_discovery().unwrap();
            assert!(
                first.is_cancelled(),
                "a restart must retire the former periodic-discovery generation"
            );
            let second = runtime
                .discovery_cancellation
                .read()
                .unwrap()
                .clone()
                .expect("a restart must create one replacement discovery generation");
            assert!(
                !second.is_cancelled(),
                "the replacement discovery generation must remain active before reload"
            );

            let replacement = config(directory.path(), &["camera-a"], false);
            runtime
                .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
                .await
                .unwrap();
            assert!(
                second.is_cancelled(),
                "disabling discovery through reload must retire the active generation"
            );
            assert!(runtime.discovery_cancellation.read().unwrap().is_none());
            assert!(
                runtime
                    .discovery_cache
                    .lock()
                    .unwrap()
                    .candidates
                    .is_empty()
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn discovery_returns_a_bounded_empty_sim_snapshot_and_replays_retained_pages() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            configuration.global.discovery.enabled = true;
            let runtime = runtime(configuration, &directory).await;
            let request = DiscoverRequest {
                backends: Vec::new(),
                timeout_ms: 100,
                limit: 1,
                cursor: None,
            };

            let empty = runtime.discover(request.clone()).await.unwrap();
            assert_eq!(empty["candidates"], json!([]));
            assert!(empty["nextCursor"].is_null());
            assert!(empty["completedAt"].is_string());

            let query = json!({ "backends": request.backends });
            let (_, cursor, completed_at) = runtime
                .cursors
                .snapshot_page(
                    "discover",
                    &query,
                    None,
                    Some(vec![
                        json!({ "backend": "onvif-rtsp", "selector": { "serial": "one" } }),
                        json!({ "backend": "onvif-rtsp", "selector": { "serial": "two" } }),
                    ]),
                    Some(json!("2026-07-12T00:00:00Z")),
                    1,
                )
                .unwrap();
            let page = runtime
                .discover(DiscoverRequest { cursor, ..request })
                .await
                .unwrap();
            assert_eq!(
                page["candidates"],
                json!([{ "backend": "onvif-rtsp", "selector": { "serial": "two" } }])
            );
            assert!(page["nextCursor"].is_null());
            assert_eq!(page["completedAt"], completed_at.unwrap());
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn reload_supervisor_barrier_cancels_before_timing_out_and_then_allows_retry() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let cancellation = CancellationToken::new();
            let completion = CancellationToken::new();
            runtime
                .supervisor_cancellations
                .write()
                .unwrap()
                .insert("camera-a".to_string(), cancellation.clone());
            runtime
                .supervisor_finished
                .write()
                .unwrap()
                .insert("camera-a".to_string(), completion.clone());

            let error = runtime
                .replace_supervisors(&["camera-a".to_string()], Duration::ZERO)
                .await
                .expect_err("an unconfirmed supervisor must veto the replacement");
            assert_eq!(error.code(), crate::ErrorCode::CameraUnavailable);
            assert!(
                cancellation.is_cancelled(),
                "the old generation must be cancelled even when its drain deadline is already elapsed"
            );

            completion.cancel();
            runtime
                .replace_supervisors(&["camera-a".to_string()], Duration::ZERO)
                .await
                .expect("a confirmed generation must make the retry safe");
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn reload_removal_interrupts_queued_work_and_retains_other_camera() {
            let directory = TempDir::new().unwrap();
            let initial = config(directory.path(), &["camera-a", "camera-b"], false);
            let runtime = runtime(initial.clone(), &directory).await;
            let queued = queued_job(&initial, "cap_reload_queued");
            runtime.catalog.accept_job(queued).await.unwrap();
            runtime
                .catalog
                .queue_job("cap_reload_queued", chrono::Utc::now().timestamp_millis())
                .await
                .unwrap();

            let replacement = config(directory.path(), &["camera-a"], false);
            let diff = runtime
                .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
                .await
                .unwrap();
            assert_eq!(diff.removed, vec!["camera-b".to_string()]);
            let job = runtime
                .catalog
                .job("cap_reload_queued")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.state, crate::model::JobState::Interrupted);
            assert_eq!(job.error_code.as_deref(), Some("PROCESS_INTERRUPTED"));
            assert!(
                job.error_message
                    .as_deref()
                    .is_some_and(|message| message.contains("configuration reload"))
            );
            assert!(runtime.registry.snapshot("camera-b").is_err());
            assert_eq!(
                runtime.registry.snapshot("camera-a").unwrap().instance,
                "camera-a"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn reload_connection_change_restarts_only_the_session_and_keeps_compatible_queue() {
            let directory = TempDir::new().unwrap();
            let initial = config(directory.path(), &["camera-a", "camera-b"], false);
            let runtime = runtime(initial.clone(), &directory).await;
            runtime
                .catalog
                .accept_job(queued_job(&initial, "cap_same_backend"))
                .await
                .unwrap();
            runtime
                .catalog
                .queue_job("cap_same_backend", chrono::Utc::now().timestamp_millis())
                .await
                .unwrap();

            let mut replacement = config(directory.path(), &["camera-a", "camera-b"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[1].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            sim.seed = Some(73);
            let diff = runtime
                .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
                .await
                .unwrap();
            assert_eq!(diff.lifecycle_changed, vec!["camera-b".to_string()]);
            assert_eq!(
                runtime
                    .catalog
                    .job("cap_same_backend")
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                crate::model::JobState::Queued
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn rejected_runtime_reload_leaves_the_generation_and_roster_unchanged() {
            let directory = TempDir::new().unwrap();
            let initial = config(directory.path(), &["camera-a"], false);
            let runtime = runtime(initial, &directory).await;
            let other_root = directory.path().join("other-output");
            std::fs::create_dir_all(&other_root).unwrap();
            let replacement = config(&other_root, &["camera-a"], true);
            assert!(
                runtime
                    .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
                    .await
                    .is_err()
            );
            assert!(
                runtime.config_snapshot().unwrap().instances[0]
                    .schedules
                    .is_empty()
            );
            assert_eq!(
                runtime.registry.ids().unwrap(),
                vec!["camera-a".to_string()]
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn runtime_config_listener_rejects_invalid_candidates_and_factory_failures_without_mutation()
         {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let app_calls = Arc::new(AtomicUsize::new(0));
            let event_calls = Arc::new(AtomicUsize::new(0));
            let listener = RuntimeConfigListener::new(
                Arc::downgrade(&runtime),
                {
                    let app_calls = Arc::clone(&app_calls);
                    Arc::new(move |_instance, _config| {
                        app_calls.fetch_add(1, Ordering::AcqRel);
                        Err(edgecommons::EdgeCommonsError::Facade(
                            "controlled application facade failure".to_string(),
                        ))
                    })
                },
                {
                    let event_calls = Arc::clone(&event_calls);
                    Arc::new(move |_instance, _config| {
                        event_calls.fetch_add(1, Ordering::AcqRel);
                        Err(edgecommons::EdgeCommonsError::Facade(
                            "controlled events facade failure".to_string(),
                        ))
                    })
                },
            );

            let mut invalid_raw = core_config_value(directory.path(), &["camera-a"], false);
            invalid_raw["component"]["instances"][0]["backend"] =
                json!({ "type": "unknown-camera-protocol" });
            let invalid = Arc::new(
                Config::from_value(COMPONENT_NAME, "gw-01", invalid_raw)
                    .expect("core accepts opaque adapter-specific backend configuration"),
            );
            assert!(
                listener.prepare_configuration_apply(invalid).await.is_err(),
                "an adapter-invalid core generation must be vetoed without facade construction"
            );
            assert_eq!(app_calls.load(Ordering::Acquire), 0);
            assert_eq!(event_calls.load(Ordering::Acquire), 0);
            assert_eq!(
                runtime.registry.ids().unwrap(),
                vec!["camera-a".to_string()]
            );
            assert!(
                runtime.config_snapshot().unwrap().instances[0]
                    .capture_profiles
                    .contains_key("main")
            );

            assert!(
                listener
                    .prepare_configuration_apply(Arc::new(core_config(
                        directory.path(),
                        &["camera-a", "camera-b"],
                        false,
                    )))
                    .await
                    .is_err(),
                "a facade-factory failure must reject the generation before runtime mutation"
            );
            assert_eq!(
                app_calls.load(Ordering::Acquire),
                1,
                "the first replacement camera is the only application facade requested"
            );
            assert_eq!(
                event_calls.load(Ordering::Acquire),
                0,
                "events construction must not follow an application-factory failure"
            );
            assert_eq!(
                runtime.registry.ids().unwrap(),
                vec!["camera-a".to_string()]
            );
            assert!(runtime.events.read().unwrap().is_empty());
            runtime.shutdown().await;
        }

        #[cfg(all(feature = "standalone", feature = "onvif"))]
        #[tokio::test]
        async fn runtime_config_listener_does_not_mutate_when_event_facade_preparation_fails() {
            let directory = TempDir::new().unwrap();
            let (port, _) = spawn_recording_mqtt_broker().await;
            let core = facade_core(&directory, port).await;
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let app_calls = Arc::new(AtomicUsize::new(0));
            let event_calls = Arc::new(AtomicUsize::new(0));
            let listener = RuntimeConfigListener::new(
                Arc::downgrade(&runtime),
                {
                    let core = Arc::clone(&core);
                    let app_calls = Arc::clone(&app_calls);
                    Arc::new(move |instance, candidate| {
                        app_calls.fetch_add(1, Ordering::AcqRel);
                        Ok(Arc::new(
                            core.instance_from_config_snapshot(instance, candidate)?
                                .app(),
                        ))
                    })
                },
                {
                    let event_calls = Arc::clone(&event_calls);
                    Arc::new(move |_instance, _candidate| {
                        event_calls.fetch_add(1, Ordering::AcqRel);
                        Err(edgecommons::EdgeCommonsError::Facade(
                            "controlled event facade preparation failure".to_string(),
                        ))
                    })
                },
            );

            let error = match listener
                .prepare_configuration_apply(Arc::new(core_config(
                    directory.path(),
                    &["camera-a"],
                    false,
                )))
                .await
            {
                Ok(_) => panic!("an event facade failure must veto the candidate"),
                Err(error) => error,
            };
            assert_eq!(error.code, "CONFIG_APPLICATION_PREPARE_FAILED");
            assert_eq!(app_calls.load(Ordering::Acquire), 1);
            assert_eq!(event_calls.load(Ordering::Acquire), 1);
            assert_eq!(
                runtime.registry.ids().unwrap(),
                vec!["camera-a".to_string()]
            );
            assert!(
                runtime.events.read().unwrap().is_empty(),
                "preparation failures must not install a partial event facade map"
            );
            runtime.shutdown().await;
        }

        #[cfg(all(feature = "standalone", feature = "onvif"))]
        #[tokio::test]
        async fn scheduled_reject_skip_emits_event_and_never_accepts_a_job() {
            let directory = TempDir::new().unwrap();
            let (port, publishes) = spawn_recording_mqtt_broker().await;
            let core = facade_core(&directory, port).await;
            let mut initial = config(directory.path(), &["camera-a"], true);
            let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
                panic!("test fixture must use the simulator backend");
            };
            sim.ptz.supported = true;
            sim.ptz.status_supported = true;
            initial.instances[0].ptz.enabled = true;
            initial.instances[0]
                .capture_profiles
                .get_mut("main")
                .unwrap()
                .capture_interlock = Some(crate::config::CaptureInterlock::Reject);
            let runtime = runtime(initial, &directory).await;
            runtime.events.write().unwrap().insert(
                "camera-a".to_string(),
                core.instance("camera-a").unwrap().events(),
            );
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;
            runtime
                .actor("camera-a")
                .unwrap()
                .ptz(
                    crate::model::PtzRequest::Continuous {
                        velocity: crate::model::PtzVector {
                            pan: 0.5,
                            tilt: 0.0,
                            zoom: 0.0,
                        },
                        timeout: Duration::from_secs(2),
                    },
                    tokio::time::Instant::now() + Duration::from_secs(1),
                    &runtime.cancellation,
                )
                .await
                .unwrap();
            let occurrence = ScheduleOccurrence {
                scope: crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
                schedule_id: "minute".to_string(),
                intended_fire_time: chrono::Utc::now(),
                admit_at: chrono::Utc::now(),
                jitter: Duration::ZERO,
            };

            runtime.submit_scheduled(&occurrence).await.unwrap();

            assert!(
                runtime
                    .catalog
                    .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
                    .await
                    .unwrap()
                    .is_empty(),
                "a rejected scheduled occurrence must not create a durable capture job"
            );
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let (topic, bytes) = loop {
                if let Some(event) = publishes
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|(topic, _)| topic.ends_with("/evt/warning/schedule-skipped"))
                    .cloned()
                {
                    break event;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "scheduled reject skip did not publish an operator event"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert!(topic.ends_with("/evt/warning/schedule-skipped"));
            let event = Message::from_slice(&bytes).unwrap();
            assert_eq!(event.header.name, "evt");
            assert_eq!(event.body["severity"], "warning");
            assert_eq!(event.body["type"], "schedule-skipped");
            assert_eq!(event.body["context"]["scheduleId"], "minute");
            assert_eq!(event.body["context"]["code"], "CAMERA_MOVING");
            runtime.shutdown().await;
        }

        #[cfg(all(feature = "standalone", feature = "onvif"))]
        #[tokio::test]
        async fn capture_lifecycle_events_are_opt_in_and_use_the_event_facade() {
            let directory = TempDir::new().unwrap();
            let (port, publishes) = spawn_recording_mqtt_broker().await;
            let core = facade_core(&directory, port).await;
            let mut configuration = config(directory.path(), &["camera-a"], false);
            configuration.global.operator_events.capture_lifecycle = true;
            let runtime = runtime(configuration, &directory).await;
            runtime.events.write().unwrap().insert(
                "camera-a".to_string(),
                core.instance("camera-a").unwrap().events(),
            );
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "lifecycle-events-enabled".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "lifecycle-events-enabled-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
                panic!("the first lifecycle capture must be newly accepted");
            };
            let capture_id = record.capture_id;
            let terminal = wait_for_terminal(&runtime, &capture_id).await;
            assert_eq!(terminal.state, crate::model::JobState::Succeeded);

            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let lifecycle = loop {
                let lifecycle = publishes
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(topic, _)| {
                        topic.ends_with("/evt/debug/capture-queued")
                            || topic.ends_with("/evt/info/capture-started")
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if lifecycle.len() == 2 {
                    break lifecycle;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "enabled capture lifecycle events were not routed through the event facade"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            let queued = lifecycle
                .iter()
                .find(|(topic, _)| topic.ends_with("/evt/debug/capture-queued"))
                .expect("exactly one queued event must be published");
            let queued = Message::from_slice(&queued.1).unwrap();
            assert_eq!(queued.header.name, "evt");
            assert_eq!(queued.body["severity"], "debug");
            assert_eq!(queued.body["type"], "capture-queued");
            assert_eq!(queued.body["context"]["captureId"], capture_id);
            assert_eq!(queued.body["context"]["trigger"], "command");
            assert_eq!(queued.body["context"]["captureProfile"], "main");
            assert_eq!(queued.body["context"]["queuePosition"], 1);
            let started = lifecycle
                .iter()
                .find(|(topic, _)| topic.ends_with("/evt/info/capture-started"))
                .expect("exactly one started event must be published");
            let started = Message::from_slice(&started.1).unwrap();
            assert_eq!(started.header.name, "evt");
            assert_eq!(started.body["severity"], "info");
            assert_eq!(started.body["type"], "capture-started");
            assert_eq!(started.body["context"]["captureId"], capture_id);
            assert_eq!(started.body["context"]["trigger"], "command");
            assert_eq!(started.body["context"]["captureProfile"], "main");
            assert_eq!(started.body["context"]["captureMode"], "simulated");

            let outbox = runtime
                .catalog
                .pending_outbox(chrono::Utc::now().timestamp_millis(), 10)
                .await
                .unwrap();
            assert_eq!(outbox.len(), 1);
            assert_eq!(outbox[0].message_kind, "terminal");
            assert!(outbox[0].topic.ends_with("/app/image/captured"));
            runtime.shutdown().await;
        }

        #[cfg(all(feature = "standalone", feature = "onvif"))]
        #[tokio::test]
        async fn disabled_capture_lifecycle_events_do_not_publish_or_change_terminal_delivery() {
            let directory = TempDir::new().unwrap();
            let (port, publishes) = spawn_recording_mqtt_broker().await;
            let core = facade_core(&directory, port).await;
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            runtime.events.write().unwrap().insert(
                "camera-a".to_string(),
                core.instance("camera-a").unwrap().events(),
            );
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "lifecycle-events-disabled".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "lifecycle-events-disabled-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
                panic!("the first disabled lifecycle capture must be newly accepted");
            };
            let terminal = wait_for_terminal(&runtime, &record.capture_id).await;
            assert_eq!(terminal.state, crate::model::JobState::Succeeded);
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                publishes.lock().unwrap().iter().all(|(topic, _)| {
                    !topic.ends_with("/evt/debug/capture-queued")
                        && !topic.ends_with("/evt/info/capture-started")
                }),
                "disabled lifecycle diagnostics must not publish"
            );
            let outbox = runtime
                .catalog
                .pending_outbox(chrono::Utc::now().timestamp_millis(), 10)
                .await
                .unwrap();
            assert_eq!(outbox.len(), 1);
            assert_eq!(outbox[0].message_kind, "terminal");
            assert!(outbox[0].topic.ends_with("/app/image/captured"));
            runtime.shutdown().await;
        }

        #[cfg(all(feature = "standalone", feature = "onvif"))]
        #[tokio::test]
        async fn saturated_lifecycle_dispatcher_drops_diagnostics_without_blocking_acceptance() {
            let directory = TempDir::new().unwrap();
            let (port, publishes) = spawn_recording_mqtt_broker().await;
            let core = facade_core(&directory, port).await;
            let mut configuration = config(directory.path(), &["camera-a"], false);
            configuration.global.operator_events.capture_lifecycle = true;
            let runtime = runtime(configuration, &directory).await;
            runtime.events.write().unwrap().insert(
                "camera-a".to_string(),
                core.instance("camera-a").unwrap().events(),
            );
            let permits = Arc::clone(&runtime.waiters.lifecycle_event_slots)
                .acquire_many_owned(
                    u32::try_from(MAX_LIFECYCLE_EVENT_PUBLISHES)
                        .expect("the fixed lifecycle capacity fits Tokio's semaphore API"),
                )
                .await
                .expect("the test holds every detached lifecycle permit");

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "saturated-lifecycle-dispatch".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "saturated-lifecycle-dispatch-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .expect("diagnostic saturation must not reject durable capture acceptance");
            let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
                panic!("the fixture capture must be newly accepted");
            };
            assert_eq!(record.state, crate::model::JobState::Queued);
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                publishes.lock().unwrap().iter().all(|(topic, _)| {
                    !topic.ends_with("/evt/debug/capture-queued")
                        && !topic.ends_with("/evt/info/capture-started")
                }),
                "a saturated bounded dispatcher must drop diagnostics rather than queue task work"
            );

            drop(permits);
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                publishes.lock().unwrap().iter().all(|(topic, _)| {
                    !topic.ends_with("/evt/debug/capture-queued")
                        && !topic.ends_with("/evt/info/capture-started")
                }),
                "dropped diagnostics must not be replayed after capacity returns"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn unavailable_lifecycle_event_routing_does_not_change_terminal_capture_outcome() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            configuration.global.operator_events.capture_lifecycle = true;
            let runtime = runtime(configuration, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "lifecycle-events-unavailable".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "lifecycle-events-unavailable-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
                panic!("the first unavailable lifecycle capture must be newly accepted");
            };
            let terminal = wait_for_terminal(&runtime, &record.capture_id).await;
            assert_eq!(terminal.state, crate::model::JobState::Succeeded);
            let outbox = runtime
                .catalog
                .pending_outbox(chrono::Utc::now().timestamp_millis(), 10)
                .await
                .unwrap();
            assert_eq!(outbox.len(), 1);
            assert_eq!(outbox[0].message_kind, "terminal");
            runtime.shutdown().await;
        }

        #[cfg(all(feature = "standalone", feature = "onvif"))]
        #[tokio::test]
        async fn reload_adds_and_removes_event_facades_with_the_camera_roster() {
            let directory = TempDir::new().unwrap();
            let (port, _) = spawn_recording_mqtt_broker().await;
            let core = facade_core(&directory, port).await;
            let initial = config(directory.path(), &["camera-a"], false);
            let runtime = runtime(initial, &directory).await;
            let replacement = config(directory.path(), &["camera-a", "camera-b"], false);
            let mut apps = BTreeMap::new();
            apps.insert(
                "camera-b".to_string(),
                Arc::new(core.instance("camera-b").unwrap().app()),
            );
            let mut events = BTreeMap::new();
            events.insert(
                "camera-b".to_string(),
                core.instance("camera-b").unwrap().events(),
            );
            let diff = runtime
                .apply_reloaded_config(replacement, apps, events)
                .await
                .unwrap();
            assert_eq!(diff.added, vec!["camera-b".to_string()]);
            assert_eq!(
                runtime
                    .events
                    .read()
                    .unwrap()
                    .get("camera-b")
                    .map(EventsFacade::instance_id),
                Some("camera-b")
            );

            let removal = config(directory.path(), &["camera-a"], false);
            runtime
                .apply_reloaded_config(removal, BTreeMap::new(), BTreeMap::new())
                .await
                .unwrap();
            assert!(
                !runtime.events.read().unwrap().contains_key("camera-b"),
                "removing a camera must retire its event facade with the rest of its runtime state"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn rejected_reload_preflight_keeps_the_prior_supervisor_serving_captures() {
            let directory = TempDir::new().unwrap();
            let mut initial = config(directory.path(), &["camera-a"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
                panic!("test fixture must use the simulator backend");
            };
            sim.capture_delay_ms = 1;
            let runtime = runtime(initial, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;
            let generation_before = runtime.registry.snapshot("camera-a").unwrap().generation;
            let supervisor = runtime
                .supervisor_cancellations
                .read()
                .unwrap()
                .get("camera-a")
                .cloned()
                .expect("the live camera must have a supervisor cancellation token");

            let listener = RuntimeConfigListener::new(
                Arc::downgrade(&runtime),
                Arc::new(|_instance, _config| {
                    Err(edgecommons::EdgeCommonsError::Config(
                        "test candidate facade construction failure".to_string(),
                    ))
                }),
                Arc::new(|_instance, _config| -> edgecommons::Result<EventsFacade> {
                    unreachable!("the application facade rejection occurs first")
                }),
            );
            let candidate = Arc::new(core_config(directory.path(), &["camera-a"], false));
            assert!(
                listener
                    .prepare_configuration_apply(candidate)
                    .await
                    .is_err(),
                "a candidate that cannot build its required facades must be rejected before Core commits it"
            );
            assert!(
                !supervisor.is_cancelled(),
                "a rejected preflight must not cancel the previous service generation"
            );
            assert_eq!(
                runtime.registry.snapshot("camera-a").unwrap().generation,
                generation_before,
                "a rejected preflight must not advance the prior runtime generation"
            );

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "rejected-reload-still-serves".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "rejected-reload-still-serves-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .expect("the prior service must continue accepting captures after rejection");
            let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
                panic!("the first capture after a rejected reload must be newly accepted");
            };
            assert_eq!(
                wait_for_terminal(&runtime, &record.capture_id).await.state,
                crate::model::JobState::Succeeded,
                "the prior service must also complete captures after rejection"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn failed_reload_transition_restores_prior_config_and_capture_service() {
            let directory = TempDir::new().unwrap();
            let mut initial = config(directory.path(), &["camera-a"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
                panic!("test fixture must use the simulator backend");
            };
            sim.capture_delay_ms = 1;
            let runtime = runtime(initial, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;
            let retired_supervisor = runtime
                .supervisor_cancellations
                .read()
                .unwrap()
                .get("camera-a")
                .cloned()
                .expect("the initial runtime must have a supervisor token");

            let mut replacement = config(directory.path(), &["camera-a"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            sim.seed = Some(712);
            // Fail the commit the way it can actually fail.
            //
            // This used to be driven by a `#[cfg(test)] fail_next_reload_after_supervisor_retirement`
            // AtomicBool -- a field on the PRODUCTION `CameraRuntime` struct, read by a branch inside
            // the production reload commit. It injected a failure that cannot occur, at a point in
            // the sequence where no real failure lives, which meant the rollback being asserted here
            // had never once been driven by anything the runtime could actually do.
            //
            // The real post-retirement failure is `registry.apply_validated_config`, and it rejects
            // a roster with duplicate camera IDs. That check sits immediately after the supervisor
            // retirement barrier, which is exactly the window this test exists to cover, so a
            // duplicated instance drives the genuine path -- and the assertion below proves the
            // supervisors really were retired before it fired.
            let duplicate = replacement.instances[0].clone();
            replacement.instances.push(duplicate);
            let checkpoint = runtime.reload_checkpoint().unwrap();
            let mut transaction = RuntimeReloadTransaction {
                runtime: Arc::clone(&runtime),
                replacement,
                apps: BTreeMap::new(),
                events: BTreeMap::new(),
                checkpoint: Some(checkpoint),
            };

            assert!(
                transaction.commit().await.is_err(),
                "a roster the registry refuses must reject the candidate"
            );
            assert!(
                retired_supervisor.is_cancelled(),
                "the failure must land AFTER the retirement barrier -- otherwise this test would                  not be covering the post-retirement rollback it claims to cover"
            );
            // Core calls rollback after any failed commit. It is intentionally idempotent because
            // commit performed the restoration before returning its error.
            transaction.rollback().await.unwrap();
            wait_for_online(&runtime, "camera-a").await;
            let crate::config::BackendConfig::Sim(sim) =
                &runtime.config_snapshot().unwrap().instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            assert_eq!(
                sim.seed, None,
                "the prior runtime configuration must be restored"
            );
            assert!(
                retired_supervisor.is_cancelled(),
                "rollback must retire the failed candidate's predecessor before installing a fresh prior supervisor"
            );
            assert!(
                !runtime
                    .supervisor_cancellations
                    .read()
                    .unwrap()
                    .get("camera-a")
                    .is_some_and(CancellationToken::is_cancelled),
                "rollback must install a live prior-config supervisor rather than revive a cancelled actor"
            );

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "failed-transition-restored-service".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "failed-transition-restored-service-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .expect("the restored prior service must accept captures");
            let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
                panic!("the first capture after rollback must be newly accepted");
            };
            assert_eq!(
                wait_for_terminal(&runtime, &record.capture_id).await.state,
                crate::model::JobState::Succeeded,
                "the restored prior service must complete captures"
            );
            runtime.shutdown().await;
        }

        #[cfg(all(feature = "standalone", feature = "onvif"))]
        #[tokio::test]
        async fn runtime_config_listener_refreshes_retained_facades_and_applies_roster_and_session_changes()
         {
            let directory = TempDir::new().unwrap();
            let (port, _) = spawn_recording_mqtt_broker().await;
            let core = facade_core(&directory, port).await;
            let mut initial = config(directory.path(), &["camera-a", "camera-b"], false);
            let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
                panic!("test fixture must use the simulator backend");
            };
            sim.capture_delay_ms = 1;
            let runtime = runtime(initial, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;
            let generation_before = runtime.registry.snapshot("camera-a").unwrap().generation;

            let app_calls = Arc::new(Mutex::new(Vec::new()));
            let event_calls = Arc::new(Mutex::new(Vec::new()));
            let listener = RuntimeConfigListener::new(
                Arc::downgrade(&runtime),
                {
                    let core = Arc::clone(&core);
                    let app_calls = Arc::clone(&app_calls);
                    Arc::new(move |instance, config| {
                        app_calls.lock().unwrap().push(instance.to_string());
                        Ok(Arc::new(
                            core.instance_from_config_snapshot(instance, config)?.app(),
                        ))
                    })
                },
                {
                    let core = Arc::clone(&core);
                    let event_calls = Arc::clone(&event_calls);
                    Arc::new(move |instance, config| {
                        event_calls.lock().unwrap().push(instance.to_string());
                        Ok(core
                            .instance_from_config_snapshot(instance, config)?
                            .events())
                    })
                },
            );

            let roster_candidate = Arc::new(core_config(
                directory.path(),
                &["camera-a", "camera-c"],
                false,
            ));
            let mut roster_transaction = listener
                .prepare_configuration_apply(roster_candidate)
                .await
                .unwrap();
            roster_transaction.commit().await.unwrap();
            assert_eq!(
                runtime.registry.ids().unwrap(),
                vec!["camera-a".to_string(), "camera-c".to_string()]
            );
            assert_eq!(
                app_calls.lock().unwrap().as_slice(),
                ["camera-a", "camera-c"]
            );
            assert_eq!(
                event_calls.lock().unwrap().as_slice(),
                ["camera-a", "camera-c"]
            );
            {
                let events = runtime.events.read().unwrap();
                assert_eq!(events.len(), 2);
                assert_eq!(
                    events.get("camera-a").map(EventsFacade::instance_id),
                    Some("camera-a")
                );
                assert_eq!(
                    events.get("camera-c").map(EventsFacade::instance_id),
                    Some("camera-c")
                );
            }

            let mut session_replacement =
                core_config_value(directory.path(), &["camera-a", "camera-c"], false);
            session_replacement["component"]["instances"][0]["backend"]["seed"] = json!(919);
            let session_candidate =
                Arc::new(Config::from_value(COMPONENT_NAME, "gw-01", session_replacement).unwrap());
            let mut session_transaction = listener
                .prepare_configuration_apply(session_candidate)
                .await
                .unwrap();
            session_transaction.commit().await.unwrap();
            assert_eq!(
                app_calls.lock().unwrap().as_slice(),
                ["camera-a", "camera-c", "camera-a", "camera-c"],
                "every retained instance must receive a current core facade on every generation"
            );
            assert_eq!(
                event_calls.lock().unwrap().as_slice(),
                ["camera-a", "camera-c", "camera-a", "camera-c"],
                "retained event facades must be refreshed, not only newly added cameras"
            );
            wait_for_online(&runtime, "camera-a").await;
            assert!(
                runtime.registry.snapshot("camera-a").unwrap().generation > generation_before,
                "a same-kind backend change must replace the retained camera session"
            );
            assert!(runtime.registry.snapshot("camera-b").is_err());
            runtime.shutdown().await;
        }

        #[test]
        fn cursor_store_preserves_page_boundaries_and_rejects_cross_query_reuse() {
            let cursors = CursorStore::default();
            let list_query = json!({ "includeCapabilities": false, "includeUnconfigured": true });
            let (cameras, unconfigured, next) = cursors
                .list_page(
                    &list_query,
                    None,
                    Some((
                        vec![json!({ "instance": "camera-a" })],
                        vec![json!({ "selector": { "serial": "unconfigured" } })],
                    )),
                    1,
                )
                .unwrap();
            assert_eq!(cameras, vec![json!({ "instance": "camera-a" })]);
            assert!(unconfigured.is_empty());
            let list_cursor = next.expect("a second retained list item needs a continuation");

            let (cameras, unconfigured, next) = cursors
                .list_page(&list_query, Some(&list_cursor), None, 1)
                .unwrap();
            assert!(cameras.is_empty());
            assert_eq!(
                unconfigured,
                vec![json!({ "selector": { "serial": "unconfigured" } })]
            );
            assert!(next.is_none());
            assert_eq!(
                cursors
                    .list_page(
                        &json!({ "includeCapabilities": true, "includeUnconfigured": true }),
                        Some(&list_cursor),
                        None,
                        1,
                    )
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::InvalidRequest,
                "a retained list cursor is bound to its original capability view"
            );

            let snapshot_query = json!({ "instance": "camera-a" });
            let (values, next, completed_at) = cursors
                .snapshot_page(
                    "ptz-presets",
                    &snapshot_query,
                    None,
                    Some(vec![json!({ "token": "one" }), json!({ "token": "two" })]),
                    Some(json!("2026-07-11T00:00:00Z")),
                    1,
                )
                .unwrap();
            assert_eq!(values, vec![json!({ "token": "one" })]);
            assert_eq!(completed_at, Some(json!("2026-07-11T00:00:00Z")));
            let snapshot_cursor = next.expect("a retained snapshot needs a continuation");
            let (values, next, completed_at) = cursors
                .snapshot_page(
                    "ptz-presets",
                    &snapshot_query,
                    Some(&snapshot_cursor),
                    None,
                    None,
                    1,
                )
                .unwrap();
            assert_eq!(values, vec![json!({ "token": "two" })]);
            assert!(next.is_none());
            assert_eq!(completed_at, Some(json!("2026-07-11T00:00:00Z")));
            assert_eq!(
                cursors
                    .snapshot_page(
                        "different-kind",
                        &snapshot_query,
                        Some(&snapshot_cursor),
                        None,
                        None,
                        1,
                    )
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::InvalidRequest,
                "a retained snapshot cursor cannot be replayed under another operation"
            );

            let jobs_query = json!({ "instance": "camera-a", "states": ["SUCCEEDED"] });
            assert_eq!(cursors.job_before(&jobs_query, None).unwrap(), None);
            let jobs_cursor = cursors
                .next_job_cursor(&jobs_query, (42, "cap_stable_page_boundary".to_string()))
                .unwrap();
            assert_eq!(
                cursors.job_before(&jobs_query, Some(&jobs_cursor)).unwrap(),
                Some((42, "cap_stable_page_boundary".to_string()))
            );
            assert_eq!(
                cursors
                    .job_before(
                        &json!({ "instance": "camera-b", "states": ["SUCCEEDED"] }),
                        Some(&jobs_cursor),
                    )
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::InvalidRequest,
                "a durable status cursor is bound to its original filter"
            );
        }

        /// The fleet queue bounds the COMPONENT, not just each camera -- a bound that never existed.
        ///
        /// Queueing used to be per-camera only, so the real worst case was
        /// `cameras x 2 x maxQueuedCapturesPerCamera` -- 2,048 descriptors at the design target --
        /// with no single number capping it and nothing able to see the fleet's backlog at all.
        #[tokio::test]
        async fn the_fleet_queue_bounds_the_component_and_each_camera() {
            let directory = TempDir::new().unwrap();
            let base = config(directory.path(), &["camera-a"], false);
            let limits = crate::config::LimitsConfig {
                max_pending_captures: 3,
                max_queued_captures_per_camera: 2,
                ..base.global.limits.clone()
            };
            let scheduler = crate::dispatch::CaptureScheduler::new(&limits).unwrap();

            // The per-camera bound still holds.
            let first = CaptureDispatcher::reserve(&scheduler, "camera-a").unwrap();
            let second = CaptureDispatcher::reserve(&scheduler, "camera-a").unwrap();
            assert_eq!(
                match CaptureDispatcher::reserve(&scheduler, "camera-a") {
                    Err(error) => error.code(),
                    Ok(_) => panic!("a third capture must not exceed the per-camera bound"),
                },
                crate::ErrorCode::QueueFull,
            );

            // And now the fleet bound holds too: camera-b may queue one, then the COMPONENT is full,
            // even though camera-c has queued nothing at all.
            let third = CaptureDispatcher::reserve(&scheduler, "camera-b").unwrap();
            assert_eq!(
                match CaptureDispatcher::reserve(&scheduler, "camera-c") {
                    Err(error) => error.code(),
                    Ok(_) => panic!("the component's own backlog bound must hold"),
                },
                crate::ErrorCode::QueueFull,
                "a camera that has queued nothing must still be refused once the FLEET is full --                  that is the bound the component never had"
            );
            assert_eq!(scheduler.pending(), 3);
            assert_eq!(scheduler.pending_for("camera-a"), 2);

            // Dropping an uncommitted reservation returns its slot, to both bounds.
            drop(second);
            assert_eq!(scheduler.pending(), 2);
            assert_eq!(scheduler.pending_for("camera-a"), 1);
            assert!(CaptureDispatcher::reserve(&scheduler, "camera-c").is_ok());
            drop((first, third));
        }

        #[tokio::test]
        async fn simulator_runtime_rejects_invalid_requests_without_creating_durable_work() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
            configuration.instances[1].enabled = false;
            let runtime = runtime(configuration, &directory).await;

            assert_eq!(
                runtime
                    .submit_capture(
                        "missing-camera".to_string(),
                        "unknown-instance".to_string(),
                        None,
                        None,
                        serde_json::Map::new(),
                        "invalid-request-test".to_string(),
                        "sb/capture-submit",
                        crate::admission::CapturePriority::Submitted,
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::UnknownInstance
            );
            assert_eq!(
                runtime
                    .submit_capture(
                        "camera-a".to_string(),
                        "unknown-profile".to_string(),
                        Some("not-configured".to_string()),
                        None,
                        serde_json::Map::new(),
                        "invalid-request-test".to_string(),
                        "sb/capture-submit",
                        crate::admission::CapturePriority::Submitted,
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::UnknownCaptureProfile
            );

            let disabled_group: GroupCaptureRequest = commands::parse_closed(json!({
                "requestId": "disabled-member-group",
                "instances": ["camera-a", "camera-b"]
            }))
            .unwrap();
            assert_eq!(
                runtime
                    .submit_group(
                        disabled_group,
                        "invalid-request-test".to_string(),
                        crate::admission::CapturePriority::Submitted,
                        None,
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::CameraDisabled,
                "group acceptance must validate every member before writing a group record"
            );
            assert!(
                runtime
                    .catalog
                    .group_by_ledger(
                        crate::catalog::LedgerKey::new(
                            "main",
                            "sb/capture-group",
                            "disabled-member-group",
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap()
                    .is_none(),
                "a rejected group must not leave an idempotency row or partial durable group"
            );

            assert_eq!(
                runtime
                    .discover(DiscoverRequest {
                        backends: Vec::new(),
                        timeout_ms: 100,
                        limit: 1,
                        cursor: None,
                    })
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::UnsupportedCapability,
                "discovery respects the explicit configuration gate"
            );
            assert_eq!(
                runtime
                    .cancel_capture(CancelRequest {
                        request_id: "missing-capture-cancel".to_string(),
                        capture_id: Some("cap_missing".to_string()),
                        capture_group_id: None,
                        reason: None,
                    })
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::CaptureNotFound
            );
            assert_eq!(
                runtime
                    .reconnect(ReconnectRequest {
                        instance: None,
                        request_id: "ambiguous-reconnect".to_string(),
                        reason: None,
                    })
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::InstanceRequired,
                "multi-camera reconnect requires an explicit target"
            );
            assert_eq!(
                runtime
                    .reconnect(ReconnectRequest {
                        instance: Some("camera-b".to_string()),
                        request_id: "disabled-reconnect".to_string(),
                        reason: None,
                    })
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::CameraDisabled
            );
            assert_eq!(
                runtime
                    .perform_presets(
                        commands::parse_closed(json!({
                            "operation": "list",
                            "instance": "camera-a",
                            "limit": 1
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::PtzDisabled
            );
            assert!(
                runtime
                    .catalog
                    .jobs_page(None, Vec::new(), None, 10)
                    .await
                    .unwrap()
                    .is_empty(),
                "rejected direct requests must not allocate capture jobs"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn simulator_runtime_submission_retries_replay_existing_work_and_reject_changes() {
            let directory = TempDir::new().unwrap();
            let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
            let runtime = runtime(configuration, &directory).await;

            let metadata = serde_json::Map::from_iter([(
                "lot".to_string(),
                serde_json::Value::String("A-17".to_string()),
            )]);
            let first = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "submitted-idempotency".to_string(),
                    None,
                    Some(30_000),
                    metadata.clone(),
                    "original-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let first_capture_id = match first {
                crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
                other => panic!("first submitted capture must insert durable work, got {other:?}"),
            };
            let retry = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "submitted-idempotency".to_string(),
                    None,
                    Some(30_000),
                    metadata.clone(),
                    "retry-correlation-must-not-replace-origin".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            assert!(matches!(
                retry,
                crate::catalog::AcceptJobOutcome::Existing(record)
                    if record.capture_id == first_capture_id
                        && record.origin_correlation_id.as_deref() == Some("original-correlation")
            ));
            let mut changed_metadata = metadata;
            changed_metadata.insert(
                "lot".to_string(),
                serde_json::Value::String("B-18".to_string()),
            );
            assert!(matches!(
                runtime
                    .submit_capture(
                        "camera-a".to_string(),
                        "submitted-idempotency".to_string(),
                        None,
                        Some(30_000),
                        changed_metadata,
                        "changed-correlation".to_string(),
                        "sb/capture-submit",
                        crate::admission::CapturePriority::Submitted,
                    )
                    .await
                    .unwrap(),
                crate::catalog::AcceptJobOutcome::Conflict
            ));

            let group_request = GroupCaptureRequest {
                request_id: "group-idempotency".to_string(),
                instances: vec!["camera-a".to_string(), "camera-b".to_string()],
                capture_profile: None,
                profile_overrides: BTreeMap::new(),
                timeout_ms: Some(30_000),
                metadata: serde_json::Map::new(),
            };
            let group = runtime
                .submit_group(
                    group_request.clone(),
                    "original-group-correlation".to_string(),
                    crate::admission::CapturePriority::Submitted,
                    None,
                )
                .await
                .unwrap();
            let replay = runtime
                .submit_group(
                    group_request.clone(),
                    "retry-group-correlation".to_string(),
                    crate::admission::CapturePriority::Submitted,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(replay.group_id, group.group_id);
            assert_eq!(
                replay.origin_correlation_id.as_deref(),
                Some("original-group-correlation")
            );
            let mut changed_group = group_request;
            changed_group.timeout_ms = Some(31_000);
            assert_eq!(
                runtime
                    .submit_group(
                        changed_group,
                        "changed-group-correlation".to_string(),
                        crate::admission::CapturePriority::Submitted,
                        None,
                    )
                    .await
                    .unwrap_err()
                    .code(),
                crate::ErrorCode::IdempotencyConflict
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn simulator_runtime_scheduled_admission_terminalizes_and_deduplicates_occurrences() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], true);
            let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            sim.capture_delay_ms = 1;
            let runtime = runtime(configuration, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let intended_fire_time = chrono::Utc::now();
            let occurrence = ScheduleOccurrence {
                scope: crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
                schedule_id: "minute".to_string(),
                intended_fire_time,
                admit_at: intended_fire_time,
                jitter: Duration::ZERO,
            };
            runtime.submit_scheduled(&occurrence).await.unwrap();
            let jobs = runtime
                .catalog
                .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
                .await
                .unwrap();
            assert_eq!(jobs.len(), 1);
            let capture_id = jobs[0].capture_id.clone();
            assert_eq!(jobs[0].trigger["type"], "schedule");
            assert_eq!(jobs[0].trigger["scheduleId"], "minute");
            assert_eq!(
                wait_for_terminal(&runtime, &capture_id).await.state,
                crate::model::JobState::Succeeded
            );
            assert!(
                !runtime
                    .has_schedule_overlap("camera-a", "minute")
                    .await
                    .unwrap()
            );

            runtime.submit_scheduled(&occurrence).await.unwrap();
            let duplicate_page = runtime
                .catalog
                .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
                .await
                .unwrap();
            assert_eq!(duplicate_page.len(), 1);
            assert_eq!(duplicate_page[0].capture_id, capture_id);
            assert_eq!(
                runtime
                    .catalog
                    .latest_schedule_occurrence("camera-a", "minute")
                    .await
                    .unwrap(),
                Some(intended_fire_time.timestamp_millis())
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn simulator_runtime_schedule_loop_skips_overlapping_occurrences() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], true);
            configuration.instances[0].schedules[0].cron = "* * * * * *".to_string();
            let plan = SchedulePlan::compile(
                "camera-a".to_string(),
                &configuration.instances[0].schedules[0],
            )
            .unwrap();
            let runtime = runtime(configuration, &directory).await;
            let cancellation = CancellationToken::new();
            let runner =
                tokio::spawn(Arc::clone(&runtime).run_schedule(plan, cancellation.clone()));

            let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            loop {
                if !runtime
                    .catalog
                    .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
                    .await
                    .unwrap()
                    .is_empty()
                {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the one-second schedule did not durably admit an occurrence"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(
                runtime
                    .has_schedule_overlap("camera-a", "minute")
                    .await
                    .unwrap()
            );
            tokio::time::sleep(Duration::from_millis(1_250)).await;
            cancellation.cancel();
            runner.await.unwrap();
            assert_eq!(
                runtime
                    .catalog
                    .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
                    .await
                    .unwrap()
                    .len(),
                1,
                "an overlap-policy skip must consume later fires without admitting a second job"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn simulator_runtime_moving_reject_interlock_skips_scheduled_admission() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], true);
            let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
            else {
                panic!("test fixture must use the simulator backend");
            };
            sim.ptz.supported = true;
            sim.ptz.status_supported = true;
            configuration.instances[0].ptz.enabled = true;
            configuration.instances[0]
                .capture_profiles
                .get_mut("main")
                .unwrap()
                .capture_interlock = Some(crate::config::CaptureInterlock::Reject);
            let runtime = runtime(configuration, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;
            runtime
                .actor("camera-a")
                .unwrap()
                .ptz(
                    crate::model::PtzRequest::Continuous {
                        velocity: crate::model::PtzVector {
                            pan: 0.5,
                            tilt: 0.0,
                            zoom: 0.0,
                        },
                        timeout: Duration::from_secs(2),
                    },
                    tokio::time::Instant::now() + Duration::from_secs(1),
                    &runtime.cancellation,
                )
                .await
                .unwrap();
            let occurrence = ScheduleOccurrence {
                scope: crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
                schedule_id: "minute".to_string(),
                intended_fire_time: chrono::Utc::now(),
                admit_at: chrono::Utc::now(),
                jitter: Duration::ZERO,
            };
            runtime.submit_scheduled(&occurrence).await.unwrap();
            assert!(
                runtime
                    .catalog
                    .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
                    .await
                    .unwrap()
                    .is_empty(),
                "a moving camera with reject interlock must not create a scheduled capture"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn runtime_startup_router_and_deferred_capture_flows_use_real_core_facades() {
            let directory = TempDir::new().unwrap();
            let router = RuntimeCommandRouter::new();
            let (port, publishes) = spawn_recording_mqtt_broker().await;
            let core = facade_core_with_router(&directory, port, Some(Arc::clone(&router))).await;
            let inbox = core
                .commands()
                .expect("the MQTT core fixture must expose a command inbox");
            for verb in CAMERA_COMMAND_VERBS {
                assert!(
                    inbox.verbs().contains(verb),
                    "router registration omitted required camera verb {verb}"
                );
            }
            let deferred = inbox.deferred_replies();

            match router
                .dispatch(
                    "sb/list",
                    command_message("sb/list", "before-runtime", json!({ "limit": 1 })),
                    deferred.clone(),
                )
                .await
            {
                CommandOutcome::ImmediateError(error) => {
                    assert_eq!(error.code, "CAMERA_UNAVAILABLE");
                }
                other => panic!("uninstalled router must reject requests, got {other:?}"),
            }

            let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
            configuration.global.state.directory = Some(
                directory
                    .path()
                    .join("startup-state")
                    .to_string_lossy()
                    .into_owned(),
            );
            for camera in &mut configuration.instances {
                let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
                    panic!("test fixture must use the simulator backend");
                };
                sim.capture_delay_ms = 1;
            }
            let resources = prepare_startup_resources(&configuration, Platform::Host)
                .await
                .unwrap();
            let mut apps = BTreeMap::new();
            let mut events = BTreeMap::new();
            for camera in &configuration.instances {
                let instance = core.instance(&camera.id).unwrap();
                apps.insert(camera.id.clone(), Arc::new(instance.app()));
                events.insert(camera.id.clone(), instance.events());
            }
            let core_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let readiness = {
                let core_ready = Arc::clone(&core_ready);
                RuntimeReadiness::new(Arc::new(move |ready| {
                    core_ready.store(ready, std::sync::atomic::Ordering::Release);
                }))
            };
            let runtime = CameraRuntime::start(
                configuration,
                resources,
                RuntimeServices {
                    apps,
                    events,
                    outbox_events: core.events(),
                    metrics: Arc::new(RecordingMetrics::default()),
                    readiness: readiness.clone(),
                    backend_context: BackendRuntimeContext::new(
                        None,
                        &crate::config::LimitsConfig::default(),
                    ),
                    messaging: core.messaging().unwrap(),
                },
            )
            .await
            .unwrap();
            for instance in ["camera-a", "camera-b"] {
                wait_for_online(&runtime, instance).await;
            }
            router.install(runtime.clone()).unwrap();
            readiness.complete_startup();
            assert!(
                core_ready.load(std::sync::atomic::Ordering::Acquire),
                "readiness must remain closed until runtime installation then become true"
            );

            let direct_publish_index = publishes.lock().unwrap().len();
            let direct_continuation = match router
                .dispatch(
                    "sb/capture",
                    command_message(
                        "sb/capture",
                        "startup-direct",
                        json!({ "instance": "camera-a", "requestId": "startup-direct" }),
                    ),
                    deferred.clone(),
                )
                .await
            {
                CommandOutcome::DeferredWithContinuation { continuation, .. } => continuation,
                other => panic!("direct capture must use deferred hand-off, got {other:?}"),
            };
            direct_continuation.await.unwrap();
            let direct = runtime
                .catalog
                .job_by_ledger(
                    crate::catalog::LedgerKey::new("camera-a", "sb/capture", "startup-direct")
                        .unwrap(),
                )
                .await
                .unwrap()
                .expect("deferred direct capture must be durably accepted");
            assert_eq!(
                wait_for_terminal(&runtime, &direct.capture_id).await.state,
                crate::model::JobState::Succeeded
            );
            let direct_reply = wait_for_recorded_reply(&publishes, direct_publish_index).await;
            assert_eq!(direct_reply.body["ok"], true);
            assert_eq!(direct_reply.body["result"]["captureId"], direct.capture_id);

            let group_publish_index = publishes.lock().unwrap().len();
            let group_continuation = match router
                .dispatch(
                    "sb/capture-group",
                    command_message(
                        "sb/capture-group",
                        "startup-group",
                        json!({
                            "requestId": "startup-group",
                            "instances": ["camera-a", "camera-b"]
                        }),
                    ),
                    deferred.clone(),
                )
                .await
            {
                CommandOutcome::DeferredWithContinuation { continuation, .. } => continuation,
                other => panic!("direct group capture must use deferred hand-off, got {other:?}"),
            };
            group_continuation.await.unwrap();
            let group = runtime
                .catalog
                .group_by_ledger(
                    crate::catalog::LedgerKey::new("main", "sb/capture-group", "startup-group")
                        .unwrap(),
                )
                .await
                .unwrap()
                .expect("deferred group capture must be durably accepted");
            let group = wait_for_group_terminal(&runtime, &group.group_id).await;
            assert_eq!(group.members.len(), 2);
            assert!(
                group
                    .members
                    .iter()
                    .all(|member| member.state == crate::model::JobState::Succeeded)
            );
            let group_reply = wait_for_recorded_reply(&publishes, group_publish_index).await;
            assert_eq!(group_reply.body["ok"], true);
            assert_eq!(group_reply.body["result"]["captureGroupId"], group.group_id);
            assert_eq!(
                group_reply.body["result"]["members"]
                    .as_array()
                    .unwrap()
                    .len(),
                2
            );

            router.begin_shutdown();
            match router
                .dispatch(
                    "sb/list",
                    command_message("sb/list", "after-shutdown", json!({ "limit": 1 })),
                    deferred,
                )
                .await
            {
                CommandOutcome::ImmediateError(error) => {
                    assert_eq!(error.code, "COMPONENT_STOPPING");
                }
                other => panic!("stopping router must reject requests, got {other:?}"),
            }
            runtime.shutdown().await;
            assert!(
                !core_ready.load(std::sync::atomic::Ordering::Acquire),
                "runtime shutdown must close the readiness bridge"
            );
            inbox.stop().await;
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        const SHORT_CAPACITY_CONFIGURED_CAMERAS: usize = 1_024;
        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        const SHORT_CAPACITY_ENABLED_CAMERAS: usize = 256;
        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        const SHORT_CAPACITY_CONCURRENT_CAPTURES: usize = 32;
        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        const SHORT_CAPACITY_FRAME_WIDTH: u32 = 3_264;
        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        const SHORT_CAPACITY_FRAME_HEIGHT: u32 = 2_448;
        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        const SHORT_CAPACITY_FRAME_BYTES: u64 =
            (SHORT_CAPACITY_FRAME_WIDTH as u64) * (SHORT_CAPACITY_FRAME_HEIGHT as u64);
        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        const IDLE_SESSION_RSS_MAXIMUM_FULL_FRAME_FRACTION_DENOMINATOR: u64 = 8;

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CapacityProcessStats {
            rss_bytes: Option<u64>,
            thread_count: Option<u64>,
            open_file_descriptors: Option<u64>,
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CapacitySample {
            phase: String,
            elapsed_millis: u64,
            configured_cameras: usize,
            enabled_cameras: usize,
            online_cameras: usize,
            live_actor_count: usize,
            queued_capture_descriptors: usize,
            queued_control_descriptors: usize,
            available_global_acquisitions: usize,
            available_resource_group_acquisitions: BTreeMap<String, usize>,
            available_in_flight_bytes: u64,
            outstanding_disk_bytes: u64,
            available_encoders: usize,
            available_writers: usize,
            process: CapacityProcessStats,
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CommandLatencySummary {
            samples: usize,
            minimum_micros: u64,
            p50_micros: u64,
            p95_micros: u64,
            maximum_micros: u64,
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct IdleSessionMemoryEvidence {
            baseline_rss_bytes: u64,
            startup_peak_rss_bytes: u64,
            roster_online_rss_bytes: u64,
            startup_peak_delta_bytes: u64,
            roster_online_delta_bytes: u64,
            full_frame_allocation_equivalent_bytes: u64,
            maximum_allowed_delta_bytes: u64,
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct ShortCapacityArtifact {
            schema_version: &'static str,
            scope: &'static str,
            configured_cameras: usize,
            enabled_simulated_sessions: usize,
            concurrent_capture_target: usize,
            frame: serde_json::Value,
            idle_session_memory: IdleSessionMemoryEvidence,
            capture_group_submit_micros: u64,
            command_latency: BTreeMap<String, CommandLatencySummary>,
            resource_samples: Vec<CapacitySample>,
            group_terminal_state: String,
            group_successful_members: usize,
            overflow_capture_terminal_state: String,
            omitted_from_this_short_run: Vec<&'static str>,
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        fn short_capacity_configuration(root: &Path) -> AdapterConfig {
            let output_root = root.join("capacity-output");
            let state_root = root.join("capacity-state");
            let instances = (0..SHORT_CAPACITY_CONFIGURED_CAMERAS)
                .map(|index| {
                    json!({
                        "id": format!("camera-{index:04}"),
                        "enabled": index < SHORT_CAPACITY_ENABLED_CAMERAS,
                        "resourceGroup": "sim-shared",
                        "backend": {
                            "type": "sim",
                            "captureDelayMs": 5_000,
                            "frame": {
                                "width": SHORT_CAPACITY_FRAME_WIDTH,
                                "height": SHORT_CAPACITY_FRAME_HEIGHT,
                                "pixelFormat": "Mono8",
                                "pattern": "checkerboard"
                            },
                            "ptz": { "supported": true, "statusSupported": true }
                        },
                        "ptz": { "enabled": true },
                        "defaultCaptureProfile": "main",
                        "captureProfiles": {
                            "main": {
                                "maximumFrameBytes": SHORT_CAPACITY_FRAME_BYTES,
                                "output": { "encoding": "raw" }
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();
            let raw = json!({
                "component": {
                    "global": {
                        "output": {
                            "rootDirectory": output_root,
                            "minimumFreeBytes": 0,
                            "minimumFreePercent": 0
                        },
                        "state": { "directory": state_root },
                        "limits": {
                            "maxConnectedCameras": SHORT_CAPACITY_ENABLED_CAMERAS,
                            "maxConcurrentCaptures": SHORT_CAPACITY_CONCURRENT_CAPTURES,
                            "maxConcurrentEncodes": 8,
                            "maxConcurrentWrites": 8,
                            "maxConcurrentConnects": 16,
                            "maxInFlightBytes": SHORT_CAPACITY_FRAME_BYTES * SHORT_CAPACITY_CONCURRENT_CAPTURES as u64,
                            "maxFrameBytesPerCamera": SHORT_CAPACITY_FRAME_BYTES,
                            "maxCamerasPerGroup": SHORT_CAPACITY_CONCURRENT_CAPTURES,
                            "resourceGroups": {
                                "sim-shared": {
                                    "maxConcurrentCaptures": SHORT_CAPACITY_CONCURRENT_CAPTURES
                                }
                            }
                        }
                    },
                    "instances": instances
                }
            });
            AdapterConfig::from_core_reload(
                &Config::from_value(COMPONENT_NAME, "capacity-lab", raw)
                    .expect("short capacity configuration must be structurally valid"),
            )
            .expect("short capacity configuration must satisfy adapter limits")
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        async fn wait_for_capacity_roster(runtime: &CameraRuntime) -> u64 {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            let mut peak_rss_bytes = 0_u64;
            loop {
                let rss_bytes = capacity_process_stats().rss_bytes.expect(
                    "Linux capacity proof requires /proc/self/status VmRSS while supervisors start",
                );
                peak_rss_bytes = peak_rss_bytes.max(rss_bytes);
                let snapshots = runtime
                    .registry
                    .snapshots(SHORT_CAPACITY_CONFIGURED_CAMERAS)
                    .expect("capacity registry must remain readable");
                let online = snapshots
                    .iter()
                    .filter(|snapshot| snapshot.state == CameraConnectionState::Online)
                    .count();
                let disabled = snapshots
                    .iter()
                    .filter(|snapshot| snapshot.state == CameraConnectionState::Disabled)
                    .count();
                let live_actor_count = runtime
                    .actors
                    .read()
                    .expect("capacity actor registry lock must remain readable")
                    .len();
                if snapshots.len() == SHORT_CAPACITY_CONFIGURED_CAMERAS
                    && online == SHORT_CAPACITY_ENABLED_CAMERAS
                    && disabled
                        == SHORT_CAPACITY_CONFIGURED_CAMERAS - SHORT_CAPACITY_ENABLED_CAMERAS
                    && live_actor_count == SHORT_CAPACITY_ENABLED_CAMERAS
                {
                    return peak_rss_bytes;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "short capacity roster did not reach 256 ONLINE live actors and 768 DISABLED cameras; online={online}, liveActors={live_actor_count}, disabled={disabled}, total={}",
                    snapshots.len(),
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        fn proc_status_value(key: &str) -> Option<u64> {
            fs::read_to_string("/proc/self/status")
                .ok()?
                .lines()
                .find_map(|line| {
                    let value = line.strip_prefix(key)?.split_whitespace().next()?;
                    value.parse::<u64>().ok()
                })
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        fn capacity_process_stats() -> CapacityProcessStats {
            CapacityProcessStats {
                rss_bytes: proc_status_value("VmRSS:").and_then(|kib| kib.checked_mul(1024)),
                thread_count: proc_status_value("Threads:"),
                open_file_descriptors: fs::read_dir("/proc/self/fd")
                    .ok()
                    .map(|entries| entries.filter_map(std::result::Result::ok).count() as u64),
            }
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        fn capacity_sample(
            runtime: &CameraRuntime,
            phase: &str,
            started: Instant,
        ) -> CapacitySample {
            let admission = runtime.admission.snapshot();
            let snapshots = runtime
                .registry
                .snapshots(SHORT_CAPACITY_CONFIGURED_CAMERAS)
                .expect("capacity registry must remain readable");
            let actors = runtime
                .actors
                .read()
                .expect("capacity actor map must remain readable");
            let queued_capture_descriptors =
                actors.values().map(|actor| actor.queued_captures()).sum();
            let queued_control_descriptors =
                actors.values().map(|actor| actor.queued_controls()).sum();
            CapacitySample {
                phase: phase.to_owned(),
                elapsed_millis: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                configured_cameras: snapshots.len(),
                enabled_cameras: snapshots.iter().filter(|snapshot| snapshot.enabled).count(),
                online_cameras: snapshots
                    .iter()
                    .filter(|snapshot| snapshot.state == CameraConnectionState::Online)
                    .count(),
                live_actor_count: actors.len(),
                queued_capture_descriptors,
                queued_control_descriptors,
                available_global_acquisitions: admission.available_acquisitions,
                available_resource_group_acquisitions: admission
                    .available_resource_group_acquisitions,
                available_in_flight_bytes: admission.available_memory_bytes,
                outstanding_disk_bytes: admission.outstanding_disk_bytes,
                available_encoders: admission.available_encoders,
                available_writers: admission.available_writers,
                process: capacity_process_stats(),
            }
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        fn summarize_latency(mut values: Vec<u64>) -> CommandLatencySummary {
            values.sort_unstable();
            let sample_count = values.len();
            assert!(
                sample_count > 0,
                "capacity command timing series must not be empty"
            );
            let p95_index = (sample_count * 95).div_ceil(100).saturating_sub(1);
            CommandLatencySummary {
                samples: sample_count,
                minimum_micros: values[0],
                p50_micros: values[(sample_count - 1) / 2],
                p95_micros: values[p95_index],
                maximum_micros: values[sample_count - 1],
            }
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        async fn time_immediate_command(
            router: &RuntimeCommandRouter,
            deferred: &DeferredReplyRegistry,
            verb: &'static str,
            suffix: &str,
            body: serde_json::Value,
        ) -> u64 {
            let started = Instant::now();
            let _ = immediate_success(
                router
                    .dispatch(verb, command_message(verb, suffix, body), deferred.clone())
                    .await,
            );
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        fn capacity_artifact_directory() -> PathBuf {
            std::env::var_os("CAMERA_ADAPTER_CAPACITY_ARTIFACT_DIR")
                .map(PathBuf::from)
                .expect(
                    "set CAMERA_ADAPTER_CAPACITY_ARTIFACT_DIR to an explicit empty artifact directory",
                )
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        fn write_capacity_artifact<T: Serialize>(directory: &Path, file_name: &str, artifact: &T) {
            fs::create_dir_all(directory).expect("capacity artifact directory must be creatable");
            let destination = directory.join(file_name);
            assert!(
                !destination.exists(),
                "refusing to overwrite existing capacity evidence: {}",
                destination.display()
            );
            let temporary = directory.join(format!(
                ".short-capacity-summary-{}.tmp",
                uuid::Uuid::now_v7()
            ));
            fs::write(
                &temporary,
                serde_json::to_vec_pretty(artifact)
                    .expect("capacity artifact must serialize to JSON"),
            )
            .expect("capacity artifact temporary file must be writable");
            fs::rename(&temporary, &destination)
                .expect("capacity artifact must atomically install in its requested directory");
        }

        /// Short Linux-only capacity proof for a simulated fleet.
        ///
        /// It is intentionally ignored so routine local test runs do not create 33 eight-megapixel
        /// images. The companion Linux runner provides an explicit artifact directory. This proves
        /// the roster and admission slice only; it is not the deferred 24-hour soak or a hardware
        /// compatibility test.
        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        #[ignore = "short Linux capacity evidence; run simulators/run-capacity-validation.sh"]
        async fn short_linux_capacity_proves_1024_configured_256_sessions_and_32_captures() {
            let artifact_directory = capacity_artifact_directory();
            let temporary_root = TempDir::new().expect("capacity test root must be creatable");
            let configuration = short_capacity_configuration(temporary_root.path());
            fs::create_dir_all(&configuration.global.output.root_directory)
                .expect("capacity output root must exist before secure storage initialization");
            let instance_ids = configuration
                .instances
                .iter()
                .map(|camera| camera.id.clone())
                .collect::<Vec<_>>();
            assert_eq!(instance_ids.len(), SHORT_CAPACITY_CONFIGURED_CAMERAS);
            assert_eq!(
                configuration
                    .instances
                    .iter()
                    .filter(|camera| camera.enabled)
                    .count(),
                SHORT_CAPACITY_ENABLED_CAMERAS
            );

            let router = RuntimeCommandRouter::new();
            let (port, _) = spawn_recording_mqtt_broker().await;
            let core = facade_core_with_router_instances(
                &temporary_root,
                port,
                Some(Arc::clone(&router)),
                &instance_ids,
            )
            .await;
            let inbox = core
                .commands()
                .expect("capacity fixture Core must expose the command inbox");
            let deferred = inbox.deferred_replies();
            let resources = prepare_startup_resources(&configuration, Platform::Host)
                .await
                .expect("capacity startup resources must be durable and valid");
            let mut apps = BTreeMap::new();
            let mut events = BTreeMap::new();
            for instance in &instance_ids {
                let instance_facade = core
                    .instance(instance)
                    .expect("configured capacity instance must build Core facades");
                apps.insert(instance.clone(), Arc::new(instance_facade.app()));
                events.insert(instance.clone(), instance_facade.events());
            }
            let idle_session_baseline_rss_bytes = capacity_process_stats().rss_bytes.expect(
                "Linux capacity proof requires /proc/self/status VmRSS before runtime startup",
            );
            let startup_rss_peak = Arc::new(AtomicU64::new(idle_session_baseline_rss_bytes));
            let startup_sampling_cancellation = CancellationToken::new();
            let startup_sampler = {
                let startup_rss_peak = Arc::clone(&startup_rss_peak);
                let cancellation = startup_sampling_cancellation.clone();
                tokio::spawn(async move {
                    loop {
                        if let Some(rss_bytes) = capacity_process_stats().rss_bytes {
                            startup_rss_peak.fetch_max(rss_bytes, Ordering::AcqRel);
                        }
                        tokio::select! {
                            _ = cancellation.cancelled() => return,
                            () = tokio::time::sleep(Duration::from_millis(5)) => {}
                        }
                    }
                })
            };
            let readiness = RuntimeReadiness::noop();
            let runtime = CameraRuntime::start(
                configuration,
                resources,
                RuntimeServices {
                    apps,
                    events,
                    outbox_events: core.events(),
                    readiness,
                    backend_context: BackendRuntimeContext::new(
                        None,
                        &crate::config::LimitsConfig::default(),
                    ),
                    messaging: core
                        .messaging()
                        .expect("capacity Core must expose messaging"),
                },
            )
            .await
            .expect("capacity runtime must start all configured supervisors");
            router
                .install(runtime.clone())
                .expect("capacity router must install exactly one complete runtime");
            let started = Instant::now();
            let roster_poll_peak_rss_bytes = wait_for_capacity_roster(&runtime).await;
            startup_sampling_cancellation.cancel();
            startup_sampler
                .await
                .expect("capacity startup RSS sampler must not panic");
            let roster_online_sample = capacity_sample(&runtime, "roster-online", started);
            let roster_online_rss_bytes = roster_online_sample.process.rss_bytes.expect(
                "Linux capacity proof requires /proc/self/status VmRSS after roster startup",
            );
            let full_frame_allocation_equivalent_bytes = SHORT_CAPACITY_FRAME_BYTES
                .checked_mul(SHORT_CAPACITY_ENABLED_CAMERAS as u64)
                .expect("configured full-frame equivalent must fit in u64");
            let maximum_allowed_delta_bytes = full_frame_allocation_equivalent_bytes
                / IDLE_SESSION_RSS_MAXIMUM_FULL_FRAME_FRACTION_DENOMINATOR;
            let startup_peak_rss_bytes = startup_rss_peak
                .load(Ordering::Acquire)
                .max(roster_poll_peak_rss_bytes)
                .max(roster_online_rss_bytes);
            let startup_peak_delta_bytes =
                startup_peak_rss_bytes.saturating_sub(idle_session_baseline_rss_bytes);
            let roster_online_delta_bytes =
                roster_online_rss_bytes.saturating_sub(idle_session_baseline_rss_bytes);
            assert!(
                startup_peak_delta_bytes <= maximum_allowed_delta_bytes,
                "256 idle SimBackend sessions increased startup RSS by {startup_peak_delta_bytes} bytes; this must remain at most one eighth of their {full_frame_allocation_equivalent_bytes}-byte full-frame equivalent"
            );
            let idle_session_memory = IdleSessionMemoryEvidence {
                baseline_rss_bytes: idle_session_baseline_rss_bytes,
                startup_peak_rss_bytes,
                roster_online_rss_bytes,
                startup_peak_delta_bytes,
                roster_online_delta_bytes,
                full_frame_allocation_equivalent_bytes,
                maximum_allowed_delta_bytes,
            };
            let mut samples = vec![roster_online_sample];

            let capture_instances = instance_ids
                .iter()
                .take(SHORT_CAPACITY_CONCURRENT_CAPTURES)
                .cloned()
                .collect::<Vec<_>>();
            let accepted_at = Instant::now();
            let group_response = immediate_success(
                router
                    .dispatch(
                        "sb/capture-group-submit",
                        command_message(
                            "sb/capture-group-submit",
                            "short-capacity-group",
                            json!({
                                "requestId": "short-capacity-group",
                                "instances": capture_instances
                            }),
                        ),
                        deferred.clone(),
                    )
                    .await,
            );
            let capture_group_submit_micros =
                u64::try_from(accepted_at.elapsed().as_micros()).unwrap_or(u64::MAX);
            let group_id = group_response["captureGroupId"]
                .as_str()
                .expect("capacity group response must contain a group id")
                .to_owned();

            let saturation_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            let saturation = loop {
                let sample = capacity_sample(&runtime, "acquisition-saturation", started);
                let group_available = sample
                    .available_resource_group_acquisitions
                    .get("sim-shared")
                    .copied();
                if sample.available_global_acquisitions == 0
                    && group_available == Some(0)
                    && sample.available_in_flight_bytes == 0
                    && sample.outstanding_disk_bytes
                        == SHORT_CAPACITY_FRAME_BYTES * SHORT_CAPACITY_CONCURRENT_CAPTURES as u64
                {
                    break sample;
                }
                samples.push(sample);
                assert!(
                    tokio::time::Instant::now() < saturation_deadline,
                    "the short capacity group never saturated global, resource-group, byte, and disk admission"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            };
            samples.push(saturation);

            let overflow_response = immediate_success(
                router
                    .dispatch(
                        "sb/capture-submit",
                        command_message(
                            "sb/capture-submit",
                            "short-capacity-overflow",
                            json!({
                                "instance": "camera-0032",
                                "requestId": "short-capacity-overflow"
                            }),
                        ),
                        inbox.deferred_replies(),
                    )
                    .await,
            );
            let overflow_capture_id = overflow_response["captureId"]
                .as_str()
                .expect("overflow capture response must contain a capture id")
                .to_owned();
            let overflow_queued = runtime
                .catalog
                .job(&overflow_capture_id)
                .await
                .expect("overflow capture query must succeed")
                .expect("overflow capture must be durable");
            assert!(
                !overflow_queued.state.is_terminal(),
                "the 33rd capture must not bypass a saturated global admission gate"
            );
            samples.push(capacity_sample(&runtime, "overflow-queued", started));

            let mut latency_samples = BTreeMap::<String, Vec<u64>>::new();
            for index in 0..20 {
                latency_samples
                    .entry("sb/list".to_owned())
                    .or_default()
                    .push(
                        time_immediate_command(
                            &router,
                            &deferred,
                            "sb/list",
                            &format!("short-capacity-list-{index}"),
                            json!({ "limit": 1 }),
                        )
                        .await,
                    );
                latency_samples
                    .entry("sb/status".to_owned())
                    .or_default()
                    .push(
                        time_immediate_command(
                            &router,
                            &deferred,
                            "sb/status",
                            &format!("short-capacity-status-{index}"),
                            json!({ "instance": "camera-0033" }),
                        )
                        .await,
                    );
                latency_samples
                    .entry("sb/ptz-stop".to_owned())
                    .or_default()
                    .push(
                        time_immediate_command(
                            &router,
                            &deferred,
                            "sb/ptz",
                            &format!("short-capacity-stop-{index}"),
                            json!({
                                "operation": "stop",
                                "instance": "camera-0033",
                                "requestId": format!("short-capacity-stop-{index}"),
                                "axes": ["pan", "tilt", "zoom"]
                            }),
                        )
                        .await,
                    );
            }
            let command_latency = latency_samples
                .into_iter()
                .map(|(verb, values)| (verb, summarize_latency(values)))
                .collect::<BTreeMap<_, _>>();
            for (verb, summary) in &command_latency {
                assert!(
                    summary.p95_micros <= 250_000,
                    "{verb} p95 was {}us while acquisitions were saturated",
                    summary.p95_micros
                );
            }

            let group =
                wait_for_group_terminal_within(&runtime, &group_id, Duration::from_secs(90)).await;
            assert_eq!(group.members.len(), SHORT_CAPACITY_CONCURRENT_CAPTURES);
            let group_successful_members = group
                .members
                .iter()
                .filter(|member| member.state == crate::model::JobState::Succeeded)
                .count();
            assert_eq!(group_successful_members, SHORT_CAPACITY_CONCURRENT_CAPTURES);
            let overflow =
                wait_for_terminal_within(&runtime, &overflow_capture_id, Duration::from_secs(90))
                    .await;
            assert_eq!(overflow.state, crate::model::JobState::Succeeded);
            samples.push(capacity_sample(&runtime, "captures-terminal", started));

            router.begin_shutdown();
            runtime.shutdown().await;
            inbox.stop().await;

            write_capacity_artifact(
                &artifact_directory,
                "short-capacity-summary.json",
                &ShortCapacityArtifact {
                    schema_version: "camera-adapter-short-capacity/v1",
                    scope: "ignored Linux short proof using the real Core facade and in-process SimBackend; not a 24-hour soak or hardware test",
                    configured_cameras: SHORT_CAPACITY_CONFIGURED_CAMERAS,
                    enabled_simulated_sessions: SHORT_CAPACITY_ENABLED_CAMERAS,
                    concurrent_capture_target: SHORT_CAPACITY_CONCURRENT_CAPTURES,
                    frame: json!({
                        "width": SHORT_CAPACITY_FRAME_WIDTH,
                        "height": SHORT_CAPACITY_FRAME_HEIGHT,
                        "pixelFormat": "Mono8",
                        "bytesPerFrame": SHORT_CAPACITY_FRAME_BYTES
                    }),
                    idle_session_memory,
                    capture_group_submit_micros,
                    command_latency,
                    resource_samples: samples,
                    group_terminal_state: format!("{:?}", group.state),
                    group_successful_members,
                    overflow_capture_terminal_state: format!("{:?}", overflow.state),
                    omitted_from_this_short_run: vec![
                        "24-hour soak execution",
                        "10,000 mixed-job workload",
                        "broker-outage recovery",
                        "reload churn",
                        "encoder and writer saturation graph",
                        "Core ping handler timing",
                        "physical-camera compatibility",
                    ],
                },
            );
        }

        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        fn capacity_facades(
            core: &edgecommons::EdgeCommons,
            instances: &[String],
        ) -> (
            BTreeMap<String, Arc<AppFacade>>,
            BTreeMap<String, EventsFacade>,
        ) {
            let mut apps = BTreeMap::new();
            let mut events = BTreeMap::new();
            for instance in instances {
                let facade = core
                    .instance(instance)
                    .expect("configured capacity instance must build Core facades");
                apps.insert(instance.clone(), Arc::new(facade.app()));
                events.insert(instance.clone(), facade.events());
            }
            (apps, events)
        }

        /// Bounded 15-minute simulator smoke for the capacity harness itself.
        ///
        /// The runner always executes the separate 8MP short proof first. This smoke switches to
        /// small deterministic frames so schedules and command traffic can run for fifteen
        /// minutes without turning a harness-construction check into a multi-hour disk benchmark.
        #[cfg(all(
            target_os = "linux",
            feature = "standalone",
            feature = "onvif",
            feature = "capacity-harness"
        ))]
        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        #[ignore = "15-minute Linux simulator smoke; run simulators/run-capacity-validation.sh --soak-duration 15m"]
        async fn fifteen_minute_linux_capacity_smoke_exercises_mixed_runtime_traffic() {
            let duration_seconds = std::env::var("CAMERA_ADAPTER_CAPACITY_SOAK_DURATION_SECS")
                .expect("runner must set CAMERA_ADAPTER_CAPACITY_SOAK_DURATION_SECS")
                .parse::<u64>()
                .expect("soak duration must be an unsigned integer");
            assert_eq!(
                duration_seconds, 900,
                "only the bounded 15-minute smoke is implemented; the 24-hour soak remains deferred"
            );
            let artifact_directory = capacity_artifact_directory();
            let temporary_root = TempDir::new().expect("capacity smoke root must be creatable");
            let mut configuration = short_capacity_configuration(temporary_root.path());
            const SMOKE_FRAME_BYTES: u64 = 640 * 480;
            const SMOKE_RESERVATION_BYTES: u64 = 1024 * 1024;
            configuration.global.limits.max_frame_bytes_per_camera = SMOKE_RESERVATION_BYTES;
            configuration.global.limits.max_in_flight_bytes =
                SMOKE_RESERVATION_BYTES * SHORT_CAPACITY_CONCURRENT_CAPTURES as u64;
            for (index, camera) in configuration.instances.iter_mut().enumerate() {
                let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
                    panic!("capacity smoke must use SimBackend");
                };
                sim.capture_delay_ms = 50;
                sim.frame.width = 640;
                sim.frame.height = 480;
                camera
                    .capture_profiles
                    .get_mut("main")
                    .expect("capacity smoke main profile must exist")
                    .maximum_frame_bytes = Some(SMOKE_RESERVATION_BYTES);
                if index < 8 {
                    camera.schedules.push(
                        serde_json::from_value(json!({
                            "id": "five-second-smoke",
                            "cron": "*/5 * * * * *",
                            "timezone": "UTC",
                            "captureProfile": "main"
                        }))
                        .expect("capacity smoke schedule must deserialize"),
                    );
                }
            }
            fs::create_dir_all(&configuration.global.output.root_directory)
                .expect("capacity smoke output root must exist before storage initialization");
            let instance_ids = configuration
                .instances
                .iter()
                .map(|camera| camera.id.clone())
                .collect::<Vec<_>>();
            let router = RuntimeCommandRouter::new();
            let (port, _) = spawn_recording_mqtt_broker().await;
            let core = facade_core_with_router_instances(
                &temporary_root,
                port,
                Some(Arc::clone(&router)),
                &instance_ids,
            )
            .await;
            let inbox = core
                .commands()
                .expect("capacity smoke Core must expose inbox");
            let deferred = inbox.deferred_replies();
            let resources = prepare_startup_resources(&configuration, Platform::Host)
                .await
                .expect("capacity smoke startup resources must be valid");
            let (apps, events) = capacity_facades(&core, &instance_ids);
            let runtime = CameraRuntime::start(
                configuration,
                resources,
                RuntimeServices {
                    apps,
                    events,
                    outbox_events: core.events(),
                    readiness: RuntimeReadiness::noop(),
                    backend_context: BackendRuntimeContext::new(
                        None,
                        &crate::config::LimitsConfig::default(),
                    ),
                    messaging: core
                        .messaging()
                        .expect("capacity smoke Core must expose messaging"),
                },
            )
            .await
            .expect("capacity smoke runtime must start");
            router
                .install(runtime.clone())
                .expect("capacity smoke router must install");
            let _ = wait_for_capacity_roster(&runtime).await;

            let started = Instant::now();
            let deadline = started + Duration::from_secs(duration_seconds);
            let mut ticks = tokio::time::interval(Duration::from_secs(1));
            let mut samples = vec![capacity_sample(&runtime, "soak-roster-online", started)];
            let mut timing = BTreeMap::<String, Vec<u64>>::new();
            let mut submitted_captures = 0_u64;
            let mut reconnects = 0_u64;
            let mut reloads = 0_u64;
            let mut tick = 0_u64;
            while Instant::now() < deadline {
                ticks.tick().await;
                tick = tick.saturating_add(1);
                let target = format!("camera-{:04}", 32 + (tick as usize % 224));
                if tick % 2 == 0 {
                    let _ = immediate_success(
                        router
                            .dispatch(
                                "sb/capture-submit",
                                command_message(
                                    "sb/capture-submit",
                                    &format!("soak-capture-{tick}"),
                                    json!({ "instance": target, "requestId": format!("soak-capture-{tick}") }),
                                ),
                                deferred.clone(),
                            )
                            .await,
                    );
                    submitted_captures = submitted_captures.saturating_add(1);
                }
                if tick % 5 == 0 {
                    timing.entry("sb/list".to_owned()).or_default().push(
                        time_immediate_command(
                            &router,
                            &deferred,
                            "sb/list",
                            &format!("soak-list-{tick}"),
                            json!({ "limit": 10 }),
                        )
                        .await,
                    );
                    timing.entry("sb/status".to_owned()).or_default().push(
                        time_immediate_command(
                            &router,
                            &deferred,
                            "sb/status",
                            &format!("soak-status-{tick}"),
                            json!({ "instance": "camera-0033" }),
                        )
                        .await,
                    );
                    timing.entry("sb/ptz-stop".to_owned()).or_default().push(
                        time_immediate_command(
                            &router,
                            &deferred,
                            "sb/ptz",
                            &format!("soak-stop-{tick}"),
                            json!({
                                "operation": "stop",
                                "instance": "camera-0033",
                                "requestId": format!("soak-stop-{tick}"),
                                "axes": ["pan", "tilt", "zoom"]
                            }),
                        )
                        .await,
                    );
                    samples.push(capacity_sample(&runtime, "soak-sample", started));
                }
                if tick % 60 == 0 {
                    runtime
                        .reconnect(ReconnectRequest {
                            instance: Some("camera-0000".to_owned()),
                            request_id: format!("soak-reconnect-{tick}"),
                            reason: Some("capacity-smoke".to_owned()),
                        })
                        .await
                        .expect("capacity smoke reconnect must be accepted");
                    reconnects = reconnects.saturating_add(1);
                }
                if tick % 180 == 0 {
                    let replacement = runtime
                        .config_snapshot()
                        .expect("capacity smoke configuration must remain readable");
                    let (apps, events) = capacity_facades(&core, &instance_ids);
                    runtime
                        .apply_reloaded_config(replacement, apps, events)
                        .await
                        .expect("capacity smoke reload must preserve the valid generation");
                    reloads = reloads.saturating_add(1);
                }
            }
            // The final workload tick can deliberately request a reconnect.  Do not label the
            // smoke complete until that bounded lifecycle transition has converged back to the
            // full configured roster; otherwise the final report would confuse an in-progress
            // reconnect with lost capacity.
            let _ = wait_for_capacity_roster(&runtime).await;
            samples.push(capacity_sample(&runtime, "soak-complete", started));
            let mut scheduled_jobs_by_camera = BTreeMap::new();
            for instance in instance_ids.iter().take(8) {
                let scheduled_jobs = runtime
                    .catalog
                    .jobs_page(Some(instance.clone()), Vec::new(), None, 1_000)
                    .await
                    .expect("capacity smoke must retain scheduled job evidence")
                    .into_iter()
                    .filter(|job| {
                        job.trigger.get("type").and_then(Value::as_str) == Some("schedule")
                            && job.trigger.get("scheduleId").and_then(Value::as_str)
                                == Some("five-second-smoke")
                    })
                    .count() as u64;
                assert!(
                    scheduled_jobs >= 120,
                    "capacity smoke must record sustained scheduled traffic for {instance}; observed {scheduled_jobs} accepted occurrences"
                );
                scheduled_jobs_by_camera.insert(instance.clone(), scheduled_jobs);
            }
            let command_latency = timing
                .into_iter()
                .map(|(verb, values)| (verb, summarize_latency(values)))
                .collect::<BTreeMap<_, _>>();
            assert!(
                submitted_captures >= 400,
                "15-minute smoke submitted too little command traffic"
            );
            assert!(
                reconnects >= 14,
                "15-minute smoke omitted reconnect traffic"
            );
            assert!(reloads >= 4, "15-minute smoke omitted reload traffic");

            router.begin_shutdown();
            runtime.shutdown().await;
            inbox.stop().await;
            write_capacity_artifact(
                &artifact_directory,
                "fifteen-minute-soak-summary.json",
                &json!({
                    "schemaVersion": "camera-adapter-capacity-smoke/v1",
                    "scope": "15-minute Linux SimBackend smoke; not a 24-hour soak or hardware test",
                    "durationSeconds": duration_seconds,
                    "configuredCameras": SHORT_CAPACITY_CONFIGURED_CAMERAS,
                    "enabledSimulatedSessions": SHORT_CAPACITY_ENABLED_CAMERAS,
                    "scheduledCameras": 8,
                    "scheduledJobsByCamera": scheduled_jobs_by_camera,
                    "frame": { "width": 640, "height": 480, "pixelFormat": "Mono8", "bytesPerFrame": SMOKE_FRAME_BYTES, "reservationBytes": SMOKE_RESERVATION_BYTES },
                    "submittedCaptures": submitted_captures,
                    "reconnects": reconnects,
                    "reloads": reloads,
                    "commandLatency": command_latency,
                    "resourceSamples": samples,
                    "omittedFromThisSmoke": ["24-hour execution", "10,000-job completion target", "broker-outage recovery", "encoder/writer saturation", "Core ping timing", "physical cameras"]
                }),
            );
        }

        fn retention_job(capture_id: &str, request_id: &str) -> crate::catalog::NewJob {
            let canonical_request = json!({ "requestId": request_id, "profile": "main" });
            crate::catalog::NewJob {
                capture_id: capture_id.to_owned(),
                instance: "camera-a".to_owned(),
                ledger_key: Some(
                    crate::catalog::LedgerKey::new("camera-a", "sb/capture", request_id).unwrap(),
                ),
                request_hash: crate::idempotency::canonical_request_hash(&canonical_request, false)
                    .unwrap(),
                canonical_request,
                effective_profile: json!({ "name": "main", "encoding": "jpeg" }),
                deadlines: crate::catalog::JobDeadlines {
                    terminal_at_ms: 10_000,
                    queue_at_ms: Some(2_000),
                    capture_at_ms: 4_000,
                    encode_at_ms: 7_000,
                    persist_at_ms: 9_000,
                },
                trigger: json!({ "type": "command", "requestId": request_id }),
                origin_correlation_id: Some(format!("corr-{capture_id}")),
                intended_output: json!({ "relativePath": format!("{capture_id}.jpg") }),
                accepted_at_ms: 1_000,
                group_id: None,
            }
        }

        fn retention_terminal(
            capture_id: &str,
            instance: &str,
            sequence: u8,
        ) -> crate::catalog::TerminalWrite {
            retention_terminal_for(capture_id, instance, None, sequence)
        }

        fn retention_terminal_for(
            capture_id: &str,
            instance: &str,
            group_id: Option<&str>,
            sequence: u8,
        ) -> crate::catalog::TerminalWrite {
            let event_key = format!("retention-event-{sequence}");
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
            let message = MessageBuilder::new("ImageCaptureFailed", "1.0")
                .correlation_id(format!("corr-{capture_id}"))
                .payload(body)
                .build();
            crate::catalog::TerminalWrite {
                state: crate::model::JobState::Failed,
                result: json!({ "state": "FAILED" }),
                error_code: Some("PROCESS_INTERRUPTED".to_owned()),
                error_message: Some("bounded failure".to_owned()),
                // Deliberately ancient: every sweep below uses the real clock, so these records
                // are far outside both configured retention windows.
                terminal_at_ms: 20_000 + i64::from(sequence),
                outbox: crate::catalog::NewOutboxMessage::from_message(
                    event_key,
                    "terminal",
                    "ecv1/test/camera-adapter/main/app/image/failed",
                    &message,
                    20_000,
                    20_000,
                )
                .unwrap(),
            }
        }

        /// Seeds two terminal direct jobs, one terminal group with a terminal member, and one
        /// completed standalone command ledger, then marks every terminal message delivered.
        async fn seed_retained_state(catalog: &Catalog) {
            for (capture, sequence) in [("retention-a", 1_u8), ("retention-b", 2)] {
                catalog
                    .accept_job(retention_job(capture, capture))
                    .await
                    .unwrap();
                catalog.queue_job(capture, 2_000).await.unwrap();
                catalog
                    .commit_terminal(capture, retention_terminal(capture, "camera-a", sequence))
                    .await
                    .unwrap();
            }

            // A capture group spans distinct camera instances by construction.
            let group_request = json!({ "requestId": "retention-group" });
            let members = [
                ("retention-member-a", "camera-a"),
                ("retention-member-b", "camera-b"),
            ]
            .into_iter()
            .map(|(capture, instance)| {
                let mut member = retention_job(capture, capture);
                member.instance = instance.to_owned();
                member.ledger_key = None;
                member.group_id = Some("retention-group-1".to_owned());
                member
            })
            .collect::<Vec<_>>();
            catalog
                .accept_group(crate::catalog::NewGroup {
                    group_id: "retention-group-1".to_owned(),
                    ledger_key: crate::catalog::LedgerKey::new(
                        "main",
                        "sb/capture-group",
                        "retention-group",
                    )
                    .unwrap(),
                    request_hash: crate::idempotency::canonical_request_hash(&group_request, false)
                        .unwrap(),
                    canonical_request: group_request,
                    origin_correlation_id: None,
                    accepted_at_ms: 1_000,
                    members,
                })
                .await
                .unwrap();
            for (capture, instance, sequence) in [
                ("retention-member-a", "camera-a", 3_u8),
                ("retention-member-b", "camera-b", 4),
            ] {
                catalog.queue_job(capture, 2_000).await.unwrap();
                catalog
                    .commit_terminal(
                        capture,
                        retention_terminal_for(
                            capture,
                            instance,
                            Some("retention-group-1"),
                            sequence,
                        ),
                    )
                    .await
                    .unwrap();
            }
            catalog
                .complete_group(
                    "retention-group-1",
                    crate::model::JobState::Failed,
                    json!({ "succeeded": 0, "failed": 2 }),
                    Some("BACKEND_ERROR".to_owned()),
                    Some("members failed".to_owned()),
                    30_000,
                )
                .await
                .unwrap();

            let ptz_request = json!({ "instance": "camera-a", "requestId": "retention-ptz" });
            let ptz_key =
                crate::catalog::LedgerKey::new("camera-a", "sb/ptz/absolute", "retention-ptz")
                    .unwrap();
            catalog
                .begin_command(
                    ptz_key.clone(),
                    crate::idempotency::canonical_request_hash(&ptz_request, false).unwrap(),
                    ptz_request,
                    1_000,
                )
                .await
                .unwrap();
            catalog
                .complete_command(
                    ptz_key,
                    crate::catalog::LedgerState::Succeeded,
                    json!({ "state": "COMMANDED" }),
                    None,
                    None,
                    2_000,
                )
                .await
                .unwrap();

            for record in catalog.pending_outbox(i64::MAX, 100).await.unwrap() {
                catalog
                    .mark_outbox_delivered(record.id, 35_000)
                    .await
                    .unwrap();
            }
        }

        // Every one of these records used to accumulate forever: the whole retention subsystem was
        // built, unit-tested, and then never called, so the state database grew until the
        // free-space floor rejected every capture.
        #[tokio::test]
        async fn retention_sweep_reclaims_state_past_its_windows_and_reports_the_counts() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let catalog = runtime.catalog.clone();
            seed_retained_state(&catalog).await;

            let now_ms = chrono::Utc::now().timestamp_millis();
            let sweep = runtime
                .retention_sweep(now_ms, RETENTION_BATCH, &CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(sweep.delivered_outbox, 4);
            assert_eq!(sweep.terminal_jobs, 2);
            assert_eq!(sweep.terminal_groups, 1);
            assert_eq!(sweep.command_ledgers, 1);
            assert_eq!(
                sweep.over_limit_jobs, 0,
                "a record count far below maxResultRecords must reclaim nothing"
            );
            assert_eq!(sweep.reclaimed(), 8);

            assert!(catalog.job("retention-a").await.unwrap().is_none());
            assert!(catalog.job("retention-b").await.unwrap().is_none());
            assert!(catalog.job("retention-member-a").await.unwrap().is_none());
            assert!(catalog.job("retention-member-b").await.unwrap().is_none());
            assert!(catalog.group("retention-group-1").await.unwrap().is_none());
            assert_eq!(
                runtime
                    .retention_sweep(now_ms, RETENTION_BATCH, &CancellationToken::new())
                    .await
                    .unwrap(),
                RetentionSweep::default(),
                "a second sweep must find nothing left to reclaim"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn retention_sweep_enforces_the_configured_result_record_maximum() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            configuration.global.state.max_result_records = 1;
            let runtime = runtime(configuration, &directory).await;
            let catalog = runtime.catalog.clone();
            // Keep both jobs inside the time window so only the count limit can reclaim them.
            for (capture, sequence) in [("count-a", 11_u8), ("count-b", 12)] {
                catalog
                    .accept_job(retention_job(capture, capture))
                    .await
                    .unwrap();
                catalog.queue_job(capture, 2_000).await.unwrap();
                catalog
                    .commit_terminal(capture, retention_terminal(capture, "camera-a", sequence))
                    .await
                    .unwrap();
            }
            for record in catalog.pending_outbox(i64::MAX, 100).await.unwrap() {
                catalog
                    .mark_outbox_delivered(record.id, 35_000)
                    .await
                    .unwrap();
            }
            let future_ms = chrono::Utc::now().timestamp_millis();
            let sweep = runtime
                .retention_sweep(future_ms, RETENTION_BATCH, &CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(sweep.over_limit_jobs + sweep.terminal_jobs, 2);
            assert!(catalog.job("count-a").await.unwrap().is_none());
            assert!(catalog.job("count-b").await.unwrap().is_none());
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn retention_sweep_yields_to_cancellation_before_touching_the_catalog() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let catalog = runtime.catalog.clone();
            seed_retained_state(&catalog).await;

            let cancellation = CancellationToken::new();
            cancellation.cancel();
            let sweep = runtime
                .retention_sweep(
                    chrono::Utc::now().timestamp_millis(),
                    RETENTION_BATCH,
                    &cancellation,
                )
                .await
                .unwrap();
            assert_eq!(sweep, RetentionSweep::default());
            assert!(
                catalog.job("retention-a").await.unwrap().is_some(),
                "a cancelled sweep must not keep issuing catalog work"
            );
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn retention_sweep_reclaims_a_backlog_in_bounded_paced_batches() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let catalog = runtime.catalog.clone();
            seed_retained_state(&catalog).await;

            // One row per catalog round trip: the sweep must keep going until the backlog is gone
            // rather than reclaim a single batch and wait an hour, and it must never issue the
            // whole backlog at once against the two-worker pool that carries the capture path.
            let sweep = runtime
                .retention_sweep(
                    chrono::Utc::now().timestamp_millis(),
                    1,
                    &CancellationToken::new(),
                )
                .await
                .unwrap();
            assert_eq!(sweep.delivered_outbox, 4);
            assert_eq!(sweep.terminal_jobs, 2);
            assert_eq!(sweep.reclaimed(), 8);
            assert!(catalog.job("retention-b").await.unwrap().is_none());
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn a_failing_retention_sweep_never_stops_the_periodic_task() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let catalog = runtime.catalog.clone();
            seed_retained_state(&catalog).await;

            // A zero batch is rejected by every prune, so every sweep fails. Retention is a
            // background reclaim: a failing sweep must be logged and retried, never propagated.
            let cancellation = CancellationToken::new();
            let sweeper = tokio::spawn(Arc::clone(&runtime).run_retention(
                cancellation.clone(),
                Duration::from_millis(5),
                0,
            ));
            tokio::time::sleep(Duration::from_millis(60)).await;
            assert!(
                !sweeper.is_finished(),
                "a failing sweep must not terminate the retention task"
            );
            assert!(catalog.job("retention-a").await.unwrap().is_some());
            cancellation.cancel();
            tokio::time::timeout(Duration::from_secs(5), sweeper)
                .await
                .expect("the retention task must still observe cancellation")
                .expect("the retention task must not panic");
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn periodic_retention_reclaims_on_its_interval_and_stops_with_the_runtime() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let catalog = runtime.catalog.clone();
            seed_retained_state(&catalog).await;

            let cancellation = CancellationToken::new();
            let sweeper = tokio::spawn(Arc::clone(&runtime).run_retention(
                cancellation.clone(),
                Duration::from_millis(20),
                RETENTION_BATCH,
            ));
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while catalog.job("retention-a").await.unwrap().is_some() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the periodic sweep never reclaimed a record past its retention window"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // Keep sweeping once the backlog is gone: an idle sweep must be a no-op, not an error
            // and not a reason for the task to stop.
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(!sweeper.is_finished());

            cancellation.cancel();
            tokio::time::timeout(Duration::from_secs(5), sweeper)
                .await
                .expect("a cancelled retention task must stop within the shutdown grace")
                .expect("the retention task must not panic");
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn start_retention_registers_a_cancellable_runtime_task() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let before = runtime.tasks.lock().unwrap().len();
            runtime.start_retention().unwrap();
            assert_eq!(runtime.tasks.lock().unwrap().len(), before + 1);
            // The task parks on the hourly interval; shutdown must still join it immediately.
            tokio::time::timeout(Duration::from_secs(5), runtime.shutdown())
                .await
                .expect("the retention task must observe runtime cancellation");
        }

        // D6: `sb/reconnect` began a ledger row and never completed it.  The row was fenced to
        // OUTCOME_UNKNOWN on the next start, which no DELETE in the catalog can match, so the row
        // was immortal and every retry answered PREVIOUS_OUTCOME_UNKNOWN forever.
        #[tokio::test]
        async fn reconnect_settles_its_ledger_and_the_row_is_reclaimable() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let accepted = runtime
                .reconnect(ReconnectRequest {
                    instance: Some("camera-a".to_string()),
                    request_id: "reconnect-settled".to_string(),
                    reason: None,
                })
                .await
                .unwrap();

            let key = crate::catalog::LedgerKey::new(
                "camera-a",
                crate::catalog::RECONNECT_VERB,
                "reconnect-settled",
            )
            .unwrap();
            let canonical =
                json!({ "instance": "camera-a", "requestId": "reconnect-settled", "reason": null });
            let hash = crate::idempotency::canonical_request_hash(&canonical, false).unwrap();
            let crate::catalog::BeginCommandOutcome::Existing(record) = runtime
                .catalog
                .begin_command(key, hash, canonical, chrono::Utc::now().timestamp_millis())
                .await
                .unwrap()
            else {
                panic!("the reconnect must have created exactly one durable ledger row");
            };
            assert_eq!(record.state, crate::catalog::LedgerState::Succeeded);
            assert_eq!(record.reply, Some(accepted));

            // Past the result-retention window the settled row is reclaimed like any other
            // completed command; an IN_PROGRESS or OUTCOME_UNKNOWN row never would be.
            let future_ms = chrono::Utc::now().timestamp_millis() + 30 * 24 * MILLIS_PER_HOUR;
            let sweep = runtime
                .retention_sweep(future_ms, RETENTION_BATCH, &CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(sweep.command_ledgers, 1);
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn startup_settles_an_interrupted_reconnect_instead_of_fencing_it_forever() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let key = crate::catalog::LedgerKey::new(
                "camera-a",
                crate::catalog::RECONNECT_VERB,
                "reconnect-crash",
            )
            .unwrap();
            let canonical =
                json!({ "instance": "camera-a", "requestId": "reconnect-crash", "reason": null });
            let hash = crate::idempotency::canonical_request_hash(&canonical, false).unwrap();
            let reply =
                json!({ "operationId": "op_prior", "instance": "camera-a", "state": "ACCEPTED" });
            // Exactly the durable state a crash between acceptance and completion leaves behind.
            runtime
                .catalog
                .begin_command(key.clone(), hash, canonical, 1_000)
                .await
                .unwrap();
            runtime
                .catalog
                .record_command_acceptance(key, reply.clone(), 1_001)
                .await
                .unwrap();

            runtime.recover_install_owned().await.unwrap();

            // The retry must return the retained acceptance, not PREVIOUS_OUTCOME_UNKNOWN.
            let retried = runtime
                .reconnect(ReconnectRequest {
                    instance: Some("camera-a".to_string()),
                    request_id: "reconnect-crash".to_string(),
                    reason: None,
                })
                .await
                .unwrap();
            assert_eq!(retried, reply);
            let future_ms = chrono::Utc::now().timestamp_millis() + 30 * 24 * MILLIS_PER_HOUR;
            let sweep = runtime
                .retention_sweep(future_ms, RETENTION_BATCH, &CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(
                sweep.command_ledgers, 1,
                "a reconnect row interrupted by a crash must remain reclaimable"
            );
            runtime.shutdown().await;
        }

        struct TeardownSession {
            capabilities: crate::model::CameraCapabilities,
            stops: Arc<Mutex<Vec<crate::model::PtzRequest>>>,
            closes: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl crate::backend::CameraSession for TeardownSession {
            fn capabilities(&self) -> &crate::model::CameraCapabilities {
                &self.capabilities
            }

            async fn status(&mut self) -> Result<crate::backend::CameraStatus> {
                unreachable!("the shutdown teardown test never reads status")
            }

            async fn capture(
                &mut self,
                _request: crate::backend::CaptureRequest,
            ) -> Result<crate::model::CaptureFrame> {
                unreachable!("the shutdown teardown test never captures")
            }

            async fn ptz(
                &mut self,
                request: crate::model::PtzRequest,
            ) -> Result<crate::model::PtzResult> {
                self.stops.lock().unwrap().push(request);
                Ok(crate::model::PtzResult::Commanded)
            }

            async fn close(&mut self) -> Result<()> {
                self.closes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        // B4: the supervisor used to `abort()` the actor on cancellation. The actor holds a CHILD
        // of that token, so it was already running its teardown; the abort dropped that future at
        // its first await point, which meant a SIGTERM during a pan left the camera panning and no
        // session was ever closed.
        #[tokio::test]
        async fn cancelled_actor_runs_its_safety_stop_and_session_close_within_the_grace() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
            let stops = Arc::new(Mutex::new(Vec::new()));
            let closes = Arc::new(AtomicUsize::new(0));
            let session = TeardownSession {
                capabilities: crate::model::CameraCapabilities {
                    capture_modes: vec![crate::model::CaptureMode::Simulated],
                    pixel_formats: vec![crate::model::PixelFormat::Rgb8],
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
                },
                stops: Arc::clone(&stops),
                closes: Arc::clone(&closes),
            };
            let (actor, handle) = CameraActor::new(
                "camera-a",
                Box::new(session),
                runtime.engine("camera-a").unwrap(),
                2,
                2,
                runtime.scheduler.capacity_signal(),
            )
            .unwrap();
            handle
                .safety_stop(crate::admission::SafetyStop {
                    pan: true,
                    tilt: true,
                    zoom: false,
                    deadline: tokio::time::Instant::now() + Duration::from_secs(5),
                })
                .unwrap();

            // Reproduce the supervisor's exact shutdown ordering: the component token is tripped
            // before the actor task has ever been polled.
            let actor_cancellation = runtime.cancellation.child_token();
            runtime.cancellation.cancel();
            let mut actor_task = tokio::spawn(actor.run(actor_cancellation));

            assert!(
                join_actor_within_grace(&mut actor_task, Duration::from_secs(10)).await,
                "the actor must complete its own teardown inside the grace budget"
            );
            let observed = stops.lock().unwrap().clone();
            assert_eq!(
                observed,
                vec![crate::model::PtzRequest::Stop {
                    pan: true,
                    tilt: true,
                    zoom: false
                }],
                "the queued shutdown safety stop must reach the transport"
            );
            assert_eq!(
                closes.load(Ordering::SeqCst),
                1,
                "the protocol session must be closed instead of leaked server-side"
            );
        }

        /// The two verbs, through the command layer that actually serves them.
        ///
        /// Also pins the ledger: a break-glass drain cancels durable work, so a retried request must
        /// return the ORIGINAL outcome rather than reach for a second wave of work the operator never
        /// saw.
        #[tokio::test]
        async fn the_queue_verbs_answer_and_the_drain_is_idempotent() {
            let directory = TempDir::new().unwrap();
            let (runtime, metrics) =
                runtime_with_metrics(config(directory.path(), &["camera-a"], false), &directory)
                    .await;

            for index in 0..2 {
                runtime
                    .submit_capture(
                        "camera-a".to_string(),
                        format!("verb-backlog-{index}"),
                        None,
                        None,
                        serde_json::Map::new(),
                        format!("verb-correlation-{index}"),
                        "sb/capture-submit",
                        crate::admission::CapturePriority::Submitted,
                    )
                    .await
                    .unwrap();
            }

            let status = runtime
                .queue_status_command(commands::QueueStatusRequest { instance: None })
                .await
                .expect("sb/queue-status must answer");
            assert_eq!(status["durableBacklog"], serde_json::json!(2));
            assert_eq!(status["dispatchQueued"], serde_json::json!(2));
            assert_eq!(
                status["cameras"][0]["instance"],
                serde_json::json!("camera-a")
            );

            // The metric sample must report the same figures the operator was just shown.
            let sampled = runtime.sample_queue_metric().await.unwrap();
            assert_eq!(sampled.get("durableBacklog"), Some(&2.0));
            assert_eq!(sampled.get("camerasConfigured"), Some(&1.0));
            assert_eq!(
                sampled.get("camerasOnline"),
                Some(&0.0),
                "no supervisor is running, so no camera is online"
            );
            runtime.metrics.sample_queue(sampled).await;
            assert_eq!(
                metrics.counts(crate::observability::QUEUE_METRIC, "durableBacklog"),
                2.0,
                "and the sample must actually reach the metric service"
            );

            let drain = commands::QueueClearRequest {
                request_id: "drain-1".to_string(),
                instance: Some("camera-a".to_string()),
                all_cameras: false,
                include_in_flight: false,
                reason: Some("line stopped".to_string()),
            };
            let cleared = runtime
                .queue_clear_command(drain.clone())
                .await
                .expect("sb/queue-clear must drain");
            assert_eq!(cleared["cancelled"], serde_json::json!(2));

            // Replayed: the ledger must return the first answer, not cancel a second wave.
            let replayed = runtime
                .queue_clear_command(drain)
                .await
                .expect("a retried drain must be idempotent");
            assert_eq!(
                replayed, cleared,
                "a retried break-glass drain must return the original outcome"
            );

            let after = runtime
                .queue_status_command(commands::QueueStatusRequest {
                    instance: Some("camera-a".to_string()),
                })
                .await
                .unwrap();
            assert_eq!(after["durableBacklog"], serde_json::json!(0));
            assert_eq!(after["dispatchQueued"], serde_json::json!(0));
        }

        /// A superseded supervisor's state write is dropped on purpose -- and must say so.
        ///
        /// D5: every one of the eight supervisor call sites discarded this with `let _ =`, so the
        /// generation fence rejecting a write and the registry lock being poisoned looked exactly
        /// like a successful update.
        #[tokio::test]
        async fn a_superseded_generation_cannot_publish_camera_state() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

            runtime.publish_camera_state(
                "camera-a",
                7,
                CameraConnectionState::Online,
                None,
                None,
                chrono::Utc::now(),
            );
            assert_eq!(
                runtime.registry.snapshot("camera-a").unwrap().state,
                CameraConnectionState::Online
            );

            // An older generation must not be able to drag the camera backwards.
            runtime.publish_camera_state(
                "camera-a",
                6,
                CameraConnectionState::Backoff,
                None,
                Some(CameraStatusError {
                    code: "BACKEND_ERROR".to_string(),
                    message: "stale".to_string(),
                    observed_at: chrono::Utc::now(),
                }),
                chrono::Utc::now(),
            );
            assert_eq!(
                runtime.registry.snapshot("camera-a").unwrap().state,
                CameraConnectionState::Online,
                "the generation fence must drop a superseded supervisor's write"
            );

            // A camera that is not configured is likewise a no-op rather than a panic.
            runtime.publish_camera_state(
                "camera-gone",
                9,
                CameraConnectionState::Offline,
                None,
                None,
                chrono::Utc::now(),
            );
        }

        /// A capture must be counted even when the component is too busy to publish its events.
        ///
        /// The lifecycle-event publish is best-effort by design: it takes a permit from a bounded
        /// pool and gives up if none is free, so that a slow event consumer can never stall a
        /// capture. That bound is right for EVENTS and catastrophic for a COUNTER. The counters
        /// originally rode inside that publish, which meant they were dropped exactly when the
        /// component was busiest -- under-reporting precisely the overload an operator would be
        /// staring at -- and raced the capture's own terminal state, which is what CI caught: the
        /// same commit passed on one runner and failed on another.
        #[tokio::test]
        async fn captures_are_counted_even_when_every_event_slot_is_taken() {
            let directory = TempDir::new().unwrap();
            let (runtime, metrics) =
                runtime_with_metrics(config(directory.path(), &["camera-a"], false), &directory)
                    .await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            // Starve the publish path completely: not one lifecycle event can be emitted.
            let _permits = Arc::clone(&runtime.waiters.lifecycle_event_slots)
                .acquire_many_owned(
                    u32::try_from(MAX_LIFECYCLE_EVENT_PUBLISHES)
                        .expect("the fixed lifecycle capacity fits Tokio's semaphore API"),
                )
                .await
                .expect("the test holds every detached lifecycle permit");

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "counted-under-starvation".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "starvation-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let capture_id = match accepted {
                crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
                other => panic!("expected a newly accepted capture, got {other:?}"),
            };
            let terminal = wait_for_terminal(&runtime, &capture_id).await;
            assert_eq!(terminal.state, crate::model::JobState::Succeeded);

            let counted = crate::observability::CAPTURE_METRIC;
            assert_eq!(
                metrics.counts(counted, "queued"),
                1.0,
                "the capture happened; a saturated EVENT pipe must not stop it being COUNTED"
            );
            assert_eq!(metrics.counts(counted, "started"), 1.0);
            assert_eq!(
                metrics.counts(counted, "succeeded"),
                1.0,
                "and its outcome is the number an operator watches -- it must survive the overload                  that makes them look"
            );

            runtime.shutdown().await;
        }

        /// Q5: camera presence is pushed, not just polled.
        ///
        /// A camera's state lived in the registry and could be learned only by asking -- `sb/list`,
        /// `sb/status`. Nothing was ever published, so a consumer that wanted to know a camera had
        /// dropped had to poll for it. The assumption that camera connectivity already reached the
        /// standard health surface did not hold: EdgeCommons ships the per-instance connectivity
        /// provider for exactly this, and nothing was registered against it.
        #[tokio::test]
        async fn every_camera_reports_its_reachability_to_the_heartbeat() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(
                config(directory.path(), &["camera-a", "camera-b"], false),
                &directory,
            )
            .await;

            // Before any supervisor runs, both cameras are configured and neither is reachable.
            let cold = runtime.camera_connectivity();
            assert_eq!(
                cold.len(),
                2,
                "every configured camera must be reported, not just live ones"
            );
            assert!(
                cold.iter().all(|camera| !camera.connected),
                "a camera that has never connected must not be reported as connected"
            );
            assert!(
                cold.iter()
                    .all(|camera| camera.state.as_deref() == Some("OFFLINE")),
                "a camera that is down must say WHICH kind of down: BACKOFF and CONNECTING are both                  `connected: false`, and an operator deciding whether to intervene needs to know which"
            );
            assert!(
                cold.iter()
                    .all(|camera| camera.attributes.contains_key("backend")
                        && camera.attributes.contains_key("generation")),
                "the open bag carries what only a camera adapter understands"
            );

            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let warm = runtime.camera_connectivity();
            let connected = warm
                .iter()
                .find(|camera| camera.instance == "camera-a")
                .expect("camera-a must still be reported");
            assert!(
                connected.connected,
                "an online camera must be reported as connected"
            );
            assert_eq!(
                connected.state.as_deref(),
                Some("ONLINE"),
                "and it must publish its own condition token, not only the normalized flag"
            );
            assert!(
                connected.detail.is_none(),
                "a healthy camera has nothing to explain: this rides a keepalive published every                  few seconds, for every camera, forever"
            );
            assert!(
                warm.iter()
                    .find(|camera| camera.instance == "camera-b")
                    .is_some_and(|camera| !camera.connected),
                "and the camera that never started must still be reported as down"
            );

            runtime.shutdown().await;
        }

        /// Q2: the component emitted no metrics at all.
        ///
        /// There was not one call site for `metrics()`, `MetricBuilder`, or `MetricService` anywhere
        /// in the crate. Every number an operator would want -- what is queued, what is running, what
        /// succeeded, what failed -- existed inside the process and left no trace outside it. A
        /// failed capture was a log line.
        ///
        /// So the assertion that matters is not that the numbers are right, it is that the wiring
        /// exists: a metric nobody emits is indistinguishable from a metric that does not exist, and
        /// nothing in the build could tell the difference.
        #[tokio::test]
        async fn captures_are_counted_as_they_happen() {
            let directory = TempDir::new().unwrap();
            let (runtime, metrics) =
                runtime_with_metrics(config(directory.path(), &["camera-a"], false), &directory)
                    .await;

            {
                use edgecommons::metrics::MetricService as _;
                assert!(
                    metrics.is_metric_defined(crate::observability::CAPTURE_METRIC)
                        && metrics.is_metric_defined(crate::observability::QUEUE_METRIC),
                    "both metrics must be defined at startup, not on first use"
                );
            }

            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "counted".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "counted-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let capture_id = match accepted {
                crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
                other => panic!("expected a newly accepted capture, got {other:?}"),
            };
            let terminal = wait_for_terminal(&runtime, &capture_id).await;
            assert_eq!(terminal.state, crate::model::JobState::Succeeded);

            let counted = crate::observability::CAPTURE_METRIC;
            assert_eq!(
                metrics.counts(counted, "queued"),
                1.0,
                "an accepted capture must be counted when it is accepted"
            );
            assert_eq!(
                metrics.counts(counted, "started"),
                1.0,
                "and again when it actually starts doing physical work"
            );
            assert_eq!(
                metrics.counts(counted, "succeeded"),
                1.0,
                "and its outcome must be counted -- this is the number an operator watches"
            );
            assert_eq!(
                metrics.counts(counted, "failed"),
                0.0,
                "a capture that succeeded must not also be counted as a failure"
            );

            runtime.shutdown().await;
        }

        /// Q3/Q4: the operator can finally SEE the queue, and drain it.
        ///
        /// Every number here already existed. `AdmissionSnapshot` was compiled only into
        /// `cfg(all(test, linux, standalone, onvif, capacity-harness))`, so the sole consumer was the
        /// capacity harness -- the observability had been built for the test rather than for the
        /// person holding the pager, and an operator watching a backlog run away had no way to ask
        /// how deep it was or to stop it.
        #[tokio::test]
        async fn queue_status_reports_the_backlog_and_queue_clear_drains_it() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            configuration.global.limits.max_queued_captures_per_camera = 4;
            let runtime = runtime(configuration, &directory).await;

            // No supervisor: the captures sit exactly where a backlog for an offline camera sits.
            for index in 0..3 {
                runtime
                    .submit_capture(
                        "camera-a".to_string(),
                        format!("backlog-{index}"),
                        None,
                        None,
                        serde_json::Map::new(),
                        format!("backlog-correlation-{index}"),
                        "sb/capture-submit",
                        crate::admission::CapturePriority::Submitted,
                    )
                    .await
                    .expect("captures must be accepted");
            }

            let status = runtime.queue_status(None).await.unwrap();
            assert_eq!(
                status.durable_backlog, 3,
                "the durable backlog is what survives a restart"
            );
            assert_eq!(status.durable_in_flight, 0);
            assert_eq!(
                status.dispatch_queued, 3,
                "and the dispatcher is holding all three"
            );
            assert_eq!(
                status.cameras,
                vec![CameraQueueDepth {
                    instance: "camera-a".to_string(),
                    queued: 3,
                    capacity: 4,
                }],
                "a camera at queued == capacity is the one answering QUEUE_FULL, so both are reported"
            );
            assert_eq!(
                status.limits.max_concurrent_captures, status.admission.available_acquisitions,
                "nothing is acquiring yet, so every acquisition permit must still be free"
            );

            // Break glass.
            let outcome = runtime
                .clear_queue(None, false, "operator drain".to_string())
                .await
                .unwrap();
            assert_eq!(outcome.cancelled, 3);
            assert!(
                outcome.failed.is_empty(),
                "the drain must not leave work behind silently"
            );

            let drained = runtime.queue_status(None).await.unwrap();
            assert_eq!(
                drained.durable_backlog, 0,
                "the durable backlog is gone, not just forgotten"
            );
            assert_eq!(
                drained.dispatch_queued, 0,
                "and the descriptors must not still be occupying the camera's queue slots"
            );
        }

        /// The drain reaches durable work regardless of whether an unknown camera is named.
        /// Rebasing a deadline must not disarm it: a capture that runs out of time still fails.
        ///
        /// The terminal-deadline task re-reads its deadline instead of firing on the one it was
        /// spawned with, because a capture's clocks are rebased when a camera finally takes it. A
        /// re-reading task that never fires would be worse than the stale one it replaced: captures
        /// would wait forever instead of dying early. It still fires -- just on the clock in force.
        #[tokio::test]
        async fn a_capture_that_runs_out_of_time_while_it_waits_still_fails() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            // The camera never comes online, so this capture waits until its terminal clock runs out.
            // The stage clocks must fit inside the terminal one, so they come down with it.
            configuration.global.timeouts.job_terminal_ms = 1_000;
            configuration.global.timeouts.capture_ms = 500;
            configuration.global.timeouts.encode_ms = 500;
            configuration.global.timeouts.persist_ms = 500;
            let runtime = runtime(configuration, &directory).await;

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "waits-forever".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "timeout-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let capture = match accepted {
                crate::catalog::AcceptJobOutcome::Inserted(record)
                | crate::catalog::AcceptJobOutcome::Existing(record) => record.capture_id,
                crate::catalog::AcceptJobOutcome::Conflict => panic!("unexpected ledger conflict"),
            };

            let record =
                wait_for_terminal_within(&runtime, &capture, Duration::from_secs(20)).await;
            assert_eq!(record.state, crate::model::JobState::Failed);
            assert_eq!(
                record.error_code.as_deref(),
                Some(crate::ErrorCode::CaptureTimeout.as_str()),
                "a capture nobody can run must still be retired by its deadline"
            );
        }

        /// The other half of the rule: a capture that really IS retired must be dropped.
        ///
        /// The requeue path exists so a store hiccup cannot destroy a capture. It must not become a
        /// path that resurrects one. A cancelled capture is no longer QUEUED, the catalog says so with
        /// an invariant error rather than a store error, and the scheduler must let it go -- otherwise
        /// it pops, fails to rebase, requeues, and spins on a capture nobody is waiting for.
        #[tokio::test]
        async fn a_capture_already_retired_is_dropped_not_requeued() {
            let directory = TempDir::new().unwrap();
            let configuration = config(directory.path(), &["camera-a"], false);
            let runtime = runtime(configuration, &directory).await;

            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "retired-before-dispatch".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "retired-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .unwrap();
            let capture = match accepted {
                crate::catalog::AcceptJobOutcome::Inserted(record)
                | crate::catalog::AcceptJobOutcome::Existing(record) => record.capture_id,
                crate::catalog::AcceptJobOutcome::Conflict => panic!("unexpected ledger conflict"),
            };
            assert_eq!(runtime.scheduler.pending(), 1);

            // Retired while it waited, before any camera could take it -- through the real cancel
            // command, not a hand-built terminal row.
            runtime
                .cancel_capture(CancelRequest {
                    request_id: "retire-before-dispatch".to_string(),
                    capture_id: Some(capture.clone()),
                    capture_group_id: None,
                    reason: Some("retired while queued".to_string()),
                })
                .await
                .unwrap();

            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;
            tokio::time::sleep(Duration::from_millis(500)).await;

            assert_eq!(
                runtime.scheduler.pending(),
                0,
                "a cancelled capture is not owed a run and must not be put back on the queue"
            );
            let record = runtime.catalog.job(&capture).await.unwrap().unwrap();
            assert_eq!(record.state, crate::model::JobState::Cancelled);
        }

        /// A group schedule resumes from its durable cursor, and skips what it missed.
        ///
        /// The cursor is what tells a restarted component the difference between an occurrence it
        /// missed while it was down and one that has not come due. With `misfirePolicy: skip`, an
        /// occurrence older than the misfire grace is consumed and discarded rather than fired late.
        #[tokio::test]
        async fn a_group_schedule_resumes_from_its_cursor_and_skips_a_misfire() {
            let directory = TempDir::new().unwrap();
            let cameras = ["camera-a", "camera-b"];
            let mut configuration = config(directory.path(), &cameras, false);
            let mut schedule =
                group_schedule("line-a-sync", &cameras, crate::config::OverlapPolicy::Skip);
            // Hourly, so the occurrences the seeded cursor missed are hours old -- far past the
            // misfire grace -- and the next one is not due for the rest of the test.
            schedule.cron = "0 0 * * * *".to_string();
            configuration.global.capture_group_schedules = vec![schedule];
            let runtime = runtime(configuration, &directory).await;
            for camera in cameras {
                runtime
                    .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
                    .unwrap();
                wait_for_online(&runtime, camera).await;
            }

            // The component was down for three hours.
            let stale = chrono::Utc::now() - chrono::Duration::hours(3);
            runtime
                .catalog
                .record_group_schedule_occurrence(
                    "line-a-sync",
                    stale.timestamp_millis(),
                    None,
                    stale.timestamp_millis(),
                )
                .await
                .unwrap();

            runtime.start_schedulers().unwrap();
            tokio::time::sleep(Duration::from_millis(600)).await;

            let cursor = runtime
                .catalog
                .group_schedule_cursor("line-a-sync")
                .await
                .unwrap()
                .expect("the schedule consumed the occurrences it missed");
            assert!(
                cursor.intended_fire_time_ms > stale.timestamp_millis(),
                "a missed occurrence is consumed, not left to be fired again on the next tick"
            );
            assert_eq!(
                cursor.last_group_id, None,
                "a misfire is skipped -- hours-late captures must not be fired at a live line"
            );
        }

        /// A group schedule that cannot read its recovery cursor does not run on a guessed one.
        ///
        /// The cursor is what separates an occurrence the component missed while it was down from one
        /// that has not come due. Without it the schedule cannot tell those apart, so it stops rather
        /// than firing a synchronised capture at a line on a cursor it invented.
        #[tokio::test]
        async fn a_group_schedule_that_cannot_read_its_cursor_does_not_run() {
            let directory = TempDir::new().unwrap();
            let cameras = ["camera-a", "camera-b"];
            let mut configuration = config(directory.path(), &cameras, false);
            configuration.global.capture_group_schedules = vec![group_schedule(
                "line-a-sync",
                &cameras,
                crate::config::OverlapPolicy::Skip,
            )];
            let runtime = runtime(configuration, &directory).await;
            for camera in cameras {
                runtime
                    .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
                    .unwrap();
                wait_for_online(&runtime, camera).await;
            }

            let database = directory
                .path()
                .join("state")
                .join("camera-adapter.sqlite3");
            let break_store = rusqlite::Connection::open(&database).unwrap();
            break_store
                .execute_batch(
                    "ALTER TABLE group_schedule_cursors RENAME TO group_schedule_cursors_unavailable",
                )
                .unwrap();

            runtime.start_schedulers().unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;

            break_store
                .execute_batch(
                    "ALTER TABLE group_schedule_cursors_unavailable RENAME TO group_schedule_cursors",
                )
                .unwrap();
            assert!(
                runtime
                    .catalog
                    .group_schedule_cursor("line-a-sync")
                    .await
                    .unwrap()
                    .is_none(),
                "a schedule that could not read its cursor must not have fired anything"
            );
        }

        /// A catalog that cannot answer "is the previous group still running?" fails CLOSED.
        ///
        /// Firing a second synchronised group on top of one that may still be moving cameras is worse
        /// than missing a cycle, so an unreadable catalog skips the occurrence rather than assuming
        /// the coast is clear.
        #[tokio::test]
        async fn group_overlap_fails_closed_when_the_catalog_cannot_answer() {
            let directory = TempDir::new().unwrap();
            let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
            let runtime = runtime(configuration, &directory).await;

            let database = directory
                .path()
                .join("state")
                .join("camera-adapter.sqlite3");
            let break_store = rusqlite::Connection::open(&database).unwrap();
            break_store
                .execute_batch("ALTER TABLE capture_groups RENAME TO capture_groups_unavailable")
                .unwrap();

            assert!(
                runtime
                    .group_schedule_overlaps(Some("grp_unknowable"))
                    .await,
                "a catalog that cannot answer is not permission to fire another group"
            );

            // And a cursor the store cannot write is survivable: the command ledger, not the cursor,
            // is what makes an occurrence exactly-once, so the schedule logs and carries on rather
            // than dying on a recovery hint.
            break_store
                .execute_batch(
                    "ALTER TABLE group_schedule_cursors RENAME TO group_schedule_cursors_unavailable",
                )
                .unwrap();
            runtime
                .record_group_occurrence("line-a-sync", chrono::Utc::now(), None)
                .await;

            break_store
                .execute_batch(
                    "ALTER TABLE group_schedule_cursors_unavailable RENAME TO group_schedule_cursors",
                )
                .unwrap();
            break_store
                .execute_batch("ALTER TABLE capture_groups_unavailable RENAME TO capture_groups")
                .unwrap();
        }

        /// An occurrence of a schedule that has been disabled or removed is not submitted.
        #[tokio::test]
        async fn an_occurrence_of_a_removed_group_schedule_is_not_submitted() {
            let directory = TempDir::new().unwrap();
            let cameras = ["camera-a", "camera-b"];
            let mut configuration = config(directory.path(), &cameras, false);
            let mut disabled =
                group_schedule("line-a-sync", &cameras, crate::config::OverlapPolicy::Skip);
            disabled.enabled = false;
            configuration.global.capture_group_schedules = vec![disabled];
            let runtime = runtime(configuration, &directory).await;

            let fire_time = chrono::DateTime::from_timestamp_millis(1_752_000_000_000).unwrap();
            let occurrence = ScheduleOccurrence {
                scope: crate::scheduler::ScheduleScope::Group("line-a-sync".to_string()),
                schedule_id: "line-a-sync".to_string(),
                intended_fire_time: fire_time,
                admit_at: fire_time,
                jitter: Duration::ZERO,
            };
            let error = runtime
                .submit_scheduled_group(&occurrence)
                .await
                .expect_err("a disabled schedule must not fire");
            assert_eq!(error.code(), crate::ErrorCode::InvalidRequest);

            let unknown = ScheduleOccurrence {
                scope: crate::scheduler::ScheduleScope::Group("gone".to_string()),
                schedule_id: "gone".to_string(),
                intended_fire_time: fire_time,
                admit_at: fire_time,
                jitter: Duration::ZERO,
            };
            assert!(
                runtime.submit_scheduled_group(&unknown).await.is_err(),
                "a schedule removed by a reload must not fire"
            );

            // A camera schedule occurrence is not a group, and must not be submitted as one.
            let camera_scoped = ScheduleOccurrence {
                scope: crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
                schedule_id: "minute".to_string(),
                intended_fire_time: fire_time,
                admit_at: fire_time,
                jitter: Duration::ZERO,
            };
            assert!(runtime.submit_scheduled(&camera_scoped).await.is_err());

            // And the converse: a GROUP occurrence has no single camera, so the camera path must
            // refuse it rather than pick one.
            assert!(
                runtime.submit_scheduled(&occurrence).await.is_err(),
                "a group occurrence cannot be submitted as a single-camera capture"
            );
        }

        /// A capture the STORE could not rebase goes back on the queue -- it is not destroyed.
        ///
        /// This is B5 wearing a different hat, and the fleet queue rebuilt it. The scheduler pops a
        /// capture, rebases its clocks onto the moment a camera actually took it, and hands it over.
        /// When that durable write failed, the code treated it as "this capture is already terminal
        /// or gone" and dropped the descriptor. It is not gone: a transient `SQLITE_BUSY` under load
        /// says nothing about the capture, only about the store. The durable row stays QUEUED, the
        /// only in-memory thing that could have driven it has been destroyed, and the capture waits
        /// for a deadline that will fail it. It cost a real member of a real group, and it only
        /// showed up under load -- which is exactly when a contended catalog returns BUSY.
        ///
        /// The store failure here is real, not injected into production code: the `jobs` table is
        /// renamed out from under the catalog, so the rebase UPDATE fails with a genuine
        /// `CameraError::Sqlite`, and is then renamed back. A store that stops answering and starts
        /// again is precisely the case that must not lose work.
        #[tokio::test]
        async fn a_capture_the_store_could_not_rebase_is_requeued_not_destroyed() {
            let directory = TempDir::new().unwrap();
            let configuration = config(directory.path(), &["camera-a"], false);
            let runtime = runtime(configuration, &directory).await;

            // The camera is deliberately offline, so the capture is accepted, durably QUEUED, and
            // parked in the fleet queue with nothing yet able to take it.
            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "stranded-by-the-store".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "store-failure-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .expect("the capture is accepted while the camera is offline");
            let capture = match accepted {
                crate::catalog::AcceptJobOutcome::Inserted(record)
                | crate::catalog::AcceptJobOutcome::Existing(record) => record.capture_id,
                crate::catalog::AcceptJobOutcome::Conflict => panic!("unexpected ledger conflict"),
            };
            assert_eq!(runtime.scheduler.pending(), 1);

            // The store stops being able to serve the rebase. Done through a SEPARATE connection to
            // the same database, so no production type grows a fault-injection hook for this.
            let database = directory
                .path()
                .join("state")
                .join("camera-adapter.sqlite3");
            let break_store = rusqlite::Connection::open(&database).unwrap();
            break_store
                .execute_batch("ALTER TABLE jobs RENAME TO jobs_unavailable")
                .unwrap();

            // The camera comes online. The scheduler pops the capture, fails to rebase it, and must
            // put it back: with the defect it drops the descriptor here and `pending()` falls to 0
            // for the rest of the process.
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;
            tokio::time::sleep(Duration::from_millis(400)).await;
            assert_eq!(
                runtime.scheduler.pending(),
                1,
                "a capture the store could not rebase is still owed a run and must stay queued"
            );

            // The store recovers. The capture must actually run -- being requeued is only half of
            // the promise.
            break_store
                .execute_batch("ALTER TABLE jobs_unavailable RENAME TO jobs")
                .unwrap();
            drop(break_store);

            let record =
                wait_for_terminal_within(&runtime, &capture, Duration::from_secs(20)).await;
            assert_eq!(
                record.state,
                crate::model::JobState::Succeeded,
                "once the store recovers, the capture it could not rebase must still run"
            );
        }

        #[tokio::test]
        async fn queue_status_and_clear_reject_an_unknown_camera() {
            let directory = TempDir::new().unwrap();
            let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

            assert_eq!(
                runtime
                    .queue_status(Some("camera-nope".to_string()))
                    .await
                    .expect_err("an unknown camera must be rejected, not reported as empty")
                    .code(),
                crate::ErrorCode::UnknownInstance
            );
            assert_eq!(
                runtime
                    .clear_queue(Some("camera-nope".to_string()), false, "drain".to_string())
                    .await
                    .expect_err("an unknown camera must be rejected")
                    .code(),
                crate::ErrorCode::UnknownInstance
            );
        }

        /// G1: an oversized group takes LONGER. It does not partially fail.
        ///
        /// `submit_group` used to build every member and hand them all to their cameras at once. A
        /// group larger than the component could serve did not degrade -- it broke: the members that
        /// could not be dispatched immediately sat with their 30-second capture clock already
        /// running, and died of CAPTURE_TIMEOUT the moment a camera was free to take them. An
        /// oversized concurrent request silently became "most of your members failed", which the
        /// group contract -- all-or-nothing acceptance, one collated result -- does not permit.
        ///
        /// Nothing in this fix is a wave scheduler. A wave is "as many as capacity allows", and that
        /// is what a central queue does by construction: the members beyond capacity simply wait, and
        /// each one's clocks start when a camera actually takes it. The whole of G1 is Q1 plus
        /// rebasing.
        ///
        /// This group is deliberately wider than the component's execution capacity: four cameras
        /// against a single concurrent-capture permit. Every member must still succeed.
        fn group_schedule(
            id: &str,
            cameras: &[&str],
            overlap: crate::config::OverlapPolicy,
        ) -> crate::config::CaptureGroupScheduleConfig {
            crate::config::CaptureGroupScheduleConfig {
                id: id.to_string(),
                enabled: true,
                // Every second: the loop polls at 200 ms, so a test does not wait on a wall clock.
                cron: "* * * * * *".to_string(),
                timezone: "UTC".to_string(),
                instances: cameras.iter().map(|camera| (*camera).to_string()).collect(),
                capture_profile: None,
                profile_overrides: BTreeMap::new(),
                misfire_policy: crate::config::MisfirePolicy::Skip,
                overlap_policy: overlap,
                jitter_seconds: 0,
                timeout_ms: None,
            }
        }

        /// Waits for a group schedule to admit an occurrence, and returns the group it admitted.
        ///
        /// Read from the schedule's own durable cursor, which is also the thing that has to survive
        /// a restart -- so a green wait here is evidence for both.
        async fn wait_for_scheduled_group(runtime: &CameraRuntime, schedule_id: &str) -> String {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                if let Ok(Some(cursor)) = runtime.catalog.group_schedule_cursor(schedule_id).await {
                    if let Some(group_id) = cursor.last_group_id {
                        return group_id;
                    }
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "group schedule '{schedule_id}' never admitted an occurrence"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }

        /// G2: a cron fires ONE synchronised group across several cameras.
        ///
        /// Before this, grouping existed only on the command path: a synchronised multi-camera
        /// capture could not be scheduled at all. The assertions that matter are that the members
        /// are exactly the schedule's cameras -- one capture each, no duplicates, none missing --
        /// and that it arrives as a single durable group rather than N unrelated captures that
        /// happened to fire at once.
        #[tokio::test]
        async fn a_group_schedule_fires_one_synchronised_group_across_its_cameras() {
            let directory = TempDir::new().unwrap();
            let cameras = ["camera-a", "camera-b", "camera-c"];
            let mut configuration = config(directory.path(), &cameras, false);
            configuration.global.capture_group_schedules = vec![group_schedule(
                "line-a-sync",
                &cameras,
                crate::config::OverlapPolicy::Skip,
            )];
            let runtime = runtime(configuration, &directory).await;
            for camera in cameras {
                runtime
                    .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
                    .unwrap();
                wait_for_online(&runtime, camera).await;
            }
            runtime.start_schedulers().unwrap();

            let group_id = wait_for_scheduled_group(&runtime, "line-a-sync").await;
            let terminal =
                wait_for_group_terminal_within(&runtime, &group_id, Duration::from_secs(20)).await;

            let mut members: Vec<_> = terminal
                .members
                .iter()
                .map(|member| member.instance.clone())
                .collect();
            members.sort();
            assert_eq!(
                members,
                vec!["camera-a", "camera-b", "camera-c"],
                "a scheduled group captures exactly its configured cameras, once each"
            );
            assert!(
                terminal
                    .members
                    .iter()
                    .all(|member| member.state == crate::model::JobState::Succeeded),
                "every member of a scheduled group must succeed: {:?}",
                terminal
                    .members
                    .iter()
                    .map(|member| member.state)
                    .collect::<Vec<_>>()
            );
        }

        /// A group schedule holds an occurrence back while its previous group is still running.
        ///
        /// The end-to-end form of the overlap rule: with a cron firing every second and captures that
        /// take much longer than that, the schedule must produce ONE group and then wait, rather than
        /// piling a new synchronised capture onto cameras that are still working through the last one.
        #[tokio::test]
        async fn a_group_schedule_does_not_stack_groups_on_cameras_still_working() {
            let directory = TempDir::new().unwrap();
            let cameras = ["camera-a", "camera-b", "camera-c"];
            let mut configuration = config(directory.path(), &cameras, false);
            configuration.global.capture_group_schedules = vec![group_schedule(
                "line-a-sync",
                &cameras,
                crate::config::OverlapPolicy::Skip,
            )];
            // One at a time, 700 ms each: the group needs ~2.1 s, and the cron comes round every 1 s.
            configuration.global.limits.max_concurrent_captures = 1;
            configuration.global.limits.max_in_flight_bytes =
                configuration.global.limits.max_frame_bytes_per_camera;
            for camera in &mut configuration.instances {
                if let crate::config::BackendConfig::Sim(sim) = &mut camera.backend {
                    sim.capture_delay_ms = 700;
                }
            }
            let runtime = runtime(configuration, &directory).await;
            for camera in cameras {
                runtime
                    .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
                    .unwrap();
                wait_for_online(&runtime, camera).await;
            }
            runtime.start_schedulers().unwrap();

            let first = wait_for_scheduled_group(&runtime, "line-a-sync").await;

            // While that group works, the cron comes round repeatedly. Every one of those occurrences
            // must be skipped, and the cursor must still point at the SAME group.
            tokio::time::sleep(Duration::from_millis(1_200)).await;
            let cursor = runtime
                .catalog
                .group_schedule_cursor("line-a-sync")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                cursor.last_group_id.as_deref(),
                Some(first.as_str()),
                "an occurrence that fires while the previous group is still running is skipped"
            );

            let terminal =
                wait_for_group_terminal_within(&runtime, &first, Duration::from_secs(20)).await;
            assert_eq!(terminal.members.len(), 3);
            assert!(
                terminal
                    .members
                    .iter()
                    .all(|member| member.state == crate::model::JobState::Succeeded)
            );
        }

        /// G2: the same occurrence submitted twice is the same group, not two.
        ///
        /// This is what makes a scheduled group exactly-once across a crash. The request id is
        /// DERIVED from the schedule and the intended fire time, so a component that dies between
        /// submitting an occurrence and recording that it did re-submits it on restart and is handed
        /// back the group it already accepted. Give the occurrence a random request id instead --
        /// the obvious thing to write -- and every restart inside a cron period duplicates the
        /// group, and the cameras are captured twice.
        #[tokio::test]
        async fn resubmitting_an_occurrence_returns_the_group_it_already_accepted() {
            let directory = TempDir::new().unwrap();
            let cameras = ["camera-a", "camera-b"];
            let mut configuration = config(directory.path(), &cameras, false);
            configuration.global.capture_group_schedules = vec![group_schedule(
                "line-a-sync",
                &cameras,
                crate::config::OverlapPolicy::Skip,
            )];
            let runtime = runtime(configuration, &directory).await;
            for camera in cameras {
                runtime
                    .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
                    .unwrap();
                wait_for_online(&runtime, camera).await;
            }

            let fire_time = chrono::DateTime::from_timestamp_millis(1_752_000_000_000).unwrap();
            let occurrence = ScheduleOccurrence {
                scope: crate::scheduler::ScheduleScope::Group("line-a-sync".to_string()),
                schedule_id: "line-a-sync".to_string(),
                intended_fire_time: fire_time,
                admit_at: fire_time,
                jitter: Duration::ZERO,
            };

            let first = runtime.submit_scheduled_group(&occurrence).await.unwrap();
            let second = runtime.submit_scheduled_group(&occurrence).await.unwrap();
            assert_eq!(
                first, second,
                "one occurrence is one group -- a re-submission must not create a second"
            );

            let group = runtime.catalog.group(first).await.unwrap().unwrap();
            assert_eq!(group.members.len(), 2);
        }

        /// G2: `overlapPolicy` is evaluated against the GROUP, not against individual members.
        ///
        /// A group is outstanding until every camera in it is terminal, so the next occurrence is
        /// skipped while any member is still running. Evaluating this per member would let a
        /// schedule fire again on the cameras that happened to finish first, tearing a synchronised
        /// capture into two half-groups.
        #[tokio::test]
        async fn group_overlap_is_evaluated_against_the_whole_group() {
            let directory = TempDir::new().unwrap();
            let cameras = ["camera-a", "camera-b", "camera-c"];
            let mut configuration = config(directory.path(), &cameras, false);
            // One capture at a time and 800 ms each: the group has ~2.4 s of work to get through, so
            // the assertion below cannot race it even on a badly loaded machine.
            configuration.global.limits.max_concurrent_captures = 1;
            configuration.global.limits.max_in_flight_bytes =
                configuration.global.limits.max_frame_bytes_per_camera;
            for camera in &mut configuration.instances {
                if let crate::config::BackendConfig::Sim(sim) = &mut camera.backend {
                    sim.capture_delay_ms = 800;
                }
            }
            let runtime = runtime(configuration, &directory).await;
            for camera in cameras {
                runtime
                    .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
                    .unwrap();
                wait_for_online(&runtime, camera).await;
            }

            assert!(
                !runtime.group_schedule_overlaps(None).await,
                "a schedule that has never fired does not overlap"
            );
            assert!(
                !runtime
                    .group_schedule_overlaps(Some("grp_never_existed"))
                    .await,
                "a group that retention has already pruned is long terminal, not an overlap"
            );

            let group = runtime
                .submit_group(
                    GroupCaptureRequest {
                        request_id: "overlap-probe".to_string(),
                        instances: cameras.iter().map(|c| (*c).to_string()).collect(),
                        capture_profile: None,
                        profile_overrides: BTreeMap::new(),
                        timeout_ms: None,
                        metadata: serde_json::Map::new(),
                    },
                    "overlap-correlation".to_string(),
                    crate::admission::CapturePriority::Scheduled,
                    None,
                )
                .await
                .unwrap();

            // Three members, one at a time, 800 ms each: the group cannot be terminal yet.
            assert!(
                runtime.group_schedule_overlaps(Some(&group.group_id)).await,
                "a group with members still running IS an overlap -- the next occurrence must skip"
            );

            let terminal =
                wait_for_group_terminal_within(&runtime, &group.group_id, Duration::from_secs(20))
                    .await;
            assert!(terminal.state.is_terminal());
            assert!(
                !runtime.group_schedule_overlaps(Some(&group.group_id)).await,
                "once every member is terminal the schedule is free to fire again"
            );
        }

        #[tokio::test]
        async fn an_oversized_group_is_sequenced_rather_than_partially_failed() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(
                directory.path(),
                &["camera-a", "camera-b", "camera-c", "camera-d"],
                false,
            );
            // One capture at a time, fleet-wide. Four members. Before Q1 this was three dead members.
            configuration.global.limits.max_concurrent_captures = 1;
            configuration.global.limits.max_in_flight_bytes =
                configuration.global.limits.max_frame_bytes_per_camera;
            // Each capture takes 250 ms and only one may run at a time, so the last member does not
            // even START until ~750 ms in. Its acceptance-time capture clock is 400 ms. If the clock
            // still began at acceptance -- as it did before Q1 -- the third and fourth members would
            // arrive at a free camera already dead. That is the defect, and these two numbers are
            // what make this test able to catch it.
            configuration.global.timeouts.capture_ms = 400;
            for camera in &mut configuration.instances {
                if let crate::config::BackendConfig::Sim(sim) = &mut camera.backend {
                    sim.capture_delay_ms = 250;
                }
            }
            let runtime = runtime(configuration, &directory).await;
            for camera in ["camera-a", "camera-b", "camera-c", "camera-d"] {
                runtime
                    .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
                    .unwrap();
                wait_for_online(&runtime, camera).await;
            }

            let group = runtime
                .submit_group(
                    GroupCaptureRequest {
                        request_id: "oversized-group".to_string(),
                        instances: vec![
                            "camera-a".to_string(),
                            "camera-b".to_string(),
                            "camera-c".to_string(),
                            "camera-d".to_string(),
                        ],
                        capture_profile: None,
                        profile_overrides: BTreeMap::new(),
                        timeout_ms: None,
                        metadata: serde_json::Map::new(),
                    },
                    "oversized-correlation".to_string(),
                    crate::admission::CapturePriority::Submitted,
                    None,
                )
                .await
                .expect("acceptance is all-or-nothing and must succeed");
            assert_eq!(group.members.len(), 4);

            let terminal = wait_for_group_terminal(&runtime, &group.group_id).await;
            let states: Vec<_> = terminal.members.iter().map(|member| member.state).collect();
            assert!(
                states
                    .iter()
                    .all(|state| *state == crate::model::JobState::Succeeded),
                "every member of an oversized group must SUCCEED -- sequencing means it takes                  longer, not that most of it fails. Got: {states:?}"
            );
            assert_eq!(
                terminal.state,
                crate::model::JobState::Succeeded,
                "and the collated group result must be a success, not a partial failure"
            );

            runtime.shutdown().await;
        }

        /// B5, carried forward: a slot must come back when its capture leaves the queue, however it
        /// leaves.
        ///
        /// The original defect was a hand-decremented counter that the `?` operator skipped on the
        /// failure path: a refused capture was destroyed AND kept its slot, and four of those made a
        /// camera answer QUEUE_FULL for the rest of the process's life. The fleet queue cannot repeat
        /// it by construction -- the slot is an RAII guard carried INSIDE the queued payload, so it is
        /// released exactly when the descriptor leaves, whether a camera takes it, it is cancelled, or
        /// it expires waiting. There is no longer a line for a `?` to skip.
        ///
        /// So this pins the PROPERTY rather than the old implementation, because the property is what
        /// mattered: a component whose slots leak stops accepting work and never says why.
        #[tokio::test]
        async fn a_cancelled_capture_returns_its_slot_to_the_fleet_queue() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            configuration.global.limits.max_queued_captures_per_camera = 1;
            configuration.global.limits.max_pending_captures = 1;
            let runtime = runtime(configuration, &directory).await;

            // No supervisor: the capture waits in the fleet queue exactly as it would for an offline
            // camera, and it is now the component's entire backlog budget.
            let accepted = runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "will-be-cancelled".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "cancel-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .expect("the first capture must be accepted");
            let capture_id = match accepted {
                crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
                other => panic!("expected a newly accepted capture, got {other:?}"),
            };
            assert_eq!(runtime.scheduler.pending(), 1);
            assert_eq!(
                runtime
                    .submit_capture(
                        "camera-a".to_string(),
                        "refused-while-full".to_string(),
                        None,
                        None,
                        serde_json::Map::new(),
                        "refused-correlation".to_string(),
                        "sb/capture-submit",
                        crate::admission::CapturePriority::Submitted,
                    )
                    .await
                    .expect_err("the queue is full, so this must be refused")
                    .code(),
                crate::ErrorCode::QueueFull,
                "and refused BEFORE anything durable is written -- an operator must see QUEUE_FULL,                  not a capture that was accepted and then immediately died"
            );

            runtime
                .engine("camera-a")
                .unwrap()
                .cancel_active(&capture_id, "operator cancellation")
                .await
                .expect("cancelling a queued capture must succeed");

            // The queue evicts a cancelled entry through its own watcher task, so give it a moment to
            // act on what the cancellation token already says.
            for _ in 0..100 {
                if runtime.scheduler.pending() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(
                runtime.scheduler.pending(),
                0,
                "a cancelled capture must give its slot back, or the component quietly stops                  accepting work and never says why"
            );

            runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "after-the-cancellation".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "recovery-correlation".to_string(),
                    "sb/capture-submit",
                    crate::admission::CapturePriority::Submitted,
                )
                .await
                .expect("the returned slot must be usable");
        }

        // The grace is an upper bound, not a new way to hang: a backend that will not finish its
        // teardown must still be abandoned inside the budget the configuration already grants.
        #[tokio::test]
        async fn a_supervisor_never_waits_for_an_actor_past_the_configured_grace() {
            let directory = TempDir::new().unwrap();
            let mut configuration = config(directory.path(), &["camera-a"], false);
            configuration.global.timeouts.shutdown_grace_ms = 0;
            configuration.global.timeouts.reload_drain_timeout_ms = 0;
            let runtime = runtime(configuration, &directory).await;
            runtime
                .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
                .unwrap();
            wait_for_online(&runtime, "camera-a").await;

            let finished = runtime
                .supervisor_finished
                .read()
                .unwrap()
                .get("camera-a")
                .cloned()
                .expect("a live supervisor must publish its completion token");
            runtime.cancellation.cancel();
            tokio::time::timeout(Duration::from_secs(5), finished.cancelled())
                .await
                .expect("a zero grace must abort the actor rather than wait for its teardown");
            runtime.shutdown().await;
        }

        #[tokio::test]
        async fn hung_actor_is_aborted_once_the_shutdown_grace_expires() {
            let mut actor_task: JoinHandle<Result<()>> =
                tokio::spawn(async { std::future::pending::<Result<()>>().await });
            let started = tokio::time::Instant::now();
            assert!(
                !join_actor_within_grace(&mut actor_task, Duration::from_millis(50)).await,
                "a backend that ignores cancellation must not hold the shutdown grace open"
            );
            assert!(actor_task.is_finished());
            assert!(started.elapsed() < Duration::from_secs(5));
        }
    }
}
