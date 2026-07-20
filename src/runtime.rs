//! Runtime composition and command-plane hand-off.
//!
//! This module deliberately separates inbox registration from construction of the durable
//! runtime. The core command inbox is subscribed during [`edgecommons::EdgeCommonsBuilder`]
//! construction, while the adapter cannot safely accept camera work until catalog recovery,
//! storage probing, and supervisor creation have completed. [`RuntimeCommandRouter`] closes that
//! short interval without a command-registration race.
//!
//! # Where the methods live
//!
//! [`CameraRuntime`] is ONE type with four planes, and the planes are modules rather than types:
//!
//! - `command` -- everything reachable from a southbound command.
//! - `supervision` -- keeping cameras connected, and the periodic work nobody asked for.
//! - `reload` -- replacing one generation of configuration with another, atomically.
//! - `schedule` -- the work the component gives itself.
//!
//! They are not separate TYPES, and that is a decision rather than an omission. Eighteen of this
//! struct's twenty-six fields are touched by more than one plane, and the shared state is not
//! incidental -- it exists precisely to couple them. `reloading` is written by the reload plane and
//! read by the command router to fence commands mid-reload. `messaging_alarm` is written by the job
//! hooks when an announcement fails and read by the alarm the supervision plane owns.
//! `discovery_cache` is written by periodic discovery and read by the `list` verb. And the schedule
//! plane CALLS the command plane, because a schedule is just another producer of captures.
//!
//! Splitting the state would give a command half owning ONE private field, a supervision half owning
//! seven, and a shared core of eighteen that both halves wrap: the same object with an extra
//! indirection, plus new `Arc` cycles where a `Weak<Self>` already exists. The methods cluster
//! cleanly by concern; the state does not. So the methods are what got split.

mod command;
mod reload;
mod schedule;
mod supervision;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
use edgecommons::platform::{Platform, Transport};
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
    config::AdapterConfig,
    jobs::{AcceptanceHook, AppTerminalAnnouncer, CaptureJobSpec, JobEngine, JobHooks},
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
    state_storage_available: bool,
    stopping: bool,
}

impl RuntimeReadinessState {
    fn is_ready(self) -> bool {
        self.startup_complete
            && self.catalog_available
            && self.state_storage_available
            && !self.stopping
    }
}

/// Combines the one-way startup gate with independently observed durable-state availability.
///
/// Readiness is about DURABLE STATE, not about messaging. A recovered catalog cannot make the
/// component ready before every startup gate has completed, and it cannot re-enable readiness after
/// shutdown begins. A broker that is down is deliberately absent from this: the component can still
/// capture, still persist, and still answer -- it simply cannot announce, which is a degradation and
/// not an outage. State changes and the external publication are serialized so a delayed older
/// callback cannot overwrite a newer unavailable state with stale readiness.
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

/// Whether the component can currently tell anyone what it captured.
///
/// One bit, and one alarm, for the whole component. It says nothing about the durable state and
/// nothing about readiness: a degraded messaging plane keeps capturing, keeps persisting, and keeps
/// answering `sb/capture-status` -- it just loses announcements while it lasts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessagingAlarmTransition {
    Degraded,
    Recovered,
}

/// The last observed announcement outcome, so the alarm is raised and cleared once per transition
/// rather than on every capture.
#[derive(Default)]
struct MessagingAlarmState {
    degraded: bool,
}

impl MessagingAlarmState {
    fn transition(&mut self, degraded: bool) -> Option<MessagingAlarmTransition> {
        if self.degraded == degraded {
            return None;
        }
        self.degraded = degraded;
        Some(if degraded {
            MessagingAlarmTransition::Degraded
        } else {
            MessagingAlarmTransition::Recovered
        })
    }
}

async fn publish_messaging_alarm(
    events: Option<EventsFacade>,
    alarms: &Mutex<MessagingAlarmState>,
    degraded: bool,
    context: serde_json::Value,
) {
    let transition = alarms
        .lock()
        .ok()
        .and_then(|mut state| state.transition(degraded));
    let (Some(events), Some(transition)) = (events, transition) else {
        return;
    };
    let result = match transition {
        MessagingAlarmTransition::Degraded => {
            events
                .raise_alarm(
                    Severity::Warning,
                    "message-publish-degraded",
                    Some(
                        "capture results are durable but cannot be announced; announcements are \
                         dropped while this lasts"
                            .to_string(),
                    ),
                    Some(context),
                )
                .await
        }
        MessagingAlarmTransition::Recovered => {
            events
                .clear_alarm(Severity::Warning, "message-publish-degraded", Some(context))
                .await
        }
    };
    if let Err(error) = result {
        tracing::warn!(error = %error, "failed to publish messaging-degradation alarm");
    }
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
                        crate::ErrorCode::BadArgs,
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
                        crate::ErrorCode::BadArgs,
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
        crate::ErrorCode::BadArgs,
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
            crate::config::BackendConfig::Rtsp(_) => false,
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

/// Everything a capture resolves out of the configuration before it becomes a durable job.
///
/// This was ~90 lines copied verbatim between `submit_capture` and `build_group_submission` -- profile
/// defaulting, deadline arithmetic, capture-mode inference, the camera summary, output-path rendering.
/// The highest-churn logic in the crate, duplicated, which is the arrangement in which a fix lands in
/// one copy and not the other and nobody finds out until the two disagree in the field.
struct ResolvedCapture {
    camera: Arc<crate::config::CameraConfig>,
    snapshot: crate::registry::CameraSnapshot,
    profile_name: String,
    profile: crate::jobs::JobProfileSnapshot,
    camera_summary: crate::messages::CameraSummary,
    capture_id: String,
    accepted_at_ms: i64,
    /// The effective terminal budget: the request's, else the profile's, else the global default.
    terminal_ms: u64,
    deadlines: crate::catalog::JobDeadlines,
    relative_path: crate::storage::RelativeOutputPath,
}

/// Builds the slot map from a roster's engines and whatever event facades go with them.
///
/// The engine is what makes a camera exist; the facade is attached to it. `start` requires a facade for
/// every camera and the reload commit requires one for every camera it adds, so in a running component
/// they are always both there -- but a camera is never DROPPED here for want of one, because a camera
/// that silently ceases to exist is a far worse failure than one that cannot publish lifecycle events.
fn new_slots(
    engines: BTreeMap<String, JobEngine>,
    mut events: BTreeMap<String, EventsFacade>,
) -> BTreeMap<String, CameraSlot> {
    engines
        .into_iter()
        .map(|(instance, engine)| {
            let events = events.remove(&instance);
            (
                instance,
                CameraSlot {
                    engine,
                    events,
                    supervisor: None,
                    session: None,
                    motion_stop: None,
                },
            )
        })
        .collect()
}

/// One camera's entire runtime presence, in one place.
///
/// This used to be SEVEN maps -- engines, events, actors, supervisor cancellations, supervisor
/// completions, session cancellations, motion stops -- each with its own lock, each keyed by the same
/// instance id, and each kept in step with the others BY HAND. Every lifecycle path had to remember all
/// seven, and they each remembered a different subset: the roster-removal block cleaned five of them,
/// the supervisor's own teardown cleaned two, rollback restored two, shutdown cleaned none, and
/// `motion_stops` was cleaned by no lifecycle path at all. That last omission is not a tidiness
/// complaint. It is how a camera came to be left physically moving with nothing left alive to stop it.
///
/// A camera is now one entry. Removing it is one `remove`, which drops its engine, its facades, its
/// supervisor, its session, and its armed stop together, because they are one thing. The failure mode
/// the seven maps kept producing -- a camera that half-exists, present in one map and absent from
/// another -- is no longer representable, which is a stronger guarantee than remembering to be careful.
struct CameraSlot {
    /// The durable capture engine. Lives as long as the camera is in the roster.
    engine: JobEngine,
    /// The camera's event-publishing facade, when one is installed.
    ///
    /// Optional because the runtime has always tolerated its absence -- the lifecycle-event lookup
    /// warns and carries on. Modelling it as mandatory would have quietly dropped any camera that
    /// lacked one, which is a worse answer than the warning.
    events: Option<EventsFacade>,
    /// The running supervisor generation, if one is running.
    supervisor: Option<Supervision>,
    /// The connected session, if the camera is connected.
    session: Option<Session>,
    /// A mandatory PTZ stop that has been armed and not yet delivered.
    motion_stop: Option<ArmedStop>,
}

/// One supervisor generation.
///
/// The two tokens belong together and were never once used apart: `cancellation` retires the
/// generation and `finished` is how a replacement knows the old one is gone. Holding them in separate
/// maps meant `start_supervisor` could overwrite one and not the other.
#[derive(Clone)]
struct Supervision {
    /// Retires this generation. A child of the process token, so shutdown reaches it too.
    cancellation: CancellationToken,
    /// Cancelled when the supervisor loop has exited. The reload drain barrier waits on this.
    finished: CancellationToken,
}

/// A camera's live session: the actor that owns it, and the token that ends it.
#[derive(Clone)]
struct Session {
    actor: CameraActorHandle,
    /// Cancels this session directly, leaving the supervisor free to reconnect.
    cancellation: CancellationToken,
}

/// A mandatory stop that has been armed and not yet delivered (DESIGN 15.5).
///
/// This used to be a bare `CancellationToken`, and the axes it was armed for lived nowhere but inside
/// the timer task's closure. So the timer was the only thing in the component that COULD deliver the
/// stop -- and every path that took the camera's actor away before the deadline dropped the stop on
/// the floor, because the timer resolves its actor lazily and simply gives up when there is not one.
///
/// Holding the axes here is what lets a retiring supervisor, or a shutting-down component, deliver the
/// stop itself while it still has an actor to deliver it through.
#[derive(Clone)]
struct ArmedStop {
    /// Retires the timer. A stop delivered by someone else must not also arrive from the deadline.
    cancellation: CancellationToken,
    pan: bool,
    tilt: bool,
    zoom: bool,
    /// The bound on the stop's own protocol call.
    ptz_ms: u64,
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
    /// The live configuration, shared rather than copied.
    ///
    /// `config_snapshot()` used to DEEP-CLONE this on every call -- the whole roster, every camera's
    /// capture profiles, every backend's allowlists: ~250 KB and thousands of allocations at 256
    /// cameras, from 34 call sites including the capture hot path, and `lifecycle_events()` did it
    /// twice per capture to read one bool. At the design's 16 captures/s that is tens of MB/s of
    /// pure alloc/free churn, and every clone takes the read lock that a reload needs to write.
    ///
    /// A snapshot is now a refcount bump. It is still a SNAPSHOT -- a reload swaps the Arc, so a
    /// caller holding one keeps the configuration it started with, which is the property the deep
    /// clone was really providing.
    config: RwLock<Arc<AdapterConfig>>,
    /// What the resolved transport can carry, in previews. Fixed for the life of the process: the
    /// transport is a startup argument, and a reload cannot change it.
    thumbnail_policy: crate::thumbnail::ThumbnailPolicy,
    backend_context: BackendRuntimeContext,
    catalog: Catalog,
    admission: AdmissionController,
    storage: StorageRoot,
    registry: Arc<CameraRegistry>,
    /// Every configured camera, and everything the runtime knows about it. See [`CameraSlot`].
    ///
    /// An entry exists exactly while the camera is in the roster. Its `supervisor`, `session` and
    /// `motion_stop` are the lifecycle state within that, and they go when the camera goes -- which is
    /// the whole reason this is one map and not seven.
    cameras: Arc<RwLock<BTreeMap<String, CameraSlot>>>,
    /// The component-main event facade: component-wide alarms, not per-camera ones.
    component_events: Option<EventsFacade>,
    storage_pressure: Option<StoragePressureMonitor>,
    storage_alarm: Arc<Mutex<StorageAlarmState>>,
    /// Whether the last terminal announcement failed. See [`MessagingAlarmState`].
    messaging_alarm: Arc<Mutex<MessagingAlarmState>>,
    readiness: RuntimeReadiness,
    /// Capture counters and sampled queue levels. See `observability::CaptureMetrics`.
    metrics: Arc<crate::observability::CaptureMetrics>,
    /// Per-camera southbound health, the standard metric every adapter in the ecosystem emits.
    health: Arc<crate::observability::FleetHealth>,
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
    /// Cameras an operator has paused via `sb/pause`.
    ///
    /// A paused camera runs its in-flight captures to completion but accepts no new commanded or
    /// scheduled capture work until `sb/resume` (SOUTHBOUND.md §2.2). The state is process-local and
    /// deliberately not durable: a fresh start is unpaused, matching the scaffold's in-memory pause.
    /// Keyed by instance id; a camera a reload removes leaves an inert entry that a restart clears and
    /// that nothing reads (its schedules and capture paths no longer exist).
    paused: RwLock<std::collections::BTreeSet<String>>,
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
    config: Arc<AdapterConfig>,
    engines: BTreeMap<String, JobEngine>,
    events: BTreeMap<String, EventsFacade>,
}

/// Process-local core deferred tokens paired with durable waiter records. The durable row makes
/// acceptance auditable; only the opaque token stays in memory, because routing remains owned by
/// the EdgeCommons command inbox.
/// Something waiting to be told how a capture or a group ended.
///
/// The registry held `DeferredReplyToken` directly. That type is minted only by the core library, has
/// private fields and no constructor an adapter can reach -- so the waiter bound below could not be
/// tested at all without putting a `#[cfg(test)]` fake inside a production type, which is precisely
/// the shape (T2) this codebase has already been bitten by. A one-method seam costs nothing, changes
/// no behaviour, and makes the bound provable.
#[async_trait::async_trait]
trait CaptureWaiter: Send + Sync {
    /// Delivers the terminal result. `true` when the caller was actually reached.
    async fn settle(&self, result: serde_json::Value) -> bool;
}

#[async_trait::async_trait]
impl CaptureWaiter for DeferredReplyToken {
    async fn settle(&self, result: serde_json::Value) -> bool {
        self.settle_success(Some(result)).await.is_ok()
    }
}

struct RuntimeJobHooks {
    catalog: Catalog,
    runtime: Mutex<Weak<CameraRuntime>>,
    lifecycle_event_slots: Arc<Semaphore>,
    /// `limits.maxDeferredWaitersPerCapture`, which until now bounded nothing.
    ///
    /// Held here rather than read from the configuration on every attach: this is on the acceptance
    /// path, and the configuration is a deep clone.
    waiter_limit: AtomicUsize,
    tokens: Mutex<HashMap<String, AttachedWaiters>>,
    group_tokens: Mutex<HashMap<String, Vec<Arc<dyn CaptureWaiter>>>>,
    pending: Mutex<HashMap<(String, String), PendingDeferredWaiter>>,
}

type PendingDeferredWaiter = (String, Arc<dyn CaptureWaiter>, String, String);
/// The callers attached to one capture, each with the waiter id its durable row is keyed by.
type AttachedWaiters = Vec<(String, Arc<dyn CaptureWaiter>)>;

impl RuntimeJobHooks {
    fn new(catalog: Catalog) -> Self {
        Self {
            // Replaced from configuration the moment the runtime exists; this is only the floor.
            waiter_limit: AtomicUsize::new(8),
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

    /// Applies `limits.maxDeferredWaitersPerCapture` to the hooks.
    fn set_waiter_limit(&self, limit: usize) {
        self.waiter_limit.store(limit.max(1), Ordering::Release);
    }

    /// Attaches one more caller to an in-flight capture, up to the configured bound.
    ///
    /// A retried direct capture attaches ANOTHER waiter to the same job (DESIGN §356), and the number
    /// of them is what `limits.maxDeferredWaitersPerCapture` exists to bound. It bounded nothing: the
    /// list was pushed to unconditionally, so a client that kept retrying grew it without limit, and
    /// every one of those tokens is held until the capture is terminal and then fanned out to.
    fn register(
        &self,
        capture_id: String,
        waiter_id: String,
        token: Arc<dyn CaptureWaiter>,
    ) -> Result<()> {
        let limit = self.waiter_limit.load(Ordering::Acquire);
        let mut tokens = self.tokens.lock().map_err(|_| {
            crate::CameraError::Catalog("deferred waiter registry is unavailable".to_string())
        })?;
        let waiters = tokens.entry(capture_id).or_default();
        if waiters.len() >= limit {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::ResourceLimit,
                "this capture already has the maximum number of callers waiting on it",
            ));
        }
        waiters.push((waiter_id, token));
        Ok(())
    }

    fn register_group(&self, group_id: String, token: Arc<dyn CaptureWaiter>) -> Result<()> {
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
            let _ = token.settle(body.clone()).await;
        }
    }

    fn prepare(
        &self,
        instance: String,
        request_id: String,
        waiter_id: String,
        token: Arc<dyn CaptureWaiter>,
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

    fn take_pending(&self, instance: &str, request_id: &str) -> Option<PendingDeferredWaiter> {
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
        // The camera answered, or it did not. `pollLatencyMs` is the acquisition round-trip, which is
        // exactly what SOUTHBOUND §5 means by a read/poll latency, and a failure IN acquisition is a
        // read error -- an encode or a disk failure is not the camera's fault and must not be counted
        // against it, or a healthy camera behind a full disk reads as a broken one.
        if let Some(runtime) = self.runtime() {
            match record.state {
                crate::model::JobState::Succeeded => {
                    if let Some(acquisition) = terminal_body
                        .get("durationsMs")
                        .and_then(|durations| durations.get("acquisition"))
                        .and_then(serde_json::Value::as_u64)
                    {
                        runtime
                            .health
                            .observed_success(&record.instance, Duration::from_millis(acquisition));
                    }
                }
                crate::model::JobState::Failed => {
                    let stage = terminal_body
                        .get("failure")
                        .and_then(|failure| failure.get("stage"))
                        .and_then(serde_json::Value::as_str);
                    if stage.is_some_and(|stage| stage.eq_ignore_ascii_case("acquiring")) {
                        runtime.health.observed_read_error(&record.instance);
                    }
                }
                _ => {}
            }
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
            if token.settle(terminal_body.clone()).await {
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

    async fn terminal_announced(&self, record: &crate::catalog::JobRecord, latency: Duration) {
        let Some(runtime) = self.runtime() else {
            return;
        };
        runtime.health.observed_publish(&record.instance, latency);
        runtime
            .note_messaging_health(true, Some(serde_json::json!({ "instance": record.instance })))
            .await;
    }

    async fn terminal_announcement_failed(
        &self,
        record: &crate::catalog::JobRecord,
        error: &crate::CameraError,
    ) {
        let Some(runtime) = self.runtime() else {
            return;
        };
        // Counted, not merely logged. A broker that is down costs announcements, and this is the
        // only number that says how many -- the capture itself is already durable and already
        // counted as SUCCEEDED/FAILED/CANCELLED by `settle_waiters`.
        runtime
            .metrics
            .count(crate::observability::ANNOUNCEMENT_FAILED_MEASURE)
            .await;
        runtime
            .note_messaging_health(
                false,
                Some(serde_json::json!({
                    "instance": record.instance,
                    "captureId": record.capture_id,
                    "errorCode": error.code().as_str(),
                })),
            )
            .await;
    }

    // A thumbnail is a convenience, and neither of these is a capture's failure: the capture is
    // already on disk and is announced either way. They are COUNTED so that a camera whose previews
    // never arrive is visible to an operator, rather than being a log line nobody aggregates -- and
    // they are counted apart, because "cannot be rendered" and "does not fit the budget" call for
    // different responses.
    async fn thumbnail_failed(&self) {
        if let Some(runtime) = self.runtime() {
            runtime
                .metrics
                .count(crate::observability::THUMBNAIL_FAILED_MEASURE)
                .await;
        }
    }

    async fn thumbnail_dropped(&self) {
        if let Some(runtime) = self.runtime() {
            runtime
                .metrics
                .count(crate::observability::THUMBNAIL_DROPPED_MEASURE)
                .await;
        }
    }

    async fn announcement_retried_without_preview(&self) {
        if let Some(runtime) = self.runtime() {
            runtime
                .metrics
                .count(crate::observability::ANNOUNCEMENT_RETRIED_WITHOUT_PREVIEW_MEASURE)
                .await;
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
        if verb != CommandVerb::Capture.as_str() {
            return Ok(());
        }
        let Some((waiter_id, token, correlation_id, request_uuid)) =
            self.take_pending(&record.instance, request_id)
        else {
            return Ok(());
        };
        // Attached before the durable row is written: a caller the bound turns away must not leave a
        // waiter row behind for a reply that will never be routed to it.
        self.register(record.capture_id.clone(), waiter_id.clone(), token)?;
        let now = chrono::Utc::now().timestamp_millis();
        self.catalog
            .add_waiter(crate::catalog::WaiterRecord {
                waiter_id,
                capture_id: record.capture_id.clone(),
                correlation_id,
                request_uuid: Some(request_uuid),
                expires_at_ms: record.deadlines.terminal_at_ms,
                created_at_ms: now,
            })
            .await?;
        Ok(())
    }
}

/// Runtime-owned facades and platform services supplied after durable startup resources exist.
///
/// Grouping these dependencies makes the boundary explicit: protocol construction, per-camera
/// terminal announcement and event publication, component-wide alarms, and readiness are
/// process-owned services rather than configuration values.
pub struct RuntimeServices {
    /// Per-camera application facades used to announce terminal results.
    pub apps: BTreeMap<String, Arc<AppFacade>>,
    /// Per-camera event facades used by schedules and camera lifecycle events.
    pub events: BTreeMap<String, EventsFacade>,
    /// Component-main event facade used for component-wide alarms (storage, messaging).
    pub component_events: EventsFacade,
    /// Combined startup/durable-state readiness bridge.
    pub readiness: RuntimeReadiness,
    /// Credential/discovery/security services required to construct a backend.
    pub backend_context: BackendRuntimeContext,
    /// Component metric service. Capture counts ride the job hooks; queue levels are sampled.
    pub metrics: Arc<dyn edgecommons::metrics::MetricService>,
    /// The RESOLVED messaging transport (`gg.args().transport`).
    ///
    /// What the component may put on the wire depends on what the wire can take, and only the
    /// resolved transport knows. It decides the thumbnail policy: Greengrass IPC caps a whole
    /// message at 10,000 bytes inside our own client library, and an MQTT broker does not.
    pub transport: Transport,
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

/// What a capture needs from the live configuration in order to resume after a restart.
struct ResumableCapture {
    resource_group: Option<String>,
    group_size: Option<usize>,
    priority: crate::admission::CapturePriority,
}

/// The queue priority a recovered capture returns with.
///
/// Taken from the durable record, so a capture keeps the place it was originally given rather than
/// being demoted for having survived a restart.
fn recovered_priority(record: &crate::catalog::JobRecord) -> crate::admission::CapturePriority {
    match record.verb.as_deref() {
        Some(verb)
            if verb == CommandVerb::Capture.as_str()
                || verb == CommandVerb::CaptureGroup.as_str() =>
        {
            crate::admission::CapturePriority::Direct
        }
        Some(verb)
            if verb == CommandVerb::CaptureSubmit.as_str()
                || verb == CommandVerb::CaptureGroupSubmit.as_str() =>
        {
            crate::admission::CapturePriority::Submitted
        }
        // No verb means no command ledger, which means a schedule fired it.
        _ => crate::admission::CapturePriority::Scheduled,
    }
}

impl CameraRuntime {
    fn config_snapshot(&self) -> Result<Arc<AdapterConfig>> {
        self.config
            .read()
            .map(|config| Arc::clone(&config))
            .map_err(|_| {
                crate::CameraError::Catalog("runtime configuration lock is unavailable".to_string())
            })
    }

    /// Whether an operator has paused this camera via `sb/pause`.
    fn is_paused(&self, instance: &str) -> bool {
        self.paused
            .read()
            .map(|paused| paused.contains(instance))
            .unwrap_or(false)
    }

    /// Sets the pause state for one camera, returning whether it CHANGED (the `changed` reply field).
    fn set_paused(&self, instance: &str, paused: bool) -> bool {
        let mut set = self
            .paused
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if paused {
            set.insert(instance.to_owned())
        } else {
            set.remove(instance)
        }
    }

    /// Refuses new capture work while the camera is paused. In-flight captures are untouched.
    fn ensure_not_paused(&self, instance: &str) -> Result<()> {
        if self.is_paused(instance) {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::InstancePaused,
                "camera is paused; resume it before submitting new capture work",
            ));
        }
        Ok(())
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
        match self.cameras.read() {
            Ok(cameras) => match cameras.get(instance).and_then(|slot| slot.events.clone()) {
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
            Arc::new(AppTerminalAnnouncer::new(app)),
            Arc::clone(&self.waiters) as Arc<dyn JobHooks>,
            self.thumbnail_policy,
        )
        .with_acceptance_hook(Arc::clone(&self.waiters) as Arc<dyn AcceptanceHook>)
    }

    /// Builds the durable runtime, recovers install-owned records, and creates one lightweight
    /// supervisor for every enabled camera.
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
            component_events,
            readiness,
            backend_context,
            metrics,
            transport,
        } = services;
        let thumbnail_policy = crate::thumbnail::ThumbnailPolicy::for_transport(transport);
        let metrics = Arc::new(crate::observability::CaptureMetrics::new(metrics));
        let health = Arc::new(crate::observability::FleetHealth::default());
        backend_context.validate_config(&config)?;
        let max_connection_attempts = config.global.limits.max_concurrent_connects;
        let storage_pressure = StoragePressureMonitor::new(
            resources.storage.canonical_root(),
            &resources.state_directory,
            &config.global.output,
            Arc::new(FilesystemSpaceProbe::default()),
        );
        let waiters = Arc::new(RuntimeJobHooks::new(resources.catalog.clone()));
        waiters.set_waiter_limit(config.global.limits.max_deferred_waiters_per_capture);
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
                    Arc::new(AppTerminalAnnouncer::new(Arc::clone(app))),
                    Arc::clone(&waiters) as Arc<dyn JobHooks>,
                    thumbnail_policy,
                )
                .with_acceptance_hook(Arc::clone(&waiters) as Arc<dyn AcceptanceHook>),
            );
        }
        // Once, here, and nowhere else: the operator is told that this transport cannot carry the
        // preview they asked for, and what they are getting instead. Not per capture.
        log_thumbnail_clamps(&config, thumbnail_policy);

        let runtime = Arc::new(Self {
            config: RwLock::new(Arc::new(config)),
            thumbnail_policy,
            backend_context,
            health,
            catalog: resources.catalog,
            admission: resources.admission,
            storage: resources.storage,
            registry: resources.registry,
            cameras: Arc::new(RwLock::new(new_slots(engines, events))),
            component_events: Some(component_events),
            storage_pressure: Some(storage_pressure),
            storage_alarm: Arc::new(Mutex::new(StorageAlarmState::default())),
            messaging_alarm: Arc::new(Mutex::new(MessagingAlarmState::default())),
            readiness,
            metrics,
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
            paused: RwLock::new(std::collections::BTreeSet::new()),
            self_reference: OnceLock::new(),
        });
        let _ = runtime.self_reference.set(Arc::downgrade(&runtime));
        waiters.attach_runtime(Arc::downgrade(&runtime));

        runtime.refresh_storage_pressure().await;
        runtime.start_capture_scheduler()?;
        runtime.start_storage_pressure_monitor()?;
        runtime.start_metric_sampler()?;
        runtime.recover_install_owned().await?;
        runtime.start_catalog_health()?;
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
        self.cameras
            .read()
            .map_err(|_| crate::CameraError::Catalog("camera slot map is unavailable".to_string()))?
            .get(instance)
            .and_then(|slot| slot.session.as_ref())
            .map(|session| session.actor.clone())
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::DeviceUnavailable,
                    format!("camera instance '{instance}' is not connected"),
                )
            })
    }

    /// Returns the durable job engine for one configured camera.
    pub fn engine(&self, instance: &str) -> Result<JobEngine> {
        self.cameras
            .read()
            .map_err(|_| crate::CameraError::Catalog("camera slot map is unavailable".to_string()))?
            .get(instance)
            .map(|slot| slot.engine.clone())
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::NoSuchInstance,
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

    /// Spawns a task the runtime will wait for at shutdown.
    ///
    /// The registry used to be push-only: every schedule, supervisor and periodic task a reload ever
    /// started stayed in it, so at 256 cameras each reload retained about five hundred `JoinHandle`s
    /// of tasks that had already finished -- forever, for the life of the process. Reaping the
    /// finished ones here costs a scan of a list that is only as long as the tasks actually running,
    /// and it happens on a path that is already spawning a thread's worth of work.
    fn spawn_task(
        &self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        let handle = tokio::spawn(task);
        let mut tasks = self.tasks.lock().map_err(|_| {
            crate::CameraError::Catalog("runtime task registry is unavailable".to_string())
        })?;
        tasks.retain(|running| !running.is_finished());
        tasks.push(handle);
        Ok(())
    }

    async fn refresh_storage_pressure(&self) -> Option<StoragePressureSnapshot> {
        let monitor = self.storage_pressure.clone()?;
        let snapshot = monitor.assess().await;
        self.readiness
            .set_state_storage_available(snapshot.state_available());
        publish_storage_alarm(
            self.component_events.clone(),
            self.storage_alarm.as_ref(),
            &snapshot,
        )
        .await;
        Some(snapshot)
    }

    /// Records whether the last terminal announcement reached the transport, and raises or clears
    /// the component's messaging alarm on the transition.
    ///
    /// Deliberately NOT a readiness gate and NOT an intake gate. Messaging being down loses
    /// announcements; it does not make a capture impossible, and a component that refused captures
    /// because a broker was unreachable would be throwing away the very data the catalog and the
    /// disk are there to keep.
    async fn note_messaging_health(&self, healthy: bool, context: Option<serde_json::Value>) {
        publish_messaging_alarm(
            self.component_events.clone(),
            self.messaging_alarm.as_ref(),
            !healthy,
            context.unwrap_or_else(|| serde_json::json!({})),
        )
        .await;
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
                crate::ErrorCode::DeviceUnavailable.as_str(),
                "the camera adapter is draining a configuration replacement",
            ));
        }
        // The only place an unknown verb exists. Past this line the verb is a type, and the match
        // below has no catch-all to fall through: a verb added to `CommandVerb` and forgotten here is
        // a compile error, not an `UNSUPPORTED_CAPABILITY` an operator discovers in production.
        let Some(verb) = CommandVerb::parse(verb) else {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::UnsupportedCapability.as_str(),
                "unsupported camera command",
            ));
        };
        // The CameraCommand operational family (instance × verb × result). `instance` is best-effort
        // from the request body ("main" for a fleet/component-scoped command); the result is recorded
        // once the outcome is known. The deferred capture verbs count acceptance (the capture's own
        // success/failure lives in camera_captures).
        let command_started = std::time::Instant::now();
        let command_instance = request
            .body
            .get("instance")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("main")
            .to_owned();
        let command_ms = |started: std::time::Instant| {
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
        };
        if verb == CommandVerb::Capture {
            let outcome = self.handle_deferred_capture(request, deferred).await;
            self.metrics.record_command(
                &command_instance,
                verb.as_str(),
                !matches!(outcome, CommandOutcome::ImmediateError(_)),
                command_ms(command_started),
            );
            return outcome;
        }
        if verb == CommandVerb::CaptureGroup {
            let outcome = self.handle_deferred_group_capture(request, deferred).await;
            self.metrics.record_command(
                &command_instance,
                verb.as_str(),
                !matches!(outcome, CommandOutcome::ImmediateError(_)),
                command_ms(command_started),
            );
            return outcome;
        }
        let config = match self.config_snapshot() {
            Ok(config) => config,
            Err(error) => return CommandOutcome::ImmediateError(command_error(&error)),
        };
        let outcome: Result<serde_json::Value> = async {
            match verb {
                CommandVerb::List => {
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
                CommandVerb::Discover => {
                    let body: DiscoverRequest = commands::parse_closed(request.body.clone())?;
                    self.discover(body).await
                }
                CommandVerb::Status => {
                    let body: StatusRequest = commands::parse_closed(request.body.clone())?;
                    body.validate()?;
                    match body.instance {
                        // The `paused` flag is stamped onto the status view so the standardized
                        // lifecycle state is visible where an operator (and the overview panel) reads
                        // status, per SOUTHBOUND.md §2.2.
                        Some(instance) => {
                            let mut view = serde_json::to_value(self.registry.snapshot(&instance)?)?;
                            if let Some(object) = view.as_object_mut() {
                                object.insert("paused".to_owned(), self.is_paused(&instance).into());
                            }
                            Ok(view)
                        }
                        None => {
                            let cameras = self
                                .registry
                                .snapshots(1_000)?
                                .into_iter()
                                .map(|snapshot| {
                                    let paused = self.is_paused(&snapshot.instance);
                                    let mut view = serde_json::to_value(snapshot)?;
                                    if let Some(object) = view.as_object_mut() {
                                        object.insert("paused".to_owned(), paused.into());
                                    }
                                    Ok(view)
                                })
                                .collect::<Result<Vec<_>>>()?;
                            Ok(serde_json::json!({ "cameras": cameras }))
                        }
                    }
                }
                CommandVerb::CaptureSubmit => {
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
                        verb.as_str(),
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
                        "statusVerb": CommandVerb::CaptureStatus.as_str(),
                    }))
                }
                CommandVerb::CaptureGroupSubmit => {
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
                CommandVerb::CaptureStatus => {
                    let body: CaptureStatusRequest = commands::parse_closed(request.body.clone())?;
                    let limit = body.limit;
                    let cursor = body.cursor.clone();
                    match body.validate()? {
                        CaptureStatusMode::Capture => {
                            let id = body.capture_id.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::BadArgs, "captureId is required"))?;
                            let job = self.catalog.job(id).await?.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::CaptureNotFound, "capture was not found"))?;
                            Ok(job_status_json(&job))
                        }
                        CaptureStatusMode::Group => {
                            let id = body.capture_group_id.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::BadArgs, "captureGroupId is required"))?;
                            let group = self.catalog.group(id).await?.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::CaptureNotFound, "capture group was not found"))?;
                            self.group_status_page(group, usize::from(limit), cursor.as_deref())
                        }
                        CaptureStatusMode::CameraRequest => {
                            let instance = body.instance.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::BadArgs, "instance is required"))?;
                            let request_id = body.request_id.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::BadArgs, "requestId is required"))?;
                            let job = self.catalog.job_by_ledger(crate::catalog::LedgerKey::new(instance, CommandVerb::Capture.as_str(), request_id)?).await?
                                .ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::CaptureNotFound, "capture was not found"))?;
                            Ok(job_status_json(&job))
                        }
                        CaptureStatusMode::GroupRequest => {
                            let request_id = body.request_id.ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::BadArgs, "requestId is required"))?;
                            let group = self.catalog.group_by_ledger(crate::catalog::LedgerKey::new("main", CommandVerb::CaptureGroup.as_str(), request_id)?).await?
                                .ok_or_else(|| crate::CameraError::rejected(crate::ErrorCode::CaptureNotFound, "capture group was not found"))?;
                            self.group_status_page(group, usize::from(limit), cursor.as_deref())
                        }
                        CaptureStatusMode::List => {
                            self.jobs_status_page(&body).await
                        }
                    }
                }
                CommandVerb::CaptureCancel => {
                    let body: CancelRequest = commands::parse_closed(request.body.clone())?;
                    self.cancel_capture(body).await
                }
                CommandVerb::QueueStatus => {
                    let body: commands::QueueStatusRequest =
                        commands::parse_closed(request.body.clone())?;
                    self.queue_status_command(body).await
                }
                CommandVerb::QueueClear => {
                    let body: commands::QueueClearRequest =
                        commands::parse_closed(request.body.clone())?;
                    self.queue_clear_command(body).await
                }
                CommandVerb::Reconnect => {
                    let body: ReconnectRequest = commands::parse_closed(request.body.clone())?;
                    self.reconnect(body).await
                }
                CommandVerb::Ptz => {
                    let body: PtzCommandRequest = commands::parse_closed(request.body.clone())?;
                    self.perform_ptz(body).await
                }
                CommandVerb::PtzPresets => {
                    let body: PtzPresetsRequest = commands::parse_closed(request.body.clone())?;
                    self.perform_presets(body).await
                }
                CommandVerb::Pause => {
                    let body: commands::PauseResumeRequest =
                        commands::parse_closed(request.body.clone())?;
                    body.validate()?;
                    let instance = self.registry.resolve_instance(body.instance.as_deref())?;
                    let changed = self.set_paused(&instance, true);
                    Ok(serde_json::json!({ "id": instance, "paused": true, "changed": changed }))
                }
                CommandVerb::Resume => {
                    let body: commands::PauseResumeRequest =
                        commands::parse_closed(request.body.clone())?;
                    body.validate()?;
                    let instance = self.registry.resolve_instance(body.instance.as_deref())?;
                    let changed = self.set_paused(&instance, false);
                    Ok(serde_json::json!({ "id": instance, "paused": false, "changed": changed }))
                }
                // The deferred verbs are answered above, before this match is reached. They are
                // named here rather than swept up by a `_`, because a `_` is what let a fifteenth
                // verb register with the inbox and then fall through to UNSUPPORTED_CAPABILITY at
                // runtime instead of failing to compile. There is no catch-all now: add a verb and
                // this match stops building until somebody decides what it does.
                CommandVerb::Capture | CommandVerb::CaptureGroup => Err(
                    crate::CameraError::Catalog(
                        "a deferred verb reached the immediate dispatch".to_string(),
                    ),
                ),
            }
        }.await;
        self.metrics.record_command(
            &command_instance,
            verb.as_str(),
            outcome.is_ok(),
            command_ms(command_started),
        );
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
/// A command this adapter answers.
///
/// The verb used to be a bare `&str` in three places that had to agree and were kept in step only by
/// eye: a `[&str; 14]` registered with the inbox, a dispatch `match` with a `_` catch-all, and the
/// durable ledger key -- typed out by hand at each call site.
///
/// The dispatch coupling was merely fragile: add a fifteenth verb to the array and it registers with
/// the inbox, then falls through the catch-all to `UNSUPPORTED_CAPABILITY` at runtime instead of
/// failing to compile.
///
/// The LEDGER coupling was worse. The verb is part of the durable idempotency key, so a typo at one of
/// those call sites silently opens a NEW idempotency namespace: the retry a caller sends to get
/// exactly-once semantics no longer finds the operation it is retrying, and does it again. Nothing
/// catches that -- not the compiler, not a test, not a log line. A key is a key.
///
/// One spelling, in one place, and a `match` that will not compile if a verb is added and forgotten.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandVerb {
    /// `sb/list`
    List,
    /// `sb/discover`
    Discover,
    /// `sb/status`
    Status,
    /// `sb/capture` -- deferred.
    Capture,
    /// `sb/capture-submit`
    CaptureSubmit,
    /// `sb/capture-group` -- deferred.
    CaptureGroup,
    /// `sb/capture-group-submit`
    CaptureGroupSubmit,
    /// `sb/capture-status`
    CaptureStatus,
    /// `sb/capture-cancel`
    CaptureCancel,
    /// `sb/queue-status`
    QueueStatus,
    /// `sb/queue-clear`
    QueueClear,
    /// `sb/reconnect`
    Reconnect,
    /// `sb/ptz`
    Ptz,
    /// `sb/ptz-presets`
    PtzPresets,
    /// `sb/pause` -- suspend new capture work for the instance (standardized lifecycle verb).
    Pause,
    /// `sb/resume` -- resume a paused instance (standardized lifecycle verb).
    Resume,
}

impl CommandVerb {
    /// Every verb the adapter answers, in the order the inbox registers them.
    pub const ALL: [Self; 16] = [
        Self::List,
        Self::Discover,
        Self::Status,
        Self::Capture,
        Self::CaptureSubmit,
        Self::CaptureGroup,
        Self::CaptureGroupSubmit,
        Self::CaptureStatus,
        Self::CaptureCancel,
        Self::QueueStatus,
        Self::QueueClear,
        Self::Reconnect,
        Self::Ptz,
        Self::PtzPresets,
        Self::Pause,
        Self::Resume,
    ];

    /// The exact wire spelling. This string is durable: it is part of the idempotency key.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::List => "sb/list",
            Self::Discover => "sb/discover",
            Self::Status => "sb/status",
            Self::Capture => "sb/capture",
            Self::CaptureSubmit => "sb/capture-submit",
            Self::CaptureGroup => "sb/capture-group",
            Self::CaptureGroupSubmit => "sb/capture-group-submit",
            Self::CaptureStatus => "sb/capture-status",
            Self::CaptureCancel => "sb/capture-cancel",
            Self::QueueStatus => "sb/queue-status",
            Self::QueueClear => "sb/queue-clear",
            Self::Reconnect => "sb/reconnect",
            Self::Ptz => "sb/ptz",
            Self::PtzPresets => "sb/ptz-presets",
            Self::Pause => "sb/pause",
            Self::Resume => "sb/resume",
        }
    }

    /// Recognises a verb arriving on the wire. The one place an unknown verb is a possibility.
    #[must_use]
    pub fn parse(verb: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|known| known.as_str() == verb)
    }
}

/// The verbs registered with the command inbox, derived from [`CommandVerb::ALL`].
#[must_use]
pub fn camera_command_verbs() -> Vec<&'static str> {
    CommandVerb::ALL.iter().map(|verb| verb.as_str()).collect()
}

/// The three edge-console panel descriptors for the camera adapter.
///
/// Core validates `id`/`title`/uniqueness; the widget kinds and bound verbs are console-interpreted,
/// so they ride verbatim. `order` 10/20/30, every panel `scope: "instance"`. Each binds only verbs
/// this adapter actually serves (SOUTHBOUND.md §2.2, the panel-trio baseline).
#[must_use]
pub fn camera_panels() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "id": "overview", "title": "Overview", "order": 10, "scope": "instance",
            "widgets": [
                { "kind": "summary", "fields": ["state", "connected", "paused", "backend"] },
                { "kind": "commandSummary", "actions": ["sb/reconnect", "sb/pause", "sb/resume"] }
            ],
            "verbs": ["sb/status", "sb/reconnect", "sb/pause", "sb/resume"]
        }),
        serde_json::json!({
            "id": "signals", "title": "Cameras", "order": 20, "scope": "instance",
            "widgets": [ { "kind": "cameraRoster" }, { "kind": "captureSurface" } ],
            "verbs": ["sb/list", "sb/status", "sb/capture", "sb/capture-status"]
        }),
        serde_json::json!({
            "id": "diagnostics", "title": "Diagnostics", "order": 30, "scope": "instance",
            "widgets": [ { "kind": "treeBrowser" }, { "kind": "keyValueList" } ],
            "verbs": ["sb/discover", "sb/queue-status"]
        }),
    ]
}

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
                && replacement.instances.iter().any(|camera| match &camera.backend {
                    crate::config::BackendConfig::OnvifRtsp(onvif) => {
                        onvif.credentials.is_some() || onvif.tls.ca.is_some()
                    }
                    crate::config::BackendConfig::Rtsp(rtsp) => {
                        rtsp.credentials.is_some() || rtsp.tls.ca.is_some()
                    }
                    _ => false,
                })
            {
                return Err(crate::CameraError::Config {
                    path: "component.instances[].backend".to_string(),
                    message: "camera secret references require credentials configured at component startup"
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
    // The detail a CALLER may see, not the one a log may. `Display` on the three variants that wrap a
    // foreign error is written for a developer, and this string leaves the component.
    let message: String = error
        .operator_detail()
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

/// Tells the operator, ONCE per camera, that this transport cannot carry the preview they configured.
///
/// A clamp is a fact about the deployment, not an event: the same configuration is deployed to
/// Greengrass and to Kubernetes, and on Greengrass a `large` preview simply cannot be put on the
/// wire. Refusing to start would be a hostile way to say so, and saying it on every capture would
/// bury it -- 45 captures an hour per camera, all reporting the same unchanging fact. So it is said
/// here: at startup, and again after a reload, because a reload can introduce a new one.
fn log_thumbnail_clamps(config: &AdapterConfig, policy: crate::thumbnail::ThumbnailPolicy) {
    for notice in crate::thumbnail::clamp_notices(config, policy) {
        tracing::warn!(
            instance = %notice.instance,
            profiles = %notice.profiles.join(", "),
            transport = ?policy.transport(),
            effective_size = ?notice.effective,
            "the configured thumbnail size is not carryable on this transport, because {}; \
             producing '{:?}' instead",
            policy.limit_reason(),
            notice.effective,
        );
    }
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
    terminal_jobs: u64,
    terminal_groups: u64,
    command_ledgers: u64,
    over_limit_jobs: u64,
}

impl RetentionSweep {
    fn reclaimed(self) -> u64 {
        self.terminal_jobs
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
        for verb in camera_command_verbs() {
            let router = Arc::clone(self);
            inbox.register_outcome(
                verb,
                outcome_handler(move |request, deferred| {
                    let router = Arc::clone(&router);
                    async move { router.dispatch(verb, request, deferred).await }
                }),
            )?;
        }
        // The edge-console panel trio (overview / signals / diagnostics). Registered on the same inbox
        // as the verbs, before the acknowledged subscription begins, so the descriptor surface is
        // advertised atomically with the command surface it drives.
        for panel in camera_panels() {
            inbox.register_panel(panel)?;
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
        // The wire codes come from `ErrorCode::as_str`, which is the one place that decides what a
        // code is called. These three were hand-typed, so the enum and the router each held their own
        // opinion of the spelling -- and a renamed variant would have left these three quietly emitting
        // the old string, which is the sort of drift nothing fails on until an operator's alarm rule
        // stops matching.
        if self.stopping.load(Ordering::Acquire) {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::ComponentStopping.as_str(),
                "the camera adapter is shutting down",
            ));
        }
        let service = match self.service.read() {
            Ok(slot) => slot.clone(),
            Err(_) => {
                return CommandOutcome::ImmediateError(CommandError::new(
                    crate::ErrorCode::BackendError.as_str(),
                    "the camera command router is unavailable",
                ));
            }
        };
        match service {
            Some(service) => service.handle_camera_command(verb, request, deferred).await,
            None => CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::DeviceUnavailable.as_str(),
                "the camera adapter is still starting",
            )),
        }
    }
}
