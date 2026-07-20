//! Bounded health/readiness state and typed camera-health observations.
//!
//! This module intentionally contains no protocol URLs, paths, serial numbers, capture IDs, or
//! arbitrary error text. Runtime adapters translate these typed values to EdgeCommons health and
//! metric facades without introducing high-cardinality dimensions.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// Stable reason that readiness is currently false.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReadinessBlocker {
    /// Initial configuration has not passed component validation.
    Configuration,
    /// Catalog migration/recovery/integrity has not completed.
    Catalog,
    /// Output root is unavailable or unsafe.
    Output,
    /// Command subscription has not reached acknowledged ACTIVE state.
    CommandPlane,
    /// Camera supervisors have not been constructed.
    Supervisors,
    /// No enabled camera instance was accepted.
    NoAcceptedCamera,
    /// Durable state cannot reserve the next bounded terminal record.
    StateCapacity,
    /// Ordered component shutdown has begun.
    Stopping,
}

/// Complete low-cardinality readiness gate state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReadinessSnapshot {
    /// Component-specific initial config validation completed.
    pub configuration_validated: bool,
    /// Catalog opened, passed integrity checks, and recovery completed.
    pub catalog_recovered: bool,
    /// Output root capability and free-space checks succeeded.
    pub output_usable: bool,
    /// Command inbox is acknowledged ACTIVE.
    pub command_plane_active: bool,
    /// All accepted camera supervisors were constructed.
    pub supervisors_created: bool,
    /// Number of enabled camera instances accepted at startup/reload.
    pub accepted_enabled_cameras: usize,
    /// State storage can commit the next bounded terminal record.
    pub state_capacity_available: bool,
    /// Ordered shutdown has started.
    pub stopping: bool,
}

impl ReadinessSnapshot {
    /// Returns blockers in stable startup-gate order.
    #[must_use]
    pub fn blockers(&self) -> Vec<ReadinessBlocker> {
        let mut blockers = Vec::with_capacity(8);
        if !self.configuration_validated {
            blockers.push(ReadinessBlocker::Configuration);
        }
        if !self.catalog_recovered {
            blockers.push(ReadinessBlocker::Catalog);
        }
        if !self.output_usable {
            blockers.push(ReadinessBlocker::Output);
        }
        if !self.command_plane_active {
            blockers.push(ReadinessBlocker::CommandPlane);
        }
        if !self.supervisors_created {
            blockers.push(ReadinessBlocker::Supervisors);
        }
        if self.accepted_enabled_cameras == 0 {
            blockers.push(ReadinessBlocker::NoAcceptedCamera);
        }
        if !self.state_capacity_available {
            blockers.push(ReadinessBlocker::StateCapacity);
        }
        if self.stopping {
            blockers.push(ReadinessBlocker::Stopping);
        }
        blockers
    }

    /// Readiness does not depend on every camera being online, nor on the messaging plane.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.blockers().is_empty()
    }
}

/// Atomically updated readiness state with a watch stream for the runtime.
#[derive(Clone)]
pub struct ReadinessTracker {
    state: Arc<RwLock<ReadinessSnapshot>>,
    changes: watch::Sender<ReadinessSnapshot>,
}

impl Default for ReadinessTracker {
    fn default() -> Self {
        Self::new(ReadinessSnapshot::default())
    }
}

impl ReadinessTracker {
    /// Creates a tracker from an explicit initial gate state.
    #[must_use]
    pub fn new(initial: ReadinessSnapshot) -> Self {
        let (changes, _receiver) = watch::channel(initial.clone());
        Self {
            state: Arc::new(RwLock::new(initial)),
            changes,
        }
    }

    /// Updates all related gates under one lock and publishes exactly one resulting snapshot.
    pub fn update(&self, mutate: impl FnOnce(&mut ReadinessSnapshot)) {
        let next = {
            let mut state = self
                .state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut candidate = state.clone();
            mutate(&mut candidate);
            *state = candidate.clone();
            candidate
        };
        self.changes.send_replace(next);
    }

    /// Current immutable snapshot.
    #[must_use]
    pub fn snapshot(&self) -> ReadinessSnapshot {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Subscribes to later atomic snapshots.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<ReadinessSnapshot> {
        self.changes.subscribe()
    }
}

/// Typed values for the standard per-instance `southbound_health` metric.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SouthboundHealthSample {
    /// One when the current camera session is online, otherwise zero.
    pub connection_state: u8,
    /// Time the last terminal announcement took to reach the transport.
    pub publish_latency_ms: Option<u64>,
    /// Last capture/status round-trip latency.
    pub poll_latency_ms: Option<u64>,
    /// Read/status errors since the previous emission.
    pub read_errors: u64,
    /// One when no successful observation exists inside the stale threshold.
    pub stale_signals: u8,
    /// Reconnects since the previous emission.
    pub reconnects: u64,
}

/// Per-camera accumulator that drains interval counters on emission.
#[derive(Debug)]
pub struct CameraHealthTracker {
    created_at: Instant,
    last_success: Option<Instant>,
    publish_latency_ms: Option<u64>,
    poll_latency_ms: Option<u64>,
    read_errors: u64,
    reconnects: u64,
}

impl CameraHealthTracker {
    /// Starts a tracker with no successful camera observation.
    #[must_use]
    pub fn new(now: Instant) -> Self {
        Self {
            created_at: now,
            last_success: None,
            publish_latency_ms: None,
            poll_latency_ms: None,
            read_errors: 0,
            reconnects: 0,
        }
    }

    /// Records one successful capture/status observation and its bounded latency.
    pub fn observe_success(&mut self, now: Instant, poll_latency: Duration) {
        self.last_success = Some(now);
        self.poll_latency_ms = Some(duration_millis(poll_latency));
    }

    /// Records how long the last terminal announcement took to reach the transport.
    pub fn observe_publish(&mut self, latency: Duration) {
        self.publish_latency_ms = Some(duration_millis(latency));
    }

    /// Increments the interval read-error counter without wrapping.
    pub fn observe_read_error(&mut self) {
        self.read_errors = self.read_errors.saturating_add(1);
    }

    /// Increments the interval reconnect counter without wrapping.
    pub fn observe_reconnect(&mut self) {
        self.reconnects = self.reconnects.saturating_add(1);
    }

    /// Emits a sample and resets only interval counters. Last observations remain gauges.
    pub fn take_sample(
        &mut self,
        online: bool,
        now: Instant,
        stale_after: Duration,
    ) -> SouthboundHealthSample {
        let observation = self.last_success.unwrap_or(self.created_at);
        let stale = now.saturating_duration_since(observation) >= stale_after;
        let sample = SouthboundHealthSample {
            connection_state: u8::from(online),
            publish_latency_ms: self.publish_latency_ms,
            poll_latency_ms: self.poll_latency_ms,
            read_errors: self.read_errors,
            stale_signals: u8::from(stale),
            reconnects: self.reconnects,
        };
        self.read_errors = 0;
        self.reconnects = 0;
        sample
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The fleet's southbound health, one tracker per camera.
///
/// `CameraHealthTracker` existed, was tested, and was called by nothing -- the third subsystem in this
/// codebase found fully built and wired to nothing, after retention and the capture metrics. This is
/// the thing that calls it.
#[derive(Debug, Default)]
pub struct FleetHealth {
    cameras: std::sync::Mutex<std::collections::BTreeMap<String, CameraHealthTracker>>,
}

impl FleetHealth {
    /// Applies an observation to one camera, creating its tracker on first sight.
    fn observe(&self, instance: &str, observation: impl FnOnce(&mut CameraHealthTracker)) {
        let mut cameras = self
            .cameras
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tracker = cameras
            .entry(instance.to_owned())
            .or_insert_with(|| CameraHealthTracker::new(Instant::now()));
        observation(tracker);
    }

    /// A camera answered: a frame was acquired, and this is how long the round-trip took.
    pub fn observed_success(&self, instance: &str, poll_latency: Duration) {
        self.observe(instance, |tracker| {
            tracker.observe_success(Instant::now(), poll_latency);
        });
    }

    /// A camera failed to answer.
    pub fn observed_read_error(&self, instance: &str) {
        self.observe(instance, CameraHealthTracker::observe_read_error);
    }

    /// A camera's session was re-established.
    pub fn observed_reconnect(&self, instance: &str) {
        self.observe(instance, CameraHealthTracker::observe_reconnect);
    }

    /// A terminal announcement for this camera reached the transport, and this is how long it took.
    ///
    /// The measurement is the time to hand the message to the transport, because that is all a
    /// fire-and-forget publication can honestly claim to know: nothing waits for a broker
    /// acknowledgement any more, so there is no delivery time to report.
    pub fn observed_publish(&self, instance: &str, latency: Duration) {
        self.observe(instance, |tracker| tracker.observe_publish(latency));
    }

    /// Drains one camera's interval counters into a sample.
    ///
    /// `stale_after` is `healthThresholds.staleSignalSecs`, which until now decided nothing at all.
    pub fn sample(
        &self,
        instance: &str,
        online: bool,
        stale_after: Duration,
    ) -> SouthboundHealthSample {
        let mut cameras = self
            .cameras
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tracker = cameras
            .entry(instance.to_owned())
            .or_insert_with(|| CameraHealthTracker::new(Instant::now()));
        tracker.take_sample(online, Instant::now(), stale_after)
    }

    /// Retires a camera a reload removed, so its tracker cannot outlive it.
    pub fn forget(&self, instance: &str) {
        self.cameras
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(instance);
    }
}

/// A `<name>Total` (monotonic since start) + `<name>Interval` (since the previous emit; reset on
/// emit) counter pair — the operational-family convention every EdgeCommons adapter follows
/// (SOUTHBOUND.md §5), so a fleet dashboard reads every adapter the same way.
#[derive(Debug, Default, Clone, Copy)]
struct Pair {
    total: f64,
    interval: f64,
}

impl Pair {
    fn add(&mut self, value: f64) {
        self.total += value;
        self.interval += value;
    }

    /// Writes both measures into `out` and RESETS the interval — the emit convention.
    fn drain_into(&self, out: &mut std::collections::HashMap<String, f64>, prefix: &str) {
        out.insert(format!("{prefix}Total"), self.total);
        out.insert(format!("{prefix}Interval"), self.interval);
    }

    fn reset_interval(&mut self) {
        self.interval = 0.0;
    }
}

/// The `(verb, result)` command counters for one `(instance, verb, result)` cell of `CameraCommand`.
#[derive(Debug, Default)]
struct CmdCounters {
    requests: Pair,
    errors: Pair,
    latency_ms: f64,
}

/// The `camera_captures` counter set, one [`Pair`] per capture-lifecycle measure. Keyed by the
/// `&'static str` measure name the job hooks already pass to [`CaptureMetrics::count`].
type CaptureCounters = std::collections::BTreeMap<&'static str, Pair>;

/// A `(instance, verb, result)` key into the `CameraCommand` family.
type CommandKey = (String, &'static str, &'static str);

/// The component's metric surface.
///
/// Three families, following the SOUTHBOUND.md §5 operational-family pattern:
///
/// * `camera_captures` COUNTS the capture lifecycle on the job hooks the runtime fires — every
///   measure is a `(Total, Interval)` counter pair, accumulated in-process and drained on the metric
///   sampler's tick. The interval counter is what preserves the "every event must be seen" guarantee
///   the immediate-emit design was reaching for: a capture that succeeded and one that failed between
///   two drains both land in the interval, so nothing is lost — the drain is a reset, not a sample.
/// * `camera_queue` SAMPLES what the component is holding right now — a level, not an event, so its
///   measures are single gauges (there is nothing to miss between samples).
/// * `CameraCommand` counts the `sb/*` command surface, dimensioned `instance` × `verb` × `result`,
///   with `(Total, Interval)` request/error pairs plus a latency sum — the reference command family.
///
/// **Recorded cardinality decision (DESIGN.md).** `camera_captures` / `camera_queue` stay
/// **fleet-scoped** (no per-instance dimension): a 256-camera fleet would otherwise mint 256 streams
/// PER MEASURE on the highest-frequency families, which is how a metrics bill and a Prometheus server
/// both die. Per-camera capture state is answered by `sb/queue-status` and the per-instance
/// connectivity the heartbeat publishes. `CameraCommand` DOES carry the `instance` dimension the
/// family pattern prescribes, because commands are operator-frequency, not capture-frequency; its
/// `(instance, verb, result)` cells are defined **lazily on first use** rather than pre-populated, so
/// its cardinality tracks what an operator actually commands rather than 256 × 16 × 2 cold streams.
pub struct CaptureMetrics {
    metrics: Arc<dyn edgecommons::metrics::MetricService>,
    /// Serializes the define-then-emit pair that `southbound_health` needs. See [`Self::emit_health`].
    health: tokio::sync::Mutex<()>,
    /// Accumulated `camera_captures` counter pairs, drained on the metric sampler's tick.
    captures: std::sync::Mutex<CaptureCounters>,
    /// Accumulated `CameraCommand` cells, drained on the metric sampler's tick.
    commands: std::sync::Mutex<std::collections::BTreeMap<CommandKey, CmdCounters>>,
}

/// Capture-lifecycle counters, drained as `(Total, Interval)` pairs on the sampler tick.
pub const CAPTURE_METRIC: &str = "camera_captures";
/// Sampled levels: what the component is holding right now.
pub const QUEUE_METRIC: &str = "camera_queue";
/// The standard per-instance southbound metric every adapter in the ecosystem emits.
pub const HEALTH_METRIC: &str = "southbound_health";
/// The operational command family: `sb/*` request/error counts and latency, by `instance`/`verb`/`result`.
pub const COMMAND_METRIC: &str = "CameraCommand";
/// The `result` dimension value for a command that succeeded.
pub const RESULT_SUCCESS: &str = "success";
/// The `result` dimension value for a command that failed.
pub const RESULT_ERROR: &str = "error";

/// Every `camera_captures` measure. Each becomes a `<measure>Total` + `<measure>Interval` pair.
///
/// The set is the parity anchor: `define_captures` and the drain both read it, so a measure the job
/// hooks count but this list omits fails to define, and one this list carries but nothing counts
/// simply reports zero — never a silent drop.
pub const CAPTURE_MEASURES: [&str; 10] = [
    "queued",
    "started",
    "succeeded",
    "failed",
    "cancelled",
    "interrupted",
    ANNOUNCEMENT_FAILED_MEASURE,
    THUMBNAIL_FAILED_MEASURE,
    THUMBNAIL_DROPPED_MEASURE,
    ANNOUNCEMENT_RETRIED_WITHOUT_PREVIEW_MEASURE,
];
/// Terminal results that are durable but were never announced.
///
/// The announcement is best-effort, so a broker that is down costs announcements and nothing else.
/// This is how many were lost -- the only place that loss is visible, and the reason it is a measure
/// on `camera_captures` rather than a log line nobody aggregates.
pub const ANNOUNCEMENT_FAILED_MEASURE: &str = "announcementFailed";
/// Configured thumbnails that could not be rendered or encoded at all.
///
/// The capture SUCCEEDED and was announced; only the preview could not be made. It is counted rather
/// than merely logged because a camera whose every thumbnail fails is a camera producing frames this
/// component cannot interpret -- worth an alarm, and invisible in a log nobody aggregates.
pub const THUMBNAIL_FAILED_MEASURE: &str = "thumbnailFailed";
/// Thumbnails that rendered but were too large for the announcement's byte ceiling.
///
/// Distinct from `thumbnailFailed` on purpose: this one says the picture was fine and the BUDGET was
/// not, which is an operator's cue to configure a smaller size rather than to suspect the camera.
pub const THUMBNAIL_DROPPED_MEASURE: &str = "thumbnailDropped";
/// Announcements that would not publish while carrying a preview, and were published again without it.
///
/// This is NOT `thumbnailDropped`, and conflating the two sends an operator to the wrong knob. That
/// one means the component measured the preview against the transport's budget and found it too big --
/// shrink the configured size. This one means the component believed the preview would fit, the
/// transport disagreed, and the result went out unadorned rather than not at all.
///
/// It is the honest signal for a transport whose limit the component has MIS-MODELLED, which has
/// already happened once: a `medium` preview sailed past every check and then met the Greengrass IPC
/// client's static 10,000-byte send buffer. A broker outage also lands here -- and lands on
/// `announcementFailed` too, which is the pair an operator should read together.
pub const ANNOUNCEMENT_RETRIED_WITHOUT_PREVIEW_MEASURE: &str = "announcementRetriedWithoutPreview";

impl CaptureMetrics {
    /// Defines the fleet-scoped metrics against the component's metric service.
    ///
    /// `camera_captures` and `camera_queue` are defined once (no dimensions vary); `CameraCommand`
    /// carries the `instance`/`verb`/`result` dimensions, so its cells are defined lazily at emit time
    /// like `southbound_health`.
    #[must_use]
    pub fn new(metrics: Arc<dyn edgecommons::metrics::MetricService>) -> Self {
        // camera_captures: every measure is a Total/Interval counter pair (SOUTHBOUND.md §5 pattern).
        let mut captures = edgecommons::metrics::MetricBuilder::create(CAPTURE_METRIC);
        for measure in CAPTURE_MEASURES {
            captures = captures
                .add_measure(format!("{measure}Total"), "Count", 60)
                .add_measure(format!("{measure}Interval"), "Count", 60);
        }
        metrics.define_metric(captures.build());
        // camera_queue: sampled levels — single gauges, nothing to pair.
        metrics.define_metric(
            edgecommons::metrics::MetricBuilder::create(QUEUE_METRIC)
                .add_measure("dispatchQueued", "Count", 60)
                .add_measure("durableBacklog", "Count", 60)
                .add_measure("durableInFlight", "Count", 60)
                .add_measure("availableAcquisitions", "Count", 60)
                .add_measure("availableEncoders", "Count", 60)
                .add_measure("availableWriters", "Count", 60)
                .add_measure("availableMemoryBytes", "Bytes", 60)
                .add_measure("outstandingDiskBytes", "Bytes", 60)
                .add_measure("camerasOnline", "Count", 60)
                .add_measure("camerasConfigured", "Count", 60)
                .build(),
        );
        Self {
            metrics,
            health: tokio::sync::Mutex::new(()),
            captures: std::sync::Mutex::new(CaptureCounters::new()),
            commands: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    /// Emits one camera's standard `southbound_health` sample, dimensioned by `instance`.
    ///
    /// This is the metric the whole ecosystem alarms on -- SOUTHBOUND §5 and DESIGN §19.1 -- and the
    /// adapter emitted nothing at all. `CameraHealthTracker` was written to produce exactly these
    /// measures and had no callers, so `healthThresholds.staleSignalSecs` decided nothing and no
    /// camera could report itself stale.
    ///
    /// The define-then-emit pair is not an accident, and it is worth explaining. The core metric API
    /// carries dimensions on the metric DEFINITION and keys definitions by name
    /// (`HashMap<String, Metric>`), while `emit_metric(name, values)` carries only values -- so one
    /// name cannot be emitted with different dimension values, which is precisely what "dimensioned
    /// by instance" requires of a multi-camera adapter. Redefining immediately before emitting is the
    /// only way to say `instance=cam-03` with today's API, and it is safe only while the pair is
    /// atomic. Hence the lock, and hence the rule: EVERY `southbound_health` emission goes through
    /// this method. The real fix belongs in the core library (dimensions at emit time), and until it
    /// lands this is the honest way to keep the contract.
    pub async fn emit_health(&self, instance: &str, sample: &SouthboundHealthSample, now: bool) {
        let metric = edgecommons::metrics::MetricBuilder::create(HEALTH_METRIC)
            .add_dimension("instance", instance)
            .add_measure("connectionState", "Count", 1)
            .add_measure("publishLatencyMs", "Milliseconds", 1)
            .add_measure("pollLatencyMs", "Milliseconds", 1)
            .add_measure("readErrors", "Count", 60)
            .add_measure("staleSignals", "Count", 60)
            .add_measure("reconnects", "Count", 60)
            .build();

        let mut values = std::collections::HashMap::with_capacity(6);
        values.insert(
            "connectionState".to_owned(),
            f64::from(sample.connection_state),
        );
        values.insert("readErrors".to_owned(), sample.read_errors as f64);
        values.insert("staleSignals".to_owned(), f64::from(sample.stale_signals));
        values.insert("reconnects".to_owned(), sample.reconnects as f64);
        // A latency that has never been observed is absent, not zero: zero is a real measurement and
        // would read as a camera answering instantly.
        if let Some(latency) = sample.publish_latency_ms {
            values.insert("publishLatencyMs".to_owned(), latency as f64);
        }
        if let Some(latency) = sample.poll_latency_ms {
            values.insert("pollLatencyMs".to_owned(), latency as f64);
        }

        let _serialized = self.health.lock().await;
        self.metrics.define_metric(metric);
        let emitted = if now {
            self.metrics.emit_metric_now(HEALTH_METRIC, values).await
        } else {
            self.metrics.emit_metric(HEALTH_METRIC, values).await
        };
        if let Err(error) = emitted {
            tracing::warn!(
                instance,
                error = %error,
                "southbound health metric could not be emitted"
            );
        }
    }

    /// The current monotonic total accumulated for a `camera_captures` measure, without emitting.
    ///
    /// A test seam only: it lets a test observe that a capture has been counted without draining
    /// (which would emit and reset the interval), so the count-landed race can be polled cleanly.
    #[cfg(test)]
    #[must_use]
    pub fn captured_total(&self, measure: &str) -> f64 {
        self.captures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(measure)
            .map_or(0.0, |pair| pair.total)
    }

    /// Records one `camera_captures` event into its `(Total, Interval)` accumulator.
    ///
    /// No longer emits at the call site: the interval counter accumulates every event since the last
    /// drain, so nothing is lost between the sampler's ticks — a success and a failure in the same
    /// window both land, which is the guarantee the old immediate-emit design was reaching for. Kept
    /// `async` (and infallible) so the job-hook call sites are unchanged.
    #[allow(clippy::unused_async)]
    pub async fn count(&self, measure: &'static str) {
        self.captures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(measure)
            .or_default()
            .add(1.0);
    }

    /// Records one `sb/*` command outcome into the `CameraCommand` family cell for its
    /// `(instance, verb, result)`. Lazily creates the cell on first sight — see the cardinality note
    /// on [`CaptureMetrics`].
    pub fn record_command(&self, instance: &str, verb: &'static str, ok: bool, latency_ms: u64) {
        let result = if ok { RESULT_SUCCESS } else { RESULT_ERROR };
        let mut commands = self
            .commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cell = commands.entry((instance.to_owned(), verb, result)).or_default();
        cell.requests.add(1.0);
        cell.latency_ms += latency_ms as f64;
        if !ok {
            cell.errors.add(1.0);
        }
    }

    /// Drains the `camera_captures` counters to the metric target and resets the intervals.
    ///
    /// Best effort: a metric target that is unhappy must never disturb the component, so this reports
    /// and moves on. Called on the metric sampler's tick.
    pub async fn drain_captures(&self) {
        let values = {
            let mut captures = self
                .captures
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Always emit the full measure set (zeros for what has not happened), so a dashboard sees
            // a stable schema rather than measures that blink in and out of existence.
            let mut values = std::collections::HashMap::with_capacity(CAPTURE_MEASURES.len() * 2);
            for measure in CAPTURE_MEASURES {
                let pair = captures.entry(measure).or_default();
                pair.drain_into(&mut values, measure);
                pair.reset_interval();
            }
            values
        };
        if let Err(error) = self.metrics.emit_metric(CAPTURE_METRIC, values).await {
            tracing::warn!(error = %error, "camera capture metrics could not be emitted");
        }
    }

    /// Drains every accumulated `CameraCommand` cell to the metric target and resets the intervals.
    ///
    /// Each `(instance, verb, result)` cell is defined immediately before it is emitted — the same
    /// define-then-emit pattern `southbound_health` uses, and for the same reason: the core metric API
    /// carries dimensions on the definition. Called on the metric sampler's tick.
    pub async fn drain_commands(&self) {
        let cells: Vec<(CommandKey, std::collections::HashMap<String, f64>)> = {
            let mut commands = self
                .commands
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            commands
                .iter_mut()
                .map(|(key, cell)| {
                    let mut values = std::collections::HashMap::with_capacity(5);
                    cell.requests.drain_into(&mut values, "commandRequests");
                    cell.errors.drain_into(&mut values, "commandErrors");
                    values.insert("commandLatencyMs".to_owned(), cell.latency_ms);
                    cell.requests.reset_interval();
                    cell.errors.reset_interval();
                    cell.latency_ms = 0.0;
                    (key.clone(), values)
                })
                .collect()
        };
        let _serialized = self.health.lock().await;
        for ((instance, verb, result), values) in cells {
            let metric = edgecommons::metrics::MetricBuilder::create(COMMAND_METRIC)
                .add_dimension("instance", &instance)
                .add_dimension("verb", verb)
                .add_dimension("result", result)
                .add_measure("commandRequestsTotal", "Count", 60)
                .add_measure("commandRequestsInterval", "Count", 60)
                .add_measure("commandErrorsTotal", "Count", 60)
                .add_measure("commandErrorsInterval", "Count", 60)
                .add_measure("commandLatencyMs", "Milliseconds", 60)
                .build();
            self.metrics.define_metric(metric);
            if let Err(error) = self.metrics.emit_metric(COMMAND_METRIC, values).await {
                tracing::warn!(
                    instance,
                    verb,
                    result,
                    error = %error,
                    "camera command metric could not be emitted"
                );
            }
        }
    }

    /// Emits one sample of what the component is currently holding.
    pub async fn sample_queue(&self, values: std::collections::HashMap<String, f64>) {
        if let Err(error) = self.metrics.emit_metric(QUEUE_METRIC, values).await {
            tracing::warn!(error = %error, "camera queue metric could not be emitted");
        }
    }
}

/// The `camera_captures` measure a terminal state counts against.
#[must_use]
pub const fn terminal_measure(state: crate::model::JobState) -> Option<&'static str> {
    match state {
        crate::model::JobState::Succeeded => Some("succeeded"),
        crate::model::JobState::Failed => Some("failed"),
        crate::model::JobState::Cancelled => Some("cancelled"),
        crate::model::JobState::Interrupted => Some("interrupted"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {

    /// Interval counters drain on emission; last-seen values do not.
    ///
    /// The distinction is the whole reason `CameraHealthTracker` exists rather than a bag of numbers:
    /// `readErrors` and `reconnects` answer "what happened since you last asked", so reporting them
    /// twice would double-count an outage. `pollLatencyMs` answers "how is it now", so a camera that
    /// has gone quiet must keep reporting the last round-trip it managed, not silently forget it.
    #[test]
    fn a_health_sample_drains_the_counters_and_keeps_the_gauges() {
        let health = FleetHealth::default();
        health.observed_success("camera-a", Duration::from_millis(42));
        health.observed_read_error("camera-a");
        health.observed_read_error("camera-a");
        health.observed_reconnect("camera-a");
        health.observed_publish("camera-a", Duration::from_millis(7));

        let first = health.sample("camera-a", true, Duration::from_secs(300));
        assert_eq!(first.connection_state, 1);
        assert_eq!(first.read_errors, 2);
        assert_eq!(first.reconnects, 1);
        assert_eq!(first.poll_latency_ms, Some(42));
        assert_eq!(first.publish_latency_ms, Some(7));
        assert_eq!(first.stale_signals, 0, "the camera answered just now");

        let second = health.sample("camera-a", true, Duration::from_secs(300));
        assert_eq!(second.read_errors, 0, "an interval counter reports once");
        assert_eq!(second.reconnects, 0, "an interval counter reports once");
        assert_eq!(
            second.poll_latency_ms,
            Some(42),
            "the last round-trip is a gauge, and a camera that has gone quiet still has one"
        );
    }

    /// A camera nobody has ever heard from is stale, not healthy.
    ///
    /// The tracker starts with no successful observation at all, and the temptation is to treat that
    /// as "fine so far". A camera that has never produced a frame is the one an operator most wants
    /// to hear about.
    #[test]
    fn a_camera_that_has_never_answered_is_stale_from_the_start() {
        let health = FleetHealth::default();
        let sample = health.sample("camera-a", true, Duration::ZERO);
        assert_eq!(sample.connection_state, 1);
        assert_eq!(
            sample.stale_signals, 1,
            "silence since startup is silence, whatever the session says"
        );
        assert_eq!(sample.poll_latency_ms, None);

        health.forget("camera-a");
    }
    /// Every terminal state a capture can reach must be counted, and nothing else may be.
    ///
    /// The counters are what an operator watches, so a terminal that maps to no measure is a capture
    /// that ended and was never counted -- invisible in exactly the way the whole component was
    /// before it emitted anything at all.
    #[test]
    fn every_terminal_state_counts_against_exactly_one_measure() {
        use crate::model::JobState;

        assert_eq!(terminal_measure(JobState::Succeeded), Some("succeeded"));
        assert_eq!(terminal_measure(JobState::Failed), Some("failed"));
        assert_eq!(terminal_measure(JobState::Cancelled), Some("cancelled"));
        assert_eq!(terminal_measure(JobState::Interrupted), Some("interrupted"));

        for state in [
            JobState::Accepted,
            JobState::Queued,
            JobState::Acquiring,
            JobState::Encoding,
            JobState::Persisting,
        ] {
            assert!(
                !state.is_terminal(),
                "a state that counts against nothing must be one the capture can still leave"
            );
            assert_eq!(
                terminal_measure(state),
                None,
                "a capture still in flight has not ended, and must not be counted as though it had"
            );
        }
    }

    /// A metric target that is unhappy must never be able to fail a capture.
    #[tokio::test]
    async fn a_failing_metric_target_is_reported_and_survived() {
        use edgecommons::metrics::MetricService;

        struct Broken;

        #[async_trait::async_trait]
        impl edgecommons::metrics::MetricService for Broken {
            fn define_metric(&self, _metric: edgecommons::metrics::Metric) {}
            fn is_metric_defined(&self, _name: &str) -> bool {
                true
            }
            async fn emit_metric(
                &self,
                _name: &str,
                _values: std::collections::HashMap<String, f64>,
            ) -> edgecommons::Result<()> {
                Err(edgecommons::EdgeCommonsError::Metrics(
                    "metric target is unavailable".to_owned(),
                ))
            }
            async fn emit_metric_now(
                &self,
                _name: &str,
                _values: std::collections::HashMap<String, f64>,
            ) -> edgecommons::Result<()> {
                Err(edgecommons::EdgeCommonsError::Metrics(
                    "metric target is unavailable".to_owned(),
                ))
            }
            async fn flush_metrics(&self) -> edgecommons::Result<()> {
                Ok(())
            }
            async fn shutdown(&self) {}
        }

        let broken = Arc::new(Broken);
        let metrics = CaptureMetrics::new(Arc::clone(&broken) as Arc<dyn MetricService>);

        // Neither call may panic or propagate: a capture that succeeded must not be reported as
        // failed because the metrics backend was down.
        metrics.count("succeeded").await;
        metrics
            .sample_queue(std::collections::HashMap::from([(
                "durableBacklog".to_owned(),
                1.0,
            )]))
            .await;

        // `CaptureMetrics` defines both metrics through this same service, so a target that refuses
        // to emit must still have accepted the definitions -- otherwise the failure being survived
        // here would be the wrong one.
        assert!(broken.is_metric_defined(CAPTURE_METRIC));
        assert!(
            broken
                .emit_metric_now(CAPTURE_METRIC, std::collections::HashMap::new())
                .await
                .is_err(),
            "the immediate path must fail the same way the buffered one does"
        );
        assert!(broken.flush_metrics().await.is_ok());
        broken.shutdown().await;
    }

    /// One emission as the target saw it: metric name, whether the immediate path was used, values.
    type Emission = (String, bool, std::collections::HashMap<String, f64>);

    /// A metric target that remembers exactly what it was asked to define and to emit.
    #[derive(Default)]
    struct RecordingMetrics {
        defined: std::sync::Mutex<Vec<edgecommons::metrics::Metric>>,
        emitted: std::sync::Mutex<Vec<Emission>>,
        refuse: bool,
    }

    impl RecordingMetrics {
        fn last_emission(&self) -> Emission {
            self.emitted
                .lock()
                .unwrap()
                .last()
                .cloned()
                .expect("the metric target must have been asked to emit something")
        }

        fn last_definition(&self) -> edgecommons::metrics::Metric {
            self.defined
                .lock()
                .unwrap()
                .last()
                .cloned()
                .expect("the metric must have been defined before it was emitted")
        }

        fn record(
            &self,
            name: &str,
            immediate: bool,
            values: std::collections::HashMap<String, f64>,
        ) -> edgecommons::Result<()> {
            self.emitted
                .lock()
                .unwrap()
                .push((name.to_owned(), immediate, values));
            if self.refuse {
                return Err(edgecommons::EdgeCommonsError::Metrics(
                    "metric target is unavailable".to_owned(),
                ));
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl edgecommons::metrics::MetricService for RecordingMetrics {
        fn define_metric(&self, metric: edgecommons::metrics::Metric) {
            self.defined.lock().unwrap().push(metric);
        }
        fn is_metric_defined(&self, name: &str) -> bool {
            self.defined
                .lock()
                .unwrap()
                .iter()
                .any(|metric| metric.get_name() == name)
        }
        async fn emit_metric(
            &self,
            name: &str,
            values: std::collections::HashMap<String, f64>,
        ) -> edgecommons::Result<()> {
            self.record(name, false, values)
        }
        async fn emit_metric_now(
            &self,
            name: &str,
            values: std::collections::HashMap<String, f64>,
        ) -> edgecommons::Result<()> {
            self.record(name, true, values)
        }
        async fn flush_metrics(&self) -> edgecommons::Result<()> {
            Ok(())
        }
        async fn shutdown(&self) {}
    }

    /// A latency that has never been observed is absent, not zero.
    ///
    /// Zero is a real measurement. Emitting it for a camera that has never answered would publish
    /// "this camera replies instantly" as the round-trip of a camera that has, in fact, replied
    /// never -- and `publishLatencyMs`/`pollLatencyMs` are averaged by whatever consumes them, so one
    /// silent camera would quietly pull a fleet's latency toward zero and hide the outage.
    ///
    /// The dimension is asserted here too, because it is the reason this method exists at all: the
    /// core API keys definitions by name and carries dimensions on the DEFINITION, so a multi-camera
    /// adapter can only say `instance=camera-a` by redefining immediately before it emits.
    #[tokio::test]
    async fn southbound_health_emits_a_latency_it_has_observed_and_omits_one_it_has_not() {
        let target = Arc::new(RecordingMetrics::default());
        let metrics = CaptureMetrics::new(
            Arc::clone(&target) as Arc<dyn edgecommons::metrics::MetricService>
        );

        metrics
            .emit_health(
                "camera-a",
                &SouthboundHealthSample {
                    connection_state: 1,
                    publish_latency_ms: Some(7),
                    poll_latency_ms: Some(42),
                    read_errors: 2,
                    stale_signals: 0,
                    reconnects: 1,
                },
                true,
            )
            .await;

        let (name, immediate, values) = target.last_emission();
        assert_eq!(name, HEALTH_METRIC);
        assert!(immediate, "`now` must reach the immediate emission path");
        assert_eq!(values.get("publishLatencyMs"), Some(&7.0));
        assert_eq!(values.get("pollLatencyMs"), Some(&42.0));
        assert_eq!(values.get("connectionState"), Some(&1.0));
        assert_eq!(values.get("readErrors"), Some(&2.0));
        assert_eq!(values.get("reconnects"), Some(&1.0));
        assert_eq!(
            target.last_definition().get_dimensions().get("instance"),
            Some(&"camera-a".to_owned()),
            "an un-dimensioned health metric cannot tell an operator WHICH camera is unwell"
        );

        metrics
            .emit_health(
                "camera-b",
                &SouthboundHealthSample {
                    connection_state: 0,
                    publish_latency_ms: None,
                    poll_latency_ms: None,
                    read_errors: 0,
                    stale_signals: 1,
                    reconnects: 0,
                },
                false,
            )
            .await;

        let (_, immediate, values) = target.last_emission();
        assert!(!immediate, "a routine sample must use the buffered path");
        assert!(
            !values.contains_key("publishLatencyMs") && !values.contains_key("pollLatencyMs"),
            "a camera that has never answered must report no latency at all, not a latency of \
             zero: {values:?}"
        );
        assert_eq!(
            values.get("staleSignals"),
            Some(&1.0),
            "the silence itself is what must be reported instead"
        );
    }

    /// A metric target that is down must not be able to take the health loop down with it.
    ///
    /// `emit_health` is called on a timer for every camera in the fleet. It returns `()` on purpose:
    /// there is no caller who could do anything useful with an emission failure, and propagating one
    /// would stop the sweep -- so a metrics backend that is briefly unavailable would stop the
    /// component reporting on all 256 cameras, including the ones that are genuinely unwell.
    #[tokio::test]
    async fn a_health_sample_that_cannot_be_emitted_is_reported_and_survived() {
        use edgecommons::metrics::MetricService;

        let target = Arc::new(RecordingMetrics {
            refuse: true,
            ..RecordingMetrics::default()
        });
        let metrics = CaptureMetrics::new(
            Arc::clone(&target) as Arc<dyn edgecommons::metrics::MetricService>
        );
        let sample = SouthboundHealthSample {
            connection_state: 1,
            publish_latency_ms: Some(3),
            poll_latency_ms: Some(4),
            read_errors: 0,
            stale_signals: 0,
            reconnects: 0,
        };

        // Neither call may panic or propagate, and the second must still be attempted after the
        // first has failed: an emission failure is not sticky.
        metrics.emit_health("camera-a", &sample, true).await;
        metrics.emit_health("camera-b", &sample, false).await;

        {
            let attempted = target.emitted.lock().unwrap();
            assert_eq!(
                attempted.len(),
                2,
                "both cameras must have been offered to the failing target: {attempted:?}"
            );
            assert!(
                attempted.iter().all(|(name, ..)| name == HEALTH_METRIC),
                "the failure being survived here must be the health emission's own"
            );
        }

        // The target is a faithful stand-in for one that is merely unable to ship: it accepted the
        // definitions and it is still usable. If it had refused the definitions too, the failure
        // survived above would be a different failure from the one production sees.
        assert!(
            target.is_metric_defined(HEALTH_METRIC),
            "the health metric must be defined even when its emission cannot be shipped"
        );
        target
            .flush_metrics()
            .await
            .expect("a target that refuses an emission may still be flushed");
        target.shutdown().await;
    }

    use super::*;

    fn ready() -> ReadinessSnapshot {
        ReadinessSnapshot {
            configuration_validated: true,
            catalog_recovered: true,
            output_usable: true,
            command_plane_active: true,
            supervisors_created: true,
            accepted_enabled_cameras: 1,
            state_capacity_available: true,
            stopping: false,
        }
    }

    #[test]
    fn readiness_requires_every_startup_gate_but_not_camera_connectivity() {
        let snapshot = ready();
        assert!(snapshot.is_ready());
        assert!(snapshot.blockers().is_empty());

        let mut blocked = snapshot;
        blocked.accepted_enabled_cameras = 0;
        blocked.state_capacity_available = false;
        blocked.stopping = true;
        assert_eq!(
            blocked.blockers(),
            [
                ReadinessBlocker::NoAcceptedCamera,
                ReadinessBlocker::StateCapacity,
                ReadinessBlocker::Stopping,
            ]
        );
    }

    #[test]
    fn blockers_are_exhaustive_in_the_published_order() {
        let blocked = ReadinessSnapshot::default();
        assert_eq!(
            blocked.blockers(),
            [
                ReadinessBlocker::Configuration,
                ReadinessBlocker::Catalog,
                ReadinessBlocker::Output,
                ReadinessBlocker::CommandPlane,
                ReadinessBlocker::Supervisors,
                ReadinessBlocker::NoAcceptedCamera,
                ReadinessBlocker::StateCapacity,
            ]
        );
        let mut stopping = ready();
        stopping.stopping = true;
        assert_eq!(stopping.blockers(), [ReadinessBlocker::Stopping]);
    }

    #[tokio::test]
    async fn tracker_publishes_one_atomic_multi_gate_transition() {
        let tracker = ReadinessTracker::default();
        let mut receiver = tracker.subscribe();
        tracker.update(|state| {
            *state = ready();
        });
        receiver.changed().await.unwrap();
        assert!(receiver.borrow_and_update().is_ready());
        assert!(tracker.snapshot().is_ready());
    }

    #[test]
    fn camera_health_staleness_and_interval_counters_are_truthful() {
        let start = Instant::now();
        let mut tracker = CameraHealthTracker::new(start);
        tracker.observe_read_error();
        tracker.observe_reconnect();
        let stale = tracker.take_sample(
            false,
            start + Duration::from_secs(5),
            Duration::from_secs(5),
        );
        assert_eq!(stale.connection_state, 0);
        assert_eq!(stale.stale_signals, 1);
        assert_eq!(stale.read_errors, 1);
        assert_eq!(stale.reconnects, 1);

        let observed = start + Duration::from_secs(6);
        tracker.observe_success(observed, Duration::from_millis(12));
        tracker.observe_publish(Duration::from_millis(4));
        let healthy = tracker.take_sample(
            true,
            observed + Duration::from_secs(4),
            Duration::from_secs(5),
        );
        assert_eq!(healthy.connection_state, 1);
        assert_eq!(healthy.stale_signals, 0);
        assert_eq!(healthy.poll_latency_ms, Some(12));
        assert_eq!(healthy.publish_latency_ms, Some(4));
        assert_eq!(healthy.read_errors, 0);
        assert_eq!(healthy.reconnects, 0);
    }

    #[test]
    fn health_counters_saturate_and_duration_conversion_never_wraps() {
        let start = Instant::now();
        let mut tracker = CameraHealthTracker::new(start);
        tracker.read_errors = u64::MAX;
        tracker.reconnects = u64::MAX;
        tracker.observe_read_error();
        tracker.observe_reconnect();
        let sample = tracker.take_sample(true, start, Duration::from_secs(1));
        assert_eq!(sample.read_errors, u64::MAX);
        assert_eq!(sample.reconnects, u64::MAX);
        assert_eq!(duration_millis(Duration::MAX), u64::MAX);
    }

    // --- the operational-family pattern: camera_captures pairs + CameraCommand -----------------------

    /// A counter Pair is monotonic in total and resets its interval on drain.
    #[test]
    fn a_counter_pair_totals_monotonically_and_its_interval_resets() {
        let mut pair = Pair::default();
        pair.add(3.0);
        let mut out = std::collections::HashMap::new();
        pair.drain_into(&mut out, "x");
        assert_eq!(out["xTotal"], 3.0);
        assert_eq!(out["xInterval"], 3.0);
        pair.reset_interval();

        pair.add(2.0);
        let mut out2 = std::collections::HashMap::new();
        pair.drain_into(&mut out2, "x");
        assert_eq!(out2["xTotal"], 5.0, "the total is monotonic across drains");
        assert_eq!(out2["xInterval"], 2.0, "the interval carries only what accrued since the reset");
    }

    /// `count` accumulates on the job hooks; a drain emits every measure as a Total/Interval pair and
    /// resets the intervals -- so a second drain with no new events reports Total unchanged, Interval 0.
    #[tokio::test]
    async fn camera_captures_accumulates_and_drains_as_total_interval_pairs() {
        let target = Arc::new(RecordingMetrics::default());
        let metrics =
            CaptureMetrics::new(Arc::clone(&target) as Arc<dyn edgecommons::metrics::MetricService>);

        metrics.count("succeeded").await;
        metrics.count("succeeded").await;
        metrics.count("failed").await;
        assert_eq!(metrics.captured_total("succeeded"), 2.0, "counts accumulate without emitting");
        assert!(
            target.emitted.lock().unwrap().is_empty(),
            "counting must not emit -- the pattern drains on the sampler tick"
        );

        metrics.drain_captures().await;
        let (_, _, first) = target.last_emission();
        assert_eq!(first["succeededTotal"], 2.0);
        assert_eq!(first["succeededInterval"], 2.0);
        assert_eq!(first["failedTotal"], 1.0);
        assert_eq!(first["queuedTotal"], 0.0, "the full schema is always emitted, zeros included");

        metrics.drain_captures().await;
        let (_, _, second) = target.last_emission();
        assert_eq!(second["succeededTotal"], 2.0, "total is monotonic across drains");
        assert_eq!(second["succeededInterval"], 0.0, "the interval reset on the previous drain");
    }

    /// A command outcome lands in its `(instance, verb, result)` cell; a drain defines and emits each
    /// cell, a failure counts BOTH a request and an error, and the definitions carry the exact dims.
    #[tokio::test]
    async fn camera_command_records_by_instance_verb_result_and_drains_with_those_dimensions() {
        use edgecommons::metrics::MetricService as _;

        let target = Arc::new(RecordingMetrics::default());
        let metrics =
            CaptureMetrics::new(Arc::clone(&target) as Arc<dyn edgecommons::metrics::MetricService>);

        metrics.record_command("camera-a", "sb/status", true, 4);
        metrics.record_command("camera-a", "sb/status", false, 6);
        metrics.drain_commands().await;

        // The success cell (errors = 0) and the error cell (a request AND an error, latency summed).
        let cells: Vec<std::collections::HashMap<String, f64>> = target
            .emitted
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _, _)| name == COMMAND_METRIC)
            .map(|(_, _, values)| values.clone())
            .collect();
        assert_eq!(cells.len(), 2, "one cell per (verb, result) touched");
        let success = cells
            .iter()
            .find(|v| v["commandErrorsTotal"] == 0.0)
            .expect("a success cell");
        assert_eq!(success["commandRequestsTotal"], 1.0);
        let error = cells
            .iter()
            .find(|v| v["commandErrorsTotal"] == 1.0)
            .expect("an error cell");
        assert_eq!(error["commandRequestsTotal"], 1.0, "a failure counts a request AND an error");
        assert_eq!(error["commandLatencyMs"], 6.0);

        // The definitions carry the low-cardinality dimensions the family pattern prescribes.
        let defined_dims: Vec<std::collections::BTreeMap<String, String>> = target
            .defined
            .lock()
            .unwrap()
            .iter()
            .filter(|metric| metric.get_name() == COMMAND_METRIC)
            .map(|metric| metric.get_dimensions().clone())
            .collect();
        assert!(!defined_dims.is_empty(), "each cell defines before it emits");
        for dims in &defined_dims {
            assert_eq!(dims.get("instance").map(String::as_str), Some("camera-a"));
            assert_eq!(dims.get("verb").map(String::as_str), Some("sb/status"));
            assert!(dims.contains_key("result"), "the result dimension is present");
        }
        assert!(target.is_metric_defined(COMMAND_METRIC));
    }

    /// A metric target that refuses every emission must never take a drain down with it — the drain is
    /// best-effort, on a timer, and a briefly-unavailable backend must not stall the component.
    #[tokio::test]
    async fn a_drain_survives_a_metric_target_that_refuses_every_emission() {
        let target = Arc::new(RecordingMetrics {
            refuse: true,
            ..RecordingMetrics::default()
        });
        let metrics =
            CaptureMetrics::new(Arc::clone(&target) as Arc<dyn edgecommons::metrics::MetricService>);

        metrics.count("succeeded").await;
        metrics.record_command("camera-a", "sb/status", false, 3);

        // Neither drain may panic or propagate, and both must still attempt the (failing) emission.
        metrics.drain_captures().await;
        metrics.drain_commands().await;

        let attempted: Vec<String> = target
            .emitted
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _, _)| name.clone())
            .collect();
        assert!(attempted.iter().any(|n| n == CAPTURE_METRIC), "the capture drain still tried to emit");
        assert!(attempted.iter().any(|n| n == COMMAND_METRIC), "the command drain still tried to emit");
    }
}
