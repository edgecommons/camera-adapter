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

    /// Readiness does not depend on every camera being online or on outbox count alone.
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
    /// Last terminal application-message publication latency.
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

    /// Records confirmed terminal-message publication latency.
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

#[cfg(test)]
mod tests {
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
    fn readiness_requires_every_startup_gate_but_not_camera_connectivity_or_outbox_count() {
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
}
