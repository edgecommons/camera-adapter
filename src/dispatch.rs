//! The fleet-wide capture queue.
//!
//! # Why this exists
//!
//! A capture used to pass through three layers, and only the last was shared:
//!
//! | Layer | Scope | Bound |
//! |---|---|---|
//! | `SupervisorDispatcher` | per camera | 4 |
//! | `CameraActor.captures` | per camera | 4 |
//! | `AdmissionController` | fleet | 32 permits |
//!
//! The admission controller is a **gate, not a scheduler**: it grants permits as resources free. It
//! never owns pending work, cannot see it, cannot order it, cannot move it. So the fleet's backlog
//! did not exist as an object -- it was scattered across `256 x 2` per-camera queues, worst case
//! 2,048 descriptors with no single number capping them, and priority was per-camera only. A
//! low-priority capture on one camera could be admitted while a `Direct` capture on another waited,
//! because nothing could see both.
//!
//! [`CaptureScheduler`] is that missing object: one queue for the whole fleet, ordered by priority
//! and age, bounded globally *and* per camera, admitting into cameras as capacity frees.
//!
//! # What it fixes for free
//!
//! An oversized group needed no wave scheduler in the end. A wave is just "pull N where N = the
//! capacity available", and that is what a central queue does by construction: members beyond
//! capacity simply wait their turn, and each one's clocks start when a camera actually takes it
//! (see `JobEngine::rebase_onto_admission`). "More work than I can do at once" degrades into
//! *taking longer* rather than into *most of your members failed*.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::actor::CameraActorHandle;
use crate::admission::{CaptureAdmissionQueue, CapturePriority, QueuedCapture};
use crate::config::LimitsConfig;
use crate::jobs::{CaptureDescriptor, CaptureDispatcher, DispatchReservation};
use crate::{CameraError, ErrorCode, Result};

/// How long a queued capture ages before gaining one priority point.
const AGING_INTERVAL: Duration = Duration::from_secs(1);

/// One capture waiting for a camera, holding the fleet slot it was admitted against.
///
/// The slot is carried *in the payload* on purpose. It is released exactly when the descriptor
/// leaves the queue -- popped for a camera, evicted on cancellation, evicted on its wait deadline --
/// because a counter that has to be decremented by hand at each of those exits is a counter that
/// will eventually be decremented at only two of them. That mistake had a name in this codebase: a
/// leaked slot bricked a camera's queue permanently.
pub struct PendingCapture {
    descriptor: CaptureDescriptor,
    _slot: FleetSlot,
}

impl PendingCapture {
    /// Takes the descriptor, releasing the fleet slot it held.
    #[must_use]
    pub fn into_descriptor(self) -> CaptureDescriptor {
        let Self { descriptor, _slot } = self;
        drop(_slot);
        descriptor
    }
}

/// The fleet-wide pending bound, plus the per-camera bound, as reservable slots.
///
/// Two-phase on purpose: capacity is reserved *before* the durable row is written, so a full queue
/// rejects a capture outright rather than accepting one durably and then failing it. That ordering
/// is the difference between an operator seeing `QUEUE_FULL` and an operator seeing a capture that
/// was accepted and immediately died.
struct FleetSlots {
    used: AtomicUsize,
    maximum: usize,
    per_camera: Mutex<HashMap<String, usize>>,
    maximum_per_camera: usize,
}

impl FleetSlots {
    fn reserve(self: &Arc<Self>, camera_id: &str) -> Result<FleetSlot> {
        {
            let mut per_camera = self
                .per_camera
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let held = per_camera.entry(camera_id.to_owned()).or_default();
            if *held >= self.maximum_per_camera {
                return Err(CameraError::rejected(
                    ErrorCode::QueueFull,
                    "camera capture queue is full",
                ));
            }
            if self
                .used
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    (current < self.maximum).then_some(current + 1)
                })
                .is_err()
            {
                return Err(CameraError::rejected(
                    ErrorCode::QueueFull,
                    "the component's capture queue is full",
                ));
            }
            *held += 1;
        }
        Ok(FleetSlot {
            slots: Arc::clone(self),
            camera_id: camera_id.to_owned(),
            released: false,
        })
    }
}

/// RAII release of one fleet slot.
struct FleetSlot {
    slots: Arc<FleetSlots>,
    camera_id: String,
    released: bool,
}

impl Drop for FleetSlot {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        self.slots.used.fetch_sub(1, Ordering::AcqRel);
        let mut per_camera = self
            .slots
            .per_camera
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(held) = per_camera.get_mut(&self.camera_id) {
            *held = held.saturating_sub(1);
            if *held == 0 {
                per_camera.remove(&self.camera_id);
            }
        }
    }
}

struct SchedulerInner {
    queue: CaptureAdmissionQueue<PendingCapture>,
    slots: Arc<FleetSlots>,
    /// The cameras that currently have a live actor. Work for a camera that is not here stays in the
    /// queue rather than being handed nowhere.
    online: RwLock<HashMap<String, CameraActorHandle>>,
    /// Raised when admissibility changes: a camera comes online, an actor frees a mailbox slot, or
    /// -- the one that actually matters -- a capture returns its execution permit.
    capacity_changed: Arc<Notify>,
    /// The component's execution bounds. The queue meters work against THESE, not against actor
    /// mailboxes.
    ///
    /// This is the correction that made sequencing real. A first cut gated on actor slots, which are
    /// plentiful (four per camera) and are not the scarce thing: every member of an oversized group
    /// was therefore handed to its camera at once, and the ones that could not get an execution
    /// permit sat inside `execute` racing a clock that had already started. The queue metered nothing
    /// and the group still partially failed.
    /// How long a capture may wait for a camera when its profile sets no queue expiry.
    default_wait: Duration,
    /// Execution slots, HELD from dispatch until the capture is terminal.
    ///
    /// Merely *checking* that a permit was free before dispatching was not enough, and the group test
    /// caught it: the permit is not actually taken until `execute` runs, so two captures could be
    /// dispatched against one free permit and the loser would queue a second time -- inside `execute`,
    /// invisibly, on a clock that had already been started for it. "Pull N where N is the available
    /// capacity" has to mean TAKE N.
    execution: Arc<Semaphore>,
    /// The slot each in-flight capture is holding, released when it reaches a terminal state.
    in_flight: Mutex<HashMap<String, OwnedSemaphorePermit>>,
}

/// One fleet-wide capture queue, shared by every producer and every camera.
#[derive(Clone)]
pub struct CaptureScheduler {
    inner: Arc<SchedulerInner>,
}

impl CaptureScheduler {
    /// Builds the scheduler from the component's limits and its execution bounds.
    pub fn new(limits: &LimitsConfig) -> Result<Self> {
        let capacity_changed = Arc::new(Notify::new());
        Ok(Self {
            inner: Arc::new(SchedulerInner {
                queue: CaptureAdmissionQueue::from_limits(
                    limits,
                    limits.max_pending_captures,
                    AGING_INTERVAL,
                )?,
                slots: Arc::new(FleetSlots {
                    used: AtomicUsize::new(0),
                    maximum: limits.max_pending_captures,
                    per_camera: Mutex::new(HashMap::new()),
                    maximum_per_camera: limits.max_queued_captures_per_camera,
                }),
                online: RwLock::new(HashMap::new()),
                capacity_changed,
                default_wait: Duration::from_millis(limits.max_queue_wait_ms),
                execution: Arc::new(Semaphore::new(limits.max_concurrent_captures)),
                in_flight: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// The signal an actor raises when it frees a capture slot.
    #[must_use]
    pub fn capacity_signal(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.capacity_changed)
    }

    /// Publishes a camera's live actor, making its queued work admissible.
    pub fn camera_online(&self, instance: &str, handle: CameraActorHandle) {
        if let Ok(mut online) = self.inner.online.write() {
            online.insert(instance.to_owned(), handle);
        }
        self.inner.capacity_changed.notify_waiters();
    }

    /// Retires a camera's actor. Its queued work stays queued -- that is the point of the queue.
    pub fn camera_offline(&self, instance: &str) {
        if let Ok(mut online) = self.inner.online.write() {
            online.remove(instance);
        }
    }

    /// Whether a capture can be handed to this camera right now.
    ///
    /// Three things must be true, and the last is the one that makes a queue a scheduler:
    ///
    /// 1. the camera has a live actor -- otherwise the capture has nowhere to go;
    /// 2. that actor has a free mailbox slot;
    /// 3. **the component has an execution permit free.**
    ///
    /// Without (3) the queue is only a waiting room with a door: every member of an oversized group
    /// is handed to its camera at once and then queues AGAIN, invisibly, inside `execute` -- on a
    /// clock that has already been started for it. Metering here, where the capture has not yet been
    /// given a clock, is what turns "more work than I can do at once" into "this takes longer".
    fn admissible(&self, instance: &str) -> bool {
        let Ok(online) = self.inner.online.read() else {
            return false;
        };
        online
            .get(instance)
            .is_some_and(|handle| handle.can_accept_capture())
    }

    /// The live actor for a camera, if it is still online.
    fn actor(&self, instance: &str) -> Option<CameraActorHandle> {
        self.inner.online.read().ok()?.get(instance).cloned()
    }

    /// Takes an execution slot, then the best capture a camera can actually be given.
    ///
    /// The slot is acquired FIRST and held by the returned capture, so the component never pulls more
    /// work out of the queue than it can actually run. Returns `None` only on shutdown.
    pub async fn next_admissible(
        &self,
        cancellation: &CancellationToken,
    ) -> Option<(QueuedCapture<PendingCapture>, OwnedSemaphorePermit)> {
        let slot = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return None,
            permit = Arc::clone(&self.inner.execution).acquire_owned() => permit.ok()?,
        };
        let scheduler = self.clone();
        let queued = self
            .inner
            .queue
            .next_admissible(cancellation, &self.inner.capacity_changed, move |camera| {
                scheduler.admissible(camera)
            })
            .await?;
        Some((queued, slot))
    }

    /// Records the execution slot a dispatched capture is holding.
    pub fn hold_execution_slot(&self, capture_id: &str, slot: OwnedSemaphorePermit) {
        self.inner
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(capture_id.to_owned(), slot);
    }

    /// Releases the execution slot a capture was holding, whatever ended it.
    ///
    /// Called from the terminal hook, which every capture passes through exactly once -- success,
    /// failure, cancellation, deadline, isolated panic. A slot released on only the happy path is a
    /// component that stops scheduling after its first failure.
    pub fn capture_finished(&self, capture_id: &str) {
        let released = self
            .inner
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(capture_id);
        if released.is_some() {
            self.inner.capacity_changed.notify_waiters();
        }
    }

    /// Hands one capture to its camera's actor.
    ///
    /// The camera may have gone offline between the pop and here -- a reconnect, a reload, a
    /// shutdown. The descriptor is returned to the caller rather than dropped, because dropping it
    /// would destroy work the component has already durably promised.
    pub fn dispatch(
        &self,
        instance: &str,
        descriptor: CaptureDescriptor,
    ) -> std::result::Result<(), CaptureDescriptor> {
        let Some(actor) = self.actor(instance) else {
            return Err(descriptor);
        };
        let Ok(reservation) = actor.reserve(instance) else {
            return Err(descriptor);
        };
        match reservation.commit(descriptor) {
            Ok(_) => Ok(()),
            // `commit` consumed the descriptor on failure. It only fails for a capture that is
            // already dead -- expired or cancelled -- which the actor is right to refuse and which
            // its own terminal-deadline task has already retired.
            Err(_) => Ok(()),
        }
    }

    /// Descriptors held across the whole fleet, reserved or queued.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.inner.slots.used.load(Ordering::Acquire)
    }

    /// Descriptors held for one camera.
    #[must_use]
    pub fn pending_for(&self, instance: &str) -> usize {
        self.inner
            .slots
            .per_camera
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(instance)
            .copied()
            .unwrap_or(0)
    }

    /// The fleet-wide pending ceiling.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.slots.maximum
    }

    /// The per-camera pending ceiling.
    #[must_use]
    pub fn capacity_per_camera(&self) -> usize {
        self.inner.slots.maximum_per_camera
    }
}

impl CaptureDispatcher for CaptureScheduler {
    fn reserve(&self, camera_id: &str) -> Result<Box<dyn DispatchReservation>> {
        let slot = self.inner.slots.reserve(camera_id)?;
        Ok(Box::new(SchedulerReservation {
            inner: Arc::clone(&self.inner),
            slot: Some(slot),
        }))
    }
}

struct SchedulerReservation {
    inner: Arc<SchedulerInner>,
    slot: Option<FleetSlot>,
}

impl DispatchReservation for SchedulerReservation {
    fn commit(mut self: Box<Self>, descriptor: CaptureDescriptor) -> Result<usize> {
        let slot = self.slot.take().ok_or_else(|| {
            CameraError::Catalog("capture queue reservation was already consumed".to_owned())
        })?;
        let instance = descriptor.instance().to_owned();

        // How long this capture may WAIT for a camera -- which is not how long it may take to run.
        // A capture that waits does not consume its execution budget; its clocks are rebased when a
        // camera takes it. If nothing were bounding the wait, a starved capture would sit here
        // forever, so an explicit `queueExpiryMs` wins and the component's default backstops it.
        let wait_deadline = descriptor
            .queue_expiry()
            .unwrap_or_else(|| Instant::now() + self.inner.default_wait);

        self.inner.queue.try_enqueue(
            instance,
            descriptor.priority(),
            wait_deadline,
            descriptor.cancellation(),
            PendingCapture {
                descriptor,
                _slot: slot,
            },
        )?;
        Ok(self.inner.slots.used.load(Ordering::Acquire))
    }
}

/// The priority a capture was queued with. Re-exported for the runtime's status surface.
pub use crate::admission::CapturePriority as SchedulerPriority;

#[allow(dead_code)]
const fn _priority_is_used(_: CapturePriority) {}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fleet queue with no room in it is a component that accepts nothing, silently.
    ///
    /// `CaptureScheduler` is the *only* way in for every capture the component will ever run, so a
    /// zero bound here is not a smaller queue -- it is a component that starts, reports itself
    /// healthy, publishes heartbeats, and rejects every capture an operator submits with
    /// `QUEUE_FULL`. The queue refuses to be built at all instead, which turns a silent misconfiguration
    /// into a startup failure that names the reason.
    #[test]
    fn a_fleet_queue_with_no_room_in_it_refuses_to_be_built() {
        for (limits, bound) in [
            (
                LimitsConfig {
                    max_pending_captures: 0,
                    ..LimitsConfig::default()
                },
                "the fleet-wide bound",
            ),
            (
                LimitsConfig {
                    max_queued_captures_per_camera: 0,
                    ..LimitsConfig::default()
                },
                "the per-camera bound",
            ),
        ] {
            let error = CaptureScheduler::new(&limits)
                .err()
                .unwrap_or_else(|| panic!("{bound} of zero must not produce a usable queue"));
            assert_eq!(
                error.code(),
                ErrorCode::BadArgs,
                "{bound} of zero is a configuration fault, not a runtime rejection"
            );
        }

        // The configured default must build, or the two refusals above prove nothing at all.
        let limits = LimitsConfig::default();
        let scheduler = CaptureScheduler::new(&limits).expect("the shipped limits must be usable");
        assert_eq!(scheduler.capacity(), limits.max_pending_captures);
        assert_eq!(
            scheduler.capacity_per_camera(),
            limits.max_queued_captures_per_camera
        );
        assert_eq!(scheduler.pending(), 0);
        assert_eq!(scheduler.pending_for("cam-a"), 0);
    }

    /// If the scheduler cannot see which cameras are live, no camera is admissible.
    ///
    /// `online` is a `std::sync::RwLock`, so a thread that panics while holding it poisons it for the
    /// rest of the process. The two readers fail *closed* on purpose. A capture that is not admitted
    /// stays in the queue, holding its slot, and is offered again -- which costs latency and nothing
    /// else. The alternatives are both worse: unwrapping would panic the one task that drains the
    /// fleet queue (every camera stops, permanently), and assuming a camera is present would hand a
    /// descriptor to an actor the scheduler can no longer prove exists, stranding the capture off the
    /// queue and owned by nobody.
    #[test]
    fn a_poisoned_camera_map_makes_every_camera_inadmissible_rather_than_dispatching_blind() {
        let scheduler =
            CaptureScheduler::new(&LimitsConfig::default()).expect("the shipped limits are usable");

        let inner = Arc::clone(&scheduler.inner);
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poisoner = std::thread::spawn(move || {
            let _held = inner.online.write().expect("the map is not yet poisoned");
            panic!("a thread died holding the online-camera map");
        })
        .join();
        std::panic::set_hook(previous);

        assert!(
            poisoner.is_err(),
            "the poisoning thread must actually panic"
        );
        assert!(
            scheduler.inner.online.read().is_err(),
            "the online-camera map must really be poisoned, or this test proves nothing"
        );

        assert!(
            !scheduler.admissible("cam-a"),
            "a camera the scheduler cannot see must not be handed work"
        );
        assert!(
            scheduler.actor("cam-a").is_none(),
            "and it must not produce an actor handle it cannot prove is current"
        );
    }
}
