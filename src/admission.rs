//! Bounded capture admission, resource reservations, and safety-priority control lanes.
//!
//! Capture descriptors remain byte-free while queued. Once selected by the aging priority queue,
//! callers acquire global, resource-group, memory, and disk capacity in the binding order. RAII
//! leases release those resources in reverse order on every error, timeout, cancellation, or panic.
//! Encoder and writer permits are separate stage bounds and are never implicitly bundled together.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::{LimitsConfig, OutputConfig};
use crate::{CameraError, ErrorCode, Result};

/// Capture admission priority from highest to lowest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CapturePriority {
    /// Deferred direct capture and deferred group members.
    Direct,
    /// Submitted capture and submitted group members.
    Submitted,
    /// Schedule-originated capture.
    Scheduled,
}

impl CapturePriority {
    const fn base_score(self) -> u128 {
        match self {
            Self::Direct => 2,
            Self::Submitted => 1,
            Self::Scheduled => 0,
        }
    }
}

/// Opaque identifier for one queued descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueueTicket(u64);

/// One descriptor selected from the capture queue.
#[derive(Debug)]
pub struct QueuedCapture<T> {
    /// Queue ticket.
    pub ticket: QueueTicket,
    /// Target camera instance.
    pub camera_id: String,
    /// Original priority class.
    pub priority: CapturePriority,
    /// Time the descriptor entered admission.
    pub enqueued_at: Instant,
    /// Absolute job/admission deadline retained across dequeue races.
    pub deadline: Instant,
    /// Job cancellation retained so later admission stages observe a dequeue race.
    pub cancellation: CancellationToken,
    /// Caller-owned descriptor payload.
    pub payload: T,
}

struct QueueEntry<T> {
    ticket: QueueTicket,
    camera_id: String,
    priority: CapturePriority,
    enqueued_at: Instant,
    deadline: Instant,
    cancellation: CancellationToken,
    expiry_task_cancel: CancellationToken,
    payload: T,
}

struct QueueState<T> {
    entries: Vec<QueueEntry<T>>,
    per_camera: HashMap<String, usize>,
}

struct QueueInner<T> {
    state: Mutex<QueueState<T>>,
    notify: Notify,
    next_ticket: AtomicU64,
    max_global_pending: usize,
    max_per_camera: usize,
    aging_interval: Duration,
}

impl<T> QueueInner<T> {
    fn remove_ticket(&self, ticket: QueueTicket) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(index) = state
            .entries
            .iter()
            .position(|entry| entry.ticket == ticket)
        else {
            return false;
        };
        let entry = state.entries.swap_remove(index);
        decrement_camera_count(&mut state.per_camera, &entry.camera_id);
        drop(state);
        entry.expiry_task_cancel.cancel();
        self.notify.notify_waiters();
        true
    }

    fn pop_best(&self, now: Instant) -> Option<QueuedCapture<T>> {
        self.pop_best_admissible(now, |_| true)
    }

    /// Pops the best-scoring entry whose camera the consumer can actually serve right now.
    ///
    /// A fleet-wide consumer cannot simply take the globally best capture: that capture may belong
    /// to a camera that is offline, or whose actor has no free slot, and popping it would strand it
    /// -- off the queue, unbounded, owned by nobody. The predicate keeps the ORDERING global while
    /// making the CHOICE only among cameras that can take work, which is the whole point of a
    /// central queue: a Direct capture on a connected camera must not wait behind a Scheduled one on
    /// a camera that is down.
    ///
    /// Expired and cancelled entries are still swept regardless of admissibility -- a dead capture
    /// on an unreachable camera must not linger and hold its slot.
    fn pop_best_admissible(
        &self,
        now: Instant,
        admissible: impl Fn(&str) -> bool,
    ) -> Option<QueuedCapture<T>> {
        let mut discarded = Vec::new();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut index = 0;
        while index < state.entries.len() {
            if state.entries[index].cancellation.is_cancelled()
                || state.entries[index].deadline <= now
            {
                let entry = state.entries.swap_remove(index);
                decrement_camera_count(&mut state.per_camera, &entry.camera_id);
                discarded.push(entry);
            } else {
                index += 1;
            }
        }

        let best_index = state
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| admissible(&entry.camera_id))
            .max_by(|(_, left), (_, right)| {
                effective_score(left, now, self.aging_interval)
                    .cmp(&effective_score(right, now, self.aging_interval))
                    .then_with(|| right.ticket.0.cmp(&left.ticket.0))
            })
            .map(|(index, _)| index);
        let selected = best_index.map(|index| {
            let entry = state.entries.swap_remove(index);
            decrement_camera_count(&mut state.per_camera, &entry.camera_id);
            entry
        });
        drop(state);

        for entry in discarded {
            entry.expiry_task_cancel.cancel();
        }
        selected.map(|entry| {
            entry.expiry_task_cancel.cancel();
            QueuedCapture {
                ticket: entry.ticket,
                camera_id: entry.camera_id,
                priority: entry.priority,
                enqueued_at: entry.enqueued_at,
                deadline: entry.deadline,
                cancellation: entry.cancellation,
                payload: entry.payload,
            }
        })
    }
}

/// A hard-bounded, per-camera-bounded capture descriptor queue with priority aging.
///
/// Direct descriptors outrank submitted descriptors, which outrank schedules at equal age. Every
/// complete aging interval adds one score point, so an older scheduled descriptor eventually
/// outranks continuously arriving direct work. Cancellation and deadline watcher tasks remove
/// descriptors even when no consumer is polling the queue.
#[derive(Clone)]
pub struct CaptureAdmissionQueue<T> {
    inner: Arc<QueueInner<T>>,
}

impl<T: Send + 'static> CaptureAdmissionQueue<T> {
    /// Creates a queue with independent global and per-camera hard bounds.
    pub fn new(
        max_global_pending: usize,
        max_per_camera: usize,
        aging_interval: Duration,
    ) -> Result<Self> {
        if max_global_pending == 0 || max_per_camera == 0 {
            return Err(CameraError::rejected(
                ErrorCode::BadArgs,
                "capture queue bounds must be non-zero",
            ));
        }
        if aging_interval.is_zero() {
            return Err(CameraError::rejected(
                ErrorCode::BadArgs,
                "capture queue aging interval must be non-zero",
            ));
        }
        Ok(Self {
            inner: Arc::new(QueueInner {
                state: Mutex::new(QueueState {
                    entries: Vec::new(),
                    per_camera: HashMap::new(),
                }),
                notify: Notify::new(),
                next_ticket: AtomicU64::new(1),
                max_global_pending,
                max_per_camera,
                aging_interval,
            }),
        })
    }

    /// Creates a queue using the configured per-camera descriptor bound.
    pub fn from_limits(
        limits: &LimitsConfig,
        max_global_pending: usize,
        aging_interval: Duration,
    ) -> Result<Self> {
        Self::new(
            max_global_pending,
            limits.max_queued_captures_per_camera,
            aging_interval,
        )
    }

    /// Enqueues without waiting. Capacity failure is reported as `QUEUE_FULL`.
    pub fn try_enqueue(
        &self,
        camera_id: impl Into<String>,
        priority: CapturePriority,
        deadline: Instant,
        cancellation: CancellationToken,
        payload: T,
    ) -> Result<QueueTicket> {
        let camera_id = camera_id.into();
        if camera_id.is_empty() {
            return Err(CameraError::rejected(
                ErrorCode::BadArgs,
                "camera id must not be empty",
            ));
        }
        if cancellation.is_cancelled() {
            return Err(cancelled_error("capture queue"));
        }
        if deadline <= Instant::now() {
            return Err(timeout_error("capture queue"));
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            CameraError::Catalog("capture admission queue requires a Tokio runtime".to_owned())
        })?;
        let ticket_value = self
            .inner
            .next_ticket
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| {
                CameraError::rejected(
                    ErrorCode::ResourceLimit,
                    "capture queue ticket space exhausted",
                )
            })?;
        let ticket = QueueTicket(ticket_value);
        let expiry_task_cancel = CancellationToken::new();
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.entries.len() >= self.inner.max_global_pending {
                return Err(queue_full("global capture descriptor queue is full"));
            }
            let camera_pending = state.per_camera.get(&camera_id).copied().unwrap_or(0);
            if camera_pending >= self.inner.max_per_camera {
                return Err(queue_full("camera capture descriptor queue is full"));
            }
            *state.per_camera.entry(camera_id.clone()).or_default() += 1;
            state.entries.push(QueueEntry {
                ticket,
                camera_id,
                priority,
                enqueued_at: Instant::now(),
                deadline,
                cancellation: cancellation.clone(),
                expiry_task_cancel: expiry_task_cancel.clone(),
                payload,
            });
        }

        let weak = Arc::downgrade(&self.inner);
        runtime.spawn(async move {
            tokio::select! {
                biased;
                _ = expiry_task_cancel.cancelled() => return,
                _ = cancellation.cancelled() => {},
                _ = tokio::time::sleep_until(deadline) => {},
            }
            if let Some(inner) = Weak::upgrade(&weak) {
                inner.remove_ticket(ticket);
            }
        });
        self.inner.notify.notify_one();
        Ok(ticket)
    }

    /// Waits for the best descriptor whose camera can be served right now.
    ///
    /// The fleet scheduler's consumer. It differs from [`Self::next`] in one way that matters: the
    /// globally best capture may target a camera that is offline or whose actor is full, and popping
    /// that would strand it. So the ORDER stays global while the CHOICE is confined to cameras that
    /// can take work -- and when nothing is admissible, it waits to be told the world changed rather
    /// than spinning.
    ///
    /// `changed` is raised by whoever alters admissibility: a camera coming online, an actor freeing
    /// a slot. Without it this would have to poll, which is the 25,600-wakeups-per-second habit the
    /// central queue exists to end.
    pub async fn next_admissible(
        &self,
        cancellation: &CancellationToken,
        changed: &Notify,
        admissible: impl Fn(&str) -> bool,
    ) -> Option<QueuedCapture<T>> {
        loop {
            // Register for BOTH wakeups before looking -- and `enable()` is what actually registers.
            //
            // `notified()` only BUILDS the future; the waiter is not registered until it is first
            // polled, and `notify_waiters()` wakes only waiters that are already registered. So
            // merely constructing the futures early is not enough: a notification that lands between
            // the look below and the `select!` that first polls them is dropped on the floor, and
            // this task then sleeps forever on work it is already holding. That is a capture stuck
            // QUEUED with nothing to drive it -- the exact shape of B5, reintroduced by its fix.
            //
            // `enable()` registers the waiter now, before the look, so that window does not exist.
            let arrived = self.inner.notify.notified();
            let capacity = changed.notified();
            tokio::pin!(arrived, capacity);
            arrived.as_mut().enable();
            capacity.as_mut().enable();
            if let Some(entry) = self.inner.pop_best_admissible(Instant::now(), &admissible) {
                return Some(entry);
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return None,
                () = arrived.as_mut() => {},
                () = capacity.as_mut() => {},
            }
        }
    }

    /// Waits for the best descriptor, or returns `None` when the consumer is cancelled.
    pub async fn next(&self, cancellation: &CancellationToken) -> Option<QueuedCapture<T>> {
        loop {
            // Enabled before the look, for the same reason as `next_admissible` above: an unenabled
            // `Notified` is not a registered waiter, and a push that lands in between is lost.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(entry) = self.inner.pop_best(Instant::now()) {
                return Some(entry);
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return None,
                _ = notified => {},
            }
        }
    }

    /// Pops the best currently queued descriptor without waiting.
    ///
    /// Actors use this during bounded shutdown to terminalize durable queued work instead of
    /// silently dropping descriptors and leaving avoidable recovery records.
    pub fn try_next(&self) -> Option<QueuedCapture<T>> {
        self.inner.pop_best(Instant::now())
    }

    /// Removes one descriptor by ticket. Returns whether it was still queued.
    pub fn remove(&self, ticket: QueueTicket) -> bool {
        self.inner.remove_ticket(ticket)
    }

    /// Current global descriptor count.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .len()
    }

    /// Current descriptor count for one camera.
    #[must_use]
    pub fn pending_for(&self, camera_id: &str) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .per_camera
            .get(camera_id)
            .copied()
            .unwrap_or(0)
    }
}

fn effective_score<T>(entry: &QueueEntry<T>, now: Instant, aging_interval: Duration) -> u128 {
    let age = now.saturating_duration_since(entry.enqueued_at).as_nanos();
    entry.priority.base_score() + age / aging_interval.as_nanos()
}

fn decrement_camera_count(counts: &mut HashMap<String, usize>, camera_id: &str) {
    if let Some(count) = counts.get_mut(camera_id) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(camera_id);
        }
    }
}

/// Stop axes and mandatory earliest completion deadline for the safety lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafetyStop {
    /// Stop pan motion.
    pub pan: bool,
    /// Stop tilt motion.
    pub tilt: bool,
    /// Stop zoom motion.
    pub zoom: bool,
    /// Tightest stop deadline.
    pub deadline: Instant,
}

impl SafetyStop {
    fn merge(&mut self, other: Self) {
        self.pan |= other.pan;
        self.tilt |= other.tilt;
        self.zoom |= other.zoom;
        self.deadline = self.deadline.min(other.deadline);
    }
}

/// Next control-lane item. Safety always precedes ordinary work.
#[derive(Debug, PartialEq, Eq)]
pub enum ControlWork<T> {
    /// Coalesced safety stop.
    SafetyStop(SafetyStop),
    /// Ordinary bounded control operation.
    Ordinary(T),
}

struct ControlState<T> {
    safety_stop: Option<SafetyStop>,
    ordinary: VecDeque<T>,
}

/// Per-camera ordinary-control queue plus an independent, non-evictable safety-stop lane.
pub struct ControlLanes<T> {
    state: Mutex<ControlState<T>>,
    ordinary_capacity: usize,
    /// Raised the moment a safety stop enters the lane.
    ///
    /// The actor polls this lane between pieces of work, which is enough for everything except the
    /// one thing the lane exists for: a capture holds the camera session for as long as it runs, and
    /// an emergency stop cannot wait that long. This signal is what lets the actor find out about a
    /// stop while it is still busy, rather than after.
    safety: Notify,
}

impl<T> ControlLanes<T> {
    /// Creates lanes with the configured ordinary-control capacity.
    pub fn from_limits(limits: &LimitsConfig) -> Result<Self> {
        Self::new(limits.max_queued_controls_per_camera)
    }

    /// Creates lanes with a hard ordinary-control capacity.
    pub fn new(ordinary_capacity: usize) -> Result<Self> {
        if !(1..=1_024).contains(&ordinary_capacity) {
            return Err(CameraError::rejected(
                ErrorCode::BadArgs,
                "ordinary control capacity must be between 1 and 1024",
            ));
        }
        Ok(Self {
            safety: Notify::new(),
            state: Mutex::new(ControlState {
                safety_stop: None,
                ordinary: VecDeque::with_capacity(ordinary_capacity),
            }),
            ordinary_capacity,
        })
    }

    /// Adds an ordinary operation without waiting or disturbing a pending safety stop.
    pub fn try_push_ordinary(&self, operation: T) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.ordinary.len() >= self.ordinary_capacity {
            return Err(queue_full("ordinary control queue is full"));
        }
        state.ordinary.push_back(operation);
        Ok(())
    }

    /// Adds or coalesces a safety stop outside ordinary capacity.
    pub fn push_safety_stop(&self, stop: SafetyStop) -> Result<()> {
        if !stop.pan && !stop.tilt && !stop.zoom {
            return Err(CameraError::rejected(
                ErrorCode::BadArgs,
                "safety stop must select at least one axis",
            ));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut state.safety_stop {
            Some(existing) => existing.merge(stop),
            None => state.safety_stop = Some(stop),
        }
        drop(state);
        self.safety.notify_waiters();
        Ok(())
    }

    /// Resolves as soon as a safety stop is waiting in the lane.
    ///
    /// The actor races this against the capture it is running, because a stop that is only noticed
    /// when the capture ends is a stop that arrives up to `jobTerminalMs` late -- at a camera that
    /// is physically moving. Enabled before the check, because `notify_waiters` only reaches waiters
    /// that have already registered, and a stop that lands in that gap would be waited on forever.
    pub async fn safety_stop_arrived(&self) {
        loop {
            let notified = self.safety.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.has_safety_stop() {
                return;
            }
            notified.await;
        }
    }

    /// Pops safety work first, then ordinary work. Actors call this before polling capture work.
    pub fn pop_next(&self) -> Option<ControlWork<T>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .safety_stop
            .take()
            .map(ControlWork::SafetyStop)
            .or_else(|| state.ordinary.pop_front().map(ControlWork::Ordinary))
    }

    /// Number of ordinary queued operations; the safety lane is intentionally excluded.
    #[must_use]
    pub fn ordinary_len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ordinary
            .len()
    }

    /// Whether a safety stop is pending.
    #[must_use]
    pub fn has_safety_stop(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .safety_stop
            .is_some()
    }
}

/// Filesystem capacity snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskSpace {
    /// Bytes available to the running user.
    pub available_bytes: u64,
    /// Filesystem total bytes.
    pub total_bytes: u64,
}

/// Async filesystem-capacity probe used by disk reservation policy.
#[async_trait]
pub trait DiskSpaceProbe: Send + Sync {
    /// Reads available and total bytes for the filesystem containing `path`.
    async fn space(&self, path: &Path) -> Result<DiskSpace>;
}

/// Production disk probe. Blocking filesystem calls run only on Tokio's blocking pool.
///
/// The permit remains owned by the blocking closure, not the cancelled async caller. A wedged
/// filesystem can therefore consume at most four blocking tasks instead of allowing repeated
/// capture cancellations to create an unbounded tail of unabortable filesystem calls.
#[derive(Debug, Clone)]
pub struct FilesystemSpaceProbe {
    permits: Arc<Semaphore>,
}

impl Default for FilesystemSpaceProbe {
    fn default() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(4)),
        }
    }
}

#[async_trait]
impl DiskSpaceProbe for FilesystemSpaceProbe {
    async fn space(&self, path: &Path) -> Result<DiskSpace> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| CameraError::Storage("disk-space probe gate closed".to_owned()))?;
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            Ok(DiskSpace {
                available_bytes: fs4::available_space(&path)?,
                total_bytes: fs4::total_space(&path)?,
            })
        })
        .await
        .map_err(|error| CameraError::Storage(format!("disk-space probe task failed: {error}")))?
    }
}

struct ByteBudgetInner {
    capacity: u64,
    available: AtomicU64,
    notify: Notify,
}

#[derive(Clone)]
struct ByteBudget {
    inner: Arc<ByteBudgetInner>,
}

impl ByteBudget {
    fn new(capacity: u64) -> Result<Self> {
        if capacity == 0 {
            return Err(CameraError::rejected(
                ErrorCode::BadArgs,
                "byte budget must be non-zero",
            ));
        }
        Ok(Self {
            inner: Arc::new(ByteBudgetInner {
                capacity,
                available: AtomicU64::new(capacity),
                notify: Notify::new(),
            }),
        })
    }

    async fn reserve(
        &self,
        amount: u64,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ByteReservation> {
        if amount == 0 || amount > self.inner.capacity {
            return Err(resource_error(
                "requested memory reservation exceeds the configured byte budget",
            ));
        }
        loop {
            if cancellation.is_cancelled() {
                return Err(cancelled_error("memory reservation"));
            }
            if deadline <= Instant::now() {
                return Err(timeout_error("memory reservation"));
            }
            let notified = self.inner.notify.notified();
            let available = self.inner.available.load(Ordering::Acquire);
            if available >= amount {
                if self
                    .inner
                    .available
                    .compare_exchange_weak(
                        available,
                        available - amount,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return Ok(ByteReservation {
                        budget: Arc::clone(&self.inner),
                        reserved: amount,
                    });
                }
                continue;
            }
            wait_for_capacity(notified, deadline, cancellation, "memory reservation").await?;
        }
    }

    fn available(&self) -> u64 {
        self.inner.available.load(Ordering::Acquire)
    }
}

pub(crate) struct ByteReservation {
    budget: Arc<ByteBudgetInner>,
    reserved: u64,
}

impl ByteReservation {
    fn shrink(&mut self, new_amount: u64) -> Result<()> {
        if new_amount > self.reserved {
            return Err(resource_error(
                "memory reservation cannot grow after admission",
            ));
        }
        let released = self.reserved - new_amount;
        self.reserved = new_amount;
        release_atomic_budget(&self.budget.available, released);
        if released != 0 {
            self.budget.notify.notify_waiters();
        }
        Ok(())
    }

    fn release_all(&mut self) {
        let released = self.reserved;
        self.reserved = 0;
        release_atomic_budget(&self.budget.available, released);
        if released != 0 {
            self.budget.notify.notify_waiters();
        }
    }
}

impl Drop for ByteReservation {
    fn drop(&mut self) {
        release_atomic_budget(&self.budget.available, self.reserved);
        self.reserved = 0;
        self.budget.notify.notify_waiters();
    }
}

struct DiskBudgetInner {
    root: PathBuf,
    minimum_free_bytes: u64,
    minimum_free_percent: u8,
    outstanding: AtomicU64,
    probe: Arc<dyn DiskSpaceProbe>,
}

#[derive(Clone)]
struct DiskBudget {
    inner: Arc<DiskBudgetInner>,
}

impl DiskBudget {
    fn new(output: &OutputConfig, probe: Arc<dyn DiskSpaceProbe>) -> Result<Self> {
        if output.minimum_free_percent > 100 {
            return Err(CameraError::rejected(
                ErrorCode::BadArgs,
                "minimum free percent must not exceed 100",
            ));
        }
        Ok(Self {
            inner: Arc::new(DiskBudgetInner {
                root: PathBuf::from(&output.root_directory),
                minimum_free_bytes: output.minimum_free_bytes,
                minimum_free_percent: output.minimum_free_percent,
                outstanding: AtomicU64::new(0),
                probe,
            }),
        })
    }

    async fn reserve(
        &self,
        amount: u64,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DiskReservation> {
        if amount == 0 {
            return Err(resource_error("disk reservation must be non-zero"));
        }
        if deadline <= Instant::now() {
            return Err(timeout_error("disk reservation"));
        }
        let probe = self.inner.probe.space(&self.inner.root);
        tokio::pin!(probe);
        let space = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(cancelled_error("disk reservation")),
            _ = tokio::time::sleep_until(deadline) => return Err(timeout_error("disk reservation")),
            result = &mut probe => result?,
        };
        if space.available_bytes > space.total_bytes {
            return Err(CameraError::Storage(
                "filesystem reported available bytes greater than total bytes".to_owned(),
            ));
        }
        let percent_floor = percentage_floor(space.total_bytes, self.inner.minimum_free_percent)?;
        let floor = self.inner.minimum_free_bytes.max(percent_floor);
        loop {
            let outstanding = self.inner.outstanding.load(Ordering::Acquire);
            let projected = outstanding
                .checked_add(amount)
                .ok_or_else(|| resource_error("disk reservation arithmetic overflowed"))?;
            let required = projected
                .checked_add(floor)
                .ok_or_else(|| resource_error("disk floor arithmetic overflowed"))?;
            if required > space.available_bytes {
                return Err(CameraError::rejected(
                    ErrorCode::StoragePressure,
                    "disk reservation would violate the configured free-space floor",
                ));
            }
            if self
                .inner
                .outstanding
                .compare_exchange_weak(outstanding, projected, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(DiskReservation {
                    budget: Arc::clone(&self.inner),
                    reserved: amount,
                });
            }
        }
    }

    fn outstanding(&self) -> u64 {
        self.inner.outstanding.load(Ordering::Acquire)
    }
}

struct DiskReservation {
    budget: Arc<DiskBudgetInner>,
    reserved: u64,
}

impl DiskReservation {
    fn shrink(&mut self, new_amount: u64) -> Result<()> {
        if new_amount > self.reserved {
            return Err(resource_error(
                "disk reservation cannot grow after admission",
            ));
        }
        let released = self.reserved - new_amount;
        self.reserved = new_amount;
        release_atomic_outstanding(&self.budget.outstanding, released);
        Ok(())
    }
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        release_atomic_outstanding(&self.budget.outstanding, self.reserved);
        self.reserved = 0;
    }
}

/// Capacity request for one selected capture descriptor.
#[derive(Debug, Clone)]
pub struct CaptureResourceRequest {
    /// Optional configured NIC/USB resource-group name.
    pub resource_group: Option<String>,
    /// Immutable accepted maximum frame/output reservation.
    pub maximum_frame_bytes: u64,
    /// Absolute admission deadline.
    pub deadline: Instant,
}

/// Global admission/resource controller built from validated adapter configuration.
#[derive(Clone)]
pub struct AdmissionController {
    global_acquisitions: Arc<Semaphore>,
    resource_groups: Arc<BTreeMap<String, Arc<Semaphore>>>,
    memory: ByteBudget,
    disk: DiskBudget,
    encoders: Arc<Semaphore>,
    writers: Arc<Semaphore>,
}

/// A bounded point-in-time view of the internal admission controls.
///
/// These are the numbers that say whether the component is coping: how much acquisition,
/// conversion and persistence capacity is left, how much frame memory is unreserved, how many bytes
/// are outstanding against the output filesystem. It deliberately contains no camera identifiers,
/// payload bytes, or paths.
///
/// It used to be compiled only into a test build --
/// `cfg(all(test, target_os = "linux", standalone, onvif, capacity-harness))` -- so the one
/// consumer was the capacity harness and an operator could never see any of it. The observability
/// had been built for the test rather than for the person holding the pager. It is now a production
/// surface, and it is what `sb/queue-status` and the emitted metrics both read.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionSnapshot {
    /// Unused global acquisition permits.
    pub available_acquisitions: usize,
    /// Unused named resource-group acquisition permits.
    pub available_resource_group_acquisitions: BTreeMap<String, usize>,
    /// Unreserved source-frame bytes.
    pub available_memory_bytes: u64,
    /// Bytes logically reserved against the output filesystem.
    pub outstanding_disk_bytes: u64,
    /// Unused image-conversion permits.
    pub available_encoders: usize,
    /// Unused image-persistence permits.
    pub available_writers: usize,
}

impl AdmissionController {
    /// Builds capacity controls from `LimitsConfig` and output free-space policy.
    pub fn new(
        limits: &LimitsConfig,
        output: &OutputConfig,
        disk_probe: Arc<dyn DiskSpaceProbe>,
    ) -> Result<Self> {
        if limits.max_concurrent_captures == 0
            || limits.max_concurrent_encodes == 0
            || limits.max_concurrent_writes == 0
        {
            return Err(CameraError::rejected(
                ErrorCode::BadArgs,
                "admission semaphore limits must be non-zero",
            ));
        }
        let resource_groups = limits
            .resource_groups
            .iter()
            .map(|(name, group)| {
                if group.max_concurrent_captures == 0
                    || group.max_concurrent_captures > limits.max_concurrent_captures
                {
                    return Err(CameraError::rejected(
                        ErrorCode::BadArgs,
                        "resource-group capacity must be between 1 and the global capture limit",
                    ));
                }
                Ok((
                    name.clone(),
                    Arc::new(Semaphore::new(group.max_concurrent_captures)),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            global_acquisitions: Arc::new(Semaphore::new(limits.max_concurrent_captures)),
            resource_groups: Arc::new(resource_groups),
            memory: ByteBudget::new(limits.max_in_flight_bytes)?,
            disk: DiskBudget::new(output, disk_probe)?,
            encoders: Arc::new(Semaphore::new(limits.max_concurrent_encodes)),
            writers: Arc::new(Semaphore::new(limits.max_concurrent_writes)),
        })
    }

    /// Acquires global, optional resource-group, memory, and disk capacity in binding order.
    /// Partial acquisition is rolled back in reverse order on every error path.
    pub async fn acquire_capture(
        &self,
        request: CaptureResourceRequest,
        cancellation: &CancellationToken,
    ) -> Result<AcquisitionLease> {
        if request.maximum_frame_bytes == 0 {
            return Err(resource_error("maximum frame reservation must be non-zero"));
        }
        let global = acquire_semaphore(
            Arc::clone(&self.global_acquisitions),
            request.deadline,
            cancellation,
            "global acquisition permit",
        )
        .await?;
        let resource_group = match request.resource_group {
            Some(name) => {
                let semaphore =
                    self.resource_groups
                        .get(&name)
                        .ok_or_else(|| CameraError::Config {
                            path: "component.instances[].resourceGroup".to_owned(),
                            message: format!("unknown admission resource group '{name}'"),
                        })?;
                Some(
                    acquire_semaphore(
                        Arc::clone(semaphore),
                        request.deadline,
                        cancellation,
                        "resource-group acquisition permit",
                    )
                    .await?,
                )
            }
            None => None,
        };
        let memory = self
            .memory
            .reserve(request.maximum_frame_bytes, request.deadline, cancellation)
            .await?;
        let disk = self
            .disk
            .reserve(request.maximum_frame_bytes, request.deadline, cancellation)
            .await?;
        Ok(AcquisitionLease {
            disk,
            memory,
            resource_group,
            global,
        })
    }

    /// Acquires an encoder permit independently of acquisition and writer permits.
    pub async fn acquire_encoder(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<EncoderPermit> {
        Ok(EncoderPermit {
            _permit: acquire_semaphore(
                Arc::clone(&self.encoders),
                deadline,
                cancellation,
                "encoder permit",
            )
            .await?,
        })
    }

    /// Acquires a writer permit independently of acquisition and encoder permits.
    pub async fn acquire_writer(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<WriterPermit> {
        Ok(WriterPermit {
            _permit: acquire_semaphore(
                Arc::clone(&self.writers),
                deadline,
                cancellation,
                "writer permit",
            )
            .await?,
        })
    }

    /// Currently available global acquisition permits.
    #[must_use]
    pub fn available_acquisitions(&self) -> usize {
        self.global_acquisitions.available_permits()
    }

    /// Currently unreserved in-flight bytes.
    #[must_use]
    pub fn available_memory_bytes(&self) -> u64 {
        self.memory.available()
    }

    /// Current logical output-filesystem reservation bytes.
    #[must_use]
    pub fn outstanding_disk_bytes(&self) -> u64 {
        self.disk.outstanding()
    }

    /// Reserve memory from the SAME budget every capture is admitted against.
    ///
    /// `maxInFlightBytes` is meant to be the component's memory ceiling, and a capture reserves its
    /// whole declared `maximumFrameBytes` up front so that it is. The thumbnail renderer was the one
    /// allocation that escaped it: a JPEG frame's DECODED size is not its file size, so decoding one
    /// asks the allocator for memory the budget never saw and never counted. Bounded, because the
    /// decode-bomb guard refuses a JPEG whose header declares more than `maximumFrameBytes` -- but
    /// bounded is not the same as reserved, and a ceiling that is quietly exceeded is not a ceiling.
    ///
    /// A caller that cannot get this reservation must go without its preview. It must NOT fail the
    /// capture: the image is already in hand, and a thumbnail never outranks the result.
    pub(crate) async fn reserve_memory(
        &self,
        amount: u64,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ByteReservation> {
        self.memory.reserve(amount, deadline, cancellation).await
    }

    /// Returns a compact, allocation-light snapshot of live admission capacity.
    #[must_use]
    pub fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            available_acquisitions: self.available_acquisitions(),
            available_resource_group_acquisitions: self
                .resource_groups
                .iter()
                .map(|(name, semaphore)| (name.clone(), semaphore.available_permits()))
                .collect(),
            available_memory_bytes: self.available_memory_bytes(),
            outstanding_disk_bytes: self.outstanding_disk_bytes(),
            available_encoders: self.encoders.available_permits(),
            available_writers: self.writers.available_permits(),
        }
    }
}

/// Capacity held while backend acquisition is active.
///
/// Field order is reverse acquisition order so implicit drop releases disk, memory, group, then
/// global capacity.
pub struct AcquisitionLease {
    disk: DiskReservation,
    memory: ByteReservation,
    resource_group: Option<OwnedSemaphorePermit>,
    global: OwnedSemaphorePermit,
}

impl AcquisitionLease {
    /// Records actual frame bytes, rejects growth, and releases group/global acquisition permits.
    pub fn finish_acquisition(mut self, actual_frame_bytes: u64) -> Result<ProcessingLease> {
        self.memory.shrink(actual_frame_bytes)?;
        let Self {
            disk,
            memory,
            resource_group,
            global,
        } = self;
        drop(resource_group);
        drop(global);
        // Dropped here rather than carried into the processing lease: the ACQUISITION permit is what
        // the scheduler meters against, and it has just been returned. Encoding and persistence have
        // their own bounds and are not the scarce thing.
        Ok(ProcessingLease { disk, memory })
    }

    /// Maximum bytes currently reserved before acquisition completes.
    #[must_use]
    pub const fn reserved_bytes(&self) -> u64 {
        self.memory.reserved
    }
}

/// Byte and disk capacity retained through encoding and persistence.
///
/// Disk is declared first so it releases before memory on drop, reversing admission order.
pub struct ProcessingLease {
    disk: DiskReservation,
    memory: ByteReservation,
}

impl ProcessingLease {
    /// Shrinks the memory reservation to known live bytes. Growth is rejected.
    pub fn shrink_memory(&mut self, bytes: u64) -> Result<()> {
        self.memory.shrink(bytes)
    }

    /// Releases all frame-memory reservation after encoding no longer needs it.
    pub fn release_memory(&mut self) {
        self.memory.release_all();
    }

    /// Shrinks the disk reservation to known encoded/output bytes. Growth is rejected.
    pub fn shrink_disk(&mut self, bytes: u64) -> Result<()> {
        self.disk.shrink(bytes)
    }

    /// Current memory reservation.
    #[must_use]
    pub const fn reserved_memory_bytes(&self) -> u64 {
        self.memory.reserved
    }

    /// Current disk reservation.
    #[must_use]
    pub const fn reserved_disk_bytes(&self) -> u64 {
        self.disk.reserved
    }
}

/// RAII encoder-stage permit.
pub struct EncoderPermit {
    _permit: OwnedSemaphorePermit,
}

/// RAII writer-stage permit.
pub struct WriterPermit {
    _permit: OwnedSemaphorePermit,
}

async fn acquire_semaphore(
    semaphore: Arc<Semaphore>,
    deadline: Instant,
    cancellation: &CancellationToken,
    label: &'static str,
) -> Result<OwnedSemaphorePermit> {
    if deadline <= Instant::now() {
        return Err(timeout_error(label));
    }
    let acquire = semaphore.acquire_owned();
    tokio::pin!(acquire);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(cancelled_error(label)),
        _ = tokio::time::sleep_until(deadline) => Err(timeout_error(label)),
        result = &mut acquire => result.map_err(|_| CameraError::Catalog(format!("{label} semaphore closed"))),
    }
}

async fn wait_for_capacity(
    notified: impl std::future::Future<Output = ()>,
    deadline: Instant,
    cancellation: &CancellationToken,
    label: &'static str,
) -> Result<()> {
    if deadline <= Instant::now() {
        return Err(timeout_error(label));
    }
    tokio::pin!(notified);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(cancelled_error(label)),
        _ = tokio::time::sleep_until(deadline) => Err(timeout_error(label)),
        _ = &mut notified => Ok(()),
    }
}

fn release_atomic_budget(value: &AtomicU64, amount: u64) {
    if amount != 0 {
        value.fetch_add(amount, Ordering::Release);
    }
}

fn release_atomic_outstanding(value: &AtomicU64, amount: u64) {
    if amount != 0 {
        value.fetch_sub(amount, Ordering::Release);
    }
}

fn percentage_floor(total: u64, percent: u8) -> Result<u64> {
    let numerator = u128::from(total)
        .checked_mul(u128::from(percent))
        .ok_or_else(|| resource_error("disk percentage floor overflowed"))?;
    let rounded = numerator
        .checked_add(99)
        .ok_or_else(|| resource_error("disk percentage rounding overflowed"))?
        / 100;
    u64::try_from(rounded).map_err(|_| resource_error("disk percentage floor exceeds u64"))
}

fn queue_full(message: &'static str) -> CameraError {
    CameraError::rejected(ErrorCode::QueueFull, message)
}

fn resource_error(message: &'static str) -> CameraError {
    CameraError::rejected(ErrorCode::ResourceLimit, message)
}

fn timeout_error(stage: &'static str) -> CameraError {
    CameraError::rejected(
        ErrorCode::CaptureTimeout,
        format!("deadline expired while waiting for {stage}"),
    )
}

fn cancelled_error(stage: &'static str) -> CameraError {
    CameraError::rejected(
        ErrorCode::CaptureCancelled,
        format!("cancelled while waiting for {stage}"),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicU64;

    use super::*;
    use crate::config::ResourceGroupConfig;

    struct FixedProbe {
        available: AtomicU64,
        total: AtomicU64,
    }

    impl FixedProbe {
        fn new(available: u64, total: u64) -> Self {
            Self {
                available: AtomicU64::new(available),
                total: AtomicU64::new(total),
            }
        }
    }

    #[async_trait]
    impl DiskSpaceProbe for FixedProbe {
        async fn space(&self, _path: &Path) -> Result<DiskSpace> {
            Ok(DiskSpace {
                available_bytes: self.available.load(Ordering::Acquire),
                total_bytes: self.total.load(Ordering::Acquire),
            })
        }
    }

    struct HangingProbe;

    #[async_trait]
    impl DiskSpaceProbe for HangingProbe {
        async fn space(&self, _path: &Path) -> Result<DiskSpace> {
            std::future::pending().await
        }
    }

    fn output(minimum_free_bytes: u64, minimum_free_percent: u8) -> OutputConfig {
        OutputConfig {
            root_directory: "C:/camera-output".to_owned(),
            camera_directory_template: "{cameraId}".to_owned(),
            file_name_template: "{captureId}.{extension}".to_owned(),
            write_metadata_sidecar: false,
            minimum_free_bytes,
            minimum_free_percent,
            directory_mode: "0750".to_owned(),
            file_mode: "0640".to_owned(),
        }
    }

    fn limits(captures: usize, encodes: usize, writes: usize, memory: u64) -> LimitsConfig {
        LimitsConfig {
            max_concurrent_captures: captures,
            max_concurrent_encodes: encodes,
            max_concurrent_writes: writes,
            max_in_flight_bytes: memory,
            max_frame_bytes_per_camera: memory,
            ..LimitsConfig::default()
        }
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    fn expect_error<T>(result: Result<T>) -> CameraError {
        match result {
            Ok(_) => panic!("operation unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    #[tokio::test]
    async fn a_capacity_change_racing_the_look_still_wakes_the_consumer() {
        let queue = CaptureAdmissionQueue::new(10, 10, Duration::from_secs(10)).unwrap();
        let cancellation = CancellationToken::new();
        queue
            .try_enqueue(
                "cam",
                CapturePriority::Scheduled,
                deadline(),
                CancellationToken::new(),
                "stranded",
            )
            .unwrap();

        let changed = Arc::new(Notify::new());
        let signal = Arc::clone(&changed);
        let looked = std::sync::atomic::AtomicBool::new(false);
        let admissible = move |_camera: &str| {
            if looked.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return true;
            }
            // The camera becomes able to take work the moment after we were asked whether it could.
            signal.notify_waiters();
            false
        };

        let entry = tokio::time::timeout(
            Duration::from_secs(5),
            queue.next_admissible(&cancellation, &changed, admissible),
        )
        .await
        .expect("a capacity change racing the look must not strand the queue")
        .expect("the queue holds one capture and must hand it over");
        assert_eq!(entry.camera_id, "cam");
        assert_eq!(entry.payload, "stranded");
    }

    #[tokio::test(start_paused = true)]
    async fn direct_then_submitted_then_scheduled_is_fifo_within_class() {
        let queue = CaptureAdmissionQueue::new(10, 10, Duration::from_secs(10)).unwrap();
        let cancellation = CancellationToken::new();
        queue
            .try_enqueue(
                "cam",
                CapturePriority::Scheduled,
                deadline(),
                CancellationToken::new(),
                "scheduled",
            )
            .unwrap();
        queue
            .try_enqueue(
                "cam",
                CapturePriority::Direct,
                deadline(),
                CancellationToken::new(),
                "direct-1",
            )
            .unwrap();
        queue
            .try_enqueue(
                "cam",
                CapturePriority::Submitted,
                deadline(),
                CancellationToken::new(),
                "submitted",
            )
            .unwrap();
        queue
            .try_enqueue(
                "cam",
                CapturePriority::Direct,
                deadline(),
                CancellationToken::new(),
                "direct-2",
            )
            .unwrap();

        let mut order = Vec::new();
        for _ in 0..4 {
            order.push(queue.next(&cancellation).await.unwrap().payload);
        }
        assert_eq!(order, ["direct-1", "direct-2", "submitted", "scheduled"]);
    }

    #[tokio::test(start_paused = true)]
    async fn aging_prevents_continuous_direct_arrivals_from_starving_schedule() {
        let queue = CaptureAdmissionQueue::new(20, 20, Duration::from_millis(10)).unwrap();
        queue
            .try_enqueue(
                "cam",
                CapturePriority::Scheduled,
                deadline(),
                CancellationToken::new(),
                "old-schedule".to_owned(),
            )
            .unwrap();
        tokio::time::advance(Duration::from_millis(31)).await;
        for index in 0..10 {
            queue
                .try_enqueue(
                    "cam",
                    CapturePriority::Direct,
                    deadline(),
                    CancellationToken::new(),
                    format!("direct-{index}"),
                )
                .unwrap();
        }
        let selected = queue.next(&CancellationToken::new()).await.unwrap();
        assert_eq!(selected.payload, "old-schedule");
    }

    #[tokio::test(start_paused = true)]
    async fn queue_bounds_and_cancel_deadline_watchers_remove_descriptors() {
        let queue = CaptureAdmissionQueue::new(2, 1, Duration::from_secs(1)).unwrap();
        let first_cancel = CancellationToken::new();
        let first = queue
            .try_enqueue(
                "cam-a",
                CapturePriority::Direct,
                deadline(),
                first_cancel.clone(),
                1,
            )
            .unwrap();
        let error = queue
            .try_enqueue(
                "cam-a",
                CapturePriority::Direct,
                deadline(),
                CancellationToken::new(),
                2,
            )
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::QueueFull);
        queue
            .try_enqueue(
                "cam-b",
                CapturePriority::Direct,
                deadline(),
                CancellationToken::new(),
                3,
            )
            .unwrap();
        assert_eq!(queue.pending(), 2);
        assert_eq!(queue.pending_for("cam-a"), 1);
        assert_eq!(
            queue
                .try_enqueue(
                    "cam-c",
                    CapturePriority::Direct,
                    deadline(),
                    CancellationToken::new(),
                    4,
                )
                .unwrap_err()
                .code(),
            ErrorCode::QueueFull
        );

        first_cancel.cancel();
        tokio::task::yield_now().await;
        assert_eq!(queue.pending_for("cam-a"), 0);
        assert!(!queue.remove(first));

        let expiring = queue
            .try_enqueue(
                "cam-c",
                CapturePriority::Scheduled,
                Instant::now() + Duration::from_millis(10),
                CancellationToken::new(),
                5,
            )
            .unwrap();
        tokio::time::advance(Duration::from_millis(11)).await;
        tokio::task::yield_now().await;
        assert!(!queue.remove(expiring));
        assert_eq!(queue.pending(), 1);
    }

    #[test]
    fn full_ordinary_lane_cannot_block_or_evict_coalesced_safety_stop() {
        let lanes = ControlLanes::new(32).unwrap();
        for operation in 0..32 {
            lanes.try_push_ordinary(operation).unwrap();
        }
        assert_eq!(
            lanes.try_push_ordinary(33).unwrap_err().code(),
            ErrorCode::QueueFull
        );
        let later = Instant::now() + Duration::from_secs(5);
        let earlier = Instant::now() + Duration::from_secs(1);
        lanes
            .push_safety_stop(SafetyStop {
                pan: true,
                tilt: false,
                zoom: false,
                deadline: later,
            })
            .unwrap();
        lanes
            .push_safety_stop(SafetyStop {
                pan: false,
                tilt: true,
                zoom: true,
                deadline: earlier,
            })
            .unwrap();
        assert!(lanes.has_safety_stop());
        assert_eq!(lanes.ordinary_len(), 32);
        assert_eq!(
            lanes.pop_next(),
            Some(ControlWork::SafetyStop(SafetyStop {
                pan: true,
                tilt: true,
                zoom: true,
                deadline: earlier,
            }))
        );
        assert_eq!(lanes.pop_next(), Some(ControlWork::Ordinary(0)));
    }

    #[tokio::test]
    async fn disk_floor_math_accounts_for_outstanding_and_never_allows_growth() {
        let probe = Arc::new(FixedProbe::new(500, 1_000));
        let budget = DiskBudget::new(&output(100, 20), probe).unwrap();
        let cancellation = CancellationToken::new();
        let mut first = budget
            .reserve(300, deadline(), &cancellation)
            .await
            .unwrap();
        assert_eq!(budget.outstanding(), 300);
        assert_eq!(
            first.shrink(301).unwrap_err().code(),
            ErrorCode::ResourceLimit
        );
        first.shrink(200).unwrap();
        let second = budget
            .reserve(100, deadline(), &cancellation)
            .await
            .unwrap();
        assert_eq!(budget.outstanding(), 300);
        assert_eq!(
            expect_error(budget.reserve(1, deadline(), &cancellation).await).code(),
            ErrorCode::StoragePressure
        );
        drop(second);
        drop(first);
        assert_eq!(budget.outstanding(), 0);
        assert_eq!(percentage_floor(1_001, 5).unwrap(), 51);

        let overflow_budget = DiskBudget::new(
            &output(0, 100),
            Arc::new(FixedProbe::new(u64::MAX, u64::MAX)),
        )
        .unwrap();
        assert_eq!(
            expect_error(
                overflow_budget
                    .reserve(1, deadline(), &CancellationToken::new())
                    .await
            )
            .code(),
            ErrorCode::ResourceLimit
        );
        assert_eq!(overflow_budget.outstanding(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn byte_waiters_are_removed_on_cancel_and_reservations_only_shrink() {
        let budget = ByteBudget::new(10).unwrap();
        let mut held = budget
            .reserve(10, deadline(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            held.shrink(11).unwrap_err().code(),
            ErrorCode::ResourceLimit
        );
        let cancellation = CancellationToken::new();
        let waiting_budget = budget.clone();
        let waiting_cancel = cancellation.clone();
        let waiter =
            tokio::spawn(
                async move { waiting_budget.reserve(1, deadline(), &waiting_cancel).await },
            );
        tokio::task::yield_now().await;
        cancellation.cancel();
        assert_eq!(
            expect_error(waiter.await.unwrap()).code(),
            ErrorCode::CaptureCancelled
        );
        assert_eq!(budget.available(), 0);
        held.shrink(4).unwrap();
        assert_eq!(budget.available(), 6);
        drop(held);
        assert_eq!(budget.available(), 10);
    }

    #[tokio::test]
    async fn resource_group_and_disk_failure_roll_back_all_earlier_capacity() {
        let mut limits = limits(2, 1, 1, 100);
        limits.resource_groups = BTreeMap::from([(
            "nic-a".to_owned(),
            ResourceGroupConfig {
                max_concurrent_captures: 1,
            },
        )]);
        let controller =
            AdmissionController::new(&limits, &output(0, 0), Arc::new(FixedProbe::new(10, 1_000)))
                .unwrap();
        let error = expect_error(
            controller
                .acquire_capture(
                    CaptureResourceRequest {
                        resource_group: Some("nic-a".to_owned()),
                        maximum_frame_bytes: 20,
                        deadline: deadline(),
                    },
                    &CancellationToken::new(),
                )
                .await,
        );
        assert_eq!(error.code(), ErrorCode::StoragePressure);
        assert_eq!(controller.available_acquisitions(), 2);
        assert_eq!(controller.available_memory_bytes(), 100);
        assert_eq!(controller.outstanding_disk_bytes(), 0);
    }

    #[tokio::test]
    async fn resource_group_waiter_holds_order_then_cancellation_releases_everything() {
        let mut limits = limits(2, 1, 1, 100);
        limits.resource_groups = BTreeMap::from([(
            "shared-nic".to_owned(),
            ResourceGroupConfig {
                max_concurrent_captures: 1,
            },
        )]);
        let controller = Arc::new(
            AdmissionController::new(
                &limits,
                &output(0, 0),
                Arc::new(FixedProbe::new(1_000, 1_000)),
            )
            .unwrap(),
        );
        let held = controller
            .acquire_capture(
                CaptureResourceRequest {
                    resource_group: Some("shared-nic".to_owned()),
                    maximum_frame_bytes: 20,
                    deadline: deadline(),
                },
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let waiter_cancel = CancellationToken::new();
        let waiter_controller = Arc::clone(&controller);
        let waiter_token = waiter_cancel.clone();
        let waiter = tokio::spawn(async move {
            waiter_controller
                .acquire_capture(
                    CaptureResourceRequest {
                        resource_group: Some("shared-nic".to_owned()),
                        maximum_frame_bytes: 20,
                        deadline: deadline(),
                    },
                    &waiter_token,
                )
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(controller.available_acquisitions(), 0);
        assert_eq!(controller.available_memory_bytes(), 80);
        waiter_cancel.cancel();
        assert_eq!(
            expect_error(waiter.await.unwrap()).code(),
            ErrorCode::CaptureCancelled
        );
        assert_eq!(controller.available_acquisitions(), 1);
        assert_eq!(controller.available_memory_bytes(), 80);
        drop(held);
        assert_eq!(controller.available_acquisitions(), 2);
        assert_eq!(controller.available_memory_bytes(), 100);

        assert_eq!(
            expect_error(
                controller
                    .acquire_capture(
                        CaptureResourceRequest {
                            resource_group: None,
                            maximum_frame_bytes: 1,
                            deadline: Instant::now(),
                        },
                        &CancellationToken::new(),
                    )
                    .await
            )
            .code(),
            ErrorCode::CaptureTimeout
        );
        assert_eq!(controller.available_acquisitions(), 2);
    }

    #[tokio::test]
    async fn cancellation_during_disk_probe_rolls_back_global_group_and_memory() {
        let mut limits = limits(1, 1, 1, 100);
        limits.resource_groups = BTreeMap::from([(
            "usb".to_owned(),
            ResourceGroupConfig {
                max_concurrent_captures: 1,
            },
        )]);
        let controller = Arc::new(
            AdmissionController::new(&limits, &output(0, 0), Arc::new(HangingProbe)).unwrap(),
        );
        let cancellation = CancellationToken::new();
        let task_controller = Arc::clone(&controller);
        let task_cancel = cancellation.clone();
        let task = tokio::spawn(async move {
            task_controller
                .acquire_capture(
                    CaptureResourceRequest {
                        resource_group: Some("usb".to_owned()),
                        maximum_frame_bytes: 40,
                        deadline: deadline(),
                    },
                    &task_cancel,
                )
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(controller.available_acquisitions(), 0);
        assert_eq!(controller.available_memory_bytes(), 60);
        cancellation.cancel();
        assert_eq!(
            expect_error(task.await.unwrap()).code(),
            ErrorCode::CaptureCancelled
        );
        assert_eq!(controller.available_acquisitions(), 1);
        assert_eq!(controller.available_memory_bytes(), 100);
    }

    #[tokio::test]
    async fn acquisition_group_memory_and_stage_permits_are_independently_bounded() {
        let mut limits = limits(2, 1, 1, 100);
        limits.resource_groups = BTreeMap::from([(
            "nic".to_owned(),
            ResourceGroupConfig {
                max_concurrent_captures: 1,
            },
        )]);
        let controller = AdmissionController::new(
            &limits,
            &output(0, 0),
            Arc::new(FixedProbe::new(1_000, 1_000)),
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let first = controller
            .acquire_capture(
                CaptureResourceRequest {
                    resource_group: Some("nic".to_owned()),
                    maximum_frame_bytes: 60,
                    deadline: deadline(),
                },
                &cancel,
            )
            .await
            .unwrap();
        assert_eq!(controller.available_acquisitions(), 1);
        assert_eq!(controller.available_memory_bytes(), 40);
        let mut processing = first.finish_acquisition(40).unwrap();
        assert_eq!(controller.available_acquisitions(), 2);
        assert_eq!(controller.available_memory_bytes(), 60);
        assert_eq!(
            processing.shrink_memory(41).unwrap_err().code(),
            ErrorCode::ResourceLimit
        );
        processing.shrink_disk(50).unwrap();

        let encoder = controller
            .acquire_encoder(deadline(), &cancel)
            .await
            .unwrap();
        let writer = controller
            .acquire_writer(deadline(), &cancel)
            .await
            .unwrap();
        let encoder_cancel = CancellationToken::new();
        encoder_cancel.cancel();
        assert_eq!(
            expect_error(
                controller
                    .acquire_encoder(deadline(), &encoder_cancel)
                    .await
            )
            .code(),
            ErrorCode::CaptureCancelled
        );
        let writer_cancel = CancellationToken::new();
        writer_cancel.cancel();
        assert_eq!(
            expect_error(controller.acquire_writer(deadline(), &writer_cancel).await).code(),
            ErrorCode::CaptureCancelled
        );
        drop(encoder);
        drop(writer);
        processing.release_memory();
        assert_eq!(controller.available_memory_bytes(), 100);
        drop(processing);
        assert_eq!(controller.outstanding_disk_bytes(), 0);
    }

    #[tokio::test]
    async fn release_targets_support_256_sessions_and_exactly_32_acquisitions() {
        let default_limits = LimitsConfig::default();
        assert_eq!(default_limits.max_connected_cameras, 256);
        assert_eq!(default_limits.max_concurrent_captures, 32);
        assert_eq!(default_limits.max_queued_controls_per_camera, 32);

        let queue =
            CaptureAdmissionQueue::from_limits(&default_limits, 256, Duration::from_secs(1))
                .unwrap();
        for camera in 0..256 {
            queue
                .try_enqueue(
                    format!("camera-{camera}"),
                    CapturePriority::Scheduled,
                    deadline(),
                    CancellationToken::new(),
                    camera,
                )
                .unwrap();
        }
        assert_eq!(queue.pending(), 256);
        assert_eq!(
            queue
                .try_enqueue(
                    "overflow",
                    CapturePriority::Direct,
                    deadline(),
                    CancellationToken::new(),
                    256,
                )
                .unwrap_err()
                .code(),
            ErrorCode::QueueFull
        );
        let consumer = CancellationToken::new();
        for _ in 0..256 {
            queue.next(&consumer).await.unwrap();
        }

        let mut capacity_limits = default_limits;
        capacity_limits.max_in_flight_bytes = 1_024;
        capacity_limits.max_frame_bytes_per_camera = 1_024;
        let controller = AdmissionController::new(
            &capacity_limits,
            &output(0, 0),
            Arc::new(FixedProbe::new(10_000, 10_000)),
        )
        .unwrap();
        let cancellation = CancellationToken::new();
        let mut leases = Vec::new();
        for _ in 0..32 {
            leases.push(
                controller
                    .acquire_capture(
                        CaptureResourceRequest {
                            resource_group: None,
                            maximum_frame_bytes: 1,
                            deadline: deadline(),
                        },
                        &cancellation,
                    )
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(controller.available_acquisitions(), 0);
        let rejected = CancellationToken::new();
        rejected.cancel();
        assert_eq!(
            expect_error(
                controller
                    .acquire_capture(
                        CaptureResourceRequest {
                            resource_group: None,
                            maximum_frame_bytes: 1,
                            deadline: deadline(),
                        },
                        &rejected,
                    )
                    .await,
            )
            .code(),
            ErrorCode::CaptureCancelled
        );
        drop(leases);
        assert_eq!(controller.available_acquisitions(), 32);
    }
}
