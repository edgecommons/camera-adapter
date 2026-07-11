//! Durable exact-envelope outbox publication and pressure observation.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use edgecommons::messaging::MessagingService;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::catalog::{Catalog, OutboxRecord, OutboxStats};
use crate::{CameraError, Result};

const DEFAULT_BATCH: usize = 100;
const INITIAL_BACKOFF_MS: u64 = 250;
const MAX_BACKOFF_MS: u64 = 60_000;
const DELAYED_AGE_MS: u64 = 60_000;

/// Minimal durable-store contract used by the publisher and deterministic tests.
#[async_trait]
pub trait OutboxStore: Send + Sync {
    /// Returns records whose retry time is due.
    async fn pending(&self, now_ms: i64, limit: usize) -> Result<Vec<OutboxRecord>>;
    /// Records one ambiguous/failed attempt and its next retry time.
    async fn failed_attempt(
        &self,
        id: i64,
        attempted_at_ms: i64,
        next_available_at_ms: i64,
        error: String,
    ) -> Result<bool>;
    /// Marks a record delivered only after positive transport confirmation.
    async fn delivered(&self, id: i64, delivered_at_ms: i64) -> Result<bool>;
    /// Current pressure counters.
    async fn stats(&self, now_ms: i64) -> Result<OutboxStats>;
}

#[async_trait]
impl OutboxStore for Catalog {
    async fn pending(&self, now_ms: i64, limit: usize) -> Result<Vec<OutboxRecord>> {
        self.pending_outbox(now_ms, limit).await
    }

    async fn failed_attempt(
        &self,
        id: i64,
        attempted_at_ms: i64,
        next_available_at_ms: i64,
        error: String,
    ) -> Result<bool> {
        self.record_outbox_attempt(id, attempted_at_ms, next_available_at_ms, error)
            .await
    }

    async fn delivered(&self, id: i64, delivered_at_ms: i64) -> Result<bool> {
        self.mark_outbox_delivered(id, delivered_at_ms).await
    }

    async fn stats(&self, now_ms: i64) -> Result<OutboxStats> {
        self.outbox_stats(now_ms).await
    }
}

/// A strict publisher that returns success only after the transport acknowledges exact bytes.
#[async_trait]
pub trait ConfirmedPublisher: Send + Sync {
    /// Sends one exact stored envelope locally at QoS 1.
    async fn publish_confirmed(
        &self,
        topic: &str,
        encoded_envelope: &[u8],
        timeout: Duration,
    ) -> Result<()>;
}

/// Adapter from the public EdgeCommons messaging service.
pub struct EdgeCommonsConfirmedPublisher {
    messaging: Arc<dyn MessagingService>,
}

impl EdgeCommonsConfirmedPublisher {
    /// Wraps a guarded EdgeCommons messaging service.
    #[must_use]
    pub fn new(messaging: Arc<dyn MessagingService>) -> Self {
        Self { messaging }
    }
}

#[async_trait]
impl ConfirmedPublisher for EdgeCommonsConfirmedPublisher {
    async fn publish_confirmed(
        &self,
        topic: &str,
        encoded_envelope: &[u8],
        timeout: Duration,
    ) -> Result<()> {
        self.messaging
            .publish_encoded_confirmed(topic, encoded_envelope, timeout)
            .await
            .map_err(|error| CameraError::Messaging(error.to_string()))
    }
}

/// Stateful delayed-delivery condition used by health/events without exposing payloads or paths.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboxPressure {
    /// Pending exact envelopes.
    pub pending: u64,
    /// Age of the oldest pending record.
    pub oldest_age_ms: u64,
    /// Largest durable retry count.
    pub max_attempts: u64,
    /// Warning threshold reached by age or count.
    pub delayed: bool,
    /// Pending count reached 80% of result-record capacity.
    pub escalated: bool,
    /// Stable sanitized last failure category, never an envelope, topic, or path.
    pub last_error: Option<String>,
}

/// Whether the durable store remained usable during the latest outbox pass.
///
/// This is deliberately independent of [`OutboxPressure`]. A broker confirmation failure keeps
/// the row durable for retry and must not make readiness false; an error reading or committing the
/// outbox means the adapter can no longer prove that terminal delivery state is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxDurability {
    /// Whether the catalog accepted all reads and writes needed by the latest bounded pass.
    pub state_capacity_available: bool,
}

impl Default for OutboxDurability {
    fn default() -> Self {
        Self {
            state_capacity_available: true,
        }
    }
}

/// Result of one bounded publication pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutboxPass {
    /// Records selected.
    pub attempted: usize,
    /// Positively acknowledged and marked delivered.
    pub delivered: usize,
    /// Failed/ambiguous records left durable for retry.
    pub failed: usize,
}

/// Single-owner sequential outbox worker.
pub struct OutboxPublisher<S, P> {
    store: Arc<S>,
    publisher: Arc<P>,
    confirmation_timeout: Duration,
    poll_interval: Duration,
    max_result_records: u64,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    pressure_tx: watch::Sender<OutboxPressure>,
    durability_tx: watch::Sender<OutboxDurability>,
}

impl<S, P> OutboxPublisher<S, P>
where
    S: OutboxStore + 'static,
    P: ConfirmedPublisher + 'static,
{
    /// Creates a publisher with bounded transport and polling deadlines.
    pub fn new(
        store: Arc<S>,
        publisher: Arc<P>,
        confirmation_timeout: Duration,
        poll_interval: Duration,
        max_result_records: u64,
    ) -> Result<Self> {
        if confirmation_timeout.is_zero() || poll_interval.is_zero() || max_result_records == 0 {
            return Err(CameraError::Config {
                path: "outbox".to_string(),
                message: "timeouts and maxResultRecords must be positive".to_string(),
            });
        }
        let (pressure_tx, _) = watch::channel(OutboxPressure::default());
        let (durability_tx, _) = watch::channel(OutboxDurability::default());
        Ok(Self {
            store,
            publisher,
            confirmation_timeout,
            poll_interval,
            max_result_records,
            clock: Arc::new(UtcMillis::now),
            pressure_tx,
            durability_tx,
        })
    }

    /// Replaces wall time for deterministic tests.
    #[cfg(test)]
    fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    /// Subscribes to delayed-delivery pressure transitions.
    pub fn pressure(&self) -> watch::Receiver<OutboxPressure> {
        self.pressure_tx.subscribe()
    }

    /// Subscribes to catalog/durable-store availability changes.
    ///
    /// Transport confirmation failures are intentionally absent from this stream: their durable
    /// retry records preserve the adapter's state capacity and only contribute to pressure.
    pub fn durability(&self) -> watch::Receiver<OutboxDurability> {
        self.durability_tx.subscribe()
    }

    /// Executes one bounded pass. Exact bytes are never reconstructed.
    pub async fn run_once(&self) -> Result<OutboxPass> {
        let result = self.run_once_inner().await;
        self.durability_tx.send_if_modified(|durability| {
            let next = OutboxDurability {
                state_capacity_available: result.is_ok(),
            };
            if *durability == next {
                false
            } else {
                *durability = next;
                true
            }
        });
        result
    }

    async fn run_once_inner(&self) -> Result<OutboxPass> {
        let now = (self.clock)();
        let records = self.store.pending(now, DEFAULT_BATCH).await?;
        let mut pass = OutboxPass {
            attempted: records.len(),
            ..OutboxPass::default()
        };
        let mut last_error = None;
        for record in records {
            match self
                .publisher
                .publish_confirmed(
                    &record.topic,
                    &record.encoded_envelope,
                    self.confirmation_timeout,
                )
                .await
            {
                Ok(()) => {
                    // A failed SQLite commit after PUBACK intentionally leaves the row pending;
                    // the next attempt reuses identical bytes and yields at-least-once delivery.
                    if self.store.delivered(record.id, (self.clock)()).await? {
                        pass.delivered += 1;
                    }
                }
                Err(_) => {
                    pass.failed += 1;
                    let attempted_at = (self.clock)();
                    let backoff = retry_backoff_ms(&record.event_key, record.attempts + 1);
                    let next =
                        attempted_at.saturating_add(i64::try_from(backoff).unwrap_or(i64::MAX));
                    let sanitized =
                        "transport confirmation failed or remained ambiguous".to_string();
                    self.store
                        .failed_attempt(record.id, attempted_at, next, sanitized.clone())
                        .await?;
                    last_error = Some(sanitized);
                }
            }
        }
        self.refresh_pressure(last_error).await?;
        Ok(pass)
    }

    /// Runs until cancellation; shutdown never deletes pending records.
    pub async fn run(&self, cancellation: CancellationToken) -> Result<()> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if let Err(error) = self.run_once().await {
                tracing::warn!(error = %error, "durable outbox pass failed; records remain pending");
            }
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                () = tokio::time::sleep(self.poll_interval) => {}
            }
        }
    }

    async fn refresh_pressure(&self, last_error: Option<String>) -> Result<()> {
        let stats = self.store.stats((self.clock)()).await?;
        let count_threshold = 100_u64.max(self.max_result_records.saturating_add(99) / 100);
        let escalated_threshold = self
            .max_result_records
            .saturating_mul(80)
            .saturating_add(99)
            / 100;
        let prior = self.pressure_tx.borrow().clone();
        let retained_error = if stats.undelivered == 0 {
            None
        } else {
            last_error.or(prior.last_error)
        };
        self.pressure_tx.send_replace(OutboxPressure {
            pending: stats.undelivered,
            oldest_age_ms: stats.oldest_age_ms,
            max_attempts: stats.max_attempts,
            delayed: stats.oldest_age_ms >= DELAYED_AGE_MS || stats.undelivered >= count_threshold,
            escalated: stats.undelivered >= escalated_threshold,
            last_error: retained_error,
        });
        Ok(())
    }
}

fn retry_backoff_ms(event_key: &str, attempt: i64) -> u64 {
    let exponent = u32::try_from(attempt.saturating_sub(1))
        .unwrap_or(u32::MAX)
        .min(8);
    let base = INITIAL_BACKOFF_MS
        .saturating_mul(1_u64 << exponent)
        .min(MAX_BACKOFF_MS);
    let mut digest = Sha256::new();
    digest.update(event_key.as_bytes());
    digest.update(attempt.to_be_bytes());
    let bytes = digest.finalize();
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&bytes[..8]);
    let jitter_bound = base / 4;
    let jitter = if jitter_bound == 0 {
        0
    } else {
        u64::from_be_bytes(prefix) % (jitter_bound + 1)
    };
    base.saturating_add(jitter).min(MAX_BACKOFF_MS)
}

struct UtcMillis;

impl UtcMillis {
    fn now() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct FakeStore {
        records: Mutex<Vec<OutboxRecord>>,
        attempts: Mutex<Vec<(i64, i64, String)>>,
        delivered: Mutex<Vec<i64>>,
        refuse_delivery: Mutex<bool>,
        fail_next_stats: Mutex<bool>,
    }

    #[async_trait]
    impl OutboxStore for FakeStore {
        async fn pending(&self, now_ms: i64, limit: usize) -> Result<Vec<OutboxRecord>> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .iter()
                .filter(|record| {
                    record.delivered_at_ms.is_none() && record.available_at_ms <= now_ms
                })
                .take(limit)
                .cloned()
                .collect())
        }

        async fn failed_attempt(
            &self,
            id: i64,
            _attempted_at_ms: i64,
            next_available_at_ms: i64,
            error: String,
        ) -> Result<bool> {
            self.attempts
                .lock()
                .unwrap()
                .push((id, next_available_at_ms, error));
            if let Some(record) = self
                .records
                .lock()
                .unwrap()
                .iter_mut()
                .find(|record| record.id == id)
            {
                record.attempts += 1;
                record.available_at_ms = next_available_at_ms;
                return Ok(true);
            }
            Ok(false)
        }

        async fn delivered(&self, id: i64, delivered_at_ms: i64) -> Result<bool> {
            self.delivered.lock().unwrap().push(id);
            if *self.refuse_delivery.lock().unwrap() {
                return Ok(false);
            }
            if let Some(record) = self
                .records
                .lock()
                .unwrap()
                .iter_mut()
                .find(|record| record.id == id && record.delivered_at_ms.is_none())
            {
                record.delivered_at_ms = Some(delivered_at_ms);
                return Ok(true);
            }
            Ok(false)
        }

        async fn stats(&self, now_ms: i64) -> Result<OutboxStats> {
            if std::mem::take(&mut *self.fail_next_stats.lock().unwrap()) {
                return Err(CameraError::Catalog(
                    "simulated durable outbox-store failure".to_string(),
                ));
            }
            let records = self.records.lock().unwrap();
            let pending: Vec<_> = records
                .iter()
                .filter(|record| record.delivered_at_ms.is_none())
                .collect();
            Ok(OutboxStats {
                undelivered: pending.len() as u64,
                oldest_age_ms: pending
                    .iter()
                    .map(|record| now_ms.saturating_sub(record.created_at_ms).max(0) as u64)
                    .max()
                    .unwrap_or(0),
                max_attempts: pending
                    .iter()
                    .map(|record| record.attempts.max(0) as u64)
                    .max()
                    .unwrap_or(0),
            })
        }
    }

    #[derive(Default)]
    struct FakePublisher {
        outcomes: Mutex<VecDeque<bool>>,
        payloads: Mutex<Vec<Vec<u8>>>,
    }

    #[async_trait]
    impl ConfirmedPublisher for FakePublisher {
        async fn publish_confirmed(
            &self,
            _topic: &str,
            encoded_envelope: &[u8],
            _timeout: Duration,
        ) -> Result<()> {
            self.payloads
                .lock()
                .unwrap()
                .push(encoded_envelope.to_vec());
            if self.outcomes.lock().unwrap().pop_front().unwrap_or(true) {
                Ok(())
            } else {
                Err(CameraError::Messaging(
                    "secret-bearing raw transport failure".to_string(),
                ))
            }
        }
    }

    fn record() -> OutboxRecord {
        OutboxRecord {
            id: 7,
            event_key: "evt_7".to_string(),
            capture_id: Some("cap_7".to_string()),
            group_id: None,
            message_kind: "terminal".to_string(),
            topic: "ecv1/device/camera-adapter/camera-a/app/image/captured".to_string(),
            envelope_uuid: "uuid-7".to_string(),
            encoded_envelope: vec![1, 2, 3, 4],
            created_at_ms: 1_000,
            available_at_ms: 1_000,
            attempts: 0,
            last_attempt_at_ms: None,
            delivered_at_ms: None,
            last_error: None,
        }
    }

    #[tokio::test]
    async fn retry_reuses_exact_bytes_and_only_confirmation_marks_delivered() {
        let store = Arc::new(FakeStore::default());
        store.records.lock().unwrap().push(record());
        let publisher = Arc::new(FakePublisher::default());
        *publisher.outcomes.lock().unwrap() = VecDeque::from([false, true]);
        let now = Arc::new(std::sync::atomic::AtomicI64::new(2_000));
        let clock_now = now.clone();
        let worker = OutboxPublisher::new(
            store.clone(),
            publisher.clone(),
            Duration::from_secs(1),
            Duration::from_millis(10),
            1_000,
        )
        .unwrap()
        .with_clock(Arc::new(move || {
            clock_now.load(std::sync::atomic::Ordering::SeqCst)
        }));

        let first = worker.run_once().await.unwrap();
        assert_eq!(first.failed, 1);
        assert!(store.delivered.lock().unwrap().is_empty());
        let next = store.attempts.lock().unwrap()[0].1;
        now.store(next, std::sync::atomic::Ordering::SeqCst);
        let second = worker.run_once().await.unwrap();
        assert_eq!(second.delivered, 1);
        let payloads = publisher.payloads.lock().unwrap();
        assert_eq!(payloads.as_slice(), [vec![1, 2, 3, 4], vec![1, 2, 3, 4]]);
        assert_eq!(store.delivered.lock().unwrap().as_slice(), [7]);
        assert_eq!(
            store.attempts.lock().unwrap()[0].2,
            "transport confirmation failed or remained ambiguous"
        );
    }

    #[tokio::test]
    async fn pressure_thresholds_are_stateful_and_payload_free() {
        let store = Arc::new(FakeStore::default());
        let mut record = record();
        record.created_at_ms = 0;
        store.records.lock().unwrap().push(record);
        let publisher = Arc::new(FakePublisher::default());
        let worker = OutboxPublisher::new(
            store.clone(),
            publisher,
            Duration::from_secs(1),
            Duration::from_millis(10),
            1,
        )
        .unwrap()
        .with_clock(Arc::new(|| 61_000));
        let mut pressure = worker.pressure();
        worker
            .refresh_pressure(Some("transport confirmation failed".to_string()))
            .await
            .unwrap();
        pressure.changed().await.unwrap();
        assert!(pressure.borrow().delayed);
        assert!(pressure.borrow().escalated);
        assert_eq!(
            pressure.borrow().last_error.as_deref(),
            Some("transport confirmation failed")
        );

        store.records.lock().unwrap()[0].delivered_at_ms = Some(61_000);
        worker.refresh_pressure(None).await.unwrap();
        pressure.changed().await.unwrap();
        assert_eq!(*pressure.borrow(), OutboxPressure::default());
    }

    #[tokio::test]
    async fn durable_store_failure_flips_readiness_and_a_later_catalog_pass_recovers() {
        let store = Arc::new(FakeStore::default());
        *store.fail_next_stats.lock().unwrap() = true;
        let publisher = Arc::new(FakePublisher::default());
        let worker = OutboxPublisher::new(
            store,
            publisher,
            Duration::from_secs(1),
            Duration::from_millis(10),
            1_000,
        )
        .unwrap()
        .with_clock(Arc::new(|| 2_000));
        let mut durability = worker.durability();

        assert!(worker.run_once().await.is_err());
        durability.changed().await.unwrap();
        assert!(!durability.borrow().state_capacity_available);

        worker.run_once().await.unwrap();
        durability.changed().await.unwrap();
        assert!(durability.borrow().state_capacity_available);
    }

    #[tokio::test]
    async fn broker_confirmation_failure_does_not_flip_durable_store_availability() {
        let store = Arc::new(FakeStore::default());
        store.records.lock().unwrap().push(record());
        let publisher = Arc::new(FakePublisher::default());
        *publisher.outcomes.lock().unwrap() = VecDeque::from([false]);
        let worker = OutboxPublisher::new(
            store,
            publisher,
            Duration::from_secs(1),
            Duration::from_millis(10),
            1_000,
        )
        .unwrap()
        .with_clock(Arc::new(|| 2_000));
        let durability = worker.durability();

        let pass = worker.run_once().await.unwrap();
        assert_eq!(pass.failed, 1);
        assert!(durability.borrow().state_capacity_available);
        assert!(
            !durability.has_changed().unwrap(),
            "broker pressure must not be mistaken for durable-store failure"
        );
    }

    #[tokio::test]
    async fn confirmed_publish_with_no_delivery_transition_is_not_counted_delivered() {
        let store = Arc::new(FakeStore::default());
        store.records.lock().unwrap().push(record());
        *store.refuse_delivery.lock().unwrap() = true;
        let publisher = Arc::new(FakePublisher::default());
        let worker = OutboxPublisher::new(
            store.clone(),
            publisher,
            Duration::from_secs(1),
            Duration::from_millis(10),
            1_000,
        )
        .unwrap()
        .with_clock(Arc::new(|| 2_000));

        let pass = worker.run_once().await.unwrap();
        assert_eq!(pass.attempted, 1);
        assert_eq!(pass.delivered, 0);
        assert_eq!(pass.failed, 0);
        assert_eq!(store.delivered.lock().unwrap().as_slice(), [7]);
    }

    #[test]
    fn constructor_rejects_zero_deadlines_or_capacity() {
        let store = Arc::new(FakeStore::default());
        let publisher = Arc::new(FakePublisher::default());
        for (confirmation_timeout, poll_interval, max_result_records) in [
            (Duration::ZERO, Duration::from_secs(1), 1),
            (Duration::from_secs(1), Duration::ZERO, 1),
            (Duration::from_secs(1), Duration::from_secs(1), 0),
        ] {
            let result = OutboxPublisher::new(
                store.clone(),
                publisher.clone(),
                confirmation_timeout,
                poll_interval,
                max_result_records,
            );
            let Err(error) = result else {
                panic!("zero values must be rejected");
            };
            assert_eq!(error.code(), crate::ErrorCode::InvalidRequest);
        }
    }

    #[tokio::test]
    async fn already_cancelled_worker_exits_without_publishing() {
        let store = Arc::new(FakeStore::default());
        store.records.lock().unwrap().push(record());
        let publisher = Arc::new(FakePublisher::default());
        let worker = OutboxPublisher::new(
            store,
            publisher.clone(),
            Duration::from_secs(1),
            Duration::from_millis(10),
            1_000,
        )
        .unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        worker.run(cancellation).await.unwrap();
        assert!(publisher.payloads.lock().unwrap().is_empty());
    }

    #[test]
    fn backoff_is_bounded_deterministic_and_event_specific() {
        assert_eq!(retry_backoff_ms("evt-a", 1), retry_backoff_ms("evt-a", 1));
        assert!(retry_backoff_ms("evt-a", 1) >= INITIAL_BACKOFF_MS);
        assert!(retry_backoff_ms("evt-a", 100) <= MAX_BACKOFF_MS);
        assert_eq!(retry_backoff_ms("evt-a", 3), 1_126);
        assert_eq!(retry_backoff_ms("evt-b", 3), 1_152);
    }
}
