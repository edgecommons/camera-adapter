//! Deterministic six-field cron scheduling with explicit DST, jitter, misfire, and overlap rules.

use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, LocalResult, SecondsFormat, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use sha2::{Digest, Sha256};

use crate::{
    CameraError, Result,
    config::{MisfirePolicy, OverlapPolicy, ScheduleConfig},
};

const MAX_OCCURRENCES_PER_EVALUATION: usize = 10_000;

/// One unjittered durable occurrence plus its deterministic admission time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleOccurrence {
    /// Camera instance token.
    pub instance: String,
    /// Configured schedule token.
    pub schedule_id: String,
    /// Unjittered cron occurrence; this is the durable deduplication time.
    pub intended_fire_time: DateTime<Utc>,
    /// Deterministically delayed admission time.
    pub admit_at: DateTime<Utc>,
    /// Stable jitter applied to the occurrence.
    pub jitter: Duration,
}

impl ScheduleOccurrence {
    /// Durable schedule-occurrence key components.
    #[must_use]
    pub fn key(&self) -> (&str, &str, DateTime<Utc>) {
        (&self.instance, &self.schedule_id, self.intended_fire_time)
    }
}

/// Result of one bounded scheduler evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleDecision {
    /// No occurrence has reached its jittered admission time.
    NotDue,
    /// Admit exactly this occurrence.
    Admit {
        /// Latest due occurrence selected by the misfire policy.
        occurrence: ScheduleOccurrence,
        /// Number of due occurrences consumed, including coalesced/skipped predecessors.
        consumed: usize,
    },
    /// Due occurrences were discarded by `misfirePolicy=skip`.
    SkippedMisfire {
        /// Latest discarded occurrence.
        latest: ScheduleOccurrence,
        /// Number of discarded occurrences.
        consumed: usize,
    },
    /// An active nonterminal job caused `overlapPolicy=skip`.
    SkippedOverlap {
        /// Latest due occurrence consumed by the overlap decision.
        occurrence: ScheduleOccurrence,
        /// Number of due occurrences consumed.
        consumed: usize,
    },
}

impl ScheduleDecision {
    /// Latest unjittered occurrence the caller must persist as consumed, even when skipped.
    #[must_use]
    pub fn consumed_through(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::NotDue => None,
            Self::Admit { occurrence, .. } | Self::SkippedOverlap { occurrence, .. } => {
                Some(occurrence.intended_fire_time)
            }
            Self::SkippedMisfire { latest, .. } => Some(latest.intended_fire_time),
        }
    }
}

/// Compiled immutable schedule plan.
#[derive(Debug, Clone)]
pub struct SchedulePlan {
    instance: String,
    schedule_id: String,
    cron: Cron,
    timezone: Tz,
    jitter_seconds: u32,
    misfire_policy: MisfirePolicy,
    overlap_policy: OverlapPolicy,
}

impl SchedulePlan {
    /// Compiles an already component-validated schedule.
    pub fn compile(instance: impl Into<String>, config: &ScheduleConfig) -> Result<Self> {
        if config.cron.split_whitespace().count() != 6 {
            return schedule_error("cron must contain exactly six fields including seconds");
        }
        let cron = Cron::from_str(&config.cron).map_err(|error| CameraError::Config {
            path: "schedule.cron".to_string(),
            message: format!("invalid six-field cron: {error}"),
        })?;
        let timezone = config
            .timezone
            .parse::<Tz>()
            .map_err(|error| CameraError::Config {
                path: "schedule.timezone".to_string(),
                message: format!("invalid IANA timezone: {error}"),
            })?;
        if config.jitter_seconds > 3_600 {
            return schedule_error("jitterSeconds must be in range 0..=3600");
        }
        Ok(Self {
            instance: instance.into(),
            schedule_id: config.id.clone(),
            cron,
            timezone,
            jitter_seconds: config.jitter_seconds,
            misfire_policy: config.misfire_policy,
            overlap_policy: config.overlap_policy,
        })
    }

    /// Returns the durable `(instance, scheduleId)` identity of this compiled plan.
    #[must_use]
    pub fn key_parts(&self) -> (String, String) {
        (self.instance.clone(), self.schedule_id.clone())
    }

    /// Whether an overlap observation can change this plan's decision at all.
    ///
    /// [`Self::evaluate`] consults `has_nonterminal_overlap` in exactly one place: after it has
    /// already decided that an occurrence is due, and only under [`OverlapPolicy::Skip`]. An overlap
    /// observation can therefore only ever turn an `Admit` into a `SkippedOverlap` -- it can never
    /// make a not-due schedule due, nor change a misfire.
    ///
    /// That matters because answering the question costs a catalog round-trip, and the poll loop
    /// used to pay it every 200 ms per schedule whether or not anything was due: at 256 cameras,
    /// ~1,280 reads/second against the same two connections that carry the capture write path.
    #[must_use]
    pub fn skips_on_overlap(&self) -> bool {
        self.overlap_policy == OverlapPolicy::Skip
    }

    /// Returns the first accepted-DST occurrence strictly after `after`.
    pub fn next_after(&self, after: DateTime<Utc>) -> Result<ScheduleOccurrence> {
        let mut cursor = after.with_timezone(&self.timezone);
        loop {
            let candidate = self
                .cron
                .find_next_occurrence(&cursor, false)
                .map_err(|error| CameraError::Catalog(format!("cron search failed: {error}")))?;
            let actually_matches = self.cron.is_time_matching(&candidate).map_err(|error| {
                CameraError::Catalog(format!("cron match verification failed: {error}"))
            })?;
            if actually_matches && is_earlier_or_unambiguous(&candidate, self.timezone) {
                return self.occurrence(candidate.with_timezone(&Utc));
            }
            // Croner intentionally permits both sides of an overlap for interval expressions;
            // the camera contract consumes only the earlier local occurrence.
            cursor = candidate;
        }
    }

    /// Applies misfire and overlap policy to occurrences after `last_consumed` and due by `now`.
    ///
    /// `last_consumed` is an unjittered intended time, not the last wall-clock poll. This prevents a
    /// positive jitter delay from being accidentally skipped by a faster poll loop.
    pub fn evaluate(
        &self,
        last_consumed: DateTime<Utc>,
        now: DateTime<Utc>,
        misfire_grace: Duration,
        has_nonterminal_overlap: bool,
    ) -> Result<ScheduleDecision> {
        let grace = chrono::Duration::from_std(misfire_grace)
            .map_err(|_| CameraError::Catalog("misfire grace is too large".to_string()))?;
        let mut cursor = last_consumed;
        let mut latest = None;
        let mut consumed = 0_usize;
        loop {
            if consumed == MAX_OCCURRENCES_PER_EVALUATION {
                return Err(CameraError::Catalog(
                    "scheduler evaluation exceeded its bounded occurrence scan".to_string(),
                ));
            }
            let occurrence = self.next_after(cursor)?;
            if occurrence.admit_at > now {
                break;
            }
            cursor = occurrence.intended_fire_time;
            latest = Some(occurrence);
            consumed += 1;
        }

        let Some(latest) = latest else {
            return Ok(ScheduleDecision::NotDue);
        };
        let is_misfire = latest.admit_at + grace < now;
        if is_misfire && self.misfire_policy == MisfirePolicy::Skip {
            return Ok(ScheduleDecision::SkippedMisfire { latest, consumed });
        }
        if has_nonterminal_overlap && self.overlap_policy == OverlapPolicy::Skip {
            return Ok(ScheduleDecision::SkippedOverlap {
                occurrence: latest,
                consumed,
            });
        }
        Ok(ScheduleDecision::Admit {
            occurrence: latest,
            consumed,
        })
    }

    fn occurrence(&self, intended_fire_time: DateTime<Utc>) -> Result<ScheduleOccurrence> {
        let jitter_seconds = stable_jitter_seconds(
            &self.instance,
            &self.schedule_id,
            intended_fire_time,
            self.jitter_seconds,
        );
        let jitter = Duration::from_secs(u64::from(jitter_seconds));
        let chrono_jitter = chrono::Duration::from_std(jitter)
            .map_err(|_| CameraError::Catalog("schedule jitter is too large".to_string()))?;
        Ok(ScheduleOccurrence {
            instance: self.instance.clone(),
            schedule_id: self.schedule_id.clone(),
            intended_fire_time,
            admit_at: intended_fire_time + chrono_jitter,
            jitter,
        })
    }
}

/// Stable jitter defined by the binding addendum.
#[must_use]
pub fn stable_jitter_seconds(
    instance: &str,
    schedule_id: &str,
    intended_fire_time: DateTime<Utc>,
    jitter_seconds: u32,
) -> u32 {
    let canonical_time = intended_fire_time.to_rfc3339_opts(SecondsFormat::Secs, true);
    let mut digest = Sha256::new();
    digest.update(instance.as_bytes());
    digest.update([0]);
    digest.update(schedule_id.as_bytes());
    digest.update([0]);
    digest.update(canonical_time.as_bytes());
    let bytes = digest.finalize();
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&bytes[..8]);
    let value = u64::from_be_bytes(prefix);
    (value % (u64::from(jitter_seconds) + 1)) as u32
}

fn is_earlier_or_unambiguous<T>(candidate: &DateTime<T>, timezone: Tz) -> bool
where
    T: TimeZone,
{
    let local = candidate.with_timezone(&timezone);
    match timezone.from_local_datetime(&local.naive_local()) {
        LocalResult::Single(_) => true,
        LocalResult::Ambiguous(earlier, _) => local == earlier,
        LocalResult::None => false,
    }
}

fn schedule_error<T>(message: impl Into<String>) -> Result<T> {
    Err(CameraError::Config {
        path: "schedule".to_string(),
        message: message.into(),
    })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn config(cron: &str, timezone: &str) -> ScheduleConfig {
        ScheduleConfig {
            id: "daily".to_string(),
            enabled: true,
            cron: cron.to_string(),
            timezone: timezone.to_string(),
            capture_profile: "main".to_string(),
            misfire_policy: MisfirePolicy::Skip,
            overlap_policy: OverlapPolicy::Skip,
            jitter_seconds: 0,
        }
    }

    /// The poll loop is allowed to skip the overlap query, so prove the query cannot matter.
    ///
    /// `run_schedule` used to open every 200 ms tick with a `jobs_page(.., 1_000)` catalog read --
    /// ~1,280 reads/second at 256 cameras, through the same two connections as the capture path's
    /// fsync-per-write transactions -- before anything had established that an occurrence was even
    /// due. It now evaluates first and asks only when the answer can still change the outcome.
    ///
    /// That is only sound if an overlap observation cannot alter any decision other than `Admit`.
    /// This is the test that says so.
    #[test]
    fn an_overlap_observation_can_only_ever_suppress_an_admit() {
        let plan = SchedulePlan::compile("camera-a", &config("0 0 3 * * *", "UTC")).unwrap();
        let grace = Duration::from_secs(60);

        // Nothing is due: the catalog cannot be consulted into relevance.
        let quiet = Utc.with_ymd_and_hms(2026, 7, 13, 3, 0, 30).unwrap();
        let last_consumed = Utc.with_ymd_and_hms(2026, 7, 13, 3, 0, 0).unwrap();
        assert_eq!(
            plan.evaluate(last_consumed, quiet, grace, false).unwrap(),
            ScheduleDecision::NotDue
        );
        assert_eq!(
            plan.evaluate(last_consumed, quiet, grace, true).unwrap(),
            ScheduleDecision::NotDue,
            "an overlap must never make a not-due schedule due -- this is what makes eliding the \
             read exact rather than merely cheap"
        );

        // Due: and here, and only here, the observation earns its round-trip.
        let due = Utc.with_ymd_and_hms(2026, 7, 14, 3, 0, 30).unwrap();
        assert!(matches!(
            plan.evaluate(last_consumed, due, grace, false).unwrap(),
            ScheduleDecision::Admit { .. }
        ));
        assert!(matches!(
            plan.evaluate(last_consumed, due, grace, true).unwrap(),
            ScheduleDecision::SkippedOverlap { .. }
        ));
        assert!(plan.skips_on_overlap());
    }

    /// A schedule that does not skip on overlap must never pay for the query at all.
    #[test]
    fn a_queueing_schedule_never_needs_the_overlap_query() {
        let mut schedule = config("0 0 3 * * *", "UTC");
        schedule.overlap_policy = OverlapPolicy::Queue;
        let plan = SchedulePlan::compile("camera-a", &schedule).unwrap();
        let grace = Duration::from_secs(60);
        let last_consumed = Utc.with_ymd_and_hms(2026, 7, 13, 3, 0, 0).unwrap();
        let due = Utc.with_ymd_and_hms(2026, 7, 14, 3, 0, 30).unwrap();

        assert!(!plan.skips_on_overlap());
        assert_eq!(
            plan.evaluate(last_consumed, due, grace, true).unwrap(),
            plan.evaluate(last_consumed, due, grace, false).unwrap(),
            "with overlapPolicy=queue the observation is inert, so asking for it is pure contention"
        );
    }

    #[test]
    fn forward_dst_nonexistent_time_is_skipped() {
        let plan =
            SchedulePlan::compile("camera-a", &config("0 30 2 * * *", "America/New_York")).unwrap();
        let after = Utc.with_ymd_and_hms(2026, 3, 7, 8, 0, 0).unwrap();
        let occurrence = plan.next_after(after).unwrap();
        // 02:30 on March 8 does not exist; the next daily occurrence is March 9 02:30 EDT.
        assert_eq!(
            occurrence.intended_fire_time,
            Utc.with_ymd_and_hms(2026, 3, 9, 6, 30, 0).unwrap()
        );
    }

    #[test]
    fn backward_dst_repeated_time_fires_only_earlier_occurrence() {
        let plan =
            SchedulePlan::compile("camera-a", &config("0 30 1 * * *", "America/New_York")).unwrap();
        let before = Utc.with_ymd_and_hms(2026, 11, 1, 4, 0, 0).unwrap();
        let first = plan.next_after(before).unwrap();
        assert_eq!(
            first.intended_fire_time,
            Utc.with_ymd_and_hms(2026, 11, 1, 5, 30, 0).unwrap()
        );
        let next = plan.next_after(first.intended_fire_time).unwrap();
        assert_eq!(
            next.intended_fire_time,
            Utc.with_ymd_and_hms(2026, 11, 2, 6, 30, 0).unwrap()
        );
    }

    #[test]
    fn jitter_is_canonical_bounded_and_repeatable() {
        let intended = Utc.with_ymd_and_hms(2026, 7, 10, 14, 0, 0).unwrap();
        let first = stable_jitter_seconds("camera-a", "daily", intended, 30);
        let second = stable_jitter_seconds("camera-a", "daily", intended, 30);
        assert_eq!(first, second);
        assert_eq!(first, 7, "pinned SHA-256 canonical-time vector");
        assert_eq!(stable_jitter_seconds("camera-a", "daily", intended, 0), 0);
    }

    #[test]
    fn skip_and_coalesce_consume_missed_occurrences_once() {
        let mut schedule = config("0 * * * * *", "UTC");
        let start = Utc.with_ymd_and_hms(2026, 7, 10, 14, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 10, 14, 5, 30).unwrap();
        let skip = SchedulePlan::compile("camera-a", &schedule).unwrap();
        assert!(matches!(
            skip.evaluate(start, now, Duration::from_secs(1), false)
                .unwrap(),
            ScheduleDecision::SkippedMisfire { consumed: 5, .. }
        ));

        schedule.misfire_policy = MisfirePolicy::Coalesce;
        let coalesce = SchedulePlan::compile("camera-a", &schedule).unwrap();
        let decision = coalesce
            .evaluate(start, now, Duration::from_secs(1), false)
            .unwrap();
        match decision {
            ScheduleDecision::Admit {
                occurrence,
                consumed,
            } => {
                assert_eq!(consumed, 5);
                assert_eq!(
                    occurrence.intended_fire_time,
                    Utc.with_ymd_and_hms(2026, 7, 10, 14, 5, 0).unwrap()
                );
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn overlap_skip_consumes_but_queue_policy_admits() {
        let mut schedule = config("0 * * * * *", "UTC");
        schedule.misfire_policy = MisfirePolicy::Coalesce;
        let start = Utc.with_ymd_and_hms(2026, 7, 10, 14, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 10, 14, 1, 0).unwrap();
        let skip = SchedulePlan::compile("camera-a", &schedule).unwrap();
        assert!(matches!(
            skip.evaluate(start, now, Duration::from_secs(1), true)
                .unwrap(),
            ScheduleDecision::SkippedOverlap { .. }
        ));
        schedule.overlap_policy = OverlapPolicy::Queue;
        let queue = SchedulePlan::compile("camera-a", &schedule).unwrap();
        assert!(matches!(
            queue
                .evaluate(start, now, Duration::from_secs(1), true)
                .unwrap(),
            ScheduleDecision::Admit { .. }
        ));
    }

    #[test]
    fn compilation_and_not_due_decisions_are_explicit() {
        assert!(SchedulePlan::compile("camera-a", &config("* * * * *", "UTC")).is_err());
        assert!(SchedulePlan::compile("camera-a", &config("0 * * * * *", "Not/AZone")).is_err());
        let mut too_much_jitter = config("0 * * * * *", "UTC");
        too_much_jitter.jitter_seconds = 3_601;
        assert!(SchedulePlan::compile("camera-a", &too_much_jitter).is_err());

        let plan = SchedulePlan::compile("camera-a", &config("0 * * * * *", "UTC")).unwrap();
        assert_eq!(
            plan.key_parts(),
            ("camera-a".to_string(), "daily".to_string())
        );
        let start = Utc.with_ymd_and_hms(2026, 7, 10, 14, 0, 0).unwrap();
        let not_due = plan
            .evaluate(
                start,
                start + chrono::Duration::seconds(30),
                Duration::ZERO,
                false,
            )
            .unwrap();
        assert_eq!(not_due, ScheduleDecision::NotDue);
        assert_eq!(not_due.consumed_through(), None);
    }
}
