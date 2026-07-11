//! Camera lifecycle state, deterministic reconnect policy, and panic isolation.
//!
//! The runtime supervisor owns connection attempts and actors. This module keeps its transition and
//! retry rules pure so connection races can be falsified without a protocol or wall-clock dependency.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Once;
use std::time::Duration;

use futures::FutureExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{CameraError, Result};

static SANITIZED_PANIC_HOOK: Once = Once::new();

/// Installs the process-wide panic hook that redacts panic payloads before any backend starts.
///
/// Catching an unwind does not suppress Rust's panic hook. Calling this once during process startup
/// is therefore part of the supervisor security boundary; only source location is retained.
pub fn install_sanitized_panic_hook() {
    SANITIZED_PANIC_HOOK.call_once(|| {
        std::panic::set_hook(Box::new(|info| {
            if let Some(location) = info.location() {
                tracing::error!(
                    file = location.file(),
                    line = location.line(),
                    "panic payload redacted at camera-adapter supervisor boundary"
                );
            } else {
                tracing::error!("panic payload redacted at camera-adapter supervisor boundary");
            }
        }));
    });
}

/// Publicly observable per-camera lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CameraLifecycleState {
    /// Configured but disabled or fully drained after removal.
    Disabled,
    /// Establishing a stable-selector protocol session and probing capabilities.
    Connecting,
    /// Session is usable.
    Online,
    /// Session remains usable but an optional capability/check is impaired.
    Degraded,
    /// Waiting before another bounded connection attempt.
    Backoff,
    /// New admission is closed while active work/session shutdown completes.
    Draining,
    /// Process-level supervisor termination completed.
    Stopped,
}

impl CameraLifecycleState {
    /// Whether the design permits a transition between lifecycle states.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        use CameraLifecycleState as State;
        match self {
            State::Disabled => matches!(next, State::Connecting | State::Stopped),
            State::Connecting => matches!(next, State::Online | State::Backoff | State::Draining),
            State::Online => matches!(next, State::Degraded | State::Backoff | State::Draining),
            State::Degraded => matches!(next, State::Online | State::Backoff | State::Draining),
            State::Backoff => matches!(next, State::Connecting | State::Draining),
            State::Draining => matches!(next, State::Disabled | State::Stopped),
            State::Stopped => false,
        }
    }
}

/// One supervisor-owned lifecycle machine and monotonic live-session generation.
#[derive(Debug, Clone)]
pub struct LifecycleMachine {
    state: CameraLifecycleState,
    session_generation: u64,
}

impl LifecycleMachine {
    /// Creates the configured initial state.
    #[must_use]
    pub const fn new(enabled: bool) -> Self {
        Self {
            state: if enabled {
                CameraLifecycleState::Connecting
            } else {
                CameraLifecycleState::Disabled
            },
            session_generation: 0,
        }
    }

    /// Current state.
    #[must_use]
    pub const fn state(&self) -> CameraLifecycleState {
        self.state
    }

    /// Monotonic generation of the most recently established new protocol session.
    #[must_use]
    pub const fn session_generation(&self) -> u64 {
        self.session_generation
    }

    /// Applies one legal transition. Connecting-to-online allocates the next session generation.
    pub fn transition(&mut self, next: CameraLifecycleState) -> Result<u64> {
        if !self.state.can_transition_to(next) {
            return Err(CameraError::Catalog(format!(
                "invalid camera lifecycle transition {:?} -> {:?}",
                self.state, next
            )));
        }
        if self.state == CameraLifecycleState::Connecting && next == CameraLifecycleState::Online {
            self.session_generation = self.session_generation.checked_add(1).ok_or_else(|| {
                CameraError::Catalog("camera session generation exhausted".to_owned())
            })?;
        }
        self.state = next;
        Ok(self.session_generation)
    }
}

/// Retry class used for observability and slower permanent/auth/config failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetryClass {
    /// Disconnect, timeout, or other potentially transient transport failure.
    Transient,
    /// Authentication, selector ambiguity, or permanent configuration/security failure.
    Permanent,
}

/// Validated reconnect timing policy.
#[derive(Debug, Clone, Copy)]
pub struct BackoffPolicy {
    minimum: Duration,
    maximum: Duration,
}

impl BackoffPolicy {
    /// Creates a policy whose maximum is the pre-jitter cap.
    pub fn new(minimum: Duration, maximum: Duration) -> Result<Self> {
        if minimum.is_zero() || maximum < minimum {
            return Err(CameraError::Config {
                path: "component.global.timeouts.reconnectBackoff".to_owned(),
                message: "minimum must be positive and no greater than maximum".to_owned(),
            });
        }
        Ok(Self { minimum, maximum })
    }

    /// Computes deterministic positive jitter for one retry attempt.
    #[must_use]
    pub fn delay(
        self,
        instance: &str,
        config_generation: u64,
        class: RetryClass,
        attempt: u32,
    ) -> Duration {
        let transient_base = saturating_double(self.minimum, attempt).min(self.maximum);
        let base = match class {
            RetryClass::Transient => transient_base,
            RetryClass::Permanent => {
                let permanent_floor = saturating_multiply(self.minimum, 8)
                    .max(Duration::from_secs(10))
                    .min(self.maximum);
                transient_base.max(permanent_floor)
            }
        };
        let mut digest = Sha256::new();
        digest.update(instance.as_bytes());
        digest.update([0]);
        digest.update(config_generation.to_be_bytes());
        digest.update([match class {
            RetryClass::Transient => 0,
            RetryClass::Permanent => 1,
        }]);
        digest.update(attempt.to_be_bytes());
        let bytes = digest.finalize();
        let sample = u16::from_be_bytes([bytes[0], bytes[1]]) as u128;
        let jitter_basis_points = sample % 2_001;
        let base_nanos = base.as_nanos();
        let jitter_nanos = base_nanos.saturating_mul(jitter_basis_points) / 10_000;
        duration_from_nanos(base_nanos.saturating_add(jitter_nanos))
    }
}

/// Runs one backend-owned future at the supervisor's panic boundary.
///
/// Native aborts and segmentation faults remain process-fatal; deployment sharding/process
/// isolation is the mitigation for those failures. [`install_sanitized_panic_hook`] must run before
/// backend creation so the ordinary panic hook cannot print a secret-bearing payload first.
pub async fn isolate_backend_panic<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(result) => result,
        Err(_) => Err(CameraError::Backend {
            backend: "supervisor",
            message: "backend task panicked at the supervisor boundary".to_owned(),
        }),
    }
}

fn saturating_double(value: Duration, attempt: u32) -> Duration {
    let multiplier = 1_u128.checked_shl(attempt.min(127)).unwrap_or(u128::MAX);
    duration_from_nanos(value.as_nanos().saturating_mul(multiplier))
}

fn saturating_multiply(value: Duration, multiplier: u128) -> Duration {
    duration_from_nanos(value.as_nanos().saturating_mul(multiplier))
}

fn duration_from_nanos(nanos: u128) -> Duration {
    let seconds = (nanos / 1_000_000_000).min(u128::from(u64::MAX)) as u64;
    let subsecond = if seconds == u64::MAX {
        999_999_999
    } else {
        (nanos % 1_000_000_000) as u32
    };
    Duration::new(seconds, subsecond)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_rejects_shortcuts_and_increments_only_new_sessions() {
        let mut machine = LifecycleMachine::new(true);
        assert!(machine.transition(CameraLifecycleState::Degraded).is_err());
        assert_eq!(machine.transition(CameraLifecycleState::Online).unwrap(), 1);
        assert_eq!(
            machine.transition(CameraLifecycleState::Degraded).unwrap(),
            1
        );
        assert_eq!(machine.transition(CameraLifecycleState::Online).unwrap(), 1);
        machine.transition(CameraLifecycleState::Backoff).unwrap();
        machine
            .transition(CameraLifecycleState::Connecting)
            .unwrap();
        assert_eq!(machine.transition(CameraLifecycleState::Online).unwrap(), 2);
    }

    #[test]
    fn retry_delay_doubles_caps_and_is_stable_with_bounded_positive_jitter() {
        let policy = BackoffPolicy::new(Duration::from_secs(1), Duration::from_secs(60)).unwrap();
        let first = policy.delay("camera-a", 7, RetryClass::Transient, 0);
        assert!((Duration::from_secs(1)..=Duration::from_millis(1_200)).contains(&first));
        assert_eq!(first, policy.delay("camera-a", 7, RetryClass::Transient, 0));
        let capped = policy.delay("camera-a", 7, RetryClass::Transient, 31);
        assert!((Duration::from_secs(60)..=Duration::from_secs(72)).contains(&capped));
        let permanent = policy.delay("camera-a", 7, RetryClass::Permanent, 0);
        assert!((Duration::from_secs(10)..=Duration::from_secs(12)).contains(&permanent));
        assert_ne!(first, policy.delay("camera-b", 7, RetryClass::Transient, 0));
    }

    #[test]
    fn lifecycle_policy_and_duration_helpers_cover_disabled_and_saturation_edges() {
        let mut disabled = LifecycleMachine::new(false);
        assert_eq!(disabled.state(), CameraLifecycleState::Disabled);
        assert_eq!(disabled.session_generation(), 0);
        disabled
            .transition(CameraLifecycleState::Connecting)
            .unwrap();
        assert_eq!(
            disabled.transition(CameraLifecycleState::Online).unwrap(),
            1
        );
        disabled.transition(CameraLifecycleState::Draining).unwrap();
        disabled.transition(CameraLifecycleState::Stopped).unwrap();
        assert!(!CameraLifecycleState::Stopped.can_transition_to(CameraLifecycleState::Stopped));
        assert!(!CameraLifecycleState::Draining.can_transition_to(CameraLifecycleState::Online));

        assert!(BackoffPolicy::new(Duration::ZERO, Duration::from_secs(1)).is_err());
        assert!(BackoffPolicy::new(Duration::from_secs(2), Duration::from_secs(1)).is_err());
        assert_eq!(
            saturating_double(Duration::from_secs(1), 3),
            Duration::from_secs(8)
        );
        assert_eq!(
            saturating_multiply(Duration::from_secs(2), 3),
            Duration::from_secs(6)
        );
        assert_eq!(duration_from_nanos(u128::MAX), Duration::MAX);
    }

    #[test]
    fn lifecycle_transition_policy_has_no_implicit_backdoors() {
        use CameraLifecycleState as State;

        let states = [
            State::Disabled,
            State::Connecting,
            State::Online,
            State::Degraded,
            State::Backoff,
            State::Draining,
            State::Stopped,
        ];
        let permitted = [
            (State::Disabled, State::Connecting),
            (State::Disabled, State::Stopped),
            (State::Connecting, State::Online),
            (State::Connecting, State::Backoff),
            (State::Connecting, State::Draining),
            (State::Online, State::Degraded),
            (State::Online, State::Backoff),
            (State::Online, State::Draining),
            (State::Degraded, State::Online),
            (State::Degraded, State::Backoff),
            (State::Degraded, State::Draining),
            (State::Backoff, State::Connecting),
            (State::Backoff, State::Draining),
            (State::Draining, State::Disabled),
            (State::Draining, State::Stopped),
        ];

        for current in states {
            for next in states {
                assert_eq!(
                    current.can_transition_to(next),
                    permitted.contains(&(current, next)),
                    "unexpected lifecycle edge {current:?} -> {next:?}",
                );
            }
        }

        let bounded = BackoffPolicy::new(Duration::from_secs(2), Duration::from_secs(15))
            .expect("valid capped policy");
        let permanent = bounded.delay("camera-a", 1, RetryClass::Permanent, 0);
        assert!((Duration::from_secs(15)..=Duration::from_secs(18)).contains(&permanent));
    }

    #[tokio::test]
    async fn panic_is_sanitized_while_typed_backend_errors_are_preserved() {
        install_sanitized_panic_hook();
        assert_eq!(isolate_backend_panic(async { Ok(7_u8) }).await.unwrap(), 7);
        let typed: Result<()> = isolate_backend_panic(async {
            Err(CameraError::Backend {
                backend: "sim",
                message: "offline".to_owned(),
            })
        })
        .await;
        assert!(matches!(
            typed,
            Err(CameraError::Backend { backend: "sim", .. })
        ));

        let panicked: Result<()> = isolate_backend_panic(async {
            panic!("simulated backend panic");
        })
        .await;
        let text = panicked.unwrap_err().to_string();
        assert!(!text.contains("must-not-escape"));
        assert!(text.contains("supervisor boundary"));
    }
}
