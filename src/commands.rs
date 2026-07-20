//! Closed request schemas and request-local validation for camera command verbs.
//!
//! Runtime validation (camera existence, enabled state, profile/capability lookup, and policy) is
//! deliberately separate. This module rejects malformed bodies before durable or physical work.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use unicode_general_category::{GeneralCategory, get_general_category};

use crate::{
    CameraError, ErrorCode, Result,
    idempotency::validate_request_id,
    model::{BackendKind, JobState, PtzVector},
};

/// Default bounded page size.
pub const DEFAULT_PAGE_LIMIT: u16 = 100;
/// Maximum bounded page size.
pub const MAX_PAGE_LIMIT: u16 = 1_000;
/// Maximum opaque cursor size in bytes.
pub const MAX_CURSOR_BYTES: usize = 4_096;

/// Parses one JSON body through a closed serde schema.
pub fn parse_closed<T: DeserializeOwned>(body: Value) -> Result<T> {
    serde_json::from_value(body).map_err(|error| {
        CameraError::rejected(
            ErrorCode::BadArgs,
            format!("request body does not match the closed schema: {error}"),
        )
    })
}

/// `sb/list` request.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListRequest {
    /// Include immutable current capability snapshots.
    #[serde(default)]
    pub include_capabilities: bool,
    /// Include bounded unconfigured discovery observations when policy permits.
    #[serde(default)]
    pub include_unconfigured: bool,
    /// Page size.
    #[serde(default = "default_page_limit")]
    pub limit: u16,
    /// Query-bound continuation cursor.
    #[serde(default)]
    pub cursor: Option<String>,
}

impl ListRequest {
    /// Validates page bounds.
    pub fn validate(&self) -> Result<()> {
        validate_page(self.limit, self.cursor.as_deref())
    }
}

/// `sb/discover` request.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiscoverRequest {
    /// Discovery-capable compiled backends; omitted means all compiled discovery backends.
    #[serde(default)]
    pub backends: Vec<BackendKind>,
    /// Discovery deadline for a new snapshot.
    #[serde(default = "default_discovery_timeout_ms")]
    pub timeout_ms: u64,
    /// Page size.
    #[serde(default = "default_page_limit")]
    pub limit: u16,
    /// Query-bound continuation cursor.
    #[serde(default)]
    pub cursor: Option<String>,
}

impl DiscoverRequest {
    /// Validates backend uniqueness, timeout, and page bounds.
    pub fn validate(&self) -> Result<()> {
        validate_page(self.limit, self.cursor.as_deref())?;
        if !(100..=300_000).contains(&self.timeout_ms) {
            return invalid("timeoutMs must be in range 100..=300000");
        }
        let mut seen = HashSet::new();
        for backend in &self.backends {
            if *backend == BackendKind::Sim {
                return invalid("discovery backends must not include sim");
            }
            if !seen.insert(*backend) {
                return invalid("discovery backends must be distinct");
            }
        }
        Ok(())
    }
}

/// `sb/status` request.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatusRequest {
    /// Optional camera target; omission selects the component summary.
    pub instance: Option<String>,
}

impl StatusRequest {
    /// Validates the optional instance token.
    pub fn validate(&self) -> Result<()> {
        validate_optional_token(self.instance.as_deref(), "instance")
    }
}

/// Shared `sb/capture` and `sb/capture-submit` body.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaptureRequest {
    /// Optional camera target, resolved by the single-camera omission rule.
    pub instance: Option<String>,
    /// Caller-owned durable idempotency key.
    pub request_id: String,
    /// Optional named profile; defaults are resolved only for a new ledger key.
    pub capture_profile: Option<String>,
    /// Optional terminal deadline.
    pub timeout_ms: Option<u64>,
    /// Bounded opaque caller metadata.
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

impl CaptureRequest {
    /// Validates request-local fields and encoded metadata size.
    pub fn validate(&self, max_metadata_bytes: usize) -> Result<()> {
        validate_optional_token(self.instance.as_deref(), "instance")?;
        validate_request_id(&self.request_id)?;
        validate_optional_token(self.capture_profile.as_deref(), "captureProfile")?;
        validate_capture_timeout(self.timeout_ms)?;
        validate_metadata(&self.metadata, max_metadata_bytes)
    }
}

/// Shared `sb/capture-group` and `sb/capture-group-submit` body.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GroupCaptureRequest {
    /// Caller-owned component-scoped durable idempotency key.
    pub request_id: String,
    /// Original result-order camera list.
    pub instances: Vec<String>,
    /// Optional common profile.
    pub capture_profile: Option<String>,
    /// Per-camera profile overrides.
    #[serde(default)]
    pub profile_overrides: BTreeMap<String, String>,
    /// Optional per-member terminal deadline.
    pub timeout_ms: Option<u64>,
    /// Bounded opaque caller metadata.
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

impl GroupCaptureRequest {
    /// Validates group bounds, uniqueness, override subset, and request-local limits.
    pub fn validate(&self, max_group: usize, max_metadata_bytes: usize) -> Result<()> {
        validate_request_id(&self.request_id)?;
        if self.instances.len() < 2 {
            return invalid("instances must contain at least two cameras");
        }
        if self.instances.len() > max_group {
            return Err(CameraError::rejected(
                ErrorCode::GroupTooLarge,
                "instances exceeds limits.maxCamerasPerGroup",
            ));
        }
        let mut seen = HashSet::with_capacity(self.instances.len());
        for instance in &self.instances {
            validate_token(instance, "instances")?;
            if !seen.insert(instance.as_str()) {
                return invalid("instances must not contain duplicates");
            }
        }
        validate_optional_token(self.capture_profile.as_deref(), "captureProfile")?;
        for (instance, profile) in &self.profile_overrides {
            if !seen.contains(instance.as_str()) {
                return invalid("profileOverrides keys must be a subset of instances");
            }
            validate_token(profile, "profileOverrides")?;
        }
        validate_capture_timeout(self.timeout_ms)?;
        validate_metadata(&self.metadata, max_metadata_bytes)
    }
}

/// `sb/capture-status` request with one validated lookup mode.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaptureStatusRequest {
    /// Exact capture lookup.
    pub capture_id: Option<String>,
    /// Exact capture-group lookup.
    pub capture_group_id: Option<String>,
    /// Camera filter or camera-scoped request lookup.
    pub instance: Option<String>,
    /// Camera-scoped request key, or group key when instance is absent.
    pub request_id: Option<String>,
    /// List-mode state filters.
    #[serde(default)]
    pub states: Vec<JobState>,
    /// Page size for group/list modes.
    #[serde(default = "default_page_limit")]
    pub limit: u16,
    /// Query-bound continuation cursor.
    pub cursor: Option<String>,
}

/// Validated status lookup discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureStatusMode {
    /// One job by capture ID.
    Capture,
    /// One group and its paged members.
    Group,
    /// One camera job by `(instance, requestId)`.
    CameraRequest,
    /// One group by its component-scoped request ID.
    GroupRequest,
    /// Paged jobs by distinct public states, optionally restricted to a camera.
    List,
}

impl CaptureStatusRequest {
    /// Validates mutually exclusive fields and returns the selected lookup mode.
    pub fn validate(&self) -> Result<CaptureStatusMode> {
        validate_page(self.limit, self.cursor.as_deref())?;
        validate_optional_token(self.instance.as_deref(), "instance")?;
        if let Some(request_id) = &self.request_id {
            validate_request_id(request_id)?;
        }
        validate_opaque_id(self.capture_id.as_deref(), "captureId")?;
        validate_opaque_id(self.capture_group_id.as_deref(), "captureGroupId")?;
        let distinct_states: HashSet<JobState> = self.states.iter().copied().collect();
        if distinct_states.len() != self.states.len() {
            return invalid("states must contain distinct values");
        }

        let no_page_override = self.limit == DEFAULT_PAGE_LIMIT && self.cursor.is_none();
        match (
            self.capture_id.is_some(),
            self.capture_group_id.is_some(),
            self.instance.is_some(),
            self.request_id.is_some(),
            self.states.is_empty(),
        ) {
            (true, false, false, false, true) if no_page_override => Ok(CaptureStatusMode::Capture),
            (false, true, false, false, true) => Ok(CaptureStatusMode::Group),
            (false, false, true, true, true) if no_page_override => {
                Ok(CaptureStatusMode::CameraRequest)
            }
            (false, false, false, true, true) if no_page_override => {
                Ok(CaptureStatusMode::GroupRequest)
            }
            (false, false, _, false, false) => Ok(CaptureStatusMode::List),
            _ => invalid("capture-status requires exactly one documented lookup mode"),
        }
    }
}

/// `sb/capture-cancel` request.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CancelRequest {
    /// Durable component-scoped cancellation key.
    pub request_id: String,
    /// Single capture target.
    pub capture_id: Option<String>,
    /// Capture-group target.
    pub capture_group_id: Option<String>,
    /// Optional operator-safe reason.
    pub reason: Option<String>,
}

impl CancelRequest {
    /// Validates exactly one target and bounded reason text.
    pub fn validate(&self) -> Result<()> {
        validate_request_id(&self.request_id)?;
        match (&self.capture_id, &self.capture_group_id) {
            (Some(capture), None) => validate_opaque_id(Some(capture), "captureId")?,
            (None, Some(group)) => validate_opaque_id(Some(group), "captureGroupId")?,
            _ => return invalid("exactly one of captureId or captureGroupId is required"),
        }
        validate_reason(self.reason.as_deref())
    }
}

/// `sb/queue-status` request.
///
/// Read-only, so it carries no `requestId`: there is nothing to make idempotent.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueueStatusRequest {
    /// Optional camera target. Absent means the whole fleet.
    pub instance: Option<String>,
}

impl QueueStatusRequest {
    /// Validates the optional camera token.
    pub fn validate(&self) -> Result<()> {
        validate_optional_token(self.instance.as_deref(), "instance")
    }
}

/// `sb/queue-clear` request -- the break-glass drain.
///
/// This cancels durable work an operator has already been promised, so it is deliberately harder to
/// fire by accident than the read-only sibling above: it is ledgered on `requestId` like every other
/// mutating verb, and it will not run fleet-wide unless the caller says so in as many words.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueueClearRequest {
    /// Durable component-scoped operation key.
    pub request_id: String,
    /// Optional camera target. Absent clears every camera, and then `allCameras` must be true.
    pub instance: Option<String>,
    /// Explicit fleet-wide consent. Required when no `instance` is given.
    #[serde(default)]
    pub all_cameras: bool,
    /// Whether to cancel captures that have already started, not only the backlog.
    ///
    /// Default false: draining the backlog is the common emergency and it destroys nothing that has
    /// begun. Cancelling in-flight work as well is a strictly bigger hammer and must be asked for.
    #[serde(default)]
    pub include_in_flight: bool,
    /// Optional operator-safe reason, recorded on every cancelled capture.
    pub reason: Option<String>,
}

impl QueueClearRequest {
    /// Validates the key, the target, and the fleet-wide guard.
    pub fn validate(&self) -> Result<()> {
        validate_request_id(&self.request_id)?;
        validate_optional_token(self.instance.as_deref(), "instance")?;
        if self.instance.is_none() && !self.all_cameras {
            return invalid(
                "clearing every camera's queue requires allCameras=true; name an instance to clear one camera",
            );
        }
        if self.instance.is_some() && self.all_cameras {
            return invalid("allCameras must not be set when an instance is named");
        }
        validate_reason(self.reason.as_deref())
    }
}

/// `sb/reconnect` request.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReconnectRequest {
    /// Optional camera target, resolved by the single-camera omission rule.
    pub instance: Option<String>,
    /// Durable camera-scoped operation key.
    pub request_id: String,
    /// Optional operator-safe reason.
    pub reason: Option<String>,
}

impl ReconnectRequest {
    /// Validates target, durable key, and reason.
    pub fn validate(&self) -> Result<()> {
        validate_optional_token(self.instance.as_deref(), "instance")?;
        validate_request_id(&self.request_id)?;
        validate_reason(self.reason.as_deref())
    }
}

/// `sb/pause` / `sb/resume` request.
///
/// The lifecycle verbs are idempotent in-memory toggles, so they carry no durable `requestId`: there
/// is nothing to make exactly-once. Only the optional instance target, resolved by the single-camera
/// omission rule.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PauseResumeRequest {
    /// Optional camera target; omission selects the sole configured camera.
    pub instance: Option<String>,
}

impl PauseResumeRequest {
    /// Validates the optional instance token.
    pub fn validate(&self) -> Result<()> {
        validate_optional_token(self.instance.as_deref(), "instance")
    }
}

/// Normalized non-negative PTZ speed.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PtzSpeed {
    /// Pan speed in `[0, 1]`.
    pub pan: f64,
    /// Tilt speed in `[0, 1]`.
    pub tilt: f64,
    /// Zoom speed in `[0, 1]`.
    pub zoom: f64,
}

impl PtzSpeed {
    fn validate(self) -> Result<()> {
        if [self.pan, self.tilt, self.zoom]
            .into_iter()
            .all(|value| value.is_finite() && (0.0..=1.0).contains(&value))
        {
            Ok(())
        } else {
            Err(CameraError::rejected(
                ErrorCode::PtzRangeError,
                "PTZ speed values must be finite and in range 0..=1",
            ))
        }
    }
}

/// PTZ stop axes.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum PtzAxis {
    /// Pan axis.
    Pan,
    /// Tilt axis.
    Tilt,
    /// Zoom axis.
    Zoom,
}

/// Closed `sb/ptz` operation variants.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(
    tag = "operation",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum PtzCommandRequest {
    /// Start continuous motion with a mandatory server-side stop.
    Continuous {
        /// Optional camera target.
        instance: Option<String>,
        /// Durable operation key.
        request_id: String,
        /// Signed normalized velocity.
        velocity: PtzVector,
        /// Mandatory stop deadline relative to acceptance.
        timeout_ms: u64,
    },
    /// Move to an absolute normalized position.
    Absolute {
        /// Optional camera target.
        instance: Option<String>,
        /// Durable operation key.
        request_id: String,
        /// Target absolute position.
        position: PtzVector,
        /// Optional non-negative normalized speed.
        speed: Option<PtzSpeed>,
    },
    /// Move by a signed normalized translation.
    Relative {
        /// Optional camera target.
        instance: Option<String>,
        /// Durable operation key.
        request_id: String,
        /// Signed translation.
        translation: PtzVector,
        /// Optional non-negative normalized speed.
        speed: Option<PtzSpeed>,
    },
    /// Stop one or more distinct axes.
    Stop {
        /// Optional camera target.
        instance: Option<String>,
        /// Durable operation key.
        request_id: String,
        /// Axes to stop.
        axes: Vec<PtzAxis>,
    },
    /// Return to home position.
    Home {
        /// Optional camera target.
        instance: Option<String>,
        /// Durable operation key.
        request_id: String,
    },
    /// Read status without durable actuation.
    Status {
        /// Optional camera target.
        instance: Option<String>,
    },
}

impl PtzCommandRequest {
    /// Validates common identifiers and operation-specific normalized ranges.
    pub fn validate(&self, max_continuous_move_ms: u64) -> Result<()> {
        let (instance, request_id) = match self {
            Self::Continuous {
                instance,
                request_id,
                velocity,
                timeout_ms,
            } => {
                if !velocity.validate_signed() {
                    return ptz_range("continuous velocity must be finite and in range -1..=1");
                }
                if *timeout_ms == 0 || *timeout_ms > max_continuous_move_ms {
                    return ptz_range("continuous timeoutMs exceeds the configured safety bound");
                }
                (instance, Some(request_id))
            }
            Self::Absolute {
                instance,
                request_id,
                position,
                speed,
            } => {
                if !position.validate_absolute() {
                    return ptz_range("absolute pan/tilt must be in -1..=1 and zoom in 0..=1");
                }
                if let Some(speed) = speed {
                    speed.validate()?;
                }
                (instance, Some(request_id))
            }
            Self::Relative {
                instance,
                request_id,
                translation,
                speed,
            } => {
                if !translation.validate_signed() {
                    return ptz_range("relative translation must be finite and in range -1..=1");
                }
                if let Some(speed) = speed {
                    speed.validate()?;
                }
                (instance, Some(request_id))
            }
            Self::Stop {
                instance,
                request_id,
                axes,
            } => {
                if axes.is_empty() || axes.len() > 3 {
                    return invalid("stop axes must contain one to three values");
                }
                if axes.iter().copied().collect::<HashSet<_>>().len() != axes.len() {
                    return invalid("stop axes must be distinct");
                }
                (instance, Some(request_id))
            }
            Self::Home {
                instance,
                request_id,
            } => (instance, Some(request_id)),
            Self::Status { instance } => (instance, None),
        };
        validate_optional_token(instance.as_deref(), "instance")?;
        if let Some(request_id) = request_id {
            validate_request_id(request_id)?;
        }
        Ok(())
    }
}

/// Closed `sb/ptz-presets` operation variants.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "operation",
    rename_all = "lowercase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum PtzPresetsRequest {
    /// List camera-issued preset tokens.
    List {
        /// Optional camera target.
        instance: Option<String>,
        /// Page size.
        #[serde(default = "default_page_limit")]
        limit: u16,
        /// Query-bound continuation cursor.
        cursor: Option<String>,
    },
    /// Recall a preset.
    Goto {
        /// Optional camera target.
        instance: Option<String>,
        /// Durable operation key.
        request_id: String,
        /// Opaque camera-issued token.
        token: String,
    },
    /// Create or replace a named preset.
    Set {
        /// Optional camera target.
        instance: Option<String>,
        /// Durable operation key.
        request_id: String,
        /// Human-readable preset name.
        name: String,
    },
    /// Remove a preset.
    Remove {
        /// Optional camera target.
        instance: Option<String>,
        /// Durable operation key.
        request_id: String,
        /// Opaque camera-issued token.
        token: String,
    },
}

impl PtzPresetsRequest {
    /// Validates target, pagination, durable keys, and bounded opaque strings.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::List {
                instance,
                limit,
                cursor,
            } => {
                validate_optional_token(instance.as_deref(), "instance")?;
                validate_page(*limit, cursor.as_deref())
            }
            Self::Goto {
                instance,
                request_id,
                token,
            }
            | Self::Remove {
                instance,
                request_id,
                token,
            } => {
                validate_optional_token(instance.as_deref(), "instance")?;
                validate_request_id(request_id)?;
                validate_opaque_text(token, "token", 1, 1_024)
            }
            Self::Set {
                instance,
                request_id,
                name,
            } => {
                validate_optional_token(instance.as_deref(), "instance")?;
                validate_request_id(request_id)?;
                validate_opaque_text(name, "name", 1, 256)
            }
        }
    }
}

fn default_page_limit() -> u16 {
    DEFAULT_PAGE_LIMIT
}

fn default_discovery_timeout_ms() -> u64 {
    5_000
}

fn validate_page(limit: u16, cursor: Option<&str>) -> Result<()> {
    if !(1..=MAX_PAGE_LIMIT).contains(&limit) {
        return invalid("limit must be in range 1..=1000");
    }
    if cursor.is_some_and(|value| value.is_empty() || value.len() > MAX_CURSOR_BYTES) {
        return invalid("cursor must contain 1 to 4096 UTF-8 bytes");
    }
    Ok(())
}

fn validate_capture_timeout(timeout_ms: Option<u64>) -> Result<()> {
    if timeout_ms.is_some_and(|value| !(1_000..=1_800_000).contains(&value)) {
        return invalid("timeoutMs must be in range 1000..=1800000");
    }
    Ok(())
}

fn validate_metadata(metadata: &Map<String, Value>, maximum: usize) -> Result<()> {
    let encoded = serde_json::to_vec(metadata)?;
    if encoded.len() > maximum {
        return invalid("metadata exceeds limits.maxMetadataBytes");
    }
    Ok(())
}

fn validate_reason(reason: Option<&str>) -> Result<()> {
    if let Some(reason) = reason {
        validate_opaque_text(reason, "reason", 0, 1_024)?;
    }
    Ok(())
}

fn validate_opaque_id(value: Option<&str>, field: &str) -> Result<()> {
    if let Some(value) = value {
        validate_opaque_text(value, field, 1, 256)?;
    }
    Ok(())
}

fn validate_opaque_text(value: &str, field: &str, minimum: usize, maximum: usize) -> Result<()> {
    if value.len() < minimum || value.len() > maximum {
        return invalid(format!(
            "{field} must contain {minimum} to {maximum} UTF-8 bytes"
        ));
    }
    if value.chars().any(|character| {
        matches!(
            get_general_category(character),
            GeneralCategory::Control | GeneralCategory::Format
        )
    }) {
        return invalid(format!(
            "{field} must not contain control or format characters"
        ));
    }
    Ok(())
}

fn validate_optional_token(value: Option<&str>, field: &str) -> Result<()> {
    if let Some(value) = value {
        validate_token(value, field)?;
    }
    Ok(())
}

fn validate_token(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')))
    {
        return invalid(format!(
            "{field} must be a non-empty UNS token using ASCII letters, digits, '.', '_', or '-'"
        ));
    }
    Ok(())
}

fn ptz_range<T>(message: impl Into<String>) -> Result<T> {
    Err(CameraError::rejected(
        ErrorCode::PtzRangeError,
        message.into(),
    ))
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(CameraError::rejected(
        ErrorCode::BadArgs,
        message.into(),
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn rejection_code<T>(result: Result<T>) -> ErrorCode {
        match result {
            Err(CameraError::Rejected { code, .. }) => code,
            Err(other) => panic!("expected command rejection, got {other:?}"),
            Ok(_) => panic!("request must be rejected"),
        }
    }

    #[test]
    fn closed_schema_rejects_unknown_fields() {
        assert!(parse_closed::<ListRequest>(json!({"surprise": true})).is_err());
    }

    #[test]
    fn list_defaults_are_exact_and_cursor_is_bounded() {
        let request: ListRequest = parse_closed(json!({})).unwrap();
        assert_eq!(request.limit, 100);
        assert!(!request.include_capabilities);
        assert!(request.validate().is_ok());
        let oversized: ListRequest = parse_closed(json!({"cursor": "x".repeat(4097)})).unwrap();
        assert!(oversized.validate().is_err());
    }

    #[test]
    fn discovery_rejects_sim_and_duplicates() {
        let simulated: DiscoverRequest = parse_closed(json!({"backends": ["sim"]})).unwrap();
        assert!(simulated.validate().is_err());
        let duplicate: DiscoverRequest =
            parse_closed(json!({"backends": ["onvif-rtsp", "onvif-rtsp"]})).unwrap();
        assert!(duplicate.validate().is_err());
    }

    #[test]
    fn capture_metadata_uses_encoded_byte_bound() {
        let request: CaptureRequest = parse_closed(json!({
            "requestId": "order-1",
            "metadata": {"unicode": "éé"}
        }))
        .unwrap();
        assert!(request.validate(64).is_ok());
        assert!(request.validate(8).is_err());
    }

    #[test]
    fn group_validation_preserves_order_but_rejects_duplicates_and_bad_overrides() {
        let valid: GroupCaptureRequest = parse_closed(json!({
            "requestId": "group-1",
            "instances": ["camera-z", "camera-a"],
            "profileOverrides": {"camera-a": "detail"}
        }))
        .unwrap();
        assert!(valid.validate(8, 8192).is_ok());
        assert_eq!(valid.instances, ["camera-z", "camera-a"]);

        let duplicate: GroupCaptureRequest = parse_closed(json!({
            "requestId": "group-1",
            "instances": ["camera-a", "camera-a"]
        }))
        .unwrap();
        assert!(duplicate.validate(8, 8192).is_err());
        let bad_override: GroupCaptureRequest = parse_closed(json!({
            "requestId": "group-1",
            "instances": ["camera-a", "camera-b"],
            "profileOverrides": {"camera-c": "detail"}
        }))
        .unwrap();
        assert!(bad_override.validate(8, 8192).is_err());
    }

    #[test]
    fn status_lookup_modes_are_mutually_exclusive() {
        let capture: CaptureStatusRequest = parse_closed(json!({"captureId": "cap_1"})).unwrap();
        assert_eq!(capture.validate().unwrap(), CaptureStatusMode::Capture);
        let group_request: CaptureStatusRequest =
            parse_closed(json!({"requestId": "group-1"})).unwrap();
        assert_eq!(
            group_request.validate().unwrap(),
            CaptureStatusMode::GroupRequest
        );
        let list: CaptureStatusRequest =
            parse_closed(json!({"states": ["FAILED"], "limit": 5})).unwrap();
        assert_eq!(list.validate().unwrap(), CaptureStatusMode::List);
        let ambiguous: CaptureStatusRequest = parse_closed(json!({
            "captureId": "cap_1",
            "requestId": "also"
        }))
        .unwrap();
        assert!(ambiguous.validate().is_err());
    }

    #[test]
    fn cancel_requires_exactly_one_target_and_safe_reason() {
        let valid: CancelRequest = parse_closed(json!({
            "requestId": "cancel-1",
            "captureId": "cap_1",
            "reason": "operator request"
        }))
        .unwrap();
        assert!(valid.validate().is_ok());
        let both: CancelRequest = parse_closed(json!({
            "requestId": "cancel-1",
            "captureId": "cap_1",
            "captureGroupId": "grp_1"
        }))
        .unwrap();
        assert!(both.validate().is_err());
        let control: CancelRequest = parse_closed(json!({
            "requestId": "cancel-1",
            "captureId": "cap_1",
            "reason": "bad\nreason"
        }))
        .unwrap();
        assert!(control.validate().is_err());
    }

    #[test]
    fn ptz_variants_enforce_request_ids_ranges_and_distinct_stop_axes() {
        let move_request: PtzCommandRequest = parse_closed(json!({
            "operation": "continuous",
            "instance": "yard",
            "requestId": "move-1",
            "velocity": {"pan": 0.5, "tilt": -0.25, "zoom": 0.0},
            "timeoutMs": 1500
        }))
        .unwrap();
        assert!(move_request.validate(2_000).is_ok());
        assert!(move_request.validate(1_000).is_err());

        let duplicate_axes: PtzCommandRequest = parse_closed(json!({
            "operation": "stop",
            "requestId": "stop-1",
            "axes": ["pan", "pan"]
        }))
        .unwrap();
        assert!(duplicate_axes.validate(2_000).is_err());

        let status: PtzCommandRequest = parse_closed(json!({"operation": "status"})).unwrap();
        assert!(status.validate(2_000).is_ok());
    }

    #[test]
    fn preset_mutations_require_request_id_but_list_does_not() {
        let list: PtzPresetsRequest = parse_closed(json!({"operation": "list"})).unwrap();
        assert!(list.validate().is_ok());
        assert!(
            parse_closed::<PtzPresetsRequest>(json!({
                "operation": "goto",
                "token": "gate"
            }))
            .is_err()
        );
    }

    #[test]
    fn request_field_bounds_fail_with_the_public_invalid_request_code() {
        let list_cases = [
            json!({"limit": 0}),
            json!({"limit": 1001}),
            json!({"cursor": ""}),
            json!({"cursor": "x".repeat(MAX_CURSOR_BYTES + 1)}),
        ];
        for body in list_cases {
            let request: ListRequest = parse_closed(body).unwrap();
            assert_eq!(
                rejection_code(request.validate()),
                ErrorCode::BadArgs
            );
        }

        let discovery_cases = [
            json!({"timeoutMs": 99}),
            json!({"timeoutMs": 300001}),
            json!({"backends": ["genicam-aravis", "genicam-aravis"]}),
        ];
        for body in discovery_cases {
            let request: DiscoverRequest = parse_closed(body).unwrap();
            assert_eq!(
                rejection_code(request.validate()),
                ErrorCode::BadArgs
            );
        }

        for body in [
            json!({"instance": "invalid/token"}),
            json!({"instance": "x".repeat(129)}),
        ] {
            let request: StatusRequest = parse_closed(body).unwrap();
            assert_eq!(
                rejection_code(request.validate()),
                ErrorCode::BadArgs
            );
        }

        for body in [
            json!({"requestId": "capture-1", "timeoutMs": 999}),
            json!({"requestId": "capture-1", "timeoutMs": 1_800_001}),
            json!({"requestId": "capture-1", "captureProfile": "bad/profile"}),
        ] {
            let request: CaptureRequest = parse_closed(body).unwrap();
            assert_eq!(
                rejection_code(request.validate(8 * 1024)),
                ErrorCode::BadArgs
            );
        }
    }

    #[test]
    fn group_and_status_modes_reject_ambiguous_or_out_of_contract_lookups() {
        let too_small: GroupCaptureRequest = parse_closed(json!({
            "requestId": "group-1",
            "instances": ["camera-a"]
        }))
        .unwrap();
        assert_eq!(
            rejection_code(too_small.validate(8, 8 * 1024)),
            ErrorCode::BadArgs
        );

        let too_large: GroupCaptureRequest = parse_closed(json!({
            "requestId": "group-1",
            "instances": ["camera-a", "camera-b", "camera-c"]
        }))
        .unwrap();
        assert_eq!(
            rejection_code(too_large.validate(2, 8 * 1024)),
            ErrorCode::GroupTooLarge
        );

        let valid_group: CaptureStatusRequest =
            parse_closed(json!({"captureGroupId": "group_1", "limit": 5})).unwrap();
        assert_eq!(valid_group.validate().unwrap(), CaptureStatusMode::Group);
        let camera_request: CaptureStatusRequest =
            parse_closed(json!({"instance": "camera-a", "requestId": "request-1"})).unwrap();
        assert_eq!(
            camera_request.validate().unwrap(),
            CaptureStatusMode::CameraRequest
        );

        for body in [
            json!({"captureId": "cap-1", "limit": 2}),
            json!({"instance": "camera-a"}),
            json!({"states": ["FAILED", "FAILED"]}),
            json!({"captureGroupId": "bad\nidentifier"}),
        ] {
            let request: CaptureStatusRequest = parse_closed(body).unwrap();
            assert_eq!(
                rejection_code(request.validate()),
                ErrorCode::BadArgs
            );
        }
    }

    #[test]
    fn every_ptz_variant_applies_its_documented_safety_boundary() {
        let absolute: PtzCommandRequest = parse_closed(json!({
            "operation": "absolute",
            "requestId": "absolute-1",
            "position": {"pan": 1.0, "tilt": -1.0, "zoom": 1.0},
            "speed": {"pan": 1.0, "tilt": 0.0, "zoom": 0.5}
        }))
        .unwrap();
        assert!(absolute.validate(2_000).is_ok());

        let absolute_bad_zoom: PtzCommandRequest = parse_closed(json!({
            "operation": "absolute",
            "requestId": "absolute-2",
            "position": {"pan": 0.0, "tilt": 0.0, "zoom": -0.1}
        }))
        .unwrap();
        assert_eq!(
            rejection_code(absolute_bad_zoom.validate(2_000)),
            ErrorCode::PtzRangeError
        );

        let relative_bad_speed = PtzCommandRequest::Relative {
            instance: None,
            request_id: "relative-1".to_string(),
            translation: PtzVector {
                pan: 0.0,
                tilt: 0.0,
                zoom: 0.0,
            },
            speed: Some(PtzSpeed {
                pan: f64::NAN,
                tilt: 0.0,
                zoom: 0.0,
            }),
        };
        assert_eq!(
            rejection_code(relative_bad_speed.validate(2_000)),
            ErrorCode::PtzRangeError
        );

        let empty_stop: PtzCommandRequest = parse_closed(json!({
            "operation": "stop", "requestId": "stop-1", "axes": []
        }))
        .unwrap();
        assert_eq!(
            rejection_code(empty_stop.validate(2_000)),
            ErrorCode::BadArgs
        );
        let home: PtzCommandRequest = parse_closed(json!({
            "operation": "home", "requestId": "home-1", "instance": "camera-a"
        }))
        .unwrap();
        assert!(home.validate(2_000).is_ok());
        let bad_status: PtzCommandRequest =
            parse_closed(json!({"operation": "status", "instance": "bad/token"})).unwrap();
        assert_eq!(
            rejection_code(bad_status.validate(2_000)),
            ErrorCode::BadArgs
        );
    }

    #[test]
    fn preset_operations_validate_pagination_tokens_and_names() {
        let list: PtzPresetsRequest =
            parse_closed(json!({"operation": "list", "limit": 0})).unwrap();
        assert_eq!(rejection_code(list.validate()), ErrorCode::BadArgs);

        for body in [
            json!({"operation": "goto", "instance": "camera-a", "requestId": "goto-1", "token": ""}),
            json!({"operation": "remove", "requestId": "remove-1", "token": "bad\u{200b}"}),
            json!({"operation": "set", "requestId": "set-1", "name": ""}),
        ] {
            let request: PtzPresetsRequest = parse_closed(body).unwrap();
            assert_eq!(
                rejection_code(request.validate()),
                ErrorCode::BadArgs
            );
        }

        for body in [
            json!({"operation": "goto", "requestId": "goto-2", "token": "gate"}),
            json!({"operation": "remove", "requestId": "remove-2", "token": "gate"}),
            json!({"operation": "set", "requestId": "set-2", "name": "north gate"}),
        ] {
            let request: PtzPresetsRequest = parse_closed(body).unwrap();
            assert!(request.validate().is_ok());
        }
    }

    /// The break-glass drain must be hard to fire by accident.
    ///
    /// `sb/queue-clear` cancels durable work the operator has already been promised. A body that
    /// merely omits `instance` would otherwise mean "cancel everything, everywhere", which is not a
    /// thing anyone should be able to do by leaving a field out.
    #[test]
    fn queue_clear_will_not_drain_the_fleet_unless_asked_in_as_many_words() {
        let one_camera: QueueClearRequest =
            parse_closed(serde_json::json!({"requestId": "r-1", "instance": "camera-a"})).unwrap();
        assert!(one_camera.validate().is_ok());
        assert!(
            !one_camera.include_in_flight,
            "draining the backlog must not also destroy captures that have already started"
        );

        let fleet: QueueClearRequest =
            parse_closed(serde_json::json!({"requestId": "r-2", "allCameras": true})).unwrap();
        assert!(fleet.validate().is_ok());

        let bare: QueueClearRequest =
            parse_closed(serde_json::json!({"requestId": "r-3"})).unwrap();
        assert_eq!(
            bare.validate().unwrap_err().code(),
            ErrorCode::BadArgs,
            "omitting the camera must not silently mean the whole fleet"
        );

        let contradictory: QueueClearRequest = parse_closed(
            serde_json::json!({"requestId": "r-4", "instance": "camera-a", "allCameras": true}),
        )
        .unwrap();
        assert_eq!(
            contradictory.validate().unwrap_err().code(),
            ErrorCode::BadArgs,
            "naming a camera and asking for the whole fleet is a contradiction, not a preference"
        );
    }

    /// The lifecycle verbs take an optional camera and nothing else -- a closed, `requestId`-free body.
    #[test]
    fn pause_resume_takes_an_optional_camera_and_rejects_anything_else() {
        let fleet: PauseResumeRequest = parse_closed(json!({})).unwrap();
        assert!(fleet.validate().is_ok());
        assert_eq!(fleet.instance, None);

        let one: PauseResumeRequest = parse_closed(json!({"instance": "camera-a"})).unwrap();
        assert!(one.validate().is_ok());

        let bad_token: PauseResumeRequest = parse_closed(json!({"instance": "bad/token"})).unwrap();
        assert_eq!(rejection_code(bad_token.validate()), ErrorCode::BadArgs);

        assert!(
            parse_closed::<PauseResumeRequest>(json!({"requestId": "r-1"})).is_err(),
            "the schema is closed: a field that does nothing must be rejected, not ignored"
        );
    }

    /// The read-only sibling carries no requestId, because there is nothing to make idempotent.
    #[test]
    fn queue_status_takes_an_optional_camera_and_nothing_else() {
        let fleet: QueueStatusRequest = parse_closed(serde_json::json!({})).unwrap();
        assert!(fleet.validate().is_ok());
        assert_eq!(fleet.instance, None);

        let one: QueueStatusRequest =
            parse_closed(serde_json::json!({"instance": "camera-a"})).unwrap();
        assert!(one.validate().is_ok());

        assert!(
            parse_closed::<QueueStatusRequest>(serde_json::json!({"requestId": "r-1"})).is_err(),
            "the schema is closed: a field that does nothing must be rejected, not ignored"
        );
    }
}
