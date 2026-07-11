//! Schema-v1 terminal application-message bodies and exact routing metadata.
//!
//! The durable outbox owns the fully encoded EdgeCommons envelope. This module owns the domain
//! body and the invariant that its convenience `correlationId` equals the envelope correlation.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use edgecommons::facades::{AppFacade, PreparedAppMessage};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    CameraError, ErrorCode, Result,
    model::{BackendKind, CaptureMode, FrameTimestampQuality, OutputEncoding, PixelFormat},
};

/// Current terminal-body schema version.
pub const TERMINAL_SCHEMA_VERSION: u8 = 1;
/// EdgeCommons application-envelope version used for terminal messages.
pub const TERMINAL_ENVELOPE_VERSION: &str = "1.0";

/// Terminal application-message kind and its exact routing contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalKind {
    /// Capture and file persistence succeeded.
    Captured,
    /// Capture failed or restart interrupted it.
    Failed,
    /// Cancellation won terminal arbitration.
    Cancelled,
}

impl TerminalKind {
    /// Exact application envelope header name.
    #[must_use]
    pub const fn header_name(self) -> &'static str {
        match self {
            Self::Captured => "ImageCaptured",
            Self::Failed => "ImageCaptureFailed",
            Self::Cancelled => "ImageCaptureCancelled",
        }
    }

    /// Exact application topic channel beneath `app/`.
    #[must_use]
    pub const fn channel(self) -> &'static str {
        match self {
            Self::Captured => "image/captured",
            Self::Failed => "image/failed",
            Self::Cancelled => "image/cancelled",
        }
    }
}

/// Durable capture origin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum CaptureTrigger {
    /// Direct or submitted camera command.
    Command {
        /// Caller-owned idempotency key.
        request_id: String,
    },
    /// One member of a software fan-out group command.
    GroupCommand {
        /// Caller-owned group idempotency key.
        request_id: String,
        /// Adapter-generated group ID.
        capture_group_id: String,
    },
    /// One intended schedule occurrence.
    Schedule {
        /// Configured schedule token.
        schedule_id: String,
        /// Deduplication time before stable jitter is applied.
        intended_fire_time: DateTime<Utc>,
    },
}

/// Capture lifecycle timestamps. Unreached stages remain absent rather than fabricated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureTimestamps {
    /// Durable request/occurrence acceptance time.
    pub requested_at: DateTime<Utc>,
    /// Backend acquisition start.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquisition_started_at: Option<DateTime<Utc>>,
    /// Camera/stream-reported frame time, when defensible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub camera_frame_at: Option<DateTime<Utc>>,
    /// Adapter receipt time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame_received_at: Option<DateTime<Utc>>,
    /// Final artifact durable-install time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persisted_at: Option<DateTime<Utc>>,
    /// Provenance of `cameraFrameAt`.
    pub camera_frame_timestamp_quality: FrameTimestampQuality,
}

/// Stage durations in milliseconds. Unreached stages remain absent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureDurations {
    /// Admission/queue duration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<u64>,
    /// Backend acquisition duration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquisition: Option<u64>,
    /// Encoding duration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<u64>,
    /// Persistence duration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence: Option<u64>,
    /// Total durable-job duration.
    pub total: u64,
}

/// Successfully installed image metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageArtifact {
    /// Canonical absolute path for local operators.
    pub absolute_path: String,
    /// Root-relative path used by file-replicator.
    pub relative_path: String,
    /// Properly encoded local file URI.
    pub file_uri: String,
    /// MIME content type.
    pub content_type: String,
    /// Final output encoding.
    pub encoding: OutputEncoding,
    /// Exact installed byte count.
    pub bytes: u64,
    /// Lower-case SHA-256 digest.
    pub sha256: String,
    /// Installed metadata sidecar relative path, when enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_sidecar_relative_path: Option<String>,
}

/// Captured frame facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FrameSummary {
    /// Pixel width.
    pub width: u32,
    /// Pixel height.
    pub height: u32,
    /// Source pixel/file format.
    pub pixel_format: PixelFormat,
    /// Protocol-neutral source encoding label.
    pub source_encoding: String,
}

/// Sanitized camera identity captured with the result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CameraSummary {
    /// Backend used for this job.
    pub backend: BackendKind,
    /// Device vendor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vendor: Option<String>,
    /// Device model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Device firmware.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub firmware: Option<String>,
    /// Stable device serial.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial: Option<String>,
}

/// Public terminal-failure description.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureSummary {
    /// Stable public error code.
    #[serde(serialize_with = "serialize_error_code")]
    pub code: ErrorCode,
    /// Last active public stage, such as `ACQUIRING`.
    pub stage: String,
    /// Whether policy permits a caller to retry with a new idempotency key.
    pub retriable: bool,
    /// Sanitized operator-safe detail.
    pub message: String,
}

/// Complete schema-v1 terminal application body.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminalBody {
    /// Fixed value `1`.
    pub schema_version: u8,
    /// Stable event deduplication ID.
    pub event_id: String,
    /// Durable capture primary key.
    pub capture_id: String,
    /// Camera instance token.
    pub camera_id: String,
    /// Must equal the enclosing envelope correlation ID.
    pub correlation_id: String,
    /// Durable origin.
    pub trigger: CaptureTrigger,
    /// Immutable effective profile name.
    pub capture_profile: String,
    /// Actual acquisition mechanism.
    pub capture_mode: CaptureMode,
    /// Lifecycle timestamps.
    pub timestamps: CaptureTimestamps,
    /// Lifecycle durations.
    pub durations_ms: CaptureDurations,
    /// Installed image, present only on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<ImageArtifact>,
    /// Frame facts when a frame was received.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame: Option<FrameSummary>,
    /// Sanitized backend/device facts.
    pub camera: CameraSummary,
    /// Bounded caller metadata copied verbatim.
    pub metadata: Map<String, Value>,
    /// Failure facts, present only for `ImageCaptureFailed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureSummary>,
    /// Optional group identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_group_id: Option<String>,
    /// Original group size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_size: Option<usize>,
    /// Bounded backend result metadata without credentials or unsafe endpoints.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub backend_metadata: BTreeMap<String, Value>,
}

/// A validated terminal message before the EdgeCommons facade stamps its exact envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct TerminalMessage {
    kind: TerminalKind,
    body: TerminalBody,
}

impl TerminalMessage {
    /// Validates kind/body invariants and creates one terminal message.
    pub fn new(kind: TerminalKind, body: TerminalBody) -> Result<Self> {
        if body.schema_version != TERMINAL_SCHEMA_VERSION {
            return invalid("terminal schemaVersion must be 1");
        }
        for (field, value) in [
            ("eventId", body.event_id.as_str()),
            ("captureId", body.capture_id.as_str()),
            ("cameraId", body.camera_id.as_str()),
            ("correlationId", body.correlation_id.as_str()),
            ("captureProfile", body.capture_profile.as_str()),
        ] {
            if value.is_empty() {
                return invalid(format!("terminal {field} must be non-empty"));
            }
        }
        match kind {
            TerminalKind::Captured if body.image.is_none() || body.failure.is_some() => {
                return invalid("ImageCaptured requires image and forbids failure");
            }
            TerminalKind::Failed if body.image.is_some() || body.failure.is_none() => {
                return invalid("ImageCaptureFailed requires failure and forbids image");
            }
            TerminalKind::Cancelled if body.image.is_some() || body.failure.is_some() => {
                return invalid("ImageCaptureCancelled forbids image and failure");
            }
            _ => {}
        }
        match (&body.capture_group_id, body.group_size) {
            (None, None) => {}
            (Some(_), Some(size)) if size >= 2 => {}
            _ => return invalid("captureGroupId and groupSize >= 2 must appear together"),
        }
        if let CaptureTrigger::GroupCommand {
            capture_group_id, ..
        } = &body.trigger
        {
            if body.capture_group_id.as_deref() != Some(capture_group_id) {
                return invalid("group trigger and terminal captureGroupId must match");
            }
        }
        Ok(Self { kind, body })
    }

    /// Creates a fresh time-sortable event ID.
    #[must_use]
    pub fn new_event_id() -> String {
        format!("evt_{}", Uuid::now_v7().simple())
    }

    /// Creates a fresh correlation for schedule-originated terminal messages.
    #[must_use]
    pub fn new_scheduled_correlation() -> String {
        Uuid::now_v7().to_string()
    }

    /// Exact application header name.
    #[must_use]
    pub const fn header_name(&self) -> &'static str {
        self.kind.header_name()
    }

    /// Exact application channel beneath `app/`.
    #[must_use]
    pub const fn channel(&self) -> &'static str {
        self.kind.channel()
    }

    /// Correlation that must be supplied to `AppFacade::prepare_correlated`.
    #[must_use]
    pub fn correlation_id(&self) -> &str {
        &self.body.correlation_id
    }

    /// Domain body for the EdgeCommons application facade.
    pub fn body_value(&self) -> Result<Value> {
        serde_json::to_value(&self.body).map_err(CameraError::from)
    }

    /// Prepares one identity-stamped exact envelope through the guarded EdgeCommons app facade.
    ///
    /// The caller persists `PreparedAppMessage::topic()` and `encoded()` in the same catalog
    /// transaction as the terminal state before attempting publication.
    pub fn prepare(&self, app: &AppFacade) -> Result<PreparedAppMessage> {
        app.prepare_correlated(
            self.header_name(),
            self.channel(),
            self.body_value()?,
            self.correlation_id(),
        )
        .map_err(|error| CameraError::Messaging(error.to_string()))
    }

    /// Validated body view.
    #[must_use]
    pub const fn body(&self) -> &TerminalBody {
        &self.body
    }
}

fn serialize_error_code<S>(code: &ErrorCode, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(code.as_str())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(CameraError::rejected(
        ErrorCode::InvalidRequest,
        message.into(),
    ))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;

    fn base_body() -> TerminalBody {
        TerminalBody {
            schema_version: 1,
            event_id: "evt_1".to_string(),
            capture_id: "cap_1".to_string(),
            camera_id: "camera-a".to_string(),
            correlation_id: "corr-1".to_string(),
            trigger: CaptureTrigger::Command {
                request_id: "order-1".to_string(),
            },
            capture_profile: "inspection".to_string(),
            capture_mode: CaptureMode::SoftwareTrigger,
            timestamps: CaptureTimestamps {
                requested_at: Utc.with_ymd_and_hms(2026, 7, 10, 14, 0, 0).unwrap(),
                acquisition_started_at: None,
                camera_frame_at: None,
                frame_received_at: None,
                persisted_at: None,
                camera_frame_timestamp_quality: FrameTimestampQuality::Unknown,
            },
            durations_ms: CaptureDurations {
                total: 1,
                ..CaptureDurations::default()
            },
            image: None,
            frame: None,
            camera: CameraSummary {
                backend: BackendKind::Sim,
                vendor: None,
                model: Some("deterministic".to_string()),
                firmware: None,
                serial: Some("sim-a".to_string()),
            },
            metadata: Map::new(),
            failure: None,
            capture_group_id: None,
            group_size: None,
            backend_metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn exact_names_channels_and_body_correlation_are_stable() {
        let mut body = base_body();
        body.image = Some(ImageArtifact {
            absolute_path: "/captures/a.jpg".to_string(),
            relative_path: "a.jpg".to_string(),
            file_uri: "file:///captures/a.jpg".to_string(),
            content_type: "image/jpeg".to_string(),
            encoding: OutputEncoding::Jpeg,
            bytes: 3,
            sha256: "00".repeat(32),
            metadata_sidecar_relative_path: None,
        });
        let message = TerminalMessage::new(TerminalKind::Captured, body).unwrap();
        assert_eq!(message.header_name(), "ImageCaptured");
        assert_eq!(message.channel(), "image/captured");
        assert_eq!(message.correlation_id(), "corr-1");
        let value = message.body_value().unwrap();
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["correlationId"], "corr-1");
        assert_eq!(
            value["trigger"],
            json!({"type":"command", "requestId":"order-1"})
        );
        assert!(value.get("failure").is_none());
    }

    #[test]
    fn kind_invariants_prevent_false_success_or_ambiguous_failure() {
        assert!(TerminalMessage::new(TerminalKind::Captured, base_body()).is_err());
        assert!(TerminalMessage::new(TerminalKind::Failed, base_body()).is_err());
        assert!(TerminalMessage::new(TerminalKind::Cancelled, base_body()).is_ok());
    }

    #[test]
    fn terminal_kinds_have_complete_stable_routing() {
        assert_eq!(TerminalKind::Captured.header_name(), "ImageCaptured");
        assert_eq!(TerminalKind::Captured.channel(), "image/captured");
        assert_eq!(TerminalKind::Failed.header_name(), "ImageCaptureFailed");
        assert_eq!(TerminalKind::Failed.channel(), "image/failed");
        assert_eq!(
            TerminalKind::Cancelled.header_name(),
            "ImageCaptureCancelled"
        );
        assert_eq!(TerminalKind::Cancelled.channel(), "image/cancelled");
    }

    #[test]
    fn rejects_wrong_schema_and_each_required_empty_identifier() {
        let mut schema = base_body();
        schema.schema_version = 2;
        let error = TerminalMessage::new(TerminalKind::Cancelled, schema).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidRequest);

        for field in [
            "event_id",
            "capture_id",
            "camera_id",
            "correlation_id",
            "capture_profile",
        ] {
            let mut body = base_body();
            match field {
                "event_id" => body.event_id.clear(),
                "capture_id" => body.capture_id.clear(),
                "camera_id" => body.camera_id.clear(),
                "correlation_id" => body.correlation_id.clear(),
                "capture_profile" => body.capture_profile.clear(),
                _ => unreachable!("test field list is exhaustive"),
            }
            let error = TerminalMessage::new(TerminalKind::Cancelled, body).unwrap_err();
            assert_eq!(error.code(), ErrorCode::InvalidRequest, "{field}");
        }
    }

    #[test]
    fn failure_serializes_stable_code_and_omits_unreached_stages() {
        let mut body = base_body();
        body.failure = Some(FailureSummary {
            code: ErrorCode::ProcessInterrupted,
            stage: "ACQUIRING".to_string(),
            retriable: true,
            message: "process restarted".to_string(),
        });
        let message = TerminalMessage::new(TerminalKind::Failed, body).unwrap();
        let value = message.body_value().unwrap();
        assert_eq!(value["failure"]["code"], "PROCESS_INTERRUPTED");
        assert!(value["timestamps"].get("persistedAt").is_none());
        assert_eq!(message.channel(), "image/failed");
    }

    #[test]
    fn group_identity_must_match_trigger_and_pair_with_size() {
        let mut body = base_body();
        body.trigger = CaptureTrigger::GroupCommand {
            request_id: "group-1".to_string(),
            capture_group_id: "grp_1".to_string(),
        };
        body.capture_group_id = Some("grp_other".to_string());
        body.group_size = Some(2);
        assert!(TerminalMessage::new(TerminalKind::Cancelled, body).is_err());
    }

    #[test]
    fn group_fields_must_be_paired_and_valid_group_messages_are_preserved() {
        let mut only_id = base_body();
        only_id.capture_group_id = Some("grp_1".to_string());
        assert!(TerminalMessage::new(TerminalKind::Cancelled, only_id).is_err());

        let mut only_size = base_body();
        only_size.group_size = Some(2);
        assert!(TerminalMessage::new(TerminalKind::Cancelled, only_size).is_err());

        let mut too_small = base_body();
        too_small.capture_group_id = Some("grp_1".to_string());
        too_small.group_size = Some(1);
        assert!(TerminalMessage::new(TerminalKind::Cancelled, too_small).is_err());

        let mut valid = base_body();
        valid.trigger = CaptureTrigger::GroupCommand {
            request_id: "group-request".to_string(),
            capture_group_id: "grp_1".to_string(),
        };
        valid.capture_group_id = Some("grp_1".to_string());
        valid.group_size = Some(3);
        let value = TerminalMessage::new(TerminalKind::Cancelled, valid)
            .unwrap()
            .body_value()
            .unwrap();
        assert_eq!(value["captureGroupId"], "grp_1");
        assert_eq!(value["groupSize"], 3);
        assert_eq!(
            value["trigger"],
            json!({
                "type": "group-command",
                "requestId": "group-request",
                "captureGroupId": "grp_1"
            })
        );
    }

    #[test]
    fn schedule_trigger_and_non_empty_backend_metadata_serialize_exactly() {
        let mut body = base_body();
        body.trigger = CaptureTrigger::Schedule {
            schedule_id: "nightly".to_string(),
            intended_fire_time: Utc.with_ymd_and_hms(2026, 7, 10, 15, 30, 0).unwrap(),
        };
        body.backend_metadata
            .insert("exposureUs".to_string(), json!(1200));
        let value = TerminalMessage::new(TerminalKind::Cancelled, body)
            .unwrap()
            .body_value()
            .unwrap();
        assert_eq!(
            value["trigger"],
            json!({
                "type": "schedule",
                "scheduleId": "nightly",
                "intendedFireTime": "2026-07-10T15:30:00Z"
            })
        );
        assert_eq!(value["backendMetadata"], json!({"exposureUs": 1200}));
        assert!(value.get("captureGroupId").is_none());
        assert!(value.get("groupSize").is_none());
    }

    #[test]
    fn generated_ids_are_prefixed_and_time_sortable_uuid_shaped() {
        let first = TerminalMessage::new_event_id();
        assert!(first.starts_with("evt_"));
        assert_eq!(first.len(), 36);
        let event_uuid = Uuid::parse_str(&first[4..]).unwrap();
        assert_eq!(event_uuid.get_version_num(), 7);
        assert!(Uuid::parse_str(&TerminalMessage::new_scheduled_correlation()).is_ok());
    }
}
