//! Protocol-neutral camera, frame, job, and PTZ value types.

use std::collections::BTreeMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Supported backend kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendKind {
    /// Deterministic in-process backend.
    Sim,
    /// Aravis-backed GenICam backend.
    GenicamAravis,
    /// ONVIF control with snapshot/RTSP capture.
    OnvifRtsp,
    /// Bare RTSP still-image capture from a raw stream URL.
    Rtsp,
}

impl BackendKind {
    /// Stable wire/config token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sim => "sim",
            Self::GenicamAravis => "genicam-aravis",
            Self::OnvifRtsp => "onvif-rtsp",
            Self::Rtsp => "rtsp",
        }
    }
}

/// Source-frame pixel formats supported by the initial release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PixelFormat {
    /// One unsigned luminance byte per pixel.
    Mono8,
    /// Interleaved red, green, blue bytes.
    #[serde(rename = "RGB8")]
    Rgb8,
    /// Interleaved blue, green, red bytes.
    #[serde(rename = "BGR8")]
    Bgr8,
    /// Complete JPEG file bytes.
    #[serde(rename = "JPEG")]
    Jpeg,
}

impl PixelFormat {
    /// Exact uncompressed byte count, or `None` for compressed formats.
    #[must_use]
    pub fn uncompressed_len(self, width: u32, height: u32) -> Option<u64> {
        let channels = match self {
            Self::Mono8 => 1_u64,
            Self::Rgb8 | Self::Bgr8 => 3,
            Self::Jpeg => return None,
        };
        u64::from(width)
            .checked_mul(u64::from(height))?
            .checked_mul(channels)
    }
}

/// Final file encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputEncoding {
    /// Preserve an already encoded source frame.
    Passthrough,
    /// JPEG output.
    Jpeg,
    /// PNG output.
    Png,
    /// TIFF output.
    Tiff,
    /// Uninterpreted source bytes plus metadata.
    Raw,
}

/// Capture mechanisms exposed by backend capabilities and profiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CaptureMode {
    /// Deterministic simulator frame acquisition.
    Simulated,
    /// GenICam software trigger.
    SoftwareTrigger,
    /// ONVIF snapshot URI retrieval.
    SnapshotUri,
    /// RTSP frame extraction.
    RtspFrame,
}

/// Truthfulness classification for a frame timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FrameTimestampQuality {
    /// Timestamp came from the camera/exposure domain.
    Camera,
    /// Timestamp came from the media stream.
    Stream,
    /// Adapter receipt time is the best available observation.
    AdapterReceive,
    /// No defensible timestamp is available.
    Unknown,
}

/// A bounded source frame returned by a backend.
#[derive(Debug, Clone)]
pub struct CaptureFrame {
    /// Source bytes.
    pub bytes: Bytes,
    /// Pixel width.
    pub width: u32,
    /// Pixel height.
    pub height: u32,
    /// Declared source pixel/file format.
    pub pixel_format: PixelFormat,
    /// Actual acquisition mode used.
    pub capture_mode: CaptureMode,
    /// Optional camera/stream timestamp.
    pub source_timestamp: Option<DateTime<Utc>>,
    /// Timestamp provenance.
    pub timestamp_quality: FrameTimestampQuality,
    /// Bounded protocol metadata.
    pub backend_metadata: BTreeMap<String, Value>,
}

/// Normalized PTZ coordinate ranges.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PtzVector {
    /// Pan position/delta/velocity in `[-1, 1]`.
    pub pan: f64,
    /// Tilt position/delta/velocity in `[-1, 1]`.
    pub tilt: f64,
    /// Zoom coordinate; absolute uses `[0, 1]`, relative/velocity use `[-1, 1]`.
    pub zoom: f64,
}

impl PtzVector {
    /// Validates pan/tilt and a relative/velocity zoom range.
    pub fn validate_signed(self) -> bool {
        [self.pan, self.tilt, self.zoom]
            .into_iter()
            .all(|value| value.is_finite() && (-1.0..=1.0).contains(&value))
    }

    /// Validates pan/tilt signed ranges and an absolute zoom range.
    pub fn validate_absolute(self) -> bool {
        self.pan.is_finite()
            && (-1.0..=1.0).contains(&self.pan)
            && self.tilt.is_finite()
            && (-1.0..=1.0).contains(&self.tilt)
            && self.zoom.is_finite()
            && (0.0..=1.0).contains(&self.zoom)
    }
}

/// PTZ operations accepted by a camera session.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum PtzRequest {
    /// Start bounded continuous motion.
    Continuous {
        /// Normalized velocity.
        velocity: PtzVector,
        /// Mandatory server-side stop timeout.
        timeout: std::time::Duration,
    },
    /// Move to an absolute normalized position.
    Absolute {
        /// Requested position.
        position: PtzVector,
        /// Optional normalized speed.
        speed: Option<PtzVector>,
    },
    /// Move by a normalized delta.
    Relative {
        /// Requested translation.
        translation: PtzVector,
        /// Optional normalized speed.
        speed: Option<PtzVector>,
    },
    /// Stop selected axes.
    Stop {
        /// Whether pan movement is stopped.
        pan: bool,
        /// Whether tilt movement is stopped.
        tilt: bool,
        /// Whether zoom movement is stopped.
        zoom: bool,
    },
    /// Return to the camera's home position.
    Home,
    /// Read PTZ status.
    Status,
    /// List presets.
    ListPresets,
    /// Recall an opaque preset token.
    GotoPreset(String),
    /// Create or replace a named preset.
    SetPreset(String),
    /// Remove an opaque preset token.
    RemovePreset(String),
}

/// Current normalized PTZ state when reported by a camera.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PtzStatus {
    /// Normalized position when the backend can report it.
    pub position: Option<PtzVector>,
    /// Motion state when the backend can report it.
    pub moving: Option<bool>,
    /// Adapter observation time.
    pub observed_at: DateTime<Utc>,
}

/// Opaque PTZ preset metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PtzPreset {
    /// Camera-issued opaque token.
    pub token: String,
    /// Optional human-readable camera name.
    pub name: Option<String>,
}

/// PTZ operation result.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum PtzResult {
    /// Camera accepted a mutating command.
    Commanded,
    /// Current status.
    Status(PtzStatus),
    /// Current presets.
    Presets(Vec<PtzPreset>),
    /// Camera-issued preset token.
    PresetToken(String),
    /// Preset was removed.
    Removed,
}

/// Immutable capability snapshot for one connected session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CameraCapabilities {
    /// Supported capture modes.
    pub capture_modes: Vec<CaptureMode>,
    /// Supported source formats.
    pub pixel_formats: Vec<PixelFormat>,
    /// Whether software trigger is supported.
    pub software_trigger: bool,
    /// Whether snapshot URI capture is supported.
    pub snapshot_uri: bool,
    /// Whether RTSP frame capture is supported.
    pub rtsp: bool,
    /// Whether PTZ movement is supported.
    pub ptz: bool,
    /// Whether PTZ status is supported.
    pub ptz_status: bool,
    /// Whether preset listing/recall is supported.
    pub presets: bool,
    /// Whether preset mutation is supported.
    pub preset_mutation: bool,
    /// Camera vendor.
    pub vendor: Option<String>,
    /// Camera model.
    pub model: Option<String>,
    /// Camera firmware.
    pub firmware: Option<String>,
    /// Stable camera serial/device identifier.
    pub serial: Option<String>,
    /// Sanitized warnings.
    pub warnings: Vec<String>,
}

/// Public durable capture states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobState {
    /// Durable acceptance exists but queue transition is incomplete.
    Accepted,
    /// Waiting for admission/camera availability.
    Queued,
    /// Backend acquisition is active.
    Acquiring,
    /// Pixel/file conversion is active.
    Encoding,
    /// Atomic file persistence is active.
    Persisting,
    /// Final image and terminal catalog state are durable.
    Succeeded,
    /// Capture ended unsuccessfully.
    Failed,
    /// Cancellation won terminal arbitration.
    Cancelled,
    /// Restart interrupted non-resumable work.
    Interrupted,
}

impl JobState {
    /// Whether no further public state transition is allowed.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }

    /// Whether the public state machine permits a forward transition.
    ///
    /// Installation/cancellation arbitration adds stricter catalog predicates; this method only
    /// defines the stage graph and never permits a transition out of a terminal state.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Accepted => matches!(next, Self::Queued | Self::Interrupted),
            Self::Queued => matches!(
                next,
                Self::Acquiring | Self::Failed | Self::Cancelled | Self::Interrupted
            ),
            Self::Acquiring => matches!(
                next,
                Self::Encoding
                    | Self::Persisting
                    | Self::Failed
                    | Self::Cancelled
                    | Self::Interrupted
            ),
            Self::Encoding => matches!(
                next,
                Self::Persisting | Self::Failed | Self::Cancelled | Self::Interrupted
            ),
            Self::Persisting => matches!(
                next,
                Self::Succeeded | Self::Failed | Self::Cancelled | Self::Interrupted
            ),
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Interrupted => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_length_is_checked() {
        assert_eq!(PixelFormat::Mono8.uncompressed_len(2, 3), Some(6));
        assert_eq!(PixelFormat::Rgb8.uncompressed_len(2, 3), Some(18));
        assert_eq!(PixelFormat::Jpeg.uncompressed_len(2, 3), None);
        assert_eq!(PixelFormat::Rgb8.uncompressed_len(u32::MAX, u32::MAX), None);
    }

    #[test]
    fn ptz_ranges_are_distinct() {
        let signed = PtzVector {
            pan: -1.0,
            tilt: 1.0,
            zoom: -0.5,
        };
        assert!(signed.validate_signed());
        assert!(!signed.validate_absolute());
        assert!(
            PtzVector {
                zoom: 0.5,
                ..signed
            }
            .validate_absolute()
        );
    }

    #[test]
    fn only_terminal_states_report_terminal() {
        assert!(!JobState::Persisting.is_terminal());
        assert!(JobState::Succeeded.is_terminal());
        assert!(JobState::Interrupted.is_terminal());
    }

    #[test]
    fn job_state_graph_is_forward_only() {
        assert!(JobState::Accepted.can_transition_to(JobState::Queued));
        assert!(JobState::Acquiring.can_transition_to(JobState::Persisting));
        assert!(JobState::Persisting.can_transition_to(JobState::Succeeded));
        assert!(!JobState::Accepted.can_transition_to(JobState::Succeeded));
        assert!(!JobState::Succeeded.can_transition_to(JobState::Failed));
    }

    #[test]
    fn tokens_ranges_and_terminal_states_cover_all_public_variants() {
        assert_eq!(BackendKind::Sim.as_str(), "sim");
        assert_eq!(BackendKind::GenicamAravis.as_str(), "genicam-aravis");
        assert_eq!(BackendKind::OnvifRtsp.as_str(), "onvif-rtsp");
        assert_eq!(BackendKind::Rtsp.as_str(), "rtsp");
        assert_eq!(PixelFormat::Bgr8.uncompressed_len(2, 3), Some(18));
        assert_eq!(PixelFormat::Mono8.uncompressed_len(0, 9), Some(0));

        assert!(
            !PtzVector {
                pan: f64::NAN,
                tilt: 0.0,
                zoom: 0.0
            }
            .validate_signed()
        );
        assert!(
            !PtzVector {
                pan: 0.0,
                tilt: 0.0,
                zoom: 1.1
            }
            .validate_absolute()
        );

        for state in [
            JobState::Succeeded,
            JobState::Failed,
            JobState::Cancelled,
            JobState::Interrupted,
        ] {
            assert!(state.is_terminal());
            assert!(!state.can_transition_to(JobState::Queued));
        }
        assert!(JobState::Queued.can_transition_to(JobState::Failed));
        assert!(JobState::Queued.can_transition_to(JobState::Cancelled));
        assert!(JobState::Encoding.can_transition_to(JobState::Interrupted));
    }
}
