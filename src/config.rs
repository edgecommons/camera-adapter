//! Closed camera-adapter configuration model and cross-field validation.
//!
//! EdgeCommons intentionally leaves `component.global` and `component.instances[]`
//! extensible. This module is the component-owned strict schema: every object rejects
//! unknown fields, applies the binding defaults, and validates resource/deadline/security
//! relationships before runtime state changes.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::str::FromStr;

use chrono_tz::Tz;
use croner::Cron;
use ipnet::IpNet;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::error::{CameraError, Result};
use crate::model::{BackendKind, CaptureMode, OutputEncoding, PixelFormat};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Fully validated camera-adapter configuration.
#[derive(Debug, Clone)]
pub struct AdapterConfig {
    /// Process-wide configuration.
    pub global: GlobalConfig,
    /// Enabled and disabled camera instances that passed strict parsing.
    pub instances: Vec<CameraConfig>,
}

/// Initial-load result, including bad instances skipped under the startup-only policy.
#[derive(Debug, Clone)]
pub struct InitialConfigLoad {
    /// Valid configuration used to start the adapter.
    pub config: AdapterConfig,
    /// Stable diagnostics for invalid instance entries.
    pub skipped: Vec<ConfigIssue>,
}

/// One operator-safe configuration diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigIssue {
    /// Instance identifier when it could be read safely.
    pub instance: Option<String>,
    /// JSON-style failing path.
    pub path: String,
    /// Stable explanation without secret material.
    pub message: String,
}

/// Process-wide component configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GlobalConfig {
    /// Output root, templates, durability, and storage floors.
    pub output: OutputConfig,
    /// Durable catalog and retention policy.
    #[serde(default)]
    pub state: StateConfig,
    /// Bounded concurrency and memory limits.
    #[serde(default)]
    pub limits: LimitsConfig,
    /// Connection, job, stage, reply, reload, and shutdown deadlines.
    #[serde(default)]
    pub timeouts: TimeoutsConfig,
    /// Protocol discovery policy.
    #[serde(default)]
    pub discovery: DiscoveryConfig,
    /// Optional verbose operator-event policy.
    #[serde(default)]
    pub operator_events: OperatorEventsConfig,
    /// Southbound-health thresholds.
    #[serde(default)]
    pub health_thresholds: HealthThresholdsConfig,
    /// HTTP/XML/decompression safety limits.
    #[serde(default)]
    pub security: SecurityConfig,
    /// Cron schedules that fire one synchronised capture across several cameras.
    ///
    /// A group schedule crosses instances, so it belongs to the component rather than to any one
    /// camera -- which is why it lives here and not under `camera.schedules`.
    #[serde(default)]
    pub capture_group_schedules: Vec<CaptureGroupScheduleConfig>,
}

/// One cron schedule that captures several cameras as a single group.
///
/// An occurrence is submitted through the same path as the `sb/capture-group` command, so a
/// scheduled group is indistinguishable from a commanded one: one durable group row, all-or-nothing
/// acceptance, and one collated terminal notification.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaptureGroupScheduleConfig {
    /// Stable schedule token, unique across group schedules.
    pub id: String,
    /// Whether future occurrences are admitted.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Required six-field expression including seconds.
    pub cron: String,
    /// IANA timezone identifier.
    pub timezone: String,
    /// The cameras captured together, in result order.
    pub instances: Vec<String>,
    /// Capture profile applied to every member without an override.
    pub capture_profile: Option<String>,
    /// Per-camera capture-profile overrides; every key must be a member.
    #[serde(default)]
    pub profile_overrides: BTreeMap<String, String>,
    /// Missed-occurrence treatment.
    #[serde(default)]
    pub misfire_policy: MisfirePolicy,
    /// Treatment when this schedule already has a nonterminal group.
    ///
    /// Evaluated against the group, not against individual members: a group is outstanding until
    /// every one of its members is terminal.
    #[serde(default)]
    pub overlap_policy: OverlapPolicy,
    /// Stable deterministic jitter bound.
    #[serde(default)]
    pub jitter_seconds: u32,
    /// Optional per-member terminal deadline.
    pub timeout_ms: Option<u64>,
}

/// Output path, atomic-file, and disk-pressure configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputConfig {
    /// Absolute output root.
    pub root_directory: String,
    /// Relative per-camera directory template.
    #[serde(default = "default_camera_directory_template")]
    pub camera_directory_template: String,
    /// Relative filename template.
    #[serde(default = "default_file_name_template")]
    pub file_name_template: String,
    /// Whether a durable JSON sidecar is required before final image exposure.
    #[serde(default)]
    pub write_metadata_sidecar: bool,
    /// Absolute minimum free bytes retained after outstanding reservations.
    #[serde(default = "default_minimum_free_bytes")]
    pub minimum_free_bytes: u64,
    /// Minimum free percentage retained after outstanding reservations.
    #[serde(default = "default_minimum_free_percent")]
    pub minimum_free_percent: u8,
    /// Unix directory mode for new output directories.
    #[serde(default = "default_directory_mode")]
    pub directory_mode: String,
    /// Unix mode for new images and sidecars.
    #[serde(default = "default_file_mode")]
    pub file_mode: String,
}

/// Durable state configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct StateConfig {
    /// Explicit absolute state root, or platform-bound default where permitted.
    pub directory: Option<String>,
    /// Terminal result and idempotency retention.
    pub result_retention_hours: u32,
    /// Soft terminal-record count cap.
    pub max_result_records: u64,
    /// Delivered outbox retention.
    pub outbox_retention_hours: u32,
    /// Restart treatment for queued jobs.
    pub queued_recovery_policy: QueuedRecoveryPolicy,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            directory: None,
            result_retention_hours: 72,
            max_result_records: 100_000,
            outbox_retention_hours: 168,
            queued_recovery_policy: QueuedRecoveryPolicy::Requeue,
        }
    }
}

/// Queued-job restart policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueuedRecoveryPolicy {
    /// Requeue still-valid, unexpired jobs.
    #[default]
    Requeue,
    /// Mark every recovered queued job interrupted.
    Interrupt,
}

/// Process-wide and per-camera capacity bounds.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct LimitsConfig {
    /// Maximum enabled supervisors/sessions.
    pub max_connected_cameras: usize,
    /// Global simultaneous acquisitions.
    pub max_concurrent_captures: usize,
    /// Global simultaneous encoders.
    pub max_concurrent_encodes: usize,
    /// Global simultaneous image-persistence writers.
    pub max_concurrent_writes: usize,
    /// Global simultaneous connection attempts.
    pub max_concurrent_connects: usize,
    /// Raw frame bytes reserved across active work.
    pub max_in_flight_bytes: u64,
    /// Default and hard per-camera frame ceiling.
    pub max_frame_bytes_per_camera: u64,
    /// Maximum encoded caller metadata object.
    pub max_metadata_bytes: usize,
    /// Capture descriptors queued per camera.
    pub max_queued_captures_per_camera: usize,
    /// Ordinary control operations queued per camera.
    pub max_queued_controls_per_camera: usize,
    /// Deferred direct-command waiters for one capture.
    pub max_deferred_waiters_per_capture: usize,
    /// Maximum group fan-out.
    pub max_cameras_per_group: usize,
    /// Captures the whole component may hold waiting for a camera.
    ///
    /// The fleet-wide backlog bound, and it did not exist before: queueing was bounded only per
    /// camera, so the real worst case was `cameras x 2 x maxQueuedCapturesPerCamera` -- 2,048
    /// descriptors at the design target -- with no single number capping it and nothing able to see
    /// the fleet's backlog at all.
    pub max_pending_captures: usize,
    /// How long a capture may wait for a camera when its profile sets no `queueExpiryMs`.
    ///
    /// A capture that waits does not spend its execution budget -- its clocks start when a camera
    /// takes it. Something must still bound the wait, or a starved capture would queue forever.
    pub max_queue_wait_ms: u64,
    /// Named shared transport bounds.
    pub resource_groups: BTreeMap<String, ResourceGroupConfig>,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_connected_cameras: 256,
            max_concurrent_captures: 32,
            max_concurrent_encodes: std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(8),
            max_concurrent_writes: 8,
            max_concurrent_connects: 16,
            // These two are not independent knobs, and shipping them as if they were is what made
            // "32 concurrent captures" a fiction. A capture reserves `max_frame_bytes_per_camera`
            // — the DECLARED cap, not the frame's real size — so the budget must hold
            // max_concurrent_captures x that cap, or the byte budget silently becomes the real
            // concurrency limit. The old pair (1 GiB / 256 MiB) admitted exactly 4.
            //
            // 64 MiB is a generous per-frame ceiling for the machine-vision sensors this adapter
            // targets: an 8 MP Mono8 frame is ~8 MB and a 20 MP RGB frame ~60 MB. The old 256 MiB
            // was not a considered limit, just a number large enough never to reject anything —
            // and it is precisely because it was so large that the budget could not cover 32 of it.
            //
            // The budget is ACCOUNTING, not an allocation: with real 8 MB frames, 32 in flight is
            // ~256 MB of actual memory. Sizing it at 32 x the cap only guarantees the budget is
            // never the thing that decides the width. Raise the cap and the validator now forces
            // you to raise the budget with it (see validate_limits).
            max_in_flight_bytes: 32 * 64 * MIB, // = 2 GiB = max_concurrent_captures x the cap below
            max_frame_bytes_per_camera: 64 * MIB,
            max_metadata_bytes: 8 * 1024,
            max_queued_captures_per_camera: 4,
            max_pending_captures: 256,
            max_queue_wait_ms: 300_000,
            max_queued_controls_per_camera: 32,
            max_deferred_waiters_per_capture: 8,
            max_cameras_per_group: 32,
            resource_groups: BTreeMap::new(),
        }
    }
}

/// One named NIC/USB transport admission bound.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceGroupConfig {
    /// Simultaneous acquisitions using this resource.
    pub max_concurrent_captures: usize,
}

/// Runtime deadline configuration in milliseconds.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct TimeoutsConfig {
    /// One backend connection attempt.
    pub connect_ms: u64,
    /// Initial reconnect delay.
    pub reconnect_backoff_min_ms: u64,
    /// Maximum reconnect delay before jitter.
    pub reconnect_backoff_max_ms: u64,
    /// Default acceptance-to-terminal deadline.
    pub job_terminal_ms: u64,
    /// Acquisition-stage cap.
    pub capture_ms: u64,
    /// Encoding-stage cap.
    pub encode_ms: u64,
    /// Persistence-stage cap.
    pub persist_ms: u64,
    /// PTZ response cap.
    pub ptz_ms: u64,
    /// Direct-reply settlement margin.
    pub reply_margin_ms: u64,
    /// Maximum core deferred-token lifetime.
    pub max_deferred_reply_lifetime_ms: u64,
    /// Reload replacement drain budget.
    pub reload_drain_timeout_ms: u64,
    /// Graceful shutdown budget.
    pub shutdown_grace_ms: u64,
}

impl Default for TimeoutsConfig {
    fn default() -> Self {
        Self {
            connect_ms: 10_000,
            reconnect_backoff_min_ms: 1_000,
            reconnect_backoff_max_ms: 60_000,
            job_terminal_ms: 90_000,
            capture_ms: 30_000,
            encode_ms: 30_000,
            persist_ms: 30_000,
            ptz_ms: 10_000,
            reply_margin_ms: 5_000,
            max_deferred_reply_lifetime_ms: 95_000,
            reload_drain_timeout_ms: 30_000,
            shutdown_grace_ms: 30_000,
        }
    }
}

/// Discovery policy.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct DiscoveryConfig {
    /// Whether periodic and command discovery are enabled.
    pub enabled: bool,
    /// Whether compact unconfigured candidates may be returned.
    pub report_unconfigured: bool,
    /// Periodic discovery interval.
    pub interval_seconds: u64,
    /// Maximum retained candidates.
    pub max_results: usize,
    /// Exact OS interface names eligible for credential-free WS-Discovery multicast.
    pub eligible_interfaces: Vec<String>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            report_unconfigured: false,
            interval_seconds: 60,
            max_results: 1_000,
            eligible_interfaces: Vec::new(),
        }
    }
}

/// Optional high-volume operator-event policy.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct OperatorEventsConfig {
    /// Emit capture queued/started diagnostic events.
    pub capture_lifecycle: bool,
}

/// Southbound-health thresholds.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct HealthThresholdsConfig {
    /// Seconds without a successful observation before staleSignals becomes one.
    pub stale_signal_secs: u64,
}

impl Default for HealthThresholdsConfig {
    fn default() -> Self {
        Self {
            stale_signal_secs: 300,
        }
    }
}

/// HTTP/XML/decompression policy shared by ONVIF instances.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct SecurityConfig {
    /// Maximum response status/header bytes.
    pub max_header_bytes: usize,
    /// Maximum decoded/compressed ratio.
    pub max_decompression_ratio: u32,
    /// Development-only permission for Basic authentication over plaintext.
    pub allow_basic_over_plaintext: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            max_header_bytes: 65_536,
            max_decompression_ratio: 100,
            allow_basic_over_plaintext: false,
        }
    }
}

/// One camera instance.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CameraConfig {
    /// Stable UNS instance token.
    pub id: String,
    /// Whether the camera accepts actuation.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Optional shared NIC/USB admission group.
    pub resource_group: Option<String>,
    /// Protocol backend configuration.
    pub backend: BackendConfig,
    /// Default profile for new requests.
    pub default_capture_profile: String,
    /// Named immutable-at-acceptance capture profiles.
    pub capture_profiles: BTreeMap<String, CaptureProfile>,
    /// Optional schedules; omission is command-only operation.
    #[serde(default)]
    pub schedules: Vec<ScheduleConfig>,
    /// PTZ policy.
    #[serde(default)]
    pub ptz: PtzConfig,
}

/// Tagged protocol backend configuration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum BackendConfig {
    /// Deterministic in-process backend.
    Sim(SimBackendConfig),
    /// Aravis GenICam backend.
    GenicamAravis(GenicamBackendConfig),
    /// ONVIF control and snapshot/RTSP backend.
    OnvifRtsp(OnvifBackendConfig),
}

impl BackendConfig {
    /// Stable kind discriminator.
    #[must_use]
    pub const fn kind(&self) -> BackendKind {
        match self {
            Self::Sim(_) => BackendKind::Sim,
            Self::GenicamAravis(_) => BackendKind::GenicamAravis,
            Self::OnvifRtsp(_) => BackendKind::OnvifRtsp,
        }
    }
}

/// Deterministic simulator configuration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SimBackendConfig {
    /// Stable simulated device identity; defaults to camera instance id.
    pub simulated_id: Option<String>,
    /// Deterministic generator seed; defaults to a hash of instance id.
    pub seed: Option<u64>,
    /// Source frame settings.
    #[serde(default)]
    pub frame: SimFrameConfig,
    /// Artificial connection latency.
    #[serde(default)]
    pub connect_delay_ms: u64,
    /// Artificial capture latency.
    #[serde(default = "default_sim_capture_delay")]
    pub capture_delay_ms: u64,
    /// Simulated PTZ capabilities.
    #[serde(default)]
    pub ptz: SimPtzConfig,
    /// Deterministic failure counters.
    #[serde(default)]
    pub faults: SimFaultConfig,
}

/// Simulator source-frame settings.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct SimFrameConfig {
    /// Width in pixels.
    pub width: u32,
    /// Height in pixels.
    pub height: u32,
    /// Source format.
    pub pixel_format: PixelFormat,
    /// Deterministic pattern.
    pub pattern: SimPattern,
}

impl Default for SimFrameConfig {
    fn default() -> Self {
        Self {
            width: 640,
            height: 480,
            pixel_format: PixelFormat::Rgb8,
            pattern: SimPattern::ColorBars,
        }
    }
}

/// Simulator frame patterns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SimPattern {
    /// SMPTE-like color bands.
    #[default]
    ColorBars,
    /// Diagonal intensity gradient.
    Gradient,
    /// Alternating checkerboard.
    Checkerboard,
    /// Seed-derived solid color.
    Solid,
}

/// Simulator PTZ capability switches.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct SimPtzConfig {
    /// Whether movement is supported.
    pub supported: bool,
    /// Whether status is observable.
    pub status_supported: bool,
    /// Whether presets are supported.
    pub presets_supported: bool,
}

impl Default for SimPtzConfig {
    fn default() -> Self {
        Self {
            supported: false,
            status_supported: true,
            presets_supported: false,
        }
    }
}

/// Simulator deterministic failure counters.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct SimFaultConfig {
    /// Disconnect after this many successful captures.
    pub disconnect_after_captures: Option<u64>,
    /// Fail each Nth capture.
    pub fail_every_nth_capture: Option<u64>,
    /// Produce an incomplete frame on each Nth capture.
    pub incomplete_every_nth_capture: Option<u64>,
}

/// Stable GenICam selector; exactly one field is required.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GenicamSelector {
    /// Camera serial number.
    pub serial: Option<String>,
    /// Camera MAC address.
    pub mac: Option<String>,
    /// Aravis/GenICam stable device identifier.
    pub device_id: Option<String>,
    /// Explicit camera IP address.
    pub ip: Option<String>,
}

/// GenICam/Aravis backend configuration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GenicamBackendConfig {
    /// Stable selector.
    pub selector: GenicamSelector,
    /// `auto`, `gige-vision`, or `usb3-vision`.
    #[serde(default)]
    pub transport: GenicamTransport,
    /// Explicit host interface when required.
    pub interface: Option<String>,
    /// `auto` or a numeric device packet size represented as JSON.
    pub packet_size: Option<Value>,
    /// Inter-packet delay.
    pub packet_delay_ns: Option<u64>,
    /// Native buffer count.
    pub buffer_count: Option<usize>,
    /// Allowlisted standard GenICam feature values.
    #[serde(default)]
    pub feature_overrides: BTreeMap<String, Value>,
}

/// GenICam transport selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GenicamTransport {
    /// Select from discovered device capabilities.
    #[default]
    Auto,
    /// GigE Vision.
    GigeVision,
    /// USB3 Vision.
    Usb3Vision,
}

/// Standard secret reference resolved lazily through EdgeCommons credentials.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretRef {
    /// Secret name/path.
    #[serde(rename = "$secret")]
    pub secret: String,
    /// Optional field within a structured secret.
    pub field: Option<String>,
}

/// Stable ONVIF WS-Discovery selector.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OnvifSelector {
    /// Exact endpoint-reference URI.
    pub endpoint_reference: String,
}

/// ONVIF TLS policy.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct TlsConfig {
    /// Optional PEM CA-bundle secret.
    pub ca: Option<SecretRef>,
    /// Whether certificate hostname verification is required.
    pub verify_hostname: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            ca: None,
            verify_hostname: true,
        }
    }
}

/// ONVIF/RTSP backend configuration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OnvifBackendConfig {
    /// Explicit ONVIF Device service URL.
    pub device_service_url: Option<String>,
    /// Stable discovery selector alternative.
    pub selector: Option<OnvifSelector>,
    /// Credential secret containing username/password.
    pub credentials: Option<SecretRef>,
    /// Opaque profile token or exact profile name.
    pub media_profile: String,
    /// Default backend capture mode.
    #[serde(default = "default_onvif_capture_mode")]
    pub capture_mode: CaptureMode,
    /// Allow only the explicitly defined safe snapshot-to-RTSP fallbacks.
    #[serde(default)]
    pub rtsp_fallback: bool,
    /// RTSP lifecycle policy.
    #[serde(default)]
    pub rtsp_session_policy: RtspSessionPolicy,
    /// Allow plaintext/TLS-verification development overrides.
    #[serde(default)]
    pub allow_insecure: bool,
    /// Allowed URI hostnames, in addition to configured endpoint host.
    #[serde(default)]
    pub allowed_uri_hosts: Vec<String>,
    /// Allowed resolved CIDRs.
    #[serde(default)]
    pub allowed_uri_cidrs: Vec<IpNet>,
    /// Maximum SOAP response bytes.
    #[serde(default = "default_max_soap_bytes")]
    pub max_soap_bytes: u64,
    /// Maximum snapshot response bytes.
    #[serde(default = "default_max_snapshot_bytes")]
    pub max_snapshot_bytes: u64,
    /// Maximum XML nesting depth.
    #[serde(default = "default_max_xml_depth")]
    pub max_xml_depth: usize,
    /// TLS trust/hostname policy.
    #[serde(default = "default_tls_config")]
    pub tls: TlsConfig,
    /// ONVIF Media service generation.
    #[serde(default)]
    pub media_service: MediaService,
    /// Authentication negotiation mode.
    #[serde(default)]
    pub authentication_mode: AuthenticationMode,
}

/// RTSP connection lifecycle policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RtspSessionPolicy {
    /// Establish/tear down for each capture.
    #[default]
    OnDemand,
    /// Retain one session per camera for at most 30 seconds.
    Warm,
}

/// ONVIF Media service selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MediaService {
    /// Prefer Media2 and fall back to Media1 only during read-only discovery.
    #[default]
    Auto,
    /// Require Media1.
    Media1,
    /// Require Media2.
    Media2,
}

/// ONVIF authentication selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthenticationMode {
    /// Negotiate using read-only requests before any actuation.
    #[default]
    Auto,
    /// Require HTTP Digest authentication.
    HttpDigest,
    /// Require WS-Security UsernameToken PasswordDigest.
    WsseDigest,
    /// Require Basic authentication, subject to plaintext policy.
    Basic,
}

/// One immutable-at-acceptance capture profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaptureProfile {
    /// Backend acquisition mode override.
    pub capture_mode: Option<CaptureMode>,
    /// Camera-offline behavior.
    pub offline_policy: Option<OfflinePolicy>,
    /// Maximum offline queue residence for `queue` policy.
    pub queue_expiry_ms: Option<u64>,
    /// Overall acceptance-to-terminal deadline override.
    pub timeout_ms: Option<u64>,
    /// Hard source/decoded frame ceiling.
    pub maximum_frame_bytes: Option<u64>,
    /// Requested GenICam source format.
    pub pixel_format: Option<PixelFormat>,
    /// GenICam region width.
    pub width: Option<u32>,
    /// GenICam region height.
    pub height: Option<u32>,
    /// GenICam region X offset.
    pub offset_x: Option<u32>,
    /// GenICam region Y offset.
    pub offset_y: Option<u32>,
    /// GenICam exposure time.
    pub exposure_micros: Option<u64>,
    /// GenICam gain.
    pub gain: Option<f64>,
    /// Required output encoding.
    pub output: ProfileOutputConfig,
    /// Per-profile PTZ/capture interlock override.
    pub capture_interlock: Option<CaptureInterlock>,
}

/// Offline admission policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OfflinePolicy {
    /// Reject immediately while offline.
    FailFast,
    /// Wait for reconnection only until the job deadline.
    WaitUntilDeadline,
    /// Retain offline until explicit queue expiry/deadline.
    Queue,
}

/// Profile output encoding configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProfileOutputConfig {
    /// Required final encoding.
    pub encoding: OutputEncoding,
    /// JPEG encoder quality.
    #[serde(default = "default_jpeg_quality")]
    pub jpeg_quality: u8,
}

/// Capture/PTZ interlock policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CaptureInterlock {
    /// Reject capture while moving.
    #[default]
    Reject,
    /// Stop motion and wait for idle/settle delay.
    StopAndSettle,
    /// Explicitly allow capture while moving.
    Allow,
}

/// Per-camera schedule.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScheduleConfig {
    /// Stable schedule token.
    pub id: String,
    /// Whether future occurrences are admitted.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Required six-field expression including seconds.
    pub cron: String,
    /// IANA timezone identifier.
    pub timezone: String,
    /// Named capture profile.
    pub capture_profile: String,
    /// Missed-occurrence treatment.
    #[serde(default)]
    pub misfire_policy: MisfirePolicy,
    /// Same-schedule nonterminal-job treatment.
    #[serde(default)]
    pub overlap_policy: OverlapPolicy,
    /// Stable deterministic jitter bound.
    #[serde(default)]
    pub jitter_seconds: u32,
}

/// Schedule misfire policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MisfirePolicy {
    /// Drop missed occurrences.
    #[default]
    Skip,
    /// Submit exactly the latest missed occurrence.
    Coalesce,
}

/// Schedule overlap policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OverlapPolicy {
    /// Skip when another occurrence remains nonterminal.
    #[default]
    Skip,
    /// Submit one ordinary bounded queued job.
    Queue,
}

/// Per-camera PTZ policy.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct PtzConfig {
    /// Whether PTZ commands are exposed.
    pub enabled: bool,
    /// Maximum continuous movement duration.
    pub maximum_continuous_move_ms: u64,
    /// Default capture/motion interlock.
    pub capture_interlock: CaptureInterlock,
    /// Settle delay after a stop.
    pub settle_ms: u64,
    /// Whether set/remove preset is permitted.
    pub allow_preset_mutation: bool,
}

impl Default for PtzConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            maximum_continuous_move_ms: 10_000,
            capture_interlock: CaptureInterlock::Reject,
            settle_ms: 750,
            allow_preset_mutation: false,
        }
    }
}

impl AdapterConfig {
    /// Parses startup configuration, skipping only invalid instance entries when at least one
    /// enabled valid instance remains.
    ///
    /// # Errors
    /// Returns [`CameraError::Config`] for an invalid global block or when no enabled valid
    /// camera remains.
    pub fn from_core_initial(config: &edgecommons::config::Config) -> Result<InitialConfigLoad> {
        let global = parse_global(config.global())?;
        let mut instances = Vec::new();
        let mut skipped = Vec::new();
        let mut seen = HashSet::new();

        for (index, raw) in config.parsed.component.instances.iter().enumerate() {
            let id = raw.get("id").and_then(Value::as_str).map(str::to_string);
            match serde_json::from_value::<CameraConfig>(raw.clone()) {
                Ok(camera) => {
                    let path = format!("component.instances[{index}]");
                    if !seen.insert(camera.id.clone()) {
                        skipped.push(ConfigIssue {
                            instance: Some(camera.id),
                            path: format!("{path}.id"),
                            message: "camera id is duplicated".to_string(),
                        });
                    } else if let Err(error) = validate_camera(&camera, &global, &path) {
                        skipped.push(issue_from_error(id, error));
                    } else {
                        instances.push(camera);
                    }
                }
                Err(_error) => skipped.push(ConfigIssue {
                    instance: id,
                    path: format!("component.instances[{index}]"),
                    message: safe_deserialization_message().to_string(),
                }),
            }
        }

        if !instances.iter().any(|camera| camera.enabled) {
            return config_error(
                "component.instances",
                "at least one enabled valid camera instance is required",
            );
        }
        validate_cross_instance(&instances, &global, &seen)?;
        Ok(InitialConfigLoad {
            config: Self { global, instances },
            skipped,
        })
    }

    /// Parses a reload candidate atomically; unlike startup, one invalid instance rejects all.
    ///
    /// # Errors
    /// Returns [`CameraError::Config`] on any global, instance, duplicate, or cross-field error.
    pub fn from_core_reload(config: &edgecommons::config::Config) -> Result<Self> {
        let global = parse_global(config.global())?;
        let mut instances = Vec::with_capacity(config.parsed.component.instances.len());
        let mut seen = HashSet::new();
        for (index, raw) in config.parsed.component.instances.iter().enumerate() {
            let path = format!("component.instances[{index}]");
            let camera: CameraConfig =
                serde_json::from_value(raw.clone()).map_err(|_error| CameraError::Config {
                    path: path.clone(),
                    message: safe_deserialization_message().to_string(),
                })?;
            if !seen.insert(camera.id.clone()) {
                return config_error(format!("{path}.id"), "camera id is duplicated");
            }
            validate_camera(&camera, &global, &path)?;
            instances.push(camera);
        }
        if !instances.iter().any(|camera| camera.enabled) {
            return config_error(
                "component.instances",
                "at least one enabled valid camera instance is required",
            );
        }
        validate_cross_instance(&instances, &global, &seen)?;
        Ok(Self { global, instances })
    }
}

fn parse_global(raw: &Value) -> Result<GlobalConfig> {
    let global: GlobalConfig =
        serde_json::from_value(raw.clone()).map_err(|_error| CameraError::Config {
            path: "component.global".to_string(),
            message: safe_deserialization_message().to_string(),
        })?;
    validate_global(&global)?;
    Ok(global)
}

fn validate_global(global: &GlobalConfig) -> Result<()> {
    require_absolute(
        &global.output.root_directory,
        "component.global.output.rootDirectory",
    )?;
    validate_template(&global.output.camera_directory_template, true)?;
    validate_template(&global.output.file_name_template, false)?;
    range(
        u64::from(global.output.minimum_free_percent),
        0,
        100,
        "component.global.output.minimumFreePercent",
    )?;
    validate_mode(
        &global.output.directory_mode,
        false,
        "component.global.output.directoryMode",
    )?;
    validate_mode(
        &global.output.file_mode,
        true,
        "component.global.output.fileMode",
    )?;

    if let Some(directory) = &global.state.directory {
        require_absolute(directory, "component.global.state.directory")?;
    }
    range(
        u64::from(global.state.result_retention_hours),
        1,
        8_760,
        "component.global.state.resultRetentionHours",
    )?;
    range(
        global.state.max_result_records,
        1_000,
        10_000_000,
        "component.global.state.maxResultRecords",
    )?;
    if global.state.outbox_retention_hours < global.state.result_retention_hours {
        return config_error(
            "component.global.state.outboxRetentionHours",
            "must be at least resultRetentionHours",
        );
    }

    let limits = &global.limits;
    range_usize(
        limits.max_connected_cameras,
        1,
        4_096,
        "limits.maxConnectedCameras",
    )?;
    range_usize(
        limits.max_concurrent_captures,
        1,
        256,
        "limits.maxConcurrentCaptures",
    )?;
    range_usize(
        limits.max_concurrent_encodes,
        1,
        64,
        "limits.maxConcurrentEncodes",
    )?;
    range_usize(
        limits.max_concurrent_writes,
        1,
        64,
        "limits.maxConcurrentWrites",
    )?;
    range_usize(
        limits.max_concurrent_connects,
        1,
        256,
        "limits.maxConcurrentConnects",
    )?;
    range(
        limits.max_in_flight_bytes,
        64 * MIB,
        u64::MAX,
        "limits.maxInFlightBytes",
    )?;
    range(
        limits.max_frame_bytes_per_camera,
        MIB,
        2 * GIB,
        "limits.maxFrameBytesPerCamera",
    )?;
    range_usize(
        limits.max_metadata_bytes,
        0,
        65_536,
        "limits.maxMetadataBytes",
    )?;
    range_usize(
        limits.max_queued_captures_per_camera,
        1,
        1_000,
        "limits.maxQueuedCapturesPerCamera",
    )?;
    range_usize(
        limits.max_queued_controls_per_camera,
        1,
        1_024,
        "limits.maxQueuedControlsPerCamera",
    )?;
    range_usize(
        limits.max_deferred_waiters_per_capture,
        1,
        64,
        "limits.maxDeferredWaitersPerCapture",
    )?;
    range_usize(
        limits.max_cameras_per_group,
        2,
        256,
        "limits.maxCamerasPerGroup",
    )?;
    // The byte budget must cover the concurrency the component ADVERTISES, not one frame of it.
    //
    // The fleet backlog must be able to hold at least one camera's worth of queued work, or the
    // per-camera bound is a lie: a single camera could never fill the queue it is allowed to fill.
    // This is B2's lesson in a different costume -- a bound that is smaller than the thing it is
    // supposed to hold does not fail loudly, it silently caps the system somewhere else.
    if limits.max_pending_captures < limits.max_queued_captures_per_camera {
        return config_error(
            "component.global.limits.maxPendingCaptures",
            format!(
                "must be at least maxQueuedCapturesPerCamera ({}); {} would cap the component's                  whole backlog below what a single camera is permitted to queue",
                limits.max_queued_captures_per_camera, limits.max_pending_captures,
            ),
        );
    }
    if limits.max_pending_captures == 0 || limits.max_queue_wait_ms == 0 {
        return config_error(
            "component.global.limits.maxPendingCaptures",
            "maxPendingCaptures and maxQueueWaitMs must be positive".to_owned(),
        );
    }

    // A capture reserves `maxFrameBytesPerCamera` — the DECLARED cap, not the frame's actual size —
    // for its whole admission. So the number of captures that can hold a memory reservation at once
    // is floor(maxInFlightBytes / maxFrameBytesPerCamera), and THAT, not maxConcurrentCaptures, is
    // the real width of the system. The old check only demanded room for a single frame, which let
    // the shipped defaults (1 GiB / 256 MiB) advertise 32-way concurrency while admitting 4: the
    // other 28 took a semaphore permit and then parked inside the byte budget until their deadline
    // expired. A fleet firing on one cron minute lost most of its captures to CAPTURE_TIMEOUT, and
    // every timeout wrote a durable row. The system was never 32-wide; it only said so.
    let required_in_flight =
        (limits.max_concurrent_captures as u64).saturating_mul(limits.max_frame_bytes_per_camera);
    if limits.max_in_flight_bytes < required_in_flight {
        let admits = limits.max_in_flight_bytes / limits.max_frame_bytes_per_camera.max(1);
        return config_error(
            "component.global.limits.maxInFlightBytes",
            format!(
                "must be at least maxConcurrentCaptures x maxFrameBytesPerCamera \
                 ({} x {} = {} bytes); {} admits only {} concurrent capture(s), so a component \
                 configured for {} would silently run {}-wide and time the rest out",
                limits.max_concurrent_captures,
                limits.max_frame_bytes_per_camera,
                required_in_flight,
                limits.max_in_flight_bytes,
                admits,
                limits.max_concurrent_captures,
                admits,
            ),
        );
    }
    for (name, group) in &limits.resource_groups {
        check_token(name, "component.global.limits.resourceGroups")?;
        if group.max_concurrent_captures == 0
            || group.max_concurrent_captures > limits.max_concurrent_captures
        {
            return config_error(
                format!("component.global.limits.resourceGroups.{name}.maxConcurrentCaptures"),
                "must be between 1 and global maxConcurrentCaptures",
            );
        }
    }

    let timeouts = &global.timeouts;
    range(timeouts.connect_ms, 100, 300_000, "timeouts.connectMs")?;
    range(
        timeouts.reconnect_backoff_min_ms,
        100,
        60_000,
        "timeouts.reconnectBackoffMinMs",
    )?;
    range(
        timeouts.reconnect_backoff_max_ms,
        timeouts.reconnect_backoff_min_ms,
        3_600_000,
        "timeouts.reconnectBackoffMaxMs",
    )?;
    range(
        timeouts.job_terminal_ms,
        1_000,
        1_800_000,
        "timeouts.jobTerminalMs",
    )?;
    for (value, path) in [
        (timeouts.capture_ms, "timeouts.captureMs"),
        (timeouts.encode_ms, "timeouts.encodeMs"),
        (timeouts.persist_ms, "timeouts.persistMs"),
    ] {
        range(value, 100, 600_000, path)?;
    }
    range(timeouts.ptz_ms, 100, 60_000, "timeouts.ptzMs")?;
    range(
        timeouts.reply_margin_ms,
        100,
        60_000,
        "timeouts.replyMarginMs",
    )?;
    range(
        timeouts.max_deferred_reply_lifetime_ms,
        1_100,
        1_860_000,
        "timeouts.maxDeferredReplyLifetimeMs",
    )?;
    if timeouts.max_deferred_reply_lifetime_ms
        < timeouts
            .job_terminal_ms
            .saturating_add(timeouts.reply_margin_ms)
    {
        return config_error(
            "component.global.timeouts.maxDeferredReplyLifetimeMs",
            "must be at least jobTerminalMs plus replyMarginMs",
        );
    }
    range(
        global.discovery.interval_seconds,
        5,
        3_600,
        "discovery.intervalSeconds",
    )?;
    range_usize(
        global.discovery.max_results,
        1,
        10_000,
        "discovery.maxResults",
    )?;
    if global.discovery.eligible_interfaces.len() > 64 {
        return config_error(
            "discovery.eligibleInterfaces",
            "must contain at most 64 explicit interface names",
        );
    }
    let mut discovery_interfaces = HashSet::new();
    for (index, interface) in global.discovery.eligible_interfaces.iter().enumerate() {
        if interface.is_empty()
            || interface.len() > 256
            || interface.chars().any(char::is_control)
            || !discovery_interfaces.insert(interface)
        {
            return config_error(
                format!("discovery.eligibleInterfaces[{index}]"),
                "must be a distinct 1..256-byte interface name without controls",
            );
        }
    }
    if global.discovery.enabled && discovery_interfaces.is_empty() {
        return config_error(
            "discovery.eligibleInterfaces",
            "at least one explicit interface is required when discovery is enabled",
        );
    }
    range(
        global.health_thresholds.stale_signal_secs,
        1,
        86_400,
        "healthThresholds.staleSignalSecs",
    )?;
    range_usize(
        global.security.max_header_bytes,
        4_096,
        1_048_576,
        "security.maxHeaderBytes",
    )?;
    range(
        u64::from(global.security.max_decompression_ratio),
        1,
        1_000,
        "security.maxDecompressionRatio",
    )?;
    Ok(())
}

fn validate_camera(camera: &CameraConfig, global: &GlobalConfig, path: &str) -> Result<()> {
    check_token(&camera.id, &format!("{path}.id"))?;
    if camera.capture_profiles.is_empty() || camera.capture_profiles.len() > 100 {
        return config_error(
            format!("{path}.captureProfiles"),
            "must contain between 1 and 100 profiles",
        );
    }
    if !camera
        .capture_profiles
        .contains_key(&camera.default_capture_profile)
    {
        return config_error(
            format!("{path}.defaultCaptureProfile"),
            "must name a configured capture profile",
        );
    }
    if camera.schedules.len() > 100 {
        return config_error(
            format!("{path}.schedules"),
            "must contain at most 100 schedules",
        );
    }
    if let Some(group) = &camera.resource_group {
        if !global.limits.resource_groups.contains_key(group) {
            return config_error(
                format!("{path}.resourceGroup"),
                "must name component.global.limits.resourceGroups entry",
            );
        }
    }

    for (name, profile) in &camera.capture_profiles {
        check_token(name, &format!("{path}.captureProfiles"))?;
        validate_profile(
            profile,
            camera.backend.kind(),
            global,
            &format!("{path}.captureProfiles.{name}"),
        )?;
    }

    let mut schedule_ids = HashSet::new();
    for (index, schedule) in camera.schedules.iter().enumerate() {
        let schedule_path = format!("{path}.schedules[{index}]");
        check_token(&schedule.id, &format!("{schedule_path}.id"))?;
        if !schedule_ids.insert(&schedule.id) {
            return config_error(format!("{schedule_path}.id"), "schedule id is duplicated");
        }
        if schedule.cron.split_whitespace().count() != 6 || Cron::from_str(&schedule.cron).is_err()
        {
            return config_error(
                format!("{schedule_path}.cron"),
                "must be a valid six-field cron expression including seconds",
            );
        }
        if schedule.timezone.parse::<Tz>().is_err() {
            return config_error(
                format!("{schedule_path}.timezone"),
                "must be an IANA timezone identifier",
            );
        }
        if !camera
            .capture_profiles
            .contains_key(&schedule.capture_profile)
        {
            return config_error(
                format!("{schedule_path}.captureProfile"),
                "must name a configured capture profile",
            );
        }
        range(
            u64::from(schedule.jitter_seconds),
            0,
            3_600,
            &format!("{schedule_path}.jitterSeconds"),
        )?;
    }

    range(
        camera.ptz.maximum_continuous_move_ms,
        100,
        60_000,
        &format!("{path}.ptz.maximumContinuousMoveMs"),
    )?;
    range(
        camera.ptz.settle_ms,
        0,
        30_000,
        &format!("{path}.ptz.settleMs"),
    )?;

    match &camera.backend {
        BackendConfig::Sim(sim) => validate_sim(sim, global, &format!("{path}.backend"))?,
        BackendConfig::GenicamAravis(genicam) => {
            validate_genicam(genicam, &format!("{path}.backend"))?
        }
        BackendConfig::OnvifRtsp(onvif) => {
            validate_onvif(onvif, global, &format!("{path}.backend"))?
        }
    }
    Ok(())
}

fn validate_profile(
    profile: &CaptureProfile,
    backend: BackendKind,
    global: &GlobalConfig,
    path: &str,
) -> Result<()> {
    if matches!(profile.offline_policy, Some(OfflinePolicy::Queue)) {
        range(
            profile.queue_expiry_ms.ok_or_else(|| CameraError::Config {
                path: format!("{path}.queueExpiryMs"),
                message: "is required when offlinePolicy is queue".to_string(),
            })?,
            100,
            86_400_000,
            &format!("{path}.queueExpiryMs"),
        )?;
    } else if profile.queue_expiry_ms.is_some() {
        return config_error(
            format!("{path}.queueExpiryMs"),
            "is valid only when offlinePolicy is queue",
        );
    }
    let terminal = profile
        .timeout_ms
        .unwrap_or(global.timeouts.job_terminal_ms);
    range(terminal, 1_000, 1_800_000, &format!("{path}.timeoutMs"))?;
    if global.timeouts.max_deferred_reply_lifetime_ms
        < terminal.saturating_add(global.timeouts.reply_margin_ms)
    {
        return config_error(
            format!("{path}.timeoutMs"),
            "requires maxDeferredReplyLifetimeMs >= timeoutMs + replyMarginMs",
        );
    }
    let maximum = profile
        .maximum_frame_bytes
        .unwrap_or(global.limits.max_frame_bytes_per_camera);
    range(maximum, MIB, 2 * GIB, &format!("{path}.maximumFrameBytes"))?;
    if maximum > global.limits.max_in_flight_bytes {
        return config_error(
            format!("{path}.maximumFrameBytes"),
            "must not exceed global maxInFlightBytes",
        );
    }
    range(
        u64::from(profile.output.jpeg_quality),
        1,
        100,
        &format!("{path}.output.jpegQuality"),
    )?;
    if let Some(gain) = profile.gain {
        if !gain.is_finite() || gain < 0.0 {
            return config_error(
                format!("{path}.gain"),
                "must be a finite non-negative number",
            );
        }
    }
    if let Some(mode) = profile.capture_mode {
        let allowed = match backend {
            BackendKind::Sim => mode == CaptureMode::Simulated,
            BackendKind::GenicamAravis => mode == CaptureMode::SoftwareTrigger,
            BackendKind::OnvifRtsp => {
                matches!(mode, CaptureMode::SnapshotUri | CaptureMode::RtspFrame)
            }
        };
        if !allowed {
            return config_error(
                format!("{path}.captureMode"),
                "capture mode is not supported by the configured backend",
            );
        }
    }
    Ok(())
}

fn validate_sim(sim: &SimBackendConfig, global: &GlobalConfig, path: &str) -> Result<()> {
    range(
        u64::from(sim.frame.width),
        1,
        16_384,
        &format!("{path}.frame.width"),
    )?;
    range(
        u64::from(sim.frame.height),
        1,
        16_384,
        &format!("{path}.frame.height"),
    )?;
    range(
        sim.connect_delay_ms,
        0,
        300_000,
        &format!("{path}.connectDelayMs"),
    )?;
    range(
        sim.capture_delay_ms,
        0,
        600_000,
        &format!("{path}.captureDelayMs"),
    )?;
    if let Some(bytes) = sim
        .frame
        .pixel_format
        .uncompressed_len(sim.frame.width, sim.frame.height)
    {
        if bytes > global.limits.max_frame_bytes_per_camera {
            return config_error(
                format!("{path}.frame"),
                "simulated frame exceeds maxFrameBytesPerCamera",
            );
        }
    }
    for (value, field) in [
        (
            sim.faults.disconnect_after_captures,
            "disconnectAfterCaptures",
        ),
        (sim.faults.fail_every_nth_capture, "failEveryNthCapture"),
        (
            sim.faults.incomplete_every_nth_capture,
            "incompleteEveryNthCapture",
        ),
    ] {
        if value == Some(0) {
            return config_error(
                format!("{path}.faults.{field}"),
                "must be positive when set",
            );
        }
    }
    Ok(())
}

fn validate_genicam(genicam: &GenicamBackendConfig, path: &str) -> Result<()> {
    let selector_count = [
        genicam.selector.serial.as_ref(),
        genicam.selector.mac.as_ref(),
        genicam.selector.device_id.as_ref(),
        genicam.selector.ip.as_ref(),
    ]
    .into_iter()
    .flatten()
    .count();
    if selector_count != 1 {
        return config_error(
            format!("{path}.selector"),
            "exactly one of serial, mac, deviceId, or ip is required",
        );
    }
    for (name, value) in [
        ("serial", genicam.selector.serial.as_deref()),
        ("mac", genicam.selector.mac.as_deref()),
        ("deviceId", genicam.selector.device_id.as_deref()),
        ("ip", genicam.selector.ip.as_deref()),
    ] {
        if let Some(value) = value {
            if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
                return config_error(
                    format!("{path}.selector.{name}"),
                    "must be 1..256 UTF-8 bytes without controls",
                );
            }
        }
    }
    if let Some(mac) = genicam.selector.mac.as_deref() {
        if !valid_mac_address(mac) {
            return config_error(
                format!("{path}.selector.mac"),
                "must contain six hexadecimal octets separated by ':' or '-'",
            );
        }
    }
    if let Some(ip) = genicam.selector.ip.as_deref() {
        if ip.parse::<std::net::Ipv4Addr>().is_err() {
            return config_error(
                format!("{path}.selector.ip"),
                "must be a valid IPv4 address",
            );
        }
    }
    if let Some(interface) = genicam.interface.as_deref() {
        if interface.is_empty() || interface.len() > 256 || interface.chars().any(char::is_control)
        {
            return config_error(
                format!("{path}.interface"),
                "must be 1..256 UTF-8 bytes without controls",
            );
        }
    }
    if genicam.transport == GenicamTransport::Usb3Vision {
        if genicam.interface.is_some() {
            return config_error(format!("{path}.interface"), "is not valid for usb3-vision");
        }
        if genicam.selector.ip.is_some() || genicam.selector.mac.is_some() {
            return config_error(
                format!("{path}.selector"),
                "ip and mac selectors require gige-vision or auto transport",
            );
        }
        if genicam.packet_size.is_some() || genicam.packet_delay_ns.is_some() {
            return config_error(
                path,
                "packetSize and packetDelayNs are not valid for usb3-vision",
            );
        }
    }
    if let Some(count) = genicam.buffer_count {
        range_usize(count, 2, 64, &format!("{path}.bufferCount"))?;
    }
    if let Some(packet_size) = &genicam.packet_size {
        let valid = packet_size.as_str() == Some("auto")
            || packet_size
                .as_u64()
                .is_some_and(|value| (1..=i32::MAX as u64).contains(&value));
        if !valid {
            return config_error(
                format!("{path}.packetSize"),
                "must be 'auto' or an integer in 1..=2147483647",
            );
        }
    }
    if genicam.feature_overrides.len() > 32 {
        return config_error(
            format!("{path}.featureOverrides"),
            "must contain at most 32 allowlisted standard features",
        );
    }
    for (name, value) in &genicam.feature_overrides {
        let valid = match name.as_str() {
            "AcquisitionFrameRateEnable" | "ReverseX" | "ReverseY" => value.is_boolean(),
            "DeviceLinkThroughputLimit" => {
                value
                    .as_u64()
                    .is_some_and(|number| number <= i64::MAX as u64)
                    || value.as_i64().is_some_and(|number| number >= 0)
            }
            "AcquisitionFrameRate" | "BlackLevel" | "Gamma" => {
                value.as_f64().is_some_and(f64::is_finite)
            }
            "BalanceWhiteAuto" | "BlackLevelAuto" => value.as_str().is_some_and(|text| {
                !text.is_empty() && text.len() <= 128 && !text.chars().any(char::is_control)
            }),
            _ => false,
        };
        if !valid {
            return config_error(
                format!("{path}.featureOverrides.{name}"),
                "is unknown or has the wrong JSON type for the standard-feature allowlist",
            );
        }
    }
    Ok(())
}

fn valid_mac_address(value: &str) -> bool {
    let separator = if value.contains(':') {
        ':'
    } else if value.contains('-') {
        '-'
    } else {
        return false;
    };
    let mut octets = value.split(separator);
    (0..6).all(|_| {
        octets.next().is_some_and(|octet| {
            octet.len() == 2 && octet.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    }) && octets.next().is_none()
}

fn validate_onvif(onvif: &OnvifBackendConfig, global: &GlobalConfig, path: &str) -> Result<()> {
    if onvif.device_service_url.is_some() == onvif.selector.is_some() {
        return config_error(
            path,
            "exactly one of deviceServiceUrl or selector.endpointReference is required",
        );
    }
    if let Some(url) = &onvif.device_service_url {
        validate_endpoint_url(
            url,
            onvif.allow_insecure,
            &format!("{path}.deviceServiceUrl"),
        )?;
    }
    if let Some(selector) = &onvif.selector {
        let len = selector.endpoint_reference.len();
        if !(1..=1_024).contains(&len) || selector.endpoint_reference.chars().any(char::is_control)
        {
            return config_error(
                format!("{path}.selector.endpointReference"),
                "must be 1..1024 bytes without control characters",
            );
        }
        if global.discovery.eligible_interfaces.is_empty() {
            return config_error(
                format!("{path}.selector"),
                "endpoint-reference selection requires discovery.eligibleInterfaces",
            );
        }
    }
    if let Some(credentials) = &onvif.credentials {
        validate_secret_ref(credentials, &format!("{path}.credentials"))?;
    }
    if let Some(ca) = &onvif.tls.ca {
        validate_secret_ref(ca, &format!("{path}.tls.ca"))?;
    }
    if onvif.media_profile.is_empty() || onvif.media_profile.len() > 1_024 {
        return config_error(format!("{path}.mediaProfile"), "must be 1..1024 bytes");
    }
    if !matches!(
        onvif.capture_mode,
        CaptureMode::SnapshotUri | CaptureMode::RtspFrame
    ) {
        return config_error(
            format!("{path}.captureMode"),
            "must be snapshot-uri or rtsp-frame",
        );
    }
    #[cfg(not(feature = "rtsp"))]
    if onvif.capture_mode == CaptureMode::RtspFrame || onvif.rtsp_fallback {
        return config_error(
            format!("{path}.captureMode"),
            "rtsp-frame capture and snapshot fallback require the 'rtsp' build feature",
        );
    }
    range(
        onvif.max_soap_bytes,
        4 * 1024,
        16 * MIB,
        &format!("{path}.maxSoapBytes"),
    )?;
    range(
        onvif.max_snapshot_bytes,
        MIB,
        2 * GIB,
        &format!("{path}.maxSnapshotBytes"),
    )?;
    range_usize(onvif.max_xml_depth, 8, 256, &format!("{path}.maxXmlDepth"))?;
    if onvif.max_snapshot_bytes > global.limits.max_in_flight_bytes {
        return config_error(
            format!("{path}.maxSnapshotBytes"),
            "must not exceed global maxInFlightBytes",
        );
    }
    let configured_plaintext = onvif
        .device_service_url
        .as_deref()
        .and_then(|value| Url::parse(value).ok())
        .is_some_and(|url| url.scheme() == "http");
    if onvif.authentication_mode == AuthenticationMode::Basic
        && configured_plaintext
        && !(onvif.allow_insecure && global.security.allow_basic_over_plaintext)
    {
        return config_error(
            format!("{path}.authenticationMode"),
            "Basic over plaintext requires security.allowBasicOverPlaintext=true",
        );
    }
    if !onvif.tls.verify_hostname && !onvif.allow_insecure {
        return config_error(
            format!("{path}.tls.verifyHostname"),
            "false requires allowInsecure=true",
        );
    }
    for (index, host) in onvif.allowed_uri_hosts.iter().enumerate() {
        if host.is_empty()
            || host.len() > 253
            || host.chars().any(char::is_control)
            || host.contains(['/', '@'])
        {
            return config_error(
                format!("{path}.allowedUriHosts[{index}]"),
                "must be a hostname or IP without path/userinfo",
            );
        }
    }
    Ok(())
}

fn validate_secret_ref(reference: &SecretRef, path: &str) -> Result<()> {
    if reference.secret.is_empty()
        || reference.secret.len() > 1_024
        || reference.secret.chars().any(char::is_control)
    {
        return config_error(
            format!("{path}.$secret"),
            "must be 1..1024 UTF-8 bytes without control characters",
        );
    }
    if let Some(field) = &reference.field {
        if field.is_empty() || field.len() > 256 || field.chars().any(char::is_control) {
            return config_error(
                format!("{path}.field"),
                "must be 1..256 UTF-8 bytes without control characters",
            );
        }
    }
    Ok(())
}

const fn safe_deserialization_message() -> &'static str {
    "contains an unknown field or a value with an invalid type"
}

fn validate_cross_instance(
    instances: &[CameraConfig],
    global: &GlobalConfig,
    declared: &HashSet<String>,
) -> Result<()> {
    let enabled = instances.iter().filter(|camera| camera.enabled).count();
    if enabled > global.limits.max_connected_cameras {
        return config_error(
            "component.instances",
            "enabled camera count exceeds limits.maxConnectedCameras",
        );
    }
    validate_group_schedules(instances, global, declared)
}

/// Validates `global.captureGroupSchedules` against the cameras the component declares.
///
/// The rules deliberately mirror [`crate::commands::GroupCaptureRequest::validate`], because an
/// occurrence is submitted down that exact path: a group schedule that could only ever be rejected
/// at fire time is a schedule that silently never fires, and the place to say so is startup.
///
/// Membership is checked against the cameras the operator *declared*, not against the ones that
/// survived validation. A camera that fails its own validation is skipped at startup by design, and
/// a group schedule naming it must not escalate that into a component that refuses to start -- the
/// occurrence simply fails to admit, and says why. A camera that was never declared at all is a
/// typo, and that is worth refusing to start for.
fn validate_group_schedules(
    instances: &[CameraConfig],
    global: &GlobalConfig,
    declared: &HashSet<String>,
) -> Result<()> {
    if global.capture_group_schedules.len() > 100 {
        return config_error(
            "component.global.captureGroupSchedules",
            "must contain at most 100 group schedules",
        );
    }
    let by_id: BTreeMap<&str, &CameraConfig> = instances
        .iter()
        .map(|camera| (camera.id.as_str(), camera))
        .collect();
    let mut schedule_ids = HashSet::new();
    for (index, schedule) in global.capture_group_schedules.iter().enumerate() {
        let path = format!("component.global.captureGroupSchedules[{index}]");
        check_token(&schedule.id, &format!("{path}.id"))?;
        if !schedule_ids.insert(&schedule.id) {
            return config_error(format!("{path}.id"), "group schedule id is duplicated");
        }
        if schedule.cron.split_whitespace().count() != 6 || Cron::from_str(&schedule.cron).is_err()
        {
            return config_error(
                format!("{path}.cron"),
                "must be a valid six-field cron expression including seconds",
            );
        }
        if schedule.timezone.parse::<Tz>().is_err() {
            return config_error(
                format!("{path}.timezone"),
                "must be an IANA timezone identifier",
            );
        }
        // A group is two or more cameras captured together; the command path rejects a shorter list
        // and the scheduler must not be able to configure one it could never submit.
        if schedule.instances.len() < 2 {
            return config_error(
                format!("{path}.instances"),
                "must name at least two cameras",
            );
        }
        if schedule.instances.len() > global.limits.max_cameras_per_group {
            return config_error(
                format!("{path}.instances"),
                "exceeds limits.maxCamerasPerGroup",
            );
        }
        let mut members = HashSet::with_capacity(schedule.instances.len());
        for instance in &schedule.instances {
            check_token(instance, &format!("{path}.instances"))?;
            if !members.insert(instance.as_str()) {
                return config_error(
                    format!("{path}.instances"),
                    "must not name the same camera twice",
                );
            }
            if !declared.contains(instance) {
                return config_error(
                    format!("{path}.instances"),
                    "must name cameras declared in component.instances",
                );
            }
        }
        if let Some(profile) = schedule.capture_profile.as_deref() {
            check_token(profile, &format!("{path}.captureProfile"))?;
        }
        for (instance, profile) in &schedule.profile_overrides {
            if !members.contains(instance.as_str()) {
                return config_error(
                    format!("{path}.profileOverrides"),
                    "keys must be a subset of instances",
                );
            }
            check_token(profile, &format!("{path}.profileOverrides"))?;
        }
        // Profiles are resolved per camera at fire time. Check the ones we can see now: a camera
        // that was skipped is absent from `by_id` and is checked when it comes back.
        for instance in &schedule.instances {
            let Some(camera) = by_id.get(instance.as_str()) else {
                continue;
            };
            let selected = schedule
                .profile_overrides
                .get(instance)
                .map(String::as_str)
                .or(schedule.capture_profile.as_deref());
            if let Some(selected) = selected {
                if !camera.capture_profiles.contains_key(selected) {
                    return config_error(
                        format!("{path}.captureProfile"),
                        format!("camera '{instance}' has no capture profile named '{selected}'"),
                    );
                }
            }
        }
        range(
            u64::from(schedule.jitter_seconds),
            0,
            3_600,
            &format!("{path}.jitterSeconds"),
        )?;
        if let Some(timeout_ms) = schedule.timeout_ms {
            range(timeout_ms, 1_000, 1_800_000, &format!("{path}.timeoutMs"))?;
        }
    }
    Ok(())
}

fn validate_endpoint_url(value: &str, allow_insecure: bool, path: &str) -> Result<()> {
    let url = Url::parse(value).map_err(|error| CameraError::Config {
        path: path.to_string(),
        message: error.to_string(),
    })?;
    if url.username() != "" || url.password().is_some() {
        return config_error(path, "URI user information is forbidden");
    }
    if url.host_str().is_none() {
        return config_error(path, "URI must contain a host");
    }
    match url.scheme() {
        "https" => Ok(()),
        "http" if allow_insecure => Ok(()),
        "http" => config_error(path, "plaintext HTTP requires allowInsecure=true"),
        _ => config_error(path, "scheme must be http or https"),
    }
}

fn validate_template(template: &str, directory: bool) -> Result<()> {
    let path = if directory {
        "component.global.output.cameraDirectoryTemplate"
    } else {
        "component.global.output.fileNameTemplate"
    };
    if template.is_empty() || template.len() > 1_024 {
        return config_error(path, "must be 1..1024 bytes");
    }
    if is_absolute_cross_platform(template) || template.split(['/', '\\']).any(|part| part == "..")
    {
        return config_error(path, "must be relative and may not contain '..'");
    }
    let allowed = [
        "cameraId",
        "yyyy",
        "MM",
        "dd",
        "timestamp",
        "captureId",
        "extension",
    ];
    let mut remainder = template;
    while let Some(open) = remainder.find('{') {
        let after = &remainder[open + 1..];
        let Some(close) = after.find('}') else {
            return config_error(path, "contains an unclosed template variable");
        };
        let variable = &after[..close];
        if !allowed.contains(&variable) {
            return config_error(path, format!("unknown template variable '{{{variable}}}'"));
        }
        remainder = &after[close + 1..];
    }
    if remainder.contains('}') {
        return config_error(path, "contains an unmatched '}'");
    }
    Ok(())
}

fn validate_mode(value: &str, file: bool, path: &str) -> Result<()> {
    if value.len() != 4
        || !value.starts_with('0')
        || !value.chars().all(|c| ('0'..='7').contains(&c))
    {
        return config_error(path, "must be a four-digit octal mode");
    }
    let mode = u16::from_str_radix(value, 8).map_err(|error| CameraError::Config {
        path: path.to_string(),
        message: error.to_string(),
    })?;
    if mode & 0o7000 != 0 || (file && mode & 0o111 != 0) {
        return config_error(path, "special bits and file execute bits are forbidden");
    }
    Ok(())
}

fn require_absolute(value: &str, path: &str) -> Result<()> {
    if !is_absolute_cross_platform(value) {
        return config_error(
            path,
            "must be an absolute POSIX, drive-qualified, or UNC path",
        );
    }
    Ok(())
}

fn is_absolute_cross_platform(value: &str) -> bool {
    if Path::new(value).is_absolute() || value.starts_with('/') || value.starts_with("\\\\") {
        return true;
    }
    let bytes = value.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

fn check_token(value: &str, path: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')))
    {
        return config_error(
            path,
            "must be a non-empty UNS token using ASCII letters, digits, '.', '_', or '-'",
        );
    }
    Ok(())
}

fn range(value: u64, minimum: u64, maximum: u64, path: &str) -> Result<()> {
    if !(minimum..=maximum).contains(&value) {
        return config_error(path, format!("must be in range {minimum}..={maximum}"));
    }
    Ok(())
}

fn range_usize(value: usize, minimum: usize, maximum: usize, path: &str) -> Result<()> {
    if !(minimum..=maximum).contains(&value) {
        return config_error(path, format!("must be in range {minimum}..={maximum}"));
    }
    Ok(())
}

fn config_error<T>(path: impl Into<String>, message: impl Into<String>) -> Result<T> {
    Err(CameraError::Config {
        path: path.into(),
        message: message.into(),
    })
}

fn issue_from_error(instance: Option<String>, error: CameraError) -> ConfigIssue {
    match error {
        CameraError::Config { path, message } => ConfigIssue {
            instance,
            path,
            message,
        },
        other => ConfigIssue {
            instance,
            path: "component.instances".to_string(),
            message: other.to_string(),
        },
    }
}

fn default_true() -> bool {
    true
}
fn default_camera_directory_template() -> String {
    "{cameraId}/{yyyy}/{MM}/{dd}".to_string()
}
fn default_file_name_template() -> String {
    "{timestamp}-{captureId}.{extension}".to_string()
}
fn default_minimum_free_bytes() -> u64 {
    GIB
}
fn default_minimum_free_percent() -> u8 {
    5
}
fn default_directory_mode() -> String {
    "0750".to_string()
}
fn default_file_mode() -> String {
    "0640".to_string()
}
fn default_sim_capture_delay() -> u64 {
    10
}
fn default_onvif_capture_mode() -> CaptureMode {
    CaptureMode::SnapshotUri
}
fn default_max_soap_bytes() -> u64 {
    MIB
}
fn default_max_snapshot_bytes() -> u64 {
    64 * MIB
}
fn default_max_xml_depth() -> usize {
    64
}
fn default_jpeg_quality() -> u8 {
    90
}
fn default_tls_config() -> TlsConfig {
    TlsConfig::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    type ConfigMutation = Box<dyn Fn(&mut Value)>;

    fn core(value: Value) -> edgecommons::config::Config {
        edgecommons::config::Config::from_value(COMPONENT, "gw-01", value).expect("core config")
    }

    fn reload_error_path(value: Value) -> String {
        match AdapterConfig::from_core_reload(&core(value)) {
            Err(CameraError::Config { path, .. }) => path,
            Err(other) => panic!("expected configuration error, got {other:?}"),
            Ok(_) => panic!("configuration must be rejected"),
        }
    }

    const COMPONENT: &str = "com.mbreissi.edgecommons.CameraAdapter";

    fn valid_config() -> Value {
        json!({
            "component": {
                "token": "camera-adapter",
                "global": { "output": { "rootDirectory": "/tmp/captures", "minimumFreeBytes": 0 } },
                "instances": [{
                    "id": "camera-a",
                    "backend": { "type": "sim" },
                    "defaultCaptureProfile": "main",
                    "captureProfiles": { "main": { "output": { "encoding": "png" } } }
                }]
            }
        })
    }

    /// A two-camera config with one valid group schedule, for the group-schedule cases below.
    fn group_schedule_config() -> Value {
        let mut value = valid_config();
        value["component"]["instances"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "id": "camera-b",
                "backend": { "type": "sim" },
                "defaultCaptureProfile": "main",
                "captureProfiles": { "main": { "output": { "encoding": "png" } } }
            }));
        value["component"]["global"]["captureGroupSchedules"] = json!([{
            "id": "line-a-sync",
            "cron": "0 */5 * * * *",
            "timezone": "America/New_York",
            "instances": ["camera-a", "camera-b"],
            "captureProfile": "main"
        }]);
        value
    }

    #[test]
    fn a_valid_group_schedule_is_accepted_with_its_defaults() {
        let config = AdapterConfig::from_core_reload(&core(group_schedule_config())).unwrap();
        let schedules = &config.global.capture_group_schedules;
        assert_eq!(schedules.len(), 1);
        let schedule = &schedules[0];
        assert_eq!(schedule.id, "line-a-sync");
        assert!(schedule.enabled);
        assert_eq!(schedule.instances, vec!["camera-a", "camera-b"]);
        assert_eq!(schedule.capture_profile.as_deref(), Some("main"));
        assert_eq!(schedule.misfire_policy, MisfirePolicy::Skip);
        assert_eq!(schedule.overlap_policy, OverlapPolicy::Skip);
        assert_eq!(schedule.jitter_seconds, 0);
        assert_eq!(schedule.timeout_ms, None);
    }

    /// Every way a group schedule can be written such that it could only ever fail at fire time.
    ///
    /// The rules mirror `GroupCaptureRequest::validate`, because an occurrence is submitted down
    /// that exact path. A group schedule the command path would reject is a schedule that silently
    /// never fires -- so it is rejected here, at startup, where somebody is looking.
    #[test]
    fn group_schedule_validation_rejects_what_the_command_path_would_reject() {
        let cases: Vec<(&str, ConfigMutation, &str)> = vec![
            (
                "a fleet backlog smaller than one camera is allowed to queue",
                Box::new(|value| {
                    value["component"]["global"]["limits"] = json!({
                        "maxPendingCaptures": 2,
                        "maxQueuedCapturesPerCamera": 4
                    });
                }),
                "component.global.limits.maxPendingCaptures",
            ),
            (
                "a fleet backlog of zero",
                Box::new(|value| {
                    value["component"]["global"]["limits"] = json!({ "maxPendingCaptures": 0 });
                }),
                "component.global.limits.maxPendingCaptures",
            ),
            (
                "a queue wait of zero",
                Box::new(|value| {
                    value["component"]["global"]["limits"] = json!({ "maxQueueWaitMs": 0 });
                }),
                "component.global.limits.maxPendingCaptures",
            ),
            (
                "a camera that is not declared anywhere",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["instances"] =
                        json!(["camera-a", "camera-typo"]);
                }),
                "component.global.captureGroupSchedules[0].instances",
            ),
            (
                "a group of one",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["instances"] =
                        json!(["camera-a"]);
                }),
                "component.global.captureGroupSchedules[0].instances",
            ),
            (
                "the same camera twice",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["instances"] =
                        json!(["camera-a", "camera-a"]);
                }),
                "component.global.captureGroupSchedules[0].instances",
            ),
            (
                "an override for a camera that is not a member",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["profileOverrides"] =
                        json!({ "camera-c": "main" });
                }),
                "component.global.captureGroupSchedules[0].profileOverrides",
            ),
            (
                "a capture profile no member camera has",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["captureProfile"] =
                        json!("wide");
                }),
                "component.global.captureGroupSchedules[0].captureProfile",
            ),
            (
                "a five-field cron",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["cron"] =
                        json!("*/5 * * * *");
                }),
                "component.global.captureGroupSchedules[0].cron",
            ),
            (
                "a timezone that is not IANA",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["timezone"] =
                        json!("EST5EDT-ish");
                }),
                "component.global.captureGroupSchedules[0].timezone",
            ),
            (
                "jitter beyond an hour",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["jitterSeconds"] =
                        json!(3_601);
                }),
                "component.global.captureGroupSchedules[0].jitterSeconds",
            ),
            (
                "a timeout the command path would refuse",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["timeoutMs"] =
                        json!(999);
                }),
                "component.global.captureGroupSchedules[0].timeoutMs",
            ),
            (
                "more cameras than a group may hold",
                Box::new(|value| {
                    value["component"]["global"]["limits"] = json!({ "maxCamerasPerGroup": 2 });
                    value["component"]["instances"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({
                            "id": "camera-c",
                            "backend": { "type": "sim" },
                            "defaultCaptureProfile": "main",
                            "captureProfiles": { "main": { "output": { "encoding": "png" } } }
                        }));
                    value["component"]["global"]["captureGroupSchedules"][0]["instances"] =
                        json!(["camera-a", "camera-b", "camera-c"]);
                }),
                "component.global.captureGroupSchedules[0].instances",
            ),
            (
                "a capture profile name that is not a UNS token",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["captureProfile"] =
                        json!("not a token");
                }),
                "component.global.captureGroupSchedules[0].captureProfile",
            ),
            (
                "an override profile name that is not a UNS token",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["profileOverrides"] =
                        json!({ "camera-b": "not a token" });
                }),
                "component.global.captureGroupSchedules[0].profileOverrides",
            ),
            (
                "an id that is not a UNS token",
                Box::new(|value| {
                    value["component"]["global"]["captureGroupSchedules"][0]["id"] =
                        json!("line a sync");
                }),
                "component.global.captureGroupSchedules[0].id",
            ),
            (
                "more group schedules than the component admits",
                Box::new(|value| {
                    let one = value["component"]["global"]["captureGroupSchedules"][0].clone();
                    let mut many = Vec::new();
                    for index in 0..101 {
                        let mut copy = one.clone();
                        copy["id"] = json!(format!("line-{index}"));
                        many.push(copy);
                    }
                    value["component"]["global"]["captureGroupSchedules"] = json!(many);
                }),
                "component.global.captureGroupSchedules",
            ),
            (
                "two schedules sharing an id",
                Box::new(|value| {
                    let first = value["component"]["global"]["captureGroupSchedules"][0].clone();
                    value["component"]["global"]["captureGroupSchedules"]
                        .as_array_mut()
                        .unwrap()
                        .push(first);
                }),
                "component.global.captureGroupSchedules[1].id",
            ),
        ];
        for (name, mutate, expected_path) in cases {
            let mut value = group_schedule_config();
            mutate(&mut value);
            assert_eq!(
                reload_error_path(value),
                expected_path,
                "group schedule with {name} must be rejected at {expected_path}"
            );
        }
    }

    /// A skipped camera must not escalate into a component that refuses to start.
    ///
    /// Startup deliberately tolerates a bad camera by skipping it. A group schedule naming that
    /// camera is still a *declared* camera, so the config stands: the schedule simply fails to admit
    /// its occurrences, and says so, until the camera is fixed. Only a camera that was never
    /// declared -- a typo -- is a config error.
    #[test]
    fn a_group_schedule_naming_a_skipped_camera_does_not_stop_the_component() {
        let mut value = group_schedule_config();
        value["component"]["instances"][1]["captureProfiles"] = json!({});
        let loaded = AdapterConfig::from_core_initial(&core(value)).unwrap();
        assert_eq!(
            loaded.skipped.len(),
            1,
            "the camera with no capture profiles is skipped"
        );
        assert_eq!(loaded.config.instances.len(), 1);
        assert_eq!(
            loaded.config.global.capture_group_schedules.len(),
            1,
            "the group schedule naming it survives -- it is declared, just not currently valid"
        );
    }

    #[test]
    fn defaults_match_binding_contract() {
        let loaded = AdapterConfig::from_core_initial(&core(valid_config())).unwrap();
        let cfg = loaded.config;
        assert!(loaded.skipped.is_empty());
        assert_eq!(cfg.instances.len(), 1);
        assert_eq!(cfg.global.limits.max_connected_cameras, 256);
        assert_eq!(cfg.global.limits.max_queued_controls_per_camera, 32);
        assert_eq!(cfg.global.timeouts.max_deferred_reply_lifetime_ms, 95_000);
        assert_eq!(cfg.global.security.max_header_bytes, 65_536);
        assert_eq!(cfg.instances[0].backend.kind(), BackendKind::Sim);
    }

    #[test]
    fn startup_skips_bad_instance_but_reload_rejects_it() {
        let mut value = valid_config();
        value["component"]["instances"]
            .as_array_mut()
            .unwrap()
            .push(json!({"id":"bad","backend":{"type":"unknown"}}));
        let core = core(value);
        let initial = AdapterConfig::from_core_initial(&core).unwrap();
        assert_eq!(initial.config.instances.len(), 1);
        assert_eq!(initial.skipped.len(), 1);
        assert!(AdapterConfig::from_core_reload(&core).is_err());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let mut value = valid_config();
        value["component"]["instances"][0]["surprise"] = json!(true);
        assert!(AdapterConfig::from_core_initial(&core(value)).is_err());
    }

    #[test]
    fn no_valid_enabled_camera_is_fatal() {
        let mut value = valid_config();
        value["component"]["instances"][0]["enabled"] = json!(false);
        assert!(AdapterConfig::from_core_initial(&core(value)).is_err());
    }

    #[test]
    fn deadline_relationship_is_enforced() {
        let mut value = valid_config();
        value["component"]["global"]["timeouts"] = json!({
            "jobTerminalMs": 90000,
            "replyMarginMs": 5000,
            "maxDeferredReplyLifetimeMs": 94999
        });
        assert!(AdapterConfig::from_core_initial(&core(value)).is_err());
    }

    #[test]
    fn onvif_requires_one_stable_binding() {
        let mut value = valid_config();
        value["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "mediaProfile": "main",
            "deviceServiceUrl": "https://10.0.0.2/onvif/device_service",
            "selector": { "endpointReference": "urn:uuid:duplicate" }
        });
        assert!(AdapterConfig::from_core_initial(&core(value)).is_err());
    }

    #[test]
    fn discovery_requires_explicit_eligible_interfaces() {
        let mut missing = valid_config();
        missing["component"]["global"]["discovery"] = json!({ "enabled": true });
        assert!(AdapterConfig::from_core_initial(&core(missing)).is_err());

        let mut configured = valid_config();
        configured["component"]["global"]["discovery"] = json!({
            "enabled": true,
            "eligibleInterfaces": ["Ethernet 2", "vEthernet (WSL)"]
        });
        let loaded = AdapterConfig::from_core_initial(&core(configured)).unwrap();
        assert_eq!(
            loaded.config.global.discovery.eligible_interfaces,
            ["Ethernet 2", "vEthernet (WSL)"]
        );
    }

    #[test]
    fn onvif_endpoint_reference_requires_explicit_eligible_interfaces() {
        let mut missing = valid_config();
        missing["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "mediaProfile": "main",
            "selector": { "endpointReference": "urn:uuid:camera-a" }
        });
        assert!(AdapterConfig::from_core_initial(&core(missing)).is_err());

        let mut configured = valid_config();
        configured["component"]["global"]["discovery"] = json!({
            "eligibleInterfaces": ["eth0"]
        });
        configured["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "mediaProfile": "main",
            "selector": { "endpointReference": "urn:uuid:camera-a" }
        });
        assert!(AdapterConfig::from_core_initial(&core(configured)).is_ok());
    }

    #[test]
    fn discovery_interface_names_are_bounded_distinct_and_control_free() {
        for interfaces in [
            json!(["eth0", "eth0"]),
            json!(["eth0\n"]),
            json!(["x".repeat(257)]),
        ] {
            let mut value = valid_config();
            value["component"]["global"]["discovery"] = json!({
                "eligibleInterfaces": interfaces
            });
            assert!(AdapterConfig::from_core_initial(&core(value)).is_err());
        }
    }

    #[test]
    fn onvif_basic_over_plaintext_requires_both_explicit_permissions() {
        let mut missing_global_permission = valid_config();
        missing_global_permission["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "mediaProfile": "main",
            "deviceServiceUrl": "http://10.0.0.2/onvif/device_service",
            "authenticationMode": "basic",
            "allowInsecure": true
        });
        assert!(AdapterConfig::from_core_initial(&core(missing_global_permission)).is_err());

        let mut missing_camera_permission = valid_config();
        missing_camera_permission["component"]["global"]["security"] = json!({
            "allowBasicOverPlaintext": true
        });
        missing_camera_permission["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "mediaProfile": "main",
            "deviceServiceUrl": "http://10.0.0.2/onvif/device_service",
            "authenticationMode": "basic"
        });
        assert!(AdapterConfig::from_core_initial(&core(missing_camera_permission)).is_err());

        let mut explicitly_allowed = valid_config();
        explicitly_allowed["component"]["global"]["security"] = json!({
            "allowBasicOverPlaintext": true
        });
        explicitly_allowed["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "mediaProfile": "main",
            "deviceServiceUrl": "http://10.0.0.2/onvif/device_service",
            "authenticationMode": "basic",
            "allowInsecure": true
        });
        assert!(AdapterConfig::from_core_initial(&core(explicitly_allowed)).is_ok());
    }

    #[test]
    fn onvif_basic_over_tls_does_not_require_plaintext_permission() {
        let mut value = valid_config();
        value["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "mediaProfile": "main",
            "deviceServiceUrl": "https://10.0.0.2/onvif/device_service",
            "authenticationMode": "basic"
        });
        assert!(AdapterConfig::from_core_initial(&core(value)).is_ok());
    }

    #[test]
    fn onvif_hostname_verification_override_requires_insecure_gate() {
        let mut rejected = valid_config();
        rejected["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "mediaProfile": "main",
            "deviceServiceUrl": "https://10.0.0.2/onvif/device_service",
            "tls": { "verifyHostname": false }
        });
        assert!(AdapterConfig::from_core_initial(&core(rejected)).is_err());

        let mut explicitly_allowed = valid_config();
        explicitly_allowed["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "mediaProfile": "main",
            "deviceServiceUrl": "https://10.0.0.2/onvif/device_service",
            "allowInsecure": true,
            "tls": { "verifyHostname": false }
        });
        assert!(AdapterConfig::from_core_initial(&core(explicitly_allowed)).is_ok());
    }

    #[test]
    fn malformed_plaintext_credentials_never_reach_diagnostics() {
        let plaintext = "do-not-log-this-camera-password";
        let mut value = valid_config();
        value["component"]["instances"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "id": "camera-secret-error",
                "backend": {
                    "type": "onvif-rtsp",
                    "deviceServiceUrl": "https://camera.test/onvif/device_service",
                    "mediaProfile": "main",
                    "credentials": plaintext
                },
                "defaultCaptureProfile": "main",
                "captureProfiles": { "main": { "output": { "encoding": "png" } } }
            }));
        let loaded = AdapterConfig::from_core_initial(&core(value)).unwrap();
        assert_eq!(loaded.skipped.len(), 1);
        let diagnostic = &loaded.skipped[0].message;
        assert_eq!(diagnostic, safe_deserialization_message());
        assert!(!diagnostic.contains(plaintext));
    }

    #[test]
    fn secret_references_are_bounded_and_control_free() {
        for reference in [
            json!({ "$secret": "" }),
            json!({ "$secret": "camera\npassword" }),
            json!({ "$secret": "x".repeat(1_025) }),
            json!({ "$secret": "camera/login", "field": "" }),
            json!({ "$secret": "camera/login", "field": "x".repeat(257) }),
        ] {
            let mut value = valid_config();
            value["component"]["instances"][0]["backend"] = json!({
                "type": "onvif-rtsp",
                "deviceServiceUrl": "https://camera.test/onvif/device_service",
                "mediaProfile": "main",
                "credentials": reference
            });
            assert!(AdapterConfig::from_core_initial(&core(value)).is_err());
        }

        let mut valid = valid_config();
        valid["component"]["instances"][0]["backend"] = json!({
            "type": "onvif-rtsp",
            "deviceServiceUrl": "https://camera.test/onvif/device_service",
            "mediaProfile": "main",
            "credentials": { "$secret": "camera/login", "field": "password" },
            "tls": { "ca": { "$secret": "camera/ca" } }
        });
        assert!(AdapterConfig::from_core_initial(&core(valid)).is_ok());
    }

    #[cfg(not(feature = "rtsp"))]
    #[test]
    fn onvif_rtsp_requirements_fail_with_stable_feature_error_when_absent() {
        for (capture_mode, fallback) in [("rtsp-frame", false), ("snapshot-uri", true)] {
            let mut value = valid_config();
            value["component"]["instances"][0]["backend"] = json!({
                "type": "onvif-rtsp",
                "mediaProfile": "main",
                "deviceServiceUrl": "https://10.0.0.2/onvif/device_service",
                "captureMode": capture_mode,
                "rtspFallback": fallback
            });
            let error = AdapterConfig::from_core_reload(&core(value))
                .expect_err("RTSP-requiring config must fail without the feature");
            assert_eq!(
                error.to_string(),
                "configuration error at component.instances[0].backend.captureMode: rtsp-frame capture and snapshot fallback require the 'rtsp' build feature"
            );
        }
    }

    #[test]
    fn templates_reject_traversal_and_unknown_variables() {
        let mut traversal = valid_config();
        traversal["component"]["global"]["output"]["cameraDirectoryTemplate"] = json!("../x");
        assert!(AdapterConfig::from_core_initial(&core(traversal)).is_err());

        let mut unknown = valid_config();
        unknown["component"]["global"]["output"]["fileNameTemplate"] = json!("{secret}.png");
        assert!(AdapterConfig::from_core_initial(&core(unknown)).is_err());
    }

    #[test]
    fn global_bounds_and_cross_field_capacity_rules_fail_closed() {
        let cases: Vec<ConfigMutation> = vec![
            Box::new(|value| {
                value["component"]["global"]["output"]["rootDirectory"] = json!("relative");
            }),
            Box::new(|value| {
                value["component"]["global"]["state"] = json!({"resultRetentionHours": 0});
            }),
            Box::new(|value| {
                value["component"]["global"]["state"] =
                    json!({"resultRetentionHours": 24, "outboxRetentionHours": 1});
            }),
            Box::new(|value| {
                value["component"]["global"]["limits"] = json!({
                    "maxInFlightBytes": 67_108_864,
                    "maxFrameBytesPerCamera": 134_217_728
                });
            }),
            Box::new(|value| {
                value["component"]["global"]["limits"] = json!({
                    "resourceGroups": {"shared": {"maxConcurrentCaptures": 0}}
                });
            }),
            Box::new(|value| {
                value["component"]["global"]["security"] = json!({"maxHeaderBytes": 1});
            }),
            // The budget must cover the concurrency the component ADVERTISES. This is the exact
            // shape of the shipped defaults before the fix — 32 x 256 MiB declared, 1 GiB budgeted —
            // which admitted 4 captures while claiming 32.
            Box::new(|value| {
                value["component"]["global"]["limits"] = json!({
                    "maxConcurrentCaptures": 32,
                    "maxFrameBytesPerCamera": 268_435_456u64, // 256 MiB
                    "maxInFlightBytes": 1_073_741_824u64      // 1 GiB -> admits 4, not 32
                });
            }),
        ];

        for mutate in cases {
            let mut value = valid_config();
            mutate(&mut value);
            assert!(AdapterConfig::from_core_initial(&core(value)).is_err());
        }
    }

    /// The byte budget IS the concurrency limit, and a config that hides that must not start.
    ///
    /// A capture reserves `maxFrameBytesPerCamera` — the DECLARED cap, not the frame's real size —
    /// for the whole of its admission. So the true width of the system is
    /// `floor(maxInFlightBytes / maxFrameBytesPerCamera)`, and if that is smaller than
    /// `maxConcurrentCaptures` the component advertises a concurrency it cannot reach: the surplus
    /// captures take a semaphore permit, park inside the byte budget, and die on their deadline.
    /// Every one of those timeouts writes a durable row, so the failure also feeds the state DB.
    ///
    /// This is not a tuning nicety. It is why the shipped defaults claimed 32-way concurrency and
    /// delivered 4, and why a soak run would have measured a 4-wide system and reported it as 32.
    #[test]
    fn a_byte_budget_that_cannot_cover_the_advertised_concurrency_is_rejected() {
        let mut value = valid_config();
        value["component"]["global"]["limits"] = json!({
            "maxConcurrentCaptures": 8,
            "maxFrameBytesPerCamera": 64 * 1024 * 1024u64, // 64 MiB
            "maxInFlightBytes": 256 * 1024 * 1024u64       // room for 4 of them, not 8
        });
        let error = AdapterConfig::from_core_initial(&core(value))
            .expect_err("a budget that admits 4 must not advertise 8");
        let message = error.to_string();
        // The error has to say what is actually wrong — an operator who is told only "invalid"
        // will "fix" it by lowering something else and still get a 4-wide system.
        assert!(
            message.contains("maxConcurrentCaptures") && message.contains("4"),
            "the error must name the real admitted width so the operator can act on it: {message}"
        );
    }

    /// The shipped defaults must satisfy the rule they impose on everyone else.
    ///
    /// Guards against the fix being half-done: it would be no use rejecting an incoherent operator
    /// config while the out-of-the-box one is itself incoherent — which is precisely what shipped.
    #[test]
    fn the_default_limits_can_actually_reach_their_own_advertised_concurrency() {
        let limits = LimitsConfig::default();
        let admits = limits.max_in_flight_bytes / limits.max_frame_bytes_per_camera;
        assert!(
            admits >= limits.max_concurrent_captures as u64,
            "defaults advertise {} concurrent captures but the byte budget admits only {}",
            limits.max_concurrent_captures,
            admits
        );
        // And the default config must pass its own validator, or the component will not start.
        assert!(AdapterConfig::from_core_initial(&core(valid_config())).is_ok());
    }

    #[test]
    fn profiles_and_simulator_faults_enforce_cross_field_rules() {
        let cases: Vec<ConfigMutation> = vec![
            Box::new(|value| {
                value["component"]["instances"][0]["captureProfiles"]["main"]["offlinePolicy"] =
                    json!("queue");
            }),
            Box::new(|value| {
                value["component"]["instances"][0]["captureProfiles"]["main"] = json!({
                    "queueExpiryMs": 1_000,
                    "output": {"encoding": "png"}
                });
            }),
            Box::new(|value| {
                value["component"]["instances"][0]["captureProfiles"]["main"]["maximumFrameBytes"] =
                    json!(1);
            }),
            Box::new(|value| {
                value["component"]["instances"][0]["captureProfiles"]["main"]["output"] =
                    json!({"encoding": "png", "jpegQuality": 0});
            }),
            Box::new(|value| {
                value["component"]["instances"][0]["captureProfiles"]["main"]["captureMode"] =
                    json!("software-trigger");
            }),
            Box::new(|value| {
                value["component"]["instances"][0]["backend"] = json!({
                    "type": "sim", "faults": {"failEveryNthCapture": 0}
                });
            }),
            Box::new(|value| {
                value["component"]["instances"][0]["backend"] = json!({
                    "type": "sim", "frame": {"width": 0}
                });
            }),
        ];

        for mutate in cases {
            let mut value = valid_config();
            mutate(&mut value);
            assert!(AdapterConfig::from_core_initial(&core(value)).is_err());
        }
    }

    #[test]
    fn genicam_selector_transport_and_feature_allowlist_are_validated_before_startup() {
        let cases: Vec<ConfigMutation> = vec![
            Box::new(|value| {
                value["component"]["instances"][0]["backend"] =
                    json!({"type": "genicam-aravis", "selector": {}});
            }),
            Box::new(|value| {
                value["component"]["instances"][0]["backend"] = json!({
                    "type": "genicam-aravis",
                    "selector": {"mac": "zz:zz:zz:zz:zz:zz"}
                });
            }),
            Box::new(|value| {
                value["component"]["instances"][0]["backend"] = json!({
                    "type": "genicam-aravis",
                    "transport": "usb3-vision",
                    "interface": "eth0",
                    "selector": {"serial": "serial-1"}
                });
            }),
            Box::new(|value| {
                value["component"]["instances"][0]["backend"] = json!({
                    "type": "genicam-aravis",
                    "selector": {"serial": "serial-1"},
                    "packetSize": 0
                });
            }),
            Box::new(|value| {
                value["component"]["instances"][0]["backend"] = json!({
                    "type": "genicam-aravis",
                    "selector": {"serial": "serial-1"},
                    "featureOverrides": {"UnapprovedFeature": true}
                });
            }),
        ];

        for mutate in cases {
            let mut value = valid_config();
            mutate(&mut value);
            assert!(AdapterConfig::from_core_initial(&core(value)).is_err());
        }

        let mut valid = valid_config();
        valid["component"]["instances"][0]["backend"] = json!({
            "type": "genicam-aravis",
            "selector": {"serial": "serial-1"},
            "transport": "gige-vision",
            "packetSize": "auto",
            "bufferCount": 4,
            "featureOverrides": {
                "ReverseX": true,
                "AcquisitionFrameRate": 30.0,
                "BalanceWhiteAuto": "Continuous"
            }
        });
        assert!(AdapterConfig::from_core_initial(&core(valid)).is_ok());
    }

    #[test]
    fn windows_and_posix_absolute_paths_are_accepted_cross_platform() {
        assert!(is_absolute_cross_platform("/var/lib/camera"));
        assert!(is_absolute_cross_platform("C:\\captures"));
        assert!(is_absolute_cross_platform("\\\\server\\share\\captures"));
        assert!(!is_absolute_cross_platform("relative/captures"));
    }

    #[test]
    fn startup_records_bad_instances_but_reload_is_atomic_and_reports_the_first_error() {
        let mut value = valid_config();
        let duplicate = value["component"]["instances"][0].clone();
        value["component"]["instances"]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
        value["component"]["instances"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "id": "malformed-camera",
                "backend": {"type": "sim"},
                "defaultCaptureProfile": "main",
                "captureProfiles": {"main": {"output": {"encoding": "png"}}},
                "schedules": "not-an-array"
            }));

        let initial = AdapterConfig::from_core_initial(&core(value.clone())).unwrap();
        assert_eq!(initial.config.instances.len(), 1);
        assert_eq!(initial.skipped.len(), 2);
        assert_eq!(initial.skipped[0].path, "component.instances[1].id");
        assert_eq!(initial.skipped[1].path, "component.instances[2]");
        assert_eq!(
            reload_error_path(value),
            "component.instances[1].id",
            "reload must reject rather than silently retain the prior valid subset"
        );
    }

    #[test]
    fn global_validation_rejects_boundary_and_relationship_violations_with_stable_paths() {
        let cases: Vec<(&str, ConfigMutation, &str)> = vec![
            (
                "free-space percentage",
                Box::new(|value| {
                    value["component"]["global"]["output"]["minimumFreePercent"] = json!(101);
                }),
                "component.global.output.minimumFreePercent",
            ),
            (
                "directory special mode",
                Box::new(|value| {
                    value["component"]["global"]["output"]["directoryMode"] = json!("1750");
                }),
                "component.global.output.directoryMode",
            ),
            (
                "file execute mode",
                Box::new(|value| {
                    value["component"]["global"]["output"]["fileMode"] = json!("0755");
                }),
                "component.global.output.fileMode",
            ),
            (
                "state directory",
                Box::new(|value| {
                    value["component"]["global"]["state"] = json!({"directory": "relative"});
                }),
                "component.global.state.directory",
            ),
            (
                "result record lower bound",
                Box::new(|value| {
                    value["component"]["global"]["state"] = json!({"maxResultRecords": 999});
                }),
                "component.global.state.maxResultRecords",
            ),
            (
                "resource group token",
                Box::new(|value| {
                    value["component"]["global"]["limits"] = json!({
                        "resourceGroups": {"bad/group": {"maxConcurrentCaptures": 1}}
                    });
                }),
                "component.global.limits.resourceGroups",
            ),
            (
                "resource group capacity",
                Box::new(|value| {
                    value["component"]["global"]["limits"] = json!({
                        "maxConcurrentCaptures": 2,
                        "resourceGroups": {"shared": {"maxConcurrentCaptures": 3}}
                    });
                }),
                "component.global.limits.resourceGroups.shared.maxConcurrentCaptures",
            ),
            (
                "reconnect relationship",
                Box::new(|value| {
                    value["component"]["global"]["timeouts"] = json!({
                        "reconnectBackoffMinMs": 200,
                        "reconnectBackoffMaxMs": 199
                    });
                }),
                "timeouts.reconnectBackoffMaxMs",
            ),
            (
                "capture timeout lower bound",
                Box::new(|value| {
                    value["component"]["global"]["timeouts"] = json!({"captureMs": 99});
                }),
                "timeouts.captureMs",
            ),
            (
                "discovery interval",
                Box::new(|value| {
                    value["component"]["global"]["discovery"] = json!({"intervalSeconds": 4});
                }),
                "discovery.intervalSeconds",
            ),
            (
                "health threshold",
                Box::new(|value| {
                    value["component"]["global"]["healthThresholds"] =
                        json!({"staleSignalSecs": 0});
                }),
                "healthThresholds.staleSignalSecs",
            ),
            (
                "decompression ratio",
                Box::new(|value| {
                    value["component"]["global"]["security"] =
                        json!({"maxDecompressionRatio": 1_001});
                }),
                "security.maxDecompressionRatio",
            ),
        ];

        for (name, mutate, expected_path) in cases {
            let mut value = valid_config();
            mutate(&mut value);
            assert_eq!(reload_error_path(value), expected_path, "{name}");
        }
    }

    #[test]
    fn camera_profile_schedule_and_simulator_cross_field_rules_are_all_checked_on_reload() {
        let cases: Vec<(&str, ConfigMutation, &str)> = vec![
            (
                "missing default profile",
                Box::new(|value| {
                    value["component"]["instances"][0]["defaultCaptureProfile"] = json!("missing");
                }),
                "component.instances[0].defaultCaptureProfile",
            ),
            (
                "unknown resource group",
                Box::new(|value| {
                    value["component"]["instances"][0]["resourceGroup"] = json!("unconfigured");
                }),
                "component.instances[0].resourceGroup",
            ),
            (
                "invalid schedule cron",
                Box::new(|value| {
                    value["component"]["instances"][0]["schedules"] = json!([{
                        "id": "daily", "cron": "* * * * *", "timezone": "UTC", "captureProfile": "main"
                    }]);
                }),
                "component.instances[0].schedules[0].cron",
            ),
            (
                "invalid schedule timezone",
                Box::new(|value| {
                    value["component"]["instances"][0]["schedules"] = json!([{
                        "id": "daily", "cron": "0 * * * * *", "timezone": "not/a-zone", "captureProfile": "main"
                    }]);
                }),
                "component.instances[0].schedules[0].timezone",
            ),
            (
                "unknown schedule profile",
                Box::new(|value| {
                    value["component"]["instances"][0]["schedules"] = json!([{
                        "id": "daily", "cron": "0 * * * * *", "timezone": "UTC", "captureProfile": "missing"
                    }]);
                }),
                "component.instances[0].schedules[0].captureProfile",
            ),
            (
                "schedule jitter",
                Box::new(|value| {
                    value["component"]["instances"][0]["schedules"] = json!([{
                        "id": "daily", "cron": "0 * * * * *", "timezone": "UTC", "captureProfile": "main", "jitterSeconds": 3601
                    }]);
                }),
                "component.instances[0].schedules[0].jitterSeconds",
            ),
            (
                "ptz safety timeout",
                Box::new(|value| {
                    value["component"]["instances"][0]["ptz"] =
                        json!({"maximumContinuousMoveMs": 99});
                }),
                "component.instances[0].ptz.maximumContinuousMoveMs",
            ),
            (
                "profile gain",
                Box::new(|value| {
                    value["component"]["instances"][0]["captureProfiles"]["main"]["gain"] =
                        json!(-0.1);
                }),
                "component.instances[0].captureProfiles.main.gain",
            ),
            (
                "simulator frame size",
                Box::new(|value| {
                    value["component"]["instances"][0]["backend"] = json!({
                        "type": "sim", "frame": {"width": 10_000, "height": 10_000, "pixelFormat": "RGB8"}
                    });
                }),
                "component.instances[0].backend.frame",
            ),
        ];

        for (name, mutate, expected_path) in cases {
            let mut value = valid_config();
            mutate(&mut value);
            assert_eq!(reload_error_path(value), expected_path, "{name}");
        }
    }

    #[test]
    fn genicam_and_onvif_security_validators_reject_their_protocol_specific_edges() {
        let cases: Vec<(&str, ConfigMutation, &str)> = vec![
            (
                "multiple genicam selectors",
                Box::new(|value| {
                    value["component"]["instances"][0]["backend"] = json!({
                        "type": "genicam-aravis", "selector": {"serial": "one", "ip": "192.0.2.10"}
                    });
                }),
                "component.instances[0].backend.selector",
            ),
            (
                "invalid genicam ip",
                Box::new(|value| {
                    value["component"]["instances"][0]["backend"] = json!({
                        "type": "genicam-aravis", "selector": {"ip": "not-an-ip"}
                    });
                }),
                "component.instances[0].backend.selector.ip",
            ),
            (
                "usb genicam mac selector",
                Box::new(|value| {
                    value["component"]["instances"][0]["backend"] = json!({
                        "type": "genicam-aravis", "transport": "usb3-vision",
                        "selector": {"mac": "00:11:22:33:44:55"}
                    });
                }),
                "component.instances[0].backend.selector",
            ),
            (
                "onvif userinfo",
                Box::new(|value| {
                    value["component"]["instances"][0]["backend"] = json!({
                        "type": "onvif-rtsp", "mediaProfile": "main",
                        "deviceServiceUrl": "https://operator:secret@camera.test/onvif/device_service"
                    });
                }),
                "component.instances[0].backend.deviceServiceUrl",
            ),
            (
                "onvif unsupported scheme",
                Box::new(|value| {
                    value["component"]["instances"][0]["backend"] = json!({
                        "type": "onvif-rtsp", "mediaProfile": "main", "deviceServiceUrl": "ftp://camera.test/"
                    });
                }),
                "component.instances[0].backend.deviceServiceUrl",
            ),
            (
                "onvif soap bound",
                Box::new(|value| {
                    value["component"]["instances"][0]["backend"] = json!({
                        "type": "onvif-rtsp", "mediaProfile": "main",
                        "deviceServiceUrl": "https://camera.test/onvif/device_service", "maxSoapBytes": 4095
                    });
                }),
                "component.instances[0].backend.maxSoapBytes",
            ),
            (
                "onvif allowed uri host",
                Box::new(|value| {
                    value["component"]["instances"][0]["backend"] = json!({
                        "type": "onvif-rtsp", "mediaProfile": "main",
                        "deviceServiceUrl": "https://camera.test/onvif/device_service", "allowedUriHosts": ["user@camera.test"]
                    });
                }),
                "component.instances[0].backend.allowedUriHosts[0]",
            ),
        ];

        for (name, mutate, expected_path) in cases {
            let mut value = valid_config();
            mutate(&mut value);
            assert_eq!(reload_error_path(value), expected_path, "{name}");
        }
    }
}
