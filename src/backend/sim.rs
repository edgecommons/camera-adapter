//! Deterministic in-process camera backend.
//!
//! `SimBackend` is a production-configurable backend: its frames, timing, failures,
//! capabilities, PTZ state, and presets pass through the same runtime as physical cameras.
//! It allocates no image-sized buffer while idle.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use image::ExtendedColorType;
use image::codecs::jpeg::JpegEncoder;
use serde_json::json;

use super::{
    CameraBackendFactory, CameraSession, CameraStatus, CaptureRequest, ConnectRequest,
    DiscoveryCandidate, DiscoveryRequest,
};
use crate::config::{BackendConfig, SimBackendConfig, SimPattern};
use crate::error::{CameraError, ErrorCode, Result};
use crate::model::{
    BackendKind, CameraCapabilities, CaptureFrame, CaptureMode, FrameTimestampQuality, PixelFormat,
    PtzPreset, PtzRequest, PtzResult, PtzStatus, PtzVector,
};

/// Stateless factory for deterministic simulator sessions.
#[derive(Debug, Default)]
pub struct SimBackendFactory;

impl SimBackendFactory {
    /// Creates a simulator factory.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[async_trait]
impl CameraBackendFactory for SimBackendFactory {
    fn kind(&self) -> BackendKind {
        BackendKind::Sim
    }

    async fn discover(&self, _request: DiscoveryRequest) -> Result<Vec<DiscoveryCandidate>> {
        // Sim cameras are explicit config, never ambient discoveries.
        Ok(Vec::new())
    }

    async fn connect(&self, request: ConnectRequest) -> Result<Box<dyn CameraSession>> {
        let BackendConfig::Sim(config) = request.backend else {
            return Err(CameraError::Backend {
                backend: "sim",
                message: "factory received a non-sim backend config".to_string(),
            });
        };
        let delay = Duration::from_millis(config.connect_delay_ms);
        tokio::select! {
            () = request.cancellation.cancelled() => {
                return Err(CameraError::rejected(ErrorCode::CaptureCancelled, "sim connection cancelled"));
            }
            () = tokio::time::sleep(delay) => {}
        }
        Ok(Box::new(SimSession::new(request.instance_id, config)))
    }
}

struct SimSession {
    id: String,
    config: SimBackendConfig,
    capabilities: CameraCapabilities,
    capture_ordinal: u64,
    closed: bool,
    position: PtzVector,
    moving: bool,
    presets: BTreeMap<String, (Option<String>, PtzVector)>,
}

impl SimSession {
    fn new(instance_id: String, config: SimBackendConfig) -> Self {
        let id = config
            .simulated_id
            .clone()
            .unwrap_or_else(|| instance_id.clone());
        let ptz = config.ptz.supported;
        let presets = config.ptz.presets_supported;
        Self {
            id: id.clone(),
            capabilities: CameraCapabilities {
                capture_modes: vec![CaptureMode::Simulated],
                pixel_formats: vec![config.frame.pixel_format],
                software_trigger: false,
                snapshot_uri: false,
                rtsp: false,
                ptz,
                ptz_status: ptz && config.ptz.status_supported,
                presets: ptz && presets,
                preset_mutation: ptz && presets,
                vendor: Some("EdgeCommons".to_string()),
                model: Some("SimBackend".to_string()),
                firmware: Some(env!("CARGO_PKG_VERSION").to_string()),
                serial: Some(id),
                warnings: Vec::new(),
            },
            config,
            capture_ordinal: 0,
            closed: false,
            position: PtzVector {
                pan: 0.0,
                tilt: 0.0,
                zoom: 0.0,
            },
            moving: false,
            presets: BTreeMap::new(),
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(CameraError::rejected(
                ErrorCode::CameraUnavailable,
                "sim camera session is closed",
            ))
        } else {
            Ok(())
        }
    }

    fn ensure_ptz(&self) -> Result<()> {
        self.ensure_open()?;
        if !self.capabilities.ptz {
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "sim camera does not advertise PTZ",
            ));
        }
        Ok(())
    }

    fn should_fire(period: Option<u64>, ordinal: u64) -> bool {
        period.is_some_and(|period| ordinal % period == 0)
    }

    fn frame_bytes(&self, ordinal: u64, limit: u64) -> Result<Vec<u8>> {
        let frame = &self.config.frame;
        let raw_format = if frame.pixel_format == PixelFormat::Jpeg {
            PixelFormat::Rgb8
        } else {
            frame.pixel_format
        };
        let expected = raw_format
            .uncompressed_len(frame.width, frame.height)
            .ok_or_else(|| {
                CameraError::rejected(
                    ErrorCode::UnsupportedPixelFormat,
                    "unsupported simulator source format",
                )
            })?;
        if expected > limit || usize::try_from(expected).is_err() {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "simulated frame exceeds the accepted maximum",
            ));
        }
        let mut bytes = vec![0_u8; expected as usize];
        fill_pattern(
            &mut bytes,
            frame.width,
            frame.height,
            raw_format,
            frame.pattern,
            self.config.seed.unwrap_or_else(|| stable_seed(&self.id)),
            ordinal,
        );
        if frame.pixel_format != PixelFormat::Jpeg {
            return Ok(bytes);
        }
        let mut jpeg = Vec::new();
        JpegEncoder::new_with_quality(Cursor::new(&mut jpeg), 90)
            .encode(&bytes, frame.width, frame.height, ExtendedColorType::Rgb8)
            .map_err(|error| CameraError::Backend {
                backend: "sim",
                message: format!("JPEG generation failed: {error}"),
            })?;
        if jpeg.len() as u64 > limit {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "simulated JPEG exceeds the accepted maximum",
            ));
        }
        Ok(jpeg)
    }
}

#[async_trait]
impl CameraSession for SimSession {
    fn capabilities(&self) -> &CameraCapabilities {
        &self.capabilities
    }

    async fn status(&mut self) -> Result<CameraStatus> {
        self.ensure_open()?;
        let ptz = self.capabilities.ptz_status.then(|| PtzStatus {
            position: Some(self.position),
            moving: Some(self.moving),
            observed_at: Utc::now(),
        });
        Ok(CameraStatus {
            online: true,
            connection_generation: 1,
            ptz,
            backend: json!({ "simulatedId": self.id, "captureOrdinal": self.capture_ordinal }),
        })
    }

    async fn capture(&mut self, request: CaptureRequest) -> Result<CaptureFrame> {
        self.ensure_open()?;
        self.capture_ordinal = self.capture_ordinal.saturating_add(1);
        let ordinal = self.capture_ordinal;
        if self
            .config
            .faults
            .disconnect_after_captures
            .is_some_and(|count| ordinal > count)
        {
            self.closed = true;
            return Err(CameraError::rejected(
                ErrorCode::CameraUnavailable,
                "simulated disconnect threshold reached",
            ));
        }
        let delay = Duration::from_millis(self.config.capture_delay_ms);
        tokio::select! {
            () = request.cancellation.cancelled() => {
                return Err(CameraError::rejected(ErrorCode::CaptureCancelled, "sim capture cancelled"));
            }
            () = tokio::time::sleep(delay) => {}
        }
        if Self::should_fire(self.config.faults.fail_every_nth_capture, ordinal) {
            return Err(CameraError::Backend {
                backend: "sim",
                message: "configured deterministic capture failure".to_string(),
            });
        }
        let mut bytes = self.frame_bytes(ordinal, request.maximum_frame_bytes)?;
        if Self::should_fire(self.config.faults.incomplete_every_nth_capture, ordinal) {
            bytes.truncate(bytes.len().saturating_sub(1));
            return Err(CameraError::Backend {
                backend: "sim",
                message: "configured deterministic incomplete frame".to_string(),
            });
        }
        let now = Utc::now();
        Ok(CaptureFrame {
            bytes: Bytes::from(bytes),
            width: self.config.frame.width,
            height: self.config.frame.height,
            pixel_format: self.config.frame.pixel_format,
            capture_mode: CaptureMode::Simulated,
            source_timestamp: Some(now),
            timestamp_quality: FrameTimestampQuality::Camera,
            backend_metadata: BTreeMap::from([
                ("simulatedId".to_string(), json!(self.id)),
                ("captureOrdinal".to_string(), json!(ordinal)),
                ("captureId".to_string(), json!(request.capture_id)),
            ]),
        })
    }

    async fn ptz(&mut self, request: PtzRequest) -> Result<PtzResult> {
        self.ensure_ptz()?;
        match request {
            PtzRequest::Continuous { velocity, .. } => {
                if !velocity.validate_signed() {
                    return Err(CameraError::rejected(
                        ErrorCode::PtzRangeError,
                        "continuous velocity is outside [-1,1]",
                    ));
                }
                self.moving = velocity
                    != PtzVector {
                        pan: 0.0,
                        tilt: 0.0,
                        zoom: 0.0,
                    };
                Ok(PtzResult::Commanded)
            }
            PtzRequest::Absolute { position, speed } => {
                if !position.validate_absolute()
                    || speed.is_some_and(|value| !value.validate_signed())
                {
                    return Err(CameraError::rejected(
                        ErrorCode::PtzRangeError,
                        "absolute PTZ vector is outside the normalized range",
                    ));
                }
                self.position = position;
                self.moving = false;
                Ok(PtzResult::Commanded)
            }
            PtzRequest::Relative { translation, speed } => {
                if !translation.validate_signed()
                    || speed.is_some_and(|value| !value.validate_signed())
                {
                    return Err(CameraError::rejected(
                        ErrorCode::PtzRangeError,
                        "relative PTZ vector is outside the normalized range",
                    ));
                }
                self.position.pan = (self.position.pan + translation.pan).clamp(-1.0, 1.0);
                self.position.tilt = (self.position.tilt + translation.tilt).clamp(-1.0, 1.0);
                self.position.zoom = (self.position.zoom + translation.zoom).clamp(0.0, 1.0);
                self.moving = false;
                Ok(PtzResult::Commanded)
            }
            PtzRequest::Stop { .. } => {
                self.moving = false;
                Ok(PtzResult::Commanded)
            }
            PtzRequest::Home => {
                self.position = PtzVector {
                    pan: 0.0,
                    tilt: 0.0,
                    zoom: 0.0,
                };
                self.moving = false;
                Ok(PtzResult::Commanded)
            }
            PtzRequest::Status => {
                if !self.capabilities.ptz_status {
                    return Err(CameraError::rejected(
                        ErrorCode::UnsupportedCapability,
                        "sim PTZ status is disabled",
                    ));
                }
                Ok(PtzResult::Status(PtzStatus {
                    position: Some(self.position),
                    moving: Some(self.moving),
                    observed_at: Utc::now(),
                }))
            }
            PtzRequest::ListPresets => {
                if !self.capabilities.presets {
                    return Err(CameraError::rejected(
                        ErrorCode::UnsupportedCapability,
                        "sim presets are disabled",
                    ));
                }
                Ok(PtzResult::Presets(
                    self.presets
                        .iter()
                        .map(|(token, (name, _))| PtzPreset {
                            token: token.clone(),
                            name: name.clone(),
                        })
                        .collect(),
                ))
            }
            PtzRequest::GotoPreset(token) => {
                let (_, position) = self.presets.get(&token).ok_or_else(|| {
                    CameraError::rejected(ErrorCode::InvalidRequest, "unknown preset token")
                })?;
                self.position = *position;
                self.moving = false;
                Ok(PtzResult::Commanded)
            }
            PtzRequest::SetPreset(name) => {
                if !self.capabilities.preset_mutation {
                    return Err(CameraError::rejected(
                        ErrorCode::UnsupportedCapability,
                        "sim preset mutation is disabled",
                    ));
                }
                let token = format!("preset-{}", self.presets.len() + 1);
                self.presets
                    .insert(token.clone(), (Some(name), self.position));
                Ok(PtzResult::PresetToken(token))
            }
            PtzRequest::RemovePreset(token) => {
                if !self.capabilities.preset_mutation {
                    return Err(CameraError::rejected(
                        ErrorCode::UnsupportedCapability,
                        "sim preset mutation is disabled",
                    ));
                }
                if self.presets.remove(&token).is_none() {
                    return Err(CameraError::rejected(
                        ErrorCode::InvalidRequest,
                        "unknown preset token",
                    ));
                }
                Ok(PtzResult::Removed)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.moving = false;
        self.closed = true;
        Ok(())
    }
}

fn stable_seed(value: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(value.as_bytes());
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(prefix)
}

fn fill_pattern(
    bytes: &mut [u8],
    width: u32,
    height: u32,
    format: PixelFormat,
    pattern: SimPattern,
    seed: u64,
    ordinal: u64,
) {
    let channels = if format == PixelFormat::Mono8 { 1 } else { 3 };
    for y in 0..height {
        for x in 0..width {
            let rgb = pixel(pattern, x, y, width, height, seed, ordinal);
            let offset = ((u64::from(y) * u64::from(width) + u64::from(x)) * channels) as usize;
            match format {
                PixelFormat::Mono8 => {
                    bytes[offset] =
                        ((u16::from(rgb[0]) + u16::from(rgb[1]) + u16::from(rgb[2])) / 3) as u8;
                }
                PixelFormat::Rgb8 => bytes[offset..offset + 3].copy_from_slice(&rgb),
                PixelFormat::Bgr8 => {
                    bytes[offset..offset + 3].copy_from_slice(&[rgb[2], rgb[1], rgb[0]])
                }
                PixelFormat::Jpeg => unreachable!("JPEG pattern generation uses RGB8"),
            }
        }
    }
}

fn pixel(
    pattern: SimPattern,
    x: u32,
    y: u32,
    width: u32,
    height: u32,
    seed: u64,
    ordinal: u64,
) -> [u8; 3] {
    match pattern {
        SimPattern::ColorBars => {
            const COLORS: [[u8; 3]; 8] = [
                [255, 255, 255],
                [255, 255, 0],
                [0, 255, 255],
                [0, 255, 0],
                [255, 0, 255],
                [255, 0, 0],
                [0, 0, 255],
                [0, 0, 0],
            ];
            let index = ((u64::from(x) * COLORS.len() as u64) / u64::from(width.max(1))) as usize;
            COLORS[index.min(COLORS.len() - 1)]
        }
        SimPattern::Gradient => [
            (u64::from(x) * 255 / u64::from(width.max(1))) as u8,
            (u64::from(y) * 255 / u64::from(height.max(1))) as u8,
            ordinal.wrapping_add(seed) as u8,
        ],
        SimPattern::Checkerboard => {
            let light = ((x / 16) + (y / 16) + ordinal as u32) % 2 == 0;
            if light { [230, 230, 230] } else { [25, 25, 25] }
        }
        SimPattern::Solid => {
            let value = seed.wrapping_add(ordinal);
            [value as u8, (value >> 8) as u8, (value >> 16) as u8]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CaptureProfile;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    fn backend(value: serde_json::Value) -> BackendConfig {
        serde_json::from_value(value).unwrap()
    }

    fn profile() -> CaptureProfile {
        serde_json::from_value(json!({"output":{"encoding":"png"}})).unwrap()
    }

    async fn session(value: serde_json::Value) -> Box<dyn CameraSession> {
        SimBackendFactory::new()
            .connect(ConnectRequest {
                instance_id: "cam-a".to_string(),
                backend: backend(value),
                timeout: Duration::from_secs(1),
                cancellation: CancellationToken::new(),
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn deterministic_frames_repeat_for_new_session() {
        let config = json!({"type":"sim","seed":7,"frame":{"width":16,"height":8,"pixelFormat":"RGB8","pattern":"gradient"}});
        let mut first = session(config.clone()).await;
        let mut second = session(config).await;
        let request = || CaptureRequest {
            capture_id: "cap-1".to_string(),
            profile: profile(),
            maximum_frame_bytes: 1_000_000,
            timeout: Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        };
        assert_eq!(
            first.capture(request()).await.unwrap().bytes,
            second.capture(request()).await.unwrap().bytes
        );
    }

    #[tokio::test]
    async fn nth_failure_is_deterministic() {
        let mut camera = session(json!({"type":"sim","faults":{"failEveryNthCapture":2}})).await;
        let request = || CaptureRequest {
            capture_id: "cap".to_string(),
            profile: profile(),
            maximum_frame_bytes: 1_000_000,
            timeout: Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        };
        assert!(camera.capture(request()).await.is_ok());
        assert_eq!(
            camera.capture(request()).await.unwrap_err().code(),
            ErrorCode::BackendError
        );
    }

    #[tokio::test]
    async fn ptz_ranges_and_presets_are_enforced() {
        let mut camera = session(json!({"type":"sim","ptz":{"supported":true,"statusSupported":true,"presetsSupported":true}})).await;
        let token = match camera
            .ptz(PtzRequest::SetPreset("home-ish".to_string()))
            .await
            .unwrap()
        {
            PtzResult::PresetToken(token) => token,
            other => panic!("unexpected result: {other:?}"),
        };
        assert!(matches!(
            camera.ptz(PtzRequest::GotoPreset(token)).await.unwrap(),
            PtzResult::Commanded
        ));
        let invalid = PtzVector {
            pan: 2.0,
            tilt: 0.0,
            zoom: 0.0,
        };
        assert_eq!(
            camera
                .ptz(PtzRequest::Continuous {
                    velocity: invalid,
                    timeout: Duration::from_secs(1)
                })
                .await
                .unwrap_err()
                .code(),
            ErrorCode::PtzRangeError
        );
    }

    #[tokio::test]
    async fn cancellation_prevents_frame_allocation_completion() {
        let mut camera = session(json!({"type":"sim","captureDelayMs":100})).await;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = camera
            .capture(CaptureRequest {
                capture_id: "cap".to_string(),
                profile: profile(),
                maximum_frame_bytes: 1_000_000,
                timeout: Duration::from_secs(1),
                cancellation,
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::CaptureCancelled);
    }

    #[tokio::test]
    async fn disconnect_and_incomplete_faults_do_not_report_a_successful_frame() {
        let mut incomplete =
            session(json!({"type":"sim","faults":{"incompleteEveryNthCapture":1}})).await;
        let request = || CaptureRequest {
            capture_id: "cap-fault".to_string(),
            profile: profile(),
            maximum_frame_bytes: 1_000_000,
            timeout: Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        };
        assert!(matches!(
            incomplete.capture(request()).await.unwrap_err().code(),
            ErrorCode::BackendError | ErrorCode::CameraUnavailable
        ));
        let mut disconnect =
            session(json!({"type":"sim","faults":{"disconnectAfterCaptures":0}})).await;
        assert_eq!(
            disconnect.capture(request()).await.unwrap_err().code(),
            ErrorCode::CameraUnavailable
        );
        disconnect.close().await.unwrap();
        assert_eq!(
            disconnect.capture(request()).await.unwrap_err().code(),
            ErrorCode::CameraUnavailable
        );
    }

    #[tokio::test]
    async fn factory_discovery_and_connection_failures_remain_bounded() {
        let factory = SimBackendFactory::new();
        assert_eq!(factory.kind(), BackendKind::Sim);
        assert!(
            factory
                .discover(DiscoveryRequest {
                    eligible_interfaces: vec!["camera-net".to_owned()],
                    timeout: Duration::from_millis(10),
                    max_results: 8,
                    cancellation: CancellationToken::new(),
                })
                .await
                .expect("simulator discovery is explicitly empty")
                .is_empty()
        );

        let wrong_backend = match factory
            .connect(ConnectRequest {
                instance_id: "cam-a".to_owned(),
                backend: backend(json!({
                    "type": "onvif-rtsp",
                    "deviceServiceUrl": "https://camera.example/onvif/device_service",
                    "mediaProfile": "main"
                })),
                timeout: Duration::from_secs(1),
                cancellation: CancellationToken::new(),
            })
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("a simulator factory cannot silently accept another backend config"),
        };
        assert_eq!(wrong_backend.code(), ErrorCode::BackendError);

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let cancelled = match factory
            .connect(ConnectRequest {
                instance_id: "cam-a".to_owned(),
                backend: backend(json!({"type": "sim", "connectDelayMs": 10})),
                timeout: Duration::from_secs(1),
                cancellation,
            })
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("cancelled connection must not create a live session"),
        };
        assert_eq!(cancelled.code(), ErrorCode::CaptureCancelled);
    }

    #[tokio::test]
    async fn simulator_emits_declared_raw_and_jpeg_formats_with_frame_bounds() {
        let request = || CaptureRequest {
            capture_id: "format-check".to_owned(),
            profile: profile(),
            maximum_frame_bytes: 1_000_000,
            timeout: Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        };
        for (pixel_format, expected_length) in [("Mono8", 12_usize), ("BGR8", 36_usize)] {
            let mut camera = session(json!({
                "type": "sim",
                "frame": {"width": 4, "height": 3, "pixelFormat": pixel_format, "pattern": "checkerboard"}
            }))
            .await;
            let frame = camera.capture(request()).await.expect("bounded raw frame");
            assert_eq!(frame.bytes.len(), expected_length);
            assert_eq!(frame.backend_metadata["captureId"], "format-check");
        }

        let mut jpeg = session(json!({
            "type": "sim",
            "frame": {"width": 16, "height": 8, "pixelFormat": "JPEG", "pattern": "color-bars"}
        }))
        .await;
        let frame = jpeg.capture(request()).await.expect("bounded JPEG frame");
        assert!(frame.bytes.starts_with(&[0xff, 0xd8]));
        assert!(frame.bytes.ends_with(&[0xff, 0xd9]));

        let mut oversized = session(json!({
            "type": "sim",
            "frame": {"width": 4, "height": 3, "pixelFormat": "RGB8"}
        }))
        .await;
        let error = oversized
            .capture(CaptureRequest {
                maximum_frame_bytes: 35,
                ..request()
            })
            .await
            .expect_err("frame ceiling is checked before allocation");
        assert_eq!(error.code(), ErrorCode::ResourceLimit);
    }

    #[tokio::test]
    async fn simulator_ptz_state_reports_motion_clamps_relative_moves_and_manages_presets() {
        let mut camera = session(json!({
            "type": "sim",
            "ptz": {"supported": true, "statusSupported": true, "presetsSupported": true}
        }))
        .await;
        let fast_pan = PtzVector {
            pan: 1.0,
            tilt: 0.0,
            zoom: 0.0,
        };
        assert!(matches!(
            camera
                .ptz(PtzRequest::Continuous {
                    velocity: fast_pan,
                    timeout: Duration::from_secs(1),
                })
                .await
                .expect("valid continuous PTZ command"),
            PtzResult::Commanded
        ));
        let moving = match camera.ptz(PtzRequest::Status).await.expect("PTZ status") {
            PtzResult::Status(status) => status,
            other => panic!("unexpected PTZ result: {other:?}"),
        };
        assert_eq!(moving.moving, Some(true));

        camera
            .ptz(PtzRequest::Absolute {
                position: PtzVector {
                    pan: 0.8,
                    tilt: -0.8,
                    zoom: 0.8,
                },
                speed: Some(PtzVector {
                    pan: 0.5,
                    tilt: 0.5,
                    zoom: 0.5,
                }),
            })
            .await
            .expect("valid absolute PTZ command");
        camera
            .ptz(PtzRequest::Relative {
                translation: PtzVector {
                    pan: 0.5,
                    tilt: -0.5,
                    zoom: 0.5,
                },
                speed: None,
            })
            .await
            .expect("valid relative PTZ command");
        let positioned = match camera.ptz(PtzRequest::Status).await.expect("PTZ status") {
            PtzResult::Status(status) => status,
            other => panic!("unexpected PTZ result: {other:?}"),
        };
        assert_eq!(
            positioned.position,
            Some(PtzVector {
                pan: 1.0,
                tilt: -1.0,
                zoom: 1.0,
            })
        );
        assert_eq!(positioned.moving, Some(false));

        let token = match camera
            .ptz(PtzRequest::SetPreset("production".to_owned()))
            .await
            .expect("preset mutation")
        {
            PtzResult::PresetToken(token) => token,
            other => panic!("unexpected PTZ result: {other:?}"),
        };
        assert_eq!(
            camera
                .ptz(PtzRequest::ListPresets)
                .await
                .expect("preset list"),
            PtzResult::Presets(vec![PtzPreset {
                token: token.clone(),
                name: Some("production".to_owned()),
            }])
        );
        assert_eq!(
            camera
                .ptz(PtzRequest::RemovePreset(token.clone()))
                .await
                .expect("existing preset removal"),
            PtzResult::Removed
        );
        assert_eq!(
            camera
                .ptz(PtzRequest::RemovePreset(token))
                .await
                .expect_err("removed preset cannot be removed twice")
                .code(),
            ErrorCode::InvalidRequest
        );
        camera.ptz(PtzRequest::Home).await.expect("home command");
        assert!(matches!(
            camera
                .ptz(PtzRequest::Stop {
                    pan: true,
                    tilt: true,
                    zoom: true,
                })
                .await
                .expect("stop command"),
            PtzResult::Commanded
        ));
    }

    #[tokio::test]
    async fn simulator_rejects_unsupported_ptz_operations_and_invalid_vectors() {
        let mut no_ptz = session(json!({"type": "sim"})).await;
        for request in [PtzRequest::Status, PtzRequest::ListPresets] {
            assert_eq!(
                no_ptz
                    .ptz(request)
                    .await
                    .expect_err("capability-gated PTZ request")
                    .code(),
                ErrorCode::UnsupportedCapability
            );
        }

        let mut ptz_without_status_or_presets = session(json!({
            "type": "sim",
            "ptz": {"supported": true, "statusSupported": false, "presetsSupported": false}
        }))
        .await;
        assert_eq!(
            ptz_without_status_or_presets
                .ptz(PtzRequest::Status)
                .await
                .expect_err("status capability is explicit")
                .code(),
            ErrorCode::UnsupportedCapability
        );
        assert_eq!(
            ptz_without_status_or_presets
                .ptz(PtzRequest::SetPreset("unavailable".to_owned()))
                .await
                .expect_err("preset mutation capability is explicit")
                .code(),
            ErrorCode::UnsupportedCapability
        );
        assert_eq!(
            ptz_without_status_or_presets
                .ptz(PtzRequest::Absolute {
                    position: PtzVector {
                        pan: 0.0,
                        tilt: 0.0,
                        zoom: -0.1,
                    },
                    speed: None,
                })
                .await
                .expect_err("absolute zoom must be non-negative")
                .code(),
            ErrorCode::PtzRangeError
        );
        assert_eq!(
            ptz_without_status_or_presets
                .ptz(PtzRequest::Relative {
                    translation: PtzVector {
                        pan: 0.0,
                        tilt: 0.0,
                        zoom: 0.0,
                    },
                    speed: Some(PtzVector {
                        pan: 2.0,
                        tilt: 0.0,
                        zoom: 0.0,
                    }),
                })
                .await
                .expect_err("relative speed shares the signed normalized range")
                .code(),
            ErrorCode::PtzRangeError
        );
    }
}
