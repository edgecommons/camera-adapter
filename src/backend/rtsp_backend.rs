//! Bare-RTSP still-image backend for a camera addressed by a raw stream URL.
//!
//! Unlike the ONVIF backend, this one performs no SOAP control: there is no device service, no
//! media-profile discovery, and no PTZ or snapshot capability. It reuses the shared RTSP engine
//! (`RtspCaptureController`) and the shared network/credential primitives in [`super::net`], so the
//! network-trust, credential, and TLS surfaces stay identical to the ONVIF RTSP path.
//!
//! `connect()` is the reachability probe: the supervisor treats a successful connect as ONLINE, so
//! it resolves credentials, builds the network trust anchor, and drives the full RTSP
//! DESCRIBE/SETUP + auth + SDP/codec validation via [`RtspCaptureController::establish`]. A dead
//! URL, bad authentication, or an unsupported codec therefore fails at connect time.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::net::{
    AddressResolver, CredentialProvider, NetClock, NonceSource, RtspNetworkAnchor, SecretBytes,
    SystemNetClock, SystemNonceSource, SystemResolver, normalize_host_text, resolve_bytes_bounded,
    resolve_login_bounded,
};
use super::rtsp::{RtspCaptureController, RtspControllerConfig};
use super::{
    CameraBackendFactory, CameraSession, CameraStatus, CaptureRequest, ConnectRequest,
    DiscoveryCandidate, DiscoveryRequest,
};
use crate::config::{BackendConfig, RtspBackendConfig, RtspSessionPolicy, SecurityConfig};
use crate::error::{CameraError, ErrorCode, Result};
use crate::model::{
    BackendKind, CameraCapabilities, CaptureFrame, CaptureMode, PixelFormat, PtzRequest, PtzResult,
};

const RTSP_BACKEND: &str = "rtsp";

fn backend_error(message: impl Into<String>) -> CameraError {
    CameraError::Backend {
        backend: RTSP_BACKEND,
        message: message.into(),
    }
}

/// A bounded, best-effort close deadline for a controller torn down by the session actor.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Factory for bare-RTSP sessions.
///
/// It holds the shared credential provider and the component-wide decode gate, plus the current
/// global security policy that bounds every RTSP transfer.
pub struct RtspBackendFactory {
    credentials: Option<Arc<dyn CredentialProvider>>,
    security: SecurityConfig,
    decode_gate: Arc<Semaphore>,
    resolver: Arc<dyn AddressResolver>,
    clock: Arc<dyn NetClock>,
    nonce_source: Arc<dyn NonceSource>,
}

impl RtspBackendFactory {
    /// Creates a factory bound to the current security policy and shared decode gate.
    #[must_use]
    pub fn new(
        credentials: Option<Arc<dyn CredentialProvider>>,
        security: SecurityConfig,
        decode_gate: Arc<Semaphore>,
    ) -> Self {
        Self {
            credentials,
            security,
            decode_gate,
            resolver: Arc::new(SystemResolver),
            clock: Arc::new(SystemNetClock),
            nonce_source: Arc::new(SystemNonceSource),
        }
    }
}

impl std::fmt::Debug for RtspBackendFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RtspBackendFactory")
            .field("has_credentials", &self.credentials.is_some())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CameraBackendFactory for RtspBackendFactory {
    fn kind(&self) -> BackendKind {
        BackendKind::Rtsp
    }

    async fn discover(&self, _request: DiscoveryRequest) -> Result<Vec<DiscoveryCandidate>> {
        // A bare RTSP camera is explicit configuration; it is never an ambient discovery.
        Ok(Vec::new())
    }

    async fn connect(&self, request: ConnectRequest) -> Result<Box<dyn CameraSession>> {
        if request.timeout.is_zero() {
            return Err(CameraError::rejected(
                ErrorCode::CaptureTimeout,
                "RTSP connection exceeded its deadline",
            ));
        }
        let BackendConfig::Rtsp(config) = request.backend else {
            return Err(backend_error("factory received a non-rtsp backend config"));
        };
        if !config.tls.verify_hostname && !config.allow_insecure {
            return Err(backend_error(
                "tls.verifyHostname=false requires allowInsecure=true",
            ));
        }
        let deadline = Instant::now() + request.timeout;

        // Resolve the credential and CA references through the SAME bounded EdgeCommons path the
        // ONVIF factory uses, so a missing provider or a hanging store fails the connect rather than
        // leaking into the RTSP transport.
        let credentials = match config.credentials.as_ref() {
            Some(reference) => Some(
                resolve_login_bounded(
                    self.credentials.as_deref().ok_or_else(|| CameraError::Config {
                        path: "component.credentials".to_owned(),
                        message: "RTSP secret references require EdgeCommons credentials".to_owned(),
                    })?,
                    reference,
                    deadline,
                    &request.cancellation,
                )
                .await?,
            ),
            None => None,
        };
        let private_ca: Option<Arc<SecretBytes>> = match config.tls.ca.as_ref() {
            Some(reference) => Some(
                resolve_bytes_bounded(
                    self.credentials.as_deref().ok_or_else(|| CameraError::Config {
                        path: "component.credentials".to_owned(),
                        message: "RTSP secret references require EdgeCommons credentials".to_owned(),
                    })?,
                    reference,
                    deadline,
                    &request.cancellation,
                )
                .await?,
            ),
            None => None,
        };

        // Build the network trust anchor from the configured stream host, its resolved addresses,
        // and the operator allowlists. `RtspCaptureController::establish` re-resolves and re-pins
        // against this anchor, so the camera's own resolved addresses are the only implicit
        // exception to the forbidden-address and allowed-CIDR policy.
        let anchor = self
            .build_anchor(&config, deadline, &request.cancellation)
            .await?;
        let source_host = anchor.configured_host.clone();

        let controller = RtspCaptureController::establish(
            RtspControllerConfig {
                stream_uri: config.url.clone(),
                anchor,
                resolver: Arc::clone(&self.resolver),
                credentials,
                nonce_source: Arc::clone(&self.nonce_source),
                private_ca,
                verify_hostname: config.tls.verify_hostname,
                allow_insecure: config.allow_insecure,
                authentication_mode: config.authentication_mode,
                security: self.security.clone(),
                session_policy: config.rtsp_session_policy,
                maximum_frame_bytes: config.max_frame_bytes,
                clock: Arc::clone(&self.clock),
                decode_gate: Arc::clone(&self.decode_gate),
            },
            deadline,
            &request.cancellation,
        )
        .await?;

        Ok(Box::new(RtspSession::new(
            controller,
            source_host,
            config.rtsp_session_policy,
        )))
    }
}

impl RtspBackendFactory {
    /// Resolves the stream host and copies the operator allowlist into an immutable trust anchor.
    async fn build_anchor(
        &self,
        config: &RtspBackendConfig,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<RtspNetworkAnchor> {
        let url = Url::parse(&config.url)
            .map_err(|_| backend_error("configured RTSP URL could not be parsed"))?;
        let host = normalize_host_text(
            url.host_str()
                .ok_or_else(|| backend_error("configured RTSP URL has no host"))?,
        )?;
        let port = url.port().unwrap_or(match url.scheme() {
            "rtsps" => 322,
            _ => 554,
        });
        let addresses = self
            .resolve_bounded(&host, port, deadline, cancellation)
            .await?;
        let allowed_hosts = config
            .allowed_uri_hosts
            .iter()
            .map(|value| normalize_host_text(value))
            .collect::<Result<BTreeSet<_>>>()?;
        Ok(RtspNetworkAnchor {
            configured_host: host,
            endpoint_addresses: addresses.into_iter().collect(),
            allowed_hosts,
            allowed_cidrs: config.allowed_uri_cidrs.clone(),
        })
    }

    /// Bounds the pre-flight DNS resolution by the connection deadline and cancellation signal.
    async fn resolve_bounded(
        &self,
        host: &str,
        port: u16,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<std::net::IpAddr>> {
        if deadline <= Instant::now() {
            return Err(CameraError::rejected(
                ErrorCode::CaptureTimeout,
                "RTSP DNS resolution exceeded its deadline",
            ));
        }
        let resolution = self.resolver.resolve(host, port);
        tokio::pin!(resolution);
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(CameraError::rejected(
                ErrorCode::CaptureCancelled,
                "RTSP DNS resolution was cancelled",
            )),
            () = tokio::time::sleep_until(deadline) => Err(CameraError::rejected(
                ErrorCode::CaptureTimeout,
                "RTSP DNS resolution exceeded its deadline",
            )),
            result = &mut resolution => result,
        }
    }
}

/// One live bare-RTSP session, owning the established capture controller.
struct RtspSession {
    controller: RtspCaptureController,
    capabilities: CameraCapabilities,
    source_host: String,
    session_policy: RtspSessionPolicy,
    closed: bool,
}

impl RtspSession {
    fn new(
        controller: RtspCaptureController,
        source_host: String,
        session_policy: RtspSessionPolicy,
    ) -> Self {
        Self {
            controller,
            capabilities: CameraCapabilities {
                capture_modes: vec![CaptureMode::RtspFrame],
                pixel_formats: vec![PixelFormat::Rgb8],
                software_trigger: false,
                snapshot_uri: false,
                rtsp: true,
                ptz: false,
                ptz_status: false,
                presets: false,
                preset_mutation: false,
                vendor: None,
                model: None,
                firmware: None,
                serial: None,
                warnings: Vec::new(),
            },
            source_host,
            session_policy,
            closed: false,
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(CameraError::rejected(
                ErrorCode::CameraUnavailable,
                "RTSP camera session is closed",
            ))
        } else {
            Ok(())
        }
    }

    fn session_policy_label(&self) -> &'static str {
        match self.session_policy {
            RtspSessionPolicy::OnDemand => "on-demand",
            RtspSessionPolicy::Warm => "warm",
        }
    }
}

#[async_trait]
impl CameraSession for RtspSession {
    fn capabilities(&self) -> &CameraCapabilities {
        &self.capabilities
    }

    async fn status(&mut self) -> Result<CameraStatus> {
        self.ensure_open()?;
        Ok(CameraStatus {
            online: true,
            connection_generation: 1,
            ptz: None,
            backend: json!({
                "sourceHost": self.source_host,
                "sessionPolicy": self.session_policy_label(),
                "captureMode": "rtsp-frame",
            }),
        })
    }

    async fn capture(&mut self, request: CaptureRequest) -> Result<CaptureFrame> {
        self.ensure_open()?;
        let mut frame = self
            .controller
            .capture(
                request.maximum_frame_bytes,
                request.timeout,
                &request.cancellation,
            )
            .await?;
        frame
            .backend_metadata
            .insert("captureId".to_owned(), json!(request.capture_id));
        Ok(frame)
    }

    async fn ptz_bounded(
        &mut self,
        _request: PtzRequest,
        _deadline: Instant,
        _cancellation: &CancellationToken,
    ) -> Result<PtzResult> {
        // The bare RTSP backend advertises no PTZ. Every request, including the actor's shutdown
        // Stop, is answered with the same unsupported-capability rejection rather than an error the
        // caller must special-case.
        Err(CameraError::rejected(
            ErrorCode::UnsupportedCapability,
            "the bare RTSP backend does not support PTZ",
        ))
    }

    async fn close(&mut self) -> Result<()> {
        if !self.closed {
            self.closed = true;
            self.controller
                .close(Instant::now() + CLOSE_TIMEOUT)
                .await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::json;
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;

    use super::RtspBackendFactory;
    use crate::backend::{CameraBackendFactory, CaptureRequest, ConnectRequest};
    use crate::config::{BackendConfig, CaptureProfile, SecurityConfig};
    use crate::model::{CaptureMode, PixelFormat};

    /// Drives the bare-RTSP backend end to end against the pinned MediaMTX simulator: `connect()`
    /// performs the live RTSP `DESCRIBE`/`SETUP` handshake and codec check, and `capture()` returns
    /// one complete RGB frame. Invoke with `CAMERA_ADAPTER_RTSP_URI` pointed at a pinned MediaMTX
    /// path (e.g. `rtsp://mediamtx:8554/camera`); ordinary unit tests stay hermetic.
    #[tokio::test]
    #[ignore = "requires the pinned MediaMTX simulator on the Compose network"]
    async fn pinned_mediamtx_backend_captures_a_complete_rgb_frame() {
        let stream_uri = std::env::var("CAMERA_ADAPTER_RTSP_URI")
            .expect("live RTSP backend test requires CAMERA_ADAPTER_RTSP_URI");
        let backend: BackendConfig = serde_json::from_value(json!({
            "type": "rtsp",
            "url": stream_uri,
            "allowInsecure": true,
        }))
        .expect("valid rtsp backend config");
        // The fixture's efficient H.265 IDR can exceed the production default ratio; raise it to the
        // documented, operator-configurable maximum, exactly as the engine fixture test does.
        let security = SecurityConfig {
            max_decompression_ratio: 1_000,
            ..SecurityConfig::default()
        };
        let factory = RtspBackendFactory::new(None, security, Arc::new(Semaphore::new(4)));

        let mut session = factory
            .connect(ConnectRequest {
                instance_id: "rtsp-cam".to_string(),
                backend,
                timeout: Duration::from_secs(10),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("connect establishes the live RTSP session");

        let capabilities = session.capabilities();
        assert_eq!(capabilities.capture_modes, vec![CaptureMode::RtspFrame]);
        assert!(capabilities.rtsp);
        assert!(!capabilities.ptz && !capabilities.snapshot_uri);

        let profile: CaptureProfile =
            serde_json::from_value(json!({ "output": { "encoding": "png" } })).unwrap();
        let frame = session
            .capture(CaptureRequest {
                capture_id: "cap-live-1".to_string(),
                profile,
                maximum_frame_bytes: 1_048_576,
                timeout: Duration::from_secs(15),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("a complete decoded RTSP frame");

        assert_eq!(frame.capture_mode, CaptureMode::RtspFrame);
        assert_eq!(frame.pixel_format, PixelFormat::Rgb8);
        assert_eq!((frame.width, frame.height), (320, 240));
        assert_eq!(frame.bytes.len(), 320 * 240 * 3);
        assert_eq!(
            frame.backend_metadata.get("captureId").and_then(|v| v.as_str()),
            Some("cap-live-1"),
        );

        session.close().await.expect("idempotent close");
        session.close().await.expect("close is idempotent");
    }
}
