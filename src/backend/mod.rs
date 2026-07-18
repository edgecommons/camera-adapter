//! Protocol backend abstraction and built-in backend factories.
//!
//! Backends know camera protocols and return bounded frames/capabilities. They do not
//! know EdgeCommons topics, SQLite, scheduling, file paths, or terminal messages.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use edgecommons::credentials::CredentialService;

use crate::config::{AdapterConfig, BackendConfig, CaptureProfile, GlobalConfig, LimitsConfig};
use crate::error::Result;
use crate::model::{
    BackendKind, CameraCapabilities, CaptureFrame, PtzRequest, PtzResult, PtzStatus,
};

#[cfg(feature = "genicam")]
pub mod genicam_aravis;
#[cfg(feature = "onvif")]
pub mod onvif;
#[cfg(feature = "rtsp")]
pub mod rtsp;
#[cfg(feature = "rtsp")]
pub mod rtsp_backend;
#[cfg(any(feature = "onvif", feature = "rtsp"))]
pub(crate) mod net;
#[cfg(all(test, feature = "onvif", not(feature = "rtsp")))]
#[path = "rtsp.rs"]
mod rtsp_contract_tests;
pub mod sim;
#[cfg(feature = "onvif")]
pub mod ws_discovery;

/// A compact, credential-free discovery candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryCandidate {
    /// Backend that found the device.
    pub backend: BackendKind,
    /// Stable selector object suitable for configuration.
    pub selector: Value,
    /// Sanitized vendor when known.
    pub vendor: Option<String>,
    /// Sanitized model when known.
    pub model: Option<String>,
    /// Compact read-only capability hints.
    pub capabilities: Value,
}

/// Bounded protocol discovery request.
#[derive(Debug, Clone)]
pub struct DiscoveryRequest {
    /// Exact OS network interfaces eligible for credential-free network discovery.
    pub eligible_interfaces: Vec<String>,
    /// Overall discovery deadline.
    pub timeout: Duration,
    /// Maximum returned candidates.
    pub max_results: usize,
    /// Cooperative cancellation.
    pub cancellation: CancellationToken,
}

/// Session connection request.
#[derive(Debug, Clone)]
pub struct ConnectRequest {
    /// Camera instance id for diagnostics and simulator identity defaults.
    pub instance_id: String,
    /// Strict backend configuration.
    pub backend: BackendConfig,
    /// Connection deadline.
    pub timeout: Duration,
    /// Cooperative cancellation.
    pub cancellation: CancellationToken,
}

/// Immutable capture request after profile resolution.
#[derive(Debug, Clone)]
pub struct CaptureRequest {
    /// Adapter-generated durable capture id.
    pub capture_id: String,
    /// Immutable effective profile.
    pub profile: CaptureProfile,
    /// Hard accepted source-frame ceiling.
    pub maximum_frame_bytes: u64,
    /// Acquisition-stage deadline.
    pub timeout: Duration,
    /// Cooperative cancellation.
    pub cancellation: CancellationToken,
}

/// Sanitized session status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CameraStatus {
    /// Whether the protocol session is usable for new work.
    pub online: bool,
    /// Monotonic connection generation.
    pub connection_generation: u64,
    /// Optional PTZ observation.
    pub ptz: Option<PtzStatus>,
    /// Sanitized backend-specific status fields.
    pub backend: Value,
}

/// Factory for discovery and stable-selector connection.
#[async_trait]
pub trait CameraBackendFactory: Send + Sync + 'static {
    /// Backend kind implemented by this factory.
    fn kind(&self) -> BackendKind;

    /// Runs bounded, read-only, credential-free discovery.
    async fn discover(&self, request: DiscoveryRequest) -> Result<Vec<DiscoveryCandidate>>;

    /// Connects one stable configured camera.
    async fn connect(&self, request: ConnectRequest) -> Result<Box<dyn CameraSession>>;
}

/// One live camera protocol session, serialized by its owning camera actor.
#[async_trait]
pub trait CameraSession: Send + 'static {
    /// Immutable capability snapshot for this connection generation.
    fn capabilities(&self) -> &CameraCapabilities;

    /// Reads sanitized session/PTZ status.
    async fn status(&mut self) -> Result<CameraStatus>;

    /// Acquires one bounded source frame.
    async fn capture(&mut self, request: CaptureRequest) -> Result<CaptureFrame>;

    /// Executes or observes a capability-gated PTZ operation.
    /// Executes a PTZ operation under the caller's deadline and cancellation signal.
    ///
    /// There is no unbounded variant, deliberately. The trait used to REQUIRE one -- a bare
    /// `ptz(&mut self, request)` with no deadline and no cancellation -- and default `ptz_bounded` on
    /// top of it. Production never called the bare one, so every backend had to implement a method
    /// nothing invoked, and `OnvifSession`'s implementation of it did what a backend does when handed
    /// an obligation with no information: it INVENTED the missing arguments, fabricating a
    /// `CancellationToken::new()` that no one holds and therefore no one can ever cancel, and a
    /// timeout of its own choosing in place of the caller's.
    ///
    /// Dead code, and a loaded gun: the first caller to reach for the obvious-looking `session.ptz()`
    /// would have got an uncancellable protocol call against a made-up deadline. The obligation is gone.
    /// A backend that wants the old wrapping behaviour asks for it explicitly, with [`bounded_ptz`].
    async fn ptz_bounded(
        &mut self,
        request: PtzRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PtzResult>;

    /// Best-effort protocol close. It must be idempotent.
    async fn close(&mut self) -> Result<()>;
}

/// Runs an unbounded protocol call under a caller-owned deadline and cancellation signal.
///
/// This is the body that used to be the default `ptz_bounded`. It is a free function now rather than a
/// default method, because a default silently applies to a backend that never thought about the bound,
/// and being explicit is the point: a non-cooperative implementation cannot keep its owning camera actor
/// blocked past the caller's deadline, and the backend has to say that it wants that guarantee.
///
/// A backend whose transport can take the deadline and the token directly (ONVIF does) should pass them
/// down instead of wrapping, because cancelling a future only drops it -- it does not reach across a
/// socket or a native worker thread.
///
/// # Errors
/// `CAPTURE_CANCELLED` if the token is or becomes cancelled, `PTZ_TIMEOUT` if the deadline passes, and
/// whatever the operation itself returns otherwise.
pub async fn bounded_ptz(
    operation: impl std::future::Future<Output = Result<PtzResult>>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PtzResult> {
    if cancellation.is_cancelled() {
        return Err(crate::CameraError::rejected(
            crate::ErrorCode::CaptureCancelled,
            "PTZ operation was cancelled before execution",
        ));
    }
    if deadline <= Instant::now() {
        return Err(crate::CameraError::rejected(
            crate::ErrorCode::PtzTimeout,
            "PTZ operation exceeded its deadline",
        ));
    }
    tokio::pin!(operation);
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(crate::CameraError::rejected(
            crate::ErrorCode::CaptureCancelled,
            "PTZ operation was cancelled",
        )),
        () = tokio::time::sleep_until(deadline) => Err(crate::CameraError::rejected(
            crate::ErrorCode::PtzTimeout,
            "PTZ operation exceeded its deadline",
        )),
        result = &mut operation => result,
    }
}

/// Runtime-owned services required to construct protocol backends safely.
///
/// This context intentionally contains no camera configuration.  Each factory is created against
/// the current validated global configuration so a compatible reload immediately applies its
/// current ONVIF discovery and security policy to reconnecting sessions.
#[derive(Clone)]
pub struct BackendRuntimeContext {
    credential_service: Option<Arc<dyn CredentialService>>,
    /// Component-wide RTSP decode-stage bound.
    ///
    /// It lives here because this context is the only backend-facing object with a process
    /// lifetime: factories are rebuilt per camera and per reconnect, so a semaphore owned by a
    /// factory would bound one camera against itself and nothing else.
    #[cfg_attr(not(feature = "rtsp"), allow(dead_code))]
    decode_gate: Arc<Semaphore>,
}

impl BackendRuntimeContext {
    /// Creates backend runtime context from the component-scoped EdgeCommons credential service.
    ///
    /// `None` is valid only while no ONVIF camera configuration refers to a secret.  Such a
    /// reference is rejected by [`Self::validate_config`] before the runtime accepts work.
    ///
    /// `limits` sizes the shared RTSP decode gate. The decode stage is part of acquisition, which
    /// the design bounds with `maxConcurrentCaptures`; sizing the gate from anything else would
    /// silently cap the component below the concurrency it advertises.
    #[must_use]
    pub fn new(
        credential_service: Option<Arc<dyn CredentialService>>,
        limits: &LimitsConfig,
    ) -> Self {
        Self {
            credential_service,
            decode_gate: Arc::new(Semaphore::new(limits.max_concurrent_captures)),
        }
    }

    /// Validates that every configured ONVIF secret reference has a real EdgeCommons service.
    ///
    /// # Errors
    /// Returns a closed configuration error rather than allowing an ONVIF session to fall back to
    /// an unavailable credential provider.
    pub fn validate_config(&self, config: &AdapterConfig) -> Result<()> {
        if self.credential_service.is_some() {
            return Ok(());
        }
        for (index, camera) in config.instances.iter().enumerate() {
            let (credentials, ca) = match &camera.backend {
                BackendConfig::OnvifRtsp(onvif) => {
                    (onvif.credentials.is_some(), onvif.tls.ca.is_some())
                }
                BackendConfig::Rtsp(rtsp) => (rtsp.credentials.is_some(), rtsp.tls.ca.is_some()),
                _ => continue,
            };
            if credentials || ca {
                return Err(crate::CameraError::Config {
                    path: format!("component.instances[{index}].backend"),
                    message: "camera secret references require a configured EdgeCommons credentials service"
                        .to_owned(),
                });
            }
        }
        Ok(())
    }

    /// Creates one factory bound to the current global policy.
    ///
    /// # Errors
    /// Returns an error when a selected native backend is unavailable, an explicit discovery
    /// policy is invalid, or required credentials are unavailable.
    pub fn factory_for(
        &self,
        config: &BackendConfig,
        global: &GlobalConfig,
    ) -> Result<Arc<dyn CameraBackendFactory>> {
        #[cfg(all(not(feature = "onvif"), not(feature = "rtsp")))]
        let _ = global;
        match config {
            BackendConfig::OnvifRtsp(_) => {
                #[cfg(feature = "onvif")]
                {
                    Ok(Arc::new(self.onvif_factory(global)?))
                }
                #[cfg(not(feature = "onvif"))]
                {
                    unavailable("onvif-rtsp", "onvif")
                }
            }
            BackendConfig::Rtsp(_) => {
                #[cfg(feature = "rtsp")]
                {
                    Ok(Arc::new(self.rtsp_factory(global)))
                }
                #[cfg(not(feature = "rtsp"))]
                {
                    unavailable("rtsp", "rtsp")
                }
            }
            _ => factory_for(config),
        }
    }

    /// Builds a bare-RTSP factory bound to the current global security policy.
    ///
    /// It shares the component-wide decode gate and resolves any `credentials`/`tls.ca` secret
    /// references through the same EdgeCommons-backed provider the ONVIF factory uses.
    #[cfg(feature = "rtsp")]
    fn rtsp_factory(&self, global: &GlobalConfig) -> rtsp_backend::RtspBackendFactory {
        let credentials = self.credential_service.as_ref().map(|service| {
            Arc::new(
                crate::credential_provider::EdgeCommonsCredentialProvider::new(Arc::clone(service)),
            ) as Arc<dyn net::CredentialProvider>
        });
        rtsp_backend::RtspBackendFactory::new(
            credentials,
            global.security.clone(),
            Arc::clone(&self.decode_gate),
        )
    }

    #[cfg(feature = "onvif")]
    fn onvif_factory(&self, global: &GlobalConfig) -> Result<onvif::OnvifBackendFactory> {
        let discovery: Arc<dyn onvif::WsDiscovery> =
            if global.discovery.eligible_interfaces.is_empty() {
                Arc::new(ws_discovery::NoEligibleInterfaceWsDiscovery)
            } else {
                Arc::new(ws_discovery::ExplicitInterfaceWsDiscovery::new(
                    global.discovery.eligible_interfaces.clone(),
                )?)
            };
        let credentials = self.credential_service.as_ref().map(|service| {
            Arc::new(
                crate::credential_provider::EdgeCommonsCredentialProvider::new(Arc::clone(service)),
            ) as Arc<dyn net::CredentialProvider>
        });
        Ok(onvif::OnvifBackendFactory::new(
            onvif::OnvifBackendDependencies {
                resolver: Arc::new(net::SystemResolver),
                discovery,
                transport: Arc::new(onvif::ReqwestOnvifTransport::default()),
                credentials,
                clock: Arc::new(net::SystemNetClock),
                nonce_source: Arc::new(net::SystemNonceSource),
                security: global.security.clone(),
                decode_gate: Arc::clone(&self.decode_gate),
            },
        ))
    }
}

/// Creates a static built-in factory for a validated backend configuration.
///
/// ONVIF construction is deliberately excluded: it requires live component services and the
/// current global network/security policy, which are supplied by [`BackendRuntimeContext`].
///
/// # Errors
/// Returns an unsupported-capability error when the requested native feature is not
/// compiled into this binary.
pub fn factory_for(config: &BackendConfig) -> Result<Arc<dyn CameraBackendFactory>> {
    match config {
        BackendConfig::Sim(_) => Ok(Arc::new(sim::SimBackendFactory::new())),
        BackendConfig::GenicamAravis(_) => {
            #[cfg(feature = "genicam")]
            {
                Ok(Arc::new(
                    genicam_aravis::GenicamAravisBackendFactory::default(),
                ))
            }
            #[cfg(not(feature = "genicam"))]
            {
                unavailable("genicam-aravis", "genicam")
            }
        }
        BackendConfig::OnvifRtsp(_) => {
            #[cfg(feature = "onvif")]
            {
                Err(crate::CameraError::Config {
                    path: "component.instances[].backend".to_owned(),
                    message: "ONVIF backend construction requires BackendRuntimeContext".to_owned(),
                })
            }
            #[cfg(not(feature = "onvif"))]
            {
                unavailable("onvif-rtsp", "onvif")
            }
        }
        BackendConfig::Rtsp(_) => {
            #[cfg(feature = "rtsp")]
            {
                Err(crate::CameraError::Config {
                    path: "component.instances[].backend".to_owned(),
                    message: "RTSP backend construction requires BackendRuntimeContext".to_owned(),
                })
            }
            #[cfg(not(feature = "rtsp"))]
            {
                unavailable("rtsp", "rtsp")
            }
        }
    }
}

fn unavailable(
    backend: &'static str,
    feature: &'static str,
) -> Result<Arc<dyn CameraBackendFactory>> {
    Err(crate::CameraError::rejected(
        crate::ErrorCode::UnsupportedCapability,
        format!("backend '{backend}' requires the '{feature}' build feature"),
    ))
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "onvif")]
    use edgecommons::credentials::{
        CredentialService, DefaultCredentialService, FileKeyProvider, KeyProvider, LocalVault,
        PutOptions,
    };

    use super::*;

    fn config(value: serde_json::Value) -> BackendConfig {
        serde_json::from_value(value).unwrap()
    }

    /// The RTSP decode stage must be exactly as wide as the concurrency the component advertises.
    ///
    /// It used to be a process-global `Semaphore::new(4)` in `backend::rtsp`, four permits for the
    /// whole component no matter how it was configured. Every decoder creation and every access-unit
    /// decode takes one, so a fleet configured for 32 concurrent captures ran its RTSP decode four
    /// wide and the rest of the captures sat in admission until their deadline expired. Nothing
    /// pointed at the real cause: the config said 32, and the component reported CAPTURE_TIMEOUT.
    #[test]
    fn decode_gate_is_as_wide_as_the_configured_capture_concurrency() {
        let limits = LimitsConfig {
            max_concurrent_captures: 7,
            ..LimitsConfig::default()
        };

        let context = BackendRuntimeContext::new(None, &limits);

        assert_eq!(
            context.decode_gate.available_permits(),
            7,
            "the decode gate must take its width from limits.maxConcurrentCaptures"
        );
    }

    /// One gate for the whole component, not one per camera.
    ///
    /// A factory is built per camera and rebuilt on every reconnect, so a semaphore owned by a
    /// factory would only ever bound a camera against itself -- which is no bound at all. The
    /// context is cloned to reach those factories, so the clone must carry the same semaphore.
    #[test]
    fn cloning_the_context_shares_one_decode_gate() {
        let context = BackendRuntimeContext::new(None, &LimitsConfig::default());

        let clone = context.clone();

        assert!(
            Arc::ptr_eq(&context.decode_gate, &clone.decode_gate),
            "every camera must contend for the same decode gate"
        );
    }

    #[cfg(feature = "onvif")]
    fn global(interfaces: &[&str], security: crate::config::SecurityConfig) -> GlobalConfig {
        let mut global: GlobalConfig = serde_json::from_value(serde_json::json!({
            "output": { "rootDirectory": "/tmp/camera-adapter" },
            "discovery": { "eligibleInterfaces": interfaces },
        }))
        .expect("minimal global configuration");
        global.security = security;
        global
    }

    fn adapter_config(backend: serde_json::Value) -> AdapterConfig {
        let core = edgecommons::config::Config::from_value(
            crate::COMPONENT_NAME,
            "test-thing",
            serde_json::json!({
                "component": {
                    "global": { "output": { "rootDirectory": "C:/camera-captures" } },
                    "instances": [{
                        "id": "camera-a",
                        "backend": backend,
                        "defaultCaptureProfile": "main",
                        "captureProfiles": { "main": { "output": { "encoding": "jpeg" } } }
                    }]
                }
            }),
        )
        .expect("core configuration");
        AdapterConfig::from_core_reload(&core).expect("valid adapter configuration")
    }

    #[cfg(feature = "onvif")]
    fn credential_service() -> (tempfile::TempDir, Arc<dyn CredentialService>) {
        let directory = tempfile::tempdir().expect("temporary vault directory");
        let key = Arc::new(FileKeyProvider::from_bytes([71_u8; 32])) as Arc<dyn KeyProvider>;
        let vault = LocalVault::open(directory.path().join("vault"), key, 2)
            .expect("test credential vault");
        let service = Arc::new(DefaultCredentialService::new(vault));
        service
            .put(
                "camera/login",
                br#"{"username":"operator","password":"camera-secret"}"#,
                PutOptions::default(),
            )
            .expect("login secret");
        (directory, service as Arc<dyn CredentialService>)
    }

    #[test]
    fn simulator_factory_is_available_and_native_features_fail_closed_when_absent() {
        assert_eq!(
            factory_for(&config(serde_json::json!({"type":"sim"})))
                .unwrap()
                .kind(),
            BackendKind::Sim
        );
        #[cfg(not(feature = "genicam"))]
        assert_eq!(
            factory_for(&config(serde_json::json!({
                "type":"genicam-aravis", "selector":{"serial":"SN-1"}
            })))
            .err()
            .expect("genicam must fail closed when its feature is absent")
            .code(),
            crate::ErrorCode::UnsupportedCapability
        );
        #[cfg(not(feature = "onvif"))]
        assert_eq!(
            factory_for(&config(serde_json::json!({
                "type":"onvif-rtsp", "deviceServiceUrl":"https://camera.test/onvif", "mediaProfile":"main"
            })))
            .err()
            .expect("ONVIF must fail closed when its feature is absent")
            .code(),
            crate::ErrorCode::UnsupportedCapability
        );
    }

    #[test]
    fn runtime_context_validates_credential_free_configs_and_delegates_simulator_factory() {
        let adapter = adapter_config(serde_json::json!({"type": "sim"}));
        let context = BackendRuntimeContext::new(None, &LimitsConfig::default());
        context
            .validate_config(&adapter)
            .expect("credential-free simulator configuration needs no secret provider");
        assert_eq!(
            context
                .factory_for(&adapter.instances[0].backend, &adapter.global)
                .expect("runtime context delegates static simulator factory")
                .kind(),
            BackendKind::Sim
        );
    }

    #[cfg(not(feature = "onvif"))]
    #[test]
    fn runtime_context_rejects_onvif_when_the_protocol_feature_is_absent() {
        let adapter = adapter_config(serde_json::json!({
            "type": "onvif-rtsp",
            "deviceServiceUrl": "https://camera.test/onvif/device_service",
            "mediaProfile": "main"
        }));
        let error = match BackendRuntimeContext::new(None, &LimitsConfig::default())
            .factory_for(&adapter.instances[0].backend, &adapter.global)
        {
            Err(error) => error,
            Ok(_) => panic!("runtime context must not silently construct a disabled ONVIF backend"),
        };
        assert_eq!(error.code(), crate::ErrorCode::UnsupportedCapability);
        assert!(error.to_string().contains("onvif"));
    }

    #[cfg(feature = "onvif")]
    #[test]
    fn runtime_context_binds_selector_discovery_to_configured_interfaces() {
        let context = BackendRuntimeContext::new(None, &LimitsConfig::default());
        let global = global(
            &["camera-net-a", "camera-net-b"],
            crate::config::SecurityConfig::default(),
        );
        let backend = config(serde_json::json!({
            "type": "onvif-rtsp",
            "selector": { "endpointReference": "urn:uuid:camera-a" },
            "mediaProfile": "main"
        }));
        let factory = context
            .onvif_factory(&global)
            .expect("explicit discovery factory");

        assert!(matches!(backend, BackendConfig::OnvifRtsp(_)));
        assert_eq!(
            factory.explicit_discovery_interfaces_for_test(),
            Some(vec!["camera-net-a".to_owned(), "camera-net-b".to_owned()])
        );
    }

    #[cfg(feature = "onvif")]
    #[test]
    fn static_onvif_factory_requires_runtime_services_but_context_constructs_one() {
        let adapter = adapter_config(serde_json::json!({
            "type": "onvif-rtsp",
            "deviceServiceUrl": "https://camera.test/onvif/device_service",
            "mediaProfile": "main"
        }));
        let static_error = match factory_for(&adapter.instances[0].backend) {
            Err(error) => error,
            Ok(_) => panic!("static ONVIF construction is intentionally unavailable"),
        };
        assert_eq!(static_error.code(), crate::ErrorCode::InvalidRequest);

        assert_eq!(
            BackendRuntimeContext::new(None, &LimitsConfig::default())
                .factory_for(&adapter.instances[0].backend, &adapter.global)
                .expect("runtime context supplies ONVIF policy/services")
                .kind(),
            BackendKind::OnvifRtsp
        );
    }

    #[cfg(feature = "onvif")]
    #[tokio::test]
    async fn runtime_context_resolves_onvif_secret_references_through_edgecommons() {
        let (_directory, service) = credential_service();
        let context = BackendRuntimeContext::new(Some(service), &LimitsConfig::default());
        let factory = context
            .onvif_factory(&global(&[], crate::config::SecurityConfig::default()))
            .expect("ONVIF factory with credential service");
        let reference = crate::config::SecretRef {
            secret: "camera/login".to_owned(),
            field: None,
        };
        let cancellation = CancellationToken::new();
        let credentials = factory
            .resolve_login_bounded_for_test(
                &reference,
                Instant::now() + Duration::from_secs(5),
                &cancellation,
            )
            .await
            .expect("EdgeCommons credential resolution");

        assert_eq!(credentials.username().expect("username"), "operator");
        assert_eq!(credentials.password().expect("password"), "camera-secret");

        // The bound is the part production actually depends on, and the old `_for_test` accessor
        // skipped it: it called the provider directly, so a credential store that hung would have
        // hung a camera connect with nothing here to notice.
        assert_eq!(
            factory
                .resolve_login_bounded_for_test(&reference, Instant::now(), &cancellation)
                .await
                .expect_err("an elapsed deadline must not reach the credential store")
                .code(),
            crate::ErrorCode::CaptureTimeout
        );

        cancellation.cancel();
        assert_eq!(
            factory
                .resolve_login_bounded_for_test(
                    &reference,
                    Instant::now() + Duration::from_secs(5),
                    &cancellation,
                )
                .await
                .expect_err("a cancelled connect must not reach the credential store")
                .code(),
            crate::ErrorCode::CaptureCancelled
        );
    }

    #[cfg(feature = "onvif")]
    #[test]
    fn runtime_context_propagates_configured_security_to_onvif_session_factory() {
        let security = crate::config::SecurityConfig {
            max_header_bytes: 32_768,
            max_decompression_ratio: 37,
            allow_basic_over_plaintext: true,
        };
        let factory = BackendRuntimeContext::new(None, &LimitsConfig::default())
            .onvif_factory(&global(&[], security.clone()))
            .expect("ONVIF factory");
        let applied = factory.security_policy_for_test();

        assert_eq!(applied.max_header_bytes, security.max_header_bytes);
        assert_eq!(
            applied.max_decompression_ratio,
            security.max_decompression_ratio
        );
        assert_eq!(
            applied.allow_basic_over_plaintext,
            security.allow_basic_over_plaintext
        );
    }

    /// Every entry point handles a bare-RTSP backend when the `rtsp` feature is not compiled in.
    ///
    /// `validate_config` must still reach the RTSP match arm and extract its secret references, and
    /// both the runtime and static factories must fail closed rather than silently omit the camera.
    #[cfg(not(feature = "rtsp"))]
    #[test]
    fn rtsp_backends_validate_and_fail_closed_without_the_protocol_feature() {
        let rtsp = config(serde_json::json!({
            "type": "rtsp",
            "url": "rtsp://cam.example:554/s",
            "allowInsecure": true,
            "credentials": { "$secret": "camera/login" }
        }));
        assert!(matches!(rtsp, BackendConfig::Rtsp(_)));

        // A credential-bearing RTSP instance without a credentials service is rejected: the Rtsp
        // arm of validate_config inspects the config's secret references.
        let mut adapter = adapter_config(serde_json::json!({"type": "sim"}));
        adapter.instances[0].backend = rtsp.clone();
        let context = BackendRuntimeContext::new(None, &LimitsConfig::default());
        let validation_error = context
            .validate_config(&adapter)
            .expect_err("an RTSP secret reference requires a configured credentials service");
        assert!(matches!(
            validation_error,
            crate::CameraError::Config { .. }
        ));

        // The runtime-bound factory fails closed because the rtsp feature is absent.
        let runtime_error = context
            .factory_for(&rtsp, &adapter.global)
            .err()
            .expect("RTSP must fail closed without the rtsp feature");
        assert_eq!(runtime_error.code(), crate::ErrorCode::UnsupportedCapability);
        assert!(runtime_error.to_string().contains("rtsp"));

        // The static factory fails closed on the same backend.
        let static_error = factory_for(&rtsp)
            .err()
            .expect("static RTSP construction must fail closed without the rtsp feature");
        assert_eq!(static_error.code(), crate::ErrorCode::UnsupportedCapability);
        assert!(static_error.to_string().contains("rtsp"));
    }

    /// A protocol call that reaches the camera the moment it is polled, and is answered at once.
    ///
    /// `reached_camera` is the assertion these tests actually turn on: once a `ContinuousMove` has
    /// left the adapter, a physical camera is moving, and no amount of dropping the future takes that
    /// back.
    async fn commands_the_camera(
        reached_camera: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<PtzResult> {
        reached_camera.store(true, std::sync::atomic::Ordering::Release);
        Ok(PtzResult::Commanded)
    }

    /// A protocol call that reaches the camera and then takes an hour to answer.
    ///
    /// Real ONVIF devices do stall -- a wedged web server, a lost TCP connection with no RST. This is
    /// that camera.
    async fn commands_a_camera_that_stalls(
        reached_camera: Arc<tokio::sync::Notify>,
    ) -> Result<PtzResult> {
        reached_camera.notify_one();
        tokio::time::sleep(Duration::from_secs(3_600)).await;
        Ok(PtzResult::Commanded)
    }

    /// A PTZ operation whose caller has already given up must never reach the camera.
    ///
    /// The pre-flight check is not redundant with the `select!` that follows it, and the asymmetry is
    /// the whole point of the bound: cancelling a future only *drops* it. It does not reach across a
    /// socket to un-issue a protocol call, so a `ContinuousMove` that has already left the adapter has
    /// already moved a physical camera. The only cancellation that can honestly be honoured is one
    /// observed BEFORE the call is made -- which is what this arm is, and why it reports the distinct
    /// "before execution" detail rather than the in-flight one.
    #[tokio::test]
    async fn bounded_ptz_refuses_an_already_cancelled_operation_before_it_reaches_the_camera() {
        let reached_camera = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = bounded_ptz(
            commands_the_camera(Arc::clone(&reached_camera)),
            Instant::now() + Duration::from_secs(30),
            &cancellation,
        )
        .await
        .expect_err("a PTZ operation whose caller has already cancelled must not be run");

        assert_eq!(error.code(), crate::ErrorCode::CaptureCancelled);
        assert!(
            error.to_string().contains("before execution"),
            "the caller must be told the camera was never commanded, not merely that the operation \
             was cancelled: {error}"
        );
        assert!(
            !reached_camera.load(std::sync::atomic::Ordering::Acquire),
            "a cancelled PTZ operation that still moves the camera is the failure this guards"
        );
    }

    /// A deadline that has already passed is a deadline, not a suggestion.
    ///
    /// The actor hands down a deadline that was computed upstream -- it can already be in the past by
    /// the time a control is popped from a queue it waited in. Issuing the protocol call anyway would
    /// let a PTZ command run against a budget that no longer exists, holding the camera's only session
    /// past the point at which the caller stopped waiting for it.
    #[tokio::test]
    async fn bounded_ptz_refuses_an_operation_whose_deadline_has_already_passed() {
        let reached_camera = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let error = bounded_ptz(
            commands_the_camera(Arc::clone(&reached_camera)),
            Instant::now() - Duration::from_millis(1),
            &CancellationToken::new(),
        )
        .await
        .expect_err("a PTZ operation with an elapsed deadline must not be run");

        assert_eq!(error.code(), crate::ErrorCode::PtzTimeout);
        assert!(
            !reached_camera.load(std::sync::atomic::Ordering::Acquire),
            "an expired PTZ operation must not still be issued to the camera"
        );
    }

    /// A backend that does not cooperate cannot keep its camera actor blocked.
    ///
    /// The actor is single-threaded per camera: whatever is awaiting the session holds it, and every
    /// other capture and control for that camera waits behind it. A protocol call that never returns
    /// would therefore wedge the camera permanently, which is exactly why the trait has no unbounded
    /// PTZ variant left to reach for.
    #[tokio::test]
    async fn bounded_ptz_cancels_an_operation_that_is_already_in_flight() {
        let cancellation = CancellationToken::new();
        let reached_camera = Arc::new(tokio::sync::Notify::new());
        let stalled = Arc::clone(&reached_camera);
        let token = cancellation.clone();

        let bounded = tokio::spawn(async move {
            bounded_ptz(
                commands_a_camera_that_stalls(stalled),
                Instant::now() + Duration::from_secs(3_600),
                &token,
            )
            .await
        });

        reached_camera.notified().await;
        cancellation.cancel();

        let error = bounded
            .await
            .expect("the bounded PTZ task must not panic")
            .expect_err("a cancelled in-flight PTZ operation must not hang its camera actor");
        assert_eq!(error.code(), crate::ErrorCode::CaptureCancelled);
        assert!(
            !error.to_string().contains("before execution"),
            "the camera WAS commanded here; reporting otherwise would tell an operator the opposite \
             of what happened: {error}"
        );
    }

    /// The deadline ends a stalled protocol call even when nobody cancels it.
    ///
    /// Cancellation needs someone to notice. The deadline does not, and it is the arm that protects
    /// the camera actor from a backend that is simply never going to answer.
    #[tokio::test(start_paused = true)]
    async fn bounded_ptz_times_out_an_operation_that_outlives_its_deadline() {
        let error = bounded_ptz(
            commands_a_camera_that_stalls(Arc::new(tokio::sync::Notify::new())),
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )
        .await
        .expect_err("a PTZ operation that never returns must be ended by its deadline");

        assert_eq!(error.code(), crate::ErrorCode::PtzTimeout);
    }

    /// The bound is transparent to an operation that finishes inside it.
    ///
    /// This is the case the other four are measured against, and it is not a formality: a wrapper that
    /// rejected a *slow* PTZ call -- one that is merely slower than a snapshot, and well inside the
    /// deadline it was given -- would break every camera with a real pan/tilt head on it, while every
    /// negative test above still passed.
    #[tokio::test(start_paused = true)]
    async fn bounded_ptz_returns_the_cameras_own_answer_when_it_arrives_inside_the_bounds() {
        let cancellation = CancellationToken::new();
        let reached_camera = Arc::new(std::sync::atomic::AtomicBool::new(false));

        assert_eq!(
            bounded_ptz(
                commands_the_camera(Arc::clone(&reached_camera)),
                Instant::now() + Duration::from_secs(30),
                &cancellation,
            )
            .await
            .expect("an operation that answers at once must not be interfered with"),
            PtzResult::Commanded
        );
        assert!(reached_camera.load(std::sync::atomic::Ordering::Acquire));

        assert_eq!(
            bounded_ptz(
                commands_a_camera_that_stalls(Arc::new(tokio::sync::Notify::new())),
                Instant::now() + Duration::from_secs(7_200),
                &cancellation,
            )
            .await
            .expect("a slow camera inside its deadline is a slow camera, not a failed one"),
            PtzResult::Commanded,
            "an hour-long PTZ call given a two-hour deadline must be allowed to finish"
        );
    }

    #[cfg(feature = "onvif")]
    #[test]
    fn runtime_context_rejects_onvif_secret_references_without_edgecommons_credentials() {
        let core = edgecommons::config::Config::from_value(
            crate::COMPONENT_NAME,
            "test-thing",
            serde_json::json!({
                "component": {
                    "global": { "output": { "rootDirectory": "C:/camera-captures" } },
                    "instances": [{
                        "id": "camera-a",
                        "backend": {
                            "type": "onvif-rtsp",
                            "deviceServiceUrl": "https://camera.test/onvif/device_service",
                            "mediaProfile": "main",
                            "credentials": { "$secret": "camera/login" }
                        },
                        "defaultCaptureProfile": "main",
                        "captureProfiles": { "main": { "output": { "encoding": "jpeg" } } }
                    }]
                }
            }),
        )
        .expect("core configuration");
        let adapter = AdapterConfig::from_core_reload(&core).expect("valid adapter configuration");
        let error = BackendRuntimeContext::new(None, &LimitsConfig::default())
            .validate_config(&adapter)
            .expect_err("secret-bearing ONVIF configuration requires the core credentials service");

        assert!(matches!(error, crate::CameraError::Config { .. }));
        assert!(
            error
                .to_string()
                .contains("EdgeCommons credentials service")
        );
    }
}
