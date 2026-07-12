//! Protocol backend abstraction and built-in backend factories.
//!
//! Backends know camera protocols and return bounded frames/capabilities. They do not
//! know EdgeCommons topics, SQLite, scheduling, file paths, or terminal messages.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use edgecommons::credentials::CredentialService;

use crate::config::{AdapterConfig, BackendConfig, CaptureProfile, GlobalConfig};
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
    async fn ptz(&mut self, request: PtzRequest) -> Result<PtzResult>;

    /// Executes a PTZ operation with a caller-owned deadline and cancellation signal.
    ///
    /// The default preserves the legacy [`Self::ptz`] implementation contract for simple backends
    /// while ensuring a non-cooperative implementation cannot keep its owning camera actor blocked
    /// past the caller's bound. Protocol backends should override this method to pass the same
    /// deadline and cancellation into their transport layer.
    async fn ptz_bounded(
        &mut self,
        request: PtzRequest,
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
        let operation = self.ptz(request);
        tokio::pin!(operation);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(crate::CameraError::rejected(
                crate::ErrorCode::CaptureCancelled,
                "PTZ operation was cancelled",
            )),
            _ = tokio::time::sleep_until(deadline) => Err(crate::CameraError::rejected(
                crate::ErrorCode::PtzTimeout,
                "PTZ operation exceeded its deadline",
            )),
            result = &mut operation => result,
        }
    }

    /// Best-effort protocol close. It must be idempotent.
    async fn close(&mut self) -> Result<()>;
}

/// Runtime-owned services required to construct protocol backends safely.
///
/// This context intentionally contains no camera configuration.  Each factory is created against
/// the current validated global configuration so a compatible reload immediately applies its
/// current ONVIF discovery and security policy to reconnecting sessions.
#[derive(Clone)]
pub struct BackendRuntimeContext {
    credential_service: Option<Arc<dyn CredentialService>>,
}

impl BackendRuntimeContext {
    /// Creates backend runtime context from the component-scoped EdgeCommons credential service.
    ///
    /// `None` is valid only while no ONVIF camera configuration refers to a secret.  Such a
    /// reference is rejected by [`Self::validate_config`] before the runtime accepts work.
    #[must_use]
    pub fn new(credential_service: Option<Arc<dyn CredentialService>>) -> Self {
        Self { credential_service }
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
            let BackendConfig::OnvifRtsp(onvif) = &camera.backend else {
                continue;
            };
            if onvif.credentials.is_some() || onvif.tls.ca.is_some() {
                return Err(crate::CameraError::Config {
                    path: format!("component.instances[{index}].backend"),
                    message: "ONVIF secret references require a configured EdgeCommons credentials service"
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
        #[cfg(not(feature = "onvif"))]
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
            _ => factory_for(config),
        }
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
            ) as Arc<dyn onvif::OnvifCredentialProvider>
        });
        Ok(onvif::OnvifBackendFactory::new(
            onvif::OnvifBackendDependencies {
                resolver: Arc::new(onvif::SystemResolver),
                discovery,
                transport: Arc::new(onvif::ReqwestOnvifTransport),
                credentials,
                clock: Arc::new(onvif::SystemOnvifClock),
                nonce_source: Arc::new(onvif::SystemNonceSource),
                security: global.security.clone(),
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
        let context = BackendRuntimeContext::new(None);
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
        let error = match BackendRuntimeContext::new(None)
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
        let context = BackendRuntimeContext::new(None);
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
            BackendRuntimeContext::new(None)
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
        let context = BackendRuntimeContext::new(Some(service));
        let factory = context
            .onvif_factory(&global(&[], crate::config::SecurityConfig::default()))
            .expect("ONVIF factory with credential service");
        let credentials = factory
            .resolve_login_for_test(&crate::config::SecretRef {
                secret: "camera/login".to_owned(),
                field: None,
            })
            .await
            .expect("EdgeCommons credential resolution");

        assert_eq!(credentials.username().expect("username"), "operator");
        assert_eq!(credentials.password().expect("password"), "camera-secret");
    }

    #[cfg(feature = "onvif")]
    #[test]
    fn runtime_context_propagates_configured_security_to_onvif_session_factory() {
        let security = crate::config::SecurityConfig {
            max_header_bytes: 32_768,
            max_decompression_ratio: 37,
            allow_basic_over_plaintext: true,
        };
        let factory = BackendRuntimeContext::new(None)
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
        let error = BackendRuntimeContext::new(None)
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
