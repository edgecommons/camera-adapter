//! ONVIF device/media/snapshot/PTZ backend with fail-closed network and XML boundaries.
//!
//! Protocol behavior is split behind resolver, WS-Discovery, HTTP transport, credential, clock, and
//! nonce seams. Production HTTP uses DNS pinning with reqwest redirects disabled; tests exercise the
//! same orchestration with deterministic in-memory implementations. No secret-bearing type exposes a
//! useful `Debug` representation.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::StreamExt;
use image::{GenericImageView, ImageFormat, ImageReader};
use ipnet::IpNet;
use md5::{Digest as _, Md5};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use rand::RngCore;
use serde_json::{Value, json};
use sha1::Sha1;
use sha2::Sha256;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use url::Url;
use zeroize::Zeroizing;

#[cfg(feature = "rtsp")]
use super::rtsp::{RtspCaptureController, RtspControllerConfig};
use super::{
    CameraBackendFactory, CameraSession, CameraStatus, CaptureRequest, ConnectRequest,
    DiscoveryCandidate, DiscoveryRequest,
};
use crate::config::{
    AuthenticationMode, BackendConfig, MediaService, OnvifBackendConfig, OnvifSelector, SecretRef,
    SecurityConfig,
};
use crate::model::{
    BackendKind, CameraCapabilities, CaptureFrame, CaptureMode, FrameTimestampQuality, PixelFormat,
    PtzPreset, PtzRequest, PtzResult, PtzStatus, PtzVector,
};
use crate::{CameraError, ErrorCode, Result};

const ONVIF_BACKEND: &str = "onvif-rtsp";
const MAX_DISCOVERY_MATCHES: usize = 10_000;
const MAX_REDIRECTS: usize = 3;
const MAX_AUTH_HEADER_BYTES: usize = 16 * 1024;
const MAX_BLOCKING_DNS_LOOKUPS: usize = 16;
const MAX_BLOCKING_IMAGE_DECODES: usize = 4;
const MAX_XML_ELEMENTS: usize = 32_768;
const MAX_XML_ATTRIBUTES: usize = 65_536;
const MAX_DISCOVERY_XADDRS: usize = 64;
const MAX_DISCOVERY_XADDR_BYTES: usize = 8 * 1024;
const DEFAULT_PTZ_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MEDIA1_NAMESPACE: &str = "http://www.onvif.org/ver10/media/wsdl";
const MEDIA2_NAMESPACE: &str = "http://www.onvif.org/ver20/media/wsdl";
const PTZ_NAMESPACE: &str = "http://www.onvif.org/ver20/ptz/wsdl";
const DEVICE_NAMESPACE: &str = "http://www.onvif.org/ver10/device/wsdl";
#[cfg(feature = "rtsp")]
const SCHEMA_NAMESPACE: &str = "http://www.onvif.org/ver10/schema";

/// Secret byte buffer that is redacted from `Debug` and overwritten on drop.
pub struct SecretBytes(Zeroizing<Vec<u8>>);

impl SecretBytes {
    /// Copies sensitive bytes into an owned, zeroing buffer.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(Zeroizing::new(bytes.into()))
    }

    /// Borrows the sensitive bytes.
    #[must_use]
    pub fn expose(&self) -> &[u8] {
        self.0.as_slice()
    }

    fn expose_utf8(&self) -> Result<&str> {
        std::str::from_utf8(self.0.as_slice())
            .map_err(|_| security_error("credential is not valid UTF-8"))
    }
}

impl std::fmt::Debug for SecretBytes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretBytes(<redacted>)")
    }
}

/// Username/password resolved lazily through a credential provider.
pub struct OnvifCredentials {
    username: SecretBytes,
    password: SecretBytes,
}

impl OnvifCredentials {
    /// Builds credentials while retaining values only in redacted, zeroing buffers.
    pub fn new(username: impl Into<Vec<u8>>, password: impl Into<Vec<u8>>) -> Result<Self> {
        let credentials = Self {
            username: SecretBytes::new(username),
            password: SecretBytes::new(password),
        };
        let username = credentials.username.expose_utf8()?;
        let password = credentials.password.expose_utf8()?;
        if username.is_empty() || username.len() > 1_024 || password.len() > 16 * 1024 {
            return Err(security_error("credential fields violate ONVIF bounds"));
        }
        if username.chars().any(char::is_control) || username.contains(':') {
            return Err(security_error(
                "credential username contains a forbidden character",
            ));
        }
        Ok(credentials)
    }

    pub(crate) fn username(&self) -> Result<&str> {
        self.username.expose_utf8()
    }

    pub(crate) fn password(&self) -> Result<&str> {
        self.password.expose_utf8()
    }
}

impl std::fmt::Debug for OnvifCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("OnvifCredentials(<redacted>)")
    }
}

/// Lazy standard-secret resolution seam.
#[async_trait]
pub trait OnvifCredentialProvider: Send + Sync {
    /// Resolves a login object containing `username` and `password`.
    async fn resolve_login(&self, reference: &SecretRef) -> Result<Arc<OnvifCredentials>>;

    /// Resolves opaque bytes, used for a private CA bundle.
    async fn resolve_bytes(&self, reference: &SecretRef) -> Result<Arc<SecretBytes>>;
}

async fn resolve_login_bounded(
    provider: &dyn OnvifCredentialProvider,
    reference: &SecretRef,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Arc<OnvifCredentials>> {
    if deadline <= Instant::now() {
        return Err(timeout_error("credential resolution"));
    }
    let resolution = provider.resolve_login(reference);
    tokio::pin!(resolution);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(cancelled_error("credential resolution")),
        _ = tokio::time::sleep_until(deadline) => Err(timeout_error("credential resolution")),
        result = &mut resolution => result,
    }
}

async fn resolve_bytes_bounded(
    provider: &dyn OnvifCredentialProvider,
    reference: &SecretRef,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Arc<SecretBytes>> {
    if deadline <= Instant::now() {
        return Err(timeout_error("credential resolution"));
    }
    let resolution = provider.resolve_bytes(reference);
    tokio::pin!(resolution);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(cancelled_error("credential resolution")),
        _ = tokio::time::sleep_until(deadline) => Err(timeout_error("credential resolution")),
        result = &mut resolution => result,
    }
}

/// Wall-clock seam for WS-Security timestamps and truthful capture observation time.
pub trait OnvifClock: Send + Sync {
    /// Current UTC time.
    fn now(&self) -> DateTime<Utc>;
}

/// Production UTC clock.
#[derive(Debug, Default)]
pub struct SystemOnvifClock;

impl OnvifClock for SystemOnvifClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Cryptographic nonce seam for Digest and WS-Security.
pub trait OnvifNonceSource: Send + Sync {
    /// Returns exactly `length` unpredictable bytes.
    fn nonce(&self, length: usize) -> Result<Vec<u8>>;
}

/// Production operating-system nonce source.
#[derive(Debug, Default)]
pub struct SystemNonceSource;

impl OnvifNonceSource for SystemNonceSource {
    fn nonce(&self, length: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0_u8; length];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        Ok(bytes)
    }
}

/// DNS resolver seam. Implementations return every selected address, not only the first.
#[async_trait]
pub trait OnvifResolver: Send + Sync {
    /// Resolves one normalized host and approved port.
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<IpAddr>>;
}

/// Production resolver using a bounded blocking system lookup.
#[derive(Debug, Default)]
pub struct SystemResolver;

fn blocking_dns_limiter() -> &'static Arc<Semaphore> {
    static LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();
    LIMITER.get_or_init(|| Arc::new(Semaphore::new(MAX_BLOCKING_DNS_LOOKUPS)))
}

#[async_trait]
impl OnvifResolver for SystemResolver {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<IpAddr>> {
        let host = host.to_owned();
        let permit = Arc::clone(blocking_dns_limiter())
            .acquire_owned()
            .await
            .map_err(|_| security_error("DNS worker limiter was closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut addresses = (host.as_str(), port)
                .to_socket_addrs()
                .map_err(|_| security_error("DNS resolution failed"))?
                .map(|address| address.ip())
                .collect::<Vec<_>>();
            addresses.sort_unstable();
            addresses.dedup();
            if addresses.is_empty() {
                return Err(security_error("DNS resolution returned no addresses"));
            }
            Ok(addresses)
        })
        .await
        .map_err(|_| security_error("DNS resolver task failed"))?
    }
}

/// One bounded WS-Discovery response.
#[derive(Clone, PartialEq, Eq)]
pub struct DiscoveryProbeMatch {
    /// Exact endpoint-reference identity.
    pub endpoint_reference: String,
    /// Reported device-service XAddrs.
    pub xaddrs: Vec<String>,
    /// Sanitized vendor hint.
    pub vendor: Option<String>,
    /// Sanitized model hint.
    pub model: Option<String>,
}

impl std::fmt::Debug for DiscoveryProbeMatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiscoveryProbeMatch")
            .field("endpoint_reference", &self.endpoint_reference)
            .field("xaddr_count", &self.xaddrs.len())
            .field("vendor", &self.vendor)
            .field("model", &self.model)
            .finish()
    }
}

/// Credential-free WS-Discovery transport seam.
#[async_trait]
pub trait WsDiscovery: Send + Sync {
    /// Exact interfaces configured for this transport, when it is explicitly scoped.
    ///
    /// The default deliberately exposes no scope: test doubles and other custom transports cannot
    /// accidentally claim production interface policy.  The production explicit-interface
    /// transport overrides this for runtime verification.
    fn explicit_interfaces(&self) -> Option<&[String]> {
        None
    }

    /// Performs one bounded probe on preconfigured eligible interfaces.
    async fn probe(
        &self,
        deadline: Instant,
        max_results: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<DiscoveryProbeMatch>>;
}

async fn probe_bounded(
    discovery: &dyn WsDiscovery,
    deadline: Instant,
    max_results: usize,
    cancellation: &CancellationToken,
) -> Result<Vec<DiscoveryProbeMatch>> {
    if deadline <= Instant::now() {
        return Err(timeout_error("WS-Discovery"));
    }
    let probe = discovery.probe(deadline, max_results, cancellation);
    tokio::pin!(probe);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(cancelled_error("WS-Discovery")),
        _ = tokio::time::sleep_until(deadline) => Err(timeout_error("WS-Discovery")),
        result = &mut probe => result,
    }
}

/// Fail-closed discovery implementation used until an eligible-interface transport is injected.
#[derive(Debug, Default)]
pub struct DisabledWsDiscovery;

#[async_trait]
impl WsDiscovery for DisabledWsDiscovery {
    async fn probe(
        &self,
        _deadline: Instant,
        _max_results: usize,
        _cancellation: &CancellationToken,
    ) -> Result<Vec<DiscoveryProbeMatch>> {
        Err(CameraError::rejected(
            ErrorCode::UnsupportedCapability,
            "WS-Discovery requires an explicitly eligible-interface transport",
        ))
    }
}

/// Validated and DNS-pinned request target.
#[derive(Clone, PartialEq, Eq)]
pub struct PinnedUri {
    url: Url,
    host: String,
    port: u16,
    address: IpAddr,
}

impl std::fmt::Debug for PinnedUri {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PinnedUri")
            .field("scheme", &self.url.scheme())
            .field("host", &self.host)
            .field("port", &self.port)
            .field("address", &self.address)
            .field("path_and_query", &"<redacted>")
            .finish()
    }
}

impl PinnedUri {
    /// Sanitized URL without user information.
    #[must_use]
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Chosen connection address.
    #[must_use]
    pub const fn address(&self) -> IpAddr {
        self.address
    }

    /// Validated hostname retained for HTTP Host, TLS SNI, and certificate verification.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    fn origin_key(&self) -> (String, String, u16) {
        (self.url.scheme().to_owned(), self.host.clone(), self.port)
    }

    fn request_target(&self) -> String {
        let mut target = self.url.path().to_owned();
        if target.is_empty() {
            target.push('/');
        }
        if let Some(query) = self.url.query() {
            target.push('?');
            target.push_str(query);
        }
        target
    }
}

/// Session URI policy anchored to the initially configured/resolved endpoint.
#[derive(Debug, Clone)]
pub struct UriPolicy {
    configured_host: String,
    scheme: String,
    port: u16,
    endpoint_addresses: BTreeSet<IpAddr>,
    allowed_hosts: BTreeSet<String>,
    allowed_cidrs: Vec<IpNet>,
}

/// Network trust anchor copied into one RTSP controller. It contains no URI path,
/// query, or credentials and is never shared between camera actors.
#[cfg(any(feature = "rtsp", test))]
#[derive(Debug, Clone)]
pub(crate) struct RtspNetworkAnchor {
    pub(crate) configured_host: String,
    pub(crate) endpoint_addresses: BTreeSet<IpAddr>,
    pub(crate) allowed_hosts: BTreeSet<String>,
    pub(crate) allowed_cidrs: Vec<IpNet>,
}

impl UriPolicy {
    /// Copies only the address/host allowlist needed to validate an ONVIF-returned
    /// RTSP URI. The RTSP scheme and port establish a separate immutable tuple.
    #[cfg(any(feature = "rtsp", test))]
    #[cfg_attr(not(feature = "rtsp"), allow(dead_code))]
    pub(crate) fn rtsp_network_anchor(&self) -> RtspNetworkAnchor {
        RtspNetworkAnchor {
            configured_host: self.configured_host.clone(),
            endpoint_addresses: self.endpoint_addresses.clone(),
            allowed_hosts: self.allowed_hosts.clone(),
            allowed_cidrs: self.allowed_cidrs.clone(),
        }
    }

    /// Resolves and pins the configured endpoint, establishing the only implicit address exception.
    pub async fn establish(
        configured_url: &str,
        config: &OnvifBackendConfig,
        resolver: &dyn OnvifResolver,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(Self, PinnedUri)> {
        let url = parse_candidate_url(configured_url, config.allow_insecure)?;
        let host = normalize_host(&url)?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| security_error("endpoint URI has no known port"))?;
        let addresses = resolve_bounded(resolver, &host, port, deadline, cancellation).await?;
        let address = *addresses
            .first()
            .ok_or_else(|| security_error("configured endpoint has no resolved address"))?;
        let allowed_hosts = config
            .allowed_uri_hosts
            .iter()
            .map(|value| normalize_host_text(value))
            .collect::<Result<BTreeSet<_>>>()?;
        let policy = Self {
            configured_host: host.clone(),
            scheme: url.scheme().to_owned(),
            port,
            endpoint_addresses: addresses.iter().copied().collect(),
            allowed_hosts,
            allowed_cidrs: config.allowed_uri_cidrs.clone(),
        };
        Ok((
            policy,
            PinnedUri {
                url,
                host,
                port,
                address,
            },
        ))
    }

    /// Revalidates a camera-returned or redirected URI and pins a current allowed address.
    pub async fn pin(
        &self,
        candidate: &str,
        resolver: &dyn OnvifResolver,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PinnedUri> {
        let url = parse_candidate_url(candidate, self.scheme == "http")?;
        if url.scheme() != self.scheme {
            return Err(security_error("camera URI changed the approved scheme"));
        }
        let host = normalize_host(&url)?;
        if host != self.configured_host && !self.allowed_hosts.contains(&host) {
            return Err(security_error("camera URI host is not allowlisted"));
        }
        let port = url
            .port_or_known_default()
            .ok_or_else(|| security_error("camera URI has no known port"))?;
        if port != self.port {
            return Err(security_error("camera URI changed the approved port"));
        }
        let addresses = resolve_bounded(resolver, &host, port, deadline, cancellation).await?;
        for address in &addresses {
            let endpoint_address =
                host == self.configured_host && self.endpoint_addresses.contains(address);
            let cidr_allowed = self
                .allowed_cidrs
                .iter()
                .any(|network| network.contains(address));
            if !endpoint_address && (is_forbidden_network_address(*address) || !cidr_allowed) {
                return Err(security_error(
                    "camera URI resolved outside the pinned endpoint and allowed CIDRs",
                ));
            }
        }
        let address = *addresses
            .first()
            .ok_or_else(|| security_error("camera URI resolved to no addresses"))?;
        Ok(PinnedUri {
            url,
            host,
            port,
            address,
        })
    }
}

async fn resolve_bounded(
    resolver: &dyn OnvifResolver,
    host: &str,
    port: u16,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<IpAddr>> {
    if deadline <= Instant::now() {
        return Err(timeout_error("DNS resolution"));
    }
    let resolution = resolver.resolve(host, port);
    tokio::pin!(resolution);
    let addresses = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(cancelled_error("DNS resolution")),
        _ = tokio::time::sleep_until(deadline) => return Err(timeout_error("DNS resolution")),
        result = &mut resolution => result?,
    };
    if addresses.is_empty() || addresses.len() > 64 {
        return Err(security_error(
            "DNS answer count is outside the supported bound",
        ));
    }
    let mut addresses = addresses;
    addresses.sort_unstable();
    addresses.dedup();
    Ok(addresses)
}

fn parse_candidate_url(value: &str, allow_plaintext: bool) -> Result<Url> {
    if value.len() > 4_096 || value.chars().any(char::is_control) {
        return Err(security_error(
            "camera URI violates length or control-character bounds",
        ));
    }
    let url = Url::parse(value).map_err(|_| security_error("camera URI is invalid"))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(security_error("camera URI user information is forbidden"));
    }
    if url.fragment().is_some() {
        return Err(security_error("camera URI fragments are forbidden"));
    }
    match url.scheme() {
        "https" => {}
        "http" if allow_plaintext => {}
        "http" => return Err(security_error("plaintext camera URI is forbidden")),
        _ => return Err(security_error("camera URI scheme is not HTTP(S)")),
    }
    if url.host_str().is_none() {
        return Err(security_error("camera URI has no host"));
    }
    Ok(url)
}

fn normalize_host(url: &Url) -> Result<String> {
    normalize_host_text(
        url.host_str()
            .ok_or_else(|| security_error("camera URI has no host"))?,
    )
}

pub(crate) fn normalize_host_text(value: &str) -> Result<String> {
    let normalized = value.trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.len() > 253
        || normalized.chars().any(char::is_control)
        || normalized.contains(['/', '@'])
    {
        return Err(security_error("URI hostname is invalid"));
    }
    Ok(normalized)
}

pub(crate) fn is_forbidden_network_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            address.is_unspecified()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_multicast()
                || address == Ipv4Addr::BROADCAST
        }
        IpAddr::V6(address) => {
            address.is_unspecified()
                || address.is_loopback()
                || address.is_multicast()
                || is_ipv6_link_local(address)
        }
    }
}

fn is_ipv6_link_local(address: Ipv6Addr) -> bool {
    (address.segments()[0] & 0xffc0) == 0xfe80
}

/// Minimal HTTP methods used by ONVIF and snapshot retrieval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnvifHttpMethod {
    /// HTTP GET.
    Get,
    /// HTTP POST.
    Post,
}

impl OnvifHttpMethod {
    fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

/// Per-request TLS policy. CA bytes and authorization are redacted by their owning types.
#[derive(Clone)]
pub struct RequestTlsPolicy {
    /// Whether hostname verification remains enabled.
    pub verify_hostname: bool,
    /// Development-only certificate-verification override.
    pub allow_invalid_certificates: bool,
    /// Optional private CA PEM bundle.
    pub ca_pem: Option<Arc<SecretBytes>>,
}

impl std::fmt::Debug for RequestTlsPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RequestTlsPolicy")
            .field("verify_hostname", &self.verify_hostname)
            .field(
                "allow_invalid_certificates",
                &self.allow_invalid_certificates,
            )
            .field("ca_pem", &self.ca_pem.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Fully bounded HTTP request passed to an injectable transport.
pub struct OnvifHttpRequest {
    /// Validated and pinned destination.
    pub target: PinnedUri,
    /// HTTP method.
    pub method: OnvifHttpMethod,
    /// Non-sensitive request headers. Authorization is held separately.
    pub headers: BTreeMap<String, String>,
    /// Sensitive Authorization value, if any.
    pub authorization: Option<SecretBytes>,
    /// Exact request body.
    pub body: Vec<u8>,
    /// Maximum response status/header bytes.
    pub max_header_bytes: usize,
    /// Maximum response body bytes.
    pub max_body_bytes: u64,
    /// Maximum decoded/compressed body ratio.
    pub max_decompression_ratio: u32,
    /// Absolute request deadline.
    pub deadline: Instant,
    /// Cooperative cancellation.
    pub cancellation: CancellationToken,
    /// TLS settings.
    pub tls: RequestTlsPolicy,
}

impl std::fmt::Debug for OnvifHttpRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OnvifHttpRequest")
            .field("target", &self.target)
            .field("method", &self.method)
            .field("headers", &self.headers)
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .field("body_bytes", &self.body.len())
            .field("max_header_bytes", &self.max_header_bytes)
            .field("max_body_bytes", &self.max_body_bytes)
            .finish()
    }
}

/// Bounded HTTP response. Header names are lower case.
pub struct OnvifHttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// Bounded response headers.
    pub headers: BTreeMap<String, String>,
    /// Bounded decoded body.
    pub body: Vec<u8>,
}

impl OnvifHttpResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// Injectable bounded HTTP transport.
#[async_trait]
pub trait OnvifHttpTransport: Send + Sync {
    /// Sends exactly one request. Redirects must remain disabled inside the transport.
    async fn send(&self, request: OnvifHttpRequest) -> Result<OnvifHttpResponse>;
}

async fn send_http_bounded(
    transport: &dyn OnvifHttpTransport,
    request: OnvifHttpRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<OnvifHttpResponse> {
    if deadline <= Instant::now() {
        return Err(timeout_error("HTTP request"));
    }
    let response = transport.send(request);
    tokio::pin!(response);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(cancelled_error("HTTP request")),
        _ = tokio::time::sleep_until(deadline) => Err(timeout_error("HTTP request")),
        result = &mut response => result,
    }
}

/// Production reqwest transport with per-request address pinning and redirects disabled.
#[derive(Debug, Default)]
pub struct ReqwestOnvifTransport;

fn collect_bounded_response_headers(
    source: &reqwest::header::HeaderMap,
    maximum_bytes: usize,
) -> Result<BTreeMap<String, String>> {
    // Reqwest does not expose the HTTP/1 reason phrase. Reserve a deliberately conservative
    // status-line allowance, then count each duplicate field exactly as it appeared logically.
    let mut wire_bytes = 256_usize;
    let mut headers = BTreeMap::<String, String>::new();
    for (name, value) in source {
        let value = value
            .to_str()
            .map_err(|_| security_error("HTTP response header is not valid text"))?;
        wire_bytes = wire_bytes
            .checked_add(name.as_str().len())
            .and_then(|size| size.checked_add(2))
            .and_then(|size| size.checked_add(value.len()))
            .and_then(|size| size.checked_add(2))
            .ok_or_else(|| security_error("HTTP response header size overflowed"))?;
        if wire_bytes > maximum_bytes {
            return Err(security_error(
                "HTTP response headers exceeded maxHeaderBytes",
            ));
        }
        headers
            .entry(name.as_str().to_ascii_lowercase())
            .and_modify(|existing| {
                existing.push_str(", ");
                existing.push_str(value);
            })
            .or_insert_with(|| value.to_owned());
    }
    wire_bytes = wire_bytes
        .checked_add(2)
        .ok_or_else(|| security_error("HTTP response header size overflowed"))?;
    if wire_bytes > maximum_bytes {
        return Err(security_error(
            "HTTP response headers exceeded maxHeaderBytes",
        ));
    }
    Ok(headers)
}

#[async_trait]
impl OnvifHttpTransport for ReqwestOnvifTransport {
    async fn send(&self, request: OnvifHttpRequest) -> Result<OnvifHttpResponse> {
        if request.deadline <= Instant::now() {
            return Err(timeout_error("HTTP request"));
        }
        if request.max_header_bytes == 0
            || request.max_body_bytes == 0
            || request.max_decompression_ratio == 0
        {
            return Err(security_error("HTTP response bounds must be non-zero"));
        }
        let socket = SocketAddr::new(request.target.address, request.target.port);
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .resolve(request.target.host(), socket)
            .tls_danger_accept_invalid_hostnames(!request.tls.verify_hostname)
            .tls_danger_accept_invalid_certs(request.tls.allow_invalid_certificates);
        if let Some(ca_pem) = &request.tls.ca_pem {
            let certificates = reqwest::Certificate::from_pem_bundle(ca_pem.expose())
                .map_err(|_| security_error("private CA bundle is invalid"))?;
            if certificates.is_empty() {
                return Err(security_error("private CA bundle contains no certificates"));
            }
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        let client = builder
            .build()
            .map_err(|_| backend_error("HTTP client construction failed"))?;
        let method = match request.method {
            OnvifHttpMethod::Get => reqwest::Method::GET,
            OnvifHttpMethod::Post => reqwest::Method::POST,
        };
        let mut outbound = client.request(method, request.target.url.clone());
        for (name, value) in &request.headers {
            if name.eq_ignore_ascii_case("authorization") || name.eq_ignore_ascii_case("host") {
                return Err(security_error("caller supplied a forbidden HTTP header"));
            }
            outbound = outbound.header(name, value);
        }
        if let Some(authorization) = &request.authorization {
            outbound =
                outbound.header(reqwest::header::AUTHORIZATION, authorization.expose_utf8()?);
        }
        if !request.body.is_empty() {
            outbound = outbound.body(request.body);
        }
        let response_future = outbound.send();
        tokio::pin!(response_future);
        let response = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => return Err(cancelled_error("HTTP request")),
            _ = tokio::time::sleep_until(request.deadline) => return Err(timeout_error("HTTP request")),
            result = &mut response_future => result.map_err(|_| backend_error("HTTP request failed"))?,
        };

        let status = response.status().as_u16();
        let headers =
            collect_bounded_response_headers(response.headers(), request.max_header_bytes)?;
        if let Some(content_encoding) = headers.get("content-encoding") {
            if !content_encoding.eq_ignore_ascii_case("identity") {
                return Err(security_error(
                    "compressed HTTP responses are unsupported and rejected fail-closed",
                ));
            }
        }
        if let Some(content_length) = headers
            .get("content-length")
            .and_then(|value| value.parse::<u64>().ok())
        {
            if content_length > request.max_body_bytes {
                return Err(security_error("HTTP response declared an oversized body"));
            }
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => return Err(cancelled_error("HTTP response body")),
            _ = tokio::time::sleep_until(request.deadline) => return Err(timeout_error("HTTP response body")),
            chunk = stream.next() => chunk,
        } {
            let chunk = chunk.map_err(|_| backend_error("HTTP response body failed"))?;
            let next_len = body
                .len()
                .checked_add(chunk.len())
                .ok_or_else(|| security_error("HTTP response body size overflowed"))?;
            if u64::try_from(next_len).unwrap_or(u64::MAX) > request.max_body_bytes {
                return Err(security_error("HTTP response body exceeded its hard limit"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(OnvifHttpResponse {
            status,
            headers,
            body,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DigestAlgorithm {
    Md5,
    Md5Sess,
    Sha256,
    Sha256Sess,
}

impl DigestAlgorithm {
    fn parse(value: Option<&str>) -> Result<Self> {
        match value.unwrap_or("MD5").trim().to_ascii_lowercase().as_str() {
            "md5" => Ok(Self::Md5),
            "md5-sess" => Ok(Self::Md5Sess),
            "sha-256" => Ok(Self::Sha256),
            "sha-256-sess" => Ok(Self::Sha256Sess),
            _ => Err(security_error("HTTP Digest algorithm is unsupported")),
        }
    }

    fn token(self) -> &'static str {
        match self {
            Self::Md5 => "MD5",
            Self::Md5Sess => "MD5-sess",
            Self::Sha256 => "SHA-256",
            Self::Sha256Sess => "SHA-256-sess",
        }
    }

    fn is_session(self) -> bool {
        matches!(self, Self::Md5Sess | Self::Sha256Sess)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DigestChallenge {
    realm: String,
    nonce: String,
    opaque: Option<String>,
    algorithm: DigestAlgorithm,
    qop_auth: bool,
}

pub(crate) fn parse_digest_challenge(header: &str) -> Result<DigestChallenge> {
    if header.len() > MAX_AUTH_HEADER_BYTES {
        return Err(security_error("authentication challenge is oversized"));
    }
    let digest_start = find_auth_scheme(header, "digest")
        .ok_or_else(|| security_error("HTTP Digest challenge was not offered"))?;
    let parameters =
        split_auth_parameters(isolate_auth_scheme_parameters(&header[digest_start..]))?;
    let realm = parameters
        .get("realm")
        .cloned()
        .ok_or_else(|| security_error("HTTP Digest challenge lacks realm"))?;
    let nonce = parameters
        .get("nonce")
        .cloned()
        .ok_or_else(|| security_error("HTTP Digest challenge lacks nonce"))?;
    if realm.len() > 1_024 || nonce.is_empty() || nonce.len() > 4_096 {
        return Err(security_error(
            "HTTP Digest challenge fields violate bounds",
        ));
    }
    let algorithm = DigestAlgorithm::parse(parameters.get("algorithm").map(String::as_str))?;
    let qop_auth = match parameters.get("qop") {
        None => false,
        Some(value) => {
            let offered = value
                .split(',')
                .map(str::trim)
                .any(|value| value.eq_ignore_ascii_case("auth"));
            if !offered {
                return Err(security_error("HTTP Digest challenge lacks qop=auth"));
            }
            true
        }
    };
    Ok(DigestChallenge {
        realm,
        nonce,
        opaque: parameters.get("opaque").cloned(),
        algorithm,
        qop_auth,
    })
}

fn isolate_auth_scheme_parameters(value: &str) -> &str {
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            ',' if !quoted => {
                let remainder = value[index + 1..].trim_start();
                let token_length = remainder
                    .chars()
                    .take_while(|character| is_http_token_character(*character))
                    .map(char::len_utf8)
                    .sum::<usize>();
                if token_length == 0 {
                    continue;
                }
                let after_token = &remainder[token_length..];
                if !after_token.chars().next().is_some_and(char::is_whitespace) {
                    continue;
                }
                let after_whitespace = after_token.trim_start();
                if !after_whitespace.starts_with('=') {
                    return &value[..index];
                }
            }
            _ => {}
        }
    }
    value
}

fn is_http_token_character(character: char) -> bool {
    character.is_ascii_alphanumeric()
        || matches!(
            character,
            '!' | '#'
                | '$'
                | '%'
                | '&'
                | '\''
                | '*'
                | '+'
                | '-'
                | '.'
                | '^'
                | '_'
                | '`'
                | '|'
                | '~'
        )
}

pub(crate) fn find_auth_scheme(header: &str, scheme: &str) -> Option<usize> {
    let lower = header.to_ascii_lowercase();
    let scheme = scheme.to_ascii_lowercase();
    let mut offset = 0;
    while let Some(found) = lower[offset..].find(&scheme) {
        let index = offset + found;
        let before_ok = index == 0
            || lower.as_bytes()[index - 1].is_ascii_whitespace()
            || lower.as_bytes()[index - 1] == b',';
        let end = index + scheme.len();
        let after_ok = end < lower.len() && lower.as_bytes()[end].is_ascii_whitespace();
        if before_ok && after_ok {
            return Some(end);
        }
        offset = end;
    }
    None
}

fn split_auth_parameters(value: &str) -> Result<BTreeMap<String, String>> {
    let mut parameters = BTreeMap::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut fields = Vec::new();
    for character in value.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        match character {
            '\\' if quoted => {
                current.push(character);
                escaped = true;
            }
            '"' => {
                quoted = !quoted;
                current.push(character);
            }
            ',' if !quoted => {
                fields.push(std::mem::take(&mut current));
            }
            _ => current.push(character),
        }
    }
    if quoted || escaped {
        return Err(security_error(
            "authentication challenge has malformed quoting",
        ));
    }
    fields.push(current);
    for field in fields {
        let field = field.trim();
        let Some((name, raw_value)) = field.split_once('=') else {
            return Err(security_error(
                "authentication challenge contains a malformed parameter",
            ));
        };
        let name = name.trim().to_ascii_lowercase();
        if name.is_empty() || parameters.contains_key(&name) {
            return Err(security_error(
                "authentication challenge has duplicate or empty parameters",
            ));
        }
        let raw_value = raw_value.trim();
        let decoded = if raw_value.starts_with('"') {
            if !raw_value.ends_with('"') || raw_value.len() < 2 {
                return Err(security_error(
                    "authentication parameter quote is incomplete",
                ));
            }
            unescape_http_quoted(&raw_value[1..raw_value.len() - 1])?
        } else {
            raw_value.to_owned()
        };
        if decoded.chars().any(char::is_control) {
            return Err(security_error(
                "authentication challenge contains control characters",
            ));
        }
        parameters.insert(name, decoded);
    }
    Ok(parameters)
}

fn unescape_http_quoted(value: &str) -> Result<String> {
    let mut result = String::with_capacity(value.len());
    let mut escaped = false;
    for character in value.chars() {
        if escaped {
            result.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else {
            result.push(character);
        }
    }
    if escaped {
        return Err(security_error("authentication quote ends with an escape"));
    }
    Ok(result)
}

fn digest_authorization(
    challenge: &DigestChallenge,
    credentials: &OnvifCredentials,
    method: OnvifHttpMethod,
    target: &str,
    nonce_count: u32,
    nonce_source: &dyn OnvifNonceSource,
) -> Result<SecretBytes> {
    digest_authorization_for_method(
        challenge,
        credentials,
        method.as_str(),
        target,
        nonce_count,
        nonce_source,
    )
}

pub(crate) fn digest_authorization_for_method(
    challenge: &DigestChallenge,
    credentials: &OnvifCredentials,
    method: &str,
    target: &str,
    nonce_count: u32,
    nonce_source: &dyn OnvifNonceSource,
) -> Result<SecretBytes> {
    if nonce_count == 0 {
        return Err(security_error("HTTP Digest nonce counter wrapped"));
    }
    if method.is_empty()
        || method.len() > 32
        || !method
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte == b'_')
    {
        return Err(security_error("Digest method token is invalid"));
    }
    let username = credentials.username()?;
    let password = credentials.password()?;
    let cnonce_bytes = nonce_source.nonce(16)?;
    if cnonce_bytes.len() != 16 {
        return Err(security_error("nonce source returned the wrong byte count"));
    }
    let cnonce = hex::encode(cnonce_bytes);
    let mut ha1 = digest_hex(
        challenge.algorithm,
        format!("{username}:{}:{password}", challenge.realm).as_bytes(),
    );
    if challenge.algorithm.is_session() {
        ha1 = digest_hex(
            challenge.algorithm,
            format!("{ha1}:{}:{cnonce}", challenge.nonce).as_bytes(),
        );
    }
    let ha2 = digest_hex(challenge.algorithm, format!("{method}:{target}").as_bytes());
    let nonce_count = format!("{nonce_count:08x}");
    let response = if challenge.qop_auth {
        digest_hex(
            challenge.algorithm,
            format!(
                "{ha1}:{}:{nonce_count}:{cnonce}:auth:{ha2}",
                challenge.nonce
            )
            .as_bytes(),
        )
    } else {
        digest_hex(
            challenge.algorithm,
            format!("{ha1}:{}:{ha2}", challenge.nonce).as_bytes(),
        )
    };
    let mut header = format!(
        "Digest username=\"{}\", realm=\"{}\", nonce=\"{}\", uri=\"{}\", response=\"{}\", algorithm={}",
        escape_http_quoted(username),
        escape_http_quoted(&challenge.realm),
        escape_http_quoted(&challenge.nonce),
        escape_http_quoted(target),
        response,
        challenge.algorithm.token()
    );
    if challenge.qop_auth {
        header.push_str(&format!(
            ", qop=auth, nc={nonce_count}, cnonce=\"{}\"",
            escape_http_quoted(&cnonce)
        ));
    }
    if let Some(opaque) = &challenge.opaque {
        header.push_str(&format!(", opaque=\"{}\"", escape_http_quoted(opaque)));
    }
    if header.len() > MAX_AUTH_HEADER_BYTES {
        return Err(security_error("generated HTTP Digest header is oversized"));
    }
    Ok(SecretBytes::new(header.into_bytes()))
}

fn digest_hex(algorithm: DigestAlgorithm, input: &[u8]) -> String {
    match algorithm {
        DigestAlgorithm::Md5 | DigestAlgorithm::Md5Sess => hex::encode(Md5::digest(input)),
        DigestAlgorithm::Sha256 | DigestAlgorithm::Sha256Sess => hex::encode(Sha256::digest(input)),
    }
}

fn escape_http_quoted(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

pub(crate) fn basic_authorization(
    credentials: &OnvifCredentials,
    secure: bool,
) -> Result<SecretBytes> {
    if !secure {
        return Err(security_error(
            "Basic authentication over plaintext is forbidden",
        ));
    }
    let mut joined =
        Vec::with_capacity(credentials.username()?.len() + credentials.password()?.len() + 1);
    joined.extend_from_slice(credentials.username()?.as_bytes());
    joined.push(b':');
    joined.extend_from_slice(credentials.password()?.as_bytes());
    let mut encoded = base64::engine::general_purpose::STANDARD
        .encode(&joined)
        .into_bytes();
    joined.fill(0);
    let mut header = Vec::with_capacity(6 + encoded.len());
    header.extend_from_slice(b"Basic ");
    header.extend_from_slice(&encoded);
    encoded.fill(0);
    Ok(SecretBytes::new(header))
}

fn wsse_header(
    credentials: &OnvifCredentials,
    clock: &dyn OnvifClock,
    nonce_source: &dyn OnvifNonceSource,
) -> Result<String> {
    let nonce = nonce_source.nonce(16)?;
    if nonce.len() != 16 {
        return Err(security_error("nonce source returned the wrong byte count"));
    }
    let created = clock.now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let mut digest_input =
        Vec::with_capacity(nonce.len() + created.len() + credentials.password.expose().len());
    digest_input.extend_from_slice(&nonce);
    digest_input.extend_from_slice(created.as_bytes());
    digest_input.extend_from_slice(credentials.password.expose());
    let digest = base64::engine::general_purpose::STANDARD.encode(Sha1::digest(&digest_input));
    digest_input.fill(0);
    let nonce = base64::engine::general_purpose::STANDARD.encode(nonce);
    Ok(format!(
        "<wsse:Security soap:mustUnderstand=\"1\" xmlns:wsse=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\" xmlns:wsu=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd\"><wsse:UsernameToken><wsse:Username>{}</wsse:Username><wsse:Password Type=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest\">{digest}</wsse:Password><wsse:Nonce EncodingType=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary\">{nonce}</wsse:Nonce><wsu:Created>{created}</wsu:Created></wsse:UsernameToken></wsse:Security>",
        xml_escape(credentials.username()?)
    ))
}

#[derive(Debug, Clone)]
enum SessionAuthentication {
    None,
    HttpDigest {
        challenge: DigestChallenge,
        nonce_count: u32,
    },
    WsseDigest,
    Basic,
}

impl SessionAuthentication {
    fn label(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::HttpDigest { .. } => "http-digest",
            Self::WsseDigest => "wsse-digest",
            Self::Basic => "basic",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct XmlNode {
    name: String,
    attributes: BTreeMap<String, String>,
    text: String,
    children: Vec<XmlNode>,
}

impl XmlNode {
    fn child(&self, name: &str) -> Option<&Self> {
        self.children.iter().find(|child| child.name == name)
    }

    fn descendants<'a>(&'a self, name: &str) -> std::vec::IntoIter<&'a Self> {
        let mut matches = Vec::new();
        self.collect_descendants(name, &mut matches);
        matches.into_iter()
    }

    fn collect_descendants<'a>(&'a self, name: &str, matches: &mut Vec<&'a Self>) {
        for child in &self.children {
            if child.name == name {
                matches.push(child);
            }
            child.collect_descendants(name, matches);
        }
    }

    fn descendant_text(&self, name: &str) -> Option<&str> {
        self.descendants(name)
            .map(|node| node.text.trim())
            .find(|text| !text.is_empty())
    }
}

struct XmlBudget {
    element_count: usize,
    attribute_count: usize,
    stored_text_bytes: usize,
    element_limit: usize,
    attribute_limit: usize,
    stored_text_limit: usize,
}

impl XmlBudget {
    fn new(document_bytes: usize) -> Self {
        Self {
            element_count: 0,
            attribute_count: 0,
            stored_text_bytes: 0,
            element_limit: MAX_XML_ELEMENTS.min(document_bytes.saturating_div(3).saturating_add(1)),
            attribute_limit: MAX_XML_ATTRIBUTES
                .min(document_bytes.saturating_div(4).saturating_add(1)),
            stored_text_limit: document_bytes,
        }
    }

    fn node(&mut self, start: &quick_xml::events::BytesStart<'_>) -> Result<XmlNode> {
        self.element_count = self
            .element_count
            .checked_add(1)
            .ok_or_else(|| security_error("SOAP/XML element count overflowed"))?;
        if self.element_count > self.element_limit {
            return Err(security_error("SOAP/XML element count exceeded its bound"));
        }
        let name = local_xml_name(start.name().as_ref())?;
        let attributes = parse_xml_attributes(start)?;
        self.attribute_count = self
            .attribute_count
            .checked_add(attributes.len())
            .ok_or_else(|| security_error("SOAP/XML attribute count overflowed"))?;
        if self.attribute_count > self.attribute_limit {
            return Err(security_error(
                "SOAP/XML attribute count exceeded its bound",
            ));
        }
        self.account_stored_text(name.len())?;
        for (attribute_name, value) in &attributes {
            self.account_stored_text(attribute_name.len())?;
            self.account_stored_text(value.len())?;
        }
        Ok(XmlNode {
            name,
            attributes,
            text: String::new(),
            children: Vec::new(),
        })
    }

    fn account_stored_text(&mut self, additional_bytes: usize) -> Result<()> {
        self.stored_text_bytes = self
            .stored_text_bytes
            .checked_add(additional_bytes)
            .ok_or_else(|| security_error("SOAP/XML aggregate text size overflowed"))?;
        if self.stored_text_bytes > self.stored_text_limit {
            return Err(security_error(
                "SOAP/XML aggregate text size exceeded its bound",
            ));
        }
        Ok(())
    }
}

fn parse_bounded_xml(bytes: &[u8], max_bytes: u64, max_depth: usize) -> Result<XmlNode> {
    if bytes.is_empty() || u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes {
        return Err(security_error("SOAP/XML body violates its byte bound"));
    }
    let lowercase = String::from_utf8_lossy(bytes).to_ascii_lowercase();
    if lowercase.contains("<!doctype") || lowercase.contains("<!entity") {
        return Err(security_error("XML DTDs and entities are forbidden"));
    }
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().trim_text(true);
    let mut stack = Vec::<XmlNode>::new();
    let mut root = None;
    let mut budget = XmlBudget::new(bytes.len());
    loop {
        match reader
            .read_event()
            .map_err(|_| security_error("SOAP/XML parsing failed"))?
        {
            Event::Start(start) => {
                if stack.len() >= max_depth {
                    return Err(security_error("SOAP/XML exceeded maxXmlDepth"));
                }
                stack.push(budget.node(&start)?);
            }
            Event::Empty(start) => {
                if stack.len() >= max_depth {
                    return Err(security_error("SOAP/XML exceeded maxXmlDepth"));
                }
                let node = budget.node(&start)?;
                attach_xml_node(&mut stack, &mut root, node)?;
            }
            Event::Text(text) => {
                let decoded = text
                    .xml_content()
                    .map_err(|_| security_error("SOAP/XML text decoding failed"))?;
                let decoded = quick_xml::escape::unescape(&decoded)
                    .map_err(|_| security_error("SOAP/XML entity decoding failed"))?;
                if let Some(current) = stack.last_mut() {
                    budget.account_stored_text(decoded.len())?;
                    current.text.push_str(&decoded);
                }
            }
            Event::CData(text) => {
                let decoded = text
                    .xml_content()
                    .map_err(|_| security_error("SOAP/XML CDATA decoding failed"))?;
                if let Some(current) = stack.last_mut() {
                    budget.account_stored_text(decoded.len())?;
                    current.text.push_str(&decoded);
                }
            }
            Event::End(_) => {
                let node = stack
                    .pop()
                    .ok_or_else(|| security_error("SOAP/XML end tag is unbalanced"))?;
                attach_xml_node(&mut stack, &mut root, node)?;
            }
            Event::DocType(_) | Event::GeneralRef(_) => {
                return Err(security_error("XML DTDs and entities are forbidden"));
            }
            Event::PI(_) => {
                return Err(security_error("XML processing instructions are forbidden"));
            }
            Event::Decl(_) | Event::Comment(_) => {}
            Event::Eof => break,
        }
    }
    if !stack.is_empty() {
        return Err(security_error("SOAP/XML document is incomplete"));
    }
    root.ok_or_else(|| security_error("SOAP/XML document has no root element"))
}

fn parse_xml_attributes(
    start: &quick_xml::events::BytesStart<'_>,
) -> Result<BTreeMap<String, String>> {
    let mut attributes = BTreeMap::new();
    for attribute in start.attributes().with_checks(true) {
        let attribute = attribute.map_err(|_| security_error("SOAP/XML attribute is invalid"))?;
        let name = local_xml_name(attribute.key.as_ref())?;
        let value = attribute
            .unescape_value()
            .map_err(|_| security_error("SOAP/XML attribute decoding failed"))?
            .into_owned();
        if attributes.insert(name, value).is_some() {
            return Err(security_error("SOAP/XML contains duplicate attributes"));
        }
    }
    Ok(attributes)
}

fn local_xml_name(bytes: &[u8]) -> Result<String> {
    let value = std::str::from_utf8(bytes)
        .map_err(|_| security_error("SOAP/XML element name is not UTF-8"))?;
    let local = value.rsplit(':').next().unwrap_or(value);
    if local.is_empty() || local.len() > 256 {
        return Err(security_error("SOAP/XML element name violates bounds"));
    }
    Ok(local.to_owned())
}

fn attach_xml_node(stack: &mut [XmlNode], root: &mut Option<XmlNode>, node: XmlNode) -> Result<()> {
    if let Some(parent) = stack.last_mut() {
        parent.children.push(node);
    } else if root.replace(node).is_some() {
        return Err(security_error("SOAP/XML has multiple root elements"));
    }
    Ok(())
}

#[derive(Debug, Clone, Default)]
struct ServiceEndpoints {
    media1: Vec<String>,
    media2: Vec<String>,
    ptz: Vec<String>,
}

fn parse_services(xml: &[u8], max_bytes: u64, max_depth: usize) -> Result<ServiceEndpoints> {
    let root = parse_bounded_xml(xml, max_bytes, max_depth)?;
    reject_soap_fault(&root)?;
    let mut endpoints = ServiceEndpoints::default();
    for service in root.descendants("Service") {
        let namespace = service.child("Namespace").map(|node| node.text.trim());
        let xaddr = service.child("XAddr").map(|node| node.text.trim());
        if let (Some(namespace), Some(xaddr)) = (namespace, xaddr) {
            if xaddr.is_empty() {
                continue;
            }
            match namespace {
                MEDIA1_NAMESPACE => endpoints.media1.push(xaddr.to_owned()),
                MEDIA2_NAMESPACE => endpoints.media2.push(xaddr.to_owned()),
                PTZ_NAMESPACE => endpoints.ptz.push(xaddr.to_owned()),
                _ => {}
            }
        }
    }
    for values in [
        &mut endpoints.media1,
        &mut endpoints.media2,
        &mut endpoints.ptz,
    ] {
        values.sort();
        values.dedup();
        if values.len() > 8 {
            return Err(security_error(
                "ONVIF advertised too many service endpoints",
            ));
        }
    }
    Ok(endpoints)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MediaProfileRecord {
    token: String,
    name: Option<String>,
    ptz_configuration_token: Option<String>,
}

fn parse_profiles(xml: &[u8], max_bytes: u64, max_depth: usize) -> Result<Vec<MediaProfileRecord>> {
    let root = parse_bounded_xml(xml, max_bytes, max_depth)?;
    reject_soap_fault(&root)?;
    let mut profiles = Vec::new();
    for profile in root.descendants("Profiles") {
        let token = profile.attributes.get("token").cloned().or_else(|| {
            profile
                .child("token")
                .map(|node| node.text.trim().to_owned())
        });
        let Some(token) = token else { continue };
        if token.is_empty() || token.len() > 1_024 || token.chars().any(char::is_control) {
            return Err(security_error("ONVIF profile token violates bounds"));
        }
        let name = profile
            .child("Name")
            .map(|node| node.text.trim().to_owned())
            .filter(|value| !value.is_empty());
        let ptz_configuration_token = profile
            .descendants("PTZConfiguration")
            .find_map(|node| node.attributes.get("token").cloned())
            .or_else(|| {
                profile
                    .descendant_text("PTZConfigurationToken")
                    .map(str::to_owned)
            });
        profiles.push(MediaProfileRecord {
            token,
            name,
            ptz_configuration_token,
        });
    }
    if profiles.len() > 1_024 {
        return Err(security_error(
            "ONVIF profile count exceeds the supported bound",
        ));
    }
    Ok(profiles)
}

fn select_profile<'a>(
    profiles: &'a [MediaProfileRecord],
    selector: &str,
) -> Result<Option<&'a MediaProfileRecord>> {
    let matching = profiles
        .iter()
        .filter(|profile| profile.token == selector || profile.name.as_deref() == Some(selector))
        .collect::<Vec<_>>();
    match matching.as_slice() {
        [] => Ok(None),
        [profile] => Ok(Some(*profile)),
        _ => Err(security_error("ONVIF media profile selector is ambiguous")),
    }
}

#[derive(Debug, Clone, Default)]
struct DeviceInformation {
    manufacturer: Option<String>,
    model: Option<String>,
    firmware: Option<String>,
    serial: Option<String>,
}

fn parse_device_information(
    xml: &[u8],
    max_bytes: u64,
    max_depth: usize,
) -> Result<DeviceInformation> {
    let root = parse_bounded_xml(xml, max_bytes, max_depth)?;
    reject_soap_fault(&root)?;
    let bounded = |name: &str| {
        root.descendant_text(name)
            .map(sanitize_protocol_text)
            .filter(|value| !value.is_empty())
    };
    Ok(DeviceInformation {
        manufacturer: bounded("Manufacturer"),
        model: bounded("Model"),
        firmware: bounded("FirmwareVersion"),
        serial: bounded("SerialNumber"),
    })
}

fn parse_ptz_configuration_token(xml: &[u8], max_bytes: u64, max_depth: usize) -> Result<String> {
    let root = parse_bounded_xml(xml, max_bytes, max_depth)?;
    reject_soap_fault(&root)?;
    let mut tokens = root
        .descendants("PTZConfiguration")
        .filter_map(|node| node.attributes.get("token"))
        .filter(|value| !value.is_empty())
        .cloned();
    let token = tokens
        .next()
        .ok_or_else(|| backend_error("camera did not expose a PTZ configuration token"))?;
    if tokens.next().is_some() {
        return Err(security_error(
            "camera exposed ambiguous PTZ configuration tokens",
        ));
    }
    if token.len() > 1_024 || token.chars().any(char::is_control) {
        return Err(security_error("PTZ configuration token violates bounds"));
    }
    Ok(token)
}

#[derive(Debug, Clone, Copy)]
struct PtzNodeFeatures {
    home: bool,
    presets: bool,
}

fn parse_ptz_node_features(
    xml: &[u8],
    max_bytes: u64,
    max_depth: usize,
) -> Result<PtzNodeFeatures> {
    let root = parse_bounded_xml(xml, max_bytes, max_depth)?;
    reject_soap_fault(&root)?;
    let node = root
        .descendants("PTZNode")
        .next()
        .ok_or_else(|| backend_error("PTZ service returned no node"))?;
    let home = node
        .descendant_text("HomeSupported")
        .is_some_and(|value| value.eq_ignore_ascii_case("true") || value == "1");
    let presets = node
        .descendant_text("MaximumNumberOfPresets")
        .and_then(|value| value.parse::<u32>().ok())
        .is_some_and(|value| value > 0);
    Ok(PtzNodeFeatures { home, presets })
}

fn parse_single_uri(xml: &[u8], max_bytes: u64, max_depth: usize) -> Result<String> {
    let root = parse_bounded_xml(xml, max_bytes, max_depth)?;
    reject_soap_fault(&root)?;
    let mut values = root
        .descendants("Uri")
        .map(|node| node.text.trim())
        .filter(|value| !value.is_empty());
    let uri = values
        .next()
        .ok_or_else(|| backend_error("ONVIF response did not contain a URI"))?;
    if values.next().is_some() {
        return Err(security_error("ONVIF response contained ambiguous URIs"));
    }
    Ok(uri.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotFallbackReason {
    CapabilityAbsent,
    ActionNotSupported,
    HttpNotFound,
    HttpMethodNotAllowed,
    HttpGone,
    HttpNotImplemented,
    TransportTimeout,
    UnsupportedContentType,
    CorruptImage,
}

impl SnapshotFallbackReason {
    #[cfg(feature = "rtsp")]
    const fn as_str(self) -> &'static str {
        match self {
            Self::CapabilityAbsent => "snapshot-capability-absent",
            Self::ActionNotSupported => "snapshot-action-not-supported",
            Self::HttpNotFound => "snapshot-http-404",
            Self::HttpMethodNotAllowed => "snapshot-http-405",
            Self::HttpGone => "snapshot-http-410",
            Self::HttpNotImplemented => "snapshot-http-501",
            Self::TransportTimeout => "snapshot-transport-timeout",
            Self::UnsupportedContentType => "snapshot-unsupported-content-type",
            Self::CorruptImage => "snapshot-corrupt-or-truncated",
        }
    }

    const fn for_http_status(status: u16) -> Option<Self> {
        match status {
            404 => Some(Self::HttpNotFound),
            405 => Some(Self::HttpMethodNotAllowed),
            410 => Some(Self::HttpGone),
            501 => Some(Self::HttpNotImplemented),
            _ => None,
        }
    }
}

#[derive(Debug)]
enum SnapshotAttemptFailure {
    Fallback {
        reason: SnapshotFallbackReason,
        error: CameraError,
    },
    Fatal(CameraError),
}

impl SnapshotAttemptFailure {
    fn fallback(reason: SnapshotFallbackReason, error: CameraError) -> Self {
        Self::Fallback { reason, error }
    }

    fn fatal(error: CameraError) -> Self {
        Self::Fatal(error)
    }

    fn into_error(self) -> CameraError {
        match self {
            Self::Fallback { error, .. } | Self::Fatal(error) => error,
        }
    }
}

impl From<CameraError> for SnapshotAttemptFailure {
    fn from(error: CameraError) -> Self {
        Self::Fatal(error)
    }
}

type SnapshotAttemptResult<T> = std::result::Result<T, SnapshotAttemptFailure>;

#[derive(Debug)]
enum SnapshotEndpointAvailability {
    Available(PinnedUri),
    Unavailable(SnapshotFallbackReason),
}

fn snapshot_unavailability_from_fault(root: &XmlNode) -> Option<SnapshotFallbackReason> {
    let fault = root.descendants("Fault").next()?;
    for value in fault.descendants("Value") {
        let code = value.text.trim().rsplit(':').next().unwrap_or_default();
        match code {
            "ActionNotSupported" => return Some(SnapshotFallbackReason::ActionNotSupported),
            // ONVIF devices also use this documented optional-operation fault
            // when the media service exposes no snapshot capability.
            "OptionalActionNotImplemented" => {
                return Some(SnapshotFallbackReason::CapabilityAbsent);
            }
            _ => {}
        }
    }
    None
}

fn reject_soap_fault(root: &XmlNode) -> Result<()> {
    if let Some(fault) = root.descendants("Fault").next() {
        let code = fault
            .descendant_text("Value")
            .or_else(|| fault.descendant_text("faultcode"))
            .unwrap_or("SOAP fault");
        let reason = fault
            .descendant_text("Text")
            .or_else(|| fault.descendant_text("faultstring"))
            .unwrap_or("request failed");
        return Err(backend_error(format!(
            "ONVIF SOAP fault {}: {}",
            sanitize_protocol_text(code),
            sanitize_protocol_text(reason)
        )));
    }
    Ok(())
}

fn soap_fault_is_authentication(xml: &[u8], max_bytes: u64, max_depth: usize) -> bool {
    parse_bounded_xml(xml, max_bytes, max_depth)
        .ok()
        .and_then(|root| {
            root.descendants("Fault").next().map(|fault| {
                let text = format!(
                    "{} {}",
                    fault.descendant_text("Value").unwrap_or_default(),
                    fault.descendant_text("Text").unwrap_or_default()
                )
                .to_ascii_lowercase();
                text.contains("notauthorized")
                    || text.contains("not authorized")
                    || text.contains("failedauthentication")
            })
        })
        .unwrap_or(false)
}

fn soap_envelope(body: &str, security_header: Option<&str>) -> Vec<u8> {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><soap:Envelope xmlns:soap=\"http://www.w3.org/2003/05/soap-envelope\"><soap:Header>{}</soap:Header><soap:Body>{body}</soap:Body></soap:Envelope>",
        security_header.unwrap_or_default()
    )
    .into_bytes()
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn sanitize_protocol_text(value: &str) -> String {
    let mut sanitized = value
        .chars()
        .filter(|character| !character.is_control())
        .take(256)
        .collect::<String>();
    if sanitized.is_empty() {
        sanitized.push_str("unspecified");
    }
    sanitized
}

#[derive(Debug, Clone)]
enum HttpAuthentication {
    Digest {
        challenge: DigestChallenge,
        nonce_count: u32,
    },
    Basic,
}

struct OnvifProtocolClient {
    resolver: Arc<dyn OnvifResolver>,
    transport: Arc<dyn OnvifHttpTransport>,
    clock: Arc<dyn OnvifClock>,
    nonce_source: Arc<dyn OnvifNonceSource>,
    credentials: Option<Arc<OnvifCredentials>>,
    policy: UriPolicy,
    tls: RequestTlsPolicy,
    security: SecurityConfig,
    allow_insecure: bool,
    authentication_mode: AuthenticationMode,
    authentications: BTreeMap<(String, String, u16), SessionAuthentication>,
    max_soap_bytes: u64,
    max_xml_depth: usize,
}

impl OnvifProtocolClient {
    fn credentials(&self) -> Result<&OnvifCredentials> {
        self.credentials.as_deref().ok_or_else(|| {
            security_error(
                "camera requested authentication but no credential reference was configured",
            )
        })
    }

    fn basic_is_allowed(&self, target: &PinnedUri) -> bool {
        target.url.scheme() == "https"
            || (self.allow_insecure && self.security.allow_basic_over_plaintext)
    }

    fn authorization_for_session(
        &mut self,
        method: OnvifHttpMethod,
        target: &PinnedUri,
    ) -> Result<Option<SecretBytes>> {
        let credentials = self.credentials.as_deref();
        let basic_allowed = self.basic_is_allowed(target);
        let origin = target.origin_key();
        let Some(authentication) = self.authentications.get_mut(&origin) else {
            return Ok(None);
        };
        match authentication {
            SessionAuthentication::HttpDigest {
                challenge,
                nonce_count,
            } => {
                *nonce_count = nonce_count
                    .checked_add(1)
                    .ok_or_else(|| security_error("HTTP Digest nonce counter wrapped"))?;
                let credentials = credentials.ok_or_else(|| {
                    security_error("HTTP Digest state exists without credentials")
                })?;
                digest_authorization(
                    challenge,
                    credentials,
                    method,
                    &target.request_target(),
                    *nonce_count,
                    self.nonce_source.as_ref(),
                )
                .map(Some)
            }
            SessionAuthentication::Basic => {
                let credentials = credentials.ok_or_else(|| {
                    security_error("Basic authentication state exists without credentials")
                })?;
                basic_authorization(credentials, basic_allowed).map(Some)
            }
            SessionAuthentication::None | SessionAuthentication::WsseDigest => Ok(None),
        }
    }

    fn wsse_for_session(&self, target: &PinnedUri) -> Result<Option<String>> {
        match self.authentications.get(&target.origin_key()) {
            Some(SessionAuthentication::WsseDigest) => wsse_header(
                self.credentials()?,
                self.clock.as_ref(),
                self.nonce_source.as_ref(),
            )
            .map(Some),
            _ => Ok(None),
        }
    }

    async fn send_soap_once(
        &mut self,
        target: PinnedUri,
        action: &str,
        body: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OnvifHttpResponse> {
        if action.len() > 2_048 || action.chars().any(char::is_control) {
            return Err(security_error("SOAP action violates bounds"));
        }
        let target_url = target.url.as_str().to_owned();
        let target = self.pin(&target_url, deadline, cancellation).await?;
        let security_header = self.wsse_for_session(&target)?;
        let envelope = soap_envelope(body, security_header.as_deref());
        if u64::try_from(envelope.len()).unwrap_or(u64::MAX) > self.max_soap_bytes {
            return Err(security_error(
                "outbound SOAP envelope exceeds maxSoapBytes",
            ));
        }
        let authorization = self.authorization_for_session(OnvifHttpMethod::Post, &target)?;
        let mut headers = BTreeMap::new();
        headers.insert(
            "content-type".to_owned(),
            format!("application/soap+xml; charset=utf-8; action=\"{action}\""),
        );
        headers.insert("accept".to_owned(), "application/soap+xml".to_owned());
        send_http_bounded(
            self.transport.as_ref(),
            OnvifHttpRequest {
                target,
                method: OnvifHttpMethod::Post,
                headers,
                authorization,
                body: envelope,
                max_header_bytes: self.security.max_header_bytes,
                max_body_bytes: self.max_soap_bytes,
                max_decompression_ratio: self.security.max_decompression_ratio,
                deadline,
                cancellation: cancellation.clone(),
                tls: self.tls.clone(),
            },
            deadline,
            cancellation,
        )
        .await
    }

    fn digest_from_response(response: &OnvifHttpResponse) -> Result<DigestChallenge> {
        let challenge = response
            .header("www-authenticate")
            .ok_or_else(|| security_error("HTTP 401 response omitted WWW-Authenticate"))?;
        parse_digest_challenge(challenge)
    }

    fn basic_was_offered(response: &OnvifHttpResponse) -> bool {
        response
            .header("www-authenticate")
            .is_some_and(|value| find_auth_scheme(value, "basic").is_some())
    }

    fn digest_was_offered(response: &OnvifHttpResponse) -> bool {
        response
            .header("www-authenticate")
            .is_some_and(|value| find_auth_scheme(value, "digest").is_some())
    }

    fn finish_soap_response(&self, response: OnvifHttpResponse) -> Result<Vec<u8>> {
        if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            return Err(security_error(
                "SOAP redirects are rejected to avoid replaying POST operations",
            ));
        }
        if response.status == 401 || response.status == 403 {
            return Err(backend_error(
                "ONVIF authentication or authorization failed",
            ));
        }
        if !response.is_success() {
            if let Ok(root) =
                parse_bounded_xml(&response.body, self.max_soap_bytes, self.max_xml_depth)
            {
                reject_soap_fault(&root)?;
            }
            return Err(backend_error(format!(
                "ONVIF HTTP request returned status {}",
                response.status
            )));
        }
        let root = parse_bounded_xml(&response.body, self.max_soap_bytes, self.max_xml_depth)?;
        reject_soap_fault(&root)?;
        Ok(response.body)
    }

    async fn establish_authentication(
        &mut self,
        target: PinnedUri,
        action: &str,
        body: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        let origin = target.origin_key();
        let initial_authentication = match self.authentication_mode {
            AuthenticationMode::WsseDigest => {
                self.credentials()?;
                SessionAuthentication::WsseDigest
            }
            AuthenticationMode::Basic => {
                self.credentials()?;
                if !self.basic_is_allowed(&target) {
                    return Err(security_error(
                        "Basic over plaintext requires both insecure-development permissions",
                    ));
                }
                SessionAuthentication::Basic
            }
            AuthenticationMode::Auto | AuthenticationMode::HttpDigest => {
                SessionAuthentication::None
            }
        };
        self.authentications
            .insert(origin.clone(), initial_authentication);

        let first = self
            .send_soap_once(target.clone(), action, body, deadline, cancellation)
            .await?;
        if self.authentication_mode == AuthenticationMode::WsseDigest
            || self.authentication_mode == AuthenticationMode::Basic
        {
            return self.finish_soap_response(first);
        }
        if first.is_success()
            && !soap_fault_is_authentication(&first.body, self.max_soap_bytes, self.max_xml_depth)
        {
            return self.finish_soap_response(first);
        }

        if Self::digest_was_offered(&first) {
            self.credentials()?;
            self.authentications.insert(
                origin.clone(),
                SessionAuthentication::HttpDigest {
                    challenge: Self::digest_from_response(&first)?,
                    nonce_count: 0,
                },
            );
        } else if self.authentication_mode == AuthenticationMode::HttpDigest {
            return Err(security_error(
                "camera did not offer required HTTP Digest authentication",
            ));
        } else if Self::basic_was_offered(&first) {
            self.credentials()?;
            if !self.basic_is_allowed(&target) {
                return Err(security_error(
                    "camera offered only forbidden Basic authentication",
                ));
            }
            self.authentications
                .insert(origin.clone(), SessionAuthentication::Basic);
        } else if self.credentials.is_some()
            && (first.status == 401
                || soap_fault_is_authentication(
                    &first.body,
                    self.max_soap_bytes,
                    self.max_xml_depth,
                ))
        {
            self.authentications
                .insert(origin, SessionAuthentication::WsseDigest);
        } else {
            return self.finish_soap_response(first);
        }

        let authenticated = self
            .send_soap_once(target, action, body, deadline, cancellation)
            .await?;
        self.finish_soap_response(authenticated)
    }

    async fn soap_call_response(
        &mut self,
        target: PinnedUri,
        action: &str,
        body: &str,
        mutating: bool,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OnvifHttpResponse> {
        let origin = target.origin_key();
        if mutating && !self.authentications.contains_key(&origin) {
            return Err(security_error(
                "mutating SOAP request has no authentication established for its origin",
            ));
        }
        let response = self
            .send_soap_once(target.clone(), action, body, deadline, cancellation)
            .await?;
        let authentication_fault = response.status == 401
            || soap_fault_is_authentication(
                &response.body,
                self.max_soap_bytes,
                self.max_xml_depth,
            );
        if !mutating && authentication_fault {
            let authentication = if self.authentication_mode != AuthenticationMode::WsseDigest
                && Self::digest_was_offered(&response)
            {
                self.credentials()?;
                SessionAuthentication::HttpDigest {
                    challenge: Self::digest_from_response(&response)?,
                    nonce_count: 0,
                }
            } else if matches!(
                self.authentication_mode,
                AuthenticationMode::Auto | AuthenticationMode::Basic
            ) && Self::basic_was_offered(&response)
            {
                self.credentials()?;
                if !self.basic_is_allowed(&target) {
                    return Err(security_error(
                        "camera offered forbidden Basic authentication",
                    ));
                }
                SessionAuthentication::Basic
            } else if self.credentials.is_some()
                && matches!(
                    self.authentication_mode,
                    AuthenticationMode::Auto | AuthenticationMode::WsseDigest
                )
            {
                SessionAuthentication::WsseDigest
            } else {
                return Ok(response);
            };
            self.authentications.insert(origin, authentication);
            let retried = self
                .send_soap_once(target, action, body, deadline, cancellation)
                .await?;
            return Ok(retried);
        }
        if response.is_success() {
            self.authentications
                .entry(origin)
                .or_insert(SessionAuthentication::None);
        }
        Ok(response)
    }

    async fn soap_call(
        &mut self,
        target: PinnedUri,
        action: &str,
        body: &str,
        mutating: bool,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        let response = self
            .soap_call_response(target, action, body, mutating, deadline, cancellation)
            .await?;
        self.finish_soap_response(response)
    }

    async fn resolve_snapshot_endpoint(
        &mut self,
        target: PinnedUri,
        action: &str,
        body: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SnapshotEndpointAvailability> {
        let response = self
            .soap_call_response(target, action, body, false, deadline, cancellation)
            .await?;
        if let Ok(root) = parse_bounded_xml(&response.body, self.max_soap_bytes, self.max_xml_depth)
        {
            if let Some(reason) = snapshot_unavailability_from_fault(&root) {
                return Ok(SnapshotEndpointAvailability::Unavailable(reason));
            }
        }
        let response = self.finish_soap_response(response)?;
        let uri = parse_single_uri(&response, self.max_soap_bytes, self.max_xml_depth)?;
        let endpoint = self.pin(&uri, deadline, cancellation).await?;
        Ok(SnapshotEndpointAvailability::Available(endpoint))
    }

    async fn pin(
        &self,
        candidate: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PinnedUri> {
        self.policy
            .pin(candidate, self.resolver.as_ref(), deadline, cancellation)
            .await
    }

    fn initial_snapshot_authentication(&self, target: &PinnedUri) -> Option<HttpAuthentication> {
        match self.authentications.get(&target.origin_key()) {
            Some(SessionAuthentication::HttpDigest { challenge, .. }) => {
                Some(HttpAuthentication::Digest {
                    challenge: challenge.clone(),
                    nonce_count: 0,
                })
            }
            Some(SessionAuthentication::Basic) => Some(HttpAuthentication::Basic),
            Some(SessionAuthentication::None | SessionAuthentication::WsseDigest) | None => None,
        }
    }

    fn authentication_label(&self, target: &PinnedUri) -> &'static str {
        self.authentications
            .get(&target.origin_key())
            .map_or("unestablished", SessionAuthentication::label)
    }

    fn authorization_for_http_state(
        &self,
        authentication: &mut Option<HttpAuthentication>,
        target: &PinnedUri,
    ) -> Result<Option<SecretBytes>> {
        match authentication {
            Some(HttpAuthentication::Digest {
                challenge,
                nonce_count,
            }) => {
                *nonce_count = nonce_count
                    .checked_add(1)
                    .ok_or_else(|| security_error("HTTP Digest nonce counter wrapped"))?;
                digest_authorization(
                    challenge,
                    self.credentials()?,
                    OnvifHttpMethod::Get,
                    &target.request_target(),
                    *nonce_count,
                    self.nonce_source.as_ref(),
                )
                .map(Some)
            }
            Some(HttpAuthentication::Basic) => {
                basic_authorization(self.credentials()?, self.basic_is_allowed(target)).map(Some)
            }
            None => Ok(None),
        }
    }

    fn negotiate_snapshot_authentication(
        &self,
        response: &OnvifHttpResponse,
        target: &PinnedUri,
    ) -> Result<HttpAuthentication> {
        if self.authentication_mode != AuthenticationMode::WsseDigest
            && Self::digest_was_offered(response)
        {
            self.credentials()?;
            return Ok(HttpAuthentication::Digest {
                challenge: Self::digest_from_response(response)?,
                nonce_count: 0,
            });
        }
        if matches!(
            self.authentication_mode,
            AuthenticationMode::Auto | AuthenticationMode::Basic
        ) && Self::basic_was_offered(response)
        {
            self.credentials()?;
            if !self.basic_is_allowed(target) {
                return Err(security_error(
                    "snapshot endpoint offered forbidden Basic authentication",
                ));
            }
            return Ok(HttpAuthentication::Basic);
        }
        Err(backend_error("snapshot endpoint authentication failed"))
    }

    async fn fetch_snapshot(
        &self,
        initial: PinnedUri,
        maximum_bytes: u64,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> SnapshotAttemptResult<(OnvifHttpResponse, PinnedUri)> {
        let mut target = initial;
        let mut authentication = self.initial_snapshot_authentication(&target);
        let mut challenge_retries = 0_u8;
        let mut redirects = 0_usize;
        loop {
            let target_url = target.url.as_str().to_owned();
            target = self
                .pin(&target_url, deadline, cancellation)
                .await
                .map_err(SnapshotAttemptFailure::fatal)?;
            let authorization = self.authorization_for_http_state(&mut authentication, &target)?;
            let mut headers = BTreeMap::new();
            headers.insert(
                "accept".to_owned(),
                "image/jpeg, image/png;q=0.9".to_owned(),
            );
            let response = send_http_bounded(
                self.transport.as_ref(),
                OnvifHttpRequest {
                    target: target.clone(),
                    method: OnvifHttpMethod::Get,
                    headers,
                    authorization,
                    body: Vec::new(),
                    max_header_bytes: self.security.max_header_bytes,
                    max_body_bytes: maximum_bytes,
                    max_decompression_ratio: self.security.max_decompression_ratio,
                    deadline,
                    cancellation: cancellation.clone(),
                    tls: self.tls.clone(),
                },
                deadline,
                cancellation,
            )
            .await
            .map_err(classify_snapshot_transport_failure)?;

            if response.status == 401 {
                if challenge_retries >= 1 {
                    return Err(SnapshotAttemptFailure::fatal(backend_error(
                        "snapshot endpoint authentication failed",
                    )));
                }
                authentication = Some(self.negotiate_snapshot_authentication(&response, &target)?);
                challenge_retries += 1;
                continue;
            }

            if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
                if redirects >= MAX_REDIRECTS {
                    return Err(SnapshotAttemptFailure::fatal(security_error(
                        "snapshot redirect count exceeded its bound",
                    )));
                }
                let location = response
                    .header("location")
                    .ok_or_else(|| security_error("snapshot redirect omitted Location"))?;
                let redirected = target
                    .url
                    .join(location)
                    .map_err(|_| security_error("snapshot redirect Location is invalid"))?;
                let previous_origin = (
                    target.url.scheme().to_owned(),
                    target.host.clone(),
                    target.port,
                );
                target = self
                    .pin(redirected.as_str(), deadline, cancellation)
                    .await?;
                let next_origin = (
                    target.url.scheme().to_owned(),
                    target.host.clone(),
                    target.port,
                );
                let origin_changed = previous_origin != next_origin;
                if origin_changed {
                    authentication = None;
                    challenge_retries = 0;
                }
                redirects += 1;
                continue;
            }

            if !response.is_success() {
                if let Some(reason) = SnapshotFallbackReason::for_http_status(response.status) {
                    return Err(SnapshotAttemptFailure::fallback(
                        reason,
                        backend_error(format!(
                            "snapshot HTTP request returned status {}",
                            response.status
                        )),
                    ));
                }
                return Err(SnapshotAttemptFailure::fatal(backend_error(format!(
                    "snapshot HTTP request returned status {}",
                    response.status
                ))));
            }
            return Ok((response, target));
        }
    }
}

fn classify_snapshot_transport_failure(error: CameraError) -> SnapshotAttemptFailure {
    if error.code() == ErrorCode::CaptureTimeout {
        SnapshotAttemptFailure::fallback(SnapshotFallbackReason::TransportTimeout, error)
    } else {
        SnapshotAttemptFailure::fatal(error)
    }
}

/// Explicit production/test dependencies for the ONVIF backend.
pub struct OnvifBackendDependencies {
    /// Bounded DNS resolver.
    pub resolver: Arc<dyn OnvifResolver>,
    /// Eligible-interface WS-Discovery transport.
    pub discovery: Arc<dyn WsDiscovery>,
    /// Redirect-disabled, bounded HTTP transport.
    pub transport: Arc<dyn OnvifHttpTransport>,
    /// Lazy secret-reference resolver.  It is absent only when the complete configuration has
    /// no ONVIF secret references; attempting to resolve a reference without it is a closed
    /// configuration error rather than an implicit fallback provider.
    pub credentials: Option<Arc<dyn OnvifCredentialProvider>>,
    /// UTC clock.
    pub clock: Arc<dyn OnvifClock>,
    /// Cryptographic nonce source.
    pub nonce_source: Arc<dyn OnvifNonceSource>,
    /// Shared HTTP/XML security limits.
    pub security: SecurityConfig,
}

#[cfg(test)]
impl Default for OnvifBackendDependencies {
    fn default() -> Self {
        Self {
            resolver: Arc::new(SystemResolver),
            discovery: Arc::new(DisabledWsDiscovery),
            transport: Arc::new(ReqwestOnvifTransport),
            credentials: None,
            clock: Arc::new(SystemOnvifClock),
            nonce_source: Arc::new(SystemNonceSource),
            security: SecurityConfig::default(),
        }
    }
}

/// ONVIF backend factory. Runtime services are injected so secret and network policy remain testable.
pub struct OnvifBackendFactory {
    dependencies: OnvifBackendDependencies,
}

impl OnvifBackendFactory {
    /// Creates a factory with explicit runtime dependencies.
    #[must_use]
    pub fn new(dependencies: OnvifBackendDependencies) -> Self {
        Self { dependencies }
    }

    #[cfg(test)]
    pub(crate) fn explicit_discovery_interfaces_for_test(&self) -> Option<Vec<String>> {
        self.dependencies
            .discovery
            .explicit_interfaces()
            .map(<[String]>::to_vec)
    }

    #[cfg(test)]
    pub(crate) async fn resolve_login_for_test(
        &self,
        reference: &SecretRef,
    ) -> Result<Arc<OnvifCredentials>> {
        let provider =
            self.dependencies
                .credentials
                .as_deref()
                .ok_or_else(|| CameraError::Config {
                    path: "component.credentials".to_owned(),
                    message: "ONVIF secret references require EdgeCommons credentials".to_owned(),
                })?;
        provider.resolve_login(reference).await
    }

    #[cfg(test)]
    pub(crate) fn security_policy_for_test(&self) -> SecurityConfig {
        self.dependencies.security.clone()
    }

    async fn resolve_endpoint(
        &self,
        selector: Option<&OnvifSelector>,
        configured_url: Option<&str>,
        config: &OnvifBackendConfig,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(UriPolicy, PinnedUri)> {
        if let Some(configured_url) = configured_url {
            return UriPolicy::establish(
                configured_url,
                config,
                self.dependencies.resolver.as_ref(),
                deadline,
                cancellation,
            )
            .await;
        }
        let selector = selector.ok_or_else(|| {
            backend_error("ONVIF configuration omitted both endpoint and discovery selector")
        })?;
        validate_endpoint_reference(&selector.endpoint_reference)?;
        let responses = probe_bounded(
            self.dependencies.discovery.as_ref(),
            deadline,
            MAX_DISCOVERY_MATCHES,
            cancellation,
        )
        .await?;
        if responses.len() > MAX_DISCOVERY_MATCHES {
            return Err(security_error(
                "WS-Discovery response count exceeded its bound",
            ));
        }
        let mut selected_identity: Option<NormalizedDiscoveryIdentity> = None;
        for response in responses {
            let normalized = NormalizedDiscoveryIdentity::from_match(response)?;
            if normalized.endpoint_reference != selector.endpoint_reference {
                continue;
            }
            if let Some(existing) = selected_identity.as_mut() {
                existing.reconcile(normalized)?;
            } else {
                selected_identity = Some(normalized);
            }
        }
        let selected_identity = selected_identity.ok_or_else(|| {
            CameraError::rejected(
                ErrorCode::CameraUnavailable,
                "selected ONVIF endpoint reference was not discovered",
            )
        })?;
        let xaddrs = selected_identity.xaddrs;
        if xaddrs.is_empty() || xaddrs.len() > 64 {
            return Err(security_error(
                "selected WS-Discovery identity has invalid XAddr count",
            ));
        }
        let first = xaddrs
            .iter()
            .next()
            .ok_or_else(|| security_error("selected WS-Discovery identity has no XAddr"))?;
        let (policy, pinned) = UriPolicy::establish(
            first,
            config,
            self.dependencies.resolver.as_ref(),
            deadline,
            cancellation,
        )
        .await?;
        for address in &policy.endpoint_addresses {
            if is_forbidden_network_address(*address)
                || !policy
                    .allowed_cidrs
                    .iter()
                    .any(|network| network.contains(address))
            {
                return Err(security_error(
                    "discovered endpoint address is outside explicit allowedUriCidrs",
                ));
            }
        }
        for xaddr in xaddrs.iter().skip(1) {
            policy
                .pin(
                    xaddr,
                    self.dependencies.resolver.as_ref(),
                    deadline,
                    cancellation,
                )
                .await?;
        }
        Ok((policy, pinned))
    }
}

#[cfg(test)]
impl Default for OnvifBackendFactory {
    fn default() -> Self {
        Self::new(OnvifBackendDependencies::default())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaVersion {
    Media1,
    Media2,
}

impl MediaVersion {
    fn namespace(self) -> &'static str {
        match self {
            Self::Media1 => MEDIA1_NAMESPACE,
            Self::Media2 => MEDIA2_NAMESPACE,
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Media1 => "trt",
            Self::Media2 => "tr2",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Media1 => "media1",
            Self::Media2 => "media2",
        }
    }
}

#[derive(Debug, Clone)]
struct SelectedMedia {
    endpoint: PinnedUri,
    version: MediaVersion,
    profile: MediaProfileRecord,
}

async fn select_media_profile(
    client: &mut OnvifProtocolClient,
    services: &ServiceEndpoints,
    selection: MediaService,
    profile_selector: &str,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<SelectedMedia> {
    let versions: &[MediaVersion] = match selection {
        MediaService::Auto => &[MediaVersion::Media2, MediaVersion::Media1],
        MediaService::Media1 => &[MediaVersion::Media1],
        MediaService::Media2 => &[MediaVersion::Media2],
    };
    for version in versions {
        let advertised = match version {
            MediaVersion::Media1 => &services.media1,
            MediaVersion::Media2 => &services.media2,
        };
        if advertised.is_empty() {
            continue;
        }
        if advertised.len() != 1 {
            return Err(security_error(
                "camera advertised ambiguous media service endpoints",
            ));
        }
        let endpoint = client.pin(&advertised[0], deadline, cancellation).await?;
        let prefix = version.prefix();
        let namespace = version.namespace();
        let body = format!("<{prefix}:GetProfiles xmlns:{prefix}=\"{namespace}\"/>");
        let action = format!("{namespace}/GetProfiles");
        let response = client
            .soap_call(
                endpoint.clone(),
                &action,
                &body,
                false,
                deadline,
                cancellation,
            )
            .await?;
        let profiles = parse_profiles(&response, client.max_soap_bytes, client.max_xml_depth)?;
        if let Some(profile) = select_profile(&profiles, profile_selector)? {
            return Ok(SelectedMedia {
                endpoint,
                version: *version,
                profile: profile.clone(),
            });
        }
        if selection != MediaService::Auto {
            break;
        }
    }
    Err(CameraError::rejected(
        ErrorCode::UnsupportedCapability,
        "configured ONVIF media service/profile is unavailable",
    ))
}

fn validate_endpoint_reference(value: &str) -> Result<()> {
    if !(1..=1_024).contains(&value.len()) || value.chars().any(char::is_control) {
        return Err(security_error(
            "WS-Discovery endpoint reference violates bounds",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedDiscoveryIdentity {
    endpoint_reference: String,
    xaddrs: BTreeSet<String>,
    vendor: Option<String>,
    model: Option<String>,
}

impl NormalizedDiscoveryIdentity {
    fn from_match(candidate: DiscoveryProbeMatch) -> Result<Self> {
        validate_endpoint_reference(&candidate.endpoint_reference)?;
        if candidate.xaddrs.is_empty() || candidate.xaddrs.len() > MAX_DISCOVERY_XADDRS {
            return Err(security_error(
                "WS-Discovery response has invalid XAddr count",
            ));
        }
        let mut xaddrs = BTreeSet::new();
        for xaddr in candidate.xaddrs {
            if xaddr.is_empty()
                || xaddr.len() > MAX_DISCOVERY_XADDR_BYTES
                || xaddr.chars().any(char::is_control)
            {
                return Err(security_error(
                    "WS-Discovery XAddr violates length or character bounds",
                ));
            }
            xaddrs.insert(xaddr);
        }
        if xaddrs.is_empty() || xaddrs.len() > MAX_DISCOVERY_XADDRS {
            return Err(security_error(
                "WS-Discovery identity has invalid normalized XAddr count",
            ));
        }
        Ok(Self {
            endpoint_reference: candidate.endpoint_reference,
            xaddrs,
            vendor: candidate.vendor.map(|value| sanitize_protocol_text(&value)),
            model: candidate.model.map(|value| sanitize_protocol_text(&value)),
        })
    }

    fn reconcile(&mut self, candidate: Self) -> Result<()> {
        if self.endpoint_reference != candidate.endpoint_reference
            || self.xaddrs != candidate.xaddrs
            || conflicting_optional_hint(self.vendor.as_deref(), candidate.vendor.as_deref())
            || conflicting_optional_hint(self.model.as_deref(), candidate.model.as_deref())
        {
            return Err(security_error(
                "conflicting WS-Discovery devices claimed the same endpoint reference",
            ));
        }
        if self.vendor.is_none() {
            self.vendor = candidate.vendor;
        }
        if self.model.is_none() {
            self.model = candidate.model;
        }
        Ok(())
    }

    fn into_probe_match(self) -> DiscoveryProbeMatch {
        DiscoveryProbeMatch {
            endpoint_reference: self.endpoint_reference,
            xaddrs: self.xaddrs.into_iter().collect(),
            vendor: self.vendor,
            model: self.model,
        }
    }
}

fn conflicting_optional_hint(left: Option<&str>, right: Option<&str>) -> bool {
    matches!((left, right), (Some(left), Some(right)) if left != right)
}

#[async_trait]
impl CameraBackendFactory for OnvifBackendFactory {
    fn kind(&self) -> BackendKind {
        BackendKind::OnvifRtsp
    }

    async fn discover(&self, request: DiscoveryRequest) -> Result<Vec<DiscoveryCandidate>> {
        if request.timeout.is_zero() || request.max_results == 0 {
            return Err(CameraError::rejected(
                ErrorCode::InvalidRequest,
                "ONVIF discovery requires positive timeout and result bounds",
            ));
        }
        let max_results = request.max_results.min(MAX_DISCOVERY_MATCHES);
        let deadline = Instant::now() + request.timeout;
        let responses = probe_bounded(
            self.dependencies.discovery.as_ref(),
            deadline,
            max_results,
            &request.cancellation,
        )
        .await?;
        if responses.len() > max_results {
            return Err(security_error(
                "WS-Discovery transport exceeded the requested result bound",
            ));
        }
        let mut unique = BTreeMap::<String, DiscoveryProbeMatch>::new();
        for response in responses {
            let normalized = NormalizedDiscoveryIdentity::from_match(response)?;
            match unique.get_mut(&normalized.endpoint_reference) {
                Some(existing) => {
                    let mut existing_normalized =
                        NormalizedDiscoveryIdentity::from_match(existing.clone())?;
                    existing_normalized.reconcile(normalized)?;
                    *existing = existing_normalized.into_probe_match();
                }
                None => {
                    unique.insert(
                        normalized.endpoint_reference.clone(),
                        normalized.into_probe_match(),
                    );
                }
            }
        }
        Ok(unique
            .into_values()
            .take(max_results)
            .map(|candidate| DiscoveryCandidate {
                backend: BackendKind::OnvifRtsp,
                selector: json!({ "endpointReference": candidate.endpoint_reference }),
                vendor: candidate.vendor.map(|value| sanitize_protocol_text(&value)),
                model: candidate.model.map(|value| sanitize_protocol_text(&value)),
                capabilities: json!({ "xaddrCount": candidate.xaddrs.len() }),
            })
            .collect())
    }

    async fn connect(&self, request: ConnectRequest) -> Result<Box<dyn CameraSession>> {
        if request.timeout.is_zero() {
            return Err(timeout_error("connection"));
        }
        let BackendConfig::OnvifRtsp(config) = request.backend else {
            return Err(backend_error(
                "ONVIF factory received a different backend type",
            ));
        };
        #[cfg(not(feature = "rtsp"))]
        if config.capture_mode == CaptureMode::RtspFrame || config.rtsp_fallback {
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "RTSP frame capture/fallback requires the 'rtsp' build feature",
            ));
        }
        if !config.tls.verify_hostname && !config.allow_insecure {
            return Err(security_error(
                "tls.verifyHostname=false requires allowInsecure=true",
            ));
        }
        let deadline = Instant::now() + request.timeout;
        let (policy, device_endpoint) = self
            .resolve_endpoint(
                config.selector.as_ref(),
                config.device_service_url.as_deref(),
                &config,
                deadline,
                &request.cancellation,
            )
            .await?;

        let credentials = match config.credentials.as_ref() {
            Some(reference) => Some(
                resolve_login_bounded(
                    self.dependencies.credentials.as_deref().ok_or_else(|| {
                        CameraError::Config {
                            path: "component.credentials".to_owned(),
                            message: "ONVIF secret references require EdgeCommons credentials"
                                .to_owned(),
                        }
                    })?,
                    reference,
                    deadline,
                    &request.cancellation,
                )
                .await?,
            ),
            None => None,
        };
        let ca_pem = match config.tls.ca.as_ref() {
            Some(reference) => Some(
                resolve_bytes_bounded(
                    self.dependencies.credentials.as_deref().ok_or_else(|| {
                        CameraError::Config {
                            path: "component.credentials".to_owned(),
                            message: "ONVIF secret references require EdgeCommons credentials"
                                .to_owned(),
                        }
                    })?,
                    reference,
                    deadline,
                    &request.cancellation,
                )
                .await?,
            ),
            None => None,
        };
        let mut client = OnvifProtocolClient {
            resolver: Arc::clone(&self.dependencies.resolver),
            transport: Arc::clone(&self.dependencies.transport),
            clock: Arc::clone(&self.dependencies.clock),
            nonce_source: Arc::clone(&self.dependencies.nonce_source),
            credentials,
            policy,
            tls: RequestTlsPolicy {
                verify_hostname: config.tls.verify_hostname,
                allow_invalid_certificates: false,
                ca_pem,
            },
            security: self.dependencies.security.clone(),
            allow_insecure: config.allow_insecure,
            authentication_mode: config.authentication_mode,
            authentications: BTreeMap::new(),
            max_soap_bytes: config.max_soap_bytes,
            max_xml_depth: config.max_xml_depth,
        };

        let get_services = format!(
            "<tds:GetServices xmlns:tds=\"{DEVICE_NAMESPACE}\"><tds:IncludeCapability>false</tds:IncludeCapability></tds:GetServices>"
        );
        let services_xml = client
            .establish_authentication(
                device_endpoint.clone(),
                &format!("{DEVICE_NAMESPACE}/GetServices"),
                &get_services,
                deadline,
                &request.cancellation,
            )
            .await?;
        let services = parse_services(&services_xml, config.max_soap_bytes, config.max_xml_depth)?;

        let device_info_xml = client
            .soap_call(
                device_endpoint,
                &format!("{DEVICE_NAMESPACE}/GetDeviceInformation"),
                &format!("<tds:GetDeviceInformation xmlns:tds=\"{DEVICE_NAMESPACE}\"/>"),
                false,
                deadline,
                &request.cancellation,
            )
            .await?;
        let device_info = parse_device_information(
            &device_info_xml,
            config.max_soap_bytes,
            config.max_xml_depth,
        )?;

        let selected_media = select_media_profile(
            &mut client,
            &services,
            config.media_service,
            &config.media_profile,
            deadline,
            &request.cancellation,
        )
        .await?;
        let prefix = selected_media.version.prefix();
        let namespace = selected_media.version.namespace();
        let snapshot_availability = if config.capture_mode == CaptureMode::SnapshotUri {
            let snapshot_body = format!(
                "<{prefix}:GetSnapshotUri xmlns:{prefix}=\"{namespace}\"><{prefix}:ProfileToken>{}</{prefix}:ProfileToken></{prefix}:GetSnapshotUri>",
                xml_escape(&selected_media.profile.token)
            );
            client
                .resolve_snapshot_endpoint(
                    selected_media.endpoint.clone(),
                    &format!("{namespace}/GetSnapshotUri"),
                    &snapshot_body,
                    deadline,
                    &request.cancellation,
                )
                .await?
        } else {
            SnapshotEndpointAvailability::Unavailable(SnapshotFallbackReason::CapabilityAbsent)
        };
        let (snapshot_endpoint, snapshot_unavailable) = match snapshot_availability {
            SnapshotEndpointAvailability::Available(endpoint) => (Some(endpoint), None),
            SnapshotEndpointAvailability::Unavailable(reason) => (None, Some(reason)),
        };
        #[cfg(not(feature = "rtsp"))]
        let _ = snapshot_unavailable;

        #[cfg(feature = "rtsp")]
        let rtsp = if config.capture_mode == CaptureMode::RtspFrame || config.rtsp_fallback {
            let stream_body = format!(
                "<{prefix}:GetStreamUri xmlns:{prefix}=\"{namespace}\"><{prefix}:StreamSetup><tt:Stream xmlns:tt=\"{SCHEMA_NAMESPACE}\">RTP-Unicast</tt:Stream><tt:Transport xmlns:tt=\"{SCHEMA_NAMESPACE}\">RTSP</tt:Transport></{prefix}:StreamSetup><{prefix}:ProfileToken>{}</{prefix}:ProfileToken></{prefix}:GetStreamUri>",
                xml_escape(&selected_media.profile.token)
            );
            let stream_xml = client
                .soap_call(
                    selected_media.endpoint.clone(),
                    &format!("{namespace}/GetStreamUri"),
                    &stream_body,
                    false,
                    deadline,
                    &request.cancellation,
                )
                .await?;
            let stream_uri =
                parse_single_uri(&stream_xml, config.max_soap_bytes, config.max_xml_depth)?;
            Some(
                RtspCaptureController::establish(
                    RtspControllerConfig {
                        stream_uri,
                        anchor: client.policy.rtsp_network_anchor(),
                        resolver: Arc::clone(&client.resolver),
                        credentials: client.credentials.clone(),
                        nonce_source: Arc::clone(&client.nonce_source),
                        private_ca: client.tls.ca_pem.clone(),
                        verify_hostname: client.tls.verify_hostname,
                        allow_insecure: client.allow_insecure,
                        authentication_mode: client.authentication_mode,
                        security: client.security.clone(),
                        session_policy: config.rtsp_session_policy,
                        maximum_frame_bytes: config.max_snapshot_bytes,
                        clock: Arc::clone(&client.clock),
                    },
                    deadline,
                    &request.cancellation,
                )
                .await?,
            )
        } else {
            None
        };

        #[cfg(feature = "rtsp")]
        if snapshot_endpoint.is_none() && rtsp.is_none() {
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "configured ONVIF profile exposes neither snapshot nor RTSP capture",
            ));
        }
        #[cfg(not(feature = "rtsp"))]
        if snapshot_endpoint.is_none() {
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "configured ONVIF profile does not provide a snapshot URI",
            ));
        }

        let mut ptz_endpoint = None;
        let mut ptz_ranges = None;
        let mut ptz_features = None;
        if !services.ptz.is_empty() {
            if services.ptz.len() != 1 {
                return Err(security_error(
                    "camera advertised ambiguous PTZ service endpoints",
                ));
            }
            let endpoint = client
                .pin(&services.ptz[0], deadline, &request.cancellation)
                .await?;
            let configuration_token =
                if let Some(token) = selected_media.profile.ptz_configuration_token.clone() {
                    token
                } else {
                    let configurations = client
                        .soap_call(
                            endpoint.clone(),
                            &format!("{PTZ_NAMESPACE}/GetConfigurations"),
                            &format!("<tptz:GetConfigurations xmlns:tptz=\"{PTZ_NAMESPACE}\"/>"),
                            false,
                            deadline,
                            &request.cancellation,
                        )
                        .await?;
                    parse_ptz_configuration_token(
                        &configurations,
                        config.max_soap_bytes,
                        config.max_xml_depth,
                    )?
                };
            let options_body = format!(
                "<tptz:GetConfigurationOptions xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ConfigurationToken>{}</tptz:ConfigurationToken></tptz:GetConfigurationOptions>",
                xml_escape(&configuration_token)
            );
            let options = client
                .soap_call(
                    endpoint.clone(),
                    &format!("{PTZ_NAMESPACE}/GetConfigurationOptions"),
                    &options_body,
                    false,
                    deadline,
                    &request.cancellation,
                )
                .await?;
            let nodes = client
                .soap_call(
                    endpoint.clone(),
                    &format!("{PTZ_NAMESPACE}/GetNodes"),
                    &format!("<tptz:GetNodes xmlns:tptz=\"{PTZ_NAMESPACE}\"/>"),
                    false,
                    deadline,
                    &request.cancellation,
                )
                .await?;
            ptz_ranges = Some(parse_ptz_ranges(
                &options,
                config.max_soap_bytes,
                config.max_xml_depth,
            )?);
            ptz_features = Some(parse_ptz_node_features(
                &nodes,
                config.max_soap_bytes,
                config.max_xml_depth,
            )?);
            ptz_endpoint = Some(endpoint);
        }

        let features = ptz_features.unwrap_or(PtzNodeFeatures {
            home: false,
            presets: false,
        });
        let capabilities = CameraCapabilities {
            capture_modes: onvif_capture_modes(snapshot_endpoint.is_some(), {
                #[cfg(feature = "rtsp")]
                {
                    rtsp.is_some()
                }
                #[cfg(not(feature = "rtsp"))]
                {
                    false
                }
            }),
            pixel_formats: vec![PixelFormat::Jpeg, PixelFormat::Rgb8],
            software_trigger: false,
            snapshot_uri: snapshot_endpoint.is_some(),
            rtsp: {
                #[cfg(feature = "rtsp")]
                {
                    rtsp.is_some()
                }
                #[cfg(not(feature = "rtsp"))]
                {
                    false
                }
            },
            ptz: ptz_endpoint.is_some(),
            ptz_status: ptz_endpoint.is_some(),
            presets: features.presets,
            preset_mutation: features.presets,
            vendor: device_info.manufacturer,
            model: device_info.model,
            firmware: device_info.firmware,
            serial: device_info.serial,
            warnings: Vec::new(),
        };
        Ok(Box::new(OnvifSession {
            instance_id: request.instance_id,
            client,
            media_endpoint: selected_media.endpoint,
            media_version: selected_media.version,
            media_profile_token: selected_media.profile.token,
            snapshot_endpoint,
            #[cfg(feature = "rtsp")]
            snapshot_unavailable,
            max_snapshot_bytes: config.max_snapshot_bytes,
            #[cfg(feature = "rtsp")]
            rtsp,
            ptz_endpoint,
            ptz_ranges,
            ptz_home: features.home,
            capabilities,
            closed: false,
        }))
    }
}

struct OnvifSession {
    instance_id: String,
    client: OnvifProtocolClient,
    media_endpoint: PinnedUri,
    media_version: MediaVersion,
    media_profile_token: String,
    snapshot_endpoint: Option<PinnedUri>,
    #[cfg(feature = "rtsp")]
    snapshot_unavailable: Option<SnapshotFallbackReason>,
    max_snapshot_bytes: u64,
    #[cfg(feature = "rtsp")]
    rtsp: Option<RtspCaptureController>,
    ptz_endpoint: Option<PinnedUri>,
    ptz_ranges: Option<PtzRanges>,
    ptz_home: bool,
    capabilities: CameraCapabilities,
    closed: bool,
}

impl OnvifSession {
    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(CameraError::rejected(
                ErrorCode::CameraUnavailable,
                "ONVIF camera session is closed",
            ))
        } else {
            Ok(())
        }
    }

    async fn ptz_call(
        &mut self,
        request: &PtzRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PtzResult> {
        self.ensure_open()?;
        let endpoint = self.ptz_endpoint.clone().ok_or_else(|| {
            CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "camera does not advertise ONVIF PTZ",
            )
        })?;
        let ranges = self.ptz_ranges.as_ref().ok_or_else(|| {
            backend_error("PTZ capability exists without normalized coordinate ranges")
        })?;
        if matches!(request, PtzRequest::Home) && !self.ptz_home {
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "camera PTZ node does not support home",
            ));
        }
        if matches!(
            request,
            PtzRequest::ListPresets
                | PtzRequest::GotoPreset(_)
                | PtzRequest::SetPreset(_)
                | PtzRequest::RemovePreset(_)
        ) && !self.capabilities.presets
        {
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "camera PTZ node does not support presets",
            ));
        }
        let (body, mutating, response_kind) =
            build_ptz_request(request, &self.media_profile_token, ranges)?;
        let action_name = ptz_action_name(request);
        let response = self
            .client
            .soap_call(
                endpoint,
                &format!("{PTZ_NAMESPACE}/{action_name}"),
                &body,
                mutating,
                deadline,
                cancellation,
            )
            .await
            .map_err(map_ptz_error)?;
        parse_ptz_response(
            response_kind,
            &response,
            self.client.max_soap_bytes,
            self.client.max_xml_depth,
            ranges,
            self.client.clock.now(),
        )
    }

    #[cfg(feature = "rtsp")]
    async fn capture_rtsp(
        &mut self,
        maximum_bytes: u64,
        timeout: Duration,
        cancellation: &CancellationToken,
        fallback_reason: Option<SnapshotFallbackReason>,
    ) -> Result<CaptureFrame> {
        let controller = self.rtsp.as_mut().ok_or_else(|| {
            CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "configured ONVIF profile has no validated RTSP stream",
            )
        })?;
        let mut frame = controller
            .capture(maximum_bytes, timeout, cancellation)
            .await?;
        if let Some(reason) = fallback_reason {
            frame.backend_metadata.insert(
                "requestedCaptureMode".to_owned(),
                Value::String("snapshot-uri".to_owned()),
            );
            frame.backend_metadata.insert(
                "fallbackReason".to_owned(),
                Value::String(reason.as_str().to_owned()),
            );
        }
        Ok(frame)
    }
}

fn onvif_capture_modes(snapshot: bool, rtsp: bool) -> Vec<CaptureMode> {
    let mut modes = Vec::with_capacity(2);
    if snapshot {
        modes.push(CaptureMode::SnapshotUri);
    }
    if rtsp {
        modes.push(CaptureMode::RtspFrame);
    }
    modes
}

fn map_ptz_error(error: CameraError) -> CameraError {
    match error {
        CameraError::Rejected {
            code: ErrorCode::CaptureTimeout,
            ..
        } => CameraError::rejected(
            ErrorCode::PtzTimeout,
            "ONVIF PTZ operation exceeded its deadline",
        ),
        error => error,
    }
}

fn ptz_action_name(request: &PtzRequest) -> &'static str {
    match request {
        PtzRequest::Continuous { .. } => "ContinuousMove",
        PtzRequest::Absolute { .. } => "AbsoluteMove",
        PtzRequest::Relative { .. } => "RelativeMove",
        PtzRequest::Stop { .. } => "Stop",
        PtzRequest::Home => "GotoHomePosition",
        PtzRequest::Status => "GetStatus",
        PtzRequest::ListPresets => "GetPresets",
        PtzRequest::GotoPreset(_) => "GotoPreset",
        PtzRequest::SetPreset(_) => "SetPreset",
        PtzRequest::RemovePreset(_) => "RemovePreset",
    }
}

#[async_trait]
impl CameraSession for OnvifSession {
    fn capabilities(&self) -> &CameraCapabilities {
        &self.capabilities
    }

    async fn status(&mut self) -> Result<CameraStatus> {
        self.ensure_open()?;
        let ptz = if self.ptz_endpoint.is_some() {
            match self
                .ptz_call(
                    &PtzRequest::Status,
                    Instant::now() + DEFAULT_PTZ_REQUEST_TIMEOUT,
                    &CancellationToken::new(),
                )
                .await?
            {
                PtzResult::Status(status) => Some(status),
                _ => {
                    return Err(backend_error(
                        "PTZ status call returned the wrong response shape",
                    ));
                }
            }
        } else {
            None
        };
        Ok(CameraStatus {
            online: true,
            connection_generation: 1,
            ptz,
            backend: json!({
                "backend": ONVIF_BACKEND,
                "instance": self.instance_id,
                "host": self.media_endpoint.host(),
                "mediaService": self.media_version.label(),
                "authentication": self.client.authentication_label(&self.media_endpoint),
            }),
        })
    }

    async fn capture(&mut self, request: CaptureRequest) -> Result<CaptureFrame> {
        self.ensure_open()?;
        let capture_mode = request
            .profile
            .capture_mode
            .unwrap_or(CaptureMode::SnapshotUri);
        let maximum_bytes = request.maximum_frame_bytes.min(self.max_snapshot_bytes);
        if maximum_bytes == 0 {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "snapshot frame bound must be positive",
            ));
        }
        #[cfg(feature = "rtsp")]
        if capture_mode == CaptureMode::RtspFrame {
            return self
                .capture_rtsp(maximum_bytes, request.timeout, &request.cancellation, None)
                .await;
        }
        if capture_mode != CaptureMode::SnapshotUri {
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "configured ONVIF capture mode is unavailable",
            ));
        }
        let Some(snapshot_endpoint) = self.snapshot_endpoint.clone() else {
            #[cfg(feature = "rtsp")]
            if let Some(reason) = self.snapshot_unavailable {
                return self
                    .capture_rtsp(
                        maximum_bytes,
                        request.timeout,
                        &request.cancellation,
                        Some(reason),
                    )
                    .await;
            }
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "configured ONVIF profile does not provide a snapshot URI",
            ));
        };
        let deadline = Instant::now() + request.timeout;
        let result = self
            .client
            .fetch_snapshot(
                snapshot_endpoint,
                maximum_bytes,
                deadline,
                &request.cancellation,
            )
            .await;
        let (response, final_target) = match result {
            Ok(result) => result,
            Err(SnapshotAttemptFailure::Fallback { reason, error: _ }) => {
                #[cfg(feature = "rtsp")]
                if self.rtsp.is_some() {
                    return self
                        .capture_rtsp(
                            maximum_bytes,
                            request.timeout,
                            &request.cancellation,
                            Some(reason),
                        )
                        .await;
                }
                #[cfg(not(feature = "rtsp"))]
                let _ = reason;
                return Err(CameraError::rejected(
                    ErrorCode::UnsupportedCapability,
                    "snapshot capture failed with an allowed fallback but no RTSP stream is configured",
                ));
            }
            Err(error) => return Err(error.into_error()),
        };
        let observed_at = self.client.clock.now();
        let result = decode_snapshot_at_safe_boundary(
            response,
            maximum_bytes,
            self.client.security.max_decompression_ratio,
            observed_at,
            final_target.host().to_owned(),
            deadline,
            &request.cancellation,
        )
        .await;
        match result {
            Ok(frame) => Ok(frame),
            Err(SnapshotAttemptFailure::Fallback { reason, error: _ }) => {
                #[cfg(feature = "rtsp")]
                if self.rtsp.is_some() {
                    return self
                        .capture_rtsp(
                            maximum_bytes,
                            request.timeout,
                            &request.cancellation,
                            Some(reason),
                        )
                        .await;
                }
                #[cfg(not(feature = "rtsp"))]
                let _ = reason;
                Err(CameraError::rejected(
                    ErrorCode::UnsupportedCapability,
                    "snapshot decode failed with an allowed fallback but no RTSP stream is configured",
                ))
            }
            Err(error) => Err(error.into_error()),
        }
    }

    async fn ptz(&mut self, request: PtzRequest) -> Result<PtzResult> {
        self.ptz_call(
            &request,
            Instant::now() + DEFAULT_PTZ_REQUEST_TIMEOUT,
            &CancellationToken::new(),
        )
        .await
    }

    async fn ptz_bounded(
        &mut self,
        request: PtzRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PtzResult> {
        self.ptz_call(&request, deadline, cancellation).await
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        #[cfg(feature = "rtsp")]
        if let Some(rtsp) = self.rtsp.as_mut() {
            rtsp.close(Instant::now() + Duration::from_secs(1)).await;
        }
        Ok(())
    }
}

fn blocking_image_limiter() -> &'static Arc<Semaphore> {
    static LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();
    LIMITER.get_or_init(|| Arc::new(Semaphore::new(MAX_BLOCKING_IMAGE_DECODES)))
}

async fn decode_snapshot_at_safe_boundary(
    response: OnvifHttpResponse,
    maximum_bytes: u64,
    maximum_decompression_ratio: u32,
    observed_at: DateTime<Utc>,
    source_host: String,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> SnapshotAttemptResult<CaptureFrame> {
    if cancellation.is_cancelled() {
        return Err(SnapshotAttemptFailure::fatal(cancelled_error(
            "snapshot decode",
        )));
    }
    if deadline <= Instant::now() {
        return Err(SnapshotAttemptFailure::fatal(timeout_error(
            "snapshot decode",
        )));
    }
    let permit = Arc::clone(blocking_image_limiter()).acquire_owned();
    tokio::pin!(permit);
    let permit = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(SnapshotAttemptFailure::fatal(cancelled_error("snapshot decoder admission"))),
        _ = tokio::time::sleep_until(deadline) => return Err(SnapshotAttemptFailure::fatal(timeout_error("snapshot decoder admission"))),
        result = &mut permit => result.map_err(|_| SnapshotAttemptFailure::fatal(security_error("snapshot decoder limiter was closed")))?,
    };
    let decode = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        snapshot_to_frame(
            response,
            maximum_bytes,
            maximum_decompression_ratio,
            observed_at,
            &source_host,
        )
    });
    tokio::pin!(decode);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(SnapshotAttemptFailure::fatal(cancelled_error("snapshot decode"))),
        _ = tokio::time::sleep_until(deadline) => Err(SnapshotAttemptFailure::fatal(timeout_error("snapshot decode"))),
        result = &mut decode => match result {
            Ok(result) => result,
            Err(_) => Err(SnapshotAttemptFailure::fatal(backend_error("snapshot decoder task failed"))),
        },
    }
}

fn snapshot_to_frame(
    response: OnvifHttpResponse,
    maximum_bytes: u64,
    maximum_decompression_ratio: u32,
    observed_at: DateTime<Utc>,
    source_host: &str,
) -> SnapshotAttemptResult<CaptureFrame> {
    if response.body.is_empty() {
        return Err(SnapshotAttemptFailure::fallback(
            SnapshotFallbackReason::CorruptImage,
            backend_error("snapshot body is empty or truncated"),
        ));
    }
    if u64::try_from(response.body.len()).unwrap_or(u64::MAX) > maximum_bytes {
        return Err(SnapshotAttemptFailure::fatal(CameraError::rejected(
            ErrorCode::ResourceLimit,
            "snapshot body violates the accepted frame bound",
        )));
    }
    let content_type = response
        .header("content-type")
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .ok_or_else(|| {
            SnapshotAttemptFailure::fatal(backend_error("snapshot response omitted Content-Type"))
        })?;
    let declared_format = match content_type.as_str() {
        "image/jpeg" | "image/jpg" => ImageFormat::Jpeg,
        "image/png" => ImageFormat::Png,
        _ => {
            return Err(SnapshotAttemptFailure::fallback(
                SnapshotFallbackReason::UnsupportedContentType,
                CameraError::rejected(
                    ErrorCode::UnsupportedPixelFormat,
                    "snapshot response content type is unsupported",
                ),
            ));
        }
    };
    let reader = ImageReader::new(Cursor::new(response.body.as_slice()))
        .with_guessed_format()
        .map_err(|_| {
            SnapshotAttemptFailure::fallback(
                SnapshotFallbackReason::CorruptImage,
                backend_error("snapshot image format probing failed"),
            )
        })?;
    if reader.format() != Some(declared_format) {
        return Err(SnapshotAttemptFailure::fatal(security_error(
            "snapshot Content-Type does not match its bytes",
        )));
    }
    let (width, height) = reader.into_dimensions().map_err(|_| {
        SnapshotAttemptFailure::fallback(
            SnapshotFallbackReason::CorruptImage,
            backend_error("snapshot image header is corrupt or truncated"),
        )
    })?;
    let decoded_bytes = u64::from(width)
        .checked_mul(u64::from(height))
        .and_then(|value| value.checked_mul(3))
        .ok_or_else(|| {
            SnapshotAttemptFailure::fatal(security_error("snapshot decoded size overflowed"))
        })?;
    if decoded_bytes > maximum_bytes {
        return Err(SnapshotAttemptFailure::fatal(CameraError::rejected(
            ErrorCode::ResourceLimit,
            "snapshot decoded frame exceeds maximumFrameBytes",
        )));
    }
    let ratio_bound = u64::try_from(response.body.len())
        .unwrap_or(u64::MAX)
        .checked_mul(u64::from(maximum_decompression_ratio))
        .ok_or_else(|| {
            SnapshotAttemptFailure::fatal(security_error(
                "snapshot decompression-ratio bound overflowed",
            ))
        })?;
    if decoded_bytes > ratio_bound {
        return Err(SnapshotAttemptFailure::fatal(CameraError::rejected(
            ErrorCode::ResourceLimit,
            "decoded snapshot exceeds the decompression-ratio bound",
        )));
    }
    let decoded =
        image::load_from_memory_with_format(&response.body, declared_format).map_err(|_| {
            SnapshotAttemptFailure::fallback(
                SnapshotFallbackReason::CorruptImage,
                backend_error("snapshot image is corrupt or truncated"),
            )
        })?;
    if decoded.dimensions() != (width, height) {
        return Err(SnapshotAttemptFailure::fatal(security_error(
            "snapshot dimensions changed during decoding",
        )));
    }
    let (bytes, pixel_format, source_encoding) = match declared_format {
        ImageFormat::Jpeg => (Bytes::from(response.body), PixelFormat::Jpeg, "jpeg"),
        ImageFormat::Png => {
            let rgb = decoded.to_rgb8().into_raw();
            if u64::try_from(rgb.len()).unwrap_or(u64::MAX) > maximum_bytes {
                return Err(SnapshotAttemptFailure::fatal(CameraError::rejected(
                    ErrorCode::ResourceLimit,
                    "decoded PNG snapshot exceeds maximumFrameBytes",
                )));
            }
            (Bytes::from(rgb), PixelFormat::Rgb8, "png")
        }
        _ => {
            return Err(SnapshotAttemptFailure::fatal(CameraError::rejected(
                ErrorCode::UnsupportedPixelFormat,
                "snapshot format is unsupported",
            )));
        }
    };
    let backend_metadata = BTreeMap::from([
        ("contentType".to_owned(), Value::String(content_type)),
        (
            "sourceEncoding".to_owned(),
            Value::String(source_encoding.to_owned()),
        ),
        (
            "sourceHost".to_owned(),
            Value::String(sanitize_protocol_text(source_host)),
        ),
    ]);
    Ok(CaptureFrame {
        bytes,
        width,
        height,
        pixel_format,
        capture_mode: CaptureMode::SnapshotUri,
        source_timestamp: Some(observed_at),
        timestamp_quality: FrameTimestampQuality::AdapterReceive,
        backend_metadata,
    })
}

#[derive(Debug, Clone, Copy)]
struct AxisRange {
    min: f64,
    max: f64,
}

impl AxisRange {
    fn map_signed(self, normalized: f64) -> Result<f64> {
        if !normalized.is_finite() || !(-1.0..=1.0).contains(&normalized) {
            return Err(CameraError::rejected(
                ErrorCode::PtzRangeError,
                "normalized PTZ value is outside [-1,1]",
            ));
        }
        Ok(self.min + ((normalized + 1.0) / 2.0) * (self.max - self.min))
    }

    fn map_unsigned(self, normalized: f64) -> Result<f64> {
        if !normalized.is_finite() || !(0.0..=1.0).contains(&normalized) {
            return Err(CameraError::rejected(
                ErrorCode::PtzRangeError,
                "normalized absolute zoom is outside [0,1]",
            ));
        }
        Ok(self.min + normalized * (self.max - self.min))
    }

    fn map_speed(self, normalized: f64) -> Result<f64> {
        if !normalized.is_finite() || !(0.0..=1.0).contains(&normalized) {
            return Err(CameraError::rejected(
                ErrorCode::PtzRangeError,
                "normalized PTZ speed is outside [0,1]",
            ));
        }
        Ok(normalized * self.min.abs().max(self.max.abs()))
    }

    fn normalize_signed(self, native: f64) -> Option<f64> {
        (self.max > self.min && native.is_finite() && (self.min..=self.max).contains(&native))
            .then(|| ((native - self.min) / (self.max - self.min)) * 2.0 - 1.0)
    }

    fn normalize_unsigned(self, native: f64) -> Option<f64> {
        (self.max > self.min && native.is_finite() && (self.min..=self.max).contains(&native))
            .then(|| (native - self.min) / (self.max - self.min))
    }
}

#[derive(Debug, Clone)]
struct PtzRanges {
    absolute_pan: AxisRange,
    absolute_tilt: AxisRange,
    absolute_zoom: AxisRange,
    relative_pan: AxisRange,
    relative_tilt: AxisRange,
    relative_zoom: AxisRange,
    velocity_pan: AxisRange,
    velocity_tilt: AxisRange,
    velocity_zoom: AxisRange,
}

fn parse_ptz_ranges(xml: &[u8], max_bytes: u64, max_depth: usize) -> Result<PtzRanges> {
    let root = parse_bounded_xml(xml, max_bytes, max_depth)?;
    reject_soap_fault(&root)?;
    let (absolute_pan, absolute_tilt) = parse_xy_space(&root, "AbsolutePanTiltPositionSpace")?;
    let absolute_zoom = parse_x_space(&root, "AbsoluteZoomPositionSpace")?;
    let (relative_pan, relative_tilt) = parse_xy_space(&root, "RelativePanTiltTranslationSpace")?;
    let relative_zoom = parse_x_space(&root, "RelativeZoomTranslationSpace")?;
    let (velocity_pan, velocity_tilt) = parse_xy_space(&root, "ContinuousPanTiltVelocitySpace")?;
    let velocity_zoom = parse_x_space(&root, "ContinuousZoomVelocitySpace")?;
    Ok(PtzRanges {
        absolute_pan,
        absolute_tilt,
        absolute_zoom,
        relative_pan,
        relative_tilt,
        relative_zoom,
        velocity_pan,
        velocity_tilt,
        velocity_zoom,
    })
}

fn parse_xy_space(root: &XmlNode, name: &str) -> Result<(AxisRange, AxisRange)> {
    let mut spaces = root.descendants(name);
    let space = spaces
        .next()
        .ok_or_else(|| backend_error("camera omitted a required PTZ coordinate space"))?;
    if spaces.next().is_some() {
        return Err(security_error(
            "camera returned ambiguous PTZ coordinate spaces",
        ));
    }
    Ok((
        parse_named_range(space, "XRange")?,
        parse_named_range(space, "YRange")?,
    ))
}

fn parse_x_space(root: &XmlNode, name: &str) -> Result<AxisRange> {
    let mut spaces = root.descendants(name);
    let space = spaces
        .next()
        .ok_or_else(|| backend_error("camera omitted a required PTZ coordinate space"))?;
    if spaces.next().is_some() {
        return Err(security_error(
            "camera returned ambiguous PTZ coordinate spaces",
        ));
    }
    parse_named_range(space, "XRange")
}

fn parse_named_range(node: &XmlNode, name: &str) -> Result<AxisRange> {
    let range = node
        .descendants(name)
        .next()
        .ok_or_else(|| backend_error("camera omitted a PTZ coordinate range"))?;
    let min = range
        .descendant_text("Min")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .ok_or_else(|| backend_error("camera returned an invalid PTZ minimum"))?;
    let max = range
        .descendant_text("Max")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .ok_or_else(|| backend_error("camera returned an invalid PTZ maximum"))?;
    if min >= max || !(max - min).is_finite() {
        return Err(backend_error("camera returned a non-increasing PTZ range"));
    }
    Ok(AxisRange { min, max })
}

fn build_ptz_request(
    request: &PtzRequest,
    profile_token: &str,
    ranges: &PtzRanges,
) -> Result<(String, bool, PtzResponseKind)> {
    validate_ptz_text(profile_token, 1_024, "profile token")?;
    let profile = xml_escape(profile_token);
    match request {
        PtzRequest::Continuous { velocity, timeout } => {
            if timeout.is_zero() {
                return Err(CameraError::rejected(
                    ErrorCode::PtzRangeError,
                    "continuous PTZ timeout must be positive",
                ));
            }
            let pan = ranges.velocity_pan.map_signed(velocity.pan)?;
            let tilt = ranges.velocity_tilt.map_signed(velocity.tilt)?;
            let zoom = ranges.velocity_zoom.map_signed(velocity.zoom)?;
            Ok((
                format!(
                    "<tptz:ContinuousMove xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ProfileToken>{profile}</tptz:ProfileToken><tptz:Velocity><tt:PanTilt xmlns:tt=\"http://www.onvif.org/ver10/schema\" x=\"{pan}\" y=\"{tilt}\"/><tt:Zoom xmlns:tt=\"http://www.onvif.org/ver10/schema\" x=\"{zoom}\"/></tptz:Velocity><tptz:Timeout>PT{}.{:03}S</tptz:Timeout></tptz:ContinuousMove>",
                    timeout.as_secs(),
                    timeout.subsec_millis()
                ),
                true,
                PtzResponseKind::Commanded,
            ))
        }
        PtzRequest::Absolute { position, speed } => {
            let pan = ranges.absolute_pan.map_signed(position.pan)?;
            let tilt = ranges.absolute_tilt.map_signed(position.tilt)?;
            let zoom = ranges.absolute_zoom.map_unsigned(position.zoom)?;
            let speed = speed.map(|speed| {
                Ok::<_, CameraError>(format!(
                    "<tptz:Speed><tt:PanTilt xmlns:tt=\"http://www.onvif.org/ver10/schema\" x=\"{}\" y=\"{}\"/><tt:Zoom xmlns:tt=\"http://www.onvif.org/ver10/schema\" x=\"{}\"/></tptz:Speed>",
                    ranges.velocity_pan.map_speed(speed.pan)?,
                    ranges.velocity_tilt.map_speed(speed.tilt)?,
                    ranges.velocity_zoom.map_speed(speed.zoom)?,
                ))
            }).transpose()?.unwrap_or_default();
            Ok((
                format!(
                    "<tptz:AbsoluteMove xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ProfileToken>{profile}</tptz:ProfileToken><tptz:Position><tt:PanTilt xmlns:tt=\"http://www.onvif.org/ver10/schema\" x=\"{pan}\" y=\"{tilt}\"/><tt:Zoom xmlns:tt=\"http://www.onvif.org/ver10/schema\" x=\"{zoom}\"/></tptz:Position>{speed}</tptz:AbsoluteMove>"
                ),
                true,
                PtzResponseKind::Commanded,
            ))
        }
        PtzRequest::Relative { translation, speed } => {
            let pan = ranges.relative_pan.map_signed(translation.pan)?;
            let tilt = ranges.relative_tilt.map_signed(translation.tilt)?;
            let zoom = ranges.relative_zoom.map_signed(translation.zoom)?;
            let speed = speed.map(|speed| {
                Ok::<_, CameraError>(format!(
                    "<tptz:Speed><tt:PanTilt xmlns:tt=\"http://www.onvif.org/ver10/schema\" x=\"{}\" y=\"{}\"/><tt:Zoom xmlns:tt=\"http://www.onvif.org/ver10/schema\" x=\"{}\"/></tptz:Speed>",
                    ranges.velocity_pan.map_speed(speed.pan)?,
                    ranges.velocity_tilt.map_speed(speed.tilt)?,
                    ranges.velocity_zoom.map_speed(speed.zoom)?,
                ))
            }).transpose()?.unwrap_or_default();
            Ok((
                format!(
                    "<tptz:RelativeMove xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ProfileToken>{profile}</tptz:ProfileToken><tptz:Translation><tt:PanTilt xmlns:tt=\"http://www.onvif.org/ver10/schema\" x=\"{pan}\" y=\"{tilt}\"/><tt:Zoom xmlns:tt=\"http://www.onvif.org/ver10/schema\" x=\"{zoom}\"/></tptz:Translation>{speed}</tptz:RelativeMove>"
                ),
                true,
                PtzResponseKind::Commanded,
            ))
        }
        PtzRequest::Stop { pan, tilt, zoom } => Ok((
            format!(
                "<tptz:Stop xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ProfileToken>{profile}</tptz:ProfileToken><tptz:PanTilt>{}</tptz:PanTilt><tptz:Zoom>{}</tptz:Zoom></tptz:Stop>",
                *pan || *tilt,
                zoom
            ),
            true,
            PtzResponseKind::Commanded,
        )),
        PtzRequest::Home => Ok((
            format!(
                "<tptz:GotoHomePosition xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ProfileToken>{profile}</tptz:ProfileToken></tptz:GotoHomePosition>"
            ),
            true,
            PtzResponseKind::Commanded,
        )),
        PtzRequest::Status => Ok((
            format!(
                "<tptz:GetStatus xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ProfileToken>{profile}</tptz:ProfileToken></tptz:GetStatus>"
            ),
            false,
            PtzResponseKind::Status,
        )),
        PtzRequest::ListPresets => Ok((
            format!(
                "<tptz:GetPresets xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ProfileToken>{profile}</tptz:ProfileToken></tptz:GetPresets>"
            ),
            false,
            PtzResponseKind::Presets,
        )),
        PtzRequest::GotoPreset(token) => {
            validate_ptz_text(token, 1_024, "preset token")?;
            Ok((
                format!(
                    "<tptz:GotoPreset xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ProfileToken>{profile}</tptz:ProfileToken><tptz:PresetToken>{}</tptz:PresetToken></tptz:GotoPreset>",
                    xml_escape(token)
                ),
                true,
                PtzResponseKind::Commanded,
            ))
        }
        PtzRequest::SetPreset(name) => {
            validate_ptz_text(name, 256, "preset name")?;
            Ok((
                format!(
                    "<tptz:SetPreset xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ProfileToken>{profile}</tptz:ProfileToken><tptz:PresetName>{}</tptz:PresetName></tptz:SetPreset>",
                    xml_escape(name)
                ),
                true,
                PtzResponseKind::PresetToken,
            ))
        }
        PtzRequest::RemovePreset(token) => {
            validate_ptz_text(token, 1_024, "preset token")?;
            Ok((
                format!(
                    "<tptz:RemovePreset xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:ProfileToken>{profile}</tptz:ProfileToken><tptz:PresetToken>{}</tptz:PresetToken></tptz:RemovePreset>",
                    xml_escape(token)
                ),
                true,
                PtzResponseKind::Removed,
            ))
        }
    }
}

fn validate_ptz_text(value: &str, maximum_bytes: usize, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > maximum_bytes || value.chars().any(char::is_control) {
        return Err(security_error(format!("{label} violates its text bound")));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum PtzResponseKind {
    Commanded,
    Status,
    Presets,
    PresetToken,
    Removed,
}

fn parse_ptz_response(
    kind: PtzResponseKind,
    xml: &[u8],
    max_bytes: u64,
    max_depth: usize,
    ranges: &PtzRanges,
    observed_at: DateTime<Utc>,
) -> Result<PtzResult> {
    let root = parse_bounded_xml(xml, max_bytes, max_depth)?;
    reject_soap_fault(&root)?;
    match kind {
        PtzResponseKind::Commanded => Ok(PtzResult::Commanded),
        PtzResponseKind::Removed => Ok(PtzResult::Removed),
        PtzResponseKind::PresetToken => {
            let token = root
                .descendant_text("PresetToken")
                .ok_or_else(|| backend_error("SetPreset response omitted PresetToken"))?;
            validate_ptz_text(token, 1_024, "camera preset token")?;
            Ok(PtzResult::PresetToken(token.to_owned()))
        }
        PtzResponseKind::Presets => {
            let mut presets = Vec::new();
            for preset in root.descendants("Preset") {
                let token = preset
                    .attributes
                    .get("token")
                    .cloned()
                    .or_else(|| preset.descendant_text("token").map(str::to_owned))
                    .ok_or_else(|| backend_error("preset response omitted token"))?;
                validate_ptz_text(&token, 1_024, "camera preset token")?;
                let name = preset
                    .child("Name")
                    .map(|node| node.text.trim().to_owned())
                    .filter(|value| !value.is_empty());
                if let Some(name) = &name {
                    validate_ptz_text(name, 256, "camera preset name")?;
                }
                presets.push(PtzPreset { token, name });
            }
            if presets.len() > 1_024 {
                return Err(security_error("preset count exceeds the supported bound"));
            }
            Ok(PtzResult::Presets(presets))
        }
        PtzResponseKind::Status => {
            let pan_tilt = root.descendants("PanTilt").next();
            let zoom = root.descendants("Zoom").next();
            let position = match (pan_tilt, zoom) {
                (Some(pan_tilt), Some(zoom)) => {
                    let native_pan = parse_xml_f64_attribute(pan_tilt, "x")?;
                    let native_tilt = parse_xml_f64_attribute(pan_tilt, "y")?;
                    let native_zoom = parse_xml_f64_attribute(zoom, "x")?;
                    Some(PtzVector {
                        pan: ranges
                            .absolute_pan
                            .normalize_signed(native_pan)
                            .ok_or_else(|| backend_error("PTZ pan status cannot be normalized"))?,
                        tilt: ranges
                            .absolute_tilt
                            .normalize_signed(native_tilt)
                            .ok_or_else(|| backend_error("PTZ tilt status cannot be normalized"))?,
                        zoom: ranges
                            .absolute_zoom
                            .normalize_unsigned(native_zoom)
                            .ok_or_else(|| backend_error("PTZ zoom status cannot be normalized"))?,
                    })
                }
                _ => None,
            };
            let statuses = root
                .descendants("MoveStatus")
                .flat_map(|node| node.children.iter())
                .map(|node| node.text.trim().to_ascii_lowercase())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            let moving =
                if statuses.is_empty() || statuses.iter().any(|value| value.contains("unknown")) {
                    None
                } else {
                    Some(statuses.iter().any(|value| value.contains("moving")))
                };
            Ok(PtzResult::Status(PtzStatus {
                position,
                moving,
                observed_at,
            }))
        }
    }
}

fn parse_xml_f64_attribute(node: &XmlNode, name: &str) -> Result<f64> {
    node.attributes
        .get(name)
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .ok_or_else(|| backend_error("PTZ status contains an invalid coordinate"))
}

fn backend_error(message: impl Into<String>) -> CameraError {
    CameraError::Backend {
        backend: ONVIF_BACKEND,
        message: message.into(),
    }
}

fn security_error(message: impl AsRef<str>) -> CameraError {
    backend_error(format!(
        "security policy rejected ONVIF input: {}",
        message.as_ref()
    ))
}

fn timeout_error(stage: &'static str) -> CameraError {
    CameraError::rejected(
        ErrorCode::CaptureTimeout,
        format!("ONVIF {stage} exceeded its deadline"),
    )
}

fn cancelled_error(stage: &'static str) -> CameraError {
    CameraError::rejected(
        ErrorCode::CaptureCancelled,
        format!("ONVIF {stage} was cancelled"),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::net::IpAddr;
    use std::str::FromStr;
    use std::sync::Mutex;

    use chrono::TimeZone;
    use image::{DynamicImage, Rgb, RgbImage};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::config::{CaptureProfile, RtspSessionPolicy, TlsConfig};

    type ResolverAnswers = BTreeMap<(String, u16), VecDeque<Vec<IpAddr>>>;

    #[derive(Debug)]
    struct SequenceResolver {
        answers: Mutex<ResolverAnswers>,
    }

    impl SequenceResolver {
        fn new(entries: &[(&str, u16, &[&str])]) -> Self {
            let mut answers = BTreeMap::new();
            for (host, port, addresses) in entries {
                answers.insert(
                    ((*host).to_owned(), *port),
                    VecDeque::from([addresses
                        .iter()
                        .map(|address| IpAddr::from_str(address).expect("test IP"))
                        .collect()]),
                );
            }
            Self {
                answers: Mutex::new(answers),
            }
        }

        fn sequence(host: &str, port: u16, answers: &[&[&str]]) -> Self {
            let values = answers
                .iter()
                .map(|addresses| {
                    addresses
                        .iter()
                        .map(|address| IpAddr::from_str(address).expect("test IP"))
                        .collect()
                })
                .collect();
            Self {
                answers: Mutex::new(BTreeMap::from([((host.to_owned(), port), values)])),
            }
        }
    }

    #[async_trait]
    impl OnvifResolver for SequenceResolver {
        async fn resolve(&self, host: &str, port: u16) -> Result<Vec<IpAddr>> {
            let mut answers = self.answers.lock().expect("resolver lock");
            let sequence = answers
                .get_mut(&(host.to_owned(), port))
                .ok_or_else(|| security_error("test resolver has no answer"))?;
            if sequence.len() > 1 {
                sequence
                    .pop_front()
                    .ok_or_else(|| security_error("test resolver sequence is empty"))
            } else {
                sequence
                    .front()
                    .cloned()
                    .ok_or_else(|| security_error("test resolver sequence is empty"))
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct HttpObservation {
        url: String,
        method: OnvifHttpMethod,
        authorization: Option<String>,
        body: String,
    }

    #[derive(Default)]
    struct MockTransport {
        responses: Mutex<VecDeque<OnvifHttpResponse>>,
        observations: Mutex<Vec<HttpObservation>>,
        next_request_blocked: Mutex<Option<Arc<tokio::sync::Notify>>>,
    }

    impl MockTransport {
        fn push(&self, response: OnvifHttpResponse) {
            self.responses
                .lock()
                .expect("response lock")
                .push_back(response);
        }

        fn observations(&self) -> Vec<HttpObservation> {
            self.observations.lock().expect("request lock").clone()
        }

        fn block_next_request(&self, started: Arc<tokio::sync::Notify>) {
            *self
                .next_request_blocked
                .lock()
                .expect("blocked request lock") = Some(started);
        }
    }

    #[async_trait]
    impl OnvifHttpTransport for MockTransport {
        async fn send(&self, request: OnvifHttpRequest) -> Result<OnvifHttpResponse> {
            let authorization = request
                .authorization
                .as_ref()
                .map(|value| String::from_utf8_lossy(value.expose()).into_owned());
            self.observations
                .lock()
                .expect("request lock")
                .push(HttpObservation {
                    url: request.target.url.to_string(),
                    method: request.method,
                    authorization,
                    body: String::from_utf8_lossy(&request.body).into_owned(),
                });
            let blocked = self
                .next_request_blocked
                .lock()
                .expect("blocked request lock")
                .take();
            if let Some(started) = blocked {
                started.notify_one();
                std::future::pending().await
            }
            self.responses
                .lock()
                .expect("response lock")
                .pop_front()
                .ok_or_else(|| backend_error("test transport response queue is empty"))
        }
    }

    #[derive(Debug)]
    struct FixedClock;

    impl OnvifClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            Utc.with_ymd_and_hms(2026, 7, 10, 14, 0, 0)
                .single()
                .expect("fixed UTC instant")
        }
    }

    #[derive(Debug)]
    struct FixedNonce;

    impl OnvifNonceSource for FixedNonce {
        fn nonce(&self, length: usize) -> Result<Vec<u8>> {
            Ok((0..length).map(|value| value as u8).collect())
        }
    }

    #[derive(Debug)]
    struct FixedCredentials;

    #[async_trait]
    impl OnvifCredentialProvider for FixedCredentials {
        async fn resolve_login(&self, _reference: &SecretRef) -> Result<Arc<OnvifCredentials>> {
            Ok(Arc::new(OnvifCredentials::new(
                b"operator".to_vec(),
                b"camera-secret".to_vec(),
            )?))
        }

        async fn resolve_bytes(&self, _reference: &SecretRef) -> Result<Arc<SecretBytes>> {
            Err(security_error("test CA was not configured"))
        }
    }

    #[derive(Debug)]
    struct FixedDiscovery(Vec<DiscoveryProbeMatch>);

    #[async_trait]
    impl WsDiscovery for FixedDiscovery {
        async fn probe(
            &self,
            _deadline: Instant,
            max_results: usize,
            _cancellation: &CancellationToken,
        ) -> Result<Vec<DiscoveryProbeMatch>> {
            Ok(self.0.iter().take(max_results).cloned().collect())
        }
    }

    fn test_config(url: Option<&str>) -> OnvifBackendConfig {
        OnvifBackendConfig {
            device_service_url: url.map(str::to_owned),
            selector: None,
            credentials: None,
            media_profile: "main".to_owned(),
            capture_mode: CaptureMode::SnapshotUri,
            rtsp_fallback: false,
            rtsp_session_policy: RtspSessionPolicy::OnDemand,
            allow_insecure: true,
            allowed_uri_hosts: Vec::new(),
            allowed_uri_cidrs: Vec::new(),
            max_soap_bytes: 1_048_576,
            max_snapshot_bytes: 1_048_576,
            max_xml_depth: 64,
            tls: TlsConfig::default(),
            media_service: MediaService::Auto,
            authentication_mode: AuthenticationMode::Auto,
        }
    }

    fn ok(body: impl Into<Vec<u8>>) -> OnvifHttpResponse {
        OnvifHttpResponse {
            status: 200,
            headers: BTreeMap::new(),
            body: body.into(),
        }
    }

    fn soap_ok(body: &str) -> OnvifHttpResponse {
        ok(soap_envelope(body, None))
    }

    fn unauthorized(challenge: &str) -> OnvifHttpResponse {
        OnvifHttpResponse {
            status: 401,
            headers: BTreeMap::from([("www-authenticate".to_owned(), challenge.to_owned())]),
            body: Vec::new(),
        }
    }

    fn png(width: u32, height: u32) -> Vec<u8> {
        let image = RgbImage::from_fn(width, height, |x, y| {
            Rgb([(x % 255) as u8, (y % 255) as u8, ((x + y) % 255) as u8])
        });
        let mut output = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(image)
            .write_to(&mut output, ImageFormat::Png)
            .expect("encode test PNG");
        output.into_inner()
    }

    async fn serve_loopback_http(response: Vec<u8>) -> (u16, tokio::task::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback HTTP fixture");
        let port = listener
            .local_addr()
            .expect("inspect loopback HTTP fixture port")
            .port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept HTTP client");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 512];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).await.expect("read HTTP request");
                assert!(read > 0, "HTTP client closed before request headers");
                request.extend_from_slice(&buffer[..read]);
            }
            stream
                .write_all(&response)
                .await
                .expect("write HTTP response");
            request
        });
        (port, server)
    }

    fn loopback_http_request(
        port: u16,
        method: OnvifHttpMethod,
        maximum_body_bytes: u64,
    ) -> OnvifHttpRequest {
        OnvifHttpRequest {
            target: PinnedUri {
                url: Url::parse(&format!("http://127.0.0.1:{port}/onvif?view=main"))
                    .expect("loopback URL"),
                host: "127.0.0.1".to_owned(),
                port,
                address: "127.0.0.1".parse().expect("loopback IP"),
            },
            method,
            headers: BTreeMap::from([("x-adapter-test".to_owned(), "onvif".to_owned())]),
            authorization: Some(SecretBytes::new("Bearer adapter-test")),
            body: b"<soap/>".to_vec(),
            max_header_bytes: 4_096,
            max_body_bytes: maximum_body_bytes,
            max_decompression_ratio: 100,
            deadline: Instant::now() + Duration::from_secs(2),
            cancellation: CancellationToken::new(),
            tls: RequestTlsPolicy {
                verify_hostname: true,
                allow_invalid_certificates: false,
                ca_pem: None,
            },
        }
    }

    async fn test_client(
        resolver: Arc<dyn OnvifResolver>,
        transport: Arc<dyn OnvifHttpTransport>,
        config: &OnvifBackendConfig,
        credentials: Option<Arc<OnvifCredentials>>,
        security: SecurityConfig,
    ) -> (OnvifProtocolClient, PinnedUri) {
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        let endpoint = config
            .device_service_url
            .as_deref()
            .expect("test endpoint URL");
        let (policy, pinned) =
            UriPolicy::establish(endpoint, config, resolver.as_ref(), deadline, &cancellation)
                .await
                .expect("establish test URI policy");
        (
            OnvifProtocolClient {
                resolver,
                transport,
                clock: Arc::new(FixedClock),
                nonce_source: Arc::new(FixedNonce),
                credentials,
                policy,
                tls: RequestTlsPolicy {
                    verify_hostname: true,
                    allow_invalid_certificates: false,
                    ca_pem: None,
                },
                security,
                allow_insecure: config.allow_insecure,
                authentication_mode: config.authentication_mode,
                authentications: BTreeMap::new(),
                max_soap_bytes: config.max_soap_bytes,
                max_xml_depth: config.max_xml_depth,
            },
            pinned,
        )
    }

    #[test]
    fn secret_types_never_debug_plaintext() {
        let credentials = OnvifCredentials::new("operator", "camera-secret").expect("credentials");
        let rendered = format!("{credentials:?}");
        assert!(!rendered.contains("operator"));
        assert!(!rendered.contains("camera-secret"));
        assert!(rendered.contains("redacted"));
        assert!(OnvifCredentials::new("ambiguous:user", "password").is_err());

        let pinned = PinnedUri {
            url: Url::parse("https://camera.test/private/path?token=top-secret").expect("test URL"),
            host: "camera.test".to_owned(),
            port: 443,
            address: "10.0.0.2".parse().expect("test IP"),
        };
        let rendered = format!("{pinned:?}");
        assert!(!rendered.contains("private"));
        assert!(!rendered.contains("top-secret"));
    }

    #[tokio::test]
    async fn production_http_transport_uses_pinned_loopback_and_enforces_response_bounds() {
        let transport = ReqwestOnvifTransport;
        let (port, server) = serve_loopback_http(
            b"HTTP/1.1 200 OK\r\nX-Result: one\r\nX-Result: two\r\nContent-Length: 5\r\n\r\nhello"
                .to_vec(),
        )
        .await;
        let response = transport
            .send(loopback_http_request(port, OnvifHttpMethod::Post, 32))
            .await
            .expect("pinned loopback request must complete");
        let request =
            String::from_utf8(server.await.expect("HTTP server task")).expect("HTTP text");
        assert!(request.starts_with("POST /onvif?view=main HTTP/1.1\r\n"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer adapter-test")
        );
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
        assert!(
            response
                .header("x-result")
                .is_some_and(|value| value.contains("one") && value.contains("two"))
        );

        let (port, server) = serve_loopback_http(
            b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n".to_vec(),
        )
        .await;
        assert!(
            transport
                .send(loopback_http_request(port, OnvifHttpMethod::Get, 32))
                .await
                .is_err()
        );
        let _ = server.await.expect("compressed-response server task");

        let (port, server) =
            serve_loopback_http(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello".to_vec())
                .await;
        assert!(
            transport
                .send(loopback_http_request(port, OnvifHttpMethod::Get, 4))
                .await
                .is_err()
        );
        let _ = server.await.expect("oversized-response server task");

        let mut invalid = loopback_http_request(1, OnvifHttpMethod::Get, 32);
        invalid
            .headers
            .insert("host".to_owned(), "forbidden".to_owned());
        assert!(transport.send(invalid).await.is_err());
    }

    #[tokio::test]
    async fn public_discovery_normalizes_duplicate_identity_before_exposing_a_candidate() {
        let factory = OnvifBackendFactory::new(OnvifBackendDependencies {
            resolver: Arc::new(SequenceResolver::new(&[])),
            discovery: Arc::new(FixedDiscovery(vec![
                DiscoveryProbeMatch {
                    endpoint_reference: "urn:uuid:camera-1".to_owned(),
                    xaddrs: vec!["http://camera.test/onvif/device_service".to_owned()],
                    vendor: Some("Edge Commons".to_owned()),
                    model: Some("Simulator".to_owned()),
                },
                DiscoveryProbeMatch {
                    endpoint_reference: "urn:uuid:camera-1".to_owned(),
                    xaddrs: vec!["http://camera.test/onvif/device_service".to_owned()],
                    vendor: Some("Edge Commons".to_owned()),
                    model: Some("Simulator".to_owned()),
                },
            ])),
            transport: Arc::new(MockTransport::default()),
            credentials: None,
            clock: Arc::new(FixedClock),
            nonce_source: Arc::new(FixedNonce),
            security: SecurityConfig::default(),
        });
        let candidates = factory
            .discover(DiscoveryRequest {
                eligible_interfaces: vec!["eth0".to_owned()],
                timeout: Duration::from_secs(1),
                max_results: 2,
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("equivalent discovery observations must coalesce");
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].selector["endpointReference"],
            "urn:uuid:camera-1"
        );
        assert_eq!(candidates[0].vendor.as_deref(), Some("Edge Commons"));
        assert_eq!(candidates[0].capabilities["xaddrCount"], 1);
    }

    #[tokio::test]
    async fn bounded_credentials_and_system_primitives_have_real_deadline_paths() {
        let reference = SecretRef {
            secret: "camera/login".to_owned(),
            field: None,
        };
        let provider = FixedCredentials;
        let credentials = resolve_login_bounded(
            &provider,
            &reference,
            Instant::now() + Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .expect("bounded login resolution");
        assert_eq!(credentials.username().expect("username"), "operator");
        assert!(
            resolve_bytes_bounded(
                &provider,
                &reference,
                Instant::now() + Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .is_err()
        );

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert_eq!(
            resolve_login_bounded(
                &provider,
                &reference,
                Instant::now() + Duration::from_secs(1),
                &cancelled,
            )
            .await
            .expect_err("cancelled credential lookup")
            .code(),
            ErrorCode::CaptureCancelled
        );
        assert_eq!(
            resolve_login_bounded(
                &provider,
                &reference,
                Instant::now(),
                &CancellationToken::new(),
            )
            .await
            .expect_err("expired credential lookup")
            .code(),
            ErrorCode::CaptureTimeout
        );

        let resolved = SystemResolver
            .resolve("127.0.0.1", 80)
            .await
            .expect("numeric loopback lookup");
        assert!(resolved.iter().any(IpAddr::is_loopback));
        assert_eq!(SystemNonceSource.nonce(24).expect("nonce").len(), 24);
        assert!(SystemOnvifClock.now() <= Utc::now());
    }

    #[test]
    fn snapshot_frame_validation_covers_fallback_fatal_and_ratio_boundaries() {
        let observed_at = FixedClock.now();
        let decode = |headers: BTreeMap<String, String>, body: Vec<u8>, limit, ratio| {
            snapshot_to_frame(
                OnvifHttpResponse {
                    status: 200,
                    headers,
                    body,
                },
                limit,
                ratio,
                observed_at,
                "camera.test",
            )
        };
        assert!(matches!(
            decode(BTreeMap::new(), Vec::new(), 1_024, 100),
            Err(SnapshotAttemptFailure::Fallback {
                reason: SnapshotFallbackReason::CorruptImage,
                ..
            })
        ));
        assert!(matches!(
            decode(
                BTreeMap::from([("content-type".to_owned(), "text/plain".to_owned())]),
                b"not-an-image".to_vec(),
                1_024,
                100,
            ),
            Err(SnapshotAttemptFailure::Fallback {
                reason: SnapshotFallbackReason::UnsupportedContentType,
                ..
            })
        ));
        assert!(matches!(
            decode(
                BTreeMap::from([("content-type".to_owned(), "image/jpeg".to_owned())]),
                png(8, 8),
                1_024_000,
                100,
            ),
            Err(SnapshotAttemptFailure::Fatal(_))
        ));
        let mut truncated_png = png(8, 8);
        truncated_png.truncate(24);
        assert!(matches!(
            decode(
                BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
                truncated_png,
                1_024,
                100,
            ),
            Err(SnapshotAttemptFailure::Fallback {
                reason: SnapshotFallbackReason::CorruptImage,
                ..
            })
        ));

        let image = RgbImage::from_pixel(32, 32, Rgb([1, 2, 3]));
        let mut encoded = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(image)
            .write_to(&mut encoded, ImageFormat::Png)
            .expect("encode compact PNG");
        assert!(matches!(
            decode(
                BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
                encoded.into_inner(),
                1_024_000,
                1,
            ),
            Err(SnapshotAttemptFailure::Fatal(error)) if error.code() == ErrorCode::ResourceLimit
        ));
    }

    #[tokio::test]
    async fn soap_response_terminal_statuses_reject_unsafe_or_invalid_responses() {
        let config = test_config(Some("http://camera.test/onvif/device_service"));
        let (mut client, endpoint) = test_client(
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])])),
            Arc::new(MockTransport::default()),
            &config,
            None,
            SecurityConfig::default(),
        )
        .await;
        for response in [
            OnvifHttpResponse {
                status: 302,
                headers: BTreeMap::new(),
                body: Vec::new(),
            },
            OnvifHttpResponse {
                status: 401,
                headers: BTreeMap::new(),
                body: Vec::new(),
            },
            OnvifHttpResponse {
                status: 500,
                headers: BTreeMap::new(),
                body: b"not XML".to_vec(),
            },
        ] {
            assert!(client.finish_soap_response(response).is_err());
        }
        assert_eq!(
            client
                .finish_soap_response(soap_ok("<GetServicesResponse/>"))
                .expect("valid SOAP response"),
            soap_envelope("<GetServicesResponse/>", None)
        );
        assert_eq!(
            client
                .credentials()
                .expect_err("missing credentials are rejected")
                .code(),
            ErrorCode::BackendError
        );
        assert!(!client.basic_is_allowed(&endpoint));
        assert!(
            client
                .authorization_for_session(OnvifHttpMethod::Post, &endpoint)
                .expect("no established authentication")
                .is_none()
        );
        assert!(
            client
                .wsse_for_session(&endpoint)
                .expect("no established WS-Security authentication")
                .is_none()
        );
    }

    #[tokio::test]
    async fn closed_session_rejects_work_after_safe_no_ptz_status() {
        let config = test_config(Some("http://camera.test/onvif/device_service"));
        let (client, endpoint) = test_client(
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])])),
            Arc::new(MockTransport::default()),
            &config,
            None,
            SecurityConfig::default(),
        )
        .await;
        let mut session = OnvifSession {
            instance_id: "camera-a".to_owned(),
            client,
            media_endpoint: endpoint,
            media_version: MediaVersion::Media1,
            media_profile_token: "main".to_owned(),
            snapshot_endpoint: None,
            max_snapshot_bytes: 1_024,
            ptz_endpoint: None,
            ptz_ranges: None,
            ptz_home: false,
            capabilities: CameraCapabilities {
                capture_modes: vec![CaptureMode::SnapshotUri],
                pixel_formats: vec![PixelFormat::Jpeg],
                software_trigger: false,
                snapshot_uri: false,
                rtsp: false,
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
            closed: false,
        };
        assert!(
            session
                .status()
                .await
                .expect("status without PTZ")
                .ptz
                .is_none()
        );
        let profile: CaptureProfile = serde_json::from_value(json!({
            "captureMode": "software-trigger",
            "output": { "encoding": "jpeg" }
        }))
        .expect("capture profile");
        assert_eq!(
            session
                .capture(CaptureRequest {
                    capture_id: "closed-session".to_owned(),
                    profile,
                    maximum_frame_bytes: 1_024,
                    timeout: Duration::from_secs(1),
                    cancellation: CancellationToken::new(),
                })
                .await
                .expect_err("unsupported capture mode")
                .code(),
            ErrorCode::UnsupportedCapability
        );
        session.close().await.expect("first close");
        session.close().await.expect("idempotent close");
        assert_eq!(
            session.status().await.expect_err("closed status").code(),
            ErrorCode::CameraUnavailable
        );
    }

    #[tokio::test]
    async fn uri_policy_rejects_dns_rebinding_and_mixed_answers() {
        let resolver: Arc<dyn OnvifResolver> = Arc::new(SequenceResolver::sequence(
            "camera.test",
            80,
            &[
                &["10.0.0.2"],
                &["10.0.0.3"],
                &["10.0.0.2", "169.254.169.254"],
            ],
        ));
        let config = test_config(Some("http://camera.test/onvif/device_service"));
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        let (policy, _) = UriPolicy::establish(
            config.device_service_url.as_deref().expect("URL"),
            &config,
            resolver.as_ref(),
            deadline,
            &cancellation,
        )
        .await
        .expect("initial endpoint");
        assert!(
            policy
                .pin(
                    "http://camera.test/snapshot",
                    resolver.as_ref(),
                    deadline,
                    &cancellation,
                )
                .await
                .is_err()
        );
        assert!(
            policy
                .pin(
                    "http://camera.test/snapshot",
                    resolver.as_ref(),
                    deadline,
                    &cancellation,
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn stored_service_endpoint_is_repinned_before_each_http_connection() {
        let resolver: Arc<dyn OnvifResolver> = Arc::new(SequenceResolver::sequence(
            "camera.test",
            80,
            &[&["10.0.0.2"], &["10.0.0.3"]],
        ));
        let transport = Arc::new(MockTransport::default());
        transport.push(soap_ok("<GetServicesResponse/>"));
        let config = test_config(Some("http://camera.test/onvif/device_service"));
        let (mut client, stored_endpoint) = test_client(
            resolver,
            transport.clone(),
            &config,
            None,
            SecurityConfig::default(),
        )
        .await;
        let cancellation = CancellationToken::new();
        assert!(
            client
                .soap_call(
                    stored_endpoint,
                    "urn:test/GetServices",
                    "<GetServices/>",
                    false,
                    Instant::now() + Duration::from_secs(2),
                    &cancellation,
                )
                .await
                .is_err()
        );
        assert!(transport.observations().is_empty());
    }

    #[tokio::test]
    async fn blocking_dns_and_decoder_worker_admission_obey_deadlines() {
        let dns_permits = Arc::clone(blocking_dns_limiter())
            .acquire_many_owned(MAX_BLOCKING_DNS_LOOKUPS as u32)
            .await
            .expect("hold DNS worker permits");
        let cancellation = CancellationToken::new();
        assert!(
            resolve_bounded(
                &SystemResolver,
                "localhost",
                80,
                Instant::now() + Duration::from_millis(20),
                &cancellation,
            )
            .await
            .is_err()
        );
        drop(dns_permits);

        let decoder_permits = Arc::clone(blocking_image_limiter())
            .acquire_many_owned(MAX_BLOCKING_IMAGE_DECODES as u32)
            .await
            .expect("hold decoder worker permits");
        let response = OnvifHttpResponse {
            status: 200,
            headers: BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
            body: png(8, 8),
        };
        assert!(
            decode_snapshot_at_safe_boundary(
                response,
                1_048_576,
                SecurityConfig::default().max_decompression_ratio,
                FixedClock.now(),
                "camera.test".to_owned(),
                Instant::now() + Duration::from_millis(20),
                &cancellation,
            )
            .await
            .is_err()
        );
        drop(decoder_permits);
    }

    #[test]
    fn bounded_xml_rejects_dtd_entities_and_excess_depth() {
        assert!(
            parse_bounded_xml(br#"<!DOCTYPE x [<!ENTITY e "boom">]><x>&e;</x>"#, 1_024, 8).is_err()
        );
        assert!(parse_bounded_xml(b"<a><b><c/></b></a>", 1_024, 2).is_err());
        assert!(parse_bounded_xml(b"<a><b/></a>", 1_024, 2).is_ok());

        let mut too_many_elements = String::from("<root>");
        for _ in 0..MAX_XML_ELEMENTS {
            too_many_elements.push_str("<x/>");
        }
        too_many_elements.push_str("</root>");
        assert!(
            parse_bounded_xml(
                too_many_elements.as_bytes(),
                too_many_elements.len() as u64,
                8,
            )
            .is_err()
        );
    }

    #[test]
    fn digest_authorization_matches_fixed_md5_vector() {
        let challenge = parse_digest_challenge(
            r#"Digest realm="test", nonce="abc123", algorithm=MD5, qop="auth""#,
        )
        .expect("challenge");
        let credentials = OnvifCredentials::new("operator", "camera-secret").expect("credentials");
        let authorization = digest_authorization(
            &challenge,
            &credentials,
            OnvifHttpMethod::Get,
            "/snapshot",
            1,
            &FixedNonce,
        )
        .expect("authorization");
        let value = authorization.expose_utf8().expect("header text");
        assert!(value.contains("response=\"ec1edaa486e7484a89f4a4a88130b012\""));
        assert!(value.contains("cnonce=\"000102030405060708090a0b0c0d0e0f\""));
        assert!(!value.contains("camera-secret"));
    }

    #[test]
    fn digest_parser_does_not_absorb_following_authentication_schemes() {
        let digest_first = parse_digest_challenge(
            r#"Digest realm="digest", nonce="n1", algorithm=MD5, qop="auth", Basic realm="basic", nonce="poison""#,
        )
        .expect("Digest followed by Basic");
        assert_eq!(digest_first.realm, "digest");
        assert_eq!(digest_first.nonce, "n1");

        let digest_second = parse_digest_challenge(
            r#"Basic realm="basic", Digest realm="digest", nonce="n2", algorithm=SHA-256, qop="auth", Bearer realm="other""#,
        )
        .expect("Digest between other schemes");
        assert_eq!(digest_second.realm, "digest");
        assert_eq!(digest_second.nonce, "n2");
        assert_eq!(digest_second.algorithm, DigestAlgorithm::Sha256);

        assert!(parse_digest_challenge(r#"Digest realm="digest", nonce="n1", malformed"#).is_err());
    }

    #[test]
    fn response_header_bound_counts_duplicate_field_lines() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.append("x-test", "alpha".parse().expect("header value"));
        headers.append("x-test", "bravo".parse().expect("header value"));
        assert!(collect_bounded_response_headers(&headers, 282).is_err());
        let collected = collect_bounded_response_headers(&headers, 288).expect("exact bound");
        assert_eq!(
            collected.get("x-test").map(String::as_str),
            Some("alpha, bravo")
        );
    }

    #[test]
    fn basic_authentication_is_double_gated_on_plaintext() {
        let credentials = OnvifCredentials::new("operator", "camera-secret").expect("credentials");
        assert!(basic_authorization(&credentials, false).is_err());
        let value = basic_authorization(&credentials, true).expect("allowed basic");
        assert!(value.expose_utf8().expect("header").starts_with("Basic "));
    }

    #[tokio::test]
    async fn auto_authentication_prefers_digest_before_basic() {
        let resolver: Arc<dyn OnvifResolver> =
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])]));
        let transport = Arc::new(MockTransport::default());
        transport.push(unauthorized(
            r#"Basic realm="fallback", Digest realm="test", nonce="abc123", algorithm=MD5, qop="auth""#,
        ));
        transport.push(soap_ok("<tds:GetServicesResponse xmlns:tds=\"urn:test\"/>"));
        let mut config = test_config(Some("http://camera.test/onvif/device_service"));
        config.authentication_mode = AuthenticationMode::Auto;
        let credentials =
            Arc::new(OnvifCredentials::new("operator", "camera-secret").expect("credentials"));
        let (mut client, endpoint) = test_client(
            resolver,
            transport.clone(),
            &config,
            Some(credentials),
            SecurityConfig::default(),
        )
        .await;
        let cancellation = CancellationToken::new();
        client
            .establish_authentication(
                endpoint.clone(),
                "urn:test/GetServices",
                "<GetServices/>",
                Instant::now() + Duration::from_secs(2),
                &cancellation,
            )
            .await
            .expect("Digest establishment");
        assert_eq!(client.authentication_label(&endpoint), "http-digest");
        let requests = transport.observations();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].authorization.is_none());
        assert!(
            requests[1]
                .authorization
                .as_deref()
                .is_some_and(|value| value.starts_with("Digest "))
        );
    }

    #[tokio::test]
    async fn auto_authentication_falls_back_to_permitted_basic_when_digest_is_unavailable() {
        let resolver: Arc<dyn OnvifResolver> =
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])]));
        let transport = Arc::new(MockTransport::default());
        transport.push(unauthorized(r#"Basic realm="camera""#));
        transport.push(soap_ok("<tds:GetServicesResponse xmlns:tds=\"urn:test\"/>"));
        let mut config = test_config(Some("http://camera.test/onvif/device_service"));
        config.authentication_mode = AuthenticationMode::Auto;
        let security = SecurityConfig {
            allow_basic_over_plaintext: true,
            ..SecurityConfig::default()
        };
        let credentials =
            Arc::new(OnvifCredentials::new("operator", "camera-secret").expect("credentials"));
        let (mut client, endpoint) = test_client(
            resolver,
            transport.clone(),
            &config,
            Some(credentials),
            security,
        )
        .await;
        let cancellation = CancellationToken::new();

        client
            .establish_authentication(
                endpoint.clone(),
                "urn:test/GetServices",
                "<GetServices/>",
                Instant::now() + Duration::from_secs(2),
                &cancellation,
            )
            .await
            .expect("permitted Basic fallback");

        assert_eq!(client.authentication_label(&endpoint), "basic");
        let requests = transport.observations();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].authorization.is_none());
        assert!(
            requests[1]
                .authorization
                .as_deref()
                .is_some_and(|value| value.starts_with("Basic "))
        );
    }

    #[tokio::test]
    async fn explicit_wsse_and_read_only_digest_retry_are_established_before_use() {
        let config = test_config(Some("http://camera.test/onvif/device_service"));
        let credentials =
            Arc::new(OnvifCredentials::new("operator", "camera-secret").expect("test credentials"));

        let wsse_transport = Arc::new(MockTransport::default());
        wsse_transport.push(soap_ok("<GetServicesResponse/>"));
        let mut wsse_config = config.clone();
        wsse_config.authentication_mode = AuthenticationMode::WsseDigest;
        let (mut wsse, endpoint) = test_client(
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])])),
            wsse_transport.clone(),
            &wsse_config,
            Some(Arc::clone(&credentials)),
            SecurityConfig::default(),
        )
        .await;
        wsse.establish_authentication(
            endpoint,
            "urn:test/GetServices",
            "<GetServices/>",
            Instant::now() + Duration::from_secs(2),
            &CancellationToken::new(),
        )
        .await
        .expect("WS-Security establishment");
        assert!(
            wsse_transport.observations()[0]
                .body
                .contains("UsernameToken")
        );

        let digest_transport = Arc::new(MockTransport::default());
        digest_transport.push(unauthorized(
            r#"Digest realm="camera", nonce="n1", algorithm=MD5, qop="auth""#,
        ));
        digest_transport.push(soap_ok("<GetStatusResponse/>"));
        let (mut digest, endpoint) = test_client(
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])])),
            digest_transport.clone(),
            &config,
            Some(credentials),
            SecurityConfig::default(),
        )
        .await;
        digest
            .soap_call(
                endpoint,
                "urn:test/GetStatus",
                "<GetStatus/>",
                false,
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await
            .expect("read-only Digest retry");
        let observations = digest_transport.observations();
        assert_eq!(observations.len(), 2);
        assert!(observations[0].authorization.is_none());
        assert!(
            observations[1]
                .authorization
                .as_deref()
                .is_some_and(|value| value.starts_with("Digest "))
        );
    }

    #[tokio::test]
    async fn snapshot_endpoint_fault_and_redirect_paths_stay_in_the_pinned_origin() {
        let config = test_config(Some("http://camera.test/onvif/device_service"));
        let fault_transport = Arc::new(MockTransport::default());
        fault_transport.push(soap_ok(
            "<s:Fault xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\"><s:Code><s:Value>ter:ActionNotSupported</s:Value></s:Code></s:Fault>",
        ));
        let (mut client, endpoint) = test_client(
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])])),
            fault_transport,
            &config,
            None,
            SecurityConfig::default(),
        )
        .await;
        assert!(matches!(
            client
                .resolve_snapshot_endpoint(
                    endpoint,
                    "urn:test/GetSnapshotUri",
                    "<GetSnapshotUri/>",
                    Instant::now() + Duration::from_secs(2),
                    &CancellationToken::new(),
                )
                .await
                .expect("snapshot SOAP fault must become a fallback reason"),
            SnapshotEndpointAvailability::Unavailable(SnapshotFallbackReason::ActionNotSupported)
        ));

        let redirect_transport = Arc::new(MockTransport::default());
        redirect_transport.push(OnvifHttpResponse {
            status: 302,
            headers: BTreeMap::from([("location".to_owned(), "/snapshot/final.png".to_owned())]),
            body: Vec::new(),
        });
        redirect_transport.push(OnvifHttpResponse {
            status: 200,
            headers: BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
            body: png(8, 8),
        });
        let (redirect_client, endpoint) = test_client(
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])])),
            redirect_transport,
            &config,
            None,
            SecurityConfig::default(),
        )
        .await;
        let (_, final_target) = redirect_client
            .fetch_snapshot(
                endpoint,
                1_048_576,
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await
            .expect("same-origin snapshot redirect");
        assert_eq!(final_target.host(), "camera.test");
        assert_eq!(final_target.url().path(), "/snapshot/final.png");
    }

    #[tokio::test]
    async fn mutating_soap_authentication_failure_is_never_retried() {
        let resolver: Arc<dyn OnvifResolver> =
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])]));
        let transport = Arc::new(MockTransport::default());
        transport.push(unauthorized(
            r#"Digest realm="test", nonce="new", algorithm=MD5, qop="auth""#,
        ));
        let config = test_config(Some("http://camera.test/onvif/device_service"));
        let credentials =
            Arc::new(OnvifCredentials::new("operator", "camera-secret").expect("credentials"));
        let (mut client, endpoint) = test_client(
            resolver,
            transport.clone(),
            &config,
            Some(credentials),
            SecurityConfig::default(),
        )
        .await;
        client.authentications.insert(
            endpoint.origin_key(),
            SessionAuthentication::HttpDigest {
                challenge: parse_digest_challenge(
                    r#"Digest realm="test", nonce="old", algorithm=MD5, qop="auth""#,
                )
                .expect("challenge"),
                nonce_count: 0,
            },
        );
        let cancellation = CancellationToken::new();
        assert!(
            client
                .soap_call(
                    endpoint,
                    "urn:test/AbsoluteMove",
                    "<AbsoluteMove/>",
                    true,
                    Instant::now() + Duration::from_secs(2),
                    &cancellation,
                )
                .await
                .is_err()
        );
        assert_eq!(transport.observations().len(), 1);
    }

    #[tokio::test]
    async fn soap_authentication_is_established_per_origin_without_header_carryover() {
        let resolver: Arc<dyn OnvifResolver> = Arc::new(SequenceResolver::new(&[
            ("camera.test", 80, &["10.0.0.2"]),
            ("ptz.test", 80, &["10.0.0.3"]),
        ]));
        let transport = Arc::new(MockTransport::default());
        transport.push(unauthorized(
            r#"Digest realm="ptz", nonce="fresh", algorithm=MD5, qop="auth""#,
        ));
        transport.push(soap_ok("<GetStatusResponse/>"));
        transport.push(soap_ok("<AbsoluteMoveResponse/>"));
        let mut config = test_config(Some("http://camera.test/onvif/device_service"));
        config.allowed_uri_hosts = vec!["ptz.test".to_owned()];
        config.allowed_uri_cidrs = vec!["10.0.0.0/24".parse().expect("CIDR")];
        let credentials =
            Arc::new(OnvifCredentials::new("operator", "camera-secret").expect("credentials"));
        let (mut client, device) = test_client(
            Arc::clone(&resolver),
            transport.clone(),
            &config,
            Some(credentials),
            SecurityConfig::default(),
        )
        .await;
        client
            .authentications
            .insert(device.origin_key(), SessionAuthentication::Basic);
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        let ptz = client
            .pin("http://ptz.test/onvif/ptz_service", deadline, &cancellation)
            .await
            .expect("PTZ target");
        client
            .soap_call(
                ptz.clone(),
                "urn:test/GetStatus",
                "<GetStatus/>",
                false,
                deadline,
                &cancellation,
            )
            .await
            .expect("read-only authentication establishment");
        client
            .soap_call(
                ptz,
                "urn:test/AbsoluteMove",
                "<AbsoluteMove/>",
                true,
                deadline,
                &cancellation,
            )
            .await
            .expect("established mutation");
        let requests = transport.observations();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].authorization.is_none());
        assert!(
            requests[1]
                .authorization
                .as_deref()
                .is_some_and(|value| value.starts_with("Digest "))
        );
        assert!(
            requests[2]
                .authorization
                .as_deref()
                .is_some_and(|value| value.starts_with("Digest "))
        );
    }

    #[tokio::test]
    async fn cross_origin_snapshot_redirect_revalidates_and_strips_authorization() {
        let resolver: Arc<dyn OnvifResolver> = Arc::new(SequenceResolver::new(&[
            ("camera.test", 80, &["10.0.0.2"]),
            ("cdn.test", 80, &["10.0.0.3"]),
        ]));
        let transport = Arc::new(MockTransport::default());
        transport.push(OnvifHttpResponse {
            status: 302,
            headers: BTreeMap::from([(
                "location".to_owned(),
                "http://cdn.test/snapshot.png".to_owned(),
            )]),
            body: Vec::new(),
        });
        transport.push(OnvifHttpResponse {
            status: 200,
            headers: BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
            body: png(8, 8),
        });
        let mut config = test_config(Some("http://camera.test/onvif/device_service"));
        config.allowed_uri_hosts = vec!["cdn.test".to_owned()];
        config.allowed_uri_cidrs = vec!["10.0.0.0/24".parse().expect("CIDR")];
        config.authentication_mode = AuthenticationMode::Basic;
        let security = SecurityConfig {
            allow_basic_over_plaintext: true,
            ..SecurityConfig::default()
        };
        let credentials =
            Arc::new(OnvifCredentials::new("operator", "camera-secret").expect("credentials"));
        let (mut client, endpoint) = test_client(
            resolver,
            transport.clone(),
            &config,
            Some(credentials),
            security,
        )
        .await;
        client
            .authentications
            .insert(endpoint.origin_key(), SessionAuthentication::Basic);
        let cancellation = CancellationToken::new();
        let (response, target) = client
            .fetch_snapshot(
                endpoint,
                1_048_576,
                Instant::now() + Duration::from_secs(2),
                &cancellation,
            )
            .await
            .expect("redirected snapshot");
        assert_eq!(target.host(), "cdn.test");
        assert_eq!(response.status, 200);
        let requests = transport.observations();
        assert!(
            requests[0]
                .authorization
                .as_deref()
                .is_some_and(|value| value.starts_with("Basic "))
        );
        assert!(requests[1].authorization.is_none());
    }

    #[test]
    fn snapshot_png_is_validated_before_bounded_rgb_decode() {
        let response = OnvifHttpResponse {
            status: 200,
            headers: BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
            body: png(16, 8),
        };
        let frame = snapshot_to_frame(
            response,
            4_096,
            SecurityConfig::default().max_decompression_ratio,
            FixedClock.now(),
            "camera.test",
        )
        .expect("PNG frame");
        assert_eq!(frame.pixel_format, PixelFormat::Rgb8);
        assert_eq!((frame.width, frame.height), (16, 8));
        assert_eq!(frame.bytes.len(), 16 * 8 * 3);

        let corrupt = OnvifHttpResponse {
            status: 200,
            headers: BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
            body: b"not-a-png".to_vec(),
        };
        assert!(
            snapshot_to_frame(
                corrupt,
                4_096,
                SecurityConfig::default().max_decompression_ratio,
                FixedClock.now(),
                "camera.test",
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn media_auto_prefers_matching_media2_profile() {
        let resolver: Arc<dyn OnvifResolver> =
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])]));
        let transport = Arc::new(MockTransport::default());
        transport.push(soap_ok(
            "<tr2:GetProfilesResponse xmlns:tr2=\"http://www.onvif.org/ver20/media/wsdl\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tr2:Profiles token=\"main\"><tt:Name>main</tt:Name></tr2:Profiles></tr2:GetProfilesResponse>",
        ));
        let config = test_config(Some("http://camera.test/onvif/device_service"));
        let (mut client, _) = test_client(
            resolver,
            transport.clone(),
            &config,
            None,
            SecurityConfig::default(),
        )
        .await;
        let services = ServiceEndpoints {
            media1: vec!["http://camera.test/onvif/media_service".to_owned()],
            media2: vec!["http://camera.test/onvif/media2_service".to_owned()],
            ptz: Vec::new(),
        };
        let cancellation = CancellationToken::new();
        let selected = select_media_profile(
            &mut client,
            &services,
            MediaService::Auto,
            "main",
            Instant::now() + Duration::from_secs(2),
            &cancellation,
        )
        .await
        .expect("Media2 selection");
        assert_eq!(selected.version, MediaVersion::Media2);
        assert_eq!(transport.observations().len(), 1);
    }

    #[test]
    fn ptz_ranges_map_and_status_normalizes_camera_coordinates() {
        let options = soap_envelope(
            "<tptz:GetConfigurationOptionsResponse xmlns:tptz=\"http://www.onvif.org/ver20/ptz/wsdl\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tptz:PTZConfigurationOptions><tt:Spaces><tt:AbsolutePanTiltPositionSpace><tt:XRange><tt:Min>-2</tt:Min><tt:Max>2</tt:Max></tt:XRange><tt:YRange><tt:Min>-2</tt:Min><tt:Max>2</tt:Max></tt:YRange></tt:AbsolutePanTiltPositionSpace><tt:AbsoluteZoomPositionSpace><tt:XRange><tt:Min>0</tt:Min><tt:Max>4</tt:Max></tt:XRange></tt:AbsoluteZoomPositionSpace><tt:RelativePanTiltTranslationSpace><tt:XRange><tt:Min>-0.5</tt:Min><tt:Max>0.5</tt:Max></tt:XRange><tt:YRange><tt:Min>-0.5</tt:Min><tt:Max>0.5</tt:Max></tt:YRange></tt:RelativePanTiltTranslationSpace><tt:RelativeZoomTranslationSpace><tt:XRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:XRange></tt:RelativeZoomTranslationSpace><tt:ContinuousPanTiltVelocitySpace><tt:XRange><tt:Min>-3</tt:Min><tt:Max>3</tt:Max></tt:XRange><tt:YRange><tt:Min>-3</tt:Min><tt:Max>3</tt:Max></tt:YRange></tt:ContinuousPanTiltVelocitySpace><tt:ContinuousZoomVelocitySpace><tt:XRange><tt:Min>-2</tt:Min><tt:Max>2</tt:Max></tt:XRange></tt:ContinuousZoomVelocitySpace></tt:Spaces></tptz:PTZConfigurationOptions></tptz:GetConfigurationOptionsResponse>",
            None,
        );
        let ranges = parse_ptz_ranges(&options, 100_000, 64).expect("PTZ ranges");
        let (request, mutating, _) = build_ptz_request(
            &PtzRequest::Absolute {
                position: PtzVector {
                    pan: 1.0,
                    tilt: -1.0,
                    zoom: 0.5,
                },
                speed: None,
            },
            "main",
            &ranges,
        )
        .expect("absolute request");
        assert!(mutating);
        assert!(request.contains("x=\"2\" y=\"-2\""));
        assert!(request.contains("<tt:Zoom"));
        assert!(request.contains("x=\"2\""));

        let (request, _, _) = build_ptz_request(
            &PtzRequest::Absolute {
                position: PtzVector {
                    pan: 0.0,
                    tilt: 0.0,
                    zoom: 0.0,
                },
                speed: Some(PtzVector {
                    pan: 0.0,
                    tilt: 0.5,
                    zoom: 1.0,
                }),
            },
            "main",
            &ranges,
        )
        .expect("non-negative speed mapping");
        assert!(request.contains("<tptz:Speed>"));
        assert!(request.contains("x=\"0\" y=\"1.5\""));
        assert!(request.contains("<tt:Zoom"));
        assert!(request.contains("x=\"2\""));

        let status = soap_envelope(
            "<tptz:GetStatusResponse xmlns:tptz=\"http://www.onvif.org/ver20/ptz/wsdl\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tptz:PTZStatus><tt:Position><tt:PanTilt x=\"2\" y=\"-2\"/><tt:Zoom x=\"4\"/></tt:Position><tt:MoveStatus><tt:PanTilt>IDLE</tt:PanTilt><tt:Zoom>IDLE</tt:Zoom></tt:MoveStatus></tptz:PTZStatus></tptz:GetStatusResponse>",
            None,
        );
        let PtzResult::Status(status) = parse_ptz_response(
            PtzResponseKind::Status,
            &status,
            100_000,
            64,
            &ranges,
            FixedClock.now(),
        )
        .expect("status") else {
            panic!("expected PTZ status");
        };
        assert_eq!(status.moving, Some(false));
        assert_eq!(status.position.expect("position").zoom, 1.0);
    }

    #[test]
    fn ptz_request_builder_preserves_operation_semantics_and_escapes_tokens() {
        let range = |min, max| AxisRange { min, max };
        let ranges = PtzRanges {
            absolute_pan: range(-2.0, 2.0),
            absolute_tilt: range(-4.0, 4.0),
            absolute_zoom: range(0.0, 5.0),
            relative_pan: range(-0.5, 0.5),
            relative_tilt: range(-1.0, 1.0),
            relative_zoom: range(-2.0, 2.0),
            velocity_pan: range(-3.0, 3.0),
            velocity_tilt: range(-4.0, 4.0),
            velocity_zoom: range(-5.0, 5.0),
        };
        let profile = "main<&>";

        let (continuous, mutating, kind) = build_ptz_request(
            &PtzRequest::Continuous {
                velocity: PtzVector {
                    pan: 0.5,
                    tilt: -1.0,
                    zoom: 0.0,
                },
                timeout: Duration::from_millis(1_500),
            },
            profile,
            &ranges,
        )
        .expect("continuous request");
        assert!(mutating);
        assert!(matches!(kind, PtzResponseKind::Commanded));
        assert!(continuous.contains("main&lt;&amp;&gt;"));
        assert!(continuous.contains("x=\"1.5\" y=\"-4\""));
        assert!(continuous.contains("<tptz:Timeout>PT1.500S</tptz:Timeout>"));

        let (relative, mutating, kind) = build_ptz_request(
            &PtzRequest::Relative {
                translation: PtzVector {
                    pan: 1.0,
                    tilt: -1.0,
                    zoom: 0.5,
                },
                speed: Some(PtzVector {
                    pan: 0.5,
                    tilt: 1.0,
                    zoom: 0.0,
                }),
            },
            profile,
            &ranges,
        )
        .expect("relative request");
        assert!(mutating);
        assert!(matches!(kind, PtzResponseKind::Commanded));
        assert!(relative.contains("x=\"0.5\" y=\"-1\""));
        assert!(relative.contains("<tptz:Speed>"));
        assert!(relative.contains("x=\"1.5\" y=\"4\""));

        let (stop, mutating, kind) = build_ptz_request(
            &PtzRequest::Stop {
                pan: false,
                tilt: true,
                zoom: false,
            },
            profile,
            &ranges,
        )
        .expect("stop request");
        assert!(mutating);
        assert!(matches!(kind, PtzResponseKind::Commanded));
        assert!(stop.contains("<tptz:PanTilt>true</tptz:PanTilt>"));
        assert!(stop.contains("<tptz:Zoom>false</tptz:Zoom>"));

        let (home, mutating, kind) =
            build_ptz_request(&PtzRequest::Home, profile, &ranges).expect("home request");
        assert!(mutating);
        assert!(matches!(kind, PtzResponseKind::Commanded));
        assert!(home.contains("<tptz:GotoHomePosition"));

        let (status, mutating, kind) =
            build_ptz_request(&PtzRequest::Status, profile, &ranges).expect("status request");
        assert!(!mutating);
        assert!(matches!(kind, PtzResponseKind::Status));
        assert!(status.contains("<tptz:GetStatus"));

        let (presets, mutating, kind) =
            build_ptz_request(&PtzRequest::ListPresets, profile, &ranges)
                .expect("list presets request");
        assert!(!mutating);
        assert!(matches!(kind, PtzResponseKind::Presets));
        assert!(presets.contains("<tptz:GetPresets"));

        let (goto, mutating, kind) = build_ptz_request(
            &PtzRequest::GotoPreset("preset<&>".to_owned()),
            profile,
            &ranges,
        )
        .expect("goto preset request");
        assert!(mutating);
        assert!(matches!(kind, PtzResponseKind::Commanded));
        assert!(goto.contains("<tptz:PresetToken>preset&lt;&amp;&gt;</tptz:PresetToken>"));

        let (set, mutating, kind) = build_ptz_request(
            &PtzRequest::SetPreset("loading<&>".to_owned()),
            profile,
            &ranges,
        )
        .expect("set preset request");
        assert!(mutating);
        assert!(matches!(kind, PtzResponseKind::PresetToken));
        assert!(set.contains("<tptz:PresetName>loading&lt;&amp;&gt;</tptz:PresetName>"));

        let (remove, mutating, kind) = build_ptz_request(
            &PtzRequest::RemovePreset("preset<&>".to_owned()),
            profile,
            &ranges,
        )
        .expect("remove preset request");
        assert!(mutating);
        assert!(matches!(kind, PtzResponseKind::Removed));
        assert!(remove.contains("<tptz:PresetToken>preset&lt;&amp;&gt;</tptz:PresetToken>"));

        assert!(
            build_ptz_request(
                &PtzRequest::Continuous {
                    velocity: PtzVector {
                        pan: 0.0,
                        tilt: 0.0,
                        zoom: 0.0,
                    },
                    timeout: Duration::ZERO,
                },
                profile,
                &ranges,
            )
            .is_err()
        );
        assert!(
            build_ptz_request(
                &PtzRequest::GotoPreset("invalid\u{0}token".to_owned()),
                profile,
                &ranges,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn discovery_reconciles_equivalent_duplicate_endpoint_references() {
        let first = DiscoveryProbeMatch {
            endpoint_reference: "urn:uuid:camera".to_owned(),
            xaddrs: vec![
                "http://camera.test/onvif/media".to_owned(),
                "http://camera.test/onvif/device_service".to_owned(),
            ],
            vendor: Some("EdgeCommons".to_owned()),
            model: None,
        };
        let equivalent = DiscoveryProbeMatch {
            endpoint_reference: "urn:uuid:camera".to_owned(),
            xaddrs: vec![
                "http://camera.test/onvif/device_service".to_owned(),
                "http://camera.test/onvif/media".to_owned(),
            ],
            vendor: None,
            model: Some("Simulator".to_owned()),
        };
        let factory = OnvifBackendFactory::new(OnvifBackendDependencies {
            discovery: Arc::new(FixedDiscovery(vec![first, equivalent])),
            ..OnvifBackendDependencies::default()
        });

        let candidates = factory
            .discover(DiscoveryRequest {
                eligible_interfaces: vec!["Ethernet".to_owned()],
                timeout: Duration::from_secs(1),
                max_results: 8,
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("equivalent duplicates should reconcile");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].vendor.as_deref(), Some("EdgeCommons"));
        assert_eq!(candidates[0].model.as_deref(), Some("Simulator"));
        assert_eq!(candidates[0].capabilities, json!({ "xaddrCount": 2 }));
    }

    #[tokio::test]
    async fn discovery_rejects_conflicting_duplicate_endpoint_references() {
        let factory = OnvifBackendFactory::new(OnvifBackendDependencies {
            discovery: Arc::new(FixedDiscovery(vec![
                DiscoveryProbeMatch {
                    endpoint_reference: "urn:uuid:camera".to_owned(),
                    xaddrs: vec!["http://camera-a.test/onvif/device_service".to_owned()],
                    vendor: None,
                    model: None,
                },
                DiscoveryProbeMatch {
                    endpoint_reference: "urn:uuid:camera".to_owned(),
                    xaddrs: vec!["http://camera-b.test/onvif/device_service".to_owned()],
                    vendor: None,
                    model: None,
                },
            ])),
            ..OnvifBackendDependencies::default()
        });

        let error = factory
            .discover(DiscoveryRequest {
                eligible_interfaces: vec!["Ethernet".to_owned()],
                timeout: Duration::from_secs(1),
                max_results: 8,
                cancellation: CancellationToken::new(),
            })
            .await
            .expect_err("conflicting duplicate EPRs must fail closed");

        assert_eq!(error.code(), ErrorCode::BackendError);
        assert!(
            error
                .to_string()
                .contains("conflicting WS-Discovery devices")
        );
    }

    #[tokio::test]
    async fn selector_resolution_reconciles_equivalent_duplicate_endpoint_references() {
        let resolver: Arc<dyn OnvifResolver> =
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])]));
        let first = DiscoveryProbeMatch {
            endpoint_reference: "urn:uuid:camera".to_owned(),
            xaddrs: vec![
                "http://camera.test/onvif/media".to_owned(),
                "http://camera.test/onvif/device_service".to_owned(),
            ],
            vendor: Some("EdgeCommons".to_owned()),
            model: None,
        };
        let equivalent = DiscoveryProbeMatch {
            endpoint_reference: "urn:uuid:camera".to_owned(),
            xaddrs: vec![
                "http://camera.test/onvif/device_service".to_owned(),
                "http://camera.test/onvif/media".to_owned(),
            ],
            vendor: None,
            model: Some("Simulator".to_owned()),
        };
        let mut config = test_config(None);
        config.selector = Some(OnvifSelector {
            endpoint_reference: "urn:uuid:camera".to_owned(),
        });
        config.allowed_uri_cidrs = vec!["10.0.0.0/24".parse().expect("CIDR")];
        let factory = OnvifBackendFactory::new(OnvifBackendDependencies {
            resolver,
            discovery: Arc::new(FixedDiscovery(vec![first, equivalent])),
            transport: Arc::new(MockTransport::default()),
            credentials: Some(Arc::new(FixedCredentials)),
            clock: Arc::new(FixedClock),
            nonce_source: Arc::new(FixedNonce),
            security: SecurityConfig::default(),
        });

        factory
            .resolve_endpoint(
                config.selector.as_ref(),
                None,
                &config,
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await
            .expect("equivalent selector duplicates should resolve once");
    }

    #[tokio::test]
    async fn selector_resolution_rejects_conflicting_duplicate_endpoint_references_before_dns() {
        let resolver: Arc<dyn OnvifResolver> = Arc::new(SequenceResolver::new(&[]));
        let mut config = test_config(None);
        config.selector = Some(OnvifSelector {
            endpoint_reference: "urn:uuid:camera".to_owned(),
        });
        config.allowed_uri_cidrs = vec!["10.0.0.0/24".parse().expect("CIDR")];
        let factory = OnvifBackendFactory::new(OnvifBackendDependencies {
            resolver,
            discovery: Arc::new(FixedDiscovery(vec![
                DiscoveryProbeMatch {
                    endpoint_reference: "urn:uuid:camera".to_owned(),
                    xaddrs: vec!["http://camera-a.test/onvif/device_service".to_owned()],
                    vendor: None,
                    model: None,
                },
                DiscoveryProbeMatch {
                    endpoint_reference: "urn:uuid:camera".to_owned(),
                    xaddrs: vec!["http://camera-b.test/onvif/device_service".to_owned()],
                    vendor: None,
                    model: None,
                },
            ])),
            transport: Arc::new(MockTransport::default()),
            credentials: Some(Arc::new(FixedCredentials)),
            clock: Arc::new(FixedClock),
            nonce_source: Arc::new(FixedNonce),
            security: SecurityConfig::default(),
        });

        let error = factory
            .resolve_endpoint(
                config.selector.as_ref(),
                None,
                &config,
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await
            .expect_err("conflicting selector duplicates must fail before DNS or connection");

        assert_eq!(error.code(), ErrorCode::BackendError);
        assert!(
            error
                .to_string()
                .contains("conflicting WS-Discovery devices")
        );
    }

    #[tokio::test]
    async fn selector_resolution_requires_explicit_cidr_and_rejects_metadata_xaddr() {
        let resolver: Arc<dyn OnvifResolver> = Arc::new(SequenceResolver::new(&[
            ("camera.test", 80, &["10.0.0.2"]),
            ("169.254.169.254", 80, &["169.254.169.254"]),
        ]));
        let good = DiscoveryProbeMatch {
            endpoint_reference: "urn:uuid:camera".to_owned(),
            xaddrs: vec!["http://camera.test/onvif/device_service".to_owned()],
            vendor: None,
            model: None,
        };
        let mut config = test_config(None);
        config.selector = Some(OnvifSelector {
            endpoint_reference: "urn:uuid:camera".to_owned(),
        });
        config.allowed_uri_cidrs = vec!["10.0.0.0/24".parse().expect("CIDR")];
        let factory = OnvifBackendFactory::new(OnvifBackendDependencies {
            resolver: Arc::clone(&resolver),
            discovery: Arc::new(FixedDiscovery(vec![good.clone()])),
            transport: Arc::new(MockTransport::default()),
            credentials: Some(Arc::new(FixedCredentials)),
            clock: Arc::new(FixedClock),
            nonce_source: Arc::new(FixedNonce),
            security: SecurityConfig::default(),
        });
        let cancellation = CancellationToken::new();
        assert!(
            factory
                .resolve_endpoint(
                    config.selector.as_ref(),
                    None,
                    &config,
                    Instant::now() + Duration::from_secs(2),
                    &cancellation,
                )
                .await
                .is_ok()
        );

        let hostile = DiscoveryProbeMatch {
            xaddrs: vec!["http://169.254.169.254/latest/meta-data".to_owned()],
            ..good
        };
        let hostile_factory = OnvifBackendFactory::new(OnvifBackendDependencies {
            resolver,
            discovery: Arc::new(FixedDiscovery(vec![hostile])),
            transport: Arc::new(MockTransport::default()),
            credentials: Some(Arc::new(FixedCredentials)),
            clock: Arc::new(FixedClock),
            nonce_source: Arc::new(FixedNonce),
            security: SecurityConfig::default(),
        });
        assert!(
            hostile_factory
                .resolve_endpoint(
                    config.selector.as_ref(),
                    None,
                    &config,
                    Instant::now() + Duration::from_secs(2),
                    &cancellation,
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn factory_connects_media2_snapshot_and_ptz_then_captures_with_bounded_cancellation() {
        let resolver: Arc<dyn OnvifResolver> =
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])]));
        let transport = Arc::new(MockTransport::default());
        transport.push(soap_ok(&format!(
            "<tds:GetServicesResponse xmlns:tds=\"{DEVICE_NAMESPACE}\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tds:Service><tds:Namespace>{MEDIA2_NAMESPACE}</tds:Namespace><tds:XAddr>http://camera.test/onvif/media2_service</tds:XAddr></tds:Service><tds:Service><tds:Namespace>{PTZ_NAMESPACE}</tds:Namespace><tds:XAddr>http://camera.test/onvif/ptz_service</tds:XAddr></tds:Service></tds:GetServicesResponse>"
        )));
        transport.push(soap_ok(&format!(
            "<tds:GetDeviceInformationResponse xmlns:tds=\"{DEVICE_NAMESPACE}\"><tds:Manufacturer>EdgeCommons</tds:Manufacturer><tds:Model>Simulator</tds:Model><tds:FirmwareVersion>1</tds:FirmwareVersion><tds:SerialNumber>SIM-1</tds:SerialNumber></tds:GetDeviceInformationResponse>"
        )));
        transport.push(soap_ok(&format!(
            "<tr2:GetProfilesResponse xmlns:tr2=\"{MEDIA2_NAMESPACE}\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tr2:Profiles token=\"main\"><tt:Name>main</tt:Name></tr2:Profiles></tr2:GetProfilesResponse>"
        )));
        transport.push(soap_ok(&format!(
            "<tr2:GetSnapshotUriResponse xmlns:tr2=\"{MEDIA2_NAMESPACE}\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tr2:MediaUri><tt:Uri>http://camera.test/snapshot/main.png</tt:Uri></tr2:MediaUri></tr2:GetSnapshotUriResponse>"
        )));
        transport.push(soap_ok(&format!(
            "<tptz:GetConfigurationsResponse xmlns:tptz=\"{PTZ_NAMESPACE}\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tptz:PTZConfiguration token=\"ptz-main\"><tt:Name>Main</tt:Name></tptz:PTZConfiguration></tptz:GetConfigurationsResponse>"
        )));
        transport.push(soap_ok(&format!(
            "<tptz:GetConfigurationOptionsResponse xmlns:tptz=\"{PTZ_NAMESPACE}\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tptz:PTZConfigurationOptions><tt:Spaces><tt:AbsolutePanTiltPositionSpace><tt:XRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:XRange><tt:YRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:YRange></tt:AbsolutePanTiltPositionSpace><tt:AbsoluteZoomPositionSpace><tt:XRange><tt:Min>0</tt:Min><tt:Max>1</tt:Max></tt:XRange></tt:AbsoluteZoomPositionSpace><tt:RelativePanTiltTranslationSpace><tt:XRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:XRange><tt:YRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:YRange></tt:RelativePanTiltTranslationSpace><tt:RelativeZoomTranslationSpace><tt:XRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:XRange></tt:RelativeZoomTranslationSpace><tt:ContinuousPanTiltVelocitySpace><tt:XRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:XRange><tt:YRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:YRange></tt:ContinuousPanTiltVelocitySpace><tt:ContinuousZoomVelocitySpace><tt:XRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:XRange></tt:ContinuousZoomVelocitySpace></tt:Spaces></tptz:PTZConfigurationOptions></tptz:GetConfigurationOptionsResponse>"
        )));
        transport.push(soap_ok(&format!(
            "<tptz:GetNodesResponse xmlns:tptz=\"{PTZ_NAMESPACE}\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tptz:PTZNode token=\"node\"><tt:MaximumNumberOfPresets>8</tt:MaximumNumberOfPresets><tt:HomeSupported>true</tt:HomeSupported></tptz:PTZNode></tptz:GetNodesResponse>"
        )));
        transport.push(OnvifHttpResponse {
            status: 200,
            headers: BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
            body: png(16, 8),
        });
        let factory = OnvifBackendFactory::new(OnvifBackendDependencies {
            resolver,
            discovery: Arc::new(DisabledWsDiscovery),
            transport: transport.clone(),
            credentials: Some(Arc::new(FixedCredentials)),
            clock: Arc::new(FixedClock),
            nonce_source: Arc::new(FixedNonce),
            security: SecurityConfig::default(),
        });
        let config = test_config(Some("http://camera.test/onvif/device_service"));
        let mut session = factory
            .connect(ConnectRequest {
                instance_id: "camera-a".to_owned(),
                backend: BackendConfig::OnvifRtsp(config),
                timeout: Duration::from_secs(3),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("connect ONVIF session");
        assert!(session.capabilities().snapshot_uri);
        assert!(session.capabilities().ptz);
        assert!(session.capabilities().presets);
        assert_eq!(session.capabilities().model.as_deref(), Some("Simulator"));
        let profile: CaptureProfile = serde_json::from_value(json!({
            "output": { "encoding": "png" }
        }))
        .expect("capture profile");
        let frame = session
            .capture(CaptureRequest {
                capture_id: "capture-1".to_owned(),
                profile,
                maximum_frame_bytes: 1_048_576,
                timeout: Duration::from_secs(2),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("capture snapshot");
        assert_eq!(frame.pixel_format, PixelFormat::Rgb8);
        assert_eq!((frame.width, frame.height), (16, 8));

        transport.push(soap_ok(&format!(
            "<tptz:GetStatusResponse xmlns:tptz=\"{PTZ_NAMESPACE}\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tptz:PTZStatus><tt:Position><tt:PanTilt x=\"0.25\" y=\"-0.5\"/><tt:Zoom x=\"0.75\"/></tt:Position><tt:MoveStatus><tt:PanTilt>MOVING</tt:PanTilt><tt:Zoom>IDLE</tt:Zoom></tt:MoveStatus></tptz:PTZStatus></tptz:GetStatusResponse>"
        )));
        let status = session.status().await.expect("read ONVIF PTZ status");
        assert!(status.ptz.is_some_and(|ptz| ptz.moving == Some(true)));

        transport.push(soap_ok(&format!(
            "<tptz:GetPresetsResponse xmlns:tptz=\"{PTZ_NAMESPACE}\" xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tptz:Preset token=\"preset-1\"><tt:Name>Loading bay</tt:Name></tptz:Preset></tptz:GetPresetsResponse>"
        )));
        assert!(matches!(
            session
                .ptz(PtzRequest::ListPresets)
                .await
                .expect("list ONVIF presets"),
            PtzResult::Presets(ref presets) if presets.len() == 1 && presets[0].token == "preset-1"
        ));

        transport.push(soap_ok(&format!(
            "<tptz:SetPresetResponse xmlns:tptz=\"{PTZ_NAMESPACE}\"><tptz:PresetToken>preset-2</tptz:PresetToken></tptz:SetPresetResponse>"
        )));
        assert!(matches!(
            session
                .ptz(PtzRequest::SetPreset("Dock".to_owned()))
                .await
                .expect("set ONVIF preset"),
            PtzResult::PresetToken(ref token) if token == "preset-2"
        ));

        transport.push(soap_ok(&format!(
            "<tptz:RemovePresetResponse xmlns:tptz=\"{PTZ_NAMESPACE}\"/>"
        )));
        assert!(matches!(
            session
                .ptz(PtzRequest::RemovePreset("preset-2".to_owned()))
                .await
                .expect("remove ONVIF preset"),
            PtzResult::Removed
        ));
        for request in [
            PtzRequest::Continuous {
                velocity: PtzVector {
                    pan: 0.25,
                    tilt: -0.25,
                    zoom: 0.5,
                },
                timeout: Duration::from_millis(250),
            },
            PtzRequest::Absolute {
                position: PtzVector {
                    pan: 0.2,
                    tilt: -0.2,
                    zoom: 0.6,
                },
                speed: Some(PtzVector {
                    pan: 0.4,
                    tilt: 0.4,
                    zoom: 0.4,
                }),
            },
            PtzRequest::Relative {
                translation: PtzVector {
                    pan: 0.1,
                    tilt: 0.1,
                    zoom: -0.1,
                },
                speed: None,
            },
            PtzRequest::Stop {
                pan: true,
                tilt: false,
                zoom: true,
            },
            PtzRequest::Home,
            PtzRequest::GotoPreset("preset-1".to_owned()),
        ] {
            transport.push(soap_ok(&format!(
                "<tptz:Response xmlns:tptz=\"{PTZ_NAMESPACE}\"/>"
            )));
            assert!(matches!(
                session.ptz(request).await.expect("issue ONVIF PTZ command"),
                PtzResult::Commanded
            ));
        }
        let transport_started = Arc::new(tokio::sync::Notify::new());
        transport.block_next_request(Arc::clone(&transport_started));
        let cancellation = CancellationToken::new();
        let ptz_cancellation = cancellation.clone();
        let ptz = tokio::spawn(async move {
            session
                .ptz_bounded(
                    PtzRequest::Status,
                    Instant::now() + Duration::from_secs(1),
                    &ptz_cancellation,
                )
                .await
        });
        transport_started.notified().await;
        cancellation.cancel();
        assert_eq!(
            ptz.await
                .expect("bounded PTZ task must not panic")
                .expect_err("cancelled ONVIF PTZ must not wait for a transport response")
                .code(),
            ErrorCode::CaptureCancelled
        );
        assert_eq!(transport.observations().len(), 19);
    }

    #[test]
    fn profile_snapshot_fault_and_ptz_helpers_fail_closed_at_protocol_boundaries() {
        let profiles = vec![
            MediaProfileRecord {
                token: "token-a".to_owned(),
                name: Some("production".to_owned()),
                ptz_configuration_token: None,
            },
            MediaProfileRecord {
                token: "token-b".to_owned(),
                name: Some("production".to_owned()),
                ptz_configuration_token: None,
            },
        ];
        assert_eq!(
            select_profile(&profiles, "token-a")
                .expect("unique token")
                .expect("token match")
                .token,
            "token-a"
        );
        assert!(select_profile(&profiles, "production").is_err());
        assert!(select_profile(&profiles, "missing").unwrap().is_none());

        let action_not_supported = parse_bounded_xml(
            &soap_envelope(
                "<s:Fault xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\"><s:Code><s:Value>ter:ActionNotSupported</s:Value></s:Code></s:Fault>",
                None,
            ),
            100_000,
            64,
        )
        .expect("bounded SOAP fault");
        assert!(matches!(
            snapshot_unavailability_from_fault(&action_not_supported),
            Some(SnapshotFallbackReason::ActionNotSupported)
        ));
        let optional_action = parse_bounded_xml(
            &soap_envelope(
                "<s:Fault xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\"><s:Code><s:Value>ter:OptionalActionNotImplemented</s:Value></s:Code></s:Fault>",
                None,
            ),
            100_000,
            64,
        )
        .expect("bounded SOAP fault");
        assert!(matches!(
            snapshot_unavailability_from_fault(&optional_action),
            Some(SnapshotFallbackReason::CapabilityAbsent)
        ));

        assert_eq!(
            map_ptz_error(timeout_error("PTZ")).code(),
            ErrorCode::PtzTimeout
        );
        assert_eq!(
            map_ptz_error(CameraError::rejected(ErrorCode::InvalidRequest, "invalid")).code(),
            ErrorCode::InvalidRequest
        );
        assert_eq!(
            onvif_capture_modes(true, false),
            vec![CaptureMode::SnapshotUri]
        );
        assert_eq!(
            onvif_capture_modes(false, true),
            vec![CaptureMode::RtspFrame]
        );
        assert!(onvif_capture_modes(false, false).is_empty());
    }
}
