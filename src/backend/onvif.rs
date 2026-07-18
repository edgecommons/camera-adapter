//! ONVIF device/media/snapshot/PTZ backend with fail-closed network and XML boundaries.
//!
//! Protocol behavior is split behind resolver, WS-Discovery, HTTP transport, credential, clock, and
//! nonce seams. Production HTTP uses DNS pinning with reqwest redirects disabled; tests exercise the
//! same orchestration with deterministic in-memory implementations. No secret-bearing type exposes a
//! useful `Debug` representation.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::StreamExt;
use image::{GenericImageView, ImageFormat, ImageReader};
use ipnet::IpNet;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde_json::{Value, json};
use sha1::Sha1;
use sha2::{Digest as _, Sha256};
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use url::Url;

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
use super::net::{
    AddressResolver, DigestChallenge, NetClock, NetworkCredentials, NonceSource, SecretBytes,
    basic_authorization, cancelled_error, digest_authorization_for_method, find_auth_scheme,
    is_forbidden_network_address, normalize_host_text, parse_digest_challenge, security_error,
    timeout_error,
};
#[cfg(any(feature = "rtsp", test))]
use super::net::RtspNetworkAnchor;
#[cfg(test)]
use super::net::{
    DigestAlgorithm, MAX_BLOCKING_DNS_LOOKUPS, SystemNetClock, SystemNonceSource, SystemResolver,
    blocking_dns_limiter, split_auth_parameters,
};

const ONVIF_BACKEND: &str = "onvif-rtsp";
const MAX_DISCOVERY_MATCHES: usize = 10_000;
const MAX_REDIRECTS: usize = 3;
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

/// Lazy standard-secret resolution seam.
#[async_trait]
pub trait OnvifCredentialProvider: Send + Sync {
    /// Resolves a login object containing `username` and `password`.
    async fn resolve_login(&self, reference: &SecretRef) -> Result<Arc<NetworkCredentials>>;

    /// Resolves opaque bytes, used for a private CA bundle.
    async fn resolve_bytes(&self, reference: &SecretRef) -> Result<Arc<SecretBytes>>;
}

async fn resolve_login_bounded(
    provider: &dyn OnvifCredentialProvider,
    reference: &SecretRef,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Arc<NetworkCredentials>> {
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
        resolver: &dyn AddressResolver,
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
        resolver: &dyn AddressResolver,
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
    resolver: &dyn AddressResolver,
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

/// What makes one HTTP client different from another.
///
/// The client cannot simply be a singleton: the address is PINNED into it (`resolve`), and the TLS
/// posture -- hostname verification, certificate acceptance, a private CA -- belongs to the camera,
/// not to the component. Two cameras with different trust settings must not share a client. Two
/// requests to the SAME camera must.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HttpClientKey {
    host: String,
    socket: SocketAddr,
    verify_hostname: bool,
    allow_invalid_certificates: bool,
    /// The private CA bundle, digested. The bytes themselves are a secret handle and are not kept.
    ca_digest: Option<[u8; 32]>,
}

/// The number of distinct client configurations kept alive.
///
/// One per camera at the design's 256, plus room for a camera whose address moves. Past it the cache
/// is emptied rather than grown: a bounded miss costs a handshake, and an unbounded map costs the
/// process.
const MAX_POOLED_CLIENTS: usize = 1_024;

/// Production reqwest transport with per-request address pinning and redirects disabled.
///
/// The client used to be built INSIDE `send()`, per request. A fresh connection pool, a fresh rustls
/// configuration, a fresh root-certificate store -- built, used once, and dropped. Keep-alive was not
/// merely unused, it was structurally impossible: every SOAP call and every snapshot GET paid a full
/// TCP and TLS handshake and left a socket in TIME_WAIT. Connecting to one camera is about nine
/// requests. At 256 cameras snapshotting every ten seconds that is ~26 connections a second sustained,
/// north of 1,500 sockets held continuously, and a TLS handshake per snapshot on hardware that has to
/// decode video with what is left.
///
/// Clients are now kept, keyed by the configuration that makes them different. Same camera, same
/// posture, same client -- and reqwest's pool does what it was built to do.
#[derive(Debug, Default)]
pub struct ReqwestOnvifTransport {
    clients: std::sync::Mutex<std::collections::HashMap<HttpClientKey, reqwest::Client>>,
}

impl ReqwestOnvifTransport {
    /// The client for this request's camera and trust posture, built once and then reused.
    fn client(&self, request: &OnvifHttpRequest, socket: SocketAddr) -> Result<reqwest::Client> {
        let key = HttpClientKey {
            host: request.target.host().to_owned(),
            socket,
            verify_hostname: request.tls.verify_hostname,
            allow_invalid_certificates: request.tls.allow_invalid_certificates,
            ca_digest: request.tls.ca_pem.as_ref().map(|pem| {
                let mut digest = Sha256::new();
                digest.update(pem.expose());
                digest.finalize().into()
            }),
        };

        if let Ok(clients) = self.clients.lock() {
            if let Some(client) = clients.get(&key) {
                return Ok(client.clone());
            }
        }

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

        if let Ok(mut clients) = self.clients.lock() {
            // A camera that keeps moving must not be able to grow this without bound. Emptying it
            // costs one handshake per camera; keeping it costs the process.
            if clients.len() >= MAX_POOLED_CLIENTS {
                clients.clear();
            }
            clients.insert(key, client.clone());
        }
        Ok(client)
    }
}

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
        let client = self.client(&request, socket)?;
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

fn digest_authorization(
    challenge: &DigestChallenge,
    credentials: &NetworkCredentials,
    method: OnvifHttpMethod,
    target: &str,
    nonce_count: u32,
    nonce_source: &dyn NonceSource,
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

fn wsse_header(
    credentials: &NetworkCredentials,
    clock: &dyn NetClock,
    nonce_source: &dyn NonceSource,
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
    resolver: Arc<dyn AddressResolver>,
    transport: Arc<dyn OnvifHttpTransport>,
    clock: Arc<dyn NetClock>,
    nonce_source: Arc<dyn NonceSource>,
    credentials: Option<Arc<NetworkCredentials>>,
    policy: UriPolicy,
    tls: RequestTlsPolicy,
    security: SecurityConfig,
    allow_insecure: bool,
    authentication_mode: AuthenticationMode,
    authentications: BTreeMap<(String, String, u16), SessionAuthentication>,
    max_soap_bytes: u64,
    max_xml_depth: usize,
    /// Shared RTSP decode-stage bound, handed to each camera's capture controller.
    #[cfg(feature = "rtsp")]
    decode_gate: Arc<Semaphore>,
}

impl OnvifProtocolClient {
    fn credentials(&self) -> Result<&NetworkCredentials> {
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
    pub resolver: Arc<dyn AddressResolver>,
    /// Eligible-interface WS-Discovery transport.
    pub discovery: Arc<dyn WsDiscovery>,
    /// Redirect-disabled, bounded HTTP transport.
    pub transport: Arc<dyn OnvifHttpTransport>,
    /// Lazy secret-reference resolver.  It is absent only when the complete configuration has
    /// no ONVIF secret references; attempting to resolve a reference without it is a closed
    /// configuration error rather than an implicit fallback provider.
    pub credentials: Option<Arc<dyn OnvifCredentialProvider>>,
    /// UTC clock.
    pub clock: Arc<dyn NetClock>,
    /// Cryptographic nonce source.
    pub nonce_source: Arc<dyn NonceSource>,
    /// Shared HTTP/XML security limits.
    pub security: SecurityConfig,
    /// Component-wide bound on concurrent blocking GStreamer operations.
    ///
    /// One semaphore is shared by every camera, so it must be injected rather than built per
    /// factory: a factory is constructed per camera and per reconnect, and a per-factory semaphore
    /// would be no bound at all. Sized from `limits.maxConcurrentCaptures` so the RTSP decode stage
    /// is exactly as wide as the concurrency the component advertises.
    pub decode_gate: Arc<Semaphore>,
}

#[cfg(test)]
impl Default for OnvifBackendDependencies {
    fn default() -> Self {
        Self {
            resolver: Arc::new(SystemResolver),
            discovery: Arc::new(DisabledWsDiscovery),
            transport: Arc::new(ReqwestOnvifTransport::default()),
            credentials: None,
            clock: Arc::new(SystemNetClock),
            nonce_source: Arc::new(SystemNonceSource),
            security: SecurityConfig::default(),
            decode_gate: Arc::new(Semaphore::new(
                crate::config::LimitsConfig::default().max_concurrent_captures,
            )),
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

    /// Resolves a secret reference through the SAME bounded path a session uses.
    ///
    /// It used to call `provider.resolve_login()` straight, which production never does: production
    /// goes through [`resolve_login_bounded`], which adds the deadline and cancellation guards. So
    /// the accessor re-implemented the production path minus exactly the part that can fail, and the
    /// test that leaned on it proved the credential wiring worked while proving nothing about the
    /// code that actually runs. It now delegates, and takes the bounds as arguments so a caller can
    /// exercise them.
    #[cfg(test)]
    pub(crate) async fn resolve_login_bounded_for_test(
        &self,
        reference: &SecretRef,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Arc<NetworkCredentials>> {
        let provider =
            self.dependencies
                .credentials
                .as_deref()
                .ok_or_else(|| CameraError::Config {
                    path: "component.credentials".to_owned(),
                    message: "ONVIF secret references require EdgeCommons credentials".to_owned(),
                })?;
        resolve_login_bounded(provider, reference, deadline, cancellation).await
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
            #[cfg(feature = "rtsp")]
            decode_gate: Arc::clone(&self.dependencies.decode_gate),
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
                        decode_gate: Arc::clone(&client.decode_gate),
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

/// Whether an encoded image actually carries a whole picture.
///
/// Decoding is NOT this check, and that is the trap. `image`'s JPEG decoder is lenient about a
/// truncated entropy-coded scan: it returns the rows it managed to reconstruct rather than failing. And
/// `snapshot_to_frame` only ever used the decode as a dimension/format sanity check before passing the
/// ORIGINAL body through as the frame -- so a snapshot whose scan simply stopped arriving decoded to the
/// declared dimensions, passed every guard, and was DELIVERED. A half-picture, structurally valid,
/// reported as a success, and self-consistent with its own digest at every link downstream.
///
/// The only thing that caught a mid-scan cut was HTTP framing. A camera or middlebox that truncates
/// without breaking `Content-Length` was invisible.
///
/// A container that never wrote its terminal marker never finished writing the picture:
///
/// * **JPEG** ends at `FF D9` (End Of Image). This is exact rather than heuristic -- inside
///   entropy-coded data every `0xFF` is byte-stuffed as `FF 00`, so a genuine `FF D9` CANNOT occur
///   within the scan. Trailing padding after it (which some cameras append) is fine: the marker is
///   searched for, not required to be the final byte.
/// * **PNG** ends with the `IEND` chunk.
fn image_carries_a_whole_picture(body: &[u8], format: ImageFormat) -> bool {
    match format {
        ImageFormat::Jpeg => body.windows(2).any(|pair| pair == [0xFF, 0xD9]),
        ImageFormat::Png => body.windows(4).any(|chunk| chunk == b"IEND"),
        // Any other format is rejected on its own terms further down; do not veto it here.
        _ => true,
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
    // The picture must be WHOLE, not merely decodable. A scan that stopped arriving still decodes to the
    // declared dimensions, and used to be delivered as a success.
    if !image_carries_a_whole_picture(&response.body, declared_format) {
        return Err(SnapshotAttemptFailure::fallback(
            SnapshotFallbackReason::CorruptImage,
            backend_error("snapshot ended before the image did; the picture is incomplete"),
        ));
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

#[cfg(test)]
mod tests {
    /// A generously-bounded PTZ call, for tests that are not about the bound.
    ///
    /// `CameraSession` deliberately offers only `ptz_bounded`: an unbounded variant is an invitation to
    /// fabricate the deadline and the cancellation token, which is precisely what the old required
    /// `ptz` drove `OnvifSession` to do. Tests that are exercising PTZ BEHAVIOUR still want to say
    /// `session.ptz(request)` without inventing a deadline in every line, so they say it here, once,
    /// where the deadline is obviously a test's and not a protocol's.
    #[async_trait]
    trait GenerouslyBoundedPtz {
        async fn ptz(&mut self, request: PtzRequest) -> Result<PtzResult>;
    }

    #[async_trait]
    impl<T: CameraSession + ?Sized> GenerouslyBoundedPtz for T {
        async fn ptz(&mut self, request: PtzRequest) -> Result<PtzResult> {
            self.ptz_bounded(
                request,
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                &tokio_util::sync::CancellationToken::new(),
            )
            .await
        }
    }

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
    impl AddressResolver for SequenceResolver {
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

    impl NetClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            Utc.with_ymd_and_hms(2026, 7, 10, 14, 0, 0)
                .single()
                .expect("fixed UTC instant")
        }
    }

    #[derive(Debug)]
    struct FixedNonce;

    impl NonceSource for FixedNonce {
        fn nonce(&self, length: usize) -> Result<Vec<u8>> {
            Ok((0..length).map(|value| value as u8).collect())
        }
    }

    #[derive(Debug)]
    struct FixedCredentials;

    #[async_trait]
    impl OnvifCredentialProvider for FixedCredentials {
        async fn resolve_login(&self, _reference: &SecretRef) -> Result<Arc<NetworkCredentials>> {
            Ok(Arc::new(NetworkCredentials::new(
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
        resolver: Arc<dyn AddressResolver>,
        transport: Arc<dyn OnvifHttpTransport>,
        config: &OnvifBackendConfig,
        credentials: Option<Arc<NetworkCredentials>>,
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
                #[cfg(feature = "rtsp")]
                decode_gate: Arc::new(Semaphore::new(
                    crate::config::LimitsConfig::default().max_concurrent_captures,
                )),
            },
            pinned,
        )
    }

    #[test]
    fn secret_types_never_debug_plaintext() {
        let credentials = NetworkCredentials::new("operator", "camera-secret").expect("credentials");
        let rendered = format!("{credentials:?}");
        assert!(!rendered.contains("operator"));
        assert!(!rendered.contains("camera-secret"));
        assert!(rendered.contains("redacted"));
        assert!(NetworkCredentials::new("ambiguous:user", "password").is_err());

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

    #[test]
    fn protocol_boundary_helpers_normalize_hosts_reject_network_addresses_and_redact_requests() {
        assert_eq!(
            normalize_host_text("Camera.Example.").expect("normalized hostname"),
            "camera.example"
        );
        for invalid in ["", "bad/host", "bad@host", "bad\u{0007}host"] {
            assert!(
                normalize_host_text(invalid).is_err(),
                "{invalid:?} must be rejected"
            );
        }

        assert!(is_forbidden_network_address(
            "fe80::1".parse().expect("IPv6 link-local address")
        ));
        assert!(is_forbidden_network_address(
            "::1".parse().expect("IPv6 loopback address")
        ));
        assert!(is_forbidden_network_address(
            "ff02::1".parse().expect("IPv6 multicast address")
        ));
        assert!(!is_forbidden_network_address(
            "2001:db8::1".parse().expect("ordinary IPv6 address")
        ));

        assert_eq!(OnvifHttpMethod::Get.as_str(), "GET");
        assert_eq!(OnvifHttpMethod::Post.as_str(), "POST");
        let tls = RequestTlsPolicy {
            verify_hostname: true,
            allow_invalid_certificates: false,
            ca_pem: Some(Arc::new(SecretBytes::new("private-ca-pem"))),
        };
        let rendered_tls = format!("{tls:?}");
        assert!(rendered_tls.contains("<redacted>"));
        assert!(!rendered_tls.contains("private-ca-pem"));

        let request = loopback_http_request(443, OnvifHttpMethod::Get, 1_024);
        let rendered_request = format!("{request:?}");
        assert!(rendered_request.contains("<redacted>"));
        assert!(!rendered_request.contains("Bearer adapter-test"));
        assert!(rendered_request.contains("body_bytes"));

        let response = OnvifHttpResponse {
            status: 204,
            headers: BTreeMap::from([("x-camera".to_owned(), "ready".to_owned())]),
            body: Vec::new(),
        };
        assert_eq!(response.header("X-Camera"), Some("ready"));
        assert!(response.is_success());
        assert!(
            !OnvifHttpResponse {
                status: 302,
                headers: BTreeMap::new(),
                body: Vec::new(),
            }
            .is_success()
        );
    }

    #[test]
    fn xml_protocol_parsers_accept_bounded_content_and_reject_faults() {
        let cdata = parse_bounded_xml(b"<Envelope><![CDATA[camera-ready]]></Envelope>", 1_024, 4)
            .expect("CDATA is ordinary bounded XML content");
        assert_eq!(cdata.text, "camera-ready");
        assert!(parse_bounded_xml(b"<Envelope><?forbidden value?></Envelope>", 1_024, 4).is_err());
        assert!(parse_bounded_xml(b"<one/><two/>", 1_024, 4).is_err());

        let services = parse_services(
            format!(
                "<Envelope><Service><Namespace>{MEDIA1_NAMESPACE}</Namespace><XAddr>https://camera.test/media</XAddr></Service><Service><Namespace>{MEDIA1_NAMESPACE}</Namespace><XAddr>https://camera.test/media</XAddr></Service><Service><Namespace>{PTZ_NAMESPACE}</Namespace><XAddr>https://camera.test/ptz</XAddr></Service><Service><Namespace>urn:unsupported</Namespace><XAddr>https://camera.test/ignored</XAddr></Service></Envelope>"
            )
            .as_bytes(),
            4_096,
            8,
        )
        .expect("bounded service document");
        assert_eq!(services.media1, vec!["https://camera.test/media"]);
        assert!(services.media2.is_empty());
        assert_eq!(services.ptz, vec!["https://camera.test/ptz"]);

        let profiles = parse_profiles(
            b"<Envelope><Profiles><token>profile-from-child</token><Name> Secondary </Name><PTZConfiguration token=\"ptz-config\" /></Profiles></Envelope>",
            4_096,
            8,
        )
        .expect("profile token child fallback");
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].token, "profile-from-child");
        assert_eq!(profiles[0].name.as_deref(), Some("Secondary"));
        assert_eq!(
            profiles[0].ptz_configuration_token.as_deref(),
            Some("ptz-config")
        );

        let fault = parse_bounded_xml(
            b"<Envelope><Fault><Value>ter:ActionNotSupported</Value><Text>camera refused operation</Text></Fault></Envelope>",
            4_096,
            8,
        )
        .expect("bounded SOAP fault document");
        assert!(matches!(
            snapshot_unavailability_from_fault(&fault),
            Some(SnapshotFallbackReason::ActionNotSupported)
        ));
        let error = reject_soap_fault(&fault).expect_err("SOAP faults must become backend errors");
        assert_eq!(error.code(), ErrorCode::BackendError);
        assert!(error.to_string().contains("camera refused operation"));
    }

    #[test]
    fn authentication_parameter_parser_handles_escaping_and_fails_closed() {
        let parameters = split_auth_parameters(
            r#"realm="camera\"operator", nonce="nonce-value", qop="auth,auth-int""#,
        )
        .expect("quoted commas and escapes are part of one challenge");
        assert_eq!(parameters["realm"], "camera\"operator");
        assert_eq!(parameters["nonce"], "nonce-value");
        assert_eq!(parameters["qop"], "auth,auth-int");

        for malformed in [
            r#"realm="unterminated"#,
            "realm",
            r#"realm="ok"suffix"#,
            "realm=first, realm=second",
            "realm=bad\u{0007}",
        ] {
            assert!(
                split_auth_parameters(malformed).is_err(),
                "malformed challenge {malformed:?} must be rejected"
            );
        }

        assert_eq!(
            find_auth_scheme(
                "Basic realm=\"camera\", Digest realm=\"operator\"",
                "digest"
            ),
            Some("Basic realm=\"camera\", Digest".len())
        );
        assert!(find_auth_scheme("NotDigest realm=\"camera\"", "digest").is_none());
    }

    /// One camera, one HTTP client -- so keep-alive is possible at all.
    ///
    /// The client was built INSIDE `send()`, per request: a fresh connection pool, a fresh rustls
    /// configuration, a fresh root-certificate store, used once and dropped. Keep-alive was not merely
    /// unused, it was structurally impossible, and every SOAP call and snapshot GET paid a full TCP and
    /// TLS handshake and left a socket in TIME_WAIT. Connecting to one camera is about nine requests;
    /// at 256 cameras snapshotting every ten seconds that is ~26 connections a second, sustained.
    ///
    /// The client cannot be a singleton either: the address is pinned into it and the TLS posture
    /// belongs to the camera. Two cameras that trust different certificate authorities must never
    /// share one -- which is the assertion that actually matters here.
    #[tokio::test]
    async fn one_camera_keeps_one_http_client_and_a_different_trust_posture_gets_its_own() {
        let transport = ReqwestOnvifTransport::default();
        let socket = SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 1]), 8_443);
        let plain = loopback_http_request(8_443, OnvifHttpMethod::Post, 32);

        let first = transport.client(&plain, socket).expect("a client is built");
        let second = transport.client(&plain, socket).expect("and then reused");
        assert_eq!(
            transport.clients.lock().unwrap().len(),
            1,
            "the same camera, asked twice, must not build a second connection pool"
        );
        // `reqwest::Client` is a handle to one pool; cloning shares it. Same pool, same keep-alive.
        drop((first, second));

        // A camera that trusts a different CA is a different client, and must be.
        let mut private_ca = loopback_http_request(8_443, OnvifHttpMethod::Post, 32);
        private_ca.tls.allow_invalid_certificates = true;
        let _ = transport
            .client(&private_ca, socket)
            .expect("its own client");
        assert_eq!(
            transport.clients.lock().unwrap().len(),
            2,
            "a different trust posture must never share a client with a stricter one"
        );
    }

    /// A self-signed CA, valid until 2126. Nothing connects to it -- it only has to be a real
    /// certificate, because the code under test parses it.
    const TEST_CA_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----
MIIDJTCCAg2gAwIBAgIUXQLMJIHNs1dimp6pKLD7Sh6kAxUwDQYJKoZIhvcNAQEL
BQAwITEfMB0GA1UEAwwWY2FtZXJhLWFkYXB0ZXItdGVzdC1jYTAgFw0yNjA3MTQw
NDMyMDRaGA8yMTI2MDYyMDA0MzIwNFowITEfMB0GA1UEAwwWY2FtZXJhLWFkYXB0
ZXItdGVzdC1jYTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANvNVlGx
9XyS10ojUtyx0BSYWpx7nwvw0ToiTPvCDKdoeAX12IwY5Zr6Tuj8+Sj8rnzqloYA
6aJX1ydGy6HjzjygxTUo2o/6V1X7UCk1h0VdRWP1hXqY5pCQdYhfefBIwT4LkoZC
AgfnO/WoUXRVP+s7dtFEem8mFDZsfW6fNMBlQPeb6a96rrDUHa5aZfDrG2AKq7gF
MqjSSgEm6oICdopEvM312z3m0L38/UILEWO/HDc8tmqI2jwGmTmqDx6c8yn9TrBy
cJuZVgmXQS2470yV7jL+nkrAnedw9Et3AdTTYmtfc0xYfyWZp50VqDBDlwP5VeJR
9NKOd4WEo4Eg6G0CAwEAAaNTMFEwHQYDVR0OBBYEFOoUND3o4j6sQstQHFL9fyJc
R+9PMB8GA1UdIwQYMBaAFOoUND3o4j6sQstQHFL9fyJcR+9PMA8GA1UdEwEB/wQF
MAMBAf8wDQYJKoZIhvcNAQELBQADggEBAAs/y4F6013kYX8aeJ9xp63HtAfV4mgM
8N0LD2CDvF34y5R8nP4D75jMih2N5miI6swfj1dYq+0/wn6Wbnx5R4eOQoGVesb7
YX1Ehi8lxMiytt80tNAlcgGgPU8NkCZ+ttiY3Y8Y4eXfOy16caZ1Hvqo/2JGVhN0
IygJCv51DJWcf8KPLIeCdwix8iHgR5EAOZM4BiFPgP6DgXXKIuPA/nr24dkAeXt8
cvm/OTzEVSowjneTcURg3GfcT41yJ58NIaNmjh+KiziZ6yby70MdBa0+mNY/ZM/j
KZqUk5o8OZ+5KRGSwv2Fwj0XMp6CIDX/2TflMDHcrGp+USYwJhWXnc4=
-----END CERTIFICATE-----
";

    /// A second, unrelated self-signed CA. Different bytes, therefore a different camera trust root.
    const OTHER_CA_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----
MIIDJzCCAg+gAwIBAgIUH/5w8MMMpIgIvYIDPGmfRZVjnXIwDQYJKoZIhvcNAQEL
BQAwIjEgMB4GA1UEAwwXY2FtZXJhLWFkYXB0ZXItb3RoZXItY2EwIBcNMjYwNzE0
MDQ0MjM2WhgPMjEyNjA2MjAwNDQyMzZaMCIxIDAeBgNVBAMMF2NhbWVyYS1hZGFw
dGVyLW90aGVyLWNhMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyaox
0TcwrnTCV7E7Tgn6a0bNRxLX0IfAlBs540D+MRunBCyW33cvCEB/p+zk4kF+VX1R
LoNK6DqZpM9JbJMLUrcX5kEdQw5pNdEmCvmSwVz6tUtH59LEieKbE84mCat2Z5R3
h/TWl/tm3kfROT0A/cmObasFuLDb2niPt+5qovH+cNgHk646zF+kbEnRD4Wj/Sdp
dgsXoBHZxAB8AtMMSGGXzWNl5VxGE7ekTyx7v7a4MOhYYCYHigcDl39U9nJh4lfy
vJD86kijF4Z0aiEE0t49AEvroGXkR3HFC5LWBH3hkbAHI8kAf8ibFa3mpYpQmWgs
+0YcKGCniN7v+iO+QQIDAQABo1MwUTAdBgNVHQ4EFgQUA8vTzPi5uGEhm384HopP
8quaet8wHwYDVR0jBBgwFoAUA8vTzPi5uGEhm384HopP8quaet8wDwYDVR0TAQH/
BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAJv7zvtE1kQwEPz67+r26hoLF7zVN
j0DZr5eyaxayW0DbskIEWQhRzegHWDM/TV/DrlPcgz+nnDbROWsiWfz12e9ooxwv
ndP7ub6FXCozZjIMOw0GglraaraDTw17h88q7De9HD7eMh9SDD+2IuGlSLRE+GP2
VgOt+CtnpoCLyVVejBgNIteFmsFfwuSt2HV3TeWbR/05IzBIP/89JQDO9YDtKPBT
/EXZJ+ZnurKVLVCTZdNH8larh2ThxKvQLw8f74HdFTV2GLDzj+IYb0iY94lTJflj
wkWsh7u3nnr9fXRpWsamYEAKGzNo0istMB6rD6cMzNfRZCMk4rXuokYWOw==
-----END CERTIFICATE-----
";

    fn request_trusting(ca_pem: &[u8]) -> OnvifHttpRequest {
        let mut request = loopback_http_request(8_443, OnvifHttpMethod::Post, 32);
        request.tls.ca_pem = Some(Arc::new(SecretBytes::new(ca_pem)));
        request
    }

    /// Two cameras behind two private CAs must not end up sharing one client.
    ///
    /// The client is what holds the trust decision -- the root store is baked into it at build time.
    /// If the cache key ignored the CA, the SECOND camera to ask would silently be handed the FIRST
    /// camera's client, and would then be validated against a certificate authority that has nothing
    /// to do with it. That is a trust boundary quietly dissolving inside a performance optimisation,
    /// so the key digests the bundle: same bytes, same client; different bytes, never.
    ///
    /// The digest is also why the bundle itself is not kept. It is a secret handle, and a cache of
    /// them, alive for the life of the process, is exactly what `SecretBytes` exists to prevent.
    #[tokio::test]
    async fn two_cameras_that_trust_different_certificate_authorities_never_share_a_client() {
        let transport = ReqwestOnvifTransport::default();
        let socket = SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 1]), 8_443);

        let private = request_trusting(TEST_CA_PEM);
        transport
            .client(&private, socket)
            .expect("a private CA bundle must produce a client that trusts it");
        transport
            .client(&request_trusting(TEST_CA_PEM), socket)
            .expect("and the same bundle must reuse it");
        assert_eq!(
            transport.clients.lock().unwrap().len(),
            1,
            "the same camera and the same trust root must not build a second connection pool"
        );

        transport
            .client(&request_trusting(OTHER_CA_PEM), socket)
            .expect("a different CA bundle is a different camera trust root");
        assert_eq!(
            transport.clients.lock().unwrap().len(),
            2,
            "a camera behind a different certificate authority must never be validated against \
             another camera's root store"
        );

        // The public-roots posture is a third, distinct key: no private CA is not the same trust
        // decision as some private CA.
        transport
            .client(
                &loopback_http_request(8_443, OnvifHttpMethod::Post, 32),
                socket,
            )
            .expect("a camera with no private CA gets its own client");
        assert_eq!(transport.clients.lock().unwrap().len(), 3);

        // And the secret itself is never retained -- only its digest.
        let keys = transport.clients.lock().unwrap();
        assert_eq!(
            keys.keys().filter(|key| key.ca_digest.is_some()).count(),
            2,
            "a private CA must be remembered as a digest, and the two must not collide"
        );
    }

    /// A private CA that cannot be parsed fails the request; it does not fall back to the public roots.
    ///
    /// Silently ignoring an unusable `tls.ca` would take a camera an operator has deliberately pinned
    /// to their own certificate authority and validate it against Mozilla's root store instead. The
    /// component would keep working, and the trust boundary the operator configured would simply not
    /// exist -- which is the failure mode this refuses.
    #[tokio::test]
    async fn a_private_ca_bundle_that_cannot_be_used_fails_the_request_rather_than_trusting_anything_else()
     {
        let transport = ReqwestOnvifTransport::default();
        let socket = SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 1]), 8_443);

        let empty = transport
            .client(
                &request_trusting(b"# a comment, and not one certificate\n"),
                socket,
            )
            .expect_err("a bundle with no certificate in it cannot be a trust root");
        assert_eq!(empty.code(), ErrorCode::BackendError);
        assert!(
            empty.to_string().contains("no certificates"),
            "an operator must be told their CA bundle is empty, not merely that something failed: \
             {empty}"
        );

        let malformed = transport
            .client(
                &request_trusting(
                    b"-----BEGIN CERTIFICATE-----\nAAAAAAAA\n-----END CERTIFICATE-----\n",
                ),
                socket,
            )
            .expect_err("a PEM block whose contents are not a certificate cannot be a trust root");
        assert_eq!(malformed.code(), ErrorCode::BackendError);

        assert!(
            transport.clients.lock().unwrap().is_empty(),
            "a bundle that was refused must not leave a client cached under its key"
        );
    }

    /// A camera whose address keeps moving must not be able to grow the cache without bound.
    ///
    /// The address is PINNED into the client (`resolve`), so every address a camera is seen at is a
    /// new key. A camera on DHCP, or a fleet behind a NAT that renumbers, would otherwise add an
    /// entry -- and a whole rustls configuration and connection pool -- forever, for the life of the
    /// process. The bound is deliberately crude: emptying the cache costs one handshake per camera,
    /// and an unbounded map costs the process.
    #[tokio::test]
    async fn the_client_cache_empties_rather_than_growing_without_bound() {
        let transport = ReqwestOnvifTransport::default();
        let socket = SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 1]), 8_443);
        let request = loopback_http_request(8_443, OnvifHttpMethod::Post, 32);
        let client = transport
            .client(&request, socket)
            .expect("one real client to fill the cache with");

        // Stand in for a camera that has been seen at MAX_POOLED_CLIENTS distinct addresses. Building
        // that many real clients would prove nothing extra and cost a rustls configuration each.
        {
            let mut clients = transport.clients.lock().unwrap();
            for index in 0..u16::try_from(MAX_POOLED_CLIENTS).expect("the bound fits a port") {
                clients.insert(
                    HttpClientKey {
                        host: "127.0.0.1".to_owned(),
                        socket: SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 1]), index),
                        verify_hostname: true,
                        allow_invalid_certificates: false,
                        ca_digest: None,
                    },
                    client.clone(),
                );
            }
            assert!(clients.len() >= MAX_POOLED_CLIENTS);
        }

        let moved = SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 2]), 8_443);
        transport
            .client(&request, moved)
            .expect("a camera that has moved again must still get a client");

        assert_eq!(
            transport.clients.lock().unwrap().len(),
            1,
            "past the bound the cache is emptied and restarted, not grown"
        );
    }

    /// A panic somewhere else in the process must not take the camera transport with it.
    ///
    /// `clients` is a `std::sync::Mutex`, so a thread that panics while holding it poisons it
    /// permanently. `client()` reads it with `if let Ok(..)` rather than `unwrap()` on purpose: the
    /// cache is an optimisation, and losing it must cost a TLS handshake, not every subsequent ONVIF
    /// request the component will ever make.
    #[tokio::test]
    async fn a_poisoned_client_cache_costs_a_handshake_and_nothing_more() {
        let transport = Arc::new(ReqwestOnvifTransport::default());
        let socket = SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 1]), 8_443);
        let request = loopback_http_request(8_443, OnvifHttpMethod::Post, 32);

        transport
            .client(&request, socket)
            .expect("a client is cached first, so the poisoned read has something to lose");

        let poisoner = Arc::clone(&transport);
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let panicked = std::thread::spawn(move || {
            let _held = poisoner
                .clients
                .lock()
                .expect("the cache is not yet poisoned");
            panic!("a thread died holding the client cache");
        })
        .join();
        std::panic::set_hook(previous);
        assert!(
            panicked.is_err(),
            "the poisoning thread must actually panic"
        );
        assert!(
            transport.clients.lock().is_err(),
            "the client cache must really be poisoned, or this test proves nothing"
        );

        transport
            .client(&request, socket)
            .expect("a poisoned cache must cost a handshake, not the camera");
    }

    #[tokio::test]
    async fn production_http_transport_uses_pinned_loopback_and_enforces_response_bounds() {
        let transport = ReqwestOnvifTransport::default();
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
            ..Default::default()
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
        assert!(SystemNetClock.now() <= Utc::now());
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
            #[cfg(feature = "rtsp")]
            snapshot_unavailable: None,
            max_snapshot_bytes: 1_024,
            #[cfg(feature = "rtsp")]
            rtsp: None,
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
        let resolver: Arc<dyn AddressResolver> = Arc::new(SequenceResolver::sequence(
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
        let resolver: Arc<dyn AddressResolver> = Arc::new(SequenceResolver::sequence(
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
        let credentials = NetworkCredentials::new("operator", "camera-secret").expect("credentials");
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
        let credentials = NetworkCredentials::new("operator", "camera-secret").expect("credentials");
        assert!(basic_authorization(&credentials, false).is_err());
        let value = basic_authorization(&credentials, true).expect("allowed basic");
        assert!(value.expose_utf8().expect("header").starts_with("Basic "));
    }

    #[tokio::test]
    async fn auto_authentication_prefers_digest_before_basic() {
        let resolver: Arc<dyn AddressResolver> =
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])]));
        let transport = Arc::new(MockTransport::default());
        transport.push(unauthorized(
            r#"Basic realm="fallback", Digest realm="test", nonce="abc123", algorithm=MD5, qop="auth""#,
        ));
        transport.push(soap_ok("<tds:GetServicesResponse xmlns:tds=\"urn:test\"/>"));
        let mut config = test_config(Some("http://camera.test/onvif/device_service"));
        config.authentication_mode = AuthenticationMode::Auto;
        let credentials =
            Arc::new(NetworkCredentials::new("operator", "camera-secret").expect("credentials"));
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
        let resolver: Arc<dyn AddressResolver> =
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
            Arc::new(NetworkCredentials::new("operator", "camera-secret").expect("credentials"));
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
            Arc::new(NetworkCredentials::new("operator", "camera-secret").expect("test credentials"));

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
        let resolver: Arc<dyn AddressResolver> =
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])]));
        let transport = Arc::new(MockTransport::default());
        transport.push(unauthorized(
            r#"Digest realm="test", nonce="new", algorithm=MD5, qop="auth""#,
        ));
        let config = test_config(Some("http://camera.test/onvif/device_service"));
        let credentials =
            Arc::new(NetworkCredentials::new("operator", "camera-secret").expect("credentials"));
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
        let resolver: Arc<dyn AddressResolver> = Arc::new(SequenceResolver::new(&[
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
            Arc::new(NetworkCredentials::new("operator", "camera-secret").expect("credentials"));
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
        let resolver: Arc<dyn AddressResolver> = Arc::new(SequenceResolver::new(&[
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
            Arc::new(NetworkCredentials::new("operator", "camera-secret").expect("credentials"));
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

    #[tokio::test]
    async fn snapshot_digest_challenge_is_negotiated_once_then_reused_for_the_retry() {
        let resolver: Arc<dyn AddressResolver> =
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])]));
        let transport = Arc::new(MockTransport::default());
        transport.push(unauthorized(
            r#"Digest realm="camera", nonce="snapshot-nonce", algorithm=MD5, qop="auth""#,
        ));
        transport.push(OnvifHttpResponse {
            status: 200,
            headers: BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
            body: png(8, 8),
        });
        let mut config = test_config(Some("http://camera.test/onvif/device_service"));
        config.allowed_uri_cidrs = vec!["10.0.0.0/24".parse().expect("CIDR")];
        let credentials =
            Arc::new(NetworkCredentials::new("operator", "camera-secret").expect("credentials"));
        let (client, endpoint) = test_client(
            resolver,
            transport.clone(),
            &config,
            Some(credentials),
            SecurityConfig::default(),
        )
        .await;
        let cancellation = CancellationToken::new();
        let (response, target) = client
            .fetch_snapshot(
                endpoint.clone(),
                1_024,
                Instant::now() + Duration::from_secs(2),
                &cancellation,
            )
            .await
            .expect("one Digest challenge retry must produce the snapshot");
        assert_eq!(response.status, 200);
        assert_eq!(target.host(), "camera.test");
        let requests = transport.observations();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].authorization.is_none());
        assert!(
            requests[1]
                .authorization
                .as_deref()
                .is_some_and(|value| value.starts_with("Digest "))
        );

        transport.push(OnvifHttpResponse {
            status: 404,
            headers: BTreeMap::new(),
            body: Vec::new(),
        });
        let fallback = match client
            .fetch_snapshot(
                endpoint.clone(),
                1_024,
                Instant::now() + Duration::from_secs(2),
                &cancellation,
            )
            .await
        {
            Err(failure) => failure,
            Ok(_) => panic!("HTTP 404 must request the configured RTSP fallback"),
        };
        assert!(matches!(
            fallback,
            SnapshotAttemptFailure::Fallback {
                reason: SnapshotFallbackReason::HttpNotFound,
                ..
            }
        ));

        transport.push(OnvifHttpResponse {
            status: 500,
            headers: BTreeMap::new(),
            body: Vec::new(),
        });
        let fatal = match client
            .fetch_snapshot(
                endpoint,
                1_024,
                Instant::now() + Duration::from_secs(2),
                &cancellation,
            )
            .await
        {
            Err(failure) => failure,
            Ok(_) => panic!("unexpected snapshot HTTP failures must be fatal"),
        };
        assert_eq!(fatal.into_error().code(), ErrorCode::BackendError);
        assert!(matches!(
            classify_snapshot_transport_failure(timeout_error("test transport")),
            SnapshotAttemptFailure::Fallback {
                reason: SnapshotFallbackReason::TransportTimeout,
                ..
            }
        ));
        assert!(matches!(
            classify_snapshot_transport_failure(backend_error("test transport")),
            SnapshotAttemptFailure::Fatal(_)
        ));
    }

    #[tokio::test]
    async fn snapshot_basic_challenge_requires_both_plaintext_permissions_before_retrying() {
        let resolver: Arc<dyn AddressResolver> =
            Arc::new(SequenceResolver::new(&[("camera.test", 80, &["10.0.0.2"])]));
        let transport = Arc::new(MockTransport::default());
        transport.push(unauthorized("Basic realm=\"camera\""));
        transport.push(OnvifHttpResponse {
            status: 200,
            headers: BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
            body: png(8, 8),
        });
        let mut config = test_config(Some("http://camera.test/onvif/device_service"));
        config.allowed_uri_cidrs = vec!["10.0.0.0/24".parse().expect("CIDR")];
        let credentials =
            Arc::new(NetworkCredentials::new("operator", "camera-secret").expect("credentials"));
        let (client, endpoint) = test_client(
            resolver,
            transport.clone(),
            &config,
            Some(credentials),
            SecurityConfig {
                allow_basic_over_plaintext: true,
                ..SecurityConfig::default()
            },
        )
        .await;
        let response = client
            .fetch_snapshot(
                endpoint,
                1_024,
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await
            .expect("explicitly permitted Basic retry must succeed");
        assert_eq!(response.0.status, 200);
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
        let resolver: Arc<dyn AddressResolver> =
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
    fn ptz_response_parser_handles_presets_status_and_malformed_camera_replies() {
        let range = |min, max| AxisRange { min, max };
        let ranges = PtzRanges {
            absolute_pan: range(-2.0, 2.0),
            absolute_tilt: range(-4.0, 4.0),
            absolute_zoom: range(0.0, 5.0),
            relative_pan: range(-1.0, 1.0),
            relative_tilt: range(-1.0, 1.0),
            relative_zoom: range(-1.0, 1.0),
            velocity_pan: range(-1.0, 1.0),
            velocity_tilt: range(-1.0, 1.0),
            velocity_zoom: range(-1.0, 1.0),
        };
        let observed_at = FixedClock.now();

        assert!(matches!(
            parse_ptz_response(
                PtzResponseKind::Commanded,
                b"<Envelope/>",
                1_024,
                8,
                &ranges,
                observed_at,
            )
            .expect("commanded response"),
            PtzResult::Commanded
        ));
        assert!(matches!(
            parse_ptz_response(
                PtzResponseKind::Removed,
                b"<Envelope/>",
                1_024,
                8,
                &ranges,
                observed_at,
            )
            .expect("removed response"),
            PtzResult::Removed
        ));
        assert!(matches!(
            parse_ptz_response(
                PtzResponseKind::PresetToken,
                b"<Envelope><PresetToken>preset-dock</PresetToken></Envelope>",
                1_024,
                8,
                &ranges,
                observed_at,
            )
            .expect("preset token response"),
            PtzResult::PresetToken(token) if token == "preset-dock"
        ));

        let PtzResult::Presets(presets) = parse_ptz_response(
            PtzResponseKind::Presets,
            b"<Envelope><Preset token=\"preset-north\"><Name>North loading bay</Name></Preset><Preset><token>preset-south</token></Preset></Envelope>",
            1_024,
            8,
            &ranges,
            observed_at,
        )
        .expect("preset list with both token representations") else {
            panic!("expected preset list");
        };
        assert_eq!(presets.len(), 2);
        assert_eq!(presets[0].token, "preset-north");
        assert_eq!(presets[0].name.as_deref(), Some("North loading bay"));
        assert_eq!(presets[1].token, "preset-south");
        assert!(presets[1].name.is_none());

        let PtzResult::Status(status) = parse_ptz_response(
            PtzResponseKind::Status,
            b"<Envelope><PTZStatus><Position><PanTilt x=\"0\" y=\"0\"/><Zoom x=\"2.5\"/></Position><MoveStatus><PanTilt>UNKNOWN</PanTilt><Zoom>MOVING</Zoom></MoveStatus></PTZStatus></Envelope>",
            1_024,
            8,
            &ranges,
            observed_at,
        )
        .expect("status with an unknown move state") else {
            panic!("expected PTZ status");
        };
        assert_eq!(status.position.expect("position").zoom, 0.5);
        assert!(status.moving.is_none());
        assert_eq!(status.observed_at, observed_at);

        assert!(
            parse_ptz_response(
                PtzResponseKind::PresetToken,
                b"<Envelope><SetPresetResponse/></Envelope>",
                1_024,
                8,
                &ranges,
                observed_at,
            )
            .is_err()
        );
        assert!(parse_ptz_response(
            PtzResponseKind::Status,
            b"<Envelope><PTZStatus><Position><PanTilt x=\"not-a-number\" y=\"0\"/><Zoom x=\"0\"/></Position></PTZStatus></Envelope>",
            1_024,
            8,
            &ranges,
            observed_at,
        )
        .is_err());
    }

    #[test]
    fn onvif_response_parsers_select_single_values_and_reject_ambiguous_camera_data() {
        let profiles = parse_profiles(
            b"<Envelope><Profiles token=\"main\"><Name>Default</Name><PTZConfigurationToken>ptz-main</PTZConfigurationToken></Profiles><Profiles token=\"aux\"><Name>Auxiliary</Name></Profiles><Profiles/></Envelope>",
            4_096,
            8,
        )
        .expect("well-formed media profiles");
        assert_eq!(profiles.len(), 2);
        assert_eq!(
            select_profile(&profiles, "Default")
                .expect("unambiguous display name")
                .expect("selected profile")
                .token,
            "main"
        );
        assert!(
            select_profile(&profiles, "missing")
                .expect("missing selector is not an error")
                .is_none()
        );
        assert!(
            select_profile(
                &[
                    MediaProfileRecord {
                        token: "one".to_owned(),
                        name: Some("same".to_owned()),
                        ptz_configuration_token: None,
                    },
                    MediaProfileRecord {
                        token: "two".to_owned(),
                        name: Some("same".to_owned()),
                        ptz_configuration_token: None,
                    },
                ],
                "same"
            )
            .is_err()
        );
        assert!(
            parse_profiles(
                format!(
                    "<Envelope><Profiles token=\"{}\"/></Envelope>",
                    "x".repeat(1_025)
                )
                .as_bytes(),
                4_096,
                8,
            )
            .is_err()
        );

        let information = parse_device_information(
            b"<Envelope><Manufacturer>EdgeCommons</Manufacturer><Model>Simulator</Model><FirmwareVersion>1.2.3</FirmwareVersion><SerialNumber>SIM-42</SerialNumber></Envelope>",
            4_096,
            8,
        )
        .expect("device information response");
        assert_eq!(information.manufacturer.as_deref(), Some("EdgeCommons"));
        assert_eq!(information.model.as_deref(), Some("Simulator"));
        assert_eq!(information.firmware.as_deref(), Some("1.2.3"));
        assert_eq!(information.serial.as_deref(), Some("SIM-42"));

        assert_eq!(
            parse_ptz_configuration_token(
                b"<Envelope><PTZConfiguration token=\"ptz-main\"/></Envelope>",
                4_096,
                8,
            )
            .expect("single PTZ configuration token"),
            "ptz-main"
        );
        assert!(parse_ptz_configuration_token(
            b"<Envelope><PTZConfiguration token=\"one\"/><PTZConfiguration token=\"two\"/></Envelope>",
            4_096,
            8,
        )
        .is_err());
        assert!(parse_ptz_configuration_token(b"<Envelope/>", 4_096, 8).is_err());

        let features = parse_ptz_node_features(
            b"<Envelope><PTZNode><HomeSupported>1</HomeSupported><MaximumNumberOfPresets>8</MaximumNumberOfPresets></PTZNode></Envelope>",
            4_096,
            8,
        )
        .expect("PTZ node features");
        assert!(features.home);
        assert!(features.presets);
        assert!(parse_ptz_node_features(b"<Envelope/>", 4_096, 8).is_err());

        assert_eq!(
            parse_single_uri(
                b"<Envelope><Uri>rtsp://camera.test/main</Uri></Envelope>",
                4_096,
                8
            )
            .expect("single media URI"),
            "rtsp://camera.test/main"
        );
        assert!(parse_single_uri(
            b"<Envelope><Uri>rtsp://camera.test/one</Uri><Uri>rtsp://camera.test/two</Uri></Envelope>",
            4_096,
            8,
        )
        .is_err());
        assert!(parse_single_uri(b"<Envelope/>", 4_096, 8).is_err());

        assert_eq!(
            SnapshotFallbackReason::for_http_status(404),
            Some(SnapshotFallbackReason::HttpNotFound)
        );
        assert_eq!(
            SnapshotFallbackReason::for_http_status(405),
            Some(SnapshotFallbackReason::HttpMethodNotAllowed)
        );
        assert_eq!(
            SnapshotFallbackReason::for_http_status(410),
            Some(SnapshotFallbackReason::HttpGone)
        );
        assert_eq!(
            SnapshotFallbackReason::for_http_status(501),
            Some(SnapshotFallbackReason::HttpNotImplemented)
        );
        assert_eq!(SnapshotFallbackReason::for_http_status(500), None);
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
        let resolver: Arc<dyn AddressResolver> =
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
            ..Default::default()
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
        let resolver: Arc<dyn AddressResolver> = Arc::new(SequenceResolver::new(&[]));
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
            ..Default::default()
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
        let resolver: Arc<dyn AddressResolver> = Arc::new(SequenceResolver::new(&[
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
            ..Default::default()
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
            ..Default::default()
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
        let resolver: Arc<dyn AddressResolver> =
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
            ..Default::default()
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

    /// A JPEG the "camera" will serve. Distinct per pixel, so a reordered or padded frame cannot
    /// accidentally compare equal to the original.
    fn jpeg(width: u32, height: u32) -> Vec<u8> {
        let image = RgbImage::from_fn(width, height, |x, y| {
            Rgb([
                (x % 251) as u8,
                (y % 241) as u8,
                ((x * 7 + y * 13) % 233) as u8,
            ])
        });
        let mut output = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(image)
            .write_to(&mut output, ImageFormat::Jpeg)
            .expect("encode test JPEG");
        output.into_inner()
    }

    /// A well-framed HTTP/1.1 200 carrying exactly `body` under `content_type`.
    ///
    /// The Content-Length is the length of what is actually written, so a truncated image is a
    /// truncated IMAGE on an otherwise perfectly valid HTTP response -- which is the defect that
    /// matters here, not a broken socket.
    fn http_image_response(content_type: &str, body: &[u8]) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }

    /// A protocol client whose snapshot endpoint is a real loopback socket, reached through the real
    /// `ReqwestOnvifTransport`.
    ///
    /// The camera is `camera.test` -- the pinned-DNS answer is `127.0.0.1`, so the production
    /// address-pinning path (`resolve` + the endpoint-address allowance) is what puts the request on
    /// the fixture's socket. Nothing about the HTTP client is mocked.
    async fn loopback_snapshot_client(port: u16) -> (OnvifProtocolClient, PinnedUri) {
        let config = test_config(Some(&format!("http://camera.test:{port}/snapshot")));
        test_client(
            Arc::new(SequenceResolver::new(&[(
                "camera.test",
                port,
                &["127.0.0.1"],
            )])),
            Arc::new(ReqwestOnvifTransport::default()),
            &config,
            None,
            SecurityConfig::default(),
        )
        .await
    }

    /// A snapshot-only ONVIF session over `client`, whose snapshot URI is `endpoint`.
    fn snapshot_session(client: OnvifProtocolClient, endpoint: PinnedUri) -> OnvifSession {
        OnvifSession {
            instance_id: "camera-a".to_owned(),
            client,
            media_endpoint: endpoint.clone(),
            media_version: MediaVersion::Media1,
            media_profile_token: "main".to_owned(),
            snapshot_endpoint: Some(endpoint),
            #[cfg(feature = "rtsp")]
            snapshot_unavailable: None,
            max_snapshot_bytes: 1_048_576,
            #[cfg(feature = "rtsp")]
            rtsp: None,
            ptz_endpoint: None,
            ptz_ranges: None,
            ptz_home: false,
            capabilities: CameraCapabilities {
                capture_modes: vec![CaptureMode::SnapshotUri],
                pixel_formats: vec![PixelFormat::Jpeg],
                software_trigger: false,
                snapshot_uri: true,
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
        }
    }

    /// A snapshot capture request with room to spare, so nothing here is about a bound.
    fn snapshot_capture_request() -> CaptureRequest {
        let profile: CaptureProfile = serde_json::from_value(json!({
            "captureMode": "snapshot-uri",
            "output": { "encoding": "jpeg" }
        }))
        .expect("capture profile");
        CaptureRequest {
            capture_id: "byte-fidelity".to_owned(),
            profile,
            maximum_frame_bytes: 1_048_576,
            timeout: Duration::from_secs(5),
            cancellation: CancellationToken::new(),
        }
    }

    /// The JPEG the frame carries is the JPEG the camera put on the wire. Byte for byte.
    ///
    /// Nothing proved this for the ONVIF path. Downstream, storage computes the artifact's SHA-256 by
    /// RE-READING the file it just wrote and then verifies the file against that same digest -- every
    /// link checks the bytes against themselves, and not one checks them against the CAMERA. A frame
    /// truncated, padded, reordered or swapped anywhere between the HTTP body and `CaptureFrame` would
    /// produce a perfectly self-consistent digest OF THE WRONG IMAGE, and the whole suite would stay
    /// green. For an adapter whose entire purpose is to deliver the picture the camera took, this is
    /// the property worth pinning.
    ///
    /// For JPEG the claim is exact rather than approximate: `snapshot_to_frame` hands the frame
    /// `Bytes::from(response.body)` -- the camera's ORIGINAL bytes -- and decodes only to sanity-check
    /// the dimensions and the declared format. So the delivered bytes must equal the served bytes
    /// EXACTLY, and their digests must agree.
    ///
    /// The camera here is a real `TcpListener` reached through the real `ReqwestOnvifTransport` and
    /// the real `fetch_snapshot` + decode path: a mock transport handing back a `Vec<u8>` it was given
    /// could not see a body mangled by the HTTP client, and that is exactly where a frame can rot.
    #[tokio::test]
    async fn the_jpeg_frame_delivered_over_real_http_is_the_jpeg_the_camera_served() {
        use sha2::Digest as _;

        let served = jpeg(64, 48);
        let (port, server) = serve_loopback_http(http_image_response("image/jpeg", &served)).await;
        let (client, endpoint) = loopback_snapshot_client(port).await;

        let deadline = Instant::now() + Duration::from_secs(10);
        let cancellation = CancellationToken::new();
        let (response, target) = client
            .fetch_snapshot(endpoint, 1_048_576, deadline, &cancellation)
            .await
            .expect("the loopback camera must serve its snapshot over real HTTP");
        let frame = decode_snapshot_at_safe_boundary(
            response,
            1_048_576,
            SecurityConfig::default().max_decompression_ratio,
            FixedClock.now(),
            target.host().to_owned(),
            deadline,
            &cancellation,
        )
        .await
        .expect("a well-formed JPEG snapshot must become a frame");

        let request =
            String::from_utf8(server.await.expect("HTTP server task")).expect("HTTP request text");
        assert!(
            request.starts_with("GET /snapshot HTTP/1.1\r\n"),
            "the frame must have come from a real HTTP GET against the camera, not from thin air: \
             {request:?}"
        );

        assert_eq!(
            frame.bytes.len(),
            served.len(),
            "the delivered frame is a different SIZE from the JPEG the camera served"
        );
        assert!(
            frame.bytes.as_ref() == served.as_slice(),
            "the delivered frame is not the image the camera served -- same length, different bytes, \
             which is precisely the corruption a self-referential digest cannot see"
        );
        assert_eq!(
            hex::encode(Sha256::digest(frame.bytes.as_ref())),
            hex::encode(Sha256::digest(&served)),
            "the digest of the frame must be the digest of the CAMERA'S bytes; a digest recomputed \
             from whatever was carried forward would agree with a corrupted frame just as happily"
        );
        assert_eq!(
            (frame.pixel_format, frame.width, frame.height),
            (PixelFormat::Jpeg, 64, 48),
            "a JPEG snapshot is delivered as the camera's own JPEG at the camera's own dimensions"
        );
        assert_eq!(
            frame.capture_mode,
            CaptureMode::SnapshotUri,
            "and it must say it came from the snapshot URI"
        );
    }

    /// The same claim, end to end through `OnvifSession::capture`: what a caller of the backend gets
    /// back is the camera's own JPEG, byte for byte.
    ///
    /// The transport-level test above pins the fetch/decode pair. This one pins the surface the rest of
    /// the adapter actually calls, with the bounds, the deadline, the capture-mode dispatch and the
    /// fallback plumbing of `capture()` in the way. A regression that mangled a frame anywhere in that
    /// wrapping -- re-encoding it, copying it through a fixed-size buffer, truncating it to the
    /// declared bound -- would leave every existing ONVIF test green and this one red.
    #[tokio::test]
    async fn session_capture_delivers_the_exact_jpeg_bytes_the_camera_put_on_the_wire() {
        use sha2::Digest as _;

        let served = jpeg(80, 60);
        let (port, server) = serve_loopback_http(http_image_response("image/jpeg", &served)).await;
        let (client, endpoint) = loopback_snapshot_client(port).await;
        let mut session = snapshot_session(client, endpoint);

        let frame = session
            .capture(snapshot_capture_request())
            .await
            .expect("a snapshot capture against a healthy camera must succeed");
        let _ = server.await.expect("HTTP server task");

        assert_eq!(
            frame.bytes.len(),
            served.len(),
            "capture() delivered a frame of a different SIZE from the JPEG the camera served"
        );
        assert!(
            frame.bytes.as_ref() == served.as_slice(),
            "capture() did not deliver the image the camera took -- the bytes handed to the caller \
             differ from the bytes the camera served over HTTP"
        );
        assert_eq!(
            hex::encode(Sha256::digest(frame.bytes.as_ref())),
            hex::encode(Sha256::digest(&served)),
            "the digest of the captured frame must be the digest of the camera's served JPEG"
        );
        assert_eq!(
            frame.backend_metadata.get("sourceEncoding"),
            Some(&Value::String("jpeg".to_owned())),
            "and the frame must declare the encoding it is actually carrying"
        );
    }

    /// For PNG the frame is NOT the file: `snapshot_to_frame` carries `decoded.to_rgb8().into_raw()`.
    /// So the property is that the delivered raster is exactly the raster an INDEPENDENT decode of the
    /// same PNG produces -- pinned here against `image`'s own decoder, run outside the capture path.
    ///
    /// Asserting byte equality with the PNG file would be asserting the wrong thing and would fail on
    /// correct code. Asserting only "some RGB8 bytes of the right length arrived" would be asserting
    /// nothing: a raster with two rows swapped, a channel rotated, or a stale buffer reused has exactly
    /// the right length. This asserts the pixels.
    #[tokio::test]
    async fn session_capture_of_a_png_delivers_the_raster_an_independent_decode_produces() {
        let served = png(32, 24);
        let expected = image::load_from_memory_with_format(&served, ImageFormat::Png)
            .expect("the fixture PNG must decode independently of the capture path")
            .to_rgb8()
            .into_raw();

        let (port, server) = serve_loopback_http(http_image_response("image/png", &served)).await;
        let (client, endpoint) = loopback_snapshot_client(port).await;
        let mut session = snapshot_session(client, endpoint);

        let frame = session
            .capture(snapshot_capture_request())
            .await
            .expect("a PNG snapshot capture must succeed");
        let _ = server.await.expect("HTTP server task");

        assert_eq!(
            (frame.pixel_format, frame.width, frame.height),
            (PixelFormat::Rgb8, 32, 24),
            "a PNG snapshot is delivered as an RGB8 raster at the camera's dimensions"
        );
        assert_eq!(
            frame.bytes.len(),
            expected.len(),
            "the delivered raster is a different SIZE from the raster the served PNG decodes to"
        );
        assert!(
            frame.bytes.as_ref() == expected.as_slice(),
            "the delivered raster is not the picture the camera sent -- same length, different \
             pixels, which is exactly what a swapped, shifted or stale buffer looks like"
        );
        assert!(
            frame.bytes.as_ref() != served.as_slice(),
            "and the PNG claim really is about the DECODED raster, not the file: if the frame were \
             the file's bytes, this test would be pinning the wrong property"
        );
    }

    /// The offset of the JPEG Start-of-Scan marker: everything before it is header, everything after
    /// it is the picture itself.
    fn start_of_scan(jpeg: &[u8]) -> usize {
        jpeg.windows(2)
            .position(|window| window == [0xFF, 0xDA])
            .expect("a JPEG the encoder produced must contain a Start-of-Scan marker")
    }

    /// A snapshot that never carried a whole picture must be REFUSED, not delivered.
    ///
    /// Three ways the wire lies, and the adapter must call all three:
    ///
    /// * the HTTP body stops short of its declared `Content-Length` -- a camera or a middlebox that
    ///   drops the connection mid-response. The bytes that DID arrive are a valid JPEG prefix; nothing
    ///   downstream would ever notice, because storage digests whatever it is handed and then verifies
    ///   that digest against itself.
    /// * the image data never arrived at all: the headers are complete, the dimensions read cleanly,
    ///   but the entropy-coded scan is absent. A decoder that shrugged and returned a blank frame would
    ///   hand the operator a picture the camera never took.
    /// * the `Content-Type` disagrees with the bytes (a PNG served as `image/jpeg`). That header is what
    ///   decides whether the ORIGINAL bytes are passed through (JPEG) or re-decoded to a raster (PNG),
    ///   so believing a camera that lies about it means delivering bytes that are not what they claim.
    ///
    /// Producing a frame at all in any of these cases is the failure.
    #[tokio::test]
    async fn a_snapshot_that_never_carried_a_whole_picture_is_refused_rather_than_delivered() {
        let whole = jpeg(64, 48);

        // The camera promises a whole JPEG and then hangs up half way through it.
        let mut short_body = http_image_response("image/jpeg", &whole);
        short_body.truncate(short_body.len() - (whole.len() / 2));
        let (port, server) = serve_loopback_http(short_body).await;
        let (client, endpoint) = loopback_snapshot_client(port).await;
        let mut session = snapshot_session(client, endpoint);
        let error = session
            .capture(snapshot_capture_request())
            .await
            .expect_err(
                "a body that stops short of its declared Content-Length must not become a frame",
            );
        let _ = server.await.expect("short-body server task");
        assert_eq!(
            error.code(),
            ErrorCode::BackendError,
            "an HTTP body cut short of its own Content-Length is a transport failure and must be \
             reported as one, never silently delivered as a picture -- got {error}"
        );

        // Complete headers -- the dimensions read perfectly -- and no picture behind them.
        let headers_only = whole[..start_of_scan(&whole)].to_vec();
        let (port, server) =
            serve_loopback_http(http_image_response("image/jpeg", &headers_only)).await;
        let (client, endpoint) = loopback_snapshot_client(port).await;
        let mut session = snapshot_session(client, endpoint);
        let error = session
            .capture(snapshot_capture_request())
            .await
            .expect_err("a JPEG whose scan data never arrived must not become a frame");
        let _ = server.await.expect("headers-only server task");
        assert_eq!(
            error.code(),
            ErrorCode::UnsupportedCapability,
            "a corrupt snapshot is a fallback-eligible failure; with no RTSP stream configured the \
             capture must fail rather than deliver a picture the camera never sent -- got {error}"
        );

        // A PNG wearing a JPEG's Content-Type.
        let served = png(32, 24);
        let (port, server) = serve_loopback_http(http_image_response("image/jpeg", &served)).await;
        let (client, endpoint) = loopback_snapshot_client(port).await;
        let mut session = snapshot_session(client, endpoint);
        let error = session
            .capture(snapshot_capture_request())
            .await
            .expect_err("bytes that disagree with their Content-Type must never become a frame");
        let _ = server.await.expect("mislabelled-snapshot server task");
        assert!(
            error
                .to_string()
                .contains("snapshot Content-Type does not match its bytes"),
            "the refusal must name the lie -- the camera's Content-Type disagreed with its bytes -- \
             got {error}"
        );
    }

    /// A picture that stopped arriving is refused, not delivered.
    ///
    /// This test used to assert the opposite, because the adapter used to do the opposite. Once the
    /// entropy-coded scan has begun, `image`'s JPEG decoder is deliberately lenient: a truncated scan
    /// does not error, it yields the rows it could reconstruct. `snapshot_to_frame` consulted that
    /// decode only as a dimension/format sanity check and then passed the ORIGINAL body through -- so
    /// the frame reaching the caller was a partial picture, with valid JPEG structure, the right
    /// dimensions, and a digest that verified against itself at every downstream link. A half-image,
    /// reported as a success.
    ///
    /// The only thing standing between the operator and that half-image was HTTP framing, which fires
    /// only when `Content-Length` disagrees. A camera that hangs up mid-scan, or a middlebox that
    /// truncates a chunked body without breaking framing, was invisible.
    ///
    /// A JPEG that never wrote its `FF D9` end-of-image marker never finished writing the picture, and
    /// that test is exact rather than heuristic: inside entropy-coded data every `0xFF` is byte-stuffed
    /// as `FF 00`, so a genuine `FF D9` cannot occur within the scan. It is now refused, and refusal on
    /// a snapshot is not a dead end -- `CorruptImage` is the fallback reason that sends the capture down
    /// the RTSP path instead.
    #[tokio::test]
    async fn a_picture_that_stopped_arriving_is_refused_rather_than_delivered_half_finished() {
        let whole = jpeg(64, 48);
        let scan = start_of_scan(&whole);
        // Well inside the picture data: every header is intact, half the picture is missing.
        let truncated = whole[..(scan + (whole.len() - scan) / 2)].to_vec();
        assert!(
            truncated.len() < whole.len(),
            "the fixture must actually be missing picture data"
        );
        assert!(
            !truncated.windows(2).any(|pair| pair == [0xFF, 0xD9]),
            "and it must be missing its end-of-image marker, which is what makes it detectable"
        );

        let (port, server) =
            serve_loopback_http(http_image_response("image/jpeg", &truncated)).await;
        let (client, endpoint) = loopback_snapshot_client(port).await;
        let mut session = snapshot_session(client, endpoint);
        let outcome = session.capture(snapshot_capture_request()).await;
        let _ = server.await.expect("mid-scan-truncation server task");

        let error = outcome.expect_err(
            "a picture whose scan never finished arriving must not be delivered as a success",
        );
        // `CorruptImage` is a FALLBACK reason, not a dead end: it retires the snapshot and sends the
        // capture down the RTSP path. This session has no RTSP, so the capture fails there -- which is
        // the correct outcome. What matters is that no half-picture reached the caller.
        assert_eq!(
            error.code(),
            ErrorCode::UnsupportedCapability,
            "the snapshot is retired as corrupt and the capture falls through to RTSP, which this              camera does not have; what must NOT happen is a half-picture delivered as a success"
        );
    }
}
