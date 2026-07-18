//! Protocol-neutral networking, credential, and HTTP-authentication primitives.
//!
//! These helpers are shared by the ONVIF and RTSP backends and know nothing about either
//! protocol message grammar. They cover secret-bearing buffers, the DNS/clock/nonce seams, the
//! forbidden-address and hostname policies, the RTSP network trust anchor, and the HTTP Digest and
//! Basic authorization builders. No secret-bearing type exposes a useful `Debug` representation.

use std::collections::BTreeMap;
#[cfg(any(feature = "rtsp", test))]
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, Utc};
#[cfg(any(feature = "rtsp", test))]
use ipnet::IpNet;
use md5::{Digest as _, Md5};
use rand::RngCore;
use sha2::Sha256;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::config::SecretRef;
use crate::{CameraError, ErrorCode, Result};

/// Backend label shared by the network primitives errors. Keeping it identical to the ONVIF
/// backend label preserves the exact error text these helpers produced before extraction.
const NET_BACKEND: &str = "onvif-rtsp";
const MAX_AUTH_HEADER_BYTES: usize = 16 * 1024;
pub(crate) const MAX_BLOCKING_DNS_LOOKUPS: usize = 16;

fn backend_error(message: impl Into<String>) -> CameraError {
    CameraError::Backend {
        backend: NET_BACKEND,
        message: message.into(),
    }
}

pub(crate) fn security_error(message: impl AsRef<str>) -> CameraError {
    backend_error(format!(
        "security policy rejected ONVIF input: {}",
        message.as_ref()
    ))
}

pub(crate) fn timeout_error(stage: &'static str) -> CameraError {
    CameraError::rejected(
        ErrorCode::CaptureTimeout,
        format!("ONVIF {stage} exceeded its deadline"),
    )
}

pub(crate) fn cancelled_error(stage: &'static str) -> CameraError {
    CameraError::rejected(
        ErrorCode::CaptureCancelled,
        format!("ONVIF {stage} was cancelled"),
    )
}


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

    pub(crate) fn expose_utf8(&self) -> Result<&str> {
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
pub struct NetworkCredentials {
    username: SecretBytes,
    pub(crate) password: SecretBytes,
}

impl NetworkCredentials {
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

impl std::fmt::Debug for NetworkCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NetworkCredentials(<redacted>)")
    }
}

/// Lazy standard-secret resolution seam shared by the ONVIF and RTSP backends.
///
/// It lives here rather than in `onvif` so that the RTSP backend, which can be built without the
/// ONVIF feature, resolves its `credentials`/`tls.ca` references through the exact same
/// EdgeCommons-backed provider the ONVIF factory uses.
#[async_trait]
pub trait CredentialProvider: Send + Sync {
    /// Resolves a login object containing `username` and `password`.
    async fn resolve_login(&self, reference: &SecretRef) -> Result<Arc<NetworkCredentials>>;

    /// Resolves opaque bytes, used for a private CA bundle.
    async fn resolve_bytes(&self, reference: &SecretRef) -> Result<Arc<SecretBytes>>;
}

/// Resolves login credentials under the caller's deadline and cancellation signal.
///
/// A credential store that hangs must not hang a camera connect: the deadline and token bound the
/// resolution exactly as production requires.
pub(crate) async fn resolve_login_bounded(
    provider: &dyn CredentialProvider,
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

/// Resolves opaque secret bytes under the caller's deadline and cancellation signal.
pub(crate) async fn resolve_bytes_bounded(
    provider: &dyn CredentialProvider,
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
pub trait NetClock: Send + Sync {
    /// Current UTC time.
    fn now(&self) -> DateTime<Utc>;
}

/// Production UTC clock.
#[derive(Debug, Default)]
pub struct SystemNetClock;

impl NetClock for SystemNetClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Cryptographic nonce seam for Digest and WS-Security.
pub trait NonceSource: Send + Sync {
    /// Returns exactly `length` unpredictable bytes.
    fn nonce(&self, length: usize) -> Result<Vec<u8>>;
}

/// Production operating-system nonce source.
#[derive(Debug, Default)]
pub struct SystemNonceSource;

impl NonceSource for SystemNonceSource {
    fn nonce(&self, length: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0_u8; length];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        Ok(bytes)
    }
}

/// DNS resolver seam. Implementations return every selected address, not only the first.
#[async_trait]
pub trait AddressResolver: Send + Sync {
    /// Resolves one normalized host and approved port.
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<IpAddr>>;
}

/// Production resolver using a bounded blocking system lookup.
#[derive(Debug, Default)]
pub struct SystemResolver;

pub(crate) fn blocking_dns_limiter() -> &'static Arc<Semaphore> {
    static LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();
    LIMITER.get_or_init(|| Arc::new(Semaphore::new(MAX_BLOCKING_DNS_LOOKUPS)))
}

#[async_trait]
impl AddressResolver for SystemResolver {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DigestAlgorithm {
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
    pub(crate) realm: String,
    pub(crate) nonce: String,
    pub(crate) opaque: Option<String>,
    pub(crate) algorithm: DigestAlgorithm,
    pub(crate) qop_auth: bool,
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

pub(crate) fn split_auth_parameters(value: &str) -> Result<BTreeMap<String, String>> {
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

pub(crate) fn digest_authorization_for_method(
    challenge: &DigestChallenge,
    credentials: &NetworkCredentials,
    method: &str,
    target: &str,
    nonce_count: u32,
    nonce_source: &dyn NonceSource,
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
    credentials: &NetworkCredentials,
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

