//! Bounded RTSP(S)-over-interleaved-TCP capture and GStreamer frame decode.
//!
//! The control transport is implemented here instead of delegating sockets to
//! `rtspsrc`: the adapter must connect to a policy-pinned IP while retaining the
//! original hostname for RTSPS SNI and certificate verification. Only complete
//! H.264/H.265 access units cross into the native decoder boundary.
#![cfg_attr(not(feature = "rtsp"), allow(dead_code))]

#[cfg(feature = "rtsp")]
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
#[cfg(feature = "rtsp")]
use std::sync::{Arc, Mutex, OnceLock};
#[cfg(feature = "rtsp")]
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
#[cfg(feature = "rtsp")]
use serde_json::Value;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::onvif::{
    OnvifResolver, RtspNetworkAnchor, is_forbidden_network_address, normalize_host_text,
};
use crate::config::AuthenticationMode;
use crate::{CameraError, ErrorCode, Result};

#[cfg(feature = "rtsp")]
use rustls::client::WebPkiServerVerifier;
#[cfg(feature = "rtsp")]
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
#[cfg(feature = "rtsp")]
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
#[cfg(feature = "rtsp")]
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
#[cfg(feature = "rtsp")]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(feature = "rtsp")]
use tokio::net::TcpStream;
#[cfg(feature = "rtsp")]
use tokio::sync::{Semaphore, watch};
#[cfg(feature = "rtsp")]
use tokio::task::JoinHandle;
#[cfg(feature = "rtsp")]
use tokio_rustls::TlsConnector;
#[cfg(feature = "rtsp")]
use zeroize::Zeroizing;

#[cfg(feature = "rtsp")]
use super::onvif::{
    DigestChallenge, OnvifClock, OnvifCredentials, OnvifNonceSource, SecretBytes,
    basic_authorization, digest_authorization_for_method, find_auth_scheme, parse_digest_challenge,
};
#[cfg(feature = "rtsp")]
use crate::config::{RtspSessionPolicy, SecurityConfig};
#[cfg(feature = "rtsp")]
use crate::model::{CaptureFrame, CaptureMode, FrameTimestampQuality, PixelFormat};
#[cfg(feature = "rtsp")]
use gst::prelude::*;
#[cfg(feature = "rtsp")]
use gstreamer as gst;
#[cfg(feature = "rtsp")]
use gstreamer_app::{AppSink, AppSrc};

const RTSP_BACKEND: &str = "onvif-rtsp";
const MAX_RTSP_RESOLVED_ADDRESSES: usize = 64;
const MAX_SDP_BYTES: usize = 1024 * 1024;
const MAX_SDP_LINES: usize = 4096;
const MAX_SDP_LINE_BYTES: usize = 4096;
const MAX_CODEC_BOOTSTRAP_BYTES: usize = 64 * 1024;
const MAX_RTSP_BODY_BYTES: usize = 1024 * 1024;
const MAX_RTSP_SESSION_ID_BYTES: usize = 1024;
const MAX_INTERLEAVED_PACKET_BYTES: usize = u16::MAX as usize;
#[cfg(feature = "rtsp")]
// A fresh RTSP reader can attach just after an IDR. Keep enough timestamp
// accounting for a normal ten-frame GOP while retaining a strict 16-frame
// (and therefore `16 * maximumFrameBytes`) bound.
const MAX_DECODER_PENDING_UNITS: usize = 16;
#[cfg(feature = "rtsp")]
const MAX_BLOCKING_TLS_CONFIGURATIONS: usize = 4;
#[cfg(feature = "rtsp")]
const WARM_SESSION_IDLE: Duration = Duration::from_secs(30);

fn backend_error(message: impl Into<String>) -> CameraError {
    CameraError::Backend {
        backend: RTSP_BACKEND,
        message: message.into(),
    }
}

fn security_error(message: impl Into<String>) -> CameraError {
    backend_error(format!(
        "security policy rejected RTSP data: {}",
        message.into()
    ))
}

fn timeout_error(stage: &'static str) -> CameraError {
    CameraError::rejected(
        ErrorCode::CaptureTimeout,
        format!("RTSP {stage} exceeded its deadline"),
    )
}

fn cancelled_error(stage: &'static str) -> CameraError {
    CameraError::rejected(
        ErrorCode::CaptureCancelled,
        format!("RTSP {stage} was cancelled"),
    )
}

/// A camera-returned RTSP URI after host, tuple, and address-set validation.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PinnedRtspUri {
    url: Url,
    host: String,
    port: u16,
    addresses: BTreeSet<IpAddr>,
    selected_address: IpAddr,
}

impl std::fmt::Debug for PinnedRtspUri {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PinnedRtspUri")
            .field("scheme", &self.url.scheme())
            .field("host", &self.host)
            .field("port", &self.port)
            .field("selected_address", &self.selected_address)
            .field("path_and_query", &"<redacted>")
            .finish()
    }
}

impl PinnedRtspUri {
    #[must_use]
    pub(crate) fn url(&self) -> &Url {
        &self.url
    }

    #[must_use]
    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    #[must_use]
    pub(crate) const fn socket_address(&self) -> SocketAddr {
        SocketAddr::new(self.selected_address, self.port)
    }

    #[must_use]
    pub(crate) fn is_tls(&self) -> bool {
        self.url.scheme() == "rtsps"
    }
}

/// Immutable URI policy for the stream URI belonging to one selected ONVIF profile.
#[derive(Debug, Clone)]
pub(crate) struct RtspUriPolicy {
    url: Url,
    scheme: String,
    host: String,
    port: u16,
    established_addresses: BTreeSet<IpAddr>,
    anchor: RtspNetworkAnchor,
    allow_insecure: bool,
}

impl RtspUriPolicy {
    pub(crate) async fn establish(
        candidate: &str,
        anchor: RtspNetworkAnchor,
        allow_insecure: bool,
        resolver: &dyn OnvifResolver,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(Self, PinnedRtspUri)> {
        let (url, host, port) = parse_rtsp_uri(candidate, allow_insecure)?;
        validate_rtsp_host(&host, &anchor)?;
        let addresses = resolve_rtsp_bounded(resolver, &host, port, deadline, cancellation).await?;
        validate_rtsp_addresses(&host, &addresses, &anchor)?;
        let established_addresses = addresses.iter().copied().collect::<BTreeSet<_>>();
        let selected_address = *addresses
            .first()
            .ok_or_else(|| security_error("stream URI resolved to no addresses"))?;
        let pinned = PinnedRtspUri {
            url: url.clone(),
            host: host.clone(),
            port,
            addresses: established_addresses.clone(),
            selected_address,
        };
        Ok((
            Self {
                url,
                scheme: pinned.url.scheme().to_owned(),
                host,
                port,
                established_addresses,
                anchor,
                allow_insecure,
            },
            pinned,
        ))
    }

    /// Re-resolves and requires the exact established address set before every
    /// new control connection. This catches both mixed answers and rebinding.
    pub(crate) async fn pin(
        &self,
        resolver: &dyn OnvifResolver,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PinnedRtspUri> {
        let (url, host, port) = parse_rtsp_uri(self.url.as_str(), self.allow_insecure)?;
        if url.scheme() != self.scheme || host != self.host || port != self.port {
            return Err(security_error(
                "stream URI changed its approved origin tuple",
            ));
        }
        validate_rtsp_host(&host, &self.anchor)?;
        let addresses = resolve_rtsp_bounded(resolver, &host, port, deadline, cancellation).await?;
        validate_rtsp_addresses(&host, &addresses, &self.anchor)?;
        let current = addresses.iter().copied().collect::<BTreeSet<_>>();
        if current != self.established_addresses {
            return Err(security_error(
                "stream URI DNS address set changed after validation",
            ));
        }
        let selected_address = *addresses
            .first()
            .ok_or_else(|| security_error("stream URI resolved to no addresses"))?;
        Ok(PinnedRtspUri {
            url,
            host,
            port,
            addresses: current,
            selected_address,
        })
    }

    fn validate_control_uri(&self, candidate: &Url) -> Result<()> {
        let (url, host, port) = parse_rtsp_uri(candidate.as_str(), self.allow_insecure)?;
        if url.scheme() != self.scheme || host != self.host || port != self.port {
            return Err(security_error(
                "SDP control URI changed the approved stream origin tuple",
            ));
        }
        Ok(())
    }
}

fn parse_rtsp_uri(candidate: &str, allow_insecure: bool) -> Result<(Url, String, u16)> {
    if candidate.is_empty() || candidate.len() > 8192 || candidate.chars().any(char::is_control) {
        return Err(security_error(
            "stream URI violates length or character bounds",
        ));
    }
    let url = Url::parse(candidate).map_err(|_| security_error("stream URI is invalid"))?;
    if url.cannot_be_a_base() || url.host_str().is_none() {
        return Err(security_error(
            "stream URI must be an absolute hierarchical URI",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(security_error("stream URI user information is forbidden"));
    }
    if url.fragment().is_some() {
        return Err(security_error("stream URI fragments are forbidden"));
    }
    match url.scheme() {
        "rtsps" => {}
        "rtsp" if allow_insecure => {}
        "rtsp" => {
            return Err(security_error(
                "plaintext RTSP requires the explicit allowInsecure gate",
            ));
        }
        _ => return Err(security_error("stream URI scheme must be rtsp or rtsps")),
    }
    let host = normalize_host_text(
        url.host_str()
            .ok_or_else(|| security_error("stream URI omitted its host"))?,
    )?;
    let port = url.port().unwrap_or_else(|| match url.scheme() {
        "rtsps" => 322,
        _ => 554,
    });
    if port == 0 {
        return Err(security_error("stream URI port is invalid"));
    }
    Ok((url, host, port))
}

fn validate_rtsp_host(host: &str, anchor: &RtspNetworkAnchor) -> Result<()> {
    if host != anchor.configured_host && !anchor.allowed_hosts.contains(host) {
        return Err(security_error("stream URI host is not allowlisted"));
    }
    Ok(())
}

fn validate_rtsp_addresses(
    host: &str,
    addresses: &[IpAddr],
    anchor: &RtspNetworkAnchor,
) -> Result<()> {
    if addresses.is_empty() || addresses.len() > MAX_RTSP_RESOLVED_ADDRESSES {
        return Err(security_error(
            "stream URI returned an invalid address count",
        ));
    }
    for address in addresses {
        let configured_endpoint =
            host == anchor.configured_host && anchor.endpoint_addresses.contains(address);
        let cidr_allowed = anchor
            .allowed_cidrs
            .iter()
            .any(|network| network.contains(address));
        if !configured_endpoint && (is_forbidden_network_address(*address) || !cidr_allowed) {
            return Err(security_error(
                "stream URI resolved outside the configured endpoint and allowed CIDRs",
            ));
        }
    }
    Ok(())
}

async fn resolve_rtsp_bounded(
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
    let mut addresses = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(cancelled_error("DNS resolution")),
        _ = tokio::time::sleep_until(deadline) => return Err(timeout_error("DNS resolution")),
        result = &mut resolution => result?,
    };
    addresses.sort_unstable();
    addresses.dedup();
    Ok(addresses)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RtspCodec {
    H264,
    H265,
}

impl RtspCodec {
    fn encoding_name(self) -> &'static str {
        match self {
            Self::H264 => "H264",
            Self::H265 => "H265",
        }
    }
}

#[derive(Debug, Clone)]
struct RtspTrack {
    control_uri: Url,
    codec: RtspCodec,
    payload_type: u8,
    #[cfg_attr(not(test), allow(dead_code))]
    clock_rate: u32,
    bootstrap_nals: Vec<Vec<u8>>,
}

#[derive(Debug, Default)]
struct SdpMedia {
    is_video: bool,
    payload_types: Vec<u8>,
    control: Option<String>,
    rtpmap: BTreeMap<u8, (String, u32)>,
    fmtp: BTreeMap<u8, String>,
}

fn parse_sdp_track(body: &[u8], base_uri: &Url, policy: &RtspUriPolicy) -> Result<RtspTrack> {
    if body.is_empty() || body.len() > MAX_SDP_BYTES {
        return Err(security_error("RTSP SDP violates its byte bound"));
    }
    let text = std::str::from_utf8(body)
        .map_err(|_| security_error("RTSP SDP is not valid UTF-8/ASCII"))?;
    let mut media = Vec::<SdpMedia>::new();
    for (line_index, raw_line) in text.lines().enumerate() {
        if line_index >= MAX_SDP_LINES || raw_line.len() > MAX_SDP_LINE_BYTES {
            return Err(security_error("RTSP SDP exceeds line bounds"));
        }
        let line = raw_line.trim_end_matches('\r');
        if line
            .chars()
            .any(|character| character.is_control() && character != '\t')
        {
            return Err(security_error("RTSP SDP contains control characters"));
        }
        if let Some(rest) = line.strip_prefix("m=") {
            let fields = rest.split_ascii_whitespace().collect::<Vec<_>>();
            if fields.len() < 4 {
                return Err(security_error("RTSP SDP media line is malformed"));
            }
            let payload_types = fields[3..]
                .iter()
                .filter_map(|value| value.parse::<u8>().ok())
                .collect::<Vec<_>>();
            media.push(SdpMedia {
                is_video: fields[0].eq_ignore_ascii_case("video")
                    && fields[2].eq_ignore_ascii_case("RTP/AVP"),
                payload_types,
                ..SdpMedia::default()
            });
            continue;
        }
        let Some(current) = media.last_mut() else {
            continue;
        };
        if let Some(value) = line.strip_prefix("a=control:") {
            if current.control.replace(value.trim().to_owned()).is_some() {
                return Err(security_error(
                    "RTSP SDP has duplicate media control attributes",
                ));
            }
        } else if let Some(value) = line.strip_prefix("a=rtpmap:") {
            let (payload, mapping) = value
                .split_once(char::is_whitespace)
                .ok_or_else(|| security_error("RTSP SDP rtpmap is malformed"))?;
            let payload = payload
                .parse::<u8>()
                .map_err(|_| security_error("RTSP SDP rtpmap payload is invalid"))?;
            let mut mapping = mapping.trim().split('/');
            let encoding = mapping
                .next()
                .ok_or_else(|| security_error("RTSP SDP rtpmap omitted its encoding"))?;
            let clock_rate = mapping
                .next()
                .ok_or_else(|| security_error("RTSP SDP rtpmap omitted its clock rate"))?
                .parse::<u32>()
                .map_err(|_| security_error("RTSP SDP rtpmap clock rate is invalid"))?;
            if mapping.next().is_some() {
                return Err(security_error("RTSP video rtpmap has unexpected channels"));
            }
            if current
                .rtpmap
                .insert(payload, (encoding.to_ascii_uppercase(), clock_rate))
                .is_some()
            {
                return Err(security_error("RTSP SDP has duplicate rtpmap payloads"));
            }
        } else if let Some(value) = line.strip_prefix("a=fmtp:") {
            let (payload, parameters) = value
                .split_once(char::is_whitespace)
                .ok_or_else(|| security_error("RTSP SDP fmtp is malformed"))?;
            let payload = payload
                .parse::<u8>()
                .map_err(|_| security_error("RTSP SDP fmtp payload is invalid"))?;
            if current
                .fmtp
                .insert(payload, parameters.trim().to_owned())
                .is_some()
            {
                return Err(security_error("RTSP SDP has duplicate fmtp payloads"));
            }
        }
    }

    for candidate in media.iter().filter(|candidate| candidate.is_video) {
        for payload_type in &candidate.payload_types {
            let Some((encoding, clock_rate)) = candidate.rtpmap.get(payload_type) else {
                continue;
            };
            let codec = match encoding.as_str() {
                "H264" => RtspCodec::H264,
                "H265" | "HEVC" => RtspCodec::H265,
                _ => continue,
            };
            if *clock_rate != 90_000 {
                return Err(CameraError::rejected(
                    ErrorCode::UnsupportedCapability,
                    "RTSP H.264/H.265 video must use a 90000 Hz RTP clock",
                ));
            }
            let control = candidate
                .control
                .as_deref()
                .ok_or_else(|| security_error("RTSP SDP video track omitted control URI"))?;
            let control_uri = base_uri
                .join(control)
                .map_err(|_| security_error("RTSP SDP control URI is invalid"))?;
            policy.validate_control_uri(&control_uri)?;
            let fmtp = candidate.fmtp.get(payload_type).map(String::as_str);
            let bootstrap_nals = parse_codec_fmtp(codec, fmtp)?;
            return Ok(RtspTrack {
                control_uri,
                codec,
                payload_type: *payload_type,
                clock_rate: *clock_rate,
                bootstrap_nals,
            });
        }
    }
    Err(CameraError::rejected(
        ErrorCode::UnsupportedCapability,
        "RTSP SDP has no supported H.264 or H.265 video track",
    ))
}

fn parse_codec_fmtp(codec: RtspCodec, fmtp: Option<&str>) -> Result<Vec<Vec<u8>>> {
    let mut parameters = BTreeMap::<String, String>::new();
    if let Some(fmtp) = fmtp {
        if fmtp.len() > MAX_SDP_LINE_BYTES {
            return Err(security_error("RTSP codec parameters are oversized"));
        }
        for field in fmtp.split(';') {
            let field = field.trim();
            if field.is_empty() {
                continue;
            }
            let (name, value) = field
                .split_once('=')
                .ok_or_else(|| security_error("RTSP codec parameter is malformed"))?;
            let name = name.trim().to_ascii_lowercase();
            if parameters.insert(name, value.trim().to_owned()).is_some() {
                return Err(security_error("RTSP codec parameter is duplicated"));
            }
        }
    }
    match codec {
        RtspCodec::H264 => {
            if parameters
                .get("packetization-mode")
                .is_some_and(|mode| mode != "0" && mode != "1")
            {
                return Err(CameraError::rejected(
                    ErrorCode::UnsupportedCapability,
                    "RTSP H.264 interleaved packetization mode is unsupported",
                ));
            }
        }
        RtspCodec::H265 => {
            if parameters
                .get("sprop-max-don-diff")
                .is_some_and(|value| value != "0")
            {
                return Err(CameraError::rejected(
                    ErrorCode::UnsupportedCapability,
                    "RTSP H.265 decoding-order-number packetization is unsupported",
                ));
            }
        }
    }
    let names: &[&str] = match codec {
        RtspCodec::H264 => &["sprop-parameter-sets"],
        RtspCodec::H265 => &["sprop-vps", "sprop-sps", "sprop-pps"],
    };
    let mut total = 0_usize;
    let mut nals = Vec::new();
    for name in names {
        let Some(value) = parameters.get(*name) else {
            continue;
        };
        for encoded in value.split(',') {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded.trim())
                .map_err(|_| security_error("RTSP SDP contains invalid base64 codec data"))?;
            if bytes.is_empty() {
                return Err(security_error("RTSP SDP contains an empty codec NAL unit"));
            }
            total = total
                .checked_add(bytes.len())
                .ok_or_else(|| security_error("RTSP codec bootstrap size overflowed"))?;
            if total > MAX_CODEC_BOOTSTRAP_BYTES {
                return Err(security_error("RTSP codec bootstrap data is oversized"));
            }
            nals.push(bytes);
        }
    }
    Ok(nals)
}

#[derive(Debug, Clone)]
struct RtspResponse {
    status: u16,
    headers: BTreeMap<String, Vec<String>>,
    body: Vec<u8>,
}

impl RtspResponse {
    fn single_header(&self, name: &'static str) -> Result<Option<&str>> {
        match self.headers.get(name) {
            None => Ok(None),
            Some(values) if values.len() == 1 => Ok(values.first().map(String::as_str)),
            Some(_) => Err(security_error(format!(
                "RTSP response has duplicate {name} headers"
            ))),
        }
    }

    fn authentication_challenges(&self) -> Option<String> {
        self.headers
            .get("www-authenticate")
            .map(|values| values.join(", "))
    }
}

fn parse_rtsp_response_head(
    head: &[u8],
    maximum_header_bytes: usize,
) -> Result<(u16, BTreeMap<String, Vec<String>>, usize)> {
    if head.is_empty() || head.len() > maximum_header_bytes || !head.ends_with(b"\r\n\r\n") {
        return Err(security_error(
            "RTSP response headers violate their byte bound",
        ));
    }
    if !head.is_ascii() {
        return Err(security_error("RTSP response headers are not ASCII"));
    }
    let text = std::str::from_utf8(head)
        .map_err(|_| security_error("RTSP response headers are not valid ASCII"))?;
    let mut lines = text[..text.len() - 4].split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| security_error("RTSP response omitted its status line"))?;
    if !status_line
        .bytes()
        .all(|byte| byte == b' ' || byte.is_ascii_graphic())
    {
        return Err(security_error(
            "RTSP response status line contains controls",
        ));
    }
    let mut status_fields = status_line.splitn(3, ' ');
    if status_fields.next() != Some("RTSP/1.0") {
        return Err(security_error("RTSP response version is unsupported"));
    }
    let status = status_fields
        .next()
        .ok_or_else(|| security_error("RTSP response omitted its status"))?
        .parse::<u16>()
        .map_err(|_| security_error("RTSP response status is invalid"))?;
    if !(100..=599).contains(&status) {
        return Err(security_error(
            "RTSP response status is outside protocol bounds",
        ));
    }
    let mut headers = BTreeMap::<String, Vec<String>>::new();
    for line in lines {
        if line.is_empty() || line.starts_with([' ', '\t']) {
            return Err(security_error(
                "RTSP response contains malformed header folding",
            ));
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| security_error("RTSP response contains a malformed header"))?;
        let name = name.trim().to_ascii_lowercase();
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(security_error("RTSP response header name is invalid"));
        }
        let value = value.trim();
        if value.chars().any(char::is_control) {
            return Err(security_error(
                "RTSP response header value contains controls",
            ));
        }
        headers.entry(name).or_default().push(value.to_owned());
    }
    if headers.contains_key("transfer-encoding") || headers.contains_key("content-encoding") {
        return Err(security_error(
            "encoded or chunked RTSP bodies are unsupported",
        ));
    }
    let content_length = match headers.get("content-length") {
        None => 0,
        Some(values) if values.len() == 1 => values[0]
            .parse::<usize>()
            .map_err(|_| security_error("RTSP Content-Length is invalid"))?,
        Some(_) => return Err(security_error("RTSP response has duplicate Content-Length")),
    };
    if content_length > MAX_RTSP_BODY_BYTES {
        return Err(security_error("RTSP response body is oversized"));
    }
    Ok((status, headers, content_length))
}

#[derive(Debug, Clone, Copy)]
struct RtpPacket<'a> {
    marker: bool,
    payload_type: u8,
    sequence: u16,
    timestamp: u32,
    payload: &'a [u8],
}

fn parse_rtp_packet(bytes: &[u8]) -> Result<RtpPacket<'_>> {
    if bytes.len() < 12 || bytes.len() > MAX_INTERLEAVED_PACKET_BYTES {
        return Err(security_error(
            "interleaved RTP packet violates byte bounds",
        ));
    }
    if bytes[0] >> 6 != 2 {
        return Err(security_error(
            "interleaved RTP packet has an invalid version",
        ));
    }
    let padding = bytes[0] & 0x20 != 0;
    let extension = bytes[0] & 0x10 != 0;
    let csrc_count = usize::from(bytes[0] & 0x0f);
    let mut offset = 12_usize
        .checked_add(csrc_count.saturating_mul(4))
        .ok_or_else(|| security_error("RTP header size overflowed"))?;
    if offset > bytes.len() {
        return Err(security_error("RTP CSRC list is truncated"));
    }
    if extension {
        if offset + 4 > bytes.len() {
            return Err(security_error("RTP extension header is truncated"));
        }
        let words = usize::from(u16::from_be_bytes([bytes[offset + 2], bytes[offset + 3]]));
        offset = offset
            .checked_add(4)
            .and_then(|value| value.checked_add(words.saturating_mul(4)))
            .ok_or_else(|| security_error("RTP extension size overflowed"))?;
        if offset > bytes.len() {
            return Err(security_error("RTP extension body is truncated"));
        }
    }
    let mut end = bytes.len();
    if padding {
        let count = usize::from(
            bytes
                .last()
                .copied()
                .ok_or_else(|| security_error("RTP padding marker is missing"))?,
        );
        if count == 0 || count > end.saturating_sub(offset) {
            return Err(security_error("RTP padding length is invalid"));
        }
        end -= count;
    }
    if offset >= end {
        return Err(security_error("RTP packet has no media payload"));
    }
    Ok(RtpPacket {
        marker: bytes[1] & 0x80 != 0,
        payload_type: bytes[1] & 0x7f,
        sequence: u16::from_be_bytes([bytes[2], bytes[3]]),
        timestamp: u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        payload: &bytes[offset..end],
    })
}

#[derive(Debug)]
struct EncodedAccessUnit {
    bytes: Bytes,
    rtp_timestamp: u32,
    compressed_bytes: u64,
    dimensions: Option<(u32, u32)>,
}

#[derive(Debug)]
struct FragmentState {
    timestamp: u32,
    expected_sequence: u16,
    nal_offset: usize,
}

#[derive(Debug)]
struct AccessUnitAssembler {
    codec: RtspCodec,
    payload_type: u8,
    maximum_bytes: u64,
    timestamp: Option<u32>,
    expected_sequence: Option<u16>,
    fragment: Option<FragmentState>,
    bytes: Vec<u8>,
    has_vcl: bool,
    bootstrap_nals: Vec<Vec<u8>>,
    bootstrap_pending: bool,
    bootstrap_injected_for_unit: bool,
    dimensions: Option<(u32, u32)>,
    incomplete_units: u64,
}

impl AccessUnitAssembler {
    fn new(track: &RtspTrack, maximum_bytes: u64) -> Result<Self> {
        if maximum_bytes == 0 {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "RTSP compressed-frame bound must be positive",
            ));
        }
        Ok(Self {
            codec: track.codec,
            payload_type: track.payload_type,
            maximum_bytes,
            timestamp: None,
            expected_sequence: None,
            fragment: None,
            bytes: Vec::new(),
            has_vcl: false,
            bootstrap_nals: track.bootstrap_nals.clone(),
            bootstrap_pending: !track.bootstrap_nals.is_empty(),
            bootstrap_injected_for_unit: false,
            dimensions: None,
            incomplete_units: 0,
        })
    }

    fn push(&mut self, packet: RtpPacket<'_>) -> Result<Option<EncodedAccessUnit>> {
        if packet.payload_type != self.payload_type {
            return Ok(None);
        }
        if self
            .expected_sequence
            .is_some_and(|expected| expected != packet.sequence)
        {
            self.discard_incomplete();
        }
        if self
            .timestamp
            .is_some_and(|timestamp| timestamp != packet.timestamp)
        {
            self.discard_incomplete();
        }
        self.timestamp.get_or_insert(packet.timestamp);
        self.expected_sequence = Some(packet.sequence.wrapping_add(1));
        match self.codec {
            RtspCodec::H264 => self.push_h264(packet)?,
            RtspCodec::H265 => self.push_h265(packet)?,
        }
        if !packet.marker {
            return Ok(None);
        }
        if self.fragment.is_some() || !self.has_vcl || self.bytes.is_empty() {
            self.discard_incomplete();
            return Ok(None);
        }
        let timestamp = self
            .timestamp
            .take()
            .ok_or_else(|| security_error("complete RTP access unit has no timestamp"))?;
        self.expected_sequence = None;
        self.has_vcl = false;
        if self.bootstrap_injected_for_unit {
            self.bootstrap_pending = false;
            self.bootstrap_nals.clear();
            self.bootstrap_injected_for_unit = false;
        }
        let bytes = std::mem::take(&mut self.bytes);
        let compressed_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        Ok(Some(EncodedAccessUnit {
            bytes: Bytes::from(bytes),
            rtp_timestamp: timestamp,
            compressed_bytes,
            dimensions: self.dimensions,
        }))
    }

    fn push_h264(&mut self, packet: RtpPacket<'_>) -> Result<()> {
        let payload = packet.payload;
        let nal_type = payload[0] & 0x1f;
        match nal_type {
            1..=23 => self.append_nal(payload, (1..=5).contains(&nal_type)),
            24 => {
                let mut remainder = &payload[1..];
                while !remainder.is_empty() {
                    if remainder.len() < 2 {
                        return Err(security_error("H.264 STAP-A length is truncated"));
                    }
                    let length = usize::from(u16::from_be_bytes([remainder[0], remainder[1]]));
                    remainder = &remainder[2..];
                    if length == 0 || length > remainder.len() {
                        return Err(security_error("H.264 STAP-A NAL length is invalid"));
                    }
                    let nal = &remainder[..length];
                    let kind = nal[0] & 0x1f;
                    self.append_nal(nal, (1..=5).contains(&kind))?;
                    remainder = &remainder[length..];
                }
                Ok(())
            }
            28 => {
                if payload.len() < 3 {
                    return Err(security_error("H.264 FU-A payload is truncated"));
                }
                let start = payload[1] & 0x80 != 0;
                let end = payload[1] & 0x40 != 0;
                if start && end {
                    return Err(security_error("H.264 FU-A cannot start and end together"));
                }
                let reconstructed = (payload[0] & 0xe0) | (payload[1] & 0x1f);
                if start {
                    if self.fragment.is_some() {
                        return Err(security_error("H.264 FU-A fragments overlap"));
                    }
                    let vcl = (1..=5).contains(&(reconstructed & 0x1f));
                    self.inject_bootstrap_before_vcl(vcl)?;
                    self.append_start_code()?;
                    let nal_offset = self.bytes.len();
                    self.append_bytes(&[reconstructed])?;
                    self.append_bytes(&payload[2..])?;
                    self.has_vcl |= vcl;
                    self.fragment = Some(FragmentState {
                        timestamp: packet.timestamp,
                        expected_sequence: packet.sequence.wrapping_add(1),
                        nal_offset,
                    });
                } else {
                    let state = self
                        .fragment
                        .as_mut()
                        .ok_or_else(|| security_error("H.264 FU-A continuation lacks start"))?;
                    if state.timestamp != packet.timestamp
                        || state.expected_sequence != packet.sequence
                    {
                        return Err(security_error("H.264 FU-A sequence is discontinuous"));
                    }
                    state.expected_sequence = packet.sequence.wrapping_add(1);
                    self.append_bytes(&payload[2..])?;
                    if end {
                        let state = self.fragment.take().ok_or_else(|| {
                            security_error("H.264 FU-A end lacks active fragment state")
                        })?;
                        let nal = self.bytes[state.nal_offset..].to_vec();
                        self.observe_nal(&nal)?;
                    }
                }
                Ok(())
            }
            _ => Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "RTSP stream uses an unsupported H.264 RTP packetization type",
            )),
        }
    }

    fn push_h265(&mut self, packet: RtpPacket<'_>) -> Result<()> {
        let payload = packet.payload;
        if payload.len() < 2 {
            return Err(security_error("H.265 RTP payload is truncated"));
        }
        let nal_type = (payload[0] >> 1) & 0x3f;
        match nal_type {
            0..=47 => self.append_nal(payload, nal_type <= 31),
            48 => {
                let mut remainder = &payload[2..];
                while !remainder.is_empty() {
                    if remainder.len() < 2 {
                        return Err(security_error("H.265 AP length is truncated"));
                    }
                    let length = usize::from(u16::from_be_bytes([remainder[0], remainder[1]]));
                    remainder = &remainder[2..];
                    if length < 2 || length > remainder.len() {
                        return Err(security_error("H.265 AP NAL length is invalid"));
                    }
                    let nal = &remainder[..length];
                    let kind = (nal[0] >> 1) & 0x3f;
                    self.append_nal(nal, kind <= 31)?;
                    remainder = &remainder[length..];
                }
                Ok(())
            }
            49 => {
                if payload.len() < 4 {
                    return Err(security_error("H.265 FU payload is truncated"));
                }
                let start = payload[2] & 0x80 != 0;
                let end = payload[2] & 0x40 != 0;
                if start && end {
                    return Err(security_error("H.265 FU cannot start and end together"));
                }
                let reconstructed_type = payload[2] & 0x3f;
                let reconstructed = [(payload[0] & 0x81) | (reconstructed_type << 1), payload[1]];
                if start {
                    if self.fragment.is_some() {
                        return Err(security_error("H.265 FU fragments overlap"));
                    }
                    let vcl = reconstructed_type <= 31;
                    self.inject_bootstrap_before_vcl(vcl)?;
                    self.append_start_code()?;
                    let nal_offset = self.bytes.len();
                    self.append_bytes(&reconstructed)?;
                    self.append_bytes(&payload[3..])?;
                    self.has_vcl |= vcl;
                    self.fragment = Some(FragmentState {
                        timestamp: packet.timestamp,
                        expected_sequence: packet.sequence.wrapping_add(1),
                        nal_offset,
                    });
                } else {
                    let state = self
                        .fragment
                        .as_mut()
                        .ok_or_else(|| security_error("H.265 FU continuation lacks start"))?;
                    if state.timestamp != packet.timestamp
                        || state.expected_sequence != packet.sequence
                    {
                        return Err(security_error("H.265 FU sequence is discontinuous"));
                    }
                    state.expected_sequence = packet.sequence.wrapping_add(1);
                    self.append_bytes(&payload[3..])?;
                    if end {
                        let state = self.fragment.take().ok_or_else(|| {
                            security_error("H.265 FU end lacks active fragment state")
                        })?;
                        let nal = self.bytes[state.nal_offset..].to_vec();
                        self.observe_nal(&nal)?;
                    }
                }
                Ok(())
            }
            _ => Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "RTSP stream uses an unsupported H.265 RTP packetization type",
            )),
        }
    }

    fn append_nal(&mut self, nal: &[u8], vcl: bool) -> Result<()> {
        self.inject_bootstrap_before_vcl(vcl)?;
        self.observe_nal(nal)?;
        self.append_start_code()?;
        self.append_bytes(nal)?;
        self.has_vcl |= vcl;
        Ok(())
    }

    fn inject_bootstrap_before_vcl(&mut self, vcl: bool) -> Result<()> {
        if !vcl || !self.bootstrap_pending || self.bootstrap_injected_for_unit {
            return Ok(());
        }
        // Retain the bounded bootstrap until this complete VCL access unit is
        // emitted. If the network drops a fragment, discard_incomplete() keeps
        // the bootstrap available for the next decodable access unit.
        let bootstrap = self.bootstrap_nals.clone();
        for bootstrap_nal in bootstrap {
            self.observe_nal(&bootstrap_nal)?;
            self.append_start_code()?;
            self.append_bytes(&bootstrap_nal)?;
        }
        self.bootstrap_injected_for_unit = true;
        Ok(())
    }

    fn observe_nal(&mut self, nal: &[u8]) -> Result<()> {
        let dimensions = match self.codec {
            RtspCodec::H264 if nal.first().is_some_and(|byte| byte & 0x1f == 7) => {
                Some(parse_h264_sps_dimensions(nal)?)
            }
            RtspCodec::H265 if nal.len() >= 2 && ((nal[0] >> 1) & 0x3f) == 33 => {
                Some(parse_h265_sps_dimensions(nal)?)
            }
            _ => None,
        };
        if let Some(dimensions) = dimensions {
            self.dimensions = Some(dimensions);
        }
        Ok(())
    }

    fn append_start_code(&mut self) -> Result<()> {
        self.append_bytes(&[0, 0, 0, 1])
    }

    fn append_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| security_error("RTSP access-unit size overflowed"))?;
        if u64::try_from(next).unwrap_or(u64::MAX) > self.maximum_bytes {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "RTSP compressed access unit exceeds maximumFrameBytes",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn discard_incomplete(&mut self) {
        if !self.bytes.is_empty() || self.fragment.is_some() {
            self.incomplete_units = self.incomplete_units.saturating_add(1);
        }
        self.timestamp = None;
        self.expected_sequence = None;
        self.fragment = None;
        self.bytes.clear();
        self.has_vcl = false;
        self.bootstrap_injected_for_unit = false;
    }
}

fn read_only_success_establishes_authentication(
    mode: AuthenticationMode,
    authentication_selected: bool,
) -> bool {
    mode != AuthenticationMode::HttpDigest || authentication_selected
}

#[derive(Debug)]
struct RbspBitReader {
    bytes: Vec<u8>,
    bit_offset: usize,
}

impl RbspBitReader {
    fn from_escaped(bytes: &[u8]) -> Result<Self> {
        let mut rbsp = Vec::with_capacity(bytes.len());
        let mut zero_count = 0_u8;
        for byte in bytes {
            if zero_count >= 2 && *byte == 0x03 {
                zero_count = 0;
                continue;
            }
            rbsp.push(*byte);
            zero_count = if *byte == 0 {
                zero_count.saturating_add(1)
            } else {
                0
            };
        }
        if rbsp.is_empty() {
            return Err(security_error("codec parameter set is empty"));
        }
        Ok(Self {
            bytes: rbsp,
            bit_offset: 0,
        })
    }

    fn bit(&mut self) -> Result<bool> {
        let byte = self
            .bytes
            .get(self.bit_offset / 8)
            .copied()
            .ok_or_else(|| security_error("codec parameter set is truncated"))?;
        let shift = 7 - (self.bit_offset % 8);
        self.bit_offset = self.bit_offset.saturating_add(1);
        Ok(byte & (1 << shift) != 0)
    }

    fn bits(&mut self, count: usize) -> Result<u64> {
        if count > 64 {
            return Err(security_error("codec bit-field width is invalid"));
        }
        let mut value = 0_u64;
        for _ in 0..count {
            value = (value << 1) | u64::from(self.bit()?);
        }
        Ok(value)
    }

    fn unsigned_exp_golomb(&mut self) -> Result<u32> {
        let mut leading_zeroes = 0_usize;
        while !self.bit()? {
            leading_zeroes = leading_zeroes.saturating_add(1);
            if leading_zeroes > 31 {
                return Err(security_error("codec Exp-Golomb value is oversized"));
            }
        }
        let suffix = self.bits(leading_zeroes)?;
        let base = (1_u64 << leading_zeroes).saturating_sub(1);
        u32::try_from(base.saturating_add(suffix))
            .map_err(|_| security_error("codec Exp-Golomb value overflowed"))
    }

    fn signed_exp_golomb(&mut self) -> Result<i32> {
        let value = self.unsigned_exp_golomb()?;
        let magnitude = i32::try_from(value.div_ceil(2))
            .map_err(|_| security_error("signed codec value overflowed"))?;
        Ok(if value % 2 == 0 {
            -magnitude
        } else {
            magnitude
        })
    }

    fn skip(&mut self, count: usize) -> Result<()> {
        let end = self
            .bit_offset
            .checked_add(count)
            .ok_or_else(|| security_error("codec bit offset overflowed"))?;
        if end > self.bytes.len().saturating_mul(8) {
            return Err(security_error("codec parameter set is truncated"));
        }
        self.bit_offset = end;
        Ok(())
    }
}

fn parse_h264_sps_dimensions(nal: &[u8]) -> Result<(u32, u32)> {
    if nal.len() < 4 || nal[0] & 0x1f != 7 {
        return Err(security_error("H.264 SPS NAL is invalid"));
    }
    let mut bits = RbspBitReader::from_escaped(&nal[1..])?;
    let profile_idc = bits.bits(8)? as u8;
    bits.skip(16)?;
    let _sps_id = bits.unsigned_exp_golomb()?;
    let mut chroma_format_idc = 1_u32;
    let mut separate_colour_plane = false;
    if matches!(
        profile_idc,
        100 | 110 | 122 | 244 | 44 | 83 | 86 | 118 | 128 | 138 | 139 | 134 | 135
    ) {
        chroma_format_idc = bits.unsigned_exp_golomb()?;
        if chroma_format_idc > 3 {
            return Err(security_error("H.264 SPS chroma format is invalid"));
        }
        if chroma_format_idc == 3 {
            separate_colour_plane = bits.bit()?;
        }
        if bits.unsigned_exp_golomb()? > 6 || bits.unsigned_exp_golomb()? > 6 {
            return Err(security_error("H.264 SPS bit depth is unsupported"));
        }
        let _qpprime_y_zero_transform_bypass = bits.bit()?;
        if bits.bit()? {
            let scaling_count = if chroma_format_idc == 3 { 12 } else { 8 };
            for index in 0..scaling_count {
                if bits.bit()? {
                    skip_h264_scaling_list(&mut bits, if index < 6 { 16 } else { 64 })?;
                }
            }
        }
    }
    if bits.unsigned_exp_golomb()? > 12 {
        return Err(security_error("H.264 SPS frame-number width is invalid"));
    }
    let pic_order_cnt_type = bits.unsigned_exp_golomb()?;
    match pic_order_cnt_type {
        0 => {
            if bits.unsigned_exp_golomb()? > 12 {
                return Err(security_error("H.264 SPS POC width is invalid"));
            }
        }
        1 => {
            let _delta_always_zero = bits.bit()?;
            let _offset_non_ref = bits.signed_exp_golomb()?;
            let _offset_top_bottom = bits.signed_exp_golomb()?;
            let cycle = bits.unsigned_exp_golomb()?;
            if cycle > 256 {
                return Err(security_error("H.264 SPS POC cycle is oversized"));
            }
            for _ in 0..cycle {
                let _offset = bits.signed_exp_golomb()?;
            }
        }
        2 => {}
        _ => return Err(security_error("H.264 SPS POC type is invalid")),
    }
    let _max_reference_frames = bits.unsigned_exp_golomb()?;
    let _gaps_allowed = bits.bit()?;
    let width_mbs = bits
        .unsigned_exp_golomb()?
        .checked_add(1)
        .ok_or_else(|| security_error("H.264 SPS width overflowed"))?;
    let height_map_units = bits
        .unsigned_exp_golomb()?
        .checked_add(1)
        .ok_or_else(|| security_error("H.264 SPS height overflowed"))?;
    let frame_mbs_only = bits.bit()?;
    if !frame_mbs_only {
        let _mb_adaptive_frame_field = bits.bit()?;
    }
    let _direct_8x8_inference = bits.bit()?;
    let (crop_left, crop_right, crop_top, crop_bottom) = if bits.bit()? {
        (
            bits.unsigned_exp_golomb()?,
            bits.unsigned_exp_golomb()?,
            bits.unsigned_exp_golomb()?,
            bits.unsigned_exp_golomb()?,
        )
    } else {
        (0, 0, 0, 0)
    };
    let chroma_array_type = if separate_colour_plane {
        0
    } else {
        chroma_format_idc
    };
    let (sub_width, sub_height) = match chroma_array_type {
        0 => (1_u32, 1_u32),
        1 => (2, 2),
        2 => (2, 1),
        3 => (1, 1),
        _ => return Err(security_error("H.264 SPS chroma array type is invalid")),
    };
    let frame_factor = if frame_mbs_only { 1_u32 } else { 2 };
    let crop_unit_x = sub_width;
    let crop_unit_y = sub_height
        .checked_mul(frame_factor)
        .ok_or_else(|| security_error("H.264 SPS crop unit overflowed"))?;
    let width = width_mbs
        .checked_mul(16)
        .and_then(|value| {
            crop_left
                .checked_add(crop_right)
                .and_then(|crop| crop.checked_mul(crop_unit_x))
                .and_then(|crop| value.checked_sub(crop))
        })
        .ok_or_else(|| security_error("H.264 SPS cropped width is invalid"))?;
    let height = height_map_units
        .checked_mul(16)
        .and_then(|value| value.checked_mul(frame_factor))
        .and_then(|value| {
            crop_top
                .checked_add(crop_bottom)
                .and_then(|crop| crop.checked_mul(crop_unit_y))
                .and_then(|crop| value.checked_sub(crop))
        })
        .ok_or_else(|| security_error("H.264 SPS cropped height is invalid"))?;
    validate_dimensions(width, height)
}

fn skip_h264_scaling_list(bits: &mut RbspBitReader, size: usize) -> Result<()> {
    let mut last_scale = 8_i32;
    let mut next_scale = 8_i32;
    for _ in 0..size {
        if next_scale != 0 {
            let delta = bits.signed_exp_golomb()?;
            next_scale = (last_scale + delta + 256) % 256;
        }
        if next_scale != 0 {
            last_scale = next_scale;
        }
    }
    Ok(())
}

fn parse_h265_sps_dimensions(nal: &[u8]) -> Result<(u32, u32)> {
    if nal.len() < 5 || ((nal[0] >> 1) & 0x3f) != 33 {
        return Err(security_error("H.265 SPS NAL is invalid"));
    }
    let mut bits = RbspBitReader::from_escaped(&nal[2..])?;
    bits.skip(4)?;
    let max_sub_layers_minus1 = bits.bits(3)? as usize;
    if max_sub_layers_minus1 > 6 {
        return Err(security_error("H.265 SPS sub-layer count is invalid"));
    }
    let _temporal_id_nesting = bits.bit()?;
    skip_h265_profile_tier_level(&mut bits, max_sub_layers_minus1)?;
    let _sps_id = bits.unsigned_exp_golomb()?;
    let chroma_format_idc = bits.unsigned_exp_golomb()?;
    if chroma_format_idc > 3 {
        return Err(security_error("H.265 SPS chroma format is invalid"));
    }
    let separate_colour_plane = chroma_format_idc == 3 && bits.bit()?;
    let coded_width = bits.unsigned_exp_golomb()?;
    let coded_height = bits.unsigned_exp_golomb()?;
    let (left, right, top, bottom) = if bits.bit()? {
        (
            bits.unsigned_exp_golomb()?,
            bits.unsigned_exp_golomb()?,
            bits.unsigned_exp_golomb()?,
            bits.unsigned_exp_golomb()?,
        )
    } else {
        (0, 0, 0, 0)
    };
    let chroma_array_type = if separate_colour_plane {
        0
    } else {
        chroma_format_idc
    };
    let (sub_width, sub_height) = match chroma_array_type {
        0 => (1_u32, 1_u32),
        1 => (2, 2),
        2 => (2, 1),
        3 => (1, 1),
        _ => return Err(security_error("H.265 SPS chroma array type is invalid")),
    };
    let width = left
        .checked_add(right)
        .and_then(|crop| crop.checked_mul(sub_width))
        .and_then(|crop| coded_width.checked_sub(crop))
        .ok_or_else(|| security_error("H.265 SPS cropped width is invalid"))?;
    let height = top
        .checked_add(bottom)
        .and_then(|crop| crop.checked_mul(sub_height))
        .and_then(|crop| coded_height.checked_sub(crop))
        .ok_or_else(|| security_error("H.265 SPS cropped height is invalid"))?;
    validate_dimensions(width, height)
}

fn skip_h265_profile_tier_level(
    bits: &mut RbspBitReader,
    max_sub_layers_minus1: usize,
) -> Result<()> {
    bits.skip(96)?;
    let mut profile_present = [false; 7];
    let mut level_present = [false; 7];
    for index in 0..max_sub_layers_minus1 {
        profile_present[index] = bits.bit()?;
        level_present[index] = bits.bit()?;
    }
    if max_sub_layers_minus1 > 0 {
        bits.skip((8 - max_sub_layers_minus1) * 2)?;
    }
    for index in 0..max_sub_layers_minus1 {
        if profile_present[index] {
            bits.skip(88)?;
        }
        if level_present[index] {
            bits.skip(8)?;
        }
    }
    Ok(())
}

fn validate_dimensions(width: u32, height: u32) -> Result<(u32, u32)> {
    if width == 0 || height == 0 || width > 65_535 || height > 65_535 {
        return Err(security_error("codec dimensions violate hard bounds"));
    }
    Ok((width, height))
}

#[cfg(feature = "rtsp")]
#[derive(Debug, Clone)]
struct PendingDecoderUnit {
    rtp_timestamp: u32,
    compressed_bytes: u64,
    dimensions: (u32, u32),
    ingested_at: Instant,
}

#[cfg(feature = "rtsp")]
#[derive(Debug, Clone)]
struct DecodedRtspFrame {
    bytes: Bytes,
    width: u32,
    height: u32,
    rtp_timestamp: u32,
    compressed_bytes: u64,
    decoder_pts: u64,
    ingested_at: Instant,
}

#[cfg(feature = "rtsp")]
struct GstreamerDecoder {
    pipeline: gst::Pipeline,
    source: AppSrc,
    sink: AppSink,
    codec: RtspCodec,
    next_pts: u64,
    pending: BTreeMap<u64, PendingDecoderUnit>,
    recently_terminal_pts: VecDeque<u64>,
}

#[cfg(feature = "rtsp")]
impl std::fmt::Debug for GstreamerDecoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GstreamerDecoder")
            .field("codec", &self.codec)
            .field("next_pts", &self.next_pts)
            .field("pending_units", &self.pending.len())
            .finish()
    }
}

#[cfg(feature = "rtsp")]
impl GstreamerDecoder {
    fn new(codec: RtspCodec, maximum_bytes: u64) -> Result<Self> {
        gst::init().map_err(|_| backend_error("GStreamer initialization failed"))?;
        if maximum_bytes == 0 {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "GStreamer frame bound must be positive",
            ));
        }
        let codec_pipeline = match codec {
            RtspCodec::H264 => "h264parse disable-passthrough=true ! avdec_h264",
            RtspCodec::H265 => "h265parse disable-passthrough=true ! avdec_h265",
        };
        // The only interpolated field is a validated integer bound. No URI, profile,
        // credential, or camera-controlled text reaches the parser.
        let description = format!(
            "appsrc name=source is-live=true block=false format=time max-bytes={maximum_bytes} leaky-type=downstream ! {codec_pipeline} ! videoconvert ! capsfilter caps=\"video/x-raw,format=RGB\" ! appsink name=sink sync=false max-buffers=1 drop=true"
        );
        let pipeline = gst::parse::launch(&description)
            .map_err(|_| backend_error("GStreamer decoder pipeline construction failed"))?
            .downcast::<gst::Pipeline>()
            .map_err(|_| backend_error("GStreamer decoder did not create a pipeline"))?;
        let source = pipeline
            .by_name("source")
            .ok_or_else(|| backend_error("GStreamer decoder omitted appsrc"))?
            .downcast::<AppSrc>()
            .map_err(|_| backend_error("GStreamer decoder source has the wrong type"))?;
        let sink = pipeline
            .by_name("sink")
            .ok_or_else(|| backend_error("GStreamer decoder omitted appsink"))?
            .downcast::<AppSink>()
            .map_err(|_| backend_error("GStreamer decoder sink has the wrong type"))?;
        let source_caps = match codec {
            RtspCodec::H264 => gst::Caps::builder("video/x-h264")
                .field("stream-format", "byte-stream")
                .field("alignment", "au")
                .build(),
            RtspCodec::H265 => gst::Caps::builder("video/x-h265")
                .field("stream-format", "byte-stream")
                .field("alignment", "au")
                .build(),
        };
        source.set_caps(Some(&source_caps));
        pipeline
            .set_state(gst::State::Playing)
            .map_err(|_| backend_error("GStreamer decoder failed to enter PLAYING"))?;
        Ok(Self {
            pipeline,
            source,
            sink,
            codec,
            next_pts: 1,
            pending: BTreeMap::new(),
            recently_terminal_pts: VecDeque::new(),
        })
    }

    fn push_and_pull(
        &mut self,
        unit: EncodedAccessUnit,
        maximum_bytes: u64,
        maximum_decompression_ratio: u32,
        ingested_at: Instant,
    ) -> Result<Option<DecodedRtspFrame>> {
        let dimensions = unit.dimensions.ok_or_else(|| {
            security_error("decoder input reached GStreamer before validated SPS dimensions")
        })?;
        let decoded_bytes = u64::from(dimensions.0)
            .checked_mul(u64::from(dimensions.1))
            .and_then(|value| value.checked_mul(3))
            .ok_or_else(|| security_error("decoded RTSP frame size overflowed"))?;
        if decoded_bytes > maximum_bytes {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "decoded RTSP frame exceeds maximumFrameBytes",
            ));
        }
        let ratio_bound = unit
            .compressed_bytes
            .checked_mul(u64::from(maximum_decompression_ratio))
            .ok_or_else(|| security_error("RTSP decompression-ratio bound overflowed"))?;
        if unit.compressed_bytes == 0 || decoded_bytes > ratio_bound {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "decoded RTSP frame exceeds the decompression-ratio bound",
            ));
        }
        let pts = self.next_pts;
        let next_pts = self
            .next_pts
            .checked_add(1_000_000)
            .ok_or_else(|| security_error("GStreamer decoder timestamp wrapped"))?;
        if self.pending.len() >= MAX_DECODER_PENDING_UNITS {
            let _ = retire_oldest_pending_decoder_unit(
                &mut self.pending,
                &mut self.recently_terminal_pts,
            )?;
        }
        self.next_pts = next_pts;
        let mut buffer = gst::Buffer::from_slice(unit.bytes);
        {
            let buffer = buffer
                .get_mut()
                .ok_or_else(|| backend_error("GStreamer input buffer was unexpectedly shared"))?;
            let timestamp = gst::ClockTime::from_nseconds(pts);
            buffer.set_pts(timestamp);
            buffer.set_dts(timestamp);
        }
        self.pending.insert(
            pts,
            PendingDecoderUnit {
                rtp_timestamp: unit.rtp_timestamp,
                compressed_bytes: unit.compressed_bytes,
                dimensions,
                ingested_at,
            },
        );
        self.source
            .push_buffer(buffer)
            .map_err(|_| backend_error("GStreamer rejected a complete encoded access unit"))?;
        let sample = self
            .sink
            .try_pull_sample(gst::ClockTime::from_mseconds(250));
        let Some(sample) = sample else {
            if self.sink.is_eos() {
                return Err(backend_error("GStreamer decoder reached end of stream"));
            }
            return Ok(None);
        };
        let caps = sample
            .caps()
            .ok_or_else(|| security_error("GStreamer output omitted negotiated caps"))?;
        if caps.size() != 1 {
            return Err(security_error("GStreamer output caps are ambiguous"));
        }
        let structure = caps
            .structure(0)
            .ok_or_else(|| security_error("GStreamer output caps are empty"))?;
        if structure.name().as_str() != "video/x-raw"
            || structure
                .get::<String>("format")
                .map_err(|_| security_error("GStreamer output format is absent"))?
                != "RGB"
        {
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedPixelFormat,
                "GStreamer decoder did not negotiate RGB output",
            ));
        }
        let width = structure
            .get::<i32>("width")
            .map_err(|_| security_error("GStreamer output width is absent"))?;
        let height = structure
            .get::<i32>("height")
            .map_err(|_| security_error("GStreamer output height is absent"))?;
        let width = u32::try_from(width)
            .map_err(|_| security_error("GStreamer output width is invalid"))?;
        let height = u32::try_from(height)
            .map_err(|_| security_error("GStreamer output height is invalid"))?;
        validate_dimensions(width, height)?;
        let output = sample
            .buffer()
            .ok_or_else(|| security_error("GStreamer output sample omitted its buffer"))?;
        if output.flags().intersects(
            gst::BufferFlags::CORRUPTED | gst::BufferFlags::DECODE_ONLY | gst::BufferFlags::GAP,
        ) {
            return Ok(None);
        }
        let output_pts = output
            .pts()
            .ok_or_else(|| security_error("GStreamer output frame omitted stream timestamp"))?
            .nseconds();
        let exact_length = decoded_rgb_frame_bytes(width, height)?;
        if exact_length > maximum_bytes
            || u64::try_from(output.size()).unwrap_or(u64::MAX) != exact_length
        {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "GStreamer output violates the exact decoded-frame bound",
            ));
        }
        let Some(pending) =
            correlate_decoder_output(&mut self.pending, &self.recently_terminal_pts, output_pts)?
        else {
            // This PTS already reached a terminal outcome (published or
            // retired at the bounded pending edge). Some H.265 decoder/parser
            // combinations surface a stale duplicate after the next input.
            // It has passed the raw-frame bound above but is never remapped,
            // copied, or published.
            return Ok(None);
        };
        if (width, height) != pending.dimensions {
            return Err(security_error(
                "GStreamer decoded dimensions differ from the validated SPS",
            ));
        }
        let mapped = output
            .map_readable()
            .map_err(|_| backend_error("GStreamer output frame is not readable"))?;
        remember_terminal_decoder_pts(&mut self.recently_terminal_pts, output_pts);
        Ok(Some(DecodedRtspFrame {
            bytes: Bytes::copy_from_slice(mapped.as_slice()),
            width,
            height,
            rtp_timestamp: pending.rtp_timestamp,
            compressed_bytes: pending.compressed_bytes,
            decoder_pts: output_pts,
            ingested_at: pending.ingested_at,
        }))
    }
}

#[cfg(feature = "rtsp")]
fn decoded_rgb_frame_bytes(width: u32, height: u32) -> Result<u64> {
    u64::from(width)
        .checked_mul(u64::from(height))
        .and_then(|value| value.checked_mul(3))
        .ok_or_else(|| security_error("GStreamer output size overflowed"))
}

#[cfg(feature = "rtsp")]
fn correlate_decoder_output(
    pending: &mut BTreeMap<u64, PendingDecoderUnit>,
    recently_terminal_pts: &VecDeque<u64>,
    output_pts: u64,
) -> Result<Option<PendingDecoderUnit>> {
    if let Some(unit) = pending.remove(&output_pts) {
        return Ok(Some(unit));
    }
    if recently_terminal_pts.contains(&output_pts) {
        return Ok(None);
    }
    Err(security_error(
        "GStreamer output timestamp was not requested",
    ))
}

#[cfg(feature = "rtsp")]
fn retire_oldest_pending_decoder_unit(
    pending: &mut BTreeMap<u64, PendingDecoderUnit>,
    recently_terminal_pts: &mut VecDeque<u64>,
) -> Result<u64> {
    // Do not retire all lower PTS values when a newer frame is decoded: H.264
    // and H.265 decoders may surface legitimate frames out of PTS order. At
    // the bounded admission edge, retire only one oldest requested unit. Its
    // exact PTS becomes terminal so a late frame is dropped, never correlated
    // with a newer input; an unissued PTS remains a hard failure below.
    let retired_pts = pending
        .iter()
        .min_by_key(|entry| (entry.1.ingested_at, *entry.0))
        .map(|(pts, _)| *pts)
        .ok_or_else(|| backend_error("GStreamer pending-frame retirement found no pending unit"))?;
    let _ = pending.remove(&retired_pts).ok_or_else(|| {
        backend_error("GStreamer pending-frame retirement lost its selected unit")
    })?;
    remember_terminal_decoder_pts(recently_terminal_pts, retired_pts);
    Ok(retired_pts)
}

#[cfg(feature = "rtsp")]
fn remember_terminal_decoder_pts(recently_terminal_pts: &mut VecDeque<u64>, output_pts: u64) {
    if recently_terminal_pts.len() >= MAX_DECODER_PENDING_UNITS {
        let _ = recently_terminal_pts.pop_front();
    }
    recently_terminal_pts.push_back(output_pts);
}

#[cfg(feature = "rtsp")]
impl Drop for GstreamerDecoder {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}

/// Whether decoding this access unit can still serve somebody.
///
/// `interest_at` is the instant of the most recent capture request (a capture accepts only a frame
/// with `ingested_at >= ready_at`, and `ready_at` *is* the interest instant), and `delivered_at` is
/// the ingest instant of the newest frame the worker has published, or `None` if it has published
/// none. A frame decoded when nobody is waiting cannot satisfy the next capture either -- that
/// capture will demand a frame ingested after *it* arrived -- so the decode is pure waste.
///
/// Kept outside `cfg(feature = "rtsp")` on purpose: this is the predicate that decides whether the
/// component burns a decode permit, and it should be provable without GStreamer installed.
fn capture_is_waiting(interest_at: Instant, delivered_at: Option<Instant>) -> bool {
    delivered_at.is_none_or(|delivered| interest_at > delivered)
}

#[cfg(feature = "rtsp")]
async fn create_decoder_bounded(
    codec: RtspCodec,
    maximum_bytes: u64,
    deadline: Instant,
    decode_gate: &Arc<Semaphore>,
    cancellation: &CancellationToken,
) -> Result<Arc<Mutex<GstreamerDecoder>>> {
    let permit = Arc::clone(decode_gate).acquire_owned();
    tokio::pin!(permit);
    let permit = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(cancelled_error("decoder admission")),
        _ = tokio::time::sleep_until(deadline) => return Err(timeout_error("decoder admission")),
        result = &mut permit => result.map_err(|_| security_error("GStreamer limiter was closed"))?,
    };
    let creation = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        GstreamerDecoder::new(codec, maximum_bytes).map(|decoder| Arc::new(Mutex::new(decoder)))
    });
    tokio::pin!(creation);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(cancelled_error("decoder creation")),
        _ = tokio::time::sleep_until(deadline) => Err(timeout_error("decoder creation")),
        result = &mut creation => result.map_err(|_| backend_error("GStreamer creation task failed"))?,
    }
}

#[cfg(feature = "rtsp")]
#[allow(clippy::too_many_arguments)]
async fn decode_bounded(
    decoder: Arc<Mutex<GstreamerDecoder>>,
    unit: EncodedAccessUnit,
    maximum_bytes: u64,
    maximum_decompression_ratio: u32,
    ingested_at: Instant,
    deadline: Instant,
    decode_gate: &Arc<Semaphore>,
    cancellation: &CancellationToken,
) -> Result<Option<DecodedRtspFrame>> {
    let permit = Arc::clone(decode_gate).acquire_owned();
    tokio::pin!(permit);
    let permit = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(cancelled_error("decoder admission")),
        _ = tokio::time::sleep_until(deadline) => return Err(timeout_error("decoder admission")),
        result = &mut permit => result.map_err(|_| security_error("GStreamer limiter was closed"))?,
    };
    let operation = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut decoder = decoder
            .lock()
            .map_err(|_| backend_error("GStreamer decoder lock was poisoned"))?;
        decoder.push_and_pull(
            unit,
            maximum_bytes,
            maximum_decompression_ratio,
            ingested_at,
        )
    });
    tokio::pin!(operation);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(cancelled_error("decode")),
        _ = tokio::time::sleep_until(deadline) => Err(timeout_error("decode")),
        result = &mut operation => result.map_err(|_| backend_error("GStreamer decoder task failed"))?,
    }
}

#[cfg(feature = "rtsp")]
trait AsyncRtspIo: AsyncRead + AsyncWrite + Unpin + Send {}

#[cfg(feature = "rtsp")]
impl<T> AsyncRtspIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

#[cfg(feature = "rtsp")]
type BoxedRtspIo = Box<dyn AsyncRtspIo>;

/// Delegates every cryptographic and chain check to rustls/webpki. The explicit
/// development override can suppress only the final name mismatch result; it
/// cannot suppress expiry, trust-anchor, signature, encoding, or revocation errors.
#[cfg(feature = "rtsp")]
#[derive(Debug)]
struct OptionalHostnameVerifier {
    inner: Arc<WebPkiServerVerifier>,
    verify_hostname: bool,
}

#[cfg(feature = "rtsp")]
impl ServerCertVerifier for OptionalHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(rustls::Error::InvalidCertificate(CertificateError::NotValidForName))
                if !self.verify_hostname =>
            {
                Ok(ServerCertVerified::assertion())
            }
            Err(rustls::Error::InvalidCertificate(CertificateError::NotValidForNameContext {
                ..
            })) if !self.verify_hostname => Ok(ServerCertVerified::assertion()),
            result => result,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[cfg(feature = "rtsp")]
fn build_tls_client_config(
    private_ca: Option<&SecretBytes>,
    verify_hostname: bool,
) -> Result<Arc<ClientConfig>> {
    let native = rustls_native_certs::load_native_certs();
    let mut roots = RootCertStore::empty();
    let (native_added, _) = roots.add_parsable_certificates(native.certs);
    let mut private_added = 0_usize;
    if let Some(private_ca) = private_ca {
        let mut reader = std::io::BufReader::new(private_ca.expose());
        for certificate in rustls_pemfile::certs(&mut reader) {
            let certificate = certificate
                .map_err(|_| security_error("private RTSP CA bundle contains invalid PEM"))?;
            roots
                .add(certificate)
                .map_err(|_| security_error("private RTSP CA certificate is invalid"))?;
            private_added = private_added.saturating_add(1);
        }
        if private_added == 0 {
            return Err(security_error(
                "private RTSP CA bundle contained no certificates",
            ));
        }
    }
    if native_added == 0 && private_added == 0 {
        return Err(security_error(
            "no TLS trust anchors are available for RTSPS",
        ));
    }
    let verifier = WebPkiServerVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|_| security_error("RTSPS certificate verifier configuration failed"))?;
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(OptionalHostnameVerifier {
            inner: verifier,
            verify_hostname,
        }))
        .with_no_client_auth();
    Ok(Arc::new(config))
}

#[cfg(feature = "rtsp")]
fn tls_configuration_limiter() -> &'static Arc<Semaphore> {
    static LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();
    LIMITER.get_or_init(|| Arc::new(Semaphore::new(MAX_BLOCKING_TLS_CONFIGURATIONS)))
}

#[cfg(feature = "rtsp")]
async fn build_tls_client_config_bounded(
    private_ca: Option<Arc<SecretBytes>>,
    verify_hostname: bool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Arc<ClientConfig>> {
    let permit = Arc::clone(tls_configuration_limiter()).acquire_owned();
    tokio::pin!(permit);
    let permit = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(cancelled_error("TLS configuration admission")),
        _ = tokio::time::sleep_until(deadline) => return Err(timeout_error("TLS configuration admission")),
        result = &mut permit => result.map_err(|_| security_error("TLS configuration limiter was closed"))?,
    };
    let configuration = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        build_tls_client_config(private_ca.as_deref(), verify_hostname)
    });
    tokio::pin!(configuration);
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(cancelled_error("TLS configuration")),
        _ = tokio::time::sleep_until(deadline) => Err(timeout_error("TLS configuration")),
        result = &mut configuration => result.map_err(|_| backend_error("TLS configuration task failed"))?,
    }
}

#[cfg(feature = "rtsp")]
#[derive(Clone)]
struct RtspClientOptions {
    credentials: Option<Arc<OnvifCredentials>>,
    nonce_source: Arc<dyn OnvifNonceSource>,
    authentication_mode: AuthenticationMode,
    basic_over_plaintext: bool,
    max_header_bytes: usize,
    tls_config: Option<Arc<ClientConfig>>,
}

#[cfg(feature = "rtsp")]
enum RtspAuthentication {
    None,
    Digest {
        challenge: DigestChallenge,
        nonce_count: u32,
    },
    Basic,
}

#[cfg(feature = "rtsp")]
impl std::fmt::Debug for RtspAuthentication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::None => "RtspAuthentication::None",
            Self::Digest { .. } => "RtspAuthentication::Digest(<redacted>)",
            Self::Basic => "RtspAuthentication::Basic(<redacted>)",
        })
    }
}

#[cfg(feature = "rtsp")]
impl RtspAuthentication {
    fn authorization(
        &mut self,
        credentials: Option<&OnvifCredentials>,
        nonce_source: &dyn OnvifNonceSource,
        method: &str,
        target: &str,
        basic_allowed: bool,
    ) -> Result<Option<SecretBytes>> {
        match self {
            Self::None => Ok(None),
            Self::Digest {
                challenge,
                nonce_count,
            } => {
                *nonce_count = nonce_count
                    .checked_add(1)
                    .ok_or_else(|| security_error("RTSP Digest nonce counter wrapped"))?;
                let credentials = credentials.ok_or_else(|| {
                    security_error("RTSP authentication requires configured credentials")
                })?;
                digest_authorization_for_method(
                    challenge,
                    credentials,
                    method,
                    target,
                    *nonce_count,
                    nonce_source,
                )
                .map(Some)
            }
            Self::Basic => {
                let credentials = credentials.ok_or_else(|| {
                    security_error("RTSP authentication requires configured credentials")
                })?;
                basic_authorization(credentials, basic_allowed).map(Some)
            }
        }
    }
}

#[cfg(feature = "rtsp")]
#[derive(Debug)]
struct InterleavedPacket {
    channel: u8,
    bytes: Vec<u8>,
}

#[cfg(feature = "rtsp")]
struct RtspConnection {
    io: BoxedRtspIo,
    read_buffer: Vec<u8>,
    next_cseq: u32,
    session_id: Option<String>,
    authentication: RtspAuthentication,
    authentication_established: bool,
    options: RtspClientOptions,
    secure: bool,
    closed: bool,
}

#[cfg(feature = "rtsp")]
impl std::fmt::Debug for RtspConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RtspConnection")
            .field("next_cseq", &self.next_cseq)
            .field("has_session", &self.session_id.is_some())
            .field("authentication", &self.authentication)
            .field(
                "authentication_established",
                &self.authentication_established,
            )
            .field("secure", &self.secure)
            .field("closed", &self.closed)
            .finish()
    }
}

#[cfg(feature = "rtsp")]
impl RtspConnection {
    async fn connect(
        target: &PinnedRtspUri,
        options: RtspClientOptions,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        if deadline <= Instant::now() {
            return Err(timeout_error("connection"));
        }
        let connection = TcpStream::connect(target.socket_address());
        tokio::pin!(connection);
        let tcp = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(cancelled_error("connection")),
            _ = tokio::time::sleep_until(deadline) => return Err(timeout_error("connection")),
            result = &mut connection => result.map_err(|_| backend_error("RTSP TCP connection failed"))?,
        };
        tcp.set_nodelay(true)
            .map_err(|_| backend_error("RTSP TCP socket configuration failed"))?;
        let secure = target.is_tls();
        let io: BoxedRtspIo = if secure {
            let tls_config = options
                .tls_config
                .as_ref()
                .ok_or_else(|| security_error("RTSPS connection omitted TLS policy"))?;
            let server_name = ServerName::try_from(target.host().to_owned())
                .map_err(|_| security_error("RTSPS hostname is invalid for SNI"))?;
            let handshake = TlsConnector::from(Arc::clone(tls_config)).connect(server_name, tcp);
            tokio::pin!(handshake);
            let tls = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(cancelled_error("TLS handshake")),
                _ = tokio::time::sleep_until(deadline) => return Err(timeout_error("TLS handshake")),
                result = &mut handshake => result.map_err(|_| backend_error("RTSPS TLS verification or handshake failed"))?,
            };
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        let authentication = if options.authentication_mode == AuthenticationMode::Basic {
            if options.credentials.is_none() {
                return Err(security_error(
                    "RTSP Basic authentication was configured without credentials",
                ));
            }
            if !(secure || options.basic_over_plaintext) {
                return Err(security_error(
                    "RTSP Basic over plaintext requires both insecure-development permissions",
                ));
            }
            RtspAuthentication::Basic
        } else {
            RtspAuthentication::None
        };
        Ok(Self {
            io,
            read_buffer: Vec::with_capacity(8192),
            next_cseq: 1,
            session_id: None,
            authentication,
            authentication_established: false,
            options,
            secure,
            closed: false,
        })
    }

    async fn negotiate(
        &mut self,
        stream_uri: &PinnedRtspUri,
        policy: &RtspUriPolicy,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<RtspTrack> {
        let options_response = self
            .send_read_only(
                "OPTIONS",
                stream_uri.url(),
                &[],
                &[],
                deadline,
                cancellation,
            )
            .await?;
        if !matches!(options_response.status, 200 | 204 | 405 | 501) {
            return Err(backend_error(format!(
                "RTSP OPTIONS returned status {}",
                options_response.status
            )));
        }
        let describe_response = self
            .send_read_only(
                "DESCRIBE",
                stream_uri.url(),
                &[("Accept", "application/sdp")],
                &[],
                deadline,
                cancellation,
            )
            .await?;
        if describe_response.status != 200 {
            return Err(backend_error(format!(
                "RTSP DESCRIBE returned status {}",
                describe_response.status
            )));
        }
        let content_type = describe_response
            .single_header("content-type")?
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            .unwrap_or_default();
        if !content_type.eq_ignore_ascii_case("application/sdp") {
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "RTSP DESCRIBE response is not application/sdp",
            ));
        }
        let base_uri = match describe_response.single_header("content-base")? {
            Some(value) => {
                Url::parse(value).map_err(|_| security_error("RTSP Content-Base URI is invalid"))?
            }
            None => stream_uri.url().clone(),
        };
        policy.validate_control_uri(&base_uri)?;
        let track = parse_sdp_track(&describe_response.body, &base_uri, policy)?;
        let setup_response = self
            .send_stateful(
                "SETUP",
                &track.control_uri,
                &[("Transport", "RTP/AVP/TCP;unicast;interleaved=0-1")],
                &[],
                deadline,
                cancellation,
            )
            .await?;
        if setup_response.status != 200 {
            return Err(backend_error(format!(
                "RTSP SETUP returned status {}",
                setup_response.status
            )));
        }
        validate_transport_response(
            setup_response
                .single_header("transport")?
                .ok_or_else(|| security_error("RTSP SETUP omitted Transport"))?,
        )?;
        let session_id = parse_session_id(
            setup_response
                .single_header("session")?
                .ok_or_else(|| security_error("RTSP SETUP omitted Session"))?,
        )?;
        self.session_id = Some(session_id);
        let play_response = self
            .send_stateful("PLAY", stream_uri.url(), &[], &[], deadline, cancellation)
            .await?;
        if play_response.status != 200 {
            return Err(backend_error(format!(
                "RTSP PLAY returned status {}",
                play_response.status
            )));
        }
        Ok(track)
    }

    async fn send_read_only(
        &mut self,
        method: &'static str,
        target: &Url,
        headers: &[(&str, &str)],
        body: &[u8],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<RtspResponse> {
        let first = self
            .send_once(method, target, headers, body, deadline, cancellation)
            .await?;
        if first.status != 401 {
            if read_only_success_establishes_authentication(
                self.options.authentication_mode,
                !matches!(self.authentication, RtspAuthentication::None),
            ) {
                self.authentication_established = true;
            }
            return Ok(first);
        }
        self.authentication = self.negotiate_authentication(&first)?;
        let retried = self
            .send_once(method, target, headers, body, deadline, cancellation)
            .await?;
        if matches!(retried.status, 401 | 403) {
            return Err(backend_error("RTSP authentication or authorization failed"));
        }
        self.authentication_established = read_only_success_establishes_authentication(
            self.options.authentication_mode,
            !matches!(self.authentication, RtspAuthentication::None),
        );
        Ok(retried)
    }

    async fn send_stateful(
        &mut self,
        method: &'static str,
        target: &Url,
        headers: &[(&str, &str)],
        body: &[u8],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<RtspResponse> {
        if !self.authentication_established {
            return Err(security_error(
                "stateful RTSP request lacks established authentication",
            ));
        }
        let response = self
            .send_once(method, target, headers, body, deadline, cancellation)
            .await?;
        if matches!(response.status, 401 | 403) {
            return Err(backend_error(
                "stateful RTSP authentication failed without replay",
            ));
        }
        Ok(response)
    }

    fn negotiate_authentication(&self, response: &RtspResponse) -> Result<RtspAuthentication> {
        let challenge = response
            .authentication_challenges()
            .ok_or_else(|| security_error("RTSP 401 omitted WWW-Authenticate"))?;
        let credentials = self.options.credentials.as_deref().ok_or_else(|| {
            security_error("camera requested RTSP authentication without credentials")
        })?;
        let _ = credentials.username()?;
        let digest_offered = find_auth_scheme(&challenge, "digest").is_some();
        let basic_offered = find_auth_scheme(&challenge, "basic").is_some();
        match self.options.authentication_mode {
            AuthenticationMode::HttpDigest if !digest_offered => Err(security_error(
                "camera did not offer required RTSP Digest authentication",
            )),
            AuthenticationMode::Basic if !basic_offered => Err(security_error(
                "camera did not offer required RTSP Basic authentication",
            )),
            AuthenticationMode::HttpDigest => Ok(RtspAuthentication::Digest {
                challenge: parse_digest_challenge(&challenge)?,
                nonce_count: 0,
            }),
            AuthenticationMode::Basic => {
                if !(self.secure || self.options.basic_over_plaintext) {
                    return Err(security_error(
                        "RTSP Basic over plaintext requires both insecure-development permissions",
                    ));
                }
                Ok(RtspAuthentication::Basic)
            }
            AuthenticationMode::Auto | AuthenticationMode::WsseDigest if digest_offered => {
                Ok(RtspAuthentication::Digest {
                    challenge: parse_digest_challenge(&challenge)?,
                    nonce_count: 0,
                })
            }
            AuthenticationMode::Auto | AuthenticationMode::WsseDigest if basic_offered => {
                if !(self.secure || self.options.basic_over_plaintext) {
                    return Err(security_error(
                        "camera offered only forbidden RTSP Basic authentication",
                    ));
                }
                Ok(RtspAuthentication::Basic)
            }
            _ => Err(security_error(
                "camera offered no supported RTSP authentication scheme",
            )),
        }
    }

    async fn send_once(
        &mut self,
        method: &'static str,
        target: &Url,
        headers: &[(&str, &str)],
        body: &[u8],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<RtspResponse> {
        if deadline <= Instant::now() {
            return Err(timeout_error("request"));
        }
        if body.len() > MAX_RTSP_BODY_BYTES {
            return Err(security_error("outbound RTSP request body is oversized"));
        }
        let cseq = self.next_cseq;
        self.next_cseq = self
            .next_cseq
            .checked_add(1)
            .ok_or_else(|| security_error("RTSP CSeq counter wrapped"))?;
        let target_text = target.as_str();
        let authorization = self.authentication.authorization(
            self.options.credentials.as_deref(),
            self.options.nonce_source.as_ref(),
            method,
            target_text,
            self.secure || self.options.basic_over_plaintext,
        )?;
        let mut request = Zeroizing::new(Vec::with_capacity(1024 + body.len()));
        request.extend_from_slice(method.as_bytes());
        request.push(b' ');
        request.extend_from_slice(target_text.as_bytes());
        request.extend_from_slice(b" RTSP/1.0\r\nCSeq: ");
        request.extend_from_slice(cseq.to_string().as_bytes());
        request.extend_from_slice(b"\r\nUser-Agent: EdgeCommons-camera-adapter/0.1\r\n");
        if let Some(session_id) = &self.session_id {
            request.extend_from_slice(b"Session: ");
            request.extend_from_slice(session_id.as_bytes());
            request.extend_from_slice(b"\r\n");
        }
        for (name, value) in headers {
            if name.is_empty()
                || name.chars().any(char::is_control)
                || value.chars().any(char::is_control)
            {
                return Err(security_error("outbound RTSP header is invalid"));
            }
            request.extend_from_slice(name.as_bytes());
            request.extend_from_slice(b": ");
            request.extend_from_slice(value.as_bytes());
            request.extend_from_slice(b"\r\n");
        }
        if let Some(authorization) = authorization.as_ref() {
            request.extend_from_slice(b"Authorization: ");
            request.extend_from_slice(authorization.expose());
            request.extend_from_slice(b"\r\n");
        }
        if !body.is_empty() {
            request.extend_from_slice(b"Content-Length: ");
            request.extend_from_slice(body.len().to_string().as_bytes());
            request.extend_from_slice(b"\r\n");
        }
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(body);
        if request.len() > self.options.max_header_bytes.saturating_add(body.len()) {
            return Err(security_error(
                "outbound RTSP request headers are oversized",
            ));
        }
        let write = self.io.write_all(request.as_slice());
        tokio::pin!(write);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(cancelled_error("request write")),
            _ = tokio::time::sleep_until(deadline) => return Err(timeout_error("request write")),
            result = &mut write => result.map_err(|_| backend_error("RTSP request write failed"))?,
        }
        let response = self.read_response(deadline, cancellation).await?;
        let response_cseq = response
            .single_header("cseq")?
            .ok_or_else(|| security_error("RTSP response omitted CSeq"))?
            .parse::<u32>()
            .map_err(|_| security_error("RTSP response CSeq is invalid"))?;
        if response_cseq != cseq {
            return Err(security_error(
                "RTSP response CSeq did not match the request",
            ));
        }
        Ok(response)
    }

    async fn read_response(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<RtspResponse> {
        let mut early_packets = 0_usize;
        loop {
            self.ensure_buffered(1, deadline, cancellation).await?;
            if self.read_buffer[0] != b'$' {
                break;
            }
            let packet = self.read_interleaved(deadline, cancellation).await?;
            early_packets = early_packets.saturating_add(1);
            if early_packets > 16 || packet.bytes.len() > MAX_RTSP_BODY_BYTES {
                return Err(security_error(
                    "RTSP server sent excessive media before its response",
                ));
            }
        }
        let header_end = loop {
            if let Some(index) = self
                .read_buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
            {
                break index + 4;
            }
            if self.read_buffer.len() >= self.options.max_header_bytes {
                return Err(security_error("RTSP response headers exceeded their bound"));
            }
            let required = self.read_buffer.len().saturating_add(1);
            self.ensure_buffered(required, deadline, cancellation)
                .await?;
        };
        let head = self.read_buffer[..header_end].to_vec();
        let (status, headers, content_length) =
            parse_rtsp_response_head(&head, self.options.max_header_bytes)?;
        let total = header_end
            .checked_add(content_length)
            .ok_or_else(|| security_error("RTSP response size overflowed"))?;
        self.ensure_buffered(total, deadline, cancellation).await?;
        let body = self.read_buffer[header_end..total].to_vec();
        self.read_buffer.drain(..total);
        Ok(RtspResponse {
            status,
            headers,
            body,
        })
    }

    async fn read_interleaved(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InterleavedPacket> {
        self.ensure_buffered(4, deadline, cancellation).await?;
        if self.read_buffer[0] != b'$' {
            return Err(security_error(
                "RTSP control channel emitted non-interleaved data while playing",
            ));
        }
        let channel = self.read_buffer[1];
        let length = usize::from(u16::from_be_bytes([
            self.read_buffer[2],
            self.read_buffer[3],
        ]));
        if length == 0 || length > MAX_INTERLEAVED_PACKET_BYTES {
            return Err(security_error("RTSP interleaved frame length is invalid"));
        }
        let total = 4_usize
            .checked_add(length)
            .ok_or_else(|| security_error("RTSP interleaved frame size overflowed"))?;
        self.ensure_buffered(total, deadline, cancellation).await?;
        let bytes = self.read_buffer[4..total].to_vec();
        self.read_buffer.drain(..total);
        Ok(InterleavedPacket { channel, bytes })
    }

    async fn ensure_buffered(
        &mut self,
        required: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let maximum = self
            .options
            .max_header_bytes
            .checked_add(MAX_RTSP_BODY_BYTES)
            .and_then(|value| value.checked_add(MAX_INTERLEAVED_PACKET_BYTES + 4))
            .ok_or_else(|| security_error("RTSP receive-buffer bound overflowed"))?;
        if required > maximum {
            return Err(security_error("RTSP receive-buffer request is oversized"));
        }
        while self.read_buffer.len() < required {
            if deadline <= Instant::now() {
                return Err(timeout_error("response read"));
            }
            let remaining = maximum.saturating_sub(self.read_buffer.len()).min(8192);
            if remaining == 0 {
                return Err(security_error("RTSP receive buffer exceeded its bound"));
            }
            let mut chunk = vec![0_u8; remaining];
            let read = self.io.read(&mut chunk);
            tokio::pin!(read);
            let count = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(cancelled_error("response read")),
                _ = tokio::time::sleep_until(deadline) => return Err(timeout_error("response read")),
                result = &mut read => result.map_err(|_| backend_error("RTSP response read failed"))?,
            };
            if count == 0 {
                return Err(backend_error(
                    "RTSP connection closed before a complete response",
                ));
            }
            self.read_buffer.extend_from_slice(&chunk[..count]);
        }
        Ok(())
    }

    async fn teardown(
        &mut self,
        target: &Url,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        if self.session_id.is_none() {
            return Ok(());
        }
        let response = self
            .send_stateful("TEARDOWN", target, &[], &[], deadline, cancellation)
            .await?;
        if response.status != 200 {
            return Err(backend_error(format!(
                "RTSP TEARDOWN returned status {}",
                response.status
            )));
        }
        Ok(())
    }
}

#[cfg(feature = "rtsp")]
fn validate_transport_response(value: &str) -> Result<()> {
    let normalized = value.to_ascii_lowercase();
    let mut fields = normalized.split(';').map(str::trim);
    if fields.next() != Some("rtp/avp/tcp") {
        return Err(security_error(
            "RTSP server did not accept interleaved TCP transport",
        ));
    }
    let mut interleaved = None;
    for field in fields {
        if let Some(value) = field.strip_prefix("interleaved=") {
            if interleaved.replace(value).is_some() {
                return Err(security_error(
                    "RTSP Transport contains duplicate interleaved channels",
                ));
            }
        }
    }
    if interleaved != Some("0-1") {
        return Err(security_error(
            "RTSP server changed the requested interleaved channels",
        ));
    }
    Ok(())
}

#[cfg(feature = "rtsp")]
fn parse_session_id(value: &str) -> Result<String> {
    let session_id = value.split(';').next().map(str::trim).unwrap_or_default();
    if session_id.is_empty()
        || session_id.len() > MAX_RTSP_SESSION_ID_BYTES
        || session_id.chars().any(char::is_control)
    {
        return Err(security_error("RTSP Session identifier violates bounds"));
    }
    Ok(session_id.to_owned())
}

#[cfg(feature = "rtsp")]
#[derive(Debug, Clone)]
struct PublishedRtspFrame {
    frame: Arc<DecodedRtspFrame>,
    codec: RtspCodec,
    incomplete_units: u64,
}

#[cfg(feature = "rtsp")]
#[derive(Debug, Clone)]
enum RtspWorkerUpdate {
    Starting {
        generation: u64,
    },
    Frame {
        generation: u64,
        frame: Arc<PublishedRtspFrame>,
    },
    Failed {
        generation: u64,
        code: ErrorCode,
        message: Arc<str>,
    },
    Closed {
        generation: u64,
    },
}

#[cfg(feature = "rtsp")]
impl RtspWorkerUpdate {
    const fn generation(&self) -> u64 {
        match self {
            Self::Starting { generation }
            | Self::Frame { generation, .. }
            | Self::Failed { generation, .. }
            | Self::Closed { generation } => *generation,
        }
    }

    const fn is_terminal(&self) -> bool {
        matches!(self, Self::Failed { .. } | Self::Closed { .. })
    }
}

#[cfg(feature = "rtsp")]
struct RtspWorkerHandle {
    updates: watch::Receiver<RtspWorkerUpdate>,
    interest: watch::Sender<Instant>,
    cancellation: CancellationToken,
    join: Option<JoinHandle<()>>,
}

#[cfg(feature = "rtsp")]
impl std::fmt::Debug for RtspWorkerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RtspWorkerHandle")
            .field("update", &*self.updates.borrow())
            .field(
                "finished",
                &self.join.as_ref().is_none_or(JoinHandle::is_finished),
            )
            .finish()
    }
}

#[cfg(feature = "rtsp")]
impl RtspWorkerHandle {
    fn is_terminal(&self) -> bool {
        self.join.as_ref().is_none_or(JoinHandle::is_finished)
            || self.updates.borrow().is_terminal()
    }

    fn mark_interest(&self, now: Instant) {
        self.interest.send_replace(now);
    }

    async fn stop(&mut self, deadline: Instant) {
        self.cancellation.cancel();
        let Some(mut join) = self.join.take() else {
            return;
        };
        if deadline <= Instant::now() {
            join.abort();
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => join.abort(),
            _ = &mut join => {},
        }
    }
}

#[cfg(feature = "rtsp")]
impl Drop for RtspWorkerHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

/// One per-camera RTSP controller. Its optional warm worker is private to the
/// owning camera actor and its watch channel retains at most one decoded frame.
#[cfg(feature = "rtsp")]
pub(crate) struct RtspCaptureController {
    policy: RtspUriPolicy,
    resolver: Arc<dyn OnvifResolver>,
    options: RtspClientOptions,
    session_policy: RtspSessionPolicy,
    maximum_frame_bytes: u64,
    maximum_decompression_ratio: u32,
    source_host: String,
    clock: Arc<dyn OnvifClock>,
    decode_gate: Arc<Semaphore>,
    worker: Option<RtspWorkerHandle>,
}

#[cfg(feature = "rtsp")]
impl std::fmt::Debug for RtspCaptureController {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RtspCaptureController")
            .field("policy", &self.policy)
            .field("session_policy", &self.session_policy)
            .field("maximum_frame_bytes", &self.maximum_frame_bytes)
            .field(
                "maximum_decompression_ratio",
                &self.maximum_decompression_ratio,
            )
            .field("source_host", &self.source_host)
            .field("warm_worker", &self.worker)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "rtsp")]
/// Construction inputs kept in one object so the security policy cannot be
/// accidentally assembled with positional booleans at call sites.
pub(crate) struct RtspControllerConfig {
    pub(crate) stream_uri: String,
    pub(crate) anchor: RtspNetworkAnchor,
    pub(crate) resolver: Arc<dyn OnvifResolver>,
    pub(crate) credentials: Option<Arc<OnvifCredentials>>,
    pub(crate) nonce_source: Arc<dyn OnvifNonceSource>,
    pub(crate) private_ca: Option<Arc<SecretBytes>>,
    pub(crate) verify_hostname: bool,
    pub(crate) allow_insecure: bool,
    pub(crate) authentication_mode: AuthenticationMode,
    pub(crate) security: SecurityConfig,
    pub(crate) session_policy: RtspSessionPolicy,
    pub(crate) maximum_frame_bytes: u64,
    pub(crate) clock: Arc<dyn OnvifClock>,
    /// Component-wide bound on concurrent blocking GStreamer operations, shared by every camera.
    ///
    /// Every decoder creation and every access-unit decode takes a permit, so this is the real
    /// width of the RTSP decode stage. It is sized from `limits.maxConcurrentCaptures` and injected
    /// rather than being a process-global, so a camera can never be silently narrower than the
    /// concurrency the component advertises.
    pub(crate) decode_gate: Arc<Semaphore>,
}

#[cfg(feature = "rtsp")]
impl RtspCaptureController {
    pub(crate) async fn establish(
        config: RtspControllerConfig,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        if config.maximum_frame_bytes == 0 {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "RTSP session frame bound must be positive",
            ));
        }
        let (policy, pinned) = RtspUriPolicy::establish(
            &config.stream_uri,
            config.anchor,
            config.allow_insecure,
            config.resolver.as_ref(),
            deadline,
            cancellation,
        )
        .await?;
        let tls_config = if pinned.is_tls() {
            Some(
                build_tls_client_config_bounded(
                    config.private_ca,
                    config.verify_hostname,
                    deadline,
                    cancellation,
                )
                .await?,
            )
        } else {
            None
        };
        let source_host = pinned.host().to_owned();
        Ok(Self {
            policy,
            resolver: config.resolver,
            options: RtspClientOptions {
                credentials: config.credentials,
                nonce_source: config.nonce_source,
                authentication_mode: config.authentication_mode,
                basic_over_plaintext: config.allow_insecure
                    && config.security.allow_basic_over_plaintext,
                max_header_bytes: config.security.max_header_bytes,
                tls_config,
            },
            session_policy: config.session_policy,
            maximum_frame_bytes: config.maximum_frame_bytes,
            maximum_decompression_ratio: config.security.max_decompression_ratio,
            source_host,
            clock: config.clock,
            decode_gate: config.decode_gate,
            worker: None,
        })
    }

    pub(crate) async fn capture(
        &mut self,
        maximum_bytes: u64,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<CaptureFrame> {
        if timeout.is_zero() {
            return Err(timeout_error("capture"));
        }
        let maximum_bytes = maximum_bytes.min(self.maximum_frame_bytes);
        if maximum_bytes == 0 {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "RTSP capture frame bound must be positive",
            ));
        }
        let ready_at = Instant::now();
        let deadline = ready_at + timeout;
        if self
            .worker
            .as_ref()
            .is_some_and(RtspWorkerHandle::is_terminal)
        {
            if let Some(mut stale) = self.worker.take() {
                stale.stop(deadline).await;
            }
        }
        if self.worker.is_none() {
            self.worker = Some(self.start_worker(deadline));
        }
        let minimum_generation = if let Some(worker) = self.worker.as_ref() {
            worker.mark_interest(ready_at);
            worker.updates.borrow().generation()
        } else {
            return Err(backend_error(
                "RTSP worker could not be established for capture",
            ));
        };
        loop {
            let update = self
                .worker
                .as_ref()
                .map(|worker| worker.updates.borrow().clone())
                .ok_or_else(|| backend_error("RTSP worker disappeared during capture"))?;
            match update {
                RtspWorkerUpdate::Frame { generation, frame }
                    if generation > minimum_generation && frame.frame.ingested_at >= ready_at =>
                {
                    validate_published_frame(
                        &frame.frame,
                        maximum_bytes,
                        self.maximum_decompression_ratio,
                    )?;
                    let observed_at = self.clock.now();
                    let result = CaptureFrame {
                        bytes: frame.frame.bytes.clone(),
                        width: frame.frame.width,
                        height: frame.frame.height,
                        pixel_format: PixelFormat::Rgb8,
                        capture_mode: CaptureMode::RtspFrame,
                        source_timestamp: Some(observed_at),
                        timestamp_quality: FrameTimestampQuality::AdapterReceive,
                        backend_metadata: BTreeMap::from([
                            (
                                "codec".to_owned(),
                                Value::String(frame.codec.encoding_name().to_owned()),
                            ),
                            (
                                "sourceHost".to_owned(),
                                Value::String(self.source_host.clone()),
                            ),
                            (
                                "rtpTimestamp".to_owned(),
                                Value::from(u64::from(frame.frame.rtp_timestamp)),
                            ),
                            (
                                "decoderPts".to_owned(),
                                Value::from(frame.frame.decoder_pts),
                            ),
                            (
                                "compressedBytes".to_owned(),
                                Value::from(frame.frame.compressed_bytes),
                            ),
                            (
                                "incompleteFrames".to_owned(),
                                Value::from(frame.incomplete_units),
                            ),
                        ]),
                    };
                    if self.session_policy == RtspSessionPolicy::OnDemand {
                        if let Some(mut completed) = self.worker.take() {
                            completed.stop(deadline).await;
                        }
                    }
                    return Ok(result);
                }
                RtspWorkerUpdate::Failed { code, message, .. } => {
                    let error = worker_failure(code, &message);
                    if let Some(mut failed) = self.worker.take() {
                        failed.stop(deadline).await;
                    }
                    return Err(error);
                }
                RtspWorkerUpdate::Closed { .. } => {
                    if let Some(mut closed) = self.worker.take() {
                        closed.stop(deadline).await;
                    }
                    return Err(CameraError::rejected(
                        ErrorCode::CameraUnavailable,
                        "RTSP stream closed before a fresh complete frame",
                    ));
                }
                _ => {}
            }
            enum WaitOutcome {
                Changed(std::result::Result<(), watch::error::RecvError>),
                Cancelled,
                TimedOut,
            }
            let wait_outcome = {
                let worker = self
                    .worker
                    .as_mut()
                    .ok_or_else(|| backend_error("RTSP worker disappeared while waiting"))?;
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => WaitOutcome::Cancelled,
                    _ = tokio::time::sleep_until(deadline) => WaitOutcome::TimedOut,
                    result = worker.updates.changed() => WaitOutcome::Changed(result),
                }
            };
            match wait_outcome {
                WaitOutcome::Cancelled => {
                    if self.session_policy == RtspSessionPolicy::OnDemand {
                        if let Some(mut cancelled) = self.worker.take() {
                            cancelled.stop(deadline).await;
                        }
                    }
                    return Err(cancelled_error("capture"));
                }
                WaitOutcome::TimedOut => {
                    if self.session_policy == RtspSessionPolicy::OnDemand {
                        if let Some(mut expired) = self.worker.take() {
                            expired.stop(deadline).await;
                        }
                    }
                    return Err(timeout_error("capture"));
                }
                WaitOutcome::Changed(result) => {
                    if result.is_err() {
                        return Err(CameraError::rejected(
                            ErrorCode::CameraUnavailable,
                            "RTSP worker stopped before producing a complete frame",
                        ));
                    }
                }
            }
        }
    }

    fn start_worker(&self, startup_deadline: Instant) -> RtspWorkerHandle {
        let (updates_tx, updates) = watch::channel(RtspWorkerUpdate::Starting { generation: 0 });
        let now = Instant::now();
        let (interest, interest_rx) = watch::channel(now);
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let policy = self.policy.clone();
        let resolver = Arc::clone(&self.resolver);
        let options = self.options.clone();
        let session_policy = self.session_policy;
        let maximum_frame_bytes = self.maximum_frame_bytes;
        let maximum_decompression_ratio = self.maximum_decompression_ratio;
        let decode_gate = Arc::clone(&self.decode_gate);
        let join = tokio::spawn(async move {
            let result = run_rtsp_worker(
                policy,
                resolver,
                options,
                session_policy,
                maximum_frame_bytes,
                maximum_decompression_ratio,
                decode_gate,
                startup_deadline,
                interest_rx,
                worker_cancellation,
                &updates_tx,
            )
            .await;
            let generation = updates_tx.borrow().generation().saturating_add(1);
            match result {
                Ok(()) => {
                    updates_tx.send_replace(RtspWorkerUpdate::Closed { generation });
                }
                Err(error) => {
                    updates_tx.send_replace(RtspWorkerUpdate::Failed {
                        generation,
                        code: error.code(),
                        message: Arc::from(sanitize_worker_error(&error)),
                    });
                }
            }
        });
        RtspWorkerHandle {
            updates,
            interest,
            cancellation,
            join: Some(join),
        }
    }

    pub(crate) async fn close(&mut self, deadline: Instant) {
        if let Some(mut worker) = self.worker.take() {
            worker.stop(deadline).await;
        }
    }
}

#[cfg(feature = "rtsp")]
impl Drop for RtspCaptureController {
    fn drop(&mut self) {
        let _ = self.worker.take();
    }
}

#[cfg(feature = "rtsp")]
#[allow(clippy::too_many_arguments)]
async fn run_rtsp_worker(
    policy: RtspUriPolicy,
    resolver: Arc<dyn OnvifResolver>,
    options: RtspClientOptions,
    session_policy: RtspSessionPolicy,
    maximum_frame_bytes: u64,
    maximum_decompression_ratio: u32,
    decode_gate: Arc<Semaphore>,
    startup_deadline: Instant,
    mut interest: watch::Receiver<Instant>,
    cancellation: CancellationToken,
    updates: &watch::Sender<RtspWorkerUpdate>,
) -> Result<()> {
    let pinned = policy
        .pin(resolver.as_ref(), startup_deadline, &cancellation)
        .await?;
    let stream_uri = pinned.url().clone();
    let mut connection =
        RtspConnection::connect(&pinned, options, startup_deadline, &cancellation).await?;
    let track = connection
        .negotiate(&pinned, &policy, startup_deadline, &cancellation)
        .await?;
    let decoder = create_decoder_bounded(
        track.codec,
        maximum_frame_bytes,
        startup_deadline,
        &decode_gate,
        &cancellation,
    )
    .await?;
    let mut assembler = AccessUnitAssembler::new(&track, maximum_frame_bytes)?;
    // The ingest instant of the newest frame this worker has published, or `None` while it has
    // published nothing. A capture accepts a frame only when `ingested_at >= ready_at` and
    // `ready_at` is exactly the interest instant, so this is all that is needed to answer "is
    // anybody still waiting for a frame?" -- see `capture` above.
    let mut delivered_at: Option<Instant> = None;
    let run_result = async {
        loop {
            let operation_deadline = match session_policy {
                RtspSessionPolicy::OnDemand => startup_deadline,
                RtspSessionPolicy::Warm => *interest.borrow() + WARM_SESSION_IDLE,
            };
            if operation_deadline <= Instant::now() {
                return Ok(());
            }
            let packet = connection.read_interleaved(operation_deadline, &cancellation);
            tokio::pin!(packet);
            let packet = if session_policy == RtspSessionPolicy::Warm {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(cancelled_error("worker")),
                    changed = interest.changed() => {
                        if changed.is_err() {
                            return Ok(());
                        }
                        continue;
                    }
                    result = &mut packet => result?,
                }
            } else {
                packet.await?
            };
            match packet.channel {
                1 => continue,
                0 => {}
                _ => {
                    return Err(security_error(
                        "RTSP server used an unnegotiated interleaved channel",
                    ));
                }
            }
            let rtp = parse_rtp_packet(&packet.bytes)?;
            let Some(unit) = assembler.push(rtp)? else {
                continue;
            };
            if unit.dimensions.is_none() {
                continue;
            }
            // Decode only while a capture is actually waiting.
            //
            // A warm worker holds the RTSP session open for 30 s after the last capture, and it
            // used to decode every assembled access unit for that whole window -- 25 frames a
            // second, per camera, each one taking a permit from the component-wide decode gate.
            // None of those frames can ever satisfy a capture: `capture` requires
            // `ingested_at >= ready_at`, so a frame decoded before the next request arrives is
            // discarded on sight. It was pure waste, and it was waste that crowded real captures
            // out of the gate; a dozen warm cameras could starve the capture path outright.
            //
            // Skipping the decode leaves the GStreamer decoder without the intervening reference
            // frames, so on the next capture it emits nothing until the stream's next IDR --
            // `push_and_pull` already reports that as `Ok(None)` and the loop simply waits. A warm
            // capture therefore costs up to one GOP rather than one frame. That is the deliberate
            // trade: warm sessions keep the expensive things (the RTSP session, the negotiated
            // track, the built pipeline) and stop paying to decode pictures nobody asked for.
            if !capture_is_waiting(*interest.borrow(), delivered_at) {
                continue;
            }
            let ingested_at = Instant::now();
            let Some(frame) = decode_bounded(
                Arc::clone(&decoder),
                unit,
                maximum_frame_bytes,
                maximum_decompression_ratio,
                ingested_at,
                operation_deadline,
                &decode_gate,
                &cancellation,
            )
            .await?
            else {
                continue;
            };
            let generation = updates
                .borrow()
                .generation()
                .checked_add(1)
                .ok_or_else(|| security_error("RTSP worker generation wrapped"))?;
            updates.send_replace(RtspWorkerUpdate::Frame {
                generation,
                frame: Arc::new(PublishedRtspFrame {
                    frame: Arc::new(frame),
                    codec: track.codec,
                    incomplete_units: assembler.incomplete_units,
                }),
            });
            delivered_at = Some(ingested_at);
        }
    }
    .await;
    let teardown_deadline = Instant::now() + Duration::from_secs(1);
    let teardown_cancellation = CancellationToken::new();
    let _ = connection
        .teardown(&stream_uri, teardown_deadline, &teardown_cancellation)
        .await;
    run_result
}

#[cfg(feature = "rtsp")]
fn validate_published_frame(
    frame: &DecodedRtspFrame,
    maximum_bytes: u64,
    maximum_decompression_ratio: u32,
) -> Result<()> {
    let decoded = u64::try_from(frame.bytes.len()).unwrap_or(u64::MAX);
    if decoded == 0 || decoded > maximum_bytes {
        return Err(CameraError::rejected(
            ErrorCode::ResourceLimit,
            "RTSP frame exceeds the accepted maximumFrameBytes",
        ));
    }
    let ratio_bound = frame
        .compressed_bytes
        .checked_mul(u64::from(maximum_decompression_ratio))
        .ok_or_else(|| security_error("RTSP request decompression bound overflowed"))?;
    if frame.compressed_bytes == 0 || decoded > ratio_bound {
        return Err(CameraError::rejected(
            ErrorCode::ResourceLimit,
            "RTSP frame exceeds the accepted decompression-ratio bound",
        ));
    }
    Ok(())
}

#[cfg(feature = "rtsp")]
fn sanitize_worker_error(error: &CameraError) -> String {
    let mut sanitized = error
        .to_string()
        .chars()
        .filter(|character| !character.is_control())
        .take(512)
        .collect::<String>();
    if sanitized.is_empty() {
        sanitized.push_str("RTSP worker failed");
    }
    sanitized
}

#[cfg(feature = "rtsp")]
fn worker_failure(code: ErrorCode, message: &str) -> CameraError {
    if code == ErrorCode::BackendError {
        backend_error(message.to_owned())
    } else {
        CameraError::rejected(code, message.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, VecDeque};
    use std::net::IpAddr;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;

    #[cfg(feature = "rtsp")]
    use crate::backend::onvif::{SystemNonceSource, SystemOnvifClock, SystemResolver};

    /// The shared decode gate, sized exactly as production sizes it.
    ///
    /// Production builds this once per component in `BackendRuntimeContext::new` from
    /// `limits.maxConcurrentCaptures`; a test that invented its own width would not be testing the
    /// bound the component actually runs with.
    #[cfg(feature = "rtsp")]
    fn test_decode_gate() -> Arc<Semaphore> {
        Arc::new(Semaphore::new(
            crate::config::LimitsConfig::default().max_concurrent_captures,
        ))
    }

    /// A warm worker must stop decoding once the capture that asked for a frame has been served.
    ///
    /// It used to decode every access unit for the whole 30 s warm window -- 25 frames a second per
    /// camera, every one of them taking a permit from the component-wide decode gate, and every one
    /// of them discarded, because the next capture will only accept a frame ingested after that
    /// capture arrived. A dozen warm cameras could starve the capture path with frames nobody had
    /// asked for.
    #[test]
    fn a_warm_worker_decodes_only_while_a_capture_is_waiting() {
        let requested_at = Instant::now();

        assert!(
            capture_is_waiting(requested_at, None),
            "a worker that has published nothing owes its caller a frame"
        );
        assert!(
            !capture_is_waiting(requested_at, Some(requested_at + Duration::from_millis(1))),
            "the capture has been served; every further decode is waste"
        );

        let asked_again_at = requested_at + Duration::from_secs(5);

        assert!(
            capture_is_waiting(
                asked_again_at,
                Some(requested_at + Duration::from_millis(1))
            ),
            "a fresh capture must resume decoding"
        );
    }

    #[derive(Debug)]
    struct SequenceResolver(Mutex<VecDeque<Vec<IpAddr>>>);

    impl SequenceResolver {
        fn new(answers: impl IntoIterator<Item = Vec<IpAddr>>) -> Self {
            Self(Mutex::new(answers.into_iter().collect()))
        }
    }

    #[async_trait]
    impl OnvifResolver for SequenceResolver {
        async fn resolve(&self, _host: &str, _port: u16) -> Result<Vec<IpAddr>> {
            self.0
                .lock()
                .expect("resolver lock")
                .pop_front()
                .ok_or_else(|| backend_error("test resolver exhausted"))
        }
    }

    fn address(value: &str) -> IpAddr {
        value.parse().expect("test IP")
    }

    fn anchor() -> RtspNetworkAnchor {
        RtspNetworkAnchor {
            configured_host: "camera.test".to_owned(),
            endpoint_addresses: BTreeSet::from([address("10.0.0.2")]),
            allowed_hosts: BTreeSet::from(["media.test".to_owned()]),
            allowed_cidrs: vec!["10.0.0.0/24".parse().expect("test CIDR")],
        }
    }

    /// Runs only inside `simulators/rtsp_validation.Dockerfile` on the Compose
    /// network. Invoke once with each pinned MediaMTX H.264/H.265 path through
    /// `CAMERA_ADAPTER_RTSP_URI`; ordinary unit tests stay fully hermetic.
    #[cfg(feature = "rtsp")]
    #[tokio::test]
    #[ignore = "requires the pinned MediaMTX simulator on the Compose network"]
    async fn pinned_mediamtx_produces_a_complete_rgb_frame() {
        let stream_uri = std::env::var("CAMERA_ADAPTER_RTSP_URI")
            .expect("live RTSP test requires CAMERA_ADAPTER_RTSP_URI");
        let url = Url::parse(&stream_uri).expect("test URI must be valid");
        let host = url.host_str().expect("test URI has a host").to_owned();
        let port = url.port_or_known_default().expect("test URI has a port");
        let resolver: Arc<dyn OnvifResolver> = Arc::new(SystemResolver);
        let addresses = resolver
            .resolve(&host, port)
            .await
            .expect("Compose service resolves");
        // The fixture's deliberately efficient H.265 IDR can exceed the
        // production default ratio. It remains within the documented, bounded
        // operator-configurable maximum and exercises that supported path.
        let security = SecurityConfig {
            max_decompression_ratio: 1_000,
            ..SecurityConfig::default()
        };
        let mut controller = RtspCaptureController::establish(
            RtspControllerConfig {
                stream_uri,
                anchor: RtspNetworkAnchor {
                    configured_host: host,
                    endpoint_addresses: addresses.into_iter().collect(),
                    allowed_hosts: BTreeSet::new(),
                    allowed_cidrs: Vec::new(),
                },
                resolver,
                credentials: None,
                nonce_source: Arc::new(SystemNonceSource),
                private_ca: None,
                verify_hostname: true,
                allow_insecure: true,
                authentication_mode: AuthenticationMode::Auto,
                security,
                session_policy: RtspSessionPolicy::OnDemand,
                maximum_frame_bytes: 1_048_576,
                clock: Arc::new(SystemOnvifClock),
                decode_gate: test_decode_gate(),
            },
            Instant::now() + Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await
        .expect("establish pinned RTSP controller");
        let frame = controller
            .capture(
                1_048_576,
                Duration::from_secs(15),
                &CancellationToken::new(),
            )
            .await
            .expect("complete decoded RTSP frame");
        assert_eq!(frame.capture_mode, CaptureMode::RtspFrame);
        assert_eq!(frame.pixel_format, PixelFormat::Rgb8);
        assert_eq!((frame.width, frame.height), (320, 240));
        assert_eq!(frame.bytes.len(), 320 * 240 * 3);
        controller
            .close(Instant::now() + Duration::from_secs(2))
            .await;
    }

    /// Exercises the retained-session path against the pinned simulator. It stays ignored because
    /// ordinary unit-test hosts do not provide GStreamer or a real RTSP endpoint.
    #[cfg(feature = "rtsp")]
    #[tokio::test]
    #[ignore = "requires the pinned MediaMTX simulator on the Compose network"]
    async fn pinned_mediamtx_warm_session_produces_two_complete_frames() {
        let stream_uri = std::env::var("CAMERA_ADAPTER_RTSP_URI")
            .expect("live RTSP test requires CAMERA_ADAPTER_RTSP_URI");
        let url = Url::parse(&stream_uri).expect("test URI must be valid");
        let host = url.host_str().expect("test URI has a host").to_owned();
        let port = url.port_or_known_default().expect("test URI has a port");
        let resolver: Arc<dyn OnvifResolver> = Arc::new(SystemResolver);
        let addresses = resolver
            .resolve(&host, port)
            .await
            .expect("Compose service resolves");
        let mut controller = RtspCaptureController::establish(
            RtspControllerConfig {
                stream_uri,
                anchor: RtspNetworkAnchor {
                    configured_host: host,
                    endpoint_addresses: addresses.into_iter().collect(),
                    allowed_hosts: BTreeSet::new(),
                    allowed_cidrs: Vec::new(),
                },
                resolver,
                credentials: None,
                nonce_source: Arc::new(SystemNonceSource),
                private_ca: None,
                verify_hostname: true,
                allow_insecure: true,
                authentication_mode: AuthenticationMode::Auto,
                security: SecurityConfig {
                    max_decompression_ratio: 1_000,
                    ..SecurityConfig::default()
                },
                session_policy: RtspSessionPolicy::Warm,
                maximum_frame_bytes: 1_048_576,
                clock: Arc::new(SystemOnvifClock),
                decode_gate: test_decode_gate(),
            },
            Instant::now() + Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await
        .expect("establish pinned warm RTSP controller");
        let first = controller
            .capture(
                1_048_576,
                Duration::from_secs(15),
                &CancellationToken::new(),
            )
            .await
            .expect("first complete decoded RTSP frame");
        let second = controller
            .capture(
                1_048_576,
                Duration::from_secs(15),
                &CancellationToken::new(),
            )
            .await
            .expect("second complete decoded RTSP frame from retained session");
        assert_eq!(first.capture_mode, CaptureMode::RtspFrame);
        assert_eq!(second.capture_mode, CaptureMode::RtspFrame);
        assert_eq!(first.pixel_format, PixelFormat::Rgb8);
        assert_eq!(second.pixel_format, PixelFormat::Rgb8);
        assert_eq!((first.width, first.height), (320, 240));
        assert_eq!((second.width, second.height), (320, 240));
        assert_eq!(first.bytes.len(), 320 * 240 * 3);
        assert_eq!(second.bytes.len(), 320 * 240 * 3);
        controller
            .close(Instant::now() + Duration::from_secs(2))
            .await;
    }

    #[tokio::test]
    async fn rtsp_uri_repin_rejects_dns_address_set_change() {
        let resolver =
            SequenceResolver::new([vec![address("10.0.0.2")], vec![address("10.0.0.3")]]);
        let cancellation = CancellationToken::new();
        let (policy, pinned) = RtspUriPolicy::establish(
            "rtsp://camera.test:554/live/main",
            anchor(),
            true,
            &resolver,
            Instant::now() + std::time::Duration::from_secs(1),
            &cancellation,
        )
        .await
        .expect("established URI");
        assert_eq!(pinned.socket_address(), "10.0.0.2:554".parse().unwrap());
        let error = policy
            .pin(
                &resolver,
                Instant::now() + std::time::Duration::from_secs(1),
                &cancellation,
            )
            .await
            .expect_err("rebind must fail");
        assert_eq!(error.code(), ErrorCode::BackendError);
        assert!(!error.to_string().contains("/live/main"));
    }

    #[tokio::test]
    async fn rtsp_uri_rejects_userinfo_unlisted_host_and_ungated_plaintext() {
        let resolver = SequenceResolver::new([vec![address("10.0.0.2")]]);
        let cancellation = CancellationToken::new();
        for uri in [
            "rtsp://user:secret@camera.test/live",
            "rtsp://hostile.test/live",
        ] {
            assert!(
                RtspUriPolicy::establish(
                    uri,
                    anchor(),
                    true,
                    &resolver,
                    Instant::now() + std::time::Duration::from_secs(1),
                    &cancellation,
                )
                .await
                .is_err()
            );
        }
        assert!(
            RtspUriPolicy::establish(
                "rtsp://camera.test/live",
                anchor(),
                false,
                &resolver,
                Instant::now() + std::time::Duration::from_secs(1),
                &cancellation,
            )
            .await
            .is_err()
        );
    }

    async fn established_policy() -> RtspUriPolicy {
        let resolver = SequenceResolver::new([vec![address("10.0.0.2")]]);
        RtspUriPolicy::establish(
            "rtsp://camera.test:554/live/main",
            anchor(),
            true,
            &resolver,
            Instant::now() + std::time::Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .expect("test policy")
        .0
    }

    #[tokio::test]
    async fn sdp_selects_supported_video_and_validates_control_origin() {
        let policy = established_policy().await;
        let sdp = b"v=0\r\nm=audio 0 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1;sprop-parameter-sets=Z0IAH+KQCgC3YC3AQEBpB4kRUA==,aM4xUg==\r\na=control:trackID=1\r\n";
        let track = parse_sdp_track(sdp, &policy.url, &policy).expect("H264 track");
        assert_eq!(track.codec, RtspCodec::H264);
        assert_eq!(track.payload_type, 96);
        assert_eq!(track.clock_rate, 90_000);
        assert_eq!(track.control_uri.host_str(), Some("camera.test"));
        assert_eq!(track.bootstrap_nals.len(), 2);

        let hostile = b"v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=control:rtsp://hostile.test/track\r\n";
        assert!(parse_sdp_track(hostile, &policy.url, &policy).is_err());
    }

    #[test]
    fn response_parser_rejects_duplicate_length_folding_and_non_ascii() {
        let duplicate = b"RTSP/1.0 200 OK\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\n";
        assert!(parse_rtsp_response_head(duplicate, 4096).is_err());
        let folded = b"RTSP/1.0 200 OK\r\nX-Test: one\r\n two\r\n\r\n";
        assert!(parse_rtsp_response_head(folded, 4096).is_err());
        let non_ascii = b"RTSP/1.0 200 OK\r\nX-Test: \xff\r\n\r\n";
        assert!(parse_rtsp_response_head(non_ascii, 4096).is_err());
        let valid = b"RTSP/1.0 200 OK\r\nCSeq: 7\r\nContent-Length: 3\r\n\r\n";
        let (status, headers, length) = parse_rtsp_response_head(valid, 4096).unwrap();
        assert_eq!(status, 200);
        assert_eq!(length, 3);
        assert_eq!(headers["cseq"], ["7"]);
    }

    #[test]
    fn singleton_response_headers_reject_duplicate_cseq_transport_and_session() {
        for (name, wire_name) in [
            ("cseq", "CSeq"),
            ("content-type", "Content-Type"),
            ("content-base", "Content-Base"),
            ("transport", "Transport"),
            ("session", "Session"),
        ] {
            let head = format!("RTSP/1.0 200 OK\r\n{wire_name}: one\r\n{wire_name}: two\r\n\r\n");
            let (status, headers, length) =
                parse_rtsp_response_head(head.as_bytes(), 4096).expect("parse test response");
            let response = RtspResponse {
                status,
                headers,
                body: vec![0_u8; length],
            };
            assert!(
                response.single_header(name).is_err(),
                "duplicate {wire_name} must be rejected"
            );
        }
    }

    #[test]
    fn required_digest_allows_public_options_then_challenged_describe() {
        let mut authentication_established = false;
        authentication_established |=
            read_only_success_establishes_authentication(AuthenticationMode::HttpDigest, false);
        assert!(
            !authentication_established,
            "public OPTIONS is not proof of Digest"
        );

        authentication_established |=
            read_only_success_establishes_authentication(AuthenticationMode::HttpDigest, true);
        assert!(
            authentication_established,
            "a successful challenged DESCRIBE establishes Digest before SETUP"
        );
    }

    #[test]
    fn required_digest_never_challenged_remains_unestablished_before_setup() {
        let options =
            read_only_success_establishes_authentication(AuthenticationMode::HttpDigest, false);
        let describe =
            read_only_success_establishes_authentication(AuthenticationMode::HttpDigest, false);
        assert!(!(options || describe));
    }

    fn rtp(
        sequence: u16,
        timestamp: u32,
        marker: bool,
        payload_type: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut bytes = vec![0x80, payload_type | if marker { 0x80 } else { 0 }];
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.extend_from_slice(&timestamp.to_be_bytes());
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn track(codec: RtspCodec) -> RtspTrack {
        RtspTrack {
            control_uri: Url::parse("rtsp://camera.test/track").unwrap(),
            codec,
            payload_type: 96,
            clock_rate: 90_000,
            bootstrap_nals: Vec::new(),
        }
    }

    fn track_with_bootstrap(codec: RtspCodec, bootstrap_nals: Vec<Vec<u8>>) -> RtspTrack {
        RtspTrack {
            bootstrap_nals,
            ..track(codec)
        }
    }

    #[test]
    fn h264_bootstrap_waits_for_vcl_and_covers_fragmented_first_vcl() {
        let track = track_with_bootstrap(RtspCodec::H264, vec![vec![0x68, 0xaa]]);
        let mut assembler = AccessUnitAssembler::new(&track, 4096).unwrap();
        let parameter_only = rtp(1, 10, true, 96, &[0x68, 0x01]);
        assert!(
            assembler
                .push(parse_rtp_packet(&parameter_only).unwrap())
                .unwrap()
                .is_none()
        );
        assert!(assembler.bootstrap_pending);
        let vcl = rtp(2, 11, true, 96, &[0x65, 0x02]);
        let complete = assembler
            .push(parse_rtp_packet(&vcl).unwrap())
            .unwrap()
            .expect("complete H.264 VCL");
        assert_eq!(
            complete.bytes.as_ref(),
            &[0, 0, 0, 1, 0x68, 0xaa, 0, 0, 0, 1, 0x65, 0x02]
        );
        assert!(!assembler.bootstrap_pending);

        let mut fragmented = AccessUnitAssembler::new(&track, 4096).unwrap();
        let start = rtp(3, 12, false, 96, &[0x7c, 0x85, 0x03]);
        let end = rtp(4, 12, true, 96, &[0x7c, 0x45, 0x04]);
        fragmented.push(parse_rtp_packet(&start).unwrap()).unwrap();
        let complete = fragmented
            .push(parse_rtp_packet(&end).unwrap())
            .unwrap()
            .expect("fragmented H.264 VCL");
        assert_eq!(
            complete.bytes.as_ref(),
            &[0, 0, 0, 1, 0x68, 0xaa, 0, 0, 0, 1, 0x65, 0x03, 0x04]
        );
    }

    #[test]
    fn h265_bootstrap_waits_for_vcl_and_covers_fragmented_first_vcl() {
        let track = track_with_bootstrap(RtspCodec::H265, vec![vec![68, 1, 0xaa]]);
        let mut assembler = AccessUnitAssembler::new(&track, 4096).unwrap();
        let parameter_only = rtp(1, 20, true, 96, &[64, 1, 0x01]);
        assert!(
            assembler
                .push(parse_rtp_packet(&parameter_only).unwrap())
                .unwrap()
                .is_none()
        );
        assert!(assembler.bootstrap_pending);
        let vcl = rtp(2, 21, true, 96, &[19 << 1, 1, 0x02]);
        let complete = assembler
            .push(parse_rtp_packet(&vcl).unwrap())
            .unwrap()
            .expect("complete H.265 VCL");
        assert_eq!(
            complete.bytes.as_ref(),
            &[0, 0, 0, 1, 68, 1, 0xaa, 0, 0, 0, 1, 19 << 1, 1, 0x02]
        );
        assert!(!assembler.bootstrap_pending);

        let mut fragmented = AccessUnitAssembler::new(&track, 4096).unwrap();
        let start = rtp(3, 22, false, 96, &[49 << 1, 1, 0x80 | 19, 0x03]);
        let end = rtp(4, 22, true, 96, &[49 << 1, 1, 0x40 | 19, 0x04]);
        fragmented.push(parse_rtp_packet(&start).unwrap()).unwrap();
        let complete = fragmented
            .push(parse_rtp_packet(&end).unwrap())
            .unwrap()
            .expect("fragmented H.265 VCL");
        assert_eq!(
            complete.bytes.as_ref(),
            &[0, 0, 0, 1, 68, 1, 0xaa, 0, 0, 0, 1, 19 << 1, 1, 0x03, 0x04,]
        );
    }

    #[test]
    fn h264_fu_a_requires_complete_contiguous_frame() {
        let mut assembler = AccessUnitAssembler::new(&track(RtspCodec::H264), 4096).unwrap();
        let start = rtp(10, 77, false, 96, &[0x7c, 0x85, 1, 2]);
        let end = rtp(11, 77, true, 96, &[0x7c, 0x45, 3, 4]);
        assert!(
            assembler
                .push(parse_rtp_packet(&start).unwrap())
                .unwrap()
                .is_none()
        );
        let unit = assembler
            .push(parse_rtp_packet(&end).unwrap())
            .unwrap()
            .expect("complete IDR");
        assert_eq!(unit.rtp_timestamp, 77);
        assert_eq!(unit.bytes.as_ref(), &[0, 0, 0, 1, 0x65, 1, 2, 3, 4]);

        let mut gapped = AccessUnitAssembler::new(&track(RtspCodec::H264), 4096).unwrap();
        gapped.push(parse_rtp_packet(&start).unwrap()).unwrap();
        let wrong = rtp(12, 77, true, 96, &[0x7c, 0x45, 3]);
        assert!(gapped.push(parse_rtp_packet(&wrong).unwrap()).is_err());
    }

    #[test]
    fn h265_fu_reconstructs_header_and_enforces_bound() {
        let mut assembler = AccessUnitAssembler::new(&track(RtspCodec::H265), 4096).unwrap();
        let start = rtp(1, 99, false, 96, &[49 << 1, 1, 0x80 | 19, 9]);
        let end = rtp(2, 99, true, 96, &[49 << 1, 1, 0x40 | 19, 8]);
        assembler.push(parse_rtp_packet(&start).unwrap()).unwrap();
        let unit = assembler
            .push(parse_rtp_packet(&end).unwrap())
            .unwrap()
            .expect("complete H265 frame");
        assert_eq!(unit.bytes.as_ref(), &[0, 0, 0, 1, 19 << 1, 1, 9, 8]);
        assert_eq!(unit.compressed_bytes, 8);

        let mut bounded = AccessUnitAssembler::new(&track(RtspCodec::H265), 6).unwrap();
        assert!(bounded.push(parse_rtp_packet(&start).unwrap()).is_err());
    }

    #[test]
    fn rtp_parser_rejects_truncated_extension_and_padding() {
        let mut extension = rtp(1, 1, true, 96, &[1]);
        extension[0] |= 0x10;
        assert!(parse_rtp_packet(&extension).is_err());
        let mut padding = rtp(1, 1, true, 96, &[1]);
        padding[0] |= 0x20;
        *padding.last_mut().unwrap() = 99;
        assert!(parse_rtp_packet(&padding).is_err());
    }

    #[test]
    fn codec_parameters_preserve_bootstrap_order_and_reject_unsupported_packetization() {
        let h264 = parse_codec_fmtp(
            RtspCodec::H264,
            Some(
                "packetization-mode=1; sprop-parameter-sets=Z0IAH+KQCgC3YC3AQEBpB4kRUA==,aM4xUg==",
            ),
        )
        .expect("supported H.264 bootstrap");
        assert_eq!(h264.len(), 2);
        assert!(h264.iter().all(|nal| !nal.is_empty()));

        let h265 = parse_codec_fmtp(
            RtspCodec::H265,
            Some("sprop-max-don-diff=0; sprop-vps=AQ==; sprop-sps=Ag==; sprop-pps=Aw=="),
        )
        .expect("supported H.265 bootstrap");
        assert_eq!(h265, vec![vec![1], vec![2], vec![3]]);

        for (codec, fmtp, code) in [
            (
                RtspCodec::H264,
                "packetization-mode=2",
                ErrorCode::UnsupportedCapability,
            ),
            (
                RtspCodec::H265,
                "sprop-max-don-diff=1",
                ErrorCode::UnsupportedCapability,
            ),
            (
                RtspCodec::H264,
                "sprop-parameter-sets=***",
                ErrorCode::BackendError,
            ),
            (RtspCodec::H264, "broken-parameter", ErrorCode::BackendError),
            (RtspCodec::H264, "x=1; x=2", ErrorCode::BackendError),
            (
                RtspCodec::H264,
                "sprop-parameter-sets=",
                ErrorCode::BackendError,
            ),
        ] {
            assert_eq!(
                parse_codec_fmtp(codec, Some(fmtp))
                    .expect_err("unsupported or malformed codec parameter")
                    .code(),
                code
            );
        }
    }

    #[tokio::test]
    async fn sdp_prefers_the_first_supported_video_track_and_rejects_bad_video_contracts() {
        let policy = established_policy().await;
        let sdp = b"v=0\r\nm=audio 0 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\nm=video 0 RTP/AVP 97 98\r\na=rtpmap:97 VP8/90000\r\na=rtpmap:98 HEVC/90000\r\na=fmtp:98 sprop-max-don-diff=0;sprop-vps=AQ==;sprop-sps=Ag==;sprop-pps=Aw==\r\na=control:trackID=2\r\n";
        let track = parse_sdp_track(sdp, &policy.url, &policy).expect("HEVC track is supported");
        assert_eq!(track.codec, RtspCodec::H265);
        assert_eq!(track.payload_type, 98);
        assert_eq!(track.bootstrap_nals, vec![vec![1], vec![2], vec![3]]);

        let bad_clock =
            b"v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/8000\r\na=control:trackID=1\r\n";
        assert_eq!(
            parse_sdp_track(bad_clock, &policy.url, &policy)
                .expect_err("H.264 requires the RFC clock rate")
                .code(),
            ErrorCode::UnsupportedCapability
        );
        let duplicate_mapping = b"v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=rtpmap:96 H264/90000\r\na=control:trackID=1\r\n";
        assert!(parse_sdp_track(duplicate_mapping, &policy.url, &policy).is_err());
    }

    #[tokio::test]
    async fn uri_policy_rejects_malformed_origins_addresses_and_cancelled_resolution() {
        for candidate in [
            "",
            "relative-path",
            "https://camera.test/live",
            "rtsps://camera.test/live#fragment",
            "rtsp://camera.test:0/live",
            "rtsp://camera.test/live\nnext",
        ] {
            assert!(
                parse_rtsp_uri(candidate, true).is_err(),
                "{candidate:?} must not be an accepted RTSP origin"
            );
        }
        let (secure, secure_host, secure_port) =
            parse_rtsp_uri("rtsps://CAMERA.test/live", false).expect("secure origin");
        assert_eq!(secure.scheme(), "rtsps");
        assert_eq!(secure_host, "camera.test");
        assert_eq!(secure_port, 322);
        assert!(parse_rtsp_uri("rtsp://camera.test/live", false).is_err());

        assert!(validate_rtsp_host("hostile.test", &anchor()).is_err());
        validate_rtsp_host("media.test", &anchor()).expect("explicitly allowed hostname");
        assert!(validate_rtsp_addresses("camera.test", &[], &anchor()).is_err());
        assert!(validate_rtsp_addresses("media.test", &[address("127.0.0.1")], &anchor()).is_err());
        validate_rtsp_addresses("media.test", &[address("10.0.0.3")], &anchor())
            .expect("allowed CIDR address");

        let resolver = SequenceResolver::new([vec![address("10.0.0.2")]]);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert_eq!(
            resolve_rtsp_bounded(
                &resolver,
                "camera.test",
                554,
                Instant::now() + Duration::from_secs(1),
                &cancelled,
            )
            .await
            .expect_err("cancelled resolution must not run")
            .code(),
            ErrorCode::CaptureCancelled
        );
        assert_eq!(
            resolve_rtsp_bounded(
                &resolver,
                "camera.test",
                554,
                Instant::now() - Duration::from_millis(1),
                &CancellationToken::new(),
            )
            .await
            .expect_err("elapsed resolution deadline")
            .code(),
            ErrorCode::CaptureTimeout
        );
    }

    #[tokio::test]
    async fn pinned_uri_exposes_only_approved_origin_fields_and_repinning_is_stable() {
        let resolver =
            SequenceResolver::new([vec![address("10.0.0.2")], vec![address("10.0.0.2")]]);
        let (policy, pinned) = RtspUriPolicy::establish(
            "rtsps://camera.test/live/private?token=redacted",
            anchor(),
            false,
            &resolver,
            Instant::now() + Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .expect("approved secure origin");
        assert_eq!(pinned.host(), "camera.test");
        assert_eq!(pinned.url().scheme(), "rtsps");
        assert!(pinned.is_tls());
        assert_eq!(pinned.addresses, BTreeSet::from([address("10.0.0.2")]));
        let debug = format!("{pinned:?}");
        assert!(debug.contains("camera.test"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("token=redacted"));

        let repinned = policy
            .pin(
                &resolver,
                Instant::now() + Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .expect("unchanged address set remains pinned");
        assert_eq!(repinned.socket_address(), "10.0.0.2:322".parse().unwrap());
        assert_eq!(RtspCodec::H264.encoding_name(), "H264");
        assert_eq!(RtspCodec::H265.encoding_name(), "H265");
        assert!(parse_rtsp_uri("rtsp:opaque", true).is_err());
    }

    #[tokio::test]
    async fn sdp_and_codec_boundaries_fail_closed_for_malformed_camera_metadata() {
        let policy = established_policy().await;
        for sdp in [
            b"".as_slice(),
            b"\xff",
            b"v=0\r\nm=video 0 RTP/AVP\r\n",
            b"v=0\r\n\x01",
            b"v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000/1\r\na=control:track\r\n",
            b"v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=control:one\r\na=control:two\r\n",
            b"v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 VP8/90000\r\na=control:track\r\n",
            b"v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 x=1\r\na=fmtp:96 x=2\r\na=control:track\r\n",
        ] {
            assert!(parse_sdp_track(sdp, &policy.url, &policy).is_err());
        }
        assert!(parse_codec_fmtp(RtspCodec::H265, Some("sprop-vps=AQ==")).is_ok());
        assert!(
            parse_codec_fmtp(
                RtspCodec::H264,
                Some(&format!("x={}", "a".repeat(MAX_SDP_LINE_BYTES + 1))),
            )
            .is_err()
        );
    }

    #[test]
    fn sdp_response_and_rtp_parsers_reject_protocol_boundary_violations() {
        for response in [
            b"RTSP/1.0 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice(),
            b"RTSP/1.0 200 OK\r\nContent-Length: nope\r\n\r\n",
            b"RTSP/1.0 600 Out of range\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n\r\n",
            b"RTSP/1.0 200 OK\r\nBad Header: one\r\n\r\n",
        ] {
            assert!(parse_rtsp_response_head(response, 4096).is_err());
        }
        let oversized = format!(
            "RTSP/1.0 200 OK\r\nContent-Length: {}\r\n\r\n",
            MAX_RTSP_BODY_BYTES + 1
        );
        assert!(parse_rtsp_response_head(oversized.as_bytes(), 4096).is_err());

        let no_payload = rtp(1, 1, true, 96, &[]);
        let bad_version = [0x40, 96, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1];
        let mut truncated_csrc = rtp(1, 1, true, 96, &[1]);
        truncated_csrc[0] |= 0x01;
        let mut truncated_extension_body = rtp(1, 1, true, 96, &[1]);
        truncated_extension_body[0] |= 0x10;
        truncated_extension_body.splice(12..12, [0, 0, 0, 1]);
        for packet in [
            &no_payload,
            &bad_version.to_vec(),
            &truncated_csrc,
            &truncated_extension_body,
        ] {
            assert!(parse_rtp_packet(packet).is_err());
        }
    }

    #[test]
    fn rtp_parser_handles_valid_csrc_extension_and_padding_without_exposing_them_as_media() {
        let mut bytes = rtp(33, 44, true, 96, &[9, 8]);
        bytes[0] |= 0x10 | 0x20 | 0x01;
        bytes.splice(12..12, [0, 0, 0, 7]);
        bytes.splice(16..16, [0xbe, 0xde, 0, 1, 1, 2, 3, 4]);
        bytes.extend_from_slice(&[0, 2]);

        let packet = parse_rtp_packet(&bytes).expect("well-formed interleaved RTP packet");
        assert!(packet.marker);
        assert_eq!(packet.payload_type, 96);
        assert_eq!(packet.sequence, 33);
        assert_eq!(packet.timestamp, 44);
        assert_eq!(packet.payload, &[9, 8]);
    }

    #[test]
    fn h264_aggregation_and_timestamp_rollover_discard_incomplete_units_safely() {
        let mut assembler = AccessUnitAssembler::new(&track(RtspCodec::H264), 4096).unwrap();
        let stap_a = rtp(1, 11, true, 96, &[0x78, 0, 2, 0x68, 1, 0, 2, 0x65, 2]);
        let complete = assembler
            .push(parse_rtp_packet(&stap_a).unwrap())
            .expect("H.264 aggregation packet")
            .expect("marker completes an access unit");
        assert_eq!(
            complete.bytes.as_ref(),
            &[0, 0, 0, 1, 0x68, 1, 0, 0, 0, 1, 0x65, 2]
        );

        let partial = rtp(2, 12, false, 96, &[0x65, 3]);
        assert!(
            assembler
                .push(parse_rtp_packet(&partial).unwrap())
                .unwrap()
                .is_none()
        );
        let next_timestamp = rtp(3, 13, true, 96, &[0x65, 4]);
        assert!(
            assembler
                .push(parse_rtp_packet(&next_timestamp).unwrap())
                .expect("timestamp change discards prior incomplete unit")
                .is_some()
        );
        assert_eq!(assembler.incomplete_units, 1);

        let wrong_payload = rtp(4, 14, true, 97, &[0x65, 5]);
        assert!(
            assembler
                .push(parse_rtp_packet(&wrong_payload).unwrap())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn h265_aggregation_sps_and_packetization_failures_are_bounded() {
        let sps = h265_sps(1920, 1088, (0, 0, 0, 4));
        assert_eq!(parse_h265_sps_dimensions(&sps).unwrap(), (1920, 1080));

        let mut assembler = AccessUnitAssembler::new(&track(RtspCodec::H265), 4096).unwrap();
        let mut access_point = vec![48 << 1, 1];
        access_point.extend_from_slice(&(u16::try_from(sps.len()).unwrap()).to_be_bytes());
        access_point.extend_from_slice(&sps);
        access_point.extend_from_slice(&[0, 3, 19 << 1, 1, 0xaa]);
        let packet = rtp(1, 25, true, 96, &access_point);
        let unit = assembler
            .push(parse_rtp_packet(&packet).unwrap())
            .expect("H.265 aggregation packet")
            .expect("IDR in aggregation packet completes the access unit");
        assert_eq!(unit.dimensions, Some((1920, 1080)));
        assert_eq!(unit.rtp_timestamp, 25);

        for payload in [
            vec![48 << 1, 1, 0],
            vec![48 << 1, 1, 0, 1, 19 << 1],
            vec![49 << 1, 1, 0x80 | 19],
            vec![50 << 1, 1, 0xaa],
        ] {
            let packet = rtp(2, 26, true, 96, &payload);
            assert!(
                AccessUnitAssembler::new(&track(RtspCodec::H265), 4096)
                    .unwrap()
                    .push(parse_rtp_packet(&packet).unwrap())
                    .is_err(),
                "{payload:?} must not be accepted"
            );
        }
        assert!(parse_h265_sps_dimensions(&[19 << 1, 1, 0, 0, 0]).is_err());
    }

    #[test]
    fn fragmented_h264_and_h265_reject_protocol_violations_without_completing_frames() {
        assert_eq!(
            AccessUnitAssembler::new(&track(RtspCodec::H264), 0)
                .expect_err("zero compressed-frame budget")
                .code(),
            ErrorCode::ResourceLimit
        );
        for payload in [
            vec![0],
            vec![24, 0],
            vec![24, 0, 0],
            vec![28, 0x85],
            vec![28, 0xc5, 1],
        ] {
            let packet = rtp(1, 1, true, 96, &payload);
            assert!(
                AccessUnitAssembler::new(&track(RtspCodec::H264), 4096)
                    .unwrap()
                    .push(parse_rtp_packet(&packet).unwrap())
                    .is_err(),
                "{payload:?} must not be accepted"
            );
        }
        let h264_start = rtp(1, 1, false, 96, &[28, 0x85, 1]);
        let h264_repeat = rtp(2, 1, false, 96, &[28, 0x85, 2]);
        let mut h264 = AccessUnitAssembler::new(&track(RtspCodec::H264), 4096).unwrap();
        h264.push(parse_rtp_packet(&h264_start).unwrap()).unwrap();
        assert!(h264.push(parse_rtp_packet(&h264_repeat).unwrap()).is_err());

        let h265_start = rtp(1, 1, false, 96, &[49 << 1, 1, 0x80 | 19, 1]);
        let h265_repeat = rtp(2, 1, false, 96, &[49 << 1, 1, 0x80 | 19, 2]);
        let mut h265 = AccessUnitAssembler::new(&track(RtspCodec::H265), 4096).unwrap();
        h265.push(parse_rtp_packet(&h265_start).unwrap()).unwrap();
        assert!(h265.push(parse_rtp_packet(&h265_repeat).unwrap()).is_err());
        let continuation = rtp(3, 1, true, 96, &[49 << 1, 1, 19, 3]);
        assert!(
            AccessUnitAssembler::new(&track(RtspCodec::H265), 4096)
                .unwrap()
                .push(parse_rtp_packet(&continuation).unwrap())
                .is_err()
        );
    }

    #[test]
    fn rbsp_reader_and_dimension_bounds_reject_truncation_without_wrapping() {
        let escaped = RbspBitReader::from_escaped(&[0, 0, 3, 0x80]).expect("escaped RBSP");
        assert_eq!(escaped.bytes, vec![0, 0, 0x80]);

        let mut unsigned = RbspBitReader::from_escaped(&[0b0110_0000]).unwrap();
        assert_eq!(unsigned.unsigned_exp_golomb().unwrap(), 2);
        let mut signed = RbspBitReader::from_escaped(&[0b0010_0000]).unwrap();
        assert_eq!(signed.signed_exp_golomb().unwrap(), 2);
        assert!(signed.bits(65).is_err());
        assert!(RbspBitReader::from_escaped(&[]).is_err());
        assert!(validate_dimensions(0, 1).is_err());
        assert!(validate_dimensions(65_536, 1).is_err());
        assert_eq!(validate_dimensions(1920, 1080).unwrap(), (1920, 1080));
    }

    #[test]
    fn h264_high_profile_sps_applies_chroma_crop_geometry() {
        let mut bits = TestBits::default();
        bits.byte(100); // High profile
        bits.byte(0);
        bits.byte(40);
        bits.ue(0); // SPS id
        bits.ue(1); // 4:2:0 chroma
        bits.ue(0);
        bits.ue(0);
        bits.bit(false);
        bits.bit(false); // no scaling matrices
        bits.ue(0);
        bits.ue(0);
        bits.ue(0);
        bits.ue(0);
        bits.bit(false);
        bits.ue(119); // 120 macroblocks = 1920
        bits.ue(67); // 68 map units = 1088
        bits.bit(true);
        bits.bit(true);
        bits.bit(true);
        bits.ue(0);
        bits.ue(0);
        bits.ue(0);
        bits.ue(4); // 4 * crop-unit-y(2) = 8 => 1080
        let mut nal = vec![0x67];
        nal.extend(bits.finish());
        assert_eq!(parse_h264_sps_dimensions(&nal).unwrap(), (1920, 1080));
    }

    fn h265_sps(width: u32, coded_height: u32, crop: (u32, u32, u32, u32)) -> Vec<u8> {
        let mut bits = TestBits::default();
        bits.zeros(4); // sps_video_parameter_set_id
        bits.zeros(3); // sps_max_sub_layers_minus1
        bits.bit(true); // sps_temporal_id_nesting_flag
        bits.zeros(96); // general profile tier level
        bits.ue(0); // sps_seq_parameter_set_id
        bits.ue(1); // 4:2:0 chroma
        bits.ue(width);
        bits.ue(coded_height);
        bits.bit(true); // conformance window
        bits.ue(crop.0);
        bits.ue(crop.1);
        bits.ue(crop.2);
        bits.ue(crop.3);
        let mut nal = vec![33 << 1, 1];
        nal.extend(bits.finish());
        nal
    }

    #[derive(Default)]
    struct TestBits {
        bytes: Vec<u8>,
        bit: usize,
    }
    impl TestBits {
        fn zeros(&mut self, count: usize) {
            for _ in 0..count {
                self.bit(false);
            }
        }

        fn bit(&mut self, value: bool) {
            if self.bit % 8 == 0 {
                self.bytes.push(0);
            }
            if value {
                let index = self.bytes.len() - 1;
                self.bytes[index] |= 1 << (7 - (self.bit % 8));
            }
            self.bit += 1;
        }
        fn byte(&mut self, value: u8) {
            for shift in (0..8).rev() {
                self.bit(value & (1 << shift) != 0);
            }
        }
        fn ue(&mut self, value: u32) {
            let code = value + 1;
            let width = 32 - code.leading_zeros();
            for _ in 1..width {
                self.bit(false);
            }
            for shift in (0..width).rev() {
                self.bit(code & (1 << shift) != 0);
            }
        }
        fn finish(mut self) -> Vec<u8> {
            self.bit(true);
            while self.bit % 8 != 0 {
                self.bit(false);
            }
            self.bytes
        }
    }

    #[cfg(feature = "rtsp")]
    #[tokio::test]
    async fn decoder_admission_and_frame_bounds_classify_cancellation_timeout_and_limits() {
        assert_eq!(
            GstreamerDecoder::new(RtspCodec::H264, 0)
                .expect_err("zero frame ceiling cannot construct a decoder")
                .code(),
            ErrorCode::ResourceLimit
        );

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert_eq!(
            create_decoder_bounded(
                RtspCodec::H264,
                1024,
                Instant::now() + Duration::from_secs(1),
                &test_decode_gate(),
                &cancelled,
            )
            .await
            .expect_err("cancelled callers cannot enter decoder admission")
            .code(),
            ErrorCode::CaptureCancelled
        );
        assert_eq!(
            create_decoder_bounded(
                RtspCodec::H264,
                1024,
                Instant::now() - Duration::from_millis(1),
                &test_decode_gate(),
                &CancellationToken::new(),
            )
            .await
            .expect_err("elapsed deadline cannot enter decoder admission")
            .code(),
            ErrorCode::CaptureTimeout
        );
    }

    /// The decoder must take a permit from the gate it was HANDED, not from a global of its own.
    ///
    /// This is the assertion the old process-global made impossible to write: a test could not hand
    /// `create_decoder_bounded` a gate and observe that it honored it, so nothing noticed that every
    /// camera in the component was queueing behind the same four permits. An exhausted gate must
    /// hold the caller in decoder admission until its deadline -- and it must be *this* gate.
    #[cfg(feature = "rtsp")]
    #[tokio::test]
    async fn decoder_admission_waits_on_the_injected_gate_not_a_process_global() {
        let gate = Arc::new(Semaphore::new(1));
        let held = Arc::clone(&gate)
            .acquire_owned()
            .await
            .expect("hold the only permit");

        let error = create_decoder_bounded(
            RtspCodec::H264,
            1024,
            Instant::now() + Duration::from_millis(50),
            &gate,
            &CancellationToken::new(),
        )
        .await
        .expect_err("an exhausted decode gate must not admit a decoder");

        assert_eq!(error.code(), ErrorCode::CaptureTimeout);
        drop(held);
        assert_eq!(gate.available_permits(), 1, "the permit must be returned");
    }

    #[cfg(feature = "rtsp")]
    #[test]
    fn decoder_timestamp_correlation_preserves_reordered_frames_and_rejects_unknown_pts() {
        let issued_at = Instant::now();
        let pending_unit = |rtp_timestamp, ingested_at| PendingDecoderUnit {
            rtp_timestamp,
            compressed_bytes: 100,
            dimensions: (320, 240),
            ingested_at,
        };
        let mut pending = BTreeMap::from([
            (10_u64, pending_unit(10, issued_at)),
            (
                20_u64,
                pending_unit(20, issued_at + Duration::from_millis(1)),
            ),
        ]);
        let mut recently_terminal = VecDeque::new();

        let later = correlate_decoder_output(&mut pending, &recently_terminal, 20)
            .expect("later pending PTS")
            .expect("later pending PTS is not terminal");
        assert_eq!(later.rtp_timestamp, 20);
        remember_terminal_decoder_pts(&mut recently_terminal, 20);
        assert!(pending.contains_key(&10));

        let earlier = correlate_decoder_output(&mut pending, &recently_terminal, 10)
            .expect("reordered earlier PTS remains pending")
            .expect("reordered earlier PTS is not terminal");
        assert_eq!(earlier.rtp_timestamp, 10);
        remember_terminal_decoder_pts(&mut recently_terminal, 10);
        assert!(
            correlate_decoder_output(&mut pending, &recently_terminal, 10)
                .expect("known duplicate PTS")
                .is_none()
        );
        assert_eq!(
            correlate_decoder_output(&mut pending, &recently_terminal, 999)
                .expect_err("unknown PTS remains a security failure")
                .code(),
            ErrorCode::BackendError
        );

        for output_pts in 100..=u64::try_from(100 + MAX_DECODER_PENDING_UNITS).unwrap() {
            remember_terminal_decoder_pts(&mut recently_terminal, output_pts);
        }
        assert_eq!(recently_terminal.len(), MAX_DECODER_PENDING_UNITS);
        assert!(!recently_terminal.contains(&10));
        assert_eq!(recently_terminal.front(), Some(&101));
    }

    #[cfg(feature = "rtsp")]
    #[test]
    fn decoder_pending_retirement_recovers_after_skipped_output_and_stays_bounded() {
        let issued_at = Instant::now();
        let pending_unit = |rtp_timestamp, ingested_at| PendingDecoderUnit {
            rtp_timestamp,
            compressed_bytes: 100,
            dimensions: (320, 240),
            ingested_at,
        };
        let mut pending = BTreeMap::new();
        let mut recently_terminal = VecDeque::new();
        for index in 1..=MAX_DECODER_PENDING_UNITS {
            let pts = u64::try_from(index).expect("bounded test PTS");
            let rtp_timestamp = u32::try_from(index).expect("bounded RTP timestamp");
            let offset =
                u64::try_from(MAX_DECODER_PENDING_UNITS - index).expect("bounded ingest offset");
            pending.insert(
                pts,
                pending_unit(rtp_timestamp, issued_at + Duration::from_millis(offset)),
            );
        }

        // The oldest requested unit is PTS 16, even though it is not the
        // lowest PTS. Retirement follows ingestion age and records that exact
        // PTS as terminal; it never guesses a replacement correlation.
        for index in (MAX_DECODER_PENDING_UNITS + 1)..=(MAX_DECODER_PENDING_UNITS + 4) {
            let retired = retire_oldest_pending_decoder_unit(&mut pending, &mut recently_terminal)
                .expect("full pending set retires one unit");
            if index == MAX_DECODER_PENDING_UNITS + 1 {
                assert_eq!(
                    retired,
                    u64::try_from(MAX_DECODER_PENDING_UNITS).expect("bounded retired PTS")
                );
            }
            let pts = u64::try_from(index).expect("bounded recovered PTS");
            let rtp_timestamp = u32::try_from(index).expect("bounded recovered RTP timestamp");
            let offset = u64::try_from(index).expect("bounded recovered ingest offset");
            pending.insert(
                pts,
                pending_unit(rtp_timestamp, issued_at + Duration::from_millis(offset)),
            );
            assert_eq!(pending.len(), MAX_DECODER_PENDING_UNITS);
        }
        assert_eq!(recently_terminal.len(), 4);
        assert!(recently_terminal.contains(&16));

        let recovered_pts =
            u64::try_from(MAX_DECODER_PENDING_UNITS + 4).expect("bounded newest recovered PTS");
        let recovered = correlate_decoder_output(&mut pending, &recently_terminal, recovered_pts)
            .expect("newer exact PTS remains correlated after a skipped frame")
            .expect("newer exact PTS is not terminal");
        assert_eq!(recovered.rtp_timestamp, 20);
        assert!(
            correlate_decoder_output(&mut pending, &recently_terminal, 16)
                .expect("retired exact PTS is a safe late drop")
                .is_none()
        );
        assert_eq!(
            correlate_decoder_output(&mut pending, &recently_terminal, 999)
                .expect_err("unknown PTS is never accepted after retirement")
                .code(),
            ErrorCode::BackendError
        );
    }
}
