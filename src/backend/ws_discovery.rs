//! Explicit-interface WS-Discovery transport for ONVIF cameras.
//!
//! Every production socket binds to an address owned by an operator-selected OS interface. There
//! is deliberately no wildcard bind or route-selected fallback. Probe responses are correlated to
//! the exact message id and parsed under fixed datagram, XML-depth, element, attribute, text, and
//! result bounds before they reach the ONVIF URI policy.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use network_interface::{Addr, NetworkInterface, NetworkInterfaceConfig};
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::{Namespace, ResolveResult};
use quick_xml::reader::NsReader;
use tokio::net::UdpSocket;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::onvif::{DiscoveryProbeMatch, WsDiscovery};
use crate::{CameraError, ErrorCode, Result};

const DISCOVERY_PORT: u16 = 3_702;
const IPV4_MULTICAST: Ipv4Addr = Ipv4Addr::new(239, 255, 255, 250);
const IPV6_MULTICAST: Ipv6Addr = Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 0x000c);
const MAX_INTERFACES: usize = 64;
const MAX_INTERFACE_ENDPOINTS: usize = 256;
const MAX_DATAGRAM_BYTES: usize = 65_535;
const MAX_XML_DEPTH: usize = 64;
const MAX_XML_ELEMENTS: usize = 4_096;
const MAX_XML_ATTRIBUTES: usize = 8_192;
const MAX_EPR_BYTES: usize = 1_024;
const MAX_XADDR_BYTES: usize = 8_192;
const MAX_XADDRS_PER_MATCH: usize = 64;
const MAX_BLOCKING_ENUMERATIONS: usize = 4;
const RETRANSMIT_AFTER: [Duration; 2] = [Duration::from_millis(150), Duration::from_millis(450)];

const SOAP12: &[u8] = b"http://www.w3.org/2003/05/soap-envelope";
const SOAP11: &[u8] = b"http://schemas.xmlsoap.org/soap/envelope/";
const WSA_2004: &str = "http://schemas.xmlsoap.org/ws/2004/08/addressing";
const WSA_2005: &str = "http://www.w3.org/2005/08/addressing";
const WSD_2005: &[u8] = b"http://schemas.xmlsoap.org/ws/2005/04/discovery";
const PROBE_MATCHES_ACTION: &str = "http://schemas.xmlsoap.org/ws/2005/04/discovery/ProbeMatches";

#[derive(Debug, Clone, PartialEq, Eq)]
struct InterfaceEndpoint {
    interface_name: String,
    local: SocketAddr,
    target: SocketAddr,
}

#[derive(Debug, Clone)]
enum EndpointSource {
    System(Arc<[String]>),
    #[cfg(test)]
    Fixed(Arc<[InterfaceEndpoint]>),
}

/// Production WS-Discovery transport constrained to exact OS interface names.
#[derive(Debug, Clone)]
pub struct ExplicitInterfaceWsDiscovery {
    source: EndpointSource,
}

impl ExplicitInterfaceWsDiscovery {
    /// Builds an explicit-interface transport. Interface addresses are refreshed for each probe.
    ///
    /// # Errors
    /// Rejects an empty, duplicate, overlong, control-bearing, or oversized interface list.
    pub fn new(eligible_interfaces: Vec<String>) -> Result<Self> {
        validate_interface_names(&eligible_interfaces)?;
        Ok(Self {
            source: EndpointSource::System(eligible_interfaces.into()),
        })
    }

    #[cfg(test)]
    fn fixed(local: SocketAddr, target: SocketAddr) -> Self {
        Self {
            source: EndpointSource::Fixed(
                vec![InterfaceEndpoint {
                    interface_name: "test-loopback".to_string(),
                    local,
                    target,
                }]
                .into(),
            ),
        }
    }

    async fn endpoints(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<InterfaceEndpoint>> {
        match &self.source {
            EndpointSource::System(names) => {
                enumerate_interfaces_bounded(Arc::clone(names), deadline, cancellation).await
            }
            #[cfg(test)]
            EndpointSource::Fixed(endpoints) => Ok(endpoints.to_vec()),
        }
    }
}

#[async_trait]
impl WsDiscovery for ExplicitInterfaceWsDiscovery {
    fn explicit_interfaces(&self) -> Option<&[String]> {
        match &self.source {
            EndpointSource::System(names) => Some(names.as_ref()),
            #[cfg(test)]
            EndpointSource::Fixed(_) => None,
        }
    }

    async fn probe(
        &self,
        deadline: Instant,
        max_results: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<DiscoveryProbeMatch>> {
        if max_results == 0 || max_results > 10_000 {
            return Err(rejected(
                ErrorCode::InvalidRequest,
                "WS-Discovery result bound must be in 1..=10000",
            ));
        }
        if deadline <= Instant::now() {
            return Err(rejected(
                ErrorCode::CaptureTimeout,
                "WS-Discovery deadline already elapsed",
            ));
        }
        let endpoints = self.endpoints(deadline, cancellation).await?;
        if endpoints.is_empty() || endpoints.len() > MAX_INTERFACE_ENDPOINTS {
            return Err(backend_error(
                "eligible interfaces did not resolve to a bounded address set",
            ));
        }

        let sockets = open_explicit_sockets(&endpoints)?;
        let message_id = format!("urn:uuid:{}", Uuid::now_v7());
        let probes = Arc::<[Arc<[u8]>]>::from([
            Arc::<[u8]>::from(build_probe(&message_id, WSA_2005).into_bytes()),
            Arc::<[u8]>::from(build_probe(&message_id, WSA_2004).into_bytes()),
        ]);
        let internal_cancellation = CancellationToken::new();
        let channel_bound = max_results
            .saturating_add(endpoints.len())
            .saturating_add(1)
            .min(10_258);
        let (sender, mut receiver) = mpsc::channel(channel_bound);
        let mut workers = Vec::with_capacity(sockets.len());
        for (socket, target) in sockets {
            let worker_sender = sender.clone();
            let worker_probes = Arc::clone(&probes);
            let worker_message_id = message_id.clone();
            let worker_cancellation = internal_cancellation.clone();
            let completion_cancellation = internal_cancellation.clone();
            workers.push(tokio::spawn(async move {
                let result = run_socket(SocketWorker {
                    socket,
                    target,
                    probes: worker_probes,
                    message_id: worker_message_id,
                    deadline,
                    max_results,
                    cancellation: worker_cancellation,
                    sender: worker_sender.clone(),
                })
                .await;
                if let Err(error) = result {
                    tokio::select! {
                        biased;
                        _ = completion_cancellation.cancelled() => {}
                        _ = worker_sender.send(WorkerEvent::Fatal(error)) => {}
                    }
                }
            }));
        }
        drop(sender);

        let mut unique = BTreeSet::new();
        let mut matches = Vec::new();
        let outcome = loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    break Err(rejected(ErrorCode::CaptureCancelled, "WS-Discovery cancelled"));
                }
                _ = tokio::time::sleep_until(deadline) => break Ok(()),
                event = receiver.recv() => match event {
                    Some(WorkerEvent::Match(mut found)) => {
                        found.xaddrs.sort();
                        found.xaddrs.dedup();
                        let key = (
                            found.endpoint_reference.clone(),
                            found.xaddrs.clone(),
                            found.vendor.clone(),
                            found.model.clone(),
                        );
                        if unique.insert(key) {
                            matches.push(found);
                            if matches.len() > max_results {
                                break Err(backend_error(
                                    "WS-Discovery unique result count exceeded its bound",
                                ));
                            }
                        }
                    }
                    Some(WorkerEvent::Fatal(error)) => break Err(error),
                    None => break Ok(()),
                }
            }
        };
        internal_cancellation.cancel();
        finish_workers(workers).await?;
        outcome?;
        Ok(matches)
    }
}

/// Explicit configuration state for an ONVIF session that does not use discovery.
///
/// Direct device-service URLs remain usable without a multicast policy.  If a caller later asks
/// that factory to discover, this transport fails with the real configuration cause instead of
/// silently routing through a wildcard socket or a default discovery implementation.
#[derive(Debug, Default)]
pub struct NoEligibleInterfaceWsDiscovery;

#[async_trait]
impl WsDiscovery for NoEligibleInterfaceWsDiscovery {
    fn explicit_interfaces(&self) -> Option<&[String]> {
        Some(&[])
    }

    async fn probe(
        &self,
        _deadline: Instant,
        _max_results: usize,
        _cancellation: &CancellationToken,
    ) -> Result<Vec<DiscoveryProbeMatch>> {
        Err(rejected(
            ErrorCode::UnsupportedCapability,
            "WS-Discovery requires configured discovery.eligibleInterfaces",
        ))
    }
}

#[derive(Debug)]
enum WorkerEvent {
    Match(DiscoveryProbeMatch),
    Fatal(CameraError),
}

struct SocketWorker {
    socket: UdpSocket,
    target: SocketAddr,
    probes: Arc<[Arc<[u8]>]>,
    message_id: String,
    deadline: Instant,
    max_results: usize,
    sender: mpsc::Sender<WorkerEvent>,
    cancellation: CancellationToken,
}

async fn run_socket(worker: SocketWorker) -> Result<()> {
    let SocketWorker {
        socket,
        target,
        probes,
        message_id,
        deadline,
        max_results,
        sender,
        cancellation,
    } = worker;
    let started = Instant::now();
    let mut send_index = 0_usize;
    let mut buffer = vec![0_u8; MAX_DATAGRAM_BYTES];
    loop {
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Ok(());
        }
        let next_send = match send_index {
            0 => Some(started),
            1..=2 => Some(started + RETRANSMIT_AFTER[send_index - 1]),
            _ => None,
        };
        if next_send.is_some_and(|instant| instant <= Instant::now()) {
            for probe in probes.iter() {
                socket
                    .send_to(probe, target)
                    .await
                    .map_err(|_| backend_error("WS-Discovery probe send failed"))?;
            }
            send_index += 1;
            continue;
        }
        let wake = next_send.map_or(deadline, |instant| instant.min(deadline));
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            _ = tokio::time::sleep_until(deadline) => return Ok(()),
            _ = tokio::time::sleep_until(wake), if wake < deadline => {},
            received = socket.recv_from(&mut buffer) => {
                let (length, _peer) = match received {
                    Ok(received) => received,
                    Err(error) if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionRefused
                    ) => continue,
                    Err(_) => return Err(backend_error("WS-Discovery response receive failed")),
                };
                match parse_probe_matches(&buffer[..length], &message_id, max_results)? {
                    ParsedDatagram::Unrelated => {}
                    ParsedDatagram::Related(found) => {
                        for candidate in found {
                            tokio::select! {
                                biased;
                                _ = cancellation.cancelled() => return Ok(()),
                                _ = tokio::time::sleep_until(deadline) => return Ok(()),
                                result = sender.send(WorkerEvent::Match(candidate)) => {
                                    if result.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn finish_workers(workers: Vec<JoinHandle<()>>) -> Result<()> {
    for worker in workers {
        worker
            .await
            .map_err(|_| backend_error("WS-Discovery worker task failed"))?;
    }
    Ok(())
}

fn open_explicit_sockets(endpoints: &[InterfaceEndpoint]) -> Result<Vec<(UdpSocket, SocketAddr)>> {
    let expected_names = endpoints
        .iter()
        .map(|endpoint| endpoint.interface_name.as_str())
        .collect::<BTreeSet<_>>();
    let mut opened_names = BTreeSet::new();
    let mut sockets = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let socket = match std::net::UdpSocket::bind(endpoint.local) {
            Ok(socket) => socket,
            Err(_) => continue,
        };
        socket
            .set_nonblocking(true)
            .map_err(|_| backend_error("failed to configure an eligible discovery socket"))?;
        if endpoint.local.is_ipv4() {
            socket
                .set_multicast_ttl_v4(1)
                .map_err(|_| backend_error("failed to scope IPv4 discovery multicast"))?;
        }
        let socket = UdpSocket::from_std(socket)
            .map_err(|_| backend_error("failed to activate an eligible discovery socket"))?;
        opened_names.insert(endpoint.interface_name.as_str());
        sockets.push((socket, endpoint.target));
    }
    if sockets.is_empty() || opened_names != expected_names {
        return Err(backend_error(
            "one or more eligible interfaces had no usable discovery socket",
        ));
    }
    Ok(sockets)
}

fn interface_enumeration_limiter() -> &'static Arc<Semaphore> {
    static LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();
    LIMITER.get_or_init(|| Arc::new(Semaphore::new(MAX_BLOCKING_ENUMERATIONS)))
}

async fn enumerate_interfaces_bounded(
    names: Arc<[String]>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<InterfaceEndpoint>> {
    let permit = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            return Err(rejected(ErrorCode::CaptureCancelled, "WS-Discovery cancelled"));
        }
        _ = tokio::time::sleep_until(deadline) => {
            return Err(rejected(ErrorCode::CaptureTimeout, "WS-Discovery interface lookup timed out"));
        }
        permit = Arc::clone(interface_enumeration_limiter()).acquire_owned() => {
            permit.map_err(|_| backend_error("WS-Discovery interface limiter closed"))?
        }
    };
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        enumerate_system_interfaces(&names)
    });
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            Err(rejected(ErrorCode::CaptureCancelled, "WS-Discovery cancelled"))
        }
        _ = tokio::time::sleep_until(deadline) => {
            Err(rejected(ErrorCode::CaptureTimeout, "WS-Discovery interface lookup timed out"))
        }
        result = task => {
            result.map_err(|_| backend_error("WS-Discovery interface lookup task failed"))?
        }
    }
}

fn enumerate_system_interfaces(names: &[String]) -> Result<Vec<InterfaceEndpoint>> {
    let interfaces = NetworkInterface::show()
        .map_err(|_| backend_error("operating-system interface enumeration failed"))?;
    let configured = names.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut found = BTreeSet::new();
    let mut endpoints = BTreeSet::new();
    for interface in interfaces {
        if !configured.contains(interface.name.as_str()) {
            continue;
        }
        found.insert(interface.name.clone());
        for address in interface.addr {
            let (local, target) = match address {
                Addr::V4(value) if usable_ip(IpAddr::V4(value.ip)) => (
                    SocketAddr::new(IpAddr::V4(value.ip), 0),
                    SocketAddr::new(IpAddr::V4(IPV4_MULTICAST), DISCOVERY_PORT),
                ),
                Addr::V6(value) if usable_ip(IpAddr::V6(value.ip)) => (
                    SocketAddr::V6(SocketAddrV6::new(value.ip, 0, 0, interface.index)),
                    SocketAddr::V6(SocketAddrV6::new(
                        IPV6_MULTICAST,
                        DISCOVERY_PORT,
                        0,
                        interface.index,
                    )),
                ),
                _ => continue,
            };
            endpoints.insert((interface.name.clone(), local, target));
            if endpoints.len() > MAX_INTERFACE_ENDPOINTS {
                return Err(backend_error(
                    "eligible interface address count exceeded its bound",
                ));
            }
        }
    }
    if found != names.iter().cloned().collect::<BTreeSet<_>>() {
        return Err(backend_error(
            "an explicitly eligible interface name does not exist",
        ));
    }
    let endpoints = endpoints
        .into_iter()
        .map(|(interface_name, local, target)| InterfaceEndpoint {
            interface_name,
            local,
            target,
        })
        .collect::<Vec<_>>();
    for name in names {
        if !endpoints
            .iter()
            .any(|endpoint| endpoint.interface_name == *name)
        {
            return Err(backend_error(
                "an explicitly eligible interface has no usable IP address",
            ));
        }
    }
    Ok(endpoints)
}

fn usable_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => !address.is_unspecified() && !address.is_multicast(),
        IpAddr::V6(address) => !address.is_unspecified() && !address.is_multicast(),
    }
}

fn validate_interface_names(names: &[String]) -> Result<()> {
    if names.is_empty() || names.len() > MAX_INTERFACES {
        return Err(backend_error(
            "WS-Discovery requires 1..=64 explicit interface names",
        ));
    }
    let mut unique = BTreeSet::new();
    for name in names {
        if name.is_empty()
            || name.len() > 256
            || name.chars().any(char::is_control)
            || !unique.insert(name)
        {
            return Err(backend_error(
                "WS-Discovery interface names must be distinct bounded text",
            ));
        }
    }
    Ok(())
}

fn build_probe(message_id: &str, addressing_namespace: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\" \
xmlns:a=\"{addressing_namespace}\" \
xmlns:d=\"http://schemas.xmlsoap.org/ws/2005/04/discovery\" \
xmlns:dn=\"http://www.onvif.org/ver10/network/wsdl\">\
<s:Header><a:Action s:mustUnderstand=\"1\">http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</a:Action>\
<a:MessageID>{message_id}</a:MessageID>\
<a:To s:mustUnderstand=\"1\">urn:schemas-xmlsoap-org:ws:2005:04:discovery</a:To></s:Header>\
<s:Body><d:Probe><d:Types>dn:NetworkVideoTransmitter</d:Types></d:Probe></s:Body></s:Envelope>"
    )
}

#[derive(Debug, PartialEq, Eq)]
enum ParsedDatagram {
    Unrelated,
    Related(Vec<DiscoveryProbeMatch>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NamespaceKind {
    Soap,
    Addressing,
    Discovery,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldKind {
    Action,
    RelatesTo,
    EndpointAddress,
    Xaddrs,
}

#[derive(Debug)]
enum ElementKind {
    Envelope,
    Header,
    Body,
    ProbeMatches,
    ProbeMatch,
    EndpointReference,
    Field(FieldKind),
    Other,
}

#[derive(Debug)]
struct ElementState {
    kind: ElementKind,
    text: String,
}

#[derive(Debug, Default)]
struct MatchBuilder {
    endpoint_reference: Option<String>,
    xaddrs: Option<Vec<String>>,
}

#[derive(Debug, Default)]
struct StructureCounts {
    envelope: usize,
    header: usize,
    body: usize,
    probe_matches: usize,
}

fn parse_probe_matches(
    bytes: &[u8],
    expected_message_id: &str,
    max_matches: usize,
) -> Result<ParsedDatagram> {
    if bytes.is_empty() || bytes.len() > MAX_DATAGRAM_BYTES {
        return Err(backend_error(
            "WS-Discovery datagram violates its byte bound",
        ));
    }
    let mut reader = NsReader::from_reader(bytes);
    reader.config_mut().trim_text(true);
    let mut buffer = Vec::new();
    let mut stack = Vec::<ElementState>::new();
    let mut element_count = 0_usize;
    let mut attribute_count = 0_usize;
    let mut text_count = 0_usize;
    let mut action = None;
    let mut relates_to = None;
    let mut current_match = None;
    let mut matches = Vec::new();
    let mut structure = StructureCounts::default();
    loop {
        let (namespace, event) = reader
            .read_resolved_event_into(&mut buffer)
            .map_err(|_| backend_error("WS-Discovery XML parsing failed"))?;
        let namespace = namespace_kind(&namespace)?;
        match event {
            Event::Start(start) => {
                push_element(
                    &start,
                    namespace,
                    &mut stack,
                    &mut current_match,
                    &mut structure,
                    &mut element_count,
                    &mut attribute_count,
                )?;
            }
            Event::Empty(start) => {
                push_element(
                    &start,
                    namespace,
                    &mut stack,
                    &mut current_match,
                    &mut structure,
                    &mut element_count,
                    &mut attribute_count,
                )?;
                finish_element(
                    &mut stack,
                    &mut current_match,
                    &mut matches,
                    &mut action,
                    &mut relates_to,
                    max_matches,
                )?;
            }
            Event::Text(text) => {
                let decoded = text
                    .xml_content()
                    .map_err(|_| backend_error("WS-Discovery XML text decoding failed"))?;
                let decoded = quick_xml::escape::unescape(&decoded)
                    .map_err(|_| backend_error("WS-Discovery XML entity decoding failed"))?;
                text_count = text_count
                    .checked_add(decoded.len())
                    .ok_or_else(|| backend_error("WS-Discovery XML text bound overflowed"))?;
                if text_count > bytes.len() {
                    return Err(backend_error(
                        "WS-Discovery XML aggregate text exceeded its bound",
                    ));
                }
                if let Some(element) = stack.last_mut() {
                    if matches!(element.kind, ElementKind::Field(_)) {
                        element.text.push_str(&decoded);
                    }
                }
            }
            Event::End(_) => {
                finish_element(
                    &mut stack,
                    &mut current_match,
                    &mut matches,
                    &mut action,
                    &mut relates_to,
                    max_matches,
                )?;
            }
            Event::DocType(_) | Event::GeneralRef(_) => {
                return Err(backend_error(
                    "WS-Discovery XML DTDs and entities are forbidden",
                ));
            }
            Event::PI(_) => {
                return Err(backend_error(
                    "WS-Discovery XML processing instructions are forbidden",
                ));
            }
            Event::CData(_) => {
                return Err(backend_error("WS-Discovery XML CDATA is forbidden"));
            }
            Event::Decl(_) | Event::Comment(_) => {}
            Event::Eof => break,
        }
        buffer.clear();
    }
    if !stack.is_empty() || current_match.is_some() {
        return Err(backend_error("WS-Discovery XML document is incomplete"));
    }
    if relates_to.as_deref() != Some(expected_message_id) {
        return Ok(ParsedDatagram::Unrelated);
    }
    if action.as_deref() != Some(PROBE_MATCHES_ACTION) {
        return Err(backend_error(
            "correlated WS-Discovery response has an invalid action",
        ));
    }
    if structure.envelope != 1
        || structure.header != 1
        || structure.body != 1
        || structure.probe_matches != 1
    {
        return Err(backend_error(
            "correlated WS-Discovery response has an invalid SOAP structure",
        ));
    }
    Ok(ParsedDatagram::Related(matches))
}

fn push_element(
    start: &BytesStart<'_>,
    namespace: NamespaceKind,
    stack: &mut Vec<ElementState>,
    current_match: &mut Option<MatchBuilder>,
    structure: &mut StructureCounts,
    element_count: &mut usize,
    attribute_count: &mut usize,
) -> Result<()> {
    if stack.len() >= MAX_XML_DEPTH {
        return Err(backend_error("WS-Discovery XML exceeded its depth bound"));
    }
    *element_count = element_count
        .checked_add(1)
        .ok_or_else(|| backend_error("WS-Discovery XML element count overflowed"))?;
    if *element_count > MAX_XML_ELEMENTS {
        return Err(backend_error(
            "WS-Discovery XML element count exceeded its bound",
        ));
    }
    for attribute in start.attributes().with_checks(true) {
        attribute.map_err(|_| backend_error("WS-Discovery XML attribute is invalid"))?;
        *attribute_count = attribute_count
            .checked_add(1)
            .ok_or_else(|| backend_error("WS-Discovery XML attribute count overflowed"))?;
        if *attribute_count > MAX_XML_ATTRIBUTES {
            return Err(backend_error(
                "WS-Discovery XML attribute count exceeded its bound",
            ));
        }
    }
    let local = start.local_name();
    let kind = match (namespace, local.as_ref()) {
        (NamespaceKind::Soap, b"Envelope") => {
            if !stack.is_empty() || structure.envelope != 0 {
                return Err(backend_error(
                    "WS-Discovery response repeats or nests its SOAP envelope",
                ));
            }
            structure.envelope += 1;
            ElementKind::Envelope
        }
        (NamespaceKind::Soap, b"Header") => {
            require_parent(
                stack,
                |kind| matches!(kind, ElementKind::Envelope),
                "SOAP header",
            )?;
            if structure.header != 0 {
                return Err(backend_error(
                    "WS-Discovery response repeats its SOAP header",
                ));
            }
            structure.header += 1;
            ElementKind::Header
        }
        (NamespaceKind::Soap, b"Body") => {
            require_parent(
                stack,
                |kind| matches!(kind, ElementKind::Envelope),
                "SOAP body",
            )?;
            if structure.body != 0 {
                return Err(backend_error("WS-Discovery response repeats its SOAP body"));
            }
            structure.body += 1;
            ElementKind::Body
        }
        (NamespaceKind::Discovery, b"ProbeMatches") => {
            require_parent(
                stack,
                |kind| matches!(kind, ElementKind::Body),
                "ProbeMatches",
            )?;
            if structure.probe_matches != 0 {
                return Err(backend_error("WS-Discovery response repeats ProbeMatches"));
            }
            structure.probe_matches += 1;
            ElementKind::ProbeMatches
        }
        (NamespaceKind::Discovery, b"ProbeMatch") => {
            require_parent(
                stack,
                |kind| matches!(kind, ElementKind::ProbeMatches),
                "ProbeMatch",
            )?;
            if current_match.replace(MatchBuilder::default()).is_some() {
                return Err(backend_error(
                    "WS-Discovery response contains nested probe matches",
                ));
            }
            ElementKind::ProbeMatch
        }
        (NamespaceKind::Addressing, b"EndpointReference") => {
            require_parent(
                stack,
                |kind| matches!(kind, ElementKind::ProbeMatch),
                "endpoint reference",
            )?;
            if current_match.is_none() {
                return Err(backend_error(
                    "WS-Discovery endpoint reference has no probe match",
                ));
            }
            ElementKind::EndpointReference
        }
        (NamespaceKind::Addressing, b"Address") => {
            require_parent(
                stack,
                |kind| matches!(kind, ElementKind::EndpointReference),
                "endpoint address",
            )?;
            if current_match.is_none() {
                return Err(backend_error(
                    "WS-Discovery endpoint address has no probe match",
                ));
            }
            ElementKind::Field(FieldKind::EndpointAddress)
        }
        (NamespaceKind::Discovery, b"XAddrs") => {
            require_parent(
                stack,
                |kind| matches!(kind, ElementKind::ProbeMatch),
                "XAddrs",
            )?;
            if current_match.is_none() {
                return Err(backend_error("WS-Discovery XAddrs have no probe match"));
            }
            ElementKind::Field(FieldKind::Xaddrs)
        }
        (NamespaceKind::Addressing, b"Action") => {
            require_parent(stack, |kind| matches!(kind, ElementKind::Header), "action")?;
            ElementKind::Field(FieldKind::Action)
        }
        (NamespaceKind::Addressing, b"RelatesTo") => {
            require_parent(
                stack,
                |kind| matches!(kind, ElementKind::Header),
                "correlation id",
            )?;
            ElementKind::Field(FieldKind::RelatesTo)
        }
        _ => ElementKind::Other,
    };
    stack.push(ElementState {
        kind,
        text: String::new(),
    });
    Ok(())
}

fn require_parent(
    stack: &[ElementState],
    predicate: impl FnOnce(&ElementKind) -> bool,
    label: &str,
) -> Result<()> {
    if stack.last().is_none_or(|parent| !predicate(&parent.kind)) {
        return Err(backend_error(format!(
            "WS-Discovery {label} is outside its required parent"
        )));
    }
    Ok(())
}

fn finish_element(
    stack: &mut Vec<ElementState>,
    current_match: &mut Option<MatchBuilder>,
    matches: &mut Vec<DiscoveryProbeMatch>,
    action: &mut Option<String>,
    relates_to: &mut Option<String>,
    max_matches: usize,
) -> Result<()> {
    let state = stack
        .pop()
        .ok_or_else(|| backend_error("WS-Discovery XML end tag is unbalanced"))?;
    match state.kind {
        ElementKind::Field(FieldKind::Action) => {
            set_once(action, bounded_text(state.text, 256, "action")?, "action")?;
        }
        ElementKind::Field(FieldKind::RelatesTo) => {
            set_once(
                relates_to,
                bounded_text(state.text, 256, "correlation id")?,
                "correlation id",
            )?;
        }
        ElementKind::Field(FieldKind::EndpointAddress) => {
            let endpoint = bounded_text(state.text, MAX_EPR_BYTES, "endpoint reference")?;
            let builder = current_match
                .as_mut()
                .ok_or_else(|| backend_error("endpoint reference escaped its probe match"))?;
            set_once(
                &mut builder.endpoint_reference,
                endpoint,
                "endpoint reference",
            )?;
        }
        ElementKind::Field(FieldKind::Xaddrs) => {
            let text = bounded_text(state.text, MAX_XADDR_BYTES, "XAddrs")?;
            let values = text
                .split_ascii_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            if values.is_empty()
                || values.len() > MAX_XADDRS_PER_MATCH
                || values.iter().any(|value| {
                    value.len() > MAX_XADDR_BYTES || value.chars().any(char::is_control)
                })
            {
                return Err(backend_error("WS-Discovery XAddrs violate their bounds"));
            }
            let builder = current_match
                .as_mut()
                .ok_or_else(|| backend_error("XAddrs escaped their probe match"))?;
            if builder.xaddrs.replace(values).is_some() {
                return Err(backend_error("WS-Discovery probe match repeats XAddrs"));
            }
        }
        ElementKind::ProbeMatch => {
            let builder = current_match
                .take()
                .ok_or_else(|| backend_error("WS-Discovery probe match is unbalanced"))?;
            let endpoint_reference = builder
                .endpoint_reference
                .ok_or_else(|| backend_error("WS-Discovery probe match lacks an endpoint"))?;
            let xaddrs = builder
                .xaddrs
                .ok_or_else(|| backend_error("WS-Discovery probe match lacks XAddrs"))?;
            matches.push(DiscoveryProbeMatch {
                endpoint_reference,
                xaddrs,
                vendor: None,
                model: None,
            });
            if matches.len() > max_matches {
                return Err(backend_error(
                    "WS-Discovery datagram result count exceeded its bound",
                ));
            }
        }
        ElementKind::Envelope
        | ElementKind::Header
        | ElementKind::Body
        | ElementKind::ProbeMatches
        | ElementKind::EndpointReference
        | ElementKind::Other => {}
    }
    Ok(())
}

fn bounded_text(value: String, maximum: usize, label: &str) -> Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(backend_error(format!(
            "WS-Discovery {label} violates its text bound"
        )));
    }
    Ok(value)
}

fn set_once(target: &mut Option<String>, value: String, label: &str) -> Result<()> {
    if target.replace(value).is_some() {
        return Err(backend_error(format!(
            "WS-Discovery response repeats {label}"
        )));
    }
    Ok(())
}

fn namespace_kind(resolution: &ResolveResult<'_>) -> Result<NamespaceKind> {
    match resolution {
        ResolveResult::Bound(Namespace(value)) if *value == SOAP12 || *value == SOAP11 => {
            Ok(NamespaceKind::Soap)
        }
        ResolveResult::Bound(Namespace(value))
            if *value == WSA_2004.as_bytes() || *value == WSA_2005.as_bytes() =>
        {
            Ok(NamespaceKind::Addressing)
        }
        ResolveResult::Bound(Namespace(value)) if *value == WSD_2005 => {
            Ok(NamespaceKind::Discovery)
        }
        ResolveResult::Bound(_) | ResolveResult::Unbound => Ok(NamespaceKind::Other),
        ResolveResult::Unknown(_) => Err(backend_error(
            "WS-Discovery XML contains an unbound namespace prefix",
        )),
    }
}

fn backend_error(message: impl Into<String>) -> CameraError {
    CameraError::Backend {
        backend: "onvif-rtsp",
        message: message.into(),
    }
}

fn rejected(code: ErrorCode, message: &'static str) -> CameraError {
    CameraError::rejected(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(message_id: &str, endpoint: &str, xaddrs: &str) -> String {
        format!(
            "<s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\" \
xmlns:a=\"http://schemas.xmlsoap.org/ws/2004/08/addressing\" \
xmlns:d=\"http://schemas.xmlsoap.org/ws/2005/04/discovery\">\
<s:Header><a:Action>{PROBE_MATCHES_ACTION}</a:Action><a:RelatesTo>{message_id}</a:RelatesTo></s:Header>\
<s:Body><d:ProbeMatches><d:ProbeMatch><a:EndpointReference><a:Address>{endpoint}</a:Address>\
</a:EndpointReference><d:XAddrs>{xaddrs}</d:XAddrs><d:MetadataVersion>1</d:MetadataVersion>\
</d:ProbeMatch></d:ProbeMatches></s:Body></s:Envelope>"
        )
    }

    #[test]
    fn interface_contract_rejects_implicit_or_ambiguous_scope() {
        assert!(ExplicitInterfaceWsDiscovery::new(Vec::new()).is_err());
        assert!(
            ExplicitInterfaceWsDiscovery::new(vec!["eth0".to_string(), "eth0".to_string()])
                .is_err()
        );
        assert!(ExplicitInterfaceWsDiscovery::new(vec!["eth0\n".to_string()]).is_err());
        assert!(ExplicitInterfaceWsDiscovery::new(vec!["x".repeat(257)]).is_err());
    }

    #[test]
    fn probe_generation_and_address_filtering_stay_within_the_fixed_protocol_contract() {
        let probe = build_probe("urn:uuid:test", WSA_2005);
        assert!(probe.contains("<a:MessageID>urn:uuid:test</a:MessageID>"));
        assert!(probe.contains("dn:NetworkVideoTransmitter"));
        assert!(probe.contains(WSA_2005));
        assert!(usable_ip(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(!usable_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED)));
        assert!(!usable_ip(IpAddr::V4(IPV4_MULTICAST)));
        assert!(usable_ip(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(!usable_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED)));
        assert!(!usable_ip(IpAddr::V6(IPV6_MULTICAST)));
    }

    #[test]
    fn parser_correlates_and_extracts_bounded_matches() {
        let message_id = "urn:uuid:11111111-2222-3333-4444-555555555555";
        let parsed = parse_probe_matches(
            response(
                message_id,
                "urn:uuid:camera-a",
                "http://camera.test/onvif/device_service",
            )
            .as_bytes(),
            message_id,
            2,
        )
        .unwrap();
        let ParsedDatagram::Related(matches) = parsed else {
            panic!("response must correlate");
        };
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].endpoint_reference, "urn:uuid:camera-a");
        assert_eq!(
            matches[0].xaddrs,
            ["http://camera.test/onvif/device_service"]
        );
    }

    #[test]
    fn parser_ignores_a_well_formed_unrelated_response() {
        let parsed = parse_probe_matches(
            response(
                "urn:uuid:other",
                "urn:uuid:camera-a",
                "http://camera.test/onvif/device_service",
            )
            .as_bytes(),
            "urn:uuid:expected",
            2,
        )
        .unwrap();
        assert_eq!(parsed, ParsedDatagram::Unrelated);
    }

    #[test]
    fn parser_rejects_namespace_spoofing_and_dtds() {
        let message_id = "urn:uuid:expected";
        let spoofed = response(
            message_id,
            "urn:uuid:camera-a",
            "http://camera.test/onvif/device_service",
        )
        .replace(
            "xmlns:d=\"http://schemas.xmlsoap.org/ws/2005/04/discovery\"",
            "xmlns:d=\"urn:hostile\"",
        );
        assert!(parse_probe_matches(spoofed.as_bytes(), message_id, 2).is_err());

        let dtd = format!(
            "<!DOCTYPE x [<!ENTITY e \"boom\">]>{}",
            response(message_id, "&e;", "http://camera.test/service")
        );
        assert!(parse_probe_matches(dtd.as_bytes(), message_id, 2).is_err());
    }

    #[test]
    fn parser_rejects_correlated_malformed_actions_and_result_bound_overflow() {
        let message_id = "urn:uuid:expected";
        let wrong_action = response(
            message_id,
            "urn:uuid:camera-a",
            "http://camera.test/onvif/device_service",
        )
        .replace(PROBE_MATCHES_ACTION, "urn:hostile-action");
        assert!(parse_probe_matches(wrong_action.as_bytes(), message_id, 1).is_err());

        let first = response(
            message_id,
            "urn:uuid:camera-a",
            "http://camera.test/onvif/device_service",
        );
        let second = first.replace("camera-a", "camera-b");
        let second_match = second
            .split_once("<d:ProbeMatch>")
            .and_then(|(_, value)| value.split_once("</d:ProbeMatch>"))
            .map(|(value, _)| format!("<d:ProbeMatch>{value}</d:ProbeMatch>"))
            .expect("second probe match");
        let two_matches = first.replace(
            "</d:ProbeMatches>",
            &format!("{second_match}</d:ProbeMatches>"),
        );
        assert!(parse_probe_matches(two_matches.as_bytes(), message_id, 1).is_err());
    }

    #[test]
    fn parser_rejects_repeated_required_fields_and_invalid_xaddr_contracts() {
        let message_id = "urn:uuid:expected";
        let base = response(
            message_id,
            "urn:uuid:camera-a",
            "http://camera.test/onvif/device_service",
        );
        for invalid in [
            base.replace(
                "</a:Action>",
                "</a:Action><a:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/ProbeMatches</a:Action>",
            ),
            base.replace(
                "</a:RelatesTo>",
                "</a:RelatesTo><a:RelatesTo>urn:uuid:expected</a:RelatesTo>",
            ),
            base.replace(
                "</d:XAddrs>",
                "</d:XAddrs><d:XAddrs>http://camera.test/second</d:XAddrs>",
            ),
            base.replace(
                "<d:XAddrs>http://camera.test/onvif/device_service</d:XAddrs>",
                "<d:XAddrs> </d:XAddrs>",
            ),
            base.replace(
                "<a:Address>urn:uuid:camera-a</a:Address>",
                "<a:Address>bad\nendpoint</a:Address>",
            ),
        ] {
            assert!(
                parse_probe_matches(invalid.as_bytes(), message_id, 2).is_err(),
                "malformed required field must not become a candidate"
            );
        }
    }

    #[tokio::test]
    async fn explicit_udp_probe_deduplicates_sorted_xaddrs_and_honors_cancellation_and_bounds() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = server.local_addr().unwrap();
        let responder = tokio::spawn(async move {
            let mut bytes = vec![0_u8; MAX_DATAGRAM_BYTES];
            let (length, peer) = server.recv_from(&mut bytes).await.unwrap();
            let request = String::from_utf8(bytes[..length].to_vec()).unwrap();
            let prefix = "<a:MessageID>";
            let start = request.find(prefix).unwrap() + prefix.len();
            let end = request[start..].find("</a:MessageID>").unwrap() + start;
            let payload = response(
                &request[start..end],
                "urn:uuid:deduplicated-camera",
                "http://camera.test/b http://camera.test/a http://camera.test/a",
            );
            server.send_to(payload.as_bytes(), peer).await.unwrap();
            server.send_to(payload.as_bytes(), peer).await.unwrap();
        });
        let transport = ExplicitInterfaceWsDiscovery::fixed("127.0.0.1:0".parse().unwrap(), target);
        let found = transport
            .probe(
                Instant::now() + Duration::from_millis(300),
                1,
                &CancellationToken::new(),
            )
            .await
            .expect("duplicate datagrams represent one bounded candidate");
        responder.await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].xaddrs,
            vec![
                "http://camera.test/a".to_owned(),
                "http://camera.test/b".to_owned(),
            ]
        );

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let error = ExplicitInterfaceWsDiscovery::fixed(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:9".parse().unwrap(),
        )
        .probe(Instant::now() + Duration::from_secs(1), 1, &cancelled)
        .await
        .expect_err("pre-cancelled discovery cannot send a probe");
        assert_eq!(error.code(), ErrorCode::CaptureCancelled);

        for max_results in [0, 10_001] {
            assert_eq!(
                ExplicitInterfaceWsDiscovery::fixed(
                    "127.0.0.1:0".parse().unwrap(),
                    "127.0.0.1:9".parse().unwrap(),
                )
                .probe(
                    Instant::now() + Duration::from_secs(1),
                    max_results,
                    &CancellationToken::new(),
                )
                .await
                .expect_err("result limit is a public input bound")
                .code(),
                ErrorCode::InvalidRequest
            );
        }
    }

    #[tokio::test]
    async fn no_eligible_interface_transport_fails_closed_and_system_scope_is_visible() {
        let disabled = NoEligibleInterfaceWsDiscovery;
        assert_eq!(disabled.explicit_interfaces(), Some(&[][..]));
        assert_eq!(
            disabled
                .probe(
                    Instant::now() + Duration::from_secs(1),
                    1,
                    &CancellationToken::new(),
                )
                .await
                .expect_err(
                    "direct ONVIF configurations must not discover through wildcard routing"
                )
                .code(),
            ErrorCode::UnsupportedCapability
        );

        let explicit = ExplicitInterfaceWsDiscovery::new(vec!["camera-net".to_owned()])
            .expect("valid configured interface name");
        assert_eq!(
            explicit.explicit_interfaces(),
            Some(&["camera-net".to_owned()][..])
        );
        assert_eq!(
            explicit
                .probe(
                    Instant::now() - Duration::from_millis(1),
                    1,
                    &CancellationToken::new(),
                )
                .await
                .expect_err("elapsed deadline rejects before interface lookup")
                .code(),
            ErrorCode::CaptureTimeout
        );
    }

    #[tokio::test]
    async fn direct_udp_harness_correlates_without_wildcard_binding() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = server.local_addr().unwrap();
        let responder = tokio::spawn(async move {
            let mut bytes = vec![0_u8; MAX_DATAGRAM_BYTES];
            let (length, peer) = server.recv_from(&mut bytes).await.unwrap();
            let request = String::from_utf8(bytes[..length].to_vec()).unwrap();
            let prefix = "<a:MessageID>";
            let start = request.find(prefix).unwrap() + prefix.len();
            let end = request[start..].find("</a:MessageID>").unwrap() + start;
            let message_id = &request[start..end];
            let payload = response(
                message_id,
                "urn:uuid:direct-camera",
                "http://127.0.0.1:18080/onvif/device_service",
            );
            server.send_to(payload.as_bytes(), peer).await.unwrap();
        });
        let transport = ExplicitInterfaceWsDiscovery::fixed("127.0.0.1:0".parse().unwrap(), target);
        let found = transport
            .probe(
                Instant::now() + Duration::from_millis(300),
                4,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        responder.await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].endpoint_reference, "urn:uuid:direct-camera");
    }

    #[tokio::test]
    #[ignore = "requires the repository ONVIF simulator direct UDP port"]
    async fn repository_simulator_direct_discovery_contract() {
        let target = std::env::var("CAMERA_ADAPTER_WSD_TARGET")
            .expect("set CAMERA_ADAPTER_WSD_TARGET, for example 127.0.0.1:13702")
            .parse()
            .expect("target must be a socket address");
        let transport = ExplicitInterfaceWsDiscovery::fixed("127.0.0.1:0".parse().unwrap(), target);
        let found = transport
            .probe(
                Instant::now() + Duration::from_secs(2),
                4,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].endpoint_reference,
            "urn:uuid:edgecommons-onvif-simulator"
        );
        assert_eq!(found[0].xaddrs.len(), 1);
    }
}
