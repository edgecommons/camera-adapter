//! Production GenICam backend using Aravis 0.8.36 or newer.
//!
//! Aravis has process-global discovery state and native calls that may block. Credential-free
//! discovery therefore runs in a fresh helper process per explicitly eligible OS interface, while
//! each connected camera is owned by one dedicated native worker thread behind a bounded queue.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::CString;
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use aravis::prelude::*;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use image::ImageReader;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Semaphore, oneshot};
use tokio_util::sync::CancellationToken;

use super::{
    CameraBackendFactory, CameraSession, CameraStatus, CaptureRequest, ConnectRequest,
    DiscoveryCandidate, DiscoveryRequest,
};
use crate::config::{BackendConfig, CaptureProfile, GenicamBackendConfig, GenicamTransport};
use crate::model::{
    BackendKind, CameraCapabilities, CaptureFrame, CaptureMode, FrameTimestampQuality, PixelFormat,
    PtzRequest, PtzResult,
};
use crate::{CameraError, ErrorCode, Result};

const BACKEND: &str = "genicam-aravis";
const WORKER_QUEUE_CAPACITY: usize = 4;
const DEFAULT_BUFFER_COUNT: usize = 2;
const NATIVE_POLL: Duration = Duration::from_millis(20);
const PROCESS_POLL: Duration = Duration::from_millis(5);
const MAX_HELPER_STDOUT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_HELPER_STDERR_BYTES: u64 = 16 * 1024;
const MAX_NATIVE_FIELD_BYTES: usize = 1_024;
const NO_GIGE_INTERFACE: &str = "camera-adapter-no-gige-interface";
const TIMESTAMP_REFRESH_NS: u64 = 60_000_000_000;

static ARAVIS_TOKEN: Mutex<Option<aravis::Aravis>> = Mutex::new(None);

/// Aravis-backed factory. Discovery concurrency is deliberately one per component process; each
/// scan may itself launch a sequence of isolated per-interface helpers.
pub struct GenicamAravisBackendFactory {
    native: Arc<dyn NativeBackend>,
    discovery_permit: Arc<Semaphore>,
}

impl Default for GenicamAravisBackendFactory {
    fn default() -> Self {
        Self {
            native: Arc::new(ProductionNativeBackend),
            discovery_permit: Arc::new(Semaphore::new(1)),
        }
    }
}

impl GenicamAravisBackendFactory {
    #[cfg(test)]
    fn with_native(native: Arc<dyn NativeBackend>) -> Self {
        Self {
            native,
            discovery_permit: Arc::new(Semaphore::new(1)),
        }
    }
}

#[async_trait]
impl CameraBackendFactory for GenicamAravisBackendFactory {
    fn kind(&self) -> BackendKind {
        BackendKind::GenicamAravis
    }

    async fn discover(&self, request: DiscoveryRequest) -> Result<Vec<DiscoveryCandidate>> {
        if request.max_results == 0 || request.max_results > 10_000 {
            return rejected(
                ErrorCode::InvalidRequest,
                "GenICam discovery result bound is outside 1..=10000",
            );
        }
        validate_interfaces(&request.eligible_interfaces)?;
        let deadline = Instant::now() + request.timeout;
        let permit = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => return cancelled("before GenICam discovery"),
            _ = tokio::time::sleep_until(deadline.into()) => return timeout("waiting for GenICam discovery admission"),
            permit = self.discovery_permit.clone().acquire_owned() => permit.map_err(|_| backend_error("discovery admission is closed"))?,
        };
        let native = self.native.clone();
        let interfaces = request.eligible_interfaces;
        let cancellation = request.cancellation.clone();
        let maximum = request.max_results;
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            native.discover(&interfaces, maximum, deadline, &cancellation)
        });
        tokio::pin!(task);
        let devices = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => return cancelled("during GenICam discovery"),
            _ = tokio::time::sleep_until(deadline.into()) => return timeout("during GenICam discovery"),
            result = &mut task => result.map_err(|_| backend_error("GenICam discovery worker stopped"))??,
        };
        Ok(devices
            .into_iter()
            .map(WireDevice::into_candidate)
            .collect())
    }

    async fn connect(&self, request: ConnectRequest) -> Result<Box<dyn CameraSession>> {
        let BackendConfig::GenicamAravis(config) = request.backend else {
            return rejected(
                ErrorCode::InvalidRequest,
                "GenICam factory received another backend configuration",
            );
        };
        let deadline = Instant::now() + request.timeout;
        let (sender, receiver) = mpsc::sync_channel(WORKER_QUEUE_CAPACITY);
        let (initial_sender, initial_receiver) = oneshot::channel();
        let native = self.native.clone();
        let cancellation = request.cancellation.clone();
        let thread = std::thread::Builder::new()
            .name(format!(
                "camera-genicam-{}",
                bounded_thread_label(&request.instance_id)
            ))
            .spawn(move || {
                let mut session = match native.connect(&config, deadline, &cancellation) {
                    Ok(session) => session,
                    Err(error) => {
                        let _ = initial_sender.send(Err(error));
                        return;
                    }
                };
                let capabilities = session.capabilities().clone();
                if initial_sender.send(Ok(capabilities)).is_err() {
                    let _ = session.close();
                    return;
                }
                native_worker_loop(session.as_mut(), receiver);
            })
            .map_err(|_| backend_error("failed to start the GenICam native worker"))?;

        let capabilities = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => {
                drop(sender);
                return cancelled("while connecting GenICam camera");
            }
            _ = tokio::time::sleep_until(deadline.into()) => {
                drop(sender);
                return timeout("while connecting GenICam camera");
            }
            result = initial_receiver => result.map_err(|_| backend_error("GenICam connect worker stopped"))??,
        };
        Ok(Box::new(GenicamSessionProxy {
            sender: Some(sender),
            thread: Some(thread),
            capabilities,
            closed: false,
        }))
    }
}

trait NativeBackend: Send + Sync + 'static {
    fn discover(
        &self,
        interfaces: &[String],
        maximum: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<WireDevice>>;

    fn connect(
        &self,
        config: &GenicamBackendConfig,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn NativeSession>>;
}

trait NativeSession: Send + 'static {
    fn capabilities(&self) -> &CameraCapabilities;
    fn status(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CameraStatus>;
    fn capture(&mut self, request: CaptureRequest, deadline: Instant) -> Result<CaptureFrame>;
    fn close(&mut self) -> Result<()>;
}

struct ProductionNativeBackend;

impl NativeBackend for ProductionNativeBackend {
    fn discover(
        &self,
        interfaces: &[String],
        maximum: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<WireDevice>> {
        let mut devices = BTreeMap::<(WireTransport, String), WireDevice>::new();

        // USB discovery is local and credential-free. A fresh helper with an impossible Linux
        // interface name guarantees that it emits no GigE packets.
        for device in run_discovery_helper(
            NO_GIGE_INTERFACE,
            WireTransport::Usb3Vision,
            maximum,
            deadline,
            cancellation,
        )? {
            devices.insert((device.transport, device.device_id.clone()), device);
        }
        for interface in interfaces {
            check_deadline(deadline, cancellation, "during scoped GenICam discovery")?;
            let remaining = maximum.saturating_sub(devices.len());
            if remaining == 0 {
                break;
            }
            for device in run_discovery_helper(
                interface,
                WireTransport::GigeVision,
                remaining,
                deadline,
                cancellation,
            )? {
                devices.insert((device.transport, device.device_id.clone()), device);
            }
        }
        Ok(devices.into_values().take(maximum).collect())
    }

    fn connect(
        &self,
        config: &GenicamBackendConfig,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn NativeSession>> {
        check_deadline(deadline, cancellation, "before GenICam selector resolution")?;
        let (camera, identity, transport, interface) = resolve_and_open(config)?;
        check_deadline(deadline, cancellation, "after GenICam selector resolution")?;
        let settings = apply_connection_settings(&camera, config, transport)?;
        let capabilities = read_capabilities(&camera, &identity)?;
        Ok(Box::new(AravisNativeSession {
            camera,
            stream: None,
            buffer_capacity: 0,
            buffer_count: config.buffer_count.unwrap_or(DEFAULT_BUFFER_COUNT),
            capabilities,
            transport,
            interface,
            packet_size: settings.packet_size,
            packet_delay_ns: settings.packet_delay_ns,
            profile: None,
            calibration: TimestampCalibration::default(),
            acquisition_started: false,
            closed: false,
        }))
    }
}

enum WorkerCommand {
    Status {
        deadline: Instant,
        cancellation: CancellationToken,
        response: oneshot::Sender<Result<CameraStatus>>,
    },
    Capture {
        request: CaptureRequest,
        deadline: Instant,
        response: oneshot::Sender<Result<CaptureFrame>>,
    },
    Close {
        response: oneshot::Sender<Result<()>>,
    },
}

fn native_worker_loop(session: &mut dyn NativeSession, receiver: mpsc::Receiver<WorkerCommand>) {
    while let Ok(command) = receiver.recv() {
        match command {
            WorkerCommand::Status {
                deadline,
                cancellation,
                response,
            } => {
                let _ = response.send(session.status(deadline, &cancellation));
            }
            WorkerCommand::Capture {
                request,
                deadline,
                response,
            } => {
                let _ = response.send(session.capture(request, deadline));
            }
            WorkerCommand::Close { response } => {
                let result = session.close();
                let _ = response.send(result);
                return;
            }
        }
    }
    let _ = session.close();
}

struct GenicamSessionProxy {
    sender: Option<SyncSender<WorkerCommand>>,
    thread: Option<JoinHandle<()>>,
    capabilities: CameraCapabilities,
    closed: bool,
}

impl GenicamSessionProxy {
    fn submit(&self, command: WorkerCommand) -> Result<()> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| backend_error("GenICam session is closed"))?;
        match sender.try_send(command) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                rejected(ErrorCode::QueueFull, "GenICam native command queue is full")
            }
            Err(TrySendError::Disconnected(_)) => {
                Err(backend_error("GenICam native worker is unavailable"))
            }
        }
    }
}

#[async_trait]
impl CameraSession for GenicamSessionProxy {
    fn capabilities(&self) -> &CameraCapabilities {
        &self.capabilities
    }

    async fn status(&mut self) -> Result<CameraStatus> {
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        let (sender, receiver) = oneshot::channel();
        self.submit(WorkerCommand::Status {
            deadline,
            cancellation: cancellation.clone(),
            response: sender,
        })?;
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline.into()) => {
                cancellation.cancel();
                timeout("reading GenICam status")
            }
            result = receiver => result.map_err(|_| backend_error("GenICam status worker stopped"))?,
        }
    }

    async fn capture(&mut self, request: CaptureRequest) -> Result<CaptureFrame> {
        let deadline = Instant::now() + request.timeout;
        let cancellation = request.cancellation.clone();
        let (sender, receiver) = oneshot::channel();
        self.submit(WorkerCommand::Capture {
            request,
            deadline,
            response: sender,
        })?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => cancelled("during GenICam acquisition"),
            _ = tokio::time::sleep_until(deadline.into()) => {
                cancellation.cancel();
                timeout("during GenICam acquisition")
            }
            result = receiver => result.map_err(|_| backend_error("GenICam capture worker stopped"))?,
        }
    }

    async fn ptz(&mut self, _request: PtzRequest) -> Result<PtzResult> {
        rejected(
            ErrorCode::UnsupportedCapability,
            "GenICam PTZ is not configured or supported",
        )
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let Some(sender) = self.sender.take() else {
            return Ok(());
        };
        let (response_sender, response_receiver) = oneshot::channel();
        match sender.try_send(WorkerCommand::Close {
            response: response_sender,
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                drop(sender);
                return rejected(
                    ErrorCode::QueueFull,
                    "GenICam native queue did not accept close",
                );
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
        drop(sender);
        let deadline = Instant::now() + Duration::from_secs(5);
        let close_result = tokio::select! {
            _ = tokio::time::sleep_until(deadline.into()) => timeout("closing GenICam session"),
            result = response_receiver => result.unwrap_or_else(|_| Ok(())),
        };
        if let Some(thread) = self.thread.take() {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .map_err(|_| backend_error("GenICam close join worker stopped"))?
                .map_err(|_| backend_error("GenICam native worker panicked"))?;
        }
        close_result
    }
}

impl Drop for GenicamSessionProxy {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        if let Some(sender) = self.sender.take() {
            let (response, _receiver) = oneshot::channel();
            let _ = sender.try_send(WorkerCommand::Close { response });
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum WireTransport {
    GigeVision,
    Usb3Vision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireDevice {
    device_id: String,
    physical_id: String,
    vendor: String,
    model: String,
    serial: String,
    address: String,
    transport: WireTransport,
    interface: Option<String>,
}

impl WireDevice {
    fn into_candidate(self) -> DiscoveryCandidate {
        let transport = self.transport;
        DiscoveryCandidate {
            backend: BackendKind::GenicamAravis,
            selector: json!({"deviceId": self.device_id}),
            vendor: nonempty(self.vendor),
            model: nonempty(self.model),
            capabilities: json!({
                "transport": transport,
                "serial": nonempty(self.serial),
                "address": nonempty(self.address),
                "interface": self.interface,
            }),
        }
    }
}

/// Sanitized helper failure. The helper never prints native error strings or discovered identities.
#[derive(Debug)]
pub struct HelperFailure(&'static str);

impl HelperFailure {
    /// Stable operator-safe summary suitable for the helper's bounded stderr.
    pub const fn safe_summary(&self) -> &'static str {
        self.0
    }
}

/// Closed entrypoint for the isolated discovery companion binary.
#[doc(hidden)]
pub fn discovery_helper_main() -> std::result::Result<(), HelperFailure> {
    let (interface, transport, maximum) = parse_helper_args(std::env::args().skip(1))?;
    let devices = scan_once(&interface, transport, maximum)
        .map_err(|_| HelperFailure("native scan failed"))?;
    serde_json::to_writer(std::io::stdout().lock(), &devices)
        .map_err(|_| HelperFailure("JSON output failed"))?;
    Ok(())
}

fn parse_helper_args(
    arguments: impl IntoIterator<Item = String>,
) -> std::result::Result<(String, WireTransport, usize), HelperFailure> {
    let values = arguments.into_iter().collect::<Vec<_>>();
    if values.len() != 6
        || values[0] != "--interface"
        || values[2] != "--transport"
        || values[4] != "--max-results"
    {
        return Err(HelperFailure("invalid closed CLI"));
    }
    let interface = values[1].clone();
    validate_interface(&interface).map_err(|_| HelperFailure("invalid interface"))?;
    let transport = match values[3].as_str() {
        "gige-vision" => WireTransport::GigeVision,
        "usb3-vision" => WireTransport::Usb3Vision,
        _ => return Err(HelperFailure("invalid transport")),
    };
    let maximum = values[5]
        .parse::<usize>()
        .ok()
        .filter(|value| (1..=10_000).contains(value))
        .ok_or(HelperFailure("invalid result bound"))?;
    Ok((interface, transport, maximum))
}

fn scan_once(interface: &str, transport: WireTransport, maximum: usize) -> Result<Vec<WireDevice>> {
    with_scoped_aravis(interface, |aravis, scope| {
        let mut devices = aravis
            .get_device_list()
            .into_iter()
            .enumerate()
            .map(|(index, info)| wire_device(index, info, scope, interface))
            .collect::<Result<Vec<_>>>()?;
        devices.retain(|device| device.transport == transport);
        devices.sort_by(|left, right| left.device_id.cmp(&right.device_id));
        devices.truncate(maximum);
        Ok(devices)
    })
}

fn wire_device(
    index: usize,
    info: aravis::DeviceInfo,
    scope: &aravis_scoped::ScopedDiscovery,
    interface: &str,
) -> Result<WireDevice> {
    let protocol = native_string(&info.protocol, "protocol")?;
    let transport = match protocol.as_str() {
        "GigEVision" => WireTransport::GigeVision,
        "USB3Vision" => WireTransport::Usb3Vision,
        _ => return Err(backend_error("Aravis returned an unsupported transport")),
    };
    let serial = scope
        .serial_number(u32::try_from(index).map_err(|_| backend_error("device index overflow"))?)
        .map_err(|_| backend_error("Aravis returned invalid serial metadata"))?
        .unwrap_or_default();
    validate_native_field(&serial, "serial")?;
    Ok(WireDevice {
        device_id: native_string(&info.id, "device id")?,
        physical_id: native_string(&info.physical_id, "physical id")?,
        vendor: native_string(&info.vendor, "vendor")?,
        model: native_string(&info.model, "model")?,
        serial,
        address: native_string(&info.address, "address")?,
        transport,
        interface: (transport == WireTransport::GigeVision).then(|| interface.to_owned()),
    })
}

fn with_scoped_aravis<T>(
    interface: &str,
    operation: impl FnOnce(&aravis::Aravis, &aravis_scoped::ScopedDiscovery) -> Result<T>,
) -> Result<T> {
    aravis_scoped::with_discovery_interface(Some(interface), |scope| {
        let mut token = ARAVIS_TOKEN.lock().unwrap_or_else(PoisonError::into_inner);
        if token.is_none() {
            *token = Some(
                aravis::Aravis::initialize()
                    .map_err(|_| backend_error("Aravis global initialization was unavailable"))?,
            );
        }
        let aravis = token
            .as_ref()
            .ok_or_else(|| backend_error("Aravis global initialization was unavailable"))?;
        operation(aravis, scope)
    })
    .map_err(|_| backend_error("invalid Aravis discovery scope"))?
}

fn run_discovery_helper(
    interface: &str,
    transport: WireTransport,
    maximum: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<WireDevice>> {
    validate_interface(interface)?;
    check_deadline(deadline, cancellation, "before GenICam helper launch")?;
    let executable = helper_executable()?;
    let mut command = Command::new(executable);
    command
        .arg("--interface")
        .arg(interface)
        .arg("--transport")
        .arg(match transport {
            WireTransport::GigeVision => "gige-vision",
            WireTransport::Usb3Vision => "usb3-vision",
        })
        .arg("--max-results")
        .arg(maximum.to_string())
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(library_path) = std::env::var_os("LD_LIBRARY_PATH") {
        command.env("LD_LIBRARY_PATH", library_path);
    }
    let mut child = command
        .spawn()
        .map_err(|_| backend_error("failed to launch GenICam discovery helper"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| backend_error("GenICam helper stdout was unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| backend_error("GenICam helper stderr was unavailable"))?;
    let stdout_reader = std::thread::spawn(move || read_bounded(stdout, MAX_HELPER_STDOUT_BYTES));
    let stderr_reader = std::thread::spawn(move || read_bounded(stderr, MAX_HELPER_STDERR_BYTES));

    let status = wait_for_child(&mut child, deadline, cancellation);
    if status.is_err() {
        // `try_wait` can itself fail.  Do not leave a helper alive merely because the
        // observation path failed; its pipes otherwise also keep the reader threads alive.
        reap_child(&mut child)?;
    }
    let stdout = stdout_reader
        .join()
        .map_err(|_| backend_error("GenICam helper stdout reader stopped"))??;
    let _stderr = stderr_reader
        .join()
        .map_err(|_| backend_error("GenICam helper stderr reader stopped"))??;
    let status = status?;
    if !status.success() {
        return Err(backend_error("GenICam discovery helper failed"));
    }
    let devices: Vec<WireDevice> = serde_json::from_slice(&stdout)
        .map_err(|_| backend_error("GenICam helper returned invalid JSON"))?;
    if devices.len() > maximum
        || devices.iter().any(|device| {
            device.transport != transport
                || (transport == WireTransport::GigeVision
                    && device.interface.as_deref() != Some(interface))
        })
    {
        return Err(backend_error("GenICam helper violated its result contract"));
    }
    validate_helper_devices(&devices, interface, transport)?;
    Ok(devices)
}

fn validate_helper_devices(
    devices: &[WireDevice],
    interface: &str,
    transport: WireTransport,
) -> Result<()> {
    let mut identities = BTreeSet::new();
    for device in devices {
        for (value, label) in [
            (&device.device_id, "device id"),
            (&device.physical_id, "physical id"),
            (&device.vendor, "vendor"),
            (&device.model, "model"),
            (&device.serial, "serial"),
            (&device.address, "address"),
        ] {
            validate_native_field(value, label)?;
        }
        if device.device_id.is_empty()
            || !identities.insert((device.transport, device.device_id.as_str()))
        {
            return Err(backend_error(
                "GenICam helper returned an invalid device identity",
            ));
        }
        match (transport, device.interface.as_deref()) {
            (WireTransport::GigeVision, Some(found)) if found == interface => {}
            (WireTransport::Usb3Vision, None) => {}
            _ => return Err(backend_error("GenICam helper violated its interface scope")),
        }
    }
    Ok(())
}

fn helper_executable() -> Result<PathBuf> {
    let current = std::env::current_exe()
        .map_err(|_| backend_error("cannot locate GenICam discovery helper"))?;
    let file_name = if cfg!(windows) {
        "camera-adapter-genicam-discover.exe"
    } else {
        "camera-adapter-genicam-discover"
    };
    let directory = current
        .parent()
        .ok_or_else(|| backend_error("cannot locate GenICam discovery helper"))?;
    let direct = directory.join(file_name);
    if direct.is_file() {
        return Ok(direct);
    }
    if directory.file_name().and_then(|value| value.to_str()) == Some("deps") {
        let test_sibling = directory
            .parent()
            .ok_or_else(|| backend_error("cannot locate GenICam discovery helper"))?
            .join(file_name);
        if test_sibling.is_file() {
            return Ok(test_sibling);
        }
    }
    Err(backend_error(
        "required GenICam discovery helper is not installed beside the adapter",
    ))
}

fn read_bounded(mut reader: impl Read, maximum: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| backend_error("failed to read bounded GenICam helper output"))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum {
        return Err(backend_error(
            "GenICam helper output exceeded its byte limit",
        ));
    }
    Ok(bytes)
}

fn wait_for_child(
    child: &mut Child,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<std::process::ExitStatus> {
    loop {
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            reap_child(child)?;
            return if cancellation.is_cancelled() {
                cancelled("during GenICam helper discovery")
            } else {
                timeout("during GenICam helper discovery")
            };
        }
        let status = match child.try_wait() {
            Ok(status) => status,
            Err(_) => {
                reap_child(child)?;
                return Err(backend_error("failed to observe GenICam helper"));
            }
        };
        if let Some(status) = status {
            return Ok(status);
        }
        std::thread::sleep(PROCESS_POLL);
    }
}

/// Terminates and waits for a helper before returning control to the caller.
///
/// A failed `kill` is harmless when the process won the exit race, but a failed `wait` means we
/// cannot honestly claim that the process was reaped.
fn reap_child(child: &mut Child) -> Result<()> {
    let _ = child.kill();
    match child.wait() {
        Ok(_) => Ok(()),
        // A previous cancellation/deadline branch may already have reaped the child before a
        // higher-level error cleanup reaches here.
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => Ok(()),
        Err(_) => Err(backend_error("failed to reap GenICam discovery helper")),
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ProfileKey {
    pixel_format: Option<PixelFormat>,
    width: Option<u32>,
    height: Option<u32>,
    offset_x: Option<u32>,
    offset_y: Option<u32>,
    exposure_micros: Option<u64>,
    gain: Option<f64>,
}

impl From<&CaptureProfile> for ProfileKey {
    fn from(profile: &CaptureProfile) -> Self {
        Self {
            pixel_format: profile.pixel_format,
            width: profile.width,
            height: profile.height,
            offset_x: profile.offset_x,
            offset_y: profile.offset_y,
            exposure_micros: profile.exposure_micros,
            gain: profile.gain,
        }
    }
}

struct AravisNativeSession {
    camera: aravis::Camera,
    stream: Option<aravis::Stream>,
    buffer_capacity: usize,
    buffer_count: usize,
    capabilities: CameraCapabilities,
    transport: WireTransport,
    interface: Option<String>,
    packet_size: Option<u32>,
    packet_delay_ns: Option<i64>,
    profile: Option<ProfileKey>,
    calibration: TimestampCalibration,
    acquisition_started: bool,
    closed: bool,
}

impl NativeSession for AravisNativeSession {
    fn capabilities(&self) -> &CameraCapabilities {
        &self.capabilities
    }

    fn status(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CameraStatus> {
        check_deadline(deadline, cancellation, "reading GenICam status")?;
        Ok(CameraStatus {
            online: !self.closed,
            connection_generation: 1,
            ptz: None,
            backend: json!({
                "transport": self.transport,
                "interface": self.interface,
                "packetSize": self.packet_size,
                "packetDelayNs": self.packet_delay_ns,
                "bufferCount": self.buffer_count,
                "acquisitionActive": self.acquisition_started,
            }),
        })
    }

    fn capture(&mut self, request: CaptureRequest, deadline: Instant) -> Result<CaptureFrame> {
        check_deadline(
            deadline,
            &request.cancellation,
            "before GenICam acquisition",
        )?;
        if request
            .profile
            .capture_mode
            .is_some_and(|mode| mode != CaptureMode::SoftwareTrigger)
        {
            return rejected(
                ErrorCode::UnsupportedCapability,
                "GenICam supports only software-trigger capture mode",
            );
        }
        if !self.capabilities.software_trigger {
            return rejected(
                ErrorCode::UnsupportedCapability,
                "camera does not advertise a Software trigger source",
            );
        }
        self.configure_profile(&request.profile, request.maximum_frame_bytes)?;
        let stream = self
            .stream
            .as_ref()
            .ok_or_else(|| backend_error("GenICam stream was not configured"))?;
        while let Some(buffer) = stream.try_pop_buffer() {
            stream.push_buffer(buffer);
        }
        self.camera
            .software_trigger()
            .map_err(|_| backend_error("GenICam software trigger failed"))?;

        loop {
            if let Err(error) = check_deadline(
                deadline,
                &request.cancellation,
                "during GenICam buffer acquisition",
            ) {
                self.reset_acquisition();
                return Err(error);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = remaining.min(NATIVE_POLL);
            let micros = u64::try_from(wait.as_micros()).unwrap_or(u64::MAX).max(1);
            let Some(buffer) = self
                .stream
                .as_ref()
                .and_then(|stream| stream.timeout_pop_buffer(micros))
            else {
                continue;
            };
            let result = frame_from_buffer(
                &buffer,
                request.maximum_frame_bytes,
                self.transport,
                &mut self.calibration,
            );
            let stream = self
                .stream
                .as_ref()
                .ok_or_else(|| backend_error("GenICam stream was removed during acquisition"))?;
            stream.push_buffer(buffer);
            return result;
        }
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.reset_acquisition();
        Ok(())
    }
}

impl AravisNativeSession {
    fn configure_profile(&mut self, profile: &CaptureProfile, maximum_bytes: u64) -> Result<()> {
        let key = ProfileKey::from(profile);
        if self.profile.as_ref() == Some(&key) {
            if u64::try_from(self.buffer_capacity).unwrap_or(u64::MAX) > maximum_bytes {
                return rejected(
                    ErrorCode::ResourceLimit,
                    "configured GenICam payload exceeds the accepted frame reservation",
                );
            }
            return Ok(());
        }
        self.reset_acquisition();
        apply_profile(&self.camera, profile)?;
        let payload = usize::try_from(
            self.camera
                .payload()
                .map_err(|_| backend_error("failed to read GenICam payload size"))?,
        )
        .map_err(|_| backend_error("GenICam payload size is not representable"))?;
        if payload == 0 || u64::try_from(payload).unwrap_or(u64::MAX) > maximum_bytes {
            return rejected(
                ErrorCode::ResourceLimit,
                "configured GenICam payload exceeds the accepted frame reservation",
            );
        }
        let stream = self
            .camera
            .create_stream()
            .map_err(|_| backend_error("failed to create GenICam stream"))?;
        for _ in 0..self.buffer_count {
            stream.push_buffer(aravis::Buffer::new_allocate(payload));
        }
        self.camera
            .start_acquisition()
            .map_err(|_| backend_error("failed to start GenICam acquisition"))?;
        self.stream = Some(stream);
        self.buffer_capacity = payload;
        self.profile = Some(key);
        self.acquisition_started = true;
        Ok(())
    }

    fn reset_acquisition(&mut self) {
        if self.acquisition_started {
            let _ = self.camera.abort_acquisition();
            let _ = self.camera.stop_acquisition();
        }
        self.stream = None;
        self.buffer_capacity = 0;
        self.profile = None;
        self.acquisition_started = false;
    }
}

#[derive(Default)]
struct TimestampCalibration {
    offset_ns: Option<i128>,
    last_system_ns: u64,
}

impl TimestampCalibration {
    fn resolve(
        &mut self,
        camera_ns: u64,
        system_ns: u64,
    ) -> (Option<DateTime<Utc>>, FrameTimestampQuality) {
        if system_ns == 0 {
            return (None, FrameTimestampQuality::Unknown);
        }
        if camera_ns == 0 || camera_ns == system_ns {
            return (
                datetime_from_ns(i128::from(system_ns)),
                FrameTimestampQuality::AdapterReceive,
            );
        }
        if self.offset_ns.is_none()
            || system_ns.saturating_sub(self.last_system_ns) >= TIMESTAMP_REFRESH_NS
        {
            self.offset_ns = Some(i128::from(system_ns) - i128::from(camera_ns));
            self.last_system_ns = system_ns;
        }
        (
            datetime_from_ns(i128::from(camera_ns) + self.offset_ns.unwrap_or_default()),
            FrameTimestampQuality::Camera,
        )
    }
}

fn datetime_from_ns(value: i128) -> Option<DateTime<Utc>> {
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = u32::try_from(value.rem_euclid(1_000_000_000)).ok()?;
    DateTime::from_timestamp(i64::try_from(seconds).ok()?, nanos)
}

fn frame_from_buffer(
    buffer: &aravis::Buffer,
    maximum_bytes: u64,
    transport: WireTransport,
    calibration: &mut TimestampCalibration,
) -> Result<CaptureFrame> {
    if buffer.status() != aravis::BufferStatus::Success {
        return Err(backend_error(
            "GenICam returned an incomplete acquisition buffer",
        ));
    }
    let payload = buffer.payload_type();
    if !matches!(
        payload,
        aravis::BufferPayloadType::Image
            | aravis::BufferPayloadType::ExtendedChunkData
            | aravis::BufferPayloadType::Jpeg
    ) {
        return rejected(
            ErrorCode::UnsupportedPixelFormat,
            "GenICam buffer payload type is unsupported",
        );
    }
    let bytes = buffer.image_data();
    if bytes.is_empty() || u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum_bytes {
        return rejected(
            ErrorCode::ResourceLimit,
            "GenICam frame exceeds the accepted byte reservation",
        );
    }
    let (width, height, pixel_format) = if payload == aravis::BufferPayloadType::Jpeg {
        let (width, height) =
            ImageReader::with_format(Cursor::new(&bytes), image::ImageFormat::Jpeg)
                .into_dimensions()
                .map_err(|_| {
                    CameraError::rejected(
                        ErrorCode::UnsupportedPixelFormat,
                        "GenICam JPEG payload is invalid",
                    )
                })?;
        (width, height, PixelFormat::Jpeg)
    } else {
        let width = positive_dimension(buffer.image_width(), "width")?;
        let height = positive_dimension(buffer.image_height(), "height")?;
        let (x_padding, y_padding) = buffer.image_padding();
        if x_padding != 0 || y_padding != 0 {
            return rejected(
                ErrorCode::UnsupportedPixelFormat,
                "padded GenICam frames are unsupported",
            );
        }
        let pixel_format = from_aravis_pixel_format(buffer.image_pixel_format())?;
        let expected = pixel_format
            .uncompressed_len(width, height)
            .ok_or_else(|| backend_error("GenICam frame size overflow"))?;
        if expected != u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
            return Err(backend_error("GenICam frame payload size mismatch"));
        }
        (width, height, pixel_format)
    };
    let camera_ns = buffer.timestamp();
    let system_ns = buffer.system_timestamp();
    let (source_timestamp, timestamp_quality) = calibration.resolve(camera_ns, system_ns);
    let mut backend_metadata = BTreeMap::new();
    backend_metadata.insert("frameId".to_owned(), json!(buffer.frame_id()));
    backend_metadata.insert("hasChunks".to_owned(), json!(buffer.has_chunks()));
    backend_metadata.insert("cameraTimestampNs".to_owned(), json!(camera_ns));
    backend_metadata.insert("systemTimestampNs".to_owned(), json!(system_ns));
    backend_metadata.insert("transport".to_owned(), json!(transport));
    Ok(CaptureFrame {
        bytes: Bytes::from(bytes),
        width,
        height,
        pixel_format,
        capture_mode: CaptureMode::SoftwareTrigger,
        source_timestamp,
        timestamp_quality,
        backend_metadata,
    })
}

fn positive_dimension(value: i32, label: &'static str) -> Result<u32> {
    u32::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| backend_error(format!("GenICam returned invalid {label}")))
}

fn from_aravis_pixel_format(format: aravis::PixelFormat) -> Result<PixelFormat> {
    if format == aravis::PixelFormat::MONO_8 {
        Ok(PixelFormat::Mono8)
    } else if format == aravis::PixelFormat::RGB_8_PACKED {
        Ok(PixelFormat::Rgb8)
    } else if format == aravis::PixelFormat::BGR_8_PACKED {
        Ok(PixelFormat::Bgr8)
    } else {
        rejected(
            ErrorCode::UnsupportedPixelFormat,
            "GenICam pixel format is unsupported",
        )
    }
}

fn to_aravis_pixel_format(format: PixelFormat) -> Option<aravis::PixelFormat> {
    match format {
        PixelFormat::Mono8 => Some(aravis::PixelFormat::MONO_8),
        PixelFormat::Rgb8 => Some(aravis::PixelFormat::RGB_8_PACKED),
        PixelFormat::Bgr8 => Some(aravis::PixelFormat::BGR_8_PACKED),
        PixelFormat::Jpeg => None,
    }
}

fn apply_profile(camera: &aravis::Camera, profile: &CaptureProfile) -> Result<()> {
    let (current_x, current_y, current_width, current_height) = camera
        .region()
        .map_err(|_| backend_error("failed to read GenICam region"))?;
    let x = requested_i32(profile.offset_x, current_x, "offsetX")?;
    let y = requested_i32(profile.offset_y, current_y, "offsetY")?;
    let width = requested_i32(profile.width, current_width, "width")?;
    let height = requested_i32(profile.height, current_height, "height")?;
    validate_axis(
        camera.x_offset_bounds(),
        camera.x_offset_increment(),
        x,
        "offsetX",
    )?;
    validate_axis(
        camera.y_offset_bounds(),
        camera.y_offset_increment(),
        y,
        "offsetY",
    )?;
    validate_axis(
        camera.width_bounds(),
        camera.width_increment(),
        width,
        "width",
    )?;
    validate_axis(
        camera.height_bounds(),
        camera.height_increment(),
        height,
        "height",
    )?;
    camera
        .set_region(x, y, width, height)
        .map_err(|_| backend_error("failed to configure GenICam region"))?;
    if camera
        .region()
        .map_err(|_| backend_error("failed to verify GenICam region"))?
        != (x, y, width, height)
    {
        return Err(backend_error("GenICam region readback mismatch"));
    }

    if let Some(format) = profile.pixel_format {
        if let Some(native) = to_aravis_pixel_format(format) {
            camera
                .set_pixel_format(native)
                .map_err(|_| backend_error("failed to configure GenICam pixel format"))?;
        } else {
            camera
                .set_pixel_format_from_string("JPEG")
                .map_err(|_| backend_error("failed to configure GenICam JPEG format"))?;
        }
        let actual = camera
            .pixel_format_as_string()
            .map_err(|_| backend_error("failed to verify GenICam pixel format"))?;
        if pixel_format_name(format) != actual.as_str() {
            return Err(backend_error("GenICam pixel-format readback mismatch"));
        }
    }
    if let Some(exposure) = profile.exposure_micros {
        let value = exposure as f64;
        validate_float_feature(camera, "ExposureTime", value)?;
        camera
            .set_exposure_time(value)
            .map_err(|_| backend_error("failed to configure GenICam exposure"))?;
        verify_float_readback(
            value,
            camera
                .exposure_time()
                .map_err(|_| backend_error("failed to verify GenICam exposure"))?,
            "exposure",
        )?;
    }
    if let Some(gain) = profile.gain {
        validate_float_feature(camera, "Gain", gain)?;
        camera
            .set_gain(gain)
            .map_err(|_| backend_error("failed to configure GenICam gain"))?;
        verify_float_readback(
            gain,
            camera
                .gain()
                .map_err(|_| backend_error("failed to verify GenICam gain"))?,
            "gain",
        )?;
    }
    camera
        .set_acquisition_mode(aravis::AcquisitionMode::Continuous)
        .map_err(|_| backend_error("failed to configure GenICam acquisition mode"))?;
    camera
        .set_trigger("Software")
        .map_err(|_| backend_error("failed to configure GenICam software trigger"))?;
    let trigger = camera
        .string("TriggerSource")
        .map_err(|_| backend_error("failed to verify GenICam trigger source"))?;
    if trigger.as_str() != "Software" {
        return Err(backend_error("GenICam trigger-source readback mismatch"));
    }
    Ok(())
}

fn requested_i32(value: Option<u32>, current: i32, label: &'static str) -> Result<i32> {
    value.map_or(Ok(current), |value| {
        i32::try_from(value).map_err(|_| {
            CameraError::rejected(
                ErrorCode::InvalidRequest,
                format!("GenICam {label} exceeds the native range"),
            )
        })
    })
}

fn validate_axis(
    bounds: std::result::Result<(i32, i32), aravis::glib::Error>,
    increment: std::result::Result<i32, aravis::glib::Error>,
    value: i32,
    label: &'static str,
) -> Result<()> {
    let (minimum, maximum) = bounds.map_err(|_| backend_error("failed to read GenICam bounds"))?;
    let increment = increment
        .map_err(|_| backend_error("failed to read GenICam increment"))?
        .max(1);
    if !(minimum..=maximum).contains(&value) || (value - minimum) % increment != 0 {
        return rejected(
            ErrorCode::InvalidRequest,
            format!("GenICam {label} violates device bounds or increment"),
        );
    }
    Ok(())
}

fn validate_float_feature(camera: &aravis::Camera, feature: &str, value: f64) -> Result<()> {
    let (minimum, maximum) = camera
        .float_bounds(feature)
        .map_err(|_| backend_error("failed to read GenICam numeric bounds"))?;
    let increment = camera
        .float_increment(feature)
        .map_err(|_| backend_error("failed to read GenICam numeric increment"))?;
    if !value.is_finite() || !(minimum..=maximum).contains(&value) {
        return rejected(
            ErrorCode::InvalidRequest,
            "GenICam numeric feature violates device bounds",
        );
    }
    if increment.is_finite() && increment > 0.0 {
        let steps = (value - minimum) / increment;
        if (steps - steps.round()).abs() > 1e-6 {
            return rejected(
                ErrorCode::InvalidRequest,
                "GenICam numeric feature violates its device increment",
            );
        }
    }
    Ok(())
}

fn verify_float_readback(expected: f64, actual: f64, label: &'static str) -> Result<()> {
    let tolerance = expected.abs().max(1.0) * 1e-9;
    if !actual.is_finite() || (actual - expected).abs() > tolerance {
        return Err(backend_error(format!("GenICam {label} readback mismatch")));
    }
    Ok(())
}

struct ConnectionSettings {
    packet_size: Option<u32>,
    packet_delay_ns: Option<i64>,
}

fn apply_connection_settings(
    camera: &aravis::Camera,
    config: &GenicamBackendConfig,
    transport: WireTransport,
) -> Result<ConnectionSettings> {
    let (packet_size, packet_delay_ns) = if transport == WireTransport::GigeVision {
        let packet_size = match config.packet_size.as_ref() {
            Some(value) if value.as_str() == Some("auto") => {
                camera
                    .gv_auto_packet_size()
                    .map_err(|_| backend_error("GenICam packet-size auto negotiation failed"))?;
                Some(
                    camera
                        .gv_get_packet_size()
                        .map_err(|_| backend_error("failed to verify GenICam packet size"))?,
                )
            }
            Some(value) => {
                let requested = value
                    .as_u64()
                    .and_then(|value| i32::try_from(value).ok())
                    .ok_or_else(|| backend_error("invalid validated GenICam packet size"))?;
                camera
                    .gv_set_packet_size(requested)
                    .map_err(|_| backend_error("failed to configure GenICam packet size"))?;
                let actual = camera
                    .gv_get_packet_size()
                    .map_err(|_| backend_error("failed to verify GenICam packet size"))?;
                if actual != requested as u32 {
                    return Err(backend_error("GenICam packet-size readback mismatch"));
                }
                Some(actual)
            }
            None => camera.gv_get_packet_size().ok(),
        };
        let packet_delay_ns = match config.packet_delay_ns {
            Some(requested) => {
                let requested = i64::try_from(requested).map_err(|_| {
                    CameraError::rejected(
                        ErrorCode::InvalidRequest,
                        "GenICam packetDelayNs exceeds the native range",
                    )
                })?;
                camera
                    .gv_set_packet_delay(requested)
                    .map_err(|_| backend_error("failed to configure GenICam packet delay"))?;
                let actual = camera
                    .gv_get_packet_delay()
                    .map_err(|_| backend_error("failed to verify GenICam packet delay"))?;
                if actual != requested {
                    return Err(backend_error("GenICam packet-delay readback mismatch"));
                }
                Some(actual)
            }
            None => camera.gv_get_packet_delay().ok(),
        };
        (packet_size, packet_delay_ns)
    } else {
        (None, None)
    };
    apply_feature_overrides(camera, &config.feature_overrides)?;
    Ok(ConnectionSettings {
        packet_size,
        packet_delay_ns,
    })
}

fn apply_feature_overrides(
    camera: &aravis::Camera,
    overrides: &BTreeMap<String, Value>,
) -> Result<()> {
    let device = camera
        .device()
        .ok_or_else(|| backend_error("GenICam camera has no device object"))?;
    for (name, value) in overrides {
        if device.feature_access_mode(name) != aravis::GcAccessMode::Rw {
            return rejected(
                ErrorCode::UnsupportedCapability,
                format!("GenICam feature '{name}' is unavailable or not read-write"),
            );
        }
        match name.as_str() {
            "AcquisitionFrameRateEnable" | "ReverseX" | "ReverseY" => {
                let expected = value
                    .as_bool()
                    .ok_or_else(|| backend_error("invalid validated boolean override"))?;
                camera
                    .set_boolean(name, expected)
                    .map_err(|_| backend_error("failed to set GenICam boolean override"))?;
                if camera
                    .boolean(name)
                    .map_err(|_| backend_error("failed to verify GenICam boolean override"))?
                    != expected
                {
                    return Err(backend_error("GenICam boolean override readback mismatch"));
                }
            }
            "DeviceLinkThroughputLimit" => {
                let expected = value
                    .as_i64()
                    .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                    .ok_or_else(|| backend_error("invalid validated integer override"))?;
                camera
                    .set_integer(name, expected)
                    .map_err(|_| backend_error("failed to set GenICam integer override"))?;
                if camera
                    .integer(name)
                    .map_err(|_| backend_error("failed to verify GenICam integer override"))?
                    != expected
                {
                    return Err(backend_error("GenICam integer override readback mismatch"));
                }
            }
            "AcquisitionFrameRate" | "BlackLevel" | "Gamma" => {
                let expected = value
                    .as_f64()
                    .ok_or_else(|| backend_error("invalid validated float override"))?;
                camera
                    .set_float(name, expected)
                    .map_err(|_| backend_error("failed to set GenICam float override"))?;
                verify_float_readback(
                    expected,
                    camera
                        .float(name)
                        .map_err(|_| backend_error("failed to verify GenICam float override"))?,
                    "feature override",
                )?;
            }
            "BalanceWhiteAuto" | "BlackLevelAuto" => {
                let expected = value
                    .as_str()
                    .ok_or_else(|| backend_error("invalid validated string override"))?;
                camera
                    .set_string(name, expected)
                    .map_err(|_| backend_error("failed to set GenICam string override"))?;
                if camera
                    .string(name)
                    .map_err(|_| backend_error("failed to verify GenICam string override"))?
                    .as_str()
                    != expected
                {
                    return Err(backend_error("GenICam string override readback mismatch"));
                }
            }
            _ => return Err(backend_error("unknown validated GenICam feature override")),
        }
    }
    Ok(())
}

fn read_capabilities(camera: &aravis::Camera, identity: &WireDevice) -> Result<CameraCapabilities> {
    let formats = camera
        .dup_available_pixel_formats_as_strings()
        .map_err(|_| backend_error("failed to read GenICam pixel formats"))?;
    let mut pixel_formats = BTreeMap::new();
    for format in formats {
        if let Some(format) = pixel_format_from_name(format.as_str()) {
            pixel_formats.insert(format_token(format), format);
        }
    }
    let software_trigger = camera
        .dup_available_trigger_sources()
        .map(|sources| sources.iter().any(|source| source.as_str() == "Software"))
        .unwrap_or(false);
    let firmware = camera.string("DeviceVersion").ok().and_then(|value| {
        let value = value.to_string();
        validate_native_field(&value, "firmware").ok()?;
        nonempty(value)
    });
    let mut warnings = Vec::new();
    if pixel_formats.is_empty() {
        warnings.push("camera exposes none of Mono8, RGB8, BGR8, or JPEG".to_owned());
    }
    if !software_trigger {
        warnings.push("camera does not expose Software trigger source".to_owned());
    }
    Ok(CameraCapabilities {
        capture_modes: software_trigger
            .then_some(CaptureMode::SoftwareTrigger)
            .into_iter()
            .collect(),
        pixel_formats: pixel_formats.into_values().collect(),
        software_trigger,
        snapshot_uri: false,
        rtsp: false,
        ptz: false,
        ptz_status: false,
        presets: false,
        preset_mutation: false,
        vendor: nonempty(identity.vendor.clone()),
        model: nonempty(identity.model.clone()),
        firmware,
        serial: nonempty(identity.serial.clone()),
        warnings,
    })
}

fn resolve_and_open(
    config: &GenicamBackendConfig,
) -> Result<(aravis::Camera, WireDevice, WireTransport, Option<String>)> {
    match config.transport {
        GenicamTransport::GigeVision => {
            let interface = required_interface(config)?;
            let (camera, identity) = open_scoped(config, WireTransport::GigeVision, interface)?;
            Ok((
                camera,
                identity,
                WireTransport::GigeVision,
                Some(interface.to_owned()),
            ))
        }
        GenicamTransport::Usb3Vision => {
            let (camera, identity) =
                open_scoped(config, WireTransport::Usb3Vision, NO_GIGE_INTERFACE)?;
            Ok((camera, identity, WireTransport::Usb3Vision, None))
        }
        GenicamTransport::Auto => {
            if config.selector.ip.is_some() || config.selector.mac.is_some() {
                let interface = required_interface(config)?;
                let (camera, identity) = open_scoped(config, WireTransport::GigeVision, interface)?;
                return Ok((
                    camera,
                    identity,
                    WireTransport::GigeVision,
                    Some(interface.to_owned()),
                ));
            }
            match open_scoped(config, WireTransport::Usb3Vision, NO_GIGE_INTERFACE) {
                Ok((camera, identity)) => {
                    return Ok((camera, identity, WireTransport::Usb3Vision, None));
                }
                Err(CameraError::Rejected {
                    code: ErrorCode::CameraUnavailable,
                    ..
                }) => {}
                Err(error) => return Err(error),
            }
            let interface = required_interface(config)?;
            let (camera, identity) = open_scoped(config, WireTransport::GigeVision, interface)?;
            Ok((
                camera,
                identity,
                WireTransport::GigeVision,
                Some(interface.to_owned()),
            ))
        }
    }
}

fn required_interface(config: &GenicamBackendConfig) -> Result<&str> {
    config.interface.as_deref().ok_or_else(|| {
        CameraError::rejected(
            ErrorCode::InvalidRequest,
            "GigE GenICam connection requires an explicit OS interface",
        )
    })
}

fn open_scoped(
    config: &GenicamBackendConfig,
    transport: WireTransport,
    interface: &str,
) -> Result<(aravis::Camera, WireDevice)> {
    with_scoped_aravis(interface, |aravis, scope| {
        let mut matches = aravis
            .get_device_list()
            .into_iter()
            .enumerate()
            .map(|(index, info)| wire_device(index, info, scope, interface))
            .collect::<Result<Vec<_>>>()?;
        matches.retain(|device| device.transport == transport && selector_matches(config, device));
        if matches.len() > 1 {
            return rejected(
                ErrorCode::InvalidRequest,
                "GenICam stable selector is ambiguous on its configured interface",
            );
        }
        let identity = match matches.pop() {
            Some(identity) => identity,
            None if transport == WireTransport::GigeVision && config.selector.ip.is_some() => {
                let ip = config.selector.ip.as_deref().ok_or_else(|| {
                    backend_error("GenICam IP selector was lost during selector resolution")
                })?;
                let camera = aravis::Camera::new(Some(ip))
                    .map_err(|_| unavailable("GenICam camera was not reachable by explicit IP"))?;
                let identity = identity_from_camera(&camera, transport, interface, ip)?;
                if !selector_matches(config, &identity) {
                    return Err(backend_error(
                        "opened GenICam identity did not match selector",
                    ));
                }
                return Ok((camera, identity));
            }
            None => return Err(unavailable("GenICam selector did not match a camera")),
        };
        let camera = aravis::Camera::new(Some(&identity.device_id))
            .map_err(|_| unavailable("GenICam camera open failed"))?;
        let verified =
            identity_from_camera(&camera, transport, interface, identity.address.as_str())?;
        if verified.device_id != identity.device_id || !selector_matches(config, &verified) {
            return Err(backend_error(
                "opened GenICam identity changed during binding",
            ));
        }
        Ok((camera, verified))
    })
}

fn identity_from_camera(
    camera: &aravis::Camera,
    transport: WireTransport,
    interface: &str,
    address: &str,
) -> Result<WireDevice> {
    let device_id = camera
        .device_id()
        .map_err(|_| backend_error("failed to verify GenICam device id"))?
        .to_string();
    let vendor = camera
        .vendor_name()
        .map_err(|_| backend_error("failed to verify GenICam vendor"))?
        .to_string();
    let model = camera
        .model_name()
        .map_err(|_| backend_error("failed to verify GenICam model"))?
        .to_string();
    let serial = camera
        .string("DeviceSerialNumber")
        .or_else(|_| camera.string("DeviceID"))
        .map_err(|_| backend_error("failed to verify GenICam serial"))?
        .to_string();
    for (value, label) in [
        (&device_id, "device id"),
        (&vendor, "vendor"),
        (&model, "model"),
        (&serial, "serial"),
    ] {
        validate_native_field(value, label)?;
    }
    Ok(WireDevice {
        device_id,
        physical_id: String::new(),
        vendor,
        model,
        serial,
        address: address.to_owned(),
        transport,
        interface: (transport == WireTransport::GigeVision).then(|| interface.to_owned()),
    })
}

fn selector_matches(config: &GenicamBackendConfig, device: &WireDevice) -> bool {
    if let Some(expected) = config.selector.serial.as_deref() {
        device.serial == expected
    } else if let Some(expected) = config.selector.mac.as_deref() {
        normalize_mac(&device.physical_id).as_deref() == normalize_mac(expected).as_deref()
    } else if let Some(expected) = config.selector.device_id.as_deref() {
        device.device_id == expected
    } else if let Some(expected) = config.selector.ip.as_deref() {
        device.address == expected
    } else {
        false
    }
}

fn normalize_mac(value: &str) -> Option<String> {
    let compact = value
        .chars()
        .filter(|character| *character != ':' && *character != '-')
        .collect::<String>();
    (compact.len() == 12 && compact.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| compact.to_ascii_lowercase())
}

fn pixel_format_from_name(value: &str) -> Option<PixelFormat> {
    match value {
        "Mono8" => Some(PixelFormat::Mono8),
        "RGB8" | "RGB8Packed" => Some(PixelFormat::Rgb8),
        "BGR8" | "BGR8Packed" => Some(PixelFormat::Bgr8),
        "JPEG" => Some(PixelFormat::Jpeg),
        _ => None,
    }
}

const fn pixel_format_name(value: PixelFormat) -> &'static str {
    match value {
        PixelFormat::Mono8 => "Mono8",
        PixelFormat::Rgb8 => "RGB8",
        PixelFormat::Bgr8 => "BGR8",
        PixelFormat::Jpeg => "JPEG",
    }
}

const fn format_token(value: PixelFormat) -> u8 {
    match value {
        PixelFormat::Mono8 => 0,
        PixelFormat::Rgb8 => 1,
        PixelFormat::Bgr8 => 2,
        PixelFormat::Jpeg => 3,
    }
}

fn native_string(value: &CString, label: &'static str) -> Result<String> {
    let value = value
        .to_str()
        .map_err(|_| backend_error(format!("Aravis returned non-UTF-8 {label}")))?
        .to_owned();
    validate_native_field(&value, label)?;
    Ok(value)
}

fn validate_native_field(value: &str, label: &'static str) -> Result<()> {
    if value.len() > MAX_NATIVE_FIELD_BYTES || value.chars().any(char::is_control) {
        return Err(backend_error(format!(
            "Aravis returned invalid {label} metadata"
        )));
    }
    Ok(())
}

fn validate_interfaces(interfaces: &[String]) -> Result<()> {
    if interfaces.len() > 64 {
        return rejected(
            ErrorCode::InvalidRequest,
            "GenICam discovery accepts at most 64 eligible interfaces",
        );
    }
    let mut unique = BTreeSet::new();
    for interface in interfaces {
        validate_interface(interface)?;
        if !unique.insert(interface) {
            return rejected(
                ErrorCode::InvalidRequest,
                "GenICam discovery interfaces must be distinct",
            );
        }
    }
    Ok(())
}

fn validate_interface(interface: &str) -> Result<()> {
    if interface.is_empty() || interface.len() > 256 || interface.chars().any(char::is_control) {
        return rejected(
            ErrorCode::InvalidRequest,
            "GenICam interface must be 1..256 UTF-8 bytes without controls",
        );
    }
    Ok(())
}

fn check_deadline(
    deadline: Instant,
    cancellation: &CancellationToken,
    stage: &'static str,
) -> Result<()> {
    if cancellation.is_cancelled() {
        cancelled(stage)
    } else if Instant::now() >= deadline {
        timeout(stage)
    } else {
        Ok(())
    }
}

fn bounded_thread_label(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
        .take(24)
        .collect()
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn rejected<T>(code: ErrorCode, message: impl Into<std::borrow::Cow<'static, str>>) -> Result<T> {
    Err(CameraError::rejected(code, message))
}

fn cancelled<T>(stage: &'static str) -> Result<T> {
    rejected(
        ErrorCode::CaptureCancelled,
        format!("capture cancelled {stage}"),
    )
}

fn timeout<T>(stage: &'static str) -> Result<T> {
    rejected(
        ErrorCode::CaptureTimeout,
        format!("capture deadline expired {stage}"),
    )
}

fn unavailable(message: &'static str) -> CameraError {
    CameraError::rejected(ErrorCode::CameraUnavailable, message)
}

fn backend_error(message: impl Into<String>) -> CameraError {
    CameraError::Backend {
        backend: BACKEND,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::config::{CaptureInterlock, GenicamSelector, OfflinePolicy, ProfileOutputConfig};
    use crate::model::OutputEncoding;

    fn capabilities() -> CameraCapabilities {
        CameraCapabilities {
            capture_modes: vec![CaptureMode::SoftwareTrigger],
            pixel_formats: vec![PixelFormat::Mono8],
            software_trigger: true,
            snapshot_uri: false,
            rtsp: false,
            ptz: false,
            ptz_status: false,
            presets: false,
            preset_mutation: false,
            vendor: Some("test".to_owned()),
            model: Some("thread-affine".to_owned()),
            firmware: None,
            serial: Some("serial-1".to_owned()),
            warnings: Vec::new(),
        }
    }

    struct ThreadAffineNative {
        captured: Arc<AtomicBool>,
    }

    impl NativeBackend for ThreadAffineNative {
        fn discover(
            &self,
            interfaces: &[String],
            _maximum: usize,
            _deadline: Instant,
            _cancellation: &CancellationToken,
        ) -> Result<Vec<WireDevice>> {
            Ok(interfaces
                .iter()
                .map(|interface| WireDevice {
                    device_id: format!("device-{interface}"),
                    physical_id: "00:11:22:33:44:55".to_owned(),
                    vendor: "test".to_owned(),
                    model: "mock".to_owned(),
                    serial: "serial-1".to_owned(),
                    address: "192.0.2.10".to_owned(),
                    transport: WireTransport::GigeVision,
                    interface: Some(interface.clone()),
                })
                .collect())
        }

        fn connect(
            &self,
            _config: &GenicamBackendConfig,
            _deadline: Instant,
            _cancellation: &CancellationToken,
        ) -> Result<Box<dyn NativeSession>> {
            Ok(Box::new(ThreadAffineSession {
                owner: std::thread::current().id(),
                capabilities: capabilities(),
                captured: self.captured.clone(),
                closed: false,
            }))
        }
    }

    struct ThreadAffineSession {
        owner: std::thread::ThreadId,
        capabilities: CameraCapabilities,
        captured: Arc<AtomicBool>,
        closed: bool,
    }

    impl ThreadAffineSession {
        fn assert_owner(&self) {
            assert_eq!(self.owner, std::thread::current().id());
        }
    }

    impl NativeSession for ThreadAffineSession {
        fn capabilities(&self) -> &CameraCapabilities {
            &self.capabilities
        }

        fn status(
            &mut self,
            _deadline: Instant,
            _cancellation: &CancellationToken,
        ) -> Result<CameraStatus> {
            self.assert_owner();
            Ok(CameraStatus {
                online: !self.closed,
                connection_generation: 1,
                ptz: None,
                backend: json!({}),
            })
        }

        fn capture(
            &mut self,
            _request: CaptureRequest,
            _deadline: Instant,
        ) -> Result<CaptureFrame> {
            self.assert_owner();
            self.captured.store(true, Ordering::SeqCst);
            Ok(CaptureFrame {
                bytes: Bytes::from_static(&[1]),
                width: 1,
                height: 1,
                pixel_format: PixelFormat::Mono8,
                capture_mode: CaptureMode::SoftwareTrigger,
                source_timestamp: Some(Utc::now()),
                timestamp_quality: FrameTimestampQuality::AdapterReceive,
                backend_metadata: BTreeMap::new(),
            })
        }

        fn close(&mut self) -> Result<()> {
            self.assert_owner();
            self.closed = true;
            Ok(())
        }
    }

    fn backend_config() -> GenicamBackendConfig {
        GenicamBackendConfig {
            selector: GenicamSelector {
                serial: Some("serial-1".to_owned()),
                ..GenicamSelector::default()
            },
            transport: GenicamTransport::Usb3Vision,
            interface: None,
            packet_size: None,
            packet_delay_ns: None,
            buffer_count: Some(2),
            feature_overrides: BTreeMap::new(),
        }
    }

    fn profile() -> CaptureProfile {
        CaptureProfile {
            capture_mode: Some(CaptureMode::SoftwareTrigger),
            offline_policy: Some(OfflinePolicy::FailFast),
            queue_expiry_ms: None,
            timeout_ms: Some(1_000),
            maximum_frame_bytes: Some(1),
            pixel_format: Some(PixelFormat::Mono8),
            width: Some(1),
            height: Some(1),
            offset_x: Some(0),
            offset_y: Some(0),
            exposure_micros: None,
            gain: None,
            output: ProfileOutputConfig {
                encoding: OutputEncoding::Raw,
                jpeg_quality: 90,
            },
            capture_interlock: Some(CaptureInterlock::Reject),
        }
    }

    #[tokio::test]
    async fn every_native_session_call_stays_on_one_worker_thread_and_ptz_is_rejected() {
        let captured = Arc::new(AtomicBool::new(false));
        let factory = GenicamAravisBackendFactory::with_native(Arc::new(ThreadAffineNative {
            captured: captured.clone(),
        }));
        let mut session = factory
            .connect(ConnectRequest {
                instance_id: "camera-a".to_owned(),
                backend: BackendConfig::GenicamAravis(backend_config()),
                timeout: Duration::from_secs(1),
                cancellation: CancellationToken::new(),
            })
            .await
            .unwrap();
        assert!(session.status().await.unwrap().online);
        let frame = session
            .capture(CaptureRequest {
                capture_id: "cap-1".to_owned(),
                profile: profile(),
                maximum_frame_bytes: 1,
                timeout: Duration::from_secs(1),
                cancellation: CancellationToken::new(),
            })
            .await
            .unwrap();
        assert_eq!(frame.bytes.as_ref(), [1]);
        assert!(captured.load(Ordering::SeqCst));
        let error = session.ptz(PtzRequest::Status).await.unwrap_err();
        assert_eq!(error.code(), ErrorCode::UnsupportedCapability);
        session.close().await.unwrap();
        session.close().await.unwrap();
    }

    #[test]
    fn selectors_are_exact_and_mac_normalization_does_not_broaden() {
        let device = WireDevice {
            device_id: "id-1".to_owned(),
            physical_id: "00:11:22:AA:BB:CC".to_owned(),
            vendor: String::new(),
            model: String::new(),
            serial: "Serial".to_owned(),
            address: "192.0.2.4".to_owned(),
            transport: WireTransport::GigeVision,
            interface: Some("eth0".to_owned()),
        };
        let mut config = backend_config();
        config.selector = GenicamSelector {
            mac: Some("00-11-22-aa-bb-cc".to_owned()),
            ..GenicamSelector::default()
        };
        assert!(selector_matches(&config, &device));
        config.selector.mac = Some("00-11-22-aa-bb-cd".to_owned());
        assert!(!selector_matches(&config, &device));
        config.selector = GenicamSelector {
            serial: Some("serial".to_owned()),
            ..GenicamSelector::default()
        };
        assert!(!selector_matches(&config, &device));
    }

    #[test]
    fn helper_cli_is_closed_and_bounded() {
        let parsed = parse_helper_args([
            "--interface".to_owned(),
            "eth0".to_owned(),
            "--transport".to_owned(),
            "gige-vision".to_owned(),
            "--max-results".to_owned(),
            "100".to_owned(),
        ])
        .unwrap();
        assert_eq!(parsed, ("eth0".to_owned(), WireTransport::GigeVision, 100));
        assert!(parse_helper_args(["--interface".to_owned()]).is_err());
        assert!(
            parse_helper_args([
                "--interface=eth0".to_owned(),
                "--transport=gige-vision".to_owned(),
                "--max-results=1".to_owned(),
            ])
            .is_err()
        );
        assert!(
            parse_helper_args([
                "--interface".to_owned(),
                "eth0".to_owned(),
                "--transport".to_owned(),
                "gige-vision".to_owned(),
                "--max-results".to_owned(),
                "10001".to_owned(),
            ])
            .is_err()
        );
    }

    #[test]
    fn pixel_format_mapping_is_closed_and_deterministically_ordered() {
        let names = ["Mono8", "RGB8Packed", "BGR8", "JPEG"];
        let formats = names
            .into_iter()
            .map(|name| pixel_format_from_name(name).expect("supported native format"))
            .collect::<Vec<_>>();
        assert_eq!(
            formats
                .iter()
                .copied()
                .map(format_token)
                .collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
        assert_eq!(pixel_format_name(PixelFormat::Rgb8), "RGB8");
        assert!(pixel_format_from_name("Mono16").is_none());
    }

    #[test]
    fn native_metadata_and_interface_validation_reject_untrusted_text() {
        assert!(validate_native_field("vendor", "vendor").is_ok());
        assert!(validate_native_field("bad\nvalue", "vendor").is_err());
        assert!(validate_native_field(&"x".repeat(MAX_NATIVE_FIELD_BYTES + 1), "vendor").is_err());
        assert!(validate_interfaces(&["eth0".to_owned(), "eth0".to_owned()]).is_err());
        assert!(validate_interfaces(&["eth0\u{0000}".to_owned()]).is_err());
        assert!(validate_interfaces(&vec!["eth0".to_owned(); 65]).is_err());
        assert_eq!(bounded_thread_label("camera 01/left#A"), "camera01leftA");
    }

    #[test]
    fn helper_results_cannot_broaden_interface_or_replace_an_identity() {
        let device = WireDevice {
            device_id: "id-1".to_owned(),
            physical_id: "physical-1".to_owned(),
            vendor: "vendor".to_owned(),
            model: "model".to_owned(),
            serial: "serial".to_owned(),
            address: "192.0.2.20".to_owned(),
            transport: WireTransport::GigeVision,
            interface: Some("eth0".to_owned()),
        };
        assert!(
            validate_helper_devices(
                std::slice::from_ref(&device),
                "eth0",
                WireTransport::GigeVision
            )
            .is_ok()
        );

        let mut wrong_scope = device.clone();
        wrong_scope.interface = Some("eth1".to_owned());
        assert!(
            validate_helper_devices(&[wrong_scope], "eth0", WireTransport::GigeVision).is_err()
        );

        assert!(
            validate_helper_devices(&[device.clone(), device], "eth0", WireTransport::GigeVision)
                .is_err()
        );
    }

    #[test]
    fn timestamp_quality_never_calls_receive_time_camera_time() {
        let mut calibration = TimestampCalibration::default();
        let (timestamp, quality) = calibration.resolve(100, 1_000_000_000_000);
        assert!(timestamp.is_some());
        assert_eq!(quality, FrameTimestampQuality::Camera);
        let (_, quality) = calibration.resolve(2_000, 2_000);
        assert_eq!(quality, FrameTimestampQuality::AdapterReceive);
        let (timestamp, quality) = calibration.resolve(0, 0);
        assert!(timestamp.is_none());
        assert_eq!(quality, FrameTimestampQuality::Unknown);
    }
}
