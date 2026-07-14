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

/// Native connects that may be in flight at once.
///
/// `connect` spawns an OS thread and, inside it, calls straight into Aravis -- which resolves the
/// camera behind `with_scoped_aravis`, a PROCESS-WIDE lock that is not cancellable and not deadline
/// aware. A thread that gets there while the lock is held blocks until it is free, and nothing about
/// the caller giving up reaches it.
///
/// The async side, meanwhile, DOES give up: it races the connect against the deadline and returns a
/// timeout. The thread it left behind does not stop existing. So a camera that has gone dark -- the
/// exact condition that produces retries -- had every retry spawn another thread, pile it onto the
/// same lock, and abandon it. Threads accumulated for as long as the camera stayed dark, which is to
/// say without bound, and an OS thread is a megabyte of stack that no amount of care elsewhere in the
/// component gets back.
///
/// Discovery had admission control from the start (`discovery_permit`). Connect never did. It does
/// now, and the permit is held by the THREAD rather than the future, because the thread is what
/// outlives the timeout. A connect that cannot get a permit before its deadline fails in async-land,
/// having spawned nothing.
const MAX_CONCURRENT_CONNECTS: usize = 4;
const TIMESTAMP_REFRESH_NS: u64 = 60_000_000_000;

static ARAVIS_TOKEN: Mutex<Option<aravis::Aravis>> = Mutex::new(None);

/// Aravis-backed factory. Discovery concurrency is deliberately one per component process; each
/// scan may itself launch a sequence of isolated per-interface helpers.
pub struct GenicamAravisBackendFactory {
    native: Arc<dyn NativeBackend>,
    discovery_permit: Arc<Semaphore>,
    connect_permit: Arc<Semaphore>,
}

impl Default for GenicamAravisBackendFactory {
    fn default() -> Self {
        Self {
            native: Arc::new(ProductionNativeBackend),
            discovery_permit: Arc::new(Semaphore::new(1)),
            connect_permit: Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTS)),
        }
    }
}

impl GenicamAravisBackendFactory {
    #[cfg(test)]
    fn with_native(native: Arc<dyn NativeBackend>) -> Self {
        Self {
            native,
            discovery_permit: Arc::new(Semaphore::new(1)),
            connect_permit: Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTS)),
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

        // Admission FIRST, and the thread only if it is granted. A connect that cannot get a permit
        // before its deadline gives up here, in async-land, having spawned nothing -- which is the
        // whole point: the thread is the resource that leaks, so the bound has to sit in front of it.
        let permit = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => return cancelled("before GenICam connect"),
            _ = tokio::time::sleep_until(deadline.into()) => {
                return timeout("waiting for GenICam connect admission");
            }
            permit = self.connect_permit.clone().acquire_owned() => {
                permit.map_err(|_| backend_error("connect admission is closed"))?
            }
        };

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
                let connected = native.connect(&config, deadline, &cancellation);

                // The permit covers the CONNECT, not the session. It is released the moment the native
                // call returns -- including the slow, lock-bound failure this exists to contain -- so a
                // camera that is merely connected costs no admission, and the number of cameras a
                // component can hold open is not quietly capped at the number of connects it may
                // attempt at once. Those are different resources and they get different bounds.
                let mut session = match connected {
                    Ok(session) => session,
                    Err(error) => {
                        drop(permit);
                        let _ = initial_sender.send(Err(error));
                        return;
                    }
                };
                drop(permit);
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

    async fn ptz_bounded(
        &mut self,
        _request: PtzRequest,
        _deadline: tokio::time::Instant,
        _cancellation: &CancellationToken,
    ) -> Result<PtzResult> {
        // Refusing takes no time, so there is no deadline to honour and nothing to cancel.
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
        let opened =
            identity_from_camera(&camera, transport, interface, identity.address.as_str())?;
        let opened = verify_open_identity(&identity, opened)?;
        if !selector_matches_after_open(config, &identity, &opened) {
            return Err(backend_error(
                "opened GenICam identity changed during binding",
            ));
        }
        Ok((camera, opened))
    })
}

/// Validates camera-reported metadata after opening a discovered Aravis device.
///
/// The normal case requires the discovery identifier and the opened GenICam
/// `DeviceID` register to agree. Aravis' pinned fake GigE fixture is the only
/// supported alias: it discovers as `Aravis-Fake-GV01` while exposing `GV01`
/// after opening. Keeping that exception explicit prevents an arbitrary
/// discovery/open race from being mistaken for a stable binding.
fn verify_open_identity(discovered: &WireDevice, opened: WireDevice) -> Result<WireDevice> {
    if discovered.transport != opened.transport
        || discovered.interface != opened.interface
        || (discovered.vendor != opened.vendor && !discovered.vendor.is_empty())
        || (discovered.model != opened.model && !discovered.model.is_empty())
        || (discovered.serial != opened.serial && !discovered.serial.is_empty())
        || (discovered.device_id != opened.device_id
            && !is_approved_aravis_fake_lookup_alias(discovered, &opened))
    {
        return Err(backend_error(
            "opened GenICam identity changed during binding",
        ));
    }
    Ok(opened)
}

fn is_approved_aravis_fake_lookup_alias(discovered: &WireDevice, opened: &WireDevice) -> bool {
    discovered.transport == WireTransport::GigeVision
        && opened.transport == WireTransport::GigeVision
        && discovered.device_id == "Aravis-Fake-GV01"
        && opened.device_id == "GV01"
        && discovered.vendor == "Aravis"
        && opened.vendor == "Aravis"
        && discovered.model == "Fake"
        && opened.model == "Fake"
        && discovered.serial == "GV01"
        && opened.serial == "GV01"
}

/// Matches a configured selector after the device has been opened and its
/// GenICam registers have been re-read. A fake-camera lookup alias is accepted
/// only after [`is_approved_aravis_fake_lookup_alias`] has authenticated the
/// complete pinned fixture shape.
fn selector_matches_after_open(
    config: &GenicamBackendConfig,
    discovered: &WireDevice,
    opened: &WireDevice,
) -> bool {
    if let Some(expected) = config.selector.device_id.as_deref() {
        return expected == opened.device_id
            || (expected == discovered.device_id
                && is_approved_aravis_fake_lookup_alias(discovered, opened));
    }
    if config.selector.mac.is_some() {
        // Aravis does not expose a post-open physical/MAC register through the
        // current binding. The discovery record remains the authoritative
        // transport-layer source for the already-selected MAC.
        return selector_matches(config, discovered);
    }
    selector_matches(config, opened)
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

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    /// A camera that has gone dark cannot make the component spawn threads without bound.
    ///
    /// `connect` spawns an OS thread that calls into Aravis behind a PROCESS-WIDE, non-cancellable
    /// lock, and the async side abandons that thread when the deadline passes. Retrying a dark camera
    /// -- the exact condition that produces retries -- therefore piled up one abandoned, parked thread
    /// per attempt, for as long as the camera stayed dark.
    ///
    /// Here twelve connects race a native backend that blocks, as the real one does when the lock is
    /// held. At most `MAX_CONCURRENT_CONNECTS` may be inside the native call at once, and the ones that
    /// cannot get in must fail WITHOUT having started a thread.
    #[tokio::test]
    async fn a_dark_camera_cannot_spawn_threads_without_bound() {
        struct BlockingNative {
            in_flight: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
            entered: Arc<AtomicUsize>,
        }

        impl NativeBackend for BlockingNative {
            fn discover(
                &self,
                _interfaces: &[String],
                _maximum: usize,
                _deadline: Instant,
                _cancellation: &CancellationToken,
            ) -> Result<Vec<WireDevice>> {
                Ok(Vec::new())
            }

            fn connect(
                &self,
                _config: &GenicamBackendConfig,
                _deadline: Instant,
                _cancellation: &CancellationToken,
            ) -> Result<Box<dyn NativeSession>> {
                self.entered.fetch_add(1, Ordering::SeqCst);
                let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(current, Ordering::SeqCst);
                // Aravis, holding the process-wide scope lock. Deadline and cancellation reach nothing
                // here -- that is the whole reason the bound has to sit in front of the thread.
                std::thread::sleep(Duration::from_millis(400));
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Err(backend_error("the camera is dark"))
            }
        }

        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(AtomicUsize::new(0));
        let factory = Arc::new(GenicamAravisBackendFactory::with_native(Arc::new(
            BlockingNative {
                in_flight: in_flight.clone(),
                peak: peak.clone(),
                entered: entered.clone(),
            },
        )));

        let attempts = (0..12).map(|index| {
            let factory = factory.clone();
            tokio::spawn(async move {
                factory
                    .connect(ConnectRequest {
                        instance_id: format!("camera-{index}"),
                        backend: BackendConfig::GenicamAravis(backend_config()),
                        timeout: Duration::from_millis(500),
                        cancellation: CancellationToken::new(),
                    })
                    .await
            })
        });
        let outcomes = futures::future::join_all(attempts).await;

        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.as_ref().unwrap().is_err()),
            "a dark camera connects to nobody"
        );
        let peak = peak.load(Ordering::SeqCst);
        assert!(
            peak <= MAX_CONCURRENT_CONNECTS,
            "{peak} native connects were in flight at once against a bound of              {MAX_CONCURRENT_CONNECTS}; every one of them is an abandoned OS thread parked on a              process-wide lock, and a camera that stays dark keeps producing them"
        );
        let entered = entered.load(Ordering::SeqCst);
        assert!(
            entered < 12,
            "all twelve attempts reached the native call, so admission admitted everything; the              attempts that cannot get a permit before their deadline must fail without starting a              thread at all"
        );
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

    fn fake_gige_profile() -> CaptureProfile {
        CaptureProfile {
            capture_mode: Some(CaptureMode::SoftwareTrigger),
            offline_policy: Some(OfflinePolicy::FailFast),
            queue_expiry_ms: None,
            timeout_ms: Some(5_000),
            maximum_frame_bytes: Some(76_800),
            pixel_format: Some(PixelFormat::Mono8),
            width: Some(320),
            height: Some(240),
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

    fn fake_gige_request(capture_id: &str) -> CaptureRequest {
        CaptureRequest {
            capture_id: capture_id.to_owned(),
            profile: fake_gige_profile(),
            maximum_frame_bytes: 76_800,
            timeout: Duration::from_secs(5),
            cancellation: CancellationToken::new(),
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

    #[tokio::test]
    #[ignore = "requires the pinned Aravis fake GigE camera on a Linux host-network interface"]
    async fn pinned_aravis_fake_discovers_and_captures_two_complete_mono8_frames() {
        let interface = std::env::var("CAMERA_ADAPTER_ARAVIS_INTERFACE")
            .expect("set CAMERA_ADAPTER_ARAVIS_INTERFACE to the fake camera interface");
        let factory = GenicamAravisBackendFactory::default();
        let candidates = factory
            .discover(DiscoveryRequest {
                eligible_interfaces: vec![interface.clone()],
                max_results: 1,
                timeout: Duration::from_secs(10),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("the pinned discovery helper must find the fake camera");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].backend, BackendKind::GenicamAravis);
        assert_eq!(
            candidates[0].selector,
            json!({"deviceId": "Aravis-Fake-GV01"})
        );
        assert_eq!(
            candidates[0].capabilities["interface"],
            Value::String(interface.clone())
        );

        let mut session = factory
            .connect(ConnectRequest {
                instance_id: "aravis-fake-gv".to_owned(),
                backend: BackendConfig::GenicamAravis(GenicamBackendConfig {
                    selector: GenicamSelector {
                        device_id: Some("Aravis-Fake-GV01".to_owned()),
                        ..GenicamSelector::default()
                    },
                    transport: GenicamTransport::GigeVision,
                    interface: Some(interface),
                    packet_size: None,
                    packet_delay_ns: None,
                    buffer_count: Some(2),
                    feature_overrides: BTreeMap::new(),
                }),
                timeout: Duration::from_secs(10),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("the fake camera must open through the production worker");
        assert!(session.capabilities().software_trigger);
        assert!(
            session
                .capabilities()
                .pixel_formats
                .contains(&PixelFormat::Mono8)
        );

        for capture_id in ["aravis-fake-first", "aravis-fake-second"] {
            let frame = session
                .capture(fake_gige_request(capture_id))
                .await
                .expect("the fake camera must return a complete software-triggered frame");
            assert_eq!((frame.width, frame.height), (320, 240));
            assert_eq!(frame.pixel_format, PixelFormat::Mono8);
            assert_eq!(frame.capture_mode, CaptureMode::SoftwareTrigger);
            assert_eq!(frame.bytes.len(), 76_800);
            assert_eq!(
                frame.backend_metadata.get("transport"),
                Some(&json!(WireTransport::GigeVision))
            );
            assert!(frame.backend_metadata.contains_key("frameId"));
        }

        let status = session
            .status()
            .await
            .expect("fake session status must succeed");
        assert_eq!(status.backend["bufferCount"], json!(2));
        assert_eq!(status.backend["acquisitionActive"], json!(true));
        session
            .close()
            .await
            .expect("fake session must close cleanly");
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
    fn opened_identity_requires_exact_device_id_or_the_pinned_fake_alias() {
        let exact = WireDevice {
            device_id: "device-42".to_owned(),
            physical_id: "00:00:00:00:00:00".to_owned(),
            vendor: "Vendor".to_owned(),
            model: "Model".to_owned(),
            serial: "SN-42".to_owned(),
            address: "192.0.2.10".to_owned(),
            transport: WireTransport::GigeVision,
            interface: Some("eth0".to_owned()),
        };
        let mut config = backend_config();
        config.selector = GenicamSelector {
            device_id: Some("device-42".to_owned()),
            ..GenicamSelector::default()
        };
        let opened = verify_open_identity(&exact, exact.clone()).expect("exact DeviceID");
        assert_eq!(
            opened.device_id, "device-42",
            "the post-open identity remains the stable selector source"
        );
        assert!(selector_matches_after_open(&config, &exact, &opened));

        let fake_lookup = WireDevice {
            device_id: "Aravis-Fake-GV01".to_owned(),
            physical_id: "00:00:00:00:00:00".to_owned(),
            vendor: "Aravis".to_owned(),
            model: "Fake".to_owned(),
            serial: "GV01".to_owned(),
            address: "192.0.2.10".to_owned(),
            transport: WireTransport::GigeVision,
            interface: Some("eth0".to_owned()),
        };
        let fake_opened = WireDevice {
            device_id: "GV01".to_owned(),
            physical_id: String::new(),
            ..fake_lookup.clone()
        };
        config.selector.device_id = Some("Aravis-Fake-GV01".to_owned());
        let opened = verify_open_identity(&fake_lookup, fake_opened.clone())
            .expect("the pinned fake camera uses a documented lookup alias");
        assert_eq!(opened.device_id, "GV01");
        assert!(selector_matches_after_open(&config, &fake_lookup, &opened));

        let unrelated_fake_alias = WireDevice {
            device_id: "Aravis-Fake-GV02".to_owned(),
            ..fake_lookup.clone()
        };
        assert!(verify_open_identity(&unrelated_fake_alias, fake_opened.clone()).is_err());
        let metadata_changed = WireDevice {
            model: "Replacement".to_owned(),
            ..fake_opened
        };
        assert!(verify_open_identity(&fake_lookup, metadata_changed).is_err());
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

    #[test]
    fn helper_records_are_bounded_scoped_and_exposed_as_safe_candidates() {
        let gige = WireDevice {
            device_id: "device-a".to_owned(),
            physical_id: "00:11:22:33:44:55".to_owned(),
            vendor: "vendor".to_owned(),
            model: "model".to_owned(),
            serial: "serial".to_owned(),
            address: "192.0.2.20".to_owned(),
            transport: WireTransport::GigeVision,
            interface: Some("eth0".to_owned()),
        };
        let candidate = gige.clone().into_candidate();
        assert_eq!(candidate.backend, BackendKind::GenicamAravis);
        assert_eq!(candidate.selector, json!({"deviceId": "device-a"}));
        assert_eq!(candidate.vendor.as_deref(), Some("vendor"));
        assert_eq!(candidate.capabilities["interface"], json!("eth0"));
        assert!(
            validate_helper_devices(
                std::slice::from_ref(&gige),
                "eth0",
                WireTransport::GigeVision
            )
            .is_ok()
        );

        let mut missing_identity = gige.clone();
        missing_identity.device_id.clear();
        assert!(
            validate_helper_devices(&[missing_identity], "eth0", WireTransport::GigeVision)
                .is_err()
        );

        let usb = WireDevice {
            transport: WireTransport::Usb3Vision,
            interface: None,
            ..gige.clone()
        };
        assert!(
            validate_helper_devices(
                std::slice::from_ref(&usb),
                "eth0",
                WireTransport::Usb3Vision
            )
            .is_ok()
        );
        let mut usb_with_interface = usb;
        usb_with_interface.interface = Some("eth0".to_owned());
        assert!(
            validate_helper_devices(&[usb_with_interface], "eth0", WireTransport::Usb3Vision)
                .is_err()
        );
    }

    #[test]
    fn helper_parser_reader_and_profile_key_cover_closed_local_boundaries() {
        let parsed = parse_helper_args([
            "--interface".to_owned(),
            "usb0".to_owned(),
            "--transport".to_owned(),
            "usb3-vision".to_owned(),
            "--max-results".to_owned(),
            "1".to_owned(),
        ])
        .expect("USB helper arguments");
        assert_eq!(parsed, ("usb0".to_owned(), WireTransport::Usb3Vision, 1));
        for arguments in [
            vec![
                "--interface".to_owned(),
                "bad\ninterface".to_owned(),
                "--transport".to_owned(),
                "gige-vision".to_owned(),
                "--max-results".to_owned(),
                "1".to_owned(),
            ],
            vec![
                "--interface".to_owned(),
                "eth0".to_owned(),
                "--transport".to_owned(),
                "other".to_owned(),
                "--max-results".to_owned(),
                "1".to_owned(),
            ],
            vec![
                "--interface".to_owned(),
                "eth0".to_owned(),
                "--transport".to_owned(),
                "gige-vision".to_owned(),
                "--max-results".to_owned(),
                "0".to_owned(),
            ],
        ] {
            assert!(parse_helper_args(arguments).is_err());
        }

        assert_eq!(
            read_bounded(std::io::Cursor::new(b"abc"), 3).unwrap(),
            b"abc"
        );
        assert!(read_bounded(std::io::Cursor::new(b"abcd"), 3).is_err());

        let mut keyed = profile();
        let original = ProfileKey::from(&keyed);
        keyed.output.jpeg_quality = 1;
        assert_eq!(ProfileKey::from(&keyed), original);
        keyed.width = Some(2);
        assert_ne!(ProfileKey::from(&keyed), original);
    }

    #[test]
    fn timestamp_and_numeric_validation_remain_bounded_and_fail_closed() {
        let mut calibration = TimestampCalibration::default();
        let (first, quality) = calibration.resolve(100, 1_000);
        assert_eq!(quality, FrameTimestampQuality::Camera);
        assert_eq!(first, datetime_from_ns(1_000));
        let (cached, quality) = calibration.resolve(200, 1_100);
        assert_eq!(quality, FrameTimestampQuality::Camera);
        assert_eq!(cached, datetime_from_ns(1_100));
        let refreshed_system = 1_000 + TIMESTAMP_REFRESH_NS;
        let (refreshed, quality) = calibration.resolve(300, refreshed_system);
        assert_eq!(quality, FrameTimestampQuality::Camera);
        assert_eq!(refreshed, datetime_from_ns(i128::from(refreshed_system)));
        assert!(datetime_from_ns(i128::MAX).is_none());

        assert_eq!(positive_dimension(1, "width").unwrap(), 1);
        assert!(positive_dimension(0, "width").is_err());
        assert!(positive_dimension(-1, "width").is_err());
        assert_eq!(requested_i32(None, 7, "width").unwrap(), 7);
        assert_eq!(requested_i32(Some(8), 7, "width").unwrap(), 8);
        assert_eq!(
            requested_i32(Some(u32::MAX), 7, "width")
                .expect_err("u32 outside native range")
                .code(),
            ErrorCode::InvalidRequest
        );
        assert!(validate_axis(Ok((2, 10)), Ok(2), 6, "width").is_ok());
        assert!(validate_axis(Ok((2, 10)), Ok(2), 7, "width").is_err());
        assert!(validate_axis(Ok((2, 10)), Ok(0), 9, "width").is_ok());
        assert!(verify_float_readback(10.0, 10.0 + 1e-10, "gain").is_ok());
        assert!(verify_float_readback(10.0, 10.1, "gain").is_err());
        assert!(verify_float_readback(10.0, f64::NAN, "gain").is_err());
    }

    #[test]
    fn selector_interface_and_metadata_helpers_reject_unsafe_edges() {
        let device = WireDevice {
            device_id: "device-id".to_owned(),
            physical_id: "00:11:22:33:44:55".to_owned(),
            vendor: String::new(),
            model: String::new(),
            serial: "serial".to_owned(),
            address: "192.0.2.20".to_owned(),
            transport: WireTransport::GigeVision,
            interface: Some("eth0".to_owned()),
        };
        let mut config = backend_config();
        config.selector = GenicamSelector {
            device_id: Some("device-id".to_owned()),
            ..GenicamSelector::default()
        };
        assert!(selector_matches(&config, &device));
        config.selector = GenicamSelector {
            ip: Some("192.0.2.20".to_owned()),
            ..GenicamSelector::default()
        };
        assert!(selector_matches(&config, &device));
        assert_eq!(
            normalize_mac("001122334455").as_deref(),
            Some("001122334455")
        );
        assert!(normalize_mac("00112233445").is_none());
        assert!(normalize_mac("00:11:22:33:44:GG").is_none());
        assert!(required_interface(&backend_config()).is_err());
        config.interface = Some("eth0".to_owned());
        assert_eq!(required_interface(&config).unwrap(), "eth0");

        let invalid_utf8 = std::ffi::CString::new(vec![0xff]).expect("nul-free bytes");
        assert!(native_string(&invalid_utf8, "vendor").is_err());
        assert_eq!(nonempty(String::new()), None);
        assert_eq!(nonempty("value".to_owned()).as_deref(), Some("value"));
        assert!(validate_interfaces(&["eth0".to_owned(), "usb0".to_owned()]).is_ok());

        let cancellation = CancellationToken::new();
        assert!(
            check_deadline(
                Instant::now() + Duration::from_secs(1),
                &cancellation,
                "test"
            )
            .is_ok()
        );
        cancellation.cancel();
        assert_eq!(
            check_deadline(
                Instant::now() + Duration::from_secs(1),
                &cancellation,
                "test"
            )
            .expect_err("cancelled caller")
            .code(),
            ErrorCode::CaptureCancelled
        );
        assert_eq!(
            check_deadline(
                Instant::now() - Duration::from_millis(1),
                &CancellationToken::new(),
                "test"
            )
            .expect_err("elapsed deadline")
            .code(),
            ErrorCode::CaptureTimeout
        );
    }

    // ---------------------------------------------------------------------------------------------
    // The frame path.
    //
    // An adapter whose whole purpose is to deliver the picture a camera took must deliver THAT
    // picture. Everything below exists to prove that the bytes the sensor exposed are the bytes the
    // caller is handed -- across the native conversion, across the worker thread, and across the
    // bound that is allowed to refuse a frame but never to shorten one.
    // ---------------------------------------------------------------------------------------------

    /// Register addresses of the Aravis fake camera, from `arvfakecamera.h`.
    ///
    /// Writing them is how a test tells the fake camera which picture to take.
    const FAKE_REGISTER_WIDTH: u32 = 0x100;
    const FAKE_REGISTER_HEIGHT: u32 = 0x104;
    const FAKE_REGISTER_GAIN_RAW: u32 = 0x110;
    const FAKE_REGISTER_EXPOSURE_TIME_US: u32 = 0x120;
    const FAKE_REGISTER_PIXEL_FORMAT: u32 = 0x128;

    /// The exposure at which the fake camera's fill pattern has a scale of exactly 1.0.
    const FAKE_EXPOSURE_TIME_US: u32 = 10_000;

    /// A real, camera-filled `ArvBuffer` -- the exact thing [`frame_from_buffer`] is handed on a live
    /// link, produced without a camera and without a network.
    ///
    /// `arv_fake_camera_fill_buffer` is the same call Aravis' own stream makes when a device delivers
    /// a frame: it stamps the buffer's status, payload type, geometry, pixel format and padding, and
    /// then writes the pixels through the camera's fill-pattern callback. The production conversion
    /// therefore runs against a genuine acquisition buffer rather than a mock of the type it consumes.
    fn camera_filled_buffer(
        width: u32,
        height: u32,
        pixel_format: aravis::PixelFormat,
        allocated_bytes: usize,
    ) -> aravis::Buffer {
        // The fake camera's serial lives in a GVBS register and must stay under 16 bytes.
        let camera = aravis::FakeCamera::new("ec-frame-01");
        for (address, value) in [
            (FAKE_REGISTER_WIDTH, width),
            (FAKE_REGISTER_HEIGHT, height),
            (FAKE_REGISTER_PIXEL_FORMAT, pixel_format.raw()),
            // Gain 0 at the default exposure makes the ramp's scale exactly 1.0, so the picture the
            // camera takes is precisely the one `expected_diagonal_ramp` predicts.
            (FAKE_REGISTER_GAIN_RAW, 0),
            (FAKE_REGISTER_EXPOSURE_TIME_US, FAKE_EXPOSURE_TIME_US),
        ] {
            assert!(
                camera.write_register(address, value),
                "the fake camera must accept register 0x{address:x}"
            );
        }
        let buffer = aravis::Buffer::new_allocate(allocated_bytes);
        camera.fill_buffer(&buffer);
        buffer
    }

    /// The exact picture the fake camera is documented to take, computed independently of it.
    ///
    /// `arv_fake_camera_diagonal_ramp` writes one byte per pixel, row-major, with the value
    /// `(x + frameId + y) % 255` multiplied by `1 + gain + log10(exposure / 10000)` -- which
    /// [`camera_filled_buffer`] pins to exactly 1.0. This is a MODEL of the image, derived from the
    /// camera's contract; it is not read back out of the buffer the code under test read. Comparing a
    /// delivered frame against the buffer it was built from would prove nothing, because a corruption
    /// in the conversion would corrupt both sides of that comparison equally.
    fn expected_diagonal_ramp(width: u32, height: u32, frame_id: u64) -> Vec<u8> {
        (0..u64::from(height))
            .flat_map(|y| {
                (0..u64::from(width))
                    .map(move |x| u8::try_from((x + y + frame_id) % 255).unwrap_or(u8::MAX))
            })
            .collect()
    }

    /// Byte-for-byte image equality, whose failure names the first pixel that was corrupted.
    #[track_caller]
    fn assert_pixels_identical(delivered: &[u8], exposed: &[u8], what: &str) {
        assert_eq!(
            delivered.len(),
            exposed.len(),
            "{what}: the camera exposed {} bytes and the adapter delivered {} -- an image that \
             changes length between the sensor and the caller has been truncated, padded, or \
             re-packed, and the picture that arrives is not the picture that was taken",
            exposed.len(),
            delivered.len()
        );
        if let Some((index, (delivered_byte, exposed_byte))) = delivered
            .iter()
            .zip(exposed)
            .enumerate()
            .find(|(_, (delivered, exposed))| delivered != exposed)
        {
            panic!(
                "{what}: byte {index} of the delivered image is 0x{delivered_byte:02x} where the \
                 camera exposed 0x{exposed_byte:02x} -- the image that was delivered is not the \
                 image the camera took"
            );
        }
    }

    /// The image the adapter delivers IS the image the GenICam camera took -- byte for byte.
    ///
    /// This is the backend's entire purpose, and nothing pinned it. `frame_from_buffer` lifts the
    /// pixels out of a native acquisition buffer into a `CaptureFrame`, and a truncation, a re-pack,
    /// or a stride mistake there yields a frame that is internally perfectly consistent -- correct
    /// length, correct geometry, a digest that verifies against itself downstream -- while showing a
    /// DIFFERENT picture than the sensor exposed. Nothing inside the frame can reveal that; the only
    /// defence is an independent model of what the camera put on the wire, which is what the fake
    /// camera's documented diagonal ramp provides.
    #[test]
    fn the_image_that_is_delivered_is_the_image_the_genicam_camera_took() {
        const WIDTH: u32 = 512;
        const HEIGHT: u32 = 512;
        let payload = usize::try_from(WIDTH * HEIGHT).expect("a 512x512 Mono8 payload fits a usize");

        let buffer = camera_filled_buffer(WIDTH, HEIGHT, aravis::PixelFormat::MONO_8, payload);
        assert_eq!(
            buffer.status(),
            aravis::BufferStatus::Success,
            "the fake camera must have completed the frame, or this test proves nothing"
        );
        let exposed = expected_diagonal_ramp(WIDTH, HEIGHT, buffer.frame_id());

        let frame = frame_from_buffer(
            &buffer,
            u64::try_from(payload).expect("the payload fits a u64"),
            WireTransport::GigeVision,
            &mut TimestampCalibration::default(),
        )
        .expect("a complete Mono8 buffer that the camera filled must become a frame");

        assert_pixels_identical(frame.bytes.as_ref(), &exposed, "a 512x512 Mono8 frame");
        assert_eq!(
            (frame.width, frame.height),
            (WIDTH, HEIGHT),
            "an image delivered with the wrong geometry is a corrupted image, however intact its \
             bytes are: every consumer reads those bytes through the width it was told"
        );
        assert_eq!(
            frame.pixel_format,
            PixelFormat::Mono8,
            "and the format is how those bytes are interpreted at all"
        );
        assert_eq!(
            u64::try_from(frame.bytes.len()).expect("the frame length fits a u64"),
            PixelFormat::Mono8
                .uncompressed_len(frame.width, frame.height)
                .expect("Mono8 is uncompressed"),
            "the delivered image must hold exactly one byte per pixel of the geometry it declares"
        );
        assert_eq!(frame.capture_mode, CaptureMode::SoftwareTrigger);
        assert_eq!(
            frame.backend_metadata.get("frameId"),
            Some(&json!(buffer.frame_id())),
            "the frame must carry the camera's own identifier for the picture it is"
        );
        assert_eq!(
            frame.backend_metadata.get("transport"),
            Some(&json!(WireTransport::GigeVision))
        );
    }

    /// A delivered row is exactly `width` bytes: no stride padding is added, and none is dropped.
    ///
    /// Cameras and imaging libraries routinely align rows to 4- or 8-byte boundaries. A conversion
    /// that introduced such a stride -- or that stripped one a camera had sent -- would shift every
    /// row after the first, producing a sheared picture whose byte count still looks plausible. The
    /// geometry here is deliberately hostile to that mistake: 61 is odd and prime, so a padded row
    /// cannot coincide with an unpadded one at any alignment.
    #[test]
    fn a_delivered_row_is_exactly_its_width_with_no_stride_padding() {
        const WIDTH: u32 = 61;
        const HEIGHT: u32 = 37;
        let payload = usize::try_from(WIDTH * HEIGHT).expect("a 61x37 Mono8 payload fits a usize");

        let buffer = camera_filled_buffer(WIDTH, HEIGHT, aravis::PixelFormat::MONO_8, payload);
        assert_eq!(buffer.status(), aravis::BufferStatus::Success);
        assert_eq!(
            buffer.image_padding(),
            (0, 0),
            "the camera sent an unpadded image, which is the case this test is about"
        );
        let exposed = expected_diagonal_ramp(WIDTH, HEIGHT, buffer.frame_id());

        let frame = frame_from_buffer(
            &buffer,
            u64::try_from(payload).expect("the payload fits a u64"),
            WireTransport::GigeVision,
            &mut TimestampCalibration::default(),
        )
        .expect("an odd-width Mono8 frame is still a frame");

        assert_pixels_identical(frame.bytes.as_ref(), &exposed, "a 61x37 Mono8 frame");
        assert_eq!(
            frame.bytes.len(),
            payload,
            "a 61-byte row must stay a 61-byte row"
        );
        let four_byte_aligned_rows =
            usize::try_from(HEIGHT).expect("the height fits a usize") * 64_usize;
        assert_ne!(
            frame.bytes.len(),
            four_byte_aligned_rows,
            "a conversion that padded each row out to a 4-byte alignment would have delivered \
             {four_byte_aligned_rows} bytes of sheared image, and every assertion about the frame's \
             own fields would still have passed"
        );
    }

    /// A pixel format the adapter cannot read is REFUSED, never relabelled as one it can.
    ///
    /// The trap is Bayer: a Bayer BG8 frame carries exactly one byte per pixel -- the same byte count
    /// as the Mono8 frame of the same geometry. Every length check in the path passes for it. Only
    /// the pixel-format mapping stands between a colour-filter mosaic and its delivery as a
    /// greyscale photograph, at full confidence, with a digest that verifies. Mono16 is the same
    /// hazard with the bytes doubled.
    #[test]
    fn a_pixel_format_the_adapter_cannot_read_is_refused_rather_than_relabelled() {
        const WIDTH: u32 = 64;
        const HEIGHT: u32 = 48;
        let mono8_bytes = usize::try_from(WIDTH * HEIGHT).expect("a 64x48 payload fits a usize");

        let bayer =
            camera_filled_buffer(WIDTH, HEIGHT, aravis::PixelFormat::BAYER_BG_8, mono8_bytes);
        assert_eq!(bayer.status(), aravis::BufferStatus::Success);
        assert_eq!(
            bayer.image_data().len(),
            mono8_bytes,
            "the hazard this test exists for: a Bayer frame is byte-for-byte the same SIZE as the \
             Mono8 frame it must never be mistaken for"
        );
        assert_eq!(
            frame_from_buffer(
                &bayer,
                u64::try_from(mono8_bytes).expect("the payload fits a u64"),
                WireTransport::GigeVision,
                &mut TimestampCalibration::default(),
            )
            .expect_err("a Bayer mosaic must never be delivered as a greyscale image")
            .code(),
            ErrorCode::UnsupportedPixelFormat,
            "a format the adapter cannot read is refused; it is not quietly reinterpreted as one it \
             can"
        );

        let mono16_bytes = 2 * mono8_bytes;
        let mono16 =
            camera_filled_buffer(WIDTH, HEIGHT, aravis::PixelFormat::MONO_16, mono16_bytes);
        assert_eq!(mono16.status(), aravis::BufferStatus::Success);
        assert_eq!(
            frame_from_buffer(
                &mono16,
                u64::try_from(mono16_bytes).expect("the payload fits a u64"),
                WireTransport::GigeVision,
                &mut TimestampCalibration::default(),
            )
            .expect_err("a 16-bit image must never be delivered as an 8-bit one")
            .code(),
            ErrorCode::UnsupportedPixelFormat,
            "delivering Mono16 bytes under a Mono8 label would show a consumer half a picture of \
             noise"
        );

        // The formats the adapter DOES read map to themselves and back, so a frame is labelled with
        // the format the camera actually sent -- an RGB image delivered as BGR is a colour-swapped
        // picture that no length check can catch.
        for (native, mapped) in [
            (aravis::PixelFormat::MONO_8, PixelFormat::Mono8),
            (aravis::PixelFormat::RGB_8_PACKED, PixelFormat::Rgb8),
            (aravis::PixelFormat::BGR_8_PACKED, PixelFormat::Bgr8),
        ] {
            assert_eq!(
                from_aravis_pixel_format(native).expect("a supported native format"),
                mapped,
                "the native format the camera reported must map to the format the frame declares"
            );
            assert_eq!(
                to_aravis_pixel_format(mapped),
                Some(native),
                "and the format the adapter asks the camera for must be the same one it reads back"
            );
        }
    }

    /// A frame larger than its reservation is REFUSED -- it is never truncated to fit.
    ///
    /// The reservation exists to bound memory, and the tempting way to honour it is to take what
    /// fits. That silently delivers the top of a picture as though it were the picture. The refusal
    /// is the only correct answer, and the same buffer at exactly its reservation must still arrive
    /// whole -- which is what proves the refusal was the bound doing its job rather than the frame
    /// being damaged.
    #[test]
    fn a_frame_larger_than_its_reservation_is_refused_and_never_truncated() {
        const WIDTH: u32 = 64;
        const HEIGHT: u32 = 48;
        let payload = u64::from(WIDTH) * u64::from(HEIGHT);

        let buffer = camera_filled_buffer(
            WIDTH,
            HEIGHT,
            aravis::PixelFormat::MONO_8,
            usize::try_from(payload).expect("the payload fits a usize"),
        );
        assert_eq!(buffer.status(), aravis::BufferStatus::Success);
        let exposed = expected_diagonal_ramp(WIDTH, HEIGHT, buffer.frame_id());

        assert_eq!(
            frame_from_buffer(
                &buffer,
                payload - 1,
                WireTransport::GigeVision,
                &mut TimestampCalibration::default(),
            )
            .expect_err("a frame one byte over its reservation must be refused")
            .code(),
            ErrorCode::ResourceLimit,
            "an over-large frame is refused whole; a bound that truncated instead would hand the \
             caller the top of the image and call it the image"
        );

        let frame = frame_from_buffer(
            &buffer,
            payload,
            WireTransport::GigeVision,
            &mut TimestampCalibration::default(),
        )
        .expect("the very same frame, at exactly its reservation, is delivered");
        assert_pixels_identical(
            frame.bytes.as_ref(),
            &exposed,
            "a frame at exactly its reservation",
        );
    }

    /// A buffer the camera never completed is refused, not delivered as a picture.
    ///
    /// An acquisition buffer carries its own verdict. One that was never filled still holds whatever
    /// its allocation happened to contain, and one the camera could not fit its image into holds a
    /// partial image; delivering either produces a photograph of nothing that is indistinguishable,
    /// downstream, from a real one.
    #[test]
    fn a_buffer_the_camera_never_completed_is_refused_rather_than_delivered() {
        const WIDTH: u32 = 64;
        const HEIGHT: u32 = 48;
        let payload = usize::try_from(WIDTH * HEIGHT).expect("a 64x48 payload fits a usize");
        let reservation = u64::try_from(payload).expect("the payload fits a u64");

        let never_filled = aravis::Buffer::new_allocate(payload);
        assert_ne!(
            never_filled.status(),
            aravis::BufferStatus::Success,
            "a buffer no camera ever wrote to has not succeeded at anything"
        );
        let error = frame_from_buffer(
            &never_filled,
            reservation,
            WireTransport::GigeVision,
            &mut TimestampCalibration::default(),
        )
        .expect_err("an unfilled buffer must never be delivered as an image");
        assert_eq!(error.code(), ErrorCode::BackendError);
        assert!(
            error.to_string().contains("incomplete acquisition buffer"),
            "the refusal must say the buffer was incomplete, got: {error}"
        );

        // The camera could not fit its picture into this one, and says so. The bytes that ARE in it
        // are a fragment; the adapter must refuse rather than deliver the fragment.
        let short = camera_filled_buffer(WIDTH, HEIGHT, aravis::PixelFormat::MONO_8, payload - 1);
        assert_eq!(
            short.status(),
            aravis::BufferStatus::SizeMismatch,
            "the camera must have refused to fit its image into the short buffer"
        );
        assert_eq!(
            frame_from_buffer(
                &short,
                reservation,
                WireTransport::GigeVision,
                &mut TimestampCalibration::default(),
            )
            .expect_err("a partial frame is not a frame")
            .code(),
            ErrorCode::BackendError,
            "a frame the camera could not complete must be refused, not delivered short"
        );
    }

    /// What the native worker was actually asked to photograph.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct SeenCaptureRequest {
        capture_id: String,
        maximum_frame_bytes: u64,
        width: Option<u32>,
        height: Option<u32>,
        pixel_format: Option<PixelFormat>,
    }

    /// A native backend whose session hands back one pinned, known picture.
    struct PinnedFrameNative {
        frame: CaptureFrame,
        refusal: Option<ErrorCode>,
        seen: Arc<Mutex<Vec<SeenCaptureRequest>>>,
    }

    impl NativeBackend for PinnedFrameNative {
        fn discover(
            &self,
            _interfaces: &[String],
            _maximum: usize,
            _deadline: Instant,
            _cancellation: &CancellationToken,
        ) -> Result<Vec<WireDevice>> {
            Ok(Vec::new())
        }

        fn connect(
            &self,
            _config: &GenicamBackendConfig,
            _deadline: Instant,
            _cancellation: &CancellationToken,
        ) -> Result<Box<dyn NativeSession>> {
            Ok(Box::new(PinnedFrameSession {
                frame: self.frame.clone(),
                refusal: self.refusal,
                seen: self.seen.clone(),
                capabilities: capabilities(),
            }))
        }
    }

    struct PinnedFrameSession {
        frame: CaptureFrame,
        refusal: Option<ErrorCode>,
        seen: Arc<Mutex<Vec<SeenCaptureRequest>>>,
        capabilities: CameraCapabilities,
    }

    impl NativeSession for PinnedFrameSession {
        fn capabilities(&self) -> &CameraCapabilities {
            &self.capabilities
        }

        fn status(
            &mut self,
            _deadline: Instant,
            _cancellation: &CancellationToken,
        ) -> Result<CameraStatus> {
            Ok(CameraStatus {
                online: true,
                connection_generation: 1,
                ptz: None,
                backend: json!({}),
            })
        }

        fn capture(&mut self, request: CaptureRequest, _deadline: Instant) -> Result<CaptureFrame> {
            self.seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(SeenCaptureRequest {
                    capture_id: request.capture_id.clone(),
                    maximum_frame_bytes: request.maximum_frame_bytes,
                    width: request.profile.width,
                    height: request.profile.height,
                    pixel_format: request.profile.pixel_format,
                });
            match self.refusal {
                Some(code) => rejected(code, "GenICam frame exceeds the accepted byte reservation"),
                None => Ok(self.frame.clone()),
            }
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// A picture with a byte at every extreme, so that any mutation of it shows.
    ///
    /// The pattern is aperiodic across rows and contains `0x00` and `0xff`, so a truncation, a
    /// dropped or duplicated row, a flipped bit, and a re-pack all change it.
    fn pinned_picture(width: u32, height: u32, pixel_format: PixelFormat) -> CaptureFrame {
        let length = usize::try_from(
            pixel_format
                .uncompressed_len(width, height)
                .expect("the pinned picture is uncompressed"),
        )
        .expect("the pinned picture fits a usize");
        let bytes = (0..length)
            .map(|index| match index % 4 {
                0 => 0x00,
                1 => 0xff,
                2 => u8::try_from(index % 251).unwrap_or(u8::MAX),
                _ => u8::try_from((index * 7) % 253).unwrap_or(u8::MAX),
            })
            .collect::<Vec<u8>>();
        CaptureFrame {
            bytes: Bytes::from(bytes),
            width,
            height,
            pixel_format,
            capture_mode: CaptureMode::SoftwareTrigger,
            source_timestamp: DateTime::from_timestamp(1_700_000_000, 123_456_789),
            timestamp_quality: FrameTimestampQuality::Camera,
            backend_metadata: BTreeMap::from([("frameId".to_owned(), json!(7_u64))]),
        }
    }

    /// The pixels the native camera produced cross the worker thread and the channel UNCHANGED.
    ///
    /// The GenICam session is a proxy: the native session lives on its own OS thread, and every frame
    /// it produces is carried back to the async caller over a channel. That hand-off is a seam where
    /// a truncation, a re-pack, or a stride mistake would substitute a different picture in silence,
    /// and nothing tested it.
    ///
    /// The request is checked in the same breath, because it is half of the same property: the native
    /// layer enforces the byte bound with the `maximum_frame_bytes` it is handed and configures the
    /// camera's region with the profile's geometry. A proxy that rewrote either on the way IN would
    /// corrupt the image just as thoroughly as one that rewrote it on the way out.
    #[tokio::test]
    async fn the_pixels_the_native_camera_produced_cross_the_worker_thread_unchanged() {
        for (width, height, pixel_format) in [
            (8_u32, 4_u32, PixelFormat::Mono8),
            (5, 3, PixelFormat::Rgb8),
            (5, 3, PixelFormat::Bgr8),
        ] {
            let exposed = pinned_picture(width, height, pixel_format);
            let seen = Arc::new(Mutex::new(Vec::new()));
            let factory = GenicamAravisBackendFactory::with_native(Arc::new(PinnedFrameNative {
                frame: exposed.clone(),
                refusal: None,
                seen: seen.clone(),
            }));
            let mut session = factory
                .connect(ConnectRequest {
                    instance_id: "camera-a".to_owned(),
                    backend: BackendConfig::GenicamAravis(backend_config()),
                    timeout: Duration::from_secs(5),
                    cancellation: CancellationToken::new(),
                })
                .await
                .expect("the pinned native camera must connect");

            let maximum_frame_bytes =
                u64::try_from(exposed.bytes.len()).expect("the picture fits a u64");
            let mut requested = profile();
            requested.width = Some(width);
            requested.height = Some(height);
            requested.pixel_format = Some(pixel_format);
            requested.maximum_frame_bytes = Some(maximum_frame_bytes);
            let frame = session
                .capture(CaptureRequest {
                    capture_id: "cap-frame-fidelity".to_owned(),
                    profile: requested,
                    maximum_frame_bytes,
                    timeout: Duration::from_secs(5),
                    cancellation: CancellationToken::new(),
                })
                .await
                .expect("the worker must deliver the frame its native session produced");

            let what = format!("a {width}x{height} {pixel_format:?} frame across the worker");
            assert_pixels_identical(frame.bytes.as_ref(), exposed.bytes.as_ref(), &what);
            assert_eq!(
                (frame.width, frame.height),
                (width, height),
                "{what}: the geometry every consumer reads those bytes through must survive the \
                 hand-off too"
            );
            assert_eq!(
                frame.pixel_format, pixel_format,
                "{what}: and so must the format that says what the bytes MEAN"
            );
            assert_eq!(frame.capture_mode, exposed.capture_mode);
            assert_eq!(
                frame.source_timestamp, exposed.source_timestamp,
                "{what}: an image delivered with somebody else's timestamp is evidence of the wrong \
                 moment"
            );
            assert_eq!(frame.timestamp_quality, exposed.timestamp_quality);
            assert_eq!(
                frame.backend_metadata, exposed.backend_metadata,
                "{what}: including the camera's own identifier for the picture this is"
            );

            let recorded = seen.lock().unwrap_or_else(PoisonError::into_inner).clone();
            assert_eq!(
                recorded,
                vec![SeenCaptureRequest {
                    capture_id: "cap-frame-fidelity".to_owned(),
                    maximum_frame_bytes,
                    width: Some(width),
                    height: Some(height),
                    pixel_format: Some(pixel_format),
                }],
                "{what}: the native layer must have been asked for exactly what the caller asked \
                 for -- a rewritten bound or geometry corrupts the image before it is ever taken"
            );
            session.close().await.expect("the session must close");
        }
    }

    /// A frame the native layer refused stays refused at the caller.
    ///
    /// The bound lives on the far side of the worker thread, so its refusal has to travel back across
    /// the same channel a frame would. A proxy that lost or softened it would leave the caller
    /// holding whatever the proxy chose to answer with instead -- and the one thing it must never
    /// answer with is a picture.
    #[tokio::test]
    async fn a_frame_the_native_layer_refused_is_never_delivered_as_a_partial_picture() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let factory = GenicamAravisBackendFactory::with_native(Arc::new(PinnedFrameNative {
            frame: pinned_picture(8, 4, PixelFormat::Mono8),
            refusal: Some(ErrorCode::ResourceLimit),
            seen: seen.clone(),
        }));
        let mut session = factory
            .connect(ConnectRequest {
                instance_id: "camera-a".to_owned(),
                backend: BackendConfig::GenicamAravis(backend_config()),
                timeout: Duration::from_secs(5),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("the pinned native camera must connect");

        let error = session
            .capture(CaptureRequest {
                capture_id: "cap-over-bound".to_owned(),
                profile: profile(),
                maximum_frame_bytes: 1,
                timeout: Duration::from_secs(5),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect_err("a frame the bound refused must not reach the caller as a frame");
        assert_eq!(
            error.code(),
            ErrorCode::ResourceLimit,
            "the refusal must arrive as a refusal; a caller that received a frame here would have \
             received a truncated image"
        );
        assert_eq!(
            seen.lock().unwrap_or_else(PoisonError::into_inner).len(),
            1,
            "and it must be the NATIVE layer's refusal -- the proxy did reach it, rather than \
             inventing an answer of its own"
        );
        session.close().await.expect("the session must close");
    }
}
