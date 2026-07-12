//! Per-camera serialized actor with independent capture, ordinary-control, and safety-stop lanes.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures::FutureExt;
use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::admission::{CaptureAdmissionQueue, ControlLanes, ControlWork, SafetyStop};
use crate::backend::CameraSession;
use crate::jobs::{CaptureDescriptor, CaptureDispatcher, DispatchReservation, JobEngine};
use crate::model::{PtzRequest, PtzResult};
use crate::{CameraError, ErrorCode, Result};

/// Handle used by command/admission code without exposing the actor-owned camera session.
#[derive(Clone)]
pub struct CameraActorHandle {
    shared: Arc<ActorShared>,
}

impl CameraActorHandle {
    /// Enqueues a bounded ordinary PTZ/control operation and awaits its serialized result.
    pub async fn ptz(
        &self,
        request: PtzRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PtzResult> {
        if !self.shared.accepting.load(Ordering::Acquire) {
            return Err(CameraError::rejected(
                ErrorCode::ComponentStopping,
                "camera actor is not accepting control work",
            ));
        }
        if deadline <= Instant::now() {
            return Err(ptz_timeout());
        }
        if cancellation.is_cancelled() {
            return Err(CameraError::rejected(
                ErrorCode::CaptureCancelled,
                "control operation was cancelled before queueing",
            ));
        }
        let operation_cancellation = CancellationToken::new();
        let (sender, receiver) = oneshot::channel();
        self.shared.controls.try_push_ordinary(OrdinaryControl {
            request,
            deadline,
            cancellation: operation_cancellation.clone(),
            result: sender,
        })?;
        self.shared.notify.notify_one();
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                operation_cancellation.cancel();
                Err(CameraError::rejected(ErrorCode::CaptureCancelled, "control operation cancelled"))
            }
            _ = tokio::time::sleep_until(deadline) => {
                operation_cancellation.cancel();
                Err(ptz_timeout())
            }
            result = receiver => result.map_err(|_| CameraError::Backend {
                backend: "actor",
                message: "camera actor stopped before replying to control work".to_string(),
            })?,
        }
    }

    /// Adds or coalesces a non-evictable safety stop and wakes the actor.
    pub fn safety_stop(&self, stop: SafetyStop) -> Result<()> {
        self.shared.controls.push_safety_stop(stop)?;
        self.shared.notify.notify_one();
        Ok(())
    }

    /// Current reserved-or-queued capture descriptor count.
    #[must_use]
    pub fn queued_captures(&self) -> usize {
        self.shared.slots.used.load(Ordering::Acquire)
    }

    /// Current ordinary-control count, excluding the independent safety lane.
    #[must_use]
    pub fn queued_controls(&self) -> usize {
        self.shared.controls.ordinary_len()
    }
}

impl CaptureDispatcher for CameraActorHandle {
    fn reserve(&self) -> Result<Box<dyn DispatchReservation>> {
        if !self.shared.accepting.load(Ordering::Acquire) {
            return Err(CameraError::rejected(
                ErrorCode::ComponentStopping,
                "camera actor is not accepting captures",
            ));
        }
        let slot = self.shared.slots.reserve()?;
        Ok(Box::new(ActorDispatchReservation {
            shared: Arc::clone(&self.shared),
            slot: Some(slot),
        }))
    }
}

/// Actor task. The caller/supervisor owns spawning, restart, session replacement, and shutdown.
pub struct CameraActor {
    shared: Arc<ActorShared>,
    engine: JobEngine,
    session: Box<dyn CameraSession>,
}

impl CameraActor {
    /// Creates an actor and non-owning command/dispatch handle.
    pub fn new(
        instance: impl Into<String>,
        session: Box<dyn CameraSession>,
        engine: JobEngine,
        max_queued_captures: usize,
        max_queued_controls: usize,
    ) -> Result<(Self, CameraActorHandle)> {
        let instance = instance.into();
        if instance.is_empty() || max_queued_captures == 0 {
            return Err(CameraError::rejected(
                ErrorCode::InvalidRequest,
                "actor instance and capture queue capacity must be non-empty",
            ));
        }
        let shared = Arc::new(ActorShared {
            instance,
            captures: CaptureAdmissionQueue::new(
                max_queued_captures,
                max_queued_captures,
                std::time::Duration::from_secs(1),
            )?,
            controls: Arc::new(ControlLanes::new(max_queued_controls)?),
            slots: Arc::new(DescriptorSlots {
                used: AtomicUsize::new(0),
                maximum: max_queued_captures,
            }),
            accepting: AtomicBool::new(true),
            notify: Notify::new(),
        });
        Ok((
            Self {
                shared: Arc::clone(&shared),
                engine,
                session,
            },
            CameraActorHandle { shared },
        ))
    }

    /// Runs serialized camera work until shutdown or an isolated session panic/fatal engine error.
    pub async fn run(mut self, shutdown: CancellationToken) -> Result<()> {
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            if let Some(control) = self.shared.controls.pop_next() {
                let result = AssertUnwindSafe(self.execute_control(control, &shutdown))
                    .catch_unwind()
                    .await;
                if result.is_err() {
                    self.shared.accepting.store(false, Ordering::Release);
                    self.reject_controls();
                    self.drain_queued_captures("camera actor failed").await;
                    let _ = self.session.close().await;
                    return Err(CameraError::Backend {
                        backend: "actor",
                        message: "camera session panicked during control work".to_string(),
                    });
                }
                continue;
            }

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = self.shared.notify.notified() => continue,
                queued = self.shared.captures.next(&shutdown) => {
                    let Some(queued) = queued else { break; };
                    let descriptor = queued.payload.into_descriptor();
                    let panic_descriptor = descriptor.clone();
                    let result = AssertUnwindSafe(
                        self.engine.execute(self.session.as_mut(), descriptor)
                    ).catch_unwind().await;
                    match result {
                        Ok(Ok(_)) => {}
                        Ok(Err(error)) => {
                            self.shared.accepting.store(false, Ordering::Release);
                            self.reject_controls();
                            self.drain_queued_captures("camera actor stopped after a fatal job error").await;
                            let _ = self.session.close().await;
                            return Err(error);
                        }
                        Err(_) => {
                            let _ = self.engine.fail_panic(&panic_descriptor).await;
                            self.shared.accepting.store(false, Ordering::Release);
                            self.reject_controls();
                            self.drain_queued_captures("camera actor stopped after a backend panic").await;
                            let _ = self.session.close().await;
                            return Err(CameraError::Backend {
                                backend: "actor",
                                message: "camera session panic was isolated to its owning actor".to_string(),
                            });
                        }
                    }
                }
            }
        }
        self.shared.accepting.store(false, Ordering::Release);
        self.drain_controls_for_shutdown().await;
        self.drain_queued_captures("component stopping").await;
        self.session.close().await
    }

    async fn drain_queued_captures(&mut self, reason: &'static str) {
        while let Some(queued) = self.shared.captures.try_next() {
            let descriptor = queued.payload.into_descriptor();
            let _ = self
                .engine
                .cancel_active(descriptor.capture_id(), reason)
                .await;
        }
    }

    fn reject_controls(&self) {
        while let Some(control) = self.shared.controls.pop_next() {
            if let ControlWork::Ordinary(operation) = control {
                let _ = operation.result.send(Err(CameraError::rejected(
                    ErrorCode::ComponentStopping,
                    "camera actor stopped before executing control work",
                )));
            }
        }
    }

    async fn drain_controls_for_shutdown(&mut self) {
        while let Some(control) = self.shared.controls.pop_next() {
            match control {
                ControlWork::SafetyStop(stop) => {
                    // A shutdown safety stop must be allowed to reach the transport even though
                    // the component cancellation token is already tripped. Its own tightened
                    // safety deadline remains the absolute bound.
                    let cancellation = CancellationToken::new();
                    let _ = self
                        .session
                        .ptz_bounded(
                            PtzRequest::Stop {
                                pan: stop.pan,
                                tilt: stop.tilt,
                                zoom: stop.zoom,
                            },
                            stop.deadline,
                            &cancellation,
                        )
                        .await;
                }
                ControlWork::Ordinary(operation) => {
                    let _ = operation.result.send(Err(CameraError::rejected(
                        ErrorCode::ComponentStopping,
                        "camera actor stopped before executing control work",
                    )));
                }
            }
        }
    }

    async fn execute_control(
        &mut self,
        control: ControlWork<OrdinaryControl>,
        shutdown: &CancellationToken,
    ) {
        match control {
            ControlWork::SafetyStop(stop) => {
                let request = PtzRequest::Stop {
                    pan: stop.pan,
                    tilt: stop.tilt,
                    zoom: stop.zoom,
                };
                // Safety stops intentionally outlive shutdown cancellation so a moving camera
                // is still given one bounded stop attempt. `ptz_bounded` prevents a hung
                // protocol call from blocking the reserved safety lane.
                let cancellation = CancellationToken::new();
                let _ = self
                    .session
                    .ptz_bounded(request, stop.deadline, &cancellation)
                    .await;
            }
            ControlWork::Ordinary(operation) => {
                let backend_cancellation = operation.cancellation.child_token();
                let future = self.session.ptz_bounded(
                    operation.request,
                    operation.deadline,
                    &backend_cancellation,
                );
                tokio::pin!(future);
                let result = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        backend_cancellation.cancel();
                        Err(CameraError::rejected(
                            ErrorCode::ComponentStopping,
                            "camera actor is stopping",
                        ))
                    },
                    _ = operation.cancellation.cancelled() => {
                        backend_cancellation.cancel();
                        Err(CameraError::rejected(
                            ErrorCode::CaptureCancelled,
                            "control operation cancelled",
                        ))
                    },
                    _ = tokio::time::sleep_until(operation.deadline) => {
                        backend_cancellation.cancel();
                        Err(ptz_timeout())
                    },
                    result = &mut future => result,
                };
                let _ = operation.result.send(result);
            }
        }
    }
}

struct ActorShared {
    instance: String,
    captures: CaptureAdmissionQueue<QueuedDescriptor>,
    controls: Arc<ControlLanes<OrdinaryControl>>,
    slots: Arc<DescriptorSlots>,
    accepting: AtomicBool,
    notify: Notify,
}

struct OrdinaryControl {
    request: PtzRequest,
    deadline: Instant,
    cancellation: CancellationToken,
    result: oneshot::Sender<Result<PtzResult>>,
}

struct QueuedDescriptor {
    descriptor: CaptureDescriptor,
    slot: DescriptorSlot,
}

impl QueuedDescriptor {
    fn into_descriptor(self) -> CaptureDescriptor {
        let Self { descriptor, slot } = self;
        drop(slot);
        descriptor
    }
}

struct DescriptorSlots {
    used: AtomicUsize,
    maximum: usize,
}

impl DescriptorSlots {
    fn reserve(self: &Arc<Self>) -> Result<DescriptorSlot> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.maximum).then_some(current + 1)
            })
            .map_err(|_| {
                CameraError::rejected(ErrorCode::QueueFull, "camera capture queue is full")
            })?;
        Ok(DescriptorSlot {
            slots: Arc::clone(self),
            released: false,
        })
    }
}

struct DescriptorSlot {
    slots: Arc<DescriptorSlots>,
    released: bool,
}

impl Drop for DescriptorSlot {
    fn drop(&mut self) {
        if !self.released {
            self.slots.used.fetch_sub(1, Ordering::AcqRel);
            self.released = true;
        }
    }
}

struct ActorDispatchReservation {
    shared: Arc<ActorShared>,
    slot: Option<DescriptorSlot>,
}

impl DispatchReservation for ActorDispatchReservation {
    fn commit(mut self: Box<Self>, descriptor: CaptureDescriptor) -> Result<usize> {
        if descriptor.instance() != self.shared.instance {
            return Err(CameraError::rejected(
                ErrorCode::UnknownInstance,
                "capture descriptor was dispatched to the wrong camera actor",
            ));
        }
        let camera_id = descriptor.instance().to_string();
        let priority = descriptor.priority();
        let deadline = descriptor.deadline();
        let cancellation = descriptor.cancellation();
        let slot = self.slot.take().ok_or_else(|| {
            CameraError::Catalog("actor dispatch reservation was already consumed".to_string())
        })?;
        self.shared.captures.try_enqueue(
            camera_id,
            priority,
            deadline,
            cancellation,
            QueuedDescriptor { descriptor, slot },
        )?;
        self.shared.notify.notify_one();
        Ok(self.shared.slots.used.load(Ordering::Acquire))
    }
}

fn ptz_timeout() -> CameraError {
    CameraError::rejected(ErrorCode::PtzTimeout, "PTZ operation exceeded its deadline")
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use chrono::Utc;
    use edgecommons::messaging::{Message, MessageBuilder};
    use serde_json::{Map, Value, json};
    use tempfile::TempDir;

    use super::*;
    use crate::admission::{AdmissionController, FilesystemSpaceProbe, SafetyStop};
    use crate::backend::sim::SimBackendFactory;
    use crate::backend::{
        CameraBackendFactory, CameraSession, CameraStatus, CaptureRequest, ConnectRequest,
    };
    use crate::catalog::{
        AcceptJobOutcome, Catalog, CatalogOptions, InstallOutcome, JobDeadlines, LedgerKey, NewJob,
        NewOutboxMessage,
    };
    use crate::config::{
        BackendConfig, CaptureInterlock, CaptureProfile, OfflinePolicy, OutputConfig,
        ProfileOutputConfig, SimBackendConfig, SimFaultConfig, SimFrameConfig, SimPattern,
        SimPtzConfig,
    };
    use crate::idempotency::canonical_request_hash;
    use crate::jobs::{
        AvailabilityGate, CaptureJobSpec, JobHooks, JobProfileSnapshot, JobSubmission,
        TerminalEnvelopeEncoder,
    };
    use crate::messages::{CameraSummary, CaptureTrigger, TerminalMessage};
    use crate::model::{
        BackendKind, CameraCapabilities, CaptureFrame, CaptureMode, JobState, OutputEncoding,
        PixelFormat, PtzVector,
    };
    use crate::storage::{
        InstallDecision, InstallGate, OutputPathVariables, StorageRoot, render_output_path,
    };

    struct TestEnvelopeEncoder;

    impl TerminalEnvelopeEncoder for TestEnvelopeEncoder {
        fn encode(
            &self,
            terminal: &TerminalMessage,
            created_at_ms: i64,
        ) -> Result<NewOutboxMessage> {
            let message = MessageBuilder::new(terminal.header_name(), "1.0")
                .correlation_id(terminal.correlation_id())
                .payload(terminal.body_value()?)
                .build();
            NewOutboxMessage::from_message(
                terminal.body().event_id.clone(),
                "terminal",
                format!(
                    "ecv1/device/camera-adapter/{}/app/{}",
                    terminal.body().camera_id,
                    terminal.channel()
                ),
                &message,
                created_at_ms,
                created_at_ms,
            )
        }
    }

    #[derive(Default)]
    struct RecordingHooks {
        waiters: AtomicUsize,
        groups: AtomicUsize,
    }

    #[async_trait]
    impl JobHooks for RecordingHooks {
        async fn settle_waiters(&self, _record: &crate::catalog::JobRecord, _body: &Value) {
            self.waiters.fetch_add(1, Ordering::SeqCst);
        }

        async fn group_member_terminal(&self, _record: &crate::catalog::JobRecord, _body: &Value) {
            self.groups.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct PausingInstallGate {
        catalog: Catalog,
        started: Notify,
        release: Notify,
        pause_before_start: AtomicBool,
        fail_after_start: AtomicBool,
    }

    #[async_trait]
    impl InstallGate for PausingInstallGate {
        async fn begin_install(
            &self,
            capture_id: &str,
            changed_at_ms: i64,
        ) -> Result<InstallDecision> {
            let pause_before = self.pause_before_start.load(Ordering::SeqCst);
            if pause_before {
                self.started.notify_one();
                self.release.notified().await;
            }
            let outcome = self
                .catalog
                .try_begin_install(capture_id.to_string(), changed_at_ms)
                .await?;
            match outcome {
                InstallOutcome::Started(_) => {
                    if !pause_before {
                        self.started.notify_one();
                        self.release.notified().await;
                    }
                    if self.fail_after_start.load(Ordering::SeqCst) {
                        return Err(CameraError::Storage(
                            "injected failure after install_started".to_string(),
                        ));
                    }
                    Ok(InstallDecision::Started)
                }
                InstallOutcome::AlreadyStarted(_) => Ok(InstallDecision::AlreadyStarted),
                InstallOutcome::WrongState(_) => Ok(InstallDecision::Rejected),
            }
        }
    }

    struct PanicSession {
        capabilities: CameraCapabilities,
    }

    struct OfflineGate;

    #[async_trait]
    impl AvailabilityGate for OfflineGate {
        async fn wait_until_ready(
            &self,
            _instance: &str,
            policy: OfflinePolicy,
            _queue_deadline_ms: Option<i64>,
            _terminal_deadline_ms: i64,
            _cancellation: &CancellationToken,
        ) -> Result<()> {
            assert_eq!(policy, OfflinePolicy::WaitUntilDeadline);
            Err(CameraError::rejected(
                ErrorCode::CameraUnavailable,
                "injected offline camera",
            ))
        }
    }

    #[async_trait]
    impl CameraSession for PanicSession {
        fn capabilities(&self) -> &CameraCapabilities {
            &self.capabilities
        }

        async fn status(&mut self) -> Result<CameraStatus> {
            unreachable!("status is not used by the capture actor")
        }

        async fn capture(&mut self, _request: CaptureRequest) -> Result<CaptureFrame> {
            panic!("injected backend panic")
        }

        async fn ptz(&mut self, _request: PtzRequest) -> Result<PtzResult> {
            Ok(PtzResult::Commanded)
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct HungControlSession {
        capabilities: CameraCapabilities,
        ordinary_started: Arc<AtomicBool>,
        safety_stops: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CameraSession for HungControlSession {
        fn capabilities(&self) -> &CameraCapabilities {
            &self.capabilities
        }

        async fn status(&mut self) -> Result<CameraStatus> {
            unreachable!("the control-lane regression test only invokes PTZ")
        }

        async fn capture(&mut self, _request: CaptureRequest) -> Result<CaptureFrame> {
            unreachable!("the control-lane regression test does not capture")
        }

        async fn ptz(&mut self, request: PtzRequest) -> Result<PtzResult> {
            match request {
                PtzRequest::Continuous { .. } => {
                    self.ordinary_started.store(true, Ordering::Release);
                    std::future::pending().await
                }
                PtzRequest::Stop { .. } => {
                    self.safety_stops.fetch_add(1, Ordering::AcqRel);
                    Ok(PtzResult::Commanded)
                }
                _ => unreachable!("the control-lane regression test only moves then stops"),
            }
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct Harness {
        catalog: Catalog,
        engine: JobEngine,
        actor: CameraActor,
        handle: CameraActorHandle,
        hooks: Arc<RecordingHooks>,
        pause: Option<Arc<PausingInstallGate>>,
        output: OutputConfig,
        _output_directory: TempDir,
        _state_directory: TempDir,
    }

    async fn harness(
        sim: SimBackendConfig,
        capture_capacity: usize,
        control_capacity: usize,
        sidecar: bool,
        pause_install: bool,
    ) -> Harness {
        let output_directory = tempfile::tempdir().unwrap();
        let state_directory = tempfile::tempdir().unwrap();
        let output = output(output_directory.path(), sidecar);
        let catalog = Catalog::open(CatalogOptions::new(state_directory.path()))
            .await
            .unwrap();
        let limits = crate::config::LimitsConfig {
            max_concurrent_captures: 2,
            max_concurrent_encodes: 2,
            max_concurrent_writes: 2,
            max_in_flight_bytes: 16 * 1024 * 1024,
            max_frame_bytes_per_camera: 4 * 1024 * 1024,
            ..crate::config::LimitsConfig::default()
        };
        let admission =
            AdmissionController::new(&limits, &output, Arc::new(FilesystemSpaceProbe::default()))
                .unwrap();
        let storage = StorageRoot::open(&output).unwrap();
        let hooks = Arc::new(RecordingHooks::default());
        let mut engine = JobEngine::new(
            catalog.clone(),
            admission,
            storage,
            Arc::new(TestEnvelopeEncoder),
            hooks.clone(),
        );
        let pause = pause_install.then(|| {
            Arc::new(PausingInstallGate {
                catalog: catalog.clone(),
                started: Notify::new(),
                release: Notify::new(),
                pause_before_start: AtomicBool::new(false),
                fail_after_start: AtomicBool::new(false),
            })
        });
        if let Some(gate) = &pause {
            engine = engine.with_install_gate(gate.clone());
        }
        let session = SimBackendFactory::new()
            .connect(ConnectRequest {
                instance_id: "cam-a".to_string(),
                backend: BackendConfig::Sim(sim),
                timeout: Duration::from_secs(1),
                cancellation: CancellationToken::new(),
            })
            .await
            .unwrap();
        let (actor, handle) = CameraActor::new(
            "cam-a",
            session,
            engine.clone(),
            capture_capacity,
            control_capacity,
        )
        .unwrap();
        Harness {
            catalog,
            engine,
            actor,
            handle,
            hooks,
            pause,
            output,
            _output_directory: output_directory,
            _state_directory: state_directory,
        }
    }

    fn output(root: &Path, sidecar: bool) -> OutputConfig {
        OutputConfig {
            root_directory: root.to_string_lossy().into_owned(),
            camera_directory_template: "{cameraId}".to_string(),
            file_name_template: "{timestamp}-{captureId}.{extension}".to_string(),
            write_metadata_sidecar: sidecar,
            minimum_free_bytes: 0,
            minimum_free_percent: 0,
            directory_mode: "0700".to_string(),
            file_mode: "0600".to_string(),
        }
    }

    fn sim(delay_ms: u64, fail: bool, ptz: bool) -> SimBackendConfig {
        SimBackendConfig {
            simulated_id: Some("sim-a".to_string()),
            seed: Some(7),
            frame: SimFrameConfig {
                width: 8,
                height: 8,
                pixel_format: PixelFormat::Rgb8,
                pattern: SimPattern::Checkerboard,
            },
            connect_delay_ms: 0,
            capture_delay_ms: delay_ms,
            ptz: SimPtzConfig {
                supported: ptz,
                status_supported: true,
                presets_supported: false,
            },
            faults: SimFaultConfig {
                fail_every_nth_capture: fail.then_some(1),
                ..SimFaultConfig::default()
            },
        }
    }

    fn submission(
        output: &OutputConfig,
        capture_id: &str,
        encoding: OutputEncoding,
    ) -> JobSubmission {
        let accepted_at_ms = Utc::now().timestamp_millis();
        let profile = JobProfileSnapshot {
            name: "inspection".to_string(),
            capture: CaptureProfile {
                capture_mode: Some(CaptureMode::Simulated),
                offline_policy: Some(OfflinePolicy::WaitUntilDeadline),
                queue_expiry_ms: None,
                timeout_ms: Some(10_000),
                maximum_frame_bytes: Some(1024 * 1024),
                pixel_format: Some(PixelFormat::Rgb8),
                width: Some(8),
                height: Some(8),
                offset_x: None,
                offset_y: None,
                exposure_micros: None,
                gain: None,
                output: ProfileOutputConfig {
                    encoding,
                    jpeg_quality: 90,
                },
                capture_interlock: Some(CaptureInterlock::Allow),
            },
            offline_policy: OfflinePolicy::WaitUntilDeadline,
            maximum_frame_bytes: 1024 * 1024,
            capture_mode: CaptureMode::Simulated,
            capture_interlock: CaptureInterlock::Allow,
            settle_ms: 0,
        };
        let relative_path = render_output_path(
            output,
            OutputPathVariables {
                camera_id: "cam-a",
                capture_id,
                timestamp: Utc::now(),
            },
            encoding,
        )
        .unwrap();
        let request_id = format!("request-{capture_id}");
        let canonical_request = json!({
            "requestId": request_id,
            "captureProfile": "inspection"
        });
        let trigger = CaptureTrigger::Command {
            request_id: request_id.clone(),
        };
        let deadlines = JobDeadlines {
            terminal_at_ms: accepted_at_ms + 10_000,
            queue_at_ms: None,
            capture_at_ms: accepted_at_ms + 4_000,
            encode_at_ms: accepted_at_ms + 6_000,
            persist_at_ms: accepted_at_ms + 8_000,
        };
        let spec = CaptureJobSpec {
            capture_id: capture_id.to_string(),
            instance: "cam-a".to_string(),
            profile: profile.clone(),
            resource_group: None,
            relative_path: relative_path.clone(),
            deadlines: deadlines.clone(),
            accepted_at_ms,
            trigger: trigger.clone(),
            correlation_id: format!("correlation-{capture_id}"),
            metadata: Map::new(),
            camera: CameraSummary {
                backend: BackendKind::Sim,
                vendor: Some("EdgeCommons".to_string()),
                model: Some("SimBackend".to_string()),
                firmware: None,
                serial: Some("sim-a".to_string()),
            },
            group_size: None,
        };
        JobSubmission {
            job: NewJob {
                capture_id: capture_id.to_string(),
                instance: "cam-a".to_string(),
                ledger_key: Some(LedgerKey::new("cam-a", "sb/capture-submit", request_id).unwrap()),
                canonical_request: canonical_request.clone(),
                request_hash: canonical_request_hash(&canonical_request, false).unwrap(),
                effective_profile: serde_json::to_value(&profile).unwrap(),
                deadlines,
                trigger: serde_json::to_value(trigger).unwrap(),
                origin_correlation_id: Some(format!("correlation-{capture_id}")),
                intended_output: json!({
                    "relativePath": relative_path.as_wire_path(),
                    "backend": "sim"
                }),
                accepted_at_ms,
                group_id: None,
            },
            spec,
            priority: crate::admission::CapturePriority::Submitted,
        }
    }

    async fn terminal(catalog: &Catalog, capture_id: &str) -> crate::catalog::JobRecord {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(record) = catalog.job(capture_id).await.unwrap() {
                    if record.state.is_terminal() {
                        return record;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("job did not become terminal")
    }

    async fn wait_for_state(catalog: &Catalog, capture_id: &str, state: JobState) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if catalog
                    .job(capture_id)
                    .await
                    .unwrap()
                    .is_some_and(|record| record.state == state)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    fn partials(root: &Path) -> Vec<std::path::PathBuf> {
        let mut found = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().ends_with(".partial"))
                {
                    found.push(path);
                }
            }
        }
        found
    }

    #[tokio::test]
    async fn sim_capture_runs_end_to_end_with_sidecar_and_one_terminal_outbox() {
        let harness = harness(sim(5, false, false), 2, 2, true, false).await;
        let Harness {
            catalog,
            engine,
            actor,
            handle,
            hooks,
            output,
            _output_directory,
            _state_directory,
            ..
        } = harness;
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(actor.run(shutdown.clone()));
        assert!(matches!(
            engine
                .accept_and_queue(&handle, submission(&output, "cap-e2e", OutputEncoding::Png))
                .await
                .unwrap(),
            AcceptJobOutcome::Inserted(_)
        ));

        let record = terminal(&catalog, "cap-e2e").await;
        assert_eq!(record.state, JobState::Succeeded);
        let image = &record.terminal_result.as_ref().unwrap()["image"];
        let path = image["absolutePath"].as_str().unwrap();
        assert!(Path::new(path).exists());
        let sidecar_path = format!("{path}.json");
        assert!(Path::new(&sidecar_path).exists());
        let sidecar: Value =
            serde_json::from_slice(&std::fs::read(&sidecar_path).unwrap()).unwrap();
        assert_eq!(sidecar, *record.terminal_result.as_ref().unwrap());
        assert!(sidecar["timestamps"]["persistedAt"].is_string());
        let outbox = catalog
            .pending_outbox(Utc::now().timestamp_millis() + 1000, 10)
            .await
            .unwrap();
        assert_eq!(outbox.len(), 1);
        let envelope = Message::from_slice(&outbox[0].encoded_envelope).unwrap();
        assert_eq!(envelope.body, sidecar);
        assert_eq!(hooks.waiters.load(Ordering::SeqCst), 1);
        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancellation_during_sim_acquisition_wins_once_and_leaves_no_image() {
        let harness = harness(sim(250, false, false), 2, 2, false, false).await;
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
        harness
            .engine
            .accept_and_queue(
                &harness.handle,
                submission(&harness.output, "cap-cancel", OutputEncoding::Raw),
            )
            .await
            .unwrap();
        wait_for_state(&harness.catalog, "cap-cancel", JobState::Acquiring).await;

        let cancelled = harness
            .engine
            .cancel_active("cap-cancel", "operator cancelled")
            .await
            .unwrap();
        assert!(cancelled.cancelled);
        assert_eq!(cancelled.state, JobState::Cancelled);
        let record = terminal(&harness.catalog, "cap-cancel").await;
        assert_eq!(record.state, JobState::Cancelled);
        assert_eq!(
            harness
                .catalog
                .pending_outbox(Utc::now().timestamp_millis() + 1000, 10)
                .await
                .unwrap()
                .len(),
            1
        );
        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn installation_cas_defeats_cancellation_and_capture_succeeds() {
        let harness = harness(sim(1, false, false), 2, 2, false, true).await;
        let gate = harness.pause.as_ref().unwrap().clone();
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
        harness
            .engine
            .accept_and_queue(
                &harness.handle,
                submission(&harness.output, "cap-install", OutputEncoding::Raw),
            )
            .await
            .unwrap();
        gate.started.notified().await;

        let cancellation = harness
            .engine
            .cancel_active("cap-install", "too late")
            .await
            .unwrap();
        assert!(!cancellation.cancelled);
        assert_eq!(cancellation.state, JobState::Persisting);
        gate.release.notify_one();
        assert_eq!(
            terminal(&harness.catalog, "cap-install").await.state,
            JobState::Succeeded
        );
        assert_eq!(
            harness
                .catalog
                .pending_outbox(Utc::now().timestamp_millis() + 1000, 10)
                .await
                .unwrap()
                .len(),
            1
        );
        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn failure_after_install_started_is_left_persisting_for_recovery() {
        let harness = harness(sim(1, false, false), 2, 2, false, true).await;
        let gate = harness.pause.as_ref().unwrap().clone();
        gate.fail_after_start.store(true, Ordering::SeqCst);
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
        harness
            .engine
            .accept_and_queue(
                &harness.handle,
                submission(&harness.output, "cap-recovery-owned", OutputEncoding::Raw),
            )
            .await
            .unwrap();
        gate.started.notified().await;
        gate.release.notify_one();
        wait_for_state(&harness.catalog, "cap-recovery-owned", JobState::Persisting).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let record = harness
            .catalog
            .job("cap-recovery-owned")
            .await
            .unwrap()
            .unwrap();
        assert!(record.install_started);
        assert_eq!(record.state, JobState::Persisting);
        assert!(
            harness
                .catalog
                .pending_outbox(Utc::now().timestamp_millis() + 1000, 10)
                .await
                .unwrap()
                .is_empty()
        );
        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn crash_window_recovery_reuses_exact_pending_success_with_and_without_sidecar() {
        for sidecar in [false, true] {
            let harness = harness(sim(1, false, false), 2, 2, sidecar, true).await;
            let gate = harness.pause.as_ref().unwrap().clone();
            gate.fail_after_start.store(true, Ordering::SeqCst);
            let capture_id = if sidecar {
                "cap-recover-sidecar"
            } else {
                "cap-recover-image"
            };
            let shutdown = CancellationToken::new();
            let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
            harness
                .engine
                .accept_and_queue(
                    &harness.handle,
                    submission(&harness.output, capture_id, OutputEncoding::Raw),
                )
                .await
                .unwrap();
            gate.started.notified().await;
            gate.release.notify_one();
            wait_for_state(&harness.catalog, capture_id, JobState::Persisting).await;
            tokio::time::sleep(Duration::from_millis(40)).await;

            let recovery = harness.catalog.job(capture_id).await.unwrap().unwrap();
            assert!(recovery.install_started);
            assert!(recovery.expected_sha256.is_some());
            assert!(recovery.expected_bytes.is_some());
            assert!(recovery.pending_success.is_some());
            assert!(Path::new(recovery.partial_path.as_deref().unwrap()).exists());
            let terminal = harness
                .engine
                .recover_install_started(recovery, &CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(terminal.state, JobState::Succeeded);
            assert!(terminal.pending_success.is_none());
            let outbox = harness
                .catalog
                .pending_outbox(Utc::now().timestamp_millis() + 1000, 10)
                .await
                .unwrap();
            assert_eq!(outbox.len(), 1);
            let envelope = Message::from_slice(&outbox[0].encoded_envelope).unwrap();
            assert_eq!(envelope.body, terminal.terminal_result.clone().unwrap());
            if sidecar {
                let image_path =
                    terminal.terminal_result.as_ref().unwrap()["image"]["absolutePath"]
                        .as_str()
                        .unwrap();
                let sidecar_body: Value =
                    serde_json::from_slice(&std::fs::read(format!("{image_path}.json")).unwrap())
                        .unwrap();
                assert_eq!(sidecar_body, envelope.body);
            }
            shutdown.cancel();
            actor_task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn recovery_rejects_valid_json_sidecar_that_differs_from_staged_terminal_body() {
        let harness = harness(sim(1, false, false), 2, 2, true, true).await;
        let gate = harness.pause.as_ref().unwrap().clone();
        gate.fail_after_start.store(true, Ordering::SeqCst);
        let capture_id = "cap-recover-tampered-sidecar";
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
        harness
            .engine
            .accept_and_queue(
                &harness.handle,
                submission(&harness.output, capture_id, OutputEncoding::Raw),
            )
            .await
            .unwrap();
        gate.started.notified().await;
        gate.release.notify_one();
        wait_for_state(&harness.catalog, capture_id, JobState::Persisting).await;
        tokio::time::sleep(Duration::from_millis(40)).await;

        let recovery = harness.catalog.job(capture_id).await.unwrap().unwrap();
        assert!(recovery.install_started);
        assert!(recovery.pending_success.is_some());
        let partial_path = recovery.partial_path.clone().unwrap();
        let sidecar_path = format!("{}.json", recovery.final_path.as_deref().unwrap());
        assert!(Path::new(&partial_path).exists());
        assert!(Path::new(&sidecar_path).exists());
        std::fs::write(&sidecar_path, b"{}\n").unwrap();

        let error = harness
            .engine
            .recover_install_started(recovery, &CancellationToken::new())
            .await
            .expect_err("valid but non-exact sidecar must never recover as success");
        assert!(
            error
                .to_string()
                .contains("does not match the exact durable terminal body")
        );
        assert!(!Path::new(&partial_path).exists());
        assert!(!Path::new(&sidecar_path).exists());
        let retained = harness.catalog.job(capture_id).await.unwrap().unwrap();
        assert_eq!(retained.state, JobState::Persisting);
        assert!(retained.pending_success.is_some());
        assert!(
            harness
                .catalog
                .pending_outbox(Utc::now().timestamp_millis() + 1000, 10)
                .await
                .unwrap()
                .is_empty()
        );

        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancellation_before_install_cas_drops_the_prepared_partial() {
        let harness = harness(sim(1, false, false), 2, 2, false, true).await;
        let gate = harness.pause.as_ref().unwrap().clone();
        gate.pause_before_start.store(true, Ordering::SeqCst);
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
        harness
            .engine
            .accept_and_queue(
                &harness.handle,
                submission(&harness.output, "cap-pre-cas", OutputEncoding::Raw),
            )
            .await
            .unwrap();
        gate.started.notified().await;

        let result = harness
            .engine
            .cancel_active("cap-pre-cas", "cancel before install")
            .await
            .unwrap();
        assert!(result.cancelled);
        gate.release.notify_one();
        assert_eq!(
            terminal(&harness.catalog, "cap-pre-cas").await.state,
            JobState::Cancelled
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(partials(Path::new(&harness.output.root_directory)).is_empty());
        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn safety_and_control_precede_capture_and_all_queues_remain_bounded() {
        let harness = harness(sim(5, false, true), 1, 1, false, false).await;
        harness
            .engine
            .accept_and_queue(
                &harness.handle,
                submission(&harness.output, "cap-first", OutputEncoding::Raw),
            )
            .await
            .unwrap();
        let second = harness
            .engine
            .accept_and_queue(
                &harness.handle,
                submission(&harness.output, "cap-second", OutputEncoding::Raw),
            )
            .await
            .unwrap_err();
        assert_eq!(second.code(), ErrorCode::QueueFull);
        assert!(harness.catalog.job("cap-second").await.unwrap().is_none());

        let handle = harness.handle.clone();
        let control = tokio::spawn(async move {
            handle
                .ptz(
                    PtzRequest::Continuous {
                        velocity: PtzVector {
                            pan: 0.5,
                            tilt: 0.0,
                            zoom: 0.0,
                        },
                        timeout: Duration::from_secs(2),
                    },
                    Instant::now() + Duration::from_secs(3),
                    &CancellationToken::new(),
                )
                .await
        });
        while harness.handle.queued_controls() == 0 {
            tokio::task::yield_now().await;
        }
        let overflow = harness
            .handle
            .ptz(
                PtzRequest::Home,
                Instant::now() + Duration::from_secs(3),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(overflow.code(), ErrorCode::QueueFull);
        harness
            .handle
            .safety_stop(SafetyStop {
                pan: true,
                tilt: true,
                zoom: true,
                deadline: Instant::now() + Duration::from_secs(2),
            })
            .unwrap();

        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
        assert_eq!(control.await.unwrap().unwrap(), PtzResult::Commanded);
        let status = harness
            .handle
            .ptz(
                PtzRequest::Status,
                Instant::now() + Duration::from_secs(3),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let PtzResult::Status(status) = status else {
            panic!("expected PTZ status");
        };
        assert_eq!(
            status.moving,
            Some(true),
            "safety stop must run before ordinary move"
        );
        assert_eq!(
            terminal(&harness.catalog, "cap-first").await.state,
            JobState::Succeeded
        );
        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn hung_ordinary_ptz_cannot_block_the_safety_lane_past_its_deadline() {
        let harness = harness(sim(1, false, false), 2, 2, false, false).await;
        let ordinary_started = Arc::new(AtomicBool::new(false));
        let safety_stops = Arc::new(AtomicUsize::new(0));
        let session = HungControlSession {
            capabilities: CameraCapabilities {
                capture_modes: vec![CaptureMode::Simulated],
                pixel_formats: vec![PixelFormat::Rgb8],
                software_trigger: false,
                snapshot_uri: false,
                rtsp: false,
                ptz: true,
                ptz_status: true,
                presets: false,
                preset_mutation: false,
                vendor: None,
                model: None,
                firmware: None,
                serial: None,
                warnings: Vec::new(),
            },
            ordinary_started: Arc::clone(&ordinary_started),
            safety_stops: Arc::clone(&safety_stops),
        };
        let (actor, handle) =
            CameraActor::new("cam-a", Box::new(session), harness.engine.clone(), 2, 2)
                .expect("test actor");
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(actor.run(shutdown.clone()));
        let control_handle = handle.clone();
        let control_cancellation = CancellationToken::new();
        let control = tokio::spawn(async move {
            control_handle
                .ptz(
                    PtzRequest::Continuous {
                        velocity: PtzVector {
                            pan: 0.5,
                            tilt: 0.0,
                            zoom: 0.0,
                        },
                        timeout: Duration::from_secs(1),
                    },
                    Instant::now() + Duration::from_millis(100),
                    &control_cancellation,
                )
                .await
        });
        while !ordinary_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        handle
            .safety_stop(SafetyStop {
                pan: true,
                tilt: true,
                zoom: true,
                deadline: Instant::now() + Duration::from_millis(200),
            })
            .expect("safety stop must use its independent lane");

        tokio::time::advance(Duration::from_millis(101)).await;
        assert_eq!(
            control
                .await
                .expect("control task must not panic")
                .expect_err("hung ordinary PTZ must time out")
                .code(),
            ErrorCode::PtzTimeout
        );
        while safety_stops.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(safety_stops.load(Ordering::Acquire), 1);
        shutdown.cancel();
        actor_task
            .await
            .expect("actor task must not panic")
            .expect("actor must shut down cleanly");
    }

    #[tokio::test]
    async fn deterministic_sim_stage_failure_commits_one_failed_outbox() {
        let harness = harness(sim(1, true, false), 2, 2, false, false).await;
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
        harness
            .engine
            .accept_and_queue(
                &harness.handle,
                submission(&harness.output, "cap-fail", OutputEncoding::Raw),
            )
            .await
            .unwrap();
        let record = terminal(&harness.catalog, "cap-fail").await;
        assert_eq!(record.state, JobState::Failed);
        assert_eq!(record.error_code.as_deref(), Some("BACKEND_ERROR"));
        assert_eq!(
            record.terminal_result.as_ref().unwrap()["failure"]["stage"],
            "ACQUIRING"
        );
        assert_eq!(
            harness
                .catalog
                .pending_outbox(Utc::now().timestamp_millis() + 1000, 10)
                .await
                .unwrap()
                .len(),
            1
        );
        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn capture_interlock_rejects_motion_but_allow_captures_through() {
        let harness = harness(sim(1, false, true), 2, 2, false, false).await;
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
        harness
            .handle
            .ptz(
                PtzRequest::Continuous {
                    velocity: PtzVector {
                        pan: 0.5,
                        tilt: 0.0,
                        zoom: 0.0,
                    },
                    timeout: Duration::from_secs(5),
                },
                Instant::now() + Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let mut rejected = submission(&harness.output, "cap-moving-reject", OutputEncoding::Raw);
        rejected.spec.profile.capture_interlock = CaptureInterlock::Reject;
        rejected.job.effective_profile = serde_json::to_value(&rejected.spec.profile).unwrap();
        harness
            .engine
            .accept_and_queue(&harness.handle, rejected)
            .await
            .unwrap();
        let record = terminal(&harness.catalog, "cap-moving-reject").await;
        assert_eq!(record.state, JobState::Failed);
        assert_eq!(record.error_code.as_deref(), Some("CAMERA_MOVING"));

        let mut allowed = submission(&harness.output, "cap-moving-allow", OutputEncoding::Raw);
        allowed.spec.profile.capture_interlock = CaptureInterlock::Allow;
        allowed.job.effective_profile = serde_json::to_value(&allowed.spec.profile).unwrap();
        harness
            .engine
            .accept_and_queue(&harness.handle, allowed)
            .await
            .unwrap();
        assert_eq!(
            terminal(&harness.catalog, "cap-moving-allow").await.state,
            JobState::Succeeded
        );
        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stop_and_settle_uses_stop_acknowledgement_when_status_is_unavailable() {
        let mut config = sim(1, false, true);
        config.ptz.status_supported = false;
        let harness = harness(config, 2, 2, false, false).await;
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
        harness
            .handle
            .ptz(
                PtzRequest::Continuous {
                    velocity: PtzVector {
                        pan: 0.5,
                        tilt: 0.0,
                        zoom: 0.0,
                    },
                    timeout: Duration::from_secs(5),
                },
                Instant::now() + Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let mut capture = submission(&harness.output, "cap-stop-settle", OutputEncoding::Raw);
        capture.spec.profile.capture_interlock = CaptureInterlock::StopAndSettle;
        capture.spec.profile.settle_ms = 1;
        capture.job.effective_profile = serde_json::to_value(&capture.spec.profile).unwrap();
        harness
            .engine
            .accept_and_queue(&harness.handle, capture)
            .await
            .unwrap();
        assert_eq!(
            terminal(&harness.catalog, "cap-stop-settle").await.state,
            JobState::Succeeded
        );
        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn immutable_offline_policy_flows_through_the_availability_seam() {
        let mut harness = harness(sim(1, false, false), 2, 2, false, false).await;
        let engine = harness
            .engine
            .clone()
            .with_availability(Arc::new(OfflineGate));
        harness.engine = engine.clone();
        harness.actor.engine = engine;
        let shutdown = CancellationToken::new();
        let actor_task = tokio::spawn(harness.actor.run(shutdown.clone()));
        harness
            .engine
            .accept_and_queue(
                &harness.handle,
                submission(&harness.output, "cap-offline", OutputEncoding::Raw),
            )
            .await
            .unwrap();

        let record = terminal(&harness.catalog, "cap-offline").await;
        assert_eq!(record.state, JobState::Failed);
        assert_eq!(record.error_code.as_deref(), Some("CAMERA_UNAVAILABLE"));
        assert_eq!(
            record.terminal_result.as_ref().unwrap()["failure"]["stage"],
            "QUEUED"
        );
        shutdown.cancel();
        actor_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn backend_panic_is_terminalized_and_isolated_to_its_actor() {
        let mut harness = harness(sim(1, false, false), 2, 2, false, false).await;
        let capabilities = harness.actor.session.capabilities().clone();
        harness.actor.session = Box::new(PanicSession { capabilities });
        let actor_task = tokio::spawn(harness.actor.run(CancellationToken::new()));
        harness
            .engine
            .accept_and_queue(
                &harness.handle,
                submission(&harness.output, "cap-panic", OutputEncoding::Raw),
            )
            .await
            .unwrap();

        assert!(actor_task.await.unwrap().is_err());
        let record = terminal(&harness.catalog, "cap-panic").await;
        assert_eq!(record.state, JobState::Failed);
        assert_eq!(record.error_code.as_deref(), Some("BACKEND_ERROR"));
        assert_eq!(
            harness
                .catalog
                .pending_outbox(Utc::now().timestamp_millis() + 1000, 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn shutdown_terminalizes_durable_queued_descriptors() {
        let harness = harness(sim(1, false, false), 2, 2, false, false).await;
        for capture_id in ["cap-shutdown-a", "cap-shutdown-b"] {
            harness
                .engine
                .accept_and_queue(
                    &harness.handle,
                    submission(&harness.output, capture_id, OutputEncoding::Raw),
                )
                .await
                .unwrap();
        }
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        harness.actor.run(shutdown).await.unwrap();

        for capture_id in ["cap-shutdown-a", "cap-shutdown-b"] {
            assert_eq!(
                terminal(&harness.catalog, capture_id).await.state,
                JobState::Cancelled
            );
        }
        assert_eq!(
            harness
                .catalog
                .pending_outbox(Utc::now().timestamp_millis() + 1000, 10)
                .await
                .unwrap()
                .len(),
            2
        );
    }
}
