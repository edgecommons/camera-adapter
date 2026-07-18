//! The simulator-backed runtime suite.
//!
//! `coverage_*` are siblings rather than more functions in here: they are new tests written against
//! production paths the suite had never reached, and keeping them separate keeps this file readable.

mod coverage_command;
mod coverage_reload;
mod coverage_supervision;

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use edgecommons::{messaging::MessageBuilder, prelude::EdgeCommonsBuilder};
use tempfile::TempDir;

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
use serde::Serialize;
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
use serde_json::Value;
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
use std::{fs, path::PathBuf, time::Instant};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::*;
use crate::jobs::testing::RecordingAnnouncer;

type RecordedMqttPublishes = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// Records what the component emits, so a test can assert that it emits anything at all.
///
/// The camera adapter shipped with zero call sites for the metric subsystem, so the useful
/// assertion is not "the numbers are right" but "the wiring exists" -- a metric nobody emits
/// is indistinguishable from a metric that does not exist.
#[derive(Default)]
/// An emission as a metric TARGET sees it: the values, and the dimensions the definition
/// carried at the moment it was emitted.
#[derive(Debug, Clone)]
struct RecordedEmission {
    metric: String,
    values: std::collections::HashMap<String, f64>,
    dimensions: std::collections::BTreeMap<String, String>,
}

#[derive(Default)]
struct RecordingMetrics {
    defined: Mutex<std::collections::HashMap<String, edgecommons::metrics::Metric>>,
    emitted: Mutex<Vec<RecordedEmission>>,
}

impl RecordingMetrics {
    fn counts(&self, metric: &str, measure: &str) -> f64 {
        self.emitted
            .lock()
            .unwrap()
            .iter()
            .filter(|emission| emission.metric == metric)
            .filter_map(|emission| emission.values.get(measure))
            .sum()
    }

    /// Every `southbound_health` emission that carried this camera's `instance` dimension.
    fn health_for(&self, instance: &str) -> Vec<RecordedEmission> {
        self.emitted
            .lock()
            .unwrap()
            .iter()
            .filter(|emission| {
                emission.metric == crate::observability::HEALTH_METRIC
                    && emission.dimensions.get("instance").map(String::as_str)
                        == Some(instance)
            })
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl edgecommons::metrics::MetricService for RecordingMetrics {
    fn define_metric(&self, metric: edgecommons::metrics::Metric) {
        self.defined
            .lock()
            .unwrap()
            .insert(metric.get_name().to_owned(), metric);
    }

    fn is_metric_defined(&self, name: &str) -> bool {
        self.defined.lock().unwrap().contains_key(name)
    }

    async fn emit_metric(
        &self,
        name: &str,
        values: std::collections::HashMap<String, f64>,
    ) -> edgecommons::Result<()> {
        // A real target is handed the definition and the values together, so the dimensions
        // are whatever the definition carries at THIS instant. Recording them any other way
        // would hide exactly the mistake worth catching.
        let dimensions = self
            .defined
            .lock()
            .unwrap()
            .get(name)
            .map(|metric| metric.get_dimensions().clone())
            .unwrap_or_default();
        self.emitted.lock().unwrap().push(RecordedEmission {
            metric: name.to_owned(),
            values,
            dimensions,
        });
        Ok(())
    }

    async fn emit_metric_now(
        &self,
        name: &str,
        values: std::collections::HashMap<String, f64>,
    ) -> edgecommons::Result<()> {
        self.emit_metric(name, values).await
    }

    async fn flush_metrics(&self) -> edgecommons::Result<()> {
        Ok(())
    }

    async fn shutdown(&self) {}
}

/// A caller waiting on a capture. It exists because `DeferredReplyToken` cannot be built
/// outside the core library -- which is exactly why the waiter bound had never been tested.
#[derive(Default)]
struct CountingWaiter {
    settled: AtomicUsize,
}

#[async_trait::async_trait]
impl CaptureWaiter for CountingWaiter {
    async fn settle(&self, _result: serde_json::Value) -> bool {
        self.settled.fetch_add(1, Ordering::AcqRel);
        true
    }
}

fn core_config_value(root: &Path, cameras: &[&str], schedules: bool) -> serde_json::Value {
    let root = root.to_string_lossy();
    let instances = cameras
        .iter()
        .map(|id| {
            let mut camera = json!({
                "id": id,
                "backend": { "type": "sim" },
                "defaultCaptureProfile": "main",
                "captureProfiles": { "main": { "output": { "encoding": "jpeg" } } }
            });
            if schedules && *id == "camera-a" {
                camera["schedules"] = json!([{
                    "id": "minute",
                    "cron": "0 * * * * *",
                    "timezone": "UTC",
                    "captureProfile": "main"
                }]);
            }
            camera
        })
        .collect::<Vec<_>>();
    let raw = json!({
        "component": {
            "global": { "output": { "rootDirectory": root.as_ref() } },
            "instances": instances,
        }
    });
    raw
}

fn core_config(root: &Path, cameras: &[&str], schedules: bool) -> Config {
    Config::from_value(
        COMPONENT_NAME,
        "gw-01",
        core_config_value(root, cameras, schedules),
    )
    .unwrap()
}

fn config(root: &Path, cameras: &[&str], schedules: bool) -> AdapterConfig {
    AdapterConfig::from_core_reload(&core_config(root, cameras, schedules)).unwrap()
}

async fn runtime(config: AdapterConfig, directory: &TempDir) -> Arc<CameraRuntime> {
    runtime_with_storage_pressure(config, directory, None).await
}

/// Builds the test runtime and hands back the metric recorder wired into it.
async fn runtime_with_metrics(
    config: AdapterConfig,
    directory: &TempDir,
) -> (Arc<CameraRuntime>, Arc<RecordingMetrics>) {
    let recorder = Arc::new(RecordingMetrics::default());
    let runtime = runtime_with_storage_pressure_and_metrics(
        config,
        directory,
        None,
        Arc::clone(&recorder) as Arc<dyn edgecommons::metrics::MetricService>,
    )
    .await;
    (runtime, recorder)
}

async fn runtime_with_storage_pressure(
    config: AdapterConfig,
    directory: &TempDir,
    storage_pressure: Option<StoragePressureMonitor>,
) -> Arc<CameraRuntime> {
    runtime_with_storage_pressure_and_metrics(
        config,
        directory,
        storage_pressure,
        Arc::new(RecordingMetrics::default()),
    )
    .await
}

/// The runtime, plus the announcer every one of its engines publishes through.
///
/// The announcer is SHARED across the engines rather than one per camera, so a test can ask what the
/// component announced -- for the whole fleet, in order -- which is the only place a volatile detail
/// of an announcement (the thumbnail) can be observed at all.
async fn runtime_with_announcer(
    config: AdapterConfig,
    directory: &TempDir,
) -> (Arc<CameraRuntime>, Arc<RecordingAnnouncer>) {
    let announcer = Arc::new(RecordingAnnouncer::default());
    let runtime = runtime_with_everything(
        config,
        directory,
        None,
        Arc::new(RecordingMetrics::default()),
        Arc::clone(&announcer),
    )
    .await;
    (runtime, announcer)
}

async fn runtime_with_storage_pressure_and_metrics(
    config: AdapterConfig,
    directory: &TempDir,
    storage_pressure: Option<StoragePressureMonitor>,
    metrics: Arc<dyn edgecommons::metrics::MetricService>,
) -> Arc<CameraRuntime> {
    runtime_with_everything(
        config,
        directory,
        storage_pressure,
        metrics,
        Arc::new(RecordingAnnouncer::default()),
    )
    .await
}

async fn runtime_with_everything(
    config: AdapterConfig,
    directory: &TempDir,
    storage_pressure: Option<StoragePressureMonitor>,
    metrics: Arc<dyn edgecommons::metrics::MetricService>,
    announcer: Arc<RecordingAnnouncer>,
) -> Arc<CameraRuntime> {
    let state = directory.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let storage = StorageRoot::open(&config.global.output).unwrap();
    let catalog = Catalog::open(CatalogOptions::new(state)).await.unwrap();
    let admission = AdmissionController::new(
        &config.global.limits,
        &config.global.output,
        Arc::new(FilesystemSpaceProbe::default()),
    )
    .unwrap();
    let registry = Arc::new(CameraRegistry::new(&config).unwrap());
    let waiters = Arc::new(RuntimeJobHooks::new(catalog.clone()));
    let mut engines = BTreeMap::new();
    for camera in &config.instances {
        engines.insert(
            camera.id.clone(),
            JobEngine::new(
                catalog.clone(),
                admission.clone(),
                storage.clone(),
                Arc::clone(&announcer) as Arc<dyn crate::jobs::TerminalAnnouncer>,
                Arc::clone(&waiters) as Arc<dyn JobHooks>,
                crate::thumbnail::ThumbnailPolicy::for_transport(edgecommons::platform::Transport::Mqtt),
            )
            .with_acceptance_hook(Arc::clone(&waiters) as Arc<dyn AcceptanceHook>),
        );
    }
    let scheduler = crate::dispatch::CaptureScheduler::new(&config.global.limits).unwrap();
    let runtime = Arc::new(CameraRuntime {
        config: RwLock::new(Arc::new(config)),
        // The harness is a HOST/standalone runtime, which is an MQTT transport.
        thumbnail_policy: crate::thumbnail::ThumbnailPolicy::for_transport(edgecommons::platform::Transport::Mqtt),
        backend_context: BackendRuntimeContext::new(
            None,
            &crate::config::LimitsConfig::default(),
        ),
        catalog,
        admission,
        storage,
        registry,
        cameras: Arc::new(RwLock::new(new_slots(engines, BTreeMap::new()))),
        component_events: None,
        storage_pressure,
        storage_alarm: Arc::new(Mutex::new(StorageAlarmState::default())),
        messaging_alarm: Arc::new(Mutex::new(MessagingAlarmState::default())),
        readiness: RuntimeReadiness::noop(),
        metrics: Arc::new(crate::observability::CaptureMetrics::new(metrics)),
        health: Arc::new(crate::observability::FleetHealth::default()),
        scheduler_cancellations: RwLock::new(HashMap::new()),
        discovery_cancellation: RwLock::new(None),
        discovery_cache: Mutex::new(DiscoveryCache::default()),
        scheduler,
        cancellation: CancellationToken::new(),
        tasks: Mutex::new(Vec::new()),
        connect_gate: Arc::new(Semaphore::new(1)),
        waiters: Arc::clone(&waiters),
        cursors: CursorStore::default(),
        reload_gate: tokio::sync::Mutex::new(()),
        reloading: AtomicBool::new(false),
        self_reference: OnceLock::new(),
    });
    let _ = runtime.self_reference.set(Arc::downgrade(&runtime));
    waiters.attach_runtime(Arc::downgrade(&runtime));
    // The fleet queue needs its consumer. Without it a capture is durably accepted, queued,
    // and then waits forever -- which is exactly what these tests would have shown.
    runtime
        .start_capture_scheduler()
        .expect("the capture scheduler must start");
    runtime
}

struct LowSpaceProbe;

#[async_trait::async_trait]
impl crate::admission::DiskSpaceProbe for LowSpaceProbe {
    async fn space(&self, _path: &std::path::Path) -> Result<crate::admission::DiskSpace> {
        Ok(crate::admission::DiskSpace {
            available_bytes: 0,
            total_bytes: 1_000,
        })
    }
}

struct ToggleSpaceProbe {
    pressured: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait::async_trait]
impl crate::admission::DiskSpaceProbe for ToggleSpaceProbe {
    async fn space(&self, _path: &std::path::Path) -> Result<crate::admission::DiskSpace> {
        let available_bytes = if self.pressured.load(std::sync::atomic::Ordering::Acquire) {
            0
        } else {
            20_000_000_000
        };
        Ok(crate::admission::DiskSpace {
            available_bytes,
            total_bytes: 40_000_000_000,
        })
    }
}

#[tokio::test]
async fn storage_pressure_rejects_fresh_capture_before_a_ledger_or_job_is_committed() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a"], false);
    let monitor = StoragePressureMonitor::new(
        configuration.global.output.root_directory.clone(),
        directory.path().join("state"),
        &configuration.global.output,
        Arc::new(LowSpaceProbe),
    );
    let runtime =
        runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;

    let error = runtime
        .submit_capture(
            "camera-a".to_string(),
            "storage-pressure-fresh".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "storage-pressure-test".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), crate::ErrorCode::StoragePressure);
    assert!(
        runtime
            .catalog
            .job_by_ledger(
                crate::catalog::LedgerKey::new(
                    "camera-a",
                    "sb/capture-submit",
                    "storage-pressure-fresh",
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .is_none(),
        "storage pressure must reject before creating an idempotency ledger or job"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn storage_pressure_rejects_fresh_groups_but_exact_group_retries_remain_replayable() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let pressured = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let monitor = StoragePressureMonitor::new(
        configuration.global.output.root_directory.clone(),
        directory.path().join("state"),
        &configuration.global.output,
        Arc::new(ToggleSpaceProbe {
            pressured: Arc::clone(&pressured),
        }),
    );
    let runtime =
        runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;
    let request = GroupCaptureRequest {
        request_id: "storage-group-existing".to_string(),
        instances: vec!["camera-a".to_string(), "camera-b".to_string()],
        capture_profile: None,
        profile_overrides: BTreeMap::new(),
        timeout_ms: None,
        metadata: serde_json::Map::new(),
    };
    let accepted = runtime
        .submit_group(
            request.clone(),
            "storage-group-original-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap();
    pressured.store(true, std::sync::atomic::Ordering::Release);

    let replay = runtime
        .submit_group(
            request.clone(),
            "storage-group-retry-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap();
    assert_eq!(replay.group_id, accepted.group_id);
    assert_eq!(
        replay.origin_correlation_id.as_deref(),
        Some("storage-group-original-correlation"),
        "pressure must not erase the durable result of an exact group retry"
    );

    let mut changed = request.clone();
    changed.timeout_ms = Some(30_000);
    assert_eq!(
        runtime
            .submit_group(
                changed,
                "storage-group-conflict-correlation".to_string(),
                crate::admission::CapturePriority::Submitted,
                None,
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::IdempotencyConflict
    );
    let fresh = GroupCaptureRequest {
        request_id: "storage-group-fresh".to_string(),
        ..request
    };
    assert_eq!(
        runtime
            .submit_group(
                fresh,
                "storage-group-fresh-correlation".to_string(),
                crate::admission::CapturePriority::Submitted,
                None,
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::StoragePressure
    );
    assert!(
        runtime
            .catalog
            .group_by_ledger(
                crate::catalog::LedgerKey::new(
                    "main",
                    "sb/capture-group",
                    "storage-group-fresh",
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .is_none(),
        "storage pressure must reject a fresh group before durable acceptance"
    );
    runtime.shutdown().await;
}

async fn spawn_recording_mqtt_broker() -> (u16, RecordedMqttPublishes) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let publishes = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&publishes);
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let recorder = Arc::clone(&recorder);
                    tokio::spawn(async move {
                        record_mqtt_connection(stream, recorder).await;
                    });
                }
                Err(_) => return,
            }
        }
    });
    (port, publishes)
}

async fn record_mqtt_connection(mut stream: TcpStream, publishes: RecordedMqttPublishes) {
    while let Some((header, payload)) = read_mqtt_packet(&mut stream).await {
        match header >> 4 {
            1 => {
                if stream.write_all(&[0x20, 0x02, 0x00, 0x00]).await.is_err() {
                    return;
                }
            }
            8 => {
                if payload.len() < 2
                    || stream
                        .write_all(&[0x90, 0x03, payload[0], payload[1], 0x00])
                        .await
                        .is_err()
                {
                    return;
                }
            }
            3 => {
                let Some((topic, bytes, packet_id)) =
                    mqtt_publish_payload(header, &payload)
                else {
                    return;
                };
                publishes.lock().unwrap().push((topic, bytes));
                if let Some(packet_id) = packet_id {
                    if stream
                        .write_all(&[0x40, 0x02, packet_id[0], packet_id[1]])
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            12 => {
                if stream.write_all(&[0xD0, 0x00]).await.is_err() {
                    return;
                }
            }
            14 => return,
            _ => {}
        }
    }
}

async fn read_mqtt_packet(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut header = [0_u8; 1];
    stream.read_exact(&mut header).await.ok()?;
    let mut remaining = 0_usize;
    let mut multiplier = 1_usize;
    loop {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await.ok()?;
        remaining = remaining.checked_add(usize::from(byte[0] & 0x7f) * multiplier)?;
        if byte[0] & 0x80 == 0 {
            break;
        }
        multiplier = multiplier.checked_mul(128)?;
        if multiplier > 128_usize.pow(4) {
            return None;
        }
    }
    let mut payload = vec![0_u8; remaining];
    stream.read_exact(&mut payload).await.ok()?;
    Some((header[0], payload))
}

fn mqtt_publish_payload(
    header: u8,
    payload: &[u8],
) -> Option<(String, Vec<u8>, Option<[u8; 2]>)> {
    let topic_length = usize::from(u16::from_be_bytes(payload.get(..2)?.try_into().ok()?));
    let topic_end = 2_usize.checked_add(topic_length)?;
    let topic = std::str::from_utf8(payload.get(2..topic_end)?)
        .ok()?
        .to_string();
    let qos = (header >> 1) & 0b11;
    let (body_start, packet_id) = if qos == 0 {
        (topic_end, None)
    } else {
        let packet_id = payload
            .get(topic_end..topic_end.checked_add(2)?)?
            .try_into()
            .ok()?;
        (topic_end.checked_add(2)?, Some(packet_id))
    };
    Some((topic, payload.get(body_start..)?.to_vec(), packet_id))
}

#[cfg(all(feature = "standalone", feature = "onvif"))]
async fn facade_core(directory: &TempDir, port: u16) -> Arc<edgecommons::EdgeCommons> {
    facade_core_with_router(directory, port, None).await
}

/// Builds the loopback core fixture, optionally configuring the adapter router before
/// the core command inbox can subscribe. This keeps the startup race test faithful to
/// the binary's construction order rather than registering test handlers after startup.
async fn facade_core_with_router(
    directory: &TempDir,
    port: u16,
    router: Option<Arc<RuntimeCommandRouter>>,
) -> Arc<edgecommons::EdgeCommons> {
    let instances = ["camera-a", "camera-b", "camera-c"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    facade_core_with_router_instances(directory, port, router, &instances).await
}

/// Builds the loopback Core fixture with the same instance roster used by a runtime test.
///
/// Core permits dynamic facade handles, but capacity validation must not use that escape
/// hatch: the configured Core roster and the adapter roster need to agree so facade
/// construction, command registration, and instance identity all follow the production
/// path.
async fn facade_core_with_router_instances(
    directory: &TempDir,
    port: u16,
    router: Option<Arc<RuntimeCommandRouter>>,
    instances: &[String],
) -> Arc<edgecommons::EdgeCommons> {
    let component_config = directory.path().join("facade-core-config.json");
    let messaging_config = directory.path().join("facade-core-messaging.json");
    std::fs::write(
        &component_config,
        serde_json::to_vec(&json!({
            "component": {
                "instances": instances.iter().map(|id| json!({ "id": id })).collect::<Vec<_>>()
            }
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(
        &messaging_config,
        serde_json::to_vec(&json!({
            "messaging": {
                "local": {
                    "host": "127.0.0.1",
                    "port": port,
                    "clientId": format!("camera-adapter-runtime-events-{}", uuid::Uuid::now_v7())
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let builder = EdgeCommonsBuilder::new(COMPONENT_NAME)
        .args(vec![
            "camera-adapter-runtime-events".into(),
            "--platform".into(),
            "HOST".into(),
            "--transport".into(),
            "MQTT".into(),
            messaging_config.into_os_string(),
            "--config".into(),
            "FILE".into(),
            component_config.into_os_string(),
            "--thing".into(),
            "camera-adapter-runtime-events".into(),
        ])
        .initial_ready(false);
    let builder = match router {
        Some(router) => builder.configure_commands(move |inbox| router.register(inbox)),
        None => builder,
    };
    Arc::new(
        builder
            .build()
            .await
            .expect("in-process MQTT fixture must build EdgeCommons facades"),
    )
}

async fn command_deferred_registry(
    directory: &TempDir,
    port: u16,
) -> (Arc<edgecommons::EdgeCommons>, DeferredReplyRegistry) {
    let component_config = directory.path().join("command-e2e-config.json");
    let messaging_config = directory.path().join("command-e2e-messaging.json");
    std::fs::write(&component_config, br#"{"component":{}}"#).unwrap();
    let client_id = format!("camera-adapter-command-e2e-{}", uuid::Uuid::now_v7());
    std::fs::write(
        &messaging_config,
        serde_json::to_vec(&json!({
            "messaging": {
                "local": {
                    "host": "127.0.0.1",
                    "port": port,
                    "clientId": client_id
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = vec![
        "camera-adapter-command-e2e".into(),
        "--platform".into(),
        "HOST".into(),
        "--transport".into(),
        "MQTT".into(),
        messaging_config.into_os_string(),
        "--config".into(),
        "FILE".into(),
        component_config.into_os_string(),
        "--thing".into(),
        "camera-adapter-command-e2e".into(),
    ];
    let app = Arc::new(
        EdgeCommonsBuilder::new(COMPONENT_NAME)
            .args(args)
            .initial_ready(false)
            .build()
            .await
            .expect("loopback EMQX command-inbox fixture must start"),
    );
    let deferred = app
        .commands()
        .expect("MQTT transport creates the command inbox")
        .deferred_replies();
    (app, deferred)
}

fn command_message(verb: &str, suffix: &str, body: serde_json::Value) -> Message {
    MessageBuilder::new(verb, "1.0")
        .correlation_id(format!("command-e2e-correlation-{suffix}"))
        .reply_to("camera-adapter-command-e2e/replies")
        .structured_payload(body)
        .build()
}

fn immediate_success(outcome: CommandOutcome) -> serde_json::Value {
    match outcome {
        CommandOutcome::ImmediateSuccess(Some(value)) => value,
        other => panic!("expected immediate command success, got {other:?}"),
    }
}

fn queued_job(config: &AdapterConfig, capture_id: &str) -> crate::catalog::NewJob {
    let camera = config
        .instances
        .iter()
        .find(|camera| camera.id == "camera-b")
        .unwrap();
    let profile = camera.capture_profiles.get("main").unwrap().clone();
    let profile = crate::jobs::JobProfileSnapshot {
        name: "main".to_string(),
        capture: profile,
        offline_policy: crate::config::OfflinePolicy::WaitUntilDeadline,
        maximum_frame_bytes: config.global.limits.max_frame_bytes_per_camera,
        capture_mode: crate::model::CaptureMode::Simulated,
        capture_interlock: camera.ptz.capture_interlock,
        settle_ms: camera.ptz.settle_ms,
    };
    let now = chrono::Utc::now().timestamp_millis();
    let trigger = crate::messages::CaptureTrigger::Command {
        request_id: "reload-queued".to_string(),
    };
    let canonical = json!({ "requestId": "reload-queued", "metadata": {} });
    crate::catalog::NewJob {
        capture_id: capture_id.to_string(),
        instance: "camera-b".to_string(),
        ledger_key: Some(
            crate::catalog::LedgerKey::new("camera-b", "sb/capture", "reload-queued")
                .unwrap(),
        ),
        request_hash: crate::idempotency::canonical_request_hash(&canonical, false)
            .unwrap(),
        canonical_request: canonical,
        effective_profile: serde_json::to_value(profile).unwrap(),
        deadlines: crate::catalog::JobDeadlines {
            terminal_at_ms: now + 60_000,
            queue_at_ms: None,
            capture_at_ms: now + 30_000,
            encode_at_ms: now + 30_000,
            persist_at_ms: now + 30_000,
        },
        trigger: serde_json::to_value(trigger).unwrap(),
        origin_correlation_id: Some("correlation".to_string()),
        intended_output: json!({ "relativePath": "camera-b/cap.jpg", "backend": "sim" }),
        accepted_at_ms: now,
        group_id: None,
    }
}

/// Waits for the fleet queue to let go of descriptors whose captures have ended.
///
/// A cancelled or expired descriptor is removed by the watcher task `try_enqueue` spawns for
/// it -- deliberately, so that work is released even when no consumer is polling the queue and
/// its camera is offline. That removal is therefore ASYNCHRONOUS, and asserting on the queue
/// the instant a cancel returns is a race with a task that has not been scheduled yet. It is a
/// race the assertion usually wins on a fast machine and lost, once, on a two-core CI runner.
async fn wait_for_queue_depth(runtime: &CameraRuntime, expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let pending = runtime.scheduler.pending();
        if pending == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the queue still holds {pending} descriptors; expected {expected}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_online(runtime: &CameraRuntime, instance: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if runtime
            .registry
            .snapshot(instance)
            .is_ok_and(|snapshot| snapshot.state == CameraConnectionState::Online)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "simulator supervisor did not reach ONLINE"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_terminal(
    runtime: &CameraRuntime,
    capture_id: &str,
) -> crate::catalog::JobRecord {
    wait_for_terminal_within(runtime, capture_id, Duration::from_secs(5)).await
}

async fn wait_for_terminal_within(
    runtime: &CameraRuntime,
    capture_id: &str,
    timeout: Duration,
) -> crate::catalog::JobRecord {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(record) = runtime.catalog.job(capture_id).await.unwrap() {
            if record.state.is_terminal() {
                return record;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "simulator capture did not reach a terminal state"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_group_terminal(
    runtime: &CameraRuntime,
    group_id: &str,
) -> crate::catalog::GroupRecord {
    wait_for_group_terminal_within(runtime, group_id, Duration::from_secs(5)).await
}

async fn wait_for_group_terminal_within(
    runtime: &CameraRuntime,
    group_id: &str,
    timeout: Duration,
) -> crate::catalog::GroupRecord {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(record) = runtime.catalog.group(group_id).await.unwrap() {
            if record.state.is_terminal() {
                return record;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "simulator capture group did not reach a terminal state"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_recorded_reply(
    publishes: &RecordedMqttPublishes,
    first_index: usize,
) -> Message {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let reply = publishes
            .lock()
            .unwrap()
            .iter()
            .skip(first_index)
            .find(|(topic, _)| topic == "camera-adapter-command-e2e/replies")
            .map(|(_, bytes)| Message::from_slice(bytes).unwrap());
        if let Some(reply) = reply {
            return reply;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "deferred command did not publish a reply to its guarded reply topic"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn simulator_runtime_exercises_capture_status_ptz_and_reconnect_idempotency() {
    let directory = TempDir::new().unwrap();
    let mut config = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut config.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    sim.ptz.supported = true;
    config.instances[0].ptz.enabled = true;
    let runtime = runtime(config, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "runtime-e2e-capture".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "runtime-e2e-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected newly accepted capture, got {other:?}"),
    };
    let terminal = wait_for_terminal(&runtime, &capture_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    assert!(terminal.terminal_result.is_some());

    let status = runtime
        .jobs_status_page(&CaptureStatusRequest {
            capture_id: None,
            capture_group_id: None,
            instance: Some("camera-a".to_string()),
            request_id: None,
            states: vec![crate::model::JobState::Succeeded],
            limit: 1,
            cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(status["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(status["jobs"][0]["captureId"], capture_id);

    let ptz: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "relative",
        "instance": "camera-a",
        "requestId": "runtime-e2e-ptz",
        "translation": { "pan": 0.1, "tilt": 0.0, "zoom": 0.0 }
    }))
    .unwrap();
    let first_ptz = runtime.perform_ptz(ptz.clone()).await.unwrap();
    assert_eq!(first_ptz["state"], "COMMANDED");
    assert_eq!(runtime.perform_ptz(ptz).await.unwrap(), first_ptz);

    let first_reconnect = runtime
        .reconnect(ReconnectRequest {
            instance: Some("camera-a".to_string()),
            request_id: "runtime-e2e-reconnect".to_string(),
            reason: Some("test reconnect".to_string()),
        })
        .await
        .unwrap();
    let duplicate_reconnect = runtime
        .reconnect(ReconnectRequest {
            instance: Some("camera-a".to_string()),
            request_id: "runtime-e2e-reconnect".to_string(),
            reason: Some("test reconnect".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(first_reconnect, duplicate_reconnect);
    wait_for_online(&runtime, "camera-a").await;
    runtime.shutdown().await;
}

/// A capture whose durable state has moved on is refused BEFORE it can reach a camera.
///
/// This test used to prove the opposite half of the contract: it corrupted a queued capture's
/// row (QUEUED -> ACQUIRING, simulating a crash/race) and asserted that the engine's fatal
/// error propagated all the way out through the actor, retiring it and forcing a reconnect.
/// That was the best the component could do when a per-camera cache pushed descriptors at an
/// actor with nothing in between.
///
/// The fleet scheduler rebases a capture onto its admission before handing it over, and only
/// a QUEUED row may be rebased -- so a capture whose durable state has moved on never reaches
/// the camera at all. The camera keeps serving. Tearing down a healthy session because a
/// DURABLE row was inconsistent was never a good trade, and now it is not made.
///
/// The supervisor-recovery path it used to cover is not lost: an actor that dies still retires
/// and advances its generation, which
/// `cancelled_actor_runs_its_safety_stop_and_session_close_within_the_grace` and the panic
/// isolation path both exercise.
#[tokio::test]
async fn a_capture_whose_durable_state_moved_on_never_reaches_the_camera() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    // Keep BACKOFF observable rather than relying on a scheduling race, while preserving
    // the same bounded reconnect policy used in production.
    configuration.global.timeouts.reconnect_backoff_min_ms = 200;
    configuration.global.timeouts.reconnect_backoff_max_ms = 200;
    let runtime = runtime(configuration, &directory).await;

    // Accept and dispatch before a session exists, then simulate the durable invariant a
    // crash/race could expose: the descriptor still says QUEUED but its record advanced.
    // `CameraActor` must surface that fatal engine error to the supervisor, which must
    // retire the actor, publish BACKOFF, and create a fresh generation rather than leave
    // the persistent dispatcher wedged behind the failed session.
    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "fatal-actor-recovery".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "fatal-actor-recovery-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected newly accepted capture, got {other:?}"),
    };
    assert!(matches!(
        runtime
            .catalog
            .compare_and_set_state(
                &capture_id,
                crate::model::JobState::Queued,
                crate::model::JobState::Acquiring,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .unwrap(),
        crate::catalog::StateCasOutcome::Changed(_)
    ));

    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    // The camera comes up and STAYS up. The scheduler refuses to rebase a row that is no
    // longer QUEUED, so the inconsistent capture is never handed to it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        let snapshot = runtime.registry.snapshot("camera-a").unwrap();
        assert_ne!(
            snapshot.state,
            CameraConnectionState::Backoff,
            "a camera must not be torn down because a DURABLE row was inconsistent -- the                      session was healthy and the capture was the thing that was wrong"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        runtime.registry.snapshot("camera-a").unwrap().state,
        CameraConnectionState::Online,
        "the camera keeps serving"
    );

    // And the camera still works: a well-formed capture succeeds on the same session.
    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "healthy-after-the-bad-row".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "healthy-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let healthy_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected a newly accepted capture, got {other:?}"),
    };
    let terminal = wait_for_terminal(&runtime, &healthy_id).await;
    assert_eq!(
        terminal.state,
        crate::model::JobState::Succeeded,
        "the session was never broken, so it must still be capturing"
    );

    runtime.shutdown().await;
}

/// A requeued capture is not just re-listed -- it actually runs.
///
/// Putting the row back on the queue is half a promise. The other half is that a camera
/// picks it up and it reaches a terminal state of its own, which is the whole point of
/// recovering it rather than interrupting it.
#[tokio::test]
async fn a_capture_requeued_after_a_restart_actually_runs() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;

    let pending = queued_job(&configuration, "cap-requeue-runs");
    runtime.catalog.accept_job(pending).await.unwrap();
    runtime
        .catalog
        .queue_job("cap-requeue-runs", chrono::Utc::now().timestamp_millis())
        .await
        .unwrap();

    runtime.recover_install_owned().await.unwrap();
    assert_eq!(runtime.scheduler.pending(), 1);

    runtime
        .start_supervisor("camera-b".to_string(), runtime.engine("camera-b").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-b").await;

    let record =
        wait_for_terminal_within(&runtime, "cap-requeue-runs", Duration::from_secs(20))
            .await;
    assert_eq!(
        record.state,
        crate::model::JobState::Succeeded,
        "the capture the previous run could not get to must complete on this one"
    );
    runtime.shutdown().await;
}

/// A capture that cannot be put back is retired -- never left QUEUED with nothing to drive it.
///
/// Recovery can run out of queue before it runs out of captures. The tempting thing is to log
/// and move on, which leaves a durable `QUEUED` row that no scheduler holds and no deadline
/// task watches -- the exact stranded-capture shape this whole branch exists to prevent. So a
/// capture that cannot be requeued falls through to being interrupted instead.
#[tokio::test]
async fn a_capture_that_cannot_be_requeued_is_interrupted_rather_than_stranded() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-b"], false);
    // Room for exactly one waiting capture, fleet-wide.
    configuration.global.limits.max_pending_captures = 1;
    configuration.global.limits.max_queued_captures_per_camera = 1;
    let runtime = runtime(configuration.clone(), &directory).await;

    for (capture, request) in [
        ("cap-refill-1", "refill-request-1"),
        ("cap-refill-2", "refill-request-2"),
    ] {
        // The shared fixture pins one ledger key; two captures need two.
        let mut pending = queued_job(&configuration, capture);
        pending.ledger_key = Some(
            crate::catalog::LedgerKey::new("camera-b", "sb/capture", request).unwrap(),
        );
        pending.trigger = serde_json::to_value(crate::messages::CaptureTrigger::Command {
            request_id: request.to_string(),
        })
        .unwrap();
        runtime.catalog.accept_job(pending).await.unwrap();
        runtime
            .catalog
            .queue_job(capture, chrono::Utc::now().timestamp_millis())
            .await
            .unwrap();
    }

    runtime.recover_install_owned().await.unwrap();

    let states = [
        runtime
            .catalog
            .job("cap-refill-1")
            .await
            .unwrap()
            .unwrap()
            .state,
        runtime
            .catalog
            .job("cap-refill-2")
            .await
            .unwrap()
            .unwrap()
            .state,
    ];
    tracing::error!(
        "DIAG states={:?} pending={}",
        states,
        runtime.scheduler.pending()
    );
    assert_eq!(
        runtime.scheduler.pending(),
        1,
        "the queue holds exactly what it has room for"
    );
    assert!(
        states.contains(&crate::model::JobState::Queued),
        "the capture that fit must be requeued: {states:?}"
    );
    assert!(
        states.contains(&crate::model::JobState::Interrupted),
        "the capture that did not fit must be retired, not left QUEUED with nothing to drive \
         it: {states:?}"
    );
    runtime.shutdown().await;
}

/// A group left waiting by the previous run comes back whole -- and runs.
///
/// The durable rows are built here and no descriptor is ever registered in memory, which is
/// exactly the state a restart leaves behind: the catalog remembers the group, the process
/// that accepted it does not.
///
/// The group SIZE is what earns this test its place. A member's runtime snapshot is rejected
/// outright unless it carries the real size of the group it belongs to, so a recovery that
/// guessed -- or that reused the interrupt path's placeholder of one -- would fail to requeue
/// every group member it ever touched, and quietly interrupt them all instead.
#[tokio::test]
async fn a_group_left_waiting_by_the_previous_run_is_requeued_whole() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;

    let group_id = "grp_restart";
    let canonical = json!({ "requestId": "group-across-restart", "metadata": {} });
    let request_hash =
        crate::idempotency::canonical_request_hash(&canonical, true).unwrap();
    let members: Vec<crate::catalog::NewJob> = ["camera-a", "camera-b"]
        .iter()
        .enumerate()
        .map(|(index, instance)| {
            let mut member =
                queued_job(&configuration, &format!("cap-group-restart-{index}"));
            member.instance = (*instance).to_string();
            member.ledger_key = None;
            member.group_id = Some(group_id.to_string());
            member.trigger =
                serde_json::to_value(crate::messages::CaptureTrigger::GroupCommand {
                    request_id: "group-across-restart".to_string(),
                    capture_group_id: group_id.to_string(),
                })
                .unwrap();
            member.intended_output = json!({
                "relativePath": format!("{instance}/cap-{index}.jpg"),
                "backend": "sim"
            });
            member
        })
        .collect();

    runtime
        .catalog
        .accept_group(crate::catalog::NewGroup {
            group_id: group_id.to_string(),
            ledger_key: crate::catalog::LedgerKey::new(
                "main",
                "sb/capture-group",
                "group-across-restart",
            )
            .unwrap(),
            canonical_request: canonical,
            request_hash,
            origin_correlation_id: Some("restart-correlation".to_string()),
            accepted_at_ms: chrono::Utc::now().timestamp_millis(),
            members,
        })
        .await
        .unwrap();
    runtime
        .catalog
        .queue_group(group_id.to_string(), chrono::Utc::now().timestamp_millis())
        .await
        .unwrap();
    assert_eq!(
        runtime.scheduler.pending(),
        0,
        "the durable group exists; nothing is holding it in memory"
    );

    runtime.recover_install_owned().await.unwrap();
    assert_eq!(
        runtime.scheduler.pending(),
        2,
        "both members of the waiting group must be requeued -- a wrong group size would \
         interrupt them all instead"
    );

    for camera in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
            .unwrap();
        wait_for_online(&runtime, camera).await;
    }

    let terminal =
        wait_for_group_terminal_within(&runtime, group_id, Duration::from_secs(20)).await;
    assert_eq!(terminal.members.len(), 2);
    assert!(
        terminal
            .members
            .iter()
            .all(|member| member.state == crate::model::JobState::Succeeded),
        "a group interrupted by a restart must complete on the next run: {:?}",
        terminal
            .members
            .iter()
            .map(|member| member.state)
            .collect::<Vec<_>>()
    );
    runtime.shutdown().await;
}

/// A recovered capture keeps the place it was originally given.
///
/// Priority is read back from the durable record, not reset: a direct capture that a caller
/// was waiting on does not get demoted behind a batch of scheduled work for the crime of
/// having survived a restart.
#[tokio::test]
async fn a_recovered_capture_keeps_its_original_priority() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;
    runtime
        .catalog
        .accept_job(queued_job(&configuration, "cap-priority"))
        .await
        .unwrap();
    let mut record = runtime.catalog.job("cap-priority").await.unwrap().unwrap();

    record.verb = Some("sb/capture".to_string());
    assert_eq!(
        recovered_priority(&record),
        crate::admission::CapturePriority::Direct
    );
    record.verb = Some("sb/capture-submit".to_string());
    assert_eq!(
        recovered_priority(&record),
        crate::admission::CapturePriority::Submitted
    );
    record.verb = Some("sb/capture-group".to_string());
    assert_eq!(
        recovered_priority(&record),
        crate::admission::CapturePriority::Direct
    );
    record.verb = None;
    assert_eq!(
        recovered_priority(&record),
        crate::admission::CapturePriority::Scheduled,
        "no command ledger means a schedule fired it"
    );
    runtime.shutdown().await;
}

/// A group member requeued without its group is refused, and so is the reverse.
///
/// This is the guard that makes the group size load-bearing rather than cosmetic: a member
/// whose snapshot does not agree with its durable group row is rejected outright. It is the
/// reason recovery has to look the size up instead of assuming one, and the reason a wrong
/// answer would have interrupted every group member instead of quietly capturing a half-group.
#[tokio::test]
async fn a_requeued_snapshot_must_agree_with_the_durable_group() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;

    // A group member, durably.
    let group_id = "grp_snapshot";
    let canonical = json!({ "requestId": "snapshot-group", "metadata": {} });
    let mut member = queued_job(&configuration, "cap-snapshot-member");
    member.ledger_key = None;
    member.group_id = Some(group_id.to_string());
    member.trigger = serde_json::to_value(crate::messages::CaptureTrigger::GroupCommand {
        request_id: "snapshot-group".to_string(),
        capture_group_id: group_id.to_string(),
    })
    .unwrap();
    let mut second = member.clone();
    second.capture_id = "cap-snapshot-member-2".to_string();
    second.instance = "camera-a".to_string();
    second.intended_output =
        json!({ "relativePath": "camera-a/two.jpg", "backend": "sim" });

    runtime
        .catalog
        .accept_group(crate::catalog::NewGroup {
            group_id: group_id.to_string(),
            ledger_key: crate::catalog::LedgerKey::new(
                "main",
                "sb/capture-group",
                "snapshot-group",
            )
            .unwrap(),
            canonical_request: canonical.clone(),
            request_hash: crate::idempotency::canonical_request_hash(&canonical, true)
                .unwrap(),
            origin_correlation_id: Some("snapshot-correlation".to_string()),
            accepted_at_ms: chrono::Utc::now().timestamp_millis(),
            members: vec![member, second],
        })
        .await
        .unwrap();
    runtime
        .catalog
        .queue_group(group_id.to_string(), chrono::Utc::now().timestamp_millis())
        .await
        .unwrap();

    let record = runtime
        .catalog
        .job("cap-snapshot-member")
        .await
        .unwrap()
        .unwrap();
    let engine = runtime.engine("camera-b").unwrap();

    // A group member with no group size is not a group member.
    let error = engine
        .requeue_recovered(
            &runtime.scheduler,
            record.clone(),
            None,
            None,
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect_err("a group member must carry the size of the group it belongs to");
    assert!(
        error
            .to_string()
            .contains("group runtime snapshot is inconsistent"),
        "the group snapshot guard must be what rejects it, not something upstream: {error}"
    );

    // And a size that does not match a group of two is equally wrong.
    let error = engine
        .requeue_recovered(
            &runtime.scheduler,
            record,
            None,
            Some(1),
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect_err("a group of one is not a group");
    assert!(
        error
            .to_string()
            .contains("group runtime snapshot is inconsistent"),
        "{error}"
    );
    assert_eq!(runtime.scheduler.pending(), 0);
    runtime.shutdown().await;
}

/// The requeue entry point refuses anything that is not simply waiting.
///
/// The state check is not decoration: a capture past `QUEUED` has already reached for a
/// camera, and putting it back on the queue would replay it.
#[tokio::test]
async fn requeue_refuses_a_capture_that_is_not_merely_queued() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;

    let pending = queued_job(&configuration, "cap-not-queued");
    runtime.catalog.accept_job(pending).await.unwrap();
    let accepted = runtime
        .catalog
        .job("cap-not-queued")
        .await
        .unwrap()
        .unwrap();

    let error = runtime
        .engine("camera-b")
        .unwrap()
        .requeue_recovered(
            &runtime.scheduler,
            accepted,
            None,
            None,
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect_err("an ACCEPTED capture never reached the queue and cannot return to it");
    assert!(matches!(error, crate::CameraError::Catalog(_)));
    assert_eq!(runtime.scheduler.pending(), 0);
    runtime.shutdown().await;
}

/// An ACCEPTED capture is retired, not resumed -- DESIGN §17.1(2).
///
/// Acceptance never completed its durable queue transition, so the capture was never really
/// waiting: it was mid-commit. Only `QUEUED` work comes back.
#[tokio::test]
async fn an_accepted_capture_is_interrupted_rather_than_requeued() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;

    // Accepted, and never queued.
    let pending = queued_job(&configuration, "cap-accepted-only");
    runtime.catalog.accept_job(pending).await.unwrap();

    runtime.recover_install_owned().await.unwrap();

    let record = runtime
        .catalog
        .job("cap-accepted-only")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.state,
        crate::model::JobState::Interrupted,
        "acceptance that never reached the queue is not queued work"
    );
    assert_eq!(runtime.scheduler.pending(), 0);
    runtime.shutdown().await;
}

/// A capture whose queue deadline expired while the process was down is retired -- §17.1(3).
///
/// `queueExpiryMs` is the capture's own statement of how long it was prepared to wait. A
/// restart does not get to extend it.
#[tokio::test]
async fn a_capture_whose_queue_deadline_expired_is_not_requeued() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;

    let mut waited_too_long = queued_job(&configuration, "cap-queue-expired");
    let now = chrono::Utc::now().timestamp_millis();
    // The terminal clock is still ahead; only the queue bound has run out, so this test
    // fails if the queue deadline is not being consulted.
    waited_too_long.deadlines.queue_at_ms = Some(now - 1_000);
    waited_too_long.deadlines.terminal_at_ms = now + 600_000;
    runtime.catalog.accept_job(waited_too_long).await.unwrap();
    runtime
        .catalog
        .queue_job("cap-queue-expired", now)
        .await
        .unwrap();

    runtime.recover_install_owned().await.unwrap();

    let record = runtime
        .catalog
        .job("cap-queue-expired")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, crate::model::JobState::Interrupted);
    assert_eq!(
        runtime.scheduler.pending(),
        0,
        "a capture that already waited longer than it agreed to is not requeued"
    );
    runtime.shutdown().await;
}

/// `queuedRecoveryPolicy: interrupt` still interrupts.
///
/// The knob was parsed, validated, and read by nothing at all -- the component always
/// interrupted, whatever the operator asked for. Both branches are now real, and this pins
/// the one that is no longer the default.
#[tokio::test]
async fn the_interrupt_recovery_policy_retires_waiting_work_instead() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-b"], false);
    configuration.global.state.queued_recovery_policy =
        crate::config::QueuedRecoveryPolicy::Interrupt;
    let runtime = runtime(configuration.clone(), &directory).await;

    let pending = queued_job(&configuration, "cap-interrupt-policy");
    runtime.catalog.accept_job(pending).await.unwrap();
    runtime
        .catalog
        .queue_job(
            "cap-interrupt-policy",
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        .unwrap();

    runtime.recover_install_owned().await.unwrap();

    let record = runtime
        .catalog
        .job("cap-interrupt-policy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, crate::model::JobState::Interrupted);
    assert_eq!(
        record.error_code.as_deref(),
        Some(crate::ErrorCode::ProcessInterrupted.as_str())
    );
    assert_eq!(
        runtime.scheduler.pending(),
        0,
        "an interrupted capture must not also be queued"
    );
    runtime.shutdown().await;
}

/// A capture whose deadline ran out while the process was down is retired, not resurrected.
///
/// Recovering waiting work must not mean running an image request that expired hours ago.
/// The capture was not interrupted -- it EXPIRED -- and the honest terminal for it is a
/// terminal, not a place in the queue.
#[tokio::test]
async fn a_capture_whose_deadline_passed_while_the_process_was_down_is_not_resurrected() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;

    let mut expired = queued_job(&configuration, "cap-expired-while-down");
    let long_ago = chrono::Utc::now().timestamp_millis() - 3_600_000;
    expired.accepted_at_ms = long_ago;
    expired.deadlines = crate::catalog::JobDeadlines {
        terminal_at_ms: long_ago + 1_000,
        queue_at_ms: None,
        capture_at_ms: long_ago + 500,
        encode_at_ms: long_ago + 500,
        persist_at_ms: long_ago + 500,
    };
    runtime.catalog.accept_job(expired).await.unwrap();
    runtime
        .catalog
        .queue_job(
            "cap-expired-while-down",
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        .unwrap();

    runtime.recover_install_owned().await.unwrap();

    let record = runtime
        .catalog
        .job("cap-expired-while-down")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, crate::model::JobState::Interrupted);
    assert_eq!(
        runtime.scheduler.pending(),
        0,
        "an image requested an hour ago and already past its deadline is not work to resume"
    );
    runtime.shutdown().await;
}

/// A capture whose camera has become a different device is retired, not resumed.
///
/// The durable profile is the contract the capture was accepted under. A camera that is no
/// longer configured -- or that is now a different backend entirely -- does not get to
/// honour it, so the capture is interrupted exactly as a reload interrupts incompatible work.
#[tokio::test]
async fn a_capture_whose_camera_is_gone_is_not_resumed() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;

    let pending = queued_job(&configuration, "cap-camera-gone");
    runtime.catalog.accept_job(pending).await.unwrap();
    runtime
        .catalog
        .queue_job("cap-camera-gone", chrono::Utc::now().timestamp_millis())
        .await
        .unwrap();

    // camera-b is no longer configured on this run.
    let replacement = config(directory.path(), &["camera-a"], false);
    *runtime.config.write().unwrap() = Arc::new(replacement.clone());
    runtime
        .registry
        .apply_validated_config(&replacement)
        .expect("the registry accepts a config without camera-b");

    runtime.recover_install_owned().await.unwrap();

    let record = runtime
        .catalog
        .job("cap-camera-gone")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, crate::model::JobState::Interrupted);
    assert_eq!(runtime.scheduler.pending(), 0);
    runtime.shutdown().await;
}

#[tokio::test]
async fn startup_recovery_requeues_waiting_work_and_fences_hazardous_commands() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;

    let pending = queued_job(&configuration, "cap-startup-recovery");
    runtime.catalog.accept_job(pending).await.unwrap();
    assert!(matches!(
        runtime
            .catalog
            .queue_job(
                "cap-startup-recovery",
                chrono::Utc::now().timestamp_millis()
            )
            .await
            .unwrap(),
        crate::catalog::StateCasOutcome::Changed(_)
    ));

    let key = crate::catalog::LedgerKey::new(
        "camera-b",
        "sb/ptz/absolute",
        "startup-recovery-ptz",
    )
    .unwrap();
    let canonical = serde_json::json!({
        "requestId": "startup-recovery-ptz",
        "pan": 0.2,
        "tilt": -0.1,
    });
    let request_hash =
        crate::idempotency::canonical_request_hash(&canonical, false).unwrap();
    assert!(matches!(
        runtime
            .catalog
            .begin_command(
                key.clone(),
                request_hash,
                canonical.clone(),
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .unwrap(),
        crate::catalog::BeginCommandOutcome::Started(_)
    ));

    runtime.recover_install_owned().await.unwrap();

    // A capture that was only ever WAITING has no side effects behind it, and
    // `queuedRecoveryPolicy` defaults to `requeue`: it goes back on the fleet queue and is
    // still owed a run, rather than being killed for having outlived the process.
    let recovered = runtime
        .catalog
        .job("cap-startup-recovery")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.state, crate::model::JobState::Queued);
    assert!(recovered.terminal_result.is_none());
    assert_eq!(
        runtime.scheduler.pending(),
        1,
        "a requeued capture must be back in the fleet queue, not merely left QUEUED in the \
         catalog with nothing to drive it"
    );

    let replay = runtime
        .catalog
        .begin_command(
            key,
            request_hash,
            canonical,
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        .unwrap();
    assert!(matches!(
        replay,
        crate::catalog::BeginCommandOutcome::Existing(record)
            if record.state == crate::catalog::LedgerState::OutcomeUnknown
                && record.reply.is_none()
    ));
    runtime.shutdown().await;
}

#[tokio::test]
async fn simulator_runtime_presets_page_and_reject_reused_keys_with_changed_arguments() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    sim.ptz.supported = true;
    sim.ptz.presets_supported = true;
    configuration.instances[0].ptz.enabled = true;
    configuration.instances[0].ptz.allow_preset_mutation = true;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let set: PtzPresetsRequest = commands::parse_closed(json!({
        "operation": "set",
        "instance": "camera-a",
        "requestId": "preset-set-1",
        "name": "loading-bay"
    }))
    .unwrap();
    let first = runtime.perform_presets(set.clone()).await.unwrap();
    let first_token = first["token"].as_str().unwrap().to_string();
    assert_eq!(runtime.perform_presets(set).await.unwrap(), first);
    assert_eq!(
        runtime
            .perform_presets(
                commands::parse_closed(json!({
                    "operation": "set",
                    "instance": "camera-a",
                    "requestId": "preset-set-1",
                    "name": "packing-line"
                }))
                .unwrap()
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::IdempotencyConflict,
        "the same request id must never authorize a changed preset mutation"
    );

    let ptz: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "relative",
        "instance": "camera-a",
        "requestId": "ptz-relative-1",
        "translation": { "pan": 0.1, "tilt": 0.0, "zoom": 0.0 }
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_ptz(ptz.clone()).await.unwrap()["state"],
        "COMMANDED"
    );
    assert_eq!(
        runtime
            .perform_ptz(
                commands::parse_closed(json!({
                    "operation": "relative",
                    "instance": "camera-a",
                    "requestId": "ptz-relative-1",
                    "translation": { "pan": 0.2, "tilt": 0.0, "zoom": 0.0 }
                }))
                .unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::IdempotencyConflict,
        "the same request id must never authorize changed PTZ motion"
    );

    let second = runtime
        .perform_presets(
            commands::parse_closed(json!({
                "operation": "set",
                "instance": "camera-a",
                "requestId": "preset-set-2",
                "name": "packing-line"
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let second_token = second["token"].as_str().unwrap().to_string();
    let first_page = runtime
        .perform_presets(
            commands::parse_closed(json!({
                "operation": "list",
                "instance": "camera-a",
                "limit": 1
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_page["presets"].as_array().unwrap().len(), 1);
    let cursor = first_page["nextCursor"].as_str().unwrap().to_string();
    let second_page = runtime
        .perform_presets(
            commands::parse_closed(json!({
                "operation": "list",
                "instance": "camera-a",
                "limit": 1,
                "cursor": cursor
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_page["presets"].as_array().unwrap().len(), 1);
    assert!(second_page["nextCursor"].is_null());

    assert_eq!(
        runtime
            .perform_presets(
                commands::parse_closed(json!({
                    "operation": "goto",
                    "instance": "camera-a",
                    "requestId": "preset-goto-1",
                    "token": first_token
                }))
                .unwrap(),
            )
            .await
            .unwrap()["state"],
        "COMMANDED"
    );
    assert_eq!(
        runtime
            .perform_presets(
                commands::parse_closed(json!({
                    "operation": "remove",
                    "instance": "camera-a",
                    "requestId": "preset-remove-1",
                    "token": second_token
                }))
                .unwrap(),
            )
            .await
            .unwrap()["removed"],
        true
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn simulator_runtime_exercises_the_full_ptz_and_preset_command_matrix() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    sim.ptz.supported = true;
    sim.ptz.status_supported = true;
    sim.ptz.presets_supported = true;
    configuration.instances[0].ptz.enabled = true;
    configuration.instances[0].ptz.allow_preset_mutation = true;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let absolute: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "absolute",
        "instance": "camera-a",
        "requestId": "ptz-absolute-matrix",
        "position": { "pan": 0.2, "tilt": -0.1, "zoom": 0.3 },
        "speed": { "pan": 0.5, "tilt": 0.4, "zoom": 0.3 }
    }))
    .unwrap();
    let absolute_reply = runtime.perform_ptz(absolute.clone()).await.unwrap();
    assert_eq!(absolute_reply["operation"], "absolute");
    assert_eq!(absolute_reply["state"], "COMMANDED");
    assert_eq!(
        runtime.perform_ptz(absolute).await.unwrap(),
        absolute_reply,
        "an exact absolute PTZ retry must replay its retained result"
    );
    assert_eq!(
        runtime
            .perform_ptz(
                commands::parse_closed(json!({
                    "operation": "absolute",
                    "instance": "camera-a",
                    "requestId": "ptz-absolute-matrix",
                    "position": { "pan": 0.3, "tilt": -0.1, "zoom": 0.3 }
                }))
                .unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::IdempotencyConflict
    );

    let relative: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "relative",
        "instance": "camera-a",
        "requestId": "ptz-relative-matrix",
        "translation": { "pan": 0.1, "tilt": 0.2, "zoom": -0.1 },
        "speed": { "pan": 0.6, "tilt": 0.5, "zoom": 0.4 }
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_ptz(relative).await.unwrap()["state"],
        "COMMANDED"
    );

    let continuous: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "ptz-continuous-matrix",
        "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
        // Long enough that the move is still active when the status query below observes it,
        // even under the slower llvm-cov instrumentation. The explicit stop below ends it, not
        // this timeout, so the value only needs to outlast the observation window.
        "timeoutMs": 5000
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_ptz(continuous).await.unwrap()["state"],
        "COMMANDED"
    );
    let moving = runtime
        .perform_ptz(
            commands::parse_closed(json!({
                "operation": "status",
                "instance": "camera-a"
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(moving["available"], true);
    assert_eq!(moving["moving"], true);

    let stop: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "stop",
        "instance": "camera-a",
        "requestId": "ptz-stop-matrix",
        "axes": ["pan", "zoom"]
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_ptz(stop).await.unwrap()["state"],
        "COMMANDED"
    );
    let stopped = runtime
        .perform_ptz(
            commands::parse_closed(json!({
                "operation": "status",
                "instance": "camera-a"
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stopped["moving"], false);

    let home: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "home",
        "instance": "camera-a",
        "requestId": "ptz-home-matrix"
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_ptz(home).await.unwrap()["state"],
        "COMMANDED"
    );

    let set: PtzPresetsRequest = commands::parse_closed(json!({
        "operation": "set",
        "instance": "camera-a",
        "requestId": "preset-matrix-set",
        "name": "matrix-home"
    }))
    .unwrap();
    let set_reply = runtime.perform_presets(set.clone()).await.unwrap();
    let token = set_reply["token"].as_str().unwrap().to_string();
    assert_eq!(runtime.perform_presets(set).await.unwrap(), set_reply);
    assert_eq!(
        runtime
            .perform_presets(
                commands::parse_closed(json!({
                    "operation": "set",
                    "instance": "camera-a",
                    "requestId": "preset-matrix-set",
                    "name": "matrix-other"
                }))
                .unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::IdempotencyConflict
    );
    assert_eq!(
        runtime
            .perform_presets(
                commands::parse_closed(json!({
                    "operation": "goto",
                    "instance": "camera-a",
                    "requestId": "preset-matrix-goto",
                    "token": token
                }))
                .unwrap(),
            )
            .await
            .unwrap()["state"],
        "COMMANDED"
    );
    let listed = runtime
        .perform_presets(
            commands::parse_closed(json!({
                "operation": "list",
                "instance": "camera-a",
                "limit": 10
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(listed["presets"].as_array().unwrap().len(), 1);
    let remove: PtzPresetsRequest = commands::parse_closed(json!({
        "operation": "remove",
        "instance": "camera-a",
        "requestId": "preset-matrix-remove",
        "token": token
    }))
    .unwrap();
    let removed = runtime.perform_presets(remove.clone()).await.unwrap();
    assert_eq!(removed["removed"], true);
    assert_eq!(runtime.perform_presets(remove).await.unwrap(), removed);
    runtime.shutdown().await;
}

#[tokio::test]
async fn simulator_runtime_discovery_and_capture_status_use_stable_bounded_pages() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.discovery.enabled = true;
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let discovery = runtime
        .discover(DiscoverRequest {
            backends: Vec::new(),
            timeout_ms: 100,
            limit: 1,
            cursor: None,
        })
        .await
        .unwrap();
    assert!(discovery["candidates"].as_array().unwrap().is_empty());
    assert!(discovery["completedAt"].is_string());
    assert!(discovery["nextCursor"].is_null());

    for request_id in ["status-page-a", "status-page-b"] {
        let accepted = runtime
            .submit_capture(
                "camera-a".to_string(),
                request_id.to_string(),
                None,
                None,
                serde_json::Map::new(),
                format!("status-page-correlation-{request_id}"),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .unwrap();
        let capture_id = match accepted {
            crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
            other => panic!("expected new status-page job, got {other:?}"),
        };
        assert_eq!(
            wait_for_terminal(&runtime, &capture_id).await.state,
            crate::model::JobState::Succeeded
        );
    }

    let first = runtime
        .jobs_status_page(&CaptureStatusRequest {
            capture_id: None,
            capture_group_id: None,
            instance: Some("camera-a".to_string()),
            request_id: None,
            states: vec![crate::model::JobState::Succeeded],
            limit: 1,
            cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(first["jobs"].as_array().unwrap().len(), 1);
    let cursor = first["nextCursor"].as_str().unwrap().to_string();
    let second = runtime
        .jobs_status_page(&CaptureStatusRequest {
            capture_id: None,
            capture_group_id: None,
            instance: Some("camera-a".to_string()),
            request_id: None,
            states: vec![crate::model::JobState::Succeeded],
            limit: 1,
            cursor: Some(cursor.clone()),
        })
        .await
        .unwrap();
    assert_eq!(second["jobs"].as_array().unwrap().len(), 1);
    assert!(second["nextCursor"].is_null());
    assert_eq!(
        runtime
            .jobs_status_page(&CaptureStatusRequest {
                capture_id: None,
                capture_group_id: None,
                instance: Some("camera-a".to_string()),
                request_id: None,
                states: vec![crate::model::JobState::Failed],
                limit: 1,
                cursor: Some(cursor),
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::InvalidRequest,
        "a cursor must be bound to its original state filter"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn simulator_runtime_cancel_ledger_replays_exact_results_and_conflicts_on_changes() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1_000;
    }
    let runtime = runtime(configuration, &directory).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "cancel-single-target".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "cancel-single-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected newly accepted cancellation target, got {other:?}"),
    };
    let single_request = CancelRequest {
        request_id: "cancel-single-ledger".to_string(),
        capture_id: Some(capture_id.clone()),
        capture_group_id: None,
        reason: Some("operator stopped this capture".to_string()),
    };
    let single = runtime
        .cancel_capture(single_request.clone())
        .await
        .unwrap();
    assert_eq!(single["captureId"], capture_id);
    assert_eq!(single["cancelled"], true);
    assert_eq!(single["state"], "CANCELLED");
    assert!(
        single["cancellationInProgress"].is_boolean(),
        "the durable cancellation CAS may precede an acquiring backend's unwind"
    );
    assert_eq!(
        runtime
            .cancel_capture(single_request.clone())
            .await
            .unwrap(),
        single,
        "an exact retry must return the stored direct-cancel result"
    );
    let mut changed_single = single_request;
    changed_single.reason = Some("different cancellation reason".to_string());
    assert_eq!(
        runtime
            .cancel_capture(changed_single)
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::IdempotencyConflict
    );

    let group = runtime
        .submit_group(
            GroupCaptureRequest {
                request_id: "cancel-group-target".to_string(),
                instances: vec!["camera-a".to_string(), "camera-b".to_string()],
                capture_profile: None,
                profile_overrides: BTreeMap::new(),
                timeout_ms: None,
                metadata: serde_json::Map::new(),
            },
            "cancel-group-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap();
    let group_request = CancelRequest {
        request_id: "cancel-group-ledger".to_string(),
        capture_id: None,
        capture_group_id: Some(group.group_id.clone()),
        reason: Some("operator cancelled the capture group".to_string()),
    };
    let group_result = runtime.cancel_capture(group_request.clone()).await.unwrap();
    assert_eq!(group_result["captureGroupId"], group.group_id);
    assert_eq!(group_result["cancelledMembers"], 2);
    assert_eq!(group_result["unchangedMembers"], 0);
    let members = group_result["members"].as_array().unwrap();
    assert_eq!(members.len(), 2);
    for member in members {
        assert!(member["captureId"].is_string());
        assert!(member["instance"].is_string());
        assert_eq!(member["cancelled"], true);
        assert_eq!(member["state"], "CANCELLED");
        assert!(
            member["cancellationInProgress"].is_boolean(),
            "an acquiring backend may still be unwinding after the durable cancellation CAS"
        );
    }
    assert_eq!(
        runtime.cancel_capture(group_request.clone()).await.unwrap(),
        group_result,
        "the component-scoped group ledger must replay its exact complete result"
    );
    let canonical_group_cancel = json!({
        "requestId": "cancel-group-ledger",
        "target": { "kind": "capture-group", "captureGroupId": group.group_id },
        "reason": "operator cancelled the capture group",
    });
    assert!(matches!(
        runtime
            .catalog
            .begin_command(
                crate::catalog::LedgerKey::new(
                    "main",
                    "sb/capture-cancel",
                    "cancel-group-ledger",
                )
                .unwrap(),
                crate::idempotency::canonical_request_hash(&canonical_group_cancel, false)
                    .unwrap(),
                canonical_group_cancel,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .unwrap(),
        crate::catalog::BeginCommandOutcome::Existing(record)
            if record.state == crate::catalog::LedgerState::Succeeded
                && record.reply.as_ref() == Some(&group_result)
    ));
    let mut changed_group = group_request;
    changed_group.reason = Some("different group cancellation reason".to_string());
    assert_eq!(
        runtime
            .cancel_capture(changed_group)
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::IdempotencyConflict
    );
    runtime.shutdown().await;
}

/// The other eight verbs, through the router that production actually calls.
///
/// `handle_camera_command` is the entry point for every southbound command, and six of the fourteen
/// verbs reached it in a test. The other eight -- discover, queue-status, queue-clear, reconnect,
/// ptz-presets, capture-group-submit, and the two deferred ones -- were only ever exercised by
/// calling the runtime methods DIRECTLY, which skips the router's parsing, its closed-schema
/// validation, its cursor handling, and its error mapping. That is the shape of a test that proves
/// the engine turns over while never checking the ignition is wired to it.
#[tokio::test]
async fn every_remaining_verb_dispatches_through_the_production_router() {
    let (port, _broker) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.ptz.supported = true;
        sim.ptz.status_supported = true;
        sim.ptz.presets_supported = true;
        camera.ptz.enabled = true;
    }
    configuration.global.discovery.enabled = true;
    let runtime = runtime(configuration, &directory).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }
    let (_app, deferred) = command_deferred_registry(&directory, port).await;

    // sb/discover -- the bounded, credential-free operator aid.
    let discovered = immediate_success(
        runtime
            .handle_camera_command(
                "sb/discover",
                command_message("sb/discover", "discover", json!({"timeoutMs": 1_000})),
                deferred.clone(),
            )
            .await,
    );
    assert!(discovered.get("candidates").is_some());

    // sb/queue-status -- the fleet queue's depth, per camera and in total.
    let queue = immediate_success(
        runtime
            .handle_camera_command(
                "sb/queue-status",
                command_message("sb/queue-status", "queue-status", json!({})),
                deferred.clone(),
            )
            .await,
    );
    assert!(queue.get("cameras").is_some(), "queue status reports per-camera depth");

    // sb/capture-group-submit -- software fan-out, one aggregated durable group.
    let group = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-group-submit",
                command_message(
                    "sb/capture-group-submit",
                    "group-submit",
                    json!({
                        "requestId": "router-group",
                        "instances": ["camera-a", "camera-b"],
                    }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert!(
        group.get("captureGroupId").is_some(),
        "a submitted group must answer with its durable group id"
    );

    // sb/ptz-presets -- the read side of the preset surface.
    let presets = immediate_success(
        runtime
            .handle_camera_command(
                "sb/ptz-presets",
                command_message(
                    "sb/ptz-presets",
                    "presets",
                    json!({"instance": "camera-a", "operation": "list"}),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert!(presets.get("presets").is_some());

    // sb/reconnect -- idempotent, and it cancels the live session rather than the camera.
    let reconnect = immediate_success(
        runtime
            .handle_camera_command(
                "sb/reconnect",
                command_message(
                    "sb/reconnect",
                    "reconnect",
                    json!({"instance": "camera-a", "requestId": "router-reconnect"}),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(reconnect["state"], "ACCEPTED");

    // sb/queue-clear -- the drain, which is idempotent by design.
    let cleared = immediate_success(
        runtime
            .handle_camera_command(
                "sb/queue-clear",
                command_message(
                    "sb/queue-clear",
                    "queue-clear",
                    json!({
                        "requestId": "router-clear",
                        "instance": "camera-a",
                        "reason": "operator drained the queue",
                    }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert!(
        cleared.get("cancelled").is_some() && cleared.get("failed").is_some(),
        "a drain reports what it could not do rather than claiming a clean sweep"
    );

    // An unknown verb is refused by the router, not by whatever it might have dispatched to.
    let unknown = runtime
        .handle_camera_command(
            "sb/not-a-verb",
            command_message("sb/not-a-verb", "unknown", json!({})),
            deferred.clone(),
        )
        .await;
    assert!(
        matches!(unknown, CommandOutcome::ImmediateError(_)),
        "a verb the component does not have must be refused at the router"
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn command_service_dispatches_immediate_verbs_with_a_real_inbox_registry() {
    let (port, _) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    // Keep acquisition active long enough for the immediate cancellation verb to win.
    sim.capture_delay_ms = 1_000;
    sim.ptz.supported = true;
    configuration.instances[0].ptz.enabled = true;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    let list = immediate_success(
        runtime
            .handle_camera_command(
                "sb/list",
                command_message("sb/list", "list", json!({"limit": 10})),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(list["cameras"].as_array().unwrap().len(), 1);
    assert_eq!(list["cameras"][0]["instance"], "camera-a");

    let status = immediate_success(
        runtime
            .handle_camera_command(
                "sb/status",
                command_message("sb/status", "status", json!({"instance": "camera-a"})),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(status["instance"], "camera-a");
    assert_eq!(status["state"], "ONLINE");

    let accepted = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-submit",
                command_message(
                    "sb/capture-submit",
                    "capture-submit",
                    json!({"instance": "camera-a", "requestId": "command-e2e-capture"}),
                ),
                deferred.clone(),
            )
            .await,
    );
    let capture_id = accepted["captureId"]
        .as_str()
        .expect("capture-submit returns a durable capture ID")
        .to_string();
    assert_eq!(accepted["state"], "QUEUED");

    let status_before_cancel = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-status",
                command_message(
                    "sb/capture-status",
                    "capture-status-before-cancel",
                    json!({"captureId": capture_id}),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(status_before_cancel["captureId"], capture_id);

    let cancelled = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-cancel",
                command_message(
                    "sb/capture-cancel",
                    "capture-cancel",
                    json!({
                        "requestId": "command-e2e-cancel",
                        "captureId": capture_id,
                        "reason": "operator cancelled command-dispatch coverage fixture"
                    }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(cancelled["captureId"], capture_id);
    assert_eq!(cancelled["state"], "CANCELLED");
    assert_eq!(cancelled["cancelled"], true);

    let status_after_cancel = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-status",
                command_message(
                    "sb/capture-status",
                    "capture-status-after-cancel",
                    json!({"captureId": capture_id}),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(status_after_cancel["state"], "CANCELLED");

    let ptz = immediate_success(
        runtime
            .handle_camera_command(
                "sb/ptz",
                command_message(
                    "sb/ptz",
                    "ptz-status",
                    json!({"operation": "status", "instance": "camera-a"}),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(ptz["available"], true);
    assert!(ptz.get("position").is_some());

    match runtime
        .handle_camera_command(
            "sb/list",
            command_message("sb/list", "invalid-list", json!({"limit": 0})),
            deferred,
        )
        .await
    {
        CommandOutcome::ImmediateError(error) => {
            assert_eq!(error.code, crate::ErrorCode::InvalidRequest.as_str());
        }
        other => panic!("invalid request must return an immediate error, got {other:?}"),
    }

    app.commands().unwrap().stop().await;
    runtime.shutdown().await;
    drop(app);
}

#[tokio::test]
async fn simulator_runtime_executes_group_capture_and_paged_group_status() {
    let directory = TempDir::new().unwrap();
    let mut config = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut config.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    let runtime = runtime(config, &directory).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    let group = runtime
        .submit_group(
            GroupCaptureRequest {
                request_id: "runtime-e2e-group".to_string(),
                instances: vec!["camera-a".to_string(), "camera-b".to_string()],
                capture_profile: None,
                profile_overrides: BTreeMap::new(),
                timeout_ms: None,
                metadata: serde_json::Map::new(),
            },
            "runtime-e2e-group-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap();
    for member in &group.members {
        assert_eq!(
            wait_for_terminal(&runtime, &member.capture_id).await.state,
            crate::model::JobState::Succeeded
        );
    }
    let completed_group = runtime
        .catalog
        .group(group.group_id.clone())
        .await
        .unwrap()
        .unwrap();
    let first = runtime.group_status_page(completed_group, 1, None).unwrap();
    assert_eq!(first["members"].as_array().unwrap().len(), 1);
    let cursor = first["nextCursor"]
        .as_str()
        .expect("two-member group must page at a limit of one");
    let second = runtime
        .group_status_page(
            runtime
                .catalog
                .group(group.group_id)
                .await
                .unwrap()
                .unwrap(),
            1,
            Some(cursor),
        )
        .unwrap();
    assert_eq!(second["members"].as_array().unwrap().len(), 1);
    assert!(second["nextCursor"].is_null());
    runtime.shutdown().await;
}

#[tokio::test]
async fn live_same_backend_reload_replaces_session_without_stale_generation() {
    let directory = TempDir::new().unwrap();
    let mut initial = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(initial, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let previous_generation = runtime.registry.snapshot("camera-a").unwrap().generation;

    let mut replacement = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    sim.seed = Some(991);
    let diff = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(diff.lifecycle_changed, vec!["camera-a".to_string()]);
    wait_for_online(&runtime, "camera-a").await;
    assert!(
        runtime.registry.snapshot("camera-a").unwrap().generation > previous_generation,
        "replacement supervisor must advance the lifecycle generation"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn reload_timeout_preserves_prior_generation_until_a_safe_retry() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a"], false);
    let runtime = runtime(initial, &directory).await;
    let generation_before = runtime.registry.snapshot("camera-a").unwrap().generation;

    // Model an old supervisor that has accepted cancellation but cannot yet prove that
    // every backend/session task has exited. A zero drain budget makes the timeout
    // deterministic without depending on executor scheduling.
    let old_cancellation = CancellationToken::new();
    let old_finished = CancellationToken::new();
    runtime
        .cameras
        .write()
        .unwrap()
        .get_mut("camera-a")
        .unwrap()
        .supervisor = Some(Supervision {
        cancellation: old_cancellation.clone(),
        finished: old_finished.clone(),
    });

    let mut replacement = config(directory.path(), &["camera-a"], false);
    replacement.global.timeouts.reload_drain_timeout_ms = 0;
    let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    sim.seed = Some(991);

    let error = runtime
        .apply_reloaded_config(replacement.clone(), BTreeMap::new(), BTreeMap::new())
        .await
        .expect_err("a candidate must be vetoed while an old supervisor is unconfirmed");
    assert_eq!(error.code(), crate::ErrorCode::CameraUnavailable);
    assert!(old_cancellation.is_cancelled());
    assert!(
        runtime
            .cameras
            .read()
            .unwrap()
            .get("camera-a")
            .and_then(|slot| slot.supervisor.as_ref())
            .is_some_and(|supervisor| supervisor.cancellation.is_cancelled()),
        "timeout must not install a replacement supervisor token"
    );
    assert_eq!(
        runtime.registry.snapshot("camera-a").unwrap().generation,
        generation_before,
        "a rejected pre-commit candidate must not advance the runtime registry"
    );
    let crate::config::BackendConfig::Sim(sim) =
        &runtime.config_snapshot().unwrap().instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    assert_eq!(
        sim.seed, None,
        "runtime configuration must remain on Core's prior generation"
    );

    // Once termination is confirmed, a retry may atomically advance the candidate and
    // only then install its replacement generation.
    old_finished.cancel();
    let diff = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .expect("a confirmed old-supervisor exit must permit retry");
    assert_eq!(diff.lifecycle_changed, vec!["camera-a".to_string()]);
    assert!(
        !runtime
            .cameras
            .read()
            .unwrap()
            .get("camera-a")
            .and_then(|slot| slot.supervisor.as_ref())
            .is_some_and(|supervisor| supervisor.cancellation.is_cancelled()),
        "the replacement supervisor starts only after the old completion signal"
    );
    let crate::config::BackendConfig::Sim(sim) =
        &runtime.config_snapshot().unwrap().instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    assert_eq!(sim.seed, Some(991));
    runtime.shutdown().await;
}

#[cfg(not(feature = "genicam"))]
#[tokio::test]
async fn supervisor_reports_unavailable_genicam_as_backoff_without_creating_an_actor() {
    let directory = TempDir::new().unwrap();
    let mut raw = core_config_value(directory.path(), &["camera-a"], false);
    raw["component"]["instances"][0]["backend"] = json!({
        "type": "genicam-aravis",
        "selector": { "serial": "genicam-feature-gate-test" }
    });
    let configuration = AdapterConfig::from_core_reload(
        &Config::from_value(COMPONENT_NAME, "gw-01", raw)
            .expect("core carries a valid adapter-specific GenICam shape"),
    )
    .expect("GenICam configuration itself is valid even when its native feature is absent");
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let snapshot = runtime.registry.snapshot("camera-a").unwrap();
        if snapshot.state == CameraConnectionState::Backoff {
            assert_eq!(
                snapshot
                    .last_error
                    .as_ref()
                    .map(|error| error.code.as_str()),
                Some(crate::ErrorCode::UnsupportedCapability.as_str())
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a missing native GenICam feature must be surfaced as BACKOFF"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    match runtime.actor("camera-a") {
        Err(error) => assert_eq!(
            error.code(),
            crate::ErrorCode::CameraUnavailable,
            "a rejected backend factory must never leave a live actor handle"
        ),
        Ok(_) => panic!("a rejected backend factory must never leave a live actor handle"),
    }
    runtime.shutdown().await;
}

#[tokio::test]
async fn runtime_command_paths_reject_disabled_ptz_and_preserve_reconnect_conflicts() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a"], false);
    let runtime = runtime(initial, &directory).await;
    let ptz: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "status", "instance": "camera-a"
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_ptz(ptz).await.unwrap_err().code(),
        crate::ErrorCode::PtzDisabled
    );
    runtime
        .reconnect(ReconnectRequest {
            instance: Some("camera-a".to_string()),
            request_id: "reconnect-conflict".to_string(),
            reason: None,
        })
        .await
        .unwrap();
    assert_eq!(
        runtime
            .reconnect(ReconnectRequest {
                instance: Some("camera-a".to_string()),
                request_id: "reconnect-conflict".to_string(),
                reason: Some("changed".to_string()),
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::IdempotencyConflict
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn reload_schedule_only_restarts_plan_without_replacing_roster() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a"], false);
    let runtime = runtime(initial, &directory).await;
    let replacement = config(directory.path(), &["camera-a"], true);
    let diff = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(diff.updated, vec!["camera-a".to_string()]);
    assert_eq!(
        runtime.config_snapshot().unwrap().instances[0]
            .schedules
            .len(),
        1
    );
    assert_eq!(runtime.scheduler_cancellations.read().unwrap().len(), 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn reload_cancels_periodic_discovery_and_clears_retained_observations() {
    let directory = TempDir::new().unwrap();
    let mut initial = config(directory.path(), &["camera-a"], false);
    initial.global.discovery.enabled = true;
    initial.global.discovery.report_unconfigured = true;
    initial.global.discovery.interval_seconds = 5;
    let runtime = runtime(initial, &directory).await;
    runtime.start_periodic_discovery().unwrap();
    let previous = runtime
        .discovery_cancellation
        .read()
        .unwrap()
        .clone()
        .expect("enabled discovery starts a cancellable generation");
    runtime
        .discovery_cache
        .lock()
        .unwrap()
        .candidates
        .push(DiscoveryCandidate {
            backend: crate::model::BackendKind::GenicamAravis,
            selector: json!({ "serial": "unconfigured" }),
            vendor: None,
            model: None,
            capabilities: json!({}),
        });
    assert_eq!(
        runtime
            .unconfigured_discoveries(&runtime.config_snapshot().unwrap())
            .unwrap()
            .len(),
        1
    );

    let replacement = config(directory.path(), &["camera-a"], false);
    runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .unwrap();
    assert!(previous.is_cancelled());
    assert!(runtime.discovery_cancellation.read().unwrap().is_none());
    assert!(
        runtime
            .discovery_cache
            .lock()
            .unwrap()
            .candidates
            .is_empty()
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn periodic_discovery_replaces_stale_cache_and_retires_each_generation() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.discovery.enabled = true;
    configuration.global.discovery.report_unconfigured = true;
    configuration.global.discovery.interval_seconds = 60;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .discovery_cache
        .lock()
        .unwrap()
        .candidates
        .push(DiscoveryCandidate {
            backend: crate::model::BackendKind::GenicamAravis,
            selector: json!({ "serial": "stale-discovery" }),
            vendor: Some("stale".to_string()),
            model: None,
            capabilities: json!({}),
        });

    runtime.start_periodic_discovery().unwrap();
    let first = runtime
        .discovery_cancellation
        .read()
        .unwrap()
        .clone()
        .expect("enabled discovery must retain its cancellation generation");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if runtime
            .discovery_cache
            .lock()
            .unwrap()
            .candidates
            .is_empty()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the simulator discovery pass did not replace stale retained observations"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    runtime.restart_periodic_discovery().unwrap();
    assert!(
        first.is_cancelled(),
        "a restart must retire the former periodic-discovery generation"
    );
    let second = runtime
        .discovery_cancellation
        .read()
        .unwrap()
        .clone()
        .expect("a restart must create one replacement discovery generation");
    assert!(
        !second.is_cancelled(),
        "the replacement discovery generation must remain active before reload"
    );

    let replacement = config(directory.path(), &["camera-a"], false);
    runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .unwrap();
    assert!(
        second.is_cancelled(),
        "disabling discovery through reload must retire the active generation"
    );
    assert!(runtime.discovery_cancellation.read().unwrap().is_none());
    assert!(
        runtime
            .discovery_cache
            .lock()
            .unwrap()
            .candidates
            .is_empty()
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn discovery_returns_a_bounded_empty_sim_snapshot_and_replays_retained_pages() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.discovery.enabled = true;
    let runtime = runtime(configuration, &directory).await;
    let request = DiscoverRequest {
        backends: Vec::new(),
        timeout_ms: 100,
        limit: 1,
        cursor: None,
    };

    let empty = runtime.discover(request.clone()).await.unwrap();
    assert_eq!(empty["candidates"], json!([]));
    assert!(empty["nextCursor"].is_null());
    assert!(empty["completedAt"].is_string());

    let query = json!({ "backends": request.backends });
    let (_, cursor, completed_at) = runtime
        .cursors
        .snapshot_page(
            "discover",
            &query,
            None,
            Some(vec![
                json!({ "backend": "onvif-rtsp", "selector": { "serial": "one" } }),
                json!({ "backend": "onvif-rtsp", "selector": { "serial": "two" } }),
            ]),
            Some(json!("2026-07-12T00:00:00Z")),
            1,
        )
        .unwrap();
    let page = runtime
        .discover(DiscoverRequest { cursor, ..request })
        .await
        .unwrap();
    assert_eq!(
        page["candidates"],
        json!([{ "backend": "onvif-rtsp", "selector": { "serial": "two" } }])
    );
    assert!(page["nextCursor"].is_null());
    assert_eq!(page["completedAt"], completed_at.unwrap());
    runtime.shutdown().await;
}

#[tokio::test]
async fn reload_supervisor_barrier_cancels_before_timing_out_and_then_allows_retry() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let cancellation = CancellationToken::new();
    let completion = CancellationToken::new();
    runtime
        .cameras
        .write()
        .unwrap()
        .get_mut("camera-a")
        .unwrap()
        .supervisor = Some(Supervision {
        cancellation: cancellation.clone(),
        finished: completion.clone(),
    });

    let error = runtime
        .replace_supervisors(&["camera-a".to_string()], Duration::ZERO)
        .await
        .expect_err("an unconfirmed supervisor must veto the replacement");
    assert_eq!(error.code(), crate::ErrorCode::CameraUnavailable);
    assert!(
        cancellation.is_cancelled(),
        "the old generation must be cancelled even when its drain deadline is already elapsed"
    );

    completion.cancel();
    runtime
        .replace_supervisors(&["camera-a".to_string()], Duration::ZERO)
        .await
        .expect("a confirmed generation must make the retry safe");
    runtime.shutdown().await;
}

#[tokio::test]
async fn reload_removal_interrupts_queued_work_and_retains_other_camera() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(initial.clone(), &directory).await;
    let queued = queued_job(&initial, "cap_reload_queued");
    runtime.catalog.accept_job(queued).await.unwrap();
    runtime
        .catalog
        .queue_job("cap_reload_queued", chrono::Utc::now().timestamp_millis())
        .await
        .unwrap();

    let replacement = config(directory.path(), &["camera-a"], false);
    let diff = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(diff.removed, vec!["camera-b".to_string()]);
    let job = runtime
        .catalog
        .job("cap_reload_queued")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, crate::model::JobState::Interrupted);
    assert_eq!(job.error_code.as_deref(), Some("PROCESS_INTERRUPTED"));
    assert!(
        job.error_message
            .as_deref()
            .is_some_and(|message| message.contains("configuration reload"))
    );
    assert!(runtime.registry.snapshot("camera-b").is_err());
    assert_eq!(
        runtime.registry.snapshot("camera-a").unwrap().instance,
        "camera-a"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn reload_connection_change_restarts_only_the_session_and_keeps_compatible_queue() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(initial.clone(), &directory).await;
    runtime
        .catalog
        .accept_job(queued_job(&initial, "cap_same_backend"))
        .await
        .unwrap();
    runtime
        .catalog
        .queue_job("cap_same_backend", chrono::Utc::now().timestamp_millis())
        .await
        .unwrap();

    let mut replacement = config(directory.path(), &["camera-a", "camera-b"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[1].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    sim.seed = Some(73);
    let diff = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(diff.lifecycle_changed, vec!["camera-b".to_string()]);
    assert_eq!(
        runtime
            .catalog
            .job("cap_same_backend")
            .await
            .unwrap()
            .unwrap()
            .state,
        crate::model::JobState::Queued
    );
    runtime.shutdown().await;
}

/// A reload that changes only the global security policy still re-evaluates the per-camera
/// restart guard. That guard tests each retained camera's backend kind against the ONVIF and RTSP
/// protocol backends; for a simulator roster both comparisons are simply false, but they must run.
#[tokio::test]
async fn reload_with_a_changed_security_policy_reevaluates_the_restart_guard() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a"], false);
    let runtime = runtime(initial.clone(), &directory).await;

    let mut replacement = config(directory.path(), &["camera-a"], false);
    replacement.global.security.max_header_bytes = 32_768;
    assert_ne!(
        initial.global.security.max_header_bytes,
        replacement.global.security.max_header_bytes,
        "the reload must actually change the security policy for the guard to be exercised"
    );

    runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .unwrap();

    assert_eq!(
        runtime
            .config_snapshot()
            .unwrap()
            .global
            .security
            .max_header_bytes,
        32_768
    );
    assert_eq!(runtime.registry.ids().unwrap(), vec!["camera-a".to_string()]);
    runtime.shutdown().await;
}

#[tokio::test]
async fn rejected_runtime_reload_leaves_the_generation_and_roster_unchanged() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a"], false);
    let runtime = runtime(initial, &directory).await;
    let other_root = directory.path().join("other-output");
    std::fs::create_dir_all(&other_root).unwrap();
    let replacement = config(&other_root, &["camera-a"], true);
    assert!(
        runtime
            .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
            .await
            .is_err()
    );
    assert!(
        runtime.config_snapshot().unwrap().instances[0]
            .schedules
            .is_empty()
    );
    assert_eq!(
        runtime.registry.ids().unwrap(),
        vec!["camera-a".to_string()]
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn runtime_config_listener_rejects_invalid_candidates_and_factory_failures_without_mutation()
 {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let app_calls = Arc::new(AtomicUsize::new(0));
    let event_calls = Arc::new(AtomicUsize::new(0));
    let listener = RuntimeConfigListener::new(
        Arc::downgrade(&runtime),
        {
            let app_calls = Arc::clone(&app_calls);
            Arc::new(move |_instance, _config| {
                app_calls.fetch_add(1, Ordering::AcqRel);
                Err(edgecommons::EdgeCommonsError::Facade(
                    "controlled application facade failure".to_string(),
                ))
            })
        },
        {
            let event_calls = Arc::clone(&event_calls);
            Arc::new(move |_instance, _config| {
                event_calls.fetch_add(1, Ordering::AcqRel);
                Err(edgecommons::EdgeCommonsError::Facade(
                    "controlled events facade failure".to_string(),
                ))
            })
        },
    );

    let mut invalid_raw = core_config_value(directory.path(), &["camera-a"], false);
    invalid_raw["component"]["instances"][0]["backend"] =
        json!({ "type": "unknown-camera-protocol" });
    let invalid = Arc::new(
        Config::from_value(COMPONENT_NAME, "gw-01", invalid_raw)
            .expect("core accepts opaque adapter-specific backend configuration"),
    );
    assert!(
        listener.prepare_configuration_apply(invalid).await.is_err(),
        "an adapter-invalid core generation must be vetoed without facade construction"
    );
    assert_eq!(app_calls.load(Ordering::Acquire), 0);
    assert_eq!(event_calls.load(Ordering::Acquire), 0);
    assert_eq!(
        runtime.registry.ids().unwrap(),
        vec!["camera-a".to_string()]
    );
    assert!(
        runtime.config_snapshot().unwrap().instances[0]
            .capture_profiles
            .contains_key("main")
    );

    assert!(
        listener
            .prepare_configuration_apply(Arc::new(core_config(
                directory.path(),
                &["camera-a", "camera-b"],
                false,
            )))
            .await
            .is_err(),
        "a facade-factory failure must reject the generation before runtime mutation"
    );
    assert_eq!(
        app_calls.load(Ordering::Acquire),
        1,
        "the first replacement camera is the only application facade requested"
    );
    assert_eq!(
        event_calls.load(Ordering::Acquire),
        0,
        "events construction must not follow an application-factory failure"
    );
    assert_eq!(
        runtime.registry.ids().unwrap(),
        vec!["camera-a".to_string()]
    );
    assert!(
        runtime
            .cameras
            .read()
            .unwrap()
            .values()
            .all(|slot| slot.events.is_none())
    );
    runtime.shutdown().await;
}

#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn runtime_config_listener_does_not_mutate_when_event_facade_preparation_fails() {
    let directory = TempDir::new().unwrap();
    let (port, _) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let app_calls = Arc::new(AtomicUsize::new(0));
    let event_calls = Arc::new(AtomicUsize::new(0));
    let listener = RuntimeConfigListener::new(
        Arc::downgrade(&runtime),
        {
            let core = Arc::clone(&core);
            let app_calls = Arc::clone(&app_calls);
            Arc::new(move |instance, candidate| {
                app_calls.fetch_add(1, Ordering::AcqRel);
                Ok(Arc::new(
                    core.instance_from_config_snapshot(instance, candidate)?
                        .app(),
                ))
            })
        },
        {
            let event_calls = Arc::clone(&event_calls);
            Arc::new(move |_instance, _candidate| {
                event_calls.fetch_add(1, Ordering::AcqRel);
                Err(edgecommons::EdgeCommonsError::Facade(
                    "controlled event facade preparation failure".to_string(),
                ))
            })
        },
    );

    let error = match listener
        .prepare_configuration_apply(Arc::new(core_config(
            directory.path(),
            &["camera-a"],
            false,
        )))
        .await
    {
        Ok(_) => panic!("an event facade failure must veto the candidate"),
        Err(error) => error,
    };
    assert_eq!(error.code, "CONFIG_APPLICATION_PREPARE_FAILED");
    assert_eq!(app_calls.load(Ordering::Acquire), 1);
    assert_eq!(event_calls.load(Ordering::Acquire), 1);
    assert_eq!(
        runtime.registry.ids().unwrap(),
        vec!["camera-a".to_string()]
    );
    assert!(
        runtime
            .cameras
            .read()
            .unwrap()
            .values()
            .all(|slot| slot.events.is_none()),
        "preparation failures must not install a partial event facade map"
    );
    runtime.shutdown().await;
}

#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn scheduled_reject_skip_emits_event_and_never_accepts_a_job() {
    let directory = TempDir::new().unwrap();
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let mut initial = config(directory.path(), &["camera-a"], true);
    let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.ptz.supported = true;
    sim.ptz.status_supported = true;
    initial.instances[0].ptz.enabled = true;
    initial.instances[0]
        .capture_profiles
        .get_mut("main")
        .unwrap()
        .capture_interlock = Some(crate::config::CaptureInterlock::Reject);
    let runtime = runtime(initial, &directory).await;
    runtime
        .cameras
        .write()
        .unwrap()
        .get_mut("camera-a")
        .unwrap()
        .events = Some(core.instance("camera-a").unwrap().events());
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    runtime
        .actor("camera-a")
        .unwrap()
        .ptz(
            crate::model::PtzRequest::Continuous {
                velocity: crate::model::PtzVector {
                    pan: 0.5,
                    tilt: 0.0,
                    zoom: 0.0,
                },
                timeout: Duration::from_secs(2),
            },
            tokio::time::Instant::now() + Duration::from_secs(1),
            &runtime.cancellation,
        )
        .await
        .unwrap();
    let occurrence = ScheduleOccurrence {
        scope: crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
        schedule_id: "minute".to_string(),
        intended_fire_time: chrono::Utc::now(),
        admit_at: chrono::Utc::now(),
        jitter: Duration::ZERO,
    };

    runtime.submit_scheduled(&occurrence).await.unwrap();

    assert!(
        runtime
            .catalog
            .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
            .await
            .unwrap()
            .is_empty(),
        "a rejected scheduled occurrence must not create a durable capture job"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let (topic, bytes) = loop {
        if let Some(event) = publishes
            .lock()
            .unwrap()
            .iter()
            .find(|(topic, _)| topic.ends_with("/evt/warning/schedule-skipped"))
            .cloned()
        {
            break event;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled reject skip did not publish an operator event"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(topic.ends_with("/evt/warning/schedule-skipped"));
    let event = Message::from_slice(&bytes).unwrap();
    assert_eq!(event.header.name, "evt");
    assert_eq!(event.body["severity"], "warning");
    assert_eq!(event.body["type"], "schedule-skipped");
    assert_eq!(event.body["context"]["scheduleId"], "minute");
    assert_eq!(event.body["context"]["code"], "CAMERA_MOVING");
    runtime.shutdown().await;
}

#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn capture_lifecycle_events_are_opt_in_and_use_the_event_facade() {
    let directory = TempDir::new().unwrap();
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.operator_events.capture_lifecycle = true;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .cameras
        .write()
        .unwrap()
        .get_mut("camera-a")
        .unwrap()
        .events = Some(core.instance("camera-a").unwrap().events());
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "lifecycle-events-enabled".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "lifecycle-events-enabled-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
        panic!("the first lifecycle capture must be newly accepted");
    };
    let capture_id = record.capture_id;
    let terminal = wait_for_terminal(&runtime, &capture_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let lifecycle = loop {
        let lifecycle = publishes
            .lock()
            .unwrap()
            .iter()
            .filter(|(topic, _)| {
                topic.ends_with("/evt/debug/capture-queued")
                    || topic.ends_with("/evt/info/capture-started")
            })
            .cloned()
            .collect::<Vec<_>>();
        if lifecycle.len() == 2 {
            break lifecycle;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "enabled capture lifecycle events were not routed through the event facade"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let queued = lifecycle
        .iter()
        .find(|(topic, _)| topic.ends_with("/evt/debug/capture-queued"))
        .expect("exactly one queued event must be published");
    let queued = Message::from_slice(&queued.1).unwrap();
    assert_eq!(queued.header.name, "evt");
    assert_eq!(queued.body["severity"], "debug");
    assert_eq!(queued.body["type"], "capture-queued");
    assert_eq!(queued.body["context"]["captureId"], capture_id);
    assert_eq!(queued.body["context"]["trigger"], "command");
    assert_eq!(queued.body["context"]["captureProfile"], "main");
    assert_eq!(queued.body["context"]["queuePosition"], 1);
    let started = lifecycle
        .iter()
        .find(|(topic, _)| topic.ends_with("/evt/info/capture-started"))
        .expect("exactly one started event must be published");
    let started = Message::from_slice(&started.1).unwrap();
    assert_eq!(started.header.name, "evt");
    assert_eq!(started.body["severity"], "info");
    assert_eq!(started.body["type"], "capture-started");
    assert_eq!(started.body["context"]["captureId"], capture_id);
    assert_eq!(started.body["context"]["trigger"], "command");
    assert_eq!(started.body["context"]["captureProfile"], "main");
    assert_eq!(started.body["context"]["captureMode"], "simulated");

    let terminal = runtime.catalog.job(&capture_id).await.unwrap().unwrap();
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    runtime.shutdown().await;
}

#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn disabled_capture_lifecycle_events_do_not_publish_or_change_terminal_delivery() {
    let directory = TempDir::new().unwrap();
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    runtime
        .cameras
        .write()
        .unwrap()
        .get_mut("camera-a")
        .unwrap()
        .events = Some(core.instance("camera-a").unwrap().events());
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "lifecycle-events-disabled".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "lifecycle-events-disabled-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
        panic!("the first disabled lifecycle capture must be newly accepted");
    };
    let terminal = wait_for_terminal(&runtime, &record.capture_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        publishes.lock().unwrap().iter().all(|(topic, _)| {
            !topic.ends_with("/evt/debug/capture-queued")
                && !topic.ends_with("/evt/info/capture-started")
        }),
        "disabled lifecycle diagnostics must not publish"
    );
    assert!(
        terminal.terminal_result.is_some(),
        "disabling the lifecycle diagnostics must not touch the durable terminal result"
    );
    runtime.shutdown().await;
}

#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn saturated_lifecycle_dispatcher_drops_diagnostics_without_blocking_acceptance() {
    let directory = TempDir::new().unwrap();
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.operator_events.capture_lifecycle = true;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .cameras
        .write()
        .unwrap()
        .get_mut("camera-a")
        .unwrap()
        .events = Some(core.instance("camera-a").unwrap().events());
    let permits = Arc::clone(&runtime.waiters.lifecycle_event_slots)
        .acquire_many_owned(
            u32::try_from(MAX_LIFECYCLE_EVENT_PUBLISHES)
                .expect("the fixed lifecycle capacity fits Tokio's semaphore API"),
        )
        .await
        .expect("the test holds every detached lifecycle permit");

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "saturated-lifecycle-dispatch".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "saturated-lifecycle-dispatch-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("diagnostic saturation must not reject durable capture acceptance");
    let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
        panic!("the fixture capture must be newly accepted");
    };
    assert_eq!(record.state, crate::model::JobState::Queued);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        publishes.lock().unwrap().iter().all(|(topic, _)| {
            !topic.ends_with("/evt/debug/capture-queued")
                && !topic.ends_with("/evt/info/capture-started")
        }),
        "a saturated bounded dispatcher must drop diagnostics rather than queue task work"
    );

    drop(permits);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        publishes.lock().unwrap().iter().all(|(topic, _)| {
            !topic.ends_with("/evt/debug/capture-queued")
                && !topic.ends_with("/evt/info/capture-started")
        }),
        "dropped diagnostics must not be replayed after capacity returns"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn unavailable_lifecycle_event_routing_does_not_change_terminal_capture_outcome() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.operator_events.capture_lifecycle = true;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "lifecycle-events-unavailable".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "lifecycle-events-unavailable-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
        panic!("the first unavailable lifecycle capture must be newly accepted");
    };
    let terminal = wait_for_terminal(&runtime, &record.capture_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    assert!(
        terminal.terminal_result.is_some(),
        "a camera with no event facade still commits its durable terminal result"
    );
    runtime.shutdown().await;
}

#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn reload_adds_and_removes_event_facades_with_the_camera_roster() {
    let directory = TempDir::new().unwrap();
    let (port, _) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let initial = config(directory.path(), &["camera-a"], false);
    let runtime = runtime(initial, &directory).await;
    let replacement = config(directory.path(), &["camera-a", "camera-b"], false);
    let mut apps = BTreeMap::new();
    apps.insert(
        "camera-b".to_string(),
        Arc::new(core.instance("camera-b").unwrap().app()),
    );
    let mut events = BTreeMap::new();
    events.insert(
        "camera-b".to_string(),
        core.instance("camera-b").unwrap().events(),
    );
    let diff = runtime
        .apply_reloaded_config(replacement, apps, events)
        .await
        .unwrap();
    assert_eq!(diff.added, vec!["camera-b".to_string()]);
    assert_eq!(
        runtime
            .cameras
            .read()
            .unwrap()
            .get("camera-b")
            .and_then(|slot| slot.events.as_ref())
            .map(EventsFacade::instance_id),
        Some("camera-b")
    );

    let removal = config(directory.path(), &["camera-a"], false);
    runtime
        .apply_reloaded_config(removal, BTreeMap::new(), BTreeMap::new())
        .await
        .unwrap();
    assert!(
        !runtime.cameras.read().unwrap().contains_key("camera-b"),
        "removing a camera must retire its event facade with the rest of its runtime state"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn rejected_reload_preflight_keeps_the_prior_supervisor_serving_captures() {
    let directory = TempDir::new().unwrap();
    let mut initial = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(initial, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let generation_before = runtime.registry.snapshot("camera-a").unwrap().generation;
    let supervisor = runtime
        .cameras
        .read()
        .unwrap()
        .get("camera-a")
        .and_then(|slot| slot.supervisor.as_ref())
        .map(|supervisor| supervisor.cancellation.clone())
        .expect("the live camera must have a supervisor cancellation token");

    let listener = RuntimeConfigListener::new(
        Arc::downgrade(&runtime),
        Arc::new(|_instance, _config| {
            Err(edgecommons::EdgeCommonsError::Config(
                "test candidate facade construction failure".to_string(),
            ))
        }),
        Arc::new(|_instance, _config| -> edgecommons::Result<EventsFacade> {
            unreachable!("the application facade rejection occurs first")
        }),
    );
    let candidate = Arc::new(core_config(directory.path(), &["camera-a"], false));
    assert!(
        listener
            .prepare_configuration_apply(candidate)
            .await
            .is_err(),
        "a candidate that cannot build its required facades must be rejected before Core commits it"
    );
    assert!(
        !supervisor.is_cancelled(),
        "a rejected preflight must not cancel the previous service generation"
    );
    assert_eq!(
        runtime.registry.snapshot("camera-a").unwrap().generation,
        generation_before,
        "a rejected preflight must not advance the prior runtime generation"
    );

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "rejected-reload-still-serves".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "rejected-reload-still-serves-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("the prior service must continue accepting captures after rejection");
    let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
        panic!("the first capture after a rejected reload must be newly accepted");
    };
    assert_eq!(
        wait_for_terminal(&runtime, &record.capture_id).await.state,
        crate::model::JobState::Succeeded,
        "the prior service must also complete captures after rejection"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn failed_reload_transition_restores_prior_config_and_capture_service() {
    let directory = TempDir::new().unwrap();
    let mut initial = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(initial, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let retired_supervisor = runtime
        .cameras
        .read()
        .unwrap()
        .get("camera-a")
        .and_then(|slot| slot.supervisor.as_ref())
        .map(|supervisor| supervisor.cancellation.clone())
        .expect("the initial runtime must have a supervisor token");

    let mut replacement = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    sim.seed = Some(712);
    // Fail the commit the way it can actually fail.
    //
    // This used to be driven by a `#[cfg(test)] fail_next_reload_after_supervisor_retirement`
    // AtomicBool -- a field on the PRODUCTION `CameraRuntime` struct, read by a branch inside
    // the production reload commit. It injected a failure that cannot occur, at a point in
    // the sequence where no real failure lives, which meant the rollback being asserted here
    // had never once been driven by anything the runtime could actually do.
    //
    // The real post-retirement failure is `registry.apply_validated_config`, and it rejects
    // a roster with duplicate camera IDs. That check sits immediately after the supervisor
    // retirement barrier, which is exactly the window this test exists to cover, so a
    // duplicated instance drives the genuine path -- and the assertion below proves the
    // supervisors really were retired before it fired.
    let duplicate = replacement.instances[0].clone();
    replacement.instances.push(duplicate);
    let checkpoint = runtime.reload_checkpoint().unwrap();
    let mut transaction = RuntimeReloadTransaction {
        runtime: Arc::clone(&runtime),
        replacement,
        apps: BTreeMap::new(),
        events: BTreeMap::new(),
        checkpoint: Some(checkpoint),
    };

    assert!(
        transaction.commit().await.is_err(),
        "a roster the registry refuses must reject the candidate"
    );
    assert!(
        retired_supervisor.is_cancelled(),
        "the failure must land AFTER the retirement barrier -- otherwise this test would                  not be covering the post-retirement rollback it claims to cover"
    );
    // Core calls rollback after any failed commit. It is intentionally idempotent because
    // commit performed the restoration before returning its error.
    transaction.rollback().await.unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let crate::config::BackendConfig::Sim(sim) =
        &runtime.config_snapshot().unwrap().instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    assert_eq!(
        sim.seed, None,
        "the prior runtime configuration must be restored"
    );
    assert!(
        retired_supervisor.is_cancelled(),
        "rollback must retire the failed candidate's predecessor before installing a fresh prior supervisor"
    );
    assert!(
        !runtime
            .cameras
            .read()
            .unwrap()
            .get("camera-a")
            .and_then(|slot| slot.supervisor.as_ref())
            .is_some_and(|supervisor| supervisor.cancellation.is_cancelled()),
        "rollback must install a live prior-config supervisor rather than revive a cancelled actor"
    );

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "failed-transition-restored-service".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "failed-transition-restored-service-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("the restored prior service must accept captures");
    let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
        panic!("the first capture after rollback must be newly accepted");
    };
    assert_eq!(
        wait_for_terminal(&runtime, &record.capture_id).await.state,
        crate::model::JobState::Succeeded,
        "the restored prior service must complete captures"
    );
    runtime.shutdown().await;
}

#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn runtime_config_listener_refreshes_retained_facades_and_applies_roster_and_session_changes()
 {
    let directory = TempDir::new().unwrap();
    let (port, _) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let mut initial = config(directory.path(), &["camera-a", "camera-b"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(initial, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let generation_before = runtime.registry.snapshot("camera-a").unwrap().generation;

    let app_calls = Arc::new(Mutex::new(Vec::new()));
    let event_calls = Arc::new(Mutex::new(Vec::new()));
    let listener = RuntimeConfigListener::new(
        Arc::downgrade(&runtime),
        {
            let core = Arc::clone(&core);
            let app_calls = Arc::clone(&app_calls);
            Arc::new(move |instance, config| {
                app_calls.lock().unwrap().push(instance.to_string());
                Ok(Arc::new(
                    core.instance_from_config_snapshot(instance, config)?.app(),
                ))
            })
        },
        {
            let core = Arc::clone(&core);
            let event_calls = Arc::clone(&event_calls);
            Arc::new(move |instance, config| {
                event_calls.lock().unwrap().push(instance.to_string());
                Ok(core
                    .instance_from_config_snapshot(instance, config)?
                    .events())
            })
        },
    );

    let roster_candidate = Arc::new(core_config(
        directory.path(),
        &["camera-a", "camera-c"],
        false,
    ));
    let mut roster_transaction = listener
        .prepare_configuration_apply(roster_candidate)
        .await
        .unwrap();
    roster_transaction.commit().await.unwrap();
    assert_eq!(
        runtime.registry.ids().unwrap(),
        vec!["camera-a".to_string(), "camera-c".to_string()]
    );
    assert_eq!(
        app_calls.lock().unwrap().as_slice(),
        ["camera-a", "camera-c"]
    );
    assert_eq!(
        event_calls.lock().unwrap().as_slice(),
        ["camera-a", "camera-c"]
    );
    {
        let cameras = runtime.cameras.read().unwrap();
        assert_eq!(cameras.values().filter(|slot| slot.events.is_some()).count(), 2);
        assert_eq!(
            cameras
                .get("camera-a")
                .and_then(|slot| slot.events.as_ref())
                .map(EventsFacade::instance_id),
            Some("camera-a")
        );
        assert_eq!(
            cameras
                .get("camera-c")
                .and_then(|slot| slot.events.as_ref())
                .map(EventsFacade::instance_id),
            Some("camera-c")
        );
    }

    let mut session_replacement =
        core_config_value(directory.path(), &["camera-a", "camera-c"], false);
    session_replacement["component"]["instances"][0]["backend"]["seed"] = json!(919);
    let session_candidate =
        Arc::new(Config::from_value(COMPONENT_NAME, "gw-01", session_replacement).unwrap());
    let mut session_transaction = listener
        .prepare_configuration_apply(session_candidate)
        .await
        .unwrap();
    session_transaction.commit().await.unwrap();
    assert_eq!(
        app_calls.lock().unwrap().as_slice(),
        ["camera-a", "camera-c", "camera-a", "camera-c"],
        "every retained instance must receive a current core facade on every generation"
    );
    assert_eq!(
        event_calls.lock().unwrap().as_slice(),
        ["camera-a", "camera-c", "camera-a", "camera-c"],
        "retained event facades must be refreshed, not only newly added cameras"
    );
    wait_for_online(&runtime, "camera-a").await;
    assert!(
        runtime.registry.snapshot("camera-a").unwrap().generation > generation_before,
        "a same-kind backend change must replace the retained camera session"
    );
    assert!(runtime.registry.snapshot("camera-b").is_err());
    runtime.shutdown().await;
}

#[test]
fn cursor_store_preserves_page_boundaries_and_rejects_cross_query_reuse() {
    let cursors = CursorStore::default();
    let list_query = json!({ "includeCapabilities": false, "includeUnconfigured": true });
    let (cameras, unconfigured, next) = cursors
        .list_page(
            &list_query,
            None,
            Some((
                vec![json!({ "instance": "camera-a" })],
                vec![json!({ "selector": { "serial": "unconfigured" } })],
            )),
            1,
        )
        .unwrap();
    assert_eq!(cameras, vec![json!({ "instance": "camera-a" })]);
    assert!(unconfigured.is_empty());
    let list_cursor = next.expect("a second retained list item needs a continuation");

    let (cameras, unconfigured, next) = cursors
        .list_page(&list_query, Some(&list_cursor), None, 1)
        .unwrap();
    assert!(cameras.is_empty());
    assert_eq!(
        unconfigured,
        vec![json!({ "selector": { "serial": "unconfigured" } })]
    );
    assert!(next.is_none());
    assert_eq!(
        cursors
            .list_page(
                &json!({ "includeCapabilities": true, "includeUnconfigured": true }),
                Some(&list_cursor),
                None,
                1,
            )
            .unwrap_err()
            .code(),
        crate::ErrorCode::InvalidRequest,
        "a retained list cursor is bound to its original capability view"
    );

    let snapshot_query = json!({ "instance": "camera-a" });
    let (values, next, completed_at) = cursors
        .snapshot_page(
            "ptz-presets",
            &snapshot_query,
            None,
            Some(vec![json!({ "token": "one" }), json!({ "token": "two" })]),
            Some(json!("2026-07-11T00:00:00Z")),
            1,
        )
        .unwrap();
    assert_eq!(values, vec![json!({ "token": "one" })]);
    assert_eq!(completed_at, Some(json!("2026-07-11T00:00:00Z")));
    let snapshot_cursor = next.expect("a retained snapshot needs a continuation");
    let (values, next, completed_at) = cursors
        .snapshot_page(
            "ptz-presets",
            &snapshot_query,
            Some(&snapshot_cursor),
            None,
            None,
            1,
        )
        .unwrap();
    assert_eq!(values, vec![json!({ "token": "two" })]);
    assert!(next.is_none());
    assert_eq!(completed_at, Some(json!("2026-07-11T00:00:00Z")));
    assert_eq!(
        cursors
            .snapshot_page(
                "different-kind",
                &snapshot_query,
                Some(&snapshot_cursor),
                None,
                None,
                1,
            )
            .unwrap_err()
            .code(),
        crate::ErrorCode::InvalidRequest,
        "a retained snapshot cursor cannot be replayed under another operation"
    );

    let jobs_query = json!({ "instance": "camera-a", "states": ["SUCCEEDED"] });
    assert_eq!(cursors.job_before(&jobs_query, None).unwrap(), None);
    let jobs_cursor = cursors
        .next_job_cursor(&jobs_query, (42, "cap_stable_page_boundary".to_string()))
        .unwrap();
    assert_eq!(
        cursors.job_before(&jobs_query, Some(&jobs_cursor)).unwrap(),
        Some((42, "cap_stable_page_boundary".to_string()))
    );
    assert_eq!(
        cursors
            .job_before(
                &json!({ "instance": "camera-b", "states": ["SUCCEEDED"] }),
                Some(&jobs_cursor),
            )
            .unwrap_err()
            .code(),
        crate::ErrorCode::InvalidRequest,
        "a durable status cursor is bound to its original filter"
    );
}

/// The fleet queue bounds the COMPONENT, not just each camera -- a bound that never existed.
///
/// Queueing used to be per-camera only, so the real worst case was
/// `cameras x 2 x maxQueuedCapturesPerCamera` -- 2,048 descriptors at the design target --
/// with no single number capping it and nothing able to see the fleet's backlog at all.
#[tokio::test]
async fn the_fleet_queue_bounds_the_component_and_each_camera() {
    let directory = TempDir::new().unwrap();
    let base = config(directory.path(), &["camera-a"], false);
    let limits = crate::config::LimitsConfig {
        max_pending_captures: 3,
        max_queued_captures_per_camera: 2,
        ..base.global.limits.clone()
    };
    let scheduler = crate::dispatch::CaptureScheduler::new(&limits).unwrap();

    // The per-camera bound still holds.
    let first = CaptureDispatcher::reserve(&scheduler, "camera-a").unwrap();
    let second = CaptureDispatcher::reserve(&scheduler, "camera-a").unwrap();
    assert_eq!(
        match CaptureDispatcher::reserve(&scheduler, "camera-a") {
            Err(error) => error.code(),
            Ok(_) => panic!("a third capture must not exceed the per-camera bound"),
        },
        crate::ErrorCode::QueueFull,
    );

    // And now the fleet bound holds too: camera-b may queue one, then the COMPONENT is full,
    // even though camera-c has queued nothing at all.
    let third = CaptureDispatcher::reserve(&scheduler, "camera-b").unwrap();
    assert_eq!(
        match CaptureDispatcher::reserve(&scheduler, "camera-c") {
            Err(error) => error.code(),
            Ok(_) => panic!("the component's own backlog bound must hold"),
        },
        crate::ErrorCode::QueueFull,
        "a camera that has queued nothing must still be refused once the FLEET is full --                  that is the bound the component never had"
    );
    assert_eq!(scheduler.pending(), 3);
    assert_eq!(scheduler.pending_for("camera-a"), 2);

    // Dropping an uncommitted reservation returns its slot, to both bounds.
    drop(second);
    assert_eq!(scheduler.pending(), 2);
    assert_eq!(scheduler.pending_for("camera-a"), 1);
    assert!(CaptureDispatcher::reserve(&scheduler, "camera-c").is_ok());
    drop((first, third));
}

#[tokio::test]
async fn simulator_runtime_rejects_invalid_requests_without_creating_durable_work() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    configuration.instances[1].enabled = false;
    let runtime = runtime(configuration, &directory).await;

    assert_eq!(
        runtime
            .submit_capture(
                "missing-camera".to_string(),
                "unknown-instance".to_string(),
                None,
                None,
                serde_json::Map::new(),
                "invalid-request-test".to_string(),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::UnknownInstance
    );
    assert_eq!(
        runtime
            .submit_capture(
                "camera-a".to_string(),
                "unknown-profile".to_string(),
                Some("not-configured".to_string()),
                None,
                serde_json::Map::new(),
                "invalid-request-test".to_string(),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::UnknownCaptureProfile
    );

    let disabled_group: GroupCaptureRequest = commands::parse_closed(json!({
        "requestId": "disabled-member-group",
        "instances": ["camera-a", "camera-b"]
    }))
    .unwrap();
    assert_eq!(
        runtime
            .submit_group(
                disabled_group,
                "invalid-request-test".to_string(),
                crate::admission::CapturePriority::Submitted,
                None,
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::CameraDisabled,
        "group acceptance must validate every member before writing a group record"
    );
    assert!(
        runtime
            .catalog
            .group_by_ledger(
                crate::catalog::LedgerKey::new(
                    "main",
                    "sb/capture-group",
                    "disabled-member-group",
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .is_none(),
        "a rejected group must not leave an idempotency row or partial durable group"
    );

    assert_eq!(
        runtime
            .discover(DiscoverRequest {
                backends: Vec::new(),
                timeout_ms: 100,
                limit: 1,
                cursor: None,
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::UnsupportedCapability,
        "discovery respects the explicit configuration gate"
    );
    assert_eq!(
        runtime
            .cancel_capture(CancelRequest {
                request_id: "missing-capture-cancel".to_string(),
                capture_id: Some("cap_missing".to_string()),
                capture_group_id: None,
                reason: None,
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::CaptureNotFound
    );
    assert_eq!(
        runtime
            .reconnect(ReconnectRequest {
                instance: None,
                request_id: "ambiguous-reconnect".to_string(),
                reason: None,
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::InstanceRequired,
        "multi-camera reconnect requires an explicit target"
    );
    assert_eq!(
        runtime
            .reconnect(ReconnectRequest {
                instance: Some("camera-b".to_string()),
                request_id: "disabled-reconnect".to_string(),
                reason: None,
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::CameraDisabled
    );
    assert_eq!(
        runtime
            .perform_presets(
                commands::parse_closed(json!({
                    "operation": "list",
                    "instance": "camera-a",
                    "limit": 1
                }))
                .unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::PtzDisabled
    );
    assert!(
        runtime
            .catalog
            .jobs_page(None, Vec::new(), None, 10)
            .await
            .unwrap()
            .is_empty(),
        "rejected direct requests must not allocate capture jobs"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn simulator_runtime_submission_retries_replay_existing_work_and_reject_changes() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(configuration, &directory).await;

    let metadata = serde_json::Map::from_iter([(
        "lot".to_string(),
        serde_json::Value::String("A-17".to_string()),
    )]);
    let first = runtime
        .submit_capture(
            "camera-a".to_string(),
            "submitted-idempotency".to_string(),
            None,
            Some(30_000),
            metadata.clone(),
            "original-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let first_capture_id = match first {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("first submitted capture must insert durable work, got {other:?}"),
    };
    let retry = runtime
        .submit_capture(
            "camera-a".to_string(),
            "submitted-idempotency".to_string(),
            None,
            Some(30_000),
            metadata.clone(),
            "retry-correlation-must-not-replace-origin".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    assert!(matches!(
        retry,
        crate::catalog::AcceptJobOutcome::Existing(record)
            if record.capture_id == first_capture_id
                && record.origin_correlation_id.as_deref() == Some("original-correlation")
    ));
    let mut changed_metadata = metadata;
    changed_metadata.insert(
        "lot".to_string(),
        serde_json::Value::String("B-18".to_string()),
    );
    assert!(matches!(
        runtime
            .submit_capture(
                "camera-a".to_string(),
                "submitted-idempotency".to_string(),
                None,
                Some(30_000),
                changed_metadata,
                "changed-correlation".to_string(),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .unwrap(),
        crate::catalog::AcceptJobOutcome::Conflict
    ));

    let group_request = GroupCaptureRequest {
        request_id: "group-idempotency".to_string(),
        instances: vec!["camera-a".to_string(), "camera-b".to_string()],
        capture_profile: None,
        profile_overrides: BTreeMap::new(),
        timeout_ms: Some(30_000),
        metadata: serde_json::Map::new(),
    };
    let group = runtime
        .submit_group(
            group_request.clone(),
            "original-group-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap();
    let replay = runtime
        .submit_group(
            group_request.clone(),
            "retry-group-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap();
    assert_eq!(replay.group_id, group.group_id);
    assert_eq!(
        replay.origin_correlation_id.as_deref(),
        Some("original-group-correlation")
    );
    let mut changed_group = group_request;
    changed_group.timeout_ms = Some(31_000);
    assert_eq!(
        runtime
            .submit_group(
                changed_group,
                "changed-group-correlation".to_string(),
                crate::admission::CapturePriority::Submitted,
                None,
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::IdempotencyConflict
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn simulator_runtime_scheduled_admission_terminalizes_and_deduplicates_occurrences() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], true);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let intended_fire_time = chrono::Utc::now();
    let occurrence = ScheduleOccurrence {
        scope: crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
        schedule_id: "minute".to_string(),
        intended_fire_time,
        admit_at: intended_fire_time,
        jitter: Duration::ZERO,
    };
    runtime.submit_scheduled(&occurrence).await.unwrap();
    let jobs = runtime
        .catalog
        .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);
    let capture_id = jobs[0].capture_id.clone();
    assert_eq!(jobs[0].trigger["type"], "schedule");
    assert_eq!(jobs[0].trigger["scheduleId"], "minute");
    assert_eq!(
        wait_for_terminal(&runtime, &capture_id).await.state,
        crate::model::JobState::Succeeded
    );
    assert!(
        !runtime
            .has_schedule_overlap("camera-a", "minute")
            .await
            .unwrap()
    );

    runtime.submit_scheduled(&occurrence).await.unwrap();
    let duplicate_page = runtime
        .catalog
        .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
        .await
        .unwrap();
    assert_eq!(duplicate_page.len(), 1);
    assert_eq!(duplicate_page[0].capture_id, capture_id);
    assert_eq!(
        runtime
            .catalog
            .latest_schedule_occurrence("camera-a", "minute")
            .await
            .unwrap(),
        Some(intended_fire_time.timestamp_millis())
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn simulator_runtime_schedule_loop_skips_overlapping_occurrences() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], true);
    configuration.instances[0].schedules[0].cron = "* * * * * *".to_string();
    let plan = SchedulePlan::compile(
        "camera-a".to_string(),
        &configuration.instances[0].schedules[0],
    )
    .unwrap();
    let runtime = runtime(configuration, &directory).await;
    let cancellation = CancellationToken::new();
    let runner =
        tokio::spawn(Arc::clone(&runtime).run_schedule(plan, cancellation.clone()));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if !runtime
            .catalog
            .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
            .await
            .unwrap()
            .is_empty()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the one-second schedule did not durably admit an occurrence"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        runtime
            .has_schedule_overlap("camera-a", "minute")
            .await
            .unwrap()
    );
    tokio::time::sleep(Duration::from_millis(1_250)).await;
    cancellation.cancel();
    runner.await.unwrap();
    assert_eq!(
        runtime
            .catalog
            .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
            .await
            .unwrap()
            .len(),
        1,
        "an overlap-policy skip must consume later fires without admitting a second job"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn simulator_runtime_moving_reject_interlock_skips_scheduled_admission() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], true);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    sim.ptz.supported = true;
    sim.ptz.status_supported = true;
    configuration.instances[0].ptz.enabled = true;
    configuration.instances[0]
        .capture_profiles
        .get_mut("main")
        .unwrap()
        .capture_interlock = Some(crate::config::CaptureInterlock::Reject);
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    runtime
        .actor("camera-a")
        .unwrap()
        .ptz(
            crate::model::PtzRequest::Continuous {
                velocity: crate::model::PtzVector {
                    pan: 0.5,
                    tilt: 0.0,
                    zoom: 0.0,
                },
                timeout: Duration::from_secs(2),
            },
            tokio::time::Instant::now() + Duration::from_secs(1),
            &runtime.cancellation,
        )
        .await
        .unwrap();
    let occurrence = ScheduleOccurrence {
        scope: crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
        schedule_id: "minute".to_string(),
        intended_fire_time: chrono::Utc::now(),
        admit_at: chrono::Utc::now(),
        jitter: Duration::ZERO,
    };
    runtime.submit_scheduled(&occurrence).await.unwrap();
    assert!(
        runtime
            .catalog
            .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
            .await
            .unwrap()
            .is_empty(),
        "a moving camera with reject interlock must not create a scheduled capture"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn runtime_startup_router_and_deferred_capture_flows_use_real_core_facades() {
    let directory = TempDir::new().unwrap();
    let router = RuntimeCommandRouter::new();
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let core = facade_core_with_router(&directory, port, Some(Arc::clone(&router))).await;
    let inbox = core
        .commands()
        .expect("the MQTT core fixture must expose a command inbox");
    for verb in camera_command_verbs() {
        assert!(
            inbox.verbs().contains(verb),
            "router registration omitted required camera verb {verb}"
        );
    }
    let deferred = inbox.deferred_replies();

    match router
        .dispatch(
            "sb/list",
            command_message("sb/list", "before-runtime", json!({ "limit": 1 })),
            deferred.clone(),
        )
        .await
    {
        CommandOutcome::ImmediateError(error) => {
            assert_eq!(error.code, "CAMERA_UNAVAILABLE");
        }
        other => panic!("uninstalled router must reject requests, got {other:?}"),
    }

    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    configuration.global.state.directory = Some(
        directory
            .path()
            .join("startup-state")
            .to_string_lossy()
            .into_owned(),
    );
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    let resources = prepare_startup_resources(&configuration, Platform::Host)
        .await
        .unwrap();
    let mut apps = BTreeMap::new();
    let mut events = BTreeMap::new();
    for camera in &configuration.instances {
        let instance = core.instance(&camera.id).unwrap();
        apps.insert(camera.id.clone(), Arc::new(instance.app()));
        events.insert(camera.id.clone(), instance.events());
    }
    let core_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readiness = {
        let core_ready = Arc::clone(&core_ready);
        RuntimeReadiness::new(Arc::new(move |ready| {
            core_ready.store(ready, std::sync::atomic::Ordering::Release);
        }))
    };
    let runtime = CameraRuntime::start(
        configuration,
        resources,
        RuntimeServices {
            apps,
            events,
            component_events: core.events(),
            metrics: Arc::new(RecordingMetrics::default()),
            // A standalone/HOST runtime is an MQTT transport.
            transport: edgecommons::platform::Transport::Mqtt,
            readiness: readiness.clone(),
            backend_context: BackendRuntimeContext::new(
                None,
                &crate::config::LimitsConfig::default(),
            ),
        },
    )
    .await
    .unwrap();
    for instance in ["camera-a", "camera-b"] {
        wait_for_online(&runtime, instance).await;
    }
    router.install(runtime.clone()).unwrap();
    readiness.complete_startup();
    assert!(
        core_ready.load(std::sync::atomic::Ordering::Acquire),
        "readiness must remain closed until runtime installation then become true"
    );

    let direct_publish_index = publishes.lock().unwrap().len();
    let direct_continuation = match router
        .dispatch(
            "sb/capture",
            command_message(
                "sb/capture",
                "startup-direct",
                json!({ "instance": "camera-a", "requestId": "startup-direct" }),
            ),
            deferred.clone(),
        )
        .await
    {
        CommandOutcome::DeferredWithContinuation { continuation, .. } => continuation,
        other => panic!("direct capture must use deferred hand-off, got {other:?}"),
    };
    direct_continuation.await.unwrap();
    let direct = runtime
        .catalog
        .job_by_ledger(
            crate::catalog::LedgerKey::new("camera-a", "sb/capture", "startup-direct")
                .unwrap(),
        )
        .await
        .unwrap()
        .expect("deferred direct capture must be durably accepted");
    assert_eq!(
        wait_for_terminal(&runtime, &direct.capture_id).await.state,
        crate::model::JobState::Succeeded
    );
    let direct_reply = wait_for_recorded_reply(&publishes, direct_publish_index).await;
    assert_eq!(direct_reply.body["ok"], true);
    assert_eq!(direct_reply.body["result"]["captureId"], direct.capture_id);

    let group_publish_index = publishes.lock().unwrap().len();
    let group_continuation = match router
        .dispatch(
            "sb/capture-group",
            command_message(
                "sb/capture-group",
                "startup-group",
                json!({
                    "requestId": "startup-group",
                    "instances": ["camera-a", "camera-b"]
                }),
            ),
            deferred.clone(),
        )
        .await
    {
        CommandOutcome::DeferredWithContinuation { continuation, .. } => continuation,
        other => panic!("direct group capture must use deferred hand-off, got {other:?}"),
    };
    group_continuation.await.unwrap();
    let group = runtime
        .catalog
        .group_by_ledger(
            crate::catalog::LedgerKey::new("main", "sb/capture-group", "startup-group")
                .unwrap(),
        )
        .await
        .unwrap()
        .expect("deferred group capture must be durably accepted");
    let group = wait_for_group_terminal(&runtime, &group.group_id).await;
    assert_eq!(group.members.len(), 2);
    assert!(
        group
            .members
            .iter()
            .all(|member| member.state == crate::model::JobState::Succeeded)
    );
    let group_reply = wait_for_recorded_reply(&publishes, group_publish_index).await;
    assert_eq!(group_reply.body["ok"], true);
    assert_eq!(group_reply.body["result"]["captureGroupId"], group.group_id);
    assert_eq!(
        group_reply.body["result"]["members"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    router.begin_shutdown();
    match router
        .dispatch(
            "sb/list",
            command_message("sb/list", "after-shutdown", json!({ "limit": 1 })),
            deferred,
        )
        .await
    {
        CommandOutcome::ImmediateError(error) => {
            assert_eq!(error.code, "COMPONENT_STOPPING");
        }
        other => panic!("stopping router must reject requests, got {other:?}"),
    }
    runtime.shutdown().await;
    assert!(
        !core_ready.load(std::sync::atomic::Ordering::Acquire),
        "runtime shutdown must close the readiness bridge"
    );
    inbox.stop().await;
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
const SHORT_CAPACITY_CONFIGURED_CAMERAS: usize = 1_024;
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
const SHORT_CAPACITY_ENABLED_CAMERAS: usize = 256;
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
const SHORT_CAPACITY_CONCURRENT_CAPTURES: usize = 32;
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
const SHORT_CAPACITY_FRAME_WIDTH: u32 = 3_264;
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
const SHORT_CAPACITY_FRAME_HEIGHT: u32 = 2_448;
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
const SHORT_CAPACITY_FRAME_BYTES: u64 =
    (SHORT_CAPACITY_FRAME_WIDTH as u64) * (SHORT_CAPACITY_FRAME_HEIGHT as u64);
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
const IDLE_SESSION_RSS_MAXIMUM_FULL_FRAME_FRACTION_DENOMINATOR: u64 = 8;
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
/// The modeled sensor-plus-transfer latency a simulated 8MP capture holds its permit for.
///
/// H2: this used to be 5,000 ms — five seconds of bare `sleep` per capture, a device for freezing 32
/// captures in the admission gate so the harness could photograph "32 concurrent". It froze 32
/// *reservations*, not 32 *frames*: nothing was on the heap. 750 ms is a defensible readout+transfer
/// time for a slow high-resolution industrial sensor, it is comfortably long enough to sample the
/// concurrent window, and — now that `SimSession::capture` holds the real frame buffer across it — it
/// is 750 ms during which 32 genuine 8-megapixel buffers are resident at once.
const SHORT_CAPACITY_TRANSFER_DELAY_MS: u64 = 750;
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
/// The least resident growth 32 concurrent 8MP captures must produce, as a fraction of their combined
/// frame bytes. Half leaves generous headroom for allocator behaviour and for captures at the edges of
/// the window that have not yet allocated or have already handed their buffer on; it is still far above
/// the ZERO the old sleep-only harness would have shown.
const CONCURRENT_CAPTURE_MEMORY_MINIMUM_FRACTION_DENOMINATOR: u64 = 2;

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CapacityProcessStats {
    rss_bytes: Option<u64>,
    thread_count: Option<u64>,
    open_file_descriptors: Option<u64>,
    /// Total CPU time (user + system) the process has consumed, in microseconds.
    ///
    /// H5: RSS alone cannot tell a healthy idle-between-captures runtime from one pinned at 100% in a
    /// poll storm — the two look identical in resident memory. This is the difference. Sampled over
    /// the run, its slope is the process's CPU utilization, and a runtime that has regressed into a
    /// busy-wait (the class of defect D1 and B6 were) shows it here and nowhere else.
    cpu_micros: Option<u64>,
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CapacitySample {
    phase: String,
    elapsed_millis: u64,
    configured_cameras: usize,
    enabled_cameras: usize,
    online_cameras: usize,
    live_actor_count: usize,
    queued_capture_descriptors: usize,
    queued_control_descriptors: usize,
    available_global_acquisitions: usize,
    available_resource_group_acquisitions: BTreeMap<String, usize>,
    available_in_flight_bytes: u64,
    outstanding_disk_bytes: u64,
    available_encoders: usize,
    available_writers: usize,
    process: CapacityProcessStats,
    /// The catalog's total footprint on disk — the SQLite database and its WAL/SHM/journal
    /// sidecars, summed — in bytes.
    ///
    /// H5: the review said the harness "cannot catch B1", the finding where the durable store grows
    /// without bound because retention was wired to nothing. `outstandingDiskBytes` is an admission
    /// *reservation counter*, not a measurement of the store; it says how much the component intends
    /// to write, never how large the catalog has actually become. This is a `stat` of the real files,
    /// sampled over the run, so the durable cost of a capture is finally observable — and its per-record
    /// slope is asserted, which is exactly the signal a per-capture-envelope regression would move.
    catalog_disk_bytes: Option<u64>,
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CommandLatencySummary {
    samples: usize,
    minimum_micros: u64,
    p50_micros: u64,
    p95_micros: u64,
    maximum_micros: u64,
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IdleSessionMemoryEvidence {
    baseline_rss_bytes: u64,
    startup_peak_rss_bytes: u64,
    roster_online_rss_bytes: u64,
    startup_peak_delta_bytes: u64,
    roster_online_delta_bytes: u64,
    full_frame_allocation_equivalent_bytes: u64,
    maximum_allowed_delta_bytes: u64,
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ShortCapacityArtifact {
    schema_version: &'static str,
    scope: &'static str,
    configured_cameras: usize,
    enabled_simulated_sessions: usize,
    concurrent_capture_target: usize,
    frame: serde_json::Value,
    idle_session_memory: IdleSessionMemoryEvidence,
    concurrent_capture_memory: serde_json::Value,
    capture_group_submit_micros: u64,
    command_latency: BTreeMap<String, CommandLatencySummary>,
    resource_samples: Vec<CapacitySample>,
    group_terminal_state: String,
    group_successful_members: usize,
    overflow_capture_terminal_state: String,
    omitted_from_this_short_run: Vec<&'static str>,
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
fn short_capacity_configuration(root: &Path) -> AdapterConfig {
    let output_root = root.join("capacity-output");
    let state_root = root.join("capacity-state");
    let instances = (0..SHORT_CAPACITY_CONFIGURED_CAMERAS)
        .map(|index| {
            json!({
                "id": format!("camera-{index:04}"),
                "enabled": index < SHORT_CAPACITY_ENABLED_CAMERAS,
                "resourceGroup": "sim-shared",
                "backend": {
                    "type": "sim",
                    "captureDelayMs": SHORT_CAPACITY_TRANSFER_DELAY_MS,
                    "frame": {
                        "width": SHORT_CAPACITY_FRAME_WIDTH,
                        "height": SHORT_CAPACITY_FRAME_HEIGHT,
                        "pixelFormat": "Mono8",
                        "pattern": "checkerboard"
                    },
                    "ptz": { "supported": true, "statusSupported": true }
                },
                "ptz": { "enabled": true },
                "defaultCaptureProfile": "main",
                "captureProfiles": {
                    "main": {
                        "maximumFrameBytes": SHORT_CAPACITY_FRAME_BYTES,
                        "output": { "encoding": "raw" }
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    let raw = json!({
        "component": {
            "global": {
                "output": {
                    "rootDirectory": output_root,
                    "minimumFreeBytes": 0,
                    "minimumFreePercent": 0
                },
                "state": { "directory": state_root },
                "limits": {
                    "maxConnectedCameras": SHORT_CAPACITY_ENABLED_CAMERAS,
                    "maxConcurrentCaptures": SHORT_CAPACITY_CONCURRENT_CAPTURES,
                    "maxConcurrentEncodes": 8,
                    "maxConcurrentWrites": 8,
                    "maxConcurrentConnects": 16,
                    "maxInFlightBytes": SHORT_CAPACITY_FRAME_BYTES * SHORT_CAPACITY_CONCURRENT_CAPTURES as u64,
                    "maxFrameBytesPerCamera": SHORT_CAPACITY_FRAME_BYTES,
                    "maxCamerasPerGroup": SHORT_CAPACITY_CONCURRENT_CAPTURES,
                    "resourceGroups": {
                        "sim-shared": {
                            "maxConcurrentCaptures": SHORT_CAPACITY_CONCURRENT_CAPTURES
                        }
                    }
                }
            },
            "instances": instances
        }
    });
    AdapterConfig::from_core_reload(
        &Config::from_value(COMPONENT_NAME, "capacity-lab", raw)
            .expect("short capacity configuration must be structurally valid"),
    )
    .expect("short capacity configuration must satisfy adapter limits")
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
async fn wait_for_capacity_roster(runtime: &CameraRuntime) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut peak_rss_bytes = 0_u64;
    loop {
        let rss_bytes = capacity_process_stats().rss_bytes.expect(
            "Linux capacity proof requires /proc/self/status VmRSS while supervisors start",
        );
        peak_rss_bytes = peak_rss_bytes.max(rss_bytes);
        let snapshots = runtime
            .registry
            .snapshots(SHORT_CAPACITY_CONFIGURED_CAMERAS)
            .expect("capacity registry must remain readable");
        let online = snapshots
            .iter()
            .filter(|snapshot| snapshot.state == CameraConnectionState::Online)
            .count();
        let disabled = snapshots
            .iter()
            .filter(|snapshot| snapshot.state == CameraConnectionState::Disabled)
            .count();
        let live_actor_count = runtime
            .cameras
            .read()
            .expect("capacity camera registry lock must remain readable")
            .values()
            .filter(|slot| slot.session.is_some())
            .count();
        if snapshots.len() == SHORT_CAPACITY_CONFIGURED_CAMERAS
            && online == SHORT_CAPACITY_ENABLED_CAMERAS
            && disabled
                == SHORT_CAPACITY_CONFIGURED_CAMERAS - SHORT_CAPACITY_ENABLED_CAMERAS
            && live_actor_count == SHORT_CAPACITY_ENABLED_CAMERAS
        {
            return peak_rss_bytes;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "short capacity roster did not reach 256 ONLINE live actors and 768 DISABLED cameras; online={online}, liveActors={live_actor_count}, disabled={disabled}, total={}",
            snapshots.len(),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
fn proc_status_value(key: &str) -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            let value = line.strip_prefix(key)?.split_whitespace().next()?;
            value.parse::<u64>().ok()
        })
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
fn capacity_process_stats() -> CapacityProcessStats {
    CapacityProcessStats {
        rss_bytes: proc_status_value("VmRSS:").and_then(|kib| kib.checked_mul(1024)),
        thread_count: proc_status_value("Threads:"),
        open_file_descriptors: fs::read_dir("/proc/self/fd")
            .ok()
            .map(|entries| entries.filter_map(std::result::Result::ok).count() as u64),
        cpu_micros: proc_self_cpu_micros(),
    }
}

/// Total CPU microseconds (user + system) from `/proc/self/stat`.
///
/// The `comm` field (field 2) can contain spaces and parentheses, so the record is split on the LAST
/// `)`; utime and stime are then the 12th and 13th whitespace tokens of the remainder (fields 14 and
/// 15 of the record). Both are in clock ticks of `USER_HZ`, which is 100 on every Linux this runs on —
/// so one tick is 10,000 microseconds.
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
fn proc_self_cpu_micros() -> Option<u64> {
    const MICROS_PER_TICK: u64 = 10_000;
    let stat = fs::read_to_string("/proc/self/stat").ok()?;
    let after_comm = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    // After the ')' the first token is `state` (field 3), so record field 14 (utime) is index 11.
    let utime = fields.get(11)?.parse::<u64>().ok()?;
    let stime = fields.get(12)?.parse::<u64>().ok()?;
    utime.checked_add(stime)?.checked_mul(MICROS_PER_TICK)
}

/// The catalog's total on-disk footprint: the SQLite database plus its WAL/SHM/journal sidecars.
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
fn catalog_disk_bytes(state_directory: &Path) -> Option<u64> {
    let mut total = 0_u64;
    let mut saw_one = false;
    for name in [
        "camera-adapter.sqlite3",
        "camera-adapter.sqlite3-wal",
        "camera-adapter.sqlite3-shm",
        "camera-adapter.sqlite3-journal",
    ] {
        if let Ok(metadata) = fs::metadata(state_directory.join(name)) {
            total = total.saturating_add(metadata.len());
            saw_one = true;
        }
    }
    saw_one.then_some(total)
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
fn capacity_sample(
    runtime: &CameraRuntime,
    phase: &str,
    started: Instant,
    state_directory: &Path,
) -> CapacitySample {
    let admission = runtime.admission.snapshot();
    let snapshots = runtime
        .registry
        .snapshots(SHORT_CAPACITY_CONFIGURED_CAMERAS)
        .expect("capacity registry must remain readable");
    let cameras = runtime
        .cameras
        .read()
        .expect("capacity camera map must remain readable");
    let live_sessions = || cameras.values().filter_map(|slot| slot.session.as_ref());
    let live_actor_count = live_sessions().count();
    let queued_capture_descriptors = live_sessions()
        .map(|session| session.actor.queued_captures())
        .sum();
    let queued_control_descriptors = live_sessions()
        .map(|session| session.actor.queued_controls())
        .sum();
    CapacitySample {
        phase: phase.to_owned(),
        elapsed_millis: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        configured_cameras: snapshots.len(),
        enabled_cameras: snapshots.iter().filter(|snapshot| snapshot.enabled).count(),
        online_cameras: snapshots
            .iter()
            .filter(|snapshot| snapshot.state == CameraConnectionState::Online)
            .count(),
        live_actor_count,
        queued_capture_descriptors,
        queued_control_descriptors,
        available_global_acquisitions: admission.available_acquisitions,
        available_resource_group_acquisitions: admission
            .available_resource_group_acquisitions,
        available_in_flight_bytes: admission.available_memory_bytes,
        outstanding_disk_bytes: admission.outstanding_disk_bytes,
        available_encoders: admission.available_encoders,
        available_writers: admission.available_writers,
        process: capacity_process_stats(),
        catalog_disk_bytes: catalog_disk_bytes(state_directory),
    }
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
fn summarize_latency(mut values: Vec<u64>) -> CommandLatencySummary {
    values.sort_unstable();
    let sample_count = values.len();
    assert!(
        sample_count > 0,
        "capacity command timing series must not be empty"
    );
    let p95_index = (sample_count * 95).div_ceil(100).saturating_sub(1);
    CommandLatencySummary {
        samples: sample_count,
        minimum_micros: values[0],
        p50_micros: values[(sample_count - 1) / 2],
        p95_micros: values[p95_index],
        maximum_micros: values[sample_count - 1],
    }
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
async fn time_immediate_command(
    router: &RuntimeCommandRouter,
    deferred: &DeferredReplyRegistry,
    verb: &'static str,
    suffix: &str,
    body: serde_json::Value,
) -> u64 {
    let started = Instant::now();
    let _ = immediate_success(
        router
            .dispatch(verb, command_message(verb, suffix, body), deferred.clone())
            .await,
    );
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
fn capacity_artifact_directory() -> PathBuf {
    std::env::var_os("CAMERA_ADAPTER_CAPACITY_ARTIFACT_DIR")
        .map(PathBuf::from)
        .expect(
            "set CAMERA_ADAPTER_CAPACITY_ARTIFACT_DIR to an explicit empty artifact directory",
        )
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
fn write_capacity_artifact<T: Serialize>(directory: &Path, file_name: &str, artifact: &T) {
    fs::create_dir_all(directory).expect("capacity artifact directory must be creatable");
    let destination = directory.join(file_name);
    assert!(
        !destination.exists(),
        "refusing to overwrite existing capacity evidence: {}",
        destination.display()
    );
    let temporary = directory.join(format!(
        ".short-capacity-summary-{}.tmp",
        uuid::Uuid::now_v7()
    ));
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(artifact)
            .expect("capacity artifact must serialize to JSON"),
    )
    .expect("capacity artifact temporary file must be writable");
    fs::rename(&temporary, &destination)
        .expect("capacity artifact must atomically install in its requested directory");
}

/// Short Linux-only capacity proof for a simulated fleet.
///
/// It is intentionally ignored so routine local test runs do not create 33 eight-megapixel
/// images. The companion Linux runner provides an explicit artifact directory. This proves
/// the roster and admission slice only; it is not the deferred 24-hour soak or a hardware
/// compatibility test.
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "short Linux capacity evidence; run simulators/run-capacity-validation.sh"]
async fn short_linux_capacity_proves_1024_configured_256_sessions_and_32_captures() {
    let artifact_directory = capacity_artifact_directory();
    let temporary_root = TempDir::new().expect("capacity test root must be creatable");
    let state_directory = temporary_root.path().join("capacity-state");
    let configuration = short_capacity_configuration(temporary_root.path());
    fs::create_dir_all(&configuration.global.output.root_directory)
        .expect("capacity output root must exist before secure storage initialization");
    let instance_ids = configuration
        .instances
        .iter()
        .map(|camera| camera.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(instance_ids.len(), SHORT_CAPACITY_CONFIGURED_CAMERAS);
    assert_eq!(
        configuration
            .instances
            .iter()
            .filter(|camera| camera.enabled)
            .count(),
        SHORT_CAPACITY_ENABLED_CAMERAS
    );

    let router = RuntimeCommandRouter::new();
    let (port, _) = spawn_recording_mqtt_broker().await;
    let core = facade_core_with_router_instances(
        &temporary_root,
        port,
        Some(Arc::clone(&router)),
        &instance_ids,
    )
    .await;
    let inbox = core
        .commands()
        .expect("capacity fixture Core must expose the command inbox");
    let deferred = inbox.deferred_replies();
    let resources = prepare_startup_resources(&configuration, Platform::Host)
        .await
        .expect("capacity startup resources must be durable and valid");
    let mut apps = BTreeMap::new();
    let mut events = BTreeMap::new();
    for instance in &instance_ids {
        let instance_facade = core
            .instance(instance)
            .expect("configured capacity instance must build Core facades");
        apps.insert(instance.clone(), Arc::new(instance_facade.app()));
        events.insert(instance.clone(), instance_facade.events());
    }
    let idle_session_baseline_rss_bytes = capacity_process_stats().rss_bytes.expect(
        "Linux capacity proof requires /proc/self/status VmRSS before runtime startup",
    );
    let startup_rss_peak = Arc::new(AtomicU64::new(idle_session_baseline_rss_bytes));
    let startup_sampling_cancellation = CancellationToken::new();
    let startup_sampler = {
        let startup_rss_peak = Arc::clone(&startup_rss_peak);
        let cancellation = startup_sampling_cancellation.clone();
        tokio::spawn(async move {
            loop {
                if let Some(rss_bytes) = capacity_process_stats().rss_bytes {
                    startup_rss_peak.fetch_max(rss_bytes, Ordering::AcqRel);
                }
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
            }
        })
    };
    let readiness = RuntimeReadiness::noop();
    let runtime = CameraRuntime::start(
        configuration,
        resources,
        RuntimeServices {
            apps,
            events,
            component_events: core.events(),
            metrics: Arc::new(RecordingMetrics::default()),
            // A standalone/HOST capacity runtime is an MQTT transport.
            transport: edgecommons::platform::Transport::Mqtt,
            readiness,
            backend_context: BackendRuntimeContext::new(
                None,
                &crate::config::LimitsConfig::default(),
            ),
        },
    )
    .await
    .expect("capacity runtime must start all configured supervisors");
    router
        .install(runtime.clone())
        .expect("capacity router must install exactly one complete runtime");
    let started = Instant::now();
    let roster_poll_peak_rss_bytes = wait_for_capacity_roster(&runtime).await;
    startup_sampling_cancellation.cancel();
    startup_sampler
        .await
        .expect("capacity startup RSS sampler must not panic");
    let roster_online_sample = capacity_sample(&runtime, "roster-online", started, &state_directory);
    let roster_online_rss_bytes = roster_online_sample.process.rss_bytes.expect(
        "Linux capacity proof requires /proc/self/status VmRSS after roster startup",
    );
    let full_frame_allocation_equivalent_bytes = SHORT_CAPACITY_FRAME_BYTES
        .checked_mul(SHORT_CAPACITY_ENABLED_CAMERAS as u64)
        .expect("configured full-frame equivalent must fit in u64");
    let maximum_allowed_delta_bytes = full_frame_allocation_equivalent_bytes
        / IDLE_SESSION_RSS_MAXIMUM_FULL_FRAME_FRACTION_DENOMINATOR;
    let startup_peak_rss_bytes = startup_rss_peak
        .load(Ordering::Acquire)
        .max(roster_poll_peak_rss_bytes)
        .max(roster_online_rss_bytes);
    let startup_peak_delta_bytes =
        startup_peak_rss_bytes.saturating_sub(idle_session_baseline_rss_bytes);
    let roster_online_delta_bytes =
        roster_online_rss_bytes.saturating_sub(idle_session_baseline_rss_bytes);
    assert!(
        startup_peak_delta_bytes <= maximum_allowed_delta_bytes,
        "256 idle SimBackend sessions increased startup RSS by {startup_peak_delta_bytes} bytes; this must remain at most one eighth of their {full_frame_allocation_equivalent_bytes}-byte full-frame equivalent"
    );
    let idle_session_memory = IdleSessionMemoryEvidence {
        baseline_rss_bytes: idle_session_baseline_rss_bytes,
        startup_peak_rss_bytes,
        roster_online_rss_bytes,
        startup_peak_delta_bytes,
        roster_online_delta_bytes,
        full_frame_allocation_equivalent_bytes,
        maximum_allowed_delta_bytes,
    };
    let mut samples = vec![roster_online_sample];

    let capture_instances = instance_ids
        .iter()
        .take(SHORT_CAPACITY_CONCURRENT_CAPTURES)
        .cloned()
        .collect::<Vec<_>>();
    let accepted_at = Instant::now();
    let group_response = immediate_success(
        router
            .dispatch(
                "sb/capture-group-submit",
                command_message(
                    "sb/capture-group-submit",
                    "short-capacity-group",
                    json!({
                        "requestId": "short-capacity-group",
                        "instances": capture_instances
                    }),
                ),
                deferred.clone(),
            )
            .await,
    );
    let capture_group_submit_micros =
        u64::try_from(accepted_at.elapsed().as_micros()).unwrap_or(u64::MAX);
    let group_id = group_response["captureGroupId"]
        .as_str()
        .expect("capacity group response must contain a group id")
        .to_owned();

    let saturation_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let saturation = loop {
        let sample = capacity_sample(&runtime, "acquisition-saturation", started, &state_directory);
        let group_available = sample
            .available_resource_group_acquisitions
            .get("sim-shared")
            .copied();
        if sample.available_global_acquisitions == 0
            && group_available == Some(0)
            && sample.available_in_flight_bytes == 0
            && sample.outstanding_disk_bytes
                == SHORT_CAPACITY_FRAME_BYTES * SHORT_CAPACITY_CONCURRENT_CAPTURES as u64
        {
            break sample;
        }
        samples.push(sample);
        assert!(
            tokio::time::Instant::now() < saturation_deadline,
            "the short capacity group never saturated global, resource-group, byte, and disk admission"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    samples.push(saturation);

    // H2: 32 concurrent 8MP captures must cost 32 real frame buffers, not 32 counters. Admission can
    // report saturation a beat before every session has finished synthesizing, so sample resident
    // memory across a short window inside the 750 ms transfer hold and take the peak — the instant all
    // 32 buffers coexist. The old sleep-only harness would have shown zero growth here; this is the
    // assertion that makes "32 concurrent" a claim about the heap.
    let in_flight_frame_bytes =
        SHORT_CAPACITY_FRAME_BYTES * SHORT_CAPACITY_CONCURRENT_CAPTURES as u64;
    let minimum_expected_growth_bytes =
        in_flight_frame_bytes / CONCURRENT_CAPTURE_MEMORY_MINIMUM_FRACTION_DENOMINATOR;
    let mut saturation_peak_rss_bytes = roster_online_rss_bytes;
    let peak_deadline = tokio::time::Instant::now() + Duration::from_millis(600);
    while tokio::time::Instant::now() < peak_deadline {
        if let Some(rss_bytes) = capacity_process_stats().rss_bytes {
            saturation_peak_rss_bytes = saturation_peak_rss_bytes.max(rss_bytes);
        }
        if saturation_peak_rss_bytes
            >= roster_online_rss_bytes.saturating_add(minimum_expected_growth_bytes)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let concurrent_capture_growth_bytes =
        saturation_peak_rss_bytes.saturating_sub(roster_online_rss_bytes);
    assert!(
        concurrent_capture_growth_bytes >= minimum_expected_growth_bytes,
        "32 concurrent 8MP captures grew resident memory by only {concurrent_capture_growth_bytes} bytes; \
         real frame buffers should add at least {minimum_expected_growth_bytes} of their \
         {in_flight_frame_bytes}-byte combined footprint. A permit held on a bare sleep would show ~0."
    );

    let overflow_response = immediate_success(
        router
            .dispatch(
                "sb/capture-submit",
                command_message(
                    "sb/capture-submit",
                    "short-capacity-overflow",
                    json!({
                        "instance": "camera-0032",
                        "requestId": "short-capacity-overflow"
                    }),
                ),
                inbox.deferred_replies(),
            )
            .await,
    );
    let overflow_capture_id = overflow_response["captureId"]
        .as_str()
        .expect("overflow capture response must contain a capture id")
        .to_owned();
    let overflow_queued = runtime
        .catalog
        .job(&overflow_capture_id)
        .await
        .expect("overflow capture query must succeed")
        .expect("overflow capture must be durable");
    assert!(
        !overflow_queued.state.is_terminal(),
        "the 33rd capture must not bypass a saturated global admission gate"
    );
    samples.push(capacity_sample(&runtime, "overflow-queued", started, &state_directory));

    let mut latency_samples = BTreeMap::<String, Vec<u64>>::new();
    for index in 0..20 {
        latency_samples
            .entry("sb/list".to_owned())
            .or_default()
            .push(
                time_immediate_command(
                    &router,
                    &deferred,
                    "sb/list",
                    &format!("short-capacity-list-{index}"),
                    json!({ "limit": 1 }),
                )
                .await,
            );
        latency_samples
            .entry("sb/status".to_owned())
            .or_default()
            .push(
                time_immediate_command(
                    &router,
                    &deferred,
                    "sb/status",
                    &format!("short-capacity-status-{index}"),
                    json!({ "instance": "camera-0033" }),
                )
                .await,
            );
        latency_samples
            .entry("sb/ptz-stop".to_owned())
            .or_default()
            .push(
                time_immediate_command(
                    &router,
                    &deferred,
                    "sb/ptz",
                    &format!("short-capacity-stop-{index}"),
                    json!({
                        "operation": "stop",
                        "instance": "camera-0033",
                        "requestId": format!("short-capacity-stop-{index}"),
                        "axes": ["pan", "tilt", "zoom"]
                    }),
                )
                .await,
            );
    }
    let command_latency = latency_samples
        .into_iter()
        .map(|(verb, values)| (verb, summarize_latency(values)))
        .collect::<BTreeMap<_, _>>();
    // The command plane must stay responsive while the capture plane is saturated — but note what
    // "saturated" now means. It used to mean 32 captures asleep on a 5-second timer, an idle process
    // against which any command was instant, so a 250 ms ceiling passed trivially and measured nothing.
    // With H2 the same 32 captures are synthesizing 8-megapixel frames and streaming ~256 MiB to disk,
    // so this samples the router under GENUINE contention. 2 s is the honest ceiling for that: fifteen
    // times under the framework's 30 s command deadline, comfortably met on the lab and in WSL, and
    // still far below the multi-second p95 a truly starved command plane would show.
    const SATURATED_COMMAND_P95_CEILING_MICROS: u64 = 2_000_000;
    for (verb, summary) in &command_latency {
        assert!(
            summary.p95_micros <= SATURATED_COMMAND_P95_CEILING_MICROS,
            "{verb} p95 was {}us while the capture plane was saturated with real 8MP work",
            summary.p95_micros
        );
    }

    let group =
        wait_for_group_terminal_within(&runtime, &group_id, Duration::from_secs(90)).await;
    assert_eq!(group.members.len(), SHORT_CAPACITY_CONCURRENT_CAPTURES);
    let group_successful_members = group
        .members
        .iter()
        .filter(|member| member.state == crate::model::JobState::Succeeded)
        .count();
    assert_eq!(group_successful_members, SHORT_CAPACITY_CONCURRENT_CAPTURES);
    let overflow =
        wait_for_terminal_within(&runtime, &overflow_capture_id, Duration::from_secs(90))
            .await;
    assert_eq!(overflow.state, crate::model::JobState::Succeeded);
    samples.push(capacity_sample(&runtime, "captures-terminal", started, &state_directory));

    router.begin_shutdown();
    runtime.shutdown().await;
    inbox.stop().await;

    write_capacity_artifact(
        &artifact_directory,
        "short-capacity-summary.json",
        &ShortCapacityArtifact {
            schema_version: "camera-adapter-short-capacity/v1",
            scope: "ignored Linux short proof using the real Core facade and in-process SimBackend; not a 24-hour soak or hardware test",
            configured_cameras: SHORT_CAPACITY_CONFIGURED_CAMERAS,
            enabled_simulated_sessions: SHORT_CAPACITY_ENABLED_CAMERAS,
            concurrent_capture_target: SHORT_CAPACITY_CONCURRENT_CAPTURES,
            frame: json!({
                "width": SHORT_CAPACITY_FRAME_WIDTH,
                "height": SHORT_CAPACITY_FRAME_HEIGHT,
                "pixelFormat": "Mono8",
                "bytesPerFrame": SHORT_CAPACITY_FRAME_BYTES
            }),
            idle_session_memory,
            concurrent_capture_memory: json!({
                "rosterOnlineRssBytes": roster_online_rss_bytes,
                "saturationPeakRssBytes": saturation_peak_rss_bytes,
                "inFlightFrameBytes": in_flight_frame_bytes,
                "observedGrowthBytes": concurrent_capture_growth_bytes,
                "minimumExpectedGrowthBytes": minimum_expected_growth_bytes,
            }),
            capture_group_submit_micros,
            command_latency,
            resource_samples: samples,
            group_terminal_state: format!("{:?}", group.state),
            group_successful_members,
            overflow_capture_terminal_state: format!("{:?}", overflow.state),
            omitted_from_this_short_run: vec![
                "24-hour soak execution",
                "10,000 mixed-job workload",
                "broker-outage recovery",
                "reload churn",
                "encoder and writer saturation graph",
                "Core ping handler timing",
                "physical-camera compatibility",
            ],
        },
    );
}

#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
fn capacity_facades(
    core: &edgecommons::EdgeCommons,
    instances: &[String],
) -> (
    BTreeMap<String, Arc<AppFacade>>,
    BTreeMap<String, EventsFacade>,
) {
    let mut apps = BTreeMap::new();
    let mut events = BTreeMap::new();
    for instance in instances {
        let facade = core
            .instance(instance)
            .expect("configured capacity instance must build Core facades");
        apps.insert(instance.clone(), Arc::new(facade.app()));
        events.insert(instance.clone(), facade.events());
    }
    (apps, events)
}

/// Bounded 15-minute simulator smoke for the capacity harness itself.
///
/// The runner always executes the separate 8MP short proof first. This smoke switches to
/// small deterministic frames so schedules and command traffic can run for fifteen
/// minutes without turning a harness-construction check into a multi-hour disk benchmark.
#[cfg(all(
    target_os = "linux",
    feature = "standalone",
    feature = "onvif",
    feature = "capacity-harness"
))]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "15-minute Linux simulator smoke; run simulators/run-capacity-validation.sh --soak-duration 15m"]
async fn fifteen_minute_linux_capacity_smoke_exercises_mixed_runtime_traffic() {
    let duration_seconds = std::env::var("CAMERA_ADAPTER_CAPACITY_SOAK_DURATION_SECS")
        .expect("runner must set CAMERA_ADAPTER_CAPACITY_SOAK_DURATION_SECS")
        .parse::<u64>()
        .expect("soak duration must be an unsigned integer");
    assert_eq!(
        duration_seconds, 900,
        "only the bounded 15-minute smoke is implemented; the 24-hour soak remains deferred"
    );
    let artifact_directory = capacity_artifact_directory();
    let temporary_root = TempDir::new().expect("capacity smoke root must be creatable");
    let state_directory = temporary_root.path().join("capacity-state");
    let mut configuration = short_capacity_configuration(temporary_root.path());
    const SMOKE_FRAME_BYTES: u64 = 640 * 480;
    const SMOKE_RESERVATION_BYTES: u64 = 1024 * 1024;
    configuration.global.limits.max_frame_bytes_per_camera = SMOKE_RESERVATION_BYTES;
    configuration.global.limits.max_in_flight_bytes =
        SMOKE_RESERVATION_BYTES * SHORT_CAPACITY_CONCURRENT_CAPTURES as u64;
    for (index, camera) in configuration.instances.iter_mut().enumerate() {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("capacity smoke must use SimBackend");
        };
        sim.capture_delay_ms = 50;
        sim.frame.width = 640;
        sim.frame.height = 480;
        camera
            .capture_profiles
            .get_mut("main")
            .expect("capacity smoke main profile must exist")
            .maximum_frame_bytes = Some(SMOKE_RESERVATION_BYTES);
        if index < 8 {
            camera.schedules.push(
                serde_json::from_value(json!({
                    "id": "five-second-smoke",
                    "cron": "*/5 * * * * *",
                    "timezone": "UTC",
                    "captureProfile": "main"
                }))
                .expect("capacity smoke schedule must deserialize"),
            );
        }
    }
    fs::create_dir_all(&configuration.global.output.root_directory)
        .expect("capacity smoke output root must exist before storage initialization");
    let instance_ids = configuration
        .instances
        .iter()
        .map(|camera| camera.id.clone())
        .collect::<Vec<_>>();
    let router = RuntimeCommandRouter::new();
    let (port, _) = spawn_recording_mqtt_broker().await;
    let core = facade_core_with_router_instances(
        &temporary_root,
        port,
        Some(Arc::clone(&router)),
        &instance_ids,
    )
    .await;
    let inbox = core
        .commands()
        .expect("capacity smoke Core must expose inbox");
    let deferred = inbox.deferred_replies();
    let resources = prepare_startup_resources(&configuration, Platform::Host)
        .await
        .expect("capacity smoke startup resources must be valid");
    let (apps, events) = capacity_facades(&core, &instance_ids);
    let runtime = CameraRuntime::start(
        configuration,
        resources,
        RuntimeServices {
            apps,
            events,
            component_events: core.events(),
            metrics: Arc::new(RecordingMetrics::default()),
            // A standalone/HOST capacity runtime is an MQTT transport.
            transport: edgecommons::platform::Transport::Mqtt,
            readiness: RuntimeReadiness::noop(),
            backend_context: BackendRuntimeContext::new(
                None,
                &crate::config::LimitsConfig::default(),
            ),
        },
    )
    .await
    .expect("capacity smoke runtime must start");
    router
        .install(runtime.clone())
        .expect("capacity smoke router must install");
    let _ = wait_for_capacity_roster(&runtime).await;

    let started = Instant::now();
    let deadline = started + Duration::from_secs(duration_seconds);
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    let mut samples = vec![capacity_sample(&runtime, "soak-roster-online", started, &state_directory)];
    let mut timing = BTreeMap::<String, Vec<u64>>::new();
    let mut submitted_captures = 0_u64;
    let mut reconnects = 0_u64;
    let mut reloads = 0_u64;
    // H4 (the part that is reachable in-process): drive more of the runtime than pull-only capture.
    // Real RTSP/ONVIF/GenICam streaming and the two-box separation are the X1..X6 rig and cannot be
    // stood up by an in-process SimBackend — but the PTZ safety-stop lane (D2, N13) and the group
    // scatter/aggregate path (D-CAM-20) are pure runtime, and they should be under load here, not only
    // in their own focused tests.
    let mut ptz_moves = 0_u64;
    let mut group_captures = 0_u64;
    let mut retention_sweeps = 0_u64;
    let sweep_cancellation = CancellationToken::new();
    let mut tick = 0_u64;
    while Instant::now() < deadline {
        ticks.tick().await;
        tick = tick.saturating_add(1);
        let target = format!("camera-{:04}", 32 + (tick as usize % 224));
        if tick % 2 == 0 {
            let _ = immediate_success(
                router
                    .dispatch(
                        "sb/capture-submit",
                        command_message(
                            "sb/capture-submit",
                            &format!("soak-capture-{tick}"),
                            json!({ "instance": target, "requestId": format!("soak-capture-{tick}") }),
                        ),
                        deferred.clone(),
                    )
                    .await,
            );
            submitted_captures = submitted_captures.saturating_add(1);
        }
        if tick % 5 == 0 {
            timing.entry("sb/list".to_owned()).or_default().push(
                time_immediate_command(
                    &router,
                    &deferred,
                    "sb/list",
                    &format!("soak-list-{tick}"),
                    json!({ "limit": 10 }),
                )
                .await,
            );
            timing.entry("sb/status".to_owned()).or_default().push(
                time_immediate_command(
                    &router,
                    &deferred,
                    "sb/status",
                    &format!("soak-status-{tick}"),
                    json!({ "instance": "camera-0033" }),
                )
                .await,
            );
            timing.entry("sb/ptz-stop".to_owned()).or_default().push(
                time_immediate_command(
                    &router,
                    &deferred,
                    "sb/ptz",
                    &format!("soak-stop-{tick}"),
                    json!({
                        "operation": "stop",
                        "instance": "camera-0033",
                        "requestId": format!("soak-stop-{tick}"),
                        "axes": ["pan", "tilt", "zoom"]
                    }),
                )
                .await,
            );
            samples.push(capacity_sample(&runtime, "soak-sample", started, &state_directory));
        }
        if tick % 60 == 0 {
            runtime
                .reconnect(ReconnectRequest {
                    instance: Some("camera-0000".to_owned()),
                    request_id: format!("soak-reconnect-{tick}"),
                    reason: Some("capacity-smoke".to_owned()),
                })
                .await
                .expect("capacity smoke reconnect must be accepted");
            reconnects = reconnects.saturating_add(1);
        }
        if tick % 180 == 0 {
            let replacement = runtime
                .config_snapshot()
                .expect("capacity smoke configuration must remain readable");
            let (apps, events) = capacity_facades(&core, &instance_ids);
            runtime
                .apply_reloaded_config((*replacement).clone(), apps, events)
                .await
                .expect("capacity smoke reload must preserve the valid generation");
            reloads = reloads.saturating_add(1);
        }
        // H4: a continuous PTZ move with a short mandatory-stop deadline, on a rotating camera. This
        // is the lane N13 found empty — a commanded move that must be stopped by a timer even if the
        // requester walks away — and it should be exercised under sustained capture load, not only in
        // isolation. The sim's PTZ is in-memory, so the interest is the runtime path, not the optics.
        if tick % 7 == 3 {
            let _ = immediate_success(
                router
                    .dispatch(
                        "sb/ptz",
                        command_message(
                            "sb/ptz",
                            &format!("soak-move-{tick}"),
                            json!({
                                "operation": "continuous",
                                "instance": target,
                                "requestId": format!("soak-move-{tick}"),
                                "velocity": { "pan": 0.4, "tilt": 0.0, "zoom": 0.0 },
                                "timeoutMs": 500
                            }),
                        ),
                        deferred.clone(),
                    )
                    .await,
            );
            ptz_moves = ptz_moves.saturating_add(1);
        }
        // H4: a small group capture fans out to several cameras and aggregates one reply, under load.
        if tick % 90 == 45 {
            let members = (0..4)
                .map(|offset| format!("camera-{:04}", 32 + ((tick as usize + offset) % 224)))
                .collect::<Vec<_>>();
            let _ = immediate_success(
                router
                    .dispatch(
                        "sb/capture-group-submit",
                        command_message(
                            "sb/capture-group-submit",
                            &format!("soak-group-{tick}"),
                            json!({
                                "requestId": format!("soak-group-{tick}"),
                                "instances": members
                            }),
                        ),
                        deferred.clone(),
                    )
                    .await,
            );
            group_captures = group_captures.saturating_add(1);
        }
        // B1's retention path, driven under load. In a 15-minute window nothing is old enough to be
        // reclaimed (the window is 72 hours), so this reclaims zero — but it proves the sweep the
        // review found wired to nothing now runs on the live runtime, repeatedly, alongside the
        // capture hot path, without erroring, deadlocking, or disturbing the roster.
        if tick % 120 == 0 {
            runtime
                .retention_sweep(chrono::Utc::now().timestamp_millis(), 500, &sweep_cancellation)
                .await
                .expect("capacity smoke retention sweep must succeed under load");
            retention_sweeps = retention_sweeps.saturating_add(1);
        }
    }
    // The final workload tick can deliberately request a reconnect.  Do not label the
    // smoke complete until that bounded lifecycle transition has converged back to the
    // full configured roster; otherwise the final report would confuse an in-progress
    // reconnect with lost capacity.
    let _ = wait_for_capacity_roster(&runtime).await;
    samples.push(capacity_sample(&runtime, "soak-complete", started, &state_directory));
    let mut scheduled_jobs_by_camera = BTreeMap::new();
    for instance in instance_ids.iter().take(8) {
        let scheduled_jobs = runtime
            .catalog
            .jobs_page(Some(instance.clone()), Vec::new(), None, 1_000)
            .await
            .expect("capacity smoke must retain scheduled job evidence")
            .into_iter()
            .filter(|job| {
                job.trigger.get("type").and_then(Value::as_str) == Some("schedule")
                    && job.trigger.get("scheduleId").and_then(Value::as_str)
                        == Some("five-second-smoke")
            })
            .count() as u64;
        assert!(
            scheduled_jobs >= 120,
            "capacity smoke must record sustained scheduled traffic for {instance}; observed {scheduled_jobs} accepted occurrences"
        );
        scheduled_jobs_by_camera.insert(instance.clone(), scheduled_jobs);
    }
    let command_latency = timing
        .into_iter()
        .map(|(verb, values)| (verb, summarize_latency(values)))
        .collect::<BTreeMap<_, _>>();
    assert!(
        submitted_captures >= 400,
        "15-minute smoke submitted too little command traffic"
    );
    assert!(
        reconnects >= 14,
        "15-minute smoke omitted reconnect traffic"
    );
    assert!(reloads >= 4, "15-minute smoke omitted reload traffic");
    assert!(ptz_moves >= 60, "15-minute smoke omitted PTZ safety-lane traffic");
    assert!(group_captures >= 5, "15-minute smoke omitted group-capture traffic");
    assert!(retention_sweeps >= 5, "15-minute smoke omitted retention sweeps");

    // ---- H5: what a single before/after RSS delta cannot see -------------------------------------
    //
    // The old harness gated exactly one memory number: the idle-startup RSS delta. It could not tell a
    // runtime that leaks a little per capture from one that is steady, could not see the durable store
    // growing without bound (B1), and could not see a poll storm burning CPU (D1/B6). These are the
    // three growth-over-time gates that close that gap. Each is generous enough not to be flaky and
    // tight enough that the regression it names would move it.

    // (1) Resident memory must not RUN AWAY. Compare an early steady window against the run's late
    //     peak. This is a coarse guard by nature — fifteen minutes is too short to separate a slow
    //     leak from allocator retention and reload churn, and trying to gate that finely is how a soak
    //     smoke becomes flaky. So the bound is deliberately generous: 256 MiB of growth over the run
    //     is not a leak worth chasing, but the unbounded, monotonic climb that a real leak or a wired-
    //     to-nothing cleanup produces sails past it. The sharp per-record signal lives in the catalog
    //     gate below; this one only has to catch a runaway.
    const RSS_RUNAWAY_BOUND_BYTES: u64 = 256 * 1024 * 1024;
    let steady_rss: Vec<u64> = samples
        .iter()
        .filter(|sample| sample.elapsed_millis >= 60_000 && sample.elapsed_millis <= 180_000)
        .filter_map(|sample| sample.process.rss_bytes)
        .collect();
    let late_rss_peak = samples
        .iter()
        .filter(|sample| sample.elapsed_millis >= 180_000)
        .filter_map(|sample| sample.process.rss_bytes)
        .max();
    let rss_growth_bound_bytes = RSS_RUNAWAY_BOUND_BYTES;
    let (rss_floor, rss_peak, rss_growth_bytes) = match (steady_rss.iter().min(), late_rss_peak) {
        (Some(&floor), Some(peak)) => {
            let growth = peak.saturating_sub(floor);
            assert!(
                growth <= rss_growth_bound_bytes,
                "resident memory grew {growth} bytes from its steady floor of {floor} to a late peak of {peak}; \
                 a 256-camera roster at rest must stay within {rss_growth_bound_bytes} bytes of itself over the run"
            );
            (floor, peak, growth)
        }
        _ => panic!("15-minute smoke must sample RSS across an early-steady and a late window"),
    };

    // (2) CPU must not be pegged. Total CPU time over the wall clock is utilization; a runtime that has
    //     regressed into a busy-wait across its eight worker threads would show several cores' worth.
    //     A mostly-idle-between-captures 256-camera smoke sits well under one core; four is a generous
    //     ceiling that still catches a poll storm.
    let cpu_series: Vec<u64> = samples
        .iter()
        .filter_map(|sample| sample.process.cpu_micros)
        .collect();
    let (cpu_start, cpu_end) = (
        *cpu_series.first().expect("CPU must be sampled at the start"),
        *cpu_series.last().expect("CPU must be sampled at the end"),
    );
    let cpu_used_micros = cpu_end.saturating_sub(cpu_start);
    let wall_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX).max(1);
    let cpu_utilization_millicores = cpu_used_micros.saturating_mul(1_000) / wall_micros;
    assert!(
        cpu_utilization_millicores <= 4_000,
        "the smoke averaged {cpu_utilization_millicores} millicores of CPU over its run; a mostly-idle \
         256-camera roster pegged near or above four full cores is the signature of a poll storm"
    );

    // (3) The durable store's per-record cost must be bounded. This is the gate the review said the
    //     harness lacked for B1: not just that retention is wired (it is, and it swept above), but that
    //     a capture's durable footprint stays small. A regression that reattached a fat per-capture
    //     payload -- the ~5 KB outbox envelope was exactly this -- would blow the bytes-per-record.
    let job_state_counts = runtime
        .catalog
        .count_jobs_by_state(None, Vec::new())
        .await
        .expect("capacity smoke must be able to count durable jobs");
    let durable_records: u64 = job_state_counts.values().copied().sum();
    let catalog_bytes = catalog_disk_bytes(&state_directory)
        .expect("capacity smoke must be able to stat the catalog on disk");
    assert!(durable_records > 0, "the smoke must have written durable jobs");
    let bytes_per_record = catalog_bytes / durable_records;
    const CATALOG_BYTES_PER_RECORD_CEILING: u64 = 16 * 1024;
    assert!(
        bytes_per_record <= CATALOG_BYTES_PER_RECORD_CEILING,
        "the catalog holds {catalog_bytes} bytes across {durable_records} durable jobs -- \
         {bytes_per_record} bytes each, over the {CATALOG_BYTES_PER_RECORD_CEILING}-byte ceiling. A \
         capture's durable cost has blown up; this is the shape B1 warned about."
    );

    router.begin_shutdown();
    runtime.shutdown().await;
    inbox.stop().await;
    write_capacity_artifact(
        &artifact_directory,
        "fifteen-minute-soak-summary.json",
        &json!({
            "schemaVersion": "camera-adapter-capacity-smoke/v1",
            "scope": "15-minute Linux SimBackend smoke; not a 24-hour soak or hardware test",
            "durationSeconds": duration_seconds,
            "configuredCameras": SHORT_CAPACITY_CONFIGURED_CAMERAS,
            "enabledSimulatedSessions": SHORT_CAPACITY_ENABLED_CAMERAS,
            "scheduledCameras": 8,
            "scheduledJobsByCamera": scheduled_jobs_by_camera,
            "frame": { "width": 640, "height": 480, "pixelFormat": "Mono8", "bytesPerFrame": SMOKE_FRAME_BYTES, "reservationBytes": SMOKE_RESERVATION_BYTES },
            "submittedCaptures": submitted_captures,
            "reconnects": reconnects,
            "reloads": reloads,
            "ptzMoves": ptz_moves,
            "groupCaptures": group_captures,
            "retentionSweeps": retention_sweeps,
            "residentMemory": {
                "steadyFloorBytes": rss_floor,
                "latePeakBytes": rss_peak,
                "growthBytes": rss_growth_bytes,
                "growthBoundBytes": rss_growth_bound_bytes
            },
            "cpu": {
                "usedMicros": cpu_used_micros,
                "wallMicros": wall_micros,
                "utilizationMillicores": cpu_utilization_millicores,
                "utilizationCeilingMillicores": 4_000
            },
            "catalog": {
                "diskBytes": catalog_bytes,
                "durableRecords": durable_records,
                "bytesPerRecord": bytes_per_record,
                "bytesPerRecordCeiling": CATALOG_BYTES_PER_RECORD_CEILING
            },
            "commandLatency": command_latency,
            "resourceSamples": samples,
            "omittedFromThisSmoke": ["24-hour execution", "10,000-job completion target", "broker-outage recovery", "encoder/writer saturation", "Core ping timing", "physical cameras", "real RTSP/ONVIF/GenICam streaming and the two-box fleet (X1-X6)"]
        }),
    );
}

fn retention_job(capture_id: &str, request_id: &str) -> crate::catalog::NewJob {
    let canonical_request = json!({ "requestId": request_id, "profile": "main" });
    crate::catalog::NewJob {
        capture_id: capture_id.to_owned(),
        instance: "camera-a".to_owned(),
        ledger_key: Some(
            crate::catalog::LedgerKey::new("camera-a", "sb/capture", request_id).unwrap(),
        ),
        request_hash: crate::idempotency::canonical_request_hash(&canonical_request, false)
            .unwrap(),
        canonical_request,
        effective_profile: json!({ "name": "main", "encoding": "jpeg" }),
        deadlines: crate::catalog::JobDeadlines {
            terminal_at_ms: 10_000,
            queue_at_ms: Some(2_000),
            capture_at_ms: 4_000,
            encode_at_ms: 7_000,
            persist_at_ms: 9_000,
        },
        trigger: json!({ "type": "command", "requestId": request_id }),
        origin_correlation_id: Some(format!("corr-{capture_id}")),
        intended_output: json!({ "relativePath": format!("{capture_id}.jpg") }),
        accepted_at_ms: 1_000,
        group_id: None,
    }
}

fn retention_terminal(
    capture_id: &str,
    instance: &str,
    sequence: u8,
) -> crate::catalog::TerminalWrite {
    retention_terminal_for(capture_id, instance, None, sequence)
}

fn retention_terminal_for(
    capture_id: &str,
    instance: &str,
    group_id: Option<&str>,
    sequence: u8,
) -> crate::catalog::TerminalWrite {
    let mut result = json!({
        "schemaVersion": 1,
        "eventId": format!("retention-event-{sequence}"),
        "captureId": capture_id,
        "cameraId": instance,
        "correlationId": format!("corr-{capture_id}"),
        "state": "FAILED",
    });
    if let Some(group_id) = group_id {
        result["captureGroupId"] = json!(group_id);
    }
    crate::catalog::TerminalWrite {
        state: crate::model::JobState::Failed,
        result,
        error_code: Some("PROCESS_INTERRUPTED".to_owned()),
        error_message: Some("bounded failure".to_owned()),
        // Deliberately ancient: every sweep below uses the real clock, so these records
        // are far outside the configured retention window.
        terminal_at_ms: 20_000 + i64::from(sequence),
    }
}

/// Seeds two terminal direct jobs, one terminal group with a terminal member, and one
/// completed standalone command ledger.
async fn seed_retained_state(catalog: &Catalog) {
    for (capture, sequence) in [("retention-a", 1_u8), ("retention-b", 2)] {
        catalog
            .accept_job(retention_job(capture, capture))
            .await
            .unwrap();
        catalog.queue_job(capture, 2_000).await.unwrap();
        catalog
            .commit_terminal(capture, retention_terminal(capture, "camera-a", sequence))
            .await
            .unwrap();
    }

    // A capture group spans distinct camera instances by construction.
    let group_request = json!({ "requestId": "retention-group" });
    let members = [
        ("retention-member-a", "camera-a"),
        ("retention-member-b", "camera-b"),
    ]
    .into_iter()
    .map(|(capture, instance)| {
        let mut member = retention_job(capture, capture);
        member.instance = instance.to_owned();
        member.ledger_key = None;
        member.group_id = Some("retention-group-1".to_owned());
        member
    })
    .collect::<Vec<_>>();
    catalog
        .accept_group(crate::catalog::NewGroup {
            group_id: "retention-group-1".to_owned(),
            ledger_key: crate::catalog::LedgerKey::new(
                "main",
                "sb/capture-group",
                "retention-group",
            )
            .unwrap(),
            request_hash: crate::idempotency::canonical_request_hash(&group_request, false)
                .unwrap(),
            canonical_request: group_request,
            origin_correlation_id: None,
            accepted_at_ms: 1_000,
            members,
        })
        .await
        .unwrap();
    for (capture, instance, sequence) in [
        ("retention-member-a", "camera-a", 3_u8),
        ("retention-member-b", "camera-b", 4),
    ] {
        catalog.queue_job(capture, 2_000).await.unwrap();
        catalog
            .commit_terminal(
                capture,
                retention_terminal_for(
                    capture,
                    instance,
                    Some("retention-group-1"),
                    sequence,
                ),
            )
            .await
            .unwrap();
    }
    catalog
        .complete_group(
            "retention-group-1",
            crate::model::JobState::Failed,
            json!({ "succeeded": 0, "failed": 2 }),
            Some("BACKEND_ERROR".to_owned()),
            Some("members failed".to_owned()),
            30_000,
        )
        .await
        .unwrap();

    let ptz_request = json!({ "instance": "camera-a", "requestId": "retention-ptz" });
    let ptz_key =
        crate::catalog::LedgerKey::new("camera-a", "sb/ptz/absolute", "retention-ptz")
            .unwrap();
    catalog
        .begin_command(
            ptz_key.clone(),
            crate::idempotency::canonical_request_hash(&ptz_request, false).unwrap(),
            ptz_request,
            1_000,
        )
        .await
        .unwrap();
    catalog
        .complete_command(
            ptz_key,
            crate::catalog::LedgerState::Succeeded,
            json!({ "state": "COMMANDED" }),
            None,
            None,
            2_000,
        )
        .await
        .unwrap();

}

// Every one of these records used to accumulate forever: the whole retention subsystem was
// built, unit-tested, and then never called, so the state database grew until the
// free-space floor rejected every capture.
#[tokio::test]
async fn retention_sweep_reclaims_state_past_its_windows_and_reports_the_counts() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let catalog = runtime.catalog.clone();
    seed_retained_state(&catalog).await;

    let now_ms = chrono::Utc::now().timestamp_millis();
    let sweep = runtime
        .retention_sweep(now_ms, RETENTION_BATCH, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(sweep.terminal_jobs, 2);
    assert_eq!(sweep.terminal_groups, 1);
    assert_eq!(sweep.command_ledgers, 1);
    assert_eq!(
        sweep.over_limit_jobs, 0,
        "a record count far below maxResultRecords must reclaim nothing"
    );
    assert_eq!(sweep.reclaimed(), 4);

    assert!(catalog.job("retention-a").await.unwrap().is_none());
    assert!(catalog.job("retention-b").await.unwrap().is_none());
    assert!(catalog.job("retention-member-a").await.unwrap().is_none());
    assert!(catalog.job("retention-member-b").await.unwrap().is_none());
    assert!(catalog.group("retention-group-1").await.unwrap().is_none());
    assert_eq!(
        runtime
            .retention_sweep(now_ms, RETENTION_BATCH, &CancellationToken::new())
            .await
            .unwrap(),
        RetentionSweep::default(),
        "a second sweep must find nothing left to reclaim"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn retention_sweep_enforces_the_configured_result_record_maximum() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.state.max_result_records = 1;
    let runtime = runtime(configuration, &directory).await;
    let catalog = runtime.catalog.clone();
    // Keep both jobs inside the time window so only the count limit can reclaim them.
    for (capture, sequence) in [("count-a", 11_u8), ("count-b", 12)] {
        catalog
            .accept_job(retention_job(capture, capture))
            .await
            .unwrap();
        catalog.queue_job(capture, 2_000).await.unwrap();
        catalog
            .commit_terminal(capture, retention_terminal(capture, "camera-a", sequence))
            .await
            .unwrap();
    }
    let future_ms = chrono::Utc::now().timestamp_millis();
    let sweep = runtime
        .retention_sweep(future_ms, RETENTION_BATCH, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(sweep.over_limit_jobs + sweep.terminal_jobs, 2);
    assert!(catalog.job("count-a").await.unwrap().is_none());
    assert!(catalog.job("count-b").await.unwrap().is_none());
    runtime.shutdown().await;
}

#[tokio::test]
async fn retention_sweep_yields_to_cancellation_before_touching_the_catalog() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let catalog = runtime.catalog.clone();
    seed_retained_state(&catalog).await;

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let sweep = runtime
        .retention_sweep(
            chrono::Utc::now().timestamp_millis(),
            RETENTION_BATCH,
            &cancellation,
        )
        .await
        .unwrap();
    assert_eq!(sweep, RetentionSweep::default());
    assert!(
        catalog.job("retention-a").await.unwrap().is_some(),
        "a cancelled sweep must not keep issuing catalog work"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn retention_sweep_reclaims_a_backlog_in_bounded_paced_batches() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let catalog = runtime.catalog.clone();
    seed_retained_state(&catalog).await;

    // One row per catalog round trip: the sweep must keep going until the backlog is gone
    // rather than reclaim a single batch and wait an hour, and it must never issue the
    // whole backlog at once against the two-worker pool that carries the capture path.
    let sweep = runtime
        .retention_sweep(
            chrono::Utc::now().timestamp_millis(),
            1,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(sweep.terminal_jobs, 2);
    assert_eq!(sweep.reclaimed(), 4);
    assert!(catalog.job("retention-b").await.unwrap().is_none());
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_failing_retention_sweep_never_stops_the_periodic_task() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let catalog = runtime.catalog.clone();
    seed_retained_state(&catalog).await;

    // A zero batch is rejected by every prune, so every sweep fails. Retention is a
    // background reclaim: a failing sweep must be logged and retried, never propagated.
    let cancellation = CancellationToken::new();
    let sweeper = tokio::spawn(Arc::clone(&runtime).run_retention(
        cancellation.clone(),
        Duration::from_millis(5),
        0,
    ));
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        !sweeper.is_finished(),
        "a failing sweep must not terminate the retention task"
    );
    assert!(catalog.job("retention-a").await.unwrap().is_some());
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), sweeper)
        .await
        .expect("the retention task must still observe cancellation")
        .expect("the retention task must not panic");
    runtime.shutdown().await;
}

#[tokio::test]
async fn periodic_retention_reclaims_on_its_interval_and_stops_with_the_runtime() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let catalog = runtime.catalog.clone();
    seed_retained_state(&catalog).await;

    let cancellation = CancellationToken::new();
    let sweeper = tokio::spawn(Arc::clone(&runtime).run_retention(
        cancellation.clone(),
        Duration::from_millis(20),
        RETENTION_BATCH,
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while catalog.job("retention-a").await.unwrap().is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the periodic sweep never reclaimed a record past its retention window"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Keep sweeping once the backlog is gone: an idle sweep must be a no-op, not an error
    // and not a reason for the task to stop.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!sweeper.is_finished());

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), sweeper)
        .await
        .expect("a cancelled retention task must stop within the shutdown grace")
        .expect("the retention task must not panic");
    runtime.shutdown().await;
}

#[tokio::test]
async fn start_retention_registers_a_cancellable_runtime_task() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let before = runtime.tasks.lock().unwrap().len();
    runtime.start_retention().unwrap();
    assert_eq!(runtime.tasks.lock().unwrap().len(), before + 1);
    // The task parks on the hourly interval; shutdown must still join it immediately.
    tokio::time::timeout(Duration::from_secs(5), runtime.shutdown())
        .await
        .expect("the retention task must observe runtime cancellation");
}

// D6: `sb/reconnect` began a ledger row and never completed it.  The row was fenced to
// OUTCOME_UNKNOWN on the next start, which no DELETE in the catalog can match, so the row
// was immortal and every retry answered PREVIOUS_OUTCOME_UNKNOWN forever.
#[tokio::test]
async fn reconnect_settles_its_ledger_and_the_row_is_reclaimable() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let accepted = runtime
        .reconnect(ReconnectRequest {
            instance: Some("camera-a".to_string()),
            request_id: "reconnect-settled".to_string(),
            reason: None,
        })
        .await
        .unwrap();

    let key = crate::catalog::LedgerKey::new(
        "camera-a",
        crate::catalog::RECONNECT_VERB,
        "reconnect-settled",
    )
    .unwrap();
    let canonical =
        json!({ "instance": "camera-a", "requestId": "reconnect-settled", "reason": null });
    let hash = crate::idempotency::canonical_request_hash(&canonical, false).unwrap();
    let crate::catalog::BeginCommandOutcome::Existing(record) = runtime
        .catalog
        .begin_command(key, hash, canonical, chrono::Utc::now().timestamp_millis())
        .await
        .unwrap()
    else {
        panic!("the reconnect must have created exactly one durable ledger row");
    };
    assert_eq!(record.state, crate::catalog::LedgerState::Succeeded);
    assert_eq!(record.reply, Some(accepted));

    // Past the result-retention window the settled row is reclaimed like any other
    // completed command; an IN_PROGRESS or OUTCOME_UNKNOWN row never would be.
    let future_ms = chrono::Utc::now().timestamp_millis() + 30 * 24 * MILLIS_PER_HOUR;
    let sweep = runtime
        .retention_sweep(future_ms, RETENTION_BATCH, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(sweep.command_ledgers, 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn startup_settles_an_interrupted_reconnect_instead_of_fencing_it_forever() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let key = crate::catalog::LedgerKey::new(
        "camera-a",
        crate::catalog::RECONNECT_VERB,
        "reconnect-crash",
    )
    .unwrap();
    let canonical =
        json!({ "instance": "camera-a", "requestId": "reconnect-crash", "reason": null });
    let hash = crate::idempotency::canonical_request_hash(&canonical, false).unwrap();
    let reply =
        json!({ "operationId": "op_prior", "instance": "camera-a", "state": "ACCEPTED" });
    // Exactly the durable state a crash between acceptance and completion leaves behind.
    runtime
        .catalog
        .begin_command(key.clone(), hash, canonical, 1_000)
        .await
        .unwrap();
    runtime
        .catalog
        .record_command_acceptance(key, reply.clone(), 1_001)
        .await
        .unwrap();

    runtime.recover_install_owned().await.unwrap();

    // The retry must return the retained acceptance, not PREVIOUS_OUTCOME_UNKNOWN.
    let retried = runtime
        .reconnect(ReconnectRequest {
            instance: Some("camera-a".to_string()),
            request_id: "reconnect-crash".to_string(),
            reason: None,
        })
        .await
        .unwrap();
    assert_eq!(retried, reply);
    let future_ms = chrono::Utc::now().timestamp_millis() + 30 * 24 * MILLIS_PER_HOUR;
    let sweep = runtime
        .retention_sweep(future_ms, RETENTION_BATCH, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        sweep.command_ledgers, 1,
        "a reconnect row interrupted by a crash must remain reclaimable"
    );
    runtime.shutdown().await;
}

struct TeardownSession {
    capabilities: crate::model::CameraCapabilities,
    stops: Arc<Mutex<Vec<crate::model::PtzRequest>>>,
    closes: Arc<AtomicUsize>,
}

#[async_trait]
impl crate::backend::CameraSession for TeardownSession {
    fn capabilities(&self) -> &crate::model::CameraCapabilities {
        &self.capabilities
    }

    async fn status(&mut self) -> Result<crate::backend::CameraStatus> {
        unreachable!("the shutdown teardown test never reads status")
    }

    async fn capture(
        &mut self,
        _request: crate::backend::CaptureRequest,
    ) -> Result<crate::model::CaptureFrame> {
        unreachable!("the shutdown teardown test never captures")
    }

    async fn ptz_bounded(
        &mut self,
        request: crate::model::PtzRequest,
        _deadline: tokio::time::Instant,
        _cancellation: &CancellationToken,
    ) -> Result<crate::model::PtzResult> {
        self.stops.lock().unwrap().push(request);
        Ok(crate::model::PtzResult::Commanded)
    }

    async fn close(&mut self) -> Result<()> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// B4: the supervisor used to `abort()` the actor on cancellation. The actor holds a CHILD
// of that token, so it was already running its teardown; the abort dropped that future at
// its first await point, which meant a SIGTERM during a pan left the camera panning and no
// session was ever closed.
#[tokio::test]
async fn cancelled_actor_runs_its_safety_stop_and_session_close_within_the_grace() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let stops = Arc::new(Mutex::new(Vec::new()));
    let closes = Arc::new(AtomicUsize::new(0));
    let session = TeardownSession {
        capabilities: crate::model::CameraCapabilities {
            capture_modes: vec![crate::model::CaptureMode::Simulated],
            pixel_formats: vec![crate::model::PixelFormat::Rgb8],
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
        stops: Arc::clone(&stops),
        closes: Arc::clone(&closes),
    };
    let (actor, handle) = CameraActor::new(
        "camera-a",
        Box::new(session),
        runtime.engine("camera-a").unwrap(),
        2,
        2,
        runtime.scheduler.capacity_signal(),
    )
    .unwrap();
    handle
        .safety_stop(crate::admission::SafetyStop {
            pan: true,
            tilt: true,
            zoom: false,
            deadline: tokio::time::Instant::now() + Duration::from_secs(5),
        })
        .unwrap();

    // Reproduce the supervisor's exact shutdown ordering: the component token is tripped
    // before the actor task has ever been polled.
    let actor_cancellation = runtime.cancellation.child_token();
    runtime.cancellation.cancel();
    let mut actor_task = tokio::spawn(actor.run(actor_cancellation));

    assert!(
        join_actor_within_grace(&mut actor_task, Duration::from_secs(10)).await,
        "the actor must complete its own teardown inside the grace budget"
    );
    let observed = stops.lock().unwrap().clone();
    assert_eq!(
        observed,
        vec![crate::model::PtzRequest::Stop {
            pan: true,
            tilt: true,
            zoom: false
        }],
        "the queued shutdown safety stop must reach the transport"
    );
    assert_eq!(
        closes.load(Ordering::SeqCst),
        1,
        "the protocol session must be closed instead of leaked server-side"
    );
}

/// The two verbs, through the command layer that actually serves them.
///
/// Also pins the ledger: a break-glass drain cancels durable work, so a retried request must
/// return the ORIGINAL outcome rather than reach for a second wave of work the operator never
/// saw.
#[tokio::test]
async fn the_queue_verbs_answer_and_the_drain_is_idempotent() {
    let directory = TempDir::new().unwrap();
    let (runtime, metrics) =
        runtime_with_metrics(config(directory.path(), &["camera-a"], false), &directory)
            .await;

    for index in 0..2 {
        runtime
            .submit_capture(
                "camera-a".to_string(),
                format!("verb-backlog-{index}"),
                None,
                None,
                serde_json::Map::new(),
                format!("verb-correlation-{index}"),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .unwrap();
    }

    let status = runtime
        .queue_status_command(commands::QueueStatusRequest { instance: None })
        .await
        .expect("sb/queue-status must answer");
    assert_eq!(status["durableBacklog"], serde_json::json!(2));
    assert_eq!(status["dispatchQueued"], serde_json::json!(2));
    assert_eq!(
        status["cameras"][0]["instance"],
        serde_json::json!("camera-a")
    );

    // The metric sample must report the same figures the operator was just shown.
    let sampled = runtime.sample_queue_metric().await.unwrap();
    assert_eq!(sampled.get("durableBacklog"), Some(&2.0));
    assert_eq!(sampled.get("camerasConfigured"), Some(&1.0));
    assert_eq!(
        sampled.get("camerasOnline"),
        Some(&0.0),
        "no supervisor is running, so no camera is online"
    );
    runtime.metrics.sample_queue(sampled).await;
    assert_eq!(
        metrics.counts(crate::observability::QUEUE_METRIC, "durableBacklog"),
        2.0,
        "and the sample must actually reach the metric service"
    );

    let drain = commands::QueueClearRequest {
        request_id: "drain-1".to_string(),
        instance: Some("camera-a".to_string()),
        all_cameras: false,
        include_in_flight: false,
        reason: Some("line stopped".to_string()),
    };
    let cleared = runtime
        .queue_clear_command(drain.clone())
        .await
        .expect("sb/queue-clear must drain");
    assert_eq!(cleared["cancelled"], serde_json::json!(2));

    // Replayed: the ledger must return the first answer, not cancel a second wave.
    let replayed = runtime
        .queue_clear_command(drain)
        .await
        .expect("a retried drain must be idempotent");
    assert_eq!(
        replayed, cleared,
        "a retried break-glass drain must return the original outcome"
    );

    let after = runtime
        .queue_status_command(commands::QueueStatusRequest {
            instance: Some("camera-a".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(after["durableBacklog"], serde_json::json!(0));
    assert_eq!(after["dispatchQueued"], serde_json::json!(0));
}

/// A superseded supervisor's state write is dropped on purpose -- and must say so.
///
/// D5: every one of the eight supervisor call sites discarded this with `let _ =`, so the
/// generation fence rejecting a write and the registry lock being poisoned looked exactly
/// like a successful update.
#[tokio::test]
async fn a_superseded_generation_cannot_publish_camera_state() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    runtime.publish_camera_state(
        "camera-a",
        7,
        CameraConnectionState::Online,
        None,
        None,
        chrono::Utc::now(),
    );
    assert_eq!(
        runtime.registry.snapshot("camera-a").unwrap().state,
        CameraConnectionState::Online
    );

    // An older generation must not be able to drag the camera backwards.
    runtime.publish_camera_state(
        "camera-a",
        6,
        CameraConnectionState::Backoff,
        None,
        Some(CameraStatusError {
            code: "BACKEND_ERROR".to_string(),
            message: "stale".to_string(),
            observed_at: chrono::Utc::now(),
        }),
        chrono::Utc::now(),
    );
    assert_eq!(
        runtime.registry.snapshot("camera-a").unwrap().state,
        CameraConnectionState::Online,
        "the generation fence must drop a superseded supervisor's write"
    );

    // A camera that is not configured is likewise a no-op rather than a panic.
    runtime.publish_camera_state(
        "camera-gone",
        9,
        CameraConnectionState::Offline,
        None,
        None,
        chrono::Utc::now(),
    );
}

/// A capture must be counted even when the component is too busy to publish its events.
///
/// The lifecycle-event publish is best-effort by design: it takes a permit from a bounded
/// pool and gives up if none is free, so that a slow event consumer can never stall a
/// capture. That bound is right for EVENTS and catastrophic for a COUNTER. The counters
/// originally rode inside that publish, which meant they were dropped exactly when the
/// component was busiest -- under-reporting precisely the overload an operator would be
/// staring at -- and raced the capture's own terminal state, which is what CI caught: the
/// same commit passed on one runner and failed on another.
#[tokio::test]
async fn captures_are_counted_even_when_every_event_slot_is_taken() {
    let directory = TempDir::new().unwrap();
    let (runtime, metrics) =
        runtime_with_metrics(config(directory.path(), &["camera-a"], false), &directory)
            .await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    // Starve the publish path completely: not one lifecycle event can be emitted.
    let _permits = Arc::clone(&runtime.waiters.lifecycle_event_slots)
        .acquire_many_owned(
            u32::try_from(MAX_LIFECYCLE_EVENT_PUBLISHES)
                .expect("the fixed lifecycle capacity fits Tokio's semaphore API"),
        )
        .await
        .expect("the test holds every detached lifecycle permit");

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "counted-under-starvation".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "starvation-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected a newly accepted capture, got {other:?}"),
    };
    let terminal = wait_for_terminal(&runtime, &capture_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);

    let counted = crate::observability::CAPTURE_METRIC;
    assert_eq!(
        metrics.counts(counted, "queued"),
        1.0,
        "the capture happened; a saturated EVENT pipe must not stop it being COUNTED"
    );
    assert_eq!(metrics.counts(counted, "started"), 1.0);
    assert_eq!(
        metrics.counts(counted, "succeeded"),
        1.0,
        "and its outcome is the number an operator watches -- it must survive the overload                  that makes them look"
    );

    runtime.shutdown().await;
}

/// Q5: camera presence is pushed, not just polled.
///
/// A camera's state lived in the registry and could be learned only by asking -- `sb/list`,
/// `sb/status`. Nothing was ever published, so a consumer that wanted to know a camera had
/// dropped had to poll for it. The assumption that camera connectivity already reached the
/// standard health surface did not hold: EdgeCommons ships the per-instance connectivity
/// provider for exactly this, and nothing was registered against it.
#[tokio::test]
async fn every_camera_reports_its_reachability_to_the_heartbeat() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(
        config(directory.path(), &["camera-a", "camera-b"], false),
        &directory,
    )
    .await;

    // Before any supervisor runs, both cameras are configured and neither is reachable.
    let cold = runtime.camera_connectivity();
    assert_eq!(
        cold.len(),
        2,
        "every configured camera must be reported, not just live ones"
    );
    assert!(
        cold.iter().all(|camera| !camera.connected),
        "a camera that has never connected must not be reported as connected"
    );
    assert!(
        cold.iter()
            .all(|camera| camera.state.as_deref() == Some("OFFLINE")),
        "a camera that is down must say WHICH kind of down: BACKOFF and CONNECTING are both                  `connected: false`, and an operator deciding whether to intervene needs to know which"
    );
    assert!(
        cold.iter()
            .all(|camera| camera.attributes.contains_key("backend")
                && camera.attributes.contains_key("generation")),
        "the open bag carries what only a camera adapter understands"
    );

    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let warm = runtime.camera_connectivity();
    let connected = warm
        .iter()
        .find(|camera| camera.instance == "camera-a")
        .expect("camera-a must still be reported");
    assert!(
        connected.connected,
        "an online camera must be reported as connected"
    );
    assert_eq!(
        connected.state.as_deref(),
        Some("ONLINE"),
        "and it must publish its own condition token, not only the normalized flag"
    );
    assert!(
        connected.detail.is_none(),
        "a healthy camera has nothing to explain: this rides a keepalive published every                  few seconds, for every camera, forever"
    );
    assert!(
        warm.iter()
            .find(|camera| camera.instance == "camera-b")
            .is_some_and(|camera| !camera.connected),
        "and the camera that never started must still be reported as down"
    );

    runtime.shutdown().await;
}

/// Q2: the component emitted no metrics at all.
///
/// There was not one call site for `metrics()`, `MetricBuilder`, or `MetricService` anywhere
/// in the crate. Every number an operator would want -- what is queued, what is running, what
/// succeeded, what failed -- existed inside the process and left no trace outside it. A
/// failed capture was a log line.
///
/// So the assertion that matters is not that the numbers are right, it is that the wiring
/// exists: a metric nobody emits is indistinguishable from a metric that does not exist, and
/// nothing in the build could tell the difference.
#[tokio::test]
async fn captures_are_counted_as_they_happen() {
    let directory = TempDir::new().unwrap();
    let (runtime, metrics) =
        runtime_with_metrics(config(directory.path(), &["camera-a"], false), &directory)
            .await;

    {
        use edgecommons::metrics::MetricService as _;
        assert!(
            metrics.is_metric_defined(crate::observability::CAPTURE_METRIC)
                && metrics.is_metric_defined(crate::observability::QUEUE_METRIC),
            "both metrics must be defined at startup, not on first use"
        );
    }

    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "counted".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "counted-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected a newly accepted capture, got {other:?}"),
    };
    let terminal = wait_for_terminal(&runtime, &capture_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);

    let counted = crate::observability::CAPTURE_METRIC;

    // The outcome counter is emitted AFTER the durable terminal write, so a terminal job does not yet
    // imply a counted one. Sampling here once made this test right almost always and wrong under load,
    // which is the worst kind of test. Wait for the count instead of assuming it has landed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while metrics.counts(counted, "succeeded") < 1.0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the capture succeeded but was never counted -- this is the number an operator watches"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(
        metrics.counts(counted, "queued"),
        1.0,
        "an accepted capture must be counted when it is accepted"
    );
    assert_eq!(
        metrics.counts(counted, "started"),
        1.0,
        "and again when it actually starts doing physical work"
    );
    assert_eq!(
        metrics.counts(counted, "succeeded"),
        1.0,
        "and its outcome must be counted -- this is the number an operator watches"
    );
    assert_eq!(
        metrics.counts(counted, "failed"),
        0.0,
        "a capture that succeeded must not also be counted as a failure"
    );

    runtime.shutdown().await;
}

/// Q3/Q4: the operator can finally SEE the queue, and drain it.
///
/// Every number here already existed. `AdmissionSnapshot` was compiled only into
/// `cfg(all(test, linux, standalone, onvif, capacity-harness))`, so the sole consumer was the
/// capacity harness -- the observability had been built for the test rather than for the
/// person holding the pager, and an operator watching a backlog run away had no way to ask
/// how deep it was or to stop it.
#[tokio::test]
async fn queue_status_reports_the_backlog_and_queue_clear_drains_it() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.limits.max_queued_captures_per_camera = 4;
    let runtime = runtime(configuration, &directory).await;

    // No supervisor: the captures sit exactly where a backlog for an offline camera sits.
    for index in 0..3 {
        runtime
            .submit_capture(
                "camera-a".to_string(),
                format!("backlog-{index}"),
                None,
                None,
                serde_json::Map::new(),
                format!("backlog-correlation-{index}"),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .expect("captures must be accepted");
    }

    let status = runtime.queue_status(None).await.unwrap();
    assert_eq!(
        status.durable_backlog, 3,
        "the durable backlog is what survives a restart"
    );
    assert_eq!(status.durable_in_flight, 0);
    assert_eq!(
        status.dispatch_queued, 3,
        "and the dispatcher is holding all three"
    );
    assert_eq!(
        status.cameras,
        vec![CameraQueueDepth {
            instance: "camera-a".to_string(),
            queued: 3,
            capacity: 4,
        }],
        "a camera at queued == capacity is the one answering QUEUE_FULL, so both are reported"
    );
    assert_eq!(
        status.limits.max_concurrent_captures, status.admission.available_acquisitions,
        "nothing is acquiring yet, so every acquisition permit must still be free"
    );

    // Break glass.
    let outcome = runtime
        .clear_queue(None, false, "operator drain".to_string())
        .await
        .unwrap();
    assert_eq!(outcome.cancelled, 3);
    assert!(
        outcome.failed.is_empty(),
        "the drain must not leave work behind silently"
    );

    let drained = runtime.queue_status(None).await.unwrap();
    assert_eq!(
        drained.durable_backlog, 0,
        "the durable backlog is gone, not just forgotten"
    );
    // The descriptors go too, but not synchronously: each one is removed by the watcher task
    // its enqueue spawned, which is what releases work whose camera is offline and whose queue
    // nobody is polling. Demanding it of the very next line is a race with a task that has not
    // run yet -- and that race is what failed once on CI and never here.
    wait_for_queue_depth(&runtime, 0).await;
    assert_eq!(
        runtime.queue_status(None).await.unwrap().dispatch_queued,
        0,
        "and the descriptors must not still be occupying the camera's queue slots"
    );
}

/// Rebasing a deadline must not disarm it: a capture that runs out of time still fails.
///
/// The terminal-deadline task re-reads its deadline instead of firing on the one it was
/// spawned with, because a capture's clocks are rebased when a camera finally takes it. A
/// re-reading task that never fires would be worse than the stale one it replaced: captures
/// would wait forever instead of dying early. It still fires -- just on the clock in force.
#[tokio::test]
async fn a_capture_that_runs_out_of_time_while_it_waits_still_fails() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    // The camera never comes online, so this capture waits until its terminal clock runs out.
    // The stage clocks must fit inside the terminal one, so they come down with it.
    configuration.global.timeouts.job_terminal_ms = 1_000;
    configuration.global.timeouts.capture_ms = 500;
    configuration.global.timeouts.encode_ms = 500;
    configuration.global.timeouts.persist_ms = 500;
    let runtime = runtime(configuration, &directory).await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "waits-forever".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "timeout-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record)
        | crate::catalog::AcceptJobOutcome::Existing(record) => record.capture_id,
        crate::catalog::AcceptJobOutcome::Conflict => panic!("unexpected ledger conflict"),
    };

    let record =
        wait_for_terminal_within(&runtime, &capture, Duration::from_secs(20)).await;
    assert_eq!(record.state, crate::model::JobState::Failed);
    assert_eq!(
        record.error_code.as_deref(),
        Some(crate::ErrorCode::CaptureTimeout.as_str()),
        "a capture nobody can run must still be retired by its deadline"
    );
}

/// The other half of the rule: a capture that really IS retired must be dropped.
///
/// The requeue path exists so a store hiccup cannot destroy a capture. It must not become a
/// path that resurrects one. A cancelled capture is no longer QUEUED, the catalog says so with
/// an invariant error rather than a store error, and the scheduler must let it go -- otherwise
/// it pops, fails to rebase, requeues, and spins on a capture nobody is waiting for.
#[tokio::test]
async fn a_capture_already_retired_is_dropped_not_requeued() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a"], false);
    let runtime = runtime(configuration, &directory).await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "retired-before-dispatch".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "retired-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record)
        | crate::catalog::AcceptJobOutcome::Existing(record) => record.capture_id,
        crate::catalog::AcceptJobOutcome::Conflict => panic!("unexpected ledger conflict"),
    };
    assert_eq!(runtime.scheduler.pending(), 1);

    // Retired while it waited, before any camera could take it -- through the real cancel
    // command, not a hand-built terminal row.
    runtime
        .cancel_capture(CancelRequest {
            request_id: "retire-before-dispatch".to_string(),
            capture_id: Some(capture.clone()),
            capture_group_id: None,
            reason: Some("retired while queued".to_string()),
        })
        .await
        .unwrap();

    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        runtime.scheduler.pending(),
        0,
        "a cancelled capture is not owed a run and must not be put back on the queue"
    );
    let record = runtime.catalog.job(&capture).await.unwrap().unwrap();
    assert_eq!(record.state, crate::model::JobState::Cancelled);
}

/// A group schedule resumes from its durable cursor rather than from "now".
///
/// The cursor is what tells a restarted component the difference between an occurrence it
/// missed while it was down and one that has not come due. A schedule that ignored it would
/// simply start afresh, and the outage would be invisible.
///
/// What this asserts is that the cursor was READ and moved on -- not what was decided about
/// the occurrences it found. That decision belongs to the misfire rule, and it is decided on
/// wall-clock time: whether the last hourly occurrence is older than the five-second grace is
/// true for 3595 seconds out of every 3600 and false for the other five. Asserting it here
/// made this test pass 99.9% of the time and fail on a CI run that happened to start at
/// 23:00:01. The rule itself is now proved where its clock can be held still, in
/// `scheduler::tests::occurrences_missed_while_the_component_was_down_are_skipped`.
#[tokio::test]
async fn a_group_schedule_resumes_from_its_durable_cursor() {
    let directory = TempDir::new().unwrap();
    let cameras = ["camera-a", "camera-b"];
    let mut configuration = config(directory.path(), &cameras, false);
    let mut schedule =
        group_schedule("line-a-sync", &cameras, crate::config::OverlapPolicy::Skip);
    // Hourly, so the occurrences the seeded cursor missed are hours old -- far past the
    // misfire grace -- and the next one is not due for the rest of the test.
    schedule.cron = "0 0 * * * *".to_string();
    configuration.global.capture_group_schedules = vec![schedule];
    let runtime = runtime(configuration, &directory).await;
    for camera in cameras {
        runtime
            .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
            .unwrap();
        wait_for_online(&runtime, camera).await;
    }

    // The component was down for three hours.
    let stale = chrono::Utc::now() - chrono::Duration::hours(3);
    runtime
        .catalog
        .record_group_schedule_occurrence(
            "line-a-sync",
            stale.timestamp_millis(),
            None,
            stale.timestamp_millis(),
        )
        .await
        .unwrap();

    runtime.start_schedulers().unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;

    let cursor = runtime
        .catalog
        .group_schedule_cursor("line-a-sync")
        .await
        .unwrap()
        .expect("the schedule consumed the occurrences it missed");
    assert!(
        cursor.intended_fire_time_ms > stale.timestamp_millis(),
        "the schedule resumed from the durable cursor and consumed what it had missed, \
         rather than starting afresh as though the outage never happened"
    );
}

/// A group schedule that cannot read its recovery cursor does not run on a guessed one.
///
/// The cursor is what separates an occurrence the component missed while it was down from one
/// that has not come due. Without it the schedule cannot tell those apart, so it stops rather
/// than firing a synchronised capture at a line on a cursor it invented.
#[tokio::test]
async fn a_group_schedule_that_cannot_read_its_cursor_does_not_run() {
    let directory = TempDir::new().unwrap();
    let cameras = ["camera-a", "camera-b"];
    let mut configuration = config(directory.path(), &cameras, false);
    configuration.global.capture_group_schedules = vec![group_schedule(
        "line-a-sync",
        &cameras,
        crate::config::OverlapPolicy::Skip,
    )];
    let runtime = runtime(configuration, &directory).await;
    for camera in cameras {
        runtime
            .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
            .unwrap();
        wait_for_online(&runtime, camera).await;
    }

    let database = directory
        .path()
        .join("state")
        .join("camera-adapter.sqlite3");
    let break_store = rusqlite::Connection::open(&database).unwrap();
    break_store
        .execute_batch(
            "ALTER TABLE group_schedule_cursors RENAME TO group_schedule_cursors_unavailable",
        )
        .unwrap();

    runtime.start_schedulers().unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    break_store
        .execute_batch(
            "ALTER TABLE group_schedule_cursors_unavailable RENAME TO group_schedule_cursors",
        )
        .unwrap();
    assert!(
        runtime
            .catalog
            .group_schedule_cursor("line-a-sync")
            .await
            .unwrap()
            .is_none(),
        "a schedule that could not read its cursor must not have fired anything"
    );
}

/// A catalog that cannot answer "is the previous group still running?" fails CLOSED.
///
/// Firing a second synchronised group on top of one that may still be moving cameras is worse
/// than missing a cycle, so an unreadable catalog skips the occurrence rather than assuming
/// the coast is clear.
#[tokio::test]
async fn group_overlap_fails_closed_when_the_catalog_cannot_answer() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(configuration, &directory).await;

    let database = directory
        .path()
        .join("state")
        .join("camera-adapter.sqlite3");
    let break_store = rusqlite::Connection::open(&database).unwrap();
    break_store
        .execute_batch("ALTER TABLE capture_groups RENAME TO capture_groups_unavailable")
        .unwrap();

    assert!(
        runtime
            .group_schedule_overlaps(Some("grp_unknowable"))
            .await,
        "a catalog that cannot answer is not permission to fire another group"
    );

    // And a cursor the store cannot write is survivable: the command ledger, not the cursor,
    // is what makes an occurrence exactly-once, so the schedule logs and carries on rather
    // than dying on a recovery hint.
    break_store
        .execute_batch(
            "ALTER TABLE group_schedule_cursors RENAME TO group_schedule_cursors_unavailable",
        )
        .unwrap();
    runtime
        .record_group_occurrence("line-a-sync", chrono::Utc::now(), None)
        .await;

    break_store
        .execute_batch(
            "ALTER TABLE group_schedule_cursors_unavailable RENAME TO group_schedule_cursors",
        )
        .unwrap();
    break_store
        .execute_batch("ALTER TABLE capture_groups_unavailable RENAME TO capture_groups")
        .unwrap();
}

/// An occurrence of a schedule that has been disabled or removed is not submitted.
#[tokio::test]
async fn an_occurrence_of_a_removed_group_schedule_is_not_submitted() {
    let directory = TempDir::new().unwrap();
    let cameras = ["camera-a", "camera-b"];
    let mut configuration = config(directory.path(), &cameras, false);
    let mut disabled =
        group_schedule("line-a-sync", &cameras, crate::config::OverlapPolicy::Skip);
    disabled.enabled = false;
    configuration.global.capture_group_schedules = vec![disabled];
    let runtime = runtime(configuration, &directory).await;

    let fire_time = chrono::DateTime::from_timestamp_millis(1_752_000_000_000).unwrap();
    let occurrence = ScheduleOccurrence {
        scope: crate::scheduler::ScheduleScope::Group("line-a-sync".to_string()),
        schedule_id: "line-a-sync".to_string(),
        intended_fire_time: fire_time,
        admit_at: fire_time,
        jitter: Duration::ZERO,
    };
    let error = runtime
        .submit_scheduled_group(&occurrence)
        .await
        .expect_err("a disabled schedule must not fire");
    assert_eq!(error.code(), crate::ErrorCode::InvalidRequest);

    let unknown = ScheduleOccurrence {
        scope: crate::scheduler::ScheduleScope::Group("gone".to_string()),
        schedule_id: "gone".to_string(),
        intended_fire_time: fire_time,
        admit_at: fire_time,
        jitter: Duration::ZERO,
    };
    assert!(
        runtime.submit_scheduled_group(&unknown).await.is_err(),
        "a schedule removed by a reload must not fire"
    );

    // A camera schedule occurrence is not a group, and must not be submitted as one.
    let camera_scoped = ScheduleOccurrence {
        scope: crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
        schedule_id: "minute".to_string(),
        intended_fire_time: fire_time,
        admit_at: fire_time,
        jitter: Duration::ZERO,
    };
    assert!(runtime.submit_scheduled(&camera_scoped).await.is_err());

    // And the converse: a GROUP occurrence has no single camera, so the camera path must
    // refuse it rather than pick one.
    assert!(
        runtime.submit_scheduled(&occurrence).await.is_err(),
        "a group occurrence cannot be submitted as a single-camera capture"
    );
}

/// A capture the STORE could not rebase goes back on the queue -- it is not destroyed.
///
/// This is B5 wearing a different hat, and the fleet queue rebuilt it. The scheduler pops a
/// capture, rebases its clocks onto the moment a camera actually took it, and hands it over.
/// When that durable write failed, the code treated it as "this capture is already terminal
/// or gone" and dropped the descriptor. It is not gone: a transient `SQLITE_BUSY` under load
/// says nothing about the capture, only about the store. The durable row stays QUEUED, the
/// only in-memory thing that could have driven it has been destroyed, and the capture waits
/// for a deadline that will fail it. It cost a real member of a real group, and it only
/// showed up under load -- which is exactly when a contended catalog returns BUSY.
///
/// The store failure here is real, not injected into production code: the `jobs` table is
/// renamed out from under the catalog, so the rebase UPDATE fails with a genuine
/// `CameraError::Sqlite`, and is then renamed back. A store that stops answering and starts
/// again is precisely the case that must not lose work.
#[tokio::test]
async fn a_capture_the_store_could_not_rebase_is_requeued_not_destroyed() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a"], false);
    let runtime = runtime(configuration, &directory).await;

    // The camera is deliberately offline, so the capture is accepted, durably QUEUED, and
    // parked in the fleet queue with nothing yet able to take it.
    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "stranded-by-the-store".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "store-failure-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("the capture is accepted while the camera is offline");
    let capture = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record)
        | crate::catalog::AcceptJobOutcome::Existing(record) => record.capture_id,
        crate::catalog::AcceptJobOutcome::Conflict => panic!("unexpected ledger conflict"),
    };
    assert_eq!(runtime.scheduler.pending(), 1);

    // The store stops being able to serve the rebase. Done through a SEPARATE connection to
    // the same database, so no production type grows a fault-injection hook for this.
    let database = directory
        .path()
        .join("state")
        .join("camera-adapter.sqlite3");
    let break_store = rusqlite::Connection::open(&database).unwrap();
    break_store
        .execute_batch("ALTER TABLE jobs RENAME TO jobs_unavailable")
        .unwrap();

    // The camera comes online. The scheduler pops the capture, fails to rebase it, and must
    // put it back: with the defect it drops the descriptor here and `pending()` falls to 0
    // for the rest of the process.
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    // Sampling ONCE after a fixed sleep asks the wrong question. The scheduler pops the
    // descriptor, fails the rebase, and pushes it back -- so there is a real instant in which it
    // is in flight and `pending()` is legitimately 0, and a loaded machine can land the sample
    // exactly there. The defect this guards is not a momentary dip: it is a descriptor that is
    // gone FOREVER. So require the queue to SETTLE at 1 and stay there.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut settled = 0;
    loop {
        if runtime.scheduler.pending() == 1 {
            settled += 1;
            if settled == 5 {
                break;
            }
        } else {
            settled = 0;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a capture the store could not rebase is still owed a run and must stay queued;                      the queue never settled back to it"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The store recovers. The capture must actually run -- being requeued is only half of
    // the promise.
    break_store
        .execute_batch("ALTER TABLE jobs_unavailable RENAME TO jobs")
        .unwrap();
    drop(break_store);

    let record =
        wait_for_terminal_within(&runtime, &capture, Duration::from_secs(20)).await;
    assert_eq!(
        record.state,
        crate::model::JobState::Succeeded,
        "once the store recovers, the capture it could not rebase must still run"
    );
}

/// The drain reaches durable work regardless of whether an unknown camera is named.
#[tokio::test]
async fn queue_status_and_clear_reject_an_unknown_camera() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    assert_eq!(
        runtime
            .queue_status(Some("camera-nope".to_string()))
            .await
            .expect_err("an unknown camera must be rejected, not reported as empty")
            .code(),
        crate::ErrorCode::UnknownInstance
    );
    assert_eq!(
        runtime
            .clear_queue(Some("camera-nope".to_string()), false, "drain".to_string())
            .await
            .expect_err("an unknown camera must be rejected")
            .code(),
        crate::ErrorCode::UnknownInstance
    );
}

/// G1: an oversized group takes LONGER. It does not partially fail.
///
/// `submit_group` used to build every member and hand them all to their cameras at once. A
/// group larger than the component could serve did not degrade -- it broke: the members that
/// could not be dispatched immediately sat with their 30-second capture clock already
/// running, and died of CAPTURE_TIMEOUT the moment a camera was free to take them. An
/// oversized concurrent request silently became "most of your members failed", which the
/// group contract -- all-or-nothing acceptance, one collated result -- does not permit.
///
/// Nothing in this fix is a wave scheduler. A wave is "as many as capacity allows", and that
/// is what a central queue does by construction: the members beyond capacity simply wait, and
/// each one's clocks start when a camera actually takes it. The whole of G1 is Q1 plus
/// rebasing.
///
/// This group is deliberately wider than the component's execution capacity: four cameras
/// against a single concurrent-capture permit. Every member must still succeed.
fn group_schedule(
    id: &str,
    cameras: &[&str],
    overlap: crate::config::OverlapPolicy,
) -> crate::config::CaptureGroupScheduleConfig {
    crate::config::CaptureGroupScheduleConfig {
        id: id.to_string(),
        enabled: true,
        // Every second: the loop polls at 200 ms, so a test does not wait on a wall clock.
        cron: "* * * * * *".to_string(),
        timezone: "UTC".to_string(),
        instances: cameras.iter().map(|camera| (*camera).to_string()).collect(),
        capture_profile: None,
        profile_overrides: BTreeMap::new(),
        misfire_policy: crate::config::MisfirePolicy::Skip,
        overlap_policy: overlap,
        jitter_seconds: 0,
        timeout_ms: None,
    }
}

/// Waits for a group schedule to admit an occurrence, and returns the group it admitted.
///
/// Read from the schedule's own durable cursor, which is also the thing that has to survive
/// a restart -- so a green wait here is evidence for both.
async fn wait_for_scheduled_group(runtime: &CameraRuntime, schedule_id: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(Some(cursor)) = runtime.catalog.group_schedule_cursor(schedule_id).await {
            if let Some(group_id) = cursor.last_group_id {
                return group_id;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "group schedule '{schedule_id}' never admitted an occurrence"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn ptz_camera(directory: &TempDir, max_move_ms: u64) -> AdapterConfig {
    let mut configuration = config(directory.path(), &["camera-a"], false);
    for camera in &mut configuration.instances {
        camera.ptz.enabled = true;
        camera.ptz.maximum_continuous_move_ms = max_move_ms;
        if let crate::config::BackendConfig::Sim(sim) = &mut camera.backend {
            sim.ptz.supported = true;
            sim.ptz.status_supported = true;
        }
    }
    configuration
}

async fn camera_is_moving(runtime: &CameraRuntime) -> bool {
    match runtime
        .actor("camera-a")
        .unwrap()
        .ptz(
            crate::model::PtzRequest::Status,
            tokio::time::Instant::now() + Duration::from_secs(2),
            &CancellationToken::new(),
        )
        .await
    {
        Ok(crate::model::PtzResult::Status(status)) => status.moving == Some(true),
        other => panic!("the camera must answer a PTZ status: {other:?}"),
    }
}

/// A continuous move is stopped by its deadline, even if the requester never comes back.
///
/// DESIGN §15.5 is a sequence diagram with a Stop timer in it: the move arms a mandatory stop
/// deadline, and the DEADLINE stops the camera -- not the requester, who may have crashed, and
/// not the camera itself, which is told the timeout but may ignore it, as many do. The timer
/// was never built. `maximumContinuousMoveMs` existed only to decorate the reply with a
/// `stopDeadline` that nothing was going to honour, and the safety lane the stop belongs in
/// had no producer at all.
///
/// So: command a move, then walk away. Nobody sends a stop. The camera must stop anyway.
#[tokio::test]
async fn a_continuous_move_is_stopped_by_its_deadline_with_nobody_asking() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let request: PtzCommandRequest = crate::commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "estop-timer",
        "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 300
    }))
    .unwrap();
    let reply = runtime.perform_ptz(request).await.unwrap();
    assert_eq!(reply["state"], "COMMANDED");
    assert!(
        !reply["stopDeadline"].is_null(),
        "a move that is armed to stop must say when"
    );
    assert!(
        camera_is_moving(&runtime).await,
        "the camera must actually be moving before the deadline"
    );

    // The requester disappears. Nothing else asks for a stop.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if !camera_is_moving(&runtime).await {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the camera is still moving long past its mandatory stop deadline"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    runtime.shutdown().await;
}

/// A group member reports the size of the group it is actually in.
///
/// `build_group_submission` used to return `group_size: Some(2)` and trust its caller to
/// overwrite it with the real member count. Two is not a placeholder that fails loudly -- it is
/// the smallest LEGAL group size, chosen so the durable layer's `size >= 2` check would wave it
/// through. A second caller, or a reordering that pushed the submission before the fix-up line,
/// would ship a five-camera group as a two-member group and nothing anywhere would object.
///
/// The builder is now told the size, so there is no value for it to invent.
#[tokio::test]
async fn a_group_member_carries_the_size_of_its_own_group() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(
        config(directory.path(), &["camera-a", "camera-b", "camera-c"], false),
        &directory,
    )
    .await;

    let submission = runtime
        .build_group_submission(
            "camera-a",
            3,
            "group-size-request",
            "grp_size_check",
            None,
            None,
            serde_json::Map::new(),
            "group-size-correlation".to_string(),
        )
        .await
        .expect("the group member must be built");

    assert_eq!(
        submission.spec.group_size,
        Some(3),
        "a member of a three-camera group must say so; reporting two is what the placeholder \
         did, and two is a legal size, so nothing downstream would have caught it"
    );
    runtime.shutdown().await;
}

/// A camera dropped from the roster leaves NOTHING behind.
///
/// This is what the consolidation buys, and it is not a tidiness argument. Removing a camera was
/// six hand-written `retain`s over six separate maps, and it forgot two of them -- `actors` and
/// `motion_stops`. The second omission is how a camera removed mid-move came to keep an armed
/// mandatory stop that nothing would ever deliver, and the camera kept moving.
///
/// A camera is one entry now. Its engine, its facade, its supervisor, its session and its armed
/// stop leave together, because they are one thing. Forgetting one of them is no longer a move
/// that exists.
#[tokio::test]
async fn removing_a_camera_removes_all_of_it() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    // Give the camera every kind of state it can have: a supervisor, a session, and an armed
    // mandatory stop with its deadline still seconds away.
    let request: PtzCommandRequest = crate::commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "estop-removal",
        "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 9_000
    }))
    .unwrap();
    runtime.perform_ptz(request).await.unwrap();
    {
        let cameras = runtime.cameras.read().unwrap();
        let slot = cameras.get("camera-a").expect("the camera exists");
        assert!(slot.supervisor.is_some(), "it has a supervisor");
        assert!(slot.session.is_some(), "it is connected");
        assert!(slot.motion_stop.is_some(), "and it is moving");
    }

    // Drop it from the roster.
    let mut replacement = ptz_camera(&directory, 10_000);
    replacement.instances.clear();
    let diff = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .expect("removing a camera is a valid reload");
    assert_eq!(diff.removed, vec!["camera-a".to_string()]);

    assert!(
        runtime.cameras.read().unwrap().is_empty(),
        "a camera dropped from the roster must leave nothing behind -- and the two the old \
         removal path forgot were the actor handle and the armed mandatory stop, which is how \
         a camera came to be left moving with nothing alive to stop it"
    );
    runtime.shutdown().await;
}

/// The stop reaches a camera whose actor is about to be taken away.
///
/// The mandatory-stop timer resolves its actor LAZILY, when the deadline fires -- right for a
/// reconnect, and catastrophic for everything else. Retire the supervisor, disable the camera,
/// drop it from the roster, or shut the component down, and the timer wakes to find no actor,
/// logs that the camera "is gone", and returns. The camera is not gone. It is still moving.
///
/// So there has to be a producer that delivers the stop while an actor still exists. This is
/// that producer, against a camera that is genuinely moving and whose deadline is nine seconds
/// away: nothing else is going to stop it, and it must stop.
#[tokio::test]
async fn a_moving_camera_is_stopped_through_the_actor_it_is_about_to_lose() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let request: PtzCommandRequest = crate::commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "estop-retire",
        "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 9_000
    }))
    .unwrap();
    runtime.perform_ptz(request).await.unwrap();
    assert!(
        camera_is_moving(&runtime).await,
        "the camera must actually be moving, or this proves nothing"
    );

    assert_eq!(
        runtime.deliver_mandatory_stops(None),
        1,
        "the moving camera must be sent its stop through the actor it still has"
    );

    // And it has to REACH the camera -- a stop that is queued and never delivered is not a stop.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if !camera_is_moving(&runtime).await {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the camera is still moving; its mandatory deadline is seconds away and nothing \
             else is coming to stop it"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    runtime.shutdown().await;
}

/// DESIGN 20.2 step 2: shutting down sends a stop to a camera in continuous motion.
///
/// It did not. `shutdown` cancelled the root token and joined its tasks; the word "stop" did
/// not appear in it. The mandatory-stop timer, seeing that cancellation, deliberately STOOD
/// DOWN -- on the stated belief that "the actor's own teardown delivers a stop to a camera that
/// is still moving". The actor's teardown delivers safety stops that were already QUEUED, and
/// on the shutdown path nobody ever queued one: the timer was the only producer of a
/// `SafetyStop` in the entire component, and it had just declined to produce.
///
/// So the component could be shut down while a camera was executing a continuous pan, and the
/// camera would go on panning. The machinery was there, tested, and fed by nothing.
#[tokio::test]
async fn shutting_down_does_not_walk_away_from_a_moving_camera() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let request: PtzCommandRequest = crate::commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "estop-shutdown",
        "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 9_000
    }))
    .unwrap();
    runtime.perform_ptz(request).await.unwrap();
    assert!(
        camera_is_moving(&runtime).await,
        "the camera must actually be moving, or this proves nothing"
    );
    assert!(
        runtime
            .cameras
            .read()
            .unwrap()
            .get("camera-a")
            .is_some_and(|slot| slot.motion_stop.is_some()),
        "the move must be armed before shutdown, or this proves nothing"
    );

    runtime.shutdown().await;

    // Drained means DELIVERED: `deliver_mandatory_stops` is the only thing on this path that
    // empties the map, and it empties it by pushing the stop at the camera's actor -- which
    // then hands it to the transport even though cancellation has already been tripped
    // (`drain_controls_for_shutdown`, proven in
    // `cancelled_actor_runs_its_safety_stop_and_session_close_within_the_grace`).
    //
    // Before the fix the entry SURVIVED shutdown, because the timer holding it returned
    // without stopping anything and nothing else touched it.
    assert!(
        runtime
            .cameras
            .read()
            .unwrap()
            .values()
            .all(|slot| slot.motion_stop.is_none()),
        "the component shut down and left a camera armed, moving, and unstopped"
    );
}

/// A supervisor being retired stops its camera before it takes the actor away.
///
/// A config reload retires supervisors. If the camera is mid-move, cancelling its supervisor
/// destroys the only actor its mandatory stop could ever have been delivered through -- and the
/// timer, firing later, would find nothing and give up. The field's own documentation said "a
/// retiring supervisor" could disarm the timer. Nothing did.
#[tokio::test]
async fn a_retiring_supervisor_stops_its_camera_before_it_disappears() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let request: PtzCommandRequest = crate::commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "estop-reload",
        "velocity": { "pan": 0.0, "tilt": 0.4, "zoom": 0.0 },
        "timeoutMs": 9_000
    }))
    .unwrap();
    runtime.perform_ptz(request).await.unwrap();
    assert!(camera_is_moving(&runtime).await);

    runtime
        .replace_supervisors(&["camera-a".to_string()], Duration::from_secs(10))
        .await
        .expect("the supervisor must retire");

    assert!(
        runtime
            .cameras
            .read()
            .unwrap()
            .values()
            .all(|slot| slot.motion_stop.is_none()),
        "a supervisor was retired out from under a moving camera and the stop it was owed \
         went with it"
    );
    runtime.shutdown().await;
}

/// An explicit stop retires the timer, so it cannot stop a LATER move.
///
/// Arm, stop, move again: the first move's timer must not fire into the second move and cut
/// it short. A safety timer that stops the wrong motion is its own hazard.
#[tokio::test]
async fn a_stopped_move_does_not_leave_a_timer_that_stops_the_next_one() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let first: PtzCommandRequest = crate::commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "first-move",
        "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 300
    }))
    .unwrap();
    runtime.perform_ptz(first).await.unwrap();

    let stop: PtzCommandRequest = crate::commands::parse_closed(json!({
        "operation": "stop",
        "instance": "camera-a",
        "requestId": "explicit-stop",
        "axes": ["pan", "tilt", "zoom"]
    }))
    .unwrap();
    runtime.perform_ptz(stop).await.unwrap();

    // A new move, well inside the first one's now-retired deadline.
    let second: PtzCommandRequest = crate::commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "second-move",
        "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 5_000
    }))
    .unwrap();
    runtime.perform_ptz(second).await.unwrap();

    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        camera_is_moving(&runtime).await,
        "the first move's timer must not stop the second move"
    );
    runtime.shutdown().await;
}

/// The safety bound belongs to the camera, not to a constant in the code.
///
/// The PTZ command was validated against a hardcoded 60 000 ms -- the widest value the field
/// accepts -- so a camera configured to move for one second would happily accept a command to
/// move for a minute, and then advertise a `stopDeadline` one second out that nothing honoured.
#[tokio::test]
async fn a_continuous_move_cannot_outlast_this_cameras_configured_bound() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 1_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let too_long: PtzCommandRequest = crate::commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "over-the-bound",
        "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
        // Well inside the schema ceiling of 60 s, and six times this camera's bound.
        "timeoutMs": 6_000
    }))
    .unwrap();
    let error = runtime
        .perform_ptz(too_long)
        .await
        .expect_err("a camera bounded to one second must not accept a six-second move");
    assert_eq!(error.code(), crate::ErrorCode::PtzRangeError);
    assert!(
        !camera_is_moving(&runtime).await,
        "a rejected move must not have moved the camera"
    );
    runtime.shutdown().await;
}

/// N9: the adapter emits `southbound_health`, per camera, and it says something true.
///
/// SOUTHBOUND §5 and DESIGN §19.1 both state that every adapter emits this metric dimensioned
/// by `instance`; DESIGN.md:854 even marks `healthThresholds.staleSignalSecs` as "live; drives
/// `southbound_health.staleSignals`". None of it existed. `CameraHealthTracker` was written to
/// produce exactly these measures and had no callers at all, so no camera could report itself
/// stale and no operator could alarm on one.
///
/// The dimension is the part that is easy to get wrong and easy to not notice: the core metric
/// API carries dimensions on the DEFINITION and keys definitions by name, so it takes real care
/// to say `instance=camera-b` rather than emit every camera's health into one nameless stream.
/// The canonical Java protocol-adapter template does not manage it -- its `emitHealth` takes an
/// instance id and never uses it.
#[tokio::test]
async fn southbound_health_is_emitted_per_camera_and_reports_the_capture_it_saw() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let (runtime, metrics) = runtime_with_metrics(configuration, &directory).await;
    for camera in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
            .unwrap();
        wait_for_online(&runtime, camera).await;
    }

    // One camera does some work; the other does none.
    let accepted = runtime
        .submit_capture(
            "camera-b".to_string(),
            "health-capture".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "health-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record)
        | crate::catalog::AcceptJobOutcome::Existing(record) => record.capture_id,
        crate::catalog::AcceptJobOutcome::Conflict => panic!("unexpected ledger conflict"),
    };
    let record =
        wait_for_terminal_within(&runtime, &capture, Duration::from_secs(20)).await;
    assert_eq!(record.state, crate::model::JobState::Succeeded);

    runtime.sample_southbound_health(false).await;

    for camera in ["camera-a", "camera-b"] {
        let samples = metrics.health_for(camera);
        assert!(
            !samples.is_empty(),
            "every configured camera must report its own southbound health: {camera}"
        );
        let latest = samples.last().unwrap();
        assert_eq!(
            latest.values.get("connectionState"),
            Some(&1.0),
            "{camera} is online and must say so"
        );
        assert_eq!(
            latest.values.get("staleSignals"),
            Some(&0.0),
            "{camera} has answered inside the stale threshold"
        );
    }

    // The camera that captured reports the round-trip it measured; the idle one has no
    // latency to report and must not invent a zero.
    let busy = metrics.health_for("camera-b");
    assert!(
        busy.iter()
            .any(|emission| emission.values.contains_key("pollLatencyMs")),
        "the camera that answered must report the round-trip it took"
    );
    let idle = metrics.health_for("camera-a");
    assert!(
        idle.iter()
            .all(|emission| !emission.values.contains_key("pollLatencyMs")),
        "a camera that has never been polled must not report a latency of zero"
    );
    runtime.shutdown().await;
}

/// N9: a camera that drops and comes back counts a reconnect, and reports it at once.
///
/// The first connection of a camera's life is NOT a reconnect. Counting it as one would put a
/// reconnect against every camera in the fleet every time the component starts -- which is the
/// shape of a fleet-wide outage, and would train an operator to ignore the measure.
///
/// The transition is emitted immediately rather than at the next sample: a camera that just
/// went down is not something to hear about thirty seconds late.
#[tokio::test]
async fn a_camera_that_drops_and_returns_counts_a_reconnect_but_its_first_connect_does_not()
{
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a"], false);
    let (runtime, metrics) = runtime_with_metrics(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    // The supervisor's own first connect brought it ONLINE. That is not a reconnect.
    runtime.sample_southbound_health(false).await;
    let first = metrics
        .health_for("camera-a")
        .last()
        .cloned()
        .expect("the camera reports its health");
    assert_eq!(
        first.values.get("reconnects"),
        Some(&0.0),
        "a camera's first connection is not a reconnect"
    );

    let generation = runtime.registry.snapshot("camera-a").unwrap().generation;
    runtime.publish_camera_state(
        "camera-a",
        generation,
        CameraConnectionState::Backoff,
        None,
        None,
        chrono::Utc::now(),
    );
    runtime.publish_camera_state(
        "camera-a",
        generation,
        CameraConnectionState::Online,
        None,
        None,
        chrono::Utc::now(),
    );

    // The transition is emitted immediately; give the detached emission a moment to land.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let down = metrics
        .health_for("camera-a")
        .iter()
        .any(|emission| emission.values.get("connectionState") == Some(&0.0));
    assert!(
        down,
        "the camera going down must be reported at once, not at the next sample"
    );

    runtime.sample_southbound_health(false).await;
    let latest = metrics.health_for("camera-a").last().cloned().unwrap();
    assert_eq!(
        latest.values.get("connectionState"),
        Some(&1.0),
        "it came back"
    );
    let reconnects: f64 = metrics
        .health_for("camera-a")
        .iter()
        .filter_map(|emission| emission.values.get("reconnects"))
        .sum();
    assert!(
        reconnects >= 1.0,
        "a camera that dropped and returned reconnected, and must say so: {reconnects}"
    );
    runtime.shutdown().await;
}

/// N9: a camera that has not answered inside the threshold says so.
///
/// This is the whole job of `healthThresholds.staleSignalSecs`, which decided nothing at all:
/// it was parsed, range-validated, and read by no code. A camera can be ONLINE and useless --
/// the session is up and nothing has come back from it -- and `staleSignals` is the only
/// measure that distinguishes that from a healthy one.
#[tokio::test]
async fn a_camera_that_has_not_answered_reports_itself_stale() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    // Nothing this camera has ever done is inside a zero-length threshold.
    configuration.global.health_thresholds.stale_signal_secs = 1;
    let (runtime, metrics) = runtime_with_metrics(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    // The camera has never produced a frame, and the threshold is one second.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    runtime.sample_southbound_health(false).await;

    let latest = metrics
        .health_for("camera-a")
        .last()
        .cloned()
        .expect("the camera reports its health");
    assert_eq!(
        latest.values.get("connectionState"),
        Some(&1.0),
        "the session is up"
    );
    assert_eq!(
        latest.values.get("staleSignals"),
        Some(&1.0),
        "a connected camera that has told us nothing is stale, and must say so"
    );
    runtime.shutdown().await;
}

/// N10: `maxDeferredWaitersPerCapture` bounds what it says it bounds.
///
/// A retried direct capture attaches ANOTHER caller to the same in-flight job (DESIGN §356),
/// and this limit exists to say how many. It bounded nothing: the list was pushed to
/// unconditionally, so a client that kept retrying grew it without limit -- and every one of
/// those tokens is held until the capture is terminal and then fanned out to.
#[tokio::test]
async fn a_capture_stops_accepting_callers_at_its_configured_bound() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a"], false);
    let runtime = runtime(configuration, &directory).await;
    runtime.waiters.set_waiter_limit(2);

    let capture = "cap-waiters";
    let attach = |waiter: &str| {
        runtime.waiters.register(
            capture.to_string(),
            waiter.to_string(),
            Arc::new(CountingWaiter::default()) as Arc<dyn CaptureWaiter>,
        )
    };

    attach("waiter-1").expect("the first caller attaches");
    attach("waiter-2").expect("the second caller fills the bound");
    let error = attach("waiter-3").expect_err("the third must be turned away");
    assert_eq!(error.code(), crate::ErrorCode::ResourceLimit);

    // And the bound is per capture, not global: a different capture still has room.
    runtime
        .waiters
        .register(
            "cap-other".to_string(),
            "waiter-4".to_string(),
            Arc::new(CountingWaiter::default()) as Arc<dyn CaptureWaiter>,
        )
        .expect("a different capture has its own bound");
    runtime.shutdown().await;
}

/// A cancelled capture lets go of the queue slot it was holding, even with its camera down.
///
/// This is the property the queue's per-entry watcher exists for, and it is worth pinning
/// because the obvious implementation does not have it: sweep cancelled descriptors only when
/// a consumer pops the queue, and a camera that is offline is never popped for -- so the
/// descriptor keeps its slot, and cancelling enough work on a camera that is down makes it
/// answer `QUEUE_FULL` for captures it could perfectly well take. Nothing pops here: no
/// supervisor is ever started.
#[tokio::test]
async fn a_cancelled_capture_releases_the_queue_slot_it_was_holding() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    // Room for exactly one waiting capture on this camera.
    configuration.global.limits.max_queued_captures_per_camera = 1;
    configuration.global.limits.max_pending_captures = 1;
    let runtime = runtime(configuration, &directory).await;

    // No supervisor: the camera is down, so nothing will ever pop this queue.
    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "doomed".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "cancel-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record)
        | crate::catalog::AcceptJobOutcome::Existing(record) => record.capture_id,
        crate::catalog::AcceptJobOutcome::Conflict => panic!("unexpected ledger conflict"),
    };
    assert_eq!(runtime.scheduler.pending(), 1);

    runtime
        .cancel_capture(CancelRequest {
            request_id: "cancel-the-doomed".to_string(),
            capture_id: Some(capture.clone()),
            capture_group_id: None,
            reason: Some("operator changed their mind".to_string()),
        })
        .await
        .unwrap();

    wait_for_queue_depth(&runtime, 0).await;
    assert_eq!(
        runtime.scheduler.pending_for("camera-a"),
        0,
        "the camera's own queue slot goes with it"
    );

    // And the freed slot is real: the camera takes new work, on a queue of exactly one.
    runtime
        .submit_capture(
            "camera-a".to_string(),
            "the-next-one".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "next-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("the slot the cancelled capture gave up is available to a live one");
    assert_eq!(runtime.scheduler.pending(), 1);
    runtime.shutdown().await;
}

/// Every verb has exactly one spelling, and it is the one the inbox registers.
///
/// The verb is part of the DURABLE idempotency key. It was typed out by hand at each ledger
/// call site, so a typo at any one of them silently opened a new idempotency namespace: the
/// retry a caller sends precisely to get exactly-once semantics would no longer find the
/// operation it was retrying, and the adapter would do it again. Nothing catches that -- not
/// the compiler, not a test, not a log line. A key is a key.
#[test]
fn every_command_verb_has_one_spelling_and_the_dispatch_knows_them_all() {
    let registered = camera_command_verbs();
    assert_eq!(registered.len(), CommandVerb::ALL.len());

    let mut seen = std::collections::BTreeSet::new();
    for verb in CommandVerb::ALL {
        assert!(
            verb.as_str().starts_with("sb/"),
            "{verb:?} is not a southbound verb"
        );
        assert!(
            seen.insert(verb.as_str()),
            "{verb:?} shares a wire spelling with another verb, so they share a durable                      idempotency namespace"
        );
        assert_eq!(
            CommandVerb::parse(verb.as_str()),
            Some(verb),
            "a verb the inbox registers must be one the router recognises"
        );
        assert!(registered.contains(&verb.as_str()));
    }
    assert_eq!(CommandVerb::parse("sb/capture-groups"), None);
    assert_eq!(CommandVerb::parse("sb/not-a-verb"), None);
}

/// A configuration snapshot shares the configuration; it does not copy it.
///
/// `config_snapshot()` deep-cloned the whole `AdapterConfig` -- every camera, every capture
/// profile, every backend allowlist -- from 34 call sites including the capture hot path, and
/// `lifecycle_events()` did it twice per capture to read a single bool. At 256 cameras that is
/// a quarter of a megabyte and thousands of allocations per call, and every one of them took
/// the read lock that a reload needs to write.
///
/// It is still a SNAPSHOT, and that is the half worth pinning: a reload swaps the pointer, so
/// a caller holding one keeps the configuration it started with. That was the only property
/// the deep clone was really providing, and sharing must not lose it.
#[tokio::test]
async fn a_config_snapshot_is_shared_and_still_a_snapshot() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    let first = runtime.config_snapshot().unwrap();
    let second = runtime.config_snapshot().unwrap();
    assert!(
        Arc::ptr_eq(&first, &second),
        "two snapshots of an unchanged configuration must be the same configuration,                  not two copies of it"
    );
    assert_eq!(first.instances.len(), 1);

    // A reload swaps the pointer. The snapshot taken before it keeps what it was given.
    let replacement = config(directory.path(), &["camera-a", "camera-b"], false);
    *runtime.config.write().unwrap() = Arc::new(replacement);

    assert_eq!(
        first.instances.len(),
        1,
        "a snapshot taken before a reload must still describe the world it was taken in"
    );
    let after = runtime.config_snapshot().unwrap();
    assert_eq!(
        after.instances.len(),
        2,
        "and a new one must see the new world"
    );
    assert!(!Arc::ptr_eq(&first, &after));
    runtime.shutdown().await;
}

/// The reaper does not give up on the one failure it exists to survive.
///
/// A capture that outlives its terminal deadline is retired by the deadline task. That task
/// threw its own error away and exited -- so when the durable store was briefly refusing
/// writes, which is precisely the condition it is there for, it did nothing at all: the
/// capture kept no terminal, and the runtime it was holding (a whole `CaptureJobSpec`) was
/// never released, for the life of the process.
///
/// Here the store is refusing writes when the deadline falls, and starts accepting them
/// again. The capture must still be retired, and the engine must let go of it.
#[tokio::test]
async fn an_expired_capture_is_retired_even_if_the_store_was_refusing_writes() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    // The camera never comes online, so this capture runs out of time where it waits.
    configuration.global.timeouts.job_terminal_ms = 1_000;
    configuration.global.timeouts.capture_ms = 500;
    configuration.global.timeouts.encode_ms = 500;
    configuration.global.timeouts.persist_ms = 500;
    let runtime = runtime(configuration, &directory).await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "expires-while-the-store-is-down".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "reaper-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record)
        | crate::catalog::AcceptJobOutcome::Existing(record) => record.capture_id,
        crate::catalog::AcceptJobOutcome::Conflict => panic!("unexpected ledger conflict"),
    };

    // The store stops accepting the write the reaper is about to make. Broken from a second
    // connection to the same database, so no production type grows a fault-injection hook.
    let database = directory
        .path()
        .join("state")
        .join("camera-adapter.sqlite3");
    let break_store = rusqlite::Connection::open(&database).unwrap();
    break_store
        .execute_batch("ALTER TABLE jobs RENAME TO jobs_unavailable")
        .unwrap();

    // The deadline falls while the store is unwell. The old reaper exited here, forever.
    tokio::time::sleep(Duration::from_millis(1_400)).await;
    assert!(
        runtime
            .engine("camera-a")
            .unwrap()
            .is_active_for_test(&capture),
        "the capture is still held while the store cannot retire it"
    );

    break_store
        .execute_batch("ALTER TABLE jobs_unavailable RENAME TO jobs")
        .unwrap();

    let record =
        wait_for_terminal_within(&runtime, &capture, Duration::from_secs(20)).await;
    assert_eq!(record.state, crate::model::JobState::Failed);
    assert_eq!(
        record.error_code.as_deref(),
        Some(crate::ErrorCode::CaptureTimeout.as_str()),
        "the store recovered, and the capture it could not retire was retired"
    );

    // And the engine let go of it. The leak was a whole CaptureJobSpec per capture, forever.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while runtime
        .engine("camera-a")
        .unwrap()
        .is_active_for_test(&capture)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "a retired capture must not still be held by the engine"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    runtime.shutdown().await;
}

/// G2: a cron fires ONE synchronised group across several cameras.
///
/// Before this, grouping existed only on the command path: a synchronised multi-camera
/// capture could not be scheduled at all. The assertions that matter are that the members
/// are exactly the schedule's cameras -- one capture each, no duplicates, none missing --
/// and that it arrives as a single durable group rather than N unrelated captures that
/// happened to fire at once.
#[tokio::test]
async fn a_group_schedule_fires_one_synchronised_group_across_its_cameras() {
    let directory = TempDir::new().unwrap();
    let cameras = ["camera-a", "camera-b", "camera-c"];
    let mut configuration = config(directory.path(), &cameras, false);
    configuration.global.capture_group_schedules = vec![group_schedule(
        "line-a-sync",
        &cameras,
        crate::config::OverlapPolicy::Skip,
    )];
    let runtime = runtime(configuration, &directory).await;
    for camera in cameras {
        runtime
            .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
            .unwrap();
        wait_for_online(&runtime, camera).await;
    }
    runtime.start_schedulers().unwrap();

    let group_id = wait_for_scheduled_group(&runtime, "line-a-sync").await;
    let terminal =
        wait_for_group_terminal_within(&runtime, &group_id, Duration::from_secs(20)).await;

    let mut members: Vec<_> = terminal
        .members
        .iter()
        .map(|member| member.instance.clone())
        .collect();
    members.sort();
    assert_eq!(
        members,
        vec!["camera-a", "camera-b", "camera-c"],
        "a scheduled group captures exactly its configured cameras, once each"
    );
    assert!(
        terminal
            .members
            .iter()
            .all(|member| member.state == crate::model::JobState::Succeeded),
        "every member of a scheduled group must succeed: {:?}",
        terminal
            .members
            .iter()
            .map(|member| member.state)
            .collect::<Vec<_>>()
    );
}

/// A group schedule holds an occurrence back while its previous group is still running.
///
/// The end-to-end form of the overlap rule: with a cron firing every second and captures that
/// take much longer than that, the schedule must produce ONE group and then wait, rather than
/// piling a new synchronised capture onto cameras that are still working through the last one.
#[tokio::test]
async fn a_group_schedule_does_not_stack_groups_on_cameras_still_working() {
    let directory = TempDir::new().unwrap();
    let cameras = ["camera-a", "camera-b", "camera-c"];
    let mut configuration = config(directory.path(), &cameras, false);
    configuration.global.capture_group_schedules = vec![group_schedule(
        "line-a-sync",
        &cameras,
        crate::config::OverlapPolicy::Skip,
    )];
    // One at a time, 700 ms each: the group needs ~2.1 s, and the cron comes round every 1 s.
    configuration.global.limits.max_concurrent_captures = 1;
    configuration.global.limits.max_in_flight_bytes =
        configuration.global.limits.max_frame_bytes_per_camera;
    for camera in &mut configuration.instances {
        if let crate::config::BackendConfig::Sim(sim) = &mut camera.backend {
            sim.capture_delay_ms = 700;
        }
    }
    let runtime = runtime(configuration, &directory).await;
    for camera in cameras {
        runtime
            .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
            .unwrap();
        wait_for_online(&runtime, camera).await;
    }
    runtime.start_schedulers().unwrap();

    let first = wait_for_scheduled_group(&runtime, "line-a-sync").await;

    // While that group works, the cron comes round repeatedly. Every one of those occurrences
    // must be skipped, and the cursor must still point at the SAME group.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let cursor = runtime
        .catalog
        .group_schedule_cursor("line-a-sync")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cursor.last_group_id.as_deref(),
        Some(first.as_str()),
        "an occurrence that fires while the previous group is still running is skipped"
    );

    let terminal =
        wait_for_group_terminal_within(&runtime, &first, Duration::from_secs(20)).await;
    assert_eq!(terminal.members.len(), 3);
    assert!(
        terminal
            .members
            .iter()
            .all(|member| member.state == crate::model::JobState::Succeeded)
    );
}

/// G2: the same occurrence submitted twice is the same group, not two.
///
/// This is what makes a scheduled group exactly-once across a crash. The request id is
/// DERIVED from the schedule and the intended fire time, so a component that dies between
/// submitting an occurrence and recording that it did re-submits it on restart and is handed
/// back the group it already accepted. Give the occurrence a random request id instead --
/// the obvious thing to write -- and every restart inside a cron period duplicates the
/// group, and the cameras are captured twice.
#[tokio::test]
async fn resubmitting_an_occurrence_returns_the_group_it_already_accepted() {
    let directory = TempDir::new().unwrap();
    let cameras = ["camera-a", "camera-b"];
    let mut configuration = config(directory.path(), &cameras, false);
    configuration.global.capture_group_schedules = vec![group_schedule(
        "line-a-sync",
        &cameras,
        crate::config::OverlapPolicy::Skip,
    )];
    let runtime = runtime(configuration, &directory).await;
    for camera in cameras {
        runtime
            .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
            .unwrap();
        wait_for_online(&runtime, camera).await;
    }

    let fire_time = chrono::DateTime::from_timestamp_millis(1_752_000_000_000).unwrap();
    let occurrence = ScheduleOccurrence {
        scope: crate::scheduler::ScheduleScope::Group("line-a-sync".to_string()),
        schedule_id: "line-a-sync".to_string(),
        intended_fire_time: fire_time,
        admit_at: fire_time,
        jitter: Duration::ZERO,
    };

    let first = runtime.submit_scheduled_group(&occurrence).await.unwrap();
    let second = runtime.submit_scheduled_group(&occurrence).await.unwrap();
    assert_eq!(
        first, second,
        "one occurrence is one group -- a re-submission must not create a second"
    );

    let group = runtime.catalog.group(first).await.unwrap().unwrap();
    assert_eq!(group.members.len(), 2);
}

/// G2: `overlapPolicy` is evaluated against the GROUP, not against individual members.
///
/// A group is outstanding until every camera in it is terminal, so the next occurrence is
/// skipped while any member is still running. Evaluating this per member would let a
/// schedule fire again on the cameras that happened to finish first, tearing a synchronised
/// capture into two half-groups.
#[tokio::test]
async fn group_overlap_is_evaluated_against_the_whole_group() {
    let directory = TempDir::new().unwrap();
    let cameras = ["camera-a", "camera-b", "camera-c"];
    let mut configuration = config(directory.path(), &cameras, false);
    // One capture at a time and 800 ms each: the group has ~2.4 s of work to get through, so
    // the assertion below cannot race it even on a badly loaded machine.
    configuration.global.limits.max_concurrent_captures = 1;
    configuration.global.limits.max_in_flight_bytes =
        configuration.global.limits.max_frame_bytes_per_camera;
    for camera in &mut configuration.instances {
        if let crate::config::BackendConfig::Sim(sim) = &mut camera.backend {
            sim.capture_delay_ms = 800;
        }
    }
    let runtime = runtime(configuration, &directory).await;
    for camera in cameras {
        runtime
            .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
            .unwrap();
        wait_for_online(&runtime, camera).await;
    }

    assert!(
        !runtime.group_schedule_overlaps(None).await,
        "a schedule that has never fired does not overlap"
    );
    assert!(
        !runtime
            .group_schedule_overlaps(Some("grp_never_existed"))
            .await,
        "a group that retention has already pruned is long terminal, not an overlap"
    );

    let group = runtime
        .submit_group(
            GroupCaptureRequest {
                request_id: "overlap-probe".to_string(),
                instances: cameras.iter().map(|c| (*c).to_string()).collect(),
                capture_profile: None,
                profile_overrides: BTreeMap::new(),
                timeout_ms: None,
                metadata: serde_json::Map::new(),
            },
            "overlap-correlation".to_string(),
            crate::admission::CapturePriority::Scheduled,
            None,
        )
        .await
        .unwrap();

    // Three members, one at a time, 800 ms each: the group cannot be terminal yet.
    assert!(
        runtime.group_schedule_overlaps(Some(&group.group_id)).await,
        "a group with members still running IS an overlap -- the next occurrence must skip"
    );

    let terminal =
        wait_for_group_terminal_within(&runtime, &group.group_id, Duration::from_secs(20))
            .await;
    assert!(terminal.state.is_terminal());
    assert!(
        !runtime.group_schedule_overlaps(Some(&group.group_id)).await,
        "once every member is terminal the schedule is free to fire again"
    );
}

#[tokio::test]
async fn an_oversized_group_is_sequenced_rather_than_partially_failed() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(
        directory.path(),
        &["camera-a", "camera-b", "camera-c", "camera-d"],
        false,
    );
    // One capture at a time, fleet-wide. Four members. Before Q1 this was three dead members.
    configuration.global.limits.max_concurrent_captures = 1;
    configuration.global.limits.max_in_flight_bytes =
        configuration.global.limits.max_frame_bytes_per_camera;
    // One capture at a time, 1.5 s each, and a capture clock of 2.8 s.
    //
    // The teeth are STRUCTURAL, not a matter of margin: member three cannot start until
    // members one and two have finished, so it always starts at or after 2 x 1.5 s = 3 s --
    // on any machine, however slow -- and 3 s is past a 2.8 s clock that began at ACCEPTANCE.
    // A slower machine only pushes it later, so the defect this test exists to see can never
    // hide behind a fast one.
    //
    // The slack, by contrast, is what a machine can eat: a rebased clock gives each member
    // 2.8 s to do 1.5 s of work, so it takes 1.3 s of scheduling overhead per capture to fail
    // this test honestly. Earlier cuts of this test allowed 150 ms and then 400 ms, and a
    // two-core CI runner under coverage instrumentation ate both.
    configuration.global.timeouts.capture_ms = 2_800;
    for camera in &mut configuration.instances {
        if let crate::config::BackendConfig::Sim(sim) = &mut camera.backend {
            sim.capture_delay_ms = 1_500;
        }
    }
    let runtime = runtime(configuration, &directory).await;
    for camera in ["camera-a", "camera-b", "camera-c", "camera-d"] {
        runtime
            .start_supervisor(camera.to_string(), runtime.engine(camera).unwrap())
            .unwrap();
        wait_for_online(&runtime, camera).await;
    }

    let group = runtime
        .submit_group(
            GroupCaptureRequest {
                request_id: "oversized-group".to_string(),
                instances: vec![
                    "camera-a".to_string(),
                    "camera-b".to_string(),
                    "camera-c".to_string(),
                    "camera-d".to_string(),
                ],
                capture_profile: None,
                profile_overrides: BTreeMap::new(),
                timeout_ms: None,
                metadata: serde_json::Map::new(),
            },
            "oversized-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .expect("acceptance is all-or-nothing and must succeed");
    assert_eq!(group.members.len(), 4);

    let terminal =
        wait_for_group_terminal_within(&runtime, &group.group_id, Duration::from_secs(30))
            .await;
    let states: Vec<_> = terminal.members.iter().map(|member| member.state).collect();
    assert!(
        states
            .iter()
            .all(|state| *state == crate::model::JobState::Succeeded),
        "every member of an oversized group must SUCCEED -- sequencing means it takes                  longer, not that most of it fails. Got: {states:?}"
    );
    assert_eq!(
        terminal.state,
        crate::model::JobState::Succeeded,
        "and the collated group result must be a success, not a partial failure"
    );

    runtime.shutdown().await;
}

/// B5, carried forward: a slot must come back when its capture leaves the queue, however it
/// leaves.
///
/// The original defect was a hand-decremented counter that the `?` operator skipped on the
/// failure path: a refused capture was destroyed AND kept its slot, and four of those made a
/// camera answer QUEUE_FULL for the rest of the process's life. The fleet queue cannot repeat
/// it by construction -- the slot is an RAII guard carried INSIDE the queued payload, so it is
/// released exactly when the descriptor leaves, whether a camera takes it, it is cancelled, or
/// it expires waiting. There is no longer a line for a `?` to skip.
///
/// So this pins the PROPERTY rather than the old implementation, because the property is what
/// mattered: a component whose slots leak stops accepting work and never says why.
#[tokio::test]
async fn a_cancelled_capture_returns_its_slot_to_the_fleet_queue() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.limits.max_queued_captures_per_camera = 1;
    configuration.global.limits.max_pending_captures = 1;
    let runtime = runtime(configuration, &directory).await;

    // No supervisor: the capture waits in the fleet queue exactly as it would for an offline
    // camera, and it is now the component's entire backlog budget.
    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "will-be-cancelled".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "cancel-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("the first capture must be accepted");
    let capture_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected a newly accepted capture, got {other:?}"),
    };
    assert_eq!(runtime.scheduler.pending(), 1);
    assert_eq!(
        runtime
            .submit_capture(
                "camera-a".to_string(),
                "refused-while-full".to_string(),
                None,
                None,
                serde_json::Map::new(),
                "refused-correlation".to_string(),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .expect_err("the queue is full, so this must be refused")
            .code(),
        crate::ErrorCode::QueueFull,
        "and refused BEFORE anything durable is written -- an operator must see QUEUE_FULL,                  not a capture that was accepted and then immediately died"
    );

    runtime
        .engine("camera-a")
        .unwrap()
        .cancel_active(&capture_id, "operator cancellation")
        .await
        .expect("cancelling a queued capture must succeed");

    // The queue evicts a cancelled entry through its own watcher task, so give it a moment to
    // act on what the cancellation token already says.
    for _ in 0..100 {
        if runtime.scheduler.pending() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        runtime.scheduler.pending(),
        0,
        "a cancelled capture must give its slot back, or the component quietly stops                  accepting work and never says why"
    );

    runtime
        .submit_capture(
            "camera-a".to_string(),
            "after-the-cancellation".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "recovery-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("the returned slot must be usable");
}

// The grace is an upper bound, not a new way to hang: a backend that will not finish its
// teardown must still be abandoned inside the budget the configuration already grants.
#[tokio::test]
async fn a_supervisor_never_waits_for_an_actor_past_the_configured_grace() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.timeouts.shutdown_grace_ms = 0;
    configuration.global.timeouts.reload_drain_timeout_ms = 0;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let finished = runtime
        .cameras
        .read()
        .unwrap()
        .get("camera-a")
        .and_then(|slot| slot.supervisor.as_ref())
        .map(|supervisor| supervisor.finished.clone())
        .expect("a live supervisor must publish its completion token");
    runtime.cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), finished.cancelled())
        .await
        .expect("a zero grace must abort the actor rather than wait for its teardown");
    runtime.shutdown().await;
}

#[tokio::test]
async fn hung_actor_is_aborted_once_the_shutdown_grace_expires() {
    let mut actor_task: JoinHandle<Result<()>> =
        tokio::spawn(async { std::future::pending::<Result<()>>().await });
    let started = tokio::time::Instant::now();
    assert!(
        !join_actor_within_grace(&mut actor_task, Duration::from_millis(50)).await,
        "a backend that ignores cancellation must not hold the shutdown grace open"
    );
    assert!(actor_task.is_finished());
    assert!(started.elapsed() < Duration::from_secs(5));
}
