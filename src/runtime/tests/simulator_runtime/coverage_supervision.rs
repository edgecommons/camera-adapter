//! Supervision-plane paths the suite had never reached.

use super::*;

/// Builds a runtime that owns a real component-level event facade, so the catalog-health observer
/// and the storage-pressure and messaging alarms have somewhere to publish.
///
/// The shared harness deliberately leaves `component_events` empty -- every test that used it
/// asserted on durable state rather than on what an operator is told. The health/alarm planes are
/// exactly the part where "what the operator is told" IS the contract, so they need the facade
/// wired in. The announcer is handed in too, because a broker that is down is a thing these tests
/// have to be able to simulate.
#[cfg(all(feature = "standalone", feature = "onvif"))]
async fn runtime_with_component_events(
    config: AdapterConfig,
    directory: &TempDir,
    component_events: EventsFacade,
    storage_pressure: Option<StoragePressureMonitor>,
    readiness: RuntimeReadiness,
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
            )
            .with_acceptance_hook(Arc::clone(&waiters) as Arc<dyn AcceptanceHook>),
        );
    }
    let scheduler = crate::dispatch::CaptureScheduler::new(&config.global.limits).unwrap();
    let runtime = Arc::new(CameraRuntime {
        config: RwLock::new(Arc::new(config)),
        backend_context: BackendRuntimeContext::new(None, &crate::config::LimitsConfig::default()),
        catalog,
        admission,
        storage,
        registry,
        cameras: Arc::new(RwLock::new(new_slots(engines, BTreeMap::new()))),
        component_events: Some(component_events),
        storage_pressure,
        storage_alarm: Arc::new(Mutex::new(StorageAlarmState::default())),
        messaging_alarm: Arc::new(Mutex::new(MessagingAlarmState::default())),
        readiness,
        metrics: Arc::new(crate::observability::CaptureMetrics::new(Arc::new(
            RecordingMetrics::default(),
        ))),
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
    runtime
        .start_capture_scheduler()
        .expect("the capture scheduler must start");
    runtime
}

/// Commits one durable terminal for `camera-b`, with the terminal clock under the test's control.
///
/// Retention is a function of how old a terminal is, so a record an assertion can rely on has to be
/// *stamped* old rather than waited for.
async fn stage_terminal_message(
    runtime: &CameraRuntime,
    configuration: &AdapterConfig,
    capture_id: &str,
    terminal_at_ms: i64,
) {
    runtime
        .catalog
        .accept_job(queued_job(configuration, capture_id))
        .await
        .unwrap();
    runtime
        .catalog
        .queue_job(capture_id, terminal_at_ms)
        .await
        .unwrap();
    let outcome = runtime
        .catalog
        .commit_terminal(
            capture_id,
            crate::catalog::TerminalWrite {
                state: crate::model::JobState::Failed,
                result: json!({
                    "schemaVersion": 1,
                    "eventId": format!("{capture_id}-terminal"),
                    "captureId": capture_id,
                    "cameraId": "camera-b",
                    "correlationId": "correlation",
                    "state": "FAILED",
                }),
                error_code: Some("PROCESS_INTERRUPTED".to_string()),
                error_message: Some("staged by the supervision coverage suite".to_string()),
                terminal_at_ms,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, crate::catalog::TerminalOutcome::Won(_)),
        "the staged terminal must be durably committed"
    );
}

/// Waits for one alarm event on `suffix` whose `active` flag matches, and returns its body.
async fn wait_for_alarm(
    publishes: &RecordedMqttPublishes,
    suffix: &str,
    active: bool,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let found = publishes
            .lock()
            .unwrap()
            .iter()
            .filter(|(topic, _)| topic.ends_with(suffix))
            .filter_map(|(_, bytes)| Message::from_slice(bytes).ok())
            .find(|message| message.body["active"] == serde_json::Value::Bool(active));
        if let Some(message) = found {
            return message.body;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no {suffix} alarm with active={active} was published"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Polls `condition` until it holds, and fails with `message` if it never does.
async fn wait_until(message: &str, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if condition() {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{message}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Reads a camera's supervisor tokens: the one that retires it, and the one it raises on its way out.
fn supervision_tokens(
    runtime: &CameraRuntime,
    instance: &str,
) -> (CancellationToken, CancellationToken) {
    let cameras = runtime.cameras.read().unwrap();
    let supervision = cameras
        .get(instance)
        .expect("the camera must be configured")
        .supervisor
        .as_ref()
        .expect("the camera must have a running supervisor generation");
    (
        supervision.cancellation.clone(),
        supervision.finished.clone(),
    )
}

/// Awaits a supervisor generation's completion signal rather than sleeping for it.
async fn await_supervisor_exit(finished: &CancellationToken) {
    tokio::time::timeout(Duration::from_secs(10), finished.cancelled())
        .await
        .expect("the supervisor generation must finish");
}

/// A broker that is down degrades messaging, and the component says so ONCE -- while it keeps
/// capturing, keeps persisting, and keeps succeeding.
///
/// This is the trade the durable outbox used to hide. The announcement is volatile now: a publish
/// that fails is logged, counted, and dropped. What must NOT happen is the component treating that
/// as a reason to stop: the capture is on disk and SUCCEEDED in the catalog, and `sb/capture-status`
/// answers for it. The alarm is what tells the operator announcements are being lost, and it clears
/// as soon as one gets through.
#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn a_broker_that_is_down_degrades_messaging_and_never_stops_the_captures() {
    let directory = TempDir::new().unwrap();
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let configuration = config(directory.path(), &["camera-a"], false);
    let announcer = Arc::new(RecordingAnnouncer::failing());
    let runtime = runtime_with_component_events(
        configuration,
        &directory,
        core.events(),
        None,
        RuntimeReadiness::noop(),
        Arc::clone(&announcer),
    )
    .await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    // Three captures, every announcement failing.
    for request in ["degraded-1", "degraded-2", "degraded-3"] {
        let accepted = runtime
            .submit_capture(
                "camera-a".to_string(),
                request.to_string(),
                None,
                None,
                serde_json::Map::new(),
                format!("{request}-correlation"),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .expect("a broker that is down must never reject a capture");
        let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
            panic!("each capture must be newly accepted while messaging is degraded");
        };
        let terminal = wait_for_terminal(&runtime, &record.capture_id).await;
        assert_eq!(
            terminal.state,
            crate::model::JobState::Succeeded,
            "a capture whose announcement failed is still a capture that SUCCEEDED"
        );
        assert!(
            terminal.terminal_result.is_some(),
            "the durable terminal body must be retained, since it is now the only record"
        );
    }
    assert_eq!(
        announcer.announcements().len(),
        3,
        "each terminal is announced exactly once -- attempted, failed, and never retried"
    );

    let raised = wait_for_alarm(&publishes, "/evt/warning/message-publish-degraded", true).await;
    assert_eq!(raised["alarm"], json!(true), "a raise must be stateful");
    assert_eq!(raised["severity"], json!("warning"));
    assert_eq!(raised["type"], json!("message-publish-degraded"));
    assert_eq!(raised["context"]["instance"], json!("camera-a"));
    let raises = publishes
        .lock()
        .unwrap()
        .iter()
        .filter(|(topic, _)| topic.ends_with("/evt/warning/message-publish-degraded"))
        .count();
    assert_eq!(
        raises, 1,
        "three lost announcements are ONE degradation, not three alarms"
    );

    // The broker comes back.
    announcer.set_failing(false);
    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "recovered".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "recovered-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let crate::catalog::AcceptJobOutcome::Inserted(record) = accepted else {
        panic!("the capture after recovery must be newly accepted");
    };
    let terminal = wait_for_terminal(&runtime, &record.capture_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    let cleared = wait_for_alarm(&publishes, "/evt/warning/message-publish-degraded", false).await;
    assert_eq!(cleared["alarm"], json!(true));
    assert_eq!(cleared["type"], json!("message-publish-degraded"));
    runtime.shutdown().await;
}

/// A catalog that reports its durable state lost closes readiness, and the health observer probes it
/// until a write commits again -- which is the only thing that reopens readiness.
///
/// Readiness is the component's answer to "may I be sent work". A durable store that failed and then
/// recovered must not leave the component parked as unready forever, and a component that only
/// *reads* successfully has proven nothing: the probe is a committed write on purpose.
#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn a_catalog_that_lost_its_durable_state_is_probed_until_it_commits_and_readiness_follows() {
    let directory = TempDir::new().unwrap();
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    // Every readiness transition is recorded rather than sampled: the observer probes a lost catalog
    // once a second, so the unready window can close before any poll of a boolean could see it. The
    // sequence of transitions is the contract anyway -- readiness closed, then readiness reopened.
    let transitions: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(Vec::new()));
    let readiness = {
        let transitions = Arc::clone(&transitions);
        RuntimeReadiness::new(Arc::new(move |value| {
            transitions.lock().unwrap().push(value);
        }))
    };
    let runtime = runtime_with_component_events(
        configuration.clone(),
        &directory,
        core.events(),
        None,
        readiness.clone(),
        Arc::new(RecordingAnnouncer::default()),
    )
    .await;
    runtime
        .start_catalog_health()
        .expect("the catalog health observer must start");
    readiness.complete_startup();
    assert_eq!(
        transitions.lock().unwrap().as_slice(),
        [true],
        "a started component with a healthy catalog must become ready"
    );

    // A raw SQLite failure, provoked through the public acceptance path: the same capture id under a
    // second, unseen ledger key collides with the jobs primary key. That is a durable-store failure
    // rather than a semantic rejection, and it is what the catalog reports availability on.
    runtime
        .catalog
        .accept_job(queued_job(&configuration, "cap-durable-loss"))
        .await
        .unwrap();
    let mut collision = queued_job(&configuration, "cap-durable-loss");
    collision.ledger_key = Some(
        crate::catalog::LedgerKey::new("camera-b", "sb/capture", "a-second-request").unwrap(),
    );
    let error = runtime.catalog.accept_job(collision).await.unwrap_err();
    assert!(
        matches!(error, crate::CameraError::Sqlite(_)),
        "the collision must surface as a durable-store failure, got {error:?}"
    );

    wait_until(
        "a lost catalog must close readiness, and the observer must probe it until a write commits \
         and readiness reopens",
        || transitions.lock().unwrap().len() >= 3,
    )
    .await;
    assert_eq!(
        transitions.lock().unwrap().as_slice(),
        [true, false, true],
        "readiness must close on the durable-state loss and reopen only after a committed probe"
    );
    runtime.shutdown().await;
}

/// A storage root that can no longer admit work raises exactly one critical alarm, and clears it when
/// the root recovers.
///
/// The alarm carries the root and its free space because the operator's next action depends on
/// *which* filesystem filled up. Re-raising it on every one-second assessment would be a pager storm,
/// so the deduplication is part of the contract rather than an optimization.
#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn a_storage_root_that_cannot_admit_work_raises_one_alarm_and_clears_it_on_recovery() {
    let directory = TempDir::new().unwrap();
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let configuration = config(directory.path(), &["camera-a"], false);
    let pressured = Arc::new(AtomicBool::new(true));
    let monitor = StoragePressureMonitor::new(
        configuration.global.output.root_directory.clone(),
        directory.path().join("state"),
        &configuration.global.output,
        Arc::new(ToggleSpaceProbe {
            pressured: Arc::clone(&pressured),
        }),
    );
    let runtime = runtime_with_component_events(
        configuration,
        &directory,
        core.events(),
        Some(monitor),
        RuntimeReadiness::noop(),
        Arc::new(RecordingAnnouncer::default()),
    )
    .await;

    let snapshot = runtime.refresh_storage_pressure().await.unwrap();
    assert!(
        snapshot.rejects_new_captures(),
        "a root with no free space must reject new capture work"
    );
    let raised = wait_for_alarm(&publishes, "/evt/critical/storage-low", true).await;
    assert_eq!(raised["severity"], json!("critical"));
    assert_eq!(raised["type"], json!("storage-low"));
    assert_eq!(
        raised["context"]["freeBytes"],
        json!(0),
        "the alarm must name the free space that triggered it"
    );
    assert!(
        raised["context"]["root"]
            .as_str()
            .is_some_and(|root| !root.is_empty()),
        "the alarm must name the root that filled up"
    );

    // A second assessment of the same unchanged pressure must not re-raise.
    runtime.refresh_storage_pressure().await.unwrap();
    let raises = publishes
        .lock()
        .unwrap()
        .iter()
        .filter(|(topic, _)| topic.ends_with("/evt/critical/storage-low"))
        .count();
    assert_eq!(
        raises, 1,
        "an unchanged storage alarm must not be republished on every assessment"
    );

    pressured.store(false, Ordering::Release);
    let recovered = runtime.refresh_storage_pressure().await.unwrap();
    assert!(
        !recovered.rejects_new_captures(),
        "a recovered root must admit capture work again"
    );
    let cleared = wait_for_alarm(&publishes, "/evt/critical/storage-low", false).await;
    assert_eq!(cleared["alarm"], json!(true));
    assert_eq!(
        cleared["context"]["freeBytes"],
        json!(0),
        "the clear must carry the context of the alarm it withdraws"
    );
    runtime.shutdown().await;
}

/// Retention reclaims a terminal job once it is past its result-retention window.
///
/// Nothing holds the job back any more: it used to be retained until its own durable message had
/// been delivered and then itself reclaimed, and there are no durable messages left.
#[tokio::test]
async fn retention_reclaims_a_terminal_job_past_its_window() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;
    let long_ago = chrono::Utc::now().timestamp_millis() - 30 * 24 * 3_600_000;
    stage_terminal_message(&runtime, &configuration, "cap-retention", long_ago).await;
    assert!(runtime.catalog.job("cap-retention").await.unwrap().is_some());

    let cancellation = CancellationToken::new();
    let sweeper = Arc::clone(&runtime);
    let sweeping = tokio::spawn({
        let cancellation = cancellation.clone();
        async move {
            sweeper
                .run_retention(cancellation, Duration::from_millis(10), 100)
                .await;
        }
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if runtime
            .catalog
            .job("cap-retention")
            .await
            .unwrap()
            .is_none()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "retention never reclaimed the terminal job past its window"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), sweeping)
        .await
        .expect("a cancelled retention loop must stop")
        .unwrap();
    runtime.shutdown().await;
}

/// A reconnect that the previous run never finished is settled as succeeded, not fenced as
/// `OUTCOME_UNKNOWN`.
///
/// Restarting re-establishes every session, which is precisely what the interrupted reconnect asked
/// for -- so the work is done. Fencing it instead would answer `PREVIOUS_OUTCOME_UNKNOWN` to every
/// exact retry, forever: the row is unreclaimable by every retention statement in the catalog.
#[tokio::test]
async fn a_reconnect_interrupted_by_the_previous_run_is_settled_rather_than_fenced() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a"], false);
    let runtime = runtime(configuration, &directory).await;
    let key = crate::catalog::LedgerKey::new(
        "camera-a",
        crate::catalog::RECONNECT_VERB,
        "reconnect-interrupted",
    )
    .unwrap();
    let canonical = json!({ "requestId": "reconnect-interrupted", "instance": "camera-a" });
    let request_hash = crate::idempotency::canonical_request_hash(&canonical, false).unwrap();
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
    assert!(
        matches!(
            replay,
            crate::catalog::BeginCommandOutcome::Existing(ref record)
                if record.state == crate::catalog::LedgerState::Succeeded
        ),
        "an interrupted reconnect must be settled by the restart that satisfied it, got {replay:?}"
    );
    runtime.shutdown().await;
}

/// A disabled camera gets no supervisor generation -- neither from the fleet start-up, nor from a
/// supervisor started at it directly. No connection attempt, no session, and the registry keeps
/// saying DISABLED.
///
/// A supervisor that connected a disabled camera anyway would hold a real device open -- and
/// disabling a camera is how an operator takes a device out of service.
#[tokio::test]
async fn a_disabled_camera_never_gets_a_session() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    configuration.instances[0].enabled = false;
    let runtime = runtime(configuration, &directory).await;

    // The fleet start-up must pass the disabled camera over entirely.
    runtime.start_supervisors().unwrap();
    assert!(
        runtime
            .cameras
            .read()
            .unwrap()
            .get("camera-a")
            .unwrap()
            .supervisor
            .is_none(),
        "starting the fleet must not give a disabled camera a supervisor generation"
    );
    wait_for_online(&runtime, "camera-b").await;

    // And a supervisor aimed at it directly -- as a reload does -- must retire at once.
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    let (_cancellation, finished) = supervision_tokens(&runtime, "camera-a");
    await_supervisor_exit(&finished).await;

    let snapshot = runtime.registry.snapshot("camera-a").unwrap();
    assert_eq!(
        snapshot.state,
        CameraConnectionState::Disabled,
        "a disabled camera must not be driven towards a connection"
    );
    assert_eq!(
        snapshot.generation, 0,
        "a supervisor that never ran must not have burned a generation"
    );
    assert!(
        runtime.actor("camera-a").is_err(),
        "a disabled camera must have no live session"
    );
    runtime.shutdown().await;
}

/// A supervisor cannot be started for a camera the component does not have, and says so instead of
/// quietly spawning a generation nothing owns.
#[tokio::test]
async fn a_supervisor_cannot_be_started_for_a_camera_that_is_not_configured() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    let error = runtime
        .start_supervisor("camera-ghost".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap_err();

    assert_eq!(
        error.code(),
        crate::ErrorCode::UnknownInstance,
        "an unconfigured camera must be rejected by name"
    );
    runtime.shutdown().await;
}

/// The health observer refuses to start without the event facade its alarms publish through, rather
/// than watching a durable store whose failures nobody could ever be told about.
#[tokio::test]
async fn the_health_observer_refuses_to_start_without_the_event_facade_its_alarms_need() {
    let directory = TempDir::new().unwrap();
    // The shared harness runtime is built without a component event facade.
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    let error = runtime.start_catalog_health().unwrap_err();

    assert!(
        matches!(&error, crate::CameraError::Catalog(message) if message.contains("events facade")),
        "the observer must refuse to run without somewhere to raise its alarms, got {error:?}"
    );
    runtime.shutdown().await;
}

/// A camera whose actor cannot be constructed is held in BACKOFF and retried -- never left reported
/// as ONLINE with nothing behind it.
///
/// The session connects before the actor is built, so this is the one window where a camera can be
/// physically connected and still have no way to be given work. Publishing ONLINE there would hand
/// the fleet queue a camera that can never take a capture.
#[tokio::test]
async fn a_camera_whose_actor_cannot_be_built_is_held_in_backoff_and_retried() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.timeouts.reconnect_backoff_min_ms = 10;
    configuration.global.timeouts.reconnect_backoff_max_ms = 20;
    let runtime = runtime(configuration, &directory).await;
    // Taken away only after the admission/queue machinery has been built from a valid limit: this is
    // a fault injected into actor construction, not an invalid component configuration.
    {
        let mut broken = (*runtime.config_snapshot().unwrap()).clone();
        broken.global.limits.max_queued_captures_per_camera = 0;
        *runtime.config.write().unwrap() = Arc::new(broken);
    }

    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();

    wait_until(
        "a camera whose actor cannot be built must be retried on a fresh generation",
        || {
            runtime.registry.snapshot("camera-a").is_ok_and(|snapshot| {
                snapshot.state == CameraConnectionState::Backoff && snapshot.generation >= 2
            })
        },
    )
    .await;
    let snapshot = runtime.registry.snapshot("camera-a").unwrap();
    assert_eq!(
        snapshot
            .last_error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("INVALID_REQUEST"),
        "the camera must carry the reason it cannot be served"
    );
    assert!(
        snapshot.connected_at.is_none(),
        "a camera with no actor must never be reported as a live session"
    );
    assert!(
        runtime.actor("camera-a").is_err(),
        "no actor handle may be published for a camera whose actor failed to build"
    );
    runtime.shutdown().await;
}

/// An announcer that panics while a capture is being finished takes down that camera's session only
/// -- the supervisor marks it BACKOFF and reconnects it on a new generation.
///
/// Panic isolation is the whole reason the actor owns the session. A panic that escaped would take
/// the process, and every other camera in the fleet, with it.
#[tokio::test]
async fn a_panic_while_finishing_a_capture_is_isolated_to_its_own_camera() {
    struct PanickingAnnouncer;

    #[async_trait::async_trait]
    impl crate::jobs::TerminalAnnouncer for PanickingAnnouncer {
        async fn announce(&self, _message: &crate::messages::TerminalMessage) -> Result<()> {
            panic!("the terminal announcer panicked inside the camera actor");
        }
    }

    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.timeouts.reconnect_backoff_min_ms = 10;
    configuration.global.timeouts.reconnect_backoff_max_ms = 20;
    let runtime = runtime(configuration, &directory).await;
    // The actor's engine is the one that panics; acceptance and the fleet queue keep the sound one,
    // so the panic happens exactly where the isolation boundary is supposed to be.
    let panicking = JobEngine::new(
        runtime.catalog.clone(),
        runtime.admission.clone(),
        runtime.storage.clone(),
        Arc::new(PanickingAnnouncer),
        Arc::clone(&runtime.waiters) as Arc<dyn JobHooks>,
    );
    runtime
        .start_supervisor("camera-a".to_string(), panicking)
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let generation = runtime.registry.snapshot("camera-a").unwrap().generation;

    runtime
        .submit_capture(
            "camera-a".to_string(),
            "panicking-capture".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "panicking-capture-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();

    wait_until(
        "the panicked session must be torn down and reconnected on a new generation",
        || {
            runtime.registry.snapshot("camera-a").is_ok_and(|snapshot| {
                snapshot.state == CameraConnectionState::Online
                    && snapshot.generation > generation
            })
        },
    )
    .await;
    assert!(
        runtime.actor("camera-a").is_ok(),
        "the reconnected camera must have a live actor again"
    );
    runtime.shutdown().await;
}

/// A capture whose camera is retired while it waits its turn is dropped, not spun on forever.
///
/// The fleet queue outlives a camera's session on purpose, but not the camera itself. A reload that
/// removes a camera leaves nothing to put the capture back for, and requeueing it would turn the
/// single fleet consumer into a hot loop that starves every other camera.
#[tokio::test]
async fn a_capture_whose_camera_is_retired_while_it_waits_its_turn_is_dropped() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    // One execution permit for the whole component, so the second capture provably waits in the
    // queue rather than being handed straight to the camera.
    configuration.global.limits.max_concurrent_captures = 1;
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 750;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    for request in ["running", "waiting"] {
        runtime
            .submit_capture(
                "camera-a".to_string(),
                request.to_string(),
                None,
                None,
                serde_json::Map::new(),
                format!("{request}-correlation"),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .unwrap();
    }
    // Retire the camera while the second capture is still holding in the queue. Its session keeps
    // running -- this is exactly the window a reload opens.
    runtime.cameras.write().unwrap().remove("camera-a");

    wait_for_queue_depth(&runtime, 0).await;
    let waiting = runtime
        .catalog
        .jobs_page(Some("camera-a".to_string()), Vec::new(), None, 10)
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.state == crate::model::JobState::Queued)
        .expect("the waiting capture must still be durable after its camera was retired");
    assert!(
        !waiting.state.is_terminal(),
        "a dropped capture is left to its deadline, not fabricated a terminal outcome"
    );
    runtime.shutdown().await;
}

/// A supervisor that can no longer read the component's configuration retires, instead of spinning
/// against a lock it will never get an answer from.
///
/// A poisoned configuration lock is unrecoverable for a camera actor -- there is nothing it can do
/// about it -- so the one thing that must not happen is a supervisor looping on it forever, burning a
/// core and a generation per pass.
#[tokio::test]
async fn a_supervisor_that_can_no_longer_read_its_configuration_retires_instead_of_spinning() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.timeouts.reconnect_backoff_min_ms = 10;
    configuration.global.timeouts.reconnect_backoff_max_ms = 20;
    let runtime = runtime(configuration, &directory).await;
    // A supervisor that is actively cycling: its actor can never be built, so it is always about to
    // re-read the configuration.
    {
        let mut broken = (*runtime.config_snapshot().unwrap()).clone();
        broken.global.limits.max_queued_captures_per_camera = 0;
        *runtime.config.write().unwrap() = Arc::new(broken);
    }
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    let (_cancellation, finished) = supervision_tokens(&runtime, "camera-a");
    wait_until("the supervisor must be cycling through its backoff", || {
        runtime
            .registry
            .snapshot("camera-a")
            .is_ok_and(|snapshot| snapshot.state == CameraConnectionState::Backoff)
    })
    .await;

    let poisoner = Arc::clone(&runtime);
    let _ = std::thread::spawn(move || {
        let _guard = poisoner.config.write().unwrap();
        panic!("the configuration lock is poisoned on purpose");
    })
    .join();
    assert!(
        runtime.config_snapshot().is_err(),
        "the configuration must now be unreadable"
    );

    await_supervisor_exit(&finished).await;
    runtime.shutdown().await;
}

/// An actor that cannot complete its shutdown teardown inside the grace budget is abandoned, so a
/// backend that will not let go cannot hold the process open.
///
/// The budget is the smaller of the shutdown grace and the reload drain timeout, and it exists for
/// exactly one reason: a native backend that hangs must cost a warning, not the shutdown.
#[tokio::test]
async fn an_actor_that_overruns_its_teardown_budget_is_abandoned_rather_than_waited_on() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    // No budget at all: whatever the actor still has to do, it is already out of time.
    configuration.global.timeouts.shutdown_grace_ms = 0;
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 2_000;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    runtime
        .submit_capture(
            "camera-a".to_string(),
            "teardown-overrun".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "teardown-overrun-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let (cancellation, finished) = supervision_tokens(&runtime, "camera-a");

    cancellation.cancel();

    // The supervisor must come back on the budget, not on the camera.
    tokio::time::timeout(Duration::from_millis(1_500), finished.cancelled())
        .await
        .expect("a supervisor must not wait past its teardown budget for a camera that will not let go");
    assert!(
        runtime.actor("camera-a").is_err(),
        "the abandoned session must not be left published as usable"
    );
    runtime.shutdown().await;
}

/// A capture accepted for a backend the camera no longer runs is retired, not resumed.
///
/// The durable profile is a contract with a specific device. A camera that has been reconfigured onto
/// another backend is not that device any more, and replaying an hours-old capture against it would
/// return an image of something else entirely.
#[tokio::test]
async fn a_capture_accepted_for_a_backend_the_camera_no_longer_runs_is_not_resumed() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;
    let mut job = queued_job(&configuration, "cap-backend-changed");
    job.intended_output = json!({ "relativePath": "camera-b/cap.jpg", "backend": "onvif-rtsp" });
    runtime.catalog.accept_job(job).await.unwrap();
    runtime
        .catalog
        .queue_job(
            "cap-backend-changed",
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        .unwrap();

    runtime.recover_install_owned().await.unwrap();

    let record = runtime
        .catalog
        .job("cap-backend-changed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.state,
        crate::model::JobState::Interrupted,
        "a capture whose camera changed backend must be retired rather than replayed"
    );
    assert_eq!(
        runtime.scheduler.pending(),
        0,
        "and it must not be put back on the fleet queue"
    );
    runtime.shutdown().await;
}

/// A supervisor started into an already-cancelled component publishes STOPPING and stops, instead of
/// opening a session the shutdown would immediately have to tear down.
#[tokio::test]
async fn a_supervisor_started_during_shutdown_publishes_stopping_and_never_connects() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    runtime.cancellation.cancel();

    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    let (_cancellation, finished) = supervision_tokens(&runtime, "camera-a");
    await_supervisor_exit(&finished).await;

    let snapshot = runtime.registry.snapshot("camera-a").unwrap();
    assert_eq!(
        snapshot.state,
        CameraConnectionState::Stopping,
        "a supervisor that finds the component cancelled must report STOPPING"
    );
    assert!(
        snapshot.connected_at.is_none(),
        "a supervisor that stops before connecting must never have been online"
    );
    assert!(
        runtime.actor("camera-a").is_err(),
        "no session may be opened during shutdown"
    );
    runtime.shutdown().await;
}

/// A camera retired while its connection is still being established never becomes ONLINE: the
/// backend's cancelled connect is treated as a failed attempt, and the next pass of the loop sees the
/// cancellation and stops.
///
/// This is the reload/shutdown race in miniature. A supervisor that ignored the cancellation here
/// would publish ONLINE for a generation that has already been superseded, and hand the fleet queue a
/// camera nobody owns.
#[tokio::test]
async fn a_camera_cancelled_while_connecting_stops_instead_of_coming_online() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    // Wide enough to cancel inside, small enough that the test never waits it out.
    sim.connect_delay_ms = 750;
    configuration.global.timeouts.reconnect_backoff_min_ms = 10;
    configuration.global.timeouts.reconnect_backoff_max_ms = 20;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    let (cancellation, finished) = supervision_tokens(&runtime, "camera-a");

    // Cancel only once the supervisor is provably inside the connect it is about to lose.
    wait_until("the supervisor must reach CONNECTING", || {
        runtime
            .registry
            .snapshot("camera-a")
            .is_ok_and(|snapshot| snapshot.state == CameraConnectionState::Connecting)
    })
    .await;
    cancellation.cancel();
    await_supervisor_exit(&finished).await;

    let snapshot = runtime.registry.snapshot("camera-a").unwrap();
    assert_eq!(
        snapshot.state,
        CameraConnectionState::Stopping,
        "a camera cancelled mid-connect must end STOPPING"
    );
    assert!(
        snapshot.connected_at.is_none(),
        "a cancelled connection attempt must never be reported as a live session"
    );
    assert!(
        runtime.actor("camera-a").is_err(),
        "a cancelled connection attempt must leave no actor behind"
    );
    runtime.shutdown().await;
}

/// The queue metric reports what the component is holding right now, including how much of the
/// configured fleet is actually connected.
///
/// The capture counters say what has happened; this says what is happening. A fleet with a healthy
/// success rate and a camera that never connects is failing, and the counters alone cannot show it.
#[tokio::test]
async fn the_queue_metric_reports_the_live_fleet_and_not_just_the_configured_one() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(
        config(directory.path(), &["camera-a", "camera-b"], false),
        &directory,
    )
    .await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let values = runtime.sample_queue_metric().await.unwrap();

    assert_eq!(
        values.get("camerasConfigured"),
        Some(&2.0),
        "every configured camera must be counted, connected or not"
    );
    assert_eq!(
        values.get("camerasOnline"),
        Some(&1.0),
        "only the camera with a live session is online"
    );
    assert_eq!(values.get("dispatchQueued"), Some(&0.0));
    assert_eq!(values.get("durableBacklog"), Some(&0.0));
    assert!(
        values
            .get("availableAcquisitions")
            .is_some_and(|value| *value > 0.0),
        "an idle component must report the acquisition capacity it still has"
    );
    runtime.shutdown().await;
}

/// The metric sampler keeps reporting on its own timer: every interval it re-samples what the
/// component is holding and the health of every camera it runs.
///
/// The clock is virtual here on purpose. The sampling interval is half a minute -- a test that waited
/// it out would be the slowest in the suite and would still be asserting on a sleep.
#[tokio::test(start_paused = true)]
async fn the_metric_sampler_keeps_reporting_the_queue_and_fleet_health_on_its_interval() {
    let directory = TempDir::new().unwrap();
    let (runtime, metrics) =
        runtime_with_metrics(config(directory.path(), &["camera-a"], false), &directory).await;

    runtime.start_metric_sampler().unwrap();

    // The deadline is real time; the sleep is virtual and auto-advances the sampler's interval.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let sampled = metrics.counts(crate::observability::QUEUE_METRIC, "camerasConfigured");
        if sampled >= 1.0 && !metrics.health_for("camera-a").is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the metric sampler never reported the queue and the fleet's health"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    runtime.shutdown().await;
}

/// Southbound health is sampled for the cameras the component actually runs, and for no others.
///
/// A disabled camera has no session to be stale about; emitting a health sample for it would put a
/// permanently unhealthy camera in front of the operator that no action can fix.
#[tokio::test]
async fn southbound_health_is_sampled_only_for_the_cameras_the_component_runs() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    configuration.instances[1].enabled = false;
    let (runtime, metrics) = runtime_with_metrics(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    runtime.sample_southbound_health(true).await;

    let online = metrics.health_for("camera-a");
    assert!(
        !online.is_empty(),
        "an enabled camera must be sampled into southbound health"
    );
    assert_eq!(
        online
            .last()
            .and_then(|emission| emission.values.get("connectionState")),
        Some(&1.0),
        "a connected camera must report its live connection state"
    );
    assert!(
        metrics.health_for("camera-b").is_empty(),
        "a disabled camera must not be sampled into southbound health"
    );
    runtime.shutdown().await;
}
