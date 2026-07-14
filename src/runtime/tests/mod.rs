use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;

use super::*;
use crate::jobs::CaptureDispatcher;

struct CountingService(AtomicUsize);

#[async_trait]
impl CameraCommandService for CountingService {
    async fn handle_camera_command(
        &self,
        verb: &'static str,
        _request: Message,
        _deferred: DeferredReplyRegistry,
    ) -> CommandOutcome {
        self.0.fetch_add(1, Ordering::AcqRel);
        CommandOutcome::ImmediateSuccess(Some(json!({ "verb": verb })))
    }
}

// Full inbox activation is covered by the core contract suite. This focused test locks the
// router's startup/shutdown hand-off without needing a broker-backed Message fixture.
#[test]
fn install_is_single_assignment_and_shutdown_is_latched() {
    let router = RuntimeCommandRouter::new();
    router
        .install(Arc::new(CountingService(AtomicUsize::new(0))))
        .unwrap();
    assert!(
        router
            .install(Arc::new(CountingService(AtomicUsize::new(0))))
            .is_err()
    );
    router.begin_shutdown();
    assert!(
        router
            .install(Arc::new(CountingService(AtomicUsize::new(0))))
            .is_err()
    );
}

#[test]
fn command_errors_keep_the_adapter_catalog_code_and_bound_the_detail() {
    let error = crate::CameraError::rejected(
        crate::ErrorCode::QueueFull,
        format!("queue unavailable\n{}", "x".repeat(512)),
    );
    let command = command_error(&error);
    assert_eq!(command.code, "QUEUE_FULL");
    assert!(!command.message.contains('\n'));
    assert!(command.message.len() <= 256);
}

#[test]
fn retained_snapshot_cursor_is_opaque_query_bound_and_stable() {
    let cursors = CursorStore::default();
    let query = json!({ "includeCapabilities": true, "includeUnconfigured": false });
    let values = vec![json!({ "instance": "a" }), json!({ "instance": "b" })];
    let (first, cursor, _) = cursors
        .snapshot_page("list", &query, None, Some(values), None, 1)
        .unwrap();
    assert_eq!(first, vec![json!({ "instance": "a" })]);
    let cursor = cursor.expect("a second page must retain a cursor");
    assert!(cursor.starts_with("cur_"));

    let (second, final_cursor, _) = cursors
        .snapshot_page("list", &query, Some(&cursor), None, None, 1)
        .unwrap();
    assert_eq!(second, vec![json!({ "instance": "b" })]);
    assert!(final_cursor.is_none());

    let changed_query = json!({ "includeCapabilities": false, "includeUnconfigured": false });
    assert_eq!(
        cursors
            .snapshot_page("list", &changed_query, Some(&cursor), None, None, 1)
            .unwrap_err()
            .code(),
        crate::ErrorCode::InvalidRequest
    );
}

#[test]
fn list_cursor_retains_configured_and_unconfigured_pages_together() {
    let cursors = CursorStore::default();
    let query = json!({ "includeCapabilities": false, "includeUnconfigured": true });
    let (cameras, unconfigured, next) = cursors
        .list_page(
            &query,
            None,
            Some((
                vec![json!({ "instance": "camera-a" }), json!({ "instance": "camera-b" })],
                vec![json!({ "backend": "onvif-rtsp", "selector": { "endpointReference": "urn:camera:new" } })],
            )),
            2,
        )
        .unwrap();
    assert_eq!(cameras.len(), 2);
    assert!(unconfigured.is_empty());
    let next = next.expect("unconfigured observation must remain in the same snapshot");

    let (cameras, unconfigured, final_cursor) =
        cursors.list_page(&query, Some(&next), None, 2).unwrap();
    assert!(cameras.is_empty());
    assert_eq!(unconfigured.len(), 1);
    assert!(final_cursor.is_none());
    assert!(
        cursors
            .list_page(
                &json!({ "includeCapabilities": false, "includeUnconfigured": false }),
                Some(&next),
                None,
                2,
            )
            .is_err()
    );
}

#[test]
fn retained_discovery_hides_only_matching_stable_configured_selectors() {
    let camera = crate::config::CameraConfig {
        id: "camera-a".to_string(),
        enabled: true,
        resource_group: None,
        backend: crate::config::BackendConfig::GenicamAravis(
            crate::config::GenicamBackendConfig {
                selector: crate::config::GenicamSelector {
                    serial: Some("SN-42".to_string()),
                    mac: None,
                    device_id: None,
                    ip: None,
                },
                transport: crate::config::GenicamTransport::Auto,
                interface: None,
                packet_size: None,
                packet_delay_ns: None,
                buffer_count: None,
                feature_overrides: BTreeMap::new(),
            },
        ),
        default_capture_profile: "main".to_string(),
        capture_profiles: BTreeMap::new(),
        schedules: Vec::new(),
        ptz: crate::config::PtzConfig::default(),
    };
    let matching = DiscoveryCandidate {
        backend: crate::model::BackendKind::GenicamAravis,
        selector: json!({ "serial": "SN-42" }),
        vendor: None,
        model: None,
        capabilities: json!({}),
    };
    let other = DiscoveryCandidate {
        selector: json!({ "serial": "SN-43" }),
        ..matching.clone()
    };
    assert!(candidate_is_configured(
        &matching,
        std::slice::from_ref(&camera)
    ));
    assert!(!candidate_is_configured(&other, &[camera]));
}

#[test]
fn retained_cursor_expiry_and_kind_confusion_fail_closed() {
    let cursors = CursorStore::default();
    let query = json!({ "instance": "camera-a" });
    let (_, cursor, _) = cursors
        .snapshot_page(
            "ptz-presets",
            &query,
            None,
            Some(vec![json!({ "token": "one" }), json!({ "token": "two" })]),
            None,
            1,
        )
        .unwrap();
    let cursor = cursor.expect("a second page must retain a cursor");
    assert!(
        cursors
            .snapshot_page("list", &query, Some(&cursor), None, None, 1)
            .is_err()
    );
    {
        let mut entries = cursors.lock().unwrap();
        let entry = entries.entries.get_mut(&cursor).unwrap();
        entry.expires_at = std::time::Instant::now() - Duration::from_secs(1);
    }
    assert!(
        cursors
            .snapshot_page("ptz-presets", &query, Some(&cursor), None, None, 1)
            .is_err()
    );
}

#[test]
fn job_cursor_binds_filters_and_preserves_typed_tuple() {
    let cursors = CursorStore::default();
    let query = json!({ "instance": "camera-a", "states": ["FAILED"] });
    let cursor = cursors
        .next_job_cursor(&query, (1234, "cap_1".to_string()))
        .unwrap();
    assert_eq!(
        cursors.job_before(&query, Some(&cursor)).unwrap(),
        Some((1234, "cap_1".to_string()))
    );
    assert!(
        cursors
            .job_before(
                &json!({ "instance": "camera-b", "states": ["FAILED"] }),
                Some(&cursor)
            )
            .is_err()
    );
}

#[test]
fn delayed_outbox_alarm_transitions_once_and_keeps_context_bounded() {
    let mut state = OutboxAlarmState::default();
    let delayed = OutboxPressure {
        pending: 101,
        oldest_age_ms: 61_000,
        max_attempts: 4,
        delayed: true,
        escalated: false,
        last_error: Some("transport confirmation failed or remained ambiguous".to_string()),
    };
    let Some(OutboxAlarmTransition::Raise(context)) = state.transition(&delayed) else {
        panic!("a delayed outbox must raise exactly one alarm");
    };
    assert_eq!(context["pending"], 101);
    assert_eq!(context["oldestAgeMs"], 61_000);
    assert_eq!(context["maxAttempts"], 4);
    assert_eq!(
        context["lastError"],
        "transport confirmation failed or remained ambiguous"
    );
    assert!(state.transition(&delayed).is_none());

    let cleared = OutboxPressure::default();
    let Some(OutboxAlarmTransition::Clear(context)) = state.transition(&cleared) else {
        panic!("a recovered outbox must clear its stateful alarm");
    };
    assert_eq!(
        context,
        json!({
            "pending": 0,
            "oldestAgeMs": 0,
            "maxAttempts": 0,
        })
    );
    assert!(state.transition(&cleared).is_none());
}

#[test]
fn storage_low_alarm_is_deduplicated_and_clears_only_after_every_root_recovers() {
    let healthy = StoragePressureSnapshot {
        output: RootPressure {
            root: PathBuf::from("/configured/output"),
            free_bytes: Some(900),
            free_percent: Some(90),
            pressured: false,
            readable: true,
        },
        state: RootPressure {
            root: PathBuf::from("/configured/state"),
            free_bytes: Some(900),
            free_percent: Some(90),
            pressured: false,
            readable: true,
        },
    };
    let mut alarms = StorageAlarmState::default();
    assert!(alarms.transition(&healthy).is_none());

    let pressured_output = StoragePressureSnapshot {
        output: RootPressure {
            root: PathBuf::from("/configured/output"),
            free_bytes: Some(99),
            free_percent: Some(9),
            pressured: true,
            readable: true,
        },
        state: RootPressure {
            root: PathBuf::from("/configured/state"),
            free_bytes: Some(900),
            free_percent: Some(90),
            pressured: false,
            readable: true,
        },
    };
    let Some(StorageAlarmTransition::Raise(context)) = alarms.transition(&pressured_output)
    else {
        panic!("a configured output floor violation must raise storage-low");
    };
    assert_eq!(context.root, "/configured/output");
    assert_eq!(context.free_bytes, Some(99));
    assert_eq!(context.free_percent, Some(9));
    assert!(alarms.transition(&pressured_output).is_none());

    let pressured_state = StoragePressureSnapshot {
        output: pressured_output.output.clone(),
        state: RootPressure {
            root: PathBuf::from("/configured/state"),
            free_bytes: Some(50),
            free_percent: Some(5),
            pressured: true,
            readable: true,
        },
    };
    let Some(StorageAlarmTransition::Raise(context)) = alarms.transition(&pressured_state)
    else {
        panic!("a new pressured root must replace the active storage-low context");
    };
    assert_eq!(context.root, "/configured/state");
    assert_eq!(context.free_bytes, Some(50));

    let recovered = StoragePressureSnapshot {
        output: RootPressure {
            free_bytes: Some(900),
            free_percent: Some(90),
            pressured: false,
            ..pressured_output.output.clone()
        },
        state: RootPressure {
            free_bytes: Some(900),
            free_percent: Some(90),
            pressured: false,
            ..pressured_state.state
        },
    };
    let Some(StorageAlarmTransition::Clear(context)) = alarms.transition(&recovered) else {
        panic!("the stateful storage-low condition must clear after both roots recover");
    };
    assert_eq!(context.root, "/configured/state");
    assert!(alarms.transition(&recovered).is_none());
}

#[test]
fn capture_lifecycle_labels_cover_every_supported_trigger_and_mode() {
    assert_eq!(
        capture_trigger_type(&crate::messages::CaptureTrigger::Command {
            request_id: "request".to_string(),
        }),
        "command"
    );
    assert_eq!(
        capture_trigger_type(&crate::messages::CaptureTrigger::GroupCommand {
            request_id: "request".to_string(),
            capture_group_id: "group".to_string(),
        }),
        "group-command"
    );
    assert_eq!(
        capture_trigger_type(&crate::messages::CaptureTrigger::Schedule {
            schedule_id: "nightly".to_string(),
            intended_fire_time: chrono::Utc::now(),
        }),
        "schedule"
    );
    assert_eq!(
        capture_mode_type(crate::model::CaptureMode::Simulated),
        "simulated"
    );
    assert_eq!(
        capture_mode_type(crate::model::CaptureMode::SoftwareTrigger),
        "software-trigger"
    );
    assert_eq!(
        capture_mode_type(crate::model::CaptureMode::SnapshotUri),
        "snapshot-uri"
    );
    assert_eq!(
        capture_mode_type(crate::model::CaptureMode::RtspFrame),
        "rtsp-frame"
    );
}

#[test]
fn outbox_recovery_cannot_make_readiness_true_before_startup_or_after_shutdown() {
    let updates = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&updates);
    let readiness = RuntimeReadiness::new(Arc::new(move |ready| {
        recorded.lock().unwrap().push(ready);
    }));

    readiness.set_outbox_available(false);
    readiness.set_outbox_available(true);
    assert_eq!(*updates.lock().unwrap(), vec![false, false]);

    readiness.complete_startup();
    readiness.begin_shutdown();
    readiness.set_outbox_available(false);
    readiness.set_outbox_available(true);
    assert_eq!(
        *updates.lock().unwrap(),
        vec![false, false, true, false, false, false]
    );
}

#[test]
fn catalog_and_outbox_durability_are_independent_readiness_gates() {
    let updates = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&updates);
    let readiness = RuntimeReadiness::new(Arc::new(move |ready| {
        recorded.lock().unwrap().push(ready);
    }));

    readiness.complete_startup();
    readiness.set_catalog_available(false);
    readiness.set_outbox_available(false);
    readiness.set_catalog_available(true);
    readiness.set_outbox_available(true);

    assert_eq!(
        *updates.lock().unwrap(),
        vec![true, false, false, false, true]
    );
}

#[test]
fn readiness_publication_is_linearizable_across_concurrent_availability_changes() {
    use std::sync::mpsc;

    let updates = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&updates);
    let (true_entered_tx, true_entered_rx) = mpsc::channel();
    let (release_true_tx, release_true_rx) = mpsc::channel();
    let release_true_rx = Arc::new(Mutex::new(release_true_rx));
    let (false_published_tx, false_published_rx) = mpsc::channel();
    let block_next_true = Arc::new(AtomicBool::new(false));
    let observe_false = Arc::new(AtomicBool::new(false));
    let readiness = RuntimeReadiness::new(Arc::new({
        let block_next_true = Arc::clone(&block_next_true);
        let observe_false = Arc::clone(&observe_false);
        move |ready| {
            if ready && block_next_true.swap(false, Ordering::AcqRel) {
                true_entered_tx
                    .send(())
                    .expect("the test must await the blocked ready publication");
                release_true_rx
                    .lock()
                    .unwrap()
                    .recv()
                    .expect("the test must release the blocked ready publication");
            }
            if !ready && observe_false.load(Ordering::Acquire) {
                let _ = false_published_tx.send(());
            }
            recorded.lock().unwrap().push(ready);
        }
    }));

    readiness.set_catalog_available(false);
    readiness.complete_startup();
    updates.lock().unwrap().clear();
    block_next_true.store(true, Ordering::Release);
    observe_false.store(true, Ordering::Release);

    let recovering = readiness.clone();
    let recovering_thread = std::thread::spawn(move || {
        recovering.set_catalog_available(true);
    });
    true_entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("ready publication did not enter its controlled delay");

    let becoming_unavailable = readiness.clone();
    let unavailable_thread = std::thread::spawn(move || {
        becoming_unavailable.set_outbox_available(false);
    });
    assert!(
        false_published_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "a newer unavailable transition must not publish before the older ready callback completes"
    );

    release_true_tx
        .send(())
        .expect("the ready publication must still be waiting for release");
    recovering_thread
        .join()
        .expect("the recovering readiness transition must finish");
    unavailable_thread
        .join()
        .expect("the unavailable readiness transition must finish");
    false_published_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("the unavailable transition must publish after the ready callback");
    assert_eq!(*updates.lock().unwrap(), vec![true, false]);
}

fn reload_config(root_directory: &str) -> serde_json::Value {
    json!({
        "component": {
            "global": { "output": { "rootDirectory": root_directory } },
            "instances": [{
                "id": "camera-a",
                "backend": { "type": "sim" },
                "defaultCaptureProfile": "main",
                "captureProfiles": { "main": { "output": { "encoding": "jpeg" } } }
            }]
        }
    })
}

#[test]
fn reload_validator_rejects_restart_only_output_root_without_touching_runtime() {
    let current = reload_config("C:/captures-a");
    let candidate = reload_config("C:/captures-b");
    let result = validate_configuration_candidate(
        candidate,
        Some(current),
        ConfigurationValidationPhase::Reload,
    )
    .unwrap();
    assert!(matches!(
        result,
        ConfigurationValidationResult::Reject { code, .. } if code == "CAMERA_CONFIG_INVALID"
    ));
}

#[test]
fn reload_validator_accepts_schedule_only_generation() {
    let current = reload_config("C:/captures-a");
    let mut candidate = current.clone();
    candidate["component"]["instances"][0]["schedules"] = json!([{
        "id": "minute",
        "cron": "0 * * * * *",
        "timezone": "UTC",
        "captureProfile": "main"
    }]);
    assert_eq!(
        validate_configuration_candidate(
            candidate,
            Some(current),
            ConfigurationValidationPhase::Reload,
        )
        .unwrap(),
        ConfigurationValidationResult::Accept
    );
}

#[test]
fn reload_validator_vetoes_onvif_secret_when_startup_has_no_credential_service() {
    let current = reload_config("C:/captures-a");
    let mut candidate = current.clone();
    candidate["component"]["instances"][0]["backend"] = json!({
        "type": "onvif-rtsp",
        "deviceServiceUrl": "https://camera.example.test/onvif/device_service",
        "credentials": { "$secret": "camera/login" },
        "mediaProfile": "primary"
    });

    let result = validate_configuration_candidate_with_credentials(
        candidate,
        Some(current),
        ConfigurationValidationPhase::Reload,
        false,
    )
    .unwrap();
    assert!(matches!(
        result,
        ConfigurationValidationResult::Reject { code, .. } if code == "CAMERA_CONFIG_INVALID"
    ));
}

#[tokio::test]
async fn runtime_config_listener_rejects_when_its_runtime_is_gone_before_factory_use() {
    let listener = RuntimeConfigListener::new(
        Weak::new(),
        Arc::new(
            |_instance, _config| -> edgecommons::Result<Arc<AppFacade>> {
                unreachable!("unavailable runtimes must not construct application facades")
            },
        ),
        Arc::new(|_instance, _config| -> edgecommons::Result<EventsFacade> {
            unreachable!("unavailable runtimes must not construct event facades")
        }),
    );
    let candidate = Arc::new(
        Config::from_value(COMPONENT_NAME, "gw-01", reload_config("C:/captures-a"))
            .expect("the core fixture must be structurally valid"),
    );
    let error = match listener.prepare_configuration_apply(candidate).await {
        Ok(_) => panic!("a dropped runtime must veto the candidate before preparation"),
        Err(error) => error,
    };
    assert_eq!(error.code, "CONFIG_APPLICATION_UNAVAILABLE");
}

#[cfg(test)]
mod simulator_runtime;

