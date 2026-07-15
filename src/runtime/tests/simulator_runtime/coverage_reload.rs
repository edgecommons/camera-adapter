//! Reload- and schedule-plane paths the suite had never reached.

use super::*;

/// A schedule that comes due every second, so a test never has to wait on a wall-clock boundary.
///
/// The fixture's `0 * * * * *` fires on the minute, which makes any test that waits for it either
/// a one-minute test or a coin flip on the time of day. Every second is the same code path.
const EVERY_SECOND: &str = "* * * * * *";
/// A cron that will not fire during a test: midnight on the first of January.
const EFFECTIVELY_NEVER: &str = "0 0 0 1 1 *";

/// Retunes the fixture's camera schedule to fire every second.
fn every_second_schedule(configuration: &mut AdapterConfig, instance: &str) {
    let camera = configuration
        .instances
        .iter_mut()
        .find(|camera| camera.id == instance)
        .expect("the fixture roster must contain the camera being retuned");
    camera.schedules[0].cron = EVERY_SECOND.to_string();
}

/// Copies `camera-a`'s fixture schedule onto another camera, so one test can watch two schedules.
fn copy_schedule_to(configuration: &mut AdapterConfig, instance: &str) {
    let schedule = configuration.instances[0].schedules[0].clone();
    configuration
        .instances
        .iter_mut()
        .find(|camera| camera.id == instance)
        .expect("the fixture roster must contain the camera being given a schedule")
        .schedules
        .push(schedule);
}

/// A group schedule over some of the fixture roster.
fn group_schedule(
    id: &str,
    cron: &str,
    instances: &[&str],
) -> crate::config::CaptureGroupScheduleConfig {
    crate::config::CaptureGroupScheduleConfig {
        id: id.to_string(),
        enabled: true,
        cron: cron.to_string(),
        timezone: "UTC".to_string(),
        instances: instances.iter().map(|id| (*id).to_string()).collect(),
        capture_profile: Some("main".to_string()),
        profile_overrides: BTreeMap::new(),
        misfire_policy: crate::config::MisfirePolicy::Skip,
        overlap_policy: crate::config::OverlapPolicy::Skip,
        jitter_seconds: 0,
        timeout_ms: None,
    }
}

/// One occurrence, as the schedule loop would build it after a cron match.
fn occurrence(scope: crate::scheduler::ScheduleScope, schedule_id: &str) -> ScheduleOccurrence {
    let now = chrono::Utc::now();
    ScheduleOccurrence {
        scope,
        schedule_id: schedule_id.to_string(),
        intended_fire_time: now,
        admit_at: now,
        jitter: Duration::ZERO,
    }
}

/// A durable job that a schedule already owns: the exact shape `has_schedule_overlap` looks for.
fn scheduled_job(
    configuration: &AdapterConfig,
    instance: &str,
    capture_id: &str,
    schedule_id: &str,
) -> crate::catalog::NewJob {
    let camera = configuration
        .instances
        .iter()
        .find(|camera| camera.id == instance)
        .unwrap();
    let profile = crate::jobs::JobProfileSnapshot {
        name: "main".to_string(),
        capture: camera.capture_profiles.get("main").unwrap().clone(),
        offline_policy: crate::config::OfflinePolicy::WaitUntilDeadline,
        maximum_frame_bytes: configuration.global.limits.max_frame_bytes_per_camera,
        capture_mode: crate::model::CaptureMode::Simulated,
        capture_interlock: camera.ptz.capture_interlock,
        settle_ms: camera.ptz.settle_ms,
    };
    let now = chrono::Utc::now().timestamp_millis();
    let trigger = crate::messages::CaptureTrigger::Schedule {
        schedule_id: schedule_id.to_string(),
        intended_fire_time: chrono::Utc::now(),
    };
    let canonical = json!({ "scheduleId": schedule_id });
    crate::catalog::NewJob {
        capture_id: capture_id.to_string(),
        instance: instance.to_string(),
        ledger_key: None,
        request_hash: crate::idempotency::canonical_request_hash(&canonical, false).unwrap(),
        canonical_request: canonical,
        effective_profile: serde_json::to_value(profile).unwrap(),
        deadlines: crate::catalog::JobDeadlines {
            terminal_at_ms: now + 600_000,
            queue_at_ms: None,
            capture_at_ms: now + 600_000,
            encode_at_ms: now + 600_000,
            persist_at_ms: now + 600_000,
        },
        trigger: serde_json::to_value(trigger).unwrap(),
        origin_correlation_id: None,
        intended_output: json!({ "relativePath": "camera-a/cap.jpg", "backend": "sim" }),
        accepted_at_ms: now,
        group_id: None,
    }
}

/// Every durable job the catalog holds for one camera, in any state.
async fn jobs_for(runtime: &CameraRuntime, instance: &str) -> Vec<crate::catalog::JobRecord> {
    runtime
        .catalog
        .jobs_page(Some(instance.to_string()), Vec::new(), None, 100)
        .await
        .unwrap()
}

/// Polls until a camera owns at least `expected` durable jobs, rather than sleeping for a guess.
async fn wait_for_job_count(
    runtime: &CameraRuntime,
    instance: &str,
    expected: usize,
) -> Vec<crate::catalog::JobRecord> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let jobs = jobs_for(runtime, instance).await;
        if jobs.len() >= expected {
            return jobs;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "'{instance}' still has {} durable jobs; expected at least {expected}",
            jobs.len()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Polls one job's durable state until the capture is physically under way.
async fn wait_for_in_flight(runtime: &CameraRuntime, capture_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let record = runtime.catalog.job(capture_id).await.unwrap();
        if record.is_some_and(|record| {
            matches!(
                record.state,
                crate::model::JobState::Acquiring
                    | crate::model::JobState::Encoding
                    | crate::model::JobState::Persisting
            )
        }) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the simulator capture never reached an active stage, so nothing was in flight"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A reload is one generation change, not three: a camera added, one removed, and one restarted in
/// the same replacement must all land together, and no camera may be left half-present -- in the
/// registry but not the slot map, or the other way round. That asymmetry is the failure this plane
/// exists to prevent, and the suite had only ever exercised the three cases one at a time.
#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn a_reload_that_adds_removes_and_restarts_cameras_in_one_step_leaves_no_camera_half_present()
{
    let directory = TempDir::new().unwrap();
    let (port, _) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let initial = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(initial, &directory).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }
    let generation_before = runtime.registry.snapshot("camera-a").unwrap().generation;

    // camera-b leaves, camera-c arrives, and camera-a's backend changes -- one replacement.
    let mut replacement = config(directory.path(), &["camera-a", "camera-c"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.seed = Some(4_242);
    let added = core.instance("camera-c").unwrap();
    let apps = BTreeMap::from([("camera-c".to_string(), Arc::new(added.app()))]);
    // The listener hands over a fresh facade for every camera it prepared -- including, harmlessly,
    // one for the camera this replacement removes. A facade for a camera that no longer exists must
    // not be installed anywhere.
    let events = BTreeMap::from([
        ("camera-a".to_string(), core.instance("camera-a").unwrap().events()),
        ("camera-b".to_string(), core.instance("camera-b").unwrap().events()),
        ("camera-c".to_string(), added.events()),
    ]);

    let diff = runtime
        .apply_reloaded_config(replacement, apps, events)
        .await
        .expect("a well-formed replacement with every required facade must commit");

    assert_eq!(diff.added, vec!["camera-c".to_string()]);
    assert_eq!(diff.removed, vec!["camera-b".to_string()]);
    assert_eq!(
        diff.lifecycle_changed,
        vec!["camera-a".to_string()],
        "only the camera whose backend changed may have its generation retired"
    );
    assert_eq!(
        runtime
            .cameras
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["camera-a".to_string(), "camera-c".to_string()],
        "the slot map is the roster: the removed camera leaves and the added one arrives together"
    );
    assert_eq!(
        runtime.registry.ids().unwrap(),
        vec!["camera-a".to_string(), "camera-c".to_string()],
        "the registry must agree with the slot map -- a camera in one and not the other half-exists"
    );
    assert!(
        runtime
            .cameras
            .read()
            .unwrap()
            .get("camera-c")
            .is_some_and(|slot| slot.events.is_some()),
        "an added camera is never installed without the event path it was required to bring"
    );
    assert!(
        runtime
            .cameras
            .read()
            .unwrap()
            .get("camera-a")
            .is_some_and(|slot| slot.events.is_some()),
        "a retained camera takes the fresh facade the listener prepared for the new generation"
    );
    // Both survivors are supervised by the NEW generation: the restarted one and the added one.
    wait_for_online(&runtime, "camera-a").await;
    wait_for_online(&runtime, "camera-c").await;
    assert!(
        runtime.registry.snapshot("camera-a").unwrap().generation > generation_before,
        "a restarted camera must be supervised by a new generation"
    );
    runtime.shutdown().await;
}

/// A reload that touches nothing a camera owns must not disturb a single camera.
///
/// Session churn is not free: a restart drops the connection, retires the actor, and makes the
/// camera briefly unavailable. Doing that because an unrelated global limit changed would make
/// every configuration edit a fleet-wide outage.
#[tokio::test]
async fn a_reload_that_changes_only_global_settings_keeps_every_camera_session_alive() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let generation_before = runtime.registry.snapshot("camera-a").unwrap().generation;

    let mut replacement = config(directory.path(), &["camera-a"], false);
    replacement.global.limits.max_deferred_waiters_per_capture = 7;
    let diff = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .expect("a global-only replacement is a valid candidate");

    assert!(
        diff.added.is_empty() && diff.removed.is_empty() && diff.lifecycle_changed.is_empty(),
        "a global-only reload changes no camera's lifecycle: {diff:?}"
    );
    assert_eq!(
        runtime.registry.snapshot("camera-a").unwrap().generation,
        generation_before,
        "a global-only reload must not retire and restart a camera's supervisor"
    );
    assert_eq!(
        runtime.registry.snapshot("camera-a").unwrap().state,
        CameraConnectionState::Online,
        "the live session survives a global-only reload"
    );
    assert_eq!(
        runtime
            .config_snapshot()
            .unwrap()
            .global
            .limits
            .max_deferred_waiters_per_capture,
        7,
        "the new global generation is nevertheless published"
    );
    runtime.shutdown().await;
}

/// A camera the replacement adds arrives with its facades or it does not arrive at all.
///
/// The commit path builds every new runtime object BEFORE it touches the published registry, and a
/// missing facade is an initialization failure rather than permission to install a camera that can
/// never publish anything. Rejecting it late would leave a mixed roster behind.
#[tokio::test]
async fn a_reload_whose_added_camera_has_no_application_facade_is_rejected_without_touching_the_roster()
 {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    let replacement = config(directory.path(), &["camera-a", "camera-c"], false);
    let error = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .expect_err("an added camera without an application facade cannot be installed");

    assert!(
        error
            .to_string()
            .contains("missing application facade for reloaded camera 'camera-c'"),
        "the rejection must name the camera and the facade it lacked: {error}"
    );
    assert_eq!(
        runtime.registry.ids().unwrap(),
        vec!["camera-a".to_string()],
        "a vetoed candidate must leave the prior roster exactly as it was"
    );
    assert_eq!(
        runtime.config_snapshot().unwrap().instances.len(),
        1,
        "a vetoed candidate must not advance the runtime configuration"
    );
    assert!(
        !runtime.cameras.read().unwrap().contains_key("camera-c"),
        "the camera the candidate would have added must not exist in any map"
    );
    runtime.shutdown().await;
}

/// The events facade is required on exactly the same terms as the application facade.
///
/// Half a facade set is the interesting case: the candidate looks constructible until the second
/// lookup fails, so this pins that the check happens in the candidate-only phase -- before any
/// supervisor is retired or any published generation moves.
#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn a_reload_whose_added_camera_has_no_events_facade_is_rejected_before_the_camera_is_installed()
{
    let directory = TempDir::new().unwrap();
    let (port, _) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let generation_before = runtime.registry.snapshot("camera-a").unwrap().generation;

    let replacement = config(directory.path(), &["camera-a", "camera-c"], false);
    let apps = BTreeMap::from([(
        "camera-c".to_string(),
        Arc::new(core.instance("camera-c").unwrap().app()),
    )]);
    let error = runtime
        .apply_reloaded_config(replacement, apps, BTreeMap::new())
        .await
        .expect_err("an added camera without an events facade cannot be installed");

    assert!(
        error
            .to_string()
            .contains("missing events facade for reloaded camera 'camera-c'"),
        "the rejection must name the camera and the facade it lacked: {error}"
    );
    assert_eq!(
        runtime.registry.snapshot("camera-a").unwrap().generation,
        generation_before,
        "the candidate is rejected before a single live supervisor is retired"
    );
    assert_eq!(
        runtime.registry.snapshot("camera-a").unwrap().state,
        CameraConnectionState::Online,
        "the complete prior service keeps running after a rejected candidate"
    );
    assert!(!runtime.cameras.read().unwrap().contains_key("camera-c"));
    runtime.shutdown().await;
}

/// A reload does not yank the camera out from under a capture that is physically in progress.
///
/// The drain barrier waits for the camera's active jobs before it cancels the supervisor, so a
/// capture that has reached the acquire/encode/persist stages is allowed to finish. Without the
/// wait, a reload would routinely destroy work the component had already accepted and promised.
#[tokio::test]
async fn a_reload_waits_for_an_in_flight_capture_before_it_retires_the_camera() {
    let directory = TempDir::new().unwrap();
    let mut initial = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 800;
    let runtime = runtime(initial, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let crate::catalog::AcceptJobOutcome::Inserted(job) = runtime
        .submit_capture(
            "camera-a".to_string(),
            "reload-drain".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "reload-drain-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap()
    else {
        panic!("a fresh capture must be inserted");
    };
    wait_for_in_flight(&runtime, &job.capture_id).await;

    // A backend change puts camera-a in the restarting set, so the drain barrier applies to it.
    let mut replacement = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 800;
    sim.seed = Some(17);
    runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .expect("a generous drain budget must let the in-flight capture finish and then commit");

    let record = runtime
        .catalog
        .job(job.capture_id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.state,
        crate::model::JobState::Succeeded,
        "the reload returned while the capture was still in {:?}: an in-flight capture must be \
         allowed to complete, not destroyed by the reload",
        record.state
    );
    runtime.shutdown().await;
}

/// The drain budget is a budget, not a promise: when it runs out the candidate is VETOED.
///
/// A capture that will not finish inside `reloadDrainTimeoutMs` must not be waited on forever, and
/// it must not be silently abandoned either. The reload fails while Core and the runtime still
/// expose the same previous configuration, which is what makes a retry safe.
#[tokio::test]
async fn a_reload_whose_drain_budget_expires_while_a_capture_is_in_flight_vetoes_the_candidate() {
    let directory = TempDir::new().unwrap();
    let mut initial = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut initial.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 5_000;
    let runtime = runtime(initial, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let crate::catalog::AcceptJobOutcome::Inserted(job) = runtime
        .submit_capture(
            "camera-a".to_string(),
            "reload-drain-timeout".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "reload-drain-timeout-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap()
    else {
        panic!("a fresh capture must be inserted");
    };
    wait_for_in_flight(&runtime, &job.capture_id).await;

    let mut replacement = config(directory.path(), &["camera-a"], false);
    replacement.global.timeouts.reload_drain_timeout_ms = 0;
    let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.seed = Some(31);

    let error = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .expect_err("an exhausted drain budget must veto the candidate rather than proceed");

    assert_eq!(error.code(), crate::ErrorCode::CameraUnavailable);
    let crate::config::BackendConfig::Sim(sim) =
        &runtime.config_snapshot().unwrap().instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    assert_eq!(
        sim.seed, None,
        "a vetoed candidate must leave the runtime on Core's prior configuration generation"
    );
    runtime.shutdown().await;
}

/// Two reloads cannot drain one camera at the same time.
///
/// The fence is what keeps the retirement barrier meaningful: a second replacement arriving while
/// the first is mid-drain could retire supervisors the first is still waiting on, and the two
/// candidates would race to publish different generations. The rollback path is fenced by the same
/// flag, for the same reason.
#[tokio::test]
async fn a_second_reload_is_rejected_while_one_is_already_draining() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let checkpoint = runtime.reload_checkpoint().unwrap();

    // Exactly what an in-progress reload leaves behind while it awaits a supervisor.
    runtime.reloading.store(true, Ordering::Release);

    let mut replacement = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.seed = Some(5);
    let error = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .expect_err("a replacement must not begin while another is draining camera work");
    assert_eq!(error.code(), crate::ErrorCode::CameraUnavailable);

    let error = runtime
        .restore_reload_checkpoint(checkpoint)
        .await
        .expect_err("a rollback must not run while another reload is still draining");
    assert_eq!(error.code(), crate::ErrorCode::CameraUnavailable);

    let crate::config::BackendConfig::Sim(sim) =
        &runtime.config_snapshot().unwrap().instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    assert_eq!(
        sim.seed, None,
        "neither rejected call may have advanced the runtime configuration"
    );
    runtime.reloading.store(false, Ordering::Release);
    runtime.shutdown().await;
}

/// Rollback is idempotent: Core may call it after a commit error that never got as far as breaking
/// anything, and what it must produce either way is a WORKING prior generation -- not merely the
/// prior data. A vetoed reload has already cancelled the camera's supervisor, so a rollback that
/// only restored maps would leave the camera configured and permanently offline.
#[tokio::test]
async fn a_rollback_after_a_vetoed_reload_restores_a_working_camera_generation() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let checkpoint = runtime.reload_checkpoint().unwrap();

    let mut replacement = config(directory.path(), &["camera-a"], false);
    replacement.global.timeouts.reload_drain_timeout_ms = 0;
    let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.seed = Some(64);
    assert_eq!(
        runtime
            .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
            .await
            .expect_err("a zero drain budget vetoes the candidate")
            .code(),
        crate::ErrorCode::CameraUnavailable
    );

    runtime
        .restore_reload_checkpoint(checkpoint)
        .await
        .expect("rollback must succeed after a failure that never reached a destructive stage");

    assert_eq!(
        runtime.registry.ids().unwrap(),
        vec!["camera-a".to_string()]
    );
    let crate::config::BackendConfig::Sim(sim) =
        &runtime.config_snapshot().unwrap().instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    assert_eq!(sim.seed, None, "rollback reinstates the checkpointed config");
    // The point of the exercise: the camera is not merely configured, it is supervised again.
    wait_for_online(&runtime, "camera-a").await;
    runtime.shutdown().await;
}

/// Rollback restores the ROSTER, not just the two maps a camera happens to appear in.
///
/// A camera the failed candidate ADDED has an engine, a facade, and a live supervisor. Restoring
/// only the prior engines and events would leave its supervisor entry behind, and the camera would
/// half-exist afterwards -- gone from the registry, still holding a live generation.
#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn a_rollback_after_a_committed_candidate_restores_the_prior_roster_and_drops_the_added_camera()
{
    let directory = TempDir::new().unwrap();
    let (port, _) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    let checkpoint = runtime.reload_checkpoint().unwrap();

    let replacement = config(directory.path(), &["camera-a", "camera-c"], false);
    let instance = core.instance("camera-c").unwrap();
    runtime
        .apply_reloaded_config(
            replacement,
            BTreeMap::from([("camera-c".to_string(), Arc::new(instance.app()))]),
            BTreeMap::from([("camera-c".to_string(), instance.events())]),
        )
        .await
        .expect("the candidate commits before Core discovers it cannot publish it");
    wait_for_online(&runtime, "camera-c").await;
    let candidate_generation = runtime
        .cameras
        .read()
        .unwrap()
        .get("camera-c")
        .and_then(|slot| slot.supervisor.clone())
        .expect("the committed candidate started a supervisor for the camera it added");

    runtime
        .restore_reload_checkpoint(checkpoint)
        .await
        .expect("rollback must undo a committed candidate Core then failed to publish");

    assert!(
        candidate_generation.cancellation.is_cancelled(),
        "every candidate supervisor is retired before the prior roster is reinstated"
    );
    assert_eq!(
        runtime.registry.ids().unwrap(),
        vec!["camera-a".to_string()],
        "the added camera is deregistered by the rollback"
    );
    assert_eq!(
        runtime
            .cameras
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["camera-a".to_string()],
        "the added camera's whole slot -- engine, facade, supervisor -- leaves with it"
    );
    assert_eq!(
        runtime.config_snapshot().unwrap().instances.len(),
        1,
        "the checkpointed configuration is the one that survives"
    );
    wait_for_online(&runtime, "camera-a").await;
    runtime.shutdown().await;
}

/// Disabling a camera is not removing it, but its queued work is just as dead.
///
/// A disabled camera keeps its registry entry and its slot -- an operator can re-enable it -- yet
/// nothing it has queued can ever run again. Leaving those jobs QUEUED would strand callers waiting
/// on captures the component has silently decided never to perform.
#[tokio::test]
async fn a_reload_that_disables_a_camera_terminalizes_its_queued_work_but_keeps_it_configured() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(initial.clone(), &directory).await;
    runtime
        .catalog
        .accept_job(queued_job(&initial, "cap_disabled_camera"))
        .await
        .unwrap();
    runtime
        .catalog
        .queue_job("cap_disabled_camera", chrono::Utc::now().timestamp_millis())
        .await
        .unwrap();

    let mut replacement = config(directory.path(), &["camera-a", "camera-b"], false);
    replacement.instances[1].enabled = false;
    let diff = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .unwrap();

    assert_eq!(diff.lifecycle_changed, vec!["camera-b".to_string()]);
    assert!(
        diff.removed.is_empty(),
        "a disabled camera is still a configured camera"
    );
    let job = runtime
        .catalog
        .job("cap_disabled_camera")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        job.state,
        crate::model::JobState::Interrupted,
        "work queued for a camera that can no longer run it must be terminalized"
    );
    assert_eq!(job.error_code.as_deref(), Some("PROCESS_INTERRUPTED"));
    assert!(
        !runtime
            .registry
            .camera_config("camera-b")
            .expect("a disabled camera keeps its registry entry")
            .enabled
    );
    assert!(
        runtime.cameras.read().unwrap().contains_key("camera-b"),
        "a disabled camera keeps its slot so it can be re-enabled by a later reload"
    );
    runtime.shutdown().await;
}

/// A scheduler task exists for exactly the schedules that can fire, and a restart leaves none of
/// the previous ones running. A stale task surviving a restart would keep admitting occurrences
/// against a cron and a profile the component no longer has.
#[tokio::test]
async fn a_disabled_camera_or_schedule_starts_no_scheduler_task_and_a_restart_retires_the_previous_ones()
 {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], true);
    configuration.instances[0].schedules[0].cron = EFFECTIVELY_NEVER.to_string();
    // A disabled schedule on a live camera.
    let mut disabled_schedule = configuration.instances[0].schedules[0].clone();
    disabled_schedule.id = "disabled-schedule".to_string();
    disabled_schedule.enabled = false;
    configuration.instances[0].schedules.push(disabled_schedule);
    // An enabled schedule on a disabled camera.
    copy_schedule_to(&mut configuration, "camera-b");
    configuration.instances[1].enabled = false;
    // One group schedule that runs, and one that does not.
    configuration
        .global
        .capture_group_schedules
        .push(group_schedule("line", EFFECTIVELY_NEVER, &["camera-a"]));
    let mut disabled_group = group_schedule("disabled-line", EFFECTIVELY_NEVER, &["camera-a"]);
    disabled_group.enabled = false;
    configuration
        .global
        .capture_group_schedules
        .push(disabled_group);
    let runtime = runtime(configuration, &directory).await;

    runtime.start_schedulers().unwrap();

    let started = runtime.scheduler_cancellations.read().unwrap().clone();
    let mut keys = started.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            ("camera-a".to_string(), "minute".to_string()),
            ("group:line".to_string(), "line".to_string()),
        ],
        "only enabled schedules on enabled cameras -- and enabled group schedules -- get a task"
    );

    // Starting again over a live map displaces each generation rather than doubling it: two tasks
    // on one cron would evaluate the same occurrence twice.
    runtime.start_schedulers().unwrap();
    assert!(
        started.values().all(CancellationToken::is_cancelled),
        "a schedule that is started again must retire the generation it displaces"
    );
    assert_eq!(
        runtime.scheduler_cancellations.read().unwrap().len(),
        keys.len(),
        "one task per schedule, however many times the schedules are started"
    );

    let started = runtime.scheduler_cancellations.read().unwrap().clone();
    runtime.restart_schedulers().unwrap();

    assert!(
        started.values().all(CancellationToken::is_cancelled),
        "a restart must retire every schedule generation it replaces"
    );
    let restarted = runtime.scheduler_cancellations.read().unwrap().clone();
    let mut restarted_keys = restarted.keys().cloned().collect::<Vec<_>>();
    restarted_keys.sort();
    assert_eq!(restarted_keys, keys, "the same schedules run after a restart");
    assert!(
        restarted
            .values()
            .all(|cancellation| !cancellation.is_cancelled()),
        "the replacement generation must be live"
    );
    runtime.shutdown().await;
}

/// A schedule is just another producer of captures: when an occurrence comes due it passes the
/// camera's capture interlock, goes through the ordinary durable capture path with the deadlines
/// its profile asks for, carries a `schedule` trigger that names it, and is recorded under the
/// unjittered intended time -- the key that makes it recoverable exactly once across a restart.
#[tokio::test]
async fn a_schedule_that_comes_due_admits_a_capture_and_records_its_durable_occurrence() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], true);
    every_second_schedule(&mut configuration, "camera-a");
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    // An idle camera under the strictest interlock: the occurrence must be let through.
    sim.ptz.supported = true;
    sim.ptz.status_supported = true;
    configuration.instances[0].ptz.enabled = true;
    let profile = configuration.instances[0]
        .capture_profiles
        .get_mut("main")
        .unwrap();
    profile.capture_interlock = Some(crate::config::CaptureInterlock::Reject);
    profile.queue_expiry_ms = Some(60_000);
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    runtime.start_schedulers().unwrap();

    let jobs = wait_for_job_count(&runtime, "camera-a", 1).await;
    let job = jobs.first().unwrap().clone();
    assert_eq!(
        job.trigger.get("type"),
        Some(&json!("schedule")),
        "a scheduled capture must be attributable to the schedule that produced it: {:?}",
        job.trigger
    );
    assert_eq!(job.trigger.get("scheduleId"), Some(&json!("minute")));
    assert!(
        job.deadlines.queue_at_ms.is_some(),
        "the profile's queue-expiry budget must be applied to a scheduled capture too"
    );
    assert_eq!(
        wait_for_terminal(&runtime, &job.capture_id).await.state,
        crate::model::JobState::Succeeded,
        "a scheduled capture runs through the same engine an operator command would"
    );

    let intended = job
        .trigger
        .get("intendedFireTime")
        .and_then(serde_json::Value::as_str)
        .expect("the schedule trigger carries its intended fire time")
        .parse::<chrono::DateTime<chrono::Utc>>()
        .unwrap();
    assert_eq!(
        runtime
            .catalog
            .latest_schedule_occurrence("camera-a", "minute")
            .await
            .unwrap(),
        Some(intended.timestamp_millis()),
        "the durable occurrence key is the UNJITTERED intended fire time"
    );
    runtime.shutdown().await;
}

/// The occurrence key is what makes a schedule exactly-once. A component that crashed between
/// admitting an occurrence and recording it must not admit it a second time, so the durable key --
/// not the scheduler's memory -- is the authority.
#[tokio::test]
async fn the_same_scheduled_occurrence_is_never_admitted_twice() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], true), &directory).await;
    let occurrence = occurrence(
        crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
        "minute",
    );

    runtime.submit_scheduled(&occurrence).await.unwrap();
    runtime
        .submit_scheduled(&occurrence)
        .await
        .expect("re-submitting a known occurrence is a no-op, not an error");

    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "the durable occurrence key must admit one capture per intended fire time"
    );
    runtime.shutdown().await;
}

/// A camera disabled between the cron firing and the occurrence being admitted must not be given
/// work. The schedule loop consumes the occurrence either way -- what it must not do is write a
/// durable job for a camera the operator has taken out of service.
#[tokio::test]
async fn a_scheduled_occurrence_for_a_disabled_camera_is_rejected_before_any_job_is_written() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], true);
    configuration.instances[0].enabled = false;
    let runtime = runtime(configuration, &directory).await;

    let error = runtime
        .submit_scheduled(&occurrence(
            crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
            "minute",
        ))
        .await
        .expect_err("a disabled camera cannot be given scheduled work");

    assert_eq!(error.code(), crate::ErrorCode::CameraDisabled);
    assert!(
        jobs_for(&runtime, "camera-a").await.is_empty(),
        "a rejected occurrence must not leave a durable job behind"
    );
    runtime.shutdown().await;
}

/// A schedule names a capture profile, and the profile can be edited away underneath it. The
/// occurrence is then unrunnable and must be rejected on the profile it cannot resolve, rather
/// than silently falling back to some other profile's settings.
#[tokio::test]
async fn a_scheduled_occurrence_whose_capture_profile_is_gone_is_rejected_as_an_unknown_profile() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], true);
    configuration.instances[0].schedules[0].capture_profile = "retired-profile".to_string();
    let runtime = runtime(configuration, &directory).await;

    let error = runtime
        .submit_scheduled(&occurrence(
            crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
            "minute",
        ))
        .await
        .expect_err("an occurrence whose profile no longer exists cannot be admitted");

    assert_eq!(error.code(), crate::ErrorCode::UnknownCaptureProfile);
    assert!(jobs_for(&runtime, "camera-a").await.is_empty());
    runtime.shutdown().await;
}

/// A schedule that has been disabled admits nothing, even for an occurrence already travelling
/// through the submission path.
#[tokio::test]
async fn a_scheduled_occurrence_for_a_schedule_that_was_disabled_is_rejected() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], true);
    configuration.instances[0].schedules[0].enabled = false;
    let runtime = runtime(configuration, &directory).await;

    let error = runtime
        .submit_scheduled(&occurrence(
            crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
            "minute",
        ))
        .await
        .expect_err("a disabled schedule cannot admit an occurrence");

    assert_eq!(error.code(), crate::ErrorCode::InvalidRequest);
    assert!(jobs_for(&runtime, "camera-a").await.is_empty());
    runtime.shutdown().await;
}

/// Scheduled work is subject to the same storage floor as commanded work. A component that cannot
/// safely write a frame must not accept a capture it will only fail later -- and it must reject it
/// BEFORE the durable job exists, or the refusal becomes a failure row instead.
#[tokio::test]
async fn a_scheduled_occurrence_is_rejected_under_storage_pressure_without_writing_a_job() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a"], true);
    let monitor = StoragePressureMonitor::new(
        configuration.global.output.root_directory.clone(),
        directory.path().join("state"),
        &configuration.global.output,
        Arc::new(LowSpaceProbe),
    );
    let runtime = runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;

    let error = runtime
        .submit_scheduled(&occurrence(
            crate::scheduler::ScheduleScope::Camera("camera-a".to_string()),
            "minute",
        ))
        .await
        .expect_err("storage pressure must reject a scheduled occurrence");

    assert_eq!(error.code(), crate::ErrorCode::StoragePressure);
    assert!(
        jobs_for(&runtime, "camera-a").await.is_empty(),
        "storage pressure must reject before the durable job is written"
    );
    assert_eq!(
        runtime
            .catalog
            .latest_schedule_occurrence("camera-a", "minute")
            .await
            .unwrap(),
        None,
        "an occurrence rejected before acceptance leaves no durable occurrence row"
    );
    runtime.shutdown().await;
}

/// A group occurrence has no single camera to submit to, and the type says so. Asking for one must
/// fail loudly rather than pick a plausible member and produce a capture nobody asked for.
#[tokio::test]
async fn a_group_occurrence_cannot_be_submitted_as_a_single_camera_capture() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], true), &directory).await;

    let error = runtime
        .submit_scheduled(&occurrence(
            crate::scheduler::ScheduleScope::Group("line".to_string()),
            "line",
        ))
        .await
        .expect_err("a group occurrence is not a camera occurrence");

    assert!(
        error
            .to_string()
            .contains("group-schedule occurrence cannot be submitted as a single camera capture"),
        "the refusal must say why: {error}"
    );
    assert!(jobs_for(&runtime, "camera-a").await.is_empty());
    runtime.shutdown().await;
}

/// `overlapPolicy=skip` means the schedule does not pile a second capture on top of one that has
/// not finished. The occurrence is consumed, not queued: a camera slower than its own cron would
/// otherwise accumulate an unbounded backlog it can never work off.
///
/// camera-b carries the same schedule with nothing outstanding, and is the positive control: once
/// IT has admitted several occurrences, several cron ticks have demonstrably been evaluated, so
/// camera-a's silence is a decision rather than a race with the poll loop.
#[tokio::test]
async fn a_schedule_whose_previous_occurrence_is_still_running_skips_instead_of_piling_on() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], true);
    every_second_schedule(&mut configuration, "camera-a");
    copy_schedule_to(&mut configuration, "camera-b");
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[1].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(configuration.clone(), &directory).await;
    // The control camera is connected, so its occurrences complete and never overlap each other.
    runtime
        .start_supervisor("camera-b".to_string(), runtime.engine("camera-b").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-b").await;

    // camera-a already owns a nonterminal capture belonging to the same schedule.
    let outstanding = scheduled_job(&configuration, "camera-a", "cap_outstanding", "minute");
    runtime
        .catalog
        .accept_scheduled_job(
            outstanding,
            "minute",
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        .unwrap();
    runtime
        .catalog
        .queue_job("cap_outstanding", chrono::Utc::now().timestamp_millis())
        .await
        .unwrap();
    assert!(
        runtime
            .has_schedule_overlap("camera-a", "minute")
            .await
            .unwrap(),
        "a nonterminal job carrying this schedule's trigger IS an overlap"
    );
    assert!(
        !runtime
            .has_schedule_overlap("camera-a", "another-schedule")
            .await
            .unwrap(),
        "overlap is per-schedule: another schedule's outstanding work must not block this one"
    );

    runtime.start_schedulers().unwrap();

    // Two admissions on the unobstructed camera prove several cron ticks were evaluated.
    wait_for_job_count(&runtime, "camera-b", 2).await;
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "the overlapping schedule must have admitted nothing beyond the job already outstanding"
    );
    runtime.shutdown().await;
}

/// A schedule fires whether or not its camera is reachable: the occurrence is durably admitted and
/// queued, and the capture happens when the camera comes back. It is not dropped -- an occurrence
/// silently lost while a camera was reconnecting is a capture nobody will ever know is missing.
///
/// And exactly ONE is outstanding while it waits: `overlapPolicy=skip` means an offline camera
/// accumulates a single queued occurrence, not one per second of the outage.
#[tokio::test]
async fn a_schedule_whose_camera_is_offline_queues_one_occurrence_and_captures_it_on_reconnect() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], true);
    every_second_schedule(&mut configuration, "camera-a");
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(configuration, &directory).await;
    assert!(
        runtime.actor("camera-a").is_err(),
        "no supervisor was started, so the camera is offline"
    );

    runtime.start_schedulers().unwrap();

    let jobs = wait_for_job_count(&runtime, "camera-a", 1).await;
    let capture_id = jobs.first().unwrap().capture_id.clone();
    assert!(
        !jobs.first().unwrap().state.is_terminal(),
        "the occurrence waits for the camera rather than being thrown away"
    );
    assert_eq!(
        jobs.len(),
        1,
        "an offline camera accumulates ONE outstanding occurrence, not one per cron tick"
    );

    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    assert_eq!(
        wait_for_terminal(&runtime, &capture_id).await.state,
        crate::model::JobState::Succeeded,
        "the occurrence admitted while the camera was offline is captured once it returns"
    );
    runtime.shutdown().await;
}

/// A group schedule fires ONCE, as one thing: one durable group row for the whole occurrence,
/// submitted through the same path as `sb/capture-group`, and the group it admitted is recorded in
/// the schedule's recovery cursor so a restart can tell whether it is still outstanding.
#[tokio::test]
async fn a_group_schedule_that_comes_due_admits_one_synchronised_group_and_records_its_cursor() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    configuration.global.capture_group_schedules.push(group_schedule(
        "line",
        EVERY_SECOND,
        &["camera-a", "camera-b"],
    ));
    let runtime = runtime(configuration, &directory).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    runtime.start_schedulers().unwrap();

    let cursor = wait_for_admitted_group(&runtime).await;
    let group_id = cursor.last_group_id.expect("the cursor names its group");
    let group = wait_for_group_terminal(&runtime, &group_id).await;
    assert_eq!(
        group.members.len(),
        2,
        "one occurrence produces ONE group containing every member camera"
    );
    for member in &group.members {
        assert_eq!(
            runtime
                .catalog
                .job(member.capture_id.clone())
                .await
                .unwrap()
                .unwrap()
                .state,
            crate::model::JobState::Succeeded,
            "every member of an admitted group is captured"
        );
    }
    runtime.shutdown().await;
}

/// A group is outstanding until every camera in it is terminal, and the next occurrence is skipped
/// while it is: no second group, and not one capture. The skip is DURABLE -- it advances the
/// cursor, so a restart cannot re-admit a consumed occurrence -- while the cursor goes on naming
/// the group that is still running, so a restart can still see that it is outstanding.
#[tokio::test]
async fn a_group_schedule_skips_an_occurrence_while_its_previous_group_is_still_running() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        // Slower than its own cron: every occurrence but the first lands on a running group.
        sim.capture_delay_ms = 4_000;
    }
    configuration.global.capture_group_schedules.push(group_schedule(
        "line",
        EVERY_SECOND,
        &["camera-a", "camera-b"],
    ));
    let runtime = runtime(configuration, &directory).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    runtime.start_schedulers().unwrap();

    let admitted = wait_for_admitted_group(&runtime).await;
    let group_id = admitted.last_group_id.clone().unwrap();

    // The next occurrence is consumed with no group: it was skipped, not admitted.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let skipped = loop {
        let cursor = runtime
            .catalog
            .group_schedule_cursor("line")
            .await
            .unwrap()
            .expect("the cursor exists once the schedule has consumed an occurrence");
        if cursor.intended_fire_time_ms > admitted.intended_fire_time_ms {
            break cursor;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the group schedule consumed no further occurrence while its group was running"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        !runtime
            .catalog
            .group(group_id.clone())
            .await
            .unwrap()
            .expect("the admitted group is durable")
            .state
            .is_terminal(),
        "the occurrence was skipped precisely because the previous group had not finished"
    );
    assert_eq!(
        skipped.last_group_id,
        Some(group_id),
        "consuming a skipped occurrence must not erase the outstanding group a restart still \
         has to recognise"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "the skipped occurrence admitted no second group, and therefore no second capture"
    );
    runtime.shutdown().await;
}

/// Polls the group schedule's durable cursor until it names a group it admitted.
async fn wait_for_admitted_group(runtime: &CameraRuntime) -> crate::catalog::GroupScheduleCursor {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let cursor = runtime
            .catalog
            .group_schedule_cursor("line")
            .await
            .unwrap()
            .filter(|cursor| cursor.last_group_id.is_some());
        if let Some(cursor) = cursor {
            return cursor;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the group schedule never admitted a group"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Periodic discovery is a LOOP, not a single pass: it must come back on its interval and replace
/// what it last observed. A cache that is only ever written once turns one network snapshot into a
/// permanent claim about cameras that may since have gone.
#[tokio::test]
async fn periodic_discovery_runs_again_after_its_interval_and_replaces_what_it_last_observed() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.discovery.enabled = true;
    configuration.global.discovery.interval_seconds = 1;
    let runtime = runtime(configuration, &directory).await;

    runtime.discovery_cache.lock().unwrap().candidates.push(stale_candidate());
    runtime.start_periodic_discovery().unwrap();
    let first_generation = runtime
        .discovery_cancellation
        .read()
        .unwrap()
        .clone()
        .expect("enabled discovery retains a cancellable generation");
    // Starting again displaces the generation instead of running two probes over one interface set.
    runtime.start_periodic_discovery().unwrap();
    assert!(
        first_generation.is_cancelled(),
        "a second start must retire the discovery generation it displaces"
    );
    wait_for_discovery_pass(&runtime, "first").await;

    // Nothing restarts the generation this time: the SAME task must come back on its interval.
    runtime.discovery_cache.lock().unwrap().candidates.push(stale_candidate());
    wait_for_discovery_pass(&runtime, "second").await;

    runtime.shutdown().await;
}

/// The schedule plane is fenced by the same flag the reload plane raises: while a replacement is
/// draining camera work, a schedule must not admit an occurrence into a generation that is about to
/// be retired. The fence pauses the loop -- it does not kill it -- so the schedule resumes the
/// moment the reload is done.
#[tokio::test]
async fn a_schedule_admits_nothing_while_a_reload_is_draining_and_resumes_when_it_ends() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], true);
    every_second_schedule(&mut configuration, "camera-a");
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    // The group plane is fenced by the same flag, and is checked here alongside the camera plane.
    configuration.global.capture_group_schedules.push(group_schedule(
        "line",
        EVERY_SECOND,
        &["camera-a", "camera-b"],
    ));
    let runtime = runtime(configuration, &directory).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    // Exactly what a reload leaves behind while it drains.
    runtime.reloading.store(true, Ordering::Release);
    runtime.start_schedulers().unwrap();

    // Several cron seconds pass with the fence up.
    let fenced = tokio::time::Instant::now() + Duration::from_millis(2_500);
    while tokio::time::Instant::now() < fenced {
        assert!(
            jobs_for(&runtime, "camera-a").await.is_empty(),
            "a schedule must not admit work into a generation a reload is retiring"
        );
        assert!(
            runtime
                .catalog
                .group_schedule_cursor("line")
                .await
                .unwrap()
                .is_none(),
            "and a group schedule must not consume an occurrence while the fence is up"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    runtime.reloading.store(false, Ordering::Release);

    // The loops were paused, not killed: they admit again as soon as the fence drops.
    wait_for_job_count(&runtime, "camera-a", 1).await;
    wait_for_admitted_group(&runtime).await;
    runtime.shutdown().await;
}

/// Periodic discovery is fenced by the same flag, for the same reason: probing the network with the
/// interface policy of a configuration that is being replaced would retain observations made under
/// rules that no longer apply.
#[tokio::test]
async fn periodic_discovery_does_not_probe_while_a_reload_is_draining() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.discovery.enabled = true;
    configuration.global.discovery.interval_seconds = 1;
    let runtime = runtime(configuration, &directory).await;

    runtime.reloading.store(true, Ordering::Release);
    runtime.discovery_cache.lock().unwrap().candidates.push(stale_candidate());
    runtime.start_periodic_discovery().unwrap();

    let fenced = tokio::time::Instant::now() + Duration::from_millis(2_500);
    while tokio::time::Instant::now() < fenced {
        assert_eq!(
            runtime.discovery_cache.lock().unwrap().candidates.len(),
            1,
            "a fenced discovery generation must not run a pass or replace what it retained"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    runtime.reloading.store(false, Ordering::Release);

    // The generation was paused, not killed.
    wait_for_discovery_pass(&runtime, "resumed").await;
    runtime.shutdown().await;
}

/// Turning discovery off stops the probing, not merely the reporting. A loop that kept sweeping the
/// network after the policy forbade it would go on making observations the operator has said the
/// component may not make.
#[tokio::test]
async fn periodic_discovery_stops_probing_once_the_policy_is_turned_off() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.discovery.enabled = true;
    configuration.global.discovery.interval_seconds = 1;
    let runtime = runtime(configuration, &directory).await;
    runtime.discovery_cache.lock().unwrap().candidates.push(stale_candidate());
    runtime.start_periodic_discovery().unwrap();
    wait_for_discovery_pass(&runtime, "first").await;

    // The live configuration says discovery is over. The generation is deliberately NOT cancelled:
    // the loop itself has to notice.
    let mut disabled = config(directory.path(), &["camera-a"], false);
    disabled.global.discovery.enabled = false;
    disabled.global.discovery.interval_seconds = 1;
    *runtime.config.write().unwrap() = Arc::new(disabled);
    runtime.discovery_cache.lock().unwrap().candidates.push(stale_candidate());

    let observed = tokio::time::Instant::now() + Duration::from_millis(3_000);
    while tokio::time::Instant::now() < observed {
        assert_eq!(
            runtime.discovery_cache.lock().unwrap().candidates.len(),
            1,
            "a disabled discovery policy must stop the sweeps, several intervals over"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    runtime.shutdown().await;
}

/// An occurrence the runtime cannot admit is CONSUMED, not retried: the schedule logs it and moves
/// to the next cron occurrence. Retrying it would violate the one-occurrence guarantee, and writing
/// a durable failure row for a capture that was never accepted would invent work that never existed.
///
/// camera-b is the positive control: its admissions prove the cron ticks were evaluated.
#[tokio::test]
async fn a_schedule_occurrence_that_cannot_be_admitted_writes_no_job_and_is_not_retried() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], true);
    every_second_schedule(&mut configuration, "camera-a");
    copy_schedule_to(&mut configuration, "camera-b");
    // The profile camera-a's schedule names has been edited away; camera-b's still resolves.
    configuration.instances[0].schedules[0].capture_profile = "retired-profile".to_string();
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[1].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-b".to_string(), runtime.engine("camera-b").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-b").await;

    runtime.start_schedulers().unwrap();

    wait_for_job_count(&runtime, "camera-b", 2).await;
    assert!(
        jobs_for(&runtime, "camera-a").await.is_empty(),
        "an unadmittable occurrence must leave no durable trace at all"
    );
    assert_eq!(
        runtime
            .catalog
            .latest_schedule_occurrence("camera-a", "minute")
            .await
            .unwrap(),
        None,
        "and no occurrence row either -- nothing was accepted"
    );
    runtime.shutdown().await;
}

/// A group occurrence that cannot be admitted is consumed just as a camera occurrence is, and the
/// cursor records that it admitted NO group. Re-admitting it after a restart would fire a
/// synchronised capture the schedule had already decided against.
#[tokio::test]
async fn a_group_schedule_occurrence_that_cannot_be_admitted_records_a_consumed_occurrence_with_no_group()
 {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    // A group is all-or-nothing, and one of its members has been taken out of service.
    configuration.instances[1].enabled = false;
    configuration.global.capture_group_schedules.push(group_schedule(
        "line",
        EVERY_SECOND,
        &["camera-a", "camera-b"],
    ));
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    runtime.start_schedulers().unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let cursor = loop {
        if let Some(cursor) = runtime
            .catalog
            .group_schedule_cursor("line")
            .await
            .unwrap()
        {
            break cursor;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the group schedule never consumed its occurrence"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_eq!(
        cursor.last_group_id, None,
        "a rejected occurrence is consumed WITHOUT a group -- there is nothing outstanding"
    );
    assert!(
        jobs_for(&runtime, "camera-a").await.is_empty(),
        "an all-or-nothing group that could not be accepted must not capture its healthy member"
    );
    runtime.shutdown().await;
}

/// A reload commits the generation it validated even when the runtime's background-task registry
/// has failed underneath it. Supervisors, schedules, and discovery are best-effort restarts AFTER
/// the atomic swap: aborting there would leave the component with a published configuration it had
/// half-applied, which is precisely the state the whole plane is built to make impossible.
#[cfg(all(feature = "standalone", feature = "onvif"))]
#[tokio::test]
async fn a_committed_reload_still_advances_its_generation_when_no_background_task_can_be_spawned() {
    let directory = TempDir::new().unwrap();
    let (port, _) = spawn_recording_mqtt_broker().await;
    let core = facade_core(&directory, port).await;
    let mut initial = config(directory.path(), &["camera-a"], true);
    initial.global.discovery.enabled = true;
    let runtime = runtime(initial, &directory).await;

    // The registry every spawn goes through is now unusable: supervisors, schedules and discovery
    // can no longer be started at all.
    let poisoner = Arc::clone(&runtime);
    let _ = std::thread::spawn(move || {
        let _held = poisoner.tasks.lock().unwrap();
        panic!("poison the runtime task registry");
    })
    .join();
    assert!(
        runtime.tasks.lock().is_err(),
        "the task registry must be poisoned for this test to exercise anything"
    );

    let mut replacement = config(directory.path(), &["camera-a", "camera-c"], true);
    replacement.global.discovery.enabled = true;
    let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.seed = Some(808);
    let instance = core.instance("camera-c").unwrap();

    let diff = runtime
        .apply_reloaded_config(
            replacement,
            BTreeMap::from([("camera-c".to_string(), Arc::new(instance.app()))]),
            BTreeMap::from([("camera-c".to_string(), instance.events())]),
        )
        .await
        .expect("a failure to spawn background work must not abort a validated generation");

    assert_eq!(diff.added, vec!["camera-c".to_string()]);
    assert_eq!(diff.lifecycle_changed, vec!["camera-a".to_string()]);
    assert_eq!(
        runtime.registry.ids().unwrap(),
        vec!["camera-a".to_string(), "camera-c".to_string()],
        "the roster is the one the candidate described"
    );
    let crate::config::BackendConfig::Sim(sim) =
        &runtime.config_snapshot().unwrap().instances[0].backend
    else {
        panic!("test fixture must use the simulator backend");
    };
    assert_eq!(
        sim.seed,
        Some(808),
        "the committed configuration generation is published, not rolled back"
    );
    runtime.shutdown().await;
}

/// Neither plane may quietly do nothing. If the map that owns the schedule tasks -- or the one that
/// owns the discovery generation -- cannot be taken, the start/restart REPORTS it, which is what
/// lets the reload path log the failure instead of leaving an operator with a configuration that
/// looks applied and a component that has silently stopped scheduling.
#[tokio::test]
async fn a_schedule_or_discovery_registry_that_cannot_be_taken_is_reported_not_ignored() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], true);
    configuration.instances[0].schedules[0].cron = EFFECTIVELY_NEVER.to_string();
    configuration.global.discovery.enabled = true;
    let runtime = runtime(configuration, &directory).await;

    let poisoner = Arc::clone(&runtime);
    let _ = std::thread::spawn(move || {
        let _schedules = poisoner.scheduler_cancellations.write().unwrap();
        panic!("poison the schedule task map");
    })
    .join();
    let poisoner = Arc::clone(&runtime);
    let _ = std::thread::spawn(move || {
        let _discovery = poisoner.discovery_cancellation.write().unwrap();
        panic!("poison the discovery cancellation");
    })
    .join();

    for error in [
        runtime.start_schedulers().unwrap_err(),
        runtime.restart_schedulers().unwrap_err(),
    ] {
        assert!(
            error.to_string().contains("schedule task map is unavailable"),
            "an unusable schedule task map must be surfaced: {error}"
        );
    }
    for error in [
        runtime.start_periodic_discovery().unwrap_err(),
        runtime.restart_periodic_discovery().unwrap_err(),
    ] {
        assert!(
            error
                .to_string()
                .contains("discovery cancellation is unavailable"),
            "an unusable discovery generation must be surfaced: {error}"
        );
    }
    runtime.shutdown().await;
}

/// The occurrence scan is BOUNDED. A cursor left far behind -- by a long outage, or a clock jump --
/// must not turn one poll tick into an unbounded cron search that pins a core and starves the
/// capture path. The evaluation is refused, loudly, and the schedule admits nothing from it.
///
/// camera-b, whose cursor is current, is the positive control: it keeps admitting throughout.
#[tokio::test]
async fn an_occurrence_window_too_large_to_scan_is_refused_rather_than_scanned() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], true);
    every_second_schedule(&mut configuration, "camera-a");
    copy_schedule_to(&mut configuration, "camera-b");
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[1].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    configuration.global.capture_group_schedules.push(group_schedule(
        "line",
        EVERY_SECOND,
        &["camera-a", "camera-b"],
    ));
    let runtime = runtime(configuration.clone(), &directory).await;
    runtime
        .start_supervisor("camera-b".to_string(), runtime.engine("camera-b").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-b").await;

    // Four hours of once-a-second occurrences is far beyond the bounded scan.
    let stale = chrono::Utc::now() - chrono::Duration::hours(4);
    runtime
        .catalog
        .accept_scheduled_job(
            scheduled_job(&configuration, "camera-a", "cap_stale_cursor", "minute"),
            "minute",
            stale.timestamp_millis(),
        )
        .await
        .unwrap();
    runtime
        .catalog
        .record_group_schedule_occurrence(
            "line",
            stale.timestamp_millis(),
            None,
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        .unwrap();

    runtime.start_schedulers().unwrap();

    // The control camera proves the poll loops are alive and evaluating.
    wait_for_job_count(&runtime, "camera-b", 2).await;
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "the refused evaluation must admit nothing: only the seeded job exists"
    );
    assert_eq!(
        runtime
            .catalog
            .group_schedule_cursor("line")
            .await
            .unwrap()
            .expect("the seeded group cursor survives")
            .intended_fire_time_ms,
        stale.timestamp_millis(),
        "a refused group evaluation consumes no occurrence either"
    );
    runtime.shutdown().await;
}

/// A rollback that cannot reinstate the configuration says so. Returning `Ok` would tell Core the
/// prior generation is live when the runtime is in fact still carrying the failed candidate's --
/// and Core would then publish a snapshot the component is not running.
#[tokio::test]
async fn a_rollback_that_cannot_reinstate_the_configuration_reports_the_failure() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let checkpoint = runtime.reload_checkpoint().unwrap();

    let poisoner = Arc::clone(&runtime);
    let _ = std::thread::spawn(move || {
        let _held = poisoner.config.write().unwrap();
        panic!("poison the runtime configuration lock");
    })
    .join();

    let error = runtime
        .restore_reload_checkpoint(checkpoint)
        .await
        .expect_err("a rollback that cannot write the configuration must not report success");
    assert!(
        error
            .to_string()
            .contains("runtime configuration lock is unavailable during rollback"),
        "the failure must name what could not be restored: {error}"
    );
    runtime.shutdown().await;
}

/// A scheduled capture inherits from its backend what the profile does not say: the capture mode a
/// GenICam camera is triggered with is not the one an ONVIF camera is, and a schedule that
/// substituted one for the other would silently capture the wrong way.
#[tokio::test]
async fn a_scheduled_capture_inherits_the_capture_mode_of_the_backend_that_will_take_it() {
    let directory = TempDir::new().unwrap();
    let mut raw = core_config_value(directory.path(), &["camera-a", "camera-b"], true);
    raw["component"]["instances"][0]["backend"] = json!({
        "type": "genicam-aravis",
        "selector": { "serial": "scheduled-genicam" }
    });
    raw["component"]["instances"][1]["backend"] = json!({
        "type": "onvif-rtsp",
        "mediaProfile": "main",
        "deviceServiceUrl": "https://10.0.0.2/onvif/device_service"
    });
    raw["component"]["instances"][1]["schedules"] = json!([{
        "id": "minute",
        "cron": "0 * * * * *",
        "timezone": "UTC",
        "captureProfile": "main"
    }]);
    let configuration = AdapterConfig::from_core_reload(
        &Config::from_value(COMPONENT_NAME, "gw-01", raw).unwrap(),
    )
    .unwrap();
    let runtime = runtime(configuration, &directory).await;

    for instance in ["camera-a", "camera-b"] {
        runtime
            .submit_scheduled(&occurrence(
                crate::scheduler::ScheduleScope::Camera(instance.to_string()),
                "minute",
            ))
            .await
            .unwrap();
    }

    assert_eq!(
        jobs_for(&runtime, "camera-a").await[0].effective_profile["captureMode"],
        json!("software-trigger"),
        "a GenICam camera is software-triggered"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-b").await[0].effective_profile["captureMode"],
        json!("snapshot-uri"),
        "an ONVIF camera takes its configured backend default"
    );
    runtime.shutdown().await;
}

/// A removed camera's queued work is terminalized to the LAST job, not to the first page of them.
///
/// The interruption reads the backlog in pages of a thousand. A camera that had more than that
/// queued -- exactly the camera an operator is most likely to remove -- would otherwise keep every
/// job past the first page in QUEUED forever: work for a camera that no longer exists, which
/// nothing will ever run and nobody will ever be told about.
#[tokio::test]
async fn removing_a_camera_terminalizes_every_page_of_its_queued_work() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(initial.clone(), &directory).await;

    // One more than the interruption's page size.
    let backlog = 1_001;
    for index in 0..backlog {
        let capture_id = format!("cap_backlog_{index:04}");
        let mut job = queued_job(&initial, &capture_id);
        // Each queued capture is its own command, so each carries its own idempotency key.
        job.ledger_key = Some(
            crate::catalog::LedgerKey::new("camera-b", "sb/capture", format!("backlog-{index}"))
                .unwrap(),
        );
        runtime.catalog.accept_job(job).await.unwrap();
        runtime
            .catalog
            .queue_job(&capture_id, chrono::Utc::now().timestamp_millis())
            .await
            .unwrap();
    }

    let replacement = config(directory.path(), &["camera-a"], false);
    let diff = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .unwrap();

    assert_eq!(diff.removed, vec!["camera-b".to_string()]);
    let remaining = runtime
        .catalog
        .count_jobs_by_state(
            Some("camera-b".to_string()),
            vec![
                crate::model::JobState::Accepted,
                crate::model::JobState::Queued,
            ],
        )
        .await
        .unwrap()
        .values()
        .sum::<u64>();
    assert_eq!(
        remaining, 0,
        "every page of the removed camera's queue must be terminalized, not just the first"
    );
    let last = runtime
        .catalog
        .job(format!("cap_backlog_{:04}", backlog - 1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(last.state, crate::model::JobState::Interrupted);
    assert_eq!(last.error_code.as_deref(), Some("PROCESS_INTERRUPTED"));
    runtime.shutdown().await;
}

/// A camera whose runtime slot has already gone is still removed cleanly.
///
/// The queue interruption cannot reach its engine, and the reload says so rather than aborting: the
/// registry generation has to advance, or the component keeps a camera the operator has deleted.
/// The work it could not reach is left exactly as it is -- never falsely reported as terminalized.
#[tokio::test]
async fn removing_a_camera_whose_slot_is_already_gone_still_advances_the_generation() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(initial.clone(), &directory).await;
    runtime
        .catalog
        .accept_job(queued_job(&initial, "cap_slotless"))
        .await
        .unwrap();
    runtime
        .catalog
        .queue_job("cap_slotless", chrono::Utc::now().timestamp_millis())
        .await
        .unwrap();
    runtime.cameras.write().unwrap().remove("camera-b");

    let replacement = config(directory.path(), &["camera-a"], false);
    let diff = runtime
        .apply_reloaded_config(replacement, BTreeMap::new(), BTreeMap::new())
        .await
        .expect("a camera with no runtime slot must not abort a validated generation");

    assert_eq!(diff.removed, vec!["camera-b".to_string()]);
    assert!(
        runtime.registry.snapshot("camera-b").is_err(),
        "the camera the operator deleted is gone from the published roster"
    );
    assert_eq!(
        runtime
            .catalog
            .job("cap_slotless")
            .await
            .unwrap()
            .unwrap()
            .state,
        crate::model::JobState::Queued,
        "work the runtime could not reach is left as it was, not falsely terminalized"
    );
    runtime.shutdown().await;
}

/// A retained observation of a camera that is not there, so a discovery pass has something to erase.
fn stale_candidate() -> DiscoveryCandidate {
    DiscoveryCandidate {
        backend: crate::model::BackendKind::GenicamAravis,
        selector: json!({ "serial": "stale-discovery" }),
        vendor: None,
        model: None,
        capabilities: json!({}),
    }
}

/// Waits until a discovery pass has replaced the retained observations with what it actually saw.
async fn wait_for_discovery_pass(runtime: &CameraRuntime, which: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if runtime
            .discovery_cache
            .lock()
            .unwrap()
            .candidates
            .is_empty()
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the {which} periodic discovery pass never replaced the retained observation"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
