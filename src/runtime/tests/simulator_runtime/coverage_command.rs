//! Command-plane paths the suite had never reached.

use super::*;

/// A group capture request the caller can retry verbatim.
fn group_request(request_id: &str, cameras: &[&str]) -> GroupCaptureRequest {
    GroupCaptureRequest {
        request_id: request_id.to_string(),
        instances: cameras.iter().map(|camera| (*camera).to_string()).collect(),
        capture_profile: None,
        profile_overrides: BTreeMap::new(),
        timeout_ms: None,
        metadata: serde_json::Map::new(),
    }
}

/// The durable group a `requestId` would be keyed by, or `None` if nothing was ever accepted.
async fn group_for(
    runtime: &CameraRuntime,
    request_id: &str,
) -> Option<crate::catalog::GroupRecord> {
    runtime
        .catalog
        .group_by_ledger(
            crate::catalog::LedgerKey::new("main", "sb/capture-group", request_id).unwrap(),
        )
        .await
        .unwrap()
}

/// Every durable capture row a camera owns, in any state.
async fn jobs_for(runtime: &CameraRuntime, instance: &str) -> Vec<crate::catalog::JobRecord> {
    runtime
        .catalog
        .jobs_page(Some(instance.to_string()), Vec::new(), None, 100)
        .await
        .unwrap()
}

/// A southbound command that names no reply topic, which is what a deferred verb cannot serve.
fn command_message_without_reply_to(verb: &str, body: serde_json::Value) -> Message {
    MessageBuilder::new(verb, "1.0")
        .correlation_id(format!("no-reply-to-{verb}"))
        .structured_payload(body)
        .build()
}

/// Drives a deferred capture to completion and hands back what its continuation answered.
async fn deferred_capture_outcome(
    runtime: &CameraRuntime,
    deferred: &DeferredReplyRegistry,
    suffix: &str,
    body: serde_json::Value,
) -> std::result::Result<(), CommandError> {
    match runtime
        .handle_deferred_capture(command_message("sb/capture", suffix, body), deferred.clone())
        .await
    {
        CommandOutcome::DeferredWithContinuation { continuation, .. } => continuation.await,
        other => panic!("a direct capture must hand off to a deferred continuation, got {other:?}"),
    }
}

/// How many replies the broker has seen on the fixture's guarded reply topic.
fn recorded_replies(publishes: &RecordedMqttPublishes) -> usize {
    publishes
        .lock()
        .unwrap()
        .iter()
        .filter(|(topic, _)| topic == "camera-adapter-command-e2e/replies")
        .count()
}

/// Waits until the broker has seen at least `expected` replies, so a fan-out can be counted.
async fn wait_for_recorded_replies(publishes: &RecordedMqttPublishes, expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let seen = recorded_replies(publishes);
        if seen >= expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {seen} of {expected} deferred callers were ever answered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The cancellation token of the mandatory stop a camera is currently carrying, if any.
fn armed_stop(runtime: &CameraRuntime, instance: &str) -> Option<CancellationToken> {
    runtime
        .cameras
        .read()
        .unwrap()
        .get(instance)
        .and_then(|slot| slot.motion_stop.as_ref())
        .map(|armed| armed.cancellation.clone())
}

/// A capture group must be a GROUP: one camera is a capture, and it is refused as one.
///
/// The floor is enforced before any durable row exists, so a caller that gets this wrong burns no
/// `requestId` and leaves nothing behind for a retry to collide with.
#[tokio::test]
async fn a_group_capture_of_one_camera_is_refused() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    let error = runtime
        .submit_group(
            group_request("group-of-one", &["camera-a"]),
            "group-of-one-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        crate::ErrorCode::BadArgs,
        "a one-camera group is not a group"
    );
    assert!(
        group_for(&runtime, "group-of-one").await.is_none(),
        "a refused group must not leave a durable ledger entry behind"
    );
    runtime.shutdown().await;
}

/// A group wider than `limits.maxCamerasPerGroup` is refused with the code that says so.
///
/// Every member of a group is admitted together, which is what the bound exists to cap. A caller
/// that asks for more cameras than the component will hold must be told GROUP_TOO_LARGE -- not have
/// its group quietly truncated to the first few.
#[tokio::test]
async fn a_group_larger_than_the_configured_limit_is_refused() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(
        directory.path(),
        &["camera-a", "camera-b", "camera-c"],
        false,
    );
    configuration.global.limits.max_cameras_per_group = 2;
    let runtime = runtime(configuration, &directory).await;

    let error = runtime
        .submit_group(
            group_request("group-too-large", &["camera-a", "camera-b", "camera-c"]),
            "group-too-large-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        crate::ErrorCode::GroupTooLarge,
        "three cameras exceed a limit of two, and the caller must be told which limit it hit"
    );
    assert!(
        group_for(&runtime, "group-too-large").await.is_none(),
        "an oversized group must not be durably accepted"
    );
    runtime.shutdown().await;
}

/// A group naming a camera that does not exist creates NOTHING -- not even for the cameras that do.
///
/// The group contract is all-or-nothing, and the only way to keep that promise is to resolve every
/// member before the first durable row is written. A member-at-a-time implementation would leave
/// camera-a holding an accepted capture that belongs to a group which was never accepted.
#[tokio::test]
async fn a_group_naming_an_unknown_camera_creates_nothing_at_all() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(
        config(directory.path(), &["camera-a", "camera-b"], false),
        &directory,
    )
    .await;

    let error = runtime
        .submit_group(
            group_request("group-unknown-member", &["camera-a", "camera-ghost"]),
            "group-unknown-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        crate::ErrorCode::NoSuchInstance,
        "a group naming a camera that is not configured is refused whole"
    );
    assert!(
        group_for(&runtime, "group-unknown-member").await.is_none(),
        "no durable group may survive a member that could not be resolved"
    );
    assert!(
        jobs_for(&runtime, "camera-a").await.is_empty(),
        "and the member that DID resolve must not be left holding a capture from a group that does \
         not exist"
    );
    runtime.shutdown().await;
}

/// A group naming a disabled camera is refused whole, for the same all-or-nothing reason.
///
/// Disabled does not mean "skip me": an operator who switched a camera off and then asked for a
/// synchronised group including it asked for something the component cannot deliver, and a partial
/// group is not a smaller version of that answer.
#[tokio::test]
async fn a_group_naming_a_disabled_camera_creates_nothing_at_all() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    configuration
        .instances
        .iter_mut()
        .find(|camera| camera.id == "camera-b")
        .expect("the fixture configures camera-b")
        .enabled = false;
    let runtime = runtime(configuration, &directory).await;

    let error = runtime
        .submit_group(
            group_request("group-disabled-member", &["camera-a", "camera-b"]),
            "group-disabled-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        crate::ErrorCode::CameraDisabled,
        "a disabled camera is refused, not quietly dropped from the group"
    );
    assert!(
        group_for(&runtime, "group-disabled-member").await.is_none(),
        "no durable group may survive a disabled member"
    );
    assert!(
        jobs_for(&runtime, "camera-a").await.is_empty(),
        "and the enabled member must not be left holding a capture of its own"
    );
    runtime.shutdown().await;
}

/// The group MEMBER builder refuses a disabled camera on its own account.
///
/// `submit_group` resolves every member first, so this guard never fires through that path -- which
/// is exactly why it is worth pinning directly. It belongs to the builder: a second caller that
/// reached it without resolving first would otherwise build a capture for a camera an operator has
/// switched off.
#[tokio::test]
async fn the_group_member_builder_refuses_a_disabled_camera() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    configuration
        .instances
        .iter_mut()
        .find(|camera| camera.id == "camera-b")
        .expect("the fixture configures camera-b")
        .enabled = false;
    let runtime = runtime(configuration, &directory).await;

    let Err(error) = runtime
        .build_group_submission(
            "camera-b",
            2,
            "builder-disabled",
            "grp_builder_disabled",
            None,
            None,
            serde_json::Map::new(),
            "builder-disabled-correlation".to_string(),
        )
        .await
    else {
        panic!("the builder must refuse a disabled camera");
    };
    assert_eq!(
        error.code(),
        crate::ErrorCode::CameraDisabled,
        "the builder must not manufacture a capture for a camera that is switched off"
    );
    runtime.shutdown().await;
}

/// A deferred caller that replays a group still in flight is ATTACHED to it, not answered early.
///
/// The replay returns the original group, so the tempting thing is to reply with the group as it
/// stands -- which, for an unfinished group, is a reply that says nothing. The caller asked for the
/// RESULT, so it is registered against the running group and answered when that group is terminal.
#[tokio::test]
async fn a_deferred_caller_replaying_an_unfinished_group_waits_for_it() {
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    let runtime = runtime(configuration, &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    // No supervisor yet: the group is durably accepted and queued, and it cannot finish.
    let accepted = runtime
        .submit_group(
            group_request("group-waiter", &["camera-a", "camera-b"]),
            "group-waiter-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap();
    assert!(
        !accepted.state.is_terminal(),
        "the group must still be running for this test to mean anything"
    );

    let token = deferred
        .defer(
            &command_message(
                "sb/capture-group",
                "group-waiter",
                json!({ "requestId": "group-waiter", "instances": ["camera-a", "camera-b"] }),
            ),
            Duration::from_secs(30),
        )
        .expect("the fixture message carries a reply topic");
    token.activate().expect("the command inbox is running");
    let replayed = runtime
        .submit_group(
            group_request("group-waiter", &["camera-a", "camera-b"]),
            "group-waiter-replay-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            Some(token),
        )
        .await
        .expect("an exact group retry must be replayable");
    assert_eq!(
        replayed.group_id, accepted.group_id,
        "the replay must return the original group, not accept a second one"
    );
    assert_eq!(
        recorded_replies(&publishes),
        0,
        "a caller waiting on an unfinished group must not be answered yet"
    );

    // Now let the group finish. The attached caller is the thing that has to be answered.
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }
    let terminal = wait_for_group_terminal(&runtime, &accepted.group_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    let reply = wait_for_recorded_reply(&publishes, 0).await;
    assert_eq!(reply.body["ok"], true);
    assert_eq!(
        reply.body["result"]["captureGroupId"], accepted.group_id,
        "the caller attached to the running group must be answered with THAT group's result"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// A deferred caller that replays a group which has already finished is answered at once.
///
/// There is nothing left to attach to: the fan-out that settles a group's waiters has already run
/// and will not run again, so registering a waiter here would hang the caller until its reply
/// lifetime expired. It is settled from the durable result instead.
#[tokio::test]
async fn a_deferred_caller_replaying_a_finished_group_is_settled_at_once() {
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    let runtime = runtime(configuration, &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    let accepted = runtime
        .submit_group(
            group_request("group-finished", &["camera-a", "camera-b"]),
            "group-finished-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap();
    let terminal = wait_for_group_terminal(&runtime, &accepted.group_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);

    let token = deferred
        .defer(
            &command_message(
                "sb/capture-group",
                "group-finished",
                json!({ "requestId": "group-finished", "instances": ["camera-a", "camera-b"] }),
            ),
            Duration::from_secs(30),
        )
        .expect("the fixture message carries a reply topic");
    token.activate().expect("the command inbox is running");
    let replayed = runtime
        .submit_group(
            group_request("group-finished", &["camera-a", "camera-b"]),
            "group-finished-replay-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            Some(token),
        )
        .await
        .expect("an exact group retry must be replayable");
    assert_eq!(replayed.group_id, accepted.group_id);

    let reply = wait_for_recorded_reply(&publishes, 0).await;
    assert_eq!(reply.body["ok"], true);
    assert_eq!(
        reply.body["result"]["captureGroupId"], accepted.group_id,
        "a caller arriving after the group finished is answered from the durable result"
    );
    assert_eq!(
        reply.body["result"]["members"].as_array().unwrap().len(),
        2,
        "and it gets the whole member vector, exactly as the original caller did"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// A submitted capture whose `requestId` is reused with different arguments is a CONFLICT.
///
/// The key is caller-owned and durable. Serving a changed request from the first acceptance would
/// quietly execute something the caller did not ask for, so the second request is refused rather
/// than answered from a row that describes different work.
#[tokio::test]
async fn a_submitted_capture_reusing_a_request_id_with_changed_arguments_conflicts() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let main = configuration.instances[0]
        .capture_profiles
        .get("main")
        .cloned()
        .expect("the fixture configures a main profile");
    configuration.instances[0]
        .capture_profiles
        .insert("secondary".to_string(), main);
    let runtime = runtime(configuration, &directory).await;

    let first = runtime
        .submit_capture(
            "camera-a".to_string(),
            "capture-conflict".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "capture-conflict-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture_id = match first {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected a newly accepted capture, got {other:?}"),
    };

    match runtime
        .submit_capture(
            "camera-a".to_string(),
            "capture-conflict".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "capture-conflict-replay".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap()
    {
        crate::catalog::AcceptJobOutcome::Existing(record) => assert_eq!(
            record.capture_id, capture_id,
            "an exact retry must return the ORIGINAL capture, not accept a second one"
        ),
        other => panic!("an exact retry must replay the existing capture, got {other:?}"),
    }

    let changed = runtime
        .submit_capture(
            "camera-a".to_string(),
            "capture-conflict".to_string(),
            Some("secondary".to_string()),
            None,
            serde_json::Map::new(),
            "capture-conflict-changed".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    assert!(
        matches!(changed, crate::catalog::AcceptJobOutcome::Conflict),
        "the same key with a different capture profile must never be served from the first job"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "and the conflicting request must not have created a second durable capture"
    );
    runtime.shutdown().await;
}

/// Storage pressure never un-accepts a capture that is already durable.
///
/// The capacity check runs before the durable row is written, so a retry arriving under pressure
/// would be answered STORAGE_PRESSURE even though its work was accepted long ago -- an operator
/// would watch a promised capture disown itself because the disk filled up afterwards. The ledger
/// is therefore consulted again before the rejection is returned.
#[tokio::test]
async fn storage_pressure_still_replays_an_exact_capture_retry() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let main = configuration.instances[0]
        .capture_profiles
        .get("main")
        .cloned()
        .expect("the fixture configures a main profile");
    configuration.instances[0]
        .capture_profiles
        .insert("secondary".to_string(), main);
    let pressured = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let monitor = StoragePressureMonitor::new(
        configuration.global.output.root_directory.clone(),
        directory.path().join("state"),
        &configuration.global.output,
        Arc::new(ToggleSpaceProbe {
            pressured: Arc::clone(&pressured),
        }),
    );
    let runtime = runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "pressure-replay".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "pressure-replay-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected a newly accepted capture, got {other:?}"),
    };
    pressured.store(true, std::sync::atomic::Ordering::Release);

    match runtime
        .submit_capture(
            "camera-a".to_string(),
            "pressure-replay".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "pressure-replay-retry".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("pressure must not un-accept work that is already durable")
    {
        crate::catalog::AcceptJobOutcome::Existing(record) => {
            assert_eq!(record.capture_id, capture_id);
        }
        other => panic!("an exact retry under pressure must replay the existing capture, got {other:?}"),
    }

    let changed = runtime
        .submit_capture(
            "camera-a".to_string(),
            "pressure-replay".to_string(),
            Some("secondary".to_string()),
            None,
            serde_json::Map::new(),
            "pressure-replay-changed".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("a conflicting retry is answered from the ledger, not from the disk");
    assert!(
        matches!(changed, crate::catalog::AcceptJobOutcome::Conflict),
        "a reused key with changed arguments stays a conflict even while the disk is full"
    );

    assert_eq!(
        runtime
            .submit_capture(
                "camera-a".to_string(),
                "pressure-fresh".to_string(),
                None,
                None,
                serde_json::Map::new(),
                "pressure-fresh-correlation".to_string(),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::StoragePressure,
        "but genuinely new work is still refused while the disk is full"
    );
    runtime.shutdown().await;
}

/// A deferred capture that reuses a `requestId` with changed arguments is refused, not served.
///
/// The deferred path registers its waiter BEFORE it knows whether the request is acceptable, so the
/// conflict has to drop that waiter again -- otherwise a rejected caller is left attached to a
/// capture it never owned, and would be answered with somebody else's result.
#[tokio::test]
async fn a_deferred_capture_reusing_a_request_id_with_changed_arguments_conflicts() {
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let main = configuration.instances[0]
        .capture_profiles
        .get("main")
        .cloned()
        .expect("the fixture configures a main profile");
    configuration.instances[0]
        .capture_profiles
        .insert("secondary".to_string(), main);
    let runtime = runtime(configuration, &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    // No supervisor: the capture is accepted and stays queued, so the retry meets a live job.
    deferred_capture_outcome(
        &runtime,
        &deferred,
        "deferred-conflict",
        json!({ "instance": "camera-a", "requestId": "deferred-conflict" }),
    )
    .await
    .expect("the first deferred capture must be accepted");

    let error = deferred_capture_outcome(
        &runtime,
        &deferred,
        "deferred-conflict-changed",
        json!({
            "instance": "camera-a",
            "requestId": "deferred-conflict",
            "captureProfile": "secondary"
        }),
    )
    .await
    .expect_err("a changed retry must not be served from the first capture");
    assert_eq!(
        error.code,
        crate::ErrorCode::IdempotencyConflict.as_str(),
        "the caller must be told its key was already used for different work"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "and the conflict must not have created a second capture"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// Two callers waiting on the SAME capture are both answered when it finishes.
///
/// A retried direct capture is not a second capture: it attaches another caller to the job already
/// running. Both tokens are held until that capture is terminal, and the terminal fan-out must
/// reach both -- answering only the first leaves the retrying client waiting for a reply that was
/// delivered to somebody else.
#[tokio::test]
async fn a_second_deferred_caller_waits_on_the_same_capture_and_both_are_answered() {
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(configuration, &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    for suffix in ["first-waiter", "second-waiter"] {
        deferred_capture_outcome(
            &runtime,
            &deferred,
            suffix,
            json!({ "instance": "camera-a", "requestId": "shared-capture" }),
        )
        .await
        .expect("both deferred callers must be accepted onto the same capture");
    }
    let jobs = jobs_for(&runtime, "camera-a").await;
    assert_eq!(
        jobs.len(),
        1,
        "the retry must attach to the running capture, not accept a second one"
    );
    assert_eq!(
        recorded_replies(&publishes),
        0,
        "neither caller may be answered while the capture is still queued"
    );

    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;
    assert_eq!(
        wait_for_terminal(&runtime, &jobs[0].capture_id).await.state,
        crate::model::JobState::Succeeded
    );
    wait_for_recorded_replies(&publishes, 2).await;
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// The waiter bound is real: a client that keeps retrying cannot grow the list without limit.
///
/// Every attached token is held in memory until the capture is terminal, which is what
/// `limits.maxDeferredWaitersPerCapture` exists to bound. The caller past the bound is refused with
/// RESOURCE_LIMIT instead of being quietly accumulated.
#[tokio::test]
async fn a_deferred_caller_past_the_waiter_bound_is_refused() {
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    // Production wires this from `limits.maxDeferredWaitersPerCapture` in `CameraRuntime::start`;
    // the minimal test builder leaves it at its floor, so the bound is set here to the same effect.
    runtime.waiters.set_waiter_limit(1);
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    deferred_capture_outcome(
        &runtime,
        &deferred,
        "bounded-first",
        json!({ "instance": "camera-a", "requestId": "bounded-capture" }),
    )
    .await
    .expect("the first caller is within the bound");

    let error = deferred_capture_outcome(
        &runtime,
        &deferred,
        "bounded-second",
        json!({ "instance": "camera-a", "requestId": "bounded-capture" }),
    )
    .await
    .expect_err("the second caller is past a bound of one and must be refused");
    assert_eq!(
        error.code,
        crate::ErrorCode::ResourceLimit.as_str(),
        "a capture already at its waiter bound must say so rather than grow"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// A caller arriving after its capture has finished is answered from the durable result.
///
/// There is no waiter left to attach to -- the terminal fan-out has already run -- so the reply
/// comes from the row the capture left behind. The alternative is a retry that hangs until its
/// reply lifetime expires, which an operator would report as the component losing their request.
#[tokio::test]
async fn a_deferred_caller_arriving_after_the_capture_finished_is_settled_from_the_durable_result() {
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(configuration, &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    deferred_capture_outcome(
        &runtime,
        &deferred,
        "settled-original",
        json!({ "instance": "camera-a", "requestId": "settled-capture" }),
    )
    .await
    .expect("the original caller must be accepted");
    let jobs = jobs_for(&runtime, "camera-a").await;
    let terminal = wait_for_terminal(&runtime, &jobs[0].capture_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    wait_for_recorded_replies(&publishes, 1).await;
    let already_answered = recorded_replies(&publishes);

    deferred_capture_outcome(
        &runtime,
        &deferred,
        "settled-late",
        json!({ "instance": "camera-a", "requestId": "settled-capture" }),
    )
    .await
    .expect("a late retry of a finished capture must be accepted, not rejected");
    wait_for_recorded_replies(&publishes, already_answered + 1).await;
    let reply = wait_for_recorded_reply(&publishes, already_answered).await;
    assert_eq!(reply.body["ok"], true);
    assert_eq!(
        reply.body["result"]["captureId"], terminal.capture_id,
        "the late caller is answered with the finished capture's own terminal result"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// A deferred verb with nowhere to reply is refused before it accepts anything.
///
/// A deferred command exists to answer later, on the caller's reply topic. Without one there is
/// nobody to answer, and running the work anyway would drive a camera for a result that could never
/// be delivered.
#[tokio::test]
async fn a_deferred_verb_with_no_reply_topic_is_refused_before_it_accepts_anything() {
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let runtime = runtime(
        config(directory.path(), &["camera-a", "camera-b"], false),
        &directory,
    )
    .await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    match runtime
        .handle_deferred_capture(
            command_message_without_reply_to(
                "sb/capture",
                json!({ "instance": "camera-a", "requestId": "no-reply-capture" }),
            ),
            deferred.clone(),
        )
        .await
    {
        CommandOutcome::ImmediateError(error) => assert_eq!(
            error.code,
            crate::ErrorCode::ReplyRequired.as_str(),
            "a capture with nowhere to send its result must be refused"
        ),
        other => panic!("a capture with no reply topic must be refused, got {other:?}"),
    }
    match runtime
        .handle_deferred_group_capture(
            command_message_without_reply_to(
                "sb/capture-group",
                json!({
                    "requestId": "no-reply-group",
                    "instances": ["camera-a", "camera-b"]
                }),
            ),
            deferred.clone(),
        )
        .await
    {
        CommandOutcome::ImmediateError(error) => assert_eq!(
            error.code,
            crate::ErrorCode::ReplyRequired.as_str(),
            "a group with nowhere to send its result must be refused"
        ),
        other => panic!("a group with no reply topic must be refused, got {other:?}"),
    }
    assert!(
        jobs_for(&runtime, "camera-a").await.is_empty(),
        "and neither refusal may leave durable work behind"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// A deferred verb validates its body BEFORE it defers, so a bad request fails fast.
///
/// There is nothing to wait for when the body is junk: deferring first and discovering that
/// afterwards would burn a deferred slot and answer the caller on the slow path for no reason.
#[tokio::test]
async fn a_deferred_verb_rejects_an_invalid_body_immediately() {
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let runtime = runtime(
        config(directory.path(), &["camera-a", "camera-b"], false),
        &directory,
    )
    .await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    match runtime
        .handle_deferred_capture(
            command_message(
                "sb/capture",
                "unknown-field",
                json!({ "instance": "camera-a", "requestId": "bad-body", "nope": true }),
            ),
            deferred.clone(),
        )
        .await
    {
        CommandOutcome::ImmediateError(error) => assert_eq!(
            error.code,
            crate::ErrorCode::BadArgs.as_str(),
            "the closed schema must refuse an unknown field"
        ),
        other => panic!("an unparseable capture body must be refused, got {other:?}"),
    }
    match runtime
        .handle_deferred_group_capture(
            command_message(
                "sb/capture-group",
                "single-member",
                json!({ "requestId": "bad-group", "instances": ["camera-a"] }),
            ),
            deferred.clone(),
        )
        .await
    {
        CommandOutcome::ImmediateError(error) => assert_eq!(
            error.code,
            crate::ErrorCode::BadArgs.as_str(),
            "a one-camera group is refused before it is ever deferred"
        ),
        other => panic!("an invalid group body must be refused, got {other:?}"),
    }
    assert!(
        jobs_for(&runtime, "camera-a").await.is_empty(),
        "and neither refusal may leave durable work behind"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// The CAMERA's continuous-move bound is enforced, not just the schema's 60-second ceiling.
///
/// `validate` only enforces the widest value the field accepts, so on its own it would let an
/// operator command a minute of motion on a camera configured to move for five seconds -- and the
/// reply would then advertise a stop deadline nothing was going to honour. The refusal also leaves
/// the `requestId` unburnt, so the caller can immediately reissue a move within the bound.
#[tokio::test]
async fn a_continuous_move_beyond_the_cameras_own_bound_is_refused() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 5_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let too_long: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "bounded-move",
        "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 10_000
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_ptz(too_long).await.unwrap_err().code(),
        crate::ErrorCode::PtzRangeError,
        "ten seconds of motion on a camera bounded to five must be refused"
    );
    assert!(
        armed_stop(&runtime, "camera-a").is_none(),
        "a refused move must not arm a mandatory stop"
    );

    let within_bound: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "bounded-move",
        "velocity": { "pan": 0.5, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 4_000
    }))
    .unwrap();
    let reply = runtime
        .perform_ptz(within_bound)
        .await
        .expect("the refusal must not have burnt the caller's requestId");
    assert_eq!(reply["state"], "COMMANDED");
    assert!(
        !reply["stopDeadline"].is_null(),
        "a move within the bound is armed to stop itself, and says when"
    );
    runtime.shutdown().await;
}

/// A zero velocity is not motion, so it arms no mandatory stop and promises no deadline.
///
/// The timer exists to end motion that nothing else will end. Arming one for a camera that is not
/// moving would queue a safety stop -- which pre-empts a running capture -- for no reason, and the
/// reply would advertise a `stopDeadline` for a move that never started.
#[tokio::test]
async fn a_zero_velocity_continuous_move_arms_no_mandatory_stop() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let still: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "zero-velocity",
        "velocity": { "pan": 0.0, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 5_000
    }))
    .unwrap();
    let reply = runtime.perform_ptz(still).await.unwrap();
    assert_eq!(reply["state"], "COMMANDED");
    assert!(
        reply["stopDeadline"].is_null(),
        "a camera that is not moving must not be promised a stop deadline"
    );
    assert!(
        armed_stop(&runtime, "camera-a").is_none(),
        "and no stop timer may be armed for motion that never started"
    );
    runtime.shutdown().await;
}

/// A superseding move retires the timer the previous one armed.
///
/// The old deadline belongs to the old move. Left running, it fires part-way through the NEW motion
/// and stops a camera the operator has just told to keep going: the earlier move's timeout silently
/// truncating a later, longer one.
#[tokio::test]
async fn a_superseding_continuous_move_retires_the_previous_armed_stop() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let first: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "superseded-move",
        "velocity": { "pan": 0.4, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 9_000
    }))
    .unwrap();
    runtime.perform_ptz(first).await.unwrap();
    let first_stop = armed_stop(&runtime, "camera-a").expect("the first move arms a stop");

    let second: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "superseding-move",
        "velocity": { "pan": 0.0, "tilt": 0.6, "zoom": 0.0 },
        "timeoutMs": 9_000
    }))
    .unwrap();
    runtime.perform_ptz(second).await.unwrap();
    assert!(
        first_stop.is_cancelled(),
        "the superseded move's timer must be retired, or its deadline would stop the new motion \
         early"
    );
    assert!(
        armed_stop(&runtime, "camera-a").is_some_and(|stop| !stop.is_cancelled()),
        "and the camera must be left carrying the NEW move's armed stop"
    );
    runtime.shutdown().await;
}

/// A PTZ command that is not itself a continuous move disarms the stop the last move armed.
///
/// The motion is over. A surviving timer would deliver a safety stop -- which pre-empts a running
/// capture -- for a move that something else already ended.
#[tokio::test]
async fn a_ptz_stop_retires_the_mandatory_stop_of_the_move_it_ended() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let move_request: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "continuous",
        "instance": "camera-a",
        "requestId": "disarmed-move",
        "velocity": { "pan": 0.4, "tilt": 0.0, "zoom": 0.0 },
        "timeoutMs": 9_000
    }))
    .unwrap();
    runtime.perform_ptz(move_request).await.unwrap();
    let armed = armed_stop(&runtime, "camera-a").expect("the move arms a stop");

    let stop: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "stop",
        "instance": "camera-a",
        "requestId": "disarming-stop",
        "axes": ["pan", "tilt", "zoom"]
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_ptz(stop).await.unwrap()["state"],
        "COMMANDED"
    );
    assert!(
        armed.is_cancelled(),
        "the stop ended the motion, so the timer that existed to end it must be retired"
    );
    assert!(
        armed_stop(&runtime, "camera-a").is_none(),
        "and the camera must be left carrying no armed stop at all"
    );
    runtime.shutdown().await;
}

/// A PTZ move the camera cannot perform fails, and the retry replays that failure.
///
/// The ledger is completed as FAILED with the outcome the camera gave, so a client retrying the
/// same `requestId` is answered from the record instead of driving a camera that has already
/// refused once. Physical actuation is never repeated behind the caller's back.
#[tokio::test]
async fn a_ptz_move_the_camera_cannot_perform_is_recorded_as_failed_and_replayed() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    // Policy permits PTZ; the CAMERA has none. The refusal is the camera's, which is what makes the
    // durable failure worth pinning.
    configuration.instances[0].ptz.enabled = true;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let request: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "relative",
        "instance": "camera-a",
        "requestId": "unsupported-move",
        "translation": { "pan": 0.1, "tilt": 0.0, "zoom": 0.0 }
    }))
    .unwrap();
    assert_eq!(
        runtime
            .perform_ptz(request.clone())
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::UnsupportedCapability,
        "a camera with no PTZ must refuse the move"
    );

    let replay = runtime
        .perform_ptz(request)
        .await
        .expect("a retry is answered from the ledger rather than re-commanding the camera");
    assert_eq!(
        replay["operation"], "relative",
        "the retry replays the recorded outcome of the command that failed"
    );
    assert!(
        replay.get("state").is_none(),
        "and it must NOT claim the move was COMMANDED, because it never was"
    );
    runtime.shutdown().await;
}

/// A preset RECALL is allowed where a preset MUTATION is not.
///
/// `ptz.allowPresetMutation` guards writes to the camera's preset table. Recalling a preset writes
/// nothing, so it must not be swept up by the same switch -- an operator who locked the table would
/// otherwise also have lost the ability to use it.
#[tokio::test]
async fn preset_mutation_is_refused_where_recall_is_not() {
    let directory = TempDir::new().unwrap();
    let mut configuration = ptz_camera(&directory, 10_000);
    configuration.instances[0].ptz.allow_preset_mutation = false;
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.ptz.presets_supported = true;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let set: PtzPresetsRequest = commands::parse_closed(json!({
        "operation": "set",
        "instance": "camera-a",
        "requestId": "mutation-set",
        "name": "loading-bay"
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_presets(set).await.unwrap_err().code(),
        crate::ErrorCode::UnsupportedCapability,
        "creating a preset writes the camera's preset table and must be refused by policy"
    );
    let remove: PtzPresetsRequest = commands::parse_closed(json!({
        "operation": "remove",
        "instance": "camera-a",
        "requestId": "mutation-remove",
        "token": "preset-1"
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_presets(remove).await.unwrap_err().code(),
        crate::ErrorCode::UnsupportedCapability,
        "and so must removing one"
    );

    // Recall passes the policy gate and reaches the camera, which answers for itself.
    let recall: PtzPresetsRequest = commands::parse_closed(json!({
        "operation": "goto",
        "instance": "camera-a",
        "requestId": "mutation-goto",
        "token": "preset-that-does-not-exist"
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_presets(recall).await.unwrap_err().code(),
        crate::ErrorCode::BadArgs,
        "recall is not a mutation: it reaches the camera, which rejects the unknown token itself"
    );
    runtime.shutdown().await;
}

/// A preset command is refused when PTZ is disabled by configuration.
///
/// The preset table is part of the PTZ surface, so a camera whose PTZ is switched off cannot be
/// driven through it either. The gate is on the camera, not on the individual verb.
#[tokio::test]
async fn preset_commands_are_refused_when_ptz_is_disabled() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    let recall: PtzPresetsRequest = commands::parse_closed(json!({
        "operation": "goto",
        "instance": "camera-a",
        "requestId": "disabled-goto",
        "token": "preset-1"
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_presets(recall).await.unwrap_err().code(),
        crate::ErrorCode::PtzDisabled,
        "a camera whose PTZ is disabled must not move to a preset either"
    );
    runtime.shutdown().await;
}

/// A preset mutation the camera cannot perform fails, and the retry replays that failure.
///
/// Policy permits the write; the camera still refuses it. That refusal is durable, so the retry is
/// answered from the ledger instead of being sent at the camera a second time.
#[tokio::test]
async fn a_preset_mutation_the_camera_cannot_perform_is_recorded_as_failed_and_replayed() {
    let directory = TempDir::new().unwrap();
    let mut configuration = ptz_camera(&directory, 10_000);
    configuration.instances[0].ptz.allow_preset_mutation = true;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let request: PtzPresetsRequest = commands::parse_closed(json!({
        "operation": "set",
        "instance": "camera-a",
        "requestId": "unsupported-preset",
        "name": "loading-bay"
    }))
    .unwrap();
    assert_eq!(
        runtime
            .perform_presets(request.clone())
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::UnsupportedCapability,
        "this camera has no preset table, so even a permitted mutation fails"
    );

    let replay = runtime
        .perform_presets(request)
        .await
        .expect("a retry is answered from the ledger rather than re-commanding the camera");
    assert_eq!(
        replay["operation"], "set",
        "the retry replays the recorded outcome of the preset command that failed"
    );
    assert!(
        replay.get("token").is_none(),
        "and it must not invent a token for a preset that was never created"
    );
    runtime.shutdown().await;
}

/// Cancelling something that does not exist is refused, and records nothing to replay.
///
/// CAPTURE_NOT_FOUND is the honest answer. Recording a successful cancellation of a capture the
/// component has never heard of would make the ledger lie to every retry that followed.
#[tokio::test]
async fn cancelling_an_unknown_capture_or_group_is_refused() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    assert_eq!(
        runtime
            .cancel_capture(CancelRequest {
                request_id: "cancel-ghost-capture".to_string(),
                capture_id: Some("cap_does_not_exist".to_string()),
                capture_group_id: None,
                reason: None,
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::CaptureNotFound,
        "a capture that was never accepted cannot be cancelled"
    );
    assert_eq!(
        runtime
            .cancel_capture(CancelRequest {
                request_id: "cancel-ghost-group".to_string(),
                capture_id: None,
                capture_group_id: Some("grp_does_not_exist".to_string()),
                reason: None,
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::CaptureNotFound,
        "and neither can a capture group that was never accepted"
    );
    runtime.shutdown().await;
}

/// Cancelling work that has already finished reports it UNCHANGED rather than failing.
///
/// A cancel that loses the race to the finish line is not an error: the operator's intent was "make
/// sure this is not running", and it is not. The reply says exactly what happened -- nothing was
/// cancelled, and the work is in the terminal state it reached on its own.
#[tokio::test]
async fn cancelling_finished_work_reports_it_unchanged() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
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
            "finished-capture".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "finished-capture-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected a newly accepted capture, got {other:?}"),
    };
    assert_eq!(
        wait_for_terminal(&runtime, &capture_id).await.state,
        crate::model::JobState::Succeeded
    );
    let cancelled = runtime
        .cancel_capture(CancelRequest {
            request_id: "cancel-finished-capture".to_string(),
            capture_id: Some(capture_id.clone()),
            capture_group_id: None,
            reason: Some("too late".to_string()),
        })
        .await
        .expect("cancelling a finished capture is not an error");
    assert_eq!(cancelled["cancelled"], false);
    assert_eq!(
        cancelled["state"], "SUCCEEDED",
        "the capture keeps the terminal state it reached on its own"
    );

    let group = runtime
        .submit_group(
            group_request("finished-group", &["camera-a", "camera-b"]),
            "finished-group-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .unwrap();
    let terminal = wait_for_group_terminal(&runtime, &group.group_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    let cancelled_group = runtime
        .cancel_capture(CancelRequest {
            request_id: "cancel-finished-group".to_string(),
            capture_id: None,
            capture_group_id: Some(group.group_id.clone()),
            reason: Some("too late".to_string()),
        })
        .await
        .expect("cancelling a finished group is not an error");
    assert_eq!(cancelled_group["cancelledMembers"], 0);
    assert_eq!(
        cancelled_group["unchangedMembers"], 2,
        "every member had already finished, and the reply must account for all of them"
    );
    for member in cancelled_group["members"].as_array().unwrap() {
        assert_eq!(member["cancelled"], false);
        assert_eq!(member["state"], "SUCCEEDED");
    }
    runtime.shutdown().await;
}

/// A cancellation that FAILED is recorded as failed, and the retry replays that -- it does not rerun.
///
/// The ledger settles on the way out of the failure too. Leaving the row IN_PROGRESS would fence it
/// to OUTCOME_UNKNOWN at the next start, and every retry of that `requestId` would then answer
/// PREVIOUS_OUTCOME_UNKNOWN for the life of the state database.
#[tokio::test]
async fn a_cancellation_whose_camera_is_gone_records_its_failure_and_replays_it() {
    let directory = TempDir::new().unwrap();
    let initial = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(initial.clone(), &directory).await;
    runtime
        .catalog
        .accept_job(queued_job(&initial, "cap_cancel_orphan"))
        .await
        .unwrap();

    // The camera leaves the roster. Its durable capture row does not.
    let diff = runtime
        .apply_reloaded_config(
            config(directory.path(), &["camera-a"], false),
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .await
        .expect("removing a camera is a valid reload");
    assert_eq!(diff.removed, vec!["camera-b".to_string()]);

    let request = CancelRequest {
        request_id: "cancel-orphan".to_string(),
        capture_id: Some("cap_cancel_orphan".to_string()),
        capture_group_id: None,
        reason: Some("the camera is gone".to_string()),
    };
    let error = runtime
        .cancel_capture(request.clone())
        .await
        .expect_err("a capture whose camera is no longer configured cannot be cancelled");
    assert_eq!(error.code(), crate::ErrorCode::NoSuchInstance);

    let replayed = runtime
        .cancel_capture(request)
        .await
        .expect("the retry is answered from the ledger rather than run a second time");
    assert_eq!(
        replayed["errorCode"],
        crate::ErrorCode::NoSuchInstance.as_str(),
        "the recorded failure is what the retry gets back"
    );
    assert!(
        replayed["errorMessage"].is_string(),
        "and it carries the operator-safe message the first attempt produced"
    );
    runtime.shutdown().await;
}

/// A fleet-wide drain that includes work in flight clears every camera, once.
///
/// The fleet-wide form has no `instance` to key its ledger by, so it is keyed component-wide; and
/// `includeInFlight` widens the drain from the backlog to every non-terminal capture. A retried
/// drain returns the ORIGINAL outcome rather than reaching for a second wave of work the operator
/// never saw.
#[tokio::test]
async fn a_fleet_wide_drain_including_work_in_flight_is_idempotent() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(
        config(directory.path(), &["camera-a", "camera-b"], false),
        &directory,
    )
    .await;
    // No supervisors: both captures sit exactly where a backlog for an offline camera sits.
    for instance in ["camera-a", "camera-b"] {
        runtime
            .submit_capture(
                instance.to_string(),
                format!("fleet-drain-{instance}"),
                None,
                None,
                serde_json::Map::new(),
                format!("fleet-drain-correlation-{instance}"),
                "sb/capture-submit",
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .unwrap();
    }

    let drain = commands::QueueClearRequest {
        request_id: "fleet-drain".to_string(),
        instance: None,
        all_cameras: true,
        include_in_flight: true,
        reason: None,
    };
    let cleared = runtime
        .queue_clear_command(drain.clone())
        .await
        .expect("a fleet-wide drain must answer");
    assert_eq!(
        cleared["cancelled"],
        serde_json::json!(2),
        "both cameras' captures must be drained, not just the first camera's"
    );
    assert!(
        cleared["failed"].as_array().unwrap().is_empty(),
        "the drain must not leave work behind silently"
    );
    assert_eq!(
        runtime.queue_clear_command(drain).await.unwrap(),
        cleared,
        "a retried fleet-wide drain must replay its original outcome"
    );
    assert_eq!(
        runtime.queue_status(None).await.unwrap().durable_backlog,
        0,
        "and the durable backlog is gone, not merely forgotten"
    );
    runtime.shutdown().await;
}

/// The queue verbs refuse a malformed target, and refuse to drain the fleet without consent.
///
/// The drain cancels durable work an operator has already been promised, so it is deliberately
/// harder to fire by accident than its read-only sibling: it will not run fleet-wide unless the
/// caller says so in as many words.
#[tokio::test]
async fn the_queue_verbs_refuse_a_malformed_or_unconsented_request() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    assert_eq!(
        runtime
            .queue_status_command(commands::QueueStatusRequest {
                instance: Some("not a token!".to_string()),
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::BadArgs,
        "an instance that is not a UNS token is refused before the catalog is touched"
    );
    assert_eq!(
        runtime
            .queue_clear_command(commands::QueueClearRequest {
                request_id: "unconsented-drain".to_string(),
                instance: None,
                all_cameras: false,
                include_in_flight: false,
                reason: None,
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::BadArgs,
        "draining every camera requires the caller to say allCameras=true"
    );
    runtime.shutdown().await;
}

/// A reconnect for a camera with no live session still settles its ledger.
///
/// Reconnect performs no physical actuation that could half-happen, so there is nothing hazardous
/// left in flight to protect. Leaving the row IN_PROGRESS because there was no session to cancel
/// would fence it to OUTCOME_UNKNOWN at the next start -- and every retry of that `requestId` would
/// answer PREVIOUS_OUTCOME_UNKNOWN for the life of the state database.
#[tokio::test]
async fn a_reconnect_for_a_camera_with_no_session_still_settles_its_ledger() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    let operation = runtime
        .reconnect(ReconnectRequest {
            instance: Some("camera-a".to_string()),
            request_id: "reconnect-no-session".to_string(),
            reason: None,
        })
        .await
        .expect("a camera with no session can still be asked to reconnect");
    assert_eq!(operation["state"], "ACCEPTED");
    assert!(operation["operationId"].is_string());

    let canonical = json!({
        "instance": "camera-a",
        "requestId": "reconnect-no-session",
        "reason": serde_json::Value::Null,
    });
    let record = match runtime
        .catalog
        .begin_command(
            crate::catalog::LedgerKey::new(
                "camera-a",
                crate::catalog::RECONNECT_VERB,
                "reconnect-no-session",
            )
            .unwrap(),
            crate::idempotency::canonical_request_hash(&canonical, false).unwrap(),
            canonical,
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        .unwrap()
    {
        crate::catalog::BeginCommandOutcome::Existing(record) => record,
        other => panic!("the reconnect must have left a durable ledger row, got {other:?}"),
    };
    assert_eq!(
        record.state,
        crate::catalog::LedgerState::Succeeded,
        "the reconnect settles its own ledger instead of leaving a row to be fenced at startup"
    );
    assert_eq!(
        record.reply.as_ref(),
        Some(&operation),
        "and a retry replays exactly the operation the first caller was given"
    );
    runtime.shutdown().await;
}

/// A disk probe that parks its FIRST caller until the test lets it go, then answers pressured.
///
/// It exists to hold one group submission inside its capacity check while another one accepts the
/// group underneath it -- the interleaving that decides whether pressure can erase work that is
/// already durable.
struct GatedSpaceProbe {
    calls: AtomicUsize,
    entered: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<Semaphore>,
    pressured: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait::async_trait]
impl crate::admission::DiskSpaceProbe for GatedSpaceProbe {
    async fn space(&self, _path: &std::path::Path) -> Result<crate::admission::DiskSpace> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            self.entered
                .store(true, std::sync::atomic::Ordering::Release);
            let permit = self
                .release
                .acquire()
                .await
                .expect("the gate is never closed");
            permit.forget();
        }
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

/// Storage pressure cannot un-accept a group that was accepted while the disk was being checked.
///
/// The capacity check and the durable insert are not one step. A submission held inside its check
/// while another caller accepts the very same group must be folded onto that group when it wakes --
/// not answered STORAGE_PRESSURE for work the component has already promised. The ledger is
/// therefore consulted a second time before the rejection is returned, and a caller waiting for the
/// result is attached to the group it was told about.
#[tokio::test]
async fn storage_pressure_cannot_erase_a_group_that_was_accepted_underneath_it() {
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(Semaphore::new(0));
    let pressured = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let monitor = StoragePressureMonitor::new(
        configuration.global.output.root_directory.clone(),
        directory.path().join("state"),
        &configuration.global.output,
        Arc::new(GatedSpaceProbe {
            calls: AtomicUsize::new(0),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            pressured: Arc::clone(&pressured),
        }),
    );
    let runtime = runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    // The held caller: it will be parked inside its capacity check, past the point where it looked
    // for an existing group and found none.
    let token = deferred
        .defer(
            &command_message(
                "sb/capture-group",
                "raced-pressure",
                json!({ "requestId": "raced-pressure", "instances": ["camera-a", "camera-b"] }),
            ),
            Duration::from_secs(30),
        )
        .expect("the fixture message carries a reply topic");
    token.activate().expect("the command inbox is running");
    let held = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            runtime
                .submit_group(
                    group_request("raced-pressure", &["camera-a", "camera-b"]),
                    "raced-pressure-held".to_string(),
                    crate::admission::CapturePriority::Direct,
                    Some(token),
                )
                .await
        })
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !entered.load(std::sync::atomic::Ordering::Acquire) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first submission never reached its storage check"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // While it is held, the same group is accepted by another caller, and the disk fills up.
    let accepted = runtime
        .submit_group(
            group_request("raced-pressure", &["camera-a", "camera-b"]),
            "raced-pressure-winner".to_string(),
            crate::admission::CapturePriority::Direct,
            None,
        )
        .await
        .expect("the second caller sees a healthy disk and accepts the group");
    pressured.store(true, std::sync::atomic::Ordering::Release);
    release.add_permits(1);

    let woken = held
        .await
        .expect("the held caller must not panic")
        .expect("a group that is already durable must not be un-accepted by pressure");
    assert_eq!(
        woken.group_id, accepted.group_id,
        "the held caller must be folded onto the group that was accepted underneath it"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "and no second set of member captures may have been created"
    );

    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }
    let terminal = wait_for_group_terminal(&runtime, &accepted.group_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    let reply = wait_for_recorded_reply(&publishes, 0).await;
    assert_eq!(
        reply.body["result"]["captureGroupId"], accepted.group_id,
        "the caller that was held is still attached to the group, and is answered with its result"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// Builds the gated-probe fixture: a runtime whose disk check can be held open on demand.
fn gated_pressure_monitor(
    configuration: &AdapterConfig,
    directory: &TempDir,
    entered: &Arc<std::sync::atomic::AtomicBool>,
    release: &Arc<Semaphore>,
    pressured: &Arc<std::sync::atomic::AtomicBool>,
) -> StoragePressureMonitor {
    StoragePressureMonitor::new(
        configuration.global.output.root_directory.clone(),
        directory.path().join("state"),
        &configuration.global.output,
        Arc::new(GatedSpaceProbe {
            calls: AtomicUsize::new(0),
            entered: Arc::clone(entered),
            release: Arc::clone(release),
            pressured: Arc::clone(pressured),
        }),
    )
}

/// Waits until the held submission is parked inside its storage check.
async fn wait_for_gate(entered: &Arc<std::sync::atomic::AtomicBool>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !entered.load(std::sync::atomic::Ordering::Acquire) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the held submission never reached its storage check"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A caller held through pressure onto a group that has already FINISHED is answered at once.
///
/// It cannot be attached to the group: the fan-out that settles a group's waiters has already run.
/// The reply therefore comes from the durable result -- the same answer the original caller got --
/// rather than a STORAGE_PRESSURE rejection of work that has already been done and delivered.
#[tokio::test]
async fn storage_pressure_cannot_turn_a_finished_group_into_a_rejection() {
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(Semaphore::new(0));
    let pressured = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let monitor =
        gated_pressure_monitor(&configuration, &directory, &entered, &release, &pressured);
    let runtime = runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    let token = deferred
        .defer(
            &command_message(
                "sb/capture-group",
                "raced-finished",
                json!({ "requestId": "raced-finished", "instances": ["camera-a", "camera-b"] }),
            ),
            Duration::from_secs(30),
        )
        .expect("the fixture message carries a reply topic");
    token.activate().expect("the command inbox is running");
    let held = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            runtime
                .submit_group(
                    group_request("raced-finished", &["camera-a", "camera-b"]),
                    "raced-finished-held".to_string(),
                    crate::admission::CapturePriority::Direct,
                    Some(token),
                )
                .await
        })
    };
    wait_for_gate(&entered).await;

    // The group is accepted, runs, and finishes -- all while the retry is still inside its check.
    let accepted = runtime
        .submit_group(
            group_request("raced-finished", &["camera-a", "camera-b"]),
            "raced-finished-winner".to_string(),
            crate::admission::CapturePriority::Direct,
            None,
        )
        .await
        .unwrap();
    let terminal = wait_for_group_terminal(&runtime, &accepted.group_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    pressured.store(true, std::sync::atomic::Ordering::Release);
    release.add_permits(1);

    let woken = held
        .await
        .expect("the held caller must not panic")
        .expect("a finished group must not be reported as a storage rejection");
    assert_eq!(woken.group_id, accepted.group_id);
    let reply = wait_for_recorded_reply(&publishes, 0).await;
    assert_eq!(reply.body["ok"], true);
    assert_eq!(
        reply.body["result"]["captureGroupId"], accepted.group_id,
        "the held caller is settled from the finished group's durable result, not left waiting on \
         a fan-out that has already happened"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// Pressure does not soften an idempotency conflict: a reused group key still conflicts.
///
/// The second look at the ledger exists to protect work that is already durable -- not to wave
/// through a request that describes DIFFERENT work under the same key. A caller that reused the key
/// with changed arguments is told so, whatever the disk is doing.
#[tokio::test]
async fn storage_pressure_still_conflicts_on_a_group_key_reused_with_changed_arguments() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(Semaphore::new(0));
    let pressured = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let monitor =
        gated_pressure_monitor(&configuration, &directory, &entered, &release, &pressured);
    let runtime = runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;

    let mut changed = group_request("raced-conflict", &["camera-a", "camera-b"]);
    changed.timeout_ms = Some(30_000);
    let held = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            runtime
                .submit_group(
                    changed,
                    "raced-conflict-held".to_string(),
                    crate::admission::CapturePriority::Direct,
                    None,
                )
                .await
        })
    };
    wait_for_gate(&entered).await;

    let accepted = runtime
        .submit_group(
            group_request("raced-conflict", &["camera-a", "camera-b"]),
            "raced-conflict-winner".to_string(),
            crate::admission::CapturePriority::Direct,
            None,
        )
        .await
        .expect("the second caller accepts the group with ITS arguments");
    pressured.store(true, std::sync::atomic::Ordering::Release);
    release.add_permits(1);

    let error = held
        .await
        .expect("the held caller must not panic")
        .expect_err("a reused key describing different work is never served from the ledger");
    assert_eq!(
        error.code(),
        crate::ErrorCode::IdempotencyConflict,
        "pressure must not turn a conflicting retry into an acceptance of somebody else's group"
    );
    assert_eq!(
        group_for(&runtime, "raced-conflict")
            .await
            .expect("the accepted group is still there")
            .group_id,
        accepted.group_id,
        "and the group that WAS accepted is left untouched by the conflicting retry"
    );
    runtime.shutdown().await;
}

/// A command still in flight answers its own duplicate from the ledger instead of actuating twice.
///
/// The ledger row is written BEFORE the camera is touched, precisely so that a client retrying
/// while the first attempt is still running cannot start a second physical movement. The duplicate
/// is told the command is in hand; it does not get a second move, a second preset write, or a
/// second reconnect.
#[tokio::test]
async fn a_command_still_in_flight_answers_its_duplicate_from_the_ledger() {
    let directory = TempDir::new().unwrap();
    let mut configuration = ptz_camera(&directory, 10_000);
    configuration.instances[0].ptz.allow_preset_mutation = true;
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.ptz.presets_supported = true;
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    // Exactly the rows the three commands write on their way to the camera, and no further.
    for (key, canonical) in [
        (
            crate::catalog::LedgerKey::new("camera-a", "sb/ptz/home", "in-flight-ptz").unwrap(),
            json!({
                "instance": "camera-a",
                "requestId": "in-flight-ptz",
                "operation": "home",
                "arguments": {},
            }),
        ),
        (
            crate::catalog::LedgerKey::new("camera-a", "sb/ptz-presets/set", "in-flight-preset")
                .unwrap(),
            json!({
                "instance": "camera-a",
                "requestId": "in-flight-preset",
                "operation": "set",
                "arguments": { "name": "in-flight-name" },
            }),
        ),
        (
            crate::catalog::LedgerKey::new(
                "camera-a",
                crate::catalog::RECONNECT_VERB,
                "in-flight-reconnect",
            )
            .unwrap(),
            json!({
                "instance": "camera-a",
                "requestId": "in-flight-reconnect",
                "reason": serde_json::Value::Null,
            }),
        ),
    ] {
        assert!(
            matches!(
                runtime
                    .catalog
                    .begin_command(
                        key,
                        crate::idempotency::canonical_request_hash(&canonical, false).unwrap(),
                        canonical,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await
                    .unwrap(),
                crate::catalog::BeginCommandOutcome::Started(_)
            ),
            "each command must be left in flight, with no outcome recorded yet"
        );
    }

    let ptz: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "home",
        "instance": "camera-a",
        "requestId": "in-flight-ptz"
    }))
    .unwrap();
    let ptz_reply = runtime
        .perform_ptz(ptz)
        .await
        .expect("a duplicate of a command in flight is answered, not refused");
    assert_eq!(ptz_reply["operation"], "home");
    assert_eq!(
        ptz_reply["state"], "COMMANDED",
        "the duplicate is told the move is in hand rather than being sent at the camera again"
    );

    let preset: PtzPresetsRequest = commands::parse_closed(json!({
        "operation": "set",
        "instance": "camera-a",
        "requestId": "in-flight-preset",
        "name": "in-flight-name"
    }))
    .unwrap();
    let preset_reply = runtime
        .perform_presets(preset)
        .await
        .expect("a duplicate preset write is answered from the ledger");
    assert_eq!(preset_reply["operation"], "set");
    assert_eq!(preset_reply["state"], "COMMANDED");
    assert!(
        preset_reply.get("token").is_none(),
        "and it must not invent a token the camera has not issued"
    );

    let reconnect = runtime
        .reconnect(ReconnectRequest {
            instance: Some("camera-a".to_string()),
            request_id: "in-flight-reconnect".to_string(),
            reason: None,
        })
        .await
        .expect("a duplicate reconnect is answered from the ledger");
    assert_eq!(reconnect["instance"], "camera-a");
    assert_eq!(
        reconnect["state"], "ACCEPTED",
        "the duplicate reconnect reports the operation already in hand"
    );
    assert_eq!(
        reconnect["operationId"], "op_in-flight-reconnect",
        "and it is the ORIGINAL request's operation, derived from its durable key"
    );

    // The same reconnect, once a crash has cost us its outcome, is a different answer entirely.
    runtime
        .catalog
        .mark_hazardous_commands_outcome_unknown(chrono::Utc::now().timestamp_millis())
        .await
        .unwrap();
    assert_eq!(
        runtime
            .reconnect(ReconnectRequest {
                instance: Some("camera-a".to_string()),
                request_id: "in-flight-reconnect".to_string(),
                reason: None,
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::PreviousOutcomeUnknown,
        "a reconnect whose outcome was lost must be admitted as unknown, not replayed as accepted"
    );
    runtime.shutdown().await;
}

/// A hazardous command whose outcome was lost to a crash is never silently retried.
///
/// PTZ, presets and cancellation all actuate something. A row left IN_PROGRESS by a crash is fenced
/// to OUTCOME_UNKNOWN at the next start, and the honest answer to a retry of that `requestId` is
/// PREVIOUS_OUTCOME_UNKNOWN: the component genuinely does not know whether the camera moved, and
/// quietly commanding it a second time is exactly the thing the ledger exists to prevent.
#[tokio::test]
async fn a_hazardous_command_whose_outcome_was_lost_is_never_silently_retried() {
    let directory = TempDir::new().unwrap();
    let mut configuration = ptz_camera(&directory, 10_000);
    configuration.instances[0].ptz.allow_preset_mutation = true;
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.ptz.presets_supported = true;
    let runtime = runtime(configuration, &directory).await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "fenced-target".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "fenced-target-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected a newly accepted capture, got {other:?}"),
    };

    // Exactly the rows the three commands would have written before the process died.
    let started = [
        (
            crate::catalog::LedgerKey::new("camera-a", "sb/ptz/absolute", "fenced-ptz").unwrap(),
            json!({
                "instance": "camera-a",
                "requestId": "fenced-ptz",
                "operation": "absolute",
                "arguments": {
                    "position": { "pan": 0.2, "tilt": -0.1, "zoom": 0.3 },
                    "speed": serde_json::Value::Null,
                },
            }),
        ),
        (
            crate::catalog::LedgerKey::new("camera-a", "sb/ptz-presets/set", "fenced-preset")
                .unwrap(),
            json!({
                "instance": "camera-a",
                "requestId": "fenced-preset",
                "operation": "set",
                "arguments": { "name": "fenced-name" },
            }),
        ),
        (
            crate::catalog::LedgerKey::new("camera-a", "sb/capture-cancel", "fenced-cancel")
                .unwrap(),
            json!({
                "requestId": "fenced-cancel",
                "target": { "kind": "capture", "captureId": &capture_id },
                "reason": "the process died mid-cancel",
            }),
        ),
    ];
    for (key, canonical) in started {
        assert!(
            matches!(
                runtime
                    .catalog
                    .begin_command(
                        key,
                        crate::idempotency::canonical_request_hash(&canonical, false).unwrap(),
                        canonical,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await
                    .unwrap(),
                crate::catalog::BeginCommandOutcome::Started(_)
            ),
            "each command must be left mid-flight, as a crash would leave it"
        );
    }

    // The restart fences every hazardous row it cannot vouch for.
    runtime.recover_install_owned().await.unwrap();
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let ptz: PtzCommandRequest = commands::parse_closed(json!({
        "operation": "absolute",
        "instance": "camera-a",
        "requestId": "fenced-ptz",
        "position": { "pan": 0.2, "tilt": -0.1, "zoom": 0.3 }
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_ptz(ptz).await.unwrap_err().code(),
        crate::ErrorCode::PreviousOutcomeUnknown,
        "the component does not know whether the camera already moved, and must say so"
    );

    let preset: PtzPresetsRequest = commands::parse_closed(json!({
        "operation": "set",
        "instance": "camera-a",
        "requestId": "fenced-preset",
        "name": "fenced-name"
    }))
    .unwrap();
    assert_eq!(
        runtime.perform_presets(preset).await.unwrap_err().code(),
        crate::ErrorCode::PreviousOutcomeUnknown,
        "nor whether the preset was already written"
    );

    assert_eq!(
        runtime
            .cancel_capture(CancelRequest {
                request_id: "fenced-cancel".to_string(),
                capture_id: Some(capture_id),
                capture_group_id: None,
                reason: Some("the process died mid-cancel".to_string()),
            })
            .await
            .unwrap_err()
            .code(),
        crate::ErrorCode::PreviousOutcomeUnknown,
        "nor whether the cancellation had already taken effect"
    );
    runtime.shutdown().await;
}

/// A drain reports the work it could NOT cancel instead of claiming a clean sweep.
///
/// The break-glass drain is reached for when the backlog has run away, so it pages -- and a page
/// whose every row resists cancellation would be fetched again, and fail again, forever. It stops
/// and says which captures it could not touch.
#[tokio::test]
async fn a_drain_reports_the_work_it_could_not_cancel_rather_than_spinning_on_it() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a"], false);
    let profile = configuration.instances[0]
        .capture_profiles
        .get("main")
        .cloned()
        .expect("the fixture configures a main profile");
    let runtime = runtime(configuration.clone(), &directory).await;

    // A durable capture for a camera this runtime has no engine for: the row a roster change
    // leaves behind, and the one a drain cannot do anything about.
    let now = chrono::Utc::now().timestamp_millis();
    let canonical = json!({ "requestId": "orphan-drain", "metadata": {} });
    runtime
        .catalog
        .accept_job(crate::catalog::NewJob {
            capture_id: "cap_orphan_drain".to_string(),
            instance: "camera-gone".to_string(),
            ledger_key: Some(
                crate::catalog::LedgerKey::new("camera-gone", "sb/capture", "orphan-drain")
                    .unwrap(),
            ),
            request_hash: crate::idempotency::canonical_request_hash(&canonical, false).unwrap(),
            canonical_request: canonical,
            effective_profile: serde_json::to_value(crate::jobs::JobProfileSnapshot {
                name: "main".to_string(),
                capture: profile,
                offline_policy: crate::config::OfflinePolicy::WaitUntilDeadline,
                maximum_frame_bytes: configuration.global.limits.max_frame_bytes_per_camera,
                capture_mode: crate::model::CaptureMode::Simulated,
                capture_interlock: configuration.instances[0].ptz.capture_interlock,
                settle_ms: configuration.instances[0].ptz.settle_ms,
            })
            .unwrap(),
            deadlines: crate::catalog::JobDeadlines {
                terminal_at_ms: now + 600_000,
                queue_at_ms: None,
                capture_at_ms: now + 300_000,
                encode_at_ms: now + 300_000,
                persist_at_ms: now + 300_000,
            },
            trigger: serde_json::to_value(crate::messages::CaptureTrigger::Command {
                request_id: "orphan-drain".to_string(),
            })
            .unwrap(),
            origin_correlation_id: Some("orphan-drain-correlation".to_string()),
            intended_output: json!({ "relativePath": "camera-gone/cap.jpg", "backend": "sim" }),
            accepted_at_ms: now,
            group_id: None,
        })
        .await
        .unwrap();

    let outcome = runtime
        .clear_queue(None, false, "operator drain".to_string())
        .await
        .expect("the drain answers even when it cannot cancel everything");
    assert_eq!(
        outcome.cancelled, 0,
        "there was nothing the drain could actually cancel"
    );
    assert_eq!(
        outcome.failed.len(),
        1,
        "and the capture it could not touch must be reported, not silently dropped"
    );
    assert_eq!(outcome.failed[0].capture_id, "cap_orphan_drain");
    assert!(
        !outcome.failed[0].error.is_empty(),
        "the operator is told WHY that capture resisted the drain"
    );
    assert_eq!(
        runtime
            .catalog
            .job("cap_orphan_drain")
            .await
            .unwrap()
            .unwrap()
            .state,
        crate::model::JobState::Accepted,
        "and the row is left exactly as it was, rather than being pretended terminal"
    );
    runtime.shutdown().await;
}

/// A camera that is down says WHY on the connectivity surface the heartbeat publishes.
///
/// `connected: false` is the flag every consumer can act on, but an operator deciding whether to
/// intervene needs the richer condition too: the state token, the camera's own message, and the
/// stable code of the error that put it there. All three ride the same element.
#[tokio::test]
async fn a_camera_that_is_down_reports_why_on_the_connectivity_surface() {
    let directory = TempDir::new().unwrap();
    let mut raw = core_config_value(directory.path(), &["camera-a"], false);
    // A GenICam camera with the native feature absent: it cannot connect, and it knows why.
    raw["component"]["instances"][0]["backend"] = json!({
        "type": "genicam-aravis",
        "selector": { "serial": "connectivity-surface-test" }
    });
    let configuration = AdapterConfig::from_core_reload(
        &Config::from_value(COMPONENT_NAME, "gw-01", raw).unwrap(),
    )
    .unwrap();
    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let samples = runtime.camera_connectivity();
        let sample = samples
            .first()
            .expect("the connectivity surface must carry the configured camera");
        if sample.state.as_deref() == Some("BACKOFF") {
            assert!(
                !sample.connected,
                "a camera in BACKOFF is not connected, and the normalized flag must say so"
            );
            assert!(
                sample
                    .detail
                    .as_deref()
                    .is_some_and(|detail| !detail.is_empty()),
                "a camera that is down must carry the reason it gave"
            );
            // WHICH code depends on how the binary was built, and that is the point of asserting the
            // shape rather than the string: without the `genicam` feature the backend does not exist at
            // all (UNSUPPORTED_CAPABILITY), and with it the backend exists and rejects the selector
            // (INVALID_REQUEST). Pinning one of them makes the test pass on Windows and fail in the
            // Aravis container, which is exactly what it did.
            let code = sample
                .attributes
                .get("lastErrorCode")
                .and_then(serde_json::Value::as_str);
            assert!(
                code == Some(crate::ErrorCode::UnsupportedCapability.as_str())
                    || code == Some(crate::ErrorCode::BadArgs.as_str()),
                "a camera that is down must carry a stable error code an operator can act on without                  reading prose; got {code:?}"
            );
            assert!(
                sample.attributes.contains_key("backend"),
                "the backend belongs on the element too: only a camera adapter understands it"
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a camera that cannot connect must reach BACKOFF on the connectivity surface"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    runtime.shutdown().await;
}

/// A queue-policy profile gives its capture a durable QUEUE deadline of its own.
///
/// `offlinePolicy: queue` means the capture may wait for a camera that is not there, and
/// `queueExpiryMs` is what stops that wait being forever. The deadline has to be durable, because
/// the wait is expected to outlive the process that accepted it.
#[tokio::test]
async fn a_queue_policy_profile_gives_its_capture_a_durable_queue_deadline() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let profile = configuration.instances[0]
        .capture_profiles
        .get_mut("main")
        .expect("the fixture configures a main profile");
    profile.offline_policy = Some(crate::config::OfflinePolicy::Queue);
    // Inside the terminal deadline: a capture may not be allowed to sit in the queue for longer
    // than it is allowed to live, and the durable layer refuses a job whose stage deadlines run
    // past its terminal one.
    profile.queue_expiry_ms = Some(20_000);
    let runtime = runtime(configuration, &directory).await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "queue-expiry".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "queue-expiry-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let record = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record,
        other => panic!("expected a newly accepted capture, got {other:?}"),
    };
    let queue_at_ms = record
        .deadlines
        .queue_at_ms
        .expect("a queue-policy capture must carry a durable queue deadline");
    assert!(
        queue_at_ms > record.accepted_at_ms,
        "the queue deadline is measured from acceptance and must lie ahead of it"
    );
    assert!(
        queue_at_ms <= record.deadlines.terminal_at_ms,
        "and it must not outlive the capture's own terminal deadline"
    );
    runtime.shutdown().await;
}

/// A discovery cursor that belongs to no retained snapshot is refused.
///
/// A continuation is a view of the ORIGINAL result, never a second probe. A cursor the component
/// cannot resolve to a retained snapshot must be refused rather than quietly answered with a fresh
/// scan the caller never asked for -- the pages would not line up, and the caller would never know.
#[tokio::test]
async fn a_discovery_cursor_that_belongs_to_no_snapshot_is_refused() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.discovery.enabled = true;
    let runtime = runtime(configuration, &directory).await;

    let error = runtime
        .discover(DiscoverRequest {
            backends: Vec::new(),
            timeout_ms: 100,
            limit: 10,
            cursor: Some("this-cursor-was-never-issued".to_string()),
        })
        .await
        .expect_err("a cursor with no snapshot behind it cannot be paged");
    assert_eq!(
        error.code(),
        crate::ErrorCode::BadArgs,
        "an unresolvable cursor is refused, not silently turned into a new discovery pass"
    );
    runtime.shutdown().await;
}

/// Concurrent identical group submissions accept exactly ONE group, and answer every caller.
///
/// The idempotency check and the durable insert are not one atomic step, so two callers can both
/// find no group and both go on to accept one. Only one insert can win, and the losers must be
/// folded onto the winner's group -- not left holding a second group, a second set of member
/// captures, or a reply that never comes.
#[tokio::test]
async fn concurrent_identical_group_submissions_accept_exactly_one_group() {
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    let runtime = runtime(configuration, &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    let callers = 4;
    let mut submissions = Vec::with_capacity(callers);
    for index in 0..callers {
        let token = deferred
            .defer(
                &command_message(
                    "sb/capture-group",
                    &format!("racing-caller-{index}"),
                    json!({ "requestId": "racing-group", "instances": ["camera-a", "camera-b"] }),
                ),
                Duration::from_secs(30),
            )
            .expect("the fixture message carries a reply topic");
        token.activate().expect("the command inbox is running");
        let runtime = Arc::clone(&runtime);
        submissions.push(tokio::spawn(async move {
            runtime
                .submit_group(
                    group_request("racing-group", &["camera-a", "camera-b"]),
                    format!("racing-correlation-{index}"),
                    crate::admission::CapturePriority::Direct,
                    Some(token),
                )
                .await
        }));
    }
    let mut group_ids = Vec::with_capacity(callers);
    for submission in submissions {
        let group = submission
            .await
            .expect("no caller may panic")
            .expect("every concurrent caller must be given a group");
        group_ids.push(group.group_id);
    }
    assert!(
        group_ids.windows(2).all(|pair| pair[0] == pair[1]),
        "every concurrent caller must be folded onto the SAME group, not handed one each: {group_ids:?}"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "and the group's camera must hold exactly one member capture, not one per caller"
    );

    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }
    let terminal = wait_for_group_terminal(&runtime, &group_ids[0]).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    wait_for_recorded_replies(&publishes, callers).await;
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// Discovery that is switched off says so, rather than answering with an empty fleet.
///
/// "No cameras found" and "discovery is disabled" are very different answers to an operator hunting
/// for a camera that will not appear.
#[tokio::test]
async fn discovery_is_refused_when_it_is_disabled_by_configuration() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    let error = runtime
        .discover(DiscoverRequest {
            backends: Vec::new(),
            timeout_ms: 100,
            limit: 10,
            cursor: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        crate::ErrorCode::UnsupportedCapability,
        "disabled discovery must be reported as disabled, not as an empty result"
    );
    runtime.shutdown().await;
}

/// Discovery probes each configured protocol backend, and reports what that backend says.
///
/// The simulator is never probed -- there is nothing on the network to find -- which is why a
/// sim-only fleet discovers nothing. A real protocol backend IS probed, and here it refuses for want
/// of an eligible interface. That refusal is the backend's, and it must reach the caller instead of
/// being flattened into an empty candidate list that looks like "nothing out there".
#[tokio::test]
async fn discovery_probes_a_protocol_backend_and_reports_its_refusal() {
    let directory = TempDir::new().unwrap();
    let core = Config::from_value(
        COMPONENT_NAME,
        "gw-01",
        json!({
            "component": {
                "global": {
                    "output": { "rootDirectory": directory.path().to_string_lossy() },
                },
                "instances": [{
                    "id": "camera-a",
                    "backend": {
                        "type": "onvif-rtsp",
                        "mediaProfile": "main",
                        "deviceServiceUrl": "https://127.0.0.1:65535/onvif/device_service",
                    },
                    "defaultCaptureProfile": "main",
                    "captureProfiles": { "main": { "output": { "encoding": "jpeg" } } }
                }],
            }
        }),
    )
    .unwrap();
    let mut configuration = AdapterConfig::from_core_reload(&core).unwrap();
    // Discovery is on, with no eligible interface to probe from: exactly the state an operator
    // reaches by enabling discovery and leaving `discovery.eligibleInterfaces` empty.
    configuration.global.discovery.enabled = true;
    let runtime = runtime(configuration, &directory).await;

    let error = runtime
        .discover(DiscoverRequest {
            backends: Vec::new(),
            timeout_ms: 100,
            limit: 10,
            cursor: None,
        })
        .await
        .expect_err("the ONVIF backend cannot probe without an eligible interface");
    assert_eq!(
        error.code(),
        crate::ErrorCode::UnsupportedCapability,
        "the backend's own refusal must reach the caller, not be flattened into an empty page"
    );

    // Naming the backend explicitly selects the same probe; it is the caller narrowing the scan,
    // not a different discovery.
    let selected = runtime
        .discover(DiscoverRequest {
            backends: vec![crate::model::BackendKind::OnvifRtsp],
            timeout_ms: 100,
            limit: 10,
            cursor: None,
        })
        .await
        .expect_err("naming the ONVIF backend probes it, and it still cannot");
    assert_eq!(selected.code(), crate::ErrorCode::UnsupportedCapability);
    runtime.shutdown().await;
}

/// The immediate command error a verb answered with, or a panic saying what it answered instead.
fn immediate_error(outcome: CommandOutcome) -> CommandError {
    match outcome {
        CommandOutcome::ImmediateError(error) => error,
        other => panic!("expected an immediate command error, got {other:?}"),
    }
}

/// A roster that mixes the simulator with the two protocol backends, with discovery reporting on.
///
/// The two ONVIF cameras are configured by DIFFERENT stable selectors -- one by its device-service
/// URL, one by its endpoint reference -- because those are the two ways a candidate can already be
/// claimed, and a component that recognises only one of them offers the operator a camera it is
/// already running.
fn mixed_backend_roster(directory: &TempDir) -> AdapterConfig {
    let profile = json!({ "main": { "output": { "encoding": "jpeg" } } });
    let core = Config::from_value(
        COMPONENT_NAME,
        "gw-01",
        json!({
            "component": {
                "global": {
                    "output": { "rootDirectory": directory.path().to_string_lossy() },
                    "discovery": {
                        "enabled": true,
                        "reportUnconfigured": true,
                        "eligibleInterfaces": ["eth0"],
                    },
                },
                "instances": [
                    {
                        "id": "camera-a",
                        "backend": { "type": "sim" },
                        "defaultCaptureProfile": "main",
                        "captureProfiles": profile,
                    },
                    {
                        "id": "camera-b",
                        "backend": { "type": "sim" },
                        "defaultCaptureProfile": "main",
                        "captureProfiles": profile,
                    },
                    {
                        "id": "camera-url",
                        "backend": {
                            "type": "onvif-rtsp",
                            "mediaProfile": "main",
                            "deviceServiceUrl": "https://127.0.0.1:65535/onvif/device_service",
                        },
                        "defaultCaptureProfile": "main",
                        "captureProfiles": profile,
                    },
                    {
                        "id": "camera-ref",
                        "backend": {
                            "type": "onvif-rtsp",
                            "mediaProfile": "main",
                            "selector": { "endpointReference": "urn:uuid:known-camera" },
                        },
                        "defaultCaptureProfile": "main",
                        "captureProfiles": profile,
                    },
                ],
            }
        }),
    )
    .expect("the mixed-backend fixture must be structurally valid");
    AdapterConfig::from_core_reload(&core).expect("the mixed-backend fixture must validate")
}

/// A retained ONVIF discovery observation with the given stable selector.
fn onvif_candidate(selector: serde_json::Value) -> DiscoveryCandidate {
    DiscoveryCandidate {
        backend: crate::model::BackendKind::OnvifRtsp,
        selector,
        vendor: Some("ACME".to_string()),
        model: Some("XZ-1".to_string()),
        capabilities: json!({}),
    }
}

/// `sb/list` pages its roster, carries capabilities only when asked, and offers only the cameras
/// nothing already claims.
///
/// Three things are being pinned here, and each of them is a way an operator gets a wrong answer:
/// the capability view is opt-in (a fleet-wide `sb/list` must not pay for every camera's full
/// snapshot); a continuation is a view of the SAME retained result, so the cameras and the
/// unconfigured observations page as one list rather than two; and a discovery observation that
/// matches a camera this component is already running is not offered as something to add. That last
/// one is the reason the ONVIF selector arms exist: a camera configured by its device-service URL
/// and a camera configured by its endpoint reference are both already claimed, and either one being
/// re-offered invites an operator to configure the same physical camera twice.
#[tokio::test]
async fn the_list_verb_pages_capabilities_on_demand_and_offers_only_unclaimed_discoveries() {
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let runtime = runtime(mixed_backend_roster(&directory), &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;
    runtime.discovery_cache.lock().unwrap().candidates = vec![
        onvif_candidate(json!({ "endpointReference": "urn:uuid:known-camera" })),
        onvif_candidate(
            json!({ "deviceServiceUrl": "https://127.0.0.1:65535/onvif/device_service" }),
        ),
        onvif_candidate(json!({ "endpointReference": "urn:uuid:brand-new" })),
    ];

    let first = immediate_success(
        runtime
            .handle_camera_command(
                "sb/list",
                command_message(
                    "sb/list",
                    "list-page-one",
                    json!({ "includeCapabilities": true, "includeUnconfigured": true, "limit": 3 }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        first["cameras"].as_array().map(Vec::len),
        Some(3),
        "the first page must hold exactly the requested number of cameras"
    );
    assert!(
        first["cameras"][0].get("capabilities").is_some(),
        "includeCapabilities must produce the full camera snapshot, not the compact view"
    );
    assert_eq!(
        first["unconfigured"].as_array().map(Vec::len),
        Some(0),
        "unconfigured observations come after the configured roster, not interleaved with it"
    );
    let cursor = first["nextCursor"]
        .as_str()
        .expect("a fourth camera and an unconfigured observation must remain")
        .to_string();

    let second = immediate_success(
        runtime
            .handle_camera_command(
                "sb/list",
                command_message(
                    "sb/list",
                    "list-page-two",
                    json!({
                        "includeCapabilities": true,
                        "includeUnconfigured": true,
                        "limit": 3,
                        "cursor": cursor,
                    }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        second["cameras"].as_array().map(Vec::len),
        Some(1),
        "the continuation must resume the SAME retained result, not restart it"
    );
    let unconfigured = second["unconfigured"]
        .as_array()
        .expect("the second page carries the unconfigured tail");
    assert_eq!(
        unconfigured.len(),
        1,
        "only the observation no configured camera claims may be offered: {unconfigured:?}"
    );
    assert_eq!(
        unconfigured[0]["selector"]["endpointReference"], "urn:uuid:brand-new",
        "the cameras claimed by endpoint reference and by device-service URL are already running"
    );
    assert!(
        second["nextCursor"].is_null(),
        "the retained result is exhausted, so there is nothing left to continue"
    );

    let compact = immediate_success(
        runtime
            .handle_camera_command(
                "sb/list",
                command_message("sb/list", "list-compact", json!({ "limit": 10 })),
                deferred.clone(),
            )
            .await,
    );
    assert!(
        compact["cameras"][0].get("capabilities").is_none(),
        "the default view is the compact one: capabilities are paid for only when asked for"
    );
    assert_eq!(
        compact["unconfigured"].as_array().map(Vec::len),
        Some(0),
        "and unconfigured observations are not volunteered to a caller that did not ask"
    );

    let stale = immediate_error(
        runtime
            .handle_camera_command(
                "sb/list",
                command_message(
                    "sb/list",
                    "list-stale-cursor",
                    json!({
                        "includeCapabilities": true,
                        "includeUnconfigured": true,
                        "limit": 3,
                        "cursor": "cur_never-issued",
                    }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        stale.code,
        crate::ErrorCode::BadArgs.as_str(),
        "a cursor that resolves to no retained result is refused, not answered from the head"
    );
    assert_eq!(
        runtime.registry().snapshots(10).unwrap().len(),
        4,
        "the verb answers from the component's own roster, which the runtime exposes"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// `sb/status` with no camera named answers for the whole fleet rather than refusing.
///
/// Naming a camera is how an operator asks about ONE camera. Omitting it is the fleet question, and
/// it has to be answerable without the operator already knowing every instance id -- which is
/// exactly what they are asking to find out.
#[tokio::test]
async fn the_status_verb_answers_for_the_whole_fleet_when_no_camera_is_named() {
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let runtime = runtime(
        config(directory.path(), &["camera-a", "camera-b"], false),
        &directory,
    )
    .await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    let fleet = immediate_success(
        runtime
            .handle_camera_command(
                "sb/status",
                command_message("sb/status", "status-fleet", json!({})),
                deferred.clone(),
            )
            .await,
    );
    let cameras = fleet["cameras"]
        .as_array()
        .expect("the fleet answer is a camera array");
    assert_eq!(
        cameras.len(),
        2,
        "every configured camera is reported, connected or not: {cameras:?}"
    );

    let one = immediate_success(
        runtime
            .handle_camera_command(
                "sb/status",
                command_message(
                    "sb/status",
                    "status-one",
                    json!({ "instance": "camera-b" }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        one["instance"], "camera-b",
        "and naming a camera answers for that camera alone"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// `sb/capture-submit` refuses a `requestId` reused with different arguments, and captures nothing.
///
/// A submitted capture is idempotent on its `requestId`: a retry must return the capture the caller
/// was already given. That promise is only worth anything if the component also REFUSES to reuse the
/// key for different work -- otherwise a caller that changed its mind about the profile silently
/// receives the old capture and never learns the new request was dropped.
#[tokio::test]
async fn the_capture_submit_verb_refuses_a_request_id_reused_with_new_arguments() {
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;

    let accepted = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-submit",
                command_message(
                    "sb/capture-submit",
                    "submit-first",
                    json!({ "requestId": "submit-reused", "instance": "camera-a" }),
                ),
                deferred.clone(),
            )
            .await,
    );
    let capture_id = accepted["captureId"]
        .as_str()
        .expect("an accepted submission answers with its capture id")
        .to_string();
    assert_eq!(
        accepted["statusVerb"], "sb/capture-status",
        "the caller is told where to ask for the outcome it did not wait for"
    );

    let conflict = immediate_error(
        runtime
            .handle_camera_command(
                "sb/capture-submit",
                command_message(
                    "sb/capture-submit",
                    "submit-second",
                    json!({
                        "requestId": "submit-reused",
                        "instance": "camera-a",
                        "metadata": { "changed": true },
                    }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        conflict.code,
        crate::ErrorCode::IdempotencyConflict.as_str(),
        "the reused key must be refused, not silently answered with the original capture"
    );
    let jobs = jobs_for(&runtime, "camera-a").await;
    assert_eq!(
        jobs.len(),
        1,
        "and the refusal must not have created a second capture: {jobs:?}"
    );
    assert_eq!(jobs[0].capture_id, capture_id);
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// `sb/capture-status` answers by capture, by group, by camera request, by group request, and as a
/// paged list -- and refuses a group cursor it never issued.
///
/// These are five different questions an operator can only ask if the component can answer them from
/// what the CALLER kept. A caller that submitted work and then restarted has its own `requestId` and
/// nothing else; if the only lookup were by `captureId`, that caller could never find out what
/// happened to the capture it is still responsible for.
#[tokio::test]
async fn the_capture_status_verb_answers_by_capture_group_request_and_list() {
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    let runtime = runtime(configuration, &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    let crate::catalog::AcceptJobOutcome::Inserted(single) = runtime
        .submit_capture(
            "camera-a".to_string(),
            "status-request".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "status-correlation".to_string(),
            CommandVerb::Capture.as_str(),
            crate::admission::CapturePriority::Direct,
        )
        .await
        .expect("the single capture must be accepted")
    else {
        panic!("a fresh requestId must insert a new capture");
    };
    let group = runtime
        .submit_group(
            group_request("status-group", &["camera-a", "camera-b"]),
            "status-group-correlation".to_string(),
            crate::admission::CapturePriority::Direct,
            None,
        )
        .await
        .expect("the group must be accepted");
    wait_for_terminal(&runtime, &single.capture_id).await;
    wait_for_group_terminal(&runtime, &group.group_id).await;

    let by_capture = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-status",
                command_message(
                    "sb/capture-status",
                    "by-capture",
                    json!({ "captureId": single.capture_id }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(by_capture["state"], "SUCCEEDED");

    let by_camera_request = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-status",
                command_message(
                    "sb/capture-status",
                    "by-camera-request",
                    json!({ "instance": "camera-a", "requestId": "status-request" }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        by_camera_request["captureId"], single.capture_id,
        "a caller that kept only its own requestId must still find its capture"
    );

    let by_group = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-status",
                command_message(
                    "sb/capture-status",
                    "by-group",
                    json!({ "captureGroupId": group.group_id, "limit": 1 }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        by_group["group"]["captureGroupId"], group.group_id,
        "the group's own descriptor comes back with its members"
    );
    assert_eq!(
        by_group["members"].as_array().map(Vec::len),
        Some(1),
        "the members page honours the requested page size"
    );
    let member_cursor = by_group["nextCursor"]
        .as_str()
        .expect("a two-camera group paged one at a time has a second page")
        .to_string();
    let member_page_two = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-status",
                command_message(
                    "sb/capture-status",
                    "by-group-page-two",
                    json!({
                        "captureGroupId": group.group_id,
                        "limit": 1,
                        "cursor": member_cursor,
                    }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        member_page_two["members"].as_array().map(Vec::len),
        Some(1),
        "and the continuation returns the group's remaining member"
    );

    let bad_cursor = immediate_error(
        runtime
            .handle_camera_command(
                "sb/capture-status",
                command_message(
                    "sb/capture-status",
                    "by-group-bad-cursor",
                    json!({
                        "captureGroupId": group.group_id,
                        "limit": 1,
                        "cursor": "cur_never-issued",
                    }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        bad_cursor.code,
        crate::ErrorCode::BadArgs.as_str(),
        "a member cursor that belongs to no retained page is refused, not answered from the head"
    );

    let by_group_request = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-status",
                command_message(
                    "sb/capture-status",
                    "by-group-request",
                    json!({ "requestId": "status-group" }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        by_group_request["group"]["captureGroupId"], group.group_id,
        "a requestId with no instance is the GROUP key, and must resolve the group"
    );

    let listed = immediate_success(
        runtime
            .handle_camera_command(
                "sb/capture-status",
                command_message(
                    "sb/capture-status",
                    "listed",
                    json!({ "states": ["SUCCEEDED"], "limit": 50 }),
                ),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(
        listed["jobs"].as_array().map(Vec::len),
        Some(3),
        "the list mode reports every durable capture in the requested state"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// A group in which some cameras succeeded and some failed is reported as PARTIAL.
///
/// The durable catalog only has the shared job vocabulary, so a mixed group is stored FAILED. That
/// is not what the operator needs to be told: "the group failed" and "three of your four cameras
/// captured" are different facts, and the second one is the one that decides whether the line has to
/// be re-run. The public aggregate keeps the distinction the durable state cannot.
#[tokio::test]
async fn a_group_whose_members_did_not_all_succeed_is_reported_as_partial() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for (index, camera) in configuration.instances.iter_mut().enumerate() {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
        if index == 1 {
            // camera-b's camera fails every capture it is asked for -- deterministically.
            sim.faults.fail_every_nth_capture = Some(1);
        }
    }
    let runtime = runtime(configuration, &directory).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    let accepted = runtime
        .submit_group(
            group_request("partial-group", &["camera-a", "camera-b"]),
            "partial-correlation".to_string(),
            crate::admission::CapturePriority::Direct,
            None,
        )
        .await
        .expect("a group with a camera that will fail is still accepted");
    let terminal =
        wait_for_group_terminal_within(&runtime, &accepted.group_id, Duration::from_secs(20)).await;

    let succeeded = terminal
        .members
        .iter()
        .filter(|member| member.state == crate::model::JobState::Succeeded)
        .count();
    assert_eq!(
        succeeded, 1,
        "exactly one camera captured; the other was configured to fail: {:?}",
        terminal
            .members
            .iter()
            .map(|member| (member.instance.clone(), member.state))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        terminal.state,
        crate::model::JobState::Failed,
        "the durable state stays in the shared vocabulary"
    );
    assert_eq!(
        terminal
            .terminal_result
            .as_ref()
            .and_then(|result| result.get("state")),
        Some(&json!("PARTIAL")),
        "but the operator is told PARTIAL, because 'the group failed' would hide the camera that worked"
    );
    runtime.shutdown().await;
}

/// A capture takes its capture mode from the BACKEND that will perform it, not from a default.
///
/// The mode decides how the frame is acquired -- an ONVIF camera is read through the snapshot URI it
/// was configured for, a GenICam camera is software-triggered -- and it is frozen into the durable
/// job at acceptance. A capture that resolved the wrong mode would be handed to the camera with the
/// wrong acquisition entirely, and the durable record would say it had been asked for correctly.
#[tokio::test]
async fn a_capture_takes_its_capture_mode_from_the_backend_that_will_perform_it() {
    let directory = TempDir::new().unwrap();
    let profile = json!({ "main": { "output": { "encoding": "jpeg" } } });
    let core = Config::from_value(
        COMPONENT_NAME,
        "gw-01",
        json!({
            "component": {
                "global": { "output": { "rootDirectory": directory.path().to_string_lossy() } },
                "instances": [
                    {
                        "id": "camera-onvif",
                        "backend": {
                            "type": "onvif-rtsp",
                            "mediaProfile": "main",
                            "deviceServiceUrl": "https://127.0.0.1:65535/onvif/device_service",
                        },
                        "defaultCaptureProfile": "main",
                        "captureProfiles": profile,
                    },
                    {
                        "id": "camera-genicam",
                        "backend": {
                            "type": "genicam-aravis",
                            "selector": { "serial": "SN-42" },
                        },
                        "defaultCaptureProfile": "main",
                        "captureProfiles": profile,
                    },
                ],
            }
        }),
    )
    .expect("the protocol-backend fixture must be structurally valid");
    let configuration =
        AdapterConfig::from_core_reload(&core).expect("the protocol-backend fixture must validate");
    let runtime = runtime(configuration, &directory).await;

    for (instance, expected) in [
        ("camera-onvif", crate::model::CaptureMode::SnapshotUri),
        ("camera-genicam", crate::model::CaptureMode::SoftwareTrigger),
    ] {
        let crate::catalog::AcceptJobOutcome::Inserted(record) = runtime
            .submit_capture(
                instance.to_string(),
                format!("mode-{instance}"),
                None,
                None,
                serde_json::Map::new(),
                "capture-mode-correlation".to_string(),
                CommandVerb::CaptureSubmit.as_str(),
                crate::admission::CapturePriority::Submitted,
            )
            .await
            .expect("an offline protocol camera still accepts a durable capture")
        else {
            panic!("a fresh requestId must insert a new capture");
        };
        assert_eq!(
            record.effective_profile.get("captureMode"),
            Some(&serde_json::to_value(expected).unwrap()),
            "{instance} must have frozen the capture mode its own backend performs"
        );
    }
    runtime.shutdown().await;
}

/// Storage pressure cannot un-accept a capture that was accepted while the disk was being checked.
///
/// The capacity check and the durable insert are not one step, so a submission held inside its check
/// can wake to find its own capture already accepted by the caller that raced it. Answering
/// STORAGE_PRESSURE then would deny work the component has already promised -- and the caller would
/// retry, and be told the same thing, about a capture that is running.
#[tokio::test]
async fn storage_pressure_cannot_erase_a_capture_that_was_accepted_underneath_it() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a"], false);
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(Semaphore::new(0));
    let pressured = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let monitor =
        gated_pressure_monitor(&configuration, &directory, &entered, &release, &pressured);
    let runtime = runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;

    let held = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "raced-capture".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "raced-capture-held".to_string(),
                    CommandVerb::CaptureSubmit.as_str(),
                    crate::admission::CapturePriority::Submitted,
                )
                .await
        })
    };
    wait_for_gate(&entered).await;

    let crate::catalog::AcceptJobOutcome::Inserted(winner) = runtime
        .submit_capture(
            "camera-a".to_string(),
            "raced-capture".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "raced-capture-winner".to_string(),
            CommandVerb::CaptureSubmit.as_str(),
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("the second caller sees a healthy disk and accepts the capture")
    else {
        panic!("the winner must insert the durable capture");
    };
    pressured.store(true, std::sync::atomic::Ordering::Release);
    release.add_permits(1);

    let woken = held
        .await
        .expect("the held caller must not panic")
        .expect("a capture that is already durable must not be un-accepted by pressure");
    let crate::catalog::AcceptJobOutcome::Existing(record) = woken else {
        panic!("the held caller must be folded onto the capture accepted underneath it: {woken:?}");
    };
    assert_eq!(
        record.capture_id, winner.capture_id,
        "and it must be the SAME capture, not a second one"
    );
    assert_eq!(
        record.origin_correlation_id.as_deref(),
        Some("raced-capture-winner"),
        "the durable record keeps the acceptance that actually happened"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "exactly one capture may exist for the key both callers used"
    );
    runtime.shutdown().await;
}

/// Storage pressure reports a CONFLICT when the key it wakes to find was reused for other work.
///
/// The re-check that keeps pressure from erasing an accepted capture must not go the other way and
/// hand the caller somebody else's capture. The key belongs to the arguments it was first used with;
/// a held caller whose arguments differ is told so, exactly as it would have been without pressure.
#[tokio::test]
async fn storage_pressure_reports_a_conflict_when_the_capture_key_was_reused_underneath_it() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a"], false);
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(Semaphore::new(0));
    let pressured = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let monitor =
        gated_pressure_monitor(&configuration, &directory, &entered, &release, &pressured);
    let runtime = runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;

    let held = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            runtime
                .submit_capture(
                    "camera-a".to_string(),
                    "raced-conflict".to_string(),
                    None,
                    None,
                    serde_json::Map::new(),
                    "raced-conflict-held".to_string(),
                    CommandVerb::CaptureSubmit.as_str(),
                    crate::admission::CapturePriority::Submitted,
                )
                .await
        })
    };
    wait_for_gate(&entered).await;

    let mut different = serde_json::Map::new();
    different.insert("changed".to_string(), json!(true));
    runtime
        .submit_capture(
            "camera-a".to_string(),
            "raced-conflict".to_string(),
            None,
            None,
            different,
            "raced-conflict-winner".to_string(),
            CommandVerb::CaptureSubmit.as_str(),
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("the winner accepts its own capture under the shared key");
    pressured.store(true, std::sync::atomic::Ordering::Release);
    release.add_permits(1);

    let woken = held
        .await
        .expect("the held caller must not panic")
        .expect("the held caller is answered, not failed");
    assert!(
        matches!(woken, crate::catalog::AcceptJobOutcome::Conflict),
        "a key reused with different arguments is a conflict, even under pressure: {woken:?}"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "and the conflicted caller must not have created a capture of its own"
    );
    runtime.shutdown().await;
}

/// A group capture whose `requestId` cannot be a durable key is refused before anything exists.
///
/// The `requestId` IS the idempotency key: it is what a retry is matched on. A group that could be
/// accepted without one -- or with one the durable layer cannot store -- has no retry semantics at
/// all, and the caller would never find out.
#[tokio::test]
async fn a_group_capture_with_an_unusable_request_id_is_refused_before_anything_durable() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(
        config(directory.path(), &["camera-a", "camera-b"], false),
        &directory,
    )
    .await;

    let error = runtime
        .submit_group(
            group_request("", &["camera-a", "camera-b"]),
            "empty-key-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .expect_err("an empty requestId cannot key a durable group");
    assert_eq!(
        error.code(),
        crate::ErrorCode::BadArgs,
        "the caller is told its key is unusable rather than given un-retryable work"
    );
    assert!(
        jobs_for(&runtime, "camera-a").await.is_empty(),
        "and nothing durable may exist for a group that was never keyed"
    );
    runtime.shutdown().await;
}

/// A deferred group caller that has already been answered is reported, not answered twice.
///
/// A retry of a finished group settles the caller from the durable result. If that settlement cannot
/// be delivered -- the reply already went out, the token is spent -- the component must say so.
/// Swallowing the failure would report success to a caller that was never reached.
#[tokio::test]
async fn a_deferred_group_caller_who_can_no_longer_be_answered_reports_a_backend_error() {
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    let runtime = runtime(configuration, &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    let accepted = runtime
        .submit_group(
            group_request("spent-token-group", &["camera-a", "camera-b"]),
            "spent-token-original".to_string(),
            crate::admission::CapturePriority::Direct,
            None,
        )
        .await
        .expect("the group is accepted");
    wait_for_group_terminal(&runtime, &accepted.group_id).await;

    let token = deferred
        .defer(
            &command_message(
                "sb/capture-group",
                "spent-token",
                json!({ "requestId": "spent-token-group", "instances": ["camera-a", "camera-b"] }),
            ),
            Duration::from_secs(30),
        )
        .expect("the fixture message carries a reply topic");
    token.activate().expect("the command inbox is running");
    token
        .settle_success(Some(json!({ "answered": "already" })))
        .await
        .expect("the caller can be answered exactly once");

    let error = runtime
        .submit_group(
            group_request("spent-token-group", &["camera-a", "camera-b"]),
            "spent-token-retry".to_string(),
            crate::admission::CapturePriority::Direct,
            Some(token),
        )
        .await
        .expect_err("a caller that cannot be settled must not be reported as settled");
    assert_eq!(
        error.code(),
        crate::ErrorCode::BackendError,
        "the component says the reply could not be delivered instead of claiming it was"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// A deferred capture caller that has already been answered is reported, not answered twice.
///
/// A direct `sb/capture` retried after its capture finished is settled from the durable result --
/// that is what makes the verb replayable at all. If that settlement cannot be delivered, because
/// the caller has already been answered and its reply token is spent, the component must say so.
/// Reporting success would hand a "we told them" to a caller that was never reached.
#[tokio::test]
async fn a_deferred_capture_caller_who_can_no_longer_be_answered_reports_a_backend_error() {
    let (port, _publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.capture_delay_ms = 1;
    let runtime = runtime(configuration, &directory).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let body = json!({ "requestId": "spent-capture", "instance": "camera-a" });
    deferred_capture_outcome(&runtime, &deferred, "spent-capture-first", body.clone())
        .await
        .expect("the first caller's capture is accepted");
    let first = jobs_for(&runtime, "camera-a").await;
    assert_eq!(first.len(), 1, "one capture was asked for and one accepted");
    wait_for_terminal(&runtime, &first[0].capture_id).await;

    let CommandOutcome::DeferredWithContinuation {
        token,
        continuation,
    } = runtime
        .handle_deferred_capture(
            command_message("sb/capture", "spent-capture-retry", body),
            deferred.clone(),
        )
        .await
    else {
        panic!("a direct capture must hand off to a deferred continuation");
    };
    // The retrying caller is answered by something else before its replay can settle it: exactly the
    // state a duplicate delivery, or a reply that already went out, leaves the token in.
    token
        .settle_success(Some(json!({ "answered": "already" })))
        .await
        .expect("the caller can be answered exactly once");

    let error = continuation
        .await
        .expect_err("a caller that cannot be settled must not be reported as settled");
    assert_eq!(
        error.code,
        crate::ErrorCode::BackendError.as_str(),
        "the component says the reply could not be delivered instead of claiming it was"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "and the replay must not have captured a second time"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// A caller held through its capacity check onto a group that FINISHED is answered from it.
///
/// It cannot be attached to the group -- the fan-out that settles a group's waiters has already
/// run -- so the durable result is the only thing left that can answer it. A caller that took the
/// slow path through the disk check must still be told what happened to the work it asked for.
#[tokio::test]
async fn a_caller_held_through_its_capacity_check_onto_a_finished_group_is_answered_from_it() {
    let (port, publishes) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.capture_delay_ms = 1;
    }
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(Semaphore::new(0));
    let pressured = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let monitor =
        gated_pressure_monitor(&configuration, &directory, &entered, &release, &pressured);
    let runtime = runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;
    let (app, deferred) = command_deferred_registry(&directory, port).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    let token = deferred
        .defer(
            &command_message(
                "sb/capture-group",
                "held-onto-finished",
                json!({ "requestId": "held-finished", "instances": ["camera-a", "camera-b"] }),
            ),
            Duration::from_secs(30),
        )
        .expect("the fixture message carries a reply topic");
    token.activate().expect("the command inbox is running");
    let held = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            runtime
                .submit_group(
                    group_request("held-finished", &["camera-a", "camera-b"]),
                    "held-finished-held".to_string(),
                    crate::admission::CapturePriority::Direct,
                    Some(token),
                )
                .await
        })
    };
    wait_for_gate(&entered).await;

    let accepted = runtime
        .submit_group(
            group_request("held-finished", &["camera-a", "camera-b"]),
            "held-finished-winner".to_string(),
            crate::admission::CapturePriority::Direct,
            None,
        )
        .await
        .expect("the second caller accepts the group");
    let terminal = wait_for_group_terminal(&runtime, &accepted.group_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    release.add_permits(1);

    let woken = held
        .await
        .expect("the held caller must not panic")
        .expect("a finished group must be handed to the caller that was held, not refused");
    assert_eq!(
        woken.group_id, accepted.group_id,
        "the held caller is folded onto the group that ran while it waited"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "and no second set of member captures may have been created"
    );
    let reply = wait_for_recorded_reply(&publishes, 0).await;
    assert_eq!(
        reply.body["result"]["captureGroupId"], accepted.group_id,
        "the held caller is answered with the durable result of the group it asked for"
    );
    runtime.shutdown().await;
    app.commands().unwrap().stop().await;
}

/// A held caller that can no longer be answered is reported as such, on both wake-up paths.
///
/// Two different lines settle a caller that wakes onto a group somebody else accepted: one when the
/// disk is healthy and the durable insert finds the group, one when the disk has filled up and the
/// pressure re-check finds it. Both hand the same answer to the same caller, so both must fail the
/// same way when that caller can no longer be reached -- an undeliverable reply reported as a
/// success is a caller left waiting forever.
#[tokio::test]
async fn a_held_caller_that_can_no_longer_be_answered_is_reported_on_both_wake_up_paths() {
    for pressure_on_wake in [false, true] {
        let (port, _publishes) = spawn_recording_mqtt_broker().await;
        let directory = TempDir::new().unwrap();
        let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
        for camera in &mut configuration.instances {
            let crate::config::BackendConfig::Sim(sim) = &mut camera.backend else {
                panic!("test fixture must use the simulator backend");
            };
            sim.capture_delay_ms = 1;
        }
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = Arc::new(Semaphore::new(0));
        let pressured = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let monitor =
            gated_pressure_monitor(&configuration, &directory, &entered, &release, &pressured);
        let runtime = runtime_with_storage_pressure(configuration, &directory, Some(monitor)).await;
        let (app, deferred) = command_deferred_registry(&directory, port).await;
        for instance in ["camera-a", "camera-b"] {
            runtime
                .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
                .unwrap();
            wait_for_online(&runtime, instance).await;
        }

        let token = deferred
            .defer(
                &command_message(
                    "sb/capture-group",
                    "held-and-spent",
                    json!({ "requestId": "held-spent", "instances": ["camera-a", "camera-b"] }),
                ),
                Duration::from_secs(30),
            )
            .expect("the fixture message carries a reply topic");
        token.activate().expect("the command inbox is running");
        let held = {
            let runtime = Arc::clone(&runtime);
            let token = token.clone();
            tokio::spawn(async move {
                runtime
                    .submit_group(
                        group_request("held-spent", &["camera-a", "camera-b"]),
                        "held-spent-held".to_string(),
                        crate::admission::CapturePriority::Direct,
                        Some(token),
                    )
                    .await
            })
        };
        wait_for_gate(&entered).await;

        let accepted = runtime
            .submit_group(
                group_request("held-spent", &["camera-a", "camera-b"]),
                "held-spent-winner".to_string(),
                crate::admission::CapturePriority::Direct,
                None,
            )
            .await
            .expect("the second caller accepts the group");
        wait_for_group_terminal(&runtime, &accepted.group_id).await;

        // The held caller is answered by something else while it waits: its token is now spent.
        token
            .settle_success(Some(json!({ "answered": "already" })))
            .await
            .expect("the caller can be answered exactly once");
        pressured.store(pressure_on_wake, std::sync::atomic::Ordering::Release);
        release.add_permits(1);

        let error = held
            .await
            .expect("the held caller must not panic")
            .expect_err("a caller that cannot be settled must not be reported as settled");
        assert_eq!(
            error.code(),
            crate::ErrorCode::BackendError,
            "an undeliverable reply must be reported (pressure on wake: {pressure_on_wake})"
        );
        runtime.shutdown().await;
        app.commands().unwrap().stop().await;
    }
}

/// Concurrent group submissions that DISAGREE accept one group and conflict the other.
///
/// Two callers can both find no group and both go on to accept one. When their arguments agree that
/// is a fold-in; when they disagree it is exactly the collision the idempotency key exists to catch,
/// and the durable insert is the last line that can catch it. Losing that race must not accept a
/// second group under the same key.
#[tokio::test]
async fn concurrent_group_submissions_that_disagree_accept_one_and_conflict_the_other() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(
        config(directory.path(), &["camera-a", "camera-b"], false),
        &directory,
    )
    .await;

    let mut submissions = Vec::new();
    for (index, wanted) in ["one-frame", "two-frames"].into_iter().enumerate() {
        let runtime = Arc::clone(&runtime);
        submissions.push(tokio::spawn(async move {
            // The metadata is part of the immutable acceptance arguments, so these two callers are
            // asking for different work under the same key.
            let mut request = group_request("disagreeing-group", &["camera-a", "camera-b"]);
            request
                .metadata
                .insert("wanted".to_string(), json!(wanted));
            runtime
                .submit_group(
                    request,
                    format!("disagreeing-correlation-{index}"),
                    crate::admission::CapturePriority::Submitted,
                    None,
                )
                .await
        }));
    }
    let mut accepted = 0_usize;
    let mut conflicts = 0_usize;
    for submission in submissions {
        match submission.await.expect("no caller may panic") {
            Ok(_) => accepted += 1,
            Err(error) => {
                assert_eq!(
                    error.code(),
                    crate::ErrorCode::IdempotencyConflict,
                    "the caller that lost the race is told its key was already used differently: {error:?}"
                );
                conflicts += 1;
            }
        }
    }
    assert_eq!(
        (accepted, conflicts),
        (1, 1),
        "exactly one of two disagreeing submissions may be accepted"
    );
    assert_eq!(
        jobs_for(&runtime, "camera-a").await.len(),
        1,
        "and the conflicted group must not have left member captures behind"
    );
    runtime.shutdown().await;
}

/// Discovery that is allowed to retain nothing probes nothing.
///
/// `discovery.maxResults` is the bound on what a probe may hand back, and it is applied BEFORE the
/// probe rather than to its results. That matters here: this ONVIF backend refuses to probe at all
/// without an eligible interface, so a component that probed first and truncated afterwards would
/// answer this request with that refusal instead of the empty result the bound requires.
#[tokio::test]
async fn discovery_that_may_retain_nothing_probes_nothing() {
    let directory = TempDir::new().unwrap();
    let mut configuration = mixed_backend_roster(&directory);
    configuration.global.discovery.max_results = 0;
    let runtime = runtime(configuration, &directory).await;

    let answer = runtime
        .discover(DiscoverRequest {
            backends: Vec::new(),
            timeout_ms: 100,
            limit: 10,
            cursor: None,
        })
        .await
        .expect("a discovery bounded to nothing succeeds without probing");
    assert_eq!(
        answer["candidates"].as_array().map(Vec::len),
        Some(0),
        "a discovery that may retain no results reports none"
    );
    assert!(
        answer["nextCursor"].is_null(),
        "and there is nothing to continue"
    );
    runtime.shutdown().await;
}

/// A drain reports the capture no live engine owns instead of claiming it drained it.
///
/// A durable capture whose in-memory runtime does not exist -- one recovery owns, or one left behind
/// by a previous process -- cannot be cancelled through the engine, because there is nothing running
/// to cancel. The drain must say so. Counting it as cancelled would tell an operator the queue was
/// clear while the row that made them reach for the break-glass drain was still sitting there.
#[tokio::test]
async fn a_drain_reports_a_capture_no_live_engine_owns_rather_than_claiming_it_drained_it() {
    let directory = TempDir::new().unwrap();
    let configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    let runtime = runtime(configuration.clone(), &directory).await;

    // A durable capture written straight to the catalog: the camera's engine has never seen it, which
    // is the state a restart leaves behind for every capture recovery has not yet adopted.
    runtime
        .catalog
        .accept_job(queued_job(&configuration, "cap-unowned"))
        .await
        .expect("the durable capture must be accepted");

    let outcome = runtime
        .clear_queue(None, true, "operator drain".to_string())
        .await
        .expect("the drain itself must not fail because one row resisted it");
    assert_eq!(
        outcome.cancelled, 0,
        "nothing was cancelled: no engine owns the capture"
    );
    assert_eq!(
        outcome.failed.len(),
        1,
        "and the row that resisted the drain is reported: {outcome:?}"
    );
    assert_eq!(outcome.failed[0].capture_id, "cap-unowned");
    assert!(
        runtime
            .catalog
            .job("cap-unowned")
            .await
            .unwrap()
            .is_some_and(|job| !job.state.is_terminal()),
        "the capture is still there, which is exactly why the drain must not claim otherwise"
    );
    runtime.shutdown().await;
}

/// `sb/queue-status` for a camera that does not exist is refused, like every other targeted verb.
///
/// A queue depth of zero and "there is no such camera" look identical to a caller reading numbers,
/// and only one of them means the camera is idle.
#[tokio::test]
async fn queue_status_for_a_camera_that_does_not_exist_is_refused() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;

    let error = runtime
        .queue_status_command(commands::QueueStatusRequest {
            instance: Some("camera-ghost".to_string()),
        })
        .await
        .expect_err("an unknown camera has no queue to report");
    assert_eq!(
        error.code(),
        crate::ErrorCode::NoSuchInstance,
        "the caller is told the camera does not exist, not handed an empty queue"
    );

    let fleet = runtime
        .queue_status_command(commands::QueueStatusRequest { instance: None })
        .await
        .expect("the fleet question is always answerable");
    assert_eq!(
        fleet["cameras"].as_array().map(Vec::len),
        Some(1),
        "and the fleet answer covers every configured camera"
    );
    runtime.shutdown().await;
}

/// A camera retired with no actor left to stop it through is reported, not counted as stopped.
///
/// This is the state a camera is in when its session drops after a move was commanded: the stop is
/// still armed, and the handle it would have been delivered through is gone. The retirement path
/// must retire the armed timer (or the old deadline would fire at a session that no longer exists)
/// and must NOT count the camera among those it stopped -- a component that reported a stop it could
/// not deliver is worse than one that reports none, because the camera is still moving either way.
#[tokio::test]
async fn a_camera_retired_with_no_actor_to_stop_it_through_is_not_counted_as_stopped() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;

    let timer = CancellationToken::new();
    runtime
        .cameras
        .write()
        .unwrap()
        .get_mut("camera-a")
        .expect("the camera is in the roster")
        .motion_stop = Some(ArmedStop {
        cancellation: timer.clone(),
        pan: true,
        tilt: false,
        zoom: false,
        ptz_ms: 1_000,
    });

    assert_eq!(
        runtime.deliver_mandatory_stops(None),
        0,
        "no actor means no stop was delivered, and the count must say so"
    );
    assert!(
        timer.is_cancelled(),
        "the armed deadline is retired: it must not fire later at a session that no longer exists"
    );
    assert!(
        armed_stop(&runtime, "camera-a").is_none(),
        "and the camera no longer carries a stop nothing is going to deliver"
    );
    runtime.shutdown().await;
}

/// A preset cursor that belongs to no retained snapshot is refused.
///
/// A preset list is retained and paged: the continuation is a view of the presets the camera
/// reported ONCE. A cursor the component cannot resolve must be refused rather than quietly answered
/// with a fresh preset list read from the camera -- the pages would not line up, and a caller
/// walking them would silently skip or repeat presets.
#[tokio::test]
async fn a_preset_cursor_that_belongs_to_no_snapshot_is_refused() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(ptz_camera(&directory, 10_000), &directory).await;

    let error = runtime
        .perform_presets(PtzPresetsRequest::List {
            instance: Some("camera-a".to_string()),
            limit: 10,
            cursor: Some("cur_never-issued".to_string()),
        })
        .await
        .expect_err("a cursor with no retained preset snapshot behind it cannot be paged");
    assert_eq!(
        error.code(),
        crate::ErrorCode::BadArgs,
        "an unresolvable cursor is refused, not turned into a second read of the camera"
    );
    runtime.shutdown().await;
}

/// A configuration candidate that is not this component's configuration is rejected, not applied.
///
/// Validation runs on a document Core has not yet committed, and its two parses -- the candidate and
/// the current configuration it is compared against -- are both fallible. Neither may be allowed to
/// become an acceptance by default: a candidate that cannot be understood is a candidate that cannot
/// be shown to be safe.
#[tokio::test]
async fn candidate_validation_rejects_what_it_cannot_understand() {
    let directory = TempDir::new().unwrap();
    let valid = json!({
        "component": {
            "global": { "output": { "rootDirectory": directory.path().to_string_lossy() } },
            "instances": [{
                "id": "camera-a",
                "backend": { "type": "sim" },
                "defaultCaptureProfile": "main",
                "captureProfiles": { "main": { "output": { "encoding": "jpeg" } } }
            }]
        }
    });

    let unreadable = validate_configuration_candidate(
        json!("this is not a configuration document at all"),
        None,
        ConfigurationValidationPhase::Initial,
    )
    .expect("validation answers rather than failing");
    assert!(
        matches!(
            unreadable,
            ConfigurationValidationResult::Reject { ref code, .. } if code == "CAMERA_CONFIG_INVALID"
        ),
        "a document that is not component configuration is rejected: {unreadable:?}"
    );

    let uncomparable = validate_configuration_candidate_with_credentials(
        valid,
        Some(json!("this is not the current configuration either")),
        ConfigurationValidationPhase::Reload,
        true,
    )
    .expect("validation answers rather than failing");
    assert!(
        matches!(
            uncomparable,
            ConfigurationValidationResult::Reject { ref code, .. } if code == "CAMERA_CONFIG_INVALID"
        ),
        "a candidate that cannot be compared against the current generation is rejected: {uncomparable:?}"
    );
}

/// Validation accepts an initial generation, and a reload that needs no credential service.
///
/// The credential veto exists because a core credential service is built only for the FIRST
/// generation: a later reload that introduces an ONVIF secret reference could not resolve it. That
/// veto must be about the secret, not about the reload -- a generation whose cameras reference no
/// secret at all is safe to apply whether or not a credential service was ever constructed, and
/// refusing it would make every reload on a credential-free component impossible.
#[tokio::test]
async fn candidate_validation_accepts_an_initial_generation_and_a_credential_free_reload() {
    let directory = TempDir::new().unwrap();
    let candidate = json!({
        "component": {
            "global": { "output": { "rootDirectory": directory.path().to_string_lossy() } },
            "instances": [{
                "id": "camera-a",
                "backend": { "type": "sim" },
                "defaultCaptureProfile": "main",
                "captureProfiles": { "main": { "output": { "encoding": "jpeg" } } }
            }]
        }
    });

    assert_eq!(
        validate_configuration_candidate(
            candidate.clone(),
            None,
            ConfigurationValidationPhase::Initial,
        )
        .expect("validation answers rather than failing"),
        ConfigurationValidationResult::Accept,
        "the initial generation is validated on its own, with nothing to compare it against"
    );
    assert_eq!(
        validate_configuration_candidate_with_credentials(
            candidate.clone(),
            Some(candidate),
            ConfigurationValidationPhase::Reload,
            false,
        )
        .expect("validation answers rather than failing"),
        ConfigurationValidationResult::Accept,
        "a reload whose cameras reference no secret needs no credential service to be safe"
    );
}

/// A durable state directory that cannot be created fails startup rather than running without one.
///
/// Everything the component promises -- idempotency, recovery, the terminal record -- lives in that
/// directory. Starting without it would produce a component that accepts captures and forgets them,
/// so the failure has to be an all-or-nothing startup failure and not a warning.
#[tokio::test]
async fn a_durable_state_directory_that_cannot_be_created_fails_startup() {
    let directory = TempDir::new().unwrap();
    let blocking_file = directory.path().join("not-a-directory");
    std::fs::write(&blocking_file, b"this path is a file").unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    configuration.global.state.directory = Some(
        blocking_file
            .join("state")
            .to_string_lossy()
            .into_owned(),
    );

    let Err(error) = prepare_startup_resources(&configuration, Platform::Host).await else {
        panic!("a state directory underneath a regular file cannot be created");
    };
    assert!(
        matches!(error, crate::CameraError::Storage(_)),
        "the failure is reported as the storage failure it is: {error:?}"
    );
}

/// A command runtime cannot be installed once shutdown has begun.
///
/// The router latches shutdown before the runtime is torn down. Installing a delegate after that
/// point would re-open the command plane onto a runtime whose durable state is already being closed,
/// and every command that arrived would be answered by a component that is going away.
#[tokio::test]
async fn a_command_runtime_cannot_be_installed_once_shutdown_has_begun() {
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a"], false), &directory).await;
    let router = RuntimeCommandRouter::new();
    router.begin_shutdown();

    let error = router
        .install(Arc::clone(&runtime) as Arc<dyn CameraCommandService>)
        .expect_err("a stopping component must not adopt a command runtime");
    assert_eq!(
        error.code(),
        crate::ErrorCode::ComponentStopping,
        "the refusal names the reason: the component is already stopping"
    );
    runtime.shutdown().await;
}

/// The image that is delivered is the image the camera produced. Byte for byte.
///
/// Nothing proved this. The chain the suite DID cover is self-referential: storage writes the bytes it
/// was handed, then computes the SHA-256 by RE-READING the file it just wrote, then verifies the file
/// against that same digest before confirming, and finally reports that digest on the wire. Every link
/// checks the file against itself. Not one of them checks the file against the CAMERA.
///
/// So a defect that truncated, padded, reordered or swapped a frame anywhere between the backend and the
/// partial write would produce a perfectly self-consistent digest OF THE WRONG IMAGE, the install would
/// verify it, the wire would report it, and all 568 tests would pass. For an adapter whose entire purpose
/// is to deliver the picture the camera took, that is the one thing worth pinning.
///
/// The simulator's frame is a pure function of its seed and the capture ordinal, so the exact bytes the
/// camera produced can be regenerated INDEPENDENTLY of the pipeline that carried them, and compared with
/// what actually landed on disk. `Raw` encoding is what makes this a byte-for-byte claim rather than a
/// pixel-for-pixel one: it is passthrough, so the persisted artifact must equal the frame exactly.
#[tokio::test]
async fn the_image_that_is_delivered_is_the_image_the_camera_took() {
    use sha2::{Digest, Sha256};

    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    // An explicit seed makes the frame reproducible; without it the sim derives one from the instance id.
    sim.seed = Some(20_260_714);
    sim.frame.width = 64;
    sim.frame.height = 48;
    let backend_config = sim.clone();

    // Passthrough, so "the same picture" means "the same bytes" and nothing re-encodes them.
    for profile in configuration.instances[0].capture_profiles.values_mut() {
        profile.output.encoding = crate::model::OutputEncoding::Raw;
    }

    let runtime = runtime(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "byte-fidelity".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "byte-fidelity-correlation".to_string(),
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

    // What the component says it delivered.
    let final_path = terminal
        .final_path
        .as_deref()
        .expect("a succeeded capture must name the artifact it installed");
    let installed_sha256 = terminal
        .installed_sha256
        .as_deref()
        .expect("a succeeded capture must report the digest it installed");
    let installed_bytes = terminal
        .installed_bytes
        .expect("a succeeded capture must report the size it installed");

    // What the camera actually produced -- regenerated from the same seed, through the backend itself,
    // with no part of the capture pipeline involved.
    use crate::backend::CameraBackendFactory as _;
    let factory = crate::backend::sim::SimBackendFactory;
    let mut session = factory
        .connect(crate::backend::ConnectRequest {
            instance_id: "camera-a".to_owned(),
            backend: crate::config::BackendConfig::Sim(backend_config),
            timeout: Duration::from_secs(5),
            cancellation: CancellationToken::new(),
        })
        .await
        .expect("the simulator must connect");
    let expected = session
        .capture(crate::backend::CaptureRequest {
            capture_id: "regenerated".to_owned(),
            profile: terminal_profile(&terminal),
            maximum_frame_bytes: 8 * 1024 * 1024,
            timeout: Duration::from_secs(5),
            cancellation: CancellationToken::new(),
        })
        .await
        .expect("the simulator must produce its first frame")
        .bytes;
    session.close().await.ok();

    // What is actually on disk.
    let delivered = std::fs::read(final_path).expect("the installed artifact must be readable");

    assert_eq!(
        delivered.len(),
        expected.len(),
        "the delivered image is a different SIZE from the frame the camera produced"
    );
    assert!(
        delivered == expected.as_ref(),
        "the delivered image is not the image the camera took -- same length, different bytes, which is \
         precisely the corruption a self-referential digest cannot see"
    );

    // And the digest the operator is handed describes THAT image, not merely the file it was taken from.
    let camera_digest = hex::encode(Sha256::digest(&expected));
    assert_eq!(
        installed_sha256, camera_digest,
        "the digest on the wire must be the digest of the CAMERA'S frame; a digest computed from the \
         file it was written to would agree with a corrupted file just as happily"
    );
    assert_eq!(
        installed_bytes,
        expected.len() as u64,
        "and the size on the wire must be the camera's frame size"
    );

    runtime.shutdown().await;
}

/// The profile the durable record says was used, so the regenerated frame is asked for on the same terms.
fn terminal_profile(record: &crate::catalog::JobRecord) -> crate::config::CaptureProfile {
    let snapshot: crate::jobs::JobProfileSnapshot =
        serde_json::from_value(record.effective_profile.clone())
            .expect("the durable record must carry the profile it used");
    snapshot.capture
}

/// The thumbnail that is announced is a downscale of THE FRAME THIS CAMERA TOOK.
///
/// The sibling of `the_image_that_is_delivered_is_the_image_the_camera_took`, and it exists for the
/// same reason. A thumbnail is the one thing in the announcement an operator LOOKS at, and a preview
/// of the wrong frame -- a stale one, a neighbouring camera's, a placeholder -- would look completely
/// convincing. Nothing else in the body could reveal it: the dimensions would be right, the byte
/// count would be right, and there is deliberately no digest to check it against.
///
/// So the frame is regenerated independently, straight from the simulator with the same seed and no
/// part of the capture pipeline involved, downscaled here with the same public filter the renderer
/// uses, and compared against the announced picture pixel by pixel. JPEG is lossy, so the comparison
/// is a tolerance -- and the tolerance is shown to have TEETH: the same measurement against the same
/// frame shifted by a few pixels blows past it by an order of magnitude, so a thumbnail that was
/// merely "a plausible picture" could not pass.
#[tokio::test]
async fn the_thumbnail_that_is_announced_is_a_downscale_of_the_frame_the_camera_took() {
    use image::{ImageReader, RgbImage, imageops};

    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a"], false);
    let crate::config::BackendConfig::Sim(sim) = &mut configuration.instances[0].backend else {
        panic!("test fixture must use the simulator backend");
    };
    sim.seed = Some(20_260_714);
    sim.frame.width = 640;
    sim.frame.height = 480;
    // A pattern with STRUCTURE, so that "is this the right picture" is a question with a sharp
    // answer: the flat colour bars barely change when the frame does, and a comparison that cannot
    // tell two different frames apart cannot vouch for one either.
    sim.frame.pattern = crate::config::SimPattern::Checkerboard;
    let backend_config = sim.clone();

    for profile in configuration.instances[0].capture_profiles.values_mut() {
        profile.thumbnail = Some(crate::config::ThumbnailConfig {
            size: crate::config::ThumbnailSize::Medium,
        });
    }

    let (runtime, announcer) = runtime_with_announcer(configuration, &directory).await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            "thumbnail-fidelity".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "thumbnail-fidelity-correlation".to_string(),
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

    // What the component ANNOUNCED -- the only place the preview exists. The durable body it was
    // built from has none, which `the_thumbnail_is_announced_and_is_in_nothing_durable` pins.
    let published = announcer.for_capture(&capture_id);
    assert_eq!(published.len(), 1, "one terminal, one announcement");
    assert!(
        terminal
            .terminal_result
            .as_ref()
            .expect("a succeeded capture commits a terminal body")
            .get("thumbnail")
            .is_none(),
        "the preview must not have reached the durable record"
    );
    let announced: crate::messages::Thumbnail =
        serde_json::from_value(published[0].body["thumbnail"].clone())
            .expect("a profile that asked for a thumbnail must have been announced one");
    assert_eq!(
        (announced.width, announced.height),
        (320, 240),
        "medium bounds the longest edge of a 640x480 frame and keeps its aspect"
    );
    let delivered = ImageReader::new(std::io::Cursor::new(
        announced
            .data_bytes()
            .expect("the announced marker must carry decodable bytes"),
    ))
    .with_guessed_format()
    .expect("the thumbnail must be a recognisable image")
    .decode()
    .expect("the thumbnail must decode")
    .to_rgb8();
    assert_eq!(delivered.dimensions(), (320, 240));

    // What the camera actually produced -- regenerated from the same seed, through the backend
    // itself, with no part of the capture pipeline involved.
    use crate::backend::CameraBackendFactory as _;
    let factory = crate::backend::sim::SimBackendFactory;
    let mut session = factory
        .connect(crate::backend::ConnectRequest {
            instance_id: "camera-a".to_owned(),
            backend: crate::config::BackendConfig::Sim(backend_config),
            timeout: Duration::from_secs(5),
            cancellation: CancellationToken::new(),
        })
        .await
        .expect("the simulator must connect");
    let regenerate = || crate::backend::CaptureRequest {
        capture_id: "regenerated".to_owned(),
        profile: terminal_profile(&terminal),
        maximum_frame_bytes: 8 * 1024 * 1024,
        timeout: Duration::from_secs(5),
        cancellation: CancellationToken::new(),
    };
    let frame = session
        .capture(regenerate())
        .await
        .expect("the simulator must produce its first frame");
    // The SECOND frame of the same camera: the most dangerous near-miss there is, because a stale
    // preview is one the operator has no way at all to recognise as stale.
    let next_frame = session
        .capture(regenerate())
        .await
        .expect("the simulator must produce its second frame");
    session.close().await.ok();
    assert_eq!(frame.pixel_format, crate::model::PixelFormat::Rgb8);
    assert_ne!(
        frame.bytes, next_frame.bytes,
        "the fixture is only useful if the camera's next frame is a different picture"
    );

    let camera_frame = RgbImage::from_raw(640, 480, frame.bytes.to_vec())
        .expect("the regenerated frame is RGB8 640x480");
    let expected = imageops::resize(
        &camera_frame,
        320,
        240,
        imageops::FilterType::Lanczos3,
    );

    // JPEG is lossy, so "the same picture" is a tolerance, not an equality. Quality 80 at 320x240
    // moves a channel by a couple of levels; 8 is comfortably above that and nowhere near the
    // ~60-100 that two DIFFERENT pictures score (asserted below).
    const TOLERANCE: f64 = 8.0;
    assert!(
        mean_channel_difference(&delivered, &expected) < TOLERANCE,
        "the announced thumbnail is not a downscale of the frame the camera took: mean channel \
         difference {:.2} exceeds the {TOLERANCE} a lossy JPEG can explain",
        mean_channel_difference(&delivered, &expected)
    );

    // The tolerance has TEETH, and this is the proof. Two pictures that a careless comparison would
    // happily accept -- the same frame shifted six pixels, and the very next frame this same camera
    // took -- must both be rejected by the same measurement, by a wide margin. Without this, a
    // tolerance of 8 could not be told apart from a tolerance of 800.
    let mut shifted = RgbImage::new(640, 480);
    for (x, y, pixel) in camera_frame.enumerate_pixels() {
        shifted.put_pixel((x + 6) % 640, y, *pixel);
    }
    let next = RgbImage::from_raw(640, 480, next_frame.bytes.to_vec())
        .expect("the camera's next frame is RGB8 640x480");
    for (what, other) in [("a 6-pixel shift of it", shifted), ("the camera's NEXT frame", next)] {
        let difference = mean_channel_difference(
            &delivered,
            &imageops::resize(&other, 320, 240, imageops::FilterType::Lanczos3),
        );
        assert!(
            difference > TOLERANCE * 3.0,
            "the tolerance must reject a picture that is merely PLAUSIBLE, or it proves nothing: \
             {what} scored {difference:.2}, and a {TOLERANCE} tolerance that accepted it would \
             vouch for any preview at all"
        );
    }

    runtime.shutdown().await;
}

/// Mean absolute per-channel difference between two same-sized RGB images, in 0..=255 levels.
fn mean_channel_difference(left: &image::RgbImage, right: &image::RgbImage) -> f64 {
    assert_eq!(
        left.dimensions(),
        right.dimensions(),
        "a difference between differently-sized pictures is not a difference, it is a bug"
    );
    let total: u64 = left
        .pixels()
        .zip(right.pixels())
        .flat_map(|(left, right)| {
            left.0
                .iter()
                .zip(right.0.iter())
                .map(|(left, right)| u64::from(left.abs_diff(*right)))
                .collect::<Vec<_>>()
        })
        .sum();
    let channels = u64::from(left.width()) * u64::from(left.height()) * 3;
    total as f64 / channels as f64
}

/// A group reply carries one body PER MEMBER -- and not one preview among them.
///
/// This is where a preview in the durable record would hurt most: the group aggregate embeds every
/// member's committed terminal body verbatim, so a 60 KiB thumbnail per capture becomes N of them in
/// a single reply. It is also the sharpest statement of the rule, because the aggregate is built
/// from the CATALOG -- if the durable bodies were clean and the aggregate still had previews, or the
/// other way round, this is the test that would say so.
#[tokio::test]
async fn a_group_reply_embeds_every_members_body_and_not_one_thumbnail() {
    let directory = TempDir::new().unwrap();
    let mut configuration = config(directory.path(), &["camera-a", "camera-b"], false);
    for camera in &mut configuration.instances {
        for profile in camera.capture_profiles.values_mut() {
            profile.thumbnail = Some(crate::config::ThumbnailConfig {
                size: crate::config::ThumbnailSize::Large,
            });
        }
    }
    let runtime = runtime(configuration, &directory).await;
    for instance in ["camera-a", "camera-b"] {
        runtime
            .start_supervisor(instance.to_string(), runtime.engine(instance).unwrap())
            .unwrap();
        wait_for_online(&runtime, instance).await;
    }

    let accepted = runtime
        .submit_group(
            group_request("group-of-two", &["camera-a", "camera-b"]),
            "group-preview-correlation".to_string(),
            crate::admission::CapturePriority::Submitted,
            None,
        )
        .await
        .expect("the group must be accepted");
    let terminal = wait_for_group_terminal(&runtime, &accepted.group_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);

    // The exact document a group's caller is answered with.
    let group = group_for(&runtime, "group-of-two")
        .await
        .expect("the durable group must exist");
    let reply = group_terminal_json(&group);
    let members = reply["members"]
        .as_array()
        .expect("a group reply carries its members");
    assert_eq!(members.len(), 2, "both members must be in the reply");
    for member in members {
        assert!(
            member["image"]["sha256"].is_string(),
            "the fixture is only meaningful if these are real, succeeded captures: {member}"
        );
        assert!(
            member.get("thumbnail").is_none(),
            "a group reply must not carry a preview per member: {member}"
        );
    }
    assert!(
        !reply.to_string().contains("thumbnail"),
        "not one preview may appear anywhere in a group reply: {reply}"
    );

    runtime.shutdown().await;
}

/// The thumbnail measures are wired to the metric an operator actually watches.
///
/// A hook that counts nothing is indistinguishable from a hook that is never called, and this
/// component shipped three fully-built subsystems wired to nothing. Both measures go through the
/// real `RuntimeJobHooks` on a real runtime, into the real `CaptureMetrics`, and are read back off
/// the metric target -- the same path `announcementFailed` takes.
#[tokio::test]
async fn a_thumbnail_that_fails_or_is_dropped_is_counted_on_the_capture_metric() {
    use crate::jobs::JobHooks as _;

    let directory = TempDir::new().unwrap();
    let (runtime, recorder) =
        runtime_with_metrics(config(directory.path(), &["camera-a"], false), &directory).await;

    runtime.waiters.thumbnail_failed().await;
    runtime.waiters.thumbnail_dropped().await;
    runtime.waiters.thumbnail_dropped().await;

    // The counts accumulate on the hooks; a drain emits their Total/Interval pairs.
    runtime.metrics.drain_captures().await;

    assert_eq!(
        recorder.counts(
            crate::observability::CAPTURE_METRIC,
            &format!("{}Total", crate::observability::THUMBNAIL_FAILED_MEASURE)
        ),
        1.0,
        "a thumbnail that could not be rendered must be visible outside the process"
    );
    assert_eq!(
        recorder.counts(
            crate::observability::CAPTURE_METRIC,
            &format!("{}Total", crate::observability::THUMBNAIL_DROPPED_MEASURE)
        ),
        2.0,
        "and so must every thumbnail that did not fit the ceiling"
    );
    assert!(
        edgecommons::metrics::MetricService::is_metric_defined(
            recorder.as_ref(),
            crate::observability::CAPTURE_METRIC
        ),
        "the measures must be DEFINED on the capture metric, not emitted into a metric that has \
         never heard of them"
    );

    runtime.shutdown().await;
}

/// The lifecycle verbs suspend a camera's new capture work and surface the state in `sb/status`.
///
/// `sb/pause` / `sb/resume` are the standardized lifecycle verbs (SOUTHBOUND.md §2.2). Pausing a
/// camera refuses NEW capture work with a stable `INSTANCE_PAUSED` code while leaving its sibling
/// untouched; the toggle is idempotent (`changed` reports whether it moved); and the paused flag is
/// visible where an operator and the overview panel read status. Resuming clears it.
#[tokio::test]
async fn pause_and_resume_suspend_new_capture_work_and_surface_in_status() {
    let (port, _broker) = spawn_recording_mqtt_broker().await;
    let directory = TempDir::new().unwrap();
    let runtime = runtime(config(directory.path(), &["camera-a", "camera-b"], false), &directory).await;
    let (_app, deferred) = command_deferred_registry(&directory, port).await;

    // Pause camera-a — idempotent, and it names the instance it moved.
    let paused = immediate_success(
        runtime
            .handle_camera_command(
                "sb/pause",
                command_message("sb/pause", "p1", json!({ "instance": "camera-a" })),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(paused["id"], json!("camera-a"));
    assert_eq!(paused["paused"], json!(true));
    assert_eq!(paused["changed"], json!(true));

    let again = immediate_success(
        runtime
            .handle_camera_command(
                "sb/pause",
                command_message("sb/pause", "p2", json!({ "instance": "camera-a" })),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(again["changed"], json!(false), "pausing an already-paused camera changes nothing");

    // Status carries the paused flag, per instance and in the fleet listing.
    let status = immediate_success(
        runtime
            .handle_camera_command(
                "sb/status",
                command_message("sb/status", "s1", json!({ "instance": "camera-a" })),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(status["paused"], json!(true), "a paused camera says so in its status");

    let fleet = immediate_success(
        runtime
            .handle_camera_command(
                "sb/status",
                command_message("sb/status", "s2", json!({})),
                deferred.clone(),
            )
            .await,
    );
    let cameras = fleet["cameras"].as_array().expect("a fleet status lists every camera");
    let a = cameras.iter().find(|c| c["instance"] == json!("camera-a")).unwrap();
    let b = cameras.iter().find(|c| c["instance"] == json!("camera-b")).unwrap();
    assert_eq!(a["paused"], json!(true));
    assert_eq!(b["paused"], json!(false), "pausing one camera must not pause its siblings");

    // New capture work for the paused camera is refused with the stable code; the sibling still works.
    let refused = runtime
        .submit_capture(
            "camera-a".to_string(),
            "while-paused".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "while-paused-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect_err("a paused camera must refuse new capture work");
    assert_eq!(refused.code(), crate::ErrorCode::InstancePaused);

    runtime
        .submit_capture(
            "camera-b".to_string(),
            "sibling-ok".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "sibling-ok-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("an un-paused sibling must still accept work");

    // Resume clears it, and capture work is admitted again.
    let resumed = immediate_success(
        runtime
            .handle_camera_command(
                "sb/resume",
                command_message("sb/resume", "r1", json!({ "instance": "camera-a" })),
                deferred.clone(),
            )
            .await,
    );
    assert_eq!(resumed["paused"], json!(false));
    assert_eq!(resumed["changed"], json!(true));

    runtime
        .submit_capture(
            "camera-a".to_string(),
            "after-resume".to_string(),
            None,
            None,
            serde_json::Map::new(),
            "after-resume-correlation".to_string(),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .expect("a resumed camera accepts capture work again");

    runtime.shutdown().await;
}

/// The panel trio is registered with the ids, orders, and instance scope the baseline prescribes.
#[test]
fn the_panel_trio_is_registered_with_the_right_ids_orders_and_scope() {
    let panels = crate::runtime::camera_panels();
    let ids: Vec<&str> = panels.iter().map(|p| p["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["overview", "signals", "diagnostics"]);
    let orders: Vec<u64> = panels.iter().map(|p| p["order"].as_u64().unwrap()).collect();
    assert_eq!(orders, vec![10, 20, 30]);
    for panel in &panels {
        assert_eq!(panel["scope"], json!("instance"), "every panel is instance-scoped");
    }
    // The overview panel binds the lifecycle verbs the adapter actually serves.
    assert_eq!(
        panels[0]["verbs"],
        json!(["sb/status", "sb/reconnect", "sb/pause", "sb/resume"])
    );
    assert_eq!(panels[2]["verbs"], json!(["sb/discover", "sb/queue-status"]));
}
