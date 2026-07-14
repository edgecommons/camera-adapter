//! The schedule plane: work the component gives itself.
//!
//! Cron-driven captures and synchronised group captures, plus periodic discovery. A schedule is
//! just another producer of captures, so these call straight into the command plane.

use super::*;

impl CameraRuntime {

    pub(super) fn start_schedulers(self: &Arc<Self>) -> Result<()> {
        let config = self.config_snapshot()?;
        for camera in &config.instances {
            if !camera.enabled {
                continue;
            }
            for schedule in camera.schedules.iter().filter(|schedule| schedule.enabled) {
                let plan = SchedulePlan::compile(camera.id.clone(), schedule)?;
                self.start_schedule_plan(plan)?;
            }
        }
        // One task per group schedule -- not one per (camera, schedule) pair. A group fires once,
        // as one thing; N member tasks racing the same cron would be N groups, or one group and
        // N-1 duplicate submissions.
        for schedule in config
            .global
            .capture_group_schedules
            .iter()
            .filter(|schedule| schedule.enabled)
        {
            let plan = SchedulePlan::compile_group(schedule)?;
            self.start_schedule_plan(plan)?;
        }
        Ok(())
    }


    fn start_schedule_plan(self: &Arc<Self>, plan: SchedulePlan) -> Result<()> {
        let key = plan.key_parts();
        let cancellation = CancellationToken::new();
        let previous = self
            .scheduler_cancellations
            .write()
            .map_err(|_| {
                crate::CameraError::Catalog("schedule task map is unavailable".to_string())
            })?
            .insert(key, cancellation.clone());
        if let Some(previous) = previous {
            previous.cancel();
        }
        let runtime = Arc::clone(self);
        self.spawn_task(async move {
            match plan.scope() {
                crate::scheduler::ScheduleScope::Camera(_) => {
                    runtime.run_schedule(plan, cancellation).await;
                }
                crate::scheduler::ScheduleScope::Group(_) => {
                    runtime.run_group_schedule(plan, cancellation).await;
                }
            }
        })
    }


    pub(super) fn restart_schedulers(self: &Arc<Self>) -> Result<()> {
        let cancellations = self
            .scheduler_cancellations
            .write()
            .map_err(|_| {
                crate::CameraError::Catalog("schedule task map is unavailable".to_string())
            })?
            .drain()
            .map(|(_, cancellation)| cancellation)
            .collect::<Vec<_>>();
        for cancellation in cancellations {
            cancellation.cancel();
        }
        self.start_schedulers()
    }


    pub(super) fn start_periodic_discovery(self: &Arc<Self>) -> Result<()> {
        let config = self.config_snapshot()?;
        if !config.global.discovery.enabled {
            return Ok(());
        }
        let cancellation = self.cancellation.child_token();
        let previous = self
            .discovery_cancellation
            .write()
            .map_err(|_| {
                crate::CameraError::Catalog("discovery cancellation is unavailable".to_string())
            })?
            .replace(cancellation.clone());
        if let Some(previous) = previous {
            previous.cancel();
        }
        let runtime = Arc::clone(self);
        self.spawn_task(async move {
            runtime.run_periodic_discovery(cancellation).await;
        })
    }


    /// Cancels the previous discovery generation even when reporting is disabled: stale network
    /// observations must not survive a policy disable/re-enable boundary.
    pub(super) fn restart_periodic_discovery(self: &Arc<Self>) -> Result<()> {
        let previous = self
            .discovery_cancellation
            .write()
            .map_err(|_| {
                crate::CameraError::Catalog("discovery cancellation is unavailable".to_string())
            })?
            .take();
        if let Some(previous) = previous {
            previous.cancel();
        }
        if let Ok(mut cache) = self.discovery_cache.lock() {
            cache.candidates.clear();
        }
        self.start_periodic_discovery()
    }


    async fn run_periodic_discovery(self: Arc<Self>, cancellation: CancellationToken) {
        loop {
            if cancellation.is_cancelled() || self.reloading.load(Ordering::Acquire) {
                if cancellation.is_cancelled() {
                    return;
                }
            } else {
                let config = match self.config_snapshot() {
                    Ok(config) => config,
                    Err(error) => {
                        tracing::error!(error = %error, "periodic discovery lost runtime configuration");
                        return;
                    }
                };
                if !config.global.discovery.enabled {
                    return;
                }
                match self
                    .discover_candidates(
                        &config,
                        None,
                        Duration::from_millis(config.global.timeouts.connect_ms),
                        cancellation.child_token(),
                    )
                    .await
                {
                    Ok(candidates) if !cancellation.is_cancelled() => {
                        if let Ok(mut cache) = self.discovery_cache.lock() {
                            cache.candidates = candidates;
                        }
                    }
                    Ok(_) => return,
                    Err(error) => {
                        tracing::warn!(error = %error, "bounded periodic camera discovery failed");
                    }
                }
            }
            let interval = match self.config_snapshot() {
                Ok(config) => Duration::from_secs(config.global.discovery.interval_seconds),
                Err(_) => return,
            };
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = self.cancellation.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }


    pub(super) async fn run_schedule(
        self: Arc<Self>,
        plan: SchedulePlan,
        schedule_cancellation: CancellationToken,
    ) {
        let (instance, schedule_id) = plan.key_parts();
        let now = chrono::Utc::now();
        let mut last_consumed = match self
            .catalog
            .latest_schedule_occurrence(instance.clone(), schedule_id.clone())
            .await
        {
            Ok(Some(milliseconds)) => chrono::DateTime::from_timestamp_millis(milliseconds)
                // A corrupt-but-schema-valid out-of-range timestamp must not turn into an
                // unbounded cron search.  Start cleanly and leave the corrupt row unavailable
                // for re-admission because the catalog dedupe key still owns it.
                .unwrap_or_else(|| now - chrono::Duration::seconds(1)),
            Ok(None) => now - chrono::Duration::seconds(1),
            Err(error) => {
                tracing::error!(
                    instance = %instance,
                    schedule_id = %schedule_id,
                    error = %error,
                    "camera schedule could not load its durable recovery cursor"
                );
                return;
            }
        };
        loop {
            if self.cancellation.is_cancelled() || schedule_cancellation.is_cancelled() {
                return;
            }
            if self.reloading.load(Ordering::Acquire) {
                tokio::select! {
                    _ = self.cancellation.cancelled() => return,
                    _ = schedule_cancellation.cancelled() => return,
                    _ = tokio::time::sleep(SCHEDULER_POLL_INTERVAL) => continue,
                }
            }
            let now = chrono::Utc::now();
            // Decide first, then ask the catalog -- and only if the answer can still change
            // anything.
            //
            // This loop used to open with `has_schedule_overlap`, a `jobs_page(.., 1_000)` that
            // rebuilds and re-prepares its SQL, on every 200 ms tick of every schedule, before
            // anything had established that an occurrence was even due. At 256 cameras that is
            // ~1,280 catalog reads a second, funnelled through the same two connections that carry
            // the capture path's fsync-per-write transactions. Nothing was due on virtually all of
            // those ticks, and an overlap observation cannot make a not-due schedule due: it is read
            // in exactly one branch of `evaluate`, and only ever turns an `Admit` into a
            // `SkippedOverlap`. So the entire read volume bought one thing -- contention.
            //
            // Evaluating with `false` first is therefore exact, not an approximation: the only
            // decision an overlap can alter is `Admit`, so that is the only one worth asking about.
            let mut decision = plan.evaluate(last_consumed, now, SCHEDULER_MISFIRE_GRACE, false);
            let admitted = match &decision {
                Ok(ScheduleDecision::Admit {
                    occurrence,
                    consumed,
                }) if plan.skips_on_overlap() => Some((occurrence.clone(), *consumed)),
                _ => None,
            };
            if let Some((occurrence, consumed)) = admitted {
                let overlap = match self.has_schedule_overlap(&instance, &schedule_id).await {
                    Ok(overlap) => overlap,
                    Err(error) => {
                        tracing::warn!(
                            instance = %instance,
                            schedule_id = %schedule_id,
                            error = %error,
                            "camera schedule could not evaluate overlap"
                        );
                        false
                    }
                };
                if overlap {
                    decision = Ok(ScheduleDecision::SkippedOverlap {
                        occurrence,
                        consumed,
                    });
                }
            }
            match decision {
                Ok(ScheduleDecision::NotDue) => {}
                Ok(ScheduleDecision::SkippedMisfire { latest, consumed }) => {
                    last_consumed = latest.intended_fire_time;
                    tracing::info!(
                        instance = %instance,
                        schedule_id = %schedule_id,
                        intended_fire_time = %latest.intended_fire_time,
                        consumed,
                        "camera schedule skipped a misfire"
                    );
                }
                Ok(ScheduleDecision::SkippedOverlap {
                    occurrence,
                    consumed,
                }) => {
                    last_consumed = occurrence.intended_fire_time;
                    tracing::info!(
                        instance = %instance,
                        schedule_id = %schedule_id,
                        intended_fire_time = %occurrence.intended_fire_time,
                        consumed,
                        "camera schedule skipped an overlapping occurrence"
                    );
                }
                Ok(ScheduleDecision::Admit {
                    occurrence,
                    consumed,
                }) => {
                    last_consumed = occurrence.intended_fire_time;
                    if let Err(error) = self.submit_scheduled(&occurrence).await {
                        // The occurrence is consumed even when capacity or the backend policy
                        // rejects it. Repeating it would violate the scheduler's one-occurrence
                        // guarantee; a new cron occurrence will be evaluated normally.
                        tracing::warn!(
                            instance = %instance,
                            schedule_id = %schedule_id,
                            intended_fire_time = %occurrence.intended_fire_time,
                            consumed,
                            error = %error,
                            "camera schedule occurrence was not admitted"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(
                        instance = %instance,
                        schedule_id = %schedule_id,
                        error = %error,
                        "camera schedule evaluation failed"
                    );
                }
            }
            tokio::select! {
                _ = self.cancellation.cancelled() => return,
                _ = schedule_cancellation.cancelled() => return,
                _ = tokio::time::sleep(SCHEDULER_POLL_INTERVAL) => {}
            }
        }
    }


    /// Runs one group schedule: fire a synchronised capture across several cameras on a cron.
    ///
    /// The occurrence is submitted through [`Self::submit_group`] -- the same path the
    /// `sb/capture-group` command takes -- so a scheduled group is indistinguishable from a
    /// commanded one: one durable group row, all-or-nothing acceptance, one collated terminal
    /// notification. What makes that safe to do from a scheduler is that `submit_group` is already
    /// idempotent on its request id, and this loop derives that id from the schedule and the
    /// intended fire time. An occurrence is therefore admitted exactly once even if the component
    /// crashes between submitting it and recording that it did.
    ///
    /// This shipped only once the fleet queue existed. On the old fire-all-and-hope dispatch, a
    /// scheduled group larger than the effective capacity would have timed out its surplus members
    /// on every tick, forever, writing a durable failure row each time.
    async fn run_group_schedule(
        self: Arc<Self>,
        plan: SchedulePlan,
        schedule_cancellation: CancellationToken,
    ) {
        let (_, schedule_id) = plan.key_parts();
        let now = chrono::Utc::now();
        let cursor = match self
            .catalog
            .group_schedule_cursor(schedule_id.clone())
            .await
        {
            Ok(cursor) => cursor,
            Err(error) => {
                tracing::error!(
                    schedule_id = %schedule_id,
                    error = %error,
                    "group schedule could not load its durable recovery cursor"
                );
                return;
            }
        };
        let mut last_consumed = cursor
            .as_ref()
            .and_then(|cursor| {
                chrono::DateTime::from_timestamp_millis(cursor.intended_fire_time_ms)
            })
            .unwrap_or_else(|| now - chrono::Duration::seconds(1));
        let mut last_group_id = cursor.and_then(|cursor| cursor.last_group_id);
        loop {
            if self.cancellation.is_cancelled() || schedule_cancellation.is_cancelled() {
                return;
            }
            if self.reloading.load(Ordering::Acquire) {
                tokio::select! {
                    _ = self.cancellation.cancelled() => return,
                    _ = schedule_cancellation.cancelled() => return,
                    _ = tokio::time::sleep(SCHEDULER_POLL_INTERVAL) => continue,
                }
            }
            let now = chrono::Utc::now();
            // Decide first, and only ask about overlap when the answer can still change something
            // -- the same rule the camera loop learned the hard way in B6. An overlap observation
            // can only ever turn an `Admit` into a `SkippedOverlap`.
            let mut decision = plan.evaluate(last_consumed, now, SCHEDULER_MISFIRE_GRACE, false);
            let admitted = match &decision {
                Ok(ScheduleDecision::Admit {
                    occurrence,
                    consumed,
                }) if plan.skips_on_overlap() => Some((occurrence.clone(), *consumed)),
                _ => None,
            };
            if let Some((occurrence, consumed)) = admitted {
                // Evaluated against the GROUP, not its members: the previous occurrence is
                // outstanding until every camera in it is terminal.
                if self.group_schedule_overlaps(last_group_id.as_deref()).await {
                    decision = Ok(ScheduleDecision::SkippedOverlap {
                        occurrence,
                        consumed,
                    });
                }
            }
            match decision {
                Ok(ScheduleDecision::NotDue) => {}
                Ok(ScheduleDecision::SkippedMisfire { latest, consumed }) => {
                    last_consumed = latest.intended_fire_time;
                    self.record_group_occurrence(&schedule_id, latest.intended_fire_time, None)
                        .await;
                    tracing::info!(
                        schedule_id = %schedule_id,
                        intended_fire_time = %latest.intended_fire_time,
                        consumed,
                        "group schedule skipped a misfire"
                    );
                }
                Ok(ScheduleDecision::SkippedOverlap {
                    occurrence,
                    consumed,
                }) => {
                    last_consumed = occurrence.intended_fire_time;
                    self.record_group_occurrence(&schedule_id, occurrence.intended_fire_time, None)
                        .await;
                    tracing::info!(
                        schedule_id = %schedule_id,
                        intended_fire_time = %occurrence.intended_fire_time,
                        consumed,
                        "group schedule skipped an occurrence whose previous group is still running"
                    );
                }
                Ok(ScheduleDecision::Admit {
                    occurrence,
                    consumed,
                }) => {
                    last_consumed = occurrence.intended_fire_time;
                    match self.submit_scheduled_group(&occurrence).await {
                        Ok(group_id) => {
                            last_group_id = Some(group_id.clone());
                            self.record_group_occurrence(
                                &schedule_id,
                                occurrence.intended_fire_time,
                                Some(group_id),
                            )
                            .await;
                        }
                        Err(error) => {
                            // The occurrence is consumed even when it could not be admitted, exactly
                            // as a camera schedule consumes one: repeating it would violate the
                            // one-occurrence guarantee. The next cron occurrence is evaluated
                            // normally.
                            self.record_group_occurrence(
                                &schedule_id,
                                occurrence.intended_fire_time,
                                None,
                            )
                            .await;
                            tracing::warn!(
                                schedule_id = %schedule_id,
                                intended_fire_time = %occurrence.intended_fire_time,
                                consumed,
                                error = %error,
                                "group schedule occurrence was not admitted"
                            );
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(
                        schedule_id = %schedule_id,
                        error = %error,
                        "group schedule evaluation failed"
                    );
                }
            }
            tokio::select! {
                _ = self.cancellation.cancelled() => return,
                _ = schedule_cancellation.cancelled() => return,
                _ = tokio::time::sleep(SCHEDULER_POLL_INTERVAL) => {}
            }
        }
    }


    /// Whether this schedule's previous group is still running.
    ///
    /// One primary-key lookup, and only when an occurrence is actually due. A group that has been
    /// pruned by retention is long terminal, so a missing row is not an overlap.
    pub(super) async fn group_schedule_overlaps(&self, last_group_id: Option<&str>) -> bool {
        let Some(group_id) = last_group_id else {
            return false;
        };
        match self.catalog.group(group_id.to_owned()).await {
            Ok(Some(group)) => !group.state.is_terminal(),
            Ok(None) => false,
            Err(error) => {
                // Fail closed: a catalog we cannot read is not a licence to pile a second group on
                // top of one that may still be running.
                tracing::warn!(
                    group_id = %group_id,
                    error = %error,
                    "group schedule could not evaluate overlap and skipped the occurrence"
                );
                true
            }
        }
    }


    pub(super) async fn record_group_occurrence(
        &self,
        schedule_id: &str,
        intended_fire_time: chrono::DateTime<chrono::Utc>,
        group_id: Option<String>,
    ) {
        if let Err(error) = self
            .catalog
            .record_group_schedule_occurrence(
                schedule_id.to_owned(),
                intended_fire_time.timestamp_millis(),
                group_id,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
        {
            // The cursor is a recovery hint, not the authority -- the command ledger is. Losing this
            // write costs a redundant submission after a restart, which the ledger absorbs.
            tracing::warn!(
                schedule_id = %schedule_id,
                error = %error,
                "group schedule could not record its recovery cursor"
            );
        }
    }


    /// Submits one group-schedule occurrence as an ordinary capture group.
    pub(super) async fn submit_scheduled_group(&self, occurrence: &ScheduleOccurrence) -> Result<String> {
        let config = self.config_snapshot()?;
        let schedule = config
            .global
            .capture_group_schedules
            .iter()
            .find(|schedule| schedule.id == occurrence.schedule_id && schedule.enabled)
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::InvalidRequest,
                    "group schedule is no longer enabled",
                )
            })?;
        // Derived, not random: this is what makes the occurrence exactly-once. The same occurrence
        // always produces the same request id, and `submit_group` answers a repeat with the group it
        // already accepted rather than a second one.
        let request_id = format!(
            "schedule:{}:{}",
            schedule.id,
            occurrence.intended_fire_time.timestamp_millis()
        );
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "scheduleId".to_string(),
            serde_json::Value::String(schedule.id.clone()),
        );
        metadata.insert(
            "intendedFireTime".to_string(),
            serde_json::Value::String(
                occurrence
                    .intended_fire_time
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            ),
        );
        let body = crate::commands::GroupCaptureRequest {
            request_id,
            instances: schedule.instances.clone(),
            capture_profile: schedule.capture_profile.clone(),
            profile_overrides: schedule.profile_overrides.clone(),
            timeout_ms: schedule.timeout_ms,
            metadata,
        };
        let correlation_id = format!("sched_{}", uuid::Uuid::now_v7());
        let group = self
            .submit_group(
                body,
                correlation_id,
                crate::admission::CapturePriority::Scheduled,
                None,
            )
            .await?;
        tracing::info!(
            schedule_id = %schedule.id,
            group_id = %group.group_id,
            members = schedule.instances.len(),
            intended_fire_time = %occurrence.intended_fire_time,
            "group schedule admitted a synchronised capture"
        );
        Ok(group.group_id)
    }


    pub(super) async fn has_schedule_overlap(&self, instance: &str, schedule_id: &str) -> Result<bool> {
        let states = vec![
            crate::model::JobState::Accepted,
            crate::model::JobState::Queued,
            crate::model::JobState::Acquiring,
            crate::model::JobState::Encoding,
            crate::model::JobState::Persisting,
        ];
        let mut before = None;
        loop {
            let page = self
                .catalog
                .jobs_page(
                    Some(instance.to_owned()),
                    states.clone(),
                    before.clone(),
                    1_000,
                )
                .await?;
            if page.iter().any(|record| {
                record.trigger.get("type")
                    == Some(&serde_json::Value::String("schedule".to_string()))
                    && record.trigger.get("scheduleId")
                        == Some(&serde_json::Value::String(schedule_id.to_owned()))
            }) {
                return Ok(true);
            }
            let Some(last) = page.last() else {
                return Ok(false);
            };
            if page.len() < 1_000 {
                return Ok(false);
            }
            before = Some((last.accepted_at_ms, last.capture_id.clone()));
        }
    }


    async fn emit_schedule_skipped(&self, occurrence: &ScheduleOccurrence) {
        let Some(instance) = occurrence.scope.camera() else {
            return;
        };
        let event = self
            .cameras
            .read()
            .ok()
            .and_then(|cameras| cameras.get(instance).and_then(|slot| slot.events.clone()));
        if let Some(event) = event {
            let _ = event
                .emit(
                    Severity::Warning,
                    "schedule-skipped",
                    Some("scheduled capture skipped because camera is moving".to_string()),
                    Some(serde_json::json!({
                        "scheduleId": occurrence.schedule_id,
                        "intendedFireTime": occurrence.intended_fire_time,
                        "code": "CAMERA_MOVING",
                    })),
                )
                .await;
        }
    }


    pub(super) async fn submit_scheduled(&self, occurrence: &ScheduleOccurrence) -> Result<()> {
        let instance = occurrence.scope.camera().ok_or_else(|| {
            crate::CameraError::Catalog(
                "a group-schedule occurrence cannot be submitted as a single camera capture"
                    .to_string(),
            )
        })?;
        let config = self.config_snapshot()?;
        let camera = self.registry.camera_config(instance)?;
        if !camera.enabled {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::CameraDisabled,
                "camera was disabled before scheduled admission",
            ));
        }
        let schedule = camera
            .schedules
            .iter()
            .find(|schedule| schedule.id == occurrence.schedule_id && schedule.enabled)
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::InvalidRequest,
                    "schedule is no longer enabled for this camera",
                )
            })?;
        let profile = camera
            .capture_profiles
            .get(&schedule.capture_profile)
            .cloned()
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::UnknownCaptureProfile,
                    "scheduled capture profile is not configured",
                )
            })?;
        if profile
            .capture_interlock
            .unwrap_or(camera.ptz.capture_interlock)
            == crate::config::CaptureInterlock::Reject
        {
            if let Ok(actor) = self.actor(instance) {
                if matches!(
                    actor
                        .ptz(
                            crate::model::PtzRequest::Status,
                            tokio::time::Instant::now()
                                + Duration::from_millis(config.global.timeouts.ptz_ms),
                            &self.cancellation,
                        )
                        .await,
                    Ok(crate::model::PtzResult::Status(status)) if status.moving == Some(true)
                ) {
                    self.emit_schedule_skipped(occurrence).await;
                    return Ok(());
                }
            }
        }
        let accepted_at_ms = chrono::Utc::now().timestamp_millis();
        let terminal_ms = profile
            .timeout_ms
            .unwrap_or(config.global.timeouts.job_terminal_ms);
        let capture_mode = profile
            .capture_mode
            .unwrap_or_else(|| match &camera.backend {
                crate::config::BackendConfig::Sim(_) => crate::model::CaptureMode::Simulated,
                crate::config::BackendConfig::GenicamAravis(_) => {
                    crate::model::CaptureMode::SoftwareTrigger
                }
                crate::config::BackendConfig::OnvifRtsp(config) => config.capture_mode,
            });
        let capture_id = format!("cap_{}", uuid::Uuid::now_v7());
        let deadlines = crate::catalog::JobDeadlines {
            terminal_at_ms: accepted_at_ms
                .saturating_add(i64::try_from(terminal_ms).unwrap_or(i64::MAX)),
            queue_at_ms: profile.queue_expiry_ms.map(|duration| {
                accepted_at_ms.saturating_add(i64::try_from(duration).unwrap_or(i64::MAX))
            }),
            capture_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.capture_ms).unwrap_or(i64::MAX),
            ),
            encode_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.encode_ms).unwrap_or(i64::MAX),
            ),
            persist_at_ms: accepted_at_ms.saturating_add(
                i64::try_from(config.global.timeouts.persist_ms).unwrap_or(i64::MAX),
            ),
        };
        let relative_path = crate::storage::render_output_path(
            &config.global.output,
            crate::storage::OutputPathVariables {
                camera_id: instance,
                capture_id: &capture_id,
                timestamp: chrono::Utc::now(),
            },
            profile.output.encoding,
        )?;
        let snapshot = self.registry.snapshot(instance)?;
        let camera_summary = crate::messages::CameraSummary {
            backend: snapshot.backend,
            vendor: snapshot
                .capabilities
                .as_ref()
                .and_then(|capabilities| capabilities.vendor.clone()),
            model: snapshot
                .capabilities
                .as_ref()
                .and_then(|capabilities| capabilities.model.clone()),
            firmware: snapshot
                .capabilities
                .as_ref()
                .and_then(|capabilities| capabilities.firmware.clone()),
            serial: snapshot
                .capabilities
                .as_ref()
                .and_then(|capabilities| capabilities.serial.clone()),
        };
        let profile_snapshot = crate::jobs::JobProfileSnapshot {
            name: schedule.capture_profile.clone(),
            capture: profile.clone(),
            // The binding deliberately gives schedules a fail-fast default even when direct
            // capture defaults to wait-until-deadline.
            offline_policy: profile
                .offline_policy
                .unwrap_or(crate::config::OfflinePolicy::FailFast),
            maximum_frame_bytes: profile
                .maximum_frame_bytes
                .unwrap_or(config.global.limits.max_frame_bytes_per_camera),
            capture_mode,
            capture_interlock: profile
                .capture_interlock
                .unwrap_or(camera.ptz.capture_interlock),
            settle_ms: camera.ptz.settle_ms,
        };
        let trigger = crate::messages::CaptureTrigger::Schedule {
            schedule_id: occurrence.schedule_id.clone(),
            intended_fire_time: occurrence.intended_fire_time,
        };
        let correlation_id = uuid::Uuid::now_v7().to_string();
        let canonical = serde_json::json!({
            "scheduleId": occurrence.schedule_id,
            "intendedFireTime": occurrence.intended_fire_time,
            "captureProfile": schedule.capture_profile,
            "effectiveProfile": profile_snapshot,
            "deadlines": {
                "terminalAtMs": deadlines.terminal_at_ms,
                "queueAtMs": deadlines.queue_at_ms,
                "captureAtMs": deadlines.capture_at_ms,
                "encodeAtMs": deadlines.encode_at_ms,
                "persistAtMs": deadlines.persist_at_ms,
            },
            "intendedOutput": {
                "relativePath": relative_path.as_wire_path(),
                "backend": snapshot.backend.as_str(),
            },
        });
        let submission = crate::jobs::JobSubmission {
            job: crate::catalog::NewJob {
                capture_id: capture_id.clone(),
                instance: instance.to_owned(),
                ledger_key: None,
                request_hash: crate::idempotency::canonical_request_hash(&canonical, false)?,
                canonical_request: canonical,
                effective_profile: serde_json::to_value(&profile_snapshot)?,
                deadlines: deadlines.clone(),
                trigger: serde_json::to_value(&trigger)?,
                origin_correlation_id: None,
                intended_output: serde_json::json!({
                    "relativePath": relative_path.as_wire_path(),
                    "backend": snapshot.backend.as_str(),
                }),
                accepted_at_ms,
                group_id: None,
            },
            spec: crate::jobs::CaptureJobSpec {
                capture_id: capture_id.clone(),
                instance: instance.to_owned(),
                profile: profile_snapshot,
                resource_group: camera.resource_group.clone(),
                relative_path,
                deadlines,
                accepted_at_ms,
                trigger,
                correlation_id,
                metadata: serde_json::Map::new(),
                camera: camera_summary,
                group_size: None,
            },
            priority: crate::admission::CapturePriority::Scheduled,
        };
        self.ensure_storage_capacity().await?;
        let outcome = self
            .catalog
            .accept_scheduled_job(
                submission.job.clone(),
                occurrence.schedule_id.clone(),
                occurrence.intended_fire_time.timestamp_millis(),
            )
            .await?;
        if matches!(outcome, crate::catalog::AcceptJobOutcome::Inserted(_)) {
            let dispatcher = self.dispatcher(instance)?;
            self.engine(instance)?
                .queue_preaccepted(&dispatcher, submission)
                .await?;
        }
        Ok(())
    }
}
