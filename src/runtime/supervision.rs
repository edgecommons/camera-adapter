//! The supervision plane: keeping cameras connected and the durable machinery running.
//!
//! One supervisor per camera -- connect, back off, hold the actor, retire -- plus the periodic
//! workers that are nobody's command: the metric sampler, the fleet capture scheduler, storage
//! pressure, retention, catalog health, and startup recovery.

use super::*;

impl CameraRuntime {

    /// Starts cooperative shutdown.  Tasks are joined only within the configured grace period so the
    /// process cannot hang behind a native backend.
    pub async fn shutdown(&self) {
        self.readiness.begin_shutdown();
        // DESIGN 20.2 step 2, and it has to happen HERE -- before the cancellation, while the actors
        // are still there to deliver through. The actor lets a safety stop outlive its own
        // cancellation precisely so that this one lands.
        self.deliver_mandatory_stops(None);
        self.cancellation.cancel();
        let grace = match self.config_snapshot() {
            Ok(config) => Duration::from_millis(config.global.timeouts.shutdown_grace_ms),
            Err(_) => Duration::from_secs(30),
        };
        let tasks = self
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks));
        let Ok(tasks) = tasks else {
            return;
        };
        let join = async move {
            for task in tasks {
                let _ = task.await;
            }
        };
        let _ = tokio::time::timeout(grace, join).await;
    }


    /// Publishes a camera state transition, and says so when it does not take.
    ///
    /// D5. `CameraRegistry::update` has two failure channels and every supervisor call site
    /// discarded both with `let _ =`:
    ///
    /// * `Err` is a poisoned registry lock. The component's camera state is now unreadable, every
    ///   subsequent transition will be lost, and nothing said a word.
    /// * `Ok(false)` is the generation fence doing its job -- this supervisor has been superseded by
    ///   a newer generation (or its camera is gone), and its update was deliberately dropped. That is
    ///   correct, and it is also exactly what an operator staring at a camera stuck in the wrong
    ///   state needs to be told, because the alternative explanation is a bug.
    ///
    /// Neither changes control flow: a superseded supervisor is already on its way out, and a
    /// poisoned lock is not something a camera actor can do anything about. What changes is that
    /// both are now visible.
    pub(super) fn publish_camera_state(
        &self,
        instance: &str,
        generation: u64,
        state: CameraConnectionState,
        capabilities: Option<crate::model::CameraCapabilities>,
        last_error: Option<CameraStatusError>,
        observed_at: chrono::DateTime<chrono::Utc>,
    ) {
        // Read before the write: the transition is what the health metric reports on, and after the
        // update there is nothing left to compare against.
        let previous = self
            .registry
            .snapshot(instance)
            .ok()
            .map(|snapshot| snapshot.state);
        match self.registry.update(
            instance,
            generation,
            state,
            capabilities,
            last_error,
            observed_at,
        ) {
            Ok(true) => self.observe_connectivity(instance, previous, state),
            Ok(false) => tracing::debug!(
                instance,
                generation,
                ?state,
                "camera state update was dropped: a newer generation owns this camera, or it is no longer configured"
            ),
            Err(error) => tracing::error!(
                instance,
                generation,
                ?state,
                error = %error,
                "camera registry is unavailable; this camera's state can no longer be published"
            ),
        }
    }


    /// Builds one `camera_queue` sample.
    ///
    /// Separated from the timer loop that calls it because a loop that fires every 30 seconds cannot
    /// be asserted on in a unit test, and "the numbers the operator sees are the numbers the
    /// component holds" is exactly the part worth asserting.
    pub(super) async fn sample_queue_metric(&self) -> Result<std::collections::HashMap<String, f64>> {
        let status = self.queue_status(None).await?;
        let configured = status.cameras.len();
        let online = self
            .registry
            .snapshots(configured.max(1))
            .map(|snapshots| {
                snapshots
                    .into_iter()
                    .filter(|snapshot| snapshot.state == CameraConnectionState::Online)
                    .count()
            })
            .unwrap_or_default();
        let mut values = std::collections::HashMap::new();
        #[allow(clippy::cast_precision_loss)]
        // Counts and byte budgets; f64 is the metric wire type.
        {
            let mut put = |name: &str, value: f64| {
                values.insert(name.to_owned(), value);
            };
            put("dispatchQueued", status.dispatch_queued as f64);
            put("durableBacklog", status.durable_backlog as f64);
            put("durableInFlight", status.durable_in_flight as f64);
            put(
                "availableAcquisitions",
                status.admission.available_acquisitions as f64,
            );
            put(
                "availableEncoders",
                status.admission.available_encoders as f64,
            );
            put(
                "availableWriters",
                status.admission.available_writers as f64,
            );
            put(
                "availableMemoryBytes",
                status.admission.available_memory_bytes as f64,
            );
            put(
                "outstandingDiskBytes",
                status.admission.outstanding_disk_bytes as f64,
            );
            put("camerasOnline", online as f64);
            put("camerasConfigured", configured as f64);
        }
        Ok(values)
    }


    /// Samples what the component is holding into the `camera_queue` metric.
    ///
    /// The counts in `camera_captures` say what has happened; this says what is happening. An
    /// operator needs both: a fleet with a healthy success rate and a backlog that only grows is
    /// failing, and the counters alone cannot show it.
    pub(super) fn start_metric_sampler(self: &Arc<Self>) -> Result<()> {
        let runtime = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => return,
                    () = tokio::time::sleep(METRIC_SAMPLE_INTERVAL) => {}
                }
                let values = match runtime.sample_queue_metric().await {
                    Ok(values) => values,
                    Err(error) => {
                        tracing::warn!(error = %error, "camera queue metrics could not be sampled");
                        continue;
                    }
                };
                runtime.metrics.sample_queue(values).await;
                runtime.sample_southbound_health(false).await;
            }
        })
    }


    /// Records a camera's connect/disconnect transition and reports it at once.
    ///
    /// SOUTHBOUND §5: transitions are emitted immediately rather than waiting out a sampling
    /// interval, because a camera that just went down is exactly what an operator does not want to
    /// hear about thirty seconds late.
    ///
    /// A `reconnect` is counted only for a camera that HAD a session and lost it. The first connect of
    /// a camera's life is not a reconnect, and counting it as one would put a reconnect against every
    /// camera in the fleet every time the component starts -- the shape of a fleet-wide outage.
    fn observe_connectivity(
        &self,
        instance: &str,
        previous: Option<CameraConnectionState>,
        state: CameraConnectionState,
    ) {
        let was_online = previous == Some(CameraConnectionState::Online);
        let is_online = state == CameraConnectionState::Online;
        if was_online == is_online {
            return;
        }
        if is_online
            && matches!(
                previous,
                Some(CameraConnectionState::Offline | CameraConnectionState::Backoff)
            )
        {
            self.health.observed_reconnect(instance);
        }

        let Ok(config) = self.config_snapshot() else {
            return;
        };
        let stale_after =
            Duration::from_secs(config.global.health_thresholds.stale_signal_secs.max(1));
        let health = Arc::clone(&self.health);
        let metrics = Arc::clone(&self.metrics);
        let instance = instance.to_owned();
        // Detached on purpose. `spawn_task` retains every handle it is given, and a fleet that
        // reconnects is a fleet that produces these constantly; the emission is best-effort and
        // outliving it by a few milliseconds at shutdown costs nothing.
        tokio::spawn(async move {
            let sample = health.sample(&instance, is_online, stale_after);
            metrics.emit_health(&instance, &sample, true).await;
        });
    }


    /// Emits one `southbound_health` sample per configured camera.
    ///
    /// SOUTHBOUND §5 and DESIGN §19.1: this is the metric an operator alarms a fleet on, and the
    /// adapter emitted none of it. `connectionState` is the camera's live session; `staleSignals` is
    /// the one that gives `healthThresholds.staleSignalSecs` a job at last -- a camera that has not
    /// answered inside the threshold says so, whether or not anything has failed.
    ///
    /// `immediate` bypasses batching, for the connect and disconnect transitions an operator must not
    /// have to wait a sampling interval to see.
    pub(super) async fn sample_southbound_health(&self, immediate: bool) {
        let Ok(config) = self.config_snapshot() else {
            return;
        };
        let stale_after =
            Duration::from_secs(config.global.health_thresholds.stale_signal_secs.max(1));
        for camera in &config.instances {
            if !camera.enabled {
                continue;
            }
            let online = self
                .registry
                .snapshot(&camera.id)
                .is_ok_and(|snapshot| snapshot.state == CameraConnectionState::Online);
            let sample = self.health.sample(&camera.id, online, stale_after);
            self.metrics
                .emit_health(&camera.id, &sample, immediate)
                .await;
        }
    }


    /// Runs the fleet capture queue: pull the best admissible capture, and give it to its camera.
    ///
    /// This is the component's only capture consumer, and it is the whole of Q1. It replaces N
    /// per-camera drain loops that each polled at 100 Hz and could see only their own camera's work.
    /// The ordering it applies is fleet-wide -- a `Direct` capture on a connected camera can no
    /// longer wait behind a `Scheduled` one that a busy camera happens to hold.
    ///
    /// It is also, without any further code, the fix for an oversized group. A wave is simply "as
    /// many as capacity allows"; the members beyond that wait here, and each one's clocks start when
    /// a camera actually takes it. "More work than I can do at once" became "this takes longer"
    /// instead of "most of your members failed".
    pub(super) fn start_capture_scheduler(self: &Arc<Self>) -> Result<()> {
        let runtime = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            loop {
                let Some((queued, slot)) = runtime.scheduler.next_admissible(&cancellation).await
                else {
                    return;
                };
                let instance = queued.camera_id.clone();
                let descriptor = queued.payload.into_descriptor();
                let capture_id = descriptor.capture_id().to_owned();

                // The capture's clocks start NOW, not when it was accepted. Without this the whole
                // queue is a way of making captures die tidily: a member that waited its turn would
                // arrive at a free camera with its entire budget already spent.
                // Everything from here to the hand-off can fail, and a descriptor dropped on any of
                // those paths is a durable row left QUEUED with nothing left alive to drive it --
                // B5, rebuilt. So the rule for this whole block is: a capture only leaves the queue
                // when it is dispatched, or when it is provably no longer owed a run.
                let timeouts = match runtime.config_snapshot() {
                    Ok(config) => config.global.timeouts.clone(),
                    Err(error) => {
                        tracing::error!(error = %error, "capture scheduler cannot read its configuration");
                        runtime.return_to_queue(descriptor);
                        tokio::time::sleep(SCHEDULER_RETRY_BACKOFF).await;
                        continue;
                    }
                };
                let engine = match runtime.engine(&instance) {
                    Ok(engine) => engine,
                    Err(error) => {
                        // The camera is gone -- a reload retired it. There is nothing to put this
                        // back for, and requeueing would spin on it forever.
                        tracing::warn!(
                            instance = %instance,
                            capture = %descriptor.capture_id(),
                            error = %error,
                            "capture scheduler has no engine for this camera; the capture is dropped"
                        );
                        continue;
                    }
                };
                if let Err(error) = engine.rebase_onto_admission(&descriptor, &timeouts).await {
                    if error.is_durable_store_failure() {
                        // The STORE hiccuped -- it did not say this capture is finished. It is still
                        // QUEUED and still owed a run, so it goes back on the queue. Dropping it here
                        // is what stranded a real capture: under load the catalog returns SQLITE_BUSY,
                        // the rebase fails transiently, and treating that as "already retired"
                        // destroys a capture that nothing else will ever pick up. The back-off keeps a
                        // sustained store outage from turning this into a spin.
                        tracing::warn!(
                            instance = %instance,
                            capture = %descriptor.capture_id(),
                            error = %error,
                            "the catalog could not rebase this capture; it stays queued and will be retried"
                        );
                        runtime.return_to_queue(descriptor);
                        tokio::time::sleep(SCHEDULER_RETRY_BACKOFF).await;
                    } else {
                        // The row is no longer QUEUED: already terminal, cancelled, or expired. Its
                        // own machinery has retired it, and there is nothing to put back.
                        tracing::debug!(
                            instance = %instance,
                            capture = %descriptor.capture_id(),
                            error = %error,
                            "capture was retired before it could be dispatched"
                        );
                    }
                    continue;
                }

                // Held from here until the capture is terminal. The slot was taken BEFORE the pop,
                // so the queue never hands a camera work the component has no capacity to run: a
                // capture that would have waited a second time -- inside `execute`, invisibly, on a
                // clock already started for it -- waits in the queue instead, where waiting is free.
                runtime.scheduler.hold_execution_slot(&capture_id, slot);

                if let Err(descriptor) = runtime.scheduler.dispatch(&instance, descriptor) {
                    runtime.scheduler.capture_finished(&capture_id);
                    // The camera went offline between the pop and the hand-off. The capture has been
                    // durably promised, so it goes back in the queue rather than being dropped on the
                    // floor -- it will be admitted when the camera returns, or expire waiting.
                    tracing::debug!(
                        instance = %instance,
                        capture = %descriptor.capture_id(),
                        "camera went offline during dispatch; the capture stays queued"
                    );
                    runtime.return_to_queue(descriptor);
                }
            }
        })
    }


    /// Puts a descriptor back on the fleet queue after a failed hand-off.
    pub(super) fn requeue(&self, descriptor: crate::jobs::CaptureDescriptor) -> Result<()> {
        let instance = descriptor.instance().to_owned();
        let reservation = crate::jobs::CaptureDispatcher::reserve(&self.scheduler, &instance)?;
        reservation.commit(descriptor)?;
        Ok(())
    }


    /// Returns a capture to the queue, and says so loudly if it cannot.
    ///
    /// A capture that can be neither dispatched nor requeued is a durable row that will sit QUEUED
    /// until its deadline retires it. That is recoverable -- the deadline task does terminalize it --
    /// but it is never routine, and it must not be silent.
    fn return_to_queue(&self, descriptor: crate::jobs::CaptureDescriptor) {
        let instance = descriptor.instance().to_owned();
        let capture = descriptor.capture_id().to_owned();
        if let Err(error) = self.requeue(descriptor) {
            tracing::error!(
                instance = %instance,
                capture = %capture,
                error = %error,
                "a capture could not be returned to the queue and will wait for its deadline"
            );
        }
    }


    pub(super) fn start_storage_pressure_monitor(self: &Arc<Self>) -> Result<()> {
        let runtime = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                let _ = runtime.refresh_storage_pressure().await;
            }
        })
    }


    /// Starts the periodic retention sweep on the runtime's own task/shutdown machinery.
    pub(super) fn start_retention(self: &Arc<Self>) -> Result<()> {
        let runtime = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            runtime
                .run_retention(cancellation, RETENTION_SWEEP_INTERVAL, RETENTION_BATCH)
                .await;
        })
    }


    /// Sweeps retained durable state on `interval` until the runtime is cancelled.
    ///
    /// The interval and batch size are parameters rather than constants read inside the loop, so
    /// the loop is directly drivable.  A failed sweep is never fatal: retention is a background
    /// reclaim, and the next interval retries it.
    pub(super) async fn run_retention(
        self: Arc<Self>,
        cancellation: CancellationToken,
        interval: Duration,
        batch: usize,
    ) {
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
            match self
                .retention_sweep(chrono::Utc::now().timestamp_millis(), batch, &cancellation)
                .await
            {
                Ok(sweep) if sweep.reclaimed() > 0 => tracing::info!(
                    terminal_jobs = sweep.terminal_jobs,
                    terminal_groups = sweep.terminal_groups,
                    command_ledgers = sweep.command_ledgers,
                    over_limit_jobs = sweep.over_limit_jobs,
                    "camera retention reclaimed durable state"
                ),
                Ok(_) => {
                    tracing::debug!("camera retention found no durable state past its windows");
                }
                Err(error) => {
                    tracing::warn!(error = %error, "camera retention sweep failed; retrying on the next interval");
                }
            }
        }
    }


    /// Runs one full retention pass and reports what it reclaimed.
    pub(super) async fn retention_sweep(
        &self,
        now_ms: i64,
        batch: usize,
        cancellation: &CancellationToken,
    ) -> Result<RetentionSweep> {
        let config = self.config_snapshot()?;
        let state = &config.global.state;
        let terminal_before_ms =
            now_ms.saturating_sub(i64::from(state.result_retention_hours) * MILLIS_PER_HOUR);
        let catalog = &self.catalog;
        Ok(RetentionSweep {
            terminal_jobs: prune_in_batches(cancellation, batch, |limit| {
                catalog.prune_terminal_jobs(terminal_before_ms, limit)
            })
            .await?,
            terminal_groups: prune_in_batches(cancellation, batch, |limit| {
                catalog.prune_terminal_groups(terminal_before_ms, limit)
            })
            .await?,
            command_ledgers: prune_in_batches(cancellation, batch, |limit| {
                catalog.prune_completed_command_ledgers(terminal_before_ms, limit)
            })
            .await?,
            over_limit_jobs: prune_in_batches(cancellation, batch, |limit| {
                catalog.enforce_result_record_limit(state.max_result_records, limit)
            })
            .await?,
        })
    }


    /// Watches the durable catalog: readiness follows it, and a disk-full failure re-assesses
    /// storage pressure so the operator sees the cause rather than the symptom.
    ///
    /// This was the outbox worker's observer loop. The outbox is gone; the catalog is not, and it is
    /// the only thing here that readiness ever depended on -- a broker that is down never belonged
    /// in a durable-state readiness gate.
    pub(super) fn start_catalog_health(self: &Arc<Self>) -> Result<()> {
        let events = self.component_events.clone().ok_or_else(|| {
            crate::CameraError::Catalog(
                "missing component events facade for catalog health".to_string(),
            )
        })?;
        let readiness = self.readiness.clone();
        let availability = self.catalog.availability();
        let catalog = self.catalog.clone();
        let storage_pressure = self.storage_pressure.clone();
        let storage_alarm = Arc::clone(&self.storage_alarm);
        let observer_cancellation = self.cancellation.clone();
        self.spawn_task(async move {
            Self::observe_catalog_health(
                availability,
                catalog,
                events,
                storage_pressure,
                storage_alarm,
                readiness,
                observer_cancellation,
            )
            .await;
        })
    }


    async fn observe_catalog_health(
        mut catalog_availability: watch::Receiver<crate::catalog::CatalogAvailability>,
        catalog: Catalog,
        events: EventsFacade,
        storage_pressure: Option<StoragePressureMonitor>,
        storage_alarm: Arc<Mutex<StorageAlarmState>>,
        readiness: RuntimeReadiness,
        cancellation: CancellationToken,
    ) {
        let mut catalog_unavailable = !catalog_availability.borrow().state_capacity_available;
        let mut recovery_probe = tokio::time::interval(Duration::from_secs(1));
        recovery_probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                changed = catalog_availability.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let current = *catalog_availability.borrow_and_update();
                    catalog_unavailable = !current.state_capacity_available;
                    readiness.set_catalog_available(current.state_capacity_available);
                    if current.disk_full {
                        if let Some(monitor) = storage_pressure.as_ref() {
                            let snapshot = monitor.assess().await;
                            readiness.set_state_storage_available(snapshot.state_available());
                            publish_storage_alarm(
                                Some(events.clone()),
                                storage_alarm.as_ref(),
                                &snapshot,
                            )
                            .await;
                        }
                    }
                }
                _ = recovery_probe.tick(), if catalog_unavailable => {
                    if catalog.probe_commit().await.is_err() {
                        tracing::warn!("catalog durable-state recovery probe did not commit");
                    }
                }
            }
        }
    }


    pub(super) fn start_supervisors(self: &Arc<Self>) -> Result<()> {
        let config = self.config_snapshot()?;
        for camera in &config.instances {
            if !camera.enabled {
                continue;
            }
            let engine = self.engine(&camera.id)?;
            self.start_supervisor(camera.id.clone(), engine)?;
        }
        Ok(())
    }


    /// Starts one isolated supervisor generation.  The child cancellation token propagates the
    /// process shutdown token but can also retire this generation during a per-camera reload.
    pub(super) fn start_supervisor(self: &Arc<Self>, instance: String, engine: JobEngine) -> Result<()> {
        let cancellation = self.cancellation.child_token();
        let finished = CancellationToken::new();
        // Both tokens land together. They used to be two maps and two acquisitions, so a
        // `start_supervisor` that displaced a running generation cancelled its token and silently
        // DROPPED its completion signal -- destroying the only way to await the generation it had just
        // told to stop. They are one value now, and cannot be half-replaced.
        let previous = self
            .cameras
            .write()
            .map_err(|_| crate::CameraError::Catalog("camera slot map is unavailable".to_string()))?
            .get_mut(&instance)
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::UnknownInstance,
                    format!("camera instance '{instance}' is not configured"),
                )
            })?
            .supervisor
            .replace(Supervision {
                cancellation: cancellation.clone(),
                finished: finished.clone(),
            });
        if let Some(previous) = previous {
            previous.cancellation.cancel();
        }
        let runtime = Arc::clone(self);
        self.spawn_task(async move {
            runtime
                .run_supervisor(instance, engine, cancellation, finished)
                .await;
        })
    }


    async fn run_supervisor(
        self: Arc<Self>,
        instance: String,
        engine: JobEngine,
        cancellation: CancellationToken,
        finished: CancellationToken,
    ) {
        self.run_supervisor_loop(instance, engine, cancellation)
            .await;
        finished.cancel();
    }


    async fn run_supervisor_loop(
        self: Arc<Self>,
        instance: String,
        engine: JobEngine,
        cancellation: CancellationToken,
    ) {
        let mut attempt = 0_u32;
        // A reload retains the registry/watch entry but explicitly advances its generation to
        // fence stale callbacks.  A replacement supervisor must continue from that fence rather
        // than restart at zero, or every one of its observations would be discarded as stale.
        let mut generation = self
            .registry
            .snapshot(&instance)
            .map_or(0, |snapshot| snapshot.generation);
        loop {
            let config = match self.config_snapshot() {
                Ok(config) => config,
                Err(error) => {
                    tracing::error!(instance = %instance, error = %error, "camera supervisor lost runtime configuration");
                    return;
                }
            };
            let global_config = &config.global;
            let camera = match self.registry.camera_config(&instance) {
                Ok(camera) if camera.enabled => camera,
                Ok(_) | Err(_) => return,
            };
            let factory = match self
                .backend_context
                .factory_for(&camera.backend, global_config)
            {
                Ok(factory) => factory,
                Err(error) => {
                    self.publish_camera_state(
                        &instance,
                        generation,
                        CameraConnectionState::Backoff,
                        None,
                        Some(status_error(&error)),
                        chrono::Utc::now(),
                    );
                    return;
                }
            };
            if cancellation.is_cancelled() {
                self.publish_camera_state(
                    &camera.id,
                    generation,
                    CameraConnectionState::Stopping,
                    None,
                    None,
                    chrono::Utc::now(),
                );
                return;
            }
            generation = generation.saturating_add(1);
            self.publish_camera_state(
                &camera.id,
                generation,
                CameraConnectionState::Connecting,
                None,
                None,
                chrono::Utc::now(),
            );
            let permit = tokio::select! {
                _ = cancellation.cancelled() => return,
                permit = self.connect_gate.clone().acquire_owned() => match permit { Ok(permit) => permit, Err(_) => return },
            };
            let request = ConnectRequest {
                instance_id: camera.id.clone(),
                backend: camera.backend.clone(),
                timeout: Duration::from_millis(global_config.timeouts.connect_ms),
                cancellation: cancellation.child_token(),
            };
            let connected =
                crate::supervisor::isolate_backend_panic(factory.connect(request)).await;
            drop(permit);
            let retry_class = match &connected {
                Err(crate::CameraError::Config { .. }) => crate::supervisor::RetryClass::Permanent,
                _ => crate::supervisor::RetryClass::Transient,
            };
            match connected {
                Ok(session) => {
                    attempt = 0;
                    let capabilities = session.capabilities().clone();
                    let (actor, handle) = match CameraActor::new(
                        camera.id.clone(),
                        session,
                        engine.clone(),
                        global_config.limits.max_queued_captures_per_camera,
                        global_config.limits.max_queued_controls_per_camera,
                        self.scheduler.capacity_signal(),
                    ) {
                        Ok(pair) => pair,
                        Err(error) => {
                            self.publish_camera_state(
                                &camera.id,
                                generation,
                                CameraConnectionState::Backoff,
                                None,
                                Some(status_error(&error)),
                                chrono::Utc::now(),
                            );
                            self.sleep_backoff(
                                &camera.id,
                                attempt,
                                crate::supervisor::RetryClass::Permanent,
                                &cancellation,
                            )
                            .await;
                            attempt = attempt.saturating_add(1);
                            continue;
                        }
                    };
                    let actor_cancellation = cancellation.child_token();
                    if let Ok(mut cameras) = self.cameras.write() {
                        if let Some(slot) = cameras.get_mut(&camera.id) {
                            slot.session = Some(Session {
                                actor: handle.clone(),
                                cancellation: actor_cancellation.clone(),
                            });
                        }
                    }
                    self.publish_camera_state(
                        &camera.id,
                        generation,
                        CameraConnectionState::Online,
                        Some(capabilities),
                        None,
                        chrono::Utc::now(),
                    );
                    let mut actor_task = tokio::spawn(actor.run(actor_cancellation));
                    // The camera is online: tell the fleet queue it can take work. There is no
                    // per-camera cache to drain any more, and therefore no loop here that drains one
                    // -- the scheduler pulls, it is not pushed to. This supervisor's only remaining
                    // job while connected is to wait for its actor to finish or be cancelled.
                    self.scheduler.camera_online(&camera.id, handle.clone());
                    let result = tokio::select! {
                        joined = &mut actor_task => joined.map_err(|error| crate::CameraError::Backend {
                            backend: "actor",
                            message: format!("actor task failed: {error}"),
                        }).and_then(|result| result),
                        _ = cancellation.cancelled() => {
                            // The actor holds a child of this token and is already winding down, so
                            // it must be awaited, not dropped: its teardown is what delivers the
                            // shutdown safety stop and closes the session. The budget is the smaller
                            // of the two deadlines that already bound this path — the shutdown grace
                            // and the reload drain timeout — so a hung backend can defeat neither.
                            let grace = Duration::from_millis(
                                global_config
                                    .timeouts
                                    .shutdown_grace_ms
                                    .min(global_config.timeouts.reload_drain_timeout_ms),
                            );
                            if !join_actor_within_grace(&mut actor_task, grace).await {
                                tracing::warn!(
                                    instance = %camera.id,
                                    grace_ms = grace.as_millis(),
                                    "camera actor did not complete its shutdown teardown within the grace budget; aborting"
                                );
                            }
                            Ok(())
                        }
                    };
                    // Its queued work stays queued. That is the entire point of a queue that outlives
                    // the session: a camera that drops does not lose the captures promised to it.
                    self.scheduler.camera_offline(&camera.id);
                    if let Ok(mut cameras) = self.cameras.write() {
                        if let Some(slot) = cameras.get_mut(&camera.id) {
                            slot.session = None;
                        }
                    }
                    if cancellation.is_cancelled() {
                        return;
                    }
                    if let Err(error) = result {
                        self.publish_camera_state(
                            &camera.id,
                            generation,
                            CameraConnectionState::Backoff,
                            None,
                            Some(status_error(&error)),
                            chrono::Utc::now(),
                        );
                    }
                }
                Err(error) => {
                    self.publish_camera_state(
                        &camera.id,
                        generation,
                        CameraConnectionState::Backoff,
                        None,
                        Some(status_error(&error)),
                        chrono::Utc::now(),
                    );
                }
            }
            self.sleep_backoff(&camera.id, attempt, retry_class, &cancellation)
                .await;
            attempt = attempt.saturating_add(1);
        }
    }


    async fn sleep_backoff(
        &self,
        instance: &str,
        attempt: u32,
        retry_class: crate::supervisor::RetryClass,
        cancellation: &CancellationToken,
    ) {
        let config = match self.config_snapshot() {
            Ok(config) => config,
            Err(error) => {
                tracing::error!(instance, error = %error, "camera supervisor cannot load reconnect policy");
                return;
            }
        };
        let policy = match crate::supervisor::BackoffPolicy::new(
            Duration::from_millis(config.global.timeouts.reconnect_backoff_min_ms),
            Duration::from_millis(config.global.timeouts.reconnect_backoff_max_ms),
        ) {
            Ok(policy) => policy,
            Err(error) => {
                tracing::error!(error = %error, "validated reconnect policy became invalid");
                return;
            }
        };
        let delay = policy.delay(instance, 1, retry_class, attempt);
        tokio::select! {
            _ = cancellation.cancelled() => {}
            _ = tokio::time::sleep(delay) => {}
        }
    }


    pub(super) async fn recover_install_owned(&self) -> Result<()> {
        // A restart re-establishes every session, which is exactly what a reconnect asked for, so
        // an interrupted reconnect is settled rather than fenced. This must run before the
        // hazardous fence below: an OUTCOME_UNKNOWN reconnect row is unreclaimable by every
        // retention statement in the catalog and answers PREVIOUS_OUTCOME_UNKNOWN forever.
        let settled = self
            .catalog
            .settle_interrupted_reconnects(chrono::Utc::now().timestamp_millis())
            .await?;
        if settled > 0 {
            tracing::info!(
                settled,
                "settled reconnect commands interrupted by the previous run"
            );
        }
        // Generic PTZ/preset commands may have crossed a physical side-effect boundary before the
        // process died. They are never replayed automatically; exact retries receive the durable
        // PREVIOUS_OUTCOME_UNKNOWN result instead.
        self.catalog
            .mark_hazardous_commands_outcome_unknown(chrono::Utc::now().timestamp_millis())
            .await?;
        // A PERSISTING record whose install CAS won has a fully staged success envelope and can
        // be reconciled without reconnecting any camera.  Other active states need a fresh
        // command/runtime recovery policy; never quietly drop them during startup.
        let policy = self.config_snapshot()?.global.state.queued_recovery_policy;
        let mut requeued = 0_usize;
        let mut interrupted = 0_usize;
        for record in self.catalog.recovery_jobs().await? {
            let engine = self.engine(&record.instance)?;
            if record.install_started {
                let cancellation = CancellationToken::new();
                engine
                    .recover_install_started(record, &cancellation)
                    .await?;
                continue;
            }
            let resumable = if policy == crate::config::QueuedRecoveryPolicy::Requeue {
                self.resumable_after_restart(&record).await?
            } else {
                None
            };
            if let Some(resumable) = resumable {
                let capture = record.capture_id.clone();
                let instance = record.instance.clone();
                match engine
                    .requeue_recovered(
                        &self.scheduler,
                        record,
                        resumable.resource_group,
                        resumable.group_size,
                        resumable.priority,
                    )
                    .await
                {
                    Ok(_) => {
                        requeued += 1;
                        continue;
                    }
                    Err(error) => {
                        // The capture could not be put back -- most likely the fleet queue is full of
                        // work recovered before it. It must still be retired, not left QUEUED with
                        // nothing to drive it, so it falls through to the interrupt below.
                        tracing::warn!(
                            instance = %instance,
                            capture = %capture,
                            error = %error,
                            "a waiting capture could not be requeued after the restart and is interrupted"
                        );
                        let Some(record) = self.catalog.job(&capture).await? else {
                            continue;
                        };
                        if record.state.is_terminal() {
                            continue;
                        }
                        engine.interrupt_recovered(record).await?;
                        interrupted += 1;
                        continue;
                    }
                }
            }
            engine.interrupt_recovered(record).await?;
            interrupted += 1;
        }
        if requeued > 0 || interrupted > 0 {
            tracing::info!(
                requeued,
                interrupted,
                policy = ?policy,
                "captures left waiting by the previous run were recovered"
            );
        }
        Ok(())
    }


    /// Decides whether a capture that was still waiting when the process died may simply resume.
    ///
    /// The rule is DESIGN §17.1: *"For `QUEUED` jobs, requeue only when `queuedRecoveryPolicy =
    /// requeue`, the queue deadline has not expired, and the snapshotted camera/profile can still
    /// run."* `Some` means resume; `None` means it is not the same piece of work any more and is
    /// retired with `PROCESS_INTERRUPTED`, like everything else the restart caught mid-flight.
    ///
    /// * **`QUEUED` only.** An `ACCEPTED` capture never completed its durable queue transition, and
    ///   §17.1 retires it; anything at `ACQUIRING` or beyond has side effects behind it, and a replay
    ///   is not a recovery.
    /// * **The queue deadline must still be ahead** -- the capture's own bound on how long it was
    ///   willing to wait, which a restart does not get to extend.
    /// * **The terminal deadline must still be ahead too.** This is stricter than §17.1 asks, and
    ///   deliberately so: a capture with no `queueExpiryMs` has no queue deadline at all, and stage
    ///   clocks are now rebased when a camera takes the capture -- so without this an image requested
    ///   hours before the crash would come back with a fresh clock and be taken for a live request.
    /// * **The camera must still be the same device** -- configured, enabled, and on the backend the
    ///   capture was accepted for. The durable profile is the contract, and a camera that has become
    ///   something else does not get to honour it.
    async fn resumable_after_restart(
        &self,
        record: &crate::catalog::JobRecord,
    ) -> Result<Option<ResumableCapture>> {
        if record.state != crate::model::JobState::Queued {
            return Ok(None);
        }
        let now_ms = chrono::Utc::now().timestamp_millis();
        if record
            .deadlines
            .queue_at_ms
            .is_some_and(|queue_at_ms| queue_at_ms <= now_ms)
        {
            return Ok(None);
        }
        if record.deadlines.terminal_at_ms <= now_ms {
            return Ok(None);
        }
        let Ok(camera) = self.registry.camera_config(&record.instance) else {
            return Ok(None);
        };
        if !camera.enabled {
            return Ok(None);
        }
        let accepted_backend = record
            .intended_output
            .get("backend")
            .and_then(serde_json::Value::as_str);
        if accepted_backend != Some(camera.backend.kind().as_str()) {
            return Ok(None);
        }
        // A group member must carry its real group size: the runtime snapshot is rejected without
        // it, and the terminal envelope reports it.
        let group_size = match record.group_id.as_ref() {
            Some(group_id) => {
                let Some(group) = self.catalog.group(group_id.clone()).await? else {
                    return Ok(None);
                };
                Some(group.members.len())
            }
            None => None,
        };
        Ok(Some(ResumableCapture {
            resource_group: camera.resource_group.clone(),
            group_size,
            priority: recovered_priority(record),
        }))
    }
}
