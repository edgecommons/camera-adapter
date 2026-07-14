//! The reload plane: replacing one generation of configuration with another, atomically.
//!
//! Retire every affected supervisor and confirm it is gone BEFORE any published generation
//! changes, so two generations can never control one camera; roll the whole thing back if the
//! candidate fails after that point.

use super::*;

impl CameraRuntime {

    /// Applies one already pre-commit-validated configuration generation without exposing a
    /// mixed roster.  All fallible preparation happens before the registry/config swap.  Existing
    /// compatible dispatchers retain their durable queued work; removal, disablement, or backend
    /// replacement terminalizes queued work with the exact reload-interruption envelope.
    pub async fn apply_reloaded_config(
        self: &Arc<Self>,
        replacement: AdapterConfig,
        apps: BTreeMap<String, Arc<AppFacade>>,
        events: BTreeMap<String, EventsFacade>,
    ) -> Result<crate::registry::RegistryDiff> {
        if self.reloading.swap(true, Ordering::AcqRel) {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::CameraUnavailable,
                "a configuration replacement is already draining camera work",
            ));
        }
        let _reloading = ReloadInProgressGuard(&self.reloading);
        self.apply_reloaded_config_inner(replacement, apps, events)
            .await
    }


    /// Performs the candidate-only half of a reload without altering any live camera generation.
    ///
    /// Core invokes this from its pre-commit application coordinator.  This method deliberately
    /// excludes supervisor cancellation, catalog changes, registry replacement, schedule changes,
    /// and readiness publication: a rejected candidate must leave the complete prior service able
    /// to keep accepting and completing captures (R-04).  The corresponding live transition is
    /// performed only from the post-commit configuration listener.
    pub(super) fn preflight_reloaded_config(
        &self,
        replacement: &AdapterConfig,
        apps: &BTreeMap<String, Arc<AppFacade>>,
        events: &BTreeMap<String, EventsFacade>,
    ) -> Result<()> {
        self.backend_context.validate_config(replacement)?;
        let previous = self.config_snapshot()?;
        if previous.global.state.directory != replacement.global.state.directory
            || previous.global.output.root_directory != replacement.global.output.root_directory
            || previous.global.output.directory_mode != replacement.global.output.directory_mode
            || previous.global.output.file_mode != replacement.global.output.file_mode
        {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::InvalidRequest,
                "state/output root security settings require component restart",
            ));
        }

        // Constructibility of every new runtime dependency is part of candidate validation.  Do
        // not retain these temporary values: the committed transition constructs its own objects
        // after Core has atomically advanced the configuration snapshot.
        let existing_engine_ids = self
            .cameras
            .read()
            .map_err(|_| crate::CameraError::Catalog("camera slot map is unavailable".to_string()))?
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        for camera in &replacement.instances {
            if existing_engine_ids.contains(&camera.id) {
                continue;
            }
            if !apps.contains_key(&camera.id) {
                return Err(crate::CameraError::Catalog(format!(
                    "missing application facade for reloaded camera '{}'",
                    camera.id
                )));
            }
            if !events.contains_key(&camera.id) {
                return Err(crate::CameraError::Catalog(format!(
                    "missing events facade for reloaded camera '{}'",
                    camera.id
                )));
            }
        }
        Ok(())
    }


    /// Captures only in-memory generation state before a prepared transaction begins its live
    /// transition. All locks are released before any await in the commit/rollback path.
    pub(super) fn reload_checkpoint(&self) -> Result<RuntimeReloadCheckpoint> {
        let config = self.config_snapshot()?;
        // Only the roster-durable half of a slot is checkpointed. The supervisor, the session and any
        // armed stop are generation state, and a rollback retires every generation before it restores
        // anything -- so carrying them across would be carrying corpses.
        let cameras = self.cameras.read().map_err(|_| crate::CameraError::Catalog("camera slot map is unavailable".to_string()))?;
        let engines = cameras
            .iter()
            .map(|(instance, slot)| (instance.clone(), slot.engine.clone()))
            .collect::<BTreeMap<_, _>>();
        let events = cameras
            .iter()
            .filter_map(|(instance, slot)| Some((instance.clone(), slot.events.clone()?)))
            .collect::<BTreeMap<_, _>>();
        drop(cameras);
        Ok(RuntimeReloadCheckpoint {
            config,
            engines,
            events,
        })
    }


    /// Restores a checkpoint after a prepared candidate fails before Core publishes it.
    ///
    /// Every currently-live supervisor is first retired and confirmed stopped. Reinstalling the
    /// prior maps/configuration before starting fresh prior-generation supervisors avoids a stale
    /// actor controlling a camera concurrently with a rollback actor. The method is idempotent:
    /// Core may call it after a commit error even when the transition did not reach a destructive
    /// stage.
    pub(super) async fn restore_reload_checkpoint(
        self: &Arc<Self>,
        checkpoint: RuntimeReloadCheckpoint,
    ) -> Result<()> {
        if self.reloading.swap(true, Ordering::AcqRel) {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::CameraUnavailable,
                "a configuration replacement is still active while restoring the prior generation",
            ));
        }
        let _reloading = ReloadInProgressGuard(&self.reloading);
        let _reload = self.reload_gate.lock().await;
        let current = self
            .config_snapshot()
            .unwrap_or_else(|_| checkpoint.config.clone());
        let mut instances = current
            .instances
            .iter()
            .map(|camera| camera.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        instances.extend(
            checkpoint
                .config
                .instances
                .iter()
                .map(|camera| camera.id.clone()),
        );
        let instances = instances.into_iter().collect::<Vec<_>>();
        let timeout =
            Duration::from_millis(checkpoint.config.global.timeouts.reload_drain_timeout_ms);
        self.replace_supervisors(&instances, timeout).await?;

        // All direct map writes happen after supervisor retirement. No lock is held across the
        // preceding await, and a poisoned map is reported so Core retains the prior snapshot.
        self.registry.apply_validated_config(&checkpoint.config)?;
        // The whole roster is reinstated in one write. It used to restore engines and events and
        // leave the other five maps alone, so a camera the failed candidate had ADDED kept its
        // supervisor tokens and half-existed afterwards. Rebuilding the slots drops that state with
        // the cameras it belonged to -- and every supervisor was retired above, so there is nothing
        // live to drop.
        *self.cameras.write().map_err(|_| {
            crate::CameraError::Catalog("camera slot map is unavailable during rollback".to_string())
        })? = new_slots(checkpoint.engines, checkpoint.events);
        *self.config.write().map_err(|_| {
            crate::CameraError::Catalog(
                "runtime configuration lock is unavailable during rollback".to_string(),
            )
        })? = checkpoint.config.clone();

        self.restart_schedulers()?;
        self.restart_periodic_discovery()?;
        for camera in checkpoint
            .config
            .instances
            .iter()
            .filter(|camera| camera.enabled)
        {
            self.start_supervisor(camera.id.clone(), self.engine(&camera.id)?)?;
        }
        Ok(())
    }


    async fn apply_reloaded_config_inner(
        self: &Arc<Self>,
        replacement: AdapterConfig,
        apps: BTreeMap<String, Arc<AppFacade>>,
        events: BTreeMap<String, EventsFacade>,
    ) -> Result<crate::registry::RegistryDiff> {
        let _reload = self.reload_gate.lock().await;
        self.preflight_reloaded_config(&replacement, &apps, &events)?;
        let previous = self.config_snapshot()?;

        let replacement_by_id = replacement
            .instances
            .iter()
            .map(|camera| (camera.id.as_str(), camera))
            .collect::<BTreeMap<_, _>>();
        // Queued work is compatible only when its backend *kind* remains the same.  A changed
        // endpoint/selector/credential reference still requires a new live session, but the
        // immutable accepted job profile remains executable by the same backend contract.
        let incompatible = previous
            .instances
            .iter()
            .filter_map(|old| match replacement_by_id.get(old.id.as_str()) {
                Some(new)
                    if new.enabled && old.enabled && old.backend.kind() == new.backend.kind() =>
                {
                    None
                }
                _ => Some(old.id.clone()),
            })
            .collect::<Vec<_>>();
        // ONVIF protocol clients retain the global network and HTTP/XML policy that existed when
        // their session was constructed.  A policy reload therefore retires otherwise unchanged
        // ONVIF sessions so the next connection cannot keep probing on an old interface set or
        // applying stale security limits.  Sim and GenICam sessions have no such global policy
        // dependency and remain live when their backend settings are unchanged.
        let onvif_runtime_policy_changed = previous.global.discovery.eligible_interfaces
            != replacement.global.discovery.eligible_interfaces
            || previous.global.security.max_header_bytes
                != replacement.global.security.max_header_bytes
            || previous.global.security.max_decompression_ratio
                != replacement.global.security.max_decompression_ratio
            || previous.global.security.allow_basic_over_plaintext
                != replacement.global.security.allow_basic_over_plaintext;
        let restarting = previous
            .instances
            .iter()
            .filter_map(|old| match replacement_by_id.get(old.id.as_str()) {
                Some(new)
                    if old.enabled
                        && new.enabled
                        && old.backend == new.backend
                        && !(onvif_runtime_policy_changed
                            && old.backend.kind() == crate::model::BackendKind::OnvifRtsp) =>
                {
                    None
                }
                _ => Some(old.id.clone()),
            })
            .collect::<Vec<_>>();

        // Build every new runtime object before changing the published registry.  An absent
        // facade is a real initialization failure, not permission to install a partial roster.
        let existing_engine_ids = self
            .cameras
            .read()
            .map_err(|_| crate::CameraError::Catalog("camera slot map is unavailable".to_string()))?
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut added = Vec::new();
        for camera in &replacement.instances {
            if existing_engine_ids.contains(&camera.id) {
                continue;
            }
            let app = apps.get(&camera.id).cloned().ok_or_else(|| {
                crate::CameraError::Catalog(format!(
                    "missing application facade for reloaded camera '{}'",
                    camera.id
                ))
            })?;
            let event = events.get(&camera.id).cloned().ok_or_else(|| {
                crate::CameraError::Catalog(format!(
                    "missing events facade for reloaded camera '{}'",
                    camera.id
                ))
            })?;
            // Engine and facade arrive together or the camera is not added at all. They used to go
            // into two maps in two separate acquisitions, which is precisely how a camera comes to
            // exist in one and not the other.
            added.push((
                camera.id.clone(),
                CameraSlot {
                    engine: self.new_engine(app),
                    events: Some(event),
                    supervisor: None,
                    session: None,
                    motion_stop: None,
                },
            ));
        }
        // Core calls this method from its pre-commit application gate. Retire every old
        // supervisor before touching any published runtime generation: a timeout must veto the
        // candidate while Core and the runtime still expose the same previous configuration.
        // Cancellation itself may leave an affected camera unavailable until the old generation
        // exits and the configuration source retries, but it must never permit two generations to
        // control one camera concurrently.
        let drain_timeout =
            Duration::from_millis(replacement.global.timeouts.reload_drain_timeout_ms);
        self.wait_for_active_jobs(&restarting, drain_timeout)
            .await?;
        self.replace_supervisors(&restarting, drain_timeout).await?;

        // The retirement barrier above has confirmed that no old generation can mutate a camera
        // after this point. All remaining fallible preparation has completed, so the registry and
        // runtime configuration can now advance as one candidate generation.
        let diff = self.registry.apply_validated_config(&replacement)?;
        self.waiters
            .set_waiter_limit(replacement.global.limits.max_deferred_waiters_per_capture);
        {
            match self.cameras.write() {
                Ok(mut cameras) => {
                    for (instance, slot) in added {
                        cameras.insert(instance, slot);
                    }
                    // The listener supplies fresh facades for all retained instances so their core
                    // configuration snapshot stays current; tests and internal callers may omit
                    // retained entries, in which case the established facade remains valid. Newly
                    // added cameras were required above and are therefore never installed without
                    // an event publishing path.
                    for (instance, event) in events {
                        if !replacement_by_id.contains_key(instance.as_str()) {
                            continue;
                        }
                        if let Some(slot) = cameras.get_mut(&instance) {
                            slot.events = Some(event);
                        }
                    }
                }
                Err(_) => {
                    tracing::error!("camera slot map became unavailable while committing reload");
                }
            }
        }
        {
            match self.config.write() {
                Ok(mut config) => *config = Arc::new(replacement.clone()),
                Err(_) => {
                    tracing::error!(
                        "runtime configuration lock became unavailable while committing reload"
                    );
                }
            }
        }
        // A reload can introduce a preview size this transport cannot carry, and the operator who
        // just deployed it is exactly the person who needs to hear so. Said once, here -- not on
        // every capture the new configuration goes on to take.
        super::log_thumbnail_clamps(&replacement, self.thumbnail_policy);

        for instance in &incompatible {
            if let Err(error) = self.interrupt_reload_queued(instance).await {
                tracing::error!(instance, error = %error, "could not terminalize incompatible queued jobs during reload");
            }
        }

        // Schedule plans are immutable.  Canceling the prior generation before constructing the
        // new plans prevents a schedule-only reload from admitting an old cron/profile after the
        // registry generation has changed.
        if let Err(error) = self.restart_schedulers() {
            tracing::error!(error = %error, "could not restart schedules after committed reload");
        }
        if let Err(error) = self.restart_periodic_discovery() {
            tracing::error!(error = %error, "could not restart periodic discovery after committed reload");
        }

        // The pre-commit retirement barrier confirmed every old supervisor exit before the
        // registry/configuration swap. New supervisors therefore cannot overlap a stale camera
        // generation or let a stale cleanup path remove their actor entry.
        for instance in &diff.added {
            if let Ok(camera) = self.registry.camera_config(instance) {
                if camera.enabled {
                    match self.engine(instance) {
                        Ok(engine) => {
                            if let Err(error) = self.start_supervisor(instance.clone(), engine) {
                                tracing::error!(instance, error = %error, "could not start added camera supervisor after committed reload");
                            }
                        }
                        Err(error) => {
                            tracing::error!(instance, error = %error, "added camera has no runtime engine after committed reload");
                        }
                    }
                }
            }
        }
        for instance in restarting.iter().filter(|instance| {
            !diff.removed.contains(instance)
                && self
                    .registry
                    .camera_config(instance)
                    .is_ok_and(|camera| camera.enabled)
        }) {
            match self.engine(instance) {
                Ok(engine) => {
                    if let Err(error) = self.start_supervisor(instance.clone(), engine) {
                        tracing::error!(instance, error = %error, "could not restart camera supervisor after committed reload");
                    }
                }
                Err(error) => {
                    tracing::error!(instance, error = %error, "restarted camera has no runtime engine after committed reload");
                }
            }
        }
        if !diff.removed.is_empty() {
            // A removed camera needs nothing torn down in the queue: it is deregistered, its work
            // is never admissible again, and each entry expires on its own wait deadline. The queue
            // outliving one camera is the same property that lets it outlive a reconnect.
            for instance in &diff.removed {
                self.scheduler.camera_offline(instance);
            }
            // ONE removal. This was six hand-written `retain`s over six maps -- and it forgot two of
            // them, `actors` and `motion_stops`, which is how a camera dropped from the roster mid-move
            // came to keep an armed stop that nothing would ever deliver. Forgetting is no longer an
            // available move: the camera's engine, facade, supervisor, session and armed stop are one
            // entry, and they leave together.
            if let Ok(mut cameras) = self.cameras.write() {
                cameras.retain(|instance, _| !diff.removed.contains(instance));
            } else {
                tracing::error!("camera slot map became unavailable while removing cameras");
            }
        }
        Ok(diff)
    }


    async fn wait_for_active_jobs(&self, instances: &[String], timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let mut any_active = false;
            for instance in instances {
                if self.has_active_job(instance).await? {
                    any_active = true;
                    break;
                }
            }
            if !any_active || tokio::time::Instant::now() >= deadline {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }


    /// Cancels complete supervisor generations, not merely live actors.  This covers a reload
    /// arriving while a backend is connecting or sleeping in exponential backoff.
    pub(super) async fn replace_supervisors(&self, instances: &[String], timeout: Duration) -> Result<()> {
        // A camera being retired may be in the middle of a continuous move, and cancelling its
        // supervisor takes away the only actor its mandatory stop could ever have been delivered
        // through. Stop it while that actor still exists. The field's own documentation has always
        // said a retiring supervisor disarms the timer; until now nothing did.
        self.deliver_mandatory_stops(Some(instances));

        // One pass over one map. The supervisor token, the session token and the completion signal
        // for a camera are the same entry, so they can no longer disagree about which cameras are
        // being retired -- which is what three separate reads with three different lock-poisoning
        // policies, ten lines apart, could do.
        let completed = {
            let cameras = self.cameras.read().map_err(|_| crate::CameraError::Catalog("camera slot map is unavailable".to_string()))?;
            let retiring = instances
                .iter()
                .filter_map(|instance| cameras.get(instance))
                .collect::<Vec<_>>();
            for slot in &retiring {
                if let Some(supervisor) = &slot.supervisor {
                    supervisor.cancellation.cancel();
                }
                // A live actor already holds a child of the supervisor token. Cancelling the session
                // directly still matters for an already-dispatched control operation, which owns its
                // own child token and would not otherwise see the retirement.
                if let Some(session) = &slot.session {
                    session.cancellation.cancel();
                }
            }
            retiring
                .iter()
                .filter_map(|slot| slot.supervisor.as_ref())
                .map(|supervisor| supervisor.finished.clone())
                .collect::<Vec<_>>()
        };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if completed.iter().all(CancellationToken::is_cancelled) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::CameraUnavailable,
                    "camera supervisor did not stop within reloadDrainTimeoutMs",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }


    async fn has_active_job(&self, instance: &str) -> Result<bool> {
        let states = vec![
            crate::model::JobState::Acquiring,
            crate::model::JobState::Encoding,
            crate::model::JobState::Persisting,
        ];
        Ok(!self
            .catalog
            .jobs_page(Some(instance.to_owned()), states, None, 1)
            .await?
            .is_empty())
    }


    async fn interrupt_reload_queued(&self, instance: &str) -> Result<()> {
        let mut before = None;
        loop {
            let page = self
                .catalog
                .jobs_page(
                    Some(instance.to_owned()),
                    vec![
                        crate::model::JobState::Accepted,
                        crate::model::JobState::Queued,
                    ],
                    before.clone(),
                    1_000,
                )
                .await?;
            let Some(last) = page.last() else {
                return Ok(());
            };
            for record in &page {
                self.engine(instance)?
                    .interrupt_for_reload(record.clone())
                    .await?;
            }
            if page.len() < 1_000 {
                return Ok(());
            }
            before = Some((last.accepted_at_ms, last.capture_id.clone()));
        }
    }
}
