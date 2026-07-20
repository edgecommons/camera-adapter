//! The command plane: everything reachable from a southbound command.
//!
//! Captures and groups, discovery, the queue verbs, cancellation, reconnect, and the PTZ
//! surface -- including the mandatory-stop timer, which lives here because arming it is part of
//! commanding a move, even though a retiring supervisor and a shutting-down component are the
//! ones that have to deliver it (DESIGN 15.5, 20.2).

use super::*;

impl CameraRuntime {

    /// Resolves one capture against the live configuration and registry.
    ///
    /// Deliberately does NOT enforce `enabled`: both entry points already resolve their instance
    /// through `registry.resolve_actuation_instance`, which does. Putting a second check here would
    /// look like the load-bearing one and is not.
    fn resolve_capture(
        &self,
        instance: &str,
        requested_profile: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<ResolvedCapture> {
        let config = self.config_snapshot()?;
        let camera = self.registry.camera_config(instance)?;
        let profile_name = requested_profile
            .map(str::to_owned)
            .unwrap_or_else(|| camera.default_capture_profile.clone());
        let profile = camera
            .capture_profiles
            .get(&profile_name)
            .cloned()
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::UnknownCaptureProfile,
                    "capture profile is not configured",
                )
            })?;
        let accepted_at_ms = chrono::Utc::now().timestamp_millis();
        let terminal_ms = timeout_ms
            .or(profile.timeout_ms)
            .unwrap_or(config.global.timeouts.job_terminal_ms);
        let capture_mode = profile
            .capture_mode
            .unwrap_or_else(|| match &camera.backend {
                crate::config::BackendConfig::Sim(_) => crate::model::CaptureMode::Simulated,
                crate::config::BackendConfig::GenicamAravis(_) => {
                    crate::model::CaptureMode::SoftwareTrigger
                }
                crate::config::BackendConfig::OnvifRtsp(config) => config.capture_mode,
                crate::config::BackendConfig::Rtsp(_) => crate::model::CaptureMode::RtspFrame,
            });
        let snapshot = self.registry.snapshot(instance)?;
        let camera_summary = crate::messages::CameraSummary {
            backend: snapshot.backend,
            vendor: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.vendor.clone()),
            model: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.model.clone()),
            firmware: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.firmware.clone()),
            serial: snapshot
                .capabilities
                .as_ref()
                .and_then(|caps| caps.serial.clone()),
        };
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
        let profile_snapshot = crate::jobs::JobProfileSnapshot {
            name: profile_name.clone(),
            capture: profile.clone(),
            offline_policy: profile
                .offline_policy
                .unwrap_or(crate::config::OfflinePolicy::WaitUntilDeadline),
            maximum_frame_bytes: profile
                .maximum_frame_bytes
                .unwrap_or(config.global.limits.max_frame_bytes_per_camera),
            capture_mode,
            capture_interlock: profile
                .capture_interlock
                .unwrap_or(camera.ptz.capture_interlock),
            settle_ms: camera.ptz.settle_ms,
        };
        Ok(ResolvedCapture {
            camera,
            snapshot,
            profile_name,
            profile: profile_snapshot,
            camera_summary,
            capture_id,
            accepted_at_ms,
            terminal_ms,
            deadlines,
            relative_path,
        })
    }


    /// Resolves and durably accepts one single-camera capture before exposing it to the persistent
    /// supervisor queue.  The returned catalog outcome is safe to use for both direct and
    /// submitted command semantics; duplicate keys never re-resolve a changed profile.
    #[allow(clippy::too_many_arguments)] // Stable command fields are intentionally explicit at this boundary.
    pub async fn submit_capture(
        &self,
        instance: String,
        request_id: String,
        requested_profile: Option<String>,
        timeout_ms: Option<u64>,
        metadata: serde_json::Map<String, serde_json::Value>,
        correlation_id: String,
        verb: &str,
        priority: crate::admission::CapturePriority,
    ) -> Result<crate::catalog::AcceptJobOutcome> {
        // Idempotency is deliberately based on caller-owned immutable arguments, before any
        // config defaults, generated identifiers, deadlines, or output paths are resolved.
        // An exact retry after a reload must return the original durable job rather than compare
        // a new resolution against its first acceptance.
        let mut canonical_arguments = serde_json::Map::new();
        canonical_arguments.insert(
            "instance".to_string(),
            serde_json::Value::String(instance.clone()),
        );
        canonical_arguments.insert(
            "requestId".to_string(),
            serde_json::Value::String(request_id.clone()),
        );
        if let Some(profile) = requested_profile.as_ref() {
            canonical_arguments.insert(
                "captureProfile".to_string(),
                serde_json::Value::String(profile.clone()),
            );
        }
        if let Some(timeout_ms) = timeout_ms {
            canonical_arguments.insert("timeoutMs".to_string(), timeout_ms.into());
        }
        canonical_arguments.insert(
            "metadata".to_string(),
            serde_json::Value::Object(metadata.clone()),
        );
        let canonical = serde_json::Value::Object(canonical_arguments);
        let request_hash = crate::idempotency::canonical_request_hash(&canonical, false)?;
        let ledger_key =
            crate::catalog::LedgerKey::new(instance.clone(), verb, request_id.clone())?;
        if let Some(existing) = self.catalog.job_by_ledger(ledger_key.clone()).await? {
            return Ok(if existing.request_hash == request_hash {
                crate::catalog::AcceptJobOutcome::Existing(existing)
            } else {
                crate::catalog::AcceptJobOutcome::Conflict
            });
        }
        // A paused camera runs its in-flight captures to completion but accepts no NEW work. Checked
        // AFTER the idempotency lookup (so an operator can still retry-to-poll a capture accepted
        // before the pause) and BEFORE any durable row or physical work (SOUTHBOUND.md §2.2).
        self.ensure_not_paused(&instance)?;

        let ResolvedCapture {
            camera,
            snapshot,
            profile: profile_snapshot,
            camera_summary,
            capture_id,
            accepted_at_ms,
            deadlines,
            relative_path,
            ..
        } = self.resolve_capture(&instance, requested_profile.as_deref(), timeout_ms)?;
        let trigger = crate::messages::CaptureTrigger::Command {
            request_id: request_id.clone(),
        };
        if let Err(error) = self.ensure_storage_capacity().await {
            if error.code() == crate::ErrorCode::StoragePressure {
                if let Some(existing) = self.catalog.job_by_ledger(ledger_key.clone()).await? {
                    return Ok(if existing.request_hash == request_hash {
                        crate::catalog::AcceptJobOutcome::Existing(existing)
                    } else {
                        crate::catalog::AcceptJobOutcome::Conflict
                    });
                }
            }
            return Err(error);
        }
        let job = crate::catalog::NewJob {
            capture_id: capture_id.clone(),
            instance: instance.clone(),
            ledger_key: Some(ledger_key),
            request_hash,
            canonical_request: canonical,
            effective_profile: serde_json::to_value(&profile_snapshot)?,
            deadlines: deadlines.clone(),
            trigger: serde_json::to_value(&trigger)?,
            origin_correlation_id: Some(correlation_id.clone()),
            intended_output: serde_json::json!({ "relativePath": relative_path.as_wire_path(), "backend": snapshot.backend.as_str() }),
            accepted_at_ms,
            group_id: None,
        };
        let submission = crate::jobs::JobSubmission {
            job,
            spec: crate::jobs::CaptureJobSpec {
                capture_id,
                instance: instance.clone(),
                profile: profile_snapshot,
                resource_group: camera.resource_group.clone(),
                relative_path,
                deadlines,
                accepted_at_ms,
                trigger,
                correlation_id,
                metadata,
                camera: camera_summary,
                group_size: None,
            },
            priority,
        };
        let dispatcher = self.dispatcher(&instance)?;
        self.engine(&instance)?
            .accept_and_queue(&dispatcher, submission)
            .await
    }


    #[allow(clippy::too_many_arguments)] // The group builder preserves each immutable acceptance fact.
    /// Builds one member of a synchronised group capture.
    ///
    /// `group_size` is the member count, and it is a PARAMETER because the builder cannot derive it
    /// and must not invent it. It used to return `Some(2)` -- a value picked to satisfy the `size >= 2`
    /// check the durable layer applies -- and rely on the caller to overwrite it with the true count.
    /// That is a lie the type system was happy to carry: a second caller, or a reordering that pushed
    /// the submission before the fix-up line, ships a five-camera group as a two-member group, and the
    /// durable layer accepts it without complaint because two is a legal size.
    pub(super) async fn build_group_submission(
        &self,
        instance: &str,
        group_size: usize,
        request_id: &str,
        capture_group_id: &str,
        requested_profile: Option<&str>,
        timeout_ms: Option<u64>,
        metadata: serde_json::Map<String, serde_json::Value>,
        correlation_id: String,
    ) -> Result<crate::jobs::JobSubmission> {
        let camera = self.registry.camera_config(instance)?;
        if !camera.enabled {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::CameraDisabled,
                "camera is disabled",
            ));
        }
        let ResolvedCapture {
            camera,
            snapshot,
            profile_name,
            profile: profile_snapshot,
            camera_summary,
            capture_id,
            accepted_at_ms,
            terminal_ms,
            deadlines,
            relative_path,
        } = self.resolve_capture(instance, requested_profile, timeout_ms)?;
        let trigger = crate::messages::CaptureTrigger::GroupCommand {
            request_id: request_id.to_owned(),
            capture_group_id: capture_group_id.to_owned(),
        };
        let canonical = serde_json::json!({
            "requestId": request_id,
            "captureGroupId": capture_group_id,
            "instance": instance,
            "captureProfile": profile_name,
            "timeoutMs": terminal_ms,
            "metadata": metadata,
            "effectiveProfile": profile_snapshot,
            "deadlines": {
                "terminalAtMs": deadlines.terminal_at_ms,
                "queueAtMs": deadlines.queue_at_ms,
                "captureAtMs": deadlines.capture_at_ms,
                "encodeAtMs": deadlines.encode_at_ms,
                "persistAtMs": deadlines.persist_at_ms,
            },
            "intendedOutput": { "relativePath": relative_path.as_wire_path(), "backend": snapshot.backend.as_str() },
        });
        let job = crate::catalog::NewJob {
            capture_id: capture_id.clone(),
            instance: instance.to_owned(),
            ledger_key: None,
            request_hash: crate::idempotency::canonical_request_hash(&canonical, false)?,
            canonical_request: canonical,
            effective_profile: serde_json::to_value(&profile_snapshot)?,
            deadlines: deadlines.clone(),
            trigger: serde_json::to_value(&trigger)?,
            origin_correlation_id: Some(correlation_id.clone()),
            intended_output: serde_json::json!({ "relativePath": relative_path.as_wire_path(), "backend": snapshot.backend.as_str() }),
            accepted_at_ms,
            group_id: Some(capture_group_id.to_owned()),
        };
        Ok(crate::jobs::JobSubmission {
            job,
            spec: crate::jobs::CaptureJobSpec {
                capture_id,
                instance: instance.to_owned(),
                profile: profile_snapshot,
                resource_group: camera.resource_group.clone(),
                relative_path,
                deadlines,
                accepted_at_ms,
                trigger,
                correlation_id,
                metadata,
                camera: camera_summary,
                group_size: Some(group_size),
            },
            priority: crate::admission::CapturePriority::Direct,
        })
    }


    pub(super) async fn submit_group(
        &self,
        body: GroupCaptureRequest,
        correlation_id: String,
        priority: crate::admission::CapturePriority,
        deferred_token: Option<DeferredReplyToken>,
    ) -> Result<crate::catalog::GroupRecord> {
        // Preserve only caller-owned group arguments in the idempotency record.  Member capture
        // IDs, group IDs, default profiles, deadlines, and output paths are acceptance facts;
        // including them here would turn an exact retry into a conflict.
        let mut canonical_arguments = serde_json::Map::new();
        canonical_arguments.insert(
            "requestId".to_string(),
            serde_json::Value::String(body.request_id.clone()),
        );
        canonical_arguments.insert(
            "instances".to_string(),
            serde_json::to_value(&body.instances)?,
        );
        if let Some(profile) = body.capture_profile.as_ref() {
            canonical_arguments.insert(
                "captureProfile".to_string(),
                serde_json::Value::String(profile.clone()),
            );
        }
        canonical_arguments.insert(
            "profileOverrides".to_string(),
            serde_json::to_value(&body.profile_overrides)?,
        );
        if let Some(timeout_ms) = body.timeout_ms {
            canonical_arguments.insert("timeoutMs".to_string(), timeout_ms.into());
        }
        canonical_arguments.insert(
            "metadata".to_string(),
            serde_json::Value::Object(body.metadata.clone()),
        );
        let canonical = serde_json::Value::Object(canonical_arguments);
        let request_hash = crate::idempotency::canonical_request_hash(&canonical, true)?;
        let ledger_key = crate::catalog::LedgerKey::new(
            "main",
            CommandVerb::CaptureGroup.as_str(),
            body.request_id.clone(),
        )?;
        if let Some(group) = self.catalog.group_by_ledger(ledger_key.clone()).await? {
            if group.request_hash != request_hash {
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::IdempotencyConflict,
                    "requestId was already used with different immutable group arguments",
                ));
            }
            if let Some(token) = deferred_token {
                if group.state.is_terminal() {
                    token
                        .settle_success(Some(group_terminal_json(&group)))
                        .await
                        .map_err(|_| {
                            crate::CameraError::rejected(
                                crate::ErrorCode::BackendError,
                                "deferred group reply could not be settled",
                            )
                        })?;
                } else {
                    self.waiters.register_group(
                        group.group_id.clone(),
                        Arc::new(token) as Arc<dyn CaptureWaiter>,
                    )?;
                }
            }
            return Ok(group);
        }

        let config = self.config_snapshot()?;
        body.validate(
            config.global.limits.max_cameras_per_group,
            config.global.limits.max_metadata_bytes,
        )?;
        // Resolve every member before creating any durable row. This gives the all-or-nothing
        // error surface required by the group contract. A group is refused outright if ANY member is
        // paused -- a partial group is not the contract (SOUTHBOUND.md §2.2).
        for instance in &body.instances {
            let resolved = self.registry.resolve_actuation_instance(Some(instance))?;
            self.ensure_not_paused(&resolved)?;
        }
        let group_id = format!("grp_{}", uuid::Uuid::now_v7());
        let mut submissions = Vec::with_capacity(body.instances.len());
        for instance in &body.instances {
            let selected = body
                .profile_overrides
                .get(instance)
                .map(String::as_str)
                .or(body.capture_profile.as_deref());
            let mut submission = self
                .build_group_submission(
                    instance,
                    body.instances.len(),
                    &body.request_id,
                    &group_id,
                    selected,
                    body.timeout_ms,
                    body.metadata.clone(),
                    correlation_id.clone(),
                )
                .await?;
            submission.priority = priority;
            submissions.push(submission);
        }
        let new_group = crate::catalog::NewGroup {
            group_id: group_id.clone(),
            ledger_key: ledger_key.clone(),
            request_hash,
            canonical_request: canonical,
            origin_correlation_id: Some(correlation_id),
            accepted_at_ms: chrono::Utc::now().timestamp_millis(),
            members: submissions
                .iter()
                .map(|submission| submission.job.clone())
                .collect(),
        };
        if let Err(error) = self.ensure_storage_capacity().await {
            if error.code() == crate::ErrorCode::StoragePressure {
                if let Some(group) = self.catalog.group_by_ledger(ledger_key.clone()).await? {
                    if group.request_hash != request_hash {
                        return Err(crate::CameraError::rejected(
                            crate::ErrorCode::IdempotencyConflict,
                            "requestId was already used with different immutable group arguments",
                        ));
                    }
                    if let Some(token) = deferred_token {
                        if group.state.is_terminal() {
                            token
                                .settle_success(Some(group_terminal_json(&group)))
                                .await
                                .map_err(|_| {
                                    crate::CameraError::rejected(
                                        crate::ErrorCode::BackendError,
                                        "deferred group reply could not be settled",
                                    )
                                })?;
                        } else {
                            self.waiters.register_group(
                                group.group_id.clone(),
                                Arc::new(token) as Arc<dyn CaptureWaiter>,
                            )?;
                        }
                    }
                    return Ok(group);
                }
            }
            return Err(error);
        }
        let outcome = self.catalog.accept_group(new_group).await?;
        let group = match outcome {
            crate::catalog::AcceptGroupOutcome::Inserted(group) => group,
            crate::catalog::AcceptGroupOutcome::Existing(group) => {
                if let Some(token) = deferred_token {
                    if group.state.is_terminal() {
                        token
                            .settle_success(Some(group_terminal_json(&group)))
                            .await
                            .map_err(|_| {
                                crate::CameraError::rejected(
                                    crate::ErrorCode::BackendError,
                                    "deferred group reply could not be settled",
                                )
                            })?;
                    } else {
                        self.waiters.register_group(
                            group.group_id.clone(),
                            Arc::new(token) as Arc<dyn CaptureWaiter>,
                        )?;
                    }
                }
                return Ok(group);
            }
            crate::catalog::AcceptGroupOutcome::Conflict => {
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::IdempotencyConflict,
                    "requestId was already used with different immutable group arguments",
                ));
            }
        };
        if let Some(token) = deferred_token {
            self.waiters.register_group(
                group.group_id.clone(),
                Arc::new(token) as Arc<dyn CaptureWaiter>,
            )?;
        }
        // Group ACCEPTED and QUEUED are separate durable commits. Queue every member in one
        // catalog transaction before exposing any descriptor, then hand those already-queued
        // records to their independent camera supervisors.
        self.catalog
            .queue_group(
                group.group_id.clone(),
                chrono::Utc::now().timestamp_millis(),
            )
            .await?;
        for submission in submissions {
            let dispatcher = self.dispatcher(&submission.spec.instance)?;
            self.engine(&submission.spec.instance)?
                .queue_preaccepted(&dispatcher, submission)
                .await?;
        }
        self.waiters
            .complete_group_if_terminal(&group.group_id)
            .await;
        self.catalog
            .group(group.group_id.clone())
            .await?
            .ok_or_else(|| {
                crate::CameraError::Catalog("accepted capture group disappeared".to_string())
            })
    }


    pub(super) async fn discover(&self, body: DiscoverRequest) -> Result<serde_json::Value> {
        body.validate()?;
        let config = self.config_snapshot()?;
        if !config.global.discovery.enabled {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::UnsupportedCapability,
                "camera discovery is disabled by configuration",
            ));
        }
        let query = serde_json::json!({ "backends": body.backends });
        if body.cursor.is_some() {
            let (candidates, next_cursor, completed_at) = self.cursors.snapshot_page(
                "discover",
                &query,
                body.cursor.as_deref(),
                None,
                None,
                usize::from(body.limit),
            )?;
            return Ok(serde_json::json!({
                "candidates": candidates,
                "nextCursor": next_cursor,
                // A continuation is a view of the original retained result, not a second probe.
                "completedAt": completed_at,
            }));
        }
        let wanted = if body.backends.is_empty() {
            None
        } else {
            Some(body.backends.clone())
        };
        let candidates = self
            .discover_candidates(
                &config,
                wanted.as_deref(),
                Duration::from_millis(body.timeout_ms),
                self.cancellation.child_token(),
            )
            .await?
            .into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let completed_at = serde_json::to_value(chrono::Utc::now())?;
        let (candidates, next_cursor, completed_at) = self.cursors.snapshot_page(
            "discover",
            &query,
            None,
            Some(candidates),
            Some(completed_at),
            usize::from(body.limit),
        )?;
        Ok(serde_json::json!({
            "candidates": candidates,
            "nextCursor": next_cursor,
            "completedAt": completed_at,
        }))
    }


    /// Executes one credential-free discovery pass, bounded across all distinct configured
    /// backend kinds.  The page size never affects the underlying snapshot: continuations may
    /// safely page through every retained discovery result up to the configured hard maximum.
    pub(super) async fn discover_candidates(
        &self,
        config: &AdapterConfig,
        wanted: Option<&[crate::model::BackendKind]>,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Vec<DiscoveryCandidate>> {
        let mut candidates = Vec::new();
        let mut attempted = Vec::new();
        for camera in &config.instances {
            let kind = camera.backend.kind();
            if kind == crate::model::BackendKind::Sim
                || wanted.is_some_and(|wanted| !wanted.contains(&kind))
                || attempted.contains(&kind)
            {
                continue;
            }
            let remaining = config
                .global
                .discovery
                .max_results
                .saturating_sub(candidates.len());
            if remaining == 0 {
                break;
            }
            attempted.push(kind);
            let factory = self
                .backend_context
                .factory_for(&camera.backend, &config.global)?;
            let discovered = factory
                .discover(crate::backend::DiscoveryRequest {
                    eligible_interfaces: config.global.discovery.eligible_interfaces.clone(),
                    timeout,
                    max_results: remaining,
                    cancellation: cancellation.child_token(),
                })
                .await?;
            for candidate in discovered {
                if candidates.len() == config.global.discovery.max_results {
                    break;
                }
                if !candidates.contains(&candidate) {
                    candidates.push(candidate);
                }
            }
        }
        Ok(candidates)
    }


    /// Returns a fresh view of retained discovery observations after excluding cameras already
    /// represented by a stable configured selector.  This is read-only and never opens a session.
    pub(super) fn unconfigured_discoveries(&self, config: &AdapterConfig) -> Result<Vec<serde_json::Value>> {
        let cache = self.discovery_cache.lock().map_err(|_| {
            crate::CameraError::Catalog("discovery cache is unavailable".to_string())
        })?;
        cache
            .candidates
            .iter()
            .filter(|candidate| !candidate_is_configured(candidate, &config.instances))
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::CameraError::from)
    }


    /// Samples every camera's reachability for the heartbeat's per-instance connectivity surface.
    ///
    /// Q5: camera presence used to be PULL-ONLY. A camera's state lived in `CameraRegistry` and could
    /// be learned only by asking -- `sb/list`, `sb/status` -- so a consumer wanting to know that a
    /// camera had dropped had to poll for it, and nothing was ever published. The assumption that
    /// camera connectivity was already reaching the standard health surface did not hold: nothing was
    /// registered against it.
    ///
    /// EdgeCommons ships exactly the mechanism this needs. The `main` state keepalive carries an
    /// `instances[]` array, fed by a provider, precisely so a multi-instance adapter can report each
    /// connection's health without minting a UNS instance per camera. This is that provider.
    ///
    /// Each optional member of the element carries what it was designed to carry, and the difference
    /// matters to whoever reads it:
    ///
    /// * `connected` -- the normalized flag every consumer can act on without knowing what a camera is.
    /// * `state` -- this component's own richer condition token. `BACKOFF` and `CONNECTING` are both
    ///   `connected: false`, and an operator deciding whether to intervene needs to know which.
    /// * `detail` -- why it is down, in the camera's own words, when it has given us any.
    /// * `attributes` -- the open bag, for what only a camera adapter understands: the backend, the
    ///   connection generation, and the stable code of the error that put it there.
    ///
    /// The same element shape answers core's built-in `status` verb, so one sampler serves both the
    /// push and the pull.
    #[must_use]
    pub fn camera_connectivity(&self) -> Vec<edgecommons::heartbeat::InstanceConnectivity> {
        let Ok(snapshots) = self.registry.snapshots(MAX_CONNECTIVITY_INSTANCES) else {
            return Vec::new();
        };
        snapshots
            .into_iter()
            .map(|snapshot| {
                let connected = snapshot.state == CameraConnectionState::Online;
                let mut attributes = serde_json::Map::new();
                attributes.insert(
                    "backend".to_owned(),
                    serde_json::to_value(snapshot.backend).unwrap_or(serde_json::Value::Null),
                );
                attributes.insert(
                    "generation".to_owned(),
                    serde_json::Value::from(snapshot.generation),
                );
                if let Some(error) = snapshot.last_error.as_ref() {
                    attributes.insert(
                        "lastErrorCode".to_owned(),
                        serde_json::Value::from(error.code.clone()),
                    );
                }
                let state = serde_json::to_value(snapshot.state)
                    .ok()
                    .and_then(|token| token.as_str().map(str::to_owned));
                let detail = snapshot
                    .last_error
                    .as_ref()
                    .filter(|_| !connected)
                    .map(|error| error.message.clone());

                let sample = edgecommons::heartbeat::InstanceConnectivity::new(
                    snapshot.instance,
                    connected,
                    detail,
                )
                .with_attributes(attributes);
                match state {
                    Some(state) => sample.with_state(state),
                    None => sample,
                }
            })
            .collect()
    }


    /// Answers `sb/queue-status`.
    ///
    /// Read-only and cheap: the admission and dispatcher numbers are atomics, and the durable
    /// counts are one grouped COUNT rather than a page of rows -- which matters, because the moment
    /// an operator asks this question is exactly the moment the catalog is least able to afford a
    /// scan.
    pub async fn queue_status(&self, instance: Option<String>) -> Result<QueueStatus> {
        if let Some(instance) = instance.as_deref() {
            // Rejects an unknown camera the same way every other targeted verb does.
            self.registry.snapshot(instance)?;
        }
        let config = self.config_snapshot()?;
        // The fleet queue evicts cancelled and expired entries itself -- each carries a watcher task
        // -- so there is no longer a sweep to run before the numbers can be trusted.
        let cameras = config
            .instances
            .iter()
            .filter(|camera| {
                instance
                    .as_deref()
                    .is_none_or(|target| target == camera.id.as_str())
            })
            .map(|camera| CameraQueueDepth {
                instance: camera.id.clone(),
                queued: self.scheduler.pending_for(&camera.id),
                capacity: self.scheduler.capacity_per_camera(),
            })
            .collect::<Vec<_>>();
        let dispatch_queued = self.scheduler.pending();

        let durable = self
            .catalog
            .count_jobs_by_state(instance, NON_TERMINAL_JOB_STATES.to_vec())
            .await?;
        let total_for = |states: &[crate::model::JobState]| -> u64 {
            states
                .iter()
                .filter_map(|state| durable.get(crate::catalog::job_state_token(*state)))
                .sum()
        };
        let durable_backlog = total_for(&BACKLOG_JOB_STATES);
        let durable_in_flight = total_for(&NON_TERMINAL_JOB_STATES) - durable_backlog;

        Ok(QueueStatus {
            admission: self.admission.snapshot(),
            limits: QueueLimits {
                max_concurrent_captures: config.global.limits.max_concurrent_captures,
                max_in_flight_bytes: config.global.limits.max_in_flight_bytes,
                max_queued_captures_per_camera: config.global.limits.max_queued_captures_per_camera,
                max_pending_captures: self.scheduler.capacity(),
            },
            cameras,
            dispatch_queued,
            durable,
            durable_backlog,
            durable_in_flight,
        })
    }


    /// Answers `sb/queue-clear` -- the break-glass drain.
    ///
    /// Cancels the durable backlog (and, only if asked, work already in flight) through the same
    /// `cancel_active` path a single `sb/capture-cancel` uses, so a drained capture reaches the same
    /// terminal state, publishes the same terminal message, and releases the same admission capacity
    /// as one cancelled by hand. There is no second cancellation mechanism to keep correct.
    ///
    /// It pages, because the whole point is that it is reached for when the backlog has run away,
    /// and a drain that tried to hold a runaway backlog in memory would fail exactly when it was
    /// needed. It reports what it could not cancel rather than claiming a clean sweep.
    pub(super) async fn clear_queue(
        &self,
        instance: Option<String>,
        include_in_flight: bool,
        reason: String,
    ) -> Result<QueueClearOutcome> {
        if let Some(instance) = instance.as_deref() {
            self.registry.snapshot(instance)?;
        }
        let states = if include_in_flight {
            NON_TERMINAL_JOB_STATES.to_vec()
        } else {
            BACKLOG_JOB_STATES.to_vec()
        };
        let mut outcome = QueueClearOutcome {
            cancelled: 0,
            already_terminal: 0,
            failed: Vec::new(),
        };
        // Cancelling moves a row out of the queried states, so each page is drawn fresh from the
        // head rather than walked with a cursor: the set shrinks under us by design.
        loop {
            let page = self
                .catalog
                .jobs_page(instance.clone(), states.clone(), None, 1_000)
                .await?;
            if page.is_empty() {
                return Ok(outcome);
            }
            let drained = page.len();
            for job in page {
                match self.engine(&job.instance) {
                    Ok(engine) => match engine.cancel_active(&job.capture_id, reason.clone()).await
                    {
                        Ok(result) if result.cancelled => outcome.cancelled += 1,
                        Ok(_) => outcome.already_terminal += 1,
                        Err(error) => outcome.failed.push(QueueClearFailure {
                            capture_id: job.capture_id,
                            error: error.to_string(),
                        }),
                    },
                    Err(error) => outcome.failed.push(QueueClearFailure {
                        capture_id: job.capture_id,
                        error: error.to_string(),
                    }),
                }
            }
            // Every row in the page resisted the drain. Another pass would fetch the same rows and
            // fail on them again, forever, so stop and say so.
            if outcome.failed.len() >= drained && outcome.cancelled == 0 {
                return Ok(outcome);
            }
        }
    }


    /// Answers `sb/queue-status` for the command layer.
    pub(super) async fn queue_status_command(
        &self,
        body: commands::QueueStatusRequest,
    ) -> Result<serde_json::Value> {
        body.validate()?;
        Ok(serde_json::to_value(
            self.queue_status(body.instance).await?,
        )?)
    }


    /// Answers `sb/queue-clear` for the command layer.
    pub(super) async fn queue_clear_command(
        &self,
        body: commands::QueueClearRequest,
    ) -> Result<serde_json::Value> {
        body.validate()?;
        let commands::QueueClearRequest {
            request_id,
            instance,
            all_cameras: _,
            include_in_flight,
            reason,
        } = body;
        let canonical_reason = reason.clone();
        let reason = reason.unwrap_or_else(|| "operator queue drain".to_string());
        let canonical = serde_json::json!({
            "requestId": &request_id,
            "instance": &instance,
            "includeInFlight": include_in_flight,
            "reason": canonical_reason,
        });
        // Ledgered like every other mutating verb: a retried drain returns the original outcome
        // instead of cancelling a second wave of work the operator never saw.
        let key = crate::catalog::LedgerKey::new(
            instance.clone().unwrap_or_else(|| "main".to_string()),
            CommandVerb::QueueClear.as_str(),
            request_id,
        )?;
        self.cancel_with_ledger(
            key,
            canonical,
            serde_json::json!({
                "cancelled": 0,
                "alreadyTerminal": 0,
                "failed": [],
            }),
            async {
                let outcome = self
                    .clear_queue(instance, include_in_flight, reason)
                    .await?;
                Ok(serde_json::to_value(outcome)?)
            },
        )
        .await
    }


    pub(super) async fn cancel_capture(&self, body: CancelRequest) -> Result<serde_json::Value> {
        body.validate()?;
        let CancelRequest {
            request_id,
            capture_id,
            capture_group_id,
            reason,
        } = body;
        let canonical_reason = reason.clone();
        let reason = reason.unwrap_or_else(|| "operator cancellation".to_string());
        if let Some(capture_id) = capture_id {
            let job = self.catalog.job(&capture_id).await?.ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::CaptureNotFound,
                    "capture was not found",
                )
            })?;
            let canonical = serde_json::json!({
                "requestId": &request_id,
                "target": { "kind": "capture", "captureId": &capture_id },
                "reason": canonical_reason,
            });
            let key = crate::catalog::LedgerKey::new(
                job.instance.clone(),
                CommandVerb::CaptureCancel.as_str(),
                request_id,
            )?;
            return self
                .cancel_with_ledger(
                    key,
                    canonical,
                    serde_json::json!({
                        "captureId": capture_id,
                        "cancelled": false,
                        "state": job.state,
                        "cancellationInProgress": false,
                    }),
                    async {
                        let outcome = self
                            .engine(&job.instance)?
                            .cancel_active(&capture_id, reason)
                            .await?;
                        Ok(serde_json::json!({
                            "captureId": capture_id,
                            "cancelled": outcome.cancelled,
                            "state": outcome.state,
                            "cancellationInProgress": outcome.cancellation_in_progress,
                        }))
                    },
                )
                .await;
        }

        let capture_group_id = capture_group_id.ok_or_else(|| {
            crate::CameraError::rejected(
                crate::ErrorCode::BadArgs,
                "captureGroupId is required",
            )
        })?;
        let group = self
            .catalog
            .group(&capture_group_id)
            .await?
            .ok_or_else(|| {
                crate::CameraError::rejected(
                    crate::ErrorCode::CaptureNotFound,
                    "capture group was not found",
                )
            })?;
        let canonical = serde_json::json!({
            "requestId": &request_id,
            "target": { "kind": "capture-group", "captureGroupId": &capture_group_id },
            "reason": canonical_reason,
        });
        let key = crate::catalog::LedgerKey::new(
            "main",
            CommandVerb::CaptureCancel.as_str(),
            request_id,
        )?;
        self.cancel_with_ledger(
            key,
            canonical,
            serde_json::json!({
                "captureGroupId": capture_group_id,
                "cancelledMembers": 0,
                "unchangedMembers": group.members.len(),
                "members": group.members.iter().map(|member| serde_json::json!({
                    "captureId": member.capture_id,
                    "instance": member.instance,
                    "cancelled": false,
                    "state": member.state,
                    "cancellationInProgress": false,
                })).collect::<Vec<_>>(),
            }),
            async {
                let mut cancelled_members = 0_u64;
                let mut unchanged_members = 0_u64;
                let mut members = Vec::with_capacity(group.members.len());
                for member in group.members {
                    let outcome = if member.state.is_terminal() {
                        unchanged_members = unchanged_members.saturating_add(1);
                        crate::jobs::CancelResult {
                            cancelled: false,
                            state: member.state,
                            cancellation_in_progress: false,
                        }
                    } else {
                        let outcome = self
                            .engine(&member.instance)?
                            .cancel_active(&member.capture_id, reason.clone())
                            .await?;
                        if outcome.cancelled {
                            cancelled_members = cancelled_members.saturating_add(1);
                        } else {
                            unchanged_members = unchanged_members.saturating_add(1);
                        }
                        outcome
                    };
                    members.push(serde_json::json!({
                        "captureId": member.capture_id,
                        "instance": member.instance,
                        "cancelled": outcome.cancelled,
                        "state": outcome.state,
                        "cancellationInProgress": outcome.cancellation_in_progress,
                    }));
                }
                Ok(serde_json::json!({
                    "captureGroupId": capture_group_id,
                    "cancelledMembers": cancelled_members,
                    "unchangedMembers": unchanged_members,
                    "members": members,
                }))
            },
        )
        .await
    }


    async fn cancel_with_ledger<F>(
        &self,
        key: crate::catalog::LedgerKey,
        canonical: serde_json::Value,
        in_progress: serde_json::Value,
        operation: F,
    ) -> Result<serde_json::Value>
    where
        F: std::future::Future<Output = Result<serde_json::Value>>,
    {
        match self
            .catalog
            .begin_command(
                key.clone(),
                crate::idempotency::canonical_request_hash(&canonical, false)?,
                canonical,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?
        {
            crate::catalog::BeginCommandOutcome::Conflict => Err(crate::CameraError::rejected(
                crate::ErrorCode::IdempotencyConflict,
                "requestId was already used with different cancellation arguments",
            )),
            crate::catalog::BeginCommandOutcome::Existing(record) => match record.state {
                crate::catalog::LedgerState::OutcomeUnknown => Err(crate::CameraError::rejected(
                    crate::ErrorCode::PreviousOutcomeUnknown,
                    "the prior cancellation outcome is unknown after restart",
                )),
                _ => Ok(record.reply.unwrap_or(in_progress)),
            },
            crate::catalog::BeginCommandOutcome::Started(_) => {
                self.catalog
                    .record_command_acceptance(
                        key.clone(),
                        in_progress,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?;
                match operation.await {
                    Ok(response) => {
                        self.catalog
                            .complete_command(
                                key,
                                crate::catalog::LedgerState::Succeeded,
                                response.clone(),
                                None,
                                None,
                                chrono::Utc::now().timestamp_millis(),
                            )
                            .await?;
                        Ok(response)
                    }
                    Err(error) => {
                        let reply = serde_json::json!({
                            "errorCode": error.code().as_str(),
                            "errorMessage": command_error(&error).message,
                        });
                        let _ = self
                            .catalog
                            .complete_command(
                                key,
                                crate::catalog::LedgerState::Failed,
                                reply,
                                Some(error.code().as_str().to_string()),
                                Some(command_error(&error).message),
                                chrono::Utc::now().timestamp_millis(),
                            )
                            .await;
                        Err(error)
                    }
                }
            }
        }
    }


    pub(super) async fn reconnect(&self, body: ReconnectRequest) -> Result<serde_json::Value> {
        body.validate()?;
        let instance = self
            .registry
            .resolve_actuation_instance(body.instance.as_deref())?;
        let canonical = serde_json::json!({ "instance": instance, "requestId": body.request_id, "reason": body.reason });
        let key = crate::catalog::LedgerKey::new(
            instance.clone(),
            crate::catalog::RECONNECT_VERB,
            body.request_id,
        )?;
        match self
            .catalog
            .begin_command(
                key.clone(),
                crate::idempotency::canonical_request_hash(&canonical, false)?,
                canonical,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?
        {
            crate::catalog::BeginCommandOutcome::Conflict => Err(crate::CameraError::rejected(
                crate::ErrorCode::IdempotencyConflict,
                "requestId was already used with different reconnect arguments",
            )),
            crate::catalog::BeginCommandOutcome::Existing(record) => match record.state {
                crate::catalog::LedgerState::OutcomeUnknown => Err(crate::CameraError::rejected(
                    crate::ErrorCode::PreviousOutcomeUnknown,
                    "the prior reconnect outcome is unknown after restart",
                )),
                _ => Ok(record.reply.unwrap_or_else(|| {
                    serde_json::json!({
                        "operationId": format!("op_{}", record.key.request_id),
                        "instance": instance,
                        "state": "ACCEPTED",
                    })
                })),
            },
            crate::catalog::BeginCommandOutcome::Started(_) => {
                let operation = serde_json::json!({
                    "operationId": format!("op_{}", uuid::Uuid::now_v7()),
                    "instance": instance,
                    "state": "ACCEPTED",
                });
                self.catalog
                    .record_command_acceptance(
                        key.clone(),
                        operation.clone(),
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?;
                if let Ok(cameras) = self.cameras.read() {
                    if let Some(session) = cameras.get(&instance).and_then(|slot| slot.session.as_ref())
                    {
                        session.cancellation.cancel();
                    }
                }
                // Signalling the session cancellation completes this operation: reconnect is a
                // bounded, idempotent request to re-establish a session and performs no physical
                // actuation that could half-happen, so nothing hazardous is left in flight. The
                // ledger is therefore settled here rather than left IN_PROGRESS forever — such a
                // row is fenced to OUTCOME_UNKNOWN on the next start, which no retention DELETE
                // can ever match, and which would make every retry answer
                // PREVIOUS_OUTCOME_UNKNOWN for the life of the state database.
                self.catalog
                    .complete_command(
                        key,
                        crate::catalog::LedgerState::Succeeded,
                        operation.clone(),
                        None,
                        None,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?;
                Ok(operation)
            }
        }
    }


    /// Arms the mandatory stop for a camera that has just been told to move (DESIGN §15.5).
    ///
    /// Returns the deadline the caller is promised, or `None` if the timer could not be armed --
    /// and `None` is never reported as a stop deadline, because advertising one that nothing will
    /// honour is worse than admitting there is none.
    ///
    /// The stop goes down the SAFETY lane, not the ordinary one. That is the whole point of the
    /// lane: it is non-evictable, it is popped before any other work, and it pre-empts a running
    /// capture. An ordinary control would queue behind exactly the work that is keeping the camera
    /// busy while it moves.
    fn arm_motion_stop(
        &self,
        instance: &str,
        timeout: Duration,
        axes: (bool, bool, bool),
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        let (pan, tilt, zoom) = axes;
        if !pan && !tilt && !zoom {
            // A zero velocity is not motion, so there is nothing to stop.
            return None;
        }
        let token = CancellationToken::new();
        let ptz_ms = self
            .config_snapshot()
            .map_or(10_000, |config| config.global.timeouts.ptz_ms);
        let previous = self.cameras.write().ok()?.get_mut(instance)?.motion_stop.replace(ArmedStop {
            cancellation: token.clone(),
            pan,
            tilt,
            zoom,
            ptz_ms,
        });
        // A superseding move retires the timer the previous one armed, or the old deadline would
        // stop the NEW motion early.
        if let Some(previous) = previous {
            previous.cancellation.cancel();
        }

        // The CURRENT actor is looked up when the timer fires, not captured now: a reconnect replaces
        // the actor, and a stop pushed at a handle the camera no longer answers is not a stop.
        let cameras = Arc::clone(&self.cameras);
        let instance = instance.to_owned();
        let deadline = tokio::time::Instant::now() + timeout;
        let shutting_down = self.cancellation.clone();
        self.spawn_task(async move {
            tokio::select! {
                () = token.cancelled() => return,
                // The component is stopping, and `shutdown` delivers the stop through the actor
                // before it cancels anything (DESIGN 20.2 step 2). Sitting here until the deadline
                // would hold shutdown open for as long as the move was allowed to last.
                //
                // That claim used to be false. This arm returned, `shutdown` stopped nothing, and
                // the camera went on moving -- the comment pointed at a mechanism that did not exist.
                () = shutting_down.cancelled() => return,
                () = tokio::time::sleep_until(deadline) => {}
            }
            // The component shutting down does not excuse a moving camera; the actor's own teardown
            // delivers a stop in that case, and this one is harmless if it loses the race.
            let actor = cameras.read().ok().and_then(|cameras| {
                cameras
                    .get(&instance)
                    .and_then(|slot| slot.session.as_ref())
                    .map(|session| session.actor.clone())
            });
            let Some(actor) = actor else {
                tracing::warn!(
                    instance = %instance,
                    "a continuous move reached its mandatory stop deadline but its camera is gone"
                );
                return;
            };
            let stop = crate::admission::SafetyStop {
                pan,
                tilt,
                zoom,
                deadline: tokio::time::Instant::now() + Duration::from_millis(ptz_ms),
            };
            match actor.safety_stop(stop) {
                Ok(()) => tracing::info!(
                    instance = %instance,
                    "continuous motion reached its mandatory stop deadline; a safety stop was queued"
                ),
                Err(error) => tracing::error!(
                    instance = %instance,
                    error = %error,
                    "a moving camera could not be sent its mandatory stop"
                ),
            }
            if let Ok(mut cameras) = cameras.write() {
                if let Some(slot) = cameras.get_mut(&instance) {
                    slot.motion_stop = None;
                }
            }
        })
        .ok()?;
        Some(chrono::Utc::now() + chrono::Duration::from_std(timeout).ok()?)
    }


    /// Retires a camera's armed stop: its motion has been ended by something else.
    fn disarm_motion_stop(&self, instance: &str) {
        let armed = self
            .cameras
            .write()
            .ok()
            .and_then(|mut cameras| cameras.get_mut(instance)?.motion_stop.take());
        if let Some(armed) = armed {
            armed.cancellation.cancel();
        }
    }


    /// Stops every camera that is still moving, through the actor that is about to be taken away.
    ///
    /// DESIGN 15.5 makes the DEADLINE, not the requester, responsible for stopping a camera in
    /// continuous motion -- and the timer that owns that deadline resolves its actor lazily, when it
    /// fires. That is right for a reconnect (a stop pushed at a handle the camera no longer answers is
    /// not a stop) and catastrophic for everything else: retire the supervisor, disable the camera,
    /// drop it from the roster, or shut the component down, and the timer wakes to find no actor, logs
    /// that the camera "is gone", and returns. The camera is not gone. It is still moving.
    ///
    /// DESIGN 20.2 step 2 -- "send safety stop to cameras with active continuous PTZ motion" -- was
    /// simply not implemented. The actor has always known how to deliver a stop that outlives its own
    /// cancellation (`drain_controls_for_shutdown`), and there is a test proving it does. Nothing ever
    /// gave it one: the timer at the mandatory deadline was the ONLY producer of a `SafetyStop` in the
    /// component, and on shutdown it deliberately stands down.
    ///
    /// So this is the missing producer. Every path that is about to take an actor away calls it FIRST,
    /// while there is still an actor to deliver through. `instances` selects the cameras being retired;
    /// `None` means all of them, which is what shutdown wants.
    pub(super) fn deliver_mandatory_stops(&self, instances: Option<&[String]>) -> usize {
        // Taken and delivered under one lock: the armed stop and the actor it must go through are
        // now the same entry, so there is no window in which one is found and the other has gone.
        let armed = {
            let Ok(mut cameras) = self.cameras.write() else {
                return 0;
            };
            cameras
                .iter_mut()
                .filter(|(instance, _)| {
                    instances.is_none_or(|retiring| retiring.contains(instance))
                })
                .filter_map(|(instance, slot)| {
                    let armed = slot.motion_stop.take()?;
                    let actor = slot.session.as_ref().map(|session| session.actor.clone());
                    Some((instance.clone(), armed, actor))
                })
                .collect::<Vec<_>>()
        };
        if armed.is_empty() {
            return 0;
        }

        let mut delivered = 0;
        for (instance, armed, actor) in armed {
            // The stop is being delivered now, so the deadline must not deliver a second one at an
            // actor that by then belongs to a different session.
            armed.cancellation.cancel();
            let Some(actor) = actor else {
                tracing::error!(
                    instance = %instance,
                    "a camera in continuous motion is being retired with no actor to stop it through"
                );
                continue;
            };
            let stop = crate::admission::SafetyStop {
                pan: armed.pan,
                tilt: armed.tilt,
                zoom: armed.zoom,
                deadline: tokio::time::Instant::now() + Duration::from_millis(armed.ptz_ms),
            };
            match actor.safety_stop(stop) {
                Ok(()) => {
                    delivered += 1;
                    tracing::info!(
                        instance = %instance,
                        "a camera in continuous motion was sent its stop before its actor was retired"
                    );
                }
                Err(error) => tracing::error!(
                    instance = %instance,
                    error = %error,
                    "a moving camera could not be sent its stop before its actor was retired"
                ),
            }
        }
        delivered
    }


    pub(super) async fn perform_ptz(&self, request: PtzCommandRequest) -> Result<serde_json::Value> {
        let config = self.config_snapshot()?;
        // The schema ceiling only -- the widest value `ptz.maximumContinuousMoveMs` accepts. The
        // CAMERA's own bound is enforced below, once the target is resolved; validating against a
        // constant is what let a camera bounded to ten seconds accept a sixty-second move.
        request.validate(60_000)?;
        let (instance, request_id, operation, physical, arguments) = match request {
            PtzCommandRequest::Continuous {
                instance,
                request_id,
                velocity,
                timeout_ms,
            } => (
                self.registry
                    .resolve_actuation_instance(instance.as_deref())?,
                Some(request_id),
                "continuous",
                Some(crate::model::PtzRequest::Continuous {
                    velocity,
                    timeout: Duration::from_millis(timeout_ms),
                }),
                serde_json::json!({ "velocity": velocity, "timeoutMs": timeout_ms }),
            ),
            PtzCommandRequest::Absolute {
                instance,
                request_id,
                position,
                speed,
            } => {
                let physical_speed = speed.map(|speed| crate::model::PtzVector {
                    pan: speed.pan,
                    tilt: speed.tilt,
                    zoom: speed.zoom,
                });
                (
                    self.registry
                        .resolve_actuation_instance(instance.as_deref())?,
                    Some(request_id),
                    "absolute",
                    Some(crate::model::PtzRequest::Absolute {
                        position,
                        speed: physical_speed,
                    }),
                    serde_json::json!({
                        "position": position,
                        "speed": speed.map(|speed| serde_json::json!({
                            "pan": speed.pan,
                            "tilt": speed.tilt,
                            "zoom": speed.zoom,
                        })),
                    }),
                )
            }
            PtzCommandRequest::Relative {
                instance,
                request_id,
                translation,
                speed,
            } => {
                let physical_speed = speed.map(|speed| crate::model::PtzVector {
                    pan: speed.pan,
                    tilt: speed.tilt,
                    zoom: speed.zoom,
                });
                (
                    self.registry
                        .resolve_actuation_instance(instance.as_deref())?,
                    Some(request_id),
                    "relative",
                    Some(crate::model::PtzRequest::Relative {
                        translation,
                        speed: physical_speed,
                    }),
                    serde_json::json!({
                        "translation": translation,
                        "speed": speed.map(|speed| serde_json::json!({
                            "pan": speed.pan,
                            "tilt": speed.tilt,
                            "zoom": speed.zoom,
                        })),
                    }),
                )
            }
            PtzCommandRequest::Stop {
                instance,
                request_id,
                axes,
            } => {
                let pan = axes.contains(&crate::commands::PtzAxis::Pan);
                let tilt = axes.contains(&crate::commands::PtzAxis::Tilt);
                let zoom = axes.contains(&crate::commands::PtzAxis::Zoom);
                (
                    self.registry
                        .resolve_actuation_instance(instance.as_deref())?,
                    Some(request_id),
                    "stop",
                    Some(crate::model::PtzRequest::Stop { pan, tilt, zoom }),
                    serde_json::json!({ "pan": pan, "tilt": tilt, "zoom": zoom }),
                )
            }
            PtzCommandRequest::Home {
                instance,
                request_id,
            } => (
                self.registry
                    .resolve_actuation_instance(instance.as_deref())?,
                Some(request_id),
                "home",
                Some(crate::model::PtzRequest::Home),
                serde_json::json!({}),
            ),
            PtzCommandRequest::Status { instance } => (
                self.registry
                    .resolve_actuation_instance(instance.as_deref())?,
                None,
                "status",
                Some(crate::model::PtzRequest::Status),
                serde_json::Value::Null,
            ),
        };
        let camera = self.registry.camera_config(&instance)?;
        if !camera.ptz.enabled {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::PtzDisabled,
                "PTZ is disabled by configuration",
            ));
        }
        // The safety bound belongs to the CAMERA. `validate` above only enforces the schema ceiling
        // (60 s, the widest value the field accepts), so on its own it would let an operator command
        // a minute of motion on a camera configured to move for ten seconds -- and the reply would
        // then advertise a stop deadline nothing was going to honour.
        let motion_timeout = match physical.as_ref() {
            Some(crate::model::PtzRequest::Continuous { timeout, .. }) => {
                let requested = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
                if requested > camera.ptz.maximum_continuous_move_ms {
                    return Err(crate::CameraError::rejected(
                        crate::ErrorCode::PtzRangeError,
                        "continuous timeoutMs exceeds this camera's ptz.maximumContinuousMoveMs",
                    ));
                }
                Some(*timeout)
            }
            _ => None,
        };
        let stopped_axes = match physical.as_ref() {
            Some(crate::model::PtzRequest::Continuous { velocity, .. }) => Some((
                velocity.pan != 0.0,
                velocity.tilt != 0.0,
                velocity.zoom != 0.0,
            )),
            _ => None,
        };
        let actor = self.actor(&instance)?;
        let physical = physical.ok_or_else(|| {
            crate::CameraError::rejected(
                crate::ErrorCode::UnsupportedCapability,
                "PTZ operation has no backend request",
            )
        })?;
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(config.global.timeouts.ptz_ms);
        if let Some(request_id) = request_id {
            let canonical = serde_json::json!({
                "instance": &instance,
                "requestId": &request_id,
                "operation": operation,
                "arguments": arguments,
            });
            let key = crate::catalog::LedgerKey::new(
                instance.clone(),
                format!("sb/ptz/{operation}"),
                request_id,
            )?;
            match self
                .catalog
                .begin_command(
                    key.clone(),
                    crate::idempotency::canonical_request_hash(&canonical, false)?,
                    canonical,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?
            {
                crate::catalog::BeginCommandOutcome::Conflict => {
                    return Err(crate::CameraError::rejected(
                        crate::ErrorCode::IdempotencyConflict,
                        "requestId was already used with different PTZ arguments",
                    ));
                }
                crate::catalog::BeginCommandOutcome::Existing(record) => {
                    match record.state {
                        crate::catalog::LedgerState::OutcomeUnknown => {
                            return Err(crate::CameraError::rejected(
                                crate::ErrorCode::PreviousOutcomeUnknown,
                                "the prior PTZ outcome is unknown after restart",
                            ));
                        }
                        _ => return Ok(record.reply.unwrap_or_else(
                            || serde_json::json!({ "operation": operation, "state": "COMMANDED" }),
                        )),
                    }
                }
                crate::catalog::BeginCommandOutcome::Started(_) => {}
            }
            // Any PTZ command that is not itself a continuous move ends the motion the last one
            // started, so its timer must not survive to stop a move it was never armed for.
            if motion_timeout.is_none() {
                self.disarm_motion_stop(&instance);
            }
            let result = actor.ptz(physical, deadline, &self.cancellation).await;
            let response = match result {
                Ok(crate::model::PtzResult::Commanded) => {
                    // DESIGN §15.5: the move is now armed to stop itself. The camera is told the
                    // timeout too, but a camera that ignores it -- and many do -- must not be the
                    // only thing between a commanded motion and a stop, and neither must the
                    // requester, who may never come back.
                    let stop_deadline = match (motion_timeout, stopped_axes) {
                        (Some(timeout), Some(axes)) => {
                            self.arm_motion_stop(&instance, timeout, axes)
                        }
                        _ => None,
                    };
                    serde_json::json!({
                        "operation": operation,
                        "state": "COMMANDED",
                        "acceptedAt": chrono::Utc::now(),
                        "stopDeadline": match stop_deadline {
                            Some(at) => serde_json::json!(at),
                            None => serde_json::Value::Null,
                        },
                    })
                }
                Ok(crate::model::PtzResult::PresetToken(token)) => {
                    serde_json::json!({ "operation": operation, "token": token })
                }
                Ok(crate::model::PtzResult::Removed) => {
                    serde_json::json!({ "operation": operation, "removed": true })
                }
                Ok(_) => {
                    return Err(crate::CameraError::rejected(
                        crate::ErrorCode::UnsupportedCapability,
                        "camera returned an unexpected PTZ response",
                    ));
                }
                Err(error) => {
                    let _ = self
                        .catalog
                        .complete_command(
                            key,
                            crate::catalog::LedgerState::Failed,
                            serde_json::json!({ "operation": operation }),
                            Some(error.code().as_str().to_string()),
                            Some(command_error(&error).message),
                            chrono::Utc::now().timestamp_millis(),
                        )
                        .await;
                    return Err(error);
                }
            };
            self.catalog
                .complete_command(
                    key,
                    crate::catalog::LedgerState::Succeeded,
                    response.clone(),
                    None,
                    None,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
            Ok(response)
        } else {
            match actor.ptz(physical, deadline, &self.cancellation).await? {
                crate::model::PtzResult::Status(status) => Ok(
                    serde_json::json!({ "position": status.position, "moving": status.moving, "available": true, "observedAt": status.observed_at }),
                ),
                _ => Err(crate::CameraError::rejected(
                    crate::ErrorCode::UnsupportedCapability,
                    "camera returned an unexpected PTZ status response",
                )),
            }
        }
    }


    pub(super) async fn perform_presets(&self, request: PtzPresetsRequest) -> Result<serde_json::Value> {
        let config = self.config_snapshot()?;
        request.validate()?;
        match request {
            PtzPresetsRequest::List {
                instance,
                limit,
                cursor,
            } => {
                let instance = self
                    .registry
                    .resolve_actuation_instance(instance.as_deref())?;
                let camera = self.registry.camera_config(&instance)?;
                if !camera.ptz.enabled {
                    return Err(crate::CameraError::rejected(
                        crate::ErrorCode::PtzDisabled,
                        "PTZ is disabled by configuration",
                    ));
                }
                let query = serde_json::json!({ "instance": instance });
                let initial = if cursor.is_none() {
                    let deadline = tokio::time::Instant::now()
                        + Duration::from_millis(config.global.timeouts.ptz_ms);
                    match self
                        .actor(&instance)?
                        .ptz(
                            crate::model::PtzRequest::ListPresets,
                            deadline,
                            &self.cancellation,
                        )
                        .await?
                    {
                        crate::model::PtzResult::Presets(presets) => Some(
                            presets
                                .into_iter()
                                .map(serde_json::to_value)
                                .collect::<std::result::Result<Vec<_>, _>>()?,
                        ),
                        _ => {
                            return Err(crate::CameraError::rejected(
                                crate::ErrorCode::UnsupportedCapability,
                                "camera returned an unexpected preset-list response",
                            ));
                        }
                    }
                } else {
                    None
                };
                let (presets, next_cursor, _) = self.cursors.snapshot_page(
                    "ptz-presets",
                    &query,
                    cursor.as_deref(),
                    initial,
                    None,
                    usize::from(limit),
                )?;
                Ok(serde_json::json!({
                    "presets": presets,
                    "nextCursor": next_cursor,
                }))
            }
            PtzPresetsRequest::Goto {
                instance,
                request_id,
                token,
            } => {
                self.perform_preset_mutation(
                    instance,
                    request_id,
                    "goto",
                    crate::model::PtzRequest::GotoPreset(token.clone()),
                    serde_json::json!({ "token": token }),
                    false,
                )
                .await
            }
            PtzPresetsRequest::Set {
                instance,
                request_id,
                name,
            } => {
                self.perform_preset_mutation(
                    instance,
                    request_id,
                    "set",
                    crate::model::PtzRequest::SetPreset(name.clone()),
                    serde_json::json!({ "name": name }),
                    true,
                )
                .await
            }
            PtzPresetsRequest::Remove {
                instance,
                request_id,
                token,
            } => {
                self.perform_preset_mutation(
                    instance,
                    request_id,
                    "remove",
                    crate::model::PtzRequest::RemovePreset(token.clone()),
                    serde_json::json!({ "token": token }),
                    true,
                )
                .await
            }
        }
    }


    async fn perform_preset_mutation(
        &self,
        requested_instance: Option<String>,
        request_id: String,
        operation: &'static str,
        physical: crate::model::PtzRequest,
        arguments: serde_json::Value,
        requires_mutation_permission: bool,
    ) -> Result<serde_json::Value> {
        let config = self.config_snapshot()?;
        let instance = self
            .registry
            .resolve_actuation_instance(requested_instance.as_deref())?;
        let camera = self.registry.camera_config(&instance)?;
        if !camera.ptz.enabled {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::PtzDisabled,
                "PTZ is disabled by configuration",
            ));
        }
        if requires_mutation_permission && !camera.ptz.allow_preset_mutation {
            return Err(crate::CameraError::rejected(
                crate::ErrorCode::UnsupportedCapability,
                "preset mutation is disabled by configuration",
            ));
        }
        let canonical = serde_json::json!({
            "instance": &instance,
            "requestId": &request_id,
            "operation": operation,
            "arguments": arguments,
        });
        let key = crate::catalog::LedgerKey::new(
            instance.clone(),
            format!("sb/ptz-presets/{operation}"),
            request_id,
        )?;
        match self
            .catalog
            .begin_command(
                key.clone(),
                crate::idempotency::canonical_request_hash(&canonical, false)?,
                canonical,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?
        {
            crate::catalog::BeginCommandOutcome::Conflict => {
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::IdempotencyConflict,
                    "requestId was already used with different preset arguments",
                ));
            }
            crate::catalog::BeginCommandOutcome::Existing(record) => {
                return match record.state {
                    crate::catalog::LedgerState::OutcomeUnknown => {
                        Err(crate::CameraError::rejected(
                            crate::ErrorCode::PreviousOutcomeUnknown,
                            "the prior preset outcome is unknown after restart",
                        ))
                    }
                    _ => Ok(record.reply.unwrap_or_else(
                        || serde_json::json!({ "operation": operation, "state": "COMMANDED" }),
                    )),
                };
            }
            crate::catalog::BeginCommandOutcome::Started(_) => {}
        }
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(config.global.timeouts.ptz_ms);
        let response = match self
            .actor(&instance)?
            .ptz(physical, deadline, &self.cancellation)
            .await
        {
            Ok(crate::model::PtzResult::Commanded) => {
                serde_json::json!({ "operation": operation, "state": "COMMANDED" })
            }
            Ok(crate::model::PtzResult::PresetToken(token)) => {
                serde_json::json!({ "operation": operation, "token": token })
            }
            Ok(crate::model::PtzResult::Removed) => {
                serde_json::json!({ "operation": operation, "removed": true })
            }
            Ok(_) => {
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::UnsupportedCapability,
                    "camera returned an unexpected preset response",
                ));
            }
            Err(error) => {
                let _ = self
                    .catalog
                    .complete_command(
                        key,
                        crate::catalog::LedgerState::Failed,
                        serde_json::json!({ "operation": operation }),
                        Some(error.code().as_str().to_string()),
                        Some(command_error(&error).message),
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await;
                return Err(error);
            }
        };
        self.catalog
            .complete_command(
                key,
                crate::catalog::LedgerState::Succeeded,
                response.clone(),
                None,
                None,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?;
        Ok(response)
    }


    pub(super) fn group_status_page(
        &self,
        group: crate::catalog::GroupRecord,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<serde_json::Value> {
        let query = serde_json::json!({ "captureGroupId": group.group_id });
        let initial = if cursor.is_none() {
            Some(
                group
                    .members
                    .iter()
                    .map(job_status_json)
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        let (members, next_cursor, _) = self.cursors.snapshot_page(
            "capture-status-group",
            &query,
            cursor,
            initial,
            None,
            limit,
        )?;
        Ok(serde_json::json!({
            "group": {
                "captureGroupId": group.group_id,
                "requestId": group.request_id,
                "state": group.state,
                "acceptedAtMs": group.accepted_at_ms,
                "terminalAtMs": group.terminal_at_ms,
                "errorCode": group.error_code,
                "errorMessage": group.error_message,
                "result": group.terminal_result,
            },
            "members": members,
            "nextCursor": next_cursor,
        }))
    }


    pub(super) async fn jobs_status_page(&self, body: &CaptureStatusRequest) -> Result<serde_json::Value> {
        let query = serde_json::json!({
            "instance": body.instance,
            "states": body.states,
        });
        let before = self.cursors.job_before(&query, body.cursor.as_deref())?;
        let requested = usize::from(body.limit);
        // Read one additional durable row to decide whether a stable continuation exists.  The
        // catalog's descending (acceptedAt,captureId) tuple keeps rows inserted after page one
        // out of every continuation without retaining an unbounded process-local job snapshot.
        let mut jobs = self
            .catalog
            .jobs_page(
                body.instance.clone(),
                body.states.clone(),
                before,
                requested.saturating_add(1),
            )
            .await?;
        let has_next = jobs.len() > requested;
        if has_next {
            jobs.truncate(requested);
        }
        let next_cursor = if has_next {
            let last = jobs.last().ok_or_else(|| {
                crate::CameraError::Catalog(
                    "paged capture-status query reported a continuation without a row".to_string(),
                )
            })?;
            Some(
                self.cursors
                    .next_job_cursor(&query, (last.accepted_at_ms, last.capture_id.clone()))?,
            )
        } else {
            None
        };
        Ok(serde_json::json!({
            "jobs": jobs.iter().map(job_status_json).collect::<Vec<_>>(),
            "nextCursor": next_cursor,
        }))
    }


    pub(super) async fn handle_deferred_capture(
        &self,
        request: Message,
        deferred: DeferredReplyRegistry,
    ) -> CommandOutcome {
        let config = match self.config_snapshot() {
            Ok(config) => config,
            Err(error) => return CommandOutcome::ImmediateError(command_error(&error)),
        };
        let body: Result<CaptureRequest> = commands::parse_closed(request.body.clone());
        let body = match body.and_then(|body| {
            body.validate(config.global.limits.max_metadata_bytes)?;
            Ok(body)
        }) {
            Ok(body) => body,
            Err(error) => return CommandOutcome::ImmediateError(command_error(&error)),
        };
        let token = match deferred.defer(
            &request,
            Duration::from_millis(config.global.timeouts.max_deferred_reply_lifetime_ms),
        ) {
            Ok(token) => token,
            Err(error) => {
                return CommandOutcome::ImmediateError(CommandError::new(
                    crate::ErrorCode::ReplyRequired.as_str(),
                    error.message,
                ));
            }
        };
        if token.activate().is_err() {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::BackendError.as_str(),
                "deferred reply could not be activated",
            ));
        }
        let Some(runtime) = self.self_reference.get().and_then(Weak::upgrade) else {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::ComponentStopping.as_str(),
                "camera runtime is not available",
            ));
        };
        let correlation_id = request.header.correlation_id.clone();
        let request_uuid = request.header.uuid.clone();
        let continuation_token = token.clone();
        CommandOutcome::deferred_with_continuation(token, async move {
            runtime
                .accept_deferred_capture(body, correlation_id, request_uuid, continuation_token)
                .await
                .map_err(|error| command_error(&error))
        })
    }


    async fn accept_deferred_capture(
        &self,
        body: CaptureRequest,
        correlation_id: String,
        request_uuid: String,
        token: DeferredReplyToken,
    ) -> Result<()> {
        let config = self.config_snapshot()?;
        let instance = self
            .registry
            .resolve_actuation_instance(body.instance.as_deref())?;
        let request_id = body.request_id.clone();
        let waiter_id = format!("wait_{}", uuid::Uuid::now_v7());
        self.waiters.prepare(
            instance.clone(),
            request_id.clone(),
            waiter_id.clone(),
            Arc::new(token.clone()) as Arc<dyn CaptureWaiter>,
            correlation_id.clone(),
            request_uuid.clone(),
        )?;
        let accepted = self
            .submit_capture(
                instance.clone(),
                request_id.clone(),
                body.capture_profile,
                body.timeout_ms,
                body.metadata,
                correlation_id.clone(),
                CommandVerb::Capture.as_str(),
                crate::admission::CapturePriority::Direct,
            )
            .await?;
        let record = match accepted {
            crate::catalog::AcceptJobOutcome::Inserted(_) => return Ok(()),
            crate::catalog::AcceptJobOutcome::Existing(record) => record,
            crate::catalog::AcceptJobOutcome::Conflict => {
                let _ = self.waiters.take_pending(&instance, &request_id);
                return Err(crate::CameraError::rejected(
                    crate::ErrorCode::IdempotencyConflict,
                    "requestId was already used with different immutable capture arguments",
                ));
            }
        };
        let _ = self.waiters.take_pending(
            &record.instance,
            record.request_id.as_deref().unwrap_or_default(),
        );
        if let Some(terminal) = record.terminal_result {
            token.settle_success(Some(terminal)).await.map_err(|_| {
                crate::CameraError::rejected(
                    crate::ErrorCode::BackendError,
                    "deferred reply could not be settled",
                )
            })?;
            return Ok(());
        }
        let now = chrono::Utc::now().timestamp_millis();
        self.catalog
            .add_waiter(crate::catalog::WaiterRecord {
                waiter_id: waiter_id.clone(),
                capture_id: record.capture_id.clone(),
                correlation_id,
                request_uuid: Some(request_uuid),
                expires_at_ms: now.saturating_add(
                    i64::try_from(config.global.timeouts.max_deferred_reply_lifetime_ms)
                        .unwrap_or(i64::MAX),
                ),
                created_at_ms: now,
            })
            .await?;
        self.waiters.register(
            record.capture_id.clone(),
            waiter_id,
            Arc::new(token.clone()) as Arc<dyn CaptureWaiter>,
        )?;
        if let Some(terminal) = self
            .catalog
            .job(record.capture_id)
            .await?
            .and_then(|job| job.terminal_result)
        {
            token.settle_success(Some(terminal)).await.map_err(|_| {
                crate::CameraError::rejected(
                    crate::ErrorCode::BackendError,
                    "deferred reply could not be settled",
                )
            })?;
        }
        Ok(())
    }


    pub(super) async fn handle_deferred_group_capture(
        &self,
        request: Message,
        deferred: DeferredReplyRegistry,
    ) -> CommandOutcome {
        let config = match self.config_snapshot() {
            Ok(config) => config,
            Err(error) => return CommandOutcome::ImmediateError(command_error(&error)),
        };
        let body: Result<GroupCaptureRequest> = commands::parse_closed(request.body.clone());
        let body = match body.and_then(|body| {
            body.validate(
                config.global.limits.max_cameras_per_group,
                config.global.limits.max_metadata_bytes,
            )?;
            Ok(body)
        }) {
            Ok(body) => body,
            Err(error) => return CommandOutcome::ImmediateError(command_error(&error)),
        };
        let token = match deferred.defer(
            &request,
            Duration::from_millis(config.global.timeouts.max_deferred_reply_lifetime_ms),
        ) {
            Ok(token) => token,
            Err(error) => {
                return CommandOutcome::ImmediateError(CommandError::new(
                    crate::ErrorCode::ReplyRequired.as_str(),
                    error.message,
                ));
            }
        };
        if token.activate().is_err() {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::BackendError.as_str(),
                "deferred reply could not be activated",
            ));
        }
        let Some(runtime) = self.self_reference.get().and_then(Weak::upgrade) else {
            return CommandOutcome::ImmediateError(CommandError::new(
                crate::ErrorCode::ComponentStopping.as_str(),
                "camera runtime is not available",
            ));
        };
        let correlation_id = request.header.correlation_id.clone();
        let continuation_token = token.clone();
        CommandOutcome::deferred_with_continuation(token, async move {
            runtime
                .submit_group(
                    body,
                    correlation_id,
                    crate::admission::CapturePriority::Direct,
                    Some(continuation_token),
                )
                .await
                .map(|_| ())
                .map_err(|error| command_error(&error))
        })
    }
}
