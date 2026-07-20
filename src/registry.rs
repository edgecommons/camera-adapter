//! Camera roster, immutable capability snapshots, and generation-safe lifecycle observations.
//!
//! Registry entries never own frames or native sessions. Supervisors update compact snapshots;
//! command/status code reads them without reaching into backend actors.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::{
    CameraError, ErrorCode, Result,
    config::{AdapterConfig, CameraConfig},
    model::{BackendKind, CameraCapabilities},
};

/// Public per-camera connection lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CameraConnectionState {
    /// Configuration explicitly disables the camera.
    Disabled,
    /// No live protocol session exists.
    Offline,
    /// A supervisor is attempting initial connection.
    Connecting,
    /// A live session passed capability probing.
    Online,
    /// A live session exists but a bounded health condition is impaired.
    Degraded,
    /// A failed connection is waiting for capped exponential backoff.
    Backoff,
    /// Reload or process shutdown is draining/stopping this camera.
    Stopping,
}

/// Sanitized lifecycle error retained for status and health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CameraStatusError {
    /// Stable public category.
    pub code: String,
    /// Operator-safe message without endpoints or credentials.
    pub message: String,
    /// Observation time.
    pub observed_at: DateTime<Utc>,
}

/// Immutable status/capability view for one camera generation.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CameraSnapshot {
    /// Camera instance token.
    pub instance: String,
    /// Configured enabled flag.
    pub enabled: bool,
    /// Backend kind (never inferred from a live response).
    pub backend: BackendKind,
    /// Supervisor session generation; stale callbacks cannot overwrite a newer generation.
    pub generation: u64,
    /// Current connection lifecycle.
    pub state: CameraConnectionState,
    /// Immutable capabilities for this generation.
    pub capabilities: Option<Arc<CameraCapabilities>>,
    /// SHA-256 of the serialized capability snapshot.
    pub capabilities_digest: Option<String>,
    /// Last successful session/connect transition.
    pub connected_at: Option<DateTime<Utc>>,
    /// Latest safely summarized failure.
    pub last_error: Option<CameraStatusError>,
    /// Snapshot observation time.
    pub updated_at: DateTime<Utc>,
}

struct RegistryEntry {
    config: Arc<CameraConfig>,
    sender: watch::Sender<CameraSnapshot>,
}

/// Diff returned after a complete validated configuration swap.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegistryDiff {
    /// Newly configured camera IDs.
    pub added: Vec<String>,
    /// Removed camera IDs.
    pub removed: Vec<String>,
    /// Existing IDs whose enabled flag or semantic backend configuration changed.
    pub lifecycle_changed: Vec<String>,
    /// Existing IDs whose remaining configuration was replaced.
    pub updated: Vec<String>,
}

/// Thread-safe camera roster.
pub struct CameraRegistry {
    entries: RwLock<BTreeMap<String, RegistryEntry>>,
}

impl CameraRegistry {
    /// Creates compact supervisor entries for every configured camera, including disabled ones.
    pub fn new(config: &AdapterConfig) -> Result<Self> {
        let mut entries = BTreeMap::new();
        for camera in &config.instances {
            let snapshot = initial_snapshot(camera);
            let (sender, _) = watch::channel(snapshot);
            if entries
                .insert(
                    camera.id.clone(),
                    RegistryEntry {
                        config: Arc::new(camera.clone()),
                        sender,
                    },
                )
                .is_some()
            {
                return Err(CameraError::Config {
                    path: "component.instances".to_string(),
                    message: "camera IDs must be unique".to_string(),
                });
            }
        }
        Ok(Self {
            entries: RwLock::new(entries),
        })
    }

    /// Sorted configured camera IDs.
    pub fn ids(&self) -> Result<Vec<String>> {
        Ok(self.read_entries()?.keys().cloned().collect::<Vec<_>>())
    }

    /// Bounded sorted snapshot list.
    pub fn snapshots(&self, maximum: usize) -> Result<Vec<CameraSnapshot>> {
        if maximum == 0 {
            return Ok(Vec::new());
        }
        Ok(self
            .read_entries()?
            .values()
            .take(maximum)
            .map(|entry| entry.sender.borrow().clone())
            .collect())
    }

    /// Exact status lookup. Disabled cameras remain visible.
    pub fn snapshot(&self, instance: &str) -> Result<CameraSnapshot> {
        self.read_entries()?
            .get(instance)
            .map(|entry| entry.sender.borrow().clone())
            .ok_or_else(|| {
                CameraError::rejected(
                    ErrorCode::NoSuchInstance,
                    format!("camera instance '{instance}' is not configured"),
                )
            })
    }

    /// Immutable current camera configuration.
    pub fn camera_config(&self, instance: &str) -> Result<Arc<CameraConfig>> {
        self.read_entries()?
            .get(instance)
            .map(|entry| entry.config.clone())
            .ok_or_else(|| {
                CameraError::rejected(
                    ErrorCode::NoSuchInstance,
                    format!("camera instance '{instance}' is not configured"),
                )
            })
    }

    /// Resolves the single-camera omission rule to a configured instance, WITHOUT enforcing enabled
    /// state.
    ///
    /// The lifecycle verbs (`sb/pause` / `sb/resume`) address a camera that need not be enabled --
    /// pausing a camera an operator is about to disable, or resuming one after re-enabling it, are
    /// both legitimate. So this is the routing-only sibling of [`Self::resolve_actuation_instance`]:
    /// same D-EIP-13 omission rule and the same `BAD_ARGS` / `NO_SUCH_INSTANCE` codes, but no
    /// `CAMERA_DISABLED` gate.
    pub fn resolve_instance(&self, requested: Option<&str>) -> Result<String> {
        let entries = self.read_entries()?;
        let instance = match requested {
            Some(instance) => instance.to_string(),
            None if entries.len() == 1 => entries.keys().next().cloned().ok_or_else(|| {
                CameraError::Catalog("registry changed during lookup".to_string())
            })?,
            None => {
                return Err(CameraError::rejected(
                    ErrorCode::BadArgs,
                    "instance is required when more than one camera is configured",
                ));
            }
        };
        if !entries.contains_key(&instance) {
            return Err(CameraError::rejected(
                ErrorCode::NoSuchInstance,
                format!("camera instance '{instance}' is not configured"),
            ));
        }
        Ok(instance)
    }

    /// Resolves the single-camera omission rule and enforces enabled state for actuation.
    pub fn resolve_actuation_instance(&self, requested: Option<&str>) -> Result<String> {
        let entries = self.read_entries()?;
        let instance = match requested {
            Some(instance) => instance.to_string(),
            None if entries.len() == 1 => entries.keys().next().cloned().ok_or_else(|| {
                CameraError::Catalog("registry changed during lookup".to_string())
            })?,
            None => {
                return Err(CameraError::rejected(
                    ErrorCode::BadArgs,
                    "instance is required when more than one camera is configured",
                ));
            }
        };
        let entry = entries.get(&instance).ok_or_else(|| {
            CameraError::rejected(
                ErrorCode::NoSuchInstance,
                format!("camera instance '{instance}' is not configured"),
            )
        })?;
        if !entry.config.enabled {
            return Err(CameraError::rejected(
                ErrorCode::CameraDisabled,
                format!("camera instance '{instance}' is disabled"),
            ));
        }
        Ok(instance)
    }

    /// Subscribes to compact lifecycle updates for one camera.
    pub fn subscribe(&self, instance: &str) -> Result<watch::Receiver<CameraSnapshot>> {
        self.read_entries()?
            .get(instance)
            .map(|entry| entry.sender.subscribe())
            .ok_or_else(|| {
                CameraError::rejected(
                    ErrorCode::NoSuchInstance,
                    format!("camera instance '{instance}' is not configured"),
                )
            })
    }

    /// Publishes a generation-safe supervisor observation.
    ///
    /// Returns `false` when the camera no longer exists or a newer generation already won.
    pub fn update(
        &self,
        instance: &str,
        generation: u64,
        state: CameraConnectionState,
        capabilities: Option<CameraCapabilities>,
        last_error: Option<CameraStatusError>,
        observed_at: DateTime<Utc>,
    ) -> Result<bool> {
        let entries = self.read_entries()?;
        let Some(entry) = entries.get(instance) else {
            return Ok(false);
        };
        let prior = entry.sender.borrow().clone();
        if generation < prior.generation {
            return Ok(false);
        }
        if !entry.config.enabled && state != CameraConnectionState::Disabled {
            return Ok(false);
        }
        let capabilities = capabilities.map(Arc::new);
        let digest = capabilities.as_deref().map(capability_digest).transpose()?;
        let connected_at = if state == CameraConnectionState::Online {
            if prior.state == CameraConnectionState::Online && prior.generation == generation {
                prior.connected_at
            } else {
                Some(observed_at)
            }
        } else {
            prior.connected_at
        };
        entry.sender.send_replace(CameraSnapshot {
            instance: instance.to_string(),
            enabled: entry.config.enabled,
            backend: entry.config.backend.kind(),
            generation,
            state,
            capabilities,
            capabilities_digest: digest,
            connected_at,
            last_error,
            updated_at: observed_at,
        });
        Ok(true)
    }

    /// Atomically replaces the complete validated roster and returns lifecycle-relevant changes.
    /// Existing watch channels are retained for IDs that survive the reload.
    pub fn apply_validated_config(&self, config: &AdapterConfig) -> Result<RegistryDiff> {
        let mut entries = self.write_entries()?;
        let incoming: BTreeMap<&str, &CameraConfig> = config
            .instances
            .iter()
            .map(|camera| (camera.id.as_str(), camera))
            .collect();
        if incoming.len() != config.instances.len() {
            return Err(CameraError::Config {
                path: "component.instances".to_string(),
                message: "camera IDs must be unique".to_string(),
            });
        }
        let mut diff = RegistryDiff::default();
        for existing in entries.keys() {
            if !incoming.contains_key(existing.as_str()) {
                diff.removed.push(existing.clone());
            }
        }
        for camera in &config.instances {
            match entries.get_mut(&camera.id) {
                None => {
                    let (sender, _) = watch::channel(initial_snapshot(camera));
                    entries.insert(
                        camera.id.clone(),
                        RegistryEntry {
                            config: Arc::new(camera.clone()),
                            sender,
                        },
                    );
                    diff.added.push(camera.id.clone());
                }
                Some(entry) => {
                    // A session must not continue under stale connection, credential-reference,
                    // selector, or transport settings.  Equality is deliberately semantic rather
                    // than a debug-string comparison so this decision never depends on formatting
                    // and never logs potentially sensitive configuration.
                    let lifecycle_changed = entry.config.enabled != camera.enabled
                        || entry.config.backend != camera.backend;
                    entry.config = Arc::new(camera.clone());
                    if lifecycle_changed {
                        diff.lifecycle_changed.push(camera.id.clone());
                        let prior = entry.sender.borrow().clone();
                        entry.sender.send_replace(CameraSnapshot {
                            instance: camera.id.clone(),
                            enabled: camera.enabled,
                            backend: camera.backend.kind(),
                            generation: prior.generation.saturating_add(1),
                            state: if camera.enabled {
                                CameraConnectionState::Stopping
                            } else {
                                CameraConnectionState::Disabled
                            },
                            capabilities: None,
                            capabilities_digest: None,
                            connected_at: prior.connected_at,
                            last_error: None,
                            updated_at: Utc::now(),
                        });
                    } else {
                        diff.updated.push(camera.id.clone());
                    }
                }
            }
        }
        for removed in &diff.removed {
            entries.remove(removed);
        }
        Ok(diff)
    }

    fn read_entries(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, BTreeMap<String, RegistryEntry>>> {
        self.entries
            .read()
            .map_err(|_| CameraError::Catalog("camera registry read lock poisoned".to_string()))
    }

    fn write_entries(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, BTreeMap<String, RegistryEntry>>> {
        self.entries
            .write()
            .map_err(|_| CameraError::Catalog("camera registry write lock poisoned".to_string()))
    }
}

fn initial_snapshot(camera: &CameraConfig) -> CameraSnapshot {
    CameraSnapshot {
        instance: camera.id.clone(),
        enabled: camera.enabled,
        backend: camera.backend.kind(),
        generation: 0,
        state: if camera.enabled {
            CameraConnectionState::Offline
        } else {
            CameraConnectionState::Disabled
        },
        capabilities: None,
        capabilities_digest: None,
        connected_at: None,
        last_error: None,
        updated_at: Utc::now(),
    }
}

fn capability_digest(capabilities: &CameraCapabilities) -> Result<String> {
    let encoded = serde_json::to_vec(capabilities)?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::config::AdapterConfig;

    fn config(two: bool, second_enabled: bool) -> AdapterConfig {
        let mut instances = vec![json!({
            "id": "camera-a",
            "backend": {"type": "sim"},
            "defaultCaptureProfile": "main",
            "captureProfiles": {"main": {"output": {"encoding": "jpeg"}}}
        })];
        if two {
            instances.push(json!({
                "id": "camera-b",
                "enabled": second_enabled,
                "backend": {"type": "sim"},
                "defaultCaptureProfile": "main",
                "captureProfiles": {"main": {"output": {"encoding": "jpeg"}}}
            }));
        }
        let value = json!({
            "component": {
                "global": {"output": {"rootDirectory": "C:/captures"}},
                "instances": instances
            }
        });
        let core =
            edgecommons::config::Config::from_value(crate::COMPONENT_NAME, "gw-01", value).unwrap();
        AdapterConfig::from_core_reload(&core).unwrap()
    }

    fn capabilities(model: &str) -> CameraCapabilities {
        CameraCapabilities {
            capture_modes: vec![crate::model::CaptureMode::Simulated],
            pixel_formats: vec![crate::model::PixelFormat::Rgb8],
            software_trigger: false,
            snapshot_uri: false,
            rtsp: false,
            ptz: false,
            ptz_status: false,
            presets: false,
            preset_mutation: false,
            vendor: Some("EdgeCommons".to_string()),
            model: Some(model.to_string()),
            firmware: None,
            serial: Some("sim-a".to_string()),
            warnings: Vec::new(),
        }
    }

    #[test]
    fn single_camera_omission_and_disabled_actuation_are_explicit() {
        let one = CameraRegistry::new(&config(false, true)).unwrap();
        assert_eq!(one.resolve_actuation_instance(None).unwrap(), "camera-a");
        let two = CameraRegistry::new(&config(true, false)).unwrap();
        assert_eq!(
            two.resolve_actuation_instance(None).unwrap_err().code(),
            ErrorCode::BadArgs
        );
        assert_eq!(
            two.resolve_actuation_instance(Some("camera-b"))
                .unwrap_err()
                .code(),
            ErrorCode::CameraDisabled
        );
        assert_eq!(
            two.snapshot("camera-b").unwrap().state,
            CameraConnectionState::Disabled
        );
    }

    /// The routing-only resolver applies the same omission rule and codes as actuation, but does NOT
    /// gate on enabled state -- a disabled camera can still be paused or resumed.
    #[test]
    fn resolve_instance_routes_without_the_enabled_gate() {
        let one = CameraRegistry::new(&config(false, true)).unwrap();
        assert_eq!(one.resolve_instance(None).unwrap(), "camera-a");

        let two = CameraRegistry::new(&config(true, false)).unwrap();
        assert_eq!(
            two.resolve_instance(None).unwrap_err().code(),
            ErrorCode::BadArgs,
            "a missing instance with two cameras is BAD_ARGS"
        );
        assert_eq!(
            two.resolve_instance(Some("missing")).unwrap_err().code(),
            ErrorCode::NoSuchInstance
        );
        assert_eq!(
            two.resolve_instance(Some("camera-b")).unwrap(),
            "camera-b",
            "a DISABLED camera still resolves -- pause/resume do not require it to be enabled"
        );
    }

    #[test]
    fn stale_generation_cannot_replace_new_capabilities() {
        let registry = CameraRegistry::new(&config(false, true)).unwrap();
        let now = Utc::now();
        assert!(
            registry
                .update(
                    "camera-a",
                    2,
                    CameraConnectionState::Online,
                    Some(capabilities("new")),
                    None,
                    now,
                )
                .unwrap()
        );
        assert!(
            !registry
                .update(
                    "camera-a",
                    1,
                    CameraConnectionState::Offline,
                    Some(capabilities("stale")),
                    None,
                    now,
                )
                .unwrap()
        );
        let snapshot = registry.snapshot("camera-a").unwrap();
        assert_eq!(snapshot.generation, 2);
        assert_eq!(
            snapshot.capabilities.as_ref().unwrap().model.as_deref(),
            Some("new")
        );
    }

    #[test]
    fn capability_digest_changes_only_with_snapshot_content() {
        let registry = CameraRegistry::new(&config(false, true)).unwrap();
        let now = Utc::now();
        registry
            .update(
                "camera-a",
                1,
                CameraConnectionState::Online,
                Some(capabilities("first")),
                None,
                now,
            )
            .unwrap();
        let first = registry
            .snapshot("camera-a")
            .unwrap()
            .capabilities_digest
            .unwrap();
        registry
            .update(
                "camera-a",
                1,
                CameraConnectionState::Online,
                Some(capabilities("second")),
                None,
                now,
            )
            .unwrap();
        let second = registry
            .snapshot("camera-a")
            .unwrap()
            .capabilities_digest
            .unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn validated_reload_reports_lifecycle_diff_and_removes_old_watchers() {
        let registry = CameraRegistry::new(&config(false, true)).unwrap();
        let receiver = registry.subscribe("camera-a").unwrap();
        let replacement = config(true, false);
        let diff = registry.apply_validated_config(&replacement).unwrap();
        assert_eq!(diff.added, ["camera-b"]);
        assert!(diff.updated.contains(&"camera-a".to_string()));
        assert!(!receiver.has_changed().unwrap());

        let original = CameraRegistry::new(&replacement).unwrap();
        let diff = original
            .apply_validated_config(&config(false, true))
            .unwrap();
        assert_eq!(diff.removed, ["camera-b"]);
        assert!(original.snapshot("camera-b").is_err());
    }

    #[test]
    fn reload_replaces_session_when_same_kind_backend_settings_change() {
        let initial = config(false, true);
        let registry = CameraRegistry::new(&initial).unwrap();
        let mut replacement = config(false, true);
        let crate::config::BackendConfig::Sim(sim) = &mut replacement.instances[0].backend else {
            panic!("test fixture must use the simulator backend");
        };
        sim.seed = Some(42);

        let diff = registry.apply_validated_config(&replacement).unwrap();
        assert_eq!(diff.lifecycle_changed, ["camera-a"]);
        assert!(diff.updated.is_empty());
        let snapshot = registry.snapshot("camera-a").unwrap();
        assert_eq!(snapshot.state, CameraConnectionState::Stopping);
        assert_eq!(snapshot.generation, 1);
    }

    #[test]
    fn unknown_lookups_and_disabled_observations_fail_closed() {
        let registry = CameraRegistry::new(&config(true, false)).unwrap();
        assert_eq!(
            registry
                .resolve_actuation_instance(Some("missing"))
                .unwrap_err()
                .code(),
            ErrorCode::NoSuchInstance
        );
        assert_eq!(
            registry.camera_config("missing").unwrap_err().code(),
            ErrorCode::NoSuchInstance
        );
        assert_eq!(
            registry.subscribe("missing").unwrap_err().code(),
            ErrorCode::NoSuchInstance
        );
        assert!(
            !registry
                .update(
                    "camera-b",
                    1,
                    CameraConnectionState::Online,
                    Some(capabilities("must-not-publish")),
                    None,
                    Utc::now(),
                )
                .unwrap()
        );
        let disabled = registry.snapshot("camera-b").unwrap();
        assert_eq!(disabled.state, CameraConnectionState::Disabled);
        assert!(disabled.capabilities.is_none());
    }

    #[test]
    fn lifecycle_observations_preserve_online_time_and_publish_sanitized_error() {
        let registry = CameraRegistry::new(&config(false, true)).unwrap();
        let connected_at = Utc::now();
        assert!(
            registry
                .update(
                    "camera-a",
                    1,
                    CameraConnectionState::Online,
                    Some(capabilities("online")),
                    None,
                    connected_at,
                )
                .unwrap()
        );
        let error_at = connected_at + chrono::Duration::seconds(1);
        assert!(
            registry
                .update(
                    "camera-a",
                    1,
                    CameraConnectionState::Backoff,
                    None,
                    Some(CameraStatusError {
                        code: "BACKEND_ERROR".to_string(),
                        message: "camera unavailable".to_string(),
                        observed_at: error_at,
                    }),
                    error_at,
                )
                .unwrap()
        );
        let snapshot = registry.snapshot("camera-a").unwrap();
        assert_eq!(snapshot.connected_at, Some(connected_at));
        assert_eq!(snapshot.state, CameraConnectionState::Backoff);
        assert_eq!(snapshot.last_error.unwrap().code, "BACKEND_ERROR");
        assert!(snapshot.capabilities.is_none());
    }
}
