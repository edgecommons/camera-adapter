//! Bounded output/state filesystem-pressure assessment for runtime admission and alarms.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::admission::{DiskSpace, DiskSpaceProbe};
use crate::config::OutputConfig;

/// One configured filesystem root and its current pressure assessment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootPressure {
    /// The configured canonical root. It is safe operator configuration, never request data.
    pub root: PathBuf,
    /// Free bytes visible to the service account when the root could be sampled.
    pub free_bytes: Option<u64>,
    /// Whole free percent of the filesystem when the root could be sampled.
    pub free_percent: Option<u8>,
    /// Whether the root violates either configured free-space floor.
    pub pressured: bool,
    /// Whether the probe could read the filesystem capacity.
    pub readable: bool,
}

impl RootPressure {
    /// Returns true when admission cannot safely rely on this root.
    #[must_use]
    pub const fn unavailable(&self) -> bool {
        !self.readable || self.pressured
    }
}

/// Combined output and state-root assessment. Output pressure rejects new capture work but does
/// not by itself invalidate durable catalog readiness; state pressure does both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoragePressureSnapshot {
    /// Image output filesystem assessment.
    pub output: RootPressure,
    /// Durable catalog filesystem assessment.
    pub state: RootPressure,
}

impl StoragePressureSnapshot {
    /// Returns true when either root must reject new capture work.
    #[must_use]
    pub const fn rejects_new_captures(&self) -> bool {
        self.output.unavailable() || self.state.unavailable()
    }

    /// Returns true only when durable catalog state cannot safely accept new work.
    #[must_use]
    pub const fn state_available(&self) -> bool {
        !self.state.unavailable()
    }

    /// Selects the single root that should represent the deduplicated component alarm.
    #[must_use]
    pub fn alarm_root(&self) -> Option<&RootPressure> {
        match (self.output.unavailable(), self.state.unavailable()) {
            (false, false) => None,
            (true, false) => Some(&self.output),
            (false, true) => Some(&self.state),
            (true, true) => {
                if pressure_rank(&self.state) <= pressure_rank(&self.output) {
                    Some(&self.state)
                } else {
                    Some(&self.output)
                }
            }
        }
    }
}

/// Runtime sampler using the existing bounded admission filesystem probe.
#[derive(Clone)]
pub struct StoragePressureMonitor {
    output_root: PathBuf,
    state_root: PathBuf,
    minimum_free_bytes: u64,
    minimum_free_percent: u8,
    probe: Arc<dyn DiskSpaceProbe>,
}

impl StoragePressureMonitor {
    /// Builds a monitor from already validated configured roots and output floors.
    #[must_use]
    pub fn new(
        output_root: impl Into<PathBuf>,
        state_root: impl Into<PathBuf>,
        output: &OutputConfig,
        probe: Arc<dyn DiskSpaceProbe>,
    ) -> Self {
        Self {
            output_root: output_root.into(),
            state_root: state_root.into(),
            minimum_free_bytes: output.minimum_free_bytes,
            minimum_free_percent: output.minimum_free_percent,
            probe,
        }
    }

    /// Samples both configured roots. A probe failure is represented without exposing its error.
    pub async fn assess(&self) -> StoragePressureSnapshot {
        let output = self.assess_root(&self.output_root).await;
        let state = if self.output_root == self.state_root {
            RootPressure {
                root: self.state_root.clone(),
                ..output.clone()
            }
        } else {
            self.assess_root(&self.state_root).await
        };
        StoragePressureSnapshot { output, state }
    }

    async fn assess_root(&self, root: &Path) -> RootPressure {
        match self.probe.space(root).await {
            Ok(space) => assess_space(
                root,
                space,
                self.minimum_free_bytes,
                self.minimum_free_percent,
            ),
            Err(_) => RootPressure {
                root: root.to_owned(),
                free_bytes: None,
                free_percent: None,
                pressured: false,
                readable: false,
            },
        }
    }
}

fn assess_space(
    root: &Path,
    space: DiskSpace,
    minimum_free_bytes: u64,
    minimum_free_percent: u8,
) -> RootPressure {
    let free_percent = if space.total_bytes == 0 {
        0
    } else {
        u8::try_from((u128::from(space.available_bytes) * 100) / u128::from(space.total_bytes))
            .unwrap_or(100)
    };
    RootPressure {
        root: root.to_owned(),
        free_bytes: Some(space.available_bytes),
        free_percent: Some(free_percent),
        pressured: space.available_bytes < minimum_free_bytes
            || free_percent < minimum_free_percent,
        readable: true,
    }
}

fn pressure_rank(root: &RootPressure) -> (u8, u8, u64) {
    match (root.readable, root.free_percent, root.free_bytes) {
        (false, _, _) => (0, 0, 0),
        (true, Some(percent), Some(bytes)) => (1, percent, bytes),
        (true, _, _) => (0, 0, 0),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;

    use super::*;
    use crate::{CameraError, Result};

    #[derive(Default)]
    struct FixedProbe {
        values: BTreeMap<PathBuf, Result<DiskSpace>>,
    }

    #[async_trait]
    impl DiskSpaceProbe for FixedProbe {
        async fn space(&self, path: &Path) -> Result<DiskSpace> {
            match self.values.get(path) {
                Some(Ok(value)) => Ok(*value),
                Some(Err(error)) => Err(CameraError::Storage(error.to_string())),
                None => Err(CameraError::Storage("unexpected probe root".to_string())),
            }
        }
    }

    fn output() -> OutputConfig {
        OutputConfig {
            root_directory: "/output".to_string(),
            camera_directory_template: "{cameraId}".to_string(),
            file_name_template: "{captureId}.{extension}".to_string(),
            write_metadata_sidecar: false,
            minimum_free_bytes: 200,
            minimum_free_percent: 10,
            directory_mode: "0750".to_string(),
            file_mode: "0640".to_string(),
        }
    }

    #[tokio::test]
    async fn pressure_selects_the_worst_configured_root_and_preserves_unknown_space() {
        let mut values = BTreeMap::new();
        values.insert(
            PathBuf::from("/output"),
            Ok(DiskSpace {
                available_bytes: 150,
                total_bytes: 1_000,
            }),
        );
        values.insert(
            PathBuf::from("/state"),
            Ok(DiskSpace {
                available_bytes: 90,
                total_bytes: 1_000,
            }),
        );
        let monitor = StoragePressureMonitor::new(
            "/output",
            "/state",
            &output(),
            Arc::new(FixedProbe { values }),
        );

        let snapshot = monitor.assess().await;
        assert!(snapshot.rejects_new_captures());
        assert!(!snapshot.state_available());
        let alarm = snapshot.alarm_root().unwrap();
        assert_eq!(alarm.root, PathBuf::from("/state"));
        assert_eq!(alarm.free_bytes, Some(90));
        assert_eq!(alarm.free_percent, Some(9));
    }

    #[tokio::test]
    async fn unreadable_state_root_is_unavailable_without_leaking_probe_detail() {
        let mut values = BTreeMap::new();
        values.insert(
            PathBuf::from("/output"),
            Ok(DiskSpace {
                available_bytes: 900,
                total_bytes: 1_000,
            }),
        );
        values.insert(
            PathBuf::from("/state"),
            Err(CameraError::Storage("secret path detail".to_string())),
        );
        let monitor = StoragePressureMonitor::new(
            "/output",
            "/state",
            &output(),
            Arc::new(FixedProbe { values }),
        );

        let snapshot = monitor.assess().await;
        assert!(snapshot.state.unavailable());
        assert_eq!(snapshot.state.free_bytes, None);
        assert_eq!(snapshot.state.free_percent, None);
        assert!(!snapshot.state.readable);
    }
}
