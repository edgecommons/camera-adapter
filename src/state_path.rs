//! Platform-bound durable state-directory resolution.
//!
//! An explicit absolute path always wins. Implicit paths are deliberately unavailable for
//! Greengrass and never fall back to a working directory, temporary directory, or component work
//! directory.

use std::path::{Component, PathBuf};

use edgecommons::platform::Platform;

use crate::{CameraError, Result};

const UNIX_STATE_DIRECTORY: &str = "/var/lib/edgecommons/camera-adapter-state";

/// Resolves the binding durable state root for the selected deployment platform.
///
/// The returned path is absolute and lexically clean. Directory creation, permission enforcement,
/// locking, and SQLite integrity checks remain catalog-open responsibilities.
pub fn resolve_state_directory(platform: Platform, configured: Option<&str>) -> Result<PathBuf> {
    if let Some(configured) = configured {
        return validate_absolute_clean(PathBuf::from(configured), "configured state directory");
    }

    match platform {
        Platform::Greengrass => Err(CameraError::Config {
            path: "component.global.state.directory".to_owned(),
            message: "is required on GREENGRASS and must identify durable host storage".to_owned(),
        }),
        Platform::Kubernetes => validate_absolute_clean(
            PathBuf::from(UNIX_STATE_DIRECTORY),
            "Kubernetes state directory",
        ),
        Platform::Host => host_default(),
    }
}

#[cfg(target_os = "linux")]
fn host_default() -> Result<PathBuf> {
    validate_absolute_clean(PathBuf::from(UNIX_STATE_DIRECTORY), "HOST state directory")
}

#[cfg(windows)]
fn host_default() -> Result<PathBuf> {
    let program_data = known_folders::get_known_folder_path(
        known_folders::KnownFolder::ProgramData,
    )
    .ok_or_else(|| CameraError::Config {
        path: "component.global.state.directory".to_owned(),
        message: "Windows ProgramData known-folder resolution failed".to_owned(),
    })?;
    validate_absolute_clean(
        program_data
            .join("EdgeCommons")
            .join("camera-adapter")
            .join("state"),
        "Windows HOST state directory",
    )
}

#[cfg(not(any(target_os = "linux", windows)))]
fn host_default() -> Result<PathBuf> {
    Err(CameraError::Config {
        path: "component.global.state.directory".to_owned(),
        message: "this HOST platform requires an explicit durable state directory".to_owned(),
    })
}

fn validate_absolute_clean(path: PathBuf, label: &'static str) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(CameraError::Config {
            path: "component.global.state.directory".to_owned(),
            message: format!("{label} must be absolute on the running platform"),
        });
    }
    if path.parent().is_none() {
        return Err(CameraError::Config {
            path: "component.global.state.directory".to_owned(),
            message: format!("{label} must not be a filesystem root"),
        });
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(CameraError::Config {
            path: "component.global.state.directory".to_owned(),
            message: format!("{label} must not contain '.' or '..' components"),
        });
    }
    if path.as_os_str().is_empty() {
        return Err(CameraError::Config {
            path: "component.global.state.directory".to_owned(),
            message: format!("{label} must not be empty"),
        });
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_absolute_path_wins_for_every_platform() {
        let path = if cfg!(windows) {
            r"C:\EdgeCommons\camera-state"
        } else {
            "/srv/edgecommons/camera-state"
        };
        for platform in [Platform::Host, Platform::Kubernetes, Platform::Greengrass] {
            assert_eq!(
                resolve_state_directory(platform, Some(path)).expect("explicit state directory"),
                PathBuf::from(path)
            );
        }
    }

    #[test]
    fn relative_root_and_parent_traversal_are_rejected() {
        assert!(resolve_state_directory(Platform::Host, Some("relative/state")).is_err());
        let root = if cfg!(windows) { r"C:\" } else { "/" };
        assert!(resolve_state_directory(Platform::Host, Some(root)).is_err());
        let traversal = if cfg!(windows) {
            r"C:\EdgeCommons\..\state"
        } else {
            "/var/lib/../state"
        };
        assert!(resolve_state_directory(Platform::Host, Some(traversal)).is_err());
    }

    #[test]
    fn greengrass_has_no_implicit_fallback() {
        let error = resolve_state_directory(Platform::Greengrass, None)
            .expect_err("Greengrass requires an explicit durable path");
        assert!(error.to_string().contains("required on GREENGRASS"));
    }

    #[test]
    fn configured_relative_paths_report_the_state_field() {
        let path = if cfg!(windows) {
            r".\camera-state"
        } else {
            "./camera-state"
        };
        let error = resolve_state_directory(Platform::Kubernetes, Some(path)).unwrap_err();
        assert!(matches!(
            error,
            CameraError::Config { ref path, .. } if path == "component.global.state.directory"
        ));
        assert!(error.to_string().contains("must be absolute"));
    }

    #[cfg(not(windows))]
    #[test]
    fn implicit_kubernetes_path_is_never_a_filesystem_root() {
        let path = resolve_state_directory(Platform::Kubernetes, None).unwrap();
        assert!(path.is_absolute());
        assert!(path.parent().is_some());
        assert_ne!(path, PathBuf::from("/"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_kubernetes_requires_an_explicit_windows_durable_path() {
        assert!(resolve_state_directory(Platform::Kubernetes, None).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_host_and_kubernetes_use_the_bound_durable_path() {
        let expected = PathBuf::from(UNIX_STATE_DIRECTORY);
        assert_eq!(
            resolve_state_directory(Platform::Host, None).unwrap(),
            expected
        );
        assert_eq!(
            resolve_state_directory(Platform::Kubernetes, None).unwrap(),
            expected
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_host_uses_program_data_known_folder_not_environment_text() {
        let path = resolve_state_directory(Platform::Host, None).expect("ProgramData known folder");
        assert!(path.is_absolute());
        assert!(path.ends_with(std::path::Path::new("EdgeCommons/camera-adapter/state")));
    }
}
