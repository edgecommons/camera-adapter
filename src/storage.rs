//! Capability-scoped image persistence and crash reconciliation.
//!
//! Linux activation requires `openat2` containment. Every descendant is resolved from an already
//! open output-root descriptor with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_XDEV`.
//! Windows uses the accepted portable persistence profile: exclusive partials, streamed checksum
//! verification, sidecar-before-final ordering, and standard-library no-overwrite finalization.
//! It deliberately does not claim Linux-equivalent hostile-local-actor containment or atomic
//! no-clobber installation. Platforms without an enabled backend fail closed.

use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

#[cfg(any(target_os = "linux", windows))]
use std::io::{Read, Seek, SeekFrom};

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde_json::Value;
#[cfg(any(target_os = "linux", windows))]
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
#[cfg(any(target_os = "linux", windows))]
use url::Url;

use crate::catalog::{Catalog, InstallOutcome};
use crate::config::OutputConfig;
use crate::encoding::EncodingRequest;
#[cfg(any(target_os = "linux", windows))]
use crate::encoding::encode_to;
use crate::messages::ImageArtifact;
use crate::model::{CaptureFrame, OutputEncoding};
use crate::{CameraError, ErrorCode, Result};

#[cfg(any(target_os = "linux", windows))]
const CHECKSUM_BUFFER_BYTES: usize = 64 * 1024;
#[cfg(any(target_os = "linux", windows))]
const MAX_SIDECAR_BYTES: u64 = 1024 * 1024;
const MAX_COMPONENT_BYTES: usize = 240;
const MAX_VARIABLE_BYTES: usize = 96;
#[cfg(target_os = "linux")]
const PARTIAL_PREFIX: &str = ".camera-adapter-";
#[cfg(windows)]
#[allow(dead_code)]
const PARTIAL_PREFIX: &str = ".camera-adapter-";

#[cfg(target_os = "linux")]
type PlatformFile = std::fs::File;
#[cfg(windows)]
type PlatformFile = std::fs::File;

/// Variables accepted by the output path templates.
#[derive(Debug, Clone)]
pub struct OutputPathVariables<'a> {
    /// Configured camera instance identifier.
    pub camera_id: &'a str,
    /// Durable capture identifier.
    pub capture_id: &'a str,
    /// Timestamp used for all date/time template fields.
    pub timestamp: DateTime<Utc>,
}

/// Validated root-relative image path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelativeOutputPath {
    path: PathBuf,
    wire: String,
}

impl RelativeOutputPath {
    /// Revalidates a root-relative `/`-separated path loaded from the durable catalog.
    pub fn from_stored(wire: &str) -> Result<Self> {
        let extension = Path::new(wire)
            .extension()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| CameraError::Storage("stored output path lacks an extension".into()))?;
        validate_relative_path(wire, extension)
    }

    /// Native root-relative path.
    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.path
    }

    /// Portable `/`-separated wire path.
    #[must_use]
    pub fn as_wire_path(&self) -> &str {
        &self.wire
    }
}

/// Renders and validates the configured directory/filename templates.
pub fn render_output_path(
    output: &OutputConfig,
    variables: OutputPathVariables<'_>,
    encoding: OutputEncoding,
) -> Result<RelativeOutputPath> {
    let camera_id = sanitize_variable(variables.camera_id, "cameraId")?;
    let capture_id = sanitize_variable(variables.capture_id, "captureId")?;
    let extension = extension_for(encoding);
    let replacements = [
        ("{cameraId}", camera_id.as_str()),
        ("{captureId}", capture_id.as_str()),
        ("{extension}", extension),
    ];
    let mut directory = output.camera_directory_template.clone();
    let mut filename = output.file_name_template.clone();
    for (needle, value) in replacements {
        directory = directory.replace(needle, value);
        filename = filename.replace(needle, value);
    }
    let date_values = [
        ("{yyyy}", format!("{:04}", variables.timestamp.year())),
        ("{MM}", format!("{:02}", variables.timestamp.month())),
        ("{dd}", format!("{:02}", variables.timestamp.day())),
        (
            "{timestamp}",
            format!(
                "{:04}{:02}{:02}T{:02}{:02}{:02}.{:03}Z",
                variables.timestamp.year(),
                variables.timestamp.month(),
                variables.timestamp.day(),
                variables.timestamp.hour(),
                variables.timestamp.minute(),
                variables.timestamp.second(),
                variables.timestamp.timestamp_subsec_millis()
            ),
        ),
    ];
    for (needle, value) in &date_values {
        directory = directory.replace(needle, value);
        filename = filename.replace(needle, value);
    }
    if directory.contains(['{', '}']) || filename.contains(['{', '}']) {
        return storage_error("output template contains an unresolved variable");
    }
    if filename.contains(['/', '\\']) {
        return storage_error("fileNameTemplate must render exactly one filename component");
    }
    let combined = if directory.is_empty() {
        filename
    } else {
        format!("{directory}/{filename}")
    };
    validate_relative_path(&combined, extension)
}

/// Disk reservation facts rechecked immediately before creating a partial file.
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageReservation {
    /// This capture's accepted maximum output reservation.
    pub current_bytes: u64,
    /// Other outstanding reservations for this output filesystem.
    pub other_bytes: u64,
}

/// One blocking prepare operation. Run it under the global writer permit or `spawn_blocking`.
pub struct PrepareCapture {
    /// Durable capture identifier; also scopes the exclusive partial name.
    pub capture_id: String,
    /// Validated final path.
    pub relative_path: RelativeOutputPath,
    /// Bounded source frame.
    pub frame: CaptureFrame,
    /// Encoding and maximum installed size.
    pub encoding: EncodingRequest,
    /// Admission's disk reservation snapshot.
    pub reservation: StorageReservation,
}

/// Authoritative outcome of the catalog's `install_started` compare-and-set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallDecision {
    /// This writer won and may expose the final image.
    Started,
    /// Installation was already irreversible; only recovery may continue it.
    AlreadyStarted,
    /// Cancellation or another terminal state won before installation.
    Rejected,
}

/// Catalog seam used between durable sidecar installation and final-image visibility.
#[async_trait]
pub trait InstallGate: Send + Sync {
    /// Attempts the durable `install_started=false -> true` transition.
    async fn begin_install(&self, capture_id: &str, changed_at_ms: i64) -> Result<InstallDecision>;
}

#[async_trait]
impl InstallGate for Catalog {
    async fn begin_install(&self, capture_id: &str, changed_at_ms: i64) -> Result<InstallDecision> {
        Ok(
            match Catalog::try_begin_install(self, capture_id.to_string(), changed_at_ms).await? {
                InstallOutcome::Started(_) => InstallDecision::Started,
                InstallOutcome::AlreadyStarted(_) => InstallDecision::AlreadyStarted,
                InstallOutcome::WrongState(_) => InstallDecision::Rejected,
            },
        )
    }
}

#[derive(Clone)]
struct StoragePolicy {
    canonical_root: PathBuf,
    write_sidecar: bool,
    minimum_free_bytes: u64,
    minimum_free_percent: u8,
    #[cfg(target_os = "linux")]
    directory_mode: u32,
    #[cfg(target_os = "linux")]
    file_mode: u32,
}

/// Observation points inside the capability's install path.
///
/// The windows this seam exposes -- between creating a partial and the parent it was resolved
/// against, and between proving a destination absent and publishing it -- are only reachable from
/// inside the capability, and a race test that cannot reach them proves nothing. The seam is
/// therefore compiled into every build: production installs [`NoopInstallObserver`] and a test
/// installs one that swaps the parent out from under the capability, but both run the same
/// syscalls in the same order. A `cfg(test)`-only hook would have made the shipped install path a
/// path no test had ever executed.
pub trait InstallObserver: Send + Sync {
    /// Fires immediately before a partial is exclusively created, with the parent already resolved.
    ///
    /// `partial` names the entry as the backend addresses it: relative to the descriptor the Linux
    /// backend creates it from, absolute on the portable backend.
    fn before_partial_open(&self, _partial: &Path) {}

    /// Fires inside the no-clobber install window -- after the parent has been revalidated (Linux)
    /// or the destination proven absent (portable), and before the link that publishes it.
    ///
    /// `destination` follows the same naming convention as `partial` above.
    fn before_install_link(&self, _destination: &Path) {}
}

/// The observer every production root installs: it watches the install path and does nothing.
#[derive(Debug, Default)]
pub struct NoopInstallObserver;

impl InstallObserver for NoopInstallObserver {}

/// Open capability for one canonical output root.
#[derive(Clone)]
pub struct StorageRoot {
    policy: Arc<StoragePolicy>,
    #[cfg(target_os = "linux")]
    capability: Arc<linux::RootCapability>,
    #[cfg(windows)]
    capability: Arc<portable::RootCapability>,
}

impl fmt::Debug for StorageRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRoot")
            .field("canonical_root", &self.policy.canonical_root)
            .field("write_sidecar", &self.policy.write_sidecar)
            .finish_non_exhaustive()
    }
}

impl StorageRoot {
    /// Opens and validates an existing absolute output root.
    ///
    /// The root itself is canonicalized once, then all descendant work is capability-relative.
    /// Unsupported platforms/filesystems are rejected at startup.
    pub fn open(output: &OutputConfig) -> Result<Self> {
        Self::open_with_observer(output, Arc::new(NoopInstallObserver))
    }

    /// Opens the same root under a caller-supplied [`InstallObserver`].
    ///
    /// Same validation, same capability, same install syscalls in the same order as
    /// [`StorageRoot::open`] -- the observer only watches. It is how a race test reaches an install
    /// window that is otherwise unreachable from outside the capability.
    pub fn open_with_observer(
        output: &OutputConfig,
        observer: Arc<dyn InstallObserver>,
    ) -> Result<Self> {
        let configured = Path::new(&output.root_directory);
        if !configured.is_absolute() {
            return storage_error("output rootDirectory must be absolute");
        }
        let canonical_root = std::fs::canonicalize(configured).map_err(|error| {
            CameraError::Storage(format!(
                "output root '{}' must already exist and be canonicalizable: {error}",
                configured.display()
            ))
        })?;
        if !canonical_root.is_dir() {
            return storage_error("output rootDirectory is not a directory");
        }
        let policy = Arc::new(StoragePolicy {
            canonical_root: canonical_root.clone(),
            write_sidecar: output.write_metadata_sidecar,
            minimum_free_bytes: output.minimum_free_bytes,
            minimum_free_percent: output.minimum_free_percent,
            #[cfg(target_os = "linux")]
            directory_mode: parse_mode(&output.directory_mode, false)?,
            #[cfg(target_os = "linux")]
            file_mode: parse_mode(&output.file_mode, true)?,
        });

        #[cfg(target_os = "linux")]
        {
            let capability = Arc::new(linux::RootCapability::open(
                &canonical_root,
                policy.directory_mode,
                policy.file_mode,
                observer,
            )?);
            Ok(Self { policy, capability })
        }
        #[cfg(windows)]
        {
            let capability = Arc::new(portable::RootCapability::open(&canonical_root, observer)?);
            Ok(Self { policy, capability })
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            let _ = policy;
            let _ = observer;
            storage_error(
                "this platform has no enabled handle-relative no-follow/no-clobber backend; output root rejected",
            )
        }
    }

    /// Canonical native output root.
    #[must_use]
    pub fn canonical_root(&self) -> &Path {
        &self.policy.canonical_root
    }

    /// Rechecks storage floors against this capture and all other outstanding reservations.
    pub fn check_storage_pressure(&self, reservation: StorageReservation) -> Result<()> {
        let available = fs4::available_space(&self.policy.canonical_root)?;
        let total = fs4::total_space(&self.policy.canonical_root)?;
        let reserved = reservation
            .current_bytes
            .checked_add(reservation.other_bytes)
            .ok_or_else(|| {
                CameraError::rejected(ErrorCode::ResourceLimit, "disk reservation overflow")
            })?;
        let remaining = available.saturating_sub(reserved);
        let remaining_percent = if total == 0 {
            0
        } else {
            ((u128::from(remaining) * 100) / u128::from(total)) as u8
        };
        if remaining < self.policy.minimum_free_bytes
            || remaining_percent < self.policy.minimum_free_percent
        {
            return Err(CameraError::rejected(
                ErrorCode::StoragePressure,
                "output filesystem would cross its configured free-space floor",
            ));
        }
        Ok(())
    }

    /// Creates an exclusive partial, streams encoding, hashes it, and durably flushes file data.
    ///
    /// This is blocking filesystem/codec work and intentionally does not acquire `install_started`.
    pub fn prepare_capture(
        &self,
        request: PrepareCapture,
        cancellation: &CancellationToken,
    ) -> Result<PreparedInstall> {
        self.check_storage_pressure(request.reservation)?;
        if request.reservation.current_bytes < request.encoding.maximum_output_bytes {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "encoding ceiling exceeds the accepted disk reservation",
            ));
        }
        if request.frame.bytes.len() as u64 > request.reservation.current_bytes {
            return Err(CameraError::rejected(
                ErrorCode::ResourceLimit,
                "source frame exceeds the accepted disk reservation",
            ));
        }
        cancelled(cancellation, "before persistence")?;

        #[cfg(target_os = "linux")]
        {
            self.prepare_linux(request, cancellation)
        }
        #[cfg(windows)]
        {
            self.prepare_windows(request, cancellation)
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            let _ = (request, cancellation);
            storage_error("storage capability backend is unavailable on this platform")
        }
    }

    #[cfg(target_os = "linux")]
    fn prepare_linux(
        &self,
        request: PrepareCapture,
        cancellation: &CancellationToken,
    ) -> Result<PreparedInstall> {
        let components = normal_components(request.relative_path.as_path())?;
        let (final_name, parent_components) = components
            .split_last()
            .ok_or_else(|| CameraError::Storage("output path has no filename".to_string()))?;
        let parent = self.capability.open_parent(parent_components, true)?;
        self.capability
            .revalidate_parent_at(&parent, parent_components)?;
        let capture_token = sanitize_variable(&request.capture_id, "captureId")?;
        let partial_name = format!("{PARTIAL_PREFIX}{capture_token}.image.partial");
        let sidecar_final_name = format!("{final_name}.json");
        let sidecar_partial_name = format!("{PARTIAL_PREFIX}{capture_token}.sidecar.partial");
        linux::ensure_absent(&parent, final_name, "final image")?;
        linux::ensure_absent(&parent, &sidecar_final_name, "metadata sidecar")?;
        let fd = self
            .capability
            .create_partial_path(parent_components, &partial_name)?;
        let mut file = PlatformFile::from(fd);
        let encoded = match encode_to(&request.frame, request.encoding, &mut file, cancellation) {
            Ok(encoded) => encoded,
            Err(error) => {
                drop(file);
                let _ = linux::remove_file(&parent, &partial_name);
                return Err(error);
            }
        };
        file.sync_all().map_err(|error| {
            CameraError::Storage(format!("failed to durably flush image partial: {error}"))
        })?;
        let (bytes, sha256) = stream_checksum(&mut file, cancellation)?;
        if bytes != encoded.bytes {
            let _ = linux::remove_file(&parent, &partial_name);
            return storage_error("encoded byte count changed before durable flush");
        }
        drop(file);

        let absolute_path = self
            .policy
            .canonical_root
            .join(request.relative_path.as_path());
        let partial_path = absolute_path
            .with_file_name(&partial_name)
            .to_string_lossy()
            .into_owned();
        let file_uri = Url::from_file_path(&absolute_path)
            .map_err(|_| CameraError::Storage("failed to build final file URI".to_string()))?
            .to_string();
        let sidecar_relative_path = self.policy.write_sidecar.then(|| {
            let mut path = request.relative_path.as_path().to_path_buf();
            path.set_file_name(&sidecar_final_name);
            path_to_wire(&path)
        });
        let artifact = ImageArtifact {
            absolute_path: absolute_path.to_string_lossy().into_owned(),
            relative_path: request.relative_path.as_wire_path().to_string(),
            file_uri,
            content_type: encoded.content_type.to_string(),
            encoding: encoded.encoding,
            bytes,
            sha256,
            metadata_sidecar_relative_path: sidecar_relative_path,
        };
        Ok(PreparedInstall {
            root: self.clone(),
            parent_components: parent_components.to_vec(),
            capture_id: request.capture_id,
            final_name: final_name.clone(),
            partial_name,
            sidecar_final_name,
            sidecar_partial_name,
            partial_path,
            artifact,
            installation_started: false,
            sidecar_installed: false,
            completed: false,
        })
    }

    #[cfg(windows)]
    fn prepare_windows(
        &self,
        request: PrepareCapture,
        cancellation: &CancellationToken,
    ) -> Result<PreparedInstall> {
        let components = normal_components(request.relative_path.as_path())?;
        let (final_name, parent_components) = components
            .split_last()
            .ok_or_else(|| CameraError::Storage("output path has no filename".to_string()))?;
        let parent = self.capability.open_parent(parent_components, true)?;
        let capture_token = sanitize_variable(&request.capture_id, "captureId")?;
        let partial_name = format!("{PARTIAL_PREFIX}{capture_token}.image.partial");
        let sidecar_final_name = format!("{final_name}.json");
        let sidecar_partial_name = format!("{PARTIAL_PREFIX}{capture_token}.sidecar.partial");
        portable::ensure_absent(&parent, final_name, "final image")?;
        portable::ensure_absent(&parent, &sidecar_final_name, "metadata sidecar")?;
        let mut file = self.capability.create_partial(&parent, &partial_name)?;
        let encoded = match encode_to(&request.frame, request.encoding, &mut file, cancellation) {
            Ok(encoded) => encoded,
            Err(error) => {
                drop(file);
                let _ = portable::remove_file(&parent, &partial_name);
                return Err(error);
            }
        };
        file.sync_all().map_err(|error| {
            CameraError::Storage(format!("failed to durably flush image partial: {error}"))
        })?;
        let (bytes, sha256) = stream_checksum(&mut file, cancellation)?;
        if bytes != encoded.bytes {
            drop(file);
            let _ = portable::remove_file(&parent, &partial_name);
            return storage_error("encoded byte count changed before durable flush");
        }
        drop(file);
        let absolute_path = self
            .policy
            .canonical_root
            .join(request.relative_path.as_path());
        let partial_path = absolute_path
            .with_file_name(&partial_name)
            .to_string_lossy()
            .into_owned();
        let file_uri = Url::from_file_path(&absolute_path)
            .map_err(|_| CameraError::Storage("failed to build final file URI".to_string()))?
            .to_string();
        let sidecar_relative_path = self.policy.write_sidecar.then(|| {
            let mut path = request.relative_path.as_path().to_path_buf();
            path.set_file_name(&sidecar_final_name);
            path_to_wire(&path)
        });
        let artifact = ImageArtifact {
            absolute_path: absolute_path.to_string_lossy().into_owned(),
            relative_path: request.relative_path.as_wire_path().to_string(),
            file_uri,
            content_type: encoded.content_type.to_string(),
            encoding: encoded.encoding,
            bytes,
            sha256,
            metadata_sidecar_relative_path: sidecar_relative_path,
        };
        Ok(PreparedInstall {
            root: self.clone(),
            parent_components: parent_components.to_vec(),
            capture_id: request.capture_id,
            final_name: final_name.clone(),
            partial_name,
            sidecar_final_name,
            sidecar_partial_name,
            partial_path,
            artifact,
            installation_started: false,
            sidecar_installed: false,
            completed: false,
        })
    }

    /// Reconciles a catalog record for which `install_started` is already true.
    pub fn reconcile_install_started(
        &self,
        request: RecoveryRequest,
        cancellation: &CancellationToken,
    ) -> Result<RecoveryOutcome> {
        #[cfg(target_os = "linux")]
        {
            self.reconcile_linux(request, cancellation)
        }
        #[cfg(windows)]
        {
            self.reconcile_windows(request, cancellation)
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            let _ = (request, cancellation);
            storage_error("storage capability backend is unavailable on this platform")
        }
    }

    #[cfg(target_os = "linux")]
    fn reconcile_linux(
        &self,
        request: RecoveryRequest,
        cancellation: &CancellationToken,
    ) -> Result<RecoveryOutcome> {
        let components = normal_components(request.relative_path.as_path())?;
        let (final_name, parents) = components
            .split_last()
            .ok_or_else(|| CameraError::Storage("recovery path has no filename".to_string()))?;
        let parent = self.capability.open_parent(parents, false)?;
        self.capability.revalidate_parent_at(&parent, parents)?;
        let token = sanitize_variable(&request.capture_id, "captureId")?;
        let partial = format!("{PARTIAL_PREFIX}{token}.image.partial");
        let sidecar = format!("{final_name}.json");

        if let Some(mut final_file) = linux::open_existing(&parent, final_name)? {
            verify_file(
                &mut final_file,
                request.expected_bytes,
                &request.expected_sha256,
                cancellation,
            )?;
            match request.sidecar_body.as_ref() {
                Some(expected) if !linux::exact_sidecar(&parent, &sidecar, expected)? => {
                    return storage_error("final image exists without its exact metadata sidecar");
                }
                None => linux::ensure_absent(
                    &parent,
                    &sidecar,
                    "unexpected metadata sidecar during recovery",
                )?,
                Some(_) => {}
            }
            return Ok(RecoveryOutcome::AlreadyInstalled);
        }

        let sidecar_present = match request.sidecar_body.as_ref() {
            Some(expected) => match linux::exact_sidecar(&parent, &sidecar, expected) {
                Ok(present) => present,
                Err(error) => {
                    // Before the image is visible both artifacts belong only to this record. A
                    // sidecar mismatch makes the partial unrecoverable because publishing it would
                    // bind an image to metadata different from the exact durable terminal body.
                    let _ = linux::remove_file(&parent, &partial);
                    let _ = linux::remove_file(&parent, &sidecar);
                    let _ = self.capability.sync_parent(&parent, parents);
                    return Err(error);
                }
            },
            None => {
                linux::ensure_absent(
                    &parent,
                    &sidecar,
                    "unexpected metadata sidecar during recovery",
                )?;
                false
            }
        };
        let Some(mut partial_file) = linux::open_existing(&parent, &partial)? else {
            if sidecar_present {
                linux::remove_file(&parent, &sidecar)?;
                self.capability.sync_parent(&parent, parents)?;
            }
            return Ok(RecoveryOutcome::MissingArtifactsCleaned);
        };
        if request.sidecar_body.is_some() && !sidecar_present {
            drop(partial_file);
            linux::remove_file(&parent, &partial)?;
            self.capability.sync_parent(&parent, parents)?;
            return Ok(RecoveryOutcome::MissingArtifactsCleaned);
        }
        if let Err(error) = verify_file(
            &mut partial_file,
            request.expected_bytes,
            &request.expected_sha256,
            cancellation,
        ) {
            drop(partial_file);
            let _ = linux::remove_file(&parent, &partial);
            if sidecar_present {
                let _ = linux::remove_file(&parent, &sidecar);
            }
            let _ = self.capability.sync_parent(&parent, parents);
            return Err(error);
        }
        drop(partial_file);
        self.capability
            .install_no_clobber(&parent, parents, &partial, final_name)?;
        self.capability.sync_parent(&parent, parents)?;
        Ok(RecoveryOutcome::InstalledFromPartial)
    }

    #[cfg(windows)]
    fn reconcile_windows(
        &self,
        request: RecoveryRequest,
        cancellation: &CancellationToken,
    ) -> Result<RecoveryOutcome> {
        let components = normal_components(request.relative_path.as_path())?;
        let (final_name, parents) = components
            .split_last()
            .ok_or_else(|| CameraError::Storage("recovery path has no filename".to_string()))?;
        let parent = self.capability.open_parent(parents, false)?;
        let token = sanitize_variable(&request.capture_id, "captureId")?;
        let partial = format!("{PARTIAL_PREFIX}{token}.image.partial");
        let sidecar = format!("{final_name}.json");

        if let Some(mut final_file) = portable::open_existing(&parent, final_name)? {
            verify_file(
                &mut final_file,
                request.expected_bytes,
                &request.expected_sha256,
                cancellation,
            )?;
            match request.sidecar_body.as_ref() {
                Some(expected) if !portable::exact_sidecar(&parent, &sidecar, expected)? => {
                    return storage_error("final image exists without its exact metadata sidecar");
                }
                None => portable::ensure_absent(
                    &parent,
                    &sidecar,
                    "unexpected metadata sidecar during recovery",
                )?,
                Some(_) => {}
            }
            return Ok(RecoveryOutcome::AlreadyInstalled);
        }

        let sidecar_present = match request.sidecar_body.as_ref() {
            Some(expected) => match portable::exact_sidecar(&parent, &sidecar, expected) {
                Ok(present) => present,
                Err(error) => {
                    let _ = portable::remove_file(&parent, &partial);
                    let _ = portable::remove_file(&parent, &sidecar);
                    let _ = portable::sync_dir(&parent);
                    return Err(error);
                }
            },
            None => {
                portable::ensure_absent(
                    &parent,
                    &sidecar,
                    "unexpected metadata sidecar during recovery",
                )?;
                false
            }
        };
        let Some(mut partial_file) = portable::open_existing(&parent, &partial)? else {
            if sidecar_present {
                portable::remove_file(&parent, &sidecar)?;
                portable::sync_dir(&parent)?;
            }
            return Ok(RecoveryOutcome::MissingArtifactsCleaned);
        };
        if request.sidecar_body.is_some() && !sidecar_present {
            drop(partial_file);
            portable::remove_file(&parent, &partial)?;
            portable::sync_dir(&parent)?;
            return Ok(RecoveryOutcome::MissingArtifactsCleaned);
        }
        if let Err(error) = verify_file(
            &mut partial_file,
            request.expected_bytes,
            &request.expected_sha256,
            cancellation,
        ) {
            drop(partial_file);
            let _ = portable::remove_file(&parent, &partial);
            if sidecar_present {
                let _ = portable::remove_file(&parent, &sidecar);
            }
            let _ = portable::sync_dir(&parent);
            return Err(error);
        }
        drop(partial_file);
        self.capability
            .install_best_effort(&parent, &partial, final_name)?;
        portable::sync_dir(&parent)?;
        Ok(RecoveryOutcome::InstalledFromPartial)
    }
}

/// Prepared image partial awaiting optional sidecar and the catalog installation CAS.
pub struct PreparedInstall {
    #[cfg(any(target_os = "linux", windows))]
    root: StorageRoot,
    #[cfg(any(target_os = "linux", windows))]
    parent_components: Vec<String>,
    capture_id: String,
    #[cfg(any(target_os = "linux", windows))]
    final_name: String,
    #[cfg(any(target_os = "linux", windows))]
    partial_name: String,
    #[cfg(any(target_os = "linux", windows))]
    sidecar_final_name: String,
    #[cfg(any(target_os = "linux", windows))]
    sidecar_partial_name: String,
    partial_path: String,
    artifact: ImageArtifact,
    installation_started: bool,
    #[cfg(any(target_os = "linux", windows))]
    sidecar_installed: bool,
    #[cfg(any(target_os = "linux", windows))]
    completed: bool,
}

impl fmt::Debug for PreparedInstall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedInstall")
            .field("capture_id", &self.capture_id)
            .field("relative_path", &self.artifact.relative_path)
            .field("installation_started", &self.installation_started)
            .finish_non_exhaustive()
    }
}

impl PreparedInstall {
    /// Absolute same-directory partial path recorded for targeted crash recovery.
    #[must_use]
    pub fn partial_path(&self) -> &str {
        &self.partial_path
    }

    /// Complete terminal image metadata. A caller uses this to construct the sidecar body before
    /// installation; the path becomes externally visible only after [`Self::install`] succeeds.
    #[must_use]
    pub fn artifact(&self) -> &ImageArtifact {
        &self.artifact
    }

    /// Installs the required sidecar first, wins the catalog CAS, then exposes the image exactly once.
    #[allow(unused_mut)]
    pub async fn install(
        mut self,
        gate: &dyn InstallGate,
        changed_at_ms: i64,
        sidecar_body: Option<&Value>,
        cancellation: &CancellationToken,
    ) -> Result<ImageArtifact> {
        #[cfg(target_os = "linux")]
        {
            let parent = self
                .root
                .capability
                .open_parent(&self.parent_components, false)?;
            self.root
                .capability
                .revalidate_parent_at(&parent, &self.parent_components)?;
            cancelled(cancellation, "before atomic installation")?;
            match (self.root.policy.write_sidecar, sidecar_body) {
                (true, Some(body)) => {
                    if !body.is_object() {
                        return storage_error("metadata sidecar body must be a JSON object");
                    }
                    self.root.capability.write_sidecar(
                        &parent,
                        &self.parent_components,
                        &self.sidecar_partial_name,
                        &self.sidecar_final_name,
                        body,
                        cancellation,
                    )?;
                    self.sidecar_installed = true;
                    self.root
                        .capability
                        .sync_parent(&parent, &self.parent_components)?;
                }
                (true, None) => return storage_error("required metadata sidecar body is missing"),
                (false, Some(_)) => {
                    return storage_error(
                        "sidecar body supplied while writeMetadataSidecar is false",
                    );
                }
                (false, None) => {}
            }

            // Do not cancel this future after submission: the durable CAS, not the local token,
            // decides whether cancellation or installation won.
            let decision = match gate.begin_install(&self.capture_id, changed_at_ms).await {
                Ok(decision) => decision,
                Err(error) => {
                    // A transport/worker error cannot prove whether the durable CAS committed.
                    // Preserve record-owned artifacts so startup reconciliation can inspect the
                    // catalog instead of deleting the only recoverable image after an ambiguous
                    // response.
                    self.installation_started = true;
                    return Err(error);
                }
            };
            match decision {
                InstallDecision::Started => self.installation_started = true,
                InstallDecision::AlreadyStarted => {
                    self.installation_started = true;
                    return storage_error(
                        "install_started was already true; startup reconciliation owns this job",
                    );
                }
                InstallDecision::Rejected => {
                    return Err(CameraError::rejected(
                        ErrorCode::CaptureCancelled,
                        "cancellation or another terminal state won before installation",
                    ));
                }
            }

            self.root.capability.install_no_clobber(
                &parent,
                &self.parent_components,
                &self.partial_name,
                &self.final_name,
            )?;
            self.root
                .capability
                .sync_parent(&parent, &self.parent_components)?;
            let mut installed = linux::open_existing(&parent, &self.final_name)?
                .ok_or_else(|| CameraError::Storage("installed image disappeared".to_string()))?;
            // Once install_started wins, local cancellation is no longer authoritative.
            let post_install = CancellationToken::new();
            verify_file(
                &mut installed,
                self.artifact.bytes,
                &self.artifact.sha256,
                &post_install,
            )?;
            self.completed = true;
            Ok(self.artifact.clone())
        }
        #[cfg(windows)]
        {
            let parent = self
                .root
                .capability
                .open_parent(&self.parent_components, false)?;
            cancelled(cancellation, "before atomic installation")?;
            match (self.root.policy.write_sidecar, sidecar_body) {
                (true, Some(body)) => {
                    if !body.is_object() {
                        return storage_error("metadata sidecar body must be a JSON object");
                    }
                    portable::write_sidecar(
                        &parent,
                        &self.sidecar_partial_name,
                        &self.sidecar_final_name,
                        body,
                        cancellation,
                    )?;
                    self.sidecar_installed = true;
                    portable::sync_dir(&parent)?;
                }
                (true, None) => return storage_error("required metadata sidecar body is missing"),
                (false, Some(_)) => {
                    return storage_error(
                        "sidecar body supplied while writeMetadataSidecar is false",
                    );
                }
                (false, None) => {}
            }

            let decision = match gate.begin_install(&self.capture_id, changed_at_ms).await {
                Ok(decision) => decision,
                Err(error) => {
                    self.installation_started = true;
                    return Err(error);
                }
            };
            match decision {
                InstallDecision::Started => self.installation_started = true,
                InstallDecision::AlreadyStarted => {
                    self.installation_started = true;
                    return storage_error(
                        "install_started was already true; startup reconciliation owns this job",
                    );
                }
                InstallDecision::Rejected => {
                    return Err(CameraError::rejected(
                        ErrorCode::CaptureCancelled,
                        "cancellation or another terminal state won before installation",
                    ));
                }
            }

            self.root.capability.install_best_effort(
                &parent,
                &self.partial_name,
                &self.final_name,
            )?;
            portable::sync_dir(&parent)?;
            let mut installed = portable::open_existing(&parent, &self.final_name)?
                .ok_or_else(|| CameraError::Storage("installed image disappeared".to_string()))?;
            let post_install = CancellationToken::new();
            verify_file(
                &mut installed,
                self.artifact.bytes,
                &self.artifact.sha256,
                &post_install,
            )?;
            self.completed = true;
            Ok(self.artifact.clone())
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            let _ = (gate, changed_at_ms, sidecar_body, cancellation);
            storage_error("storage capability backend is unavailable on this platform")
        }
    }
}

impl Drop for PreparedInstall {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if !self.completed && !self.installation_started {
            if let Ok(parent) = self
                .root
                .capability
                .open_parent(&self.parent_components, false)
            {
                let _ = linux::remove_file(&parent, &self.partial_name);
                let _ = linux::remove_file(&parent, &self.sidecar_partial_name);
                if self.sidecar_installed {
                    let _ = linux::remove_file(&parent, &self.sidecar_final_name);
                }
                let _ = self
                    .root
                    .capability
                    .sync_parent(&parent, &self.parent_components);
            }
        }
        #[cfg(windows)]
        if !self.completed && !self.installation_started {
            if let Ok(parent) = self
                .root
                .capability
                .open_parent(&self.parent_components, false)
            {
                let _ = portable::remove_file(&parent, &self.partial_name);
                let _ = portable::remove_file(&parent, &self.sidecar_partial_name);
                if self.sidecar_installed {
                    let _ = portable::remove_file(&parent, &self.sidecar_final_name);
                }
                let _ = portable::sync_dir(&parent);
            }
        }
    }
}

/// Material needed to reconcile one `PERSISTING/install_started=true` catalog record.
pub struct RecoveryRequest {
    /// Durable capture identifier.
    pub capture_id: String,
    /// Stored intended final path.
    pub relative_path: RelativeOutputPath,
    /// Durable expected image byte count.
    pub expected_bytes: u64,
    /// Durable lower-case SHA-256 digest.
    pub expected_sha256: String,
    /// Exact durably staged terminal body required in the sidecar, or `None` when disabled.
    pub sidecar_body: Option<Value>,
}

/// Result of targeted crash reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryOutcome {
    /// Final image (and required sidecar) already existed and verified.
    AlreadyInstalled,
    /// Verified partial was no-clobber installed.
    InstalledFromPartial,
    /// No recoverable image remained; exact record-owned artifacts were removed.
    MissingArtifactsCleaned,
}

fn validate_relative_path(path: &str, extension: &str) -> Result<RelativeOutputPath> {
    if path.is_empty() || Path::new(path).is_absolute() {
        return storage_error("rendered output path must be non-empty and relative");
    }
    let normalized = path.replace('\\', "/");
    let mut components = Vec::new();
    for component in Path::new(&normalized).components() {
        match component {
            Component::Normal(value) => {
                let value = value.to_str().ok_or_else(|| {
                    CameraError::Storage("output path is not valid UTF-8".to_string())
                })?;
                validate_component(value)?;
                components.push(value.to_string());
            }
            _ => return storage_error("rendered output path contains traversal or a root/prefix"),
        }
    }
    let filename = components
        .last()
        .ok_or_else(|| CameraError::Storage("rendered output path is empty".to_string()))?;
    let actual_extension = Path::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if !actual_extension.eq_ignore_ascii_case(extension) {
        return storage_error(format!(
            "rendered filename extension '{actual_extension}' does not match '{extension}'"
        ));
    }
    let wire = components.join("/");
    Ok(RelativeOutputPath {
        path: components.iter().collect(),
        wire,
    })
}

#[cfg(any(target_os = "linux", windows))]
#[allow(dead_code)]
fn normal_components(path: &Path) -> Result<Vec<String>> {
    path.components()
        .map(|component| match component {
            Component::Normal(value) => value
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| CameraError::Storage("output component is not UTF-8".to_string())),
            _ => storage_error("output path is not strictly relative"),
        })
        .collect()
}

fn validate_component(value: &str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.len() > MAX_COMPONENT_BYTES
        || value.ends_with(['.', ' '])
        || value
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | '\\' | ':' | '\0'))
    {
        return storage_error(format!("unsafe output path component '{value}'"));
    }
    let stem = value
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.as_bytes()[3].is_ascii_digit()
            && stem.as_bytes()[3] != b'0')
    {
        return storage_error(format!("reserved output path component '{value}'"));
    }
    Ok(())
}

fn sanitize_variable(value: &str, field: &str) -> Result<String> {
    if value.is_empty() {
        return storage_error(format!("{field} must be non-empty"));
    }
    let mut output = String::with_capacity(value.len().min(MAX_VARIABLE_BYTES));
    for character in value.chars() {
        let safe = if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
            character
        } else {
            '_'
        };
        if output.len() + safe.len_utf8() > MAX_VARIABLE_BYTES {
            break;
        }
        output.push(safe);
    }
    if output.is_empty() || output == "." || output == ".." {
        return storage_error(format!("{field} did not contain a safe filename character"));
    }
    Ok(output)
}

fn extension_for(encoding: OutputEncoding) -> &'static str {
    match encoding {
        OutputEncoding::Passthrough | OutputEncoding::Jpeg => "jpg",
        OutputEncoding::Png => "png",
        OutputEncoding::Tiff => "tiff",
        OutputEncoding::Raw => "raw",
    }
}

#[cfg(target_os = "linux")]
fn parse_mode(value: &str, file: bool) -> Result<u32> {
    if value.len() != 4 || !value.starts_with('0') {
        return storage_error("output mode must be four-digit octal");
    }
    let mode = u32::from_str_radix(value, 8)
        .map_err(|error| CameraError::Storage(format!("invalid octal mode '{value}': {error}")))?;
    let maximum = if file { 0o640 } else { 0o750 };
    if mode > 0o777 || mode & !maximum != 0 {
        return storage_error("output mode is more permissive than the design's 0640/0750 ceiling");
    }
    Ok(mode)
}

#[cfg(any(target_os = "linux", windows))]
#[allow(dead_code)]
fn path_to_wire(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(any(target_os = "linux", windows))]
fn stream_checksum(
    file: &mut PlatformFile,
    cancellation: &CancellationToken,
) -> Result<(u64, String)> {
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; CHECKSUM_BUFFER_BYTES];
    let mut bytes = 0_u64;
    loop {
        cancelled(cancellation, "while checksumming the partial")?;
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| CameraError::Storage("image byte count overflow".to_string()))?;
    }
    Ok((bytes, hex::encode(digest.finalize())))
}

#[cfg(any(target_os = "linux", windows))]
fn verify_file(
    file: &mut PlatformFile,
    expected_bytes: u64,
    expected_sha256: &str,
    cancellation: &CancellationToken,
) -> Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != expected_bytes {
        return storage_error("persisted image size/type does not match the catalog record");
    }
    let (bytes, sha256) = stream_checksum(file, cancellation)?;
    if bytes != expected_bytes || !sha256.eq_ignore_ascii_case(expected_sha256) {
        return storage_error("persisted image checksum does not match the catalog record");
    }
    Ok(())
}

fn cancelled(cancellation: &CancellationToken, stage: &'static str) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(CameraError::rejected(
            ErrorCode::CaptureCancelled,
            format!("capture cancelled {stage}"),
        ))
    } else {
        Ok(())
    }
}

fn storage_error<T>(message: impl Into<String>) -> Result<T> {
    Err(CameraError::Storage(message.into()))
}

#[cfg(target_os = "linux")]
mod linux {
    use std::fs::File;
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, OwnedFd};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use rustix::fs::{AtFlags, Mode, OFlags, ResolveFlags};
    use rustix::io::Errno;
    use serde_json::Value;
    use tokio_util::sync::CancellationToken;

    use super::{InstallObserver, MAX_SIDECAR_BYTES, cancelled, storage_error};
    use crate::{CameraError, Result};

    const RESOLVE: ResolveFlags = ResolveFlags::BENEATH
        .union(ResolveFlags::NO_SYMLINKS)
        .union(ResolveFlags::NO_MAGICLINKS)
        .union(ResolveFlags::NO_XDEV);

    pub(super) struct RootCapability {
        canonical: PathBuf,
        fd: OwnedFd,
        device: u64,
        directory_mode: Mode,
        file_mode: Mode,
        observer: Arc<dyn InstallObserver>,
    }

    impl RootCapability {
        pub(super) fn open(
            canonical: &Path,
            directory_mode: u32,
            file_mode: u32,
            observer: Arc<dyn InstallObserver>,
        ) -> Result<Self> {
            let fd = rustix::fs::open(
                canonical,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| CameraError::Storage(format!("cannot open output root: {error}")))?;
            rustix::fs::openat2(
                &fd,
                ".",
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
                RESOLVE,
            )
            .map_err(|error| {
                CameraError::Storage(format!(
                    "output filesystem lacks required openat2 containment: {error}"
                ))
            })?;
            let stat = rustix::fs::fstat(&fd).map_err(|error| {
                CameraError::Storage(format!("cannot stat output root: {error}"))
            })?;
            if stat.st_mode & 0o022 != 0 {
                return storage_error(
                    "output root must not be writable by group or other principals",
                );
            }
            Ok(Self {
                canonical: canonical.to_path_buf(),
                fd,
                device: stat.st_dev,
                directory_mode: Mode::from_raw_mode(directory_mode),
                file_mode: Mode::from_raw_mode(file_mode),
                observer,
            })
        }

        pub(super) fn open_parent(&self, components: &[String], create: bool) -> Result<OwnedFd> {
            let mut current = rustix::io::dup(&self.fd).map_err(|error| {
                CameraError::Storage(format!("cannot duplicate root handle: {error}"))
            })?;
            for component in components {
                current = match open_directory(&current, component) {
                    Ok(directory) => directory,
                    Err(Errno::NOENT) if create => {
                        match rustix::fs::mkdirat(&current, component, self.directory_mode) {
                            Ok(()) | Err(Errno::EXIST) => {}
                            Err(error) => {
                                return storage_error(format!(
                                    "cannot create output directory '{component}': {error}"
                                ));
                            }
                        }
                        open_directory(&current, component).map_err(|error| {
                            CameraError::Storage(format!(
                                "created output directory '{component}' is unsafe: {error}"
                            ))
                        })?
                    }
                    Err(error) => {
                        return storage_error(format!(
                            "cannot open output directory '{component}' without following links: {error}"
                        ));
                    }
                };
                let stat = rustix::fs::fstat(&current).map_err(|error| {
                    CameraError::Storage(format!("cannot inspect output directory: {error}"))
                })?;
                if stat.st_dev != self.device {
                    return storage_error("output path crossed a mount/filesystem boundary");
                }
                if stat.st_mode & 0o022 != 0 {
                    return storage_error(
                        "output directory must not be writable by group or other principals",
                    );
                }
            }
            self.revalidate_parent_at(&current, components)?;
            Ok(current)
        }

        pub(super) fn revalidate_parent_at(
            &self,
            parent: &OwnedFd,
            components: &[String],
        ) -> Result<()> {
            let proc_path = PathBuf::from(format!("/proc/self/fd/{}", parent.as_raw_fd()));
            let current = std::fs::read_link(&proc_path).map_err(|error| {
                CameraError::Storage(format!(
                    "cannot revalidate final parent capability: {error}"
                ))
            })?;
            let expected = components
                .iter()
                .fold(self.canonical.clone(), |mut path, item| {
                    path.push(item);
                    path
                });
            if current != expected || current.to_string_lossy().ends_with(" (deleted)") {
                return storage_error(
                    "final parent capability no longer names its exact output-root-relative path",
                );
            }
            Ok(())
        }

        pub(super) fn create_partial_path(
            &self,
            parent_components: &[String],
            name: &str,
        ) -> Result<OwnedFd> {
            let path = parent_components
                .iter()
                .map(String::as_str)
                .chain(std::iter::once(name))
                .collect::<PathBuf>();
            self.observer.before_partial_open(&path);
            rustix::fs::openat2(
                &self.fd,
                &path,
                OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                self.file_mode,
                RESOLVE,
            )
            .map_err(|error| {
                CameraError::Storage(format!(
                    "cannot exclusively create partial '{name}': {error}"
                ))
            })
        }

        pub(super) fn install_no_clobber(
            &self,
            parent: &OwnedFd,
            parent_components: &[String],
            partial: &str,
            final_name: &str,
        ) -> Result<()> {
            self.revalidate_parent_at(parent, parent_components)?;
            self.observer.before_install_link(Path::new(final_name));
            rustix::fs::linkat(parent, partial, parent, final_name, AtFlags::empty()).map_err(
                |error| {
                    CameraError::Storage(format!(
                        "no-clobber install '{final_name}' failed: {error}"
                    ))
                },
            )?;
            if let Err(validation_error) = self.revalidate_parent_at(parent, parent_components) {
                let rollback = rustix::fs::unlinkat(parent, final_name, AtFlags::empty());
                let _ = rustix::fs::fsync(parent);
                return match rollback {
                    Ok(()) => storage_error(format!(
                        "output parent moved during no-clobber install; final link rolled back: {validation_error}"
                    )),
                    Err(rollback_error) => storage_error(format!(
                        "output parent moved during no-clobber install and final-link rollback failed: {validation_error}; rollback: {rollback_error}"
                    )),
                };
            }
            if let Err(error) = rustix::fs::unlinkat(parent, partial, AtFlags::empty()) {
                // The final hard link is already visible. Preserve the partial for targeted recovery
                // rather than claiming a clean success.
                return storage_error(format!(
                    "installed '{final_name}' but could not remove its partial: {error}"
                ));
            }
            Ok(())
        }

        pub(super) fn write_sidecar(
            &self,
            parent: &OwnedFd,
            parent_components: &[String],
            partial: &str,
            final_name: &str,
            body: &Value,
            cancellation: &CancellationToken,
        ) -> Result<()> {
            let fd = self.create_partial_path(parent_components, partial)?;
            let mut file = File::from(fd);
            let write_result = (|| -> Result<()> {
                let mut writer = SidecarWriter {
                    file: &mut file,
                    written: 0,
                    cancellation,
                };
                serde_json::to_writer(&mut writer, body)?;
                writer.write_all(b"\n")?;
                writer.flush()?;
                Ok(())
            })();
            if let Err(error) = write_result {
                drop(file);
                let _ = remove_file(parent, partial);
                return Err(error);
            }
            file.sync_all().map_err(|error| {
                CameraError::Storage(format!("failed to durably flush sidecar partial: {error}"))
            })?;
            drop(file);
            cancelled(cancellation, "before sidecar installation")?;
            self.install_no_clobber(parent, parent_components, partial, final_name)
        }

        pub(super) fn sync_parent(
            &self,
            parent: &OwnedFd,
            parent_components: &[String],
        ) -> Result<()> {
            self.revalidate_parent_at(parent, parent_components)?;
            rustix::fs::fsync(parent).map_err(|error| {
                CameraError::Storage(format!("failed to sync output directory: {error}"))
            })
        }
    }

    fn open_directory(parent: &OwnedFd, name: &str) -> rustix::io::Result<OwnedFd> {
        rustix::fs::openat2(
            parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            RESOLVE,
        )
    }

    pub(super) fn ensure_absent(parent: &OwnedFd, name: &str, label: &str) -> Result<()> {
        match rustix::fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
            Err(Errno::NOENT) => Ok(()),
            Ok(_) => storage_error(format!(
                "{label} '{name}' already exists; overwrite refused"
            )),
            Err(error) => storage_error(format!("cannot inspect {label} '{name}': {error}")),
        }
    }

    pub(super) fn open_existing(parent: &OwnedFd, name: &str) -> Result<Option<File>> {
        match rustix::fs::openat2(
            parent,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            RESOLVE,
        ) {
            Ok(fd) => Ok(Some(File::from(fd))),
            Err(Errno::NOENT) => Ok(None),
            Err(error) => storage_error(format!("cannot safely open '{name}': {error}")),
        }
    }

    pub(super) fn exact_sidecar(
        parent: &OwnedFd,
        name: &str,
        expected_body: &Value,
    ) -> Result<bool> {
        if !expected_body.is_object() {
            return storage_error("expected metadata sidecar body must be a JSON object");
        }
        let Some(mut file) = open_existing(parent, name)? else {
            return Ok(false);
        };
        let metadata = file.metadata().map_err(|error| {
            CameraError::Storage(format!("cannot inspect metadata sidecar '{name}': {error}"))
        })?;
        if !metadata.is_file() || metadata.len() > MAX_SIDECAR_BYTES {
            return storage_error(format!(
                "metadata sidecar '{name}' is not a bounded regular file"
            ));
        }

        let mut expected = serde_json::to_vec(expected_body)?;
        expected.push(b'\n');
        if u64::try_from(expected.len()).unwrap_or(u64::MAX) > MAX_SIDECAR_BYTES {
            return storage_error("expected metadata sidecar exceeds one MiB");
        }
        let mut actual = Vec::with_capacity(metadata.len() as usize);
        file.read_to_end(&mut actual).map_err(|error| {
            CameraError::Storage(format!("cannot read metadata sidecar '{name}': {error}"))
        })?;
        let actual_value: Value = serde_json::from_slice(&actual).map_err(|error| {
            CameraError::Storage(format!(
                "metadata sidecar '{name}' is invalid JSON: {error}"
            ))
        })?;
        if !actual_value.is_object() {
            return storage_error(format!(
                "metadata sidecar '{name}' must contain a JSON object"
            ));
        }
        if actual != expected {
            return storage_error(format!(
                "metadata sidecar '{name}' does not match the exact durable terminal body"
            ));
        }
        Ok(true)
    }

    pub(super) fn remove_file(parent: &OwnedFd, name: &str) -> Result<()> {
        match rustix::fs::unlinkat(parent, name, AtFlags::empty()) {
            Ok(()) | Err(Errno::NOENT) => Ok(()),
            Err(error) => storage_error(format!("cannot remove '{name}': {error}")),
        }
    }

    struct SidecarWriter<'a> {
        file: &'a mut File,
        written: u64,
        cancellation: &'a CancellationToken,
    }

    impl std::io::Write for SidecarWriter<'_> {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            if self.cancellation.is_cancelled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "sidecar write cancelled",
                ));
            }
            let next = self
                .written
                .checked_add(buffer.len() as u64)
                .filter(|bytes| *bytes <= MAX_SIDECAR_BYTES)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::FileTooLarge,
                        "sidecar exceeded one MiB",
                    )
                })?;
            let written = self.file.write(buffer)?;
            self.written = next - (buffer.len() - written) as u64;
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }
}

/// Portable Windows output backend.
///
/// This backend uses only `std::fs` because the accepted Windows profile is intentionally
/// best-effort. It refuses ordinary pre-existing destinations and symlinks it observes, but it
/// cannot provide Linux's handle-relative containment proof against a hostile local actor.
/// Deployment ownership and ACL guidance remain the boundary for that case.
#[cfg(windows)]
mod portable {
    use std::fs::{self, File, OpenOptions};
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use serde_json::Value;
    use tokio_util::sync::CancellationToken;

    use super::{InstallObserver, MAX_SIDECAR_BYTES, cancelled, storage_error};
    use crate::{CameraError, Result};

    pub(super) struct RootCapability {
        canonical_root: PathBuf,
        observer: Arc<dyn InstallObserver>,
    }

    impl RootCapability {
        pub(super) fn open(
            canonical_root: &Path,
            observer: Arc<dyn InstallObserver>,
        ) -> Result<Self> {
            Ok(Self {
                canonical_root: canonical_root.to_path_buf(),
                observer,
            })
        }

        pub(super) fn open_parent(&self, components: &[String], create: bool) -> Result<PathBuf> {
            let mut current = self.canonical_root.clone();
            for component in components {
                let next = current.join(component);
                match fs::symlink_metadata(&next) {
                    Ok(metadata) => validate_directory_component(&next, &metadata)?,
                    Err(error) if create && error.kind() == std::io::ErrorKind::NotFound => {
                        fs::create_dir(&next).map_err(|create_error| {
                            CameraError::Storage(format!(
                                "cannot create output directory '{}': {create_error}",
                                component
                            ))
                        })?;
                        let metadata = fs::symlink_metadata(&next).map_err(|inspect_error| {
                            CameraError::Storage(format!(
                                "cannot inspect created output directory '{}': {inspect_error}",
                                component
                            ))
                        })?;
                        validate_directory_component(&next, &metadata)?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        return storage_error(format!(
                            "output directory '{}' does not exist",
                            component
                        ));
                    }
                    Err(error) => {
                        return storage_error(format!(
                            "cannot inspect output directory '{}': {error}",
                            component
                        ));
                    }
                }
                current = fs::canonicalize(&next).map_err(|error| {
                    CameraError::Storage(format!(
                        "cannot canonicalize output directory '{}': {error}",
                        component
                    ))
                })?;
                if !current.starts_with(&self.canonical_root) {
                    return storage_error("output path escaped the configured root");
                }
            }
            Ok(current)
        }

        pub(super) fn create_partial(&self, parent: &Path, name: &str) -> Result<File> {
            let partial = parent.join(name);
            self.observer.before_partial_open(&partial);
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&partial)
                .map_err(|error| {
                    CameraError::Storage(format!(
                        "cannot exclusively create partial '{name}': {error}"
                    ))
                })
        }

        pub(super) fn install_best_effort(
            &self,
            parent: &Path,
            partial: &str,
            final_name: &str,
        ) -> Result<()> {
            ensure_absent(parent, final_name, "final image")?;
            // The gap this observation point names is the one the portable profile cannot close:
            // between the absence proof above and the link below, a local actor can create the
            // destination. `complete_no_overwrite` is what has to refuse it.
            self.observer.before_install_link(&parent.join(final_name));
            complete_no_overwrite(parent, partial, final_name, "final image")
        }
    }

    fn validate_directory_component(path: &Path, metadata: &fs::Metadata) -> Result<()> {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return storage_error(format!(
                "output path component '{}' is not a regular directory",
                path.display()
            ));
        }
        Ok(())
    }

    pub(super) fn write_sidecar(
        parent: &Path,
        partial: &str,
        final_name: &str,
        body: &Value,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(parent.join(partial))
            .map_err(|error| {
                CameraError::Storage(format!(
                    "cannot exclusively create sidecar partial '{partial}': {error}"
                ))
            })?;
        let write_result = (|| -> Result<()> {
            let mut writer = SidecarWriter {
                file: &mut file,
                written: 0,
                cancellation,
            };
            serde_json::to_writer(&mut writer, body)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
            Ok(())
        })();
        if let Err(error) = write_result {
            drop(file);
            let _ = remove_file(parent, partial);
            return Err(error);
        }
        file.sync_all().map_err(|error| {
            CameraError::Storage(format!("failed to durably flush sidecar partial: {error}"))
        })?;
        drop(file);
        cancelled(cancellation, "before sidecar installation")?;
        install_no_overwrite(parent, partial, final_name, "metadata sidecar")
    }

    fn install_no_overwrite(
        parent: &Path,
        partial: &str,
        final_name: &str,
        label: &str,
    ) -> Result<()> {
        ensure_absent(parent, final_name, label)?;
        complete_no_overwrite(parent, partial, final_name, label)
    }

    fn complete_no_overwrite(
        parent: &Path,
        partial: &str,
        final_name: &str,
        label: &str,
    ) -> Result<()> {
        fs::hard_link(parent.join(partial), parent.join(final_name)).map_err(|error| {
            CameraError::Storage(format!(
                "Windows {label} collision or no-overwrite install failed for '{final_name}': {error}"
            ))
        })?;
        remove_file(parent, partial).map_err(|error| {
            CameraError::Storage(format!(
                "Windows {label} '{final_name}' was installed but its partial cleanup failed: {error}"
            ))
        })
    }

    pub(super) fn sync_dir(_parent: &Path) -> Result<()> {
        // Windows does not offer a portable directory metadata flush through `std::fs`. Data files
        // are flushed before finalization; catalog-led reconciliation covers the remaining entry
        // window.
        Ok(())
    }

    pub(super) fn ensure_absent(parent: &Path, name: &str, label: &str) -> Result<()> {
        let path = parent.join(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                storage_error(format!("{label} '{name}' is a symlink; overwrite refused"))
            }
            Ok(_) => storage_error(format!(
                "{label} '{name}' already exists; overwrite refused"
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => storage_error(format!("cannot inspect {label} '{name}': {error}")),
        }
    }

    pub(super) fn open_existing(parent: &Path, name: &str) -> Result<Option<File>> {
        let path = parent.join(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                storage_error(format!("cannot safely open '{name}': not a regular file"))
            }
            Ok(_) => File::open(&path)
                .map(Some)
                .map_err(|error| CameraError::Storage(format!("cannot open '{name}': {error}"))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => storage_error(format!("cannot inspect '{name}': {error}")),
        }
    }

    pub(super) fn exact_sidecar(parent: &Path, name: &str, expected_body: &Value) -> Result<bool> {
        if !expected_body.is_object() {
            return storage_error("expected metadata sidecar body must be a JSON object");
        }
        let Some(mut file) = open_existing(parent, name)? else {
            return Ok(false);
        };
        let metadata = file.metadata().map_err(|error| {
            CameraError::Storage(format!("cannot inspect metadata sidecar '{name}': {error}"))
        })?;
        if !metadata.is_file() || metadata.len() > MAX_SIDECAR_BYTES {
            return storage_error(format!(
                "metadata sidecar '{name}' is not a bounded regular file"
            ));
        }
        let mut expected = serde_json::to_vec(expected_body)?;
        expected.push(b'\n');
        let expected_len = u64::try_from(expected.len()).map_err(|_| {
            CameraError::Storage("expected metadata sidecar length overflow".into())
        })?;
        if expected_len > MAX_SIDECAR_BYTES {
            return storage_error("expected metadata sidecar exceeds one MiB");
        }
        let mut actual = Vec::with_capacity(metadata.len() as usize);
        file.read_to_end(&mut actual).map_err(|error| {
            CameraError::Storage(format!("cannot read metadata sidecar '{name}': {error}"))
        })?;
        let actual_value: Value = serde_json::from_slice(&actual).map_err(|error| {
            CameraError::Storage(format!(
                "metadata sidecar '{name}' is invalid JSON: {error}"
            ))
        })?;
        if !actual_value.is_object() {
            return storage_error(format!(
                "metadata sidecar '{name}' must contain a JSON object"
            ));
        }
        if actual != expected {
            return storage_error(format!(
                "metadata sidecar '{name}' does not match the exact durable terminal body"
            ));
        }
        Ok(true)
    }

    pub(super) fn remove_file(parent: &Path, name: &str) -> Result<()> {
        let path = parent.join(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                storage_error(format!("cannot remove '{name}': not a regular file"))
            }
            Ok(_) => fs::remove_file(&path)
                .map_err(|error| CameraError::Storage(format!("cannot remove '{name}': {error}"))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => storage_error(format!("cannot inspect '{name}' for removal: {error}")),
        }
    }

    struct SidecarWriter<'a> {
        file: &'a mut File,
        written: u64,
        cancellation: &'a CancellationToken,
    }

    impl Write for SidecarWriter<'_> {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            if self.cancellation.is_cancelled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "sidecar write cancelled",
                ));
            }
            let next = self
                .written
                .checked_add(buffer.len() as u64)
                .filter(|bytes| *bytes <= MAX_SIDECAR_BYTES)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::FileTooLarge,
                        "sidecar exceeded one MiB",
                    )
                })?;
            let written = self.file.write(buffer)?;
            self.written = next - (buffer.len() - written) as u64;
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(any(target_os = "linux", windows))]
    use std::collections::BTreeMap;
    #[cfg(target_os = "linux")]
    use std::sync::Mutex;

    #[cfg(any(target_os = "linux", windows))]
    use bytes::Bytes;
    use chrono::TimeZone;

    use super::*;
    #[cfg(any(target_os = "linux", windows))]
    use crate::model::{CaptureMode, FrameTimestampQuality, PixelFormat};

    fn output(root: &Path, sidecar: bool) -> OutputConfig {
        OutputConfig {
            root_directory: root.to_string_lossy().into_owned(),
            camera_directory_template: "{cameraId}/{yyyy}/{MM}/{dd}".to_string(),
            file_name_template: "{timestamp}-{captureId}.{extension}".to_string(),
            write_metadata_sidecar: sidecar,
            minimum_free_bytes: 0,
            minimum_free_percent: 0,
            directory_mode: "0700".to_string(),
            file_mode: "0600".to_string(),
        }
    }

    fn variables<'a>(camera_id: &'a str, capture_id: &'a str) -> OutputPathVariables<'a> {
        OutputPathVariables {
            camera_id,
            capture_id,
            timestamp: Utc.with_ymd_and_hms(2026, 7, 10, 12, 34, 56).unwrap(),
        }
    }

    #[cfg(any(target_os = "linux", windows))]
    #[allow(dead_code)]
    fn raw_frame() -> CaptureFrame {
        CaptureFrame {
            bytes: Bytes::from_static(&[1, 2, 3, 4]),
            width: 2,
            height: 2,
            pixel_format: PixelFormat::Mono8,
            capture_mode: CaptureMode::Simulated,
            source_timestamp: None,
            timestamp_quality: FrameTimestampQuality::AdapterReceive,
            backend_metadata: BTreeMap::new(),
        }
    }

    #[cfg(any(target_os = "linux", windows))]
    #[allow(dead_code)]
    fn prepare_request(output: &OutputConfig, capture_id: &str, camera_id: &str) -> PrepareCapture {
        PrepareCapture {
            capture_id: capture_id.to_string(),
            relative_path: render_output_path(
                output,
                variables(camera_id, capture_id),
                OutputEncoding::Raw,
            )
            .unwrap(),
            frame: raw_frame(),
            encoding: EncodingRequest {
                encoding: OutputEncoding::Raw,
                jpeg_quality: 90,
                maximum_output_bytes: 1024,
            },
            reservation: StorageReservation {
                current_bytes: 1024,
                other_bytes: 0,
            },
        }
    }

    #[test]
    fn templates_are_sanitized_relative_and_extension_locked() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = output(temp.path(), false);
        let rendered = render_output_path(
            &config,
            variables("cam/../../one", "cap:one"),
            OutputEncoding::Png,
        )
        .unwrap();
        assert_eq!(
            rendered.as_wire_path(),
            "cam_______one/2026/07/10/20260710T123456.000Z-cap_one.png"
        );

        config.camera_directory_template = "../escape".to_string();
        assert!(render_output_path(&config, variables("cam", "cap"), OutputEncoding::Raw).is_err());
        config.camera_directory_template = "safe".to_string();
        config.file_name_template = "CON.raw".to_string();
        assert!(render_output_path(&config, variables("cam", "cap"), OutputEncoding::Raw).is_err());
        config.file_name_template = "capture.jpg".to_string();
        assert!(render_output_path(&config, variables("cam", "cap"), OutputEncoding::Raw).is_err());
    }

    #[test]
    fn stored_paths_and_template_edges_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = output(temp.path(), false);

        for path in ["", "/absolute.raw", "../escape.raw", "nested/CON.raw"] {
            assert!(
                RelativeOutputPath::from_stored(path).is_err(),
                "stored path '{path}' must be rejected"
            );
        }
        let oversized = format!("nested/{}.raw", "x".repeat(MAX_COMPONENT_BYTES + 1));
        assert!(RelativeOutputPath::from_stored(&oversized).is_err());
        assert!(RelativeOutputPath::from_stored("nested/capture.raw").is_ok());

        config.camera_directory_template.clear();
        config.file_name_template = "{captureId}.{extension}".to_string();
        let root_file = render_output_path(&config, variables("cam", "cap"), OutputEncoding::Tiff)
            .expect("empty directory template is a root-relative filename");
        assert_eq!(root_file.as_wire_path(), "cap.tiff");

        config.file_name_template = "{unresolved}.raw".to_string();
        assert!(render_output_path(&config, variables("cam", "cap"), OutputEncoding::Raw).is_err());
        config.file_name_template = "nested/{captureId}.raw".to_string();
        assert!(render_output_path(&config, variables("cam", "cap"), OutputEncoding::Raw).is_err());
    }

    #[cfg(any(target_os = "linux", windows))]
    #[test]
    fn root_pressure_and_pre_persistence_rejections_are_typed() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = output(temp.path(), false);
        let root = StorageRoot::open(&config).expect("output root");
        assert_eq!(
            root.canonical_root(),
            std::fs::canonicalize(temp.path()).unwrap()
        );
        assert!(format!("{root:?}").contains("StorageRoot"));

        let overflow = root
            .check_storage_pressure(StorageReservation {
                current_bytes: u64::MAX,
                other_bytes: 1,
            })
            .expect_err("reservation arithmetic must not wrap");
        assert_eq!(overflow.code(), ErrorCode::ResourceLimit);

        config.minimum_free_bytes = u64::MAX;
        let pressured = StorageRoot::open(&config)
            .expect("same output root")
            .check_storage_pressure(StorageReservation::default())
            .expect_err("impossible free-space floor must reject admission");
        assert_eq!(pressured.code(), ErrorCode::StoragePressure);

        let mut too_small = prepare_request(&output(temp.path(), false), "cap-ceiling", "cam");
        too_small.encoding.maximum_output_bytes = too_small.reservation.current_bytes + 1;
        let error = root
            .prepare_capture(too_small, &CancellationToken::new())
            .expect_err("unreserved encoding ceiling must be rejected");
        assert_eq!(error.code(), ErrorCode::ResourceLimit);

        let mut source_exceeds = prepare_request(&output(temp.path(), false), "cap-source", "cam");
        source_exceeds.reservation.current_bytes = 3;
        source_exceeds.encoding.maximum_output_bytes = 3;
        let error = root
            .prepare_capture(source_exceeds, &CancellationToken::new())
            .expect_err("source bytes must fit the accepted reservation");
        assert_eq!(error.code(), ErrorCode::ResourceLimit);

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let error = root
            .prepare_capture(
                prepare_request(&output(temp.path(), false), "cap-cancelled", "cam"),
                &cancelled,
            )
            .expect_err("cancelled persistence must not create a partial");
        assert_eq!(error.code(), ErrorCode::CaptureCancelled);

        let mut invalid_encoding =
            prepare_request(&output(temp.path(), false), "cap-encoding", "cam");
        invalid_encoding.encoding.jpeg_quality = 0;
        let error = root
            .prepare_capture(invalid_encoding, &CancellationToken::new())
            .expect_err("invalid encoder settings must remove the exclusive partial");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);

        let relative = "cam/2026/07/10/20260710T123456.000Z-cap-encoding.raw";
        assert!(!temp.path().join(relative).exists());
    }

    #[cfg(not(any(target_os = "linux", windows)))]
    #[test]
    fn unsupported_platform_rejects_root_instead_of_using_path_string_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let error = StorageRoot::open(&output(temp.path(), false)).unwrap_err();
        assert!(error.to_string().contains("no enabled handle-relative"));
    }

    #[cfg(target_os = "linux")]
    mod linux_tests {
        use std::fs;
        use std::os::unix::fs::{MetadataExt, symlink};
        use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

        use super::*;

        struct Gate {
            decision: AtomicU8,
            calls: AtomicUsize,
            sidecar: Mutex<Option<PathBuf>>,
            image: Mutex<Option<PathBuf>>,
            cancel_on_call: Mutex<Option<CancellationToken>>,
        }

        impl Gate {
            fn new(decision: InstallDecision) -> Self {
                Self {
                    decision: AtomicU8::new(decision as u8),
                    calls: AtomicUsize::new(0),
                    sidecar: Mutex::new(None),
                    image: Mutex::new(None),
                    cancel_on_call: Mutex::new(None),
                }
            }

            fn inspect_order(&self, sidecar: PathBuf, image: PathBuf) {
                *self.sidecar.lock().unwrap() = Some(sidecar);
                *self.image.lock().unwrap() = Some(image);
            }
        }

        #[async_trait]
        impl InstallGate for Gate {
            async fn begin_install(
                &self,
                _capture_id: &str,
                _changed_at_ms: i64,
            ) -> Result<InstallDecision> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if let Some(sidecar) = self.sidecar.lock().unwrap().as_ref() {
                    assert!(
                        sidecar.exists(),
                        "sidecar must be visible before catalog CAS"
                    );
                }
                if let Some(image) = self.image.lock().unwrap().as_ref() {
                    assert!(
                        !image.exists(),
                        "image must not be visible before catalog CAS"
                    );
                }
                if let Some(cancellation) = self.cancel_on_call.lock().unwrap().take() {
                    cancellation.cancel();
                }
                Ok(match self.decision.load(Ordering::SeqCst) {
                    value if value == InstallDecision::Started as u8 => InstallDecision::Started,
                    value if value == InstallDecision::AlreadyStarted as u8 => {
                        InstallDecision::AlreadyStarted
                    }
                    _ => InstallDecision::Rejected,
                })
            }
        }

        /// Runs one filesystem race at one of the capability's install-window observation points.
        ///
        /// The race fires once and once only: a second swap on a later install would be a different
        /// experiment than the one the test claims to run.
        struct RaceObserver {
            partial_open: Mutex<Option<Box<dyn FnOnce() + Send>>>,
            install_link: Mutex<Option<Box<dyn FnOnce() + Send>>>,
        }

        impl RaceObserver {
            fn on_partial_open(race: impl FnOnce() + Send + 'static) -> Arc<Self> {
                Arc::new(Self {
                    partial_open: Mutex::new(Some(Box::new(race))),
                    install_link: Mutex::new(None),
                })
            }

            fn on_install_link(race: impl FnOnce() + Send + 'static) -> Arc<Self> {
                Arc::new(Self {
                    partial_open: Mutex::new(None),
                    install_link: Mutex::new(Some(Box::new(race))),
                })
            }
        }

        impl InstallObserver for RaceObserver {
            fn before_partial_open(&self, _partial: &Path) {
                if let Some(race) = self.partial_open.lock().unwrap().take() {
                    race();
                }
            }

            fn before_install_link(&self, _destination: &Path) {
                if let Some(race) = self.install_link.lock().unwrap().take() {
                    race();
                }
            }
        }

        fn partials(root: &Path) -> Vec<PathBuf> {
            fn visit(path: &Path, found: &mut Vec<PathBuf>) {
                for entry in fs::read_dir(path).unwrap() {
                    let entry = entry.unwrap();
                    let path = entry.path();
                    if path.is_dir() {
                        visit(&path, found);
                    } else if path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with(PARTIAL_PREFIX))
                    {
                        found.push(path);
                    }
                }
            }
            let mut found = Vec::new();
            visit(root, &mut found);
            found
        }

        #[tokio::test]
        async fn sidecar_is_durable_and_visible_before_cas_then_image_installs() {
            let temp = tempfile::tempdir().unwrap();
            let config = output(temp.path(), true);
            let root = StorageRoot::open(&config).unwrap();
            let cancellation = CancellationToken::new();
            let prepared = root
                .prepare_capture(prepare_request(&config, "cap-1", "cam-1"), &cancellation)
                .unwrap();
            let image_path = PathBuf::from(&prepared.artifact().absolute_path);
            let sidecar_path = image_path.with_file_name(format!(
                "{}.json",
                image_path.file_name().unwrap().to_string_lossy()
            ));
            let gate = Gate::new(InstallDecision::Started);
            gate.inspect_order(sidecar_path.clone(), image_path.clone());
            let body = serde_json::json!({
                "captureId": "cap-1",
                "image": prepared.artifact(),
            });

            let installed = prepared
                .install(&gate, 1, Some(&body), &cancellation)
                .await
                .unwrap();

            assert_eq!(fs::read(&image_path).unwrap(), [1, 2, 3, 4]);
            assert_eq!(installed.bytes, 4);
            assert!(sidecar_path.exists());
            let sidecar: Value = serde_json::from_slice(&fs::read(sidecar_path).unwrap()).unwrap();
            assert_eq!(sidecar["captureId"], "cap-1");
            assert!(partials(temp.path()).is_empty());
            assert_eq!(gate.calls.load(Ordering::SeqCst), 1);

            let parent_mode = image_path.parent().unwrap().metadata().unwrap().mode() & 0o777;
            let file_mode = image_path.metadata().unwrap().mode() & 0o777;
            assert_eq!(
                parent_mode & !0o700,
                0,
                "umask may remove but never add mode bits"
            );
            assert_eq!(file_mode & !0o600, 0, "image mode must not exceed 0600");
            assert!(image_path.metadata().unwrap().nlink() >= 1);
        }

        #[tokio::test]
        async fn cancellation_before_or_at_gate_cleans_owned_artifacts() {
            let temp = tempfile::tempdir().unwrap();
            let config = output(temp.path(), true);
            let root = StorageRoot::open(&config).unwrap();

            let cancellation = CancellationToken::new();
            let prepared = root
                .prepare_capture(prepare_request(&config, "cap-before", "cam"), &cancellation)
                .unwrap();
            cancellation.cancel();
            let error = prepared
                .install(
                    &Gate::new(InstallDecision::Started),
                    1,
                    Some(&serde_json::json!({})),
                    &cancellation,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), ErrorCode::CaptureCancelled);
            assert!(partials(temp.path()).is_empty());

            let cancellation = CancellationToken::new();
            let prepared = root
                .prepare_capture(prepare_request(&config, "cap-gate", "cam"), &cancellation)
                .unwrap();
            let image = PathBuf::from(&prepared.artifact().absolute_path);
            let sidecar = image.with_file_name(format!(
                "{}.json",
                image.file_name().unwrap().to_string_lossy()
            ));
            let error = prepared
                .install(
                    &Gate::new(InstallDecision::Rejected),
                    1,
                    Some(&serde_json::json!({})),
                    &cancellation,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), ErrorCode::CaptureCancelled);
            assert!(!image.exists());
            assert!(!sidecar.exists());
            assert!(partials(temp.path()).is_empty());
        }

        #[tokio::test]
        async fn cancellation_after_catalog_cas_cannot_cancel_installation() {
            let temp = tempfile::tempdir().unwrap();
            let config = output(temp.path(), false);
            let root = StorageRoot::open(&config).unwrap();
            let cancellation = CancellationToken::new();
            let prepared = root
                .prepare_capture(prepare_request(&config, "cap-cas", "cam"), &cancellation)
                .unwrap();
            let gate = Gate::new(InstallDecision::Started);
            *gate.cancel_on_call.lock().unwrap() = Some(cancellation.clone());
            let artifact = prepared
                .install(&gate, 1, None, &cancellation)
                .await
                .unwrap();
            assert!(Path::new(&artifact.absolute_path).exists());
        }

        #[tokio::test]
        async fn existing_target_and_partial_are_never_overwritten() {
            let temp = tempfile::tempdir().unwrap();
            let config = output(temp.path(), false);
            let root = StorageRoot::open(&config).unwrap();
            let cancellation = CancellationToken::new();
            let prepared = root
                .prepare_capture(prepare_request(&config, "cap-race", "cam"), &cancellation)
                .unwrap();
            let final_path = PathBuf::from(&prepared.artifact().absolute_path);
            fs::write(&final_path, b"pre-existing").unwrap();
            assert!(
                prepared
                    .install(&Gate::new(InstallDecision::Started), 1, None, &cancellation,)
                    .await
                    .is_err()
            );
            assert_eq!(fs::read(&final_path).unwrap(), b"pre-existing");

            let first = root
                .prepare_capture(
                    prepare_request(&config, "cap-exclusive", "other"),
                    &cancellation,
                )
                .unwrap();
            assert!(
                root.prepare_capture(
                    prepare_request(&config, "cap-exclusive", "other"),
                    &cancellation,
                )
                .is_err()
            );
            drop(first);
            assert!(partials(temp.path()).iter().all(|path| {
                !path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains("cap-exclusive")
            }));

            let sidecar_config = output(temp.path(), true);
            let sidecar_root = StorageRoot::open(&sidecar_config).unwrap();
            let sidecar_prepared = sidecar_root
                .prepare_capture(
                    prepare_request(&sidecar_config, "cap-sidecar", "sidecar-cam"),
                    &cancellation,
                )
                .unwrap();
            let sidecar_image = PathBuf::from(&sidecar_prepared.artifact().absolute_path);
            let sidecar_path = sidecar_image.with_file_name(format!(
                "{}.json",
                sidecar_image.file_name().unwrap().to_string_lossy()
            ));
            fs::write(&sidecar_path, b"pre-existing-sidecar").unwrap();
            let gate = Gate::new(InstallDecision::Started);
            assert!(
                sidecar_prepared
                    .install(&gate, 1, Some(&serde_json::json!({})), &cancellation,)
                    .await
                    .is_err()
            );
            assert_eq!(fs::read(&sidecar_path).unwrap(), b"pre-existing-sidecar");
            assert!(!sidecar_image.exists());
            assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn symlink_traversal_and_symlink_target_are_rejected() {
            let root_dir = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            let mut config = output(root_dir.path(), false);
            config.camera_directory_template = "escape".to_string();
            symlink(outside.path(), root_dir.path().join("escape")).unwrap();
            let root = StorageRoot::open(&config).unwrap();
            let cancellation = CancellationToken::new();
            assert!(
                root.prepare_capture(prepare_request(&config, "cap-link", "cam"), &cancellation,)
                    .is_err()
            );
            assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);

            fs::remove_file(root_dir.path().join("escape")).unwrap();
            fs::create_dir(root_dir.path().join("escape")).unwrap();
            let relative =
                render_output_path(&config, variables("cam", "cap-target"), OutputEncoding::Raw)
                    .unwrap();
            let target = outside.path().join("target");
            fs::write(&target, b"outside").unwrap();
            symlink(&target, root_dir.path().join(relative.as_path())).unwrap();
            assert!(
                root.prepare_capture(prepare_request(&config, "cap-target", "cam"), &cancellation,)
                    .is_err()
            );
            assert_eq!(fs::read(target).unwrap(), b"outside");
        }

        #[test]
        fn parent_swap_race_is_re_resolved_from_the_root_capability() {
            let root_dir = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            let mut config = output(root_dir.path(), false);
            config.camera_directory_template = "camera".to_string();
            fs::create_dir(root_dir.path().join("camera")).unwrap();
            let original = root_dir.path().join("camera");
            let moved = outside.path().join("moved-parent");
            let replacement_target = outside.path().to_path_buf();
            let root = StorageRoot::open_with_observer(
                &config,
                RaceObserver::on_partial_open(move || {
                    fs::rename(&original, &moved).unwrap();
                    symlink(&replacement_target, &original).unwrap();
                }),
            )
            .unwrap();

            let error = root
                .prepare_capture(
                    prepare_request(&config, "cap-swap", "cam"),
                    &CancellationToken::new(),
                )
                .expect_err("swapped parent must fail closed");

            assert!(error.to_string().contains("partial"));
            assert!(partials(outside.path()).is_empty());
        }

        #[tokio::test]
        async fn parent_move_during_final_link_rolls_back_external_visibility() {
            let root_dir = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            let mut config = output(root_dir.path(), false);
            config.camera_directory_template = "camera".to_string();
            let original = root_dir.path().join("camera");
            let moved = outside.path().join("moved-parent");
            let original_for_race = original.clone();
            let moved_for_race = moved.clone();
            let root = StorageRoot::open_with_observer(
                &config,
                RaceObserver::on_install_link(move || {
                    fs::rename(&original_for_race, &moved_for_race).unwrap();
                    fs::create_dir(&original_for_race).unwrap();
                }),
            )
            .unwrap();
            let cancellation = CancellationToken::new();
            let prepared = root
                .prepare_capture(
                    prepare_request(&config, "cap-install-race", "cam"),
                    &cancellation,
                )
                .unwrap();
            let final_name = PathBuf::from(&prepared.artifact().absolute_path)
                .file_name()
                .unwrap()
                .to_owned();

            let error = prepared
                .install(&Gate::new(InstallDecision::Started), 1, None, &cancellation)
                .await
                .expect_err("moved parent must fail closed after rolling back the final link");

            assert!(error.to_string().contains("parent moved"));
            assert!(!moved.join(&final_name).exists());
            assert!(!original.join(&final_name).exists());
        }

        #[test]
        fn source_frame_must_fit_the_accepted_reservation() {
            let temp = tempfile::tempdir().unwrap();
            let config = output(temp.path(), false);
            let root = StorageRoot::open(&config).unwrap();
            let mut request = prepare_request(&config, "cap-source-limit", "cam");
            request.encoding.maximum_output_bytes = 3;
            request.reservation.current_bytes = 3;

            let error = root
                .prepare_capture(request, &CancellationToken::new())
                .expect_err("oversized source frame must be rejected before persistence");

            assert_eq!(error.code(), ErrorCode::ResourceLimit);
            assert!(partials(temp.path()).is_empty());
        }

        #[test]
        fn configured_modes_cannot_exceed_the_design_ceiling() {
            let temp = tempfile::tempdir().unwrap();
            let mut config = output(temp.path(), false);
            config.directory_mode = "0770".to_string();
            assert!(StorageRoot::open(&config).is_err());

            config.directory_mode = "0750".to_string();
            config.file_mode = "0660".to_string();
            assert!(StorageRoot::open(&config).is_err());
        }

        #[test]
        fn install_started_reconciliation_installs_verified_partial_or_cleans_orphan_sidecar() {
            let temp = tempfile::tempdir().unwrap();
            let config = output(temp.path(), false);
            let root = StorageRoot::open(&config).unwrap();
            let cancellation = CancellationToken::new();
            let mut prepared = root
                .prepare_capture(
                    prepare_request(&config, "cap-recover", "cam"),
                    &cancellation,
                )
                .unwrap();
            let relative = render_output_path(
                &config,
                variables("cam", "cap-recover"),
                OutputEncoding::Raw,
            )
            .unwrap();
            let expected_bytes = prepared.artifact().bytes;
            let expected_sha256 = prepared.artifact().sha256.clone();
            let final_path = PathBuf::from(&prepared.artifact().absolute_path);
            let installed_relative = relative.clone();
            prepared.installation_started = true;
            std::mem::forget(prepared);

            let outcome = root
                .reconcile_install_started(
                    RecoveryRequest {
                        capture_id: "cap-recover".to_string(),
                        relative_path: relative,
                        expected_bytes,
                        expected_sha256: expected_sha256.clone(),
                        sidecar_body: None,
                    },
                    &cancellation,
                )
                .unwrap();
            assert_eq!(outcome, RecoveryOutcome::InstalledFromPartial);
            assert_eq!(fs::read(&final_path).unwrap(), [1, 2, 3, 4]);

            let invalid_sidecar = final_path.with_file_name(format!(
                "{}.json",
                final_path.file_name().unwrap().to_string_lossy()
            ));
            fs::write(&invalid_sidecar, b"[]").unwrap();
            let error = root
                .reconcile_install_started(
                    RecoveryRequest {
                        capture_id: "cap-recover".to_string(),
                        relative_path: installed_relative,
                        expected_bytes,
                        expected_sha256: expected_sha256.clone(),
                        sidecar_body: Some(serde_json::json!({"captureId": "cap-recover"})),
                    },
                    &cancellation,
                )
                .expect_err("a non-object sidecar must not satisfy recovery");
            assert!(error.to_string().contains("JSON object"));
            fs::remove_file(invalid_sidecar).unwrap();

            let mut sidecar_config = output(temp.path(), true);
            sidecar_config.camera_directory_template = "orphan".to_string();
            let relative = render_output_path(
                &sidecar_config,
                variables("cam", "cap-orphan"),
                OutputEncoding::Raw,
            )
            .unwrap();
            let final_path = temp.path().join(relative.as_path());
            fs::create_dir_all(final_path.parent().unwrap()).unwrap();
            let sidecar = final_path.with_file_name(format!(
                "{}.json",
                final_path.file_name().unwrap().to_string_lossy()
            ));
            let sidecar_body = serde_json::json!({"captureId": "cap-orphan"});
            let mut sidecar_bytes = serde_json::to_vec(&sidecar_body).unwrap();
            sidecar_bytes.push(b'\n');
            fs::write(&sidecar, sidecar_bytes).unwrap();
            let root = StorageRoot::open(&sidecar_config).unwrap();
            let outcome = root
                .reconcile_install_started(
                    RecoveryRequest {
                        capture_id: "cap-orphan".to_string(),
                        relative_path: relative,
                        expected_bytes: 4,
                        expected_sha256: "00".repeat(32),
                        sidecar_body: Some(sidecar_body),
                    },
                    &cancellation,
                )
                .unwrap();
            assert_eq!(outcome, RecoveryOutcome::MissingArtifactsCleaned);
            assert!(!sidecar.exists());
        }

        #[test]
        fn recovery_never_claims_or_deletes_sidecar_when_job_disabled_sidecars() {
            let temp = tempfile::tempdir().unwrap();
            let config = output(temp.path(), false);
            let root = StorageRoot::open(&config).unwrap();
            let cancellation = CancellationToken::new();

            let mut prepared = root
                .prepare_capture(
                    prepare_request(&config, "cap-foreign-present", "cam-present"),
                    &cancellation,
                )
                .unwrap();
            let relative = render_output_path(
                &config,
                variables("cam-present", "cap-foreign-present"),
                OutputEncoding::Raw,
            )
            .unwrap();
            let partial_path = prepared.partial_path().to_owned();
            let final_path = PathBuf::from(&prepared.artifact().absolute_path);
            let sidecar_path = final_path.with_file_name(format!(
                "{}.json",
                final_path.file_name().unwrap().to_string_lossy()
            ));
            fs::write(&sidecar_path, b"{\"foreign\":true}\n").unwrap();
            let expected_bytes = prepared.artifact().bytes;
            let expected_sha256 = prepared.artifact().sha256.clone();
            prepared.installation_started = true;
            std::mem::forget(prepared);

            let error = root
                .reconcile_install_started(
                    RecoveryRequest {
                        capture_id: "cap-foreign-present".to_string(),
                        relative_path: relative,
                        expected_bytes,
                        expected_sha256,
                        sidecar_body: None,
                    },
                    &cancellation,
                )
                .expect_err("foreign sidecar must block partial recovery");
            assert!(error.to_string().contains("unexpected metadata sidecar"));
            assert_eq!(fs::read(&sidecar_path).unwrap(), b"{\"foreign\":true}\n");
            assert!(Path::new(&partial_path).exists());
            assert!(!final_path.exists());

            let missing_relative = render_output_path(
                &config,
                variables("cam-missing", "cap-foreign-missing"),
                OutputEncoding::Raw,
            )
            .unwrap();
            let missing_final = temp.path().join(missing_relative.as_path());
            fs::create_dir_all(missing_final.parent().unwrap()).unwrap();
            let missing_sidecar = missing_final.with_file_name(format!(
                "{}.json",
                missing_final.file_name().unwrap().to_string_lossy()
            ));
            fs::write(&missing_sidecar, b"{\"foreign\":true}\n").unwrap();
            let error = root
                .reconcile_install_started(
                    RecoveryRequest {
                        capture_id: "cap-foreign-missing".to_string(),
                        relative_path: missing_relative,
                        expected_bytes: 4,
                        expected_sha256: "00".repeat(32),
                        sidecar_body: None,
                    },
                    &cancellation,
                )
                .expect_err("foreign sidecar must block missing-partial cleanup");
            assert!(error.to_string().contains("unexpected metadata sidecar"));
            assert_eq!(fs::read(&missing_sidecar).unwrap(), b"{\"foreign\":true}\n");
            assert!(!missing_final.exists());
        }
    }

    #[cfg(windows)]
    mod windows_tests {
        use std::fs;
        use std::sync::Mutex;

        use super::*;

        /// Creates the destination inside the portable install window, and records where it did it.
        ///
        /// This is the collision the portable profile cannot prevent -- only refuse -- so the test
        /// has to create it in the one instant the capability believes the destination is free.
        #[derive(Default)]
        struct LateCollisionObserver {
            observed: Mutex<Option<PathBuf>>,
        }

        impl InstallObserver for LateCollisionObserver {
            fn before_install_link(&self, destination: &Path) {
                *self
                    .observed
                    .lock()
                    .expect("the observed destination is writable") =
                    Some(destination.to_path_buf());
                fs::write(destination, b"foreign image")
                    .expect("foreign final created after preflight");
            }
        }

        struct Gate;

        #[async_trait::async_trait]
        impl InstallGate for Gate {
            async fn begin_install(
                &self,
                _capture_id: &str,
                _changed_at_ms: i64,
            ) -> Result<InstallDecision> {
                Ok(InstallDecision::Started)
            }
        }

        struct DecisionGate(InstallDecision);

        #[async_trait::async_trait]
        impl InstallGate for DecisionGate {
            async fn begin_install(
                &self,
                _capture_id: &str,
                _changed_at_ms: i64,
            ) -> Result<InstallDecision> {
                Ok(self.0)
            }
        }

        struct FailingGate;

        #[async_trait::async_trait]
        impl InstallGate for FailingGate {
            async fn begin_install(
                &self,
                _capture_id: &str,
                _changed_at_ms: i64,
            ) -> Result<InstallDecision> {
                Err(CameraError::Storage(
                    "simulated catalog uncertainty".to_owned(),
                ))
            }
        }

        #[tokio::test]
        async fn portable_profile_installs_flushed_image_after_exact_sidecar() {
            let temp = tempfile::tempdir().expect("temporary output root");
            let config = output(temp.path(), true);
            let root = StorageRoot::open(&config).expect("portable root opens");
            let cancellation = CancellationToken::new();
            let prepared = root
                .prepare_capture(
                    prepare_request(&config, "cap-windows-sidecar", "cam"),
                    &cancellation,
                )
                .expect("exclusive partial is prepared");
            let final_path = PathBuf::from(&prepared.artifact().absolute_path);
            let sidecar_path = final_path.with_file_name(format!(
                "{}.json",
                final_path
                    .file_name()
                    .expect("final name")
                    .to_string_lossy()
            ));
            let body = serde_json::json!({"captureId": "cap-windows-sidecar", "kind": "test"});

            let artifact = prepared
                .install(&Gate, 1, Some(&body), &cancellation)
                .await
                .expect("portable install succeeds");

            assert_eq!(artifact.bytes, 4);
            assert_eq!(fs::read(&final_path).expect("final image"), [1, 2, 3, 4]);
            let mut expected_sidecar = serde_json::to_vec(&body).expect("sidecar JSON");
            expected_sidecar.push(b'\n');
            assert_eq!(
                fs::read(&sidecar_path).expect("metadata sidecar"),
                expected_sidecar
            );
        }

        #[tokio::test]
        async fn portable_profile_refuses_final_collision_without_overwrite() {
            let temp = tempfile::tempdir().expect("temporary output root");
            let config = output(temp.path(), false);
            let root = StorageRoot::open(&config).expect("portable root opens");
            let cancellation = CancellationToken::new();
            let prepared = root
                .prepare_capture(
                    prepare_request(&config, "cap-windows-collision", "cam"),
                    &cancellation,
                )
                .expect("exclusive partial is prepared");
            let final_path = PathBuf::from(&prepared.artifact().absolute_path);
            fs::write(&final_path, b"foreign image").expect("foreign final");

            let error = prepared
                .install(&Gate, 1, None, &cancellation)
                .await
                .expect_err("collision must not overwrite an existing final image");

            assert_eq!(error.code(), ErrorCode::PersistenceFailed);
            assert_eq!(
                fs::read(&final_path).expect("foreign final remains"),
                b"foreign image"
            );
        }

        #[tokio::test]
        async fn portable_profile_does_not_overwrite_a_final_that_appears_after_preflight() {
            let temp = tempfile::tempdir().expect("temporary output root");
            let config = output(temp.path(), false);
            let race = Arc::new(LateCollisionObserver::default());
            let root = StorageRoot::open_with_observer(
                &config,
                Arc::clone(&race) as Arc<dyn InstallObserver>,
            )
            .expect("portable root opens");
            let cancellation = CancellationToken::new();
            let prepared = root
                .prepare_capture(
                    prepare_request(&config, "cap-windows-race", "cam"),
                    &cancellation,
                )
                .expect("exclusive partial is prepared");
            let final_path = PathBuf::from(&prepared.artifact().absolute_path);

            let error = prepared
                .install(&Gate, 1, None, &cancellation)
                .await
                .expect_err("late collision must not overwrite the foreign final image");

            assert_eq!(
                race.observed
                    .lock()
                    .expect("the observed destination is readable")
                    .as_deref(),
                Some(final_path.as_path()),
                "the install window must be driven at the destination the capture announced"
            );
            assert_eq!(error.code(), ErrorCode::PersistenceFailed);
            assert_eq!(
                fs::read(&final_path).expect("foreign final remains"),
                b"foreign image"
            );
        }

        #[tokio::test]
        async fn portable_profile_rejects_invalid_sidecar_and_gate_outcomes_without_visibility() {
            let temp = tempfile::tempdir().expect("temporary output root");
            let sidecar_config = output(temp.path(), true);
            let sidecar_root = StorageRoot::open(&sidecar_config).expect("portable root opens");

            let missing_body = sidecar_root
                .prepare_capture(
                    prepare_request(&sidecar_config, "cap-missing-body", "cam"),
                    &CancellationToken::new(),
                )
                .expect("prepared image");
            let missing_body_final = PathBuf::from(&missing_body.artifact().absolute_path);
            let error = missing_body
                .install(&Gate, 1, None, &CancellationToken::new())
                .await
                .expect_err("sidecar-enabled install requires an object body");
            assert!(error.to_string().contains("required metadata sidecar body"));
            assert!(!missing_body_final.exists());

            let non_object = sidecar_root
                .prepare_capture(
                    prepare_request(&sidecar_config, "cap-non-object", "cam"),
                    &CancellationToken::new(),
                )
                .expect("prepared image");
            let error = non_object
                .install(
                    &Gate,
                    1,
                    Some(&serde_json::json!(null)),
                    &CancellationToken::new(),
                )
                .await
                .expect_err("metadata sidecar must be an object");
            assert!(error.to_string().contains("must be a JSON object"));

            let no_sidecar_config = output(temp.path(), false);
            let no_sidecar_root =
                StorageRoot::open(&no_sidecar_config).expect("portable root opens");
            let unexpected_body = no_sidecar_root
                .prepare_capture(
                    prepare_request(&no_sidecar_config, "cap-unexpected-body", "cam"),
                    &CancellationToken::new(),
                )
                .expect("prepared image");
            let error = unexpected_body
                .install(
                    &Gate,
                    1,
                    Some(&serde_json::json!({"unexpected": true})),
                    &CancellationToken::new(),
                )
                .await
                .expect_err("sidecar body is forbidden when disabled");
            assert!(error.to_string().contains("writeMetadataSidecar is false"));

            let rejected = no_sidecar_root
                .prepare_capture(
                    prepare_request(&no_sidecar_config, "cap-gate-rejected", "cam"),
                    &CancellationToken::new(),
                )
                .expect("prepared image");
            let rejected_final = PathBuf::from(&rejected.artifact().absolute_path);
            let error = rejected
                .install(
                    &DecisionGate(InstallDecision::Rejected),
                    1,
                    None,
                    &CancellationToken::new(),
                )
                .await
                .expect_err("rejected catalog CAS must prevent visibility");
            assert_eq!(error.code(), ErrorCode::CaptureCancelled);
            assert!(!rejected_final.exists());
        }

        #[tokio::test]
        async fn portable_profile_preserves_partial_when_catalog_install_is_ambiguous() {
            let temp = tempfile::tempdir().expect("temporary output root");
            let config = output(temp.path(), false);
            let root = StorageRoot::open(&config).expect("portable root opens");

            for (capture_id, gate) in [
                (
                    "cap-already-started",
                    &DecisionGate(InstallDecision::AlreadyStarted) as &dyn InstallGate,
                ),
                ("cap-catalog-error", &FailingGate as &dyn InstallGate),
            ] {
                let prepared = root
                    .prepare_capture(
                        prepare_request(&config, capture_id, "cam"),
                        &CancellationToken::new(),
                    )
                    .expect("prepared image");
                let partial_path = PathBuf::from(prepared.partial_path());
                let error = prepared
                    .install(gate, 1, None, &CancellationToken::new())
                    .await
                    .expect_err("ambiguous catalog result preserves recovery material");
                assert!(
                    partial_path.exists(),
                    "{capture_id} partial must remain for recovery"
                );
                assert!(
                    error.to_string().contains("install_started")
                        || error.to_string().contains("simulated catalog uncertainty")
                );
            }
        }

        #[test]
        fn portable_profile_recovery_cleans_incomplete_and_rejects_tampering() {
            let temp = tempfile::tempdir().expect("temporary output root");
            let config = output(temp.path(), true);
            let root = StorageRoot::open(&config).expect("portable root opens");
            let cancellation = CancellationToken::new();

            let mut missing_sidecar = root
                .prepare_capture(
                    prepare_request(&config, "cap-missing-sidecar", "cam"),
                    &cancellation,
                )
                .expect("prepared image");
            let missing_sidecar_partial = PathBuf::from(missing_sidecar.partial_path());
            let missing_sidecar_relative = missing_sidecar.artifact().relative_path.clone();
            let missing_sidecar_bytes = missing_sidecar.artifact().bytes;
            let missing_sidecar_hash = missing_sidecar.artifact().sha256.clone();
            missing_sidecar.installation_started = true;
            std::mem::forget(missing_sidecar);
            let outcome = root
                .reconcile_install_started(
                    RecoveryRequest {
                        capture_id: "cap-missing-sidecar".to_owned(),
                        relative_path: RelativeOutputPath::from_stored(&missing_sidecar_relative)
                            .expect("stored relative path"),
                        expected_bytes: missing_sidecar_bytes,
                        expected_sha256: missing_sidecar_hash,
                        sidecar_body: Some(serde_json::json!({"captureId": "cap-missing-sidecar"})),
                    },
                    &cancellation,
                )
                .expect("missing required sidecar is cleaned");
            assert_eq!(outcome, RecoveryOutcome::MissingArtifactsCleaned);
            assert!(!missing_sidecar_partial.exists());

            let no_sidecar_config = output(temp.path(), false);
            let no_sidecar_root =
                StorageRoot::open(&no_sidecar_config).expect("portable root opens");
            let mut corrupt = no_sidecar_root
                .prepare_capture(
                    prepare_request(&no_sidecar_config, "cap-corrupt", "cam"),
                    &cancellation,
                )
                .expect("prepared image");
            let corrupt_partial = PathBuf::from(corrupt.partial_path());
            let corrupt_relative = corrupt.artifact().relative_path.clone();
            let corrupt_bytes = corrupt.artifact().bytes;
            let corrupt_hash = corrupt.artifact().sha256.clone();
            fs::write(&corrupt_partial, b"tampered").expect("tamper partial");
            corrupt.installation_started = true;
            std::mem::forget(corrupt);
            let error = no_sidecar_root
                .reconcile_install_started(
                    RecoveryRequest {
                        capture_id: "cap-corrupt".to_owned(),
                        relative_path: RelativeOutputPath::from_stored(&corrupt_relative)
                            .expect("stored relative path"),
                        expected_bytes: corrupt_bytes,
                        expected_sha256: corrupt_hash,
                        sidecar_body: None,
                    },
                    &cancellation,
                )
                .expect_err("tampered partial cannot be recovered");
            assert!(
                error.to_string().contains("checksum") || error.to_string().contains("size"),
                "tampered recovery error must identify the verification failure: {error}"
            );
            assert!(!corrupt_partial.exists());
        }

        #[test]
        fn portable_profile_recovery_installs_verified_partial() {
            let temp = tempfile::tempdir().expect("temporary output root");
            let config = output(temp.path(), false);
            let root = StorageRoot::open(&config).expect("portable root opens");
            let cancellation = CancellationToken::new();
            let mut prepared = root
                .prepare_capture(
                    prepare_request(&config, "cap-windows-recovery", "cam"),
                    &cancellation,
                )
                .expect("exclusive partial is prepared");
            let relative_path = prepared.artifact().relative_path.clone();
            let expected_bytes = prepared.artifact().bytes;
            let expected_sha256 = prepared.artifact().sha256.clone();
            let final_path = PathBuf::from(&prepared.artifact().absolute_path);
            prepared.installation_started = true;
            std::mem::forget(prepared);

            let outcome = root
                .reconcile_install_started(
                    RecoveryRequest {
                        capture_id: "cap-windows-recovery".to_string(),
                        relative_path: RelativeOutputPath::from_stored(&relative_path)
                            .expect("stored relative path"),
                        expected_bytes,
                        expected_sha256,
                        sidecar_body: None,
                    },
                    &cancellation,
                )
                .expect("verified partial recovery");

            assert_eq!(outcome, RecoveryOutcome::InstalledFromPartial);
            assert_eq!(
                fs::read(final_path).expect("installed recovery image"),
                [1, 2, 3, 4]
            );
        }

        #[test]
        fn portable_profile_rejects_non_directory_component() {
            let temp = tempfile::tempdir().expect("temporary output root");
            let mut config = output(temp.path(), false);
            config.camera_directory_template = "camera".to_string();
            fs::write(temp.path().join("camera"), b"not a directory").expect("blocking file");
            let root = StorageRoot::open(&config).expect("portable root opens");

            let error = root
                .prepare_capture(
                    prepare_request(&config, "cap-windows-component", "cam"),
                    &CancellationToken::new(),
                )
                .expect_err("file component must not be traversed");

            assert!(error.to_string().contains("not a regular directory"));
        }
    }
}
