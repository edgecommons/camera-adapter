//! Deterministic in-process camera backend.
//!
//! `SimBackend` is a production-configurable backend: its frames, timing, failures,
//! capabilities, PTZ state, and presets pass through the same runtime as physical cameras.
//! It allocates no image-sized buffer while idle.
//!
//! Its frames are synthetic by default. The `playlist` pattern instead replays a directory of real
//! image files, one per capture, so a downstream vision component receives genuine imagery through
//! the ordinary camera path: the same encoding stage, the same sidecar-before-image finalization,
//! the same catalog record, and the same `ImageCaptured` announcement.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use bytes::Bytes;
use chrono::Utc;
use image::codecs::jpeg::{JpegDecoder, JpegEncoder};
use image::{ExtendedColorType, GenericImageView, ImageDecoder, ImageFormat};
use serde_json::{Value, json};

use super::{
    CameraBackendFactory, CameraSession, CameraStatus, CaptureRequest, ConnectRequest,
    DiscoveryCandidate, DiscoveryRequest,
};
use crate::config::{
    BackendConfig, SimBackendConfig, SimPattern, SimPlaylistConfig, SimPlaylistOrder,
};
use crate::error::{CameraError, ErrorCode, Result};
use crate::model::{
    BackendKind, CameraCapabilities, CaptureFrame, CaptureMode, FrameTimestampQuality,
    OutputEncoding, PixelFormat, PtzPreset, PtzRequest, PtzResult, PtzStatus, PtzVector,
};

/// Stateless factory for deterministic simulator sessions.
#[derive(Debug, Default)]
pub struct SimBackendFactory;

impl SimBackendFactory {
    /// Creates a simulator factory.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[async_trait]
impl CameraBackendFactory for SimBackendFactory {
    fn kind(&self) -> BackendKind {
        BackendKind::Sim
    }

    async fn discover(&self, _request: DiscoveryRequest) -> Result<Vec<DiscoveryCandidate>> {
        // Sim cameras are explicit config, never ambient discoveries.
        Ok(Vec::new())
    }

    async fn connect(&self, request: ConnectRequest) -> Result<Box<dyn CameraSession>> {
        let BackendConfig::Sim(config) = request.backend else {
            return Err(CameraError::Backend {
                backend: "sim",
                message: "factory received a non-sim backend config".to_string(),
            });
        };
        let delay = Duration::from_millis(config.connect_delay_ms);
        tokio::select! {
            () = request.cancellation.cancelled() => {
                return Err(CameraError::rejected(ErrorCode::CaptureCancelled, "sim connection cancelled"));
            }
            () = tokio::time::sleep(delay) => {}
        }
        // The playlist is read HERE, once, and never again for this session: a directory walk is
        // filesystem work with an unbounded worst case, and doing it per capture would put it on the
        // acquisition deadline of every single frame. Reading it at connect also gives the replay a
        // fixed list to be deterministic about -- `sb/reconnect` is what re-reads a changed directory.
        let identity = simulated_id(&config, &request.instance_id);
        let seed = simulated_seed(&config, &identity);
        let playlist = match config.frame.pattern.playlist() {
            Some(settings) => {
                let settings = settings.clone();
                Some(
                    tokio::task::spawn_blocking(move || Playlist::load(&settings, seed))
                        .await
                        .map_err(|error| CameraError::Backend {
                            backend: "sim",
                            message: format!("simulated playlist load task failed: {error}"),
                        })??,
                )
            }
            None => None,
        };
        Ok(Box::new(SimSession::new(
            request.instance_id,
            config,
            playlist,
        )))
    }
}

struct SimSession {
    id: String,
    config: SimBackendConfig,
    capabilities: CameraCapabilities,
    capture_ordinal: u64,
    closed: bool,
    position: PtzVector,
    moving: bool,
    presets: BTreeMap<String, (Option<String>, PtzVector)>,
    /// Present exactly when the configured pattern replays files.
    playlist: Option<Playlist>,
}

impl SimSession {
    fn new(instance_id: String, config: SimBackendConfig, playlist: Option<Playlist>) -> Self {
        let id = simulated_id(&config, &instance_id);
        let ptz = config.ptz.supported;
        let presets = config.ptz.presets_supported;
        Self {
            id: id.clone(),
            capabilities: CameraCapabilities {
                capture_modes: vec![CaptureMode::Simulated],
                // A playlist reports what a replayed FILE is, not what the synthetic generator was
                // configured to emit: a JPEG passed through byte for byte, or the pixels a decoded
                // file yields. Advertising `frame.pixelFormat` here would describe a generator this
                // session never runs.
                pixel_formats: if playlist.is_some() {
                    vec![PixelFormat::Jpeg, PixelFormat::Rgb8, PixelFormat::Mono8]
                } else {
                    vec![config.frame.pixel_format]
                },
                software_trigger: false,
                snapshot_uri: false,
                rtsp: false,
                ptz,
                ptz_status: ptz && config.ptz.status_supported,
                presets: ptz && presets,
                preset_mutation: ptz && presets,
                vendor: Some("EdgeCommons".to_string()),
                model: Some("SimBackend".to_string()),
                firmware: Some(env!("CARGO_PKG_VERSION").to_string()),
                serial: Some(id),
                warnings: Vec::new(),
            },
            config,
            capture_ordinal: 0,
            closed: false,
            position: PtzVector {
                pan: 0.0,
                tilt: 0.0,
                zoom: 0.0,
            },
            moving: false,
            presets: BTreeMap::new(),
            playlist,
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(CameraError::rejected(
                ErrorCode::DeviceUnavailable,
                "sim camera session is closed",
            ))
        } else {
            Ok(())
        }
    }

    fn ensure_ptz(&self) -> Result<()> {
        self.ensure_open()?;
        if !self.capabilities.ptz {
            return Err(CameraError::rejected(
                ErrorCode::UnsupportedCapability,
                "sim camera does not advertise PTZ",
            ));
        }
        Ok(())
    }

    fn should_fire(period: Option<u64>, ordinal: u64) -> bool {
        period.is_some_and(|period| ordinal % period == 0)
    }

    /// The owned inputs a synthesis needs, so it can run on a blocking thread with no borrow of `self`.
    fn frame_recipe(&self) -> FrameRecipe {
        FrameRecipe {
            frame: self.config.frame.clone(),
            seed: simulated_seed(&self.config, &self.id),
        }
    }
}

/// Everything a frame synthesis needs, owned — so it crosses a `spawn_blocking` boundary.
#[derive(Clone)]
struct FrameRecipe {
    frame: crate::config::SimFrameConfig,
    seed: u64,
}

/// Synthesize one frame's bytes. CPU-bound, and deliberately a free function taking OWNED inputs:
/// a real camera does not fill megapixels on the async reactor, and neither should the simulator that
/// stands in for a fleet of them. `capture()` runs this on a blocking thread so the runtime being
/// measured is never the thread doing the pixel work — that pollution was review finding H3, where the
/// harness synthesized every frame on the very reactor whose scheduling latency it was measuring.
fn synthesize_frame(recipe: &FrameRecipe, ordinal: u64, limit: u64) -> Result<Vec<u8>> {
    let frame = &recipe.frame;
    let raw_format = if frame.pixel_format == PixelFormat::Jpeg {
        PixelFormat::Rgb8
    } else {
        frame.pixel_format
    };
    let expected = raw_format
        .uncompressed_len(frame.width, frame.height)
        .ok_or_else(|| {
            CameraError::rejected(
                ErrorCode::UnsupportedPixelFormat,
                "unsupported simulator source format",
            )
        })?;
    if expected > limit || usize::try_from(expected).is_err() {
        return Err(CameraError::rejected(
            ErrorCode::ResourceLimit,
            "simulated frame exceeds the accepted maximum",
        ));
    }
    let mut bytes = vec![0_u8; expected as usize];
    fill_pattern(
        &mut bytes,
        frame.width,
        frame.height,
        raw_format,
        &frame.pattern,
        recipe.seed,
        ordinal,
    );
    if frame.pixel_format != PixelFormat::Jpeg {
        return Ok(bytes);
    }
    let mut jpeg = Vec::new();
    JpegEncoder::new_with_quality(Cursor::new(&mut jpeg), 90)
        .encode(&bytes, frame.width, frame.height, ExtendedColorType::Rgb8)
        .map_err(|error| CameraError::Backend {
            backend: "sim",
            message: format!("JPEG generation failed: {error}"),
        })?;
    if jpeg.len() as u64 > limit {
        return Err(CameraError::rejected(
            ErrorCode::ResourceLimit,
            "simulated JPEG exceeds the accepted maximum",
        ));
    }
    Ok(jpeg)
}

#[async_trait]
impl CameraSession for SimSession {
    fn capabilities(&self) -> &CameraCapabilities {
        &self.capabilities
    }

    async fn status(&mut self) -> Result<CameraStatus> {
        self.ensure_open()?;
        let ptz = self.capabilities.ptz_status.then(|| PtzStatus {
            position: Some(self.position),
            moving: Some(self.moving),
            observed_at: Utc::now(),
        });
        let mut backend = json!({ "simulatedId": self.id, "captureOrdinal": self.capture_ordinal });
        if let Some(playlist) = self.playlist.as_ref() {
            backend["playlist"] = playlist.diagnostics();
        }
        Ok(CameraStatus {
            online: true,
            connection_generation: 1,
            ptz,
            backend,
        })
    }

    async fn capture(&mut self, request: CaptureRequest) -> Result<CaptureFrame> {
        self.ensure_open()?;
        self.capture_ordinal = self.capture_ordinal.saturating_add(1);
        let ordinal = self.capture_ordinal;
        // A capture cancelled before it begins allocates nothing. This mirrors a real backend that
        // never opens the socket, and it keeps the "prevents frame allocation" contract even now that
        // synthesis happens up front rather than after the latency wait.
        if request.cancellation.is_cancelled() {
            return Err(CameraError::rejected(
                ErrorCode::CaptureCancelled,
                "sim capture cancelled",
            ));
        }
        if self
            .config
            .faults
            .disconnect_after_captures
            .is_some_and(|count| ordinal > count)
        {
            self.closed = true;
            return Err(CameraError::rejected(
                ErrorCode::DeviceUnavailable,
                "simulated disconnect threshold reached",
            ));
        }
        if Self::should_fire(self.config.faults.fail_every_nth_capture, ordinal) {
            return Err(CameraError::Backend {
                backend: "sim",
                message: "configured deterministic capture failure".to_string(),
            });
        }
        // H3: acquire OFF the reactor. `spawn_blocking` runs the pixel fill (and any JPEG encode), or
        // the playlist file read and decode, on the blocking pool, so a fleet of 256 simulated cameras
        // filling megapixels never blocks the async worker threads whose scheduling and dispatch
        // latency the capacity harness exists to measure.
        let limit = request.maximum_frame_bytes;
        let mut acquired = match self.playlist.as_mut() {
            Some(playlist) => {
                let index = playlist.take()?;
                let entry = playlist.entry(index).clone();
                let root = playlist.root.clone();
                let encoding = request.profile.output.encoding;
                tokio::task::spawn_blocking(move || {
                    read_playlist_frame(&entry, &root, index, limit, encoding)
                })
                .await
                .map_err(|error| CameraError::Backend {
                    backend: "sim",
                    message: format!("simulated playlist read task failed: {error}"),
                })??
            }
            None => {
                let recipe = self.frame_recipe();
                let frame = recipe.frame.clone();
                let bytes =
                    tokio::task::spawn_blocking(move || synthesize_frame(&recipe, ordinal, limit))
                        .await
                        .map_err(|error| CameraError::Backend {
                            backend: "sim",
                            message: format!("simulated frame synthesis task failed: {error}"),
                        })??;
                AcquiredFrame {
                    bytes,
                    width: frame.width,
                    height: frame.height,
                    pixel_format: frame.pixel_format,
                    playlist: None,
                }
            }
        };
        if Self::should_fire(self.config.faults.incomplete_every_nth_capture, ordinal) {
            acquired
                .bytes
                .truncate(acquired.bytes.len().saturating_sub(1));
            return Err(CameraError::Backend {
                backend: "sim",
                message: "configured deterministic incomplete frame".to_string(),
            });
        }
        // H2: model the sensor/transfer latency while HOLDING the real frame buffer. The old harness
        // held its capture permit on a bare `sleep` that touched no memory, so "32 concurrent 8MP
        // captures" was 32 accounting reservations over an idle process — the peak that 32 real frame
        // buffers actually cost was never on the heap at once. Now the bytes are live for the whole
        // latency window, so concurrency is measured in resident pages, not just in a counter.
        let delay = Duration::from_millis(self.config.capture_delay_ms);
        if !delay.is_zero() {
            tokio::select! {
                () = request.cancellation.cancelled() => {
                    return Err(CameraError::rejected(ErrorCode::CaptureCancelled, "sim capture cancelled"));
                }
                () = tokio::time::sleep(delay) => {}
            }
        }
        let now = Utc::now();
        let mut backend_metadata = BTreeMap::from([
            ("simulatedId".to_string(), json!(self.id)),
            ("captureOrdinal".to_string(), json!(ordinal)),
            ("captureId".to_string(), json!(request.capture_id)),
        ]);
        // The originating file travels with the frame, so the terminal announcement, the catalog
        // record, and the metadata sidecar all name the image that was replayed. Without it a replayed
        // capture is indistinguishable from a synthetic one once it is on disk.
        if let Some(facts) = acquired.playlist {
            backend_metadata.insert(
                "playlist".to_string(),
                json!({ "sourcePath": facts.source_path, "index": facts.index }),
            );
        }
        Ok(CaptureFrame {
            bytes: Bytes::from(acquired.bytes),
            width: acquired.width,
            height: acquired.height,
            pixel_format: acquired.pixel_format,
            capture_mode: CaptureMode::Simulated,
            source_timestamp: Some(now),
            timestamp_quality: FrameTimestampQuality::Camera,
            backend_metadata,
        })
    }

    async fn ptz_bounded(
        &mut self,
        request: PtzRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PtzResult> {
        crate::backend::bounded_ptz(self.apply_ptz(request), deadline, cancellation).await
    }

    async fn close(&mut self) -> Result<()> {
        self.moving = false;
        self.closed = true;
        Ok(())
    }
}

impl SimSession {
    /// The simulated PTZ state machine. Entirely in memory, so it cannot outrun a deadline.
    async fn apply_ptz(&mut self, request: PtzRequest) -> Result<PtzResult> {
        self.ensure_ptz()?;
        match request {
            PtzRequest::Continuous { velocity, .. } => {
                if !velocity.validate_signed() {
                    return Err(CameraError::rejected(
                        ErrorCode::PtzRangeError,
                        "continuous velocity is outside [-1,1]",
                    ));
                }
                self.moving = velocity
                    != PtzVector {
                        pan: 0.0,
                        tilt: 0.0,
                        zoom: 0.0,
                    };
                Ok(PtzResult::Commanded)
            }
            PtzRequest::Absolute { position, speed } => {
                if !position.validate_absolute()
                    || speed.is_some_and(|value| !value.validate_signed())
                {
                    return Err(CameraError::rejected(
                        ErrorCode::PtzRangeError,
                        "absolute PTZ vector is outside the normalized range",
                    ));
                }
                self.position = position;
                self.moving = false;
                Ok(PtzResult::Commanded)
            }
            PtzRequest::Relative { translation, speed } => {
                if !translation.validate_signed()
                    || speed.is_some_and(|value| !value.validate_signed())
                {
                    return Err(CameraError::rejected(
                        ErrorCode::PtzRangeError,
                        "relative PTZ vector is outside the normalized range",
                    ));
                }
                self.position.pan = (self.position.pan + translation.pan).clamp(-1.0, 1.0);
                self.position.tilt = (self.position.tilt + translation.tilt).clamp(-1.0, 1.0);
                self.position.zoom = (self.position.zoom + translation.zoom).clamp(0.0, 1.0);
                self.moving = false;
                Ok(PtzResult::Commanded)
            }
            PtzRequest::Stop { .. } => {
                self.moving = false;
                Ok(PtzResult::Commanded)
            }
            PtzRequest::Home => {
                self.position = PtzVector {
                    pan: 0.0,
                    tilt: 0.0,
                    zoom: 0.0,
                };
                self.moving = false;
                Ok(PtzResult::Commanded)
            }
            PtzRequest::Status => {
                if !self.capabilities.ptz_status {
                    return Err(CameraError::rejected(
                        ErrorCode::UnsupportedCapability,
                        "sim PTZ status is disabled",
                    ));
                }
                Ok(PtzResult::Status(PtzStatus {
                    position: Some(self.position),
                    moving: Some(self.moving),
                    observed_at: Utc::now(),
                }))
            }
            PtzRequest::ListPresets => {
                if !self.capabilities.presets {
                    return Err(CameraError::rejected(
                        ErrorCode::UnsupportedCapability,
                        "sim presets are disabled",
                    ));
                }
                Ok(PtzResult::Presets(
                    self.presets
                        .iter()
                        .map(|(token, (name, _))| PtzPreset {
                            token: token.clone(),
                            name: name.clone(),
                        })
                        .collect(),
                ))
            }
            PtzRequest::GotoPreset(token) => {
                let (_, position) = self.presets.get(&token).ok_or_else(|| {
                    CameraError::rejected(ErrorCode::BadArgs, "unknown preset token")
                })?;
                self.position = *position;
                self.moving = false;
                Ok(PtzResult::Commanded)
            }
            PtzRequest::SetPreset(name) => {
                if !self.capabilities.preset_mutation {
                    return Err(CameraError::rejected(
                        ErrorCode::UnsupportedCapability,
                        "sim preset mutation is disabled",
                    ));
                }
                let token = format!("preset-{}", self.presets.len() + 1);
                self.presets
                    .insert(token.clone(), (Some(name), self.position));
                Ok(PtzResult::PresetToken(token))
            }
            PtzRequest::RemovePreset(token) => {
                if !self.capabilities.preset_mutation {
                    return Err(CameraError::rejected(
                        ErrorCode::UnsupportedCapability,
                        "sim preset mutation is disabled",
                    ));
                }
                if self.presets.remove(&token).is_none() {
                    return Err(CameraError::rejected(
                        ErrorCode::BadArgs,
                        "unknown preset token",
                    ));
                }
                Ok(PtzResult::Removed)
            }
        }
    }

}

/// The simulated device identity: the configured override, or the camera instance id.
fn simulated_id(config: &SimBackendConfig, instance_id: &str) -> String {
    config
        .simulated_id
        .clone()
        .unwrap_or_else(|| instance_id.to_owned())
}

/// The generator/shuffle seed: the configured value, or a stable hash of the simulated identity.
fn simulated_seed(config: &SimBackendConfig, id: &str) -> u64 {
    config.seed.unwrap_or_else(|| stable_seed(id))
}

/// Hard ceiling on playlist members, so a directory pointed at a whole filesystem fails fast.
const MAX_PLAYLIST_FILES: usize = 10_000;
/// Hard ceiling on directory nesting, for the same reason.
const MAX_PLAYLIST_DEPTH: usize = 32;

const JPEG_MAGIC: [u8; 3] = [0xff, 0xd8, 0xff];
const PNG_MAGIC: [u8; 8] = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];

/// One acquisition's bytes plus the facts the frame reports about them.
struct AcquiredFrame {
    bytes: Vec<u8>,
    width: u32,
    height: u32,
    pixel_format: PixelFormat,
    playlist: Option<PlaylistFacts>,
}

/// Where a replayed frame came from.
struct PlaylistFacts {
    /// `/`-separated path relative to the playlist directory.
    source_path: String,
    /// Position of the file in the replay order.
    index: usize,
}

/// One playlist member.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PlaylistEntry {
    /// `/`-separated path relative to the playlist directory, and the `sorted` order's sort key.
    relative: String,
    /// Absolute path opened at capture time.
    absolute: PathBuf,
}

/// A directory of real image files, read once at connect and replayed as captures.
///
/// Containment is structural rather than checked after the fact: the walk rejects every symbolic
/// link it meets and descends only real directories under the canonicalized root, so no member can
/// name a file outside the directory. [`read_playlist_frame`] re-checks at capture time, because the
/// directory is a live filesystem and a member can be replaced between connect and capture.
#[derive(Debug)]
struct Playlist {
    /// Directory as configured, for diagnostics.
    directory: PathBuf,
    /// Canonicalized directory, for containment.
    root: PathBuf,
    entries: Vec<PlaylistEntry>,
    /// Position the next capture replays.
    cursor: usize,
    loop_playlist: bool,
}

impl Playlist {
    /// Reads and orders the directory. Blocking: callers run it on the blocking pool.
    ///
    /// # Errors
    /// `DEVICE_UNAVAILABLE` when the directory cannot be read, holds a symbolic link, nests or grows
    /// past the bounds above, or matches no file at all -- a camera that can never produce a frame
    /// must fail to connect rather than accept captures it will refuse one at a time.
    fn load(config: &SimPlaylistConfig, seed: u64) -> Result<Self> {
        let root = std::fs::canonicalize(&config.directory).map_err(|error| {
            playlist_unavailable(format!("playlist directory cannot be opened: {error}"))
        })?;
        let mut entries = Vec::new();
        collect_playlist(&root, &root, &config.include, 0, &mut entries)?;
        if entries.is_empty() {
            return Err(playlist_unavailable(
                "playlist directory holds no file matching the include globs",
            ));
        }
        entries.sort_by(|left, right| left.relative.cmp(&right.relative));
        if config.order == SimPlaylistOrder::Seeded {
            shuffle_playlist(&mut entries, seed);
        }
        Ok(Self {
            directory: config.directory.clone(),
            root,
            entries,
            cursor: 0,
            loop_playlist: config.loop_playlist,
        })
    }

    /// The member at `index`.
    fn entry(&self, index: usize) -> &PlaylistEntry {
        &self.entries[index]
    }

    /// The position this capture replays, moving the cursor to the next file.
    ///
    /// # Errors
    /// `DEVICE_UNAVAILABLE` once an unlooped playlist has replayed its last file.
    fn take(&mut self) -> Result<usize> {
        if self.cursor >= self.entries.len() {
            if !self.loop_playlist {
                return Err(playlist_unavailable(
                    "playlist is spent: every file has been replayed and loop is false",
                ));
            }
            self.cursor = 0;
        }
        let index = self.cursor;
        self.cursor += 1;
        Ok(index)
    }

    /// Position the next capture replays, or the file count once a spent playlist cannot restart.
    fn next_index(&self) -> usize {
        if self.cursor >= self.entries.len() && self.loop_playlist {
            0
        } else {
            self.cursor
        }
    }

    /// The playlist as session status reports it.
    fn diagnostics(&self) -> Value {
        json!({
            "count": self.entries.len(),
            "index": self.next_index(),
            "directory": self.directory.display().to_string(),
        })
    }
}

fn playlist_unavailable(message: impl std::fmt::Display) -> CameraError {
    CameraError::rejected(ErrorCode::DeviceUnavailable, format!("sim {message}"))
}

/// Walks `directory` and appends every included file. Deterministic, and it follows no link.
fn collect_playlist(
    root: &Path,
    directory: &Path,
    include: &[String],
    depth: usize,
    entries: &mut Vec<PlaylistEntry>,
) -> Result<()> {
    if depth > MAX_PLAYLIST_DEPTH {
        return Err(playlist_unavailable(format!(
            "playlist directory nests deeper than {MAX_PLAYLIST_DEPTH} levels"
        )));
    }
    let listing = std::fs::read_dir(directory).map_err(|error| {
        playlist_unavailable(format!("playlist directory cannot be listed: {error}"))
    })?;
    let mut children = Vec::new();
    for child in listing {
        let child = child.map_err(|error| {
            playlist_unavailable(format!("playlist directory entry cannot be read: {error}"))
        })?;
        children.push(child.path());
    }
    children.sort();
    for path in children {
        let relative = relative_token(root, &path).ok_or_else(|| {
            playlist_unavailable("playlist paths must be valid UTF-8 with no parent references")
        })?;
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
            playlist_unavailable(format!("playlist entry {relative} cannot be read: {error}"))
        })?;
        if metadata.is_symlink() {
            return Err(playlist_unavailable(format!(
                "playlist entry {relative} is a symbolic link, which can name a file outside the playlist directory"
            )));
        }
        if metadata.is_dir() {
            collect_playlist(root, &path, include, depth + 1, entries)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        if !include.iter().any(|glob| glob_matches(glob, &relative)) {
            continue;
        }
        if entries.len() >= MAX_PLAYLIST_FILES {
            return Err(playlist_unavailable(format!(
                "playlist directory holds more than {MAX_PLAYLIST_FILES} matching files"
            )));
        }
        entries.push(PlaylistEntry {
            relative,
            absolute: path,
        });
    }
    Ok(())
}

/// `path` relative to `root` as a `/`-separated token, or `None` when it is not plainly nested.
fn relative_token(root: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?;
    let mut token = String::new();
    for component in relative.components() {
        let std::path::Component::Normal(part) = component else {
            return None;
        };
        if !token.is_empty() {
            token.push('/');
        }
        token.push_str(part.to_str()?);
    }
    Some(token)
}

/// Fisher-Yates over the sorted list, so `seeded` is a shuffle and still reproducible.
fn shuffle_playlist(entries: &mut [PlaylistEntry], seed: u64) {
    let mut state = seed;
    for index in (1..entries.len()).rev() {
        let pick = (next_random(&mut state) % (index as u64 + 1)) as usize;
        entries.swap(index, pick);
    }
}

/// SplitMix64.
///
/// Owned rather than taken from `rand` on purpose: the order a `seeded` playlist replays in is
/// configuration-visible behaviour, and pinning the generator here means a dependency bump cannot
/// silently reorder a fixture that a downstream test asserts against.
fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Matches one include glob against a `/`-separated relative path.
///
/// `**` spans any number of path segments, `*` matches within one segment, `?` matches one
/// character, and everything else is a literal. Matching is case-sensitive, so a directory behaves
/// the same on a case-insensitive filesystem as on a case-sensitive one.
fn glob_matches(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    glob_match(&pattern, &text)
}

fn glob_match(pattern: &[char], text: &[char]) -> bool {
    match pattern.first() {
        None => text.is_empty(),
        Some('*') if pattern.get(1) == Some(&'*') => {
            let rest = &pattern[2..];
            // `**/name` also matches `name`: the separator after `**` stands for zero segments too.
            if rest.first() == Some(&'/') && glob_match(&rest[1..], text) {
                return true;
            }
            (0..=text.len()).any(|split| glob_match(rest, &text[split..]))
        }
        Some('*') => {
            let rest = &pattern[1..];
            let bound = text
                .iter()
                .position(|value| *value == '/')
                .unwrap_or(text.len());
            (0..=bound).any(|split| glob_match(rest, &text[split..]))
        }
        Some('?') => {
            matches!(text.first(), Some(value) if *value != '/')
                && glob_match(&pattern[1..], &text[1..])
        }
        Some(literal) => {
            matches!(text.first(), Some(value) if value == literal)
                && glob_match(&pattern[1..], &text[1..])
        }
    }
}

/// Reads one playlist file and turns it into a frame. Blocking: callers run it on the blocking pool.
///
/// A JPEG replayed into a byte-preserving output (`passthrough` or `raw`) is handed on exactly as it
/// sits on disk, so the announced `image.sha256` IS the file's digest and a consumer can verify the
/// installed artifact against its source. Every other combination decodes to pixels and lets the
/// ordinary encoding stage produce the requested file, because this component's passthrough contract
/// is a complete JPEG source and a re-encode is the only honest way to answer for anything else.
///
/// # Errors
/// `RESOURCE_LIMIT` when the file or its decoded pixels exceed the accepted frame ceiling,
/// `UNSUPPORTED_PIXEL_FORMAT` when the file is not a decodable JPEG or PNG, and
/// `DEVICE_UNAVAILABLE` when it has disappeared or no longer resolves inside the playlist directory.
fn read_playlist_frame(
    entry: &PlaylistEntry,
    root: &Path,
    index: usize,
    limit: u64,
    encoding: OutputEncoding,
) -> Result<AcquiredFrame> {
    let relative = entry.relative.as_str();
    let metadata = std::fs::symlink_metadata(&entry.absolute).map_err(|error| {
        playlist_unavailable(format!("playlist file {relative} cannot be read: {error}"))
    })?;
    if metadata.is_symlink() {
        return Err(playlist_unavailable(format!(
            "playlist file {relative} became a symbolic link after the playlist was read"
        )));
    }
    let canonical = std::fs::canonicalize(&entry.absolute).map_err(|error| {
        playlist_unavailable(format!(
            "playlist file {relative} cannot be resolved: {error}"
        ))
    })?;
    if !canonical.starts_with(root) {
        return Err(playlist_unavailable(format!(
            "playlist file {relative} resolves outside the playlist directory"
        )));
    }
    if metadata.len() > limit {
        return Err(CameraError::rejected(
            ErrorCode::ResourceLimit,
            format!("playlist file {relative} exceeds the accepted maximum frame size"),
        ));
    }
    let bytes = std::fs::read(&entry.absolute).map_err(|error| {
        playlist_unavailable(format!("playlist file {relative} cannot be read: {error}"))
    })?;
    let format = playlist_format(&bytes).ok_or_else(|| {
        CameraError::rejected(
            ErrorCode::UnsupportedPixelFormat,
            format!("playlist file {relative} is neither JPEG nor PNG"),
        )
    })?;
    let facts = PlaylistFacts {
        source_path: entry.relative.clone(),
        index,
    };
    if format == ImageFormat::Jpeg
        && matches!(encoding, OutputEncoding::Passthrough | OutputEncoding::Raw)
    {
        let (width, height) = JpegDecoder::new(Cursor::new(&bytes))
            .map_err(|error| {
                CameraError::rejected(
                    ErrorCode::UnsupportedPixelFormat,
                    format!("playlist file {relative} is not a decodable JPEG: {error}"),
                )
            })?
            .dimensions();
        return Ok(AcquiredFrame {
            bytes,
            width,
            height,
            pixel_format: PixelFormat::Jpeg,
            playlist: Some(facts),
        });
    }
    let decoded = image::load_from_memory_with_format(&bytes, format).map_err(|error| {
        CameraError::rejected(
            ErrorCode::UnsupportedPixelFormat,
            format!("playlist file {relative} cannot be decoded: {error}"),
        )
    })?;
    let (width, height) = decoded.dimensions();
    let (bytes, pixel_format) = if decoded.color().has_color() {
        (decoded.to_rgb8().into_raw(), PixelFormat::Rgb8)
    } else {
        (decoded.to_luma8().into_raw(), PixelFormat::Mono8)
    };
    if bytes.len() as u64 > limit {
        return Err(CameraError::rejected(
            ErrorCode::ResourceLimit,
            format!("playlist file {relative} decodes past the accepted maximum frame size"),
        ));
    }
    Ok(AcquiredFrame {
        bytes,
        width,
        height,
        pixel_format,
        playlist: Some(facts),
    })
}

/// The file's real format, from its magic bytes rather than its name.
fn playlist_format(bytes: &[u8]) -> Option<ImageFormat> {
    if bytes.starts_with(&JPEG_MAGIC) {
        Some(ImageFormat::Jpeg)
    } else if bytes.starts_with(&PNG_MAGIC) {
        Some(ImageFormat::Png)
    } else {
        None
    }
}

fn stable_seed(value: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(value.as_bytes());
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(prefix)
}

fn fill_pattern(
    bytes: &mut [u8],
    width: u32,
    height: u32,
    format: PixelFormat,
    pattern: &SimPattern,
    seed: u64,
    ordinal: u64,
) {
    let channels = if format == PixelFormat::Mono8 { 1 } else { 3 };
    for y in 0..height {
        for x in 0..width {
            let rgb = pixel(pattern, x, y, width, height, seed, ordinal);
            let offset = ((u64::from(y) * u64::from(width) + u64::from(x)) * channels) as usize;
            match format {
                PixelFormat::Mono8 => {
                    bytes[offset] =
                        ((u16::from(rgb[0]) + u16::from(rgb[1]) + u16::from(rgb[2])) / 3) as u8;
                }
                PixelFormat::Rgb8 => bytes[offset..offset + 3].copy_from_slice(&rgb),
                PixelFormat::Bgr8 => {
                    bytes[offset..offset + 3].copy_from_slice(&[rgb[2], rgb[1], rgb[0]])
                }
                PixelFormat::Jpeg => unreachable!("JPEG pattern generation uses RGB8"),
            }
        }
    }
}

fn pixel(
    pattern: &SimPattern,
    x: u32,
    y: u32,
    width: u32,
    height: u32,
    seed: u64,
    ordinal: u64,
) -> [u8; 3] {
    match pattern {
        SimPattern::ColorBars => {
            const COLORS: [[u8; 3]; 8] = [
                [255, 255, 255],
                [255, 255, 0],
                [0, 255, 255],
                [0, 255, 0],
                [255, 0, 255],
                [255, 0, 0],
                [0, 0, 255],
                [0, 0, 0],
            ];
            let index = ((u64::from(x) * COLORS.len() as u64) / u64::from(width.max(1))) as usize;
            COLORS[index.min(COLORS.len() - 1)]
        }
        SimPattern::Gradient => [
            (u64::from(x) * 255 / u64::from(width.max(1))) as u8,
            (u64::from(y) * 255 / u64::from(height.max(1))) as u8,
            ordinal.wrapping_add(seed) as u8,
        ],
        SimPattern::Checkerboard => {
            let light = ((x / 16) + (y / 16) + ordinal as u32) % 2 == 0;
            if light { [230, 230, 230] } else { [25, 25, 25] }
        }
        SimPattern::Solid => {
            let value = seed.wrapping_add(ordinal);
            [value as u8, (value >> 8) as u8, (value >> 16) as u8]
        }
        SimPattern::Playlist(_) => {
            unreachable!("a playlist frame is read from disk, never generated pixel by pixel")
        }
    }
}

#[cfg(test)]
mod tests {
    /// A generously-bounded PTZ call, for tests that are not about the bound.
    ///
    /// `CameraSession` deliberately offers only `ptz_bounded`: an unbounded variant is an invitation to
    /// fabricate the deadline and the cancellation token, which is precisely what the old required
    /// `ptz` drove `OnvifSession` to do. Tests that are exercising PTZ BEHAVIOUR still want to say
    /// `session.ptz(request)` without inventing a deadline in every line, so they say it here, once,
    /// where the deadline is obviously a test's and not a protocol's.
    #[async_trait]
    trait GenerouslyBoundedPtz {
        async fn ptz(&mut self, request: PtzRequest) -> Result<PtzResult>;
    }

    #[async_trait]
    impl<T: CameraSession + ?Sized> GenerouslyBoundedPtz for T {
        async fn ptz(&mut self, request: PtzRequest) -> Result<PtzResult> {
            self.ptz_bounded(
                request,
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                &tokio_util::sync::CancellationToken::new(),
            )
            .await
        }
    }

    use super::*;
    use crate::config::CaptureProfile;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    fn backend(value: serde_json::Value) -> BackendConfig {
        serde_json::from_value(value).unwrap()
    }

    fn profile() -> CaptureProfile {
        serde_json::from_value(json!({"output":{"encoding":"png"}})).unwrap()
    }

    async fn session(value: serde_json::Value) -> Box<dyn CameraSession> {
        SimBackendFactory::new()
            .connect(ConnectRequest {
                instance_id: "cam-a".to_string(),
                backend: backend(value),
                timeout: Duration::from_secs(1),
                cancellation: CancellationToken::new(),
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn deterministic_frames_repeat_for_new_session() {
        let config = json!({"type":"sim","seed":7,"frame":{"width":16,"height":8,"pixelFormat":"RGB8","pattern":"gradient"}});
        let mut first = session(config.clone()).await;
        let mut second = session(config).await;
        let request = || CaptureRequest {
            capture_id: "cap-1".to_string(),
            profile: profile(),
            maximum_frame_bytes: 1_000_000,
            timeout: Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        };
        assert_eq!(
            first.capture(request()).await.unwrap().bytes,
            second.capture(request()).await.unwrap().bytes
        );
    }

    #[tokio::test]
    async fn nth_failure_is_deterministic() {
        let mut camera = session(json!({"type":"sim","faults":{"failEveryNthCapture":2}})).await;
        let request = || CaptureRequest {
            capture_id: "cap".to_string(),
            profile: profile(),
            maximum_frame_bytes: 1_000_000,
            timeout: Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        };
        assert!(camera.capture(request()).await.is_ok());
        assert_eq!(
            camera.capture(request()).await.unwrap_err().code(),
            ErrorCode::BackendError
        );
    }

    #[tokio::test]
    async fn ptz_ranges_and_presets_are_enforced() {
        let mut camera = session(json!({"type":"sim","ptz":{"supported":true,"statusSupported":true,"presetsSupported":true}})).await;
        let token = match camera
            .ptz(PtzRequest::SetPreset("home-ish".to_string()))
            .await
            .unwrap()
        {
            PtzResult::PresetToken(token) => token,
            other => panic!("unexpected result: {other:?}"),
        };
        assert!(matches!(
            camera.ptz(PtzRequest::GotoPreset(token)).await.unwrap(),
            PtzResult::Commanded
        ));
        let invalid = PtzVector {
            pan: 2.0,
            tilt: 0.0,
            zoom: 0.0,
        };
        assert_eq!(
            camera
                .ptz(PtzRequest::Continuous {
                    velocity: invalid,
                    timeout: Duration::from_secs(1)
                })
                .await
                .unwrap_err()
                .code(),
            ErrorCode::PtzRangeError
        );
    }

    #[tokio::test]
    async fn cancellation_prevents_frame_allocation_completion() {
        let mut camera = session(json!({"type":"sim","captureDelayMs":100})).await;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = camera
            .capture(CaptureRequest {
                capture_id: "cap".to_string(),
                profile: profile(),
                maximum_frame_bytes: 1_000_000,
                timeout: Duration::from_secs(1),
                cancellation,
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::CaptureCancelled);
    }

    #[tokio::test]
    async fn disconnect_and_incomplete_faults_do_not_report_a_successful_frame() {
        let mut incomplete =
            session(json!({"type":"sim","faults":{"incompleteEveryNthCapture":1}})).await;
        let request = || CaptureRequest {
            capture_id: "cap-fault".to_string(),
            profile: profile(),
            maximum_frame_bytes: 1_000_000,
            timeout: Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        };
        assert!(matches!(
            incomplete.capture(request()).await.unwrap_err().code(),
            ErrorCode::BackendError | ErrorCode::DeviceUnavailable
        ));
        let mut disconnect =
            session(json!({"type":"sim","faults":{"disconnectAfterCaptures":0}})).await;
        assert_eq!(
            disconnect.capture(request()).await.unwrap_err().code(),
            ErrorCode::DeviceUnavailable
        );
        disconnect.close().await.unwrap();
        assert_eq!(
            disconnect.capture(request()).await.unwrap_err().code(),
            ErrorCode::DeviceUnavailable
        );
    }

    #[tokio::test]
    async fn factory_discovery_and_connection_failures_remain_bounded() {
        let factory = SimBackendFactory::new();
        assert_eq!(factory.kind(), BackendKind::Sim);
        assert!(
            factory
                .discover(DiscoveryRequest {
                    eligible_interfaces: vec!["camera-net".to_owned()],
                    timeout: Duration::from_millis(10),
                    max_results: 8,
                    cancellation: CancellationToken::new(),
                })
                .await
                .expect("simulator discovery is explicitly empty")
                .is_empty()
        );

        let wrong_backend = match factory
            .connect(ConnectRequest {
                instance_id: "cam-a".to_owned(),
                backend: backend(json!({
                    "type": "onvif-rtsp",
                    "deviceServiceUrl": "https://camera.example/onvif/device_service",
                    "mediaProfile": "main"
                })),
                timeout: Duration::from_secs(1),
                cancellation: CancellationToken::new(),
            })
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("a simulator factory cannot silently accept another backend config"),
        };
        assert_eq!(wrong_backend.code(), ErrorCode::BackendError);

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let cancelled = match factory
            .connect(ConnectRequest {
                instance_id: "cam-a".to_owned(),
                backend: backend(json!({"type": "sim", "connectDelayMs": 10})),
                timeout: Duration::from_secs(1),
                cancellation,
            })
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("cancelled connection must not create a live session"),
        };
        assert_eq!(cancelled.code(), ErrorCode::CaptureCancelled);
    }

    #[tokio::test]
    async fn simulator_emits_declared_raw_and_jpeg_formats_with_frame_bounds() {
        let request = || CaptureRequest {
            capture_id: "format-check".to_owned(),
            profile: profile(),
            maximum_frame_bytes: 1_000_000,
            timeout: Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        };
        for (pixel_format, expected_length) in [("Mono8", 12_usize), ("BGR8", 36_usize)] {
            let mut camera = session(json!({
                "type": "sim",
                "frame": {"width": 4, "height": 3, "pixelFormat": pixel_format, "pattern": "checkerboard"}
            }))
            .await;
            let frame = camera.capture(request()).await.expect("bounded raw frame");
            assert_eq!(frame.bytes.len(), expected_length);
            assert_eq!(frame.backend_metadata["captureId"], "format-check");
        }

        let mut jpeg = session(json!({
            "type": "sim",
            "frame": {"width": 16, "height": 8, "pixelFormat": "JPEG", "pattern": "color-bars"}
        }))
        .await;
        let frame = jpeg.capture(request()).await.expect("bounded JPEG frame");
        assert!(frame.bytes.starts_with(&[0xff, 0xd8]));
        assert!(frame.bytes.ends_with(&[0xff, 0xd9]));

        let mut oversized = session(json!({
            "type": "sim",
            "frame": {"width": 4, "height": 3, "pixelFormat": "RGB8"}
        }))
        .await;
        let error = oversized
            .capture(CaptureRequest {
                maximum_frame_bytes: 35,
                ..request()
            })
            .await
            .expect_err("frame ceiling is checked before allocation");
        assert_eq!(error.code(), ErrorCode::ResourceLimit);
    }

    #[tokio::test]
    async fn simulator_ptz_state_reports_motion_clamps_relative_moves_and_manages_presets() {
        let mut camera = session(json!({
            "type": "sim",
            "ptz": {"supported": true, "statusSupported": true, "presetsSupported": true}
        }))
        .await;
        let fast_pan = PtzVector {
            pan: 1.0,
            tilt: 0.0,
            zoom: 0.0,
        };
        assert!(matches!(
            camera
                .ptz(PtzRequest::Continuous {
                    velocity: fast_pan,
                    timeout: Duration::from_secs(1),
                })
                .await
                .expect("valid continuous PTZ command"),
            PtzResult::Commanded
        ));
        let moving = match camera.ptz(PtzRequest::Status).await.expect("PTZ status") {
            PtzResult::Status(status) => status,
            other => panic!("unexpected PTZ result: {other:?}"),
        };
        assert_eq!(moving.moving, Some(true));

        camera
            .ptz(PtzRequest::Absolute {
                position: PtzVector {
                    pan: 0.8,
                    tilt: -0.8,
                    zoom: 0.8,
                },
                speed: Some(PtzVector {
                    pan: 0.5,
                    tilt: 0.5,
                    zoom: 0.5,
                }),
            })
            .await
            .expect("valid absolute PTZ command");
        camera
            .ptz(PtzRequest::Relative {
                translation: PtzVector {
                    pan: 0.5,
                    tilt: -0.5,
                    zoom: 0.5,
                },
                speed: None,
            })
            .await
            .expect("valid relative PTZ command");
        let positioned = match camera.ptz(PtzRequest::Status).await.expect("PTZ status") {
            PtzResult::Status(status) => status,
            other => panic!("unexpected PTZ result: {other:?}"),
        };
        assert_eq!(
            positioned.position,
            Some(PtzVector {
                pan: 1.0,
                tilt: -1.0,
                zoom: 1.0,
            })
        );
        assert_eq!(positioned.moving, Some(false));

        let token = match camera
            .ptz(PtzRequest::SetPreset("production".to_owned()))
            .await
            .expect("preset mutation")
        {
            PtzResult::PresetToken(token) => token,
            other => panic!("unexpected PTZ result: {other:?}"),
        };
        assert_eq!(
            camera
                .ptz(PtzRequest::ListPresets)
                .await
                .expect("preset list"),
            PtzResult::Presets(vec![PtzPreset {
                token: token.clone(),
                name: Some("production".to_owned()),
            }])
        );
        assert_eq!(
            camera
                .ptz(PtzRequest::RemovePreset(token.clone()))
                .await
                .expect("existing preset removal"),
            PtzResult::Removed
        );
        assert_eq!(
            camera
                .ptz(PtzRequest::RemovePreset(token))
                .await
                .expect_err("removed preset cannot be removed twice")
                .code(),
            ErrorCode::BadArgs
        );
        camera.ptz(PtzRequest::Home).await.expect("home command");
        assert!(matches!(
            camera
                .ptz(PtzRequest::Stop {
                    pan: true,
                    tilt: true,
                    zoom: true,
                })
                .await
                .expect("stop command"),
            PtzResult::Commanded
        ));
    }

    #[tokio::test]
    async fn simulator_rejects_unsupported_ptz_operations_and_invalid_vectors() {
        let mut no_ptz = session(json!({"type": "sim"})).await;
        for request in [PtzRequest::Status, PtzRequest::ListPresets] {
            assert_eq!(
                no_ptz
                    .ptz(request)
                    .await
                    .expect_err("capability-gated PTZ request")
                    .code(),
                ErrorCode::UnsupportedCapability
            );
        }

        let mut ptz_without_status_or_presets = session(json!({
            "type": "sim",
            "ptz": {"supported": true, "statusSupported": false, "presetsSupported": false}
        }))
        .await;
        assert_eq!(
            ptz_without_status_or_presets
                .ptz(PtzRequest::Status)
                .await
                .expect_err("status capability is explicit")
                .code(),
            ErrorCode::UnsupportedCapability
        );
        assert_eq!(
            ptz_without_status_or_presets
                .ptz(PtzRequest::SetPreset("unavailable".to_owned()))
                .await
                .expect_err("preset mutation capability is explicit")
                .code(),
            ErrorCode::UnsupportedCapability
        );
        assert_eq!(
            ptz_without_status_or_presets
                .ptz(PtzRequest::Absolute {
                    position: PtzVector {
                        pan: 0.0,
                        tilt: 0.0,
                        zoom: -0.1,
                    },
                    speed: None,
                })
                .await
                .expect_err("absolute zoom must be non-negative")
                .code(),
            ErrorCode::PtzRangeError
        );
        assert_eq!(
            ptz_without_status_or_presets
                .ptz(PtzRequest::Relative {
                    translation: PtzVector {
                        pan: 0.0,
                        tilt: 0.0,
                        zoom: 0.0,
                    },
                    speed: Some(PtzVector {
                        pan: 2.0,
                        tilt: 0.0,
                        zoom: 0.0,
                    }),
                })
                .await
                .expect_err("relative speed shares the signed normalized range")
                .code(),
            ErrorCode::PtzRangeError
        );
    }

    // ---- playlist pattern -------------------------------------------------

    use std::fs;
    use tempfile::TempDir;

    /// A deterministic RGB8 buffer whose content depends on `tint`.
    fn rgb_pixels(width: u32, height: u32, tint: u8) -> Vec<u8> {
        (0..(width * height * 3))
            .map(|index| (index as u8).wrapping_mul(7).wrapping_add(tint))
            .collect()
    }

    fn jpeg_bytes(width: u32, height: u32, tint: u8) -> Vec<u8> {
        let mut bytes = Vec::new();
        JpegEncoder::new_with_quality(Cursor::new(&mut bytes), 92)
            .encode(
                &rgb_pixels(width, height, tint),
                width,
                height,
                ExtendedColorType::Rgb8,
            )
            .expect("the fixture encoder produces a JPEG");
        bytes
    }

    fn png_bytes(width: u32, height: u32, tint: u8) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut encoder = png::Encoder::new(Cursor::new(&mut bytes), width, height);
            encoder.set_color(png::ColorType::Rgb);
            encoder.set_depth(png::BitDepth::Eight);
            let mut writer = encoder.write_header().expect("PNG header");
            writer
                .write_image_data(&rgb_pixels(width, height, tint))
                .expect("PNG pixels");
        }
        bytes
    }

    fn grayscale_png_bytes(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut encoder = png::Encoder::new(Cursor::new(&mut bytes), width, height);
            encoder.set_color(png::ColorType::Grayscale);
            encoder.set_depth(png::BitDepth::Eight);
            let mut writer = encoder.write_header().expect("PNG header");
            writer
                .write_image_data(&vec![0x40_u8; (width * height) as usize])
                .expect("PNG pixels");
        }
        bytes
    }

    /// Writes one fixture file, creating whatever directories its relative path names.
    fn write_fixture(root: &Path, relative: &str, bytes: &[u8]) -> PathBuf {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("fixture parent directory");
        }
        fs::write(&path, bytes).expect("fixture file");
        path
    }

    /// A three-JPEG playlist directory: `a.jpg`, `b.jpg`, `nested/c.jpg`.
    fn three_jpeg_directory() -> TempDir {
        let directory = TempDir::new().expect("playlist directory");
        write_fixture(directory.path(), "a.jpg", &jpeg_bytes(8, 4, 1));
        write_fixture(directory.path(), "b.jpg", &jpeg_bytes(8, 4, 2));
        write_fixture(directory.path(), "nested/c.jpg", &jpeg_bytes(8, 4, 3));
        directory
    }

    fn playlist_settings(directory: &Path, overrides: serde_json::Value) -> SimPlaylistConfig {
        let mut value = json!({ "directory": directory.display().to_string() });
        let object = value.as_object_mut().expect("a playlist object");
        for (key, entry) in overrides.as_object().expect("overrides object") {
            object.insert(key.clone(), entry.clone());
        }
        serde_json::from_value(value).expect("valid playlist settings")
    }

    fn playlist_backend(directory: &Path, overrides: serde_json::Value) -> serde_json::Value {
        let mut playlist = json!({ "directory": directory.display().to_string() });
        let object = playlist.as_object_mut().expect("a playlist object");
        for (key, entry) in overrides.as_object().expect("overrides object") {
            object.insert(key.clone(), entry.clone());
        }
        json!({
            "type": "sim",
            "seed": 11,
            "captureDelayMs": 0,
            "frame": { "pattern": { "playlist": playlist } }
        })
    }

    fn encoding_profile(encoding: &str) -> CaptureProfile {
        serde_json::from_value(json!({ "output": { "encoding": encoding } }))
            .expect("valid capture profile")
    }

    fn playlist_request(capture_id: &str, encoding: &str) -> CaptureRequest {
        CaptureRequest {
            capture_id: capture_id.to_owned(),
            profile: encoding_profile(encoding),
            maximum_frame_bytes: 1_000_000,
            timeout: Duration::from_secs(1),
            cancellation: CancellationToken::new(),
        }
    }

    /// A connect attempt whose failure is the point.
    async fn connect_failure(value: serde_json::Value) -> CameraError {
        match SimBackendFactory::new()
            .connect(ConnectRequest {
                instance_id: "cam-a".to_string(),
                backend: backend(value),
                timeout: Duration::from_secs(1),
                cancellation: CancellationToken::new(),
            })
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("this configuration must not produce a live session"),
        }
    }

    #[test]
    fn include_globs_span_segments_bound_wildcards_and_stay_case_sensitive() {
        assert!(glob_matches("**/*.jpg", "a.jpg"));
        assert!(glob_matches("**/*.jpg", "one/two/a.jpg"));
        assert!(glob_matches("*.jpg", "a.jpg"));
        // A single `*` stops at a separator, which is the whole reason `**` exists.
        assert!(!glob_matches("*.jpg", "one/a.jpg"));
        assert!(glob_matches("one/*/b.png", "one/two/b.png"));
        assert!(!glob_matches("one/*/b.png", "one/two/three/b.png"));
        assert!(glob_matches("cam?/a.jpg", "cam1/a.jpg"));
        assert!(!glob_matches("cam?/a.jpg", "cam/a.jpg"));
        assert!(!glob_matches("**/*.jpg", "a.JPG"));
        assert!(glob_matches("**", "any/depth/at/all.png"));
        assert!(!glob_matches("", "a.jpg"));
    }

    #[test]
    fn a_playlist_takes_only_included_files_and_orders_them_by_relative_path() {
        let directory = TempDir::new().expect("playlist directory");
        write_fixture(directory.path(), "b.jpg", &jpeg_bytes(4, 4, 1));
        write_fixture(directory.path(), "a.png", &png_bytes(4, 4, 2));
        write_fixture(directory.path(), "nested/c.jpeg", &jpeg_bytes(4, 4, 3));
        write_fixture(directory.path(), "notes.txt", b"not an image");
        write_fixture(directory.path(), "nested/thumbnail.gif", b"not included");

        let playlist = Playlist::load(&playlist_settings(directory.path(), json!({})), 7)
            .expect("a playlist of the default image extensions");
        assert_eq!(
            playlist
                .entries
                .iter()
                .map(|entry| entry.relative.as_str())
                .collect::<Vec<_>>(),
            vec!["a.png", "b.jpg", "nested/c.jpeg"],
            "the default includes take jpg/jpeg/png at any depth and nothing else"
        );

        let narrowed = Playlist::load(
            &playlist_settings(directory.path(), json!({ "include": ["nested/**/*.jpeg"] })),
            7,
        )
        .expect("a narrowed playlist");
        assert_eq!(
            narrowed
                .entries
                .iter()
                .map(|entry| entry.relative.as_str())
                .collect::<Vec<_>>(),
            vec!["nested/c.jpeg"]
        );
    }

    #[test]
    fn a_seeded_order_is_a_stable_shuffle_of_the_sorted_order() {
        let directory = TempDir::new().expect("playlist directory");
        for index in 0..8 {
            write_fixture(
                directory.path(),
                &format!("frame-{index}.jpg"),
                &jpeg_bytes(4, 4, index as u8),
            );
        }
        let settings = playlist_settings(directory.path(), json!({ "order": "seeded" }));
        let sorted = Playlist::load(&playlist_settings(directory.path(), json!({})), 4_242)
            .expect("sorted playlist");
        let first = Playlist::load(&settings, 4_242).expect("seeded playlist");
        let again = Playlist::load(&settings, 4_242).expect("seeded playlist");
        let other_seed = Playlist::load(&settings, 99).expect("seeded playlist");

        assert_eq!(
            first.entries, again.entries,
            "the same seed must replay the same order, or a downstream fixture cannot be asserted"
        );
        assert_ne!(
            first.entries, sorted.entries,
            "a seeded order that equals the sorted order is not a shuffle"
        );
        assert_ne!(first.entries, other_seed.entries);
        let mut permuted = first.entries.clone();
        permuted.sort_by(|left, right| left.relative.cmp(&right.relative));
        assert_eq!(
            permuted, sorted.entries,
            "a shuffle reorders the playlist; it never adds or drops a file"
        );
    }

    #[test]
    fn a_looping_playlist_restarts_and_an_unlooped_one_refuses_after_its_last_file() {
        let directory = three_jpeg_directory();
        let mut looping = Playlist::load(&playlist_settings(directory.path(), json!({})), 1)
            .expect("looping playlist");
        assert_eq!(
            (0..5)
                .map(|_| looping.take().expect("a looping playlist never runs out"))
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 0, 1]
        );

        let mut once = Playlist::load(
            &playlist_settings(directory.path(), json!({ "loop": false })),
            1,
        )
        .expect("single-pass playlist");
        assert_eq!(
            (0..3)
                .map(|_| once.take().expect("every file is replayed once"))
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let spent = once.take().expect_err("a spent playlist has no frame");
        assert_eq!(spent.code(), ErrorCode::DeviceUnavailable);
        assert!(spent.to_string().contains("spent"));
        assert_eq!(
            once.diagnostics()["index"],
            3,
            "a spent playlist reports the end rather than pretending to be back at the start"
        );

        let mut wrapped = Playlist::load(&playlist_settings(directory.path(), json!({})), 1)
            .expect("looping playlist");
        for _ in 0..3 {
            wrapped.take().expect("every file replayed once");
        }
        assert_eq!(
            wrapped.diagnostics()["index"],
            0,
            "a looping playlist reports the file the next capture will replay"
        );
    }

    #[test]
    fn a_relative_token_refuses_a_path_that_is_not_plainly_nested() {
        let root = Path::new("/playlist");
        assert_eq!(
            relative_token(root, &root.join("nested").join("b.jpg")).as_deref(),
            Some("nested/b.jpg")
        );
        assert_eq!(
            relative_token(root, Path::new("/playlist/../b.jpg")),
            None,
            "a parent reference is how a relative path leaves the directory it is relative to"
        );
        assert_eq!(relative_token(root, Path::new("/elsewhere/b.jpg")), None);
    }

    #[test]
    fn the_playlist_walk_is_bounded_in_depth_and_in_file_count() {
        let directory = three_jpeg_directory();
        let include = vec!["**/*.jpg".to_string()];

        let mut entries = Vec::new();
        let too_deep = collect_playlist(
            directory.path(),
            directory.path(),
            &include,
            MAX_PLAYLIST_DEPTH + 1,
            &mut entries,
        )
        .expect_err("a tree deeper than the bound is refused rather than walked");
        assert_eq!(too_deep.code(), ErrorCode::DeviceUnavailable);
        assert!(too_deep.to_string().contains("nests deeper"));

        let mut already_full: Vec<PlaylistEntry> = (0..MAX_PLAYLIST_FILES)
            .map(|index| PlaylistEntry {
                relative: format!("{index}.jpg"),
                absolute: directory.path().join(format!("{index}.jpg")),
            })
            .collect();
        let too_many = collect_playlist(
            directory.path(),
            directory.path(),
            &include,
            0,
            &mut already_full,
        )
        .expect_err("a directory with more matching files than the bound is refused");
        assert!(too_many.to_string().contains("more than"));
    }

    #[tokio::test]
    async fn a_member_that_disappears_after_the_playlist_was_read_fails_only_its_capture() {
        let directory = TempDir::new().expect("playlist directory");
        write_fixture(directory.path(), "a.jpg", &jpeg_bytes(4, 4, 1));
        write_fixture(directory.path(), "b.jpg", &jpeg_bytes(4, 4, 2));
        let mut camera =
            session(playlist_backend(directory.path(), json!({ "loop": false }))).await;
        fs::remove_file(directory.path().join("a.jpg")).expect("remove the first member");

        let gone = camera
            .capture(playlist_request("cap-1", "passthrough"))
            .await
            .expect_err("a member that is no longer there is not a frame");
        assert_eq!(gone.code(), ErrorCode::DeviceUnavailable);
        assert!(gone.to_string().contains("cannot be read"));
        camera
            .capture(playlist_request("cap-2", "passthrough"))
            .await
            .expect("the rest of the playlist still replays");
    }

    #[tokio::test]
    async fn a_member_the_decoder_or_the_frame_ceiling_rejects_fails_its_capture() {
        let directory = TempDir::new().expect("playlist directory");
        // Intact PNG signature, ruined payload: the format is recognized and the decode still fails.
        let mut corrupt = png_bytes(4, 4, 1);
        corrupt.truncate(40);
        write_fixture(directory.path(), "a.png", &corrupt);
        write_fixture(directory.path(), "b.png", &grayscale_png_bytes(128, 128));

        let mut camera = session(playlist_backend(directory.path(), json!({}))).await;
        let undecodable = camera
            .capture(playlist_request("cap-1", "png"))
            .await
            .expect_err("a truncated PNG is not a frame");
        assert_eq!(undecodable.code(), ErrorCode::UnsupportedPixelFormat);
        assert!(undecodable.to_string().contains("cannot be decoded"));

        // A compressed file can sit well inside the ceiling that its pixels blow straight through,
        // which is why the decoded buffer is measured as well as the file.
        let over_ceiling = camera
            .capture(CaptureRequest {
                maximum_frame_bytes: 4_096,
                ..playlist_request("cap-2", "png")
            })
            .await
            .expect_err("the decoded frame is bounded too");
        assert_eq!(over_ceiling.code(), ErrorCode::ResourceLimit);
        assert!(over_ceiling.to_string().contains("decodes past"));
    }

    #[tokio::test]
    async fn a_replayed_jpeg_reaches_the_pipeline_as_the_bytes_on_disk() {
        let directory = TempDir::new().expect("playlist directory");
        let first = jpeg_bytes(16, 12, 5);
        let second = jpeg_bytes(8, 8, 9);
        write_fixture(directory.path(), "a.jpg", &first);
        write_fixture(directory.path(), "b.jpg", &second);

        let mut camera = session(playlist_backend(directory.path(), json!({}))).await;
        let frame = camera
            .capture(playlist_request("cap-1", "passthrough"))
            .await
            .expect("the first playlist file");
        assert_eq!(
            frame.bytes.as_ref(),
            first.as_slice(),
            "a passthrough capture must install the file byte for byte, or its sha256 is not the \
             digest of the image it claims to have replayed"
        );
        assert_eq!((frame.width, frame.height), (16, 12));
        assert_eq!(frame.pixel_format, PixelFormat::Jpeg);
        assert_eq!(frame.capture_mode, CaptureMode::Simulated);
        assert_eq!(frame.backend_metadata["playlist"]["sourcePath"], "a.jpg");
        assert_eq!(frame.backend_metadata["playlist"]["index"], 0);

        let next = camera
            .capture(playlist_request("cap-2", "passthrough"))
            .await
            .expect("the second playlist file");
        assert_eq!(next.bytes.as_ref(), second.as_slice());
        assert_eq!((next.width, next.height), (8, 8));
        assert_eq!(next.backend_metadata["playlist"]["sourcePath"], "b.jpg");
        assert_eq!(next.backend_metadata["playlist"]["index"], 1);
    }

    #[tokio::test]
    async fn a_replayed_file_is_decoded_when_the_profile_asks_for_a_re_encode() {
        let directory = TempDir::new().expect("playlist directory");
        write_fixture(directory.path(), "colour.png", &png_bytes(6, 5, 3));
        write_fixture(directory.path(), "grey.png", &grayscale_png_bytes(6, 5));
        write_fixture(directory.path(), "photo.jpg", &jpeg_bytes(6, 5, 4));

        let mut camera = session(playlist_backend(directory.path(), json!({}))).await;
        let colour = camera
            .capture(playlist_request("cap-1", "png"))
            .await
            .expect("a colour PNG decodes to RGB8");
        assert_eq!(colour.pixel_format, PixelFormat::Rgb8);
        assert_eq!((colour.width, colour.height), (6, 5));
        assert_eq!(colour.bytes.len(), 6 * 5 * 3);

        let grey = camera
            .capture(playlist_request("cap-2", "png"))
            .await
            .expect("a grayscale PNG decodes to Mono8");
        assert_eq!(grey.pixel_format, PixelFormat::Mono8);
        assert_eq!(grey.bytes.len(), 6 * 5);

        let photo = camera
            .capture(playlist_request("cap-3", "png"))
            .await
            .expect("a JPEG asked for as PNG is decoded rather than refused");
        assert_eq!(photo.pixel_format, PixelFormat::Rgb8);
        assert_eq!(photo.bytes.len(), 6 * 5 * 3);
        assert_eq!(
            photo.backend_metadata["playlist"]["sourcePath"],
            "photo.jpg"
        );
    }

    #[tokio::test]
    async fn playlist_capabilities_and_status_describe_the_replay() {
        let directory = three_jpeg_directory();
        let mut camera = session(playlist_backend(directory.path(), json!({}))).await;
        assert_eq!(
            camera.capabilities().pixel_formats,
            vec![PixelFormat::Jpeg, PixelFormat::Rgb8, PixelFormat::Mono8],
            "a playlist reports what a replayed file is, not what the generator was configured to emit"
        );

        let before = camera.status().await.expect("session status");
        assert_eq!(before.backend["playlist"]["count"], 3);
        assert_eq!(before.backend["playlist"]["index"], 0);
        assert_eq!(
            before.backend["playlist"]["directory"],
            directory.path().display().to_string(),
            "diagnostics name the directory as it was configured"
        );

        camera
            .capture(playlist_request("cap-1", "passthrough"))
            .await
            .expect("one replayed capture");
        let after = camera.status().await.expect("session status");
        assert_eq!(after.backend["playlist"]["index"], 1);

        let synthetic = session(json!({"type": "sim"})).await;
        assert!(
            synthetic
                .capabilities()
                .pixel_formats
                .contains(&PixelFormat::Rgb8)
        );
    }

    #[tokio::test]
    async fn a_playlist_that_can_never_produce_a_frame_refuses_to_connect() {
        let empty = TempDir::new().expect("playlist directory");
        write_fixture(empty.path(), "notes.txt", b"not an image");
        let no_match = connect_failure(playlist_backend(empty.path(), json!({}))).await;
        assert_eq!(no_match.code(), ErrorCode::DeviceUnavailable);
        assert!(no_match.to_string().contains("no file matching"));

        let missing = empty.path().join("absent");
        let unopenable = connect_failure(playlist_backend(&missing, json!({}))).await;
        assert_eq!(unopenable.code(), ErrorCode::DeviceUnavailable);
    }

    #[tokio::test]
    async fn a_playlist_file_the_pipeline_cannot_accept_fails_the_capture_not_the_session() {
        let directory = TempDir::new().expect("playlist directory");
        write_fixture(directory.path(), "a.jpg", &jpeg_bytes(32, 32, 1));
        write_fixture(directory.path(), "b.jpg", b"\xff\xd8\xffnot really a JPEG");
        write_fixture(directory.path(), "c.png", &png_bytes(4, 4, 2));

        let mut camera = session(playlist_backend(directory.path(), json!({}))).await;
        let oversized = camera
            .capture(CaptureRequest {
                maximum_frame_bytes: 16,
                ..playlist_request("cap-1", "passthrough")
            })
            .await
            .expect_err("the frame ceiling is checked before the file is read");
        assert_eq!(oversized.code(), ErrorCode::ResourceLimit);

        let undecodable = camera
            .capture(playlist_request("cap-2", "passthrough"))
            .await
            .expect_err("a truncated JPEG is not a frame");
        assert_eq!(undecodable.code(), ErrorCode::UnsupportedPixelFormat);

        let still_serving = camera
            .capture(playlist_request("cap-3", "png"))
            .await
            .expect("a refused file does not close the session");
        assert_eq!(still_serving.backend_metadata["playlist"]["index"], 2);
    }

    #[tokio::test]
    async fn a_file_that_is_no_image_at_all_is_refused_with_the_format_code() {
        let directory = TempDir::new().expect("playlist directory");
        write_fixture(directory.path(), "a.jpg", b"GIF89a and not a JPEG");
        let mut camera = session(playlist_backend(directory.path(), json!({}))).await;
        let error = camera
            .capture(playlist_request("cap-1", "passthrough"))
            .await
            .expect_err("an extension is not evidence of a format");
        assert_eq!(error.code(), ErrorCode::UnsupportedPixelFormat);
        assert!(error.to_string().contains("neither JPEG nor PNG"));
    }

    /// A playlist may not name a file outside its directory, and a link is how that happens.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_symbolic_link_is_refused_at_load_and_at_capture() {
        let outside = TempDir::new().expect("directory outside the playlist");
        let secret = write_fixture(outside.path(), "elsewhere.jpg", &jpeg_bytes(4, 4, 1));
        let directory = TempDir::new().expect("playlist directory");
        write_fixture(directory.path(), "a.jpg", &jpeg_bytes(4, 4, 2));
        std::os::unix::fs::symlink(&secret, directory.path().join("linked.jpg"))
            .expect("the fixture link");

        let refused = connect_failure(playlist_backend(directory.path(), json!({}))).await;
        assert_eq!(refused.code(), ErrorCode::DeviceUnavailable);
        assert!(refused.to_string().contains("symbolic link"));

        // The same check runs again at capture time, because the directory is a live filesystem and
        // a member can be swapped for a link between connect and capture.
        fs::remove_file(directory.path().join("linked.jpg")).expect("remove the fixture link");
        let mut camera = session(playlist_backend(directory.path(), json!({}))).await;
        fs::remove_file(directory.path().join("a.jpg")).expect("remove the member");
        std::os::unix::fs::symlink(&secret, directory.path().join("a.jpg"))
            .expect("swap the member for a link");
        let swapped = camera
            .capture(playlist_request("cap-1", "passthrough"))
            .await
            .expect_err("a member replaced by a link is not replayed");
        assert_eq!(swapped.code(), ErrorCode::DeviceUnavailable);
        assert!(swapped.to_string().contains("symbolic link"));
    }
}
