//! The optional, opt-in thumbnail of a captured frame.
//!
//! A thumbnail is a **convenience**, and it is treated like one everywhere in this module: it is off
//! unless a capture profile asks for it, it is derived from the camera's frame rather than from the
//! file the frame was written to, and nothing it can do -- an undecodable frame, an encoder that
//! refuses, a picture that will not fit -- is ever allowed to harm the capture it belongs to. Every
//! failure path here ends in a [`ThumbnailOutcome`] the caller can announce *without*, never in an
//! error the caller must propagate.
//!
//! Two bounds shape the rest:
//!
//! * **The longest edge.** Cameras are 4:3 and 16:9, so a fixed width x height would either distort
//!   the picture or letterbox it. The configured [`ThumbnailSize`] bounds the LONGEST edge and the
//!   other axis follows, and a frame already smaller than the bound is carried at its own size --
//!   never upscaled, because inventing pixels is not a preview.
//! * **The byte budget, which belongs to the TRANSPORT.** A preview rides inside the terminal
//!   announcement, so what it may cost is whatever the transport the component actually resolved can
//!   carry -- and the two transports are not close. Greengrass IPC caps a whole message at 10,000
//!   bytes inside our own client library; an MQTT broker takes a megabyte. [`ThumbnailPolicy`] is
//!   that answer, derived from the resolved [`Transport`]: it caps the SIZE (a profile asking for
//!   more is clamped down, never rejected) and the BYTES (the quality ladder encodes down to it, and
//!   a picture that still will not fit is dropped rather than the message).
//!
//! The second bound is not theoretical. A previous cut of this feature offered `medium` and `large`
//! previews to Greengrass IPC and lost 90 announcements to NOMEM on the lab device -- every capture
//! succeeded, every image landed, and nobody was told about any of them.

use std::io::Cursor;

use image::codecs::jpeg::{JpegDecoder, JpegEncoder};
use image::imageops::FilterType;
use image::{
    DynamicImage, ExtendedColorType, GenericImageView, ImageBuffer, ImageDecoder, Luma, Pixel, Rgb,
    imageops,
};

use edgecommons::platform::Transport;

use crate::config::ThumbnailSize;
use crate::messages::Thumbnail;
use crate::model::{CaptureFrame, PixelFormat};

/// What the Greengrass IPC transport can carry, in bytes of encoded preview.
///
/// The binding constraint is NOT a Greengrass protocol limit and NOT the Java nucleus. It is the IPC
/// client this component links: `aws-greengrass-component-sdk` encodes the whole eventstream packet
/// into a **static 10,000-byte buffer** (`GG_IPC_MAX_MSG_LEN`, `csrc/ipc/client.c`), and
/// `eventstream_encode` answers NOMEM above it -- inside our own process, before a byte reaches the
/// nucleus. The packet must also hold the envelope (the largest terminal body measured on the lab
/// device was 1,677 bytes), the protobuf and eventstream headers, and the topic. 6 KiB leaves real
/// margin inside 10,000, and `small` at this budget carried 45/45 captures on the device.
const IPC_BUDGET_BYTES: usize = 6 * 1024;

/// What the MQTT transport can carry, in bytes of encoded preview.
///
/// A broker takes a megabyte without blinking, so the binding constraint here is the messaging
/// library's 64 KiB cap on a single binary value -- not the transport. This leaves that cap room for
/// the rest of the envelope.
const MQTT_BUDGET_BYTES: usize = 60 * 1024;

/// The MQTT budget must stay under the library's binary-value bound, or a thumbnail could fail an
/// announcement to BUILD -- a failure no transport is involved in and no retry could survive.
///
/// Asserted at COMPILE time, because it is the one relationship here that nothing at runtime would
/// notice being wrong until an announcement was already lost.
const _: () = assert!(
    MQTT_BUDGET_BYTES < edgecommons::messaging::message::MAX_BINARY_BODY_BYTES,
    "the thumbnail budget must leave the messaging library's binary-value bound room for the rest \
     of the envelope"
);

/// What the resolved transport can actually carry.
///
/// # Why this exists
///
/// A thumbnail was supposed to be unable to harm a capture. On the lab device it harmed 90 of them:
/// every capture SUCCEEDED and every image landed on disk, but on Greengrass IPC the `medium` and
/// `large` previews made the announcement itself undeliverable -- 45/45 NOMEM on each of two
/// cameras. The result was durable and nobody was told about it, which is a real loss dressed up as
/// a convenience feature.
///
/// The engine sheds a preview that costs an announcement (see `JobEngine::announce_terminal`), and
/// that safety net stays -- but discovering a transport's limit by FAILING against it is not a
/// policy. This is the policy: the component asks what the transport it actually resolved can carry,
/// and never offers it more than that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThumbnailPolicy {
    transport: Transport,
    largest: ThumbnailSize,
    budget: usize,
}

impl ThumbnailPolicy {
    /// The policy for one resolved transport.
    ///
    /// The transport is the one the component actually started with (`gg.args().transport`, already
    /// resolved from `--platform`/`--transport`/auto) -- not a guess from the config file.
    #[must_use]
    pub const fn for_transport(transport: Transport) -> Self {
        match transport {
            Transport::Ipc => Self {
                transport,
                largest: ThumbnailSize::Small,
                budget: IPC_BUDGET_BYTES,
            },
            Transport::Mqtt => Self {
                transport,
                largest: ThumbnailSize::Large,
                budget: MQTT_BUDGET_BYTES,
            },
        }
    }

    /// The transport this policy was derived from.
    #[must_use]
    pub const fn transport(self) -> Transport {
        self.transport
    }

    /// The largest size this transport can carry.
    #[must_use]
    pub const fn largest_size(self) -> ThumbnailSize {
        self.largest
    }

    /// The byte budget for one encoded preview on this transport.
    #[must_use]
    pub const fn budget_bytes(self) -> usize {
        self.budget
    }

    /// The size actually produced for a profile that asked for `requested`.
    ///
    /// A size the transport cannot carry is CLAMPED DOWN, never rejected. The same configuration is
    /// deployed to Greengrass and to Kubernetes, and refusing to start on one of them because a
    /// preview is too big for its transport would be a hostile way to report a convenience.
    #[must_use]
    pub const fn effective_size(self, requested: ThumbnailSize) -> ThumbnailSize {
        if requested.longest_edge() > self.largest.longest_edge() {
            self.largest
        } else {
            requested
        }
    }

    /// Whether `requested` is larger than this transport can carry.
    #[must_use]
    pub const fn clamps(self, requested: ThumbnailSize) -> bool {
        requested.longest_edge() > self.largest.longest_edge()
    }

    /// Why this transport caps the preview, in words an operator can act on.
    #[must_use]
    pub const fn limit_reason(self) -> &'static str {
        match self.transport {
            Transport::Ipc => {
                "the Greengrass IPC client caps a whole message at 10,000 bytes (GG_IPC_MAX_MSG_LEN)"
            }
            Transport::Mqtt => "the messaging library caps one binary value at 64 KiB",
        }
    }
}

/// One camera whose configured preview size the resolved transport cannot carry.
///
/// Produced ONCE, from the configuration, at startup and on reload -- never per capture. A clamp is
/// a fact about the deployment, not an event; logging it 45 times per camera per hour would bury the
/// one line that tells an operator why their `large` previews are arriving at 160 px.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClampNotice {
    /// The camera instance whose profiles were clamped.
    pub instance: String,
    /// The capture profiles that asked for more than the transport can carry.
    pub profiles: Vec<String>,
    /// The size that is produced instead.
    pub effective: ThumbnailSize,
}

/// Every camera whose configured preview size this transport must clamp -- one notice per camera.
///
/// The caller logs these at WARN, once, at startup and after a reload. Nothing else in the component
/// reports a clamp, and the per-capture path ([`render`]) deliberately cannot: it is handed the
/// policy and returns a picture, with no way to say a word about it.
#[must_use]
pub fn clamp_notices(config: &crate::config::AdapterConfig, policy: ThumbnailPolicy) -> Vec<ClampNotice> {
    let mut notices = Vec::new();
    for camera in &config.instances {
        let mut profiles: Vec<String> = camera
            .capture_profiles
            .iter()
            .filter(|(_, profile)| {
                profile
                    .thumbnail
                    .is_some_and(|thumbnail| policy.clamps(thumbnail.size))
            })
            .map(|(name, _)| name.clone())
            .collect();
        if profiles.is_empty() {
            continue;
        }
        profiles.sort();
        notices.push(ClampNotice {
            instance: camera.id.clone(),
            profiles,
            effective: policy.largest_size(),
        });
    }
    notices
}

/// The JPEG qualities tried, in order, until one fits the policy's byte budget.
///
/// This is the whole "quality" surface, and it is deliberately not configurable: the component owns
/// the trade between fidelity and the byte budget, because the budget is not negotiable -- it is
/// whatever the transport can actually carry.
pub const QUALITY_LADDER: [u8; 3] = [80, 65, 50];

/// The one resampling filter. Fixed so that the same frame always yields the same thumbnail.
const FILTER: FilterType = FilterType::Lanczos3;

/// What rendering one thumbnail produced.
///
/// There is no error variant that a caller could propagate, on purpose: every outcome here is one
/// the capture survives.
#[derive(Debug, Clone, PartialEq)]
pub enum ThumbnailOutcome {
    /// A thumbnail that fits the transport's budget and can be announced.
    Rendered(Thumbnail),
    /// The frame rendered, but the smallest encoding still exceeded the transport's byte budget.
    ///
    /// Carries the smallest encoded size that was tried, which is what an operator needs in order to
    /// understand why a configured thumbnail is not arriving.
    Dropped {
        /// The smallest encoded size the quality ladder achieved, in bytes.
        bytes: usize,
    },
    /// The frame could not be interpreted or encoded at all.
    Failed {
        /// Operator-safe reason, free of paths, endpoints, and credentials.
        reason: String,
    },
}

/// Renders one thumbnail of `frame`, bounded by what `policy` says the transport can carry.
///
/// `requested` is what the capture profile asked for. What is produced is
/// [`ThumbnailPolicy::effective_size`] of it -- clamped down, silently and per capture, because the
/// operator was already told once at startup (see [`clamp_notices`]) and a WARN per capture would
/// only bury it.
///
/// The source is the camera's frame, so the thumbnail shows the same picture the artifact is made
/// of. It is nevertheless a LOSSY re-encode of it and carries no digest: see [`Thumbnail`].
///
/// `maximum_frame_bytes` is the capture's own admitted frame ceiling
/// (`captureProfiles.*.maximumFrameBytes`), and it bounds how large a picture this may DECODE. It
/// matters only for a JPEG frame, and it matters a great deal: a JPEG's decoded size is not its file
/// size, and a 653-byte file whose header declares 65500x65500 decodes to 12.8 GB. Nothing else in
/// the capture pipeline ever decodes a camera's JPEG -- `passthrough` copies its bytes and the
/// encoder refuses to re-encode it -- so this is the one place a camera's header could be believed
/// about how much memory to reserve. It is not believed past the ceiling the capture was admitted
/// with, and the decode is refused before a byte of it is allocated.
///
/// How many bytes rendering a thumbnail from this frame will ask the allocator for, or `None` when
/// it will ask for nothing worth reserving.
///
/// Only a JPEG is ever decoded here. The raw formats are scaled through a *view* over bytes the
/// capture already holds, and allocate nothing but the small scaled output. A JPEG has to be expanded
/// into pixels first, and its decoded size is emphatically not its file size -- which is exactly why
/// this allocation is the one the byte budget could not see.
///
/// The answer comes from the JPEG HEADER, before a single pixel is allocated. That is the same source
/// the decode-bomb guard in [`decode`] already trusts, and the reason the memory can be reserved
/// *before* it is spent rather than discovered afterwards. A header this component cannot parse
/// yields `None`: the render will fail on it in a moment anyway, and it will fail without allocating.
#[must_use]
pub(crate) fn declared_decode_bytes(frame: &CaptureFrame) -> Option<u64> {
    if frame.pixel_format != PixelFormat::Jpeg {
        return None;
    }
    let decoder = JpegDecoder::new(Cursor::new(frame.bytes.as_ref())).ok()?;
    Some(decoder.total_bytes())
}

/// This is CPU-bound and must be called from a blocking context (the capture pipeline calls it
/// inside the same `spawn_blocking` as the encoder, under the same admission permits).
///
/// # Panics
/// Never. Every failure is a [`ThumbnailOutcome`].
#[must_use]
pub fn render(
    frame: &CaptureFrame,
    requested: ThumbnailSize,
    policy: ThumbnailPolicy,
    maximum_frame_bytes: u64,
) -> ThumbnailOutcome {
    let size = policy.effective_size(requested);
    let pixels = match decode(frame, size.longest_edge(), maximum_frame_bytes) {
        Ok(pixels) => pixels,
        Err(reason) => return ThumbnailOutcome::Failed { reason },
    };
    let (width, height) = pixels.dimensions();

    let mut smallest = usize::MAX;
    for quality in QUALITY_LADDER {
        let encoded = match encode(&pixels, quality) {
            Ok(encoded) => encoded,
            Err(reason) => return ThumbnailOutcome::Failed { reason },
        };
        smallest = smallest.min(encoded.len());
        if encoded.len() <= policy.budget_bytes() {
            return match Thumbnail::new(width, height, &encoded) {
                Ok(thumbnail) => ThumbnailOutcome::Rendered(thumbnail),
                Err(error) => ThumbnailOutcome::Failed {
                    reason: error.to_string(),
                },
            };
        }
    }
    ThumbnailOutcome::Dropped { bytes: smallest }
}

/// A decoded, already-downscaled thumbnail image.
///
/// Grayscale is kept grayscale rather than promoted to RGB: a Mono8 camera's preview is smaller and
/// no less true for staying single-channel.
enum Pixels {
    /// Single-channel preview of a Mono8 (or grayscale JPEG) frame.
    Gray(ImageBuffer<Luma<u8>, Vec<u8>>),
    /// Three-channel preview of an RGB8/BGR8/colour-JPEG frame.
    Color(ImageBuffer<Rgb<u8>, Vec<u8>>),
}

impl Pixels {
    fn dimensions(&self) -> (u32, u32) {
        match self {
            Self::Gray(buffer) => (buffer.width(), buffer.height()),
            Self::Color(buffer) => (buffer.width(), buffer.height()),
        }
    }
}

/// Interprets the frame's declared format and downscales it, without ever materialising a
/// full-resolution copy of an uncompressed frame -- or decoding a compressed one past its ceiling.
fn decode(frame: &CaptureFrame, bound: u32, maximum_frame_bytes: u64) -> Result<Pixels, String> {
    if frame.width == 0 || frame.height == 0 {
        return Err("frame has no dimensions".to_string());
    }
    let (width, height) = target(frame.width, frame.height, bound);
    match frame.pixel_format {
        PixelFormat::Mono8 => {
            let view = GrayView::new(frame, maximum_frame_bytes)?;
            Ok(Pixels::Gray(scale(&view, width, height)))
        }
        PixelFormat::Rgb8 => {
            let view = RgbView::new(frame, maximum_frame_bytes)?;
            Ok(Pixels::Color(scale(&view, width, height)))
        }
        PixelFormat::Bgr8 => {
            let view = BgrView::new(frame, maximum_frame_bytes)?;
            Ok(Pixels::Color(scale(&view, width, height)))
        }
        PixelFormat::Jpeg => {
            // The header, and ONLY the header, so far. `total_bytes` is the exact size the picture
            // would decode to, and it is available before a single pixel is allocated.
            let decoder = JpegDecoder::new(Cursor::new(frame.bytes.as_ref()))
                .map_err(|error| format!("declared JPEG is not decodable: {error}"))?;
            let decoded_bytes = decoder.total_bytes();
            if decoded_bytes > maximum_frame_bytes {
                // A camera's JPEG header is not a promise. Believing this one would mean allocating
                // whatever it asked for -- gigabytes, from a file of a few hundred bytes -- and an
                // OOM is the one way a thumbnail could take the whole component down with it.
                return Err(format!(
                    "declared JPEG would decode to {decoded_bytes} bytes, over the capture's \
                     {maximum_frame_bytes}-byte frame ceiling"
                ));
            }
            let decoded = DynamicImage::from_decoder(decoder)
                .map_err(|error| format!("declared JPEG could not be decoded: {error}"))?;
            match decoded {
                DynamicImage::ImageLuma8(buffer) => Ok(Pixels::Gray(scale(&buffer, width, height))),
                other => Ok(Pixels::Color(scale(&other.to_rgb8(), width, height))),
            }
        }
    }
}

/// The thumbnail's dimensions: the longest edge bounded, the aspect ratio preserved, never upscaled.
fn target(width: u32, height: u32, bound: u32) -> (u32, u32) {
    let longest = width.max(height);
    if longest <= bound {
        // A frame smaller than the bound is carried at its own size. Upscaling would invent pixels
        // the camera never produced, and a bigger blurry picture is not a better preview.
        return (width, height);
    }
    let scaled = |value: u32| -> u32 {
        let numerator = u64::from(value) * u64::from(bound) + u64::from(longest) / 2;
        let value = (numerator / u64::from(longest)) as u32;
        // The short edge of an extreme aspect ratio must still be a pixel, or there is no image.
        value.max(1)
    };
    if width >= height {
        (bound, scaled(height))
    } else {
        (scaled(width), bound)
    }
}

/// Resamples `source` to exactly `width` x `height`, or copies it when it is already that size.
///
/// The already-that-size case is not an optimisation: running an unscaled image through a resampling
/// filter would perturb every pixel of a frame nobody asked to change.
fn scale<P, I>(source: &I, width: u32, height: u32) -> ImageBuffer<P, Vec<u8>>
where
    P: Pixel<Subpixel = u8> + 'static,
    I: GenericImageView<Pixel = P>,
{
    if source.dimensions() == (width, height) {
        let mut buffer = ImageBuffer::new(width, height);
        for (x, y, pixel) in source.pixels() {
            buffer.put_pixel(x, y, pixel);
        }
        return buffer;
    }
    imageops::resize(source, width, height, FILTER)
}

/// Encodes the downscaled image as a JPEG at one quality from the ladder.
fn encode(pixels: &Pixels, quality: u8) -> Result<Vec<u8>, String> {
    let mut encoded = Vec::new();
    let mut encoder = JpegEncoder::new_with_quality(Cursor::new(&mut encoded), quality);
    let result = match pixels {
        Pixels::Gray(buffer) => encoder.encode(
            buffer.as_raw(),
            buffer.width(),
            buffer.height(),
            ExtendedColorType::L8,
        ),
        Pixels::Color(buffer) => encoder.encode(
            buffer.as_raw(),
            buffer.width(),
            buffer.height(),
            ExtendedColorType::Rgb8,
        ),
    };
    result.map_err(|error| format!("thumbnail JPEG encoding failed: {error}"))?;
    Ok(encoded)
}

/// Checks that an uncompressed frame is exactly as long as its declared format and size require,
/// and that it is within the ceiling the capture was admitted with.
fn declared_len(frame: &CaptureFrame, maximum_frame_bytes: u64) -> Result<(), String> {
    let expected = frame
        .pixel_format
        .uncompressed_len(frame.width, frame.height)
        .ok_or_else(|| "frame dimensions overflow the byte-count domain".to_string())?;
    let actual = frame.bytes.len() as u64;
    if expected != actual {
        return Err(format!(
            "frame declares {}x{} but carries {actual} bytes, not {expected}",
            frame.width, frame.height
        ));
    }
    if expected > maximum_frame_bytes {
        return Err(format!(
            "frame is {expected} bytes, over the capture's {maximum_frame_bytes}-byte frame ceiling"
        ));
    }
    Ok(())
}

/// Borrowed Mono8 frame. Zero-copy: an uncompressed frame is never duplicated to be downscaled.
struct GrayView<'a> {
    bytes: &'a [u8],
    width: u32,
    height: u32,
}

impl<'a> GrayView<'a> {
    fn new(frame: &'a CaptureFrame, maximum_frame_bytes: u64) -> Result<Self, String> {
        declared_len(frame, maximum_frame_bytes)?;
        Ok(Self {
            bytes: frame.bytes.as_ref(),
            width: frame.width,
            height: frame.height,
        })
    }
}

impl GenericImageView for GrayView<'_> {
    type Pixel = Luma<u8>;

    fn dimensions(&self) -> (u32, u32) {
        (self.width, self.height)
    }

    fn get_pixel(&self, x: u32, y: u32) -> Self::Pixel {
        let offset = (y as usize * self.width as usize) + x as usize;
        Luma([self.bytes[offset]])
    }
}

/// Borrowed RGB8 frame.
struct RgbView<'a> {
    bytes: &'a [u8],
    width: u32,
    height: u32,
}

impl<'a> RgbView<'a> {
    fn new(frame: &'a CaptureFrame, maximum_frame_bytes: u64) -> Result<Self, String> {
        declared_len(frame, maximum_frame_bytes)?;
        Ok(Self {
            bytes: frame.bytes.as_ref(),
            width: frame.width,
            height: frame.height,
        })
    }
}

impl GenericImageView for RgbView<'_> {
    type Pixel = Rgb<u8>;

    fn dimensions(&self) -> (u32, u32) {
        (self.width, self.height)
    }

    fn get_pixel(&self, x: u32, y: u32) -> Self::Pixel {
        let offset = ((y as usize * self.width as usize) + x as usize) * 3;
        Rgb([
            self.bytes[offset],
            self.bytes[offset + 1],
            self.bytes[offset + 2],
        ])
    }
}

/// Borrowed BGR8 frame, presented as RGB.
struct BgrView<'a> {
    bytes: &'a [u8],
    width: u32,
    height: u32,
}

impl<'a> BgrView<'a> {
    fn new(frame: &'a CaptureFrame, maximum_frame_bytes: u64) -> Result<Self, String> {
        declared_len(frame, maximum_frame_bytes)?;
        Ok(Self {
            bytes: frame.bytes.as_ref(),
            width: frame.width,
            height: frame.height,
        })
    }
}

impl GenericImageView for BgrView<'_> {
    type Pixel = Rgb<u8>;

    fn dimensions(&self) -> (u32, u32) {
        (self.width, self.height)
    }

    fn get_pixel(&self, x: u32, y: u32) -> Self::Pixel {
        let offset = ((y as usize * self.width as usize) + x as usize) * 3;
        Rgb([
            self.bytes[offset + 2],
            self.bytes[offset + 1],
            self.bytes[offset],
        ])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use image::ImageReader;

    use super::*;
    use crate::model::{CaptureMode, FrameTimestampQuality};

    /// A generous stand-in for one capture's `maximumFrameBytes`, big enough for every fixture here.
    const CEILING: u64 = 8 * 1024 * 1024;

    /// The permissive transport: an MQTT broker, which carries all three sizes.
    fn mqtt() -> ThumbnailPolicy {
        ThumbnailPolicy::for_transport(Transport::Mqtt)
    }

    /// The strict one: Greengrass IPC, whose client caps a whole message at 10,000 bytes.
    fn ipc() -> ThumbnailPolicy {
        ThumbnailPolicy::for_transport(Transport::Ipc)
    }

    fn frame(format: PixelFormat, bytes: Vec<u8>, width: u32, height: u32) -> CaptureFrame {
        CaptureFrame {
            bytes: Bytes::from(bytes),
            width,
            height,
            pixel_format: format,
            capture_mode: CaptureMode::Simulated,
            source_timestamp: None,
            timestamp_quality: FrameTimestampQuality::Camera,
            backend_metadata: BTreeMap::new(),
        }
    }

    /// An RGB8 frame whose pixels vary with position, so a resize cannot be faked by a flat colour.
    fn rgb(width: u32, height: u32) -> CaptureFrame {
        let mut bytes = Vec::with_capacity((width * height * 3) as usize);
        for y in 0..height {
            for x in 0..width {
                bytes.extend_from_slice(&[(x % 256) as u8, (y % 256) as u8, ((x + y) % 256) as u8]);
            }
        }
        frame(PixelFormat::Rgb8, bytes, width, height)
    }

    fn rendered(outcome: ThumbnailOutcome) -> Thumbnail {
        match outcome {
            ThumbnailOutcome::Rendered(thumbnail) => thumbnail,
            other => panic!("the frame must have produced a thumbnail, got {other:?}"),
        }
    }

    /// The bound is the LONGEST edge, and the other axis follows it -- for both camera aspects.
    ///
    /// Cameras are 4:3 AND 16:9. Fixing width x height would distort one of them or letterbox it, so
    /// the contract is the longest edge; this pins that the SHORT edge is derived from it and never
    /// clamped to it.
    #[test]
    fn the_longest_edge_is_bounded_and_the_aspect_ratio_survives() {
        assert_eq!(target(1920, 1080, 320), (320, 180), "16:9, landscape");
        assert_eq!(target(1024, 768, 320), (320, 240), "4:3, landscape");
        assert_eq!(target(768, 1024, 320), (240, 320), "4:3, portrait");
        assert_eq!(target(2000, 10, 160), (160, 1), "a slit still has one row");
        assert_eq!(
            target(640, 480, 640),
            (640, 480),
            "a frame exactly at the bound is untouched"
        );
    }

    /// Every configured size bounds the longest edge to exactly its number.
    #[test]
    fn each_size_bounds_the_longest_edge_to_its_own_number() {
        for (size, edge) in [
            (ThumbnailSize::Small, 160),
            (ThumbnailSize::Medium, 320),
            (ThumbnailSize::Large, 640),
        ] {
            let thumbnail = rendered(render(&rgb(1024, 768), size, mqtt(), CEILING));
            assert_eq!(
                thumbnail.width, edge,
                "{size:?} must bound the longest edge to {edge}"
            );
            assert_eq!(
                thumbnail.height,
                edge * 3 / 4,
                "{size:?} must keep the 4:3 aspect of the frame"
            );
        }
    }

    /// A frame smaller than the bound is carried at its own size, never blown up.
    ///
    /// Upscaling would invent pixels the camera never produced -- a bigger, blurrier picture that
    /// claims more resolution than the frame it came from.
    #[test]
    fn a_frame_smaller_than_the_bound_is_never_upscaled() {
        let thumbnail = rendered(render(&rgb(100, 60), ThumbnailSize::Large, mqtt(), CEILING));
        assert_eq!(
            (thumbnail.width, thumbnail.height),
            (100, 60),
            "a 100x60 frame must stay 100x60 under a 640px bound"
        );
    }

    /// The bytes are a real JPEG, and its dimensions are exactly the ones reported beside it.
    ///
    /// A consumer lays the picture out from `width`/`height` before it decodes it. If the two ever
    /// disagreed, every consumer would render a stretched or clipped preview and have no way to know.
    #[test]
    fn the_bytes_decode_as_a_jpeg_of_exactly_the_reported_dimensions() {
        for format in [PixelFormat::Mono8, PixelFormat::Rgb8, PixelFormat::Bgr8] {
            let source = rgb(640, 480);
            let bytes = match format {
                PixelFormat::Mono8 => vec![90_u8; 640 * 480],
                _ => source.bytes.to_vec(),
            };
            let thumbnail = rendered(render(
                &frame(format, bytes, 640, 480),
                ThumbnailSize::Medium,
                mqtt(),
                CEILING,
            ));
            let data = thumbnail.data_bytes().expect("the marker must carry bytes");
            assert_eq!(
                thumbnail.bytes,
                data.len() as u64,
                "{format:?}: the reported size must be the size of the bytes carried"
            );
            let decoded = ImageReader::new(Cursor::new(&data))
                .with_guessed_format()
                .expect("the thumbnail must be a recognisable image format")
                .decode()
                .unwrap_or_else(|error| panic!("{format:?}: thumbnail must decode: {error}"));
            assert_eq!(
                decoded.dimensions(),
                (thumbnail.width, thumbnail.height),
                "{format:?}: the announced dimensions must be the JPEG's own"
            );
        }
    }

    /// A JPEG frame is decoded, downscaled, and re-encoded -- not copied through as-is.
    #[test]
    fn a_jpeg_frame_is_decoded_before_it_is_downscaled() {
        let source = rgb(800, 600);
        let mut encoded = Vec::new();
        JpegEncoder::new_with_quality(Cursor::new(&mut encoded), 92)
            .encode(source.bytes.as_ref(), 800, 600, ExtendedColorType::Rgb8)
            .expect("fixture JPEG");
        let thumbnail = rendered(render(
            &frame(PixelFormat::Jpeg, encoded.clone(), 800, 600),
            ThumbnailSize::Small,
            mqtt(),
            CEILING,
        ));
        assert_eq!((thumbnail.width, thumbnail.height), (160, 120));
        let data = thumbnail.data_bytes().expect("the marker must carry bytes");
        assert_ne!(
            data, encoded,
            "a JPEG source must be decoded and re-encoded at thumbnail size, not passed through"
        );
        assert_eq!(
            ImageReader::new(Cursor::new(&data))
                .with_guessed_format()
                .expect("format")
                .decode()
                .expect("decode")
                .dimensions(),
            (160, 120)
        );
    }

    /// A frame this module cannot interpret is a clean "no thumbnail", never a panic.
    ///
    /// The renderer runs BEFORE the capture pipeline's own `encoding::validate_frame`, so it is the
    /// first thing in the component to look at a camera's bytes and it must survive anything they
    /// contain. Every one of these is a frame a backend could hand it.
    #[test]
    fn a_frame_that_cannot_be_interpreted_fails_cleanly_and_never_panics() {
        let cases = [
            ("JPEG whose header does not survive truncation", {
                let source = rgb(64, 48);
                let mut encoded = Vec::new();
                JpegEncoder::new_with_quality(Cursor::new(&mut encoded), 90)
                    .encode(source.bytes.as_ref(), 64, 48, ExtendedColorType::Rgb8)
                    .expect("fixture JPEG");
                encoded.truncate(encoded.len() / 2);
                frame(PixelFormat::Jpeg, encoded, 64, 48)
            }),
            (
                "JPEG that is not a JPEG",
                frame(PixelFormat::Jpeg, vec![0; 64], 8, 8),
            ),
            (
                "RGB8 frame of the wrong length",
                frame(PixelFormat::Rgb8, vec![1, 2, 3], 8, 8),
            ),
            (
                "Mono8 frame of the wrong length",
                frame(PixelFormat::Mono8, vec![1, 2, 3], 8, 8),
            ),
            (
                "BGR8 frame of the wrong length",
                frame(PixelFormat::Bgr8, vec![1, 2, 3], 8, 8),
            ),
            (
                "frame with no dimensions",
                frame(PixelFormat::Rgb8, vec![], 0, 0),
            ),
        ];
        for (what, source) in cases {
            match render(&source, ThumbnailSize::Medium, mqtt(), CEILING) {
                ThumbnailOutcome::Failed { reason } => {
                    assert!(!reason.is_empty(), "{what}: a failure must say why")
                }
                other => panic!("{what} must be a clean failure, got {other:?}"),
            }
        }
    }

    /// A JPEG header that declares a picture bigger than the capture's ceiling is REFUSED, not decoded.
    ///
    /// A JPEG's decoded size is not its file size, and a camera is not a trusted party: 653 bytes of
    /// JPEG whose header says 65500x65500 decode to 12.8 GB. This is the ONLY code in the component
    /// that decodes a camera's JPEG -- `passthrough` copies its bytes and the encoder refuses to
    /// re-encode one -- so it is the only place that header could be believed, and it must not be
    /// believed past the memory the capture was admitted with. An OOM is not a degraded preview; it
    /// is the whole component dying because a thumbnail was optional.
    ///
    /// The frame is otherwise entirely valid, which is the point: it passes `validate_frame` (the
    /// declared dimensions match the header's), so the CAPTURE succeeds and only the preview does not.
    /// What we RESERVE has to be what the decoder ACTUALLY allocates, or the reservation is theatre.
    ///
    /// Both halves matter. A JPEG is expanded into pixels, and the size of those pixels is nothing
    /// like the size of the file -- that gap is the whole reason the byte budget could not see this
    /// allocation. The raw formats decode nothing at all: they are scaled through a *view* over bytes
    /// the capture already holds and already reserved, so reserving for them a second time would
    /// halve the component's capacity to buy nothing.
    #[test]
    fn the_bytes_reserved_are_exactly_the_bytes_the_decode_will_allocate() {
        let pixels = ImageBuffer::<Rgb<u8>, Vec<u8>>::from_fn(64, 48, |x, y| {
            Rgb([(x * 4) as u8, (y * 5) as u8, ((x + y) * 2) as u8])
        });
        let mut encoded = Vec::new();
        JpegEncoder::new_with_quality(&mut encoded, 90)
            .encode_image(&DynamicImage::ImageRgb8(pixels))
            .expect("encode the fixture JPEG");

        let jpeg = frame(PixelFormat::Jpeg, encoded.clone(), 64, 48);
        let declared = declared_decode_bytes(&jpeg).expect("a JPEG is decoded, so it is reserved");
        assert_eq!(
            declared,
            u64::from(64_u32 * 48 * 3),
            "the reservation is the DECODED size -- 64x48 RGB -- and not the {} bytes of the file",
            encoded.len()
        );
        assert!(
            declared > encoded.len() as u64,
            "which is the entire point: the pixels dwarf the file, and the budget never saw them"
        );

        for raw in [PixelFormat::Rgb8, PixelFormat::Bgr8, PixelFormat::Mono8] {
            assert!(
                declared_decode_bytes(&frame(raw, vec![0; 64 * 48 * 3], 64, 48)).is_none(),
                "{raw:?} is scaled through a view and allocates nothing worth reserving"
            );
        }
    }

    #[test]
    fn a_jpeg_header_that_lies_about_its_size_is_refused_before_a_byte_is_allocated() {
        let (jpeg, width, height) = oversized_jpeg_header();
        assert!(
            jpeg.len() < 1_024,
            "the fixture must be a SMALL file making a large claim"
        );

        let source = frame(PixelFormat::Jpeg, jpeg, width, height);
        // 8 MiB is a generous frame ceiling; the header claims 1.2 GB.
        match render(&source, ThumbnailSize::Medium, mqtt(), CEILING) {
            ThumbnailOutcome::Failed { reason } => assert!(
                reason.contains("ceiling"),
                "the refusal must name the ceiling it enforced: {reason}"
            ),
            other => panic!(
                "a JPEG claiming to decode to 1.2 GB must be refused, not decoded, got {other:?}"
            ),
        }

        // And the ceiling is a real bound, not a blanket refusal of JPEG: raise it above the claim
        // and the SAME frame is no longer refused for its size.
        let honest = rgb(320, 240);
        let mut encoded = Vec::new();
        JpegEncoder::new_with_quality(Cursor::new(&mut encoded), 90)
            .encode(honest.bytes.as_ref(), 320, 240, ExtendedColorType::Rgb8)
            .expect("fixture JPEG");
        let exact = u64::from(320_u32 * 240 * 3);
        assert!(
            matches!(
                render(
                    &frame(PixelFormat::Jpeg, encoded.clone(), 320, 240),
                    ThumbnailSize::Small,
                    mqtt(),
                    exact,
                ),
                ThumbnailOutcome::Rendered(_)
            ),
            "a frame that decodes to exactly its ceiling is within it"
        );
        assert!(
            matches!(
                render(
                    &frame(PixelFormat::Jpeg, encoded, 320, 240),
                    ThumbnailSize::Small,
                    mqtt(),
                    exact - 1,
                ),
                ThumbnailOutcome::Failed { .. }
            ),
            "and one byte over it is not"
        );
    }

    /// A small, structurally valid JPEG whose SOF header claims 20000x20000 (1.2 GB decoded).
    ///
    /// Hand-patched rather than encoded, because encoding a real 20000x20000 image is the very
    /// allocation this test exists to prove never happens.
    fn oversized_jpeg_header() -> (Vec<u8>, u32, u32) {
        const CLAIMED: u16 = 20_000;
        let mut jpeg = Vec::new();
        JpegEncoder::new_with_quality(Cursor::new(&mut jpeg), 90)
            .encode(&[128_u8; 32 * 32 * 3], 32, 32, ExtendedColorType::Rgb8)
            .expect("fixture JPEG");
        let sof = jpeg
            .windows(2)
            .position(|marker| marker == [0xff, 0xc0])
            .expect("a baseline JPEG has an SOF0 marker");
        // SOF0: ff c0, length(2), precision(1), height(2), width(2), components...
        jpeg[sof + 5..sof + 7].copy_from_slice(&CLAIMED.to_be_bytes());
        jpeg[sof + 7..sof + 9].copy_from_slice(&CLAIMED.to_be_bytes());
        (jpeg, u32::from(CLAIMED), u32::from(CLAIMED))
    }

    /// A picture that will not fit the ceiling is DROPPED, and the ladder was tried first.
    ///
    /// The ceiling is load-bearing: over it, `binary_value` refuses the value and the announcement
    /// itself would fail to build. So the last resort is losing the thumbnail, never the message.
    #[test]
    fn a_thumbnail_that_will_not_fit_the_ceiling_is_dropped_after_the_ladder() {
        // Per-pixel pseudo-random noise: incompressible on purpose, so 640x640 of it cannot be
        // squeezed under the MQTT preview budget even at the bottom of the quality ladder.
        let (width, height) = (640_u32, 640_u32);
        let mut bytes = Vec::with_capacity((width * height * 3) as usize);
        let mut state = 0x2545_F491_4F6C_DD1D_u64;
        for _ in 0..width * height * 3 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            bytes.push(state as u8);
        }
        let noise = frame(PixelFormat::Rgb8, bytes, width, height);

        match render(&noise, ThumbnailSize::Large, mqtt(), CEILING) {
            ThumbnailOutcome::Dropped { bytes } => {
                assert!(
                    bytes > mqtt().budget_bytes(),
                    "a dropped thumbnail must be one that genuinely did not fit: {bytes} bytes"
                );
            }
            other => panic!("incompressible noise at 640px must not fit the MQTT budget, got {other:?}"),
        }

        // ...and the same noise DOES fit once the picture is small enough, which proves the drop
        // above is the ceiling talking and not a broken encoder.
        let small = rendered(render(&noise, ThumbnailSize::Small, mqtt(), CEILING));
        assert!(
            small.bytes <= mqtt().budget_bytes() as u64,
            "a small thumbnail of the same frame must fit: {} bytes",
            small.bytes
        );
    }

    /// Each transport permits exactly what it can carry -- IPC one size, MQTT all three.
    ///
    /// These two numbers per transport are the whole policy, and they were both learned the
    /// expensive way: on the lab device, `medium` and `large` on Greengrass IPC lost 45 of 45
    /// announcements each to NOMEM, and `small` lost none.
    #[test]
    fn each_transport_permits_only_what_it_can_actually_carry() {
        let ipc = ipc();
        assert_eq!(
            ipc.largest_size(),
            ThumbnailSize::Small,
            "the IPC client encodes a whole message into a 10,000-byte buffer; only `small` fits"
        );
        assert_eq!(ipc.budget_bytes(), 6 * 1024);
        assert_eq!(ipc.transport(), Transport::Ipc);
        assert!(ipc.clamps(ThumbnailSize::Large) && ipc.clamps(ThumbnailSize::Medium));
        assert!(!ipc.clamps(ThumbnailSize::Small));

        let mqtt = mqtt();
        assert_eq!(
            mqtt.largest_size(),
            ThumbnailSize::Large,
            "a broker carries a megabyte; nothing here needs clamping"
        );
        assert_eq!(mqtt.budget_bytes(), 60 * 1024);
        assert_eq!(mqtt.transport(), Transport::Mqtt);
        for size in [
            ThumbnailSize::Small,
            ThumbnailSize::Medium,
            ThumbnailSize::Large,
        ] {
            assert!(!mqtt.clamps(size), "{size:?} must be carryable on MQTT");
            assert_eq!(mqtt.effective_size(size), size, "and produced as asked");
        }
    }

    /// A size the transport cannot carry is CLAMPED DOWN, never rejected and never attempted.
    ///
    /// The same configuration is deployed to Greengrass and to Kubernetes. Refusing to start on the
    /// one whose transport is smaller would be a hostile way to report a convenience -- and sending
    /// it anyway is what cost the lab 90 announcements.
    #[test]
    fn a_size_the_transport_cannot_carry_is_clamped_down_not_rejected() {
        let ipc = ipc();
        assert_eq!(ipc.effective_size(ThumbnailSize::Large), ThumbnailSize::Small);
        assert_eq!(
            ipc.effective_size(ThumbnailSize::Medium),
            ThumbnailSize::Small
        );
        assert_eq!(
            ipc.effective_size(ThumbnailSize::Small),
            ThumbnailSize::Small,
            "what already fits is left alone"
        );

        // And the render path honours it: a profile asking for `large` on IPC gets 160 px.
        let thumbnail = rendered(render(&rgb(1024, 768), ThumbnailSize::Large, ipc, CEILING));
        assert_eq!(
            (thumbnail.width, thumbnail.height),
            (160, 120),
            "a `large` profile on IPC must produce a `small` picture, not a large one nobody receives"
        );
    }

    /// A `small` preview fits inside what the IPC client can actually put on the wire.
    ///
    /// The budget is 6 KiB of an ~10,000-byte packet that must also hold the envelope (the largest
    /// terminal body measured on the device was 1,677 bytes), the headers and the topic. If a
    /// `small` thumbnail did not fit that, the policy would be permitting a size it cannot deliver.
    #[test]
    fn a_small_preview_fits_what_the_ipc_client_can_put_on_the_wire() {
        for frame in [rgb(1920, 1080), rgb(1024, 768), rgb(640, 480)] {
            let thumbnail = rendered(render(&frame, ThumbnailSize::Small, ipc(), CEILING));
            assert!(
                thumbnail.bytes <= ipc().budget_bytes() as u64,
                "a small preview must fit the IPC budget: {} bytes",
                thumbnail.bytes
            );
            assert_eq!(thumbnail.width.max(thumbnail.height), 160);
        }
    }

    /// On MQTT, `large` is produced AT `large` -- the clamp is a property of the transport, not a
    /// blanket downgrade that would quietly rob every deployment of the size it asked for.
    #[test]
    fn on_mqtt_a_large_preview_is_produced_at_large() {
        let thumbnail = rendered(render(&rgb(1920, 1080), ThumbnailSize::Large, mqtt(), CEILING));
        assert_eq!(
            (thumbnail.width, thumbnail.height),
            (640, 360),
            "a broker can carry 640 px, so 640 px is what a `large` profile must get"
        );
        assert!(thumbnail.bytes <= mqtt().budget_bytes() as u64);
    }

    /// The clamp is reported ONCE per camera, from the configuration -- never per capture.
    ///
    /// A clamp is a fact about the deployment, not an event. Forty-five captures an hour per camera,
    /// each re-announcing the same unchanging fact, is how the one line an operator needed gets
    /// buried. The per-capture path cannot even produce a notice: [`render`] is handed the policy and
    /// returns a picture.
    #[test]
    fn a_clamp_is_reported_once_per_camera_and_never_per_capture() {
        let directory = tempfile::tempdir().unwrap();
        let config = clamp_fixture(directory.path());

        let notices = clamp_notices(&config, ipc());
        assert_eq!(
            notices.len(),
            1,
            "one notice per CAMERA, however many of its profiles are clamped: {notices:?}"
        );
        assert_eq!(notices[0].instance, "camera-a");
        assert_eq!(
            notices[0].profiles,
            ["big", "middling"],
            "and it names every profile that is being clamped -- but not the one that fits, and not \
             the one that asked for no preview at all"
        );
        assert_eq!(notices[0].effective, ThumbnailSize::Small);

        assert!(
            clamp_notices(&config, mqtt()).is_empty(),
            "a transport that can carry what was asked for has nothing to report"
        );
    }

    /// A camera with `large`, `medium`, `small`, and no-thumbnail profiles.
    fn clamp_fixture(root: &std::path::Path) -> crate::config::AdapterConfig {
        let raw = serde_json::json!({
            "component": {
                "global": { "output": { "rootDirectory": root.to_string_lossy() } },
                "instances": [{
                    "id": "camera-a",
                    "backend": { "type": "sim" },
                    "defaultCaptureProfile": "fitting",
                    "captureProfiles": {
                        "big": { "output": { "encoding": "jpeg" }, "thumbnail": { "size": "large" } },
                        "middling": { "output": { "encoding": "jpeg" }, "thumbnail": { "size": "medium" } },
                        "fitting": { "output": { "encoding": "jpeg" }, "thumbnail": { "size": "small" } },
                        "plain": { "output": { "encoding": "jpeg" } }
                    }
                }]
            }
        });
        let core = edgecommons::config::Config::from_value(
            crate::COMPONENT_NAME,
            "gw-01",
            raw,
        )
        .expect("the fixture configuration must be valid");
        crate::config::AdapterConfig::from_core_reload(&core)
            .expect("the fixture configuration must be accepted")
    }

    /// Whatever is rendered fits the ceiling, and therefore fits `binary_value`'s 64 KiB bound.
    ///
    /// This is the invariant the announcement depends on: a `Rendered` thumbnail can always be
    /// stamped into the terminal body. (That the ceiling itself stays under the library's bound is a
    /// compile-time assertion in this module -- it is not a thing a test should be the first to
    /// notice.)
    #[test]
    fn everything_rendered_is_within_the_ceiling_and_the_ladder_is_the_agreed_one() {
        assert_eq!(
            QUALITY_LADDER,
            [80, 65, 50],
            "the ladder is the agreed one: 80, then 65, then 50"
        );
        let thumbnail = rendered(render(&rgb(1920, 1080), ThumbnailSize::Large, mqtt(), CEILING));
        assert!(thumbnail.bytes <= mqtt().budget_bytes() as u64);
        assert!(thumbnail.data_bytes().is_ok());
    }

    /// A grayscale frame stays grayscale, and a colour frame stays colour.
    #[test]
    fn a_mono_frame_produces_a_grayscale_preview() {
        let mut bytes = Vec::with_capacity(64 * 64);
        for y in 0..64_u32 {
            for x in 0..64_u32 {
                bytes.push(((x * 4) ^ (y * 4)) as u8);
            }
        }
        let thumbnail = rendered(render(
            &frame(PixelFormat::Mono8, bytes, 64, 64),
            ThumbnailSize::Small,
            mqtt(),
            CEILING,
        ));
        let decoded = ImageReader::new(Cursor::new(
            thumbnail.data_bytes().expect("the marker must carry bytes"),
        ))
        .with_guessed_format()
        .expect("format")
        .decode()
        .expect("decode");
        assert!(
            matches!(decoded, DynamicImage::ImageLuma8(_)),
            "a Mono8 frame has no colour to preserve, and a 3x larger preview would invent one"
        );
    }

    /// The same frame always yields the same bytes: one filter, one ladder, no ambient state.
    #[test]
    fn rendering_is_deterministic() {
        let source = rgb(500, 400);
        let first = rendered(render(&source, ThumbnailSize::Medium, mqtt(), CEILING));
        let second = rendered(render(&source, ThumbnailSize::Medium, mqtt(), CEILING));
        assert_eq!(
            first.data_bytes().unwrap(),
            second.data_bytes().unwrap(),
            "a deterministic filter and a deterministic ladder must produce identical bytes"
        );
    }

    /// A BGR8 frame is presented as RGB, not as its own channel order read backwards.
    #[test]
    fn a_bgr_frame_is_not_previewed_with_its_channels_swapped() {
        // A flat, unambiguous red: BGR8 stores it as (0, 0, 255).
        let bgr = frame(PixelFormat::Bgr8, [0_u8, 0, 255].repeat(64 * 64), 64, 64);
        let thumbnail = rendered(render(&bgr, ThumbnailSize::Small, mqtt(), CEILING));
        let decoded = ImageReader::new(Cursor::new(thumbnail.data_bytes().unwrap()))
            .with_guessed_format()
            .expect("format")
            .decode()
            .expect("decode")
            .to_rgb8();
        let pixel = decoded.get_pixel(32, 32).0;
        assert!(
            pixel[0] > 200 && pixel[1] < 60 && pixel[2] < 60,
            "a red BGR8 frame must preview as red, not blue: {pixel:?}"
        );
    }
}
