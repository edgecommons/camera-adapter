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
//! * **The byte ceiling.** [`edgecommons::messaging::message::binary_value`] refuses a binary value
//!   over `MAX_BINARY_BODY_BYTES` (64 KiB), and the thumbnail rides inside the terminal announcement. A
//!   thumbnail that blew that ceiling would fail the ANNOUNCEMENT ITSELF to build, and the capture's
//!   terminal message would be lost -- so this module encodes down a quality ladder against a
//!   deliberately lower ceiling ([`MAX_THUMBNAIL_BYTES`], 48 KiB) and, if even the bottom of the
//!   ladder will not fit, drops the thumbnail rather than the message.

use std::io::Cursor;

use image::codecs::jpeg::{JpegDecoder, JpegEncoder};
use image::imageops::FilterType;
use image::{
    DynamicImage, ExtendedColorType, GenericImageView, ImageBuffer, ImageDecoder, Luma, Pixel, Rgb,
    imageops,
};

use crate::config::ThumbnailSize;
use crate::messages::Thumbnail;
use crate::model::{CaptureFrame, PixelFormat};

/// The component's own ceiling for the encoded thumbnail, deliberately under the library's.
///
/// The messaging library caps one binary value at 64 KiB and returns an error above it. That error
/// would surface as "the terminal announcement could not be built", so the margin between this
/// number and that one is what guarantees a thumbnail can never cost a capture its announcement.
pub const MAX_THUMBNAIL_BYTES: usize = 48 * 1024;

/// The component's ceiling must stay under the library's, or a thumbnail could fail an announcement.
///
/// Asserted at COMPILE time, because it is the one relationship in this module that nothing at
/// runtime would notice being wrong: a thumbnail that fits 64 KiB but not the margin is announced
/// happily, right up until the envelope it is stamped into overflows the library's bound.
const _: () = assert!(
    MAX_THUMBNAIL_BYTES < edgecommons::messaging::message::MAX_BINARY_BODY_BYTES,
    "the thumbnail ceiling must leave the messaging library's binary-value bound room for the rest \
     of the envelope"
);

/// The JPEG qualities tried, in order, until one fits [`MAX_THUMBNAIL_BYTES`].
///
/// This is the whole "quality" surface, and it is deliberately not configurable: the component owns
/// the trade between fidelity and the byte ceiling, because the ceiling is not negotiable.
pub const QUALITY_LADDER: [u8; 3] = [80, 65, 50];

/// The one resampling filter. Fixed so that the same frame always yields the same thumbnail.
const FILTER: FilterType = FilterType::Lanczos3;

/// What rendering one thumbnail produced.
///
/// There is no error variant that a caller could propagate, on purpose: every outcome here is one
/// the capture survives.
#[derive(Debug, Clone, PartialEq)]
pub enum ThumbnailOutcome {
    /// A thumbnail that fits the ceiling and can be announced.
    Rendered(Thumbnail),
    /// The frame rendered, but the smallest encoding still exceeded [`MAX_THUMBNAIL_BYTES`].
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

/// Renders one thumbnail of `frame`, bounded by `size` and by [`MAX_THUMBNAIL_BYTES`].
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
/// This is CPU-bound and must be called from a blocking context (the capture pipeline calls it
/// inside the same `spawn_blocking` as the encoder, under the same admission permits).
///
/// # Panics
/// Never. Every failure is a [`ThumbnailOutcome`].
#[must_use]
pub fn render(
    frame: &CaptureFrame,
    size: ThumbnailSize,
    maximum_frame_bytes: u64,
) -> ThumbnailOutcome {
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
        if encoded.len() <= MAX_THUMBNAIL_BYTES {
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
            let thumbnail = rendered(render(&rgb(1024, 768), size, CEILING));
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
        let thumbnail = rendered(render(&rgb(100, 60), ThumbnailSize::Large, CEILING));
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
            match render(&source, ThumbnailSize::Medium, CEILING) {
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
    #[test]
    fn a_jpeg_header_that_lies_about_its_size_is_refused_before_a_byte_is_allocated() {
        let (jpeg, width, height) = oversized_jpeg_header();
        assert!(
            jpeg.len() < 1_024,
            "the fixture must be a SMALL file making a large claim"
        );

        let source = frame(PixelFormat::Jpeg, jpeg, width, height);
        // 8 MiB is a generous frame ceiling; the header claims 1.2 GB.
        match render(&source, ThumbnailSize::Medium, CEILING) {
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
        // squeezed under 48 KiB even at the bottom of the quality ladder.
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

        match render(&noise, ThumbnailSize::Large, CEILING) {
            ThumbnailOutcome::Dropped { bytes } => {
                assert!(
                    bytes > MAX_THUMBNAIL_BYTES,
                    "a dropped thumbnail must be one that genuinely did not fit: {bytes} bytes"
                );
            }
            other => panic!("incompressible noise at 640px must not fit 48 KiB, got {other:?}"),
        }

        // ...and the same noise DOES fit once the picture is small enough, which proves the drop
        // above is the ceiling talking and not a broken encoder.
        let small = rendered(render(&noise, ThumbnailSize::Small, CEILING));
        assert!(
            small.bytes <= MAX_THUMBNAIL_BYTES as u64,
            "a small thumbnail of the same frame must fit: {} bytes",
            small.bytes
        );
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
        let thumbnail = rendered(render(&rgb(1920, 1080), ThumbnailSize::Large, CEILING));
        assert!(thumbnail.bytes <= MAX_THUMBNAIL_BYTES as u64);
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
        let first = rendered(render(&source, ThumbnailSize::Medium, CEILING));
        let second = rendered(render(&source, ThumbnailSize::Medium, CEILING));
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
        let thumbnail = rendered(render(&bgr, ThumbnailSize::Small, CEILING));
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
