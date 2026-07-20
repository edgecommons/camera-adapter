//! Bounded, streaming image validation and encoding.
//!
//! Encoders write directly to the caller's partial file. They never construct an encoded
//! image-sized `Vec`; BGR conversion is performed through a virtual JPEG view or one row at a
//! time for PNG/TIFF.

use std::io::{Cursor, Seek, SeekFrom, Write};

use image::codecs::jpeg::{JpegDecoder, JpegEncoder};
use image::{ExtendedColorType, GenericImageView, ImageDecoder, Rgb};
use tokio_util::sync::CancellationToken;

use crate::model::{CaptureFrame, OutputEncoding, PixelFormat};
use crate::{CameraError, ErrorCode, Result};

const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// Fully resolved encoding parameters for one accepted capture.
#[derive(Debug, Clone, Copy)]
pub struct EncodingRequest {
    /// Requested final encoding.
    pub encoding: OutputEncoding,
    /// JPEG quality in the inclusive range `1..=100`.
    pub jpeg_quality: u8,
    /// Hard ceiling for the installed image, already covered by admission's disk reservation.
    pub maximum_output_bytes: u64,
}

/// Stable facts known after encoding finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedImage {
    /// Final encoding.
    pub encoding: OutputEncoding,
    /// Filename extension without a dot.
    pub extension: &'static str,
    /// MIME content type.
    pub content_type: &'static str,
    /// Exact number of bytes written to the partial file.
    pub bytes: u64,
}

/// Validates `frame` and streams the requested output directly into `sink`.
///
/// `sink` must be an empty, exclusively-created, seekable partial file. The production storage
/// backends pass an exclusively-created file handle; seeking is required by the PNG, TIFF, and
/// JPEG encoders and is therefore part of this persistence boundary. The byte ceiling is enforced
/// while encoding, not after an unbounded allocation. Cancellation is checked on every output
/// write and between source rows/chunks.
pub fn encode_to<W: Write + Seek>(
    frame: &CaptureFrame,
    request: EncodingRequest,
    sink: &mut W,
    cancellation: &CancellationToken,
) -> Result<EncodedImage> {
    validate_frame(frame)?;
    if request.maximum_output_bytes == 0 {
        return Err(CameraError::rejected(
            ErrorCode::ResourceLimit,
            "maximum output bytes must be positive",
        ));
    }
    if !(1..=100).contains(&request.jpeg_quality) {
        return Err(CameraError::rejected(
            ErrorCode::BadArgs,
            "JPEG quality must be between 1 and 100",
        ));
    }
    cancelled(cancellation)?;

    let (extension, content_type) = output_identity(frame.pixel_format, request.encoding)?;
    let mut bounded = BoundedWriter::new(sink, request.maximum_output_bytes, cancellation);
    let encoded = match request.encoding {
        OutputEncoding::Raw | OutputEncoding::Passthrough => {
            stream_copy(frame.bytes.as_ref(), &mut bounded, cancellation)
                .map_err(|error| error.to_string())
        }
        OutputEncoding::Jpeg => encode_jpeg(frame, request.jpeg_quality, &mut bounded),
        OutputEncoding::Png => encode_png(frame, &mut bounded, cancellation),
        OutputEncoding::Tiff => encode_tiff(frame, &mut bounded, cancellation),
    };

    if let Some(violation) = bounded.violation {
        return Err(violation.into_error());
    }
    if cancellation.is_cancelled() {
        return Err(CameraError::rejected(
            ErrorCode::CaptureCancelled,
            "capture cancelled during encoding",
        ));
    }
    encoded.map_err(|error| CameraError::Storage(format!("image encoding failed: {error}")))?;
    bounded
        .flush()
        .map_err(|error| CameraError::Storage(format!("partial-file flush failed: {error}")))?;

    Ok(EncodedImage {
        encoding: request.encoding,
        extension,
        content_type,
        bytes: bounded.high_water,
    })
}

fn validate_frame(frame: &CaptureFrame) -> Result<()> {
    if frame.width == 0 || frame.height == 0 || frame.bytes.is_empty() {
        return unsupported("frame dimensions and bytes must be non-empty");
    }
    if frame.pixel_format != PixelFormat::Jpeg {
        let expected = frame
            .pixel_format
            .uncompressed_len(frame.width, frame.height)
            .ok_or_else(|| {
                unsupported_error("uncompressed frame dimensions overflow the byte-count domain")
            })?;
        if expected != frame.bytes.len() as u64 {
            return unsupported(format!(
                "{} frame length is {}, expected {expected} for {}x{}",
                pixel_name(frame.pixel_format),
                frame.bytes.len(),
                frame.width,
                frame.height
            ));
        }
    }
    if frame.pixel_format == PixelFormat::Jpeg {
        validate_jpeg(frame)?;
    }
    Ok(())
}

fn validate_jpeg(frame: &CaptureFrame) -> Result<()> {
    let decoder = JpegDecoder::new(Cursor::new(frame.bytes.as_ref()))
        .map_err(|error| unsupported_error(format!("declared JPEG is not decodable: {error}")))?;
    let dimensions = decoder.dimensions();
    if dimensions != (frame.width, frame.height) {
        return unsupported(format!(
            "declared JPEG dimensions are {}x{}, not {}x{}",
            dimensions.0, dimensions.1, frame.width, frame.height
        ));
    }
    Ok(())
}

fn output_identity(
    source: PixelFormat,
    output: OutputEncoding,
) -> Result<(&'static str, &'static str)> {
    match output {
        OutputEncoding::Passthrough if source == PixelFormat::Jpeg => Ok(("jpg", "image/jpeg")),
        OutputEncoding::Passthrough => {
            unsupported("passthrough requires a declared complete JPEG source")
        }
        OutputEncoding::Jpeg if source != PixelFormat::Jpeg => Ok(("jpg", "image/jpeg")),
        OutputEncoding::Jpeg => unsupported(
            "JPEG source bytes require passthrough; JPEG quality is never silently ignored",
        ),
        OutputEncoding::Png if source != PixelFormat::Jpeg => Ok(("png", "image/png")),
        OutputEncoding::Png => {
            unsupported("PNG output requires Mono8, RGB8, or BGR8 source pixels")
        }
        OutputEncoding::Tiff if source != PixelFormat::Jpeg => Ok(("tiff", "image/tiff")),
        OutputEncoding::Tiff => {
            unsupported("TIFF output requires Mono8, RGB8, or BGR8 source pixels")
        }
        OutputEncoding::Raw => Ok(("raw", "application/octet-stream")),
    }
}

fn encode_jpeg<W: Write + Seek>(
    frame: &CaptureFrame,
    quality: u8,
    writer: &mut W,
) -> std::result::Result<(), String> {
    let mut encoder = JpegEncoder::new_with_quality(writer, quality);
    match frame.pixel_format {
        PixelFormat::Mono8 => encoder
            .encode(
                frame.bytes.as_ref(),
                frame.width,
                frame.height,
                ExtendedColorType::L8,
            )
            .map_err(|error| error.to_string()),
        PixelFormat::Rgb8 => encoder
            .encode(
                frame.bytes.as_ref(),
                frame.width,
                frame.height,
                ExtendedColorType::Rgb8,
            )
            .map_err(|error| error.to_string()),
        PixelFormat::Bgr8 => encoder
            .encode_image(&BgrView::new(
                frame.bytes.as_ref(),
                frame.width,
                frame.height,
            ))
            .map_err(|error| error.to_string()),
        PixelFormat::Jpeg => unreachable!("validated JPEG is copied before conversion dispatch"),
    }
}

fn encode_png<W: Write + Seek>(
    frame: &CaptureFrame,
    writer: &mut W,
    cancellation: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut encoder = png::Encoder::new(writer, frame.width, frame.height);
    encoder.set_depth(png::BitDepth::Eight);
    encoder.set_color(match frame.pixel_format {
        PixelFormat::Mono8 => png::ColorType::Grayscale,
        PixelFormat::Rgb8 | PixelFormat::Bgr8 => png::ColorType::Rgb,
        PixelFormat::Jpeg => unreachable!("JPEG-to-PNG rejected before encoding"),
    });
    let mut png_writer = encoder.write_header().map_err(|error| error.to_string())?;
    {
        let mut stream = png_writer
            .stream_writer_with_size(STREAM_CHUNK_BYTES)
            .map_err(|error| error.to_string())?;
        match frame.pixel_format {
            PixelFormat::Mono8 | PixelFormat::Rgb8 => {
                stream_copy(frame.bytes.as_ref(), &mut stream, cancellation)
            }
            PixelFormat::Bgr8 => stream_bgr_rows(frame, &mut stream, cancellation),
            PixelFormat::Jpeg => unreachable!("JPEG-to-PNG rejected before encoding"),
        }
        .map_err(|error| error.to_string())?;
        stream.finish().map_err(|error| error.to_string())?;
    }
    png_writer.finish().map_err(|error| error.to_string())
}

fn encode_tiff<W: Write + Seek>(
    frame: &CaptureFrame,
    writer: &mut W,
    cancellation: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut encoder = tiff::encoder::TiffEncoder::new(writer).map_err(|error| error.to_string())?;
    match frame.pixel_format {
        PixelFormat::Mono8 => {
            let mut image = encoder
                .new_image::<tiff::encoder::colortype::Gray8>(frame.width, frame.height)
                .map_err(|error| error.to_string())?;
            image.rows_per_strip(1).map_err(|error| error.to_string())?;
            write_tiff_rows(
                &mut image,
                frame.bytes.as_ref(),
                frame.width as usize,
                cancellation,
            )?;
            image.finish().map_err(|error| error.to_string())
        }
        PixelFormat::Rgb8 => {
            let mut image = encoder
                .new_image::<tiff::encoder::colortype::RGB8>(frame.width, frame.height)
                .map_err(|error| error.to_string())?;
            image.rows_per_strip(1).map_err(|error| error.to_string())?;
            write_tiff_rows(
                &mut image,
                frame.bytes.as_ref(),
                frame.width as usize * 3,
                cancellation,
            )?;
            image.finish().map_err(|error| error.to_string())
        }
        PixelFormat::Bgr8 => {
            let mut image = encoder
                .new_image::<tiff::encoder::colortype::RGB8>(frame.width, frame.height)
                .map_err(|error| error.to_string())?;
            image.rows_per_strip(1).map_err(|error| error.to_string())?;
            let row_bytes = frame.width as usize * 3;
            let mut row = vec![0_u8; row_bytes];
            for source in frame.bytes.chunks_exact(row_bytes) {
                if cancellation.is_cancelled() {
                    return Err("encoding cancelled".to_string());
                }
                bgr_to_rgb(source, &mut row);
                image.write_strip(&row).map_err(|error| error.to_string())?;
            }
            image.finish().map_err(|error| error.to_string())
        }
        PixelFormat::Jpeg => unreachable!("JPEG-to-TIFF rejected before encoding"),
    }
}

fn write_tiff_rows<W, C, K>(
    image: &mut tiff::encoder::ImageEncoder<'_, W, C, K>,
    bytes: &[u8],
    row_bytes: usize,
    cancellation: &CancellationToken,
) -> std::result::Result<(), String>
where
    W: Write + Seek,
    C: tiff::encoder::colortype::ColorType<Inner = u8>,
    K: tiff::encoder::TiffKind,
{
    for row in bytes.chunks_exact(row_bytes) {
        if cancellation.is_cancelled() {
            return Err("encoding cancelled".to_string());
        }
        image.write_strip(row).map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn stream_bgr_rows<W: Write>(
    frame: &CaptureFrame,
    writer: &mut W,
    cancellation: &CancellationToken,
) -> std::io::Result<()> {
    let row_bytes = frame.width as usize * 3;
    let mut row = vec![0_u8; row_bytes];
    for source in frame.bytes.chunks_exact(row_bytes) {
        check_cancel_io(cancellation)?;
        bgr_to_rgb(source, &mut row);
        writer.write_all(&row)?;
    }
    Ok(())
}

fn bgr_to_rgb(source: &[u8], target: &mut [u8]) {
    for (source, target) in source.chunks_exact(3).zip(target.chunks_exact_mut(3)) {
        target.copy_from_slice(&[source[2], source[1], source[0]]);
    }
}

fn stream_copy<W: Write>(
    bytes: &[u8],
    writer: &mut W,
    cancellation: &CancellationToken,
) -> std::io::Result<()> {
    for chunk in bytes.chunks(STREAM_CHUNK_BYTES) {
        check_cancel_io(cancellation)?;
        writer.write_all(chunk)?;
    }
    Ok(())
}

fn check_cancel_io(cancellation: &CancellationToken) -> std::io::Result<()> {
    if cancellation.is_cancelled() {
        Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "encoding cancelled",
        ))
    } else {
        Ok(())
    }
}

fn cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(CameraError::rejected(
            ErrorCode::CaptureCancelled,
            "capture cancelled before encoding completed",
        ))
    } else {
        Ok(())
    }
}

fn unsupported<T>(message: impl Into<String>) -> Result<T> {
    Err(unsupported_error(message))
}

fn unsupported_error(message: impl Into<String>) -> CameraError {
    CameraError::rejected(ErrorCode::UnsupportedPixelFormat, message.into())
}

fn pixel_name(format: PixelFormat) -> &'static str {
    match format {
        PixelFormat::Mono8 => "Mono8",
        PixelFormat::Rgb8 => "RGB8",
        PixelFormat::Bgr8 => "BGR8",
        PixelFormat::Jpeg => "JPEG",
    }
}

#[derive(Debug, Clone, Copy)]
enum WriteViolation {
    Cancelled,
    Limit,
}

impl WriteViolation {
    fn into_error(self) -> CameraError {
        match self {
            Self::Cancelled => CameraError::rejected(
                ErrorCode::CaptureCancelled,
                "capture cancelled during encoding",
            ),
            Self::Limit => CameraError::rejected(
                ErrorCode::ResourceLimit,
                "encoded image exceeded its reserved byte ceiling",
            ),
        }
    }
}

struct BoundedWriter<'a, W> {
    inner: &'a mut W,
    cancellation: &'a CancellationToken,
    maximum: u64,
    position: u64,
    high_water: u64,
    violation: Option<WriteViolation>,
}

impl<'a, W> BoundedWriter<'a, W> {
    fn new(inner: &'a mut W, maximum: u64, cancellation: &'a CancellationToken) -> Self {
        Self {
            inner,
            cancellation,
            maximum,
            position: 0,
            high_water: 0,
            violation: None,
        }
    }
}

impl<W: Write> Write for BoundedWriter<'_, W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            self.violation = Some(WriteViolation::Cancelled);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "encoding cancelled",
            ));
        }
        let end = self
            .position
            .checked_add(buffer.len() as u64)
            .filter(|end| *end <= self.maximum)
            .ok_or_else(|| {
                self.violation = Some(WriteViolation::Limit);
                std::io::Error::new(
                    std::io::ErrorKind::FileTooLarge,
                    "encoded image exceeded reserved bytes",
                )
            })?;
        let written = self.inner.write(buffer)?;
        self.position += written as u64;
        self.high_water = self.high_water.max(self.position);
        debug_assert!(self.position <= end);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Seek> Seek for BoundedWriter<'_, W> {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        if self.cancellation.is_cancelled() {
            self.violation = Some(WriteViolation::Cancelled);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "encoding cancelled",
            ));
        }
        let position = self.inner.seek(position)?;
        if position > self.maximum {
            self.violation = Some(WriteViolation::Limit);
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "encoder seek exceeded reserved bytes",
            ));
        }
        self.position = position;
        Ok(position)
    }
}

struct BgrView<'a> {
    bytes: &'a [u8],
    width: u32,
    height: u32,
}

impl<'a> BgrView<'a> {
    fn new(bytes: &'a [u8], width: u32, height: u32) -> Self {
        Self {
            bytes,
            width,
            height,
        }
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
    use std::io;

    use bytes::Bytes;
    use chrono::Utc;
    use image::ImageReader;

    use super::*;
    use crate::model::{CaptureMode, FrameTimestampQuality};

    fn frame(format: PixelFormat, bytes: Vec<u8>, width: u32, height: u32) -> CaptureFrame {
        CaptureFrame {
            bytes: Bytes::from(bytes),
            width,
            height,
            pixel_format: format,
            capture_mode: CaptureMode::Simulated,
            source_timestamp: Some(Utc::now()),
            timestamp_quality: FrameTimestampQuality::Camera,
            backend_metadata: BTreeMap::new(),
        }
    }

    fn request(encoding: OutputEncoding) -> EncodingRequest {
        EncodingRequest {
            encoding,
            jpeg_quality: 90,
            maximum_output_bytes: 1024 * 1024,
        }
    }

    #[test]
    fn raw_and_jpeg_passthrough_are_exact() {
        let cancellation = CancellationToken::new();
        let raw = frame(PixelFormat::Mono8, vec![1, 2, 3, 4], 2, 2);
        let mut sink = Cursor::new(Vec::new());
        let result =
            encode_to(&raw, request(OutputEncoding::Raw), &mut sink, &cancellation).expect("raw");
        assert_eq!(sink.into_inner(), vec![1, 2, 3, 4]);
        assert_eq!(result.extension, "raw");

        let mut jpeg_bytes = Cursor::new(Vec::new());
        JpegEncoder::new(&mut jpeg_bytes)
            .encode(&[10, 20, 30], 1, 1, ExtendedColorType::Rgb8)
            .expect("fixture JPEG");
        let jpeg = frame(PixelFormat::Jpeg, jpeg_bytes.into_inner(), 1, 1);
        let mut sink = Cursor::new(Vec::new());
        encode_to(
            &jpeg,
            request(OutputEncoding::Passthrough),
            &mut sink,
            &cancellation,
        )
        .expect("passthrough");
        assert_eq!(sink.into_inner(), jpeg.bytes.as_ref());

        let error = encode_to(
            &jpeg,
            request(OutputEncoding::Jpeg),
            &mut Cursor::new(Vec::new()),
            &cancellation,
        )
        .expect_err("JPEG quality must not be silently ignored for encoded input");
        assert_eq!(error.code(), ErrorCode::UnsupportedPixelFormat);
    }

    #[test]
    fn mono_rgb_and_bgr_convert_without_an_encoded_staging_buffer() {
        let cancellation = CancellationToken::new();
        let cases = [
            frame(PixelFormat::Mono8, vec![0, 255], 2, 1),
            frame(PixelFormat::Rgb8, vec![255, 0, 0, 0, 255, 0], 2, 1),
            frame(PixelFormat::Bgr8, vec![0, 0, 255, 0, 255, 0], 2, 1),
        ];
        for frame in &cases {
            for encoding in [
                OutputEncoding::Jpeg,
                OutputEncoding::Png,
                OutputEncoding::Tiff,
            ] {
                let mut sink = Cursor::new(Vec::new());
                let encoded =
                    encode_to(frame, request(encoding), &mut sink, &cancellation).expect("encode");
                assert_eq!(encoded.bytes, sink.get_ref().len() as u64);
                let decoded = ImageReader::new(Cursor::new(sink.into_inner()))
                    .with_guessed_format()
                    .expect("format")
                    .decode()
                    .expect("decode");
                assert_eq!(decoded.dimensions(), (2, 1));
            }
        }
    }

    #[test]
    fn invalid_source_mislabelling_is_rejected() {
        let cancellation = CancellationToken::new();
        let invalid = frame(PixelFormat::Rgb8, vec![1, 2], 1, 1);
        let error = encode_to(
            &invalid,
            request(OutputEncoding::Png),
            &mut Cursor::new(Vec::new()),
            &cancellation,
        )
        .expect_err("bad length");
        assert_eq!(error.code(), ErrorCode::UnsupportedPixelFormat);

        let raw = frame(PixelFormat::Mono8, vec![1], 1, 1);
        let error = encode_to(
            &raw,
            request(OutputEncoding::Passthrough),
            &mut Cursor::new(Vec::new()),
            &cancellation,
        )
        .expect_err("bad passthrough");
        assert_eq!(error.code(), ErrorCode::UnsupportedPixelFormat);
    }

    #[test]
    fn output_limit_and_cancellation_are_typed() {
        let frame = frame(PixelFormat::Rgb8, vec![255, 0, 0], 1, 1);
        let cancellation = CancellationToken::new();
        let mut tiny = request(OutputEncoding::Png);
        tiny.maximum_output_bytes = 4;
        let error = encode_to(&frame, tiny, &mut Cursor::new(Vec::new()), &cancellation)
            .expect_err("bounded");
        assert_eq!(error.code(), ErrorCode::ResourceLimit);

        cancellation.cancel();
        let error = encode_to(
            &frame,
            request(OutputEncoding::Raw),
            &mut Cursor::new(Vec::new()),
            &cancellation,
        )
        .expect_err("cancelled");
        assert_eq!(error.code(), ErrorCode::CaptureCancelled);
    }

    #[test]
    fn output_identity_and_frame_validation_reject_unsupported_combinations() {
        assert_eq!(
            output_identity(PixelFormat::Jpeg, OutputEncoding::Passthrough).unwrap(),
            ("jpg", "image/jpeg")
        );
        assert_eq!(
            output_identity(PixelFormat::Mono8, OutputEncoding::Jpeg).unwrap(),
            ("jpg", "image/jpeg")
        );
        assert_eq!(
            output_identity(PixelFormat::Rgb8, OutputEncoding::Png).unwrap(),
            ("png", "image/png")
        );
        assert_eq!(
            output_identity(PixelFormat::Bgr8, OutputEncoding::Tiff).unwrap(),
            ("tiff", "image/tiff")
        );
        assert_eq!(
            output_identity(PixelFormat::Jpeg, OutputEncoding::Raw).unwrap(),
            ("raw", "application/octet-stream")
        );
        for (source, output) in [
            (PixelFormat::Mono8, OutputEncoding::Passthrough),
            (PixelFormat::Jpeg, OutputEncoding::Jpeg),
            (PixelFormat::Jpeg, OutputEncoding::Png),
            (PixelFormat::Jpeg, OutputEncoding::Tiff),
        ] {
            assert_eq!(
                output_identity(source, output).unwrap_err().code(),
                ErrorCode::UnsupportedPixelFormat
            );
        }

        let cancellation = CancellationToken::new();
        let empty = frame(PixelFormat::Mono8, vec![], 1, 1);
        assert_eq!(
            encode_to(
                &empty,
                request(OutputEncoding::Raw),
                &mut Cursor::new(Vec::new()),
                &cancellation,
            )
            .unwrap_err()
            .code(),
            ErrorCode::UnsupportedPixelFormat
        );
        let source = frame(PixelFormat::Mono8, vec![1], 1, 1);
        let mut no_capacity = request(OutputEncoding::Raw);
        no_capacity.maximum_output_bytes = 0;
        assert_eq!(
            encode_to(
                &source,
                no_capacity,
                &mut Cursor::new(Vec::new()),
                &cancellation,
            )
            .unwrap_err()
            .code(),
            ErrorCode::ResourceLimit
        );
        let mut invalid_quality = request(OutputEncoding::Raw);
        invalid_quality.jpeg_quality = 0;
        assert_eq!(
            encode_to(
                &source,
                invalid_quality,
                &mut Cursor::new(Vec::new()),
                &cancellation,
            )
            .unwrap_err()
            .code(),
            ErrorCode::BadArgs
        );
    }

    #[test]
    fn declared_jpeg_must_decode_and_match_the_declared_dimensions() {
        let cancellation = CancellationToken::new();
        let malformed = frame(PixelFormat::Jpeg, vec![0xff, 0xd8, 0xff], 1, 1);
        assert_eq!(
            encode_to(
                &malformed,
                request(OutputEncoding::Passthrough),
                &mut Cursor::new(Vec::new()),
                &cancellation,
            )
            .expect_err("truncated JPEG bytes must not be persisted as an image")
            .code(),
            ErrorCode::UnsupportedPixelFormat
        );

        let mut encoded = Cursor::new(Vec::new());
        JpegEncoder::new(&mut encoded)
            .encode(&[10, 20, 30], 1, 1, ExtendedColorType::Rgb8)
            .expect("fixture JPEG");
        let wrong_dimensions = frame(PixelFormat::Jpeg, encoded.into_inner(), 2, 1);
        assert_eq!(
            encode_to(
                &wrong_dimensions,
                request(OutputEncoding::Passthrough),
                &mut Cursor::new(Vec::new()),
                &cancellation,
            )
            .expect_err("declared dimensions are part of the frame contract")
            .code(),
            ErrorCode::UnsupportedPixelFormat
        );
    }

    #[test]
    fn sink_failures_and_a_post_write_cancellation_never_report_success() {
        let source = frame(PixelFormat::Mono8, vec![7], 1, 1);
        let cancellation = CancellationToken::new();
        assert_eq!(
            encode_to(
                &source,
                request(OutputEncoding::Raw),
                &mut WriteFailSink,
                &cancellation,
            )
            .expect_err("write failure")
            .code(),
            ErrorCode::PersistenceFailed
        );
        assert_eq!(
            encode_to(
                &source,
                request(OutputEncoding::Raw),
                &mut FlushFailSink::default(),
                &cancellation,
            )
            .expect_err("flush failure")
            .code(),
            ErrorCode::PersistenceFailed
        );

        let cancellation = CancellationToken::new();
        let mut sink = CancelOnWriteSink {
            cancellation: cancellation.clone(),
            bytes: Vec::new(),
        };
        assert_eq!(
            encode_to(
                &source,
                request(OutputEncoding::Raw),
                &mut sink,
                &cancellation,
            )
            .expect_err("cancellation racing with the final write must win")
            .code(),
            ErrorCode::CaptureCancelled
        );
        assert_eq!(sink.bytes, vec![7]);
    }

    #[test]
    fn bounded_writer_tracks_seek_high_water_and_typed_violations() {
        let cancellation = CancellationToken::new();
        let mut sink = Cursor::new(Vec::new());
        {
            let mut writer = BoundedWriter::new(&mut sink, 2, &cancellation);
            assert_eq!(std::io::Write::write(&mut writer, &[1, 2]).unwrap(), 2);
            writer.seek(SeekFrom::Start(1)).unwrap();
            assert_eq!(std::io::Write::write(&mut writer, &[3]).unwrap(), 1);
            assert_eq!(writer.high_water, 2);
            assert!(writer.seek(SeekFrom::Start(3)).is_err());
            assert!(matches!(writer.violation, Some(WriteViolation::Limit)));
        }
        assert_eq!(sink.into_inner(), vec![1, 3]);

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut sink = Cursor::new(Vec::new());
        let mut writer = BoundedWriter::new(&mut sink, 2, &cancellation);
        assert!(std::io::Write::write(&mut writer, &[1]).is_err());
        assert!(matches!(writer.violation, Some(WriteViolation::Cancelled)));
    }

    #[test]
    fn bgr_pixel_helpers_preserve_channel_order_and_names() {
        let mut converted = [0_u8; 6];
        bgr_to_rgb(&[1, 2, 3, 4, 5, 6], &mut converted);
        assert_eq!(converted, [3, 2, 1, 6, 5, 4]);
        let view = BgrView::new(&[1, 2, 3, 4, 5, 6], 2, 1);
        assert_eq!(view.get_pixel(1, 0).0, [6, 5, 4]);
        assert_eq!(pixel_name(PixelFormat::Mono8), "Mono8");
        assert_eq!(pixel_name(PixelFormat::Rgb8), "RGB8");
        assert_eq!(pixel_name(PixelFormat::Bgr8), "BGR8");
        assert_eq!(pixel_name(PixelFormat::Jpeg), "JPEG");
    }

    #[test]
    fn supported_partial_sink_contract_is_seekable() {
        fn assert_seekable_partial_sink<W: Write + Seek>() {}

        // StorageRoot supplies an exclusively-created File; Cursor is its deterministic test
        // equivalent. A write-only stream is intentionally not a valid image persistence sink.
        assert_seekable_partial_sink::<Cursor<Vec<u8>>>();
        assert_seekable_partial_sink::<std::fs::File>();
    }

    struct WriteFailSink;

    impl Write for WriteFailSink {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("simulated partial-file write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Seek for WriteFailSink {
        fn seek(&mut self, _position: SeekFrom) -> io::Result<u64> {
            Ok(0)
        }
    }

    #[derive(Default)]
    struct FlushFailSink {
        bytes: Vec<u8>,
    }

    impl Write for FlushFailSink {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("simulated partial-file flush failure"))
        }
    }

    impl Seek for FlushFailSink {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            match position {
                SeekFrom::Start(position) => Ok(position),
                SeekFrom::Current(0) | SeekFrom::End(0) => Ok(self.bytes.len() as u64),
                SeekFrom::Current(_) | SeekFrom::End(_) => {
                    Err(io::Error::other("unexpected encoder seek"))
                }
            }
        }
    }

    struct CancelOnWriteSink {
        cancellation: CancellationToken,
        bytes: Vec<u8>,
    }

    impl Write for CancelOnWriteSink {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(buffer);
            self.cancellation.cancel();
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Seek for CancelOnWriteSink {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            match position {
                SeekFrom::Start(position) => Ok(position),
                SeekFrom::Current(0) | SeekFrom::End(0) => Ok(self.bytes.len() as u64),
                SeekFrom::Current(_) | SeekFrom::End(_) => {
                    Err(io::Error::other("unexpected encoder seek"))
                }
            }
        }
    }
}
