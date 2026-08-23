//! The simulator `playlist` pattern driven through the real capture and storage path.
//!
//! The unit tests beside `SimBackend` prove what the backend hands upwards. These prove the part
//! that only the assembled component can: a replayed file reaches disk through the ordinary
//! finalization -- sidecar first, then the image made visible atomically -- and the digest the
//! announcement and the sidecar carry is the digest of the file that was replayed.

use std::fs;

use image::ExtendedColorType;
use image::codecs::jpeg::JpegEncoder;
use sha2::{Digest, Sha256};

use super::*;

/// A deterministic JPEG fixture.
fn jpeg_fixture(width: u32, height: u32, tint: u8) -> Vec<u8> {
    let pixels: Vec<u8> = (0..(width * height * 3))
        .map(|index| (index as u8).wrapping_mul(7).wrapping_add(tint))
        .collect();
    let mut bytes = Vec::new();
    JpegEncoder::new_with_quality(std::io::Cursor::new(&mut bytes), 92)
        .encode(&pixels, width, height, ExtendedColorType::Rgb8)
        .expect("the fixture encoder produces a JPEG");
    bytes
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// One camera replaying `playlist` into `root`, with the sidecar the design requires.
fn playlist_config(root: &Path, playlist: &Path) -> AdapterConfig {
    let raw = json!({
        "component": {
            "global": {
                "output": {
                    "rootDirectory": root.display().to_string(),
                    "writeMetadataSidecar": true
                }
            },
            "instances": [{
                "id": "camera-a",
                "backend": {
                    "type": "sim",
                    "captureDelayMs": 0,
                    "frame": {
                        "pattern": {
                            "playlist": { "directory": playlist.display().to_string() }
                        }
                    }
                },
                "defaultCaptureProfile": "replay",
                "captureProfiles": { "replay": { "output": { "encoding": "passthrough" } } }
            }]
        }
    });
    AdapterConfig::from_core_reload(&Config::from_value(COMPONENT_NAME, "gw-01", raw).unwrap())
        .unwrap()
}

/// Submits one capture and returns the committed terminal body.
async fn replay_once(runtime: &CameraRuntime, request_id: &str) -> serde_json::Value {
    let accepted = runtime
        .submit_capture(
            "camera-a".to_string(),
            request_id.to_string(),
            None,
            None,
            serde_json::Map::new(),
            format!("{request_id}-correlation"),
            "sb/capture-submit",
            crate::admission::CapturePriority::Submitted,
        )
        .await
        .unwrap();
    let capture_id = match accepted {
        crate::catalog::AcceptJobOutcome::Inserted(record) => record.capture_id,
        other => panic!("expected a newly accepted capture, got {other:?}"),
    };
    let terminal = wait_for_terminal(runtime, &capture_id).await;
    assert_eq!(terminal.state, crate::model::JobState::Succeeded);
    terminal
        .terminal_result
        .clone()
        .expect("a succeeded capture commits its terminal body")
}

/// The whole point of the pattern: what lands on disk is the image that was replayed.
///
/// A synthetic pattern can prove the plumbing runs. It cannot prove that the file a vision component
/// picks up is the file an operator put in the directory -- and that is exactly the claim a
/// line-clearance rehearsal rests on. So this asserts the digest three times over, on the three
/// artifacts a consumer can actually reach: the announced terminal body, the durable sidecar beside
/// the image, and the installed bytes themselves.
#[tokio::test]
async fn a_replayed_capture_installs_the_file_it_names_with_that_file_s_own_digest() {
    let directory = TempDir::new().unwrap();
    let playlist = TempDir::new().unwrap();
    let first = jpeg_fixture(24, 16, 5);
    let second = jpeg_fixture(12, 12, 9);
    fs::write(playlist.path().join("a.jpg"), &first).unwrap();
    fs::write(playlist.path().join("b.jpg"), &second).unwrap();

    let runtime = runtime(
        playlist_config(directory.path(), playlist.path()),
        &directory,
    )
    .await;
    runtime
        .start_supervisor("camera-a".to_string(), runtime.engine("camera-a").unwrap())
        .unwrap();
    wait_for_online(&runtime, "camera-a").await;

    let body = replay_once(&runtime, "replay-1").await;
    let digest = sha256_hex(&first);

    assert_eq!(body["backendMetadata"]["playlist"]["sourcePath"], "a.jpg");
    assert_eq!(body["backendMetadata"]["playlist"]["index"], 0);
    assert_eq!(body["frame"]["width"], 24);
    assert_eq!(body["frame"]["height"], 16);
    assert_eq!(body["frame"]["pixelFormat"], "JPEG");
    assert_eq!(body["image"]["encoding"], "passthrough");
    assert_eq!(body["image"]["contentType"], "image/jpeg");
    assert_eq!(body["image"]["bytes"], first.len());
    assert_eq!(
        body["image"]["sha256"], digest,
        "the announced digest must be the digest of the replayed file, not of a re-encode of it"
    );

    let installed = fs::read(body["image"]["absolutePath"].as_str().unwrap()).unwrap();
    assert_eq!(
        installed, first,
        "a passthrough replay installs the playlist file byte for byte"
    );

    let sidecar_relative = body["image"]["metadataSidecarRelativePath"]
        .as_str()
        .expect("the sidecar is written before the image becomes visible");
    let sidecar: serde_json::Value =
        serde_json::from_slice(&fs::read(directory.path().join(sidecar_relative)).unwrap())
            .expect("the sidecar is the committed terminal document");
    assert_eq!(sidecar["image"]["sha256"], digest);
    assert_eq!(
        sidecar["backendMetadata"]["playlist"]["sourcePath"],
        "a.jpg"
    );
    assert_eq!(sidecar["backendMetadata"]["playlist"]["index"], 0);

    let next = replay_once(&runtime, "replay-2").await;
    assert_eq!(next["backendMetadata"]["playlist"]["sourcePath"], "b.jpg");
    assert_eq!(next["backendMetadata"]["playlist"]["index"], 1);
    assert_eq!(next["image"]["sha256"], sha256_hex(&second));
    assert_eq!(next["frame"]["width"], 12);

    runtime.shutdown().await;
}
