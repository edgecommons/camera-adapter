# camera-adapter

`camera-adapter` is an EdgeCommons Rust southbound component for durable image capture from
ONVIF/RTSP and GenICam cameras. It owns the capture job catalog, bounded admission,
crash-recoverable image persistence, camera actors, and terminal application messages; image bytes are written to
disk rather than sent over MQTT or Greengrass IPC.

## Current implementation status

The current implementation includes durable startup, bounded admission, a SQLite job catalog,
a simulated backend, ONVIF snapshot/RTSP, and optional Linux GenICam/Aravis capture. It remains
short of general release. The [acceptance matrix](ACCEPTANCE-MATRIX.md) is the release record;
its dated evidence is historical validation, not a fresh test run from this documentation review.

Recorded evidence includes four-language local MQTT and real Greengrass IPC core conformance;
Greengrass scheduled capture and announcements; Kubernetes ConfigMap/Downward API capture with
thumbnail decoding; the 1,024-configured/256-session/32-capture short proof and 15-minute simulator
smoke; native-feature coverage; and cross-host simulated GigE and 32-camera RTSP capture.

Remaining gates include the southbound command inbox over Greengrass IPC, PVC-backed streaming
on a StatefulSet, FileReplicator and Dallas integration, and deployment threat review. Cross-host
WS-Discovery remains blocked by multicast forwarding in the test environment. The 24-hour soak
is explicitly deferred and is not a current gate. Physical-camera validation is waived because
hardware is unavailable; simulator evidence does not certify camera models or firmware.

Configured output and state roots are monitored against the output free-space floors. A low or
unreadable root raises the stateful critical `storage-low` alarm and rejects new captures with
`STORAGE_PRESSURE`; state-root pressure also makes the component unready until recovery.

The design remains the binding source for scope and release gates:

- [DESIGN.md](DESIGN.md)
- [implementation requirements and traceability](IMPLEMENTATION_SPEC.md)

## Build

The default build is the standalone ONVIF snapshot path. It does not include native RTSP or
GenICam libraries.

```powershell
cargo build --release
```

Feature choices are explicit:

```powershell
# ONVIF plus native GStreamer RTSP capture (Linux native dependencies required)
cargo build --release --no-default-features --features standalone,onvif,rtsp

# Bare-RTSP-only capture, without the ONVIF backend (native GStreamer required)
cargo build --release --no-default-features --features standalone,rtsp

# Linux Aravis GenICam support (Aravis 0.8.36 or newer required)
cargo build --release --no-default-features --features standalone,onvif,genicam
```

The `rtsp` feature no longer implies `onvif`: it builds the standalone `rtsp` backend (a camera addressed
by a raw `rtsp://`/`rtsps://` URL) and can be enabled with or without `onvif`.

The Rust package declares MSRV 1.85. Use the locked dependency graph; do not update native or
container dependencies opportunistically while building a deployment image.

## Run

The adapter requires at least one enabled, valid camera instance and absolute output storage. On
Linux HOST state defaults to `/var/lib/edgecommons/camera-adapter-state`; on Windows HOST it
defaults to the ProgramData known folder. Windows output uses the accepted portable persistence
profile: exclusive partials, flushed checksums, sidecar-before-final ordering, and standard-library
no-overwrite finalization. A collision or finalization failure is reported as `PERSISTENCE_FAILED`;
this profile does not claim Linux-equivalent hostile-local-actor containment.
The configured output filesystem must support same-directory hard links; an unsupported hard-link
finalization is likewise reported as `PERSISTENCE_FAILED`.
Greengrass has no implicit state-directory fallback.

```powershell
./target/release/camera-adapter --platform HOST --transport MQTT C:\path\to\camera-adapter.json -c FILE C:\path\to\camera-adapter.json
```

The complete deployment runbooks cover durable mounts, service identities, Windows deployment ACL guidance, Docker,
Greengrass, and Kubernetes:

- [HOST deployment](docs/deployment/host.md)
- [Greengrass deployment](docs/deployment/greengrass.md)
- [Kubernetes deployment](docs/deployment/kubernetes.md)
- [simulators and acceptance limits](simulators/README.md)
- [compatibility and validation register](docs/reference/compatibility.md)

## Security boundaries

Camera URLs are validated and pinned before connections, XML and response sizes are bounded, and
camera credentials are references to the EdgeCommons credential service rather than inline JSON
values. Treat the output directory as sensitive operational data: it contains captures and may
reveal facility layout. Do not make the directory broadly writable or bridge absolute paths
northbound without an explicit policy.

## License

BUSL-1.1. See [`LICENSE`](LICENSE).
