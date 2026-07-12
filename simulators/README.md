# Camera adapter simulator stack

This directory contains the deterministic protocol test stack required by
`IMPLEMENTATION_SPEC.md` section 12. It is acceptance infrastructure, not hardware-compatibility evidence.
Physical-camera validation is waived for this project because no hardware is available; simulator results
do not change the excluded hardware claims.

## Services

- `onvif-sim`: repository-owned ONVIF Device, Media1, Media2, PTZ, snapshot HTTP, and WS-Discovery
  fixture service. A container loads one immutable JSON fixture at startup.
- `mediamtx`: pinned MediaMTX 1.19.2 `ffmpeg` image publishing deterministic H.264 and H.265 test
  streams.
- `toxiproxy`: pinned fault proxy for TCP latency, disconnect, timeout, and half-open scenarios.
- `arv-fake-gv-camera`: built separately from Aravis 0.8.36 because GigE Vision discovery requires
  Linux host or L2-capable networking; ordinary Docker NAT is not accepted as discovery evidence.

All third-party image identities are recorded in `image-lock.json`. The locally built ONVIF image
also pins its Python base by multi-platform index digest.

## Run

```powershell
./simulators/verify.ps1
# On a Linux-backed Docker engine with host/L2 networking:
./simulators/verify.ps1 -LinuxL2 -AravisInterface eth0
```

The normal endpoints exposed on the host are:

- ONVIF device service: `http://127.0.0.1:18080/onvif/device_service`
- ONVIF TLS device service: `https://camera.test:18443/onvif/device_service` using the test-only CA
  at `onvif_sim/fixtures/tls/ca-cert.pem`
- direct WS-Discovery harness: UDP `127.0.0.1:13702`
- TLS WS-Discovery harness: UDP `127.0.0.1:13703`
- RTSP H.264: `rtsp://127.0.0.1:18554/camera`
- RTSP H.265: `rtsp://127.0.0.1:18554/camera-h265`
- Toxiproxy API: `http://127.0.0.1:18474`
- Proxied ONVIF: `http://127.0.0.1:28080`
- Proxied RTSP: `rtsp://127.0.0.1:28554/camera`

### Native RTSP decoder validation

The Rust decoder is validated from a pinned Linux image, on the Compose network,
so `onvif-sim` and `mediamtx` retain their service names and no host-network
shortcut weakens the URI-pinning test. The reproducible coverage runner starts
MediaMTX, builds the image with pinned `cargo-llvm-cov 0.8.7` and matching
`llvm-tools-preview`, runs both ignored decoder tests, and writes a separate
LCOV artifact for each H.264/H.265 fixture and session policy:

```powershell
./simulators/run-rtsp-native-coverage.ps1 -CoverageOutput C:\tmp\camera-adapter-rtsp-coverage
```

It mounts the whole EdgeCommons workspace read-only (the adapter depends on
the sibling Core crate), writes Cargo target and registry state only to named
Docker volumes, and writes only the four requested LCOV artifacts to
`CoverageOutput`. The volumes are intentionally retained for a repeatable fast
rerun; remove them explicitly only when a clean native rebuild is required.
These four fixture-level artifacts prove native decoder execution only. They
are not an aggregate adapter coverage report and must not be used to claim the
project's 90% coverage gate is satisfied.

The image keeps the adapter's Rust 1.85.1 MSRV toolchain for ordinary native
decoder tests. Since `cargo-llvm-cov 0.8.7` itself requires Rust 1.87, the
runner invokes the separately pinned 1.87.0 coverage toolchain only for the
LCOV runs; this is not an adapter MSRV change.

For a one-off decoder-only run without coverage, build the ephemeral image
after bringing up the stack, then run the ignored test once for each codec:

```powershell
docker build -f simulators/rtsp_validation.Dockerfile -t camera-adapter-rtsp-validation .
$network = 'camera-adapter-simulators_default'
$workspace = (Resolve-Path ..).Path
foreach ($path in @('camera', 'camera-h265')) {
  docker run --rm --network $network -v "${workspace}:/edgecommons" -w /edgecommons/camera-adapter `
    -e "CAMERA_ADAPTER_RTSP_URI=rtsp://mediamtx:8554/$path" `
    camera-adapter-rtsp-validation test --features rtsp `
    backend::rtsp::tests::pinned_mediamtx_produces_a_complete_rgb_frame -- --ignored --exact
}
```

Each invocation must produce an exact 320x240 RGB frame of 230,400 bytes.
This proves the native pinned decoder against the deterministic H.264/H.265
streams; it does not replace a physical-camera compatibility test.

For multicast WS-Discovery or GigE Vision evidence, run the relevant service on a Linux host network.
Do not reinterpret bridge/NAT results as multicast or L2 discovery coverage.

On a Linux host with the camera-facing interface selected, start the exact Aravis 0.8.36 fake camera:

```bash
ARAVIS_INTERFACE=eth0 docker compose -f simulators/compose.yaml --profile linux-l2 \
  up --build aravis-fake
```

Its source commit and Debian package snapshot are immutable inputs in `image-lock.json`. The fake camera
uses host networking because a bridge-only success is not valid GigE Vision discovery evidence.

Verify both L2 discovery and acquisition from the same Linux network namespace:

```bash
docker compose -f simulators/compose.yaml --profile linux-l2 exec -T aravis-fake \
  arv-tool-0.8 --gv-discovery-interface=eth0
docker compose -f simulators/compose.yaml --profile linux-l2 exec -T aravis-fake \
  arv-camera-test-0.8 --gv-discovery-interface=eth0 --name=Aravis-Fake-GV01 \
  --width=320 --height=240 --duration=3
```

The same pinned Aravis installation can compile and exercise the adapter's GenICam discovery
helper. This is the native feature gate; it is separate from physical-camera compatibility:

```bash
docker build -f simulators/aravis_fake/AdapterValidation.Dockerfile \
  -t camera-adapter-aravis-validation simulators/aravis_fake
docker run --rm --network host -v "$PWD/..:/edgecommons" -w /edgecommons/camera-adapter \
  camera-adapter-aravis-validation build --features genicam --bin camera-adapter-genicam-discover
docker run --rm --network host --entrypoint /edgecommons/camera-adapter/target/debug/camera-adapter-genicam-discover \
  -v "$PWD/..:/edgecommons" -w /edgecommons/camera-adapter \
  camera-adapter-aravis-validation \
  --interface eth0 --transport gige-vision --max-results 1
```

Use the same selected interface in all three commands. The validation image pins Rust 1.85.1 by
digest—the adapter's declared MSRV—and inherits the exact locally built Aravis 0.8.36 image; it is
only a test builder and is not a shipped runtime image.

The acceptance check requires at least one discovered `Aravis-Fake-GV01`, completed buffers, and zero
failed, missing, or size-mismatched buffers. Replace `eth0` consistently when the selected host interface
has another name.

## Fault fixtures

Set `SIM_FIXTURE` to a mounted fixture path before starting `onvif_sim/server.py`. Included examples
cover hostile returned/discovered URIs, malformed SOAP, and truncated snapshots. The fixture schema also
supports `delayMs`, `oversize`, `wrong-type`, safe/unsafe redirects, connection disconnect, forced SOAP
faults, and DTD responses. Each fault requires a fresh immutable service instance so a client cannot
mutate the adversary during a test.

The committed TLS private key is intentionally public and valid only for this deterministic simulator.
Never use the test CA or server key outside test infrastructure. The certificate covers `camera.test`,
`localhost`, `127.0.0.1`, and `::1`, and is valid from 2026-01-01 through 2036-01-01. Client tests use it
to distinguish an explicitly trusted fixture CA from system trust and hostname-verification failures.

## Security and licensing note

The simulator runs as an unprivileged numeric user, with a read-only filesystem, all Linux capabilities
dropped, and `no-new-privileges`. MediaMTX is MIT licensed, Toxiproxy is MIT licensed, Aravis is LGPL-2.1
or later, GStreamer is LGPL-2.1 or later, FFmpeg licensing depends on its compiled codec set, and the
Docker Official Python image contains Debian/Python components under their respective licenses. A release
must refresh the dependency/license scan and vulnerability evidence for the exact digests in
`image-lock.json`; this note is not that scan.
