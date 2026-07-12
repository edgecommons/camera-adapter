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

### Short simulated capacity proof

`run-capacity-validation-container.sh` is the Linux-only entry point for the first capacity-validation
slice. It builds `capacity_validation.Dockerfile`, which pins the same Rust 1.85.1 image digest used by
the native validators and adds Python 3 for evidence validation. It mounts the whole workspace read-only,
keeps Cargo target/registry/git state in named volumes, and runs the workload with a read-only root
filesystem, `/tmp` tmpfs, all Linux capabilities dropped, no-new-privileges, and `--network none`.
The in-process MQTT fixture uses loopback and continues to work in that namespace. A preceding bridge-only
`cargo fetch --locked` may populate named Cargo cache volumes; it does not run the workload or write
evidence. This removes any dependency on host Cargo or Python. To work with rootless/user-namespace Docker,
the wrapper determines the invoking host uid:gid, uses a temporary root setup container with only `CHOWN`
to initialize the three named Cargo volumes, and then runs prefetch and workload as that uid:gid with
`HOME=/tmp`. Before Cargo runs, the workload identity creates and removes a private probe in the new/empty
host-owned artifact directory. The wrapper never makes evidence directories or files world-writable.

The inner `run-capacity-validation.sh` enables the non-default `capacity-harness` feature, which isolates
this live-lab-only workload from ordinary `cargo llvm-cov --lib` coverage. No production code is excluded:
the feature contains only test instrumentation and the ignored workload, whose evidence is recorded in the
explicit artifact instead of a unit-coverage percentage.

```bash
bash simulators/run-capacity-validation-container.sh \
  --artifact-dir /home/marc/camera-adapter-capacity-short-$(date +%Y%m%dT%H%M%S) \
  --source-revision <full-commit> \
  --source-bundle /home/marc/camera-adapter-capacity-source.tar.gz
```

The short proof configures 1,024 camera entries, opens 256 enabled simulated sessions, submits one
32-member 3,264×2,448 Mono8 group (7,990,272 bytes per frame), and verifies that a thirty-third capture
remains behind saturated global/resource-group/byte/disk admission until capacity is released. It also
records 20 router-boundary samples each for `sb/list`, `sb/status`, and PTZ stop while acquisition is
saturated. The command samples exclude MQTT transport time; Core's built-in `ping` has no equivalent
adapter-boundary timer yet and is intentionally not claimed by this slice.

The requested artifact directory must be new or empty. Before it starts Cargo, the runner creates
`capacity-run-manifest.json` with the invoked command, UTC start, source revision/provenance, pinned
toolchain, and kernel. It creates that file with exclusive creation and makes it read-only. After each
test it validates the produced JSON's exact schema, scope, and required acceptance values, then creates a
separate read-only hash attestation chained to the run manifest (`short-capacity-artifact-attestation.json`
and, when requested, `fifteen-minute-soak-artifact-attestation.json`). The attestation contains the
SHA-256 of its JSON artifact; the runner refuses to reuse a populated evidence directory.

When the staged source intentionally has no `.git` directory, pass both the full commit revision and the
exact uploaded source tarball to the container wrapper. The wrapper rejects a missing, empty, or symlinked
bundle and computes its SHA-256 itself before passing that digest to the inner runner. Archive provenance
is not treated as immutable without both identifiers. The short JSON schema is
`camera-adapter-short-capacity/v1` and has these bounded sections:

```bash
bash simulators/run-capacity-validation-container.sh \
  --artifact-dir /home/marc/camera-adapter-capacity-15m-$(date +%Y%m%dT%H%M%S) \
  --source-revision <full-commit> \
  --source-bundle /home/marc/camera-adapter-capacity-source.tar.gz \
  --soak-duration 15m
```

| Field | Evidence |
|---|---|
| `configuredCameras` / `enabledSimulatedSessions` | Exact 1,024-entry roster and 256 connected sessions. |
| `frame` / `concurrentCaptureTarget` | Exact 8MP Mono8 workload and 32-capture target. |
| `idleSessionMemory` | RSS immediately before runtime startup, roster-online RSS, their delta, and a machine-independent bound of one eighth of 256 full 8MP Mono8 frames. This proves idle sessions did not allocate a frame per camera. |
| `resourceSamples` | Global/resource-group permits, in-flight and disk bytes, encoder/writer availability, queue depth, RSS, threads, and open descriptors at bounded phases. |
| `commandLatency` | Minimum, p50, p95, and maximum router-boundary latency for the three exercised commands. |
| terminal states | Group success count and the deferred thirty-third capture outcome. |
| `omittedFromThisShortRun` | Explicit exclusions so this artifact cannot be mistaken for wider validation. |

### Bounded 15-minute simulator smoke

For a separate, bounded mixed-workload check on a true Linux host such as `lab-5950x`, add
`--soak-duration 15m`. The runner always executes the preceding short 8MP proof first, then writes
`fifteen-minute-soak-summary.json` alongside it:

```bash
bash simulators/run-capacity-validation-container.sh \
  --artifact-dir /home/marc/camera-adapter-capacity-15m-$(date +%Y%m%dT%H%M%S) \
  --source-revision <full-commit> \
  --source-bundle /home/marc/camera-adapter-capacity-source.tar.gz \
  --soak-duration 15m
```

The second test retains the 1,024 configured/256 enabled SimBackend roster, switches its traffic to
640×480 Mono8 frames, and proves sustained runtime activity: eight schedules fire every five seconds
(at least 120 durable occurrences per scheduled camera), one direct capture is submitted every two
seconds, `sb/list`/`sb/status`/PTZ stop are timed every five seconds, a session reconnect is requested
every minute, and a valid configuration generation is reapplied every three minutes. The artifact records
the command latency samples, resource/process samples, accepted scheduled-job counts, and operation counts.

This is deliberately a **partial simulator smoke**, not a 24-hour soak, full 8MP-duration test,
10,000-job workload, broker-outage exercise, encoder/writer saturation graph, Core ping benchmark, or
hardware compatibility result. It must be reported with that scope even when it passes.

When the 15-minute mode succeeds, the runner additionally writes a deterministic,
human-readable `capacity-test-report.md` only after both JSON artifacts have passed validation and both
JSON attestations have chained to the run manifest. The report contains source/toolchain/kernel/command
provenance; an explicit PASS verdict and criteria-versus-observed-results table; short-proof capacity,
idle-session RSS, p95, and resource results; 15-minute workload and per-camera schedule counts; p95 and
resource summaries; artifact hashes; and explicit exclusions. It labels direct captures as accepted
submissions, not a terminal-completion count. A separate
`capacity-test-report-artifact-attestation.json` binds the report SHA-256 to the same run manifest and
is identified in the report with verification instructions. No report is written for short-only mode
because it would be incomplete.

This is not the 24-hour soak. The full soak execution is deferred to a later validation phase and is not
a current gate. It remains necessary before any future scale-performance or general-release claim, along
with the separate 10,000-job, broker-outage, and encoder/writer saturation scenarios. Do not
reinterpret the short proof as protocol, L2, cross-container, physical-camera, or hardware compatibility
evidence.

### Native RTSP decoder validation

The Rust decoder is validated from a pinned Linux image, on the Compose network,
so `onvif-sim` and `mediamtx` retain their service names and no host-network
shortcut weakens the URI-pinning test. The reproducible coverage runner starts
MediaMTX, builds the image with pinned `cargo-llvm-cov 0.8.7` and matching
`llvm-tools-preview`, runs both ignored decoder tests, and writes a separate
LCOV artifact for each H.264/H.265 fixture and session policy. It then runs the
ordinary native-feature library suite plus all four fixtures again to export a
measured aggregate JSON summary with a dedicated `src/backend/rtsp.rs` line
percentage:

```powershell
./simulators/run-rtsp-native-coverage.ps1 -CoverageOutput C:\tmp\camera-adapter-rtsp-coverage
```

It mounts the whole EdgeCommons workspace read-only (the adapter depends on
the sibling Core crate), writes Cargo target and registry state only to named
Docker volumes, and writes only the four requested LCOV artifacts to
`CoverageOutput` plus `rtsp-native-summary.json`. The volumes are intentionally
retained for a repeatable fast rerun; remove them explicitly only when a clean
native rebuild is required. The fixture LCOV files prove individual decoder
paths; the JSON summary measures the native RTSP test scope. Neither is an
adapter-wide coverage report or proof that the project's 90% gate is satisfied.

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
helper. This is the native feature gate; it is separate from physical-camera compatibility. Tag
the locally built fake image with its freshly resolved image hash before building the validation
layer; neither validation Dockerfile has a mutable implicit base:

```bash
fake_image_id=$(docker image inspect --format '{{.Id}}' camera-adapter-simulators-aravis-fake)
fake_image_ref="camera-adapter-aravis-validation-input:${fake_image_id#sha256:}"
docker tag camera-adapter-simulators-aravis-fake "$fake_image_ref"
docker build -f simulators/aravis_fake/AdapterValidation.Dockerfile \
  --build-arg "ARAVIS_RUNTIME_IMAGE=$fake_image_ref" \
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
