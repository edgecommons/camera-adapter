# camera-adapter — component notes

EdgeCommons **southbound camera adapter** (Rust). Crate/binary `camera-adapter`, GG component
`com.mbreissi.edgecommons.CameraAdapter`. Depends on the `edgecommons` Rust library. If this repo
lives inside the EdgeCommons org umbrella workspace, read its root `AGENTS.md` first (org repo map,
design-fidelity contract, validation matrix, platform/transport model); everything below is this
component's own detail.

## What it is

Connects to cameras (ONVIF/RTSP, bare RTSP, GenICam/Aravis, plus an in-process simulator), captures
still images on demand and on schedules, and publishes capture announcements onto the Unified
Namespace. It is a **camera**, not a signal adapter: a data point is an image on disk, not a
`SouthboundSignalUpdate`. The image bytes are the data plane (files, delivered by `file-replicator`);
the bus carries control and terminal metadata (`app/image/*` announcements, `evt` operator events).

It serves the canonical `southbound_health` metric plus operational families (`camera_captures`,
`camera_queue`, `CameraCommand`), the standardized lifecycle verbs `sb/pause` / `sb/resume` /
`sb/reconnect`, and 16 domain `sb/*` verbs (`sb/capture` and friends, `sb/ptz*`, queue verbs) on the
D-U28 component command inbox — SOUTHBOUND.md §2.2 sanctions `sb/capture`-style domain verbs. Runs on
`GREENGRASS` / `HOST` / `KUBERNETES` via `edgecommons`, with no platform branching in this component.

## The seam

`src/backend/`'s `CameraBackendFactory` / `CameraSession` trait pair is the one place protocol
knowledge lives; the `SimBackend` implementation is compiled into every build and drives the
deterministic test suite. Everything above the seam — the durable job catalog (`src/catalog.rs`), the
fleet capture scheduler (`src/dispatch.rs`), the command plane (`src/runtime/command.rs`), the metric
families (`src/observability.rs`) — is written against the trait and does not change when a protocol
is added. The boundary rule: a backend knows protocols; it does not know EdgeCommons topics, the UNS,
envelopes, or metrics.

## Config location

This component's settings live under `component.global` / `component.instances[]` in the EdgeCommons
config document (`config.schema.json` is the contract; `src/config.rs` is the parser); the sibling
sections (`tags`, `hierarchy`, `identity`, `messaging`, `metricEmission`, `logging`, `heartbeat`) are
the standard `edgecommons` envelope, owned by the canonical schema and not redeclared here.
`test-configs/` and `docs/sample-configurations.md` carry runnable examples.

## Validation expectations

- `cargo test` covers every module against the simulator and a mocked device-control channel — no
  network, no broker, no camera required.
- The coverage gate is **90% line + a 95% diff gate** (`.github/workflows/ci.yml`'s `coverage` job):
  `cargo llvm-cov report --summary-only --fail-under-lines 90` plus a `diff-cover --fail-under=95` on
  the changed lines. Do not lower a gate or exclude testable code to pass it — add tests.
- The `rtsp` (GStreamer) and `genicam` (Aravis) backends need system libraries; they are compiled and
  exercised in the simulator-backed containers (`simulators/rtsp_validation.Dockerfile`,
  `simulators/native_all_validation.Dockerfile`) and the `rtsp-backend` CI job, not the default build.
  Their live-fixture suites are `#[ignore]`d and run against MediaMTX / real devices.
- `Cargo.lock` **is committed** (git-sourced), so a fresh clone and CI resolve reproducibly.
- `edgecommons component validate` checks this repo's config against `config.schema.json`.

## Org conventions this component inherits

- Southbound routing/availability error codes are the standardized `BAD_ARGS` / `NO_SUCH_INSTANCE` /
  `DEVICE_UNAVAILABLE` (SOUTHBOUND.md §2.2); domain codes (`CAPTURE_*`, `PTZ_*`, …) are camera-specific.
- Instance routing is D-EIP-13/D-U28: body `instance`, optional iff exactly one camera is configured.
- Builders/facades are the construction path (`app()`, `events()`, `commands()`, `MetricBuilder`) —
  never hand-built topics or envelopes.
- Runtime artifacts (durable state DBs, captured images, TLS fixtures, logs, build output) stay out of
  Git.
