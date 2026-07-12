# Acceptance matrix

This is the live local release record for the binding design. `Validated` means evidence exists for the
current branch; `Blocked` means the requirement remains a release gate and is not represented as complete;
`In progress` means a repeatable harness exists but its required evidence has not yet been recorded; and
`Waived` records an explicit project decision and never implies untested hardware compatibility. A
`Deferred` activity is intentionally postponed to a later validation phase and is not a current gate.

| ID | Status | Source and test/evidence |
|---|---|---|
| TR-GOAL | Validated | `IMPLEMENTATION_SPEC.md`; unit tests reject excluded formats and unsafe requests. |
| TR-ARCH | Validated | `src/{runtime,actor,admission,registry,supervisor}.rs`; bounded actor/admission tests. |
| TR-LIFE | Validated | Runtime/supervisor reconnect and capability tests. |
| TR-JOB | Validated | `src/{jobs,catalog,idempotency}.rs`; catalog, cancellation, idempotency, recovery tests. |
| TR-STORAGE | Validated | `src/storage.rs`; no-clobber, sidecar ordering, fsync, path and pressure tests. |
| TR-CONFIG | Validated locally | Closed configuration tests plus pre-commit apply/rejection and safe supervisor-retry regressions. |
| TR-BACKEND | Validated for simulator contract | In-process, ONVIF/WS-Discovery, RTSP and Aravis simulator evidence. Physical-camera validation is waived because no hardware is available; no hardware compatibility claim is made. |
| TR-PHYSICAL | Waived | Project owner confirmed physical-camera tests will not be run because no camera hardware is available. Supported-model, firmware, and device-timing claims remain explicitly excluded. |
| TR-CORE-P1 | Validated | Four-language local MQTT matrix (16 deferred + 16 confirmed edges) and lab-5950x IPC matrix (16 + 16 verified edges). |
| TR-MSG | Validated locally | Command/router/outbox correlation and broker-outage tests; deployment capture smoke is recorded below. |
| TR-CMD | Validated locally | Closed request-schema and runtime command tests. |
| TR-CAPACITY | In progress | `simulators/run-capacity-validation-container.sh` runs the ignored Linux short proof in a pinned Rust 1.85.1/Python image, with source mounted read-only, named Cargo volumes, dropped capabilities, no-new-privileges, tmpfs, and no workload network. It initializes only Cargo volumes through a restricted root setup container, then runs prefetch/workload as the invoking host uid:gid and confirms artifact-directory writability with a create/remove probe before Cargo; it never relaxes artifact permissions to world-writable. It exercises 1,024 configured entries, 256 enabled SimBackend sessions and live actors, and 32 concurrent 8MP captures with bounded resource/process and router-latency evidence. It records pre-runtime, startup-peak, and final-roster RSS, rejecting peak idle-session growth above one eighth of the 256-frame 8MP equivalent. A new/empty artifact directory receives a write-once run manifest and per-test SHA-256 attestations after schema/scope/value validation; Git-less archive runs require a full commit revision plus a real exact staged tarball, whose SHA-256 the wrapper computes itself. Its optional `--soak-duration 15m` follows that proof with a partial mixed-traffic simulator smoke (schedules, commands, PTZ/status, reconnects, and valid reloads), then produces a validated, manifest-chained human-readable `capacity-test-report.md` and report attestation. No lab artifact is recorded yet. The 24-hour soak execution is explicitly deferred to a later phase and is not a current gate; neither test may be represented as that soak. |
| TR-RECOVERY | Validated locally | Catalog/outbox crash-recovery and stable-envelope tests. |
| TR-SEC | Validated locally | SSRF/DNS/XML/decompression/path/credential and no-overwrite tests; deployment threat review remains required. |
| TR-OBS | Validated locally | Readiness, storage and outbox alarm tests. |
| TR-RUNTIME | Validated locally | Startup/command races, linearizable readiness, atomic reload rejection, and safe supervisor-retry tests. |
| TR-DEPLOY | Blocked | HOST simulator smoke exists; Greengrass and kind/hardware-cluster gates are not recorded. |
| TR-INTEGRATION | Blocked | File-replicator and bottling-company evidence is not recorded. |
| TR-VALIDATION | Blocked | On Linux lab `enp7s0`, serial `standalone,onvif,genicam` library coverage with the pinned Aravis helper and two-frame fake-camera fixture is 41,416/44,950 lines (92.14%): 348 ordinary tests passed (2 ignored) and the ignored native fixture passed. The merged JSON report records `src/backend/genicam_aravis.rs` at 1,876/2,305 lines (81.39%). RTSP-enabled native scope is 40,951/44,583 lines (91.85%) across 341 ordinary native-feature tests and four live MediaMTX fixtures; `src/backend/rtsp.rs` is 3,251/3,899 lines (83.38%). Separately, the hardened Linux lab `standalone,native-all` serial library scope passed 349 tests (4 ignored), but measures 40,720/46,799 lines (87.01%), below the 90% gate; its source entries are `src/backend/genicam_aravis.rs` 1,005/2,305 (43.60%), `src/backend/rtsp.rs` 2,120/3,899 (54.37%), and `src/backend/onvif.rs` 6,753/7,202 (93.77%). This deterministic combined-feature scope excludes the live MediaMTX and Aravis fake-camera fixtures and makes no L2, cross-container, cross-host, physical-camera, or global-adapter coverage claim. Windows Docker Desktop is invalid evidence for the unfulfilled L2 claim. Scale/soak is tracked separately. |
| TR-DOCS | Validated | Diátaxis set, exact command/event/terminal references, deployment runbooks, and compatibility register were audited against source. |

## Recorded local commands

```text
cargo +stable fmt --all -- --check
cargo +1.85.0 test --locked --no-default-features --lib -- --test-threads 1
cargo +1.85.0 test --locked --no-default-features --features standalone,onvif --lib -- --test-threads 1
CARGO_TARGET_DIR=/tmp/camera-adapter-coverage-portable cargo llvm-cov --locked --no-default-features --features standalone,onvif --fail-under-lines 90
simulators/verify.ps1 -LinuxL2 -AravisInterface eth0
bash simulators/run-genicam-native-coverage.sh --interface enp7s0 --coverage-output /tmp/camera-adapter-genicam-coverage --skip-build --in-container-fake --aggregate-coverage
docker compose -f deploy/docker/compose.yaml up --build -d --wait
CAMERA_ADAPTER_DOCKER_E2E=1 CAMERA_ADAPTER_DOCKER_E2E_HOST=127.0.0.1 CAMERA_ADAPTER_DOCKER_E2E_PORT=1884 cargo test --locked --no-default-features --features standalone --test docker_capture_submit
simulators/run-rtsp-native-coverage.ps1 -CoverageOutput C:\tmp\camera-adapter-rtsp-coverage
bash simulators/run-native-all-validation.sh --skip-build --coverage-output /home/marc/camera-adapter-native-all-validation-20260712T092046/coverage
```

No physical camera is represented as passing. Physical-camera validation is waived because hardware is not
available; the [compatibility register](reference/compatibility.md) records the resulting exclusion from
hardware-compatibility claims.

## Available capacity command

The following is an unexecuted Linux/lab command, not recorded evidence. It writes a new
`short-capacity-summary.json` artifact and refuses to overwrite an existing one:

```bash
bash simulators/run-capacity-validation-container.sh \
  --artifact-dir /home/marc/camera-adapter-capacity-short-$(date +%Y%m%dT%H%M%S) \
  --source-revision <full-commit> \
  --source-bundle /home/marc/camera-adapter-capacity-source.tar.gz
```

Appending `--soak-duration 15m` runs the short proof first and then writes
`fifteen-minute-soak-summary.json` for the partial mixed-workload simulator smoke. The 24-hour soak is
deferred and no command in this record starts it.
