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
| TR-CAPACITY | Validated for 15-minute simulator smoke | `simulators/run-capacity-validation-container.sh` runs the ignored Linux short proof in a pinned Rust 1.85.1/Python image, with source mounted read-only, named Cargo volumes, dropped capabilities, no-new-privileges, tmpfs, and no workload network. It initializes only Cargo volumes through a restricted root setup container, then runs prefetch/workload as the invoking host uid:gid and confirms artifact-directory writability with a create/remove probe before Cargo; it never relaxes artifact permissions to world-writable. It exercises 1,024 configured entries, 256 enabled SimBackend sessions and live actors, and 32 concurrent 8MP captures with bounded resource/process and router-latency evidence. It records pre-runtime, startup-peak, and final-roster RSS, rejecting peak idle-session growth above one eighth of the 256-frame 8MP equivalent. A new/empty artifact directory receives a write-once run manifest and per-test SHA-256 attestations after schema/scope/value validation; Git-less archive runs require a full commit revision plus a real exact staged tarball, whose SHA-256 the wrapper computes itself. Lab-5950x evidence recorded 2026-07-12 from `7dadd09c35e96abfa7fdfedc9c7a9d65cc11a421` and staged bundle `0c887aa1ae9c9766efaa443f3e94885b6534225a6bda5fea00c26d34a28f2063`: the short proof passed with a 4,931,584-byte startup delta against a 255,688,704-byte bound, and the 900-second smoke passed with 450 direct captures, 15 reconnects, 5 reloads, 180 scheduled jobs for each of eight cameras, and a terminal roster of 256 online actors. The 24-hour soak execution is explicitly deferred to a later phase and is not a current gate; neither test may be represented as that soak. |
| TR-RECOVERY | Validated locally | Catalog/outbox crash-recovery and stable-envelope tests. |
| TR-SEC | Validated locally | SSRF/DNS/XML/decompression/path/credential and no-overwrite tests; deployment threat review remains required. |
| TR-OBS | Validated locally | Readiness, storage and outbox alarm tests. |
| TR-RUNTIME | Validated locally | Startup/command races, linearizable readiness, atomic reload rejection, and safe supervisor-retry tests. |
| TR-DEPLOY | Blocked | HOST simulator smoke exists; Greengrass and kind/hardware-cluster gates are not recorded. |
| TR-INTEGRATION | Blocked | File-replicator and bottling-company evidence is not recorded. |
| TR-VALIDATION | Validated for simulator/native coverage | On Linux lab `enp7s0`, the committed `4ecb245512b9479c41eabc5f899efa0d75ac7944` source archive `c206cade567518b8fe8c157355c1badbc84fe15204cb9a4d56872d6fc5bdff9b` produced 42,899/46,992 covered lines (91.29%) in the hardened `standalone,native-all` aggregate. The scope includes 350 deterministic serial library tests, four pinned MediaMTX H.264/H.265 first-frame/warm-session fixtures, same-container Aravis discovery, and a two-frame Aravis capture fixture. Ordinary tests remain network-none; live fixtures are separately scoped. This is not L2, cross-container/cross-host GigE, physical-camera, hardware-compatibility, or global-adapter coverage evidence. Windows Docker Desktop is invalid evidence for the unfulfilled L2 claim. Scale/soak is tracked separately. |
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
bash simulators/run-native-all-validation.sh --skip-build --coverage-output /home/marc/camera-adapter-native-all-validation-20260712T160300/coverage-native-all --interface enp7s0
bash simulators/run-capacity-validation-container.sh --artifact-dir /home/marc/camera-adapter-capacity-validation-20260712T153000/artifacts --source-revision 7dadd09c35e96abfa7fdfedc9c7a9d65cc11a421 --source-bundle /home/marc/camera-adapter-capacity-validation-20260712T153000/camera-adapter-capacity-lab-20260712T153000.tar.gz --soak-duration 15m
```

No physical camera is represented as passing. Physical-camera validation is waived because hardware is not
available; the [compatibility register](reference/compatibility.md) records the resulting exclusion from
hardware-compatibility claims.

## Repeatable capacity command

The following Linux/lab command writes a new `short-capacity-summary.json` artifact and refuses to
overwrite existing evidence:

```bash
bash simulators/run-capacity-validation-container.sh \
  --artifact-dir /home/marc/camera-adapter-capacity-short-$(date +%Y%m%dT%H%M%S) \
  --source-revision <full-commit> \
  --source-bundle /home/marc/camera-adapter-capacity-source.tar.gz
```

Appending `--soak-duration 15m` runs the short proof first and then writes
`fifteen-minute-soak-summary.json` for the partial mixed-workload simulator smoke. The 24-hour soak is
deferred and no command in this record starts it.
