# Acceptance matrix

This is the live local release record for the binding design. `Validated` means evidence exists for the
current branch; `Blocked` means the requirement remains a release gate and is not represented as complete.

| ID | Status | Source and test/evidence |
|---|---|---|
| TR-GOAL | Validated | `IMPLEMENTATION_SPEC.md`; unit tests reject excluded formats and unsafe requests. |
| TR-ARCH | Validated | `src/{runtime,actor,admission,registry,supervisor}.rs`; bounded actor/admission tests. |
| TR-LIFE | Validated | Runtime/supervisor reconnect and capability tests. |
| TR-JOB | Validated | `src/{jobs,catalog,idempotency}.rs`; catalog, cancellation, idempotency, recovery tests. |
| TR-STORAGE | Validated | `src/storage.rs`; no-clobber, sidecar ordering, fsync, path and pressure tests. |
| TR-CONFIG | Validated locally | Closed configuration tests plus pre-commit apply/rejection and safe supervisor-retry regressions. |
| TR-BACKEND | Partially validated | In-process, ONVIF/WS-Discovery, RTSP and Aravis simulator evidence; physical register is blocked. |
| TR-CORE-P1 | Validated | Four-language local MQTT matrix (16 deferred + 16 confirmed edges) and lab-5950x IPC matrix (16 + 16 verified edges). |
| TR-MSG | Validated locally | Command/router/outbox correlation and broker-outage tests; deployment capture smoke is recorded below. |
| TR-CMD | Validated locally | Closed request-schema and runtime command tests. |
| TR-CAPACITY | Blocked | 256-camera/32-capture resource graph and 24-hour soak evidence are not recorded. |
| TR-RECOVERY | Validated locally | Catalog/outbox crash-recovery and stable-envelope tests. |
| TR-SEC | Validated locally | SSRF/DNS/XML/decompression/path/credential and no-overwrite tests; deployment threat review remains required. |
| TR-OBS | Validated locally | Readiness, storage and outbox alarm tests. |
| TR-RUNTIME | Validated locally | Startup/command races, linearizable readiness, atomic reload rejection, and safe supervisor-retry tests. |
| TR-DEPLOY | Blocked | HOST simulator smoke exists; Greengrass and kind/hardware-cluster gates are not recorded. |
| TR-INTEGRATION | Blocked | File-replicator and bottling-company evidence is not recorded. |
| TR-VALIDATION | Blocked | Functional suites are green, but the authoritative Cargo LLVM ONVIF coverage gate is 88.85%; scale/soak and physical evidence also remain. |
| TR-DOCS | Validated | Diátaxis set, exact command/event/terminal references, deployment runbooks, and compatibility register were audited against source. |

## Recorded local commands

```text
cargo +stable fmt --all -- --check
cargo +1.85.0 test --locked --no-default-features --lib -- --test-threads 1
cargo +1.85.0 test --locked --no-default-features --features standalone,onvif --lib -- --test-threads 1
simulators/verify.ps1 -LinuxL2 -AravisInterface eth0
docker compose -f deploy/docker/compose.yaml up --build -d --wait
CAMERA_ADAPTER_DOCKER_E2E=1 CAMERA_ADAPTER_DOCKER_E2E_HOST=127.0.0.1 CAMERA_ADAPTER_DOCKER_E2E_PORT=1884 cargo test --locked --no-default-features --features standalone --test docker_capture_submit
```

No physical camera is represented as passing. See the [compatibility register](reference/compatibility.md)
for each required class and its `NOT RUN — HARDWARE UNAVAILABLE` status.
