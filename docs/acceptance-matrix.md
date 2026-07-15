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
| TR-BACKEND | Validated for simulator contract, incl. cross-host L2 GenICam | In-process, ONVIF/WS-Discovery, RTSP and Aravis simulator evidence, and a two-box cross-host L2 GigE run (TR-L2-GIGE). Physical-camera validation is waived because no hardware is available; no hardware compatibility claim is made. |
| TR-PHYSICAL | Waived | Project owner confirmed physical-camera tests will not be run because no camera hardware is available. Supported-model, firmware, and device-timing claims remain explicitly excluded. |
| TR-CORE-P1 | Validated | Four-language local MQTT matrix (16 deferred + 16 confirmed edges) and lab-5950x IPC matrix (16 + 16 verified edges). |
| TR-MSG | Validated locally | Command/router/announcement correlation and broker-outage tests; deployment capture smoke is recorded below. |
| TR-CMD | Validated locally | Closed request-schema and runtime command tests. |
| TR-CAPACITY | Validated for 15-minute simulator smoke | `simulators/run-capacity-validation-container.sh` runs the ignored Linux short proof in a pinned Rust 1.85.1/Python image, with source mounted read-only, named Cargo volumes, dropped capabilities, no-new-privileges, tmpfs, and no workload network. A restricted root initializer mounts only each named Cargo volume and has only `CHOWN` plus `DAC_READ_SEARCH`, allowing repeatable ownership initialization of private Cargo directories; prefetch/workload run as the invoking host uid:gid and an artifact writability probe runs before Cargo. It never relaxes evidence permissions to world-writable. The harness exercises 1,024 configured entries, 256 enabled SimBackend sessions and live actors, and 32 concurrent 8MP captures with bounded resource/process and router-latency evidence. A new/empty artifact directory receives a write-once run manifest, per-result SHA-256 attestations, and an attested human-readable report after schema/scope/value validation. Lab-5950x evidence recorded 2026-07-12 from `cb1d0ce0e99d7c315db2ca1c9036e52d901cc468` and exact staged bundle `519907d036fc369bc59a70c96a3f12b888cf8fe10620aa794ad290b90056b5ba`: the short proof passed with a 5,079,040-byte startup delta against a 255,688,704-byte bound; the 900.63-second mixed-workload smoke passed with 450 accepted capture submissions (not a terminal-completion count), 15 reconnects, 5 reloads, 180 accepted scheduled jobs for each of eight cameras, and a terminal roster of 256 online cameras and 256 actors. The 24-hour soak execution is explicitly deferred to a later phase and is not a current gate; this evidence is not hardware, 24-hour-soak, 10,000-job, broker-outage, or GenICam/L2 evidence. |
| TR-RECOVERY | Validated locally | Catalog crash-recovery and durable-terminal-body announcement tests. |
| TR-SEC | Validated locally | SSRF/DNS/XML/decompression/path/credential and no-overwrite tests; deployment threat review remains required. |
| TR-OBS | Validated locally | Readiness, storage and messaging-degradation alarm tests. |
| TR-RUNTIME | Validated locally | Startup/command races, linearizable readiness, atomic reload rejection, and safe supervisor-retry tests. |
| TR-DEPLOY | Validated on both deployed platforms; command flow and camera VLAN not covered | **GREENGRASS (2026-07-14, lab-5950x, real Java nucleus, thing `lab-5950x`):** the component was deployed from a recipe + artifact with `greengrass-cli deployment create` and ran 4 cameras x 45 scheduled captures. Terminal announcements were observed over real IPC; the run exposed a live defect (`medium`/`large` thumbnails destroyed 90 of 180 announcements with `NOMEM` from the component SDK's 10,000-byte static send buffer), which the transport-aware `ThumbnailPolicy` fixed -- the re-run lost 0 announcements. 0 of 48 sidecars and 0 of 100 catalog rows carried a preview, on the device. **KUBERNETES (2026-07-14, k3s v1.35.5 on lab-5950x):** `--platform KUBERNETES -c CONFIGMAP` with the config mounted from a ConfigMap and identity from the Downward API; 3 cameras produced announcements on the MQTT wire whose thumbnails were decoded from the captured bytes at 160x120 / 320x240 / 640x360 (small / medium / large, aspect preserved, native protobuf bytes with no base64). 0 announcements lost, 0 clamps, 0 of 69 sidecars carried a preview. **Still not covered:** the southbound command inbox over Greengrass IPC (only the publish/announce direction was exercised), camera-VLAN capture against physical hardware (waived under TR-PHYSICAL -- no camera hardware), and PVC-backed streaming on a StatefulSet. |
| TR-INTEGRATION | Blocked | File-replicator and bottling-company evidence is not recorded. |
| TR-VALIDATION | Validated for simulator/native coverage | On Linux lab `enp7s0`, the committed `4ecb245512b9479c41eabc5f899efa0d75ac7944` source archive `c206cade567518b8fe8c157355c1badbc84fe15204cb9a4d56872d6fc5bdff9b` produced 42,899/46,992 covered lines (91.29%) in the hardened `standalone,native-all` aggregate. The scope includes 350 deterministic serial library tests, four pinned MediaMTX H.264/H.265 first-frame/warm-session fixtures, same-container Aravis discovery, and a two-frame Aravis capture fixture. Ordinary tests remain network-none; live fixtures are separately scoped. This same-container run is not itself physical-camera or hardware-compatibility evidence; cross-host L2 GigE is now validated separately (see TR-L2-GIGE below). Windows Docker Desktop remains invalid evidence for the L2 claim. Scale/soak is tracked separately. **The lockfile breakage that stopped this harness (untracked `Cargo.lock` + `:ro` `--locked` mounts since 3c0d83d) is fixed:** the rtsp, capacity and genicam harnesses have each been re-run against current code. The genicam native-coverage harness (`standalone,onvif,genicam`) passes 626 library tests plus the live GigE fixture; the capacity 15-minute soak passes with its new H2-H5 gates. |
| TR-L2-GIGE | Validated (2026-07-15) | Two-box cross-host L2 GigE Vision, the gap TR-VALIDATION previously disclaimed. Fleet host `192.168.1.193` (`ens33`) ran `arv-fake-gv-camera` (Aravis 0.8.36 from source) + an MQTT broker with host networking; the adapter ran on `lab-5950x` `192.168.1.229` (`enp7s0`) from a runnable genicam image (`simulators/two-box/`, Aravis baked in from source), on a different physical machine. The adapter's production genicam discovery found `Aravis-Fake-GV01` at 192.168.1.193 over GVCP, then captured on a 5s schedule: 25 real Mono8 512x512 PNGs in ~2 minutes, each with a `sha256` and a sidecar naming `backend: genicam-aravis`, `firmware: 0.8.36`, `transport: gige-vision`, `captureMode: software-trigger`; announcements reached the broker (`app/image/captured`, camera `ONLINE`); the native connect stayed bounded (41 threads, 0 errors, stable). Reproduce with `simulators/two-box/run-genicam-l2.sh`. Not physical-camera evidence (waived) and not a benchmark. |
| TR-XHOST-RTSP | Validated (2026-07-15); T1 blocked by env | Two-box ONVIF/RTSP over a real cross-host wire. Fleet on `192.168.1.193` (mediamtx serving H.264 720p + the ONVIF simulator answering SOAP/snapshot, RTSP URI and ONVIF endpoint repointed at the fleet IP + an MQTT broker); the adapter on `lab-5950x` `192.168.1.229` ran 32 warm ONVIF-RTSP cameras through its production `onvif-rtsp` backend. In 55s: 639 real 1280x720 RGB8 `rtsp-frame` captures; **0 session restarts** (the B3 decode-gate storm the old 4-permit process-global would show at 12-16 warm streams is absent at 32); **19 `database is locked` events under 32 concurrent catalog writers, each retried, with 0 camera disconnects** (B6's SQLITE_BUSY-retry fix holding under exactly the contention the review predicted); threads bounded (585), CPU ~10%. This exercises B3, D3, R1 and B6. **T1 (WS-Discovery) not completed cross-host:** the adapter's multicast probes egress `enp7s0` correctly (`ip route get 239.255.255.250 dev enp7s0`) but never reach the bridged VM (tcpdump on `ens33:3702` saw none) -- GVCP broadcast floods and works, WS-Discovery multicast is pruned by the hypervisor bridge/switch IGMP handling; the adapter side is correct and the sim answers WS-Discovery in the same-container harness, so this is an environment limit, not a defect. Reproduce with `simulators/two-box/run-rtsp-onvif-l2.sh`. **X5 / B6 severity, edge-class storage:** the same 32-stream run with the state directory on a dm-delay device at 15ms fsync (edge eMMC/SD class) produced 77 captures (vs 639 on NVMe -- ~8x lower durable throughput) and 52 `database is locked` events (vs 19 -- ~3x the contention), with STILL 0 storage-pressure rejections, 0 session restarts and 0 camera disconnects. B6's severity (contention scaling with slow fsync) is real and its fix (SQLITE_BUSY retries, never disconnects) holds under it. |
| TR-DOCS | Validated | Diátaxis set, exact command/event/terminal references, deployment runbooks, and compatibility register were audited against source. |

## Recorded local commands

```text
cargo +stable fmt --all -- --check
cargo +1.85.0 test --no-default-features --lib -- --test-threads 1
cargo +1.85.0 test --no-default-features --features standalone,onvif --lib -- --test-threads 1
CARGO_TARGET_DIR=/tmp/camera-adapter-coverage-portable cargo llvm-cov --no-default-features --features standalone,onvif --fail-under-lines 90
simulators/verify.ps1 -LinuxL2 -AravisInterface eth0
bash simulators/run-genicam-native-coverage.sh --interface enp7s0 --coverage-output /tmp/camera-adapter-genicam-coverage --skip-build --in-container-fake --aggregate-coverage
docker compose -f deploy/docker/compose.yaml up --build -d --wait
CAMERA_ADAPTER_DOCKER_E2E=1 CAMERA_ADAPTER_DOCKER_E2E_HOST=127.0.0.1 CAMERA_ADAPTER_DOCKER_E2E_PORT=1884 cargo test --no-default-features --features standalone --test docker_capture_submit
simulators/run-rtsp-native-coverage.ps1 -CoverageOutput C:\tmp\camera-adapter-rtsp-coverage
bash simulators/run-native-all-validation.sh --skip-build --coverage-output /home/marc/camera-adapter-native-all-validation-20260712T160300/coverage-native-all --interface enp7s0
bash simulators/run-capacity-validation-container.sh --artifact-dir /home/marc/camera-adapter-capacity-validation-20260712T153000/artifacts --source-revision 7dadd09c35e96abfa7fdfedc9c7a9d65cc11a421 --source-bundle /home/marc/camera-adapter-capacity-validation-20260712T153000/camera-adapter-capacity-lab-20260712T153000.tar.gz --soak-duration 15m
cargo +1.90.0 build --release --no-default-features --features greengrass,onvif
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
