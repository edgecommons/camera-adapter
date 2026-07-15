# Two-box GenICam validation — real cross-host L2 GigE Vision

The in-process capacity harness and the same-container genicam harness both run the load generator and
the adapter on one machine. This rig does not: the fake GigE camera runs on one host and the adapter on
another, and the adapter discovers and captures it over **real GVCP/GVSP multicast on the shared LAN**.

Two rigs live here: `run-genicam-l2.sh` (GigE Vision, below) and `run-rtsp-onvif-l2.sh` (ONVIF/RTSP -- reaches B3/D3/R1/B6 with N warm streams; see the note in that script on why WS-Discovery/T1 needs a fleet host on the physical LAN, not a bridged VM).

The GigE rig exists to close the gap `ACCEPTANCE-MATRIX.md` names outright — *"not L2, cross-container/cross-host
GigE, physical-camera evidence"* — for the GenICam backend, which no in-process or same-container test can
reach: cross-host discovery, the bounded native connect (review finding **D4**), and buffer acquisition
against a camera that is genuinely a different host at the far end of a wire.

## Roles (the fleet and the adapter MUST be different physical hosts)

| Role | Runs | Notes |
|---|---|---|
| **build** | the aravis-from-source images | Any Docker host with this repo. Never the edge device. |
| **FLEET** | `arv-fake-gv-camera` + MQTT broker | Linux Docker host, host networking, on the adapter's L2. The broker is kept off the adapter box (review X6). |
| **SUT** | the genicam adapter image | Where the component ships (e.g. `lab-5950x`). Aravis is baked into the image from source; the host needs no Aravis. |

Aravis is built **from source at ≥ 0.8.36** in both images (`simulators/aravis_fake/Dockerfile`). Distribution
packages are older than the 0.8.36 floor `native/aravis-scoped/build.rs` enforces, and the shipped
`camera-adapter/Dockerfile` deliberately excludes `genicam` for exactly that reason — so this validation
image is separate infrastructure, never the shipped artifact, and never runs on the edge device.

## Run

```bash
FLEET_HOST=marc@192.168.1.193 FLEET_IF=ens33 \
SUT_HOST=marc@192.168.1.229   SUT_IF=enp7s0  \
simulators/two-box/run-genicam-l2.sh
```

The script builds both images on the build host, ships them to the fleet and SUT hosts, starts the fake
camera + broker on the fleet, runs the adapter on the SUT, and verifies real frames accumulate with
`backend=genicam-aravis` provenance in their sidecars.

## What a passing run establishes

- The adapter's **production genicam discovery** finds a camera on another host over L2
  (`{"deviceId":"Aravis-Fake-GV01","transport":"gige-vision","interface":"<SUT_IF>"}`).
- The adapter **connects and captures** over GVSP: real Mono8 frames, PNG-encoded, persisted with a
  `sha256` and a metadata sidecar naming `backend: genicam-aravis`, `firmware: 0.8.36`, `transport:
  gige-vision`.
- The capture **announcement** reaches the broker on the fleet host
  (`ecv1/<thing>/camera-adapter/<cam>/app/image/captured`), and the camera reports `ONLINE` on
  `main/state`.
- The native connect stays **bounded** — no thread runaway from the connect path.

## What it does NOT establish

A **physical** GenICam camera (waived — no hardware), and performance/soak numbers. It is a correctness
and reachability proof for the cross-host L2 GigE path, not a benchmark.
