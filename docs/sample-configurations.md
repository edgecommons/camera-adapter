# Sample Configurations

Complete, ready-to-adapt configurations for the **Camera Adapter**
(`com.mbreissi.edgecommons.CameraAdapter`), one per realistic scenario. Each sample is a valid fragment of
an EdgeCommons configuration document; the prose after it explains **what every option does and how it
changes runtime behavior** — which backend the camera uses, when it captures, how the frame is encoded and
persisted, and what message the capture produces.

For the exhaustive option list see [reference/configuration.md](reference/configuration.md); for the
commands and terminal messages these cameras answer and emit see
[reference/messaging-interface.md](reference/messaging-interface.md); for the reasoning behind the
lifecycle, admission, and durability models see [explanation.md](explanation.md); and for task recipes see
[how-to-guides.md](how-to-guides.md).

> **How config reaches the component.** The adapter reads one JSON document from the `-c/--config` source,
> which defaults by platform: `HOST` → `FILE`, `GREENGRASS` → `GG_CONFIG` (the deployment's
> `ComponentConfiguration`), `KUBERNETES` → `CONFIGMAP` (a mounted directory). Adapter-owned settings live
> under `component.global` and `component.instances[]`; the sibling sections (`messaging`, `logging`,
> `heartbeat`, `metricEmission`, `credentials`, …) are standard EdgeCommons sections. Both adapter objects
> are **closed** — an unknown key is rejected and the component refuses to start (or, on reload, keeps the
> configuration it already has), so a typo fails loudly instead of being ignored.

This page is organized as:

- **[The instance and profile model](#the-instance-and-profile-model-read-this-first)** — how a camera and
  its capture profiles are wired. Read this first; every example below builds on it.
- **[§1](#1-command-only-simulator)**–**[§2](#2-scheduled-simulator-fleet-member)** — the deterministic
  `sim` backend: command-only, then scheduled.
- **[§3](#3-onvif-snapshot)**–**[§4](#4-onvif-with-rtsp-fallback)** — ONVIF network cameras: plain snapshot,
  then snapshot with RTSP fallback.
- **[§5](#5-bare-rtsp-camera-no-onvif)** — a camera addressed by a raw RTSP URL, with no ONVIF.
- **[§6](#6-gige-vision-genicam)**–**[§7](#7-usb3-vision-genicam)** — GenICam machine-vision cameras over
  GigE Vision and USB3 Vision.
- **[§8](#8-ptz-policy-on-a-camera)** — enabling PTZ safely on a camera.
- **[§9](#9-a-group-schedule)** — capturing several cameras together on a cron.
- **[§10](#10-a-thumbnail-preview-on-the-bus)** — attaching a preview to the published result.

---

## The instance and profile model (read this first)

Two pieces of structure drive every example: how a **camera instance** is wired, and how its **capture
profiles** describe the frame. Both are spelled out once here.

### A camera is one `component.instances[]` entry

Every instance is one camera and one EdgeCommons identity. It has four required parts and a few optional
ones:

| Field | Meaning |
|-------|---------|
| `id` (required) | The camera's instance id — stamped on every message it emits and used to select it in a command body (`instance`). |
| `backend` (required) | The protocol implementation, an object **tagged by `type`**: `{ "type": "sim" \| "onvif-rtsp" \| "rtsp" \| "genicam-aravis", … }`. The backend's own fields sit flat beside `type` inside this object. |
| `defaultCaptureProfile` (required) | The profile used when a command or schedule names none. |
| `captureProfiles` (required) | A map of named profiles (below). |
| `enabled` | Defaults `true`; a disabled camera is validated but does not accept actuation. |
| `resourceGroup` | Names a shared acquisition cap (a NIC or USB controller) declared under `global.limits.resourceGroups`. |
| `schedules` | Per-camera cron schedules. Omit it for a **command-only** camera. |
| `ptz` | Per-camera PTZ policy (see [§8](#8-ptz-policy-on-a-camera)). |

### A capture profile describes the frame and the file

Each named profile carries a **required `output` object** and optional overrides:

- `output.encoding` (required) — `passthrough`, `jpeg`, `png`, `tiff`, or `raw`. `output.jpegQuality`
  defaults to `90`. **`passthrough` requires a JPEG source and `jpeg` refuses one** — the adapter never
  silently re-encodes, so pass a camera's JPEG through with `passthrough` and use `jpeg` only to *encode* a
  raw frame.
- `captureMode` — for ONVIF, `snapshot-uri` (default) or `rtsp-frame`; for GenICam, `software-trigger`; for
  `sim`, `simulated`. Usually left to the backend default.
- `captureInterlock` — `reject` (default), `stopAndSettle`, or `allow`: what a capture does when the camera
  is moving under PTZ. Note the **camelCase** `stopAndSettle`.
- `pixelFormat` (GenICam) — a **case-sensitive** token: `Mono8`, `RGB8`, `BGR8`, or `JPEG`. `rgb8` in
  lower case is not a valid value.
- `thumbnail` — `{ "size": "small" \| "medium" \| "large" }` to attach a preview to the result (see
  [§10](#10-a-thumbnail-preview-on-the-bus)).
- `timeoutMs`, `offlinePolicy`, `queueExpiryMs`, `maximumFrameBytes`, and the GenICam
  `width`/`height`/`offsetX`/`offsetY`/`exposureMicros`/`gain` sensor overrides, when a camera needs them.

### Every production document sets the durable roots

The instances below are fragments. A complete document also sets absolute durable roots and a component
token — the minimum HOST shape is:

```json
{
  "component": {
    "token": "camera-adapter",
    "global": {
      "output": { "rootDirectory": "/var/lib/edgecommons/camera-adapter-output", "writeMetadataSidecar": true },
      "state": { "directory": "/var/lib/edgecommons/camera-adapter-state" }
    },
    "instances": [ /* one or more of the instances below */ ]
  }
}
```

`component.token` is a **core** EdgeCommons field (not adapter-owned): it is the component's UNS identity —
the `{component}` segment in `ecv1/{device}/{component}/{instance}/{class}` — under which the adapter
publishes and is addressed on the bus (its command inbox, state, metrics, and per-camera messages). Set it
to `camera-adapter` as shown. If you omit it, the core falls back to the short form of the full component
name (`com.mbreissi.edgecommons.CameraAdapter` → `CameraAdapter`), and the component would appear at
`ecv1/{device}/CameraAdapter/...` instead of the documented `ecv1/{device}/camera-adapter/...` that
consumers subscribe to.

Both roots must be absolute and durable, and the file-name or camera-directory template must include
`{captureId}` so two captures can never collide on one path. Camera credentials are **never** inline: they
are `{ "$secret": "<name>" }` references to the EdgeCommons credential service, allowed only at
`backend.credentials` and `backend.tls.ca`. See the [deployment runbooks](deployment/host.md) for the roots,
ownership, and service identity.

---

## 1. Command-only simulator

```json
{
  "id": "sim-a",
  "backend": { "type": "sim", "frame": { "width": 640, "height": 480, "pixelFormat": "RGB8", "pattern": "color-bars" } },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "output": { "encoding": "png" } } }
}
```

The `sim` backend synthesizes a deterministic 640×480 `RGB8` colour-bar frame entirely in process — no
network, no device. With **no `schedules`**, this camera is command-only: it captures nothing until an
`sb/capture` or `sb/capture-submit` naming `instance: "sim-a"` arrives. When one does, the actor generates
the frame and the `inspection` profile encodes it losslessly to **PNG** (valid because the source is raw
`RGB8`, not JPEG), writes the file under the output root, and announces `ImageCaptured` with the file's path,
size, and `sha256`. This is the fixture behind the [tutorial](tutorial.md) and the right starting point for
exercising the command and message plumbing before a real camera exists.

## 2. Scheduled simulator fleet member

```json
{
  "id": "sim-hourly",
  "backend": { "type": "sim" },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "output": { "encoding": "jpeg", "jpegQuality": 90 } } },
  "schedules": [{ "id": "hourly", "cron": "0 0 * * * *", "timezone": "UTC", "captureProfile": "inspection" }]
}
```

The same simulator with a **schedule** added. `cron` is a **six-field, seconds-inclusive** expression, so
`0 0 * * * *` fires at the top of every hour in the schedule's IANA `timezone`. Each occurrence is submitted
through the *same* path as a command capture — same admission, deadline, persistence, and terminal message —
so a scheduled capture is indistinguishable from a commanded one on the bus. The `inspection` profile encodes
the raw frame to **JPEG** at quality 90. A schedule's `misfirePolicy` and `overlapPolicy` both default to
`skip`, so a missed or still-running occurrence is dropped rather than piled up; use `coalesce` / `queue`
only when you want the latest missed or one bounded overlap.

## 3. ONVIF snapshot

```json
{
  "id": "dock-camera",
  "backend": {
    "type": "onvif-rtsp",
    "deviceServiceUrl": "https://camera.example/onvif/device_service",
    "credentials": { "$secret": "cameras/dock" },
    "mediaProfile": "main",
    "allowedUriHosts": ["camera.example"]
  },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "captureMode": "snapshot-uri", "output": { "encoding": "png" } } }
}
```

A real ONVIF network camera. The adapter reaches the device service at `deviceServiceUrl`, authenticates
with the whole credential secret `cameras/dock` (whose UTF-8 JSON value holds exactly `username` and
`password`), selects the `main` media profile, and — in the default `snapshot-uri` mode — fetches the
camera's snapshot over DNS-pinned HTTP. `allowedUriHosts` is the endpoint allowlist: the adapter will only
follow a snapshot or RTSP URI whose host is listed, on top of the per-connection address validation it
always does. Because the `inspection` profile asks for **PNG** and the camera returns JPEG, the adapter
decodes the snapshot and re-encodes it losslessly; ask for `passthrough` instead if you want the camera's
original JPEG bytes stored verbatim (and their digest to be the digest of exactly what the camera sent).

## 4. ONVIF with RTSP fallback

```json
{
  "id": "line-camera",
  "backend": {
    "type": "onvif-rtsp",
    "deviceServiceUrl": "https://line-camera.example/onvif/device_service",
    "credentials": { "$secret": "cameras/line" },
    "mediaProfile": "main",
    "allowedUriHosts": ["line-camera.example"],
    "captureMode": "snapshot-uri",
    "rtspFallback": true
  },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "output": { "encoding": "jpeg", "jpegQuality": 90 } } }
}
```

The same ONVIF camera, hardened against a flaky snapshot endpoint. It still captures via `snapshot-uri`
first, but `rtspFallback: true` lets a **truncated snapshot** — one whose data ends before the picture does —
fall back to extracting a complete frame from the camera's RTSP stream instead of failing. RTSP frame
extraction needs the adapter built with the `rtsp` feature and the matching GStreamer runtime packaged
(see the [HOST runbook](deployment/host.md)); without it, an incomplete snapshot simply fails with a stable
error. Set `captureMode: "rtsp-frame"` instead of the fallback when you want to *require* RTSP extraction on
every capture rather than only on a bad snapshot.

## 5. Bare-RTSP camera (no ONVIF)

```json
{
  "id": "line-cam",
  "backend": {
    "type": "rtsp",
    "url": "rtsp://line-cam.example:554/stream1",
    "credentials": { "$secret": "cameras/line" },
    "allowedUriHosts": ["line-cam.example"]
  },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": {
    "inspection": { "captureMode": "rtsp-frame", "output": { "encoding": "jpeg", "jpegQuality": 90 } }
  }
}
```

For a camera that speaks RTSP but not ONVIF, the `rtsp` backend connects straight to the stream `url` — no
device service, no media-profile discovery. On `connect()` the adapter performs the full RTSP
`DESCRIBE`/`SETUP` handshake, authenticates with the `cameras/line` secret (RTSP Basic/Digest — the URL
must **not** embed `user:pass@`), and validates the stream's codec, so an unreachable or misconfigured
camera is reported offline immediately rather than only at the first capture. `captureMode` is `rtsp-frame`
— its only valid value here — so each capture decodes one complete frame to RGB and the profile encodes it
(to JPEG in this example). `allowedUriHosts` pins the endpoint on top of the per-connection address
validation the backend always does. For an encrypted camera, use an `rtsps://` URL with `tls.ca` /
`tls.verifyHostname`; plaintext `rtsp://` requires `allowInsecure: true`. The `rtsp` backend is compiled
only with the native `rtsp` feature (it needs the GStreamer runtime) — see the
[deployment runbooks](deployment/host.md). It has no PTZ, snapshot, or discovery.

## 6. GigE Vision (GenICam)

```json
{
  "id": "gige-a",
  "backend": { "type": "genicam-aravis", "selector": { "serial": "CAM-001" }, "transport": "gige-vision", "interface": "enp2s0" },
  "defaultCaptureProfile": "raw",
  "captureProfiles": { "raw": { "output": { "encoding": "raw" } } }
}
```

A machine-vision camera on the Linux-native GenICam backend (built only with the `genicam` feature). The
`selector` binds the camera by **exactly one** stable key — here its `serial` — so a reboot or DHCP change
never rebinds the instance to a different device; `mac`, `deviceId`, and `ip` are the other selector choices.
`transport: "gige-vision"` and the explicit host `interface` tell the backend which NIC to use; GigE
discovery and acquisition also require listing that interface in `global.discovery.eligibleInterfaces`, since
the adapter never sweeps all NICs. The `raw` profile writes the camera's raw pixel buffer straight to disk
with no re-encoding — the right choice when a downstream pipeline wants the sensor bytes. For a smaller file,
choose `png`/`tiff` (from a raw pixel format) or `jpeg`.

## 7. USB3 Vision (GenICam)

```json
{
  "id": "usb-a",
  "backend": { "type": "genicam-aravis", "selector": { "deviceId": "usb3-1" }, "transport": "usb3-vision" },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "output": { "encoding": "png" } } }
}
```

The same backend over USB3 Vision. A USB camera has no NIC, so there is no `interface` and no discovery-
interface list — the `deviceId` selector binds it directly, and `transport: "usb3-vision"` picks the USB
path. The `inspection` profile encodes each frame to PNG. USB3 Vision under Kubernetes needs an explicit,
least-privilege device mapping; see the [Kubernetes runbook](deployment/kubernetes.md).

## 8. PTZ policy on a camera

```json
{
  "id": "dock-ptz",
  "backend": {
    "type": "onvif-rtsp",
    "deviceServiceUrl": "https://dock-ptz.example/onvif/device_service",
    "credentials": { "$secret": "cameras/dock-ptz" },
    "mediaProfile": "main",
    "allowedUriHosts": ["dock-ptz.example"]
  },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "output": { "encoding": "jpeg" }, "captureInterlock": "reject" } },
  "ptz": { "enabled": true, "maximumContinuousMoveMs": 10000, "allowPresetMutation": false }
}
```

PTZ is off until `ptz.enabled` is set. `maximumContinuousMoveMs` caps a continuous move: an `sb/ptz`
continuous command **must** carry a `timeoutMs` no larger than this, and the adapter arms its own stop for
that instant so the camera always stops on time even if the operator disconnects. `allowPresetMutation:
false` lets operators recall presets but not overwrite them. The profile's `captureInterlock: "reject"` means
a capture requested while the camera is moving is refused with `CAMERA_MOVING` rather than producing a
motion-blurred frame; `stopAndSettle` would instead stop the camera and wait `ptz.settleMs` before capturing.
See [how-to: move or stop PTZ safely](how-to-guides.md#move-or-stop-ptz-safely).

## 9. A group schedule

```json
"captureGroupSchedules": [{
  "id": "line-a-sync",
  "cron": "0 */5 * * * *",
  "timezone": "America/New_York",
  "instances": ["cam-01", "cam-02", "cam-03"],
  "captureProfile": "inspection",
  "profileOverrides": { "cam-03": "inspection-wide" },
  "timeoutMs": 30000
}]
```

A group schedule lives under `component.global.captureGroupSchedules`, not under any one camera, because it
crosses instances. Every five minutes it captures `cam-01`, `cam-02`, and `cam-03` **as one group**: the
occurrence is submitted exactly like an `sb/capture-group` command, so acceptance is **all-or-nothing**, the
members share one durable `captureGroupId`, and one collated terminal notification reports them together.
`captureProfile` applies to every member; `profileOverrides` swaps it per camera (here `cam-03` uses a wider
profile). `instances` names at least two cameras and at most `limits.maxCamerasPerGroup`. `overlapPolicy` is
evaluated against the whole group, so a slow member holds the next occurrence back rather than letting the
group tear into halves a cycle apart. This is software fan-out — it does not claim hardware-synchronized
acquisition.

## 10. A thumbnail preview on the bus

```json
{
  "id": "dock-preview",
  "backend": {
    "type": "onvif-rtsp",
    "deviceServiceUrl": "https://camera.example/onvif/device_service",
    "credentials": { "$secret": "cameras/dock" },
    "mediaProfile": "main",
    "allowedUriHosts": ["camera.example"]
  },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": {
    "inspection": { "output": { "encoding": "png" }, "thumbnail": { "size": "medium" } }
  }
}
```

Because the full image never travels on the bus, `thumbnail` attaches a small JPEG preview to the *published
result* so a console can show the picture without fetching the file. `size` bounds the thumbnail's longest
edge — `small` 160 px, `medium` 320 px, `large` 640 px — with aspect preserved and no upscaling. The preview
is announcement-only: it is never written to the sidecar, never stored in the catalog, and carries no
digest. The transport caps the size it can carry — Greengrass IPC carries only `small` and reduces a larger
request to it, while MQTT carries all three — and a preview never costs the result: if the message cannot be
published with it, the result is announced again without it. See the
[messaging reference](reference/messaging-interface.md#capture-thumbnail) for the wire shape.
