# How-to guides

Task recipes for operating the camera adapter. Each assumes you already have a working instance — start from
the [tutorial](tutorial.md) or a [sample configuration](sample-configurations.md) if you do not — and cross-
references the [reference](reference/configuration.md) for the full option set.

## Schedule a camera

Add a named schedule to a camera instance to capture on a cron instead of waiting for a command. A scheduled
occurrence follows the *same* admission, deadline, persistence, and terminal-message path as a command
capture, so it is indistinguishable from one on the bus.

```json
"schedules": [{
  "id": "hourly-inspection",
  "cron": "0 0 * * * *",
  "timezone": "America/New_York",
  "captureProfile": "inspection"
}]
```

`cron` is a **six-field, seconds-inclusive** expression evaluated in the schedule's IANA `timezone`; the
example fires at the top of every hour. Schedule `id`s are stable per camera. `misfirePolicy` and
`overlapPolicy` default to `skip` — a missed or still-running occurrence is dropped rather than accumulated.
Inspect a scheduled job with `sb/capture-status`. A schedule is not a substitute for an idempotent command:
when an upstream caller needs a *particular* result, submit a capture with a `requestId` and read its
outcome.

## Move or stop PTZ safely

Enable PTZ per camera with `ptz.enabled`, then submit `sb/ptz` with a durable `requestId`. A **continuous**
move must carry a bounded `timeoutMs` no larger than the camera's `ptz.maximumContinuousMoveMs`; the adapter
arms its own stop for that instant, so the camera stops on time even if the requester disconnects or the
camera ignores its own timeout.

```json
{
  "instance": "dock-ptz",
  "requestId": "operator-2026-07-11-001",
  "operation": "continuous",
  "velocity": { "pan": 0.2, "tilt": 0.0, "zoom": 0.0 },
  "timeoutMs": 2000
}
```

Use `operation: "stop"` before maintenance — the stop takes a safety lane served ahead of queued controls
and captures, cancelling a capture in progress if it must. Preset *mutation* stays disabled until the
instance sets `ptz.allowPresetMutation: true`, so operators can recall presets without being able to
overwrite them. A capture requested while the camera is moving is governed by the profile's
`captureInterlock` (`reject`, `stopAndSettle`, or `allow`).

## Use ONVIF authentication and TLS

Reference a **whole** credential secret through `backend.credentials` — never inline a username or password.
Its UTF-8 JSON value contains exactly `username` and `password`.

```json
"backend": {
  "type": "onvif-rtsp",
  "deviceServiceUrl": "https://camera.example/onvif/device_service",
  "credentials": { "$secret": "cameras/dock" },
  "mediaProfile": "main",
  "allowedUriHosts": ["camera.example"],
  "tls": { "verifyHostname": true, "ca": { "$secret": "cameras/dock-ca" } }
}
```

Keep `backend.tls.verifyHostname` enabled. For a camera with a private CA, point `backend.tls.ca` at a PEM
secret. Basic authentication over plaintext HTTP is refused unless the component-wide
`global.security.allowBasicOverPlaintext` is explicitly set — reserve that for a controlled development
fixture, never a production camera.

## Tune GenICam conservatively

GenICam is a Linux-native optional feature (the `genicam` build). Bind each camera by **exactly one** stable
selector (`serial`, `mac`, `deviceId`, or `ip`), set the specific host `interface` when the camera is on a
dedicated NIC, and start from the device packet size and buffer count the camera vendor recommends. GigE
discovery and acquisition require listing the interface in `global.discovery.eligibleInterfaces` — the
adapter never sweeps all NICs. Do not treat an ordinary Docker NAT run as proof of GigE multicast discovery
or UDP acquisition; see the [Kubernetes runbook](deployment/kubernetes.md) for the networking a real GigE
camera needs.

## Configure RTSP fallback

Choose `captureMode: "snapshot-uri"` (the default) for normal ONVIF snapshots. Set `backend.rtspFallback:
true` to let a *truncated* snapshot fall back to extracting a complete frame from the RTSP stream, or set
`captureMode: "rtsp-frame"` to require RTSP extraction on every capture. Either RTSP path needs the adapter
built with the `rtsp` feature and the matching GStreamer runtime packaged. In every case, list only the
explicit safe hosts in `backend.allowedUriHosts`.

## Capture from a bare-RTSP camera (no ONVIF)

For a camera that has no ONVIF service, use the `rtsp` backend and point it straight at the stream:

```json
"backend": {
  "type": "rtsp",
  "url": "rtsp://line-cam.example:554/stream1",
  "credentials": { "$secret": "cameras/line" },
  "allowedUriHosts": ["line-cam.example"]
}
```

Put no credentials in the `url` (`rtsp://user:pass@…` is rejected) — supply them as a `$secret`. Use an
`rtsps://` URL with `tls.ca` / `tls.verifyHostname` for an encrypted camera; plaintext `rtsp://` requires
`allowInsecure: true`. The only valid `captureMode` is `rtsp-frame`, and the backend has no PTZ, snapshot,
or discovery. It is built with the `rtsp` feature (which no longer requires `onvif`) plus the GStreamer
runtime. See the [sample configuration](sample-configurations.md#5-bare-rtsp-camera-no-onvif).

## Hand completed files to file-replicator

The adapter and [file-replicator](https://docs.edgecommons.mbreissi.com/components/file-replicator/) couple
through the disk and the bus, not through code. Keep the adapter **state** root private, and grant the
file-replicator identity read/traverse access only to the **output** root. Consume the terminal
`ImageCaptured` message's `image.relativePath`, `image.sha256`, and `image.bytes`, and **verify the checksum**
before replicating. Never consume a partial file, and never treat the catalog or its database files as input
— a final image is only ever visible after it has been fully written, checked, and finalized.
