# How-to guides

## Schedule a camera

Add a named schedule to one camera instance. Schedule IDs are stable per camera and use the instance
timezone. A scheduled occurrence follows the same admission, deadline, persistence, and terminal-message
path as a command capture.

```json
{
  "id": "hourly-inspection",
  "cron": "0 0 * * * *",
  "timezone": "America/New_York",
  "captureProfile": "inspection"
}
```

Use `sb/capture-status` to inspect a scheduled job. Do not use a schedule as a substitute for an
idempotent command when the upstream caller needs a particular result.

## Move or stop PTZ safely

Enable PTZ per camera, then submit `sb/ptz` with a durable `requestId`. Continuous motion requires a
bounded `timeoutMs`; the adapter schedules a stop even when the requester disconnects. Use
`operation: "stop"` before maintenance. Preset mutation remains disabled unless the instance explicitly
permits it with `allowPresetMutation: true`.

```json
{
  "instance": "dock-ptz",
  "requestId": "operator-2026-07-11-001",
  "operation": "continuous",
  "velocity": { "pan": 0.2, "tilt": 0.0, "zoom": 0.0 },
  "timeoutMs": 2000
}
```

## Use ONVIF authentication and TLS

Reference a whole credential secret through `backend.credentials`. Keep `tls.verifyHostname` enabled.
For a camera with a private CA, point `tls.ca` to a PEM secret. Basic authentication over plaintext is
disabled unless the component-wide `security.allowBasicOverPlaintext` is explicitly set for a controlled
development fixture.

## Tune GenICam conservatively

GenICam is a Linux-native optional feature. Select a camera by exactly one stable selector, set the
specific host `interface` when needed, and allow only documented standard feature overrides. Start with
the device packet size and buffer count recommended by the camera vendor. Do not rely on ordinary Docker
NAT as proof of GigE multicast discovery or UDP acquisition.

## Configure RTSP fallback

Choose `captureMode: "snapshot-uri"` for normal ONVIF snapshots. Set `rtspFallback: true` to permit a
safe RTSP fallback from that path, or use `captureMode: "rtsp-frame"` to require RTSP extraction. In each
case, allow only explicit safe URI hosts and package the matching GStreamer runtime. The native RTSP feature
has simulator evidence; physical encoder compatibility is waived for this project and is not claimed.

## Hand completed files to file-replicator

Keep the adapter state root private. Grant the file-replicator identity read and traverse access only to
the output root. Consume terminal `image.relativePath`, `image.sha256`, and `image.bytes`, then verify the
checksum before remote replication. Never consume partial files or use catalog/database files as input.
