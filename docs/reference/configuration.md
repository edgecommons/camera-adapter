# Configuration reference

Adapter-owned configuration lives under `component.global` and `component.instances`. Both objects are
closed: unknown adapter keys are rejected before runtime state changes. The surrounding EdgeCommons
configuration, messaging, credentials, and platform fields follow the core schema.

## Global fields

| Field | Default | Meaning |
|---|---:|---|
| `output.rootDirectory` | required | Absolute output root. |
| `output.cameraDirectoryTemplate` | `{cameraId}/{yyyy}/{MM}/{dd}` | Relative per-camera directory template. |
| `output.fileNameTemplate` | `{timestamp}-{captureId}.{extension}` | Relative final image name template. |
| `output.writeMetadataSidecar` | `false` | Write and finalize JSON metadata before exposing the image. |
| `output.minimumFreeBytes` | 1 GiB | Free-byte floor after reservations. |
| `output.minimumFreePercent` | 5 | Free-space percentage floor after reservations. |
| `output.directoryMode` | `0750` | Mode for new output directories on Unix. |
| `output.fileMode` | `0640` | Mode for final images and sidecars on Unix. |
| `state.directory` | platform-dependent | Explicit durable catalog/outbox root; mandatory for Greengrass. |
| `state.resultRetentionHours` | 72 | Terminal ledger retention. |
| `state.maxResultRecords` | 100000 | Soft cap for terminal records. |
| `state.outboxRetentionHours` | 168 | Delivered outbox retention. |
| `state.queuedRecoveryPolicy` | `requeue` | `requeue` valid work after restart or `interrupt` it. |
| `limits.maxConnectedCameras` | 256 | Maximum enabled camera supervisors. |
| `limits.maxConcurrentCaptures` | 32 | Captures the component runs at once, fleet-wide. A capture holds one of these from the moment a camera takes it until it is terminal. |
| `limits.maxConcurrentEncodes` | `min(CPU,8)` | Global encoding concurrency. |
| `limits.maxConcurrentWrites` | 8 | Global persistence concurrency. |
| `limits.maxConcurrentConnects` | 16 | Global connection-attempt concurrency. |
| `limits.maxInFlightBytes` | 2 GiB | Global raw-frame reservation cap. Must be at least `maxConcurrentCaptures` x `maxFrameBytesPerCamera`, because a capture reserves the declared ceiling rather than its frame's real size. |
| `limits.maxFrameBytesPerCamera` | 64 MiB | Per-camera frame ceiling. |
| `limits.maxMetadataBytes` | 8192 | Encoded caller metadata cap. |
| `limits.maxQueuedCapturesPerCamera` | 4 | Capture queue cap per camera. |
| `limits.maxPendingCaptures` | 256 | Captures the fleet queue holds in total. Must be at least `maxQueuedCapturesPerCamera`. Submitting past it is rejected with `QUEUE_FULL`. |
| `limits.maxQueueWaitMs` | 300000 | How long a capture may wait for a camera when its profile sets no `queueExpiryMs`. A capture that waits longer expires without running. |
| `limits.maxQueuedControlsPerCamera` | 32 | Ordinary control queue cap per camera. |
| `limits.maxDeferredWaitersPerCapture` | 8 | Deferred direct-waiter cap. |
| `limits.maxCamerasPerGroup` | 32 | Group capture fan-out cap. |
| `limits.resourceGroups.{name}.maxConcurrentCaptures` | required when named | Shared NIC/USB acquisition cap for cameras selecting that resource group. |
| `timeouts.captureMs` | 30000 | Acquisition-stage cap. |
| `timeouts.encodeMs` | 30000 | Encoding-stage cap. |
| `timeouts.persistMs` | 30000 | Persistence-stage cap. |
| `timeouts.ptzMs` | 10000 | PTZ response cap. |
| `timeouts.jobTerminalMs` | 90000 | Deadline covering every stage, measured from the moment a camera takes the capture. The wait beforehand is bounded by `queueExpiryMs` or `limits.maxQueueWaitMs`. |
| `timeouts.connectMs` | 10000 | One backend connection-attempt cap. |
| `timeouts.reconnectBackoffMinMs` / `reconnectBackoffMaxMs` | 1000 / 60000 | Jittered reconnect range. |
| `timeouts.replyMarginMs` | 5000 | Reserved margin before a direct reply deadline. |
| `timeouts.maxDeferredReplyLifetimeMs` | 95000 | Upper bound for a Core deferred reply. |
| `timeouts.reloadDrainTimeoutMs` / `shutdownGraceMs` | 30000 / 30000 | Reload drain and ordered shutdown bounds. |
| `discovery.enabled` | `false` | Permit periodic and command discovery. |
| `discovery.reportUnconfigured` | `false` | Return compact unconfigured candidates when discovery is enabled. |
| `discovery.intervalSeconds` / `maxResults` | 60 / 1000 | Periodic discovery cadence and retained-result cap. |
| `discovery.eligibleInterfaces` | `[]` | Exact OS interfaces permitted for WS-Discovery; no wildcard fallback. |
| `operatorEvents.captureLifecycle` | `false` | Emit capture queued/started operator diagnostics. |
| `healthThresholds.staleSignalSecs` | 300 | Mark a camera stale after this interval without observation. |
| `security.maxHeaderBytes` | 65536 | ONVIF HTTP header/status limit. |
| `security.maxDecompressionRatio` | 100 | Decoded/compressed response ratio limit. |
| `security.allowBasicOverPlaintext` | `false` | Development-only exception for Basic auth over HTTP. |
| `captureGroupSchedules` | `[]` | Cron schedules that capture several cameras together as one group. |

## Camera instance fields

Every instance has `id`, `backend`, `defaultCaptureProfile`, and `captureProfiles`. `enabled` defaults to
true; disabled instances are retained for configuration validation but do not accept actuation. Optional
`resourceGroup` applies a shared transport acquisition bound. `schedules` is omitted for command-only use.

`backend.type` is one of `sim`, `onvif-rtsp`, or `genicam-aravis`. A GenICam selector provides exactly one
of `serial`, `mac`, `deviceId`, or `ip`. An ONVIF backend provides exactly one of `deviceServiceUrl` or
`selector.endpointReference`, a `mediaProfile`, optional credential/TLS references, and an allowlist for
snapshot or RTSP URI hosts.

Each named capture profile chooses `passthrough`, `jpeg`, `png`, `tiff`, or `raw`; `jpegQuality` defaults
to 90. It may override capture mode, offline handling, deadline, frame ceiling, GenICam width/height/
offset/exposure/gain, and motion interlock. The profile's `captureInterlock` is `reject`, `stop-and-settle`,
or `allow`. Unsupported Bayer/PFNC input is rejected as `UNSUPPORTED_PIXEL_FORMAT`; raw bytes are never
mislabeled as RGB.

An ONVIF backend defaults to `captureMode: "snapshot-uri"`, `rtspFallback: false`,
`rtspSessionPolicy: "on-demand"`, `mediaService: "auto"`, and `authenticationMode: "auto"`.
`maxSoapBytes`, `maxSnapshotBytes`, and `maxXmlDepth` default to 1 MiB, 64 MiB, and 64. `allowInsecure`
defaults to false. Use `allowedUriHosts` and `allowedUriCidrs` only for deliberate additional endpoint
authority; they do not disable per-connection address validation.

`ptz` defaults to `enabled: false`, `maximumContinuousMoveMs: 10000`, `captureInterlock: "reject"`,
`settleMs: 750`, and `allowPresetMutation: false`.

## Backend and schedule fields

`sim` accepts optional deterministic `simulatedId`, `seed`, a frame (`width`, `height`, `pixelFormat`,
`pattern`), `connectDelayMs`, `captureDelayMs` (default 10), PTZ capability switches, and deterministic
fault counters. It is intended for configured test and development cameras.

`genicam-aravis` accepts a single selector, `transport` (`auto`, `gige-vision`, or `usb3-vision`), optional
host `interface`, `packetSize`, `packetDelayNs`, `bufferCount`, and allowlisted `featureOverrides`. It is
compiled only with the native GenICam feature.

Each schedule requires `id`, six-field seconds-inclusive `cron`, IANA `timezone`, and `captureProfile`.
`enabled` defaults true; `misfirePolicy` defaults `skip`, `overlapPolicy` defaults `skip`, and
`jitterSeconds` defaults zero. `coalesce` admits only the latest missed occurrence and `queue` permits one
ordinary bounded queued overlap.

## Group schedules

A schedule under a camera captures that camera. A schedule under `global.captureGroupSchedules` captures
several cameras together, as one group:

```json
"captureGroupSchedules": [{
  "id": "line-a-sync",
  "cron": "0 */5 * * * *",
  "timezone": "America/New_York",
  "instances": ["cam-01", "cam-02", "cam-03"],
  "captureProfile": "inspect",
  "profileOverrides": { "cam-03": "inspect-wide" },
  "timeoutMs": 30000
}]
```

An occurrence is submitted exactly as an `sb/capture-group` command is: acceptance is all-or-nothing, the
members share one durable group, and one collated terminal notification reports them together.

`id`, `cron`, `timezone`, and `instances` are required, and `instances` names at least two cameras declared
in `component.instances`, at most `limits.maxCamerasPerGroup`, each once. `captureProfile` applies to every
member; `profileOverrides` replaces it per camera and its keys must be members. `enabled` defaults true,
`misfirePolicy` and `overlapPolicy` default to `skip`, and `jitterSeconds` defaults to zero. `timeoutMs`
bounds each member and ranges from 1000 to 1800000.

`overlapPolicy` is evaluated against the group: the schedule holds an occurrence back while any member of
its previous group is still running, so a group is never torn into halves that fire a cycle apart.

A group larger than the component's spare capacity is sequenced rather than failed. Members wait in the
fleet queue, and each one's stage deadlines start when a camera takes it -- so an oversized group takes
longer, and every member still runs. A member waits at most its profile's `queueExpiryMs`, or
`limits.maxQueueWaitMs`.

`ptz` defaults to disabled. Its policy bounds continuous movement and disables preset mutation by default.
Use the [sample configurations](../sample-configurations.md) for complete shapes.
