# Messaging interface

All commands use the shipped main inbox:

```text
ecv1/{device}/camera-adapter/main/cmd/sb/{verb}
```

Select a camera with the closed JSON body field `instance`; omit it only when the documented single-camera
selection rule applies. Do not construct a per-instance command topic. Every mutating request supplies a
caller-owned bounded `requestId` for durable idempotency.

| Verb | Body highlights | Result |
|---|---|---|
| `sb/list` | `includeCapabilities`, `includeUnconfigured`, `limit`, `cursor` | Paged configured-camera view. |
| `sb/discover` | `backends`, `timeoutMs`, `limit`, `cursor` | Bounded discovery snapshot. |
| `sb/status` | optional `instance` | Component or camera status. |
| `sb/capture` | `instance`, `requestId`, `captureProfile`, `timeoutMs`, `metadata` | Deferred terminal reply. |
| `sb/capture-submit` | same as capture | Immediate durable acceptance with `captureId`. |
| `sb/capture-group` | `requestId`, `instances`, profile fields, timeout, metadata | Deferred aggregated group result. |
| `sb/capture-group-submit` | same group body | Immediate group acceptance. |
| `sb/capture-status` | one lookup mode: capture ID, group ID, `(instance,requestId)`, group request ID, or paged state list | Job/group status. |
| `sb/capture-cancel` | `requestId`, exactly one capture or group ID, optional reason | Durable cancellation outcome. |
| `sb/queue-status` | optional `instance` | Live admission capacity, per-camera queue depth, and durable backlog. |
| `sb/queue-clear` | `requestId`, `instance` or `allCameras: true`, optional `includeInFlight`, optional reason | Cancels the durable backlog and reports what it cancelled. |
| `sb/reconnect` | `instance`, `requestId`, optional reason | Requests reconnect. |
| `sb/ptz` | `operation` plus normalized vector/axes as appropriate | Bounded PTZ result or status. |
| `sb/ptz-presets` | `operation: list|goto|set|remove` | Paged presets or durable operation. |

Malformed bodies, unknown fields, invalid values, unavailable cameras, capacity pressure, and unsupported
capabilities return stable errors. A command reply is correlated with the incoming envelope. Normal capture
completion is an application message, not an operator event: its body includes schema version, durable
event and capture IDs, camera ID, original correlation ID, trigger, effective profile/mode, timestamps,
durations, optional output path/checksum/size, metadata, and a stable failure summary when unsuccessful.

A terminal message is an announcement, not a delivery. It is published once, best effort, after the
terminal state is durably committed, and it is never retried. It is lost when the broker or IPC transport
is unavailable, and when the component stops between the commit and the publish. The capture itself is
unaffected: the image and its sidecar are on disk, the catalog holds the terminal state, and
`sb/capture-status` answers for it — that pair, not the message, is authoritative. A consumer that must
not miss an outcome polls `sb/capture-status` rather than relying on the announcement.

The announcement carries the terminal body the catalog committed, `eventId` included. `sb/capture-status`
reports that same body, so a result read from status and a result received as a message agree field for
field.

## Queue depth and the break-glass drain

`sb/queue-status` answers whether the component is coping and, if it is not, where the work is stuck. It
is read-only and takes an optional `instance` to narrow the answer to one camera. The reply reports live
admission capacity (`admission`: unused acquisition/encoder/writer permits, unreserved frame memory,
outstanding disk bytes) alongside the configured ceilings (`limits`) those numbers should be read against;
per-camera dispatcher depth (`cameras[]` with `queued` and `capacity`, plus the `dispatchQueued` total),
which is what a camera answers `QUEUE_FULL` against; and the durable backlog (`durable`, counted by state
token, split into `durableBacklog` for captures promised but not started and `durableInFlight` for captures
already doing physical work). The durable counts are the only ones that survive a restart.

`sb/queue-clear` cancels the durable backlog. It targets one camera by `instance`, or the whole fleet when
`allCameras` is `true` — omitting `instance` without `allCameras` is rejected, so a fleet-wide drain cannot
result from a missing field. By default it cancels only captures that have not started; `includeInFlight:
true` also cancels captures already acquiring, encoding, or persisting. Each capture is cancelled through
the same path as `sb/capture-cancel`, so it reaches the same terminal state, publishes the same terminal
application message, and releases the same admission capacity. The reply reports `cancelled`,
`alreadyTerminal` (captures that finished on their own before the drain reached them), and `failed[]` —
a drain reports what it could not cancel rather than claiming a clean sweep. Like every mutating verb it is
ledgered on `requestId`, so a retry returns the original outcome instead of cancelling a second wave of
work.

## Capture lifecycle diagnostics

`component.global.operatorEvents.captureLifecycle` is disabled by default. When enabled, the adapter
publishes best-effort, non-durable per-camera `evt` diagnostics through the EdgeCommons `events()`
facade:

| Type | Severity | Stable `context` |
|---|---|---|
| `capture-queued` | `debug` | `captureId`, `trigger` (`command`, `group-command`, or `schedule`), `captureProfile`, bounded one-based `queuePosition` snapshot |
| `capture-started` | `info` | `captureId`, `trigger`, `captureProfile`, `captureMode` |

The facade supplies the standard `evt` envelope body and routes to
`ecv1/{device}/camera-adapter/{cameraId}/evt/{severity}/{type}`. Context deliberately excludes
request metadata, correlation IDs, output paths, image data, and backend/device details. Publication is
detached from capture admission and acquisition, bounded to five seconds, and capped at 64 concurrent
diagnostic sends; saturation drops the diagnostic rather than delaying a capture. Failure is logged but
cannot reject, retry, or reclassify a capture. Terminal completion is announced on the
`app/image/{captured|failed|cancelled}` contract and is never emitted as an operator lifecycle
event.

## Terminal application messages

Terminal messages use the selected camera instance application namespace. Their exact header and channel
are `ImageCaptured` / `image/captured`, `ImageCaptureFailed` / `image/failed`, or
`ImageCaptureCancelled` / `image/cancelled`. The envelope has version `1.0`; its correlation ID equals
body `correlationId`.

The version-1 body always contains `schemaVersion`, `eventId`, `captureId`, `cameraId`, `correlationId`,
`trigger`, `captureProfile`, `captureMode`, `timestamps`, `durationsMs`, `camera`, and caller `metadata`.

A group fired by `global.captureGroupSchedules` reports exactly as a commanded group does: its members carry
`trigger` `group-command` and the group produces one collated terminal result. Its `metadata` carries
`scheduleId` and `intendedFireTime`, which identify the schedule and the occurrence that produced it.
Successful messages additionally contain `image` (`absolutePath`, `relativePath`, `fileUri`, `contentType`,
`encoding`, `bytes`, `sha256`, and optional `metadataSidecarRelativePath`) and normally `frame` facts.
Failed messages contain `failure` (`code`, `stage`, `retriable`, `message`) and no `image`; cancelled
messages contain neither image nor failure. Group-member messages pair `captureGroupId` with `groupSize`.
`backendMetadata` is bounded and never contains credentials or unsafe endpoints.

## Stable errors

The public error `code` is one of `INSTANCE_REQUIRED`, `UNKNOWN_INSTANCE`, `CAMERA_DISABLED`,
`CAMERA_UNAVAILABLE`, `CAMERA_MOVING`, `UNSUPPORTED_CAPABILITY`, `INVALID_REQUEST`,
`UNKNOWN_CAPTURE_PROFILE`, `QUEUE_FULL`, `GROUP_TOO_LARGE`, `RESOURCE_LIMIT`, `CAPTURE_TIMEOUT`,
`CAPTURE_CANCELLED`, `PROCESS_INTERRUPTED`, `CAPTURE_NOT_FOUND`, `IDEMPOTENCY_CONFLICT`,
`PREVIOUS_OUTCOME_UNKNOWN`, `REPLY_REQUIRED`, `UNSUPPORTED_PIXEL_FORMAT`, `STORAGE_PRESSURE`,
`PERSISTENCE_FAILED`, `PTZ_DISABLED`, `PTZ_RANGE_ERROR`, `PTZ_TIMEOUT`, `COMPONENT_STOPPING`, or
`BACKEND_ERROR`. Clients should branch on `code`, not the sanitized human-readable message.

`STORAGE_PRESSURE` on a capture submission means the output or state root is below its configured
free-space floor, or cannot be read. The component refuses new captures until that root recovers; a
broker it cannot reach never has this effect, because messaging is not what a capture consumes.

## Operator events

`schedule-skipped` is a warning event emitted for a scheduled capture skipped because the camera is
moving. It contains the camera/schedule occurrence context, not image bytes or credentials. Component
alarms `storage-low` and `message-publish-degraded` are documented in the
[metrics reference](metrics.md). Capture lifecycle diagnostics are documented above.

## Capture thumbnail

When a capture profile asks for one, the published capture result carries a `thumbnail` beside `artifact`:

```jsonc
"thumbnail": {
  "encoding": "jpeg",
  "width":  320,
  "height": 240,
  "bytes":  14231,
  "data":   "<raw JPEG bytes>"
}
```

`data` is a binary value: on the wire it is raw bytes, not a base64 string.

The thumbnail is a lossy re-encode of the same camera frame the artifact is derived from. It carries **no
digest**, deliberately — it cannot be verified against the artifact, and a `sha256` beside the artifact's
own would invite the belief that it can. The artifact's `sha256` describes the installed image and nothing
else.

A thumbnail is present only in the published result. It is never written to the metadata sidecar, never
stored in the catalog, and never included in a `sb/capture` reply or a group reply. A result republished
after a restart therefore carries no thumbnail: the frame it would be made from is gone.

A thumbnail is never allowed to cost the result it decorates. If a result cannot be published with its
thumbnail, it is published again without it, and the thumbnail is dropped.

`thumbnail` may also be smaller than the profile asked for: the messaging transport decides what it can
carry, and a size it cannot carry is reduced to one it can. See the capture-profile reference.

`thumbnail` may be absent even when a profile asks for one — a frame the component cannot render, or a
thumbnail that will not fit the message's byte ceiling, is left out. The capture still succeeds and its
image is still installed.
