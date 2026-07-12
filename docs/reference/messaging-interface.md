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
| `sb/reconnect` | `instance`, `requestId`, optional reason | Requests reconnect. |
| `sb/ptz` | `operation` plus normalized vector/axes as appropriate | Bounded PTZ result or status. |
| `sb/ptz-presets` | `operation: list|goto|set|remove` | Paged presets or durable operation. |

Malformed bodies, unknown fields, invalid values, unavailable cameras, capacity pressure, and unsupported
capabilities return stable errors. A command reply is correlated with the incoming envelope. Normal capture
completion is an application message, not an operator event: its body includes schema version, durable
event and capture IDs, camera ID, original correlation ID, trigger, effective profile/mode, timestamps,
durations, optional output path/checksum/size, metadata, and a stable failure summary when unsuccessful.

The outbox preserves one encoded terminal envelope through retry. Consumers must tolerate at-least-once
delivery by deduplicating its stable event ID.

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
cannot reject, retry, or reclassify a capture. Terminal completion remains the
durable `app/image/{captured|failed|cancelled}` contract and is never emitted as an operator lifecycle
event.

## Terminal application messages

Terminal messages use the selected camera instance application namespace. Their exact header and channel
are `ImageCaptured` / `image/captured`, `ImageCaptureFailed` / `image/failed`, or
`ImageCaptureCancelled` / `image/cancelled`. The envelope has version `1.0`; its correlation ID equals
body `correlationId`.

The version-1 body always contains `schemaVersion`, `eventId`, `captureId`, `cameraId`, `correlationId`,
`trigger`, `captureProfile`, `captureMode`, `timestamps`, `durationsMs`, `camera`, and caller `metadata`.
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

## Operator events

`schedule-skipped` is a warning event emitted for a scheduled capture skipped because the camera is
moving. It contains the camera/schedule occurrence context, not image bytes or credentials. Component
alarms `storage-low` and `message-delivery-delayed` are documented in the
[metrics reference](metrics.md). Capture lifecycle diagnostics are documented above.
