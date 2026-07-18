# Messaging interface

Every command is a request/reply exchange on the component command inbox:

```text
ecv1/{device}/camera-adapter/cmd/sb/{verb}
```

Select a camera with the JSON body field `instance`; do not construct a per-instance command topic. The
reply is correlated with the incoming envelope. Normal capture *completion* is a separate terminal
application message, not the command reply — see [Terminal application messages](#terminal-application-messages).

## Conventions

These rules apply to every verb below.

- **Closed request schema.** Bodies are parsed with `deny_unknown_fields`: any field not listed for a
  verb is rejected with `INVALID_REQUEST`. All field names are **camelCase** on the wire.
- **Selecting a camera.** Actuation verbs take an optional `instance`. Omit it only when exactly one
  camera is configured — the sole camera is then used. With more than one camera, omission is
  `INSTANCE_REQUIRED`; an unknown name is `UNKNOWN_INSTANCE`; a disabled camera is `CAMERA_DISABLED`. An
  `instance` token is non-empty, ≤128 bytes, ASCII letters/digits/`.`/`_`/`-`.
- **Idempotency.** Every *mutating* verb requires a caller-owned `requestId` (1–256 bytes, no control
  characters). A retry with the same `requestId` and the same arguments returns the original outcome; a
  reused `requestId` with **different** arguments is `IDEMPOTENCY_CONFLICT`; an operation whose outcome
  was lost to a restart mid-flight is `PREVIOUS_OUTCOME_UNKNOWN`. Read-only verbs take no `requestId`.
- **Deferred vs. immediate.** `sb/capture` and `sb/capture-group` are **deferred**: the reply is held
  open until the work is terminal, then settles with the full terminal body. Their `-submit` siblings
  return **immediately** with durable identifiers, and you observe completion through
  `sb/capture-status` or the terminal message. All other verbs reply immediately.
- **Pagination.** Paged verbs take `limit` (1–1000, default 100) and an opaque `cursor` (1–4096 bytes);
  the reply carries `nextCursor` (a string, or `null` on the last page). A continuation is served from the
  snapshot the first page captured, not a fresh read.
- **Errors.** A failed command replies `{ "errorCode": "...", "errorMessage": "..." }`. Branch on
  `errorCode` (the stable [error codes](#stable-errors)), never the human-readable message. While a
  configuration reload is draining, every verb replies `CAMERA_UNAVAILABLE`.
- **`JobState` vocabulary.** A capture's durable state is one of `ACCEPTED`, `QUEUED`, `ACQUIRING`,
  `ENCODING`, `PERSISTING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `INTERRUPTED`.

---

# Capture verbs

## `sb/capture`

**What it does.** Accepts one single-camera capture and holds the reply open (deferred) until the capture
reaches a terminal state, then settles with the full terminal body.

**Input payload**

| Field | Type | Required | Meaning |
|---|---|---|---|
| `instance` | string | optional* | Target camera (*single-camera omission rule). |
| `requestId` | string | **yes** | Durable idempotency key, 1–256 bytes. |
| `captureProfile` | string | optional | Named profile (≤128 bytes); defaults to the camera's `defaultCaptureProfile`. Unknown → `UNKNOWN_CAPTURE_PROFILE`. |
| `timeoutMs` | u64 | optional | 1000–1800000. Defaults to the profile's `timeoutMs`, else `global.timeouts.jobTerminalMs`. |
| `metadata` | object | optional | Opaque caller metadata, copied verbatim into the result; encoded size ≤ `limits.maxMetadataBytes`. |

**Response payload.** No adapter-defined acceptance body — acceptance is the framework's deferred-reply
mechanism. The settled reply is the capture's [terminal body](#terminal-application-messages) (schema
version 1: `schemaVersion`, `eventId`, `captureId`, `cameraId`, `correlationId`, `trigger`,
`captureProfile`, `captureMode`, `timestamps`, `durationsMs`, `camera`, `metadata`, plus `image` on
success or `failure` on failure).

**Example — request**
```json
{ "instance": "camera-a", "requestId": "order-2026-07-18-001",
  "captureProfile": "detail", "timeoutMs": 30000,
  "metadata": { "operator": "breissim", "ticket": "INC-4412" } }
```
**Example — settled reply (success)**
```json
{ "schemaVersion": 1, "eventId": "evt_018f9c…", "captureId": "cap_018f9c2b…",
  "cameraId": "camera-a", "correlationId": "c-771a…",
  "trigger": { "type": "command", "requestId": "order-2026-07-18-001" },
  "captureProfile": "detail", "captureMode": "snapshot-uri",
  "timestamps": { "requestedAt": "2026-07-18T10:15:04Z", "persistedAt": "2026-07-18T10:15:06Z",
                  "cameraFrameTimestampQuality": "adapter-receive" },
  "durationsMs": { "acquisition": 900, "encoding": 300, "persistence": 80, "total": 1400 },
  "image": { "absolutePath": "/var/lib/edgecommons/camera-adapter-output/camera-a/2026/07/18/…-cap_018f9c2b.jpg",
             "relativePath": "camera-a/2026/07/18/…-cap_018f9c2b.jpg",
             "fileUri": "file:///var/lib/edgecommons/camera-adapter-output/camera-a/2026/07/18/…-cap_018f9c2b.jpg",
             "contentType": "image/jpeg", "encoding": "jpeg", "bytes": 184223, "sha256": "9f2c…e1" },
  "camera": { "backend": "onvif-rtsp", "vendor": "Acme", "model": "X9" },
  "metadata": { "operator": "breissim", "ticket": "INC-4412" } }
```

## `sb/capture-submit`

**What it does.** Durably accepts one single-camera capture and returns its identifiers immediately, without
waiting for completion. Poll `sb/capture-status`, or consume the terminal message, for the outcome.

**Input payload.** Identical to [`sb/capture`](#sbcapture).

**Response payload**

| Field | Type | Meaning |
|---|---|---|
| `captureId` | string | Durable capture key (`cap_…`). |
| `state` | `JobState` | Current durable state — typically `ACCEPTED` or `QUEUED`. |
| `acceptedAt` | RFC3339 | When the capture was durably accepted. |
| `statusVerb` | string | Literal `"sb/capture-status"` — where to poll. |

**Example — request / response**
```json
{ "instance": "camera-a", "requestId": "batch-77", "captureProfile": "detail" }
```
```json
{ "captureId": "cap_018f9c2b0a1e7c3d", "state": "QUEUED",
  "acceptedAt": "2026-07-18T10:15:04.512Z", "statusVerb": "sb/capture-status" }
```

## `sb/capture-group`

**What it does.** Accepts a multi-camera group capture and holds the reply open until every member is
terminal, then settles with one aggregate body. Acceptance is all-or-nothing: if any member cannot be
resolved, no durable work is written.

**Input payload**

| Field | Type | Required | Meaning |
|---|---|---|---|
| `requestId` | string | **yes** | Component-scoped durable key, 1–256 bytes. |
| `instances` | string[] | **yes** | Result-order camera list. 2 ≤ length ≤ `limits.maxCamerasPerGroup`, no duplicates. Fewer than 2 → `INVALID_REQUEST`; more than the cap → `GROUP_TOO_LARGE`. |
| `captureProfile` | string | optional | Common profile applied to every member; defaults per camera. |
| `profileOverrides` | object | optional | Per-camera profile override (`{ cameraId: profile }`); keys must be a subset of `instances`. |
| `timeoutMs` | u64 | optional | 1000–1800000, per member. |
| `metadata` | object | optional | Opaque; encoded size ≤ `limits.maxMetadataBytes`. |

**Response payload.** No adapter-defined acceptance body (deferred). The settled reply aggregates the group:

| Field | Type | Meaning |
|---|---|---|
| `captureGroupId` | string | Durable group key (`grp_…`). |
| `requestId` | string | The group's `requestId`. |
| `state` | string | Aggregate outcome: `COMPLETED` (all succeeded), `PARTIAL` (some), or `FAILED` (none). Note: **not** a `JobState`. |
| `members` | array | Each member's [terminal body](#terminal-application-messages); group members additionally carry `captureGroupId` and `groupSize`. |

**Example — request**
```json
{ "requestId": "sync-shot-9", "instances": ["camera-a", "camera-b", "camera-c"],
  "captureProfile": "detail", "profileOverrides": { "camera-c": "wide" }, "timeoutMs": 20000 }
```
**Example — settled reply**
```json
{ "captureGroupId": "grp_018f9c2b7f10", "requestId": "sync-shot-9", "state": "PARTIAL",
  "members": [
    { "schemaVersion": 1, "captureId": "cap_…a", "cameraId": "camera-a", "captureGroupId": "grp_018f9c2b7f10",
      "groupSize": 3, "captureProfile": "detail", "image": { "relativePath": "camera-a/…jpg", "sha256": "…" }, "…": "full terminal body" },
    { "schemaVersion": 1, "captureId": "cap_…b", "cameraId": "camera-b", "captureGroupId": "grp_018f9c2b7f10",
      "groupSize": 3, "failure": { "code": "CAPTURE_TIMEOUT", "stage": "ACQUIRING", "retriable": true, "message": "no frame within deadline" }, "…": "full terminal body" }
  ] }
```

## `sb/capture-group-submit`

**What it does.** Durably accepts a group and returns the group id, its shared state, and per-member
identifiers immediately.

**Input payload.** Identical to [`sb/capture-group`](#sbcapture-group). (Submit and deferred group share
one idempotency namespace on `requestId`.)

**Response payload**

| Field | Type | Meaning |
|---|---|---|
| `captureGroupId` | string | Durable group key (`grp_…`). |
| `state` | `JobState` | Shared durable acceptance state, e.g. `QUEUED`. |
| `members` | array | One entry per member: `{ instance, captureId, state }`. |

**Example — request / response**
```json
{ "requestId": "sync-batch-4", "instances": ["camera-a", "camera-b"] }
```
```json
{ "captureGroupId": "grp_018f9c2b7f10", "state": "QUEUED",
  "members": [
    { "instance": "camera-a", "captureId": "cap_018f9c2b0001", "state": "QUEUED" },
    { "instance": "camera-b", "captureId": "cap_018f9c2b0002", "state": "QUEUED" } ] }
```

## `sb/capture-status`

**What it does.** Reads durable status for a capture, a group, or a filtered page of captures. It is the
authority on what happened — a consumer that must not miss an outcome polls this rather than relying on the
terminal announcement. It selects **exactly one** of five lookup modes from the field combination; an
ambiguous or empty combination is `INVALID_REQUEST`.

**Input payload / lookup modes**

| Mode | Fields | Returns |
|---|---|---|
| Capture | `captureId` only | One capture's status. Missing → `CAPTURE_NOT_FOUND`. |
| Group | `captureGroupId` (+ `limit`/`cursor`) | A paged group body. |
| CameraRequest | `instance` + `requestId` | The capture that `(instance, requestId)` submitted. |
| GroupRequest | `requestId` only | The group that `requestId` submitted (paged). |
| List | `states` (required, non-empty), optional `instance`, `limit`/`cursor` | A paged list of captures in those states. |

Field bounds: `captureId`/`captureGroupId`/`requestId` are opaque, 1–256 bytes; `states` is a distinct
subset of the `JobState` vocabulary.

**Response payload.** A **single-job** body (`Capture`/`CameraRequest`):

| Field | Type | Meaning |
|---|---|---|
| `captureId` | string | The capture. |
| `instance` | string | Its camera. |
| `state` | `JobState` | Current durable state. |
| `acceptedAtMs` | i64 | Acceptance time (epoch ms). |
| `terminalAtMs` | i64 \| null | Terminal time, or null if not terminal. |
| `captureGroupId` | string \| null | Group, if part of one. |
| `errorCode` / `errorMessage` | string \| null | Set on failure. |
| `result` | object \| null | The full [terminal body](#terminal-application-messages) once terminal. |

A **group page** (`Group`/`GroupRequest`) returns `{ group: {…}, members: [ single-job, … ], nextCursor }`,
where `group` carries `captureGroupId`, `requestId`, `state`, `acceptedAtMs`, `terminalAtMs`, `errorCode`,
`errorMessage`, `result`. A **list page** returns `{ jobs: [ single-job, … ], nextCursor }`.

**Example — request / response (List mode)**
```json
{ "instance": "camera-a", "states": ["FAILED", "CANCELLED"], "limit": 50 }
```
```json
{ "jobs": [
    { "captureId": "cap_018f9c2b0001", "instance": "camera-a", "state": "FAILED",
      "acceptedAtMs": 1789012504512, "terminalAtMs": 1789012506010, "captureGroupId": null,
      "errorCode": "CAPTURE_TIMEOUT", "errorMessage": "no frame within deadline",
      "result": { "schemaVersion": 1, "…": "terminal body" } } ],
  "nextCursor": null }
```

## `sb/capture-cancel`

**What it does.** Cancels a single capture or a whole group, idempotently, and reports what actually changed.
Cancellation runs the same terminal path as any other outcome (the capture reaches `CANCELLED`, publishes a
terminal message, and releases its admission capacity).

**Input payload**

| Field | Type | Required | Meaning |
|---|---|---|---|
| `requestId` | string | **yes** | Durable cancellation key, 1–256 bytes. |
| `captureId` | string | one-of* | Cancel this capture. |
| `captureGroupId` | string | one-of* | Cancel this group. |
| `reason` | string | optional | Operator-safe text, ≤1024 bytes; defaults to `"operator cancellation"`. |

*Exactly one of `captureId` / `captureGroupId` is required. Missing target → `CAPTURE_NOT_FOUND`.

**Response payload.** For a **single capture**: `{ captureId, cancelled (bool), state (JobState),
cancellationInProgress (bool) }`. For a **group**: `{ captureGroupId, cancelledMembers (u64),
unchangedMembers (u64), members: [ { captureId, instance, cancelled, state, cancellationInProgress }, … ] }`.

**Example — request / response (single)**
```json
{ "requestId": "cancel-88", "captureId": "cap_018f9c2b0001", "reason": "operator request" }
```
```json
{ "captureId": "cap_018f9c2b0001", "cancelled": true, "state": "CANCELLED", "cancellationInProgress": false }
```

---

# Camera roster and discovery

## `sb/list`

**What it does.** Returns a paginated roster of configured cameras — a compact element by default, or the
full immutable snapshot with `includeCapabilities` — optionally appended with bounded discovery of
unconfigured cameras.

**Input payload**

| Field | Type | Required | Meaning |
|---|---|---|---|
| `includeCapabilities` | bool | optional (false) | Return the full `CameraSnapshot` per camera instead of the compact element. |
| `includeUnconfigured` | bool | optional (false) | Append discovered unconfigured cameras (only when `discovery.reportUnconfigured` is also enabled). |
| `limit` | u16 | optional (100) | Page size, 1–1000. |
| `cursor` | string | optional | Continuation. |

**Response payload.** `{ cameras: [...], unconfigured: [...], nextCursor }`. A **compact** camera element is
`{ instance, enabled, state, backend }`; the **full** element (under `includeCapabilities`) adds
`generation`, `capabilities` (see [metrics — per-camera presence](metrics.md#per-camera-presence) and the
capability model), `capabilitiesDigest`, `connectedAt`, `lastError`, `updatedAt`. `unconfigured` entries are
discovery candidates `{ backend, selector, vendor, model, capabilities }`.

**Example — request / response**
```json
{ "includeCapabilities": false, "limit": 10 }
```
```json
{ "cameras": [
    { "instance": "camera-a", "enabled": true, "state": "ONLINE", "backend": "onvif-rtsp" },
    { "instance": "camera-b", "enabled": false, "state": "DISABLED", "backend": "sim" } ],
  "unconfigured": [], "nextCursor": null }
```

## `sb/status`

**What it does.** Returns one camera's full snapshot when `instance` is given, otherwise every camera's
snapshot.

**Input payload.** `{ instance? }` — optional camera token.

**Response payload.** With `instance`: a single `CameraSnapshot` (`instance`, `enabled`, `backend`,
`generation`, `state`, `capabilities`, `capabilitiesDigest`, `connectedAt`, `lastError`, `updatedAt`).
Without: `{ cameras: [ CameraSnapshot, … ] }`.

**Example — request / response**
```json
{ "instance": "camera-a" }
```
```json
{ "instance": "camera-a", "enabled": true, "backend": "onvif-rtsp", "generation": 7, "state": "ONLINE",
  "capabilities": { "captureModes": ["snapshot-uri"], "ptz": true, "presets": true, "presetMutation": false,
                    "vendor": "Acme", "model": "X9", "warnings": [] },
  "capabilitiesDigest": "3f2a…", "connectedAt": "2026-07-18T11:59:00Z",
  "lastError": null, "updatedAt": "2026-07-18T12:00:00Z" }
```

## `sb/discover`

**What it does.** Runs one bounded, credential-free discovery pass across the compiled backends (or serves a
continuation of a retained pass). Discovery must be enabled in config, or the verb replies
`UNSUPPORTED_CAPABILITY`.

**Input payload**

| Field | Type | Required | Meaning |
|---|---|---|---|
| `backends` | string[] | optional ([]) | Restrict to these backends (distinct; may not include `sim`). Empty = all discovery-capable backends. |
| `timeoutMs` | u64 | optional (5000) | Probe budget, 100–300000. |
| `limit` | u16 | optional (100) | Page size, 1–1000. |
| `cursor` | string | optional | Continuation (serves the retained snapshot; no new probe). |

**Response payload.** `{ candidates: [ { backend, selector, vendor, model, capabilities }, … ], nextCursor,
completedAt }`.

**Example — request / response**
```json
{ "backends": ["onvif-rtsp"], "timeoutMs": 3000, "limit": 50 }
```
```json
{ "candidates": [
    { "backend": "onvif-rtsp", "selector": { "endpointReference": "urn:…" },
      "vendor": "Acme", "model": "X9", "capabilities": { "ptz": true } } ],
  "nextCursor": null, "completedAt": "2026-07-18T12:00:03.481Z" }
```

---

# Queue and connection

## `sb/queue-status`

**What it does.** A read-only view of whether the component is coping and, if not, where work is stuck: live
admission capacity, the configured ceilings to read it against, per-camera dispatcher depth, and the durable
non-terminal counts (the only ones that survive a restart). Omit `instance` for the whole fleet, or supply
it to narrow to one camera.

**Input payload.** `{ instance? }`.

**Response payload**

| Field | Type | Meaning |
|---|---|---|
| `admission` | object | Unused permits and reservations: `availableAcquisitions`, `availableResourceGroupAcquisitions` (map), `availableMemoryBytes`, `outstandingDiskBytes`, `availableEncoders`, `availableWriters`. |
| `limits` | object | The configured ceilings: `maxConcurrentCaptures`, `maxInFlightBytes`, `maxQueuedCapturesPerCamera`, `maxPendingCaptures`. |
| `cameras` | array | Per camera: `{ instance, queued, capacity }` (what a camera answers `QUEUE_FULL` against). |
| `dispatchQueued` | usize | Total descriptors across all camera dispatchers. |
| `durable` | object | Map of non-terminal `JobState` → count. |
| `durableBacklog` | u64 | `ACCEPTED + QUEUED` (accepted but not started). |
| `durableInFlight` | u64 | `ACQUIRING + ENCODING + PERSISTING` (doing physical work). |

**Example — request / response**
```json
{}
```
```json
{ "admission": { "availableAcquisitions": 30, "availableResourceGroupAcquisitions": { "ptz-cams": 2 },
                 "availableMemoryBytes": 268435456, "outstandingDiskBytes": 0,
                 "availableEncoders": 4, "availableWriters": 4 },
  "limits": { "maxConcurrentCaptures": 32, "maxInFlightBytes": 536870912,
              "maxQueuedCapturesPerCamera": 4, "maxPendingCaptures": 256 },
  "cameras": [ { "instance": "camera-a", "queued": 2, "capacity": 4 } ],
  "dispatchQueued": 2, "durable": { "QUEUED": 2, "ACQUIRING": 1 },
  "durableBacklog": 2, "durableInFlight": 1 }
```

## `sb/queue-clear`

**What it does.** Break-glass drain: cancels the durable backlog for one camera or the whole fleet, through
the same cancel path as `sb/capture-cancel`, and reports what it could not cancel. Each cancelled capture
reaches `CANCELLED` and publishes its terminal message.

**Input payload**

| Field | Type | Required | Meaning |
|---|---|---|---|
| `requestId` | string | **yes** | Durable drain key. |
| `instance` | string | one-of* | Drain this camera. |
| `allCameras` | bool | one-of* | Drain the whole fleet. |
| `includeInFlight` | bool | optional (false) | Also cancel started work (`ACQUIRING`/`ENCODING`/`PERSISTING`), not just the backlog. |
| `reason` | string | optional | ≤1024 bytes; defaults to `"operator queue drain"`. |

*Provide `instance`, **or** `allCameras: true` — not both, not neither. Omitting `instance` without
`allCameras` is rejected, so a fleet-wide drain cannot result from a missing field.

**Response payload.** `{ cancelled (usize), alreadyTerminal (usize), failed: [ { captureId, error }, … ] }`.

**Example — request / response**
```json
{ "requestId": "drain-2026-07-18-01", "instance": "camera-a", "reason": "runaway backlog" }
```
```json
{ "cancelled": 12, "alreadyTerminal": 1, "failed": [ { "captureId": "cap_018f…", "error": "camera actor is offline" } ] }
```

## `sb/reconnect`

**What it does.** Idempotently requests re-establishment of a camera's live session (cancels the current
session, not the camera); ledgered and settled immediately.

**Input payload.** `{ instance? , requestId (required), reason? }`.

**Response payload.** `{ operationId, instance, state: "ACCEPTED" }`.

**Example — request / response**
```json
{ "instance": "camera-a", "requestId": "reconnect-77", "reason": "operator forced refresh" }
```
```json
{ "operationId": "op_018f2c3d-…", "instance": "camera-a", "state": "ACCEPTED" }
```

---

# PTZ

## `sb/ptz`

**What it does.** Executes one PTZ operation, selected by the `operation` field. PTZ must be enabled on the
camera (`ptz.enabled`), or the verb replies `PTZ_DISABLED`. Motion values are **normalized**: pan/tilt in
`[-1, 1]`, zoom position in `[0, 1]` and zoom delta in `[-1, 1]`, velocities in `[-1, 1]`.

**Input payload** — `operation` (kebab-case) plus per-operation fields. The four mutating operations take
`instance?` and `requestId` (required); `status` is read-only and takes neither `requestId`.

| `operation` | Fields | Notes |
|---|---|---|
| `continuous` | `velocity` (`{pan,tilt,zoom}`, required), `timeoutMs` (required) | `timeoutMs` > 0, ≤ 60000, and ≤ the camera's `ptz.maximumContinuousMoveMs` (else `PTZ_RANGE_ERROR`). The adapter arms its own stop for that instant. |
| `absolute` | `position` (required), `speed` (`{pan,tilt,zoom}` in `[0,1]`, optional) | Move to an absolute position. |
| `relative` | `translation` (required), `speed` (optional) | Move by a delta. |
| `stop` | `axes` (array of `pan`/`tilt`/`zoom`, 1–3, distinct, required) | Stop the named axes. |
| `home` | — | Go to the configured home. |
| `status` | `instance?` | Read current PTZ status (no `requestId`). |

**Response payload.** A mutating operation replies `{ operation, state: "COMMANDED", acceptedAt,
stopDeadline }`, where `stopDeadline` is set **only** for a continuous move that armed a stop (null
otherwise). `status` replies `{ position ({pan,tilt,zoom} | null), moving (bool | null), available (bool),
observedAt }`.

**Example — continuous move**
```json
{ "operation": "continuous", "instance": "camera-a", "requestId": "ptz-101",
  "velocity": { "pan": 0.5, "tilt": -0.25, "zoom": 0.0 }, "timeoutMs": 1500 }
```
```json
{ "operation": "continuous", "state": "COMMANDED",
  "acceptedAt": "2026-07-18T12:00:00.500Z", "stopDeadline": "2026-07-18T12:00:02.000Z" }
```

## `sb/ptz-presets`

**What it does.** The preset surface: `list` presets (paged), or `goto` / `set` / `remove` a preset (durable,
ledgered). `set` and `remove` require `ptz.allowPresetMutation`; `goto` does not.

**Input payload** — `operation` (lowercase):

| `operation` | Fields | Notes |
|---|---|---|
| `list` | `instance?`, `limit?`, `cursor?` | Paged; no `requestId`. |
| `goto` | `instance?`, `requestId` (required), `token` (required) | Recall a preset by opaque token (1–1024 bytes). |
| `set` | `instance?`, `requestId` (required), `name` (required) | Create/update a preset named `name` (1–256 bytes). Requires mutation permission. |
| `remove` | `instance?`, `requestId` (required), `token` (required) | Delete a preset. Requires mutation permission. |

**Response payload.** `list` → `{ presets: [ { token, name (string|null) }, … ], nextCursor }`. `goto` →
`{ operation: "goto", state: "COMMANDED" }`. `set` → `{ operation: "set", token }` (the camera-issued token).
`remove` → `{ operation: "remove", removed: true }`.

**Example — list, then set**
```json
{ "operation": "list", "instance": "camera-a", "limit": 50 }
```
```json
{ "presets": [ { "token": "PRESET_1", "name": "north gate" }, { "token": "PRESET_2", "name": null } ],
  "nextCursor": null }
```
```json
{ "operation": "set", "instance": "camera-a", "requestId": "preset-set-9", "name": "north gate" }
```
```json
{ "operation": "set", "token": "PRESET_7" }
```

---

# Terminal application messages

Capture *completion* is announced as an application message, separate from the command reply. Its header and
channel are `ImageCaptured` / `image/captured`, `ImageCaptureFailed` / `image/failed`, or
`ImageCaptureCancelled` / `image/cancelled`, published on the camera instance's application namespace. The
envelope has version `1.0`, and its correlation ID equals the body's `correlationId`.

**The announcement is not a delivery.** It is published **once, best effort, after** the terminal state is
durably committed, and is **never retried**. It is lost when the broker/IPC transport is unavailable, and
when the component stops between the commit and the publish. The capture is unaffected — the image and
sidecar are on disk, the catalog holds the terminal state, and `sb/capture-status` answers for it; that
pair, not the message, is authoritative. A consumer that must not miss an outcome polls `sb/capture-status`.
The body a message carries is exactly the `result` that `sb/capture-status` returns, field for field.

The version-1 terminal body (`schemaVersion: 1`):

| Field | Type | Meaning |
|---|---|---|
| `schemaVersion` | 1 | Body schema version. |
| `eventId` | string | Deduplication id (`evt_…`). |
| `captureId` | string | Durable capture key. |
| `cameraId` | string | Camera instance. |
| `correlationId` | string | Original request correlation. |
| `trigger` | object | `{ type: "command" \| "group-command" \| "schedule", … }`; command triggers carry `requestId`, schedules carry `scheduleId`/`intendedFireTime`. |
| `captureProfile` | string | Effective profile. |
| `captureMode` | enum | Actual acquisition mechanism (`snapshot-uri`, `rtsp-frame`, `software-trigger`, `simulated`). |
| `timestamps` | object | `requestedAt`, optional `acquisitionStartedAt`/`cameraFrameAt`/`frameReceivedAt`/`persistedAt`, and `cameraFrameTimestampQuality` (`device`/`adapter-receive`/`unknown`). |
| `durationsMs` | object | Optional `queue`/`acquisition`/`encoding`/`persistence`, required `total`. |
| `image` | object | **Success only**: `absolutePath`, `relativePath`, `fileUri`, `contentType`, `encoding`, `bytes`, `sha256`, optional `metadataSidecarRelativePath`. |
| `frame` | object | When a frame arrived: `width`, `height`, `pixelFormat`, `sourceEncoding`. |
| `camera` | object | `backend`, optional `vendor`/`model`/`firmware`/`serial`. |
| `metadata` | object | Caller metadata, verbatim. |
| `failure` | object | **Failure only**: `code`, `stage`, `retriable`, `message`. |
| `captureGroupId`, `groupSize` | string, usize | Group members only. |
| `backendMetadata` | object | Bounded, credential-free; omitted when empty. |

A successful body carries `image` and no `failure`; a failed body carries `failure` and no `image`; a
cancelled body carries neither.

## Capture lifecycle diagnostics

`component.global.operatorEvents.captureLifecycle` is disabled by default. When enabled, the adapter
publishes best-effort, non-durable per-camera `evt` diagnostics through the EdgeCommons `events()` facade,
routed to `ecv1/{device}/camera-adapter/{cameraId}/evt/{severity}/{type}`:

| Type | Severity | Stable `context` |
|---|---|---|
| `capture-queued` | `debug` | `captureId`, `trigger` (`command`/`group-command`/`schedule`), `captureProfile`, one-based `queuePosition` |
| `capture-started` | `info` | `captureId`, `trigger`, `captureProfile`, `captureMode` |

Context deliberately excludes request metadata, correlation IDs, output paths, image data, and
backend/device details. Publication is bounded and capped; saturation drops the diagnostic rather than
delaying a capture. Terminal completion is announced on the `app/image/*` contract above, never as a
lifecycle event.

## Stable errors

The public error `code` is one of `INSTANCE_REQUIRED`, `UNKNOWN_INSTANCE`, `CAMERA_DISABLED`,
`CAMERA_UNAVAILABLE`, `CAMERA_MOVING`, `UNSUPPORTED_CAPABILITY`, `INVALID_REQUEST`,
`UNKNOWN_CAPTURE_PROFILE`, `QUEUE_FULL`, `GROUP_TOO_LARGE`, `RESOURCE_LIMIT`, `CAPTURE_TIMEOUT`,
`CAPTURE_CANCELLED`, `PROCESS_INTERRUPTED`, `CAPTURE_NOT_FOUND`, `IDEMPOTENCY_CONFLICT`,
`PREVIOUS_OUTCOME_UNKNOWN`, `REPLY_REQUIRED`, `UNSUPPORTED_PIXEL_FORMAT`, `STORAGE_PRESSURE`,
`PERSISTENCE_FAILED`, `PTZ_DISABLED`, `PTZ_RANGE_ERROR`, `PTZ_TIMEOUT`, `COMPONENT_STOPPING`, or
`BACKEND_ERROR`. Branch on `code`, not the sanitized human-readable message.

`STORAGE_PRESSURE` on a capture submission means the output or state root is below its configured
free-space floor, or cannot be read. The component refuses new captures until that root recovers; a broker
it cannot reach never has this effect, because messaging is not what a capture consumes.

## Operator events

`schedule-skipped` is a warning `evt` emitted for a scheduled capture skipped because the camera is moving.
It carries the camera/schedule occurrence context, not image bytes or credentials. The component alarms
`storage-low` and `message-publish-degraded` are documented in the [metrics reference](metrics.md).

## Capture thumbnail

When a capture profile asks for one, the published capture result carries a `thumbnail` beside the image
metadata:

```jsonc
"thumbnail": { "encoding": "jpeg", "width": 320, "height": 240, "bytes": 14231, "data": "<raw JPEG bytes>" }
```

`data` is a binary value — raw bytes on the wire, not base64. The thumbnail is a lossy re-encode of the same
frame and carries **no digest**: only the artifact's `sha256` describes the installed image. It is present
**only** in the published announcement — never written to the sidecar, stored in the catalog, or included in
an `sb/capture` reply, a group reply, or a result republished after a restart. It is also never allowed to
cost the result: if a message cannot be published with the thumbnail, the result is published again without
it, and the thumbnail is dropped and counted. The transport caps the size it can carry — Greengrass IPC
carries only `small` and reduces a larger request to it; MQTT carries all three. See the
[capture-profile reference](configuration.md#camera-instance-fields) for `thumbnail.size`.
