# Explanation — How the Camera Adapter Works and Why

This page explains the ideas behind the adapter so that its configuration options and message shapes make
sense as a whole. If you only need a specific option or a step-by-step procedure, the
[reference](reference/configuration.md) and the [how-to guides](how-to-guides.md) are quicker.

## What the component is for

Most EdgeCommons southbound adapters read a protocol and publish a stream of small `SouthboundSignalUpdate`
messages — a Modbus register, an OPC UA node. The camera adapter is different in kind: its data product is
an **image file**, not a bus message. It connects to many cameras at once, captures still images on a
schedule or on command, writes each one as a complete, checksum-verified file under a local output root,
and then publishes a small message that *announces* the file and says where it is.

**The file is the data plane; the bus carries control and metadata.** Image bytes are never sent over MQTT
or Greengrass IPC. A 12-megapixel frame does not belong on a control bus, and copying it there would couple
acquisition to delivery. Instead the adapter persists the image and publishes its paths, size, and digest;
a downstream component such as [file-replicator](https://docs.edgecommons.mbreissi.com/components/file-replicator/)
watches the output root and moves the file onward. Acquisition and delivery stay independently deployable.

The adapter is one runtime with **one configuration model, one command contract, and one published-message
contract**, behind **multiple backends**. ONVIF/RTSP network cameras and GenICam machine-vision cameras are
not separate components — they are backend implementations selected per camera. Each camera is one
`component.instances[]` entry with its own EdgeCommons instance identity, and every message it produces is
stamped with that identity.

## Durable intent first, camera I/O second

The adapter is built in two halves, and keeping them apart is what makes it safe under load.

A request is first turned into **durable intent**. It passes the closed request schema (unknown fields are
rejected, not ignored), the per-camera **idempotency ledger** keyed on the caller's `requestId`, **admission
control**, and finally a **catalog transaction** that records the accepted capture in SQLite. Only after
that does the second half run: a single **camera actor**, serialized per camera, acquires the frame, encodes
it, and persists the file.

The actor is isolated per camera. One camera's authentication failure, slow reconnect, or protocol quirk
blocks only that camera's queue — every other camera keeps capturing. This is why the adapter can hold 256
configured cameras in one process: an idle or broken camera costs a supervisor and a small queue, not an
image-sized buffer.

## The capture lifecycle: acceptance is not completion

Two things happen to a capture, and they are deliberately distinct: it is **accepted**, and later it
**completes**. Conflating them is the most common integration mistake.

- **`sb/capture-submit`** returns immediately with a durable `captureId`. That reply means "recorded and
  queued," never "image written." Completion arrives later as a terminal application message, or you read it
  from status.
- **`sb/capture`** uses a *deferred reply*: the caller's request stays open and the single terminal reply is
  settled by the same durable job. It is the request/reply convenience over the same machinery.
- **`sb/capture-group`** fans one request out into independent per-camera jobs joined by a shared
  `captureGroupId`, and returns one aggregated reply. It is software fan-out; it makes **no** hardware-
  synchronization claim, so it is the right tool for "an evidence set from these cameras," not for sub-frame
  simultaneous acquisition.

Every capture carries the original correlation ID from acceptance through its terminal message, so an
asynchronous consumer can tie an announcement back to the request that caused it. A capture ends in exactly
one **terminal state** — `SUCCEEDED`, `FAILED`, `CANCELLED`, or `INTERRUPTED` — and that state, in the
catalog, is the authority on what happened.

## Bounded admission: a fast caller cannot sink the fleet

Camera *count* is not a safe bound on memory or bandwidth, so the adapter admits work against explicit,
byte-aware limits rather than trusting callers to behave. Global limits cover connected cameras, concurrent
acquisition, encoding, persistence, connection attempts, **raw bytes in flight**, per-camera queues, and the
total pending-capture backlog. A capture also takes a **reservation** against output and state free space
*before* any work begins, using its profile's declared frame ceiling rather than the frame's eventual real
size.

The result is that backpressure is always a **stable, named rejection** — `QUEUE_FULL`, `RESOURCE_LIMIT`,
`GROUP_TOO_LARGE`, or `STORAGE_PRESSURE` — never unbounded growth of memory, tasks, or database rows. A
`resourceGroup` adds a shared acquisition cap for cameras that contend for one physical resource (a NIC or a
USB controller), so a full GigE link throttles its own cameras without starving the rest of the fleet.

## The catalog is the source of truth

A single SQLite **catalog** owns all durable job state, including the encoded **terminal body** of each
finished capture. The transaction that commits a capture's terminal state commits that body in the same
step, and the writer that wins the transaction is the one — and the only one — that announces the result.
Because the committed body *is* the record, a result read later from `sb/capture-status` is identical, field
for field, to the message that announced it.

A **final image becomes visible only after** the partial file has been fully written, its bytes checked for
completeness, the data flushed, the optional metadata sidecar installed, and no-overwrite finalization has
succeeded. A success message can therefore never point at a half-written file. If two captures would resolve
to the same output path, the second is refused with `PERSISTENCE_FAILED` rather than silently overwriting
the first — which is why the file-name or camera-directory template must include `{captureId}`.

## Crash recovery: keep capturing, never discard silently

An unattended edge device must survive a corrupt database without a human. When the catalog cannot be read —
an unusable file, or one that fails SQLite's integrity check — the adapter moves it aside to
`camera-adapter.sqlite3.corrupt` (with its WAL and shared-memory sidecars), logs an error naming it, and
starts on a fresh, empty catalog so capture continues. One quarantined copy is kept; a later corruption
replaces it. The capture *records* in the damaged file are lost, so `sb/capture-status` can no longer answer
for them — but the images and sidecars they named are still on disk under the output root.

A catalog whose schema version is *newer* than the running adapter is the opposite case: its rows are
intact, so discarding them would be data loss. The adapter refuses to start and leaves the file untouched
rather than throw away records it does not understand.

Captures that were still queued when the process stopped are governed by `state.queuedRecoveryPolicy`.
`requeue` puts a capture back on the queue only if it had not yet reached a camera, its deadlines have not
passed, and its camera is still configured, enabled, and on the same backend; `interrupt` retires them all
with `PROCESS_INTERRUPTED`. Anything a camera had already started is retired either way — the adapter will
not silently re-drive physical work it cannot prove was incomplete.

## The image is the image the camera produced

Fidelity is a contract, not a best effort. When a camera's own encoding already matches the requested
output, the snapshot is **passed through unchanged**, and the `sha256` reported with the result is the digest
of exactly those bytes. The adapter never silently substitutes a mode or an encoding you did not ask for: a
profile that requests `jpeg` will not quietly re-encode a passthrough JPEG, and a profile that requests
`passthrough` requires a JPEG source — the mismatch is an error, not a silent conversion.

An encoded image that arrives **incomplete** — a JPEG or PNG whose data ends before the picture does — is
refused rather than delivered. Completeness is checked directly, because a truncated image still *decodes* to
its declared dimensions and would otherwise look valid. On an ONVIF camera with `rtspFallback: true`, an
incomplete snapshot falls back to the RTSP stream; otherwise the capture fails and says why. Unsupported
Bayer/PFNC pixel input is rejected as `UNSUPPORTED_PIXEL_FORMAT` — raw bytes are never mislabeled as RGB.

## The result is durable; the announcement is not

This is the single most important thing to understand about the adapter's messaging.

A capture's terminal state, its image, and its sidecar are all committed **before** anything is published.
The terminal application message is then sent **once, best effort, and never retried**. It is an
*announcement*, not a delivery.

So a broker or IPC transport the adapter cannot reach makes it **degraded, not stopped**: it keeps
capturing, keeps persisting, keeps retrying the connection, and simply does not announce. Each announcement
it cannot send is logged, counted as `camera_captures.announcementFailed`, and raises the
`message-publish-degraded` alarm, which the next successful announcement clears. Nothing about a broker
outage ever fails or rejects a capture — messaging is not a resource a capture consumes.

The trade-off is explicit: an announcement dropped while the broker is down, or lost because the component
stopped between the commit and the publish, is **gone for good**. The catalog and `sb/capture-status` remain
the authority on what was captured. A consumer that must not miss an outcome **polls `sb/capture-status`**
rather than relying on the announcement to arrive.

## PTZ safety: a move always stops

Pan/tilt/zoom is disabled per camera until `ptz.enabled` is set, and preset *mutation* stays disabled until
`ptz.allowPresetMutation` is set — reading and recalling presets does not imply permission to overwrite
them.

A continuous move must carry a bounded `timeoutMs`, which may not exceed the camera's
`ptz.maximumContinuousMoveMs`. Before it replies, the adapter **arms its own stop** for that instant and
reports it as `stopDeadline`. The camera is told the timeout too, but the adapter does not rely on it: the
move stops on time whether or not the camera honors its own timeout, and whether or not the requester is
still connected. The stop travels a **safety lane** served ahead of queued controls and captures — a capture
in progress is cancelled to let it through, because a camera that has been told to stop matters more than an
image. An explicit stop, or any later PTZ command, retires the armed stop.

## The backends

One command and message contract sits over three backends, each advertising the capabilities it actually
has:

- **`sim`** is a deterministic in-process camera for logic testing and development. It synthesizes seeded
  frames (color-bars, gradient, checkerboard, solid), can model PTZ, and can inject deterministic faults
  (fail every *n*th capture, deliver an incomplete frame, disconnect after *n*). It exercises the adapter's
  own behavior, never a real protocol or device timing.
- **`onvif-rtsp`** does strict ONVIF service and media-profile selection and captures via the snapshot URI;
  when the `rtsp` feature is compiled in, it can also extract a complete frame from the RTSP stream, either
  as a fallback from a bad snapshot or as the required capture mode. It provides the PTZ implementation.
- **`genicam-aravis`** is an optional Linux-native backend (built only with the `genicam` feature) for GigE
  Vision and USB3 Vision cameras, using a software trigger and buffer acquisition through Aravis.

These are software implementations of the protocols. Specific physical camera models, NICs, USB topologies,
and deployment environments are **not** certified by the adapter — see [compatibility](reference/compatibility.md)
for exactly what the simulator stack does and does not establish.

## Thumbnails: an optional preview on the bus

Because the image itself never travels on the bus, a capture profile may opt into a small **thumbnail** so a
consumer can *see* the picture without fetching the file. It is deliberately constrained: it is a lossy JPEG
re-encode carried **only in the announcement**, never written to the sidecar or stored in the catalog, and
it carries **no digest** — a preview must not invite the belief that it can be verified against the real
image, whose `sha256` describes the installed file and nothing else.

The messaging transport decides what size can be carried. On Greengrass **IPC**, a whole message is encoded
into a fixed 10,000-byte buffer, so only a `small` thumbnail fits and a larger configured size is reduced to
`small` (announced once per camera at startup). On **MQTT** all three sizes fit. And a thumbnail never costs
the result it decorates: if an announcement carrying one cannot be published, the result is announced again
without it, and the drop is counted. A result republished after a restart carries no thumbnail at all — the
frame it would be made from is gone.
