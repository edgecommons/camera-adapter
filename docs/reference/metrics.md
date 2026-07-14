# Metrics and alarms reference

The adapter emits three metrics through the EdgeCommons metric subsystem, which routes them to the target
selected by the component's `metricEmission` configuration.

`southbound_health` is the standard metric every adapter in the ecosystem emits, dimensioned by
`instance` so each camera reports its own. It is sampled every 30 seconds, and emitted immediately when
a camera connects or disconnects.

| Measure | Unit | Meaning |
|---|---|---|
| `connectionState` | Count | 1 while the camera's session is live, 0 otherwise. |
| `pollLatencyMs` | Milliseconds | The last acquisition round-trip. Absent until the camera has produced a frame. |
| `publishLatencyMs` | Milliseconds | How long the camera's last terminal message took to reach the transport. Absent until one has. |
| `readErrors` | Count | Acquisition failures in the interval. A failure to encode or to write to disk is not counted: it is not the camera's fault. |
| `staleSignals` | Count | 1 when the camera has produced nothing within `healthThresholds.staleSignalSecs`, 0 otherwise. A camera can be connected and stale. |
| `reconnects` | Count | Sessions re-established in the interval. A camera's first connection is not a reconnect. |

`camera_captures` counts captures as they happen. It is emitted at the moment of each event, never sampled,
so a capture that starts and finishes between two collection intervals is still counted.

| Measure | Unit | Counted when |
|---|---|---|
| `queued` | Count | A capture is durably accepted and queued. |
| `started` | Count | A capture begins physical acquisition. |
| `succeeded` | Count | A capture reaches `SUCCEEDED`. |
| `failed` | Count | A capture reaches `FAILED`. |
| `cancelled` | Count | A capture reaches `CANCELLED`. |
| `interrupted` | Count | A capture reaches `INTERRUPTED`. |
| `announcementFailed` | Count | A durable terminal result could not be announced. The capture is committed, its image is on disk, and the message is dropped rather than retried; this is the count of results nobody was told about. |
| `thumbnailFailed` | Count | A capture profile asked for a thumbnail and the component could not render one from the frame. The capture succeeds and is announced without it. |
| `thumbnailDropped` | Count | A thumbnail rendered but exceeded the byte ceiling a message may carry, so it was left out. The capture succeeds and is announced without it. |

`camera_queue` samples what the component is currently holding, every 30 seconds. These are levels rather
than events, so there is nothing to miss between samples.

| Measure | Unit | Meaning |
|---|---|---|
| `dispatchQueued` | Count | Captures waiting in the fleet queue, across all cameras. |
| `durableBacklog` | Count | Captures durably accepted but not started (`ACCEPTED` + `QUEUED`). |
| `durableInFlight` | Count | Captures acquiring, encoding, or persisting. |
| `availableAcquisitions` | Count | Unused global acquisition permits. |
| `availableEncoders` | Count | Unused image-conversion permits. |
| `availableWriters` | Count | Unused image-persistence permits. |
| `availableMemoryBytes` | Bytes | Unreserved source-frame memory. |
| `outstandingDiskBytes` | Bytes | Bytes reserved against the output filesystem. |
| `camerasOnline` | Count | Cameras whose session is online. |
| `camerasConfigured` | Count | Cameras in the current configuration. |

Neither metric carries a per-camera dimension. A 256-camera fleet would otherwise mint 256 metric streams
per measure. Per-camera queue depth is answered by `sb/queue-status`.

## Per-camera presence

Every configured camera's reachability is published in the component's `main` state keepalive, in the
`instances[]` array, and the same element answers the built-in `status` verb. Consumers learn that a
camera has dropped from the keepalive rather than by polling `sb/list` or `sb/status`.

| Member | Meaning |
|---|---|
| `instance` | The camera ID. |
| `connected` | True only while the camera's protocol session is online. The normalized flag any consumer can act on. |
| `state` | The camera's own condition token: `ONLINE`, `CONNECTING`, `BACKOFF`, `OFFLINE`, `DEGRADED`, `DISABLED`, `STOPPING`. `BACKOFF` and `CONNECTING` are both `connected: false`, and they call for different responses. |
| `detail` | Why the camera is down, in its own words, when it has reported an error. A healthy camera carries none. |
| `attributes` | Camera-specific data: `backend`, the connection `generation`, and `lastErrorCode` when an error is known. |

Readiness is a component gate, not a claim that every camera is online. It requires validated
configuration, recovered catalog, usable output, active acknowledged command subscription, constructed
supervisors, at least one accepted enabled camera, available state capacity, and no shutdown.

| Alarm | Severity | Raised when | Cleared when |
|---|---|---|---|
| `storage-low` | critical | Output or state root is unreadable or falls below the configured free-space floor. | Every configured root is usable again. |
| `message-publish-degraded` | warning | A terminal announcement cannot be published. Capture results stay durable and `sb/capture-status` keeps answering; the announcements are dropped while it lasts. | A terminal announcement is published again. |

Neither alarm gates readiness on messaging: a component that cannot reach its broker keeps capturing and
keeps persisting.

Alarm context carries bounded storage/free-space information, or the camera instance, capture ID, and
stable error code of the announcement that failed. It intentionally excludes camera URLs, file paths beyond
the affected root, credentials, request metadata, and arbitrary camera error text. Capture-level outcomes
belong in terminal application messages rather than metrics dimensions.
