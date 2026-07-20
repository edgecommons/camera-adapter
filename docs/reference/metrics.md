# Metrics and alarms reference

The adapter emits four metrics through the EdgeCommons metric subsystem, which routes them to the target
selected by the component's `metricEmission` configuration.

The operational families follow the EdgeCommons `(Total, Interval)` counter convention: every counter is
emitted as a pair — `<name>Total` is monotonic since start, and `<name>Interval` is what accrued since the
previous emission and resets on each one. Levels (gauges) and latency sums are single measures.

`southbound_health` is the standard metric every adapter in the ecosystem emits, dimensioned by
`instance` so each camera reports its own. It is sampled every 30 seconds, and emitted immediately when
a camera connects or disconnects.

| Measure | Unit | Meaning |
|---|---|---|
| `connectionState` | Count | 1 while the camera's session is live, 0 otherwise. |
| `publishLatencyMs` | Milliseconds | How long the camera's last terminal message took to reach the transport. Absent until one has. |
| `pollLatencyMs` | Milliseconds | The last acquisition round-trip. Absent until the camera has produced a frame. |
| `readErrors` | Count | Acquisition failures in the interval. A failure to encode or to write to disk is not counted: it is not the camera's fault. |
| `staleSignals` | Count | 1 when the camera has produced nothing within `healthThresholds.staleSignalSecs`, 0 otherwise. A camera can be connected and stale. |
| `reconnects` | Count | Sessions re-established in the interval. A camera's first connection is not a reconnect. |

`camera_captures` counts the capture lifecycle. Each measure below is a `(Total, Interval)` counter pair,
accumulated on the capture hooks and drained every 30 seconds. The interval counter carries every event
since the last drain, so a capture that starts and finishes between two drains is still counted — the drain
is a reset, not a sample, and nothing is lost. Each row names the base measure; the emitted names are
`<measure>Total` and `<measure>Interval`.

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
| `announcementRetriedWithoutPreview` | Count | An announcement carrying a thumbnail could not be published, so it was re-published without the thumbnail. The capture succeeds; the preview is shed so the result is still announced. |

`CameraCommand` is the operational command family, dimensioned by `instance`, `verb` (the `sb/*` command),
and `result` (`success` or `error`). Its cells are drained every 30 seconds, and a cell is created the
first time an `(instance, verb, result)` combination is seen, so its cardinality tracks the commands an
operator actually issues.

| Measure | Unit | Meaning |
|---|---|---|
| `commandRequests` | Count | Commands handled for this `(instance, verb, result)` — a `(Total, Interval)` pair. |
| `commandErrors` | Count | Of those, the ones that failed — a `(Total, Interval)` pair (the `error` result cell). |
| `commandLatencyMs` | Milliseconds | Summed command-handling latency over the interval. |

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

`camera_captures` and `camera_queue` are fleet-scoped and carry no per-camera dimension: a 256-camera fleet
would otherwise mint 256 metric streams per measure on the highest-frequency families. Per-camera queue
depth is answered by `sb/queue-status`, and per-camera reachability by the state keepalive below.
`CameraCommand` does carry the `instance` dimension, because commands are operator-frequency rather than
capture-frequency.

## Per-camera presence

Every configured camera's reachability is published in the component's state keepalive, in the
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
