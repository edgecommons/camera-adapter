# Metrics and alarms reference

The adapter emits two metrics through the EdgeCommons metric subsystem, which routes them to the target
selected by the component's `metricEmission` configuration.

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

`camera_queue` samples what the component is currently holding, every 30 seconds. These are levels rather
than events, so there is nothing to miss between samples.

| Measure | Unit | Meaning |
|---|---|---|
| `dispatchQueued` | Count | Descriptors waiting in the supervisor dispatchers across all cameras. |
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
`instances[]` array: `instance` is the camera ID, `connected` is true only while its session is online,
and `detail` names the connection state and, when the camera is down and an error is known, the stable
error code. A healthy camera carries no `detail`. Consumers learn that a camera has dropped from the
keepalive rather than by polling `sb/list` or `sb/status`.

The keepalive stays low-cardinality by design: `detail` never carries error text or a camera URL, because
it is published for every camera every few seconds.

Readiness is a component gate, not a claim that every camera is online. It requires validated
configuration, recovered catalog, usable output, active acknowledged command subscription, constructed
supervisors, at least one accepted enabled camera, available state capacity, and no shutdown.

| Alarm | Severity | Raised when | Cleared when |
|---|---|---|---|
| `storage-low` | critical | Output or state root is unreadable or falls below the configured free-space floor. | Every configured root is usable again. |
| `message-delivery-delayed` | warning | Durable terminal outbox pressure crosses its threshold. | The outbox recovers. |

Alarm context carries bounded storage/free-space or outbox-age/count information. It intentionally excludes
camera URLs, file paths beyond the affected root, credentials, request metadata, and arbitrary camera error
text. Capture-level outcomes belong in terminal application messages rather than metrics dimensions.
