# Explanation

The adapter separates durable intent from camera I/O. A command first passes the closed request schema,
per-camera idempotency ledger, admission control, and catalog transaction. Only then does one serialized
camera actor acquire, encode, and persist an image. The actor is isolated per camera: one camera's
authentication, reconnect, or protocol failure does not block another camera.

Capture acceptance and completion are distinct. `sb/capture-submit` returns a durable capture ID quickly;
the terminal application message is the authoritative outcome. `sb/capture` uses a deferred reply whose
terminal outcome is settled by the same durable job. Both carry the original correlation ID. Group capture
is software fan-out with one aggregated reply; it deliberately makes no hardware synchronization claim.

Bounded admission prevents a fast caller from exhausting the device. Global limits cover connected cameras,
acquisition, encoding, persistence, connection attempts, bytes in flight, and per-camera queues. A capture
reservation accounts for output and state capacity before work begins. Backpressure therefore results in a
stable rejected command, not unbounded memory, task, or database growth.

The SQLite catalog owns durable job state, including the encoded terminal body of each finished capture.
The transaction that commits a terminal state also commits that body, and the writer that wins it
announces the result once. A final image becomes visible only after the partial file is written, checked,
flushed, optional metadata sidecar is installed, and no-overwrite finalization succeeds.

A catalog that cannot be read — an unusable file, or one that fails SQLite's integrity check — is moved
aside and the adapter starts on a new, empty catalog, so an unattended device keeps capturing. The damaged
file is kept next to the state directory's database as `camera-adapter.sqlite3.corrupt` (with its
write-ahead log and shared-memory sidecars) and the adapter logs an error naming it. One quarantined copy
is retained; a subsequent corruption replaces it. The capture records held in the damaged file are lost, so
`sb/capture-status` can no longer answer for them; the images and sidecars they name stay on disk under the
output root. A catalog whose schema version is *newer* than the running adapter is a different
case: its rows are intact, so the adapter refuses to start rather than discard them, and the file is left
untouched.

The image a capture delivers is the image the camera produced. A snapshot is passed through unchanged
where the camera's encoding is already the requested output, and the digest reported with a result is the
digest of those bytes. An encoded image that arrives incomplete — a JPEG or PNG whose data ends before the
picture does — is refused rather than delivered: a partial image still decodes to the declared dimensions,
so completeness is checked directly. On an ONVIF camera with `rtspFallback: true`, an incomplete snapshot
falls back to the RTSP stream; otherwise the capture fails and says why.

The result of a capture is durable; the message that announces it is not. A terminal state, its image, and
its sidecar are committed before anything is published, and the announcement is then sent once, best
effort, and never retried. A component that cannot reach its broker is therefore degraded, not stopped: it
keeps capturing, keeps persisting, keeps retrying the connection, and simply does not announce. Each
announcement it cannot send is logged, counted as `camera_captures.announcementFailed`, and raises the
`message-publish-degraded` alarm, which the next successful announcement clears. Nothing about a broker
outage fails or rejects a capture — but an announcement dropped while it lasts, or one lost because the
component stopped between the commit and the publish, is gone for good. `sb/capture-status` and the
catalog remain the authority on what was captured.

`sim` is deterministic and supports logic testing. `onvif-rtsp` uses strict ONVIF service/profile selection
and can use GStreamer RTSP extraction when compiled. `genicam-aravis` is an optional Linux-native backend.
Simulator evidence establishes the implemented protocol paths; it does not certify physical models, NICs,
USB topologies, cameras, or deployment environments.
