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

The SQLite catalog owns durable job state and the outbox owns exactly one encoded terminal envelope per
terminal job. The outbox retries uncertain broker delivery without changing that envelope's UUID. A final
image becomes visible only after the partial file is written, checked, flushed, optional metadata sidecar
is installed, and no-overwrite finalization succeeds.

`sim` is deterministic and supports logic testing. `onvif-rtsp` uses strict ONVIF service/profile selection
and can use GStreamer RTSP extraction when compiled. `genicam-aravis` is an optional Linux-native backend.
Simulator evidence establishes the implemented protocol paths; it does not certify physical models, NICs,
USB topologies, cameras, or deployment environments.
