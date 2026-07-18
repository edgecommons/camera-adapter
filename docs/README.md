# Camera Adapter — Documentation

`com.mbreissi.edgecommons.CameraAdapter` is the EdgeCommons **southbound image-capture adapter**. It
connects to many industrial cameras at once — **ONVIF/RTSP** network video cameras and **GenICam**
machine-vision cameras (GigE Vision and USB3 Vision) — and captures **still images** on a schedule or on
command. Each capture becomes a complete, checksum-verified **file** under a local output root, and the
adapter announces it on the fleet's **Unified Namespace** with the paths, digest, and metadata a consumer
needs to find and trust it. Built on the `edgecommons` Rust library, it runs wherever you deploy it — as a
Greengrass v2 component (IPC), a standalone HOST process, or a single-writer Kubernetes pod.

The organizing idea is that **the file is the data product and the bus carries control**. Image bytes are
never published over MQTT or Greengrass IPC; they are written to disk, and a downstream component such as
[file-replicator](https://docs.edgecommons.mbreissi.com/components/file-replicator/) picks them up from
there. The adapter appears on the bus as `ecv1/{device}/camera-adapter/…` and answers UNS commands on its
component inbox (`ecv1/{device}/camera-adapter/cmd/sb/{verb}`) — capture, group capture, status, queue
control, reconnect, and capability-gated **PTZ** — alongside the library built-ins. A durable SQLite
catalog makes submissions idempotent and lets `sb/capture-status` answer for any capture across a restart,
so a broker outage degrades the adapter (it keeps capturing and persisting) rather than stopping it.

| Doc | Start here when you want to… |
|-----|------------------------------|
| **[Tutorial](tutorial.md)** | learn by doing — bring one adapter up against the checked-in ONVIF simulator, submit a real command, and get the correlated reply |
| **[How-to guides](how-to-guides.md)** | accomplish a specific task — schedule a camera, move/stop PTZ safely, use ONVIF auth and TLS, tune GenICam, configure RTSP fallback, hand files to file-replicator |
| **[Sample configurations](sample-configurations.md)** | copy a complete, annotated config for your scenario and understand what each option makes the adapter do |
| **[Reference](reference/configuration.md)** | look up an exact option, command, message field, metric, or supported protocol |
| **[Explanation](explanation.md)** | understand how it works and why — durable intent vs. camera I/O, the capture lifecycle, bounded admission, the catalog, image fidelity, and announcement-vs-delivery |

## Reference

- **[Configuration](reference/configuration.md)** — every `component.global` and `component.instances[]` field, with defaults and meaning.
- **[Messaging interface](reference/messaging-interface.md)** — the `sb/*` command verbs, terminal `app/image/*` messages, error codes, and lifecycle events.
- **[Metrics and alarms](reference/metrics.md)** — the health, capture, and queue metrics, the per-camera presence element, and the two component alarms.
- **[Compatibility](reference/compatibility.md)** — what the simulator stack validates, and the boundary of that validation.

## Deployment

- **[HOST](deployment/host.md)** — one process for a disjoint camera set on Linux or Windows, plus Docker.
- **[Greengrass](deployment/greengrass.md)** — the recipe template, durable paths, and least-privilege IPC policy.
- **[Kubernetes](deployment/kubernetes.md)** — a single active pod with a `ReadWriteOnce` PVC.

## Quick routing

- **"I'm new here."** → [Tutorial](tutorial.md).
- **"What does this config option do?"** → [Reference — Configuration](reference/configuration.md).
- **"How do I trigger a capture, and what message comes back?"** → [Reference — Messaging interface](reference/messaging-interface.md).
- **"How do I capture on a schedule instead of a command?"** → [How-to — Schedule a camera](how-to-guides.md#schedule-a-camera).
- **"How do I capture several cameras as one set?"** → [Reference — Group schedules](reference/configuration.md#group-schedules) and the `sb/capture-group` verb.
- **"How do I move a PTZ camera without leaving it moving?"** → [How-to — Move or stop PTZ safely](how-to-guides.md#move-or-stop-ptz-safely).
- **"Where do captured files go, and how does the consumer read them?"** → [How-to — Hand completed files to file-replicator](how-to-guides.md#hand-completed-files-to-file-replicator).
- **"Why didn't I get a capture message?"** → [Explanation — Announcement, not delivery](explanation.md#the-result-is-durable-the-announcement-is-not).
- **"What does this metric or alarm mean?"** → [Reference — Metrics and alarms](reference/metrics.md).
- **"Is my physical camera supported?"** → [Reference — Compatibility](reference/compatibility.md).

## Audience

These docs are for **integrators and operators** — the people who deploy the adapter, write its
configuration, and build the clients that command it and consume its captures. They describe the shipped
runtime contract; they do not cover modifying the adapter's own source. The internal design and release
gates live in the repository's `DESIGN.md` and `IMPLEMENTATION_SPEC.md`, which are not part of this site.
