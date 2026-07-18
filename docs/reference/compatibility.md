# Compatibility

The camera adapter is designed for standards-compliant ONVIF/RTSP network cameras and GenICam (GigE
Vision and USB3 Vision) machine-vision cameras — including RTSP-only cameras with no ONVIF, through the
dedicated `rtsp` backend. It is **validated against deterministic protocol simulators**, not against
physical hardware: a passing simulator exercises a protocol path, but it does
not establish that a specific physical camera model, NIC, USB topology, cluster CNI, or deployment
platform will interoperate without adjustment. Real devices vary in how faithfully they implement the
standards, so you must validate your own cameras — see [Physical cameras](#physical-cameras).

## Simulator stack

| Layer | Implementation | What it exercises | What it does not establish |
|---|---|---|---|
| In-process cameras | Rust `SimBackend` | Deterministic frame generation, actor/job behavior, PTZ ranges, cancellation and fault injection | Any camera protocol or device timing |
| ONVIF + WS-Discovery | `simulators/onvif_sim` | Device/media services, Media1/Media2 selection, snapshot URI, PTZ/presets, Digest/WSSE fixtures, TLS, malformed/hostile responses | Vendor SOAP quirks or hardware PTZ movement |
| RTSP | MediaMTX, pinned by digest in `simulators/image-lock.json` | H.264/H.265 negotiation and complete RGB frame extraction | Camera encoder interoperability or physical acquisition timing |
| GigE Vision | Aravis 0.8.36 `arv-fake-gv-camera` | Explicit interface discovery, selector binding, software trigger, buffer acquisition | USB3 Vision or vendor-specific GenICam nodes |
| Fault injection | Toxiproxy plus Linux `tc netem` | TCP latency/disconnect/half-open and Linux network fault scenarios | Production network performance or safety-policy bypass |

Run the suite from [simulators/README.md](../../simulators/README.md). Multicast WS-Discovery and GigE
discovery require Linux host/L2 networking; a Docker bridge/NAT run does not exercise multicast or L2
discovery.

## Physical cameras

No physical camera has been validated. No hardware was available for this project, so no specific camera
model, firmware, NIC, USB topology, or encoder has been exercised against the adapter, and none is
certified — the adapter must not be represented as hardware-certified on the strength of simulator
results.

This is a **validation gap, not a design limitation**. The adapter implements the ONVIF, RTSP, and
GenICam standards and is intended to work with compliant devices. In practice, cameras differ in how
faithfully they implement those standards, and a specific model's quirks — a non-conformant SOAP
response, an unusual authentication flow, an encoder that negotiates unexpectedly — may require a change
to the adapter to interoperate. **Validate each camera model and firmware in your own environment before
relying on it in production**, and treat a newly introduced model as unproven until you have. Exercise at
least:

- GigE Vision and USB3 Vision vendor cameras
- ONVIF Profile S/T vendor cameras
- ONVIF PTZ operations and presets on hardware
- HTTPS/Digest ONVIF cameras
- ONVIF cameras that fall back to RTSP
- your resolution and pixel-format matrix

## Platform support

| Platform | Checked-in artifact | Notes |
|---|---|---|
| HOST / Linux | Dockerfile, service guidance, native simulator stack | Runs with the ONVIF/RTSP simulator and native stack. |
| HOST / Windows | ProgramData state path, portable output profile, deployment ACL guidance | The portable no-overwrite persistence profile is supported; the Linux hostile-local-actor containment guarantees do not apply on Windows. |
| Greengrass | Recipe template and IPC policy | Runs over Greengrass IPC. |
| Kubernetes | ConfigMap, RWO PVC, single-replica Deployment template | Runs as a single active pod with a PVC. |
