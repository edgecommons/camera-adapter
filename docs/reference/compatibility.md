# Compatibility

The camera adapter is validated against deterministic protocol simulators. A passing simulator
exercises a protocol path; it does not establish that a specific physical camera model, NIC, USB
topology, cluster CNI, or deployment platform is supported.

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

Physical cameras are not supported. No physical camera model, firmware, NIC, USB topology, or encoder
is validated, including:

- GigE Vision and USB3 Vision vendor cameras
- ONVIF Profile S/T vendor cameras
- ONVIF PTZ operations and presets on hardware
- HTTPS/Digest ONVIF cameras
- ONVIF cameras requiring RTSP fallback
- the high-resolution and pixel-format matrix

## Platform support

| Platform | Checked-in artifact | Notes |
|---|---|---|
| HOST / Linux | Dockerfile, service guidance, native simulator stack | Runs with the ONVIF/RTSP simulator and native stack. |
| HOST / Windows | ProgramData state path, portable output profile, deployment ACL guidance | The portable no-overwrite persistence profile is supported; the Linux hostile-local-actor containment guarantees do not apply on Windows. |
| Greengrass | Recipe template and IPC policy | Runs over Greengrass IPC. |
| Kubernetes | ConfigMap, RWO PVC, single-replica Deployment template | Runs as a single active pod with a PVC. |
