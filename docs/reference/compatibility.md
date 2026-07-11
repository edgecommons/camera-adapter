# Compatibility and acceptance register

This page separates deterministic simulator evidence from hardware compatibility. A green
simulator is useful protocol evidence; it is never proof that an unlisted camera model, NIC, USB
topology, cluster CNI, or deployment platform is supported.

## Implemented simulator stack

| Layer | Checked-in implementation | What it exercises | What it does not establish |
|---|---|---|---|
| In-process cameras | Rust `SimBackend` | Deterministic frame generation, actor/job behavior, PTZ ranges, cancellation and fault injection | Any camera protocol or device timing |
| ONVIF + WS-Discovery | `simulators/onvif_sim` | Device/media services, Media1/Media2 selection, snapshot URI, PTZ/presets, Digest/WSSE fixtures, TLS, malformed/hostile responses | Vendor SOAP quirks or hardware PTZ movement |
| RTSP | MediaMTX 1.19.2, pinned in `simulators/image-lock.json` | Pinned H.264/H.265 negotiation and complete RGB frame extraction | Camera encoder interoperability or physical acquisition timing |
| GigE Vision | Aravis 0.8.36 `arv-fake-gv-camera` | Explicit interface discovery, selector binding, software trigger, buffer acquisition | USB3 Vision or vendor-specific GenICam nodes |
| Fault injection | Toxiproxy plus Linux `tc netem` | TCP latency/disconnect/half-open and Linux network fault scenarios | Production network performance or safety-policy bypass |

Run the documented suite from [simulators/README.md](../../simulators/README.md). The direct
WS-Discovery harness is deterministic CI coverage; multicast and GigE discovery require Linux
host/L2 networking. A Docker bridge/NAT success is not multicast or L2 discovery evidence.

## Physical camera register

No physical camera model is recorded as passing in this repository. Required model classes remain
`NOT RUN — HARDWARE UNAVAILABLE` until the release register captures model, firmware, selector,
network/USB settings, format, capability, and result.

| Required class | Status | Release claim permitted |
|---|---|---|
| Two GigE Vision vendor families | NOT RUN — HARDWARE UNAVAILABLE | None |
| Two USB3 Vision vendor families | NOT RUN — HARDWARE UNAVAILABLE | None |
| Two ONVIF Profile S/T vendor families | NOT RUN — HARDWARE UNAVAILABLE | None |
| ONVIF PTZ camera (all operations/presets) | NOT RUN — HARDWARE UNAVAILABLE | None |
| HTTPS/Digest ONVIF camera | NOT RUN — HARDWARE UNAVAILABLE | None |
| ONVIF camera requiring RTSP fallback | NOT RUN — HARDWARE UNAVAILABLE | None |
| High-resolution and supported pixel-format matrix | NOT RUN — HARDWARE UNAVAILABLE | None |

## Platform evidence status

| Path | Current checked-in artifact | Validation status |
|---|---|---|
| HOST/Linux | Dockerfile, service guidance, native simulator stack | Simulator/native gates exist; scale/soak and physical camera gates remain required |
| HOST/Windows | ProgramData state path plus portable output profile and deployment ACL guidance | Unit tests cover the portable no-overwrite persistence profile; end-to-end service and hardware gates are not recorded, and it does not claim Linux hostile-local-actor containment |
| Greengrass | Recipe template and IPC policy | Deployment/IPC hardware gate not recorded |
| Kubernetes | ConfigMap, RWO PVC, single-replica Deployment template | kind and hardware-cluster gates not recorded |
| Full system | No camera entry in the bottling-company harness yet | Not run |

Before promotion, attach immutable command logs, image digests/package versions, test and coverage
results, representative terminal envelopes, capture checksums/metadata, resource/soak graphs, and
the physical-camera entries to the release record.
