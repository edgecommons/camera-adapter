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

The project owner has explicitly waived physical-camera testing because no hardware is available. No
physical camera model is recorded as passing, and this waiver does not authorize a claim of compatibility
with any model, firmware, NIC, USB topology, encoder, or device timing.

| Required class | Status | Release claim permitted |
|---|---|---|
| Two GigE Vision vendor families | WAIVED — NO HARDWARE AVAILABLE | None |
| Two USB3 Vision vendor families | WAIVED — NO HARDWARE AVAILABLE | None |
| Two ONVIF Profile S/T vendor families | WAIVED — NO HARDWARE AVAILABLE | None |
| ONVIF PTZ camera (all operations/presets) | WAIVED — NO HARDWARE AVAILABLE | None |
| HTTPS/Digest ONVIF camera | WAIVED — NO HARDWARE AVAILABLE | None |
| ONVIF camera requiring RTSP fallback | WAIVED — NO HARDWARE AVAILABLE | None |
| High-resolution and supported pixel-format matrix | WAIVED — NO HARDWARE AVAILABLE | None |

## Platform evidence status

| Path | Current checked-in artifact | Validation status |
|---|---|---|
| HOST/Linux | Dockerfile, service guidance, native simulator stack | Simulator/native gates exist; scale/soak remains required. Physical camera evidence is waived, with no hardware compatibility claim. |
| HOST/Windows | ProgramData state path plus portable output profile and deployment ACL guidance | Unit tests cover the portable no-overwrite persistence profile; end-to-end service and hardware gates are not recorded, and it does not claim Linux hostile-local-actor containment |
| Greengrass | Recipe template and IPC policy | Deployment/IPC hardware gate not recorded |
| Kubernetes | ConfigMap, RWO PVC, single-replica Deployment template | kind and hardware-cluster gates not recorded |
| Full system | No camera entry in the bottling-company harness yet | Not run |

Before promotion, attach immutable command logs, image digests/package versions, test and coverage
results, representative terminal envelopes, capture checksums/metadata, resource/soak graphs, and the
physical-camera waiver with its excluded claims to the release record.
