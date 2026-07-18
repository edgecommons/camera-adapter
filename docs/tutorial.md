# Tutorial: your first capture

This tutorial brings up one adapter against the checked-in ONVIF **simulator**, submits a real MQTT command,
and receives the correlated acceptance reply — the whole command path, end to end, with no camera hardware.
It then shows the one change that turns the simulated camera into a real ONVIF one. It is a local functional
walkthrough, not physical-camera certification.

By the end you will have seen an `sb/capture-submit` command accepted, a durable `captureId` returned, and
understood why acceptance is not yet a written image.

## 1. Run the simulator deployment

From the EdgeCommons umbrella, start the isolated stack and wait for readiness:

```bash
docker compose -f camera-adapter/deploy/docker/compose.yaml up --build -d --wait
curl --fail http://127.0.0.1:18081/readyz
```

The stack is a self-contained acceptance fixture: a pinned EMQX broker, an anonymous ONVIF simulator, and
the adapter, wired together. It binds its EMQX listener only to `127.0.0.1:1884` and its health port only to
`127.0.0.1:18081` — loopback, not a production broker. A `200` from `/readyz` means the adapter validated its
config, recovered its catalog, found its output usable, and acknowledged its command subscription.

## 2. Submit a capture

Run the test client from the adapter checkout. It publishes a genuine MQTT command and asserts the reply:

```bash
CAMERA_ADAPTER_DOCKER_E2E=1 \
CAMERA_ADAPTER_DOCKER_E2E_HOST=127.0.0.1 \
CAMERA_ADAPTER_DOCKER_E2E_PORT=1884 \
cargo test --no-default-features --features standalone --test docker_capture_submit
```

It publishes to the component command inbox `ecv1/NOT_GREENGRASS/camera-adapter/cmd/sb/capture-submit` and
asserts that the reply carries the **same correlation ID**, `ok: true`, and a durable `captureId`, and that
the capture is recorded as `ACCEPTED`/`QUEUED`.

That is the key idea to carry forward: `sb/capture-submit` returns *acceptance*, not an image. The capture
completes a moment later and is announced as a terminal `ImageCaptured` application message — or, if you must
not miss it, read it back with `sb/capture-status`. The [explanation](explanation.md#the-capture-lifecycle-acceptance-is-not-completion)
covers why the two are separate.

## 3. Turn the simulated camera into a real ONVIF one

Create durable output and state roots as described in the [HOST runbook](deployment/host.md), then replace
the simulator instance with an `onvif-rtsp` one. The only structural change is the **`backend`** object —
everything else about an instance (its `id`, `defaultCaptureProfile`, `captureProfiles`) stays the same:

```json
{
  "id": "loading-dock",
  "backend": {
    "type": "onvif-rtsp",
    "credentials": { "$secret": "cameras/loading-dock" },
    "deviceServiceUrl": "https://camera.example/onvif/device_service",
    "mediaProfile": "main",
    "allowedUriHosts": ["camera.example"],
    "tls": { "verifyHostname": true }
  },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "output": { "encoding": "png" } } }
}
```

Use a **whole credential reference**, not a password in the file: the `cameras/loading-dock` secret is a
UTF-8 JSON value containing exactly `username` and `password`. Keep `backend.allowInsecure` false, keep
`backend.tls.verifyHostname` true, and point `backend.tls.ca` at a PEM secret when the camera presents a
private CA. Physical camera models are not certified — validate your specific model's firmware,
authentication mode, PTZ behavior, and selected media profile before relying on it.

## What next

- Copy a complete config for your scenario from the [sample configurations](sample-configurations.md).
- Add a schedule, PTZ, or RTSP fallback from the [how-to guides](how-to-guides.md).
- Look up any command, message field, or option in the [reference](reference/configuration.md).
