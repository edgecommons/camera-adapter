# Tutorial: first capture

This tutorial starts one adapter against the checked-in ONVIF simulator, submits a real MQTT command,
and receives the correlated acceptance reply. It is a local functional tutorial; it is not physical-camera
certification.

## Run the simulator deployment

From the EdgeCommons umbrella, start the isolated stack and wait for readiness:

```bash
docker compose -f camera-adapter/deploy/docker/compose.yaml up --build -d --wait
curl --fail http://127.0.0.1:18081/readyz
```

The stack binds its anonymous EMQX listener only to `127.0.0.1:1884`. It is an acceptance fixture,
not a production broker.

## Submit a capture

Run the test client from the adapter checkout:

```bash
CAMERA_ADAPTER_DOCKER_E2E=1 \
CAMERA_ADAPTER_DOCKER_E2E_HOST=127.0.0.1 \
CAMERA_ADAPTER_DOCKER_E2E_PORT=1884 \
cargo test --no-default-features --features standalone --test docker_capture_submit
```

The test publishes to `ecv1/NOT_GREENGRASS/camera-adapter/cmd/sb/capture-submit` and asserts
that the reply has the same correlation ID, `ok: true`, and a durable `captureId`. Completion is
reported later as a terminal application message; acceptance never means an image has already been
written.

## Connect one physical ONVIF camera

Physical camera models are not certified — validate your specific model's firmware, authentication
mode, PTZ behavior, and selected profile before relying on it. Create durable output and state roots
as described in the [HOST runbook](deployment/host.md), then replace the simulator backend with one
explicit `onvif-rtsp` instance. Use a whole credential reference, not a password in configuration:

```json
{
  "type": "onvif-rtsp",
  "credentials": { "$secret": "cameras/loading-dock" },
  "deviceServiceUrl": "https://camera.example/onvif/device_service",
  "mediaProfile": "main",
  "allowedUriHosts": ["camera.example"]
}
```

The referenced UTF-8 JSON secret contains exactly `username` and `password`. Keep `allowInsecure`
false and use a TLS CA reference when a private camera CA is required.
