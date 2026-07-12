# Sample configurations

Each object is embedded under `component.instances[]` in a complete EdgeCommons configuration. Every
production configuration also sets absolute durable `component.global.output.rootDirectory` and
`component.global.state.directory` paths.

## Command-only simulator

```json
{
  "id": "sim-a",
  "backend": { "type": "sim", "frame": { "width": 640, "height": 480, "pixelFormat": "rgb8", "pattern": "color-bars" } },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "output": { "encoding": "png" } } }
}
```

## Scheduled simulator fleet member

```json
{
  "id": "sim-hourly",
  "backend": { "type": "sim" },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "output": { "encoding": "jpeg", "jpegQuality": 90 } } },
  "schedules": [{ "id": "hourly", "cron": "0 0 * * * *", "timezone": "UTC", "captureProfile": "inspection" }]
}
```

## ONVIF snapshot

```json
{
  "id": "dock-camera",
  "backend": {
    "type": "onvif-rtsp",
    "deviceServiceUrl": "https://camera.example/onvif/device_service",
    "credentials": { "$secret": "cameras/dock" },
    "mediaProfile": "main",
    "allowedUriHosts": ["camera.example"]
  },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "captureMode": "snapshot-uri", "output": { "encoding": "png" } } }
}
```

## ONVIF RTSP fallback

```json
{
  "id": "line-camera",
  "backend": {
    "type": "onvif-rtsp",
    "deviceServiceUrl": "https://line-camera.example/onvif/device_service",
    "mediaProfile": "main",
    "allowedUriHosts": ["line-camera.example"],
    "captureMode": "snapshot-uri",
    "rtspFallback": true
  },
  "defaultCaptureProfile": "inspection",
  "captureProfiles": { "inspection": { "output": { "encoding": "jpeg", "jpegQuality": 90 } } }
}
```

## GigE Vision and USB3 Vision

```json
{ "id": "gige-a", "backend": { "type": "genicam-aravis", "selector": { "serial": "CAM-001" }, "transport": "gige-vision", "interface": "enp2s0" }, "defaultCaptureProfile": "raw", "captureProfiles": { "raw": { "output": { "encoding": "raw" } } } }
```

```json
{ "id": "usb-a", "backend": { "type": "genicam-aravis", "selector": { "deviceId": "usb3-1" }, "transport": "usb3-vision" }, "defaultCaptureProfile": "inspection", "captureProfiles": { "inspection": { "output": { "encoding": "png" } } } }
```

## PTZ policy

```json
{
  "enabled": true,
  "maximumContinuousMoveMs": 10000,
  "allowPresetMutation": false
}
```

The model/firmware and platform combinations in the physical compatibility register remain `NOT RUN`
until evidence is recorded.
