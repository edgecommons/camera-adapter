# HOST deployment

The HOST profile runs one adapter process for one disjoint set of cameras. Do not run two active
processes against the same `state.directory` or the same physical camera.

## Linux

Linux x86_64 and aarch64 are the Tier-1 packaging targets. ONVIF snapshot capture has no native
camera SDK requirement. RTSP requires the matching GStreamer runtime; GenICam requires an
architecture-matched Aravis installation at version 0.8.25 or newer. The native feature is not a
license to substitute an older distribution package.

Create two durable directories outside a repository, temporary filesystem, or container overlay.
The account that starts the service owns the state directory. If file-replicator needs to read
captures, put both processes in one explicitly managed group; do not make captures world-readable.

```bash
sudo install -d -o edgecamera -g edgecamera -m 0700 \
  /var/lib/edgecommons/camera-adapter-state
sudo install -d -o edgecamera -g edgeimages -m 0750 \
  /var/lib/edgecommons/camera-adapter-output
```

The adapter creates database, WAL, shared-memory, lock, and state-temporary files with restrictive
state permissions. New image directories default to `0750`; images and JSON sidecars default to
`0640`. A more restrictive umask is respected. The service account needs write access to both
roots; a consumer such as file-replicator needs only read/traverse access to the output group.

Use an absolute configuration. This is a safe template: it will be offline until its placeholder
endpoint and profile are replaced, rather than silently controlling a discovered camera.

```json
{
  "health": { "enabled": true, "port": 8081 },
  "messaging": {
    "local": { "host": "127.0.0.1", "port": 1883, "clientId": "camera-adapter" }
  },
  "component": {
    "token": "camera-adapter",
    "global": {
      "output": {
        "rootDirectory": "/var/lib/edgecommons/camera-adapter-output",
        "minimumFreeBytes": 1073741824,
        "writeMetadataSidecar": true
      },
      "state": { "directory": "/var/lib/edgecommons/camera-adapter-state" }
    },
    "instances": [{
      "id": "camera-01",
      "backend": {
        "type": "onvif-rtsp",
        "deviceServiceUrl": "https://replace-with-camera.example/onvif/device_service",
        "mediaProfile": "replace-with-profile-token",
        "allowedUriHosts": ["replace-with-camera.example"]
      },
      "defaultCaptureProfile": "inspection",
      "captureProfiles": {
        "inspection": { "output": { "encoding": "png" } }
      }
    }]
  }
}
```

Start with the explicit platform/profile combination:

```bash
camera-adapter --platform HOST --transport MQTT /etc/edgecommons/camera-adapter.json \
  -c FILE /etc/edgecommons/camera-adapter.json
```

`/livez` reports that the process health server is running. `/readyz` and `/startupz` are `200`
only after messaging is connected and the adapter has finished its durable startup gates; they are
`503` during startup, shutdown, while the state directory is below its configured free-space floor
or cannot be read, or while the catalog/outbox cannot safely complete a durable pass. The output
and state roots use `minimumFreeBytes` and `minimumFreePercent`; either low/unreadable root raises
the deduplicated critical `storage-low` alarm with its configured root and observed free space.
New captures are rejected with `STORAGE_PRESSURE` until the affected root recovers. Output pressure
does not itself make the catalog unready; state-directory pressure does.
Temporary broker confirmation failures do not by themselves make the component unready: they remain
in the durable outbox and, when its pressure threshold is reached, raise the stateful warning alarm
`message-delivery-delayed`. The example exposes port 8081 only because it sets `health.enabled`
explicitly.

For GigE Vision, set a non-empty `component.global.discovery.eligibleInterfaces` list of exact
interface names. The adapter deliberately does not sweep all NICs. Size MTU, receive buffers,
device packet size, and packet delay for the camera VLAN; ordinary users do not need elevated
packet-capture capabilities.

## Windows HOST

The binding default for state is the absolute ProgramData known-folder path
`%ProgramData%\EdgeCommons\camera-adapter\state`; it is resolved by the OS, not trusted from an
environment-variable string. The output profile uses exclusive partial files, streams and flushes
the image checksum, installs an optional metadata sidecar before the image, and then uses
standard-library no-overwrite finalization. A detected collision or finalization failure is
terminal `PERSISTENCE_FAILED`; the adapter never overwrites a final image.
The output filesystem must support same-directory hard links; an unsupported hard-link
finalization is also `PERSISTENCE_FAILED`.

The Windows profile is not equivalent to Linux `openat2` containment: it does not claim hostile
local-actor containment. Deploy the service with ownership and ACLs
that prevent untrusted local principals from modifying the state or output roots. The adapter does
not set output DACLs itself. For example:

```json
{
  "component": {
    "token": "camera-adapter",
    "global": {
      "output": {
        "rootDirectory": "C:\\ProgramData\\EdgeCommons\\camera-adapter\\output",
        "minimumFreeBytes": 1073741824
      }
    },
    "instances": ["replace with one enabled, valid camera instance"]
  }
}
```

The `instances` placeholder above is intentionally not runnable; a real configuration must contain
at least one enabled, strictly valid camera instance. Do not place camera usernames or passwords in
the file. Reference a whole EdgeCommons credential secret such as
`{ "$secret": "cameras/loading-dock" }`; its UTF-8 JSON value must have exactly `username` and
`password` fields.

Before registering the service, create a restrictive DACL. Replace the service SID with the identity
actually used by the service. Do not grant `Users`, `Authenticated Users`, or broad share groups.

```powershell
$root = 'C:\ProgramData\EdgeCommons\camera-adapter'
New-Item -ItemType Directory -Force -Path "$root\state", "$root\output" | Out-Null
icacls $root /inheritance:r
icacls $root /grant:r `
  'SYSTEM:(OI)(CI)F' `
  'BUILTIN\Administrators:(OI)(CI)F' `
  'NT SERVICE\CameraAdapter:(OI)(CI)M'
```

Keep the ownership/ACL record with the deployment. Windows GenICam and native GStreamer packaging
are not claimed as release support, and Windows service plus physical-camera validation remains an
explicit unrun release gate.

## Docker

The checked-in [Dockerfile](../../Dockerfile) has two explicit targets:

- `onvif` (default): standalone ONVIF snapshot capture;
- `rtsp`: ONVIF plus the packaged GStreamer runtime for RTSP frame capture.

Build from the EdgeCommons umbrella because the adapter has a sibling path dependency on
`core/libs/rust`:

```bash
docker build -f camera-adapter/Dockerfile --target onvif -t camera-adapter:onvif .
docker build -f camera-adapter/Dockerfile --target rtsp -t camera-adapter:rtsp .
```

Both base images and Debian package snapshots are pinned. The current base pins are Linux/amd64;
publish a separately reviewed arm64 build before treating a container as arm64 support. Every
production container must explicitly mount writable durable state and output roots. Do not rely on
an image layer, anonymous volume, working directory, or `/tmp` for `state.directory`.

The simulator Compose integration is intentionally separate from production configuration:

```bash
docker compose -f camera-adapter/deploy/docker/compose.yaml up --build -d
curl --fail http://127.0.0.1:18081/readyz
```

It uses an isolated no-auth ONVIF fixture, a pinned EMQX image, an initialized named data volume,
and a loopback-only health port. It is an acceptance harness, not a camera or credential deployment
recipe. See [the simulator README](../../simulators/README.md) for protocol and native test commands.
