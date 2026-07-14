# Greengrass deployment

The adapter uses Greengrass IPC when started with `--platform GREENGRASS -c GG_CONFIG`. The
checked-in [recipe](../../recipe.yaml) is a package template: replace its artifact URI and apply a
complete component configuration containing at least one enabled, valid camera before starting it.
Its empty instance list intentionally fails the adapter's configuration gate rather than discovering
or controlling cameras by default.

## Package and artifact

Build the architecture-matched Linux binary with the Greengrass transport feature. Build and test
this feature on Linux or WSL because the Greengrass SDK is Linux-only.

```bash
cargo +1.90.0 build --release --no-default-features --features greengrass,onvif
```

For RTSP, add `rtsp` and package the matching GStreamer runtime libraries. For GenICam, package a
reviewed native Aravis installation at 0.8.36 or newer with the binary; do not install arbitrary
native packages at component start. The recipe does not claim a prebuilt GenICam or RTSP artifact.

## Durable paths and ownership

Greengrass has **no implicit state-directory default**. Set both paths to durable host storage in
the component configuration, never to the Nucleus component work directory:

```json
{
  "component": {
    "token": "camera-adapter",
    "global": {
      "output": {
        "rootDirectory": "/var/lib/edgecommons/camera-adapter/output",
        "minimumFreeBytes": 1073741824,
        "writeMetadataSidecar": true
      },
      "state": {
        "directory": "/var/lib/edgecommons/camera-adapter/state"
      }
    },
    "instances": ["supply one enabled, valid camera instance"]
  }
}
```

Before deployment, the device owner creates the roots and gives only the Greengrass run-as identity
write access. The state root is `0700`; capture roots are `0750` and final captures/sidecars are
`0640` by default. If file-replicator consumes captures, use a controlled shared group for the
output tree only. State must remain inaccessible to other component accounts.

## IPC least privilege

The recipe grants local IPC publishing under the component's `ecv1/*/camera-adapter/*` namespace
and command subscription only under `ecv1/*/camera-adapter/main/cmd/*`. It does not grant MQTT proxy,
shadow, broad file, or cloud permissions. Add a separate, narrow dependency and policy only when a
concrete deployment needs one; for example, a vault backed by a cloud provider needs its own
reviewed credential integration.

Greengrass validation is still a release gate. A recipe that parses or installs is not evidence that
command replies, terminal application messages, native capture, or camera VLAN access work through
real IPC. Record that result against the `lab-5950x` gate before claiming Greengrass support.
