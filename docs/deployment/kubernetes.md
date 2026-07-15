# Kubernetes deployment

The checked-in manifests under [`k8s/`](../../k8s/) model one active adapter pod and one
`ReadWriteOnce` PVC. They are a deployment template, not a claim that a cluster camera NIC, USB
device, broker, image registry, or physical camera has been validated.

## Apply the baseline

1. Build and publish a reviewed image with an immutable digest. The `image:` placeholder is
   intentionally unusable until replaced.
2. Label the specific camera-connected node and replace the `nodeSelector` value with the
   cluster's authoritative label.
3. Replace the camera endpoint/profile placeholders in `k8s/configmap.yaml`. Add credential
   configuration only through the EdgeCommons vault; never add a password to the ConfigMap.
4. Review PVC class, capacity, and access mode for the measured image volume and retention policy.
5. Apply the complete directory, with `configmap.yaml` before `deployment.yaml`.

```bash
kubectl apply -f camera-adapter/k8s/configmap.yaml
kubectl apply -f camera-adapter/k8s/deployment.yaml
```

The config volume is mounted at `/etc/edgecommons` as a directory, never with `subPath`, so the
`CONFIGMAP` source can receive the kubelet's atomic update. The adapter uses separate `/state` and
`/output` subdirectories under the same RWO PVC class:

```text
/var/lib/edgecommons/camera-adapter/state
/var/lib/edgecommons/camera-adapter/output
```

Both are explicit absolute paths. An unmounted or ephemeral path is a deployment error, not a
fallback condition. The pod runs non-root with `fsGroup` matching its user; validate the selected
storage driver's ownership behavior before deployment. Never share the catalog PVC between active
replicas.

## Networking and devices

The baseline uses ONVIF/RTSP configuration and normal pod networking. For GigE Vision, ordinary
pod NAT is not adequate proof of multicast discovery or camera UDP reception. Choose one of
`hostNetwork`, Multus, or a dedicated secondary interface, then set
`component.global.discovery.eligibleInterfaces` to the exact interface names visible *inside the
pod*. Keep the list empty when discovery is disabled; the adapter never falls back to all
interfaces.

USB3 Vision requires a device plugin or explicit, least-privilege device mapping with stable
permissions. Neither mapping is included in the generic manifest because it is node- and vendor-
specific and must be reviewed for each deployment.

## Probes and shutdown

The manifest explicitly enables the EdgeCommons health server on port 8081:

- `/livez` is process liveness only;
- `/readyz` and `/startupz` require messaging connectivity and completed adapter startup;
- readiness turns non-OK when shutdown begins.

`terminationGracePeriodSeconds: 60` exceeds the default 30-second adapter shutdown grace. If the
configuration increases `component.global.timeouts.shutdownGraceMs`, increase Kubernetes grace as
well. A rollout uses `Recreate` because an overlapping pod would violate the state-directory
single-writer contract.

## Credentials and telemetry

The baseline ConfigMap deliberately has no credential reference. For an authenticated ONVIF camera,
configure the core `credentials` service with a suitable cluster key provider and reference a
whole JSON secret from the camera backend. Keep the KEK and vault material in a Kubernetes Secret
or external secret provider, not in ConfigMap data.

The checked-in image does not claim Prometheus support. Add an EdgeCommons Prometheus feature,
Service, and ServiceMonitor only as one coordinated, tested packaging change; do not create a
scrape target for a port the image has not compiled.

Physical cameras and hardware-cluster camera-network access are not supported.
