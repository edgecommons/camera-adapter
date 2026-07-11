# Camera adapter documentation

These documents describe the checked-in deployment artifacts and the currently implemented
runtime boundaries. `DESIGN.md` is the binding design and contains future release gates; it is not
a statement that every planned command or hardware path is already released.

| Need | Document |
|---|---|
| Deploy on Linux or Windows HOST | [HOST runbook](deployment/host.md) |
| Deploy through Greengrass IPC | [Greengrass runbook](deployment/greengrass.md) |
| Deploy a single active pod with a PVC | [Kubernetes runbook](deployment/kubernetes.md) |
| Understand simulator and hardware limits | [Compatibility register](reference/compatibility.md) |

The deterministic simulator commands are maintained in [../simulators/README.md](../simulators/README.md).
