# Camera adapter documentation

These documents describe the checked-in deployment artifacts and the currently implemented
runtime boundaries. `DESIGN.md` is the binding design and contains future release gates; it is not
a statement that every planned command or hardware path is already released.

| Need | Document |
|---|---|
| First simulated capture | [Tutorial](tutorial.md) |
| Common operating tasks | [How-to guides](how-to-guides.md) |
| Copyable configurations | [Sample configurations](sample-configurations.md) |
| Understand lifecycle and backpressure | [Explanation](explanation.md) |
| Configure the component | [Configuration reference](reference/configuration.md) |
| Send commands and consume terminal results | [Messaging reference](reference/messaging-interface.md) |
| Monitor health and alarms | [Metrics reference](reference/metrics.md) |
| Deploy on Linux or Windows HOST | [HOST runbook](deployment/host.md) |
| Deploy through Greengrass IPC | [Greengrass runbook](deployment/greengrass.md) |
| Deploy a single active pod with a PVC | [Kubernetes runbook](deployment/kubernetes.md) |
| Understand simulator and hardware limits | [Compatibility register](reference/compatibility.md) |
| Inspect requirement and release evidence | [Acceptance matrix](acceptance-matrix.md) |

The deterministic simulator commands are maintained in [../simulators/README.md](../simulators/README.md).
