# Native fake-Aravis validation

`run-genicam-native-coverage.ps1` validates the production GenICam path against
the pinned Aravis 0.8.36 fake GigE Vision camera. Run it only on a Linux-backed
Docker engine that supports host networking; bridge or NAT networking is not
GigE Vision discovery evidence.

```powershell
./simulators/run-genicam-native-coverage.ps1 `
  -Interface eth0 `
  -CoverageOutput C:\tmp\camera-adapter-genicam-coverage
```

The runner builds the fake camera and validation image, mounts the whole
EdgeCommons workspace read-only, keeps Cargo state in named volumes, and writes
`genicam-fake-gv-mono8.lcov` to `CoverageOutput`. It exercises the real helper
process, an explicit `Aravis-Fake-GV01` selector, a software-triggered 320x240
Mono8 capture twice on the same session, complete 76,800-byte payload checks,
and orderly close.

The result is fixture-scoped protocol and buffer evidence. It is not an
adapter-wide coverage measurement, does not satisfy the 90% adapter coverage
gate by itself, and does not make any physical camera, firmware, encoder, NIC,
or device-timing compatibility claim.
