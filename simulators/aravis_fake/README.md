# Native fake-Aravis validation

`run-genicam-native-coverage.sh` validates the production GenICam path against
the pinned Aravis 0.8.36 fake GigE Vision camera. Run it only from a true Linux
host or Linux runner that has the camera-facing L2 interface. Windows Docker
Desktop is rejected even when it runs Linux containers: its host-network mode
is not the required shared L2 namespace. Bridge or NAT networking is not GigE
Vision discovery evidence.

Use the checked-in Bash runner on the Linux host; it needs no PowerShell
installation:

```bash
bash ./simulators/run-genicam-native-coverage.sh \
  --interface eth0 \
  --coverage-output /tmp/camera-adapter-genicam-coverage
```

`run-genicam-native-coverage.ps1` remains only as a legacy external-L2
diagnostic for PowerShell on Linux. It rejects Windows hosts, including Windows
PowerShell 5.1, and is not the supported validation path.

The runner builds the fake camera and validation image, mounts the whole
EdgeCommons workspace read-only, keeps Cargo state in named volumes, and writes
`genicam-fake-gv-mono8.lcov` to `CoverageOutput`. It exercises the real helper
process, an explicit `Aravis-Fake-GV01` selector, a software-triggered 320x240
Mono8 capture twice on the same session, complete 76,800-byte payload checks,
and orderly close.

This focused fixture intentionally uses a 64 MiB temporary filesystem because
it does not create a TempDir-backed output root. A full
`standalone,genicam` library suite must instead mount `/tmp` at 2 GiB or more:
the ordinary test configuration correctly retains the production default of
1 GiB free space, so a smaller TempDir turns every capture into a deliberate
`StoragePressure` rejection. Do not lower that product default to accommodate
a test container.

To measure the relevant aggregate library coverage, append
`--aggregate-coverage`. The runner first executes the ordinary
`standalone,onvif,genicam` library suite serially with a 2 GiB temporary
filesystem, then merges the instrumented helper and same-container fixture
profiles into `standalone-onvif-genicam-fake-gv.lcov`. It also writes a JSON
line-coverage summary beside that artifact. This remains same-container
protocol/buffer evidence, not an L2 or physical-camera claim.

The runner checks the spawned helper's JSON discovery result before it invokes
the lib test; that test independently performs production discovery before its
captures. A helper's separately spawned process does not automatically
contribute to a lib test's LCOV profile, so helper-process coverage is claimed
only from the merged aggregate profile.

`--network-container <fake-container>` is available only to diagnose a
same-network-namespace simulator topology. Its result, if any, is native
Aravis protocol/buffer evidence only; it does not validate cross-container or
cross-host GigE packet delivery.

If the fake camera's upstream Aravis client can capture only from the same
container (a simulator topology limitation), stop the Compose fake camera and
run the dedicated same-container mode instead:

```bash
docker compose -f simulators/compose.yaml --profile linux-l2 stop aravis-fake
bash ./simulators/run-genicam-native-coverage.sh \
  --interface enp7s0 \
  --coverage-output /tmp/camera-adapter-genicam-coverage \
  --skip-build \
  --in-container-fake
```

That mode starts a fresh fake camera beside each helper/test command in the
validation container. It validates the native adapter and fake-camera buffer
contract only; it is not cross-container or cross-host GigE evidence.
The runner refuses to use that mode while the host-network Compose
`aravis-fake` service is running, avoiding an ambiguous interface bind race.

The result is fixture-scoped protocol and buffer evidence. It is not an
adapter-wide coverage measurement, does not satisfy the 90% adapter coverage
gate by itself, and does not make any physical camera, firmware, encoder, NIC,
or device-timing compatibility claim.
