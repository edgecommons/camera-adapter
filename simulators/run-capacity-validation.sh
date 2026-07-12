#!/usr/bin/env bash
# Run the deliberately short Linux capacity proof against the real runtime and
# Core facade path. It is ignored because it creates 33 eight-megapixel images;
# the separate 24-hour soak remains explicitly deferred.
set -euo pipefail
umask 077

original_command=("$0" "$@")
artifact_dir=
target_dir=
soak_duration=

usage() {
    cat <<'EOF'
Usage: run-capacity-validation.sh --artifact-dir PATH [--target-dir PATH] [--soak-duration 15m]

  --artifact-dir PATH  Required new or empty directory for immutable evidence
  --target-dir PATH    Optional Cargo target directory; keeps build output out of the source tree
  --soak-duration 15m  After the short proof, run the bounded 15-minute simulator smoke

This is a Linux-only, short simulated capacity proof. It exercises 1,024
configured entries, 256 enabled SimBackend sessions, a 32-member 8MP capture
group, and a bounded overflow capture. `--soak-duration 15m` adds a partial
mixed-workload smoke; neither mode runs a 24-hour soak.
EOF
}

fail() {
    printf '%s\n' "$*" >&2
    exit 2
}

while (($#)); do
    case "$1" in
        --artifact-dir) artifact_dir=${2:?missing artifact directory}; shift 2 ;;
        --target-dir) target_dir=${2:?missing target directory}; shift 2 ;;
        --soak-duration) soak_duration=${2:?missing soak duration}; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ $(uname -s) == Linux ]] || fail 'short capacity validation requires a true Linux host such as lab-5950x'
[[ -n $artifact_dir ]] || fail '--artifact-dir is required so capacity evidence is never lost in a transient directory'
[[ -z $soak_duration || $soak_duration == 15m ]] || fail '--soak-duration currently accepts only 15m; the 24-hour soak is deferred'
command -v cargo >/dev/null || fail 'cargo is required'
command -v python3 >/dev/null || fail 'python3 is required to validate capacity evidence'
command -v sha256sum >/dev/null || fail 'sha256sum is required to attest capacity evidence'

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
adapter_root=$(cd -- "$script_dir/.." && pwd)

if [[ -e $artifact_dir || -L $artifact_dir ]]; then
    [[ -d $artifact_dir && ! -L $artifact_dir ]] || fail "artifact directory must be a real directory, not a file or symlink: $artifact_dir"
    [[ -z $(find "$artifact_dir" -mindepth 1 -maxdepth 1 -print -quit) ]] || fail "artifact directory must be new or empty: $artifact_dir"
else
    mkdir -p -- "$artifact_dir"
fi
artifact_root=$(cd -- "$artifact_dir" && pwd)

if git -C "$adapter_root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    source_revision=$(git -C "$adapter_root" rev-parse HEAD)
    if [[ -n $(git -C "$adapter_root" status --porcelain --untracked-files=all) ]]; then
        source_tree_state=dirty
    else
        source_tree_state=clean
    fi
    source_provenance=git-worktree
    source_bundle_sha256=
else
    source_revision=${CAMERA_ADAPTER_SOURCE_REVISION:-}
    [[ $source_revision =~ ^[[:xdigit:]]{40}([[:xdigit:]]{24})?$ ]] || fail 'staged source has no Git metadata; set CAMERA_ADAPTER_SOURCE_REVISION to the full 40- or 64-hex commit revision'
    source_revision=$(tr '[:upper:]' '[:lower:]' <<<"$source_revision")
    source_bundle_sha256=${CAMERA_ADAPTER_SOURCE_BUNDLE_SHA256:-}
    [[ $source_bundle_sha256 =~ ^[[:xdigit:]]{64}$ ]] || fail 'staged source has no Git metadata; set CAMERA_ADAPTER_SOURCE_BUNDLE_SHA256 to the exact uploaded tarball SHA-256'
    source_bundle_sha256=$(tr '[:upper:]' '[:lower:]' <<<"$source_bundle_sha256")
    source_tree_state=archive
    source_provenance=staged-archive
fi

printf -v invoked_command '%q ' "${original_command[@]}"
invoked_command=${invoked_command% }
started_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
cargo_version=$(cargo --version)
rustc_version=$(rustc --version)
kernel=$(uname -srmo)
manifest="$artifact_root/capacity-run-manifest.json"
python3 - "$manifest" "$started_at_utc" "$invoked_command" "$source_revision" "$source_tree_state" "$source_provenance" "$source_bundle_sha256" "$cargo_version" "$rustc_version" "$kernel" <<'PY'
import json
import os
import sys

(
    destination,
    started_at_utc,
    command,
    source_revision,
    source_tree_state,
    source_provenance,
    source_bundle_sha256,
    cargo_version,
    rustc_version,
    kernel,
) = sys.argv[1:]
manifest = {
    "schemaVersion": "camera-adapter-capacity-run-manifest/v1",
    "startedAtUtc": started_at_utc,
    "command": command,
    "source": {
        "revision": source_revision,
        "treeState": source_tree_state,
        "provenance": source_provenance,
        "bundleSha256": source_bundle_sha256 or None,
    },
    "toolchain": {"cargo": cargo_version, "rustc": rustc_version},
    "kernel": kernel,
    "expectedArtifacts": ["short-capacity-summary.json", "fifteen-minute-soak-summary.json"],
}
with open(destination, "x", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=2, sort_keys=True)
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
PY
chmod a-w -- "$manifest"
manifest_sha256=$(sha256sum -- "$manifest" | awk '{print $1}')

summary="$artifact_root/short-capacity-summary.json"
soak_summary="$artifact_root/fifteen-minute-soak-summary.json"
runtime_tmp="$artifact_root/runtime-tmp"
mkdir -p -- "$runtime_tmp"
export CAMERA_ADAPTER_CAPACITY_ARTIFACT_DIR="$artifact_root"
export TMPDIR="$runtime_tmp"
if [[ -n $target_dir ]]; then
    export CARGO_TARGET_DIR=$(mkdir -p -- "$target_dir" && cd -- "$target_dir" && pwd)
fi

validate_short_summary() {
    python3 - "$summary" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    value = json.load(handle)

def require(condition, message):
    if not condition:
        raise SystemExit(f"invalid short capacity artifact: {message}")

def integer(value, message):
    require(isinstance(value, int) and not isinstance(value, bool), message)
    return value

require(value.get("schemaVersion") == "camera-adapter-short-capacity/v1", "schemaVersion")
require(value.get("scope") == "ignored Linux short proof using the real Core facade and in-process SimBackend; not a 24-hour soak or hardware test", "scope")
require(value.get("configuredCameras") == 1024, "configuredCameras")
require(value.get("enabledSimulatedSessions") == 256, "enabledSimulatedSessions")
require(value.get("concurrentCaptureTarget") == 32, "concurrentCaptureTarget")
frame = value.get("frame")
require(isinstance(frame, dict), "frame")
require(frame.get("width") == 3264 and frame.get("height") == 2448, "frame dimensions")
require(frame.get("pixelFormat") == "Mono8" and frame.get("bytesPerFrame") == 7_990_272, "frame format")
memory = value.get("idleSessionMemory")
require(isinstance(memory, dict), "idleSessionMemory")
baseline = integer(memory.get("baselineRssBytes"), "idleSessionMemory.baselineRssBytes")
peak = integer(memory.get("startupPeakRssBytes"), "idleSessionMemory.startupPeakRssBytes")
roster = integer(memory.get("rosterOnlineRssBytes"), "idleSessionMemory.rosterOnlineRssBytes")
peak_delta = integer(memory.get("startupPeakDeltaBytes"), "idleSessionMemory.startupPeakDeltaBytes")
delta = integer(memory.get("rosterOnlineDeltaBytes"), "idleSessionMemory.rosterOnlineDeltaBytes")
full_frame_equivalent = integer(memory.get("fullFrameAllocationEquivalentBytes"), "idleSessionMemory.fullFrameAllocationEquivalentBytes")
maximum_delta = integer(memory.get("maximumAllowedDeltaBytes"), "idleSessionMemory.maximumAllowedDeltaBytes")
require(peak >= roster, "idleSessionMemory peak must cover roster RSS")
require(peak_delta == max(0, peak - baseline), "idleSessionMemory peak delta")
require(delta == max(0, roster - baseline), "idleSessionMemory delta")
require(full_frame_equivalent == frame["bytesPerFrame"] * 256, "idleSessionMemory full-frame equivalent")
require(maximum_delta == full_frame_equivalent // 8, "idleSessionMemory maximum delta")
require(peak_delta <= maximum_delta, "idleSessionMemory peak delta bound")
require(value.get("groupTerminalState") == "Succeeded", "groupTerminalState")
require(value.get("groupSuccessfulMembers") == 32, "groupSuccessfulMembers")
require(value.get("overflowCaptureTerminalState") == "Succeeded", "overflowCaptureTerminalState")
samples = value.get("resourceSamples")
require(isinstance(samples, list) and any(sample.get("phase") == "roster-online" for sample in samples if isinstance(sample, dict)), "resourceSamples roster-online")
latency = value.get("commandLatency")
require(isinstance(latency, dict), "commandLatency")
for verb in ("sb/list", "sb/status", "sb/ptz-stop"):
    summary = latency.get(verb)
    require(isinstance(summary, dict) and summary.get("samples") == 20, f"commandLatency.{verb}.samples")
    require(integer(summary.get("p95Micros"), f"commandLatency.{verb}.p95Micros") <= 250_000, f"commandLatency.{verb}.p95Micros")
PY
}

validate_soak_summary() {
    python3 - "$soak_summary" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    value = json.load(handle)

def require(condition, message):
    if not condition:
        raise SystemExit(f"invalid 15-minute capacity artifact: {message}")

def integer(value, message):
    require(isinstance(value, int) and not isinstance(value, bool), message)
    return value

require(value.get("schemaVersion") == "camera-adapter-capacity-smoke/v1", "schemaVersion")
require(value.get("scope") == "15-minute Linux SimBackend smoke; not a 24-hour soak or hardware test", "scope")
require(value.get("durationSeconds") == 900, "durationSeconds")
require(value.get("configuredCameras") == 1024, "configuredCameras")
require(value.get("enabledSimulatedSessions") == 256, "enabledSimulatedSessions")
require(value.get("scheduledCameras") == 8, "scheduledCameras")
require(integer(value.get("submittedCaptures"), "submittedCaptures") >= 400, "submittedCaptures")
require(integer(value.get("reconnects"), "reconnects") >= 14, "reconnects")
require(integer(value.get("reloads"), "reloads") >= 4, "reloads")
scheduled = value.get("scheduledJobsByCamera")
require(isinstance(scheduled, dict), "scheduledJobsByCamera")
for index in range(8):
    camera = f"camera-{index:04d}"
    require(integer(scheduled.get(camera), f"scheduledJobsByCamera.{camera}") >= 120, f"scheduledJobsByCamera.{camera}")
samples = value.get("resourceSamples")
require(isinstance(samples, list), "resourceSamples")
phases = {sample.get("phase") for sample in samples if isinstance(sample, dict)}
require({"soak-roster-online", "soak-complete"}.issubset(phases), "resourceSamples phases")
latency = value.get("commandLatency")
require(isinstance(latency, dict), "commandLatency")
for verb in ("sb/list", "sb/status", "sb/ptz-stop"):
    summary = latency.get(verb)
    require(isinstance(summary, dict) and integer(summary.get("samples"), f"commandLatency.{verb}.samples") > 0, f"commandLatency.{verb}.samples")
    integer(summary.get("p95Micros"), f"commandLatency.{verb}.p95Micros")
PY
}

attest_file() {
    local name=$1
    local artifact=$2
    local attestation="$artifact_root/${name}-artifact-attestation.json"
    local artifact_sha256
    artifact_sha256=$(sha256sum -- "$artifact" | awk '{print $1}')
    python3 - "$attestation" "$name" "$(basename -- "$artifact")" "$artifact_sha256" "$manifest_sha256" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" <<'PY'
import json
import os
import sys

destination, test_name, artifact_name, artifact_sha256, manifest_sha256, attested_at_utc = sys.argv[1:]
attestation = {
    "schemaVersion": "camera-adapter-capacity-artifact-attestation/v1",
    "test": test_name,
    "attestedAtUtc": attested_at_utc,
    "runManifest": {"file": "capacity-run-manifest.json", "sha256": manifest_sha256},
    "artifact": {"file": artifact_name, "sha256": artifact_sha256},
}
with open(destination, "x", encoding="utf-8") as handle:
    json.dump(attestation, handle, indent=2, sort_keys=True)
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
PY
    chmod a-w -- "$attestation"
    printf '%s artifact attestation: %s\n' "$name" "$attestation"
}

generate_capacity_report() {
    local report="$artifact_root/capacity-test-report.md"
    local short_attestation="$artifact_root/short-capacity-artifact-attestation.json"
    local soak_attestation="$artifact_root/fifteen-minute-soak-artifact-attestation.json"
    [[ ! -e $report ]] || fail "refusing to overwrite existing capacity report: $report"
    [[ -s $manifest && -s $summary && -s $soak_summary && -s $short_attestation && -s $soak_attestation ]] || fail 'capacity report requires the manifest, both validated summaries, and both attestations'
    python3 - "$report" "$manifest" "$summary" "$soak_summary" "$short_attestation" "$soak_attestation" <<'PY'
import hashlib
import json
import os
import sys

report_path, manifest_path, short_path, soak_path, short_attestation_path, soak_attestation_path = sys.argv[1:]

def load(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)

def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def require(condition, message):
    if not condition:
        raise SystemExit(f"cannot generate capacity report: {message}")

def cell(value):
    return str(value).replace("|", "\\|").replace("`", "\\`").replace("\n", "<br>")

def display(value):
    return "n/a" if value is None else cell(value)

def resource_value(sample, key, process_key=None):
    if process_key is not None:
        process = sample.get("process")
        return process.get(process_key) if isinstance(process, dict) else None
    return sample.get(key)

def resource_range(samples, key, process_key=None):
    values = [resource_value(sample, key, process_key) for sample in samples]
    values = [value for value in values if isinstance(value, int) and not isinstance(value, bool)]
    return (min(values), max(values)) if values else (None, None)

def resource_summary_rows(samples):
    metrics = (
        ("Online cameras", "onlineCameras", None),
        ("Live actors", "liveActorCount", None),
        ("Queued captures", "queuedCaptureDescriptors", None),
        ("Queued controls", "queuedControlDescriptors", None),
        ("Available global acquisitions", "availableGlobalAcquisitions", None),
        ("Available in-flight bytes", "availableInFlightBytes", None),
        ("Outstanding disk bytes", "outstandingDiskBytes", None),
        ("Available encoders", "availableEncoders", None),
        ("Available writers", "availableWriters", None),
        ("RSS bytes", "process", "rssBytes"),
        ("Threads", "process", "threadCount"),
        ("Open file descriptors", "process", "openFileDescriptors"),
    )
    return [(label, *resource_range(samples, key, process_key)) for label, key, process_key in metrics]

def resource_snapshot_rows(samples):
    rows = []
    for sample in samples:
        process = sample.get("process") if isinstance(sample.get("process"), dict) else {}
        resource_groups = sample.get("availableResourceGroupAcquisitions")
        resource_group_permits = resource_groups.get("sim-shared") if isinstance(resource_groups, dict) else None
        rows.append((
            sample.get("phase"),
            sample.get("onlineCameras"),
            sample.get("liveActorCount"),
            sample.get("queuedCaptureDescriptors"),
            sample.get("queuedControlDescriptors"),
            sample.get("availableGlobalAcquisitions"),
            resource_group_permits,
            sample.get("availableInFlightBytes"),
            sample.get("outstandingDiskBytes"),
            process.get("rssBytes"),
            process.get("threadCount"),
            process.get("openFileDescriptors"),
        ))
    return rows

def latency_rows(latency):
    return [
        (verb, latency[verb].get("samples"), latency[verb].get("p95Micros"), latency[verb].get("maximumMicros"))
        for verb in ("sb/list", "sb/status", "sb/ptz-stop")
    ]

manifest = load(manifest_path)
short = load(short_path)
soak = load(soak_path)
short_attestation = load(short_attestation_path)
soak_attestation = load(soak_attestation_path)
manifest_hash = sha256(manifest_path)

require(manifest.get("schemaVersion") == "camera-adapter-capacity-run-manifest/v1", "run manifest schema")
for attestation, expected_name, artifact_path in (
    (short_attestation, "short-capacity", short_path),
    (soak_attestation, "fifteen-minute-soak", soak_path),
):
    require(attestation.get("schemaVersion") == "camera-adapter-capacity-artifact-attestation/v1", f"{expected_name} attestation schema")
    require(attestation.get("test") == expected_name, f"{expected_name} attestation name")
    run_manifest = attestation.get("runManifest")
    artifact = attestation.get("artifact")
    require(isinstance(run_manifest, dict) and run_manifest.get("file") == "capacity-run-manifest.json" and run_manifest.get("sha256") == manifest_hash, f"{expected_name} manifest chain")
    require(isinstance(artifact, dict) and artifact.get("file") == os.path.basename(artifact_path) and artifact.get("sha256") == sha256(artifact_path), f"{expected_name} artifact hash")

source = manifest.get("source")
toolchain = manifest.get("toolchain")
require(isinstance(source, dict) and isinstance(toolchain, dict), "manifest provenance fields")
short_samples = short.get("resourceSamples")
soak_samples = soak.get("resourceSamples")
require(isinstance(short_samples, list) and isinstance(soak_samples, list) and short_samples and soak_samples, "resource samples")
short_latency = short.get("commandLatency")
soak_latency = soak.get("commandLatency")
require(isinstance(short_latency, dict) and isinstance(soak_latency, dict), "command latency")
require(all(verb in short_latency and verb in soak_latency for verb in ("sb/list", "sb/status", "sb/ptz-stop")), "required command latency verbs")

short_hash = sha256(short_path)
soak_hash = sha256(soak_path)
short_attestation_hash = sha256(short_attestation_path)
soak_attestation_hash = sha256(soak_attestation_path)
idle_memory = short.get("idleSessionMemory", {})
scheduled_jobs = soak.get("scheduledJobsByCamera", {})
short_exclusions = short.get("omittedFromThisShortRun", [])
soak_exclusions = soak.get("omittedFromThisSmoke", [])
exclusions = sorted({*short_exclusions, *soak_exclusions})

lines = [
    "# Camera-adapter capacity test report",
    "",
    "This report is generated only after both capacity JSON artifacts validate and their SHA-256 attestations chain to the immutable run manifest. It covers the short 8MP admission proof and the bounded 15-minute simulator smoke; it is not a 24-hour soak or a hardware result.",
    "",
    "## Provenance",
    "",
    "| Field | Value |",
    "|---|---|",
    f"| UTC start | {display(manifest.get('startedAtUtc'))} |",
    f"| Command | `{cell(manifest.get('command'))}` |",
    f"| Source revision | `{display(source.get('revision'))}` |",
    f"| Source provenance | {display(source.get('provenance'))} ({display(source.get('treeState'))}) |",
    f"| Source bundle SHA-256 | `{display(source.get('bundleSha256'))}` |",
    f"| Cargo | `{display(toolchain.get('cargo'))}` |",
    f"| Rustc | `{display(toolchain.get('rustc'))}` |",
    f"| Kernel | `{display(manifest.get('kernel'))}` |",
    "",
    "## Short 8MP admission proof",
    "",
    "| Metric | Result |",
    "|---|---:|",
    f"| Configured cameras | {display(short.get('configuredCameras'))} |",
    f"| Enabled simulated sessions | {display(short.get('enabledSimulatedSessions'))} |",
    f"| Concurrent capture target | {display(short.get('concurrentCaptureTarget'))} |",
    f"| Frame | {display(short.get('frame', {}).get('width'))}×{display(short.get('frame', {}).get('height'))} {display(short.get('frame', {}).get('pixelFormat'))}; {display(short.get('frame', {}).get('bytesPerFrame'))} bytes |",
    f"| Group terminal state / successful members | {display(short.get('groupTerminalState'))} / {display(short.get('groupSuccessfulMembers'))} |",
    f"| Overflow capture terminal state | {display(short.get('overflowCaptureTerminalState'))} |",
    f"| RSS before runtime | {display(idle_memory.get('baselineRssBytes'))} bytes |",
    f"| Peak RSS during 256-session startup | {display(idle_memory.get('startupPeakRssBytes'))} bytes |",
    f"| RSS at 256-online roster | {display(idle_memory.get('rosterOnlineRssBytes'))} bytes |",
    f"| Peak idle-session startup RSS delta | {display(idle_memory.get('startupPeakDeltaBytes'))} bytes |",
    f"| Idle-session roster RSS delta | {display(idle_memory.get('rosterOnlineDeltaBytes'))} bytes |",
    f"| Allowed idle-session delta | {display(idle_memory.get('maximumAllowedDeltaBytes'))} bytes (one eighth of {display(idle_memory.get('fullFrameAllocationEquivalentBytes'))}) |",
    "",
    "### Router-boundary latency while acquisition was saturated",
    "",
    "| Command | Samples | p95 (µs) | Maximum (µs) |",
    "|---|---:|---:|---:|",
]
lines.extend(f"| {cell(verb)} | {display(samples)} | {display(p95)} | {display(maximum)} |" for verb, samples, p95, maximum in latency_rows(short_latency))
lines.extend([
    "",
    "### Short-proof resource samples",
    "",
    "| Phase | Online | Actors | Queued captures | Queued controls | Global permits | sim-shared permits | Available in-flight bytes | Outstanding disk bytes | RSS bytes | Threads | FDs |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
])
for row in resource_snapshot_rows(short_samples):
    lines.append("| " + " | ".join(display(value) for value in row) + " |")

lines.extend([
    "",
    "## Bounded 15-minute simulator smoke",
    "",
    "| Metric | Result |",
    "|---|---:|",
    f"| Duration | {display(soak.get('durationSeconds'))} seconds |",
    f"| Configured cameras / enabled sessions | {display(soak.get('configuredCameras'))} / {display(soak.get('enabledSimulatedSessions'))} |",
    f"| Scheduled cameras | {display(soak.get('scheduledCameras'))} |",
    f"| Submitted direct captures | {display(soak.get('submittedCaptures'))} |",
    f"| Reconnects | {display(soak.get('reconnects'))} |",
    f"| Valid reload applications | {display(soak.get('reloads'))} |",
    "",
    "### Accepted scheduled jobs per camera",
    "",
    "| Camera | Accepted scheduled jobs |",
    "|---|---:|",
])
for camera in sorted(scheduled_jobs):
    lines.append(f"| {cell(camera)} | {display(scheduled_jobs[camera])} |")
lines.extend([
    "",
    "### Router-boundary latency during the smoke",
    "",
    "| Command | Samples | p95 (µs) | Maximum (µs) |",
    "|---|---:|---:|---:|",
])
lines.extend(f"| {cell(verb)} | {display(samples)} | {display(p95)} | {display(maximum)} |" for verb, samples, p95, maximum in latency_rows(soak_latency))
lines.extend([
    "",
    "### 15-minute resource summary",
    "",
    f"{len(soak_samples)} samples were recorded. The table reports the observed minimum and maximum for each field; the JSON artifact retains every sample.",
    "",
    "| Metric | Minimum | Maximum |",
    "|---|---:|---:|",
])
lines.extend(f"| {cell(label)} | {display(minimum)} | {display(maximum)} |" for label, minimum, maximum in resource_summary_rows(soak_samples))
lines.extend([
    "",
    "### 15-minute boundary resource snapshots",
    "",
    "| Phase | Online | Actors | Queued captures | Queued controls | Global permits | sim-shared permits | Available in-flight bytes | Outstanding disk bytes | RSS bytes | Threads | FDs |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
])
for row in resource_snapshot_rows([soak_samples[0], soak_samples[-1]]):
    lines.append("| " + " | ".join(display(value) for value in row) + " |")

lines.extend([
    "",
    "## Artifact hashes and manifest chain",
    "",
    "| File | SHA-256 |",
    "|---|---|",
    f"| `capacity-run-manifest.json` | `{manifest_hash}` |",
    f"| `short-capacity-summary.json` | `{short_hash}` |",
    f"| `short-capacity-artifact-attestation.json` | `{short_attestation_hash}` |",
    f"| `fifteen-minute-soak-summary.json` | `{soak_hash}` |",
    f"| `fifteen-minute-soak-artifact-attestation.json` | `{soak_attestation_hash}` |",
    "",
    "## Explicit exclusions",
    "",
    "This report does **not** establish a 24-hour soak, 10,000-job workload, broker-outage recovery, encoder/writer saturation graph, Core ping benchmark, physical-camera compatibility, GenICam/L2 behavior, or hardware certification.",
    "",
])
lines.extend(f"- {cell(exclusion)}" for exclusion in exclusions)
lines.append("")

with open(report_path, "x", encoding="utf-8") as handle:
    handle.write("\n".join(lines))
    handle.flush()
    os.fsync(handle.fileno())
PY
    chmod a-w -- "$report"
    printf 'Capacity test report: %s\n' "$report"
}

cd -- "$adapter_root"
cargo test --locked --no-default-features --features standalone,onvif,capacity-harness --lib \
    runtime::tests::simulator_runtime::short_linux_capacity_proves_1024_configured_256_sessions_and_32_captures \
    -- --ignored --exact --test-threads 1

[[ -s $summary ]] || fail "short capacity test completed without the required evidence artifact: $summary"
validate_short_summary
attest_file short-capacity "$summary"
printf 'Short capacity artifact: %s\n' "$summary"
if [[ -z $soak_duration ]]; then
    printf '%s\n' 'Scope: simulated short proof only; 24-hour soak execution remains deferred.'
    exit 0
fi

CAMERA_ADAPTER_CAPACITY_SOAK_DURATION_SECS=900 \
    cargo test --locked --no-default-features --features standalone,onvif,capacity-harness --lib \
    runtime::tests::simulator_runtime::fifteen_minute_linux_capacity_smoke_exercises_mixed_runtime_traffic \
    -- --ignored --exact --test-threads 1
[[ -s $soak_summary ]] || fail "15-minute smoke completed without the required evidence artifact: $soak_summary"
validate_soak_summary
attest_file fifteen-minute-soak "$soak_summary"
generate_capacity_report
attest_file capacity-test-report "$artifact_root/capacity-test-report.md"
printf '15-minute partial-smoke artifact: %s\n' "$soak_summary"
printf '%s\n' 'Scope: partial simulator smoke only; the 24-hour soak execution remains deferred.'
