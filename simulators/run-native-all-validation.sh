#!/usr/bin/env bash
# Exercise the combined Aravis + GStreamer feature set. The ordinary library
# suite runs in a network-none container. When coverage is requested, its
# profile is extended by separately scoped MediaMTX and same-container Aravis
# fixtures; those fixtures never weaken the deterministic library-test
# boundary or become L2 / physical-camera evidence.
set -euo pipefail

image=camera-adapter-native-all-validation
coverage_output=
interface=eth0
skip_build=false
skip_simulator_start=false

usage() {
    cat <<'EOF'
Usage: run-native-all-validation.sh [options]

  --image NAME                 Combined validation image tag
  --coverage-output PATH       Write native-all-summary.json and enforce >=90% line coverage
  --interface NAME             Linux interface for the same-container fake camera (default: eth0)
  --skip-build                 Reuse validation images already built locally
  --skip-simulator-start       Reuse an already-running MediaMTX Compose service
EOF
}

while (($#)); do
    case "$1" in
        --image) image=${2:?missing image}; shift 2 ;;
        --coverage-output) coverage_output=${2:?missing coverage output}; shift 2 ;;
        --interface) interface=${2:?missing interface}; shift 2 ;;
        --skip-build) skip_build=true; shift ;;
        --skip-simulator-start) skip_simulator_start=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ $(uname -s) == Linux ]] || { printf 'combined native validation requires Linux\n' >&2; exit 2; }
[[ -n $interface && $interface != *$'\n'* ]] || {
    printf 'interface must be a non-empty Linux interface name\n' >&2
    exit 2
}
command -v docker >/dev/null || { printf 'docker is required\n' >&2; exit 127; }

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
adapter_root=$(cd -- "$script_dir/.." && pwd)
workspace_root=$(cd -- "$adapter_root/.." && pwd)
compose_file="$script_dir/compose.yaml"

if [[ $skip_build != true ]]; then
    # Build the fake runtime explicitly before deriving either validation layer.
    # BuildKit cannot consume a bare local sha256 image ID in FROM, so tag the
    # freshly built local image with that ID. The derived reference is never
    # `latest` and requires no registry lookup.
    docker compose -f "$compose_file" --profile linux-l2 build aravis-fake
    fake_image_id=$(docker image inspect --format '{{.Id}}' camera-adapter-simulators-aravis-fake)
    fake_image_ref="camera-adapter-aravis-validation-input:${fake_image_id#sha256:}"
    docker tag camera-adapter-simulators-aravis-fake "$fake_image_ref"
    docker build -f "$script_dir/aravis_fake/AdapterValidation.Dockerfile" \
        --build-arg "ARAVIS_RUNTIME_IMAGE=$fake_image_ref" \
        -t camera-adapter-aravis-validation "$script_dir/aravis_fake"
    validation_image_id=$(docker image inspect --format '{{.Id}}' camera-adapter-aravis-validation)
    validation_image_ref="camera-adapter-native-all-validation-input:${validation_image_id#sha256:}"
    docker tag camera-adapter-aravis-validation "$validation_image_ref"
    docker build -f "$script_dir/native_all_validation.Dockerfile" \
        --build-arg "ARAVIS_VALIDATION_IMAGE=$validation_image_ref" \
        -t "$image" "$adapter_root"
fi

target_volume=camera-adapter-native-all-target
registry_volume=camera-adapter-native-all-registry
git_volume=camera-adapter-native-all-git
for volume in "$target_volume" "$registry_volume" "$git_volume"; do
    docker volume create "$volume" >/dev/null
done

source_mount="$workspace_root:/edgecommons:ro"
target_mount="$target_volume:/coverage-target"
registry_mount="$registry_volume:/usr/local/cargo/registry"
git_mount="$git_volume:/usr/local/cargo/git"

network_none_run=(
    docker run --rm --network none --read-only --tmpfs /tmp:size=2g,mode=1777
    --cap-drop ALL --security-opt no-new-privileges:true
    -v "$source_mount"
    -v "$target_mount"
    -v "$registry_mount"
    -v "$git_mount"
    -w /edgecommons/camera-adapter
    -e CARGO_TARGET_DIR=/coverage-target
    -e TMPDIR=/tmp
)

# Populate only named Cargo cache volumes before the network-none library test.
# The committed lockfile is still authoritative; network access never reaches
# the deterministic test container itself.
prefetch_run=(
    docker run --rm --network bridge --read-only --tmpfs /tmp:size=64m,mode=1777
    --cap-drop ALL --security-opt no-new-privileges:true
    -v "$source_mount"
    -v "$target_mount"
    -v "$registry_mount"
    -v "$git_mount"
    -w /edgecommons/camera-adapter
    -e CARGO_TARGET_DIR=/coverage-target
    -e TMPDIR=/tmp
)
"${prefetch_run[@]}" "$image" +1.87.0 fetch --locked

if [[ -z $coverage_output ]]; then
    "${network_none_run[@]}" "$image" +1.87.0 test --locked --offline --no-default-features \
        --features standalone,native-all --lib -- --test-threads 1
    printf '%s\n' 'Scope: deterministic serial combined-feature library compatibility only; no network, L2, or physical-camera claim.'
    exit 0
fi

coverage_root=$(mkdir -p -- "$coverage_output" && cd -- "$coverage_output" && pwd)
summary_in_volume=/coverage-target/native-all-summary.json
summary_on_host="$coverage_root/native-all-summary.json"

# The same-container fixture must be the only fake camera that can bind the
# selected interface. A host-network Compose fake would make the result
# topology-ambiguous, so fail before any profile data can be mistaken as ours.
running_fake=$(docker compose -f "$compose_file" --profile linux-l2 \
    ps --status running --services aravis-fake)
if [[ -n $running_fake ]]; then
    printf '%s\n' 'stop the Compose aravis-fake service before native-all fixture coverage' >&2
    exit 2
fi

# Establish the baseline in a fully isolated container. The following fixture
# commands deliberately reuse this profile with --no-clean, but run elsewhere.
"${network_none_run[@]}" "$image" +1.87.0 llvm-cov clean --workspace
"${network_none_run[@]}" "$image" +1.87.0 llvm-cov test --locked --offline --no-default-features \
    --features standalone,native-all --lib -- --test-threads 1

# Live RTSP fixtures are intentionally on the Compose network: the URI must
# resolve the pinned service name instead of using a host-network shortcut.
if [[ $skip_simulator_start != true ]]; then
    docker compose -f "$compose_file" up -d --wait mediamtx
fi
mediamtx_container=$(docker compose -f "$compose_file" ps -q mediamtx)
[[ -n $mediamtx_container ]] || {
    printf 'MediaMTX is not running; omit --skip-simulator-start or start the Compose service first\n' >&2
    exit 1
}
mapfile -t mediamtx_networks < <(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$mediamtx_container")
[[ ${#mediamtx_networks[@]} -eq 1 ]] || {
    printf 'expected exactly one Compose network for MediaMTX, found %d\n' "${#mediamtx_networks[@]}" >&2
    exit 1
}
rtsp_network=${mediamtx_networks[0]}

run_rtsp_fixture() {
    local path=$1
    local test_filter=$2
    printf 'Running MediaMTX fixture %s (%s)\n' "$path" "$test_filter"
    docker run --rm --network "$rtsp_network" --read-only --tmpfs /tmp:size=64m,mode=1777 \
        --cap-drop ALL --security-opt no-new-privileges:true \
        -v "$source_mount" \
        -v "$target_mount" \
        -v "$registry_mount" \
        -v "$git_mount" \
        -w /edgecommons/camera-adapter \
        -e CARGO_TARGET_DIR=/coverage-target \
        -e "CAMERA_ADAPTER_RTSP_URI=rtsp://mediamtx:8554/$path" \
        "$image" +1.87.0 llvm-cov test --locked --offline --no-clean --no-default-features \
        --features standalone,native-all --lib "$test_filter" \
        -- --ignored --exact --test-threads 1
}

for path in camera camera-h265; do
    run_rtsp_fixture "$path" 'backend::rtsp::tests::pinned_mediamtx_produces_a_complete_rgb_frame'
    run_rtsp_fixture "$path" 'backend::rtsp::tests::pinned_mediamtx_warm_session_produces_two_complete_frames'
done

# The Aravis fake process and client share one hardened host-network container.
# This topology proves native protocol/buffer handling only. It does not turn a
# cross-container failure into an L2 or external-camera success claim.
run_genicam_fixture() {
    docker run --rm --network host --read-only --tmpfs /tmp:size=64m,mode=1777 \
        --cap-drop ALL --security-opt no-new-privileges:true \
        -v "$source_mount" \
        -v "$target_mount" \
        -v "$registry_mount" \
        -v "$git_mount" \
        -w /edgecommons/camera-adapter \
        -e CARGO_TARGET_DIR=/coverage-target \
        -e "CAMERA_ADAPTER_ARAVIS_INTERFACE=$interface" \
        --entrypoint /bin/sh "$image" -c '
            set -eu
            interface=$1
            shift
            /opt/aravis/bin/arv-fake-gv-camera-0.8 --interface="$interface" &
            fake_pid=$!
            trap "kill $fake_pid 2>/dev/null || true; wait $fake_pid 2>/dev/null || true" EXIT
            ready=false
            for attempt in $(seq 1 30); do
                discovery=$(/opt/aravis/bin/arv-tool-0.8 --gv-discovery-interface="$interface" 2>/dev/null || true)
                if printf "%s" "$discovery" | grep -Fq "Aravis-Fake-GV01"; then
                    ready=true
                    break
                fi
                sleep 1
            done
            if [ "$ready" != true ]; then
                echo "in-container fake camera was not discoverable" >&2
                exit 1
            fi
            cargo "$@"
        ' -- "$interface" "$@"
}

printf 'Running same-container Aravis helper\n'
helper_output=$(run_genicam_fixture +1.87.0 llvm-cov run --locked --offline --no-clean \
    --no-default-features --features standalone,native-all \
    --bin camera-adapter-genicam-discover -- \
    --interface "$interface" --transport gige-vision --max-results 1)
printf '%s\n' "$helper_output"
if ! grep -Fq '"deviceId":"Aravis-Fake-GV01"' <<<"$helper_output"; then
    printf 'instrumented GenICam helper did not discover Aravis-Fake-GV01\n' >&2
    exit 1
fi

printf 'Running same-container Aravis two-frame capture fixture\n'
run_genicam_fixture +1.87.0 llvm-cov test --locked --offline --no-clean \
    --no-default-features --features standalone,native-all --lib \
    backend::genicam_aravis::tests::pinned_aravis_fake_discovers_and_captures_two_complete_mono8_frames \
    -- --ignored --exact --test-threads 1

# Export before enforcing the threshold, so a failed gate still leaves a
# re-readable named-volume artifact for diagnosis. The final report is the
# combined profile: network-none ordinary tests + four RTSP fixtures + real
# Aravis discovery helper + two-frame Aravis fixture.
"${network_none_run[@]}" "$image" +1.87.0 llvm-cov report --locked --offline --json --summary-only \
    --output-path "$summary_in_volume"
docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges:true \
    -v "$target_volume:/coverage-target:ro" --entrypoint /bin/cat "$image" "$summary_in_volume" \
    > "$summary_on_host"
[[ -s $summary_on_host ]] || { printf 'native-all coverage summary was not produced\n' >&2; exit 1; }

printf 'Combined native-all coverage summary: %s\n' "$summary_on_host"
"${network_none_run[@]}" "$image" +1.87.0 llvm-cov report --locked --offline --fail-under-lines 90
printf '%s\n' 'Coverage gate scope: network-none serial library tests plus separately scoped MediaMTX and same-container Aravis fixtures.'
printf '%s\n' 'Fixture limits: no cross-container/cross-host GigE, L2, external-camera, or physical-camera compatibility claim.'
