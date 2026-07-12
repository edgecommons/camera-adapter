#!/usr/bin/env bash
# Run the production GenICam path against the pinned fake Aravis GigE camera.
# External mode requires a real Linux/L2 namespace; --in-container-fake is
# deliberately narrower native protocol/buffer evidence, never an L2 claim.
set -euo pipefail

interface=eth0
coverage_output="${TMPDIR:-/tmp}/camera-adapter-genicam-coverage"
image=camera-adapter-aravis-validation
skip_build=false
skip_simulator_start=false
network_mode=host
network_container=
in_container_fake=false
aggregate_coverage=false

usage() {
    cat <<'EOF'
Usage: run-genicam-native-coverage.sh [options]

  --interface NAME          Linux camera-facing interface (default: eth0)
  --coverage-output PATH    Directory for generated LCOV and JSON artifacts
  --image NAME              Validation image tag
  --skip-build              Reuse the validation image
  --skip-simulator-start    Reuse the running fake camera
  --network-container NAME  Join a running fake camera's network namespace
  --in-container-fake       Start the fake camera beside each validation command
  --aggregate-coverage      Merge serial standalone,onvif,genicam library coverage with the fixture
EOF
}

while (($#)); do
    case "$1" in
        --interface) interface=${2:?missing interface}; shift 2 ;;
        --coverage-output) coverage_output=${2:?missing coverage output}; shift 2 ;;
        --image) image=${2:?missing image}; shift 2 ;;
        --skip-build) skip_build=true; shift ;;
        --skip-simulator-start) skip_simulator_start=true; shift ;;
        --network-container)
            network_container=${2:?missing container name}
            network_mode="container:$network_container"
            shift 2
            ;;
        --in-container-fake) in_container_fake=true; shift ;;
        --aggregate-coverage) aggregate_coverage=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ $(uname -s) != Linux ]]; then
    printf 'native fake-Aravis coverage requires a true Linux host/L2 namespace\n' >&2
    exit 2
fi
if [[ -z $interface || $interface == *$'\n'* ]]; then
    printf 'interface must be a non-empty Linux interface name\n' >&2
    exit 2
fi
command -v docker >/dev/null || { printf 'docker is required\n' >&2; exit 127; }
if [[ $in_container_fake == true && $network_mode != host ]]; then
    printf '%s\n' '--in-container-fake cannot be combined with --network-container' >&2
    exit 2
fi
if [[ -n $network_container ]]; then
    # A supplied network namespace is itself the simulator topology; do not
    # start an unrelated Compose service and mistake it for the requested one.
    skip_simulator_start=true
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
adapter_root=$(cd -- "$script_dir/.." && pwd)
workspace_root=$(cd -- "$adapter_root/.." && pwd)
coverage_root=$(mkdir -p -- "$coverage_output" && cd -- "$coverage_output" && pwd)
compose_file="$script_dir/compose.yaml"
aravis_dockerfile="$script_dir/aravis_fake/AdapterValidation.Dockerfile"
aravis_context="$script_dir/aravis_fake"

# A host-network Compose fake binds the same interface and makes an
# in-container result topology-ambiguous. Refuse that combination rather than
# silently testing whichever camera happened to win the bind race.
if [[ $in_container_fake == true ]]; then
    running_fake=$(docker compose -f "$compose_file" --profile linux-l2 \
        ps --status running --services aravis-fake)
    if [[ -n $running_fake ]]; then
        printf '%s\n' 'stop the Compose aravis-fake service before using --in-container-fake' >&2
        exit 2
    fi
fi

if [[ $in_container_fake != true && $skip_simulator_start != true ]]; then
    ARAVIS_INTERFACE="$interface" docker compose -f "$compose_file" --profile linux-l2 \
        up -d --build aravis-fake
fi
if [[ $skip_build != true ]]; then
    docker compose -f "$compose_file" --profile linux-l2 build aravis-fake
    fake_image_id=$(docker image inspect --format '{{.Id}}' camera-adapter-simulators-aravis-fake)
    fake_image_ref="camera-adapter-aravis-validation-input:${fake_image_id#sha256:}"
    docker tag camera-adapter-simulators-aravis-fake "$fake_image_ref"
    docker build -f "$aravis_dockerfile" \
        --build-arg "ARAVIS_RUNTIME_IMAGE=$fake_image_ref" \
        -t "$image" "$aravis_context"
fi

# Container start is not camera readiness. Do not let a first-run image build
# race the helper and silently record an empty discovery result as evidence.
if [[ $in_container_fake != true ]]; then
    fake_ready=false
    for ((attempt = 1; attempt <= 30; attempt++)); do
        if [[ -n $network_container ]]; then
            discovery=$(docker exec "$network_container" \
                arv-tool-0.8 "--gv-discovery-interface=$interface" 2>/dev/null || true)
        else
            discovery=$(docker compose -f "$compose_file" --profile linux-l2 exec -T aravis-fake \
                arv-tool-0.8 "--gv-discovery-interface=$interface" 2>/dev/null || true)
        fi
        if grep -Fq 'Aravis-Fake-GV01' <<<"$discovery"; then
            fake_ready=true
            break
        fi
        sleep 1
    done
    if [[ $fake_ready != true ]]; then
        printf 'pinned fake camera was not discoverable through interface %s\n' "$interface" >&2
        exit 1
    fi
fi

target_volume=camera-adapter-genicam-coverage-target
registry_volume=camera-adapter-genicam-coverage-registry
git_volume=camera-adapter-genicam-coverage-git
for volume in "$target_volume" "$registry_volume" "$git_volume"; do
    docker volume create "$volume" >/dev/null
done

run_coverage_command() {
    local tmpfs_size=$1
    shift
    local common_run=(
        docker run --rm --network "$network_mode" --read-only "--tmpfs=/tmp:size=$tmpfs_size,mode=1777"
        --cap-drop ALL --security-opt no-new-privileges:true
        -v "$workspace_root:/edgecommons:ro"
        -v "$target_volume:/coverage-target"
        -v "$registry_volume:/usr/local/cargo/registry"
        -v "$git_volume:/usr/local/cargo/git"
        -w /edgecommons/camera-adapter
        -e CARGO_TARGET_DIR=/coverage-target
        -e "CAMERA_ADAPTER_ARAVIS_INTERFACE=$interface"
    )
    if [[ $in_container_fake != true ]]; then
        "${common_run[@]}" "$image" "$@"
        return
    fi
    "${common_run[@]}" --entrypoint /bin/sh "$image" -c '
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

run_coverage_command 64m +1.87.0 llvm-cov clean --workspace

if [[ $aggregate_coverage == true ]]; then
    # Runtime fixtures create TempDir-backed output roots. They correctly retain
    # the production 1 GiB free-space floor, so this hardened runner gives
    # those roots a larger temporary filesystem without changing that policy.
    # Serial execution avoids cross-test broker and filesystem interference.
    run_coverage_command 2g +1.87.0 llvm-cov test --locked --no-clean --lcov \
        --no-default-features --features standalone,onvif,genicam --lib \
        --output-path /coverage-target/standalone-onvif-genicam-ordinary.lcov \
        -- --test-threads 1
fi

# This executes the real sibling helper under coverage before the library test.
# The later no-clean report merges that profile with the parent-process profile.
features=standalone,genicam
if [[ $aggregate_coverage == true ]]; then
    features=standalone,onvif,genicam
fi
helper_output=$(run_coverage_command 64m +1.87.0 llvm-cov run --locked --no-clean --lcov \
    --no-default-features --features "$features" \
    --output-path /coverage-target/genicam-discovery-helper.lcov \
    --bin camera-adapter-genicam-discover -- \
    --interface "$interface" --transport gige-vision --max-results 1)
printf '%s\n' "$helper_output"
if ! grep -Fq '"deviceId":"Aravis-Fake-GV01"' <<<"$helper_output"; then
    printf 'instrumented GenICam helper did not discover Aravis-Fake-GV01\n' >&2
    exit 1
fi

# The Cargo target volume is writable from the fully hardened container. Copy
# the finished report out from a read-only mount as the invoking host user;
# this avoids weakening the validation container merely to write a bind mount.
report_name=genicam-fake-gv-mono8.lcov
if [[ $aggregate_coverage == true ]]; then
    report_name=standalone-onvif-genicam-fake-gv.lcov
fi
artifact="/coverage-target/$report_name"
run_coverage_command 64m +1.87.0 llvm-cov test --locked --no-clean \
    --no-default-features --features "$features" --lib \
    --lcov --output-path "$artifact" \
    backend::genicam_aravis::tests::pinned_aravis_fake_discovers_and_captures_two_complete_mono8_frames \
    -- --ignored --exact --test-threads 1

host_artifact="$coverage_root/$report_name"
docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges:true \
    -v "$target_volume:/coverage-target:ro" \
    --entrypoint /bin/cat "$image" "$artifact" > "$host_artifact"
[[ -s $host_artifact ]] || { printf 'native GenICam coverage artifact was not produced\n' >&2; exit 1; }

coverage_label='Native GenICam fixture LCOV coverage'
if [[ $aggregate_coverage == true ]]; then
    coverage_label='Merged GenICam LCOV DA coverage'
fi
awk -v label="$coverage_label" '
    /^SF:/ {
        in_genicam = ($0 ~ /\/src\/backend\/genicam_aravis\.rs$/)
        next
    }
    in_genicam && /^DA:/ {
        split($0, parts, ":")
        split(parts[2], values, ",")
        lines += 1
        if (values[2] > 0) hits += 1
    }
    END {
        if (lines == 0) exit 2
        printf "%s: %d/%d lines (%.2f%%)\n", label, hits, lines, 100 * hits / lines
    }
' "$host_artifact"

printf 'Native fake-Aravis fixture LCOV artifact: %s\n' "$host_artifact"
if [[ $aggregate_coverage == true ]]; then
    summary="$coverage_root/standalone-onvif-genicam-fake-gv.summary.json"
    summary_artifact=/coverage-target/standalone-onvif-genicam-fake-gv.summary.json
    run_coverage_command 64m +1.87.0 llvm-cov report --locked --json --summary-only \
        --output-path "$summary_artifact"
    docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges:true \
        -v "$target_volume:/coverage-target:ro" \
        --entrypoint /bin/cat "$image" "$summary_artifact" > "$summary"
    [[ -s $summary ]] || { printf 'aggregate coverage summary was not produced\n' >&2; exit 1; }
    printf 'Aggregate feature coverage summary: %s\n' "$summary"
    cat "$summary"
fi
if [[ $in_container_fake == true ]]; then
    printf '%s\n' 'Scope: same-container native Aravis protocol/buffer evidence; no cross-container or cross-host GigE claim.'
fi
printf '%s\n' 'This fixture evidence is not adapter-wide coverage or physical-camera compatibility evidence.'
