#!/usr/bin/env bash
# Run the Linux-only capacity workload inside a pinned Rust/Python image. The
# workload container itself has no external network; its in-process MQTT peer
# uses loopback only. A separate cache-population step may use bridge networking
# before the isolated test begins.
set -euo pipefail
umask 077

artifact_dir=
source_revision=
source_bundle=
image=camera-adapter-capacity-validation:rust-1.85.1
target_volume=camera-adapter-capacity-target
registry_volume=camera-adapter-capacity-registry
git_volume=camera-adapter-capacity-git
soak_duration=

usage() {
    cat <<'EOF'
Usage: run-capacity-validation-container.sh --artifact-dir PATH --source-revision COMMIT --source-bundle PATH [options]

  --artifact-dir PATH           Required new or empty host directory for evidence
  --source-revision COMMIT      Required full 40- or 64-hex commit revision
  --source-bundle PATH          Required real, non-symlink exact staged source tarball; its SHA-256 is computed here
  --soak-duration 15m           Run the optional bounded 15-minute smoke after the short proof
  --image NAME                  Capacity validation image tag
  --target-volume NAME          Named Cargo target volume
  --registry-volume NAME        Named Cargo registry volume
  --git-volume NAME             Named Cargo git volume

The test container mounts the whole workspace read-only, writes only to the
explicit evidence directory and named Cargo volumes, drops all capabilities,
uses no-new-privileges, a read-only root filesystem, tmpfs /tmp, and no network.
The wrapper computes the manifest bundle SHA-256 directly from `--source-bundle`.
Cargo volumes and both workload containers use the invoking host uid:gid; a
temporary root setup container initializes only the named Cargo volumes.
EOF
}

fail() {
    printf '%s\n' "$*" >&2
    exit 2
}

while (($#)); do
    case "$1" in
        --artifact-dir) artifact_dir=${2:?missing artifact directory}; shift 2 ;;
        --source-revision) source_revision=${2:?missing source revision}; shift 2 ;;
        --source-bundle) source_bundle=${2:?missing source bundle path}; shift 2 ;;
        --soak-duration) soak_duration=${2:?missing soak duration}; shift 2 ;;
        --image) image=${2:?missing image tag}; shift 2 ;;
        --target-volume) target_volume=${2:?missing target volume}; shift 2 ;;
        --registry-volume) registry_volume=${2:?missing registry volume}; shift 2 ;;
        --git-volume) git_volume=${2:?missing git volume}; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ $(uname -s) == Linux ]] || fail 'capacity container validation requires a true Linux Docker host such as lab-5950x'
[[ -n $artifact_dir ]] || fail '--artifact-dir is required'
[[ $source_revision =~ ^[[:xdigit:]]{40}([[:xdigit:]]{24})?$ ]] || fail '--source-revision must be a full 40- or 64-hex commit revision'
[[ -n $source_bundle ]] || fail '--source-bundle is required'
[[ -f $source_bundle && ! -L $source_bundle && -s $source_bundle ]] || fail '--source-bundle must be a non-empty real file, not a symlink'
[[ -z $soak_duration || $soak_duration == 15m ]] || fail '--soak-duration currently accepts only 15m; the 24-hour soak is deferred'
command -v docker >/dev/null || fail 'docker is required'
command -v sha256sum >/dev/null || fail 'sha256sum is required to bind the staged source tarball'
command -v id >/dev/null || fail 'id is required to select the workload identity'

source_revision=$(tr '[:upper:]' '[:lower:]' <<<"$source_revision")
source_bundle=$(cd -- "$(dirname -- "$source_bundle")" && pwd)/$(basename -- "$source_bundle")
source_bundle_sha256=$(sha256sum -- "$source_bundle" | awk '{print $1}')
[[ $source_bundle_sha256 =~ ^[[:xdigit:]]{64}$ ]] || fail 'could not compute a SHA-256 for --source-bundle'
host_uid=$(id -u)
host_gid=$(id -g)
[[ $host_uid =~ ^[0-9]+$ && $host_gid =~ ^[0-9]+$ ]] || fail 'host uid:gid must be numeric'
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
adapter_root=$(cd -- "$script_dir/.." && pwd)
workspace_root=$(cd -- "$adapter_root/.." && pwd)
[[ -f $workspace_root/core/libs/rust/Cargo.toml ]] || fail "workspace root must include core/libs/rust: $workspace_root"
[[ -f $workspace_root/core/proto/edgecommons/v1/value.proto ]] || fail "workspace root must include core/proto build inputs: $workspace_root"

if [[ -e $artifact_dir || -L $artifact_dir ]]; then
    [[ -d $artifact_dir && ! -L $artifact_dir ]] || fail "artifact directory must be a real directory, not a file or symlink: $artifact_dir"
    [[ -z $(find "$artifact_dir" -mindepth 1 -maxdepth 1 -print -quit) ]] || fail "artifact directory must be new or empty: $artifact_dir"
else
    mkdir -p -- "$artifact_dir"
fi
artifact_root=$(cd -- "$artifact_dir" && pwd)

docker build --pull=false \
    -f "$script_dir/capacity_validation.Dockerfile" \
    -t "$image" \
    "$script_dir"

for volume in "$target_volume" "$registry_volume" "$git_volume"; do
    docker volume create "$volume" >/dev/null
    docker run --rm --read-only \
        --user 0:0 --cap-drop ALL --cap-add CHOWN --cap-add DAC_READ_SEARCH --security-opt no-new-privileges:true \
        -v "$volume:/volume" \
        --entrypoint /bin/chown "$image" \
        -R --no-dereference -- "$host_uid:$host_gid" /volume
done

source_mount="$workspace_root:/edgecommons:ro"
artifact_mount="$artifact_root:/capacity-artifacts"
target_mount="$target_volume:/capacity-target"
registry_mount="$registry_volume:/usr/local/cargo/registry"
git_mount="$git_volume:/usr/local/cargo/git"
common_run=(
    docker run --rm --read-only
    --tmpfs /tmp:rw,nosuid,nodev,noexec,size=2g,mode=1777
    --cap-drop ALL --security-opt no-new-privileges:true
    --pids-limit 2048
    --user "$host_uid:$host_gid"
    -v "$source_mount"
    -v "$target_mount"
    -v "$registry_mount"
    -v "$git_mount"
    -w /edgecommons/camera-adapter
    -e CARGO_TARGET_DIR=/capacity-target
    -e TMPDIR=/tmp
    -e HOME=/tmp
    -e CAMERA_ADAPTER_SOURCE_REVISION="$source_revision"
    -e CAMERA_ADAPTER_SOURCE_BUNDLE_SHA256="$source_bundle_sha256"
)

# THE LOCKFILE. `Cargo.lock` is untracked and gitignored, so a clean checkout has none -- and every
# cargo run below passes `--locked`, which REFUSES to create one ("cannot create the lock file ...
# because --locked was passed"). Dropping `--locked` does not rescue it either: the source is mounted
# `:ro` on purpose and cargo writes the lock next to the workspace Cargo.toml (CARGO_TARGET_DIR does
# not move it), so cargo would then die on "Read-only file system (os error 30)". Two walls, one
# behind the other -- and that second error is verbatim the one 3c0d83d's own message quotes.
#
# So the lock is bind-mounted as a single FILE from OUTSIDE the source tree, generated once by the
# networked prep run. The source tree is never written to -- the immutability this script depends on
# holds exactly -- and `--locked` goes back to asserting something true. Generating it inside the
# container also keeps the lockfile version within what the pinned toolchain can read, which a
# host-side `cargo generate-lockfile` would not guarantee.
lock_root="${TMPDIR:-/tmp}/camera-adapter-capacity-lock"
mkdir -p -- "$lock_root"
lock_file="$lock_root/Cargo.lock"
# Docker creates a DIRECTORY at a bind source that does not exist; the file must exist first.
[[ -f $lock_file ]] || : > "$lock_file"
lock_mount_ro="$lock_file:/edgecommons/camera-adapter/Cargo.lock:ro"
lock_mount_rw="$lock_file:/edgecommons/camera-adapter/Cargo.lock"

test_run=("${common_run[@]}" --network none -v "$artifact_mount" -v "$lock_mount_ro")
probe_name=".camera-adapter-capacity-artifact-probe-${host_uid}-${host_gid}-$$"
probe_path="$artifact_root/$probe_name"
cleanup_probe() {
    rm -f -- "$probe_path" 2>/dev/null || true
}
trap cleanup_probe EXIT
"${test_run[@]}" --entrypoint /bin/sh "$image" -c '
    set -eu
    probe=$1
    [ ! -e "$probe" ]
    : > "$probe"
    [ -f "$probe" ]
    rm -- "$probe"
    [ ! -e "$probe" ]
' sh "/capacity-artifacts/$probe_name"
[[ ! -e $probe_path ]] || fail 'artifact writable-identity preflight left a probe behind'
trap - EXIT

# Populate only named Cargo cache volumes before the isolated workload starts.
# The source mount remains read-only, and `--locked` retains Cargo.lock as the
# dependency authority. The capacity test itself below runs with --network none.
prefetch_run=("${common_run[@]}" --network bridge -v "$lock_mount_rw")
"${prefetch_run[@]}" --entrypoint cargo "$image" generate-lockfile
[[ -s $lock_file ]] || { echo "the prep run did not produce a Cargo.lock at $lock_file" >&2; exit 1; }
"${prefetch_run[@]}" --entrypoint cargo "$image" fetch --locked

inner_args=(
    /edgecommons/camera-adapter/simulators/run-capacity-validation.sh
    --artifact-dir /capacity-artifacts
    --target-dir /capacity-target
)
if [[ -n $soak_duration ]]; then
    inner_args+=(--soak-duration "$soak_duration")
fi
"${test_run[@]}" --entrypoint /bin/bash "$image" "${inner_args[@]}"

printf '%s\n' 'Scope: test workload ran with no external network; the earlier bridge step populated Cargo cache volumes only.'
