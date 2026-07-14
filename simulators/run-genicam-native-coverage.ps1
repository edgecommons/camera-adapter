<#
.SYNOPSIS
Runs the production GenICam adapter path against the pinned Aravis fake GigE
camera and writes one fixture-scoped LCOV artifact.

.DESCRIPTION
This requires a Linux-backed Docker engine with host-network support. It does
not claim physical-camera compatibility or adapter-wide coverage: it proves
only the pinned fake camera's discovery, software trigger, complete Mono8
buffer, session reuse, and close path.
#>
[CmdletBinding()]
param(
    [string]$Interface = 'eth0',
    [string]$CoverageOutput = (Join-Path ([System.IO.Path]::GetTempPath()) 'camera-adapter-genicam-coverage'),
    [string]$Image = 'camera-adapter-aravis-validation',
    [switch]$SkipBuild,
    [switch]$SkipSimulatorStart
)

$ErrorActionPreference = 'Stop'

if ($env:OS -eq 'Windows_NT') {
    throw 'Native fake-Aravis coverage requires a true Linux host/L2 namespace. Windows Docker Desktop Linux containers are not accepted evidence; run this script from a native Linux or WSL Linux host connected to the camera-facing interface.'
}

function Invoke-Docker {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

if ([string]::IsNullOrWhiteSpace($Interface)) {
    throw 'Interface must name the Linux camera-facing host interface'
}

$adapterRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$workspaceRoot = Split-Path -Parent $adapterRoot
$composeFile = Join-Path $PSScriptRoot 'compose.yaml'
$aravisDockerfile = Join-Path $PSScriptRoot 'aravis_fake/AdapterValidation.Dockerfile'
$aravisContext = Join-Path $PSScriptRoot 'aravis_fake'
$coverageRoot = [System.IO.Path]::GetFullPath($CoverageOutput)

New-Item -ItemType Directory -Force -Path $coverageRoot | Out-Null

if (-not $SkipSimulatorStart) {
    # The fake camera and validation client intentionally share the Linux host
    # network. A bridge/NAT result would not prove GigE Vision discovery.
    $env:ARAVIS_INTERFACE = $Interface
    Invoke-Docker -Arguments @(
        'compose', '-f', $composeFile, '--profile', 'linux-l2',
        'up', '-d', '--build', 'aravis-fake'
    )
}

if (-not $SkipBuild) {
    Invoke-Docker -Arguments @(
        'compose', '-f', $composeFile, '--profile', 'linux-l2', 'build', 'aravis-fake'
    )
    $fakeImageId = (& docker image inspect --format '{{.Id}}' 'camera-adapter-simulators-aravis-fake').Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($fakeImageId)) {
        throw 'unable to resolve the freshly built Aravis fake image ID'
    }
    $fakeImageReference = "camera-adapter-aravis-validation-input:$($fakeImageId -replace '^sha256:', '')"
    Invoke-Docker -Arguments @('tag', 'camera-adapter-simulators-aravis-fake', $fakeImageReference)
    Invoke-Docker -Arguments @(
        'build', '-f', $aravisDockerfile,
        '--build-arg', "ARAVIS_RUNTIME_IMAGE=$fakeImageReference",
        '-t', $Image, $aravisContext
    )
}

# Cargo state is kept off the read-only source mount. The named volumes are
# retained so a repeat run need not rebuild the native dependency graph.
$targetVolume = 'camera-adapter-genicam-coverage-target'
$registryVolume = 'camera-adapter-genicam-coverage-registry'
$gitVolume = 'camera-adapter-genicam-coverage-git'
foreach ($volume in @($targetVolume, $registryVolume, $gitVolume)) {
    Invoke-Docker -Arguments @('volume', 'create', $volume)
}

$sourceMount = "${workspaceRoot}:/edgecommons:ro"
$targetMount = "${targetVolume}:/coverage-target"
$registryMount = "${registryVolume}:/usr/local/cargo/registry"
$gitMount = "${gitVolume}:/usr/local/cargo/git"
$artifactMount = "${coverageRoot}:/coverage-artifacts"
# THE LOCKFILE. `Cargo.lock` is untracked and gitignored, so a clean checkout has none -- and the runs
# below pass `--locked`, which REFUSES to create one ("cannot create the lock file ... because
# --locked was passed"). Dropping `--locked` does not rescue it either: the source is mounted `:ro` on
# purpose and cargo writes the lock next to the workspace Cargo.toml (CARGO_TARGET_DIR does not move
# it), so cargo would then die on "Read-only file system (os error 30)". Two walls, one behind the
# other.
#
# So the lock is bind-mounted as a single FILE from OUTSIDE the source tree, generated once by the
# networked prep run below. The source tree is never written to -- the immutability this script
# depends on holds exactly -- and `--locked` goes back to asserting something true.
$lockRoot = Join-Path $coverageRoot 'lock'
New-Item -ItemType Directory -Force -Path $lockRoot | Out-Null
$lockFile = Join-Path $lockRoot 'Cargo.lock'
if (-not (Test-Path -LiteralPath $lockFile)) {
    # Docker creates a DIRECTORY at a bind source that does not exist; the file must be there first.
    New-Item -ItemType File -Path $lockFile | Out-Null
}
$lockMountRw = "${lockFile}:/edgecommons/camera-adapter/Cargo.lock"
$lockMount = "${lockFile}:/edgecommons/camera-adapter/Cargo.lock:ro"

# The one step with a network and a writable lock. Everything after it is hardened and `--locked`.
Invoke-Docker -Arguments @(
    'run', '--rm', '--network', 'bridge', '--read-only', '--tmpfs', '/tmp:size=64m,mode=1777',
    '-v', $sourceMount,
    '-v', $lockMountRw,
    '-v', $targetMount,
    '-v', $registryMount,
    '-v', $gitMount,
    '-w', '/edgecommons/camera-adapter',
    '-e', 'CARGO_TARGET_DIR=/coverage-target',
    $Image,
    '+1.87.0', 'generate-lockfile'
)
if ((Get-Item -LiteralPath $lockFile).Length -eq 0) {
    throw "the prep run did not produce a Cargo.lock at $lockFile"
}

$artifact = '/coverage-artifacts/genicam-fake-gv-mono8.lcov'
$commonRunArguments = @(
    'run', '--rm', '--network', 'host', '--read-only', '--tmpfs', '/tmp:size=64m,mode=1777',
    '-v', $sourceMount,
    '-v', $lockMount,
    '-v', $targetMount,
    '-v', $registryMount,
    '-v', $gitMount,
    '-v', $artifactMount,
    '-w', '/edgecommons/camera-adapter',
    '-e', 'CARGO_TARGET_DIR=/coverage-target',
    '-e', "CAMERA_ADAPTER_ARAVIS_INTERFACE=$Interface",
    $Image
)

# Start from an explicit clean profile set so this fixture artifact cannot
# accidentally aggregate unrelated prior coverage from the retained volume.
Invoke-Docker -Arguments ($commonRunArguments + @(
    '+1.87.0', 'llvm-cov', 'clean', '--workspace'
))
# Build and execute the real production helper at cargo-llvm-cov's fixed target
# location. This avoids a test-harness binary and contributes a separately
# collected profile that the later no-clean library test report can merge.
Invoke-Docker -Arguments ($commonRunArguments + @(
    '+1.87.0', 'llvm-cov', 'run', '--locked', '--no-report', '--no-default-features',
    '--features', 'standalone,genicam',
    '--bin', 'camera-adapter-genicam-discover', '--',
    '--interface', $Interface, '--transport', 'gige-vision', '--max-results', '1'
))
Invoke-Docker -Arguments ($commonRunArguments + @(
    '+1.87.0', 'llvm-cov', 'test', '--locked', '--no-clean',
    '--no-default-features', '--features', 'standalone,genicam',
    '--lib', '--lcov', '--output-path', $artifact,
    'backend::genicam_aravis::tests::pinned_aravis_fake_discovers_and_captures_two_complete_mono8_frames',
    '--', '--ignored', '--exact', '--test-threads', '1'
))

$hostArtifact = Join-Path $coverageRoot 'genicam-fake-gv-mono8.lcov'
if (-not (Test-Path -LiteralPath $hostArtifact) -or (Get-Item -LiteralPath $hostArtifact).Length -eq 0) {
    throw "native GenICam coverage did not produce $hostArtifact"
}

$moduleLines = 0
$moduleHits = 0
$isGenicamModule = $false
foreach ($line in Get-Content -LiteralPath $hostArtifact) {
    if ($line.StartsWith('SF:')) {
        $normalized = $line.Substring(3).Replace('\', '/')
        $isGenicamModule = $normalized.EndsWith('/src/backend/genicam_aravis.rs')
        continue
    }
    if ($isGenicamModule -and $line -match '^DA:\d+,(\d+)$') {
        $moduleLines++
        if ([int64]$Matches[1] -gt 0) {
            $moduleHits++
        }
    }
}
if ($moduleLines -eq 0) {
    throw "native GenICam coverage artifact did not contain src/backend/genicam_aravis.rs"
}
$moduleCoverage = [math]::Round((100.0 * $moduleHits) / $moduleLines, 2)

Write-Host "Native fake-Aravis fixture LCOV artifact: $hostArtifact"
Write-Host "Native GenICam module fixture coverage: $moduleHits/$moduleLines lines ($moduleCoverage%)"
Write-Host 'This artifact is not an adapter-wide coverage result and does not certify physical camera compatibility.'
