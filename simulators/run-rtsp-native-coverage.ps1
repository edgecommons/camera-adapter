[CmdletBinding()]
param(
    [string]$CoverageOutput = (Join-Path ([System.IO.Path]::GetTempPath()) 'camera-adapter-rtsp-coverage'),
    [string]$Image = 'camera-adapter-rtsp-validation',
    [string]$Network = 'camera-adapter-simulators_default',
    [switch]$SkipBuild,
    [switch]$SkipSimulatorStart
)

$ErrorActionPreference = 'Stop'

function Invoke-Docker {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

$adapterRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$workspaceRoot = Split-Path -Parent $adapterRoot
$composeFile = Join-Path $PSScriptRoot 'compose.yaml'
$coverageRoot = [System.IO.Path]::GetFullPath($CoverageOutput)

New-Item -ItemType Directory -Force -Path $coverageRoot | Out-Null

if (-not $SkipSimulatorStart) {
    Invoke-Docker -Arguments @('compose', '-f', $composeFile, 'up', '-d', '--wait', 'mediamtx')
}

if (-not $SkipBuild) {
    Invoke-Docker -Arguments @('build', '-f', (Join-Path $PSScriptRoot 'rtsp_validation.Dockerfile'), '-t', $Image, $adapterRoot)
}

# Named volumes prevent Cargo from writing into the read-only source mount and
# make a rerun reuse dependency downloads and native build products.
$targetVolume = 'camera-adapter-rtsp-coverage-target'
$registryVolume = 'camera-adapter-rtsp-coverage-registry'
$gitVolume = 'camera-adapter-rtsp-coverage-git'
foreach ($volume in @($targetVolume, $registryVolume, $gitVolume)) {
    Invoke-Docker -Arguments @('volume', 'create', $volume)
}

$sourceMount = "${workspaceRoot}:/edgecommons:ro"
$targetMount = "${targetVolume}:/coverage-target"
$registryMount = "${registryVolume}:/usr/local/cargo/registry"
$gitMount = "${gitVolume}:/usr/local/cargo/git"
$artifactMount = "${coverageRoot}:/coverage-artifacts"

# THE LOCKFILE. `Cargo.lock` is committed and git-sourced, but this harness cannot build against the
# in-tree copy directly: the source is mounted `:ro` on purpose, and the workspace it mounts carries
# the developer's gitignored `.cargo/config.toml` `[patch]`, which redirects `edgecommons` to a local
# path -- so a build here would need to REWRITE the committed git-sourced lock to a path source, and
# cargo cannot write it on the read-only mount (`Read-only file system (os error 30)`).
#
# So a WRITABLE OVERLAY lock is bind-mounted as a single FILE from outside the source tree, masking the
# committed one, and generated once, in a container, by the prep run below (with the patch active, it
# becomes a path-sourced lock the `:ro` runs then use consistently under `--locked`). The source tree
# itself is never written to -- the immutability this script depends on is preserved exactly.
# Generating it in the container also keeps the lockfile version inside what the pinned toolchain can
# read, which a host-side `cargo generate-lockfile` would not guarantee.
$lockRoot = Join-Path $coverageRoot 'lock'
New-Item -ItemType Directory -Force -Path $lockRoot | Out-Null
$lockFile = Join-Path $lockRoot 'Cargo.lock'
if (-not (Test-Path -LiteralPath $lockFile)) {
    # Docker creates a DIRECTORY at a bind source that does not exist; the file must be there first.
    New-Item -ItemType File -Path $lockFile | Out-Null
}
$lockMountRw = "${lockFile}:/edgecommons/camera-adapter/Cargo.lock"
$lockMount = "${lockFile}:/edgecommons/camera-adapter/Cargo.lock:ro"

# Resolve dependencies once, with the lock writable and the network up. Every run after this one is
# hardened and offline-shaped: `:ro` source, `:ro` lock, `--locked`.
Invoke-Docker -Arguments @(
    'run', '--rm', '--network', $Network,
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

foreach ($fixture in @(
    @{ Name = 'h264'; Path = 'camera' },
    @{ Name = 'h265'; Path = 'camera-h265' }
)) {
    foreach ($test in @(
        @{ Name = 'first-frame'; Filter = 'backend::rtsp::tests::pinned_mediamtx_produces_a_complete_rgb_frame' },
        @{ Name = 'warm-session'; Filter = 'backend::rtsp::tests::pinned_mediamtx_warm_session_produces_two_complete_frames' }
    )) {
        $artifact = "/coverage-artifacts/rtsp-$($fixture.Name)-$($test.Name).lcov"
        Invoke-Docker -Arguments @(
            'run', '--rm', '--network', $Network,
            '-v', $sourceMount,
        '-v', $lockMount,
            '-v', $targetMount,
            '-v', $registryMount,
            '-v', $gitMount,
            '-v', $artifactMount,
            '-w', '/edgecommons/camera-adapter',
            '-e', 'CARGO_TARGET_DIR=/coverage-target',
            '-e', "CAMERA_ADAPTER_RTSP_URI=rtsp://mediamtx:8554/$($fixture.Path)",
            $Image,
            '+1.87.0', 'llvm-cov', 'test', '--locked', '--no-default-features', '--features', 'standalone,onvif,rtsp',
            '--lib', '--lcov', '--output-path', $artifact,
            $test.Filter,
            '--', '--ignored', '--exact'
        )

        $hostArtifact = Join-Path $coverageRoot "rtsp-$($fixture.Name)-$($test.Name).lcov"
        if (-not (Test-Path -LiteralPath $hostArtifact) -or (Get-Item -LiteralPath $hostArtifact).Length -eq 0) {
            throw "native RTSP coverage did not produce $hostArtifact"
        }
    }
}

# The four reports above are intentionally isolated fixture evidence. Re-run
# the ordinary native-feature library suite plus both live codecs without
# cleaning between invocations, then export one measured aggregate. This is a
# native RTSP scope report, not the adapter-wide coverage gate.
$summaryArtifact = '/coverage-artifacts/rtsp-native-summary.json'
Invoke-Docker -Arguments @(
    'run', '--rm', '--network', $Network,
    '-v', $sourceMount,
    '-v', $lockMount,
    '-v', $targetMount,
    '-v', $registryMount,
    '-v', $gitMount,
    '-v', $artifactMount,
    '-w', '/edgecommons/camera-adapter',
    '-e', 'CARGO_TARGET_DIR=/coverage-target',
    $Image,
    '+1.87.0', 'llvm-cov', 'clean'
)
Invoke-Docker -Arguments @(
    'run', '--rm', '--network', $Network,
    '-v', $sourceMount,
    '-v', $lockMount,
    '-v', $targetMount,
    '-v', $registryMount,
    '-v', $gitMount,
    '-v', $artifactMount,
    '-w', '/edgecommons/camera-adapter',
    '-e', 'CARGO_TARGET_DIR=/coverage-target',
    $Image,
    '+1.87.0', 'llvm-cov', 'test', '--no-clean', '--locked',
    '--no-default-features', '--features', 'standalone,onvif,rtsp', '--lib',
    '--json', '--summary-only', '--output-path', $summaryArtifact
)
foreach ($fixture in @(
    @{ Name = 'h264'; Path = 'camera' },
    @{ Name = 'h265'; Path = 'camera-h265' }
)) {
    Invoke-Docker -Arguments @(
        'run', '--rm', '--network', $Network,
        '-v', $sourceMount,
        '-v', $lockMount,
        '-v', $targetMount,
        '-v', $registryMount,
        '-v', $gitMount,
        '-v', $artifactMount,
        '-w', '/edgecommons/camera-adapter',
        '-e', 'CARGO_TARGET_DIR=/coverage-target',
        '-e', "CAMERA_ADAPTER_RTSP_URI=rtsp://mediamtx:8554/$($fixture.Path)",
        $Image,
        '+1.87.0', 'llvm-cov', 'test', '--no-clean', '--locked',
        '--no-default-features', '--features', 'standalone,onvif,rtsp', '--lib',
        '--json', '--summary-only', '--output-path', $summaryArtifact,
        # SERIALIZED, and it must be. This filter matches BOTH live fixtures, and they share one
        # MediaMTX stream -- run concurrently, the warm-session test's second capture starves behind
        # the cold test's session and dies on CAPTURE_TIMEOUT. The isolated runs above never showed it
        # because `--exact` gives each of them a single test. Serialized, both pass in under two
        # seconds; in parallel, one of them fails. The product is fine; the harness was racing itself.
        'backend::rtsp::tests::pinned_mediamtx', '--', '--ignored', '--test-threads', '1'
    )
}
Invoke-Docker -Arguments @(
    'run', '--rm', '--network', $Network,
    '-v', $sourceMount,
    '-v', $lockMount,
    '-v', $targetMount,
    '-v', $registryMount,
    '-v', $gitMount,
    '-v', $artifactMount,
    '-w', '/edgecommons/camera-adapter',
    '-e', 'CARGO_TARGET_DIR=/coverage-target',
    $Image,
    '+1.87.0', 'llvm-cov', 'report', '--locked', '--json', '--summary-only',
    '--output-path', $summaryArtifact
)
$hostSummary = Join-Path $coverageRoot 'rtsp-native-summary.json'
if (-not (Test-Path -LiteralPath $hostSummary) -or (Get-Item -LiteralPath $hostSummary).Length -eq 0) {
    throw "native RTSP coverage did not produce $hostSummary"
}
$summary = Get-Content -LiteralPath $hostSummary -Raw | ConvertFrom-Json
$rtspSummary = @($summary.data | ForEach-Object { $_.files } | Where-Object {
    $_.filename -replace '\\', '/' -match '/src/backend/rtsp\.rs$'
})
if ($rtspSummary.Count -ne 1) {
    throw "native RTSP coverage summary did not contain exactly one src/backend/rtsp.rs entry"
}
$lines = $rtspSummary[0].summary.lines
$linePercent = if ($lines.count -eq 0) { 0 } else { [math]::Round((100 * $lines.covered) / $lines.count, 2) }
Write-Host "Native RTSP aggregate: $($lines.covered)/$($lines.count) lines ($linePercent%)"
Write-Host "Native H.264/H.265 fixture LCOV and aggregate summary artifacts: $coverageRoot"
