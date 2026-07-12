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
    Invoke-Docker -Arguments @('build', '-f', $aravisDockerfile, '-t', $Image, $aravisContext)
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
$artifact = '/coverage-artifacts/genicam-fake-gv-mono8.lcov'
$commonRunArguments = @(
    'run', '--rm', '--network', 'host', '--read-only', '--tmpfs', '/tmp:size=64m,mode=1777',
    '-v', $sourceMount,
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
Invoke-Docker -Arguments ($commonRunArguments + @(
    '+1.87.0', 'llvm-cov', 'test', '--locked', '--no-clean', '--no-report',
    '--no-default-features', '--features', 'standalone,genicam',
    '--lib', '--bin', 'camera-adapter-genicam-discover',
    'backend::genicam_aravis::tests::pinned_aravis_fake_discovers_and_captures_two_complete_mono8_frames',
    '--', '--ignored', '--exact', '--test-threads', '1'
))
Invoke-Docker -Arguments ($commonRunArguments + @(
    '+1.87.0', 'llvm-cov', 'report', '--lcov', '--output-path', $artifact
))

$hostArtifact = Join-Path $coverageRoot 'genicam-fake-gv-mono8.lcov'
if (-not (Test-Path -LiteralPath $hostArtifact) -or (Get-Item -LiteralPath $hostArtifact).Length -eq 0) {
    throw "native GenICam coverage did not produce $hostArtifact"
}

Write-Host "Native fake-Aravis fixture LCOV artifact: $hostArtifact"
Write-Host 'This artifact is not an adapter-wide coverage result and does not certify physical camera compatibility.'
