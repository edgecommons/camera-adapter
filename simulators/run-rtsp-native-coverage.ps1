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

Write-Host "Native H.264/H.265 first-frame and warm-session fixture LCOV artifacts: $coverageRoot"
