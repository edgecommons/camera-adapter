[CmdletBinding()]
param(
    [switch]$LinuxL2,
    [string]$AravisInterface = 'eth0'
)

$ErrorActionPreference = 'Stop'
$composeFile = Join-Path $PSScriptRoot 'compose.yaml'

function Invoke-DockerChecked {
    param([Parameter(Mandatory)][string[]]$Arguments)

    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker command failed with exit code $LASTEXITCODE"
    }
}

# The simulator's TLS material is minted, not committed, so it must exist before the image that bakes
# it in is built. Idempotent: a no-op once the certificates are present and unexpired.
& (Join-Path $PSScriptRoot 'generate-tls-fixtures.ps1')
if ($LASTEXITCODE -ne 0) {
    throw "could not generate the simulator TLS fixtures"
}

Invoke-DockerChecked @('compose', '-f', $composeFile, 'config', '--quiet')
Invoke-DockerChecked @(
    'compose', '-f', $composeFile, '--profile', 'verify', 'run', '--rm', '--build',
    'onvif-sim-tests'
)
Invoke-DockerChecked @(
    'compose', '-f', $composeFile, 'up', '-d', '--build', '--wait',
    'onvif-sim', 'onvif-sim-tls', 'mediamtx', 'toxiproxy'
)

& (Join-Path $PSScriptRoot 'configure-toxiproxy.ps1') | Out-Null

$health = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/healthz' -TimeoutSec 5
if ($health.status -ne 'ok') {
    throw 'ONVIF simulator health payload was not ok'
}
$tlsHealth = Invoke-RestMethod -Uri 'https://127.0.0.1:18443/healthz' -TimeoutSec 5 `
    -SkipCertificateCheck
if ($tlsHealth.status -ne 'ok') {
    throw 'ONVIF TLS simulator health payload was not ok'
}

$paths = Invoke-RestMethod -Uri 'http://127.0.0.1:19997/v3/paths/list' -TimeoutSec 5
$expectedStreams = @{
    'camera' = 'H264'
    'camera-h265' = 'H265'
}
foreach ($entry in $expectedStreams.GetEnumerator()) {
    $path = $paths.items | Where-Object name -EQ $entry.Key
    if ($null -eq $path -or -not $path.ready -or $path.tracks -notcontains $entry.Value) {
        throw "MediaMTX path '$($entry.Key)' is not ready with codec '$($entry.Value)'"
    }
    if ($path.inboundFramesInError -ne 0) {
        throw "MediaMTX path '$($entry.Key)' reported inbound frame errors"
    }
}

$proxies = Invoke-RestMethod -Uri 'http://127.0.0.1:18474/proxies' -TimeoutSec 5 `
    -UserAgent 'camera-adapter-simulator/1.0'
foreach ($name in @('onvif', 'rtsp')) {
    if ($null -eq $proxies.$name -or -not $proxies.$name.enabled) {
        throw "Toxiproxy route '$name' is missing or disabled"
    }
}

if ($LinuxL2) {
    $env:ARAVIS_INTERFACE = $AravisInterface
    Invoke-DockerChecked @(
        'compose', '-f', $composeFile, '--profile', 'linux-l2', 'up', '-d', '--build',
        'aravis-fake'
    )
    $discovery = & docker compose -f $composeFile --profile linux-l2 exec -T aravis-fake `
        arv-tool-0.8 "--gv-discovery-interface=$AravisInterface"
    if ($LASTEXITCODE -ne 0 -or $discovery -notmatch 'Aravis-Fake-GV01') {
        throw 'Aravis fake camera was not discovered through the selected interface'
    }
    $acquisition = & docker compose -f $composeFile --profile linux-l2 exec -T aravis-fake `
        arv-camera-test-0.8 "--gv-discovery-interface=$AravisInterface" `
        '--name=Aravis-Fake-GV01' '--width=320' '--height=240' '--duration=3'
    if ($LASTEXITCODE -ne 0) {
        throw "Aravis acquisition failed with exit code $LASTEXITCODE"
    }
    $acquisitionText = $acquisition -join "`n"
    foreach ($required in @(
        'n_completed_buffers',
        'n_failures             = 0',
        'n_missing_frames       = 0',
        'n_size_mismatch_errors = 0'
    )) {
        if ($acquisitionText -notmatch [regex]::Escape($required)) {
            throw "Aravis acquisition evidence omitted '$required'"
        }
    }
    $completed = [regex]::Match($acquisitionText, 'n_completed_buffers\s*=\s*(\d+)')
    if (-not $completed.Success -or [int]$completed.Groups[1].Value -lt 1) {
        throw 'Aravis acquisition completed no buffers'
    }
}

[pscustomobject]@{
    Onvif = 'healthy'
    OnvifTls = 'healthy'
    RtspH264 = 'ready'
    RtspH265 = 'ready'
    Toxiproxy = 'configured'
    AravisL2 = if ($LinuxL2) { 'verified' } else { 'not-requested' }
}
