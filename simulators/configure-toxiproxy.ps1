[CmdletBinding()]
param(
    [string]$Api = 'http://127.0.0.1:18474'
)

$ErrorActionPreference = 'Stop'
$definitions = @(
    [ordered]@{
        name = 'onvif'
        listen = '[::]:28080'
        upstream = 'onvif-sim:8080'
        enabled = $true
    },
    [ordered]@{
        name = 'rtsp'
        listen = '[::]:28554'
        upstream = 'mediamtx:8554'
        enabled = $true
    }
)

foreach ($definition in $definitions) {
    $uri = "$Api/proxies/$($definition.name)"
    try {
        $existing = Invoke-RestMethod -Method Get -Uri $uri -TimeoutSec 5 `
            -UserAgent 'camera-adapter-simulator/1.0'
        if ($existing.listen -ne $definition.listen -or $existing.upstream -ne $definition.upstream) {
            throw "Existing proxy '$($definition.name)' does not match the versioned topology"
        }
        continue
    }
    catch {
        if ($_.Exception.Response.StatusCode.value__ -ne 404) {
            throw
        }
    }

    $body = $definition | ConvertTo-Json -Compress
    Invoke-RestMethod -Method Post -Uri "$Api/proxies" -ContentType 'application/json' `
        -Body $body -TimeoutSec 5 -UserAgent 'camera-adapter-simulator/1.0' | Out-Null
}

Invoke-RestMethod -Method Get -Uri "$Api/proxies" -TimeoutSec 5 `
    -UserAgent 'camera-adapter-simulator/1.0'
