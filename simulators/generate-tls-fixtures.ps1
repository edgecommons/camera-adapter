<#
.SYNOPSIS
    Generates the ONVIF simulator's throwaway TLS material.

.DESCRIPTION
    These certificates used to be COMMITTED, private key and all. That is a bad habit even when the
    key is worthless -- and once this repository went public, GitHub's secret scanning was entirely
    right to flag it. A private key in a public repository is a private key in a public repository;
    the reader has to take our word for it that this one guards nothing.

    So it is minted here instead, on demand, and never tracked. The material is deliberately
    uninteresting: a self-signed test CA and one server certificate for `camera.test`, valid for ten
    years, guarding a simulator that serves fake cameras to a test suite.

    Idempotent: regenerates only when the material is missing or within 30 days of expiry, so a
    normal verify.ps1 run costs nothing. Use -Force to mint a fresh set regardless.

    Requires openssl. It ships with Git for Windows; if `openssl` is not on PATH, add
    `C:\Program Files\Git\usr\bin` to it.
#>
[CmdletBinding()]
param(
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

$dir = Join-Path $PSScriptRoot 'onvif_sim/fixtures/tls'
$caCert = Join-Path $dir 'ca-cert.pem'
$caKey = Join-Path $dir 'ca-key.pem'
$serverCert = Join-Path $dir 'server-cert.pem'
$serverKey = Join-Path $dir 'server-key.pem'

if (-not (Get-Command openssl -ErrorAction SilentlyContinue)) {
    throw "openssl is not on PATH. It ships with Git for Windows: add 'C:\Program Files\Git\usr\bin'."
}

function Invoke-OpenSsl {
    param([Parameter(Mandatory)][string[]]$Arguments)

    $output = & openssl @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "openssl $($Arguments -join ' ') failed:`n$output"
    }
}

if (-not $Force -and (Test-Path $caCert) -and (Test-Path $serverCert) -and (Test-Path $serverKey)) {
    # `-checkend` exits non-zero when the certificate expires inside the given window.
    & openssl x509 -in $serverCert -noout -checkend 2592000 *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "TLS fixtures are present and valid: $dir"
        return
    }
    Write-Host 'TLS fixtures expire within 30 days; regenerating.'
}

New-Item -ItemType Directory -Force -Path $dir | Out-Null
$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Force -Path $tmp | Out-Null

try {
    # The server certificate's extensions. `subjectAltName` is the load-bearing one: the adapter pins
    # the hostname it dialled, so a certificate without `camera.test` here fails hostname verification
    # and the TLS tests fail in a way that looks like a bug in the adapter rather than a bad fixture.
    $ext = Join-Path $tmp 'server.ext'
    @(
        'basicConstraints = critical, CA:FALSE'
        'keyUsage = critical, digitalSignature, keyAgreement'
        'extendedKeyUsage = serverAuth'
        'subjectAltName = DNS:camera.test, DNS:localhost, IP:127.0.0.1, IP:::1'
    ) | Set-Content -Path $ext -Encoding ascii

    # The CA. Its key stays in the fixtures directory, which is gitignored in its entirety -- it signs
    # nothing but this one simulator certificate and is regenerated whenever anyone asks.
    Invoke-OpenSsl @('ecparam', '-name', 'prime256v1', '-genkey', '-noout', '-out', $caKey)
    Invoke-OpenSsl @(
        'req', '-new', '-x509', '-key', $caKey, '-out', $caCert, '-days', '3650', '-sha256',
        '-subj', '/O=EdgeCommons Test Fixtures/CN=Camera Simulator Test CA',
        '-addext', 'basicConstraints=critical,CA:TRUE,pathlen:0',
        '-addext', 'keyUsage=critical,digitalSignature,keyCertSign,cRLSign'
    )

    # The server certificate for the simulator.
    $csr = Join-Path $tmp 'server.csr'
    Invoke-OpenSsl @('ecparam', '-name', 'prime256v1', '-genkey', '-noout', '-out', $serverKey)
    Invoke-OpenSsl @(
        'req', '-new', '-key', $serverKey, '-out', $csr, '-sha256',
        '-subj', '/O=EdgeCommons Test Fixtures/CN=camera.test'
    )
    Invoke-OpenSsl @(
        'x509', '-req', '-in', $csr, '-CA', $caCert, '-CAkey', $caKey, '-CAcreateserial',
        '-out', $serverCert, '-days', '3650', '-sha256', '-extfile', $ext
    )
}
finally {
    Remove-Item -Recurse -Force -Path $tmp -ErrorAction SilentlyContinue
}

Write-Host "Minted throwaway TLS fixtures in $dir"
& openssl x509 -in $serverCert -noout -subject -issuer -ext subjectAltName
