param(
    [Alias('Host')]
    [string]$ServerHost = "127.0.0.1",
    [int]$Port = 8080,
    [switch]$SkipCertBootstrap,
    [Alias('?')]
    [switch]$Help
)

$ErrorActionPreference = "Stop"

if ($Help) {
    Write-Host "Usage: .\\scripts\\start-server.ps1 [-ServerHost <ip>] [-Port <port>] [-SkipCertBootstrap]"
    return
}

$nodeCmd = Get-Command node -ErrorAction SilentlyContinue
if (-not $nodeCmd) {
    throw "Node.js was not found in PATH. Install Node.js or add it to PATH."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$caPemPath = Join-Path $repoRoot ".mitm-proxy\certs\ca.pem"

Push-Location $repoRoot
try {
    if (-not $SkipCertBootstrap -and -not (Test-Path -LiteralPath $caPemPath)) {
        Write-Host "MITM CA not found. Generating certificate authority files..."
        & node "scripts/generate-cert.js"
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to generate MITM certificate authority files."
        }
    }

    $env:PROXY_HOST = $ServerHost
    $env:PROXY_PORT = "$Port"

    Write-Host "Starting server on ${ServerHost}:$Port"
    & node "server/server.js"
    if ($LASTEXITCODE -ne 0) {
        throw "Server exited with code $LASTEXITCODE."
    }
} finally {
    Pop-Location
}