param(
    [string]$Username,
    [string]$ServerHost = "127.0.0.1",
    [int]$ServerPort = 8080,
    [int]$ListenPort = 8899,
    [int]$AdminPort = 19081,
    [string]$GeoGuessrExe = "D:\Program Files (x86)\Steam\steamapps\common\GeoGuessr Duels\GeoGuessr.exe",
    [string]$ProfileDir,
    [switch]$CaptureJs
)

$ErrorActionPreference = "Stop"

function Test-PortAvailable {
    param([int]$Port)

    $listener = $null
    try {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $Port)
        $listener.Start()
        return $true
    } catch {
        return $false
    } finally {
        if ($listener) {
            $listener.Stop()
        }
    }
}

function Get-FreeListenPort {
    param(
        [int]$StartPort,
        [int]$MaxAttempts = 50
    )

    for ($i = 0; $i -lt $MaxAttempts; $i++) {
        $candidate = $StartPort + $i
        if (Test-PortAvailable -Port $candidate) {
            return $candidate
        }
    }

    throw "No free local proxy port found starting at $StartPort."
}

if (-not $Username) {
    $Username = Read-Host "Enter username"
}

$Username = "$Username".Trim()
if (-not $Username) {
    throw "Username is required."
}

if (-not (Test-Path -LiteralPath $GeoGuessrExe)) {
    throw "GeoGuessr executable not found: $GeoGuessrExe"
}

$nodeCmd = Get-Command node -ErrorAction SilentlyContinue
if (-not $nodeCmd) {
    throw "Node.js was not found in PATH. Install Node.js or add it to PATH."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

if (-not $ProfileDir) {
    $safeUser = ($Username -replace "[^a-zA-Z0-9._-]", "_")
    $baseProfileRoot = $env:LOCALAPPDATA
    if (-not $baseProfileRoot) {
        $baseProfileRoot = $repoRoot
    }
    $ProfileDir = Join-Path $baseProfileRoot ("RioGeo-Client-" + $safeUser)
}

$requestedPort = $ListenPort
$ListenPort = Get-FreeListenPort -StartPort $requestedPort
if ($ListenPort -ne $requestedPort) {
    Write-Host "Requested port $requestedPort is in use, using $ListenPort instead."
}

$clientArgs = @(
    "client/proxy.js",
    "--server", $ServerHost,
    "--server-port", "$ServerPort",
    "--admin-port", "$AdminPort",
    "--listen-port", "$ListenPort",
    "--username", $Username
)

if ($CaptureJs) {
    $clientArgs += "--capture-js"
}

$clientProcess = Start-Process -FilePath "node" -ArgumentList $clientArgs -WorkingDirectory $repoRoot -PassThru

$proxyValue = "http=127.0.0.1:$ListenPort;https=127.0.0.1:$ListenPort"
$appArgs = @(
    "--proxy-server=$proxyValue",
    "--disable-quic",
    "--proxy-bypass-list=<-loopback>",
    "--user-data-dir=$ProfileDir"
)

$appProcess = Start-Process -FilePath $GeoGuessrExe -ArgumentList $appArgs -PassThru

Write-Host "Started client proxy PID: $($clientProcess.Id)"
Write-Host "Started GeoGuessr PID: $($appProcess.Id)"
Write-Host "Username: $Username"
Write-Host "Server: ${ServerHost}:$ServerPort"
Write-Host "Local proxy: 127.0.0.1:$ListenPort"
Write-Host "Profile dir: $ProfileDir"
if ($CaptureJs) {
    Write-Host "JS capture: enabled (saved to raw\\)"
}
Write-Host ""
Write-Host "To stop the client proxy later: Stop-Process -Id $($clientProcess.Id)"
