[CmdletBinding()]
param(
    [ValidateSet("start", "stop", "status")]
    [string]$Action = "start"
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$perm = Join-Path $root "perm_files"
$venv = Join-Path $root ".venv"
$pidDir = Join-Path $root ".pids"

function Get-PidPath([string]$Name) { Join-Path $pidDir "$Name.pid" }

function Stop-RecordedProcess([string]$Name) {
    $path = Get-PidPath $Name
    if (-not (Test-Path -LiteralPath $path)) { return }
    $text = (Get-Content -LiteralPath $path -Raw).Trim()
    if ($text -match '^\d+$') {
        $process = Get-Process -Id ([int]$text) -ErrorAction SilentlyContinue
        if ($process) {
            Stop-Process -Id $process.Id -ErrorAction SilentlyContinue
            $process.WaitForExit(5000) | Out-Null
        }
    }
    Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
}

function Show-Status {
    $allRunning = $true
    foreach ($name in @("store", "master", "dashboard")) {
        $path = Get-PidPath $name
        $pidValue = if (Test-Path -LiteralPath $path) { (Get-Content -LiteralPath $path -Raw).Trim() } else { "" }
        $process = if ($pidValue -match '^\d+$') { Get-Process -Id ([int]$pidValue) -ErrorAction SilentlyContinue } else { $null }
        if ($process) { Write-Host "[OK] $name running (PID $pidValue)" -ForegroundColor Green }
        else { Write-Host "[DOWN] $name" -ForegroundColor Red; $allRunning = $false }
    }
    return $allRunning
}

if ($Action -eq "stop") {
    Stop-RecordedProcess "dashboard"
    Stop-RecordedProcess "master"
    Stop-RecordedProcess "store"
    Write-Host "[OK] Titan Master bundle stopped."
    exit 0
}
if ($Action -eq "status") {
    if (Show-Status) { exit 0 } else { exit 1 }
}

foreach ($required in @(
    (Join-Path $perm "TitanStore.jar"),
    (Join-Path $perm "titan-orchestrator-1.0-SNAPSHOT.jar"),
    (Join-Path $perm "server_dashboard.py"),
    (Join-Path $root "config.properties")
)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) { throw "Missing required file: $required" }
}
if (-not (Get-Command java -ErrorAction SilentlyContinue)) { throw "Java 17+ is required." }
$python = Get-Command python -ErrorAction SilentlyContinue
$pyLauncher = Get-Command py -ErrorAction SilentlyContinue
if (-not $python -and -not $pyLauncher) { throw "Python 3 is required." }

New-Item -ItemType Directory -Path $pidDir, (Join-Path $root "uploads"), (Join-Path $root "titan_workspace"), (Join-Path $root "telemetry") -Force | Out-Null
$venvPython = Join-Path $venv "Scripts/python.exe"
if (-not (Test-Path -LiteralPath $venvPython)) {
    Write-Host "[SETUP] Creating dashboard virtual environment..."
    if ($python) { & $python.Source -m venv $venv }
    else { & $pyLauncher.Source -3 -m venv $venv }
    if ($LASTEXITCODE -ne 0) { throw "Could not create Python virtual environment." }
}
$ready = Join-Path $venv ".titan-dependencies-ready"
if (-not (Test-Path -LiteralPath $ready)) {
    Write-Host "[SETUP] Installing dashboard and SDK dependencies..."
    & $venvPython -m pip install --quiet flask requests pyyaml
    if ($LASTEXITCODE -ne 0) { throw "Dashboard dependency installation failed." }
    & $venvPython -m pip install --quiet -e $root
    if ($LASTEXITCODE -ne 0) { throw "Titan SDK installation failed." }
    New-Item -ItemType File -Path $ready -Force | Out-Null
}

Stop-RecordedProcess "dashboard"
Stop-RecordedProcess "master"
Stop-RecordedProcess "store"

$store = Start-Process -FilePath "java" -ArgumentList @("-jar", (Join-Path $perm "TitanStore.jar")) `
    -WorkingDirectory $root -RedirectStandardOutput (Join-Path $root "store.log") `
    -RedirectStandardError (Join-Path $root "store.err.log") -WindowStyle Hidden -PassThru
$store.Id | Set-Content -LiteralPath (Get-PidPath "store") -Encoding ascii
Start-Sleep -Seconds 2

$master = Start-Process -FilePath "java" -ArgumentList @("-cp", (Join-Path $perm "titan-orchestrator-1.0-SNAPSHOT.jar"), "titan.TitanMaster") `
    -WorkingDirectory $root -RedirectStandardOutput (Join-Path $root "master.log") `
    -RedirectStandardError (Join-Path $root "master.err.log") -WindowStyle Hidden -PassThru
$master.Id | Set-Content -LiteralPath (Get-PidPath "master") -Encoding ascii
Start-Sleep -Seconds 2

$previousPythonPath = $env:PYTHONPATH
$env:PYTHONPATH = "$root$([IO.Path]::PathSeparator)$perm"
$dashboard = Start-Process -FilePath $venvPython -ArgumentList @((Join-Path $perm "server_dashboard.py")) `
    -WorkingDirectory $root -RedirectStandardOutput (Join-Path $root "dashboard.log") `
    -RedirectStandardError (Join-Path $root "dashboard.err.log") -WindowStyle Hidden -PassThru
$dashboard.Id | Set-Content -LiteralPath (Get-PidPath "dashboard") -Encoding ascii
$env:PYTHONPATH = $previousPythonPath
Start-Sleep -Seconds 2

if (-not (Show-Status)) { throw "One or more services failed. Inspect the log files in $root." }
Write-Host "[READY] Master 9090; dashboard 127.0.0.1:5000" -ForegroundColor Green

