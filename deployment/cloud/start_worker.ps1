[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$MasterHost,
    [int]$WorkerPort = 8080,
    [string]$Capability = "GENERAL",
    [bool]$Permanent = $true,
    [int]$MasterPort = 9090
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$jar = Join-Path $root "Worker.jar"
$venv = Join-Path $root ".venv"
$venvPython = Join-Path $venv "Scripts/python.exe"

if (-not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw "Missing $jar" }
if (-not (Test-Path -LiteralPath (Join-Path $root "config.properties") -PathType Leaf)) { throw "Missing config.properties" }
if (-not (Get-Command java -ErrorAction SilentlyContinue)) { throw "Java 17+ is required." }
$python = Get-Command python -ErrorAction SilentlyContinue
$pyLauncher = Get-Command py -ErrorAction SilentlyContinue
if (-not $python -and -not $pyLauncher) { throw "Python 3 is required." }

if (-not (Test-Path -LiteralPath $venvPython)) {
    Write-Host "[SETUP] Creating worker virtual environment..."
    if ($python) { & $python.Source -m venv $venv }
    else { & $pyLauncher.Source -3 -m venv $venv }
    if ($LASTEXITCODE -ne 0) { throw "Could not create Python virtual environment." }
}
$ready = Join-Path $venv ".titan-sdk-ready"
if (-not (Test-Path -LiteralPath $ready)) {
    Write-Host "[SETUP] Installing Titan SDK..."
    & $venvPython -m pip install --quiet -e $root
    if ($LASTEXITCODE -ne 0) { throw "Titan SDK installation failed." }
    New-Item -ItemType File -Path $ready -Force | Out-Null
}

$env:PATH = "$(Join-Path $venv 'Scripts')$([IO.Path]::PathSeparator)$env:PATH"
$env:TITAN_HOST = $MasterHost
$env:TITAN_PORT = [string]$MasterPort
$permanentArg = if ($Permanent) { "true" } else { "false" }

Write-Host "[START] Titan Worker $WorkerPort -> ${MasterHost}:$MasterPort ($Capability, permanent=$permanentArg)"
Push-Location $root
try {
    & java -jar $jar $WorkerPort $MasterHost $MasterPort $Capability $permanentArg
    exit $LASTEXITCODE
} finally {
    Pop-Location
}

