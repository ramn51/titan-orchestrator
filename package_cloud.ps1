[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [string]$OutputDirectory = "dist/cloud",
    [string]$MavenPath = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$output = [System.IO.Path]::GetFullPath((Join-Path $root $OutputDirectory))
$stage = Join-Path $output "_staging"
$jar = Join-Path $root "target/titan-orchestrator-1.0-SNAPSHOT.jar"
$storeJar = Join-Path $root "perm_files/TitanStore.jar"
$cloudFiles = Join-Path $root "deployment/cloud"

function Resolve-Maven {
    if ($MavenPath) {
        if (-not (Test-Path -LiteralPath $MavenPath -PathType Leaf)) {
            throw "Maven executable not found: $MavenPath"
        }
        return (Resolve-Path -LiteralPath $MavenPath).Path
    }

    $command = Get-Command mvn.cmd -ErrorAction SilentlyContinue
    if (-not $command) { $command = Get-Command mvn -ErrorAction SilentlyContinue }
    if ($command) { return $command.Source }

    $wrapper = Join-Path $root "mvnw.cmd"
    if (Test-Path -LiteralPath $wrapper -PathType Leaf) { return $wrapper }

    $wrapperCache = Join-Path $HOME ".m2/wrapper/dists"
    if (Test-Path -LiteralPath $wrapperCache -PathType Container) {
        $cached = Get-ChildItem -LiteralPath $wrapperCache -Filter mvn.cmd -File -Recurse |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -First 1
        if ($cached) { return $cached.FullName }
    }

    throw "Maven was not found. Install Maven, provide -MavenPath, or use -SkipBuild with a current target JAR."
}

function Assert-File([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Required file not found: $Path"
    }
}

function Copy-Directory([string]$Source, [string]$Destination) {
    if (-not (Test-Path -LiteralPath $Source -PathType Container)) {
        throw "Required directory not found: $Source"
    }
    Copy-Item -LiteralPath $Source -Destination $Destination -Recurse -Force
}

Write-Host "Titan cloud bundle packager" -ForegroundColor Cyan
Write-Host "Source: $root"

if (-not $SkipBuild) {
    $maven = Resolve-Maven
    Write-Host "Building the current engine JAR..." -ForegroundColor Yellow
    Push-Location $root
    try {
        & $maven -q -DskipTests package
        if ($LASTEXITCODE -ne 0) { throw "Maven package failed with exit code $LASTEXITCODE." }
    } finally {
        Pop-Location
    }
} elseif (Test-Path -LiteralPath $jar) {
    $jarTime = (Get-Item -LiteralPath $jar).LastWriteTimeUtc
    $newerSource = Get-ChildItem -LiteralPath (Join-Path $root "src/main/java") -Filter *.java -File -Recurse |
        Where-Object LastWriteTimeUtc -gt $jarTime |
        Select-Object -First 1
    if ($newerSource -or (Get-Item -LiteralPath (Join-Path $root "pom.xml")).LastWriteTimeUtc -gt $jarTime) {
        throw "The target JAR is older than engine source. Run without -SkipBuild so the bundle cannot contain stale code."
    }
}

Assert-File $jar
Assert-File $storeJar
Assert-File (Join-Path $cloudFiles "start_master.sh")
Assert-File (Join-Path $cloudFiles "start_worker.sh")
Assert-File (Join-Path $cloudFiles "start_master.ps1")
Assert-File (Join-Path $cloudFiles "start_worker.ps1")

New-Item -ItemType Directory -Path $output -Force | Out-Null
if (Test-Path -LiteralPath $stage) {
    $resolvedOutput = [System.IO.Path]::GetFullPath($output).TrimEnd('\') + '\'
    $resolvedStage = [System.IO.Path]::GetFullPath($stage)
    if (-not $resolvedStage.StartsWith($resolvedOutput, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to clear staging outside output directory: $resolvedStage"
    }
    Remove-Item -LiteralPath $stage -Recurse -Force
}
New-Item -ItemType Directory -Path $stage | Out-Null

$master = Join-Path $stage "titan-master-bundle"
$masterPerm = Join-Path $master "perm_files"
$worker = Join-Path $stage "titan-worker-bundle"
New-Item -ItemType Directory -Path $masterPerm, $worker -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $master "uploads"), (Join-Path $master "titan_workspace"), (Join-Path $master "telemetry") -Force | Out-Null

# Master engine, store, SDK, and modular dashboard.
Copy-Item -LiteralPath $jar -Destination (Join-Path $masterPerm "titan-orchestrator-1.0-SNAPSHOT.jar")
Copy-Item -LiteralPath $jar -Destination (Join-Path $masterPerm "Worker.jar")
Copy-Item -LiteralPath $storeJar -Destination (Join-Path $masterPerm "TitanStore.jar")
Copy-Item -LiteralPath (Join-Path $root "perm_files/server_dashboard.py") -Destination $masterPerm
foreach ($directory in @("dag_viewer", "cluster_monitor", "templates", "static", "dag_samples")) {
    Copy-Directory (Join-Path $root "perm_files/$directory") $masterPerm
}
Copy-Directory (Join-Path $root "titan_sdk") $master
Copy-Item -LiteralPath (Join-Path $root "setup.py") -Destination $master
Copy-Item -LiteralPath (Join-Path $cloudFiles "start_master.sh") -Destination $master
Copy-Item -LiteralPath (Join-Path $cloudFiles "start_master.ps1") -Destination $master
Copy-Item -LiteralPath (Join-Path $cloudFiles "config.master.properties") -Destination (Join-Path $master "config.properties")

# Worker engine and SDK. The worker startup script prepends its virtual environment
# to PATH so ScriptExecutorHandler's `python` command resolves consistently.
Copy-Item -LiteralPath $jar -Destination (Join-Path $worker "Worker.jar")
Copy-Directory (Join-Path $root "titan_sdk") $worker
Copy-Item -LiteralPath (Join-Path $root "setup.py") -Destination $worker
Copy-Item -LiteralPath (Join-Path $cloudFiles "start_worker.sh") -Destination $worker
Copy-Item -LiteralPath (Join-Path $cloudFiles "start_worker.ps1") -Destination $worker
Copy-Item -LiteralPath (Join-Path $cloudFiles "config.worker.properties") -Destination (Join-Path $worker "config.properties")

Get-ChildItem -LiteralPath $stage -Directory -Recurse -Filter __pycache__ | Remove-Item -Recurse -Force
Get-ChildItem -LiteralPath $stage -File -Recurse -Include *.pyc,*.pyo | Remove-Item -Force

$branch = (& git -C $root branch --show-current).Trim()
$commit = (& git -C $root rev-parse HEAD).Trim()
$hash = (Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash
$buildInfo = @"
Titan cloud bundle
Branch: $branch
Commit: $commit
Engine SHA-256: $hash
Packaged UTC: $([DateTime]::UtcNow.ToString("o"))
"@
Set-Content -LiteralPath (Join-Path $master "BUILD_INFO.txt") -Value $buildInfo -Encoding utf8NoBOM
Set-Content -LiteralPath (Join-Path $worker "BUILD_INFO.txt") -Value $buildInfo -Encoding utf8NoBOM

# Preserve otherwise-empty runtime directories inside ZIP archives.
Set-Content -LiteralPath (Join-Path $master "uploads/.gitkeep") -Value "" -Encoding ascii
Set-Content -LiteralPath (Join-Path $master "titan_workspace/.gitkeep") -Value "" -Encoding ascii
Set-Content -LiteralPath (Join-Path $master "telemetry/.gitkeep") -Value "" -Encoding ascii

$masterZip = Join-Path $output "titan-master-bundle.zip"
$workerZip = Join-Path $output "titan-worker-bundle.zip"
Remove-Item -LiteralPath $masterZip, $workerZip -Force -ErrorAction SilentlyContinue
Compress-Archive -LiteralPath $master -DestinationPath $masterZip -CompressionLevel Optimal
Compress-Archive -LiteralPath $worker -DestinationPath $workerZip -CompressionLevel Optimal
Remove-Item -LiteralPath $stage -Recurse -Force

Write-Host "Bundles ready" -ForegroundColor Green
Get-Item -LiteralPath $masterZip, $workerZip |
    Select-Object FullName, Length, @{Name="SHA256";Expression={(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash}} |
    Format-Table -AutoSize
