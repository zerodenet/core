[CmdletBinding()]
param(
    [string]$OutputRoot = ""
)

$ErrorActionPreference = "Stop"

$version = "v26.3.27"
$expectedArchiveSha256 = "d004c39288ce9ada487c6f398c7c545f7d749e44bdfdd59dbc9f865afba4e1ad"
$archiveName = "Xray-windows-64.zip"
$releaseUrl = "https://github.com/XTLS/Xray-core/releases/download/$version/$archiveName"

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $repositoryRoot = Split-Path -Parent $PSScriptRoot
    $OutputRoot = Join-Path $repositoryRoot "target\interop-tools"
}

$resolvedOutputRoot = [System.IO.Path]::GetFullPath($OutputRoot)
$installDirectory = Join-Path $resolvedOutputRoot "xray-$version"
$archivePath = Join-Path $installDirectory $archiveName
$binaryPath = Join-Path $installDirectory "xray.exe"

New-Item -ItemType Directory -Force -Path $installDirectory | Out-Null

if (-not (Test-Path -LiteralPath $archivePath -PathType Leaf)) {
    Write-Host "Downloading pinned Xray interop fixture $version..."
    Invoke-WebRequest -UseBasicParsing -Uri $releaseUrl -OutFile $archivePath
}

$actualArchiveSha256 = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actualArchiveSha256 -ne $expectedArchiveSha256) {
    throw "Xray archive checksum mismatch: expected $expectedArchiveSha256, got $actualArchiveSha256 at $archivePath"
}

if (-not (Test-Path -LiteralPath $binaryPath -PathType Leaf)) {
    Expand-Archive -LiteralPath $archivePath -DestinationPath $installDirectory -Force
}

$versionOutput = (& $binaryPath version 2>&1 | Out-String).Trim()
if ($LASTEXITCODE -ne 0) {
    throw "Xray version probe failed with exit code $LASTEXITCODE"
}
if ($versionOutput -notmatch "Xray 26\.3\.27") {
    throw "Unexpected Xray version output: $versionOutput"
}

Write-Host "Verified Xray interop fixture:"
Write-Host "  version: $version"
Write-Host "  archive sha256: $actualArchiveSha256"
Write-Host "  binary: $binaryPath"
Write-Output "XRAY_BIN=$binaryPath"
