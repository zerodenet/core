#!/usr/bin/env pwsh

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory
)

$ErrorActionPreference = 'Stop'

# Wintun is pinned so a release rebuild downloads the same signed upstream
# distribution. Update all three values together after reviewing a new release.
$WintunVersion = '0.14.1'
$WintunArchiveSha256 = '07c256185d6ee3652e09fa55c0b673e2624b565e02c4b9091c79ca7d2f24ef51'
$WintunArchiveUrl = "https://www.wintun.net/builds/wintun-$WintunVersion.zip"

$TemporaryDirectory = Join-Path ([System.IO.Path]::GetTempPath()) ("zero-wintun-" + [guid]::NewGuid().ToString('N'))
$ArchivePath = Join-Path $TemporaryDirectory 'wintun.zip'
$ExtractedDirectory = Join-Path $TemporaryDirectory 'extracted'

try {
    [System.IO.Directory]::CreateDirectory($TemporaryDirectory) | Out-Null
    Invoke-WebRequest -Uri $WintunArchiveUrl -OutFile $ArchivePath

    $ActualSha256 = (Get-FileHash -LiteralPath $ArchivePath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($ActualSha256 -ne $WintunArchiveSha256) {
        throw "Wintun $WintunVersion archive checksum mismatch: expected $WintunArchiveSha256, got $ActualSha256"
    }

    Expand-Archive -LiteralPath $ArchivePath -DestinationPath $ExtractedDirectory
    $DistributionRoot = Join-Path $ExtractedDirectory 'wintun'
    $DllSource = Join-Path $DistributionRoot 'bin/amd64/wintun.dll'
    $LicenseSource = Join-Path $DistributionRoot 'LICENSE.txt'
    if (-not (Test-Path -LiteralPath $DllSource -PathType Leaf)) {
        throw "Wintun $WintunVersion archive does not contain bin/amd64/wintun.dll"
    }
    if (-not (Test-Path -LiteralPath $LicenseSource -PathType Leaf)) {
        throw "Wintun $WintunVersion archive does not contain LICENSE.txt"
    }

    [System.IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
    Copy-Item -LiteralPath $DllSource -Destination (Join-Path $OutputDirectory 'wintun.dll') -Force
    Copy-Item -LiteralPath $LicenseSource -Destination (Join-Path $OutputDirectory 'wintun-LICENSE.txt') -Force

    Write-Host "Staged Wintun $WintunVersion amd64 runtime (archive SHA-256 $WintunArchiveSha256)"
}
finally {
    if ([System.IO.Directory]::Exists($TemporaryDirectory)) {
        [System.IO.Directory]::Delete($TemporaryDirectory, $true)
    }
}
