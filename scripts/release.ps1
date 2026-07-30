#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Manage the Zero version contract and publish a sealed release.

.DESCRIPTION
    During development, compatibility changes remain under Unreleased. A
    normal release moves that entry to the requested version, updates the
    workspace package version, commits both files, creates an annotated tag,
    and pushes the branch and tag to every configured remote.

.EXAMPLE
    ./scripts/release.ps1 -Check
    ./scripts/release.ps1 -Version 0.0.16 -DryRun
    ./scripts/release.ps1 -Version 0.0.16 -NoPush
    ./scripts/release.ps1 -Version 0.0.17-dev -StartDevelopment
#>

param(
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$Version,

    [switch]$DryRun,
    [switch]$NoPush,
    [string]$Message,
    [switch]$Check,
    [switch]$CheckRelease,
    [switch]$StartDevelopment,

    [Parameter(DontShow = $true)]
    [switch]$SealOnly
)

$ErrorActionPreference = "Stop"
$repoRoot = if ($env:ZERO_REPO_ROOT) { $env:ZERO_REPO_ROOT } else { Join-Path $PSScriptRoot ".." }
Set-Location $repoRoot

$cargoTomlPath = "Cargo.toml"
$breakingChangesPath = "release/breaking-changes.md"
$rowMarker = "<!-- version-contract:unreleased-row -->"
$emptyRow = "| ``Unreleased`` | — | 暂无待发布的兼容性变更 $rowMarker |"
$emptyBodyComment = "<!-- 在这里登记已实现但尚未封板的兼容性变更。 -->"
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)

function Read-Utf8([string]$Path) {
    return [System.IO.File]::ReadAllText((Join-Path (Get-Location) $Path), [System.Text.Encoding]::UTF8)
}

function Write-Utf8([string]$Path, [string]$Content) {
    [System.IO.File]::WriteAllText((Join-Path (Get-Location) $Path), $Content, $utf8NoBom)
}

function Assert-Version([string]$Value, [bool]$Development) {
    if ($Value -notmatch '^\d+\.\d+\.\d+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$') {
        throw "Invalid version '$Value'; expected X.Y.Z or X.Y.Z-suffix."
    }
    $isDevelopment = $Value.EndsWith("-dev", [System.StringComparison]::Ordinal)
    if ($Development -and -not $isDevelopment) {
        throw "Development version must end with '-dev'."
    }
    if (-not $Development -and $isDevelopment) {
        throw "Release version must not end with '-dev'."
    }
}

function Get-WorkspaceVersion([string]$CargoContent) {
    $section = [regex]::Match($CargoContent, '(?ms)^\[workspace\.package\]\s*\r?\n(?<body>.*?)(?=^\[|\z)')
    if (-not $section.Success) {
        throw "Cargo.toml has no [workspace.package] section."
    }
    $match = [regex]::Match($section.Groups['body'].Value, '(?m)^version\s*=\s*"(?<version>[^"]+)"')
    if (-not $match.Success) {
        throw "[workspace.package] has no version field."
    }
    return $match.Groups['version'].Value
}

function Set-WorkspaceVersion([string]$CargoContent, [string]$Value) {
    $pattern = '(?ms)(^\[workspace\.package\]\s*\r?\n.*?^version\s*=\s*")[^"]+(".*?$)'
    $match = [regex]::Match($CargoContent, $pattern)
    if (-not $match.Success) {
        throw "Failed to locate workspace package version."
    }
    return $CargoContent.Substring(0, $match.Index) +
        $match.Groups[1].Value + $Value + $match.Groups[2].Value +
        $CargoContent.Substring($match.Index + $match.Length)
}

function Get-UnreleasedRow([string]$BreakingContent) {
    $pattern = '(?m)^\| `Unreleased` \|[^\r\n]*' + [regex]::Escape($rowMarker) + '[^\r\n]*\r?$'
    $matches = [regex]::Matches($BreakingContent, $pattern)
    if ($matches.Count -ne 1) {
        throw "Compatibility matrix must contain exactly one marked Unreleased row."
    }
    return $matches[0].Value.TrimEnd("`r")
}

function Get-UnreleasedBody([string]$BreakingContent) {
    $matches = [regex]::Matches($BreakingContent, '(?ms)^## Unreleased\r?\n(?<body>.*?)(?=^## |\z)')
    if ($matches.Count -ne 1) {
        throw "Breaking changes must contain exactly one '## Unreleased' section."
    }
    return $matches[0].Groups['body'].Value
}

function Test-SubstantiveBody([string]$Body) {
    return ([regex]::Replace($Body, '<!--.*?-->', '', 'Singleline')).Trim().Length -gt 0
}

function Assert-DevelopmentContract([string]$CargoContent, [string]$BreakingContent) {
    $currentVersion = Get-WorkspaceVersion $CargoContent
    Assert-Version $currentVersion $true
    [void](Get-UnreleasedRow $BreakingContent)
    [void](Get-UnreleasedBody $BreakingContent)
    if ($BreakingContent.IndexOf($currentVersion, [System.StringComparison]::Ordinal) -ge 0) {
        throw "Development version '$currentVersion' must not be bound into the compatibility ledger."
    }
    return $currentVersion
}

function Assert-UnsealedContract([string]$CargoContent, [string]$BreakingContent) {
    $currentVersion = Get-WorkspaceVersion $CargoContent
    if ($currentVersion.EndsWith("-dev", [System.StringComparison]::Ordinal)) {
        Assert-Version $currentVersion $true
        if ($BreakingContent.IndexOf($currentVersion, [System.StringComparison]::Ordinal) -ge 0) {
            throw "Development version '$currentVersion' must not be bound into the compatibility ledger."
        }
    }
    else {
        Assert-Version $currentVersion $false
    }
    [void](Get-UnreleasedRow $BreakingContent)
    [void](Get-UnreleasedBody $BreakingContent)
    return $currentVersion
}

function Assert-ReleaseContract(
    [string]$CargoContent,
    [string]$BreakingContent,
    [string]$ReleaseVersion
) {
    Assert-Version $ReleaseVersion $false
    $currentVersion = Get-WorkspaceVersion $CargoContent
    if ($currentVersion -ne $ReleaseVersion) {
        throw "Cargo workspace version '$currentVersion' does not match release '$ReleaseVersion'."
    }
    if ((Get-UnreleasedRow $BreakingContent) -ne $emptyRow) {
        throw "Release requires an empty Unreleased matrix row."
    }
    if (Test-SubstantiveBody (Get-UnreleasedBody $BreakingContent)) {
        throw "Release requires an empty Unreleased section."
    }
    if ($BreakingContent -notmatch ('(?m)^## ' + [regex]::Escape($ReleaseVersion) + '\r?$')) {
        throw "Breaking changes has no release section for '$ReleaseVersion'."
    }
    if ($BreakingContent -notmatch ('(?m)^\| `' + [regex]::Escape($ReleaseVersion) + '` \|')) {
        throw "Compatibility matrix has no release row for '$ReleaseVersion'."
    }
}

function Prepare-ReleaseContract(
    [string]$CargoContent,
    [string]$BreakingContent,
    [string]$ReleaseVersion
) {
    [void](Assert-UnsealedContract $CargoContent $BreakingContent)
    Assert-Version $ReleaseVersion $false
    if ($BreakingContent -match ('(?m)^## ' + [regex]::Escape($ReleaseVersion) + '\r?$')) {
        throw "Release '$ReleaseVersion' already exists in breaking changes."
    }

    $unreleasedBody = Get-UnreleasedBody $BreakingContent
    if (-not (Test-SubstantiveBody $unreleasedBody)) {
        throw "Cannot prepare a release with an empty Unreleased section."
    }
    $unreleasedRow = Get-UnreleasedRow $BreakingContent
    $releasedRow = $unreleasedRow.Replace('`Unreleased`', "``$ReleaseVersion``").Replace($rowMarker, '')
    $releasedRow = [regex]::Replace($releasedRow, '\s+\|$', ' |')
    $newline = if ($BreakingContent.Contains("`r`n")) { "`r`n" } else { "`n" }

    $rowPattern = '(?m)^\| `Unreleased` \|[^\r\n]*' + [regex]::Escape($rowMarker) + '[^\r\n]*\r?$'
    $rowReplacement = $emptyRow + $newline + $releasedRow
    $nextBreaking = [regex]::Replace(
        $BreakingContent,
        $rowPattern,
        [System.Text.RegularExpressions.MatchEvaluator]{ param($match) $rowReplacement },
        1
    )
    $headingReplacement = "## Unreleased${newline}${newline}${emptyBodyComment}${newline}${newline}## $ReleaseVersion"
    $nextBreaking = [regex]::Replace(
        $nextBreaking,
        '(?m)^## Unreleased\r?$',
        [System.Text.RegularExpressions.MatchEvaluator]{ param($match) $headingReplacement },
        1
    )
    $nextCargo = Set-WorkspaceVersion $CargoContent $ReleaseVersion
    Assert-ReleaseContract $nextCargo $nextBreaking $ReleaseVersion
    return @($nextCargo, $nextBreaking)
}

function Assert-CleanTree {
    $gitStatus = git status --porcelain
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to inspect Git working tree."
    }
    if ($gitStatus) {
        throw "Working tree is not clean. Commit or stash changes before changing versions."
    }
}

if (-not (Test-Path $cargoTomlPath) -or -not (Test-Path $breakingChangesPath)) {
    throw "Cargo.toml or breaking-changes.md was not found in '$repoRoot'."
}

$cargoContent = Read-Utf8 $cargoTomlPath
$breakingContent = Read-Utf8 $breakingChangesPath

if ($Check) {
    $currentVersion = Get-WorkspaceVersion $cargoContent
    if ($currentVersion.EndsWith("-dev")) {
        [void](Assert-DevelopmentContract $cargoContent $breakingContent)
        Write-Host "Development contract is valid ($currentVersion, Unreleased)." -ForegroundColor Green
    }
    elseif ((Get-UnreleasedRow $breakingContent) -ne $emptyRow -or
        (Test-SubstantiveBody (Get-UnreleasedBody $breakingContent))) {
        [void](Assert-UnsealedContract $cargoContent $breakingContent)
        Write-Host "Unsealed release contract is valid ($currentVersion, Unreleased)." -ForegroundColor Green
    }
    else {
        Assert-ReleaseContract $cargoContent $breakingContent $currentVersion
        Write-Host "Release contract is valid ($currentVersion)." -ForegroundColor Green
    }
    exit 0
}

if ($CheckRelease) {
    if (-not $Version) { throw "-CheckRelease requires -Version." }
    Assert-ReleaseContract $cargoContent $breakingContent $Version
    Write-Host "Release contract is valid ($Version)." -ForegroundColor Green
    exit 0
}

if ($StartDevelopment) {
    if (-not $Version) { throw "-StartDevelopment requires -Version X.Y.Z-dev." }
    Assert-Version $Version $true
    $currentVersion = Get-WorkspaceVersion $cargoContent
    if ($currentVersion.EndsWith("-dev")) {
        [void](Assert-DevelopmentContract $cargoContent $breakingContent)
    }
    elseif ((Get-UnreleasedRow $breakingContent) -ne $emptyRow -or
        (Test-SubstantiveBody (Get-UnreleasedBody $breakingContent))) {
        [void](Assert-UnsealedContract $cargoContent $breakingContent)
    }
    else {
        Assert-ReleaseContract $cargoContent $breakingContent $currentVersion
    }
    $nextCargo = Set-WorkspaceVersion $cargoContent $Version
    [void](Assert-DevelopmentContract $nextCargo $breakingContent)
    if ($DryRun) {
        Write-Host "[DRY RUN] Cargo version: $currentVersion -> $Version" -ForegroundColor Green
    }
    else {
        Assert-CleanTree
        Write-Utf8 $cargoTomlPath $nextCargo
        Write-Host "Development contract opened for $Version." -ForegroundColor Green
    }
    exit 0
}

if (-not $Version) {
    throw "Version is required for a release (for example: -Version 0.0.16)."
}
Assert-Version $Version $false
$prepared = Prepare-ReleaseContract $cargoContent $breakingContent $Version

if ($SealOnly) {
    if ($DryRun) {
        Write-Host "[DRY RUN] Would seal Unreleased and Cargo as $Version." -ForegroundColor Green
    }
    else {
        Write-Utf8 $cargoTomlPath $prepared[0]
        Write-Utf8 $breakingChangesPath $prepared[1]
        Write-Host "Release contract sealed for $Version." -ForegroundColor Green
    }
    exit 0
}

Assert-CleanTree
$tagName = "v$Version"
$Message = if ($Message) { $Message } else { "release: v$Version" }
$currentBranch = git rev-parse --abbrev-ref HEAD
$remotes = (git remote) | ForEach-Object { $_.Trim() } | Where-Object { $_ }
if (-not $remotes) {
    throw "No Git remotes are configured."
}

Write-Host "Current branch: $currentBranch" -ForegroundColor Cyan
Write-Host "Cargo version: $(Get-WorkspaceVersion $cargoContent) -> $Version" -ForegroundColor Yellow
Write-Host "Tag: $tagName" -ForegroundColor Yellow
Write-Host "Remotes: $($remotes -join ', ')" -ForegroundColor Cyan

if ($DryRun) {
    Write-Host "[DRY RUN] Would seal Cargo and breaking changes, commit, tag $tagName, and push to: $($remotes -join ', ')" -ForegroundColor Green
    exit 0
}

$confirm = Read-Host "Proceed with release v$Version? [y/N]"
if ($confirm -notmatch '^[yY]') {
    Write-Host "Aborted." -ForegroundColor Red
    exit 0
}

Write-Utf8 $cargoTomlPath $prepared[0]
Write-Utf8 $breakingChangesPath $prepared[1]
Assert-ReleaseContract (Read-Utf8 $cargoTomlPath) (Read-Utf8 $breakingChangesPath) $Version

git add Cargo.toml release/breaking-changes.md
git commit -m $Message
git tag -a $tagName -m $Message

if (-not $NoPush) {
    foreach ($remote in $remotes) {
        git push $remote $currentBranch
        git push $remote $tagName
        Write-Host "${remote}: pushed ${currentBranch} + ${tagName}" -ForegroundColor Green
    }
}
else {
    Write-Host "Skipped push (-NoPush). Commit and tag are local only." -ForegroundColor Yellow
}
