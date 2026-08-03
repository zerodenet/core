#!/usr/bin/env pwsh
<#
.SYNOPSIS
    PowerShell wrapper for the canonical Bash release policy.

.DESCRIPTION
    The release state machine is implemented once in scripts/release.sh.
    This wrapper preserves the Windows command surface and forwards every
    operation to Git for Windows Bash, preventing the Bash and PowerShell
    implementations from drifting apart.
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
    [switch]$SealOnly,
    [switch]$AllowGap,
    [string]$Remote = "origin",
    [string]$Next,
    [ValidateSet("patch", "minor", "major")]
    [string]$Bump = "patch",
    [string]$CheckTransition,
    [string]$HeadRef = "HEAD",
    [string]$VerifyTag
)

$ErrorActionPreference = "Stop"
$repoRoot = if ($env:ZERO_REPO_ROOT) {
    $env:ZERO_REPO_ROOT
}
else {
    (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Find-Bash {
    $command = Get-Command bash -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }

    $candidates = @(
        "$env:ProgramFiles\Git\bin\bash.exe",
        "$env:ProgramFiles\Git\usr\bin\bash.exe",
        "${env:ProgramFiles(x86)}\Git\bin\bash.exe"
    )
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path $candidate)) {
            return $candidate
        }
    }

    throw "Git Bash was not found. Install Git for Windows or add bash to PATH."
}

$arguments = [System.Collections.Generic.List[string]]::new()

if ($Check) {
    $arguments.Add("--check")
}
elseif ($CheckTransition) {
    $arguments.Add("--check-transition")
    $arguments.Add($CheckTransition)
    $arguments.Add($HeadRef)
}
elseif ($VerifyTag) {
    $arguments.Add("--verify-tag")
    $arguments.Add($VerifyTag)
}
elseif ($Next) {
    $arguments.Add("--next")
    $arguments.Add($Next)
    $arguments.Add("--bump")
    $arguments.Add($Bump)
}
else {
    if ($Version) { $arguments.Add($Version) }
    if ($CheckRelease) { $arguments.Add("--check-release") }
    if ($StartDevelopment) { $arguments.Add("--start-development") }
    if ($SealOnly) { $arguments.Add("--seal-only") }
    if ($DryRun) { $arguments.Add("--dry-run") }
    if ($NoPush) { $arguments.Add("--no-push") }
    if ($AllowGap) { $arguments.Add("--allow-gap") }
    if ($Message) {
        $arguments.Add("--message")
        $arguments.Add($Message)
    }
    if ($Remote) {
        $arguments.Add("--remote")
        $arguments.Add($Remote)
    }
}

$bash = Find-Bash
Push-Location $repoRoot
try {
    & $bash "scripts/release.sh" @arguments
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
