param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('Enable', 'Collect')][string]$Mode,
    [Parameter(Mandatory=$true)][string]$OutputDirectory
)
$ErrorActionPreference = 'Stop'
$directory = New-Item -ItemType Directory -Force -Path $OutputDirectory
$key = 'HKLM:\SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps\zero.exe'
if ($Mode -eq 'Enable') {
    # Per executable, on the isolated Actions runner only. No stack limit,
    # exception handler, native owner or shutdown deadline is changed.
    New-Item -Path $key -Force | Out-Null
    New-ItemProperty -Path $key -Name DumpFolder -Value $directory.FullName -PropertyType ExpandString -Force | Out-Null
    New-ItemProperty -Path $key -Name DumpType -Value 1 -PropertyType DWord -Force | Out-Null
    New-ItemProperty -Path $key -Name DumpCount -Value 4 -PropertyType DWord -Force | Out-Null
    Write-Host 'Enabled original zero.exe native crash capture on this runner.'
    exit 0
}
try {
    $files = @()
    # Retain the actual matching image/PDB rather than using symbols rebuilt
    # from the same source later; exception context is not a Rust backtrace.
    if (Test-Path target/debug/zero.exe) {
        Copy-Item target/debug/zero.exe -Destination $directory.FullName -Force
    }
    if (Test-Path Cargo.lock) { Copy-Item Cargo.lock -Destination $directory.FullName -Force }
    $target = (Resolve-Path target/debug).Path
    Get-ChildItem target/debug -Filter 'zero*.pdb' -Recurse -File -ErrorAction SilentlyContinue | Where-Object { $_.Name -match '^zero(?:-[0-9a-f]+)?\.pdb$' } | ForEach-Object {
        $relative = [IO.Path]::GetRelativePath($target, $_.FullName)
        $destination = Join-Path (Join-Path $directory.FullName 'symbols') $relative
        New-Item -ItemType Directory -Force -Path (Split-Path $destination) | Out-Null
        Copy-Item $_.FullName -Destination $destination -Force
    }
    $debugger = Get-Command cdb.exe -ErrorAction SilentlyContinue
    if (!$debugger) {
        $debuggers = Join-Path ${env:ProgramFiles(x86)} 'Windows Kits\10\Debuggers'
        if (Test-Path $debuggers) {
            $debugger = Get-ChildItem $debuggers -Filter cdb.exe -Recurse -File | Where-Object { $_.Directory.Name -eq 'x64' } | Select-Object -First 1
        }
    }
    $dumps = @(Get-ChildItem $directory.FullName -Filter '*.dmp' -File)
    foreach ($dump in $dumps) {
        if ($debugger) {
            $program = if ($debugger.Source) { $debugger.Source } else { $debugger.FullName }
            & $program -z $dump.FullName -y $directory.FullName -c '.reload; .ecxr; kv; ~*k; q' 2>&1 | Out-File -Encoding utf8 (Join-Path $directory.FullName ($dump.BaseName + '.stack.txt'))
            Write-Host "Original crash debugger exit: $LASTEXITCODE"
        }
    }
    foreach ($file in (Get-ChildItem $directory.FullName -File -Recurse)) {
        $files += @{name=[IO.Path]::GetRelativePath($directory.FullName, $file.FullName); bytes=$file.Length; sha256=(Get-FileHash $file.FullName -Algorithm SHA256).Hash}
    }
    @{source=$env:GITHUB_SHA; run=$env:GITHUB_RUN_ID; attempt=$env:GITHUB_RUN_ATTEMPT; dump_count=$dumps.Count; files=$files} | ConvertTo-Json -Depth 5 | Out-File -Encoding utf8 (Join-Path $directory.FullName 'manifest.json')
    if (!$dumps.Count) { Write-Warning 'No native dump was produced; this is not evidence that a reported stack overflow was fixed.' }
} finally {
    Remove-Item -Path $key -Force -ErrorAction SilentlyContinue
}
