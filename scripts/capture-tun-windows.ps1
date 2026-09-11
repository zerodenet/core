#!/usr/bin/env pwsh
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Start', 'Stop')]
    [string]$Mode,
    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory
)

$ErrorActionPreference = 'Stop'
$CapturePath = Join-Path $OutputDirectory 'tun.etl'
if ($Mode -eq 'Start') {
    New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
    pktmon filter remove
    if ($LASTEXITCODE -ne 0) { throw 'Cannot reset packet filters' }
    # Only the test's HTTP/TLS ports; avoid unrelated runner control traffic.
    foreach ($Port in @(80, 8080, 8443)) {
        pktmon filter add "zero-test-$Port" -t TCP -p $Port
        if ($LASTEXITCODE -ne 0) { throw "Cannot add TCP port $Port capture filter" }
    }
    pktmon start --capture --pkt-size 160 --file-size 128 --file-name $CapturePath
    if ($LASTEXITCODE -ne 0) { throw 'Cannot start TUN packet capture' }
} else {
    pktmon stop
    if ($LASTEXITCODE -ne 0) { throw 'Cannot stop TUN packet capture' }
    pktmon etl2pcap $CapturePath --out (Join-Path $OutputDirectory 'tun.pcapng')
    if ($LASTEXITCODE -ne 0) { throw 'Cannot convert TUN packet capture' }
    $TextPath = Join-Path $OutputDirectory 'tun.txt'
    pktmon etl2txt $CapturePath --out $TextPath --verbose 3
    if ($LASTEXITCODE -ne 0) { throw 'Cannot format TUN packet capture' }
    # Keep reset provenance visible in job logs; the complete capture is an artifact.
    # Prefer the end of the capture because a failed test exits immediately after
    # its unexpected reset, while earlier deliberate early-closes are expected.
    $ResetEvidence = @(
        Select-String -Path $TextPath -Pattern '8080.*Flags \[R|Flags \[R.*8080' |
            Select-Object -Last 16 | ForEach-Object { $_.Line.Trim() }
    )
    $ResetEvidence | ForEach-Object { Write-Output $_ }
    if ($env:GITHUB_ACTIONS -eq 'true' -and $ResetEvidence.Count -gt 0) {
        $Tail = ($ResetEvidence | Select-Object -Last 16) -join "`n"
        $Escaped = $Tail.Replace('%', '%25').Replace("`r", '%0D').Replace("`n", '%0A')
        Write-Output "::notice title=Windows TUN packet reset evidence::$Escaped"
    }
}
