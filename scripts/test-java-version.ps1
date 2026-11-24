$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'java-version.ps1')
function java { "openjdk version `"$script:testVersion`"" }

foreach ($testVersion in @('21', '21.0.5', '25.0.5.1', '25-ea', '27+35')) {
    Assert-JavaVersion | Out-Null
}
foreach ($testVersion in @('1.8.0', '17.0.12', '20', '210invalid', 'invalid')) {
    $rejected = $false
    try { Assert-JavaVersion | Out-Null } catch { $rejected = $true }
    if (-not $rejected) { throw "Expected $testVersion to be rejected" }
}
Write-Host 'Java version checks passed.'
