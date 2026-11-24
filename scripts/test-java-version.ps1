$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'java-version.ps1')
function java { "openjdk version `"$script:testVersion`"" }

foreach ($testVersion in @('25', '25.0.5.1', '25-ea', '27+35')) {
    Assert-JavaVersion | Out-Null
}
foreach ($testVersion in @('1.8.0', '17.0.12', '20', '21', '21.0.5', '24.0.2', '250invalid', 'invalid')) {
    $rejected = $false
    try { Assert-JavaVersion | Out-Null } catch { $rejected = $true }
    if (-not $rejected) { throw "Expected $testVersion to be rejected" }
}

$savedJavaHome = $env:JAVA_HOME
$savedMavenHome = $env:MAVEN_USER_HOME
$savedSkipRc = $env:MAVEN_SKIP_RC
$env:JAVA_HOME = $null
$env:MAVEN_USER_HOME = Join-Path ([IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString())
try {
    $testVersion = '25.0.5'
    try {
        & (Join-Path $PSScriptRoot 'setup.ps1') -Offline | Out-Null
        throw 'Expected offline setup to require a cached Maven distribution'
    } catch {
        if ($_.Exception.Message -notmatch 'Offline setup requires the pinned Maven distribution') { throw }
    }
} finally {
    $env:JAVA_HOME = $savedJavaHome
    $env:MAVEN_USER_HOME = $savedMavenHome
    $env:MAVEN_SKIP_RC = $savedSkipRc
}
Write-Host 'Java version checks passed.'
