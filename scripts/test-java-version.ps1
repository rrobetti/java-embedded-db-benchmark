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

$savedJavaHome = $env:JAVA_HOME
$savedPath = $env:PATH
$savedSkipRc = $env:MAVEN_SKIP_RC
$testDir = Join-Path ([IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString())
$testJavaHome = Join-Path $testDir 'jdk with spaces'
$bin = Join-Path $testJavaHome 'bin'
$suffix = if ($IsWindows) { '.exe' } else { '' }
function java {
    $global:LASTEXITCODE = 0
    if ($args[0] -eq '-XshowSettings:properties') { "    java.home = $testJavaHome" }
    'openjdk version "21.0.5"'
}
function javac {
    $global:LASTEXITCODE = $script:compilerExit
    "javac $script:compilerVersion"
}
try {
    New-Item -ItemType Directory -Path $bin -Force | Out-Null
    foreach ($tool in @('java', 'javac')) {
        New-Item -ItemType File -Path (Join-Path $bin "$tool$suffix") | Out-Null
    }
    $compilerVersion = '21.0.5'
    $compilerExit = 0
    $env:JAVA_HOME = $null
    Initialize-JavaBuild | Out-Null
    if ($env:JAVA_HOME -ne $testJavaHome -or $env:MAVEN_SKIP_RC -ne 'true' -or
        $env:PATH.Split([IO.Path]::PathSeparator)[0] -ne $bin) {
        throw 'Expected the active JDK to be selected and protected from Maven startup files.'
    }
    foreach ($compilerVersion in @('21', '21.0.5', '25.0.5.1', '25-ea', '27+35')) {
        Initialize-JavaBuild | Out-Null
    }
    foreach ($compilerVersion in @('17.0.12', '20', '210invalid', 'invalid')) {
        $rejected = $false
        try { Initialize-JavaBuild | Out-Null } catch { $rejected = $true }
        if (-not $rejected) { throw "Expected compiler $compilerVersion to be rejected" }
    }
    $compilerVersion = '21.0.5'
    $compilerExit = 1
    $rejected = $false
    try { Initialize-JavaBuild | Out-Null } catch { $rejected = $true }
    if (-not $rejected) { throw 'Expected a failing compiler to be rejected' }
    $compilerExit = 0
    $env:JAVA_HOME = Join-Path $testDir 'missing'
    $rejected = $false
    try { Initialize-JavaBuild | Out-Null } catch { $rejected = $true }
    if (-not $rejected) { throw 'Expected invalid JAVA_HOME to be rejected instead of using PATH' }
    $env:JAVA_HOME = $testJavaHome
    Remove-Item (Join-Path $bin "javac$suffix")
    $rejected = $false
    try { Initialize-JavaBuild | Out-Null } catch { $rejected = $true }
    if (-not $rejected) { throw 'Expected a runtime without javac to be rejected' }
} finally {
    $env:JAVA_HOME = $savedJavaHome
    $env:PATH = $savedPath
    $env:MAVEN_SKIP_RC = $savedSkipRc
    Remove-Item -Recurse -Force $testDir
}
Write-Host 'Java version checks passed.'
