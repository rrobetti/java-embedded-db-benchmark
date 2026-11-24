param([switch]$Offline)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
if ($env:JAVA_HOME) { $env:PATH = "$(Join-Path $env:JAVA_HOME 'bin')$([IO.Path]::PathSeparator)$env:PATH" }
if (-not (Get-Command java -ErrorAction SilentlyContinue)) { throw 'Install JDK 21 manually and set JAVA_HOME.' }
$version = (& java -version 2>&1 | Out-String)
if ($version -notmatch 'version "21([."]|-)') { throw "JDK 21 is required. Set JAVA_HOME to JDK 21.`n$version" }
$os = [Runtime.InteropServices.RuntimeInformation]::OSDescription
$arch = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture
$cpu = [Environment]::ProcessorCount
$ram = 'unknown'
if ($IsWindows) {
    $ram = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
} elseif ($IsLinux) {
    $mem = Get-Content /proc/meminfo | Where-Object { $_ -match '^MemTotal:' }
    if ($mem -match '(\d+)') { $ram = [long]$Matches[1] * 1024 }
} elseif ($IsMacOS) {
    $ram = & sysctl -n hw.memsize
}
Write-Host "OS=$os arch=$arch logicalCPUs=$cpu physicalRAMBytes=$ram"
Write-Host $version
$nativeTmp = if ($env:BENCHMARK_NATIVE_TMP) { $env:BENCHMARK_NATIVE_TMP } else { Join-Path $PWD 'target/native-tmp' }
New-Item -ItemType Directory -Force -Path $nativeTmp | Out-Null
$nativeTmp = (Resolve-Path $nativeTmp).Path
$probe = Join-Path $nativeTmp "permission-probe-$PID"
try { [IO.File]::WriteAllText($probe, 'probe') } finally { if (Test-Path $probe) { Remove-Item $probe } }
if ($IsWindows) {
    if (-not (Test-Path './mvnw.cmd')) { throw 'Restore the Maven wrapper.' }
    if (-not $Offline) { & ./mvnw.cmd -B dependency:go-offline; if ($LASTEXITCODE -ne 0) { throw 'Dependency resolution failed.' } }
    $mavenArgs = @('-B'); if ($Offline) { $mavenArgs += '-o' }; $mavenArgs += 'package'
    & ./mvnw.cmd @mavenArgs
} else {
    if (-not (Test-Path './mvnw')) { throw 'Restore the Maven wrapper.' }
    if (-not $Offline) { & bash ./mvnw -B dependency:go-offline; if ($LASTEXITCODE -ne 0) { throw 'Dependency resolution failed.' } }
    $mavenArgs = @('./mvnw', '-B'); if ($Offline) { $mavenArgs += '-o' }; $mavenArgs += 'package'
    & bash @mavenArgs
}
if ($LASTEXITCODE -ne 0) { throw 'Maven package failed.' }
& java "-Djava.io.tmpdir=$nativeTmp" -cp target/benchmark.jar io.github.embeddedbench.db.DatabaseSmoke
if ($LASTEXITCODE -ne 0) {
    throw 'Connectivity failed. Check JDK 21, native OS/architecture support, writable/executable temp storage and Windows DLL/security policy. Set BENCHMARK_NATIVE_TMP to a user-owned directory. No elevated install is attempted.'
}
