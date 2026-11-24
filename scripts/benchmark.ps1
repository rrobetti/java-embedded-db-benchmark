$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$offline = $false
$benchmarkArgs = @()
$heap = '512m'
for ($i = 0; $i -lt $args.Count; $i++) {
    if ($args[$i] -eq '--offline') { $offline = $true; continue }
    if ($args[$i] -eq '--jvm-heap') {
        if ($i + 1 -ge $args.Count) { throw '--jvm-heap needs a value.' }
        $heap = $args[$i + 1]
    } elseif ($args[$i] -like '--jvm-heap=*') {
        $heap = $args[$i].Substring('--jvm-heap='.Length)
    }
    $benchmarkArgs += $args[$i]
}
if ($heap -notmatch '^[1-9][0-9]*[kKmMgG]?$') { throw "Invalid JVM heap: $heap" }
if ($env:JAVA_HOME) { $env:PATH = "$(Join-Path $env:JAVA_HOME 'bin')$([IO.Path]::PathSeparator)$env:PATH" }
if (-not (Get-Command java -ErrorAction SilentlyContinue)) { throw 'Install JDK 21 manually and set JAVA_HOME.' }
$version = (& java -version 2>&1 | Out-String)
if ($version -notmatch 'version "21([."]|-)') { throw 'JDK 21 is required. Set JAVA_HOME to JDK 21.' }
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
Write-Host 'Resource policy: Java heap cap and supervisor safety checks; no OS hard limit is installed.'
if (-not (Test-Path 'target/benchmark.jar')) {
    & (Join-Path $PSScriptRoot 'setup.ps1') -Offline:$offline
}
$nativeTmp = if ($env:BENCHMARK_NATIVE_TMP) { $env:BENCHMARK_NATIVE_TMP } else { Join-Path $PWD 'target/native-tmp' }
New-Item -ItemType Directory -Force -Path $nativeTmp | Out-Null
$nativeTmp = (Resolve-Path $nativeTmp).Path
$probe = Join-Path $nativeTmp "permission-probe-$PID"
try { [IO.File]::WriteAllText($probe, 'probe') } finally { if (Test-Path $probe) { Remove-Item $probe } }
if ($benchmarkArgs.Count -eq 0) { $benchmarkArgs = @('--profile', 'quick') }
& java "-Xmx$heap" "-Djava.io.tmpdir=$nativeTmp" -jar target/benchmark.jar @benchmarkArgs
exit $LASTEXITCODE
