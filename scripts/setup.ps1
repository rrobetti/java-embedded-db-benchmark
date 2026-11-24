param([switch]$Offline)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$env:MAVEN_SKIP_RC = 'true'
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
& java -version
$nativeTmp = if ($env:BENCHMARK_NATIVE_TMP) { $env:BENCHMARK_NATIVE_TMP } else { Join-Path $PWD 'target/native-tmp' }
New-Item -ItemType Directory -Force -Path $nativeTmp | Out-Null
$nativeTmp = (Resolve-Path $nativeTmp).Path
$probe = Join-Path $nativeTmp "permission-probe-$PID"
try { [IO.File]::WriteAllText($probe, 'probe') } finally { if (Test-Path $probe) { Remove-Item $probe } }
if ($Offline) {
    $properties = Get-Content -Raw '.mvn/wrapper/maven-wrapper.properties' | ConvertFrom-StringData
    $distributionUrl = $properties.distributionUrl
    if (-not $distributionUrl) { throw 'Missing wrapper distributionUrl.' }
    if ($env:MVNW_REPOURL) {
        $distributionUrl = "$env:MVNW_REPOURL/org/apache/maven/" + ($distributionUrl -replace '^.*/org/apache/maven/', '')
    }
    $distributionName = ($distributionUrl -replace '^.*/', '') -replace '\.zip$', '' -replace '-bin$', ''
    if ($IsWindows) {
        $sha = [Security.Cryptography.SHA256]::Create()
        try { $cacheKey = ($sha.ComputeHash([byte[]][char[]]$distributionUrl) | ForEach-Object { $_.ToString('x2') }) -join '' }
        finally { $sha.Dispose() }
        $mavenExecutable = 'mvn.cmd'
    } else {
        [long]$hash = 0
        foreach ($character in $distributionUrl.ToCharArray()) { $hash = ($hash * 31 + [int]$character) % 4294967296 }
        $cacheKey = $hash.ToString('x')
        $mavenExecutable = 'mvn'
    }
    $mavenHome = if ($env:MAVEN_USER_HOME) { $env:MAVEN_USER_HOME } else { Join-Path $HOME '.m2' }
    $cachedMaven = Join-Path $mavenHome "wrapper/dists/$distributionName/$cacheKey/bin/$mavenExecutable"
    if (-not (Test-Path $cachedMaven -PathType Leaf)) {
        throw 'Offline setup requires the pinned Maven distribution already cached. Run online setup first.'
    }
}
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
& java -Xmx1024m "-Djava.io.tmpdir=$nativeTmp" "-Dorg.sqlite.tmpdir=$nativeTmp" -cp target/benchmark.jar io.github.embeddedbench.db.DatabaseSmoke
if ($LASTEXITCODE -ne 0) {
    throw 'Connectivity failed. Check the Java version, native OS/architecture support, writable/executable temp storage and Windows DLL/security policy. Set BENCHMARK_NATIVE_TMP to a user-owned directory. No elevated install is attempted.'
}
