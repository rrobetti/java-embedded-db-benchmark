function Assert-JavaVersion {
    if (-not (Get-Command java -ErrorAction SilentlyContinue)) { throw 'Install JDK 21 or later and set JAVA_HOME.' }
    $version = (& java -version 2>&1 | Out-String)
    $major = 0
    if ($version -notmatch 'version "([0-9]+)([."+-])' -or
        -not [int]::TryParse($Matches[1], [ref]$major) -or $major -lt 21) {
        throw "JDK 21 or later is required. Set JAVA_HOME to a JDK 21 or later installation.`n$version"
    }
    return $version
}

function Initialize-JavaBuild {
    if (-not $env:JAVA_HOME) {
        if (-not (Get-Command java -ErrorAction SilentlyContinue)) { throw 'Install JDK 21 or later and set JAVA_HOME.' }
        $settings = (& java '-XshowSettings:properties' -version 2>&1 | Out-String)
        if ($LASTEXITCODE -ne 0 -or $settings -notmatch '(?m)^\s*java.home = (.+)\r?$') {
            throw 'Cannot detect the active JDK. Set JAVA_HOME to a JDK 21 or later installation.'
        }
        $env:JAVA_HOME = $Matches[1].TrimEnd("`r")
        $suffix = if ($IsWindows) { '.exe' } else { '' }
        if (-not (Test-Path (Join-Path $env:JAVA_HOME "bin/javac$suffix") -PathType Leaf) -and
            (Get-Command javac -ErrorAction SilentlyContinue)) {
            $compilerSettings = (& javac '-J-XshowSettings:properties' -version 2>&1 | Out-String)
            if ($LASTEXITCODE -eq 0 -and $compilerSettings -match '(?m)^\s*java.home = (.+)\r?$') {
                $compilerHome = $Matches[1].TrimEnd("`r")
                if ((Test-Path (Join-Path $compilerHome "bin/java$suffix") -PathType Leaf) -and
                    (Test-Path (Join-Path $compilerHome "bin/javac$suffix") -PathType Leaf)) {
                    $env:JAVA_HOME = $compilerHome
                }
            }
        }
    }
    $bin = Join-Path $env:JAVA_HOME 'bin'
    $suffix = if ($IsWindows) { '.exe' } else { '' }
    foreach ($tool in @('java', 'javac')) {
        if (-not (Test-Path (Join-Path $bin "$tool$suffix") -PathType Leaf)) {
            throw "JAVA_HOME must point to a complete JDK 21 or later (with java and javac): $env:JAVA_HOME. Install a JDK with javac or set JAVA_HOME to an installed JDK."
        }
    }
    $env:PATH = "$bin$([IO.Path]::PathSeparator)$env:PATH"
    $version = Assert-JavaVersion
    $compilerVersion = (& javac -version 2>&1 | Out-String)
    $major = 0
    if ($LASTEXITCODE -ne 0 -or $compilerVersion -notmatch '^javac ([0-9]+)([."+-]|\s*$)' -or
        -not [int]::TryParse($Matches[1], [ref]$major) -or $major -lt 21) {
        throw "JDK 21 or later is required. Set JAVA_HOME to a complete JDK 21 or later installation.`n$compilerVersion"
    }
    # Maven startup files can replace JAVA_HOME after the checks above.
    $env:MAVEN_SKIP_RC = 'true'
    return $version
}
