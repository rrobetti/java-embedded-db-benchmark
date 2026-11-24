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
