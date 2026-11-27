$ErrorActionPreference = 'Stop'
& python (Join-Path $PSScriptRoot 'cache_refresh.py') @args
exit $LASTEXITCODE
