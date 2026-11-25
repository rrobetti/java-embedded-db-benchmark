$ErrorActionPreference = 'Stop'
& python (Join-Path $PSScriptRoot 'load_scalability.py') @args
exit $LASTEXITCODE
