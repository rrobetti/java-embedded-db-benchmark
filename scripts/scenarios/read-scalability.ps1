$ErrorActionPreference = 'Stop'
& python (Join-Path $PSScriptRoot 'read_scalability.py') @args
exit $LASTEXITCODE
