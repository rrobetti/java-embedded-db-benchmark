$ErrorActionPreference = 'Stop'
& python (Join-Path $PSScriptRoot 'memory_capacity.py') @args
exit $LASTEXITCODE
