$ErrorActionPreference = 'Stop'
& python (Join-Path $PSScriptRoot 'read_write_interference.py') @args
exit $LASTEXITCODE
