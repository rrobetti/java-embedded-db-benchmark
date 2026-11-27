$ErrorActionPreference = 'Stop'
& python (Join-Path $PSScriptRoot 'endurance.py') @args
exit $LASTEXITCODE
