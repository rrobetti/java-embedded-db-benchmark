$ErrorActionPreference = 'Stop'
& python (Join-Path $PSScriptRoot 'memory_reclamation.py') @args
exit $LASTEXITCODE
