$ErrorActionPreference = 'Stop'
& python (Join-Path $PSScriptRoot 'ingestion_comparison.py') @args
exit $LASTEXITCODE
