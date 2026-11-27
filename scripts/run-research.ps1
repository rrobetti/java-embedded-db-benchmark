$ErrorActionPreference = 'Stop'
& python (Join-Path $PSScriptRoot 'run_research.py') @args
exit $LASTEXITCODE
