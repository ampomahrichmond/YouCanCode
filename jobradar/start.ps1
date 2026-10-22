# Start (or cleanly restart) JobRadar on http://localhost:8000
# Usage:  .\start.ps1            -> restarts in this window (Ctrl+C to stop)
#         .\start.ps1 -Background -> restarts detached, logs to jobradar.log
param([switch]$Background)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

# Stop any previous JobRadar server (uvicorn + its worker) and sweep child
# process (python -m app.sweep), including orphans.
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -match "app\.main:app|-m app\.sweep" -and $_.CommandLine -match [regex]::Escape($root) } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Start-Sleep -Seconds 2

$uvicorn = Join-Path $root ".venv\Scripts\uvicorn.exe"
if (-not (Test-Path $uvicorn)) { Write-Error "virtualenv missing: run  python -m venv .venv; .venv\Scripts\pip install -r requirements.txt"; exit 1 }

if ($Background) {
  $log = Join-Path $root "jobradar.log"
  Start-Process -FilePath $uvicorn -ArgumentList "app.main:app","--port","8000" -WorkingDirectory $root `
    -RedirectStandardError $log -WindowStyle Hidden | Out-Null
  Start-Sleep -Seconds 4
  Write-Host "JobRadar running at http://localhost:8000  (log: $log)"
} else {
  Write-Host "JobRadar starting at http://localhost:8000  (Ctrl+C to stop)"
  & $uvicorn app.main:app --port 8000
}
