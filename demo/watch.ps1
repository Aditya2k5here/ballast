Write-Host "Live gateway stats, refreshed every second. Ctrl+C to stop." -ForegroundColor Cyan
Write-Host ""
Write-Host ("{0,-9} {1,6} {2,10} {3,9} {4,9} {5,9}" -f "time","limit","in_flight","served","shed","breaker") -ForegroundColor Yellow
while ($true) {
  try {
    $d = Invoke-RestMethod -Uri "http://127.0.0.1:8081/snapshot" -TimeoutSec 2
    Write-Host ("{0,-9} {1,6} {2,10} {3,9} {4,9} {5,9}" -f (Get-Date -Format "HH:mm:ss"),$d.limit,$d.in_flight,$d.ok,$d.shed,$d.breaker)
  } catch {
    Write-Host "gateway not reachable yet - is window [2] running?" -ForegroundColor Red
  }
  Start-Sleep -Seconds 1
}
