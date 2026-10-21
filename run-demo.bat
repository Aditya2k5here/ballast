@echo off
title Ballast demo
cd /d "%~dp0bin"
echo === Starting fake backend (8 workers, ~250 rps capacity) ===
start "ballast-upstream" ballast-upstream.exe -workers 8 -fast 25ms -slow 120ms
timeout /t 2 /nobreak >nul
echo === Starting Ballast gateway (adaptive limiter) ===
start "ballast-gateway" ballast.exe -upstream http://127.0.0.1:9101 -limiter gradient
timeout /t 2 /nobreak >nul
start "" http://localhost:8081/snapshot
echo.
echo === Sending 1000 requests/sec for 20 seconds (4x overload) ===
echo     Refresh the browser tab to watch the limit change live.
echo.
ballast-load.exe -rate 1000 -duration 20s
echo.
echo === Final gateway snapshot ===
curl -s http://localhost:8081/snapshot
echo.
echo.
echo Press any key to stop the backend and gateway...
pause >nul
taskkill /im ballast.exe /f >nul 2>&1
taskkill /im ballast-upstream.exe /f >nul 2>&1
