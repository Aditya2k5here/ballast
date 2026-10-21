@echo off
taskkill /im ballast.exe /f >nul 2>&1
taskkill /im ballast-upstream.exe /f >nul 2>&1
taskkill /im ballast-load.exe /f >nul 2>&1
echo Stopped backend, gateway and load. You can close the other windows.
timeout /t 5
