@echo off
title [4] LOAD - 1000 requests/sec for 60s (4x overload)
cd /d "%~dp0..\bin"
echo Sending 1000 requests/sec for 60 seconds = 4x what the backend can handle.
echo Mix: critical / standard / bulk. Watch window [3] while this runs.
echo.
ballast-load.exe -rate 1000 -duration 60s
echo.
pause
