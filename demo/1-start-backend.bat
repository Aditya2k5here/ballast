@echo off
title [1] BACKEND - fake server (8 workers, ~250 req/s)
cd /d "%~dp0..\bin"
echo Fake backend: 8 workers, 25ms fast / 120ms slow, can handle ~250 requests/sec.
echo Leave this window open.
echo.
ballast-upstream.exe -workers 8 -fast 25ms -slow 120ms
pause
