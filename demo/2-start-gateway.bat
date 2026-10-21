@echo off
title [2] BALLAST GATEWAY - adaptive limiter
cd /d "%~dp0..\bin"
echo Ballast gateway on :8080, admin/metrics on :8081, forwarding to backend on :9101.
echo Leave this window open.
echo.
ballast.exe -upstream http://127.0.0.1:9101 -limiter gradient
pause
