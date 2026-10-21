@echo off
title [3] LIVE VIEW - what the gateway is doing every second
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0watch.ps1"
pause
