@echo off
title [5] FAULT - backend 5x slower
curl.exe -s -XPOST http://127.0.0.1:9102/profile -d @"%~dp0slow.json"
echo.
echo Backend is now 5x slower. Watch the LIMIT in window [3] drop.
timeout /t 5
