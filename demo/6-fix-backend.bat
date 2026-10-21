@echo off
title [6] RECOVER - backend back to normal
curl.exe -s -XPOST http://127.0.0.1:9102/profile -d @"%~dp0normal.json"
echo.
echo Backend back to normal. Watch the LIMIT in window [3] climb back up.
timeout /t 5
