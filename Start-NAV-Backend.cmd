@echo off
setlocal
set "ROOT_DIR=%~dp0"
set "PORT=3000"

where node >nul 2>nul
if errorlevel 1 (
  echo Node.js was not found in PATH.
  echo Install Node.js 20+ and try again.
  pause
  exit /b 1
)

cd /d "%ROOT_DIR%"
echo Starting Funalytics NAV backend on http://127.0.0.1:%PORT%
node src\server.js
