@echo off
title TCP Client
REM Double-click to start the TCP client.
REM Keep this file in the same folder as tcp-client.js

REM Go to the folder where this script lives
cd /d "%~dp0"

REM Check Node.js is installed
where node >nul 2>nul
if errorlevel 1 (
  echo Node.js is not installed. Get it from https://nodejs.org
  pause
  exit /b 1
)

REM Install socket.io-client on first run
if not exist "node_modules\socket.io-client" (
  echo Installing dependencies - first run only...
  call npm install socket.io-client
  if errorlevel 1 (
    echo Install failed.
    pause
    exit /b 1
  )
)

REM Run the client
node tcp-client.js 192.168.0.190 20001 --url=http://localhost:9000 --reconnect

echo.
echo Client stopped.
pause