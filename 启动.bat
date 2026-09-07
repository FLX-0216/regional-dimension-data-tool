@echo off
title Streamlit Launcher
cd /d "%~dp0"

REM ===== 1. Detect Python (try py launcher, then python, then python3) =====
set "PY="
where py >nul 2>nul && set "PY=py"
if not defined PY (where python >nul 2>nul && set "PY=python")
if not defined PY (where python3 >nul 2>nul && set "PY=python3")
if not defined PY (
  echo.
  echo ============================================================
  echo   Python not found on this PC.
  echo   Please install Python 3.10 or newer from:
  echo       https://www.python.org/downloads/
  echo   IMPORTANT: during install, check the box
  echo   "Add Python to PATH". Then double-click this file again.
  echo.
  echo ============================================================
  echo.
  pause
  exit /b 1
)

REM ===== 2. Install dependencies (skips if already installed) =====
echo Checking / installing dependencies, please wait...
%PY% -m pip install -r requirements.txt --quiet
if errorlevel 1 (
  echo.
  echo pip install failed. Please check your network, or
  echo right-click this file and choose "Run as administrator".
  echo.
  pause
  exit /b 1
)

REM ===== 3. Start Streamlit server in a new window =====
echo.
echo Starting Streamlit, your browser will open shortly...
start "Streamlit" %PY% -m streamlit run app.py --server.headless true --browser.gatherUsageStats false --server.port 8501

REM ===== 4. Wait for the server, then open the browser =====
timeout /t 7 >nul
start "" http://localhost:8501
echo.
echo App started! If your browser did not open, visit:
echo     http://localhost:8501
echo.
echo To stop the app, close the window titled "Streamlit".
echo.
pause
