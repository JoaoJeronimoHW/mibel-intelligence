@echo off
rem Run the MIBEL pipeline for the last N days from cmd, e.g.:
rem   mibel 30
rem   mibel 7 --end 2026-09-15
rem   mibel 90 --skip entsoe
rem Works from any directory: it switches to the project folder first.
setlocal
cd /d "%~dp0"
if not exist "venv\Scripts\python.exe" (
    echo [ERROR] venv not found. Run: python -m venv venv ^&^& venv\Scripts\pip install -r requirements.txt
    exit /b 1
)
"venv\Scripts\python.exe" -m src.pipeline %*
exit /b %ERRORLEVEL%
