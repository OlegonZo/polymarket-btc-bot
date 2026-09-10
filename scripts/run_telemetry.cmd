@echo off
setlocal

set "REPO=%~dp0.."
"%REPO%\.venv\Scripts\python.exe" "%REPO%\telemetry.py" run --db "%REPO%\data\telemetry_calibration.sqlite3" --duration-days 14 --resume-latest >> "%REPO%\data\telemetry_calibration.log" 2>> "%REPO%\data\telemetry_calibration.error.log"
