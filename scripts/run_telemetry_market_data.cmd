@echo off
setlocal

set "REPO=%~dp0.."
"%REPO%\.venv\Scripts\python.exe" "%REPO%\telemetry.py" run --db "%REPO%\data\telemetry_calibration_market_data_20260910.sqlite3" --duration-days 14 --binance-profile market-data --resume-latest >> "%REPO%\data\telemetry_calibration_market_data_20260910.log" 2>> "%REPO%\data\telemetry_calibration_market_data_20260910.error.log"
