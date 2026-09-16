@echo off
setlocal
for %%I in ("%~dp0..") do set "REPO=%%~fI"
"%REPO%\.venv\Scripts\python.exe" "%REPO%\telemetry.py" run --db "%REPO%\data\telemetry_observation_v2_20260916.sqlite3" --duration-days 14 --binance-profile failover --resume-latest >> "%REPO%\data\telemetry_observation_v2_20260916.log" 2>> "%REPO%\data\telemetry_observation_v2_20260916.error.log"
exit /b %errorlevel%
