@echo off
echo Starting Daily Monitor server on http://localhost:5050/
echo Press Ctrl+C to stop.
echo.
cd /d "%~dp0"
start "" "http://localhost:5050/"
python pnl_server.py --port 5050
pause
