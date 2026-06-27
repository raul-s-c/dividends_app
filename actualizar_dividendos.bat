@echo off
cd /d "%~dp0"
python dividend_calendar_pipeline.py --daily-update --lookback-days 95 --forward-days 550 --workers 8
pause
