@echo off
cd /d "%~dp0"
python dividend_calendar_pipeline.py --source nasdaq --incremental --lookback-days 95 --forward-days 550 --workers 8 --include-unmatched
pause
