@echo off
cd /d "%~dp0"
python dividend_calendar_pipeline.py --source nasdaq --start 2025-01-01 --end 2027-01-01 --workers 8 --include-unmatched
pause
