@echo off
echo Installing required Python packages for PostgreSQL CSV Loader...
echo.

pip install pandas>=1.3.0
pip install psycopg2-binary>=2.9.0
pip install SQLAlchemy>=1.4.0

echo.
echo Installation completed!
echo.
echo Next steps:
echo 1. Copy config_template.py to config.py
echo 2. Update database credentials in config.py
echo 3. Run: python csv_loader.py
echo.
pause
