@echo off
title Graian Capital Management — Data Quality Pipeline
color 1F

echo.
echo  ================================================
echo   GRAIAN CAPITAL MANAGEMENT
echo   Data Quality Pipeline v1.0.0
echo  ================================================
echo.
echo  Starting pipeline... please wait.
echo.

:: Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python is not installed.
    echo  Please install Python from https://python.org
    echo  Then run this file again.
    pause
    exit /b 1
)

:: Install all dependencies from requirements.txt
echo  Installing required packages...
pip install -r requirements.txt --quiet
echo  Dependencies ready.
echo.

:: Start the Flask app in background
echo  Launching pipeline server...
start /B python app.py

:: Wait 3 seconds for server to start
timeout /t 3 /nobreak >nul

:: Open browser automatically
echo  Opening browser...
start http://localhost:5000

echo.
echo  ================================================
echo   Pipeline is running at http://localhost:5000
echo   Keep this window open while using the tool.
echo   Close this window to stop the server.
echo  ================================================
echo.

:: Keep window open so server stays alive
pause
