@echo off
echo ============================================
echo   PDR Processing Dashboard - Launcher
echo ============================================
echo.

:: Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH.
    echo Please install Python 3.10+ from https://python.org
    pause
    exit /b 1
)

:: Check/install dependencies
echo Checking dependencies...
pip show streamlit >nul 2>&1
if errorlevel 1 (
    echo Installing required packages (first-time only)...
    pip install streamlit pandas openpyxl plotly rapidfuzz
    echo.
)

SET PORT=8506

:: Launch the app
echo Starting PDR Processing Dashboard...
echo.
echo The app will open in your browser at http://localhost:%PORT%
echo Press Ctrl+C in this window to stop the app.
echo.
streamlit run "%~dp0pdr_app.py" --server.port %PORT% --server.headless true

pause
