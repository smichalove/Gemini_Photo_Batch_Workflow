@echo off
echo ===================================================
echo Windows Batch Photo Retry (Safety Violations)
echo ===================================================
:: This script launches the python routine to attempt to bypass false
:: positive safety blocks on images by utilizing a neutral prompt layout.

set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
set GOOGLE_APPLICATION_CREDENTIALS=%~dp0auth\service_account.json

cd /d "%~dp0"

echo Activating virtual environment (venv-photos)...
if exist "venv-photos\Scripts\activate.bat" (
    call "venv-photos\Scripts\activate.bat"
    python retry_safety_violations.py
) else (
    echo WARNING: venv-photos\Scripts\activate.bat not found. Attempting to use global python.
    python retry_safety_violations.py
)
