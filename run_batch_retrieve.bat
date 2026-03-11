@echo off
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

:: ===================================================
:: Windows Batch Photo Retriever (Vertex AI)
:: ===================================================
:: This script checks for completed Vertex AI batch jobs and retrieves the output descriptions.

:: Change directory to the location of this script dynamically
cd /d "%~dp0"

echo Activating virtual environment (venv-photos)...
:: Activate the Python virtual environment to ensure dependencies are available
if exist "venv-photos\Scripts\activate.bat" (
    call "venv-photos\Scripts\activate.bat"
) else (
    echo WARNING: venv-photos\Scripts\activate.bat not found. Attempting to run without venv.
)

:: Set the required Google Application Credentials dynamically to the auth folder in this repo
set GOOGLE_APPLICATION_CREDENTIALS=%~dp0auth\service_account.json

echo.
echo Checking for completed batch jobs...
:: Execute the python retrieval script
python batch_check_and_retrieve_windows.py

echo.
echo Script execution finished.
