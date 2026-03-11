@echo off
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

:: ===================================================
:: Windows Batch Photo Submitter (Vertex AI)
:: ===================================================
:: This script triggers the python file to scan directories, upload, and submit jobs to Vertex.

:: Change directory to the location of this script dynamically
cd /d "%~dp0"

echo Activating virtual environment (venv-photos)...
:: Activate the Python virtual environment to ensure dependencies are available
if exist "venv-photos\Scripts\activate.bat" (
    call "venv-photos\Scripts\activate.bat"
    echo Installing requirements...
    pip install -r requirements.txt -q
) else (
    echo WARNING: venv-photos\Scripts\activate.bat not found. Attempting to run without venv.
)

:: Set the required Google Application Credentials dynamically to the auth folder in this repo
set GOOGLE_APPLICATION_CREDENTIALS=%~dp0auth\service_account.json

echo.
echo Running batch submission script...
:: Execute the python submission script
python batch_submit_photos_windows.py

echo.
echo Script execution finished.
