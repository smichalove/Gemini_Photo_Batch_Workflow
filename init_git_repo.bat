@echo off
cd /d "%~dp0"

:: ==========================================
:: Initializing Git Repository
:: ==========================================
:: This script sets up the local git repository and creates an initial commit.
echo.

echo 1. Running git init...
:: Initialize the empty local Git repository
git init

:: (The .gitignore file is managed externally)

echo 2. Staging files...
:: Stage all current files
git add .

echo 3. Creating initial commit...
:: Commit staged files with initial message
git commit -m "Initial commit of Gemini Photo Batch Workflow project"

echo 4. Setting branch to main...
:: Ensure the default branch is named main
git branch -M main

echo.
echo ✅ Repository initialized successfully!
pause
