@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

:: ==========================================
:: Pushing to GitHub
:: ==========================================
:: This script commits recent changes and pushes them to a remote GitHub repository.
echo.

:: 1. Ensure all files are tracked and committed
echo Staging and committing changes...
git add .
:: Attempt commit; hide output if nothing to commit
git commit -m "Update workflow scripts" >nul 2>&1

:: 2. Check if remote is configured
:: By querying the origin URL
git remote get-url origin >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Remote 'origin' is not configured.
    echo.
    :: Default repo url
    set REPO_URL=https://github.com/smichalove/Gemini_Photo_Batch_Workflow.git
    echo Setting remote origin to !REPO_URL!...
    echo (Ensure you have created the empty repository on GitHub!)
    
    :: Add the new remote
    git remote add origin !REPO_URL!
) else (
    echo Remote 'origin' is already configured:
    git remote get-url origin
)

:: 3. Push to main
echo.
echo Pushing to main branch...
:: Ensure branch is named main before push
git branch -M main
:: Push changes upstream
git push -u origin main

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ⚠️ Push failed. Attempting to pull remote changes...
    echo (This is necessary if you initialized the repo with a README or License)
    :: Pull with unrelated histories allowed
    git pull origin main --allow-unrelated-histories --no-edit
    echo Retrying push...
    git push -u origin main
)

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ❌ Push failed!
    echo.
    echo The repository URL is currently set to:
    git remote get-url origin
    echo.
    
    :: Give user a chance to fix the URL manually
    set /p CHANGE_URL="Did you create the repo with a different name? Update URL? (Y/N): "
    if /i "!CHANGE_URL!"=="Y" (
        set /p NEW_URL="Enter the new GitHub Repository URL: "
        git remote set-url origin !NEW_URL!
        echo.
        echo Retrying push...
        git push -u origin main
        if !ERRORLEVEL! NEQ 0 exit /b
    ) else (
        echo Opening GitHub creation page...
        start https://github.com/new
        pause
        exit /b
    )
)

echo.
echo ✅ Push complete!
pause
