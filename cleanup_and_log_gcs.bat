@echo off
echo ===================================================
echo GCS Storage Cleanup and Logging
echo ===================================================

cd /d "H:\Wan_project"

echo.
echo [1/3] Logging current space before cleanup...
echo ---------------------------------------------------
call gcloud.cmd storage du -s gs://mutua-477100-batch-images/

echo.
echo [2/3] Running cleanup script...
echo ---------------------------------------------------
python cleanup_gcs_images.py

echo.
echo [3/3] Logging current space after cleanup...
echo ---------------------------------------------------
call gcloud.cmd storage du -s gs://mutua-477100-batch-images/

echo.
echo Done.
