@echo off
echo === Cleaning old build ===
rmdir /s /q build
rmdir /s /q dist
if exist CrawlData.spec del CrawlData.spec

@REM echo.
@REM echo === Building new exe ===
@REM pyinstaller --onefile --add-data "config/db_config.yaml;config" CrawlData.py
@REM
@REM if errorlevel 1 (
@REM     echo Build failed! Exiting...
@REM     pause
@REM     exit /b 1
@REM )
@REM
@REM echo.
@REM echo === Running the new exe ===
@REM cd dist
@REM CrawlData.exe
@REM pause
