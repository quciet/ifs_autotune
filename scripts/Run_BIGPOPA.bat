@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Always run from repository root even when double-clicked.
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"
cd /d "%REPO_ROOT%"

set "SETUP_BAT=%SCRIPT_DIR%Setup_BIGPOPA.bat"
set "VENV_PY=backend\.venv\Scripts\python.exe"

echo ========================================================
echo BIGPOPA launch
echo Repo: %REPO_ROOT%
echo ========================================================

if not exist "backend\pyproject.toml" (
  echo [ERROR] Could not find backend\pyproject.toml.
  echo Run this script from inside the BIGPOPA repository.
  exit /b 1
)

if not exist "%VENV_PY%" (
  echo [ERROR] backend\.venv is missing.
  echo Run "%SETUP_BAT%" first.
  exit /b 1
)

call :check_npm
if errorlevel 1 exit /b 1

if exist "frontend\package.json" (
  if not exist "frontend\node_modules" (
    echo [ERROR] frontend dependencies are not installed.
    echo Run "%SETUP_BAT%" first.
    exit /b 1
  )
) else (
  echo [WARN] frontend\package.json not found; frontend background process was not started.
)

if exist "desktop\package.json" (
  if not exist "desktop\node_modules" (
    echo [ERROR] desktop dependencies are not installed.
    echo Run "%SETUP_BAT%" first.
    exit /b 1
  )
) else (
  echo [ERROR] desktop\package.json not found.
  echo Ensure the desktop directory exists and contains package.json, then re-run this script.
  exit /b 1
)

echo [1/2] Prerequisites look ready.
echo [2/2] Launching BIGPOPA ...
if exist "frontend\package.json" (
  echo Starting BIGPOPA Frontend in background ^(same console^) ...
  REM Use start /b so Vite runs in the background without opening a new window; Electron runs in foreground to keep a single terminal workflow.
  pushd "%REPO_ROOT%\frontend"
  start /b "" npm run dev
  popd
)

echo Starting BIGPOPA Desktop in foreground ^(same console^) ...
pushd "%REPO_ROOT%\desktop"
call npm run start:electron
popd
echo.
echo BIGPOPA is launching in a single console. Keep this window running while using the app.
echo Backend Python tools run from backend\.venv on demand from Electron.

exit /b 0

:check_npm
where npm >nul 2>nul
if errorlevel 1 (
  echo [ERROR] npm not found.
  echo Install Node.js LTS ^(includes npm^) and re-run this script.
  exit /b 1
)
exit /b 0
