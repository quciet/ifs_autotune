@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Always run from repository root even when double-clicked.
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"
cd /d "%REPO_ROOT%"

set "VENV_PY=backend\.venv\Scripts\python.exe"
set "PYTHON_CMD="

echo ========================================================
echo BIGPOPA setup and launch
echo Repo: %REPO_ROOT%
echo ========================================================

if not exist "backend\pyproject.toml" (
  echo [ERROR] Could not find backend\pyproject.toml.
  echo Run this script from inside the BIGPOPA repository.
  exit /b 1
)

call :find_python
if errorlevel 1 exit /b 1

if not exist "backend\.venv\Scripts\python.exe" (
  echo [1/6] Creating required virtual environment at backend\.venv ...
  call %PYTHON_CMD% -m venv backend\.venv
  if errorlevel 1 (
    echo [ERROR] Failed to create backend\.venv.
    exit /b 1
  )
) else (
  echo [1/6] Using existing required virtual environment: backend\.venv
)

echo [2/6] Upgrading pip in backend\.venv ...
"%VENV_PY%" -m pip install -U pip
if errorlevel 1 (
  echo [ERROR] pip upgrade failed.
  exit /b 1
)

echo [3/6] Installing backend dependencies (editable install) ...
"%VENV_PY%" -m pip install -e backend
if errorlevel 1 (
  echo [ERROR] Backend install failed.
  echo Try deleting backend\*.egg-info and run this script again.
  exit /b 1
)

call :check_npm
if errorlevel 1 exit /b 1

if exist "frontend\package.json" (
  echo [4/6] Installing frontend dependencies ...
  pushd frontend
  call npm install
  if errorlevel 1 (
    popd
    echo [ERROR] Frontend npm install failed.
    exit /b 1
  )
  popd
) else (
  echo [4/6] No frontend\package.json found; skipping frontend dependency install.
)

if exist "desktop\package.json" (
  echo [5/6] Installing desktop/Electron dependencies ...
  pushd desktop
  call npm install
  if errorlevel 1 (
    popd
    echo [ERROR] Desktop npm install failed.
    exit /b 1
  )
  popd
) else (
  echo [5/6] No desktop\package.json found; skipping desktop dependency install.
)

echo [6/6] Launching BIGPOPA ...
if exist "desktop\package.json" (
  echo Starting frontend and desktop windows...
  start "BIGPOPA Frontend" cmd /k "cd /d \"%REPO_ROOT%\frontend\" && npm run dev"
  start "BIGPOPA Desktop" cmd /k "cd /d \"%REPO_ROOT%\desktop\" && npm run start:electron"
  echo.
  echo BIGPOPA is launching. Keep both windows open while using the app.
  echo Backend Python tools run from backend\.venv on demand from Electron.
) else (
  echo [ERROR] desktop\package.json not found. Cannot launch Electron app.
  exit /b 1
)

exit /b 0

:find_python
where py >nul 2>nul
if not errorlevel 1 (
  py -3.11 -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)" >nul 2>nul
  if not errorlevel 1 (
    set "PYTHON_CMD=py -3.11"
    echo Using Python launcher: py -3.11
    goto :eof
  )
)

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python 3.11+ not found.
  echo Install Python 3.11 or newer, then re-run this script.
  exit /b 1
)

python -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)" >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Found python, but version is below 3.11.
  echo Install Python 3.11 or newer, then re-run this script.
  exit /b 1
)

set "PYTHON_CMD=python"
echo Using Python executable: python
exit /b 0

:check_npm
where npm >nul 2>nul
if errorlevel 1 (
  echo [ERROR] npm not found.
  echo Install Node.js LTS ^(includes npm^) and re-run this script.
  exit /b 1
)
exit /b 0
