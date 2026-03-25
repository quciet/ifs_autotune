@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Always run from repository root even when double-clicked.
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"
cd /d "%REPO_ROOT%"

set "SETUP_BAT=%SCRIPT_DIR%Setup_BIGPOPA.bat"
set "VENV_PY=backend\.venv\Scripts\python.exe"
set "BIGPOPA_DB=%REPO_ROOT%\desktop\output\bigpopa.db"
set "OUTPUT_ROOT=%REPO_ROOT%\desktop\output\analysis"
set "DEFAULT_LIMIT=400"
set "DEFAULT_WINDOW=25"
set "DATASET_ID="
set "LIMIT=%DEFAULT_LIMIT%"
set "WINDOW=%DEFAULT_WINDOW%"

echo ========================================================
echo BIGPOPA trend analysis
echo Repo: %REPO_ROOT%
echo ========================================================

if not exist "%VENV_PY%" (
  echo [ERROR] backend\.venv is missing.
  echo Run "%SETUP_BAT%" first.
  exit /b 1
)

if not exist "%BIGPOPA_DB%" (
  echo [ERROR] Could not find "%BIGPOPA_DB%".
  echo Run BIGPOPA first so bigpopa.db is created.
  exit /b 1
)

"%VENV_PY%" -c "import analysis.analyze_latest_runs, matplotlib" >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Analysis dependencies are not ready in backend\.venv.
  echo Run "%SETUP_BAT%" first.
  exit /b 1
)

echo Press Enter to accept the default shown in brackets.
set /p "DATASET_ID=Dataset id override [latest dataset]: "
set "DATASET_ID=%DATASET_ID: =%"
set /p "LIMIT=Number of latest runs to analyze [%DEFAULT_LIMIT%]: "
set "LIMIT=%LIMIT: =%"
if not defined LIMIT set "LIMIT=%DEFAULT_LIMIT%"
set /p "WINDOW=Rolling window size [%DEFAULT_WINDOW%]: "
set "WINDOW=%WINDOW: =%"
if not defined WINDOW set "WINDOW=%DEFAULT_WINDOW%"

echo.
echo Running trend analysis ...
if defined DATASET_ID (
  "%VENV_PY%" -m analysis.analyze_latest_runs --bigpopa-db "%BIGPOPA_DB%" --limit %LIMIT% --window %WINDOW% --output-root "%OUTPUT_ROOT%" --dataset-id "%DATASET_ID%"
) else (
  "%VENV_PY%" -m analysis.analyze_latest_runs --bigpopa-db "%BIGPOPA_DB%" --limit %LIMIT% --window %WINDOW% --output-root "%OUTPUT_ROOT%"
)
if errorlevel 1 (
  echo [ERROR] Trend analysis failed.
  exit /b 1
)

echo.
echo Trend analysis finished.
exit /b 0
