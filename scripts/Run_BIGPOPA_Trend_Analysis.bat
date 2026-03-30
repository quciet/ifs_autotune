@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Always run from repository root even when double-clicked.
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"
cd /d "%REPO_ROOT%"

set "SETUP_BAT=%SCRIPT_DIR%Setup_BIGPOPA.bat"
set "VENV_PY=backend\.venv\Scripts\python.exe"
set "VENV_CFG=backend\.venv\pyvenv.cfg"
set "BIGPOPA_DB=%REPO_ROOT%\desktop\output\bigpopa.db"
set "OUTPUT_ROOT=%REPO_ROOT%\desktop\output\analysis"
set "DEFAULT_LIMIT=400"
set "DEFAULT_WINDOW=25"
set "DATASET_ID="
set "LIMIT=%DEFAULT_LIMIT%"
set "WINDOW=%DEFAULT_WINDOW%"
set "ERR_MSG="
set "ERR_HINT="
set "ERR_DETAILS_FILE="
set "ERR_DETAILS_LINE="
set "PRECHECK_STDERR=%TEMP%\bigpopa_trend_analysis_precheck_%RANDOM%_%RANDOM%.log"
set "RUN_STDERR=%TEMP%\bigpopa_trend_analysis_run_%RANDOM%_%RANDOM%.log"

echo ========================================================
echo BIGPOPA trend analysis
echo Repo: %REPO_ROOT%
echo ========================================================

if not exist "%VENV_PY%" (
  set "ERR_MSG=backend\.venv is missing."
  set "ERR_HINT=Run %SETUP_BAT% first."
  goto :fail
)

if not exist "%BIGPOPA_DB%" (
  set "ERR_MSG=Could not find \"%BIGPOPA_DB%\"."
  set "ERR_HINT=Run BIGPOPA first so bigpopa.db is created."
  goto :fail
)

call :check_stale_venv
if errorlevel 1 goto :fail

"%VENV_PY%" -c "import analysis.analyze_latest_runs, matplotlib" >nul 2>"%PRECHECK_STDERR%"
if errorlevel 1 (
  set "ERR_MSG=Analysis dependencies are not ready in backend\.venv."
  set "ERR_HINT=Run %SETUP_BAT% to rebuild backend\.venv automatically for this repo location."
  set "ERR_DETAILS_FILE=%PRECHECK_STDERR%"
  goto :fail
)

echo Press Enter to accept the default shown in brackets.
set /p "DATASET_ID=Dataset id override [latest dataset]: "
call :trim_value DATASET_ID
set /p "LIMIT=Number of latest runs to analyze [%DEFAULT_LIMIT%]: "
call :trim_value LIMIT
if not defined LIMIT set "LIMIT=%DEFAULT_LIMIT%"
set /p "WINDOW=Rolling window size [%DEFAULT_WINDOW%]: "
call :trim_value WINDOW
if not defined WINDOW set "WINDOW=%DEFAULT_WINDOW%"

echo.
echo Running trend analysis ...

if defined DATASET_ID (
  "%VENV_PY%" -m analysis.analyze_latest_runs --bigpopa-db "%BIGPOPA_DB%" --limit %LIMIT% --window %WINDOW% --output-root "%OUTPUT_ROOT%" --dataset-id "%DATASET_ID%" 2>"%RUN_STDERR%"
) else (
  "%VENV_PY%" -m analysis.analyze_latest_runs --bigpopa-db "%BIGPOPA_DB%" --limit %LIMIT% --window %WINDOW% --output-root "%OUTPUT_ROOT%" 2>"%RUN_STDERR%"
)
if errorlevel 1 (
  set "ERR_MSG=Trend analysis failed."
  set "ERR_HINT=Review the Python error below, then rerun. If the environment looks stale, run %SETUP_BAT%."
  set "ERR_DETAILS_FILE=%RUN_STDERR%"
  goto :fail
)

echo.
echo Trend analysis finished.
call :cleanup
exit /b 0

:check_stale_venv
if not exist "%VENV_CFG%" (
  if exist "%VENV_PY%" (
    set "ERR_MSG=backend\.venv is broken or stale for this repo location."
    set "ERR_HINT=Run %SETUP_BAT% to rebuild backend\.venv automatically for %REPO_ROOT%."
    set "ERR_DETAILS_LINE=backend\.venv is missing pyvenv.cfg."
    exit /b 1
  )
  exit /b 0
)
set "VENV_COMMAND="
for /f "usebackq delims=" %%L in ("%VENV_CFG%") do (
  set "CFG_LINE=%%L"
  if /I "!CFG_LINE:~0,10!"=="command = " set "VENV_COMMAND=!CFG_LINE:~10!"
)
if not defined VENV_COMMAND exit /b 0

set "EXPECTED_VENV=%REPO_ROOT%\backend\.venv"
set "VENV_COMMAND_MATCH=!VENV_COMMAND:%EXPECTED_VENV%=!"
if /I "!VENV_COMMAND_MATCH!"=="!VENV_COMMAND!" (
  set "ERR_MSG=backend\.venv is broken or stale for this repo location."
  set "ERR_HINT=This repo appears to have moved. Run %SETUP_BAT% to rebuild backend\.venv automatically for %REPO_ROOT%."
  set "ERR_DETAILS_LINE=Recorded venv command: !VENV_COMMAND!"
  exit /b 1
)
exit /b 0

:fail
echo.
echo [ERROR] %ERR_MSG%
if defined ERR_HINT echo %ERR_HINT%
if defined ERR_DETAILS_LINE echo %ERR_DETAILS_LINE%
if defined ERR_DETAILS_FILE if exist "%ERR_DETAILS_FILE%" (
  echo.
  echo Python/launcher details:
  type "%ERR_DETAILS_FILE%"
)

echo.
echo Press any key to close this window.
pause >nul
call :cleanup
exit /b 1

:cleanup
if exist "%PRECHECK_STDERR%" del /q "%PRECHECK_STDERR%" >nul 2>nul
if exist "%RUN_STDERR%" del /q "%RUN_STDERR%" >nul 2>nul
exit /b 0

:trim_value
setlocal EnableDelayedExpansion
set "VALUE=!%~1!"
if not defined VALUE (
  endlocal & set "%~1="
  exit /b 0
)

for /f "tokens=* delims= " %%A in ("!VALUE!") do set "VALUE=%%A"
:trim_value_tail
if defined VALUE if "!VALUE:~-1!"==" " (
  set "VALUE=!VALUE:~0,-1!"
  goto :trim_value_tail
)

if not defined VALUE (
  endlocal & set "%~1="
  exit /b 0
)

endlocal & set "%~1=%VALUE%"
exit /b 0
