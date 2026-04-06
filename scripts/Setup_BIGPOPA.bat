@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Always run from repository root even when double-clicked.
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"
cd /d "%REPO_ROOT%"

set "VENV_DIR=%REPO_ROOT%\backend\.venv"
set "VENV_PY=backend\.venv\Scripts\python.exe"
set "VENV_CFG=backend\.venv\pyvenv.cfg"
set "PYTHON_EXE="
set "PYTHON_ARGS="
set "PYTHON_DISPLAY="

echo ========================================================
echo BIGPOPA setup
echo Repo: %REPO_ROOT%
echo ========================================================
echo Searching automatically for Python 3.11+ via py, PATH, and common machine install locations ...

if not exist "backend\pyproject.toml" (
  echo [ERROR] Could not find backend\pyproject.toml.
  echo Run this script from inside the BIGPOPA repository.
  exit /b 1
)

call :find_python
if errorlevel 1 exit /b 1

call :repair_venv
if errorlevel 1 exit /b 1

if not exist "%VENV_PY%" (
  echo [1/5] Creating required virtual environment at backend\.venv ...
  call :run_python -m venv backend\.venv
  if errorlevel 1 (
    echo [ERROR] Failed to create backend\.venv.
    exit /b 1
  )
) else (
  echo [1/5] Using existing required virtual environment: backend\.venv
)

echo [2/5] Upgrading pip in backend\.venv ...
"%VENV_PY%" -m pip install -U pip
if errorlevel 1 (
  echo [ERROR] pip upgrade failed.
  exit /b 1
)

echo [3/5] Installing backend dependencies ^(editable install^) ...
"%VENV_PY%" -m pip install -e backend
if errorlevel 1 (
  echo [ERROR] Backend install failed.
  echo Try deleting backend\*.egg-info and run this script again.
  exit /b 1
)

call :check_npm
if errorlevel 1 exit /b 1

if exist "frontend\package.json" (
  echo [4/5] Installing frontend dependencies ...
  pushd frontend
  call npm install
  if errorlevel 1 (
    popd
    echo [ERROR] Frontend npm install failed.
    exit /b 1
  )
  popd
) else (
  echo [4/5] frontend\package.json not found; skipping frontend dependency install.
)

if exist "desktop\package.json" (
  echo [5/5] Installing desktop/Electron dependencies ...
  pushd desktop
  call npm install
  if errorlevel 1 (
    popd
    echo [ERROR] Desktop npm install failed.
    exit /b 1
  )
  popd
) else (
  echo [ERROR] desktop\package.json not found.
  echo Ensure the desktop directory exists and contains package.json, then re-run this script.
  exit /b 1
)

echo.
echo BIGPOPA setup is complete.
echo If you move this repository to a new folder later, re-run this script to rebuild backend\.venv for the new path.
echo Use "%SCRIPT_DIR%Run_BIGPOPA.bat" to launch the app.
echo Use Trend Analysis from the desktop app's Tune page to generate the latest trend analysis.
echo.
echo Press any key to continue.
pause >nul
exit /b 0

:find_python
for /f "delims=" %%I in ('where py 2^>nul') do (
  call :try_python_candidate "%%~fI" "-3.11" "Python launcher: %%~fI -3.11"
  if not errorlevel 1 exit /b 0
)

for /f "delims=" %%I in ('where python 2^>nul') do (
  call :try_python_candidate "%%~fI" "" "Python executable: %%~fI"
  if not errorlevel 1 exit /b 0
)

call :discover_python_fallback
if defined PYTHON_EXE (
  echo Using %PYTHON_DISPLAY%
  exit /b 0
)

echo [ERROR] Python 3.11+ not found.
echo Checked: py -3.11
echo Checked: python on PATH
echo Checked: %%LocalAppData%%\Programs\Python\Python*\python.exe
echo Checked: %%ProgramFiles%%\Python*\python.exe
echo Checked: %%ProgramFiles^(x86^)%%\Python*\python.exe
echo Install Python 3.11 or newer, then re-run this script.
exit /b 1

:try_python_candidate
setlocal
set "CANDIDATE_EXE=%~1"
set "CANDIDATE_ARGS=%~2"
set "CANDIDATE_DISPLAY=%~3"

if not defined CANDIDATE_EXE (
  endlocal
  exit /b 1
)

"%CANDIDATE_EXE%" %CANDIDATE_ARGS% -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)" >nul 2>nul
if errorlevel 1 (
  endlocal
  exit /b 1
)

endlocal & set "PYTHON_EXE=%~1" & set "PYTHON_ARGS=%~2" & set "PYTHON_DISPLAY=%~3"
echo Using %PYTHON_DISPLAY%
exit /b 0

:discover_python_fallback
set "PYTHON_EXE="
set "PYTHON_ARGS="
set "PYTHON_DISPLAY="
for /f "usebackq delims=" %%I in (`powershell -NoProfile -Command "$patterns = @($env:LocalAppData + '\Programs\Python\Python*\python.exe', $env:ProgramFiles + '\Python*\python.exe', ${env:ProgramFiles(x86)} + '\Python*\python.exe'); $items = foreach ($pattern in $patterns) { if ($pattern) { Get-ChildItem -Path $pattern -File -ErrorAction SilentlyContinue } }; $best = foreach ($item in ($items ^| Sort-Object FullName -Descending -Unique)) { try { $ver = & $item.FullName -c 'import sys; print(''%%d.%%d.%%d'' %% sys.version_info[:3])'; if ($LASTEXITCODE -eq 0) { [pscustomobject]@{ Path = $item.FullName; Version = [version]$ver } } } catch { } }; $best ^| Where-Object { $_.Version -ge [version]'3.11.0' } ^| Sort-Object Version -Descending ^| Select-Object -First 1 -ExpandProperty Path"`) do (
  set "PYTHON_EXE=%%~fI"
)
if defined PYTHON_EXE (
  set "PYTHON_DISPLAY=Python executable: %PYTHON_EXE%"
)
exit /b 0

:run_python
"%PYTHON_EXE%" %PYTHON_ARGS% %*
exit /b %errorlevel%

:repair_venv
setlocal EnableDelayedExpansion
if not exist "%VENV_DIR%" (
  endlocal
  exit /b 0
)

set "EXPECTED_VENV=%REPO_ROOT%\backend\.venv"
for %%I in ("%VENV_DIR%") do set "RESOLVED_VENV=%%~fI"
if /I not "!RESOLVED_VENV!"=="!EXPECTED_VENV!" (
  echo [ERROR] Refusing to remove backend\.venv because path validation failed.
  echo Expected: !EXPECTED_VENV!
  echo Resolved: !RESOLVED_VENV!
  endlocal
  exit /b 1
)

set "VENV_COMMAND="
set "VENV_REASON="
set "VENV_HAS_PYTHON=0"
set "VENV_HAS_CFG=0"

if exist "%VENV_PY%" set "VENV_HAS_PYTHON=1"
if exist "%VENV_CFG%" set "VENV_HAS_CFG=1"

if "!VENV_HAS_CFG!"=="0" (
  set "VENV_REASON=backend\.venv is missing pyvenv.cfg."
)

if "!VENV_HAS_PYTHON!"=="0" (
  if defined VENV_REASON (
    set "VENV_REASON=!VENV_REASON! backend\.venv is missing Scripts\python.exe."
  ) else (
    set "VENV_REASON=backend\.venv is missing Scripts\python.exe."
  )
)

if "!VENV_HAS_CFG!"=="1" (
  for /f "usebackq delims=" %%L in ("%VENV_CFG%") do (
    set "CFG_LINE=%%L"
    if /I "!CFG_LINE:~0,10!"=="command = " set "VENV_COMMAND=!CFG_LINE:~10!"
  )
  if defined VENV_COMMAND (
    set "VENV_COMMAND_MATCH=!VENV_COMMAND:%EXPECTED_VENV%=!"
    if /I "!VENV_COMMAND_MATCH!"=="!VENV_COMMAND!" (
      if defined VENV_REASON (
        set "VENV_REASON=!VENV_REASON! backend\.venv was created for a different repository path."
      ) else (
        set "VENV_REASON=backend\.venv was created for a different repository path."
      )
    )
  )
)

if not defined VENV_REASON (
  endlocal
  exit /b 0
)

echo [1/5] Detected broken or stale backend\.venv.
echo !VENV_REASON!
if defined VENV_COMMAND echo Recorded venv command: !VENV_COMMAND!
echo Rebuilding backend\.venv for "%REPO_ROOT%" ...
endlocal

rmdir /s /q "%VENV_DIR%"
if exist "%VENV_DIR%" (
  echo [ERROR] Failed to remove "%VENV_DIR%".
  echo Close BIGPOPA, Electron, and any Python processes still using backend\.venv, then run:
  echo   rmdir /s /q "%VENV_DIR%"
  echo After that, rerun "%SCRIPT_DIR%Setup_BIGPOPA.bat".
  exit /b 1
)
exit /b 0

:check_npm
where npm >nul 2>nul
if errorlevel 1 (
  echo [ERROR] npm not found.
  echo Install Node.js LTS ^(includes npm^) and re-run this script.
  exit /b 1
)
exit /b 0
