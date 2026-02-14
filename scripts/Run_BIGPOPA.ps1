$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir '..')).Path
Set-Location $repoRoot

$venvPython = Join-Path $repoRoot 'backend\.venv\Scripts\python.exe'

Write-Host '========================================================'
Write-Host 'BIGPOPA setup and launch'
Write-Host "Repo: $repoRoot"
Write-Host '========================================================'

if (-not (Test-Path 'backend\pyproject.toml')) {
  throw 'Could not find backend\pyproject.toml. Run this script from inside the BIGPOPA repository.'
}

function Get-PythonCommand {
  if (Get-Command py -ErrorAction SilentlyContinue) {
    & py -3.11 -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)" *> $null
    if ($LASTEXITCODE -eq 0) { return @('py', '-3.11') }
  }

  if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw 'Python 3.11+ not found. Install Python 3.11 or newer and retry.'
  }

  & python -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)" *> $null
  if ($LASTEXITCODE -ne 0) {
    throw 'Found python, but version is below 3.11. Install Python 3.11+ and retry.'
  }

  return @('python')
}

function Invoke-Python {
  param(
    [string[]]$Command,
    [string[]]$Args
  )

  $exe = $Command[0]
  $prefix = @()
  if ($Command.Length -gt 1) {
    $prefix = $Command[1..($Command.Length - 1)]
  }

  & $exe @prefix @Args
}

$pythonCmd = Get-PythonCommand
Write-Host "Using Python command: $($pythonCmd -join ' ')"

if (-not (Test-Path $venvPython)) {
  Write-Host '[1/6] Creating required virtual environment at backend\.venv ...'
  Invoke-Python -Command $pythonCmd -Args @('-m', 'venv', 'backend/.venv')
  if ($LASTEXITCODE -ne 0) { throw 'Failed to create backend\.venv.' }
} else {
  Write-Host '[1/6] Using existing required virtual environment: backend\.venv'
}

Write-Host '[2/6] Upgrading pip in backend\.venv ...'
& $venvPython -m pip install -U pip
if ($LASTEXITCODE -ne 0) { throw 'pip upgrade failed.' }

Write-Host '[3/6] Installing backend dependencies (editable install) ...'
& $venvPython -m pip install -e backend
if ($LASTEXITCODE -ne 0) {
  throw 'Backend install failed. Try deleting backend\*.egg-info and run again.'
}

if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
  throw 'npm not found. Install Node.js LTS (includes npm) and retry.'
}

if (Test-Path 'frontend\package.json') {
  Write-Host '[4/6] Installing frontend dependencies ...'
  Push-Location frontend
  npm install
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw 'Frontend npm install failed.' }
  Pop-Location
} else {
  Write-Host '[4/6] frontend\package.json not found; skipping frontend dependency install and frontend launch.'
}

if (Test-Path 'desktop\package.json') {
  Write-Host '[5/6] Installing desktop/Electron dependencies ...'
  Push-Location desktop
  npm install
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw 'Desktop npm install failed.' }
  Pop-Location
} else {
  throw 'desktop\package.json not found. Ensure the desktop directory exists and contains package.json, then retry.'
}

Write-Host '[6/6] Launching BIGPOPA ...'
if (Test-Path 'frontend\package.json') {
  Write-Host 'Starting BIGPOPA Frontend window...'
  # Use -WorkingDirectory instead of Set-Location in a child process to avoid fragile path/quoting issues.
  Start-Process powershell -WorkingDirectory (Join-Path $repoRoot 'frontend') -ArgumentList '-NoExit', '-Command', 'npm run dev' -WindowStyle Normal
} else {
  Write-Host '[WARN] frontend\package.json not found; frontend window was not started.'
}

Write-Host 'Starting BIGPOPA Desktop window...'
# Use -WorkingDirectory instead of Set-Location in a child process to avoid fragile path/quoting issues.
Start-Process powershell -WorkingDirectory (Join-Path $repoRoot 'desktop') -ArgumentList '-NoExit', '-Command', 'npm run start:electron' -WindowStyle Normal
Write-Host 'BIGPOPA is launching. Keep opened windows running while using the app.'
Write-Host 'Backend Python tools run from backend\.venv on demand from Electron.'
