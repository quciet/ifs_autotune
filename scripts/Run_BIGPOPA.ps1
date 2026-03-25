$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir '..')).Path
Set-Location $repoRoot

$setupScript = Join-Path $scriptDir 'Setup_BIGPOPA.ps1'
$venvPython = Join-Path $repoRoot 'backend\.venv\Scripts\python.exe'

Write-Host '========================================================'
Write-Host 'BIGPOPA launch'
Write-Host "Repo: $repoRoot"
Write-Host '========================================================'

if (-not (Test-Path 'backend\pyproject.toml')) {
  throw 'Could not find backend\pyproject.toml. Run this script from inside the BIGPOPA repository.'
}

if (-not (Test-Path $venvPython)) {
  throw "backend\.venv is missing. Run `"$setupScript`" first."
}

if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
  throw 'npm not found. Install Node.js LTS (includes npm) and retry.'
}

if (Test-Path 'frontend\package.json') {
  if (-not (Test-Path 'frontend\node_modules')) {
    throw "frontend dependencies are not installed. Run `"$setupScript`" first."
  }
} else {
  Write-Host '[WARN] frontend\package.json not found; frontend background process was not started.'
}

if (Test-Path 'desktop\package.json') {
  if (-not (Test-Path 'desktop\node_modules')) {
    throw "desktop dependencies are not installed. Run `"$setupScript`" first."
  }
} else {
  throw 'desktop\package.json not found. Ensure the desktop directory exists and contains package.json, then retry.'
}

Write-Host '[1/2] Prerequisites look ready.'
Write-Host '[2/2] Launching BIGPOPA ...'
if (Test-Path 'frontend\package.json') {
  Write-Host 'Starting BIGPOPA Frontend in background (same console) ...'
  $frontendLog = Join-Path $repoRoot 'frontend\vite-dev.log'
  $null = Start-Process npm -WorkingDirectory (Join-Path $repoRoot 'frontend') -ArgumentList 'run', 'dev' -RedirectStandardOutput $frontendLog -RedirectStandardError $frontendLog -PassThru
}

Write-Host 'Starting BIGPOPA Desktop in foreground (same console) ...'
Push-Location desktop
npm run start:electron
if ($LASTEXITCODE -ne 0) { Pop-Location; throw 'Desktop launch failed.' }
Pop-Location
Write-Host 'BIGPOPA is launching in a single console. Keep this window running while using the app.'
Write-Host 'Backend Python tools run from backend\.venv on demand from Electron.'
