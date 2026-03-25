$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir '..')).Path
Set-Location $repoRoot

$setupScript = Join-Path $scriptDir 'Setup_BIGPOPA.ps1'
$venvPython = Join-Path $repoRoot 'backend\.venv\Scripts\python.exe'
$bigpopaDb = Join-Path $repoRoot 'desktop\output\bigpopa.db'
$outputRoot = Join-Path $repoRoot 'desktop\output\analysis'
$defaultLimit = 400
$defaultWindow = 25

Write-Host '========================================================'
Write-Host 'BIGPOPA trend analysis'
Write-Host "Repo: $repoRoot"
Write-Host '========================================================'

if (-not (Test-Path $venvPython)) {
  throw "backend\.venv is missing. Run `"$setupScript`" first."
}

if (-not (Test-Path $bigpopaDb)) {
  throw "Could not find `"$bigpopaDb`". Run BIGPOPA first so bigpopa.db is created."
}

& $venvPython -c "import analysis.analyze_latest_runs, matplotlib" *> $null
if ($LASTEXITCODE -ne 0) {
  throw "Analysis dependencies are not ready in backend\.venv. Run `"$setupScript`" first."
}

Write-Host 'Press Enter to accept the default shown in brackets.'
$datasetId = Read-Host 'Dataset id override [latest dataset]'
$limitInput = Read-Host "Number of latest runs to analyze [$defaultLimit]"
$windowInput = Read-Host "Rolling window size [$defaultWindow]"

$limit = if ([string]::IsNullOrWhiteSpace($limitInput)) { $defaultLimit } else { [int]$limitInput }
$window = if ([string]::IsNullOrWhiteSpace($windowInput)) { $defaultWindow } else { [int]$windowInput }

$args = @(
  '-m',
  'analysis.analyze_latest_runs',
  '--bigpopa-db',
  $bigpopaDb,
  '--limit',
  "$limit",
  '--window',
  "$window",
  '--output-root',
  $outputRoot
)
if (-not [string]::IsNullOrWhiteSpace($datasetId)) {
  $args += @('--dataset-id', $datasetId)
}

Write-Host ''
Write-Host 'Running trend analysis ...'
& $venvPython @args
if ($LASTEXITCODE -ne 0) {
  throw 'Trend analysis failed.'
}

Write-Host ''
Write-Host 'Trend analysis finished.'
