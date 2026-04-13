$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir '..')).Path
Set-Location $repoRoot

$venvDir = Join-Path $repoRoot 'backend\.venv'
$venvPython = Join-Path $repoRoot 'backend\.venv\Scripts\python.exe'
$venvCfg = Join-Path $repoRoot 'backend\.venv\pyvenv.cfg'

Write-Host '========================================================'
Write-Host 'BIGPOPA setup'
Write-Host "Repo: $repoRoot"
Write-Host '========================================================'

if (-not (Test-Path 'backend\pyproject.toml')) {
  throw 'Could not find backend\pyproject.toml. Run this script from inside the BIGPOPA repository.'
}

function Get-PythonCommand {
  if (Get-Command py -ErrorAction SilentlyContinue) {
    try {
      & py -3.11 -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)" *> $null
    } catch {
      $global:LASTEXITCODE = 1
    }
    if ($LASTEXITCODE -eq 0) { return @('py', '-3.11') }
  }

  if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw 'Python 3.11+ not found. Install Python 3.11 or newer and retry.'
  }

  try {
    & python -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)" *> $null
  } catch {
    $global:LASTEXITCODE = 1
  }
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

function Normalize-PathString {
  param(
    [string]$Path
  )

  if ([string]::IsNullOrWhiteSpace($Path)) {
    return $null
  }

  $trimmed = $Path.Trim().Trim('"')
  if ([string]::IsNullOrWhiteSpace($trimmed)) {
    return $null
  }

  return [System.IO.Path]::GetFullPath($trimmed)
}

function Resolve-PythonExecutable {
  param(
    [string[]]$Command
  )

  try {
    $resolved = Invoke-Python -Command $Command -Args @('-c', 'import sys; print(sys.executable)')
  } catch {
    throw 'Failed to resolve the selected Python executable.'
  }
  if ($LASTEXITCODE -ne 0) {
    throw 'Failed to resolve the selected Python executable.'
  }

  $resolvedPath = $resolved | Select-Object -First 1
  $normalized = Normalize-PathString -Path $resolvedPath
  if (-not $normalized) {
    throw 'Failed to resolve the selected Python executable.'
  }

  return $normalized
}

function Repair-Venv {
  param(
    [string]$RepoRoot,
    [string]$VenvDir,
    [string]$VenvPython,
    [string]$VenvCfg,
    [string]$CurrentPythonExecutable
  )

  if (-not (Test-Path $VenvDir)) {
    return
  }

  $expectedVenv = Normalize-PathString -Path $VenvDir
  $resolvedVenv = Normalize-PathString -Path ((Resolve-Path $VenvDir).Path)
  if (-not [string]::Equals($resolvedVenv, $expectedVenv, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to remove backend\.venv because path validation failed.`nExpected: $expectedVenv`nResolved: $resolvedVenv"
  }

  $reasons = [System.Collections.Generic.List[string]]::new()
  $venvCommand = $null
  $venvHome = $null
  $venvExecutable = $null

  if (-not (Test-Path $VenvCfg)) {
    $reasons.Add('backend\.venv is missing pyvenv.cfg.')
  }

  if (-not (Test-Path $VenvPython)) {
    $reasons.Add('backend\.venv is missing Scripts\python.exe.')
  }

  if (Test-Path $VenvCfg) {
    foreach ($line in Get-Content $VenvCfg) {
      if ($line.StartsWith('home = ', [System.StringComparison]::OrdinalIgnoreCase)) {
        $venvHome = $line.Substring(7)
      } elseif ($line.StartsWith('executable = ', [System.StringComparison]::OrdinalIgnoreCase)) {
        $venvExecutable = $line.Substring(13)
      } elseif ($line.StartsWith('command = ', [System.StringComparison]::OrdinalIgnoreCase)) {
        $venvCommand = $line.Substring(10)
      }
    }

    if ($venvCommand -and $venvCommand.IndexOf($expectedVenv, [System.StringComparison]::OrdinalIgnoreCase) -lt 0) {
      $reasons.Add('backend\.venv was created for a different repository path.')
    }

    $recordedPythonExecutable = $null
    if ($venvExecutable) {
      $recordedPythonExecutable = Normalize-PathString -Path $venvExecutable
    } elseif ($venvHome) {
      $recordedPythonExecutable = Normalize-PathString -Path (Join-Path $venvHome 'python.exe')
    }

    if ($recordedPythonExecutable -and $CurrentPythonExecutable -and -not [string]::Equals($recordedPythonExecutable, $CurrentPythonExecutable, [System.StringComparison]::OrdinalIgnoreCase)) {
      $reasons.Add('backend\.venv was created with a different Python executable.')
    }
  }

  if ($reasons.Count -eq 0) {
    return
  }

  Write-Host '[1/5] Detected broken or stale backend\.venv.'
  Write-Host ($reasons -join ' ')
  if ($venvCommand) {
    Write-Host "Recorded venv command: $venvCommand"
  }
  Write-Host "Rebuilding backend\.venv for `"$RepoRoot`" ..."

  Remove-Item -LiteralPath $VenvDir -Recurse -Force
  if (Test-Path $VenvDir) {
    throw "Failed to remove `"$VenvDir`". Close BIGPOPA, Electron, and any Python processes still using backend\.venv, then run:`n  Remove-Item -LiteralPath `"$VenvDir`" -Recurse -Force`nAfter that, rerun `"$scriptDir\Setup_BIGPOPA.ps1`"."
  }
}

$pythonCmd = Get-PythonCommand
Write-Host "Using Python command: $($pythonCmd -join ' ')"
$pythonExecutable = Resolve-PythonExecutable -Command $pythonCmd

Repair-Venv -RepoRoot $repoRoot -VenvDir $venvDir -VenvPython $venvPython -VenvCfg $venvCfg -CurrentPythonExecutable $pythonExecutable

if (-not (Test-Path $venvPython)) {
  Write-Host '[1/5] Creating required virtual environment at backend\.venv ...'
  Invoke-Python -Command $pythonCmd -Args @('-m', 'venv', 'backend/.venv')
  if ($LASTEXITCODE -ne 0) { throw 'Failed to create backend\.venv.' }
} else {
  Write-Host '[1/5] Using existing required virtual environment: backend\.venv'
}

Write-Host '[2/5] Upgrading pip in backend\.venv ...'
& $venvPython -m pip install -U pip
if ($LASTEXITCODE -ne 0) { throw 'pip upgrade failed.' }

Write-Host '[3/5] Installing backend dependencies (editable install) ...'
& $venvPython -m pip install -e backend
if ($LASTEXITCODE -ne 0) {
  throw 'Backend install failed. Try deleting backend\*.egg-info and run again.'
}

if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
  throw 'npm not found. Install Node.js LTS (includes npm) and retry.'
}

if (Test-Path 'frontend\package.json') {
  Write-Host '[4/5] Installing frontend dependencies ...'
  Push-Location frontend
  npm install
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw 'Frontend npm install failed.' }
  Pop-Location
} else {
  Write-Host '[4/5] frontend\package.json not found; skipping frontend dependency install.'
}

if (Test-Path 'desktop\package.json') {
  Write-Host '[5/5] Installing desktop/Electron dependencies ...'
  Push-Location desktop
  npm install
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw 'Desktop npm install failed.' }
  Pop-Location
} else {
  throw 'desktop\package.json not found. Ensure the desktop directory exists and contains package.json, then retry.'
}

Write-Host ''
Write-Host 'BIGPOPA setup is complete.'
Write-Host "Use `"$scriptDir\Run_BIGPOPA.bat`" to launch the app."
Write-Host "Use Trend Analysis from the desktop app's Tune page to generate the latest trend analysis."
