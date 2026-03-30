$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir '..')).Path
Set-Location $repoRoot

$setupScript = Join-Path $scriptDir 'Setup_BIGPOPA.bat'
$venvPython = Join-Path $repoRoot 'backend\.venv\Scripts\python.exe'
$venvCfg = Join-Path $repoRoot 'backend\.venv\pyvenv.cfg'
$bigpopaDb = Join-Path $repoRoot 'desktop\output\bigpopa.db'
$outputRoot = Join-Path $repoRoot 'desktop\output\analysis'
$defaultLimit = 400
$defaultWindow = 25

function New-LauncherException {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Message,
        [string] $Hint,
        [string[]] $Details
    )

    $exception = [System.Exception]::new($Message)
    if ($Hint) {
        $exception.Data['Hint'] = $Hint
    }
    if ($Details -and $Details.Count -gt 0) {
        $exception.Data['Details'] = [string[]] $Details
    }
    return $exception
}

function Get-StaleVenvDetails {
    param(
        [Parameter(Mandatory = $true)]
        [string] $RepoRoot,
        [Parameter(Mandatory = $true)]
        [string] $VenvCfgPath,
        [Parameter(Mandatory = $true)]
        [string] $SetupPath
    )

    if (-not (Test-Path $VenvCfgPath)) {
        $venvPythonPath = Join-Path $RepoRoot 'backend\.venv\Scripts\python.exe'
        if (Test-Path $venvPythonPath) {
            return @{
                Message = 'backend\.venv is broken or stale for this repo location.'
                Hint = "Run `"$SetupPath`" to rebuild backend\.venv automatically for `"$RepoRoot`"."
                Details = @('backend\.venv is missing pyvenv.cfg.')
            }
        }
        return $null
    }

    $commandLine = Get-Content $VenvCfgPath |
        Where-Object { $_ -like 'command = *' } |
        Select-Object -First 1
    if (-not $commandLine) {
        return $null
    }

    $recordedCommand = $commandLine.Substring(10)
    $expectedVenv = Join-Path $RepoRoot 'backend\.venv'
    if ($recordedCommand -like "*$expectedVenv*") {
        return $null
    }

    return @{
        Message = 'backend\.venv is broken or stale for this repo location.'
        Hint = "This repo appears to have moved. Run `"$SetupPath`" to rebuild backend\.venv automatically for `"$RepoRoot`"."
        Details = @("Recorded venv command: $recordedCommand")
    }
}

function Fail-Launcher {
    param(
        [Parameter(Mandatory = $true)]
        [System.Exception] $Exception
    )

    $hint = $null
    if ($Exception.Data.Contains('Hint')) {
        $hint = [string] $Exception.Data['Hint']
    }

    $details = @()
    if ($Exception.Data.Contains('Details')) {
        $details = [string[]] $Exception.Data['Details']
    }

    Write-Host ''
    Write-Host "[ERROR] $($Exception.Message)"

    if ($hint) {
        Write-Host $hint
    }

    if ($details.Count -gt 0) {
        Write-Host ''
        Write-Host 'Python/launcher details:'
        foreach ($detail in $details) {
            Write-Host $detail
        }
    }

    Write-Host ''
    Read-Host 'Press Enter to close this window' | Out-Null
    exit 1
}

Write-Host '========================================================'
Write-Host 'BIGPOPA trend analysis'
Write-Host "Repo: $repoRoot"
Write-Host '========================================================'

try {
    if (-not (Test-Path $venvPython)) {
        throw (New-LauncherException 'backend\.venv is missing.' "Run `"$setupScript`" first.")
    }

    if (-not (Test-Path $bigpopaDb)) {
        throw (New-LauncherException "Could not find `"$bigpopaDb`"." 'Run BIGPOPA first so bigpopa.db is created.')
    }

    $staleVenv = Get-StaleVenvDetails -RepoRoot $repoRoot -VenvCfgPath $venvCfg -SetupPath $setupScript
    if ($staleVenv) {
        throw (New-LauncherException $staleVenv.Message $staleVenv.Hint $staleVenv.Details)
    }

    $precheckOutput = & $venvPython -c "import analysis.analyze_latest_runs, matplotlib" 2>&1
    if ($LASTEXITCODE -ne 0) {
        $details = @($precheckOutput | ForEach-Object { "$_" } | Where-Object { $_ })
        throw (New-LauncherException 'Analysis dependencies are not ready in backend\.venv.' "Run `"$setupScript`" to rebuild backend\.venv automatically for this repo location." $details)
    }

    Write-Host 'Press Enter to accept the default shown in brackets.'
    $datasetId = (Read-Host 'Dataset id override [latest dataset]').Trim()
    $limitInput = Read-Host "Number of latest runs to analyze [$defaultLimit]"
    $windowInput = Read-Host "Rolling window size [$defaultWindow]"

    $limit = if ([string]::IsNullOrWhiteSpace($limitInput)) { $defaultLimit } else { [int] $limitInput }
    $window = if ([string]::IsNullOrWhiteSpace($windowInput)) { $defaultWindow } else { [int] $windowInput }

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
    $analysisOutput = & $venvPython @args 2>&1
    if ($analysisOutput.Count -gt 0) {
        $analysisOutput | ForEach-Object { Write-Host $_ }
    }
    if ($LASTEXITCODE -ne 0) {
        $details = @($analysisOutput | ForEach-Object { "$_" } | Where-Object { $_ })
        throw (New-LauncherException 'Trend analysis failed.' "Review the Python error below, then rerun. If the environment looks stale, run `"$setupScript`"." $details)
    }

    Write-Host ''
    Write-Host 'Trend analysis finished.'
}
catch {
    Fail-Launcher -Exception $_.Exception
}
