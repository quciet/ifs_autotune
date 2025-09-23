const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const { spawn } = require('node:child_process');
const path = require('node:path');

const isDev = !app.isPackaged;
const STATIC_IFS_ARGS = ['-1', 'true', 'true', '1', 'false'];
let mainWindow = null;
let lastValidatedPath = null;
let lastBaseYear = null;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
  });

  if (isDev) {
    const devServerURL = process.env.VITE_DEV_SERVER_URL || 'http://localhost:5173';
    mainWindow.loadURL(devServerURL);
  } else {
    const indexPath = path.join(__dirname, 'frontend', 'dist', 'index.html');
    mainWindow.loadFile(indexPath);
  }
}

app.whenReady().then(() => {
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

ipcMain.handle('dialog:selectFolder', async () => {
  const { canceled, filePaths } = await dialog.showOpenDialog({
    properties: ['openDirectory'],
  });

  if (canceled || filePaths.length === 0) {
    return null;
  }

  return filePaths[0];
});

ipcMain.handle('validate-ifs-folder', async (_event, folderPath) => {
  return new Promise((resolve) => {
    const scriptPath = path.join(__dirname, '..', 'backend', 'validate_ifs.py');
    const fallbackResponse = { valid: false, missingFiles: ['Python error'] };
    let resolved = false;

    const finish = (payload) => {
      if (
        payload &&
        typeof payload === 'object' &&
        Object.prototype.hasOwnProperty.call(payload, 'valid')
      ) {
        if (payload.valid) {
          lastValidatedPath = path.resolve(folderPath);
          const candidateBaseYear = Number(payload.base_year);
          lastBaseYear = Number.isFinite(candidateBaseYear)
            ? candidateBaseYear
            : null;
        } else {
          lastValidatedPath = null;
          lastBaseYear = null;
        }
      }

      if (!resolved) {
        resolved = true;
        resolve(payload);
      }
    };

    try {
      if (typeof folderPath !== 'string' || folderPath.trim().length === 0) {
        finish(fallbackResponse);
        return;
      }

      const pythonProcess = spawn('python', [scriptPath, folderPath], {
        cwd: path.join(__dirname, '..'),
        windowsHide: true,
      });

      let stdout = '';
      let stderr = '';

      pythonProcess.stdout.on('data', (data) => {
        stdout += data.toString();
      });

      pythonProcess.stderr.on('data', (data) => {
        stderr += data.toString();
      });

      pythonProcess.on('error', () => {
        finish(fallbackResponse);
      });

      pythonProcess.on('close', (code) => {
        if (stderr.trim() || code !== 0) {
          finish(fallbackResponse);
          return;
        }

        try {
          const parsed = JSON.parse(stdout);
          finish(parsed);
        } catch (err) {
          finish(fallbackResponse);
        }
      });
    } catch (error) {
      finish(fallbackResponse);
    }
  });
});

ipcMain.handle('run-ifs', async (_event, payload) => {
  if (!lastValidatedPath) {
    return { status: 'error', message: 'Please validate an IFs folder first.' };
  }

  const desiredEndYear = Number(payload?.end_year ?? 2050);
  if (!Number.isFinite(desiredEndYear) || desiredEndYear <= 0) {
    return { status: 'error', message: 'Invalid end year provided.' };
  }

  const candidateBaseYear = Number(payload?.base_year ?? lastBaseYear ?? NaN);
  const baseYear = Number.isFinite(candidateBaseYear) ? candidateBaseYear : null;

  const outputDirectoryRaw =
    typeof payload?.output_dir === 'string' ? payload.output_dir : null;
  if (!outputDirectoryRaw || !outputDirectoryRaw.trim()) {
    return {
      status: 'error',
      message: 'Please choose an output folder before running IFs.',
    };
  }

  const resolvedOutputDirectory = path.resolve(outputDirectoryRaw);
  const scriptPath = path.join(__dirname, '..', 'backend', 'run_ifs.py');
  const args = [
    scriptPath,
    '--ifs-root',
    lastValidatedPath,
    '--end-year',
    String(desiredEndYear),
    '--output-dir',
    resolvedOutputDirectory,
    '--start-token',
    '5',
    '--log',
    'jrs.txt',
    '--websessionid',
    'qsdqsqsdqsdqsdqs',
  ];

  if (baseYear != null) {
    args.push('--base-year', String(baseYear));
  }

  if (isDev) {
    const ifsExecutable = path.join(lastValidatedPath, 'net8', 'ifs.exe');
    const commandPreview = [
      ifsExecutable,
      '5',
      String(desiredEndYear),
      ...STATIC_IFS_ARGS,
      '--log',
      'jrs.txt',
      '--websessionid',
      'qsdqsqsdqsdqsdqs',
    ];
    console.log('Launching IFs via runner with command:', commandPreview.join(' '));
    console.log('Output directory:', resolvedOutputDirectory);
    if (baseYear != null) {
      console.log('Base year for progress calculations:', baseYear);
    }
  }

  return new Promise((resolve) => {
    let resolved = false;
    let lastYear = null;
    let lastPercent = null;

    const clampPercent = (value) => Math.max(0, Math.min(100, value));

    const computePercent = (year) => {
      if (!Number.isFinite(baseYear)) {
        return undefined;
      }
      if (desiredEndYear === baseYear) {
        return year >= desiredEndYear ? 100 : 0;
      }
      const denominator = desiredEndYear - baseYear;
      if (denominator === 0) {
        return undefined;
      }
      return clampPercent(((year - baseYear) / denominator) * 100);
    };

    const finish = (payload) => {
      if (!resolved) {
        resolved = true;
        resolve(payload);
      }
    };

    const fallback = (message) => ({ status: 'error', message });

    let pythonProcess;
    try {
      pythonProcess = spawn('python', args, {
        cwd: path.join(__dirname, '..'),
        windowsHide: true,
      });
    } catch (error) {
      finish(fallback('Unable to launch the IFs runner.'));
      return;
    }

    let stderr = '';
    let stdoutBuffer = '';
    const jsonCandidates = [];

    const sendProgress = (year, explicitPercent) => {
      if (!mainWindow || mainWindow.isDestroyed()) {
        return;
      }

      const resolvedPercent =
        typeof explicitPercent === 'number' && Number.isFinite(explicitPercent)
          ? clampPercent(explicitPercent)
          : computePercent(year);

      if (lastYear === year && (resolvedPercent == null || resolvedPercent === lastPercent)) {
        return;
      }

      lastYear = year;
      if (resolvedPercent != null) {
        lastPercent = resolvedPercent;
      }

      const progressPayload =
        resolvedPercent != null ? { year, percent: resolvedPercent } : { year };
      mainWindow.webContents.send('ifs-progress', progressPayload);
    };

    const handleStdoutLine = (line) => {
      const trimmed = line.trim();
      if (!trimmed) {
        return;
      }

      const progressMatch = /^Year\s+(\d{1,4})/i.exec(trimmed);
      if (progressMatch) {
        const year = Number(progressMatch[1]);
        if (Number.isFinite(year)) {
          sendProgress(year);
        }
        return;
      }

      jsonCandidates.push(trimmed);
    };

    pythonProcess.stdout.on('data', (data) => {
      const chunk = data.toString();
      stdoutBuffer += chunk;

      const lines = stdoutBuffer.split(/\r?\n/);
      stdoutBuffer = lines.pop() ?? '';
      lines.forEach(handleStdoutLine);
    });

    pythonProcess.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    pythonProcess.on('error', (error) => {
      finish(fallback(error?.message || 'Failed to execute IFs.'));
    });

    pythonProcess.on('close', (code) => {
      if (stdoutBuffer) {
        handleStdoutLine(stdoutBuffer);
        stdoutBuffer = '';
      }

      if (stderr.trim()) {
        finish(fallback('IFs runner reported an error.'));
        return;
      }

      if (code !== 0 && jsonCandidates.length === 0) {
        finish(fallback('IFs runner exited unexpectedly.'));
        return;
      }

      try {
        let parsed = null;
        for (let idx = jsonCandidates.length - 1; idx >= 0; idx -= 1) {
          const candidate = jsonCandidates[idx];
          try {
            parsed = JSON.parse(candidate);
            break;
          } catch (err) {
            // Not JSON, keep searching backwards.
          }
        }

        if (parsed && parsed.status === 'success') {
          if (typeof parsed.end_year === 'number') {
            sendProgress(parsed.end_year, 100);
          }
          if (typeof parsed.base_year === 'number' && Number.isFinite(parsed.base_year)) {
            lastBaseYear = parsed.base_year;
          }
          finish(parsed);
          return;
        }

        if (parsed && parsed.status === 'error') {
          finish(parsed);
          return;
        }

        finish(fallback('Unexpected IFs runner response.'));
      } catch (error) {
        finish(fallback('Unable to parse IFs runner response.'));
      }
    });
  });
});
