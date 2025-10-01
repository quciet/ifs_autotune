const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const { spawn } = require('node:child_process');
const path = require('node:path');
const fs = require('fs');

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

ipcMain.handle('select-folder', async (_event, payload = {}) => {
  const { type, defaultPath } = payload;

  let startPath = defaultPath;
  if (!startPath && type === 'output') {
    startPath = path.join(app.getAppPath(), 'output');
    if (!fs.existsSync(startPath)) {
      fs.mkdirSync(startPath, { recursive: true });
    }
  }

  const { canceled, filePaths } = await dialog.showOpenDialog({
    properties: ['openDirectory'],
    defaultPath: startPath,
  });

  if (canceled || filePaths.length === 0) {
    return null;
  }

  return filePaths[0];
});

ipcMain.handle('select-input-file', async (_event, payload = {}) => {
  const defaultPath =
    typeof payload?.defaultPath === 'string' && payload.defaultPath.trim().length > 0
      ? payload.defaultPath
      : path.join(app.getAppPath(), 'input');

  const { canceled, filePaths } = await dialog.showOpenDialog({
    properties: ['openFile'],
    defaultPath,
    filters: [{ name: 'Excel Files', extensions: ['xlsx'] }],
  });

  if (canceled || filePaths.length === 0) {
    return null;
  }

  return filePaths[0];
});

ipcMain.handle('get-default-output-dir', async () => {
  const outputPath = path.join(app.getAppPath(), 'output');
  if (!fs.existsSync(outputPath)) {
    fs.mkdirSync(outputPath, { recursive: true });
  }
  return outputPath;
});

const REQUIRED_INPUT_SHEETS = ['AnalFunc', 'TablFunc', 'IFsVar', 'DataDict'];

function normalizeValidationPayload(payload) {
  if (typeof payload === 'string') {
    return {
      ifsPath: payload.trim() || null,
      outputPath: null,
      inputFilePath: null,
    };
  }

  if (!payload || typeof payload !== 'object') {
    return { ifsPath: null, outputPath: null, inputFilePath: null };
  }

  const rawIfs =
    typeof payload.ifsPath === 'string'
      ? payload.ifsPath
      : typeof payload.path === 'string'
      ? payload.path
      : typeof payload.folderPath === 'string'
      ? payload.folderPath
      : null;
  const rawOutput =
    typeof payload.outputPath === 'string'
      ? payload.outputPath
      : typeof payload.outputDirectory === 'string'
      ? payload.outputDirectory
      : null;
  const rawInput =
    typeof payload.inputFilePath === 'string'
      ? payload.inputFilePath
      : typeof payload.inputFile === 'string'
      ? payload.inputFile
      : null;

  const cleaned = (value) => {
    if (typeof value !== 'string') {
      return null;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  };

  return {
    ifsPath: cleaned(rawIfs),
    outputPath: cleaned(rawOutput),
    inputFilePath: cleaned(rawInput),
  };
}

function createFallbackValidation(normalized, missingFiles) {
  const sheets = REQUIRED_INPUT_SHEETS.reduce((acc, sheet) => {
    acc[sheet] = false;
    return acc;
  }, {});

  return {
    valid: false,
    missingFiles,
    pathChecks: {
      ifsFolder: {
        displayPath: normalized.ifsPath,
        exists: false,
        readable: false,
        writable: null,
        message: 'Validation failed.',
      },
      outputFolder: {
        displayPath: normalized.outputPath,
        exists: false,
        readable: false,
        writable: false,
        message: 'Validation failed.',
      },
      inputFile: {
        displayPath: normalized.inputFilePath,
        exists: false,
        readable: false,
        message: 'Validation failed.',
        sheets,
        missingSheets: [...REQUIRED_INPUT_SHEETS],
      },
    },
  };
}

function ensurePathChecks(payload, normalized) {
  if (!payload || typeof payload !== 'object') {
    return createFallbackValidation(normalized, ['Python error']);
  }

  if (!payload.pathChecks || typeof payload.pathChecks !== 'object') {
    const fallback = createFallbackValidation(normalized, ['Python error']);
    return { ...payload, pathChecks: fallback.pathChecks };
  }

  return payload;
}

ipcMain.handle('validate-ifs-folder', async (_event, rawPayload) => {
  const normalized = normalizeValidationPayload(rawPayload);
  return new Promise((resolve) => {
    const scriptPath = path.join(__dirname, '..', 'backend', 'validate_ifs.py');
    const fallbackResponse = createFallbackValidation(normalized, ['Python error']);
    let resolved = false;

    const finish = (payload) => {
      const result = ensurePathChecks(payload, normalized);
      if (
        result &&
        typeof result === 'object' &&
        Object.prototype.hasOwnProperty.call(result, 'valid')
      ) {
        if (result.valid) {
          const displayPath = result.pathChecks?.ifsFolder?.displayPath;
          lastValidatedPath = displayPath ? path.resolve(displayPath) : null;
          const candidateBaseYear = Number(result.base_year);
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
        resolve(result);
      }
    };

    try {
      if (!normalized.ifsPath) {
        const missingPathFallback = createFallbackValidation(normalized, [
          'No folder path provided',
        ]);
        finish(missingPathFallback);
        return;
      }

      const args = [scriptPath, normalized.ifsPath];
      if (normalized.outputPath) {
        args.push('--output-path', normalized.outputPath);
      }
      if (normalized.inputFilePath) {
        args.push('--input-file', normalized.inputFilePath);
      }

      const pythonProcess = spawn('python', args, {
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

ipcMain.handle('model_setup', async (_event, payload = {}) => {
  if (!lastValidatedPath) {
    return { status: 'error', message: 'Please validate an IFs folder first.' };
  }

  const desiredEndYear = Number(payload?.endYear ?? payload?.end_year ?? NaN);
  if (!Number.isFinite(desiredEndYear) || desiredEndYear <= 0) {
    return { status: 'error', message: 'Invalid end year provided.' };
  }

  const rawBaseYear = payload?.baseYear ?? payload?.base_year ?? lastBaseYear ?? null;
  const candidateBaseYear = Number(rawBaseYear);
  const baseYear = Number.isFinite(candidateBaseYear) ? candidateBaseYear : null;
  if (baseYear != null) {
    lastBaseYear = baseYear;
  }

  const parameters =
    payload && typeof payload === 'object' && typeof payload.parameters === 'object'
      ? payload.parameters
      : {};
  const coefficients =
    payload && typeof payload === 'object' && typeof payload.coefficients === 'object'
      ? payload.coefficients
      : {};
  const paramDim =
    payload && typeof payload === 'object' && typeof payload.param_dim_dict === 'object'
      ? payload.param_dim_dict
      : {};

  const scriptPath = path.join(__dirname, '..', 'backend', 'model_setup.py');
  const args = [
    scriptPath,
    '--payload',
    JSON.stringify({
      ifs_root: lastValidatedPath,
      baseYear,
      endYear: desiredEndYear,
      parameters,
      coefficients,
      param_dim_dict: paramDim,
    }),
  ];

  return new Promise((resolve) => {
    let resolved = false;

    const finish = (result) => {
      if (!resolved) {
        resolved = true;
        resolve(result);
      }
    };

    let pythonProcess;
    try {
      pythonProcess = spawn('python', args, {
        cwd: path.join(__dirname, '..'),
        windowsHide: true,
      });
    } catch (error) {
      finish({ status: 'error', message: 'Unable to launch model setup.' });
      return;
    }

    let stdout = '';
    let stderr = '';

    pythonProcess.stdout.on('data', (data) => {
      stdout += data.toString();
    });

    pythonProcess.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    pythonProcess.on('error', (error) => {
      finish({ status: 'error', message: error?.message || 'Model setup failed.' });
    });

    pythonProcess.on('close', () => {
      if (stderr.trim()) {
        finish({ status: 'error', message: stderr.trim() });
        return;
      }

      const trimmed = stdout.trim();
      if (!trimmed) {
        finish({ status: 'error', message: 'No response from model setup.' });
        return;
      }

      const lines = trimmed.split(/\r?\n/);
      for (let idx = lines.length - 1; idx >= 0; idx -= 1) {
        const candidate = lines[idx];
        try {
          const parsed = JSON.parse(candidate);
          if (parsed && parsed.status === 'success') {
            finish(parsed);
            return;
          }
          if (parsed && parsed.status === 'error') {
            finish(parsed);
            return;
          }
        } catch (error) {
          // continue searching
        }
      }

      finish({ status: 'error', message: 'Unexpected response from model setup.' });
    });
  });
});

function launchIFsRun(payload) {
  if (!lastValidatedPath) {
    return Promise.resolve({
      status: 'error',
      message: 'Please validate an IFs folder first.',
    });
  }

  const desiredEndYear = Number(payload?.end_year ?? payload?.endYear ?? 2050);
  if (!Number.isFinite(desiredEndYear) || desiredEndYear <= 0) {
    return Promise.resolve({ status: 'error', message: 'Invalid end year provided.' });
  }

  const candidateBaseYear = Number(payload?.base_year ?? payload?.baseYear ?? lastBaseYear ?? NaN);
  const baseYear = Number.isFinite(candidateBaseYear) ? candidateBaseYear : null;

  const outputDirectoryRaw =
    typeof payload?.output_dir === 'string'
      ? payload.output_dir
      : typeof payload?.outputDir === 'string'
      ? payload.outputDir
      : null;
  if (!outputDirectoryRaw || !outputDirectoryRaw.trim()) {
    return Promise.resolve({
      status: 'error',
      message: 'Please choose an output folder before running IFs.',
    });
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

    const finish = (result) => {
      if (!resolved) {
        resolved = true;
        resolve(result);
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
}

ipcMain.handle('run-ifs', async (_event, payload) => launchIFsRun(payload));
ipcMain.handle('run_ifs', async (_event, payload) => launchIFsRun(payload));
