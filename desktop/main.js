const { app, BrowserWindow, ipcMain, dialog, shell } = require('electron');
const { spawn } = require("child_process");
const path = require('node:path');
const fs = require('fs');

const isDev = !app.isPackaged;
const STATIC_IFS_ARGS = ['-1', 'true', 'true', '1', 'false'];
const DEFAULT_OUTPUT_DIR = () => path.join(app.getAppPath(), 'output');

const ensureDirectoryExists = (dirPath) => {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
};

const ensureAppFolders = () => {
  const outputDir = DEFAULT_OUTPUT_DIR();
  ensureDirectoryExists(outputDir);
};

const getDefaultOutputDirectory = () => {
  const outputDir = DEFAULT_OUTPUT_DIR();
  ensureDirectoryExists(outputDir);
  return outputDir;
};

const resolveExistingPath = (value) => {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  return path.resolve(trimmed);
};

const isPathWithinRoot = (targetPath, rootPath) => {
  const relative = path.relative(rootPath, targetPath);
  return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
};

const imageMimeTypeForPath = (targetPath) => {
  const extension = path.extname(targetPath).toLowerCase();
  if (extension === '.png') {
    return 'image/png';
  }
  if (extension === '.jpg' || extension === '.jpeg') {
    return 'image/jpeg';
  }
  if (extension === '.gif') {
    return 'image/gif';
  }
  if (extension === '.webp') {
    return 'image/webp';
  }
  return null;
};

let mainWindow = null;
let lastValidatedPath = null;
let lastBaseYear = null;

// Keep ML job state in the Electron main process so renderer reloads/crashes
// can re-attach to running work without restarting the Python process.
const mlJobState = {
  running: false,
  startedAt: null,
  lastUpdateAt: null,
  pid: null,
  progress: null,
  exitCode: null,
  error: null,
  ifsPath: null,
  ifsValidated: false,
  inputProfileId: null,
  outputDir: null,
  stopRequested: false,
  stopAcknowledged: false,
  finalResult: null,
  terminationReason: null,
  runConfig: null,
};

const getSafeMLJobState = () => ({
  running: mlJobState.running,
  startedAt: mlJobState.startedAt,
  lastUpdateAt: mlJobState.lastUpdateAt,
  pid: mlJobState.pid,
  progress: mlJobState.progress,
  exitCode: mlJobState.exitCode,
  error: mlJobState.error,
  ifsPath: mlJobState.ifsPath,
  ifsValidated: mlJobState.ifsValidated,
  inputProfileId: mlJobState.inputProfileId,
  outputDir: mlJobState.outputDir,
  stopRequested: mlJobState.stopRequested,
  stopAcknowledged: mlJobState.stopAcknowledged,
  finalResult: mlJobState.finalResult,
  terminationReason: mlJobState.terminationReason,
  runConfig: mlJobState.runConfig,
});

const updateMLJobContext = ({
  ifsPath,
  ifsValidated,
  inputProfileId,
  outputDir,
  runConfig,
}) => {
  if (typeof ifsPath === 'string') {
    const trimmed = ifsPath.trim();
    mlJobState.ifsPath = trimmed.length > 0 ? trimmed : null;
  }

  if (typeof ifsValidated === 'boolean') {
    mlJobState.ifsValidated = ifsValidated;
  }

  if (typeof inputProfileId === 'number' && Number.isFinite(inputProfileId) && inputProfileId > 0) {
    mlJobState.inputProfileId = inputProfileId;
  } else if (inputProfileId === null) {
    mlJobState.inputProfileId = null;
  }

  if (typeof outputDir === 'string') {
    const trimmed = outputDir.trim();
    mlJobState.outputDir = trimmed.length > 0 ? trimmed : null;
  }

  if (runConfig && typeof runConfig === 'object') {
    mlJobState.runConfig = { ...runConfig };
  }
};

const sendToRenderer = (channel, payload) => {
  if (!mainWindow || mainWindow.isDestroyed()) {
    return;
  }

  mainWindow.webContents.send(channel, payload);
};

let currentMLProcess = null;
const desktopCapabilities = Object.freeze({
  trendAnalysis: true,
  openPath: true,
  trendDatasetOptions: true,
  imagePreview: true,
});
let currentMLStopFile = null;
let currentMLFinalPayload = null;

const normalizeTerminationReason = (value) => {
  if (value === 'completed' || value === 'stopped_gracefully') {
    return value;
  }

  return null;
};

const cleanupCurrentMLStopFile = () => {
  if (!currentMLStopFile) {
    return;
  }

  try {
    if (fs.existsSync(currentMLStopFile)) {
      fs.unlinkSync(currentMLStopFile);
    }
  } catch (error) {
    console.warn('[ml] unable to remove stop file:', error);
  }

  currentMLStopFile = null;
};

const buildMLDriverResolvePayload = ({ code, parsedPayload, fallbackBaseYear, fallbackEndYear }) => {
  const parsedData =
    parsedPayload && parsedPayload.data && typeof parsedPayload.data === 'object'
      ? parsedPayload.data
      : {};
  const terminationReason = normalizeTerminationReason(parsedData.terminationReason);
  const fallbackMessage =
    terminationReason === 'stopped_gracefully'
      ? 'ML optimization stopped after the current run.'
      : code === 0
      ? 'ML optimization completed successfully.'
      : `ML optimization exited with code ${code}.`;

  return {
    status: 'success',
    stage: 'ml_driver',
    message:
      parsedPayload && typeof parsedPayload.message === 'string' && parsedPayload.message.trim().length > 0
        ? parsedPayload.message
        : fallbackMessage,
    data: {
      code: typeof code === 'number' ? code : null,
      best_model_id:
        typeof parsedData.best_model_id === 'string' && parsedData.best_model_id.trim().length > 0
          ? parsedData.best_model_id
          : null,
      best_fit_pooled:
        typeof parsedData.best_fit_pooled === 'number' && Number.isFinite(parsedData.best_fit_pooled)
          ? parsedData.best_fit_pooled
          : null,
      iterations:
        typeof parsedData.iterations === 'number' && Number.isFinite(parsedData.iterations)
          ? parsedData.iterations
          : null,
      terminationReason,
      base_year:
        typeof parsedData.base_year === 'number' && Number.isFinite(parsedData.base_year)
          ? parsedData.base_year
          : typeof fallbackBaseYear === 'number' && Number.isFinite(fallbackBaseYear)
          ? fallbackBaseYear
          : null,
      end_year:
        typeof parsedData.end_year === 'number' && Number.isFinite(parsedData.end_year)
          ? parsedData.end_year
          : typeof fallbackEndYear === 'number' && Number.isFinite(fallbackEndYear)
          ? fallbackEndYear
          : null,
      dataset_id:
        typeof parsedData.dataset_id === 'string' && parsedData.dataset_id.trim().length > 0
          ? parsedData.dataset_id.trim()
          : null,
    },
  };
};

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    minWidth: 960,
    minHeight: 700,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  mainWindow.setResizable(true);
  mainWindow.on('resize', () => {
    mainWindow.webContents.send('window-resized');
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
  ensureAppFolders();
  console.log(`[py] using: ${getVenvPython()}`);
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

ipcMain.on('app:quit', () => {
  app.quit();
});

ipcMain.handle('app:quit', async () => {
  app.quit();
  return { ok: true };
});

ipcMain.handle('select-folder', async (_event, payload = {}) => {
  const { type, defaultPath } = payload;

  let startPath = defaultPath;
  if (!startPath && type === 'output') {
    startPath = getDefaultOutputDirectory();
  }

  if (startPath && type === 'output') {
    ensureDirectoryExists(startPath);
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

ipcMain.handle('get-default-output-dir', async () => {
  return getDefaultOutputDirectory();
});

ipcMain.handle('profiles:list', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const ifsStaticId =
    typeof payload.ifsStaticId === 'number' && Number.isFinite(payload.ifsStaticId)
      ? Math.trunc(payload.ifsStaticId)
      : null;
  if (!outputFolder || !ifsStaticId || ifsStaticId <= 0) {
    throw new Error('profiles:list requires outputFolder and ifsStaticId');
  }
  return runProfileCommand('list', [
    '--output-folder',
    outputFolder,
    '--ifs-static-id',
    String(ifsStaticId),
    ...(payload.includeArchived ? ['--include-archived'] : []),
  ]);
});

ipcMain.handle('profiles:get', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const profileId =
    typeof payload.profileId === 'number' && Number.isFinite(payload.profileId)
      ? Math.trunc(payload.profileId)
      : null;
  if (!outputFolder || !profileId || profileId <= 0) {
    throw new Error('profiles:get requires outputFolder and profileId');
  }
  const args = ['--output-folder', outputFolder, '--profile-id', String(profileId)];
  if (typeof payload.ifsRoot === 'string' && payload.ifsRoot.trim()) {
    args.push('--ifs-root', payload.ifsRoot.trim());
  }
  return runProfileCommand('get', args);
});

ipcMain.handle('profiles:create', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const ifsStaticId =
    typeof payload.ifsStaticId === 'number' && Number.isFinite(payload.ifsStaticId)
      ? Math.trunc(payload.ifsStaticId)
      : null;
  const name = typeof payload.name === 'string' ? payload.name.trim() : '';
  if (!outputFolder || !ifsStaticId || ifsStaticId <= 0 || !name) {
    throw new Error('profiles:create requires outputFolder, ifsStaticId, and name');
  }
  const args = [
    '--output-folder',
    outputFolder,
    '--ifs-static-id',
    String(ifsStaticId),
    '--name',
    name,
  ];
  if (typeof payload.description === 'string' && payload.description.trim()) {
    args.push('--description', payload.description.trim());
  }
  return runProfileCommand('create', args);
});

ipcMain.handle('profiles:updateMeta', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const profileId =
    typeof payload.profileId === 'number' && Number.isFinite(payload.profileId)
      ? Math.trunc(payload.profileId)
      : null;
  if (!outputFolder || !profileId || profileId <= 0) {
    throw new Error('profiles:updateMeta requires outputFolder and profileId');
  }
  const args = ['--output-folder', outputFolder, '--profile-id', String(profileId)];
  if (typeof payload.name === 'string') {
    args.push('--name', payload.name);
  }
  if (typeof payload.description === 'string') {
    args.push('--description', payload.description);
  }
  return runProfileCommand('update-meta', args);
});

ipcMain.handle('profiles:duplicate', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const profileId =
    typeof payload.profileId === 'number' && Number.isFinite(payload.profileId)
      ? Math.trunc(payload.profileId)
      : null;
  const name = typeof payload.name === 'string' ? payload.name.trim() : '';
  if (!outputFolder || !profileId || profileId <= 0 || !name) {
    throw new Error('profiles:duplicate requires outputFolder, profileId, and name');
  }
  return runProfileCommand('duplicate', [
    '--output-folder',
    outputFolder,
    '--profile-id',
    String(profileId),
    '--name',
    name,
  ]);
});

ipcMain.handle('profiles:archive', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const profileId =
    typeof payload.profileId === 'number' && Number.isFinite(payload.profileId)
      ? Math.trunc(payload.profileId)
      : null;
  if (!outputFolder || !profileId || profileId <= 0) {
    throw new Error('profiles:archive requires outputFolder and profileId');
  }
  return runProfileCommand('archive', [
    '--output-folder',
    outputFolder,
    '--profile-id',
    String(profileId),
    '--archived',
    payload.archived === false ? 'false' : 'true',
  ]);
});

ipcMain.handle('profiles:delete', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const profileId =
    typeof payload.profileId === 'number' && Number.isFinite(payload.profileId)
      ? Math.trunc(payload.profileId)
      : null;
  if (!outputFolder || !profileId || profileId <= 0) {
    throw new Error('profiles:delete requires outputFolder and profileId');
  }
  return runProfileCommand('delete', [
    '--output-folder',
    outputFolder,
    '--profile-id',
    String(profileId),
  ]);
});

ipcMain.handle('profiles:saveParameters', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const profileId =
    typeof payload.profileId === 'number' && Number.isFinite(payload.profileId)
      ? Math.trunc(payload.profileId)
      : null;
  if (!outputFolder || !profileId || profileId <= 0) {
    throw new Error('profiles:saveParameters requires outputFolder and profileId');
  }
  return runProfileCommand(
    'save-parameters',
    ['--output-folder', outputFolder, '--profile-id', String(profileId), '--stdin-json'],
    { stdinData: JSON.stringify(payload.rows ?? []) },
  );
});

ipcMain.handle('profiles:saveCoefficients', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const profileId =
    typeof payload.profileId === 'number' && Number.isFinite(payload.profileId)
      ? Math.trunc(payload.profileId)
      : null;
  if (!outputFolder || !profileId || profileId <= 0) {
    throw new Error('profiles:saveCoefficients requires outputFolder and profileId');
  }
  return runProfileCommand(
    'save-coefficients',
    ['--output-folder', outputFolder, '--profile-id', String(profileId), '--stdin-json'],
    { stdinData: JSON.stringify(payload.rows ?? []) },
  );
});

ipcMain.handle('profiles:saveOutputs', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const profileId =
    typeof payload.profileId === 'number' && Number.isFinite(payload.profileId)
      ? Math.trunc(payload.profileId)
      : null;
  if (!outputFolder || !profileId || profileId <= 0) {
    throw new Error('profiles:saveOutputs requires outputFolder and profileId');
  }
  return runProfileCommand(
    'save-outputs',
    ['--output-folder', outputFolder, '--profile-id', String(profileId), '--stdin-json'],
    { stdinData: JSON.stringify(payload.rows ?? []) },
  );
});

ipcMain.handle('profiles:saveMlSettings', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const profileId =
    typeof payload.profileId === 'number' && Number.isFinite(payload.profileId)
      ? Math.trunc(payload.profileId)
      : null;
  if (!outputFolder || !profileId || profileId <= 0) {
    throw new Error('profiles:saveMlSettings requires outputFolder and profileId');
  }
  return runProfileCommand(
    'save-ml-settings',
    ['--output-folder', outputFolder, '--profile-id', String(profileId), '--stdin-json'],
    { stdinData: JSON.stringify(payload.mlSettings ?? {}) },
  );
});

ipcMain.handle('profiles:validate', async (_event, payload = {}) => {
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const profileId =
    typeof payload.profileId === 'number' && Number.isFinite(payload.profileId)
      ? Math.trunc(payload.profileId)
      : null;
  if (!outputFolder || !profileId || profileId <= 0) {
    throw new Error('profiles:validate requires outputFolder and profileId');
  }
  const args = ['--output-folder', outputFolder, '--profile-id', String(profileId)];
  if (typeof payload.ifsRoot === 'string' && payload.ifsRoot.trim()) {
    args.push('--ifs-root', payload.ifsRoot.trim());
  }
  return runProfileCommand('validate', args);
});

const REQUIRED_INPUT_SHEETS = ['AnalFunc', 'TablFunc', 'IFsVar', 'DataDict'];

const RUN_IFS_SCRIPT_NAMES = new Set(['run_ifs.py', 'ml_driver.py']);

const getRepoRoot = () => path.join(__dirname, '..');
const getBackendDir = () => path.join(getRepoRoot(), 'backend');

// Always run backend scripts with backend/.venv so Electron uses consistent, required dependencies.
function getVenvPython() {
  const venvPython =
    process.platform === 'win32'
      ? path.join(getBackendDir(), '.venv', 'Scripts', 'python.exe')
      : path.join(getBackendDir(), '.venv', 'bin', 'python');

  if (fs.existsSync(venvPython)) {
    return venvPython;
  }

  const envPython = process.env.BIGPOPA_PYTHON;
  if (typeof envPython === 'string' && envPython.trim().length > 0) {
    return envPython.trim();
  }

  console.warn('[py] WARNING: backend\\.venv not found; falling back to system python');
  return 'python';
}

function spawnPython(args, options = {}) {
  const pythonExe = getVenvPython();
  const quiet = options && typeof options === 'object' ? options.quiet === true : false;
  if (!quiet) {
    console.log(`[py] using: ${pythonExe}`);
  }
  return spawn(pythonExe, args, options);
}

function runPythonScript(scriptName, args = [], options = {}) {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(getBackendDir(), scriptName);
    const pythonArgs = ['-u', scriptPath, ...args];
    const quiet = options && typeof options === 'object' ? options.quiet === true : false;
    const progressChannel =
      options && typeof options === 'object' && typeof options.progressChannel === 'string'
        ? options.progressChannel.trim()
        : '';
    const stdinData =
      options && typeof options === 'object' && typeof options.stdinData === 'string'
        ? options.stdinData
        : null;

    const processOptions = {
      cwd: getRepoRoot(),
      windowsHide: true,
      shell: false,
      env: {
        ...process.env,
        PYTHONUNBUFFERED: '1',
      },
    };

    const isRunIFsScript = RUN_IFS_SCRIPT_NAMES.has(scriptName);
    const isMlDriver = scriptName === 'ml_driver.py';
    const isMlProgress = scriptName === 'ml_progress.py';
    const window = mainWindow;
    let stdout = '';
    let stderr = '';
    let stdoutBuffer = '';

    const sendProgressUpdate = (message) => {
      if (!progressChannel || !window || window.isDestroyed()) {
        return;
      }

      const normalized = typeof message === 'string' ? message.trim() : '';
      if (!normalized) {
        return;
      }

      window.webContents.send(progressChannel, normalized);
    };

    const emitRunIFsProgress = (line) => {
      if (!isRunIFsScript || !window || window.isDestroyed()) {
        return;
      }

      const trimmed = typeof line === 'string' ? line.trim() : '';
      if (!trimmed) {
        return;
      }

      const yearMatch = trimmed.match(/Year\s+(\d{1,4})/i);
      if (yearMatch) {
        const year = Number.parseInt(yearMatch[1], 10);
        if (Number.isFinite(year)) {
          window.webContents.send('ifs-progress', { year });
        }
      }

      console.log('IFS stdout:', trimmed);
    };

    const handleStdoutLine = (line) => {
      const trimmed = line.trim();
      if (!trimmed) return;

      if (!isMlProgress) {
        console.log("[PYTHON]", trimmed);
      }

      let progressMessage = trimmed;
      try {
        const parsed = JSON.parse(trimmed);
        if (parsed && typeof parsed === "object") {
          const parsedMessage =
            typeof parsed.message === "string" ? parsed.message.trim() : "";
          if (parsedMessage.length > 0) {
            // Prefix with status so we see [debug], [warn], [info]
            progressMessage = `[${parsed.status || "log"}] ${parsedMessage}`;
          } else if (parsed.status) {
            progressMessage = `[${parsed.status}] ${trimmed}`;
          }
        }
      } catch {
        // Not JSON, keep as raw line
      }

      if (progressChannel) {
        sendProgressUpdate(progressMessage);
      }
      emitRunIFsProgress(trimmed);
    };

    const pythonProcess = spawnPython(pythonArgs, { ...processOptions, quiet });

    if (stdinData !== null) {
      pythonProcess.stdin.write(stdinData);
      pythonProcess.stdin.end();
    }

    pythonProcess.stdout.on('data', (data) => {
      const text = data.toString();
      stdout += text;
      stdoutBuffer += text;

      if (isMlDriver && window && !window.isDestroyed()) {
        window.webContents.send('ml-progress', text);
      }

      const lines = stdoutBuffer.split(/\r?\n/);
      stdoutBuffer = lines.pop() ?? '';

      for (const line of lines) {
        handleStdoutLine(line);
      }
    });

    pythonProcess.stderr.on('data', (data) => {
      const text = data.toString();
      console.error('[PYTHON-ERR]', text.trim());
      stderr += text;
    });

    pythonProcess.on('error', (error) => {
      reject(error);
    });

    pythonProcess.on('close', (code) => {
      if (stdoutBuffer.trim()) {
        handleStdoutLine(stdoutBuffer);
        stdoutBuffer = '';
      }

      if (code !== 0) {
        reject(new Error(stderr.trim() || `Exited with code ${code}`));
        return;
      }

      try {
        const trimmedStdout = stdout.trim();
        if (!trimmedStdout) {
          throw new Error('No stdout to parse');
        }

        // Search for the last valid JSON line in the stdout
        const lines = trimmedStdout.split(/\r?\n/);
        let parsedResult = null;
        for (let i = lines.length - 1; i >= 0; i--) {
          const line = lines[i].trim();
          if (!line) continue;
          try {
            parsedResult = JSON.parse(line);
            break;
          } catch {
            continue; // skip non-JSON lines like "Year 2030"
          }
        }

        if (parsedResult) {
          resolve(parsedResult);
        } else {
          console.warn('[BIGPOPA] No JSON found in stdout. Returning raw log output.');
          resolve({ status: 'raw_output', message: trimmedStdout });
        }
      } catch (error) {
        reject(new Error(`Failed to parse Python output safely: ${error.message}`));
      }
    });
  });
}

function normalizeValidationPayload(payload) {
  if (typeof payload === 'string') {
    return {
      ifsPath: payload.trim() || null,
      outputPath: null,
      inputProfileId: null,
    };
  }

  if (!payload || typeof payload !== 'object') {
    return { ifsPath: null, outputPath: null, inputProfileId: null };
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
  const rawProfileId =
    typeof payload.inputProfileId === 'number'
      ? payload.inputProfileId
      : typeof payload.profileId === 'number'
      ? payload.profileId
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
    inputProfileId:
      Number.isFinite(rawProfileId) && rawProfileId > 0 ? Math.trunc(rawProfileId) : null,
  };
}

async function runProfileCommand(command, args = [], options = {}) {
  const response = await runPythonScript(path.join('db', 'input_profiles.py'), [command, ...args], options);
  if (!response || typeof response !== 'object') {
    throw new Error('Unexpected profile response.');
  }
  if (response.ok !== true) {
    throw new Error(
      typeof response.error === 'string' && response.error.trim().length > 0
        ? response.error
        : 'Profile request failed.',
    );
  }
  return response.data;
}

function createFallbackValidation(normalized, missingFiles) {
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
      inputProfile: {
        displayPath:
          typeof normalized.inputProfileId === 'number'
            ? `Profile ${normalized.inputProfileId}`
            : null,
        exists: Boolean(normalized.inputProfileId),
        readable: Boolean(normalized.inputProfileId),
        writable: null,
        message: 'Validation failed.',
        profileId: normalized.inputProfileId,
        valid: false,
        errors: [],
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


ipcMain.handle('debug:pythonPath', async () => {
  const pythonExe = getVenvPython();
  console.log(`[py] using: ${pythonExe}`);
  return pythonExe;
});

ipcMain.handle('validate-ifs-folder', async (_event, rawPayload) => {
  const normalized = normalizeValidationPayload(rawPayload);
  const fallbackResponse = createFallbackValidation(normalized, ['Python error']);
  updateMLJobContext({
    ifsPath: normalized.ifsPath ?? '',
    ifsValidated: false,
    inputProfileId: normalized.inputProfileId,
    outputDir: normalized.outputPath ?? '',
  });
  console.log('[ml] validation updated', {
    ifsPath: mlJobState.ifsPath,
    ifsValidated: mlJobState.ifsValidated,
  });

  if (!normalized.ifsPath) {
    lastValidatedPath = null;
    lastBaseYear = null;
    return createFallbackValidation(normalized, ['No folder path provided']);
  }

  const args = [normalized.ifsPath];
  if (normalized.outputPath) {
    args.push('--output-path', normalized.outputPath);
  }
  if (normalized.inputProfileId) {
    args.push('--input-profile-id', String(normalized.inputProfileId));
  }

  try {
    const response = await runPythonScript('validate_ifs.py', args);
    // === BIGPOPA internal tool check ===
    try {
      const parquetReaderPath = path.join(__dirname, '..', 'backend', 'tools', 'ParquetReaderlite.exe');
      const parquetReaderExists = fs.existsSync(parquetReaderPath);

      // Add to requirements list
      if (!response.requirements) response.requirements = [];
      response.requirements.push({
        file: 'backend/tools/ParquetReaderlite.exe',
        exists: parquetReaderExists,
      });

      // If missing, mark as invalid and add to missingFiles list
      if (!parquetReaderExists) {
        if (!response.missingFiles) response.missingFiles = [];
        response.missingFiles.push('backend/tools/ParquetReaderlite.exe (missing from BIGPOPA app)');
        response.valid = false;
      }

      console.log(
        `[BIGPOPA] Checked internal tool at: ${parquetReaderPath} - exists=${parquetReaderExists}`
      );
    } catch (err) {
      console.warn('[BIGPOPA] Failed to verify ParquetReaderlite.exe:', err);
    }
    const result = ensurePathChecks(response, normalized);
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
        updateMLJobContext({
          ifsPath: lastValidatedPath ?? normalized.ifsPath ?? '',
          ifsValidated: true,
          inputProfileId: normalized.inputProfileId,
          outputDir: normalized.outputPath ?? '',
          runConfig: {
            baseYear: lastBaseYear,
          },
        });
      } else {
        lastValidatedPath = null;
        lastBaseYear = null;
        updateMLJobContext({
          ifsPath: normalized.ifsPath ?? '',
          ifsValidated: false,
          inputProfileId: normalized.inputProfileId,
          outputDir: normalized.outputPath ?? '',
        });
      }
    }
    console.log('[ml] validation updated', {
      ifsPath: mlJobState.ifsPath,
      ifsValidated: mlJobState.ifsValidated,
    });
    return result;
  } catch (error) {
    lastValidatedPath = null;
    lastBaseYear = null;
    updateMLJobContext({
      ifsPath: normalized.ifsPath ?? '',
      ifsValidated: false,
      inputProfileId: normalized.inputProfileId,
      outputDir: normalized.outputPath ?? '',
    });
    console.log('[ml] validation updated', {
      ifsPath: mlJobState.ifsPath,
      ifsValidated: mlJobState.ifsValidated,
    });
    return ensurePathChecks(fallbackResponse, normalized);
  }
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
  const scriptPath = path.join(__dirname, '..', 'backend', 'ml_driver.py');
  const bigpopaPath = path.join(resolvedOutputDirectory, 'bigpopa.db');
  const args = [
    scriptPath,
    '--ifs-root',
    lastValidatedPath,
    '--end-year',
    String(desiredEndYear),
    '--output-folder',
    resolvedOutputDirectory,
    '--bigpopa-db',
    bigpopaPath,
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
      pythonProcess = spawnPython(args, {
        cwd: getRepoRoot(),
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
ipcMain.handle("run-ml", async (_event, args) => {
  if (mlJobState.running) {
    return {
      code: null,
      alreadyRunning: true,
      job: getSafeMLJobState(),
    };
  }

  return new Promise((resolve, reject) => {
    let settled = false;

    const resolveOnce = (value) => {
      if (!settled) {
        settled = true;
        resolve(value);
      }
    };

    const rejectOnce = (error) => {
      if (!settled) {
        settled = true;
        reject(error);
      }
    };

    try {
      if (
        typeof args?.inputProfileId !== 'number' ||
        !Number.isFinite(args.inputProfileId) ||
        args.inputProfileId <= 0
      ) {
        throw new Error('run-ml requires an inputProfileId');
      }

      cleanupCurrentMLStopFile();
      currentMLFinalPayload = null;

      const stopFilePath = path.join(
        args.outputFolder,
        `.bigpopa-ml-stop-${Date.now()}.signal`,
      );
      currentMLStopFile = stopFilePath;

      if (fs.existsSync(stopFilePath)) {
        fs.unlinkSync(stopFilePath);
      }

      const py = spawnPython(
        (() => {
          const baseArgs = [
            "-u",
            path.join(getBackendDir(), "ml_driver.py"),
            "--ifs-root", args.ifsRoot,
            "--end-year", args.endYear,
            "--output-folder", args.outputFolder,
            "--initial-model-id", args.initialModelId,
            "--input-profile-id", String(args.inputProfileId),
            "--bigpopa-db", path.join(args.outputFolder, "bigpopa.db"),
            "--stop-file", stopFilePath,
          ];

          if (args.baseYear != null) {
            baseArgs.push("--base-year", String(args.baseYear));
          }

          if (typeof args.artifactRetentionMode === "string" && args.artifactRetentionMode.trim().length > 0) {
            baseArgs.push("--artifact-retention", args.artifactRetentionMode.trim());
          }

          return baseArgs;
        })(),
        {
          shell: true,
          cwd: getRepoRoot(),
          env: {
            ...process.env,
            PYTHONUNBUFFERED: "1",
          },
        }
      );

      currentMLProcess = py;
      mlJobState.running = true;
      mlJobState.startedAt = Date.now();
      mlJobState.lastUpdateAt = Date.now();
      mlJobState.pid = py.pid ?? null;
      mlJobState.progress = null;
      mlJobState.exitCode = null;
      mlJobState.error = null;
      mlJobState.stopRequested = false;
      mlJobState.stopAcknowledged = false;
      mlJobState.finalResult = null;
      mlJobState.terminationReason = null;
      updateMLJobContext({
        ifsPath: args.ifsRoot,
        ifsValidated: true,
        inputProfileId:
          typeof args.inputProfileId === 'number' && Number.isFinite(args.inputProfileId)
            ? args.inputProfileId
            : null,
        outputDir: args.outputFolder ?? '',
        runConfig: {
          endYear: args.endYear,
          baseYear: args.baseYear ?? null,
          initialModelId: args.initialModelId ?? null,
          datasetId:
            typeof args.datasetId === 'string' && args.datasetId.trim().length > 0
              ? args.datasetId.trim()
              : null,
        },
      });
      console.log(`[ml] started pid=${mlJobState.pid ?? 'unknown'}`);

      let stdoutBuffer = '';

      const handleLine = (line) => {
        const trimmed = line.trim();
        if (!trimmed) return;

        mlJobState.lastUpdateAt = Date.now();
        console.log("[ML-DRIVER]", trimmed);

        if (trimmed.startsWith("[ML-STATUS]")) {
          const progressMatch = trimmed.match(/\[(\d+)\/(\d+)\]/);
          if (progressMatch) {
            mlJobState.progress = {
              done: Number.parseInt(progressMatch[1], 10),
              total: Number.parseInt(progressMatch[2], 10),
              text: `${progressMatch[1]}/${progressMatch[2]}`,
            };
          }

          if (trimmed.toLowerCase().includes('graceful stop acknowledged')) {
            mlJobState.stopAcknowledged = true;
          }

          sendToRenderer("ml-log", trimmed);
          return;
        }

        const yearMatch = trimmed.match(/^Year\s+(\d{1,4})/i);
        if (yearMatch) {
          const year = Number.parseInt(yearMatch[1], 10);
          if (Number.isFinite(year)) {
            sendToRenderer("ifs-progress", { year });
          }
          return;
        }

        let statusMessage = trimmed;
        try {
          const parsed = JSON.parse(trimmed);
          if (parsed && typeof parsed === "object") {
            if (parsed.stage === 'ml_driver' && parsed.status === 'success') {
              currentMLFinalPayload = parsed;

              const finalData = parsed.data && typeof parsed.data === 'object' ? parsed.data : {};
              mlJobState.finalResult = {
                best_model_id:
                  typeof finalData.best_model_id === 'string' ? finalData.best_model_id : null,
                best_fit_pooled:
                  typeof finalData.best_fit_pooled === 'number' ? finalData.best_fit_pooled : null,
                iterations:
                  typeof finalData.iterations === 'number' ? finalData.iterations : null,
              };
              mlJobState.terminationReason = normalizeTerminationReason(finalData.terminationReason);
            }

            if (parsed.stage === 'ml_driver' && parsed.status === 'error') {
              mlJobState.error =
                typeof parsed.message === 'string' && parsed.message.trim().length > 0
                  ? parsed.message
                  : mlJobState.error;
            }

            const parsedMessage =
              typeof parsed.message === "string" ? parsed.message.trim() : "";
            if (parsedMessage.length > 0) {
              statusMessage = `[${parsed.status || "log"}] ${parsedMessage}`;
            } else if (parsed.status) {
              statusMessage = `[${parsed.status}] ${trimmed}`;
            }
          }
        } catch {
          // Not JSON, treat as raw text status update
        }

        sendToRenderer("model-setup-progress", statusMessage);
      };

      py.stdout.on("data", (data) => {
        stdoutBuffer += data.toString();
        const lines = stdoutBuffer.split(/\r?\n/);
        stdoutBuffer = lines.pop() ?? '';
        lines.forEach(handleLine);
      });

      py.stderr.on("data", (data) => {
        const text = data.toString().trim();
        if (text) {
          mlJobState.lastUpdateAt = Date.now();
          mlJobState.error = text;
          console.error("[ML-DRIVER-ERR]", text);
          sendToRenderer("ml-log", `[ERROR] ${text}`);
        }
      });

      py.on('error', (error) => {
        currentMLProcess = null;
        mlJobState.running = false;
        mlJobState.lastUpdateAt = Date.now();
        mlJobState.error = error?.message || 'Failed to execute ML driver.';
        mlJobState.exitCode = null;
        console.log('[ml] exited code=null');
        cleanupCurrentMLStopFile();
        rejectOnce(error);
      });

      py.on("close", (code) => {
        if (stdoutBuffer.trim()) {
          handleLine(stdoutBuffer);
        }

        currentMLProcess = null;
        mlJobState.running = false;
        mlJobState.lastUpdateAt = Date.now();
        mlJobState.exitCode = typeof code === 'number' ? code : null;

        const resolvedPayload = buildMLDriverResolvePayload({
          code,
          parsedPayload: currentMLFinalPayload,
          fallbackBaseYear: args.baseYear ?? null,
          fallbackEndYear: args.endYear ?? null,
        });
        mlJobState.finalResult =
          resolvedPayload.data.best_model_id ||
          resolvedPayload.data.best_fit_pooled != null ||
          resolvedPayload.data.iterations != null
            ? {
                best_model_id: resolvedPayload.data.best_model_id,
                best_fit_pooled: resolvedPayload.data.best_fit_pooled,
                iterations: resolvedPayload.data.iterations,
              }
            : null;
        mlJobState.terminationReason = resolvedPayload.data.terminationReason;

        console.log(`[ml] exited code=${mlJobState.exitCode ?? 'null'}`);
        cleanupCurrentMLStopFile();
        resolveOnce(resolvedPayload);
      });
    } catch (err) {
      currentMLProcess = null;
      mlJobState.running = false;
      mlJobState.lastUpdateAt = Date.now();
      mlJobState.error = err instanceof Error ? err.message : String(err);
      mlJobState.exitCode = null;
      cleanupCurrentMLStopFile();
      rejectOnce(err);
    }
  });
});
ipcMain.handle('ml:requestStop', async () => {
  if (!mlJobState.running || !currentMLProcess || !currentMLStopFile) {
    return {
      accepted: false,
      alreadyRequested: false,
      stopRequested: mlJobState.stopRequested,
      stopAcknowledged: mlJobState.stopAcknowledged,
    };
  }

  if (mlJobState.stopRequested) {
    return {
      accepted: false,
      alreadyRequested: true,
      stopRequested: true,
      stopAcknowledged: mlJobState.stopAcknowledged,
    };
  }

  fs.writeFileSync(
    currentMLStopFile,
    JSON.stringify({ requestedAt: new Date().toISOString() }),
    'utf8',
  );
  mlJobState.stopRequested = true;
  mlJobState.lastUpdateAt = Date.now();

  return {
    accepted: true,
    alreadyRequested: false,
    stopRequested: true,
    stopAcknowledged: mlJobState.stopAcknowledged,
  };
});
ipcMain.handle('ml:jobStatus', async () => {
  return getSafeMLJobState();
});
ipcMain.handle('ml:lowerPanelViewChanged', async (_event, payload = {}) => {
  const view =
    payload?.view === 'progress' || payload?.view === 'log'
      ? payload.view
      : null;
  const datasetId =
    typeof payload?.datasetId === 'string' && payload.datasetId.trim().length > 0
      ? payload.datasetId.trim()
      : null;
  if (view === 'progress') {
    console.log(
      datasetId
        ? `[ml] switched to ML Progress view (dataset_id=${datasetId})`
        : '[ml] switched to ML Progress view',
    );
  } else if (view === 'log') {
    console.log('[ml] switched to ML Optimization Log view');
  }
  return { ok: true };
});
ipcMain.handle('ml:getProgressHistory', async (_event, payload = {}) => {
  const outputDir =
    typeof payload?.outputDir === "string" && payload.outputDir.trim().length > 0
      ? payload.outputDir.trim()
      : mlJobState.outputDir;
  const datasetId =
    typeof payload?.datasetId === "string" && payload.datasetId.trim().length > 0
      ? payload.datasetId.trim()
      : typeof mlJobState.runConfig?.datasetId === "string" &&
        mlJobState.runConfig.datasetId.trim().length > 0
      ? mlJobState.runConfig.datasetId.trim()
      : null;
  const modelId = !datasetId
    ? typeof payload?.modelId === "string" && payload.modelId.trim().length > 0
      ? payload.modelId.trim()
      : typeof mlJobState.runConfig?.initialModelId === "string" &&
        mlJobState.runConfig.initialModelId.trim().length > 0
      ? mlJobState.runConfig.initialModelId.trim()
      : null
    : null;
  const rawSinceRunId =
    typeof payload?.sinceRunId === "number"
      ? payload.sinceRunId
      : typeof payload?.sinceProgressRowId === "number"
      ? payload.sinceProgressRowId
      : payload?.sinceOutputRowId;
  const sinceRunId =
    typeof rawSinceRunId === "number" &&
    Number.isFinite(rawSinceRunId) &&
    rawSinceRunId > 0
      ? Math.trunc(rawSinceRunId)
      : null;

  if (!outputDir) {
    return {
      status: "success",
      stage: "ml_progress",
      message: "No output directory selected.",
      data: { dataset_id: datasetId, trials: [] },
    };
  }

  if (!datasetId && !modelId) {
    return {
      status: "success",
      stage: "ml_progress",
      message: "No dataset is available yet to resolve ML progress history.",
      data: { dataset_id: null, trials: [] },
    };
  }

  const args = [
    "--bigpopa-db",
    path.join(outputDir, "bigpopa.db"),
  ];

  if (datasetId) {
    args.push("--dataset-id", datasetId);
  } else if (modelId) {
    args.push("--model-id", modelId);
  }

  if (sinceRunId != null) {
    args.push("--since-run-id", String(sinceRunId));
  }

  return runPythonScript("ml_progress.py", args, { quiet: true });
});
ipcMain.handle('desktop:getCapabilities', async () => ({
  ...desktopCapabilities,
}));
ipcMain.handle('analysis:runTrendAnalysis', async (_event, payload = {}) => {
  const outputDir =
    typeof payload?.outputDir === 'string' && payload.outputDir.trim().length > 0
      ? payload.outputDir.trim()
      : null;
  const datasetId =
    typeof payload?.datasetId === 'string' && payload.datasetId.trim().length > 0
      ? payload.datasetId.trim()
      : null;
  const rawLimit = Number(payload?.limit);
  const rawWindow = Number(payload?.window);
  const limit = Number.isFinite(rawLimit) ? Math.trunc(rawLimit) : 400;
  const window = Number.isFinite(rawWindow) ? Math.trunc(rawWindow) : 25;

  if (!outputDir) {
    return {
      status: 'error',
      stage: 'trend_analysis',
      message: 'Choose an output folder before running trend analysis.',
      data: {},
    };
  }

  if (limit <= 0) {
    return {
      status: 'error',
      stage: 'trend_analysis',
      message: 'Trend analysis limit must be greater than 0.',
      data: { limit },
    };
  }

  if (window <= 0) {
    return {
      status: 'error',
      stage: 'trend_analysis',
      message: 'Trend analysis rolling window must be greater than 0.',
      data: { window },
    };
  }

  const bigpopaDb = path.join(outputDir, 'bigpopa.db');
  if (!fs.existsSync(bigpopaDb)) {
    return {
      status: 'error',
      stage: 'trend_analysis',
      message: `Could not find "${bigpopaDb}".`,
      data: { bigpopa_db: bigpopaDb },
    };
  }

  const args = [
    '--bigpopa-db',
    bigpopaDb,
    '--limit',
    String(limit),
    '--window',
    String(window),
    '--output-root',
    path.join(outputDir, 'analysis'),
  ];

  if (datasetId) {
    args.push('--dataset-id', datasetId);
  }

  return runPythonScript('trend_analysis.py', args, { quiet: true });
});
ipcMain.handle('analysis:getTrendDatasetOptions', async (_event, payload = {}) => {
  const outputDir = resolveExistingPath(payload?.outputDir);

  if (!outputDir) {
    return {
      status: 'error',
      stage: 'trend_dataset_options',
      message: 'Choose an output folder before loading dataset options.',
      data: { dataset_ids: [], latest_dataset_id: null },
    };
  }

  const bigpopaDb = path.join(outputDir, 'bigpopa.db');
  if (!fs.existsSync(bigpopaDb)) {
    return {
      status: 'error',
      stage: 'trend_dataset_options',
      message: `Could not find "${bigpopaDb}".`,
      data: { bigpopa_db: bigpopaDb, dataset_ids: [], latest_dataset_id: null },
    };
  }

  return runPythonScript(
    'trend_dataset_options.py',
    ['--bigpopa-db', bigpopaDb],
    { quiet: true },
  );
});
ipcMain.handle('analysis:getImagePreview', async (_event, payload = {}) => {
  const targetPath = resolveExistingPath(payload?.targetPath);
  const allowedRoot = resolveExistingPath(payload?.allowedRoot);

  if (!targetPath || !allowedRoot) {
    return {
      ok: false,
      error: 'A preview path and allowed root are required.',
    };
  }

  if (!fs.existsSync(allowedRoot) || !fs.statSync(allowedRoot).isDirectory()) {
    return {
      ok: false,
      error: 'The requested preview root is unavailable.',
    };
  }

  if (!isPathWithinRoot(targetPath, allowedRoot)) {
    return {
      ok: false,
      error: 'Preview requests are limited to generated analysis artifacts.',
    };
  }

  if (!fs.existsSync(targetPath) || !fs.statSync(targetPath).isFile()) {
    return {
      ok: false,
      error: 'The requested preview file was not found.',
    };
  }

  const mimeType = imageMimeTypeForPath(targetPath);
  if (!mimeType) {
    return {
      ok: false,
      error: 'Only image previews are supported for this artifact.',
    };
  }

  const encoded = fs.readFileSync(targetPath).toString('base64');
  return {
    ok: true,
    dataUrl: `data:${mimeType};base64,${encoded}`,
    mimeType,
    targetPath,
  };
});
ipcMain.handle('shell:openPath', async (_event, payload = {}) => {
  const targetPath =
    typeof payload?.targetPath === 'string' && payload.targetPath.trim().length > 0
      ? payload.targetPath.trim()
      : null;

  if (!targetPath) {
    return { ok: false, error: 'No path provided.' };
  }

  const result = await shell.openPath(targetPath);
  if (typeof result === 'string' && result.trim().length > 0) {
    return { ok: false, error: result };
  }

  return { ok: true, error: null };
});
ipcMain.handle('run_ifs', async (_event, payload) => {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Invalid payload for run_ifs');
  }

  if (!payload.validatedPath) {
    throw new Error('run_ifs requires a validatedPath');
  }

  if (!payload.outputDirectory) {
    throw new Error('run_ifs requires an outputDirectory');
  }

  if (payload.endYear == null) {
    throw new Error('run_ifs requires an endYear');
  }

  const args = [
    '--ifs-root',
    payload.validatedPath,
    '--end-year',
    String(payload.endYear),
    '--output-folder',
    payload.outputDirectory,
    '--bigpopa-db',
    path.join(payload.outputDirectory, 'bigpopa.db'),
  ];

  if (payload.baseYear != null) {
    args.push('--base-year', String(payload.baseYear));
  }

  if (
    typeof payload.artifactRetentionMode === 'string' &&
    payload.artifactRetentionMode.trim().length > 0
  ) {
    args.push('--artifact-retention', payload.artifactRetentionMode.trim());
  }

  return runPythonScript('ml_driver.py', args);
});

ipcMain.handle('extract_compare', async (_event, payload) => {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Invalid payload for extract_compare');
  }

  const { ifsRoot, modelDb, inputProfileId, modelId, ifsId, bigpopaDb, outputDir } = payload;

  if (!ifsRoot || !modelDb || !modelId || !ifsId) {
    throw new Error(
      'extract_compare requires ifsRoot, modelDb, modelId, and ifsId',
    );
  }

  const args = [
    '--ifs-root',
    ifsRoot,
    '--model-db',
    modelDb,
    '--model-id',
    modelId,
    '--ifs-id',
    String(ifsId),
  ];
  if (typeof inputProfileId === 'number' && Number.isFinite(inputProfileId) && inputProfileId > 0) {
    args.push('--input-profile-id', String(inputProfileId));
  }
  if (typeof bigpopaDb === 'string' && bigpopaDb.trim()) {
    args.push('--bigpopa-db', bigpopaDb.trim());
  }
  if (typeof outputDir === 'string' && outputDir.trim()) {
    args.push('--output-dir', outputDir.trim());
  }

  return runPythonScript('extract_compare.py', args);
});

ipcMain.handle('model_setup', async (_event, payload) => {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Invalid payload for model_setup');
  }

  const validatedPath =
    typeof payload.validatedPath === 'string' ? payload.validatedPath.trim() : '';
  const inputProfileId =
    typeof payload.inputProfileId === 'number' && Number.isFinite(payload.inputProfileId)
      ? Math.trunc(payload.inputProfileId)
      : null;

  if (!validatedPath) {
    throw new Error('model_setup requires a validatedPath');
  }

  if (!inputProfileId || inputProfileId <= 0) {
    throw new Error('model_setup requires an inputProfileId');
  }

  const baseYear = payload.baseYear ?? null;
  const endYear = payload.endYear;
  const outputFolder =
    typeof payload.outputFolder === 'string' ? payload.outputFolder.trim() : '';
  const artifactRetentionMode =
    typeof payload.artifactRetentionMode === 'string'
      ? payload.artifactRetentionMode.trim()
      : '';

  const args = [
    '--ifs-root',
    validatedPath,
    '--input-profile-id',
    String(inputProfileId),
    '--base-year',
    String(baseYear ?? ''),
    '--end-year',
    String(endYear),
  ];

  if (outputFolder) {
    args.push('--output-folder', outputFolder);
  }
  if (artifactRetentionMode) {
    args.push('--artifact-retention', artifactRetentionMode);
  }
  return runPythonScript('model_setup.py', args, {
    progressChannel: 'model-setup-progress',
  });
});

ipcMain.handle('validate_ifs', async (_event, payload = {}) => {
  if (!payload.ifsPath || typeof payload.ifsPath !== 'string') {
    throw new Error('validate_ifs requires an ifsPath');
  }

  const args = [payload.ifsPath];

  if (payload.outputPath) {
    args.push('--output-path', payload.outputPath);
  }

  if (
    typeof payload.inputProfileId === 'number' &&
    Number.isFinite(payload.inputProfileId) &&
    payload.inputProfileId > 0
  ) {
    args.push('--input-profile-id', String(Math.trunc(payload.inputProfileId)));
  }

  return runPythonScript('validate_ifs.py', args);
});
