const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const isDev = !app.isPackaged;
let mainWindow = null;
let lastValidatedPath = null;

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
        } else {
          lastValidatedPath = null;
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

  const scriptPath = path.join(__dirname, '..', 'backend', 'run_ifs.py');
  const progressPath = path.join(lastValidatedPath, 'RUNFILES', 'progress.txt');
  const args = [
    scriptPath,
    '--ifs-root',
    lastValidatedPath,
    '--end-year',
    String(desiredEndYear),
    '--start-token',
    '5',
    '--log',
    'jrs.txt',
    '--websessionid',
    'qsdqsqsdqsdqsdqs',
  ];

  return new Promise((resolve) => {
    let resolved = false;
    let pollTimer = null;
    let lastYear = null;

    const finish = (payload) => {
      if (!resolved) {
        resolved = true;
        if (pollTimer) {
          clearInterval(pollTimer);
          pollTimer = null;
        }
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

    let stdout = '';
    let stderr = '';

    const sendProgress = (year) => {
      if (lastYear === year) {
        return;
      }
      lastYear = year;
      if (mainWindow && !mainWindow.isDestroyed()) {
        mainWindow.webContents.send('ifs-progress', year);
      }
    };

    const pollProgress = () => {
      fs.promises
        .readFile(progressPath, 'utf8')
        .then((content) => {
          const trimmed = content.trim();
          if (!trimmed) {
            return;
          }

          const lines = trimmed.split(/\r?\n/);
          const lastLine = lines[lines.length - 1];
          if (!lastLine) {
            return;
          }

          const [yearToken] = lastLine.trim().split(/\s+/);
          const year = Number(yearToken);
          if (Number.isFinite(year)) {
            sendProgress(year);
          }
        })
        .catch(() => {
          // progress file might not exist yet; ignore errors
        });
    };

    pythonProcess.stdout.on('data', (data) => {
      stdout += data.toString();
    });

    pythonProcess.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    pythonProcess.on('error', (error) => {
      finish(fallback(error?.message || 'Failed to execute IFs.'));
    });

    pollTimer = setInterval(pollProgress, 1000);
    pollProgress();

    pythonProcess.on('close', (code) => {
      if (pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
      }

      if (stderr.trim()) {
        finish(fallback('IFs runner reported an error.'));
        return;
      }

      if (code !== 0 && stdout.trim().length === 0) {
        finish(fallback('IFs runner exited unexpectedly.'));
        return;
      }

      try {
        const parsed = JSON.parse(stdout || '{}');
        if (parsed && parsed.status === 'ok') {
          if (typeof parsed.end_year === 'number') {
            sendProgress(parsed.end_year);
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
