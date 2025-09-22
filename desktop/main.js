const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const { spawn } = require('node:child_process');
const path = require('node:path');

const isDev = !app.isPackaged;

function createWindow() {
  const mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
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
