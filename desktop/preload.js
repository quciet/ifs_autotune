const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electron', {
  selectFolder: (type, defaultPath) =>
    ipcRenderer.invoke('select-folder', { type, defaultPath }),
  selectFile: (defaultPath) =>
    ipcRenderer.invoke('select-input-file', { defaultPath }),
  getDefaultOutputDir: () => ipcRenderer.invoke('get-default-output-dir'),
  getDefaultInputFile: () => ipcRenderer.invoke('get-default-input-file'),
  invoke: (channel, data) => ipcRenderer.invoke(channel, data),
  onMLProgress: (callback) => {
    const subscription = (_event, line) => {
      callback(line);
    };

    ipcRenderer.on('ml-progress', subscription);

    return () => {
      ipcRenderer.removeListener('ml-progress', subscription);
    };
  },
  onMLLog(callback) {
    const subscription = (_evt, data) => callback(data);

    ipcRenderer.on("ml-log", subscription);

    return () => {
      ipcRenderer.removeListener("ml-log", subscription);
    };
  },
  on: (channel, callback) => {
    const subscription = (_event, ...args) => {
      callback(...args);
    };

    ipcRenderer.on(channel, subscription);

    return () => {
      ipcRenderer.removeListener(channel, subscription);
    };
  },
});
