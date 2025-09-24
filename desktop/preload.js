const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electron', {
  selectFolder: (type, defaultPath) =>
    ipcRenderer.invoke('select-folder', { type, defaultPath }),
  getDefaultOutputDir: () => ipcRenderer.invoke('get-default-output-dir'),
  invoke: (channel, data) => ipcRenderer.invoke(channel, data),
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
