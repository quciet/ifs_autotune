const { contextBridge, ipcRenderer } = require('electron');
const { app } = require('electron');
const path = require('path');

contextBridge.exposeInMainWorld('electron', {
  selectFolder: async () => ipcRenderer.invoke('dialog:selectFolder'),
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
  getDefaultOutputDir: () => {
    return path.join(app.getPath('documents'), 'IFs_Output');
  },
});
