const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electron', {
  selectFolder: async () => ipcRenderer.invoke('dialog:selectFolder'),
  invoke: (channel, data) => ipcRenderer.invoke(channel, data),
});
