import { contextBridge, ipcRenderer } from 'electron'
contextBridge.exposeInMainWorld('koreNext', {
  openExternal: (url: string) => ipcRenderer.invoke('open-external', url),
  notify: (title: string, body: string) => ipcRenderer.invoke('notify', title, body),
  pickDirectory: () => ipcRenderer.invoke('pick-directory') as Promise<string | null>,
})

