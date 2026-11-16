import { app, BrowserWindow, Notification, dialog, ipcMain, shell } from 'electron'
import path from 'node:path'

app.setName('KoreAgent Next')

// A second Electron process using the same Chromium profile can lock Cache and
// GPUCache files on Windows. Keep one desktop instance per user profile and
// forward activation to the existing window instead of opening a second one.
const hasSingleInstance = app.requestSingleInstanceLock()
if (!hasSingleInstance) {
  app.quit()
}

app.commandLine.appendSwitch('disable-gpu-disk-cache')

function surfaceUrl() {
  const developmentUrl = process.env.VITE_DEV_SERVER_URL || (!app.isPackaged ? 'http://127.0.0.1:5173' : '')
  return developmentUrl
}

let mainWindow: BrowserWindow | null = null

function createWindow() {
  const win = new BrowserWindow({
    width: 1440, height: 900, minWidth: 980, minHeight: 650, title: 'KoreAgent Next',
    backgroundColor: '#eef3f8',
    webPreferences: { preload: path.join(__dirname, 'preload.js'), contextIsolation: true, nodeIntegration: false },
  })
  mainWindow = win
  win.on('closed', () => { if (mainWindow === win) mainWindow = null })
  const developmentUrl = surfaceUrl()
  if (developmentUrl) win.loadURL(developmentUrl)
  else win.loadFile(path.join(__dirname, '../../dist/index.html'))
}

if (hasSingleInstance) {
app.on('second-instance', () => {
  if (!mainWindow) return
  if (mainWindow.isMinimized()) mainWindow.restore()
  mainWindow.show()
  mainWindow.focus()
})

app.whenReady().then(() => {
  ipcMain.handle('open-external', (_e, url: string) => shell.openExternal(url))
  ipcMain.handle('notify', (_e, title: string, body: string) => new Notification({ title, body }).show())
  ipcMain.handle('pick-directory', async () => {
    const result = await dialog.showOpenDialog({ properties: ['openDirectory', 'createDirectory'], title: '选择 Agent 工作区' })
    return result.canceled ? null : (result.filePaths[0] || null)
  })
  createWindow(); app.on('activate', () => { if (!BrowserWindow.getAllWindows().length) createWindow() })
})
}
app.on('window-all-closed', () => { if (process.platform !== 'darwin') app.quit() })

