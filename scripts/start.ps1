Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Backend = Join-Path $ProjectRoot 'backend'
$Desktop = Join-Path $ProjectRoot 'desktop'
$VirtualEnvironment = Join-Path $Backend '.venv'
$Python = Join-Path $VirtualEnvironment 'Scripts\python.exe'
$Pip = Join-Path $VirtualEnvironment 'Scripts\pip.exe'
$NodeModules = Join-Path $Desktop 'node_modules'
$EnvironmentFile = Join-Path $Backend '.env'
$EnvironmentExample = Join-Path $Backend '.env.example'

if (-not (Test-Path -LiteralPath $EnvironmentFile) -and (Test-Path -LiteralPath $EnvironmentExample)) {
  Copy-Item -LiteralPath $EnvironmentExample -Destination $EnvironmentFile
  Write-Host '[KoreAgent Next] Created backend\.env from .env.example. You may configure the model in the app or edit this file.'
}

function Invoke-CheckedCommand {
  param(
    [Parameter(Mandatory = $true)][string]$FilePath,
    [Parameter(Mandatory = $true)][string[]]$Arguments,
    [Parameter(Mandatory = $true)][string]$FailureMessage
  )

  & $FilePath @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "$FailureMessage (exit code: $LASTEXITCODE)"
  }
}

function Stop-ProcessTree {
  param([Parameter(Mandatory = $true)][int]$RootId)

  # The Windows venv python.exe is a launcher and may leave the real Python
  # child alive when only the launcher is stopped. Stop descendants first so
  # a closed app cannot leave a second SQLite Host behind.
  $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId = $RootId" -ErrorAction SilentlyContinue)
  foreach ($child in $children) {
    Stop-ProcessTree -RootId ([int]$child.ProcessId)
  }
  Stop-Process -Id $RootId -Force -ErrorAction SilentlyContinue
}

$PyLauncher = Get-Command 'py.exe' -ErrorAction SilentlyContinue
$PythonCommand = Get-Command 'python.exe' -ErrorAction SilentlyContinue
if (-not $PyLauncher -and -not $PythonCommand) {
  throw 'Python was not found. Install Python 3.11 or newer and reopen PowerShell.'
}

if (-not (Test-Path -LiteralPath $Python) -or -not (Test-Path -LiteralPath $Pip)) {
  if (Test-Path -LiteralPath $VirtualEnvironment) {
    Write-Host '[KoreAgent Next] Removing the incomplete Python environment...'
    try {
      Remove-Item -LiteralPath $VirtualEnvironment -Recurse -Force
    } catch {
      throw "Cannot remove the incomplete Python environment. Close programs using this project and run this script again. Details: $($_.Exception.Message)"
    }
  }

  # Prefer 3.12, but honor the documented Python 3.11+ requirement when
  # the Windows launcher exists without a 3.12 installation.
  $Candidates = @()
  if ($PyLauncher) {
    $Candidates += @{ Executable = $PyLauncher.Source; Prefix = @('-3.12') }
    $Candidates += @{ Executable = $PyLauncher.Source; Prefix = @('-3') }
  }
  if ($PythonCommand) {
    $Candidates += @{ Executable = $PythonCommand.Source; Prefix = @() }
  }
  $VenvLauncher = $null
  foreach ($Candidate in $Candidates) {
    $ProbeArguments = @($Candidate.Prefix) + @('-c', 'import sys; print(sys.executable) if sys.version_info >= (3, 11) else sys.exit(1)')
    try {
      $Interpreter = & $Candidate.Executable @ProbeArguments 2>$null
      if ($LASTEXITCODE -eq 0 -and $Interpreter -and (Test-Path -LiteralPath ([string]$Interpreter))) {
        $VenvLauncher = [string]$Interpreter
        break
      }
    } catch {
      # Try the next installed interpreter.
    }
  }
  if (-not $VenvLauncher) {
    throw 'A working Python 3.11 or newer was not found. Install it and reopen PowerShell.'
  }
  Write-Host '[KoreAgent Next] Creating the Python virtual environment...'
  $VenvArguments = @('-m', 'venv', $VirtualEnvironment)
  Invoke-CheckedCommand -FilePath $VenvLauncher `
    -Arguments $VenvArguments `
    -FailureMessage 'Failed to create the Python virtual environment.'

  if (-not (Test-Path -LiteralPath $Python) -or -not (Test-Path -LiteralPath $Pip)) {
    throw 'Python reported success, but the virtual environment is incomplete. Repair or reinstall Python 3.11 or newer and try again.'
  }
}

$PreviousErrorAction = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
& $Python -c 'import fastapi, uvicorn, aiosqlite, cryptography, croniter, pypdf, openpyxl' 2>$null
$BackendDependenciesReady = $LASTEXITCODE -eq 0
$ErrorActionPreference = $PreviousErrorAction
if (-not $BackendDependenciesReady) {
  Write-Host '[KoreAgent Next] Installing backend dependencies...'
  Invoke-CheckedCommand -FilePath $Python `
    -Arguments @('-m', 'pip', 'install', '-r', (Join-Path $Backend 'requirements.txt')) `
    -FailureMessage 'Failed to install backend dependencies.'
}

$NpmCommand = Get-Command 'npm.cmd' -ErrorAction SilentlyContinue
if (-not $NpmCommand) {
  $NpmPath = Join-Path ${env:ProgramFiles} 'nodejs\npm.cmd'
  if (Test-Path -LiteralPath $NpmPath) {
    $NpmCommand = $NpmPath
  } else {
    throw 'npm.cmd was not found. Install Node.js 20 or newer and reopen PowerShell.'
  }
}
$NpmExecutable = if ($NpmCommand -is [string]) { $NpmCommand } else { $NpmCommand.Source }
$NodeJsDirectory = Split-Path -Parent $NpmExecutable
if (-not ($env:Path -split ';' | Where-Object { $_ -eq $NodeJsDirectory })) {
  $env:Path = "$NodeJsDirectory;$env:Path"
}

$RequiredDesktopFiles = @(
  (Join-Path $NodeModules '.bin\concurrently.cmd'),
  (Join-Path $NodeModules '.bin\vite.cmd'),
  (Join-Path $NodeModules 'electron\dist\electron.exe')
)
$DesktopDependenciesReady = $true
foreach ($RequiredFile in $RequiredDesktopFiles) {
  if (-not (Test-Path -LiteralPath $RequiredFile)) {
    $DesktopDependenciesReady = $false
    break
  }
}

if (-not $DesktopDependenciesReady) {
  Write-Host '[KoreAgent Next] Desktop dependencies are missing or incomplete.'
  if (Test-Path -LiteralPath $NodeModules) {
    Write-Host '[KoreAgent Next] Removing the incomplete node_modules directory...'
    try {
      Remove-Item -LiteralPath $NodeModules -Recurse -Force
    } catch {
      throw "Cannot remove the incomplete node_modules directory. Close Electron, Node.js, editors, and terminals using this project, then run this script again. Details: $($_.Exception.Message)"
    }
  }

  $PreviousElectronMirror = $env:ELECTRON_MIRROR
  if ([string]::IsNullOrWhiteSpace($PreviousElectronMirror)) {
    $env:ELECTRON_MIRROR = 'https://npmmirror.com/mirrors/electron/'
    Write-Host '[KoreAgent Next] Using the Electron download mirror for this installation.'
  }

  try {
    Invoke-CheckedCommand -FilePath $NpmExecutable `
      -Arguments @('--prefix', $Desktop, 'ci', '--no-audit', '--no-fund') `
      -FailureMessage 'Failed to install desktop dependencies. Check the network connection and run the script again.'
  } finally {
    if ([string]::IsNullOrWhiteSpace($PreviousElectronMirror)) {
      Remove-Item Env:\ELECTRON_MIRROR -ErrorAction SilentlyContinue
    } else {
      $env:ELECTRON_MIRROR = $PreviousElectronMirror
    }
  }

  foreach ($RequiredFile in $RequiredDesktopFiles) {
    if (-not (Test-Path -LiteralPath $RequiredFile)) {
      throw "Desktop dependency installation completed but a required file is still missing: $RequiredFile"
    }
  }
}

Write-Host '[KoreAgent Next] Starting backend and desktop application...'
$HealthUrl = 'http://127.0.0.1:8765/health'
$DevUrl = 'http://127.0.0.1:5173/'
$OwnsHost = $false
$HostProcess = $null
$HostIsProjectProcess = $false
$ExistingHostHealthy = $false

# Reuse an already healthy local Host when it is current. If the backend source
# is newer than the process, restart that Host so a second launcher cannot keep
# serving stale application code after an update.
try {
  $ExistingHealth = Invoke-WebRequest -UseBasicParsing -Uri $HealthUrl -TimeoutSec 2
  if ($ExistingHealth.StatusCode -eq 200) {
    $ExistingHostHealthy = $true
    $ExistingHealthPayload = $null
    try { $ExistingHealthPayload = $ExistingHealth.Content | ConvertFrom-Json } catch { $ExistingHealthPayload = $null }
    # A process can remain healthy while serving an older backend checkout.
    # Check a current RPC capability before deciding to reuse it; otherwise a
    # newly added UI action would receive "Method not found" until manual restart.
    $HostMissingCurrentApi = -not $ExistingHealthPayload -or -not ($ExistingHealthPayload.rpc_methods -contains 'goal.plan.preview')
    $PortOwner = (Get-NetTCPConnection -LocalPort 8765 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1).OwningProcess
    $HostInfo = if ($PortOwner) { Get-CimInstance Win32_Process -Filter "ProcessId = $PortOwner" -ErrorAction SilentlyContinue } else { $null }
    $LatestBackendChange = Get-ChildItem -LiteralPath (Join-Path $Backend 'kore_next') -File -Recurse -ErrorAction SilentlyContinue |
      Where-Object { $_.Extension -eq '.py' } |
      Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1
    $HostStart = if ($PortOwner) { (Get-Process -Id $PortOwner -ErrorAction SilentlyContinue).StartTime.ToUniversalTime() } else { $null }
    $HostIsProjectProcess = $HostInfo -and $HostInfo.CommandLine -match 'kore_next\.server'
    $HostIsStale = $HostIsProjectProcess -and ($HostMissingCurrentApi -or ($LatestBackendChange -and $HostStart -and ($LatestBackendChange.LastWriteTimeUtc -gt $HostStart.AddSeconds(1))))
    if ($HostIsStale) {
      Write-Host '[KoreAgent Next] Backend source changed; restarting the existing Host to load the latest code.'
      $Root = [int]$PortOwner
      while ($Root) {
        $Parent = Get-CimInstance Win32_Process -Filter "ProcessId = $Root" -ErrorAction SilentlyContinue
        if (-not $Parent -or -not $Parent.ParentProcessId) { break }
        $Ancestor = Get-CimInstance Win32_Process -Filter "ProcessId = $($Parent.ParentProcessId)" -ErrorAction SilentlyContinue
        if (-not $Ancestor -or $Ancestor.CommandLine -notmatch 'kore_next\.server') { break }
        $Root = [int]$Ancestor.ProcessId
      }
      Stop-ProcessTree -RootId $Root
      Start-Sleep -Milliseconds 500
      $HostProcess = Start-Process -FilePath $Python `
        -ArgumentList '-m', 'kore_next.server' `
        -WorkingDirectory $Backend `
        -WindowStyle Hidden `
        -PassThru
      $OwnsHost = $true
    } else {
      Write-Host "[KoreAgent Next] Reusing the existing Host at $HealthUrl"
    }
  }
} catch {
  if (-not $HostProcess) {
    $HostProcess = Start-Process -FilePath $Python `
      -ArgumentList '-m', 'kore_next.server' `
      -WorkingDirectory $Backend `
      -WindowStyle Hidden `
      -PassThru
    $OwnsHost = $true
  }
}

if (-not $HostProcess -and -not $ExistingHostHealthy) {
  $HostProcess = Start-Process -FilePath $Python `
    -ArgumentList '-m', 'kore_next.server' `
    -WorkingDirectory $Backend `
    -WindowStyle Hidden `
    -PassThru
  $OwnsHost = $true
}

# Do not open a desktop window while the Host is still starting (or after it
# has already crashed).  A missing optional dependency used to make the Host
# exit immediately while Electron kept showing an endless settings spinner.
$HostReady = $false
for ($Attempt = 0; $Attempt -lt 30; $Attempt++) {
  if ($HostProcess -and $HostProcess.HasExited) {
    throw "The backend Host exited before it became ready. Check the Python dependencies and backend configuration."
  }
  try {
    $Health = Invoke-WebRequest -UseBasicParsing -Uri $HealthUrl -TimeoutSec 2
    if ($Health.StatusCode -eq 200) {
      $HostReady = $true
      break
    }
  } catch {
    Start-Sleep -Seconds 1
  }
}
if (-not $HostReady) {
  throw "The backend Host did not become ready at $HealthUrl."
}

try {
  # When another KoreAgent instance already owns the Vite port, reuse that
  # renderer instead of starting a second Vite server on 5174.  A second
  # dev-server makes `wait-on tcp:5173` exit and leaves the desktop startup
  # script reporting a false failure.
  $ReuseDevServer = $false
  try {
    $DevResponse = Invoke-WebRequest -UseBasicParsing -Uri $DevUrl -TimeoutSec 2
    $ReuseDevServer = $DevResponse.StatusCode -eq 200
  } catch {
    $ReuseDevServer = $false
  }

  if ($ReuseDevServer) {
    Write-Host "[KoreAgent Next] Reusing the existing Vite dev server at $DevUrl"
    $ElectronExecutable = Join-Path $NodeModules 'electron\dist\electron.exe'
    Invoke-CheckedCommand -FilePath $ElectronExecutable `
      -Arguments @($Desktop) `
      -FailureMessage 'The desktop application exited with an error.'
  } else {
    Invoke-CheckedCommand -FilePath $NpmExecutable `
      -Arguments @('--prefix', $Desktop, 'run', 'electron:dev') `
      -FailureMessage 'The desktop application exited with an error.'
  }
} finally {
  if ($OwnsHost -and $HostProcess -and -not $HostProcess.HasExited) {
    Stop-ProcessTree -RootId $HostProcess.Id
  }
}
