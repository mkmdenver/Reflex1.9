<#
.SYNOPSIS
  Moves a local Garnet build to Program Files, adds to PATH, installs as a Windows Service,
  falls back to a Scheduled Task if service start fails, opens firewall, and verifies port.

.NOTES
  - Run this script in an elevated (Administrator) PowerShell 7 session.
  - Default source path is your current extracted folder from Downloads.
#>

[CmdletBinding()]
param(
  # Adjust if your extracted folder differs
  [string]$Source       = 'C:\Users\mike.malone\Downloads\win-x64-based-readytorun\net9.0',
  [string]$InstallDir   = 'C:\Program Files\Garnet',
  [int]   $Port         = 6379,
  [string]$ServiceName  = 'GarnetServer',
  [string]$TaskName     = 'GarnetServer (Reflex)'
)

function Assert-Admin {
  $id = [Security.Principal.WindowsIdentity]::GetCurrent()
  $p  = New-Object Security.Principal.WindowsPrincipal($id)
  if (-not $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Please run this script in an elevated PowerShell (Run as Administrator)."
  }
}

function Copy-Garnet {
  param([string]$From, [string]$To)
  if (-not (Test-Path $From)) { throw "Source folder not found: $From" }
  if (-not (Test-Path (Join-Path $From 'GarnetServer.exe'))) {
    throw "GarnetServer.exe not found under: $From"
  }
  if (-not (Test-Path $To)) { New-Item -ItemType Directory -Path $To | Out-Null }

  Write-Host "Copying files from '$From' to '$To'..."
  # Robocopy is more reliable for bulk copies; fallback to Copy-Item if needed
  $null = robocopy $From $To /MIR /NFL /NDL /NJH /NJS /NP
  if ($LASTEXITCODE -ge 8) {
    Write-Warning "Robocopy returned $LASTEXITCODE; falling back to Copy-Item."
    Copy-Item -Path (Join-Path $From '*') -Destination $To -Recurse -Force
  }
}

function Add-ToSystemPath {
  param([string]$Dir)
  $current = [System.Environment]::GetEnvironmentVariable('Path','Machine')
  if ($current -notlike "*$Dir*") {
    [System.Environment]::SetEnvironmentVariable('Path', "$current;$Dir", 'Machine')
    Write-Host "Added to PATH (Machine): $Dir"
    Write-Host "Open a NEW terminal to pick up PATH changes."
  } else {
    Write-Host "PATH already contains: $Dir"
  }
}

function Open-Firewall {
  param([int]$Port)
  $ruleName = "Garnet_$Port"
  $existing = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
  if (-not $existing) {
    New-NetFirewallRule -Name $ruleName -DisplayName $ruleName -Direction Inbound -Protocol TCP -LocalPort $Port -Action Allow | Out-Null
    Write-Host "Firewall rule opened for TCP $Port"
  } else {
    Write-Host "Firewall rule already present for TCP $Port"
  }
}

function Try-InstallService {
  param([string]$SvcName, [string]$ExePath, [int]$Port)
  Write-Host "Creating Windows Service '$SvcName'..."
  # sc.exe spacing after "=" is required
  $bin = "`"$ExePath`" --port $Port"
  $create = sc.exe create "$SvcName" binPath= "$bin" start= auto DisplayName= "Garnet (Redis-compatible) Server"
  $create | Out-Null

  Start-Sleep -Seconds 1
  Write-Host "Starting service '$SvcName'..."
  $start = sc.exe start "$SvcName"
  $start | Out-Null

  # Wait up to ~10s for RUNNING
  $ok = $false
  for ($i=0; $i -lt 10; $i++) {
    Start-Sleep -Seconds 1
    try {
      $svc = Get-Service -Name $SvcName -ErrorAction Stop
      if ($svc.Status -eq 'Running') { $ok = $true; break }
    } catch { }
  }

  if ($ok) {
    Write-Host "Service '$SvcName' is running."
    return $true
  } else {
    Write-Warning "Service '$SvcName' did not reach RUNNING. It may not support Windows Service control."
    Write-Host "Stopping and removing service..."
    sc.exe stop "$SvcName" | Out-Null
    sc.exe delete "$SvcName" | Out-Null
    return $false
  }
}

function Install-ScheduledTask {
  param([string]$TaskName, [string]$ExePath, [int]$Port)
  Write-Host "Registering Scheduled Task '$TaskName' (runs as SYSTEM at startup)..."
  $action    = New-ScheduledTaskAction  -Execute $ExePath -Argument ("--port {0}" -f $Port)
  $trigger   = New-ScheduledTaskTrigger -AtStartup
  $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -RunLevel Highest
  try {
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Force | Out-Null
  } catch {
    throw "Failed to register Scheduled Task '$TaskName': $($_.Exception.Message)"
  }

  # Start immediately
  Start-ScheduledTask -TaskName $TaskName
  Write-Host "Scheduled Task '$TaskName' started."
}

function Test-GarnetPort {
  param([int]$Port)
  Write-Host "Verifying Garnet is listening on TCP $Port ..."
  $res = Test-NetConnection 127.0.0.1 -Port $Port
  if ($res.TcpTestSucceeded) {
    Write-Host "Success: Garnet is listening on 127.0.0.1:$Port"
    return $true
  } else {
    Write-Warning "Garnet is not listening on 127.0.0.1:$Port yet."
    return $false
  }
}

# ------------------ MAIN ------------------
try {
  Assert-Admin

  # 1) Move / install
  Copy-Garnet -From $Source -To $InstallDir

  $exe = Join-Path $InstallDir 'GarnetServer.exe'
  if (-not (Test-Path $exe)) {
    throw "GarnetServer.exe not found at $exe after copy."
  }

  # 2) PATH + Firewall
  Add-ToSystemPath -Dir $InstallDir
  Open-Firewall -Port $Port

  # 3) Try Windows Service first; fallback to Scheduled Task
  $serviceOk = Try-InstallService -SvcName $ServiceName -ExePath $exe -Port $Port
  if (-not $serviceOk) {
    Install-ScheduledTask -TaskName $TaskName -ExePath $exe -Port $Port
  }

  # 4) Verify port
  # Allow a few seconds for process to bind
  Start-Sleep -Seconds 2
  if (-not (Test-GarnetPort -Port $Port)) {
    Write-Host "Waiting a bit more and rechecking..."
    Start-Sleep -Seconds 5
    Test-GarnetPort -Port $Port
  }

  Write-Host "`n=== Done ==="
  Write-Host "If you just updated PATH, open a NEW terminal to use 'GarnetServer.exe' directly."
  Write-Host "REDIS_URL example for Reflex: redis://127.0.0.1:$Port/0"
}
catch {
  Write-Error $_.Exception.Message
  exit 1
}
