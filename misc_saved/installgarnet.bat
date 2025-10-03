@echo off
setlocal

REM Add dotnet to PATH for this session
set PATH=%PATH%;C:\Program Files\dotnet

REM Run PowerShell inline
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"$garnetPath = 'C:\Program Files\Garnet'; ^
$exePath = Join-Path $garnetPath 'GarnetServer.exe'; ^
$serviceName = 'GarnetServer'; ^
$dotnetExe = 'C:\Program Files\dotnet\dotnet.exe'; ^
if (-not (Test-Path $dotnetExe)) { Write-Error 'dotnet CLI not found. Please install .NET 9 Runtime.'; exit 1 }; ^
$dotnetVersion = & $dotnetExe --list-runtimes | Select-String 'Microsoft.NETCore.App 9.0'; ^
if (-not $dotnetVersion) { Write-Error 'Missing .NET 9 runtime. Please install it.'; exit 1 }; ^
if (-not (Test-Path $exePath)) { Write-Error 'GarnetServer.exe not found.'; exit 1 }; ^
if (Get-Service -Name $serviceName -ErrorAction SilentlyContinue) { Stop-Service -Name $serviceName -Force; sc.exe delete $serviceName | Out-Null; Start-Sleep -Seconds 2 }; ^
New-Service -Name $serviceName -BinaryPathName \"`\"$exePath`\"\" -DisplayName 'Garnet Server' -StartupType Automatic; ^
Start-Service -Name $serviceName; ^
Start-Sleep -Seconds 5; ^
$tcpTest = Test-NetConnection -ComputerName 127.0.0.1 -Port 6379; ^
if ($tcpTest.TcpTestSucceeded) { Write-Host '✅ Garnet is running and listening on TCP 6379' } else { Write-Warning '⚠️ Garnet service started but is not listening on TCP 6379'; Write-Warning 'Check logs at C:\ProgramData\Garnet\garnet-6379.log' }"

endlocal
pause
