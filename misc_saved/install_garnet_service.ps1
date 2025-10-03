# Garnet install path
$garnetPath = "C:\Program Files\Garnet"
$exePath = Join-Path $garnetPath "GarnetServer.exe"
$serviceName = "GarnetServer"

# Try to locate dotnet manually
$dotnetExe = "C:\Program Files\dotnet\dotnet.exe"
if (-not (Test-Path $dotnetExe)) {
    Write-Error "dotnet CLI not found. Please install .NET 9 Runtime from https://aka.ms/dotnet-core-applaunch?missing_runtime=true"
    exit 1
}

# Add dotnet to PATH for current session
$env:Path += ";C:\Program Files\dotnet"

# Check if .NET 9 runtime is installed
$dotnetVersion = & $dotnetExe --list-runtimes | Select-String "Microsoft.NETCore.App 9.0"
if (-not $dotnetVersion) {
    Write-Error "Missing .NET 9 runtime. Please install it from https://aka.ms/dotnet-core-applaunch?missing_runtime=true"
    exit 1
}

# Check if GarnetServer.exe exists
if (-not (Test-Path $exePath)) {
    Write-Error "GarnetServer.exe not found at $exePath"
    exit 1
}

# Remove existing service if present
if (Get-Service -Name $serviceName -ErrorAction SilentlyContinue) {
    Write-Host "Removing existing Garnet service..."
    Stop-Service -Name $serviceName -Force
    sc.exe delete $serviceName | Out-Null
    Start-Sleep -Seconds 2
}

# Create Garnet service
New-Service -Name $serviceName -BinaryPathName "`"$exePath`"" -DisplayName "Garnet Server" -StartupType Automatic

# Start the service
Start-Service -Name $serviceName

# Wait a few seconds for Garnet to initialize
Start-Sleep -Seconds 5

# Test TCP connectivity
$tcpTest = Test-NetConnection -ComputerName 127.0.0.1 -Port 6379
if ($tcpTest.TcpTestSucceeded) {
    Write-Host "✅ Garnet is running and listening on TCP 6379"
} else {
    Write-Warning "⚠️ Garnet service started but is not listening on TCP 6379"
    Write-Warning "Check logs at C:\ProgramData\Garnet\garnet-6379.log"
}
