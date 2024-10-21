<#
.SYNOPSIS
This is a Powershell script to install/run the Google Cloud Storage Testbench on Windows.

.DESCRIPTION
This is a Powershell script to install/run the Google Cloud Storage Testbench on Windows.

.PARAMETER RunTestbench
Attempt to execute the testbench without installing (assumes already installed.)

.PARAMETER Clean
Remove the existing testbench installation.

.LINK
https://github.com/TileDB-Inc/TileDB
#>

[CmdletBinding()]
Param(
    [switch]$RunTestbench,
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

$version = "v0.49.0"
$testbenchPath = "$env:TEMP\storage-testbench-$version"
$venvPath = "$env:TEMP\storage-testbench-venv"

# Clean the existing testbench installation
if ($Clean) {
    Write-Host "Cleaning existing testbench installation..."
    Remove-Item -Recurse -Force $testbenchPath
    Remove-Item -Recurse -Force $venvPath
}

if (!$RunTestbench) {
    #if testbench is not installed
    if (!(Test-Path $testbenchPath)) {
        #install testbench
        Write-Host "Installing Google Cloud Storage Testbench..."
        git clone --branch $version --depth 1 https://github.com/googleapis/storage-testbench.git $testbenchPath
        Write-Host "Setting up Python virtual environment..."
        py -m venv $venvPath
        & $venvPath\Scripts\activate
        py -m pip install -e $testbenchPath
    }
}

# Export the CLOUD_STORAGE_EMULATOR_ENDPOINT environment variable
$env:CLOUD_STORAGE_EMULATOR_ENDPOINT = "http://localhost:9000"

# Run the testbench
& $venvPath\Scripts\activate
# $testbenchCmd = "start `"Google Cloud Storage Testbench`" py testbench_run.py localhost 9000 10"
$testbenchCmd = "start `"Google Cloud Storage Testbench`" /D `"$testbenchPath`" py testbench_run.py localhost 9000 10"
cmd /c $testbenchCmd

# Wait for the testbench to be prepared. This was added to fix failures in the windows-2019-gcs job.
Start-Sleep -Seconds 5

Write-Host "Finished."
