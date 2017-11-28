<#
.SYNOPSIS
This is a Powershell script to install TileDB dependencies on Windows.

.DESCRIPTION
This is a Powershell script to install TileDB dependencies on Windows.

.LINK
https://github.com/TileDB-Inc/TileDB

#>

[CmdletBinding()]
Param(
)

# Return the directory containing this script file.
function Get-ScriptsDirectory {
    Split-Path -Parent $PSCommandPath
}

$TileDBRootDirectory = Split-Path -Parent (Get-ScriptsDirectory)
$InstallPrefix = Join-Path $TileDBRootDirectory "deps-install"
$DownloadDirectory = Get-ScriptsDirectory
$DownloadZlibDest = Join-Path $DownloadDirectory "zlib.zip"

function DownloadFile([string] $URL, [string] $Dest) {
    Write-Host "Downloading $URL to $Dest..."
    Invoke-WebRequest -Uri $URL -OutFile $Dest
}

function DownloadIfNotExists([string] $Path, [string] $URL) {
    if (!(Test-Path $Path)) {
	DownloadFile $URL $Path
    }
}

Add-Type -AssemblyName System.IO.Compression.FileSystem
function Unzip([string] $Zipfile, [string] $Dest) {
    Write-Host "Extracting $Zipfile to $Dest..."
    [System.IO.Compression.ZipFile]::ExtractToDirectory($Zipfile, $Dest)
}

function Install-Zlib {
    $ZlibRoot = (Join-Path $DownloadDirectory "zlib-1.2.11")
    if (!(Test-Path $ZlibRoot)) {
	DownloadIfNotExists $DownloadZlibDest "https://zlib.net/zlib1211.zip"
	Unzip $DownloadZlibDest $DownloadDirectory
    }
    Push-Location
    Set-Location $ZlibRoot
    if (!(Test-Path build)) {
	New-Item -ItemType Directory -Path build
    }
    Set-Location build
    cmake -DCMAKE_INSTALL_PREFIX="$InstallPrefix" ..
    cmake --build . --config Release --target INSTALL
    Pop-Location
}

function Install-All-Deps {
    Install-Zlib
}

Install-All-Deps

Write-Host "Finished."
