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
$InstallPrefix = Join-Path $TileDBRootDirectory "dist"
$StagingDirectory = Join-Path (Get-ScriptsDirectory) "deps-staging"
$DownloadZlibDest = Join-Path $StagingDirectory "zlib.zip"
$DownloadLz4Dest = Join-Path $StagingDirectory "lz4.zip"
$DownloadBloscDest = Join-Path $StagingDirectory "blosc.zip"
$DownloadZstdDest = Join-Path $StagingDirectory "zstd.zip"
$DownloadBzip2Dest = Join-Path $StagingDirectory "bzip2.zip"

function DownloadFile([string] $URL, [string] $Dest) {
    Write-Host "Downloading $URL to $Dest..."
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
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
    $ZlibRoot = (Join-Path $StagingDirectory "zlib-1.2.11")
    if (!(Test-Path $ZlibRoot)) {
	DownloadIfNotExists $DownloadZlibDest "https://zlib.net/zlib1211.zip"
	Unzip $DownloadZlibDest $StagingDirectory
    }
    Push-Location
    Set-Location $ZlibRoot
    if (!(Test-Path build)) {
	New-Item -ItemType Directory -Path build
    }
    Set-Location build
    cmake -A X64 -DCMAKE_INSTALL_PREFIX="$InstallPrefix" -DSKIP_INSTALL_FILES=ON ..
    cmake --build . --config Release --target INSTALL
    Pop-Location
}

function Install-LZ4 {
    $Lz4Root = (Join-Path $StagingDirectory "lz4")
    if (!(Test-Path $Lz4Root)) {
	DownloadIfNotExists $DownloadLz4Dest "https://github.com/lz4/lz4/releases/download/v1.8.0/lz4_v1_8_0_win64.zip"
	New-Item -ItemType Directory -Path $Lz4Root
	Unzip $DownloadLz4Dest $Lz4Root
    }
    $DllDir = Join-Path $Lz4Root "dll"
    Copy-Item (Join-Path $DllDir "liblz4.lib") (Join-Path (Join-Path $InstallPrefix "lib") "liblz4.lib")
    Copy-Item (Join-Path $DllDir "liblz4.so.1.8.0.dll") (Join-Path (Join-Path $InstallPrefix "bin") "liblz4.dll")
    $IncDir = Join-Path $Lz4Root "include"
    Copy-Item (Join-Path $IncDir "*") (Join-Path $InstallPrefix "include")
}

function Install-Blosc {
    $BloscRoot = (Join-Path $StagingDirectory "c-blosc-1.12.1")
    if (!(Test-Path $BloscRoot)) {
	DownloadIfNotExists $DownloadBloscDest "https://github.com/Blosc/c-blosc/archive/v1.12.1.zip"
	Unzip $DownloadBloscDest $StagingDirectory
    }
    Push-Location
    Set-Location $BloscRoot
    if (!(Test-Path build)) {
	New-Item -ItemType Directory -Path build
    }
    Set-Location build
    cmake -A X64 -DCMAKE_INSTALL_PREFIX="$InstallPrefix" ..
    cmake --build . --config Release --target INSTALL
    $BloscDllDest = (Join-Path (Join-Path "$InstallPrefix" "bin") "blosc.dll")
    if (Test-Path $BloscDllDest) {
	Remove-Item $BloscDllDest
    }
    Move-Item (Join-Path (Join-Path "$InstallPrefix" "lib") "blosc.dll") $BloscDllDest
    Pop-Location
}

function Install-Zstd {
    $ZstdRoot = (Join-Path $StagingDirectory "zstd")
    if (!(Test-Path $ZstdRoot)) {
	DownloadIfNotExists $DownloadZstdDest "https://github.com/facebook/zstd/releases/download/v1.3.2/zstd-v1.3.2-win64.zip"
	New-Item -ItemType Directory -Path $ZstdRoot
	Unzip $DownloadZstdDest $ZstdRoot
    }
    $DllDir = Join-Path $ZstdRoot "dll"
    Copy-Item (Join-Path $DllDir "libzstd.lib") (Join-Path (Join-Path $InstallPrefix "lib") "libzstd.lib")
    Copy-Item (Join-Path $DllDir "libzstd.dll") (Join-Path (Join-Path $InstallPrefix "bin") "libzstd.dll")
    $IncDir = Join-Path $ZstdRoot "include"
    Copy-Item (Join-Path $IncDir "*") (Join-Path $InstallPrefix "include")
}

function Install-Bzip2 {
    $Bzip2Root = (Join-Path $StagingDirectory "bzip2")
    if (!(Test-Path $Bzip2Root)) {
	DownloadIfNotExists $DownloadBzip2Dest "https://github.com/TileDB-Inc/bzip2-windows/releases/download/v1.0.6/bzip2-1.0.6.zip"
	New-Item -ItemType Directory -Path $Bzip2Root
	Unzip $DownloadBzip2Dest $Bzip2Root
    }
    Push-Location
    Set-Location (Join-Path $Bzip2Root "bzip2-1.0.6")
    if (!(Test-Path build)) {
	New-Item -ItemType Directory -Path build
    }
    Set-Location build
    cmake -A X64 -DCMAKE_INSTALL_PREFIX="$InstallPrefix" ..
    cmake --build . --config Release --target INSTALL
    Pop-Location
}

function Install-All-Deps {
    if (!(Test-Path $StagingDirectory)) {
	New-Item -ItemType Directory -Path $StagingDirectory
    }
    Install-Zlib
    Install-LZ4
    Install-Blosc
    Install-Zstd
    Install-Bzip2
}

Install-All-Deps

Write-Host "Finished."
