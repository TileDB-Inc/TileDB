<#
.SYNOPSIS
This is a Powershell script to install the S3 SDK on Windows.

.DESCRIPTION
This is a Powershell script to install the S3 SDK on Windows.

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

function DownloadFile([string] $URL, [string] $Dest) {
    Write-Host "Downloading $URL to $Dest..."
    Invoke-WebRequest -Uri $URL -OutFile $Dest
}

function DownloadIfNotExists([string] $Path, [string] $URL) {
    if (!(Test-Path $Path)) {
	DownloadFile $URL $Path
    }
}

# Unzips all directories inside the archive with paths starting with $ZipNestedDir
Add-Type -AssemblyName System.IO.Compression.FileSystem
function UnzipDirsMatching([string] $Zipfile, [string] $ZipNestedDir, [string] $Dest) {
    Write-Host "Extracting $Zipfile\$ZipNestedDir* to $Dest..."
    $Archive = [System.IO.Compression.ZipFile]::OpenRead($Zipfile)
    foreach ($Entry in $Archive.Entries) {
	$EntryIsDir = ([string]::IsNullOrEmpty([System.IO.Path]::GetFileName($Entry.FullName)))
	if (!$EntryIsDir -and $Entry.FullName.StartsWith($ZipNestedDir)) {
	    $NestedDest = Join-Path $Dest $Entry.FullName
	    $DirToCreate = Split-Path -Path $NestedDest
	    if (!(Test-Path $DirToCreate)) {
		New-Item -ItemType Directory -Path $DirToCreate | Out-Null
	    }
	    [System.IO.Compression.ZipFileExtensions]::ExtractToFile($Entry, $NestedDest)
	}
    }
}

# Unzips a single file from the archive.
function UnzipSpecific([string] $Zipfile, [string] $ZipPath, [string] $Dest) {
    Write-Host "Extracting $Zipfile\$ZipPath to $Dest..."
    $Archive = [System.IO.Compression.ZipFile]::OpenRead($Zipfile)
    $Entry = $Archive.GetEntry($ZipPath)
    $NestedDest = Join-Path $Dest $Entry.FullName
    $DirToCreate = Split-Path -Path $NestedDest
    if (!(Test-Path $DirToCreate)) {
	New-Item -ItemType Directory -Path $DirToCreate | Out-Null
    }
    [System.IO.Compression.ZipFileExtensions]::ExtractToFile($Entry, $NestedDest)
}

function Install-S3 {
    $S3Version = "1.3.21"
    $S3Root = (Join-Path $StagingDirectory "s3")
    $DownloadS3Dest = Join-Path $StagingDirectory "s3.zip"
    if (!(Test-Path $S3Root)) {
	DownloadIfNotExists $DownloadS3Dest "https://github.com/aws/aws-sdk-cpp/archive/$S3Version.zip"
	New-Item -ItemType Directory -Path $S3Root
	UnzipDirsMatching $DownloadS3Dest "aws-sdk-cpp-$S3Version/aws-cpp-sdk-config" $S3Root
	UnzipDirsMatching $DownloadS3Dest "aws-sdk-cpp-$S3Version/aws-cpp-sdk-core" $S3Root
	UnzipDirsMatching $DownloadS3Dest "aws-sdk-cpp-$S3Version/aws-cpp-sdk-s3" $S3Root
	UnzipDirsMatching $DownloadS3Dest "aws-sdk-cpp-$S3Version/aws-cpp-sdk-transfer" $S3Root
	UnzipDirsMatching $DownloadS3Dest "aws-sdk-cpp-$S3Version/cmake" $S3Root
	UnzipDirsMatching $DownloadS3Dest "aws-sdk-cpp-$S3Version/code-generation" $S3Root
	UnzipDirsMatching $DownloadS3Dest "aws-sdk-cpp-$S3Version/scripts" $S3Root
	UnzipDirsMatching $DownloadS3Dest "aws-sdk-cpp-$S3Version/testing-resources" $S3Root
	UnzipDirsMatching $DownloadS3Dest "aws-sdk-cpp-$S3Version/toolchains" $S3Root
	UnzipSpecific $DownloadS3Dest "aws-sdk-cpp-$S3Version/CMakeLists.txt" $S3Root
    }
    Push-Location
    Set-Location "$S3Root/aws-sdk-cpp-$S3Version"
    if (!(Test-Path build)) {
    	New-Item -ItemType Directory -Path build
    }
    Set-Location build
    cmake -A X64 -DCMAKE_INSTALL_PREFIX="$InstallPrefix" -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="/wd4996 /wd4530" -DBUILD_ONLY="s3;core;transfer;config" -DCUSTOM_MEMORY_MANAGEMENT=0 ..
    cmake --build . --config Release --target INSTALL
    Pop-Location
}

function Install-All-Deps {
    if (!(Test-Path $StagingDirectory)) {
	New-Item -ItemType Directory -Path $StagingDirectory
    }
    Install-S3
}

Install-All-Deps

Write-Host "Finished."
