<#
.SYNOPSIS
This is a Powershell script to install the GCS server on Windows.

.DESCRIPTION
This is a Powershell script to install the GCS server on Windows.

.LINK
https://github.com/TileDB-Inc/TileDB
#>

[CmdletBinding()]
Param(
)

# Check for python, leave if unable to find.
if(!(Get-Command python -ErrorAction SilentlyContinue)){
  Write-Host "***ERROR: Unable to find python, aborting!"
  exit -1
}

# Return the directory containing this script file.
function Get-ScriptsDirectory {
    Split-Path -Parent $PSCommandPath
}

$TileDBRootDirectory = Split-Path -Parent (Get-ScriptsDirectory)
$InstallPrefix = Join-Path $TileDBRootDirectory "dist"
$StagingDirectory = Join-Path (Get-ScriptsDirectory) "deps-staging"
#$CertsDirectory = Join-Path $TileDBRootDirectory "test/inputs/test_certs"
New-Item -ItemType Directory -Path (Join-Path $InstallPrefix bin) -ea 0

function DownloadFile([string] $URL, [string] $Dest) {
    Write-Host "Downloading $URL to $Dest..."
    Invoke-WebRequest -Uri $URL -OutFile $Dest
}

function DownloadIfNotExists([string] $Path, [string] $URL) {
    if (!(Test-Path $Path)) {
        # ? uses TLS1.2
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        DownloadFile $URL $Path
    }
}

function install_gcs() {
    if (!(Test-Path $StagingDirectory)) {
        New-Item -ItemType Directory -Path $StagingDirectory
    }


#    git clone https://github.com/googleapis/google-cloud-cpp.git $StagingDirectory
#    #sudo pip3 install -r google-cloud-cpp/google/cloud/storage/emulator/requirements.txt
#	pip3 install -r $StagingDirectory/google-cloud-cpp/google/cloud/storage/emulator/requirements.txt

    $GCSVersion = "1.23.0"
	Write-Host "GCSVersion: $GCSVersion"
	$GCSVersionName = "v$GCSVersion"
	Write-Host "GCSVersionName: $GCSVersionName"
    #curl -L https://github.com/googleapis/google-cloud-cpp/archive/$GCSVersionName.tar.gz -o $StagingDirectory/google-cloud-cpp.$GCSVersionName.tar.gz
	#$fetchpath = "https://github.com/googleapis/google-cloud-cpp/archive/$GCSVersionName.tar.gz"
	$fetchpath = "https://github.com/googleapis/google-cloud-cpp/archive/$GCSVersionName.zip"
	write-host "fetching: $fetchpath"
	#$writepath = "$StagingDirectory/google-cloud-cpp.$GCSVersionName.tar.gz"
	$writepath = "$StagingDirectory/google-cloud-cpp.$GCSVersionName.zip"
	write-host "to: $writepath"
    # curl -L $fetchpath -o $writepath
	DownloadIfNotExists $writepath $fetchpath
	#exit 1
    #tar -xf /tmp/google-cloud-cpp.tar.gz -C /tmp
	#Expand-Archive -LiteralPath $StagingDirectory/google-cloud-cpp.$GCSVersionName.tar.gz -DestinationPath $InstallPrefix
	Expand-Archive -LiteralPath $writepath -DestinationPath $InstallPrefix
    pip3 install -r $InstallPrefix/google-cloud-cpp-$GCSVersion/google/cloud/storage/emulator/requirements.txt
}


install_gcs
