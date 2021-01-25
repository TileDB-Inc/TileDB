<#
NOTE: This does take measurable time to run, evaluate whether running
repeatedly in CI is preferable to just having an image that has had it run
to complete installation with it remaining available in that image (runtime
cost when CI instances currently overloaded versus required image storage -
and whether other dependencies might also require nodejs separately from
azurite.

#>

<#
.SYNOPSIS
This is a Powershell script to run previously installed azurite on Windows.

.DESCRIPTION
This is a Powershell script to run previously installed azurite server on Windows.

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
        # TLS1.2 seems to work for nodejs site
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        DownloadFile $URL $Path
    }
}

function run-batchfile-modifying-path([string] $scriptPath) {
    Set-StrictMode -Version 3
    $tempfile = [IO.Path]::GetTempFileName()
    cmd /c " `"$scriptPath`" && set > `"$tempfile`" "
    Get-Content $tempfile | ForEach-Object {
        if($_ -match "^(PATH)=(.*)$") {
            Set-Content "env:PATH" $matches[2]
        }
    }
    Remove-Item $tempfile
}


function run-azurite() {
    $azuriteDataPath= (Join-Path $env:TEMP "azurite-data")
    $NodeJSGenericInstalledPath = Join-Path $InstallPrefix "NodeJS"
    if(!(Test-Path $NodeJSGenericInstalledPath)) {
	    Write-Host "Unable to find $NodeJSGenericInstalledPath!"
	}
    #presumes that the generic 'NodeJS' link to the versioned install directory exists.
    $NodeJSInitBatchFile = (Join-Path $NodeJSGenericInstalledPath "nodevars.bat")
    Write-Host "NodeJSGenericInstalledPath: " + $NodeJSGenericInstalledPath

    #need this to have access to 'node' of nodejs...
    run-batchfile-modifying-path $NodeJSInitBatchFile

    #now actually run azurite
    $npxCmdPath = (Join-Path $NodeJSGenericInstalledPath "npx.cmd")
    $azuriteDebugLog = (Join-Path $azuriteDataPath "Debug.Log")
    Write-Host "azuriteDebugLog: " + $azuriteDebugLog
    #parameters for azurite-blob taken from those used in run-azurite.sh
    #seems that npx(/npm?) is causing desired window title to be overwritten (title shown is 'npm')...?
    #despite... https://stackoverflow.com/questions/30611803/how-to-set-the-title-of-a-windows-console-while-npm-is-running
    $npxCmd = "start `"azurite host window`" " + $npxCmdPath + " azurite-blob" + " --silent --location " + $azuriteDataPath + " --debug " + $azuriteDebugLog + " --blobPort 10000 --blobHost 127.0.0.1 "
    Write-Host "npxCmd: $npxCmd"
    #this will leave a command prompt window open with nodejs/azurite running in it.
    cmd /c $npxCmd
}

function remove-preexisting() {
    #***!!!for now ignoring that these might be used by other packages (minio?)
    #-force needed due to possible presence of 'NodeJS' link
    Remove-Item -force $InstallPrefix
	Remove-Item $StagingDirectory
}

run-azurite

Write-Host "Finished."
