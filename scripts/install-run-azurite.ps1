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
This is a Powershell script to install nodejs/azurite on Windows.

.DESCRIPTION
This is a Powershell script to install the nodejs/azurite server on Windows.

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

function Install-azurite {
    $NodeJSRoot = (Join-Path $StagingDirectory "nodejs")
    Write-Host "nodeJSRoot: " + $NodeJSRoot
    #$azuriteDataPath= (Join-Path $InstallPrefix "azurite-data")
    $azuriteDataPath= (Join-Path $env:TEMP "azurite-data")
    Write-Host "azuriteDataPath: " + $azuriteDataPath
    if(!(Test-Path $azuriteDataPath)){
       New-Item -ItemType Directory -Path $azuriteDataPath
	   #dir $azuriteDataPath
    }
    $DownloadNodeJSArchiveDest = (Join-Path $NodeJSRoot "")
    Write-Host "DownloadNodeJSArchiveDest: $DownloadNodeJSArchiveDest"
    $NodeJSVersion = "v14.15.4"
    if (!(Test-Path $NodeJSRoot)) {
       New-Item -ItemType Directory -Path $NodeJSRoot
	   #dir $NodeJSRoot
    }
    #pieces based on paths in downloaded archive - good until they change paths
    $NodeJSVersionedInstalledPath = (Join-Path $InstallPrefix ("node-" + $NodeJSVersion + "-win-x64") )
     if (!(Test-Path $NodeJSVersionedInstalledPath)){
        #nodejs binary archives have versioned filenames, be prepared to change if this version no longer accessible.
        $NodeJSBinaryArchiveName = "node-" + $NodeJSVersion + "-win-x64.zip"
        $NodeJSBinarySourceString = "https://nodejs.org/dist/"
        $NodeJSBinarySourceURL = "https://nodejs.org/dist/" + $NodeJSVersion + "/" + $NodeJSBinaryArchiveName
        $NodeJSArchivePathName = (Join-Path $DownloadNodeJSArchiveDest $NodeJSBinaryArchiveName)
        Write-Host "Downloading " + $NodeJSBinarySourceURL + " to " + $DownloadNodeJSArchiveDest
        DownloadIfNotExists $NodeJSArchivePathName $NodeJSBinarySourceURL
        $NodeJSArchivePathName = (Join-Path $DownloadNodeJSArchiveDest $NodeJSBinaryArchiveName)
        Write-Host "NodeJSArchivePathName: " + $NodeJSArchivePathName
        if (!(Test-Path $NodeJSArchivePathName)){
           Write-Host "***error, not finding $NodeJSArchivePathName!"
           #TBD: How to fault/exit?
           exit -1
        }
        #magic hash number retrieved from https://nodejs.org/dist/v14.15.4/SHASUMS256.txt.asc
        #b2a0765240f8fbd3ba90a050b8c87069d81db36c9f3745aff7516e833e4d2ed6  node-v14.15.4-win-x64.zip
        $archiveHashExpected = "b2a0765240f8fbd3ba90a050b8c87069d81db36c9f3745aff7516e833e4d2ed6"
        $computedHash = (Get-FileHash -Algorithm SHA256 $NodeJSArchivePathName).Hash.ToLower()
        if(! ($archiveHashExpected.Equals($computedHash))) {
           Write-Host "***Error: unexpected hash value for archive $NodeJSArchivePathName"
           Write-Host "(expected $archiveHashExpected)"
           Write-Host "(got      $computedHash)"
           #TBD: How to fault/exit/error?
           Exit -2
        }
        else {
           Write-Host "checksum match, archive $NodeJSArchivePathName"
        }
        Expand-Archive -LiteralPath $NodeJSArchivePathName -DestinationPath $InstallPrefix
        #Create a 'nodejs' directory link name so other scripts can more
        $cmdLinkGenericNodeJSName = "mklink /j " + (Join-Path $InstallPrefix "NodeJS") + " " + $NodeJSVersionedInstalledPath
        cmd /c $cmdLinkGenericNodeJSName
    }
    $NodeJSGenericInstalledPath = Join-Path $InstallPrefix "NodeJS"
    $NodeJSInitBatchFile = (Join-Path $NodeJSGenericInstalledPath "nodevars.bat")
    Write-Host "NodeJSGenericInstalledPath: " + $NodeJSGenericInstalledPath

    #need this to have access to 'node' of nodejs...
    run-batchfile-modifying-path $NodeJSInitBatchFile
    $CurPath = Get-Content "env:PATH"
    Write-Host "path: " $CurPath #can examine to check that 'node' will be accessible
    $npmCmdPath = (Join-Path $NodeJSGenericInstalledPath "npm.cmd")
    Push-Location
    #change directory into nodejs to avoid npm complaints and creation of package.json etc. 'elsewhere'
    cd $NodeJSGenericInstalledPath
    write-host "installing azurite from:"
    Get-Location
    cmd /c "$npmCmdPath install -g azurite" #install globally
    Pop-Location

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

function Install-All-Deps {
    if (!(Test-Path $StagingDirectory)) {
        New-Item -ItemType Directory -Path $StagingDirectory
    }
    Install-azurite
}

function remove-preexisting() {
    #***!!!for now ignoring that these might be used by other packages (minio?)
    #-force needed due to possible presence of 'NodeJS' link
    Remove-Item -force $InstallPrefix
	Remove-Item $StagingDirectory
}

Install-All-Deps

Write-Host "Finished."
