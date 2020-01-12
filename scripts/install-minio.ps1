<#
.SYNOPSIS
This is a Powershell script to install the Minio server on Windows.

.DESCRIPTION
This is a Powershell script to install the Minio server on Windows.

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

#Set-PSDebug -Trace 2

$TileDBRootDirectory = Split-Path -Parent (Get-ScriptsDirectory)
Write-Host "TileDBRootDirectory: '$TileDBRootDirectory'"
$InstallPrefix = Join-Path $TileDBRootDirectory "dist"
$StagingDirectory = Join-Path (Get-ScriptsDirectory) "deps-staging"
$CertsDirectory = Join-Path $TileDBRootDirectory "test\inputs\test_certs"
Write-Host "Certs Directory: '$CertsDirectory' ---"
ls $CertsDirectory
Write-Host "---"
New-Item -ItemType Directory -Path (Join-Path $InstallPrefix bin) -ea 0

function DownloadFile([string] $URL, [string] $Dest) {
    Write-Host "Downloading $URL to $Dest..."
    Invoke-WebRequest -Uri $URL -OutFile $Dest
}

function DownloadIfNotExists([string] $Path, [string] $URL) {
    if (!(Test-Path $Path)) {
        # Minio uses TLS1.2
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        DownloadFile $URL $Path
    }
}
function Install-Minio {
    $MinioRoot = (Join-Path $StagingDirectory "minio")
    $DownloadMinioDest = Join-Path $MinioRoot "minio.exe"
    if (!(Test-Path $MinioRoot)) {
        New-Item -ItemType Directory -Path $MinioRoot
        DownloadIfNotExists $DownloadMinioDest "https://dl.minio.io/server/minio/release/windows-amd64/minio.exe"
    }
    Copy-Item $DownloadMinioDest (Join-Path $InstallPrefix "bin")
    ls "$InstallPrefix\bin"
}

function Export-Env {
    $env:MINIO_ACCESS_KEY = "minio"
    $env:MINIO_SECRET_KEY = "miniosecretkey"
    $env:AWS_ACCESS_KEY_ID = "minio"
    $env:AWS_SECRET_ACCESS_KEY = "miniosecretkey"
}

function Run-Minio {
    $ExePath = Join-Path $InstallPrefix "bin\minio.exe"
    $ServerConfigDir = Join-Path $InstallPrefix "etc\minio"
    $ServerDataDir = Join-Path $InstallPrefix "usr\local\share\minio"
    
    Write-Host "args -- ExePath: '$ExePath' ServerConfigDir: '$ServerConfigDir' ServerDataDir: '$ServerDataDir'"
    
    if (!(Test-Path $ServerDataDir)) {
        New-Item -ItemType Directory -Path $ServerDataDir
    }
    if (!(Test-Path $ServerConfigDir)) {
        New-Item -ItemType Directory -Path $ServerConfigDir
    }
    $global:minio_proc = Start-Process -FilePath $ExePath -NoNewWindow -PassThru -ArgumentList "server --address localhost:9999 --config-dir $ServerConfigDir --certs-dir $CertsDirectory $ServerDataDir"

    Start-Sleep 3.0

    Write-Host "Powershell version: " $PSVersionTable.PSVersion.Major


    if ($PSVersionTable.PSVersion.Major -ge 6) {
        Write-Host "Checking Minio with '-SkipCertificateCheck':"
        Write-Host "--------------------------------------------------------------------------------"
        Invoke-WebRequest -Uri https://127.0.0.1:9999/minio/health/live -SkipCertificateCheck
        Write-Host "--------------------------------------------------------------------------------"
        Invoke-WebRequest -Uri https://127.0.0.1:9999/minio/health/ready -SkipCertificateCheck
        Write-Host "--------------------------------------------------------------------------------"
    } else {
        Write-Host "Skipping minio check (powershell version < 6)"
    }

    python (Join-Path $TileDBRootDirectory "scripts/test-minio.py")
    if ($LastExitCode -ne 0) {
        throw "minio test failed!"
    }
}

function Install-All-Deps {
    if (!(Test-Path $StagingDirectory)) {
        New-Item -ItemType Directory -Path $StagingDirectory
    }
    Install-Minio
    Export-Env
    Run-Minio
}

Install-All-Deps

Write-Host "Finished with minio"
