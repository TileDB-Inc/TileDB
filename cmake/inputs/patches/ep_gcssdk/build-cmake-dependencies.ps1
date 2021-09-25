# !/usr/bin/env powershell
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Stop on errors. This is similar to `set -e` on Unix shells.
$ErrorActionPreference = "Stop"

# First check the required environment variables.
if (-not (Test-Path env:CONFIG)) {
    Write-Host -ForegroundColor Red `
        "Aborting build because the CONFIG environment variable is not set."
    Exit 1
}
if (-not (Test-Path env:VCPKG_TRIPLET)) {
    Write-Host -ForegroundColor Red `
        "Aborting build because the VCPKG_TRIPLET environment variable is not set."
    Exit 1
}

$RunningCI = (Test-Path env:RUNNING_CI) -and `
    ($env:RUNNING_CI -eq "yes")
$IsCI = (Test-Path env:KOKORO_JOB_TYPE) -and `
    ($env:KOKORO_JOB_TYPE -eq "CONTINUOUS_INTEGRATION")
$IsPR = (Test-Path env:KOKORO_JOB_TYPE) -and `
    ($env:KOKORO_JOB_TYPE -eq "PRESUBMIT_GITHUB")
$HasBuildCache = (Test-Path env:BUILD_CACHE)

$vcpkg_base = "vcpkg"
$packages = @("zlib", "openssl",
              "protobuf", "c-ares", "benchmark",
              "grpc", "gtest", "crc32c", "curl",
              "nlohmann-json")
$vcpkg_flags=@(
    "--triplet", "${env:VCPKG_TRIPLET}")
if ($args.count -ge 1) {
    $vcpkg_base, $packages = $args
    $vcpkg_flags=("--triplet", "${env:VCPKG_TRIPLET}")
}
$vcpkg_dir = "cmake-out\${vcpkg_base}"

$vcpkg_version = "5214a247018b3bf2d793cea188ea2f2c150daddd"
$vcpkg_tool_version = "2021-01-13-768d8f95c9e752603d2c5901c7a7c7fbdb08af35"

New-Item -ItemType Directory -Path "cmake-out" -ErrorAction SilentlyContinue
# Download the right version of `vcpkg`
if (Test-Path "${vcpkg_dir}") {
    Write-Host -ForegroundColor Green "`n$(Get-Date -Format o) vcpkg directory already exists."
} else {
    ForEach($_ in (1, 2, 3)) {
        if ( $_ -ne 1) {
            Start-Sleep -Seconds (60 * $_)
        }
        Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) " `
            "Downloading vcpkg ports archive [$_]"
        try {
            (New-Object System.Net.WebClient).Downloadfile(
                    "https://github.com/microsoft/vcpkg/archive/${vcpkg_version}.zip",
                    "cmake-out\${vcpkg_version}.zip")
            break
        } catch {
            Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) download error"
        }
    }
#    7z x -ocmake-out "cmake-out\${vcpkg_version}.zip" -aoa -bsp0
    Expand-Archive -DestinationPath cmake-out -Path "cmake-out\${vcpkg_version}.zip"
    if ($LastExitCode) {
        # Ignore errors, caching failures should not break the build.
        Write-Host -ForegroundColor Red "`n$(Get-Date -Format o) " `
            "extracting vcpkg from archive failed with exit code ${LastExitCode}."
        Exit 1
    }
    Set-Location "cmake-out"
    Rename-Item "vcpkg-${vcpkg_version}" "${vcpkg_base}"
    Set-Location ".."
}
if (-not (Test-Path "${vcpkg_dir}")) {
    Write-Host -ForegroundColor Red "Missing vcpkg directory (${vcpkg_dir})."
    Exit 1
}

if (Test-Path "${vcpkg_dir}\vcpkg.exe") {
    Write-Host -ForegroundColor Green "`n$(Get-Date -Format o) vcpkg executable already exists."
} else {
    ForEach($_ in (1, 2, 3)) {
        if ( $_ -ne 1) {
            Start-Sleep -Seconds (60 * $_)
        }
        Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) " `
            "Downloading vcpkg executable [$_]"
        try {
            (New-Object System.Net.WebClient).Downloadfile(
                    "https://github.com/microsoft/vcpkg-tool/releases/download/${vcpkg_tool_version}/vcpkg.exe",
                    "${vcpkg_dir}\vcpkg.exe")
            break
        } catch {
            Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) download error"
        }
    }
}
if (-not (Test-Path "${vcpkg_dir}\vcpkg.exe")) {
    Write-Host -ForegroundColor Red "Missing vcpkg executable (${vcpkg_dir}\vcpkg.exe)."
    Exit 1
}

# If BUILD_CACHE is set (which typically is on Kokoro builds), try
# to download and extract the build cache.
if ($RunningCI -and $HasBuildCache) {
    gcloud auth activate-service-account `
        --key-file "${env:KOKORO_GFILE_DIR}/build-results-service-account.json"
    Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) " `
        "rsync vcpkg binary cache from GCS."
    New-Item "${env:LOCALAPPDATA}\vcpkg" -ItemType Directory -ErrorAction SilentlyContinue
    New-Item "${env:LOCALAPPDATA}\vcpkg\archives" -ItemType Directory -ErrorAction SilentlyContinue
    gsutil -m -q rsync -r ${env:BUILD_CACHE} "${env:LOCALAPPDATA}\vcpkg\archives"
    if ($LastExitCode) {
        # Ignore errors, caching failures should not break the build.
        Write-Host "gsutil download failed with exit code $LastExitCode"
    }
}

# Integrate installed packages into the build environment.
&"${vcpkg_dir}\vcpkg.exe" integrate install
if ($LastExitCode) {
    Write-Host -ForegroundColor Red "vcpkg integrate failed with exit code $LastExitCode"
    Exit ${LastExitCode}
}

# Remove old versions of the packages.
Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) Cleanup old vcpkg package versions."
&"${vcpkg_dir}\vcpkg.exe" remove ${vcpkg_flags} --outdated --recurse
if ($LastExitCode) {
    Write-Host -ForegroundColor Red "vcpkg remove --outdated failed with exit code $LastExitCode"
    Exit ${LastExitCode}
}

Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) Building vcpkg package versions."
foreach ($pkg in $packages) {
    &"${vcpkg_dir}\vcpkg.exe" install ${vcpkg_flags} "${pkg}"
    if ($LastExitCode) {
        Write-Host -ForegroundColor Red "vcpkg install $pkg failed with exit code $LastExitCode"
        Exit ${LastExitCode}
    }
}

Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) vcpkg list"
&"${vcpkg_dir}\vcpkg.exe" list

Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) Cleanup vcpkg buildtrees"
Get-ChildItem -Recurse -File `
        -ErrorAction SilentlyContinue `
        -Path "buildtrees" | `
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) Disk(s) size and space for troubleshooting"
    Get-CimInstance -Class CIM_LogicalDisk | `
        Select-Object -Property DeviceID, DriveType, VolumeName, `
            @{L='FreeSpaceGB';E={"{0:N2}" -f ($_.FreeSpace /1GB)}}, `
            @{L="Capacity";E={"{0:N2}" -f ($_.Size/1GB)}}

# Do not update the vcpkg cache on PRs, it might dirty the cache for any
# PRs running in parallel, and it is a waste of time in most cases.
if ($RunningCI -and $IsCI -and $HasBuildCache) {
        Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) " `
    "rsync vcpkg binary cache to GCS."
    gcloud auth activate-service-account `
        --key-file "${env:KOKORO_GFILE_DIR}/build-results-service-account.json"
    gsutil -m -q rsync -r "${env:LOCALAPPDATA}\vcpkg\archives" ${env:BUILD_CACHE}
} else {
    Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) " `
      "vcpkg not updated IsCI = $IsCI, IsPR = $IsPR, " `
      "HasBuildCache = $HasBuildCache."
}

Write-Host -ForegroundColor Yellow "`n$(Get-Date -Format o) Disk(s) size and space for troubleshooting"
Get-CimInstance -Class CIM_LogicalDisk | `
    Select-Object -Property DeviceID, DriveType, VolumeName, `
        @{L='FreeSpaceGB';E={"{0:N2}" -f ($_.FreeSpace /1GB)}}, `
        @{L="Capacity";E={"{0:N2}" -f ($_.Size/1GB)}}
