<#
.SYNOPSIS
This is a Powershell script to generate vcpkg empty port overlays.

.DESCRIPTION
This script is intended to be used by downstream packagers to
select which dependencies to acquire from the system instead
of building them with vcpkg.

Because the set of system dependencies is in most cases statically
known, you are suggested to run this script locally and commit its
output to source control.

To get all dependencies from the system, you should entirely disable
downloading vcpkg by defining the TILEDB_DISABLE_AUTO_VCPKG environment
variable, instead of using this script.

.PARAMETER ManifestFile
Path to the vcpkg manifest file to read.
This is important to generate the features for each port.
Defaults to "vcpkg.json".

.PARAMETER Output
Path to the directory to output the empty port overlays.
Defaults to "system-ports".

.PARAMETER Ports
List of ports to generate empty port overlays for.

.PARAMETER FeaturePorts
List of features to generate empty port overlays for their dependencies.

.EXAMPLE

    .\gen-system-ports.ps1 -ManifestFile ../TileDB/vcpkg.json -Output system-ports -Ports bzip2,zlib,zstd -FeaturePorts azure,gcs,s3,serialization

.LINK
https://github.com/TileDB-Inc/TileDB
#>

[CmdletBinding()]
Param(
    [string]$ManifestFile = "vcpkg.json",
    [string]$Output = "system-ports",
    [string[]]$Ports = @(),
    [string[]]$FeaturePorts = @()
)

$manifest = Get-Content $ManifestFile | ConvertFrom-Json -AsHashtable

function Get-DependencyName($dep) {
    if ($dep -is [string]) {
        return $dep
    }
    return $dep.name
}

$features = @{}

$manifest.dependencies + ($manifest.features.Values | ForEach-Object { $_.dependencies })
| ForEach-Object {
    $depName = Get-DependencyName $_
    # Add the dependency to the features list if it doesn't exist.
    if (-not $features.ContainsKey($depName)) {
        $features[$depName] = New-Object Collections.Generic.List[string]
    }
    if ($null -ne $_.features) {
        $_.features
        | ForEach-Object {
            $features[$depName].Add($_)
        }
    }
}

$Ports + ($FeaturePorts | ForEach-Object { $manifest.features[$_].dependencies } | ForEach-Object { Get-DependencyName $_ })
| ForEach-Object {
    mkdir "$Output/$_" -Force > $null
    $portManifest = @{
        name             = $_
        "version-string" = "system"
    }
    if (-not $features.ContainsKey($_)) {
        Write-Warning "Port $_ was not specified in manifest file; features will not be generated."
    }
    elseif ($features[$_].Count -gt 0) {
        $portManifest.features = , ($features[$_] | ForEach-Object { @{ name = $_; description = "" } })
    }
    $portManifest
    | ConvertTo-Json -Depth 10
    | Out-File "$Output/$_/vcpkg.json"
    "set(VCPKG_POLICY_EMPTY_PACKAGE enabled)" | Out-File "$Output/$_/portfile.cmake"
}
