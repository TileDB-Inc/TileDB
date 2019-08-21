# PowerShell script for performing TileDB bootstrapping process on
# Windows.

<#
.SYNOPSIS
This is a Powershell script to bootstrap a TileDB build on Windows.

.DESCRIPTION
This script will check dependencies, and run the CMake build generator
to generate a Visual Studio solution file that can be used to build or
develop TileDB.

.PARAMETER Prefix
Installs files in tree rooted at PREFIX (defaults to TileDB\dist).

.PARAMETER Dependency
Semicolon separated list to binary dependencies.

.PARAMETER CMakeGenerator
Optionally specify the CMake generator string, e.g. "Visual Studio 15
2017". Check 'cmake --help' for a list of supported generators.

.PARAMETER EnableDebug
Enable Debug build.

.PARAMETER EnableCoverage
Enable build with code coverage support.

.PARAMETER EnableVerbose
Enable verbose status messages.

.PARAMETER EnableS3
Enables building with the S3 storage backend.

.PARAMETER EnableSerialization
Enables building with serialization support.

.PARAMETER EnableStaticTileDB
Enables building TileDB as a static library.

.PARAMETER EnableBuildDeps
Enables building TileDB dependencies from source (superbuild)

.PARAMETER EnableTools
Enables building TileDB CLI tools (experimental)

.Parameter DisableWerror
Disable use of warnings-as-errors (/WX) during build.

.Parameter DisableCppApi
Disable building the TileDB C++ API.

.Parameter DisableTBB
Disable use of TBB for parallelization.

.PARAMETER BuildProcesses
Number of parallel compile jobs.

.LINK
https://github.com/TileDB-Inc/TileDB

.EXAMPLE
..\bootstrap.ps1 -Prefix "\path\to\install" -Dependency "\path\to\dep1;\path\to\dep2"

#>

# Note -Debug and -Verbose are included in the PowerShell
# CommonParameters list, so we don't duplicate them here.
# TODO: that means they don't appear in the -? usage message.
[CmdletBinding()]
Param(
    [string]$Prefix,
    [string]$Dependency,
    [string]$CMakeGenerator,
    [switch]$EnableDebug,
    [switch]$EnableCoverage,
    [switch]$EnableVerbose,
    [switch]$EnableS3,
    [switch]$EnableSerialization,
    [switch]$EnableStaticTileDB,
    [switch]$EnableTools,
    [switch]$EnableBuildDeps,
    [switch]$DisableWerror,
    [switch]$DisableCppApi,
    [switch]$DisableTests,
    [switch]$DisableTBB,
    [switch]$DisableStats,
    [Alias('J')]
    [int]
    $BuildProcesses = $env:NUMBER_OF_PROCESSORS
)

# Return the directory containing this script file.
function Get-ScriptDirectory {
    Split-Path -Parent $PSCommandPath
}

# Absolute path of TileDB directories.
$SourceDirectory = Get-ScriptDirectory
$BinaryDirectory = (Get-Item -Path ".\" -Verbose).FullName

# Choose the default install prefix.
$DefaultPrefix = Join-Path $SourceDirectory "dist"

# Choose the default dependency install prefix.
$DefaultDependency = $DefaultPrefix

# Set TileDB build type
$BuildType = "Release"
if ($EnableDebug.IsPresent) {
    $BuildType = "Debug"
}
if ($EnableCoverage.IsPresent) {
    $BuildType = "Coverage"
}

# Set TileDB verbosity
$Verbosity = "OFF"
if ($EnableVerbose.IsPresent) {
    $Verbosity = "ON"
}

# Set TileDB S3 flag
$UseS3 = "OFF"
if ($EnableS3.IsPresent) {
    $UseS3 = "ON"
}

$UseSerialization = "OFF"
if ($EnableSerialization.IsPresent) {
    $UseSerialization = "ON"
}

$Werror = "ON"
if ($DisableWerror.IsPresent) {
    $Werror = "OFF"
}

$CppApi = "ON"
if ($DisableCppApi.IsPresent) {
    $CppApi = "OFF"
}

$Tests = "ON"
if ($DisableTests.IsPresent) {
    $Tests = "OFF"
}

$TBB = "ON"
if ($DisableTBB.IsPresent) {
    $TBB = "OFF"
}

$Stats = "ON"
if ($DisableStats.IsPresent) {
    $Stats = "OFF"
}

$TileDBStatic = "OFF";
if ($EnableStaticTileDB.IsPresent) {
    $TileDBStatic = "ON"
}

$TileDBTools = "OFF";
if ($EnableTools.IsPresent) {
    $TileDBTools = "ON"
}

$TileDBBuildDeps = "OFF";
if ($EnableBuildDeps.IsPresent) {
    $TileDBBuildDeps = "ON"
}

# Set TileDB prefix
$InstallPrefix = $DefaultPrefix
if ($Prefix.IsPresent) {
    $InstallPrefix = $Prefix
}

# Set TileDB dependency directory string
$DependencyDir = $DefaultDependency
if ($Dependency.IsPresent) {
    $DependencyDir = $Dependency
}

# Set CMake generator type.
$GeneratorFlag = ""
if ($PSBoundParameters.ContainsKey("CMakeGenerator")) {
    $GeneratorFlag = "-G""$CMakeGenerator"""
}

# Enforce out-of-source build
if ($SourceDirectory -eq $BinaryDirectory) {
    Throw "Cannot build the project in the source directory! Out-of-source build is enforced!"
}

# Check cmake binary
if ((Get-Command "cmake.exe" -ErrorAction SilentlyContinue) -eq $null) {
    Throw "Unable to find cmake.exe in your PATH."
}
if ($CMakeGenerator -eq $null) {
    Throw "Could not identify a generator for CMake. Do you have Visual Studio installed?"
}

# Run CMake.
# We use Invoke-Expression so we can echo the command to the user.
$CommandString = "cmake -A X64 -DCMAKE_BUILD_TYPE=$BuildType -DCMAKE_INSTALL_PREFIX=""$InstallPrefix"" -DCMAKE_PREFIX_PATH=""$DependencyDir"" -DMSVC_MP_FLAG=""/MP$BuildProcesses"" -DTILEDB_VERBOSE=$Verbosity -DTILEDB_S3=$UseS3 -DTILEDB_SERIALIZATION=$UseSerialization -DTILEDB_WERROR=$Werror -DTILEDB_CPP_API=$CppApi -DTILEDB_TESTS=$Tests -DTILEDB_TBB=$TBB -DTILEDB_TBB_SHARED=ON -DTILEDB_STATS=$Stats -DTILEDB_STATIC=$TileDBStatic -DTILEDB_FORCE_ALL_DEPS=$TileDBBuildDeps -DTILEDB_TOOLS=$TileDBTools $GeneratorFlag ""$SourceDirectory"""
Write-Host $CommandString
Write-Host
Invoke-Expression "$CommandString"

Write-Host "Bootstrap success. Run 'cmake --build . --config $BuildType' to build and 'cmake --build . --target check --config $BuildType' to test."
