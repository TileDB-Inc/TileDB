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

.PARAMETER VcpkgBaseTriplet
Optionally specify the vcpkg target triplet, e.g. "x64-windows-release".
Defaults to automatically detecting it from the environment.

.PARAMETER Dependency
Semicolon separated list to binary dependencies.

.PARAMETER Linkage
Specify the linkage type to build TileDB with. Valid values are
"static" and "shared". Default is "shared".

.PARAMETER RemoveDeprecations
Build TileDB without any deprecated APIs.

.PARAMETER Architecture
Specify the architecture to configure for.

.PARAMETER CMakeGenerator
Optionally specify the CMake generator string, e.g. "Visual Studio 15
2017". Check 'cmake --help' for a list of supported generators.

.PARAMETER EnableDebug
Enable Debug build.
When using multi-config CMake generators this option is ignored and
you can just pass "--config Debug" when building with CMake.

.PARAMETER EnableAssert
Enable Assertions in compiled code (always on for debug build;
default off in release).

.PARAMETER EnableReleaseSymbols
Enable symbols with Release build.
When using multi-config CMake generators this option is ignored and
you can just pass "--config RelWithDebInfo" when building with CMake.

.PARAMETER EnableCoverage
Enable build with code coverage support.

.PARAMETER EnableVerbose
Enable verbose status messages.

.PARAMETER EnableAzure
Enables building with the Azure storage backend.

.PARAMETER EnableS3
Enables building with the S3 storage backend.

.PARAMETER EnableGcs
Enables building with the GCS storage backend.

.PARAMETER EnableSerialization
Enables building with serialization support.

.PARAMETER EnableStaticTileDB
Enables building TileDB as a static library.
Deprecated, use -Linkage static instead.

.PARAMETER EnableBuildDeps
Unused, kept for compatibility.

.PARAMETER EnableTools
Enables building TileDB CLI tools (experimental)

.PARAMETER EnableArrowTests
Enables the compilation of the arrow adapter unit tests

.PARAMETER EnableAwsS3Config
Enables AWS S3 configuration for unit tests

.PARAMETER DisableWebP
Disables building of WebP and simple linkage test

.PARAMETER EnableRust
Enables building Rust tests

.Parameter DisableWerror
Disable use of warnings-as-errors (/WX) during build.

.Parameter DisableCppApi
Disable building the TileDB C++ API.

.Parameter DisableTests
Disable building the TileDB tests.

.Parameter DisableStats
Disables internal TileDB statistics.

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
    [string]$VcpkgBaseTriplet,
    [string]$Dependency,
    [string]$Linkage = "shared",
    [switch]$RemoveDeprecations,
    [string]$Architecture,
    [string]$CMakeGenerator,
    [switch]$EnableAssert,
    [switch]$EnableDebug,
    [switch]$EnableReleaseSymbols,
    [switch]$EnableCoverage,
    [switch]$EnableVerbose,
    [switch]$EnableAzure,
    [switch]$EnableS3,
    [switch]$EnableGcs,
    [switch]$EnableSerialization,
    [switch]$EnableStaticTileDB,
    [switch]$EnableTools,
    [switch]$EnableExperimentalFeatures,
    [switch]$EnableBuildDeps,
    [switch]$EnableArrowTests,
    [switch]$EnableAwsS3Config,
    [switch]$DisableWebP,
    [switch]$EnableRust,
    [switch]$DisableWerror,
    [switch]$DisableCppApi,
    [switch]$DisableTests,
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
$DefaultPrefix = Join-Path $BinaryDirectory "dist"

# Choose the default dependency install prefix.
$DefaultDependency = $DefaultPrefix

# Set the vcpkg base triplet.
if ($VcpkgBaseTriplet.IsPresent) {
    $VcpkgBaseTriplet = "-DTILEDB_VCPKG_BASE_TRIPLET=$VcpkgBaseTriplet"
}

# Set assertion mode
# No-op for a debug build.
$AssertionMode = "OFF"
if ($EnableAssert.IsPresent) {
  $AssertionMode = "ON"
}

# Set TileDB build type
$BuildType = "Release"
if ($EnableDebug.IsPresent) {
    $BuildType = "Debug"
}
if ($EnableReleaseSymbols.IsPresent) {
    $BuildType = "RelWithDebInfo"
}

if ($EnableCoverage.IsPresent) {
    $BuildType = "Coverage"
}

# Set TileDB verbosity
$Verbosity = "OFF"
if ($EnableVerbose.IsPresent) {
    $Verbosity = "ON"
}

if ($RemoveDeprecations.IsPresent) {
    $_RemoveDeprecations = "ON"
}
else {
    $_RemoveDeprecations = "OFF"
}

# Set TileDB Azure flag
$UseAzure = "OFF"
if ($EnableAzure.IsPresent) {
    $UseAzure = "ON"
}

# Set TileDB S3 flag
$UseS3 = "OFF"
if ($EnableS3.IsPresent) {
    $UseS3 = "ON"
}

# Set TileDB GCS flag
$UseGcs = "OFF"
if ($EnableGcs.IsPresent) {
    $UseGcs = "ON"
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

$Stats = "ON"
if ($DisableStats.IsPresent) {
    $Stats = "OFF"
}

$BuildWebP="ON"
if ($DisableWebP.IsPresent) {
  $BuildWebP="OFF"
}

$BuildRust="OFF"
if ($EnableRust.IsPresent) {
  $BuildRust="ON"
}

$BuildSharedLibs = "ON";
if ($Linkage -eq "static") {
    $BuildSharedLibs = "OFF"
}
elseif ($Linkage -ne "shared") {
    Write-Error "Invalid linkage type: $Linkage. Valid values are 'static' and 'shared'."
    exit 1
}

if ($EnableStaticTileDB.IsPresent) {
    Write-Warning "-EnableStaticTileDB is deprecated and will be removed in a future version. Use -Linkage static instead."
    $BuildSharedLibs = "OFF"
    if ($Linkage -eq "shared") {
        Write-Error "Cannot specify -EnableStaticTileDB alongside -Linkage shared."
        exit 1
    }
}

$TileDBTools = "OFF";
if ($EnableTools.IsPresent) {
    $TileDBTools = "ON"
}

$TileDBExperimentalFeatures = "OFF"
if ($EnableExperimentalFeatures.IsPresent) {
    $TileDBExperimentalFeatures = "ON"
}

if ($EnableBuildDeps.IsPresent) {
    Write-Warning "-EnableBuildDeps has no effect and will be removed in a future version. Vcpkg builds all dependencies by default, please consult the guide in doc/dev/BUILD.md or vcpkg's documentation to see how to provide your own dependencies."
}

$ArrowTests="OFF"
if ($EnableArrowTests.IsPresent) {
    $ArrowTests="ON"
}

$ConfigureS3="OFF"
if ($EnableAwsS3Config.IsPresent) {
    $ConfigureS3="ON"
}

# Set TileDB prefix
$InstallPrefix = $DefaultPrefix
if (![string]::IsNullOrEmpty($Prefix)) {
    $InstallPrefix = $Prefix
}

# Set TileDB dependency directory string
$DependencyDir = $DefaultDependency
if (![string]::IsNullOrEmpty($Dependency)) {
    $DependencyDir = $Dependency
}

$ArchFlag = ""
if ($PSBoundParameters.ContainsKey("Architecture")) {
    $ArchFlag = "-A $Architecture"
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
if ($null -eq (Get-Command "cmake.exe" -ErrorAction SilentlyContinue)) {
    Throw "Unable to find cmake.exe in your PATH."
}
if ($CMakeGenerator -eq $null) {
    Throw "Could not identify a generator for CMake. Do you have Visual Studio installed?"
}

# Run CMake.
# We use Invoke-Expression so we can echo the command to the user.
$CommandString = "cmake $ArchFlag -DCMAKE_BUILD_TYPE=$BuildType -DCMAKE_INSTALL_PREFIX=""$InstallPrefix"" $VcpkgBaseTriplet -DCMAKE_PREFIX_PATH=""$DependencyDir"" -DMSVC_MP_FLAG=""/MP$BuildProcesses"" -DTILEDB_ASSERTIONS=$AssertionMode -DTILEDB_VERBOSE=$Verbosity -DTILEDB_AZURE=$UseAzure -DTILEDB_S3=$UseS3 -DTILEDB_GCS=$UseGcs -DTILEDB_SERIALIZATION=$UseSerialization -DTILEDB_WERROR=$Werror -DTILEDB_CPP_API=$CppApi -DTILEDB_TESTS=$Tests -DTILEDB_STATS=$Stats -DBUILD_SHARED_LIBS=$BuildSharedLibs -DTILEDB_REMOVE_DEPRECATIONS=$_RemoveDeprecations -DTILEDB_TOOLS=$TileDBTools -DTILEDB_EXPERIMENTAL_FEATURES=$TileDBExperimentalFeatures -DTILEDB_WEBP=$BuildWebP -DTILEDB_RUST=$BuildRust -DTILEDB_CRC32=$BuildCrc32 -DTILEDB_ARROW_TESTS=$ArrowTests -DTILEDB_TESTS_AWS_S3_CONFIG=$ConfigureS3 $GeneratorFlag ""$SourceDirectory"""
Write-Host $CommandString
Write-Host
Invoke-Expression "$CommandString"

Write-Host "Bootstrap success. Run 'cmake --build . --config $BuildType' to build and 'cmake --build . --target check --config $BuildType' to test."
