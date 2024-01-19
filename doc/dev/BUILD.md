---
title: Building TileDB from source
---

## Prerequisites

* CMake 3.21 or greater
* C++ compiler with C++20 support
    * The minimum compiler versions TileDB is being tested in CI are:
        * MSVC from Visual Studio 2019 16.11
        * GCC 10.3 on Linux and Windows via MinGW
        * Apple Clang 14
* Git (required by vcpkg)
* curl (required by vcpkg on non-Windows)

## Downloading the source code

Begin by downloading a release tarball or by cloning the TileDB GitHub repo and checking out a release tag. In the following script `<version>` is the version you wish to use (e.g., `2.18.0`)

```
git clone https://github.com/TileDB-Inc/TileDB.git
cd TileDB
git checkout <version>
```

## Configuring the build tree

### Configuring with the bootstrap scripts

TileDB provides bootstrap scripts to allow easier configuration of a build. The bootstrap script on macOS and Linux is located in [bootstrap](../../bootstrap), while on Windows it is located in [bootstrap.ps1](../../bootstrap.ps1) and runs on [PowerShell](https://learn.microsoft.com/en-us/powershell/).

You can use the bootstrap scripts like this:

**macOS and Linux**

```bash
mkdir build
cd build
# You can see a list of all available options by running ../bootstrap --help
../bootstrap <flags>
```

**Windows**

```powershell
mkdir build
cd build
# You can see a list of all available options by running Get-Help ..\bootstrap.ps1 -Detailed
..\bootstrap.ps1 <flags>
```

### Configuring with CMake

Alternatively, you can configure the build by directly running CMake. The following example works in all operating systems:

```bash
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -GNinja -DTILEDB_S3=ON
```

### Configuration options

The following are the most common configuration options:

|macOS/Linux flag|Windows flag|CMake variable|Description|
|----------------|------------|--------------|-----------|
|`--prefix=PREFIX`|`-Prefix=PREFIX`|`CMAKE_INSTALL_PREFIX=<PREFIX>`|Install files in tree rooted at `PREFIX` (defaults to `TileDB/dist`)|
|`--linkage=shared/static`|`-Linkage=shared/static`|`BUILD_SHARED_LIBS=ON/OFF`|Linkage of the compiled TileDB library (defaults to `shared`) |
|`--remove-deprecations`|`-RemoveDeprecations`|`TILEDB_REMOVE_DEPRECATIONS=ON`|Build TileDB without deprecated APIs|
|`--enable-debug`|`-EnableDebug`|`CMAKE_BUILD_TYPE=Debug`|Enables debug build|
|`--enable-release-symbols`|`-EnableReleaseSymbols`|`CMAKE_BUILD_TYPE=RelWithDebInfo`|Enables release build with debug symbols|
|`--enable-coverage`|`-EnableCoverage`|`CMAKE_BUILD_TYPE=Coverage`|Enables build with code coverage support|
|`--enable-verbose`|`-EnableVerbose`|`TILEDB_VERBOSE=ON`|Enables verbose status messages|
|`--enable-hdfs`|_(HDFS is unsupported on Windows)_|`TILEDB_HDFS=ON`|Enables building with HDFS storage backend support|
|`--enable-s3`|`-EnableS3`|`TILEDB_S3=ON`|Enables building with S3 storage backend support|
|`--enable-azure`|`-EnableAzure`|`TILEDB_AZURE=ON`|Enables building with Azure Blob Storage backend support|
|`--enable-gcs`|`-EnableGcs`|`TILEDB_GCS=ON`|Enables building with Google Cloud Storage backend support|
|`--enable-serialization`|`-EnableSerialization`|`TILEDB_SERIALIZATION=ON`|Enables building with Serialization and TileDB Cloud support|
|`--disable-avx2`|_(Unavailable)_|`COMPILER_SUPPORTS_AVX2=FALSE`|Disables use of the AVX2 instruction set|
|`--disable-webp`|`-DisableWebp`|`TILEDB_WEBP=OFF`|Disables building with support for the WebP filter|
|`--disable-werror`|`-DisableWerror`|`TILEDB_WERROR=OFF`|Disables building with the `-Werror` flag|
|`--disable-cpp-api`|`-DisableCppApi`|`TILEDB_CPP_API=OFF`|Disables building the TileDB C++ API|
|`--disable-stats`|`-DisableStats`|`TILEDB_STATS=OFF`|Disables internal TileDB statistics|
|`--disable-tests`|`-DisableTests`|`TILEDB_TESTS=OFF`|Disables building the TileDB test suite|

> [!TIP]
> You can see all TileDB-specific CMake variables in [BuildOptions.cmake](../../cmake/Options/BuildOptions.cmake).

## Building and installing

Once the build tree has been configured, you can build and install TileDB by running the following commands from the build tree:

```bash
cmake --build . -j <num-procs>
cmake --build . --target install-tiledb
```

If you are building with a multi-config generator (e.g., Visual Studio), you will have to specify the configuration to build with the `--config` option. Also you can directly invoke the underlying build tool:

```bash
make -j4
make install-tiledb
```

The following are the most important targets:

|Target|Description|
|------|-----------|
|`install-tiledb`|Installs the TileDB library and headers.|
|`check`|Builds and runs all TileDB tests.|
|`examples`|Builds all TileDB examples.|

## Advanced

### Dependency management with vcpkg

TileDB uses [vcpkg](https://vcpkg.io) to download and build its dependencies. By default, the vcpkg repository will be cloned automatically when configuring the build tree. To avoid repeatedly cloning the repository, and take full advantage of vcpkg's [binary caching](https://learn.microsoft.com/en-us/vcpkg/users/binarycaching) feature, you are recommended to clone the vcpkg repository manually and set the `VCPKG_ROOT` environment variable on your machine to the path of the repository.

> [!NOTE]
> If you have set `VCPKG_ROOT` on your machine and encountered errors like `error: no version database entry for <port> at <version>` it means that your vcpkg repository is out of date. You can update it by running `git pull` in the repository directory.

Vcpkg will not be automatically downloaded if:

* The `TILEDB_DISABLE_AUTO_VCPKG` environment variable has been defined.
* The build tree has been configured by directly calling CMake and the `CMAKE_TOOLCHAIN_FILE` variable has been set by the user.

In these cases no dependencies CMake will find the dependencies based on the rules of the [`find_package`](https://cmake.org/cmake/help/latest/command/find_package.html#command:find_package) command. The user is responsible for providing them.
