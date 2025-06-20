# Building from source

## macOS and Linux

Begin by downloading a [release tarball](https://github.com/TileDB-Inc/TileDB/releases) or by cloning this repo and checking out a release tag (where `<version>` is the version you wish to use, such as `2.27.2`):

```bash
$ git clone https://github.com/TileDB-Inc/TileDB.git
$ cd TileDB
$ git checkout <version>
```

To **configure** TileDB, use the `bootstrap` script:

```bash
$ mkdir build
$ cd build
$ ../bootstrap <flags>
$ # Or use CMake directly instead of bootstrap:
$ # cmake <flags> ..
```

The flags for the bootstrap script and the CMake equivalents are as follows:

| **Flag**                 | **Description**                                                      | **CMake Equivalent**            |
| ------------------------ | -------------------------------------------------------------------- | ------------------------------- |
| `--help`                 | Prints command line flag options                                     | N/A                             |
| `--prefix=PREFIX`        | Install files in tree rooted at `PREFIX` (defaults to `TileDB/dist`) | `CMAKE_INSTALL_PREFIX=<PREFIX>` |
| `--dependency=DIRs`      | Colon separated list to binary dependencies                          | `CMAKE_PREFIX_PATH=<DIRs>`      |
| `--enable-debug`         | Enable debug build                                                   | `CMAKE_BUILD_TYPE=Debug`        |
| `--enable-coverage`      | Enable build with code coverage support                              | `CMAKE_BUILD_TYPE=Coverage`     |
| `--enable-verbose`       | Enable verbose status messages                                       | `TILEDB_VERBOSE=ON`             |
| `--enable-assertions`    | Enable build with assertions enabled. Always on for debug builds.    | `TILEDB_ASSERTIONS=ON`          |
| `--enable-hdfs`          | Enables building with HDFS storage backend support                   | `TILEDB_HDFS=ON`                |
| `--enable-s3`            | Enables building with S3 storage backend support                     | `TILEDB_S3=ON`                  |
| `--enable-azure`         | Enables building with Azure Blob Storage backend support             | `TILEDB_AZURE=ON`               |
| `--enable-gcs`           | Enables building with Google Cloud Storage backend support           | `TILEDB_GCS=ON`                 |
| `--enable-serialization` | Enables building with Serialization and TileDB Cloud support         | `TILEDB_SERIALIZATION=ON`       |
| `--enable-static-tiledb` | Enables building TileDB as a static library                          | `TILEDB_STATIC=ON`              |
| `--disable-werror`       | Disables building with the `-Werror` flag                            | `TILEDB_WERROR=OFF`             |
| `--disable-cpp-api`      | Disables building the TileDB C++ API                                 | `TILEDB_CPP_API=OFF`            |
| `--disable-stats`        | Disables internal TileDB statistics                                  | `TILEDB_STATS=OFF`              |
| `--disable-tests`        | Disables building the TileDB test suite                              | `TILEDB_TESTS=OFF`              |

To **build** after configuration, run the generated `make` script:

```bash
$ make -j <nprocs>
```

To **install** to the configured prefix, run the following:

```bash
$ make install
```

Note that building against the installed shared library requires setting the library search path at build-time or run-time, as documented in [Usage](USAGE.md). System-wide installations requiring `sudo` permissions may avoid this step by running `sudo ldconfig` after installation.

Other helpful `Makefile` targets are as follows:

```bash
# Runs the tests
make check

# Builds the examples
make examples
```

## Windows

Building TileDB on Windows has been tested to work with Microsoft Visual Studio 2019 and later. You can install the free [Community Edition](https://www.visualstudio.com/vs/community/) if you’d like the full IDE, or the [Build Tools](https://www.visualstudio.com/downloads/#Other%20Tools%20and%20Frameworks) if you don’t need or want the IDE installed.

During the Visual Studio setup process, make sure the **Git for Windows** component is selected if you do not already have a working Git installation. Also, be sure to select the CMake component if you do not have a working CMake installation.

In addition, you will need to install [PowerShell](https://docs.microsoft.com/en-us/powershell/) (which you can download for free through Microsoft).

To build and install TileDB, first open PowerShell and clone this repository. Then, checkout a release tag (where `<version>` is the version you wish to use, such as `2.27.2`):

```powershell
> git clone https://github.com/TileDB-Inc/TileDB.git
> cd TileDB
> git checkout <version>
```

Next, ensure the CMake binaries are in your path. If you installed Visual Studio, run the following:

```powershell
> $env:Path += ";C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin"
> # If you installed the build tools, run the following instead:
> # $env:Path += ";C:\Program Files (x86)\Microsoft Visual Studio\2017\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin"
```

Create a build directory and **configure** TileDB:

```powershell
> mkdir build
> cd build
> ..\bootstrap.ps1 <flags>
> # Or use CMake directly:
> # cmake <flags> ..
```

The flags for the bootstrap script and the CMake equivalents are as follows:

| **Flag**               | **Description**                                                                                                                      | **CMake Equivalent**            |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------- |
| `-?`                   | Display a usage message.                                                                                                             | n/a                             |
| `-Prefix`              | Install files in tree rooted at `PREFIX` (defaults to `TileDB\dist`)                                                                 | `CMAKE_INSTALL_PREFIX=<PREFIX>` |
| `-Dependency`          | Semicolon separated list to binary dependencies.                                                                                     | `CMAKE_PREFIX_PATH=<DIRs>`      |
| `-CMakeGenerator`      | Optionally specify the CMake generator string, e.g. “Visual Studio 15 2017”. Check ‘cmake –help’ for a list of supported generators. | `-G <generator>`                |
| `-EnableDebug`         | Enable debug build                                                                                                                   | `CMAKE_BUILD_TYPE=Debug`        |
| `-EnableVerbose`       | Enable verbose status messages.                                                                                                      | `TILEDB_VERBOSE=ON`             |
| `-EnableAssert`        | Enables building with assertions enabled. Always on for debug builds.                                                                | `TILEDB_ASSERTIONS=ON`          |
| `-EnableS3`            | Enables building with the S3 storage backend.                                                                                        | `TILEDB_S3=ON`                  |
| `-EnableGcs`           | Enables building the Google Cloud Storage backend                                                                                    | `TILEDB_GCS=ON`                 |
| `-EnableSerialization` | Enabled serialization and TileDB Cloud support                                                                                       | `TILEDB_SERIALZIATION=ON`       |
| `-EnableStaticTileDB`  | Enables building TileDB as a static library                                                                                          | `TILEDB_STATIC=ON`              |
| `-DisableWerror`       | Disables building with the `/WX` flag                                                                                                | `TILEDB_WERROR=OFF`             |
| `-DisableCppApi`       | Disables building the TileDB C++ API                                                                                                 | `TILEDB_CPP_API=OFF`            |
| `-DisableTBB`          | Disables use of TBB for parallelization                                                                                              | `TILEDB_TBB=OFF`                |
| `-DisableStats`        | Disables internal TileDB statistics                                                                                                  | `TILEDB_STATS=OFF`              |
| `-DisableTests`        | Disables building the TileDB test suite                                                                                              | `TILEDB_TESTS=OFF`              |

To **build** after configuration, run the following:

```bash
> cmake --build . --config Release
```

To **install**, run the following:

```bash
> cmake --build . --target install-tiledb --config Release
```

Other helpful build targets are as follows:

```bash
# Runs the tests
cmake --build . --target check --config Release

# Builds the examples
cmake --build . --target examples --config Release
```

**Warning:** If you build `libtiledb` in `Release` mode, make sure to build `check` and `examples` in `Release` mode as well. Similarly, if you build `libtiledb` in `Debug` mode, you need to build `check` and `examples` in `Debug` mode as well. Otherwise, the test and example executables will not run properly.

Should you experience any problem with the build, it is always a good idea to delete the `build` and `dist` directories in your TileDB repo path and restart the process, as `cmake`’s cached state could present some unexpected problems.

## Cygwin

[Cygwin](https://cygwin.com/) is a Unix like environment and command line interface for Microsoft Windows that provides a large collection of GNU/OpenSource tools (including the `gcc` toolchain) and supporting libraries that provide substantial POSIX API functionality. TileDB is able to compile from source in the Cygwin environment if Intel TBB is disabled and some TileDB dependencies are installed as Cygwin packages.

The following Cygwin packages need to be installed:

* `gcc`/`g++`
* `git`
* `cmake`
* `make`
* `lz4-devel`
* `zlib-devel`
* `libzstd-devel (+src)`
* `bzip2 (+src)`
* `openssl-devel`

You can then clone and build TileDB using `git`/`cmake`/`make`:

```bash
$ git clone https://github.com/TileDB-Inc/TileDB.git
$ cd TileDB
$ git checkout <version>
$ mkdir build && cd build
$ cmake -DTILEDB_TBB=OFF ..
$ make
$ make check
```
