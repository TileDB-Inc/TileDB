---
title: CMake
---

TileDB uses CMake for its configuration, build system, and package management.

## Build Options

The `TILEDB_*` options are used primarily for enabling/disabling extra components (e.g. S3 support, test suite) and setting build or compilation settings (e.g. error on warn, superbuild). The options are defined in a single code block in the TileDB root `CMakeLists.txt` file.

If it is decided a new option is needed, the following must be updated in addition to the root `CMakeLists.txt` file:

1. the bootstrap scripts (`bootstrap`, `bootstrap.ps1`)
2. continuous integration coverage to ensure the option is exercised in a CI build (edit github actions workflows under `.github/workflows`)
3. the build documentation contained in our [online docs](../../../docs/building-from-source.md) and the [README](../../../README.md)


## TileDB Object Libraries

The main TileDB library is in the process of being broken into discrete [object libraries](https://cmake.org/cmake/help/latest/manual/cmake-buildsystem.7.html#object-libraries). 

_TODO About the object libraries and best practices_

## TileDB Static and Shared Libraries

_TODO About combining the object libraries into the main libraries created by CMake_

## Adding a dependency

There are several steps to adding a new dependency to TileDB. See [TileDB wiki on GitHub](https://github.com/TileDB-Inc/TileDB/wiki/Adding-a-new-dependency-to-TileDB) for more information.

## Using the common modules

* _TODO  Describe (generally speaking) what is included in the cmake/common*.cmake modules_
* _TODO Guidelines for adding to the modules (including best practices for CMake macros and functions)._


