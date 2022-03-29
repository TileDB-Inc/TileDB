# TileDB Example CMake Usage

This directory contains a minimal example of how to add TileDB as a dependency to your project using CMake.

## Instructions

Follow these instructions to build the example project.

### macOS and Linux

First, build and install TileDB locally:

    $ cd TileDB
    $ mkdir build
    $ cd build
    $ ../bootstrap && make && make install

Now you can build the example project. Note that we are passing the path to the TileDB installation in `CMAKE_PREFIX_PATH`:

    $ cd TileDB/examples/cmake_project
    $ mkdir build
    $ cd build
    $ cmake -DCMAKE_PREFIX_PATH=/path/to/TileDB/dist .. && make

This creates the `ExampleExe` binary in the build directory. You should be able to execute it directly, e.g.:

    $ ./ExampleExe
    You are using TileDB version 1.3.0

### Windows

In PowerShell, first build and install TileDB locally:

    > cd TileDB
    > mkdir build
    > cd build
    > ..\bootstrap.ps1 
    > cmake --build . --config Release
    > cd tiledb
    > cmake --build . --config Release --target install

Now you can build the example project. Note that we are passing the path to the TileDB installation in `CMAKE_PREFIX_PATH`:

    > cd TileDB\examples\cmake_project
    > mkdir build
    > cd build
    > cmake -A X64 -DCMAKE_PREFIX_PATH=\path\to\TileDB\dist ..
    > cmake --build . --config Release

This creates the `ExampleExe` binary in the build directory. You should be able to execute it directly, e.g.:

    > .\ExampleExe.exe
    You are using TileDB version 1.3.0

Note: on Windows, you must explicitly pass the additional CMake argument `-A X64` to specify building the example for 64-bit architectures. This is because the TileDB library is itself only offered in 64-bit.

## Files

- `CMakeLists.txt`: The top-level CMake list file of the example project.
- `src/main.cc`: The source file for the example executable.
