#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2018-2021 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

cmake_minimum_required(VERSION 3.21)
project(TileDBPngIngestion)

# Set C++17 as required standard for all C++ targets (required to use the TileDB
# C++ API).
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find TileDB.
#
# If TileDB is not installed globally on your system, either set
# CMAKE_PREFIX_PATH on the CMake command line:
#   $ cmake -DCMAKE_PREFIX_PATH=/path/to/TileDB-installation ..
# or you can hardcode it here e.g.
#   list(APPEND CMAKE_PREFIX_PATH "/path/to/TileDB-installation")
find_package(TileDB 
  ${TILEDB_VERSION} 
  REQUIRED
  )

# Set up the example program.
add_executable(tiledb_png "src/main.cc")

# Link the example program with the TileDB shared library.
# This also configures include paths to find the TileDB headers.
target_link_libraries(tiledb_png TileDB::tiledb)

# Find and link with libpng.
find_package(ZLIB REQUIRED)
find_package(PNG REQUIRED)
target_link_libraries(tiledb_png "${PNG_LIBRARIES}")
target_include_directories(tiledb_png PRIVATE "${PNG_INCLUDE_DIRS}")
target_compile_definitions(tiledb_png PRIVATE "${PNG_DEFINITIONS}")
