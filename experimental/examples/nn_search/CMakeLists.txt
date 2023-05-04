#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2023 TileDB, Inc.
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

cmake_minimum_required(VERSION 3.22)

# The cmake options don't seem to really work
if ("${CMAKE_BINARY_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
    message(FATAL_ERROR "In-source builds are disabled.
    Please create a subfolder and use `cmake ..` inside it.
    IMPORTANT: cmake will still have created CMakeCache.txt and CMakeFiles/*.
               You must delete them, or cmake will refuse to work.")
endif() # yes, end-markers and even else() need empty parens

project(tdb)

set(CMAKE_CXX_STANDARD 20)

# -----------------------------------------------------------------------------
# Extend the module path so we can find our custom modules
# -----------------------------------------------------------------------------
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

# -----------------------------------------------------------------------------
# Build types and default flags
# -----------------------------------------------------------------------------

message("Architecture is ${CMAKE_HOST_SYSTEM_PROCESSOR}")
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(FCONCEPTS_DIAGNOSTICS_DEPTH "-fconcepts-diagnostics-depth=3")
endif()

if (CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL arm64)
    set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fno-elide-constructors ${FCONCEPTS_DIAGNOSTICS_DEPTH} " CACHE STRING "" FORCE)
    set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -DNDEBUG " CACHE STRING "" FORCE)
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-Ofast -g -DNDEBUG" CACHE STRING "" FORCE)
    set(CMAKE_CXX_FLAGS_MINSIZEREL "-Os -DNDEBUG " CACHE STRING "" FORCE)
else()
    set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fno-elide-constructors ${FCONCEPTS_DIAGNOSTICS_DEPTH} " CACHE STRING "" FORCE)
    set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -march=native -DNDEBUG " CACHE STRING "" FORCE)
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-Ofast -g -march=native -DNDEBUG" CACHE STRING "" FORCE)
    set(CMAKE_CXX_FLAGS_MINSIZEREL "-Os -march=native -DNDEBUG " CACHE STRING "" FORCE)
endif()

# Default to Release build
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "No build type selected, default to Release")
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type (default Release)" FORCE)
endif()

# Control compiler-specific flags.
include(CompilerFlags)

# -----------------------------------------------------------------------------
# Interface library for multi-threading
# -----------------------------------------------------------------------------
# Control compiler-specific flags.
include(MultiThreading)

# -----------------------------------------------------------------------------
# Interface library for BLAS
# -----------------------------------------------------------------------------
# Control compiler-specific flags.
include(BLAS)




if (DEFINED TileDB_DIR)
    set(CMAKE_PREFIX_PATH "${TileDB_DIR};{CMAKE_PREFIX_PATH}")
    message(STATUS "TileDB_DIR is set to ${TileDB_DIR} -- find_package will search there first.")
else()
    message(STATUS "TileDB_DIR is not set -- find_package will use default search path.")
endif()

find_package(TileDB REQUIRED)

if (TileDB_FOUND)
    message(STATUS "Found TileDB")
    get_target_property(_TDB_INCLUDES TileDB::tiledb_shared INTERFACE_INCLUDE_DIRECTORIES)
    message(STATUS "TileDB include directories are ${_TDB_INCLUDES}")
endif()

include(Docopt)

include(mdspan)

add_subdirectory(src)
add_subdirectory(src/test)
