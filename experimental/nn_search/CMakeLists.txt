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

add_library(flat_tbb INTERFACE)
target_compile_definitions(flat_tbb INTERFACE EXECUTION_POLICY)

find_package(Threads REQUIRED)
target_link_libraries(flat_tbb INTERFACE Threads::Threads)

find_package(TBB REQUIRED)
target_link_libraries(flat_tbb INTERFACE TBB::tbb)

# find_package(OpenMP REQUIRED)
# include(FindOpenMP)
# target_link_libraries(flat_tbb INTERFACE OpenMP::OpenMP_CXX)

add_library(flat_blas INTERFACE)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # include macOS-specific flags for native BLAS
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -framework Accelerate")
else()
    # include flags for other operating systems BLAS
    # find_package(MKL CONFIG REQUIRED)
    find_package(MKL REQUIRED)
    include_directories(${MKL_INCLUDE_DIRS})
    # target_link_libraries(${PROJECT_NAME} ${MKL_LIBRARIES})

    target_compile_options(flat_blas INTERFACE $<TARGET_PROPERTY:MKL::MKL,INTERFACE_COMPILE_OPTIONS>)
    target_include_directories(flat_blas INTERFACE $<TARGET_PROPERTY:MKL::MKL,INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(flat_blas INTERFACE $<LINK_ONLY:MKL::MKL>)

endif()

include(Docopt)

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

add_subdirectory(src)
add_subdirectory(src/test)
