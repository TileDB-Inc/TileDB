#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2016 MIT and Intel Corporation
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

project(TileDB)
cmake_minimum_required(VERSION 2.8)

# Set the cmake Module path
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# Set shared library suffix
if(APPLE)
  set(SHARED_LIB_SUFFIX ".dylib")
else()
  set(SHARED_LIB_SUFFIX ".so")
endif()

# Only for Mac OS X warnings
if(APPLE)
  set(CMAKE_MACOSX_RPATH 1)
endif()

# Default user definitions
set(USE_MPI False CACHE BOOL "Enables MPI")
if(APPLE)
  set(USE_OPENMP False CACHE BOOL "Enables OpenMP")
else()
  set(USE_OPENMP True CACHE BOOL "Enables OpenMP")
endif()
set(TILEDB_VERBOSE False CACHE BOOL "Prints TileDB errors with verbosity")
set(MAC_ADDRESS_INTERFACE "" 
    CACHE STRING "The interface carrying the MAC address of the machine."
)
set(USE_PARALLEL_SORT False CACHE BOOL "Enables parallel sorting.")
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Find required library dependencies
find_package(ZLIB REQUIRED)
include_directories(${ZLIB_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${ZLIB_LIBRARIES})
find_package(LZ4 REQUIRED)
include_directories(${LZ4_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${LZ4_LIBRARIES})
find_package(BLOSC REQUIRED)
include_directories(${BLOSC_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${BLOSC_LIBRARIES})
find_package(ZSTD REQUIRED)
include_directories(${ZSTD_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${ZSTD_LIBRARIES})
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${OPENSSL_LIBRARIES})
find_package(Bzip2 REQUIRED)
include_directories(${BZIP2_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${BZIP2_LIBRARIES})

# Find optional library dependencies
if(USE_MPI)
  find_package(MPI REQUIRED)
  include_directories(${MPI_CXX_INCLUDE_PATH})
  set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${MPI_CXX_LIBRARIES})
endif()
if(USE_OPENMP AND NOT APPLE)
  find_package(OpenMP REQUIRED)
endif()

# Set C++ 2011 flag
include(SetCXX2011Flag)

# Set compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG -g3 -gdwarf-3 -Wall -fvisibility=hidden")
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3 -fvisibility=hidden")
set(CMAKE_CXX_FLAGS_COVERAGE "-DDEBUG -g3 -gdwarf-3 --coverage")
if(NOT APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-literal-suffix")
endif()
if(USE_MPI)
  set(CMAKE_CXX_COMPILER ${MPI_CXX_COMPILER})
endif()
if(USE_OPENMP AND NOT APPLE)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

# Add definitions
add_definitions(-D_FILE_OFFSET_BITS=64)
if(USE_MPI)
  add_definitions(-DHAVE_MPI)
endif()
if(USE_OPENMP AND NOT APPLE)
  add_definitions(-DHAVE_OPENMP)
endif()
if(TILEDB_VERBOSE)
  add_definitions(-DTILEDB_VERBOSE)
  message(STATUS "The TileDB library is compiled with verbosity.")
endif()
if(MAC_ADDRESS_INTERFACE)
  add_definitions(-DTILEDB_MAC_ADDRESS_INTERFACE=${MAC_ADDRESS_INTERFACE})
  message(STATUS "Set MAC address interface to ${MAC_ADDRESS_INTERFACE}.")
endif()
if(USE_PARALLEL_SORT)
  add_definitions(-DUSE_PARALLEL_SORT)
  message(STATUS "Will use parallel sort.")
endif()

# Enable testing
enable_testing()

# Build TileDB library
add_subdirectory(core)

# Build examples
add_subdirectory(examples)

# Build unit tests
add_subdirectory(test)

############################################################
# "make format" and "make check-format" targets
############################################################
set(SCRIPTS_DIR "${CMAKE_SOURCE_DIR}/scripts")

find_package(ClangTools)
if (${CLANG_FORMAT_FOUND})
  # runs clang format and updates files in place.
  add_custom_target(format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 1
    `find ${CMAKE_CURRENT_SOURCE_DIR}/core/include
          ${CMAKE_CURRENT_SOURCE_DIR}/core/src
          ${CMAKE_CURRENT_SOURCE_DIR}/test/src -name \\*.cc -or -name \\*.h`)

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 0
    `find ${CMAKE_CURRENT_SOURCE_DIR}/core/include
          ${CMAKE_CURRENT_SOURCE_DIR}/core/src
          ${CMAKE_CURRENT_SOURCE_DIR}/test/src -name \\*.cc -or -name \\*.h`)
endif()

###########################################################
# Doxygen documentation
###########################################################
find_package(Doxygen)
if(DOXYGEN_FOUND)
  file(GLOB_RECURSE TILEDB_CORE_HEADERS "core/include/*.h")
  add_custom_command(
     OUTPUT ${CMAKE_BINARY_DIR}/doxyfile.in
      COMMAND mkdir -p doxygen
      COMMAND echo INPUT = ${CMAKE_SOURCE_DIR}/doc/mainpage.dox 
              ${TILEDB_CORE_HEADERS} > ${CMAKE_BINARY_DIR}/doxyfile.in
      COMMAND echo FILE_PATTERNS = *.h >> ${CMAKE_BINARY_DIR}/doxyfile.in
      COMMENT "Preparing for Doxygen documentation" VERBATIM
  )
  add_custom_target(
      doc ${DOXYGEN_EXECUTABLE} ${CMAKE_SOURCE_DIR}/doc/Doxyfile.mk > 
          ${CMAKE_BINARY_DIR}/Doxyfile.log 2>&1
      COMMENT "Generating API documentation with Doxygen" VERBATIM
      DEPENDS ${CMAKE_BINARY_DIR}/doxyfile.in
  )
endif(DOXYGEN_FOUND)

###########################################################
# Uninstall
###########################################################
set(CMD "xargs printf '-- Uninstalling: %s\\\\n' <install_manifest.txt") 
add_custom_target(
   uninstall 
   COMMAND echo "Uninstalling TileDB..."
   COMMAND eval "${CMD}"
   COMMAND xargs rm -f < install_manifest.txt
   COMMAND echo "TileDB uninstalled"
)
