#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017-2018 TileDB, Inc.
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

# Default user definitions
set(USE_HDFS False CACHE BOOL "Enables HDFS support using the official Hadoop JNI bindings")
set(USE_S3 False CACHE BOOL "Enables S3/minio support using aws-cpp-sdk")
set(TILEDB_VERBOSE False CACHE BOOL "Prints TileDB errors with verbosity")
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Only for Mac OS X warnings
if(APPLE)
  set(CMAKE_MACOSX_RPATH 1)
endif()

set(TILEDB_LIB_DEPENDENCIES)

# Find required library dependencies
find_package(ZLIB REQUIRED)
include_directories(${ZLIB_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${ZLIB_LIBRARIES})
find_package(LZ4 REQUIRED)
include_directories(${LZ4_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${LZ4_LIBRARIES})
find_package(BLOSC REQUIRED)
include_directories(${BLOSC_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${BLOSC_LIBRARIES})
find_package(ZSTD REQUIRED)
include_directories(${ZSTD_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${ZSTD_LIBRARIES})
find_package(Bzip2 REQUIRED)
include_directories(${BZIP2_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${BZIP2_LIBRARIES})

# On Windows, set a variable for the dependency install location.
if (WIN32)
  set(TILEDB_DEPENDENCY_INSTALL_DIR "${CMAKE_SOURCE_DIR}/deps-install")
endif()

# Find optional library dependencies
if(USE_HDFS)
  find_package(LIBJVM REQUIRED)
  set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${LIBJVM_LIBRARIES})
  
  find_package(LIBHDFS REQUIRED)
  include_directories(${LIBHDFS_INCLUDE_DIR})
  # This variable is defined in FindJNI module included in the FindHDFS.cmake file
  set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${LIBHDFS_LIBRARY})
endif()

if(USE_S3)  
  cmake_minimum_required(VERSION 3.2)
  find_package(AWSSDK)

  message(STATUS "AWSSDK_LIB_DIR: ${AWSSDK_LIB_DIR}")
  message(STATUS "AWSSDK_INC_DIR: ${AWSSDK_INCLUDE_DIR}")
  message(STATUS "AWSSDK_BIN_DIR: ${AWSSDK_BIN_DIR}")
 
  set(AWS_SERVICE s3)
  AWSSDK_LIB_DEPS(AWS_SERVICE AWS_DEPS)
 
  message(STATUS "AWS service ${AWS_SERVICE} depends on libs: ${AWS_DEPS}")
  AWSSDK_DETERMINE_LIBS_TO_LINK(AWS_SERVICE AWS_LINKED_LIBS)
  message(STATUS "AWS linked libs: ${AWS_LINKED_LIBS}")
  
  set(AWS_LIBRARIES)
  foreach(LIB ${AWS_LINKED_LIBS})
    find_library("AWS_FOUND_${LIB}" NAMES ${LIB} PATHS ${AWSSDK_LIB_DIR} NO_DEFAULT_PATH)
    message(STATUS "AWS LIB PATH: ${LIB} ${AWS_FOUND_${LIB}}")
    set(AWS_LIBRARIES ${AWS_LIBRARIES} ${AWS_FOUND_${LIB}})
  endforeach()

  if (APPLE)
    add_definitions(-DUNDER_MACOS)
  endif()

  if (WIN32)
    add_definitions(-DUSE_IMPORT_EXPORT)
    include_directories(${AWSSDK_INCLUDE_DIR})
    set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${AWS_LIBRARIES})
  else()
    message(STATUS "AWS_DEPS_LIBRARIES: ${AWS_DEPS_LIBRARIES}")
    message(STATUS "AWS_DEPS_INCLUDE_DIRS: ${AWS_DEPS_INCLUDE_DIRS}")
    message(STATUS "AWS_DEPS_CFLAGS_OTHER: ${AWS_DEPS_CFLAGS_OTHER}")
    include_directories(${AWS_DEPS_INCLUDE_DIRS})
    set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${AWS_LIBRARIES})
  endif()
endif()

# Set C++ 2011 flag
include(SetCXX2011Flag REQUIRED)

# Set compiler flags
if (WIN32)
  # We disable some warnings that are not present in gcc/clang -Wall:
  #   C4101: unreferenced local variable
  #   C4244: conversion warning of floating point to integer type.
  #   C4456: local variable hiding previous local variable
  #   C4457: local variable hiding function parameter
  #   C4702: unreachable code
  #   C4996: deprecation warning about e.g. sscanf.
  set(MSVC_DISABLE_WARNINGS "/wd4101 /wd4244 /wd4456 /wd4457 /wd4702 /wd4996")
  # Note: MSVC has the behavior of -fvisibility=hidden by default (you must
  # explicitly export all symbols you want exported).
  set(CMAKE_CXX_FLAGS_DEBUG "/DDEBUG /Od /Zi /W4 /WX ${MSVC_DISABLE_WARNINGS}")
  set(CMAKE_CXX_FLAGS_RELEASE "/DNDEBUG /Ox /W4 /WX ${MSVC_DISABLE_WARNINGS}")
  # Disable GDI (which we don't need, and causes some macro
  # re-definition issues if wingdi.h is included)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /DNOGDI")
  # Add /MPn flag from CMake invocation (if defined).
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MSVC_MP_FLAG}")
else()
  set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG -O0 -g3 -ggdb3 -gdwarf-3 -fvisibility=hidden -Werror -Wall -Wextra")
  set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3 -fvisibility=hidden -Werror -Wall -Wextra")
  set(CMAKE_CXX_FLAGS_COVERAGE "-DDEBUG -g3 -gdwarf-3 --coverage -Werror -Wall -Wextra")

  if(NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-literal-suffix")
  endif()
endif()

# Enable sanitizers only on clang for now
if(SANITIZER)
  if(NOT CMAKE_BUILD_TYPE MATCHES "Debug")
    message(FATAL_ERROR "Sanitizers only enabled for Debug build")
  endif()
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    string(TOLOWER ${SANITIZER} SANITIZER)
    if(NOT SANITIZER MATCHES "^(address|memory|thread|undefined)$")
      message(FATAL_ERROR "Unknown clang sanitizer: ${SANITIZER})")
    else()
      message(STATUS "The TileDB library is compiled with sanitizer ${SANITIZER} enabled")
    endif()
    set(_SANITIZER_COMPILE_OPTIONS "-g -fno-omit-frame-pointer -fno-optimize-sibling-calls")
    set(_SANITIZER_COMPILE_FLAG "-fsanitize=${SANITIZER}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${_SANITIZER_COMPILE_OPTIONS} ${_SANITIZER_COMPILE_FLAG}" )
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${_SANITIZER_COMPILE_FLAG}")
  else()
    message(WARNING "Sanitizers only enabled for the clang compiler")
  endif()
endif()


# Add definitions
add_definitions(-D_FILE_OFFSET_BITS=64)
if(USE_HDFS)
  add_definitions(-DHAVE_HDFS)
  message(STATUS "The TileDB library is compiled with HDFS support.")
endif()
if(USE_S3)
  add_definitions(-DHAVE_S3)
  message(STATUS "The TileDB library is compiled with S3 support.")
endif()
if(TILEDB_VERBOSE)
  add_definitions(-DTILEDB_VERBOSE)
  message(STATUS "The TileDB library is compiled with verbosity.")
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
          ${CMAKE_CURRENT_SOURCE_DIR}/test/src
          ${CMAKE_CURRENT_SOURCE_DIR}/examples/src
          -name \\*.cc -or -name \\*.c -or -name \\*.h`)

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 0
    `find ${CMAKE_CURRENT_SOURCE_DIR}/core/include
          ${CMAKE_CURRENT_SOURCE_DIR}/core/src
          ${CMAKE_CURRENT_SOURCE_DIR}/test/src
          ${CMAKE_CURRENT_SOURCE_DIR}/examples/src
          -name \\*.cc -or -name \\*.c -or -name \\*.h`)
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
