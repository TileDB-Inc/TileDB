#
# test/performanace/CMakeLists.txt (derived from test/CMakeLists.txt)
#
#
# The MIT License
#
# Copyright (c) 2017-2022 TileDB, Inc.
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

# These options not exposed in bootstrap script.
option(TILEDB_TESTS_AWS_S3_CONFIG "Use an S3 config appropriate for AWS in tests" OFF)

# Include TileDB core header directories
set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../..")

# Include the C API directory so that the C++ 'tiledb' file can directly
# include "tiledb.h".
list(APPEND TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../tiledb/sm/c_api")

message(STATUS "CMAKE_CURRENT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}")
list(APPEND TILEDB_MSYS_HANDLE_LEAKAGE_SOURCES
  "msys_handle_leakage/unit-cppapi-consolidation-with-timestamps.cc"
)

if (TILEDB_VERBOSE)
  add_definitions(-DTILEDB_VERBOSE)
endif()


# unit test executable
add_executable(
  tiledb_explore_msys_handle_leakage EXCLUDE_FROM_ALL
  $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>
  ${TILEDB_MSYS_HANDLE_LEAKAGE_SOURCES}
  "msys_handle_leakage/unit.cc"
)

add_dependencies(tiledb_explore_msys_handle_leakage tiledb_test_support_lib)

# We want tests to continue as normal even as the API is changing,
# so don't warn for deprecations, since they'll be escalated to errors.
if (NOT MSVC)
  target_compile_options(tiledb_unit PRIVATE -Wno-deprecated-declarations)
  target_compile_options(tiledb_test_support_lib PRIVATE -Wno-deprecated-declarations)
endif()

target_include_directories(
  tiledb_explore_msys_handle_leakage BEFORE PRIVATE
    ${TILEDB_CORE_INCLUDE_DIR}
    ${TILEDB_EXPORT_HEADER_DIR}
)

target_link_libraries(tiledb_explore_msys_handle_leakage
  PUBLIC
    TILEDB_CORE_OBJECTS_ILIB
    tiledb_Catch2WithMain
    tiledb_test_support_lib
)

# need to properly link the signal chaining library for catch to work with the jni hdfs interface
# see: https://docs.oracle.com/javase/9/vm/signal-chaining.htm


# This is necessary only because we are linking directly to the core objects.
# Other users (e.g. the examples) do not need this flag.
target_compile_definitions(tiledb_explore_msys_handle_leakage PRIVATE -Dtiledb_EXPORTS)

target_compile_definitions(tiledb_explore_msys_handle_leakage PRIVATE
  -DTILEDB_TEST_INPUTS_DIR="${CMAKE_CURRENT_SOURCE_DIR}/inputs"
)

# Linking dl is only needed on linux with gcc
if (${CMAKE_SYSTEM_NAME} MATCHES "Linux" AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set_target_properties(tiledb_explore_msys_handle_leakage PROPERTIES
      LINK_FLAGS "-Wl,--no-as-needed -ldl"
    )
endif()
