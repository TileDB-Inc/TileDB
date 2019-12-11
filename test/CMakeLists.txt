#
# test/CMakeLists.txt
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

find_package(Catch_EP REQUIRED)

# These options not exposed in bootstrap script.
option(TILEDB_TESTS_AWS_S3_CONFIG "Use an S3 config appropriate for AWS in tests" OFF)
option(TILEDB_TESTS_ENABLE_REST "Enables REST tests (requires running REST server)" OFF)

# Include TileDB core header directories
set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")
# Include the C API directory so that the C++ 'tiledb' file can directly
# include "tiledb.h".
list(APPEND TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../tiledb/sm/c_api")

# Gather the test source files
set(TILEDB_TEST_SOURCES
  src/helpers.h
  src/helpers.cc
  src/unit-backwards_compat.cc
  src/unit-buffer.cc
  src/unit-bufferlist.cc
  src/unit-capi-any.cc
  src/unit-capi-array_schema.cc
  src/unit-capi-async.cc
  src/unit-capi-array.cc
  src/unit-capi-buffer.cc
  src/unit-capi-config.cc
  src/unit-capi-consolidation.cc
  src/unit-capi-dense_array.cc
  src/unit-capi-dense_array_2.cc
  src/unit-capi-dense_neg.cc
  src/unit-capi-dense_vector.cc
  src/unit-capi-enum_values.cc
  src/unit-capi-error.cc
  src/unit-capi-filter.cc
  src/unit-capi-incomplete.cc
  src/unit-capi-incomplete-2.cc
  src/unit-capi-metadata.cc
  src/unit-capi-object_mgmt.cc
  src/unit-capi-query.cc
  src/unit-capi-query_2.cc
  src/unit-capi-sparse_array.cc
  src/unit-capi-sparse_neg.cc
  src/unit-capi-sparse_neg_2.cc
  src/unit-capi-sparse_real.cc
  src/unit-capi-stats.cc
  src/unit-capi-sparse_real_2.cc
  src/unit-capi-string.cc
  src/unit-capi-uri.cc
  src/unit-capi-version.cc
  src/unit-capi-vfs.cc
  src/unit-CellSlabIter.cc
  src/unit-compression-dd.cc
  src/unit-compression-rle.cc
  src/unit-ctx.cc
  src/unit-encryption.cc
  src/unit-filter-buffer.cc
  src/unit-filter-pipeline.cc
  src/unit-hdfs-filesystem.cc
  src/unit-lru_cache.cc
  src/unit-Reader.cc
  src/unit-ReadCellSlabIter.cc
  src/unit-rtree.cc
  src/unit-s3.cc
  src/unit-s3-no-multipart.cc
  src/unit-status.cc
  src/unit-Consolidator.cc
  src/unit-Subarray.cc
  src/unit-SubarrayPartitioner-dense.cc
  src/unit-SubarrayPartitioner-error.cc
  src/unit-SubarrayPartitioner-sparse.cc
  src/unit-tbb.cc
  src/unit-threadpool.cc
  src/unit-Tile.cc
  src/unit-TileDomain.cc
  src/unit-uri.cc
  src/unit-uuid.cc
  src/unit-vfs.cc
  src/unit-win-filesystem.cc
  src/unit.cc
)

if (TILEDB_CPP_API)
  list(APPEND TILEDB_TEST_SOURCES
    src/unit-cppapi-array.cc
    src/unit-cppapi-config.cc
    src/unit-cppapi-consolidation.cc
    src/unit-cppapi-datetimes.cc
    src/unit-cppapi-filter.cc
    src/unit-cppapi-metadata.cc
    src/unit-cppapi-query.cc
    src/unit-cppapi-schema.cc
    src/unit-cppapi-subarray.cc
    src/unit-cppapi-type.cc
    src/unit-cppapi-updates.cc
    src/unit-cppapi-util.cc
    src/unit-cppapi-vfs.cc
  )
endif()

if (TILEDB_SERIALIZATION)
  list(APPEND TILEDB_TEST_SOURCES
    src/unit-capi-serialized_queries.cc
  )
endif()

if (TILEDB_TESTS_ENABLE_REST)
  list(APPEND TILEDB_TEST_SOURCES
    src/unit-capi-rest-dense_array.cc
  )
endif()

# unit test executable
add_executable(
  tiledb_unit EXCLUDE_FROM_ALL
  $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>
  ${TILEDB_TEST_SOURCES}
)

# TODO: Remove when KV API has been removed.
if (NOT WIN32)
  target_compile_options(tiledb_unit PRIVATE -Wno-deprecated-declarations)
endif()

target_include_directories(
  tiledb_unit BEFORE PRIVATE
    ${TILEDB_CORE_INCLUDE_DIR}
    ${TILEDB_EXPORT_HEADER_DIR}
)

target_link_libraries(tiledb_unit
  PUBLIC
    TILEDB_CORE_OBJECTS_ILIB
    Catch::Catch
)

# need to properly link the signal chaining library for catch to work with the jni hdfs interface
# see: https://docs.oracle.com/javase/9/vm/signal-chaining.htm
if (TILEDB_HDFS)
  find_package(LIBJVM REQUIRED)
  target_link_libraries(tiledb_unit
    PUBLIC
      LibJVM::libjvm
      LibJVM::libjsig
  )
  target_compile_definitions(tiledb_unit PRIVATE -DHAVE_HDFS)
  target_include_directories(tiledb_unit PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../external/include"
    )
endif()

if (TILEDB_S3)
  target_compile_definitions(tiledb_unit PRIVATE -DHAVE_S3)
endif()

if (TILEDB_TESTS_AWS_S3_CONFIG)
  message(STATUS "Tests built with AWS S3 config")
  target_compile_definitions(tiledb_unit PRIVATE -DTILEDB_TESTS_AWS_S3_CONFIG)
endif()

if (TILEDB_TBB)
  target_compile_definitions(tiledb_unit PRIVATE -DHAVE_TBB)
endif()

if (TILEDB_SERIALIZATION)
  target_compile_definitions(tiledb_unit PRIVATE -DTILEDB_SERIALIZATION)
endif()

# This is necessary only because we are linking directly to the core objects.
# Other users (e.g. the examples) do not need this flag.
target_compile_definitions(tiledb_unit PRIVATE -DTILEDB_CORE_OBJECTS_EXPORTS)

target_compile_definitions(tiledb_unit PRIVATE
  -DTILEDB_TEST_INPUTS_DIR="${CMAKE_CURRENT_SOURCE_DIR}/inputs"
)

# Linking dl is only needed on linux with gcc
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set_target_properties(tiledb_unit PROPERTIES
      LINK_FLAGS "-Wl,--no-as-needed -ldl"
    )
endif()

add_test(
  NAME "tiledb_unit"
  COMMAND $<TARGET_FILE:tiledb_unit> --durations=yes
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

# Set the PATH on Windows so that the test executable can find the TBB library.
if (TILEDB_TBB AND WIN32)
  set(PATH_STRING "$ENV{PATH};${TBB_LIB_DIR}")
  # Handle escaped semicolons. See
  # https://cmake.org/pipermail/cmake/2010-December/041176.html
  string(REPLACE "\\;" ";" PATH_STRING "${PATH_STRING}")
  string(REPLACE ";" "\\;" PATH_STRING "${PATH_STRING}")
  set_tests_properties("tiledb_unit" PROPERTIES ENVIRONMENT "PATH=${PATH_STRING}")
endif()

if(TILEDB_HDFS)
  # need to force flat namespace for the signal chaining to work on macOS
  if (APPLE)
    set_tests_properties("tiledb_unit" PROPERTIES ENVIRONMENT "DYLD_FORCE_FLAT_NAMESPACE=1")
  endif()
endif()

# Add custom target 'check'
add_custom_target(
  check COMMAND ${CMAKE_CTEST_COMMAND} -V -C ${CMAKE_BUILD_TYPE}
  DEPENDS tiledb_unit
)
