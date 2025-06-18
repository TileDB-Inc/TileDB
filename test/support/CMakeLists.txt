#
# test/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2022-2023 TileDB, Inc.
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

find_package(Catch2 REQUIRED)

# Include TileDB core header directories
set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_SOURCE_DIR}")

# Include the C API directory so that the C++ 'tiledb' file can directly
# include "tiledb.h".
list(APPEND TILEDB_CORE_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api")

# Gather the test source files
set(TILEDB_TEST_SUPPORT_SOURCES
  rapidcheck/show.cc
  src/array_helpers.cc
  src/array_schema_helpers.cc
  src/ast_helpers.h
  src/ast_helpers.cc
  src/helpers.h
  src/helpers.cc
  src/helpers-dimension.h
  src/mem_helpers.h
  src/mem_helpers.cc
  src/serialization_wrappers.cc
  src/stats.cc
  src/temporary_local_directory.cc
  src/vfs_helpers.cc
  )

if (TILEDB_VERBOSE)
  add_definitions(-DTILEDB_VERBOSE)
endif()

add_library(tiledb_test_support_lib STATIC EXCLUDE_FROM_ALL
  $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>
  ${TILEDB_TEST_SUPPORT_SOURCES}
)

target_include_directories(tiledb_test_support_lib
  PRIVATE
    $<TARGET_PROPERTY:Catch2::Catch2,INTERFACE_INCLUDE_DIRECTORIES>
)

if (NOT MSVC)
  target_compile_options(tiledb_test_support_lib PRIVATE -Wno-deprecated-declarations)
endif()

target_link_libraries(tiledb_test_support_lib PRIVATE $<BUILD_INTERFACE:common>)
target_link_libraries(tiledb_test_support_lib PRIVATE baseline)

target_include_directories(
  tiledb_test_support_lib BEFORE PRIVATE
    ${TILEDB_CORE_INCLUDE_DIR}
    ${TILEDB_EXPORT_HEADER_DIR}
)

if (TILEDB_RUST)
  add_dependencies(tiledb_test_support_lib rust_h)
  target_include_directories(
    tiledb_test_support_lib PUBLIC
    ${TILEDB_OXIDIZE_INCLUDE_DIR}
  )
endif()

if (TILEDB_TESTS_AWS_S3_CONFIG)
  message(STATUS "Tests built with AWS S3 config")
  target_compile_definitions(tiledb_test_support_lib PRIVATE -DTILEDB_TESTS_AWS_S3_CONFIG)
endif()

if (TILEDB_SERIALIZATION)
  target_compile_definitions(tiledb_test_support_lib PRIVATE -DTILEDB_SERIALIZATION)
endif()

target_compile_definitions(tiledb_test_support_lib PRIVATE -Dtiledb_EXPORTS)

target_compile_definitions(tiledb_test_support_lib PRIVATE
  -DTILEDB_TEST_INPUTS_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../inputs"
)

target_link_libraries(tiledb_test_support_lib
  PUBLIC
    TILEDB_CORE_OBJECTS_ILIB
    Catch2::Catch2
)

# tdb_catch object library
include(common NO_POLICY_SCOPE)
include(object_library)

commence(object_library tdb_catch)
  this_target_sources(tdb_catch_prng.cc)
  this_target_link_libraries(Catch2::Catch2)
  this_target_object_libraries(seedable_global_PRNG)
conclude(object_library)

commence(object_library mem_helpers)
  this_target_sources(src/mem_helpers.cc)
  this_target_object_libraries(baseline)
conclude(object_library)

add_test_subdirectory()
