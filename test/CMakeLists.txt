#
# test/CMakeLists.txt
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

find_package(Catch_EP REQUIRED)

# These options not exposed in bootstrap script.
option(TILEDB_TESTS_AWS_S3_CONFIG "Use an S3 config appropriate for AWS in tests" OFF)
option(TILEDB_TESTS_ENABLE_REST "Enables REST tests (requires running REST server)" OFF)


# Arrow integration test config and dependencies
# Override from environment if not set in cache
if (NOT DEFINED $CACHE{TILEDB_ARROW_TESTS})
  if ($ENV{TILEDB_ARROW_TESTS})
    message(STATUS "Enabling Apache Arrow integration test")
    # Technically this would be an "option", but we want conditional override from ENV
    set(TILEDB_ARROW_TESTS ON CACHE BOOL "Enable Arrow tests (requires Python with pyarrow and numpy)" FORCE)
    mark_as_advanced(TILEDB_ARROW_TESTS)
  endif()
endif()

if (${TILEDB_ARROW_TESTS})
  # Reworked FindPython was introducted in 3.12 with features used below
  if (CMAKE_VERSION VERSION_LESS "3.12")
    message(FATAL_ERROR "CMake >= 3.12 is required for TileDB Arrow Tests. (found ${CMAKE_VERSION})")
  endif()
  # Tell CMake to check the Python registry entry last on Windows
  set(Python_FIND_REGISTRY "LAST")
  # Tell CMake to prefer Python from the PATH
  set(Python_FIND_STRATEGY "LOCATION")
  find_package(Python COMPONENTS Interpreter Development REQUIRED)
  find_package(pybind11)

  message(STATUS "Configuring Apache Arrow integration test with Python ${Python_VERSION} (${Python_EXECUTABLE})")

  # If we can't find the pybind11 cmake config (not available in pypi yet)
  # try to find with the current executable.
  if (NOT ${pybind11_FOUND})
    # Get the include arguments from the python executable (has "-I" compiler option)
    execute_process(COMMAND  ${Python_EXECUTABLE} -m pybind11 --includes
                    OUTPUT_VARIABLE CMD_PYBIND11_INCLUDE
                    RESULT_VARIABLE CMD_PYBIND11_RESULT
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (${CMD_PYBIND11_RESULT})
      message(FATAL_ERROR "Unable to find pybind11 via cmake or 'python3 -m pybind11 --includes'")
    endif()

    # Convert args to list
    separate_arguments(CMD_PARSED_INCLUDES NATIVE_COMMAND ${CMD_PYBIND11_INCLUDE})
    # Remove the "-I" from each include
    foreach(INCL_PATH IN LISTS CMD_PARSED_INCLUDES)
      string(REPLACE "-I" "" INCL_PATH ${INCL_PATH})
      list(APPEND PYBIND11_INCLUDE_DIRECTORIES ${INCL_PATH})
    endforeach()

    file(TO_CMAKE_PATH "${Python_SITELIB}" SAFE_Python_SITELIB)
    set(pybind11_FOUND TRUE CACHE BOOL "pybind11 include path found")
    add_library(pybind11::embed INTERFACE IMPORTED)
    target_include_directories(pybind11::embed INTERFACE ${PYBIND11_INCLUDE_DIRECTORIES})
    target_link_libraries(pybind11::embed INTERFACE Python::Python)
    target_compile_definitions(pybind11::embed INTERFACE -DTILEDB_PYTHON_SITELIB_PATH="${SAFE_Python_SITELIB}")
  endif()
  file(TO_CMAKE_PATH ${CMAKE_CURRENT_BINARY_DIR} SAFE_CURRENT_BINARY_DIR)
  target_compile_definitions(pybind11::embed INTERFACE -DTILEDB_PYTHON_UNIT_PATH="${SAFE_CURRENT_BINARY_DIR}")
endif()

# Include TileDB core header directories
set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")
# Include the C API directory so that the C++ 'tiledb' file can directly
# include "tiledb.h".
list(APPEND TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../tiledb/sm/c_api")

# Gather the test source files
set(TILEDB_TEST_SUPPORT_SOURCES
  src/ast_helpers.h
  src/ast_helpers.cc
  src/helpers.h
  src/helpers.cc
  src/helpers-dimension.h
  src/vfs_helpers.cc
  src/serialization_wrappers.cc
  )

set(TILEDB_UNIT_TEST_SOURCES
  src/helpers.h
  src/helpers-dimension.h
  src/test-capi-query-error-handling.cc
  src/unit-azure.cc
  src/unit-backwards_compat.cc
  src/unit-bufferlist.cc
  src/unit-capi-any.cc
  src/unit-capi-array.cc
  src/unit-capi-array_schema.cc
  src/unit-capi-async.cc
  src/unit-capi-attributes.cc
  src/unit-capi-buffer.cc
  src/unit-capi-config.cc
  src/unit-capi-context.cc
  src/unit-capi-consolidation.cc
  src/unit-capi-dense_array.cc
  src/unit-capi-dense_array_2.cc
  src/unit-capi-dense_neg.cc
  src/unit-capi-dense_vector.cc
  src/unit-capi-enum_values.cc
  src/unit-capi-error.cc
  src/unit-capi-filestore.cc
  src/unit-capi-fill_values.cc
  src/unit-capi-filter.cc
  src/unit-capi-fragment_info.cc
  src/unit-capi-group.cc
  src/unit-capi-incomplete.cc
  src/unit-capi-incomplete-2.cc
  src/unit-capi-metadata.cc
  src/unit-capi-nullable.cc
  src/unit-capi-object_mgmt.cc
  src/unit-capi-query.cc
  src/unit-capi-query_2.cc
  src/unit-capi-smoke-test.cc
  src/unit-capi-sparse_array.cc
  src/unit-capi-sparse_heter.cc
  src/unit-capi-sparse_neg.cc
  src/unit-capi-sparse_neg_2.cc
  src/unit-capi-sparse_real.cc
  src/unit-capi-sparse_real_2.cc
  src/unit-capi-string.cc
  src/unit-capi-string_dims.cc
  src/unit-capi-update-queries.cc
  src/unit-capi-uri.cc
  src/unit-capi-version.cc
  src/unit-capi-vfs.cc
  src/unit-CellSlabIter.cc
  src/unit-compression-dd.cc
  src/unit-compression-rle.cc
  src/unit-crypto.cc
  src/unit-ctx.cc
  src/unit-DenseTiler.cc
  src/unit-dimension.cc
  src/unit-domain.cc
  src/unit-duplicates.cc
  src/unit-empty-var-length.cc
  src/unit-filter-buffer.cc
  src/unit-filter-pipeline.cc
  src/unit-global-order.cc
  src/unit-gcs.cc
  src/unit-gs.cc
  src/unit-hdfs-filesystem.cc
  src/unit-lru_cache.cc
  src/unit-tile-metadata.cc
  src/unit-tile-metadata-generator.cc
  src/unit-bytevecvalue.cc
  src/unit-ReadCellSlabIter.cc
  src/unit-Reader.cc
  src/unit-resource-pool.cc
  src/unit-result-coords.cc
  src/unit-result-tile.cc
  src/unit-rtree.cc
  src/unit-s3-no-multipart.cc
  src/unit-s3.cc
  src/unit-sparse-global-order-reader.cc
  src/unit-sparse-unordered-with-dups-reader.cc
  src/unit-status.cc
  src/unit-Subarray.cc
  src/unit-SubarrayPartitioner-dense.cc
  src/unit-SubarrayPartitioner-error.cc
  src/unit-SubarrayPartitioner-sparse.cc
  src/unit-Tile.cc
  src/unit-TileDomain.cc
  src/unit-uuid.cc
  src/unit-vfs.cc
  src/unit-win-filesystem.cc
  )

if (TILEDB_CPP_API)
  list(APPEND TILEDB_UNIT_TEST_SOURCES
    src/unit-cppapi-array-deletes.cc
    src/unit-cppapi-array.cc
    src/unit-cppapi-checksum.cc
    src/unit-cppapi-config.cc
    src/unit-cppapi-consolidation-sparse.cc
    src/unit-cppapi-consolidation.cc
    src/unit-cppapi-consolidation-with-timestamps.cc
    src/unit-cppapi-datetimes.cc
    src/unit-cppapi-deletes.cc
    src/unit-cppapi-dense-qc-coords-mode.cc
    src/unit-cppapi-time.cc
    src/unit-cppapi-fill_values.cc
    src/unit-cppapi-filter.cc
    src/unit-cppapi-float-scaling-filter.cc
    src/unit-cppapi-xor-filter.cc
    src/unit-cppapi-fragment_info.cc
    src/unit-cppapi-group.cc
    src/unit-cppapi-hilbert.cc
    src/unit-cppapi-metadata.cc
    src/unit-cppapi-nullable.cc
    src/unit-cppapi-query.cc
    src/cpp-integration-query-condition.cc
    src/unit-cppapi-schema.cc
    src/unit-cppapi-string-dims.cc
    src/unit-cppapi-subarray.cc
    src/unit-cppapi-type.cc
    src/unit-cppapi-update-queries.cc
    src/unit-cppapi-updates.cc
    src/unit-cppapi-util.cc
    src/unit-cppapi-var-offsets.cc
    src/unit-cppapi-vfs.cc
  )
endif()

if (TILEDB_SERIALIZATION)
  list(APPEND TILEDB_UNIT_TEST_SOURCES
    src/unit-capi-serialized_queries.cc
    src/unit-capi-serialized_queries_using_subarray.cc
    src/unit-QueryCondition-serialization.cc
    src/unit-filter-serialization.cc
    src/unit-curl.cc
  )
endif()

if (TILEDB_TESTS_ENABLE_REST)
  list(APPEND TILEDB_UNIT_TEST_SOURCES
    src/unit-capi-rest-dense_array.cc
  )
endif()

if (TILEDB_ARROW_TESTS)
  list(APPEND TILEDB_UNIT_TEST_SOURCES
    src/unit-arrow.cc
    ${CMAKE_SOURCE_DIR}/tiledb/sm/cpp_api/arrow_io_impl.h
  )
endif()

if (TILEDB_EXPERIMENTAL_FEATURES)
  list(APPEND TILEDB_UNIT_TEST_SOURCES
    src/experimental_helpers.cc
    src/test-capi-dimension_labels.cc
    src/test-capi-dense-array-dimension-label.cc
    src/test-capi-dense-array-dimension-label-var.cc
    src/test-capi-sparse-array-dimension-label.cc
    src/test-dimension_labels-read-consistency.cc
    src/unit_intersect_fragments.cc
  )
endif()

if (TILEDB_VERBOSE)
  add_definitions(-DTILEDB_VERBOSE)
endif()

add_library(tiledb_test_support_lib EXCLUDE_FROM_ALL
  $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>
  ${TILEDB_TEST_SUPPORT_SOURCES}
)

target_include_directories(tiledb_test_support_lib
  PRIVATE
    $<TARGET_PROPERTY:Catch2::Catch2,INTERFACE_INCLUDE_DIRECTORIES>
)

# unit test executable
add_executable(
  tiledb_unit EXCLUDE_FROM_ALL
  $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>
  ${TILEDB_UNIT_TEST_SOURCES}
  "src/unit.cc"
)

add_dependencies(tiledb_unit tiledb_test_support_lib)

# We want tests to continue as normal even as the API is changing,
# so don't warn for deprecations, since they'll be escalated to errors.
if (NOT MSVC)
  target_compile_options(tiledb_unit PRIVATE -Wno-deprecated-declarations)
  target_compile_options(tiledb_test_support_lib PRIVATE -Wno-deprecated-declarations)
endif()

target_include_directories(
  tiledb_unit BEFORE PRIVATE
    ${TILEDB_CORE_INCLUDE_DIR}
    ${TILEDB_EXPORT_HEADER_DIR}
)
target_include_directories(
  tiledb_test_support_lib BEFORE PRIVATE
    ${TILEDB_CORE_INCLUDE_DIR}
    ${TILEDB_EXPORT_HEADER_DIR}
)

target_link_libraries(tiledb_unit
  PUBLIC
    TILEDB_CORE_OBJECTS_ILIB
    Catch2::Catch2
    tiledb_test_support_lib
)

target_link_libraries(tiledb_unit PRIVATE $<BUILD_INTERFACE:common>)

target_link_libraries(tiledb_test_support_lib
  PUBLIC
    TILEDB_CORE_OBJECTS_ILIB
    Catch2::Catch2
)

target_link_libraries(tiledb_test_support_lib PRIVATE $<BUILD_INTERFACE:common>)

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
  target_compile_definitions(tiledb_test_support_lib PRIVATE -DHAVE_HDFS)
  target_include_directories(tiledb_test_support_lib PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../external/include"
    )
endif()

if (TILEDB_S3)
  target_compile_definitions(tiledb_unit PRIVATE -DHAVE_S3)
  target_compile_definitions(tiledb_test_support_lib PRIVATE -DHAVE_S3)
endif()

if (TILEDB_TESTS_AWS_S3_CONFIG)
  message(STATUS "Tests built with AWS S3 config")
  target_compile_definitions(tiledb_unit PRIVATE -DTILEDB_TESTS_AWS_S3_CONFIG)
  target_compile_definitions(tiledb_test_support_lib PRIVATE -DTILEDB_TESTS_AWS_S3_CONFIG)
endif()

if (TILEDB_AZURE)
  target_compile_definitions(tiledb_unit PRIVATE -DHAVE_AZURE)
  target_compile_definitions(tiledb_test_support_lib PRIVATE -DHAVE_AZURE)
endif()

if (TILEDB_GCS)
  target_compile_definitions(tiledb_unit PRIVATE -DHAVE_GCS)
  target_compile_definitions(tiledb_test_support_lib PRIVATE -DHAVE_GCS)
endif()

if (TILEDB_SERIALIZATION)
  target_compile_definitions(tiledb_unit PRIVATE -DTILEDB_SERIALIZATION)
  target_compile_definitions(tiledb_test_support_lib PRIVATE -DTILEDB_SERIALIZATION)
endif()

if (TILEDB_ARROW_TESTS)
  target_link_libraries(tiledb_unit PRIVATE pybind11::embed)

  # install the python helper next to the executable for import
  configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/unit_arrow.py" "${CMAKE_CURRENT_BINARY_DIR}" COPYONLY)
endif()

# This is necessary only because we are linking directly to the core objects.
# Other users (e.g. the examples) do not need this flag.
target_compile_definitions(tiledb_unit PRIVATE -DTILEDB_CORE_OBJECTS_EXPORTS)
target_compile_definitions(tiledb_test_support_lib PRIVATE -DTILEDB_CORE_OBJECTS_EXPORTS)

target_compile_definitions(tiledb_unit PRIVATE
  -DTILEDB_TEST_INPUTS_DIR="${CMAKE_CURRENT_SOURCE_DIR}/inputs"
)

target_compile_definitions(tiledb_test_support_lib PRIVATE
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

# CI tests
add_subdirectory(ci)
if(WIN32)
  add_subdirectory(performance)
endif()

#add tests related to external items
add_subdirectory(external)
