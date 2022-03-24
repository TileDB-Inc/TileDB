#
# tiledb/appl/blob_array/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2022 TileDB, Inc.
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

if (POLICY CMP0076) # CMP0076 unknown to vs2017 cmake version 3.12.18081601-MSVC_2
  cmake_policy(SET CMP0076 NEW)  # CMake 3.13 - converts relative paths to absolute
endif()
find_package(Catch_EP REQUIRED)

add_executable(unit_blob_array EXCLUDE_FROM_ALL)

target_compile_definitions(unit_blob_array PRIVATE
  -DTILEDB_MODULE_CMAKE_LISTS_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
  -DTILEDB_TEST_INPUTS_DIR="${CMAKE_SOURCE_DIR}/test/inputs"
)

if (TILEDB_S3)
#  add_definitions(-DHAVE_S3)
  target_compile_definitions(unit_blob_array PRIVATE
    -DHAVE_S3
  )
endif()

# Azure dependencies
if (TILEDB_AZURE)
#  add_definitions(-DHAVE_AZURE)
  target_compile_definitions(unit_blob_array PRIVATE
    -DHAVE_AZURE
  )
endif()

# GCS dependencies
if (TILEDB_GCS)
#  add_definitions(-DHAVE_GCS)
  target_compile_definitions(unit_blob_array PRIVATE
    -DHAVE_GCS
  )
endif()

if (TILEDB_HDFS)
  if(NOT WIN32)
#   add_definitions(-DHAVE_HDFS)
  target_compile_definitions(unit_blob_array PRIVATE
    -DHAVE_HDFS
  )
  endif()
endif()

add_dependencies(unit_blob_array TILEDB_CORE_OBJECTS)

#target_link_libraries(unit_blob_array PUBLIC Catch2::Catch2)
# Note: tiledb_shared defined in 'outer' level, from which, and
# -after- this file is included
target_link_libraries(
    unit_blob_array PUBLIC 
    Catch2::Catch2 
    $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>
    tiledb_test_support_lib
    )


message(STATUS "CMAKE_MODULE_PATH is ${CMAKE_MODULE_PATH}")
message(STATUS "CMAKE_PREFIX_PATH is ${CMAKE_PREFIX_PATH}")

target_link_libraries(unit_blob_array
  PUBLIC
    TILEDB_CORE_OBJECTS_ILIB
    Catch2::Catch2
)

message(STATUS "CMAKE_SOURCE_DIR is ${CMAKE_SOURCE_DIR}")
message(STATUS "CMAKE_BINARY_DIR is ${CMAKE_BINARY_DIR}")

# Include TileDB core header directories
set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/")
# Include the C API directory so that the C++ 'tiledb' file can directly
# include "tiledb.h".
list(APPEND TILEDB_CORE_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api")

message(STATUS "TILEDB_CORE_INCLUDE_DIR is ${TILEDB_CORE_INCLUDE_DIR}")
message(STATUS "TILEDB_EXPORT_HEADER_DIR is ${TILEDB_EXPORT_HEADER_DIR}")

target_include_directories(
  unit_blob_array BEFORE PRIVATE
    ${TILEDB_CORE_INCLUDE_DIR}
    ${TILEDB_EXPORT_HEADER_DIR}
    ${CMAKE_BINARY_DIR}/tiledb
)

set(BA_TMP_DIR "${CMAKE_BINARY_DIR}/../externals/install/include")
message(STATUS "BA_TMP_DIR is ${BA_TMP_DIR}")
target_include_directories(unit_blob_array PRIVATE
     ${BA_TMP_DIR}
)

# Sources for code under test
#these come with the rest of tiledb objects...
target_sources(unit_blob_array PUBLIC
  test/unit_blob_array_main.cc
)

add_test(
        NAME "unit_blob_array"
        COMMAND $<TARGET_FILE:unit_blob_array>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
