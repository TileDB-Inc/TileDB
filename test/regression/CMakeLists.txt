#
# test/regression/CMakeLists.txt
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

set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../..")
list(APPEND TILEDB_CORE_INCLUDE_DIR "${CMAKE_BINARY_DIR}/tiledb/")

find_package(Catch2 REQUIRED)

set(SOURCES
  targets/sc-12024.cc
  targets/sc-15387.cc
  targets/sc-18836.cc
  targets/sc-23712.cc
  targets/sc-44758.cc
)

if (TILEDB_SERIALIZATION)
  list(APPEND SOURCES targets/sc-18250.cc)
endif()

if (TILEDB_CPP_API)
  list(APPEND SOURCES targets/sc-24079.cc)
  list(APPEND SOURCES targets/sc-25116.cc)
  list(APPEND SOURCES targets/sc-29682.cc)
  list(APPEND SOURCES targets/sc-33480.cc)
  list(APPEND SOURCES targets/sc-35424.cc)
  list(APPEND SOURCES targets/sc-36372.cc)
  list(APPEND SOURCES targets/sc-38300.cc)
  list(APPEND SOURCES targets/sc-48428.cc)
  list(APPEND SOURCES targets/sc-52975.cc)
  list(APPEND SOURCES targets/sc-53334.cc)
  list(APPEND SOURCES targets/sc-53791.cc)
  list(APPEND SOURCES targets/sc-53970.cc)
  list(APPEND SOURCES targets/sc-54473.cc)
  list(APPEND SOURCES targets/sc-64121.cc)
endif()

add_executable(tiledb_regression
  EXCLUDE_FROM_ALL
  regression.cc
  ${SOURCES}
)

target_compile_definitions(tiledb_regression PRIVATE
  -DTILEDB_TEST_INPUTS_DIR="${CMAKE_SOURCE_DIR}/test/inputs/"
)

if (NOT MSVC)
  target_compile_options(tiledb_regression PRIVATE -Wno-deprecated-declarations)
endif()

target_link_libraries(tiledb_regression
  PUBLIC
    tiledb_Catch2WithMain
    local_install
    tiledb
)

target_include_directories(tiledb_regression
  PRIVATE
    ${CMAKE_INSTALL_PREFIX}/include
    "${TILEDB_CORE_INCLUDE_DIR}"
)

add_test(
  NAME "tiledb_regression"
  COMMAND $<TARGET_FILE:tiledb_regression> --durations=yes
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

if (BUILD_SHARED_LIBS)
  # Add the location of the tiledb shared library to PATH, to help Windows find it.
  set_property(TEST tiledb_regression PROPERTY ENVIRONMENT_MODIFICATION "PATH=path_list_append:$<TARGET_FILE_DIR:tiledb>")
endif()
