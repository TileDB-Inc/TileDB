#
# tiledb/sm/array/test/CMakeLists.txt
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

find_package(Catch_EP REQUIRED)

# Array tests
add_executable(unit_array EXCLUDE_FROM_ALL)
target_link_libraries(unit_array PRIVATE array)
target_link_libraries(unit_array PUBLIC Catch2::Catch2)

target_sources(unit_array PUBLIC main.cc unit_array_directory.cc)

add_test(
  NAME "unit_array"
  COMMAND $<TARGET_FILE:unit_array>
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

# Consistency tests
add_executable(unit_consistency EXCLUDE_FROM_ALL
  $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)

target_link_libraries(unit_consistency
  PUBLIC
    Catch2::Catch2WithMain
    TILEDB_CORE_OBJECTS_ILIB
)

target_sources(unit_consistency PUBLIC main.cc unit_consistency.cc)

add_test(
  NAME "unit_consistency"
  COMMAND $<TARGET_FILE:unit_consistency>
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
