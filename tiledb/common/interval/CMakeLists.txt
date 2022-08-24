#
# tiledb/common/interval/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2021 TileDB, Inc.
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

add_executable(unit_interval EXCLUDE_FROM_ALL)
target_link_libraries(unit_interval PUBLIC Catch2::Catch2WithMain)
#target_include_directories(unit_interval PUBLIC Catch2::Catch2)
target_include_directories(unit_interval       
  PUBLIC
  #$<TARGET_PROPERTY:Catch2::Catch2,INTERFACE_INCLUDE_DIRECTORIES>
  ${TILEDB_EP_INSTALL_PREFIX}/include
  )
  
print_target_properties(unit_interval)
print_target_properties(Catch2::Catch2)

# Sources for code under test
target_sources(unit_interval PUBLIC
  interval.cc
)

# Sources for tests
target_sources(unit_interval PUBLIC
  test/main.cc
        test/unit_interval_bound.cc
        test/unit_interval_constructors.cc
        test/unit_interval_operations.cc
        test/unit_interval_predicates.cc
        test/unit_interval_types.cc
)

add_test(
        NAME "unit_interval"
        COMMAND $<TARGET_FILE:unit_interval>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
