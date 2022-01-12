#
# tools/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2018-2021 TileDB, Inc.
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

cmake_minimum_required(VERSION 3.21)
project(test_tools)

add_executable(main EXCLUDE_FROM_ALL
  main.cc
  #$<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>
)

# List of all tool targets
set(TILEDB_TEST_TOOLS main)

# Set up link and include directories for all tools
foreach(TOOL ${TILEDB_TEST_TOOLS})
  target_link_libraries(${TOOL} PRIVATE
    TILEDB_CORE_OBJECTS_ILIB
  )
  target_include_directories(${TOOL} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
    #${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/
  )
endforeach()

# Add custom target 'test_tools' to build all tools.
add_custom_target(test_tools ALL DEPENDS ${TILEDB_TEST_TOOLS})