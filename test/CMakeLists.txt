#
# test/CMakeLists.txt
#
#
# The MIT License
#
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

# Include TileDB core header directories
file(GLOB TILEDB_CORE_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/core/include/*")

# catch unit-test library
add_library(
  catch_main EXCLUDE_FROM_ALL
  OBJECT "src/unit.cc"
)

set_target_properties(catch_main PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED ON
)

target_include_directories(
  catch_main PRIVATE
  ${CMAKE_SOURCE_DIR}/external/include/catch
)

# Gather the test source files
file(GLOB_RECURSE TILEDB_TEST_SOURCES "src/unit-*.cc")

# unit test executable
add_executable(
  tiledb_unit EXCLUDE_FROM_ALL
  $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>
  $<TARGET_OBJECTS:catch_main>
  ${TILEDB_TEST_SOURCES}
)

set_target_properties(
  tiledb_unit PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED ON
)

target_link_libraries(
  tiledb_unit
  ${TILEDB_LIB_DEPENDENCIES} -lpthread tiledb_shared ${S3_LIB_DEPENDENCIES}
)

target_include_directories(
  tiledb_unit BEFORE PRIVATE
  ${TILEDB_CORE_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/external/include/catch
)

if (APPLE)
  add_test(
    NAME "tiledb_unit"
    COMMAND env DYLD_FORCE_FLAT_NAMESPACE=1 $<TARGET_FILE:tiledb_unit> --durations=yes
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test
  )
else()
   add_test(
    NAME "tiledb_unit"
    COMMAND $<TARGET_FILE:tiledb_unit> --durations=yes
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test
  )
endif()


# Add custom target 'check'
add_custom_target(
  check COMMAND ${CMAKE_CTEST_COMMAND} -V
  DEPENDS tiledb_unit
)
