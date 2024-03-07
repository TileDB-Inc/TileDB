#
# tiledb/api/c_api/array/test/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2023 TileDB, Inc.
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

include(common NO_POLICY_SCOPE)
find_package(Catch_EP REQUIRED)

# #TODO: link only against a new CAPI array handle, not all core objects
add_executable(
    unit_capi_array EXCLUDE_FROM_ALL
)

target_link_libraries(unit_capi_array
  PUBLIC
    Catch2::Catch2WithMain
    export
    TILEDB_CORE_OBJECTS_ILIB
    TILEDB_CORE_OBJECTS
)

# Sources for tests
target_sources(unit_capi_array PUBLIC  
    unit_capi_array.cc
)

if (TILEDB_HDFS)
  target_include_directories(unit_capi_array PRIVATE "${TILEDB_EXTERNAL_INCLUDE}")
  set_source_files_properties(unit_capi_array.cc PROPERTIES INCLUDE_DIRECTORIES "${TILEDB_EXTERNAL_INCLUDE}")
endif()

add_test(
    NAME "unit_capi_array"
    COMMAND $<TARGET_FILE:unit_capi_array>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
