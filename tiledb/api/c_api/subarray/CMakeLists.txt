#
# tiledb/api/c_api/subarray/CMakeLists.txt
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

list(APPEND SOURCES
  subarray_api.cc
)
gather_sources(${SOURCES})

#
# Object library for other units to depend upon
#
add_library(capi_subarray OBJECT ${SOURCES})
# 'export' pulls in definitions to make APIs publically visible
target_link_libraries(capi_subarray PUBLIC export)
# pulls, at the least, definitions needed to find spdlog.h
target_link_libraries(capi_subarray PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#

#
# Cannot currently produce lower level OBJECT libraries needed 
# to achieve unit/test linkage due to the circular dependencies and inability
# of cmake to handle such circularity with OBJECT libraries.
#

# add_executable(compile_capi_subarray EXCLUDE_FROM_ALL)
# target_sources(compile_capi_subarray PRIVATE test/compile_capi_subarray_main.cc
# )
# target_link_libraries(compile_capi_subarray PRIVATE capi_subarray_stub)
# target_link_libraries(compile_capi_subarray PRIVATE capi_error)
# add_dependencies(all_link_complete compile_capi_subarray)

# add_test_subdirectory()
