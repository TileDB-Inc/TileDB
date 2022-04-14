#
# tiledb/sm/subarray/CMakeLists.txt
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

include(common NO_POLICY_SCOPE)

#
# `range_subset` object library
#
add_library(range_subset OBJECT range_subset.cc)
target_link_libraries(range_subset PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(range_subset PUBLIC constants $<TARGET_OBJECTS:constants>)
target_link_libraries(range_subset PUBLIC thread_pool $<TARGET_OBJECTS:thread_pool>)
target_link_libraries(range_subset PUBLIC range $<TARGET_OBJECTS:range>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_range_subset EXCLUDE_FROM_ALL)
target_link_libraries(compile_range_subset PRIVATE range_subset)
target_sources(compile_range_subset PRIVATE test/compile_range_subset_main.cc)

if (TILEDB_TESTS)
    add_subdirectory(test)
endif()
