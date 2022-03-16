#
# tiledb/sm/subarray/test/CMakeLists.txt
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

add_executable(unit_range_multi_subset EXCLUDE_FROM_ALL)
target_link_libraries(unit_range_multi_subset
    PRIVATE range_multi_subset
    PUBLIC Catch2::Catch2
)
target_sources(unit_range_multi_subset PUBLIC main.cc unit_range_multi_subset.cc)

add_test(
    NAME "unit_range_multi_subset"
    COMMAND $<TARGET_FILE:unit_range_multi_subset>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
