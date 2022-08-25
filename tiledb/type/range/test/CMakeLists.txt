#
# tiledb/type/range/test/CMakeLists.txt
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
find_package(Catch_EP REQUIRED)

add_executable(unit_range EXCLUDE_FROM_ALL
  main.cc
  unit_range.cc
  unit_check_range_is_subset.cc
  unit_check_range_is_valid.cc
  unit_crop_range.cc
  unit_range_str.cc
)
target_link_libraries(unit_range
  PRIVATE range
  PUBLIC Catch2::Catch2
)
add_test(
  NAME "unit_range"
  COMMAND $<TARGET_FILE:unit_range>
  WORKING_DIRECTORY ${CMAKE_CURRENT_BUILD_DIR}
)
