#
# tiledb/api/c_api/dimension_label/test/CMakeLists.txt
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

add_executable(unit_capi_dimension_label EXCLUDE_FROM_ALL)
find_package(Catch_EP REQUIRED)
target_link_libraries(unit_capi_dimension_label PUBLIC Catch2::Catch2WithMain)
target_link_libraries(unit_capi_dimension_label PUBLIC capi_dimension_label_stub)
target_link_libraries(unit_capi_dimension_label PUBLIC generic_tile_io)

# Sources for tests
target_sources(unit_capi_dimension_label PUBLIC
    unit_capi_dimension_label.cc
)

add_test(
    NAME "unit_capi_dimension_label"
    COMMAND $<TARGET_FILE:unit_capi_dimension_label>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
