#
# tiledb/api/c_api/filesystem/test/CMakeLists.txt
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

add_executable(unit_capi_object_type EXCLUDE_FROM_ALL)
target_sources(unit_capi_object_type PUBLIC unit_capi_object_type.cc)
target_link_libraries(unit_capi_object_type PUBLIC capi_object)
target_link_libraries(unit_capi_object_type PUBLIC Catch2::Catch2WithMain)

add_test(
  NAME "unit_capi_object_type"
  COMMAND $<TARGET_FILE:unit_capi_object_type>
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(unit_capi_object_walk_order EXCLUDE_FROM_ALL)
target_sources(unit_capi_object_walk_order PUBLIC unit_capi_object_walk_order.cc)
target_link_libraries(unit_capi_object_walk_order PUBLIC capi_object)
target_link_libraries(unit_capi_object_walk_order PUBLIC Catch2::Catch2WithMain)

add_test(
  NAME "unit_capi_object_walk_order"
  COMMAND $<TARGET_FILE:unit_capi_object_walk_order>
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
