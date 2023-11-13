#
# tiledb/api/c_api/dimension_label/CMakeLists.txt
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
    dimension_label_api.cc
)
gather_sources(${SOURCES})

#
# Object library for other units to depend upon
#
add_library(capi_dimension_label_stub OBJECT ${SOURCES})
target_link_libraries(capi_dimension_label_stub PUBLIC capi_context_stub $<TARGET_OBJECTS:capi_context_stub>)
target_link_libraries(capi_dimension_label_stub PUBLIC capi_datatype $<TARGET_OBJECTS:capi_datatype>)
target_link_libraries(capi_dimension_label_stub PUBLIC capi_data_order $<TARGET_OBJECTS:capi_data_order>)
target_link_libraries(capi_dimension_label_stub PUBLIC array_schema $<TARGET_OBJECTS:array_schema>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_capi_dimension_label_stub EXCLUDE_FROM_ALL)
target_link_libraries(compile_capi_dimension_label_stub PRIVATE capi_dimension_label_stub)
target_link_libraries(compile_capi_dimension_label_stub PRIVATE generic_tile_io)
target_sources(compile_capi_dimension_label_stub PRIVATE test/compile_capi_dimension_label_main.cc)
add_dependencies(all_link_complete compile_capi_dimension_label_stub)

add_test_subdirectory()
