#
# tiledb/sm/dimension_label/CMakeLists.txt
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

add_library(uniform_mapping OBJECT uniform_mapping.cc)
target_link_libraries(uniform_mapping PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(uniform_mapping PUBLIC misc_types $<TARGET_OBJECTS:misc_types>)
target_link_libraries(uniform_mapping PUBLIC constants $<TARGET_OBJECTS:constants>)

add_library(dimension_label OBJECT dimension_label.cc )
target_link_libraries(dimension_label PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(dimension_label PUBLIC misc_types $<TARGET_OBJECTS:misc_types>)
target_link_libraries(dimension_label PUBLIC uniform_mapping $<TARGET_OBJECTS:uniform_mapping>)

add_executable(compile_dimension_label EXCLUDE_FROM_ALL)
target_link_libraries(compile_dimension_label PRIVATE dimension_label)
target_sources(compile_dimension_label PRIVATE test/compile_dimension_label_main.cc)

if (TILEDB_TESTS)
    add_subdirectory(test)
endif()
