#
# tiledb/sm/array_schema/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021 TileDB, Inc.
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
# `attribute` object library
#
add_library(attribute OBJECT attribute.cc)
target_link_libraries(attribute PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(attribute PUBLIC buffer $<TARGET_OBJECTS:buffer>)
target_link_libraries(attribute PUBLIC constants $<TARGET_OBJECTS:constants>)
target_link_libraries(attribute PUBLIC filter_pipeline $<TARGET_OBJECTS:filter_pipeline>)
target_link_libraries(attribute PUBLIC stringx $<TARGET_OBJECTS:stringx>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_attribute EXCLUDE_FROM_ALL)
target_link_libraries(compile_attribute PRIVATE attribute)
target_sources(compile_attribute PRIVATE test/compile_attribute_main.cc)

#
# `dimension` object library
#
add_library(dimension OBJECT dimension.cc)
target_link_libraries(dimension PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(dimension PUBLIC filter_pipeline $<TARGET_OBJECTS:filter_pipeline>)
target_link_libraries(dimension PUBLIC misc_types $<TARGET_OBJECTS:misc_types>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_dimension EXCLUDE_FROM_ALL)
target_link_libraries(compile_dimension PRIVATE dimension)
target_sources(compile_dimension PRIVATE test/compile_dimension_main.cc)

#
# `domain` object library
#
add_library(domain OBJECT domain.cc)
target_link_libraries(domain PUBLIC datum $<TARGET_OBJECTS:datum>)
target_link_libraries(domain PUBLIC dimension $<TARGET_OBJECTS:dimension>)
target_link_libraries(domain PUBLIC math $<TARGET_OBJECTS:math>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_domain EXCLUDE_FROM_ALL)
target_link_libraries(compile_domain PRIVATE domain)
target_sources(compile_domain PRIVATE test/compile_domain_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_array_schema EXCLUDE_FROM_ALL)
    target_link_libraries(unit_array_schema PRIVATE attribute domain)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_array_schema PUBLIC Catch2::Catch2)

    # Sources for tests
    target_sources(unit_array_schema PUBLIC test/main.cc test/unit_dimension.cc test/unit_domain_data.cc)

    add_test(
        NAME "unit_array_schema"
        COMMAND $<TARGET_FILE:unit_array_schema>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
