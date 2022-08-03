#
# tiledb/storage_format/delete_condition/CMakeLists.txt
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

set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../..")

include(common NO_POLICY_SCOPE)

if (TILEDB_TESTS)
    add_executable(unit_delete_condition EXCLUDE_FROM_ALL)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_delete_condition PUBLIC Catch2::Catch2)
    target_link_libraries(unit_delete_condition PUBLIC ast_test_support_lib)

    # Sources for tests
    target_sources(unit_delete_condition PUBLIC test/main.cc test/unit_delete_condition.cc)

    target_include_directories(unit_delete_condition PRIVATE "${TILEDB_CORE_INCLUDE_DIR}")

    # The dependencies can't yet be factored into separate object libraries
    target_link_libraries(unit_delete_condition PUBLIC TILEDB_CORE_OBJECTS)
    target_link_libraries(unit_delete_condition PUBLIC TILEDB_CORE_OBJECTS_ILIB)

    add_test(
        NAME "unit_delete_condition"
        COMMAND $<TARGET_FILE:unit_delete_condition>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()