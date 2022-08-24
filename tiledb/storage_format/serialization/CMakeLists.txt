#
# tiledb/storage_format/serialization/CMakeLists.txt
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

include(common NO_POLICY_SCOPE)

if (TILEDB_TESTS)
    add_executable(unit_serializers EXCLUDE_FROM_ALL)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_serializers PUBLIC Catch2::Catch2WithMain)
    target_include_directories(unit_serializers PUBLIC ${TILEDB_EP_INSTALL_PREFIX}/include ${TILEDB_SOURCE_ROOT_DIR}/test/support)
    #print_target_properties(unit_serializers)

    # Sources for tests
    target_sources(unit_serializers PUBLIC test/main.cc test/unit_serializers.cc)

    add_test(
        NAME "unit_serializers"
        COMMAND $<TARGET_FILE:unit_serializers>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
