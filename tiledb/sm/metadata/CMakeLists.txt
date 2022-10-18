#
# tiledb/sm/metadata/CMakeLists.txt
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

include(common NO_POLICY_SCOPE)

#
# `constants` object library
#
add_library(metadata OBJECT metadata.cc)
target_link_libraries(metadata PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(metadata PUBLIC buffer $<TARGET_OBJECTS:buffer>)
target_link_libraries(metadata PUBLIC constants $<TARGET_OBJECTS:constants>)
target_link_libraries(metadata PUBLIC time $<TARGET_OBJECTS:time>)
target_link_libraries(metadata PUBLIC vfs $<TARGET_OBJECTS:vfs>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_metadata EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_metadata)
target_link_libraries(compile_metadata PRIVATE metadata)
target_sources(compile_metadata PRIVATE test/compile_metadata_main.cc)


if (TILEDB_TESTS)
    add_executable(unit_metadata EXCLUDE_FROM_ALL)
    target_link_libraries(unit_metadata PRIVATE metadata)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_metadata PUBLIC Catch2::Catch2WithMain)
    target_link_libraries(unit_metadata PRIVATE tile)

    # Sources for tests
    target_sources(unit_metadata PUBLIC test/main.cc test/unit_metadata.cc)

    add_test(
        NAME "unit_metadata"
        COMMAND $<TARGET_FILE:unit_metadata>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
