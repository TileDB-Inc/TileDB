#
# experimental/tiledb/common/node/CMakeLists.txt
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

include(common NO_POLICY_SCOPE)

list(APPEND SOURCES
    node.cc
)
gather_sources(${SOURCES})

#
# Object library for other units to depend upon
#
add_library(node OBJECT ${SOURCES})
target_link_libraries(node PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_node EXCLUDE_FROM_ALL)
target_link_libraries(compile_node PRIVATE node)
target_sources(compile_node PRIVATE test/compile_node_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_node EXCLUDE_FROM_ALL)
    target_link_libraries(unit_node PUBLIC node)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_node PUBLIC Catch2::Catch2)
    target_link_libraries(unit_node PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_node PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_node PUBLIC
            test/main.cc
            test/unit_node.cc
            )

    add_test(
            NAME "unit_node"
            COMMAND $<TARGET_FILE:unit_node> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
