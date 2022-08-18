#
# experimental/tiledb/common/edge/CMakeLists.txt
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
    edge.cc
)
gather_sources(${SOURCES})

#
# Object library for other units to depend upon
#
add_library(edge OBJECT ${SOURCES})
target_link_libraries(edge PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_edge EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_edge)
target_link_libraries(compile_edge PRIVATE edge)
target_sources(compile_edge PRIVATE test/compile_edge_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_edge EXCLUDE_FROM_ALL)
    target_link_libraries(unit_edge PUBLIC edge)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_edge PUBLIC Catch2::Catch2)
    target_link_libraries(unit_edge PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_edge PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_edge PUBLIC
            test/main.cc
            test/unit_edge.cc
            )

    add_test(
            NAME "unit_edge"
            COMMAND $<TARGET_FILE:unit_edge> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
