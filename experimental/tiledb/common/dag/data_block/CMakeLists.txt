#
# experimental/tiledb/common/data_block/CMakeLists.txt
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
    data_block.cc
    pool_allocator.cc
)
gather_sources(${SOURCES})

#
# Object library for other units to depend upon
#
add_library(data_block OBJECT ${SOURCES})
target_link_libraries(data_block PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_data_block EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_data_block)
target_link_libraries(compile_data_block PRIVATE data_block)
target_sources(compile_data_block PRIVATE test/compile_data_block_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_data_block EXCLUDE_FROM_ALL)
    target_link_libraries(unit_data_block PUBLIC data_block)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_data_block PUBLIC Catch2::Catch2)
    target_link_libraries(unit_data_block PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_data_block PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_data_block PUBLIC
            test/main.cc
            test/unit_data_block.cc
            )

    add_test(
            NAME "unit_data_block"
            COMMAND $<TARGET_FILE:unit_data_block> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_dependencies(check unit_data_block)
endif()

list(APPEND POOL_SOURCES
    pool_allocator.cc
)
gather_sources(${POOL_SOURCES})

#
# Object library for other units to depend upon
#
add_library(pool_allocator OBJECT ${POOL_SOURCES})
target_link_libraries(pool_allocator PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_pool_allocator EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_pool_allocator)
target_link_libraries(compile_pool_allocator PRIVATE pool_allocator)
target_sources(compile_pool_allocator PRIVATE test/compile_pool_allocator_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_pool_allocator EXCLUDE_FROM_ALL)
    target_link_libraries(unit_pool_allocator PUBLIC pool_allocator)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_pool_allocator PUBLIC Catch2::Catch2)
    target_link_libraries(unit_pool_allocator PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_pool_allocator PUBLIC ${DEPENDENT_POOL_SOURCES})

    # Sources for tests
    target_sources(unit_pool_allocator PUBLIC
            test/main.cc
            test/unit_pool_allocator.cc
            )

    add_test(
            NAME "unit_pool_allocator"
            COMMAND $<TARGET_FILE:unit_pool_allocator> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_dependencies(check unit_pool_allocator)
endif()

# list(APPEND SOURCES
# )
# gather_sources(${SOURCES})

#
# Object library for other units to depend upon
#
# add_library(blocks_and_ports OBJECT ${SOURCES})
# target_link_libraries(blocks_and_ports PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
# add_executable(compile_blocks_and_ports EXCLUDE_FROM_ALL)
# target_link_libraries(compile_blocks_and_ports PRIVATE blocks_and_ports)
# target_sources(compile_blocks_and_ports PRIVATE test/compile_blocks_and_ports_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_blocks_and_ports EXCLUDE_FROM_ALL)
#    target_link_libraries(unit_blocks_and_ports PUBLIC blocks_and_ports)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_blocks_and_ports PUBLIC Catch2::Catch2)
#    target_link_libraries(unit_blocks_and_ports PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_blocks_and_ports PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_blocks_and_ports PUBLIC
            test/main.cc
            test/unit_blocks_and_ports.cc
            )

    add_test(
            NAME "unit_blocks_and_ports"
            COMMAND $<TARGET_FILE:unit_blocks_and_ports> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_dependencies(check unit_blocks_and_ports)
endif()

