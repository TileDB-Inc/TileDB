#
# experimental/tiledb/common/nodes/CMakeLists.txt
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

################################################################
# Include subdirectory with unit tests                         #
################################################################
if (TILEDB_TESTS)
    add_subdirectory(test)
endif()

dag_add_header_only_object_library(simple_node)
dag_add_header_only_object_library(general_node)


if (YACK)
#list(APPEND SOURCES
#    nodes.cc
#)
#gather_sources(${SOURCES})

#
# Object library for other units to depend upon
#
#add_library(nodes OBJECT ${SOURCES})
# target_link_libraries(nodes PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_nodes EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_nodes)
target_sources(compile_nodes PRIVATE test/compile_nodes_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_simple EXCLUDE_FROM_ALL)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_simple PUBLIC Catch2::Catch2)

    # Sources for code elsewhere required for tests
    target_sources(unit_simple PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_simple PUBLIC
            test/unit_simple.cc
            )

    add_test(
            NAME "unit_simple"
            COMMAND $<TARGET_FILE:unit_simple> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()

if (TILEDB_TESTS)
    add_executable(unit_general EXCLUDE_FROM_ALL)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_general PUBLIC Catch2::Catch2)

    # Sources for code elsewhere required for tests
    target_sources(unit_general PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_general PUBLIC
            test/unit_general.cc
            )

    add_test(
            NAME "unit_general"
            COMMAND $<TARGET_FILE:unit_general> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()



#
# Since there are no components, there is no object library to test compile
#
# add_executable(compile_nodes_concurrency EXCLUDE_FROM_ALL)
# target_link_libraries(compile_nodes_concurrency PRIVATE nodes_concurrency)
# target_sources(compile_nodes_concurrency PRIVATE test/compile_nodes_concurrency_main.cc)
# add_dependencies(all_link_complete compile_nodes_concurrency)

if (TILEDB_TESTS)
    add_executable(unit_nodes_concurrency EXCLUDE_FROM_ALL)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_nodes_concurrency PUBLIC Catch2::Catch2)

    # Sources for code elsewhere required for tests
    target_sources(unit_nodes_concurrency PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_nodes_concurrency PUBLIC
            test/unit_nodes_concurrency.cc
            )

    add_test(
            NAME "unit_nodes_concurrency"
            COMMAND $<TARGET_FILE:unit_nodes_concurrency> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()

#
# Since there are no components, there is no object library to test compile
#
# add_executable(compile_util_functions EXCLUDE_FROM_ALL)
# target_link_libraries(compile_util_functions PRIVATE util_functions)
# target_sources(compile_util_functions PRIVATE test/compile_util_functions_main.cc)
# add_dependencies(all_link_complete compile_util_functions)

if (TILEDB_TESTS)
    add_executable(unit_util_functions EXCLUDE_FROM_ALL)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_util_functions PUBLIC Catch2::Catch2)

    # Sources for code elsewhere required for tests
    target_sources(unit_util_functions PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_util_functions PUBLIC
            test/unit_util_functions.cc
            )

    add_test(
            NAME "unit_util_functions"
            COMMAND $<TARGET_FILE:unit_util_functions> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()


# list(APPEND NODES_SIEVE_SOURCES
#    test/nodes_sieve.cc
# )
# gather_sources(${NODES_SIEVE_SOURCES})

#
# Object library for other units to depend upon
#
#add_library(nodes_sieve OBJECT ${NODES_SIEVE_SOURCES})
# target_link_libraries(nodes_sieve PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Since there are no components, there is no object library to test compile
#
# add_executable(compile_nodes_sieve EXCLUDE_FROM_ALL)
# target_link_libraries(compile_nodes_sieve PRIVATE nodes_sieve)
# target_sources(compile_nodes_sieve PRIVATE test/compile_nodes_sieve_main.cc)

if (TILEDB_TESTS)
    add_executable(nodes_sieve EXCLUDE_FROM_ALL)

#    target_link_libraries(nodes_sieve PUBLIC nodes_sieve)
#    find_package(Catch_EP REQUIRED)
#    target_link_libraries(nodes_sieve PUBLIC Catch2::Catch2)
#    target_link_libraries(nodes_sieve PUBLIC $<TARGET_OBJECTS:thread_pool>)
#    target_link_libraries(nodes_sieve PUBLIC $<TARGET_OBJECTS:baseline>)
#    target_link_libraries(nodes_sieve PUBLIC baseline>)

    # Sources for code elsewhere required for tests
    target_sources(nodes_sieve PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(nodes_sieve PUBLIC
            test/nodes_sieve.cc
            )

# Not a test (yet) per se
#    add_test(
#            NAME "nodes_sieve"
#            COMMAND $<TARGET_FILE:nodes_sieve> --durations=yes
#            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#    )
endif()


#
# Test-compile of object library ensures link-completeness
#
# add_executable(compile_stop_source EXCLUDE_FROM_ALL)
# add_dependencies(all_link_complete compile_stop_source)
# target_sources(compile_stop_source PRIVATE test/compile_stop_source_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_stop_source EXCLUDE_FROM_ALL)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_stop_source PUBLIC Catch2::Catch2)

    # Sources for code elsewhere required for tests
    target_sources(unit_stop_source PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_stop_source PUBLIC
            test/unit_stop_source.cc
            )

    add_test(
            NAME "unit_stop_source"
            COMMAND $<TARGET_FILE:unit_stop_source> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
endif()
