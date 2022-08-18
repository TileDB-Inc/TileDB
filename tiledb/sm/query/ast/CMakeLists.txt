#
# tiledb/sm/query/ast/CMakeLists.txt
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

set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../..")

include(common NO_POLICY_SCOPE)

list(APPEND SOURCES
  query_ast.cc
)

#
# Object library for other units to depend upon
#
add_library(query_ast OBJECT ${SOURCES})
target_link_libraries(query_ast PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_query_ast EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_query_ast)
target_sources(compile_query_ast PRIVATE test/compile_query_ast_main.cc)

target_link_libraries(compile_query_ast PRIVATE query_ast)
target_link_libraries(compile_query_ast PRIVATE array_schema)

if (TILEDB_TESTS)
    add_executable(unit_ast EXCLUDE_FROM_ALL)
    target_sources(unit_ast PUBLIC test/main.cc test/unit-query-ast.cc)
    target_link_libraries(unit_ast PRIVATE query_ast)
    target_link_libraries(unit_ast PRIVATE array_schema)

    target_include_directories(unit_ast PRIVATE "${TILEDB_CORE_INCLUDE_DIR}")

    ################################################################
    #
    # Define test support library for ast.
    #
    ################################################################

    set(AST_TEST_SUPPORT_SOURCES
      ${CMAKE_SOURCE_DIR}/test/support/src/ast_helpers.h
      ${CMAKE_SOURCE_DIR}/test/support/src/ast_helpers.cc
      )

    add_library(ast_test_support_lib EXCLUDE_FROM_ALL ${AST_TEST_SUPPORT_SOURCES})

    find_package(Catch_EP REQUIRED)

    target_include_directories(ast_test_support_lib
      PUBLIC
	$<TARGET_PROPERTY:Catch2::Catch2,INTERFACE_INCLUDE_DIRECTORIES>
    )

    # We want tests to continue as normal even as the API is changing,
    # so don't warn for deprecations, since they'll be escalated to errors.
    if (NOT MSVC)
      target_compile_options(ast_test_support_lib PRIVATE -Wno-deprecated-declarations)
    endif()

    target_link_libraries(unit_ast PRIVATE ast_test_support_lib)

    ################################################################
    #
    # End ast_test_support_lib
    #
    ################################################################

    # We want tests to continue as normal even as the API is changing,
    # so don't warn for deprecations, since they'll be escalated to errors.
    if (NOT MSVC)
      target_compile_options(unit_ast PRIVATE -Wno-deprecated-declarations)
    endif()

    add_test(
        NAME "query_ast"
        COMMAND $<TARGET_FILE:unit_ast>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
