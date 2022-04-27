#
# tiledb/sm/query/ast/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2021-2022 TileDB, Inc.
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
# Test-compile of object library ensures link-completeness
#
add_library(query_ast OBJECT query_ast.cc)

add_executable(compile_ast EXCLUDE_FROM_ALL)
target_link_libraries(compile_ast PRIVATE query_ast)
target_sources(compile_ast PRIVATE
        test/compile_ast_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_ast EXCLUDE_FROM_ALL)
    target_link_libraries(unit_ast PRIVATE query_ast)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_ast PUBLIC Catch2::Catch2)

    # Sources for tests
    target_include_directories(unit_ast PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../../../tiledb/sm/c_api")
    target_include_directories(unit_ast PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tiledb/tiledb")
    target_sources(unit_ast PUBLIC test/main.cc test/unit-query-ast.cc)

    add_test(
        NAME "query_ast"
        COMMAND $<TARGET_FILE:unit_ast>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()