#
# tiledb/sm/query/ast/test/CMakeLists.txt
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

include(unit_test)

################################################################
#
# Define test support library for ast.
#
################################################################
set(AST_TEST_SUPPORT_SOURCES
    ${CMAKE_SOURCE_DIR}/test/support/src/ast_helpers.h
    ${CMAKE_SOURCE_DIR}/test/support/src/ast_helpers.cc
    )
add_library(ast_test_support_lib STATIC EXCLUDE_FROM_ALL ${AST_TEST_SUPPORT_SOURCES})
# We want tests to continue as normal even as the API is changing,
# so don't warn for deprecations, since they'll be escalated to errors.
if (NOT MSVC)
  target_compile_options(ast_test_support_lib PRIVATE -Wno-deprecated-declarations)
endif()
################################################################
#
# End ast_test_support_lib
#
################################################################

commence(unit_test ast)
    this_target_object_libraries(query_ast expr_rs oxidize_rs)
    this_target_sources(main.cc unit-query-ast.cc unit-query-to-datafusion.cc)
    target_include_directories(unit_ast PRIVATE "${TILEDB_CORE_INCLUDE_DIR}")
    this_target_link_libraries(ast_test_support_lib)
    # We want tests to continue as normal even as the API is changing,
    # so don't warn for deprecations, since they'll be escalated to errors.
    if (NOT MSVC)
        target_compile_options(unit_ast PRIVATE -Wno-deprecated-declarations)
    endif()
conclude(unit_test)
