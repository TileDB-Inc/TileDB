#
# experimental/tiledb/common/dag/nodes/test/CMakeLists.txt
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


#
# Define unit tests
#
dag_add_header_only_unit_test(general_nodes)
dag_add_header_only_unit_test(simple_nodes)
dag_add_header_only_unit_test(segmented_nodes)
dag_add_header_only_unit_test(segmented_mimo_nodes)
dag_add_header_only_unit_test(stop_source)
dag_add_header_only_unit_test(util_functions)
dag_add_header_only_unit_test(nodes_concurrency)

#
# Define sieve test -- it is not catch-based
#
# Define unit test
#
add_executable(unit_nodes_sieve EXCLUDE_FROM_ALL)
#
# Required libraries
#
#
# Define sources for test
#
target_sources(unit_nodes_sieve PUBLIC
    unit_nodes_sieve.cc
)
#
# Add unit test to global suite of unit tests
#
add_test(
    NAME "unit_nodes_sieve"
    COMMAND $<TARGET_FILE:unit_nodes_sieve>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
add_dependencies(all_unit_tests "unit_nodes_sieve")

