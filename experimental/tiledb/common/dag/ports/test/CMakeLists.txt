#
# experimental/tiledb/common/dag/ports/test/CMakeLists.txt
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
# Define unit tests for pool_allocator, data_block, and blocks_and_ports
#
dag_add_header_only_unit_test(pseudo_nodes)
dag_add_header_only_unit_test(concurrency)
dag_add_header_only_unit_test(ports)
#
# Define sieve test -- it is not catch-based
#
# Define unit test
#
add_executable(unit_ports_sieve EXCLUDE_FROM_ALL)
#
# Required libraries
#
#
# Define sources for test
#
target_sources(unit_ports_sieve PUBLIC
    unit_ports_sieve.cc
)
#
# Add unit test to global suite of unit tests
#
add_test(
    NAME "unit_ports_sieve"
    COMMAND $<TARGET_FILE:unit_ports_sieve>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
add_dependencies(all_unit_tests "unit_ports_sieve")
