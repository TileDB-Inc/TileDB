#
# experimental/tiledb/common/edge/test/CMakeLists.txt
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

################################################################
# Unit test for edge                                           #
################################################################

#
# Define unit test
#
add_executable(unit_edge EXCLUDE_FROM_ALL)

#
# Required libraries, notably the associated object library
#
# NOTE: edge is header-only
#
# target_link_libraries(unit_edge PUBLIC edge)
target_link_libraries(unit_edge PUBLIC Catch2::Catch2)

#
# Define sources for test
#
target_sources(unit_edge PUBLIC
    main.cc
    unit_edge.cc
)

#
# Add unit test to global suite of unit tests
#
add_test(
    NAME "unit_edge"
    COMMAND $<TARGET_FILE:unit_edge> --durations=yes
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)


################################################################
# Unit test for edges with data_block                          #
################################################################

#
# Define unit test
#
add_executable(unit_block_edge EXCLUDE_FROM_ALL)

#
# Required libraries, notably the associated object library
#
# NOTE: data_block and edge are header-only
#
# target_link_libraries(unit_block_edge PUBLIC edge)
# target_link_libraries(unit_block_edge PUBLIC data_block)
target_link_libraries(unit_block_edge PUBLIC Catch2::Catch2)

#
# Define sources for test
#
target_sources(unit_block_edge PUBLIC
    main.cc
    unit_block_edge.cc
)

#
# Add unit test to global suite of unit tests
#
add_test(
    NAME "unit_block_edge"
    COMMAND $<TARGET_FILE:unit_block_edge> --durations=yes
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)


################################################################
# Unit test for edges with pseudo_node                         #
################################################################

#
# Define unit test
#
add_executable(unit_edge_pseudo_nodes EXCLUDE_FROM_ALL)

#
# Required libraries
#
# NOTE: data_block, edge, and pseudo_nodes are header-only
#
# target_link_libraries(unit_edge_pseudo_nodes PUBLIC edge)
# target_link_libraries(unit_edge_pseudo_nodes PUBLIC data_block)
target_link_libraries(unit_edge_pseudo_nodes PUBLIC Catch2::Catch2)

#
# Define sources for test
#
target_sources(unit_edge_pseudo_nodes PUBLIC
    main.cc
    unit_edge_pseudo_nodes.cc
)

#
# Add unit test to global suite of unit tests
#
add_test(
    NAME "unit_edge_pseudo_nodes"
    COMMAND $<TARGET_FILE:unit_edge_pseudo_nodes> --durations=yes
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
