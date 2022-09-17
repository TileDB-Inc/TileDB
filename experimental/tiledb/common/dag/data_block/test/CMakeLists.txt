#
# experimental/tiledb/common/data_block/test/CMakeLists.txt
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
# Unit test for pool_allocator                                 #
################################################################

#
# Define unit test
#
add_executable(unit_pool_allocator EXCLUDE_FROM_ALL)

#
# Required libraries, notably the associated object library
#
# NOTE: pool_allocator is header only
#
# target_link_libraries(unit_pool_allocator PUBLIC pool_allocator)
target_link_libraries(unit_pool_allocator PUBLIC Catch2::Catch2)

#
# Define sources for test
#
target_sources(unit_pool_allocator PUBLIC
        main.cc
        unit_pool_allocator.cc
)

#
# Add unit test to global suite of unit tests
#
add_test(
        NAME "unit_pool_allocator"
        COMMAND $<TARGET_FILE:unit_pool_allocator> --durations=yes
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)


################################################################
# Unit test for data_block                                     #
################################################################

#
# Define unit test
#
add_executable(unit_data_block EXCLUDE_FROM_ALL)

#
# Required libraries, notably the associated object library
#
# NOTE: data_block is header only
# 
# target_link_libraries(unit_data_block PUBLIC data_block)
# ?? target_link_libraries(unit_data_block PUBLIC pool_allocator)
target_link_libraries(unit_data_block PUBLIC Catch2::Catch2)

#
# Define sources for test
#
target_sources(unit_data_block PUBLIC
        main.cc
        unit_data_block.cc
)

#
# Add unit test to global suite of unit tests
#
add_test(
        NAME "unit_data_block"
        COMMAND $<TARGET_FILE:unit_data_block> --durations=yes
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)


################################################################
# Unit test for transferring data_block with task graph ports  #
################################################################

#
# Define unit test
#
add_executable(unit_blocks_and_ports EXCLUDE_FROM_ALL)

#
# Required libraries, notably the associated object libraries
#
# NOTE: This test does require ports and data_block (and 
# pool_allocator), which are currently header only libraries.
# So there is nothing to link against.
#
# target_link_libraries(unit_data_block PUBLIC ports)
# target_link_libraries(unit_data_block PUBLIC data_block)
# ?? target_link_libraries(unit_data_block PUBLIC pool_allocator)
#
target_link_libraries(unit_blocks_and_ports PUBLIC Catch2::Catch2)

#
# Define sources for test
#
target_sources(unit_blocks_and_ports PUBLIC
        main.cc
        unit_blocks_and_ports.cc
)

#
# Add unit test to global suite of unit tests
#
add_test(
        NAME "unit_blocks_and_ports"
        COMMAND $<TARGET_FILE:unit_data_block> --durations=yes
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
