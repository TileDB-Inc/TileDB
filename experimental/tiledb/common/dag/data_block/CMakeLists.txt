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

################################################################
# Include subdirectory with unit tests                         #
################################################################
if (TILEDB_TESTS)
    #
    # All unit tests depend on Catch
    #
    find_package(Catch_EP REQUIRED)
    add_subdirectory(test)
endif()


################################################################
# Define the pool_allocator object library                     #
################################################################

#
# Sources for the object library
#
list(APPEND POOL_SOURCES
    pool_allocator.cc
)
gather_sources(${POOL_SOURCES})

#
# Object library for other units to depend upon
#
add_library(pool_allocator OBJECT ${POOL_SOURCES})

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_pool_allocator EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_pool_allocator)
target_link_libraries(compile_pool_allocator PRIVATE pool_allocator)
target_sources(compile_pool_allocator PRIVATE test/compile_pool_allocator_main.cc)


################################################################
# Define the data_block object library                         #
################################################################

#
# Sources for the object library
#
list(APPEND BLOCK_SOURCES
    data_block.cc
    pool_allocator.cc
)
gather_sources(${BLOCK_SOURCES})

#
# Object library for other units to depend upon
#
add_library(data_block OBJECT ${BLOCK_SOURCES})

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_data_block EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_data_block)
target_link_libraries(compile_data_block PRIVATE data_block)
target_sources(compile_data_block PRIVATE test/compile_data_block_main.cc)

