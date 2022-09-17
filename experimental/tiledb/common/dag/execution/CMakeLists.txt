#
# experimental/tiledb/common/execution/CMakeLists.txt
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
# Define the threadpool object library                         #
################################################################

list(APPEND THREADPOOL_SOURCES
  threadpool.cc
)
gather_sources(${THREADPOOL_SOURCES})

#
# Object library for other units to depend upon
#
add_library(threadpool OBJECT ${THREADPOOL_SOURCES})

#
# Test-compile of object library ensures link-completeness
#
# NOTE: threadpool depends on bounded_buffer object library,
# which is header only.
#
add_executable(compile_threadpool EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_threadpool)
target_link_libraries(compile_threadpool PRIVATE threadpool)
target_sources(compile_threadpool PRIVATE test/compile_threadpool_main.cc)


################################################################
# Define the stop_token object library                         #
################################################################

list(APPEND STOP_TOKEN_SOURCES
  stop_token.cc
)
gather_sources(${STOP_TOKEN_SOURCES})

#
# Object library for other units to depend upon
#
add_library(stop_token OBJECT ${STOP_TOKEN_SOURCES})

#
# Test-compile of object library ensures link-completeness
#
# NOTE: stop_token is header only
#
add_executable(compile_stop_token EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_stop_token)
target_link_libraries(compile_stop_token PRIVATE stop_token)
target_sources(compile_stop_token PRIVATE test/compile_stop_token_main.cc)


################################################################
# Define the bountiful object library                          #
################################################################

list(APPEND BOUNTIFUL_SOURCES
  bountiful.cc
)
gather_sources(${BOUNTIFUL_SOURCES})

#
# Object library for other units to depend upon
#
add_library(bountiful OBJECT ${BOUNTIFUL_SOURCES})

#
# Test-compile of object library ensures link-completeness
#
# NOTE: bountiful is header only
#
add_executable(compile_bountiful EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_bountiful)
target_link_libraries(compile_bountiful PRIVATE bountiful)
target_sources(compile_bountiful PRIVATE test/compile_bountiful_main.cc)
