#
# tiledb/sm/rtree/CMakeLists.txt
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

#
# Object library for other units to depend upon
#
add_library(rtree OBJECT rtree.cc)
target_link_libraries(rtree PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(rtree PUBLIC dimension $<TARGET_OBJECTS:dimension>)
target_link_libraries(rtree PUBLIC domain $<TARGET_OBJECTS:domain>)
target_link_libraries(rtree PUBLIC math $<TARGET_OBJECTS:math>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_rtree EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_rtree)
target_link_libraries(compile_rtree PRIVATE rtree)
target_sources(compile_rtree PRIVATE
    test/compile_rtree_main.cc $<TARGET_OBJECTS:rtree>
)

if (TILEDB_TESTS)
  add_subdirectory(test)
endif()
