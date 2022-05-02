#
# tiledb/common/CMakeLists.txt
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

include(common-root)
include(common)

add_subdirectory(dynamic_memory)
add_subdirectory(exception)
add_subdirectory(governor)
add_subdirectory(thread_pool)
add_subdirectory(types)

if (TILEDB_TESTS)
    add_subdirectory(interval)
endif()

get_gathered(COMMON_SOURCES)
set(TILEDB_COMMON_SOURCES ${COMMON_SOURCES} PARENT_SCOPE)

#
# `baseline` object library
#
add_library(baseline OBJECT
    logger.cc governor/governor.cc heap_profiler.cc heap_memory.cc
    exception/exception.cc exception/status.cc
)
if (APPLE)
#  set_target_properties(compressors PROPERTIES "OSX_ARCHITECTURES" "arm64;x86_64")
  set_target_properties(baseline PROPERTIES "OSX_ARCHITECTURES" "x86_64;arm64")
endif()
find_package(Spdlog_EP REQUIRED)
target_link_libraries(baseline PUBLIC spdlog::spdlog)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_baseline EXCLUDE_FROM_ALL)
target_link_libraries(compile_baseline PRIVATE baseline)
target_sources(compile_baseline PRIVATE
    test/compile_baseline_main.cc $<TARGET_OBJECTS:baseline>
)

#
# `stringx` object library
#
add_library(stringx OBJECT
    stdx_string.cc
)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_stringx EXCLUDE_FROM_ALL)
target_link_libraries(compile_stringx PRIVATE stringx)
target_sources(compile_stringx PRIVATE
    test/compile_stringx_main.cc $<TARGET_OBJECTS:stringx>
)
