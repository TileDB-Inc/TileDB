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
include(object_library)

#
# Project-wide definitions and compiler options
#
add_library(common INTERFACE)
if (TILEDB_EXPERIMENTAL_FEATURES)
  target_compile_definitions(common INTERFACE TILEDB_EXPERIMENTAL_FEATURES)
endif()

#
# Subdirectories
#
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
commence(object_library baseline)
    this_target_sources(logger.cc governor/governor.cc
        heap_profiler.cc heap_memory.cc
        exception/exception.cc exception/status.cc)
    find_package(Spdlog_EP REQUIRED)
    target_link_libraries(baseline PUBLIC spdlog::spdlog)
    target_link_libraries(baseline PUBLIC common)
conclude(object_library)

#
# `stringx` object library
#
commence(object_library stringx)
    this_target_sources(stdx_string.cc)
conclude(object_library)

if (TILEDB_TESTS)
  find_package(Catch_EP REQUIRED)
  add_executable(unit_experimental EXCLUDE_FROM_ALL)
  target_link_libraries(unit_experimental
    PRIVATE baseline
    PUBLIC Catch2::Catch2WithMain
  )
  target_sources(unit_experimental
    PUBLIC
      test/main.cc
      $<IF:$<BOOL:${TILEDB_EXPERIMENTAL_FEATURES}>,
        test/unit_is_experimental.cc,
        test/unit_is_not_experimental.cc>
  )
  add_test(
    NAME "unit_experimental"
    COMMAND $<TARGET_FILE:unit_experimental>
  )
endif()
