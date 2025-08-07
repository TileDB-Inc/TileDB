#
# tiledb/common/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021-2025 TileDB, Inc.
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
if (TILEDB_RUST)
  add_dependencies(common rust_h)
  target_include_directories(common INTERFACE ${TILEDB_OXIDIZE_INCLUDE})
endif ()

#
# Subdirectories
#
add_subdirectory(algorithm)
add_subdirectory(dynamic_memory)
add_subdirectory(evaluator)
add_subdirectory(exception)
add_subdirectory(filesystem)
add_subdirectory(governor)
add_subdirectory(interval)
add_subdirectory(random)
add_subdirectory(thread_pool)
add_subdirectory(types)
add_subdirectory(util)

get_gathered(COMMON_SOURCES)
set(TILEDB_COMMON_SOURCES ${COMMON_SOURCES} PARENT_SCOPE)

add_library(assert_header INTERFACE)
    # can be removed when c++20 `<format>` is consistently supported
find_package(fmt REQUIRED)
target_link_libraries(assert_header INTERFACE fmt::fmt)

#
# `assert` object library
#
commence(object_library assert)
    this_target_sources(assert.cc)
    target_link_libraries(assert PUBLIC assert_header)
conclude(object_library)

#
# `baseline` object library
#
commence(object_library baseline)
    this_target_sources(
        logger.cc
        dynamic_memory/dynamic_memory.cc
        exception/exception.cc
        exception/status.cc
        governor/governor.cc
        heap_profiler.cc
        heap_memory.cc
        memory_tracker.cc
        pmr.cc
    )
    if (TILEDB_USE_CPP17_PMR)
      this_target_sources(polymorphic_allocator/polymorphic_allocator.cc)
    endif()
    find_package(nlohmann_json REQUIRED)
    find_package(spdlog REQUIRED)
    target_link_libraries(baseline PUBLIC nlohmann_json::nlohmann_json)
    target_link_libraries(baseline PUBLIC spdlog::spdlog)
    target_link_libraries(baseline PUBLIC common)
conclude(object_library)

#
# `stringx` object library
#
commence(object_library stringx)
    this_target_sources(stdx_string.cc)
conclude(object_library)

add_test_subdirectory()
