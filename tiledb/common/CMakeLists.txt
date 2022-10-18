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
add_library(baseline OBJECT
    logger.cc governor/governor.cc heap_profiler.cc heap_memory.cc
    exception/exception.cc exception/status.cc uuid.cc
)
if(WIN32)
    if(MSVC)
        find_library(RPCRT4_LIBRARY rpcrt4)
        message(STATUS "Found Win32 lib bcrypt: ${RPCRT4_LIBRARY}")
        target_link_libraries(baseline PUBLIC ${RPCRT4_LIBRARY})
    else()
        message(STATUS "Linking to Win32 lib rpcrt4")
        target_link_libraries(baseline PUBLIC -lrpcrt4)
    endif()
else()
    find_package(OpenSSL_EP REQUIRED)
    target_link_libraries(baseline PUBLIC OpenSSL::Crypto)
endif()
find_package(Spdlog_EP REQUIRED)
target_link_libraries(baseline PUBLIC spdlog::spdlog)
target_link_libraries(baseline PUBLIC common)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_baseline EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_baseline)
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
add_dependencies(all_link_complete compile_stringx)
target_link_libraries(compile_stringx PRIVATE stringx)
target_sources(compile_stringx PRIVATE
    test/compile_stringx_main.cc $<TARGET_OBJECTS:stringx>
)

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
