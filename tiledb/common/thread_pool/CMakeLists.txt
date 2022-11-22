#
# tiledb/common/thread_pool/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021 TileDB, Inc.
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
include(object_library)

list(APPEND SOURCES
    thread_pool.cc
)
gather_sources(${SOURCES})

#
# `thread_pool` object library
#
commence(object_library thread_pool)
    this_target_sources(${SOURCES})
    this_target_object_libraries(baseline)
conclude(object_library)

if (TILEDB_TESTS)
    add_executable(unit_thread_pool EXCLUDE_FROM_ALL)
    target_link_libraries(unit_thread_pool PUBLIC thread_pool)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_thread_pool PUBLIC Catch2::Catch2WithMain)
    target_link_libraries(unit_thread_pool PUBLIC $<TARGET_OBJECTS:cancelable_tasks>)

    # Sources for code elsewhere required for tests
    target_sources(unit_thread_pool PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_thread_pool PUBLIC
            test/main.cc
            test/unit_thread_pool.cc
            )

    add_test(
            NAME "unit_thread_pool"
            COMMAND $<TARGET_FILE:unit_thread_pool> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
