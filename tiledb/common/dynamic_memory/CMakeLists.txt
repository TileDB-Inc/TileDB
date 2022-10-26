#
# tiledb/common/dynamic_memory/CMakeLists.txt
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

list(APPEND SOURCES
    dynamic_memory.cc
)
gather_sources(${SOURCES})

list(APPEND DEPENDENT_SOURCES
    ../governor/governor.cc
    ../heap_profiler.cc
)

if (TILEDB_TESTS)
    find_package(Catch_EP REQUIRED)

    add_executable(unit_dynamic_memory EXCLUDE_FROM_ALL)
    target_link_libraries(unit_dynamic_memory PUBLIC Catch2::Catch2WithMain)
    # Link to Threads::Threads if library or flag needed to enable threading.
    # The heap_profiler dependency requires threads. The dependency should be
    # moved to heap_profiler if/when it is added to its own object library.
    if (CMAKE_THREAD_LIBS_INIT)
        target_link_libraries(unit_dynamic_memory PUBLIC Threads::Threads)
    endif()

    # Sources for code under test
    target_sources(unit_dynamic_memory PUBLIC ${SOURCES})

    # Sources for required code
    target_sources(unit_dynamic_memory PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_dynamic_memory PUBLIC
        test/main.cc
        test/unit_dynamic_memory.cc
    )

    add_test(
        NAME "unit_dynamic_memory"
        COMMAND $<TARGET_FILE:unit_dynamic_memory>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_dependencies(check unit_dynamic_memory)
endif()

# TODO: Implementation of allocator_unique

# TODO: Provenance with copy constructor
# TODO: Provenance with select_on_copy_construction

# TODO: TracedAllocator variant with its own std::string storage
