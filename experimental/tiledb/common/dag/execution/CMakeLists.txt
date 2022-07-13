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

list(APPEND SOURCES
  threadpool.cc
)
gather_sources(${SOURCES})

#
# Object library for other units to depend upon
#
add_library(execution OBJECT ${SOURCES})
target_link_libraries(execution PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_execution EXCLUDE_FROM_ALL)
target_link_libraries(compile_execution PRIVATE execution)
target_sources(compile_execution PRIVATE test/compile_execution_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_execution EXCLUDE_FROM_ALL)
    target_link_libraries(unit_execution PUBLIC execution)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_execution PUBLIC Catch2::Catch2)
    target_link_libraries(unit_execution PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_execution PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_execution PUBLIC
            test/main.cc
            test/unit_execution.cc
            )

    add_test(
            NAME "unit_execution"
            COMMAND $<TARGET_FILE:unit_execution> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
	    )

    add_executable(unit_threadpool EXCLUDE_FROM_ALL)
    target_link_libraries(unit_threadpool PUBLIC execution)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_threadpool PUBLIC Catch2::Catch2)
    target_link_libraries(unit_threadpool PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_threadpool PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_threadpool PUBLIC
            test/main.cc
            test/unit_threadpool.cc
            )

    add_test(
            NAME "unit_threadpool"
            COMMAND $<TARGET_FILE:unit_threadpool> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}

	    )

endif()
