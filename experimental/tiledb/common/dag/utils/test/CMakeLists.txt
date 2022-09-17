#
# experimental/tiledb/common/dag/utils/test/CMakeLists.txt
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


#
# Define unit tests 
#
dag_add_header_only_unit_test(bounded_buffer)
dag_add_header_only_unit_test(range_join)
dag_add_header_only_unit_test(spinlock)





if (YACK)
list(APPEND SOURCES
  range_join.cc
  spinlock.cc
)
gather_sources(${SOURCES})

#
# Object library for other units to depend upon
#
add_library(utils OBJECT ${SOURCES})
# target_link_libraries(utils PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_utils EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_utils)
target_link_libraries(compile_utils PRIVATE utils)
target_sources(compile_utils PRIVATE test/compile_utils_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_range_join EXCLUDE_FROM_ALL)
    target_link_libraries(unit_range_join PUBLIC utils)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_range_join PUBLIC Catch2::Catch2)
#    target_link_libraries(unit_range_join PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_range_join PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_range_join PUBLIC
            test/main.cc
            test/unit_range_join.cc
            )

    add_test(
            NAME "unit_range_join"
            COMMAND $<TARGET_FILE:unit_range_join> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )

    add_executable(unit_spinlock EXCLUDE_FROM_ALL)
    target_link_libraries(unit_spinlock PUBLIC utils)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_spinlock PUBLIC Catch2::Catch2)
#    target_link_libraries(unit_spinlock PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_spinlock PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_spinlock PUBLIC
            test/main.cc
            test/unit_spinlock.cc
            )

    add_test(
            NAME "unit_spinlock"
            COMMAND $<TARGET_FILE:unit_spinlock> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )

endif()


#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_bounded_buffer EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_bounded_buffer)
target_sources(compile_bounded_buffer PRIVATE test/compile_bounded_buffer_main.cc)

if (TILEDB_TESTS)

    add_executable(unit_bounded_buffer EXCLUDE_FROM_ALL)
    target_link_libraries(unit_bounded_buffer PUBLIC utils)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_bounded_buffer PUBLIC Catch2::Catch2)
#    target_link_libraries(unit_bounded_buffer PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
#    target_sources(unit_bounded_buffer PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_bounded_buffer PUBLIC
            test/unit_bounded_buffer.cc
            )

    add_test(
            NAME "unit_bounded_buffer"
            COMMAND $<TARGET_FILE:unit_bounded_buffer> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )

endif()
endif()
