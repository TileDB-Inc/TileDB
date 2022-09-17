#
# experimental/tiledb/common/execution/test/CMakeLists.txt
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


################################################################
# Unit test for threadpool                                     #
################################################################

#
# Define unit test
#
add_executable(unit_threadpool EXCLUDE_FROM_ALL)

#
# Required libraries, notably the associated object library
#
# NOTE: threadpool and bounded_buffer are header-only
#
# target_link_libraries(unit_edge PUBLIC threadpool)
# target_link_libraries(unit_edge PUBLIC bounded_buffer)
target_link_libraries(unit_threadpool PUBLIC Catch2::Catch2)

#
# Define sources for test
#
target_sources(unit_threadpool PUBLIC
    main.cc
    unit_threadpool.cc
)

#
# Add unit test to global suite of unit tests
#
add_test(
    NAME "unit_threadpool"
    COMMAND $<TARGET_FILE:unit_threadpool> --durations=yes
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

################################################################
# Unit test for stop_token                                     #
################################################################

#
# Define unit test
#
add_executable(unit_stop_token EXCLUDE_FROM_ALL)

#
# Required libraries, notably the associated object library
#
# NOTE: stop_token and bounded_buffer are header-only
#
# target_link_libraries(unit_edge PUBLIC stop_token)
# target_link_libraries(unit_edge PUBLIC bounded_buffer)
target_link_libraries(unit_stop_token PUBLIC Catch2::Catch2)


#
# Define sources for test
#
target_sources(unit_stop_token PUBLIC
    main.cc
    unit_stop_token.cc
)

#
# Sources for code elsewhere required for tests
#
target_sources(unit_stop_token PUBLIC ${DEPENDENT_SOURCES})

#
# Add unit test to global suite of unit tests
#
add_test(
    NAME "unit_stop_token"
    COMMAND $<TARGET_FILE:unit_stop_token> --durations=yes
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)


if (YACK)


if (TILEDB_TESTS)
    add_executable(unit_bountiful EXCLUDE_FROM_ALL)
    target_link_libraries(unit_bountiful PUBLIC execution)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_bountiful PUBLIC Catch2::Catch2)
#    target_link_libraries(unit_bountiful PUBLIC $<TARGET_OBJECTS:thread_pool>)



    # Sources for tests
    target_sources(unit_bountiful PUBLIC
            test/main.cc
            test/unit_bountiful.cc
            )

    add_test(
            NAME "unit_bountiful"
            COMMAND $<TARGET_FILE:unit_bountiful> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}

	    )

endif()

endif()
