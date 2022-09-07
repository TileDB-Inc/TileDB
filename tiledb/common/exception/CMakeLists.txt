#
# tiledb/common/exception/CMakeLists.txt
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
    exception.cc status.cc
)
gather_sources(${SOURCES})

if (TILEDB_TESTS)
    find_package(Catch_EP REQUIRED)

    add_executable(unit_exception EXCLUDE_FROM_ALL)
    target_link_libraries(unit_exception PUBLIC Catch2::Catch2WithMain)
    target_include_directories(unit_exception 
      PUBLIC
      ${TILEDB_EP_INSTALL_PREFIX}/include
      )
    target_link_libraries(unit_exception PUBLIC baseline)

    # Sources for code under test
    target_sources(unit_exception PUBLIC
      # Don't use the SOURCES variable because the files are already in the
      # baseline object library
      # ${SOURCES}
    )

    # Sources for tests
    target_sources(unit_exception PUBLIC
      test/main.cc
      test/unit_exception.cc
    )

    add_test(
            NAME "unit_exception"
            COMMAND $<TARGET_FILE:unit_exception>
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
