#
# CMakeLists.txt
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

# Expects invocation wtih -DCMAKE_PREFIX_PATH=<directory-of-tiledb-installation>

cmake_minimum_required(VERSION 3.21)

project(Test_TileDBConfigVersion_h NONE)

if (NOT CMAKE_PREFIX_PATH)
  message(FATAL_ERROR "Need parameter -DCMAKE_PREFIX_PATH=<tiledb-installation-path>")
else()
  if (NOT EXISTS "${CMAKE_PREFIX_PATH}/include/TileDB/tiledb_version.h")
    message(FATAL_ERROR "Missing tiledb_version.h at ${CMAKE_PREFIX_PATH}")
  endif()
endif()

# Parse version numbers out of tiledb_version.h file

file(STRINGS ${CMAKE_PREFIX_PATH}/include/TileDB/tiledb_version.h
  version_h_lines
)


set(SEARCH_VARIABLES "TILEDB_VERSION_MAJOR" "TILEDB_VERSION_MINOR" "TILEDB_VERSION_PATCH")
foreach(version_h_line ${version_h_lines})
    foreach(_substitution_variable ${SEARCH_VARIABLES})
        string(TOUPPER ${_substitution_variable} _substitution_variable_upper)
        set(${_substitution_variable_upper}_LEN 0)
        if (${${_substitution_variable_upper}})
          string(LENGTH ${${_substitution_variable_upper}} ${_substitution_variable_upper}_LEN)
        endif()
        if (NOT ${_substitution_variable_upper}_LEN)
            string(REGEX MATCH "#define[ \t]+${_substitution_variable}[ \t]+([0-9]+)" MACTHED_STRING ${version_h_line})
            if (MACTHED_STRING)
              string(LENGTH ${CMAKE_MATCH_1} CMAKE_MATCH_1_LEN)
              message(STATUS "found len ${CMAKE_MATCH_1_LEN}, \"${CMAKE_MATCH_1}\" in ${version_h_line}")
                set(${_substitution_variable_upper} ${CMAKE_MATCH_1})
            endif()
        endif()
    endforeach()
endforeach()

message(STATUS "TILEDB_VERSION_MAJOR is \"${TILEDB_VERSION_MAJOR}\"")
message(STATUS "TILEDB_VERSION_MINOR is \"${TILEDB_VERSION_MINOR}\"")
message(STATUS "TILEDB_VERSION_PATCH is \"${TILEDB_VERSION_PATCH}\"")

set(count_unacceptable 0)
find_package(TileDB "${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}" )
if (NOT TileDB_FOUND)
  message(STATUS "BAD: Unable to find expected TileDB M.m version!")
  math(EXPR count_unacceptable "${count_unacceptable}+1")
else()
  message(STATUS "found TileDB satisfying version ${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}!")
endif()

find_package(TileDB "${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}.${TILEDB_VERSION_PATCH}" EXACT)
if (NOT TileDB_FOUND)
  message(STATUS "Unable to find expected TileDB M.m.P version!")
  math(EXPR count_unacceptable "${count_unacceptable}+1")
else()
  message(STATUS "found TileDB version ${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}.${TILEDB_VERSION_PATCH}!")
endif()

find_package(TileDB "${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}.${TILEDB_VERSION_PATCH}")
if (NOT TileDB_FOUND)
  message(STATUS "Failed to find TileDB for version ${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}.${TILEDB_VERSION_PATCH}!")
  math(EXPR count_unacceptable "${count_unacceptable}+1")
else()
  message(STATUS "FOUND TileDB satisfying version ${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}.${TILEDB_VERSION_PATCH}!")
endif()
unset(TileDB_FOUND)

if(count_unacceptable)
  message(FATAL_ERROR "${count_unacceptable} unexpected result encountered!")
endif()

