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

# Expects invocation with -DCMAKE_PREFIX_PATH=<directory-of-tiledb-installation>

cmake_minimum_required(VERSION 3.21)

project(Test_TileDBConfigVersion_h)

# Parse version numbers out of tiledb_version.h file

find_file(
        tiledb_version_h tiledb_version.h
        PATH_SUFFIXES "include/tiledb" REQUIRED
)
file(READ ${tiledb_version_h} ver)

string(REGEX MATCH "TILEDB_VERSION_MAJOR ([0-9]*)" _ ${ver})
set(ver_major ${CMAKE_MATCH_1})

string(REGEX MATCH "TILEDB_VERSION_MINOR ([0-9]*)" _ ${ver})
set(ver_minor ${CMAKE_MATCH_1})

string(REGEX MATCH "TILEDB_VERSION_PATCH ([0-9]*)" _ ${ver})
set(ver_patch ${CMAKE_MATCH_1})

set(VERSION "${ver_major}.${ver_minor}.${ver_patch}")

set(count_unacceptable 0)
find_package(TileDB "${ver_major}.${ver_minor}" )
if (NOT TileDB_FOUND)
  message(STATUS "BAD: Unable to find expected TileDB M.m version!")
  math(EXPR count_unacceptable "${count_unacceptable}+1")
else()
  message(STATUS "found TileDB satisfying version ${ver_major}.${ver_minor}!")
endif()

find_package(TileDB "${ver_major}.${ver_minor}.${ver_patch}" EXACT)
if (NOT TileDB_FOUND)
  message(STATUS "Unable to find expected TileDB M.m.P version!")
  math(EXPR count_unacceptable "${count_unacceptable}+1")
else()
  message(STATUS "found TileDB version ${ver_major}.${ver_minor}.${ver_patch}!")
endif()

find_package(TileDB "${ver_major}.${ver_minor}.${ver_patch}")
if (NOT TileDB_FOUND)
  message(STATUS "Failed to find TileDB for version ${ver_major}.${ver_minor}.${ver_patch}!")
  math(EXPR count_unacceptable "${count_unacceptable}+1")
else()
  message(STATUS "FOUND TileDB satisfying version ${ver_major}.${ver_minor}.${ver_patch}!")
endif()
unset(TileDB_FOUND)

if(count_unacceptable)
  message(FATAL_ERROR "${count_unacceptable} unexpected result encountered!")
endif()

