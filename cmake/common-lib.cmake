#
# cmake/common-lib.cmake
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
############################################################
# Common build environment
############################################################
#
# This file supports common initialization of the CMake build environment across
# disparate directories, each with a CmakeLists.txt file capable of independent
# build. We support these use cases for build:
#
#   1. Compilation of a minimal set of source files for unit testing. A well-
#      designed unit is relatively self-contained. Building a separate
#      executable without relying on the "whole library" context encourages good
#      design.
#
#   2. Independent compilation of external dependencies. As of this writing, the
#      only way to compile external dependencies is to run the entire
#      superbuild. With the ability to set up a common build environment, it is
#      possible to run builds on these dependencies one at a time.
#        a. When working on the library code, this allows incremental update of
#           a particular build environment.
#        b. When working on an update to an external dependency, this allows
#           independent and isolated development of the build for that
#           particular dependency.
#
#   3. Separate compilation of defect-replication executables. A common facility
#      allows other executables to be added "to the side of" the source tree
#      without needing to modify the source tree in any way, particularly
#      without changing the root CMakeLists.txt
#
#   4. Compilation of examples.
#
# The common facilities here provide build-independence, but not metabuild-
# independence. This means that any individual target can be built with its
# minimal dependencies, but that the metabuild files (CMakeLists.txt) are meant
# to be built all together as a whole. One of the consequences of this is that
# CMake also uses a common build directory for the metabuild. This usually
# should not cause a problem, but it means that, for example, experiments with
# common configuration are limited to one-at-a-time per build-directory.
#
# Common environment elements fall into two categories, depending on whether
# they depend on the calling directory or not. Compiler flags that set up
# directory paths usually depend on the calling directory. Other flags, such as
# those about code generation and warnings, usually do not.
#
############################################################
# Usage
############################################################
#
# The common build environment is implemented as a number of CMake modules,
# each with its own `*.cmake` file.
#
# 1. The module `common` is included in the CMakeLists.txt file for each
#    individual unit. It sets up all the housekeeping all a unit to integrate
#    itself into a larger context easily.
# 2. The module `common-root` is included at some super-directory of the units.
#    It provides functions for setting up a context for the units in its
#    subdirectories
# 3. This module `common-lib` contains function definitions that support the
#    modules `common` and `common-root`.
# 4. The module `accumulator` contains functions that define a (pseudo-)class.
#
############################################################
# Requirements for the common environment
############################################################
#
# 1. `CMAKE_MODULE_PATH` must include the directory in which this file is
#    present. This allows this file to find its own included files.
#
# 2. The variable `CMAKE_SOURCE_DIR` is taken to be the root of the entire
#    source tree.
#
#    a. CMake does not distinguish between "the root of an entire source tree"
#       and "the root of the source tree of the project". Achieving metabuild-
#       independence would require getting to root of the source tree as a whole
#       with some kind of search-upward-through-parent-directory algorithm. No
#       such code is present at the current time.
#
#    b. This variable does not appear in the cache. Its value, though, is
#       present in the cache as `CMAKE_HOME_DIRECTORY`, a name not recommended
#       for CMake code.
#
# 3. The variable `CMAKE_PROJECT_NAME` is used as a sanity check. At the current
#    time only "TileDB" and "TileDB-Superbuild" are accepted as valid. This
#    check requires that all common setup happen at the top level and that the
#    `project()` command occur before adding any subdirectories.
#
#    a. Both of the valid projects use the source root as their source
#       directory. If that ever changes, the setup for this file will have to
#       change accordingly.
#
############################################################

include_guard()
include(accumulator)

#
# gathering_point()
#
# Define the current directory as a gathering point for sources under its
# control. At the present time each directory support only a single, anonymous
# gathering point
#
function(gathering_point)
    define_accumulator(GATHERED_SOURCES)
endfunction()

#
# gather_sources( <relative_source_1> [...] )
#
# Gather sources in the current directory by their absolute file names
#
# Limitation for CMake pre-3.20: Sources in SOURCE_LIST must be passed with
# paths relative to their directory. CMake pre-3.20 does offer any reliable
# way to allow this function to mix relative and absolute path names
#
function(gather_sources)
    set(SOURCES_ABS)
    foreach(SOURCE_RELATIVE IN LISTS ARGN)
        list(APPEND SOURCES_ABS "${CMAKE_CURRENT_SOURCE_DIR}/${SOURCE_RELATIVE}")
    endforeach()
    put_into(ACCUMULATOR GATHERED_SOURCES LIST ${SOURCES_ABS})
endfunction()

#
# get_gathered( <variable> )
#
# Sets a variable to a list value of all the sources gathered at the current
# gathering point
#
function(get_gathered VAR)
    retrieve_from(X ACCUMULATOR GATHERED_SOURCES)
    set(${VAR} ${X} PARENT_SCOPE)
endfunction()

#
# Conditionally include the test subdirectory
#
function(add_test_subdirectory)
  if (TILEDB_TESTS)
    add_subdirectory(test)
  endif()
endfunction()
