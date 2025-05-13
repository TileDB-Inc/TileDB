#
# cmake/common.cmake
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
#
############################################################
# Usage
############################################################
#
# ```
# include(common NO_POLICY_SCOPE)
# ```
#
# The first line of each CMakeLists.txt should be a single `include` commond. In
# order for this to work, the CMake variable `CMAKE_MODULE_PATH` must be set
# prior to the command. In ordinary use, this will be set explicitly at the top
# level and then be visible to any other CMakeLists.txt file incorporated with
# `add_subdirectory`. It is also possible to specify this variable with `-D` on
# the command line, but this won't achieve metabuild-independence in most cases.
#
# Note 1. It's `common` and not `common.cmake`. In order for the module path
#    to work, it needs a module name and not a file name.
#
# Note 2. NO_POLICY_SCOPE is required so that the common module can set policies
#    in the scope of the caller. If this is not set, policies are set only
#    for the scope of the module and then discarded.
#
############################################################
# Requirements for this file
############################################################
#
# 1. The variable `CMAKE_PROJECT_NAME` is used as a sanity check. At the current
#    time only "TileDB" and "TileDB-Superbuild" are accepted as valid. This
#    check requires that all common setup happen at the top level and that the
#    `project()` command occur before adding any subdirectories.
#
#    a. Both of the valid projects use the source root as their source
#       directory. If that ever changes, the setup for this file will have to
#       change accordingly.
#
############################################################

include(common-lib)

#
# Must include common.cmake after project() has been called.
#
if (NOT DEFINED CMAKE_PROJECT_NAME)
    message(FATAL "No CMake project name is defined. Call `project()` before including this file.")
endif()
#
# Only certain projects are allowed currently. Since we don't currently have the
# ability to locate the source root from an arbitrary subdirectory, this check
# ensures that we inform the user that what they're trying won't work.
#
if (NOT ${CMAKE_PROJECT_NAME} EQUAL "TileDB" AND ${CMAKE_PROJECT_NAME} EQUAL "TileDB-Superbuild")
    message(FATAL "Only projects \"TileDB\" and \"TileDB-Superbuild\" are supported at present.")
endif()
#
# This is a sanity check to ensure that the `project()` command sets the source
# directory.
#
if (NOT DEFINED CMAKE_SOURCE_DIR)
    message(FATAL Huh? A project is defined but there's no top-level source directory.)
endif()

#
# Set include path to the root of the source tree.
#
include_directories(${CMAKE_SOURCE_DIR})
cmake_path(SET TILEDB_SOURCE_ROOT NORMALIZE ${CMAKE_SOURCE_DIR})
cmake_path(APPEND TILEDB_SOURCE_ROOT "external/include" OUTPUT_VARIABLE TILEDB_EXTERNAL_INCLUDE)


#
# Add Rust-generated header files to the include path
#
set(TILEDB_OXIDIZE_INCLUDE tiledb/oxidize/include PARENT_SCOPE)
include_directories(${CMAKE_BINARY_DIR}/${TILEDB_OXIDIZE_INCLUDE})
