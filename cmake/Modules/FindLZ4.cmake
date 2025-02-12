#
# FindLZ4.cmake
#
#
# The MIT License
#
# Copyright (c) 2024 TileDB, Inc.
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
# Finds the LZ4 library.
# This module defines:
#   - The lz4::lz4 imported target

find_package(lz4 CONFIG)

# The casing of the imported target's name differs. In the official CMake build system of
# lz4, it is called LZ4::lz4 (not yet released, see LZ4 pull requests 1372, 1413), while
# vcpkg uses its own build system called lz4::lz4, and Conda does not use CMake at all.
# Once lz4 releases a new version with these PRs merged, and vcpkg and Conda update to
# that version, and we update our vcpkg baseline to that version, we can simplify this logic.
if (lz4_FOUND)
  if(TARGET lz4::lz4)
    return()
  elseif(TARGET LZ4::lz4)
    add_library(lz4::lz4 ALIAS LZ4::lz4)
    return()
  endif()
endif()

# Package not found, search in system paths.
find_library(LZ4_LIBRARIES
  NAMES
    lz4 liblz4
  PATH_SUFFIXES lib bin
)

find_path(LZ4_INCLUDE_DIR
  NAMES lz4.h
  PATH_SUFFIXES include
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LZ4
  REQUIRED_VARS LZ4_LIBRARIES LZ4_INCLUDE_DIR
)

if (LZ4_FOUND AND NOT TARGET lz4::lz4)
  add_library(lz4::lz4 UNKNOWN IMPORTED)
  set_target_properties(lz4::lz4 PROPERTIES
    IMPORTED_LOCATION "${LZ4_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}"
  )
endif()
