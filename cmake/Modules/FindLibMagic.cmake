#
# FindLibMagic.cmake
#
# The MIT License
#
# Copyright (c) 2018-2021 TileDB, Inc.
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
# Finds the Magic library
# This module defines:
#   - libmagic imported target
#   - libmagic_DICTIONARY, location to magic.mgc

# Include some common helper functions.
include(TileDBCommon)

# The vcpkg libmagic port exports an unofficial CMake package. Try to find it first.
find_package(unofficial-libmagic CONFIG)
if (unofficial-libmagic_FOUND)
  set(libmagic_DICTIONARY ${unofficial-libmagic_DICTIONARY})
else()
  # If the unofficial package is not found, try finding libmagic using the standard find commands.
  find_path(libmagic_INCLUDE_DIR NAMES magic.h)
  find_library(libmagic_LIBRARIES magic)
  find_file(libmagic_DICTIONARY magic.mgc
    PATH_SUFFIXES share/libmagic/misc share/misc
  )

  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(libmagic
    REQUIRED_VARS
      libmagic_INCLUDE_DIR
      libmagic_LIBRARIES
      libmagic_DICTIONARY
  )

  add_library(unofficial::libmagic::libmagic UNKNOWN IMPORTED)
  set_target_properties(unofficial::libmagic::libmagic PROPERTIES
    IMPORTED_LOCATION "${libmagic_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${libmagic_INCLUDE_DIR}"
  )
endif()
add_library(libmagic ALIAS unofficial::libmagic::libmagic)
