#
# FindZlib_EP.cmake
#
# The MIT License
#
# Copyright (c) 2018 TileDB, Inc.
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
# Finds the Zlib library, installing with an ExternalProject as necessary.
# This module defines:
#   - ZLIB_INCLUDE_DIR, directory containing headers
#   - ZLIB_LIBRARIES, the Zlib library path
#   - ZLIB_FOUND, whether Zlib has been found
#   - The Zlib::Zlib imported target

# Include some common helper functions.
include(TileDBCommon)

# Search the path set during the superbuild for the EP.
set(ZLIB_PATHS ${TILEDB_EP_INSTALL_PREFIX})

# First try the builtin find module.
find_package(ZLIB QUIET ${TILEDB_DEPS_NO_DEFAULT_PATH})

# Next try finding the superbuild external project
if (NOT ZLIB_FOUND)
  find_path(ZLIB_INCLUDE_DIR
    NAMES zlib.h
    PATHS ${ZLIB_PATHS}
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  # Link statically if installed with the EP.
  find_library(ZLIB_LIBRARIES
    NAMES
      libz${CMAKE_STATIC_LIBRARY_SUFFIX}
      zlibstatic${CMAKE_STATIC_LIBRARY_SUFFIX}
      zlibstaticd${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${ZLIB_PATHS}
    PATH_SUFFIXES lib
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(Zlib
    REQUIRED_VARS ZLIB_LIBRARIES ZLIB_INCLUDE_DIR
  )
endif()

# If not found, add it as an external project
if (NOT ZLIB_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Zlib as an external project")

    if (WIN32)
      set(CFLAGS_DEF "")
    else()
      set(CFLAGS_DEF "-DCMAKE_C_FLAGS=-fPIC")
    endif()

    ExternalProject_Add(ep_zlib
      PREFIX "externals"
      URL "http://prdownloads.sourceforge.net/libpng/zlib1211.zip?download"
      URL_HASH SHA1=bccd93ad3cee39c3d08eee68d45b3e11910299f2
      DOWNLOAD_NAME "zlib1211.zip"
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_BUILD_TYPE=Release
        ${CFLAGS_DEF}
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
    )
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_zlib)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_ZLIB_EP_BUILT=TRUE
    )
  else()
    message(FATAL_ERROR "Unable to find Zlib")
  endif()
endif()

if (ZLIB_FOUND AND NOT TARGET Zlib::Zlib)
  message(STATUS "Found Zlib: ${ZLIB_LIBRARIES}")
  add_library(Zlib::Zlib UNKNOWN IMPORTED)
  set_target_properties(Zlib::Zlib PROPERTIES
    IMPORTED_LOCATION "${ZLIB_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${ZLIB_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (TILEDB_ZLIB_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(Zlib::Zlib)
endif()
