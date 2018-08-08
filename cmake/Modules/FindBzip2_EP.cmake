#
# FindBzip2_EP.cmake
#
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
# Finds the Bzip2 library, installing with an ExternalProject as necessary.
# This module defines:
#   - BZIP2_INCLUDE_DIR, directory containing headers
#   - BZIP2_LIBRARIES, the Bzip2 library path
#   - BZIP2_FOUND, whether Bzip2 has been found
#   - The Bzip2::Bzip2 imported target

# Include some common helper functions.
include(TileDBCommon)

# First check for a static version in the EP prefix.
find_library(BZIP2_LIBRARIES
  NAMES
    libbz2static${CMAKE_STATIC_LIBRARY_SUFFIX}
    libbz2${CMAKE_STATIC_LIBRARY_SUFFIX}
  PATHS ${TILEDB_EP_INSTALL_PREFIX}
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH
)

if (BZIP2_LIBRARIES)
  set(BZIP2_STATIC_EP_FOUND TRUE)
  find_path(BZIP2_INCLUDE_DIR
    NAMES bzlib.h
    PATHS ${TILEDB_EP_INSTALL_PREFIX}
    PATH_SUFFIXES include
    NO_DEFAULT_PATH
  )
else()
  set(BZIP2_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(BZIP2_LIBRARIES
    NAMES
      bz2 libbz2
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
  find_path(BZIP2_INCLUDE_DIR
    NAMES bzlib.h
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Bzip2
  REQUIRED_VARS BZIP2_LIBRARIES BZIP2_INCLUDE_DIR
)

if (NOT BZIP2_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Bzip2 as an external project")
    if (WIN32)
      ExternalProject_Add(ep_bzip2
        PREFIX "externals"
        URL "https://github.com/TileDB-Inc/bzip2-windows/releases/download/v1.0.6/bzip2-1.0.6.zip"
        URL_HASH SHA1=d11c3f0be92805a4c35f384845beb99eb6a96f6e
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        UPDATE_COMMAND ""
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
      )
    else()
      # We build bzip2 with -fPIC on non-Windows platforms so that we can link the static library
      # to the shared TileDB library. This allows us to avoid having to install the bzip2 libraries
      # alongside TileDB.
      ExternalProject_Add(ep_bzip2
        PREFIX "externals"
        URL "https://github.com/TileDB-Inc/bzip2/releases/download/1.0.6/bzip2-1.0.6.tar.gz"
        URL_HASH SHA1=3f89f861209ce81a6bab1fd1998c0ef311712002
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE TRUE
        BUILD_COMMAND ""
        INSTALL_COMMAND $(MAKE) CFLAGS=-fPIC PREFIX=${TILEDB_EP_INSTALL_PREFIX} install
        UPDATE_COMMAND ""
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
      )
    endif()
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_bzip2)
  else()
    message(FATAL_ERROR "Unable to find Bzip2")
  endif()
endif()

if (BZIP2_FOUND AND NOT TARGET Bzip2::Bzip2)
  add_library(Bzip2::Bzip2 UNKNOWN IMPORTED)
  set_target_properties(Bzip2::Bzip2 PROPERTIES
    IMPORTED_LOCATION "${BZIP2_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${BZIP2_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (BZIP2_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(Bzip2::Bzip2)
endif()