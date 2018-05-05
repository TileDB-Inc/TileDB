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

# Search the path set during the superbuild for the EP.
set(BZIP2_PATHS ${TILEDB_EP_INSTALL_PREFIX})

find_path(BZIP2_INCLUDE_DIR
  NAMES bzlib.h
  PATHS ${BZIP2_PATHS}
  PATH_SUFFIXES include
  ${TILEDB_DEPS_NO_DEFAULT_PATH}
)

# Link statically if installed with the EP.
if (TILEDB_USE_STATIC_BZIP2)
  find_library(BZIP2_LIBRARIES
    NAMES
      libbz2static${CMAKE_STATIC_LIBRARY_SUFFIX}
      libbz2${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${BZIP2_PATHS}
    PATH_SUFFIXES lib
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
else()
  find_library(BZIP2_LIBRARIES
    NAMES
      bz2 libbz2
    PATHS ${BZIP2_PATHS}
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Bzip2
  REQUIRED_VARS BZIP2_LIBRARIES BZIP2_INCLUDE_DIR
)

if (NOT BZIP2_FOUND OR TILEDB_FORCE_ALL_DEPS)
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
      URL "http://www.bzip.org/1.0.6/bzip2-1.0.6.tar.gz"
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
  SET(TILEDB_USE_STATIC_BZIP2 TRUE CACHE INTERNAL "")
  SET(BZIP2_FOUND TRUE)
  set(BZIP2_LIBRARIES "${TILEDB_EP_INSTALL_PREFIX}/lib${LIB_SUFFIX}/libbz2static${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(BZIP2_INCLUDE_DIR "${TILEDB_EP_INSTALL_PREFIX}/include")

  # INTERFACE_INCLUDE_DIRECTORIES does not allow for empty directories in config phase,
  # thus we need to create the directory. See https://cmake.org/Bug/view.php?id=15052
  file(MAKE_DIRECTORY ${BZIP2_INCLUDE_DIR})
endif()

if (BZIP2_FOUND AND NOT TARGET Bzip2::Bzip2)
  add_library(Bzip2::Bzip2 UNKNOWN IMPORTED)
  set_target_properties(Bzip2::Bzip2 PROPERTIES
    IMPORTED_LOCATION "${BZIP2_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${BZIP2_INCLUDE_DIR}"
  )
endif()
