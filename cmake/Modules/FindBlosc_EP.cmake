#
# FindBlosc_EP.cmake
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
# Finds the Blosc library, installing with an ExternalProject as necessary.
# This module defines:
#   - BLOSC_INCLUDE_DIR, directory containing headers
#   - BLOSC_LIBRARIES, the Blosc library path
#   - BLOSC_FOUND, whether Blosc has been found
#   - The Blosc::Blosc imported target

# Include some common helper functions.
include(TileDBCommon)

# First check for a static version in the EP prefix.
find_library(BLOSC_LIBRARIES
  NAMES
    libblosc${CMAKE_STATIC_LIBRARY_SUFFIX}
  PATHS ${TILEDB_EP_INSTALL_PREFIX}
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH
)

if (BLOSC_LIBRARIES)
  set(BLOSC_STATIC_EP_FOUND TRUE)
  find_path(BLOSC_INCLUDE_DIR
    NAMES blosc.h
    PATHS ${TILEDB_EP_INSTALL_PREFIX}
    PATH_SUFFIXES include
    NO_DEFAULT_PATH
  )
else()
  set(BLOSC_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(BLOSC_LIBRARIES
    NAMES
      blosc libblosc
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
  find_path(BLOSC_INCLUDE_DIR
    NAMES blosc.h
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Blosc
  REQUIRED_VARS BLOSC_LIBRARIES BLOSC_INCLUDE_DIR
)

if (NOT BLOSC_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding BLOSC as an external project")
    ExternalProject_Add(ep_blosc
      PREFIX "externals"
      URL "https://github.com/Blosc/c-blosc/archive/v1.14.2.zip"
      URL_HASH SHA1=15a2791d36da7d84ed10816db28ebf5028235543
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_CXX_FLAGS=-fPIC
        -DCMAKE_C_FLAGS=-fPIC
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
    )

    if (WIN32)
      # blosc.{dll,lib} gets installed in lib dir; move it to bin.
      ExternalProject_Add_Step(ep_blosc move_dll
        DEPENDEES INSTALL
        COMMAND
          ${CMAKE_COMMAND} -E rename
            ${TILEDB_EP_INSTALL_PREFIX}/lib/blosc.dll
            ${TILEDB_EP_INSTALL_PREFIX}/bin/blosc.dll &&
          ${CMAKE_COMMAND} -E rename
            ${TILEDB_EP_INSTALL_PREFIX}/lib/blosc.lib
            ${TILEDB_EP_INSTALL_PREFIX}/bin/blosc.lib
      )
    endif()

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_blosc)
  else()
    message(FATAL_ERROR "Unable to find Blosc")
  endif()
endif()

# Create the imported target for Blosc
if (BLOSC_FOUND AND NOT TARGET Blosc::Blosc)
  add_library(Blosc::Blosc UNKNOWN IMPORTED)
  set_target_properties(Blosc::Blosc PROPERTIES
    IMPORTED_LOCATION "${BLOSC_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${BLOSC_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (BLOSC_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(Blosc::Blosc)
endif()