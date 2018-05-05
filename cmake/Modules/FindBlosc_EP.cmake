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

# Search the path set during the superbuild for the EP.
set(BLOSC_PATHS ${TILEDB_EP_INSTALL_PREFIX})

find_path(BLOSC_INCLUDE_DIR
  NAMES blosc.h
  PATHS ${BLOSC_PATHS}
  PATH_SUFFIXES include
  ${TILEDB_DEPS_NO_DEFAULT_PATH}
)

# Link statically if installed with the EP.
if (TILEDB_USE_STATIC_BLOSC)
  find_library(BLOSC_LIBRARIES
    NAMES
      libblosc${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${BLOSC_PATHS}
    PATH_SUFFIXES lib
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
else()
  find_library(BLOSC_LIBRARIES
    NAMES
      blosc libblosc
    PATHS ${BLOSC_PATHS}
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Blosc
  REQUIRED_VARS BLOSC_LIBRARIES BLOSC_INCLUDE_DIR
)

if (NOT BLOSC_FOUND OR TILEDB_FORCE_ALL_DEPS)
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
  set(TILEDB_USE_STATIC_BLOSC TRUE CACHE INTERNAL "")
  set(BLOSC_FOUND TRUE)
  set(BLOSC_LIBRARIES "${TILEDB_EP_INSTALL_PREFIX}/lib${LIB_SUFFIX}/libblosc${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(BLOSC_INCLUDE_DIR "${TILEDB_EP_INSTALL_PREFIX}/include")

  # INTERFACE_INCLUDE_DIRECTORIES does not allow for empty directories in config phase,
  # thus we need to create the directory. See https://cmake.org/Bug/view.php?id=15052
  file(MAKE_DIRECTORY ${BLOSC_INCLUDE_DIR})
endif()

if (BLOSC_FOUND AND NOT TARGET Blosc::Blosc)
  add_library(Blosc::Blosc UNKNOWN IMPORTED)
  set_target_properties(Blosc::Blosc PROPERTIES
    IMPORTED_LOCATION "${BLOSC_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${BLOSC_INCLUDE_DIR}"
  )
endif()
