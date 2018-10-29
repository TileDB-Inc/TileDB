#
# FindLZ4_EP.cmake
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
# Finds the LZ4 library, installing with an ExternalProject as necessary.
# This module defines:
#   - LZ4_INCLUDE_DIR, directory containing headers
#   - LZ4_LIBRARIES, the LZ4 library path
#   - LZ4_FOUND, whether LZ4 has been found
#   - The LZ4::LZ4 imported target

# Include some common helper functions.
include(TileDBCommon)

# First check for a static version in the EP prefix.
find_library(LZ4_LIBRARIES
  NAMES
    liblz4${CMAKE_STATIC_LIBRARY_SUFFIX}
    liblz4_static${CMAKE_STATIC_LIBRARY_SUFFIX}
    lz4${CMAKE_STATIC_LIBRARY_SUFFIX}
  PATHS ${TILEDB_EP_INSTALL_PREFIX}
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH
)

if (LZ4_LIBRARIES)
  set(LZ4_STATIC_EP_FOUND TRUE)
  find_path(LZ4_INCLUDE_DIR
    NAMES lz4.h
    PATHS ${TILEDB_EP_INSTALL_PREFIX}
    PATH_SUFFIXES include
    NO_DEFAULT_PATH
  )
else()
  set(LZ4_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(LZ4_LIBRARIES
    NAMES
      lz4 liblz4
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
  find_path(LZ4_INCLUDE_DIR
    NAMES lz4.h
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LZ4
  REQUIRED_VARS LZ4_LIBRARIES LZ4_INCLUDE_DIR
)

if (NOT LZ4_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding LZ4 as an external project")
    if (WIN32)
      set(ARCH_SPEC -A X64)
    endif()
    set(LZ4_CMAKE_DIR "${TILEDB_EP_SOURCE_DIR}/ep_lz4/contrib/cmake_unofficial")
    ExternalProject_Add(ep_lz4
      PREFIX "externals"
      URL "https://github.com/lz4/lz4/archive/v1.8.2.zip"
      URL_HASH SHA1=ebf6c227965318ecd73820ade8f5dbd83d48b3e8
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND}
          ${ARCH_SPEC}
          -DLZ4_BUILD_LEGACY_LZ4C=OFF
          -DLZ4_POSITION_INDEPENDENT_LIB=ON
          -DBUILD_SHARED_LIBS=OFF
          -DBUILD_STATIC_LIBS=ON
          -DCMAKE_BUILD_TYPE=Release
          -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
          ${LZ4_CMAKE_DIR}
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
    )
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_lz4)
  else()
    message(FATAL_ERROR "Unable to find LZ4")
  endif()
endif()

if (LZ4_FOUND AND NOT TARGET LZ4::LZ4)
  add_library(LZ4::LZ4 UNKNOWN IMPORTED)
  set_target_properties(LZ4::LZ4 PROPERTIES
    IMPORTED_LOCATION "${LZ4_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (LZ4_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(LZ4::LZ4)
endif()
