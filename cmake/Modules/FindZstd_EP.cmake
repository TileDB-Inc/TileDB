#
# FindZstd_EP.cmake
#
#
# The MIT License
#
# Copyright (c) 2017-2018 TileDB, Inc.
# Copyright (c) 2016 MIT and Intel Corporation
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
# Finds the Zstd library, installing with an ExternalProject as necessary.
# This module defines:
#   - ZSTD_INCLUDE_DIR, directory containing headers
#   - ZSTD_LIBRARIES, the Zstd library path
#   - ZSTD_FOUND, whether Zstd has been found
#   - The Zstd::Zstd imported target

# Include some common helper functions.
include(TileDBCommon)

# First check for a static version in the EP prefix.
find_library(ZSTD_LIBRARIES
  NAMES
    libzstd${CMAKE_STATIC_LIBRARY_SUFFIX}
    zstd_static${CMAKE_STATIC_LIBRARY_SUFFIX}
  PATHS ${TILEDB_EP_INSTALL_PREFIX}
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH
)

if (ZSTD_LIBRARIES)
  set(ZSTD_STATIC_EP_FOUND TRUE)
  find_path(ZSTD_INCLUDE_DIR
    NAMES zstd.h
    PATHS ${TILEDB_EP_INSTALL_PREFIX}
    PATH_SUFFIXES include
    NO_DEFAULT_PATH
  )
else()
  set(ZSTD_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(ZSTD_LIBRARIES
    NAMES
      zstd libzstd
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
  find_path(ZSTD_INCLUDE_DIR
    NAMES zstd.h
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Zstd
  REQUIRED_VARS ZSTD_LIBRARIES ZSTD_INCLUDE_DIR
)

if (NOT ZSTD_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Zstd as an external project")

    if (WIN32)
      set(ARCH_SPEC -A X64)
      set(LDFLAGS_DEF "")
    else()
      set(LDFLAGS_DEF "-DCMAKE_C_FLAGS=-fPIC")
    endif()

    ExternalProject_Add(ep_zstd
      PREFIX "externals"
      URL "https://github.com/facebook/zstd/archive/v1.3.4.zip"
      URL_HASH SHA1=2f33cb8af3c964124be67ff4a50824a85b5e1907
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND}
        ${ARCH_SPEC}
        ${LDFLAGS_DEF}
        -DCMAKE_BUILD_TYPE=Release
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        ${TILEDB_EP_BASE}/src/ep_zstd/build/cmake
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
    )
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_zstd)
  else()
    message(FATAL_ERROR "Unable to find Zstd")
  endif()
endif()

if (ZSTD_FOUND AND NOT TARGET Zstd::Zstd)
  add_library(Zstd::Zstd UNKNOWN IMPORTED)
  set_target_properties(Zstd::Zstd PROPERTIES
    IMPORTED_LOCATION "${ZSTD_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (ZSTD_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(Zstd::Zstd)
endif()
