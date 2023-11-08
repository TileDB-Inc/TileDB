#
# FindZstd_EP.cmake
#
#
# The MIT License
#
# Copyright (c) 2017-2021 TileDB, Inc.
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
#   - The ${ZSTD_TARGET} imported target

# Include some common helper functions.
include(TileDBCommon)

if (TILEDB_VCPKG)
  find_package(zstd CONFIG REQUIRED)
  if (TARGET zstd::libzstd_static)
    set(ZSTD_TARGET zstd::libzstd_static)
    set(ZSTD_STATIC_EP_FOUND TRUE)
  else()
    set(ZSTD_TARGET zstd::libzstd_shared)
  endif()
  set(ZSTD_FOUND TRUE)
else()
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
  elseif(NOT TILEDB_FORCE_ALL_DEPS)
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
endif()

if (NOT ZSTD_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Zstd as an external project")

    if (WIN32)
      set(ARCH_SPEC -A X64)
      set(CFLAGS_DEF "")
    else()
      set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
    endif()

    ExternalProject_Add(ep_zstd
      PREFIX "externals"
      # Set download name to avoid collisions with only the version number in the filename
      DOWNLOAD_NAME ep_zstd.zip
      URL "https://github.com/facebook/zstd/archive/v1.4.8.zip"
      URL_HASH SHA1=8323c212a779ada25f5d587349326e84b047b536
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND}
        ${ARCH_SPEC}
        "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
        -DCMAKE_BUILD_TYPE=Release
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
	# Disable building ZSTD shared library as this is not used by superbuild
	# When using superbuild for zstd + curl the shared library was being selected
	# on some envs due to ordering of search paths.
	# This caused linker errors for projects using libtiledb because of missing shared zstd library.
	-DZSTD_BUILD_SHARED=OFF
        ${TILEDB_EP_BASE}/src/ep_zstd/build/cmake
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_zstd)
    set(TILEDB_ZSTD_DIR "${TILEDB_EP_INSTALL_PREFIX}")
  else()
    message(FATAL_ERROR "Unable to find Zstd")
  endif()
endif()

if (ZSTD_FOUND AND NOT ZSTD_TARGET)
  add_library(zstd::libzstd UNKNOWN IMPORTED)
  set_target_properties(zstd::libzstd PROPERTIES
    IMPORTED_LOCATION "${ZSTD_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_INCLUDE_DIR}"
  )
  set(ZSTD_TARGET zstd::libzstd)
endif()

# If we built a static EP, install it if required.
if (ZSTD_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(${ZSTD_TARGET})
endif()
