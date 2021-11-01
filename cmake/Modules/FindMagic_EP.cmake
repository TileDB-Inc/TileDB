#
# FindMagic_EP.cmake
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
# Finds the Magic library, installing with an ExternalProject as necessary.
# This module defines:
#   - MAGIC_INCLUDE_DIR, directory containing headers
#   - MAGIC_LIBRARIES, the Magic library path
#   - MAGIC_FOUND, whether Magic has been found
#   - The Magic::Magic imported target

# Include some common helper functions.
include(TileDBCommon)

# Search the path set during the superbuild for the EP.
set(MAGIC_PATHS ${TILEDB_EP_INSTALL_PREFIX})

# Try the builtin find module unless built w/ EP superbuild
if ((NOT TILEDB_FORCE_ALL_DEPS) AND (NOT TILEDB_MAGIC_EP_BUILT))
  find_package(magic ${TILEDB_DEPS_NO_DEFAULT_PATH} QUIET)
endif()

# Next try finding the superbuild external project
if (NOT MAGIC_FOUND)
  find_path(MAGIC_INCLUDE_DIR
    NAMES magic.h
    PATHS ${MAGIC_PATHS}
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  if (NOT MAGIC_INCLUDE_DIR)
    find_path(MAGIC_INCLUDE_DIR
      NAMES file/file.h
      PATHS ${MAGIC_PATHS}
      PATH_SUFFIXES include
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )
  endif()

  # Link statically if installed with the EP.
  find_library(MAGIC_LIBRARIES
    magic
    PATHS ${MAGIC_PATHS}
    PATH_SUFFIXES lib
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(Magic
    REQUIRED_VARS MAGIC_LIBRARIES MAGIC_INCLUDE_DIR
  )
endif()

# If not found, add it as an external project
if (NOT MAGIC_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Magic as an external project")

    if (WIN32)
      set(CFLAGS_DEF "")
    else()
      set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
      set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS} -fPIC")
    endif()

    ExternalProject_Add(ep_magic
      PREFIX "externals"
      URL "ftp://ftp.astron.com/pub/file/file-5.41.tar.gz"
      URL_HASH SHA1=8d80d2d50f4000e087aa60ffae4f099c63376762
      DOWNLOAD_NAME "file-5.41.tar.gz"
      UPDATE_COMMAND ""
      CONFIGURE_COMMAND
            ${TILEDB_EP_BASE}/src/ep_magic/configure --prefix=${TILEDB_EP_INSTALL_PREFIX} CFLAGS=${CFLAGS_DEF} CXXFLAGS=${CXXFLAGS_DEF}
      BUILD_IN_SOURCE TRUE
      BUILD_COMMAND $(MAKE)
      INSTALL_COMMAND $(MAKE) install
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_magic)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_MAGIC_EP_BUILT=TRUE
    )

    set(TILEDB_MAGIC_DIR "${TILEDB_EP_INSTALL_PREFIX}")

  else()
    message(FATAL_ERROR "Unable to find Magic")
  endif()
endif()

if (MAGIC_FOUND AND NOT TARGET Magic::Magic)
  message(STATUS "Found Magic, adding imported target: ${MAGIC_LIBRARIES}")
  add_library(Magic::Magic UNKNOWN IMPORTED)
  set_target_properties(Magic::Magic PROPERTIES
    IMPORTED_LOCATION "${MAGIC_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${MAGIC_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (TILEDB_MAGIC_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(Magic::Magic)
endif()
