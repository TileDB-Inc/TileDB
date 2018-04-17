#
# FindTBB_EP.cmake
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
# Finds the Intel TBB library, installing with an ExternalProject as necessary.
# This module defines:
#   - TBB_INCLUDE_DIR, directory containing headers
#   - TBB_LIBRARIES, the TBB library path
#   - TBB_FOUND, whether TBB has been found
#   - The TBB::TBB imported target

# Search the path set during the superbuild for the EP.
set(TBB_PATHS ${TILEDB_EP_INSTALL_PREFIX})

find_path(TBB_INCLUDE_DIR
  NAMES tbb/tbb.h
  PATHS ${TBB_PATHS}
  PATH_SUFFIXES include
  ${TILEDB_DEPS_NO_DEFAULT_PATH}
)

find_library(TBB_LIBRARIES
  NAMES
    tbb libtbb
  PATHS ${TBB_PATHS}
  PATH_SUFFIXES lib bin
  ${TILEDB_DEPS_NO_DEFAULT_PATH}
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(TBB
  REQUIRED_VARS TBB_LIBRARIES TBB_INCLUDE_DIR
)

if (NOT TBB_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding TBB as an external project")
    set(TBB_URL)
    set(TBB_SHA1SUM)

    if (APPLE)
      # macOS
      set(TBB_URL "https://github.com/01org/tbb/releases/download/2018_U3/tbb2018_20180312oss_mac.tgz")
      set(TBB_SHA1SUM 6e0229e5bd7d457c756ec6b31ab39cceb7f109a1)
    elseif(NOT WIN32)
      # Linux
      set(TBB_URL "https://github.com/01org/tbb/releases/download/2018_U3/tbb2018_20180312oss_lin.tgz")
      set(TBB_SHA1SUM 00bcc86c8d64c5a186f0411fa8ddfc6cb28ba516)
    else()
      # Win32
      message(FATAL_ERROR "Not yet implemented.")
    endif()

    ExternalProject_Add(ep_tbb
      PREFIX "externals"
      URL "${TBB_URL}"
      URL_HASH SHA1=${TBB_SHA1SUM}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      UPDATE_COMMAND ""
      INSTALL_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
    )

    # Simple install step by copying the binaries.
    ExternalProject_Add_step(ep_tbb copy_install
      DEPENDEES download
      COMMAND
        ${CMAKE_COMMAND} -E make_directory "${TILEDB_EP_INSTALL_PREFIX}/lib" &&
        ${CMAKE_COMMAND} -E make_directory "${TILEDB_EP_INSTALL_PREFIX}/include" &&
        ${CMAKE_COMMAND} -E copy_if_different
          ${TILEDB_EP_SOURCE_DIR}/ep_tbb/lib/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${TILEDB_EP_INSTALL_PREFIX}/lib &&
        ${CMAKE_COMMAND} -E remove_directory
          ${TILEDB_EP_SOURCE_DIR}/ep_tbb/include/serial &&
        ${CMAKE_COMMAND} -E remove
          ${TILEDB_EP_SOURCE_DIR}/ep_tbb/include/index.html &&
        ${CMAKE_COMMAND} -E copy_directory
          ${TILEDB_EP_SOURCE_DIR}/ep_tbb/include
          ${TILEDB_EP_INSTALL_PREFIX}/include
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_tbb)
  else()
    message(FATAL_ERROR "Unable to find TBB")
  endif()
endif()

if (TBB_FOUND AND NOT TARGET TBB::TBB)
  add_library(TBB::TBB UNKNOWN IMPORTED)
  set_target_properties(TBB::TBB PROPERTIES
    IMPORTED_LOCATION "${TBB_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${TBB_INCLUDE_DIR}"
  )
endif()
