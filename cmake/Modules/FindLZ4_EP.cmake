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

# Search the path set during the superbuild for the EP.
set(LZ4_PATHS ${TILEDB_EP_INSTALL_PREFIX})

find_path(LZ4_INCLUDE_DIR
  NAMES lz4.h
  PATHS ${LZ4_PATHS}
  PATH_SUFFIXES include
  ${TILEDB_DEPS_NO_DEFAULT_PATH}
)


# Link statically if installed with the EP.
if (TILEDB_USE_STATIC_LZ4)
  find_library(LZ4_LIBRARIES
    NAMES
      liblz4${CMAKE_STATIC_LIBRARY_SUFFIX}
      liblz4_static${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${LZ4_PATHS}
    PATH_SUFFIXES lib
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
else()
  find_library(LZ4_LIBRARIES
    NAMES
      lz4 liblz4
    PATHS ${LZ4_PATHS}
    PATH_SUFFIXES lib bin
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
      ExternalProject_Add(ep_lz4
        PREFIX "externals"
        URL "https://github.com/lz4/lz4/releases/download/v1.8.2/lz4_v1_8_2_win64.zip"
        URL_HASH SHA1=330f97a88b91a4b9a15ddfe063321849a3a9e62d
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE TRUE
        BUILD_COMMAND ""
        INSTALL_COMMAND
          ${CMAKE_COMMAND} -E make_directory ${TILEDB_EP_INSTALL_PREFIX}/bin &&
          ${CMAKE_COMMAND} -E make_directory ${TILEDB_EP_INSTALL_PREFIX}/lib &&
          ${CMAKE_COMMAND} -E make_directory ${TILEDB_EP_INSTALL_PREFIX}/include &&
          ${CMAKE_COMMAND} -E copy_if_different
            ${TILEDB_EP_BASE}/src/ep_lz4/dll/liblz4.def
            ${TILEDB_EP_BASE}/src/ep_lz4/dll/liblz4.lib
            ${TILEDB_EP_INSTALL_PREFIX}/bin &&
          ${CMAKE_COMMAND} -E copy_if_different
            ${TILEDB_EP_BASE}/src/ep_lz4/dll/liblz4.so.1.8.2.dll
            ${TILEDB_EP_INSTALL_PREFIX}/bin/liblz4.dll &&
          ${CMAKE_COMMAND} -E copy_if_different
            ${TILEDB_EP_BASE}/src/ep_lz4/static/liblz4_static.lib
            ${TILEDB_EP_INSTALL_PREFIX}/lib &&
          ${CMAKE_COMMAND} -E copy_if_different
            ${TILEDB_EP_BASE}/src/ep_lz4/include/lz4.h
            ${TILEDB_EP_BASE}/src/ep_lz4/include/lz4frame.h
            ${TILEDB_EP_BASE}/src/ep_lz4/include/lz4hc.h
            ${TILEDB_EP_INSTALL_PREFIX}/include
        UPDATE_COMMAND ""
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
      )
    else()
      ExternalProject_Add(ep_lz4
        PREFIX "externals"
        URL "https://github.com/lz4/lz4/archive/v1.8.2.zip"
        URL_HASH SHA1=ebf6c227965318ecd73820ade8f5dbd83d48b3e8
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE TRUE
        INSTALL_COMMAND $(MAKE) PREFIX=${TILEDB_EP_INSTALL_PREFIX} install
        UPDATE_COMMAND ""
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
      )
    endif()
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_lz4)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_USE_STATIC_LZ4=TRUE
    )
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
if (TILEDB_USE_STATIC_LZ4 AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(LZ4::LZ4)
endif()