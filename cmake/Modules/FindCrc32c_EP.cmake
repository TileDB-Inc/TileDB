#
# FindCrc32c_EP.cmake
#
# The MIT License
#
# Copyright (c) 2022 TileDB, Inc.
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
# Finds the Crc32c library, installing with an ExternalProject as necessary.
# This module defines:
#   - Crc32c_INCLUDE_DIR, directory containing headers
#   - Crc32c_LIBRARIES, the Crc32c library path
#   - Crc32c_FOUND, whether Crc32c has been found
#   - The Crc32c::crc32c imported target

# Include some common helper functions.
include(TileDBCommon)

if(TILEDB_CRC32C_EP_BUILT)
  find_package(Crc32c CONFIG REQUIRED
    HINTS
      ${TILEDB_EP_INSTALL_PREFIX}/lib/cmake
    ${TILEDB_DEPS_NO_DEFAULT_PATH})
elseif (NOT TILEDB_FORCE_ALL_DEPS)
  # seems no standard findCrc32c.cmake, so silence warnings with QUIET
  find_package(Crc32c QUIET ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()

# if not yet built add it as an external project
if (NOT TILEDB_CRC32C_EP_BUILT AND NOT Crc32c_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding crc32c as an external project")

    set(TILEDB_CRC32C_BUILD_VERSION 1.1.2)
    set(TILEDB_CRC32C_URL
        "https://github.com/google/crc32c/archive/refs/tags/${TILEDB_CRC32C_BUILD_VERSION}.tar.gz")
    set(TILEDB_CRC32C_SHA256
        "ac07840513072b7fcebda6e821068aa04889018f24e10e46181068fb214d7e56")

    ExternalProject_Add(
        ep_crc32c
        EXCLUDE_FROM_ALL ON
        PREFIX "externals"
        INSTALL_DIR "${TILEDB_EP_INSTALL_PREFIX}"
        URL ${TILEDB_CRC32C_URL}
        URL_HASH SHA256=${TILEDB_CRC32C_SHA256}
        LIST_SEPARATOR |
        CMAKE_ARGS
                   -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
                   -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
                   -DCRC32C_BUILD_TESTS=OFF
                   -DCRC32C_BUILD_BENCHMARKS=OFF
                   -DCRC32C_USE_GLOG=OFF
        LOG_DOWNLOAD ON
        LOG_CONFIGURE ON
        LOG_BUILD ON
        LOG_INSTALL ON)

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_crc32c)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_CRC32C_EP_BUILT=TRUE
    )
  else()
    message(FATAL_ERROR "Unable to find crc32c")
  endif()
endif ()

if (Crc32c_FOUND AND NOT TARGET Crc32c::crc32c)
  message(STATUS "Found crc32c, adding imported target: ${Crc32c_LIBRARIES}")
  add_library(Crc32c::crc32c UNKNOWN IMPORTED)
  set_target_properties(Crc32c::crc32c PROPERTIES
    IMPORTED_LOCATION "${Crc32c_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${Crc32c_INCLUDE_DIR}"
  )
endif()
