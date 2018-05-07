#
# FindSpdlog_EP.cmake
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
# Finds the Spdlog library, installing with an ExternalProject as necessary.
# This module defines:
#   - SPDLOG_INCLUDE_DIR, directory containing headers
#   - SPDLOG_FOUND, whether Spdlog has been found
#   - The Spdlog::Spdlog imported target

# Search the path set during the superbuild for the EP.
set(SPDLOG_PATHS ${TILEDB_EP_SOURCE_DIR}/ep_spdlog/include)

find_path(SPDLOG_INCLUDE_DIR
  NAMES spdlog/spdlog.h
  PATHS ${SPDLOG_PATHS}
  ${TILEDB_DEPS_NO_DEFAULT_PATH}
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Spdlog
  REQUIRED_VARS SPDLOG_INCLUDE_DIR
)

if (NOT SPDLOG_FOUND OR TILEDB_FORCE_ALL_DEPS)
  message(STATUS "Adding Spdlog as an external project")
  ExternalProject_Add(ep_spdlog
    PREFIX "externals"
    URL "https://github.com/gabime/spdlog/archive/v0.16.3.zip"
    URL_HASH SHA1=00a732da1449c15b787491a924d63590c1649710
    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
    -DCMAKE_CXX_FLAGS=-fPIC
    -DCMAKE_C_FLAGS=-fPIC
    UPDATE_COMMAND ""
    LOG_DOWNLOAD TRUE
  )
  list(APPEND TILEDB_EXTERNAL_PROJECTS ep_spdlog)
  SET(SPDLOG_FOUND TRUE)
  set(SPDLOG_INCLUDE_DIR "${TILEDB_EP_INSTALL_PREFIX}/include/spdlog")

  # INTERFACE_INCLUDE_DIRECTORIES does not allow for empty directories in config phase,
  # thus we need to create the directory. See https://cmake.org/Bug/view.php?id=15052
  file(MAKE_DIRECTORY ${SPDLOG_INCLUDE_DIR})
endif()

if (SPDLOG_FOUND AND NOT TARGET Spdlog::Spdlog)
  add_library(Spdlog::Spdlog INTERFACE IMPORTED)
  set_target_properties(Spdlog::Spdlog PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${SPDLOG_INCLUDE_DIR}"
  )
endif()