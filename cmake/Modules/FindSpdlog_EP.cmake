#
# FindSpdlog_EP.cmake
#
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
# Finds the Spdlog library, installing with an ExternalProject as necessary.
# This module defines:
#   - SPDLOG_INCLUDE_DIR, directory containing headers
#   - SPDLOG_FOUND, whether Spdlog has been found
#   - The Spdlog::Spdlog imported target

# Search the path set during the superbuild for the EP.
set(SPDLOG_PATHS ${TILEDB_EP_SOURCE_DIR}/ep_spdlog/include)

if (NOT TILEDB_FORCE_ALL_DEPS OR TILEDB_SPDLOG_EP_BUILT)
  find_path(SPDLOG_INCLUDE_DIR
    NAMES spdlog/spdlog.h
    PATHS ${SPDLOG_PATHS}
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Spdlog
  REQUIRED_VARS SPDLOG_INCLUDE_DIR
)

if (NOT SPDLOG_FOUND AND TILEDB_SUPERBUILD)
  message(STATUS "Adding Spdlog as an external project")
  ExternalProject_Add(ep_spdlog
    PREFIX "externals"
    # Set download name to avoid collisions with only the version number in the filename
    DOWNLOAD_NAME ep_spdlog.zip
    URL "https://github.com/gabime/spdlog/archive/v0.16.3.zip"
    URL_HASH SHA1=00a732da1449c15b787491a924d63590c1649710
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    LOG_DOWNLOAD TRUE
    LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
  )
  list(APPEND TILEDB_EXTERNAL_PROJECTS ep_spdlog)
  list(APPEND FORWARD_EP_CMAKE_ARGS
    -DTILEDB_SPDLOG_EP_BUILT=TRUE
  )
endif()

if (SPDLOG_FOUND AND NOT TARGET Spdlog::Spdlog)
  add_library(Spdlog::Spdlog INTERFACE IMPORTED)
  find_package(fmt QUIET)
  if (${fmt_FOUND})
    target_link_libraries(Spdlog::Spdlog INTERFACE fmt::fmt)
  endif()
  set_target_properties(Spdlog::Spdlog PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${SPDLOG_INCLUDE_DIR}"
  )
endif()
