#
# FindNlohmann_json_EP.cmake
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
# Finds the nlohmann_json library, installing with an ExternalProject as necessary.
# This module defines:
#   - NLOHMANN_JSON_INCLUDE_DIR, directory containing headers
#   - NLOHMANN_JSON_FOUND, whether Nlohmann_json has been found
#   - The nlohmann_json::nlohmann_json imported target

# Search the path set during the superbuild for the EP.
set(NLOHMANN_JSON_PATHS ${TILEDB_EP_INSTALL_PREFIX}/include)

find_path(NLOHMANN_JSON_INCLUDE_DIR
  NAMES nlohmann/json.hpp
  PATHS ${NLOHMANN_JSON_PATHS}
  ${TILEDB_DEPS_NO_DEFAULT_PATH}
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(nlohmann_json
  REQUIRED_VARS NLOHMANN_JSON_INCLUDE_DIR
)

if (NOT NLOHMANN_JSON_FOUND AND TILEDB_SUPERBUILD)
  message(STATUS "Adding nlohmann_json as an external project")
  ExternalProject_Add(ep_nlohmann_json
    PREFIX "externals"
    URL "https://github.com/nlohmann/json/archive/v3.1.2.tar.gz"
    URL_HASH SHA256=e8fffa6cbdb3c15ecdff32eebf958b6c686bc188da8ad5c6489462d16f83ae54
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
    UPDATE_COMMAND ""
    LOG_DOWNLOAD TRUE
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
  )
  list(APPEND TILEDB_EXTERNAL_PROJECTS ep_nlohmann_json)
endif()

if (NLOHMANN_JSON_FOUND AND NOT TARGET nlohmann_json::nlohmann_json)
  add_library(nlohmann_json::nlohmann_json INTERFACE IMPORTED)
  set_target_properties(nlohmann_json::nlohmann_json PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${NLOHMANN_JSON_INCLUDE_DIR}"
  )
endif()