#
# FindCurl_EP.cmake
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
# Finds the Curl library, installing with an ExternalProject as necessary.
# This module defines:
#   - CURL_INCLUDE_DIR, directory containing headers
#   - CURL_LIBRARIES, the Curl library path
#   - CURL_FOUND, whether Curl has been found
#   - The Curl::Curl imported target

# Search the path set during the superbuild for the EP.
set(CURL_PATHS ${TILEDB_EP_INSTALL_PREFIX})

# First try the CMake-provided find script.
find_package(CURL QUIET ${TILEDB_DEPS_NO_DEFAULT_PATH})

# Next try finding the superbuild external project
if (NOT CURL_FOUND)
  find_path(CURL_INCLUDE_DIR
    NAMES curl/curl.h
    PATHS ${CURL_PATHS}
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  # Link statically if installed with the EP.
  find_library(CURL_LIBRARIES
    NAMES
      libcurl${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${CURL_PATHS}
    PATH_SUFFIXES lib
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(Curl
    REQUIRED_VARS CURL_LIBRARIES CURL_INCLUDE_DIR
  )
endif()

if (NOT CURL_FOUND AND TILEDB_SUPERBUILD)
  message(STATUS "Adding Curl as an external project")

  if (WIN32)
    message(FATAL_ERROR "Curl external project unimplemented on Windows.")
  endif()

  if (TARGET ep_openssl)
    set(DEPENDS ep_openssl)
  endif()

  set(TILEDB_CURL_LIBS "-ldl -lpthread")
  configure_file(
    "${TILEDB_CMAKE_INPUTS_DIR}/configure-curl.sh.in"
    "${CMAKE_CURRENT_BINARY_DIR}/configure-curl.sh"
    @ONLY
  )

  ExternalProject_Add(ep_curl
    PREFIX "externals"
    URL "https://curl.haxx.se/download/curl-7.59.0.zip"
    URL_HASH SHA1=4558ff1b78396c57cf176e03f702ae87378fd776
    CONFIGURE_COMMAND
      ${CMAKE_CURRENT_BINARY_DIR}/configure-curl.sh
      ${TILEDB_EP_BASE}/src/ep_curl/configure
        --prefix=${TILEDB_EP_INSTALL_PREFIX}
        --enable-optimize
        --enable-shared=no
        --disable-ldap
        --with-pic=yes
        --with-ssl=${TILEDB_EP_INSTALL_PREFIX}
    BUILD_IN_SOURCE TRUE
    BUILD_COMMAND $(MAKE)
    INSTALL_COMMAND $(MAKE) install
    UPDATE_COMMAND ""
    LOG_DOWNLOAD TRUE
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
    DEPENDS ${DEPENDS}
  )
  list(APPEND TILEDB_EXTERNAL_PROJECTS ep_curl)
endif()

if (CURL_FOUND AND NOT TARGET Curl::Curl)
  message(STATUS "Found Curl: ${CURL_LIBRARIES}")
  add_library(Curl::Curl UNKNOWN IMPORTED)
  set_target_properties(Curl::Curl PROPERTIES
    IMPORTED_LOCATION "${CURL_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${CURL_INCLUDE_DIR};${CURL_INCLUDE_DIRS}"
  )
endif()
