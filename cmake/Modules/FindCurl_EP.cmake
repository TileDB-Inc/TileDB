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

# Include some common helper functions.
include(TileDBCommon)

# Search the path set during the superbuild for the EP.
set(CURL_PATHS ${TILEDB_EP_INSTALL_PREFIX})

# If the EP was built, it will install the CURLConfig.cmake file, which we
# can use with find_package. CMake uses CMAKE_PREFIX_PATH to locate find
# modules.
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${TILEDB_EP_INSTALL_PREFIX}")

# First try the CMake-provided find script.
find_package(CURL QUIET ${TILEDB_DEPS_NO_DEFAULT_PATH} CONFIG)

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

  if (TARGET ep_openssl)
    set(DEPENDS ep_openssl)
    set(with_ssl "-DOPENSSL_ROOT_DIR=${TILEDB_EP_INSTALL_PREFIX}")
    message(WARNING "with_ssl=${with_ssl}")
  endif()

  set(TILEDB_CURL_LIBS "-ldl -lpthread")
  configure_file(
    "${TILEDB_CMAKE_INPUTS_DIR}/configure-curl.sh.in"
    "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_FILES_DIRECTORY}/configure-curl.sh"
    @ONLY
  )
  file(COPY
    "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_FILES_DIRECTORY}/configure-curl.sh"
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}"
    FILE_PERMISSIONS
      OWNER_READ OWNER_WRITE OWNER_EXECUTE
      GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  if (WIN32)
    set(USE_WINSSL "-DCMAKE_USE_WINSSL=ON")
  endif()
  message(WARNING "with_ssl=${with_ssl}")

  ExternalProject_Add(ep_curl
    PREFIX "externals"
    URL "https://curl.haxx.se/download/curl-7.64.1.tar.gz"
    URL_HASH SHA1=54ee48d81eb9f90d3efdc6cdf964bd0a23abc364
    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
    -DCMAKE_BUILD_TYPE=Release
    -DBUILD_SHARED_LIBS=OFF
    -DCURL_DISABLE_LDAP=ON
    -DCURL_DISABLE_LDAPS=ON
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=true
    "${USE_WINSSL}"
    "${with_ssl}"
    "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
    UPDATE_COMMAND ""
    LOG_DOWNLOAD TRUE
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
    DEPENDS ${DEPENDS}
  )
  list(APPEND TILEDB_EXTERNAL_PROJECTS ep_curl)
  list(APPEND FORWARD_EP_CMAKE_ARGS
    -DTILEDB_CURL_EP_BUILT=TRUE
  )
endif()

if (CURL_FOUND)
  message(STATUS "Found CURL: ${CURL_LIBRARIES} (found version \"${CURL_VERSION}\")")

    # If the libcurl target is missing this is an older version of curl found, don't attempt to use cmake target
    if (NOT TARGET CURL::libcurl)
      message(WARNING "cmake libcurl target not found, static linking against libcurl will result in missing symbols")
      add_library(CURL::libcurl UNKNOWN IMPORTED)
      set_target_properties(CURL::libcurl PROPERTIES
        IMPORTED_LOCATION "${CURL_LIBRARIES}"
        INTERFACE_INCLUDE_DIRECTORIES "${CURL_INCLUDE_DIR};${CURL_INCLUDE_DIRS}"
      )
    # If the CURL::libcurl target exists this is new enough curl version to rely on the cmake target properties
    else()
      get_target_property(LIBCURL_TYPE CURL::libcurl TYPE)
      # CURL_STATICLIB is missing for curl versions <7.61.1
      if(CURL_VERSION VERSION_LESS 7.61.1 AND LIBCURL_TYPE STREQUAL "STATIC_LIBRARY")
        set_target_properties(CURL::libcurl PROPERTIES
                INTERFACE_COMPILE_DEFINITIONS CURL_STATICLIB)
      endif()
    endif()

endif()

# If we built a static EP, install it if required.
if (TILEDB_CURL_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(CURL::libcurl)
endif()
