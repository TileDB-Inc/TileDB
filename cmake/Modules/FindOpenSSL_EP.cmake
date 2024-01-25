#
# FindOpenSSL_EP.cmake
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
# # The above copyright notice and this permission notice shall be included in
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
# Finds the OpenSSL library, installing with an ExternalProject as necessary.
# This module defines:
#   - OPENSSL_INCLUDE_DIR, directory containing headers
#   - OPENSSL_LIBRARIES, the OpenSSL library path
#   - OPENSSL_FOUND, whether OpenSSL has been found
#   - The OpenSSL::SSL and OpenSSL::Crypto imported targets

# Include some common helper functions.
include(TileDBCommon)

if (TILEDB_VCPKG)
  find_package(OpenSSL REQUIRED)
  return()
endif()

# Search the path set during the superbuild for the EP.
set(OPENSSL_PATHS ${TILEDB_EP_INSTALL_PREFIX})

# Add /usr/local/opt, as Homebrew sometimes installs it there.
set (HOMEBREW_BASE "/usr/local/opt/openssl")
if (NOT TILEDB_DEPS_NO_DEFAULT_PATH)
  list(APPEND OPENSSL_PATHS ${HOMEBREW_BASE})
endif()

# First try the CMake-provided find script.
# NOTE: must use OPENSSL_ROOT_DIR. HINTS does not work.
set(OPENSSL_ROOT_DIR ${OPENSSL_PATHS})
if (NOT TILEDB_FORCE_ALL_DEPS)
  find_package(OpenSSL
    1.1.0
    ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()

# Next try finding the superbuild external project
if (NOT OPENSSL_FOUND AND TILEDB_FORCE_ALL_DEPS)
  find_path(OPENSSL_INCLUDE_DIR
    NAMES openssl/ssl.h
    PATHS ${OPENSSL_PATHS}
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  # Link statically if installed with the EP.
  find_library(OPENSSL_SSL_LIBRARY
    NAMES
      libssl${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${OPENSSL_PATHS}
    PATH_SUFFIXES lib
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  find_library(OPENSSL_CRYPTO_LIBRARY
    NAMES
      libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${OPENSSL_PATHS}
    PATH_SUFFIXES lib
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(OpenSSL
    REQUIRED_VARS OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY OPENSSL_INCLUDE_DIR
  )
endif()

if (NOT OPENSSL_FOUND AND TILEDB_SUPERBUILD)
  message(STATUS "Adding OpenSSL as an external project")

  if (WIN32)
    message(FATAL_ERROR "OpenSSL external project unimplemented on Windows.")
  endif()

  # Support cross compilation of MacOS
  if (CMAKE_OSX_ARCHITECTURES STREQUAL arm64)
    set(OPENSSL_CONFIG_CMD ${CMAKE_COMMAND} -E env "ARCHFLAGS=\\\\-arch arm64" ${TILEDB_EP_BASE}/src/ep_openssl/Configure darwin64-arm64-cc --prefix=${TILEDB_EP_INSTALL_PREFIX} no-shared -fPIC)
  elseif(CMAKE_OSX_ARCHITECTURES STREQUAL x86_64)
    set(OPENSSL_CONFIG_CMD ${CMAKE_COMMAND} -E env "ARCHFLAGS=\\\\-arch x86_64" ${TILEDB_EP_BASE}/src/ep_openssl/Configure darwin64-x86_64-cc --prefix=${TILEDB_EP_INSTALL_PREFIX} no-shared -fPIC)
  else()
    set(OPENSSL_CONFIG_CMD ${TILEDB_EP_BASE}/src/ep_openssl/config --prefix=${TILEDB_EP_INSTALL_PREFIX} no-shared -fPIC)
  endif()

  ExternalProject_Add(ep_openssl
    PREFIX "externals"
    URL "https://github.com/openssl/openssl/archive/OpenSSL_1_1_1i.zip"
    URL_HASH SHA1=627938302f681dfac186a9225b65368516b4f484
    CONFIGURE_COMMAND ${OPENSSL_CONFIG_CMD}
    BUILD_IN_SOURCE TRUE
    UPDATE_COMMAND ""
    LOG_DOWNLOAD TRUE
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
    LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
  )

  set(TILEDB_OPENSSL_DIR "${TILEDB_EP_INSTALL_PREFIX}")

  list(APPEND TILEDB_EXTERNAL_PROJECTS ep_openssl)
  list(APPEND FORWARD_EP_CMAKE_ARGS
    -DTILEDB_OPENSSL_EP_BUILT=TRUE
  )
endif()

if (OPENSSL_FOUND)
  message(STATUS "Found OpenSSL: ${OPENSSL_SSL_LIBRARY}")
  message(STATUS "Found OpenSSL crypto: ${OPENSSL_CRYPTO_LIBRARY}")
  message(STATUS "OpenSSL Root: ${OPENSSL_ROOT_DIR}")

  if (DEFINED OPENSSL_INCLUDE_DIR AND "${OPENSSL_INCLUDE_DIR}" MATCHES "${HOMEBREW_BASE}.*")
    # define TILEDB_OPENSSL_DIR so we can ensure curl links the homebrew version
    set(TILEDB_OPENSSL_DIR "${HOMEBREW_BASE}")
  endif()


  if (NOT TARGET OpenSSL::SSL)
    add_library(OpenSSL::SSL UNKNOWN IMPORTED)
    set_target_properties(OpenSSL::SSL PROPERTIES
      IMPORTED_LOCATION "${OPENSSL_SSL_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_INCLUDE_DIR}"
    )
  endif()

  if (NOT TARGET OpenSSL::Crypto)
    add_library(OpenSSL::Crypto UNKNOWN IMPORTED)
    set_target_properties(OpenSSL::Crypto PROPERTIES
      IMPORTED_LOCATION "${OPENSSL_CRYPTO_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_INCLUDE_DIR}"
      )
  endif()
endif()
