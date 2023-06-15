#
# FindCurl_EP.cmake
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

if (TARGET CURL OR NOT (TILEDB_SERIALIZATION OR TILEDB_GCS OR TILEDB_S3 OR TILEDB_AZURE))
  return()
endif()

if (TILEDB_VCPKG)
  find_package(CURL REQUIRED ${TILEDB_DEPS_NO_DEFAULT_PATH})
  install_target_libs(CURL::libcurl)
  return()
endif()

# === Superbuild

# Search the path set during the superbuild for the EP.
set(CURL_PATHS ${TILEDB_EP_INSTALL_PREFIX})

# If the EP was built, it will install the CURLConfig.cmake file, which we
# can use with find_package.

# First try the CMake-provided find script.
if (NOT TILEDB_FORCE_ALL_DEPS)
  find_package(CURL ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()

# Next try finding the superbuild external project
if (NOT CURL_FOUND)
  find_path(CURL_INCLUDE_DIR
    NAMES curl/curl.h
    PATHS ${CURL_PATHS}
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH})

  # Link statically if installed with the EP.
  find_library(CURL_LIBRARIES
    NAMES
      libcurl${CMAKE_STATIC_LIBRARY_SUFFIX}
      libcurl-d${CMAKE_STATIC_LIBRARY_SUFFIX}
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
    set(WITH_SSL "-DCMAKE_USE_SCHANNEL=ON")
    ExternalProject_Add(ep_curl
      PREFIX "externals"
      # Set download name to avoid collisions with only the version number in the filename
      DOWNLOAD_NAME ep_curl.tar.gz
      URL
        "https://github.com/TileDB-Inc/tiledb-deps-mirror/releases/download/2.11-deps/curl-7.74.0.tar.gz"
        "https://curl.se/download/curl-7.74.0.tar.gz"
      URL_HASH SHA1=cd7239cf9223b39ade86a14eb37fe68f5656eae9
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_BUILD_TYPE=Release
        -DBUILD_SHARED_LIBS=OFF
        -DCURL_DISABLE_LDAP=ON
        -DCURL_DISABLE_LDAPS=ON
        -DCMAKE_USE_LIBSSH2=OFF
        -DCURL_STATICLIB=ON
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DHTTP_ONLY=ON
        # NOTE: as of Curl 7.74 there is no way to set ZSTD paths for cmake
        # ZSTD is not enabled until curl support being able to point to superbuild
        #-DCURL_ZSTD=ON
        "${WITH_SSL}"
        "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )

  else()
    set(DEPENDS)
    if (TARGET ep_openssl)
      # This branch specifically intends that curl will find our OpenSSL
      # via pkg-config. Ensure it exists to avoid confusing errors.
      find_package(PkgConfig REQUIRED)

      list(APPEND DEPENDS ep_openssl)
      set(WITH_SSL "--with-ssl")
      set(SSL_PKG_CONFIG_PATH "${TILEDB_EP_INSTALL_PREFIX}/lib/pkgconfig:${TILEDB_EP_INSTALL_PREFIX}/lib64/pkgconfig")
    elseif (TILEDB_OPENSSL_DIR)
      # ensure that curl links against the same libSSL
      set(WITH_SSL "--with-ssl=${TILEDB_OPENSSL_DIR}")
    else()
      message(WARNING "TileDB FindOpenSSL_EP did not set TILEDB_OPENSSL_DIR. Falling back to autotools detection.")
      # ensure that curl config errors out if SSL not available
      set(WITH_SSL "--with-ssl")
    endif()

    if (TARGET ep_zlib)
      list(APPEND DEPENDS ep_zlib)
      set(WITH_ZLIB "--with-zlib=${TILEDB_EP_INSTALL_PREFIX}")
    elseif (TILEDB_ZLIB_DIR)
      # ensure that curl links against the same libz
      set(WITH_ZLIB "--with-zlib=${TILEDB_ZLIB_DIR}")
    else()
      message(WARNING "TileDB FindZlib_EP did not set TILEDB_ZLIB_DIR. Falling back to autotools detection.")
      # ensure that curl config errors out if SSL not available
      set(WITH_ZLIB "--with-zlib")
    endif()

    if (APPLE)
      set(WITH_CA_BUNDLE "--with-ca-bundle=/etc/ssl/cert.pem")
    endif()

    if (TARGET ep_zstd)
      list(APPEND DEPENDS ep_zstd)
      set(WITH_ZLIB "--with-zstd=${TILEDB_EP_INSTALL_PREFIX}")
    elseif (TILEDB_ZSTD_DIR)
      # ensure that curl links against the same libz
      set(WITH_ZSTD "--with-zstd=${TILEDB_ZSTD_DIR}")
    else()
      message(WARNING "TileDB FindZstd_EP did not set TILEDB_ZSTD_DIR. Falling back to autotools detection.")
      # ensure that curl config errors out if SSL not available
      set(WITH_ZSTD "--with-zstd")
    endif()

    # Support cross compilation of MacOS
    if (CMAKE_OSX_ARCHITECTURES STREQUAL arm64)
      set(CURL_CROSS_COMPILATION_FLAGS "CFLAGS=${CFLAGS} -arch arm64" "LDFLAGS=${LDFLAGS} -arch arm64" --host=aarch64-apple-darwin)
    elseif(CMAKE_OSX_ARCHITECTURES STREQUAL x86_64)
      set(CURL_CROSS_COMPILATION_FLAGS "CFLAGS=${CFLAGS} -arch x86_64" "LDFLAGS=${LDFLAGS} -arch x86_64" --host=x86_64-apple-darwin)
    endif()

    ExternalProject_Add(ep_curl
      PREFIX "externals"
      URL "https://curl.se/download/curl-7.74.0.tar.gz"
      URL_HASH SHA1=cd7239cf9223b39ade86a14eb37fe68f5656eae9
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND} -E env PKG_CONFIG_PATH=${SSL_PKG_CONFIG_PATH} ${TILEDB_EP_BASE}/src/ep_curl/configure
          --prefix=${TILEDB_EP_INSTALL_PREFIX}
          --enable-http
          --enable-optimize
          --enable-shared=no
          --with-pic=yes
          --without-brotli
          --without-bearssl
          --without-cyassl
          --without-wolfssl
          --without-polarssl
          --without-mbedtls
          --without-gnutls
          --without-gssapi
          --without-idn2
          --without-libmetalink
          --without-libssh2
          --without-librtmp
          --without-nghttp2
          --without-zstd
          --disable-ares
          --disable-dict
          --disable-file
          --disable-ftp
          --disable-gopher
          --disable-imap
          --disable-mqtt
          --disable-ldap
          --disable-ldaps
          --disable-pop3
          --disable-rtmp
          --disable-rtsp
          --disable-scp
          --disable-sftp
          --disable-smb
          --disable-smtp
          --disable-telnet
          --disable-tftp
          ${WITH_SSL}
          ${WITH_ZLIB}
          ${WITH_ZSTD}
          ${WITH_CA_BUNDLE}
          ${CURL_CROSS_COMPILATION_FLAGS}
      BUILD_IN_SOURCE TRUE
      BUILD_COMMAND $(MAKE)
      INSTALL_COMMAND $(MAKE) install
      DEPENDS ${DEPENDS}
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )
  endif()

  list(APPEND TILEDB_EXTERNAL_PROJECTS ep_curl)
  list(APPEND FORWARD_EP_CMAKE_ARGS
    -DTILEDB_CURL_EP_BUILT=TRUE
  )
endif()

if (CURL_FOUND)
  message(STATUS "Found CURL: '${CURL_LIBRARIES}' (found version \"${CURL_VERSION}\")")

  # If the libcurl target is missing this is an older version of curl found, don't attempt to use cmake target
  if (NOT TARGET CURL::libcurl)
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
