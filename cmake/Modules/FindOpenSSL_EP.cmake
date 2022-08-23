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

#INCLUDE(FindPerl)
#INCLUDE(FindPerlLibs)
if(WIN32)
  if(TILEDB_SUPERBUILD)
    # The find_package(Perl) can (has) found msys versions of perl locally, which the
    # openssl package is not happy with because those versions do not return windows-y
    # paths (returns forward slashes rather than back-slashes.)
    # Hence, we are retrieving a version of perl which does meet requirements
    # rather than slowly 'filtering' through the various versions that might
    # not meet the configuration requirements.
    set(PERL_COMMAND "PERL_COMMAND-NOTFOUND")
    # 32bit is slightly smaller and sufficient for configuring openssl.
    ExternalProject_Add(strawberry_perl_portable
        PREFIX "externals"
        URL "https://strawberryperl.com/download/5.32.1.1/strawberry-perl-5.32.1.1-32bit-portable.zip"
        URL_HASH SHA1=28bca91cadd6651c2b2463db8587c170bf17f2fa
        CONFIGURE_COMMAND ""
        BUILD_ALWAYS OFF
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        LOG_DOWNLOAD TRUE
    )
  else() # NOT SUPERBUILD
    set(PERL_COMMAND "PERL_COMMAND-NOTFOUND")
    find_program(PERL_COMMAND "perl.exe")
    # find_program(PERL_COMMAND "perl.exe" ...what paths to specify?...)
  endif()
endif()

if (NOT OPENSSL_FOUND AND TILEDB_SUPERBUILD)
  message(STATUS "Adding OpenSSL as an external project")

  if(WIN32)
    set(config "VC-WIN64A")
    set(do_script "OpenSSL-build.bat")
    set(tdb_findopenssl_need_depends 0)
    if(NOT PERL_COMMAND)
      set(PERL_COMMAND "${TILEDB_EP_BASE}/src/strawberry_perl_portable/perl/bin/perl.exe")
      set(tdb_findopenssl_need_depends 1)
    endif()
    get_filename_component(PERL_BIN_DIR ${PERL_COMMAND} PATH)

    set(proj "tdb-openssl-help")  
    set(TILEDB_EP_OPENSSL_DIR "${TILEDB_EP_BASE}/src/ep_openssl")
    set(TILEDB_EP_OPENSSL_HELP_DIR "${TILEDB_EP_BASE}/src/${proj}")
    file(MAKE_DIRECTORY "$TILEDB_EP_OPENSSL_DIR}")

    file(WRITE "${TILEDB_EP_OPENSSL_HELP_DIR}/${proj}-configure.bat"
    "set PATH=%PATH%;${PERL_BIN_DIR}
path
ECHO ${PERL_BIN_DIR}
POWERSHELL ls ${PERL_BIN_DIR}
POWERSHELL ls ${PERL_BIN_DIR}/perl.exe
perl Configure ${config} no-asm --prefix=${TILEDB_EP_INSTALL_PREFIX} --openssldir=${TILEDB_EP_INSTALL_PREFIX}/ssl
") #call OpenSSL-configure.bat")

    file(WRITE "${TILEDB_EP_OPENSSL_HELP_DIR}/${proj}-build.bat"
    "set PATH=%PATH%;${PERL_BIN_DIR}
path
nmake")

    file(WRITE "${TILEDB_EP_OPENSSL_HELP_DIR}/${proj}-install.bat"
    "set PATH=%PATH%;${PERL_BIN_DIR}
path
nmake install")

    if(tdb_findopenssl_need_depends)
      set(tdb_findopenssl_OPTIONAL_DEPENDS "strawberry_perl_portable")
    else()
      set(tdb_findopenssl_OPTIONAL_DEPENDS)
    endif()
    
    message(STATUS "optional_depends: ${OPTIONAL_DEPENDS}")

    ExternalProject_Add(ep_openssl
      PREFIX "externals"
      # Use github archive instead of the one hosted on openssl.org because of CMake bug #13251
      URL "https://github.com/openssl/openssl/archive/OpenSSL_1_1_1i.zip"
      URL_HASH SHA1=627938302f681dfac186a9225b65368516b4f484
      CMAKE_ARGS -A X64 -DPERL_COMMAND=${PERL_COMMAND}
      BUILD_IN_SOURCE 1
      CONFIGURE_COMMAND "${TILEDB_EP_OPENSSL_HELP_DIR}/${proj}-configure.bat"
      BUILD_ALWAYS OFF
      BUILD_COMMAND "${TILEDB_EP_OPENSSL_HELP_DIR}/${proj}-build.bat"
      INSTALL_COMMAND "${TILEDB_EP_OPENSSL_HELP_DIR}/${proj}-install.bat"
      DEPENDS ${tdb_findopenssl_OPTIONAL_DEPENDS}
      LOG_DOWNLOAD TRUE
      )

  else() # *nix
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
      BUILD_COMMAND $(MAKE)
      INSTALL_COMMAND $(MAKE) install
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )
  endif()

  set(TILEDB_OPENSSL_DIR "${TILEDB_EP_INSTALL_PREFIX}")

  list(APPEND TILEDB_EXTERNAL_PROJECTS ep_openssl)
  list(APPEND FORWARD_EP_CMAKE_ARGS
    -DTILEDB_OPENSSL_EP_BUILT=TRUE
  )
endif()

if (OPENSSL_FOUND)
  message(STATUS "Found OpenSSL: ${OPENSSL_SSL_LIBRARY} -- OpenSSL crypto: ${OPENSSL_CRYPTO_LIBRARY} -- root: ${OPENSSL_ROOT_DIR}")

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

# If we built a static EP, install it if required.
if (TILEDB_OPENSSL_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(OpenSSL::SSL)
  install_target_libs(OpenSSL::Crypto)
endif()
