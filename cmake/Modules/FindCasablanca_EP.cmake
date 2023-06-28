#
# FindCasablanca_EP.cmake
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
# This module finds the Azure REST C++ SDK, installing it with an ExternalProject if
# necessary. It then defines the imported by target cpprest::cpprest.

# Include some common helper functions.
include(TileDBCommon)

# Azure dependencies
if (TILEDB_VCPKG)
    return()
endif()

###############################################################################
# Start superbuild/unmanaged/legacy version
###############################################################################

# This is the install subdirectory for azure headers and libs, to avoid pollution
set(TILEDB_EP_AZURE_INSTALL_PREFIX "${TILEDB_EP_INSTALL_PREFIX}/azurecpplite")

# First check for a static version in the EP prefix.
find_package(cpprestsdk
        PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
        ${TILEDB_DEPS_NO_DEFAULT_PATH}
)

if (NOT cpprestsdk_FOUND)
  if (TILEDB_SUPERBUILD)
      message(STATUS "Could NOT find Casablanca")
      message(STATUS "Adding Casablanca as an external project")

    set(DEPENDS)
    if (TARGET ep_openssl)
      list(APPEND DEPENDS ep_openssl)
    endif()
    if (TARGET ep_websocketpp)
      list(APPEND DEPENDS ep_websocketpp)
    endif()

    ExternalProject_Add(ep_casablanca
      PREFIX "externals"
      URL "https://github.com/Microsoft/cpprestsdk/archive/2.10.18.zip"
      URL_HASH SHA1=bd4b7ab9cf52ee92a2a5b47fdff34117551164ed
      DOWNLOAD_NAME cpprestsdk-2.10.18.zip
      CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DBUILD_SHARED_LIBS=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_AZURE_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_casablanca)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_CASABLANCA_EP_BUILT=TRUE
    )
  else ()
    message(FATAL_ERROR "Could not find CASABLANCA (required).")
  endif ()
endif ()

if (cpprestsdk_FOUND AND NOT TARGET cpprestsdk::cpprest)
  add_library(cpprestsdk::cpprest UNKNOWN IMPORTED)
  set_target_properties(cpprestsdk::cpprest PROPERTIES
    IMPORTED_LOCATION "${cpprestsdk_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${cpprestsdk_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if(TARGET cpprestsdk::cpprest)
  get_target_property(target_type cpprestsdk::cpprest TYPE)
  if (target_type STREQUAL STATIC_LIBRARY AND TILEDB_INSTALL_STATIC_DEPS)
    install_target_libs(cpprestsdk::cpprest)
  endif()
endif()
