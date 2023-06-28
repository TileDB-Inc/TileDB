#
# FindWebSocketpp_EP.cmake
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
# This module finds the Webscoket C++ library, installing it with an ExternalProject if
# necessary. It then defines the imported by target websocketpp::websocketpp.

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
find_package(websocketpp
        PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
        ${TILEDB_DEPS_NO_DEFAULT_PATH}
)

if (NOT websocketpp_FOUND)
  if (TILEDB_SUPERBUILD)
      message(STATUS "Could NOT find websocketpp")
      message(STATUS "Adding websocketpp as an external project")

    set(DEPENDS)
    if (TARGET ep_openssl)
      list(APPEND DEPENDS ep_openssl)
    endif()

    ExternalProject_Add(ep_websocketpp
      PREFIX "externals"
      URL "https://github.com/zaphoyd/websocketpp/archive/0.8.2.zip"
      URL_HASH SHA1=55c878d6e6a0416e2d6b6eb108ff6cc83d34cc2e
      DOWNLOAD_NAME websocketpp-0.8.2.zip
      CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_AZURE_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_websocketpp)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_WEBSOCKETPP_EP_BUILT=TRUE
    )
  else ()
    message(FATAL_ERROR "Could not find ep_websocketpp (required).")
  endif ()
endif ()


if (websocketpp_FOUND AND NOT TARGET websocketpp::websocketpp)
  add_library(websocketpp::websocketpp UNKNOWN IMPORTED)
  set_target_properties(websocketpp::websocketpp PROPERTIES
          IMPORTED_LOCATION "${websocketpp_LIBRARIES}"
          INTERFACE_INCLUDE_DIRECTORIES "${websocketpp_INCLUDE_DIR}"
          )
endif()

# If we built a static EP, install it if required.
if(TARGET websocketpp::websocketpp)
  get_target_property(target_type websocketpp::websocketpp TYPE)
  if (target_type STREQUAL STATIC_LIBRARY AND TILEDB_INSTALL_STATIC_DEPS)
    install_target_libs(websocketpp::websocketpp)
  endif()
endif()