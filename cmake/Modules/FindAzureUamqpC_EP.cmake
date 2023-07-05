#
# FindAzureUamqpC_EP.cmake
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
# This module finds the Azure , installing it with an ExternalProject if
# necessary. It then defines the imported by target Azure_Storage_Common::Azure_Storage_Common.

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
find_package(uamqp
        PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
        ${TILEDB_DEPS_NO_DEFAULT_PATH}
)

if (NOT uamqp_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Could NOT find uamqp")
    message(STATUS "Adding uamqp as an external project")

    set(CFLAGS_DEF "-Wno-error=array-parameter")

    ExternalProject_Add(ep_uamqp
      PREFIX "externals"
      GIT_REPOSITORY "https://github.com/Azure/azure-uamqp-c.git"
      GIT_TAG e76dd6be7ab4cda356dcad082cf8c8c18a454d51
      CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_AZURE_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_AZURE_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
        "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
        "-DCMAKE_CXX_FLAGS=${CFLAGS_DEF}"
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_uamqp)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_uamqp_EP_BUILT=TRUE
    )
  else ()
    message(FATAL_ERROR "Could not find ep_uamqp (required).")
  endif ()
endif ()


if (uamqp_FOUND AND NOT TARGET uamqp::uamqp)
  add_library(uamqp::uamqp UNKNOWN IMPORTED)
  set_target_properties(uamqp::uamqp PROPERTIES
          IMPORTED_LOCATION "${uamqp_LIBRARIES}"
          INTERFACE_INCLUDE_DIRECTORIES "${uamqp_INCLUDE_DIR}"
          )
endif()

# If we built a static EP, install it if required.
if(TARGET uamqp::uamqp)
  get_target_property(target_type uamqp::uamqp TYPE)
  if (target_type STREQUAL STATIC_LIBRARY AND TILEDB_INSTALL_STATIC_DEPS)
    install_target_libs(uamqp::uamqp)
  endif()
endif()
