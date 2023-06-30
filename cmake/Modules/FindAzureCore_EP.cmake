#
# FindAzureCore_EP.cmake
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
# This module finds the Azure core SDK, installing it with an ExternalProject if
# necessary. It then defines the imported by target Azure::azure-core.

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
find_library(AZURECORE_LIBRARIES
        NAMES
        libazure-core${CMAKE_STATIC_LIBRARY_SUFFIX}
        azure-core${CMAKE_STATIC_LIBRARY_SUFFIX}
        PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
        PATH_SUFFIXES lib lib64
        NO_DEFAULT_PATH
        )

#on win32... '.lib' also used for lib ref'ing .dll!!!
#So, if perchance, in some environments, a .lib existed along with its corresponding .dll,
#then we could be incorrectly assuming/attempting a static build and probably will fail to
#appropriately include/package the .dll, since the code is (incorrectly) assuming .lib is indicative of a
#static library being used.

if (AZURECORE_LIBRARIES)
  set(AZURECORE_STATIC_EP_FOUND TRUE)
  find_path(AZURECORE_INCLUDE_DIR
          NAMES azure/core.hpp
          PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
          PATH_SUFFIXES include
          NO_DEFAULT_PATH
          )
elseif(NOT TILEDB_FORCE_ALL_DEPS)
  set(AZURECORE_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(AZURECORE_LIBRARIES
          NAMES
          libazure-core #*nix name
          azure-core #windows name
          PATH_SUFFIXES lib64 lib bin
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
          )
  find_path(AZURECORE_INCLUDE_DIR
          NAMES azure/core.hpp
          PATH_SUFFIXES include
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
          )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Azurecore
        REQUIRED_VARS AZURECORE_LIBRARIES AZURECORE_INCLUDE_DIR
        )

if (NOT AZURECORE_FOUND)
  if (TILEDB_SUPERBUILD)
      message(STATUS "Could NOT find azure-core")
      message(STATUS "Adding azure-core as an external project")

    set(DEPENDS)
    if (TARGET ep_openssl)
      list(APPEND DEPENDS ep_openssl)
    endif()
    if(TARGET ep_uamqp)
      list(APPEND DEPENDS ep_uamqp)
    endif()

    ExternalProject_Add(ep_azure_core
      PREFIX "externals"
      URL "https://github.com/Azure/azure-sdk-for-cpp/archive/azure-core_1.10.0.zip"
      URL_HASH SHA1=95fd8d56b439145228570c16ade5c0a73bbf6904
      DOWNLOAD_NAME azure-core_1.10.0.zip
      CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DBUILD_SHARED_LIBS=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_AZURE_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DWARNINGS_AS_ERRORS=OFF
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
	-DFETCH_SOURCE_DEPS=ON
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_azure_core)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_AZURE_CORE_EP_BUILT=TRUE
    )
  else ()
    message(FATAL_ERROR "Could not find ep_azure_core (required).")
  endif ()
endif ()

if (AZURECORE_FOUND AND NOT TARGET Azure::azure-core)
  add_library(Azure::azure-core UNKNOWN IMPORTED)
  set_target_properties(Azure::azure-core PROPERTIES
          IMPORTED_LOCATION "${AZURECORE_LIBRARIES}"
          INTERFACE_INCLUDE_DIRECTORIES "${AZURECORE_INCLUDE_DIR}"
          )
endif()

# If we built a static EP, install it if required.
if (AZURECORE_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(Azure::azure-core)
endif()
