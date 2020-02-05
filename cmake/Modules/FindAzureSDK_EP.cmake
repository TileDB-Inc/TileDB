#
# FindAzureSDK_EP.cmake
#
#
# The MIT License
#
# Copyright (c) 2018-2020 TileDB, Inc.
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
# This module finds the Azure C++ SDK, installing it with an ExternalProject if
# necessary. It then defines the imported target AzureSDK::AzureSDK.

# Include some common helper functions.
include(TileDBCommon)

# First check for a static version in the EP prefix.
find_library(AZURESDK_LIBRARIES
  NAMES
    libazurestorage${CMAKE_STATIC_LIBRARY_SUFFIX}
  PATHS ${TILEDB_EP_INSTALL_PREFIX}
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH
)

if (AZURESDK_LIBRARIES)
  set(AZURESDK_STATIC_EP_FOUND TRUE)
else()
  set(AZURESDK_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(AZURESDK_LIBRARIES
    NAMES
      libazurestorage
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(AzureSDK
  REQUIRED_VARS AZURESDK_LIBRARIES
)

if (NOT AZURESDK_FOUND)
  if (TILEDB_SUPERBUILD)
    set(DEPENDS)
    if (TARGET ep_casablanca)
      list(APPEND DEPENDS ep_casablanca)
    endif()

    ExternalProject_Add(ep_azuresdk
      PREFIX "externals"
      URL "https://github.com/Azure/azure-storage-cpp/archive/v7.1.0.zip"
      URL_HASH SHA1=b9cf8d282312cc49320cd26bdeba34d19f5f79e2
      CMAKE_ARGS
        -S ${TILEDB_EP_BASE}/src/ep_azuresdk/Microsoft.WindowsAzure.Storage
        -DCMAKE_BUILD_TYPE=Release
        -DBUILD_SHARED_LIBS=OFF
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_azuresdk)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_AZURESDK_EP_BUILT=TRUE
    )
  else ()
    message(FATAL_ERROR "Could not find AZURESDK (required).")
  endif ()
endif ()

if (AZURESDK_FOUND AND NOT TARGET AzureSDK::AzureSDK)
  add_library(AzureSDK::AzureSDK UNKNOWN IMPORTED)
  set_target_properties(AzureSDK::AzureSDK PROPERTIES
    IMPORTED_LOCATION "${AZURESDK_LIBRARIES}"
    #INTERFACE_INCLUDE_DIRECTORIES "${AZURESDK_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (AZURESDK_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(AzureSDK::AzureSDK)
endif()