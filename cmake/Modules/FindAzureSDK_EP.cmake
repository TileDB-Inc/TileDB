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
    libazure-storage-lite${CMAKE_STATIC_LIBRARY_SUFFIX}
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
      libazure-storage-lite
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
    if (TARGET ep_curl)
      list(APPEND DEPENDS ep_curl)
    endif()
    if (TARGET ep_openssl)
      list(APPEND DEPENDS ep_openssl)
    endif()
    if (TARGET ep_zlib)
      list(APPEND DEPENDS ep_zlib)
    endif()

    if (WIN32)
      set(CFLAGS_DEF "${CMAKE_C_FLAGS}")
      set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS}")
    else()
      set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
      set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS} -fPIC")
    endif()

    ExternalProject_Add(ep_azuresdk
      PREFIX "externals"
      URL "https://github.com/Azure/azure-storage-cpplite/archive/v0.2.0.zip"
      URL_HASH SHA1=058975ccac9b60b522c9f7fd044a3d2aaec9f893
      CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=Release
        -DBUILD_SHARED_LIBS=OFF
        -DBUILD_TESTS=OFF
        -DBUILD_SAMPLES=OFF
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_CXX_FLAGS=-fPIC
        -DCMAKE_C_FLAGS=-fPIC
      #INSTALL_COMMAND $(MAKE) CXXFLAGS=-fPIC PREFIX=${TILEDB_EP_INSTALL_PREFIX} install
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
  )
endif()

# If we built a static EP, install it if required.
if (AZURESDK_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(AzureSDK::AzureSDK)
endif()
