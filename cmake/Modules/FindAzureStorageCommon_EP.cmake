#
# FindAzureStorageCommon_EP.cmake
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
# This module finds the Azure Storage Common SDK, installing it with an ExternalProject if
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

find_library(AZURE_STORAGE_COMMON_LIBRARIES
        NAMES
        libazure-storage-common${CMAKE_STATIC_LIBRARY_SUFFIX}
        azure-storage-common${CMAKE_STATIC_LIBRARY_SUFFIX}
        PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
        PATH_SUFFIXES lib lib64
        NO_DEFAULT_PATH
        )
find_library(AZURE_STORAGE_BLOBS_LIBRARIES
        NAMES
        libazure-storage-blobs${CMAKE_STATIC_LIBRARY_SUFFIX}
        azure-storage-blobs${CMAKE_STATIC_LIBRARY_SUFFIX}
        PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
        PATH_SUFFIXES lib lib64
        NO_DEFAULT_PATH
        )

#on win32... '.lib' also used for lib ref'ing .dll!!!
#So, if perchance, in some environments, a .lib existed along with its corresponding .dll,
#then we could be incorrectly assuming/attempting a static build and probably will fail to
#appropriately include/package the .dll, since the code is (incorrectly) assuming .lib is indicative of a
#static library being used.

if (AZURE_STORAGE_COMMON_LIBRARIES)
  set(AZURE_STORAGE_COMMON_STATIC_EP_FOUND TRUE)
  find_path(AZURE_STORAGE_COMMON_INCLUDE_DIR
          NAMES azure/storage/blobs.hpp
          PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
          PATH_SUFFIXES include
          NO_DEFAULT_PATH
          )
elseif(NOT TILEDB_FORCE_ALL_DEPS)
  set(AZURE_STORAGE_COMMON_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(AZURE_STORAGE_COMMON_LIBRARIES
          NAMES
          libazure-storage-common #*nix name
          azure-storage-common #windows name
          PATH_SUFFIXES lib64 lib bin
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
          )
  find_library(AZURE_STORAGE_BLOBS_LIBRARIES
          NAMES
          libazure-storage-blobs #*nix name
          azure-storage-blobs #windows name
          PATH_SUFFIXES lib64 lib bin
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
          )
  find_path(AZURE_STORAGE_COMMON_INCLUDE_DIR
          NAMES azure/storage/blobs.hpp
          PATH_SUFFIXES include
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
          )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Azure_Storage_Common
        REQUIRED_VARS AZURE_STORAGE_COMMON_LIBRARIES AZURE_STORAGE_COMMON_INCLUDE_DIR
        )


if (NOT AZURE_STORAGE_COMMON_FOUND)
  if (TILEDB_SUPERBUILD)
      message(STATUS "Could NOT find azure-storage-common")
      message(STATUS "Adding azure-storage-common as an external project")

    set(DEPENDS)
    if (TARGET ep_openssl)
      list(APPEND DEPENDS ep_openssl)
    endif()

    ExternalProject_Add(ep_azure_storage_common
      PREFIX "externals"
      URL "https://github.com/Azure/azure-sdk-for-cpp/archive/azure-storage-common_12.3.2.zip"
      URL_HASH SHA1=09fc97b3f4c4f8e8976704dfc1ecefd14b2ed1bc
      DOWNLOAD_NAME azure-storage-common_12.3.2.zip
      CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DBUILD_SHARED_LIBS=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_AZURE_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DDWARNINGS_AS_ERRORS=OFF
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_azure_storage_common)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_AZURE_STORAGE_COMMON_EP_BUILT=TRUE
    )
  else ()
    message(FATAL_ERROR "Could not find ep_azure_storage_common (required).")
  endif ()
endif ()

if (AZURE_STORAGE_COMMON_FOUND AND NOT TARGET Azure::azure-storage-common)
  add_library(Azure::azure-storage-common UNKNOWN IMPORTED)
  set_target_properties(Azure::azure-storage-common PROPERTIES
          IMPORTED_LOCATION "${AZURE_STORAGE_COMMON_LIBRARIES}"
          INTERFACE_INCLUDE_DIRECTORIES "${AZURE_STORAGE_COMMON_INCLUDE_DIR}"
          )
  add_library(Azure::azure-storage-common-blobs UNKNOWN IMPORTED)
  set_target_properties(Azure::azure-storage-common-blobs PROPERTIES
          IMPORTED_LOCATION "${AZURE_STORAGE_BLOBS_LIBRARIES}"
          INTERFACE_INCLUDE_DIRECTORIES "${AZURE_STORAGE_COMMON_INCLUDE_DIR}"
          )
#  target_link_libraries(Azure::azure-storage-common Azure::azure-storage-common-blobs)
endif()

# If we built a static EP, install it if required.
if (AZURE_STORAGE_COMMON_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(Azure::azure-storage-common Azure::azure-storage-common-blobs)
endif()