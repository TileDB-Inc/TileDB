#
# FindAzureStorageBlobs_EP.cmake
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
# This module finds the Azure Storage Blobs SDK, installing it with an ExternalProject if
# necessary. It then defines the imported by target Azure_Storage_Blobs::Azure_Storage_Blobs.

# Include some common helper functions.
include(TileDBCommon)

# Azure dependencies
if (TILEDB_VCPKG)
    return()
endif()

###############################################################################
# Start superbuild/unmanaged/legacy version
###############################################################################

find_library(AZURE_STORAGE_BLOBS_LIBRARIES
        NAMES
        libazure-storage-blobs${CMAKE_STATIC_LIBRARY_SUFFIX}
        azure-storage-blobs${CMAKE_STATIC_LIBRARY_SUFFIX}
        PATHS "${TILEDB_EP_INSTALL_PREFIX}"
        PATH_SUFFIXES lib lib64
        NO_DEFAULT_PATH
        )

#on win32... '.lib' also used for lib ref'ing .dll!!!
#So, if perchance, in some environments, a .lib existed along with its corresponding .dll,
#then we could be incorrectly assuming/attempting a static build and probably will fail to
#appropriately include/package the .dll, since the code is (incorrectly) assuming .lib is indicative of a
#static library being used.

if (AZURE_STORAGE_BLOBS_LIBRARIES)
  set(AZURE_STORAGE_BLOBS_STATIC_EP_FOUND TRUE)
  find_path(AZURE_STORAGE_BLOBS_INCLUDE_DIR
          NAMES azure/storage/blobs.hpp
          PATHS "${TILEDB_EP_INSTALL_PREFIX}"
          PATH_SUFFIXES include
          NO_DEFAULT_PATH
          )
elseif(NOT TILEDB_FORCE_ALL_DEPS)
  set(AZURE_STORAGE_BLOBS_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(AZURE_STORAGE_BLOBS_LIBRARIES
          NAMES
          libazure-storage-blobs #*nix name
          azure-storage-blobs #windows name
          PATH_SUFFIXES lib64 lib bin
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
          )
  find_path(AZURE_STORAGE_BLOBS_INCLUDE_DIR
          NAMES azure/storage/blobs.hpp
          PATH_SUFFIXES include
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
          )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Azure_Storage_Blobs
        REQUIRED_VARS AZURE_STORAGE_BLOBS_LIBRARIES AZURE_STORAGE_BLOBS_INCLUDE_DIR
        )


if (NOT AZURE_STORAGE_BLOBS_FOUND)
  if (TILEDB_SUPERBUILD)
      message(STATUS "Could NOT find azure-storage-blobs")
      message(STATUS "Adding azure-storage-blobs as an external project")

    set(DEPENDS ep_azure_storage_common)

    ExternalProject_Add(ep_azure_storage_blobs
      PREFIX "externals"
      URL "https://github.com/Azure/azure-sdk-for-cpp/archive/azure-storage-blobs_12.6.0.zip"
      URL_HASH SHA1=4d033a68b74f486284542528e558509c8624f21d
      DOWNLOAD_NAME azure-storage-blobs_12.6.0.zip
      SOURCE_SUBDIR sdk/storage/azure-storage-blobs
      CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=$<CONFIG>
        -DBUILD_SHARED_LIBS=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DWARNINGS_AS_ERRORS=OFF
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
        -DDISABLE_AZURE_CORE_OPENTELEMETRY=ON
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_azure_storage_blobs)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_AZURE_STORAGE_BLOBS_EP_BUILT=TRUE
    )
  else ()
    message(FATAL_ERROR "Could not find ep_azure_storage_blobs (required).")
  endif ()
endif ()

if (AZURE_STORAGE_BLOBS_FOUND AND NOT TARGET Azure::azure-storage-blobs)
  add_library(Azure::azure-storage-blobs UNKNOWN IMPORTED)
  set_target_properties(Azure::azure-storage-blobs PROPERTIES
          IMPORTED_LOCATION "${AZURE_STORAGE_BLOBS_LIBRARIES}"
          INTERFACE_INCLUDE_DIRECTORIES "${AZURE_STORAGE_BLOBS_INCLUDE_DIR}"
          )
endif()

# If we built a static EP, install it if required.
if (AZURE_STORAGE_BLOBS_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(Azure::azure-storage-blobs)
endif()
