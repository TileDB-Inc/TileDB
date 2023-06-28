#
# FindAzureSDK_EP.cmake
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
# This module finds the Azure C++ SDK, installing it with an ExternalProject if
# necessary. It then defines the imported target AzureSDK::AzureSDK.

# Include some common helper functions.
include(TileDBCommon)

# Azure dependencies
if (TILEDB_VCPKG)
    message(STATUS "The TileDB library is compiled with Azure support.")
    find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
    if (NOT TARGET LibLZMA)
        find_package(LibLZMA REQUIRED)
    endif()
    if (NOT TARGET LibXml2::LibXml2)
        find_package(libxml2 CONFIG REQUIRED)
        install_target_libs(LibXml2::LibXml2)
    endif()
    SET(AzureSDK_LINK_LIBRARIES "Azure::azure-storage-blobs;Azure::azure-storage-common;Azure::azure-core")
    SET(AzureSDK_EXTRA_LIBS "LibXml2::LibXml2;LibLZMA::LibLZMA")
#    target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
#            INTERFACE
#            Azure::azure-storage-blobs
#            Azure::azure-storage-common
#            Azure::azure-core
#            LibXml2::LibXml2
#            LibLZMA::LibLZMA
#            )
    install_all_target_libs("Azure::azure-storage-blobs;Azure::azure-storage-common;Azure::azure-core")
    return()
endif()

###############################################################################
# Start superbuild/unmanaged/legacy version
###############################################################################

# This is the install subdirectory for azure headers and libs, to avoid pollution
set(TILEDB_EP_AZURE_INSTALL_PREFIX "${TILEDB_EP_INSTALL_PREFIX}/azurecpplite")

# First check for a static version in the EP prefix.
#find_package(azure-storage-blobs-cpp
#        PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
#        ${TILEDB_DEPS_NO_DEFAULT_PATH}
#        REQUIRED
#)

find_library(AZURESDK_LIBRARIES
  NAMES
    libazurestorage${CMAKE_STATIC_LIBRARY_SUFFIX}
    azurestorage${CMAKE_STATIC_LIBRARY_SUFFIX}
  PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH
)

#on win32... '.lib' also used for lib ref'ing .dll!!!
#So, if perchance, in some environments, a .lib existed along with its corresponding .dll,
#then we could be incorrectly assuming/attempting a static build and probably will fail to
#appropriately include/package the .dll, since the code is (incorrectly) assuming .lib is indicative of a
#static library being used.

if (AZURESDK_LIBRARIES)
  set(AZURESDK_STATIC_EP_FOUND TRUE)
  find_path(AZURESDK_INCLUDE_DIR
    NAMES was/blob.h
    PATHS "${TILEDB_EP_AZURE_INSTALL_PREFIX}"
    PATH_SUFFIXES include
    NO_DEFAULT_PATH
  )
elseif(NOT TILEDB_FORCE_ALL_DEPS)
  set(AZURESDK_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(AZURESDK_LIBRARIES
    NAMES
      libazurestorage #*nix name
      azurestorage #windows name
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
  find_path(AZURESDK_INCLUDE_DIR
    NAMES was/blob.h
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(AzureSDK
  REQUIRED_VARS AZURESDK_LIBRARIES AZURESDK_INCLUDE_DIR
)

if (NOT AZURESDK_FOUND)
  if (TILEDB_SUPERBUILD)
      message(STATUS "Could NOT find AzureSDK")
      message(STATUS "Adding AzureSDK as an external project")

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
    if (TARGET ep_casablanca)
      list(APPEND DEPENDS ep_casablanca)
    endif()
    if (TARGET ep_azure_core)
      list(APPEND DEPENDS ep_azure_core)
    endif()
    if (TARGET ep_azure_storage_common)
      list(APPEND DEPENDS ep_azure_storage_common)
    endif()

    if(WIN32)
      ExternalProject_Add(ep_azuresdk
        PREFIX "externals"
        URL "https://github.com/Azure/azure-storage-cpp/archive/v7.5.0.zip"
        URL_HASH SHA1=f1be570a9383ce975506ffb92d81a561abd57486
        DOWNLOAD_NAME azure-storage-cpp-v7.5.0.zip
        CMAKE_ARGS
          -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
          -DBUILD_SHARED_LIBS=OFF
          -DBUILD_TESTS=OFF
          -DBUILD_SAMPLES=OFF
          -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
          -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_AZURE_INSTALL_PREFIX}
          -DCMAKE_CXX_FLAGS=${CXXFLAGS_DEF}
          -DCMAKE_C_FLAGS=${CFLAGS_DEF}
          -DCMAKE_FIND_FRAMEWORK=LAST
          -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
          -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
          -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
        SOURCE_SUBDIR Microsoft.WindowsAzure.Storage
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
        LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
        DEPENDS ${DEPENDS}
    )
    else()
      ExternalProject_Add(ep_azuresdk
        PREFIX "externals"
        URL "https://github.com/Azure/azure-storage-cpp/archive/v7.5.0.zip"
        URL_HASH SHA1=f1be570a9383ce975506ffb92d81a561abd57486
        DOWNLOAD_NAME azure-storage-cpp-v7.5.0.zip
        CMAKE_ARGS
          -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
          -DBUILD_SHARED_LIBS=OFF
          -DCMAKE_POSITION_INDEPENDENT_CODE=ON
          -DBUILD_TESTS=OFF
          -DBUILD_SAMPLES=OFF
          -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
          -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_AZURE_INSTALL_PREFIX}
          -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
          -DCMAKE_FIND_FRAMEWORK=LAST
        SOURCE_SUBDIR Microsoft.WindowsAzure.Storage
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
        LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
        DEPENDS ${DEPENDS}
      )
    endif()

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_azuresdk)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_AZURESDK_EP_BUILT=TRUE
    )
  else ()
    message(FATAL_ERROR "Could not find AZURESDK (required).")
  endif ()
endif ()

if (AZURESDK_FOUND AND NOT TARGET Azure::azure-storage-blobs)
  add_library(Azure::azure-storage-blobs UNKNOWN IMPORTED)
  set_target_properties(Azure::azure-storage-blobs PROPERTIES
    IMPORTED_LOCATION "${AZURESDK_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${AZURESDK_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (AZURESDK_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(Azure::azure-storage-blobs)
endif()
