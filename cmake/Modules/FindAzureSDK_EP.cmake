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

# First check for a static version in the EP prefix.
find_library(AZURESDK_LIBRARIES
  NAMES
    libazure-storage-lite${CMAKE_STATIC_LIBRARY_SUFFIX}
    azure-storage-lite${CMAKE_STATIC_LIBRARY_SUFFIX}
  PATHS ${TILEDB_EP_INSTALL_PREFIX}
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
    NAMES get_blob_request_base.h
    PATHS ${TILEDB_EP_INSTALL_PREFIX}
    PATH_SUFFIXES include
    NO_DEFAULT_PATH
  )
elseif(NOT TILEDB_FORCE_ALL_DEPS)
  set(AZURESDK_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(AZURESDK_LIBRARIES
    NAMES
      libazure-storage-lite #*nix name
      azure-storage-lite #windows name
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
  find_path(AZURESDK_INCLUDE_DIR
    NAMES get_blob_request_base.h
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
      if(MSVC)
        set(CXXFLAGS_DEF " -I${TILEDB_EP_INSTALL_PREFIX}/include /Dazure_storage_lite_EXPORTS /DCURL_STATICLIB=1 ${CMAKE_CXX_FLAGS}")
        set(CFLAGS_DEF " -I${TILEDB_EP_INSTALL_PREFIX}/include /Dazure_storage_lite_EXPORTS                  ${CMAKE_C_FLAGS}")
      endif()
    else()
      #put our switch first in case other items are empty, leaving problematic blank space at beginning
      set(CFLAGS_DEF "-fPIC ${CMAKE_C_FLAGS}")
      set(CXXFLAGS_DEF "-fPIC ${CMAKE_CXX_FLAGS}")
    endif()
    
    if (WIN32)
        # needed for applying patches on windows
        find_package(Git REQUIRED)
        #see comment on this answer - https://stackoverflow.com/a/45698220
        #and this - https://stackoverflow.com/a/62967602 (same thread, different answer/comment)
    else()
        set(PATCH patch -N -p1)
    endif()
    
    if(WIN32)
      ExternalProject_Add(ep_azuresdk
        PREFIX "externals"
        URL "https://github.com/Azure/azure-storage-cpplite/archive/v0.2.0.zip"
        URL_HASH SHA1=058975ccac9b60b522c9f7fd044a3d2aaec9f893
        CMAKE_ARGS
          -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
          -DBUILD_SHARED_LIBS=OFF
          -DBUILD_TESTS=OFF
          -DBUILD_SAMPLES=OFF
          -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
          -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
          -DCMAKE_CXX_FLAGS=${CXXFLAGS_DEF}
          -DCMAKE_C_FLAGS=${CFLAGS_DEF}
          -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        PATCH_COMMAND
          cd ${CMAKE_SOURCE_DIR} &&
          ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_azuresdk < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_azuresdk/remove-uuid-dep.patch &&
          ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_azuresdk < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_azuresdk/azurite-support.patch &&
          ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --check --apply --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_azuresdk < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_azuresdk/azure-storage-lite-base64.patch &&
          ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --check --apply --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_azuresdk < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_azuresdk/azure-storage-lite-storage_url.patch
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
        URL "https://github.com/Azure/azure-storage-cpplite/archive/v0.2.0.zip"
        URL_HASH SHA1=058975ccac9b60b522c9f7fd044a3d2aaec9f893
        CMAKE_ARGS
          -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
          -DBUILD_SHARED_LIBS=OFF
          -DBUILD_TESTS=OFF
          -DBUILD_SAMPLES=OFF
          -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
          -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
          -DCMAKE_CXX_FLAGS=${CXXFLAGS_DEF}
          -DCMAKE_C_FLAGS=${CFLAGS_DEF}
          -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        PATCH_COMMAND
          echo starting patching for azure &&
          ${PATCH} < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_azuresdk/remove-uuid-dep.patch &&
          ${PATCH} < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_azuresdk/azurite-support.patch &&
          echo done patches for azure
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

if (AZURESDK_FOUND AND NOT TARGET AzureSDK::AzureSDK)
  add_library(AzureSDK::AzureSDK UNKNOWN IMPORTED)
  set_target_properties(AzureSDK::AzureSDK PROPERTIES
    IMPORTED_LOCATION "${AZURESDK_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${AZURESDK_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (AZURESDK_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(AzureSDK::AzureSDK)
endif()
