#
# FindGCSSDK_EP.cmake
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
# This module finds the GCS C++ SDK, installing it with an ExternalProject if
# necessary. It then defines the imported targets GCSSDK::<component>, e.g.
# GCSSDK::storage-client or GCSSDK::google_cloud_cpp_common.

# Include some common helper functions.
include(TileDBCommon)

# If the EP was built, it will install the storage_client-config.cmake file,
# which we can use with find_package. CMake uses CMAKE_PREFIX_PATH to locate find
# modules.
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${TILEDB_EP_INSTALL_PREFIX}")

# Try searching for the SDK in the EP prefix.
set(GCSSDK_DIR "${TILEDB_EP_INSTALL_PREFIX}")

# First check for a static version in the EP prefix.
find_library(GCSSDK_LIBRARY
  NAMES
    libstorage_client${CMAKE_STATIC_LIBRARY_SUFFIX}
  PATHS ${TILEDB_EP_INSTALL_PREFIX}
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH
)

if (GCSSDK_LIBRARY)
  set(GCSSDK_STATIC_EP_FOUND TRUE)
  find_path(GCSSDK_INCLUDE_DIR
    NAMES client.h
    PATHS ${TILEDB_EP_INSTALL_PREFIX}
    PATH_SUFFIXES include/google/cloud/storage
    NO_DEFAULT_PATH
  )
elseif(NOT TILEDB_FORCE_ALL_DEPS)
  set(GCSSDK_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(GCSSDK_LIBRARY
    NAMES
      libstorage_client
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
  find_path(GCSSDK_INCLUDE_DIR
    NAMES client.h
    PATH_SUFFIXES include/google/cloud/storage
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GCSSDK
  REQUIRED_VARS GCSSDK_LIBRARY GCSSDK_INCLUDE_DIR
)

if (NOT GCSSDK_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Could NOT find GCSSDK")
    message(STATUS "Adding GCSSDK as an external project")

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

    # Fetch the number of CPUs on the this sytem.
    include(ProcessorCount)
    processorcount(NCPU)

    ExternalProject_Add(ep_gcssdk
      PREFIX "externals"
      URL "https://github.com/googleapis/google-cloud-cpp/archive/v0.20.0.zip"
      URL_HASH SHA1=6e7931a0e62779dfbd07427373373bf1ad6f8686
      BUILD_IN_SOURCE 1
      PATCH_COMMAND
        patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/build.patch &&
        patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/ls.patch
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND} -Hsuper -Bcmake-out
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DBUILD_SHARED_LIBS=OFF
        -DBUILD_SAMPLES=OFF
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DOPENSSL_ROOT_DIR=${TILEDB_OPENSSL_DIR}
      BUILD_COMMAND ${CMAKE_COMMAND} --build cmake-out -- -j${NCPU}
      INSTALL_COMMAND
          cp -r ${TILEDB_EP_SOURCE_DIR}/ep_gcssdk/cmake-out/external/lib ${TILEDB_EP_INSTALL_PREFIX}
        COMMAND
          cp -r ${TILEDB_EP_SOURCE_DIR}/ep_gcssdk/cmake-out/external/include ${TILEDB_EP_INSTALL_PREFIX}
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_gcssdk)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_GCSSDK_EP_BUILT=TRUE
    )

  endif()
endif()

if (GCSSDK_FOUND)
  # Build a list of all GCS libraries to link with.
  list(APPEND GCSSDK_LINKED_LIBS "storage_client"
                                 "google_cloud_cpp_common"
                                 "crc32c")

  foreach (LIB ${GCSSDK_LINKED_LIBS})
    find_library(GCS_FOUND_${LIB}
      NAMES lib${LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}
      PATHS ${TILEDB_EP_INSTALL_PREFIX}
      PATH_SUFFIXES lib
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )

    message(STATUS "Found GCS lib: ${LIB} (${GCS_FOUND_${LIB}})")
    if (NOT TARGET GCSSDK::${LIB})
      add_library(GCSSDK::${LIB} UNKNOWN IMPORTED)
      set_target_properties(GCSSDK::${LIB} PROPERTIES
        IMPORTED_LOCATION "${GCS_FOUND_${LIB}}"
        INTERFACE_INCLUDE_DIRECTORIES ${TILEDB_EP_INSTALL_PREFIX}/include
      )
    endif()

    # If we built a static EP, install it if required.
    if (GCSSDK_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
      install_target_libs(GCSSDK::${LIB})
    endif()

  endforeach ()
endif()
