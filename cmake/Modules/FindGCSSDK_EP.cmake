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
# The storage client is installed as the cmake package storage_client
# TODO: This should be replaced with proper find_package as google installs cmake targets for the subprojects
if (NOT TILEDB_FORCE_ALL_DEPS OR TILEDB_GCSSDK_EP_BUILT)
  find_package(storage_client
    PATHS ${TILEDB_EP_INSTALL_PREFIX}
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
  set(GCSSDK_FOUND ${storage_client_FOUND})
endif()

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
      URL "https://github.com/googleapis/google-cloud-cpp/archive/v1.14.0.zip"
      URL_HASH SHA1=2cc7e5a3b62fb37f1accd405818557e990b91190
      BUILD_IN_SOURCE 1
      PATCH_COMMAND
        patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/build.patch &&
        patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/ls.patch &&
        patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/disable_tests.patch &&
        patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/disable_examples.patch
      CONFIGURE_COMMAND
         ${CMAKE_COMMAND} -Hsuper -Bcmake-out
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DBUILD_SHARED_LIBS=OFF
        -DBUILD_SAMPLES=OFF
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DOPENSSL_ROOT_DIR=${TILEDB_OPENSSL_DIR}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        # Disable unused api features to speed up build
        -DGOOGLE_CLOUD_CPP_ENABLE_BIGQUERY=OFF
        -DGOOGLE_CLOUD_CPP_ENABLE_BIGTABLE=OFF
        -DGOOGLE_CLOUD_CPP_ENABLE_SPANNER=OFF
        -DGOOGLE_CLOUD_CPP_ENABLE_FIRESTORE=OFF
        -DGOOGLE_CLOUD_CPP_ENABLE_STORAGE=ON
        -DGOOGLE_CLOUD_CPP_ENABLE_PUBSUB=OFF
        -DBUILD_TESTING=OFF
        # Google uses their own variable instead of CMAKE_INSTALL_PREFIX
        -DGOOGLE_CLOUD_CPP_EXTERNAL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
      BUILD_COMMAND ${CMAKE_COMMAND} --build cmake-out -- -j${NCPU}
      # There is no install command, the build process installs the libraries
      INSTALL_COMMAND ""
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
  else()
    message(FATAL_ERROR "Unable to find GCSSDK")
  endif()
endif()

# If we found the SDK but it didn't have a cmake target build them
if (GCSSDK_FOUND AND NOT TARGET storage_client)
  # Build a list of all GCS libraries to link with.
  list(APPEND GCSSDK_LINKED_LIBS "storage_client"
                                 "google_cloud_cpp_common"
                                 "crc32c")

  foreach (LIB ${GCSSDK_LINKED_LIBS})
    find_library(GCS_FOUND_${LIB}
      NAMES lib${LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}
      PATHS ${TILEDB_EP_INSTALL_PREFIX}
      PATH_SUFFIXES lib lib64
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )
    message(STATUS "Found GCS lib: ${LIB} (${GCS_FOUND_${LIB}})")

    # If we built a static EP, install it if required.
    if (GCSSDK_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
      install_target_libs(GCSSDK::${LIB})
    endif()
  endforeach ()

  if (NOT TARGET storage_client)
    add_library(storage_client UNKNOWN IMPORTED)
    set_target_properties(storage_client PROPERTIES
      IMPORTED_LOCATION "${GCS_FOUND_storage_client};${GCS_FOUND_google_cloud_cpp_common};${GCS_FOUND_crc32c}"
      INTERFACE_INCLUDE_DIRECTORIES ${TILEDB_EP_INSTALL_PREFIX}/include
    )
  endif()
endif()
