#
# FindAWSSDK_EP.cmake
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
# This module finds the AWS C++ SDK, installing it with an ExternalProject if
# necessary. It then defines the imported targets AWSSDK::<component>, e.g.
# AWSSDK::aws-cpp-sdk-s3 or AWSSDK::aws-cpp-sdk-core.

# Include some common helper functions.
include(TileDBCommon)

##-----------------------------------
# early WIN32 audit of path length for aws sdk build where
# insufficient available path length causes sdk build failure.
if (WIN32 AND NOT TILEDB_SKIP_S3AWSSDK_DIR_LENGTH_CHECK)
  if (TILEDB_SUPERBUILD AND TILEDB_S3)
    string(LENGTH ${CMAKE_CURRENT_BINARY_DIR} LENGTH_CMAKE_CURRENT_BINARY_DIR)
    if ( NOT (LENGTH_CMAKE_CURRENT_BINARY_DIR LESS 61))
      message(FATAL_ERROR " build directory path likely too long for building awssdk/dependencies!")
      return()
    endif()
  endif()
endif()
##-----------------------------------

# If the EP was built, it will install the AWSSDKConfig.cmake file, which we
# can use with find_package. CMake uses CMAKE_PREFIX_PATH to locate find
# modules.
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${TILEDB_EP_INSTALL_PREFIX}")

if(DEFINED ENV{AWSSDK_ROOT_DIR})
  set(AWSSDK_ROOT_DIR $ENV{AWSSDK_ROOT_DIR})
else()
  set(AWSSDK_ROOT_DIR "${TILEDB_EP_INSTALL_PREFIX}")
endif()

# Check to see if the SDK is installed (which provides the find module).
# This will either use the system-installed AWSSDK find module (if present),
# or the superbuild-installed find module.
if (TILEDB_SUPERBUILD)
  # Don't use find_package in superbuild if we are forcing all deps.
  # That's because the AWSSDK config file hard-codes a search of /usr,
  # /usr/local, etc.
  if (NOT TILEDB_FORCE_ALL_DEPS)
    find_package(AWSSDK CONFIG)
  endif()
else()
  find_package(AWSSDK CONFIG)
endif()

if (NOT AWSSDK_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Could NOT find AWSSDK")
    message(STATUS "Adding AWSSDK as an external project")

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

    # Set AWS cmake build to use specified build type except for gcc
    # For aws sdk and gcc we must always build in release mode
    # See https://github.com/TileDB-Inc/TileDB/issues/1351 and
    # https://github.com/awslabs/aws-checksums/issues/8
    set(AWS_CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE})
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(AWS_CMAKE_BUILD_TYPE "Release")
    endif()

    # Work around for: https://github.com/aws/aws-sdk-cpp/pull/1187
    # AWS SDK builds it's "aws-common" dependencies using `execute_process` to run cmake directly,
    # and does not pass the CMAKE_GENERATOR PLATFORM, so those projects default to Win32 builds,
    # causing linkage errors.
    if (CMAKE_GENERATOR MATCHES "Visual Studio 14.*" OR CMAKE_GENERATOR MATCHES "Visual Studio 15.*"
      AND NOT CMAKE_GENERATOR MATCHES ".*Win64")

      set(_CMKGEN_OLD "${CMAKE_GENERATOR}")
      set(_CMKPLT_OLD "${CMAKE_GENERATOR_PLATFORM}")
      set(CMAKE_GENERATOR "${CMAKE_GENERATOR} Win64")
      set(CMAKE_GENERATOR_PLATFORM "")
    endif()

    if (WIN32)
      find_package(Git REQUIRED)
      set(CONDITIONAL_PATCH cd ${CMAKE_SOURCE_DIR} && ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_awssdk < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_awssdk/awsccommon.patch &&
                                                      ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_awssdk < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_awssdk/awsconfig_cmake_3.22.patch)
    else()
      set(CONDITIONAL_PATCH patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_awssdk/awsccommon.patch &&
                            patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_awssdk/awsconfig_cmake_3.22.patch)
    endif()
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR NOT WIN32)
      set(CONDITIONAL_CXX_FLAGS "-DCMAKE_CXX_FLAGS=-Wno-nonnull -Wno-error=deprecated-declarations")
    endif()

    ExternalProject_Add(ep_awssdk
      PREFIX "externals"
      # Set download name to avoid collisions with only the version number in the filename
      DOWNLOAD_NAME ep_awssdk.zip
      URL "https://github.com/aws/aws-sdk-cpp/archive/1.8.84.zip"
      URL_HASH SHA1=e32a53a01c75ca7fdfe9feed9c5bbcedd98708e3
      PATCH_COMMAND
        ${CONDITIONAL_PATCH}
      CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${AWS_CMAKE_BUILD_TYPE}
        -DENABLE_TESTING=OFF
        -DBUILD_ONLY=s3\\$<SEMICOLON>core\\$<SEMICOLON>identity-management\\$<SEMICOLON>sts
        -DBUILD_SHARED_LIBS=OFF
        -DCMAKE_INSTALL_BINDIR=lib
        -DENABLE_UNITY_BUILD=ON
        -DCUSTOM_MEMORY_MANAGEMENT=0
        ${CONDITIONAL_CXX_FLAGS}
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DCMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}
        -DCMAKE_OSX_SYSROOT=${CMAKE_OSX_SYSROOT}
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    # restore cached values
    if (DEFINED _CMKGEN_OLD)

      set(CMAKE_GENERATOR "${_CMKGEN_OLD}")
      set(CMAKE_GENERATOR_PLATFORM "${_CMKPLT_OLD}")
    endif()

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_awssdk)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_AWSSDK_EP_BUILT=TRUE
    )
  else ()
    message(FATAL_ERROR "Could not find AWSSDK (required).")
  endif ()
endif ()

if (AWSSDK_FOUND)
  set(AWS_SERVICES s3)
  AWSSDK_DETERMINE_LIBS_TO_LINK(AWS_SERVICES AWS_LINKED_LIBS)
  list(APPEND AWS_LINKED_LIBS aws-c-common
                              aws-c-event-stream
                              aws-checksums
                              aws-cpp-sdk-sts
                              aws-cpp-sdk-identity-management)

  foreach (LIB ${AWS_LINKED_LIBS})
    if (NOT ${LIB} MATCHES "aws-*")
      continue()
    endif()

    find_library("AWS_FOUND_${LIB}"
      NAMES ${LIB}
      PATHS ${AWSSDK_LIB_DIR}
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )
    message(STATUS "Found AWS lib: ${LIB} (${AWS_FOUND_${LIB}})")
    if (NOT TARGET AWSSDK::${LIB})
      add_library(AWSSDK::${LIB} UNKNOWN IMPORTED)
      set_target_properties(AWSSDK::${LIB} PROPERTIES
        IMPORTED_LOCATION "${AWS_FOUND_${LIB}}"
        INTERFACE_INCLUDE_DIRECTORIES "${AWSSDK_INCLUDE_DIR}"
      )
    endif()

    # If we built a static EP, install it if required.
    if (TILEDB_AWSSDK_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
      install_target_libs(AWSSDK::${LIB})
    endif()

  endforeach ()

  # the AWSSDK does not include links to some transitive dependencies
  # ref: github<dot>com/aws<slash>aws-sdk-cpp/issues/1074#issuecomment-466252911
  if (WIN32)
    list(APPEND AWS_EXTRA_LIBS userenv ws2_32 wininet winhttp bcrypt version)
  endif()
endif ()
