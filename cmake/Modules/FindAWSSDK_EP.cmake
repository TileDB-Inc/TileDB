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

set(AWS_SERVICES identity-management sts s3)

if(TILEDB_VCPKG)
  # Provides:  ${AWSSDK_LINK_LIBRARIES} ${AWSSDK_PLATFORM_DEPS}
  # TODO: We may need to conditionally use ${AWSSDK_PLATFORM_DEPS} here for dynamically linked deps, but
  #       it lists bare "pthread;curl" which leads to linkage of system versions. For static linkage, we
  #       handle those elsewhere at the moment.
  find_package(AWSSDK REQUIRED QUIET COMPONENTS ${AWS_SERVICES})

  if (TILEDB_STATIC)

    set(AWSSDK_EXTRA_LIBS)
    # Since AWS SDK 1.11, AWSSDK_THIRD_PARTY_LIBS was replaced by AWSSDK_COMMON_RUNTIME_LIBS.
    foreach(TARGET IN LISTS AWSSDK_THIRD_PARTY_LIBS AWSSDK_COMMON_RUNTIME_LIBS)
      if (TARGET AWS::${TARGET})
        list(APPEND AWSSDK_EXTRA_LIBS "AWS::${TARGET}")
      endif()
    endforeach()
    install_all_target_libs("${AWSSDK_EXTRA_LIBS}")
  endif()

  install_all_target_libs("${AWSSDK_LINK_LIBRARIES}")
  return()
endif()


###############################################################################
# Start superbuild/unmanaged/legacy version
###############################################################################

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
    find_package(AWSSDK CONFIG COMPONENTS ${AWS_SERVICES})
  endif()
else()
  find_package(AWSSDK CONFIG COMPONENTS ${AWS_SERVICES})
endif()

if (NOT AWSSDK_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Could NOT find AWSSDK")
    message(STATUS "Adding AWSSDK as an external project")

    set(DEPENDS)
    if (NOT WIN32 AND TARGET ep_curl)
      list(APPEND DEPENDS ep_curl)
    endif()
    if (NOT WIN32 AND TARGET ep_openssl)
      list(APPEND DEPENDS ep_openssl)
    endif()
    if (TARGET ep_zlib)
      list(APPEND DEPENDS ep_zlib)
    endif()

    # Set AWS cmake build to use specified build type except for gcc
    # For aws sdk and gcc we must always build in release mode
    # See https://github.com/TileDB-Inc/TileDB/issues/1351 and
    # https://github.com/awslabs/aws-checksums/issues/8
    set(AWS_CMAKE_BUILD_TYPE $<CONFIG>)
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

    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR NOT WIN32)
      set(CONDITIONAL_CXX_FLAGS "-DCMAKE_CXX_FLAGS=-Wno-nonnull -Wno-error=deprecated-declarations")
    endif()

    ExternalProject_Add(ep_awssdk
      PREFIX "externals"
      # Set download name to avoid collisions with only the version number in the filename
      DOWNLOAD_NAME ep_awssdk.zip
      # We download with git clone because the repository has submodules
      GIT_REPOSITORY "https://github.com/aws/aws-sdk-cpp.git"
      GIT_TAG "1.11.160"
      CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${AWS_CMAKE_BUILD_TYPE}
        -DENABLE_TESTING=OFF
        -DAWS_SDK_WARNINGS_ARE_ERRORS=OFF
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
  if (TILEDB_STATIC)
    set(AWSSDK_EXTRA_LIBS)
    # Since AWS SDK 1.11, AWSSDK_THIRD_PARTY_LIBS was replaced by AWSSDK_COMMON_RUNTIME_LIBS.
    foreach(TARGET IN LISTS AWSSDK_THIRD_PARTY_LIBS AWSSDK_COMMON_RUNTIME_LIBS)
      if (TARGET AWS::${TARGET})
        list(APPEND AWSSDK_EXTRA_LIBS "AWS::${TARGET}")
      endif()
    endforeach()
    install_all_target_libs("${AWSSDK_EXTRA_LIBS}")
  endif()

  install_all_target_libs("${AWSSDK_LINK_LIBRARIES}")
endif ()
