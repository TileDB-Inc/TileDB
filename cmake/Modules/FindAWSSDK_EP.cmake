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
      set(CONDITIONAL_PATCH cd ${CMAKE_SOURCE_DIR} && ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_awssdk < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_awssdk/aws_event_loop_windows.v1.9.291.patch)
    else()
      set(CONDITIONAL_PATCH "")
    endif()

    ExternalProject_Add(ep_awssdk
      PREFIX "externals"
      # Set download name to avoid collisions with only the version number in the filename
      DOWNLOAD_NAME ep_awssdk.zip
      GIT_REPOSITORY "https://github.com/aws/aws-sdk-cpp"
      GIT_TAG 1.9.291
      GIT_SUBMODULES_RECURSE ON #TBD: Seems 'ON' may be default, is this needed? Is this correct?
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
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DCMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}
        -DCMAKE_OSX_SYSROOT=${CMAKE_OSX_SYSROOT}
        -DBUILD_DEPS=ON
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
  # append these libs in an order manually, by trial and error, determined to support successful
  # linking of dependencies on *nix.  (windows doesn't seem to care or 'just lucked out'.)
  list(APPEND AWS_LINKED_LIBS #aws-c-common
          aws-cpp-sdk-s3
        aws-cpp-sdk-core
        aws-crt-cpp
        #AWSSDK::aws-c-auth
        #AWSSDK::aws-c-cal
        #AWSSDK::aws-c-common
        #AWSSDK::aws-c-compression
        aws-c-event-stream
        #xAWSSDK::aws-c-io
        aws-c-mqtt
        aws-c-s3
        aws-c-auth
        aws-c-http
        aws-c-compression
        aws-checksums
        aws-c-sdkutils
        aws-crt-cpp
        #xAWSSDK::aws-c-s3
        aws-cpp-sdk-s3
        aws-c-event-stream
        aws-c-mqtt
        aws-checksums
        aws-c-auth
        #AWSSDK::aws-c-http
        aws-c-sdkutils
        aws-c-io
        aws-c-compression
        aws-c-cal
        aws-cpp-sdk-cognito-identity
        aws-cpp-sdk-identity-management
        aws-cpp-sdk-sts
        aws-c-common
        #s2n # There is a libs2n.a generated and in install/lib path... but not an INTERFACE, hmm...
  )
  if (NOT WIN32)
    list(APPEND AWS_LINKED_LIBS #aws-c-common
                              s2n # There is a libs2n.a generated and in install/lib path... but not an INTERFACE, hmm...
    )
  endif()

  set(TILEDB_AWS_LINK_TARGETS "")
  foreach (LIB ${AWS_LINKED_LIBS})
    if ((NOT ${LIB} MATCHES "aws-*" )
        AND (NOT LIB STREQUAL "s2n") )
      message(STATUS "AWS_LINKED_LIBS, skipping ${LIB}")
      continue()
    endif()

    find_library("AWS_FOUND_${LIB}"
      NAMES ${LIB}
      PATHS ${AWSSDK_LIB_DIR}
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )
    if ("${AWS_FOUND_${LIB}}" MATCHES ".*-NOTFOUND")
      message(STATUS "not found, ${LIB}")
      continue()
    else()
      message(STATUS "for ${LIB} adding link to ${AWS_FOUND_${LIB}}")
    endif()
    if (NOT TARGET AWSSDK::${LIB})
      add_library(AWSSDK::${LIB} UNKNOWN IMPORTED)
      set_target_properties(AWSSDK::${LIB} PROPERTIES
        IMPORTED_LOCATION "${AWS_FOUND_${LIB}}"
        INTERFACE_INCLUDE_DIRECTORIES "${AWSSDK_INCLUDE_DIR}"
      )
    endif()

    list(APPEND TILEDB_AWS_LINK_TARGETS "AWSSDK::${LIB}")

    # If we built a static EP, install it if required.
    if (TILEDB_AWSSDK_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
      install_target_libs(AWSSDK::${LIB})
    endif()

  endforeach ()

  # the AWSSDK does not include links to some transitive dependencies
  # ref: github<dot>com/aws<slash>aws-sdk-cpp/issues/1074#issuecomment-466252911
  if (WIN32)
    list(APPEND AWS_EXTRA_LIBS userenv ws2_32 wininet winhttp bcrypt version crypt32 secur32 Ncrypt)
  endif()
endif ()
