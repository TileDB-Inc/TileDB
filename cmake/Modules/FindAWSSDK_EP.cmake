#
# FindAWSSDK_EP.cmake
#
#
# The MIT License
#
# Copyright (c) 2018 TileDB, Inc.
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

# If the EP was built, it will install the AWSSDKConfig.cmake file, which we
# can use with find_package. CMake uses CMAKE_PREFIX_PATH to locate find
# modules.
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${TILEDB_EP_INSTALL_PREFIX}")

# Try searching for the SDK in the EP prefix.
set(AWSSDK_ROOT_DIR "${TILEDB_EP_INSTALL_PREFIX}")

# AWS Linked Libs List
set(AWS_LINKED_LIBS)

# Check to see if the SDK is installed (which provides the find module).
# This will either use the system-installed AWSSDK find module (if present),
# or the superbuild-installed find module.
find_package(AWSSDK CONFIG QUIET)
if (NOT AWSSDK_FOUND OR TILEDB_FORCE_ALL_DEPS)
  message(STATUS "Could NOT find AWSSDK")
  message(STATUS "Adding AWSSDK as an external project")

  set(DEPENDS)
  if (TARGET ep_curl)
    list(APPEND DEPENDS ep_curl)
  endif()
  if (TARGET ep_openssl)
    list(APPEND DEPENDS ep_openssl)
  endif()

  ExternalProject_Add(ep_awssdk
    PREFIX "externals"
    URL "https://github.com/aws/aws-sdk-cpp/archive/1.4.23.zip"
    URL_HASH SHA1=32030b0fc44d956c1f0dc606044d555c155d40a6
    CMAKE_ARGS
      -DCMAKE_BUILD_TYPE=Release
      -DENABLE_TESTING=OFF
      -DBUILD_ONLY=s3\\;core
      -DBUILD_SHARED_LIBS=OFF
      -DCMAKE_INSTALL_BINDIR=lib
      -DENABLE_UNITY_BUILD=ON
      -DCUSTOM_MEMORY_MANAGEMENT=0
      -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
      -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
      -DCMAKE_CXX_FLAGS=-fPIC
      -DCMAKE_C_FLAGS=-fPIC
    UPDATE_COMMAND ""
    LOG_DOWNLOAD TRUE
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
    DEPENDS ${DEPENDS}
  )
  list(APPEND TILEDB_EXTERNAL_PROJECTS ep_awssdk)
  list(APPEND AWS_LINKED_LIBS aws-cpp-sdk-core aws-cpp-sdk-s3)

  set(AWSSDK_INCLUDE_DIR "${TILEDB_EP_INSTALL_PREFIX}/include/aws")

  # INTERFACE_INCLUDE_DIRECTORIES does not allow for empty directories in config phase,
  # thus we need to create the directory. See https://cmake.org/Bug/view.php?id=15052
  file(MAKE_DIRECTORY ${AWSSDK_INCLUDE_DIR})

  foreach (LIB ${AWS_LINKED_LIBS})
    if (NOT TARGET AWSSDK::${LIB})
      add_library(AWSSDK::${LIB} UNKNOWN IMPORTED)
      set_target_properties(AWSSDK::${LIB} PROPERTIES
        IMPORTED_LOCATION "${TILEDB_EP_INSTALL_PREFIX}/lib/lib${LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}"
        INTERFACE_INCLUDE_DIRECTORIES "${AWSSDK_INCLUDE_DIR}"
      )
    endif()
  endforeach ()
  set(AWSSDK_FOUND TRUE)
elseif (AWSSDK_FOUND)
  set(AWS_SERVICES s3)
  AWSSDK_DETERMINE_LIBS_TO_LINK(AWS_SERVICES AWS_LINKED_LIBS)
  foreach (LIB ${AWS_LINKED_LIBS})
    find_library("AWS_FOUND_${LIB}"
      NAMES ${LIB}
      PATHS ${AWSSDK_LIB_DIR}
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )
    message(STATUS "Found AWS lib: ${LIB}")
    if (NOT TARGET AWSSDK::${LIB})
      add_library(AWSSDK::${LIB} UNKNOWN IMPORTED)
      set_target_properties(AWSSDK::${LIB} PROPERTIES
        IMPORTED_LOCATION "${AWS_FOUND_${LIB}}"
        INTERFACE_INCLUDE_DIRECTORIES "${AWSSDK_INCLUDE_DIR}"
      )
    endif()
  endforeach ()
else()
  message(FATAL_ERROR "AWSSDK not found and not built as dependency!")
endif ()
