#
# Findabsl_EP.cmake
#
# The MIT License
#
# Copyright (c) 2022 TileDB, Inc.
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
# Finds the absl (abseil) library, installing with an ExternalProject as necessary.
# This module defines:
#   - absl_INCLUDE_DIR, directory containing headers
#   - absl_LIBRARIES, the Magic library path
#   - absl_FOUND, whether Magic has been found
#   - The absl imported target

# Include some common helper functions.
include(TileDBCommon)

if(TILEDB_ABSL_EP_BUILT)
  find_package(absl CONFIG REQUIRED
    HINTS
      ${TILEDB_EP_INSTALL_PREFIX}/lib/cmake
    ${TILEDB_DEPS_NO_DEFAULT_PATH})
elseif (NOT TILEDB_FORCE_ALL_DEPS)
  # seems no standard findabsl.cmake, so silence warnings with QUIET
  find_package(absl QUIET ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()

# if not yet built add it as an external project
if(NOT TILEDB_ABSL_EP_BUILT AND NOT absl_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding absl as an external project")

    set(TILEDB_ABSL_BUILD_VERSION 20211102.0)
    set(TILEDB_ABSEIL_CPP_URL
        "https://github.com/abseil/abseil-cpp/archive/${TILEDB_ABSL_BUILD_VERSION}.tar.gz")
    set(TILEDB_ABSEIL_CPP_SHA256
        "dcf71b9cba8dc0ca9940c4b316a0c796be8fab42b070bb6b7cab62b48f0e66c4")

    ExternalProject_Add(
        ep_absl
        EXCLUDE_FROM_ALL ON
        PREFIX "externals"
        INSTALL_DIR "${TILEDB_EP_INSTALL_PREFIX}"
        URL ${TILEDB_ABSEIL_CPP_URL}
        URL_HASH SHA256=${TILEDB_ABSEIL_CPP_SHA256}
        #LIST_SEPARATOR |
        CMAKE_ARGS
             -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
             -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
             -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS}
             -DBUILD_TESTING=OFF
             -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
             -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
             -DCMAKE_CXX_FLAGS=-fPIC
             -DCMAKE_C_FLAGS=-fPIC
             -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        LOG_CONFIGURE OFF
        LOG_BUILD ON
        LOG_INSTALL ON
        LOG_OUTPUT_ON_FAILURE ON
      )
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_absl)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_ABSL_EP_BUILT=TRUE
    )

  else()
    message(FATAL_ERROR "Unable to find absl")
  endif()
endif()

if(absl_FOUND)
  # not currently trying to create all (55 at this time) targets, just attempt
  # sanity check of their existence and warn/error if not found.
  include(${CMAKE_SOURCE_DIR}/test/external/src/absl_library_targets.cmake)
  foreach(tgt ${TILEDB_ABSL_LIBRARY_TARGETS})
    if(NOT TARGET ${tgt})
      if(TILEDB_ABSL_EP_BUILT)
        message(FATAL_ERROR "built absl missing target ${tgt} from absl_library_targets.cmake (did absl changes occur?)")
      else()
        message(WARNING "absl missing expected target ${tgt} from absl_library_targets.cmake (different absl version?)")
      endif()
    endif()
  endforeach()
endif()
