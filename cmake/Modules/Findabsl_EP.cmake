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

# Search the path set during the superbuild for the EP.
set(ABSL_PATHS ${TILEDB_EP_INSTALL_PREFIX})

if(TILEDB_ABSL_EP_BUILT)
  find_package(absl PATHS ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()

# if not yet built add it as an external project
if(NOT TILEDB_ABSL_EP_BUILT)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding absl as an external project")

#    if (WIN32)
#      set(CFLAGS_DEF "")
#    else()
#      set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
#      set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS} -fPIC")
#    endif()

    set(TILEDB_ABSEIL_CPP_URL
        "https://github.com/abseil/abseil-cpp/archive/20200225.2.tar.gz")
    set(TILEDB_ABSEIL_CPP_SHA256
        "f41868f7a938605c92936230081175d1eae87f6ea2c248f41077c8f88316f111")

#    set_external_project_build_parallel_level(PARALLEL)
#    set_external_project_vars()

#    include(ExternalProject)
    ExternalProject_Add(
        ep_absl
        EXCLUDE_FROM_ALL ON
        PREFIX "externals"
        INSTALL_DIR "${TILEDB_EP_INSTALL_PREFIX}"
        URL ${TILEDB_ABSEIL_CPP_URL}
        URL_HASH SHA256=${TILEDB_ABSEIL_CPP_SHA256}
        #LIST_SEPARATOR |
        CMAKE_ARGS #${GOOGLE_CLOUD_CPP_EXTERNAL_PROJECT_CCACHE}
                   #-DCMAKE_INSTALL_PREFIX="${TILEDB_EP_INSTALL_PREFIX}"
                   -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                   -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
                   -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
                   -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
                   -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS}
                   -DBUILD_TESTING=OFF
                   -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
                   #-DCMAKE_INSTALL_RPATH=${GOOGLE_CLOUD_CPP_INSTALL_RPATH}
                   -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
                   -DCMAKE_CXX_FLAGS=-fPIC
                   -DCMAKE_C_FLAGS=-fPIC
                   -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
#        BUILD_COMMAND ${CMAKE_COMMAND} --build . --config ${CMAKE_BUILD_TYPE}
        BUILD_COMMAND ${CMAKE_COMMAND} --build . --config ${CMAKE_BUILD_TYPE}
        PATCH_COMMAND ${CMAKE_COMMAND} -P
                      "${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_absl/abseil-patch.cmake"
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

if (absl_FOUND AND NOT TARGET absl)
  message(STATUS "Found absl, adding imported target: ${absl_LIBRARIES}")
  add_library(absl UNKNOWN IMPORTED)
  set_target_properties(absl PROPERTIES
    IMPORTED_LOCATION "${absl_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${absl_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (TILEDB_ABSL_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(absl)
endif()
