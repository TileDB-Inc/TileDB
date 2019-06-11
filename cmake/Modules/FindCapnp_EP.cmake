#
# FindCapnp_EP.cmake
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
# Finds the Capnp library, installing with an ExternalProject as necessary.
# This module defines:
#   - CAPNP_FOUND, whether Capnp has been found
#   - The CapnProto::{capnp,kj,capnp-json} imported targets

# Include some common helper functions.
include(TileDBCommon)

# If the EP was built, it will install the CapnProtoConfig.cmake file, which we
# can use with find_package. CMake uses CMAKE_PREFIX_PATH to locate find
# modules.
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${TILEDB_EP_INSTALL_PREFIX}")

# First try the CMake find module.
find_package(CapnProto QUIET ${TILEDB_DEPS_NO_DEFAULT_PATH})
set(CAPNP_FOUND ${CapnProto_FOUND})

# needed for cherry-pick
find_package(Git REQUIRED)

# If not found, add it as an external project
if (NOT CAPNP_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Capnp as an external project")

    if (WIN32)
      set(CFLAGS_DEF "${CMAKE_C_FLAGS}")
      set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS}")
    else()
      set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
      set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS} -fPIC")
    endif()

    # we cherry-pick fdbf035619 to fix installation on windowsa
    #   https://github.com/capnproto/capnproto/commit/fdbf035619ab2f9e25173bb7361e7e19a52e0fa1
    ExternalProject_Add(ep_capnp
      PREFIX "externals"
      #URL "https://github.com/capnproto/capnproto/archive/v0.6.1.tar.gz"
      #URL_HASH SHA1=2aec1f83cc4851ae58e1419c87f11f8aa63a9392
      GIT_REPOSITORY "https://github.com/capnproto/capnproto.git"
      GIT_TAG "v0.6.1"
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND}
          ${ARCH_SPEC}
          -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
          -DCMAKE_BUILD_TYPE=Release
          -DBUILD_TESTING=OFF
          "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
          "-DCMAKE_CXX_FLAGS=${CXXFLAGS_DEF}"
          ${TILEDB_EP_BASE}/src/ep_capnp/c++
      PATCH_COMMAND 
        ${GIT_EXECUTABLE} cherry-pick fdbf035619
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_capnp)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_CAPNP_EP_BUILT=TRUE
    )
  else()
    message(FATAL_ERROR "Unable to find Capnp")
  endif()
endif()

if (CAPNP_FOUND)
  # List of all required Capnp libraries.
  set(CAPNP_LIB_NAMES capnp kj capnp-json)
  foreach(LIB ${CAPNP_LIB_NAMES})
    if (NOT TARGET CapnProto::${LIB})
      message(FATAL_ERROR "Required target CapnProto::${LIB} not defined")
    endif()
    message(STATUS "Found CapnProto lib: ${LIB}")
    # If we built a static EP, install it if required.
    if (TILEDB_CAPNP_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
      install_target_libs(CapnProto::${LIB})
    endif()
  endforeach()
endif()
