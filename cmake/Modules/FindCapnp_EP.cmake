#
# FindCapnp_EP.cmake
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
# Finds the Capnp library, installing with an ExternalProject as necessary.
# This module defines:
#   - CAPNP_FOUND, whether Capnp has been found
#   - The CapnProto::{capnp,kj,capnp-json} imported targets

# Include some common helper functions.
include(TileDBCommon)

if (TILEDB_VCPKG AND TILEDB_SERIALIZATION)
  find_package(CapnProto REQUIRED)
  install_all_target_libs("CapnProto::capnp;CapnProto::capnp-json;CapnProto::kj")
  return()
endif()


# If the EP was built, it will install the CapnProtoConfig.cmake file, which we
# can use with find_package.

set(TILEDB_CAPNPROTO_VERSION 0.8.0)
set(TILEDB_CAPNPROTO_GITTAG "v0.8.0")
set(TILEDB_CAPNPROTO_HASH_SPEC "SHA1=6910b8872602c46c8b0e9692dc2889c1808a5950")
set(TILEDB_CAPNPROTO_URL "https://github.com/capnproto/capnproto/archive/v0.8.0.tar.gz")

# First try the CMake find module.
if (NOT TILEDB_FORCE_ALL_DEPS OR TILEDB_CAPNP_EP_BUILT)
  if (TILEDB_CAPNP_EP_BUILT)
    # If we built it from the source always force no default path
    SET(TILEDB_CAPNP_NO_DEFAULT_PATH NO_DEFAULT_PATH)
  else()
    SET(TILEDB_CAPNP_NO_DEFAULT_PATH ${TILEDB_DEPS_NO_DEFAULT_PATH})
  endif()

  find_package(CapnProto
    ${TILEDB_CAPNPROTO_VERSION} EXACT
    PATHS ${TILEDB_EP_INSTALL_PREFIX}
    ${TILEDB_CAPNP_NO_DEFAULT_PATH}
    )
  set(CAPNP_FOUND ${CapnProto_FOUND})
endif()

# If not found, add it as an external project
if (NOT CAPNP_FOUND)
  message(STATUS "Cap'n Proto was not found")
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Capnp as an external project")

    if (WIN32)
      find_package(Git REQUIRED)
      set(CONDITIONAL_PATCH
           cd ${CMAKE_SOURCE_DIR} &&
           ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_capnp < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_capnp/capnp_CMakeLists.txt.patch &&
           ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_capnp < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_capnp/windows-sanity.h.patch)
    else()
      set(CONDITIONAL_PATCH "")
    endif()

    ExternalProject_Add(ep_capnp
       PREFIX "externals"
       DOWNLOAD_NAME ep_capnp.${TILEDB_CAPNPROTO_VERSION}.tar.gz
       URL ${TILEDB_CAPNPROTO_URL}
       URL_HASH ${TILEDB_CAPNPROTO_HASH_SPEC}
      CMAKE_ARGS
         -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
         -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
         -DCMAKE_BUILD_TYPE=Release
         -DBUILD_TESTING=OFF
         -DCMAKE_POSITION_INDEPENDENT_CODE=ON
         -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
       PATCH_COMMAND
         ${CONDITIONAL_PATCH}
       LOG_DOWNLOAD TRUE
       LOG_CONFIGURE TRUE
       LOG_BUILD TRUE
       LOG_INSTALL TRUE
       LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_capnp)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_CAPNP_EP_BUILT=TRUE
    )
  else()
    message(FATAL_ERROR "Unable to find Capnp")
  endif()
else()
  add_definitions(${CAPNP_DEFINITIONS})
endif()


if(WIN32)
  if(TILEDB_CAPNP_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
    install_target_libs(CapnProto::capnp)
    install_target_libs(CapnProto::kj)
    install_target_libs(CapnProto::capnp-json)
  endif()
elseif(CAPNP_FOUND AND NOT WIN32)
  # We handle the found case first because ubuntu's install of capnproto is missing libcapnp-json
  # so we must first make sure the found case has all the appropriate libs, else we will build from source
  # List of all required Capnp libraries.
  set(CAPNP_LIB_NAMES capnp kj capnp-json)
  foreach(LIB ${CAPNP_LIB_NAMES})
    if (NOT TARGET CapnProto::${LIB} AND NOT CAPNP_LIBRARIES)
      message(FATAL_ERROR "Required target CapnProto::${LIB} not defined")
    elseif (TARGET CapnProto::${LIB})
      message(STATUS "Found CapnProto lib: ${LIB}")
      # If we built a static EP, install it if required.
      if (TILEDB_CAPNP_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
        message(STATUS "Adding CapnProto lib: ${LIB} to install target")
        install_target_libs(CapnProto::${LIB})
      endif()
    elseif (NOT TARGET CapnProto::${LIB})
      message(STATUS "NOT Found Target for CapnProto lib: ${LIB}")
      set(SHOULD_CAPNP_LIBRARIES 1)
    endif()
  endforeach()
  if (SHOULD_CAPNP_LIBRARIES)
    message(STATUS "Checking libraries in ${CAPNP_LIBRARIES}")
    foreach(NEEDED_LIB ${CAPNP_LIB_NAMES})
      foreach(LIB ${CAPNP_LIBRARIES})
        if (LIB MATCHES "lib${NEEDED_LIB}.")
          set(FOUND_${NEEDED_LIB} 1)
          if (NOT TARGET CapnProto::${NEEDED_LIB})
            message(STATUS "Adding target for CapnProto::${NEEDED_LIB} from CAPNP_LIBRARIES")
            add_library(CapnProto::${NEEDED_LIB} UNKNOWN IMPORTED)
            set_target_properties(CapnProto::${NEEDED_LIB} PROPERTIES
                  IMPORTED_LOCATION "${LIB}"
                  INTERFACE_INCLUDE_DIRECTORIES "${CAPNP_INCLUDE_DIR}"
                  )
          endif()
        endif()
      endforeach()
      if (NOT FOUND_${NEEDED_LIB})
        message("Cap'n Proto ${NEEDED_LIB} not found on system, reverting to external project")
        unset(CAPNP_FOUND)
      endif()
    endforeach()
  endif()
endif()





