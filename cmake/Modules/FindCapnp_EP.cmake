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
#   - CAPNP_INCLUDE_DIR, directory containing headers
#   - CAPNP_LIBRARIES, the Capnp library path
#   - CAPNP_FOUND, whether Capnp has been found
#   - The Capnp::Capnp imported target

# Include some common helper functions.
include(TileDBCommon)

# Search the path set during the superbuild for the EP.
set(CAPNP_PATHS ${TILEDB_EP_INSTALL_PREFIX})

find_path(CAPNP_INCLUDE_DIR
  NAMES capnp/serialize.h
  PATHS ${CAPNP_PATHS}
  PATH_SUFFIXES include
  ${TILEDB_DEPS_NO_DEFAULT_PATH}
)

set(CAPNP_LIBS capnp kj capnp-json)
set(REQUIRED_VARS)
foreach(LIB ${CAPNP_LIBS})
# Link statically if installed with the EP.
if (TILEDB_USE_STATIC_CAPNP)
  find_library(CAPNP_LIB_${LIB} lib${LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}
          PATHS ${CAPNP_PATHS}
          PATH_SUFFIXES lib lib64
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
          )
  LIST(APPEND REQUIRED_VARS "CAPNP_LIB_${LIB}")
else()
  find_library(CAPNP_LIB_${LIB} lib${LIB}
          PATHS ${CAPNP_PATHS}
          PATH_SUFFIXES lib lib64 bin
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
          )
  LIST(APPEND REQUIRED_VARS "CAPNP_LIB_${LIB}")
endif()
endforeach()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Capnp
  REQUIRED_VARS ${REQUIRED_VARS} CAPNP_INCLUDE_DIR
)

if (NOT CAPNP_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding CAPNP as an external project")
    ExternalProject_Add(ep_capnp
      PREFIX "externals"
      URL "https://github.com/capnproto/capnproto/archive/v0.6.1.tar.gz"
      URL_HASH SHA1=2aec1f83cc4851ae58e1419c87f11f8aa63a9392
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND}
        ${ARCH_SPEC}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_CXX_FLAGS=-fPIC
        -DCMAKE_C_FLAGS=-fPIC
        ${TILEDB_EP_BASE}/src/ep_capnp/c++
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
    )

    if (WIN32)
      # capnp.{dll,lib} gets installed in lib dir; move it to bin.
      ExternalProject_Add_Step(ep_capnp move_dll
        DEPENDEES INSTALL
        COMMAND
          ${CMAKE_COMMAND} -E rename
            ${TILEDB_EP_INSTALL_PREFIX}/lib/capnp.dll
            ${TILEDB_EP_INSTALL_PREFIX}/bin/capnp.dll &&
          ${CMAKE_COMMAND} -E rename
            ${TILEDB_EP_INSTALL_PREFIX}/lib/capnp.lib
            ${TILEDB_EP_INSTALL_PREFIX}/bin/capnp.lib
          ${CMAKE_COMMAND} -E rename
            ${TILEDB_EP_INSTALL_PREFIX}/lib/kj.dll
            ${TILEDB_EP_INSTALL_PREFIX}/bin/kj.dll &&
          ${CMAKE_COMMAND} -E rename
            ${TILEDB_EP_INSTALL_PREFIX}/lib/kj.lib
            ${TILEDB_EP_INSTALL_PREFIX}/bin/kj.lib
      )
    endif()

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_capnp)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_USE_STATIC_CAPNP=TRUE
    )
  else()
    message(FATAL_ERROR "Unable to find Capnp")
  endif()
endif()

if(CAPNP_FOUND)
  foreach(LIB ${CAPNP_LIBS})
    add_library(Capnp::${LIB} UNKNOWN IMPORTED)
    set_target_properties(Capnp::${LIB} PROPERTIES
            IMPORTED_LOCATION "${CAPNP_LIB_${LIB}}"
            INTERFACE_INCLUDE_DIRECTORIES "${CAPNP_INCLUDE_DIR}"
            )
    # If we built a static EP, install it if required.
    if (TILEDB_USE_STATIC_CAPNP AND TILEDB_INSTALL_STATIC_DEPS)
      install_target_libs(Capnp::${LIB})
    endif()
  endforeach()
endif()
