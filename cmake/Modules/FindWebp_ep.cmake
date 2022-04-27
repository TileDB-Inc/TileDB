# FindWebp_EP.cmake
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
# Fetches and builds libwebp for use by tiledb.
# TBD...
# This module defines:
#   - libmagic_INCLUDE_DIR, directory containing headers
#   - libmagic_LIBRARIES, the Magic library path
#   - libmagic_FOUND, whether Magic has been found
#   - The libmagic imported target

# Include some common helper functions.
include(TileDBCommon)

# Search the path set during the superbuild for the EP.
set(WEBP_PATHS ${TILEDB_EP_INSTALL_PREFIX})

if (TILEDB_WEBP_EP_BUILT)
  find_package(webp PATHS ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()

if (TILEDB_WEBP_EP_BUILT)
  find_path(webp_INCLUDE_DIR
    NAMES encode.h decode.h demux.h mux.h mux_types.h
    PATHS ${WEBP_PATHS}
    PATH_SUFFIXES include/webp
    ${NO_DEFAULT_PATH}
  )

  # Link statically if installed with the EP.
  # TBD: Does this find/include the multiple libraries comprising webp?
  find_library(webp_LIBRARIES
    webp
    PATHS ${WEBP_PATHS}
    PATH_SUFFIXES lib a
    #${TILEDB_DEPS_NO_DEFAULT_PATH}
    ${NO_DEFAULT_PATH}
  )
  
  message(STATUS "B4 webp_INCLUDE_DIR is ${wepb_INCLUDE_DIR}")
  message(STATUS "B4 webp_LIBRARIES is ${wepb_LIBRARIES}")

  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(webp
    REQUIRED_VARS webp_LIBRARIES webp_INCLUDE_DIR
  )

  message(STATUS "AF webp_INCLUDE_DIR is ${wepb_INCLUDE_DIR}")
  message(STATUS "AF webp_LIBRARIES is ${wepb_LIBRARIES}")

endif()

# if not yet built add it as an external project
if(NOT TILEDB_WEBP_EP_BUILT)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Webp as an external project")

    #if (WIN32)
    #  set(CFLAGS_DEF "")
    #else()
    #  set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
    #  set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS} -fPIC")
    #endif()

    # For both windows/nix using tiledb fork of file-windows from julian-r who has build a cmake version and patched for msvc++
    # that was modified by tiledb to also build with cmake for nix
    ExternalProject_Add(ep_webp
      PREFIX "externals"
      GIT_REPOSITORY "https://chromium.googlesource.com/webm/libwebp"
      #GIT_TAG "release-1.?.?" # after 'static' addition in some release
      GIT_TAG "a19a25bb03757d5bb14f8d9755ab39f06d0ae5ef" # from branch 'main' history
      GIT_SUBMODULES_RECURSE TRUE
      UPDATE_COMMAND ""
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
        -DWEBP_LINK_STATIC=ON
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_webp)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_WEBP_EP_BUILT=TRUE
    )

    set(TILEDB_WEBP_DIR "${TILEDB_EP_INSTALL_PREFIX}")

  else()
    message(FATAL_ERROR "Unable to find Webp")
  endif()
endif()

if (webp_FOUND AND NOT TARGET webp)
  message(STATUS "Found webp, adding imported target: ${webp_LIBRARIES}")
  add_library(webp UNKNOWN IMPORTED)
  set_target_properties(webp PROPERTIES
    IMPORTED_LOCATION "${webp_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${webp_INCLUDE_DIR}"
  )
endif()

# If we built a static EP, install it if required.
if (TILEDB_WEBP_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(webp imagedec imageenc imageioutil webpdecoder webpdmux webpmux)
endif()
