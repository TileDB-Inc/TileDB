# FindWebp_EP.cmake
#
# The MIT License
#
# Copyright (c) 2023 TileDB, Inc.
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

# Include some common helper functions.
include(FindPackageHandleStandardArgs)
include(TileDBCommon)

if (NOT TILEDB_FORCE_ALL_DEPS OR TILEDB_WEBP_EP_BUILT)
  find_package(WebP QUIET PATHS ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()

# If WebP is not built and installed via CMake (i.e., when installed
# via Homebrew on macOS) there is no WebPConfig.cmake file installed
# which prevents CMake's `find_package` from discovering WebP.
# These `find_{path|library}` checks will locate a system libwebp
# if one exists.
if (NOT WebP_FOUND AND NOT TILEDB_FORCE_ALL_DEPS)
  find_library(WEBP_LIBRARIES
    NAMES
      ${CMAKE_STATIC_LIBRARY_PREFIX}webp${CMAKE_STATIC_LIBRARY_SUFFIX}
  )
  find_library(WEBPDECODER_LIBRARIES
    NAMES
      ${CMAKE_STATIC_LIBRARY_PREFIX}webpdecoder${CMAKE_STATIC_LIBRARY_SUFFIX}
  )
  find_library(WEBPDEMUX_LIBRARIES
    NAMES
      ${CMAKE_STATIC_LIBRARY_PREFIX}webpdemux${CMAKE_STATIC_LIBRARY_SUFFIX}
  )
  find_library(WEBPMUX_LIBRARIES
    NAMES
      ${CMAKE_STATIC_LIBRARY_PREFIX}webpmux${CMAKE_STATIC_LIBRARY_SUFFIX}
  )
  find_path(WEBP_INCLUDE_DIR
    NAMES webp/decode.h
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(WebP
    REQUIRED_VARS
      WEBP_LIBRARIES
      WEBPDECODER_LIBRARIES
      WEBPDEMUX_LIBRARIES
      WEBPMUX_LIBRARIES
      WEBP_INCLUDE_DIR
  )
endif()

# if not yet built add it as an external project
if(NOT WebP_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Webp as an external project")

    # that was modified by tiledb to also build with cmake for nix
    ExternalProject_Add(ep_webp
      PREFIX "externals"
      GIT_REPOSITORY "https://chromium.googlesource.com/webm/libwebp"
      #GIT_TAG "release-1.?.?" # after 'static' addition in some release
      # from branch 'main' history as the 'static' support added apr 12 2022
      # at implementation time is not yet in release branch/tag.
      GIT_TAG "a19a25bb03757d5bb14f8d9755ab39f06d0ae5ef"
      GIT_SUBMODULES_RECURSE TRUE
      UPDATE_COMMAND ""
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
        -DWEBP_LINK_STATIC=ON
        -DWEBP_BUILD_ANIM_UTILS=OFF
        -DWEBP_BUILD_CWEBP=OFF
        -DWEBP_BUILD_DWEBP=OFF
        -DWEBP_BUILD_GIF2WEBP=OFF
        -DWEBP_BUILD_IMG2WEBP=OFF
        -DWEBP_BUILD_VWEBP=OFF
        -DWEBP_BUILD_WEBPINFO=OFF
        -DWEBP_BUILD_WEBPMUX=OFF
        -DWEBP_BUILD_EXTRAS=OFF
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

if (WebP_FOUND AND NOT TARGET WebP::webp)
  add_library(WebP::webp UNKNOWN IMPORTED)
  set_target_properties(WebP::webp PROPERTIES
    IMPORTED_LOCATION "${WEBP_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${WEBP_INCLUDE_DIR}"
  )
endif()

if (WebP_FOUND AND NOT TARGET WebP::webpdecoder)
  add_library(WebP::webpdecoder UNKNOWN IMPORTED)
  set_target_properties(WebP::webpdecoder PROPERTIES
    IMPORTED_LOCATION "${WEBPDECODER_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${WEBP_INCLUDE_DIR}"
  )
endif()

if (WebP_FOUND AND NOT TARGET WebP::webpdemux)
  add_library(WebP::webpdemux UNKNOWN IMPORTED)
  set_target_properties(WebP::webpdemux PROPERTIES
    IMPORTED_LOCATION "${WEBPDEMUX_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${WEBP_INCLUDE_DIR}"
  )
endif()

if (WebP_FOUND AND NOT TARGET WebP::webpmux)
  add_library(WebP::webpmux UNKNOWN IMPORTED)
  set_target_properties(WebP::webpmux PROPERTIES
    IMPORTED_LOCATION "${WEBPMUX_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${WEBP_INCLUDE_DIR}"
  )
endif()

# Always building static EP, install it..
if (TILEDB_WEBP_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  # One install_target_libs() with all of these was only installing the first item.
  install_target_libs(WebP::webp)
  install_target_libs(WebP::webpdecoder)
  install_target_libs(WebP::webpdemux)
  install_target_libs(WebP::webpmux)
endif()
