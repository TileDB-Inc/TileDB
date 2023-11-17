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

# Include some common helper functions.
include(TileDBCommon)

if (TILEDB_VCPKG)
  find_package(WebP REQUIRED)
  return()
endif()

if(TILEDB_WEBP_EP_BUILT)
  find_package(WebP REQUIRED PATHS ${TILEDB_EP_INSTALL_PREFIX} NO_DEFAULT_PATH)
endif()

# if not yet built add it as an external project
if(NOT TILEDB_WEBP_EP_BUILT)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Webp as an external project")

    # that was modified by tiledb to also build with cmake for nix
    ExternalProject_Add(ep_webp
      PREFIX "externals"
      GIT_REPOSITORY "https://chromium.googlesource.com/webm/libwebp"
      GIT_TAG "v1.3.2"
      GIT_SUBMODULES_RECURSE TRUE
      UPDATE_COMMAND ""
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_BUILD_TYPE=$<CONFIG>
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
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DCMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}
        -DCMAKE_OSX_SYSROOT=${CMAKE_OSX_SYSROOT}
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
