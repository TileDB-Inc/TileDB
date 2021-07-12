#
# FindSpdlog_EP.cmake
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
# Finds the Spdlog library, installing with an ExternalProject as necessary.
# This module defines:
#   - SPDLOG_INCLUDE_DIR, directory containing headers
#   - SPDLOG_FOUND, whether Spdlog has been found
#   - The spdlog::spdlog imported target

# Include some common helper functions.
include(TileDBCommon)

# If the EP was built, it will install the storage_client-config.cmake file,
# which we can use with find_package. CMake uses CMAKE_PREFIX_PATH to locate find
# modules.
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${TILEDB_EP_INSTALL_PREFIX}")


# First try the CMake find module.
if (NOT TILEDB_FORCE_ALL_DEPS OR TILEDB_SPDLOG_EP_BUILT)
  if (TILEDB_SPDLOG_EP_BUILT)
    # If we built it from the source always force no default path
    SET(TILEDB_SPDLOG_NO_DEFAULT_PATH NO_DEFAULT_PATH)
  else()
    SET(TILEDB_SPDLOG_NO_DEFAULT_PATH ${TILEDB_DEPS_NO_DEFAULT_PATH})
  endif()

  find_package(spdlog
          PATHS ${TILEDB_EP_INSTALL_PREFIX}
          ${TILEDB_SPDLOG_NO_DEFAULT_PATH}
          )
  set(SPDLOG_FOUND ${spdlog_FOUND})
endif()

if (NOT SPDLOG_FOUND)
  if(TILEDB_SUPERBUILD)
    if (WIN32)
      find_package(Git REQUIRED)
      set(CONDITIONAL_PATCH cd ${CMAKE_SOURCE_DIR} && ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_spdlog < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_spdlog/spdlog.patch)
    else()
      set(CONDITIONAL_PATCH patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_spdlog/spdlog.patch)
    endif()

    message(STATUS "Adding spdlog as an external project")
    ExternalProject_Add(ep_spdlog
      PREFIX "externals"
      # Set download name to avoid collisions with only the version number in the filename
      DOWNLOAD_NAME ep_spdlog.zip
      URL "https://github.com/gabime/spdlog/archive/v1.8.5.zip"
      URL_HASH SHA1=178f7858e4f2a3c12f4b130eb67a54bb149d2cca
      PATCH_COMMAND
        ${CONDITIONAL_PATCH}
      CMAKE_ARGS
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DSPDLOG_BUILD_SHARED=OFF
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_spdlog)
    list(APPEND FORWARD_EP_CMAKE_ARGS
            -DTILEDB_SPDLOG_EP_BUILT=TRUE
            )
  else()
    message(FATAL_ERROR "Unable to find spdlog")
  endif()
endif()

if (spdlog_FOUND AND NOT TARGET spdlog::spdlog)
  add_library(spdlog::spdlog INTERFACE IMPORTED)
  find_package(fmt QUIET)
  if (${fmt_FOUND})
    target_link_libraries(spdlog::spdlog INTERFACE fmt::fmt)
  endif()
  set_target_properties(spdlog::spdlog PROPERTIES
          INTERFACE_INCLUDE_DIRECTORIES "${SPDLOG_INCLUDE_DIR}"
          )
  # If the target is defined we need to handle external fmt build types
elseif(TARGET spdlog::spdlog)
  if (SPDLOG_FMT_EXTERNAL)
    # Since we are using header only we need to define this
    add_definitions("-DSPDLOG_FMT_EXTERNAL=1")
    find_package(fmt REQUIRED)
    if (${fmt_FOUND})
      target_link_libraries(spdlog::spdlog INTERFACE fmt::fmt)
    endif()
  endif()
endif()

# If we built a static EP, install it if required.
if (TILEDB_SPDLOG_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(spdlog::spdlog)
endif()