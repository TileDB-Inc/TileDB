#
# FindClipp_EP.cmake
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
# Finds the Clipp library, installing with an ExternalProject as necessary.
#
# This module defines:
#   - CLIPP_INCLUDE_DIR, directory containing headers
#   - CLIPP_FOUND, whether Clipp has been found
#   - The Clipp::Clipp imported target

# Search the path set during the superbuild for the EP.
set(CLIPP_PATHS ${TILEDB_EP_INSTALL_PREFIX})

if (TILEDB_VCPKG)
  find_package(clipp REQUIRED)
  return()
endif()

if (NOT TILEDB_FORCE_ALL_DEPS OR TILEDB_CLIPP_EP_BUILT)
  find_path(CLIPP_INCLUDE_DIR
    NAMES clipp.h
    PATHS ${CLIPP_PATHS}
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Clipp
  REQUIRED_VARS CLIPP_INCLUDE_DIR
)

if (NOT CLIPP_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Clipp as an external project")
    ExternalProject_Add(ep_clipp
      PREFIX "externals"
      # Set download name to avoid collisions with only the version number in the filename
      DOWNLOAD_NAME ep_clipp.zip
      URL "https://github.com/muellan/clipp/archive/v1.2.3.zip"
      URL_HASH SHA1=5cb4255d29e1b47b5d5abf7481befe659ffc3ee1
      UPDATE_COMMAND ""
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      INSTALL_COMMAND
        ${CMAKE_COMMAND} -E make_directory ${TILEDB_EP_INSTALL_PREFIX}/include
        COMMAND
          ${CMAKE_COMMAND} -E copy_if_different
            ${TILEDB_EP_BASE}/src/ep_clipp/include/clipp.h
            ${TILEDB_EP_INSTALL_PREFIX}/include/
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_clipp)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_CLIPP_EP_BUILT=TRUE
    )
  else()
    message(FATAL_ERROR "Unable to find Clipp")
  endif()
endif()

# Create the imported target for Clipp
if (CLIPP_FOUND AND NOT TARGET clipp::clipp)
  add_library(clipp::clipp INTERFACE IMPORTED)
  set_target_properties(clipp::clipp PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${CLIPP_INCLUDE_DIR}"
  )
endif()
