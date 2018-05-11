#
# FindTBB_EP.cmake
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
# Finds the Intel TBB library, installing with an ExternalProject as necessary.
# This module defines:
#   - TBB_FOUND, whether TBB has been found
#   - TBB_LIB_DIR, the directory in the build tree with the TBB libraries
#   - The TBB::tbb imported target

############################################################
# Helper functions specific to TBB
############################################################

#
# Builds the TBB external project such that a subsequent find_package(TBB)
# will find it. This is a macro so the variables are set in the parent scope.
#
macro(build_tbb_ep)
  set(TBB_SRC_DIR "${TILEDB_EP_BASE}/src/ep_tbb")
  set(TBB_BUILD_CMAKE "${TBB_SRC_DIR}/cmake/TBBBuild.cmake")
  # Check if the superbuild downloaded TBB.
  if (EXISTS "${TBB_BUILD_CMAKE}")
    include(${TBB_BUILD_CMAKE})
    if (WIN32)
      # On Windows the superbuild downloads the binaries, so just set the
      # search path appropriately.
      set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${TBB_SRC_DIR}")
    else()
      tbb_build(TBB_ROOT ${TBB_SRC_DIR} CONFIG_DIR TBB_DIR)
    endif()
  endif()
endmacro()

#
# Adds TBB libraries to the TileDB installation manifest.
#
function(install_tbb)
  get_target_property(TBB_LIBRARIES_RELEASE TBB::tbb IMPORTED_LOCATION_RELEASE)
  get_target_property(TBB_LIBRARIES_DEBUG TBB::tbb IMPORTED_LOCATION_DEBUG)

  # Get library directory for multiarch linux distros
  include(GNUInstallDirs)

  if (WIN32)
    # Install the DLLs to bin.
    if (CMAKE_BUILD_TYPE MATCHES "Release")
      install(FILES ${TBB_LIBRARIES_RELEASE} DESTINATION ${CMAKE_INSTALL_BINDIR})
    else()
      install(FILES ${TBB_LIBRARIES_DEBUG} DESTINATION ${CMAKE_INSTALL_BINDIR})
    endif()

    # Install the import libraries (.lib) to lib.
    get_target_property(TBB_LIBRARIES_IMPLIB_RELEASE TBB::tbb IMPORTED_IMPLIB_RELEASE)
    get_target_property(TBB_LIBRARIES_IMPLIB_DEBUG TBB::tbb IMPORTED_IMPLIB_DEBUG)
    if (CMAKE_BUILD_TYPE MATCHES "Release")
      install(FILES ${TBB_LIBRARIES_IMPLIB_RELEASE} DESTINATION ${CMAKE_INSTALL_LIBDIR})
    else()
      install(FILES ${TBB_LIBRARIES_IMPLIB_DEBUG} DESTINATION ${CMAKE_INSTALL_LIBDIR})
    endif()
  else()
    # Just install the libraries to lib.
    if (CMAKE_BUILD_TYPE MATCHES "Release")
      install(FILES ${TBB_LIBRARIES_RELEASE} DESTINATION ${CMAKE_INSTALL_LIBDIR})
    else()
      install(FILES ${TBB_LIBRARIES_DEBUG} DESTINATION ${CMAKE_INSTALL_LIBDIR})
    endif()
  endif()
endfunction()

#
# Sets a cache variable TBB_LIB_DIR pointing to the TBB libraries in the build tree
# (for use by e.g. the tests).
#
function(save_tbb_dir)
  if (TARGET TBB::tbb)
    set(TBB_LIB_IMPLOC)
    if (CMAKE_BUILD_TYPE MATCHES "Release")
      get_target_property(TBB_LIB_IMPLOC TBB::tbb IMPORTED_LOCATION_RELEASE)
    else()
      get_target_property(TBB_LIB_IMPLOC TBB::tbb IMPORTED_LOCATION_DEBUG)
    endif()
    unset(TBB_LIB_DIR CACHE)
    get_filename_component(TBB_LIB_DIR "${TBB_LIB_IMPLOC}" DIRECTORY CACHE)
  endif()
endfunction()

############################################################
# Regular superbuild EP setup.
############################################################

# Check to see if the SDK is installed (which provides the find module).
# This will either use the system-installed AWSSDK find module (if present),
# or the superbuild-installed find module.
if (TILEDB_SUPERBUILD)
  # Don't use find_package in superbuild if we are forcing all deps.
  if (NOT TILEDB_FORCE_ALL_DEPS)
    find_package(TBB CONFIG QUIET)
  endif()
else()
  # Try finding a system TBB first.
  find_package(TBB CONFIG QUIET)

  if (TBB_FOUND)
    message(STATUS "Found system TBB: ${TBB_IMPORTED_TARGETS}")
  else()
    # Build/setup the EP.
    build_tbb_ep()

    # Try finding again.
    find_package(TBB CONFIG QUIET)

    # If we found TBB that was built by the EP, we now add it to the TileDB
    # installation manifest.
    if (TBB_FOUND)
      message(STATUS "Found EP TBB: ${TBB_IMPORTED_TARGETS}")
      # Add TBB libraries to the TileDB installation manifest.
      install_tbb()
    endif()
  endif()

  # Save the library directory to a cache variable.
  save_tbb_dir()
endif()

if (NOT TBB_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Could NOT find TBB")
    message(STATUS "Adding TBB as an external project")

    if (WIN32)
      ExternalProject_Add(ep_tbb
        PREFIX "externals"
        URL "https://github.com/01org/tbb/releases/download/2018_U3/tbb2018_20180312oss_win.zip"
        URL_HASH SHA1=7f0b4b227679637f7a4065b0377d55d12fac983b
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
      )
    else()
      ExternalProject_Add(ep_tbb
        PREFIX "externals"
        URL "https://github.com/01org/tbb/archive/2018_U3.zip"
        URL_HASH SHA1=c17ae26f2be1dd7ca9586f795d07a226ceca2dc2
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
      )
    endif()

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_tbb)
  else()
    message(FATAL_ERROR "Unable to find TBB")
  endif()
endif()
