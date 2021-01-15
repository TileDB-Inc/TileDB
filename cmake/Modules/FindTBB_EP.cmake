#
# FindTBB_EP.cmake
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
  if (WIN32)
    set(TBB_SRC_DIR "${TILEDB_EP_BASE}/src/ep_tbb/tbb")
  else()
    set(TBB_SRC_DIR "${TILEDB_EP_BASE}/src/ep_tbb")
  endif()
  set(TBB_BUILD_DIR "${TILEDB_EP_BASE}/src/ep_tbb-build")
  set(TBB_BUILD_PREFIX "")
  set(TBB_BUILD_CMAKE "${TBB_SRC_DIR}/cmake/TBBBuild.cmake")
  set(TBB_MAKE_ARGS
    "tbb_build_dir=${TBB_BUILD_DIR}"
    "tbb_build_prefix=${TBB_BUILD_PREFIX}"
  )

  if (NOT TILEDB_TBB_SHARED)
    # Adding big_iron.inc specifies that TBB should be built as a static lib.
    list(APPEND TBB_MAKE_ARGS "extra_inc=big_iron.inc")
  endif()

  # Check if the superbuild downloaded TBB.
  if (EXISTS "${TBB_BUILD_CMAKE}")
    include(${TBB_BUILD_CMAKE})
    if (WIN32)
      # On Windows the superbuild downloads the binaries, so just set the
      # search path appropriately.
      set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${TBB_SRC_DIR}")
    else()
      tbb_build(TBB_ROOT ${TBB_SRC_DIR}
        CONFIG_DIR TBB_DIR
        MAKE_ARGS ${TBB_MAKE_ARGS}
      )
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

  if (WIN32 AND TILEDB_TBB_SHARED)
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
    # Just install the libraries to lib on non-Windows and on Windows if
    # TBB is built as a static library.
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

#
# Search and manually create a TBB::tbb target if possible.
#
function(backup_find_tbb)
  # Also search the TBB EP build tree for the include and library.
  set(TBB_EXTRA_SEARCH_PATHS
    ${TBB_SRC_DIR}
    ${TBB_BUILD_DIR}/${TBB_BUILD_PREFIX}_release
  )

  if (NOT WIN32)
    find_path(TBB_INCLUDE_DIR
      NAMES tbb/tbb.h
      PATHS ${TBB_EXTRA_SEARCH_PATHS}
      PATH_SUFFIXES include
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )

    find_library(TBB_LIBRARIES
      NAMES
        tbb
        ${CMAKE_STATIC_LIBRARY_PREFIX}tbb${CMAKE_STATIC_LIBRARY_SUFFIX}
      PATHS ${TBB_EXTRA_SEARCH_PATHS}
      PATH_SUFFIXES lib
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )

    include(FindPackageHandleStandardArgs)
    FIND_PACKAGE_HANDLE_STANDARD_ARGS(TBB
      REQUIRED_VARS TBB_LIBRARIES TBB_INCLUDE_DIR
    )

    if (TBB_FOUND AND NOT TARGET TBB::tbb)
      add_library(TBB::tbb UNKNOWN IMPORTED)
      set_target_properties(TBB::tbb PROPERTIES
        IMPORTED_LOCATION_RELEASE "${TBB_LIBRARIES}"
        IMPORTED_LOCATION_DEBUG "${TBB_LIBRARIES}"
        IMPORTED_LOCATION "${TBB_LIBRARIES}"
        INTERFACE_INCLUDE_DIRECTORIES "${TBB_INCLUDE_DIR}"
      )
      set(TBB_IMPORTED_TARGETS TBB::tbb PARENT_SCOPE)
    endif()
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
    find_package(TBB CONFIG)
  endif()
else()
  find_package(TBB CONFIG)

  if (NOT TARGET TBB::tbb)
    # Build/setup the EP.
    build_tbb_ep()

    # Try finding again.
    if (TILEDB_TBB_SHARED)
      find_package(TBB CONFIG)
    else()
      # The TBB find module won't work for static TBB, so use our manual method.
      backup_find_tbb()
    endif()

    # Install TBB libraries if:
    # - Windows (always); or
    # - TBB was built as a shared library; or
    # - Both TileDB and TBB were built as static libraries
    if (TARGET TBB::tbb)
      if (WIN32 OR TILEDB_TBB_SHARED OR (TILEDB_INSTALL_STATIC_DEPS AND NOT TILEDB_TBB_SHARED))
        # Add TBB libraries to the TileDB installation manifest.
        install_tbb()
      endif()
    endif()
  endif()

  # Save the library directory to a cache variable.
  save_tbb_dir()
endif()

# The TBB find_package() support is pretty spotty, and can fail e.g. with
# Homebrew installed versions. Try a backup method here.
if (NOT TARGET TBB::tbb)
  backup_find_tbb()
endif()

if (TARGET TBB::tbb)
  message(STATUS "Found TBB imported target: ${TBB_IMPORTED_TARGETS}")
  set(TBB_FOUND TRUE)
else()
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding TBB as an external project")

    if (WIN32)
      # On Windows we download pre-built binaries.
      ExternalProject_Add(ep_tbb
        PREFIX "externals"
        # Set download name to avoid collisions with only the version number in the filename
        DOWNLOAD_NAME ep_tbb.zip
        URL "https://github.com/intel/tbb/releases/download/v2020.1/tbb-2020.1-win.zip"
        URL_HASH SHA1=fdcdba2026f4c11f1cb3cbc36ff45d828219e580
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
        LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      )
    else()
      ExternalProject_Add(ep_tbb
        PREFIX "externals"
        # Set download name to avoid collisions with only the version number in the filename
        DOWNLOAD_NAME ep_tbb.tar.gz
        URL "https://github.com/intel/tbb/archive/v2020.1.tar.gz"
        URL_HASH SHA1=c42a33b5fc42aedeba75c204a5367593e28a6977
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
        LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      )
    endif()

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_tbb)
  else()
    message(FATAL_ERROR "Unable to find TBB")
  endif()
endif()
