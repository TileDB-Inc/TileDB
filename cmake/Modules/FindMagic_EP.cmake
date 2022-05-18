#
# FindMagic_EP.cmake
#
# The MIT License
#
# Copyright (c) 2018-2022 TileDB, Inc.
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
# Finds the Magic library, installing with an ExternalProject as necessary.
# This module defines:
#   - libmagic_INCLUDE_DIR, directory containing headers
#   - libmagic_LIBRARIES, the Magic library path
#   - libmagic_FOUND, whether Magic has been found
#   - libmagic_DICTIONARY, whether magic.mgc has been found
#   - The libmagic imported target

# Include some common helper functions.
include(TileDBCommon)

# Search the path set during the superbuild for the EP.
set(LIBMAGIC_PATHS ${TILEDB_EP_INSTALL_PREFIX})

# Try the builtin find module unless built w/ EP superbuild
if ((NOT TILEDB_FORCE_ALL_DEPS) AND (NOT TILEDB_LIBMAGIC_EP_BUILT))
  find_package(Magic ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()

if (NOT MAGIC_FOUND AND TILEDB_LIBMAGIC_EP_BUILT)
  find_package(libmagic PATHS ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()

if (NOT MAGIC_FOUND AND TILEDB_LIBMAGIC_EP_BUILT)
  find_path(libmagic_INCLUDE_DIR
    NAMES magic.h
    PATHS ${LIBMAGIC_PATHS}
    PATH_SUFFIXES include
    ${NO_DEFAULT_PATH}
  )

  if (NOT libmagic_INCLUDE_DIR)
    find_path(libmagic_INCLUDE_DIR
      NAMES file/file.h
      PATHS ${LIBMAGIC_PATHS}
      PATH_SUFFIXES include
      ${NO_DEFAULT_PATH}
    )
  endif()

  # Link statically if installed with the EP.
  find_library(libmagic_LIBRARIES
    libmagic
    PATHS ${LIBMAGIC_PATHS}
    PATH_SUFFIXES lib a
    #${TILEDB_DEPS_NO_DEFAULT_PATH}
    ${NO_DEFAULT_PATH}
  )

  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(libmagic
    REQUIRED_VARS libmagic_LIBRARIES libmagic_INCLUDE_DIR
  )
endif()

# if not yet built add it is used as an external project
if (NOT MAGIC_FOUND AND NOT TILEDB_LIBMAGIC_EP_BUILT)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Magic as an external project")

    if (WIN32)
      set(CFLAGS_DEF "")
    else()
      set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
      set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS} -fPIC")
    endif()

    # For both windows/nix using tiledb fork of file-windows from julian-r who has build a cmake version and patched for msvc++
    # that was modified by tiledb to also build with cmake for nix
    ExternalProject_Add(ep_magic
      PREFIX "externals"
      GIT_REPOSITORY "https://github.com/TileDB-Inc/file-windows.git"
      GIT_TAG "494060c2ea02494aabfbfc75e76d0af95c36156c"
      GIT_SUBMODULES_RECURSE TRUE
      UPDATE_COMMAND ""
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
        -Dlibmagic_STATIC_LIB=ON
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
    )
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_magic)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_LIBMAGIC_EP_BUILT=TRUE
    )

  else()
    message(FATAL_ERROR "Unable to find Magic")
  endif()
endif()

find_file(libmagic_DICTIONARY magic.mgc
  PATHS ${LIBMAGIC_PATHS}
  PATH_SUFFIXES bin share
  ${NO_DEFAULT_PATH}
)

if (libmagic_FOUND AND NOT TARGET libmagic)
  message(STATUS "Found Magic, adding imported target: ${libmagic_LIBRARIES}")
  add_library(libmagic UNKNOWN IMPORTED)
  set_target_properties(libmagic PROPERTIES
    IMPORTED_LOCATION "${libmagic_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${libmagic_INCLUDE_DIR}"
  )
  ## see comment in FindMagic.cmake: we need to set this here
  ## NB not currently used in CMakeLists.txt
  set(libmagic_LibraryName "libmagic")
endif()

# If we built a static EP, install it if required.
if (TILEDB_LIBMAGIC_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(libmagic)
  install_target_libs(pcre2-posix)
  install_target_libs(pcre2-8)
endif()
