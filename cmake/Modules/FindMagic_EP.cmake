#
# FindMagic_EP.cmake
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
# Finds the Magic library, installing with an ExternalProject as necessary.
# This module defines:
#   - libmagic_INCLUDE_DIR, directory containing headers
#   - libmagic_LIBRARIES, the Magic library path
#   - libmagic_FOUND, whether Magic has been found
#   - The libmagic imported target

# Include some common helper functions.
include(TileDBCommon)

# Search the path set during the superbuild for the EP.
set(LIBMAGIC_PATHS ${TILEDB_EP_INSTALL_PREFIX})

# Try the builtin find module unless built w/ EP superbuild
if ((NOT TILEDB_FORCE_ALL_DEPS) AND (NOT TILEDB_LIBMAGIC_EP_BUILT))
  find_package(libmagic ${TILEDB_DEPS_NO_DEFAULT_PATH} QUIET)
elseif(TILEDB_LIBMAGIC_EP_BUILT)
  find_package(libmagic PATHS ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_DEPS_NO_DEFAULT_PATH})
endif()

# Next try finding the superbuild external project
if (NOT libmagic_FOUND)
  find_path(libmagic_INCLUDE_DIR
    NAMES magic.h
    PATHS ${LIBMAGIC_PATHS}
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  if (NOT libmagic_INCLUDE_DIR)
    find_path(libmagic_INCLUDE_DIR
      NAMES file/file.h
      PATHS ${LIBMAGIC_PATHS}
      PATH_SUFFIXES include
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )
  endif()

  # Link statically if installed with the EP.
  find_library(libmagic_LIBRARIES
    magic
    PATHS ${LIBMAGIC_PATHS}
    PATH_SUFFIXES lib a
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )

  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(libmagic
    REQUIRED_VARS libmagic_LIBRARIES libmagic_INCLUDE_DIR
  )
endif()

# If not found, add it as an external project
if (NOT libmagic_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Magic as an external project")

    if (WIN32)
      set(CFLAGS_DEF "")
    else()
      set(CFLAGS_DEF "${CMAKE_C_FLAGS} -fPIC")
      set(CXXFLAGS_DEF "${CMAKE_CXX_FLAGS} -fPIC")
    endif()

    if (WIN32)
      # For windows we need to use file-windows from julian-r who has build a cmake version and patched for msvc++
      ExternalProject_Add(ep_magic
              PREFIX "externals"
              GIT_REPOSITORY "https://github.com/TileDB-Inc/file-windows.git"
              GIT_TAG "cmake-install-support"
              GIT_SUBMODULES_RECURSE TRUE
              UPDATE_COMMAND ""
              CMAKE_ARGS
                -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
                -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                "-DCMAKE_C_FLAGS=${CFLAGS_DEF}"
              LOG_DOWNLOAD TRUE
              LOG_CONFIGURE TRUE
              LOG_BUILD TRUE
              LOG_INSTALL TRUE
              LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
              )
    else()
      ExternalProject_Add(ep_magic
        PREFIX "externals"
        URL "ftp://ftp.astron.com/pub/file/file-5.41.tar.gz"
        URL_HASH SHA1=8d80d2d50f4000e087aa60ffae4f099c63376762
        DOWNLOAD_NAME "file-5.41.tar.gz"
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND
              ${TILEDB_EP_BASE}/src/ep_magic/configure --prefix=${TILEDB_EP_INSTALL_PREFIX} CFLAGS=${CFLAGS_DEF} CXXFLAGS=${CXXFLAGS_DEF}
        BUILD_IN_SOURCE TRUE
        BUILD_COMMAND $(MAKE)
        INSTALL_COMMAND $(MAKE) install
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
        LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      )
    endif()
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_magic)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_LIBMAGIC_EP_BUILT=TRUE
    )

    set(TILEDB_LIBMAGIC_DIR "${TILEDB_EP_INSTALL_PREFIX}")

  else()
    message(FATAL_ERROR "Unable to find Magic")
  endif()
endif()

if (libmagic_FOUND AND NOT TARGET libmagic)
  message(STATUS "Found Magic, adding imported target: ${libmagic_LIBRARIES}")
  add_library(libmagic UNKNOWN IMPORTED)
  set_target_properties(libmagic PROPERTIES
    IMPORTED_LOCATION "${libmagic_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${libmagic_INCLUDE_DIR}"
  )
endif()

#########################
# from https://stackoverflow.com/a/56738858
## https://stackoverflow.com/questions/32183975/how-to-print-all-the-properties-of-a-target-in-cmake/56738858#56738858
## https://stackoverflow.com/a/56738858/3743145

## Get all properties that cmake supports
#execute_process(COMMAND cmake --help-property-list OUTPUT_VARIABLE CMAKE_PROPERTY_LIST)
execute_process(COMMAND "${CMAKE_COMMAND}" --help-property-list OUTPUT_VARIABLE CMAKE_PROPERTY_LIST)
## Convert command output into a CMake list
STRING(REGEX REPLACE ";" "\\\\;" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
STRING(REGEX REPLACE "\n" ";" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")

list(REMOVE_DUPLICATES CMAKE_PROPERTY_LIST)

function(print_target_properties tgt)
    if(NOT TARGET ${tgt})
      message("There is no target named '${tgt}'")
      return()
    endif()

    foreach (prop ${CMAKE_PROPERTY_LIST})
        string(REPLACE "<CONFIG>" "${CMAKE_BUILD_TYPE}" prop ${prop})
        get_target_property(propval ${tgt} ${prop})
        if (propval)
            message ("${tgt} ${prop} = ${propval}")
        endif()
    endforeach(prop)
endfunction(print_target_properties)
#########################

if(MSYS)
  set(LIBREGEX_PATHS ${TILEDB_EP_INSTALL_PREFIX})
  message(STATUS "dlh MSYS from findmagic_ep.cmake")
  # on our msys build, a .dll is found, links ok.
  # on our rtools40 build, a static libmagic.a is found, but it requires a 
  # corresponding static regex.a to satisfy regcomp/regexec/regerror/something-else
  find_package(libregex )
  message(STATUS "libregex_FOUND ${libregex_FOUND}")
  if (NOT libregex_FOUND)
    find_path(libregex_INCLUDE_DIR
      NAMES regex.h
      PATHS ${LIBREGEX_PATHS}
      PATH_SUFFIXES include
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )

    if (NOT libregex_INCLUDE_DIR)
      message(STATUS "dlh did NOT find regex.h!")
      #find_path(libregex_INCLUDE_DIR
      #  NAMES file/file.h
      #  PATHS ${LIBREGEX_PATHS}
      #  PATH_SUFFIXES include
      #  ${TILEDB_DEPS_NO_DEFAULT_PATH}
      #)
    endif()

    # Link statically if installed with the EP.
    find_library(libregex_LIBRARIES
      regex
      PATHS ${LIBREGEX_PATHS}
      PATH_SUFFIXES lib a
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )

    include(FindPackageHandleStandardArgs)
    FIND_PACKAGE_HANDLE_STANDARD_ARGS(libregex
      REQUIRED_VARS libregex_LIBRARIES libregex_INCLUDE_DIR
    )
  
    message(STATUS "second libregex_FOUND ${libregex_FOUND}")
  endif()

  if(libregex_FOUND)
    if(TARGET libregex)
      print_target_properties( libregex )
    else()
      message(STATUS "libregex NOT a target :(")
    endif()
  endif()
  
  if(libregex_FOUND)
    if(TARGET tiledb)
      # target_link_libraries(tiledb public libregex_LIBRARIES)
      # target_link_libraries(libmagic public libregex_LIBRARIES)
      add_library(tiledb libregex)
    endif()
    if(TARGET tiledbstatic)
      add_library(tiledbstatic libregex)
    endif()
  endif()
else()
  message(STATUS "dlh MSYS NOT from findmagic_ep.cmake")
endif()

# If we built a static EP, install it if required.
if (TILEDB_LIBMAGIC_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
  install_target_libs(libmagic)
endif()
