#
# FindCatch_EP.cmake
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
# Finds the Catch library, installing with an ExternalProject as necessary.
# This module defines:
#   - CATCH_INCLUDE_DIR, directory containing headers
#   - CATCH2_FOUND, whether Catch has been found
#   - TILEDB_CATCH2_SOURCES_DIR directory containing headers, lib source
#   - The Catch2::Catch2 imported target

# Include some common helper functions.
include(TileDBCommon)

# Search the path set during the superbuild for the EP.
message(VERBOSE "searching for catch in ${TILEDB_EP_SOURCE_DIR}")
set(CATCH_PATHS ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_EP_SOURCE_DIR}/ep_catch/src)

if (NOT TILEDB_FORCE_ALL_DEPS OR TILEDB_CATCH_EP_BUILT)
  find_path(CATCH_INCLUDE_DIR
    NAMES catch2/catch_all.hpp
    PATHS ${CATCH_PATHS}
    #PATH_SUFFIXES "catch2"
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

if(0)
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Catch2
  REQUIRED_VARS 
    CATCH_INCLUDE_DIR 
    #CATCH_LIBRARIES
)
else()
  find_package(Catch2
    HINTS
      ${CATCH_PATHS}
    )
  if(Catch2_FOUND)
    set(CATCH_INCLUDE_DIR ${Catch2_INCLUDE_DIR})
    set(CATCH_LIBRARIES ${Catch2_LIBRARIES})
  endif()
endif()

message("CATCH_INCLUDE_DIR is \"${CATCH_INCLUDE_DIR}\", CATCH_LIBRARIES is \"${CATCH_LIBRARIES}\"")

if(Catch2_FOUND)
  #print_target_properties(Catch2)
  print_target_properties(Catch2::Catch2WithMain)
  print_target_properties(Catch2::Catch2)
  set(TILEDB_CATCH2_SOURCES_DIR ${TILEDB_EP_SOURCE_DIR}/ep_catch/src/catch2)
  message("Catch2_FOUND is ${Catch2_FOUND}, CATCH_INCLUDE_DIR is \"${CATCH_INCLUDE_DIR}\", CATCH_LIBRARIES is \"${CATCH_LIBRARIES}\"")
else()
  message("TILEDB_SUPERBUILD is ${TILEDB_SUPERBUILD}, Catch2_FOUND is ${Catch2_FOUND}, CATCH2_FOUND is ${CATCH2_FOUND}")
endif()

if (NOT CATCH2_FOUND AND TILEDB_SUPERBUILD)
  message(STATUS "Adding Catch as an external project")
  ExternalProject_Add(ep_catch
    PREFIX "externals"
    # Set download name to avoid collisions with only the version number in the filename
    DOWNLOAD_NAME ep_catch.zip
    #URL "https://github.com/catchorg/Catch2/archive/v2.13.8.zip"
    #URL_HASH SHA1=73adf43795abb7f481f07d307d11ac1fbc7a6015
    URL "https://github.com/catchorg/Catch2/archive/v3.1.0.zip"
    URL_HASH SHA1=b23753594a743feabd4e30f83b31ebb31081092a
    CMAKE_ARGS
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
      -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
      # https://stackoverflow.com/questions/66227246/catch2-undefined-reference-to-catchstringmaker
      # catch build reportedly defaults to c++14, apparently building as cxx17 avoids...
      -DCMAKE_CXX_STANDARD=17 # to avoid undefined ...Catch::StringMaker
    #CONFIGURE_COMMAND ""
    #BUILD_COMMAND ""
    #INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    LOG_DOWNLOAD TRUE
    LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
  )
  list(APPEND TILEDB_EXTERNAL_PROJECTS ep_catch)
  list(APPEND FORWARD_EP_CMAKE_ARGS
    -DTILEDB_CATCH_EP_BUILT=TRUE
  )
endif()

if (CATCH2_FOUND AND NOT TARGET Catch2::Catch2)
  add_library(Catch2::Catch2 INTERFACE IMPORTED)
  set_target_properties(Catch2::Catch2 PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES 
      "${CATCH_INCLUDE_DIR}"
      "${CATCH_INCLUDE_DIR}/.."
  )
endif()
