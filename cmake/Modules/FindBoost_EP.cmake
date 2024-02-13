#
# FindBoost_EP.cmake
#
#
# The MIT License
#
# Copyright (c) 2024 TileDB, Inc.
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
# Finds the Boost library, installing with an ExternalProject as necessary.
# This module defines:
#   Boost::container - The target to link against

# Include some common helper functions.
include(TileDBCommon)

if(TILEDB_VCPKG)
  set(Boost_NO_SYSTEM_PATHS ON)
  find_package(Boost REQUIRED COMPONENTS container)
  return()
endif()

# First check for a static version in the EP prefix.
find_library(BOOST_LIBRARIES
  NAMES
    libboost_container${CMAKE_STATIC_LIBRARY_SUFFIX}
  PATHS ${TILEDB_EP_INSTALL_PREFIX}
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH
)

if (BOOST_LIBRARIES)
  set(BOOST_STATIC_EP_FOUND TRUE)
  find_path(BOOST_INCLUDE_DIR
    NAMES boost/container/pmr/polymorphic_allocator.hpp
    PATHS ${TILEDB_EP_INSTALL_PREFIX}
    PATH_SUFFIXES include
    NO_DEFAULT_PATH
  )
elseif(NOT TILEDB_FORCE_ALL_DEPS)
  set(BOOST_STATIC_EP_FOUND FALSE)
  # Static EP not found, search in system paths.
  find_library(BOOST_LIBRARIES
    NAMES boost_container
    PATH_SUFFIXES lib bin
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
  find_path(BOOST_INCLUDE_DIR
    NAMES boost/container/pmr/polymorphic_allocator.hpp
    PATH_SUFFIXES include
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Boost
  REQUIRED_VARS BOOST_LIBRARIES BOOST_INCLUDE_DIR
)

if (NOT BOOST_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding Boost as an external project")

    set(boost_URL "https://boostorg.jfrog.io/artifactory/main/release/1.83.0/source/boost_1_83_0.tar.gz")
    set(boost_SHA256 "c0685b68dd44cc46574cce86c4e17c0f611b15e195be9848dfd0769a0a207628")
    set(boost_INSTALL ${TILEDB_EP_INSTALL_PREFIX})
    set(BOOST_INCLUDE_DIR ${boost_INSTALL}/include)
    set(BOOST_LIB_DIR ${boost_INSTALL}/lib)

    if (WIN32)
      ExternalProject_Add(ep_boost
        PREFIX boost
        URL ${boost_URL}
        URL_HASH SHA256=${boost_SHA256}
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND bootstrap --with-libraries=container --prefix=${boost_INSTALL}
        BUILD_COMMAND .\\b2 -d0 install link=static variant=release threading=multi runtime-link=static
        INSTALL_COMMAND ""
        INSTALL_DIR ${boost_INSTALL}
      )
    else()
      ExternalProject_Add(ep_boost
        PREFIX boost
        URL ${boost_URL}
        URL_HASH SHA256=${boost_SHA256}
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND ./bootstrap.sh --with-libraries=container --prefix=${boost_INSTALL}
        BUILD_COMMAND ./b2 -d0 install link=static variant=release threading=multi runtime-link=static
        INSTALL_COMMAND ""
        INSTALL_DIR ${boost_INSTALL}
      )
    endif()

    list(APPEND FORWARD_EP_CMAKE_ARGS -DTILEDB_BOOST_EP_BUILT=TRUE)
    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_boost)
  else()
    message(FATAL_ERROR "Unable to find Boost")
  endif()
endif()

if (BOOST_FOUND AND NOT TARGET Boost::container)
  add_library(Boost::container UNKNOWN IMPORTED)
  set_target_properties(Boost::container PROPERTIES
    IMPORTED_LOCATION "${BOOST_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${BOOST_INCLUDE_DIR}"
  )
endif()
