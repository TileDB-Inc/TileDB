#
# TileDBCommon.cmake
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
# This file defines some common helper functions used by the external projects.
#

# Get library directory for multiarch linux distros
include(GNUInstallDirs)

#
# Stores the IMPORTED_LOCATION* target property of LIB_TARGET in RESULT_VAR.
# On Windows, preferentially tries the IMPORTED_IMPLIB* target property instead.
#
function(get_imported_location RESULT_VAR LIB_TARGET)
  if (WIN32)
    # Try several methods to find the imported location.
    get_target_property(TMP ${LIB_TARGET} IMPORTED_IMPLIB)
    if (TMP MATCHES "NOTFOUND")
      get_target_property(TMP ${LIB_TARGET} IMPORTED_IMPLIB_RELWITHDEBINFO)
    endif()
    if (TMP MATCHES "NOTFOUND")
      get_target_property(TMP ${LIB_TARGET} IMPORTED_IMPLIB_RELEASE)
    endif()
    if (TMP MATCHES "NOTFOUND")
      get_target_property(TMP ${LIB_TARGET} IMPORTED_IMPLIB_DEBUG)
    endif()
  endif()
  # Try several methods to find the imported location.
  if (TMP MATCHES "NOTFOUND" OR NOT WIN32)
    get_target_property(TMP ${LIB_TARGET} IMPORTED_LOCATION)
  endif()
  if (TMP MATCHES "NOTFOUND")
    get_target_property(TMP ${LIB_TARGET} IMPORTED_LOCATION_RELWITHDEBINFO)
  endif()
  if (TMP MATCHES "NOTFOUND")
    get_target_property(TMP ${LIB_TARGET} IMPORTED_LOCATION_RELEASE)
  endif()
  if (TMP MATCHES "NOTFOUND")
    get_target_property(TMP ${LIB_TARGET} IMPORTED_LOCATION_DEBUG)
  endif()
  set(${RESULT_VAR} "${TMP}" PARENT_SCOPE)
endfunction()

#
# Concatenates the library name of the given imported target to the installation
# prefix and stores the result in RESULT_VAR. If the given library does not reside
# in the external projects build directory (i.e. it was not built by an EP), then
# just return the imported location.
#
function(get_installed_location RESULT_VAR LIB_TARGET)
  get_imported_location(IMP_LOC ${LIB_TARGET})
  if ("${IMP_LOC}" MATCHES "^${TILEDB_EP_BASE}")
    get_filename_component(LIB_NAME "${IMP_LOC}" NAME)
    set(INSTALLED_PATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/${LIB_NAME}")
  else()
    set(INSTALLED_PATH "${IMP_LOC}")
  endif()
  set(${RESULT_VAR} "${INSTALLED_PATH}" PARENT_SCOPE)
endfunction()

#
# Adds imported libraries from the given target to the TileDB installation
# manifest.
#
function(install_target_libs LIB_TARGET)
  get_imported_location(TARGET_LIBRARIES ${LIB_TARGET})
  if (TARGET_LIBRARIES MATCHES "NOTFOUND")
    message(FATAL_ERROR "Could not determine library location for ${LIB_TARGET}")
  endif()
  if (WIN32 AND ${TARGET_LIBRARIES} MATCHES "${CMAKE_SHARED_LIBRARY_SUFFIX}$")
    install(FILES ${TARGET_LIBRARIES} DESTINATION ${CMAKE_INSTALL_BINDIR})
  else()
    install(FILES ${TARGET_LIBRARIES} DESTINATION ${CMAKE_INSTALL_LIBDIR})
  endif()
endfunction()

#
# Prepares arguments for an external-project invocation of cmake, forwarding a
# designated list of variables to the project.
#
# This function is a workaround for a deficiency of CMake. When a generator is
# specified, CMake forwards the generator spec to external projects. It does not,
# however, forward any toolchain file (defined with CMAKE_TOOLCHAIN_FILE) or any 
# cache variables set by non-default generators. A user should set any cache
# variables early, either directly (-D) or with an initial cache script (-C), and
# finally set TILEDB_FORWARDED_CACHE_VARIABLES to a list of these variable names.
#
# For each variable <VAR>, if <VAR>_TYPE is defined, it's used to set the cache
# type of the variable. This allows the (hopefully rare) circumstance where a user
# may have a need to manually examine or alter the cache of an external project
# with CMake-GUI.
#
# [in] global TILEDB_FORWARDED_CACHE_VARIABLES. Contains the variable list.
# [out] argument RESULT_VAR: Result assigned to RESULT_VAR in parent scope
#
function(propagate_cache_variables RESULT_VAR)
  foreach (cv IN LISTS TILEDB_FORWARDED_CACHE_VARIABLES)
    if (DEFINED ${cv})
      if (DEFINED ${cv}_TYPE)
        set (cv_type ${${cv}_TYPE})
      else()
        set (cv_type STRING)
      endif()
      list (APPEND result "-D${cv}:${cv_type}=${${cv}}")
    endif()
  endforeach()
  set (${RESULT_VAR} ${result} PARENT_SCOPE)
endfunction()
