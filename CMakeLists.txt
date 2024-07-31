#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017-2023 TileDB, Inc.
# Copyright (c) 2016 MIT and Intel Corporation
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

############################################################
# CMake setup
############################################################

cmake_minimum_required(VERSION 3.21)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Options")

include(CIConfig)
include(BuildOptions)
include(global-policies NO_POLICY_SCOPE)
include(Sanitizer)
include(TileDBToolchain)
include(Doxygen)
include(Format)

############################################################
# Parse version file
# credit: https://stackoverflow.com/a/47084079

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/tiledb/sm/c_api/tiledb_version.h" VERFILE)
if (NOT VERFILE)
  message(FATAL_ERROR "Failed to parse tiledb_version.h!")
endif()

string(REGEX MATCH "TILEDB_VERSION_MAJOR ([0-9])*" _ ${VERFILE})
set(TILEDB_VERSION_MAJOR ${CMAKE_MATCH_1})
string(REGEX MATCH "TILEDB_VERSION_MINOR ([0-9]+)*" _ ${VERFILE})
set(TILEDB_VERSION_MINOR ${CMAKE_MATCH_1})
string(REGEX MATCH "TILEDB_VERSION_PATCH ([0-9]+)*" _ ${VERFILE})
set(TILEDB_VERSION_PATCH ${CMAKE_MATCH_1})

set(TILEDB_VERSION "${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}.${TILEDB_VERSION_PATCH}")

############################################################
# Check for regex characters in the most important paths
# fixes https://github.com/TileDB-Inc/TileDB/issues/1799
option(TILEDB_ALLOW_REGEX_CHAR_PATH "If true, allow regex characters in source, build, or install path." FALSE)
mark_as_advanced(TILEDB_ALLOW_REGEX_CHAR_PATH)
set(REGEX_CHARS "[\\^\\$\\+\\*\\?\\|\\(\\)]") # note: must be escaped, and regex doesn't work with \[\] entries
set(REGEX_CHAR_PATH_MSG " contains a REGEX character and may break CMakeList processing. Please use"
                        " a different path, or set TILEDB_ALLOW_REGEX_CHAR_PATH to override.")
if (NOT TILEDB_ALLOW_REGEX_CHAR_PATH)
  if (CMAKE_CURRENT_SOURCE_DIR MATCHES ${REGEX_CHARS})
    message(FATAL_ERROR "CMAKE_CURRENT_SOURCE_DIR ${REGEX_CHAR_PATH_MSG}:\n  '${CMAKE_CURRENT_SOURCE_DIR}'")
  elseif (CMAKE_CURRENT_SOURCE_DIR MATCHES ${REGEX_CHARS})
    message(FATAL_ERROR "CMAKE_CURRENT_BINARY_DIR ${REGEX_CHAR_PATH_MSG}:\n  '${CMAKE_CURRENT_BINARY_DIR}'")
  elseif (CMAKE_CURRENT_SOURCE_DIR MATCHES ${REGEX_CHARS})
    message(FATAL_ERROR "CMAKE_INSTALL_PREFIX ${REGEX_CHAR_PATH_MSG}:\n  '${CMAKE_INSTALL_PREFIX}'")
  endif()
endif()

############################################################

set(TILEDB_CMAKE_INPUTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/cmake/inputs")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(APPLE)
  # Use @rpath on macOS for building shared libraries.
  set(CMAKE_MACOSX_RPATH ON)
  # Don't allow macOS .frameworks to be used for dependencies.
  set(CMAKE_FIND_FRAMEWORK NEVER)
endif()

# Set C++20 as default required standard for all C++ targets.
set(CMAKE_CXX_STANDARD 20 CACHE STRING "C++ Standard")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(CMAKE_SYSTEM_NAME MATCHES "CYGWIN")
  # Use GNU extensions under Cygwin
  set(CMAKE_CXX_EXTENSIONS ON)
else()
  set(CMAKE_CXX_EXTENSIONS OFF)
endif()

# Set -fvisibility=hidden (or equivalent) flags by default.
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

# Disable warnings from Boost
set(Boost_NO_WARN_NEW_VERSIONS ON)

project(TileDB)
message(STATUS "Starting TileDB regular build.")
message(STATUS "  CMake version: ${CMAKE_VERSION}")

############################################################
# Compile options/definitions for all targets
############################################################

# NOTE: Config-specific options must use the $<CONFIG> generator expression
#       and not check the value of CMAKE_BUILD_TYPE. The reason is that
#       the latter is not set when using multi-config generators like
#       Visual Studio or Xcode.

# Set compiler flags
if (MSVC)
  # Turn on standards-conformance mode
  add_compile_options("/permissive-")
  # /EH: Enables standard C++ stack unwinding.
  # s: Catches only standard C++ exceptions when you use catch(...) syntax.
  # c: The compiler assumes that functions declared as extern "C" never throw a C++ exception
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:/EHsc>)
  # We disable some warnings that are not present in gcc/clang -Wall:
  #   C4101: unreferenced local variable
  #   C4146: unary minus operator applied to unsigned type
  #   C4244: conversion warning of floating point to integer type.
  #   C4251: C++ export warning
  #   C4456: local variable hiding previous local variable
  #   C4457: local variable hiding function parameter
  #   C4702: unreachable code
  #   C4800: warning implicit cast int to bool
  add_compile_options(/W4 /wd4101 /wd4146 /wd4244 /wd4251 /wd4456 /wd4457 /wd4702 /wd4800)
  # Warnings as errors:
  if (TILEDB_WERROR)
    add_compile_options(/WX)
  endif()
  # Turn off MSVC deprecation of certain standard library functions. This allows
  # other deprecations to remain visible.
  add_compile_definitions("_CRT_SECURE_NO_WARNINGS")
  # We currently need to suppress warnings about deprecation (C4996) for two cases:
  #   1. C++ API functions that call deprecated C API functions
  #   2. two warnings in `test/src/helpers.cc` that call deprecated C API functions
  add_compile_options(/wd4996)
  # Disable GDI (which we don't need, and causes some macro
  # re-definition issues if wingdi.h is included)
  add_compile_definitions("NOGDI")
  # Add /MPn flag from CMake invocation (if defined).
  add_compile_options(${MSVC_MP_FLAG})
  # Build-specific flags
  add_compile_definitions("$<IF:$<CONFIG:Debug>,DEBUG,NDEBUG>")
  add_compile_options(
    # /Od: Disable optimizations
    # /bigobj: Increase number of sections in .obj file
    "$<$<CONFIG:Debug>:/Od;/bigobj>"
    # /Ox: Enable most speed optimizations
    "$<$<CONFIG:Release,RelWithDebInfo>:/Ox>"
    # /Zi: Generate debug info in a separate .pdb file
    "$<$<CONFIG:Debug,RelWithDebInfo>:/Zi>")
else()
  add_compile_options(-Wall -Wextra)
  if (TILEDB_WERROR)
    add_compile_options(-Werror)
  endif()
  # Build-specific flags
  add_compile_definitions("$<IF:$<CONFIG:Debug,Coverage>,DEBUG,NDEBUG>")
  add_compile_options(
    "$<$<CONFIG:Debug>:-O0;-g3;-ggdb3;-gdwarf-3>"
    "$<$<CONFIG:Coverage>:-g3;-gdwarf-3;--coverage>"
    "$<$<CONFIG:Release,RelWithDebInfo>:-O3>"
    "$<$<CONFIG:RelWithDebInfo>:-g3;-ggdb3;-gdwarf-3>")
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(
      "$<$<CONFIG:Debug>:-fstandalone-debug>"
      "$<$<CONFIG:Coverage>:-fprofile-instr-generate;-fcoverage-mapping>")
    add_link_options("$<$<CONFIG:Coverage>:--coverage;-fprofile-instr-generate;-fcoverage-mapping>")
  endif()

  # Disable newer Clang warnings about unqualified calls to std::move
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    if (${CMAKE_CXX_COMPILER_VERSION} VERSION_GREATER_EQUAL "14.0.3")
      add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wno-unqualified-std-cast-call>)
    endif()
  endif()

endif()

# Definitions for all targets
add_definitions(-D_FILE_OFFSET_BITS=64)

# AVX2 flag
include(CheckAVX2Support)
CheckAVX2Support()
if (COMPILER_SUPPORTS_AVX2)
  add_compile_options(${COMPILER_AVX2_FLAG})
endif()

if(TILEDB_SANITIZER)
  validate_sanitizer_options()
endif()

include(DetectStdPmr)

if(TILEDB_USE_CPP17_PMR)
  message(STATUS "Building with cpp17::pmr")
  add_definitions(-DUSE_CPP17_PMR)
else()
  message(STATUS "Building with std::pmr")
endif()

#######################################################
# Header Files
#######################################################
#
# Legacy headers: All headers for both C and C++ API in a single directory.
#
# Public header structure. Users of the library see this view. External source
# code uses these names; they should be considered unchangeable.
# - `<root>/`
#     `tiledb/`
#       - C API headers
#         `tiledb.h`
#         `tiledb_experimental.h`
#          other `tiledb_*.h`
#       - C++ API headers
#         `tiledb`
#         `tiledb_experimental`
#          other `tiledb_*`
#
# Headers assume `-I<root>`. To wit, for the C API use this:
# ```C
# #include <tiledb/tiledb.h>
# ```
# For the C++ API use this:
# ```C++
# #include <tiledb/tiledb>
# ```

# Private header structure. The compiler of a user program sees this view. Only
# TileDB source uses these names. They can be changed at will, but installation
# will also have to changed, as may other aspects of the build.
# - `<root>/`
#     `tiledb/`
#         `api/`
#             `c_api/`
#                 `api_external_common.h`
#                 `tiledb_export.h` (auto-generated)
#                 `<section>/` (multiple)
#                     `<section>_api_external.h`

##################################
# C API include files
##################################
#
# FILENAME headers are copied only as their file name to ${INCLUDE_BASE}/tiledb
# RELATIVE headers are copied as their path from RELATIVE_HEADER_BASE to ${INCLUDE_BASE}
# In all cases the included path begins with `tiledb/`
# The export header is handled separatedly below.

list(APPEND TILEDB_C_API_FILENAME_HEADERS
    "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/tiledb.h"
    "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/tiledb_deprecated.h"
    "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/tiledb_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/tiledb_version.h"
    "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/tiledb_experimental.h"
    "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/tiledb_dimension_label_experimental.h"
)

if (TILEDB_SERIALIZATION)
    list(APPEND TILEDB_C_API_FILENAME_HEADERS
        "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/tiledb_serialization.h"
    )
endif()

list(APPEND TILEDB_C_API_RELATIVE_HEADERS
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/api_external_common.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/array_schema/array_schema_api_deprecated.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/array_schema/array_schema_api_experimental.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/array_schema/array_schema_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/array_schema/array_type_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/array_schema/layout_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/attribute/attribute_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/attribute/attribute_api_external_experimental.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/buffer/buffer_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/buffer_list/buffer_list_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/config/config_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/context/context_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/current_domain/current_domain_api_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/current_domain/current_domain_api_external_experimental.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/data_order/data_order_api_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/data_order/data_order_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/datatype/datatype_api_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/datatype/datatype_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/dimension/dimension_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/dimension_label/dimension_label_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/domain/domain_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/enumeration/enumeration_api_experimental.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/error/error_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/filesystem/filesystem_api_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/filesystem/filesystem_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/filter/filter_api_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/filter/filter_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/filter_list/filter_list_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/group/group_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/ndrectangle/ndrectangle_api_external_experimental.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/object/object_api_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/object/object_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/query/query_api_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/query/query_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/query_aggregate/query_aggregate_api_external_experimental.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/query_field/query_field_api_external_experimental.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/query_plan/query_plan_api_external_experimental.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/string/string_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/vfs/vfs_api_enum.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/vfs/vfs_api_external.h"
    "${CMAKE_SOURCE_DIR}/tiledb/api/c_api/vfs/vfs_api_experimental.h"
)
set(TILEDB_C_API_RELATIVE_HEADER_BASE "${CMAKE_CURRENT_SOURCE_DIR}")

##################################
# Export Header
##################################
# The export header is automatically generated later in the build (using the
# main library target). When installed, it's in the same directory as the
# compiler-visible, interface-hidden header `api_external_common.h`.

set(TILEDB_EXPORT_HEADER_DIR "${CMAKE_BINARY_DIR}/tiledb")
set(TILEDB_EXPORT_HEADER_NAME "tiledb_export.h")
set(TILEDB_EXPORT_HEADER "${TILEDB_EXPORT_HEADER_DIR}/${TILEDB_EXPORT_HEADER_NAME}")
set(TILEDB_EXPORT_HEADER_LOCALINSTALL_PATH "tiledb/api/c_api/${TILEDB_EXPORT_HEADER_NAME}")

##################################
# Library Configuration
##################################
#
# `configuration_definitions` is an interface library that contains compiler
# definitions corresponding to the configuration variables passed to CMake

add_library(configuration_definitions INTERFACE)

if (TILEDB_AZURE)
  target_compile_definitions(configuration_definitions INTERFACE -DHAVE_AZURE)
endif()

if (TILEDB_GCS)
  target_compile_definitions(configuration_definitions INTERFACE -DHAVE_GCS)
endif()

if (TILEDB_HDFS)
  target_compile_definitions(configuration_definitions INTERFACE -DHAVE_HDFS)
endif()

if (TILEDB_S3)
  target_compile_definitions(configuration_definitions INTERFACE -DHAVE_S3)
endif()

##################################
# Local install
##################################
#
# User code within this project requires manual installation that mimics a
# an installation from a distribution artifact. It's required by examples,
# integration tests, and regression tests.
#
# Caveat: The "installation" is done at _configuration_ time, not at _build_ time.
# This is a side-effect of using `configure_file`. It's not necessary and it may
# be desirable to change it to operate at build time in the future.

set(TILEDB_LOCALINSTALL_DIR "${CMAKE_BINARY_DIR}/dist-in-build")
set(TILEDB_LOCALINSTALL_INCLUDE "${TILEDB_LOCALINSTALL_DIR}/include")

#####################
# C API
#####################
foreach(HEADER ${TILEDB_C_API_FILENAME_HEADERS})
  cmake_path(GET HEADER FILENAME HEADER_STRIPPED)
  configure_file(${HEADER} ${TILEDB_LOCALINSTALL_INCLUDE}/tiledb/${HEADER_STRIPPED} COPYONLY)
endforeach()
foreach(HEADER ${TILEDB_C_API_RELATIVE_HEADERS})
  cmake_path(RELATIVE_PATH HEADER
      BASE_DIRECTORY ${TILEDB_C_API_RELATIVE_HEADER_BASE}
      OUTPUT_VARIABLE HEADER_STRIPPED
      )
  configure_file(${HEADER} ${TILEDB_LOCALINSTALL_INCLUDE}/${HEADER_STRIPPED} COPYONLY)
endforeach()
# `configure_file` for the export header happens after it's been generated


#####################
# C++ API
#####################
file(GLOB TILEDB_CPP_HEADERS
    "${CMAKE_SOURCE_DIR}/tiledb/sm/cpp_api/*.h"
    "${CMAKE_SOURCE_DIR}/tiledb/sm/cpp_api/tiledb"
    "${CMAKE_SOURCE_DIR}/tiledb/sm/cpp_api/tiledb_experimental"
    )
foreach(HEADER ${TILEDB_CPP_HEADERS})
  string(REGEX
      REPLACE "^${CMAKE_SOURCE_DIR}/tiledb/sm/cpp_api/" ""
      HEADER_STRIPPED ${HEADER}
      )
  configure_file(${HEADER} ${TILEDB_LOCALINSTALL_INCLUDE}/tiledb/${HEADER_STRIPPED} COPYONLY)
endforeach()

#####################
# Interface library
#####################
add_library(local_install INTERFACE)
target_include_directories(local_install INTERFACE ${TILEDB_LOCALINSTALL_INCLUDE})
target_include_directories(local_install INTERFACE ${CMAKE_SOURCE_DIR})

############################################################
# Enable testing and add subdirectories
############################################################

# Enable testing
enable_testing()

# -------------------------------------------------------
# Accumulators for object libraries and unit tests
# -------------------------------------------------------
#
# All link-completeness targets from object libraries are aggregated onto a
# single target.
#
# 1. Before any subdirectories are added:
#    a) Declare a target for all link-completeness checks. It's a phantom target
#       having only dependencies and with no build actions of its own.
#    b) Declare an accumulator for link-completeness targets.
# 2. For each object library within the various subdirectories, add a
#    link-completeness target, either
#    a) manually, as an explicit dependency of the `all_link_complete` target.
#    b) automatically, as an implicit part of an object library environment.
#       The environment puts the link-completess target into the accumulator.
# 3. After all the subdirectories are added:
#    a) The list of all targets from the accumulator is added as a dependency
#       of `all_link_complete`.
#
# All unit tests are similarly aggregated onto the target `all_unit_tests`.
#
include(accumulator)

add_custom_target(all_link_complete)
define_accumulator(object_library_compile_targets)

define_accumulator(unit_test_targets)
#
# This is the legacy position of this target declaration; it should appear in
# test section below. It cannot right now because certain tests are declaring
# dependencies on it directly. It should be initialized only through
# accumulators that gather the names of unit test targets.
#
add_custom_target(all_unit_tests)

# -------------------------------------------------------
# Subdirectories
# -------------------------------------------------------

# Build the TileDB library experimental features
add_subdirectory(experimental)
if (TILEDB_TESTS)
  # Isolate the experimental unit tests into their own target
  add_custom_target(experimental_unit_tests)
  retrieve_from(Unit_Tests ACCUMULATOR unit_test_targets)
  if (${Unit_Tests})
    add_dependencies(experimental_unit_tests ${Unit_Tests})
  endif()
  reset_accumulator(ACCUMULATOR unit_test_targets)
endif()

# Build the TileDB library
add_subdirectory(tiledb)

# Build examples
add_subdirectory(examples)

add_subdirectory(experimental/experimental_examples)

# Build tools
if (TILEDB_TOOLS)
  add_subdirectory(tools)
endif()

# -------------------------------------------------------
# Tests
# -------------------------------------------------------
if (TILEDB_TESTS)
  add_subdirectory(test/support)
  add_subdirectory(test)

  # ----------------------------------
  # All unit test executables from `unit_test` environments are dependencies of
  # the `all_unit_tests` phantom target.
  #
  retrieve_from(Unit_Tests ACCUMULATOR unit_test_targets)
  add_custom_target(ordinary_unit_tests)
  add_dependencies(ordinary_unit_tests ${Unit_Tests})
  add_dependencies(all_unit_tests ordinary_unit_tests)
  add_dependencies(all_unit_tests experimental_unit_tests)

  # ----------------------------------
  # Target `tests` build and runs all test executables
  add_custom_target(tests)
  add_dependencies(tests all_unit_tests)

  # ----------------------------------
  # Test targets added explicitly
  add_dependencies(tests tiledb_unit)
  add_dependencies(tests tiledb_regression)
  add_dependencies(tests test_assert)

  # C API support
  add_dependencies(tests unit_capi_handle unit_capi_exception_wrapper)
  # C API basics
  add_dependencies(tests unit_capi_config unit_capi_context)
  add_dependencies(tests unit_capi_array)
  add_dependencies(tests unit_capi_array_schema)
  add_dependencies(tests unit_capi_buffer)
  add_dependencies(tests unit_capi_buffer_list)
  add_dependencies(tests unit_capi_data_order)
  add_dependencies(tests unit_capi_datatype)
  add_dependencies(tests unit_capi_filesystem)
  add_dependencies(tests unit_capi_query_type)
  add_dependencies(tests unit_capi_vfs)
  # C API array schema
  add_dependencies(tests unit_capi_filter unit_capi_filter_list unit_capi_dimension_label)

  if (ENABLE_MAGIC_TEST)
    add_dependencies(tests unit_mgc_dict)
  endif()

  add_subdirectory(test/regression)

  # Add custom target 'check', which builds and runs all tests.
  # This includes both the main test suite in the 'test' directory (consisting of
  # tiledb_unit, tiledb_regression and other tests), as well as the standalone
  # tests in subdirectories of the 'tiledb' directory.
  add_custom_target(check
    COMMAND ${CMAKE_CTEST_COMMAND} -V -C $<CONFIG>
    DEPENDS tests
    USES_TERMINAL
  )
endif()

# -------------------------------------------------------
#
# All the compile-targets from object library definitions are dependencies of
# the `all_link_complete` phantom target.
#
retrieve_from(Compile_Targets ACCUMULATOR object_library_compile_targets)
add_dependencies(all_link_complete ${Compile_Targets})

###########################################################
# Uninstall
###########################################################

set(CMD "xargs printf -- '-- Uninstalling: %s\\\\n' <install_manifest.txt")
add_custom_target(
   uninstall
   COMMAND echo "Uninstalling TileDB from ${CMAKE_INSTALL_PREFIX}..."
   COMMAND eval "${CMD}"
   COMMAND xargs rm -f < install_manifest.txt
   COMMAND rmdir "${CMAKE_INSTALL_PREFIX}/include/tiledb"
   COMMAND echo "TileDB uninstalled"
)

###########################################################
# Run the optional extra cmake include
###########################################################

if(TILEDB_EXTRA_CMAKE_INCLUDE)
  get_filename_component(
    EXTRA_INCLUDE_ABSPATH
    "${TILEDB_EXTRA_CMAKE_INCLUDE}"
    ABSOLUTE
  )
  include("${EXTRA_INCLUDE_ABSPATH}")
endif()
