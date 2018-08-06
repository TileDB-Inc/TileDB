#
# tiledb/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017-2018 TileDB, Inc.
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
# CMake policies
############################################################

# C++ library, allow the VISIBLITY_PRESET for each c++ target to work
cmake_policy(SET CMP0063 NEW)

############################################################
# Source files
############################################################

# The core header directory.
set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")

# List of API headers (to be installed)
set(TILEDB_PUBLIC_HEADERS
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb.h
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb_enum.h
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb_version.h
)

if (TILEDB_CPP_API)
  list(APPEND TILEDB_PUBLIC_HEADERS
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/tiledb
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array_schema.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/attribute.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/compressor.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/config.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/context.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/core_interface.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/deleter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/dimension.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/domain.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/exception.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/filter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/filter_list.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/group.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/map.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/map_schema.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/object.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/object_iter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/schema_base.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/stats.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/type.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/utils.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/version.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/vfs.h
  )
else()
  message(STATUS "TileDB C++ API is not built.")
endif()

# List of all core source files
set(TILEDB_CORE_SOURCES
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/rest/capnp/array.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/rest/capnp/tiledb-rest.capnp.c++
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array/array.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/array_schema.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/attribute.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/dimension.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/domain.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/const_buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/preallocated_buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cache/lru_cache.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/bzip_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/dd_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/gzip_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/lz4_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/rle_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/zstd_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/encryption/encryption.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/encryption/encryption_key.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/encryption/encryption_key_validation.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/encryption/encryption_openssl.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/encryption/encryption_win32.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/hdfs_filesystem.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/posix.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/vfs.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/vfs_file_handle.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/win.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/bit_width_reduction_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/bitshuffle_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/byteshuffle_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/compression_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/encryption_aes256gcm_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter_buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter_pipeline.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter_storage.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/noop_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/positive_delta_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/fragment_metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/global_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/openssl_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/signal_handlers.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/tbb_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/watchdog.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/kv/kv.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/kv/kv_item.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/kv/kv_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/constants.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/logger.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/stats.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/status.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/thread_pool.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/uri.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/utils.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/uuid.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/win_constants.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/query.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/reader.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/writer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/dense_cell_range_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/context.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/config.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/config_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/open_array.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/storage_manager.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/tile.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/tile_io.cc
)

# 'External' source files included in the source tree.
set(TILEDB_EXTERNALS_INCLUDE_DIRS
  "${CMAKE_CURRENT_SOURCE_DIR}/../external/include"
  "${CMAKE_CURRENT_SOURCE_DIR}/../external/include/bitshuffle"
  "${CMAKE_CURRENT_SOURCE_DIR}/../external/include/blosc"
)
set(TILEDB_EXTERNALS_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/src/md5/md5.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/src/bitshuffle/iochain.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/src/bitshuffle/bitshuffle_core.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/src/blosc/shuffle.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/src/blosc/shuffle-avx2.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/src/blosc/shuffle-generic.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/src/blosc/shuffle-sse2.cc
)

############################################################
# Build core objects as a reusable object library
############################################################

add_library(TILEDB_CORE_OBJECTS OBJECT 
    ${TILEDB_CORE_SOURCES}
    ${TILEDB_EXTERNALS_SOURCES}
)

# Compile all core sources with PIC
set_property(TARGET TILEDB_CORE_OBJECTS PROPERTY POSITION_INDEPENDENT_CODE ON)

target_include_directories(TILEDB_CORE_OBJECTS
  PRIVATE
    "${TILEDB_CORE_INCLUDE_DIR}"
    "${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api"
    "${TILEDB_EXTERNALS_INCLUDE_DIRS}"
)

if (TILEDB_CPP_API)
  target_include_directories(TILEDB_CORE_OBJECTS
    PRIVATE
      "${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api"
  )
endif()

############################################################
# Compile options/definitions
############################################################

if (SANITIZER)
  if (NOT CMAKE_BUILD_TYPE MATCHES "Debug")
    message(FATAL_ERROR "Sanitizers only enabled for Debug build")
  endif()
  # Enable sanitizers only on clang for now
  if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "Sanitizers only enabled for the clang compiler")
  endif()
  string(TOLOWER ${SANITIZER} SANITIZER)
  if (NOT SANITIZER MATCHES "^(address|memory|thread|undefined)$")
    message(FATAL_ERROR "Unknown clang sanitizer: ${SANITIZER})")
  else()
    message(STATUS "The TileDB library is compiled with sanitizer ${SANITIZER} enabled")
  endif()
  target_compile_options(TILEDB_CORE_OBJECTS
    PRIVATE
      -g -fno-omit-frame-pointer -fno-optimize-sibling-calls -fsanitize=${SANITIZER}
  )
endif()

if (TILEDB_VERBOSE)
  add_definitions(-DTILEDB_VERBOSE)
  message(STATUS "The TileDB library is compiled with verbosity.")
endif()

############################################################
# Dependencies: set up includes/linking
############################################################

# Unfortunately, with CMake < 3.12, you can't use target_link_libraries()
# on TILEDB_CORE_OBJECTS. This workaround uses an interface library
# so we can use the targets created by the calls to find_package().
add_library(TILEDB_CORE_OBJECTS_ILIB INTERFACE)

# Find OpenSSL first in case it's needed for S3
if (NOT WIN32)
  find_package(OpenSSL_EP REQUIRED)
endif()

# S3 dependencies
if (TILEDB_S3)
  message(STATUS "The TileDB library is compiled with S3 support.")
  find_package(Curl_EP REQUIRED)
  find_package(AWSSDK_EP REQUIRED)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      AWSSDK::aws-cpp-sdk-s3
      AWSSDK::aws-cpp-sdk-core
  )
  if (NOT WIN32)
    # No Curl required on Windows.
    target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
      INTERFACE
        Curl::Curl
    )
  endif()
  add_definitions(-DHAVE_S3)
endif()

# HDFS dependencies
if (TILEDB_HDFS)
  if(WIN32)
    message(FATAL_ERROR "TileDB HDFS backend is not supported for Windows builds currently")
  else()
    message(STATUS "The TileDB library is compiled with HDFS support.")
    add_definitions(-DHAVE_HDFS)
  endif()
endif()

# TBB dependency
if (TILEDB_TBB)
  find_package(TBB_EP REQUIRED)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      TBB::tbb
  )
  add_definitions(-DHAVE_TBB)
endif()

# Sanitizer linker flags
if (SANITIZER)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      -fsanitize=${SANITIZER}
  )
endif()

# Coverage linker flags
if (CMAKE_BUILD_TYPE MATCHES "Coverage")
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      --coverage
  )
endif()

if (NOT WIN32)
  # On Linux, must explicitly link -lpthread -ldl in order for static linking
  # to libzstd or libcurl to work.
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE pthread dl)
endif()

# Required dependencies. These come last as any prior dependencies relying on
# these must be listed first for Linux. E.g. AWSSDK and Curl depend on Zlib,
# which is installed here.
find_package(Bzip2_EP REQUIRED)
find_package(LZ4_EP REQUIRED)
find_package(Spdlog_EP REQUIRED)
find_package(Zlib_EP REQUIRED)
find_package(Zstd_EP REQUIRED)
find_package(Capnp_EP REQUIRED)
target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
  INTERFACE
    Bzip2::Bzip2
    LZ4::LZ4
    Spdlog::Spdlog
    Zlib::Zlib
    Zstd::Zstd
    Capnp::capnp
    Capnp::capnp-json
    Capnp::kj
)
if (NOT WIN32)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      OpenSSL::SSL
      OpenSSL::Crypto
  )
endif()

# Win32 specific libraries
if (WIN32)
  set(WIN32_LIBS shlwapi rpcrt4 bcrypt)

  if (TILEDB_S3)
    list(APPEND WIN32_LIBS bcrypt winhttp wininet userenv version)
  endif()

  foreach (LIB ${WIN32_LIBS})
    find_library(LIB_${LIB} ${LIB})
    message(STATUS "Found Win32 lib ${LIB}: ${LIB_${LIB}}")
    target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE ${LIB_${LIB}})
  endforeach()
endif()

# Copy over dependency info (e.g. include directories) to the core objects.
target_compile_definitions(TILEDB_CORE_OBJECTS
  PRIVATE
    $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS_ILIB,INTERFACE_COMPILE_DEFINITIONS>
)
target_include_directories(TILEDB_CORE_OBJECTS
  PRIVATE
    $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS_ILIB,INTERFACE_INCLUDE_DIRECTORIES>
)

############################################################
# Generated dependency information (for installation)
############################################################

# This section defines variables that will be used in the generation of the
# TileDBConfig.cmake file, used for find_package(TileDB). This is only required
# for static TileDB, as its dependencies are not embedded in the static object
# (as they are for the shared object).
if (TILEDB_STATIC)
  # TILEDB_STATIC_DEP_STRING will be inserted literally into TileDBConfig.cmake.
  set(TILEDB_STATIC_DEP_STRING)
  # For get_installed_location().
  include(TileDBCommon)

  # Helper to generate CMake code
  macro(append_dep_lib LIB_TARGET)
    if (TARGET ${LIB_TARGET})
      get_installed_location(LOC ${LIB_TARGET})
      string(CONCAT TILEDB_STATIC_DEP_STRING
        "${TILEDB_STATIC_DEP_STRING}"
        "if (NOT TARGET ${LIB_TARGET})\n"
        "  add_library(${LIB_TARGET} UNKNOWN IMPORTED)\n"
        "  set_target_properties(${LIB_TARGET} PROPERTIES IMPORTED_LOCATION ${LOC})\n"
        "endif()\n"
      )
    endif()
  endmacro()

  append_dep_lib(Bzip2::Bzip2)
  append_dep_lib(Curl::Curl)
  append_dep_lib(LZ4::LZ4)
  append_dep_lib(TBB::tbb)
  append_dep_lib(Zlib::Zlib)
  append_dep_lib(Zstd::Zstd)
  append_dep_lib(Capnp::capnp)
  append_dep_lib(Capnp::capnp-json)
  append_dep_lib(Capnp::kj)
  append_dep_lib(OpenSSL::SSL)
  append_dep_lib(OpenSSL::Crypto)
  append_dep_lib(AWSSDK::aws-cpp-sdk-s3)
  append_dep_lib(AWSSDK::aws-cpp-sdk-core)

  # Spdlog is a special case because it is header-only.
  string(CONCAT TILEDB_STATIC_DEP_STRING
    "${TILEDB_STATIC_DEP_STRING}"
    "if (NOT TARGET Spdlog::Spdlog)\n"
    "  add_library(Spdlog::Spdlog INTERFACE IMPORTED)\n"
    "endif()"
  )
endif()

############################################################
# TileDB static and shared library targets
############################################################

add_library(tiledb_shared SHARED $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)

# Target properties
set_target_properties(tiledb_shared
  PROPERTIES
    OUTPUT_NAME "tiledb"
)

# Link the dependencies specified earlier
target_link_libraries(tiledb_shared
  PRIVATE
    $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS_ILIB,INTERFACE_LINK_LIBRARIES>
)

if (TILEDB_STATIC)
  add_library(tiledb_static STATIC $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)
  set_target_properties(tiledb_static
    PROPERTIES
      OUTPUT_NAME "tiledb"
  )
  target_link_libraries(tiledb_static
    PRIVATE
      $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS_ILIB,INTERFACE_LINK_LIBRARIES>
  )
  # On Windows we must name the static library something else to avoid
  # name clash with the DLL's "import library" .lib file.
  if (WIN32)
    set_target_properties(tiledb_static PROPERTIES OUTPUT_NAME "tiledbstatic")
  endif()
endif()

############################################################
# API symbol exports (and public headers for install)
############################################################

include(GenerateExportHeader)

# Generates the file 'tiledb_export.h' suitable for the current compiler.
generate_export_header(TILEDB_CORE_OBJECTS
  BASE_NAME tiledb
)

# Set variables in the parent scope so the tests and examples can reference it.
set(TILEDB_EXPORT_HEADER_NAME "tiledb_export.h" PARENT_SCOPE)
set(TILEDB_EXPORT_HEADER_DIR "${CMAKE_CURRENT_BINARY_DIR}" PARENT_SCOPE)
set(TILEDB_EXPORT_HEADER "${CMAKE_CURRENT_BINARY_DIR}/tiledb_export.h")
set(TILEDB_EXPORT_HEADER "${TILEDB_EXPORT_HEADER}" PARENT_SCOPE)

# Set related compiler settings
target_compile_definitions(TILEDB_CORE_OBJECTS PRIVATE -DTILEDB_CORE_OBJECTS_EXPORTS)
target_include_directories(TILEDB_CORE_OBJECTS PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

# Add the generated header to the public headers list
list(APPEND TILEDB_PUBLIC_HEADERS
  ${TILEDB_EXPORT_HEADER}
)

# Set the public headers, which are the ones that get installed.
set_target_properties(tiledb_shared
  PROPERTIES
    PUBLIC_HEADER "${TILEDB_PUBLIC_HEADERS}"
)
if (TILEDB_STATIC)
  set_target_properties(tiledb_static
    PROPERTIES
      PUBLIC_HEADER "${TILEDB_PUBLIC_HEADERS}"
  )
endif()

############################################################
# Installation
############################################################

# Get library directory for multiarch linux distros
include(GNUInstallDirs)
# Include module with function 'configure_package_config_file'
include(CMakePackageConfigHelpers)

# Set rpath so the TileDB dynamic dependencies can be located.
if (NOT WIN32)
  set_target_properties(tiledb_shared
    PROPERTIES
      INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}"
  )
endif()

# Override CMAKE_INSTALL_LIBDIR value from GNUInstallDirs if specified.
if (NOT TILEDB_INSTALL_LIBDIR STREQUAL "")
  set(CMAKE_INSTALL_LIBDIR "${TILEDB_INSTALL_LIBDIR}" CACHE STRING "" FORCE)
endif()

# List of targets to install.
set(TILEDB_INSTALL_TARGETS
  tiledb_shared
)
if (TILEDB_STATIC)
  list(APPEND TILEDB_INSTALL_TARGETS
    tiledb_static
    TILEDB_CORE_OBJECTS_ILIB
  )
endif()

# Set directory where TileDBConfig.cmake will be installed
set(CONFIG_INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/TileDB")
# Set name for export target file (will be installed to CONFIG_INSTALL_DIR)
set(TARGETS_EXPORT_NAME "TileDBTargets")

# Note on Windows, the DLL counts as "runtime" and should go into bin.
install(
  TARGETS ${TILEDB_INSTALL_TARGETS}
  EXPORT ${TARGETS_EXPORT_NAME}
  PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tiledb
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Path to generated cmake file
set(PROJECT_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/TileDBConfig.cmake")

# Generate 'TileDBConfig.cmake'
# This process requires these variables to be defined at this point:
#   * TARGETS_EXPORT_NAME
#   * PROJECT_NAME
#   * TILEDB_STATIC_DEP_STRING
configure_package_config_file(
  "${TILEDB_CMAKE_INPUTS_DIR}/Config.cmake.in"
  "${PROJECT_CONFIG}"
  INSTALL_DESTINATION "${CONFIG_INSTALL_DIR}"
)

# Install config file to <prefix>/lib/cmake/TileDB/TileDBConfig.cmake
install(
  FILES "${PROJECT_CONFIG}"
  DESTINATION "${CONFIG_INSTALL_DIR}"
)

# Install targets file to <prefix>/lib/cmake/TileDB/TileDBTargets.cmake
install(
  EXPORT "${TARGETS_EXPORT_NAME}"
  NAMESPACE "${PROJECT_NAME}::"
  DESTINATION "${CONFIG_INSTALL_DIR}"
)
