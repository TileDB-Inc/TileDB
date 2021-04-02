#
# tiledb/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017-2021 TileDB, Inc.
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
###########################################################

# The core header directory.
set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")

# List of API headers (to be installed)
set(TILEDB_PUBLIC_HEADERS
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb.h
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb_enum.h
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb_version.h
)

if (TILEDB_SERIALIZATION)
  list(APPEND TILEDB_PUBLIC_HEADERS
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb_serialization.h
  )
endif()

if (TILEDB_CPP_API)
  list(APPEND TILEDB_PUBLIC_HEADERS
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/tiledb
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array_schema.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/attribute.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/config.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/context.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/core_interface.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/deleter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/dimension.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/domain.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/exception.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/filter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/filter_list.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/fragment_info.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/group.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/object.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/object_iter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query_condition.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/schema_base.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/stats.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/type.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/utils.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/version.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/vfs.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/arrowio
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/arrow_io_impl.h
  )
else()
  message(STATUS "TileDB C++ API is not built.")
endif()

# List of all core source files
set(TILEDB_CORE_SOURCES
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/heap_memory.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/heap_profiler.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/logger.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/status.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/thread_pool.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array/array.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/array_schema.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/attribute.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/dimension.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/domain.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/buffer_list.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/const_buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/preallocated_buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cache/buffer_lru_cache.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/bzip_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/dd_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/gzip_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/lz4_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/rle_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/zstd_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/config/config.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/config/config_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/encryption_key.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/encryption_key_validation.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto_openssl.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto_win32.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/azure.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/gcs.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/mem_filesystem.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/hdfs_filesystem.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/posix.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3_thread_pool_executor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/vfs.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/vfs_file_handle.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/win.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/bit_width_reduction_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/bitshuffle_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/byteshuffle_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/checksum_md5_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/checksum_sha256_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/compression_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/encryption_aes256gcm_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter_buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter_pipeline.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter_storage.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/noop_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/positive_delta_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/fragment_info.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/fragment_metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/global_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/libcurl_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/openssl_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/signal_handlers.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/tbb_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/watchdog.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/metadata/metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/cancelable_tasks.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/constants.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/uri.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/utils.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/uuid.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/win_constants.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/work_arounds.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/query.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/query_condition.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/reader.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/result_tile.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/read_cell_slab_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/writer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/write_cell_slab_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_client.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rtree/rtree.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/stats/stats.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/stats/timer_stat.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/context.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/open_array.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/storage_manager.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/cell_slab_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/subarray.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/subarray_partitioner.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/chunked_buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/tile.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/generic_tile_io.cc
)

if (TILEDB_SERIALIZATION)
  list(APPEND TILEDB_CORE_SOURCES
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/curl.cc
  )

  #NOTE: These properties are being set possibly *before* tiledb-rest.capnp.c++ has been generated.
  if(NOT WIN32)
    if($false)  #section will be needed when mov *n*x to v0.8.0
    #capnproto v0.8.0 requires at least c++14, msvc2017 seems ok, gcc 7.5 is not by default
    set_source_files_properties(
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_client.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp.c++
      PROPERTIES
      #eventually, not yet in cmake 3.3, added in 3.11 - COMPILE_OPTIONS "-std=c++14"
      #   C4267: 'argument': conversion from 'size_t' to '<various-lesser-types>', possible loss of data
      #   C4100 '<some-name>' unreferenced format parameter
        COMPILE_FLAGS "-std=c++14"
    )
    endif()
  endif()
  if(MSVC)
    set_source_files_properties(
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_client.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp.c++
      PROPERTIES
      #eventually, not yet in cmake 3.3, added in 3.11 - COMPILE_OPTIONS "/wd4267 /wd4100"
      #   C4267: 'argument': conversion from 'size_t' to '<various-lesser-types>', possible loss of data
      #   C4100 '<some-name>' unreferenced format parameter
      #vs2017/19 std:c++14 or greater by default
      #vs2015, is not so specify it (std:c++14, std::c++latest not avail vs2015 til *update 3*)
      #TBD: would std:c++latest be better choice?
        COMPILE_FLAGS "/wd4267 /wd4100 /std:c++14"
    )
  endif()

endif()

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
	# to pickup <build_dir>/tiledb for capnp gen'd files
    "${CMAKE_CURRENT_BINARY_DIR}/.." 
)

# Build a separate copy of the object library for use with static TileDB
# on Windows. See https://github.com/TileDB-Inc/TileDB/issues/673
if (WIN32 AND TILEDB_STATIC)
  add_library(TILEDB_CORE_OBJECTS_STATIC OBJECT
    ${TILEDB_CORE_SOURCES}
    ${TILEDB_EXTERNALS_SOURCES}
  )

  # Compile all core sources with PIC
  set_property(TARGET TILEDB_CORE_OBJECTS_STATIC PROPERTY POSITION_INDEPENDENT_CODE ON)

  target_compile_definitions(TILEDB_CORE_OBJECTS_STATIC
    PRIVATE
      -DTILEDB_STATIC_DEFINE
  )
endif()

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
  string(TOLOWER ${SANITIZER} SANITIZER)
  if (NOT SANITIZER MATCHES "^(address|memory|leak|thread|undefined)$")
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

if (TILEDB_STATS)
  add_definitions(-DTILEDB_STATS)
  message(STATUS "The TileDB library is compiled with stats enabled.")
endif()

if (TILEDB_SERIALIZATION)
  add_definitions(-DTILEDB_SERIALIZATION)
  message(STATUS "The TileDB library is compiled with query serialization enabled.")

  if(MSVC)
    if (TILEDB_CURL_EP_BUILT)
      target_compile_definitions(TILEDB_CORE_OBJECTS PRIVATE -DCURL_STATICLIB=1)
    else()
      target_compile_definitions(TILEDB_CORE_OBJECTS PRIVATE                   )
    endif()
  endif()
endif()

############################################################
# Dependencies: set up includes/linking
############################################################

# Unfortunately, with CMake < 3.12, you can't use target_link_libraries()
# on TILEDB_CORE_OBJECTS. This workaround uses an interface library
# so we can use the targets created by the calls to find_package().
add_library(TILEDB_CORE_OBJECTS_ILIB INTERFACE)

# Find OpenSSL first in case it's needed for S3 or Azure
if (NOT WIN32)
  find_package(OpenSSL_EP REQUIRED)
endif()

# S3 dependencies
if (TILEDB_S3)
  message(STATUS "The TileDB library is compiled with S3 support.")
  find_package(AWSSDK_EP REQUIRED COMPONENTS s3)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      AWSSDK::aws-cpp-sdk-s3
      AWSSDK::aws-cpp-sdk-core
      AWSSDK::aws-c-event-stream
      AWSSDK::aws-checksums
      AWSSDK::aws-c-common
      AWSSDK::aws-cpp-sdk-identity-management
      AWSSDK::aws-cpp-sdk-sts)
  add_definitions(-DHAVE_S3)
endif()

# Azure dependencies
if (TILEDB_AZURE)
  message(STATUS "The TileDB library is compiled with Azure support.")
  find_package(AzureSDK_EP REQUIRED COMPONENTS azure)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      AzureSDK::AzureSDK
  )
  add_definitions(-DHAVE_AZURE)

  if(MSVC)
    if (TILEDB_CURL_EP_BUILT)
      target_compile_definitions(TILEDB_CORE_OBJECTS PRIVATE -DCURL_STATICLIB=1 -Dazure_storage_lite_EXPORTS)
    else()
      target_compile_definitions(TILEDB_CORE_OBJECTS PRIVATE                    -Dazure_storage_lite_EXPORTS)
    endif()
  endif()
endif()

# GCS dependencies
if (TILEDB_GCS)
  message(STATUS "The TileDB library is compiled with GCS support.")
  find_package(GCSSDK_EP REQUIRED COMPONENTS gcs)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
       storage_client
  )
  add_definitions(-DHAVE_GCS)

  # work around for GCS finding the shared version of zlib: we will see the Zlib::Zlib
  # transitive linkage later, and we can use the correct version there.
  unset(ZLIB_FOUND)
  unset(ZLIB_LIBRARIES)
  unset(ZLIB_INCLUDE_DIR)

  get_target_property(_GCS_INTERFACE_LIBS storage_client INTERFACE_LINK_LIBRARIES)
  if ("ZLIB::ZLIB" IN_LIST _GCS_INTERFACE_LIBS)
    list(REMOVE_ITEM _GCS_INTERFACE_LIBS "ZLIB::ZLIB")
    set_property(TARGET storage_client PROPERTY INTERFACE_LINK_LIBRARIES "${_GCS_INTERFACE_LIBS}")
  endif()
endif()

# Libcurl
if (TILEDB_S3 OR TILEDB_AZURE OR TILEDB_GCS OR TILEDB_SERIALIZATION)
  if (NOT WIN32)
    find_package(Curl_EP REQUIRED)
    target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
      INTERFACE
        CURL::libcurl
    )
  elseif (WIN32 AND (TILEDB_AZURE OR TILEDB_SERIALIZATION))
    find_package(Curl_EP REQUIRED)
    target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
      INTERFACE
        CURL::libcurl
    )
  endif()
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
  add_definitions(-DHAVE_TBB -DTBB_SUPPRESS_DEPRECATED_MESSAGES)
endif()

# Serialization
if (TILEDB_SERIALIZATION)
  find_package(Capnp_EP REQUIRED)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      CapnProto::capnp
      CapnProto::capnp-json
      CapnProto::kj
  )
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

# Required dependencies. These come last as any prior dependencies relying on
# these must be listed first for Linux. E.g. AWSSDK and Curl depend on Zlib,
# which is installed here.
find_package(Bzip2_EP REQUIRED)
find_package(LZ4_EP REQUIRED)
find_package(Spdlog_EP REQUIRED)
find_package(Zlib_EP REQUIRED)
find_package(Zstd_EP REQUIRED)
target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
  INTERFACE
    Bzip2::Bzip2
    LZ4::LZ4
    Spdlog::Spdlog
    Zlib::Zlib
    Zstd::Zstd
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
  if(TILEDB_AZURE OR TILEDB_SERIALIZATION)
    #ws2_32.lib, crypt32.lib needed to satisfy curl dependencies
    set(WIN32_LIBS ${WIN32_LIBS} ws2_32 crypt32)
  endif()

  if (TILEDB_S3)
    list(APPEND WIN32_LIBS "${AWS_EXTRA_LIBS}")
  endif()

  foreach (LIB ${WIN32_LIBS})
    if(MSVC)
      find_library(LIB_${LIB} ${LIB})
      message(STATUS "Found Win32 lib ${LIB}: ${LIB_${LIB}}")
      target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE ${LIB_${LIB}})
    else()
      message(STATUS "Linking to Win32 lib ${LIB}")
      target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE -l${LIB})
    endif()
  endforeach()
endif()

# macOS specific libraries
if(APPLE)
  if(TILEDB_S3)
    # this is a transitive dependency for statically linking S3 SDK
    target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE "-framework CoreFoundation")
  endif()
endif()

# On Linux, must explicitly link -lpthread -ldl in order for static linking
# to libzstd and tbb.
if (NOT WIN32)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE pthread dl)
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
# Append Curl linking information
############################################################

# Find curl link dependencies.
# This needs to be done after curl is built because we use
# curl-config to query the dependency libraries and then add
# those to the target. Ref:
#   - https://github.com/TileDB-Inc/TileDB/issues/1080
#   - https://github.com/TileDB-Inc/TileDB/pull/1253
# NOTE: just like the static dependencies in TileDBConfig create above
#       (via TILEDB_DEP_STRING), this linkage embeds absolute paths.
if ((TILEDB_S3 OR TILEDB_AZURE OR TILEDB_GCS OR TILEDB_SERIALIZATION) AND NOT WIN32)
  if (TILEDB_CURL_EP_BUILT)
    set(CURL_CONFIG_BINARY "${TILEDB_EP_BASE}/install/bin/curl-config")

      if (NOT EXISTS "${CURL_CONFIG_BINARY}")
        message(WARNING "Missing '${CURL_CONFIG_BINARY}': TileDB targets may have linker errors!")
      else()
        message(STATUS "Found curl-config: '${CURL_CONFIG_BINARY}'")

        if(${CURL_LIBRARIES} MATCHES "${CMAKE_SHARED_LIBRARY_SUFFIX}$")
          execute_process(
                  COMMAND ${CURL_CONFIG_BINARY} --libs
                  OUTPUT_VARIABLE CURL_APPEND_LIBS
                  RESULT_VARIABLE CMD_RESULT
                  OUTPUT_STRIP_TRAILING_WHITESPACE
          )

        else()
          execute_process(
            COMMAND ${CURL_CONFIG_BINARY} --static-libs
            OUTPUT_VARIABLE CURL_APPEND_LIBS
            RESULT_VARIABLE CMD_RESULT
            OUTPUT_STRIP_TRAILING_WHITESPACE
          )
        endif()

        if (NOT CMD_RESULT EQUAL 0)
          message(WARNING
            "TileDB::tiledb_static link target may lack transitive link dependencies"
            "'${CURL_CONFIG_BINARY} --static-libs' failed with error: "
            "${CMD_RESULT}")
        elseif (CURL_APPEND_LIBS)
          # ^ must check output, because it might be empty and break regex below
          # Make a list. The variable passed to target_link_libraries *must* be a list.
          string(REGEX REPLACE "[ \t\r\n]" ";" CURL_APPEND_LIBS ${CURL_APPEND_LIBS})

          message(STATUS "Computed initial transitive Curl library links to TileDB targets: '${CURL_APPEND_LIBS}'")
        endif()
      endif()
  endif()

endif()

if (CURL_APPEND_LIBS)
  # OpenSSL is always linked on POSIX for encryption. If we include curl's
  # linkage we might double link if we built openssl in superbuild
  # but the system library exists. This happens on macOS 10.14 and 10.15
  # where openssl is deprecated but the library still ships. OpenSSL headers
  # are not shipped, thus we build a superbuild of openssl. The -lcrypto then
  # ends up trying to link against the system shared library while we already
  # linked the static lib from TileDB supper build.
  # End result, we remove the -lssl and -lcrypto from the list of curl extra libs
  # The same problem exists for libz, so we remove -lz also.
  list(REMOVE_ITEM CURL_APPEND_LIBS "-lssl" "-lcrypto" "-lz")
  message(STATUS "Adding final transitive Curl library links to TileDB targets: '${CURL_APPEND_LIBS}'")
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
    ${CURL_APPEND_LIBS})
endif()

############################################################
# capnproto file generation
############################################################

if(TILEDB_SERIALIZATION)
  if(TILEDB_CAPNP_EP_BUILT)
    set(TILEDB_CAPNPROTOEXEPATH "${TILEDB_CAPNPEXEC_PATH}")
  else()
    find_package(CapnProto 0.6.1 #*not* EXACT since windows/*n*x using 0.8.0, 0.6.1 respectively
      PATHS  ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )
    set(TILEDB_CAPNPROTOEXEPATH "${CAPNP_EXECUTABLE}")
  endif()

  #make files 'known' via add_custom_command() so ("tiledb-rest.capnp.c++") can be added to TILEDB_CORE_OBJECTS (further below)
  if(WIN32)
    add_custom_command(
	  DEPENDS ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp
      OUTPUT #BYPRODUCTS
        #i.e. <builddir>/tiledb/tiledb/sm/serialization/...
        ${CMAKE_CURRENT_BINARY_DIR}/sm/serialization/tiledb-rest.capnp.c++
        ${CMAKE_CURRENT_BINARY_DIR}/sm/serialization/tiledb-rest.capnp.h
      COMMAND
        powershell "${CMAKE_CURRENT_SOURCE_DIR}/../scripts/capnpupdate.ps1 ${CMAKE_CURRENT_BINARY_DIR}/sm/serialization ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization"
      WORKING_DIRECTORY
        ${TILEDB_EP_BASE}/..

      COMMENT "(re-)generate tiledb-rest.capnp.<h,c++>"
    )
  else()
    add_custom_command(
	  DEPENDS ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp
      OUTPUT #BYPRODUCTS
        ${CMAKE_CURRENT_BINARY_DIR}/sm/serialization/tiledb-rest.capnp.c++
        ${CMAKE_CURRENT_BINARY_DIR}/sm/serialization/tiledb-rest.capnp.h
      COMMAND
        bash -c "${CMAKE_CURRENT_SOURCE_DIR}/../scripts/capnpupdate.sh ${CMAKE_CURRENT_BINARY_DIR}/sm/serialization ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization"
      WORKING_DIRECTORY
        ${TILEDB_EP_BASE}/..
      COMMENT "(re-)generate tiledb-rest.capnp.<h,c++>"
    )
  endif()
  
  set_source_files_properties(
    "${CMAKE_CURRENT_BINARY_DIR}/sm/serialization/tiledb-rest.capnp.c++"
    "${CMAKE_CURRENT_BINARY_DIR}/sm/serialization/tiledb-rest.capnp.h"
    PROPERTIES GENERATED TRUE
    )
	
  set_property(SOURCE 
    ${CMAKE_CURRENT_BINARY_DIR}/sm/serialization/tiledb-rest.capnp.c++
	APPEND PROPERTY OBJECT_DEPENDS ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp
	)
  set_property(SOURCE 
    ${CMAKE_CURRENT_BINARY_DIR}/sm/serialization/tiledb-rest.capnp.h
	APPEND PROPERTY OBJECT_DEPENDS ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp
	)

  target_sources(TILEDB_CORE_OBJECTS PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}/sm/serialization/tiledb-rest.capnp.c++
  )

endif()


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
  macro(append_dep_lib APPEND_LIB)
    if (TARGET ${APPEND_LIB})
      get_installed_location(TARGET_LOC ${APPEND_LIB})
      get_target_property(TARGET_LIBS ${APPEND_LIB} INTERFACE_LINK_LIBRARIES)

      string(CONCAT TILEDB_STATIC_DEP_STRING
        "${TILEDB_STATIC_DEP_STRING}"
        "if (NOT TARGET ${APPEND_LIB})\n"
        "  add_library(${APPEND_LIB} UNKNOWN IMPORTED)\n"
        "  set_target_properties(${APPEND_LIB} PROPERTIES IMPORTED_LOCATION ${TARGET_LOC})\n"
      )

      string(CONCAT TILEDB_STATIC_DEP_STRING
        "${TILEDB_STATIC_DEP_STRING}"
        "endif()\n"
      )

    endif()
  endmacro()

  append_dep_lib(Bzip2::Bzip2)
  append_dep_lib(LZ4::LZ4)
  append_dep_lib(TBB::tbb)
  append_dep_lib(Zlib::Zlib)
  append_dep_lib(ZLIB::ZLIB)
  append_dep_lib(Zstd::Zstd)
  append_dep_lib(CapnProto::capnp)
  append_dep_lib(CapnProto::capnp-json)
  append_dep_lib(CapnProto::kj)
  append_dep_lib(OpenSSL::SSL)
  append_dep_lib(OpenSSL::Crypto)
  append_dep_lib(CURL::libcurl)
  append_dep_lib(AWSSDK::aws-cpp-sdk-s3)
  append_dep_lib(AWSSDK::aws-cpp-sdk-core)
  append_dep_lib(AWSSDK::aws-c-event-stream)
  append_dep_lib(AWSSDK::aws-checksums)
  append_dep_lib(AWSSDK::aws-c-common)
  append_dep_lib(AWSSDK::aws-cpp-sdk-sts)
  append_dep_lib(AWSSDK::aws-cpp-sdk-identity-management)
  append_dep_lib(AzureSDK::AzureSDK)
  append_dep_lib(GCSSDK::storage_client)
  append_dep_lib(GCSSDK::google_cloud_cpp_common)
  append_dep_lib(GCSSDK::crc32c)

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

if (TILEDB_VERSION AND CMAKE_SYSTEM_NAME MATCHES "Linux")
  set_target_properties(tiledb_shared
    PROPERTIES
      SOVERSION "${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}")
endif()

# Link the dependencies specified earlier
target_link_libraries(tiledb_shared
  PRIVATE
    $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS_ILIB,INTERFACE_LINK_LIBRARIES>
)

if (TILEDB_STATIC)
  if (WIN32)
    # Copy over all include directories, compile options, etc, from the regular
    # core objects.
    target_compile_definitions(TILEDB_CORE_OBJECTS_STATIC
      PRIVATE
        $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS,COMPILE_DEFINITIONS>
    )
    target_compile_options(TILEDB_CORE_OBJECTS_STATIC
      PRIVATE
        $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS,COMPILE_OPTIONS>
    )
    target_include_directories(TILEDB_CORE_OBJECTS_STATIC
      PRIVATE
        $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS,INCLUDE_DIRECTORIES>
    )
    # Create the target
    add_library(tiledb_static STATIC
      $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS_STATIC>
    )
    target_compile_definitions(tiledb_static
      INTERFACE
        -DTILEDB_STATIC_DEFINE
    )
    # On Windows we must name the static library something else to avoid
    # name clash with the DLL's "import library" .lib file.
    set_target_properties(tiledb_static
      PROPERTIES
        OUTPUT_NAME "tiledbstatic"
    )
  else()
    add_library(tiledb_static STATIC $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)
    set_target_properties(tiledb_static
      PROPERTIES
        OUTPUT_NAME "tiledb"
    )
  endif()

  target_link_libraries(tiledb_static
    PRIVATE
      $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS_ILIB,INTERFACE_LINK_LIBRARIES>
  )
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

# Don't re-export symbols from static (archive) libraries
#   Prevents conflicts with other versions of (e.g.) OpenSSL
#   loaded in the same process namespace, which can cause
#   crashes if the versions are not compatible.
if (CMAKE_SYSTEM_NAME MATCHES "Linux")
  set_target_properties(tiledb_shared
    PROPERTIES
      LINK_FLAGS "-Wl,--exclude-libs=ALL")
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

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/sm/c_api/tiledb_version.h" ver)

string(REGEX MATCH "TILEDB_VERSION_MAJOR ([0-9]*)" _ ${ver})
set(ver_major ${CMAKE_MATCH_1})

string(REGEX MATCH "TILEDB_VERSION_MINOR ([0-9]*)" _ ${ver})
set(ver_minor ${CMAKE_MATCH_1})

string(REGEX MATCH "TILEDB_VERSION_PATCH ([0-9]*)" _ ${ver})
set(ver_patch ${CMAKE_MATCH_1})

set(VERSION "${ver_major}.${ver_minor}.${ver_patch}")

# PKG Config file
configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/inputs/tiledb.pc.in
        ${CMAKE_CURRENT_BINARY_DIR}/tiledb.pc
        @ONLY
)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/tiledb.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
