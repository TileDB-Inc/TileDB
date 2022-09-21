#
# tiledb/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017-2022 TileDB, Inc.
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
# Find packages needed for object libraries
############################################################

# FindThreads defines the following:
#  * Threads_FOUND - If a supported thread library was found.
#  * CMAKE_THREAD_LIBS_INIT - The thread library to use. This is empty if the thread
#    functions are provided by the system libraries and no special flags are needed to
#    use them.
#  * CMAKE_USE_WIN32_THREADS_INIT - If the found thread library is the win32 one.
#  * CMAKE_USE_PTHREADS_INIT - If the found thread library is pthread compatible.
#  * CMAKE_HP_PTHREADS_INIT - If the found thread library is the HP thread library.
find_package(Threads REQUIRED)

############################################################
# Subdirectories
############################################################

add_subdirectory(api)
add_subdirectory(common)
add_subdirectory(type)
add_subdirectory(sm)
add_subdirectory(storage_format)

############################################################
# Header files
############################################################
# The headers in this section are those to be installed in the base include
# directory, not in subdirectories of the base. The other include files are
# specified in the installation section

# The core header directory.
set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")

# List of API headers (to be installed)
set(TILEDB_PUBLIC_HEADERS ${TILEDB_C_API_FILENAME_HEADERS})

if (TILEDB_SERIALIZATION)
  list(APPEND TILEDB_PUBLIC_HEADERS
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb_serialization.h
  )
endif()

if (TILEDB_EXPERIMENTAL_FEATURES)
  list(APPEND TILEDB_PUBLIC_HEADERS
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/experimental/tiledb_dimension_label.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/experimental/tiledb_enum_experimental.h
  )
endif()

if (TILEDB_CPP_API)
  list(APPEND TILEDB_PUBLIC_HEADERS
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/tiledb
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/tiledb_experimental
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array_schema.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array_schema_evolution.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/attribute.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/config.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/context.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/core_interface.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/deleter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/dimension.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/domain.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/error.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/exception.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/filter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/filter_list.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/fragment_info.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/group.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/group_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/object.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/object_iter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query_condition.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/schema_base.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/stats.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/subarray.h
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

############################################################
# Source files
############################################################

# List of all core source files
set(TILEDB_CORE_SOURCES
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/heap_memory.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/heap_profiler.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/logger.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/memory.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/stdx_string.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/interval/interval.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/types/dynamic_typed_datum.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array/array.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array/array_directory.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array/consistency.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/array_schema.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/array_schema_evolution.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/attribute.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/dimension.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/dimension_label_schema.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/dimension_label_reference.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/domain.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/dimension_label/dimension_label.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/buffer_list.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/api_argument_validator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb_filestore.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb_group.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cache/buffer_lru_cache.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/bzip_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/dd_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/dict_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/gzip_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/lz4_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/rle_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/util/gzip_wrappers.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/zstd_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/config/config.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/config/config_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/array_meta_consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/commits_consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/fragment_consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/fragment_meta_consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/group_meta_consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/encryption_key.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/encryption_key_validation.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto_openssl.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto_win32.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/azure.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/gcs.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/mem_filesystem.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/hdfs_filesystem.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/path_win.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/posix.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3_thread_pool_executor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/uri.cc
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
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter_create.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter_pipeline.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/filter_storage.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/float_scaling_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/xor_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/noop_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/positive_delta_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/fragment_info.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/fragment_metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/global_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/libcurl_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/signal_handlers.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/watchdog.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_directory.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_member.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_member_v1.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_v1.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/metadata/metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/cancelable_tasks.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/constants.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/mgc_dict.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/parse_argument.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/tdb_math.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/tdb_time.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/types.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/utils.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/uuid.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/win_constants.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/work_arounds.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/ast/query_ast.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/deletes_and_updates/deletes.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/deletes_and_updates/serialization.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/dimension_label/array_dimension_label_queries.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/dimension_label/dimension_label_query.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/hilbert_order.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/legacy/cell_slab_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/legacy/reader.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/legacy/read_cell_slab_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/query.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/query_condition.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/dense_reader.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/reader_base.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/result_tile.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/sparse_global_order_reader.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/sparse_index_reader_base.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/sparse_unordered_with_dups_reader.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/strategy_base.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/update_value.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/writers/dense_tiler.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/writers/global_order_writer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/writers/ordered_writer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/writers/unordered_writer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/writers/writer_base.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_client.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rtree/rtree.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema_evolution.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/config.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/group.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/stats/global_stats.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/stats/stats.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/stats/timer_stat.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/context.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/storage_manager.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/range_subset.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/subarray.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/subarray_partitioner.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/subarray_tile_overlap.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/tile_cell_slab_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/tile.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/generic_tile_io.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/tile_metadata_generator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/writer_tile.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/storage_format/uri/generate_uri.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/storage_format/uri/parse_uri.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/type/range/range.cc
)
list(APPEND TILEDB_CORE_SOURCES ${TILEDB_COMMON_SOURCES})
list(APPEND TILEDB_CORE_SOURCES ${TILEDB_API_SOURCES})

if (TILEDB_EXPERIMENTAL_FEATURES)
  list(APPEND TILEDB_CORE_SOURCES
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/experimental/tiledb_dimension_label.cc)
endif()


#openssl3 md5 deprecation mitigation
if(MSVC)
  set_source_files_properties(${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto_openssl.cc PROPERTIES COMPILE_OPTIONS "/wd4996")
else()
  set_source_files_properties(${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto_openssl.cc PROPERTIES COMPILE_OPTIONS "-Wno-deprecated-declarations")
endif()

if (TILEDB_SERIALIZATION)
  list(APPEND TILEDB_CORE_SOURCES
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/curl.cc
  )

  if(NOT WIN32)
    set_source_files_properties(
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_client.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema_evolution.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/config.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/group.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp.c++
      PROPERTIES
        COMPILE_FLAGS "-Wno-unused-parameter"
    )
  endif()
  if(MSVC)
    set_source_files_properties(
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_client.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema_evolution.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/config.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/group.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp.c++
      PROPERTIES
        #   C4267: 'argument': conversion from 'size_t' to '<various-lesser-types>', possible loss of data
        #   C4100 '<some-name>' unreferenced format parameter
        COMPILE_FLAGS "/wd4267 /wd4100"
    )
  endif()

endif()

# 'External' source files included in the source tree.
list(APPEND TILEDB_EXTERNALS_INCLUDE_DIRS
  "${CMAKE_CURRENT_SOURCE_DIR}/../external/include"
  "${CMAKE_CURRENT_SOURCE_DIR}/../external/include/bitshuffle"
  "${CMAKE_CURRENT_SOURCE_DIR}/../external/include/blosc"
)
list(APPEND TILEDB_EXTERNALS_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/src/md5/md5.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/src/bitshuffle/iochain.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/src/bitshuffle/bitshuffle_core.cc
)

#
# Delegate configuration and detection of platform-specific code generation to
# the subdirectory. For why the separation isn't clean, see comments there.
#
add_subdirectory(
  ${CMAKE_CURRENT_SOURCE_DIR}/../external/blosc
  ${CMAKE_CURRENT_BINARY_DIR}/../external/blosc)
list(APPEND TILEDB_EXTERNALS_INCLUDE_DIRS ${TileDB_blosc_INCLUDE_DIRS})
list(APPEND TILEDB_EXTERNALS_SOURCES ${TileDB_blosc_SOURCES})
set_source_files_properties(
  ${TileDB_blosc_SOURCES}
  PROPERTIES COMPILE_OPTIONS "${TileDB_blosc_COMPILE_OPTIONS}")

############################################################
# Build core objects as a reusable object library
############################################################

add_library(TILEDB_CORE_OBJECTS OBJECT
    ${TILEDB_CORE_SOURCES}
    ${TILEDB_EXTERNALS_SOURCES}
)

# Use BUILD_INTERFACE generator to work around
#   https://gitlab.kitware.com/cmake/cmake/-/issues/20736
target_link_libraries(TILEDB_CORE_OBJECTS PRIVATE $<BUILD_INTERFACE:common>)

############################################################
# provide actions/target for preparation of magic.mgc data for embedding/build

set(MGC_GZIPPED_BIN_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/..")
set(MGC_GZIPPED_BIN_OUTPUT_FILE "${MGC_GZIPPED_BIN_OUTPUT_DIRECTORY}/magic_mgc_gzipped.bin")
set(MGC_GZIPPED_BIN_INPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/sm/misc")
set(MGC_GZIPPED_BIN_INPUT_FILE "${MGC_GZIPPED_BIN_INPUT_DIRECTORY}/magic_mgc_gzipped.bin.tar.bz2")

add_custom_command(
  OUTPUT "${MGC_GZIPPED_BIN_OUTPUT_FILE}"
  DEPENDS "${MGC_GZIPPED_BIN_INPUT_FILE}"
  COMMAND ${CMAKE_COMMAND} -E tar x "${MGC_GZIPPED_BIN_INPUT_FILE}"
  WORKING_DIRECTORY "${MGC_GZIPPED_BIN_OUTPUT_DIRECTORY}"
)
add_custom_target(gen_mgc_unarch ALL
  DEPENDS ${MGC_GZIPPED_BIN_OUTPUT_FILE}
)
add_dependencies(TILEDB_CORE_OBJECTS gen_mgc_unarch)

add_custom_target(
  update-embedded-magic-data
  COMMAND "$<TARGET_FILE:tdb_gzip_embedded_data>" < "${libmagic_DICTIONARY}" "${MGC_GZIPPED_BIN_OUTPUT_FILE}"
  # need to work in 'local' directory with no prefix paths so no paths are included in archive
  WORKING_DIRECTORY "${MGC_GZIPPED_BIN_OUTPUT_DIRECTORY}"
  COMMAND ${CMAKE_COMMAND} -E tar cvj "magic_mgc_gzipped.bin.tar.bz2" "magic_mgc_gzipped.bin"
  COMMAND ${CMAKE_COMMAND} -E copy "magic_mgc_gzipped.bin.tar.bz2" "${MGC_GZIPPED_BIN_INPUT_FILE}"
  DEPENDS "${libmagic_DICTIONARY}"
  COMMENT "Re-generate ${MGC_GZIPPED_BIN_INPUT_FILE} for embedded magic.mgc support"
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

# blosc link dependencies
target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE ${TileDB_blosc_LINK_OPTIONS})

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
find_package(Magic_EP REQUIRED)
target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
  INTERFACE
    Bzip2::Bzip2
    LZ4::LZ4
    spdlog::spdlog
    Zlib::Zlib
    Zstd::Zstd
    libmagic
)

if(TILEDB_CRC32)
  find_package(crc32c_EP REQUIRED)
  find_package(crc32c CONFIG REQUIRED
    HINTS
      ${TILEDB_EP_INSTALL_PREFIX}/lib/cmake
    )

  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
    Crc32c::crc32c
  )
endif()

if(TILEDB_WEBP)
  find_package(Webp_EP REQUIRED)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
    WebP::webp
  )
endif()

if(TILEDB_ABSEIL)
  find_package(absl_EP REQUIRED)

  include(${CMAKE_SOURCE_DIR}/test/external/src/absl_library_targets.cmake)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
    ${TILEDB_ABSL_LIBRARY_TARGETS}
  )
endif()

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

# Link to Threads::Threads if library or flag needed to enable threading.
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE Threads::Threads)
endif()

# On Linux, must explicitly link -ldl for static linking to libzstd.
if (NOT WIN32)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE dl)
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

  # If we are cross compiling curl likely includes the arch details in the linker
  # commands. In this case we want to remove them from the link libs as cmake
  # will handle any linker flags for cross compilation.
  SET(index 0)
  foreach(CURL_APPEND_LIB ${CURL_APPEND_LIBS})
    if(${CURL_APPEND_LIB} STREQUAL "-arch")
      # remove "-arch"
      LIST(REMOVE_AT CURL_APPEND_LIBS ${index})
      # remove arch i.e. arm64 or x86_64
      # there are two items because we split on space above
      LIST(REMOVE_AT CURL_APPEND_LIBS ${index})
    endif()
    MATH(EXPR index "${index}+1")
  endforeach()

  message(STATUS "Adding final transitive Curl library links to TileDB targets: '${CURL_APPEND_LIBS}'")
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
    ${CURL_APPEND_LIBS})
endif()

############################################################
# capnproto generated sources
############################################################

# Serialization
if(TILEDB_SERIALIZATION)
  find_package(Capnp_EP REQUIRED)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
          INTERFACE
          CapnProto::capnp
          CapnProto::capnp-json
          CapnProto::kj
          )

  if (WIN32)
    set(TILEDB_SERIALIZATION_GENERATED_SUBDIR win32)
  else()
    set(TILEDB_SERIALIZATION_GENERATED_SUBDIR posix)
  endif()

  target_sources(TILEDB_CORE_OBJECTS PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization/${TILEDB_SERIALIZATION_GENERATED_SUBDIR}/tiledb-rest.capnp.c++
  )
  if (WIN32 AND TILEDB_STATIC)
    target_sources(TILEDB_CORE_OBJECTS_STATIC PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization/${TILEDB_SERIALIZATION_GENERATED_SUBDIR}/tiledb-rest.capnp.c++
    )
  endif()

  ###################################
  # capnproto file generation target
  ###################################

  # This target regenerates the checked-in generated source files for serialization
  # support. It must be re-run whenever `tiledb-rest.capnp` is updated.

  set(TILEDB_SERIALIZATION_GENERATED_DIR
      ${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization/${TILEDB_SERIALIZATION_GENERATED_SUBDIR})

  # We only need to override the include path and PATH env for EP builds
  if(TILEDB_CAPNP_EP_BUILT)
    list(APPEND CAPNP_COMPILE_COMMAND "${CMAKE_COMMAND}" -E env PATH="${TILEDB_EP_BASE}/install/bin" "${CAPNP_EXECUTABLE}" compile -I "${TILEDB_EP_BASE}/install/include" -oc++:"${TILEDB_SERIALIZATION_GENERATED_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization/tiledb-rest.capnp" --src-prefix="${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization")
  else()
    list(APPEND CAPNP_COMPILE_COMMAND "${CAPNP_EXECUTABLE}" compile -oc++:"${TILEDB_SERIALIZATION_GENERATED_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization/tiledb-rest.capnp" --src-prefix "${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization")
  endif()

  # note: run the compiler under `cmake -E env ...` because we need to run capnp executable
  #       the capnp driver needs to be able to find the plugin executables (eg capnpc-c++)
  add_custom_target(
    update-serialization
      ${CMAKE_COMMAND} -E echo "CAPNP_COMPILE_COMMAND: '${CAPNP_COMPILE_COMMAND}'"
    COMMAND
      "${CAPNP_COMPILE_COMMAND}"
    DEPENDS ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp
    WORKING_DIRECTORY
      ${TILEDB_EP_BASE}/..
    COMMAND_EXPAND_LISTS
    COMMENT "Re-generate tiledb-rest.capnp.<h,c++> files for serialization support"
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
      cmake_path(GET TARGET_LOC FILENAME TARGET_LOC_FILENAME)
      get_target_property(TARGET_LIBS ${APPEND_LIB} INTERFACE_LINK_LIBRARIES)

      string(CONCAT TILEDB_STATIC_DEP_STRING
        "${TILEDB_STATIC_DEP_STRING}"
        "if (NOT TARGET ${APPEND_LIB})\n"
        "  add_library(${APPEND_LIB} UNKNOWN IMPORTED)\n"
        "  set_target_properties(${APPEND_LIB} PROPERTIES IMPORTED_LOCATION \${PACKAGE_PREFIX_DIR}/lib/${TARGET_LOC_FILENAME})\n"
      )

      string(CONCAT TILEDB_STATIC_DEP_STRING
        "${TILEDB_STATIC_DEP_STRING}"
        "endif()\n"
      )

    endif()
  endmacro()

  append_dep_lib(Bzip2::Bzip2)
  append_dep_lib(LZ4::LZ4)
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
  append_dep_lib(spdlog::spdlog)
  append_dep_lib(libmagic)
  append_dep_lib(Crc32c::crc32c)
  append_dep_lib(WebP::webp)
  if(TILEDB_ABSEIL)
    foreach(tgt ${TILEDB_ABSL_LIBRARIES})
      append_dep_lib(${tgt})
    endforeach()
  endif()
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
# Generation of the header (still) happens here, but `TILEDB_EXPORT_*` symbols
# are defined in the parent directory.

# Generates the file 'tiledb_export.h' suitable for the current compiler.
include(GenerateExportHeader)
generate_export_header(TILEDB_CORE_OBJECTS
  BASE_NAME tiledb
)
# Install locally
configure_file(${TILEDB_EXPORT_HEADER} ${TILEDB_LOCALINSTALL_INCLUDE}/${TILEDB_EXPORT_HEADER_LOCALINSTALL_PATH} COPYONLY)

target_compile_definitions(TILEDB_CORE_OBJECTS PRIVATE -DTILEDB_CORE_OBJECTS_EXPORTS)
target_include_directories(TILEDB_CORE_OBJECTS PRIVATE ${TILEDB_EXPORT_HEADER_DIR})

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
  PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/tiledb"
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

# Install each of the RELATIVE headers to the subdirectory it belongs in
# TODO: Change to use `FILE_SET` after upgrade to CMake 3.23
foreach(HEADER ${TILEDB_C_API_RELATIVE_HEADERS})
  cmake_path(RELATIVE_PATH HEADER
      BASE_DIRECTORY ${TILEDB_C_API_RELATIVE_HEADER_BASE}
      OUTPUT_VARIABLE HEADER_STRIPPED
      )
  cmake_path(REMOVE_FILENAME HEADER_STRIPPED OUTPUT_VARIABLE HEADER_RELATIVE)
  install(
      FILES ${HEADER}
      DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${HEADER_RELATIVE}"
  )
endforeach()
# Install the export directory
install(
    FILES ${TILEDB_EXPORT_HEADER}
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${TILEDB_EXPORT_HEADER_INSTALL_PATH}"
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

if (TILEDB_VERSION)
  set(TILEDB_PROJECT_CONFIG_VERSION_FILE "${CMAKE_CURRENT_BINARY_DIR}/TileDBConfigVersion.cmake")
  write_basic_package_version_file("${TILEDB_PROJECT_CONFIG_VERSION_FILE}"
    VERSION "${TILEDB_VERSION}"
    COMPATIBILITY SameMinorVersion
  )
  install(
    FILES "${TILEDB_PROJECT_CONFIG_VERSION_FILE}"
    DESTINATION "${CONFIG_INSTALL_DIR}"
  )
endif()

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

if (WIN32)
  set(PKGCONF_REQ_PRIV "libmagic pcre2-8 pcre2-posix")
  string(CONCAT PKGCONF_REQ_PRIV ${PKGCONF_REQ_PRIV} " webp imagedec imageenc imageioutil webpdecoder webpdmux webpmux")
endif()

# PKG Config file
configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/inputs/tiledb.pc.in
        ${CMAKE_CURRENT_BINARY_DIR}/tiledb.pc
        @ONLY
)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/tiledb.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
