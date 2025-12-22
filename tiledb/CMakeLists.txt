#
# tiledb/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017-2025 TileDB, Inc.
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

# Compile all core sources with PIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

############################################################
# Subdirectories
############################################################

add_subdirectory(api)
add_subdirectory(as_built)
add_subdirectory(common)
add_subdirectory(oxidize)
add_subdirectory(platform)
add_subdirectory(type)
add_subdirectory(sm)
add_subdirectory(stdx)
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

if (TILEDB_CPP_API)
  list(APPEND TILEDB_PUBLIC_HEADERS
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/tiledb
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/tiledb_experimental
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array_schema.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array_schema_evolution.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/array_schema_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/as_built_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/attribute.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/attribute_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/channel_operation.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/channel_operator.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/config.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/consolidation_plan_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/context.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/core_interface.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/deleter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/dimension.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/dimension_label_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/domain.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/enumeration_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/error.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/exception.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/filter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/filter_list.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/fragment_info.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/group.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/log.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/object_iter.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/object.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/profile_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query_channel.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query_condition_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query_condition.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/query_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/schema_base.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/stats.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/capi_string.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/subarray.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/ndrectangle.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/current_domain.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/subarray_experimental.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/type.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/utils.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/version.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/vfs.h
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api/vfs_experimental.h
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
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/memory.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/stdx_string.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/filesystem/home_directory.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/interval/interval.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/common/types/dynamic_typed_datum.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/platform/cert_file.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array/array.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array/array_directory.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array/array_operations.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/array_schema.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/array_schema_evolution.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/array_schema_operations.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/attribute.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/dimension.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/dimension_label.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/domain.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/enumeration.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/ndrectangle.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/array_schema/current_domain.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/buffer.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/buffer/buffer_list.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/api_argument_validator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb_dimension_label.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api/tiledb_filestore.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/bzip_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/dd_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/delta_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/dict_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/gzip_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/lz4_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/rle_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/compressors/zstd_compressor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/config/config.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/config/config_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidation_plan/consolidation_plan.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/array_meta_consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/commits_consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/fragment_consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/fragment_meta_consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/consolidator/group_meta_consolidator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/encryption_key.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto_openssl.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto_win32.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/curl/curl_init.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/azure.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/gcs.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/local.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/mem_filesystem.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/path_win.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/posix.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3_thread_pool_executor.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3/AWSCredentialsProviderChain.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3/GeneralHTTPCredentialsProvider.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3/STSCredentialsProvider.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/s3/STSProfileWithWebIdentityCredentialsProvider.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/ssl_config.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/uri.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/vfs.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/vfs_file_handle.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/win.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filesystem/filesystem_base.cc
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
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/webp_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/noop_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/filter/positive_delta_filter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/fragment_identifier.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/fragment_info.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/fragment_metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/loaded_fragment_metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/ondemand_fragment_metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/fragment/v1v2preloaded_fragment_metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/global_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/signal_handlers.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/global_state/watchdog.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_details.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_details_v1.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_details_v2.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_directory.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_member.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_member_v1.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/group/group_member_v2.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/metadata/metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/cancelable_tasks.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/constants.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/parse_argument.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/rectangle.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/tdb_math.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/tdb_time.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/types.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/misc/win_constants.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/object/object.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/object/object_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/object/object_mutex.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/ast/query_ast.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/deletes_and_updates/deletes_and_updates.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/deletes_and_updates/serialization.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/dimension_label/array_dimension_label_queries.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/dimension_label/dimension_label_query.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/dimension_label/index_data.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/hilbert_order.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/legacy/cell_slab_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/legacy/reader.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/legacy/read_cell_slab_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/query.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/query_condition.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/query_remote_buffer_storage.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/query_state.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/aggregators/count_aggregator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/aggregators/min_max_aggregator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/aggregators/operation.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/aggregators/output_buffer_validator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/aggregators/safe_sum.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/aggregators/sum_aggregator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/dense_reader.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query/readers/ordered_dim_label_reader.cc
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
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/query_plan/query_plan.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_client.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_profile.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rtree/rtree.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_directory.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema_evolution.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/config.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/current_domain.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/enumeration.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/fragment_info.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/fragment_metadata.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/fragments.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/group.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query_aggregates.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query_plan.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/consolidation.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/vacuum.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/rest_capabilities.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/stats/global_stats.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/stats/stats.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/cancellation_source.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/context.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/context_resources.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/storage_manager/storage_manager.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/range_subset.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/relevant_fragment_generator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/subarray.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/subarray_partitioner.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/subarray_tile_overlap.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/subarray/tile_cell_slab_iter.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/tile.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/generic_tile_io.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/tile_metadata_generator.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/tile/writer_tile_tuple.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/storage_format/uri/generate_uri.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/storage_format/uri/parse_uri.cc
  ${TILEDB_CORE_INCLUDE_DIR}/tiledb/type/range/range.cc
)
list(APPEND TILEDB_CORE_SOURCES ${TILEDB_COMMON_SOURCES})
list(APPEND TILEDB_CORE_SOURCES ${TILEDB_API_SOURCES})

#openssl3 md5 deprecation mitigation
if(MSVC)
  set_source_files_properties(${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto_openssl.cc PROPERTIES COMPILE_OPTIONS "/wd4996")
else()
  set_source_files_properties(${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/crypto/crypto_openssl.cc PROPERTIES COMPILE_OPTIONS "-Wno-deprecated-declarations")
endif()

if (TILEDB_SERIALIZATION)
  list(APPEND TILEDB_CORE_SOURCES
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/curl.cc
    ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_client_remote.cc
  )

  if(NOT WIN32)
    set_source_files_properties(
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_client_remote.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_directory.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema_evolution.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/config.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/current_domain.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/enumeration.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/fragment_info.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/fragment_metadata.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/fragments.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/group.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query_aggregates.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query_plan.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/consolidation.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/vacuum.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp.c++
      PROPERTIES
        COMPILE_FLAGS "-Wno-unused-parameter"
    )
  endif()
  if(MSVC)
    set_source_files_properties(
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/rest/rest_client_remote.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_directory.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/array_schema_evolution.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/config.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/current_domain.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/enumeration.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/fragment_info.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/fragment_metadata.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/fragments.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/group.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query_aggregates.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/query_plan.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/consolidation.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/vacuum.cc
      ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp.c++
      PROPERTIES
        #   C4267: 'argument': conversion from 'size_t' to '<various-lesser-types>', possible loss of data
        #   C4100 '<some-name>' unreferenced format parameter
        COMPILE_FLAGS "/wd4267 /wd4100"
    )
  endif()

endif()

############################################################
# Build core objects as a reusable object library
############################################################

add_library(TILEDB_CORE_OBJECTS OBJECT
    ${TILEDB_CORE_SOURCES}
)

# List of libraries to be linked to TILEDB_CORE_OBJECTS.
set(TILEDB_CORE_OBJECTS_LIBS
  assert
  baseline
)
if (TILEDB_RUST)
  list(APPEND TILEDB_CORE_OBJECTS_LIBS tiledb_core_objects_oxidize)
endif ()

list(TRANSFORM TILEDB_CORE_OBJECTS_LIBS PREPEND "$<TARGET_OBJECTS:" OUTPUT_VARIABLE TILEDB_CORE_OBJECTS_LIBS_SOURCES)
list(TRANSFORM TILEDB_CORE_OBJECTS_LIBS_SOURCES APPEND ">" OUTPUT_VARIABLE TILEDB_CORE_OBJECTS_LIBS_SOURCES)

target_sources(TILEDB_CORE_OBJECTS
  PUBLIC
    ${TILEDB_CORE_OBJECTS_LIBS_SOURCES}
)

target_link_libraries(TILEDB_CORE_OBJECTS
  PUBLIC
    ${TILEDB_CORE_OBJECTS_LIBS}
)

target_link_libraries(TILEDB_CORE_OBJECTS INTERFACE configuration_definitions)

target_include_directories(TILEDB_CORE_OBJECTS
  PRIVATE
    "${TILEDB_CORE_INCLUDE_DIR}"
    "${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api"
	# to pickup <build_dir>/tiledb for capnp gen'd files
    "${CMAKE_CURRENT_BINARY_DIR}/.."
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

if (TILEDB_REMOVE_DEPRECATIONS)
  add_definitions(-DTILEDB_REMOVE_DEPRECATIONS)
  message(STATUS "The TileDB library is compiled without deprecated APIs.")
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
endif()

############################################################
# Dependencies: set up includes/linking
############################################################

# Unfortunately, with CMake < 3.12, you can't use target_link_libraries()
# on TILEDB_CORE_OBJECTS. This workaround uses an interface library
# so we can use the targets created by the calls to find_package().
add_library(TILEDB_CORE_OBJECTS_ILIB INTERFACE)

# object store definitions
target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE configuration_definitions)

# Find OpenSSL first in case it's needed for S3 or Azure
if (NOT WIN32)
  find_package(OpenSSL REQUIRED)
endif()

# S3 dependencies
if (TILEDB_S3)
  message(STATUS "The TileDB library is compiled with S3 support.")

  set(AWS_SERVICES identity-management sts s3)
  find_package(AWSSDK REQUIRED QUIET COMPONENTS ${AWS_SERVICES})
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      ${AWSSDK_LINK_LIBRARIES}
      )
endif()

# Azure dependencies
if (TILEDB_AZURE)
  message(STATUS "The TileDB library is compiled with Azure support.")

  find_package(azure-identity-cpp CONFIG REQUIRED)
  find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
  find_package(azure-storage-files-datalake-cpp CONFIG REQUIRED)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
          INTERFACE
          Azure::azure-identity
          Azure::azure-storage-blobs
          Azure::azure-storage-files-datalake
          )
endif()

if (TILEDB_GCS)
  find_package(google_cloud_cpp_storage CONFIG REQUIRED)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE
          google-cloud-cpp::storage
          )
endif()

# Libcurl
if (TILEDB_SERIALIZATION)
  find_package(CURL REQUIRED)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      CURL::libcurl
    )
endif()

# Sanitizer linker flags
if (TILEDB_SANITIZER)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      -fsanitize=${TILEDB_SANITIZER}
  )
endif()

# Coverage linker flags
# TODO: Use $<CONFIG:Coverage> after we remove the superbuild.
if (CMAKE_BUILD_TYPE MATCHES "Coverage")
  if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "Compiling with code coverage is only supported with Clang.")
  endif()
  target_link_options(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
      --coverage
  )
endif()

# Required dependencies. These come last as any prior dependencies relying on
# these must be listed first for Linux. E.g. AWSSDK and Curl depend on Zlib,
# which is installed here.
find_package(Blosc2 CONFIG REQUIRED)
find_package(BZip2 REQUIRED)
find_package(LZ4 MODULE REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(spdlog REQUIRED)
find_package(ZLIB REQUIRED)
find_package(zstd CONFIG REQUIRED)
target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
  INTERFACE
    Blosc2::blosc2
    BZip2::BZip2
    lz4::lz4
    $<BUILD_INTERFACE:nlohmann_json::nlohmann_json>
    spdlog::spdlog
    ZLIB::ZLIB
    zstd::libzstd
)

if(TILEDB_WEBP)
  find_package(WebP REQUIRED)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
    INTERFACE
    WebP::webp
  )
  add_definitions(-DTILEDB_WEBP)
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
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE shlwapi rpcrt4 bcrypt)
endif()

# macOS specific libraries
if(APPLE)
  if(TILEDB_S3)
    # this is a transitive dependency for statically linking S3 SDK
    target_link_libraries(TILEDB_CORE_OBJECTS_ILIB INTERFACE "-framework CoreFoundation -framework SystemConfiguration -framework Security")
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
# capnproto generated sources
############################################################

# Serialization
if(TILEDB_SERIALIZATION)
  find_package(CapnProto REQUIRED)
  target_link_libraries(TILEDB_CORE_OBJECTS_ILIB
          INTERFACE
          CapnProto::capnp
          CapnProto::capnp-json
          CapnProto::kj
          )

  target_sources(TILEDB_CORE_OBJECTS PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization/tiledb-rest.capnp.c++
  )

  ###################################
  # capnproto file generation target
  ###################################

  # This target regenerates the checked-in generated source files for serialization
  # support. It must be re-run whenever `tiledb-rest.capnp` is updated.

  set(TILEDB_SERIALIZATION_GENERATED_DIR
      ${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization)

  # We only need to override the include path and PATH env for EP builds
  # note: We run the compiler under `cmake -E env ...` because we need to run capnp executable
  #       the capnp driver needs to be able to find the plugin executables (eg capnpc-c++).
  #       For system installed capnp binaries we rely on the user to have their PATH
  #       include the directory with CapNProto plugin binaries.
  set(CAPNP_PLUGIN_DIR $<TARGET_FILE_DIR:CapnProto::capnp_tool>)
  list(APPEND CAPNP_COMPILE_COMMAND "${CMAKE_COMMAND}" -E env PATH="${CAPNP_PLUGIN_DIR}" "${CAPNP_EXECUTABLE}" compile "-I${CAPNP_INCLUDE_DIRECTORY}")

  # Add the rest of the capnp compile command
  list(APPEND CAPNP_COMPILE_COMMAND -oc++:"${TILEDB_SERIALIZATION_GENERATED_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization/tiledb-rest.capnp" --src-prefix="${CMAKE_CURRENT_SOURCE_DIR}/sm/serialization")

  add_custom_target(
    update-serialization
      ${CMAKE_COMMAND} -E echo "CAPNP_COMPILE_COMMAND: '${CAPNP_COMPILE_COMMAND}'"
    COMMAND
      "${CAPNP_COMPILE_COMMAND}"
    DEPENDS ${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/serialization/tiledb-rest.capnp
    COMMAND_EXPAND_LISTS
    COMMENT "Re-generate tiledb-rest.capnp.<h,c++> files for serialization support"
    )

endif()


############################################################
# TileDB library targets
############################################################

# CMake will determine the library's linkage from
# the value of the BUILD_SHARED_LIBS variable.
add_library(tiledb $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)

# Export the target as either tiledb_shared or tiledb_static for compatibility.
# The exported config will create the unified tiledb target that links to either
# of them.
if(BUILD_SHARED_LIBS)
  set_target_properties(tiledb PROPERTIES EXPORT_NAME tiledb_shared)
else()
  set_target_properties(tiledb PROPERTIES EXPORT_NAME tiledb_static)
endif()

if(TILEDB_REMOVE_DEPRECATIONS)
  target_compile_definitions(tiledb PUBLIC TILEDB_REMOVE_DEPRECATIONS)
endif()

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/sm/c_api/tiledb_version.h" ver)

string(REGEX MATCH "TILEDB_VERSION_MAJOR ([0-9]*)" _ ${ver})
set(ver_major ${CMAKE_MATCH_1})

string(REGEX MATCH "TILEDB_VERSION_MINOR ([0-9]*)" _ ${ver})
set(ver_minor ${CMAKE_MATCH_1})

string(REGEX MATCH "TILEDB_VERSION_PATCH ([0-9]*)" _ ${ver})
set(ver_patch ${CMAKE_MATCH_1})

set(VERSION "${ver_major}.${ver_minor}.${ver_patch}")

if (WIN32 AND BUILD_SHARED_LIBS)
  # WIN32 RC file creation (for VERSIONINFO)
  set(TDB_WIN32_FILEVERSION "${ver_major},${ver_minor},${ver_patch},0")
  set(TDB_WIN32_FILEVERSION_STR "\"${ver_major}.${ver_minor}.${ver_patch}\"")
  configure_file(
          ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/inputs/tiledb_win32_verinfo.rc.in
          ${CMAKE_CURRENT_BINARY_DIR}/tiledb_win32_verinfo.rc
          @ONLY
  )

  # The .rc file will be created elsewhere (currently below) via configure_file()
  target_sources(tiledb PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/tiledb_win32_verinfo.rc)
  target_include_directories(tiledb
    PRIVATE
      "${TILEDB_CORE_INCLUDE_DIR}"
    )
endif()

if (TILEDB_VERSION AND CMAKE_SYSTEM_NAME MATCHES "Linux")
  set_target_properties(tiledb
    PROPERTIES
      SOVERSION "${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}")
endif()

# Link the dependencies specified earlier
target_link_libraries(tiledb
  PRIVATE
    # TODO: TILEDB_CORE_OBJECTS_ILIB should be removed and dependencies should be specified
    # without directly getting target_properties.
    $<GENEX_EVAL:$<TARGET_PROPERTY:TILEDB_CORE_OBJECTS_ILIB,INTERFACE_LINK_LIBRARIES>>
    $<BUILD_INTERFACE:TILEDB_CORE_OBJECTS>
)

if (WIN32 AND NOT BUILD_SHARED_LIBS)
  # On Windows we must name the static library something else to avoid
  # name clash with the DLL's "import library" .lib file.
  set_target_properties(tiledb
    PROPERTIES
      OUTPUT_NAME "tiledbstatic"
  )
endif()

############################################################
# API symbol exports (and public headers for install)
############################################################
# Generation of the header (still) happens here, but `TILEDB_EXPORT_*` symbols
# are defined in the parent directory.

# Generates the file 'tiledb_export.h' suitable for the current compiler.
include(GenerateExportHeader)
generate_export_header(tiledb
  BASE_NAME tiledb
)
# Install locally
configure_file(${TILEDB_EXPORT_HEADER} ${TILEDB_LOCALINSTALL_INCLUDE}/${TILEDB_EXPORT_HEADER_LOCALINSTALL_PATH} COPYONLY)

target_compile_definitions(TILEDB_CORE_OBJECTS PRIVATE -Dtiledb_EXPORTS)
target_include_directories(TILEDB_CORE_OBJECTS PRIVATE ${TILEDB_EXPORT_HEADER_DIR})

# Add the generated header to the public headers list
list(APPEND TILEDB_PUBLIC_HEADERS
  ${TILEDB_EXPORT_HEADER}
)

# Set the public headers, which are the ones that get installed.
set_target_properties(tiledb
  PROPERTIES
    PUBLIC_HEADER "${TILEDB_PUBLIC_HEADERS}"
)

# Don't re-export symbols from static (archive) libraries
#   Prevents conflicts with other versions of (e.g.) OpenSSL
#   loaded in the same process namespace, which can cause
#   crashes if the versions are not compatible.
if (CMAKE_SYSTEM_NAME MATCHES "Linux")
  set_target_properties(tiledb
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
if (BUILD_SHARED_LIBS AND NOT WIN32)
  set_target_properties(tiledb
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
  tiledb
)
if (NOT BUILD_SHARED_LIBS)
  list(APPEND TILEDB_INSTALL_TARGETS
      TILEDB_CORE_OBJECTS_ILIB
      configuration_definitions
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

if (WIN32 AND TILEDB_WEBP)
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

include(${CMAKE_SOURCE_DIR}/cmake/package.cmake)

# Add alias for compatibility with earlier TileDB versions.
add_custom_target(install-tiledb
  COMMENT "The install-tiledb target is deprecated and can lead to worse performance. Use install instead."
  COMMAND ${CMAKE_COMMAND} --build . --target install --config $<CONFIG>
  WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
)
