/**
 * @file   helpers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file declares some test suite helper functions.
 */

#ifndef TILEDB_TEST_HELPERS_H
#define TILEDB_TEST_HELPERS_H

#include "tiledb.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb_serialization.h"

#include <mutex>
#include <sstream>
#include <string>
#include <thread>

// A mutex for protecting the thread-unsafe Catch2 macros.
extern std::mutex catch2_macro_mutex;

// A thread-safe variant of the CHECK macro.
#define CHECK_SAFE(a)                                     \
  {                                                       \
    std::lock_guard<std::mutex> lock(catch2_macro_mutex); \
    CHECK(a);                                             \
  }

// A thread-safe variant of the REQUIRE macro.
#define REQUIRE_SAFE(a)                                   \
  {                                                       \
    std::lock_guard<std::mutex> lock(catch2_macro_mutex); \
    REQUIRE(a);                                           \
  }

namespace tiledb {

namespace sm {
class SubarrayPartitioner;
}

namespace test {

// A dummy `Stats` instance. This is useful for constructing
// objects that require a parent `Stats` object. These stats are
// never used.
static tiledb::sm::stats::Stats g_helper_stats("test");

// For easy reference
typedef std::pair<tiledb_filter_type_t, int> Compressor;
template <class T>
using SubarrayRanges = std::vector<std::vector<T>>;

/**
 * Helper struct for the buffers of an attribute/dimension
 * (fixed- or var-sized).
 */
struct QueryBuffer {
  /**
   * For fixed-sized attributes/dimensions, it contains the fixed-sized values.
   * For var-sized attributes/dimensions, it contains the offsets.
   * var buffer is nullptr.
   */
  void* fixed_;
  /** Size of fixed buffer. */
  uint64_t fixed_size_;
  /**
   * For fixed-sized attributes/dimensions, it is `nullptr`.
   * For var-sized attributes/dimensions, it contains the var-sized values.
   */
  void* var_;
  /** Size of var buffer. */
  uint64_t var_size_;
};
/** Map attribute/dimension name -> QueryBuffer */
typedef std::map<std::string, QueryBuffer> QueryBuffers;

/**
 * Get the config for using the refactored readers.
 *
 * @return Using the refactored readers or not.
 */
bool use_refactored_readers();

/**
 * Checks that the input partitioner produces the input partitions
 * (i.e., subarrays).
 *
 * @tparam T The datatype of the subarray of the partitioner.
 * @param partitioner The partitioner.
 * @param partitions The ranges to be checked.
 * @param last_unsplittable Whether the last partition is unsplittable.
 */
template <class T>
void check_partitions(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<T>>& partitions,
    bool last_unsplittable);

/**
 * Checks if the input subarray has the input subarray ranges.
 *
 * @tparam T The subarray domain datatype
 * @param subarray The subarray to be checked.
 * @param ranges The ranges to be checked (a vector of ranges per dimension).
 */
template <class T>
void check_subarray(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<T>& ranges);

/**
 * Closes an array.
 *
 * @param ctx The TileDB context.
 * @param array The array to be closed.
 */
void close_array(tiledb_ctx_t* ctx, tiledb_array_t* array);

/**
 * Small wrapper to test round trip serialization in array create
 * @param ctx TileDB context
 * @param path path to create array at
 * @param array_schema array schema to create
 * @param serialize_array_schema whether to round-trip schema through
 * serialization
 */
int array_create_wrapper(
    tiledb_ctx_t* ctx,
    const std::string& path,
    tiledb_array_schema_t* array_schema,
    bool serialize_array_schema);

/**
 * Helper method to create an array.
 *
 * @param ctx TileDB context.
 * @param array_name The array name.
 * @param array_type The array type (dense or sparse).
 * @param dim_names The names of dimensions.
 * @param dim_types The types of dimensions.
 * @param dim_domains The domains of dimensions.
 * @param tile_extents The tile extents of dimensions.
 * @param attr_names The names of attributes.
 * @param attr_types The types of attributes.
 * @param cell_val_num The number of values per cell of attributes.
 * @param compressors The compressors of attributes.
 * @param tile_order The tile order.
 * @param cell_order The cell order.
 * @param capacity The tile capacity.
 * @param allows_dups Whether the array allows coordinate duplicates.
 * @param serialize_array_schema whether to round-trip through serialization or
 * not
 */

void create_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_array_type_t array_type,
    const std::vector<std::string>& dim_names,
    const std::vector<tiledb_datatype_t>& dim_types,
    const std::vector<void*>& dim_domains,
    const std::vector<void*>& tile_extents,
    const std::vector<std::string>& attr_names,
    const std::vector<tiledb_datatype_t>& attr_types,
    const std::vector<uint32_t>& cell_val_num,
    const std::vector<std::pair<tiledb_filter_type_t, int>>& compressors,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order,
    uint64_t capacity,
    bool allows_dups = false,
    bool serialize_array_schema = false);

/**
 * Helper method to create an encrypted array.
 *
 * @param ctx TileDB context.
 * @param array_name The array name.
 * @param enc_type The encryption type.
 * @param key The key to encrypt the array with.
 * @param key_len The key length.
 * @param array_type The array type (dense or sparse).
 * @param dim_names The names of dimensions.
 * @param dim_types The types of dimensions.
 * @param dim_domains The domains of dimensions.
 * @param tile_extents The tile extents of dimensions.
 * @param attr_names The names of attributes.
 * @param attr_types The types of attributes.
 * @param cell_val_num The number of values per cell of attributes.
 * @param compressors The compressors of attributes.
 * @param tile_order The tile order.
 * @param cell_order The cell order.
 * @param capacity The tile capacity.
 */

void create_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t enc_type,
    const char* key,
    uint32_t key_len,
    tiledb_array_type_t array_type,
    const std::vector<std::string>& dim_names,
    const std::vector<tiledb_datatype_t>& dim_types,
    const std::vector<void*>& dim_domains,
    const std::vector<void*>& tile_extents,
    const std::vector<std::string>& attr_names,
    const std::vector<tiledb_datatype_t>& attr_types,
    const std::vector<uint32_t>& cell_val_num,
    const std::vector<std::pair<tiledb_filter_type_t, int>>& compressors,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order,
    uint64_t capacity);

/**
 * Helper method that creates a directory.
 *
 * @param path The name of the directory to be created.
 * @param ctx The TileDB context.
 * @param vfs The VFS object that will create the directory.
 */
void create_dir(const std::string& path, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

/**
 * Helper method that creates an S3 bucket (if it does not already exist).
 *
 * @param bucket_name The name of the bucket to be created.
 * @param s3_supported The bucket will be created only if this is `true`.
 * @param ctx The TileDB context.
 * @param vfs The VFS object that will create the bucket.
 */
void create_s3_bucket(
    const std::string& bucket_name,
    bool s3_supported,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs);

/**
 * Helper method that creates an Azure container (if it does not already exist).
 *
 * @param container_name The name of the container to be created.
 * @param azure_supported The container will be created only if this is `true`.
 * @param ctx The TileDB context.
 * @param vfs The VFS object that will create the bucket.
 */
void create_azure_container(
    const std::string& container_name,
    bool azure_supported,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs);

/**
 * Creates a subarray for the input array.
 *
 * @tparam T The datatype of the subarray domain.
 * @param array The input array.
 * @param ranges The ranges of the subarray to be created.
 * @param layout The layout of the subarray.
 * @param subarray The subarray to be set.
 * @param coalesce_ranges Whether the subarray should coalesce ranges.
 */
template <class T>
void create_subarray(
    tiledb::sm::Array* array,
    const SubarrayRanges<T>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges = false);

/**
 * Helper method that creates a TileDB context and a VFS object.
 *
 * @param s3_supported Indicates whether S3 is supported or not.
 * @param azure_supported Indicates whether Azure is supported or not.
 * @param ctx The TileDB context to be created.
 * @param vfs The VFS object to be created.
 */
void create_ctx_and_vfs(
    bool s3_supported,
    bool azure_supported,
    tiledb_ctx_t** ctx,
    tiledb_vfs_t** vfs);

/**
 * Helper function to get the supported filesystems.
 *
 * @param s3_supported Set to `true` if S3 is supported.
 * @param hdfs_supported Set to `true` if HDFS is supported.
 * @param azure_supported Set to `true` if Azure is supported.
 * @param gcs_supported Set to `true` if GCS is supported.
 */
void get_supported_fs(
    bool* s3_supported,
    bool* hdfs_supported,
    bool* azure_supported,
    bool* gcs_supported);

/**
 * Opens an array.
 *
 * @param ctx The TileDB context.
 * @param array The array to be opened.
 * @param query_type The query type.
 */
void open_array(tiledb_ctx_t* ctx, tiledb_array_t* array, tiledb_query_type_t);

/**
 * Returns a random bucket name, with `prefix` as prefix and using
 * the thread id as a "random" suffix.
 *
 * @param prefix The prefix of the bucket name.
 * @return A random bucket name.
 */
std::string random_name(const std::string& prefix);

/**
 * Helper method that removes a directory.
 *
 * @param path The name of the directory to be removed.
 * @param ctx The TileDB context.
 * @param vfs The VFS object that will remove the directory.
 */
void remove_dir(const std::string& path, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

/**
 * Helper method that removes an S3 bucket.
 *
 * @param bucket_name The name of the bucket to be removed.
 * @param s3_supported The bucket is removed only when this is `true`.
 * @param ctx The TileDB context.
 * @param vfs The VFS object that will remove the bucket.
 */
void remove_s3_bucket(
    const std::string& bucket_name,
    bool s3_supported,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs);

/**
 * Helper method to configure a single-stage filter list with the given
 * compressor and add it to the given attribute.
 *
 * @param ctx TileDB context
 * @param attr Attribute to set filter list on
 * @param compressor Compressor type to use
 * @param level Compression level to use
 */
int set_attribute_compression_filter(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_filter_type_t compressor,
    int32_t level);

/**
 * Performs a single write to an array, at a timestamp.
 *
 * @param ctx The TileDB context.
 * @param array_name The array name.
 * @param timestamp The timestamp to write at.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    uint64_t timestamp,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);

/**
 * Performs a single write to an array, at a timestamp.
 *
 * @param ctx The TileDB context.
 * @param array_name The array name.
 * @param encyrption_type The type of encryption.
 * @param key The encryption key.
 * @param key_len The encryption key length.
 * @param timestamp The timestamp to write at.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t encryption_type,
    const char* key,
    uint64_t key_len,
    uint64_t timestamp,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);

/**
 * Performs a single write to an array.
 *
 * @param ctx The TileDB context.
 * @param array_name The array name.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);

/**
 * Performs a single write to an array, inside a given subarray.
 *
 * @param ctx The TileDB context.
 * @param array_name The array name.
 * @param subarray The subarray to write into.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    const void* subarray,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);

/**
 * Performs a single write to an array, inside a given subarray and
 * at a timestamp.
 *
 * @param ctx The TileDB context.
 * @param array_name The array name.
 * @param timestamp The timestamp to write at.
 * @param subarray The subarray to write into.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    uint64_t timestamp,
    const void* subarray,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);

/**
 * Performs a single write to an array, inside a given subarray and
 * at a timestamp.
 *
 * @param ctx The TileDB context.
 * @param array_name The array name.
 * @param encyrption_type The type of encryption.
 * @param key The encryption key.
 * @param key_len The encryption key length.
 * @param timestamp The timestamp to write at.
 * @param subarray The subarray to write into.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t encryption_type,
    const char* key,
    uint64_t key_len,
    uint64_t timestamp,
    const void* subarray,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);

/**
 * Performs a single write to an array at a timestamp.
 *
 * @param ctx The TileDB context.
 * @param array_name The array name.
 * @param timestamp The timestamp to write at.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 * @param uri The written fragment URI.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    uint64_t timestamp,
    tiledb_layout_t layout,
    const QueryBuffers& buffers,
    std::string* uri);

/**
 * Performs a single write to an array at a timestamp.
 *
 * @param ctx The TileDB context.
 * @param array_name The array name.
 * @param encyrption_type The type of encryption.
 * @param key The encryption key.
 * @param key_len The encryption key length.
 * @param timestamp The timestamp to write at.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 * @param uri The written fragment URI.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t encryption_type,
    const char* key,
    uint64_t key_len,
    uint64_t timestamp,
    tiledb_layout_t layout,
    const QueryBuffers& buffers,
    std::string* uri);

/**
 * Performs a single write to an array, inside a given subarray and
 * at a timestamp.
 *
 * @param ctx The TileDB context.
 * @param array_name The array name.
 * @param timestamp The timestamp to write at.
 * @param subarray The subarray to write into.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 * @param uri The written fragment URI.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    uint64_t timestamp,
    const void* subarray,
    tiledb_layout_t layout,
    const QueryBuffers& buffers,
    std::string* uri);

/**
 * Performs a single write to an array, inside a given subarray and
 * at a timestamp.
 *
 * @param ctx The TileDB context.
 * @param array_name The array name.
 * @param encyrption_type The type of encryption.
 * @param key The encryption key.
 * @param key_len The encryption key length.
 * @param timestamp The timestamp to write at.
 * @param subarray The subarray to write into.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 * @param uri The written fragment URI.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t encryption_type,
    const char* key,
    uint64_t key_len,
    uint64_t timestamp,
    const void* subarray,
    tiledb_layout_t layout,
    const QueryBuffers& buffers,
    std::string* uri);

/**
 * Performs a single read to an array.
 *
 * @tparam T The array domain type.
 * @param ctx The TileDB context.
 * @param array The input array.
 * @param The subarray ranges.
 * @param layout The query layout.
 * @param buffers The attribute/dimension buffers to be read.
 */
template <class T>
void read_array(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<T>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);

/**
 * Returns the number of fragments in the input array,
 * appropriately excluding special files and subdirectories.
 */
int32_t num_fragments(const std::string& array_name);

}  // End of namespace test

}  // End of namespace tiledb

#endif
