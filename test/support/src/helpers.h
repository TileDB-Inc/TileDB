/**
 * @file   helpers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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

#include "test/support/src/coords_workaround.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb.h"
#include "tiledb/api/c_api/subarray/subarray_api_internal.h"
#include "tiledb/common/common.h"
#include "tiledb/common/random/random_label.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb_serialization.h"

#include <mutex>
#include <sstream>
#include <string>
#include <thread>

/**
 * Helper function to set environment variables across platforms.
 *
 * @param __name Name of the environment variable.
 * @param __value Value of the environment variable.
 * @return 0 on success, -1 on error.
 */
int setenv_local(const char* __name, const char* __value);

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

namespace tiledb::sm {
class SubarrayPartitioner;
}

namespace tiledb::test {

// A dummy `Stats` instance. This is useful for constructing
// objects that require a parent `Stats` object. These stats are
// never used.
static tiledb::sm::stats::Stats g_helper_stats("test");

// A dummy `Logger` instance. This is useful for constructing
// objects that require a parent `Logger` object.
shared_ptr<Logger> g_helper_logger(void);

// For easy reference
typedef std::pair<tiledb_filter_type_t, int> Compressor;
template <class T>
using SubarrayRanges = std::vector<std::vector<T>>;

/**
 * Throws if the return code is not OK.
 * For use in test setup for object allocation.
 *
 * @param rc Return code from a TileDB C-API setup function.
 */
void throw_if_setup_failed(int rc);

/**
 * Throws if the condition is not met.
 * For use in test setup for object allocation.
 *
 * @param condition Condition to check from a TileDB C-API setup function.
 */
void throw_if_setup_failed(bool condition);

/**
 * Check the return code for a TileDB C-API function is TILEDB_ERR and
 * compare the last error message from the local TileDB context to an expected
 * error message.
 *
 * @param ctx Context to check for the error and error message.
 * @param rc Return code from a TileDB C-API function.
 * @param expected_msg The expected message from the last error.
 * @param contains If true, error messages are matched with ContainsSubstring
 */
void check_tiledb_error_with(
    tiledb_ctx_t* ctx,
    int rc,
    const std::string& expected_msg,
    bool contains = false);

/**
 * Checks the return code for a TileDB C-API function is TILEDB_OK. If not,
 * if will add a failed assert to the Catch2 test and print the last error
 * message from the local TileDB context.
 *
 * @param ctx Context to check for the error and error message.
 * @param rc Return code from a TileDB C-API function.
 */
void check_tiledb_ok(tiledb_ctx_t* ctx, int rc);

/**
 * Require the return code for a TileDB C-API function is TILEDB_ERR and
 * compare the last error message from the local TileDB context to an expected
 * error message.
 *
 * @param ctx Context to check for the error and error message.
 * @param rc Return code from a TileDB C-API function.
 * @param expected_msg The expected message from the last error.
 * @param contains If true, error messages are matched with ContainsSubstring
 */
void require_tiledb_error_with(
    tiledb_ctx_t* ctx,
    int rc,
    const std::string& expected_msg,
    bool contains = false);

/**
 * Requires the return code for a TileDB C-API function is TILEDB_OK. If not,
 * it will end the Catch2 test and print the last error message from the local
 * TileDB context.
 *
 * @param ctx Context to check for the error and error message.
 * @param rc Return code from a TileDB C-API function.
 */
void require_tiledb_ok(tiledb_ctx_t* ctx, int rc);

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

// server side buffers
using ServerDataBuffers = std::vector<std::vector<uint8_t>>;
using ServerOffsetsBuffers = std::vector<std::vector<uint64_t>>;
struct ServerQueryBuffers {
  ServerDataBuffers attr_or_dim_data;
  ServerOffsetsBuffers attr_or_dim_off;
  ServerDataBuffers attr_or_dim;
  ServerDataBuffers attr_or_dim_nullable;
};

/**
 * Get the config for using the refactored dense reader.
 *
 * @return Using the refactored reader or not.
 */
bool use_refactored_dense_reader();

/**
 * Get the config for using the refactored sparse global order reader.
 *
 * @return Using the refactored reader or not.
 */
bool use_refactored_sparse_global_order_reader();

/**
 * Get the config for using the refactored unordered with dups reader.
 *
 * @return Using the refactored reader or not.
 */
bool use_refactored_sparse_unordered_with_dups_reader();

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

template <class T>
void check_subarray(
    tiledb::Subarray& subarray, const SubarrayRanges<T>& ranges);

template <class T>
bool subarray_equiv(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);

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
 * @param serialize_array_schema Whether to round-trip through serialization or
 * not.
 * @param nullable Whether the attributes are nullable or not.
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
    bool serialize_array_schema = false,
    const optional<std::vector<bool>>& nullable = nullopt);

/**
 * Helper method to create an encrypted array.
 *
 * @param ctx TileDB context.
 * @param array_name The array name.
 * @param enc_type The encryption type.
 * @param key The key to encrypt the array with.
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
 * Helper method to create an array schema.
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
 * not
 */
tiledb_array_schema_t* create_array_schema(
    tiledb_ctx_t* ctx,
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
    bool allows_dups = false);

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
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<T>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges = false);

/**
 * Creates a capi subarray for the input array.
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
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<T>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges = false);

/**
 * Creates a c++ api subarray for the input array.
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
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<T>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges = false);

/**
 * Helper function to get the supported filesystems.
 * Supports VFS override via "--vfs" command line argument.
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
    bool* gcs_supported,
    bool* rest_s3_supported);

/**
 * Opens an array.
 *
 * @param ctx The TileDB context.
 * @param array The array to be opened.
 * @param query_type The query type.
 */
void open_array(tiledb_ctx_t* ctx, tiledb_array_t* array, tiledb_query_type_t);

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
 * @param timestamp The timestamp to write at.
 * @param layout The layout to write into.
 * @param buffers The attribute/dimension buffers to be written.
 */
void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t encryption_type,
    const char* key,
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
 * Returns the number of commits in the input array,
 * given a context.
 */
int32_t num_commits(Context ctx, const std::string& array_name);

/**
 * Returns the number of commits in the input array,
 * appropriately excluding special files and subdirectories.
 */
int32_t num_commits(const std::string& array_name);

/**
 * Returns the number of fragments in the input array,
 * given a context.
 */
int32_t num_fragments(Context ctx, const std::string& array_name);

/**
 * Returns the number of fragments in the input array,
 * appropriately excluding special files and subdirectories.
 */
int32_t num_fragments(const std::string& array_name);

/**
 * Creates a random string of length l.
 */
std::string random_string(const uint64_t l);

/**
 * Gets the fragments directory from the array directory.
 */
std::string get_fragment_dir(std::string array_dir);

/**
 * Gets the commit directory from the array directory.
 */
std::string get_commit_dir(std::string array_dir);

/**
 * This class checks for and maintains a count of
 * each expected file extension on the given path.
 */
class FileCount {
 public:
  FileCount(
      VFS vfs, std::string path, std::vector<std::string> expected_extensions);
  ~FileCount() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Retrieve the file counts of all extensions.
   *
   * @return std::map<std::string, uint64_t> file counts of all extensions
   */
  const std::map<std::string, uint64_t>& file_count() const;

  /**
   * Retrieve the file count of the given file extension
   *
   * @param name
   * @return uint64_t file count of the given extension
   */
  uint64_t file_count(std::string extension);

  /**
   * Retrieve the size of the path
   *
   * @return uint64_t path size
   */
  uint64_t dir_size();

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The file extension name and its count on the path. */
  std::map<std::string, uint64_t> file_count_;

  /** The full size of the path. */
  uint64_t dir_size_;
};

/** This class checks for and maintains a count of the expected file extensions
 * in the commits directory the given array path. */
class CommitsDirectory : public FileCount {
 public:
  CommitsDirectory(VFS vfs, std::string array_name)
      : FileCount(
            vfs,
            array_name + "/" + tiledb::sm::constants::array_commits_dir_name,
            {tiledb::sm::constants::vacuum_file_suffix,
             tiledb::sm::constants::write_file_suffix,
             tiledb::sm::constants::delete_file_suffix,
             tiledb::sm::constants::update_file_suffix,
             tiledb::sm::constants::con_commits_file_suffix,
             tiledb::sm::constants::ignore_file_suffix}) {};
  ~CommitsDirectory() = default;
};

/**
 * Check count of values against a vector of expected counts for an array.
 */
template <class T>
void check_counts(span<T> vals, std::vector<uint64_t> expected);

/*
 * Helper function that serializes a query from a "client" or "server"
 * perspective.
 *
 * @param ctx Context.
 * @param query Tiledb query to serialize.
 * @param serialized Output buffer with serialized query.
 * @param clientside True if client serializes, false if server serializes.
 */
int serialize_query(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    std::vector<uint8_t>* serialized,
    bool clientside);

/**
 * Helper function that deserializes a query from a "client" perspective.
 *
 * @param ctx Context.
 * @param serialized Serialized buffer to deserialize into query object.
 * @param query Input/Output deserialized Tiledb query object.
 * @param clientside True if client deserializes, false if server deserializes.
 */
int deserialize_query(
    tiledb_ctx_t* ctx,
    std::vector<uint8_t>& serialized,
    tiledb_query_t* query,
    bool clientside);

/**
 * Helper function that deserializes a query from a "server" perspective.
 *
 * @param ctx Context.
 * @param serialized Serialized buffer to deserialize into query object.
 * @param query Output deserialized Tiledb query object with deserialized array
 * in it.
 * @param array_uri URI of the array the query is for.
 * @param clientside True if client deserializes, false if server deserializes.
 */
int deserialize_array_and_query(
    tiledb_ctx_t* ctx,
    std::vector<uint8_t>& serialized,
    tiledb_query_t** query,
    const char* array_uri,
    bool clientside);

/**
 * Helper function that generates a fragment uri for an array.
 * If the `array` arg is specified, the full URI is generated using
 * the write version of the array schema and the timestamp
 * the array was opened at. If unspecified, the function
 * will generate only the fragment name using the current timestamp
 * and the latest library format version.
 *
 * @param array A TileDB array
 * @return A test fragment uri
 */
sm::URI generate_fragment_uri(sm::Array* array);

/**
 * Helper function to create a sparse array using format version 11.
 *
 * @param ctx TileDB context.
 * @param array_name The name of the new array to create.
 */
void create_sparse_array_v11(tiledb_ctx_t* ctx, const std::string& array_name);

/**
 * Helper function to write data to a format version 11 sparse array.
 *
 * @param ctx TileDB context.
 * @param array_name The name of the array to write to.
 * @param timestamp The timestamp to open the array for writing.
 */
void write_sparse_v11(
    tiledb_ctx_t* ctx, const std::string& array_name, uint64_t timestamp);

/**
 * Helper function to validate data read from a format version 11 sparse array.
 *
 * @param ctx TileDB context.
 * @param array_name The name of the array to read from.
 * @param timestamp The timestamp to open the array for reading.
 */
void read_sparse_v11(
    tiledb_ctx_t* ctx, const std::string& array_name, uint64_t timestamp);

/**
 * Helper function to test two array schemas are equivalent.
 *
 * @param schema1 Expected array schema.
 * @param schema2 Actual array schema.
 */
void schema_equiv(
    const sm::ArraySchema& schema1, const sm::ArraySchema& schema2);

/**
 * Helper function to check equivalence of two shared pointers.
 * This requires implementing equality operator for the type T.
 */
template <typename T>
bool shared_ptr_equiv(shared_ptr<T> a, shared_ptr<T> b) {
  if (a == b) {
    return true;
  }
  return a && b && *a == *b;
}

}  // namespace tiledb::test

#endif
