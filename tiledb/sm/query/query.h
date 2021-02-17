/**
 * @file   query.h
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
 * This file defines class Query.
 */

#ifndef TILEDB_QUERY_H
#define TILEDB_QUERY_H

#include <functional>
#include <sstream>
#include <utility>
#include <vector>

#include "tiledb/common/logger.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/reader.h"
#include "tiledb/sm/query/validity_vector.h"
#include "tiledb/sm/query/writer.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class Subarray;
class StorageManager;

enum class QueryStatus : uint8_t;
enum class QueryType : uint8_t;

/** Processes a (read/write) query. */
class Query {
 public:
  /* ********************************* */
  /*          PUBLIC DATATYPES         */
  /* ********************************* */

  /**
   * Contains any current state related to (de)serialization of this query.
   * Mostly this supports setting buffers on this query that were allocated
   * internally as a part of deserialization (as opposed to user-set buffers).
   */
  struct SerializationState {
    /** Serialization state for a single attribute. */
    struct AttrState {
      /**
       * Buffer holding (or wrapping) fixed-length data, either attribute or
       * offset data.
       */
      Buffer fixed_len_data;

      /** Buffer holding (or wrapping) variable-length data. */
      Buffer var_len_data;

      /** Buffer holding (or wrapping) validity vector data. */
      Buffer validity_len_data;

      /** Value holding the length of the fixed-length data. */
      uint64_t fixed_len_size = 0;

      /** Value holding the length of the variable-length data. */
      uint64_t var_len_size = 0;

      /** Value holding the length of the validity vector data. */
      uint64_t validity_len_size = 0;
    };

    /** Serialization state per attribute. */
    std::unordered_map<std::string, AttrState> attribute_states;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor. The query type is inherited from the query type of the
   * input array. An optional fragment URI is given as input in
   * case the query will be used as writes and the given URI should be used
   * for the name of the new fragment to be created.
   *
   * @note Array must be a properly opened array.
   */
  Query(
      StorageManager* storage_manager,
      Array* array,
      URI fragment_uri = URI(""));

  /** Destructor. */
  ~Query();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Adds a range to the (read/write) query on the input dimension by index,
   * in the form of (start, end, stride).
   * The range components must be of the same type as the domain type of the
   * underlying array.
   */
  Status add_range(
      unsigned dim_idx, const void* start, const void* end, const void* stride);

  /**
   * Adds a variable-sized range to the (read/write) query on the input
   * dimension by index, in the form of (start, end).
   */
  Status add_range_var(
      unsigned dim_idx,
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size);

  /** Retrieves the number of ranges of the subarray for the given dimension
   * index. */
  Status get_range_num(unsigned dim_idx, uint64_t* range_num) const;

  /**
   * Retrieves a range from a dimension index in the form (start, end, stride).
   *
   * @param dim_idx The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   * @param stride The range stride to retrieve.
   * @return Status
   */
  Status get_range(
      unsigned dim_idx,
      uint64_t range_idx,
      const void** start,
      const void** end,
      const void** stride) const;

  /**
   * Retrieves a range's sizes for a variable-length dimension index
   *
   * @param dim_idx The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start_size range start size in bytes
   * @param end_size range end size in bytes
   * @return Status
   */
  Status get_range_var_size(
      unsigned dim_idx,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const;

  /**
   * Retrieves a range from a variable-length dimension index in the form
   * (start, end).
   *
   * @param dim_idx The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   * @return Status
   */
  Status get_range_var(
      unsigned dim_idx, uint64_t range_idx, void* start, void* end) const;

  /**
   * Adds a range to the (read/write) query on the input dimension by name,
   * in the form of (start, end, stride).
   * The range components must be of the same type as the domain type of the
   * underlying array.
   */

  Status add_range_by_name(
      const std::string& dim_name,
      const void* start,
      const void* end,
      const void* stride);

  /**
   * Adds a variable-sized range to the (read/write) query on the input
   * dimension by name, in the form of (start, end).
   */
  Status add_range_var_by_name(
      const std::string& dim_name,
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size);

  /** Retrieves the number of ranges of the subarray for the given dimension
   * name. */
  Status get_range_num_from_name(
      const std::string& dim_name, uint64_t* range_num) const;

  /**
   * Retrieves a range from a dimension name in the form (start, end, stride).
   *
   * @param dim_name The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   * @param stride The range stride to retrieve.
   * @return Status
   */
  Status get_range_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      const void** start,
      const void** end,
      const void** stride) const;

  /**
   * Retrieves a range's sizes for a variable-length dimension name
   *
   * @param dim_name The dimension name to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start_size range start size in bytes
   * @param end_size range end size in bytes
   * @return Status
   */
  Status get_range_var_size_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const;

  /**
   * Retrieves a range from a variable-length dimension name in the form (start,
   * end).
   *
   * @param dim_name The dimension name to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   * @return Status
   */
  Status get_range_var_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      void* start,
      void* end) const;

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  Status get_est_result_size(const char* name, uint64_t* size);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized
   * attribute/dimension.
   */
  Status get_est_result_size(
      const char* name, uint64_t* size_off, uint64_t* size_val);

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized,
   * nullable attribute.
   */
  Status get_est_result_size_nullable(
      const char* name, uint64_t* size_val, uint64_t* size_validity);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized,
   * nullable attribute.
   */
  Status get_est_result_size_nullable(
      const char* name,
      uint64_t* size_off,
      uint64_t* size_val,
      uint64_t* size_validity);

  /** Retrieves the number of written fragments. */
  Status get_written_fragment_num(uint32_t* num) const;

  /** Retrieves the URI of the written fragment with the input index. */
  Status get_written_fragment_uri(uint32_t idx, const char** uri) const;

  /**
   * Retrieves the timestamp range [t1,t2] of the written fragment with the
   * input index.
   */
  Status get_written_fragment_timestamp_range(
      uint32_t idx, uint64_t* t1, uint64_t* t2) const;

  /** Returns the array. */
  const Array* array() const;

  /** Returns the array. */
  Array* array();

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /** Returns the names of the buffers set by the user for the query. */
  std::vector<std::string> buffer_names() const;

  /**
   * Gets the query buffer for the input attribute/dimension.
   * An empty string means the special default attribute.
   */
  QueryBuffer buffer(const std::string& name) const;

  /**
   * Marks a query that has not yet been started as failed. This should not be
   * called asynchronously to cancel an in-progress query; for that use the
   * parent StorageManager's cancellation mechanism.
   * @return Status
   */
  Status cancel();

  /**
   * Finalizes the query, flushing all internal state. Applicable only to global
   * layout writes. It has no effect for any other query type.
   */
  Status finalize();

  /**
   * Retrieves the buffer of a fixed-sized attribute/dimension.
   *
   * @param name The buffer attribute/dimension name. An empty string means
   *     the special default attribute/dimension.
   * @param buffer The buffer to be retrieved.
   * @param buffer_size A pointer to the buffer size to be retrieved.
   * @return Status
   */
  Status get_buffer(
      const char* name, void** buffer, uint64_t** buffer_size) const;

  /**
   * Retrieves the offsets and values buffers of a var-sized
   * attribute/dimension.
   *
   * @param name The attribute/dimension name. An empty string means
   *     the special default attribute/dimension.
   * @param buffer_off The offsets buffer to be retrieved.
   * @param buffer_off_size A pointer to the offsets buffer size to be
   * retrieved.
   * @param buffer_val The values buffer to be retrieved.
   * @param buffer_val_size A pointer to the values buffer size to be retrieved.
   * @return Status
   */
  Status get_buffer(
      const char* name,
      uint64_t** buffer_off,
      uint64_t** buffer_off_size,
      void** buffer_val,
      uint64_t** buffer_val_size) const;

  /**
   * Retrieves the buffer and validity bytemap of a fixed-sized, nullable
   * attribute.
   *
   * @param name The buffer attribute name. An empty string means
   *     the special default attribute.
   * @param buffer The buffer to be retrieved.
   * @param buffer_size A pointer to the buffer size to be retrieved.
   * @param buffer The buffer to be retrieved.
   * @param buffer_size A pointer to the buffer size to be retrieved.
   * @return Status
   */
  Status get_buffer_vbytemap(
      const char* name,
      void** buffer,
      uint64_t** buffer_size,
      uint8_t** buffer_validity_bytemap,
      uint64_t** buffer_validity_bytemap_size) const;

  /**
   * Retrieves the offsets, values, and validity bytemap buffers of
   * a var-sized, nullable attribute.
   *
   * @param name The attribute name. An empty string means
   *     the special default attribute.
   * @param buffer_off The offsets buffer to be retrieved.
   * @param buffer_off_size A pointer to the offsets buffer size to be
   * retrieved.
   * @param buffer_val The values buffer to be retrieved.
   * @param buffer_val_size A pointer to the values buffer size to be retrieved.
   * @return Status
   */
  Status get_buffer_vbytemap(
      const char* name,
      uint64_t** buffer_off,
      uint64_t** buffer_off_size,
      void** buffer_val,
      uint64_t** buffer_val_size,
      uint8_t** buffer_validity_bytemap,
      uint64_t** buffer_validity_bytemap_size) const;

  /**
   * Returns the serialization state associated with the given attribute.
   *
   * @param attribute Attribute to get serialization state for
   * @param state Set to point to the serialization state
   * @return Status
   */
  Status get_attr_serialization_state(
      const std::string& attribute, SerializationState::AttrState** state);

  /**
   * Used by serialization to get the map of result sizes
   * @return
   */
  std::unordered_map<std::string, Subarray::ResultSize>
  get_est_result_size_map();

  /**
   * Used by serialization to get the map of max mem sizes
   * @return
   */
  std::unordered_map<std::string, Subarray::MemorySize> get_max_mem_size_map();

  /**
   * Returns `true` if the query has results. Applicable only to read
   * queries (it returns `false` for write queries).
   */
  bool has_results() const;

  /** Initializes the query. */
  Status init();

  /** Returns the first fragment uri. */
  URI first_fragment_uri() const;

  /** Returns the last fragment uri. */
  URI last_fragment_uri() const;

  /** Returns the cell layout. */
  Layout layout() const;

  /** Processes a query. */
  Status process();

  /** Returns the Reader. */
  const Reader* reader() const;

  /** Returns the Reader. */
  Reader* reader();

  /** Returns the Writer. */
  const Writer* writer() const;

  /** Returns the Writer. */
  Writer* writer();

  /**
   * Disables checking the global order. Applicable only to writes.
   * This option will supercede the config.
   */
  Status disable_check_global_order();

  /**
   * Sets the config for the Query
   *
   * This function overrides the config for Query-level parameters only.
   * Semantically, the initial query config is copied from the context
   * config upon initialization. Note that The context config is immutable
   * at the C API level because tiledb_ctx_get_config always returns a copy.
   *
   * Config parameters set here will *only* be applied within the Query.
   */
  Status set_config(const Config& config);

  /**
   * Sets the buffer for a fixed-sized attribute/dimension.
   *
   * @param name The attribute/dimension to set the buffer for.
   * @param buffer The buffer that either have the input data to be written,
   *     or will hold the data to be read.
   * @param buffer_size In the case of writes, this is the size of `buffer`
   *     in bytes. In the case of reads, this initially contains the allocated
   *     size of `buffer`, but after the termination of the query
   *     it will contain the size of the useful (read) data in `buffer`.
   * @param check_null_buffers If true (default), null buffers are not allowed.
   * @return Status
   */
  Status set_buffer(
      const std::string& name,
      void* buffer,
      uint64_t* buffer_size,
      bool check_null_buffers = true);

  /**
   * Sets the buffer for a var-sized attribute/dimension.
   *
   * @param name The attribute/dimension to set the buffer for.
   * @param buffer_off The buffer that either have the input data to be written,
   *     or will hold the data to be read. This buffer holds the starting
   *     offsets of each cell value in `buffer_val`.
   * @param buffer_off_size In the case of writes, it is the size of
   *     `buffer_off` in bytes. In the case of reads, this initially contains
   *     the allocated size of `buffer_off`, but after the termination of the
   *     function it will contain the size of the useful (read) data in
   *     `buffer_off`.
   * @param buffer_val The buffer that either have the input data to be written,
   *     or will hold the data to be read. This buffer holds the actual
   *     var-sized cell values.
   * @param buffer_val_size In the case of writes, it is the size of
   *     `buffer_val` in bytes. In the case of reads, this initially contains
   *     the allocated size of `buffer_val`, but after the termination of the
   *     query it will contain the size of the useful (read) data in
   *     `buffer_val`.
   * @param check_null_buffers If true (default), null buffers are not allowed.
   * @return Status
   */
  Status set_buffer(
      const std::string& name,
      uint64_t* buffer_off,
      uint64_t* buffer_off_size,
      void* buffer_val,
      uint64_t* buffer_val_size,
      bool check_null_buffers = true);

  /**
   * Sets the buffer for a fixed-sized, nullable attribute with a validity
   * bytemap.
   *
   * @param name The attribute to set the buffer for.
   * @param buffer The buffer that either have the input data to be written,
   *     or will hold the data to be read.
   * @param buffer_size In the case of writes, this is the size of `buffer`
   *     in bytes. In the case of reads, this initially contains the allocated
   *     size of `buffer`, but after the termination of the query
   *     it will contain the size of the useful (read) data in `buffer`.
   * @param buffer_validity_bytemap The buffer that either have the validity
   * bytemap associated with the input data to be written, or will hold the
   * validity bytemap to be read.
   * @param buffer_validity_bytemap_size In the case of writes, this is the size
   * of `buffer_validity_bytemap` in bytes. In the case of reads, this initially
   *     contains the allocated size of `buffer_validity_bytemap`, but after the
   *     termination of the query it will contain the size of the useful (read)
   * data in `buffer_validity_bytemap`.
   * @param check_null_buffers If true (default), null buffers are not allowed.
   * @return Status
   */
  Status set_buffer_vbytemap(
      const std::string& name,
      void* buffer,
      uint64_t* buffer_size,
      uint8_t* buffer_validity_bytemap,
      uint64_t* buffer_validity_bytemap_size,
      bool check_null_buffers = true);

  /**
   * Sets the buffer for a var-sized, nullable attribute with a validity
   * bytemap.
   *
   * @param name The attribute to set the buffer for.
   * @param buffer_off The buffer that either have the input data to be written,
   *     or will hold the data to be read. This buffer holds the starting
   *     offsets of each cell value in `buffer_val`.
   * @param buffer_off_size In the case of writes, it is the size of
   *     `buffer_off` in bytes. In the case of reads, this initially contains
   *     the allocated size of `buffer_off`, but after the termination of the
   *     function it will contain the size of the useful (read) data in
   *     `buffer_off`.
   * @param buffer_val The buffer that either have the input data to be written,
   *     or will hold the data to be read. This buffer holds the actual
   *     var-sized cell values.
   * @param buffer_val_size In the case of writes, it is the size of
   *     `buffer_val` in bytes. In the case of reads, this initially contains
   *     the allocated size of `buffer_val`, but after the termination of the
   *     query it will contain the size of the useful (read) data in
   *     `buffer_val`.
   * @param buffer_validity_bytemap The buffer that either have the validity
   * bytemap associated with the input data to be written, or will hold the
   * validity bytemap to be read.
   * @param buffer_validity_bytemap_size In the case of writes, this is the size
   * of `buffer_validity_bytemap` in bytes. In the case of reads, this initially
   *     contains the allocated size of `buffer_validity_bytemap`, but after the
   *     termination of the query it will contain the size of the useful (read)
   * data in `buffer_validity_bytemap`.
   * @param check_null_buffers If true (default), null buffers are not allowed.
   * @return Status
   */
  Status set_buffer_vbytemap(
      const std::string& name,
      uint64_t* buffer_off,
      uint64_t* buffer_off_size,
      void* buffer_val,
      uint64_t* buffer_val_size,
      uint8_t* buffer_validity_bytemap,
      uint64_t* buffer_validity_bytemap_size,
      bool check_null_buffers = true);

  /**
   * Used by serialization to set the estimated result size
   *
   * @param est_result_size map to set
   * @param max_mem_size map to set
   * @return Status
   */
  Status set_est_result_size(
      std::unordered_map<std::string, Subarray::ResultSize>& est_result_size,
      std::unordered_map<std::string, Subarray::MemorySize>& max_mem_size);

  /**
   * Sets the cell layout of the query. The function will return an error
   * if the queried array is a key-value store (because it has its default
   * layout for both reads and writes.
   */
  Status set_layout(Layout layout);

  /**
   * Sets the condition for filtering results in a read query.
   *
   * @param condition The condition object.
   * @return Status
   */
  Status set_condition(const QueryCondition& condition);

  /**
   * This is applicable only to dense arrays (errors out for sparse arrays),
   * and only in the case where the array is opened in a way that all its
   * fragments are sparse. Also it is only applicable to read queries.
   * If the input is `true`, then the dense array will be read in
   * "sparse mode", i.e., the sparse read algorithm will be executing,
   * returning results only for the non-empty cells.
   *
   * @param sparse_mode This sets the sparse mode.
   * @return Status
   */
  Status set_sparse_mode(bool sparse_mode);

  /**
   * Set query status, needed for json deserialization
   * @param status
   * @return Status
   */
  void set_status(QueryStatus status);

  /**
   * Sets the query subarray. If it is null, then the subarray will be set to
   * the entire domain.
   *
   * @param subarray The subarray to be set.
   * @return Status
   *
   * @note Setting a subarray for sparse arrays, or for dense arrays
   *     when performing unordered (sparse) writes, has no effect
   *     (will be ingnored).
   */
  Status set_subarray(const void* subarray);

  /** Sets the query subarray, without performing any checks. */
  Status set_subarray_unsafe(const NDRange& subarray);

  /** Submits the query to the storage manager. */
  Status submit();

  /**
   * Submits the query to the storage manager. The query will be
   * processed asynchronously (i.e., in a non-blocking manner).
   * Once the query is completed, the input callback function will
   * be executed using the input callback data.
   */
  Status submit_async(std::function<void(void*)> callback, void* callback_data);

  /** Returns the query status. */
  QueryStatus status() const;

  /** Returns the query type. */
  QueryType type() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array the query is associated with. */
  Array* array_;

  /** A function that will be called upon the completion of an async query. */
  std::function<void(void*)> callback_;

  /** The data input to the callback function. */
  void* callback_data_;

  /** The layout of the cells in the result of the subarray. */
  Layout layout_;

  /** The query status. */
  QueryStatus status_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The query type. */
  QueryType type_;

  /** Query reader. */
  Reader reader_;

  /** Query writer. */
  Writer writer_;

  /** The current serialization state. */
  SerializationState serialization_state_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  Status check_set_fixed_buffer(const std::string& name);

  /**
   * Internal routine for setting fixed-sized, nullable attribute buffers with
   * a ValidityVector.
   */
  Status set_buffer(
      const std::string& name,
      void* buffer,
      uint64_t* buffer_size,
      ValidityVector&& validity_vector,
      bool check_null_buffers = true);

  /**
   * Internal routine for setting var-sized, nullable attribute buffers with
   * a ValidityVector.
   */
  Status set_buffer(
      const std::string& name,
      uint64_t* buffer_off,
      uint64_t* buffer_off_size,
      void* buffer_val,
      uint64_t* buffer_val_size,
      ValidityVector&& validity_vector,
      bool check_null_buffers = true);

  /**
   * Internal routine for getting fixed-sized, nullable attribute buffers with
   * a ValidityVector.
   */
  Status get_buffer(
      const char* name,
      void** buffer,
      uint64_t** buffer_size,
      const ValidityVector** validity_vector) const;

  /**
   * Internal routine for getting fixed-sized, nullable attribute buffers with
   * a ValidityVector.
   */
  Status get_buffer(
      const char* name,
      uint64_t** buffer_off,
      uint64_t** buffer_off_size,
      void** buffer_val,
      uint64_t** buffer_val_size,
      const ValidityVector** validity_vector) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_H
