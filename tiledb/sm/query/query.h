/**
 * @file   query.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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

#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/dense_cell_range_iter.h"
#include "tiledb/sm/query/reader.h"
#include "tiledb/sm/query/writer.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/subarray.h"

#include <functional>
#include <sstream>
#include <utility>
#include <vector>

namespace tiledb {
namespace sm {

class Array;

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
      /** Value holding the length of the fixed-length data. */
      uint64_t fixed_len_size = 0;
      /** Value holding the length of the variable-length data. */
      uint64_t var_len_size = 0;
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
   * Adds a range to the (read/write) query on the input dimension,
   * in the form of (start, end, stride).
   * The range components must be of the same type as the domain type of the
   * underlying array.
   */
  Status add_range(
      unsigned dim_idx, const void* start, const void* end, const void* stride);

  /** Retrieves the number of ranges of the subarray for the given dimension. */
  Status get_range_num(unsigned dim_idx, uint64_t* range_num) const;

  /**
   * Retrieves a range from a dimension in the form (start, end, stride).
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
   * Gets the estimated result size (in bytes) for the input fixed-sized
   * attribute.
   */
  Status get_est_result_size(const char* attr_name, uint64_t* size);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized
   * attribute.
   */
  Status get_est_result_size(
      const char* attr_name, uint64_t* size_off, uint64_t* size_val);

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

  /**
   * Return vector of attributes
   * @return attributes for query
   */
  std::vector<std::string> attributes() const;

  /** Gets the AttributeBuffer for an attribute. */
  AttributeBuffer attribute_buffer(const std::string& attribute_name) const;

  /**
   * Marks a query that has not yet been started as failed. This should not be
   * called asynchronously to cancel an in-progress query; for that use the
   * parent StorageManager's cancellation mechanism.
   * @return Status
   */
  Status cancel();

  /**
   * Check the validity of the provided buffer offsets for a variable attribute.
   *
   * @param buffer_off Offset buffer
   * @param buffer_off_size Pointer to size of offset buffer
   * @param buffer_val_size Pointer to size of data buffer
   * @return Status
   */
  static Status check_var_attr_offsets(
      const uint64_t* buffer_off,
      const uint64_t* buffer_off_size,
      const uint64_t* buffer_val_size);

  /**
   * Finalizes the query, flushing all internal state. Applicable only to global
   * layout writes. It has no effect for any other query type.
   */
  Status finalize();

  /**
   * Retrieves the buffer of a fixed-sized attribute.
   *
   * @param attribute The buffer attribute. An empty string means
   *     the special default attribute.
   * @param buffer The buffer to be retrieved.
   * @param buffer_size A pointer to the buffer size to be retrieved.
   * @return Status
   */
  Status get_buffer(
      const char* attribute, void** buffer, uint64_t** buffer_size) const;

  /**
   * Retrieves the offsets and values buffers of a var-sized attribute.
   *
   * @param attribute The buffer attribute.
   * @param buffer_off The offsets buffer to be retrieved.
   * @param buffer_off_size A pointer to the offsets buffer size to be
   * retrieved.
   * @param buffer_val The values buffer to be retrieved.
   * @param buffer_val_size A pointer to the values buffer size to be retrieved.
   * @return Status
   */
  Status get_buffer(
      const char* attribute,
      uint64_t** buffer_off,
      uint64_t** buffer_off_size,
      void** buffer_val,
      uint64_t** buffer_val_size) const;

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
   * Sets the buffer for a fixed-sized attribute.
   *
   * @param attribute The attribute to set the buffer for.
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
      const std::string& attribute,
      void* buffer,
      uint64_t* buffer_size,
      bool check_null_buffers = true);

  /**
   * Sets the buffer for a var-sized attribute.
   *
   * @param attribute The attribute to set the buffer for.
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
      const std::string& attribute,
      uint64_t* buffer_off,
      uint64_t* buffer_off_size,
      void* buffer_val,
      uint64_t* buffer_val_size,
      bool check_null_buffers = true);

  /**
   * Sets the cell layout of the query. The function will return an error
   * if the queried array is a key-value store (because it has its default
   * layout for both reads and writes.
   */
  Status set_layout(Layout layout);

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
   * @param check_expanded_domain If `true`, the subarray bounds will be
   *     checked against the expanded domain of the array. This is important
   *     in dense consolidation with space tiles not fully dividing the
   *     dimension domain.
   * @return Status
   */
  Status set_subarray(const void* subarray, bool check_expanded_domain = false);

  /**
   * Sets the query subarray.
   *
   * @param subarray The subarray to be set.
   * @return Status
   */
  Status set_subarray(const Subarray& subarray);

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

  /**
   * Correctness checks on `subarray`.
   *
   * @param subarray The subarray to be checked
   * @param check_expanded_domain If `true`, the subarray bounds will be
   *     checked against the expanded domain of the array. This is important
   *     in dense consolidation with space tiles not fully dividing the
   *     dimension domain.
   */
  Status check_subarray(
      const void* subarray, bool check_expanded_domain = false) const;

  /**
   * Correctness checks on `subarray`.
   *
   * @param subarray The subarray to be checked
   * @param check_expanded_domain If `true`, the subarray bounds will be
   *     checked against the expanded domain of the array. This is important
   *     in dense consolidation with space tiles not fully dividing the
   *     dimension domain.
   */
  template <
      typename T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  Status check_subarray(const T* subarray, bool check_expanded_domain) const {
    auto array_schema = this->array_schema();
    auto domain = array_schema->domain();
    auto dim_num = domain->dim_num();

    // Check subarray bounds
    return check_subarray_bounds(
        subarray, domain, dim_num, check_expanded_domain);
  }

  template <
      typename T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  Status check_subarray(const T* subarray, bool check_expanded_domain) const {
    (void)check_expanded_domain;  // Non-applicable to real domains
    auto array_schema = this->array_schema();
    auto domain = array_schema->domain();
    auto dim_num = domain->dim_num();

    // Check for NaN
    for (unsigned int i = 0; i < dim_num; ++i) {
      if (std::isnan(subarray[2 * i]) || std::isnan(subarray[2 * i + 1]))
        return LOG_STATUS(Status::QueryError("Subarray contains NaN"));
    }

    // Check subarray bounds
    return check_subarray_bounds(subarray, domain, dim_num);
  }

  /**
   * Checks that the subarray bounds are contained within the domain dimensions.
   *
   * @param subarray The subarray to check
   * @param domain the domain of the subarray
   * @param dim_num the number of dimensions in the subarray and domain
   * @param check_expanded_domain If `true`, the subarray bounds will be
   *     checked against the expanded domain of the array. This is important
   *     in dense consolidation with space tiles not fully dividing the
   *     dimension domain.
   * @return Status
   */
  template <typename T>
  Status check_subarray_bounds(
      const T* subarray,
      const Domain* const domain,
      const unsigned int dim_num,
      bool check_expanded_domain = false) const {
    T low, high;
    for (unsigned int i = 0; i < dim_num; ++i) {
      auto dim_domain = static_cast<const T*>(domain->dimension(i)->domain());
      if (array_schema()->dense() && check_expanded_domain) {
        auto tile_extent =
            *static_cast<const T*>(domain->dimension(i)->tile_extent());
        low = dim_domain[0];
        high = ((((dim_domain[1] - dim_domain[0]) / tile_extent) + 1) *
                tile_extent) -
               1 + dim_domain[0];
      } else {
        low = dim_domain[0];
        high = dim_domain[1];
      }

      if (subarray[2 * i] < low || subarray[2 * i + 1] > high)
        return LOG_STATUS(Status::QueryError(
            "Subarray out of bounds. " +
            format_subarray_bounds(subarray, domain, dim_num)));
      if (subarray[2 * i] > subarray[2 * i + 1])
        return LOG_STATUS(Status::QueryError(
            "Subarray lower bound is larger than upper bound. " +
            format_subarray_bounds(subarray, domain, dim_num)));
    }

    return Status::Ok();
  }

  /**
   * Returns a formatted string containing the subarray bounds and domain
   * dimension bounds. For example:
   * "subarray: [1, 2, 1, 2] domain: [1, 4, 1, 4]"
   *
   * @param subarray The subarray to format from
   * @param domain the domain of the subarray
   * @param dim_num the number of dimensions in the subarray and domain
   * @return string
   */
  template <typename T>
  std::string format_subarray_bounds(
      const T* subarray,
      const Domain* const domain,
      const unsigned int dim_num) const {
    std::stringstream subarray_ss;
    std::stringstream domain_ss;

    subarray_ss << "subarray: [";
    domain_ss << "domain: [";

    for (unsigned int i = 0; i < dim_num; ++i) {
      auto dim_domain = static_cast<const T*>(domain->dimension(i)->domain());

      if (i != 0) {
        subarray_ss << ", ";
        domain_ss << ", ";
      }

      subarray_ss << subarray[2 * i] << ", " << subarray[2 * i + 1];

      domain_ss << dim_domain[0] << ", " << dim_domain[1];
    }

    subarray_ss << "]";
    domain_ss << "]";

    subarray_ss << " " << domain_ss.str();

    return subarray_ss.str();
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_H
