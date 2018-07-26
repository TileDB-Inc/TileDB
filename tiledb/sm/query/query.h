/**
 * @file   query.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include <functional>
#include <vector>

namespace tiledb {
namespace sm {

/** Processes a (read/write) query. */
class Query {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Consturctor */
  Query() = default;

  /** Constructor. */
  Query(
      StorageManager* storage_manager,
      QueryType type,
      const ArraySchema* array_schema,
      const std::vector<FragmentMetadata*>& fragment_metadata);

  /** Destructor. */
  ~Query();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /**
   * Return vector of attributes
   * @return attributes for query
   */
  std::vector<std::string> attributes() const;

  /**
   * Return all attribute buffers
   * @return unordered_map of attribute buffers
   */
  std::unordered_map<std::string, AttributeBuffer> attribute_buffers() const;

  /**
   * Return a copy of the buffer for a given attribute
   * @tparam T
   * @param attribute name of buffer to retrieve
   * @return a std::pair which contains two pairs. First pair is a pointer to
   * the value buffer and its size. The second pair is a pointer to the offset
   * buffer and its size. Note the first pair (the value buffer) is of type T
   * where the second pair (the offset buffer) is always type uint64_t
   */
  template <class T>
  std::pair<std::pair<T*, uint64_t>, std::pair<uint64_t*, uint64_t>> buffer(
      const std::string& attribute) const {
    AttributeBuffer buffer;
    if (type_ == QueryType::WRITE) {
      buffer = writer_.buffer(attribute);
    } else {  // READ
      buffer = reader_.buffer(attribute);
    }

    // Check if buffer is null
    if (buffer.buffer_ == nullptr || buffer.buffer_size_ == nullptr) {
      LOG_STATUS(
          Status::QueryError("Cannot get buffer; buffer attribute is nullptr"));
      return {{nullptr, 0}, {nullptr, 0}};
    }

    // Get array schema, used for validating template type vs attribute datatype
    auto array_schema = this->array_schema();
    if (array_schema == nullptr) {
      LOG_STATUS(Status::QueryError("Cannot get buffer; Array schema not set"));
      return {{nullptr, 0}, {nullptr, 0}};
    }

    // Get attribute object
    auto attr = array_schema->attribute(attribute);
    if (attr == nullptr) {
      LOG_STATUS(Status::QueryError(
          "Cannot get buffer; Attribute from Array schema is nullptr"));
      return {{nullptr, 0}, {nullptr, 0}};
    }

    // Validate template type matches attribute datatype
    if (!tiledb::sm::utils::check_template_type_to_datatype<T>(attr->type())
             .ok()) {
      LOG_STATUS(Status::QueryError("Template does not match attribute type"));
      return {{nullptr, 0}, {nullptr, 0}};
    }

    std::pair<uint64_t*, uint64_t> offset_buffer;
    if (buffer.buffer_var_ != nullptr && buffer.buffer_var_size_ != nullptr)
      offset_buffer = std::pair<uint64_t*, uint64_t>(
          static_cast<uint64_t*>(buffer.buffer_var_),
          *buffer.buffer_var_size_ / sizeof(uint64_t));
    // Return pair of pairs with value and offset buffers
    return std::pair<std::pair<T*, uint64_t>, std::pair<uint64_t*, uint64_t>>(
        std::pair<T*, uint64_t>(
            static_cast<T*>(buffer.buffer_),
            *buffer.buffer_size_ / datatype_size(attr->type())),
        offset_buffer);
  };

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
   * Copy all buffers from one query object to calling query object
   * @param query
   * @return status object, Ok() on success, error on failure of copying
   */
  Status copy_buffers(const Query& query);

  /**
   * WIP function for copying data from a partial json object into current query
   * object. The reason for needing this is when we de-serialize a json object
   * ot a query object it is not a valid object since there is no storage
   * manager and no open array. We have no storage manager because inside the
   * nlohmann namespace we have no context to build the object. This will be
   * true for ceral and other json libraries that could be used.
   * @param query
   * @return Status OK on success, Error if there is a failure
   */
  Status copy_json_wip(const Query& query);

  /**
   * Finalizes the query, flushing all internal state. Applicable only to global
   * layout writes. It has no effect for any other query type.
   */
  Status finalize();

  /** Returns the number of fragments involved in the (read) query. */
  unsigned fragment_num() const;

  /** Returns a vector with the fragment URIs. */
  std::vector<URI> fragment_uris() const;

  /**
   * Returns `true` if the query has results. Applicable only to read
   * queries (it returns `false` for write queries).
   */
  bool has_results() const;

  /** Initializes the query. */
  Status init();

  /** Returns the last fragment uri. */
  URI last_fragment_uri() const;

  /** Returns the cell layout. */
  Layout layout() const;

  /** Processes a query. */
  Status process();

  /**
   * Sets the buffer for a fixed-sized attribute.
   *
   * @param attribute The attribute to set the buffer for.
   * @param buffer The buffer that either have the input data to be written,
   *     or will hold the data to be read.
   * @param buffer_size In the case of writes, this is the size of `buffer`
   *     in bytes. In the case of reads, this initially contains the allocated
   *     size of `buffer`, but after the termination of the function
   *     it will contain the size of the useful (read) data in `buffer`.
   * @return Status
   */
  Status set_buffer(
      const std::string& attribute, void* buffer, uint64_t* buffer_size);

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
   *     function it will contain the size of the useful (read) data in
   *     `buffer_val`.
   * @return Status
   */
  Status set_buffer(
      const std::string& attribute,
      uint64_t* buffer_off,
      uint64_t* buffer_off_size,
      void* buffer_val,
      uint64_t* buffer_val_size);

  /**
   * Sets the callback function and its data input that will be called
   * upon the completion of an asynchronous query.
   */
  void set_callback(
      const std::function<void(void*)>& callback, void* callback_data);

  /** Sets the fragment URI. Applicable only to write queries. */
  void set_fragment_uri(const URI& fragment_uri);

  /**
   * Sets the cell layout of the query. The function will return an error
   * if the queried array is a key-value store (because it has its default
   * layout for both reads and writes.
   */
  Status set_layout(Layout layout);

  /**
   * Sets the query subarray. If it is null, then the subarray will be set to
   * the entire domain.
   *
   * @param subarray The subarray to be set.
   * @return Status
   */
  Status set_subarray(const void* subarray);

  /**
   * Return the query subarray
   * @tparam T
   * @return vector containing copy of the subarray
   */
  template <class T>
  std::vector<T> subarray() const {
    void* subarray = nullptr;
    if (type_ == QueryType::WRITE) {
      subarray = writer_.subarray();
    } else {  // READ
      subarray = reader_.subarray();
    }

    if (subarray == nullptr) {
      LOG_STATUS(
          Status::QueryError("Cannot get subarray; subarray is nullptr"));
      return {};
    }

    auto array_schema = this->array_schema();
    if (array_schema == nullptr) {
      LOG_STATUS(
          Status::QueryError("Cannot get subarray; Array schema not set"));
      return {};
    }

    // Get subarray size
    uint64_t subarray_size = 2 * array_schema->coords_size();
    uint64_t subarray_length =
        subarray_size / datatype_size(array_schema->coords_type());

    // Check template matches datatype
    if (!tiledb::sm::utils::check_template_type_to_datatype<T>(
             array_schema->domain()->type())
             .ok())
      return {};
    // Return a copy of the subarray using the iterator constructor for vector
    // casting to templated type The templated type has already been validated
    // in the above switch statement
    return std::vector<T>(
        static_cast<T*>(subarray), static_cast<T*>(subarray) + subarray_length);
  };

  /** Returns the query status. */
  QueryStatus status() const;

  /** Returns the query type. */
  QueryType type() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** A function that will be called upon the completion of an async query. */
  std::function<void(void*)> callback_;

  /** The data input to the callback function. */
  void* callback_data_;

  /** The layout of the cells in the result of the subarray. */
  Layout layout_;

  /** The query status. */
  QueryStatus status_;

  /** The query type. */
  QueryType type_;

  /** Query reader. */
  Reader reader_;

  /** Wuery writer. */
  Writer writer_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Correctness checks on the bounds of `subarray`. */
  Status check_subarray_bounds(const void* subarray) const;

  /** Correctness checks for `subarray`. */
  template <class T>
  Status check_subarray_bounds(const T* subarray) const;

  /** Sets the array schema. */
  void set_array_schema(const ArraySchema* array_schema);

  /** Sets the fragment metadata. */
  void set_fragment_metadata(
      const std::vector<FragmentMetadata*>& fragment_metadata);

  /** Sets the storage manager. */
  void set_storage_manager(StorageManager* storage_manager);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_H
