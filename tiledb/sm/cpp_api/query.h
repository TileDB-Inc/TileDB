/**
 * @file   tiledb_cpp_api_query.h
 *
 * @author Ravi Gaddipati
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
 * This file declares the C++ API for the TileDB Query object.
 */

#ifndef TILEDB_CPP_API_QUERY_H
#define TILEDB_CPP_API_QUERY_H

#include "array.h"
#include "array_schema.h"
#include "context.h"
#include "core_interface.h"
#include "deleter.h"
#include "exception.h"
#include "tiledb.h"
#include "type.h"
#include "utils.h"

#include <algorithm>
#include <cassert>
#include <functional>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace tiledb {

/**
 * Construct and execute read/write queries on a tiledb::Array.
 *
 * @details
 * See examples for more usage details.
 *
 * **Example:**
 *
 * @code{.cpp}
 * Query query(ctx, "my_dense_array", TILEDB_WRITE);
 * query.set_layout(TILEDB_GLOBAL_ORDER);
 * std::vector a1_data = {1, 2, 3};
 * query.set_buffer("a1", a1_data);
 * query.submit();
 * @endcode
 */
class Query {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** The query or query attribute status. */
  enum class Status { FAILED, COMPLETE, INPROGRESS, INCOMPLETE, UNINITIALIZED };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Creates a TileDB query object.
   *
   * When creating a query, the storage manager "opens" the array in read or
   * write mode based on the query type, incrementing the array's reference
   * count and loading the array metadata (schema and fragment metadata) into
   * its main-memory cache.
   *
   * The storage manager also acquires a **shared lock** on the array. This
   * means multiple read and write queries to the same array can be made
   * concurrently (in TileDB, only consolidation requires an exclusive lock for
   * a short period of time).
   *
   * To "close" an array, the user should call `Query::finalize()` (see the
   * documentation of that function for more details).
   *
   * @param ctx TileDB context
   * @param array_uri Array URI
   * @param type The TileDB query type
   */
  Query(const Context& ctx, const Array& array, tiledb_query_type_t type)
      : ctx_(ctx)
      , schema_(ctx, array.uri())
      , uri_(array.uri()) {
    tiledb_query_t* q;
    ctx.handle_error(tiledb_query_alloc(ctx, array, type, &q));
    query_ = std::shared_ptr<tiledb_query_t>(q, deleter_);
  }

  Query(const Query&) = default;
  Query(Query&& o) = default;
  Query& operator=(const Query&) = default;
  Query& operator=(Query&& o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the query type. */
  tiledb_query_type_t query_type() const {
    auto& ctx = ctx_.get();
    tiledb_query_type_t query_type;
    ctx.handle_error(tiledb_query_get_type(ctx, query_.get(), &query_type));
    return query_type;
  }

  /** Sets the data layout of the buffers.  */
  Query& set_layout(tiledb_layout_t layout) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_set_layout(ctx, query_.get(), layout));
    return *this;
  }

  /** Returns the query status. */
  Status query_status() const {
    tiledb_query_status_t status;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_get_status(ctx, query_.get(), &status));
    return to_status(status);
  }

  /** Submits the query. Call will block until query is complete. */
  Status submit() {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_submit(ctx, query_.get()));
    return query_status();
  }

  /**
   * Submit an async query, with callback.
   *
   * @param callback Callback function.
   * @param data Data to pass to callback.
   * @return Status of submitted query.
   */
  template <typename Fn>
  void submit_async(const Fn& callback) {
    std::function<void(void*)> wrapper = [&](void*) { callback(); };
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb::impl::tiledb_query_submit_async_func(
        ctx, query_.get(), &wrapper, nullptr));
  }

  /** Submit an async query (non-blocking). */
  void submit_async() {
    submit_async([]() {});
  }

  /**
   * Flushes all internal state of a query object and finalizes the query.
   * This is applicable only to global layout writes. It has no effect for
   * any other query type.
   */
  void finalize() {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_finalize(ctx, query_.get()));
  }

  /**
   * Returns the number of elements in the result buffers. This is a map
   * from the attribute name to a pair of values.
   *
   * The first is number of elements for var size attributes, and the second
   * is number of elements in the data buffer. For fixed sized attributes
   * (and coordinates), the first is always 0.
   *
   * If the query has not been submitted, an empty map is returned.
   */
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
  result_buffer_elements() const {
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> elements;
    if (buff_sizes_.size() == 0)
      return {};  // Query hasn't been submitted
    for (const auto& b_it : buff_sizes_) {
      auto attr_name = b_it.first;
      auto size_pair = b_it.second;
      auto var =
          (attr_name != TILEDB_COORDS &&
           schema_.attribute(attr_name).cell_val_num() == TILEDB_VAR_NUM);
      auto element_size = element_sizes_.find(attr_name)->second;
      elements[attr_name] = (var) ? std::pair<uint64_t, uint64_t>(
                                        size_pair.first / sizeof(uint64_t),
                                        size_pair.second / element_size) :
                                    std::pair<uint64_t, uint64_t>(
                                        0, size_pair.second / element_size);
    }
    return elements;
  }

  /**
   * Sets a subarray, defined in the order dimensions were added.
   * Coordinates are inclusive.
   *
   * @note set_subarray(std::vector) is preferred as it is safer.
   *
   * @tparam T Array domain datatype
   * @param pairs Subarray pointer defined as an array of [start, stop] values
   * per dimension.
   * @param size The number of subarray elements.
   */
  template <typename T = uint64_t>
  void set_subarray(const T* pairs, uint64_t size) {
    impl::type_check<T>(schema_.domain().type());
    auto& ctx = ctx_.get();
    if (size != schema_.domain().ndim() * 2) {
      throw SchemaMismatch(
          "Subarray should have num_dims * 2 values: (low, high) for each "
          "dimension.");
    }
    ctx.handle_error(tiledb_query_set_subarray(ctx, query_.get(), pairs));
    subarray_cell_num_ = pairs[1] - pairs[0] + 1;
    for (unsigned i = 2; i < size - 1; i += 2) {
      subarray_cell_num_ *= (pairs[i + 1] - pairs[i] + 1);
    }
  }

  /**
   * Sets a subarray, defined in the order dimensions were added.
   * Coordinates are inclusive.
   *
   * @tparam T Array domain datatype.  Should always be a vector of the domain
   * type.
   * @param pairs The subarray defined as a vector of [start, stop] coordinates
   * per dimension.
   */
  template <typename Vec>
  void set_subarray(const Vec& pairs) {
    set_subarray(pairs.data(), pairs.size());
  }

  /**
   * Sets a subarray, defined in the order dimensions were added.
   * Coordinates are inclusive.
   *
   * @tparam T Array domain datatype.  Should always be a vector of the domain
   * type.
   * @param pairs List of [start, stop] coordinates per dimension.
   */
  template <typename T = uint64_t>
  void set_subarray(const std::initializer_list<T>& l) {
    set_subarray(std::vector<T>(l));
  }

  /**
   * Sets a subarray, defined in the order dimensions were added.
   * Coordinates are inclusive.
   *
   * @note set_subarray(std::vector) is preferred and avoids an extra copy.
   *
   * @tparam T Array domain datatype.  Should always be a vector of the domain
   * type.
   * @param pairs The subarray defined as pairs of [start, stop] per dimension.
   */
  template <typename T = uint64_t>
  void set_subarray(const std::vector<std::array<T, 2>>& pairs) {
    std::vector<T> buf;
    buf.reserve(pairs.size() * 2);
    std::for_each(
        pairs.begin(), pairs.end(), [&buf](const std::array<T, 2>& p) {
          buf.push_back(p[0]);
          buf.push_back(p[1]);
        });
    set_subarray(buf);
  }

  /** Set the coordinate buffer for unordered queries
   *
   * @note set_coordinates(std::vector) is preferred as it is safer.
   *
   * @tparam T Array domain datatype
   * @param buf Coordinate array buffer pointer
   * @param size The number of elements in the coordinate array buffer
   * **/
  template <typename T>
  void set_coordinates(T* buf, uint64_t size) {
    set_buffer(TILEDB_COORDS, buf, size);
  }

  /** Set the coordinate buffer for unordered queries
   *
   * @tparam Vec Array domain datatype. Should always be a vector of the domain
   * type.
   * @param buf Coordinate vector
   * **/
  template <typename Vec>
  void set_coordinates(Vec& buf) {
    set_coordinates(buf.data(), buf.size());
  }

  /**
   * Sets a buffer for a fixed-sized attribute.
   *
   * @note set_buffer(std::string, std::vector) is preferred as it is safer.
   *
   * @tparam Vec buffer. Should always be a vector type of the attribute type.
   * @param attr Attribute name
   * @param buff Buffer array pointer with elements of the attribute type.
   * @param nelements Number of array elements
   **/
  template <typename T>
  void set_buffer(const std::string& attr, T* buff, uint64_t nelements) {
    auto ctx = ctx_.get();
    auto size = nelements * sizeof(T);
    buff_sizes_[attr] = std::pair<uint64_t, uint64_t>(0, size);
    element_sizes_[attr] = sizeof(T);
    ctx.handle_error(tiledb_query_set_buffer(
        ctx, query_.get(), attr.c_str(), buff, &(buff_sizes_[attr].second)));
  }

  /**
   * Sets a buffer for a fixed-sized attribute.
   *
   * @tparam Vec buffer. Should always be a vector type of the attribute type.
   * @param attr Attribute name
   * @param buf Buffer vector with elements of the attribute type.
   **/
  template <typename Vec>
  void set_buffer(const std::string& attr, Vec& buf) {
    return set_buffer(attr, buf.data(), buf.size());
  }

  /**
   * Sets a buffer for a variable-sized attribute.
   *
   * @note set_buffer(std::string, std::vector, std::vector) is preferred as it
   *is safer.
   *
   * @tparam Vec buffer type. Should always be a vector of the attribute type.
   * @param attr Attribute name
   * @param offsets Offsets array pointer where a new element begins in the data
   *buffer.
   * @param offsets_nelements Number of elements in offsets buffer.
   * @param data Buffer array pointer with elements of the attribute type.
   *        For variable sized attributes, the buffer should be flattened.
   * @param data_nelements Number of array elements in data buffer.
   **/
  template <typename T>
  void set_buffer(
      const std::string& attr,
      uint64_t* offsets,
      uint64_t offset_nelements,
      T* data,
      uint64_t data_nelements) {
    auto ctx = ctx_.get();
    auto data_size = data_nelements * sizeof(T);
    auto offset_size = offset_nelements * sizeof(uint64_t);
    element_sizes_[attr] = sizeof(T);
    buff_sizes_[attr] = std::pair<uint64_t, uint64_t>(offset_size, data_size);
    ctx.handle_error(tiledb_query_set_buffer_var(
        ctx,
        query_.get(),
        attr.c_str(),
        offsets,
        &(buff_sizes_[attr].first),
        (void*)data,
        &(buff_sizes_[attr].second)));
  }

  /**
   * Sets a buffer for a variable-sized attribute.
   *
   * @tparam Vec buffer type. Should always be a vector of the attribute type.
   * @param attr Attribute name
   * @param offsets Offsets where a new element begins in the data buffer.
   * @param data Buffer vector with elements of the attribute type.
   *        For variable sized attributes, the buffer should be flattened. E.x.
   *        an attribute of type std::string should have a buffer Vec type of
   *        std::string, where the values of each cell are concatenated.
   **/
  template <typename Vec>
  void set_buffer(
      const std::string& attr, std::vector<uint64_t>& offsets, Vec& data) {
    set_buffer(attr, offsets.data(), offsets.size(), &data[0], data.size());
  }

  /**
   * Sets a buffer for a variable-sized attribute.
   *
   * @tparam Vec buffer type. Should always be a vector of the attribute type.
   * @param attr Attribute name
   * @param buf pair of offset, data buffers
   **/
  template <typename Vec>
  void set_buffer(
      const std::string& attr, std::pair<std::vector<uint64_t>, Vec>& buf) {
    set_buffer(attr, buf.first, buf.second);
  }

  /* ********************************* */
  /*         STATIC FUNCTIONS          */
  /* ********************************* */

  /** Converts the TileDB C query status to a C++ query status. */
  static Status to_status(const tiledb_query_status_t& status) {
    switch (status) {
      case TILEDB_INCOMPLETE:
        return Status::INCOMPLETE;
      case TILEDB_COMPLETED:
        return Status::COMPLETE;
      case TILEDB_INPROGRESS:
        return Status::INPROGRESS;
      case TILEDB_FAILED:
        return Status::FAILED;
      case TILEDB_UNINITIALIZED:
        return Status::UNINITIALIZED;
    }
    assert(false);
    return Status::UNINITIALIZED;
  }

  /** Converts the TileDB C query type to a string representation. */
  static std::string to_str(tiledb_query_type_t type) {
    switch (type) {
      case TILEDB_READ:
        return "READ";
      case TILEDB_WRITE:
        return "WRITE";
    }
    return "";  // silence error
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The buffer sizes that were set along with the buffers to the TileDB
   * query. It is a map from the attribute name to a pair of sizes.
   * For var-sized attributes, the first element of the pair is the
   * offsets size and the second is the var-sized values size. For
   * fixed-sized attributes, the first is always 0, and the second is
   * the values size. All sizes are in bytes.
   */
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> buff_sizes_;

  /**
   * Stores the size of a single element for the buffer set for a given
   * attribute.
   */
  std::unordered_map<std::string, uint64_t> element_sizes_;

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the TileDB C query object. */
  std::shared_ptr<tiledb_query_t> query_;

  /** The schema of the array the query targets at. */
  ArraySchema schema_;

  /** Number of cells set by `set_subarray`, influences `resize_buffer`. */
  uint64_t subarray_cell_num_ = 0;

  /** URI of array being queried. **/
  std::string uri_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Get a string representation of a query status for an output stream. */
inline std::ostream& operator<<(std::ostream& os, const Query::Status& stat) {
  switch (stat) {
    case tiledb::Query::Status::INCOMPLETE:
      os << "INCOMPLETE";
      break;
    case tiledb::Query::Status::INPROGRESS:
      os << "INPROGRESS";
      break;
    case tiledb::Query::Status::FAILED:
      os << "FAILED";
      break;
    case tiledb::Query::Status::COMPLETE:
      os << "COMPLETE";
      break;
    case tiledb::Query::Status::UNINITIALIZED:
      os << "UNINITIALIZED";
      break;
  }
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_QUERY_H
