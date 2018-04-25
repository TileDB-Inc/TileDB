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

#include "array_schema.h"
#include "context.h"
#include "core_interface.h"
#include "deleter.h"
#include "exception.h"
#include "tiledb.h"
#include "type.h"
#include "utils.h"

#include <algorithm>
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
class TILEDB_EXPORT Query {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** The query or query attribute status. */
  enum class Status { FAILED, COMPLETE, INPROGRESS, INCOMPLETE, UNDEF };

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
   * @param type Query type
   */
  Query(
      const Context& ctx,
      const std::string& array_uri,
      tiledb_query_type_t type);

  Query(const Query&) = default;
  Query(Query&& o) = default;
  Query& operator=(const Query&) = default;
  Query& operator=(Query&& o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Sets the data layout of the buffers.  */
  Query& set_layout(tiledb_layout_t layout);

  /** Returns the query status. */
  Status query_status() const;

  /** Submits the query. Call will block until query is complete. */
  Status submit();

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
    prepare_submission();
    ctx.handle_error(tiledb::impl::tiledb_query_submit_async(
        ctx, query_.get(), wrapper, nullptr));
  }

  /** Submit an async query (non-blocking). */
  void submit_async() {
    submit_async([]() {});
  }

  /**
   * Finalizes a TileDB query object, flushing all internal state.
   *
   * This function has two effects.
   *
   * (i) If the query was writing in global order, it flushes the internal
   * state. It is **required** to finalize global-order write query objects in
   * order to ensure correct execution.
   *
   * (ii) For any query, it "closes" the corresponding array. This causes the
   * storage manager to decrement the reference count of the array. When the
   * reference count reaches 0, the storage manager evicts the array metadata
   * (schema and fragment metadata) from its in-memory cache, and releases all
   * related locks. Therefore, it is advantageous to wait to finalize query
   * objects on the same array until you are done issuing queries to that array
   * or consolidation is needed.
   */
  void finalize();

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
  result_buffer_elements() const;

  /** Clears all attribute buffers. */
  void reset_buffers();

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
    if (size != schema_.domain().rank() * 2) {
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
  void set_coordinates(const T* buf, uint64_t size) {
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
   * @param buf Buffer array pointer with elements of the attribute type.
   * @param size Number of array elements
   **/
  template <typename T>
  void set_buffer(const std::string& attr, const T* buf, uint64_t size) {
    uint64_t element_size = 0;
    if (array_attributes_.count(attr)) {
      const auto& a = array_attributes_.at(attr);
      impl::type_check<T>(a.type(), 0);
      element_size = a.variable_sized() ? sizeof(T) : a.cell_size();
    } else if (attr == TILEDB_COORDS) {
      impl::type_check<T>(schema_.domain().type());
      element_size = tiledb_datatype_size(schema_.domain().type());
    } else {
      throw AttributeError("Attribute does not exist: " + attr);
    }
    attr_buffs_[attr] = std::make_tuple<uint64_t, uint64_t, void*>(
        sizeof(T) * size,
        std::move(element_size),
        const_cast<void*>(reinterpret_cast<const void*>(buf)));
    attrs_.emplace_back(attr);
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
   * @param offsets_size Number of elements in offsets buffer.
   * @param data Buffer array pointer with elements of the attribute type.
   *        For variable sized attributes, the buffer should be flattened.
   * @param size Number of array elements in data buffer.
   **/
  template <typename T>
  void set_buffer(
      const std::string& attr,
      const uint64_t* offsets,
      uint64_t offset_size,
      const T* data,
      uint64_t size) {
    if (attr == TILEDB_COORDS) {
      throw TileDBError("Cannot set coordinate buffer as variable sized.");
    }
    set_buffer(attr, data, size);
    var_offsets_[attr] = std::tuple<uint64_t, uint64_t, void*>(
        TILEDB_OFFSET_SIZE * offset_size,
        TILEDB_OFFSET_SIZE,
        const_cast<void*>(reinterpret_cast<const void*>(offsets)));
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
    set_buffer(attr, offsets.data(), offsets.size(), data.data(), data.size());
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
  static Status to_status(const tiledb_query_status_t& status);

  /** Converts the TileDB C query type to a string representation. */
  static std::string to_str(tiledb_query_type_t type);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The buffers that will be passed to a TileDB C query. */
  std::vector<void*> all_buff_;

  /** On init get the attributes of the underlying array schema. */
  std::unordered_map<std::string, Attribute> array_attributes_;

  /** Attribute names for buffers set by the user for this query. */
  std::vector<std::string> attrs_;

  /** The attribute names that will be passed to a TileDB C query. */
  std::vector<const char*> attr_names_;

  /** The buffer sizes that will be passed to a TileDB C query. */
  std::vector<uint64_t> buff_sizes_;

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

  /**
   * Keeps track the offsets buffer of a variable-sized attribute.
   *
   * Format:
   * Size of the vector, size of vector::value_type, vector.data()
   */
  std::unordered_map<std::string, std::tuple<uint64_t, uint64_t, void*>>
      var_offsets_;

  /**
   * Keeps track the data buffer for an attribute.
   *
   * Format:
   * Size of the vector, size of vector::value_type, vector.data()
   */
  std::unordered_map<std::string, std::tuple<uint64_t, uint64_t, void*>>
      attr_buffs_;

  /**
   * Keeps track of buffer element sizes to convert at return. This is
   * the datatype size * cell_num.
   **/
  std::vector<uint64_t> sub_tsize_;

  /** URI of array being queried. **/
  std::string uri_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Collate buffers and attach them to the query. */
  void prepare_submission();
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Get a string representation of a query status for an output stream. */
TILEDB_EXPORT std::ostream& operator<<(
    std::ostream& os, const Query::Status& stat);

}  // namespace tiledb

#endif  // TILEDB_CPP_API_QUERY_H
