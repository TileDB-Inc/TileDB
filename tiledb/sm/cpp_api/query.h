/**
 * @file   query.h
 *
 * @author Ravi Gaddipati
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
#include "query_condition.h"
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
 * // Open the array for writing
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, "my_dense_array", TILEDB_WRITE);
 * Query query(ctx, array);
 * query.set_layout(TILEDB_GLOBAL_ORDER);
 * std::vector a1_data = {1, 2, 3};
 * query.set_data_buffer("a1", a1_data);
 * query.submit();
 * query.finalize();
 * array.close();
 * @endcode
 */
class Query {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** The query or query attribute status. */
  enum class Status {
    /** Query failed. */
    FAILED,
    /** Query completed (all data has been read) */
    COMPLETE,
    /** Query is in progress */
    INPROGRESS,
    /** Query completed (but not all data has been read) */
    INCOMPLETE,
    /** Query not initialized.  */
    UNINITIALIZED
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Creates a TileDB query object.
   *
   * The query type (read or write) must be the same as the type used to open
   * the array object.
   *
   * The storage manager also acquires a **shared lock** on the array. This
   * means multiple read and write queries to the same array can be made
   * concurrently (in TileDB, only consolidation requires an exclusive lock for
   * a short period of time).
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Open the array for writing
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, "my_array", TILEDB_WRITE);
   * tiledb::Query query(ctx, array, TILEDB_WRITE);
   * @endcode
   *
   * @param ctx TileDB context
   * @param array Open Array object
   * @param type The TileDB query type
   */
  Query(const Context& ctx, const Array& array, tiledb_query_type_t type)
      : ctx_(ctx)
      , array_(array)
      , schema_(array.schema()) {
    tiledb_query_t* q;
    ctx.handle_error(
        tiledb_query_alloc(ctx.ptr().get(), array.ptr().get(), type, &q));
    query_ = std::shared_ptr<tiledb_query_t>(q, deleter_);
  }

  /**
   * Creates a TileDB query object.
   *
   * The query type (read or write) is inferred from the array object, which was
   * opened with a specific query type.
   *
   * The storage manager also acquires a **shared lock** on the array. This
   * means multiple read and write queries to the same array can be made
   * concurrently (in TileDB, only consolidation requires an exclusive lock for
   * a short period of time).
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Open the array for writing
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, "my_array", TILEDB_WRITE);
   * Query query(ctx, array);
   * // Equivalent to:
   * // Query query(ctx, array, TILEDB_WRITE);
   * @endcode
   *
   * @param ctx TileDB context
   * @param array Open Array object
   */
  Query(const Context& ctx, const Array& array)
      : ctx_(ctx)
      , array_(array)
      , schema_(array.schema()) {
    tiledb_query_t* q;
    auto type = array.query_type();
    ctx.handle_error(
        tiledb_query_alloc(ctx.ptr().get(), array.ptr().get(), type, &q));
    query_ = std::shared_ptr<tiledb_query_t>(q, deleter_);
  }

  Query(const Query&) = default;
  Query(Query&&) = default;
  Query& operator=(const Query&) = default;
  Query& operator=(Query&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns a shared pointer to the C TileDB query object. */
  std::shared_ptr<tiledb_query_t> ptr() const {
    return query_;
  }

  /** Returns the query type (read or write). */
  tiledb_query_type_t query_type() const {
    auto& ctx = ctx_.get();
    tiledb_query_type_t query_type;
    ctx.handle_error(
        tiledb_query_get_type(ctx.ptr().get(), query_.get(), &query_type));
    return query_type;
  }

  /**
   * Sets the layout of the cells to be written or read.
   *
   * @param layout For a write query, this specifies the order of the cells
   *     provided by the user in the buffers. For a read query, this specifies
   *     the order of the cells that will be retrieved as results and stored
   *     in the user buffers. The layout can be one of the following:
   *    - `TILEDB_COL_MAJOR`:
   *      This means column-major order with respect to the subarray.
   *    - `TILEDB_ROW_MAJOR`:
   *      This means row-major order with respect to the subarray.
   *    - `TILEDB_GLOBAL_ORDER`:
   *      This means that cells are stored or retrieved in the array global
   *      cell order.
   *    - `TILEDB_UNORDERED`:
   *      This is applicable only to writes for sparse arrays, or for sparse
   *      writes to dense arrays. It specifies that the cells are unordered and,
   *      hence, TileDB must sort the cells in the global cell order prior to
   *      writing.
   * @return Reference to this Query
   */
  Query& set_layout(tiledb_layout_t layout) {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_query_set_layout(ctx.ptr().get(), query_.get(), layout));
    return *this;
  }

  /** Returns the layout of the query. */
  tiledb_layout_t query_layout() const {
    auto& ctx = ctx_.get();
    tiledb_layout_t query_layout;
    ctx.handle_error(
        tiledb_query_get_layout(ctx.ptr().get(), query_.get(), &query_layout));
    return query_layout;
  }

  /**
   * Sets the read query condition.
   *
   * Note that only one query condition may be set on a query at a time. This
   * overwrites any previously set query condition. To apply more than one
   * condition at a time, use the `QueryCondition::combine` API to construct
   * a single object.
   *
   * @param condition The query condition object.
   * @return Reference to this Query
   */
  Query& set_condition(const QueryCondition& condition) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_set_condition(
        ctx.ptr().get(), query_.get(), condition.ptr().get()));
    return *this;
  }

  /** Returns the array of the query. */
  const Array& array() {
    return array_;
  }

  /** Returns the query status. */
  Status query_status() const {
    tiledb_query_status_t status;
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_query_get_status(ctx.ptr().get(), query_.get(), &status));
    return to_status(status);
  }

  /**
   * Returns `true` if the query has results. Applicable only to read
   * queries (it returns `false` for write queries).
   */
  bool has_results() const {
    int ret;
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_query_has_results(ctx.ptr().get(), query_.get(), &ret));
    return (bool)ret;
  }

  /**
   * Submits the query. Call will block until query is complete.
   *
   * @note
   * `finalize()` must be invoked after finish writing in global
   * layout (via repeated invocations of `submit()`), in order to
   * flush any internal state. For the case of reads, if the returned status is
   * `TILEDB_INCOMPLETE`, TileDB could not fit the entire result in the user's
   * buffers. In this case, the user should consume the read results (if any),
   * optionally reset the buffers with `set_data_buffer()`, and then
   * resubmit the query until the status becomes `TILEDB_COMPLETED`. If all
   * buffer sizes after the termination of this function become 0, then this
   * means that **no** useful data was read into the buffers, implying that the
   * larger buffers are needed for the query to proceed. In this case, the users
   * must reallocate their buffers (increasing their size), reset the buffers
   * with `set_data_buffer()`, and resubmit the query.
   *
   * @return Query status
   */
  Status submit() {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_submit(ctx.ptr().get(), query_.get()));
    return query_status();
  }

  /**
   * Submit an async query, with callback. Call returns immediately.
   *
   * @note Same notes apply as `Query::submit()`.
   *
   * **Example:**
   * @code{.cpp}
   * // Create query
   * tiledb::Query query(...);
   * // Submit with callback
   * query.submit_async([]() { std::cout << "Callback: query completed.\n"; });
   * @endcode
   *
   * @param callback Callback function.
   */
  template <typename Fn>
  void submit_async(const Fn& callback) {
    std::function<void(void*)> wrapper = [&](void*) { callback(); };
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb::impl::tiledb_query_submit_async_func(
        ctx.ptr().get(), query_.get(), &wrapper, nullptr));
  }

  /**
   * Submit an async query, with no callback. Call returns immediately.
   *
   * @note Same notes apply as `Query::submit()`.
   *
   * **Example:**
   * @code{.cpp}
   * // Create query
   * tiledb::Query query(...);
   * // Submit with no callback
   * query.submit_async();
   * @endcode
   */
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
    ctx.handle_error(tiledb_query_finalize(ctx.ptr().get(), query_.get()));
  }

  /**
   * Returns the number of elements in the result buffers from a read query.
   * This is a map from the attribute name to a pair of values.
   *
   * The first is number of elements (offsets) for var size attributes, and the
   * second is number of elements in the data buffer. For fixed sized attributes
   * (and coordinates), the first is always 0.
   *
   * For variable sized attributes: the first value is the
   * number of cells read, i.e. the number of offsets read for the attribute.
   * The second value is the total number of elements in the data buffer. For
   * example, a read query on a variable-length `float` attribute that reads
   * three cells would return 3 for the first number in the pair. If the total
   * amount of `floats` read across the three cells was 10, then the second
   * number in the pair would be 10.
   *
   * For fixed-length attributes, the first value is always 0. The second value
   * is the total number of elements in the data buffer. For example, a read
   * query on a single `float` attribute that reads three cells would return 3
   * for the second value. A read query on a `float` attribute with cell_val_num
   * 2 that reads three cells would return 3 * 2 = 6 for the second value.
   *
   * If the query has not been submitted, an empty map is returned.
   *
   * **Example:**
   * @code{.cpp}
   * // Submit a read query.
   * query.submit();
   * auto result_el = query.result_buffer_elements();
   *
   * // For fixed-sized attributes, `.second` is the number of elements
   * // that were read for the attribute across all cells. Note: number of
   * // elements and not number of bytes.
   * auto num_a1_elements = result_el["a1"].second;
   *
   * // Coords are also fixed-sized.
   * auto num_coords = result_el["__coords"].second;
   *
   * // In variable attributes, e.g. std::string type, need two buffers,
   * // one for offsets and one for cell data ("elements").
   * auto num_a2_offsets = result_el["a2"].first;
   * auto num_a2_elements = result_el["a2"].second;
   * @endcode
   */
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
  result_buffer_elements() const {
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> elements;
    if (buff_sizes_.empty())
      return std::unordered_map<
          std::string,
          std::pair<uint64_t, uint64_t>>();  // Query hasn't been submitted
    for (const auto& b_it : buff_sizes_) {
      auto attr_name = b_it.first;
      auto size_tuple = b_it.second;
      auto var =
          ((attr_name != "__coords") &&
           ((schema_.has_attribute(attr_name) &&
             schema_.attribute(attr_name).cell_val_num() == TILEDB_VAR_NUM) ||
            (schema_.domain().has_dimension(attr_name) &&
             schema_.domain().dimension(attr_name).cell_val_num() ==
                 TILEDB_VAR_NUM)));
      auto element_size = element_sizes_.find(attr_name)->second;
      elements[attr_name] = var ?
                                std::pair<uint64_t, uint64_t>(
                                    std::get<0>(size_tuple) / sizeof(uint64_t),
                                    std::get<1>(size_tuple) / element_size) :
                                std::pair<uint64_t, uint64_t>(
                                    0, std::get<1>(size_tuple) / element_size);
    }
    return elements;
  }

  /**
   * Returns the number of elements in the result buffers from a read query.
   * This is a map from the attribute name to a tuple of values.
   *
   * The first is number of elements (offsets) for var size attributes, and the
   * second is number of elements in the data buffer. For fixed sized attributes
   * (and coordinates), the first is always 0. The third element is the size of
   * the validity bytemap buffer.
   *
   *
   * For variable sized attributes: the first value is the
   * number of cells read, i.e. the number of offsets read for the attribute.
   * The second value is the total number of elements in the data buffer. For
   * example, a read query on a variable-length `float` attribute that reads
   * three cells would return 3 for the first number in the pair. If the total
   * amount of `floats` read across the three cells was 10, then the second
   * number in the pair would be 10.
   *
   * For fixed-length attributes, the first value is always 0. The second value
   * is the total number of elements in the data buffer. For example, a read
   * query on a single `float` attribute that reads three cells would return 3
   * for the second value. A read query on a `float` attribute with cell_val_num
   * 2 that reads three cells would return 3 * 2 = 6 for the second value.
   *
   * If the query has not been submitted, an empty map is returned.
   *
   * **Example:**
   * @code{.cpp}
   * // Submit a read query.
   * query.submit();
   * auto result_el = query.result_buffer_elements_nullable();
   *
   * // For fixed-sized attributes, the second tuple element is the number of
   * // elements that were read for the attribute across all cells. Note: number
   * // of elements and not number of bytes.
   * auto num_a1_elements = std::get<1>(result_el["a1"]);
   *
   * // In variable attributes, e.g. std::string type, need two buffers,
   * // one for offsets and one for cell data ("elements").
   * auto num_a2_offsets = std::get<0>(result_el["a2"]);
   * auto num_a2_elements = std::get<1>(result_el["a2"]);
   *
   * // For both fixed-size and variable-sized attributes, the third tuple
   * // element is the number of elements in the validity bytemap.
   * auto num_a1_validity_values = std::get<2>(result_el["a1"]);
   * auto num_a2_validity_values = std::get<2>(result_el["a2"]);
   * @endcode
   */
  std::unordered_map<std::string, std::tuple<uint64_t, uint64_t, uint64_t>>
  result_buffer_elements_nullable() const {
    std::unordered_map<std::string, std::tuple<uint64_t, uint64_t, uint64_t>>
        elements;
    if (buff_sizes_.empty())
      return std::unordered_map<
          std::string,
          std::tuple<uint64_t, uint64_t, uint64_t>>();  // Query hasn't been
                                                        // submitted
    for (const auto& b_it : buff_sizes_) {
      auto attr_name = b_it.first;
      auto size_tuple = b_it.second;
      auto var =
          (schema_.has_attribute(attr_name) &&
           schema_.attribute(attr_name).cell_val_num() == TILEDB_VAR_NUM) ||
          (schema_.domain().has_dimension(attr_name) &&
           schema_.domain().dimension(attr_name).cell_val_num() ==
               TILEDB_VAR_NUM);
      auto element_size = element_sizes_.find(attr_name)->second;
      elements[attr_name] = var ?
                                std::tuple<uint64_t, uint64_t, uint64_t>(
                                    std::get<0>(size_tuple) / sizeof(uint64_t),
                                    std::get<1>(size_tuple) / element_size,
                                    std::get<2>(size_tuple) / sizeof(uint8_t)) :
                                std::tuple<uint64_t, uint64_t, uint64_t>(
                                    0,
                                    std::get<1>(size_tuple) / element_size,
                                    std::get<2>(size_tuple) / sizeof(uint8_t));
    }

    return elements;
  }

  /**
   * Adds a 1D range along a subarray dimension index, in the form
   * (start, end, stride). The datatype of the range
   * must be the same as the dimension datatype.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Set a 1D range on dimension 0, assuming the domain type is int64.
   * int64_t start = 10;
   * int64_t end = 20;
   * // Stride is optional
   * subarray.add_range(0, start, end);
   * @endcode
   *
   * @tparam T The dimension datatype
   * @param dim_idx The index of the dimension to add the range to.
   * @param start The range start to add.
   * @param end The range end to add.
   * @param stride The range stride to add.
   * @return Reference to this Query
   */
  template <class T>
  Query& add_range(uint32_t dim_idx, T start, T end, T stride = 0) {
    impl::type_check<T>(schema_.domain().dimension(dim_idx).type());
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_add_range(
        ctx.ptr().get(),
        query_.get(),
        dim_idx,
        &start,
        &end,
        (stride == 0) ? nullptr : &stride));
    return *this;
  }

  /**
   * Adds a 1D range along a subarray dimension name, specified by its name, in
   * the form (start, end, stride). The datatype of the range must be the same
   * as the dimension datatype.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Set a 1D range on dimension "rows", assuming the domain type is int64.
   * int64_t start = 10;
   * int64_t end = 20;
   * const std::string dim_name = "rows";
   * // Stride is optional
   * subarray.add_range(dim_name, start, end);
   * @endcode
   *
   * @tparam T The dimension datatype
   * @param dim_name The name of the dimension to add the range to.
   * @param start The range start to add.
   * @param end The range end to add.
   * @param stride The range stride to add.
   * @return Reference to this Query
   */
  template <class T>
  Query& add_range(const std::string& dim_name, T start, T end, T stride = 0) {
    impl::type_check<T>(schema_.domain().dimension(dim_name).type());
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_add_range_by_name(
        ctx.ptr().get(),
        query_.get(),
        dim_name.c_str(),
        &start,
        &end,
        (stride == 0) ? nullptr : &stride));
    return *this;
  }

  /**
   * Adds a 1D string range along a subarray dimension index, in the form
   * (start, end). Applicable only to variable-sized dimensions
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Set a 1D range on dimension 0, assuming the domain type is int64.
   * int64_t start = 10;
   * int64_t end = 20;
   * // Stride is optional
   * subarray.add_range(0, start, end);
   * @endcode
   *
   * @tparam T The dimension datatype
   * @param dim_idx The index of the dimension to add the range to.
   * @param start The range start to add.
   * @param end The range end to add.
   * @return Reference to this Query
   */
  Query& add_range(
      uint32_t dim_idx, const std::string& start, const std::string& end) {
    impl::type_check<char>(schema_.domain().dimension(dim_idx).type());
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_add_range_var(
        ctx.ptr().get(),
        query_.get(),
        dim_idx,
        start.c_str(),
        start.size(),
        end.c_str(),
        end.size()));
    return *this;
  }

  /**
   * Adds a 1D string range along a subarray dimension name, in the form (start,
   * end). Applicable only to variable-sized dimensions
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Set a 1D range on dimension "rows", assuming the domain type is int64.
   * int64_t start = 10;
   * int64_t end = 20;
   * const std::string dim_name = "rows";
   * // Stride is optional
   * subarray.add_range(dim_name, start, end);
   * @endcode
   *
   * @tparam T The dimension datatype
   * @param dim_name The name of the dimension to add the range to.
   * @param start The range start to add.
   * @param end The range end to add.
   * @return Reference to this Query
   */
  Query& add_range(
      const std::string& dim_name,
      const std::string& start,
      const std::string& end) {
    impl::type_check<char>(schema_.domain().dimension(dim_name).type());
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_add_range_var_by_name(
        ctx.ptr().get(),
        query_.get(),
        dim_name.c_str(),
        start.c_str(),
        start.size(),
        end.c_str(),
        end.size()));
    return *this;
  }

  /**
   * Retrieves the number of ranges for a given dimension index.
   *
   * **Example:**
   *
   * @code{.cpp}
   * unsigned dim_idx = 0;
   * uint64_t range_num = query.range_num(dim_idx);
   * @endcode
   *
   * @param dim_idx The dimension index.
   * @return The number of ranges.
   */
  uint64_t range_num(unsigned dim_idx) const {
    auto& ctx = ctx_.get();
    uint64_t range_num;
    ctx.handle_error(tiledb_query_get_range_num(
        ctx.ptr().get(), query_.get(), dim_idx, &range_num));
    return range_num;
  }

  /**
   * Retrieves the number of ranges for a given dimension name.
   *
   * **Example:**
   *
   * @code{.cpp}
   * unsigned dim_name = "rows";
   * uint64_t range_num = query.range_num(dim_name);
   * @endcode
   *
   * @param dim_name The dimension name.
   * @return The number of ranges.
   */
  uint64_t range_num(const std::string& dim_name) const {
    auto& ctx = ctx_.get();
    uint64_t range_num;
    ctx.handle_error(tiledb_query_get_range_num_from_name(
        ctx.ptr().get(), query_.get(), dim_name.c_str(), &range_num));
    return range_num;
  }

  /**
   * Retrieves a range for a given dimension index and range id.
   * The template datatype must be the same as that of the
   * underlying array.
   *
   * **Example:**
   *
   * @code{.cpp}
   * unsigned dim_idx = 0;
   * unsigned range_idx = 0;
   * auto range = query.range<int32_t>(dim_idx, range_idx);
   * @endcode
   *
   * @tparam T The dimension datatype.
   * @param dim_idx The dimension index.
   * @param range_idx The range index.
   * @return A triplet of the form (start, end, stride).
   */
  template <class T>
  std::array<T, 3> range(unsigned dim_idx, uint64_t range_idx) {
    impl::type_check<T>(schema_.domain().dimension(dim_idx).type());
    auto& ctx = ctx_.get();
    const void *start, *end, *stride;
    ctx.handle_error(tiledb_query_get_range(
        ctx.ptr().get(),
        query_.get(),
        dim_idx,
        range_idx,
        &start,
        &end,
        &stride));
    std::array<T, 3> ret = {{*(const T*)start,
                             *(const T*)end,
                             (stride == nullptr) ? 0 : *(const T*)stride}};
    return ret;
  }

  /**
   * Retrieves a range for a given dimension name and range id.
   * The template datatype must be the same as that of the
   * underlying array.
   *
   * **Example:**
   *
   * @code{.cpp}
   * unsigned dim_name = "rows";
   * unsigned range_idx = 0;
   * auto range = query.range<int32_t>(dim_name, range_idx);
   * @endcode
   *
   * @tparam T The dimension datatype.
   * @param dim_name The dimension name.
   * @param range_idx The range index.
   * @return A triplet of the form (start, end, stride).
   */
  template <class T>
  std::array<T, 3> range(const std::string& dim_name, uint64_t range_idx) {
    impl::type_check<T>(schema_.domain().dimension(dim_name).type());
    auto& ctx = ctx_.get();
    const void *start, *end, *stride;
    ctx.handle_error(tiledb_query_get_range_from_name(
        ctx.ptr().get(),
        query_.get(),
        dim_name.c_str(),
        range_idx,
        &start,
        &end,
        &stride));
    std::array<T, 3> ret = {{*(const T*)start,
                             *(const T*)end,
                             (stride == nullptr) ? 0 : *(const T*)stride}};
    return ret;
  }

  /**
   * Retrieves a range for a given variable length string dimension index and
   * range id.
   *
   * **Example:**
   *
   * @code{.cpp}
   * unsigned dim_idx = 0;
   * unsigned range_idx = 0;
   * std::array<std::string, 2> range = query.range(dim_idx, range_idx);
   * @endcode
   *
   * @param dim_idx The dimension index.
   * @param range_idx The range index.
   * @return A pair of the form (start, end).
   */
  std::array<std::string, 2> range(unsigned dim_idx, uint64_t range_idx) {
    impl::type_check<char>(schema_.domain().dimension(dim_idx).type());
    auto& ctx = ctx_.get();
    uint64_t start_size, end_size;
    ctx.handle_error(tiledb_query_get_range_var_size(
        ctx.ptr().get(),
        query_.get(),
        dim_idx,
        range_idx,
        &start_size,
        &end_size));

    std::string start;
    start.resize(start_size);
    std::string end;
    end.resize(end_size);
    ctx.handle_error(tiledb_query_get_range_var(
        ctx.ptr().get(), query_.get(), dim_idx, range_idx, &start[0], &end[0]));
    std::array<std::string, 2> ret = {{std::move(start), std::move(end)}};
    return ret;
  }

  /**
   * Retrieves a range for a given variable length string dimension name and
   * range id.
   *
   * **Example:**
   *
   * @code{.cpp}
   * unsigned dim_name = "rows";
   * unsigned range_idx = 0;
   * std::array<std::string, 2> range = query.range(dim_name, range_idx);
   * @endcode
   *
   * @param dim_name The dimension name.
   * @param range_idx The range index.
   * @return A pair of the form (start, end).
   */
  std::array<std::string, 2> range(
      const std::string& dim_name, uint64_t range_idx) {
    impl::type_check<char>(schema_.domain().dimension(dim_name).type());
    auto& ctx = ctx_.get();
    uint64_t start_size, end_size;
    ctx.handle_error(tiledb_query_get_range_var_size_from_name(
        ctx.ptr().get(),
        query_.get(),
        dim_name.c_str(),
        range_idx,
        &start_size,
        &end_size));

    std::string start;
    start.resize(start_size);
    std::string end;
    end.resize(end_size);
    ctx.handle_error(tiledb_query_get_range_var_from_name(
        ctx.ptr().get(),
        query_.get(),
        dim_name.c_str(),
        range_idx,
        &start[0],
        &end[0]));
    std::array<std::string, 2> ret = {{std::move(start), std::move(end)}};
    return ret;
  }

  /**
   * Retrieves the estimated result size for a fixed-size attribute.
   *
   * **Example:**
   *
   * @code{.cpp}
   * uint64_t est_size = query.est_result_size("attr1");
   * @endcode
   *
   * @param attr_name The attribute name.
   * @return The estimated size in bytes.
   */
  uint64_t est_result_size(const std::string& attr_name) const {
    auto& ctx = ctx_.get();
    uint64_t size = 0;
    ctx.handle_error(tiledb_query_get_est_result_size(
        ctx.ptr().get(), query_.get(), attr_name.c_str(), &size));
    return size;
  }

  /**
   * Retrieves the estimated result size for a variable-size attribute.
   *
   * **Example:**
   *
   * @code{.cpp}
   * std::array<uint64_t, 2> est_size =
   *     query.est_result_size_var("attr1");
   * @endcode
   *
   * @param attr_name The attribute name.
   * @return An array with first element containing the estimated size of
   *    the result offsets in bytes, and second element containing the
   *    estimated size of the result values in bytes.
   */
  std::array<uint64_t, 2> est_result_size_var(
      const std::string& attr_name) const {
    auto& ctx = ctx_.get();
    uint64_t size_off = 0, size_val = 0;
    ctx.handle_error(tiledb_query_get_est_result_size_var(
        ctx.ptr().get(),
        query_.get(),
        attr_name.c_str(),
        &size_off,
        &size_val));
    return {size_off, size_val};
  }

  /**
   * Retrieves the estimated result size for a fixed-size, nullable attribute.
   *
   * **Example:**
   *
   * @code{.cpp}
   * std::array<uint64_t, 2> est_size =
   *    query.est_result_size_nullable("attr1");
   * @endcode
   *
   * @param attr_name The attribute name.
   * @return An array with first element containing the estimated size of
   *    the result values in bytes, and second element containing the
   *    estimated size of the result validity values in bytes.
   */
  std::array<uint64_t, 2> est_result_size_nullable(
      const std::string& attr_name) const {
    auto& ctx = ctx_.get();
    uint64_t size_val = 0;
    uint64_t size_validity = 0;
    ctx.handle_error(tiledb_query_get_est_result_size_nullable(
        ctx.ptr().get(),
        query_.get(),
        attr_name.c_str(),
        &size_val,
        &size_validity));
    return {size_val, size_validity};
  }

  /**
   * Retrieves the estimated result size for a variable-size, nullable
   * attribute.
   *
   * **Example:**
   *
   * @code{.cpp}
   * std::array<uint64_t, 3> est_size =
   *     query.est_result_size_var_nullable("attr1");
   * @endcode
   *
   * @param attr_name The attribute name.
   * @return An array with first element containing the estimated size of
   *    the offset values in bytes, second element containing the
   *    estimated size of the result values in bytes, and the third element
   *    containing the estimated size of the validity values in bytes.
   */
  std::array<uint64_t, 3> est_result_size_var_nullable(
      const std::string& attr_name) const {
    auto& ctx = ctx_.get();
    uint64_t size_off = 0;
    uint64_t size_val = 0;
    uint64_t size_validity = 0;
    ctx.handle_error(tiledb_query_get_est_result_size_var_nullable(
        ctx.ptr().get(),
        query_.get(),
        attr_name.c_str(),
        &size_off,
        &size_val,
        &size_validity));
    return {size_off, size_val, size_validity};
  }

  /**
   * Returns the number of written fragments. Applicable only to WRITE queries.
   */
  uint32_t fragment_num() const {
    auto& ctx = ctx_.get();
    uint32_t num;
    ctx.handle_error(
        tiledb_query_get_fragment_num(ctx.ptr().get(), query_.get(), &num));
    return num;
  }

  /**
   * Returns the URI of the written fragment with the input index. Applicable
   * only to WRITE queries.
   */
  std::string fragment_uri(uint32_t idx) const {
    auto& ctx = ctx_.get();
    const char* uri;
    ctx.handle_error(tiledb_query_get_fragment_uri(
        ctx.ptr().get(), query_.get(), idx, &uri));
    return uri;
  }

  /**
   * Returns the timestamp range of the written fragment with the input index.
   * Applicable only to WRITE queries.
   */
  std::pair<uint64_t, uint64_t> fragment_timestamp_range(uint32_t idx) const {
    auto& ctx = ctx_.get();
    uint64_t t1, t2;
    ctx.handle_error(tiledb_query_get_fragment_timestamp_range(
        ctx.ptr().get(), query_.get(), idx, &t1, &t2));
    return std::make_pair(t1, t2);
  }

  /**
   * Sets a subarray, defined in the order dimensions were added.
   * Coordinates are inclusive. For the case of writes, this is meaningful only
   * for dense arrays, and specifically dense writes.
   *
   * @note `set_subarray(std::vector<T>)` is preferred as it is safer.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_READ);
   * int subarray[] = {0, 3, 0, 3};
   * Query query(ctx, array);
   * query.set_subarray(subarray, 4);
   * @endcode
   *
   * @tparam T Type of array domain.
   * @param pairs Subarray pointer defined as an array of [start, stop] values
   * per dimension.
   * @param size The number of subarray elements.
   */
  template <typename T = uint64_t>
  Query& set_subarray(const T* pairs, uint64_t size) {
    impl::type_check<T>(schema_.domain().type());
    auto& ctx = ctx_.get();
    if (size != schema_.domain().ndim() * 2) {
      throw SchemaMismatch(
          "Subarray should have num_dims * 2 values: (low, high) for each "
          "dimension.");
    }
    ctx.handle_error(
        tiledb_query_set_subarray(ctx.ptr().get(), query_.get(), pairs));
    subarray_cell_num_ = pairs[1] - pairs[0] + 1;
    for (unsigned i = 2; i < size - 1; i += 2) {
      subarray_cell_num_ *= (pairs[i + 1] - pairs[i] + 1);
    }
    return *this;
  }

  /**
   * Sets a subarray, defined in the order dimensions were added.
   * Coordinates are inclusive. For the case of writes, this is meaningful only
   * for dense arrays, and specifically dense writes.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_READ);
   * std::vector<int> subarray = {0, 3, 0, 3};
   * Query query(ctx, array);
   * query.set_subarray(subarray);
   * @endcode
   *
   * @tparam Vec Vector datatype. Should always be a vector of the domain type.
   * @param pairs The subarray defined as a vector of [start, stop] coordinates
   * per dimension.
   */
  template <typename Vec>
  Query& set_subarray(const Vec& pairs) {
    return set_subarray(pairs.data(), pairs.size());
  }

  /**
   * Sets a subarray, defined in the order dimensions were added.
   * Coordinates are inclusive. For the case of writes, this is meaningful only
   * for dense arrays, and specifically dense writes.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_READ);
   * Query query(ctx, array);
   * query.set_subarray({0, 3, 0, 3});
   * @endcode
   *
   * @tparam T Type of array domain.
   * @param pairs List of [start, stop] coordinates per dimension.
   */
  template <typename T = uint64_t>
  Query& set_subarray(const std::initializer_list<T>& l) {
    return set_subarray(std::vector<T>(l));
  }

  /**
   * Sets a subarray, defined in the order dimensions were added.
   * Coordinates are inclusive.
   *
   * @note set_subarray(std::vector) is preferred and avoids an extra copy.
   *
   * @tparam T Type of array domain.
   * @param pairs The subarray defined as pairs of [start, stop] per dimension.
   */
  template <typename T = uint64_t>
  Query& set_subarray(const std::vector<std::array<T, 2>>& pairs) {
    std::vector<T> buf;
    buf.reserve(pairs.size() * 2);
    std::for_each(
        pairs.begin(), pairs.end(), [&buf](const std::array<T, 2>& p) {
          buf.push_back(p[0]);
          buf.push_back(p[1]);
        });
    return set_subarray(buf);
  }

  /**
   * Set the query config.
   *
   * Setting configuration with this function overrides the following
   * Query-level parameters only:
   *
   * - `sm.memory_budget`
   * - `sm.memory_budget_var`
   * - `sm.var_offsets.mode`
   * - `sm.var_offsets.extra_element`
   * - `sm.var_offsets.bitsize`
   * - `sm.check_coord_dups`
   * - `sm.check_coord_oob`
   * - `sm.check_global_order`
   * - `sm.dedup_coords`
   */
  Query& set_config(const Config& config) {
    auto ctx = ctx_.get();

    ctx.handle_error(tiledb_query_set_config(
        ctx.ptr().get(), query_.get(), config.ptr().get()));

    return *this;
  }

  /**
   * Get the config
   * @return Config
   */
  Config config() const {
    tiledb_config_t* config;
    tiledb_query_get_config(ctx_.get().ptr().get(), query_.get(), &config);

    return Config(&config);
  }

  /**
   * Set the coordinate buffer.
   *
   * The coordinate buffer has been deprecated. Set the coordinates for
   * each individual dimension with the `set_buffer` API. Consult the current
   * documentation for more information.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * // Write to points (0,1) and (2,3) in a 2D array with int domain.
   * int coords[] = {0, 1, 2, 3};
   * Query query(ctx, array);
   * query.set_layout(TILEDB_UNORDERED).set_coordinates(coords, 4);
   * @endcode
   *
   * @note set_coordinates(std::vector<T>) is preferred as it is safer.
   *
   * @tparam T Type of array domain.
   * @param buf Coordinate array buffer pointer
   * @param size The number of elements in the coordinate array buffer
   * **/
  template <typename T>
  TILEDB_DEPRECATED Query& set_coordinates(T* buf, uint64_t size) {
    impl::type_check<T>(schema_.domain().type());
    return set_data_buffer("__coords", buf, size);
  }

  /**
   * Set the coordinate buffer for unordered queries.
   *
   * The coordinate buffer has been deprecated. Set the coordinates for
   * each individual dimension with the `set_buffer` API. Consult the current
   * documentation for more information.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * // Write to points (0,1) and (2,3) in a 2D array with int domain.
   * std::vector<int> coords = {0, 1, 2, 3};
   * Query query(ctx, array);
   * query.set_layout(TILEDB_UNORDERED).set_coordinates(coords);
   * @endcode
   *
   * @tparam Vec Vector datatype. Should always be a vector of the domain type.
   * @param buf Coordinate vector
   * **/
  template <typename Vec>
  TILEDB_DEPRECATED Query& set_coordinates(Vec& buf) {
    return set_coordinates(buf.data(), buf.size());
  }

  /**
   * Sets a buffer for a fixed-sized attribute/dimension.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * int data_a1[] = {0, 1, 2, 3};
   * Query query(ctx, array);
   * query.set_buffer("a1", data_a1, 4);
   * @endcode
   *
   * @note set_buffer(std::string, std::vector) is preferred as it is safer.
   *
   * @tparam T Attribute/Dimension value type
   * @param name Attribute/Dimension name
   * @param buff Buffer array pointer with elements of the
   *     attribute/dimension type.
   * @param nelements Number of array elements
   **/
  template <typename T>
  TILEDB_DEPRECATED Query& set_buffer(
      const std::string& name, T* buff, uint64_t nelements) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    auto is_dim = schema_.domain().has_dimension(name);
    if (name != "__coords" && !is_attr && !is_dim)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute/Dimension '") + name +
          "' does not exist");
    else if (is_attr)
      impl::type_check<T>(schema_.attribute(name).type());
    else if (is_dim)
      impl::type_check<T>(schema_.domain().dimension(name).type());
    else if (name == "__coords")
      impl::type_check<T>(schema_.domain().type());

    return set_data_buffer(name, buff, nelements, sizeof(T));
  }

  /**
   * Sets a buffer for a fixed-sized attribute/dimension.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * std::vector<int> data_a1 = {0, 1, 2, 3};
   * Query query(ctx, array);
   * query.set_buffer("a1", data_a1);
   * @endcode
   *
   * @tparam T Attribute/Dimension value type
   * @param name Attribute/Dimension name
   * @param buf Buffer vector with elements of the attribute/dimension type.
   **/
  template <typename T>
  TILEDB_DEPRECATED Query& set_buffer(
      const std::string& name, std::vector<T>& buf) {
    return set_data_buffer(name, buf.data(), buf.size(), sizeof(T));
  }

  /**
   * Sets a buffer for a fixed-sized attribute/dimension.
   *
   * @note This unsafe version does not perform type checking; the given buffer
   * is assumed to be the correct type, and the size of an element in the given
   * buffer is assumed to be the size of the datatype of the attribute.
   *
   * @param name Attribute/Dimension name
   * @param buff Buffer array pointer with elements of the attribute type.
   * @param nelements Number of array elements in buffer
   **/
  TILEDB_DEPRECATED Query& set_buffer(
      const std::string& name, void* buff, uint64_t nelements) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    auto is_dim = schema_.domain().has_dimension(name);
    if (name != "__coords" && !is_attr && !is_dim)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute/Dimension '") + name +
          "' does not exist");

    // Compute element size (in bytes).
    size_t element_size = 0;
    if (name == "__coords")
      element_size = tiledb_datatype_size(schema_.domain().type());
    else if (is_attr)
      element_size = tiledb_datatype_size(schema_.attribute(name).type());
    else if (is_dim)
      element_size =
          tiledb_datatype_size(schema_.domain().dimension(name).type());

    return set_data_buffer(name, buff, nelements, element_size);
  }

  /**
   * Sets a buffer for a variable-sized attribute/dimension.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * int data_a1[] = {0, 1, 2, 3};
   * uint64_t offsets_a1[] = {0, 8};
   * Query query(ctx, array);
   * query.set_buffer("a1", offsets_a1, 2, data_a1, 4);
   * @endcode
   *
   * @note set_buffer(std::string, std::vector, std::vector) is preferred as it
   * is safer.
   *
   * @tparam T Attribute/Dimension value type
   * @param name Attribute/Dimension name
   * @param offsets Offsets array pointer where a new element begins in the data
   *        buffer.
   * @param offsets_nelements Number of elements in offsets buffer.
   * @param data Buffer array pointer with elements of the attribute type.
   *        For variable sized attributes, the buffer should be flattened.
   * @param data_nelements Number of array elements in data buffer.
   **/
  template <typename T>
  TILEDB_DEPRECATED Query& set_buffer(
      const std::string& name,
      uint64_t* offsets,
      uint64_t offset_nelements,
      T* data,
      uint64_t data_nelements) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    auto is_dim = schema_.domain().has_dimension(name);
    if (!is_attr && !is_dim)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute/Dimension '") + name +
          "' does not exist");
    else if (is_attr)
      impl::type_check<T>(schema_.attribute(name).type());
    else if (is_dim)
      impl::type_check<T>(schema_.domain().dimension(name).type());

    set_data_buffer(name, data, data_nelements, sizeof(T));
    set_offsets_buffer(name, offsets, offset_nelements);
    return *this;
  }

  /**
   * Sets a buffer for a variable-sized attribute/dimension.
   *
   * @note This unsafe version does not perform type checking; the given buffer
   * is assumed to be the correct type, and the size of an element in the given
   * buffer is assumed to be the size of the datatype of the attribute.
   *
   * @param name Attribute/Dimension name
   * @param offsets Offsets array pointer where a new element begins in the data
   *        buffer.
   * @param offsets_nelements Number of elements in offsets buffer.
   * @param data Buffer array pointer with elements of the attribute type.
   * @param data_nelements Number of array elements in data buffer.
   **/
  TILEDB_DEPRECATED Query& set_buffer(
      const std::string& name,
      uint64_t* offsets,
      uint64_t offset_nelements,
      void* data,
      uint64_t data_nelements) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    auto is_dim = schema_.domain().has_dimension(name);
    if (!is_attr && !is_dim)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute/Dimension '") + name +
          "' does not exist");

    // Compute element size (in bytes).
    auto type = is_attr ? schema_.attribute(name).type() :
                          schema_.domain().dimension(name).type();
    size_t element_size = tiledb_datatype_size(type);

    set_data_buffer(name, data, data_nelements, element_size);
    set_offsets_buffer(name, offsets, offset_nelements);
    return *this;
  }

  /**
   * Sets a buffer for a variable-sized attribute/dimension.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * std::vector<int> data_a1 = {0, 1, 2, 3};
   * std::vector<uint64_t> offsets_a1 = {0, 8};
   * Query query(ctx, array);
   * query.set_buffer("a1", offsets_a1, data_a1);
   * @endcode
   *
   * @tparam T Attribute/Dimension value type
   * @param name Attribute/Dimension name
   * @param offsets Offsets where a new element begins in the data buffer.
   * @param data Buffer vector with elements of the attribute type.
   *        For variable sized attributes, the buffer should be flattened. E.x.
   *        an attribute of type std::string should have a buffer Vec type of
   *        std::string, where the values of each cell are concatenated.
   **/
  template <typename T>
  TILEDB_DEPRECATED Query& set_buffer(
      const std::string& name,
      std::vector<uint64_t>& offsets,
      std::vector<T>& data) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    auto is_dim = schema_.domain().has_dimension(name);
    if (!is_attr && !is_dim)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute/Dimension '") + name +
          "' does not exist");
    else if (is_attr)
      impl::type_check<T>(schema_.attribute(name).type());
    else if (is_dim)
      impl::type_check<T>(schema_.domain().dimension(name).type());

    set_data_buffer(name, data.data(), data.size(), sizeof(T));
    set_offsets_buffer(name, offsets.data(), offsets.size());
    return *this;
  }

  /**
   * Sets a buffer for a variable-sized attribute/dimension.
   *
   * @tparam T Attribute/Dimension value type
   * @param attr Attribute/Dimension name
   * @param buf Pair of offset, data buffers
   **/
  template <typename T>
  TILEDB_DEPRECATED Query& set_buffer(
      const std::string& name,
      std::pair<std::vector<uint64_t>, std::vector<T>>& buf) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    auto is_dim = schema_.domain().has_dimension(name);
    if (!is_attr && !is_dim)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute/Dimension '") + name +
          "' does not exist");
    else if (is_attr)
      impl::type_check<T>(schema_.attribute(name).type());
    else if (is_dim)
      impl::type_check<T>(schema_.domain().dimension(name).type());

    set_data_buffer(name, buf.second);
    set_offsets_buffer(name, buf.first);
    return *this;
  }

  /**
   * Sets a buffer for a string-typed variable-sized attribute/dimension.
   *
   * @param name Attribute/Dimension name
   * @param offsets Offsets where a new element begins in the data buffer.
   * @param data Pre-allocated string buffer.
   **/
  TILEDB_DEPRECATED Query& set_buffer(
      const std::string& name,
      std::vector<uint64_t>& offsets,
      std::string& data) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    auto is_dim = schema_.domain().has_dimension(name);
    if (!is_attr && !is_dim)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute/Dimension '") + name +
          "' does not exist");
    else if (is_attr)
      impl::type_check<char>(schema_.attribute(name).type());
    else if (is_dim)
      impl::type_check<char>(schema_.domain().dimension(name).type());

    set_data_buffer(name, &data[0], data.size(), sizeof(char));
    set_offsets_buffer(name, offsets.data(), offsets.size());
    return *this;
  }

  /**
   * Sets the data for a fixed/var-sized attribute/dimension.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * int data_a1[] = {0, 1, 2, 3};
   * Query query(ctx, array);
   * query.set_data_buffer("a1", data_a1, 4);
   * @endcode
   *
   * @note set_data_buffer(std::string, std::vector) is preferred as it is
   * safer.
   *
   * @tparam T Attribute/Dimension value type
   * @param name Attribute/Dimension name
   * @param buff Buffer array pointer with elements of the
   *     attribute/dimension type.
   * @param nelements Number of array elements
   **/
  template <typename T>
  Query& set_data_buffer(const std::string& name, T* buff, uint64_t nelements) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    auto is_dim = schema_.domain().has_dimension(name);
    if (name != "__coords" && !is_attr && !is_dim)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute/Dimension '") + name +
          "' does not exist");
    else if (is_attr)
      impl::type_check<T>(schema_.attribute(name).type());
    else if (is_dim)
      impl::type_check<T>(schema_.domain().dimension(name).type());
    else if (name == "__coords")
      impl::type_check<T>(schema_.domain().type());

    return set_data_buffer(name, buff, nelements, sizeof(T));
  }

  /**
   * Sets the data for a fixed/var-sized attribute/dimension.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * std::vector<int> data_a1 = {0, 1, 2, 3};
   * Query query(ctx, array);
   * query.set_data_buffer("a1", data_a1);
   * @endcode
   *
   * @tparam T Attribute/Dimension value type
   * @param name Attribute/Dimension name
   * @param buf Buffer vector with elements of the attribute/dimension type.
   **/
  template <typename T>
  Query& set_data_buffer(const std::string& name, std::vector<T>& buf) {
    return set_data_buffer(name, buf.data(), buf.size(), sizeof(T));
  }

  /**
   * Sets the data for a fixed/var-sized attribute/dimension.
   *
   * @note This unsafe version does not perform type checking; the given buffer
   * is assumed to be the correct type, and the size of an element in the given
   * buffer is assumed to be the size of the datatype of the attribute.
   *
   * @param name Attribute/Dimension name
   * @param buff Buffer array pointer with elements of the attribute type.
   * @param nelements Number of array elements in buffer
   **/
  Query& set_data_buffer(
      const std::string& name, void* buff, uint64_t nelements) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    auto is_dim = schema_.domain().has_dimension(name);
    if (name != "__coords" && !is_attr && !is_dim)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute/Dimension '") + name +
          "' does not exist");

    // Compute element size (in bytes).
    size_t element_size = 0;
    if (name == "__coords")
      element_size = tiledb_datatype_size(schema_.domain().type());
    else if (is_attr)
      element_size = tiledb_datatype_size(schema_.attribute(name).type());
    else if (is_dim)
      element_size =
          tiledb_datatype_size(schema_.domain().dimension(name).type());

    return set_data_buffer(name, buff, nelements, element_size);
  }

  /**
   * Sets the data for a fixed/var-sized attribute/dimension.
   *
   * @param name Attribute/Dimension name
   * @param data Pre-allocated string buffer.
   **/
  Query& set_data_buffer(const std::string& name, std::string& data) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    auto is_dim = schema_.domain().has_dimension(name);
    if (!is_attr && !is_dim)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute/Dimension '") + name +
          "' does not exist");
    else if (is_attr)
      impl::type_check<char>(schema_.attribute(name).type());
    else if (is_dim)
      impl::type_check<char>(schema_.domain().dimension(name).type());

    return set_data_buffer(name, &data[0], data.size(), sizeof(char));
  }

  /**
   * Sets the offset buffer for a var-sized attribute/dimension.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * uint64_t offsets_a1[] = {0, 8};
   * Query query(ctx, array);
   * query.set_offsets_buffer("a1", offsets_a1, 2);
   * @endcode
   *
   * @note set_offsets_buffer(std::string, std::vector, std::vector) is
   * preferred as it is safer.
   *
   * @param attr Attribute/Dimension name
   * @param offsets Offsets array pointer where a new element begins in the data
   *        buffer.
   * @param offsets_nelements Number of elements in offsets buffer.
   **/
  Query& set_offsets_buffer(
      const std::string& attr, uint64_t* offsets, uint64_t offset_nelements) {
    auto ctx = ctx_.get();
    auto offset_size = offset_nelements * sizeof(uint64_t);
    auto buff_sizes_iter = buff_sizes_.find(attr);
    if (buff_sizes_iter == buff_sizes_.end()) {
      buff_sizes_[attr] =
          std::tuple<uint64_t, uint64_t, uint64_t>(offset_size, 0, 0);
    } else {
      auto& second = buff_sizes_iter->second;
      buff_sizes_[attr] = std::tuple<uint64_t, uint64_t, uint64_t>(
          offset_size, std::get<1>(second), std::get<2>(second));
    }

    ctx.handle_error(tiledb_query_set_offsets_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        offsets,
        &std::get<0>(buff_sizes_[attr])));
    return *this;
  }

  /**
   * Sets the offset buffer for a var-sized attribute/dimension.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * std::vector<uint64_t> offsets_a1 = {0, 8};
   * Query query(ctx, array);
   * query.set_offsets_buffer("a1", offsets_a1);
   * @endcode
   *
   * @param name Attribute/Dimension name
   * @param offsets Offsets where a new element begins in the data buffer.
   **/
  Query& set_offsets_buffer(
      const std::string& name, std::vector<uint64_t>& offsets) {
    return set_offsets_buffer(name, offsets.data(), offsets.size());
  }

  /**
   * Sets the validity buffer for nullable attribute/dimension.
   *
   * @tparam T Attribute value type
   * @param attr Attribute name
   * @param validity_bytemap The validity bytemap buffer.
   * @param validity_bytemap_nelements The number of values within
   *     `validity_bytemap_nelements`
   **/
  Query& set_validity_buffer(
      const std::string& attr,
      uint8_t* validity_bytemap,
      uint64_t validity_bytemap_nelements) {
    auto ctx = ctx_.get();
    size_t validity_size = validity_bytemap_nelements * sizeof(uint8_t);
    auto buff_sizes_iter = buff_sizes_.find(attr);
    if (buff_sizes_iter == buff_sizes_.end()) {
      buff_sizes_[attr] =
          std::tuple<uint64_t, uint64_t, uint64_t>(0, 0, validity_size);
    } else {
      auto& second = buff_sizes_iter->second;
      buff_sizes_[attr] = std::tuple<uint64_t, uint64_t, uint64_t>(
          std::get<0>(second), std::get<1>(second), validity_size);
    }

    ctx.handle_error(tiledb_query_set_validity_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        validity_bytemap,
        &std::get<2>(buff_sizes_[attr])));
    return *this;
  }

  /**
   * Sets the validity buffer for nullable attribute/dimension.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * std::vector<uint8_t> validity_bytemap = {1, 1, 0, 1};
   * Query query(ctx, array);
   * query.set_validity_buffer("a1", validity_bytemap);
   * @endcode
   *
   * @param name Attribute name
   * @param validity_bytemap Buffer vector with elements of the attribute
   *     validity values.
   **/
  Query& set_validity_buffer(
      const std::string& name, std::vector<uint8_t>& validity_bytemap) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    if (!is_attr)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute '") + name +
          "' does not exist");
    return set_validity_buffer(
        name, validity_bytemap.data(), validity_bytemap.size());
  }

  /**
   * Sets a buffer for a fixed-sized, nullable attribute.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * int data_a1[] = {0, 1, 2, 3};
   * uint8_t validity_bytemap[] = {1, 1, 0, 1};
   * Query query(ctx, array);
   * query.set_buffer("a1", data_a1, 4, validity_bytemap, 4);
   * @endcode
   *
   * @note set_buffer_nullable(std::string, std::vector, std::vector)
   * is preferred as it is safer.
   *
   * @tparam T Attribute value type
   * @param name Attribute name
   * @param data Buffer array pointer with elements of the
   *     attribute type.
   * @param data_nelements Number of array elements in `data`
   * @param validity_bytemap The validity bytemap buffer.
   * @param validity_bytemap_nelements The number of values within
   *     `validity_bytemap_nelements`
   **/
  template <typename T>
  TILEDB_DEPRECATED Query& set_buffer_nullable(
      const std::string& name,
      T* data,
      uint64_t data_nelements,
      uint8_t* validity_bytemap,
      uint64_t validity_bytemap_nelements) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    if (!is_attr)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute '") + name +
          "' does not exist");
    else
      impl::type_check<T>(schema_.attribute(name).type());

    set_data_buffer(name, data, data_nelements);
    set_validity_buffer(name, validity_bytemap, validity_bytemap_nelements);
    return *this;
  }

  /**
   * Sets a buffer for a fixed-sized, nullable attribute.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * std::vector<int> data_a1 = {0, 1, 2, 3};
   * std::vector<uint8_t> validity_bytemap = {1, 1, 0, 1};
   * Query query(ctx, array);
   * query.set_buffer("a1", data_a1, validity_bytemap);
   * @endcode
   *
   * @tparam T Attribute value type
   * @param name Attribute name
   * @param buf Buffer vector with elements of the attribute/dimension type.
   * @param validity_bytemap Buffer vector with elements of the attribute
   *     validity values.
   **/
  template <typename T>
  TILEDB_DEPRECATED Query& set_buffer_nullable(
      const std::string& name,
      std::vector<T>& buf,
      std::vector<uint8_t>& validity_bytemap) {
    set_data_buffer(name, buf.data(), buf.size(), sizeof(T));
    set_validity_buffer(name, validity_bytemap.data(), validity_bytemap.size());
    return *this;
  }

  /**
   * Sets a buffer for a fixed-sized, nullable attribute.
   *
   * @note This unsafe version does not perform type checking; the given buffer
   * is assumed to be the correct type, and the size of an element in the given
   * buffer is assumed to be the size of the datatype of the attribute.
   *
   * @param name Attribute name
   * @param data Buffer array pointer with elements of the attribute type.
   * @param data_nelements Number of array elements in buffer
   * @param validity_bytemap The validity bytemap buffer.
   * @param validity_bytemap_nelements The number of values within
   *     `validity_bytemap_nelements`
   **/
  TILEDB_DEPRECATED Query& set_buffer_nullable(
      const std::string& name,
      void* data,
      uint64_t data_nelements,
      uint8_t* validity_bytemap,
      uint64_t validity_bytemap_nelements) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    if (!is_attr)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute '") + name +
          "' does not exist");

    // Compute element size (in bytes).
    size_t element_size = tiledb_datatype_size(schema_.attribute(name).type());

    set_data_buffer(name, data, data_nelements, element_size);
    set_validity_buffer(name, validity_bytemap, validity_bytemap_nelements);
    return *this;
  }

  /**
   * Sets a buffer for a variable-sized, nullable attribute.
   *
   * @note This unsafe version does not perform type checking; the given buffer
   * is assumed to be the correct type, and the size of an element in the given
   * buffer is assumed to be the size of the datatype of the attribute.
   *
   * @param name Attribute name
   * @param offsets Offsets array pointer where a new element begins in the data
   *        buffer.
   * @param offsets_nelements Number of elements in offsets buffer.
   * @param data Buffer array pointer with elements of the attribute type.
   * @param data_nelements Number of array elements in data buffer.
   * @param validity_bytemap The validity bytemap buffer.
   * @param validity_bytemap_nelements The number of values within
   *     `validity_bytemap_nelements`
   **/
  TILEDB_DEPRECATED Query& set_buffer_nullable(
      const std::string& name,
      uint64_t* offsets,
      uint64_t offset_nelements,
      void* data,
      uint64_t data_nelements,
      uint8_t* validity_bytemap,
      uint64_t validity_bytemap_nelements) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    if (!is_attr)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute '") + name +
          "' does not exist");

    // Compute element size (in bytes).
    auto type = schema_.attribute(name).type();
    size_t element_size = tiledb_datatype_size(type);

    set_data_buffer(name, data, data_nelements, element_size);
    set_offsets_buffer(name, offsets, offset_nelements);
    set_validity_buffer(name, validity_bytemap, validity_bytemap_nelements);
    return *this;
  }

  /**
   * Sets a buffer for a variable-sized, nullable attribute.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * std::vector<int> data_a1 = {0, 1, 2, 3};
   * std::vector<uint64_t> offsets_a1 = {0, 8};
   * std::vector<uint8_t> validity_bytemap = {1, 1, 0, 1};
   * Query query(ctx, array);
   * query.set_buffer("a1", offsets_a1, data_a1, validity_bytemap);
   * @endcode
   *
   * @tparam T Attribute value type
   * @param name Attribute name
   * @param offsets Offsets where a new element begins in the data buffer.
   * @param data Buffer vector with elements of the attribute type.
   *        For variable sized attributes, the buffer should be flattened. E.x.
   *        an attribute of type std::string should have a buffer Vec type of
   *        std::string, where the values of each cell are concatenated.
   * @param validity_bytemap Buffer vector with elements of the attribute
   *     validity values.
   **/
  template <typename T>
  TILEDB_DEPRECATED Query& set_buffer_nullable(
      const std::string& name,
      std::vector<uint64_t>& offsets,
      std::vector<T>& data,
      std::vector<uint8_t>& validity_bytemap) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    if (!is_attr)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute '") + name +
          "' does not exist");
    else
      impl::type_check<T>(schema_.attribute(name).type());

    set_data_buffer(name, data.data(), data.size(), sizeof(T));
    set_offsets_buffer(name, offsets.data(), offsets.size());
    set_validity_buffer(name, validity_bytemap.data(), validity_bytemap.size());
    return *this;
  }

  /**
   * Sets a buffer for a variable-sized, nullable attribute.
   *
   * @tparam T Attribute value type
   * @param attr Attribute name
   * @param buf Pair of offset, data, validity bytemap buffers
   **/
  template <typename T>
  TILEDB_DEPRECATED Query& set_buffer_nullable(
      const std::string& name,
      std::tuple<std::vector<uint64_t>, std::vector<T>, std::vector<uint8_t>>&
          buf) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    if (!is_attr)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute '") + name +
          "' does not exist");
    impl::type_check<T>(schema_.attribute(name).type());

    set_data_buffer(name, std::get<1>(buf));
    set_offsets_buffer(name, std::get<0>(buf));
    set_validity_buffer(name, std::get<2>(buf));
    return *this;
  }

  /**
   * Sets a buffer for a string-typed variable-sized, nullable attribute.
   *
   * @param name Attribute name
   * @param offsets Offsets where a new element begins in the data buffer.
   * @param data Pre-allocated string buffer.
   * @param validity_bytemap Buffer vector with elements of the attribute
   *     validity values.
   **/
  TILEDB_DEPRECATED Query& set_buffer_nullable(
      const std::string& name,
      std::vector<uint64_t>& offsets,
      std::string& data,
      std::vector<uint8_t>& validity_bytemap) {
    // Checks
    auto is_attr = schema_.has_attribute(name);
    if (!is_attr)
      throw TileDBError(
          std::string("Cannot set buffer; Attribute '") + name +
          "' does not exist");
    else
      impl::type_check<char>(schema_.attribute(name).type());

    set_data_buffer(name, &data[0], data.size(), sizeof(char));
    set_offsets_buffer(name, offsets.data(), offsets.size());
    set_validity_buffer(name, validity_bytemap.data(), validity_bytemap.size());
    return *this;
  }

  /**
   * Gets a buffer for a fixed-sized attribute/dimension.
   *
   * @param name Attribute/dimension name
   * @param data Buffer array pointer with elements of the attribute type.
   * @param data_nelements Number of array elements.
   * @param element_size Size of array elements (in bytes).
   **/
  TILEDB_DEPRECATED Query& get_buffer(
      const std::string& name,
      void** data,
      uint64_t* data_nelements,
      uint64_t* element_size) {
    auto ctx = ctx_.get();
    uint64_t* data_nbytes = nullptr;
    auto elem_size_iter = element_sizes_.find(name);
    if (elem_size_iter == element_sizes_.end()) {
      throw TileDBError(
          "[TileDB::C++API] Error: No buffer set for attribute '" + name +
          "'!");
    }

    ctx.handle_error(tiledb_query_get_data_buffer(
        ctx.ptr().get(), query_.get(), name.c_str(), data, &data_nbytes));

    assert(*data_nbytes % elem_size_iter->second == 0);

    *data_nelements = (*data_nbytes) / elem_size_iter->second;
    *element_size = elem_size_iter->second;

    return *this;
  }

  /**
   * Gets a buffer for a var-sized attribute/dimension.
   *
   * @param name Attribute/dimension name
   * @param offsets Offsets array pointer with elements of uint64_t type.
   * @param offsets_nelements Number of array elements.
   * @param data Buffer array pointer with elements of the attribute type.
   * @param data_nelements Number of array elements.
   * @param element_size Size of array elements (in bytes).
   **/
  TILEDB_DEPRECATED Query& get_buffer(
      const std::string& name,
      uint64_t** offsets,
      uint64_t* offsets_nelements,
      void** data,
      uint64_t* data_nelements,
      uint64_t* element_size) {
    auto ctx = ctx_.get();
    uint64_t* offsets_nbytes = nullptr;
    uint64_t* data_nbytes = nullptr;
    auto elem_size_iter = element_sizes_.find(name);
    if (elem_size_iter == element_sizes_.end()) {
      throw TileDBError(
          "[TileDB::C++API] Error: No buffer set for attribute '" + name +
          "'!");
    }

    ctx.handle_error(tiledb_query_get_data_buffer(
        ctx.ptr().get(), query_.get(), name.c_str(), data, &data_nbytes));
    ctx.handle_error(tiledb_query_get_offsets_buffer(
        ctx.ptr().get(), query_.get(), name.c_str(), offsets, &offsets_nbytes));

    assert(*data_nbytes % elem_size_iter->second == 0);
    assert(*offsets_nbytes % sizeof(uint64_t) == 0);

    *data_nelements = (*data_nbytes) / elem_size_iter->second;
    *offsets_nelements = (*offsets_nbytes) / sizeof(uint64_t);
    *element_size = elem_size_iter->second;

    return *this;
  }

  /**
   * Retrieves the data buffer of a fixed/var-sized attribute/dimension.
   *
   * @param name Attribute/dimension name
   * @param data Buffer array pointer with elements of the attribute type.
   * @param data_nelements Number of array elements.
   * @param element_size Size of array elements (in bytes).
   **/
  Query& get_data_buffer(
      const std::string& name,
      void** data,
      uint64_t* data_nelements,
      uint64_t* element_size) {
    auto ctx = ctx_.get();
    uint64_t* data_nbytes = nullptr;
    auto elem_size_iter = element_sizes_.find(name);
    if (elem_size_iter == element_sizes_.end()) {
      throw TileDBError(
          "[TileDB::C++API] Error: No buffer set for attribute '" + name +
          "'!");
    }

    ctx.handle_error(tiledb_query_get_data_buffer(
        ctx.ptr().get(), query_.get(), name.c_str(), data, &data_nbytes));

    assert(*data_nbytes % elem_size_iter->second == 0);

    *data_nelements = (*data_nbytes) / elem_size_iter->second;
    *element_size = elem_size_iter->second;

    return *this;
  }

  /**
   * Retrieves the offset buffer for a var-sized attribute/dimension.
   *
   * @param name Attribute/dimension name
   * @param offsets Offsets array pointer with elements of uint64_t type.
   * @param offsets_nelements Number of array elements.
   **/
  Query& get_offsets_buffer(
      const std::string& name,
      uint64_t** offsets,
      uint64_t* offsets_nelements) {
    auto ctx = ctx_.get();
    uint64_t* offsets_nbytes = nullptr;
    ctx.handle_error(tiledb_query_get_offsets_buffer(
        ctx.ptr().get(), query_.get(), name.c_str(), offsets, &offsets_nbytes));

    *offsets_nelements = (*offsets_nbytes) / sizeof(uint64_t);

    return *this;
  }

  /**
   * Retrieves the validity buffer for a nullable attribute/dimension.
   *
   * @param name Attribute name
   * @param validity_bytemap Buffer array pointer with elements of the
   *     attribute validity values.
   * @param validity_bytemap_nelements Number of validity bytemap elements.
   **/
  Query& get_validity_buffer(
      const std::string& name,
      uint8_t** validity_bytemap,
      uint64_t* validity_bytemap_nelements) {
    auto ctx = ctx_.get();
    uint64_t* validity_bytemap_nbytes = nullptr;

    ctx.handle_error(tiledb_query_get_validity_buffer(
        ctx.ptr().get(),
        query_.get(),
        name.c_str(),
        validity_bytemap,
        &validity_bytemap_nbytes));

    *validity_bytemap_nelements = *validity_bytemap_nbytes / sizeof(uint8_t);

    return *this;
  }

  /**
   * Gets a buffer for a fixed-sized, nullable attribute.
   *
   * @param name Attribute name
   * @param data Buffer array pointer with elements of the attribute type.
   * @param data_nelements Number of array elements.
   * @param data_element_size Size of array elements (in bytes).
   * @param validity_bytemap Buffer array pointer with elements of the
   *     attribute validity values.
   * @param validity_bytemap_nelements Number of validity bytemap elements.
   **/
  TILEDB_DEPRECATED Query& get_buffer_nullable(
      const std::string& name,
      void** data,
      uint64_t* data_nelements,
      uint64_t* data_element_size,
      uint8_t** validity_bytemap,
      uint64_t* validity_bytemap_nelements) {
    auto ctx = ctx_.get();
    uint64_t* data_nbytes = nullptr;
    uint64_t* validity_bytemap_nbytes = nullptr;
    auto elem_size_iter = element_sizes_.find(name);
    if (elem_size_iter == element_sizes_.end()) {
      throw TileDBError(
          "[TileDB::C++API] Error: No buffer set for attribute '" + name +
          "'!");
    }

    ctx.handle_error(tiledb_query_get_data_buffer(
        ctx.ptr().get(), query_.get(), name.c_str(), data, &data_nbytes));
    ctx.handle_error(tiledb_query_get_validity_buffer(
        ctx.ptr().get(),
        query_.get(),
        name.c_str(),
        validity_bytemap,
        &validity_bytemap_nbytes));

    assert(*data_nbytes % elem_size_iter->second == 0);

    *data_nelements = *data_nbytes / elem_size_iter->second;
    *data_element_size = elem_size_iter->second;
    *validity_bytemap_nelements = *validity_bytemap_nbytes / sizeof(uint8_t);

    return *this;
  }

  /**
   * Gets a buffer for a var-sized, nullable attribute.
   *
   * @param name Attribute name
   * @param offsets Offsets array pointer with elements of uint64_t type.
   * @param offsets_nelements Number of array elements.
   * @param data Buffer array pointer with elements of the attribute type.
   * @param data_nelements Number of array elements.
   * @param element_size Size of array elements (in bytes).
   * @param validity_bytemap Buffer array pointer with elements of the
   *     attribute validity values.
   * @param validity_bytemap_nelements Number of validity bytemap elements.
   **/
  TILEDB_DEPRECATED Query& get_buffer_nullable(
      const std::string& name,
      uint64_t** offsets,
      uint64_t* offsets_nelements,
      void** data,
      uint64_t* data_nelements,
      uint64_t* element_size,
      uint8_t** validity_bytemap,
      uint64_t* validity_bytemap_nelements) {
    auto ctx = ctx_.get();
    uint64_t* offsets_nbytes = nullptr;
    uint64_t* data_nbytes = nullptr;
    uint64_t* validity_bytemap_nbytes = nullptr;
    auto elem_size_iter = element_sizes_.find(name);
    if (elem_size_iter == element_sizes_.end()) {
      throw TileDBError(
          "[TileDB::C++API] Error: No buffer set for attribute '" + name +
          "'!");
    }

    ctx.handle_error(tiledb_query_get_data_buffer(
        ctx.ptr().get(), query_.get(), name.c_str(), data, &data_nbytes));
    ctx.handle_error(tiledb_query_get_offsets_buffer(
        ctx.ptr().get(), query_.get(), name.c_str(), offsets, &offsets_nbytes));
    ctx.handle_error(tiledb_query_get_validity_buffer(
        ctx.ptr().get(),
        query_.get(),
        name.c_str(),
        validity_bytemap,
        &validity_bytemap_nbytes));

    assert(*data_nbytes % elem_size_iter->second == 0);
    assert(*offsets_nbytes % sizeof(uint64_t) == 0);

    *data_nelements = (*data_nbytes) / elem_size_iter->second;
    *offsets_nelements = (*offsets_nbytes) / sizeof(uint64_t);
    *element_size = elem_size_iter->second;
    *validity_bytemap_nelements = *validity_bytemap_nbytes / sizeof(uint8_t);

    return *this;
  }

  /** Returns a JSON-formatted string of the stats. */
  std::string stats() {
    auto ctx = ctx_.get();
    char* c_str;
    ctx.handle_error(
        tiledb_query_get_stats(ctx.ptr().get(), query_.get(), &c_str));

    // Copy `c_str` into `str`.
    std::string str(c_str);
    free(c_str);

    return str;
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
   * query. It is a map from the attribute name to a tuple of sizes.
   * For var-sized attributes, the first element of the tuple is the
   * offsets size and the second is the var-sized values size. For
   * fixed-sized attributes, the first is always 0, and the second is
   * the values size. All sizes are in bytes. If the attribute is nullable,
   * the third element in the tuple is the size of the validity buffer.
   */
  std::unordered_map<std::string, std::tuple<uint64_t, uint64_t, uint64_t>>
      buff_sizes_;

  /**
   * Stores the size of a single element for the buffer set for a given
   * attribute.
   */
  std::unordered_map<std::string, uint64_t> element_sizes_;

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** The TileDB array. */
  std::reference_wrapper<const Array> array_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the TileDB C query object. */
  std::shared_ptr<tiledb_query_t> query_;

  /** The schema of the array the query targets at. */
  ArraySchema schema_;

  /** Number of cells set by `set_subarray`, influences `resize_buffer`. */
  uint64_t subarray_cell_num_ = 0;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Sets a buffer for a fixed-sized attribute.
   *
   * @param attr Attribute name
   * @param buff Buffer array pointer with elements of the attribute type.
   * @param nelements Number of array elements.
   * @param element_size Size of array elements (in bytes).
   **/
  TILEDB_DEPRECATED Query& set_buffer(
      const std::string& attr,
      void* buff,
      uint64_t nelements,
      size_t element_size) {
    auto ctx = ctx_.get();
    size_t size = nelements * element_size;
    buff_sizes_[attr] = std::tuple<uint64_t, uint64_t, uint64_t>(0, size, 0);
    element_sizes_[attr] = element_size;
    ctx.handle_error(tiledb_query_set_data_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        buff,
        &std::get<1>(buff_sizes_[attr])));
    return *this;
  }

  /**
   * Sets a buffer for a variable-sized attribute.
   *
   * @tparam T Attribute value type
   * @param attr Attribute name
   * @param offsets Offsets array pointer where a new element begins in the data
   *        buffer.
   * @param offsets_nelements Number of elements in offsets buffer.
   * @param data Buffer array pointer with elements of the attribute type.
   *        For variable sized attributes, the buffer should be flattened.
   * @param data_nelements Number of array elements in data buffer.
   * @param data_element_size Size of data array elements (in bytes).
   **/
  TILEDB_DEPRECATED Query& set_buffer(
      const std::string& attr,
      uint64_t* offsets,
      uint64_t offset_nelements,
      void* data,
      uint64_t data_nelements,
      size_t data_element_size) {
    auto ctx = ctx_.get();
    auto data_size = data_nelements * data_element_size;
    auto offset_size = offset_nelements * sizeof(uint64_t);
    element_sizes_[attr] = data_element_size;
    buff_sizes_[attr] =
        std::tuple<uint64_t, uint64_t, uint64_t>(offset_size, data_size, 0);
    ctx.handle_error(tiledb_query_set_data_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        data,
        &std::get<1>(buff_sizes_[attr])));
    ctx.handle_error(tiledb_query_set_offsets_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        offsets,
        &std::get<0>(buff_sizes_[attr])));
    return *this;
  }

  /**
   * Sets the data for a fixed/var-sized attribute/dimension.
   *
   * @param attr Attribute name
   * @param data Buffer array pointer with elements of the attribute type.
   * @param data_nelements Number of array elements.
   * @param data_element_size Size of array elements (in bytes).
   **/
  Query& set_data_buffer(
      const std::string& attr,
      void* data,
      uint64_t data_nelements,
      size_t data_element_size) {
    auto ctx = ctx_.get();
    auto data_size = data_nelements * data_element_size;
    element_sizes_[attr] = data_element_size;
    auto buff_sizes_iter = buff_sizes_.find(attr);
    if (buff_sizes_iter == buff_sizes_.end()) {
      buff_sizes_[attr] =
          std::tuple<uint64_t, uint64_t, uint64_t>(0, data_size, 0);
    } else {
      auto& second = buff_sizes_iter->second;
      buff_sizes_[attr] = std::tuple<uint64_t, uint64_t, uint64_t>(
          std::get<0>(second), data_size, std::get<2>(second));
    }

    ctx.handle_error(tiledb_query_set_data_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        data,
        &std::get<1>(buff_sizes_[attr])));
    return *this;
  }

  /**
   * Sets a buffer for a nullable, fixed-sized attribute.
   *
   * @param attr Attribute name
   * @param data Buffer array pointer with elements of the attribute type.
   * @param data_nelements Number of array elements.
   * @param data_element_size Size of array elements (in bytes).
   * @param validity_bytemap Buffer array pointer with validity bytemap values.
   * @param validity_bytemap_nelements Number of validity bytemap elements.
   **/
  TILEDB_DEPRECATED Query& set_buffer_nullable(
      const std::string& attr,
      void* data,
      uint64_t data_nelements,
      size_t data_element_size,
      uint8_t* validity_bytemap,
      uint64_t validity_bytemap_nelements) {
    auto ctx = ctx_.get();
    size_t data_size = data_nelements * data_element_size;
    size_t validity_size = validity_bytemap_nelements * sizeof(uint8_t);
    buff_sizes_[attr] =
        std::tuple<uint64_t, uint64_t, uint64_t>(0, data_size, validity_size);
    element_sizes_[attr] = data_element_size;
    ctx.handle_error(tiledb_query_set_data_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        data,
        &std::get<1>(buff_sizes_[attr])));
    ctx.handle_error(tiledb_query_set_validity_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        validity_bytemap,
        &std::get<2>(buff_sizes_[attr])));
    return *this;
  }

  /**
   * Sets a buffer for a nullable, variable-sized attribute.
   *
   * @tparam T Attribute value type
   * @param attr Attribute name
   * @param offsets Offsets array pointer where a new element begins in the data
   *        buffer.
   * @param offsets_nelements Number of elements in offsets buffer.
   * @param data Buffer array pointer with elements of the attribute type.
   *        For variable sized attributes, the buffer should be flattened.
   * @param data_nelements Number of array elements in data buffer.
   * @param data_element_size Size of data array elements (in bytes).
   * @param validity_bytemap Buffer array pointer with validity bytemap values.
   * @param validity_bytemap_nelements Number of validity bytemap elements.
   **/
  TILEDB_DEPRECATED Query& set_buffer_nullable(
      const std::string& attr,
      uint64_t* offsets,
      uint64_t offset_nelements,
      void* data,
      uint64_t data_nelements,
      size_t data_element_size,
      uint8_t* validity_bytemap,
      uint64_t validity_bytemap_nelements) {
    auto ctx = ctx_.get();
    auto data_size = data_nelements * data_element_size;
    auto offset_size = offset_nelements * sizeof(uint64_t);
    size_t validity_size = validity_bytemap_nelements * sizeof(uint8_t);
    element_sizes_[attr] = data_element_size;
    buff_sizes_[attr] = std::tuple<uint64_t, uint64_t, uint64_t>(
        offset_size, data_size, validity_size);
    ctx.handle_error(tiledb_query_set_data_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        data,
        &std::get<1>(buff_sizes_[attr])));
    ctx.handle_error(tiledb_query_set_offsets_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        offsets,
        &std::get<0>(buff_sizes_[attr])));
    ctx.handle_error(tiledb_query_set_validity_buffer(
        ctx.ptr().get(),
        query_.get(),
        attr.c_str(),
        validity_bytemap,
        &std::get<2>(buff_sizes_[attr])));
    return *this;
  }
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
