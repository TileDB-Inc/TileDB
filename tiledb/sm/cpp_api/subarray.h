/**
 * @file   subarray.h
 *
 * @author David Hoke
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
 * This file declares the C++ API for the TileDB Subarray object.
 */

#ifndef TILEDB_CPP_API_SUBARRAY_H
#define TILEDB_CPP_API_SUBARRAY_H

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

class Query;

/**
 * Construct and support manipulation of a possibly multiple-range subarray for
 * optional use with Query object operations.
 *
 * @details
 * See examples for more usage details.
 *
 * **Example:**
 * @code{.cpp}
 * // Open the array for writing
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, "my_dense_array", TILEDB_WRITE);
 * Query query(ctx, array);
 * std::vector a1_data = {1, 2, 3};
 * query.set_buffer("a1", a1_data);
 * tiledb::Subarray subarray(ctx, array);
 * subarray.set_layout(TILEDB_GLOBAL_ORDER);
 * std::vector<int32_t> subarray_indices = {1, 2};
 * subarray.add_range(0, subarray_indices[0], subarray_indices[1]);
 * query.set_subarray(subarray);
 * query.submit();
 * query.finalize();
 * array.close();
 * @endcode
 */
class Subarray {
 public:
  /**
   * Creates a TileDB Subarray object.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Open the array for writing
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, "my_array", TILEDB_WRITE);
   * tiledb::Subarray subarray(ctx, array);
   * @endcode
   *
   * @param ctx TileDB context
   * @param array Open Array object
   * @param coalesce_ranges When enabled, ranges will attempt to coalesce
   *     with existing ranges as they are added.
   */
  Subarray(
      const tiledb::Context& ctx,
      const tiledb::Array& array,
      bool coalesce_ranges = true)
      : ctx_(ctx)
      , array_(array)
      , schema_(array.schema()) {
    tiledb_subarray_t* capi_subarray;
    ctx.handle_error(tiledb_subarray_alloc(
        ctx.ptr().get(), array.ptr().get(), &capi_subarray));
    tiledb_subarray_set_coalesce_ranges(
        ctx.ptr().get(), capi_subarray, coalesce_ranges);
    subarray_ = std::shared_ptr<tiledb_subarray_t>(capi_subarray, deleter_);
  }

  /** Set the coalesce_ranges flag for the subarray. */
  Subarray& set_coalesce_ranges(bool coalesce_ranges) {
    ctx_.get().handle_error(tiledb_subarray_set_coalesce_ranges(
        ctx_.get().ptr().get(), subarray_.get(), coalesce_ranges));
    return *this;
  }

  /** Replace/update -this- Subarray's shared_ptr to data to reference the
   * passed subarray.
   *
   * @param capi_subarray is a c_api subarray to be referenced by this cpp_api
   * subarray entity.
   */

  Subarray& replace_subarray_data(tiledb_subarray_t* capi_subarray) {
    subarray_ = std::shared_ptr<tiledb_subarray_t>(capi_subarray, deleter_);
    return *this;
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
  Subarray& add_range(uint32_t dim_idx, T start, T end, T stride = 0) {
    impl::type_check<T>(schema_.domain().dimension(dim_idx).type());
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_subarray_add_range(
        ctx.ptr().get(),
        subarray_.get(),
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
  Subarray& add_range(
      const std::string& dim_name, T start, T end, T stride = 0) {
    impl::type_check<T>(schema_.domain().dimension(dim_name).type());
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_subarray_add_range_by_name(
        ctx.ptr().get(),
        subarray_.get(),
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
  Subarray& add_range(
      uint32_t dim_idx, const std::string& start, const std::string& end) {
    impl::type_check<char>(schema_.domain().dimension(dim_idx).type());
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_subarray_add_range_var(
        ctx.ptr().get(),
        subarray_.get(),
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
  Subarray& add_range(
      const std::string& dim_name,
      const std::string& start,
      const std::string& end) {
    impl::type_check<char>(schema_.domain().dimension(dim_name).type());
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_subarray_add_range_var_by_name(
        ctx.ptr().get(),
        subarray_.get(),
        dim_name.c_str(),
        start.c_str(),
        start.size(),
        end.c_str(),
        end.size()));
    return *this;
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
   * int subarray_vals[] = {0, 3, 0, 3};
   * Subarray subarray(ctx, array);
   * subarray.set_subarray(subarray_vals, 4);
   * @endcode
   *
   * @tparam T Type of array domain.
   * @param pairs Subarray pointer defined as an array of [start, stop] values
   * per dimension.
   * @param size The number of subarray elements.
   *
   * @note The number of pairs passed should equal number of dimensions
   *       of the array associated with the subarray, or the
   *       number of elements in subarray_vals should equal that number of
   *       dimensions * 2.
   */
  template <typename T = uint64_t>
  Subarray& set_subarray(const T* pairs, uint64_t size) {
    impl::type_check<T>(schema_.domain().type());
    auto& ctx = ctx_.get();
    if (size != schema_.domain().ndim() * 2) {
      throw SchemaMismatch(
          "Subarray should have num_dims * 2 values: (low, high) for each "
          "dimension.");
    }
    ctx.handle_error(
        tiledb_subarray_set_subarray(ctx.ptr().get(), subarray_.get(), pairs));
    return *this;
  }

  /**
   * Set the query config.
   *
   * Setting configuration with this function overrides the following
   * Query-level parameters only:
   *
   * - `sm.memory_budget`
   * - `sm.memory_budget_var`
   * - `sm.sub_partitioner_memory_budget`
   * - `sm.var_offsets.mode`
   * - `sm.var_offsets.extra_element`
   * - `sm.var_offsets.bitsize`
   * - `sm.check_coord_dups`
   * - `sm.check_coord_oob`
   * - `sm.check_global_order`
   * - `sm.dedup_coords`
   */
  Subarray& set_config(const Config& config) {
    auto ctx = ctx_.get();

    ctx.handle_error(tiledb_subarray_set_config(
        ctx.ptr().get(), subarray_.get(), config.ptr().get()));

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
   * std::vector<int> subarray_vals = {0, 3, 0, 3};
   * Subarray subarray(ctx, array);
   * subarray.set_subarray(subarray_vals);
   * @endcode
   *
   * @tparam Vec Vector datatype. Should always be a vector of the domain type.
   * @param pairs The subarray defined as a vector of [start, stop] coordinates
   * per dimension.
   */
  template <typename Vec>
  Subarray& set_subarray(const Vec& pairs) {
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
   * Subarray subarray(ctx, array);
   * subarray.set_subarray({0, 3, 0, 3});
   * @endcode
   *
   * @tparam T Type of array domain.
   * @param pairs List of [start, stop] coordinates per dimension.
   */
  template <typename T = uint64_t>
  Subarray& set_subarray(const std::initializer_list<T>& l) {
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
  Subarray& set_subarray(const std::vector<std::array<T, 2>>& pairs) {
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
   * Retrieves the number of ranges for a given dimension index.
   *
   * **Example:**
   *
   * @code{.cpp}
   * unsigned dim_idx = 0;
   * uint64_t range_num = subarray.range_num(dim_idx);
   * @endcode
   *
   * @param dim_idx The dimension index.
   * @return The number of ranges.
   */
  uint64_t range_num(unsigned dim_idx) const {
    auto& ctx = ctx_.get();
    uint64_t range_num;
    ctx.handle_error(tiledb_subarray_get_range_num(
        ctx.ptr().get(), subarray_.get(), dim_idx, &range_num));
    return range_num;
  }

  /**
   * Retrieves the number of ranges for a given dimension name.
   *
   * **Example:**
   *
   * @code{.cpp}
   * unsigned dim_name = "rows";
   * uint64_t range_num = subarray.range_num(dim_name);
   * @endcode
   *
   * @param dim_name The dimension name.
   * @return The number of ranges.
   */
  uint64_t range_num(const std::string& dim_name) const {
    auto& ctx = ctx_.get();
    uint64_t range_num;
    ctx.handle_error(tiledb_subarray_get_range_num_from_name(
        ctx.ptr().get(), subarray_.get(), dim_name.c_str(), &range_num));
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
   * auto range = subarray.range<int32_t>(dim_idx, range_idx);
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
    ctx.handle_error(tiledb_subarray_get_range(
        ctx.ptr().get(),
        subarray_.get(),
        dim_idx,
        range_idx,
        &start,
        &end,
        &stride));
    std::array<T, 3> ret = {
        {*(const T*)start,
         *(const T*)end,
         (stride == nullptr) ? (const T)0 : *(const T*)stride}};
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
   * auto range = subarray.range<int32_t>(dim_name, range_idx);
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
    ctx.handle_error(tiledb_subarray_get_range_from_name(
        ctx.ptr().get(),
        subarray_.get(),
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
   * std::array<std::string, 2> range = subarray.range(dim_idx, range_idx);
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
    ctx.handle_error(tiledb_subarray_get_range_var_size(
        ctx.ptr().get(),
        subarray_.get(),
        dim_idx,
        range_idx,
        &start_size,
        &end_size));

    std::string start;
    start.resize(start_size);
    std::string end;
    end.resize(end_size);

    ctx.handle_error(tiledb_subarray_get_range_var(
        ctx.ptr().get(),
        subarray_.get(),
        dim_idx,
        range_idx,
        &start[0],
        &end[0]));
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
   * std::array<std::string, 2> range = subarray.range(dim_name, range_idx);
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
    ctx.handle_error(tiledb_subarray_get_range_var_size_from_name(
        ctx.ptr().get(),
        subarray_.get(),
        dim_name.c_str(),
        range_idx,
        &start_size,
        &end_size));

    std::string start;
    start.resize(start_size);
    std::string end;
    end.resize(end_size);

    ctx.handle_error(tiledb_subarray_get_range_var_from_name(
        ctx.ptr().get(),
        subarray_.get(),
        dim_name.c_str(),
        range_idx,
        &start[0],
        &end[0]));
    std::array<std::string, 2> ret = {{std::move(start), std::move(end)}};
    return ret;
  }

  /** Returns the C TileDB subarray object. */
  std::shared_ptr<tiledb_subarray_t> ptr() const {
    return subarray_;
  }

  /** Returns the array the subarray is associated with. */
  const Array& array() const {
    return array_.get();
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** The TileDB array. */
  std::reference_wrapper<const Array> array_;

  /** The subarray entity itself.  */
  std::shared_ptr<tiledb_subarray_t> subarray_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** The schema of the array the query targets at. */
  ArraySchema schema_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_SUBARRAY_H
