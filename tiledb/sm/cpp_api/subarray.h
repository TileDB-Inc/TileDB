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

#ifndef TILEDB_CPP_API_SUBARRAY_H
#define TILEDB_CPP_API_SUBARRAY_H

#include "array.h"
#include "array_schema.h"
#include "context.h"
#include "core_interface.h"
#include "deleter.h"
#include "exception.h"
//#include "query.h"
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
 * Construct and support manipulation of a possibly multiple-range subarray for optional
 * use with Query object operations.
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
 * query.set_layout(TILEDB_GLOBAL_ORDER);
 * std::vector a1_data = {1, 2, 3};
 * query.set_buffer("a1", a1_data);
 * tiledb::Subarray subarray(ctx, array);
 * std::vector<int32_t> subarray_indices = {1, 2};
 * subarray.add_range(0, subarray_indices[0], subarray_indices[1]);
 * query.submit_with_subarray(subarray);
 * query.finalize();
 * array.close();
 * @endcode
 */
class Subarray {
public:
    
  Subarray(const tiledb::Context& ctx, const tiledb::Array& array) 
	    : ctx_(ctx)
      , array_(array)
      , schema_(array.schema()) {
    tiledb_subarray_t* capi_subarray;
    ctx.handle_error(tiledb_subarray_alloc(ctx.ptr().get(), array.ptr().get(), &capi_subarray));
    subarray_ = std::shared_ptr<tiledb_subarray_t>(capi_subarray, deleter_);
  }

  Subarray(const tiledb::Query& query);
  #if 0
      : ctx_(query.ctx())
      , array_(query.array()) 
      , schema(query.array().schema()) {
      tiledb_subarray_t *loc_subarray;
    ctx.handle(tiledb_subarray_alloc(ctx_.ptr().get(), array_.ptr().get(), &loc_subarray));
  }
  #endif
  
  int32_t tiledb_subarray_set_subarray(
      tiledb_ctx_t* ctx,
      tiledb_subarray_t* subarray_s,
      const void* subarray_v);

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
  TILEDB_DEPRECATED
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

   */
  tiledb_subarray_t* capi_subarray() const {
    return subarray_.get();
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
   * uint64_t range_num = query.range_num(dim_name);
   * @endcode
   *
   * @param dim_name The dimension name.
   * @return The number of ranges.
   */
  uint64_t range_num(const std::string& dim_name) const {
    auto& ctx = ctx_.get();
    uint64_t range_num;
    ctx.handle_error(tiledb_subarray_get_range_num_from_name(
        ctx.ptr().get(),
        subarray_.get(),
        dim_name.c_str(),
        &range_num));
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
    ctx.handle_error(tiledb_subarray_get_range(
        ctx.ptr().get(),
        subarray_.get(),
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
    ctx.handle_error(tiledb_subarray_get_est_result_size(
        ctx.ptr().get(), subarray_.get(), attr_name.c_str(), &size));
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
    ctx.handle_error(tiledb_subarray_get_est_result_size_var(
        ctx.ptr().get(),
        subarray_.get(),
        attr_name.c_str(),
        &size_off,
        &size_val));
    return {size_off, size_val};
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** The TileDB array. */
  std::reference_wrapper<const Array> array_;

  std::shared_ptr<tiledb_subarray_t> subarray_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** The schema of the array the query targets at. */
  ArraySchema schema_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_SUBARRAY_H
