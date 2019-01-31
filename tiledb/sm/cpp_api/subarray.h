/**
 * @file   subarray.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB Subarray.
 */

#ifndef TILEDB_CPP_API_SUBARRAY_H
#define TILEDB_CPP_API_SUBARRAY_H

#include "array.h"
#include "context.h"
#include "deleter.h"
#include "dimension.h"
#include "tiledb.h"
#include "type.h"

#include <memory>
#include <type_traits>

namespace tiledb {

/**
 * Represents a query subarray.
 *
 * **Example:**
 *
 * @code{.cpp}
 * Array array(ctx, array_name, TILEDB_READ);
 * Query query(ctx, array);
 * Subarray subarray(ctx, array, TILEDB_UNORDERED);
 *
 * // Set a 1D range on dimension 0
 * int64_t range[] = { 10, 20 };
 * subarray.add_range(0, range);
 * @endcode
 *
 **/
class Subarray {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  explicit Subarray(const Context& ctx, Array& array, tiledb_layout_t layout)
      : ctx_(ctx) {
    tiledb_subarray_t* subarray;
    ctx.handle_error(tiledb_subarray_alloc(ctx, array, layout, &subarray));
    subarray_ = std::shared_ptr<tiledb_subarray_t>(subarray, deleter_);
  }

  Subarray(const Context& ctx, tiledb_subarray_t* subarray)
      : ctx_(ctx) {
    subarray_ = std::shared_ptr<tiledb_subarray_t>(subarray, deleter_);
  }

  Subarray(const Subarray&) = default;
  Subarray(Subarray&&) = default;
  Subarray& operator=(const Subarray&) = default;
  Subarray& operator=(Subarray&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Adds a 1D range along a subarray dimension. The datatype of the range
   * must be the same as the subarray domain type.
   *
   * **Example:**
   *
   * @code{.cpp}
   * Subarray subarray(ctx, array, TILEDB_UNORDERED);
   *
   * // Set a 1D range on dimension 0, assuming the domain type is int64.
   * int64_t range[] = { 10, 20 };
   * subarray.add_range(0, range);
   * @endcode
   *
   * @param dim_idx The index of the dimension to add the range to.
   * @param range The range to add.
   * @return Reference to this Subarray
   */
  Subarray& add_range(uint32_t dim_idx, const void* range) {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_subarray_add_range(ctx, subarray_.get(), dim_idx, range));
    return *this;
  }

  /**
   * Retrieves the estimated result size for a fixed-size attribute.
   *
   * **Example:**
   *
   * @code{.cpp}
   * Subarray subarray(ctx, array, TILEDB_UNORDERED);
   * // ...
   * uint64_t est_size = subarray.est_result_size("attr1");
   * @endcode
   *
   * @param attr_name The attribute name.
   * @return The estimated size in bytes.
   */
  uint64_t est_result_size(const std::string& attr_name) const {
    auto& ctx = ctx_.get();
    uint64_t size = 0;
    ctx.handle_error(tiledb_subarray_get_est_result_size(
        ctx, subarray_.get(), attr_name.c_str(), &size));
    return size;
  }

  /**
   * Retrieves the estimated result size for a variable-size attribute.
   *
   * **Example:**
   *
   * @code{.cpp}
   * Subarray subarray(ctx, array, TILEDB_UNORDERED);
   * // ...
   * std::pair<uint64_t, uint64_t> est_size =
   *     subarray.est_result_size("attr1");
   * @endcode
   *
   * @param attr_name The attribute name.
   * @return A pair with first element containing the estimated number of
   *    result offsets, and second element containing the estimated number of
   *    result value bytes.
   */
  std::pair<uint64_t, uint64_t> est_result_size_var(
      const std::string& attr_name) const {
    auto& ctx = ctx_.get();
    uint64_t size_off = 0, size_val = 0;
    ctx.handle_error(tiledb_subarray_get_est_result_size_var(
        ctx, subarray_.get(), attr_name.c_str(), &size_off, &size_val));
    return std::make_pair(size_off / sizeof(uint64_t), size_val);
  }

  /** Returns a shared pointer to the C TileDB subarray object. */
  std::shared_ptr<tiledb_subarray_t> ptr() const {
    return subarray_;
  }

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_subarray_t*() const {
    return subarray_.get();
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the C TileDB subarray object. */
  std::shared_ptr<tiledb_subarray_t> subarray_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_SUBARRAY_H
