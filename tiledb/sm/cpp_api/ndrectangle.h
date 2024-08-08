/**
 * @file   ndrectangle.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB NDRectangle.
 */

#ifndef TILEDB_CPP_API_NDRECTANGLE_H
#define TILEDB_CPP_API_NDRECTANGLE_H

#include "context.h"
#include "deleter.h"
#include "dimension.h"
#include "domain.h"
#include "tiledb.h"
#include "type.h"

#include <functional>
#include <memory>
#include <type_traits>

namespace tiledb {

class NDRectangle {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor
   *
   * @param ctx The TileDB context
   * @param domain The input Domain
   */
  explicit NDRectangle(const tiledb::Context& ctx, const tiledb::Domain& domain)
      : ctx_(ctx) {
    tiledb_ndrectangle_t* capi_ndrect;
    ctx.handle_error(tiledb_ndrectangle_alloc(
        ctx.ptr().get(), domain.ptr().get(), &capi_ndrect));
    ndrect_ = std::shared_ptr<tiledb_ndrectangle_t>(capi_ndrect, deleter_);
  }

  /**
   * Constructor
   *
   * @param ctx The TileDB context
   * @param ndrect The NDRectangle C API pointer
   *
   * @note The object will manage the lifetime of tiledb_ndrectangle_t*
   */
  NDRectangle(const tiledb::Context& ctx, tiledb_ndrectangle_t* ndrect)
      : ctx_(ctx) {
    ndrect_ = std::shared_ptr<tiledb_ndrectangle_t>(ndrect, deleter_);
  }

  NDRectangle(const NDRectangle&) = default;
  NDRectangle(NDRectangle&&) = default;
  NDRectangle& operator=(const NDRectangle&) = default;
  NDRectangle& operator=(NDRectangle&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Adds an 1D range along a dimension name, in the form (start, end). The
   * datatype of the range must be the same as the dimension datatype.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Set an 1D range on dimension 0, assuming the domain type is int64.
   * int64_t start = 10;
   * int64_t end = 20;
   * ndrect.set_range(0, start, end);
   * @endcode
   *
   * @tparam T The dimension datatype. Must be an integer or a floating point
   * number.
   * @param dim_name The name of the dimension to add the range to.
   * @param start The range start to add.
   * @param end The range end to add.
   * @return Reference to this NDRectangle.
   */
  template <
      class T,
      std::enable_if_t<
          std::is_integral_v<T> || std::is_floating_point_v<T>,
          bool> = true>
  NDRectangle& set_range(const std::string& dim_name, T start, T end) {
    auto& ctx = ctx_.get();

    // Create the tiledb_range_t struct and fill it
    tiledb_range_t range;
    range.min = static_cast<const void*>(&start);
    range.min_size = sizeof(T);
    range.max = static_cast<const void*>(&end);
    range.max_size = sizeof(T);

    // Pass the struct to tiledb_ndrectangle_set_range_for_name
    ctx.handle_error(tiledb_ndrectangle_set_range_for_name(
        ctx.ptr().get(), ndrect_.get(), dim_name.c_str(), &range));

    return *this;
  }

  /**
   * Adds an 1D range along a dimension index, in the form (start, end). The
   * datatype of the range must be the same as the dimension datatype.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Set an 1D range on dimension 0, assuming the domain type is int64.
   * int64_t start = 10;
   * int64_t end = 20;
   * ndrect.set_range(0, start, end);
   * @endcode
   *
   * @tparam T The dimension datatype. Must be an integer or a floating point
   * number.
   * @param dim_idx The index of the dimension to add the range to.
   * @param start The range start to add.
   * @param end The range end to add.
   * @return Reference to this NDRectangle.
   */
  template <
      class T,
      std::enable_if_t<
          std::is_integral_v<T> || std::is_floating_point_v<T>,
          bool> = true>
  NDRectangle& set_range(uint32_t dim_idx, T start, T end) {
    auto& ctx = ctx_.get();
    // Create the tiledb_range_t struct and fill it
    tiledb_range_t range;
    range.min = static_cast<const void*>(&start);
    range.min_size = sizeof(T);
    range.max = static_cast<const void*>(&end);
    range.max_size = sizeof(T);

    // Pass the struct to tiledb_ndrectangle_set_range
    ctx.handle_error(tiledb_ndrectangle_set_range(
        ctx.ptr().get(), ndrect_.get(), dim_idx, &range));

    return *this;
  }

  /**
   * Adds a 1D string range along a dimension index, in the form (start, end).
   * Applicable only to variable-sized dimensions
   *
   * @param dim_idx The index of the dimension to add the range to.
   * @param start The range start to add.
   * @param end The range end to add.
   * @return Reference to this NDRectangle.
   */
  NDRectangle& set_range(
      uint32_t dim_idx, const std::string& start, const std::string& end) {
    auto& ctx = ctx_.get();
    // Create the tiledb_range_t struct and fill it
    tiledb_range_t range;
    range.min = static_cast<const void*>(start.c_str());
    range.min_size = start.size();
    range.max = static_cast<const void*>(end.c_str());
    range.max_size = end.size();

    // Pass the struct to tiledb_ndrectangle_set_range
    ctx.handle_error(tiledb_ndrectangle_set_range(
        ctx.ptr().get(), ndrect_.get(), dim_idx, &range));

    return *this;
  }

  /**
   * Adds a 1D string range along a dimension index, in the form (start, end).
   * Applicable only to variable-sized dimensions
   *
   * @param dim_name The name of the dimension to add the range to.
   * @param start The range start to add.
   * @param end The range end to add.
   * @return Reference to this NDRectangle.
   */
  NDRectangle& set_range(
      const std::string& dim_name,
      const std::string& start,
      const std::string& end) {
    auto& ctx = ctx_.get();
    // Create the tiledb_range_t struct and fill it
    tiledb_range_t range;
    range.min = static_cast<const void*>(start.c_str());
    range.min_size = start.size();
    range.max = static_cast<const void*>(end.c_str());
    range.max_size = end.size();

    // Pass the struct to tiledb_ndrectangle_set_range_for_name
    ctx.handle_error(tiledb_ndrectangle_set_range_for_name(
        ctx.ptr().get(), ndrect_.get(), dim_name.c_str(), &range));

    return *this;
  }

  /**
   * Retrieves a range for a given dimension name. The template datatype must be
   * the same as that of the underlying array.
   *
   * @tparam T The dimension datatype.
   * @param dim_name The dimension name.
   * @return A duplex of the form (start, end).
   */
  template <class T>
  std::array<T, 2> range(const std::string& dim_name) {
    auto& ctx = ctx_.get();
    tiledb_range_t range;
    ctx.handle_error(tiledb_ndrectangle_get_range_from_name(
        ctx.ptr().get(), ndrect_.get(), dim_name.c_str(), &range));

    return {*(const T*)range.min, *(const T*)range.max};
  }

  /**
   * Retrieves a range for a given dimension index. The template datatype must
   * be the same as that of the underlying array.
   *
   * @tparam T The dimension datatype.
   * @param dim_idx The dimension index.
   * @return A duplex of the form (start, end).
   */
  template <class T>
  std::array<T, 2> range(unsigned dim_idx) {
    auto& ctx = ctx_.get();
    tiledb_range_t range;
    ctx.handle_error(tiledb_ndrectangle_get_range(
        ctx.ptr().get(), ndrect_.get(), dim_idx, &range));

    return {*(const T*)range.min, *(const T*)range.max};
  }

  /**
   * Returns the C TileDB ndrect object.
   *
   * @return The C pointer to the NDRectangle object
   */
  std::shared_ptr<tiledb_ndrectangle_t> ptr() const {
    return ndrect_;
  }
  /**
   * Get the data type of the range at idx
   *
   * @param dim_idx The dimension index.
   * @return The datatype of the range.
   */
  tiledb_datatype_t range_dtype(unsigned dim_idx) {
    auto& ctx = ctx_.get();

    tiledb_datatype_t dtype;
    ctx.handle_error(tiledb_ndrectangle_get_dtype(
        ctx.ptr().get(), ndrect_.get(), dim_idx, &dtype));

    return dtype;
  }

  /**
   * Get the data type of the range by name
   *
   * @param dim_name The dimension name.
   * @return The datatype of the range.
   */
  tiledb_datatype_t range_dtype(const std::string& dim_name) {
    auto& ctx = ctx_.get();

    tiledb_datatype_t dtype;
    ctx.handle_error(tiledb_ndrectangle_get_dtype_from_name(
        ctx.ptr().get(), ndrect_.get(), dim_name.c_str(), &dtype));

    return dtype;
  }

  /**
   * Get the number of dimensions associated with the NDRectangle.
   *
   * @return The number of dimensions.
   */
  uint32_t dim_num() {
    auto& ctx = ctx_.get();

    uint32_t ndim;
    ctx.handle_error(
        tiledb_ndrectangle_get_dim_num(ctx.ptr().get(), ndrect_.get(), &ndim));

    return ndim;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_ndrectangle_t> ndrect_;

  /** An auxiliary deleter. */
  impl::Deleter deleter_;
};

/**
 * Retrieves a range for a given variable length string dimension index and
 * range id.
 *
 * @param dim_name The dimension name.
 * @param range_idx The range index.
 * @return A pair of the form (start, end).
 */
template <>
inline std::array<std::string, 2> NDRectangle::range<std::string>(
    const std::string& dim_name) {
  auto& ctx = ctx_.get();
  tiledb_range_t range;
  ctx.handle_error(tiledb_ndrectangle_get_range_from_name(
      ctx.ptr().get(), ndrect_.get(), dim_name.c_str(), &range));

  std::string start_str(static_cast<const char*>(range.min), range.min_size);
  std::string end_str(static_cast<const char*>(range.max), range.max_size);

  return {std::move(start_str), std::move(end_str)};
}

/**
 * Retrieves a range for a given variable length string dimension index and
 * range id.
 *
 * @param dim_idx The dimension index.
 * @param range_idx The range index.
 * @return A pair of the form (start, end).
 */
template <>
inline std::array<std::string, 2> NDRectangle::range(unsigned dim_idx) {
  auto& ctx = ctx_.get();
  tiledb_range_t range;

  ctx.handle_error(tiledb_ndrectangle_get_range(
      ctx.ptr().get(), ndrect_.get(), dim_idx, &range));

  std::string start_str(static_cast<const char*>(range.min), range.min_size);
  std::string end_str(static_cast<const char*>(range.max), range.max_size);

  return {std::move(start_str), std::move(end_str)};
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_NDRECTANGLE_H
