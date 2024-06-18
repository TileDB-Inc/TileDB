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

/**
 * Represents an NDRectangle. This is similar to the non empty domain API with
 *the exception is doesn’t differentiate between fixed and var size. It will be
 *used in the future to replace the return value of the non empty domain API.
 *Note that an NDRectangle object should have all dimensions specified before
 *it can be used in any call to set an ND rectangle
 *
 * @details
 * **Example:**
 *
 * @code{.cpp}
 *
 * TODO
 * @endcode
 *
 **/
class NDRectangle {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  NDRectangle(const tiledb::Context& ctx, const tiledb::Domain& domain)
      : ctx_(ctx)
      , domain_(domain) {
    tiledb_ndrectangle_t* capi_ndrect;
    ctx.handle_error(tiledb_ndrectangle_alloc(ctx.ptr().get(), &capi_ndrect));
    ndrect_ = std::shared_ptr<tiledb_ndrectangle_t>(capi_ndrect, deleter_);
  }

  NDRectangle(const NDRectangle&) = default;
  NDRectangle(NDRectangle&&) = default;
  NDRectangle& operator=(const NDRectangle&) = default;
  NDRectangle& operator=(NDRectangle&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Set the range on an N-dimensional rectangle for a dimension name,
   * **Example:**
   *
   * @code{.c}
   * tiledb_range_t range;
   * range.min = &min;
   * range.min_size = sizeof(min);
   * range.max = &max;
   * range.max_size = sizeof(max);
   * nd.set_range(ctx, "dim", &range);
   * @endcode
   *
   * @param dim_name The name of the dimension to add the range to.
   * @param range The range to add.
   * @return Reference to this NDRectangle.
   */
  NDRectangle& set_range(const std::string& dim_name, tiledb_range_t range) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_ndrectangle_set_range(
        ctx.ptr().get(), ndrect_.get(), dim_name.c_str(), &range));
    return *this;
  }

  /**
   * Set the range on an N-dimensional rectangle for dimension at idx,
   * **Example:**
   *
   * @code{.c}
   * tiledb_range_t range;
   * range.min = &min;
   * range.min_size = sizeof(min);
   * range.max = &max;
   * range.max_size = sizeof(max);
   * nd.set_range(ctx, 1, &range);
   * @endcode
   *
   * @param dim_idx The index of the dimension to add the range to.
   * @param range The range to add.
   * @return Reference to this NDRectangle.
   */
  NDRectangle& set_range(uint32_t dim_idx, tiledb_range_t range) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_ndrectangle_set_range(
        ctx.ptr().get(), ndrect_.get(), dim_idx, &range));
    return *this;
  }

  /**
   * Get the range set on an N-dimensional rectangle for a dimension name.
   *
   * @param dim_name The dimension name.
   * @return The requested range.
   */
  tiledb_range_t range(const std::string& dim_name) {
    auto& ctx = ctx_.get();
    tiledb_range_t range;
    ctx.handle_error(tiledb_ndrectangle_get_range_from_name(
        ctx.ptr().get(), ndrect_.get(), dim_name.c_str(), &range));
    return range;
  }

  /**
   * Get the range set on an N-dimensional rectangle for a dimension index.
   *
   * @param dim_idx The dimension index.
   * @return The requested range.
   */
  tiledb_range_t range(unsigned dim_idx) {
    auto& ctx = ctx_.get();
    tiledb_range_t range;
    ctx.handle_error(tiledb_ndrectangle_get_range(
        ctx.ptr().get(), ndrect_.get(), dim_idx, &range));
    return range;
  }

  /** Returns the C TileDB ndrect object. */
  std::shared_ptr<tiledb_ndrectangle_t> ptr() const {
    return ndrect_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** The domain of the schema the query targets at */
  Domain domain_;

  /** Pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_ndrectangle_t> ndrect_;
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Get a string representation of the domain for an output stream. */
inline std::ostream& operator<<(std::ostream& os, const NDRectangle& d) {
  os << "NDRectangle<";  // TODO
  os << '>';
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_NDRECTANGLE_H
