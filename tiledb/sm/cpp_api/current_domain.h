/**
 * @file   current_domain.h
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
 * This file declares the C++ API for the TileDB current_domain object.
 */

#ifndef TILEDB_CPP_API_CURRENT_DOMAIN_H
#define TILEDB_CPP_API_CURRENT_DOMAIN_H

#include "context.h"
#include "core_interface.h"
#include "deleter.h"
#include "exception.h"
#include "ndrectangle.h"
#include "tiledb.h"
#include "tiledb_experimental.h"
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
 *
 * @details
 * See examples for more usage details.
 *
 * **Example:**
 *
 * @code{.cpp}
 *
 * @endcode
 */
class CurrentDomain {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** The shape type. */
  enum class Shape {
    /** N-DImensional rectangle.*/
    NDRECTANGLE
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Creates a TileDB current_domain object.
   *
   * @param ctx TileDB context
   * @param type The TileDB shape type
   */
  CurrentDomain(const Context& ctx, tiledb_shape_type_t type)
      : ctx_(ctx) {
    tiledb_current_domain_t* cd;
    ctx.handle_error(tiledb_current_domain_alloc(ctx.ptr().get(), type, &cd));
    current_domain_ = std::shared_ptr<tiledb_current_domain_t>(cd, deleter_);
  }

  CurrentDomain(const CurrentDomain&) = default;
  CurrentDomain(CurrentDomain&&) = default;
  CurrentDomain& operator=(const CurrentDomain&) = default;
  CurrentDomain& operator=(CurrentDomain&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns a shared pointer to the C TileDB current_domain object. */
  std::shared_ptr<tiledb_current_domain_t> ptr() const {
    return current_domain_;
  }

  /** Returns the shape type. */
  Shape shape_type() const {
    tiledb_shape_type_t type;
    auto& ctx = ctx_.get();
    // ctx.handle_error(); TODO
    return to_status(type);
  }

  /**
   * Set the NDRectangle
   *
   * @param ndrect The ndrect to be used.
   */
  CurrentDomain& set_ndrect(const NDRectangle& ndrect) {
    auto& ctx = ctx_.get();
    // ctx.handle_error(); TODO

    return *this;
  }

  /**
   * Get the NDRectangle
   * @return NDRectangle
   */
  NDRectangle ndrect() const {
    tiledb_ndrect_t* nd;
    // TODO
    return NDRectangle(&nd);
  }

  /**
   * Returns `true` if the current_domain is empty
   */
  bool is_empty() const {
    int ret;
    auto& ctx = ctx_.get();
    // ctx.handle_error(); TODO
    return (bool)ret;
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the TileDB C current_domain object. */
  std::shared_ptr<tiledb_current_domain_t> current_domain_;
}
/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Get a string representation of a current_domain status for an output stream.
 */
inline std::ostream&
operator<<(std::ostream& os, const Shape& shape) {
  switch (shape) {
    case NDRECTANGLE:
      os << "NDRECTANGLE";
      break;
  }
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_CURRENT_DOMAIN_H
