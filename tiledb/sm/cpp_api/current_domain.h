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
class CurrentDomain {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Creates a TileDB current_domain object.
   *
   * @param ctx TileDB context
   * @param type The TileDB currentDomain type
   */
  explicit CurrentDomain(const Context& ctx)
      : ctx_(ctx) {
    tiledb_current_domain_t* cd;
    ctx.handle_error(tiledb_current_domain_create(ctx.ptr().get(), &cd));
    current_domain_ = std::shared_ptr<tiledb_current_domain_t>(cd, deleter_);
  }

  /**
   * Creates a TileDB current_domain object.
   *
   * @param ctx TileDB context
   * @param cd The TileDB currentDomain C api pointer
   */
  CurrentDomain(const tiledb::Context& ctx, tiledb_current_domain_t* cd)
      : ctx_(ctx) {
    current_domain_ = std::shared_ptr<tiledb_current_domain_t>(cd, deleter_);
  }

  CurrentDomain(const CurrentDomain&) = default;
  CurrentDomain(CurrentDomain&&) = default;
  CurrentDomain& operator=(const CurrentDomain&) = default;
  CurrentDomain& operator=(CurrentDomain&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Returns a shared pointer to the C TileDB currentDomain object.
   *
   * @return The C API pointer
   */
  std::shared_ptr<tiledb_current_domain_t> ptr() const {
    return current_domain_;
  }

  /**
   * Returns the currentDomain type.
   *
   * @return The currentDomain type.
   */
  tiledb_current_domain_type_t type() const {
    tiledb_current_domain_type_t type;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_current_domain_get_type(
        ctx.ptr().get(), current_domain_.get(), &type));
    return type;
  }

  /**
   * Set a N-dimensional rectangle representation on a current domain.
   * Error if the current domain passed is not empty.
   *
   * @param ndrect The N-dimensional rectangle to be used.
   * @return Reference to this `currentDomain` instance.
   */
  CurrentDomain& set_ndrectangle(const NDRectangle& ndrect) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_current_domain_set_ndrectangle(
        ctx.ptr().get(), current_domain_.get(), ndrect.ptr().get()));

    return *this;
  }

  /**
   * Get the N-dimensional rectangle associated with the current domain object,
   * error if the current domain is empty or a different representation is set.
   *
   * @return The N-dimensional rectangle of the current domain
   */
  NDRectangle ndrectangle() const {
    tiledb_ndrectangle_t* nd;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_current_domain_get_ndrectangle(
        ctx.ptr().get(), current_domain_.get(), &nd));
    return NDRectangle(ctx, nd);
  }

  /**
   * Check if the current_domain is empty
   *
   * @return True if the currentDomain is empty
   */
  bool is_empty() const {
    uint32_t ret;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_current_domain_get_is_empty(
        ctx.ptr().get(), current_domain_.get(), &ret));
    return static_cast<bool>(ret);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Pointer to the TileDB C current_domain object. */
  std::shared_ptr<tiledb_current_domain_t> current_domain_;

  /** An auxiliary deleter. */
  impl::Deleter deleter_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_CURRENT_DOMAIN_H
