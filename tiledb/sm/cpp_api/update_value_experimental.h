/**
 * @file   update_value.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB UpdateValue object.
 */

#ifndef TILEDB_CPP_API_UPDATE_VALUE_H
#define TILEDB_CPP_API_UPDATE_VALUE_H

#include "context.h"
#include "tiledb.h"

#include <string>
#include <type_traits>

namespace tiledb {

class UpdateValue {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Creates a TileDB update value object.
   * @param ctx TileDB context.
   */
  UpdateValue(
      const Context& ctx,
      const std::string& field_name,
      const void* condition_value,
      uint64_t condition_value_size)
      : ctx_(ctx) {
    tiledb_update_value_t* uv;
    ctx.handle_error(tiledb_update_value_alloc(
        ctx.ptr().get(),
        field_name.c_str(),
        condition_value,
        condition_value_size,
        &uv));
    update_value_ = std::shared_ptr<tiledb_update_value_t>(uv, deleter_);
  }

  /** Copy constructor. */
  UpdateValue(const UpdateValue&) = default;

  /** Move constructor. */
  UpdateValue(UpdateValue&&) = default;

  /** Destructor. */
  ~UpdateValue() = default;

  /**
   * Constructs an instance directly from a C-API query condition object.
   *
   * @param ctx The TileDB context.
   * @param qc The C-API query condition object.
   */
  UpdateValue(const Context& ctx, tiledb_update_value_t* const uv)
      : ctx_(ctx)
      , update_value_(std::shared_ptr<tiledb_update_value_t>(uv, deleter_)) {
  }

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Copy-assignment operator. */
  UpdateValue& operator=(const UpdateValue&) = default;

  /** Move-assignment operator. */
  UpdateValue& operator=(UpdateValue&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns a shared pointer to the C TileDB update value object. */
  std::shared_ptr<tiledb_update_value_t> ptr() const {
    return update_value_;
  }

  /**
   * Sets the update value.
   *
   * Note that more than one update value may be set on a query.
   *
   * @param update_value The query condition object.
   * @return Reference to this Query
   */
  UpdateValue& add_to_query(Query& query) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_query_add_update_value(
        ctx.ptr().get(), query.ptr().get(), update_value_.get()));
    return *this;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Pointer to the TileDB C query object. */
  std::shared_ptr<tiledb_update_value_t> update_value_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_UPDATE_VALUE_H
