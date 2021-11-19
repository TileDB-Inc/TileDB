/**
 * @file   error.h
 *
 * @author Rebekah Davis
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
 * This file declares the C++ API for the TileDB Error object.
 */

#ifndef TILEDB_CPP_API_ERROR_H
#define TILEDB_CPP_API_ERROR_H

#include "context.h"
#include "deleter.h"
#include "tiledb.h"

namespace tiledb {

/**
 * Represents a C++ error.
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Error error(ctx);
 * const char* message = error.error_message();
 * printf("Last error: %s\n", message);
 * @endcode
 */
class Error {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Construct an Error.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Error error(ctx);
   * @endcode
   *
   * @param ctx TileDB context
   */
  Error(const Context& ctx)
      : ctx_(ctx) {
    tiledb_error_t* error;
    tiledb_ctx_get_last_error(ctx.ptr().get(), &error);
    error_ = std::shared_ptr<tiledb_error_t>(error, deleter_);
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /** Returns the C TileDB error object. */
  std::shared_ptr<tiledb_error_t> ptr() const {
    return error_;
  }

  const std::string error_message() {
    const char* msg = nullptr;
    tiledb_error_message(error_.get(), &msg);
    return std::string(msg);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** The TileDB C config object. */
  std::shared_ptr<tiledb_error_t> error_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_ERROR_H