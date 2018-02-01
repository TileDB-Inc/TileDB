/**
 * @file   tiledb_cpp_api_context.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB Context object.
 */

#ifndef TILEDB_CPP_API_CONTEXT_H
#define TILEDB_CPP_API_CONTEXT_H

#include "tiledb.h"
#include "tiledb_cpp_api_config.h"
#include "tiledb_cpp_api_object.h"

#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

namespace tdb {

class ArraySchema;

/**
 * A TileDB context wraps a TileDB storage manager.
 * Throws `runtime_error` exceptions.
 */
class Context {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Context();

  /** Constructor with config paramters. */
  explicit Context(const Config &config);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Error handler for the TileDB C API calls. Throws an exception
   * in case of error.
   *
   * @param rc If != TILEDB_OK, call error handler
   */
  void handle_error(int rc) const;

  /** Returns the C TileDB context object. */
  std::shared_ptr<tiledb_ctx_t> ptr() const;

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_ctx_t *() const;

  /**
   * Sets the error handler callback. If none is set, the
   * `default_error_handler` is used.
   */
  Context &set_error_handler(
      const std::function<void(const std::string &)> &fn);

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * The default error handler callback.
   *
   * @param msg An error message.
   */
  static void default_error_handler(const std::string &msg);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The C TileDB context object. */
  std::shared_ptr<tiledb_ctx_t> ctx_;

  /** An error handler callback. */
  std::function<void(const std::string &)> error_handler_;
};

}  // namespace tdb

#endif  // TILEDB_CPP_API_CONTEXT_H
