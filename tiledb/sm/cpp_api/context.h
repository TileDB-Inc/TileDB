/**
 * @file   context.h
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
 * This file declares the C++ API for the TileDB Context object.
 */

#ifndef TILEDB_CPP_API_CONTEXT_H
#define TILEDB_CPP_API_CONTEXT_H

#include "config.h"
#include "exception.h"
#include "tiledb.h"

#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

namespace tiledb {

/**
 * A TileDB context wraps a TileDB storage manager "instance."
 * Most objects and functions will require a Context.
 *
 * Internal error handling is also defined by the Context; the default error
 * handler throws a TileDBError with a specific message.
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 * // Use ctx when creating other objects:
 * tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
 *
 * // Set a custom error handler:
 * ctx.set_error_handler([](const std::string &msg) {
 *     std::cerr << msg << std::endl;
 * });
 * @endcode
 *
 */
class Context {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor. Creates a TileDB Context with default configuration.
   * @throws TileDBError if construction fails
   */
  Context() {
    tiledb_ctx_t* ctx;
    if (tiledb_ctx_alloc(nullptr, &ctx) != TILEDB_OK)
      throw TileDBError("[TileDB::C++API] Error: Failed to create context");
    ctx_ = std::shared_ptr<tiledb_ctx_t>(ctx, Context::free);
    error_handler_ = default_error_handler;

    set_tag("x-tiledb-api-language", "c++");
  }

  /**
   * Constructor. Creates a TileDB context with the given configuration.
   * @throws TileDBError if construction fails
   */
  explicit Context(const Config& config) {
    tiledb_ctx_t* ctx;
    if (tiledb_ctx_alloc(config.ptr().get(), &ctx) != TILEDB_OK)
      throw TileDBError("[TileDB::C++API] Error: Failed to create context");
    ctx_ = std::shared_ptr<tiledb_ctx_t>(ctx, Context::free);
    error_handler_ = default_error_handler;

    set_tag("x-tiledb-api-language", "c++");
  }

  /**
   * Constructor. Creates a TileDB context from the given pointer.
   * @param own=true If false, disables underlying cleanup upon destruction.
   * @throws TileDBError if construction fails
   */
  Context(tiledb_ctx_t* ctx, bool own = true) {
    if (ctx == nullptr)
      throw TileDBError(
          "[TileDB::C++API] Error: Failed to create Context from pointer");

    ctx_ = std::shared_ptr<tiledb_ctx_t>(ctx, [own](tiledb_ctx_t* p) {
      if (own) {
        Context::free(p);
      }
    });

    error_handler_ = default_error_handler;

    set_tag("x-tiledb-api-language", "c++");
  }
  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Error handler for the TileDB C API calls. Throws an exception
   * in case of error.
   *
   * @param rc If != TILEDB_OK, calls error handler
   */
  void handle_error(int rc) const {
    // Do nothing if there is not error
    if (rc == TILEDB_OK)
      return;

    // Get error
    const auto& ctx = ctx_.get();
    tiledb_error_t* err = nullptr;
    const char* msg = nullptr;
    rc = tiledb_ctx_get_last_error(ctx, &err);
    if (rc != TILEDB_OK) {
      tiledb_error_free(&err);
      error_handler_("[TileDB::C++API] Error: Non-retrievable error occurred");
    }

    // Get error message
    rc = tiledb_error_message(err, &msg);
    if (rc != TILEDB_OK) {
      tiledb_error_free(&err);
      error_handler_("[TileDB::C++API] Error: Non-retrievable error occurred");
    }
    auto msg_str = std::string(msg);

    // Clean up
    tiledb_error_free(&err);

    // Throw exception
    error_handler_(msg_str);
  }

  /** Returns the C TileDB context object. */
  std::shared_ptr<tiledb_ctx_t> ptr() const {
    return ctx_;
  }

  /**
   * Sets the error handler callback. If none is set, the
   * `default_error_handler` is used. The callback accepts an error
   * message.
   *
   * @param fn Error handler callback function
   * @return Reference to this Context
   */
  Context& set_error_handler(
      const std::function<void(const std::string&)>& fn) {
    error_handler_ = fn;
    return *this;
  }

  /** Returns a copy of the configuration of the context. **/
  Config config() const {
    tiledb_config_t* c;
    handle_error(tiledb_ctx_get_config(ctx_.get(), &c));
    return Config(&c);
  }

  /**
   * Return true if the given filesystem backend is supported.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * bool s3_supported = ctx.is_supported_fs(TILEDB_S3);
   * @endcode
   *
   * @param fs Filesystem to check
   */
  bool is_supported_fs(tiledb_filesystem_t fs) const {
    int ret;
    handle_error(tiledb_ctx_is_supported_fs(ctx_.get(), fs, &ret));
    return ret != 0;
  }

  /**
   * Cancels all background or async tasks associated with this context.
   */
  void cancel_tasks() const {
    handle_error(tiledb_ctx_cancel_tasks(ctx_.get()));
  }

  /** Sets a string/string KV tag on the context. */
  void set_tag(const std::string& key, const std::string& value) {
    handle_error(tiledb_ctx_set_tag(ctx_.get(), key.c_str(), value.c_str()));
  }

  /** Returns a JSON-formatted string of the stats. */
  std::string stats() {
    char* c_str;
    handle_error(tiledb_ctx_get_stats(ctx_.get(), &c_str));

    // Copy `c_str` into `str`.
    std::string str(c_str);
    ::free(c_str);

    return str;
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * The default error handler callback.
   * @throws TileDBError with the error message
   */
  static void default_error_handler(const std::string& msg) {
    throw TileDBError(msg);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The C TileDB context object. */
  std::shared_ptr<tiledb_ctx_t> ctx_;

  /** An error handler callback. */
  std::function<void(const std::string&)> error_handler_;

  /** Wrapper function for freeing a context C object. */
  static void free(tiledb_ctx_t* ctx) {
    tiledb_ctx_free(&ctx);
  }
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_CONTEXT_H
