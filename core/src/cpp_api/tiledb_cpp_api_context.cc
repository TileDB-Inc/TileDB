/**
 * @file   tiledb_cpp_api_context.cc
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
 * This file defines the C++ API for the TileDB Context object.
 */

#include "tiledb_cpp_api_context.h"
#include "tiledb_cpp_api_array_schema.h"

namespace tdb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Context::Context() {
  tiledb_ctx_t *ctx;
  if (tiledb_ctx_create(&ctx, nullptr) != TILEDB_OK)
    throw std::runtime_error(
        "[TileDB::C++API] Error: Failed to create context");
  ctx_ = std::shared_ptr<tiledb_ctx_t>(ctx, tiledb_ctx_free);
  error_handler_ = default_error_handler;
}

Context::Context(const Config &config) {
  tiledb_ctx_t *ctx;
  if (tiledb_ctx_create(&ctx, config.ptr()) != TILEDB_OK)
    throw std::runtime_error(
        "[TileDB::C++API] Error: Failed to create context");
  ctx_ = std::shared_ptr<tiledb_ctx_t>(ctx, tiledb_ctx_free);
  error_handler_ = default_error_handler;
}

/* ********************************* */
/*                API                */
/* ********************************* */

void Context::handle_error(int rc) const {
  // Do nothing if there is not error
  if (rc == TILEDB_OK)
    return;

  // Get error
  const auto &ctx = ctx_.get();
  tiledb_error_t *err = nullptr;
  const char *msg = nullptr;
  rc = tiledb_error_last(ctx, &err);
  if (rc != TILEDB_OK) {
    tiledb_error_free(ctx, err);
    error_handler_("[TileDB::C++API] Error: Non-retrievable error occurred");
  }

  // Get error message
  rc = tiledb_error_message(ctx, err, &msg);
  if (rc != TILEDB_OK) {
    tiledb_error_free(ctx, err);
    error_handler_("[TileDB::C++API] Error: Non-retrievable error occurred");
  }
  auto msg_str = std::string(msg);

  // Clean up
  tiledb_error_free(ctx, err);

  // Throw exception
  error_handler_(msg_str);
}

tiledb_ctx_t *Context::ptr() const {
  return ctx_.get();
}

Context &Context::set_error_handler(
    const std::function<void(const std::string &)> &fn) {
  error_handler_ = fn;
  return *this;
}

/* ********************************* */
/*          STATIC FUNCTIONS         */
/* ********************************* */

void Context::default_error_handler(const std::string &msg) {
  throw std::runtime_error(msg);
}

}  // namespace tdb
