/**
 * @file   tiledb_cpp_api_context.cc
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "context.h"
#include "array_schema.h"
#include "exception.h"

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Context::Context() {
  tiledb_ctx_t* ctx;
  if (tiledb_ctx_create(&ctx, nullptr) != TILEDB_OK)
    throw TileDBError("[TileDB::C++API] Error: Failed to create context");
  ctx_ = std::shared_ptr<tiledb_ctx_t>(ctx, Context::free);
  error_handler_ = default_error_handler;
}

Context::Context(const Config& config) {
  tiledb_ctx_t* ctx;
  if (tiledb_ctx_create(&ctx, config) != TILEDB_OK)
    throw TileDBError("[TileDB::C++API] Error: Failed to create context");
  ctx_ = std::shared_ptr<tiledb_ctx_t>(ctx, Context::free);
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

std::shared_ptr<tiledb_ctx_t> Context::ptr() const {
  return ctx_;
}

Context::operator tiledb_ctx_t*() const {
  return ctx_.get();
}

Context& Context::set_error_handler(
    const std::function<void(const std::string&)>& fn) {
  error_handler_ = fn;
  return *this;
}

/* ********************************* */
/*          STATIC FUNCTIONS         */
/* ********************************* */

void Context::default_error_handler(const std::string& msg) {
  throw TileDBError(msg);
}

void Context::free(tiledb_ctx_t* ctx) {
  tiledb_ctx_free(&ctx);
}

}  // namespace tiledb
