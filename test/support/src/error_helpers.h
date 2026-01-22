/**
 * @file   error_helpers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file declares helper functions, macros, etc for reporting errors.
 */

#ifndef TILEDB_TEST_ERROR_HELPERS_H
#define TILEDB_TEST_ERROR_HELPERS_H

#include "tiledb.h"

#include <test/support/assert_helpers.h>

#include <stdexcept>
#include <string>

/**
 * Calls a C API function and returns its status if it is not TILEDB_OK.
 */
#define RETURN_IF_ERR(thing) \
  do {                       \
    auto rc = (thing);       \
    if (rc != TILEDB_OK) {   \
      return rc;             \
    }                        \
  } while (0)

namespace tiledb::test {

/**
 * Helper function for not just reporting the return code of a C API call
 * but also the error message.
 *
 * Usage:
 * ```
 * auto rc = c_api_invocation();
 * REQUIRE(std::optional<std::string>() == error_if_any(rc));
 * ```
 */
std::optional<std::string> error_if_any(tiledb_ctx_t* ctx, auto apirc) {
  if (apirc == TILEDB_OK) {
    return std::nullopt;
  }

  tiledb_error_t* error = NULL;
  if (tiledb_ctx_get_last_error(ctx, &error) != TILEDB_OK) {
    return "Internal error: tiledb_ctx_get_last_error";
  } else if (error == nullptr) {
    // probably should be unreachable
    return "";
  }

  const char* msg;
  if (tiledb_error_message(error, &msg) != TILEDB_OK) {
    return "Internal error: tiledb_error_message";
  } else {
    return std::string(msg);
  }
}

/**
 * Asserts that a C API call does not return error.
 */
template <typename Asserter>
void capi_try(tiledb_ctx_t* ctx, int rc) {
  const std::optional<std::string> maybe_err =
      tiledb::test::error_if_any(ctx, rc);
  ASSERTER(maybe_err == std::optional<std::string>{});
}

/**
 * Throws a `std::runtime_error` if the operation returning `thing`
 * did not return `TILEDB_OK`.
 */
void throw_if_error(tiledb_ctx_t* ctx, capi_return_t thing);

}  // namespace tiledb::test

/**
 * Asserts that a C API call does not return error.
 */
#define TRY(ctx, thing) tiledb::test::capi_try<Asserter>(ctx, thing)

#endif
