/**
 * @file   unit-capi-context.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
 * Tests the C API context object.
 */

#include <string>
#include <thread>

#include <catch2/catch_test_macros.hpp>
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"

TEST_CASE("C API: Test context", "[capi][context]") {
  tiledb_config_t* config{nullptr};
  tiledb_error_t* error{nullptr};
  int rc{tiledb_config_alloc(&config, &error)};
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Check alloc and alloc_with_error on non-error case
  // It is allowed to create a thread pool with concurrency level = 0
  rc = tiledb_config_set(config, "sm.compute_concurrency_level", "0", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  tiledb_ctx_t* ctx{nullptr};
  rc = tiledb_ctx_alloc(config, &ctx);
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_free(&ctx);
  CHECK(ctx == nullptr);

  rc = tiledb_ctx_alloc_with_error(config, &ctx, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  tiledb_ctx_free(&ctx);
  CHECK(ctx == nullptr);

  // Now check with failure
  std::string too_large{
      std::to_string(256 * std::thread::hardware_concurrency())};
  rc = tiledb_config_set(
      config, "sm.compute_concurrency_level", too_large.c_str(), &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);

  rc = tiledb_ctx_alloc(config, &ctx);
  CHECK(rc == TILEDB_ERR);
  tiledb_ctx_free(&ctx);
  CHECK(ctx == nullptr);

  rc = tiledb_ctx_alloc_with_error(config, &ctx, &error);
  CHECK(rc == TILEDB_ERR);
  tiledb_ctx_free(&ctx);
  CHECK(ctx == nullptr);

  const char* err_msg;
  rc = tiledb_error_message(error, &err_msg);
  CHECK(rc == TILEDB_OK);
  CHECK(tiledb::stdx::string::ends_with(
      std::string(err_msg),
      "Error initializing thread pool of concurrency level " + too_large +
          "; Requested size too large"));

  // Check another non-failure
  // Set this to non-error value
  rc = tiledb_config_set(
      config,
      "sm.compute_concurrency_level",
      std::to_string(4).c_str(),
      &error);

  // Set deprecated config
  rc = tiledb_config_set(
      config, "sm.num_reader_threads", std::to_string(7).c_str(), &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);

  rc = tiledb_ctx_alloc(config, &ctx);
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_free(&ctx);
  CHECK(ctx == nullptr);

  tiledb_error_free(&error);
  tiledb_config_free(&config);
}
