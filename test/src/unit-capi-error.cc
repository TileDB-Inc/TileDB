/**
 * @file   unit-capi-error.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests for the C API error return code.
 */

#include "catch.hpp"
#include "tiledb.h"

#include <iostream>

TEST_CASE("C API: Test error and error message", "[capi], [error]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_create(&ctx, nullptr);
  CHECK(rc == TILEDB_OK);

  const char* bad_path = nullptr;
  rc = tiledb_group_create(ctx, bad_path);
  CHECK(rc == TILEDB_ERR);

  tiledb_error_t* err;
  rc = tiledb_error_last(ctx, &err);
  CHECK(rc == TILEDB_OK);

  const char* errmsg;
  rc = tiledb_error_message(ctx, err, &errmsg);
  CHECK(rc == TILEDB_OK);
  CHECK_THAT(
      errmsg, Catch::Equals("Error: Invalid group directory argument is NULL"));

  // Clean up
  tiledb_error_free(ctx, err);
  tiledb_ctx_free(ctx);
}
