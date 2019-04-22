/**
 * @file   unit-capi-buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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
 * Tests for TileDB's buffer C API.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"

TEST_CASE("C API: Test buffer", "[capi], [buffer]") {
  tiledb_ctx_t* ctx;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);
  tiledb_buffer_t* buffer;
  REQUIRE(tiledb_buffer_alloc(ctx, &buffer) == TILEDB_OK);

  uint64_t size = 123;
  REQUIRE(tiledb_buffer_get_size(ctx, buffer, &size) == TILEDB_OK);
  REQUIRE(size == 0);

  tiledb_datatype_t type;
  REQUIRE(tiledb_buffer_get_type(ctx, buffer, &type) == TILEDB_OK);
  REQUIRE(type == TILEDB_UINT8);
  REQUIRE(tiledb_buffer_set_type(ctx, buffer, TILEDB_INT32) == TILEDB_OK);
  REQUIRE(tiledb_buffer_get_type(ctx, buffer, &type) == TILEDB_OK);
  REQUIRE(type == TILEDB_INT32);

  tiledb_buffer_free(&buffer);
  tiledb_ctx_free(&ctx);
}
