/**
 * @file   tiledb/api/c_api/buffer/test/unit_capi_buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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

#include <catch2/catch_test_macros.hpp>
#include "../buffer_api_external.h"
#include "tiledb/api/c_api/context/context_api_external.h"

TEST_CASE("C API: Test buffer", "[capi][buffer]") {
  tiledb_ctx_handle_t* ctx{nullptr};
  tiledb_ctx_alloc(nullptr, &ctx);

  tiledb_buffer_t* buffer;
  REQUIRE(tiledb_buffer_alloc(ctx, &buffer) == TILEDB_OK);

  SECTION("- Check size and data pointer") {
    void* data;
    uint64_t size = 123;
    REQUIRE(tiledb_buffer_get_data(ctx, buffer, &data, &size) == TILEDB_OK);
    REQUIRE(data == nullptr);
    REQUIRE(size == 0);
  }

  SECTION("- Check get/set datatype") {
    tiledb_datatype_t type;
    REQUIRE(tiledb_buffer_get_type(ctx, buffer, &type) == TILEDB_OK);
    REQUIRE(type == TILEDB_UINT8);
    REQUIRE(tiledb_buffer_set_type(ctx, buffer, TILEDB_INT32) == TILEDB_OK);
    REQUIRE(tiledb_buffer_get_type(ctx, buffer, &type) == TILEDB_OK);
    REQUIRE(type == TILEDB_INT32);
  }

  SECTION("- Check get/set buffer") {
    const unsigned alloc_size = 123;
    void* alloc = std::malloc(alloc_size);
    REQUIRE(
        tiledb_buffer_set_data(ctx, buffer, alloc, alloc_size) == TILEDB_OK);

    // Check size/data
    void* data;
    uint64_t size = 123;
    REQUIRE(tiledb_buffer_get_data(ctx, buffer, &data, &size) == TILEDB_OK);
    REQUIRE(data == alloc);
    REQUIRE(size == alloc_size);

    // Check it works to set again
    REQUIRE(
        tiledb_buffer_set_data(ctx, buffer, alloc, alloc_size) == TILEDB_OK);

    // Buffers can alias
    tiledb_buffer_t* buffer2;
    REQUIRE(tiledb_buffer_alloc(ctx, &buffer2) == TILEDB_OK);
    REQUIRE(
        tiledb_buffer_set_data(ctx, buffer2, alloc, alloc_size) == TILEDB_OK);
    tiledb_buffer_free(&buffer2);

    // Check setting to nullptr works
    REQUIRE(tiledb_buffer_set_data(ctx, buffer, nullptr, 0) == TILEDB_OK);
    REQUIRE(tiledb_buffer_get_data(ctx, buffer, &data, &size) == TILEDB_OK);
    REQUIRE(size == 0);
    REQUIRE(data == nullptr);

    std::free(alloc);
  }

  tiledb_buffer_free(&buffer);
  tiledb_ctx_free(&ctx);
}
