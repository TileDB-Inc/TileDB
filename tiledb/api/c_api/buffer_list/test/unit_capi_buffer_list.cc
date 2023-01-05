/**
 * @file   tiledb/api/c_api/buffer_list/test/unit_capi_buffer_list.cc
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
 * Tests for TileDB's buffer_list C API.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/buffer_list.h"
#include "../buffer_list_api_external.h"
#include "../buffer_list_api_internal.h"

using namespace tiledb::sm;

TEST_CASE("C API: Test empty BufferList", "[capi][buffer][bufferlist]") {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);
  tiledb_buffer_list_t* buffer_list = nullptr;
  REQUIRE(tiledb_buffer_list_alloc(ctx, &buffer_list) == TILEDB_OK);

  uint64_t num_buffers = 123;
  REQUIRE(
      tiledb_buffer_list_get_num_buffers(ctx, buffer_list, &num_buffers) ==
      TILEDB_OK);
  REQUIRE(num_buffers == 0);

  uint64_t total_size = 123;
  REQUIRE(
      tiledb_buffer_list_get_total_size(ctx, buffer_list, &total_size) ==
      TILEDB_OK);
  REQUIRE(total_size == 0);

  tiledb_buffer_t* b;
  REQUIRE(tiledb_buffer_list_get_buffer(ctx, buffer_list, 0, &b) == TILEDB_ERR);

  tiledb_buffer_list_free(&buffer_list);
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C API: Test BufferList get buffers", "[capi][buffer][bufferlist]") {
  // Create a testing buffer list
  BufferList buffer_list;
  Buffer buff1, buff2;
  const char data1[3] = {1, 2, 3}, data2[4] = {4, 5, 6, 7};
  REQUIRE(buff1.write(data1, sizeof(data1)).ok());
  REQUIRE(buff2.write(data2, sizeof(data2)).ok());
  REQUIRE(buffer_list.add_buffer(std::move(buff1)).ok());
  REQUIRE(buffer_list.add_buffer(std::move(buff2)).ok());

  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);
  tiledb_buffer_list_t* c_buffer_list = nullptr;
  REQUIRE(tiledb_buffer_list_alloc(ctx, &c_buffer_list) == TILEDB_OK);
  // For testing only: set the underlying buffer list ptr to the one we created
  c_buffer_list->set_buffer_list(buffer_list);

  // Check num buffers and size
  uint64_t num_buffers = 123;
  REQUIRE(
      tiledb_buffer_list_get_num_buffers(ctx, c_buffer_list, &num_buffers) ==
      TILEDB_OK);
  REQUIRE(num_buffers == 2);
  uint64_t total_size = 123;
  REQUIRE(
      tiledb_buffer_list_get_total_size(ctx, c_buffer_list, &total_size) ==
      TILEDB_OK);
  REQUIRE(total_size == sizeof(data1) + sizeof(data2));

  // Check flatten
  tiledb_buffer_t* tmp;
  REQUIRE(tiledb_buffer_list_flatten(ctx, c_buffer_list, &tmp) == TILEDB_OK);
  void* tmp_data;
  uint64_t tmp_size;
  REQUIRE(tiledb_buffer_get_data(ctx, tmp, &tmp_data, &tmp_size) == TILEDB_OK);
  REQUIRE(tmp_size == total_size);
  REQUIRE(!std::memcmp((char*)tmp_data, &data1[0], sizeof(data1)));
  REQUIRE(
      !std::memcmp((char*)tmp_data + sizeof(data1), &data2[0], sizeof(data2)));
  tiledb_buffer_free(&tmp);

  // Get each buffer
  tiledb_buffer_t* b = nullptr;
  REQUIRE(
      tiledb_buffer_list_get_buffer(ctx, c_buffer_list, 0, &b) == TILEDB_OK);
  void* b_data = nullptr;
  uint64_t b_size = 123;
  REQUIRE(tiledb_buffer_get_data(ctx, b, &b_data, &b_size) == TILEDB_OK);
  REQUIRE(b_size == sizeof(data1));
  REQUIRE(!std::memcmp(b_data, &data1[0], sizeof(data1)));
  tiledb_buffer_free(&b);

  REQUIRE(
      tiledb_buffer_list_get_buffer(ctx, c_buffer_list, 1, &b) == TILEDB_OK);
  REQUIRE(tiledb_buffer_get_data(ctx, b, &b_data, &b_size) == TILEDB_OK);
  REQUIRE(b_size == sizeof(data2));
  REQUIRE(!std::memcmp(b_data, &data2[0], sizeof(data2)));
  tiledb_buffer_free(&b);

  REQUIRE(
      tiledb_buffer_list_get_buffer(ctx, c_buffer_list, 2, &b) == TILEDB_ERR);

  // Clean up
  tiledb_buffer_list_free(&c_buffer_list);
  tiledb_ctx_free(&ctx);
}
