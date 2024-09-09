/**
 * @file tiledb/api/c_api/buffer_list/test/unit_capi_buffer_list.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB Inc.
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
 * Tests for TileDB's buffer list C API.
 */

#include <test/support/tdb_catch.h>

#include "../buffer_list_api_external.h"
#include "../buffer_list_api_internal.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/buffer_list.h"

TEST_CASE(
    "C API: tiledb_buffer_list_alloc argument validation",
    "[capi][buffer_list]") {
  tiledb_ctx_t* ctx;
  capi_return_t rc = tiledb_ctx_alloc(nullptr, &ctx);
  tiledb_buffer_list_t* buf_list;

  SECTION("success") {
    rc = tiledb_buffer_list_alloc(ctx, &buf_list);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(buf_list != nullptr);
    tiledb_buffer_list_free(&buf_list);
  }
  SECTION("null context") {
    rc = tiledb_buffer_list_alloc(nullptr, &buf_list);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null buffer list") {
    rc = tiledb_buffer_list_alloc(ctx, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_ctx_free(&ctx);
}

TEST_CASE(
    "C API: tiledb_buffer_list_free argument validation",
    "[capi][buffer_list]") {
  REQUIRE_NOTHROW(tiledb_buffer_list_free(nullptr));
}

struct ordinary_buffer_list {
  tiledb_ctx_handle_t* ctx{nullptr};
  tiledb_buffer_list_handle_t* buffer_list{nullptr};
  ordinary_buffer_list() {
    auto rc = tiledb_ctx_alloc(nullptr, &ctx);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test context");
    }
    rc = tiledb_buffer_list_alloc(ctx, &buffer_list);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test buffer list");
    }
    if (buffer_list == nullptr) {
      throw std::logic_error(
          "tiledb_buffer_list_alloc returned OK but without buffer list");
    }
  }
  ~ordinary_buffer_list() {
    tiledb_buffer_list_free(&buffer_list);
    tiledb_ctx_free(&ctx);
  }
};

TEST_CASE(
    "C API: tiledb_buffer_list_get_num_buffers argument validation",
    "[capi][buffer_list]") {
  ordinary_buffer_list x;
  uint64_t num_buff;

  SECTION("success") {
    auto rc{
        tiledb_buffer_list_get_num_buffers(x.ctx, x.buffer_list, &num_buff)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    CHECK(num_buff == 0);
  }
  SECTION("null context") {
    auto rc{
        tiledb_buffer_list_get_num_buffers(nullptr, x.buffer_list, &num_buff)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null buffer_list") {
    auto rc{tiledb_buffer_list_get_num_buffers(x.ctx, nullptr, &num_buff)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null num_buffers") {
    auto rc{tiledb_buffer_list_get_num_buffers(x.ctx, x.buffer_list, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_buffer_list_get_buffer argument validation",
    "[capi][buffer_list]") {
  ordinary_buffer_list x;
  uint64_t idx = 0;
  tiledb_buffer_t* buff;
  /*
   * "success" case in [capi][buffer][bufferlist]; too much overhead to set up.
   */
  SECTION("null context") {
    auto rc{tiledb_buffer_list_get_buffer(nullptr, x.buffer_list, idx, &buff)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null buffer_list") {
    auto rc{tiledb_buffer_list_get_buffer(x.ctx, nullptr, idx, &buff)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null buffer") {
    auto rc{tiledb_buffer_list_get_buffer(x.ctx, x.buffer_list, idx, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_buffer_list_get_total_size argument validation",
    "[capi][buffer_list]") {
  ordinary_buffer_list x;
  uint64_t size;

  SECTION("success") {
    auto rc{tiledb_buffer_list_get_total_size(x.ctx, x.buffer_list, &size)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    CHECK(size == 0);
  }
  SECTION("null context") {
    auto rc{tiledb_buffer_list_get_total_size(nullptr, x.buffer_list, &size)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null buffer list") {
    auto rc{tiledb_buffer_list_get_total_size(x.ctx, nullptr, &size)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null size") {
    auto rc{tiledb_buffer_list_get_total_size(x.ctx, x.buffer_list, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_buffer_list_flatten argument validation",
    "[capi][buffer_list]") {
  ordinary_buffer_list x;
  tiledb_buffer_t* buff;
  /*
   * "success" case in [capi][buffer][bufferlist]; too much overhead to set up.
   */
  SECTION("null context") {
    auto rc{tiledb_buffer_list_flatten(nullptr, x.buffer_list, &buff)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null buffer list") {
    auto rc{tiledb_buffer_list_flatten(x.ctx, nullptr, &buff)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null buffer") {
    auto rc{tiledb_buffer_list_flatten(x.ctx, x.buffer_list, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: Test BufferList get buffers", "[capi][buffer][bufferlist]") {
  // Create a testing buffer list
  ordinary_buffer_list x;
  auto& buff1{x.buffer_list->buffer_list().emplace_buffer()};
  auto& buff2{x.buffer_list->buffer_list().emplace_buffer()};
  const char data1[3] = {1, 2, 3}, data2[4] = {4, 5, 6, 7};
  buff1.assign(span(data1, sizeof(data1)));
  buff2.assign(span(data2, sizeof(data2)));

  // Check num buffers and size
  uint64_t num_buffers = 123;
  REQUIRE(
      tiledb_buffer_list_get_num_buffers(x.ctx, x.buffer_list, &num_buffers) ==
      TILEDB_OK);
  REQUIRE(num_buffers == 2);
  uint64_t total_size = 123;
  REQUIRE(
      tiledb_buffer_list_get_total_size(x.ctx, x.buffer_list, &total_size) ==
      TILEDB_OK);
  REQUIRE(total_size == sizeof(data1) + sizeof(data2));

  // Check flatten
  tiledb_buffer_t* tmp;
  REQUIRE(tiledb_buffer_list_flatten(x.ctx, x.buffer_list, &tmp) == TILEDB_OK);
  void* tmp_data;
  uint64_t tmp_size;
  REQUIRE(
      tiledb_buffer_get_data(x.ctx, tmp, &tmp_data, &tmp_size) == TILEDB_OK);
  REQUIRE(tmp_size == total_size);
  REQUIRE(!std::memcmp((char*)tmp_data, &data1[0], sizeof(data1)));
  REQUIRE(
      !std::memcmp((char*)tmp_data + sizeof(data1), &data2[0], sizeof(data2)));
  tiledb_buffer_free(&tmp);

  // Get each buffer
  tiledb_buffer_t* b = nullptr;
  REQUIRE(
      tiledb_buffer_list_get_buffer(x.ctx, x.buffer_list, 0, &b) == TILEDB_OK);
  void* b_data = nullptr;
  uint64_t b_size = 123;
  REQUIRE(tiledb_buffer_get_data(x.ctx, b, &b_data, &b_size) == TILEDB_OK);
  REQUIRE(b_size == sizeof(data1));
  REQUIRE(!std::memcmp(b_data, &data1[0], sizeof(data1)));
  tiledb_buffer_free(&b);

  REQUIRE(
      tiledb_buffer_list_get_buffer(x.ctx, x.buffer_list, 1, &b) == TILEDB_OK);
  REQUIRE(tiledb_buffer_get_data(x.ctx, b, &b_data, &b_size) == TILEDB_OK);
  REQUIRE(b_size == sizeof(data2));
  REQUIRE(!std::memcmp(b_data, &data2[0], sizeof(data2)));
  tiledb_buffer_free(&b);

  REQUIRE(
      tiledb_buffer_list_get_buffer(x.ctx, x.buffer_list, 2, &b) == TILEDB_ERR);
}
