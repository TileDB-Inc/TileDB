/**
 * @file unit-buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Tests the `BufferList` class.
 */

#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

#include <tiledb/sm/misc/tdb_catch.h>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE("BufferList: Test append", "[buffer][bufferlist]") {
  Buffer buff1, buff2;
  const char data1[3] = {1, 2, 3}, data2[4] = {4, 5, 6, 7};
  REQUIRE(buff1.write(data1, sizeof(data1)).ok());
  REQUIRE(buff2.write(data2, sizeof(data2)).ok());
  REQUIRE(buff1.data() != nullptr);
  REQUIRE(buff2.data() != nullptr);

  BufferList buffer_list;
  REQUIRE(buffer_list.num_buffers() == 0);
  REQUIRE(buffer_list.total_size() == 0);

  REQUIRE(buffer_list.add_buffer(std::move(buff1)).ok());
  REQUIRE(buffer_list.add_buffer(std::move(buff2)).ok());
  REQUIRE(buffer_list.num_buffers() == 2);
  REQUIRE(buffer_list.total_size() == sizeof(data1) + sizeof(data2));
  REQUIRE(buff1.data() == nullptr);
  REQUIRE(buff2.data() == nullptr);

  Buffer *b1 = nullptr, *b2 = nullptr;
  REQUIRE(buffer_list.get_buffer(0, &b1).ok());
  REQUIRE(buffer_list.get_buffer(1, &b2).ok());
  REQUIRE(!buffer_list.get_buffer(2, nullptr).ok());
  REQUIRE(b1->size() == sizeof(data1));
  REQUIRE(b2->size() == sizeof(data2));
  REQUIRE(!std::memcmp(b1->data(), &data1[0], sizeof(data1)));
  REQUIRE(!std::memcmp(b2->data(), &data2[0], sizeof(data2)));
}

TEST_CASE("BufferList: Test read", "[buffer][bufferlist]") {
  BufferList buffer_list;
  auto data = static_cast<char*>(std::malloc(10));

  REQUIRE(!buffer_list.read(data, 1).ok());
  REQUIRE(buffer_list.read(data, 0).ok());
  REQUIRE(buffer_list.read(nullptr, 0).ok());

  Buffer buff1, buff2;
  const char data1[3] = {1, 2, 3}, data2[4] = {4, 5, 6, 7};
  REQUIRE(buff1.write(data1, sizeof(data1)).ok());
  REQUIRE(buff2.write(data2, sizeof(data2)).ok());
  REQUIRE(buffer_list.add_buffer(std::move(buff1)).ok());
  REQUIRE(buffer_list.add_buffer(std::move(buff2)).ok());

  REQUIRE(buffer_list.read(data, 2).ok());
  REQUIRE(data[0] == 1);
  REQUIRE(data[1] == 2);
  REQUIRE(buffer_list.read(data + 2, 2).ok());
  REQUIRE(data[2] == 3);
  REQUIRE(data[3] == 4);
  REQUIRE(buffer_list.read(data + 4, 3).ok());
  REQUIRE(data[4] == 5);
  REQUIRE(data[5] == 6);
  REQUIRE(data[6] == 7);
  REQUIRE(!buffer_list.read(data, 1).ok());

  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  REQUIRE(buffer_list.read(data, 7).ok());
  REQUIRE(!std::memcmp(data, &data1[0], sizeof(data1)));
  REQUIRE(!std::memcmp(data + 3, &data2[0], sizeof(data2)));

  uint64_t num_read = 0;
  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  REQUIRE(buffer_list.read_at_most(data, 2, &num_read).ok());
  REQUIRE(num_read == 2);
  REQUIRE(!std::memcmp(data, &data1[0], 2));

  num_read = 0;
  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  REQUIRE(buffer_list.read_at_most(data, 100, &num_read).ok());
  REQUIRE(num_read == 7);
  REQUIRE(!std::memcmp(data, &data1[0], sizeof(data1)));
  REQUIRE(!std::memcmp(data + 3, &data2[0], sizeof(data2)));

  num_read = 0;
  std::memset(data, 0, 10);
  buffer_list.reset_offset();
  REQUIRE(buffer_list.read_at_most(data, 0, &num_read).ok());
  REQUIRE(num_read == 0);

  std::free(data);
}

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
  delete c_buffer_list->buffer_list_;
  c_buffer_list->buffer_list_ = &buffer_list;

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
  c_buffer_list->buffer_list_ = nullptr;
  tiledb_buffer_list_free(&c_buffer_list);
  tiledb_ctx_free(&ctx);
}