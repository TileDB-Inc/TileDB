/**
 * @file unit-lru_cache.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file unit-tests class BufferLRUCache.
 */

#include "catch.hpp"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/cache/buffer_lru_cache.h"

using namespace tiledb::common;
using namespace tiledb::sm;

#define CACHE_SIZE 10 * sizeof(int)
#define CACHE_ZERO_SIZE 0

struct BufferLRUCacheFx {
  BufferLRUCache* lru_cache_;

  BufferLRUCacheFx() {
    lru_cache_ = new BufferLRUCache(CACHE_SIZE);
  }

  ~BufferLRUCacheFx() {
    delete lru_cache_;
  }

  bool check_key_order(const std::string& golden_order) {
    auto it = lru_cache_->item_iter_begin();
    auto it_end = lru_cache_->item_iter_end();
    std::string keys;
    for (; it != it_end; ++it)
      keys += it->key_;
    return keys == golden_order;
  }
};

TEST_CASE_METHOD(
    BufferLRUCacheFx, "Unit-test class BufferLRUCache", "[lru_cache]") {
  // Insert an object larger than CACHE_SIZE
  Buffer v;
  CHECK(v.realloc(CACHE_SIZE + 1).ok());
  bool success;
  Status st = lru_cache_->insert("key", std::move(v));
  CHECK(st.ok());
  Buffer v_buf;
  st = lru_cache_->read("key", &v_buf, 0, sizeof(int), &success);
  CHECK(st.ok());
  CHECK(!success);

  // Prepare some vectors
  Buffer v1;
  Buffer v2;
  Buffer v3;
  CHECK(v1.realloc(sizeof(int) * 3).ok());
  CHECK(v2.realloc(sizeof(int) * 3).ok());
  CHECK(v3.realloc(sizeof(int) * 3).ok());
  v1.set_size(v1.alloced_size());
  v2.set_size(v2.alloced_size());
  v3.set_size(v3.alloced_size());

  for (int i = 0; i < 3; ++i) {
    static_cast<int*>(v1.data())[i] = i;
    static_cast<int*>(v2.data())[i] = 3 + i;
    static_cast<int*>(v3.data())[i] = 6 + i;
  }

  // Insert 3 items in the cache
  Buffer v1_cp(v1);
  st = lru_cache_->insert("v1", std::move(v1_cp));
  CHECK(st.ok());
  Buffer v2_cp(v2);
  st = lru_cache_->insert("v2", std::move(v2_cp));
  CHECK(st.ok());
  Buffer v3_cp(v3);
  st = lru_cache_->insert("v3", std::move(v3_cp));
  CHECK(st.ok());

  // Check that the order in the linked list is v1-v2-v3
  CHECK(check_key_order("v1v2v3"));

  // Read non-existent item
  v_buf.reset_offset();
  st = lru_cache_->read("v", &v_buf, 0, sizeof(int), &success);
  CHECK(st.ok());
  CHECK(!success);

  // Read full v3
  v_buf.reset_offset();
  st = lru_cache_->read("v3", &v_buf, 0, 3 * sizeof(int), &success);
  CHECK(st.ok());
  CHECK(success);
  CHECK(!memcmp(v_buf.data(), v3.data(), 3 * sizeof(int)));

  // Check that the order in the linked list is v1-v2-v3
  CHECK(check_key_order("v1v2v3"));

  // Read partial v2
  v_buf.reset_offset();
  st = lru_cache_->read("v2", &v_buf, sizeof(int), sizeof(int), &success);
  CHECK(st.ok());
  CHECK(success);
  CHECK(v_buf.value<int>(0) == v2.value<int>(1 * sizeof(int)));

  // Check that the order in the linked list is v1-v3-v2
  CHECK(check_key_order("v1v3v2"));

  // Read out of bounds
  v_buf.reset_offset();
  st = lru_cache_->read("v2", &v_buf, sizeof(int), 4 * sizeof(int), &success);
  CHECK(!st.ok());

  // Test eviction
  Buffer v4;
  CHECK(v4.realloc(sizeof(int) * 5).ok());
  st = lru_cache_->insert("v4", std::move(v4));
  CHECK(st.ok());

  // Check that the order in the linked list is v2-v4
  CHECK(check_key_order("v2v4"));

  // Test clear
  lru_cache_->clear();
  auto it = lru_cache_->item_iter_begin();
  auto it_end = lru_cache_->item_iter_end();
  CHECK(it == it_end);
}

TEST_CASE_METHOD(
    BufferLRUCacheFx, "BufferLRUCache item invalidation", "[lru_cache]") {
  Buffer v1;
  Buffer v2;
  Buffer v3;
  Buffer v4;
  CHECK(v1.realloc(sizeof(int) * 3).ok());
  CHECK(v2.realloc(sizeof(int) * 3).ok());
  CHECK(v3.realloc(sizeof(int) * 3).ok());
  CHECK(v4.realloc(sizeof(int) * 4).ok());
  v1.set_size(v1.alloced_size());
  v2.set_size(v2.alloced_size());
  v3.set_size(v3.alloced_size());
  v4.set_size(v4.alloced_size());
  for (int i = 0; i < 3; ++i) {
    static_cast<int*>(v1.data())[i] = i + 1;
    static_cast<int*>(v2.data())[i] = 3 + i + 1;
    static_cast<int*>(v3.data())[i] = 6 + i + 1;
    static_cast<int*>(v4.data())[i] = 9 + i + 1;
  }

  Buffer v1_cp(v1);
  Status st = lru_cache_->insert("v1", std::move(v1_cp));
  CHECK(st.ok());
  Buffer v2_cp(v2);
  st = lru_cache_->insert("v2", std::move(v2_cp));
  CHECK(st.ok());

  // Check invalidate non-existent key
  bool success;
  st = lru_cache_->invalidate("key", &success);
  CHECK(st.ok());
  CHECK(!success);
  CHECK(check_key_order("v1v2"));

  // Check invalidate head of list
  st = lru_cache_->invalidate("v1", &success);
  CHECK(st.ok());
  CHECK(success);
  CHECK(check_key_order("v2"));
  st = lru_cache_->invalidate("v1", &success);
  CHECK(st.ok());
  CHECK(!success);
  CHECK(check_key_order("v2"));

  Buffer v3_cp(v3);
  st = lru_cache_->insert("v3", std::move(v3_cp));
  CHECK(st.ok());
  Buffer v4_cp(v4);
  st = lru_cache_->insert("v4", std::move(v4_cp));
  CHECK(st.ok());
  CHECK(check_key_order("v2v3v4"));

  // Check invalidate middle of list
  st = lru_cache_->invalidate("v3", &success);
  CHECK(st.ok());
  CHECK(success);
  CHECK(check_key_order("v2v4"));

  // Check invalidate end of list
  st = lru_cache_->invalidate("v4", &success);
  CHECK(st.ok());
  CHECK(success);
  CHECK(check_key_order("v2"));

  // Check invalidate final element
  st = lru_cache_->invalidate("v2", &success);
  CHECK(st.ok());
  CHECK(success);
  auto it = lru_cache_->item_iter_begin();
  auto it_end = lru_cache_->item_iter_end();
  CHECK(it == it_end);
}

TEST_CASE("BufferLRUCache of 0 capacity", "[lru_cache]") {
  // Create 0 size cache
  BufferLRUCache* lru_cache = new BufferLRUCache(CACHE_ZERO_SIZE);
  auto it = lru_cache->item_iter_begin();
  auto it_end = lru_cache->item_iter_end();
  CHECK(it == it_end);

  // Test insert
  Buffer v;
  CHECK(v.realloc(CACHE_ZERO_SIZE + 1).ok());
  Status st = lru_cache->insert("key", std::move(v));
  CHECK(st.ok());

  // Test read
  Buffer v_buf;
  bool success;
  st = lru_cache->read("key", &v_buf, 0, sizeof(int), &success);
  CHECK(st.ok());
  CHECK(!success);

  // Test invalidate
  st = lru_cache->invalidate("key", &success);
  CHECK(st.ok());
  CHECK(!success);

  // Test clear
  lru_cache->clear();
  it = lru_cache->item_iter_begin();
  it_end = lru_cache->item_iter_end();
  CHECK(it == it_end);

  delete lru_cache;
}