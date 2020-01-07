/**
 * @file unit-lru_cache.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file unit-tests class LRUCache.
 */

#include "catch.hpp"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/cache/lru_cache.h"

using namespace tiledb::sm;

#define CACHE_SIZE 10 * sizeof(int)

struct LRUCacheFx {
  LRUCache* lru_cache_;

  LRUCacheFx() {
    lru_cache_ = new LRUCache(CACHE_SIZE);
  }

  ~LRUCacheFx() {
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

TEST_CASE_METHOD(LRUCacheFx, "Unit-test class LRUCache", "[lru_cache]") {
  // Insert a null object
  Status st = lru_cache_->insert("key", nullptr, 20);
  CHECK(!st.ok());

  // Insert an object larger than CACHE_SIZE
  int v;
  bool success;
  st = lru_cache_->insert("key", &v, CACHE_SIZE + 1);
  CHECK(st.ok());
  Buffer v_buf;
  st = lru_cache_->read("key", &v_buf, 0, sizeof(int), &success);
  CHECK(st.ok());
  CHECK(!success);

  // Prepare some vectors
  auto v1 = static_cast<int*>(std::malloc(sizeof(int) * 3));
  auto v2 = static_cast<int*>(std::malloc(sizeof(int) * 3));
  auto v3 = static_cast<int*>(std::malloc(sizeof(int) * 3));
  for (int i = 0; i < 3; ++i) {
    v1[i] = i;
    v2[i] = 3 + i;
    v3[i] = 6 + i;
  }

  // Insert 3 items in the cache
  st = lru_cache_->insert("v1", v1, 3 * sizeof(int));
  CHECK(st.ok());
  st = lru_cache_->insert("v2", v2, 3 * sizeof(int));
  CHECK(st.ok());
  st = lru_cache_->insert("v3", v3, 3 * sizeof(int));
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
  CHECK(!memcmp(v_buf.data(), v3, 3 * sizeof(int)));

  // Check that the order in the linked list is v1-v2-v3
  CHECK(check_key_order("v1v2v3"));

  // Read partial v2
  v_buf.reset_offset();
  st = lru_cache_->read("v2", &v_buf, sizeof(int), sizeof(int), &success);
  CHECK(st.ok());
  CHECK(success);
  CHECK(v_buf.value<int>(0) == v2[1]);

  // Check that the order in the linked list is v1-v3-v2
  CHECK(check_key_order("v1v3v2"));

  // Read out of bounds
  v_buf.reset_offset();
  st = lru_cache_->read("v2", &v_buf, sizeof(int), 4 * sizeof(int), &success);
  CHECK(!st.ok());

  // Test eviction
  auto v4 = static_cast<int*>(std::malloc(sizeof(int) * 5));
  st = lru_cache_->insert("v4", v4, 5 * sizeof(int));
  CHECK(st.ok());

  // Check that the order in the linked list is v2-v4
  CHECK(check_key_order("v2v4"));

  // Test clear
  lru_cache_->clear();
  auto it = lru_cache_->item_iter_begin();
  auto it_end = lru_cache_->item_iter_end();
  CHECK(it == it_end);
}

TEST_CASE_METHOD(LRUCacheFx, "LRUCache item invalidation", "[lru_cache]") {
  auto v1 = static_cast<int*>(std::malloc(sizeof(int) * 3));
  auto v2 = static_cast<int*>(std::malloc(sizeof(int) * 3));
  auto v3 = static_cast<int*>(std::malloc(sizeof(int) * 3));
  auto v4 = static_cast<int*>(std::malloc(sizeof(int) * 3));
  for (int i = 0; i < 3; ++i) {
    v1[i] = i + 1;
    v2[i] = 3 + i + 1;
    v3[i] = 6 + i + 1;
    v4[i] = 9 + i + 1;
  }
  Status st = lru_cache_->insert("v1", &v1[0], 3 * sizeof(int));
  CHECK(st.ok());
  st = lru_cache_->insert("v2", &v2[0], 3 * sizeof(int));
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

  st = lru_cache_->insert("v3", &v3[0], 3 * sizeof(int));
  CHECK(st.ok());
  st = lru_cache_->insert("v4", &v4[0], 3 * sizeof(int));
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