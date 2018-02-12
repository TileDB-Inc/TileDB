/**
 * @file unit-lru_cache.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
  st = lru_cache_->read("key", &v, 0, sizeof(int), &success);
  CHECK(st.ok());
  CHECK(!success);

  // Prepare some vectors
  auto v1 = new int[3];
  auto v2 = new int[3];
  auto v3 = new int[3];
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
  st = lru_cache_->read("v", &v, 0, sizeof(int), &success);
  CHECK(st.ok());
  CHECK(!success);

  // Read full v3
  int b3[3];
  st = lru_cache_->read("v3", b3, 0, 3 * sizeof(int), &success);
  CHECK(st.ok());
  CHECK(success);
  CHECK(!memcmp(b3, v3, 3 * sizeof(int)));

  // Check that the order in the linked list is v1-v2-v3
  CHECK(check_key_order("v1v2v3"));

  // Read partial v2
  int b2;
  st = lru_cache_->read("v2", &b2, sizeof(int), sizeof(int), &success);
  CHECK(st.ok());
  CHECK(success);
  CHECK(b2 == v2[1]);

  // Check that the order in the linked list is v1-v3-v2
  CHECK(check_key_order("v1v3v2"));

  // Read out of bounds
  st = lru_cache_->read("v2", &b2, sizeof(int), 4 * sizeof(int), &success);
  CHECK(!st.ok());

  // Test eviction
  auto v4 = new int[5];
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
