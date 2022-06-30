/**
 * @file unit_pool_allocator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Tests the pool_allocator class.
 */

#include "unit_pool_allocator.h"
#include <array>
#include <memory>
#include "experimental/tiledb/common/dag/data_block/pool_allocator.h"

using namespace tiledb::common;

class big_class {
  [[maybe_unused]] std::array<char, 2 * 1024 * 1024> storage_;
};

class small_class {
  [[maybe_unused]] std::array<char, 4 * 1024> storage_;
};

/**
 * Test size of big and small classes
 */
TEST_CASE("Pool Allocator: Test chunk sizes", "[pool_allocator]") {
  CHECK(sizeof(big_class) == 2 * 1024 * 1024);
  CHECK(sizeof(small_class) == 4 * 1024);
}

template <class T>
void test_alloc() {
  pool_allocator<sizeof(T)> p;

  auto p1 = p.allocate();
  auto p2 = p.allocate();
  CHECK(p1 - p2 == sizeof(T));
}

template <class T>
void test_alloc_dealloc() {
  pool_allocator<sizeof(T)> p;

  auto p1 = p.allocate();
  auto p2 = p.allocate();
  CHECK(p1 - p2 == sizeof(T));

  p.deallocate(p2);
  p.deallocate(p1);
  auto q1 = p.allocate();
  auto q2 = p.allocate();
  CHECK(p1 == q1);
  CHECK(p2 == q2);
}

template <class T>
void test_alloc_dealloc_scan() {
  pool_allocator<sizeof(T)> p;

  auto p1 = p.allocate();
  auto p2 = p.allocate();
  CHECK(p1 - p2 == sizeof(T));

  p.deallocate(p2);
  p.deallocate(p1);
  auto q1 = p.allocate();
  auto q2 = p.allocate();
  CHECK(p1 == q1);
  CHECK(p2 == q2);
}

/**
 * Test allocation and deallocation
 */
TEST_CASE(
    "Pool Allocator: Test allocation and deallocation", "[pool_allocator]") {
  SECTION("big class") {
    test_alloc<big_class>();
  }

  SECTION("small class") {
    test_alloc<small_class>();
  }

  SECTION("big class") {
    test_alloc_dealloc<big_class>();
  }

  SECTION("small class") {
    test_alloc_dealloc<small_class>();
  }

  // scan_all(mark);
  // scan_all(sweep);

  // return 0;
}

template <class T>
void test_shared() {
  pool_allocator<sizeof(T)> p;
  auto p1 = p.allocate();
  auto d = [&](auto px) { p.deallocate(px); };

  // Create and destroy a shared pointer to p1 */
  std::shared_ptr<std::byte> u;
  {
    std::shared_ptr<std::byte> s(p1, d);
    CHECK(u.use_count() == 0);
    CHECK(s.use_count() == 1);
    u = s;

    // Two objects are sharing p1
    CHECK(s.use_count() == 2);
    CHECK(u.use_count() == 2);

    CHECK(s.get() == p1);
    CHECK(u.get() == p1);
    // s goes out of scope
  }
  CHECK(u.get() == p1);
  CHECK(u.use_count() == 1);

  // Get another object, should be different from p1
  auto p2 = p.allocate();
  CHECK(p1 != p2);
  p.deallocate(p2);

  // Get rid of u, should return p1 to pool
  u.reset();
  CHECK(u.use_count() == 0);

  // Get top object of pool, should be p1 again
  auto p3 = p.allocate();
  CHECK(p1 == p3);
}

/**
 * Test allocation and deallocation with std::shared_ptr
 */
TEST_CASE("Pool Allocator: Test use with std::shared_ptr", "[pool_allocator]") {
  SECTION("big class") {
    test_shared<big_class>();
  }
  SECTION("small class") {
    test_shared<small_class>();
  }
}
