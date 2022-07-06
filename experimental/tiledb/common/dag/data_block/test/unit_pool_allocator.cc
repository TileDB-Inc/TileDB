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
#include <vector>
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
  PoolAllocator<sizeof(T)> p{};

  auto p1 = p.allocate();
  auto p2 = p.allocate();
  CHECK(p1 - p2 == sizeof(T));
}

template <class T>
void test_alloc_dealloc() {
  PoolAllocator<sizeof(T)> p;

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
  PoolAllocator<sizeof(T)> p;

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
    "Pool Allocator: Test allocation and deallocation", "[PoolAllocator]") {
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
}

#if 0

template <class T>
void test_shared() {
  PoolAllocator<sizeof(T)> p;
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
TEST_CASE("Pool Allocator: Test use with std::shared_ptr", "[PoolAllocator]") {
  SECTION("big class") {
    test_shared<big_class>();
  }
  SECTION("small class") {
    test_shared<small_class>();
  }
}

template <class T>
void test_singleton() {
  auto& p = SingletonPoolAllocator<sizeof(T)>::get_instance();
  auto& r = SingletonPoolAllocator<sizeof(T)>::get_instance();

  auto p1 = p.allocate();
  auto p2 = p.allocate();
  CHECK(p1 - p2 == sizeof(T));

  p.deallocate(p2);
  p.deallocate(p1);
  auto q1 = p.allocate();
  auto q2 = p.allocate();
  CHECK(p1 == q1);
  CHECK(p2 == q2);

  p.deallocate(q2);
  p.deallocate(q1);
  auto r1 = r.allocate();
  auto r2 = r.allocate();
  CHECK(p1 == r1);
  CHECK(p2 == r2);
}

template <class T>
void test_pool_allocator() {
  /* Instantiate two allocators */
  auto p = PoolAllocator<sizeof(T)>{};
  auto r = PoolAllocator<sizeof(T)>{};

  auto p1 = p.allocate();
  auto p2 = p.allocate();
  CHECK(p1 - p2 == sizeof(T));

  p.deallocate(p2);
  p.deallocate(p1);
  auto q1 = p.allocate();
  auto q2 = p.allocate();
  CHECK(p1 == q1);
  CHECK(p2 == q2);

  p.deallocate(q2);
  p.deallocate(q1);

  /* Get an element from second allocator and compare to first */
  auto r1 = r.allocate();
  auto r2 = r.allocate();
  CHECK(p1 == r1);
  CHECK(p2 == r2);
}

/**
 * Test getting a block from one allocator, deallocating, and getting the block
 * from a different allocator.
 */
template <class T>
void test_both_allocators() {
  /* Instantiate two allocators, one from PoolAllocator class and one from
   * SingletonPoolAllocator object */
  auto p = PoolAllocator<sizeof(T)>{};
  auto& r = SingletonPoolAllocator<sizeof(T)>::get_instance();

  auto p1 = p.allocate();
  auto p2 = p.allocate();
  CHECK(p1 - p2 == sizeof(T));

  p.deallocate(p2);
  p.deallocate(p1);
  auto q1 = p.allocate();
  auto q2 = p.allocate();
  CHECK(p1 == q1);
  CHECK(p2 == q2);

  p.deallocate(q2);
  p.deallocate(q1);

  /* Get an element from second allocator and compare to first */
  auto r1 = r.allocate();
  auto r2 = r.allocate();
  CHECK(p1 == r1);
  CHECK(p2 == r2);
}

/**
 * Test allocation and deallocation with singleton
 */
TEST_CASE(
    "Pool Allocator: Test use with SingletonPoolAllocator", "[PoolAllocator]") {
  SECTION("big class") {
    test_singleton<big_class>();
  }
  SECTION("small class") {
    test_singleton<small_class>();
  }
}

/**
 * Test allocation and deallocation with PoolAllocator (also singleton)
 */
TEST_CASE("Pool Allocator: Test use with PoolAllocator", "[PoolAllocator]") {
  SECTION("big class") {
    test_pool_allocator<big_class>();
  }
  SECTION("small class") {
    test_pool_allocator<small_class>();
  }
}

/**
 * Test allocation and deallocation with PoolAllocator (also singleton)
 */
TEST_CASE(
    "Pool Allocator: Test use with PoolAllocator and SingletonPoolAllocator",
    "[PoolAllocator]") {
  SECTION("big class") {
    test_both_allocators<big_class>();
  }
  SECTION("small class") {
    test_both_allocators<small_class>();
  }
}

TEST_CASE(
    "Pool Allocator: Allocate vector with PoolAllocator - compile only",
    "[PoolAllocator]") {
  std::vector<std::byte, PoolAllocator<1024 * 1024>> v(10);
  CHECK(v.size() == 10);
}

template <class T>
void test_big_allocate() {
  auto p = PoolAllocator<sizeof(T)>{};

  size_t N = 64'000'000 / sizeof(T);

  std::vector<std::byte*> v(N);

  for (size_t i = 0; i < N; ++i) {
    v[i] = p.allocate();
  }

  for (size_t i = 0; i < N; ++i) {
    p.deallocate(v[i]);
  }

  for (size_t i = 0; i < N; ++i) {
    CHECK(p.allocate() == v[N - 1 - i]);
  }
}

TEST_CASE(
    "Pool Allocator: Allocate more DataBlocks than initial array"
    "[PoolAllocator]") {
  SECTION("big class") {
    test_big_allocate<big_class>();
  }
  SECTION("small class") {
    test_big_allocate<small_class>();
  }
}
#endif
