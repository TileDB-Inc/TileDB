/**
 * @file unit-resource-pool.cc
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
 * Tests the `ResourcePool` class.
 */

#include <mutex>
#include <thread>
#include <vector>

#include "tiledb/sm/misc/resource_pool.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace tiledb::sm;
using Catch::Matchers::StartsWith;

TEST_CASE("Buffer: Test resource pool", "[resource-pool]") {
  ResourcePool<int> pool(3);

  {
    // Get the maximum number of resources and set them.
    ResourceGuard r1(pool);
    ResourceGuard r2(pool);
    ResourceGuard r3(pool);

    r1.get() = 7;
    r2.get() = 8;
    r3.get() = 9;

    // Try to get one more resource should throw an exception.
    REQUIRE_THROWS_WITH(
        ResourceGuard(pool),
        StartsWith("Ran out of resources in resource pool"));
  }

  {
    // Validate we can get access to the same resources again.
    std::vector<bool> found(3, false);
    ResourceGuard r1(pool);
    ResourceGuard r2(pool);
    ResourceGuard r3(pool);

    found[r1.get() - 7] = true;
    found[r2.get() - 7] = true;
    found[r3.get() - 7] = true;

    REQUIRE(found[0]);
    REQUIRE(found[1]);
    REQUIRE(found[2]);
  }
}

TEST_CASE("Buffer: Test blocking resource pool", "[resource-pool]") {
  std::thread t1;

  BlockingResourcePool<int> pool(3);

  ResourceGuard r1(pool);
  ResourceGuard r2(pool);
  r1.get() = 7;
  r2.get() = 8;

  {
    // Get another resouce to reach maximum pool capacity
    ResourceGuard r3(pool);
    r3.get() = 9;

    // Request a resource when pool is full, the thread should block.
    t1 = std::thread([&] {
      ResourceGuard r4(pool);
      r4.get() = 10;
    });

    // Validate we can get access to the first 3 resources.
    REQUIRE(r1.get() == 7);
    REQUIRE(r2.get() == 8);
    REQUIRE(r3.get() == 9);

    // take r3 resource out of scope to release it
  }

  t1.join();

  ResourceGuard r4(pool);

  REQUIRE(r1.get() == 7);
  REQUIRE(r2.get() == 8);
  // check the old resource is gone, and one of the new ones is there
  REQUIRE(r4.get() == 10);
}

/*
 * Test disabled to avoid non-deterministic behaviour on CI, as flow-control is
 * done unreliably using sleep_for. When `barrier` (C++20) will be available, it
 * could be adapted and enabled.
 */
TEST_CASE("Buffer: Test blocking resource pool possible deadlock", "[.]") {
  std::thread t1, t2;

  BlockingResourcePool<int> pool(3);

  ResourceGuard r1(pool);
  r1.get() = 7;

  {
    ResourceGuard r2(pool);
    r2.get() = 8;

    {
      // Get another resouce to reach maximum pool capacity
      ResourceGuard r3(pool);
      r3.get() = 9;

      // Request a resource when pool is full, the thread should block.
      t1 = std::thread([&] {
        ResourceGuard r4(pool);
        r4.get() = 10;
        // wait so that resource is not immediately released
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      });

      t2 = std::thread([&] {
        ResourceGuard r5(pool);
        r5.get() = 11;
        // wait so that resource is not immediately released
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      });

      // Validate we can get access to the first 3 resources.
      REQUIRE(r1.get() == 7);
      REQUIRE(r2.get() == 8);
      REQUIRE(r3.get() == 9);

      // wait before releasing so that threads request for a resource and block
      std::this_thread::sleep_for(std::chrono::milliseconds(1));

      // take r3 resource out of scope to release it
    }
    // take r2 resource out of scope to release it
  }

  t1.join();
  t2.join();

  ResourceGuard r4(pool);
  ResourceGuard r5(pool);

  REQUIRE(r1.get() == 7);
  REQUIRE((r4.get() == 11 || r4.get() == 10));
  auto other_thread_value = (r4.get() == 11) ? 10 : 11;
  REQUIRE(r5.get() == other_thread_value);
}
