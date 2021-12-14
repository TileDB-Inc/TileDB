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

#include <catch.hpp>

using namespace tiledb::sm;

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
        ResourceGuard(pool), "Ran out of resources in resource pool");
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
  auto blocked = true;

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
      blocked = false;
      r4.get() = 10;
    });

    // Validate we can get access to the first 3 resources.
    REQUIRE(r1.get() == 7);
    REQUIRE(r2.get() == 8);
    REQUIRE(r3.get() == 9);

    // take r3 resource out of scope to release it
  }

  // wait for thread to get unblocked
  for (int i = 0; (i < 10) && blocked; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  if (blocked) {
    FAIL("Failing test, thread took too long to get unblocked");
  }

  t1.join();

  ResourceGuard r4(pool);

  REQUIRE(r1.get() == 7);
  REQUIRE(r2.get() == 8);
  // check the old resource is gone, and one of the new ones is there
  REQUIRE(r4.get() == 10);
}