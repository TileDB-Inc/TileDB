/**
 * @file unit_join.cc
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
 * Tests the join class.
 */

#include <iostream>
#include <list>
#include <vector>
#include "external/include/span/span.hpp"

#include <test/support/tdb_catch.h>
#include "experimental/tiledb/common/dag/utils/range_join.h"

using namespace tiledb::common;

TEST_CASE("Join: Test construct", "[join]") {
}

TEST_CASE("Join: Test list of list", "[join]") {
  std::list<int> a{1, 2, 3, 4};
  std::list<int> b{5, 6, 7, 8};
  std::list<int> c{1, 2, 3, 4, 5, 6, 7, 8};
  std::list<std::list<int>> d{a, b};
  auto e = join(d);

  SECTION("check c vs e") {
    CHECK(std::equal(c.begin(), c.end(), e.begin()));
  }
  SECTION("check e vs c") {
    CHECK(std::equal(e.begin(), e.end(), c.begin()));
  }
}

TEST_CASE("Join: Test list of vector", "[join]") {
  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};
  std::list<std::vector<int>> d{a, b};
  auto e = join(d);

  SECTION("check c vs e, list") {
    std::list<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(c.begin(), c.end(), e.begin()));
  }
  SECTION("check e vs c, list") {
    std::list<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(e.begin(), e.end(), c.begin()));
  }
  SECTION("check c vs e, vector") {
    std::vector<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(c.begin(), c.end(), e.begin()));
  }
  SECTION("check e vs c, vector") {
    std::vector<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(e.begin(), e.end(), c.begin()));
  }
}

TEST_CASE("Join: Test vector of lists", "[join]") {
  std::list<int> a{1, 2, 3, 4};
  std::list<int> b{5, 6, 7, 8};
  std::vector<std::list<int>> d{a, b};
  auto e = join(d);

  SECTION("check c vs e, list") {
    std::list<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(c.begin(), c.end(), e.begin()));
  }
  SECTION("check e vs c, list") {
    std::list<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(e.begin(), e.end(), c.begin()));
  }
  SECTION("check c vs e, vector") {
    std::vector<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(c.begin(), c.end(), e.begin()));
  }
  SECTION("check e vs c, vector") {
    std::vector<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(e.begin(), e.end(), c.begin()));
  }
}

TEST_CASE("Join: Test list of spans", "[join]") {
  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};
  std::list<tcb::span<int>> d{tcb::span<int>{a}, tcb::span<int>{b}};
  auto e = join(d);

  SECTION("check c vs e, list") {
    std::list<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(c.begin(), c.end(), e.begin()));
  }
  SECTION("check e vs c, list") {
    std::list<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(e.begin(), e.end(), c.begin()));
  }
  SECTION("check c vs e, vector") {
    std::vector<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(c.begin(), c.end(), e.begin()));
  }
  SECTION("check e vs c, vector") {
    std::vector<int> c{1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::equal(e.begin(), e.end(), c.begin()));
  }
}

TEST_CASE("Join: Truncated list of spans", "[join]") {
  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};
  std::list<tcb::span<int>> d{tcb::span<int>{a.data(), a.size() - 1},
                              tcb::span<int>{b.data(), b.size() - 2}};
  auto e = join(d);

  SECTION("check c vs e, list") {
    std::list<int> c{1, 2, 3, 5, 6};
    CHECK(std::equal(c.begin(), c.end(), e.begin()));
  }
  SECTION("check e vs c, list") {
    std::list<int> c{1, 2, 3, 5, 6};
    CHECK(std::equal(e.begin(), e.end(), c.begin()));
  }
  SECTION("check c vs e, vector") {
    std::vector<int> c{1, 2, 3, 5, 6};
    CHECK(std::equal(c.begin(), c.end(), e.begin()));
  }
  SECTION("check e vs c, vector") {
    std::vector<int> c{1, 2, 3, 5, 6};
    CHECK(std::equal(e.begin(), e.end(), c.begin()));
  }
}

TEST_CASE("Join: join of join", "[join]") {
  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};
  std::vector<int> c{9, 10, 11, 12};
  std::vector<int> d{13, 14, 15, 16};
  std::list<std::vector<int>> e{std::vector<int>{a}, std::vector<int>{b}};
  std::list<std::vector<int>> f{std::vector<int>{c}, std::vector<int>{d}};
  auto g = join(e);
  auto h = join(f);
  std::list<join<std::list<std::vector<int>>>> i{g, h};
  auto j = join{i};
  std::list<int> k{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

  SECTION("check j vs k") {
    CHECK(std::equal(j.begin(), j.end(), k.begin()));
  }
  SECTION("check k vs j") {
    CHECK(std::equal(k.begin(), k.end(), j.begin()));
  }
}
