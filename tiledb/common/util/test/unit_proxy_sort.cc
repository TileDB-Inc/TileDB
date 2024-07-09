/**
 * @file   unit_proxy_sort.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file implements unit tests for `proxy_sort` that will be used as
 * part of TileDB external sort.
 */

#include <catch2/catch_all.hpp>
#include <ranges>
#include <string>
#include <vector>
#include "tiledb/common/util/proxy_sort.h"

TEST_CASE("proxy_sort: Null test", "[proxy_sort][null_test]") {
  REQUIRE(true);
}

TEST_CASE("proxy_sort: four ways, less than", "[proxy_sort]") {
  std::vector<int> x{3, 1, 4, 1, 5, 9, 2, 6, 5, 3};
  std::vector<int> y(x);
  std::vector<int> expected{1, 1, 2, 3, 3, 4, 5, 5, 6, 9};
  std::vector<int> perm(x.size());

  SECTION("proxy_sort_no_init") {
    std::iota(perm.begin(), perm.end(), 0);
    proxy_sort_no_init(x, perm);
  }
  SECTION("proxy_sort") {
    std::fill(perm.begin(), perm.end(), 0);
    proxy_sort(x, perm);
  }
  SECTION("proxy_sort_no_init") {
    std::iota(perm.begin(), perm.end(), 0);
    proxy_sort_no_init(x, perm, std::less<>());
  }
  SECTION("proxy_sort") {
    std::fill(perm.begin(), perm.end(), 0);
    proxy_sort(x, perm, std::less<>());
  }
  SECTION("stable_proxy_sort_no_init") {
    std::iota(perm.begin(), perm.end(), 0);
    stable_proxy_sort_no_init(x, perm);
  }
  SECTION("stable_proxy_sort") {
    std::fill(perm.begin(), perm.end(), 0);
    stable_proxy_sort(x, perm);
  }
  SECTION("stable_proxy_sort_no_init") {
    std::iota(perm.begin(), perm.end(), 0);
    stable_proxy_sort_no_init(x, perm, std::less<>());
  }
  SECTION("stable_proxy_sort") {
    std::fill(perm.begin(), perm.end(), 0);
    stable_proxy_sort(x, perm, std::less<>());
  }

  CHECK(std::equal(x.begin(), x.end(), y.begin()));

  for (size_t i = 1; i < x.size(); ++i) {
    CHECK(x[perm[i - 1]] <= x[perm[i]]);
  }
  for (size_t i = 0; i < x.size(); ++i) {
    CHECK(x[perm[i]] == expected[i]);
  }
}

TEST_CASE("proxy_sort: strings two ways, greater than", "[proxy_sort]") {
  std::vector<std::string> x{
      "three",
      "point",
      "one",
      "four",
      "one",
      "five",
      "nine",
      "two",
      "six",
      "five",
      "three"};
  std::vector<std::string> y(x);
  std::vector<std::string> expected{
      "five",
      "five",
      "four",
      "nine",
      "one",
      "one",
      "point",
      "six",
      "three",
      "three",
      "two"};
  std::vector<std::string> reverse_expected(expected.rbegin(), expected.rend());
  std::vector<int> perm(x.size());

  SECTION("less than") {
    SECTION("proxy_sort_no_init") {
      std::iota(perm.begin(), perm.end(), 0);
      proxy_sort_no_init(x, perm);
    }
    SECTION("proxy_sort") {
      std::fill(perm.begin(), perm.end(), 0);
      proxy_sort(x, perm);
    }
    SECTION("proxy_sort_no_init") {
      std::iota(perm.begin(), perm.end(), 0);
      proxy_sort_no_init(x, perm, std::less<>());
    }
    SECTION("proxy_sort") {
      std::fill(perm.begin(), perm.end(), 0);
      proxy_sort(x, perm, std::less<>());
    }
    SECTION("stable_proxy_sort_no_init") {
      std::iota(perm.begin(), perm.end(), 0);
      proxy_sort_no_init(x, perm);
    }
    SECTION("stable_proxy_sort") {
      std::fill(perm.begin(), perm.end(), 0);
      proxy_sort(x, perm);
    }
    SECTION("stable_proxy_sort_no_init") {
      std::iota(perm.begin(), perm.end(), 0);
      proxy_sort_no_init(x, perm, std::less<>());
    }
    SECTION("stable_proxy_sort") {
      std::fill(perm.begin(), perm.end(), 0);
      proxy_sort(x, perm, std::less<>());
    }

    CHECK(std::equal(x.begin(), x.end(), y.begin()));

    for (size_t i = 1; i < x.size(); ++i) {
      CHECK(x[perm[i - 1]] <= x[perm[i]]);
    }
    for (size_t i = 0; i < x.size(); ++i) {
      CHECK(x[perm[i]] == expected[i]);
    }
  }
  SECTION("greater than") {
    SECTION("proxy_sort_no_init") {
      std::iota(perm.begin(), perm.end(), 0);
      proxy_sort_no_init(x, perm, std::greater<>());
    }
    SECTION("proxy_sort") {
      std::fill(perm.begin(), perm.end(), 0);
      proxy_sort(x, perm, std::greater<>());
    }
    SECTION("stable_proxy_sort_no_init") {
      std::iota(perm.begin(), perm.end(), 0);
      stable_proxy_sort_no_init(x, perm, std::greater<>());
    }
    SECTION("stable_proxy_sort") {
      std::fill(perm.begin(), perm.end(), 0);
      stable_proxy_sort(x, perm, std::greater<>());
    }
    CHECK(std::equal(x.begin(), x.end(), y.begin()));

    for (size_t i = 1; i < x.size(); ++i) {
      CHECK(x[perm[i - 1]] >= x[perm[i]]);
    }
    for (size_t i = 0; i < x.size(); ++i) {
      CHECK(x[perm[i]] == reverse_expected[i]);
    }
  }
}
