/**
 * @file   unit_permutation_sort.cc
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
 * This file implements unit tests for sorting for/with a permutation view.
 */

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <iostream>
#include <random>
#include <ranges>
#include <vector>
#include "tiledb/common/util/alt_var_length_view.h"
#include "tiledb/common/util/permutation_view.h"

#include "tiledb/common/util/proxy_sort.h"
#include "tiledb/common/util/var_length_view.h"
#include "tiledb/stdx/__ranges/zip_view.h"

#include "tiledb/common/util/permute.h"

TEST_CASE("permutation_sort: Null test", "[permutation_sort][null_test]") {
  REQUIRE(true);
}

TEST_CASE("permutation_sort: integers", "[permutation_sort]") {
  unsigned int seed = Catch::rngSeed();
  size_t N = 100'000;
  std::mt19937 g(seed);

  std::vector<int> init(N);
  std::iota(begin(init), end(init), 0);

  std::vector<int> perm(N);
  std::iota(begin(perm), end(perm), 0);

  auto init_19 = std::vector<int>(N);
  std::iota(begin(init_19), end(init_19), 19);

  std::vector<int> unshuffled(init);
  std::vector<int> shuffled(unshuffled);

  std::shuffle(shuffled.begin(), shuffled.end(), g);
  std::vector<int> shuffled_copy(shuffled);

  /* Sort the shuffled vector */
  proxy_sort_no_init(shuffled, perm);

  /* Generate the inverse permutation */
  std::vector<int> i_perm(init);
  proxy_sort_no_init(perm, i_perm);

  SECTION("check setup") {
    CHECK(!std::equal(shuffled.begin(), shuffled.end(), unshuffled.begin()));
    CHECK(std::equal(shuffled.begin(), shuffled.end(), shuffled_copy.begin()));

    auto b = permutation_view(init_19, perm);
    CHECK(std::equal(b.begin(), b.end(), perm.begin(), [](auto a, auto b) {
      return a == b + 19;
    }));

    auto yy = permutation_view(shuffled, perm);  // -> unshuffled
    auto zz = permutation_view(yy, i_perm);      // -> shuffled
    CHECK(std::equal(begin(zz), end(zz), begin(shuffled)));
  }

  SECTION("sort, permute") {
    auto x = permutation_view(shuffled, perm);
    CHECK(std::equal(x.begin(), x.end(), unshuffled.begin()));
    CHECK(std::equal(shuffled.begin(), shuffled.end(), shuffled_copy.begin()));
  }

  SECTION("sort, permute, zip") {
    /* Create permutation view */
    auto x = permutation_view(shuffled, perm);

    /* Zip permutation view with init_19 */
    auto z = zip(x, init_19);

    /* First component of z should appear to be unshuffled */
    CHECK(std::equal(begin(z), z.end(), begin(unshuffled), [](auto a, auto b) {
      return std::get<0>(a) == b;
    }));

    CHECK(std::equal(begin(z), end(z), begin(init), [](auto a, auto b) {
      return std::get<1>(a) == b + 19;
    }));
  }

  SECTION("sort, zip, permute") {
    /* Zip shuffled with init_19 */
    auto z = zip(shuffled, init_19);

    /* Permute the zipped view */
    auto x = permutation_view(z, perm);

    /* First component of x should appear to be unshuffled */
    CHECK(std::equal(begin(x), end(x), begin(unshuffled), [](auto a, auto b) {
      return std::get<0>(a) == b;
    }));

    CHECK(std::equal(begin(x), end(x), begin(perm), [](auto a, auto b) {
      return std::get<1>(a) == b + 19;
    }));
  }

  SECTION("sort, permute, inv_permute") {
    /* Create permutation view */
    auto x = permutation_view(shuffled, perm);

    /* Inverse permute the permutation view */
    auto y = permutation_view(x, i_perm);

    /* y should appear to be shuffled */
    CHECK(std::equal(begin(y), end(y), begin(shuffled)));
  }
}

TEST_CASE("permutation_sort: multiple integers", "[permutation_sort]") {
  unsigned int seed = Catch::rngSeed();
  size_t N = 100'000;
  std::mt19937 g(seed);

  std::vector<int> init(N);
  std::iota(begin(init), end(init), 0);

  auto x = std::vector<std::vector<int>>(7, std::vector<int>(init));
  for (auto& v : x) {
    std::shuffle(begin(v), end(v), g);
  }

  auto perm = std::vector<std::vector<int>>(7, std::vector<int>(init));
  for (size_t i = 0; i < size(x); ++i) {
    proxy_sort(x[i], perm[i]);
  }

  auto i_perm = std::vector<std::vector<int>>(7, std::vector<int>(init));
  for (size_t i = 0; i < size(perm); ++i) {
    proxy_sort(perm[i], i_perm[i]);
  }

  SECTION("check setup") {
    for (size_t i = 0; i < size(x); ++i) {
      auto a = permutation_view(x[i], perm[i]);
      CHECK(std::equal(begin(a), end(a), begin(init)));

      auto b = permutation_view(a, i_perm[i]);
      CHECK(std::equal(begin(b), end(b), begin(x[i])));
    }
  }

  SECTION("sort, zip, permute some") {
    /* Zip shuffled with init_19 */
    auto z = zip(x[0], x[1], x[2], x[3], x[4], x[5], x[6]);

    /* Permute the zipped view */
    auto x = permutation_view(z, perm[0]);

    /* First component of x should appear to be unshuffled */
    CHECK(std::equal(begin(x), end(x), begin(init), [](auto a, auto b) {
      return std::get<0>(a) == b;
    }));
  }

  SECTION("sort, zip, permute all") {
    for (size_t j = 0; j < size(x); ++j) {
      auto z =
          zip(x[j % 7],
              x[(j + 1) % 7],
              x[(j + 2) % 7],
              x[(j + 3) % 7],
              x[(j + 4) % 7],
              x[(j + 5) % 7],
              x[(j + 6) % 7]);

      auto p0 = permutation_view(z, perm[j % 7]);
      CHECK(std::equal(begin(p0), end(p0), begin(init), [](auto a, auto b) {
        return std::get<0>(a) == b;
      }));
      auto p1 = permutation_view(z, perm[(j + 1) % 7]);
      CHECK(std::equal(begin(p1), end(p1), begin(init), [](auto a, auto b) {
        return std::get<1>(a) == b;
      }));
      auto p2 = permutation_view(z, perm[(j + 2) % 7]);
      CHECK(std::equal(begin(p2), end(p2), begin(init), [](auto a, auto b) {
        return std::get<2>(a) == b;
      }));
      auto p3 = permutation_view(z, perm[(j + 3) % 7]);
      CHECK(std::equal(begin(p3), end(p3), begin(init), [](auto a, auto b) {
        return std::get<3>(a) == b;
      }));
      auto p4 = permutation_view(z, perm[(j + 4) % 7]);
      CHECK(std::equal(begin(p4), end(p4), begin(init), [](auto a, auto b) {
        return std::get<4>(a) == b;
      }));
      auto p5 = permutation_view(z, perm[(j + 5) % 7]);
      CHECK(std::equal(begin(p5), end(p5), begin(init), [](auto a, auto b) {
        return std::get<5>(a) == b;
      }));
      auto p6 = permutation_view(z, perm[(j + 6) % 7]);
      CHECK(std::equal(begin(p6), end(p6), begin(init), [](auto a, auto b) {
        return std::get<6>(a) == b;
      }));
    }
  }
}

TEST_CASE(
    "permutation_sort: direct proxy_sort multiple integers",
    "[permutation_sort]") {
  unsigned int seed = Catch::rngSeed();
  size_t N = 100'000;
  std::mt19937 g(seed);

  std::vector<int> init(N);
  std::iota(begin(init), end(init), 0);

  auto x = std::vector<std::vector<int>>(7, std::vector<int>(init));
  for (auto& v : x) {
    std::shuffle(begin(v), end(v), g);
  }

  CHECK(!std::equal(begin(x[0]), end(x[0]), begin(init)));

  auto perm = std::vector<std::vector<int>>(7, std::vector<int>(init));
  auto i_perm = std::vector<std::vector<int>>(7, std::vector<int>(init));

  SECTION("zip, sort some") {
    /* Zip shuffled with init_19 */
    auto z = zip(x[0], x[1], x[2], x[3], x[4], x[5], x[6]);

    /* Create permutation view with zipped view */
    auto x = permutation_view(z, perm[0]);

    SECTION("less") {
      SECTION("x.proxy_sort_no_init") {
        x.proxy_sort_no_init();
      }
      SECTION("x.proxy_sort_") {
        x.proxy_sort();
      }

      SECTION("x.proxy_sort_no_init") {
        x.proxy_sort_no_init(std::less<>());
      }
      SECTION("x.proxy_sort_") {
        x.proxy_sort(std::less<>());
      }
      SECTION("x.proxy_sort_no_init") {
        proxy_sort_no_init(x);
      }
      SECTION("x.proxy_sort_") {
        proxy_sort(x);
      }
      SECTION("x.proxy_sort_no_init") {
        proxy_sort_no_init(x, std::less<>());
      }
      SECTION("x.proxy_sort_") {
        proxy_sort(x, std::less<>());
      }
      SECTION("std::sort") {
        std::sort(x);
      }
      SECTION("std::sort") {
        std::sort(x, std::less<>());
      }

      /* First component of x should appear to be unshuffled */
      CHECK(std::equal(begin(x), end(x), begin(init), [](auto a, auto b) {
        return std::get<0>(a) == b;
      }));
    }
    SECTION("greater") {
      auto reverse_index = std::vector<int>(init);
      std::ranges::reverse(reverse_index);
      SECTION("x.proxy_sort_no_init") {
        x.proxy_sort_no_init(std::greater<>());
      }
      SECTION("x.proxy_sort_") {
        x.proxy_sort(std::greater<>());
      }
      SECTION("x.proxy_sort_no_init") {
        proxy_sort_no_init(x, std::greater<>());
      }
      SECTION("x.proxy_sort_") {
        proxy_sort(x, std::greater<>());
      }
      SECTION("std::sort") {
        std::sort(x, std::greater<>());
      }

      /* First component of x should appear to be unshuffled */
      CHECK(std::equal(
          begin(x), end(x), begin(reverse_index), [](auto a, auto b) {
            return std::get<0>(a) == b;
          }));
    }
  }

  SECTION("sort, zip, permute all") {
    for (size_t j = 0; j < size(x); ++j) {
      auto z =
          zip(x[j % 7],
              x[(j + 1) % 7],
              x[(j + 2) % 7],
              x[(j + 3) % 7],
              x[(j + 4) % 7],
              x[(j + 5) % 7],
              x[(j + 6) % 7]);

      auto v = permutation_view(z, perm[0]);
      proxy_sort(v);
      CHECK(std::equal(begin(v), end(v), begin(init), [](auto a, auto b) {
        return std::get<0>(a) == b;
      }));
    }
  }
}

/**
 * This function is set up to time various ways of sorting a set of arrays
 * according to how one of them is ordered.  It compares the time of sorting
 * with a proxy sort and in-place permutation vs in-place sort.
 *
 * Doing in-place sort with the zip view is the most efficient (especially when
 * parallelized.
 *
 * To do the timings, access a version of timer.h (e.g., from the experimental
 * dag hierarchy and uncomment the timer related lines.  To do the timing of
 * parallel zip sort you will also need to use a parallel sort algorithm.  You
 * may also want to increase the size of N to 20'000'000.
 */
TEST_CASE("permutation_sort: time", "[permutation_sort]") {
  uint32_t seed = Catch::rngSeed();
  size_t N = 2'000'000;
  std::mt19937 g(seed);

  std::vector<int> perm(N);
  std::iota(begin(perm), end(perm), 0);

  auto init_19 = std::vector<int>(N);
  std::iota(begin(init_19), end(init_19), 19);
  std::vector<int> shuffled(init_19);

  std::shuffle(shuffled.begin(), shuffled.end(), g);
  std::vector<int> sorted0(shuffled);
  std::vector<int> sorted1(shuffled);
  std::vector<int> sorted2(shuffled);
  std::vector<int> sorted3(shuffled);
  std::vector<int> sorted4(shuffled);
  std::vector<int> sorted5(shuffled);
  std::vector<int> sorted6(shuffled);
  std::vector<int> sorted7(shuffled);
  std::vector<int> sorted8(shuffled);
  std::vector<int> sorted9(shuffled);

  CHECK(!std::ranges::equal(shuffled, init_19));

  // Different numbers of vectors have different performance characteristics
  // auto z = zip(shuffled, sorted0, sorted1, sorted2);
  // auto z = zip(shuffled, sorted0, sorted1, sorted2, sorted3, sorted4);
  // auto z = zip(shuffled, sorted0, sorted1, sorted2, sorted3, sorted4,
  // sorted5, sorted6);
  auto z =
      zip(shuffled,
          sorted0,
          sorted1,
          sorted2,
          sorted3,
          sorted4,
          sorted5,
          sorted6,
          sorted7,
          sorted8,
          sorted9);

  std::vector<char> done(N, 0);

  SECTION("separate sort") {
    std::ranges::sort(shuffled);
    std::ranges::sort(sorted0);
    std::ranges::sort(sorted1);
    std::ranges::sort(sorted2);

    CHECK(std::ranges::equal(shuffled, init_19));
    CHECK(std::ranges::equal(sorted0, init_19));
    CHECK(std::ranges::equal(sorted1, init_19));
    CHECK(std::ranges::equal(sorted2, init_19));
  }
  SECTION("proxy sort permute") {
    proxy_sort(shuffled, perm);

    permute(shuffled, perm);
    permute(sorted0, perm);
    permute(sorted1, perm);
    permute(sorted2, perm);

    CHECK(std::ranges::equal(shuffled, init_19));
    CHECK(std::ranges::equal(sorted0, init_19));
    CHECK(std::ranges::equal(sorted1, init_19));
    CHECK(std::ranges::equal(sorted2, init_19));
  }
  SECTION("proxy sort permute zip") {
    proxy_sort(shuffled, perm);

    permute(z, perm, done);

    CHECK(std::ranges::equal(shuffled, init_19));
    CHECK(std::ranges::equal(sorted0, init_19));
    CHECK(std::ranges::equal(sorted1, init_19));
    CHECK(std::ranges::equal(sorted2, init_19));
  }

  SECTION("zip sort") {
    std::sort(z.begin(), z.end(), [](auto&& a, auto&& b) {
      return std::get<0>(a) < std::get<0>(b);
    });

    CHECK(std::ranges::equal(shuffled, init_19));
    CHECK(std::ranges::equal(sorted0, init_19));
    CHECK(std::ranges::equal(sorted1, init_19));
    CHECK(std::ranges::equal(sorted2, init_19));
  }

// Enable if pmergesort is available
#if 0
  SECTION("zip par sort") {

    pmergesort(z.begin(), z.end(), [](auto&& a, auto&& b) {
     return std::get<0>(a) < std::get<0>(b);
    });

    CHECK(std::ranges::equal(shuffled, init_19));
    CHECK(std::ranges::equal(sorted0, init_19));
    CHECK(std::ranges::equal(sorted1, init_19));
    CHECK(std::ranges::equal(sorted2, init_19));
  }
#endif
}
