/**
 * @file   tiledb/common/util/test/unit_sort_zip.cc
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
 * This file implements unit tests for the sort_zip class.
 */

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <vector>
#include "tiledb/common/util/alt_var_length_view.h"
#include "tiledb/stdx/__ranges/zip_view.h"

TEST_CASE("sort_zip: Null test", "[zip_view][null_test]") {
  REQUIRE(true);
}

using avl_test_type =
    alt_var_length_view<std::vector<double>, std::vector<size_t>>;

TEST_CASE("sort_zip: swap alt_var_length_view", "[zip_view]") {
  std::vector<double> q = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<double> r = {
      21.0, 20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0, 13.0, 12.0};

  std::vector<size_t> o = {0, 3, 6, 10};
  std::vector<size_t> p = {0, 2, 7, 10};

  auto a = avl_test_type{q, o};
  std::swap(a[0], a[1]);
  CHECK(std::ranges::equal(a[0], std::vector<double>{4.0, 5.0, 6.0}));
  CHECK(std::ranges::equal(a[1], std::vector<double>{1.0, 2.0, 3.0}));

  std::swap(*(a.begin()), *((a.begin()) + 1));
  CHECK(std::ranges::equal(a[0], std::vector<double>{1.0, 2.0, 3.0}));
  CHECK(std::ranges::equal(a[1], std::vector<double>{4.0, 5.0, 6.0}));

  auto x = a.begin();
  auto y = x + 1;
  std::swap(*x, *y);
  CHECK(std::ranges::equal(a[0], std::vector<double>{4.0, 5.0, 6.0}));
  CHECK(std::ranges::equal(a[1], std::vector<double>{1.0, 2.0, 3.0}));

  auto b = avl_test_type{r, p};
  std::swap(b[0], b[1]);
  CHECK(std::ranges::equal(
      b[0], std::vector<double>{19.0, 18.0, 17.0, 16.0, 15.0}));
  CHECK(std::ranges::equal(b[1], std::vector<double>{21.0, 20.0}));
}

TEST_CASE("sort_zip: iter_swap alt_var_length_view", "[zip_view]") {
  std::vector<double> q = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<double> r = {
      21.0, 20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0, 13.0, 12.0};

  std::vector<size_t> o = {0, 3, 6, 10};
  std::vector<size_t> p = {0, 2, 7, 10};

  auto a = avl_test_type{q, o};
  auto a0 = a.begin();
  auto a1 = a0 + 1;
  std::iter_swap(a0, a1);
  CHECK(std::ranges::equal(a0[0], std::vector<double>{4.0, 5.0, 6.0}));
  CHECK(std::ranges::equal(a0[1], std::vector<double>{1.0, 2.0, 3.0}));

  std::iter_swap(a0, a0 + 1);
  CHECK(std::ranges::equal(a0[0], std::vector<double>{1.0, 2.0, 3.0}));
  CHECK(std::ranges::equal(a0[1], std::vector<double>{4.0, 5.0, 6.0}));

  std::swap(*a0, *a1);
  CHECK(std::ranges::equal(a0[0], std::vector<double>{4.0, 5.0, 6.0}));
  CHECK(std::ranges::equal(a0[1], std::vector<double>{1.0, 2.0, 3.0}));

  std::swap(*a0, *(a0 + 1));
  CHECK(std::ranges::equal(a0[0], std::vector<double>{1.0, 2.0, 3.0}));
  CHECK(std::ranges::equal(a0[1], std::vector<double>{4.0, 5.0, 6.0}));

  auto b = avl_test_type{r, p};
  auto b0 = b.begin();
  auto b1 = b0 + 1;
  std::iter_swap(b0, b1);
  CHECK(std::ranges::equal(
      b[0], std::vector<double>{19.0, 18.0, 17.0, 16.0, 15.0}));
  CHECK(std::ranges::equal(b[1], std::vector<double>{21.0, 20.0}));

  std::iter_swap(b1, b0);
  CHECK(std::ranges::equal(b[0], std::vector<double>{21.0, 20.0}));
  CHECK(std::ranges::equal(
      b[1], std::vector<double>{19.0, 18.0, 17.0, 16.0, 15.0}));

  std::iter_swap(b0, b0);
  CHECK(std::ranges::equal(b[0], std::vector<double>{21.0, 20.0}));
  CHECK(std::ranges::equal(
      b[1], std::vector<double>{19.0, 18.0, 17.0, 16.0, 15.0}));

  std::iter_swap(b1, b1);
  CHECK(std::ranges::equal(b[0], std::vector<double>{21.0, 20.0}));
  CHECK(std::ranges::equal(
      b[1], std::vector<double>{19.0, 18.0, 17.0, 16.0, 15.0}));
}

TEST_CASE("sort_zip: iter_swap zip view", "[zip_view]") {
  std::vector<int> a = {1, 2, 3, 4, 5};
  std::vector<int> b = {5, 4, 3, 2, 1};

  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};
  auto v = alt_var_length_view{r, o};

  SECTION("a") {
    auto z = zip(a);
    iter_swap(z.begin(), z.begin());
    CHECK(a == std::vector<int>{1, 2, 3, 4, 5});
    auto it = z.begin();
    iter_swap(it, it + 1);
    CHECK(a == std::vector<int>{2, 1, 3, 4, 5});
  }
  SECTION("a, b") {
    auto z = zip(a, b);
    iter_swap(z.begin(), z.begin());
    CHECK(a == std::vector<int>{1, 2, 3, 4, 5});
    CHECK(b == std::vector<int>{5, 4, 3, 2, 1});
    auto it = z.begin();
    iter_swap(it, it + 1);
    CHECK(a == std::vector<int>{2, 1, 3, 4, 5});
    CHECK(b == std::vector<int>{4, 5, 3, 2, 1});
    it++;
    iter_swap(it, it + 1);
    CHECK(a == std::vector<int>{2, 3, 1, 4, 5});
    CHECK(b == std::vector<int>{4, 3, 5, 2, 1});
  }
  SECTION("a, b, v") {
    auto z = zip(a, b, v);
    iter_swap(z.begin(), z.begin());
    CHECK(a == std::vector<int>{1, 2, 3, 4, 5});
    CHECK(b == std::vector<int>{5, 4, 3, 2, 1});
    CHECK(std::ranges::equal(v[0], std::vector<double>{1.0, 2.0, 3.0}));
    CHECK(std::ranges::equal(v[1], std::vector<double>{4.0, 5.0, 6.0}));
    CHECK(std::ranges::equal(v[2], std::vector<double>{7.0, 8.0, 9.0, 10.0}));
    auto it = z.begin();
    iter_swap(it, it + 1);
    CHECK(a == std::vector<int>{2, 1, 3, 4, 5});
    CHECK(b == std::vector<int>{4, 5, 3, 2, 1});
    CHECK(std::ranges::equal(v[0], std::vector<double>{4.0, 5.0, 6.0}));
    CHECK(std::ranges::equal(v[1], std::vector<double>{1.0, 2.0, 3.0}));
    CHECK(std::ranges::equal(v[2], std::vector<double>{7.0, 8.0, 9.0, 10.0}));
    it++;
    iter_swap(it, it + 1);
    CHECK(a == std::vector<int>{2, 3, 1, 4, 5});
    CHECK(b == std::vector<int>{4, 3, 5, 2, 1});
    CHECK(std::ranges::equal(v[0], std::vector<double>{4.0, 5.0, 6.0}));
    CHECK(std::ranges::equal(v[1], std::vector<double>{7.0, 8.0, 9.0, 10.0}));
    CHECK(std::ranges::equal(v[2], std::vector<double>{1.0, 2.0, 3.0}));
  }
}

template <class Out, class T>
concept one = requires(Out&& o, T&& t) { *o = std::forward<T>(t); };
template <class Out, class T>
concept two =
    requires(Out&& o, T&& t) { *std::forward<Out>(o) = std::forward<T>(t); };
template <class Out, class T>
concept three = requires(Out&& o, T&& t) {
  const_cast<const std::iter_reference_t<Out>&&>(*o) = std::forward<T>(t);
};
template <class Out, class T>
concept four = requires(Out&& o, T&& t) {
  const_cast<const std::iter_reference_t<Out>&&>(*std::forward<Out>(o)) =
      std::forward<T>(t);
};

TEST_CASE("sort_zip: mini sort zip view", "[zip_view]") {
  std::vector<int> a = {1, 2, 3, 4, 5};
  std::vector<int> b = {5, 4, 3, 2, 1};

  SECTION("a") {
    auto z = zip(a);
    std::sort(z.begin(), z.end());
    CHECK(a == std::vector<int>{1, 2, 3, 4, 5});
  }
}

TEST_CASE("sort_zip: range sort zip view concepts", "[zip_view]") {
  using VI = std::ranges::iterator_t<std::vector<int>>;
  using ZI = std::ranges::iterator_t<stdx::ranges::zip_view<std::vector<int>>>;

  CHECK(std::forward_iterator<VI>);
  CHECK(std::indirectly_movable_storable<VI, VI>);
  CHECK(std::indirectly_swappable<VI, VI>);
  CHECK(std::permutable<VI>);
  CHECK(std::sortable<VI>);

  CHECK(std::indirectly_readable<ZI>);

  // This will work in C++23 but not C++20
  // Hence, we cannot use std::ranges::sort with zip_view in C++20
#if 0
  {
    int i = 0;
    using T = std::tuple<int&>;

    auto xx = T{i};
    const_cast<const std::tuple<int&>&&>(xx) = xx;
  }
#endif

  CHECK(one<ZI, std::iter_rvalue_reference_t<ZI>>);
  CHECK(one<ZI, std::iter_value_t<ZI>>);
  CHECK(two<ZI, std::iter_rvalue_reference_t<ZI>>);
  CHECK(two<ZI, std::iter_value_t<ZI>>);

  // These need to pass in order for zip_view to work with std::ranges::sort
  // (all due to std::tuple issues -- see above)
  CHECK(!three<ZI, std::iter_rvalue_reference_t<ZI>>);
  CHECK(!three<ZI, std::iter_value_t<ZI>>);
  CHECK(!four<ZI, std::iter_rvalue_reference_t<ZI>>);
  CHECK(!four<ZI, std::iter_value_t<ZI>>);

  // Once three and four don't pass, the following chain will not pass
  CHECK(!std::indirectly_writable<ZI, std::iter_rvalue_reference_t<ZI>>);
  CHECK(!std::indirectly_writable<ZI, std::iter_value_t<ZI>>);
  CHECK(!std::indirectly_movable<ZI, ZI>);
  CHECK(!std::indirectly_movable_storable<ZI, ZI>);
  CHECK(!std::permutable<ZI>);
  CHECK(!std::sortable<ZI>);

  CHECK(std::movable<std::iter_value_t<ZI>>);
  CHECK(std::constructible_from<
        std::iter_value_t<ZI>,
        std::iter_rvalue_reference_t<ZI>>);
  CHECK(std::assignable_from<
        std::iter_value_t<ZI>&,
        std::iter_rvalue_reference_t<ZI>>);
  CHECK(std::forward_iterator<ZI>);
  CHECK(std::indirectly_swappable<ZI, ZI>);
  CHECK(std::is_swappable_v<ZI>);
}

#if 0
TEST_CASE("sort_zip: range sort zip view", "[zip_view]") {
  std::vector<int> a = {1, 2, 3, 4, 5};
  std::vector<int> b = {5, 4, 3, 2, 1};

  SECTION("a") {
      auto z = zip(a);
      std::ranges::sort(z);
      CHECK(a == std::vector<int>{1, 2, 3, 4, 5});
  }
}
#endif

TEST_CASE("sort_zip: swap zip view", "[zip_view]") {
  std::vector<int> a = {1, 2, 3, 4, 5};
  std::vector<int> b = {5, 4, 3, 2, 1};
  auto z0 = zip(a);

  swap(z0[0], z0[1]);
  CHECK(a == std::vector<int>{2, 1, 3, 4, 5});
  CHECK(b == std::vector<int>{5, 4, 3, 2, 1});

  auto z1 = zip(a, b);
  swap(z1[2], z1[3]);
  CHECK(a == std::vector<int>{2, 1, 4, 3, 5});
  CHECK(b == std::vector<int>{5, 4, 2, 3, 1});
}

TEST_CASE("sort_zip: mini iter_swap zip view", "[zip_view]") {
  std::vector<int> a = {1, 2, 3, 4, 5};
  std::vector<int> b = {5, 4, 3, 2, 1};
  auto z0 = zip(a);
  auto z00 = z0.begin();
  auto z01 = z00 + 1;

  iter_swap(z00, z01);
  CHECK(a == std::vector<int>{2, 1, 3, 4, 5});
  CHECK(b == std::vector<int>{5, 4, 3, 2, 1});

  auto z1 = zip(a, b);
  swap(z1[2], z1[3]);
  CHECK(a == std::vector<int>{2, 1, 4, 3, 5});
  CHECK(b == std::vector<int>{5, 4, 2, 3, 1});
}

TEST_CASE("sort_zip: sort zip view", "[zip_view]") {
  std::vector<int> a = {1, 2, 3, 4, 5};
  std::vector<int> b = {5, 4, 3, 2, 1};

  SECTION("a") {
    auto z = zip(a);
    std::sort(z.begin(), z.end());
    CHECK(a == std::vector<int>{1, 2, 3, 4, 5});
  }
  SECTION("b") {
    auto z = zip(b);
    std::sort(z.begin(), z.end());
    CHECK(a == std::vector<int>{1, 2, 3, 4, 5});
  }
  SECTION("a, b") {
    auto z = zip(a, b);
    std::sort(z.begin(), z.end());
    CHECK(a == std::vector<int>{1, 2, 3, 4, 5});
    CHECK(b == std::vector<int>{5, 4, 3, 2, 1});
  }
  SECTION("b, a") {
    auto z = zip(b, a);
    std::sort(z.begin(), z.end());
    CHECK(a == std::vector<int>{5, 4, 3, 2, 1});
    CHECK(b == std::vector<int>{1, 2, 3, 4, 5});
  }
}

TEST_CASE(
    "sort_zip: sort zip view containing alt_var_length_view", "[zip_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};
  auto v = alt_var_length_view{r, o};
  std::vector<int> a{8, 6, 7};

  auto z = zip(a, v);
  std::sort(z.begin(), z.end(), [](const auto& x, const auto& y) {
    return std::get<0>(x) < std::get<0>(y);
  });
  CHECK(a == std::vector<int>{6, 7, 8});
  CHECK(std::ranges::equal(v[0], std::vector<double>{4.0, 5.0, 6.0}));
  CHECK(std::ranges::equal(v[1], std::vector<double>{7.0, 8.0, 9.0, 10.0}));
  CHECK(std::ranges::equal(v[2], std::vector<double>{1.0, 2.0, 3.0}));
}

TEST_CASE("sort_zip: sort zip view using alt_var_length_view", "[zip_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};
  auto v = alt_var_length_view{r, o};
  std::vector<int> a{8, 6, 7};

  auto z = zip(a, v);
  std::sort(z.begin(), z.end(), [](const auto& x, const auto& y) {
    return std::get<1>(x)[0] > std::get<1>(y)[0];
  });
  CHECK(a == std::vector<int>{7, 6, 8});
  CHECK(std::ranges::equal(v[0], std::vector<double>{7.0, 8.0, 9.0, 10.0}));
  CHECK(std::ranges::equal(v[1], std::vector<double>{4.0, 5.0, 6.0}));
  CHECK(std::ranges::equal(v[2], std::vector<double>{1.0, 2.0, 3.0}));
}
