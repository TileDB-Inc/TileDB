/**
 * @file   unit_alt_var_length_view.cc
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
 * This file implements unit tests for the alt_var_length_view class.
 */

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <vector>
#include "tiledb/common/util/alt_var_length_view.h"

TEST_CASE(
    "alt_var_length_view: Null test", "[alt_var_length_view][null_test]") {
  REQUIRE(true);
}

// Test that the alt_var_length_view satisfies the expected concepts
TEST_CASE(
    "alt_var_length_view: Range concepts", "[alt_var_length_view][concepts]") {
  using test_type = alt_var_length_view<std::vector<double>, std::vector<int>>;

  CHECK(std::ranges::range<test_type>);
  CHECK(!std::ranges::borrowed_range<test_type>);
  CHECK(std::ranges::sized_range<test_type>);
  CHECK(std::ranges::view<test_type>);
  CHECK(std::ranges::input_range<test_type>);

  // @todo: Do we actually want alt_var_length_view to be an output_range?
  CHECK(std::ranges::
            output_range<test_type, std::ranges::range_value_t<test_type>>);
  CHECK(std::ranges::forward_range<test_type>);
  CHECK(std::ranges::bidirectional_range<test_type>);
  CHECK(std::ranges::random_access_range<test_type>);

  // @todo: Do we actually want alt_var_length_view to be a contiguous_range?
  CHECK(std::ranges::contiguous_range<test_type>);
  CHECK(std::ranges::common_range<test_type>);
  CHECK(std::ranges::viewable_range<test_type>);

  CHECK(std::ranges::view<test_type>);
}

struct dummy_compare {
  bool operator()(auto&&, auto&&) const {
    return true;
  }
};

// Test that the alt_var_length_view iterators satisfy the expected concepts
TEST_CASE(
    "alt_var_length_view: Iterator concepts",
    "[alt_var_length_view][concepts]") {
  using test_type = alt_var_length_view<std::vector<double>, std::vector<int>>;
  using test_type_iterator = std::ranges::iterator_t<test_type>;
  using test_type_const_iterator = std::ranges::iterator_t<const test_type>;

  CHECK(std::input_or_output_iterator<test_type_iterator>);
  CHECK(std::input_or_output_iterator<test_type_const_iterator>);
  CHECK(std::input_iterator<test_type_iterator>);
  CHECK(std::input_iterator<test_type_const_iterator>);

  // @todo: Do we actually want alt_var_length_view to be an output_range?
  CHECK(std::output_iterator<
        test_type_iterator,
        std::ranges::range_value_t<test_type>>);
  CHECK(!std::output_iterator<
        test_type_const_iterator,
        std::ranges::range_value_t<test_type>>);
  CHECK(std::forward_iterator<test_type_iterator>);
  CHECK(std::forward_iterator<test_type_const_iterator>);
  CHECK(std::bidirectional_iterator<test_type_iterator>);
  CHECK(std::bidirectional_iterator<test_type_const_iterator>);
  CHECK(std::random_access_iterator<test_type_iterator>);
  CHECK(std::random_access_iterator<test_type_const_iterator>);

  using ZI = test_type_iterator;
  CHECK(std::indirectly_writable<ZI, std::iter_rvalue_reference_t<ZI>>);
  CHECK(std::indirectly_writable<ZI, std::iter_value_t<ZI>>);

  CHECK(std::indirectly_movable<ZI, ZI>);

  CHECK(std::indirectly_movable_storable<ZI, ZI>);

  CHECK(std::movable<std::iter_value_t<ZI>>);
  CHECK(std::constructible_from<
        std::iter_value_t<ZI>,
        std::iter_rvalue_reference_t<ZI>>);
  CHECK(std::assignable_from<
        std::iter_value_t<ZI>&,
        std::iter_rvalue_reference_t<ZI>>);
  CHECK(std::forward_iterator<ZI>);

  CHECK(std::indirectly_swappable<ZI, ZI>);
  CHECK(std::permutable<ZI>);
  CHECK(!std::sortable<ZI>);

  CHECK(std::sortable<ZI, dummy_compare>);
}

// Test that the alt_var_length_view value_type satisfies the expected concepts
TEST_CASE(
    "alt_var_length_view: value_type concepts",
    "[alt_var_length_view][concepts]") {
  using test_type = alt_var_length_view<std::vector<double>, std::vector<int>>;
  CHECK(std::ranges::range<test_type>);

  using test_iterator_type = std::ranges::iterator_t<test_type>;
  using test_iterator_value_type = std::iter_value_t<test_iterator_type>;
  using test_iterator_reference_type =
      std::iter_reference_t<test_iterator_type>;

  using range_value_type = std::ranges::range_value_t<test_type>;
  using range_reference_type = std::ranges::range_reference_t<test_type>;

  CHECK(std::is_same_v<test_iterator_value_type, range_value_type>);
  CHECK(std::is_same_v<test_iterator_reference_type, range_reference_type>);
}

// Simple test that the alt_var_length_view can be constructed
TEST_CASE("alt_var_length_view: Basic constructor", "[alt_var_length_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};
  std::vector<std::vector<double>> expected = {
      {1.0, 2.0, 3.0},
      {4.0, 5.0, 6.0},
      {7.0, 8.0, 9.0, 10.0},
  };

  SECTION("iterator pair") {
    auto u = alt_var_length_view(r.begin(), r.end(), o.begin(), o.end());
    auto v = alt_var_length_view{r.begin(), r.end(), o.begin(), o.end()};
    alt_var_length_view w(r.begin(), r.end(), o.begin(), o.end());
    alt_var_length_view x{r.begin(), r.end(), o.begin(), o.end()};

    CHECK(size(u) == 3);
    CHECK(size(v) == 3);
    CHECK(size(w) == 3);
    CHECK(size(x) == 3);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }
  SECTION("iterator pair with size") {
    auto u = alt_var_length_view(r.begin(), r.end(), 6, o.begin(), o.end(), 3);
    auto v = alt_var_length_view{r.begin(), r.end(), 6, o.begin(), o.end(), 3};
    alt_var_length_view w(r.begin(), r.end(), 6, o.begin(), o.end(), 3);
    alt_var_length_view x{r.begin(), r.end(), 6, o.begin(), o.end(), 3};

    CHECK(size(u) == 2);
    CHECK(size(v) == 2);
    CHECK(size(w) == 2);
    CHECK(size(x) == 2);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }
  SECTION("range") {
    auto u = alt_var_length_view(r, o);
    auto v = alt_var_length_view{r, o};
    alt_var_length_view w(r, o);
    alt_var_length_view x{r, o};

    CHECK(size(u) == 3);
    CHECK(size(v) == 3);
    CHECK(size(w) == 3);
    CHECK(size(x) == 3);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }

  SECTION("range with size") {
    auto u = alt_var_length_view(r, 6, o, 3);
    auto v = alt_var_length_view{r, 6, o, 3};
    alt_var_length_view w(r, 6, o, 3);
    alt_var_length_view x{r, 6, o, 3};

    CHECK(size(u) == 2);
    CHECK(size(v) == 2);
    CHECK(size(w) == 2);
    CHECK(size(x) == 2);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }

  SECTION("iterator pair, tiledb format") {
    auto u =
        alt_var_length_view(r.begin(), r.end(), o.begin(), o.end() - 1, 10);
    auto v =
        alt_var_length_view{r.begin(), r.end(), o.begin(), o.end() - 1, 10};
    alt_var_length_view w(r.begin(), r.end(), o.begin(), o.end() - 1, 10);
    alt_var_length_view x{r.begin(), r.end(), o.begin(), o.end() - 1, 10};

    CHECK(size(u) == 3);
    CHECK(size(v) == 3);
    CHECK(size(w) == 3);
    CHECK(size(x) == 3);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }

  SECTION("iterator pair with size, tiledb format") {
    auto u =
        alt_var_length_view(r.begin(), r.end(), 6, o.begin(), o.end(), 2, 6);
    auto v =
        alt_var_length_view{r.begin(), r.end(), 6, o.begin(), o.end(), 2, 6};
    alt_var_length_view w(r.begin(), r.end(), 6, o.begin(), o.end(), 2, 6);
    alt_var_length_view x{r.begin(), r.end(), 6, o.begin(), o.end(), 2, 6};

    CHECK(size(u) == 2);
    CHECK(size(v) == 2);
    CHECK(size(w) == 2);
    CHECK(size(x) == 2);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }

  SECTION("range, tiledb format") {
    auto u = alt_var_length_view(r, std::ranges::views::take(o, 3), 10);
    auto v = alt_var_length_view{r, std::ranges::views::take(o, 3), 10};
    alt_var_length_view w(r, std::ranges::views::take(o, 3), 10);
    alt_var_length_view x{r, std::ranges::views::take(o, 3), 10};

    CHECK(size(u) == 3);
    CHECK(size(v) == 3);
    CHECK(size(w) == 3);
    CHECK(size(x) == 3);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }

  SECTION("range with size, tiledb format") {
    auto u = alt_var_length_view(r, 6, o, 2, 6);
    auto v = alt_var_length_view{r, 6, o, 2, 6};
    alt_var_length_view w(r, 6, o, 2, 6);
    alt_var_length_view x{r, 6, o, 2, 6};

    CHECK(size(u) == 2);
    CHECK(size(v) == 2);
    CHECK(size(w) == 2);
    CHECK(size(x) == 2);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }
}

// Check that the sizes of the alt_var_length_view are correct
TEST_CASE("alt_var_length_view: size()", "[alt_var_length_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};

  alt_var_length_view<std::vector<double>, std::vector<size_t>> v;
  SECTION("arrow format") {
    std::vector<size_t> o = {0, 3, 6, 10};
    v = alt_var_length_view{r, o};
  }
  CHECK(size(v) == 3);
  std::vector<size_t> o = {0, 3, 6};
  v = alt_var_length_view{r, o, 10};
  CHECK((v.begin() + 1)->size() == 3);
  CHECK((v.begin() + 2)->size() == 4);

  size_t count = 0;
  for (auto i : v) {
    (void)i;
    count++;
  }
  CHECK(count == 3);
  count = 0;
  for (auto i = v.begin(); i != v.end(); ++i) {
    count++;
  }
  CHECK(count == 3);
}

// Check that various properties of iterators hold
TEST_CASE("alt_var_length_view: Basic iterators", "[alt_var_length_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};

  alt_var_length_view<std::vector<double>, std::vector<size_t>> v;
  auto end_v = GENERATE(0, 10);

  if (end_v == 0) {
    std::vector<size_t> o = {0, 3, 6, 10};
    v = alt_var_length_view(r, o);
  } else {
    std::vector<size_t> o = {0, 3, 6};
    v = alt_var_length_view(r, o, end_v);
  }

  auto a = v.begin();
  auto b = v.end();
  auto c = begin(v);
  auto d = end(v);

  SECTION("Check begin and end") {
    REQUIRE(a == a);
    CHECK(a == c);
    CHECK(b == d);
  }
  SECTION("Check dereference") {
    CHECK(std::equal(a->begin(), a->end(), r.begin()));
    CHECK(std::equal(a->begin(), a->end(), c->begin()));
    // CHECK(std::ranges::equal(*a, r));  // size() not the same
    CHECK(std::ranges::equal(*a, *c));
  }
  SECTION("Check  pre-increment + dereference") {
    ++a;
    ++c;
    CHECK(std::equal(a->begin(), a->end(), c->begin()));
    CHECK(std::equal(a->begin(), a->end(), r.begin() + 3));
    CHECK(std::ranges::equal(*a, *c));
    ++a;
    ++c;
    CHECK(std::equal(a->begin(), a->end(), c->begin()));
    CHECK(std::equal(a->begin(), a->end(), r.begin() + 6));
    CHECK(std::ranges::equal(*a, *c));
  }
  SECTION("Check  post-increment + dereference") {
    a++;
    c++;
    CHECK(std::equal(a->begin(), a->end(), c->begin()));
    CHECK(std::equal(a->begin(), a->end(), r.begin() + 3));
    CHECK(std::ranges::equal(*a, *c));
    a++;
    c++;
    CHECK(std::equal(a->begin(), a->end(), c->begin()));
    CHECK(std::equal(a->begin(), a->end(), r.begin() + 6));
    CHECK(std::ranges::equal(*a, *c));
  }
  SECTION("operator[]") {
    CHECK(std::equal(a[0].begin(), a[0].end(), r.begin()));
    CHECK(std::equal(a[1].begin(), a[1].end(), r.begin() + 3));
    CHECK(std::equal(a[2].begin(), a[2].end(), r.begin() + 6));
    CHECK(std::equal(a[0].begin(), a[0].end(), c[0].begin()));
    CHECK(std::equal(a[1].begin(), a[1].end(), c[1].begin()));
    CHECK(std::equal(a[2].begin(), a[2].end(), c[2].begin()));
    CHECK(!std::equal(a[0].begin(), a[0].end(), c[1].begin()));
    CHECK(!std::equal(a[0].begin(), a[0].end(), c[2].begin()));

    CHECK(a[0][0] == 1.0);
    CHECK(a[0][1] == 2.0);
    CHECK(a[0][2] == 3.0);
    CHECK(a[1][0] == 4.0);
    CHECK(a[1][1] == 5.0);
    CHECK(a[1][2] == 6.0);
    CHECK(a[2][0] == 7.0);
    CHECK(a[2][1] == 8.0);
    CHECK(a[2][2] == 9.0);
    CHECK(a[2][3] == 10.0);
  }
  SECTION("More operator[]") {
    size_t count = 0;
    for (auto i : v) {
      for (auto j : i) {
        ++count;
        CHECK(j == count);
      }
    }
  }
  SECTION("Equality, etc") {
    CHECK(a == c);
    CHECK(!(a != c));
    CHECK(!(a < c));
    CHECK(!(a > c));
    CHECK(a <= c);
    CHECK(a >= c);

    CHECK(a != b);
    CHECK(!(a == b));
    CHECK(a < b);
    CHECK(!(a > b));
    CHECK(a <= b);
    CHECK(!(a >= b));

    ++a;
    CHECK(a != c);
    CHECK(!(a == c));
    CHECK(!(a < c));
    CHECK(a > c);
    CHECK(!(a <= c));
    CHECK(a >= c);
    --a;
    CHECK(a == c);
    CHECK(!(a != c));
    CHECK(!(a < c));
    CHECK(!(a > c));
    CHECK(a <= c);
    CHECK(a >= c);
    c++;
    CHECK(a != c);
    CHECK(!(a == c));
    CHECK(!(a > c));
    CHECK(a < c);
    CHECK(!(a >= c));
    CHECK(a <= c);
    c--;
    CHECK(a == c);
    CHECK(!(a != c));
    CHECK(!(a < c));
    CHECK(!(a > c));
    CHECK(a <= c);
    CHECK(a >= c);
  }
}

// Test that we can read and write to the elements of the alt_var_length_view
TEST_CASE("alt_var_length_view: Viewness", "[alt_var_length_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<double> s = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};
  std::vector<size_t> m = {0, 3, 6, 10};
  std::vector<double> q = {
      21.0, 20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0, 13.0, 12.0};
  std::vector<size_t> p = {0, 2, 7, 10};
  std::vector<size_t> n = {0, 3, 6, 10};

  alt_var_length_view<std::vector<double>, std::vector<size_t>> u, v, w, x, y,
      z;
  SECTION("Arrow format") {
    u = alt_var_length_view(q, n);
    v = alt_var_length_view(r, o);
    w = alt_var_length_view(q, p);
    x = alt_var_length_view(r, m);
    y = alt_var_length_view(s, m);
    z = alt_var_length_view(s, n);
  }
  SECTION("TileDB format") {
    std::vector<size_t> o = {0, 3, 6};
    std::vector<size_t> p = {0, 2, 7};
    u = alt_var_length_view(q, o, 10);
    v = alt_var_length_view(r, o, 10);
    w = alt_var_length_view(q, p, 10);
    x = alt_var_length_view(r, o, 10);
    y = alt_var_length_view(s, o, 10);
    z = alt_var_length_view(s, o, 10);
  }

  REQUIRE(v.begin() == v.begin());
  REQUIRE(v.end() == v.end());

  // MSVC in debug iterators mode fails with an assert if iterators of different
  // collections are compared.
  // https://learn.microsoft.com/en-us/cpp/standard-library/debug-iterator-support?view=msvc-170#incompatible-iterators
#if !defined(_ITERATOR_DEBUG_LEVEL) || _ITERATOR_DEBUG_LEVEL != 2
  CHECK(v.begin() != w.begin());
  CHECK(v.begin() != u.begin());
  CHECK(w.begin() != u.begin());

  CHECK(v.end() != w.end());
  CHECK(v.end() != u.end());
  CHECK(w.end() != u.end());
#endif

  for (size_t i = 0; i < 3; ++i) {
    CHECK(v.size() == x.size());
    CHECK(std::equal(
        v.begin()[i].begin(), v.begin()[i].end(), x.begin()[i].begin()));
    for (size_t j = 0; j < x.size(); ++j) {
      CHECK(v.begin()[i][j] == x.begin()[i][j]);
    }
  }
  for (auto&& i : y) {
    for (auto& j : i) {
      j += 13.0;
    }
  }
  for (size_t i = 0; i < 3; ++i) {
    CHECK(v.size() == x.size());
    for (size_t j = 0; j < x.size(); ++j) {
      CHECK(y.begin()[i][j] == v.begin()[i][j] + 13.0);
      CHECK(z.begin()[i][j] == y.begin()[i][j]);
      CHECK(z.cbegin()[i][j] == y.begin()[i][j]);
      CHECK(z.cbegin()[i][j] == y.cbegin()[i][j]);
      CHECK(z.cbegin()[i][j] == z.begin()[i][j]);
    }
  }
}

// Test that the alt_var_length_view can be sorted
TEST_CASE("alt_var_length_view: Sort", "[alt_var_length_view]") {
  std::vector<double> r = {
      1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0};
  std::vector<size_t> o = {0, 3, 6, 10, 12};
  alt_var_length_view<std::vector<double>, std::vector<size_t>> v(r, o);

  SECTION("Sort by size, ascending") {
    std::vector<std::vector<double>> expected = {
        {11.0, 12.0},
        {1.0, 2.0, 3.0},
        {4.0, 5.0, 6.0},
        {7.0, 8.0, 9.0, 10.0},
    };

    std::sort(v.begin(), v.end(), [](auto& a, auto& b) {
      return a.size() < b.size();
    });

    CHECK(v.begin()->size() == 2);
    CHECK((v.begin() + 1)->size() == 3);
    CHECK((v.begin() + 2)->size() == 3);
    CHECK((v.begin() + 3)->size() == 4);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }
  SECTION("Sort by size, descending") {
    std::vector<std::vector<double>> expected = {
        {7.0, 8.0, 9.0, 10.0},
        {1.0, 2.0, 3.0},
        {4.0, 5.0, 6.0},
        {11.0, 12.0},
    };

    std::sort(v.begin(), v.end(), [](auto& a, auto& b) {
      return a.size() > b.size();
    });

    CHECK(v.begin()->size() == 4);
    CHECK((v.begin() + 1)->size() == 3);
    CHECK((v.begin() + 2)->size() == 3);
    CHECK((v.begin() + 3)->size() == 2);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }
  SECTION("Sort by first element, ascending") {
    std::vector<std::vector<double>> expected = {
        {1.0, 2.0, 3.0},
        {4.0, 5.0, 6.0},
        {7.0, 8.0, 9.0, 10.0},
        {11.0, 12.0},
    };

    std::sort(v.begin(), v.end(), [](auto& a, auto& b) { return a[0] < b[0]; });

    CHECK(v.begin()->size() == 3);
    CHECK((v.begin() + 1)->size() == 3);
    CHECK((v.begin() + 2)->size() == 4);
    CHECK((v.begin() + 3)->size() == 2);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }
  SECTION("Sort by first element, descending") {
    std::vector<std::vector<double>> expected = {
        {11.0, 12.0},
        {7.0, 8.0, 9.0, 10.0},
        {4.0, 5.0, 6.0},
        {1.0, 2.0, 3.0},
    };

    std::sort(v.begin(), v.end(), [](auto& a, auto& b) { return a[0] > b[0]; });

    CHECK(v.begin()->size() == 2);
    CHECK((v.begin() + 1)->size() == 4);
    CHECK((v.begin() + 2)->size() == 3);
    CHECK((v.begin() + 3)->size() == 3);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }
}

TEST_CASE("alt_var_length_view: Sort and actualize", "[alt_var_length_view]") {
  std::vector<double> r = {
      1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0};
  std::vector<double> s(r);
  std::vector<size_t> o = {0, 3, 6, 10, 12};
  alt_var_length_view<std::vector<double>, std::vector<size_t>> v(r, o);

  SECTION("Sort by size, ascending") {
    std::vector<std::vector<double>> expected = {
        {11.0, 12.0},
        {1.0, 2.0, 3.0},
        {4.0, 5.0, 6.0},
        {7.0, 8.0, 9.0, 10.0},
    };

    std::sort(v.begin(), v.end(), [](auto& a, auto& b) {
      return a.size() < b.size();
    });

    CHECK(v.begin()->size() == 2);
    CHECK((v.begin() + 1)->size() == 3);
    CHECK((v.begin() + 2)->size() == 3);
    CHECK((v.begin() + 3)->size() == 4);

    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }

    // Check the underlying data has not changed even though sorted
    CHECK(std::ranges::equal(r, s));
    std::vector<double> scratch(size(r));

    actualize(v, r, o, scratch);

    // Check the underlying data has changed to the expected sorted order
    CHECK(std::ranges::equal(
        r,
        std::vector<double>{
            11.0, 12.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}));

    // The alt_var_length_view should still "look" the same
    CHECK(std::ranges::equal(o, std::vector<size_t>{2, 3, 3, 4, 12}));
    for (auto&& i : v) {
      CHECK(std::ranges::equal(i, expected[&i - &*v.begin()]));
    }
  }
}
