/**
 * @file   unit_var_length_view.cc
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
 * This file implements unit tests for the var_length_view class.
 */

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <vector>
#include "tiledb/common/util/var_length_view.h"

TEST_CASE("var_length_view: Null test", "[var_length_view][null_test]") {
  REQUIRE(true);
}

// Test that the var_length_view satisfies the expected concepts
TEST_CASE("var_length_view: Range concepts", "[var_length_view][concepts]") {
  using test_type = var_length_view<std::vector<double>, std::vector<int>>;

  CHECK(std::ranges::range<test_type>);
  CHECK(!std::ranges::borrowed_range<test_type>);
  CHECK(std::ranges::sized_range<test_type>);
  CHECK(std::ranges::view<test_type>);
  CHECK(std::ranges::input_range<test_type>);
  CHECK(!std::ranges::
            output_range<test_type, std::ranges::range_value_t<test_type>>);
  CHECK(std::ranges::forward_range<test_type>);
  CHECK(std::ranges::bidirectional_range<test_type>);
  CHECK(std::ranges::random_access_range<test_type>);
  CHECK(!std::ranges::contiguous_range<test_type>);
  CHECK(std::ranges::common_range<test_type>);
  CHECK(std::ranges::viewable_range<test_type>);

  CHECK(std::ranges::view<test_type>);
}

// Test that the var_length_view iterators satisfy the expected concepts
TEST_CASE("var_length_view: Iterator concepts", "[var_length_view][concepts]") {
  using test_type = var_length_view<std::vector<double>, std::vector<int>>;
  using test_type_iterator = std::ranges::iterator_t<test_type>;
  using test_type_const_iterator = std::ranges::iterator_t<const test_type>;

  CHECK(std::input_or_output_iterator<test_type_iterator>);
  CHECK(std::input_or_output_iterator<test_type_const_iterator>);
  CHECK(std::input_iterator<test_type_iterator>);
  CHECK(std::input_iterator<test_type_const_iterator>);
  CHECK(!std::output_iterator<
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
}

// Test that the var_length_view value_type satisfies the expected concepts
TEST_CASE(
    "var_length_view: value_type concepts", "[var_length_view][concepts]") {
  using test_type = var_length_view<std::vector<double>, std::vector<int>>;
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

// Simple test that the var_length_view can be constructed
TEST_CASE("var_length_view: Constructors", "[var_length_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};

  SECTION("iterator pair") {
    auto u = var_length_view(r.begin(), r.end(), o.begin(), o.end());
    auto v = var_length_view{r.begin(), r.end(), o.begin(), o.end()};
    var_length_view w(r.begin(), r.end(), o.begin(), o.end());
    var_length_view x{r.begin(), r.end(), o.begin(), o.end()};

    CHECK(size(u) == 3);
    CHECK(size(v) == 3);
    CHECK(size(w) == 3);
    CHECK(size(x) == 3);
  }
  SECTION("iterator pair with size") {
    auto u = var_length_view(r.begin(), r.end(), 6, o.begin(), o.end(), 3);
    auto v = var_length_view{r.begin(), r.end(), 6, o.begin(), o.end(), 3};
    var_length_view w(r.begin(), r.end(), 6, o.begin(), o.end(), 3);
    var_length_view x{r.begin(), r.end(), 6, o.begin(), o.end(), 3};

    CHECK(size(u) == 2);
    CHECK(size(v) == 2);
    CHECK(size(w) == 2);
    CHECK(size(x) == 2);
  }
  SECTION("range") {
    auto u = var_length_view(r, o);
    auto v = var_length_view{r, o};
    var_length_view w(r, o);
    var_length_view x{r, o};

    CHECK(size(u) == 3);
    CHECK(size(v) == 3);
    CHECK(size(w) == 3);
    CHECK(size(x) == 3);
  }
  SECTION("range with size") {
    auto u = var_length_view(r, 6, o, 3);
    auto v = var_length_view{r, 6, o, 3};
    var_length_view w(r, 6, o, 3);
    var_length_view x{r, 6, o, 3};

    CHECK(size(u) == 2);
    CHECK(size(v) == 2);
    CHECK(size(w) == 2);
    CHECK(size(x) == 2);
  }
}

// Check that the sizes of the var_length_view are correct
TEST_CASE("var_length_view: size()", "[var_length_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};

  auto v = var_length_view{r, o};
  CHECK(size(v) == 3);
  CHECK(v.begin()->size() == 3);
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
TEST_CASE("var_length_view: Basic iterators", "[var_length_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};

  auto v = var_length_view(r, o);

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

// Test that we can read and write to the elements of the var_length_view
TEST_CASE("var_length_view: Viewness", "[var_length_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<double> s = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};
  std::vector<size_t> m = {0, 3, 6, 10};
  std::vector<double> q = {
      21.0, 20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0, 13.0, 12.0};
  std::vector<size_t> p = {0, 2, 7, 10};
  std::vector<size_t> n = {0, 3, 6, 10};

  auto v = var_length_view(r, o);
  auto w = var_length_view(q, p);
  auto u = var_length_view(q, n);
  auto x = var_length_view(r, m);
  auto y = var_length_view(s, m);
  auto z = var_length_view(s, n);

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
    for (long j = 0; j < x.size(); ++j) {
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
    for (long j = 0; j < x.size(); ++j) {
      CHECK(y.begin()[i][j] == v.begin()[i][j] + 13.0);
      CHECK(z.begin()[i][j] == y.begin()[i][j]);
      CHECK(z.cbegin()[i][j] == y.begin()[i][j]);
      CHECK(z.cbegin()[i][j] == y.cbegin()[i][j]);
      CHECK(z.cbegin()[i][j] == z.begin()[i][j]);
    }
  }
}
