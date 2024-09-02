/**
 * @file   unit_permutation_view.cc
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
 * This file implements unit tests for the permutation_view class.
 */

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <vector>
#include "tiledb/common/util/permutation_view.h"
#include "tiledb/common/util/var_length_view.h"

TEST_CASE("permutation_view: Null test", "[permutation_view][null_test]") {
  REQUIRE(true);
}

// Test that the permutation_view satisfies the expected concepts
TEST_CASE("permutation_view: Range concepts", "[permutation_view][concepts]") {
  using test_type = permutation_view<std::vector<double>, std::vector<int>>;

  CHECK(std::ranges::range<test_type>);
  CHECK(!std::ranges::borrowed_range<test_type>);
  CHECK(std::ranges::sized_range<test_type>);
  CHECK(std::ranges::view<test_type>);
  CHECK(std::ranges::input_range<test_type>);
  CHECK(std::ranges::
            output_range<test_type, std::ranges::range_value_t<test_type>>);
  CHECK(std::ranges::forward_range<test_type>);
  CHECK(std::ranges::bidirectional_range<test_type>);
  CHECK(std::ranges::random_access_range<test_type>);
  CHECK(!std::ranges::contiguous_range<test_type>);
  CHECK(std::ranges::common_range<test_type>);
  CHECK(std::ranges::viewable_range<test_type>);

  CHECK(std::ranges::view<test_type>);
}

// Test that the permutation_view iterators satisfy the expected concepts
TEST_CASE(
    "permutation_view: Iterator concepts", "[permutation_view][concepts]") {
  using test_type = permutation_view<std::vector<double>, std::vector<int>>;
  using test_type_iterator = std::ranges::iterator_t<test_type>;
  using test_type_const_iterator = std::ranges::iterator_t<const test_type>;

  CHECK(std::input_or_output_iterator<test_type_iterator>);
  CHECK(std::input_or_output_iterator<test_type_const_iterator>);
  CHECK(std::input_iterator<test_type_iterator>);
  CHECK(std::input_iterator<test_type_const_iterator>);
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
}

// Test that the permutation_view value_type satisfies the expected concepts
TEST_CASE(
    "permutation_view: value_type concepts", "[permutation_view][concepts]") {
  using test_type = permutation_view<std::vector<double>, std::vector<int>>;
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

TEST_CASE("permutation_view: simple constructor", "permutation_view") {
  std::vector<int> v{1, 2, 3, 4, 5};
  std::vector<size_t> p{4, 3, 2, 1, 0};
  permutation_view view(v, p);
  CHECK(view.size() == 5);
  CHECK(view[0] == 5);
  CHECK(view[1] == 4);
  CHECK(view[2] == 3);
  CHECK(view[3] == 2);
  CHECK(view[4] == 1);

  int off = 1;
  for (auto&& i : view) {
    CHECK(i == 6 - off++);
  }

  CHECK(std::ranges::equal(view, std::vector<int>{5, 4, 3, 2, 1}));
}

TEST_CASE(
    "permutation_view: check various iterator properties hold",
    "[permutation_view]") {
  std::vector<int> v{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<size_t> p{9, 1, 3, 2, 8, 6, 5, 7, 4, 0};
  std::vector<int> expected{10, 2, 4, 3, 9, 7, 6, 8, 5, 1};

  permutation_view view(v, p);
  CHECK(std::equal(view.begin(), view.end(), expected.begin()));
  CHECK(std::equal(view.cbegin(), view.cend(), expected.begin()));
  CHECK(std::ranges::equal(view, expected));

  auto it = view.begin();
  CHECK(*it == 10);
  CHECK(*(it + 1) == 2);
  CHECK(it[2] == 4);
  it[3] = 100;
  CHECK(it[3] == 100);
  CHECK(view[3] == 100);
  *it++ = 200;
  CHECK(view[0] == 200);
  CHECK(*it == 2);

  CHECK(it == view.begin() + 1);
  CHECK(it > view.begin());
  CHECK(it >= view.begin());
  CHECK(view.begin() < it);
  CHECK(view.begin() <= it);
  CHECK(it < view.end());
  CHECK(it <= view.end());
  CHECK(view.end() > it);
  CHECK(view.end() >= it);

  --it;
  CHECK(it == view.begin());
  view[9] = 1000;
  CHECK(it[9] == 1000);
  CHECK(view.end() - view.begin() == 10);
  CHECK(view.end() - it == 10);
  ++it;
  CHECK(it == view.begin() + 1);
  CHECK(view.end() - it == 9);

  auto it2 = it + 5;
  CHECK(it2 - it == 5);
  CHECK(it2 - 5 == it);
  CHECK(it2 - 6 == view.begin());
  CHECK(it2 - view.begin() == 6);

  CHECK(*it2 == 6);

  auto cit = view.cbegin();
  (void)cit;
  // Error: Not assignable
  // *cit = 1;
}

TEST_CASE(
    "permutation_view: permute var length view",
    "[permutation_view][var_length_view]") {
  std::vector<double> q = {
      21.0, 20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0, 13.0, 12.0};
  std::vector<size_t> p = {0, 2, 7, 10};
  std::vector<size_t> o = {2, 0, 1};

  std::vector<std::vector<double>> expected{
      {21.0, 20.0}, {19.0, 18.0, 17.0, 16.0, 15.0}, {14.0, 13.0, 12.0}};
  auto w = var_length_view(q, p);
  CHECK(std::ranges::equal(*(w.begin()), expected[0]));
  CHECK(std::ranges::equal(*(w.begin() + 1), expected[1]));
  CHECK(std::ranges::equal(*(w.begin() + 2), expected[2]));

  auto x = permutation_view(w, o);
  CHECK(std::ranges::equal(*(x.begin()), expected[2]));
  CHECK(std::ranges::equal(*(x.begin() + 1), expected[0]));
  CHECK(std::ranges::equal(*(x.begin() + 2), expected[1]));
}
