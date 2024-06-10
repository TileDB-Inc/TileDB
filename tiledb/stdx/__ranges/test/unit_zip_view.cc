/**
 * @file   tiledb/stdx/__ranges/test/unit_zip_view.cc
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
 * This file implements unit tests for the zip_view class.
 */

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <type_traits>
#include <vector>
#include "tiledb/common/util/alt_var_length_view.h"
#include "tiledb/stdx/__ranges/zip_view.h"

TEST_CASE("zip_view: Null test", "[zip_view][null_test]") {
  REQUIRE(true);
}

TEST_CASE("zip_view: Should not copy", "[zip_view]") {
  struct foo : public std::vector<int> {
    explicit foo(int N)
        : std::vector<int>(N) {
    }
    foo(const foo&)
        : std::vector<int>() {
      CHECK(false);
      CHECK(true);
    }
    foo(foo&&) = delete;
    foo& operator=(foo&) = delete;
    foo& operator=(foo&&) = delete;
    foo() = delete;
    ~foo() = default;
  };

  auto f = foo(10);
  auto g = foo(10);
  auto h = foo(10);
  [[maybe_unused]] auto z = zip(f, g, h);
}

/** Test that the zip_view type satisfies the expected range concepts */
TEST_CASE("zip_view: Range concepts", "[zip_view][concepts]") {
  using test_type =
      stdx::ranges::zip_view<std::vector<double>, std::vector<int>>;

  CHECK(std::ranges::range<test_type>);
  CHECK(!std::ranges::borrowed_range<test_type>);
  CHECK(std::ranges::sized_range<test_type>);
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

/** Test that the zip_view iterator satisfies the expected concepts */
TEST_CASE("zip_view: Iterator concepts", "[zip_view][concepts]") {
  using test_type =
      stdx::ranges::zip_view<std::vector<double>, std::vector<int>>;
  using test_type_iterator = std::ranges::iterator_t<test_type>;
  using test_type_const_iterator = std::ranges::iterator_t<const test_type>;
  // using test_type_const_iterator = decltype((const test_type){}.begin());
  CHECK(std::is_same_v<
        test_type_const_iterator,
        decltype(std::declval<const test_type>().begin())>);

  CHECK(std::input_or_output_iterator<test_type_iterator>);
  CHECK(std::input_iterator<test_type_iterator>);

  CHECK(std::input_or_output_iterator<test_type_const_iterator>);

  /*
   * The following tests fail for the const iterator.  Whcih seems to be
   * correct behavior:
   * cf https://quuxplusone.github.io/blog/2023/08/13/non-const-iterable-ranges/
   *
   * The tests here are the fine grained constituents of forward iterator,
   * which we include to zoom in on exactly where the failure is.  It appears
   * to be in the common_reference concepts below.  The concept checks fail in
   * that the code does not even compile.
   */
  CHECK(!std::input_iterator<test_type_const_iterator>);
  CHECK(!std::indirectly_readable<test_type_const_iterator>);
  CHECK(!std::common_reference_with<
        std::iter_reference_t<test_type_const_iterator>&&,
        std::iter_value_t<test_type_const_iterator>&>);

  CHECK(std::common_reference_with<
        std::iter_reference_t<test_type_const_iterator>&&,
        std::iter_reference_t<test_type_const_iterator>&>);
  CHECK(std::common_reference_with<
        std::iter_reference_t<test_type_const_iterator>&&,
        std::iter_rvalue_reference_t<test_type_const_iterator>&>);

  CHECK(!std::output_iterator<
        test_type_const_iterator,
        std::ranges::range_value_t<test_type>>);
  CHECK(!std::forward_iterator<test_type_const_iterator>);
  CHECK(!std::bidirectional_iterator<test_type_const_iterator>);
  CHECK(!std::random_access_iterator<test_type_const_iterator>);

  /*
   * These will not compile
  using T = std::iter_reference_t<test_type_const_iterator>;
  using U = std::iter_value_t<test_type_const_iterator>;

  CHECK(std::same_as<
        std::common_reference_t<T, U>,
        std::common_reference_t<U, T>>);
  CHECK(std::convertible_to<T, std::common_reference_t<T, U>>);
  CHECK(std::convertible_to<U, std::common_reference_t<T, U>>);
   */

  CHECK(!std::output_iterator<
        test_type_iterator,
        std::ranges::range_value_t<test_type>>);

  CHECK(std::forward_iterator<test_type_iterator>);
  CHECK(std::bidirectional_iterator<test_type_iterator>);
  CHECK(std::random_access_iterator<test_type_iterator>);
}

// Test that the zip_view value_type satisfies the expected concepts
TEST_CASE("zip_view: value_type concepts", "[zip_view][concepts]") {
  using test_type =
      stdx::ranges::zip_view<std::vector<double>, std::vector<int>>;
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

/** Test zip_view constructors */
TEST_CASE("zip_view: constructor", "[zip_view]") {
  std::vector<int> a{1, 2, 3};
  std::vector<int> b{4, 5, 6};
  std::vector<int> c{7, 8, 9};

  SECTION("Zip one range") {
    auto z = zip(a);
    auto it = z.begin();
    CHECK(*it == std::tuple{1});
    ++it;
    CHECK(*it == std::tuple{2});
    ++it;
    CHECK(*it == std::tuple{3});
    it = z.begin();
    std::get<0>(*it) = 99;
    CHECK(a[0] == 99);
  }

  SECTION("Zip three ranges") {
    auto z = zip(a, b, c);
    auto it = z.begin();
    CHECK(*it == std::tuple{1, 4, 7});
    ++it;
    CHECK(*it == std::tuple{2, 5, 8});
    ++it;
    CHECK(*it == std::tuple{3, 6, 9});
    it = z.begin();
    std::get<0>(*it) = 41;
    std::get<1>(*it) = 42;
    std::get<2>(*it) = 43;
    CHECK(a[0] == 41);
    CHECK(b[0] == 42);
    CHECK(c[0] == 43);
  }
}

/** Test that size returns the min of the sizes of the input ranges */
TEST_CASE("zip_view: size()", "[zip_view]") {
  std::vector<int> a{1, 2, 3};
  std::vector<int> b{4, 5, 6, 7, 8, 9};
  std::vector<int> c{10, 11, 12, 13};

  CHECK(zip(a).size() == 3);
  CHECK(zip(b).size() == 6);
  CHECK(zip(c).size() == 4);
  CHECK(zip(a, b).size() == 3);
  CHECK(zip(a, c).size() == 3);
  CHECK(zip(b, c).size() == 4);
  CHECK(zip(a, b, c).size() == 3);
}

/** The end() of the zipped ranges should be begin() + size() */
TEST_CASE("zip_view: end()", "[zip_view]") {
  std::vector<int> a{1, 2, 3};
  std::vector<int> b{4, 5, 6, 7, 8, 9};
  std::vector<int> c{10, 11, 12, 13};

  [[maybe_unused]] auto x = zip(a).begin();
  [[maybe_unused]] auto y = zip(a).end();

  CHECK(zip(a).end() == zip(a).begin() + 3);
  CHECK(zip(b).end() == zip(b).begin() + 6);
  CHECK(zip(c).end() == zip(c).begin() + 4);
  CHECK(zip(a, b).end() == zip(a, b).begin() + 3);
  CHECK(zip(a, c).end() == zip(a, c).begin() + 3);
  CHECK(zip(b, c).end() == zip(b, c).begin() + 4);
  CHECK(zip(a, b, c).end() == zip(a, b, c).begin() + 3);

  CHECK(zip(a).end() - zip(a).begin() == 3);
  CHECK(zip(b).end() - zip(b).begin() == 6);
  CHECK(zip(c).end() - zip(c).begin() == 4);
  CHECK(zip(a, b).end() - zip(a, b).begin() == 3);
  CHECK(zip(a, c).end() - zip(a, c).begin() == 3);
  CHECK(zip(b, c).end() - zip(b, c).begin() == 4);
  CHECK(zip(a, b, c).end() - zip(a, b, c).begin() == 3);
}

/** Exercise the zip_view iterator */
TEST_CASE("zip_view: basic iterator properties", "[zip_view]") {
  std::vector<int> a{1, 2, 3};
  std::vector<int> b{4, 5, 6, 7, 8, 9};
  std::vector<int> c{10, 11, 12, 13};

  auto z = zip(a, b, c);
  auto it = z.begin();
  CHECK(it == begin(z));
  auto it2 = z.begin();
  CHECK(it == it2);
  CHECK(*it == *it2);
  it++;
  CHECK(it != it2);
  it2++;
  CHECK(it == it2);
  CHECK(*it == *it2);
  auto jt = z.end();
  CHECK(jt == end(z));
  CHECK(it != jt);
  CHECK(it < jt);
  CHECK(it <= jt);
  CHECK(jt > it);
  CHECK(jt >= it);
  CHECK(jt == jt);
  CHECK(jt >= jt);
  CHECK(jt <= jt);

  it = z.begin();
  auto x = *it++;
  CHECK(x == std::tuple{1, 4, 10});
  CHECK(it == z.begin() + 1);

  it = z.begin();
  auto y = *++it;
  CHECK(y == std::tuple{2, 5, 11});
  CHECK(it == z.begin() + 1);

  CHECK(it[0] == *it);
  // MSVC in debug iterators mode fails with an assert if out of range iterators
  // are dereferenced.
  // https://learn.microsoft.com/en-us/cpp/standard-library/debug-iterator-support
#if !defined(_ITERATOR_DEBUG_LEVEL) || _ITERATOR_DEBUG_LEVEL != 2
  CHECK(it[1] == *(it + 1));
  CHECK(it[2] == *(it + 2));
#endif
  CHECK(it[0] == std::tuple{2, 5, 11});
}

/** Test zip with an alt_var_length_view */
TEST_CASE("zip_view: alt_var_length_view", "[zip_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};
  auto v = alt_var_length_view{r, o};
  std::vector<int> a{1, 2, 3};
  std::vector<int> b{4, 5, 6, 7, 8, 9};
  std::vector<int> c{10, 11, 12, 13};

  auto z = zip(a, b, c, v);
  auto it = z.begin();
  CHECK(std::get<0>(*it) == 1);
  CHECK(std::get<1>(*it) == 4);
  CHECK(std::get<2>(*it) == 10);
  CHECK(std::ranges::equal(
      std::get<3>(*it++), std::vector<double>{1.0, 2.0, 3.0}));
  CHECK(std::ranges::equal(
      std::get<3>(*it++), std::vector<double>{4.0, 5.0, 6.0}));
  CHECK(std::ranges::equal(
      std::get<3>(*it++), std::vector<double>{7.0, 8.0, 9.0, 10.0}));
}

/** Use zip_view with std::for_each and with range-based for */
TEST_CASE(
    "zip_view: for, std::for_each with alt_var_length_view", "[zip_view]") {
  std::vector<double> r = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<size_t> o = {0, 3, 6, 10};
  auto v = alt_var_length_view{r, o};
  std::vector<int> a{8, 6, 7};

  auto z = zip(a, v);

  SECTION("for_each") {
    size_t count = 0;
    std::for_each(z.begin(), z.end(), [&a, &count](auto x) {
      auto&& [i, j] = x;
      CHECK(i == a[count]);
      switch (count) {
        case 0:
          CHECK(std::ranges::equal(j, std::vector<double>{1.0, 2.0, 3.0}));
          break;
        case 1:
          CHECK(std::ranges::equal(j, std::vector<double>{4.0, 5.0, 6.0}));
          break;
        case 2:
          CHECK(
              std::ranges::equal(j, std::vector<double>{7.0, 8.0, 9.0, 10.0}));
          break;
      }
      ++count;
    });
  }
  SECTION("for") {
    size_t count = 0;
    for (auto x : z) {
      auto&& [i, j] = x;
      CHECK(i == a[count]);
      switch (count) {
        case 0:
          CHECK(std::ranges::equal(j, std::vector<double>{1.0, 2.0, 3.0}));
          break;
        case 1:
          CHECK(std::ranges::equal(j, std::vector<double>{4.0, 5.0, 6.0}));
          break;
        case 2:
          CHECK(
              std::ranges::equal(j, std::vector<double>{7.0, 8.0, 9.0, 10.0}));
          break;
      }
      ++count;
    }
  }
}
