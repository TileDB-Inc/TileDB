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
 * Tests various aspects of the join view.
 *
 */

#include <iostream>
#include <list>
#include <vector>
#include "external/include/span/span.hpp"

#include <test/support/tdb_catch.h>
#include "experimental/tiledb/common/dag/utility/range_join.h"
#include "experimental/tiledb/common/dag/utility/traits.h"

#include "../print_types.h"

using namespace tiledb::common;

template <class T>
[[nodiscard]] T non_const_constructor(T x) {
  CHECK(!std::is_const_v<T>);

  auto a = x.begin();
  auto b = x.end();
  auto c = x.cbegin();
  auto d = x.cend();

  CHECK(has_iterator_v<decltype(x)>);
  CHECK(has_begin_end_v<decltype(x)>);

  using Inner = inner_range_t<T>;

  if constexpr (!is_span_v<Inner>) {
    CHECK(has_const_iterator_v<decltype(x)>);

    CHECK(!is_const_iterator_v<typename decltype(x)::iterator>);
    CHECK(is_const_iterator_v<typename decltype(x)::const_iterator>);

    CHECK(!is_const_iterator_v<decltype(a)>);
    CHECK(!is_const_iterator_v<decltype(b)>);

    CHECK(is_const_iterator_v<decltype(c)>);
    CHECK(is_const_iterator_v<decltype(d)>);
  }

  while (a != b) {
    *a++ += 1;
  }

  return x;
}

template <class T>
[[nodiscard]] T const_constructor(const T x) {
  CHECK(std::is_const_v<decltype(x)>);
  [[maybe_unused]] auto a = x.begin();
  [[maybe_unused]] auto b = x.end();

  CHECK(has_iterator_v<decltype(x)>);
  CHECK(has_begin_end_v<decltype(x)>);

  if constexpr (!is_span_v<T>) {
    CHECK(is_const_iterator_v<typename decltype(x)::const_iterator>);
    CHECK(is_const_iterator_v<decltype(a)>);
    CHECK(is_const_iterator_v<decltype(b)>);
  }

  // This should fail to compile, unfortunately catch can't check for that.
  // Should periodically uncomment this and verify manually.
  //  while (a != b) {
  //    *a++ += 1;
  //  }

  return x;
}

template <class T>
[[nodiscard]] T non_const_move(T&& x) {
  CHECK(!std::is_const_v<decltype(x)>);

  // rvalue reference doesn't like ::
  // CHECK(has_iterator_v<decltype(x)>);
  CHECK(has_begin_end_v<decltype(x)>);

  auto a = x.begin();
  auto b = x.end();

  if constexpr (!is_span_v<T>) {
    CHECK(!is_const_iterator_v<decltype(a)>);
    CHECK(!is_const_iterator_v<decltype(b)>);
  }

  while (a != b) {
    *a++ += 1;
  }

  return std::move(x);
}

template <class T>
[[nodiscard]] T non_const_reference(T& x) {
  auto a = x.begin();
  auto b = x.end();

  while (a != b) {
    *a++ += 1;
  }

  return x;
}

template <class T>
[[nodiscard]] T const_reference(const T& x) {
  // This should fail to compile, unfortunately catch can't check for that.
  // Should periodically uncomment this and verify manually.
  // auto a = x.begin();
  // auto b = x.end();
  // while (a != b) {
  //   *a++ += 1;
  //  }

  return x;
}

template <class R>
void test_constructors_etc() {
  using I = typename R::value_type;

  I a{1, 2, 3, 4};
  I b{5, 6, 7, 8};
  I c{1, 2, 3, 4, 5, 6, 7, 8};
  R d{a, b};

  auto v = std::vector<int>(10);

  auto uv = non_const_constructor(v);
  auto vv = const_constructor(v);
  auto wv = non_const_move(std::move(v));
  auto xv = non_const_reference(v);
  auto yv = const_reference(v);

  auto e = join(d);

  auto ue = non_const_constructor(e);
  auto ve = const_constructor(e);
  auto we = non_const_move(std::move(e));
  auto xe = non_const_reference(e);
  auto ye = const_reference(e);
}

TEST_CASE("Join: Test construct", "[join]") {
  SECTION("list of lists") {
    test_constructors_etc<std::list<std::list<int>>>();
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
  std::list<tcb::span<int>> d{
      tcb::span<int>{a.data(), a.size() - 1},
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

// This works for g++-11 but not for Mac c++ (clang)
// template <template <class> class Outer, class Inner>
// void foo()
// }
// foo<std::vector, std::vector<std::vector<int>>();

template <class Outer, class Inner>
void test_iterators() {
  Inner a{1, 2, 3, 4};
  Inner b{5, 6, 7, 8};
  Inner c{1, 2, 3, 4, 5, 6, 7, 8};
  Outer d{a, b};  // clang doesn't allow Outer<Inner>

  auto e = join(d);

  auto ba = a.begin();
  auto be = e.begin();
  CHECK(*ba == 1);
  CHECK(*be == 1);
  CHECK(*be == *ba);

  *be = 19;
  CHECK(*be == 19);

  // Note that join was passed the inner containers by value, hence
  CHECK(*ba != *be);

  be++;
  CHECK(*be == 2);

  be = e.begin();
  *be = 1;

  for (auto& j : e) {
    j++;
  }
  auto bc = c.begin();
  CHECK(e.size() == c.size());
  while (bc != c.end()) {
    CHECK(*be++ == (*bc++ + 1));
  }
}

TEST_CASE("Join: iterators", "[join]") {
  SECTION("list of lists") {
    test_iterators<std::list<std::list<int>>, std::list<int>>();
  }
  SECTION("list of vectors") {
    test_iterators<std::list<std::vector<int>>, std::vector<int>>();
  }
  SECTION("vector of lists") {
    test_iterators<std::vector<std::list<int>>, std::list<int>>();
  }
  SECTION("vector of vectors") {
    test_iterators<std::vector<std::vector<int>>, std::vector<int>>();
  }
}

template <class Outer, class Inner>
void test_operator_bracket() {
  Inner a{1, 2, 3, 4};
  Inner b{5, 6, 7, 8};
  Inner c{1, 2, 3, 4, 5, 6, 7, 8};
  Outer d{a, b};  // clang doesn't allow Outer<Inner>

  auto e = join(d);

  CHECK(e.size() == a.size() + b.size());

  e[0] = 19;
  CHECK(e[0] == 19);
  e[0] = 1;
  for (size_t i = 0; i < size(c); ++i) {
    CHECK(e[i] == c[i]);
  }
}

TEST_CASE("Join: operator[]", "[join]") {
  SECTION("vector of vectors") {
    test_operator_bracket<std::vector<std::vector<int>>, std::vector<int>>();
  }

  // This will be rejected because operator[] is only defined for random access
  // ranges of random access ranges
  //
  //  SECTION("list of vectors") {
  //    test_operator_bracket<std::list<std::vector<int>>, std::vector<int>>();
  //  }
}

/*
 * Test range of spans.  Note that since spans are also a view, copies of them
 * still reference the same underlying data.  Thus, changes to the underlying
 * data should be reflected in the joined view.
 */
template <template <class, class> class Outer, class Inner>
void test_modifying_range_of_spans() {
  Inner a{1, 2, 3, 4};
  Inner b{5, 6, 7, 8};
  Inner c{1, 2, 3, 4, 5, 6, 7, 8};

  Outer<tcb::span<int>, std::allocator<tcb::span<int>>> d{
      tcb::span<int>{a.data(), a.size()}, tcb::span<int>{b.data(), b.size()}};
  auto e = join(d);

  CHECK(e.size() == a.size() + b.size());

  auto bc = c.begin();
  auto be = e.begin();
  while (bc != c.end()) {
    CHECK(*be++ == *bc++);
  }

  /*
   * Modify the underlying spans, increment them by 1
   */
  for (auto& j : a) {
    ++j;
  }
  for (auto& j : b) {
    ++j;
  }
  bc = c.begin();
  be = e.begin();
  while (bc != c.end()) {
    CHECK(*be++ == (*bc++ + 1));
  }
}

TEST_CASE("Join: Test iterator list of spans", "[join]") {
  SECTION("list of vectors") {
    test_modifying_range_of_spans<std::list, std::vector<int>>();
  }
  SECTION("vector of vectors") {
    test_modifying_range_of_spans<std::vector, std::vector<int>>();
  }
}
