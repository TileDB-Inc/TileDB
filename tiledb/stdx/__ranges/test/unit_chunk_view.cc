/**
 * @file   tiledb/stdx/__ranges/test/unit_chunk_view.cc
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
 * This file implements unit tests for the chunk_view class.
 */

#include <numeric>

#include <catch2/catch_all.hpp>
#include "tiledb/stdx/__ranges/chunk_view.h"

TEST_CASE("chunk_view: null test", "[chunk_view][null test]") {
  REQUIRE(true);
}

// Test that the chunk_view satisfies the expected concepts
TEST_CASE("chunk_view: Range concepts", "[chunk_view][concepts]") {
  using test_type = stdx::ranges::chunk_view<std::vector<double>>;

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

// Test that the chunk_view iterators satisfy the expected concepts
TEST_CASE("chunk_view: Iterator concepts", "[chunk_view][concepts]") {
  using test_type = stdx::ranges::chunk_view<std::vector<double>>;
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

// Test that the chunk_view value_type satisfies the expected concepts
TEST_CASE("chunk_view: value_type concepts", "[chunk_view][concepts]") {
  using test_type = stdx::ranges::chunk_view<std::vector<double>>;
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

std::vector<double> v10 = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
std::vector<double> v11 = {
    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0};
std::vector<double> v12 = {
    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0};
std::vector<double> v13 = {
    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0};
std::vector<double> v14 = {
    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0};
std::vector<double> v15 = {
    1.0,
    2.0,
    3.0,
    4.0,
    5.0,
    6.0,
    7.0,
    8.0,
    9.0,
    10.0,
    11.0,
    12.0,
    13.0,
    14.0,
    15.0};
std::vector<double> v16 = {
    1.0,
    2.0,
    3.0,
    4.0,
    5.0,
    6.0,
    7.0,
    8.0,
    9.0,
    10.0,
    11.0,
    12.0,
    13.0,
    14.0,
    15.0,
    16.0};

// Simple test that a chunk_view can be constructed
TEST_CASE("chunk_view: Constructor", "[chunk_view]") {
  [[maybe_unused]] auto c = _cpo::chunk(v10, 2);
  [[maybe_unused]] auto d = _cpo::chunk(v10, 3);
  [[maybe_unused]] auto e = _cpo::chunk(v11, 2);
  [[maybe_unused]] auto f = _cpo::chunk(v11, 3);
}

TEST_CASE("chunk_view: Constructor + size", "[chunk_view]") {
  auto c = _cpo::chunk(v10, 2);
  CHECK(c.size() == 5);
  auto d = _cpo::chunk(v10, 3);
  CHECK(d.size() == 4);
  auto e = _cpo::chunk(v11, 2);  // 2, 2, 2, 2, 2, 1
  CHECK(e.size() == 6);
  auto f = _cpo::chunk(v11, 3);  // 3, 3, 3, 2
  CHECK(f.size() == 4);
  auto g = _cpo::chunk(v12, 2);
  CHECK(g.size() == 6);
  auto h = _cpo::chunk(v12, 3);
  CHECK(h.size() == 4);
  auto i = _cpo::chunk(v12, 4);
  CHECK(i.size() == 3);
}

TEST_CASE("chunk_view: Iterators begin and end", "[chunk_view]") {
  for (auto&& v : {v10, v11, v12, v13, v14, v15, v16}) {
    auto a = _cpo::chunk(v, 2);
    auto b = a.begin();
    auto c = a.end();
    auto d = a.begin();
    auto e = a.end();
    CHECK(b != c);
    CHECK(d != e);
    CHECK(b == d);
    CHECK(c == e);
  }

  for (auto&& v : {v11, v12}) {
    auto a = _cpo::chunk(v, 2);
    auto b = a.begin();
    CHECK(b == a.begin());
    CHECK(b != a.end());

    ++b;
    CHECK(b != a.begin());
    CHECK(b != a.end());

    ++b;
    CHECK(b != a.begin());
    CHECK(b != a.end());

    ++b;
    CHECK(b != a.begin());
    CHECK(b != a.end());

    ++b;
    CHECK(b != a.begin());
    CHECK(b != a.end());

    ++b;
    CHECK(b != a.begin());
    CHECK(b != a.end());

    ++b;
    CHECK(b != a.begin());
    CHECK(b == a.end());
  }
}

TEST_CASE("chunk_view: Iterators ++ and --", "[chunk_view]") {
  for (auto&& v : {v10, v11, v12, v13, v14, v15, v16}) {
    auto a = _cpo::chunk(v, 2);
    auto b = a.begin();
    [[maybe_unused]] auto c = a.end();
    auto d = a.begin();
    auto e = a.end();
    while (b != e) {
      ++b;
      d++;
      CHECK(b == d);
    }
  }

  for (auto&& v : {v10, v11, v12, v13, v14, v15, v16}) {
    auto a = _cpo::chunk(v, 2);
    auto b = a.begin();
    [[maybe_unused]] auto c = a.end();
    auto d = a.begin();
    auto e = a.end();
    while (b != e) {
      ++b;
      --b;
      b++;
      d++;
      d--;
      ++d;
      CHECK(b == d);
    }
  }
}

TEST_CASE("chunk_view: Iterators + 1", "[chunk_view]") {
  for (auto ch : {2, 3, 4, 9, 10, 11}) {
    for (auto&& v : {v10, v11, v12, v13, v14, v15, v16}) {
      auto a = _cpo::chunk(v, ch);
      auto b = a.begin();
      [[maybe_unused]] auto c = a.end();
      auto d = a.begin();
      auto e = a.end();

      while (b != e) {
        auto x = b;
        b += 1;
        if (b == e) {
          break;
        }
        CHECK(b != d);
        CHECK(b == d + 1);
        CHECK(b != d + 2);
        d++;
        CHECK(b == d);
        CHECK(b == (x + 1));
      }
    }
  }
}

TEST_CASE("chunk_view: Iterators - 1 - 1", "[chunk_view]") {
  auto a = _cpo::chunk(v11, 3);
  auto b = a.begin();
  auto c = b;
  auto d = a.begin();
  auto e = a.end();

  CHECK(b == c);
  CHECK(c == b);
  CHECK(b == d);
  CHECK(d == b);
  CHECK(c == d);
  CHECK(c == c);
  CHECK(d == c);
  CHECK(c == a.begin());
  CHECK(b != e);
  CHECK(b == c);
  CHECK(c == b);

  auto x = b;
  ++b;
  auto y = b;

  CHECK(c != b);
  CHECK(c == (b - 1));
  CHECK(b == (c + 1));
  CHECK(c == x);
  CHECK(c == y - 1);
  CHECK(y == (c + 1));
  CHECK(y == ((b - 1) + 1));

  --b;
  x = b;

  CHECK(c == x);
  CHECK(x == b);
  CHECK(c == b);
  CHECK(b == c);
  CHECK(b == a.begin());
}

TEST_CASE("chunk_view: Iterators - 1", "[chunk_view]") {
  for (auto ch : {2, 3, 4, 9, 10, 11}) {
    for (auto&& vx : {v10, v11, v12, v13, v14, v15, v16}) {
      auto a = _cpo::chunk(vx, ch);
      auto b = a.begin();
      [[maybe_unused]] auto c = a.end();
      auto d = a.begin();
      auto e = a.end();

      {
        auto dh = 9;
        auto a = _cpo::chunk(vx, dh);
        auto bb = a.begin();
        auto dd = a.begin();
        CHECK(bb == dd);

        ++bb;

        if (bb == e) {
          break;
        }
        --bb;
        if (!(bb == a.begin())) {
          [[maybe_unused]] auto asdf = 1;  // to allow debugging breakpoint
        }
        CHECK(bb == a.begin());
      }

      while (b != e) {
        CHECK(b == d);

        ++b;
        if (b == e) {
          break;
        }
        --b;

        // CHECK(b == d);
        b++;
        d++;
        CHECK(b == d);
      }
    }
  }
}

TEST_CASE("chunk_view: Iterators", "[chunk_view]") {
  for (auto&& v : {v10, v11, v12, v13, v14, v15, v16}) {
    for (size_t i = 1; i <= v.size(); ++i) {
      auto a = _cpo::chunk(v, (long)i);
      auto b = a.begin();
      CHECK(b->size() == (unsigned)i);
    }
  }
}

TEST_CASE("chunk_view: Iterators values", "[chunk_view]") {
  auto a = _cpo::chunk(v10, 5);
  auto b = a.begin();

  CHECK(b->size() == 5);
  CHECK((*b)[0] == 1.0);
  CHECK((*b)[1] == 2.0);
  CHECK((*b)[2] == 3.0);
  CHECK((*b)[3] == 4.0);
  CHECK((*b)[4] == 5.0);
  ++b;
  CHECK(b->size() == 5);
  CHECK((*b)[0] == 6.0);
  CHECK((*b)[1] == 7.0);
  CHECK((*b)[2] == 8.0);
  CHECK((*b)[3] == 9.0);
  CHECK((*b)[4] == 10.0);
  ++b;
  CHECK(b == a.end());
}

TEST_CASE("chunk_view: Larger vector", "[chunk_view]") {
  size_t num_elements = 8 * 1024;
  size_t chunk_size = 128;
  size_t num_chunks = num_elements / chunk_size;

  // Don't worry about boundary cases for now
  REQUIRE(num_elements % num_chunks == 0);

  std::vector<int> base_17(num_elements);
  std::iota(begin(base_17), end(base_17), 0);

  REQUIRE(size(base_17) == num_elements);

  REQUIRE(
      std::ranges::equal(base_17, std::vector<int>(num_elements, 0)) == false);

  auto a = _cpo::chunk(base_17, (long)chunk_size);

  SECTION("Verify base chunk view") {
    for (size_t i = 0; i < num_chunks; ++i) {
      auto current_chunk = a[i];
      for (size_t j = 0; j < chunk_size; ++j) {
        CHECK(current_chunk[j] == base_17[i * chunk_size + j]);
      }
    }
  }

  SECTION("Verify base chunk view") {
    for (size_t i = 0; i < num_chunks; ++i) {
      auto current_chunk = a[i];
      for (size_t j = 0; j < chunk_size; ++j) {
        CHECK(current_chunk[j] == base_17[i * chunk_size + j]);
        current_chunk[j] = 0;
        CHECK(current_chunk[j] == 0);
      }
    }
    CHECK(std::ranges::equal(base_17, std::vector<int>(num_elements, 0)));
  }
}
