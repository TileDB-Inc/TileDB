/**
 * @file   unit_view_combo.cc
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
 * This file implements tests to exercise combinations of different views, most
 * notably, combinations of various views with `chunk_view`
 */

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <concepts>
#include <numeric>
#include <ranges>
#include <vector>

#include <tiledb/common/util/alt_var_length_view.h>
#include <tiledb/common/util/permutation_view.h>
#include <tiledb/common/util/var_length_view.h>
#include <tiledb/stdx/__ranges/chunk_view.h>
#include <tiledb/stdx/__ranges/zip_view.h>

#include <tiledb/common/util/print_types.h>

// The null test, just to make sure none of the include files are broken
TEST_CASE("view combo: Null test", "[view_combo][null_test]") {
  REQUIRE(true);
}

TEST_CASE("view combo: chunk a chunk view", "[view_combo]") {
  size_t num_elements = 32 * 1024;
  size_t chunk_size = 128;
  size_t chunk_chunk_size = 8;
  size_t num_chunks = num_elements / chunk_size;
  size_t num_chunk_chunks = num_elements / (chunk_size * chunk_chunk_size);

  // Make sure we don't have any constructive / destructive interference
  REQUIRE(chunk_size != chunk_chunk_size);
  REQUIRE(num_chunks != chunk_size);
  REQUIRE(num_chunk_chunks != chunk_size);
  REQUIRE(num_chunks != num_chunk_chunks);

  // Don't worry abount boundary cases for now
  REQUIRE(num_elements % chunk_size == 0);
  REQUIRE(num_elements % (chunk_chunk_size * chunk_size) == 0);
  REQUIRE(
      4 * chunk_size * chunk_chunk_size <
      num_elements);  // At least four outer chunks

  std::vector<int> base_17(num_elements);
  std::vector<int> base_m31(num_elements);
  std::iota(begin(base_17), end(base_17), 17);
  std::iota(begin(base_m31), end(base_m31), -31);

  REQUIRE(size(base_17) == num_elements);
  REQUIRE(std::ranges::equal(base_17, base_m31) == false);
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

  SECTION("Verify chunked chunk view") {
    auto b = _cpo::chunk(a, (long)chunk_chunk_size);
    for (size_t i = 0; i < num_chunk_chunks; ++i) {
      auto current_chunk = b[i];
      for (size_t j = 0; j < chunk_chunk_size; ++j) {
        auto inner_chunk = current_chunk[j];
        for (size_t k = 0; k < chunk_size; ++k) {
          CHECK(
              inner_chunk[k] ==
              base_17[i * chunk_chunk_size * chunk_size + j * chunk_size + k]);
        }
      }
    }
  }
}

std::vector<double> qq = {
    21.0, 20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0, 13.0, 12.0};
std::vector<double> rr = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
std::vector<size_t> pp = {0, 2, 7, 10};
std::vector<size_t> nn = {0, 3, 6, 10};
std::vector<size_t> oo = {2, 0, 1};
std::vector<size_t> mm = {1, 0};
std::vector<size_t> ll = {0, 1};

struct subrange_equal {
  template <class R, class S>
  bool operator()(R&& a, S&& b) {
    if (size(a) != size(b)) {
      return false;
    }
    for (size_t i = 0; i < size(a); ++i) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  }
};

TEST_CASE(
    "view combo: chunk var_length_view and alt_var_length view",
    "[view_combo]") {
  auto check_chunk_view = [](auto& a) {
    CHECK(a[0][0][0] == 21.0);
    CHECK(a[0][0][1] == 20.0);
    CHECK(a[0][1][0] == 19.0);
    CHECK(a[0][1][1] == 18.0);
    CHECK(a[0][1][2] == 17.0);
    CHECK(a[0][1][3] == 16.0);
    CHECK(a[0][1][4] == 15.0);
    CHECK(a[1][0][0] == 14.0);
    CHECK(a[1][0][1] == 13.0);
    CHECK(a[1][0][2] == 12.0);

    CHECK(std::ranges::size(a) == 2);
    CHECK(std::ranges::size(a[0]) == 2);
    CHECK(std::ranges::size(a[1]) == 1);
    CHECK(std::ranges::size(a[0][0]) == 2);
    CHECK(std::ranges::size(a[0][1]) == 5);
    CHECK(std::ranges::size(a[1][0]) == 3);

    CHECK(std::ranges::equal(a[0][0], std::vector<double>{21.0, 20.0}));
    CHECK(std::ranges::equal(
        a[0][1], std::vector<double>{19.0, 18.0, 17.0, 16.0, 15.0}));
    CHECK(std::ranges::equal(a[1][0], std::vector<double>{14.0, 13.0, 12.0}));

    CHECK(std::ranges::equal(
        a[0],
        std::vector<std::vector<double>>{
            {21.0, 20.0},
            {19.0, 18.0, 17.0, 16.0, 15.0},
        },
        subrange_equal{}));

    CHECK(std::ranges::equal(
        a[1],
        std::vector<std::vector<double>>{
            {14.0, 13.0, 12.0},
        },
        subrange_equal{}));
  };

  auto u = var_length_view(qq, pp);
  auto a = stdx::ranges::chunk_view(u, 2);
  check_chunk_view(a);

  auto v = alt_var_length_view(qq, pp);
  auto b = stdx::ranges::chunk_view(v, 2);
  check_chunk_view(b);
}

TEST_CASE("view combo: chunk a permutation view", "[view_combo]") {
  /* See unit_permutation_view.cc for validation of the permutation */
  auto w = var_length_view(qq, pp);
  auto x = permutation_view(w, oo);

  auto a = stdx::ranges::chunk_view(x, 2);
  CHECK(std::ranges::equal(
      a[0],
      std::vector<std::vector<double>>{
          {14.0, 13.0, 12.0},
          {21.0, 20.0},
      },
      subrange_equal{}));

  CHECK(std::ranges::equal(
      a[1],
      std::vector<std::vector<double>>{
          {19.0, 18.0, 17.0, 16.0, 15.0},
      },
      subrange_equal{}));
}

TEST_CASE("view combo: permute a chunk view", "[view_combo]") {
  auto w = alt_var_length_view(qq, pp);
  auto x = stdx::ranges::chunk_view(w, 2);
  auto a = permutation_view(x, ll);

  CHECK(std::ranges::equal(
      a[0],
      std::vector<std::vector<double>>{
          {21.0, 20.0},
          {19.0, 18.0, 17.0, 16.0, 15.0},
      },
      subrange_equal{}));

  CHECK(std::ranges::equal(
      a[1],
      std::vector<std::vector<double>>{
          {14.0, 13.0, 12.0},
      },
      subrange_equal{}));

  auto b = permutation_view(x, mm);

  CHECK(std::ranges::equal(
      b[0],
      std::vector<std::vector<double>>{
          {14.0, 13.0, 12.0},
      },
      subrange_equal{}));

  CHECK(std::ranges::equal(
      b[1],
      std::vector<std::vector<double>>{
          {21.0, 20.0},
          {19.0, 18.0, 17.0, 16.0, 15.0},
      },
      subrange_equal{}));
}

// #define print_types

#include <tiledb/common/util/print_types.h>
TEST_CASE("view combo: chunk a zip view", "[view_combo]") {
  auto z = zip(qq, rr, rr);

  // Verify what zip_view returns
  CHECK(std::get<0>(z[0]) == 21.0);
  CHECK(std::get<0>(z[1]) == 20.0);
  CHECK(std::get<0>(z[2]) == 19.0);
  CHECK(std::get<0>(z[3]) == 18.0);
  CHECK(std::get<1>(z[0]) == 1.0);
  CHECK(std::get<1>(z[1]) == 2.0);
  CHECK(std::get<1>(z[2]) == 3.0);
  CHECK(std::get<1>(*(z.begin() + 3)) == 4.0);

  CHECK(std::ranges::equal(
      z,
      std::vector<std::tuple<double, double, double>>{
          {21.0, 1.0, 1.0},
          {20.0, 2.0, 2.0},
          {19.0, 3.0, 3.0},
          {18.0, 4.0, 4.0},
          {17.0, 5.0, 5.0},
          {16.0, 6.0, 6.0},
          {15.0, 7.0, 7.0},
          {14.0, 8.0, 8.0},
          {13.0, 9.0, 9.0},
          {12.0, 10.0, 10.0},
      }));

  auto a = stdx::ranges::chunk_view(z, 2);
  CHECK(std::ranges::size(a) == 5);
  CHECK(std::ranges::size(a[0]) == 2);

  CHECK(std::get<0>(a[0][0]) == 21.0);
  CHECK(std::get<0>(a[0][1]) == 20.0);
  CHECK(std::get<1>(a[0][0]) == 1.0);
  CHECK(std::get<1>(a[0][1]) == 2.0);
  CHECK(std::get<0>(a[1][0]) == 19.0);
  CHECK(std::get<0>(a[1][1]) == 18.0);
  CHECK(std::get<1>(a[1][0]) == 3.0);
  CHECK(std::get<1>(a[1][1]) == 4.0);

  auto&& [b, c] = a[0];
  auto&& [d, e] = a[1];
  (void)d;
  (void)e;

  auto&& [f, g, n] = b[0];
  auto&& [h, i, o] = b[1];
  auto&& [j, k, p] = c[0];
  auto&& [l, m, q] = c[1];

  CHECK(std::get<0>(b[0]) == 21.0);
  CHECK(std::get<1>(b[0]) == 1.0);
  CHECK(std::get<2>(b[0]) == 1.0);
  CHECK(std::get<0>(b[1]) == 20.0);
  CHECK(std::get<1>(b[1]) == 2.0);
  CHECK(std::get<2>(b[1]) == 2.0);
  CHECK(std::get<0>(b[1]) == 20.0);
  CHECK(std::get<1>(b[1]) == 2.0);
  CHECK(std::get<1>(b[1]) == 2.0);
  CHECK(std::get<0>(c[0]) == 19.0);
  CHECK(std::get<1>(c[0]) == 3.0);
  CHECK(std::get<0>(c[1]) == 18.0);
  CHECK(std::get<1>(c[1]) == 4.0);
  CHECK(f == 21.0);
  CHECK(h == 20.0);
  CHECK(j == 19.0);
  CHECK(g == 1.0);
  CHECK(q == 4.0);

  CHECK(b[0] == std::tuple{21.0, 1.0, 1.0});
  CHECK(b[1] == std::tuple{20.0, 2.0, 2.0});
  CHECK(c[0] == std::tuple{19.0, 3.0, 3.0});
  CHECK(c[1] == std::tuple{18.0, 4.0, 4.0});
  CHECK(c[2] == std::tuple{17.0, 5.0, 5.0});

  // a[0] is the same as the first two elements of z
  CHECK(std::ranges::equal(
      a[0],
      std::vector<std::tuple<double, double, double>>{
          {21.0, 1.0, 1.0},
          {20.0, 2.0, 2.0},
      }));

  CHECK(std::equal(b, b + 1, std::begin(z)));
  CHECK(std::equal(c, c + 1, std::begin(z) + 2));
}

TEST_CASE("view combo: zip a chunk view", "[view_combo]") {
  auto c = chunk(qq, 2);
  auto d = chunk(rr, 3);

  auto z = zip(c, d, d);
  CHECK(size(z) == 4);  // 3, 3, 3, 1

  CHECK(std::ranges::equal(std::get<0>(z[0]), std::vector<double>{21.0, 20.0}));
  CHECK(std::ranges::equal(
      std::get<1>(z[0]), std::vector<double>{1.0, 2.0, 3.0}));
}
