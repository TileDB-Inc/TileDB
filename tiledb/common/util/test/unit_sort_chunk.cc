/**
 * @file   tiledb/common/util/test/unit_sort_chunk.cc
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
 * This file implements tests for sorting chunks in a chunk_view over a vector.
 */

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <numeric>
#include <ranges>
#include <vector>
#include "oberon.h"
#include "tiledb/common/util/alt_var_length_view.h"
#include "tiledb/stdx/ranges"

#include "tiledb/common/util/print_types.h"

TEST_CASE("sort_chunk: Null test", "[sort_chunk][null_test]") {
}

/*
 * Test that sorting each chunk in a chunk view will sort blocks of elements in
 * the underlying vector.
 */
TEST_CASE("sort_chunk: std::vector", "[sort_chunk]") {
  size_t N = 1024;
  size_t chunk_size = 16;
  size_t num_chunks = N / chunk_size;

  std::vector<int> v(N);
  std::iota(v.begin(), v.end(), 17);
  std::ranges::reverse(v);
  std::vector<int> expected(v);

  /* Sort blocks of underlying vector */
  for (size_t i = 0; i < num_chunks; ++i) {
    for (size_t j = 0; j < chunk_size; ++j) {
      std::sort(
          expected.begin() + i * chunk_size,
          expected.begin() + (i + 1) * chunk_size);
    }
  }

  /* Create chunk view of v */
  auto cv = stdx::views::chunk(v, (long)chunk_size);
  CHECK(std::ranges::size(cv) == (long)num_chunks);

  /* Sort each chunk */
  for (auto&& cu : cv) {
    std::sort(cu.begin(), cu.end());
  }

  CHECK(std::ranges::equal(v, expected));
}

TEST_CASE("sort_chunk: zip_view of std::vector", "sort_chunk") {
  size_t N = 1024;
  size_t chunk_size = 16;
  size_t num_chunks = N / chunk_size;

  std::vector<int> v(N), w(N + 5);
  std::iota(v.begin(), v.end(), 17);
  std::iota(w.begin(), w.end(), -13);

  auto z = stdx::views::zip(v, w);

  std::ranges::reverse(v);
  std::ranges::reverse(w);
  std::vector<int> xv(v), xw(w);

  /* Sort blocks of underlying vectors */
  for (size_t i = 0; i < num_chunks; ++i) {
    for (size_t j = 0; j < chunk_size; ++j) {
      std::sort(xv.begin() + i * chunk_size, xv.begin() + (i + 1) * chunk_size);
      std::sort(xw.begin() + i * chunk_size, xw.begin() + (i + 1) * chunk_size);
    }
  }

  /* Create chunk view of v */
  auto cz = stdx::ranges::chunk_view(z, chunk_size);
  CHECK(std::ranges::size(cz) == (long)num_chunks);

  /* Sort each chunk */
  for (auto&& cy : cz) {
    std::sort(cy.begin(), cy.end());
  }

  CHECK(std::ranges::equal(z, stdx::views::zip(xv, xw)));
  CHECK(!std::ranges::is_sorted(z));
  CHECK(!std::ranges::is_sorted(xv));
  CHECK(!std::ranges::is_sorted(xw));
}

TEST_CASE("sort_chunk: zip_view of vector and alt_var_length_view") {
  auto ob1 = alt_var_length_view(oberon::cs1, oberon::ps1);
  REQUIRE(size(ob1) == 10);
  std::vector<int> v(10);
  std::iota(v.begin(), v.end(), 17);
  std::ranges::reverse(v);

  auto z = stdx::views::zip(v, ob1);
  auto cz = stdx::views::chunk(z, 2);
  CHECK(size(cz) == 5);
  CHECK(size(cz[0]) == 2);

  for (auto&& cy : cz) {
    std::sort(cy.begin(), cy.end(), [](auto&& a, auto&& b) {
      return std::get<0>(a) < std::get<0>(b);
    });
  }

  // 26, 25, 24, 23, 22, 21, 20, 19, 18, 17
  // -> 25, 26, 23, 24, 21, 22, 19, 20, 17, 18
  // Where ox lips and the nod ding vio let grows
  // -> ox Where and lips nod the vio ding grows let
  CHECK(std::get<0>(cz[0][0]) == 25);
  CHECK(std::get<0>(cz[0][1]) == 26);
  CHECK(std::ranges::equal(get<1>(cz[0][0]), std::vector<char>{'o', 'x'}));
  CHECK(std::ranges::equal(
      get<1>(cz[0][1]), std::vector<char>{'W', 'h', 'e', 'r', 'e'}));
  CHECK(std::ranges::equal(get<1>(cz[1][0]), std::vector<char>{'a', 'n', 'd'}));
  CHECK(std::ranges::equal(
      get<1>(cz[1][1]), std::vector<char>{'l', 'i', 'p', 's'}));
  CHECK(std::ranges::equal(get<1>(cz[2][0]), std::vector<char>{'n', 'o', 'd'}));
  CHECK(std::ranges::equal(get<1>(cz[2][1]), std::vector<char>{'t', 'h', 'e'}));
  CHECK(std::ranges::equal(get<1>(cz[3][0]), std::vector<char>{'v', 'i', 'o'}));
  CHECK(std::ranges::equal(
      get<1>(cz[3][1]), std::vector<char>{'d', 'i', 'n', 'g'}));
  CHECK(std::ranges::equal(
      get<1>(cz[4][0]), std::vector<char>{'g', 'r', 'o', 'w', 's'}));
  CHECK(std::ranges::equal(get<1>(cz[4][1]), std::vector<char>{'l', 'e', 't'}));
}
