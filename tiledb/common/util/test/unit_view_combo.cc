/**
 * @file   unit_zip_view.cc
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

#include <tiledb/common/util/chunk_view.h>
#include <tiledb/common/util/var_length_view.h>
#include <tiledb/common/util/alt_var_length_view.h>
#include <tiledb/common/util/zip_view.h>
#include <tiledb/common/util/permutation_view.h>

// The null test, just to make sure none of the include files are broken
TEST_CASE("view combo: Null test", "[view_combo][null_test]") {
  REQUIRE(true);
}

TEST_CASE("view combo: chunk a chunk view", "[view_combo]") {
  size_t num_elements = 32*1024;
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
  REQUIRE(4 * chunk_size * chunk_chunk_size < num_elements); // At least four outer chunks

  std::vector<int> base_17(num_elements);
  std::vector<int> base_m31(num_elements);
  std::iota(begin(base_17), end(base_17), 17);
  std::iota(begin(base_m31), end(base_m31), -31);

  REQUIRE(size(base_17) == num_elements);
  REQUIRE(std::ranges::equal(base_17, base_m31) == false);
  REQUIRE(
      std::ranges::equal(base_17, std::vector<int>(num_elements, 0)) == false);

  auto a = _cpo::chunk(base_17, (long) chunk_size);

  SECTION("Verify base chunk view") {
    for (size_t i = 0; i < num_chunks; ++i) {
      auto chunk = a[i];
      for (size_t j = 0; j < chunk_size; ++j) {
        CHECK(chunk[j] == base_17[i * chunk_size + j]);
      }
    }
  }

  SECTION("Verify chunked chunk view") {
    auto b = _cpo::chunk(a, (long) chunk_chunk_size);
        for (size_t i = 0; i < num_chunk_chunks; ++i) {
          auto chunk = b[i];
          for (size_t j = 0; j < chunk_chunk_size; ++j) {
                auto inner_chunk = chunk[j];
                for (size_t k = 0; k < chunk_size; ++k) {
                  CHECK(inner_chunk[k] == base_17[i * chunk_chunk_size * chunk_size + j * chunk_size + k]);
                }
          }
        }
  }
}

TEST_CASE("view combo: chunk a var_length view", "[view_combo]") {
}

TEST_CASE("view combo: chunk an alt_var_length view", "[view_combo]") {
}

TEST_CASE("view combo: chunk a permutation view", "[view_combo]") {
}

TEST_CASE("view combo: permute a chunk view", "[view_combo]") {
}

TEST_CASE("view combo: chunk a zip view", "[view_combo]") {
}

TEST_CASE("view combo: zip a chunk view", "[view_combo]") {
}

TEST_CASE("view combo: chunk a zipped chunk view", "[view_combo]") {
}

TEST_CASE("view combo: zip a chunked chunk view", "[view_combo]") {
}

TEST_CASE("view combo: zip a chunked zipped chunk view", "[view_combo]") {
}

TEST_CASE("view combo: num_elements % num_chunks != 0", "[view_combo]") {
}


