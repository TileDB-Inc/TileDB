/**
 * @file   unit_block_view.cc
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

#include <catch2/catch_all.hpp>
#include "tiledb/common/util/chunk_view.h"

TEST_CASE("chunk_view: null test", "[chunk_view][null test]") {
  REQUIRE(true);
}

// Test that the chunk_view satisfies the expected concepts
TEST_CASE("chunk_view: Range concepts", "[chunk_view][concepts]") {
  using test_type = chunk_view<std::vector<double>>;

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
  using test_type = chunk_view<std::vector<double>>;
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
TEST_CASE(
    "chunk_view: value_type concepts", "[chunk_view][concepts]") {
  using test_type = chunk_view<std::vector<double>>;
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
