/**
 * @file   tiledb/common/util/var_length_util.h
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
 * This file contains utilities for working with var length data.  Notably
 *   - conversion routing between a vector of offsets and a vector of lengths
 *
 * From lengths to offsets is an inclusive_scan:
 *   std::inclusive_scan(begin(bar), end(bar), begin(baz)+1);
 *
 * From offsets to lengths is an adjacent_difference:
 *   std::adjacent_difference(begin(baz)+1, end(baz), begin(bat));
 *
 * Note that we have to special case the arrow offset format vs the tiledb
 * offset format.
 */

#ifndef TILEDB_VAR_LENGTH_UTIL_H
#define TILEDB_VAR_LENGTH_UTIL_H

#include <algorithm>
#include <numeric>
#include <ranges>

/**
 * @brief Convert a vector of lengths to a vector of offsets, in place.
 * Arrow format.  In the arrow format, the length of the offsets array is
 * one more than the length of the length array
 * the first element is always 0, and the
 * last element is the total length of the data.
 * @tparam R
 * @tparam S
 * @param offsets
 * @param lengths
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range S>
void lengths_to_offsets(const R& lengths, S& offsets) {
  if (std::size(offsets) == std::size(lengths) + 1) {
    std::inclusive_scan(
        std::begin(lengths), std::end(lengths), std::begin(offsets) + 1);
  } else if (std::size(offsets) == std::size(lengths)) {
    std::inclusive_scan(
        std::begin(lengths), std::end(lengths) - 1, std::begin(offsets) + 1);
  } else {
    throw std::runtime_error("Invalid lengths and offsets sizes");
  }
}

/**
 * @brief Convert a vector of offsets to a vector of lengths, in place.
 * Arrow format.  In the arrow format, the length of the offsets array is
 * one more than the length of the length array
 * the first element is always 0, and the
 * last element is the total length of the data.
 * @tparam R
 * @tparam S
 * @param offsets
 * @param lengths
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range S>
void offsets_to_lengths(const R& offsets, S& lengths) {
  assert(std::size(offsets) == std::size(lengths) + 1);
  std::adjacent_difference(
      std::begin(offsets) + 1, std::end(offsets), std::begin(lengths));
}

/**
 * @brief Convert a vector of offsets to a vector of lengths, in place.
 * Arrow format.  In the arrow format, the length of the offsets array is
 * one more than the length of the length array
 * the first element is always 0, and the
 * last element is the total length of the data.
 * @tparam R
 * @tparam S
 * @param offsets
 * @param lengths
 * @param total_length aka the final offset
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range S>
void offsets_to_lengths(const R& offsets, S& lengths, size_t total_length) {
  assert(std::size(offsets) == std::size(lengths));
  std::adjacent_difference(
      std::begin(offsets) + 1, std::end(offsets), std::begin(lengths));
  lengths.back() =
      static_cast<decltype(offsets.back())>(total_length) - offsets.back();
}

#endif  // TILEDB_VAR_LENGTH_UTIL_H
