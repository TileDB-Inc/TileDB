/**
 * @file safe_sum.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file implements class SafeSum.
 */

#include "tiledb/sm/query/readers/aggregators/safe_sum.h"

#include <limits>

namespace tiledb::sm {

/** Specialization of op for int64_t sums. */
template <>
void SafeSum::op<int64_t>(int64_t value, int64_t& sum, uint64_t) {
  if (sum > 0 && value > 0 &&
      (sum > (std::numeric_limits<int64_t>::max() - value))) {
    throw std::overflow_error("overflow on sum");
  }

  if (sum < 0 && value < 0 &&
      (sum < (std::numeric_limits<int64_t>::min() - value))) {
    throw std::overflow_error("overflow on sum");
  }

  sum += value;
}

/** Specialization of op for uint64_t sums. */
template <>
void SafeSum::op<uint64_t>(uint64_t value, uint64_t& sum, uint64_t) {
  if (sum > (std::numeric_limits<uint64_t>::max() - value)) {
    throw std::overflow_error("overflow on sum");
  }

  sum += value;
}

/** Specialization of op for double sums. */
template <>
void SafeSum::op<double>(double value, double& sum, uint64_t) {
  if ((sum < 0.0) == (value < 0.0) &&
      (std::abs(sum) >
       (std::numeric_limits<double>::max() - std::abs(value)))) {
    throw std::overflow_error("overflow on sum");
  }

  sum += value;
}

}  // namespace tiledb::sm
