/**
 * @file
 * /Users/robertbindar/TileDB/TileDB/tiledb/common/floating_point_summation.h
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
 */

#ifndef TILEDB_COMMON_FLOATING_POINT_SUMMATION_H
#define TILEDB_COMMON_FLOATING_POINT_SUMMATION_H

#include <span>

namespace tiledb::common::floating_point_summation {

/* Change this to modify at what span size the pairwise algorithm
   switches from recursion to naive iterative sum */
static constexpr size_t PairwiseBaseSize = 128;

/**
 * Interface for various floating point summation algorithms
 *
 * @param x a span of elements to be summed
 * @return the sum over the span of elements
 */
template <class T>
requires std::floating_point<T>
class SummationAlgorithm {
  virtual T operator()(const std::span<T>& x) = 0;
};

/**
 * Functor class that implements pairwise summation.
 * Pairwise summation adds the elements of the span recursively
 * until the span becomes sufficiently small. This threshold can be configured
 * using `PairwiseBaseSize` variable.
 */
template <class T>
requires std::floating_point<T>
class PairwiseSum : SummationAlgorithm<T> {
 public:
  virtual T operator()(const std::span<T>& x) {
    // Sanity checks
    static_assert(PairwiseBaseSize > 0);

    if (x.size() <= PairwiseBaseSize) {
      T sum{0};
      for (std::size_t i = 0; i < x.size(); ++i) {
        sum += x[i];
      }
      return sum;
    }
    size_t mid = std::floor(x.size() / 2);
    return operator()(x.first(mid)) + operator()(x.subspan(mid));
  }
};

/**
 * Functor class that implements naive summation over a span.
 * Used for now in tests and benchmarks.
 */
template <class T>
requires std::floating_point<T>
class NaiveSum : SummationAlgorithm<T> {
 public:
  virtual T operator()(const std::span<T>& x) {
    T sum = 0;
    for (size_t i = 0; i < x.size(); ++i) {
      sum += x[i];
    }
    return sum;
  }
};

}  // namespace tiledb::common::floating_point_summation
#endif  // TILEDB_COMMON_FLOATING_POINT_SUMMATION_H
