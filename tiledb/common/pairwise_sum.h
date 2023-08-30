/**
 * @file /Users/robertbindar/TileDB/TileDB/tiledb/common/pairwise_sum.h
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

#ifndef TILEDB_COMMON_PAIRWISE_SUM_H
#define TILEDB_COMMON_PAIRWISE_SUM_H

#include <span>

namespace tiledb::common {

template <class T, std::size_t Base = 128>
requires std::floating_point<T> T pairwise_sum(const std::span<T>& x) {
  // Sanity checks
  static_assert(Base > 0);

  if (x.size() <= Base) {
    T sum{0};
    for (std::size_t i = 0; i < x.size(); ++i) {
      sum += x[i];
    }
    return sum;
  }
  size_t mid = std::floor(x.size() / 2);
  return pairwise_sum<T, Base>(x.first(mid)) +
         pairwise_sum<T, Base>(x.subspan(mid));
}

}  // namespace tiledb::common
#endif  // TILEDB_COMMON_PAIRWISE_SUM_H
