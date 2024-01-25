/**
 * @file   safe_sum.h
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
 * This file defines class SafeSum.
 */

#ifndef TILEDB_SAFE_SUM_H
#define TILEDB_SAFE_SUM_H

#include <atomic>
#include <stdexcept>

namespace tiledb::sm {

struct SafeSum {
 public:
  /**
   * Sum function that prevent wrap arounds on overflow.
   *
   * @param value Value to add to the sum.
   * @param sum Computed sum.
   */
  template <typename SUM_T>
  void op(SUM_T value, SUM_T& sum, uint64_t);

  /**
   * Sum function for atomics that prevent wrap arounds on overflow.
   *
   * @param value Value to add to the sum.
   * @param sum Computed sum.
   */
  template <typename SUM_T>
  void safe_sum(SUM_T value, std::atomic<SUM_T>& sum) {
    // Start by saving the current sum value from the atomic to operate on in
    // 'cur_sum'. Then compute the new sum in 'new_sum'.
    // std::atomic_compare_exchange_weak will only update the value and return
    // true if the value hasn't changed since we saved it in 'cur_sum'.
    SUM_T cur_sum;
    SUM_T new_sum;
    do {
      new_sum = cur_sum = sum;
      op(value, new_sum, true);
    } while (!std::atomic_compare_exchange_weak(&sum, &cur_sum, new_sum));
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_SAFE_SUM_H
