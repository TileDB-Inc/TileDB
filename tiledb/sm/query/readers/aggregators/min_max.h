/**
 * @file   min_max.h
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
 * This file defines class MinMax.
 */

#ifndef TILEDB_MIN_MAX_H
#define TILEDB_MIN_MAX_H

namespace tiledb::sm {

template <class Op>
struct MinMax {
 public:
  /**
   * Min max function.
   *
   * @param value Value to compare against.
   * @param sum Computed min/max.
   * @param count Current count of values.
   * @param
   */
  template <typename MIN_MAX_T>
  void op(MIN_MAX_T value, MIN_MAX_T& min_max, uint64_t count) {
    if (count == 0) {
      min_max = value;
    } else if (op_(value, min_max)) {
      min_max = value;
    }
  }

 private:
  Op op_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_MIN_MAX_H
