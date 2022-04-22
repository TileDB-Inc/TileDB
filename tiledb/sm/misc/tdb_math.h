/**
 * @file tdb_math.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file contains useful (global) functions.
 */

#ifndef TILEDB_UTILS_MATH_H
#define TILEDB_UTILS_MATH_H

#include <cstdint>

namespace tiledb::sm::utils::math {

/** Returns the value of x/y (integer division) rounded up. */
uint64_t ceil(uint64_t x, uint64_t y);

/** Returns log_b(x). */
double log(double b, double x);

/**
 * Computes a * b, but it checks for overflow. In case the product
 * overflows, it returns std::numeric_limits<T>::max().
 */
template <class T>
T safe_mul(T a, T b);

/**
 * Returns the maximum power of 2 minus one that is smaller than
 * or equal to `value`.
 */
uint64_t left_p2_m1(uint64_t value);

/**
 * Returns the minimum power of 2 minus one that is larger than
 * or equal to `value`.
 */
uint64_t right_p2_m1(uint64_t value);

}  // namespace tiledb::sm::utils::math

#endif  // TILEDB_UTILS_MATH_H
