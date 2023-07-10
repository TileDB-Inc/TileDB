/**
 * @file math.cc
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
 * This file implements useful (global) functions.
 */

#include "tdb_math.h"
#include <cmath>
#include <limits>

namespace tiledb::sm::utils::math {

uint64_t ceil(uint64_t x, uint64_t y) {
  if (y == 0)
    return 0;

  return x / y + (x % y != 0);
}

double log(double b, double x) {
  return ::log(x) / ::log(b);
}

template <class T>
T safe_mul(T a, T b) {
  T prod = a * b;

  // Check for overflow only for integers
  if (std::is_integral<T>::value) {
    if (prod / a != b)  // Overflow
      return std::numeric_limits<T>::max();
  }

  return prod;
}

uint64_t left_p2_m1(uint64_t value) {
  // Edge case
  if (value == UINT64_MAX)
    return value;

  uint64_t ret = 0;        // Min power of 2 minus 1
  while (ret <= value) {
    ret = (ret << 1) | 1;  // Next larger power of 2 minus 1
  }

  return ret >> 1;
}

uint64_t right_p2_m1(uint64_t value) {
  // Edge case
  if (value == 0)
    return value;

  uint64_t ret = UINT64_MAX;  // Max power of 2 minus 1
  while (ret >= value) {
    ret >>= 1;                // Next smaller power of 2 minus 1
  }

  return (ret << 1) | 1;
}

template int8_t safe_mul<int8_t>(int8_t a, int8_t b);
template uint8_t safe_mul<uint8_t>(uint8_t a, uint8_t b);
template int16_t safe_mul<int16_t>(int16_t a, int16_t b);
template uint16_t safe_mul<uint16_t>(uint16_t a, uint16_t b);
template int32_t safe_mul<int32_t>(int32_t a, int32_t b);
template uint32_t safe_mul<uint32_t>(uint32_t a, uint32_t b);
template int64_t safe_mul<int64_t>(int64_t a, int64_t b);
template uint64_t safe_mul<uint64_t>(uint64_t a, uint64_t b);
template float safe_mul<float>(float a, float b);
template double safe_mul<double>(double a, double b);

}  // namespace tiledb::sm::utils::math
