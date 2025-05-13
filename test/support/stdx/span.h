/**
 * @file test/support/stdx/tuple.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file defines operators for lexicographically comparing one `std::span`
 * to another.
 *
 * This is not provided by the standard library apparently due to concerns
 * about shallow comparisons.  We don't have those concerns for our test code.
 */

#ifndef TILEDB_TEST_SUPPORT_SPAN_H
#define TILEDB_TEST_SUPPORT_SPAN_H

#include <span>

namespace std {

template <typename T>
static bool lexcmp(std::span<const T> left, std::span<const T> right) {
  return std::lexicographical_compare(
      left.begin(), left.end(), right.begin(), right.end());
}

template <typename T>
bool operator<(std::span<const T> left, std::span<const T> right) {
  return lexcmp(left, right);
}

template <typename T>
bool operator<=(std::span<const T> left, std::span<const T> right) {
  return lexcmp(left, right) || !lexcmp(right, left);
}

template <typename T>
bool operator==(std::span<const T> left, std::span<const T> right) {
  return !lexcmp(left, right) && !lexcmp(right, left);
}

template <typename T>
bool operator>=(std::span<const T> left, std::span<const T> right) {
  return lexcmp(right, left) || !lexcmp(left, right);
}

template <typename T>
bool operator>(std::span<const T> left, std::span<const T> right) {
  return lexcmp(right, left);
}

template <typename T>
bool operator!=(std::span<const T> left, std::span<const T> right) {
  return lexcmp(left, right) || lexcmp(right, left);
}

}  // namespace std

#endif
