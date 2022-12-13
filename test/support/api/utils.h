/**
 * @file utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Implementation of the v3 API
 */

#ifndef TILEDB_API_V3_UTILS_H
#define TILEDB_API_V3_UTILS_H

#include <vector>

namespace tiledb::test::api::v3::utils {

template<typename T>
std::vector<T> range(T min, T max) {
  if (max < min) {
    // Can dimensions be in reverse? Seems weird
    throw std::logic_error("invalid empty range");
  }
  std::vector<T> ret(max - min + static_cast<T>(1));
  std::iota(ret.begin(), ret.end(), min);
  return ret;
}

template<typename T>
std::vector<T> fill(size_t elems, std::function<T ()> generator) {
  std::vector<T> ret(elems);
  for(size_t i = 0; i < elems; i++) {
    ret[i] = generator();
  }
  return ret;
}

} // namespace tiledb::test::api::v3::utils

#endif // TILEDB_API_V3_UTILS_H

