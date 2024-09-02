/**
 * @file stdx_string.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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

#include "stdx_string.h"
#include <algorithm>

namespace tiledb::stdx::string {

bool starts_with(std::string_view value, std::string_view prefix) {
  if (prefix.size() > value.size())
    return false;
  return std::equal(prefix.begin(), prefix.end(), value.begin());
}

bool ends_with(std::string_view value, std::string_view suffix) {
  if (suffix.size() > value.size())
    return false;
  return value.compare(value.size() - suffix.size(), suffix.size(), suffix) ==
         0;
}

size_t common_prefix_size(std::string_view a, std::string_view b) {
  size_t size = std::min(a.size(), b.size());
  for (size_t i = 0; i < size; ++i) {
    if (a[i] != b[i])
      return i;
  }
  return size;
}

}  // namespace tiledb::stdx::string
