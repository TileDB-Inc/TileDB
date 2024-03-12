/**
 * @file stdx_string.h
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

#ifndef TILEDB_COMMON_STDX_STRING_H
#define TILEDB_COMMON_STDX_STRING_H

#include <string>

namespace tiledb::stdx::string {

/**
 * Checks if a string starts with a certain prefix.
 *
 * C++20: Replace with std::string::starts_with, a member function. This
 * function is a non-member equivalent.
 *
 * @param value The base string.
 * @param prefix The prefix string to be tested.
 * @return `true` if `value` starts with `prefix`; `false` otherwise.
 */
bool starts_with(std::string_view value, std::string_view prefix);

/**
 * Checks if a string ends with a certain suffix.
 *
 * C++20: Replace with std::string::ends_with, a member function. This
 * function is a non-member equivalent.
 *
 * @param value The base string.
 * @param suffix The suffix to be tested.
 * @return `true` if `value` ends with `suffix`; `false` otherwise.
 */
bool ends_with(std::string_view value, std::string_view suffix);

/**
 * Returns the size of the common prefix between `a` and `b`.
 *
 * Postcondition: Let n be the return value.
 *      a[0..n) == b[0..n)  (These ranges are empty if n == 0.)
 *      n == length of a or n == length of b or a[n] != b[n]
 */
size_t common_prefix_size(std::string_view a, std::string_view b);

}  // namespace tiledb::stdx::string

/*
 * Functions forwarded into the legacy namespace.
 */
namespace tiledb::sm::utils::parse {
inline bool starts_with(std::string_view value, std::string_view prefix) {
  return tiledb::stdx::string::starts_with(value, prefix);
}
inline bool ends_with(std::string_view value, std::string_view suffix) {
  return tiledb::stdx::string::ends_with(value, suffix);
}
}  // namespace tiledb::sm::utils::parse
#endif  // TILEDB_COMMON_STDX_STRING_H
