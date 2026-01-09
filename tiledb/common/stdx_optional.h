/**
 * @file stdx_optional.h
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
 */

#ifndef TILEDB_COMMON_STDX_OPTIONAL_H
#define TILEDB_COMMON_STDX_OPTIONAL_H

namespace stdx::optional {

/**
 * Maps an optional value to a different optional value by applying a function
 * to the contained value, if present.
 *
 * C++23: Replace with `std::optional::<T>::transform`.
 *
 * @param maybe_value The base optional value
 * @param f The transforming function
 */
template <typename T, std::invocable<T> F>
std::optional<decltype(std::declval<F>()(std::declval<T>()))> transform(
    std::optional<T> maybe_value, const F& f) {
  if (maybe_value.has_value()) {
    return std::optional(f(maybe_value.value()));
  } else {
    return std::nullopt;
  }
}

}  // namespace stdx::optional

#endif
