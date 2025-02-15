/**
 * @file test/support/stdx/fold.h
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
 * This file defines functions which do things you would want a fold expression
 * to do but can't figure out how to make them do it.
 */

#ifndef TILEDB_TEST_SUPPORT_FOLD_H
#define TILEDB_TEST_SUPPORT_FOLD_H

namespace stdx {

template <typename T, T... S, typename F>
constexpr bool fold_sequence(std::integer_sequence<T, S...>, F&& f) {
  return (f(std::integral_constant<T, S>{}) || ...);
}

template <typename T, T First, T... Rest>
struct fold_optional_t {
  template <typename R, typename F>
  static constexpr std::optional<R> fold(F&& f) {
    const auto maybe = f(std::integral_constant<T, First>{});
    if (maybe.has_value()) {
      return maybe;
    } else {
      return fold_optional_t<T, Rest...>::template fold<R, F>(f);
    }
  }
};

template <typename T, T Only>
struct fold_optional_t<T, Only> {
  template <typename R, typename F>
  static constexpr std::optional<R> fold(F&& f) {
    return f(std::integral_constant<T, Only>{});
  }
};

template <typename R, typename T, T... S, typename F>
constexpr std::optional<R> fold_optional(
    std::integer_sequence<T, S...>, F&& f) {
  return fold_optional_t<T, S...>::template fold<R, F>(f);
}

}  // namespace stdx

#endif
