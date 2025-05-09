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
 * This file defines functions for manipulating `std::tuple`.
 */

#ifndef TILEDB_TEST_SUPPORT_TUPLE_H
#define TILEDB_TEST_SUPPORT_TUPLE_H

#include <utility>
#include <vector>

namespace stdx {

template <typename... Ts>
constexpr auto decay_types(std::tuple<Ts...> const&)
    -> std::tuple<std::decay_t<Ts>...>;

template <typename Tuple>
using decay_tuple = decltype(decay_types(std::declval<Tuple>()));

/**
 * @return the transposition of row-oriented tuples into column-oriented tuples
 */
template <typename... Ts>
std::tuple<std::vector<Ts>...> transpose(std::vector<std::tuple<Ts...>> rows) {
  std::tuple<std::vector<Ts>...> cols;

  std::apply(
      [&](std::vector<Ts>&... col) { (col.reserve(rows.size()), ...); }, cols);

  for (size_t i = 0; i < rows.size(); i++) {
    std::apply(
        [&](std::vector<Ts>&... col) {
          std::apply([&](Ts... cell) { (col.push_back(cell), ...); }, rows[i]);
        },
        cols);
  }

  return cols;
}

/**
 * @return a tuple whose fields are const references to the argument's fields
 */
template <typename... Ts>
std::tuple<const std::decay_t<Ts>&...> reference_tuple(
    const std::tuple<Ts...>& tuple) {
  return std::apply(
      [](const std::decay_t<Ts>&... field) {
        return std::forward_as_tuple(field...);
      },
      tuple);
}

// Helper function used for `value_type_tuple_t`.
// This is not intended to be evaluated.
template <typename... Ts>
constexpr std::tuple<typename Ts::value_type...> value_type_tuple_f(
    std::tuple<Ts...> tuple);

/**
 * Maps a tuple type `(T1, T2, ...)`,
 * to a type `(T1::value_type, T2::value_type, ...)`.
 */
template <typename Tuple>
using value_type_tuple_t = decltype(value_type_tuple_f(std::declval<Tuple>()));

/**
 * Provides types and definitions for splitting a tuple into
 * left and right tuples at field N.
 */
template <typename Tuple, size_t N>
struct split_tuple {
 private:
  template <size_t... iprefix, size_t... isuffix>
  static auto impl(
      Tuple& tuple,
      std::index_sequence<iprefix...>,
      std::index_sequence<isuffix...>) {
    return std::make_pair(
        std::forward_as_tuple(std::get<iprefix>(tuple)...),
        std::forward_as_tuple(std::get<N + isuffix>(tuple)...));
  }

 public:
  static auto value(Tuple& tuple) {
    return impl(
        tuple,
        std::make_index_sequence<N>{},
        std::make_index_sequence<std::tuple_size_v<Tuple> - N>{});
  }
};

/**
 * Given two tuples of vectors, extends each of the fields of `dst`
 * with the corresponding field of `src`.
 */
template <template <typename T> typename Container, typename... Ts>
void extend(
    std::tuple<Container<Ts>&...>& dest,
    std::tuple<const Container<Ts>&...> src) {
  std::apply(
      [&](Container<Ts>&... dest_col) {
        std::apply(
            [&](const Container<Ts>&... src_col) {
              (dest_col.reserve(dest_col.size() + src_col.size()), ...);
              (dest_col.insert(dest_col.end(), src_col.begin(), src_col.end()),
               ...);
            },
            src);
      },
      dest);
}

/**
 * Selects the positions given by `idx` from each field of `records` to
 * construct a new tuple.
 *
 * @return a new tuple containing just the selected positions
 */
template <template <typename T> typename Container, typename... Ts>
std::tuple<Container<Ts>...> select(
    std::tuple<const Container<Ts>&...> records,
    std::span<const uint64_t> idxs) {
  std::tuple<Container<Ts>...> selected;

  auto select_into = [&idxs]<typename T>(
                         Container<T>& dest, const Container<T>& src) {
    dest.reserve(idxs.size());
    for (auto i : idxs) {
      dest.push_back(src[i]);
    }
  };

  std::apply(
      [&](Container<Ts>&... sel) {
        std::apply(
            [&](const Container<Ts>&... col) {
              (select_into.template operator()<Ts>(sel, col), ...);
            },
            records);
      },
      selected);

  return selected;
}

}  // namespace stdx

#endif
