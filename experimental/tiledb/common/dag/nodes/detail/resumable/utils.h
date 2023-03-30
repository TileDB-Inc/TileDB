/**
* @file   utils.h
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
*/


#ifndef TILEDB_DAG_NODES_DETAIL_RESUMABLE_UTILS_H
#define TILEDB_DAG_NODES_DETAIL_RESUMABLE_UTILS_H

#include <tuple>
#include <type_traits>
#include <utility>
#include <functional>
#include <cstdint>
#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"

namespace tiledb::common {

/**
 * template meta program to check if all types in a parameter pack are the same
 * @tparam T The type to check
 * @tparam Ts The parameter pack to check against
 */
template <class T, class... Ts>
struct are_same : std::conjunction<std::is_same<T, Ts>...> {};

/**
 * Convenience alias for are_same
 */
template <class T, class... Ts>
inline constexpr static bool are_same_v = are_same<T, Ts...>::value;


/**
   * Helper function to deal with tuples.  Applies the same single input single
   * output function to elements of an input tuple to set values of an output
   * tuple.
   *
   * @note Elements are processed in order from 0 to sizeof(Ts)-1
   *
   * @pre sizeof(Ts) == sizeof(Us)
 */
template <size_t I = 0, class Fn, class... Ts, class... Us>
constexpr void tuple_map(
    Fn&& f, const std::tuple<Ts...>& in, std::tuple<Us...>& out) {
  static_assert(sizeof(in) == sizeof(out));
  if constexpr (I == sizeof...(Ts)) {
    return;
  } else {
    std::get<I>(out) = std::forward<Fn>(f)(std::get<I>(in));
    tuple_map(I + 1, std::forward<Fn>(f), in, out);
  }
}

template <size_t I = 0, class Op, class Fn, class... Ts>
constexpr auto tuple_fold(Op&& op, Fn&& f, const std::tuple<Ts...>& in) {
  // static_assert(I == 0);
  static_assert(I >= 0);
  // static_assert(sizeof...(Ts) > 0 && sizeof...(Ts) < 10);
  if constexpr (I == sizeof...(Ts) - 1) {
    return f(std::get<I>(in));
  } else {
    return op(f(std::get<I>(in)), tuple_fold<I + 1>(op, f, in));
  }
}

/**
 * Some type aliases for the function enclosed by the node, to allow empty
 * inputs or empty outputs (for creating producer or consumer nodes for
 * testing/validation from function node).
 */
template <
    class BlocksIn = std ::tuple<std::tuple<>>,
    class BlocksOut = std::tuple<std::tuple<>>>
struct fn_type;

template <class... BlocksIn, class... BlocksOut>
struct fn_type<std::tuple<BlocksIn...>, std::tuple<BlocksOut...>> {
  using type =
      std::function<std::tuple<BlocksOut...>(const std::tuple<BlocksIn...>&)>;
};

template <class... BlocksOut>
struct fn_type<std::tuple<>, std::tuple<BlocksOut...>> {
  using type = std::function<std::tuple<BlocksOut...>(std::stop_source&)>;
};

template <class... BlocksIn>
struct fn_type<std::tuple<BlocksIn...>, std::tuple<>> {
  using type = std::function<void(const std::tuple<BlocksIn...>&)>;
};

}
#endif // TILEDB_DAG_NODES_DETAIL_RESUMABLE_UTILS_H
