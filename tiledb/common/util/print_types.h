/**
 * @file   tiledb/common/util/print_types.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file implements a compile-time debugging utility for investigating
 * the specific types of objects.  It is the equivalent of a compile-time
 * `printf`, but for types and is useful for debugging compile-time issues
 * related to templates.  It is intended for development use only and should
 * not be included in production code.
 *
 * As a compile-time "printf" for types, it will generate a compiler error
 * whose message will contain the expanded types of the variables passed to it.
 * Note that the `print_types` takes *variable* as argument, not actual types
 * or template parameters.  The programmer will need to inspect the error
 * message to determine the types of the variables.
 *
 * @example
 *
 *   // See https://godbolt.org/z/3q341cs57 for running example
 *   #include <vector>
 *   #include "tiledb/common/util/print_types.h"
 *
 *   template <class S, class T>
 *   void foo(const S& s, const T& t) {
 *     // Note we pass s and t, not S and T
 *     print_types(s, t);
 *   }
 *
 *   int main() {
 *     foo(1.0, std::vector<std::vector<int>>{{1, 2}, {3, 4}});
 *   }
 *
 * This will produce an error message like the following (gcc 12.1):
 *   <source>:18:31: error: invalid use of incomplete type 'struct
 *   print_types_t<double, std::vector<std::vector<int, std::allocator<int> >,
 *   std::allocator<std::vector<int, std::allocator<int> > > > >' 18 |   return
 *   print_types_t<Ts...>{};
 *
 * Or like the following (clang 16.0):
 *   <source>:18:10: error: implicit instantiation of undefined template
 *   'print_types_t<double, std::vector<std::vector<int>>>' return
 *   print_types_t<Ts...>{};
 *
 *   The types of the variable `s` and `t` are contained in the template
 *   arguments shown for `print_types`.  In this case they are respectively
 *   `double` and `std::vector<std::vector<int>>`.
 *
 * This file is based on the `print_types` utility from NWGraph.
 * Author Luke D'Alessandro.
 */

#ifndef TILEDB_PRINT_TYPES_H
#define TILEDB_PRINT_TYPES_H

template <class... Ts>
struct print_types_t;

/*
 * Print (as compiler error message), the types of the
 * variadic argument list.  E.g.,
 *   print_types(foo, bar, baz);
 */
template <class... Ts>
constexpr auto print_types(Ts...) {
  return print_types_t<Ts...>{};
}

#endif  // TILEDB_PRINT_TYPES_H
