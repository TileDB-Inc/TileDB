/**
 * @file tiledb/common/unreachable.h
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
 * This file contains an implementation of C++23 `std::unreachable()`.
 *
 * @section REFERENCE
 *
 * http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2021/p0627r6.pdf
 * https://en.cppreference.com/w/cpp/utility/unreachable
 */

#ifndef TILEDB_UNREACHABLE_H
#define TILEDB_UNREACHABLE_H

#include <cstdlib>

namespace stdx {

/**
 * Anticipatory implementation of C++23 `std::unreachable`.
 *
 * The current version supports GCC, clang, and MSVC at minimum. Other compilers
 * that support `__builtin_unreachable` will also work.
 *
 * The github project [Hedley](https://nemequ.github.io/hedley/) has a more
 * extensive implementation with more compiler support should it ever be needed.
 */
[[noreturn]] inline void unreachable() {
#ifdef _MSC_VER
  /*
   * Use of `__assume(0)` is from the Microsoft documentation
   * https://docs.microsoft.com/en-us/cpp/intrinsics/assume
   */
  __assume(0);
#else
  /*
   * gcc: https://gcc.gnu.org/onlinedocs/gcc/Other-Builtins.html#index-_005f_005fbuiltin_005funreachable
   * clang: https://clang.llvm.org/docs/LanguageExtensions.html#builtin-unreachable
   */
  __builtin_unreachable();
#endif
  /*
   * Should be impossible to call; only present as defense. The only situation
   * where this is relevant is if this function is compiled with a compiler that
   * does not have the built-in but ignores it instead of ending compilation
   * with an error.
   */
  abort();
}

}  // namespace stdx

#endif  // TILEDB_UNREACHABLE_H
