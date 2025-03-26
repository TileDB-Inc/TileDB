/**
 * @file test/support/rapidcheck/show.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2025 TileDB, Inc.
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
 * This file provides forward declarations of `rc::detail::showValue`
 * overloads, which seemingly must be included prior to the rapidcheck
 * header files.
 */

#ifndef TILEDB_RAPIDCHECK_SHOW_H_
#define TILEDB_RAPIDCHECK_SHOW_H_

#include <test/support/stdx/optional.h>

#include <ostream>

namespace tiledb {
namespace sm {
struct ASTNode;
}
}  // namespace tiledb

// forward declarations of `showValue` overloads
// (these must be declared prior to including `rapidcheck/Show.hpp` for some
// reason)
namespace rc::detail {

/**
 * Specializes `show` for `std::optional<T>` to print the final test case after
 * shrinking
 */
template <stdx::is_optional_v T>
void showValue(const T& value, std::ostream& os);

/**
 * Specializes `show` for query ASTNode
 */
void showValue(const tiledb::sm::ASTNode& value, std::ostream& os);

}  // namespace rc::detail

#endif
