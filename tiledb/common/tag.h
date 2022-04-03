/**
 * @file tiledb/sm/array_schema/domain_typed_data_view.h
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
 * This file defines a class Tag useful for template argument deduction.
 */

#ifndef TILEDB_COMMON_TAG_H
#define TILEDB_COMMON_TAG_H

#include <type_traits>

namespace tiledb ::common {
/**
 * State-free class that carries a type.
 *
 * This class support template argument deduction for constructors. C++ syntax
 * does not allow direct specification of template arguments in a template
 * constructor (for example `T<class_arguments><constructor_arguments>` is not
 * a thing). Instead, a template constructor must infer its arguments from
 * an argument list. This class has no state, but does have a type that can
 * be matched.
 *
 * @example
 * ```
 * class Example {
 *   template<class P> Example(Tag<P>, int);
 * };
 *
 * struct Policy {
 *   P() = default;
 *   static void behavior();
 * };
 *
 * void foo() {
 *   Example x{Tag<Policy>{}, 0};  // May treat 0 with varying behavior
 * }
 *```
 *
 * @tparam T The type carried by the tag
 */
template <class T>
struct Tag {
  using type = T;
  Tag() = default;
  Tag(const Tag&) = delete;
  Tag& operator=(const Tag&) = delete;
  Tag(Tag&&) = delete;
  Tag& operator=(Tag&&) = delete;
};

static_assert(std::is_default_constructible_v<Tag<void>>);
static_assert(!std::is_copy_constructible_v<Tag<void>>);
static_assert(!std::is_copy_assignable_v<Tag<void>>);
static_assert(!std::is_move_constructible_v<Tag<void>>);
static_assert(!std::is_move_assignable_v<Tag<void>>);
}  // namespace tiledb::common

#endif  // TILEDB_COMMON_TAG_H
