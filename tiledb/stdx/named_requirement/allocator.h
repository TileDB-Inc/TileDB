/**
 * @file   tiledb/stdx/named_requirement/allocator.h
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
 * @section Description
 *
 * This file contains a concept that a class template is an allocator.
 *
 * The named requirement `Allocator` is not entirely testable with a `concept`.
 * There are two kinds of limitations:
 * - Behavioral requirements that relate to preconditions and postconditions are
 *   not expressible with a concept.
 * - Quantification over types is not available to ensure that the requirements
 *   are satisfied with any template arguments.
 *
 * Behavioral requirement must be tested in running code, and since there is no
 * proof system, any such testing would be exemplary and not exhaustive.
 *
 * - Reflexivity, symmetry, and transitivity of the equality operator
 * - Equality after copy and move construction
 * - Move constructor does not change the value of its argument
 * - The truth of `is_always_equal`
 * - Equality of pointers converted to `void *` and back
 *
 * Type quantification is tested within the concept in an exemplary way rather than an
 * exhaustive one. Two opaque types are defined to stand in place of arbitrary ones.
 *
 * @section Reference
 *
 * C++ named requirements: Allocator https://en.cppreference.com/w/cpp/named_req/Allocator
 */

#ifndef TILEDB_NAMED_REQUIREMENTS_ALLOCATOR_H
#define TILEDB_NAMED_REQUIREMENTS_ALLOCATOR_H

#include <utility> // std::move

namespace stdx::named_requirement {

namespace {
class X {
 public:
  X() = delete;
};
class Y {
 public:
  Y() = delete;
};
}

/**
 *
 * @section Maturity
 *
 * This class is incomplete.
 * - It makes no attempt to verify the properties of optional
 *
 * @tparam A
 */
template <template <class> class A>
concept allocator =
    requires(A<X> a) { typename A<X>::value_type; } &&
    std::copy_constructible<A<X>> &&
    std::move_constructible<A<X>> &&
//    std::is_copy_assignable_v<A<X>> &&
//    std::is_move_assignable_v<A<X>> &&
    std::destructible<A<X>>

    //  A<opaque1>(a); // copy constructor
//  A<opaque1>::operator=(a); // copy assignment
//  A<opaque2>(a); // copy conversion
//  A<opaque1>(std::move(a)); // move constructor
//  A<opaque1>::operator=(std::move(a)); // move assignment
//  A<opaque2>(std::move(a)); // move conversion
//};
;
}

#endif
