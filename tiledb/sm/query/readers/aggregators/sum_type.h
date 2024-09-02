/**
 * @file   sum_type.h
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
 * @section DESCRIPTION
 *
 * This file defines sum types in relation to basic types.
 */

#ifndef TILEDB_SUM_TYPE_H
#define TILEDB_SUM_TYPE_H
namespace tiledb::sm {

template <class T>
concept SignedInt = std::signed_integral<T>;

template <class T>
concept UnsignedInt = std::unsigned_integral<T>;

template <class T>
concept FloatingPoint = std::floating_point<T>;

template <class T>
concept SummableType = FloatingPoint<T> || SignedInt<T> || UnsignedInt<T>;

/** Convert basic type to a sum type. **/
template <typename T>
  requires SummableType<T>
struct sum_type_data {};

// Signed integer specializations.
template <SignedInt T>
struct sum_type_data<T> {
  using type = T;
  typedef int64_t sum_type;
  static constexpr Datatype tiledb_datatype = Datatype::INT64;
};

// Unsigned integer specializations.
template <UnsignedInt T>
struct sum_type_data<T> {
  using type = T;
  typedef uint64_t sum_type;
  static constexpr Datatype tiledb_datatype = Datatype::UINT64;
};

// Floating point specializations.
template <FloatingPoint T>
struct sum_type_data<T> {
  using type = T;
  typedef double sum_type;
  static constexpr Datatype tiledb_datatype = Datatype::FLOAT64;
};

}  // namespace tiledb::sm

#endif  // TILEDB_SUM_TYPE_H
