/**
 * @file   helpers-dimension.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Helpers for tests involving dimensions.
 */

#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/type/range/range.h"

#ifndef TILEDB_HELPERS_DIMENSION_H
#define TILEDB_HELPERS_DIMENSION_H

namespace tiledb {
namespace test {

/**
 * A typed version of the standard range class. Constructs Range objects without
 * requiring the caller to know anything about its data structures.
 *
 * @tparam T
 */
template <class T>
class TypedRange : public tiledb::type::Range {
 public:
  /**
   * Construct a Range from a single interval.
   * @param low Lower bound of interval
   * @param high Upper bound of interval
   * @pre low <= high
   */
  TypedRange(T low, T high)
      : tiledb::type::Range(create_test_memory_tracker()) {
    T x[2] = {low, high};
    set_range(&x[0], 2 * sizeof(T));
    /* Commentary: There's no way of initializing a Range without copying from
     * existing memory. As a result any value-initializing constructor first
     * needs to build an array and then copy it.
     */
  }
};

using Datatype = tiledb::sm::Datatype;

template <class T>
struct RangeTraits {
  static Datatype datatype;
};

template <>
struct RangeTraits<int32_t> {
  static constexpr Datatype datatype = Datatype::INT32;
};

template <>
struct RangeTraits<int64_t> {
  static constexpr Datatype datatype = Datatype::INT64;
};

template <>
struct RangeTraits<uint32_t> {
  static constexpr Datatype datatype = Datatype::UINT32;
};

template <>
struct RangeTraits<uint64_t> {
  static constexpr Datatype datatype = Datatype::UINT64;
};

template <>
struct RangeTraits<float> {
  static constexpr Datatype datatype = Datatype::FLOAT32;
};

template <>
struct RangeTraits<double> {
  static constexpr Datatype datatype = Datatype::FLOAT64;
};

}  // namespace test
}  // namespace tiledb

#endif  // TILEDB_HELPERS_DIMENSION_H
