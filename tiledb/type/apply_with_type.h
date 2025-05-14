/**
 * @file tiledb/type/apply_with_type.h
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
 * This file declares the apply_with_type function
 */

#ifndef TILEDB_APPLY_WITH_TYPE_H
#define TILEDB_APPLY_WITH_TYPE_H

#include "tiledb/sm/enums/datatype.h"
#include "tiledb/type/datatype_traits.h"

using tiledb::sm::Datatype;

namespace tiledb::type {

template <class T>
concept TileDBFundamental = std::integral<T> || std::floating_point<T>;

template <class T>
concept TileDBIntegral = std::integral<T> && !std::is_same_v<T, char>;

template <class T>
concept TileDBNumeric = TileDBIntegral<T> || std::floating_point<T>;

/**
 * Execute a callback instantiated based on the tiledb Datatype
 * passed as argument
 *
 * @param query A C api query pointer
 */
template <class Fn, class... Args>
inline auto apply_with_type(Fn&& f, Datatype type, Args&&... args) {
#define CASE(type) \
  case (type):     \
    return f(datatype_traits<(type)>::value_type{}, std::forward<Args>(args)...)

  switch (type) {
    CASE(Datatype::INT32);
    CASE(Datatype::INT64);
    CASE(Datatype::INT8);
    CASE(Datatype::UINT8);
    CASE(Datatype::INT16);
    CASE(Datatype::UINT16);
    CASE(Datatype::UINT32);
    CASE(Datatype::UINT64);
    CASE(Datatype::FLOAT32);
    CASE(Datatype::FLOAT64);
    CASE(Datatype::DATETIME_YEAR);
    CASE(Datatype::DATETIME_MONTH);
    CASE(Datatype::DATETIME_WEEK);
    CASE(Datatype::DATETIME_DAY);
    CASE(Datatype::DATETIME_HR);
    CASE(Datatype::DATETIME_MIN);
    CASE(Datatype::DATETIME_SEC);
    CASE(Datatype::DATETIME_MS);
    CASE(Datatype::DATETIME_US);
    CASE(Datatype::DATETIME_NS);
    CASE(Datatype::DATETIME_PS);
    CASE(Datatype::DATETIME_FS);
    CASE(Datatype::DATETIME_AS);
    CASE(Datatype::TIME_HR);
    CASE(Datatype::TIME_MIN);
    CASE(Datatype::TIME_SEC);
    CASE(Datatype::TIME_MS);
    CASE(Datatype::TIME_US);
    CASE(Datatype::TIME_NS);
    CASE(Datatype::TIME_PS);
    CASE(Datatype::TIME_FS);
    CASE(Datatype::TIME_AS);
    CASE(Datatype::STRING_ASCII);
    CASE(Datatype::STRING_UTF8);
    default: {
      throw std::logic_error(
          "Datatype::" + datatype_str(type) + " is not a supported Datatype");
    }
  }
}

}  // namespace tiledb::type

#endif
