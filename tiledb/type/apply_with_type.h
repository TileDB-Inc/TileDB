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
  switch (type) {
    case Datatype::INT32: {
      return f(int32_t{}, std::forward<Args>(args)...);
    }
    case Datatype::INT64: {
      return f(int64_t{}, std::forward<Args>(args)...);
    }
    case Datatype::INT8: {
      return f(int8_t{}, std::forward<Args>(args)...);
    }
    case Datatype::UINT8: {
      return f(uint8_t{}, std::forward<Args>(args)...);
    }
    case Datatype::INT16: {
      return f(int16_t{}, std::forward<Args>(args)...);
    }
    case Datatype::UINT16: {
      return f(uint16_t{}, std::forward<Args>(args)...);
    }
    case Datatype::UINT32: {
      return f(uint32_t{}, std::forward<Args>(args)...);
    }
    case Datatype::UINT64: {
      return f(uint64_t{}, std::forward<Args>(args)...);
    }
    case Datatype::FLOAT32: {
      return f(float{}, std::forward<Args>(args)...);
    }
    case Datatype::FLOAT64: {
      return f(double{}, std::forward<Args>(args)...);
    }
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS: {
      return f(int64_t{}, std::forward<Args>(args)...);
    }
    case Datatype::STRING_ASCII: {
      return f(char{}, std::forward<Args>(args)...);
    }
    default: {
      throw std::logic_error(
          "Datatype::" + datatype_str(type) + " is not a valid Datatype");
    }
  }
}

}  // namespace tiledb::type

#endif
