/**
 * @file dynamic_typed_datum.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 */

#include "dynamic_typed_datum.h"
#include <string_view>

std::ostream& operator<<(
    std::ostream& os, const tdb::DynamicTypedDatumView& x) {
  using Datatype = tiledb::sm::Datatype;

  switch (x.type_) {
    case Datatype::INT32:
      os << *static_cast<const int32_t*>(x.datum_.content());
      break;
    case Datatype::INT64:
      os << *static_cast<const int64_t*>(x.datum_.content());
      break;
    case Datatype::FLOAT32:
      os << *static_cast<const float*>(x.datum_.content());
      break;
    case Datatype::FLOAT64:
      os << *static_cast<const double*>(x.datum_.content());
      break;
    case Datatype::CHAR:
      os << *static_cast<const char*>(x.datum_.content());
      break;
    case Datatype::INT8:
      os << *static_cast<const int8_t*>(x.datum_.content());
      break;
    case Datatype::UINT8:
      os << *static_cast<const uint8_t*>(x.datum_.content());
      break;
    case Datatype::INT16:
      os << *static_cast<const int16_t*>(x.datum_.content());
      break;
    case Datatype::UINT16:
      os << *static_cast<const uint16_t*>(x.datum_.content());
      break;
    case Datatype::UINT32:
      os << *static_cast<const uint32_t*>(x.datum_.content());
      break;
    case Datatype::UINT64:
      os << *static_cast<const uint64_t*>(x.datum_.content());
      break;
    case Datatype::STRING_ASCII:
      os << std::string_view(
          static_cast<const char*>(x.datum_.content()), x.datum_.size());
      break;
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::CATEGORICAL_UTF8:
    case Datatype::ANY:
      os << "???";
      break;
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
    case Datatype::TIME_AS:
      os << *static_cast<const int64_t*>(x.datum_.content());
      break;
    default:
      break;
  }
  return os;
}
