/**
 * @file tiledb/type/range/range.cc
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
 */

#include "tiledb/type/range/range.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb::type {

std::string range_str(const Range& range, const tiledb::sm::Datatype type) {
  if (range.empty())
    return sm::constants::null_str;

  if (range.var_size() && type != sm::Datatype::STRING_ASCII)
    throw std::invalid_argument(
        "Converting a variable range to a string is only supported for type " +
        datatype_str(sm::Datatype::STRING_ASCII) + ".");

  std::stringstream ss;
  switch (type) {
    case sm::Datatype::INT8:
      ss << "[" << std::to_string(range.typed_data<int8_t>()[0]) << ", "
         << std::to_string(range.typed_data<int8_t>()[1]) << "]";
      break;
    case sm::Datatype::UINT8:
      ss << "[" << std::to_string(range.typed_data<uint8_t>()[0]) << ", "
         << std::to_string(range.typed_data<uint8_t>()[1]) << "]";
      break;
    case sm::Datatype::INT16:
      ss << "[" << ((int16_t*)range.data())[0] << ", "
         << ((int16_t*)range.data())[1] << "]";
      break;
    case sm::Datatype::UINT16:
      ss << "[" << ((uint16_t*)range.data())[0] << ", "
         << ((uint16_t*)range.data())[1] << "]";
      break;
    case sm::Datatype::INT32:
      ss << "[" << ((int32_t*)range.data())[0] << ", "
         << ((int32_t*)range.data())[1] << "]";
      break;
    case sm::Datatype::UINT32:
      ss << "[" << ((uint32_t*)range.data())[0] << ", "
         << ((uint32_t*)range.data())[1] << "]";
      break;
    case sm::Datatype::INT64:
      ss << "[" << ((int64_t*)range.data())[0] << ", "
         << ((int64_t*)range.data())[1] << "]";
      break;
    case sm::Datatype::UINT64:
      ss << "[" << ((uint64_t*)range.data())[0] << ", "
         << ((uint64_t*)range.data())[1] << "]";
      break;
    case sm::Datatype::FLOAT32:
      ss << "[" << ((float*)range.data())[0] << ", "
         << ((float*)range.data())[1] << "]";
      break;
    case sm::Datatype::FLOAT64:
      ss << "[" << ((double*)range.data())[0] << ", "
         << ((double*)range.data())[1] << "]";
      break;
    case sm::Datatype::DATETIME_YEAR:
    case sm::Datatype::DATETIME_MONTH:
    case sm::Datatype::DATETIME_WEEK:
    case sm::Datatype::DATETIME_DAY:
    case sm::Datatype::DATETIME_HR:
    case sm::Datatype::DATETIME_MIN:
    case sm::Datatype::DATETIME_SEC:
    case sm::Datatype::DATETIME_MS:
    case sm::Datatype::DATETIME_US:
    case sm::Datatype::DATETIME_NS:
    case sm::Datatype::DATETIME_PS:
    case sm::Datatype::DATETIME_FS:
    case sm::Datatype::DATETIME_AS:
    case sm::Datatype::TIME_HR:
    case sm::Datatype::TIME_MIN:
    case sm::Datatype::TIME_SEC:
    case sm::Datatype::TIME_MS:
    case sm::Datatype::TIME_US:
    case sm::Datatype::TIME_NS:
    case sm::Datatype::TIME_PS:
    case sm::Datatype::TIME_FS:
    case sm::Datatype::TIME_AS:
      ss << "[" << ((int64_t*)range.data())[0] << ", "
         << ((int64_t*)range.data())[1] << "]";
      break;
    case sm::Datatype::STRING_ASCII:
      ss << "[" << std::string(range.start_str()) << ", "
         << std::string(range.end_str()) << "]";
      break;
    default:
      throw std::invalid_argument(
          "Converting a range to a string is not supported for type " +
          datatype_str(type) + ".");
  }
  return ss.str();
}

}  // namespace tiledb::type
