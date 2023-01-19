/**
 * @file data_order.h
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
 * This defines the TileDB Data Order enum.
 */

#ifndef TILEDB_DATA_ORDER_H
#define TILEDB_DATA_ORDER_H

#include <cassert>

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb::sm {

/** Defines the ordering of data in a dimension data. */
enum class DataOrder : uint8_t {
#define TILEDB_DATA_ORDER_ENUM(id) id
#include "tiledb/api/c_api/data_order/data_order_api_enum.h"
#undef TILEDB_DATA_ORDER_ENUM
};

/**
 * Converts an input DataOrder enum to a string representation.
 *
 * @param order The target DataOrder enum.
 * @returns The constant string representation of the enum.
 */
inline const std::string& data_order_str(DataOrder order) {
  switch (order) {
    case DataOrder::UNORDERED_DATA:
      return constants::data_unordered_str;
    case DataOrder::INCREASING_DATA:
      return constants::data_increasing_str;
    case DataOrder::DECREASING_DATA:
      return constants::data_decreasing_str;
    default:
      return constants::empty_str;
  }
}

/**
 * Converts an uint8_t value to a DataOrder enum.
 *
 * Used for deserializing the data order. Throws an error if the data order's
 * enumeration is not valid.
 */
inline DataOrder data_order_from_int(uint8_t data_order_int) {
  auto data_order = DataOrder(data_order_int);
  switch (data_order) {
    case DataOrder::UNORDERED_DATA:
    case DataOrder::INCREASING_DATA:
    case DataOrder::DECREASING_DATA:
      return data_order;
    default:
      throw std::runtime_error(
          "Invalid DataOrder( " +
          std::to_string(static_cast<uint16_t>(data_order_int)) + ")");
  }
}

/** Converts a string to a DataOrder enum. */
inline DataOrder data_order_from_str(const std::string& str) {
  if (str == constants::data_unordered_str) {
    return DataOrder::UNORDERED_DATA;
  } else if (str == constants::data_increasing_str) {
    return DataOrder::INCREASING_DATA;
  } else if (str == constants::data_decreasing_str) {
    return DataOrder::DECREASING_DATA;
  } else {
    throw std::runtime_error(
        "Invalid input string '" + str +
        "' does not correspond to a data order type.");
  }
}

}  // namespace tiledb::sm

#endif  // TILEDB_DATA_ORDER_H
