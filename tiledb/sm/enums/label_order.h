/**
 * @file label_order.h
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
 * This defines the TileDB Label Order enum.
 */

#ifndef TILEDB_LABEL_ORDER_H
#define TILEDB_LABEL_ORDER_H

#include <cassert>

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb::sm {

/** Defines the query statuses. */
enum class LabelOrder : uint8_t {
#define TILEDB_LABEL_ORDER_ENUM(id) id
#include "tiledb/sm/c_api/experimental/tiledb_enum.h"
#undef TILEDB_LABEL_ORDER_ENUM
};

inline const std::string& label_order_str(LabelOrder order) {
  switch (order) {
    case LabelOrder::UNORDERED_LABELS:
      return constants::label_unordered_str;
    case LabelOrder::INCREASING_LABELS:
      return constants::label_increasing_str;
    case LabelOrder::DECREASING_LABELS:
      return constants::label_decreasing_str;
    default:
      return constants::empty_str;
  }
}

}  // namespace tiledb::sm

#endif  // TILEDB_LABEL_ORDER_H
