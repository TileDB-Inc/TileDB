/**
 * @file walk_order.h
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
 * This file defines the tiledb WalkOrder enum that maps to the
 * tiledb_walk_order_t C-api enum
 */

#ifndef TILEDB_WALK_ORDER_H
#define TILEDB_WALK_ORDER_H

#include "tiledb/sm/misc/constants.h"

namespace tiledb {
namespace sm {

enum class WalkOrder : uint8_t {
#define TILEDB_WALK_ORDER_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_WALK_ORDER_ENUM
};

/** Returns the string representation of the input walkorder type. */
inline const std::string& walkorder_str(WalkOrder walkorder) {
  switch (walkorder) {
    case WalkOrder::PREORDER:
      return constants::walkorder_preorder_str;
    case WalkOrder::POSTORDER:
      return constants::walkorder_postorder_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the walkorder given a string representation. */
inline Status walkorder_enum(
    const std::string& walkorder_str, WalkOrder* walkorder) {
  if (walkorder_str == constants::walkorder_preorder_str)
    *walkorder = WalkOrder::PREORDER;
  else if (walkorder_str == constants::walkorder_postorder_str)
    *walkorder = WalkOrder::POSTORDER;
  else
    return Status_Error("Invalid WalkOrder " + walkorder_str);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_WALK_ORDER_H
