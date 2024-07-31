/**
 * @file layout.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This defines the tiledb Layout enum that maps to tiledb_layout_t C-api
 * enum.
 */

#ifndef TILEDB_LAYOUT_H
#define TILEDB_LAYOUT_H

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

#include <cassert>

using namespace tiledb::common;

namespace tiledb::sm {

/** Defines a layout for the cell or tile order. */
enum class Layout : uint8_t {
#define TILEDB_LAYOUT_ENUM(id) id
#include "tiledb/api/c_api/array_schema/layout_enum.h"
#undef TILEDB_LAYOUT_ENUM
};

/** Returns the string representation of the input layout. */
inline const std::string& layout_str(Layout layout) {
  switch (layout) {
    case Layout::COL_MAJOR:
      return constants::col_major_str;
    case Layout::ROW_MAJOR:
      return constants::row_major_str;
    case Layout::GLOBAL_ORDER:
      return constants::global_order_str;
    case Layout::UNORDERED:
      return constants::unordered_str;
    case Layout::HILBERT:
      return constants::hilbert_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the layout enum given a string representation. */
inline Status layout_enum(const std::string& layout_str, Layout* layout) {
  if (layout_str == constants::col_major_str)
    *layout = Layout::COL_MAJOR;
  else if (layout_str == constants::row_major_str)
    *layout = Layout::ROW_MAJOR;
  else if (layout_str == constants::global_order_str)
    *layout = Layout::GLOBAL_ORDER;
  else if (layout_str == constants::unordered_str)
    *layout = Layout::UNORDERED;
  else if (layout_str == constants::hilbert_str)
    *layout = Layout::HILBERT;
  else {
    return Status_Error("Invalid Layout " + layout_str);
  }
  return Status::Ok();
}

/* Throws error if tile order's enumeration is not 0 or 1. */
inline void ensure_tile_order_is_valid(uint8_t layout_enum) {
  if (layout_enum != 0 && layout_enum != 1)
    throw std::runtime_error(
        "[Tile order] Invalid Layout enum " + std::to_string(layout_enum));
}

/* Throws error if cell order's enumeration is greater than 4. */
inline void ensure_cell_order_is_valid(uint8_t layout_enum) {
  if (layout_enum > 4)
    throw std::runtime_error(
        "[Cell order] Invalid Layout enum " + std::to_string(layout_enum));
}

}  // namespace tiledb::sm

#endif  // TILEDB_LAYOUT_H
