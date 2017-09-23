/**
 * @file layout.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "constants.h"

namespace tiledb {

/** Defines a layout for the cell or tile order. */
enum class Layout : char {
#define TILEDB_LAYOUT_ENUM(id) id
#include "tiledb_enum.inc"
#undef TILEDB_LAYOUT_ENUM
};

/** Returns the string representation of the input layout. */
inline const char* layout_str(Layout layout) {
  if (layout == Layout::COL_MAJOR)
    return constants::col_major_str;
  if (layout == Layout::ROW_MAJOR)
    return constants::row_major_str;
  if (layout == Layout::GLOBAL_ORDER)
    return constants::global_order_str;
  if (layout == Layout::UNORDERED)
    return constants::unordered_str;

  return nullptr;
}

}  // namespace tiledb

#endif  // TILEDB_LAYOUT_H
