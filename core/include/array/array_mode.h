/**
 * @file array_mode.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 *            Copyright (c) 2016 MIT and Intel Corporation
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
 * This defines the tiledb ArrayMode enum that maps to tiledb_array_mode_t C-api
 * enum.
 */

#ifndef TILEDB_ARRAY_MODE_H
#define TILEDB_ARRAY_MODE_H

namespace tiledb {

enum class ArrayMode : char {
#define TILEDB_ARRAY_MODE_ENUM(id) id
#include "tiledb_enum.inc"
#undef TILEDB_ARRAY_MODE_ENUM
};

inline bool is_read_mode(const ArrayMode mode) {
  return mode == ArrayMode::READ || mode == ArrayMode::READ_SORTED_COL ||
         mode == ArrayMode::READ_SORTED_ROW;
}

inline bool is_write_mode(const ArrayMode mode) {
  return mode == ArrayMode::WRITE || mode == ArrayMode::WRITE_SORTED_COL ||
         mode == ArrayMode::WRITE_SORTED_ROW ||
         mode == ArrayMode::WRITE_UNSORTED;
}
}  // namespace tiledb

#endif  // TILEDB_ARRAY_MODE_H
