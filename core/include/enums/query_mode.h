/**
 * @file query_mode.h
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
 * This defines the tiledb ArrayMode enum that maps to tiledb_array_mode_t C-api
 * enum.
 */

#ifndef TILEDB_QUERY_MODE_H
#define TILEDB_QUERY_MODE_H

namespace tiledb {

enum class QueryMode : char {
#define TILEDB_QUERY_MODE_ENUM(id) id
#include "tiledb_enum.inc"
#undef TILEDB_QUERY_MODE_ENUM
};

inline bool is_read_mode(const QueryMode mode) {
  return mode == QueryMode::READ || mode == QueryMode::READ_SORTED_COL ||
         mode == QueryMode::READ_SORTED_ROW;
}

inline bool is_write_mode(const QueryMode mode) {
  return mode == QueryMode::WRITE || mode == QueryMode::WRITE_SORTED_COL ||
         mode == QueryMode::WRITE_SORTED_ROW ||
         mode == QueryMode::WRITE_UNSORTED;
}

}  // namespace tiledb

#endif  // TILEDB_QUERY_MODE_H
