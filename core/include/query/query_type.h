/**
 * @file query_type.h
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
 * This defines the QueryType enum that maps to tiledb_query_type_t C-API.
 * enum.
 */

#ifndef TILEDB_QUERY_TYPE_H
#define TILEDB_QUERY_TYPE_H

namespace tiledb {

enum class QueryType : char {
#define TILEDB_QUERY_TYPE_ENUM(id) id
#include "tiledb_enum.inc"
#undef TILEDB_QUERY_TYPE_ENUM
};

inline bool is_read_mode(const QueryType type) {
  return type == QueryType::READ || type == QueryType::READ_SORTED_COL ||
         type == QueryType::READ_SORTED_ROW;
}

inline bool is_write_mode(const QueryType type) {
  return type == QueryType::WRITE || type == QueryType::WRITE_SORTED_COL ||
         type == QueryType::WRITE_SORTED_ROW ||
         type == QueryType::WRITE_UNSORTED;
}

}  // namespace tiledb

#endif  // TILEDB_QUERY_TYPE_H
