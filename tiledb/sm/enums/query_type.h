/**
 * @file query_type.h
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
 * This defines the TileDB QueryType enum that maps to tiledb_query_type_t C-API
 * enum.
 */

#ifndef TILEDB_QUERY_TYPE_H
#define TILEDB_QUERY_TYPE_H

#include <cassert>
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

namespace tiledb {
namespace sm {

/** Defines the query type. */
enum class QueryType : uint8_t {
#define TILEDB_QUERY_TYPE_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_QUERY_TYPE_ENUM
};

/** Returns the string representation of the input query type. */
inline const std::string& query_type_str(QueryType query_type) {
  switch (query_type) {
    case QueryType::READ:
      return constants::query_type_read_str;
    case QueryType::WRITE:
      return constants::query_type_write_str;
    case QueryType::DELETE:
      return constants::query_type_delete_str;
    case QueryType::UPDATE:
      return constants::query_type_update_str;
    case QueryType::MODIFY_EXCLUSIVE:
      return constants::query_type_modify_exclusive_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the query type given a string representation. */
inline Status query_type_enum(
    const std::string& query_type_str, QueryType* query_type) {
  if (query_type_str == constants::query_type_read_str)
    *query_type = QueryType::READ;
  else if (query_type_str == constants::query_type_write_str)
    *query_type = QueryType::WRITE;
  else if (query_type_str == constants::query_type_delete_str)
    *query_type = QueryType::DELETE;
  else if (query_type_str == constants::query_type_update_str)
    *query_type = QueryType::UPDATE;
  else if (query_type_str == constants::query_type_modify_exclusive_str)
    *query_type = QueryType::MODIFY_EXCLUSIVE;
  else {
    return Status_Error("Invalid QueryType " + query_type_str);
  }
  return Status::Ok();
}

/* Throws error if cell order's enumeration is greater than 4. */
inline void ensure_query_type_is_valid(QueryType type) {
  if (type > QueryType::MODIFY_EXCLUSIVE)
    throw std::runtime_error(
        "Invalid query type " + std::to_string(static_cast<uint8_t>(type)));
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_TYPE_H
