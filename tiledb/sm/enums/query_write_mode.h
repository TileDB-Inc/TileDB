/**
 * @file query_write_mode.h
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
 * This defines the TileDB QueryWriteMode enum that maps to
 * tiledb_query_write_mode_t C-API enum.
 */

#ifndef TILEDB_QUERY_WRITE_MODE_H
#define TILEDB_QUERY_WRITE_MODE_H

#include <cassert>
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

namespace tiledb {
namespace sm {

/** Defines the query type. */
enum class QueryWriteMode : uint8_t {
#define TILEDB_QUERY_WRITE_MODE_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_QUERY_WRITE_MODE_ENUM
};

/** Returns the string representation of the input query write mode. */
inline const std::string& query_type_str(QueryWriteMode query_write_mode) {
  switch (query_write_mode) {
    case QueryWriteMode::DEFAULT:
      return constants::query_write_mode_default_str;
    case QueryWriteMode::SEPARATE_ATTRIBUTES:
      return constants::query_write_mode_separate_attributes_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the query type given a string representation. */
inline Status query_write_mode_enum(
    const std::string& query_write_mode_str, QueryWriteMode* query_write_mode) {
  if (query_write_mode_str == constants::query_write_mode_default_str)
    *query_write_mode = QueryWriteMode::DEFAULT;
  else if (
      query_write_mode_str ==
      constants::query_write_mode_separate_attributes_str)
    *query_write_mode = QueryWriteMode::SEPARATE_ATTRIBUTES;
  else {
    return Status_Error("Invalid QueryWriteMode" + query_write_mode_str);
  }
  return Status::Ok();
}

/* Throws error if cell order's enumeration is greater than 1. */
inline void ensure_query_write_mode_is_valid(QueryWriteMode type) {
  if (type > QueryWriteMode::SEPARATE_ATTRIBUTES)
    throw std::runtime_error(
        "Invalid query type " + std::to_string(static_cast<uint8_t>(type)));
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_WRITE_MODE_H
