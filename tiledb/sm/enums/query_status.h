/**
 * @file query_status.h
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
 * This defines the tiledb QueryStatus enum that maps to tiledb_query_status_t
 * C-api enum.
 */

#ifndef TILEDB_QUERY_STATUS_H
#define TILEDB_QUERY_STATUS_H

#include <cassert>

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

namespace tiledb {
namespace sm {

/** Defines the query statuses. */
enum class QueryStatus : uint8_t {
#define TILEDB_QUERY_STATUS_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_QUERY_STATUS_ENUM
};

/** Returns the string representation of the input querystatus type. */
inline const std::string& query_status_str(QueryStatus query_status) {
  switch (query_status) {
    case QueryStatus::FAILED:
      return constants::query_status_failed_str;
    case QueryStatus::COMPLETED:
      return constants::query_status_completed_str;
    case QueryStatus::INPROGRESS:
      return constants::query_status_inprogress_str;
    case QueryStatus::INCOMPLETE:
      return constants::query_status_incomplete_str;
    case QueryStatus::UNINITIALIZED:
      return constants::query_status_uninitialized_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the query status given a string representation. */
inline Status query_status_enum(
    const std::string& query_status_str, QueryStatus* query_status) {
  if (query_status_str == constants::query_status_failed_str)
    *query_status = QueryStatus::FAILED;
  else if (query_status_str == constants::query_status_completed_str)
    *query_status = QueryStatus::COMPLETED;
  else if (query_status_str == constants::query_status_inprogress_str)
    *query_status = QueryStatus::INPROGRESS;
  else if (query_status_str == constants::query_status_incomplete_str)
    *query_status = QueryStatus::INCOMPLETE;
  else if (query_status_str == constants::query_status_uninitialized_str)
    *query_status = QueryStatus::UNINITIALIZED;
  else {
    return Status::Error("Invalid QueryStatus " + query_status_str);
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_STATUS_H
