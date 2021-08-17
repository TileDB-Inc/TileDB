/**
 * @file query_condition_op.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This defines the tiledb QueryConditionOp enum that maps to
 * tiledb_query_condition_op_t C-api enum.
 */

#ifndef TILEDB_QUERY_CONDITION_OP_H
#define TILEDB_QUERY_CONDITION_OP_H

#include <cassert>

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Defines the query condition ops. */
enum class QueryConditionOp : uint8_t {
#define TILEDB_QUERY_CONDITION_OP_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_QUERY_CONDITION_OP_ENUM
};

/** Returns the string representation of the input QueryConditionOp type. */
inline const std::string& query_condition_op_str(
    QueryConditionOp query_condition_op) {
  switch (query_condition_op) {
    case QueryConditionOp::LT:
      return constants::query_condition_op_lt_str;
    case QueryConditionOp::LE:
      return constants::query_condition_op_le_str;
    case QueryConditionOp::GT:
      return constants::query_condition_op_gt_str;
    case QueryConditionOp::GE:
      return constants::query_condition_op_ge_str;
    case QueryConditionOp::EQ:
      return constants::query_condition_op_eq_str;
    case QueryConditionOp::NE:
      return constants::query_condition_op_ne_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the query condition_op given a string representation. */
inline Status query_condition_op_enum(
    const std::string& query_condition_op_str,
    QueryConditionOp* query_condition_op) {
  if (query_condition_op_str == constants::query_condition_op_lt_str)
    *query_condition_op = QueryConditionOp::LT;
  else if (query_condition_op_str == constants::query_condition_op_le_str)
    *query_condition_op = QueryConditionOp::LE;
  else if (query_condition_op_str == constants::query_condition_op_gt_str)
    *query_condition_op = QueryConditionOp::GT;
  else if (query_condition_op_str == constants::query_condition_op_ge_str)
    *query_condition_op = QueryConditionOp::GE;
  else if (query_condition_op_str == constants::query_condition_op_eq_str)
    *query_condition_op = QueryConditionOp::EQ;
  else if (query_condition_op_str == constants::query_condition_op_ne_str)
    *query_condition_op = QueryConditionOp::NE;
  else {
    return Status::Error("Invalid QueryConditionOp " + query_condition_op_str);
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_CONDITION_OP_H
