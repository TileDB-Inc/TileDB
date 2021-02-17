/**
 * @file query_condition_condition_op.h
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
 * This defines the tiledb QueryConditionCombinationOp enum that maps to
 * tiledb_query_condition_combination_op_t C-api enum.
 */

#ifndef TILEDB_QUERY_CONDITION_COMBINATION_OP_H
#define TILEDB_QUERY_CONDITION_COMBINATION_OP_H

#include <cassert>

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Defines the query condition ops. */
enum class QueryConditionCombinationOp : uint8_t {
#define TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM
};

/**
 * Returns the string representation of the input QueryConditionCombinationOp
 * type.
 */
inline const std::string& query_condition_combination_op_str(
    QueryConditionCombinationOp query_condition_combination_op) {
  switch (query_condition_combination_op) {
    case QueryConditionCombinationOp::AND:
      return constants::query_condition_combination_op_and_str;
    case QueryConditionCombinationOp::OR:
      return constants::query_condition_combination_op_or_str;
    case QueryConditionCombinationOp::NOT:
      return constants::query_condition_combination_op_not_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the query condition_op given a string representation. */
inline Status query_condition_combination_op_enum(
    const std::string& query_condition_combination_op_str,
    QueryConditionCombinationOp* query_condition_combination_op) {
  if (query_condition_combination_op_str ==
      constants::query_condition_combination_op_and_str)
    *query_condition_combination_op = QueryConditionCombinationOp::AND;
  else if (
      query_condition_combination_op_str ==
      constants::query_condition_combination_op_or_str)
    *query_condition_combination_op = QueryConditionCombinationOp::OR;
  else if (
      query_condition_combination_op_str ==
      constants::query_condition_combination_op_not_str)
    *query_condition_combination_op = QueryConditionCombinationOp::NOT;
  else {
    return Status::Error(
        "Invalid QueryConditionCombinationOp " +
        query_condition_combination_op_str);
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_CONDITION_COMBINATION_OP_H
