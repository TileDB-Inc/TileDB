/**
 * @file query_helpers.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 */
#include "test/support/src/query_helpers.h"
#include "tiledb/stdx/utility/to_underlying.h"
#include "tiledb/type/apply_with_type.h"

#include <sstream>

namespace tiledb::test {

using namespace tiledb::sm;

/**
 * @return a SQL representation of a `QueryConditionOp`
 */
static const char* to_sql_op(QueryConditionOp op) {
  switch (op) {
    case QueryConditionOp::LT:
      return "<";
    case QueryConditionOp::LE:
      return "<=";
    case QueryConditionOp::EQ:
      return "=";
    case QueryConditionOp::GE:
      return ">=";
    case QueryConditionOp::GT:
      return ">";
    case QueryConditionOp::NE:
      return "<>";
    default:
      throw std::logic_error(
          "Invalid query condition op: " +
          std::to_string(stdx::to_underlying(op)));
  }
}

/**
 * @return a SQL representation of the query condition syntax tree
 */
std::string to_sql(const ASTNode& ast, const ArraySchema& schema) {
  const ASTNodeVal* valnode = static_cast<const ASTNodeVal*>(&ast);
  const ASTNodeExpr* exprnode = dynamic_cast<const ASTNodeExpr*>(&ast);

  std::stringstream os;
  if (valnode) {
    const auto fname = valnode->get_field_name();
    const auto op = valnode->get_op();
    const auto bytes = valnode->get_data();

    std::stringstream value;

    apply_with_type(
        [&](auto t) {
          using T = decltype(t);
          value << *reinterpret_cast<const T*>(bytes.data());
        },
        schema.type(fname));

    os << fname << " " << to_sql_op(op) << " " << value.str();
  } else if (exprnode) {
    const auto op = exprnode->get_combination_op();
    const auto& children = exprnode->get_children();
    if (op == QueryConditionCombinationOp::NOT) {
      assert(children.size() == 1);
      os << "NOT ";
    }
    for (unsigned i = 0; i < children.size(); i++) {
      if (i != 0) {
        os << " " << query_condition_combination_op_str(op) << " ";
      }
      os << "(" << to_sql(*children[i].get(), schema) << ")";
    }
  } else {
    throw std::logic_error(
        "Invalid query condition syntax tree node: " +
        std::string(typeid(ast).name()));
  }
  return os.str();
}

}  // namespace tiledb::test
