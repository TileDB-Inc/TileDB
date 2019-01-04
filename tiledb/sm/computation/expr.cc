/**
 * @file   expr.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file implements class Expr.
 */

#include "tiledb/sm/computation/expr.h"
#include "tiledb/sm/computation/ir/ir_visitor.h"
#include "tiledb/sm/computation/parse/parser.h"

namespace tiledb {
namespace sm {

namespace computation {

/**
 * Simple visitor that verifies whether all Name nodes refer to valid
 * attributes.
 */
class AttributeNameVerifier : public IRVisitor {
 public:
  explicit AttributeNameVerifier(const ArraySchema* array_schema)
      : array_schema_(array_schema)
      , status_(Status::Ok()) {
  }

  Status check_expr(Expr* expr, std::set<std::string>* used_attrs) {
    used_attrs_ = used_attrs;
    status_ = Status::Ok();
    RETURN_NOT_OK(expr->accept(this));
    return status_;
  }

  Status visit(const Name* node) override {
    bool has_attribute;
    RETURN_NOT_OK(array_schema_->has_attribute(node->name(), &has_attribute));
    if (!has_attribute) {
      status_ = Status::ExprError(
          "Expression refers to attribute '" + node->name() +
          "' but array schema contains no such attribute.");
    }
    used_attrs_->insert(node->name());
    return Status::Ok();
  }

 private:
  const ArraySchema* array_schema_;

  std::set<std::string>* used_attrs_;

  Status status_;
};

}  // namespace computation

Expr::Expr() {
}

Status Expr::compile(const ArraySchema* array_schema) {
  computation::Parser parser(str_);
  RETURN_NOT_OK(parser.parse(&expr_));

  computation::AttributeNameVerifier check_names(array_schema);
  RETURN_NOT_OK(check_names.check_expr(expr_.get(), &attributes_required_));

  return Status::Ok();
}

Status Expr::set_expr(const std::string& str) {
  str_ = str;
  return Status::Ok();
}

const std::set<std::string>& Expr::attributes_required() const {
  return attributes_required_;
}

const computation::Expr* Expr::expr() const {
  return expr_.get();
}

}  // namespace sm
}  // namespace tiledb
