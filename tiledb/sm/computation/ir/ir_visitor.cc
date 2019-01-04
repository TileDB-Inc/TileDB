/**
 * @file   ir_visitor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This file defines the IRVisitor class.
 */

#include "tiledb/sm/computation/ir/ir_visitor.h"

namespace tiledb {
namespace sm {
namespace computation {

IRVisitor::IRVisitor() {
}

Status IRVisitor::visit(const Name*) {
  return Status::Ok();
}

Status IRVisitor::visit(const BinOp* op) {
  if (op->lhs() != nullptr)
    RETURN_NOT_OK(op->lhs()->accept(this));
  if (op->rhs() != nullptr)
    RETURN_NOT_OK(op->rhs()->accept(this));
  return Status::Ok();
}

Status IRVisitor::visit(const UnOp* op) {
  if (op->expr() != nullptr)
    RETURN_NOT_OK(op->expr()->accept(this));
  return Status::Ok();
}

Status IRVisitor::visit(const Int*) {
  return Status::Ok();
}

Status IRVisitor::visit(const Float*) {
  return Status::Ok();
}

}  // namespace computation
}  // namespace sm
}  // namespace tiledb
