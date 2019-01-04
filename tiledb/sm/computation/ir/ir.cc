/**
 * @file   ir.cc
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
 * This file defines the arithmetic query expression IR.
 */

#include "tiledb/sm/computation/ir/ir.h"
#include "tiledb/sm/computation/ir/ir_visitor.h"

namespace tiledb {
namespace sm {
namespace computation {

IRNode::IRNode()
    : type_(Type::Undefined) {
}

IRNode::IRNode(Type type)
    : type_(type) {
}

IRNode::Type IRNode::type() const {
  return type_;
}

Expr::Expr()
    : IRNode() {
}

Expr::Expr(IRNode::Type type)
    : IRNode(type) {
}

Status Expr::accept(IRVisitor* visitor) const {
  (void)visitor;
  return Status::ExprError("Visiting Expr base class is ambiguous.");
}

Name::Name()
    : Expr(IRNode::Type::Name) {
}

std::unique_ptr<Name> Name::create(const std::string& attribute) {
  auto node = new Name;
  node->name_ = attribute;
  return std::unique_ptr<Name>(node);
}

Status Name::accept(IRVisitor* visitor) const {
  return visitor->visit(this);
}

const std::string& Name::name() const {
  return name_;
}

Int::Int()
    : Expr(IRNode::Type::Int) {
}

std::unique_ptr<Int> Int::create(int64_t value) {
  auto node = new Int;
  node->value_ = value;
  return std::unique_ptr<Int>(node);
}

Status Int::accept(IRVisitor* visitor) const {
  return visitor->visit(this);
}

int64_t Int::value() const {
  return value_;
}

Float::Float()
    : Expr(IRNode::Type::Float) {
}

std::unique_ptr<Float> Float::create(double value) {
  auto node = new Float;
  node->value_ = value;
  return std::unique_ptr<Float>(node);
}

Status Float::accept(IRVisitor* visitor) const {
  return visitor->visit(this);
}

BinOp::BinOp()
    : Expr(IRNode::Type::BinOp) {
}

std::unique_ptr<BinOp> BinOp::create(
    Operator op, std::unique_ptr<Expr> lhs, std::unique_ptr<Expr> rhs) {
  std::unique_ptr<BinOp> node(new BinOp);
  node->op_ = op;
  node->lhs_ = std::move(lhs);
  node->rhs_ = std::move(rhs);
  return node;
}

Status BinOp::accept(IRVisitor* visitor) const {
  return visitor->visit(this);
}

BinOp::Operator BinOp::op() const {
  return op_;
}

const Expr* BinOp::lhs() const {
  return lhs_.get();
}

const Expr* BinOp::rhs() const {
  return rhs_.get();
}

UnOp::UnOp()
    : Expr(IRNode::Type::UnOp) {
}

std::unique_ptr<UnOp> UnOp::create(Operator op, std::unique_ptr<Expr> expr) {
  std::unique_ptr<UnOp> node(new UnOp);
  node->op_ = op;
  node->expr_ = std::move(expr);
  return node;
}

Status UnOp::accept(IRVisitor* visitor) const {
  return visitor->visit(this);
}

UnOp::Operator UnOp::op() const {
  return op_;
}

const Expr* UnOp::expr() const {
  return expr_.get();
}

}  // namespace computation
}  // namespace sm
}  // namespace tiledb
