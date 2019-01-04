/**
 * @file   ir.h
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
 * This file declares the arithmetic query expression IR.
 */

#ifndef TILEDB_IR_H
#define TILEDB_IR_H

#include <memory>

#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {
namespace computation {

class IRVisitor;

class IRNode {
 public:
  enum class Type {
    Undefined,
    BinOp,
    UnOp,
    Name,
    Int,
    Float,
  };

  IRNode();

  IRNode(Type type);

  virtual ~IRNode() = default;

  virtual Status accept(IRVisitor* visitor) const = 0;

  Type type() const;

 protected:
  Type type_;
};

class Expr : public IRNode {
 public:
  Expr();

  Expr(IRNode::Type type);

  virtual ~Expr() = default;

  Status accept(IRVisitor* visitor) const override;

 private:
};

class Name : public Expr {
 public:
  static std::unique_ptr<Name> create(const std::string& attribute);

  Status accept(IRVisitor* visitor) const override;

  const std::string& name() const;

 private:
  Name();

  std::string name_;
};

class Int : public Expr {
 public:
  static std::unique_ptr<Int> create(int64_t value);

  Status accept(IRVisitor* visitor) const override;

  int64_t value() const;

 private:
  Int();

  int64_t value_;
};

class Float : public Expr {
 public:
  static std::unique_ptr<Float> create(double value);

  Status accept(IRVisitor* visitor) const override;

 private:
  Float();

  double value_;
};

class BinOp : public Expr {
 public:
  enum class Operator { Add, Sub, Mul, Div, Mod };

  static std::unique_ptr<BinOp> create(
      Operator op, std::unique_ptr<Expr> lhs, std::unique_ptr<Expr> rhs);

  Status accept(IRVisitor* visitor) const override;

  Operator op() const;

  const Expr* lhs() const;

  const Expr* rhs() const;

 private:
  BinOp();

  Operator op_;
  std::unique_ptr<Expr> lhs_;
  std::unique_ptr<Expr> rhs_;
};

class UnOp : public Expr {
 public:
  enum class Operator { Add, Sub };

  static std::unique_ptr<UnOp> create(Operator op, std::unique_ptr<Expr> expr);

  Status accept(IRVisitor* visitor) const override;

  Operator op() const;

  const Expr* expr() const;

 private:
  UnOp();

  Operator op_;
  std::unique_ptr<Expr> expr_;
};

}  // namespace computation
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_EXPR_H
