/**
 * @file unit-computation-parser.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests the computation tokenizer/parser.
 */

#include "tiledb/sm/computation/parse/parser.h"
#include "tiledb/sm/computation/parse/tokenizer.h"

#include <catch.hpp>

using namespace tiledb::sm::computation;

TEST_CASE("Computation: Test tokenizer (simple)", "[computation]") {
  SECTION(" - Numbers") {
    Token t;
    REQUIRE(Tokenizer("123").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Int);
    REQUIRE(t.value() == "123");

    REQUIRE(Tokenizer("1.23").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Fractional);
    REQUIRE(t.value() == "1.23");
    REQUIRE(Tokenizer(".23").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Fractional);
    REQUIRE(t.value() == ".23");
    REQUIRE(Tokenizer("0.23").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Fractional);
    REQUIRE(t.value() == "0.23");
    REQUIRE(Tokenizer("23.").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Fractional);
    REQUIRE(t.value() == "23.");

    REQUIRE(Tokenizer("1.23e6").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Fractional);
    REQUIRE(t.value() == "1.23e6");
    REQUIRE(Tokenizer("1.23e61").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Fractional);
    REQUIRE(t.value() == "1.23e61");
    REQUIRE(Tokenizer(".23e6").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Fractional);
    REQUIRE(t.value() == ".23e6");
    REQUIRE(Tokenizer("1.23e-16").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Fractional);
    REQUIRE(t.value() == "1.23e-16");
    REQUIRE(Tokenizer("1.23e+16").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Fractional);
    REQUIRE(t.value() == "1.23e+16");
    REQUIRE(Tokenizer("1.23E-6").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Fractional);
    REQUIRE(t.value() == "1.23E-6");

    REQUIRE(!Tokenizer(".").peek(&t).ok());
    REQUIRE(!Tokenizer("1.23e").peek(&t).ok());
    REQUIRE(!Tokenizer("1.23e/2").peek(&t).ok());
    REQUIRE(!Tokenizer("1e").peek(&t).ok());
  }

  SECTION(" - Symbols") {
    Token t;
    REQUIRE(Tokenizer("x").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Symbol);
    REQUIRE(t.value() == "x");
    REQUIRE(Tokenizer("abc").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Symbol);
    REQUIRE(t.value() == "abc");
    REQUIRE(Tokenizer("_abc").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Symbol);
    REQUIRE(t.value() == "_abc");
    REQUIRE(Tokenizer("abc_").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Symbol);
    REQUIRE(t.value() == "abc_");
    REQUIRE(Tokenizer("abc_def").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Symbol);
    REQUIRE(t.value() == "abc_def");
    REQUIRE(Tokenizer("abc123").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Symbol);
    REQUIRE(t.value() == "abc123");
    REQUIRE(Tokenizer("abc123_def").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Symbol);
    REQUIRE(t.value() == "abc123_def");
    REQUIRE(Tokenizer("abc_1").peek(&t).ok());
    REQUIRE(t.type() == Token::Type::Symbol);
    REQUIRE(t.value() == "abc_1");
  }
}

TEST_CASE("Computation: Test parser (simple)", "[computation]") {
  std::unique_ptr<Expr> e;

  SECTION("- Atoms") {
    REQUIRE(Parser("(123)").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::Int);
    REQUIRE(Parser("1.23e16").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::Float);
    REQUIRE(Parser("((((1.23e16))))").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::Float);
    REQUIRE(Parser("1.23e-16").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::Float);
    REQUIRE(Parser("1.23e+16").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::Float);
    REQUIRE(Parser("a").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::Name);
    REQUIRE(Parser("abc").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::Name);
    REQUIRE(Parser("  a_bc").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::Name);
    REQUIRE(Parser("var12").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::Name);
  }

  SECTION("- Unary operators") {
    REQUIRE(Parser("-2").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::UnOp);
    auto unop = static_cast<UnOp*>(e.get());
    REQUIRE(unop->op() == UnOp::Operator::Sub);
    REQUIRE(unop->expr()->type() == IRNode::Type::Int);
    REQUIRE(static_cast<const Int*>(unop->expr())->value() == 2);

    REQUIRE(Parser("+3").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::UnOp);
    unop = static_cast<UnOp*>(e.get());
    REQUIRE(unop->op() == UnOp::Operator::Add);
    REQUIRE(unop->expr()->type() == IRNode::Type::Int);
    REQUIRE(static_cast<const Int*>(unop->expr())->value() == 3);

    REQUIRE(Parser("- (-+---+((34)))").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::UnOp);
    unop = static_cast<UnOp*>(e.get());
    REQUIRE(unop->op() == UnOp::Operator::Sub);
    REQUIRE(unop->expr()->type() == IRNode::Type::UnOp);

    REQUIRE(Parser("- (-+---+((3 * 4 + 2)))").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::UnOp);
    unop = static_cast<UnOp*>(e.get());
    REQUIRE(unop->op() == UnOp::Operator::Sub);
    REQUIRE(unop->expr()->type() == IRNode::Type::UnOp);
  }

  SECTION("- Binary operators") {
    REQUIRE(Parser("1 + 2").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::BinOp);
    auto binop = static_cast<BinOp*>(e.get());
    REQUIRE(binop->op() == BinOp::Operator::Add);
    REQUIRE(binop->lhs()->type() == IRNode::Type::Int);
    REQUIRE(static_cast<const Int*>(binop->lhs())->value() == 1);
    REQUIRE(binop->rhs()->type() == IRNode::Type::Int);
    REQUIRE(static_cast<const Int*>(binop->rhs())->value() == 2);

    REQUIRE(Parser("1 * 2").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::BinOp);
    binop = static_cast<BinOp*>(e.get());
    REQUIRE(binop->op() == BinOp::Operator::Mul);

    REQUIRE(Parser("2 * 3 + 4").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::BinOp);
    binop = static_cast<BinOp*>(e.get());
    REQUIRE(binop->op() == BinOp::Operator::Add);

    REQUIRE(Parser("2 * (3 + 4)").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::BinOp);
    binop = static_cast<BinOp*>(e.get());
    REQUIRE(binop->op() == BinOp::Operator::Mul);

    REQUIRE(Parser("2 * var1 + var2").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::BinOp);
    binop = static_cast<BinOp*>(e.get());
    REQUIRE(binop->op() == BinOp::Operator::Add);

    // Same precedence -> leftmost wins
    REQUIRE(Parser("2 * 3 / 2").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::BinOp);
    binop = static_cast<BinOp*>(e.get());
    REQUIRE(binop->op() == BinOp::Operator::Mul);

    REQUIRE(Parser("2 * 3 - (10 % 2)").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::BinOp);
    binop = static_cast<BinOp*>(e.get());
    REQUIRE(binop->op() == BinOp::Operator::Sub);

    REQUIRE(Parser("2 * +3 / - 2").parse(&e).ok());
    REQUIRE(e->type() == IRNode::Type::BinOp);
    binop = static_cast<BinOp*>(e.get());
    REQUIRE(binop->op() == BinOp::Operator::Mul);
  }
}