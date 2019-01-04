/**
 * @file   parser.cc
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
 * This file defines class Parser.
 */

#include "tiledb/sm/computation/parse/parser.h"
#include "tiledb/sm/misc/logger.h"

namespace tiledb {
namespace sm {
namespace computation {

Parser::Parser(const std::string& str)
    : tokenizer_(str) {
}

Status Parser::parse(std::unique_ptr<Expr>* e) {
  return expr(e);
}

Status Parser::expr(std::unique_ptr<Expr>* e) {
  if (e == nullptr)
    return LOG_STATUS(
        Status::ParserError("Parsing error; null output variable."));

  RETURN_NOT_OK(addsub(e));

  return Status::Ok();
}

Status Parser::addsub(std::unique_ptr<Expr>* e) {
  RETURN_NOT_OK(muldiv(e));

  Token t;
  RETURN_NOT_OK(tokenizer_.peek(&t));
  while (t.type() == Token::Type::Plus || t.type() == Token::Type::Minus) {
    RETURN_NOT_OK(tokenizer_.next());

    std::unique_ptr<Expr> rhs;
    RETURN_NOT_OK(addsub(&rhs));

    BinOp::Operator op;
    switch (t.type()) {
      case Token::Type::Plus:
        op = BinOp::Operator::Add;
        break;
      case Token::Type::Minus:
        op = BinOp::Operator::Sub;
        break;
      default:
        return LOG_STATUS(
            Status::ParserError("Unexpected token while parsing addsub node."));
    }
    *e = BinOp::create(op, std::move(*e), std::move(rhs));
    RETURN_NOT_OK(tokenizer_.peek(&t));
  }

  return Status::Ok();
}

Status Parser::muldiv(std::unique_ptr<Expr>* e) {
  RETURN_NOT_OK(unop(e));

  Token t;
  RETURN_NOT_OK(tokenizer_.peek(&t));
  while (t.type() == Token::Type::Star || t.type() == Token::Type::Slash ||
         t.type() == Token::Type::Percent) {
    RETURN_NOT_OK(tokenizer_.next());

    std::unique_ptr<Expr> rhs;
    RETURN_NOT_OK(muldiv(&rhs));

    BinOp::Operator op;
    switch (t.type()) {
      case Token::Type::Star:
        op = BinOp::Operator::Mul;
        break;
      case Token::Type::Slash:
        op = BinOp::Operator::Div;
        break;
      case Token::Type::Percent:
        op = BinOp::Operator::Mod;
        break;
      default:
        return LOG_STATUS(
            Status::ParserError("Unexpected token while parsing muldiv node."));
    }
    *e = BinOp::create(op, std::move(*e), std::move(rhs));
    RETURN_NOT_OK(tokenizer_.peek(&t));
  }

  return Status::Ok();
}

Status Parser::unop(std::unique_ptr<Expr>* e) {
  Token t;
  RETURN_NOT_OK(tokenizer_.peek(&t));

  if (t.type() != Token::Type::Plus && t.type() != Token::Type::Minus) {
    RETURN_NOT_OK(fragment(e));
    return Status::Ok();
  }

  while (t.type() == Token::Type::Plus || t.type() == Token::Type::Minus) {
    switch (t.type()) {
      case Token::Type::Plus:
        RETURN_NOT_OK(tokenizer_.next());
        RETURN_NOT_OK(unop(e));
        *e = UnOp::create(UnOp::Operator::Add, std::move(*e));
        break;
      case Token::Type::Minus:
        RETURN_NOT_OK(tokenizer_.next());
        RETURN_NOT_OK(unop(e));
        *e = UnOp::create(UnOp::Operator::Sub, std::move(*e));
        break;
      default:
        return LOG_STATUS(
            Status::ParserError("Unexpected token while parsing unop node."));
    }
    RETURN_NOT_OK(tokenizer_.peek(&t));
  }

  return Status::Ok();
}

Status Parser::fragment(std::unique_ptr<Expr>* e) {
  Token t;
  RETURN_NOT_OK(tokenizer_.peek(&t));

  if (t.type() == Token::Type::LParen) {
    // '(' expr ')'
    RETURN_NOT_OK(tokenizer_.next());
    RETURN_NOT_OK(expr(e));
    RETURN_NOT_OK(consume_token(Token::Type::RParen, "Unmatched '('."));
  } else {
    RETURN_NOT_OK(atom(e));
    RETURN_NOT_OK(tokenizer_.peek(&t));
    if (t.type() == Token::Type::LParen) {
      // The current 'e' atom is the function name.
      RETURN_NOT_OK(funcall(std::move(*e), e));
    }
  }

  return Status::Ok();
}

Status Parser::funcall(std::unique_ptr<Expr> name, std::unique_ptr<Expr>* e) {
  (void)name;
  (void)e;
  return LOG_STATUS(Status::ParserError("Unimplemented node: funcall"));
}

Status Parser::atom(std::unique_ptr<Expr>* e) {
  Token t;
  RETURN_NOT_OK(tokenizer_.peek(&t));
  switch (t.type()) {
    case Token::Type::Int:
    case Token::Type::Fractional:
      RETURN_NOT_OK(number(t, e));
      break;
    case Token::Type::Symbol:
      RETURN_NOT_OK(name(t, e));
      break;
    default:
      return LOG_STATUS(Status::ParserError(
          "Parsing error; unexpected token '" + t.value() +
          "' when parsing atom."));
  }
  RETURN_NOT_OK(tokenizer_.next());
  return Status::Ok();
}

Status Parser::number(const Token& token, std::unique_ptr<Expr>* e) const {
  switch (token.type()) {
    case Token::Type::Int:
      *e = Int::create(std::stoll(token.value()));
      break;
    case Token::Type::Fractional:
      *e = Float::create(std::stod(token.value()));
      break;
    default:
      return LOG_STATUS(Status::ParserError(
          "Parsing error; unexpected token '" + token.value() +
          "' when parsing number."));
  }

  return Status::Ok();
}

Status Parser::name(const Token& token, std::unique_ptr<Expr>* e) const {
  switch (token.type()) {
    case Token::Type::Symbol:
      *e = Name::create(token.value());
      break;
    default:
      return LOG_STATUS(Status::ParserError(
          "Parsing error; unexpected token '" + token.value() +
          "' when parsing name."));
  }

  return Status::Ok();
}

Status Parser::expect(bool cond, const std::string& err_msg) const {
  if (!cond)
    return LOG_STATUS(Status::ParserError("Parsing error; " + err_msg));

  return Status::Ok();
}

Status Parser::consume_token(Token::Type type, const std::string& err_msg) {
  Token t;
  RETURN_NOT_OK(tokenizer_.peek(&t));
  RETURN_NOT_OK(expect(t.type() == type, err_msg));
  RETURN_NOT_OK(tokenizer_.next());
  return Status::Ok();
}

}  // namespace computation
}  // namespace sm
}  // namespace tiledb
