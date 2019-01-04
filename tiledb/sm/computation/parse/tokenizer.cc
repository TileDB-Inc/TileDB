/**
 * @file   tokenizer.cc
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
 * This file defines class Token and Tokenizer.
 */

#include "tiledb/sm/computation/parse/tokenizer.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {
namespace computation {

Token::Token()
    : type_(Type::None) {
}

Token::Token(Type t, const std::string& s)
    : type_(t)
    , value_(s) {
}

bool Token::defined() const {
  return type_ != Type::None;
}

Token::Type Token::type() const {
  return type_;
}

const std::string& Token::value() const {
  return value_;
}

unsigned Token::length() const {
  return value_.size();
}

Token Token::LParen() {
  return Token(Type::LParen, "(");
}

Token Token::RParen() {
  return Token(Type::RParen, ")");
}

Token Token::Plus() {
  return Token(Type::Plus, "+");
}

Token Token::Percent() {
  return Token(Type::Percent, "%");
}

Token Token::Minus() {
  return Token(Type::Minus, "-");
}

Token Token::Star() {
  return Token(Type::Star, "*");
}

Token Token::Slash() {
  return Token(Type::Slash, "/");
}

Token Token::Symbol(const std::string& s) {
  return Token(Type::Symbol, s);
}

Token Token::Int(const std::string& s) {
  return Token(Type::Int, s);
}

Token Token::Fractional(const std::string& s) {
  return Token(Type::Fractional, s);
}

Token Token::EOS() {
  return Token(Type::EOS, "<EOS>");
}

Tokenizer::Tokenizer(const std::string& str)
    : str_(str) {
  offset_ = 0;
  skip_whitespace();
}

bool Tokenizer::is_whitespace(char c) {
  return c == ' ' || c == '\n' || c == '\t' || c == '\r';
}

bool Tokenizer::is_digit(char c) {
  return c >= 0x30 && c <= 0x39;
}

bool Tokenizer::is_alphanumeric(char c) {
  return is_digit(c) || (c >= 0x41 && c <= 0x5a) || (c >= 0x61 && c <= 0x7a);
}

bool Tokenizer::is_symbol_char(char c) {
  return is_alphanumeric(c) || c == '_';
}

bool Tokenizer::is_sign(char c) {
  return c == '-' || c == '+';
}

bool Tokenizer::is_e(char c) {
  return c == 'e' || c == 'E';
}

Status Tokenizer::peek(Token* token) const {
  if (token == nullptr)
    return LOG_STATUS(
        Status::TokenizerError("Cannot peek token; null output variable."));

  if (eos()) {
    *token = Token::EOS();
    return Status::Ok();
  }

  char c = curchar();
  switch (c) {
    case '(':
      *token = Token::LParen();
      return Status::Ok();
    case ')':
      *token = Token::RParen();
      return Status::Ok();
    case '%':
      *token = Token::Percent();
      return Status::Ok();
    case '+':
      *token = Token::Plus();
      return Status::Ok();
    case '-':
      *token = Token::Minus();
      return Status::Ok();
    case '*':
      *token = Token::Star();
      return Status::Ok();
    case '/':
      *token = Token::Slash();
      return Status::Ok();
    default:
      // Do nothing.
      break;
  }

  if (is_digit(c) || c == '.')
    return get_number_token(token);

  return get_symbol_token(token);
}

Status Tokenizer::next() {
  Token t;
  RETURN_NOT_OK(peek(&t));

  if (!t.defined() || t.type() == Token::Type::EOS)
    return Status::Ok();

  if (offset_ + t.length() > str_.size())
    return LOG_STATUS(Status::TokenizerError(
        "Cannot advance token; offset would exceed string length."));

  offset_ += t.length();
  skip_whitespace();

  return Status::Ok();
}

char Tokenizer::curchar() const {
  return str_[offset_];
}

bool Tokenizer::eos() const {
  return offset_ >= str_.size();
}

Status Tokenizer::get_number_token(Token* token) const {
  unsigned idx = offset_;
  std::string token_str;

  // Optional preceding digits.
  while (idx < str_.size() && is_digit(str_[idx]))
    token_str += str_[idx++];

  // '.' with optional preceding digits is tokenized as a fractional number.
  // Digit(s) with following 'e' is also tokenized as fractional.
  if (str_[idx] == '.' || is_e(str_[idx])) {
    bool scientific = is_e(str_[idx]);
    token_str += str_[idx];
    idx++;

    // Optional sign following 'e'
    if (scientific && idx < str_.size() && is_sign(str_[idx]))
      token_str += str_[idx++];

    // Digits following '.' or 'e'
    while (idx < str_.size() && is_digit(str_[idx]))
      token_str += str_[idx++];

    // Fractional scientific e.g. "1.23e+2"
    if (idx < str_.size() && is_e(str_[idx])) {
      token_str += str_[idx++];

      // Some sanity checks
      RETURN_NOT_OK(expect(
          !scientific,
          "Tokenizer error; expected regular fractional preceding 'e'."));
      RETURN_NOT_OK(expect(
          idx < str_.size(),
          "Tokenizer error; unexpected EOS parsing fractional token."));
      RETURN_NOT_OK(expect(
          is_sign(str_[idx]) || is_digit(str_[idx]),
          "Tokenizer error; expected sign or digit following 'e'."));

      // Optional sign following 'e'
      if (is_sign(str_[idx]))
        token_str += str_[idx++];

      RETURN_NOT_OK(expect(
          idx < str_.size() && is_digit(str_[idx]),
          "Tokenizer error; expected digit following sign."));

      // Trailing digits
      while (idx < str_.size() && is_digit(str_[idx]))
        token_str += str_[idx++];
    }

    // Check that the token ends with a digit. The only exception to this is
    // fractional tokens such as "1." which we want to allow.
    char lastchar = token_str[token_str.size() - 1];
    if (!is_digit(lastchar))
      RETURN_NOT_OK(expect(
          is_digit(token_str[0]) && !scientific,
          "Tokenizer error; expected fractional token to end with digit."));

    *token = Token::Fractional(token_str);
    return Status::Ok();
  }

  *token = Token::Int(token_str);
  return Status::Ok();
}

Status Tokenizer::get_symbol_token(Token* token) const {
  std::string token_str;
  unsigned idx = offset_;

  while (idx < str_.size() && is_symbol_char(str_[idx]))
    token_str += str_[idx++];

  *token = Token::Symbol(token_str);
  return Status::Ok();
}

void Tokenizer::skip_whitespace() {
  while (!eos() && is_whitespace(curchar()))
    offset_++;
}

Status Tokenizer::expect(bool cond, const std::string& msg) const {
  if (!cond)
    return LOG_STATUS(Status::TokenizerError(msg));

  return Status::Ok();
}

}  // namespace computation
}  // namespace sm
}  // namespace tiledb
