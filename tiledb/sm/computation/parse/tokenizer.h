/**
 * @file   tokenizer.h
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
 * This file declares class Token and Tokenizer.
 */

#ifndef TILEDB_TOKENIZER_H
#define TILEDB_TOKENIZER_H

#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {
namespace computation {

/**
 * A Token represents a string of characters and an associated type.
 */
class Token {
 public:
  enum class Type {
    EOS,
    Int,
    Fractional,
    LParen,
    RParen,
    Percent,
    Plus,
    Minus,
    Star,
    Slash,
    Symbol,
    None
  };

  Token();

  Token(Type t, const std::string& s);

  bool defined() const;

  Type type() const;

  const std::string& value() const;

  unsigned length() const;

  static Token LParen();

  static Token RParen();

  static Token Plus();

  static Token Percent();

  static Token Minus();

  static Token Star();

  static Token Slash();

  static Token Symbol(const std::string& s);

  static Token Int(const std::string& s);

  static Token Fractional(const std::string& s);

  static Token EOS();

 private:
  Type type_;
  std::string value_;
};

/**
 * A Tokenizer converts a string to a stream of Tokens.
 */
class Tokenizer {
 public:
  /** Constructor. */
  explicit Tokenizer(const std::string& str);

  /**
   * Gets the Token at the current head of the stream.
   *
   * @param token Set to the token at the current head of the stream.
   * @return Status
   */
  Status peek(Token* token) const;

  /**
   * Skips the token at the head of the stream.
   *
   * @return Status
   */
  Status next();

 private:
  /**
   * Returns true if the given character is whitespace.
   *
   * whitespace := ' ' | \t | \n | \r
   */
  static inline bool is_whitespace(char c);

  /**
   * Returns true if the given character is a digit.
   *
   * digit := '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9'
   */
  static inline bool is_digit(char c);

  /**
   * Returns true if the given character is alphanumeric.
   *
   * letter := 'a' | 'b' | ... | 'z' | 'A' | 'B' | ... | 'Z'
   * alphanum := letter | digit
   */
  static inline bool is_alphanumeric(char c);

  /**
   * Returns true if the given character can be a part of a symbol (e.g.
   * attribute name).
   *
   * symbolchar := alphanum | '_'
   *
   */
  static inline bool is_symbol_char(char c);

  /**
   * Returns true if the given character is a sign character.
   *
   * sign := '-' | '+'
   */
  static inline bool is_sign(char c);

  /**
   * Returns true if the given character is an E character for scientific
   * notation.
   *
   * e := 'e' | 'E'
   */
  static inline bool is_e(char c);

  /** The string being tokenized. */
  const std::string& str_;

  /** The current character offset. */
  unsigned offset_;

  /** Returns the character at the current offset. */
  char curchar() const;

  /**
   * Parses a numeric token at the current offset. Note that a preceding sign
   * character is not considered part of the token (it is better represented
   * as a unary arithmetic operator in parsing).
   *
   * integral   := digit { digit }
   * fractional := { digit } '.' digit { digit }
   *             | digit '.' { digit }
   * scientific := ( integral | fractional ) e [ sign ] integral
   * numeric    := integral | fractional | scientific
   *
   * @param token Set to the parsed token.
   * @return Status
   */
  Status get_number_token(Token* token) const;

  /**
   * Parses a symbol token at the current offset.
   *
   * symbol := symbolchar { symbolchar }
   *
   * @param token Set to the parsed token.
   * @return Status
   */
  Status get_symbol_token(Token* token) const;

  /** Returns true if the offset is at the end of the string. */
  bool eos() const;

  /** Advances the offset past any whitespace. */
  void skip_whitespace();

  /**
   * Checks the given condition and returns an error status with the given
   * message if it is false.
   *
   * @param cond Condition to check
   * @param msg Error message to return if cond is false.
   * @return Status
   */
  Status expect(bool cond, const std::string& msg) const;
};

}  // namespace computation
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TOKENIZER_H
