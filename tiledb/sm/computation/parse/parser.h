/**
 * @file   parser.h
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
 * This file declares class Parser.
 */

#ifndef TILEDB_PARSER_H
#define TILEDB_PARSER_H

#include "tiledb/sm/computation/ir/ir.h"
#include "tiledb/sm/computation/parse/tokenizer.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {
namespace computation {

/**
 * Parses a string in the TileDB arithmetic language into an IR.
 *
 * The high-level grammar of the language is:
 *
 * digit      := '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9'
 * sign       := '-' | '+'
 * e          := 'e' | 'E'
 * integral   := digit { digit }
 * fractional := { digit } '.' digit { digit }
 *             | digit '.' { digit }
 * scientific := ( integral | fractional ) e [ sign ] integral
 * number     := integral | fractional | scientific
 *
 * letter     := 'a' | 'b' | ... | 'z' | 'A' | 'B' | ... | 'Z'
 * alphanum   := letter | digit
 * name       := ( letter | '_' ) { alphanum | '_' }
 *
 * expr       := addsub
 *
 * exprlist   := expr { ',' expr }
 *
 * addsub     := addsub ( '+' | '-' ) muldiv
 *             | muldiv
 *
 * muldiv     := muldiv ( '*' | '/' | '%' ) unop
 *             | unop
 *
 * unop       := [ '-' | '+' ] fragment
 *
 * fragment   := '(' expr ')'
 *             | funcall
 *             | atom
 *
 * funcall    := name '(' exprlist ')'
 *
 * atom       := number
 *             | name
 *
 * Note the parser implements some changes to this grammar internally to
 * eliminate left-recursion.
 *
 */
class Parser {
 public:
  Parser(const std::string& str);

  Status parse(std::unique_ptr<Expr>* e);

 private:
  Tokenizer tokenizer_;

  /** Parses an Expr. */
  Status expr(std::unique_ptr<Expr>* e);

  /** Parses an `addsub` nonterminal. */
  Status addsub(std::unique_ptr<Expr>* e);

  /** Parses a `muldiv` nonterminal. */
  Status muldiv(std::unique_ptr<Expr>* e);

  /** Parses a `unop` nonterminal. */
  Status unop(std::unique_ptr<Expr>* e);

  /** Parses a `fragment` nonterminal. */
  Status fragment(std::unique_ptr<Expr>* e);

  /** Parses a `funcall` nonterminal. */
  Status funcall(std::unique_ptr<Expr> name, std::unique_ptr<Expr>* e);

  /** Parses an `atom` nonterminal. */
  Status atom(std::unique_ptr<Expr>* e);

  /**
   * Checks the given condition and returns an error status with the given
   * message if it is false.
   *
   * @param cond Condition to check
   * @param err_msg Error message to return if cond is false.
   * @return Status
   */
  Status expect(bool cond, const std::string& err_msg) const;

  /**
   * Advances the tokenizer past the current token, throwing an error if the
   * current token's type is not the given type.
   *
   * @param type Type to compare against current token
   * @param err_msg Error message if type does not match
   * @return Status.
   */
  Status consume_token(Token::Type type, const std::string& err_msg);

  /**
   * Parses a number from the given token.
   */
  Status number(const Token& token, std::unique_ptr<Expr>* e) const;

  /**
   * Parses a name from the given token.
   */
  Status name(const Token& token, std::unique_ptr<Expr>* e) const;
};

}  // namespace computation

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_PARSER_H
