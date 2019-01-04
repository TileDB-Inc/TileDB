/**
 * @file   expr.h
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
 * This file defines class Expr.
 */

#ifndef TILEDB_EXPR_H
#define TILEDB_EXPR_H

#include <set>

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/computation/ir/ir.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

class Expr {
 public:
  Expr();

  Status compile(const ArraySchema* array_schema);

  Status set_expr(const std::string& str);

  const std::set<std::string>& attributes_required() const;

  const computation::Expr* expr() const;

 private:
  std::string str_;

  std::unique_ptr<computation::Expr> expr_;

  std::set<std::string> attributes_required_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_EXPR_H
