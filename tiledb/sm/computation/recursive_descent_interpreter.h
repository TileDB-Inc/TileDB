/**
 * @file   recursive_descent_interpreter.h
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
 * This file declares class RecursiveDescentInterpreter.
 */

#ifndef TILEDB_RECURSIVE_DESCENT_INTERPRETER_H
#define TILEDB_RECURSIVE_DESCENT_INTERPRETER_H

#include <unordered_map>

#include "tiledb/sm/computation/expr.h"
#include "tiledb/sm/computation/expr_executor.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/query/types.h"

namespace tiledb {
namespace sm {
namespace computation {

class RecursiveDescentInterpreter {
 public:
  RecursiveDescentInterpreter();

  Status interpret(const Expr* expr, Environment* env);

 private:
  struct TypedBuffer {
    Buffer buffer;
    Datatype datatype;
  };

  Environment* env_;

  Status interpret(const Expr* imm, TypedBuffer* result);

  Status interpret(const Int* imm, TypedBuffer* result);

  Status interpret(const Name* name, TypedBuffer* result);

  Status interpret(const BinOp* name, TypedBuffer* result);
};

}  // namespace computation
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RECURSIVE_DESCENT_INTERPRETER_H
