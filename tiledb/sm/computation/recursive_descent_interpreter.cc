/**
 * @file   recursive_descent_interpreter.cc
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
 * This file implements class RecursiveDescentInterpreter.
 */

#include "tiledb/sm/computation/recursive_descent_interpreter.h"

namespace tiledb {
namespace sm {
namespace computation {

RecursiveDescentInterpreter::RecursiveDescentInterpreter() {
}

Status RecursiveDescentInterpreter::interpret(
    const Expr* expr, Environment* env) {
  env_ = env;

  // Interpret the IR.
  // TODO: This allocates temporary storage at each node. Some sort of register
  //       pool would likely be more appropriate. Because the IR is just a
  //       single expr tree, we might be able to use e.g. Ershov numbering.
  //       Ref: Aho, Lam et al 2nd edition section 8.10.
  TypedBuffer result;
  RETURN_NOT_OK(interpret(expr, &result));

  // Copy the result into the output buffer
  Buffer* output;
  RETURN_NOT_OK(env_->output(&output));
  if (output->owns_data()) {
    output->reset_size();
    output->reset_offset();
    RETURN_NOT_OK(output->write(result.buffer.data(), result.buffer.size()));
  } else {
    if (result.buffer.size() > output->size())
      return LOG_STATUS(Status::ExprError(
          "Cannot interpret expr; output buffer not large enough for result."));
    std::memcpy(output->data(), result.buffer.data(), result.buffer.size());
  }

  return Status::Ok();
}

Status RecursiveDescentInterpreter::interpret(
    const Expr* expr, TypedBuffer* result) {
  switch (expr->type()) {
    case IRNode::Type::Int:
      RETURN_NOT_OK(interpret(dynamic_cast<const Int*>(expr), result));
      break;
    case IRNode::Type::Name:
      RETURN_NOT_OK(interpret(dynamic_cast<const Name*>(expr), result));
      break;
    case IRNode::Type::BinOp:
      RETURN_NOT_OK(interpret(dynamic_cast<const BinOp*>(expr), result));
      break;
    default:
      return LOG_STATUS(
          Status::ExprError("Cannot interpret expr; unknown IR node type."));
  }

  return Status::Ok();
}

Status RecursiveDescentInterpreter::interpret(
    const Int* imm, TypedBuffer* result) {
  // TODO: handle other integer widths
  using T = int32_t;
  const Datatype datatype = Datatype::INT32;
  const T value = static_cast<T>(imm->value());

  // Resize the buffer
  const auto num_cells = env_->num_cells();
  const auto type_size = datatype_size(datatype);
  RETURN_NOT_OK(result->buffer.realloc(num_cells * type_size));

  // Materialize the integer value into the result buffer
  auto* dest = static_cast<T*>(result->buffer.data());
  for (uint64_t i = 0; i < num_cells; i++)
    dest[i] = value;

  result->datatype = datatype;
  result->buffer.set_size(num_cells * type_size);

  return Status::Ok();
}

Status RecursiveDescentInterpreter::interpret(
    const Name* name, TypedBuffer* result) {
  Datatype datatype = Datatype::UINT8;
  ConstBuffer src(nullptr, 0);
  if (!env_->lookup(name->name(), &datatype, &src))
    return LOG_STATUS(Status::ExprError(
        "Cannot interpret expr; unknown reference to '" + name->name() + "'."));

  // Resize the buffer
  const auto num_cells = env_->num_cells();
  const auto type_size = datatype_size(datatype);
  RETURN_NOT_OK(result->buffer.realloc(num_cells * type_size));

  // Materialize the attribute value into the result buffer
  result->buffer.reset_offset();
  RETURN_NOT_OK(result->buffer.write(&src));
  result->datatype = datatype;

  return Status::Ok();
}

Status RecursiveDescentInterpreter::interpret(
    const BinOp* op, TypedBuffer* result) {
  TypedBuffer lhs, rhs;
  RETURN_NOT_OK(interpret(op->lhs(), &lhs));
  RETURN_NOT_OK(interpret(op->rhs(), &rhs));

  // TODO: handling arbitrary lhs/rhs datatypes will require some sort of
  //   code generation (either statically by generating all permutations of
  //   arithmetic ops, or at runtime with e.g. LLVM).
  if (lhs.datatype != Datatype::INT32 || rhs.datatype != Datatype::INT32)
    return LOG_STATUS(
        Status::ExprError("Cannot interpret expr; arithmetic only implemented "
                          "on int32 attributes."));

  const auto datatype = Datatype::INT32;
  const auto num_cells = env_->num_cells();
  RETURN_NOT_OK(result->buffer.realloc(num_cells * datatype_size(datatype)));

  const auto* x = static_cast<const int32_t*>(lhs.buffer.data());
  const auto* y = static_cast<const int32_t*>(rhs.buffer.data());
  auto* r = static_cast<int32_t*>(result->buffer.data());

  switch (op->op()) {
    case BinOp::Operator::Add: {
      for (uint64_t i = 0; i < num_cells; i++) {
        r[i] = x[i] + y[i];
      }
      break;
    }
    case BinOp::Operator::Sub: {
      for (uint64_t i = 0; i < num_cells; i++) {
        r[i] = x[i] - y[i];
      }
      break;
    }
    case BinOp::Operator::Mul: {
      for (uint64_t i = 0; i < num_cells; i++) {
        r[i] = x[i] * y[i];
      }
      break;
    }
    case BinOp::Operator::Div: {
      for (uint64_t i = 0; i < num_cells; i++) {
        r[i] = x[i] / y[i];
      }
      break;
    }
    case BinOp::Operator::Mod: {
      for (uint64_t i = 0; i < num_cells; i++) {
        r[i] = x[i] % y[i];
      }
      break;
    }
    default:
      return LOG_STATUS(
          Status::ExprError("Cannot interpret expr; unhandled binop type."));
  }

  result->datatype = datatype;
  result->buffer.reset_offset();
  result->buffer.set_size(num_cells * datatype_size(datatype));

  return Status::Ok();
}

}  // namespace computation
}  // namespace sm
}  // namespace tiledb
