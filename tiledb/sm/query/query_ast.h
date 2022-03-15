/**
 * @file query_ast.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * Defines the Query AST node classes.
 */

#ifndef TILEDB_QUERY_AST_H
#define TILEDB_QUERY_AST_H

#include <sstream>

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
enum ASTNodeTag : char { VAL, EXPR };

/** Represents a simple terminal/predicate **/
class ASTNode {
 public:
  ASTNode(ASTNodeTag tag)
      : tag_(tag) {
  }
  virtual ASTNodeTag get_tag() const {
    return tag_;
  }
  virtual ~ASTNode() {
  }
  ASTNodeTag tag_;
};
class ASTNodeVal : public ASTNode {
  /** Value constructor. */
 public:
  ASTNodeVal(
      std::string field_name,
      const void* const condition_value,
      const uint64_t condition_value_size,
      const QueryConditionOp op)
      : ASTNode(ASTNodeTag::VAL)
      , field_name_(field_name)
      , op_(op) {
    condition_value_data_.resize(condition_value_size);
    condition_value_ = nullptr;

    if (condition_value != nullptr) {
      // Using an empty string litteral for empty string as vector::resize(0)
      // doesn't guarantee an allocation.
      condition_value_ =
          condition_value_size == 0 ? (void*)"" : condition_value_data_.data();
      memcpy(
          condition_value_data_.data(), condition_value, condition_value_size);
    }
  }

  /** Copy constructor. */
  ASTNodeVal(const ASTNodeVal& rhs)
      : ASTNode(ASTNodeTag::VAL)
      , field_name_(rhs.field_name_)
      , condition_value_data_(rhs.condition_value_data_)
      , condition_value_(
            rhs.condition_value_ == nullptr ? nullptr :
                                              condition_value_data_.data())
      , op_(rhs.op_) {
  }

  /** Move constructor. */
  ASTNodeVal(ASTNodeVal&& rhs)
      : ASTNode(ASTNodeTag::VAL)
      , field_name_(std::move(rhs.field_name_))
      , condition_value_data_(std::move(rhs.condition_value_data_))
      , condition_value_(
            rhs.condition_value_ == nullptr ? nullptr :
                                              condition_value_data_.data())
      , op_(rhs.op_) {
  }

  /** Assignment operator. */
  ASTNodeVal& operator=(const ASTNodeVal& rhs) {
    field_name_ = rhs.field_name_;
    condition_value_data_ = rhs.condition_value_data_;
    condition_value_ = rhs.condition_value_ == nullptr ?
                           nullptr :
                           condition_value_data_.data();
    op_ = rhs.op_;
    tag_ = ASTNodeTag::VAL;

    return *this;
  }

  /** Move-assignment operator. */
  ASTNodeVal& operator=(ASTNodeVal&& rhs) {
    field_name_ = std::move(rhs.field_name_);
    condition_value_data_ = std::move(rhs.condition_value_data_);
    condition_value_ = rhs.condition_value_ == nullptr ?
                           nullptr :
                           condition_value_data_.data();
    op_ = rhs.op_;
    tag_ = ASTNodeTag::VAL;

    return *this;
  }

  ASTNodeTag get_tag() const override {
    return tag_;
  }

  /** The attribute name. */
  std::string field_name_;

  /** The value data. */
  ByteVecValue condition_value_data_;

  /** The value to compare against. */
  void* condition_value_;

  /** The comparison operator. */
  QueryConditionOp op_;
};

class ASTNodeExpr : public ASTNode {
 public:
  ASTNodeExpr(
      std::vector<shared_ptr<ASTNode>> nodes, QueryConditionCombinationOp c_op)
      : ASTNode(ASTNodeTag::EXPR)
      , nodes_(nodes)
      , combination_op_(c_op) {
  }

  /** Copy constructor. */
  ASTNodeExpr(const ASTNodeExpr& rhs)
      : ASTNode(ASTNodeTag::EXPR)
      , nodes_(rhs.nodes_)
      , combination_op_(rhs.combination_op_){};

  /** Move constructor. */
  ASTNodeExpr(ASTNodeExpr&& rhs)
      : ASTNode(ASTNodeTag::EXPR)
      , nodes_(std::move(rhs.nodes_))
      , combination_op_(rhs.combination_op_){};

  /** Assignment operator. */
  ASTNodeExpr& operator=(const ASTNodeExpr& rhs) {
    nodes_ = rhs.nodes_;
    combination_op_ = rhs.combination_op_;
    tag_ = ASTNodeTag::EXPR;
    return *this;
  }

  /** Move-assignment operator. */
  ASTNodeExpr& operator=(ASTNodeExpr&& rhs) {
    nodes_ = std::move(rhs.nodes_);
    combination_op_ = rhs.combination_op_;
    tag_ = ASTNodeTag::EXPR;
    return *this;
  }

  ASTNodeTag get_tag() const override {
    return tag_;
  }

  /** The node list **/
  std::vector<shared_ptr<ASTNode>> nodes_;

  /** The combination operation **/
  QueryConditionCombinationOp combination_op_;
};
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_AST_H