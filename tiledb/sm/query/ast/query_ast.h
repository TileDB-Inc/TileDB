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

#include <iostream>
#include <memory>
#include <sstream>
#include <unordered_set>
#include <utility>

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
enum ASTNodeTag : char { NIL, VAL, EXPR };

class ASTNode {
 public:
  virtual ASTNodeTag get_tag() = 0;
  virtual std::string to_str() = 0;
  virtual tdb_unique_ptr<ASTNode> clone() = 0;
  virtual ~ASTNode() {
  }
};
/** Represents a simple terminal/predicate **/
class ASTNodeVal : public ASTNode {
  /** Value constructor. */
 public:
  ASTNodeVal(
      std::string field_name,
      const void* const condition_value,
      const uint64_t condition_value_size,
      const QueryConditionOp op)
      : field_name_(field_name)
      , op_(op) {
    condition_value_data_.resize(condition_value_size);
    condition_value_ = nullptr;

    if (condition_value != nullptr) {
      // Using an empty string literal for empty string as vector::resize(0)
      // doesn't guarantee an allocation.
      condition_value_ = condition_value_size == 0 ?
                               (void*)"" :
                               condition_value_data_.data();
      memcpy(
          condition_value_data_.data(), condition_value, condition_value_size);
    }
  }

  ~ASTNodeVal() {
  }

  /** Copy constructor. */
  ASTNodeVal(const ASTNodeVal& rhs)
      : field_name_(rhs.field_name_)
      , condition_value_data_(rhs.condition_value_data_)
      , condition_value_(rhs.condition_value_)
      , op_(rhs.op_) {
  }

  /** Move constructor. */
  ASTNodeVal(ASTNodeVal&& rhs)
      : field_name_(std::move(rhs.field_name_))
      , condition_value_data_(std::move(rhs.condition_value_data_))
      , condition_value_(std::move(rhs.condition_value_))
      , op_(rhs.op_) {
  }

  /** Assignment operator. */
  ASTNodeVal& operator=(const ASTNodeVal& rhs) {
    if (this != &rhs) {
      field_name_ = rhs.field_name_;
      condition_value_data_ = rhs.condition_value_data_;
      condition_value_ = rhs.condition_value_;
      op_ = rhs.op_;
    }

    return *this;
  }

  /** Move-assignment operator. */
  ASTNodeVal& operator=(ASTNodeVal&& rhs) {
    field_name_ = std::move(rhs.field_name_);
    condition_value_data_ = std::move(rhs.condition_value_data_);
    condition_value_ = std::move(rhs.condition_value_);
    op_ = rhs.op_;

    return *this;
  }

  ASTNodeTag get_tag() {
    return tag_;
  }

  std::string to_str() {
    std::string result_str;
    result_str = field_name_ + " " + query_condition_op_str(op_) + " ";
    if (condition_value_) {
      result_str += condition_value_data_.to_hex_str();
    } else {
      result_str += "null";
    }
    return result_str;
  }

  tdb_unique_ptr<ASTNode> clone() {
    return tdb_unique_ptr<ASTNode>(tdb_new(
        ASTNodeVal,
        field_name_,
        condition_value_data_.data(),
        condition_value_data_.size(),
        op_));
  }

  /** The attribute name. */
  std::string field_name_;

  /** The value data. */
  ByteVecValue condition_value_data_;

  /** Pointer to value data. */
  void *condition_value_;

  /** The comparison operator. */
  QueryConditionOp op_;

 private:
  static const ASTNodeTag tag_{ASTNodeTag::VAL};
};

class ASTNodeExpr : public ASTNode {
 public:
  ASTNodeExpr(
      std::vector<tdb_unique_ptr<ASTNode>>&& nodes,
      QueryConditionCombinationOp c_op)
      : combination_op_(c_op) {
    for (const auto& elem : nodes) {
      nodes_.push_back(elem->clone());
    }
  }

  ~ASTNodeExpr() {
  }

  /** Copy constructor. */
  ASTNodeExpr(const ASTNodeExpr& rhs)
      : combination_op_(rhs.combination_op_) {
    for (const auto& elem : rhs.nodes_) {
      nodes_.push_back(elem->clone());
    }
  };

  /** Move constructor. */
  ASTNodeExpr(ASTNodeExpr&& rhs)
      : nodes_(std::move(rhs.nodes_))
      , combination_op_(rhs.combination_op_){};

  /** Assignment operator. */
  ASTNodeExpr& operator=(const ASTNodeExpr& rhs) {
    if (this != &rhs) {
      for (const auto& elem : rhs.nodes_) {
        nodes_.push_back(elem->clone());
      }
      combination_op_ = rhs.combination_op_;
    }
    return *this;
  }

  /** Move-assignment operator. */
  ASTNodeExpr& operator=(ASTNodeExpr&& rhs) {
    nodes_ = std::move(rhs.nodes_);
    combination_op_ = rhs.combination_op_;
    return *this;
  }

  ASTNodeTag get_tag() {
    return tag_;
  }

  std::string to_str() {
    std::string result_str;
    result_str = "(";
    for (size_t i = 0; i < nodes_.size(); i++) {
      auto ptr = nodes_[i].get();
      if (ptr != nullptr) {
        result_str += ptr->to_str();
        if (i != nodes_.size() - 1) {
          result_str += " ";
          result_str += query_condition_combination_op_str(combination_op_);
          result_str += " ";
        }
      }
    }
    result_str += ")";
    return result_str;
  }

  tdb_unique_ptr<ASTNode> clone() {
    std::vector<tdb_unique_ptr<ASTNode>> nodes_copy;
    for (const auto& node : nodes_) {
      nodes_copy.push_back(node->clone());
    }
    return tdb_unique_ptr<ASTNode>(
        tdb_new(ASTNodeExpr, std::move(nodes_copy), combination_op_));
  }

  /** The node list **/
  std::vector<tdb_unique_ptr<ASTNode>> nodes_;

  /** The combination operation **/
  QueryConditionCombinationOp combination_op_;

 private:
  static const ASTNodeTag tag_{ASTNodeTag::EXPR};
};

tdb_unique_ptr<ASTNode> ast_combine(
    const tdb_unique_ptr<ASTNode>& lhs,
    const tdb_unique_ptr<ASTNode>& rhs,
    QueryConditionCombinationOp combination_op);

void ast_get_field_names(std::unordered_set<std::string> &field_name_set, const tdb_unique_ptr<ASTNode>& node);

bool ast_is_previously_supported(const tdb_unique_ptr<ASTNode>& node);

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_AST_H