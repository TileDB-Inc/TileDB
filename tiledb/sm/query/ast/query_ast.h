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
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
enum ASTNodeTag : char { NIL, VAL, EXPR };

class ASTNode {
 public:
  /** Returns the tag attached to the node. */
  virtual ASTNodeTag get_tag() = 0;
  /** Returns a deep copy of the node. */
  virtual tdb_unique_ptr<ASTNode> clone() = 0;
  /** Default virtual destructor. */
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
      , condition_value_data_(condition_value_size)
      , condition_value_view_(
            (condition_value != nullptr && condition_value_size == 0 ?
                 (void*)"" :
                 condition_value_data_.data()),
            condition_value_data_.size())
      , op_(op) {
    if (condition_value_view_.size() != 0) {
      memcpy(
          condition_value_data_.data(), condition_value, condition_value_size);
    }
  };

  /** Default destructor. */
  ~ASTNodeVal() {
  }

  /** Copy constructor. */
  ASTNodeVal(const ASTNodeVal& rhs)
      : field_name_(rhs.field_name_)
      , condition_value_data_(rhs.condition_value_data_)
      , condition_value_view_(
            (rhs.condition_value_view_.content() == nullptr ?
                 nullptr :
                 condition_value_data_.data()),
            condition_value_data_.size())
      , op_(rhs.op_){};

  /** Move constructor. */
  ASTNodeVal(ASTNodeVal&& rhs)
      : field_name_(std::move(rhs.field_name_))
      , condition_value_data_(std::move(rhs.condition_value_data_))
      , condition_value_view_(
            (rhs.condition_value_view_.content() == nullptr ?
                 nullptr :
                 condition_value_data_.data()),
            condition_value_data_.size())
      , op_(rhs.op_){};

  /** Assignment operator. */
  ASTNodeVal& operator=(const ASTNodeVal& rhs) {
    if (this != &rhs) {
      field_name_ = rhs.field_name_;
      condition_value_data_ = rhs.condition_value_data_;
      condition_value_view_ = UntypedDatumView(
          (rhs.condition_value_view_.content() == nullptr ?
               nullptr :
               condition_value_data_.data()),
          condition_value_data_.size());
      op_ = rhs.op_;
    }
    return *this;
  }

  /** Move-assignment operator. */
  ASTNodeVal& operator=(ASTNodeVal&& rhs) {
    field_name_ = std::move(rhs.field_name_);
    condition_value_data_ = std::move(rhs.condition_value_data_);
    condition_value_view_ = std::move(rhs.condition_value_view_);
    op_ = rhs.op_;

    return *this;
  }

  /** Returns the tag attached to the node. */
  ASTNodeTag get_tag() {
    return tag_;
  }

  /** Returns a deep copy of the node. */
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

  /** A view of the value data. */
  UntypedDatumView condition_value_view_;

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

  /** Default destructor. */
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

  /** Returns the tag attached to the node. */
  ASTNodeTag get_tag() {
    return tag_;
  }

  /** Returns a deep copy of the node. */
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

/**
 * @brief This function takes two AST trees (lhs and rhs) and combines
 * them into one combined AST, connecting them with the combination op.
 *
 * @param lhs left AST
 * @param rhs right AST
 * @param combination_op the combination op to combine the nodes with
 * @return unique pointer containing the values of the combined op
 */
tdb_unique_ptr<ASTNode> ast_combine(
    const tdb_unique_ptr<ASTNode>& lhs,
    const tdb_unique_ptr<ASTNode>& rhs,
    QueryConditionCombinationOp combination_op);

/**
 * @brief Returns a set of field names from all the value nodes in the AST.
 *
 * @param field_name_set The set to store the field names in.
 * @param node The node we are collecting field names from.
 */
void ast_get_field_names(
    std::unordered_set<std::string>& field_name_set,
    const tdb_unique_ptr<ASTNode>& node);

/**
 * @brief Returns true if the ast is previously supported by older versions of
 * TileDB. (i.e. is either a value node or only has AND combination ops. )
 * @param node The AST we are evaluating this condition on.
 */
bool ast_is_previously_supported(const tdb_unique_ptr<ASTNode>& node);

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_AST_H