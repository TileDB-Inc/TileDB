/**
 * @file query_ast.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
#include <optional>
#include <sstream>
#include <unordered_set>
#include <utility>

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ASTNode {
 public:
  /**
   * @brief ASTNode class method used for casing between value and expression
   * nodes.
   * @return Returns true if the ASTNode is an expression node.
   */
  virtual bool is_expr() const = 0;
  /** Returns a deep copy of the node. */
  virtual tdb_unique_ptr<ASTNode> clone() const = 0;
  /** Returns a set of field names from all the value nodes in the AST. */
  virtual void get_field_names(
      std::unordered_set<std::string>& field_name_set) const = 0;
  /** Returns true is the AST is previously supported by previous versions of
   * TileDB. */
  virtual bool is_or_supported() const = 0;
  /** Returns whether a tree is a valid QC AST according to the array schema. */
  virtual Status check_node_validity(const ArraySchema& array_schema) const = 0;
  /** Combines AST node with rhs being passed in. */
  virtual tdb_unique_ptr<ASTNode> combine(
      const tdb_unique_ptr<ASTNode>& rhs,
      const QueryConditionCombinationOp& combination_op) = 0;

  /** Value node getter methods */
  virtual const std::string& get_field_name() const = 0;
  virtual const UntypedDatumView& get_condition_value_view() const = 0;
  virtual const QueryConditionOp& get_op() const = 0;

  /** Expression node getter methods */
  virtual const std::vector<tdb_unique_ptr<ASTNode>>& get_children() const = 0;
  virtual const QueryConditionCombinationOp& get_combination_op() const = 0;

  /** Default virtual destructor. */
  virtual ~ASTNode() {
  }
};

/** Represents a simple terminal/predicate **/
class ASTNodeVal : public ASTNode {
 public:
  /** Value constructor. */
  ASTNodeVal(
      const std::string& field_name,
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

  DISABLE_COPY(ASTNodeVal);
  DISABLE_MOVE(ASTNodeVal);
  DISABLE_COPY_ASSIGN(ASTNodeVal);
  DISABLE_MOVE_ASSIGN(ASTNodeVal);

  /** Returns true if it is an expression node. */
  bool is_expr() const override;

  /** Returns a deep copy of the node. */
  tdb_unique_ptr<ASTNode> clone() const override;

  /** Returns a set of field names from all the value nodes in the AST. */
  void get_field_names(
      std::unordered_set<std::string>& field_name_set) const override;

  /** Returns true is the AST is previously supported by previous versions of
   * TileDB. */
  bool is_or_supported() const override;

  /** Returns whether a tree is a valid QC AST according to the array schema. */
  Status check_node_validity(const ArraySchema& array_schema) const override;

  /** Combines AST node with rhs being passed in. */
  tdb_unique_ptr<ASTNode> combine(
      const tdb_unique_ptr<ASTNode>& rhs,
      const QueryConditionCombinationOp& combination_op) override;

  /** Value node getter methods */
  const std::string& get_field_name() const override;
  const UntypedDatumView& get_condition_value_view() const override;
  const QueryConditionOp& get_op() const override;

  /** Expression node getter methods */
  const std::vector<tdb_unique_ptr<ASTNode>>& get_children() const override;
  const QueryConditionCombinationOp& get_combination_op() const override;

 private:
  /** The attribute name. */
  std::string field_name_;

  /** The value data. */
  ByteVecValue condition_value_data_;

  /** A view of the value data. */
  UntypedDatumView condition_value_view_;

  /** The comparison operator. */
  QueryConditionOp op_;
};

class ASTNodeExpr : public ASTNode {
 public:
  ASTNodeExpr(
      std::vector<tdb_unique_ptr<ASTNode>> nodes,
      const QueryConditionCombinationOp c_op)
      : nodes_(std::move(nodes))
      , combination_op_(c_op) {
  }

  /** Default destructor. */
  ~ASTNodeExpr() {
  }

  DISABLE_COPY(ASTNodeExpr);
  DISABLE_MOVE(ASTNodeExpr);
  DISABLE_COPY_ASSIGN(ASTNodeExpr);
  DISABLE_MOVE_ASSIGN(ASTNodeExpr);

  /** Returns true if it is an expression node. */
  bool is_expr() const override;

  /** Returns a deep copy of the node. */
  tdb_unique_ptr<ASTNode> clone() const override;

  /** Returns a set of field names from all the value nodes in the AST. */
  void get_field_names(
      std::unordered_set<std::string>& field_name_set) const override;

  /** Returns true is the AST is previously supported by previous versions of
   * TileDB. */
  bool is_or_supported() const override;

  /** Returns whether a tree is a valid QC AST according to the array schema. */
  Status check_node_validity(const ArraySchema& array_schema) const override;

  /** Combines AST node with rhs being passed in. */
  tdb_unique_ptr<ASTNode> combine(
      const tdb_unique_ptr<ASTNode>& rhs,
      const QueryConditionCombinationOp& combination_op) override;

  /** Value node getter methods */
  const std::string& get_field_name() const override;
  const UntypedDatumView& get_condition_value_view() const override;
  const QueryConditionOp& get_op() const override;

  /** Expression node getter methods */
  const std::vector<tdb_unique_ptr<ASTNode>>& get_children() const override;
  const QueryConditionCombinationOp& get_combination_op() const override;

 private:
  /** The node list **/
  std::vector<tdb_unique_ptr<ASTNode>> nodes_;

  /** The combination operation **/
  QueryConditionCombinationOp combination_op_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_AST_H