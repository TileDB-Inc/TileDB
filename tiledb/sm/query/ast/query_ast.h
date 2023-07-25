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
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ASTNegationT {};
static constexpr ASTNegationT ASTNegation{};

/**
 * @brief The ASTNode class is an abstract class that contains virtual
 * methods used by both the value and expression node implementation
 * classes. ASTNodes are used to represent a QueryCondition in AST form.
 */
class ASTNode {
 public:
  /**
   * @brief ASTNode class method used for casing between value and expression
   * nodes.
   *
   * @return True if the ASTNode is an expression node.
   */
  virtual bool is_expr() const = 0;

  /**
   * @brief ASTNode class method used in the QueryCondition copy constructor
   * ASTNode::combine, and testing that returns a copy of the caller node.
   *
   * @return tdb_unique_ptr<ASTNode> A deep copy of the ASTNode.
   */
  virtual tdb_unique_ptr<ASTNode> clone() const = 0;

  /**
   * @brief ASTNode class method used in the QueryCondition that returns a copy
   * of the caller node, but negated.
   *
   * @return tdb_unique_ptr<ASTNode> A negated deep copy of the ASTNode.
   */
  virtual tdb_unique_ptr<ASTNode> get_negated_tree() const = 0;

  /**
   * @brief Gets the set of field names from all the value nodes in the ASTNode.
   *
   * @param field_name_set The set variable the function populates.
   */
  virtual void get_field_names(
      std::unordered_set<std::string>& field_name_set) const = 0;

  /**
   * @brief Gets the set of field names from all the value nodes that reference
   * an enumerated field.
   *
   * @param field_name_set The set variable the function populates.
   */
  virtual void get_enumeration_field_names(
      std::unordered_set<std::string>& field_name_set) const = 0;

  /**
   * @brief Returns true if the AST is previously supported by previous versions
   * of TileDB. This means that the AST should only have AND combination ops,
   * and should not be a layered tree. Used in query condition serialization.
   *
   * @return True if the AST is previously supported by previous versions of
   * TileDB.
   */
  virtual bool is_backwards_compatible() const = 0;

  /**
   * @brief Update an node value condition values that refer to enumerated
   * attributes.
   *
   * @param array_schema The array schema with all relevant enumerations loaded.
   */
  virtual void rewrite_enumeration_conditions(
      const ArraySchema& array_schema) = 0;

  /**
   * @brief Checks whether the node is valid based on the array schema of the
   * array that the query condition that contains this AST node will execute a
   * query on.
   *
   * @param array_schema The array schema that represents the array being
   * queried on.
   * @return Status OK if successful.
   */
  virtual Status check_node_validity(const ArraySchema& array_schema) const = 0;

  /**
   * @brief Combines two ASTNodes (the caller node and rhs) with the combination
   * op and returns the result.
   *
   * @param rhs The ASTNode we are combining the caller node with.
   * @param combination_op The combination op.
   * @return tdb_unique_ptr<ASTNode> The combined node.
   */
  virtual tdb_unique_ptr<ASTNode> combine(
      const tdb_unique_ptr<ASTNode>& rhs,
      const QueryConditionCombinationOp& combination_op) = 0;

  /**
   * @brief Get the field name object.
   * This is an AST value node getter method.
   * It should throw an exception if called on an expression node.
   *
   * @return const std::string& The field name.
   */
  virtual const std::string& get_field_name() const = 0;

  /**
   * @brief Get the condition value view object.
   * This is an AST value node getter method.
   * It should throw an exception if called on an expression node.
   *
   * @return const UntypedDatumView& The condition value view object,
   * representing the value to compare to.
   */
  virtual const UntypedDatumView& get_condition_value_view() const = 0;

  /**
   * @brief Get the comparison op.
   * This is an AST value node getter method.
   * It should throw an exception if called on an expression node.
   *
   * @return const QueryConditionOp& The comparison op.
   */
  virtual const QueryConditionOp& get_op() const = 0;

  /**
   * @brief Get the vector of children nodes.
   * This is an AST expression node getter method.
   * It should throw an exception if called on an value node.
   *
   * @return const std::vector<tdb_unique_ptr<ASTNode>>& The list of children
   * for this node.
   */
  virtual const std::vector<tdb_unique_ptr<ASTNode>>& get_children() const = 0;

  /**
   * @brief Get the combination op.
   * This is an AST expression node getter method.
   * It should throw an exception if called on an value node.
   *
   * @return const QueryConditionCombinationOp& The combination op.
   */
  virtual const QueryConditionCombinationOp& get_combination_op() const = 0;

  /**
   * @brief Return whether this node's condition should be applied against
   * the attributes enumerated values or the underlying index data if
   * applicable for a given attribute.
   *
   * @return bool If true, apply this condition against the enumerated values.
   */
  virtual bool use_enumeration() const = 0;

  /**
   * @brief By default, a query condition is applied against an enumeration's
   * values. This can be disabled to apply a given condition against the
   * underlying integer data stored for the attribute by passing `false` to
   * this method.
   *
   * @param use_enumeration A bool indicating whether this condition should be
   *        applied against the enumerations values or not.
   */
  virtual void set_use_enumeration(bool use_enumeration) = 0;

  /**
   * @brief Default virtual destructor.
   */
  virtual ~ASTNode() {
  }
};

/**
 * @brief The ASTNodeVal class can be used to represent a simple predicate.
 */
class ASTNodeVal : public ASTNode {
 public:
  /**
   * @brief Construct a new ASTNodeVal object.
   *
   * @param field_name The name of the field this operation applies to.
   * @param condition_value  The value to compare to.
   * @param condition_value_size The byte size of condition_value.
   * @param op The relational operation between the value of the field
   *     and `condition_value`.
   * @param use_enumeration Whether or not to use an associated
   *     enumeration at query time.
   */
  ASTNodeVal(
      const std::string& field_name,
      const void* const condition_value,
      const uint64_t condition_value_size,
      const QueryConditionOp op,
      bool use_enumeration = true)
      : field_name_(field_name)
      , condition_value_data_(condition_value_size)
      , condition_value_view_(
            (condition_value != nullptr && condition_value_size == 0 ?
                 (void*)"" :
                 condition_value_data_.data()),
            condition_value_data_.size())
      , op_(op)
      , use_enumeration_(use_enumeration) {
    if (condition_value_view_.size() != 0) {
      memcpy(
          condition_value_data_.data(), condition_value, condition_value_size);
    }
  };

  /**
   * @brief Copy constructor.
   */
  ASTNodeVal(const ASTNodeVal& rhs)
      : field_name_(rhs.field_name_)
      , condition_value_data_(rhs.condition_value_data_)
      , condition_value_view_(
            (rhs.condition_value_view_.content() != nullptr &&
                     rhs.condition_value_view_.size() == 0 ?
                 (void*)"" :
                 condition_value_data_.data()),
            condition_value_data_.size())
      , op_(rhs.op_)
      , use_enumeration_(rhs.use_enumeration_) {
  }

  /**
   * @brief Copy constructor, negated.
   */
  ASTNodeVal(const ASTNodeVal& rhs, ASTNegationT)
      : field_name_(rhs.field_name_)
      , condition_value_data_(rhs.condition_value_data_)
      , condition_value_view_(
            (rhs.condition_value_view_.content() != nullptr &&
                     rhs.condition_value_view_.size() == 0 ?
                 (void*)"" :
                 condition_value_data_.data()),
            condition_value_data_.size())
      , op_(negate_query_condition_op(rhs.op_))
      , use_enumeration_(rhs.use_enumeration_) {
  }

  /**
   * @brief Default destructor of ASTNodeVal.
   */
  ~ASTNodeVal() {
  }

  DISABLE_MOVE(ASTNodeVal);
  DISABLE_COPY_ASSIGN(ASTNodeVal);
  DISABLE_MOVE_ASSIGN(ASTNodeVal);

  /**
   * @brief ASTNode class method used for casing between value and expression
   * nodes.
   *
   * @return True if the ASTNode is an expression node.
   */
  bool is_expr() const override;

  /**
   * @brief ASTNode class method used in the QueryCondition copy constructor
   * ASTNode::combine, and testing that returns a copy of the caller node.
   *
   * @return tdb_unique_ptr<ASTNode> A deep copy of the ASTNode.
   */
  tdb_unique_ptr<ASTNode> clone() const override;

  /**
   * @brief ASTNode class method used in the QueryCondition that returns a copy
   * of the caller node, but negated.
   *
   * @return tdb_unique_ptr<ASTNode> A negated deep copy of the ASTNode.
   */
  tdb_unique_ptr<ASTNode> get_negated_tree() const override;

  /**
   * @brief Gets the set of field names from all the value nodes in the ASTNode.
   *
   * @param field_name_set The set variable the function populates.
   */
  void get_field_names(
      std::unordered_set<std::string>& field_name_set) const override;

  /**
   * @brief Gets the set of field names from all the value nodes that reference
   * an enumerated field.
   *
   * @param field_name_set The set variable the function populates.
   */
  void get_enumeration_field_names(
      std::unordered_set<std::string>& field_name_set) const override;

  /**
   * @brief Returns true if the AST is previously supported by previous versions
   * of TileDB. This means that the AST should only have AND combination ops,
   * and should not be a layered tree. Used in query condition serialization.
   *
   * @return True if the AST is previously supported by previous versions of
   * TileDB.
   */
  bool is_backwards_compatible() const override;

  /**
   * Update any node value condition values that refer to enumerated attributes.
   * For any value condition on an attribute that has an Enumeration, the
   * user specified value is passed to the attribute's enumeration `index_of`
   * method to replace the user provided value with the Enumeration's value
   * index.
   *
   * @param array_schema The array schema with all relevant enumerations loaded.
   */
  void rewrite_enumeration_conditions(const ArraySchema& array_schema) override;

  /**
   * @brief Checks whether the node is valid based on the array schema of the
   * array that the query condition that contains this AST node will execute a
   * query on.
   *
   * @param array_schema The array schema that represents the array being
   * queried on.
   * @return Status OK if successful.
   */
  Status check_node_validity(const ArraySchema& array_schema) const override;

  /**
   * @brief Combines two ASTNodes (the caller node and rhs) with the combination
   * op and returns the result.
   *
   * @param rhs The ASTNode we are combining the caller node with.
   * @param combination_op The combination op.
   * @return tdb_unique_ptr<ASTNode> The combined node.
   */
  tdb_unique_ptr<ASTNode> combine(
      const tdb_unique_ptr<ASTNode>& rhs,
      const QueryConditionCombinationOp& combination_op) override;

  /**
   * @brief Get the field name object.
   * This is an AST value node getter method.
   * It should throw an exception if called on an expression node.
   *
   * @return const std::string& The field name.
   */
  const std::string& get_field_name() const override;

  /**
   * @brief Get the condition value view object.
   * This is an AST value node getter method.
   * It should throw an exception if called on an expression node.
   *
   * @return const UntypedDatumView& The condition value view object,
   * representing the value to compare to.
   */
  const UntypedDatumView& get_condition_value_view() const override;

  /**
   * @brief Get the comparison op.
   * This is an AST value node getter method.
   * It should throw an exception if called on an expression node.
   *
   * @return const QueryConditionOp& The comparison op.
   */
  const QueryConditionOp& get_op() const override;

  /**
   * @brief Get the vector of children nodes.
   * This is an AST expression node getter method.
   * It should throw an exception if called on an value node.
   *
   * @return const std::vector<tdb_unique_ptr<ASTNode>>& The list of children
   * for this node.
   */
  const std::vector<tdb_unique_ptr<ASTNode>>& get_children() const override;

  /**
   * @brief Get the combination op.
   * This is an AST expression node getter method.
   * It should throw an exception if called on an value node.
   *
   * @return const QueryConditionCombinationOp& The combination op.
   */
  const QueryConditionCombinationOp& get_combination_op() const override;

  /**
   * @brief Return whether this node's condition should be applied against
   * the attributes enumerated values or the underlying index data if
   * applicable for a given attribute.
   *
   * @return bool If true, apply this condition against the enumerated values.
   */
  bool use_enumeration() const override;

  /**
   * @brief By default, a query condition is applied against an enumeration's
   * values. This can be disabled to apply a given condition against the
   * underlying integer data stored for the attribute by passing `false` to
   * this method.
   *
   * @param use_enumeration A bool indicating whether this condition should be
   *        applied against the enumerations values or not.
   */
  void set_use_enumeration(bool use_enumeration) override;

 private:
  /** The attribute name. */
  std::string field_name_;

  /** The value data. */
  ByteVecValue condition_value_data_;

  /** A view of the value data. */
  UntypedDatumView condition_value_view_;

  /** The comparison operator. */
  QueryConditionOp op_;

  /** Whether this condiiton applies to enumerated values if applicable */
  bool use_enumeration_;
};

/**
 * @brief The ASTNodeExpr class can be used to represent a complex predicate.
 */
class ASTNodeExpr : public ASTNode {
 public:
  /**
   * @brief Expression node constructor.
   *
   * @param nodes A list of the children of this node.
   * @param c_op The logical operation to combine the nodes' children.
   */
  ASTNodeExpr(
      std::vector<tdb_unique_ptr<ASTNode>> nodes,
      const QueryConditionCombinationOp c_op)
      : nodes_(std::move(nodes))
      , combination_op_(c_op) {
  }

  /**
   * @brief Copy constructor.
   */
  ASTNodeExpr(const ASTNodeExpr& rhs)
      : combination_op_(rhs.combination_op_) {
    for (auto& node : rhs.nodes_) {
      nodes_.push_back(node->clone());
    }
  }

  /**
   * @brief Copy constructor, negated.
   */
  ASTNodeExpr(const ASTNodeExpr& rhs, ASTNegationT)
      : combination_op_(negate_qc_combination_op(rhs.combination_op_)) {
    for (auto& node : rhs.nodes_) {
      nodes_.push_back(node->get_negated_tree());
    }
  }

  /**
   * @brief Default destructor of ASTNodeExpr.
   */
  ~ASTNodeExpr() {
  }

  DISABLE_MOVE(ASTNodeExpr);
  DISABLE_COPY_ASSIGN(ASTNodeExpr);
  DISABLE_MOVE_ASSIGN(ASTNodeExpr);

  /**
   * @brief ASTNode class method used for casing between value and expression
   * nodes.
   *
   * @return True if the ASTNode is an expression node.
   */
  bool is_expr() const override;

  /**
   * @brief ASTNode class method used in the QueryCondition copy constructor
   *        ASTNode::combine, and testing that returns a copy of the caller
   * node.
   *
   * @return tdb_unique_ptr<ASTNode> A deep copy of the ASTNode.
   */
  tdb_unique_ptr<ASTNode> clone() const override;

  /**
   * @brief ASTNode class method used in the QueryCondition that returns a copy
   * of the caller node, but negated.
   *
   * @return tdb_unique_ptr<ASTNode> A negated deep copy of the ASTNode.
   */
  tdb_unique_ptr<ASTNode> get_negated_tree() const override;

  /**
   * @brief Gets the set of field names from all the value nodes in the ASTNode.
   *
   * @param field_name_set The set variable the function populates.
   */
  void get_field_names(
      std::unordered_set<std::string>& field_name_set) const override;

  /**
   * @brief Gets the set of field names from all the value nodes that reference
   * an enumerated field.
   *
   * @param field_name_set The set variable the function populates.
   */
  void get_enumeration_field_names(
      std::unordered_set<std::string>& field_name_set) const override;

  /**
   * @brief Returns true if the AST is previously supported by previous versions
   * of TileDB. This means that the AST should only have AND combination ops,
   * and should not be a layered tree. Used in query condition serialization.
   *
   * @return True if the AST is previously supported by previous versions of
   * TileDB.
   */
  bool is_backwards_compatible() const override;

  /**
   * @brief Update an node value condition values that refer to enumerated
   * attributes.
   *
   * @param array_schema The array schema with all relevant enumerations loaded.
   */
  void rewrite_enumeration_conditions(const ArraySchema& array_schema) override;

  /**
   * @brief Checks whether the node is valid based on the array schema of the
   * array that the query condition that contains this AST node will execute a
   * query on.
   *
   * @param array_schema The array schema that represents the array being
   * queried on.
   * @return Status OK if successful.
   */
  Status check_node_validity(const ArraySchema& array_schema) const override;

  /**
   * @brief Combines two ASTNodes (the caller node and rhs) with the combination
   * op and returns the result.
   *
   * @param rhs The ASTNode we are combining the caller node with.
   * @param combination_op The combination op.
   * @return tdb_unique_ptr<ASTNode> The combined node.
   */
  tdb_unique_ptr<ASTNode> combine(
      const tdb_unique_ptr<ASTNode>& rhs,
      const QueryConditionCombinationOp& combination_op) override;

  /**
   * @brief Get the field name object.
   * This is an AST value node getter method.
   * It should throw an exception if called on an expression node.
   *
   * @return const std::string& The field name.
   */
  const std::string& get_field_name() const override;

  /**
   * @brief Get the condition value view object.
   * This is an AST value node getter method.
   * It should throw an exception if called on an expression node.
   *
   * @return const UntypedDatumView& The condition value view object,
   * representing the value to compare to.
   */
  const UntypedDatumView& get_condition_value_view() const override;

  /**
   * @brief Get the comparison op.
   * This is an AST value node getter method.
   * It should throw an exception if called on an expression node.
   *
   * @return const QueryConditionOp& The comparison op.
   */
  const QueryConditionOp& get_op() const override;

  /**
   * @brief Get the vector of children nodes.
   * This is an AST expression node getter method.
   * It should throw an exception if called on an value node.
   *
   * @return const std::vector<tdb_unique_ptr<ASTNode>>& The list of children
   * for this node.
   */
  const std::vector<tdb_unique_ptr<ASTNode>>& get_children() const override;

  /**
   * @brief Get the combination op.
   * This is an AST expression node getter method.
   * It should throw an exception if called on an value node.
   *
   * @return const QueryConditionCombinationOp& The combination op.
   */
  const QueryConditionCombinationOp& get_combination_op() const override;

  /**
   * @brief Return whether this node's condition should be applied against
   * the attributes enumerated values or the underlying index data if
   * applicable for a given attribute.
   *
   * This method always throws when called on an expression node.
   *
   * @return bool If true, apply this condition against the enumerated values.
   */
  bool use_enumeration() const override;

  /**
   * @brief By default, a query condition is applied against an enumeration's
   * values. This can be disabled to apply a given condition against the
   * underlying integer data stored for the attribute by passing `false` to
   * this method.
   *
   * When called on an expression node this value is recursively applied
   * against all value nodes in the AST.
   *
   * @param use_enumeration A bool indicating whether this condition should be
   *        applied against the enumerations values or not.
   */
  void set_use_enumeration(bool use_enumeration) override;

 private:
  /** The node list **/
  std::vector<tdb_unique_ptr<ASTNode>> nodes_;

  /** The combination operation **/
  QueryConditionCombinationOp combination_op_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_AST_H
