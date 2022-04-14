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
 * Implements the Query ASTNode classes.
 */

#include "query_ast.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
/** Returns true is the node is an expression node. */
bool ASTNodeVal::is_expr() const {
  return false;
}

/** Returns a deep copy of the node. */
tdb_unique_ptr<ASTNode> ASTNodeVal::clone() const {
  return tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal,
      field_name_,
      condition_value_data_.data(),
      condition_value_data_.size(),
      op_));
}

void ASTNodeVal::get_field_names(
    std::unordered_set<std::string>& field_name_set) const {
  field_name_set.insert(field_name_);
}

bool ASTNodeVal::is_previously_supported() const {
  return true;
}

Status ASTNodeVal::check(const ArraySchema& array_schema) const {
  const uint64_t condition_value_size = condition_value_data_.size();
  const auto attribute = array_schema.attribute(field_name_);
  // Check that the field_name represents an attribute in the array
  // schema corresponding to this QueryCondition object.
  if (!attribute) {
    return Status_QueryConditionError(
        "Value node field name is not an attribute " + field_name_);
  }

  // Ensure that null value can only be used with equality operators.
  if (condition_value_view_.content() == nullptr) {
    if (op_ != QueryConditionOp::EQ && op_ != QueryConditionOp::NE) {
      return Status_QueryConditionError(
          "Null value can only be used with equality operators");
    }

    // Ensure that an attribute that is marked as nullable
    // corresponds to a type that is nullable.
    if ((!attribute->nullable()) &&
        (attribute->type() != Datatype::STRING_ASCII &&
         attribute->type() != Datatype::CHAR)) {
      return Status_QueryConditionError(
          "Null value can only be used with nullable attributes");
    }
  }

  // Ensure that non-empty attributes are only var-sized for
  // ASCII strings.
  if (attribute->var_size() && attribute->type() != Datatype::STRING_ASCII &&
      attribute->type() != Datatype::CHAR &&
      condition_value_view_.content() != nullptr) {
    return Status_QueryConditionError(
        "Value node non-empty attribute may only be var-sized for ASCII "
        "strings: " +
        field_name_);
  }

  // Ensure that non string fixed size attributes store only one value per cell.
  if (attribute->cell_val_num() != 1 &&
      attribute->type() != Datatype::STRING_ASCII &&
      attribute->type() != Datatype::CHAR && (!attribute->var_size())) {
    return Status_QueryConditionError(
        "Value node attribute must have one value per cell for non-string "
        "fixed size attributes: " +
        field_name_);
  }

  // Ensure that the condition value size matches the attribute's
  // value size.
  if (attribute->cell_size() != constants::var_size &&
      attribute->cell_size() != condition_value_size &&
      !(attribute->nullable() && condition_value_view_.content() == nullptr) &&
      attribute->type() != Datatype::STRING_ASCII &&
      attribute->type() != Datatype::CHAR && (!attribute->var_size())) {
    return Status_QueryConditionError(
        "Value node condition value size mismatch: " +
        std::to_string(attribute->cell_size()) +
        " != " + std::to_string(condition_value_size));
  }

  // Ensure that the attribute type is valid.
  switch (attribute->type()) {
    case Datatype::ANY:
      return Status_QueryConditionError(
          "Value node attribute type may not be of type 'ANY': " + field_name_);
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
      return Status_QueryConditionError(
          "Value node attribute type may not be a UTF/UCS string: " +
          field_name_);
    default:
      break;
  }
  return Status::Ok();
}

tdb_unique_ptr<ASTNode> ASTNodeVal::combine(
    const tdb_unique_ptr<ASTNode>& rhs,
    const QueryConditionCombinationOp& combination_op) {
  std::vector<tdb_unique_ptr<ASTNode>> ast_nodes;
  if (!rhs->is_expr()) {
    ast_nodes.push_back(this->clone());
    ast_nodes.push_back(rhs->clone());
  } else {  // rhs is an expression node.
    // lhs is a simple tree, rhs is a compound tree.
    ast_nodes.push_back(this->clone());
    if (rhs->get_node_combination_op() == combination_op) {
      for (const auto& elem : rhs->get_node_children()) {
        ast_nodes.push_back(elem->clone());
      }
    } else {
      ast_nodes.push_back(rhs->clone());
    }
  }
  return tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeExpr, std::move(ast_nodes), combination_op));
}

/** Value node getter methods */
const std::string& ASTNodeVal::get_node_field_name() const {
  return field_name_;
}
const ByteVecValue& ASTNodeVal::get_node_condition_value_data() const {
  return condition_value_data_;
}
const UntypedDatumView& ASTNodeVal::get_node_condition_value_view() const {
  return condition_value_view_;
}
const QueryConditionOp& ASTNodeVal::get_node_op() const {
  return op_;
}

/** Expression node getter methods */
const std::vector<tdb_unique_ptr<ASTNode>>& ASTNodeVal::get_node_children()
    const {
  throw std::runtime_error(
      "ASTNodeVal::get_node_children: called get_node_children() on a value "
      "node.");
}
const QueryConditionCombinationOp& ASTNodeVal::get_node_combination_op() const {
  throw std::runtime_error(
      "ASTNodeVal::get_node_combination_op: called get_node_combination_op on "
      "a value node.");
}

/** Returns true is the node is an expression node. */
bool ASTNodeExpr::is_expr() const {
  return true;
}

/** Returns a deep copy of the node. */
tdb_unique_ptr<ASTNode> ASTNodeExpr::clone() const {
  std::vector<tdb_unique_ptr<ASTNode>> nodes_copy;
  for (const auto& node : nodes_) {
    nodes_copy.push_back(node->clone());
  }
  return tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeExpr, std::move(nodes_copy), combination_op_));
}

void ASTNodeExpr::get_field_names(
    std::unordered_set<std::string>& field_name_set) const {
  for (const auto& child : nodes_) {
    child->get_field_names(field_name_set);
  }
}

bool ASTNodeExpr::is_previously_supported() const {
  if (combination_op_ != QueryConditionCombinationOp::AND) {
    return false;
  }
  for (const auto& child : nodes_) {
    if (child->is_expr()) {
      return false;
    }
  }
  return true;
}

Status ASTNodeExpr::check(const ArraySchema& array_schema) const {
  // If the node is a compound expression node, ensure there are at least
  // two children in the node and then run a check on each child node.
  if (nodes_.size() < 2) {
    return Status_QueryConditionError(
        "Non value AST node does not have at least 2 children.");
  }
  for (const auto& child : nodes_) {
    RETURN_NOT_OK(child->check(array_schema));
  }

  return Status::Ok();
}

tdb_unique_ptr<ASTNode> ASTNodeExpr::combine(
    const tdb_unique_ptr<ASTNode>& rhs,
    const QueryConditionCombinationOp& combination_op) {
  std::vector<tdb_unique_ptr<ASTNode>> ast_nodes;
  if (!rhs->is_expr()) {
    if (combination_op == combination_op_) {
      for (const auto& child : nodes_) {
        ast_nodes.push_back(child->clone());
      }
    } else {
      ast_nodes.push_back(this->clone());
    }
    ast_nodes.push_back(rhs->clone());
  } else {
    if (combination_op == combination_op_) {
      for (const auto& child : nodes_) {
        ast_nodes.push_back(child->clone());
      }
    } else {
      ast_nodes.push_back(this->clone());
    }

    if (combination_op == rhs->get_node_combination_op()) {
      for (const auto& child : rhs->get_node_children()) {
        ast_nodes.push_back(child->clone());
      }
    } else {
      ast_nodes.push_back(rhs->clone());
    }
  }
  return tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeExpr, std::move(ast_nodes), combination_op));
}

/** Value node getter methods */
const std::string& ASTNodeExpr::get_node_field_name() const {
  throw std::runtime_error(
      "ASTNodeExpr::get_node_field_name: called get_node_field_name() on a "
      "expression node.");
}
const ByteVecValue& ASTNodeExpr::get_node_condition_value_data() const {
  throw std::runtime_error(
      "ASTNodeExpr::get_node_condition_value_data: called "
      "get_node_condition_value_data() on a expression node.");
}
const UntypedDatumView& ASTNodeExpr::get_node_condition_value_view() const {
  throw std::runtime_error(
      "ASTNodeExpr::get_node_condition_value_view: called "
      "get_node_condition_value_view() on a expression node.");
}
const QueryConditionOp& ASTNodeExpr::get_node_op() const {
  throw std::runtime_error(
      "ASTNodeExpr::get_node_op: called get_node_op() on a expression node.");
}

/** Expression node getter methods */
const std::vector<tdb_unique_ptr<ASTNode>>& ASTNodeExpr::get_node_children()
    const {
  return nodes_;
}
const QueryConditionCombinationOp& ASTNodeExpr::get_node_combination_op()
    const {
  return combination_op_;
}

}  // namespace sm
}  // namespace tiledb