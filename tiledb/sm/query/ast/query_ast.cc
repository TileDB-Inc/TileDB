/**
 * @file query_ast.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/misc/integral_type_casts.h"

using namespace tiledb::common;

namespace tiledb::sm {

static inline bool supported_string_type(Datatype type) {
  return (
      type == Datatype::CHAR || type == Datatype::STRING_ASCII ||
      type == Datatype::STRING_UTF8);
}

ASTNodeVal::ASTNodeVal(
    const std::string& field_name,
    const void* const data,
    uint64_t data_size,
    const void* const offsets,
    uint64_t offsets_size,
    const QueryConditionOp op,
    bool use_enumeration)
    : field_name_(field_name)
    , is_null_(false)
    , op_(op)
    , use_enumeration_(use_enumeration) {
  if (data == nullptr && data_size != 0) {
    throw std::invalid_argument(
        "ASTNodeVal set membership data must not be nullptr");
  }

  if (data != nullptr && data_size == 0) {
    throw std::invalid_argument(
        "ASTNodeVal set membership data size must be greater "
        "than zero when data is provided.");
  }

  if (offsets == nullptr) {
    throw std::invalid_argument(
        "ASTNodeVal set membership offsets must not be nullptr");
  }

  if (offsets_size == 0) {
    throw std::invalid_argument(
        "ASTNodeVal set membership offsets size must be greater than zero.");
  }

  if (offsets_size % sizeof(uint64_t) != 0) {
    throw std::invalid_argument(
        "ASTNodeVal set membership offsets is not a multiple of "
        "uint64_t size.");
  }

  uint64_t num_offsets = offsets_size / constants::cell_var_offset_size;
  auto offset_elems = static_cast<const uint64_t*>(offsets);
  for (uint64_t i = 1; i < num_offsets; i++) {
    if (offset_elems[i] < offset_elems[i - 1]) {
      throw std::invalid_argument(
          "ASTNodeVal set membership offsets must not decrease.");
    }
  }

  if (offset_elems[num_offsets - 1] > data_size) {
    throw std::invalid_argument(
        "ASTNodeVal invalid set membership offsets invalid for data size: "
        "last offset " +
        std::to_string(offset_elems[num_offsets - 1]) +
        " is larger than data size " + std::to_string(data_size));
  }

  if (op_ != QueryConditionOp::IN && op_ != QueryConditionOp::NOT_IN) {
    throw std::invalid_argument(
        "ASTNodeVal invalid set membership operator in set "
        "membership constructor.");
  }

  data_ = ByteVecValue(data_size);
  memcpy(data_.data(), data, data_size);

  offsets_ = ByteVecValue(offsets_size);
  memcpy(offsets_.data(), offsets, offsets_size);

  generate_members();
}

bool ASTNodeVal::is_expr() const {
  return false;
}

tdb_unique_ptr<ASTNode> ASTNodeVal::clone() const {
  return tdb_unique_ptr<ASTNode>(tdb_new(ASTNodeVal, *this));
}

tdb_unique_ptr<ASTNode> ASTNodeVal::get_negated_tree() const {
  return tdb_unique_ptr<ASTNode>(tdb_new(ASTNodeVal, *this, ASTNegation));
}

void ASTNodeVal::get_field_names(
    std::unordered_set<std::string>& field_name_set) const {
  field_name_set.insert(field_name_);
}

void ASTNodeVal::get_enumeration_field_names(
    std::unordered_set<std::string>& field_name_set) const {
  if (use_enumeration_) {
    field_name_set.insert(field_name_);
  }
}

bool ASTNodeVal::is_backwards_compatible() const {
  if (op_ == QueryConditionOp::IN || op_ == QueryConditionOp::NOT_IN) {
    return false;
  }

  return true;
}

void ASTNodeVal::rewrite_enumeration_conditions(
    const ArraySchema& array_schema) {
  // This is called by the Query class before applying a query condition. This
  // works by looking up each related enumeration and translating the
  // condition's value to reflect the underlying index value. I.e., if the
  // query condition was created with `my_attr = "foo"`, and `my_attr` is an
  // attribute with an enumeration, this will replace the condition's value
  // with the index value returned by `Enumeration::index_of()`.

  if (!use_enumeration_) {
    return;
  }

  if (is_null_) {
    return;
  }

  if (!array_schema.is_attr(field_name_)) {
    return;
  }

  auto attr = array_schema.attribute(field_name_);
  auto enmr_name = attr->get_enumeration_name();
  if (!enmr_name.has_value()) {
    return;
  }

  auto enumeration = array_schema.get_enumeration(enmr_name.value());
  if (!enumeration) {
    throw std::logic_error(
        "Missing required enumeration for field '" + field_name_ + "'");
  }

  if (!enumeration->ordered()) {
    switch (op_) {
      case QueryConditionOp::LT:
      case QueryConditionOp::LE:
      case QueryConditionOp::GT:
      case QueryConditionOp::GE:
        throw std::logic_error(
            "Cannot apply an inequality operator against an unordered "
            "Enumeration");
      default:
        break;
    }
  }

  auto val_size = datatype_size(attr->type());

  if (op_ != QueryConditionOp::IN && op_ != QueryConditionOp::NOT_IN) {
    auto idx = enumeration->index_of(get_value_ptr(), get_value_size());
    if (idx == constants::enumeration_missing_value) {
      if (op_ == QueryConditionOp::NE) {
        op_ = QueryConditionOp::ALWAYS_TRUE;
      } else {
        op_ = QueryConditionOp::ALWAYS_FALSE;
      }
      data_ = ByteVecValue(val_size);
      utils::safe_integral_cast_to_datatype(0, attr->type(), data_);
    } else {
      data_ = ByteVecValue(val_size);
      utils::safe_integral_cast_to_datatype(idx, attr->type(), data_);
    }
  } else {
    // Buffers and writers for the new data/offsets memory
    std::vector<uint8_t> data_buffer(val_size * members_.size());
    std::vector<uint8_t> offsets_buffer(
        constants::cell_var_offset_size * members_.size());
    Serializer data_writer(data_buffer.data(), data_buffer.size());
    Serializer offsets_writer(offsets_buffer.data(), offsets_buffer.size());

    ByteVecValue curr_data(val_size);
    uint64_t curr_offset = 0;
    uint64_t num_offsets = 0;

    for (auto& member : members_) {
      auto idx = enumeration->index_of(member.data(), member.size());
      if (idx == constants::enumeration_missing_value) {
        continue;
      }

      utils::safe_integral_cast_to_datatype(idx, attr->type(), curr_data);
      data_writer.write(curr_data.data(), curr_data.size());
      offsets_writer.write(curr_offset);
      curr_offset += val_size;
      num_offsets += 1;
    }

    auto total_data_size = curr_offset;
    auto total_offsets_size = num_offsets * constants::cell_var_offset_size;

    data_ = ByteVecValue(total_data_size);
    std::memcpy(data_.data(), data_buffer.data(), total_data_size);

    offsets_ = ByteVecValue(total_offsets_size);
    std::memcpy(offsets_.data(), offsets_buffer.data(), total_offsets_size);

    generate_members();
  }

  use_enumeration_ = false;
}

Status ASTNodeVal::check_node_validity(const ArraySchema& array_schema) const {
  // Ensure that the field exists.
  if (!array_schema.is_field(field_name_)) {
    return Status_QueryConditionError("Field doesn't exist");
  }

  const auto nullable = array_schema.is_nullable(field_name_);
  const auto var_size = array_schema.var_size(field_name_);
  const auto type = array_schema.type(field_name_);
  const auto cell_size = array_schema.cell_size(field_name_);
  const auto cell_val_num = array_schema.cell_val_num(field_name_);

  bool has_enumeration = false;
  if (array_schema.is_attr(field_name_)) {
    auto attr = array_schema.attribute(field_name_);
    has_enumeration = attr->get_enumeration_name().has_value();
  }

  // Ensure that null value can only be used with equality operators.
  if (is_null_) {
    if (op_ != QueryConditionOp::EQ && op_ != QueryConditionOp::NE) {
      return Status_QueryConditionError(
          "Null value can only be used with equality operators");
    }

    // Ensure that an attribute that is marked as nullable
    // corresponds to a type that is nullable.
    if ((!nullable) && !supported_string_type(type)) {
      return Status_QueryConditionError(
          "Null value can only be used with nullable attributes");
    }
  }

  // Ensure that non-empty attributes are only var-sized for
  // ASCII and UTF-8 strings.
  if (var_size && !supported_string_type(type) && !is_null_) {
    return Status_QueryConditionError(
        "Value node non-empty attribute may only be var-sized for ASCII "
        "strings: " +
        field_name_);
  }

  // Ensure that non string fixed size attributes store only one value per cell.
  if (cell_val_num != 1 && !supported_string_type(type) && !var_size) {
    return Status_QueryConditionError(
        "Value node attribute must have one value per cell for non-string "
        "fixed size attributes: " +
        field_name_);
  }

  // Ensure that the condition value size matches the attribute's
  // value size.
  if (cell_size != constants::var_size && cell_size != data_.size() &&
      !(nullable && is_null_) && !supported_string_type(type) && (!var_size) &&
      !(op_ == QueryConditionOp::IN || op_ == QueryConditionOp::NOT_IN)) {
    return Status_QueryConditionError(
        "Value node condition value size mismatch: " +
        std::to_string(cell_size) + " != " + std::to_string(data_.size()));
  }

  // When applying a set membership test against a fixed size field that
  // does not have an enumeration, ensure that all set members have the
  // correct size.
  if (cell_size != constants::var_size && !has_enumeration &&
      (op_ == QueryConditionOp::IN || op_ == QueryConditionOp::NOT_IN)) {
    for (auto& member : members_) {
      if (member.size() != cell_size) {
        throw Status_QueryConditionError(
            "Value node set memmber size mismatch: " +
            std::to_string(cell_size) + " != " + std::to_string(member.size()));
      }
    }
  }

  // Ensure that the attribute type is valid.
  switch (type) {
    case Datatype::ANY:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
      return Status_QueryConditionError(
          "Unsupported value node attribute type " + datatype_str(type) +
          " on field " + field_name_);
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
    ast_nodes.push_back(clone());
    ast_nodes.push_back(rhs->clone());
  } else {  // rhs is an expression node.
    // lhs is a simple tree, rhs is a compound tree.
    ast_nodes.push_back(clone());
    if (rhs->get_combination_op() == combination_op) {
      for (const auto& elem : rhs->get_children()) {
        ast_nodes.push_back(elem->clone());
      }
    } else {
      ast_nodes.push_back(rhs->clone());
    }
  }
  return tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeExpr, std::move(ast_nodes), combination_op));
}

const std::string& ASTNodeVal::get_field_name() const {
  return field_name_;
}

const void* ASTNodeVal::get_value_ptr() const {
  if (is_null_) {
    return nullptr;
  }

  if (op_ == QueryConditionOp::IN || op_ == QueryConditionOp::NOT_IN) {
    return &members_;
  }

  if (data_.size() == 0) {
    return static_cast<const void*>("");
  }

  return data_.data();
}

uint64_t ASTNodeVal::get_value_size() const {
  if (is_null_) {
    return 0;
  }

  if (op_ == QueryConditionOp::IN || op_ == QueryConditionOp::NOT_IN) {
    return 0;
  }

  return data_.size();
}

const ByteVecValue& ASTNodeVal::get_data() const {
  return data_;
}

const ByteVecValue& ASTNodeVal::get_offsets() const {
  return offsets_;
}

const QueryConditionOp& ASTNodeVal::get_op() const {
  return op_;
}

const std::vector<tdb_unique_ptr<ASTNode>>& ASTNodeVal::get_children() const {
  throw std::runtime_error(
      "ASTNodeVal::get_children: Cannot get children from an AST value node.");
}

const QueryConditionCombinationOp& ASTNodeVal::get_combination_op() const {
  throw std::runtime_error(
      "ASTNodeVal::get_combination_op: Cannot get combination op from an AST "
      "value node.");
}

bool ASTNodeVal::use_enumeration() const {
  return use_enumeration_;
}

void ASTNodeVal::set_use_enumeration(bool use_enumeration) {
  use_enumeration_ = use_enumeration;
}

void ASTNodeVal::generate_members() {
  // Non-set conditions don't genereate members.
  if (op_ != QueryConditionOp::IN && op_ != QueryConditionOp::NOT_IN) {
    return;
  }

  members_.clear();

  // This two phase static_cast is needed to convert from offsets_'s internal
  // uint8_t pointer into a uint64_t pointer.
  auto void_offsets = static_cast<const void*>(offsets_.data());
  auto offset_elems = static_cast<const uint64_t*>(void_offsets);
  uint64_t num_offsets = offsets_.size() / constants::cell_var_offset_size;

  for (uint64_t i = 0; i < num_offsets; i++) {
    uint64_t start = offset_elems[i];
    uint64_t length;

    if (i + 1 < num_offsets) {
      length = offset_elems[i + 1] - offset_elems[i];
    } else {
      length = data_.size() - offset_elems[i];
    }

    // This two phase static_cast is needed to convert from data_'s internal
    // uint8_t pointer into a char pointer.
    auto void_data = static_cast<void*>(data_.data() + start);
    auto data_ptr = static_cast<char*>(void_data);
    auto member = std::string_view(data_ptr, length);
    members_.insert(member);
  }
}

bool ASTNodeExpr::is_expr() const {
  return true;
}

tdb_unique_ptr<ASTNode> ASTNodeExpr::clone() const {
  return tdb_unique_ptr<ASTNode>(tdb_new(ASTNodeExpr, *this));
}

tdb_unique_ptr<ASTNode> ASTNodeExpr::get_negated_tree() const {
  return tdb_unique_ptr<ASTNode>(tdb_new(ASTNodeExpr, *this, ASTNegation));
}

void ASTNodeExpr::get_field_names(
    std::unordered_set<std::string>& field_name_set) const {
  for (const auto& child : nodes_) {
    child->get_field_names(field_name_set);
  }
}

void ASTNodeExpr::get_enumeration_field_names(
    std::unordered_set<std::string>& field_name_set) const {
  for (const auto& child : nodes_) {
    child->get_enumeration_field_names(field_name_set);
  }
}

bool ASTNodeExpr::is_backwards_compatible() const {
  if (combination_op_ != QueryConditionCombinationOp::AND) {
    return false;
  }
  for (const auto& child : nodes_) {
    if (child->is_expr()) {
      return false;
    }
    if (!child->is_backwards_compatible()) {
      return false;
    }
  }
  return true;
}

void ASTNodeExpr::rewrite_enumeration_conditions(
    const ArraySchema& array_schema) {
  for (auto& child : nodes_) {
    child->rewrite_enumeration_conditions(array_schema);
  }
}

Status ASTNodeExpr::check_node_validity(const ArraySchema& array_schema) const {
  // If the node is a compound expression node, ensure there are at least
  // two children in the node and then run a check on each child node.
  if (nodes_.size() < 2) {
    return Status_QueryConditionError(
        "Non value AST node does not have at least 2 children.");
  }
  for (const auto& child : nodes_) {
    RETURN_NOT_OK(child->check_node_validity(array_schema));
  }

  return Status::Ok();
}

tdb_unique_ptr<ASTNode> ASTNodeExpr::combine(
    const tdb_unique_ptr<ASTNode>& rhs,
    const QueryConditionCombinationOp& combination_op) {
  std::vector<tdb_unique_ptr<ASTNode>> ast_nodes;
  if (combination_op == combination_op_) {
    for (const auto& child : nodes_) {
      ast_nodes.push_back(child->clone());
    }
  } else {
    ast_nodes.push_back(clone());
  }

  if (rhs->is_expr() && combination_op == rhs->get_combination_op()) {
    for (const auto& child : rhs->get_children()) {
      ast_nodes.push_back(child->clone());
    }
  } else {
    ast_nodes.push_back(rhs->clone());
  }

  return tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeExpr, std::move(ast_nodes), combination_op));
}

const std::string& ASTNodeExpr::get_field_name() const {
  throw std::runtime_error(
      "ASTNodeExpr::get_field_name: Cannot get field name from an AST "
      "expression node.");
}

const void* ASTNodeExpr::get_value_ptr() const {
  throw std::runtime_error(
      "ASTNodeExpr::get_value_ptr: Cannot get a value pointer from "
      "an AST expression node.");
}

uint64_t ASTNodeExpr::get_value_size() const {
  throw std::runtime_error(
      "ASTNodeExpr::get_value_size: Cannot get a value size from "
      "an AST expression node.");
}

const ByteVecValue& ASTNodeExpr::get_data() const {
  throw std::runtime_error(
      "ASTNodeExpr::get_data: Cannot get data from an AST expression node.");
}

const ByteVecValue& ASTNodeExpr::get_offsets() const {
  throw std::runtime_error(
      "ASTNodeExpr::get_data: Cannot get offsets from an AST expression node.");
}

const QueryConditionOp& ASTNodeExpr::get_op() const {
  throw std::runtime_error(
      "ASTNodeExpr::get_op: Cannot get op from an AST expression node.");
}

const std::vector<tdb_unique_ptr<ASTNode>>& ASTNodeExpr::get_children() const {
  return nodes_;
}

const QueryConditionCombinationOp& ASTNodeExpr::get_combination_op() const {
  return combination_op_;
}

bool ASTNodeExpr::use_enumeration() const {
  throw std::runtime_error(
      "ASTNodeExpr::use_enumeration: Cannot get enumeration status from "
      "an AST expression node.");
}

void ASTNodeExpr::set_use_enumeration(bool use_enumeration) {
  for (auto& child : nodes_) {
    child->set_use_enumeration(use_enumeration);
  }
}

}  // namespace tiledb::sm
