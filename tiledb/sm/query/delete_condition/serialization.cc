/**
 * @file tiledb/sm/query/delete_condition/serialization.cc
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
 * This file contains functions for parsing URIs for storage of an array.
 */
#include "serialization.h"

namespace tiledb::sm::delete_condition::serialize {

storage_size_t get_serialized_condition_size(
    const tdb_unique_ptr<tiledb::sm::ASTNode>& node) {
  if (node == nullptr) {
    return 0;
  }

  storage_size_t size = sizeof(bool);  // is_expr
  if (!node->is_expr()) {
    size += sizeof(QueryConditionOp);                 // Query condition op.
    size += sizeof(storage_size_t);                   // Field name size.
    size += node->get_field_name().length();          // Field name.
    size += sizeof(storage_size_t);                   // Value size.
    size += node->get_condition_value_view().size();  // Value.
    return size;
  } else {
    const auto& nodes = node->get_children();
    size += sizeof(QueryConditionCombinationOp);  // Query condition op.
    size += sizeof(storage_size_t);               // Children num.
    for (storage_size_t i = 0; i < nodes.size(); i++) {
      size += get_serialized_condition_size(nodes[i]);
    }
  }

  return size;
}

void serialize_delete_condition_impl(
    const tdb_unique_ptr<tiledb::sm::ASTNode>& node,
    std::vector<uint8_t>& buff,
    storage_size_t& idx) {
  if (node == nullptr) {
    return;
  }

  assert(idx <= buff.size());

  // Serialize is_expr.
  const auto node_type =
      node->is_expr() ? NodeType::EXPRESSION : NodeType::VALUE;
  buff[idx++] = static_cast<uint8_t>(node_type);
  if (!node->is_expr()) {
    // Get values.
    const auto op = node->get_op();
    const auto field_name_length = node->get_field_name().length();
    const auto& value = node->get_condition_value_view();
    const auto value_length = value.size();
    assert(
        idx + sizeof(op) + sizeof(field_name_length) + field_name_length +
            sizeof(value_length) + value_length <=
        buff.size());

    // Serialize op.
    memcpy(&buff[idx], &op, sizeof(op));
    idx += sizeof(op);

    // Serialize field name, size then value.
    memcpy(&buff[idx], &field_name_length, sizeof(field_name_length));
    idx += sizeof(field_name_length);
    memcpy(&buff[idx], node->get_field_name().data(), field_name_length);
    idx += field_name_length;

    // Serialize value, size then content.
    memcpy(&buff[idx], &value_length, sizeof(value_length));
    idx += sizeof(value_length);
    memcpy(&buff[idx], value.content(), value_length);
    idx += value_length;
  } else {
    // Get values.
    const auto& nodes = node->get_children();
    const auto nodes_size = nodes.size();
    const auto combinatin_op = node->get_combination_op();
    assert(idx + sizeof(nodes_size) + sizeof(combinatin_op) <= buff.size());

    // Serialize combination op.
    memcpy(&buff[idx], &combinatin_op, sizeof(combinatin_op));
    idx += sizeof(combinatin_op);

    // Serialize children num.
    memcpy(&buff[idx], &nodes_size, sizeof(nodes_size));
    idx += sizeof(nodes_size);

    for (storage_size_t i = 0; i < nodes.size(); i++) {
      serialize_delete_condition_impl(nodes[i], buff, idx);
    }
  }
}

std::vector<uint8_t> serialize_delete_condition(
    const QueryCondition& query_condition) {
  std::vector<uint8_t> ret(
      get_serialized_condition_size(query_condition.ast()));

  storage_size_t size = 0;
  serialize_delete_condition_impl(query_condition.ast(), ret, size);

  return ret;
}

tdb_unique_ptr<ASTNode> deserialize_delete_condition_impl(
    const std::vector<uint8_t>& buff, storage_size_t& idx) {
  assert(idx < buff.size());

  // Deserialize is_expr.
  NodeType node_type = static_cast<NodeType>(buff[idx++]);
  if (node_type == NodeType::VALUE) {
    // Deserialize op.
    auto op = QueryConditionOp::LT;
    ;
    memcpy(&op, &buff[idx], sizeof(op));
    idx += sizeof(op);
    ensure_qc_op_is_valid(op);

    // Deserialize field name, size then value.
    storage_size_t field_name_length;
    memcpy(&field_name_length, &buff[idx], sizeof(field_name_length));
    idx += sizeof(field_name_length);
    auto field_name_value = reinterpret_cast<const char*>(&buff[idx]);
    idx += field_name_length;
    auto field_name = std::string(field_name_value, field_name_length);

    // Deserialize value, size then content.
    storage_size_t value_length;
    memcpy(&value_length, &buff[idx], sizeof(value_length));
    idx += sizeof(value_length);
    const void* value = &buff[idx];
    idx += value_length;

    return tdb_unique_ptr<ASTNode>(
        tdb_new(ASTNodeVal, field_name, value, value_length, op));
  } else if (node_type == NodeType::EXPRESSION) {
    // Deserialize combination op.
    auto combination_op = QueryConditionCombinationOp::AND;
    memcpy(&combination_op, &buff[idx], sizeof(combination_op));
    idx += sizeof(combination_op);
    ensure_qc_combo_op_is_valid(combination_op);

    // Deserialize children num.
    storage_size_t nodes_size = 0;
    memcpy(&nodes_size, &buff[idx], sizeof(nodes_size));
    idx += sizeof(nodes_size);

    std::vector<tdb_unique_ptr<ASTNode>> ast_nodes;
    for (storage_size_t i = 0; i < nodes_size; i++) {
      ast_nodes.push_back(deserialize_delete_condition_impl(buff, idx));
    }

    return tdb_unique_ptr<ASTNode>(
        tdb_new(ASTNodeExpr, std::move(ast_nodes), combination_op));
  } else {
    throw std::logic_error("Cannot deserialize, unknown node type.");
  }
}

QueryCondition deserialize_delete_condition(const std::vector<uint8_t>& buff) {
  QueryCondition query_condition;
  storage_size_t idx = 0;
  query_condition.set_ast(deserialize_delete_condition_impl(buff, idx));
  return query_condition;
}

}  // namespace tiledb::sm::delete_condition::serialize
