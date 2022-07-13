/**
 * @file tiledb/sm/query/deletes_and_updates/serialization.cc
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
 * This file contains functions for serializing deletes and updates data
 * to/from disk.
 */
#include "serialization.h"
#include "tiledb/storage_format/serialization/serializers.h"

namespace tiledb::sm::deletes_and_updates::serialization {

void serialize_condition_impl(
    const tdb_unique_ptr<tiledb::sm::ASTNode>& node, Serializer& serializer) {
  if (node == nullptr) {
    return;
  }

  // Serialize is_expr.
  const auto node_type =
      node->is_expr() ? NodeType::EXPRESSION : NodeType::VALUE;
  serializer.write(static_cast<uint8_t>(node_type));
  if (!node->is_expr()) {
    // Get values.
    const auto op = node->get_op();
    const auto field_name = node->get_field_name();
    const uint32_t field_name_length =
        static_cast<uint32_t>(field_name.length());
    const auto& value = node->get_condition_value_view();
    const storage_size_t value_length = value.size();
    // Serialize op.
    serializer.write(op);

    // Serialize field name.
    serializer.write(field_name_length);
    serializer.write(field_name.data(), field_name_length);

    // Serialize value.
    serializer.write(value_length);
    serializer.write(value.content(), value.size());
  } else {
    // Get values.
    const auto& nodes = node->get_children();
    const auto nodes_size = nodes.size();
    const auto combinatin_op = node->get_combination_op();

    // Serialize combination op.
    serializer.write(combinatin_op);

    // Serialize children num.
    serializer.write(nodes_size);

    for (storage_size_t i = 0; i < nodes.size(); i++) {
      serialize_condition_impl(nodes[i], serializer);
    }
  }
}

storage_size_t get_serialized_condition_size(
    const tdb_unique_ptr<tiledb::sm::ASTNode>& node) {
  if (node == nullptr) {
    return 0;
  }

  Serializer serializer;
  serialize_condition_impl(node, serializer);

  return serializer.size();
}

std::vector<uint8_t> serialize_condition(
    const QueryCondition& query_condition) {
  std::vector<uint8_t> ret(
      get_serialized_condition_size(query_condition.ast()));

  Serializer serializer(ret.data(), ret.size());
  serialize_condition_impl(query_condition.ast(), serializer);
  serializer.ensure_full_buffer_written();

  return ret;
}

tdb_unique_ptr<ASTNode> deserialize_condition_impl(Deserializer& deserializer) {
  // Deserialize is_expr.
  auto node_type = deserializer.read<NodeType>();
  if (node_type == NodeType::VALUE) {
    // Deserialize op.
    auto op = deserializer.read<QueryConditionOp>();
    ensure_qc_op_is_valid(op);

    // Deserialize field name.
    auto field_name_size = deserializer.read<uint32_t>();
    auto field_name_data = deserializer.get_ptr<char>(field_name_size);
    std::string field_name(field_name_data, field_name_size);

    // Deserialize value.
    auto value_size = deserializer.read<storage_size_t>();
    auto value_data = deserializer.get_ptr<void>(value_size);

    return tdb_unique_ptr<ASTNode>(
        tdb_new(ASTNodeVal, field_name, value_data, value_size, op));
  } else if (node_type == NodeType::EXPRESSION) {
    // Deserialize combination op.
    auto combination_op = deserializer.read<QueryConditionCombinationOp>();
    ensure_qc_combo_op_is_valid(combination_op);

    // Deserialize children num.
    auto nodes_size = deserializer.read<storage_size_t>();

    std::vector<tdb_unique_ptr<ASTNode>> ast_nodes;
    for (storage_size_t i = 0; i < nodes_size; i++) {
      ast_nodes.push_back(deserialize_condition_impl(deserializer));
    }

    return tdb_unique_ptr<ASTNode>(
        tdb_new(ASTNodeExpr, std::move(ast_nodes), combination_op));
  } else {
    throw std::logic_error("Cannot deserialize, unknown node type.");
  }
}

QueryCondition deserialize_condition(
    const std::string& condition_marker,
    const void* buff,
    const storage_size_t size) {
  Deserializer deserializer(buff, size);
  return QueryCondition(
      condition_marker, deserialize_condition_impl(deserializer));
}

}  // namespace tiledb::sm::deletes_and_updates::serialization
