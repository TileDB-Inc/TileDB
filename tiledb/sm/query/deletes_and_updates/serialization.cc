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
  serializer.write<uint8_t>(static_cast<uint8_t>(node_type));
  if (!node->is_expr()) {
    // Get values.
    const auto op = node->get_op();
    const auto field_name = node->get_field_name();
    const uint32_t field_name_length =
        static_cast<uint32_t>(field_name.length());
    const auto& data = node->get_data();
    const auto& offsets = node->get_offsets();

    // Serialize op.
    serializer.write<uint8_t>(static_cast<uint8_t>(op));

    // Serialize field name.
    serializer.write<uint32_t>(field_name_length);
    serializer.write(field_name.data(), field_name_length);

    // Serialize data.
    serializer.write<uint64_t>(data.size());
    serializer.write(data.data(), data.size());

    // Serialize offsets for sets
    if (op == QueryConditionOp::IN || op == QueryConditionOp::NOT_IN) {
      serializer.write<uint64_t>(offsets.size());
      serializer.write(offsets.data(), offsets.size());
    }
  } else {
    // Get values.
    const auto& nodes = node->get_children();
    const auto nodes_size = nodes.size();
    const auto combinatin_op = node->get_combination_op();

    // Serialize combination op.
    serializer.write<uint8_t>(static_cast<uint8_t>(combinatin_op));

    // Serialize children num.
    serializer.write<size_t>(nodes_size);

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

  SizeComputationSerializer size_computation_serializer;
  serialize_condition_impl(node, size_computation_serializer);

  return size_computation_serializer.size();
}

shared_ptr<WriterTile> serialize_condition(
    const QueryCondition& query_condition,
    shared_ptr<MemoryTracker> memory_tracker) {
  auto tile{WriterTile::from_generic(
      get_serialized_condition_size(query_condition.ast()), memory_tracker)};

  Serializer serializer(tile->data(), tile->size());
  serialize_condition_impl(query_condition.ast(), serializer);

  return tile;
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
    auto data_size = deserializer.read<storage_size_t>();
    auto data = deserializer.get_ptr<void>(data_size);

    if (op != QueryConditionOp::IN && op != QueryConditionOp::NOT_IN) {
      return tdb_unique_ptr<ASTNode>(
          tdb_new(ASTNodeVal, field_name, data, data_size, op));
    }

    // For sets we have to deserialize the offsets
    auto offsets_size = deserializer.read<storage_size_t>();
    auto offsets = deserializer.get_ptr<void>(offsets_size);

    return tdb_unique_ptr<ASTNode>(tdb_new(
        ASTNodeVal, field_name, data, data_size, offsets, offsets_size, op));
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
    const uint64_t condition_index,
    const std::string& condition_marker,
    const void* buff,
    const storage_size_t size) {
  Deserializer deserializer(buff, size);
  return QueryCondition(
      condition_index,
      condition_marker,
      deserialize_condition_impl(deserializer));
}

void serialize_update_values_impl(
    const std::vector<UpdateValue>& update_values, Serializer& serializer) {
  serializer.write<uint64_t>(update_values.size());
  for (const auto& update_value : update_values) {
    uint64_t field_name_size = update_value.field_name().length();
    serializer.write<uint64_t>(field_name_size);
    serializer.write(update_value.field_name().data(), field_name_size);

    uint64_t value_size = update_value.view().size();
    serializer.write<uint64_t>(value_size);
    serializer.write(update_value.view().content(), value_size);
  }
}

storage_size_t get_serialized_update_condition_and_values_size(
    const tdb_unique_ptr<tiledb::sm::ASTNode>& node,
    const std::vector<UpdateValue>& update_values) {
  SizeComputationSerializer size_computation_serializer;
  serialize_condition_impl(node, size_computation_serializer);
  serialize_update_values_impl(update_values, size_computation_serializer);

  return size_computation_serializer.size();
}

shared_ptr<WriterTile> serialize_update_condition_and_values(
    const QueryCondition& query_condition,
    const std::vector<UpdateValue>& update_values,
    shared_ptr<MemoryTracker> memory_tracker) {
  auto tile{WriterTile::from_generic(
      get_serialized_update_condition_and_values_size(
          query_condition.ast(), update_values),
      memory_tracker)};

  Serializer serializer(tile->data(), tile->size());
  serialize_condition_impl(query_condition.ast(), serializer);
  serialize_update_values_impl(update_values, serializer);

  return tile;
}
std::vector<UpdateValue> deserialize_update_values_impl(
    Deserializer& deserializer) {
  auto num = deserializer.read<uint64_t>();
  std::vector<UpdateValue> ret;
  ret.reserve(num);
  for (uint64_t i = 0; i < num; i++) {
    uint64_t field_name_size = deserializer.read<uint64_t>();
    std::string field_name;
    field_name.resize(field_name_size);
    deserializer.read(field_name.data(), field_name_size);

    uint64_t value_size = deserializer.read<uint64_t>();
    std::vector<uint8_t> value(value_size);
    deserializer.read(value.data(), value_size);

    ret.emplace_back(field_name, value.data(), value_size);
  }

  return ret;
}

tuple<QueryCondition, std::vector<UpdateValue>>
deserialize_update_condition_and_values(
    const uint64_t condition_index,
    const std::string& condition_marker,
    const void* buff,
    const storage_size_t size) {
  Deserializer deserializer(buff, size);
  return {
      QueryCondition(
          condition_index,
          condition_marker,
          deserialize_condition_impl(deserializer)),
      deserialize_update_values_impl(deserializer)};
}

}  // namespace tiledb::sm::deletes_and_updates::serialization
