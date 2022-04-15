/**
 * @file query_condition.cc
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
 * Implements the QueryCondition class.
 */

#include "tiledb/sm/query/query_condition.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/misc/utils.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

QueryCondition::QueryCondition() {
}

QueryCondition::QueryCondition(const QueryCondition& rhs)
    : tree_(rhs.tree_ == nullptr ? nullptr : rhs.tree_->clone()) {
}

QueryCondition::QueryCondition(QueryCondition&& rhs)
    : tree_(std::move(rhs.tree_)) {
}

QueryCondition::~QueryCondition() {
}

QueryCondition& QueryCondition::operator=(const QueryCondition& rhs) {
  if (this != &rhs) {
    tree_ = rhs.tree_ == nullptr ? nullptr : rhs.tree_->clone();
  }

  return *this;
}

QueryCondition& QueryCondition::operator=(QueryCondition&& rhs) {
  tree_ = std::move(rhs.tree_);
  return *this;
}

Status QueryCondition::init(
    std::string&& field_name,
    const void* const condition_value,
    const uint64_t condition_value_size,
    const QueryConditionOp& op) {
  if (tree_) {
    return Status_QueryConditionError("Cannot reinitialize query condition");
  }

  // AST Construction.
  tree_ = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal, field_name, condition_value, condition_value_size, op));

  return Status::Ok();
}

Status QueryCondition::check(const ArraySchema& array_schema) const {
  if (!tree_)
    return Status::Ok();
  RETURN_NOT_OK(tree_->check_node_validity(array_schema));
  return Status::Ok();
}

Status QueryCondition::combine(
    const QueryCondition& rhs,
    const QueryConditionCombinationOp combination_op,
    QueryCondition* const combined_cond) const {
  if (combination_op != QueryConditionCombinationOp::AND &&
      combination_op != QueryConditionCombinationOp::OR) {
    return Status_QueryConditionError(
        "Cannot combine query conditions; Only the 'AND' "
        "and 'OR' combination ops are supported");
  }

  combined_cond->field_names_.clear();
  combined_cond->clauses_.clear();
  combined_cond->combination_ops_.clear();
  combined_cond->tree_ = this->tree_->combine(rhs.tree_, combination_op);
  return Status::Ok();
}

bool QueryCondition::empty() const {
  return tree_ == nullptr;
}

std::unordered_set<std::string>& QueryCondition::field_names() const {
  if (field_names_.empty() && tree_ != nullptr) {
    tree_->get_field_names(field_names_);
  }

  return field_names_;
}

/** Full template specialization for `char*` and `QueryConditionOp::LT`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::LT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size < rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::LE. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::LE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size <= rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::GT`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::GT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size > rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::GE`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::GE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size >= rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::EQ`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::EQ> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == rhs) {
      return true;
    }

    if (lhs == nullptr || rhs == nullptr) {
      return false;
    }

    if (lhs_size != rhs_size) {
      return false;
    }

    return strncmp(
               static_cast<const char*>(lhs),
               static_cast<const char*>(rhs),
               lhs_size) == 0;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::NE`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::NE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (rhs == nullptr && lhs != nullptr) {
      return true;
    }

    if (lhs == nullptr || rhs == nullptr) {
      return false;
    }

    if (lhs_size != rhs_size) {
      return true;
    }

    return strncmp(
               static_cast<const char*>(lhs),
               static_cast<const char*>(rhs),
               lhs_size) != 0;
  }
};

/** Partial template specialization for `QueryConditionOp::LT`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::LT> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return lhs != nullptr &&
           *static_cast<const T*>(lhs) < *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::LE`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::LE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return lhs != nullptr &&
           *static_cast<const T*>(lhs) <= *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::GT`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::GT> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return lhs != nullptr &&
           *static_cast<const T*>(lhs) > *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::GE`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::GE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return lhs != nullptr &&
           *static_cast<const T*>(lhs) >= *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::EQ`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::EQ> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    if (lhs == rhs) {
      return true;
    }

    if (lhs == nullptr || rhs == nullptr) {
      return false;
    }

    return *static_cast<const T*>(lhs) == *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::NE`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::NE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    if (rhs == nullptr && lhs != nullptr) {
      return true;
    }

    if (lhs == nullptr || rhs == nullptr) {
      return false;
    }

    return *static_cast<const T*>(lhs) != *static_cast<const T*>(rhs);
  }
};

template <typename T, QueryConditionOp Op>
void QueryCondition::apply_ast_node(
    const tdb_unique_ptr<ASTNode>& node,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<uint8_t>& result_cell_bitmap) const {
  const std::string& field_name = node->get_field_name();
  const void* condition_value_content =
      node->get_condition_value_view().content();
  const size_t condition_value_size = node->get_condition_value_view().size();
  uint64_t starting_index = 0;
  for (const auto& rcs : result_cell_slabs) {
    ResultTile* const result_tile = rcs.tile_;
    const uint64_t start = rcs.start_;
    const uint64_t length = rcs.length_;

    // Handle an empty range.
    if (result_tile == nullptr && !nullable) {
      const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
          fill_value.data(),
          fill_value.size(),
          condition_value_content,
          condition_value_size);
      if (!cmp) {
        for (size_t c = starting_index; c < starting_index + length; ++c) {
          result_cell_bitmap[c] = 0;
        }
      }
    } else {
      const auto tile_tuple = result_tile->tile_tuple(field_name);
      uint8_t* buffer_validity = nullptr;

      if (nullable) {
        const auto& tile_validity = std::get<2>(*tile_tuple);
        buffer_validity = static_cast<uint8_t*>(tile_validity.data());
      }

      // Start the pending result cell slab at the start position
      // of the current result cell slab.
      uint64_t c = 0;

      if (var_size) {
        const auto& tile = std::get<1>(*tile_tuple);
        const char* buffer = static_cast<char*>(tile.data());
        const uint64_t buffer_size = tile.size();

        const auto& tile_offsets = std::get<0>(*tile_tuple);
        const uint64_t* buffer_offsets =
            static_cast<uint64_t*>(tile_offsets.data());
        const uint64_t buffer_offsets_el =
            tile_offsets.size() / constants::cell_var_offset_size;

        // Iterate through each cell in this slab.
        while (c < length) {
          const uint64_t buffer_offset = buffer_offsets[start + c * stride];
          const uint64_t next_cell_offset =
              (start + c * stride + 1 < buffer_offsets_el) ?
                  buffer_offsets[start + c * stride + 1] :
                  buffer_size;
          const uint64_t cell_size = next_cell_offset - buffer_offset;

          const bool null_cell =
              (nullable && buffer_validity[start + c * stride] == 0);

          // Get the cell value.
          const void* const cell_value =
              null_cell ? nullptr : buffer + buffer_offset;

          // Compare the cell value against the value in the value node.
          const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
              cell_value,
              cell_size,
              condition_value_content,
              condition_value_size);
          result_cell_bitmap[starting_index + c] &= (uint8_t)cmp;
          ++c;
        }
      } else {
        const auto& tile = std::get<0>(*tile_tuple);
        const char* buffer = static_cast<char*>(tile.data());
        const uint64_t cell_size = tile.cell_size();
        uint64_t buffer_offset = start * cell_size;
        const uint64_t buffer_offset_inc = stride * cell_size;

        // Iterate through each cell in this slab.
        while (c < length) {
          const bool null_cell =
              nullable && buffer_validity[start + c * stride] == 0;

          // Get the cell value.
          const void* const cell_value =
              null_cell ? nullptr : buffer + buffer_offset;
          buffer_offset += buffer_offset_inc;

          // Compare the cell value against the value in the value node.
          const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
              cell_value,
              cell_size,
              condition_value_content,
              condition_value_size);
          result_cell_bitmap[starting_index + c] &= (uint8_t)cmp;
          ++c;
        }
      }
    }
    starting_index += length;
  }
}

template <typename T>
void QueryCondition::apply_ast_node(
    const tdb_unique_ptr<ASTNode>& node,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<uint8_t>& result_cell_bitmap) const {
  switch (node->get_op()) {
    case QueryConditionOp::LT:
      apply_ast_node<T, QueryConditionOp::LT>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
      break;
    case QueryConditionOp::LE:
      apply_ast_node<T, QueryConditionOp::LE>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
      break;
    case QueryConditionOp::GT:
      apply_ast_node<T, QueryConditionOp::GT>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
      break;
    case QueryConditionOp::GE:
      apply_ast_node<T, QueryConditionOp::GE>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
      break;
    case QueryConditionOp::EQ:
      apply_ast_node<T, QueryConditionOp::EQ>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
      break;
    case QueryConditionOp::NE:
      apply_ast_node<T, QueryConditionOp::NE>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
      break;
    default:
      throw std::runtime_error(
          "QueryCondition::apply_ast_node: Cannot perform query comparison; "
          "Unknown query condition operator.");
  }

  return;
}

void QueryCondition::apply_ast_node(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    const uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<uint8_t>& result_cell_bitmap) const {
  const auto attribute = array_schema.attribute(node->get_field_name());
  if (!attribute) {
    throw std::runtime_error(
        "QueryCondition::apply_ast_node: Unknown attribute " +
        node->get_field_name());
  }

  const ByteVecValue fill_value = attribute->fill_value();
  const bool var_size = attribute->var_size();
  const bool nullable = attribute->nullable();
  switch (attribute->type()) {
    case Datatype::INT8: {
      apply_ast_node<int8_t>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::UINT8: {
      apply_ast_node<uint8_t>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::INT16: {
      apply_ast_node<int16_t>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::UINT16: {
      apply_ast_node<uint16_t>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::INT32: {
      apply_ast_node<int32_t>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::UINT32: {
      apply_ast_node<uint32_t>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::INT64: {
      apply_ast_node<int64_t>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::UINT64: {
      apply_ast_node<uint64_t>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::FLOAT32: {
      apply_ast_node<float>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::FLOAT64: {
      apply_ast_node<double>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::STRING_ASCII: {
      apply_ast_node<char*>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::CHAR: {
      if (var_size) {
        apply_ast_node<char*>(
            node,
            stride,
            var_size,
            nullable,
            fill_value,
            result_cell_slabs,
            result_cell_bitmap);
      } else {
        apply_ast_node<char>(
            node,
            stride,
            var_size,
            nullable,
            fill_value,
            result_cell_slabs,
            result_cell_bitmap);
      }
    } break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS: {
      apply_ast_node<int64_t>(
          node,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          result_cell_bitmap);
    } break;
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      throw std::runtime_error(
          "QueryCondition::apply_ast_node: Cannot perform query comparison; "
          "Unsupported query "
          "conditional type on " +
          node->get_field_name());
  }

  return;
}

void QueryCondition::apply_tree(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<uint8_t>& result_cell_bitmap) const {
  // Case on the type of the tree node.
  if (!node->is_expr()) {
    // In the simple value node case, run the value evaluator function and
    // return the result back.
    apply_ast_node(
        node, array_schema, stride, result_cell_slabs, result_cell_bitmap);
  } else {
    // For each child, declare a new result buffer, combine it all into the
    // other one based on the combination op.
    const QueryConditionCombinationOp& combination_op =
        node->get_combination_op();
    if (combination_op == QueryConditionCombinationOp::AND) {
      std::fill(result_cell_bitmap.begin(), result_cell_bitmap.end(), 1);
    } else if (combination_op == QueryConditionCombinationOp::OR) {
      std::fill(result_cell_bitmap.begin(), result_cell_bitmap.end(), 0);
    }

    for (const auto& child : node->get_children()) {
      std::vector<uint8_t> child_result_cell_bitmap(
          result_cell_bitmap.size(), 1);
      apply_tree(
          child,
          array_schema,
          stride,
          result_cell_slabs,
          child_result_cell_bitmap);

      switch (combination_op) {
        case QueryConditionCombinationOp::AND: {
          for (size_t c = 0; c < result_cell_bitmap.size(); ++c) {
            result_cell_bitmap[c] *= child_result_cell_bitmap[c];
          }
        } break;
        case QueryConditionCombinationOp::OR: {
          for (size_t c = 0; c < result_cell_bitmap.size(); ++c) {
            result_cell_bitmap[c] |= child_result_cell_bitmap[c];
          }
        } break;
        case QueryConditionCombinationOp::NOT: {
          throw std::runtime_error(
              "Query condition NOT operator is not supported in the TileDB "
              "system.");
        }
        default: {
          throw std::logic_error(
              "apply_tree: invalid combination op, should not get here.");
        }
      }
    }
  }
}

Status QueryCondition::apply(
    const ArraySchema& array_schema,
    std::vector<ResultCellSlab>& result_cell_slabs,
    const uint64_t stride) const {
  if (!tree_) {
    return Status::Ok();
  }

  size_t total_lengths = 0;
  for (const auto& elem : result_cell_slabs) {
    total_lengths += elem.length_;
  }

  std::vector<uint8_t> result_cell_bitmap(total_lengths, 1);
  apply_tree(
      tree_, array_schema, stride, result_cell_slabs, result_cell_bitmap);

  std::vector<ResultCellSlab> ret;
  uint64_t starting_index = 0;
  for (const auto& elem : result_cell_slabs) {
    uint64_t pending_start = elem.start_;
    uint64_t pending_length = 0;
    for (size_t c = 0; c < elem.length_; ++c) {
      if (result_cell_bitmap[starting_index + c]) {
        pending_length += 1;
      } else {
        if (pending_length > 0) {
          ret.emplace_back(elem.tile_, pending_start, pending_length);
        }
        pending_length = 0;
        pending_start = elem.start_ + c + 1;
      }
    }
    starting_index += elem.length_;
    if (pending_length > 0) {
      ret.emplace_back(elem.tile_, pending_start, pending_length);
    }
  }

  result_cell_slabs = std::move(ret);
  return Status::Ok();
}

template <typename T, QueryConditionOp Op>
void QueryCondition::apply_ast_node_dense(
    const tdb_unique_ptr<ASTNode>& node,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    const bool var_size,
    span<uint8_t>& result_buffer) const {
  const std::string& field_name = node->get_field_name();
  const void* condition_value_content =
      node->get_condition_value_view().content();
  const size_t condition_value_size = node->get_condition_value_view().size();

  // Get the nullable buffer.
  const auto tile_tuple = result_tile->tile_tuple(field_name);

  if (var_size) {
    // Get var data buffer and tile offsets buffer.
    const auto& tile = std::get<1>(*tile_tuple);
    const char* buffer = static_cast<char*>(tile.data());
    const uint64_t buffer_size = tile.size();

    const auto& tile_offsets = std::get<0>(*tile_tuple);
    const uint64_t* buffer_offsets =
        static_cast<uint64_t*>(tile_offsets.data()) + src_cell;
    const uint64_t buffer_offsets_el =
        tile_offsets.size() / constants::cell_var_offset_size;

    // Iterate through each cell in this slab.
    for (uint64_t c = 0; c < result_buffer.size(); ++c) {
      // Check the previous cell here, which breaks vectorization but as this
      // is string data requiring a strcmp which cannot be vectorized, this is
      // ok.
      if (result_buffer[c] != 0) {
        const uint64_t buffer_offset = buffer_offsets[start + c * stride];
        const uint64_t next_cell_offset =
            (start + c * stride + 1 < buffer_offsets_el) ?
                buffer_offsets[start + c * stride + 1] :
                buffer_size;
        const uint64_t cell_size = next_cell_offset - buffer_offset;

        // Get the cell value.
        const void* const cell_value = buffer + buffer_offset;

        // Compare the cell value against the value in the value node.
        const bool cmp = BinaryCmp<T, Op>::cmp(
            cell_value,
            cell_size,
            condition_value_content,
            condition_value_size);

        // Set the value.
        result_buffer[c] &= (uint8_t)cmp;
      }
    }
  } else {
    // Get the fixed size data buffers.
    const auto& tile = std::get<0>(*tile_tuple);
    const char* buffer = static_cast<char*>(tile.data());
    const uint64_t cell_size = tile.cell_size();
    uint64_t buffer_offset = (start + src_cell) * cell_size;
    const uint64_t buffer_offset_inc = stride * cell_size;

    // Iterate through each cell in this slab.
    for (uint64_t c = 0; c < result_buffer.size(); ++c) {
      // Get the cell value.
      const void* const cell_value = buffer + buffer_offset;
      buffer_offset += buffer_offset_inc;

      // Compare the cell value against the value in the value node.
      const bool cmp = BinaryCmp<T, Op>::cmp(
          cell_value, cell_size, condition_value_content, condition_value_size);

      // Set the value.
      result_buffer[c] &= (uint8_t)cmp;
    }
  }
}

template <typename T>
Status QueryCondition::apply_ast_node_dense(
    const tdb_unique_ptr<ASTNode>& node,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    const bool var_size,
    span<uint8_t>& result_buffer) const {
  switch (node->get_op()) {
    case QueryConditionOp::LT:
      apply_ast_node_dense<T, QueryConditionOp::LT>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
      break;
    case QueryConditionOp::LE:
      apply_ast_node_dense<T, QueryConditionOp::LE>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
      break;
    case QueryConditionOp::GT:
      apply_ast_node_dense<T, QueryConditionOp::GT>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
      break;
    case QueryConditionOp::GE:
      apply_ast_node_dense<T, QueryConditionOp::GE>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
      break;
    case QueryConditionOp::EQ:
      apply_ast_node_dense<T, QueryConditionOp::EQ>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
      break;
    case QueryConditionOp::NE:
      apply_ast_node_dense<T, QueryConditionOp::NE>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
      break;
    default:
      return Status_QueryConditionError(
          "Cannot perform query comparison; Unknown query "
          "condition operator");
  }

  return Status::Ok();
}

Status QueryCondition::apply_ast_node_dense(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    span<uint8_t>& result_buffer) const {
  const auto attribute = array_schema.attribute(node->get_field_name());
  if (!attribute) {
    return Status_QueryConditionError(
        "Unknown attribute " + node->get_field_name());
  }

  const bool var_size = attribute->var_size();
  const bool nullable = attribute->nullable();

  // Process the validity buffer now.
  if (nullable) {
    const auto tile_tuple = result_tile->tile_tuple(node->get_field_name());
    const auto& tile_validity = std::get<2>(*tile_tuple);
    const auto buffer_validity =
        static_cast<uint8_t*>(tile_validity.data()) + src_cell;
    ;

    // Null values can only be specified for equality operators.
    if (node->get_condition_value_view().content() == nullptr) {
      if (node->get_op() == QueryConditionOp::NE) {
        for (uint64_t c = 0; c < result_buffer.size(); ++c) {
          result_buffer[c] *= buffer_validity[start + c * stride] != 0;
        }
      } else {
        for (uint64_t c = 0; c < result_buffer.size(); ++c) {
          result_buffer[c] *= buffer_validity[start + c * stride] == 0;
        }
      }
      return Status::Ok();
    } else {
      // Turn off bitmap values for null cells.
      for (uint64_t c = 0; c < result_buffer.size(); ++c) {
        result_buffer[c] *= buffer_validity[start + c * stride] != 0;
      }
    }
  }

  switch (attribute->type()) {
    case Datatype::INT8:
      return apply_ast_node_dense<int8_t>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::UINT8:
      return apply_ast_node_dense<uint8_t>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::INT16:
      return apply_ast_node_dense<int16_t>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::UINT16:
      return apply_ast_node_dense<uint16_t>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::INT32:
      return apply_ast_node_dense<int32_t>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::UINT32:
      return apply_ast_node_dense<uint32_t>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::INT64:
      return apply_ast_node_dense<int64_t>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::UINT64:
      return apply_ast_node_dense<uint64_t>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::FLOAT32:
      return apply_ast_node_dense<float>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::FLOAT64:
      return apply_ast_node_dense<double>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::STRING_ASCII:
      return apply_ast_node_dense<char*>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::CHAR:
      if (var_size) {
        return apply_ast_node_dense<char*>(
            node,
            result_tile,
            start,
            src_cell,
            stride,
            var_size,
            result_buffer);
      }
      return apply_ast_node_dense<char>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      return apply_ast_node_dense<int64_t>(
          node, result_tile, start, src_cell, stride, var_size, result_buffer);
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      return Status_QueryConditionError(
          "Cannot perform query comparison; Unsupported query "
          "conditional type on " +
          node->get_field_name());
  }

  return Status::Ok();
}

void QueryCondition::apply_tree_dense(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    span<uint8_t>& result_buffer) const {
  // Case on the type of the tree node.
  if (!node->is_expr()) {
    // In the simple value node case, run the value evaluator function and
    // return the result back.
    Status s = apply_ast_node_dense(
        node,
        array_schema,
        result_tile,
        start,
        src_cell,
        stride,
        result_buffer);
    if (!s.ok()) {
      throw std::runtime_error(
          "apply_tree_dense: apply_ast_node_dense failed.");
    }
  } else {
    // For each child, declare a new result buffer, combine it all into the
    // other one based on the combination op.
    const QueryConditionCombinationOp& combination_op =
        node->get_combination_op();
    if (combination_op == QueryConditionCombinationOp::AND) {
      std::fill(result_buffer.begin(), result_buffer.end(), 1);
    } else if (combination_op == QueryConditionCombinationOp::OR) {
      std::fill(result_buffer.begin(), result_buffer.end(), 0);
    }
    for (const auto& child : node->get_children()) {
      std::vector<uint8_t> child_result_vector(result_buffer.size(), 1);
      span<uint8_t> child_result_buffer(
          child_result_vector.data(), result_buffer.size());
      apply_tree_dense(
          child,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          child_result_buffer);

      switch (combination_op) {
        // Note that a bitwise AND operator is used to combine
        // the accumulator result buffer and child result buffer in the AND
        // combination op case, and bitwise OR in the OR combination op case.
        case QueryConditionCombinationOp::AND: {
          for (uint64_t c = 0; c < result_buffer.size(); ++c) {
            result_buffer[c] &= child_result_buffer[c];
          }
        } break;
        case QueryConditionCombinationOp::OR: {
          for (uint64_t c = 0; c < result_buffer.size(); ++c) {
            result_buffer[c] |= child_result_buffer[c];
          }
        } break;
        case QueryConditionCombinationOp::NOT: {
          throw std::runtime_error(
              "QueryCondition NOT operator not supported in TileDB system.");
        } break;
        default: {
          throw std::logic_error(
              "apply_tree_dense: invalid combination op, should not get here.");
        }
      }
    }
  }
}

Status QueryCondition::apply_dense(
    const ArraySchema& array_schema,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t length,
    const uint64_t src_cell,
    const uint64_t stride,
    uint8_t* result_buffer) {
  // Iterate through the tree.
  span<uint8_t> result_span(result_buffer + start, length);
  apply_tree_dense(
      tree_, array_schema, result_tile, start, src_cell, stride, result_span);
  return Status::Ok();
}

/** Full template specialization for `char*` and `QueryConditionOp::LT`. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::LT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size < rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::LE. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::LE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size <= rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::GT`. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::GT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size > rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::GE`. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::GE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size >= rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::EQ`. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::EQ> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs_size != rhs_size) {
      return false;
    }

    return strncmp(
               static_cast<const char*>(lhs),
               static_cast<const char*>(rhs),
               lhs_size) == 0;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::NE`. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::NE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs_size != rhs_size) {
      return true;
    }

    return strncmp(
               static_cast<const char*>(lhs),
               static_cast<const char*>(rhs),
               lhs_size) != 0;
  }
};

/** Partial template specialization for `QueryConditionOp::LT`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::LT> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) < *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::LE`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::LE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) <= *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::GT`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::GT> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) > *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::GE`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::GE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) >= *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::EQ`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::EQ> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) == *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::NE`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::NE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) != *static_cast<const T*>(rhs);
  }
};

template <typename T, QueryConditionOp Op, typename BitmapType>
void QueryCondition::apply_ast_node_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    ResultTile& result_tile,
    const bool var_size,
    std::vector<BitmapType>& result_bitmap) const {
  const auto tile_tuple = result_tile.tile_tuple(node->get_field_name());
  const void* condition_value_content =
      node->get_condition_value_view().content();
  const size_t condition_value_size = node->get_condition_value_view().size();

  if (var_size) {
    // Get var data buffer and tile offsets buffer.
    const auto& tile = std::get<1>(*tile_tuple);
    const char* buffer = static_cast<char*>(tile.data());
    const uint64_t buffer_size = tile.size();

    const auto& tile_offsets = std::get<0>(*tile_tuple);
    const uint64_t* buffer_offsets =
        static_cast<uint64_t*>(tile_offsets.data());
    const uint64_t buffer_offsets_el =
        tile_offsets.size() / constants::cell_var_offset_size;

    // Iterate through each cell.
    for (uint64_t c = 0; c < buffer_offsets_el; ++c) {
      // Check the previous cell here, which breaks vectorization but as this
      // is string data requiring a strcmp which cannot be vectorized, this is
      // ok.
      if (result_bitmap[c] != 0) {
        const uint64_t buffer_offset = buffer_offsets[c];
        const uint64_t next_cell_offset =
            (c + 1 < buffer_offsets_el) ? buffer_offsets[c + 1] : buffer_size;
        const uint64_t cell_size = next_cell_offset - buffer_offset;

        // Get the cell value.
        const void* const cell_value = buffer + buffer_offset;

        // Compare the cell value against the value in the value node.
        const bool cmp = BinaryCmp<T, Op>::cmp(
            cell_value,
            cell_size,
            condition_value_content,
            condition_value_size);

        // Set the value.
        result_bitmap[c] *= cmp;
      }
    }
  } else {
    // Get the fixed size data buffers.
    const auto& tile = std::get<0>(*tile_tuple);
    const char* buffer = static_cast<char*>(tile.data());
    const uint64_t cell_size = tile.cell_size();
    const uint64_t buffer_el = tile.size() / cell_size;

    // Iterate through each cell without checking the bitmap to enable
    // vectorization.
    for (uint64_t c = 0; c < buffer_el; ++c) {
      // Get the cell value.
      const void* const cell_value = buffer + c * cell_size;

      // Compare the cell value against the value in the value node.
      const bool cmp = BinaryCmp<T, Op>::cmp(
          cell_value, cell_size, condition_value_content, condition_value_size);

      // Set the value.
      result_bitmap[c] *= cmp;
    }
  }
}

template <typename T, typename BitmapType>
Status QueryCondition::apply_ast_node_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    ResultTile& result_tile,
    const bool var_size,
    std::vector<BitmapType>& result_bitmap) const {
  switch (node->get_op()) {
    case QueryConditionOp::LT:
      apply_ast_node_sparse<T, QueryConditionOp::LT>(
          node, result_tile, var_size, result_bitmap);
      break;
    case QueryConditionOp::LE:
      apply_ast_node_sparse<T, QueryConditionOp::LE>(
          node, result_tile, var_size, result_bitmap);
      break;
    case QueryConditionOp::GT:
      apply_ast_node_sparse<T, QueryConditionOp::GT>(
          node, result_tile, var_size, result_bitmap);
      break;
    case QueryConditionOp::GE:
      apply_ast_node_sparse<T, QueryConditionOp::GE>(
          node, result_tile, var_size, result_bitmap);
      break;
    case QueryConditionOp::EQ:
      apply_ast_node_sparse<T, QueryConditionOp::EQ>(
          node, result_tile, var_size, result_bitmap);
      break;
    case QueryConditionOp::NE:
      apply_ast_node_sparse<T, QueryConditionOp::NE>(
          node, result_tile, var_size, result_bitmap);
      break;
    default:
      return Status_QueryConditionError(
          "Cannot perform query comparison; Unknown query "
          "condition operator");
  }

  return Status::Ok();
}

template <typename BitmapType>
Status QueryCondition::apply_ast_node_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    std::vector<BitmapType>& result_bitmap) const {
  const auto attribute = array_schema.attribute(node->get_field_name());
  if (!attribute) {
    return Status_QueryConditionError(
        "Unknown attribute " + node->get_field_name());
  }

  const bool var_size = attribute->var_size();
  const bool nullable = attribute->nullable();

  // Process the validity buffer now.

  if (nullable) {
    const auto tile_tuple = result_tile.tile_tuple(node->get_field_name());
    const auto& tile_validity = std::get<2>(*tile_tuple);
    const auto buffer_validity = static_cast<uint8_t*>(tile_validity.data());

    // Null values can only be specified for equality operators.
    if (node->get_condition_value_view().content() == nullptr) {
      if (node->get_op() == QueryConditionOp::NE) {
        for (uint64_t c = 0; c < result_tile.cell_num(); c++) {
          result_bitmap[c] *= buffer_validity[c] != 0;
        }
      } else {
        for (uint64_t c = 0; c < result_tile.cell_num(); c++) {
          result_bitmap[c] *= buffer_validity[c] == 0;
        }
      }
      return Status::Ok();
    } else {
      // Turn off bitmap values for null cells.
      for (uint64_t c = 0; c < result_tile.cell_num(); c++) {
        result_bitmap[c] *= buffer_validity[c] != 0;
      }
    }
  }

  switch (attribute->type()) {
    case Datatype::INT8:
      return apply_ast_node_sparse<int8_t, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::UINT8:
      return apply_ast_node_sparse<uint8_t, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::INT16:
      return apply_ast_node_sparse<int16_t, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::UINT16:
      return apply_ast_node_sparse<uint16_t, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::INT32:
      return apply_ast_node_sparse<int32_t, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::UINT32:
      return apply_ast_node_sparse<uint32_t, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::INT64:
      return apply_ast_node_sparse<int64_t, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::UINT64:
      return apply_ast_node_sparse<uint64_t, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::FLOAT32:
      return apply_ast_node_sparse<float, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::FLOAT64:
      return apply_ast_node_sparse<double, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::STRING_ASCII:
      return apply_ast_node_sparse<char*, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::CHAR:
      if (var_size) {
        return apply_ast_node_sparse<char*, BitmapType>(
            node, result_tile, var_size, result_bitmap);
      }
      return apply_ast_node_sparse<char, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      return apply_ast_node_sparse<int64_t, BitmapType>(
          node, result_tile, var_size, result_bitmap);
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      return Status_QueryConditionError(
          "Cannot perform query comparison; Unsupported query "
          "conditional type on " +
          node->get_field_name());
  }

  return Status::Ok();
}

template <typename BitmapType>
void QueryCondition::apply_tree_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    std::vector<BitmapType>& result_bitmap) const {
  // Case on the type of the tree node.
  if (!node->is_expr()) {
    // In the simple value node case, run the value evaluator function and
    // return the result back.
    Status s = apply_ast_node_sparse<BitmapType>(
        node, array_schema, result_tile, result_bitmap);
    if (!s.ok()) {
      throw std::runtime_error(
          "apply_tree_sparse: apply_ast_node_sparse failed.");
    }
  } else {
    // For each child, declare a new result buffer, combine it all into the
    // other one based on the combination op.
    const QueryConditionCombinationOp& combination_op =
        node->get_combination_op();
    if (combination_op == QueryConditionCombinationOp::AND) {
      std::fill(result_bitmap.begin(), result_bitmap.end(), 1);
    } else if (combination_op == QueryConditionCombinationOp::OR) {
      std::fill(result_bitmap.begin(), result_bitmap.end(), 0);
    }
    for (const auto& child : node->get_children()) {
      std::vector<BitmapType> child_result_bitmap(result_bitmap.size(), true);
      apply_tree_sparse(child, array_schema, result_tile, child_result_bitmap);
      // Note that the multiplication operator (which functions
      // effectively as a bitwise AND) is used to combine the accumulator result
      // buffer and child result buffer in the AND combination op case, and
      // bitwise OR in the OR combination op case.
      switch (combination_op) {
        case QueryConditionCombinationOp::AND: {
          for (uint64_t c = 0; c < result_bitmap.size(); ++c) {
            result_bitmap[c] *= child_result_bitmap[c];
          }
        } break;
        case QueryConditionCombinationOp::OR: {
          for (uint64_t c = 0; c < result_bitmap.size(); ++c) {
            result_bitmap[c] |= child_result_bitmap[c];
          }
        } break;
        case QueryConditionCombinationOp::NOT: {
          throw std::runtime_error(
              "Query condition NOT operator is not supported in the TileDB "
              "system.");
        }
        default: {
          throw std::logic_error(
              "apply_tree_sparse: invalid combination op, should not get "
              "here.");
        }
      }
    }
  }
}

template <typename BitmapType>
Status QueryCondition::apply_sparse(
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    std::vector<BitmapType>& result_bitmap,
    uint64_t* cell_count) {
  // Iterate through the tree.
  apply_tree_sparse<BitmapType>(
      tree_, array_schema, result_tile, result_bitmap);
  *cell_count = std::accumulate(result_bitmap.begin(), result_bitmap.end(), 0);

  return Status::Ok();
}

tdb_unique_ptr<ASTNode>& QueryCondition::ast() {
  return tree_;
}

void QueryCondition::set_clauses(std::vector<Clause>&& clauses) {
  // This is because AND nodes are the only structure supported currently within
  // serialization.
  std::vector<Clause> temp_clauses = std::move(clauses);
  if (temp_clauses.size() == 0) {
    tree_ = nullptr;
  } else if (temp_clauses.size() == 1) {
    tree_ = tdb_unique_ptr<ASTNode>(tdb_new(
        ASTNodeVal,
        temp_clauses[0].field_name_,
        temp_clauses[0].condition_value_data_.data(),
        temp_clauses[0].condition_value_data_.size(),
        temp_clauses[0].op_));
  } else {
    std::vector<tdb_unique_ptr<ASTNode>> nodes;
    for (const auto& clause : temp_clauses) {
      nodes.emplace_back(tdb_unique_ptr<ASTNode>(tdb_new(
          ASTNodeVal,
          clause.field_name_,
          clause.condition_value_data_.data(),
          clause.condition_value_data_.size(),
          clause.op_)));
    }

    tree_ = tdb_unique_ptr<ASTNode>(tdb_new(
        ASTNodeExpr, std::move(nodes), QueryConditionCombinationOp::AND));
  }
}

void QueryCondition::set_combination_ops(
    std::vector<QueryConditionCombinationOp>&& combination_ops) {
  auto temp_combination_ops = std::move(combination_ops);
  (void)temp_combination_ops;
}

static void ast_get_clauses(
    std::vector<QueryCondition::Clause>& clauses_vector,
    const tdb_unique_ptr<ASTNode>& node) {
  if (!node)
    return;
  if (!node->is_expr()) {
    QueryCondition::Clause c(
        node->get_field_name(),
        node->get_condition_value_view().content(),
        node->get_condition_value_view().size(),
        node->get_op());
    clauses_vector.emplace_back(c);
  } else {
    for (const auto& child : node->get_children()) {
      ast_get_clauses(clauses_vector, child);
    }
  }
}

std::vector<QueryCondition::Clause> QueryCondition::clauses() const {
  if (tree_ && !tree_->is_or_supported()) {
    throw std::runtime_error(
        "From QueryCondition::clauses(): OR serialization not supported.");
  }

  if (clauses_.empty() && tree_) {
    ast_get_clauses(clauses_, tree_);
  }
  return clauses_;
}

std::vector<QueryConditionCombinationOp> QueryCondition::combination_ops()
    const {
  if (tree_ && !tree_->is_or_supported()) {
    throw std::runtime_error(
        "From QueryCondition::combination_ops(): OR serialization not "
        "supported.");
  }
  if (combination_ops_.empty() && tree_) {
    if (clauses_.empty())
      ast_get_clauses(clauses_, tree_);
    for (size_t i = 0; i < clauses_.size() - 1; ++i) {
      combination_ops_.emplace_back(QueryConditionCombinationOp::AND);
    }
  }

  return combination_ops_;
}

// Explicit template instantiations.
template Status QueryCondition::apply_sparse<uint8_t>(
    const ArraySchema& array_schema,
    ResultTile&,
    std::vector<uint8_t>&,
    uint64_t*);
template Status QueryCondition::apply_sparse<uint64_t>(
    const ArraySchema& array_schema,
    ResultTile&,
    std::vector<uint64_t>&,
    uint64_t*);
}  // namespace sm
}  // namespace tiledb
