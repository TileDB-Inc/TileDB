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
    const QueryConditionOp op) {
  if (tree_) {
    return Status_QueryConditionError("Cannot reinitialize query condition");
  }

  // AST Construction.
  tree_ = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal, field_name, condition_value, condition_value_size, op));

  return Status::Ok();
}

static Status value_node_check(
    const ArraySchema& array_schema, const ASTNodeVal& node) {
  const std::string field_name = node.field_name_;
  const uint64_t condition_value_size = node.condition_value_data_.size();
  const auto attribute = array_schema.attribute(field_name);
  // We should check that the field_name represents an attribute in the array
  // schema corresponding to this QueryCondition object.
  if (!attribute) {
    return Status_QueryConditionError(
        "Value node field name is not an attribute " + field_name);
  }

  // We should ensure that we only run null equality or inequality checks.
  if (node.condition_value_view_.content() == nullptr) {
    if (node.op_ != QueryConditionOp::EQ && node.op_ != QueryConditionOp::NE) {
      return Status_QueryConditionError(
          "Null value can only be used with equality operators");
    }

    // We should ensure that an attribute that is marked as nullable
    // corresponds to a type that is nullable.
    if ((!attribute->nullable()) &&
        (attribute->type() != Datatype::STRING_ASCII &&
         attribute->type() != Datatype::CHAR)) {
      return Status_QueryConditionError(
          "Null value can only be used with nullable attributes");
    }
  }

  // We should ensure that non-empty attributes are only var-sized for
  // ASCII strings.
  if (attribute->var_size() && attribute->type() != Datatype::STRING_ASCII &&
      attribute->type() != Datatype::CHAR &&
      node.condition_value_view_.content() != nullptr) {
    return Status_QueryConditionError(
        "Value node non-empty attribute may only be var-sized for ASCII "
        "strings: " +
        field_name);
  }

  // We should ensure that for non string fixed size attributes, we store only
  // one value per cell.
  if (attribute->cell_val_num() != 1 &&
      attribute->type() != Datatype::STRING_ASCII &&
      attribute->type() != Datatype::CHAR && (!attribute->var_size())) {
    return Status_QueryConditionError(
        "Value node attribute must have one value per cell for non-string "
        "fixed size attributes: " +
        field_name);
  }

  // We should ensure that the condition value size matches the attribute's
  // value size.
  if (attribute->cell_size() != constants::var_size &&
      attribute->cell_size() != condition_value_size &&
      !(attribute->nullable() &&
        node.condition_value_view_.content() == nullptr) &&
      attribute->type() != Datatype::STRING_ASCII &&
      attribute->type() != Datatype::CHAR && (!attribute->var_size())) {
    return Status_QueryConditionError(
        "Value node condition value size mismatch: " +
        std::to_string(attribute->cell_size()) +
        " != " + std::to_string(condition_value_size));
  }

  // We should ensure that the attribute type is valid.
  switch (attribute->type()) {
    case Datatype::ANY:
      return Status_QueryConditionError(
          "Value node attribute type may not be of type 'ANY': " + field_name);
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
      return Status_QueryConditionError(
          "Value node attribute type may not be a UTF/UCS string: " +
          field_name);
    default:
      break;
  }
  return Status::Ok();
}

static Status ast_check(
    const ArraySchema& array_schema, const tdb_unique_ptr<ASTNode>& node) {
  // Ensure that there are no null nodes in the tree.
  if (!node)
    return Status_QueryConditionError(
        "AST node in recursive tree structure should not be null.");
  // Case on the tag of the tree node.
  if (node->get_tag() == ASTNodeTag::VAL) {
    // If the node is a value node, then we run the check on value nodes.
    auto node_ptr = dynamic_cast<ASTNodeVal*>(node.get());
    if (node_ptr)
      return value_node_check(array_schema, *node_ptr);
    return Status_QueryConditionError("AST value node is null.");
  } else if (node->get_tag() == ASTNodeTag::EXPR) {
    // If the node is a compound expression node, we ensure there are at least
    // two children in the node and then run a check on each child nodes.
    auto node_ptr = dynamic_cast<ASTNodeExpr*>(node.get());
    if (node_ptr->nodes_.size() < 2) {
      return Status_QueryConditionError(
          "Non value AST node does not have at least 2 children.");
    }
    for (const auto& child : node_ptr->nodes_) {
      RETURN_NOT_OK(ast_check(array_schema, child));
    }
  } else {
    return Status_QueryConditionError("AST node has invalid tag.");
  }
  return Status::Ok();
}

Status QueryCondition::check(const ArraySchema& array_schema) const {
  if (!tree_)
    return Status::Ok();
  RETURN_NOT_OK(ast_check(array_schema, tree_));
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
  combined_cond->tree_ = ast_combine(this->tree_, rhs.tree_, combination_op);
  return Status::Ok();
}

bool QueryCondition::empty() const {
  return tree_ == nullptr;
}

std::unordered_set<std::string>& QueryCondition::field_names() const {
  if (field_names_.empty()) {
    ast_get_field_names(field_names_, tree_);
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

/** Used to create a new result slab in QueryCondition::apply_ast_node. */
uint64_t create_new_result_slab(
    uint64_t start,
    uint64_t pending_start,
    uint64_t stride,
    uint64_t current,
    ResultTile* const result_tile,
    std::vector<ResultCellSlab>& result_cell_slabs) {
  // Create a result cell slab if there are pending cells.
  if (pending_start != start + current) {
    const uint64_t rcs_start = start + ((pending_start - start) * stride);
    const uint64_t rcs_length = current - (pending_start - start);
    result_cell_slabs.emplace_back(result_tile, rcs_start, rcs_length);
  }

  // Return the new start of the pending result cell slab.
  return start + current + 1;
}

template <typename T, QueryConditionOp Op>
std::vector<ResultCellSlab> QueryCondition::apply_ast_node(
    const ASTNodeVal& node,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs) const {
  std::vector<ResultCellSlab> ret;
  const std::string& field_name = node.field_name_;

  for (const auto& rcs : result_cell_slabs) {
    ResultTile* const result_tile = rcs.tile_;
    const uint64_t start = rcs.start_;
    const uint64_t length = rcs.length_;

    // Handle an empty range.
    if (result_tile == nullptr && !nullable) {
      const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
          fill_value.data(),
          fill_value.size(),
          node.condition_value_view_.content(),
          node.condition_value_data_.size());
      if (cmp) {
        ret.emplace_back(result_tile, start, length);
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
      uint64_t pending_start = start;
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
              node.condition_value_view_.content(),
              node.condition_value_data_.size());
          if (!cmp) {
            pending_start = create_new_result_slab(
                start, pending_start, stride, c, result_tile, ret);
          }

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
              node.condition_value_view_.content(),
              node.condition_value_data_.size());
          if (!cmp) {
            pending_start = create_new_result_slab(
                start, pending_start, stride, c, result_tile, ret);
          }

          ++c;
        }
      }

      // Create the final result cell slab if there are pending cells.
      create_new_result_slab(start, pending_start, stride, c, result_tile, ret);
    }
  }

  return ret;
}

template <typename T>
tuple<Status, optional<std::vector<ResultCellSlab>>>
QueryCondition::apply_ast_node(
    const ASTNodeVal& node,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs) const {
  std::vector<ResultCellSlab> ret;
  switch (node.op_) {
    case QueryConditionOp::LT:
      ret = apply_ast_node<T, QueryConditionOp::LT>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    case QueryConditionOp::LE:
      ret = apply_ast_node<T, QueryConditionOp::LE>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    case QueryConditionOp::GT:
      ret = apply_ast_node<T, QueryConditionOp::GT>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    case QueryConditionOp::GE:
      ret = apply_ast_node<T, QueryConditionOp::GE>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    case QueryConditionOp::EQ:
      ret = apply_ast_node<T, QueryConditionOp::EQ>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    case QueryConditionOp::NE:
      ret = apply_ast_node<T, QueryConditionOp::NE>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    default:
      return {Status_QueryConditionError(
                  "Cannot perform query comparison; Unknown query "
                  "condition operator"),
              nullopt};
  }

  return {Status::Ok(), std::move(ret)};
}

tuple<Status, optional<std::vector<ResultCellSlab>>>
QueryCondition::apply_ast_node(
    const ASTNodeVal& node,
    const ArraySchema& array_schema,
    const uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs) const {
  const auto attribute = array_schema.attribute(node.field_name_);
  if (!attribute) {
    return {Status_QueryConditionError("Unknown attribute " + node.field_name_),
            nullopt};
  }

  const ByteVecValue fill_value = attribute->fill_value();
  const bool var_size = attribute->var_size();
  const bool nullable = attribute->nullable();
  switch (attribute->type()) {
    case Datatype::INT8:
      return apply_ast_node<int8_t>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::UINT8:
      return apply_ast_node<uint8_t>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::INT16:
      return apply_ast_node<int16_t>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::UINT16:
      return apply_ast_node<uint16_t>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::INT32:
      return apply_ast_node<int32_t>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::UINT32:
      return apply_ast_node<uint32_t>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::INT64:
      return apply_ast_node<int64_t>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::UINT64:
      return apply_ast_node<uint64_t>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::FLOAT32:
      return apply_ast_node<float>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::FLOAT64:
      return apply_ast_node<double>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::STRING_ASCII:
      return apply_ast_node<char*>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::CHAR:
      if (var_size) {
        return apply_ast_node<char*>(
            node, stride, var_size, nullable, fill_value, result_cell_slabs);
      }
      return apply_ast_node<char>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
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
      return apply_ast_node<int64_t>(
          node, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      return {Status_QueryConditionError(
                  "Cannot perform query comparison; Unsupported query "
                  "conditional type on " +
                  node.field_name_),
              nullopt};
  }

  return {Status::Ok(), nullopt};
}

/**
 * @brief This function takes two ResultCellSlab vectors and merges them so they
 * contain the union of the ranges represented in its two arguments.
 *
 * Since the ResultCellSlab object is essentially a range object that contains
 * a start index and a length, this function will iterate through all the slabs
 * in both vectors and determine if there are any overlapping ranges that we
 * should consider and merge together into one range in the return accumulator,
 * ret.
 *
 * This function assumes that the ranges in both arr1 and arr2 are sorted by
 * their start index.
 *
 * @param arr1 Array of ResultCellSlabs to merge.
 * @param arr2 Array of ResultCellSlabs to merge.
 * @return The vector that merges these ResultCellSlab vectors together.
 */
static std::vector<ResultCellSlab> result_slabs_merge_union(
    const std::vector<ResultCellSlab>& arr1,
    const std::vector<ResultCellSlab>& arr2) {
  size_t iter1 = 0;
  size_t iter2 = 0;
  std::vector<ResultCellSlab> ret;
  while (iter1 < arr1.size() && iter2 < arr2.size()) {
    ResultCellSlab less;
    if (arr1[iter1].start_ < arr2[iter2].start_) {
      less = arr1[iter1];
      iter1 += 1;
    } else {
      less = arr2[iter2];
      iter2 += 1;
    }

    if (ret.size() > 0 &&
        ret.back().start_ + ret.back().length_ >= less.start_) {
      ret.back().length_ = less.start_ + less.length_ - ret.back().start_;
    } else {
      ret.push_back(less);
    }
  }

  while (iter1 < arr1.size()) {
    if (ret.size() > 0 &&
        ret.back().start_ + ret.back().length_ >= arr1[iter1].start_) {
      ret.back().length_ =
          arr1[iter1].start_ + arr1[iter1].length_ - ret.back().start_;
    } else {
      ret.push_back(arr1[iter1]);
    }
    iter1 += 1;
  }

  while (iter2 < arr2.size()) {
    if (ret.size() > 0 &&
        ret.back().start_ + ret.back().length_ >= arr2[iter2].start_) {
      ret.back().length_ =
          arr2[iter2].start_ + arr2[iter2].length_ - ret.back().start_;
    } else {
      ret.push_back(arr2[iter2]);
    }
    iter2 += 1;
  }
  return ret;
}

/**
 * @brief This function takes two ResultCellSlab vectors and merges them so they
 * contain the intersection of the ranges represented in its two arguments.
 *
 * Since the ResultCellSlab object is essentially a range object that contains
 * a start index and a length, this function will iterate through all the slabs
 * in both vectors and determine if there are any overlapping ranges that we
 * should add to the result accumulator vector, ret.
 *
 * This function assumes that the ranges in both arr1 and arr2 are sorted by
 * their start index.
 *
 * @param arr1 Array of ResultCellSlabs to merge.
 * @param arr2 Array of ResultCellSlabs to merge.
 * @return The vector that merges these ResultCellSlab vectors together.
 */
static std::vector<ResultCellSlab> result_slabs_merge_intersection(
    const std::vector<ResultCellSlab>& arr1,
    const std::vector<ResultCellSlab>& arr2) {
  size_t iter1 = 0;
  size_t iter2 = 0;
  std::vector<ResultCellSlab> ret;
  while (iter1 < arr1.size() && iter2 < arr2.size()) {
    // interval is [start, end)
    size_t start = std::max(arr1[iter1].start_, arr2[iter2].start_);
    size_t end = std::min(
        arr1[iter1].start_ + arr1[iter1].length_,
        arr2[iter2].start_ + arr2[iter2].length_);

    if (start < end) {
      ResultCellSlab c(arr1[iter1].tile_, start, end - start);
      ret.push_back(c);
    }

    if (arr1[iter1].start_ + arr1[iter1].length_ <
        arr2[iter2].start_ + arr2[iter2].length_) {
      iter1 += 1;
    } else {
      iter2 += 1;
    }
  }

  return ret;
}

std::vector<ResultCellSlab> QueryCondition::apply_tree(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs) const {
  // Case on the tag of the tree node.
  if (node->get_tag() == ASTNodeTag::VAL) {
    // In the simple value node case, we run the value evaluator function and
    // return the result back.
    auto node_ptr = dynamic_cast<ASTNodeVal*>(node.get());
    auto&& [st, tmp_result_cell_slabs] =
        apply_ast_node(*node_ptr, array_schema, stride, result_cell_slabs);
    if (!st.ok()) {
      throw std::runtime_error("From apply_tree: apply_ast_node failed.");
    }
    return tmp_result_cell_slabs.value();
  } else if (node->get_tag() == ASTNodeTag::EXPR) {
    // In the recursive case, for all the children of the node, we obtain
    // the result accumulator vectors and combine them using the
    // result_slabs_merge_union (for OR combination op) and
    // result_slabs_merge_intersection (for AND combination op) functions above.
    auto expr_ptr = dynamic_cast<ASTNodeExpr*>(node.get());
    std::vector<std::vector<ResultCellSlab>> child_result_cell_slabs;
    for (const auto& child : expr_ptr->nodes_) {
      auto child_vec =
          apply_tree(child, array_schema, stride, result_cell_slabs);
      child_result_cell_slabs.push_back(child_vec);
    }

    switch (expr_ptr->combination_op_) {
      case QueryConditionCombinationOp::AND: {
        return std::accumulate(
            child_result_cell_slabs.begin(),
            child_result_cell_slabs.end(),
            result_cell_slabs,
            result_slabs_merge_intersection);
      } break;
      case QueryConditionCombinationOp::OR: {
        return std::accumulate(
            child_result_cell_slabs.begin(),
            child_result_cell_slabs.end(),
            std::vector<ResultCellSlab>(),
            result_slabs_merge_union);
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
  throw std::logic_error("apply_tree: invalid node tag, should not get here.");
}

Status QueryCondition::apply(
    const ArraySchema& array_schema,
    std::vector<ResultCellSlab>& result_cell_slabs,
    const uint64_t stride) const {
  if (!tree_) {
    return Status::Ok();
  }

  auto tmp_result_cell_slabs =
      apply_tree(tree_, array_schema, stride, result_cell_slabs);
  result_cell_slabs = std::move(tmp_result_cell_slabs);

  return Status::Ok();
}

template <typename T, QueryConditionOp Op>
void QueryCondition::apply_ast_node_dense(
    const ASTNodeVal& node,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    const bool var_size,
    std::vector<uint8_t>& result_buffer) const {
  const std::string& field_name = node.field_name_;

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
            node.condition_value_view_.content(),
            node.condition_value_data_.size());

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
          cell_value,
          cell_size,
          node.condition_value_view_.content(),
          node.condition_value_data_.size());

      // Set the value.
      result_buffer[c] &= (uint8_t)cmp;
    }
  }
}

template <typename T>
Status QueryCondition::apply_ast_node_dense(
    const ASTNodeVal& node,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    const bool var_size,
    std::vector<uint8_t>& result_buffer) const {
  switch (node.op_) {
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
    const ASTNodeVal& node,
    const ArraySchema& array_schema,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    std::vector<uint8_t>& result_buffer) const {
  const auto attribute = array_schema.attribute(node.field_name_);
  if (!attribute) {
    return Status_QueryConditionError("Unknown attribute " + node.field_name_);
  }

  const bool var_size = attribute->var_size();
  const bool nullable = attribute->nullable();

  // Process the validity buffer now.
  if (nullable) {
    const auto tile_tuple = result_tile->tile_tuple(node.field_name_);
    const auto& tile_validity = std::get<2>(*tile_tuple);
    const auto buffer_validity =
        static_cast<uint8_t*>(tile_validity.data()) + src_cell;
    ;

    // Null values can only be specified for equality operators.
    if (node.condition_value_view_.content() == nullptr) {
      if (node.op_ == QueryConditionOp::NE) {
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
          node.field_name_);
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
    std::vector<uint8_t>& result_buffer) const {
  // Case on the tag of the tree node.
  if (node->get_tag() == ASTNodeTag::VAL) {
    // In the simple value node case, we run the value evaluator function and
    // return the result back.
    const ASTNodeVal* node_ptr = dynamic_cast<ASTNodeVal*>(node.get());
    Status s = apply_ast_node_dense(
        *node_ptr,
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
  } else if (node->get_tag() == ASTNodeTag::EXPR) {
    // For each child, declare a new result buffer, combine it all into the
    // other one based on the combination op.
    auto expr_ptr = dynamic_cast<ASTNodeExpr*>(node.get());
    if (expr_ptr->combination_op_ == QueryConditionCombinationOp::AND) {
      std::fill(result_buffer.begin(), result_buffer.end(), 1);
    } else if (expr_ptr->combination_op_ == QueryConditionCombinationOp::OR) {
      std::fill(result_buffer.begin(), result_buffer.end(), 0);
    }
    for (const auto& child : expr_ptr->nodes_) {
      std::vector<uint8_t> child_result_buffer(result_buffer.size(), 1);
      apply_tree_dense(
          child,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          child_result_buffer);

      switch (expr_ptr->combination_op_) {
        // Note that we use a bitwise AND operator to combine
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
  } else {
    throw std::logic_error(
        "apply_tree_dense: invalid node tag, should not get here.");
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
  std::vector<uint8_t> result_vector(length, 1);
  apply_tree_dense(
      tree_, array_schema, result_tile, start, src_cell, stride, result_vector);

  // Copy into buffer.
  std::copy(result_vector.begin(), result_vector.end(), result_buffer + start);
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
    const ASTNodeVal& node,
    ResultTile& result_tile,
    const bool var_size,
    std::vector<BitmapType>& result_bitmap) const {
  const auto tile_tuple = result_tile.tile_tuple(node.field_name_);

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
            node.condition_value_view_.content(),
            node.condition_value_data_.size());

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
          cell_value,
          cell_size,
          node.condition_value_view_.content(),
          node.condition_value_data_.size());

      // Set the value.
      result_bitmap[c] *= cmp;
    }
  }
}

template <typename T, typename BitmapType>
Status QueryCondition::apply_ast_node_sparse(
    const ASTNodeVal& node,
    ResultTile& result_tile,
    const bool var_size,
    std::vector<BitmapType>& result_bitmap) const {
  switch (node.op_) {
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
    const ASTNodeVal& node,
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    std::vector<BitmapType>& result_bitmap) const {
  const auto attribute = array_schema.attribute(node.field_name_);
  if (!attribute) {
    return Status_QueryConditionError("Unknown attribute " + node.field_name_);
  }

  const bool var_size = attribute->var_size();
  const bool nullable = attribute->nullable();

  // Process the validity buffer now.

  if (nullable) {
    const auto tile_tuple = result_tile.tile_tuple(node.field_name_);
    const auto& tile_validity = std::get<2>(*tile_tuple);
    const auto buffer_validity = static_cast<uint8_t*>(tile_validity.data());

    // Null values can only be specified for equality operators.
    if (node.condition_value_view_.content() == nullptr) {
      if (node.op_ == QueryConditionOp::NE) {
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
          node.field_name_);
  }

  return Status::Ok();
}

template <typename BitmapType>
void QueryCondition::apply_tree_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    std::vector<BitmapType>& result_bitmap) const {
  // Case on the tag of the tree node.
  if (node->get_tag() == ASTNodeTag::VAL) {
    // In the simple value node case, we run the value evaluator function and
    // return the result back.
    auto node_ptr = dynamic_cast<ASTNodeVal*>(node.get());
    Status s = apply_ast_node_sparse<BitmapType>(
        *node_ptr, array_schema, result_tile, result_bitmap);
    if (!s.ok()) {
      throw std::runtime_error(
          "apply_tree_sparse: apply_ast_node_sparse failed.");
    }
  } else if (node->get_tag() == ASTNodeTag::EXPR) {
    // For each child, declare a new result buffer, combine it all into the
    // other one based on the combination op.
    auto expr_ptr = dynamic_cast<ASTNodeExpr*>(node.get());
    if (expr_ptr->combination_op_ == QueryConditionCombinationOp::AND) {
      std::fill(result_bitmap.begin(), result_bitmap.end(), 1);
    } else if (expr_ptr->combination_op_ == QueryConditionCombinationOp::OR) {
      std::fill(result_bitmap.begin(), result_bitmap.end(), 0);
    }
    for (const auto& child : expr_ptr->nodes_) {
      std::vector<BitmapType> child_result_bitmap(result_bitmap.size(), true);
      apply_tree_sparse(child, array_schema, result_tile, child_result_bitmap);
      // Note that we use the multiplication operator (which functions
      // effectively as a bitwise AND) to combine the accumulator result buffer
      // and child result buffer in the AND combination op case, and bitwise OR
      // in the OR combination op case.
      switch (expr_ptr->combination_op_) {
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
  } else {
    throw std::logic_error(
        "apply_tree_sparse: invalid node tag, should not get here.");
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
  // This is because we only support AND nodes currently within serialization.
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
  if (node->get_tag() == ASTNodeTag::VAL) {
    auto val_ptr = dynamic_cast<ASTNodeVal*>(node.get());

    QueryCondition::Clause c(
        val_ptr->field_name_,
        val_ptr->condition_value_data_.data(),
        val_ptr->condition_value_data_.size(),
        val_ptr->op_);
    clauses_vector.emplace_back(c);
  } else if (node->get_tag() == ASTNodeTag::EXPR) {
    auto expr_ptr = dynamic_cast<ASTNodeExpr*>(node.get());
    for (const auto& child : expr_ptr->nodes_) {
      ast_get_clauses(clauses_vector, child);
    }
  }
}

std::vector<QueryCondition::Clause> QueryCondition::clauses() const {
  if (!ast_is_previously_supported(tree_)) {
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
  if (!ast_is_previously_supported(tree_)) {
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
