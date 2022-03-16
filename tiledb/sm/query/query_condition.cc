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

#include <iostream>
#include <map>
#include <mutex>
#include <numeric>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

QueryCondition::QueryCondition() {
}

QueryCondition::QueryCondition(const QueryCondition& rhs)
    : clauses_(rhs.clauses_)
    , combination_ops_(rhs.combination_ops_)
    , tree_(rhs.tree_) {
}

QueryCondition::QueryCondition(QueryCondition&& rhs)
    : clauses_(std::move(rhs.clauses_))
    , combination_ops_(std::move(rhs.combination_ops_))
    , tree_(rhs.tree_) {
}

QueryCondition::~QueryCondition() {
}

QueryCondition& QueryCondition::operator=(const QueryCondition& rhs) {
  clauses_ = rhs.clauses_;
  combination_ops_ = rhs.combination_ops_;
  tree_ = rhs.tree_;

  return *this;
}

QueryCondition& QueryCondition::operator=(QueryCondition&& rhs) {
  clauses_ = std::move(rhs.clauses_);
  combination_ops_ = std::move(rhs.combination_ops_);
  tree_ = std::move(rhs.tree_);

  return *this;
}

Status QueryCondition::init(
    std::string&& field_name,
    const void* const condition_value,
    const uint64_t condition_value_size,
    const QueryConditionOp op) {
  if (!clauses_.empty()) {
    return Status_QueryConditionError("Cannot reinitialize query condition");
  }

  std::string field_name_copy = field_name;
  std::string field_name_ref = std::move(field_name);

  clauses_.emplace_back(
      std::move(field_name_ref), condition_value, condition_value_size, op);

  // AST Construction
  tree_ = make_shared<ASTNodeVal>(
      HERE(), field_name_copy, condition_value, condition_value_size, op);

  return Status::Ok();
}

Status QueryCondition::check(const ArraySchema& array_schema) const {
  for (const auto& clause : clauses_) {
    const std::string field_name = clause.field_name_;
    const uint64_t condition_value_size = clause.condition_value_data_.size();

    const auto attribute = array_schema.attribute(field_name);
    if (!attribute) {
      return Status_QueryConditionError(
          "Clause field name is not an attribute " + field_name);
    }

    if (clause.condition_value_ == nullptr) {
      if (clause.op_ != QueryConditionOp::EQ &&
          clause.op_ != QueryConditionOp::NE) {
        return Status_QueryConditionError(
            "Null value can only be used with equality operators");
      }

      if ((!attribute->nullable()) &&
          (attribute->type() != Datatype::STRING_ASCII &&
           attribute->type() != Datatype::CHAR)) {
        return Status_QueryConditionError(
            "Null value can only be used with nullable attributes");
      }
    }

    if (attribute->var_size() && attribute->type() != Datatype::STRING_ASCII &&
        attribute->type() != Datatype::CHAR &&
        clause.condition_value_ != nullptr) {
      return Status_QueryConditionError(
          "Clause non-empty attribute may only be var-sized for ASCII "
          "strings: " +
          field_name);
    }

    if (attribute->cell_val_num() != 1 &&
        attribute->type() != Datatype::STRING_ASCII &&
        attribute->type() != Datatype::CHAR && (!attribute->var_size())) {
      return Status_QueryConditionError(
          "Clause attribute must have one value per cell for non-string fixed "
          "size "
          "attributes: " +
          field_name);
    }

    if (attribute->cell_size() != constants::var_size &&
        attribute->cell_size() != condition_value_size &&
        !(attribute->nullable() && clause.condition_value_ == nullptr) &&
        attribute->type() != Datatype::STRING_ASCII &&
        attribute->type() != Datatype::CHAR && (!attribute->var_size())) {
      return Status_QueryConditionError(
          "Clause condition value size mismatch: " +
          std::to_string(attribute->cell_size()) +
          " != " + std::to_string(condition_value_size));
    }

    switch (attribute->type()) {
      case Datatype::ANY:
        return Status_QueryConditionError(
            "Clause attribute type may not be of type 'ANY': " + field_name);
      case Datatype::STRING_UTF8:
      case Datatype::STRING_UTF16:
      case Datatype::STRING_UTF32:
      case Datatype::STRING_UCS2:
      case Datatype::STRING_UCS4:
        return Status_QueryConditionError(
            "Clause attribute type may not be a UTF/UCS string: " + field_name);
      default:
        break;
    }
  }

  return Status::Ok();
}

Status QueryCondition::combine(
    const QueryCondition& rhs,
    const QueryConditionCombinationOp combination_op,
    QueryCondition* const combined_cond) const {
  assert(combination_op == QueryConditionCombinationOp::AND);
  if (combination_op != QueryConditionCombinationOp::AND) {
    return Status_QueryConditionError(
        "Cannot combine query conditions; Only the 'AND' "
        "combination op is supported");
  }

  combined_cond->clauses_ = clauses_;
  combined_cond->clauses_.insert(
      combined_cond->clauses_.end(), rhs.clauses_.begin(), rhs.clauses_.end());

  combined_cond->combination_ops_ = combination_ops_;
  combined_cond->combination_ops_.emplace_back(combination_op);
  combined_cond->combination_ops_.insert(
      combined_cond->combination_ops_.end(),
      rhs.combination_ops_.begin(),
      rhs.combination_ops_.end());

  combined_cond->field_names_.clear();

  // AST Construction
  std::vector<shared_ptr<ASTNode>> ast_nodes;
  if (this->tree_->get_tag() == ASTNodeTag::VAL) {
    if (rhs.tree_->get_tag() == ASTNodeTag::VAL) {
      ast_nodes.push_back(this->tree_);
      ast_nodes.push_back(rhs.tree_);
    } else {
      // rhs is expression, lhs is value
      auto rhs_tree_expr = dynamic_cast<ASTNodeExpr*>(rhs.tree_.get());
      ast_nodes.push_back(this->tree_);
      ast_nodes.insert(
          ast_nodes.end(),
          rhs_tree_expr->nodes_.begin(),
          rhs_tree_expr->nodes_.end());
    }
  } else if (rhs.tree_->get_tag() == ASTNodeTag::VAL) {
    // lhs is expression, rhs is value
    auto lhs_tree_expr = dynamic_cast<ASTNodeExpr*>(this->tree_.get());
    ast_nodes.insert(
        ast_nodes.end(),
        lhs_tree_expr->nodes_.begin(),
        lhs_tree_expr->nodes_.end());
    ast_nodes.push_back(rhs.tree_);
  } else {
    // Both trees are expression trees
    auto lhs_tree_expr = dynamic_cast<ASTNodeExpr*>(this->tree_.get());
    auto rhs_tree_expr = dynamic_cast<ASTNodeExpr*>(rhs.tree_.get());

    if (combination_op == lhs_tree_expr->combination_op_ &&
        lhs_tree_expr->combination_op_ == rhs_tree_expr->combination_op_) {
      // same op
      ast_nodes.insert(
          ast_nodes.end(),
          lhs_tree_expr->nodes_.begin(),
          lhs_tree_expr->nodes_.end());
      ast_nodes.insert(
          ast_nodes.end(),
          rhs_tree_expr->nodes_.begin(),
          rhs_tree_expr->nodes_.end());
    } else if (combination_op == lhs_tree_expr->combination_op_) {
      ast_nodes.insert(
          ast_nodes.end(),
          lhs_tree_expr->nodes_.begin(),
          lhs_tree_expr->nodes_.end());
      ast_nodes.push_back(rhs.tree_);
    } else if (combination_op == rhs_tree_expr->combination_op_) {
      ast_nodes.push_back(this->tree_);
      ast_nodes.insert(
          ast_nodes.end(),
          rhs_tree_expr->nodes_.begin(),
          rhs_tree_expr->nodes_.end());
    } else {
      ast_nodes.push_back(this->tree_);
      ast_nodes.push_back(rhs.tree_);
    }
  }

  combined_cond->tree_ =
      make_shared<ASTNodeExpr>(HERE(), ast_nodes, combination_op);
  return Status::Ok();
}

bool QueryCondition::empty() const {
  return clauses_.empty();
}

std::unordered_set<std::string>& QueryCondition::field_names() const {
  if (field_names_.empty()) {
    for (const auto& clause : clauses_) {
      field_names_.insert(clause.field_name_);
    }
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

/** Used to create a new result slab in QueryCondition::apply_clause. */
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
std::vector<ResultCellSlab> QueryCondition::apply_clause(
    const QueryCondition::Clause& clause,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs) const {
  std::vector<ResultCellSlab> ret;
  const std::string& field_name = clause.field_name_;

  for (const auto& rcs : result_cell_slabs) {
    ResultTile* const result_tile = rcs.tile_;
    const uint64_t start = rcs.start_;
    const uint64_t length = rcs.length_;

    // Handle an empty range.
    if (result_tile == nullptr && !nullable) {
      const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
          fill_value.data(),
          fill_value.size(),
          clause.condition_value_,
          clause.condition_value_data_.size());
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

          // Compare the cell value against the value in the clause.
          const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
              cell_value,
              cell_size,
              clause.condition_value_,
              clause.condition_value_data_.size());
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

          // Compare the cell value against the value in the clause.
          const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
              cell_value,
              cell_size,
              clause.condition_value_,
              clause.condition_value_data_.size());
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
QueryCondition::apply_clause(
    const Clause& clause,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs) const {
  std::vector<ResultCellSlab> ret;
  switch (clause.op_) {
    case QueryConditionOp::LT:
      ret = apply_clause<T, QueryConditionOp::LT>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    case QueryConditionOp::LE:
      ret = apply_clause<T, QueryConditionOp::LE>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    case QueryConditionOp::GT:
      ret = apply_clause<T, QueryConditionOp::GT>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    case QueryConditionOp::GE:
      ret = apply_clause<T, QueryConditionOp::GE>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    case QueryConditionOp::EQ:
      ret = apply_clause<T, QueryConditionOp::EQ>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
      break;
    case QueryConditionOp::NE:
      ret = apply_clause<T, QueryConditionOp::NE>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
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
QueryCondition::apply_clause(
    const QueryCondition::Clause& clause,
    const ArraySchema& array_schema,
    const uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs) const {
  const auto attribute = array_schema.attribute(clause.field_name_);
  if (!attribute) {
    return {
        Status_QueryConditionError("Unknown attribute " + clause.field_name_),
        nullopt};
  }

  const ByteVecValue fill_value = attribute->fill_value();
  const bool var_size = attribute->var_size();
  const bool nullable = attribute->nullable();
  switch (attribute->type()) {
    case Datatype::INT8:
      return apply_clause<int8_t>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::UINT8:
      return apply_clause<uint8_t>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::INT16:
      return apply_clause<int16_t>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::UINT16:
      return apply_clause<uint16_t>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::INT32:
      return apply_clause<int32_t>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::UINT32:
      return apply_clause<uint32_t>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::INT64:
      return apply_clause<int64_t>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::UINT64:
      return apply_clause<uint64_t>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::FLOAT32:
      return apply_clause<float>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::FLOAT64:
      return apply_clause<double>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::STRING_ASCII:
      return apply_clause<char*>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
    case Datatype::CHAR:
      if (var_size) {
        return apply_clause<char*>(
            clause, stride, var_size, nullable, fill_value, result_cell_slabs);
      }
      return apply_clause<char>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
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
      return apply_clause<int64_t>(
          clause, stride, var_size, nullable, fill_value, result_cell_slabs);
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
                  clause.field_name_),
              nullopt};
  }

  return {Status::Ok(), nullopt};
}

Status QueryCondition::apply(
    const ArraySchema& array_schema,
    std::vector<ResultCellSlab>& result_cell_slabs,
    const uint64_t stride) const {
  if (clauses_.empty()) {
    return Status::Ok();
  }

  // Iterate through each clause, mutating the result cell
  // slabs to skip cells that do not fit into any of the
  // clauses. This assumes all clauses are combined with a
  // logical "AND".
  for (const auto& clause : clauses_) {
    auto&& [st, tmp_result_cell_slabs] =
        apply_clause(clause, array_schema, stride, result_cell_slabs);
    RETURN_NOT_OK(st);
    result_cell_slabs = std::move(*tmp_result_cell_slabs);
  }

  return Status::Ok();
}

template <typename T, QueryConditionOp Op>
void QueryCondition::apply_clause_dense(
    const QueryCondition::Clause& clause,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t length,
    const uint64_t src_cell,
    const uint64_t stride,
    const bool var_size,
    uint8_t* result_buffer) const {
  const std::string& field_name = clause.field_name_;

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
    for (uint64_t c = 0; c < length; ++c) {
      // Check the previous cell here, which breaks vectorization but as this
      // is string data requiring a strcmp which cannot be vectorized, this is
      // ok.
      if (result_buffer[start + c] != 0) {
        const uint64_t buffer_offset = buffer_offsets[start + c * stride];
        const uint64_t next_cell_offset =
            (start + c * stride + 1 < buffer_offsets_el) ?
                buffer_offsets[start + c * stride + 1] :
                buffer_size;
        const uint64_t cell_size = next_cell_offset - buffer_offset;

        // Get the cell value.
        const void* const cell_value = buffer + buffer_offset;

        // Compare the cell value against the value in the clause.
        const bool cmp = BinaryCmp<T, Op>::cmp(
            cell_value,
            cell_size,
            clause.condition_value_,
            clause.condition_value_data_.size());

        // Set the value.
        result_buffer[start + c] &= (uint8_t)cmp;
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
    for (uint64_t c = 0; c < length; ++c) {
      // Get the cell value.
      const void* const cell_value = buffer + buffer_offset;
      buffer_offset += buffer_offset_inc;

      // Compare the cell value against the value in the clause.
      const bool cmp = BinaryCmp<T, Op>::cmp(
          cell_value,
          cell_size,
          clause.condition_value_,
          clause.condition_value_data_.size());

      // Set the value.
      result_buffer[start + c] &= (uint8_t)cmp;
    }
  }
}

template <typename T>
Status QueryCondition::apply_clause_dense(
    const Clause& clause,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t length,
    const uint64_t src_cell,
    const uint64_t stride,
    const bool var_size,
    uint8_t* result_buffer) const {
  switch (clause.op_) {
    case QueryConditionOp::LT:
      apply_clause_dense<T, QueryConditionOp::LT>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
      break;
    case QueryConditionOp::LE:
      apply_clause_dense<T, QueryConditionOp::LE>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
      break;
    case QueryConditionOp::GT:
      apply_clause_dense<T, QueryConditionOp::GT>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
      break;
    case QueryConditionOp::GE:
      apply_clause_dense<T, QueryConditionOp::GE>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
      break;
    case QueryConditionOp::EQ:
      apply_clause_dense<T, QueryConditionOp::EQ>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
      break;
    case QueryConditionOp::NE:
      apply_clause_dense<T, QueryConditionOp::NE>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
      break;
    default:
      return Status_QueryConditionError(
          "Cannot perform query comparison; Unknown query "
          "condition operator");
  }

  return Status::Ok();
}

Status QueryCondition::apply_clause_dense(
    const QueryCondition::Clause& clause,
    const ArraySchema& array_schema,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t length,
    const uint64_t src_cell,
    const uint64_t stride,
    uint8_t* result_buffer) const {
  const auto attribute = array_schema.attribute(clause.field_name_);
  if (!attribute) {
    return Status_QueryConditionError(
        "Unknown attribute " + clause.field_name_);
  }

  const bool var_size = attribute->var_size();
  const bool nullable = attribute->nullable();

  // Process the validity buffer now.
  if (nullable) {
    const auto tile_tuple = result_tile->tile_tuple(clause.field_name_);
    const auto& tile_validity = std::get<2>(*tile_tuple);
    const auto buffer_validity =
        static_cast<uint8_t*>(tile_validity.data()) + src_cell;
    ;

    // Null values can only be specified for equality operators.
    if (clause.condition_value_ == nullptr) {
      if (clause.op_ == QueryConditionOp::NE) {
        for (uint64_t c = 0; c < length; ++c) {
          result_buffer[start + c] *= buffer_validity[start + c * stride] != 0;
        }
      } else {
        for (uint64_t c = 0; c < length; ++c) {
          result_buffer[start + c] *= buffer_validity[start + c * stride] == 0;
        }
      }
      return Status::Ok();
    } else {
      // Turn off bitmap values for null cells.
      for (uint64_t c = 0; c < length; ++c) {
        result_buffer[start + c] *= buffer_validity[start + c * stride] != 0;
      }
    }
  }

  switch (attribute->type()) {
    case Datatype::INT8:
      return apply_clause_dense<int8_t>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::UINT8:
      return apply_clause_dense<uint8_t>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::INT16:
      return apply_clause_dense<int16_t>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::UINT16:
      return apply_clause_dense<uint16_t>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::INT32:
      return apply_clause_dense<int32_t>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::UINT32:
      return apply_clause_dense<uint32_t>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::INT64:
      return apply_clause_dense<int64_t>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::UINT64:
      return apply_clause_dense<uint64_t>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::FLOAT32:
      return apply_clause_dense<float>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::FLOAT64:
      return apply_clause_dense<double>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::STRING_ASCII:
      return apply_clause_dense<char*>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
    case Datatype::CHAR:
      if (var_size) {
        return apply_clause_dense<char*>(
            clause,
            result_tile,
            start,
            length,
            src_cell,
            stride,
            var_size,
            result_buffer);
      }
      return apply_clause_dense<char>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
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
      return apply_clause_dense<int64_t>(
          clause,
          result_tile,
          start,
          length,
          src_cell,
          stride,
          var_size,
          result_buffer);
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
          clause.field_name_);
  }

  return Status::Ok();
}

Status QueryCondition::apply_dense(
    const ArraySchema& array_schema,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t length,
    const uint64_t src_cell,
    const uint64_t stride,
    uint8_t* result_buffer) {
  // Iterate through each clause.
  // This assumes all clauses are combined with a logical "AND".
  for (const auto& clause : clauses_) {
    RETURN_NOT_OK(apply_clause_dense(
        clause,
        array_schema,
        result_tile,
        start,
        length,
        src_cell,
        stride,
        result_buffer));
  }

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
void QueryCondition::apply_clause_sparse(
    const QueryCondition::Clause& clause,
    ResultTile& result_tile,
    const bool var_size,
    std::vector<BitmapType>& result_bitmap) const {
  const auto tile_tuple = result_tile.tile_tuple(clause.field_name_);

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

        // Compare the cell value against the value in the clause.
        const bool cmp = BinaryCmp<T, Op>::cmp(
            cell_value,
            cell_size,
            clause.condition_value_,
            clause.condition_value_data_.size());

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

      // Compare the cell value against the value in the clause.
      const bool cmp = BinaryCmp<T, Op>::cmp(
          cell_value,
          cell_size,
          clause.condition_value_,
          clause.condition_value_data_.size());

      // Set the value.
      result_bitmap[c] *= cmp;
    }
  }
}

template <typename T, typename BitmapType>
Status QueryCondition::apply_clause_sparse(
    const Clause& clause,
    ResultTile& result_tile,
    const bool var_size,
    std::vector<BitmapType>& result_bitmap) const {
  switch (clause.op_) {
    case QueryConditionOp::LT:
      apply_clause_sparse<T, QueryConditionOp::LT>(
          clause, result_tile, var_size, result_bitmap);
      break;
    case QueryConditionOp::LE:
      apply_clause_sparse<T, QueryConditionOp::LE>(
          clause, result_tile, var_size, result_bitmap);
      break;
    case QueryConditionOp::GT:
      apply_clause_sparse<T, QueryConditionOp::GT>(
          clause, result_tile, var_size, result_bitmap);
      break;
    case QueryConditionOp::GE:
      apply_clause_sparse<T, QueryConditionOp::GE>(
          clause, result_tile, var_size, result_bitmap);
      break;
    case QueryConditionOp::EQ:
      apply_clause_sparse<T, QueryConditionOp::EQ>(
          clause, result_tile, var_size, result_bitmap);
      break;
    case QueryConditionOp::NE:
      apply_clause_sparse<T, QueryConditionOp::NE>(
          clause, result_tile, var_size, result_bitmap);
      break;
    default:
      return Status_QueryConditionError(
          "Cannot perform query comparison; Unknown query "
          "condition operator");
  }

  return Status::Ok();
}

template <typename BitmapType>
Status QueryCondition::apply_clause_sparse(
    const QueryCondition::Clause& clause,
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    std::vector<BitmapType>& result_bitmap) const {
  const auto attribute = array_schema.attribute(clause.field_name_);
  if (!attribute) {
    return Status_QueryConditionError(
        "Unknown attribute " + clause.field_name_);
  }

  const bool var_size = attribute->var_size();
  const bool nullable = attribute->nullable();

  // Process the validity buffer now.

  if (nullable) {
    const auto tile_tuple = result_tile.tile_tuple(clause.field_name_);
    const auto& tile_validity = std::get<2>(*tile_tuple);
    const auto buffer_validity = static_cast<uint8_t*>(tile_validity.data());

    // Null values can only be specified for equality operators.
    if (clause.condition_value_ == nullptr) {
      if (clause.op_ == QueryConditionOp::NE) {
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
      return apply_clause_sparse<int8_t, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::UINT8:
      return apply_clause_sparse<uint8_t, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::INT16:
      return apply_clause_sparse<int16_t, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::UINT16:
      return apply_clause_sparse<uint16_t, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::INT32:
      return apply_clause_sparse<int32_t, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::UINT32:
      return apply_clause_sparse<uint32_t, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::INT64:
      return apply_clause_sparse<int64_t, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::UINT64:
      return apply_clause_sparse<uint64_t, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::FLOAT32:
      return apply_clause_sparse<float, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::FLOAT64:
      return apply_clause_sparse<double, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::STRING_ASCII:
      return apply_clause_sparse<char*, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
    case Datatype::CHAR:
      if (var_size) {
        return apply_clause_sparse<char*, BitmapType>(
            clause, result_tile, var_size, result_bitmap);
      }
      return apply_clause_sparse<char, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
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
      return apply_clause_sparse<int64_t, BitmapType>(
          clause, result_tile, var_size, result_bitmap);
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
          clause.field_name_);
  }

  return Status::Ok();
}

template <typename BitmapType>
Status QueryCondition::apply_sparse(
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    std::vector<BitmapType>& result_bitmap,
    uint64_t* cell_count) {
  // Iterate through each clause.
  // This assumes all clauses are combined with a logical "AND".
  for (const auto& clause : clauses_) {
    RETURN_NOT_OK(
        apply_clause_sparse(clause, array_schema, result_tile, result_bitmap));
  }

  *cell_count = std::accumulate(result_bitmap.begin(), result_bitmap.end(), 0);

  return Status::Ok();
}

std::string QueryCondition::ast_to_str() {
  return tree_->to_str();
}

void QueryCondition::set_clauses(std::vector<Clause>&& clauses) {
  clauses_ = std::move(clauses);
}

void QueryCondition::set_combination_ops(
    std::vector<QueryConditionCombinationOp>&& combination_ops) {
  combination_ops_ = std::move(combination_ops);
}

std::vector<QueryCondition::Clause> QueryCondition::clauses() const {
  return clauses_;
}

std::vector<QueryConditionCombinationOp> QueryCondition::combination_ops()
    const {
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
