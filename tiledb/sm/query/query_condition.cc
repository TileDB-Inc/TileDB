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

using namespace tiledb::common;

namespace tiledb {
namespace sm {

QueryCondition::QueryCondition() {
}

QueryCondition::QueryCondition(
    std::string&& field_name,
    const void* const condition_value,
    const uint64_t condition_value_size,
    const QueryConditionOp op) {
  clauses_.emplace_back(
      std::move(field_name), condition_value, condition_value_size, op);
}

QueryCondition::QueryCondition(const QueryCondition& rhs)
    : clauses_(rhs.clauses_)
    , combination_ops_(rhs.combination_ops_) {
}

QueryCondition::QueryCondition(QueryCondition&& rhs)
    : clauses_(std::move(rhs.clauses_))
    , combination_ops_(std::move(rhs.combination_ops_)) {
}

QueryCondition::~QueryCondition() {
}

QueryCondition& QueryCondition::operator=(const QueryCondition& rhs) {
  clauses_ = rhs.clauses_;
  combination_ops_ = rhs.combination_ops_;

  return *this;
}

QueryCondition& QueryCondition::operator=(QueryCondition&& rhs) {
  clauses_ = std::move(rhs.clauses_);
  combination_ops_ = std::move(rhs.combination_ops_);

  return *this;
}

Status QueryCondition::check(const ArraySchema* const array_schema) const {
  for (const auto& clause : clauses_) {
    const std::string field_name = clause.field_name_;
    const uint64_t condition_value_size = clause.condition_value_.size();

    const Attribute* const attribute = array_schema->attribute(field_name);
    if (!attribute) {
      return Status::QueryConditionError(
          "Clause field name is not an attribute " + field_name);
    }

    if (attribute->var_size()) {
      return Status::QueryConditionError(
          "Clause attribute may not be var-sized: " + field_name);
    }

    if (attribute->nullable()) {
      return Status::QueryConditionError(
          "Clause attribute may not be nullable: " + field_name);
    }

    if (attribute->cell_val_num() != 1 &&
        attribute->type() != Datatype::STRING_ASCII) {
      return Status::QueryConditionError(
          "Clause attribute must have one value per cell for non-string "
          "attributes: " +
          field_name);
    }

    if (attribute->cell_size() != condition_value_size) {
      return Status::QueryConditionError(
          "Clause condition value size mismatch: " +
          std::to_string(attribute->cell_size()) +
          " != " + std::to_string(condition_value_size));
    }

    switch (attribute->type()) {
      case Datatype::ANY:
        return Status::QueryConditionError(
            "Clause attribute type may not be of type 'ANY': " + field_name);
      case Datatype::STRING_UTF8:
      case Datatype::STRING_UTF16:
      case Datatype::STRING_UTF32:
      case Datatype::STRING_UCS2:
      case Datatype::STRING_UCS4:
        return Status::QueryConditionError(
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
    return Status::QueryConditionError(
        "Cannot combine query conditions; Only the 'AND' "
        "comination op is supported");
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

  return Status::Ok();
}

bool QueryCondition::empty() const {
  return clauses_.empty();
}

std::unordered_set<std::string> QueryCondition::field_names() const {
  if (field_names_.empty()) {
    for (const auto& clause : clauses_) {
      field_names_.insert(clause.field_name_);
    }
  }

  return field_names_;
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

template <typename T, QueryConditionOp Op>
void QueryCondition::apply_clause(
    const QueryCondition::Clause& clause,
    const uint64_t stride,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<ResultCellSlab>* const out_result_cell_slabs) const {
  const std::string& field_name = clause.field_name_;

  for (const auto& rcs : result_cell_slabs) {
    ResultTile* const result_tile = rcs.tile_;
    const uint64_t start = rcs.start_;
    const uint64_t length = rcs.length_;

    // Handle an empty range.
    if (result_tile == nullptr) {
      const bool cmp = BinaryCmp<T, Op>::cmp(
          fill_value.data(),
          fill_value.size(),
          clause.condition_value_.data(),
          clause.condition_value_.size());
      if (cmp) {
        out_result_cell_slabs->emplace_back(result_tile, start, length);
      }
    } else {
      const auto tile_tuple = result_tile->tile_tuple(field_name);
      const auto& tile = std::get<0>(*tile_tuple);
      const uint64_t cell_size = tile.cell_size();
      ChunkedBuffer* const chunked_buffer = tile.chunked_buffer();
      char* const buffer =
          static_cast<char*>(chunked_buffer->get_contiguous_unsafe());

      // Start the pending result cell slab at the start position
      // of the current result cell slab.
      uint64_t pending_start = start;

      // Iterate through each cell in this slab.
      uint64_t c = 0;
      uint64_t buffer_offset = start * cell_size;
      uint64_t buffer_offset_inc = stride * cell_size;
      while (c < length) {
        // Get the cell value.
        const void* const cell_value = buffer + buffer_offset;
        buffer_offset += buffer_offset_inc;

        // Compare the cell value against the value in the clause.
        const bool cmp = BinaryCmp<T, Op>::cmp(
            cell_value,
            cell_size,
            clause.condition_value_.data(),
            clause.condition_value_.size());

        if (!cmp) {
          // Create a result cell slab if there are pending cells.
          if (pending_start != start + c) {
            const uint64_t rcs_start =
                start + ((pending_start - start) * stride);
            const uint64_t rcs_length = c - (pending_start - start);
            out_result_cell_slabs->emplace_back(
                result_tile, rcs_start, rcs_length);
          }

          // Set the start of the pending result cell slab.
          pending_start = start + c + 1;
        }

        ++c;
      }

      // Create the final result cell slab if there are pending cells.
      if (pending_start != start + c) {
        const uint64_t rcs_start = start + ((pending_start - start) * stride);
        const uint64_t rcs_length = c - (pending_start - start);
        out_result_cell_slabs->emplace_back(result_tile, rcs_start, rcs_length);
      }
    }
  }
}

template <typename T>
Status QueryCondition::apply_clause(
    const Clause& clause,
    const uint64_t stride,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<ResultCellSlab>* const out_result_cell_slabs) const {
  switch (clause.op_) {
    case QueryConditionOp::LT:
      apply_clause<T, QueryConditionOp::LT>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
      break;
    case QueryConditionOp::LE:
      apply_clause<T, QueryConditionOp::LE>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
      break;
    case QueryConditionOp::GT:
      apply_clause<T, QueryConditionOp::GT>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
      break;
    case QueryConditionOp::GE:
      apply_clause<T, QueryConditionOp::GE>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
      break;
    case QueryConditionOp::EQ:
      apply_clause<T, QueryConditionOp::EQ>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
      break;
    case QueryConditionOp::NE:
      apply_clause<T, QueryConditionOp::NE>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
      break;
    default:
      return Status::QueryConditionError(
          "Cannot perform query comparison; Unknown query "
          "condition operator");
  }

  return Status::Ok();
}

Status QueryCondition::apply_clause(
    const QueryCondition::Clause& clause,
    const ArraySchema* const array_schema,
    const uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<ResultCellSlab>* const out_result_cell_slabs) const {
  const Attribute* const attribute =
      array_schema->attribute(clause.field_name_);
  if (!attribute) {
    return Status::QueryConditionError(
        "Unknown attribute " + clause.field_name_);
  }

  const ByteVecValue fill_value = attribute->fill_value();
  switch (attribute->type()) {
    case Datatype::INT8:
      return apply_clause<int8_t>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::UINT8:
      return apply_clause<uint8_t>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::INT16:
      return apply_clause<int16_t>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::UINT16:
      return apply_clause<uint16_t>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::INT32:
      return apply_clause<int32_t>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::UINT32:
      return apply_clause<uint32_t>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::INT64:
      return apply_clause<int64_t>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::UINT64:
      return apply_clause<uint64_t>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::FLOAT32:
      return apply_clause<float>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::FLOAT64:
      return apply_clause<double>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::STRING_ASCII:
      return apply_clause<char*>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::CHAR:
      return apply_clause<char>(
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
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
          clause, stride, fill_value, result_cell_slabs, out_result_cell_slabs);
    case Datatype::ANY:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      return Status::QueryConditionError(
          "Cannot perform query comparison; Unsupported query "
          "conditional type on " +
          clause.field_name_);
  }

  return Status::Ok();
}

Status QueryCondition::apply(
    const ArraySchema* const array_schema,
    std::vector<ResultCellSlab>* const result_cell_slabs,
    const uint64_t stride) const {
  if (clauses_.empty()) {
    return Status::Ok();
  }

  // Iterate through each clause, mutating the result cell
  // slabs to skip cells that do not fit into any of the
  // clauses. This assumes all clauses are combined with a
  // logical "AND".
  for (const auto& clause : clauses_) {
    std::vector<ResultCellSlab> tmp_result_cell_slabs;
    RETURN_NOT_OK(apply_clause(
        clause,
        array_schema,
        stride,
        *result_cell_slabs,
        &tmp_result_cell_slabs));
    *result_cell_slabs = tmp_result_cell_slabs;
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
