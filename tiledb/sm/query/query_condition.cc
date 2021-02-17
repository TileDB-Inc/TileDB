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
    const uint64_t condition_value_size = clause.condition_value_size_;

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

  return Status::Ok();
}

bool QueryCondition::empty() const {
  return clauses_.empty();
}

std::unordered_set<std::string> QueryCondition::field_names() const {
  std::unordered_set<std::string> field_names_set;
  for (const auto& clause : clauses_) {
    field_names_set.insert(clause.field_name_);
  }

  return field_names_set;
}

template <class T>
bool QueryCondition::cmp_clause(
    void* const cell_value, const Clause& clause) const {
  switch (clause.op_) {
    case QueryConditionOp::LT:
      return *static_cast<const T*>(cell_value) <
             *static_cast<const T*>(clause.condition_value_);
    case QueryConditionOp::LE:
      return *static_cast<const T*>(cell_value) <=
             *static_cast<const T*>(clause.condition_value_);
    case QueryConditionOp::GT:
      return *static_cast<const T*>(cell_value) >
             *static_cast<const T*>(clause.condition_value_);
    case QueryConditionOp::GE:
      return *static_cast<const T*>(cell_value) >=
             *static_cast<const T*>(clause.condition_value_);
    case QueryConditionOp::EQ:
      return *static_cast<const T*>(cell_value) ==
             *static_cast<const T*>(clause.condition_value_);
    case QueryConditionOp::NE:
      return *static_cast<const T*>(cell_value) !=
             *static_cast<const T*>(clause.condition_value_);
    default:
      LOG_FATAL("Unknown query condition op");
  }

  return false;
}

bool QueryCondition::cmp_clause_str(
    void* const cell_value,
    const uint64_t cell_value_size,
    const Clause& clause) const {
  std::string cell_value_str(
      static_cast<const char*>(cell_value), cell_value_size);
  std::string condition_value_str(
      static_cast<const char*>(clause.condition_value_),
      clause.condition_value_size_);

  switch (clause.op_) {
    case QueryConditionOp::LT:
      return cell_value_str < condition_value_str;
    case QueryConditionOp::LE:
      return cell_value_str <= condition_value_str;
    case QueryConditionOp::GT:
      return cell_value_str > condition_value_str;
    case QueryConditionOp::GE:
      return cell_value_str >= condition_value_str;
    case QueryConditionOp::EQ:
      return cell_value_str == condition_value_str;
    case QueryConditionOp::NE:
      return cell_value_str != condition_value_str;
    default:
      LOG_FATAL("Unknown query condition op");
  }

  return false;
}

template <class T>
Status QueryCondition::cmp_clause(
    ResultTile* const result_tile,
    const uint64_t cell_idx,
    const Clause& clause,
    bool* const cmp) const {
  T cell_value;
  RETURN_NOT_OK(
      result_tile->read(clause.field_name_, &cell_value, 0, cell_idx, 1));
  *cmp = cmp_clause<T>(&cell_value, clause);
  return Status::Ok();
}

Status QueryCondition::cmp_clause_str(
    ResultTile* const result_tile,
    const uint64_t cell_idx,
    const Clause& clause,
    bool* const cmp) const {
  const Tile& tile = std::get<0>(*result_tile->tile_tuple(clause.field_name_));
  const uint64_t cell_size = tile.cell_size();

  char* const cell_value = static_cast<char*>(tdb_malloc(cell_size));

  const Status st =
      result_tile->read(clause.field_name_, cell_value, 0, cell_idx, 1);
  if (!st.ok()) {
    tdb_free(cell_value);
    return st;
  }

  *cmp = cmp_clause_str(cell_value, cell_size, clause);

  tdb_free(cell_value);

  return Status::Ok();
}

Status QueryCondition::cmp_cell(
    const ArraySchema* const array_schema,
    ResultTile* const result_tile,
    const uint64_t cell_idx,
    bool* const cmp) const {
  if (clauses_.empty() || result_tile->cell_num() == 0) {
    *cmp = true;
    return Status::Ok();
  }

  // We have a combination op between each clause, where each
  // combination op is an 'AND'.
  assert(clauses_.size() - 1 == combination_ops_.size());

  for (const auto& clause : clauses_) {
    const Attribute* const attribute =
        array_schema->attribute(clause.field_name_);
    if (!attribute) {
      return LOG_STATUS(Status::QueryConditionError(
          "Unknown attribute " + clause.field_name_));
    }

    const Datatype type = attribute->type();
    switch (type) {
      case Datatype::INT8:
        RETURN_NOT_OK(cmp_clause<int8_t>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::UINT8:
        RETURN_NOT_OK(cmp_clause<uint8_t>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::INT16:
        RETURN_NOT_OK(cmp_clause<int16_t>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::UINT16:
        RETURN_NOT_OK(cmp_clause<uint16_t>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::INT32:
        RETURN_NOT_OK(cmp_clause<int32_t>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::UINT32:
        RETURN_NOT_OK(cmp_clause<uint32_t>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::INT64:
        RETURN_NOT_OK(cmp_clause<int64_t>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::UINT64:
        RETURN_NOT_OK(cmp_clause<uint64_t>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::FLOAT32:
        RETURN_NOT_OK(cmp_clause<float>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::FLOAT64:
        RETURN_NOT_OK(cmp_clause<double>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::STRING_ASCII:
        RETURN_NOT_OK(cmp_clause_str(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::CHAR:
        RETURN_NOT_OK(cmp_clause<char>(result_tile, cell_idx, clause, cmp));
        break;
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
        RETURN_NOT_OK(cmp_clause<int64_t>(result_tile, cell_idx, clause, cmp));
        break;
      case Datatype::ANY:
      case Datatype::STRING_UTF8:
      case Datatype::STRING_UTF16:
      case Datatype::STRING_UTF32:
      case Datatype::STRING_UCS2:
      case Datatype::STRING_UCS4:
      default:
        return LOG_STATUS(Status::ReaderError(
            "Cannot perform query comparison; Unsupported query "
            "conditional type on " +
            clause.field_name_));
    }

    if (!*cmp) {
      return Status::Ok();
    }
  }

  *cmp = true;
  return Status::Ok();
}

Status QueryCondition::cmp_cell_fill_value(
    const ArraySchema* const array_schema, bool* const cmp) const {
  if (clauses_.empty()) {
    *cmp = true;
    return Status::Ok();
  }

  // We have a combination op between each clause, where each
  // combination op is an 'AND'.
  assert(clauses_.size() - 1 == combination_ops_.size());

  for (const auto& clause : clauses_) {
    const Attribute* const attribute =
        array_schema->attribute(clause.field_name_);
    if (!attribute) {
      return LOG_STATUS(Status::QueryConditionError(
          "Unknown attribute " + clause.field_name_));
    }

    const Datatype type = attribute->type();
    ByteVecValue fill_value = attribute->fill_value();
    switch (type) {
      case Datatype::INT8:
        *cmp = cmp_clause<int8_t>(fill_value.data(), clause);
        break;
      case Datatype::UINT8:
        *cmp = cmp_clause<uint8_t>(fill_value.data(), clause);
        break;
      case Datatype::INT16:
        *cmp = cmp_clause<int16_t>(fill_value.data(), clause);
        break;
      case Datatype::UINT16:
        *cmp = cmp_clause<uint16_t>(fill_value.data(), clause);
        break;
      case Datatype::INT32:
        *cmp = cmp_clause<int32_t>(fill_value.data(), clause);
        break;
      case Datatype::UINT32:
        *cmp = cmp_clause<uint32_t>(fill_value.data(), clause);
        break;
      case Datatype::INT64:
        *cmp = cmp_clause<int64_t>(fill_value.data(), clause);
        break;
      case Datatype::UINT64:
        *cmp = cmp_clause<uint64_t>(fill_value.data(), clause);
        break;
      case Datatype::FLOAT32:
        *cmp = cmp_clause<float>(fill_value.data(), clause);
        break;
      case Datatype::FLOAT64:
        *cmp = cmp_clause<double>(fill_value.data(), clause);
        break;
      case Datatype::STRING_ASCII:
        *cmp = cmp_clause_str(fill_value.data(), fill_value.size(), clause);
        break;
      case Datatype::CHAR:
        *cmp = cmp_clause<char>(fill_value.data(), clause);
        break;
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
        *cmp = cmp_clause<int64_t>(fill_value.data(), clause);
        break;
      case Datatype::ANY:
      case Datatype::STRING_UTF8:
      case Datatype::STRING_UTF16:
      case Datatype::STRING_UTF32:
      case Datatype::STRING_UCS2:
      case Datatype::STRING_UCS4:
      default:
        return LOG_STATUS(Status::ReaderError(
            "Cannot perform query comparison; Unsupported query "
            "conditional type on " +
            clause.field_name_));
    }

    if (!*cmp) {
      return Status::Ok();
    }
  }

  *cmp = true;
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
