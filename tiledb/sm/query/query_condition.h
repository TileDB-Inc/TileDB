/**
 * @file query_condition.h
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
 * Defines the QueryCondition class.
 */

#ifndef TILEDB_QUERY_CONDITION_H
#define TILEDB_QUERY_CONDITION_H

#include <unordered_set>

#include "tiledb/common/logger.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/query/result_cell_slab.h"
#include "tiledb/sm/query/result_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

enum class QueryConditionCombinationOp : uint8_t;

class QueryCondition {
 public:
  /* ********************************* */
  /*          PUBLIC DATATYPES         */
  /* ********************************* */

  /** Represents a single, conditional clause. */
  struct Clause {
    /** Value constructor. */
    Clause(
        std::string&& field_name,
        const void* const condition_value,
        const uint64_t condition_value_size,
        const QueryConditionOp op)
        : field_name_(std::move(field_name))
        , op_(op) {
      condition_value_data_.resize(condition_value_size);
      condition_value_ =
          condition_value == nullptr ? nullptr : condition_value_data_.data();
      if (condition_value != nullptr) {
        memcpy(
            condition_value_data_.data(),
            condition_value,
            condition_value_size);
      }
    };

    /** Copy constructor. */
    Clause(const Clause& rhs)
        : field_name_(rhs.field_name_)
        , condition_value_data_(rhs.condition_value_data_)
        , condition_value_(
              rhs.condition_value_ == nullptr ? nullptr :
                                                condition_value_data_.data())
        , op_(rhs.op_){};

    /** Move constructor. */
    Clause(Clause&& rhs)
        : field_name_(std::move(rhs.field_name_))
        , condition_value_data_(std::move(rhs.condition_value_data_))
        , condition_value_(
              rhs.condition_value_ == nullptr ? nullptr :
                                                condition_value_data_.data())
        , op_(rhs.op_){};

    /** Assignment operator. */
    Clause& operator=(const Clause& rhs) {
      field_name_ = rhs.field_name_;
      condition_value_data_ = rhs.condition_value_data_;
      condition_value_ = rhs.condition_value_ == nullptr ?
                             nullptr :
                             condition_value_data_.data();
      op_ = rhs.op_;

      return *this;
    }

    /** Move-assignment operator. */
    Clause& operator=(Clause&& rhs) {
      field_name_ = std::move(rhs.field_name_);
      condition_value_data_ = std::move(rhs.condition_value_data_);
      condition_value_ = rhs.condition_value_ == nullptr ?
                             nullptr :
                             condition_value_data_.data();
      op_ = rhs.op_;

      return *this;
    }

    /** The attribute name. */
    std::string field_name_;

    /** The value data. */
    ByteVecValue condition_value_data_;

    /** The value to compare against. */
    void* condition_value_;

    /** The comparison operator. */
    QueryConditionOp op_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  QueryCondition();

  /** Copy constructor. */
  QueryCondition(const QueryCondition& rhs);

  /** Move constructor. */
  QueryCondition(QueryCondition&& rhs);

  /** Destructor. */
  ~QueryCondition();

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Copy-assignment operator. */
  QueryCondition& operator=(const QueryCondition& rhs);

  /** Move-assignment operator. */
  QueryCondition& operator=(QueryCondition&& rhs);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Initializes the instance.
   *
   * @param field_name The name of the field this operation applies to.
   * @param condition_value The value to compare to.
   * @param condition_value_size The byte size of condition_value.
   * @param op The relational operation between the value of the field
   *     and `condition_value`.
   */
  Status init(
      std::string&& field_name,
      const void* condition_value,
      uint64_t condition_value_size,
      QueryConditionOp op);

  /**
   * Verifies that the current state contains supported comparison
   * operations. Currently, we support the following:
   *   - Fixed-size, single-value, non-nullable attributes.
   *   - The QueryConditionCombinationOp::AND operator.
   *
   * @param array_schema The current array schena.
   * @return Status
   */
  Status check(const ArraySchema* array_schema) const;

  /**
   * Combines this instance with the right-hand-side instance by
   * the given operator.
   *
   * @param rhs The condition instance to combine with.
   * @param combination_op The operation to combine them.
   * @param combined_cond The output condition to mutate. This is
   *    expected to be allocated by the caller.
   * @return Status
   */
  Status combine(
      const QueryCondition& rhs,
      QueryConditionCombinationOp combination_op,
      QueryCondition* combined_cond) const;

  /**
   * Returns true if this condition does not have any conditional clauses.
   */
  bool empty() const;

  /**
   * Returns a set of all unique field names among the conditional clauses.
   */
  std::unordered_set<std::string> field_names() const;

  /**
   * Applies this query condition to `result_cell_slabs`.
   *
   * @param array_schema The array schema associated with `result_cell_slabs`.
   * @param result_cell_slabs The cell slabs to filter. Mutated to remove cell
   *   slabs that do not meet the criteria in this query condition.
   * @param stride The stride between cells.
   * @return Status
   */
  Status apply(
      const ArraySchema* array_schema,
      std::vector<ResultCellSlab>* result_cell_slabs,
      uint64_t stride) const;

  /**
   * Sets the clauses. This is internal state to only be used in
   * the serialization path.
   */
  void set_clauses(std::vector<Clause>&& clauses);

  /**
   * Sets the combination ops. This is internal state to only be used in
   * the serialization path.
   */
  void set_combination_ops(
      std::vector<QueryConditionCombinationOp>&& combination_ops);

  /**
   * Returns the clauses. This is internal state to only be used in
   * the serialization path.
   */
  std::vector<Clause> clauses() const;

  /**
   * Returns the combination ops. This is internal state to only be used in
   * the serialization path.
   */
  std::vector<QueryConditionCombinationOp> combination_ops() const;

 private:
  /* ********************************* */
  /*         PRIVATE DATATYPES         */
  /* ********************************* */

  /**
   * Performs a binary comparison between two primitive types.
   * We use a `struct` here because it can support partial
   * template specialization while a standard function does not.
   */
  template <typename T, QueryConditionOp Cmp>
  struct BinaryCmp;

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * All clauses in this condition. Clauses `clauses_[i]` and
   * `clauses_[i + 1]` are combined by the `combination_ops_[i]`
   * operator.
   */
  std::vector<Clause> clauses_;

  /** Caches all field names in `clauses_`.  */
  mutable std::unordered_set<std::string> field_names_;

  /** Logical operators to combine clauses stored in `clauses_`. */
  std::vector<QueryConditionCombinationOp> combination_ops_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Applies a clause on primitive-typed result cell slabs,
   * templated for a query condition operator.
   *
   * @param clause The clause to apply.
   * @param stride The stride between cells.
   * @param var_size The attribute is var sized or not.
   * @param nullable The attribute is nullable or not.
   * @param fill_value The fill value for the cells.
   * @param result_cell_slabs The input cell slabs.
   * @param out_result_cell_slabs The filtered cell slabs.
   */
  template <typename T, QueryConditionOp Op>
  void apply_clause(
      const Clause& clause,
      uint64_t stride,
      const bool var_size,
      const bool nullable,
      const ByteVecValue& fill_value,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      std::vector<ResultCellSlab>* out_result_cell_slabs) const;

  /**
   * Applies a clause on primitive-typed result cell slabs.
   *
   * @param clause The clause to apply.
   * @param stride The stride between cells.
   * @param var_size The attribute is var sized or not.
   * @param nullable The attribute is nullable or not.
   * @param fill_value The fill value for the cells.
   * @param result_cell_slabs The input cell slabs.
   * @param out_result_cell_slabs The filtered cell slabs.
   */
  template <typename T>
  Status apply_clause(
      const Clause& clause,
      uint64_t stride,
      const bool var_size,
      const bool nullable,
      const ByteVecValue& fill_value,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      std::vector<ResultCellSlab>* out_result_cell_slabs) const;

  /**
   * Applies a clause to filter result cells from the input
   * result cell slabs.
   *
   * @param clause The clause to apply.
   * @param array_schema The current array schema.
   * @param stride The stride between cells.
   * @param result_cell_slabs The input cell slabs.
   * @param out_result_cell_slabs The filtered cell slabs.
   */
  Status apply_clause(
      const QueryCondition::Clause& clause,
      const ArraySchema* const array_schema,
      uint64_t stride,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      std::vector<ResultCellSlab>* out_result_cell_slabs) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_CONDITION_H
