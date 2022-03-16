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

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/query/query_ast.h"
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
      condition_value_ = nullptr;

      if (condition_value != nullptr) {
        // Using an empty string litteral for empty string as vector::resize(0)
        // doesn't guarantee an allocation.
        condition_value_ = condition_value_size == 0 ?
                               (void*)"" :
                               condition_value_data_.data();
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
  Status check(const ArraySchema& array_schema) const;

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
  std::unordered_set<std::string>& field_names() const;

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
      const ArraySchema& array_schema,
      std::vector<ResultCellSlab>& result_cell_slabs,
      uint64_t stride) const;

  /**
   * Applies this query condition to a set of cells.
   *
   * @param array_schema The array schema.
   * @param result_tile The result tile to get the cells from.
   * @param start The start cell.
   * @param length The number of cells to process.
   * @param src_cell The cell offset in the source tile.
   * @param stride The stride between cells.
   * @param result_buffer The buffer to use for results.
   * @return Status
   */
  Status apply_dense(
      const ArraySchema& array_schema,
      ResultTile* result_tile,
      const uint64_t start,
      const uint64_t length,
      const uint64_t src_cell,
      const uint64_t stride,
      uint8_t* result_buffer);

  /**
   * Applies this query condition to a set of cells.
   *
   * @param array_schema The array schema.
   * @param result_tile The result tile to get the cells from.
   * @param result_bitmap The bitmap to use for results.
   * @param cell_count The cell count after condition is applied.
   * @return Status
   */
  template <typename BitmapType>
  Status apply_sparse(
      const ArraySchema& array_schema,
      ResultTile& result_tile,
      std::vector<BitmapType>& result_bitmap,
      uint64_t* cell_count);

  /**
   * Returns the string representation of the AST.
   */
  std::string ast_to_str();

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
   *
   * Note that this comparator includes if statements that will
   * prevent vectorization.
   */
  template <typename T, QueryConditionOp Cmp>
  struct BinaryCmpNullChecks;

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

  /** AST Tree structure **/
  shared_ptr<tiledb::sm::ASTNode> tree_{};

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
   * @return The filtered cell slabs.
   */
  template <typename T, QueryConditionOp Op>
  std::vector<ResultCellSlab> apply_clause(
      const Clause& clause,
      uint64_t stride,
      const bool var_size,
      const bool nullable,
      const ByteVecValue& fill_value,
      const std::vector<ResultCellSlab>& result_cell_slabs) const;

  /**
   * Applies a clause on primitive-typed result cell slabs.
   *
   * @param clause The clause to apply.
   * @param stride The stride between cells.
   * @param var_size The attribute is var sized or not.
   * @param nullable The attribute is nullable or not.
   * @param fill_value The fill value for the cells.
   * @param result_cell_slabs The input cell slabs.
   * @return Status, filtered cell slabs.
   */
  template <typename T>
  tuple<Status, optional<std::vector<ResultCellSlab>>> apply_clause(
      const Clause& clause,
      uint64_t stride,
      const bool var_size,
      const bool nullable,
      const ByteVecValue& fill_value,
      const std::vector<ResultCellSlab>& result_cell_slabs) const;

  /**
   * Applies a clause to filter result cells from the input
   * result cell slabs.
   *
   * @param clause The clause to apply.
   * @param array_schema The current array schema.
   * @param stride The stride between cells.
   * @param result_cell_slabs The input cell slabs.
   * @return Status, filtered cell slabs.
   */
  tuple<Status, optional<std::vector<ResultCellSlab>>> apply_clause(
      const QueryCondition::Clause& clause,
      const ArraySchema& array_schema,
      uint64_t stride,
      const std::vector<ResultCellSlab>& result_cell_slabs) const;

  /**
   * Applies a clause on a dense result tile,
   * templated for a query condition operator.
   *
   * @param clause The clause to apply.
   * @param result_tile The result tile to get the cells from.
   * @param start The start cell.
   * @param length The number of cells to process.
   * @param src_cell The cell offset in the source tile.
   * @param stride The stride between cells.
   * @param var_size The attribute is var sized or not.
   * @param result_buffer The result buffer.
   */
  template <typename T, QueryConditionOp Op>
  void apply_clause_dense(
      const QueryCondition::Clause& clause,
      ResultTile* result_tile,
      const uint64_t start,
      const uint64_t length,
      const uint64_t src_cell,
      const uint64_t stride,
      const bool var_size,
      uint8_t* result_buffer) const;

  /**
   * Applies a clause on a dense result tile.
   *
   * @param clause The clause to apply.
   * @param result_tile The result tile to get the cells from.
   * @param start The start cell.
   * @param length The number of cells to process.
   * @param src_cell The cell offset in the source tile.
   * @param stride The stride between cells.
   * @param var_size The attribute is var sized or not.
   * @param result_buffer The result buffer.
   * @return Status.
   */
  template <typename T>
  Status apply_clause_dense(
      const Clause& clause,
      ResultTile* result_tile,
      const uint64_t start,
      const uint64_t length,
      const uint64_t src_cell,
      const uint64_t stride,
      const bool var_size,
      uint8_t* result_buffer) const;

  /**
   * Applies a clause to filter result cells from the input
   * result tile.
   *
   * @param clause The clause to apply.
   * @param array_schema The current array schema.
   * @param result_tile The result tile to get the cells from.
   * @param start The start cell.
   * @param length The number of cells to process.
   * @param src_cell The cell offset in the source tile.
   * @param stride The stride between cells.
   * @param result_buffer The result buffer.
   * @return Status.
   */
  Status apply_clause_dense(
      const QueryCondition::Clause& clause,
      const ArraySchema& array_schema,
      ResultTile* result_tile,
      const uint64_t start,
      const uint64_t length,
      const uint64_t src_cell,
      const uint64_t stride,
      uint8_t* result_buffer) const;

  /**
   * Applies a clause on a sparse result tile,
   * templated for a query condition operator.
   *
   * @param clause The clause to apply.
   * @param result_tile The result tile to get the cells from.
   * @param var_size The attribute is var sized or not.
   * @param result_bitmap The result bitmap.
   */
  template <typename T, QueryConditionOp Op, typename BitmapType>
  void apply_clause_sparse(
      const QueryCondition::Clause& clause,
      ResultTile& result_tile,
      const bool var_size,
      std::vector<BitmapType>& result_bitmap) const;

  /**
   * Applies a clause on a sparse result tile.
   *
   * @param clause The clause to apply.
   * @param result_tile The result tile to get the cells from.
   * @param var_size The attribute is var sized or not.
   * @param result_bitmap The result bitmap.
   * @return Status.
   */
  template <typename T, typename BitmapType>
  Status apply_clause_sparse(
      const Clause& clause,
      ResultTile& result_tile,
      const bool var_size,
      std::vector<BitmapType>& result_bitmap) const;

  /**
   * Applies a clause to filter result cells from the input
   * result tile.
   *
   * @param clause The clause to apply.
   * @param array_schema The current array schema.
   * @param result_tile The result tile to get the cells from.
   * @param result_bitmap The result bitmap.
   * @return Status.
   */
  template <typename BitmapType>
  Status apply_clause_sparse(
      const QueryCondition::Clause& clause,
      const ArraySchema& array_schema,
      ResultTile& result_tile,
      std::vector<BitmapType>& result_bitmap) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_CONDITION_H
