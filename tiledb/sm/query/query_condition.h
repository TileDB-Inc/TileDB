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
#include "tiledb/sm/query/result_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

enum class QueryConditionOp : uint8_t;
enum class QueryConditionCombinationOp : uint8_t;

class QueryCondition {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  QueryCondition();

  /** Value constructor.
   *
   * @param field_name The name of the field this operation applies to.
   * @param condition_value The value to compare to.
   * @param condition_value_size The byte size of condition_value.
   * @param op The relational operation between the value of the field
   *     and `condition_value`.
   */
  QueryCondition(
      std::string&& field_name,
      const void* condition_value,
      uint64_t condition_value_size,
      QueryConditionOp op);

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
   * Compares the cell in `result_tile` at `cell_idx` against all of
   * the conditional clauses. The output, `cmp` determines whether
   * the cell should be extracted or filtered.
   *
   * @param array_schema The array schema.
   * @param result_tile The tile containing the cell value.
   * @param cell_idx The index of the cell within `result_tile`.
   * @param cmp Mutates to true if the cell should be extracted.
   *   Otherwise, this mutates to false to indicate that the cell
   *   should be filtered out from the results.
   * @return Status
   */
  Status cmp_cell(
      const ArraySchema* array_schema,
      ResultTile* result_tile,
      uint64_t cell_idx,
      bool* cmp) const;

  /**
   * Compares the against fill values for the attributes in the conditional
   * clauses.
   *
   * @param array_schema The array schema.
   * @param cmp Mutates to true if the cell should be extracted.
   *   Otherwise, this mutates to false to indicate that the cell
   *   should be filtered out from the results.
   * @return Status
   */
  Status cmp_cell_fill_value(const ArraySchema* array_schema, bool* cmp) const;

 private:
  /* ********************************* */
  /*         PRIVATE DATATYPES         */
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
        , condition_value_(condition_value)
        , condition_value_size_(condition_value_size)
        , op_(op){};

    /** Copy constructor. */
    Clause(const Clause& rhs)
        : field_name_(rhs.field_name_)
        , condition_value_(rhs.condition_value_)
        , condition_value_size_(rhs.condition_value_size_)
        , op_(rhs.op_){};

    /** Move constructor. */
    Clause(Clause&& rhs)
        : field_name_(std::move(rhs.field_name_))
        , condition_value_(rhs.condition_value_)
        , condition_value_size_(rhs.condition_value_size_)
        , op_(rhs.op_){};

    /** Assignment operator. */
    Clause& operator=(const Clause& rhs) {
      field_name_ = rhs.field_name_;
      condition_value_ = rhs.condition_value_;
      condition_value_size_ = rhs.condition_value_size_;
      op_ = rhs.op_;

      return *this;
    }

    /** Move-assignment operator. */
    Clause& operator=(Clause&& rhs) {
      field_name_ = std::move(rhs.field_name_);
      condition_value_ = rhs.condition_value_;
      condition_value_size_ = rhs.condition_value_size_;
      op_ = rhs.op_;

      return *this;
    }

    /** The attribute name. */
    std::string field_name_;

    /** The value to compare against. */
    const void* condition_value_;

    /** The byte size of `condition_value_`. */
    uint64_t condition_value_size_;

    /** The comparison operator. */
    QueryConditionOp op_;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * All clauses in this condition. Clauses `clauses_[i]` and
   * `clauses_[i + 1]` are combined by the `combination_ops_[i]`
   * operator.
   */
  std::vector<Clause> clauses_;

  /** Logical operators to combine clauses stored in `clauses_`. */
  std::vector<QueryConditionCombinationOp> combination_ops_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Compares a value against an individual conditional clause.
   *
   * @tparam T the primtive cell type.
   * @param cell_value The cell value, assumed to be of size T.
   * @param clause The clause to compare against.
   * @return true if the cell should be extracted.
   *   Otherwise, this mutates to false to indicate that the cell
   *   should be filtered out from the results.
   */
  template <class T>
  bool cmp_clause(void* cell_value, const Clause& clause) const;

  /**
   * Compares a string value against an individual conditional clause.
   *
   * @tparam T the primtive cell type.
   * @param cell_value The string value, assumed to be of size T.
   * @param clause The clause to compare against.
   * @return true if the cell should be extracted.
   *   Otherwise, this mutates to false to indicate that the cell
   *   should be filtered out from the results.
   */
  bool cmp_clause_str(
      void* cell_value, uint64_t cell_value_size, const Clause& clause) const;

  /**
   * Compares a cell against an individual conditional clause.
   *
   * @tparam T the primtive cell type.
   * @param result_tile The tile containing the cell value.
   * @param cell_idx The index of the cell within `result_tile`.
   * @param clause The clause to compare against.
   * @param cmp Mutates to true if the cell should be extracted.
   *   Otherwise, this mutates to false to indicate that the cell
   *   should be filtered out from the results.
   */
  template <class T>
  Status cmp_clause(
      ResultTile* result_tile,
      uint64_t cell_idx,
      const Clause& clause,
      bool* cmp) const;

  /**
   * Compares a string cell against an individual conditional clause.
   *
   * @param result_tile The tile containing the cell value.
   * @param cell_idx The index of the cell within `result_tile`.
   * @param clause The clause to compare against.
   * @param cmp Mutates to true if the cell should be extracted.
   *   Otherwise, this mutates to false to indicate that the cell
   *   should be filtered out from the results.
   */
  Status cmp_clause_str(
      ResultTile* result_tile,
      uint64_t cell_idx,
      const Clause& clause,
      bool* cmp) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_CONDITION_H
