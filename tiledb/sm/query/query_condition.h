/**
 * @file query_condition.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB, Inc.
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

#include "external/include/span/span.hpp"

#include <unordered_set>

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/query/ast/query_ast.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class FragmentMetadata;
struct ResultCellSlab;
class ResultTile;

enum class QueryConditionCombinationOp : uint8_t;

class QueryCondition {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  QueryCondition();

  /** Constructor from a marker. */
  QueryCondition(const std::string& condition_marker);

  /** Constructor from a tree. */
  QueryCondition(tdb_unique_ptr<tiledb::sm::ASTNode>&& tree);

  /** Constructor from a tree and marker. */
  QueryCondition(
      const uint64_t condition_index,
      const std::string& condition_marker,
      tdb_unique_ptr<tiledb::sm::ASTNode>&& tree);

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
      const QueryConditionOp& op);

  /**
   * Translate any query conditions against enumerated attributes to the
   * underlying attribute type.
   *
   * @param array_schema The current array schema with all required enumerations
   * loaded.
   */
  void rewrite_enumeration_conditions(const ArraySchema& array_schema);

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
   * @brief API call to negate query condition.
   *
   * @param combined_cond Where the negated condition is stored.
   * @return Status
   */
  Status negate(
      QueryConditionCombinationOp combination_op,
      QueryCondition* combined_cond) const;

  /**
   * Returns true if this condition does not have any nodes in the AST
   * representing the query condition.
   */
  bool empty() const;

  /**
   * Returns a set of all unique field names among the value nodes in the AST
   * representing the query condition.
   */
  std::unordered_set<std::string>& field_names() const;

  /**
   * Returns a set of all unique field names that reference an enumerated
   * attribute condition in the AST representing the query condition.
   */
  std::unordered_set<std::string>& enumeration_field_names() const;

  /**
   * Returns the timestamp for this condition.
   */
  uint64_t condition_timestamp() const;

  /**
   * Applies this query condition to `result_cell_slabs`.
   *
   * @param array_schema The array schema associated with `result_cell_slabs`.
   * @param fragment_metadata The fragment metadata.
   * @param result_cell_slabs The cell slabs to filter. Mutated to remove cell
   *   slabs that do not meet the criteria in this query condition.
   * @param stride The stride between cells.
   * @return Status
   */
  Status apply(
      const ArraySchema& array_schema,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
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
   * @param cell_slab_coords The cell slab coordinates.
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
      const void* cell_slab_coords,
      uint8_t* result_buffer);

  /**
   * Applies this query condition to a set of cells.
   *
   * @param array_schema The array schema.
   * @param result_tile The result tile to get the cells from.
   * @param result_bitmap The bitmap to use for results.
   * @return Status
   */
  template <typename BitmapType>
  Status apply_sparse(
      const ArraySchema& array_schema,
      ResultTile& result_tile,
      std::vector<BitmapType>& result_bitmap);

  /**
   * Reverse the query condition using De Morgan's law.
   */
  QueryCondition negated_condition();

  /**
   * Sets the AST. This is internal state to only be used in
   * the serialization path.
   */
  void set_ast(tdb_unique_ptr<ASTNode>&& ast);

  /**
   * Returns the AST object. This is internal state to only be used in testing
   * and the serialization path.
   */
  const tdb_unique_ptr<ASTNode>& ast() const;

  /**
   * Returns the condition marker.
   */
  const std::string& condition_marker() const;

  /**
   * Returns the condition index.
   */
  uint64_t condition_index() const;

  /**
   * By default, a query condition is applied against the enumerated values
   * of an attribute. Setting use_enumeration to false prevents the translation
   * and applies this query condition directly against the underlying integral
   * attribute data.
   *
   * @param use_enumeration A bool indicating whether to use the enumeration
   *        values.
   */
  void set_use_enumeration(bool use_enumeration);

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

  /**
   * Performs dense condition on dimensions.
   */
  template <
      typename T,
      QueryConditionOp Op,
      typename CombinationOp,
      typename Enable = T>
  struct DenseDimCondition;

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */
  /** Marker used to reference which file the condition came from. */
  std::string condition_marker_;

  /** Index for the condition. */
  size_t condition_index_;

  /** AST Tree structure representing the condition. **/
  tdb_unique_ptr<tiledb::sm::ASTNode> tree_{};

  /** Caches all field names in the value nodes of the AST.  */
  mutable std::unordered_set<std::string> field_names_;

  /** Caches all field names that references enumerations in the AST. */
  mutable std::unordered_set<std::string> enumeration_field_names_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Applies a value node on primitive-typed result cell slabs,
   * templated for a query condition operator.
   *
   * @param node The value node to apply.
   * @param fragment_metadata The fragment metadata.
   * @param stride The stride between cells.
   * @param var_size The attribute is var sized or not.
   * @param nullable The attribute is nullable or not.
   * @param fill_value The fill value for the cells.
   * @param combination_op The combination op.
   * @param result_cell_bitmap The input cell bitmap.
   * @return The filtered cell slabs.
   */
  template <typename T, QueryConditionOp Op, typename CombinationOp>
  void apply_ast_node(
      const tdb_unique_ptr<ASTNode>& node,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
      uint64_t stride,
      const bool var_size,
      const bool nullable,
      const ByteVecValue& fill_value,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      CombinationOp combination_op,
      std::vector<uint8_t>& result_cell_bitmap) const;

  /**
   * Applies a value node on primitive-typed result cell slabs.
   *
   * @param node The value node to apply.
   * @param fragment_metadata The fragment metadata.
   * @param stride The stride between cells.
   * @param var_size The attribute is var sized or not.
   * @param nullable The attribute is nullable or not.
   * @param fill_value The fill value for the cells.
   * @param combination_op The combination op.
   * @param result_cell_bitmap The input cell bitmap.
   * @return Status, filtered cell slabs.
   */
  template <typename T, typename CombinationOp>
  void apply_ast_node(
      const tdb_unique_ptr<ASTNode>& node,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
      uint64_t stride,
      const bool var_size,
      const bool nullable,
      const ByteVecValue& fill_value,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      CombinationOp combination_op,
      std::vector<uint8_t>& result_cell_bitmap) const;

  /**
   * Applies a value node to filter result cells from the input
   * result cell slabs.
   *
   * @param node The value node to apply.
   * @param array_schema The current array schema.
   * @param fragment_metadata The fragment metadata.
   * @param stride The stride between cells.
   * @param combination_op The combination op.
   * @param result_cell_bitmap The input cell bitmap.
   * @return Status, filtered cell slabs.
   */
  template <typename CombinationOp>
  void apply_ast_node(
      const tdb_unique_ptr<ASTNode>& node,
      const ArraySchema& array_schema,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
      uint64_t stride,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      CombinationOp combination_op,
      std::vector<uint8_t>& result_cell_bitmap) const;

  /**
   * Applies the query condition represented with the AST to
   * `result_cell_slabs`.
   *
   * @param node The node to apply.
   * @param array_schema The array schema associated with `result_cell_slabs`.
   * @param fragment_metadata The fragment metadata.
   * @param stride The stride between cells.
   * @param combination_op The combination op.
   * @param result_cell_bitmap A bitmap representation of cell slabs to filter.
   * Mutated to remove cell slabs that do not meet the criteria in this query
   * condition.
   * @return Filtered cell slabs.
   */
  template <typename CombinationOp = std::logical_and<uint8_t>>
  void apply_tree(
      const tdb_unique_ptr<ASTNode>& node,
      const ArraySchema& array_schema,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
      uint64_t stride,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      CombinationOp combination_op,
      std::vector<uint8_t>& result_cell_bitmap) const;

  /**
   * Applies a value node on a dense result tile,
   * templated for a query condition operator.
   *
   * @param node The value node to apply.
   * @param array_schema The array schema.
   * @param result_tile The result tile to get the cells from.
   * @param start The start cell.
   * @param src_cell The cell offset in the source tile.
   * @param stride The stride between cells.
   * @param var_size The attribute is var sized or not.
   * @param nullable The attribute is nullable or not.
   * @param combination_op The combination op.
   * @param cell_slab_coords The cell slab coordinates.
   * @param result_buffer The result buffer.
   */
  template <typename T, QueryConditionOp Op, typename CombinationOp>
  void apply_ast_node_dense(
      const tdb_unique_ptr<ASTNode>& node,
      const ArraySchema& array_schema,
      ResultTile* result_tile,
      const uint64_t start,
      const uint64_t src_cell,
      const uint64_t stride,
      const bool var_size,
      const bool nullable,
      CombinationOp combination_op,
      const void* cell_slab_coords,
      span<uint8_t> result_buffer) const;

  /**
   * Applies a value node on a dense result tile.
   *
   * @param node The node to apply.
   * @param array_schema The array schema.
   * @param result_tile The result tile to get the cells from.
   * @param start The start cell.
   * @param src_cell The cell offset in the source tile.
   * @param stride The stride between cells.
   * @param var_size The attribute is var sized or not.
   * @param nullable The attribute is nullable or not.
   * @param combination_op The combination op.
   * @param cell_slab_coords The cell slab coordinates.
   * @param result_buffer The result buffer.
   * @return Status.
   */
  template <typename T, typename CombinationOp>
  void apply_ast_node_dense(
      const tdb_unique_ptr<ASTNode>& node,
      const ArraySchema& array_schema,
      ResultTile* result_tile,
      const uint64_t start,
      const uint64_t src_cell,
      const uint64_t stride,
      const bool var_size,
      const bool nullable,
      CombinationOp combination_op,
      const void* cell_slab_coords,
      span<uint8_t> result_buffer) const;

  /**
   * Applies a value node to filter result cells from the input
   * result tile.
   *
   * @param node The node to apply.
   * @param array_schema The current array schema.
   * @param result_tile The result tile to get the cells from.
   * @param start The start cell.
   * @param src_cell The cell offset in the source tile.
   * @param stride The stride between cells.
   * @param combination_op The combination op.
   * @param  The cell slab coordinates.
   * @param result_buffer The result buffer.
   * @return Status.
   */
  template <typename CombinationOp>
  void apply_ast_node_dense(
      const tdb_unique_ptr<ASTNode>& node,
      const ArraySchema& array_schema,
      ResultTile* result_tile,
      const uint64_t start,
      const uint64_t src_cell,
      const uint64_t stride,
      CombinationOp combination_op,
      const void* cell_slab_coords,
      span<uint8_t> result_buffer) const;

  /**
   * Applies the query condition represented with the AST to a set of cells.
   *
   * @param node The node to apply.
   * @param array_schema The array schema.
   * @param result_tile The result tile to get the cells from.
   * @param start The start cell.
   * @param src_cell The cell offset in the source tile.
   * @param stride The stride between cells.
   * @param combination_op The combination op.
   * @param cell_slab_coords The cell slab coordinates.
   * @param result_buffer The buffer to use for results.
   * @return Void.
   */
  template <typename CombinationOp = std::logical_and<uint8_t>>
  void apply_tree_dense(
      const tdb_unique_ptr<ASTNode>& node,
      const ArraySchema& array_schema,
      ResultTile* result_tile,
      const uint64_t start,
      const uint64_t src_cell,
      const uint64_t stride,
      CombinationOp combination_op,
      const void* cell_slab_coords,
      span<uint8_t> result_buffer) const;

  /**
   * Applies a value node on a sparse result tile,
   * templated for a query condition operator.
   *
   * @param node The node to apply.
   * @param result_tile The result tile to get the cells from.
   * @param var_size The attribute is var sized or not.
   * @param combination_op The combination op.
   * @param result_bitmap The result bitmap.
   */
  template <
      typename T,
      QueryConditionOp Op,
      typename BitmapType,
      typename CombinationOp,
      typename nullable>
  void apply_ast_node_sparse(
      const tdb_unique_ptr<ASTNode>& node,
      ResultTile& result_tile,
      const bool var_size,
      CombinationOp combination_op,
      std::vector<BitmapType>& result_bitmap) const;

  /**
   * Applies a value node on a sparse result tile,
   * templated on the nullable operator.
   *
   * @param node The node to apply.
   * @param result_tile The result tile to get the cells from.
   * @param var_size The attribute is var sized or not.
   * @param combination_op The combination op.
   * @param result_bitmap The result bitmap.
   * @return Status.
   */
  template <
      typename T,
      typename BitmapType,
      typename CombinationOp,
      typename nullable>
  void apply_ast_node_sparse(
      const tdb_unique_ptr<ASTNode>& node,
      ResultTile& result_tile,
      const bool var_size,
      CombinationOp combination_op,
      std::vector<BitmapType>& result_bitmap) const;

  /**
   * Applies a value node on a sparse result tile.
   *
   * @param node The node to apply.
   * @param result_tile The result tile to get the cells from.
   * @param var_size The attribute is var sized or not.
   * @param nullable The attribute is nullable or not.
   * @param combination_op The combination op.
   * @param result_bitmap The result bitmap.
   */
  template <typename T, typename BitmapType, typename CombinationOp>
  void apply_ast_node_sparse(
      const tdb_unique_ptr<ASTNode>& node,
      ResultTile& result_tile,
      const bool var_size,
      const bool nullable,
      CombinationOp combination_op,
      std::vector<BitmapType>& result_bitmap) const;

  /**
   * Applies a value node to filter result cells from the input
   * result tile.
   *
   * @param node The node to apply.
   * @param array_schema The current array schema.
   * @param result_tile The result tile to get the cells from.
   * @param combination_op The combination op.
   * @param result_bitmap The result bitmap.
   * @return Status.
   */
  template <typename BitmapType, typename CombinationOp>
  void apply_ast_node_sparse(
      const tdb_unique_ptr<ASTNode>& node,
      const ArraySchema& array_schema,
      ResultTile& result_tile,
      CombinationOp combination_op,
      std::vector<BitmapType>& result_bitmap) const;

  /**
   * Applies the query condition represented with the AST to a set of cells.
   *
   * @param node The node to apply.
   * @param array_schema The array schema.
   * @param result_tile The result tile to get the cells from.
   * @param combination_op The combination op.
   * @param result_bitmap The bitmap to use for results.
   * @return Void.
   */
  template <
      typename BitmapType,
      typename CombinationOp = std::logical_and<BitmapType>>
  void apply_tree_sparse(
      const tdb_unique_ptr<ASTNode>& node,
      const ArraySchema& array_schema,
      ResultTile& result_tile,
      CombinationOp combination_op,
      std::vector<BitmapType>& result_bitmap) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_CONDITION_H
