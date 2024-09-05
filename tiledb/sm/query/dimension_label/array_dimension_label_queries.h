/*
 * @file array_dimension_label_queries.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * Class for managing all dimension label queries in an array query.
 */

#ifndef TILEDB_ARRAY_DIMENSION_LABEL_QUERIES_H
#define TILEDB_ARRAY_DIMENSION_LABEL_QUERIES_H

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/query/dimension_label/dimension_label_query.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <unordered_map>

using namespace tiledb::common;

namespace tiledb::sm {

class Array;
class DimensionLabel;
class Query;
class QueryBuffer;
class Subarray;

enum class QueryType : uint8_t;

class ArrayDimensionLabelQueries : public JobBranch {
 public:
  /**
   * Size type for the number of dimensions of an array and for dimension
   * indices.
   *
   * Note: This should be the same as `Domain::dimension_size_type`. We're
   * not including `domain.h`, otherwise we'd use that definition here.
   */
  using dimension_size_type = unsigned int;

  /** Default constructor is not C.41 compliant. */
  ArrayDimensionLabelQueries() = delete;

  /**
   * Virtual destructor is override from `JobBranch`.
   */
  ~ArrayDimensionLabelQueries() override = default;

  /**
   * Constructor.
   *
   * @param parent The parent of this query as a job
   * @param array Parent array the dimension labels are defined on.
   * @param subarray Subarray for the query on the parent array.
   * @param label_buffers A map of query buffers containing label data.
   * @param array_buffers A map of query buffers containing dimension and
   *     attribute data for the parent array.
   * @param fragment_name Optional fragment name for writing fragments.
   */
  ArrayDimensionLabelQueries(
      JobParent& parent,
      Array* array,
      const Subarray& subarray,
      const std::unordered_map<std::string, QueryBuffer>& label_buffers,
      const std::unordered_map<std::string, QueryBuffer>& array_buffers,
      const optional<std::string>& fragment_name);

  /** Disable copy and move. */
  DISABLE_COPY_AND_COPY_ASSIGN(ArrayDimensionLabelQueries);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ArrayDimensionLabelQueries);

  /** Returns ``true`` if all queries are completed. */
  bool completed() const;

  /** Returns ``true`` if the range queries are completed. */
  inline bool completed_range_queries() const {
    return range_query_status_ == QueryStatus::COMPLETED;
  }

  /** Returns ``true`` if there are any data queries. */
  inline bool has_data_query() const {
    return !data_queries_.empty();
  }

  /** Returns ``true`` if there are any range queries. */
  inline bool has_range_query() const {
    return !range_queries_.empty();
  }

  /**
   * Returns a label range query by dimension index.
   * Throws if there is no label range query on dim_idx.
   *
   * @param dim_idx Dimension index for label range query.
   * @returns Pointer to DimensionLabelQuery on dim_idx
   */
  DimensionLabelQuery* get_range_query(dimension_size_type dim_idx) const;

  /**
   * Returns ``true`` if there is a range query on the requested dimension.
   *
   * @param dim_idx Index to check for a range query on.
   * @returns ``true`` if there is a range query on the requested dimension and
   *     ``false`` otherwise.
   */
  inline bool has_range_query(dimension_size_type dim_idx) const {
    return label_range_queries_by_dim_idx_[dim_idx] != nullptr;
  }

  /**
   * Returns a label data query by dimension index.
   * Throws if there is no label data query on dim_idx.
   *
   * @param dim_idx Dimension index for label data query.
   * @returns Vector of DimensionLabelQuery on dim_idx
   */
  std::vector<DimensionLabelQuery*> get_data_query(
      dimension_size_type dim_idx) const;

  /**
   * Returns ``true`` if there is a data query on the requested dimension.
   *
   * @param dim_idx Index to check for a data query on.
   * @returns ``true`` if there is a data query on the requested dimension and
   *     ``false`` otherwise.
   */
  inline bool has_data_query(dimension_size_type dim_idx) const {
    return !label_data_queries_by_dim_idx_[dim_idx].empty();
  }

  /** Process all data queries. */
  void process_data_queries();

  /**
   * Process all data queries and updates the ranges on the parent subarray.
   *
   * @param parent_query The parent array query to update ranges on.
   */
  void process_range_queries(Query* parent_query);

 private:
  /** The context resources. */
  ContextResources& resources_;

  /** Map from label name to dimension label opened by this query. */
  std::unordered_map<std::string, shared_ptr<Array>> dimension_labels_;

  /** Dimension label range queries */
  std::vector<tdb_unique_ptr<DimensionLabelQuery>> range_queries_;

  /**
   * Non-owning vector for accessing range query by dimension index.
   *
   * Note: This vector is always sized to the number of dimensions in the array.
   * There can be at most one query per dimension. If there is no query on the
   * dimension, it contains a null pointer.
   */
  std::vector<DimensionLabelQuery*> label_range_queries_by_dim_idx_;

  /**
   * Dimension label data queries.
   *
   * Note: For the data queries, the element order to the queries is
   * unimportant and does not correspond to dimension index or any other value.
   */
  std::vector<tdb_unique_ptr<DimensionLabelQuery>> data_queries_;

  /**
   * Non-owning vector for accessing data query by dimension index.
   *
   * Note: The outer vector is always sized to the number of dimensions in the
   * array. There can be multiple queries on a dimension. If there is no query
   * on the dimension, the inner vector will be empty.
   */
  std::vector<std::vector<DimensionLabelQuery*>> label_data_queries_by_dim_idx_;

  /** The status of the range queries. */
  QueryStatus range_query_status_;

  /**
   * The name of the new fragment to be created for writes.
   *
   * If not set, the fragment will be created using the latest array timestamp
   * and a generated UUID.
   */
  optional<std::string> fragment_name_;

  /**
   * Initializes read queries.
   *
   * @param array Array for the parent query.
   * @param subarray Subarray for the parent query.
   * @param label_buffers A map of query buffers with label buffers.
   * @param array_buffers Non-label buffers set on the parent query.
   */
  void add_read_queries(
      Array* array,
      const Subarray& subarray,
      const std::unordered_map<std::string, QueryBuffer>& label_buffers,
      const std::unordered_map<std::string, QueryBuffer>& array_buffers);

  /**
   * Initializes write queries.
   *
   * @param array Array for the parent query.
   * @param subarray Subarray for the parent query.
   * @param label_buffers A map of query buffers with label buffers.
   * @param array_buffers Non-label buffers set on the parent query.
   */
  void add_write_queries(
      Array* array,
      const Subarray& subarray,
      const std::unordered_map<std::string, QueryBuffer>& label_buffers,
      const std::unordered_map<std::string, QueryBuffer>& array_buffers);

  /**
   * Opens a dimension label.
   *
   * @param array Array the dimension label is defined on.
   * @param dim_label_uri URI the dimension label is stored at.
   * @param dim_label_name Name of the dimension label.
   * @param query_type Query type to open the dimension label as.
   */
  shared_ptr<Array> open_dimension_label(
      Array* array,
      const URI& dim_label_uri,
      const std::string& dim_label_name,
      const QueryType& query_type);

  /**
   * Derived from `JobBranch`
   */
  ContextResources& resources() const override {
    return resources_;
  }
};

}  // namespace tiledb::sm

#endif
