/**
 * @file dimension_label_query.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Classes for querying (reading/writing) a dimension label using the index
 * dimension for setting the subarray.
 */

#ifndef TILEDB_DIMENSION_LABEL_QUERY_H
#define TILEDB_DIMENSION_LABEL_QUERY_H

#include "tiledb/common/common.h"
#include "tiledb/sm/query/dimension_label/index_data.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;

namespace tiledb::sm {

class DimensionLabel;
class QueryBuffer;
class Subarray;

/**
 * Class for locally generated status exceptions.
 *
 * Note: This intentionally returns the error as TileDB::DimensionLabelQuery.
 */
class DimensionLabelDataQueryStatusException : public StatusException {
 public:
  explicit DimensionLabelDataQueryStatusException(const std::string& msg)
      : StatusException("DimensionLabelQuery", msg) {
  }
};

/** Dimension label query for writing ordered data. */
class DimensionLabelQuery : public Query {
 public:
  /** Default constructor is not C.41 compliant. */
  DimensionLabelQuery() = delete;

  /**
   * Constructor for queries to read or write label data.
   *
   * @param storage_manager Storage manager object.
   * @param name Name of the dimension label.
   * @param dimension_label Opened dimension label for the query.
   * @param parent_subarrray Subarray of the parent array.
   * @param label_buffer Query buffer for the label data.
   * @param index_buffer Query buffer for the index data. May be empty if no
   *     index buffer is set.
   * @param dim_idx Index of the dimension on the parent array this dimension
   *     label is for.
   * @param fragment_name Name to use when writing the fragment.
   */
  DimensionLabelQuery(
      StorageManager* storage_manager,
      stats::Stats* stats,
      const std::string& name,
      DimensionLabel* dimension_label,
      const Subarray& parent_subarray,
      const QueryBuffer& label_buffer,
      const QueryBuffer& index_buffer,
      const uint32_t dim_idx,
      optional<std::string> fragment_name);

  /** Disable copy and move. */
  DISABLE_COPY_AND_COPY_ASSIGN(DimensionLabelQuery);
  DISABLE_MOVE_AND_MOVE_ASSIGN(DimensionLabelQuery);

  /** Returns ``true`` if the query status is completed. */
  bool completed() const;

  /** Returns the name of the dimension label. */
  inline const std::string& name() const {
    return name_;
  }

 private:
  /**
   * Internally managed index data.
   *
   * Note: May be null if the index data is set and managed by the user.
   */
  tdb_unique_ptr<IndexData> index_data_;

  /** The name of the dimension label. */
  std::string name_;

  /**
   * Initialize query for reading label data.
   *
   * @param dimension_label Opened dimension label for the query.
   * @param parent_subarrray Subarray of the parent array.
   * @param label_buffer Query buffer for the label data.
   * @param dim_idx Index of the dimension on the parent array this dimension
   */
  void initialize_read_labels_query(
      DimensionLabel* dimension_label,
      const Subarray& parent_subarray,
      const QueryBuffer& label_buffer,
      const uint32_t dim_idx);

  /** TODO: Docs */
  void initialize_ordered_write_query(
      stats::Stats* stats,
      DimensionLabel* dimension_label,
      const Subarray& parent_subarray,
      const QueryBuffer& label_buffer,
      const QueryBuffer& index_buffer,
      const uint32_t dim_idx);

  /** TODO: Docs */
  void initialize_unordered_write_query(
      DimensionLabel* dimension_label,
      const Subarray& parent_subarray,
      const QueryBuffer& label_buffer,
      const QueryBuffer& index_buffer,
      const uint32_t dim_idx);
};
}  // namespace tiledb::sm

#endif
