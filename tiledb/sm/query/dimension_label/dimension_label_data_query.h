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
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;

namespace tiledb::sm {

class DimensionLabel;
class Query;
class QueryBuffer;
class Subarray;
class IndexData;

/**
 * Return a Status_DimensionQueryError error class Status with a given
 * message.
 *
 * Note: This intentionally returns the error as TileDB::DimensionLabelQuery.
 **/
inline Status Status_DimensionLabelDataQueryError(const std::string& msg) {
  return {"[TileDB::DimensionLabelQuery] Error", msg};
}

class DimensionLabelDataQuery {
 public:
  /** Destructor. */
  virtual ~DimensionLabelDataQuery() = default;

  /**
   * Adds ranges to a query initialize with label ranges.
   *
   * @param is_point_ranges If ``true`` the data contains point ranges.
   *     Otherwise, it contains standard ranges.
   * @param start Pointer to the start of the range array.
   * @param count Number of total elements in the range array.
   */
  virtual void add_index_ranges_from_label(
      const bool is_point_range, const void* start, const uint64_t count) = 0;

  /** Returns ``true`` if the query status for both queries is completed. */
  virtual bool completed() const = 0;

  /** Processes both queries if they exist. */
  virtual void process() = 0;
};

/** Dimension label query for reading label data. */
class DimensionLabelReadDataQuery : public DimensionLabelDataQuery {
 public:
  /** Default constructor is not C.41 compliant. */
  DimensionLabelReadDataQuery() = delete;

  /**
   * Constructor.
   *
   * @param storage_manager Storage manager object.
   * @param dimension_label Opened dimension label for the query.
   * @param parent_subarrray Subarray of the parent array.
   * @param label_buffer Query buffer for the label data.
   * @param dim_idx Index of the dimension on the parent array this dimension
   */
  DimensionLabelReadDataQuery(
      StorageManager* storage_manager,
      DimensionLabel* dimension_label,
      const Subarray& parent_subarray,
      const QueryBuffer& label_buffer,
      const uint32_t dim_idx);

  /** Disable copy and move. */
  DISABLE_COPY_AND_COPY_ASSIGN(DimensionLabelReadDataQuery);
  DISABLE_MOVE_AND_MOVE_ASSIGN(DimensionLabelReadDataQuery);

  /**
   * Adds ranges to a query initialize with label ranges.
   *
   * @param is_point_ranges If ``true`` the data contains point ranges.
   *     Otherwise, it contains standard ranges.
   * @param start Pointer to the start of the range array.
   * @param count Number of total elements in the range array.
   */
  void add_index_ranges_from_label(
      const bool is_point_range,
      const void* start,
      const uint64_t count) override;

  /** Returns ``true`` if the query status for both queries is completed. */
  bool completed() const override;

  /** Processes both queries if they exist. */
  void process() override;

 private:
  /** Query on the dimension label indexed array. */
  tdb_unique_ptr<Query> query_{nullptr};
};

/** Dimension label query for writing ordered data. */
class OrderedWriteDataQuery : public DimensionLabelDataQuery {
 public:
  /** Default constructor is not C.41 compliant. */
  OrderedWriteDataQuery() = delete;

  /**
   * Constructor for when index buffer is not set.
   *
   * @param storage_manager Storage manager object.
   * @param dimension_label Opened dimension label for the query.
   * @param parent_subarrray Subarray of the parent array.
   * @param label_buffer Query buffer for the label data.
   * @param index_buffer Query buffer for the index data. May be empty if no
   *     index buffer is set.
   * @param dim_idx Index of the dimension on the parent array this dimension
   *     label is for.
   * @param fragment_name Name to use when writing the fragment.
   */
  OrderedWriteDataQuery(
      StorageManager* storage_manager,
      stats::Stats* stats,
      DimensionLabel* dimension_label,
      const Subarray& parent_subarray,
      const QueryBuffer& label_buffer,
      const QueryBuffer& index_buffer,
      const uint32_t dim_idx,
      optional<std::string> fragment_name);

  /** Disable copy and move. */
  DISABLE_COPY_AND_COPY_ASSIGN(OrderedWriteDataQuery);
  DISABLE_MOVE_AND_MOVE_ASSIGN(OrderedWriteDataQuery);

  /**
   * Adds ranges to a query initialize with label ranges. Not valid on write
   * query.
   */
  void add_index_ranges_from_label(
      const bool is_point_range,
      const void* start,
      const uint64_t count) override;

  /** Returns ``true`` if the query status for both queries is completed. */
  bool completed() const override;

  /** Processes both queries if they exist. */
  void process() override;

 private:
  /** Class stats object for timing. */
  stats::Stats* stats_;

  /** Query on the dimension label indexed array. */
  tdb_unique_ptr<Query> query_;
};

/** Writer for unordered dimension labels. */
class UnorderedWriteDataQuery : public DimensionLabelDataQuery {
 public:
  /** Default constructor is not C.41 compliant. */
  UnorderedWriteDataQuery() = delete;

  /**
   * Constructor.
   *
   * @param storage_manager Storage manager object.
   * @param dimension_label Opened dimension label for the query.
   * @param parent_subarrray Subarray of the parent array.
   * @param label_buffer Query buffer for the label data.
   * @param index_buffer Query buffer for the index data. May be empty if no
   *     index buffer is set.
   * @param dim_idx Index of the dimension on the parent array this dimension
   *     label is for.
   * @param fragment_name Name to use when writing the fragment.
   */
  UnorderedWriteDataQuery(
      StorageManager* storage_manager,
      DimensionLabel* dimension_label,
      const Subarray& parent_subarray,
      const QueryBuffer& label_buffer,
      const QueryBuffer& index_buffer,
      const uint32_t dim_idx,
      optional<std::string> fragment_name);

  /** Disable copy and move. */
  DISABLE_COPY_AND_COPY_ASSIGN(UnorderedWriteDataQuery);
  DISABLE_MOVE_AND_MOVE_ASSIGN(UnorderedWriteDataQuery);

  /**
   * Adds ranges to a query initialize with label ranges. Not valid on write
   * query.
   */
  void add_index_ranges_from_label(
      const bool is_point_range,
      const void* start,
      const uint64_t count) override;

  /** Returns ``true`` if the query status for both queries is completed. */
  bool completed() const override;

  /** Processes both queries if they exist. */
  void process() override;

 private:
  /** Query on the dimension label indexed array. */
  tdb_unique_ptr<Query> indexed_array_query_{nullptr};

  /** Query on the dimension label labelled array. */
  tdb_unique_ptr<Query> labelled_array_query_{nullptr};

  /** Internally managed index data for sparse write to labelled array. */
  tdb_unique_ptr<IndexData> index_data_;
};

}  // namespace tiledb::sm

#endif
