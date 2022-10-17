/**
 * @file dimension_label_range_query.h
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
 * Classes for creating queries to read ranges.
 */

#ifndef TILEDB_DIMENSION_LABEL_RANGE_QUERY_H
#define TILEDB_DIMENSION_LABEL_RANGE_QUERY_H

#include "tiledb/common/common.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;

namespace tiledb::sm {

class DimensionLabel;
class IndexData;
class QueryBuffer;
class Subarray;

/**
 * Class for locally generated status exceptions.
 *
 * Note: This intentionally returns the error as TileDB::DimensionLabelQuery.
 */
class DimensionLabelRangeQueryStatusException : public StatusException {
 public:
  explicit DimensionLabelRangeQueryStatusException(const std::string& msg)
      : StatusException("DimensionLabelQuery", msg) {
  }
};

class DimensionLabelRangeQuery {
 public:
  /** Destructor. */
  virtual ~DimensionLabelRangeQuery() = default;

  /**
   * Retrieves the computed ranges from the query.
   *
   * @returns [is_point_range, range_data, count]
   *   * is_point_range: If ``true`` the returned data is stored as point
   * ranges, otherwise it stored as [start, end] range pairs.
   *   * range_data: Pointer to the start of the range data.
   *   * count: Total number of points stored in the range data.
   */
  virtual tuple<bool, const void*, uint64_t> computed_ranges() const = 0;

  /** Returns ``true`` if the query status is completed. */
  virtual bool completed() const = 0;

  /** Processes the dimension label query. */
  virtual void process() = 0;
};

class OrderedRangeQuery : public DimensionLabelRangeQuery {
 public:
  /** Default constructor is not C.41 compliant. */
  OrderedRangeQuery() = delete;

  /**
   * Contructor.
   *
   * @param storage_manager Storage manager object.
   * @param dimension_label Opened dimension label for the query.
   * @param label_ranges Label ranges to read index ranges from.
   */
  OrderedRangeQuery(
      StorageManager* storage_manager,
      DimensionLabel* dimension_label,
      const std::vector<Range>& label_ranges);

  /** Disable copy and move. */
  DISABLE_COPY_AND_COPY_ASSIGN(OrderedRangeQuery);
  DISABLE_MOVE_AND_MOVE_ASSIGN(OrderedRangeQuery);

  /**
   * Returns the computed index ranges.
   *
   * @returns [is_point_ranges, start, count]
   *  * is_point_ranges: If ``true`` the data contains point ranges.
   *  * start: Pointer to the start of the range data.
   *  * count: Number of total elements in the range data.
   */
  tuple<bool, const void*, uint64_t> computed_ranges() const;

  /** Returns ``true`` if the query status is completed. */
  bool completed() const;

  /** Processes the dimension label query. */
  void process();

 private:
  /** Query on the dimension label. */
  tdb_unique_ptr<Query> query_;

  /** Data to store index data in. */
  tdb_unique_ptr<IndexData> index_data_;
};

}  // namespace tiledb::sm

#endif
