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

class DimensionLabelRangeQuery : public virtual Query {
 public:
  /** Destructor. */
  virtual ~DimensionLabelRangeQuery() = default;

  /** Returns ``true`` if the query status is completed. */
  virtual bool completed() const = 0;

  /** Returns the name of the dimension label. */
  inline const std::string& name() const {
    return name_;
  }

  /** Returns the internally stored index data. */
  virtual IndexData* index_data() = 0;

 private:
  /** Name of the dimension label. */
  std::string name_;
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

  /** Returns ``true`` if the query status is completed. */
  bool completed() const override;

  /** TODO Docs */
  IndexData* index_data() override;

 private:
  /** Data to store index data in. */
  tdb_unique_ptr<IndexData> index_data_;
};

}  // namespace tiledb::sm

#endif
