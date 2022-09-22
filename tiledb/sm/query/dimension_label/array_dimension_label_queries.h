/**
 * @file array_dimension_label_queries.h
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
 * Class for managing all dimension label queries in an array query.
 */

#ifndef TILEDB_ARRAY_DIMENSION_LABEL_QUERIES_H
#define TILEDB_ARRAY_DIMENSION_LABEL_QUERIES_H

#include "tiledb/common/common.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/query/dimension_label/dimension_label_query.h"

#include <unordered_map>

using namespace tiledb::common;

namespace tiledb::sm {

class Array;
class DimensionLabelReference;
class Query;
class QueryBuffer;
class StorageManager;
class Subarray;

enum class QueryType : uint8_t;

/**
 * Return a Status_DimensionQueryError error class Status with a given
 * message.
 *
 * Note: Returns error as a QueryError.
 ***/
inline Status Status_DimensionLabelQueryError(const std::string& msg) {
  return {"[TileDB::Query] Error", msg};
}

class ArrayDimensionLabelQueries {
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

  /** Constructor. */
  ArrayDimensionLabelQueries(
      StorageManager* storage_manager,
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

  /** Cancels all dimension label queries. */
  void cancel();

  /** Finalizes all dimension label queries. */
  void finalize();

  /** Returns ``true`` if there are any data queries. */
  inline bool has_data_query() const {
    return !data_queries_.empty();
  }

  /** Returns ``true`` if there are any range queries. */
  inline bool has_range_query() const {
    return !range_queries_map_.empty();
  }

  /**
   * Returns ``true`` if there is a range query on the requested dimension.
   *
   * @param dim_idx Index to check for a range query on.
   */
  inline bool has_range_query(dimension_size_type dim_idx) const {
    return range_queries_[dim_idx] != nullptr;
  }

  /** Process all data queries. */
  void process_data_queries();

  /**
   * Process all data queries and updates the ranges on the parent subarray.
   *
   * @param subarray The subarray to set the updated dimension ranges on.
   */
  void process_range_queries(Subarray& subarray);

 private:
  /** The storage manager. */
  StorageManager* storage_manager_;

  /** Map from label name to dimension label opened by this query. */
  std::unordered_map<std::string, tdb_unique_ptr<DimensionLabel>>
      dimension_labels_;

  /** Map from label name to dimension label range query. */
  std::unordered_map<std::string, tdb_unique_ptr<DimensionLabelQuery>>
      range_queries_map_;

  /**
   * Non-owning vector for accesing query by dimension index.
   *
   * Note: This vector is always sized to the number of dimensions in the array.
   * There can be at most on query per dimension. If there is no query on the
   * dimension, it contains a nullpointer.
   */
  std::vector<DimensionLabelQuery*> range_queries_;

  /**
   * Dimension label data queries.
   *
   * Note: For the data queries, the element order to the queries is
   * unimportant and does not correspond to dimension index or any other value.
   */
  std::vector<tdb_unique_ptr<DimensionLabelQuery>> data_queries_;

  /** Non-owning map from label name to dimension label data query. */
  std::unordered_map<std::string, DimensionLabelQuery*> data_queries_map_;

  /** The status of the range queries. */
  QueryStatus range_query_status_;

  optional<std::string> fragment_name_;

  /**
   * Initializes all queries for reading label data.
   *
   * @param array The array for the parent query.
   * @param subarray The subarray for the parent query.
   * @param label_buffers A map of query buffers with label buffers.
   */
  void add_data_queries_for_read(
      Array* array,
      const Subarray& subarray,
      const std::unordered_map<std::string, QueryBuffer>& label_buffers);

  /**
   * Initializes all queries for writing label data.
   *
   * @param array The array for the parent query.
   * @param subarray The subarray for the parent query.
   * @param label_buffers A map of query buffers with label buffers.
   * @param array_buffers Non-label buffers set on the parent query.
   */
  void add_data_queries_for_write(
      Array* array,
      const Subarray& subarray,
      const std::unordered_map<std::string, QueryBuffer>& label_buffers,
      const std::unordered_map<std::string, QueryBuffer>& array_buffers);

  /**
   * Initializes all queries for reading dimension ranges.
   *
   *
   * @param array The array for the parent query.
   * @param subarray The subarray for the parent query.
   * @param label_buffers A map of query buffers with label buffers.
   * @param array_buffers Non-label buffers set on the parent query.
   */
  void add_range_queries(
      Array* array,
      const Subarray& subarray,
      const std::unordered_map<std::string, QueryBuffer>& label_buffers,
      const std::unordered_map<std::string, QueryBuffer>& array_buffers);

  /**
   * Opens a dimension label.
   *
   * @param array The array the dimension label is defined on.
   * @param query_type The query type to open the dimension label as.
   * @param open_indexed_array If ``true``, open the indexed array.
   * @param open_labelled_array If ``true``, open the labelled array.
   */
  DimensionLabel* open_dimension_label(
      Array* array,
      const DimensionLabelReference& dim_label_ref,
      const QueryType& query_type,
      const bool open_indexed_array,
      const bool open_labelled_array);
};

}  // namespace tiledb::sm

#endif
