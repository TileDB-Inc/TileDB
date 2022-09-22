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
 * Classes for query a dimension label.
 */

#ifndef TILEDB_DIMENSION_LABEL_QUERY_H
#define TILEDB_DIMENSION_LABEL_QUERY_H

#include "tiledb/common/common.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;

namespace tiledb::sm {

class DimensionLabel;
class Query;
class QueryBuffer;
class StorageManager;
class Subarray;

class DimensionLabelQuery {
 public:
  /** Default constructor. */
  DimensionLabelQuery() = default;

  /**
   * General constructor.
   *
   * @param storage_manager Storage manager object.
   * @param dimension_label Opened dimension label for the query.
   * @param add_indexed_query If ``true``, create a query on the indexed array.
   * @param add_labelled_query If ``true``, create a query on the labelled
   *     array.
   * @param fragment_name Optional fragment name for writing fragments.
   */
  DimensionLabelQuery(
      StorageManager* storage_manager,
      DimensionLabel* dimension_label,
      bool add_indexed_query,
      bool add_labelled_query,
      optional<std::string> fragment_name = nullopt);

  /** Destructor. */
  virtual ~DimensionLabelQuery() = default;

  /** Disable copy and move. */
  DISABLE_COPY_AND_COPY_ASSIGN(DimensionLabelQuery);
  DISABLE_MOVE_AND_MOVE_ASSIGN(DimensionLabelQuery);

  /** Cancel both queries if they exist. */
  void cancel();

  /** Returns ``true`` if the query status for both queries is completed. */
  bool completed() const;

  /** Finalizes both queries if they exist. */
  void finalize();

  /** Processes both queries if they exist. */
  void process();

 protected:
  /** Query on the dimension label indexed array. */
  tdb_unique_ptr<Query> indexed_array_query{nullptr};

  /** Query on the dimension label labelled array. */
  tdb_unique_ptr<Query> labelled_array_query{nullptr};
};

/** Dimension label query for reading label data. */
class DimensionLabelReadDataQuery : public DimensionLabelQuery {
 public:
  DimensionLabelReadDataQuery() = delete;

  DimensionLabelReadDataQuery(
      StorageManager* storage_manager,
      DimensionLabel* dimension_label,
      const Subarray& parent_subarray,
      const QueryBuffer& label_buffer,
      const uint32_t dim_idx);
};

/** Base class for internally managed index data. */
class IndexData {
 public:
  virtual ~IndexData() = default;
  virtual void* data() = 0;
  virtual uint64_t* data_size() = 0;
};

/** Typed class for internally managed index data. */
template <
    typename T,
    typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
class TypedIndexData : public IndexData {
 public:
  /**
   * Constructor.
   *
   * This will create a vector of index values starting at the range lower bound
   * and continuing to the range upper bound.
   *
   * @param range The range to create index data for.
   */
  TypedIndexData(const type::Range& range)
      : data_{}
      , data_size_{0} {
    auto range_data = static_cast<const T*>(range.data());
    T min_value = range_data[0];
    T max_value = range_data[1];
    if (max_value < min_value) {
      throw std::invalid_argument(
          "Invalid range - cannot have lower bound greater than the upper "
          "bound.");
    }
    data_.reserve(max_value - min_value + 1);
    for (T val = min_value; val <= max_value; ++val) {
      data_.push_back(val);
    }
    data_size_ = sizeof(T) * data_.size();
  }

  /** Pointer access to index data. */
  void* data() override {
    return data_.data();
  }

  /** Pointer access to data size. */
  uint64_t* data_size() override {
    return &data_size_;
  }

 private:
  /** Vector of index data. */
  std::vector<T> data_;

  /** Size of the index data. */
  uint64_t data_size_;
};

/** Dimension label query for writing ordered data. */
class OrderedWriteDataQuery : public DimensionLabelQuery {
 public:
  /** Default constructor is not C.41 compliant. */
  OrderedWriteDataQuery() = delete;

  /**
   * Constructor for when index buffer is set.
   *
   * @param storage_manager Storage manager object.
   * @param dimension_label Opened dimension label for the query.
   * @param index_buffer Query buffer for the index data.
   * @param label_buffer Query buffer for the label data.
   * @param fragment_name Name to use when writing the fragment.
   */
  OrderedWriteDataQuery(
      StorageManager* storage_manager,
      DimensionLabel* dimension_label,
      const QueryBuffer& index_buffer,
      const QueryBuffer& label_buffer,
      optional<std::string> fragment_name);

  /**
   * Constructor for when index buffer is not set.
   *
   * @param storage_manager Storage manager object.
   * @param dimension_label Opened dimension label for the query.
   * @param parent_subarrray Subarray of the parent array.
   * @param label_buffer Query buffer for the label data.
   * @param dim_idx Index of the dimension on the parent array this dimension
   *     label is for.
   * @param fragment_name Name to use when writing the fragment.
   */
  OrderedWriteDataQuery(
      StorageManager* storage_manager,
      DimensionLabel* dimension_label,
      const Subarray& parent_subarray,
      const QueryBuffer& label_buffer,
      const uint32_t dim_idx,
      optional<std::string> fragment_name);

 private:
  tdb_unique_ptr<IndexData> index_data_;
};

/** Writer for unordered dimension labels. */
class UnorderedWriteDataQuery : public DimensionLabelQuery {
 public:
  /** Default constructor is not C.41. */
  UnorderedWriteDataQuery() = delete;

  /**
   * Constructor.
   *
   * @param storage_manager Storage manager object.
   * @param dimension_label Opened dimension label for the query.
   * @param index_buffer Query buffer for the index data.
   * @param label_buffer Query buffer for the label data.
   * @param fragment_name Name to use when writing the fragment.
   */
  UnorderedWriteDataQuery(
      StorageManager* storage_manager,
      DimensionLabel* dimension_label,
      const QueryBuffer& index_buffer,
      const QueryBuffer& label_buffer,
      optional<std::string> fragment_name);
};

}  // namespace tiledb::sm

#endif
