/**
 * @file dimension_label_query.cc
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
 * Classes for querying (reading/writing) a dimension label.
 *
 * Note: The current implementation uses a class that stores two `Query` objects
 * and all operations check if each of the query is initialized or is `nullptr`.
 * This is to support the temporary dual-array dimension label design. Once a
 * reader for the ordered dimension label is implemented and the projections for
 * the unordered dimension label are implemented, each `DimensionLabelQuery`
 * will only contain a single `Query` object that must be constructed on
 * initialization.
 */

#include "tiledb/sm/query/dimension_label/dimension_label_query.h"
#include "tiledb/common/common.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/subarray/subarray.h"

#include <algorithm>
#include <functional>

using namespace tiledb::common;

namespace tiledb::sm {

DimensionLabelQuery::DimensionLabelQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    bool add_indexed_query,
    bool add_labelled_query,
    optional<std::string> fragment_name)
    : indexed_array_query{add_indexed_query ?
                              tdb_unique_ptr<Query>(tdb_new(
                                  Query,
                                  storage_manager,
                                  dimension_label->indexed_array(),
                                  fragment_name)) :
                              nullptr}
    , labelled_array_query{add_labelled_query ?
                               tdb_unique_ptr<Query>(tdb_new(
                                   Query,
                                   storage_manager,
                                   dimension_label->labelled_array(),
                                   fragment_name)) :
                               nullptr} {
}

void DimensionLabelQuery::cancel() {
  if (indexed_array_query) {
    throw_if_not_ok(indexed_array_query->cancel());
  }
  if (labelled_array_query) {
    throw_if_not_ok(labelled_array_query->cancel());
  }
}

bool DimensionLabelQuery::completed() const {
  return (!indexed_array_query ||
          indexed_array_query->status() == QueryStatus::COMPLETED) &&
         (!labelled_array_query ||
          labelled_array_query->status() == QueryStatus::COMPLETED);
}

void DimensionLabelQuery::finalize() {
  if (indexed_array_query) {
    throw_if_not_ok(indexed_array_query->finalize());
  }
  if (labelled_array_query) {
    throw_if_not_ok(labelled_array_query->finalize());
  }
}

void DimensionLabelQuery::process() {
  if (indexed_array_query) {
    throw_if_not_ok(indexed_array_query->init());
    throw_if_not_ok(indexed_array_query->process());
  }
  if (labelled_array_query) {
    throw_if_not_ok(labelled_array_query->init());
    throw_if_not_ok(labelled_array_query->process());
  }
}

DimensionLabelReadDataQuery::DimensionLabelReadDataQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    const Subarray& parent_subarray,
    const QueryBuffer& label_buffer,
    const uint32_t dim_idx)
    : DimensionLabelQuery{storage_manager, dimension_label, true, false} {
  // Set the layout (ordered, 1D).
  throw_if_not_ok(indexed_array_query->set_layout(Layout::ROW_MAJOR));

  // Set the subarray.
  Subarray subarray{*indexed_array_query->subarray()};
  throw_if_not_ok(
      subarray.set_ranges_for_dim(0, parent_subarray.ranges_for_dim(dim_idx)));
  throw_if_not_ok(indexed_array_query->set_subarray(subarray));

  // Set the label data buffer.
  indexed_array_query->set_dimension_label_buffer(
      dimension_label->label_attribute()->name(), label_buffer);
}

/**
 * Creates a buffer to hold index data needed for dimension label write.
 *
 * @param Datatype of the index data to create.
 * @param Range to create data for.
 * @returns A pointer to an index data object.
 */
tdb_unique_ptr<IndexData> create_index_data(
    const Datatype type, const Range& input_range) {
  switch (type) {
    case Datatype::INT8:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<int8_t>, input_range));
    case Datatype::UINT8:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<uint8_t>, input_range));
    case Datatype::INT16:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<int16_t>, input_range));
    case Datatype::UINT16:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<uint16_t>, input_range));
    case Datatype::INT32:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<int32_t>, input_range));
    case Datatype::UINT32:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<uint32_t>, input_range));
    case Datatype::INT64:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<int64_t>, input_range));
    case Datatype::UINT64:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<uint64_t>, input_range));
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<int64_t>, input_range));
    default:
      stdx::unreachable();
  }
}

/**
 * Typed implementation to check if data is sorted.
 *
 * TODO: This is a quick-and-dirty implementation while we decide where sorting
 * is handled for ordered dimension labels. If we keep this design, we should
 * consider optimizing (parallelizing?) the loops in this check.
 *
 * @param buffer Buffer to check for sort.
 * @param buffer_size Total size of the buffer.
 * @param increasing If ``true`` check if the data is stricly increasing. If
 *     ``false``, check if the data is strictly decreasing.
 */
template <
    typename T,
    typename std::enable_if<std::is_arithmetic<T>::value>::type* = nullptr>
bool is_sorted_buffer_impl(
    const T* buffer, const uint64_t* buffer_size, bool increasing) {
  uint64_t num_values = *buffer_size / sizeof(T);
  if (increasing) {
    for (uint64_t index{0}; index < num_values - 1; ++index) {
      if (buffer[index + 1] < buffer[index]) {
        return false;
      }
    }
  } else {
    for (uint64_t index{0}; index < num_values - 1; ++index) {
      if (buffer[index + 1] > buffer[index]) {
        return false;
      }
    }
  }
  return true;
}

/**
 * Checks if the input buffer is sorted.
 *
 * @param buffer Buffer to check for sort.
 * @param type Datatype of the input buffer.
 * @param increasing If ``true`` check if the data is strictly increasing. If
 *     ``false``, check if the data is strictly decreasing.
 */
bool is_sorted_buffer(
    const QueryBuffer& buffer, const Datatype type, bool increasing) {
  switch (type) {
    case Datatype::INT8:
      return is_sorted_buffer_impl<int8_t>(
          static_cast<const int8_t*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    case Datatype::UINT8:
      return is_sorted_buffer_impl<uint8_t>(
          static_cast<const uint8_t*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    case Datatype::INT16:
      return is_sorted_buffer_impl<int16_t>(
          static_cast<const int16_t*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    case Datatype::UINT16:
      return is_sorted_buffer_impl<uint16_t>(
          static_cast<const uint16_t*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    case Datatype::INT32:
      return is_sorted_buffer_impl<int32_t>(
          static_cast<const int32_t*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    case Datatype::UINT32:
      return is_sorted_buffer_impl<uint32_t>(
          static_cast<const uint32_t*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    case Datatype::INT64:
      return is_sorted_buffer_impl<int64_t>(
          static_cast<const int64_t*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    case Datatype::UINT64:
      return is_sorted_buffer_impl<uint64_t>(
          static_cast<const uint64_t*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    case Datatype::FLOAT32:
      return is_sorted_buffer_impl<float>(
          static_cast<const float*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    case Datatype::FLOAT64:
      return is_sorted_buffer_impl<double>(
          static_cast<const double*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return is_sorted_buffer_impl<int64_t>(
          static_cast<const int64_t*>(buffer.buffer_),
          buffer.buffer_size_,
          increasing);
    default:
      stdx::unreachable();
  }

  return true;
}

OrderedWriteDataQuery::OrderedWriteDataQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    const Subarray& parent_subarray,
    const QueryBuffer& label_buffer,
    const QueryBuffer& index_buffer,
    const uint32_t dim_idx,
    optional<std::string> fragment_name)
    : DimensionLabelQuery{
          storage_manager, dimension_label, true, true, fragment_name} {
  // Verify that data isn't already written to the dimension label. This check
  // is only needed until the new ordered dimension label reader is implemented.
  if (!dimension_label->labelled_array()->is_empty() ||
      !dimension_label->indexed_array()->is_empty()) {
    throw StatusException(Status_DimensionLabelQueryError(
        "Cannot write to dimension label. Currently ordered dimension "
        "labels can only be written to once."));
  }

  // Verify the label data is sorted in the correct order.
  if (!is_sorted_buffer(
          label_buffer,
          dimension_label->label_dimension()->type(),
          dimension_label->label_order() == LabelOrder::INCREASING_LABELS)) {
    throw StatusException(Status_DimensionLabelQueryError(
        "Failed to create dimension label query. The label data is not in the "
        "expected order."));
  }

  // Create locally stored index data if the index buff is empty. Otherwise,
  // check the index buffer satisfies all write constraints.
  bool use_local_index = index_buffer.buffer_ == nullptr;
  if (use_local_index) {
    // Check parent subarray satisfies all write constraints.
    if (!parent_subarray.is_default(dim_idx)) {
      // Check only one range is set.
      const auto& ranges = parent_subarray.ranges_for_dim(dim_idx);
      if (ranges.size() != 1) {
        throw StatusException(Status_DimensionLabelQueryError(
            "Failed to create dimension label query. Dimension label writes "
            "can only be set for a single range."));
      }

      // Check the range is equal to the whole domain.
      const Range& input_range = ranges[0];
      const Range& index_domain = dimension_label->index_dimension()->domain();
      if (!(input_range == index_domain)) {
        throw StatusException(Status_DimensionLabelQueryError(
            "Failed to create dimension label query. Currently dimension "
            "labels only support writing the full array."));
      }
    }

    // Create index data for attribute on sparse labelled array
    index_data_ = create_index_data(
        dimension_label->index_dimension()->type(),
        parent_subarray.ranges_for_dim(dim_idx)[0]);

  } else {
    const auto index_type = dimension_label->index_dimension()->type();

    // Check that all the index data is included.
    if (*index_buffer.buffer_size_ / datatype_size(index_type) !=
        dimension_label->index_dimension()->domain_range(
            dimension_label->index_dimension()->domain())) {
      throw StatusException(Status_DimensionLabelQueryError(
          "Failed to create dimension label query. Currently dimension "
          "labels only support writing the full array."));
    }

    // Check the index data is sorted in increasing order.
    if (!is_sorted_buffer(index_buffer, index_type, true)) {
      throw StatusException(Status_DimensionLabelQueryError(
          "Failed to create dimension label query. The input data on "
          "dimension " +
          std::to_string(dim_idx) + " must be strictly increasing."));
    }
  }

  // Set-up labelled array query (sparse array)
  throw_if_not_ok(labelled_array_query->set_layout(Layout::UNORDERED));
  labelled_array_query->set_dimension_label_buffer(
      dimension_label->label_attribute()->name(), label_buffer);
  if (use_local_index) {
    throw_if_not_ok(labelled_array_query->set_data_buffer(
        dimension_label->index_attribute()->name(),
        index_data_->data(),
        index_data_->data_size(),
        true));
  } else {
    labelled_array_query->set_dimension_label_buffer(
        dimension_label->index_attribute()->name(), index_buffer);
  }

  // Set-up indexed array query (dense array)
  throw_if_not_ok(indexed_array_query->set_layout(Layout::ROW_MAJOR));
  indexed_array_query->set_dimension_label_buffer(
      dimension_label->label_attribute()->name(), label_buffer);
}

UnorderedWriteDataQuery::UnorderedWriteDataQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    const Subarray& parent_subarray,
    const QueryBuffer& label_buffer,
    const QueryBuffer& index_buffer,
    const uint32_t dim_idx,
    optional<std::string> fragment_name)
    : DimensionLabelQuery{
          storage_manager, dimension_label, true, true, fragment_name} {
  // Create locally stored index data if the index buffer is empty.
  bool use_local_index = index_buffer.buffer_ == nullptr;
  if (use_local_index) {
    // Check only one range on the subarray is set.
    if (!parent_subarray.is_default(dim_idx)) {
      const auto& ranges = parent_subarray.ranges_for_dim(dim_idx);
      if (ranges.size() != 1) {
        throw StatusException(Status_DimensionLabelQueryError(
            "Failed to create dimension label query. Dimension label writes "
            "can only be set for a single range."));
      }
    }

    // Create the index data.
    index_data_ = create_index_data(
        dimension_label->index_dimension()->type(),
        parent_subarray.ranges_for_dim(dim_idx)[0]);
  }

  // Set-up labelled array (sparse array).
  throw_if_not_ok(labelled_array_query->set_layout(Layout::UNORDERED));
  labelled_array_query->set_dimension_label_buffer(
      dimension_label->label_dimension()->name(), label_buffer);
  if (use_local_index) {
    throw_if_not_ok(labelled_array_query->set_data_buffer(
        dimension_label->index_attribute()->name(),
        index_data_->data(),
        index_data_->data_size(),
        true));
  } else {
    labelled_array_query->set_dimension_label_buffer(
        dimension_label->index_attribute()->name(), index_buffer);
  }

  // Set-up indexed array query (sparse array).
  throw_if_not_ok(indexed_array_query->set_layout(Layout::UNORDERED));
  indexed_array_query->set_dimension_label_buffer(
      dimension_label->label_attribute()->name(), label_buffer);
  if (use_local_index) {
    throw_if_not_ok(indexed_array_query->set_data_buffer(
        dimension_label->index_dimension()->name(),
        index_data_->data(),
        index_data_->data_size(),
        true));
  } else {
    indexed_array_query->set_dimension_label_buffer(
        dimension_label->index_dimension()->name(), index_buffer);
  }
}

}  // namespace tiledb::sm
