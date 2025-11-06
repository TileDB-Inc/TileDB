/**
 * @file dimension_label_query.cc
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
 * Classes for querying (reading/writing) a dimension label using the index
 * dimension for setting the subarray.
 */

#include "tiledb/sm/query/dimension_label/dimension_label_query.h"
#include "tiledb/common/common.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/enums/data_order.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/subarray/subarray.h"

#include <algorithm>
#include <functional>

using namespace tiledb::common;

namespace tiledb::sm {

DimensionLabelQuery::DimensionLabelQuery(
    ContextResources& resources,
    StorageManager* storage_manager,
    shared_ptr<Array> dim_label,
    const DimensionLabel& dim_label_ref,
    const Subarray& parent_subarray,
    const QueryBuffer& label_buffer,
    const QueryBuffer& index_buffer,
    optional<std::string> fragment_name)
    : Query(
          resources,
          CancellationSource(storage_manager),
          storage_manager,
          dim_label,
          fragment_name)
    , dim_label_name_{dim_label_ref.name()} {
  switch (dim_label->get_query_type()) {
    case (QueryType::READ):
      initialize_read_labels_query(
          parent_subarray,
          dim_label_ref.label_attr_name(),
          label_buffer,
          dim_label_ref.dimension_index());
      break;

    case (QueryType::WRITE): {
      // Initialize write query for the appropriate label order type.
      switch (dim_label_ref.label_order()) {
        case (DataOrder::INCREASING_DATA):
        case (DataOrder::DECREASING_DATA):
          initialize_ordered_write_query(
              parent_subarray,
              dim_label_ref.label_attr_name(),
              label_buffer,
              index_buffer,
              dim_label_ref.dimension_index());
          break;

        case (DataOrder::UNORDERED_DATA):
          initialize_unordered_write_query(
              parent_subarray,
              dim_label_ref.label_attr_name(),
              label_buffer,
              index_buffer,
              dim_label_ref.dimension_index());
          break;

        default:
          // Invalid label order type.
          throw DimensionLabelQueryException(
              "Unrecognized label order " +
              data_order_str(dim_label_ref.label_order()));
      }
      break;
    }

    default:
      throw DimensionLabelQueryException(
          "Query type " + query_type_str(dim_label->get_query_type()) +
          " not supported for dimension label queries.");
  }
}

DimensionLabelQuery::DimensionLabelQuery(
    ContextResources& resources,
    StorageManager* storage_manager,
    shared_ptr<Array> dim_label,
    const DimensionLabel& dim_label_ref,
    std::span<const Range> label_ranges)
    : Query(
          resources,
          CancellationSource(storage_manager),
          storage_manager,
          dim_label,
          nullopt)
    , dim_label_name_{dim_label_ref.name()}
    , index_data_{IndexDataCreate::make_index_data(
          array_schema().dimension_ptr(0)->type(),
          2 * label_ranges.size(),
          false)} {
  // Check dimension label order is supported.
  switch (dim_label_ref.label_order()) {
    case (DataOrder::INCREASING_DATA):
    case (DataOrder::DECREASING_DATA):
      break;
    case (DataOrder::UNORDERED_DATA):
      throw DimensionLabelQueryException(
          "Support for reading ranges from unordered labels is not yet "
          "implemented.");
    default:
      // Invalid label order type.
      throw DimensionLabelQueryException(
          "Unrecognized label order " +
          data_order_str(dim_label_ref.label_order()));
  }

  // Set the basic query properies.
  throw_if_not_ok(set_layout(Layout::ROW_MAJOR));
  set_dimension_label_ordered_read(
      dim_label_ref.label_order() == DataOrder::INCREASING_DATA);

  // Set the subarray.
  Subarray subarray{*this->subarray()};
  subarray.set_attribute_ranges(dim_label_ref.label_attr_name(), label_ranges);
  set_subarray(subarray);

  // Set index data buffer that will store the computed ranges.
  throw_if_not_ok(set_data_buffer(
      array_schema().dimension_ptr(0)->name(),
      index_data_->data(),
      index_data_->data_size(),
      true));
}

bool DimensionLabelQuery::completed() const {
  return status() == QueryStatus::COMPLETED;
}

void DimensionLabelQuery::initialize_read_labels_query(
    const Subarray& parent_subarray,
    const std::string& label_attr_name,
    const QueryBuffer& label_buffer,
    const uint32_t dim_idx) {
  // Set the layout (ordered, 1D).
  throw_if_not_ok(set_layout(Layout::ROW_MAJOR));

  // Set the subarray if it has index ranges added to it.
  if (!parent_subarray.is_default(dim_idx) &&
      !parent_subarray.has_label_ranges(dim_idx)) {
    Subarray subarray{*this->subarray()};
    subarray.set_ranges_for_dim(0, parent_subarray.ranges_for_dim(dim_idx));
    set_subarray(subarray);
  }

  // Set the label data buffer.
  set_dimension_label_buffer(label_attr_name, label_buffer);
}

void DimensionLabelQuery::initialize_ordered_write_query(
    const Subarray& parent_subarray,
    const std::string& label_attr_name,
    const QueryBuffer& label_buffer,
    const QueryBuffer& index_buffer,
    const uint32_t dim_idx) {
  // Set query layout.
  throw_if_not_ok(set_layout(Layout::ROW_MAJOR));

  // Set the label data buffer.
  set_dimension_label_buffer(label_attr_name, label_buffer);

  // Set the subarray.
  if (index_buffer.buffer_ == nullptr) {
    // Set the subarray if it has index ranges added to it.
    if (!parent_subarray.is_default(dim_idx)) {
      Subarray subarray{*this->subarray()};
      subarray.set_ranges_for_dim(0, parent_subarray.ranges_for_dim(dim_idx));
      if (subarray.range_num() > 1) {
        throw DimensionLabelQueryException(
            "Dimension label writes can only be set for a single range.");
      }
      set_subarray(subarray);
    }

  } else {
    // Set the subarray using the points from the index buffer. Throw an error
    // if more than one range is created (only happens if index data is not
    // ordered).
    uint64_t count = *index_buffer.buffer_size_ /
                     datatype_size(array_schema().dimension_ptr(0)->type());
    Subarray subarray{*this->subarray()};
    subarray.set_coalesce_ranges(true);
    subarray.add_point_ranges(0, index_buffer.buffer_, count);
    if (subarray.range_num() > 1) {
      throw DimensionLabelQueryException(
          "The dimension data must contain consecutive points when writing to "
          "a dimension label.");
    }
    set_subarray(subarray);
  }
}

void DimensionLabelQuery::initialize_unordered_write_query(
    const Subarray& parent_subarray,
    const std::string& label_attr_name,
    const QueryBuffer& label_buffer,
    const QueryBuffer& index_buffer,
    const uint32_t dim_idx) {
  // Create locally stored index data if the index buffer is empty.
  bool use_local_index = index_buffer.buffer_ == nullptr;
  if (use_local_index) {
    // Check only one range on the subarray is set.
    if (!parent_subarray.is_default(dim_idx)) {
      const auto& ranges = parent_subarray.ranges_for_dim(dim_idx);
      if (ranges.size() != 1) {
        throw DimensionLabelQueryException(
            "Dimension label writes can only be set for a single range.");
      }
    }

    // Create the index data.
    index_data_ = tdb_unique_ptr<IndexData>(IndexDataCreate::make_index_data(
        array_schema().dimension_ptr(0)->type(),
        parent_subarray.ranges_for_dim(dim_idx)[0]));
  }

  // Set-up indexed array query (sparse array).
  throw_if_not_ok(set_layout(Layout::UNORDERED));
  set_dimension_label_buffer(label_attr_name, label_buffer);
  if (use_local_index) {
    throw_if_not_ok(set_data_buffer(
        array_schema().dimension_ptr(0)->name(),
        index_data_->data(),
        index_data_->data_size(),
        true));
  } else {
    set_dimension_label_buffer(
        array_schema().dimension_ptr(0)->name(), index_buffer);
  }
}

}  // namespace tiledb::sm
