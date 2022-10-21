/**
 * @file array_dimension_label_queries.cc
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

#include "tiledb/sm/query/dimension_label/array_dimension_label_queries.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/array_schema/dimension_label_schema.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/label_order.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/query/dimension_label/dimension_label_query_create.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/storage_format/uri/generate_uri.h"

#include <algorithm>

using namespace tiledb::common;

namespace tiledb::sm {

ArrayDimensionLabelQueries::ArrayDimensionLabelQueries(
    StorageManager* storage_manager,
    stats::Stats* stats,
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_buffers,
    const optional<std::string>& fragment_name)
    : storage_manager_(storage_manager)
    , stats_(stats)
    , label_range_queries_by_dim_idx_(subarray.dim_num(), nullptr)
    , label_data_queries_by_dim_idx_(subarray.dim_num())
    , range_query_status_{QueryStatus::UNINITIALIZED}
    , fragment_name_{fragment_name} {
  switch (array->get_query_type()) {
    case (QueryType::READ):
      // Add dimension label queries for parent array open for reading.
      add_read_queries(array, subarray, label_buffers, array_buffers);
      break;

    case (QueryType::WRITE):

      // If no label buffers, then we are reading index ranges from label ranges
      // for writing to the main array.
      if (label_buffers.empty()) {
        add_read_queries(array, subarray, label_buffers, array_buffers);
        break;

      } else {
        // Cannot both read label ranges and write label data on the same write.
        if (subarray.has_label_ranges()) {
          throw DimensionLabelQueryStatusException(
              "Failed to add dimension label queries. Cannot set both label "
              "buffer and label range on a write query.");
        }

        // If fragment name is not set, set it.
        // TODO: As implemented, the timestamp for the dimension label fragment
        // may be different than the main array. This should be updated to
        // either always get the fragment name from the parent array on writes
        // or to get the timestamp_end from the parent array. This fix is
        // blocked by current discussion on a timestamp refactor design.
        if (!fragment_name_.has_value()) {
          fragment_name_ = storage_format::generate_fragment_name(
              array->timestamp_end_opened_at(),
              array->array_schema_latest().write_version());
        }

        // Add dimension label queries for parent array open for writing.
        add_write_queries(array, subarray, label_buffers, array_buffers);
        break;
      }

    case (QueryType::DELETE):
    case (QueryType::UPDATE):
    case (QueryType::MODIFY_EXCLUSIVE):
      if (!label_buffers.empty() || subarray.has_label_ranges()) {
        throw DimensionLabelQueryStatusException(
            "Failed to add dimension label queries. Query type " +
            query_type_str(array->get_query_type()) +
            " is not supported for dimension labels.");
      }
      break;

    default:
      throw DimensionLabelQueryStatusException(
          "Failed to add dimension label queries. Unknown query type " +
          query_type_str(array->get_query_type()) + ".");
  }
  range_query_status_ =
      range_queries_.empty() ? QueryStatus::COMPLETED : QueryStatus::INPROGRESS;
}

bool ArrayDimensionLabelQueries::completed() const {
  return range_query_status_ == QueryStatus::COMPLETED &&
         std::all_of(
             data_queries_.cbegin(),
             data_queries_.cend(),
             [](const auto& query) { return query->completed(); });
}

void ArrayDimensionLabelQueries::process_data_queries() {
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      data_queries_.size(),
      [&](const size_t query_idx) {
        throw_if_not_ok(data_queries_[query_idx]->init());
        throw_if_not_ok(data_queries_[query_idx]->process());
        return Status::Ok();
      }));
}

void ArrayDimensionLabelQueries::process_range_queries(Query* parent_query) {
  // Process queries and update the subarray.
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      label_range_queries_by_dim_idx_.size(),
      [&](const uint32_t dim_idx) {
        auto& range = label_range_queries_by_dim_idx_[dim_idx];
        if (range) {
          // Process the query.
          throw_if_not_ok(range->init());
          throw_if_not_ok(range->process());

          // Update data queries and the parent query with the dimension ranges.
          auto [is_point_ranges, range_data, count] =
              label_range_queries_by_dim_idx_[dim_idx]->computed_ranges();
          for (auto& data_query : label_data_queries_by_dim_idx_[dim_idx]) {
            data_query->add_index_ranges_from_label(
                0, is_point_ranges, range_data, count);
          }
          parent_query->add_index_ranges_from_label(
              dim_idx, is_point_ranges, range_data, count);
        }
        return Status::Ok();
      }));

  // Mark the range query as completed.
  range_query_status_ = QueryStatus::COMPLETED;
}

void ArrayDimensionLabelQueries::add_read_queries(
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_buffers,
    const std::unordered_map<std::string, QueryBuffer>&) {
  // Add queries for the dimension labels that have ranges added to the
  // subarray.
  for (ArraySchema::dimension_size_type dim_idx{0};
       dim_idx < subarray.dim_num();
       ++dim_idx) {
    // Continue to the next dimension if this dimension does not have any
    // label ranges.
    if (!subarray.has_label_ranges(dim_idx)) {
      continue;
    }

    // Get the dimension label reference from the array.
    const auto& label_name = subarray.get_label_name(dim_idx);
    const auto& dim_label_ref =
        array->array_schema_latest().dimension_label_reference(label_name);

    // Unordered labels are not supported yet.
    if (dim_label_ref.label_order() == LabelOrder::UNORDERED_LABELS) {
      throw DimensionLabelQueryStatusException(
          "Support for reading ranges from unordered labels is not yet "
          "implemented.");
    }

    // Open the indexed array.
    auto* dim_label =
        open_dimension_label(array, dim_label_ref, QueryType::READ);

    // Get subarray ranges.
    auto& label_ranges = subarray.ranges_for_label(label_name);

    // Create the range query.
    range_queries_.emplace_back(
        tdb_new(OrderedRangeQuery, storage_manager_, dim_label, label_ranges));
    label_range_queries_by_dim_idx_[dim_idx] = range_queries_.back().get();
  }

  // Add remaining dimension label queries.
  for (const auto& [label_name, label_buffer] : label_buffers) {
    // Get the dimension label reference from the array.
    const auto& dim_label_ref =
        array->array_schema_latest().dimension_label_reference(label_name);
    const auto dim_idx = dim_label_ref.dimension_id();

    // Open the indexed array.
    auto dim_label_iter = dimension_labels_.find(label_name);
    auto* dim_label =
        dim_label_iter == dimension_labels_.end() ?
            open_dimension_label(array, dim_label_ref, QueryType::READ) :
            dim_label_iter->second.get();

    // Create the data query.
    data_queries_.emplace_back(tdb_new(
        DimensionLabelReadDataQuery,
        storage_manager_,
        dim_label,
        subarray,
        label_buffer,
        dim_idx));
    label_data_queries_by_dim_idx_[dim_idx].push_back(
        data_queries_.back().get());
  }
}

void ArrayDimensionLabelQueries::add_write_queries(
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_buffers) {
  // Add queries to write data to dimension labels.
  for (const auto& [label_name, label_buffer] : label_buffers) {
    // Get the dimension label reference from the array.
    const auto& dim_label_ref =
        array->array_schema_latest().dimension_label_reference(label_name);
    const auto dim_idx = dim_label_ref.dimension_id();

    // Verify that this subarray is not set to use labels.
    if (subarray.has_label_ranges(dim_label_ref.dimension_id())) {
      throw DimensionLabelQueryStatusException(
          "Failed to initialize dimension label query for label '" +
          label_name +
          "'. Cannot write label data when subarray is set by label range.");
    }

    // Open both arrays in the dimension label.
    auto* dim_label =
        open_dimension_label(array, dim_label_ref, QueryType::WRITE);

    // Get the index_buffer from the array buffers.
    const auto& dim_name = array->array_schema_latest()
                               .dimension_ptr(dim_label_ref.dimension_id())
                               ->name();
    const auto& index_buffer_pair = array_buffers.find(dim_name);

    data_queries_.emplace_back(DimensionLabelQueryCreate::make_write_query(
        label_name,
        dim_label_ref.label_order(),
        storage_manager_,
        stats_->create_child("DimensionLabelQuery"),
        dim_label,
        subarray,
        label_buffer,
        (index_buffer_pair == array_buffers.end()) ? QueryBuffer() :
                                                     index_buffer_pair->second,
        dim_idx,
        fragment_name_));
    label_data_queries_by_dim_idx_[dim_idx].push_back(
        data_queries_.back().get());
  }
}

DimensionLabel* ArrayDimensionLabelQueries::open_dimension_label(
    Array* array,
    const DimensionLabelReference& dim_label_ref,
    const QueryType& query_type) {
  // Create the dimension label.
  auto label_iter = dimension_labels_.try_emplace(
      dim_label_ref.name(),
      tdb_new(
          DimensionLabel,
          dim_label_ref.uri(array->array_uri()),
          storage_manager_));
  DimensionLabel* dim_label = (label_iter.first->second).get();

  // Open the dimension label. Handling encrypted dimension labels is not yet
  // implemented.
  dim_label->open(
      query_type,
      array->timestamp_start(),
      array->timestamp_end(),
      EncryptionType::NO_ENCRYPTION,
      nullptr,
      0);

  // Check the dimension label is compatible with the dim label reference and
  // dimension from the parent array.
  dim_label->is_compatible(
      dim_label_ref,
      array->array_schema_latest().dimension_ptr(dim_label_ref.dimension_id()));

  return dim_label;
}

}  // namespace tiledb::sm
