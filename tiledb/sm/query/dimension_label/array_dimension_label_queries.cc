/**
 * @file dimension_label_queries.cc
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
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/query/dimension_label/dimension_label_query.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/storage_format/uri/generate_uri.h"

#include <algorithm>

using namespace tiledb::common;

namespace tiledb::sm {

ArrayDimensionLabelQueries::ArrayDimensionLabelQueries(
    StorageManager* storage_manager,
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_buffers,
    const optional<std::string>& fragment_name)
    : range_queries_(subarray.dim_num(), nullptr)
    , range_query_status_{QueryStatus::UNINITIALIZED} {
  switch (array->get_query_type()) {
    case (QueryType::READ):
      add_range_queries(
          storage_manager, array, subarray, label_buffers, array_buffers);
      add_data_queries_for_read(
          storage_manager, array, subarray, label_buffers);
      break;

    case (QueryType::WRITE):

      add_range_queries(
          storage_manager, array, subarray, label_buffers, array_buffers);
      add_data_queries_for_write(
          storage_manager,
          array,
          subarray,
          label_buffers,
          array_buffers,
          fragment_name.has_value() ?
              fragment_name.value() :
              storage_format::generate_fragment_name(
                  array->timestamp_end_opened_at(),
                  array->array_schema_latest().write_version()));
      break;

    case (QueryType::DELETE):
    case (QueryType::UPDATE):
    case (QueryType::MODIFY_EXCLUSIVE):
      if (!label_buffers.empty() || subarray.has_label_ranges()) {
        throw StatusException(Status_DimensionLabelQueryError(
            "Failed to add dimension label queries. Query type " +
            query_type_str(array->get_query_type()) +
            " is not supported for dimension labels."));
      }
      break;

    default:
      throw StatusException(Status_DimensionLabelQueryError(
          "Failed to add dimension label queries. Unknown query type " +
          query_type_str(array->get_query_type()) + "."));
  }
  range_query_status_ = range_queries_map_.empty() ? QueryStatus::COMPLETED :
                                                     QueryStatus::INPROGRESS;
}

void ArrayDimensionLabelQueries::cancel() {
  for (auto& [label_name, query] : range_queries_map_) {
    query->cancel();
  }
  for (auto& [label_name, query] : data_queries_) {
    query->cancel();
  }
}

void ArrayDimensionLabelQueries::finalize() {
  for (auto& [label_name, query] : range_queries_map_) {
    query->finalize();
  }
  for (auto& [label_name, query] : data_queries_) {
    query->finalize();
  }
}

void ArrayDimensionLabelQueries::process_data_queries() {
  // TODO: Parallel?
  for (auto& [label_name, query] : data_queries_) {
    query->process();
  }
}

void ArrayDimensionLabelQueries::process_range_queries(Subarray& subarray) {
  // TODO: Parallel?
  for (auto& [label_name, query] : range_queries_map_) {
    query->process();
  }

  // TODO: Do something smart for throwing errors and setting ranges.
  // Update the subarray.
  for (dimension_size_type dim_idx{0}; dim_idx < subarray.dim_num();
       ++dim_idx) {
    const auto& range_query = range_queries_[dim_idx];

    // Continue to next dimension index if no range query.
    if (!range_query) {
      continue;
    }

    // Throw for now if this is reached. This function is still mainly a
    // placeholder for the time being.
    throw StatusException(Status_DimensionLabelQueryError(
        "Support for querying arrays by label ranges is not yet implemented."));
  }
}

void ArrayDimensionLabelQueries::add_data_queries_for_read(
    StorageManager* storage_manager,
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_buffers) {
  for (const auto& [label_name, label_buffer] : label_buffers) {
    // Skip adding a new query if this dimension label was already used to
    // add a range query above.
    if (range_queries_map_.find(label_name) != range_queries_map_.end()) {
      continue;
    }

    // Get the dimension label reference from the array.
    const auto& dim_label_ref =
        array->array_schema_latest().dimension_label_reference(label_name);
    // TODO: Ensure label order type is valid with
    // ensure_label_order_is_valid.

    // Open the indexed array.
    auto* dim_label = open_dimension_label(
        storage_manager, array, dim_label_ref, QueryType::READ, true, false);

    // Create the data query.
    data_queries_[label_name] = tdb_unique_ptr<DimensionLabelQuery>(tdb_new(
        DimensionLabelReadDataQuery,
        storage_manager,
        dim_label,
        subarray,
        label_buffer,
        dim_label_ref.dimension_id()));
  }
}

void ArrayDimensionLabelQueries::add_data_queries_for_write(
    StorageManager* storage_manager,
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_buffers,
    const std::string& fragment_name) {
  for (const auto& [label_name, label_buffer] : label_buffers) {
    // Skip adding a new query if this dimension label was already used to
    // add a range query above.
    if (range_queries_map_.find(label_name) != range_queries_map_.end()) {
      continue;
    }

    // Get the dimension label reference from the array.
    const auto& dim_label_ref =
        array->array_schema_latest().dimension_label_reference(label_name);

    // Open both arrays in the dimension label.
    auto* dim_label = open_dimension_label(
        storage_manager, array, dim_label_ref, QueryType::WRITE, true, true);

    // Get the index_buffer from the array buffers.
    const auto& dim_name = array->array_schema_latest()
                               .dimension_ptr(dim_label_ref.dimension_id())
                               ->name();
    const auto& index_buffer_pair = array_buffers.find(dim_name);

    switch (dim_label_ref.label_order()) {
      case (LabelOrder::INCREASING_LABELS):
      case (LabelOrder::DECREASING_LABELS):
        if (index_buffer_pair == array_buffers.end()) {
          data_queries_[label_name] =
              tdb_unique_ptr<DimensionLabelQuery>(tdb_new(
                  OrderedWriteDataQuery,
                  storage_manager,
                  dim_label,
                  subarray,
                  label_buffer,
                  dim_label_ref.dimension_id(),
                  fragment_name));
        } else {
          data_queries_[label_name] =
              tdb_unique_ptr<DimensionLabelQuery>(tdb_new(
                  OrderedWriteDataQuery,
                  storage_manager,
                  dim_label,
                  index_buffer_pair->second,
                  label_buffer,
                  fragment_name));
        }
        break;

      case (LabelOrder::UNORDERED_LABELS):
        if (index_buffer_pair == array_buffers.end()) {
          throw StatusException(Status_DimensionLabelQueryError(
              "Cannot read range data from unordered label '" + label_name +
              "'; Missing a data buffer for dimension '" + dim_name + "'."));
        }
        data_queries_[label_name] = tdb_unique_ptr<DimensionLabelQuery>(tdb_new(
            UnorderedWriteDataQuery,
            storage_manager,
            dim_label,
            index_buffer_pair->second,
            label_buffer,
            fragment_name));
        break;

      default:
        // Invalid label order type.
        throw StatusException(Status_DimensionLabelQueryError(
            "Cannot initialize dimension label '" + label_name +
            "'; Dimension label order " +
            label_order_str(dim_label_ref.label_order()) + " not supported."));
    }
  }
}

void ArrayDimensionLabelQueries::add_range_queries(
    StorageManager*,
    Array*,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>&,
    const std::unordered_map<std::string, QueryBuffer>&) {
  // Add queries for dimension labels set on the subarray.
  for (ArraySchema::dimension_size_type dim_idx{0};
       dim_idx < subarray.dim_num();
       ++dim_idx) {
    // Continue to the next dimension if this dimension does not have any
    // label ranges.
    if (!subarray.has_label_ranges(dim_idx)) {
      continue;
    }

    // This function and infrastructure around the range queries is currently
    // a placeholder.
    throw StatusException(Status_DimensionLabelQueryError(
        "Querying arrays by dimension label ranges has not yet been "
        "implemented."));
  }
}

bool ArrayDimensionLabelQueries::completed() const {
  return std::all_of(
             range_queries_map_.cbegin(),
             range_queries_map_.cend(),
             [](const auto& query) { return query.second->completed(); }) &&
         std::all_of(
             data_queries_.cbegin(),
             data_queries_.cend(),
             [](const auto& query) { return query.second->completed(); });
}

DimensionLabel* ArrayDimensionLabelQueries::open_dimension_label(
    StorageManager* storage_manager,
    Array* array,
    const DimensionLabelReference& dim_label_ref,
    const QueryType& query_type,
    const bool open_indexed_array,
    const bool open_labelled_array) {
  const auto& uri = dim_label_ref.uri();
  bool is_relative = true;  // TODO: Fix to move this to array
  auto dim_label_uri =
      is_relative ? array->array_uri().join_path(uri.to_string()) : uri;

  // Create dimension label.
  dimension_labels_[dim_label_ref.name()] = tdb_unique_ptr<DimensionLabel>(
      tdb_new(DimensionLabel, dim_label_uri, storage_manager));
  auto* dim_label = dimension_labels_[dim_label_ref.name()].get();

  // Currently there is no way to open just one of these arrays. This is a
  // placeholder for a single array open is implemented.
  if (open_indexed_array || open_labelled_array) {
    // Open the dimension label.
    // TODO: Fix how the encryption is handled.
    // TODO: Fix the timestamps for writes.
    dim_label->open(
        query_type,
        array->timestamp_start(),
        array->timestamp_end(),
        EncryptionType::NO_ENCRYPTION,
        nullptr,
        0);

    // Check the dimension label schema matches the definition provided in
    // the dimension label reference.
    // TODO: Move to array schema.
    const auto& label_schema = dim_label->schema();
    if (!label_schema.is_compatible_label(
            array->array_schema_latest().dimension_ptr(
                dim_label_ref.dimension_id()))) {
      throw StatusException(
          Status_DimensionLabelQueryError("TODO: Add error msg."));
    }
    if (label_schema.label_order() != dim_label_ref.label_order()) {
      throw StatusException(
          Status_DimensionLabelQueryError("TODO: Add error msg."));
    }
    if (label_schema.label_dimension()->type() != dim_label_ref.label_type()) {
      throw StatusException(
          Status_DimensionLabelQueryError("TODO: Add error msg."));
    }
    if (!(label_schema.label_dimension()->domain() ==
          dim_label_ref.label_domain())) {
      throw StatusException(
          Status_DimensionLabelQueryError("TODO: Add error msg."));
    }
    if (label_schema.label_dimension()->cell_val_num() !=
        dim_label_ref.label_cell_val_num()) {
      throw StatusException(
          Status_DimensionLabelQueryError("TODO: Add error msg."));
    }
  }

  return dim_label;
}

}  // namespace tiledb::sm
