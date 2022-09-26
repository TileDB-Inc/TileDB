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
    : storage_manager_(storage_manager)
    , range_queries_(subarray.dim_num(), nullptr)
    , range_query_status_{QueryStatus::UNINITIALIZED}
    , fragment_name_{fragment_name} {
  switch (array->get_query_type()) {
    case (QueryType::READ):
      // Add necessary dimension label queries.
      add_range_queries(array, subarray, label_buffers, array_buffers);
      add_data_queries_for_read(array, subarray, label_buffers);
      break;

    case (QueryType::WRITE):
      // If fragment name is not set, set it.
      if (!fragment_name_.has_value()) {
        fragment_name_ = storage_format::generate_fragment_name(
            array->timestamp_end_opened_at(),
            array->array_schema_latest().write_version());
      }

      // Add dimesnion label queries.
      add_range_queries(array, subarray, label_buffers, array_buffers);
      add_data_queries_for_write(array, subarray, label_buffers, array_buffers);
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
  // Cancel range queries.
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      range_queries_.size(),
      [&](const size_t dim_idx) {
        if (range_queries_[dim_idx]) {
          range_queries_[dim_idx]->cancel();
        }
        return Status::Ok();
      }));

  // Cancel data queries.
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      data_queries_.size(),
      [&](const size_t query_idx) {
        data_queries_[query_idx]->cancel();
        return Status::Ok();
      }));
}

bool ArrayDimensionLabelQueries::completed() const {
  return range_query_status_ == QueryStatus::COMPLETED &&
         std::all_of(
             data_queries_map_.cbegin(),
             data_queries_map_.cend(),
             [](const auto& query) { return query.second->completed(); });
}

void ArrayDimensionLabelQueries::finalize() {
  // Finalize range queries.
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      range_queries_.size(),
      [&](const size_t dim_idx) {
        if (range_queries_[dim_idx]) {
          range_queries_[dim_idx]->finalize();
        }
        return Status::Ok();
      }));

  // Finalize data queries.
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      data_queries_.size(),
      [&](const size_t query_idx) {
        data_queries_[query_idx]->finalize();
        return Status::Ok();
      }));
}

void ArrayDimensionLabelQueries::process_data_queries() {
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      data_queries_.size(),
      [&](const size_t query_idx) {
        data_queries_[query_idx]->process();
        return Status::Ok();
      }));
}

void ArrayDimensionLabelQueries::process_range_queries(Subarray& subarray) {
  // Process range queries before updating any of the subarray ranges.
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      range_queries_.size(),
      [&](const size_t dim_idx) {
        if (range_queries_[dim_idx]) {
          range_queries_[dim_idx]->process();
        }
        return Status::Ok();
      }));

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

  range_query_status_ = QueryStatus::COMPLETED;
}

void ArrayDimensionLabelQueries::add_data_queries_for_read(
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

    // Open the indexed array.
    auto* dim_label = open_dimension_label(
        array, dim_label_ref, QueryType::READ, true, false);

    // Create the data query.
    data_queries_.emplace_back(tdb_unique_ptr<DimensionLabelQuery>(tdb_new(
        DimensionLabelReadDataQuery,
        storage_manager_,
        dim_label,
        subarray,
        label_buffer,
        dim_label_ref.dimension_id())));
    data_queries_map_[label_name] = data_queries_.back().get();
  }
}

void ArrayDimensionLabelQueries::add_data_queries_for_write(
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_buffers) {
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
        array, dim_label_ref, QueryType::WRITE, true, true);

    // Get the index_buffer from the array buffers.
    const auto& dim_name = array->array_schema_latest()
                               .dimension_ptr(dim_label_ref.dimension_id())
                               ->name();
    const auto& index_buffer_pair = array_buffers.find(dim_name);

    switch (dim_label_ref.label_order()) {
      case (LabelOrder::INCREASING_LABELS):
      case (LabelOrder::DECREASING_LABELS):
        // Create order label writer.
        data_queries_.emplace_back(tdb_unique_ptr<DimensionLabelQuery>(tdb_new(
            OrderedWriteDataQuery,
            storage_manager_,
            dim_label,
            subarray,
            label_buffer,
            (index_buffer_pair == array_buffers.end()) ?
                QueryBuffer() :
                index_buffer_pair->second,
            dim_label_ref.dimension_id(),
            fragment_name_)));
        data_queries_map_[label_name] = data_queries_.back().get();
        break;

      case (LabelOrder::UNORDERED_LABELS):
        // Create unordered label writer.
        data_queries_.emplace_back(tdb_unique_ptr<DimensionLabelQuery>(tdb_new(
            UnorderedWriteDataQuery,
            storage_manager_,
            dim_label,
            subarray,
            label_buffer,
            (index_buffer_pair == array_buffers.end()) ?
                QueryBuffer() :
                index_buffer_pair->second,
            dim_label_ref.dimension_id(),
            fragment_name_)));
        data_queries_map_[label_name] = data_queries_.back().get();
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

DimensionLabel* ArrayDimensionLabelQueries::open_dimension_label(
    Array* array,
    const DimensionLabelReference& dim_label_ref,
    const QueryType& query_type,
    const bool open_indexed_array,
    const bool open_labelled_array) {
  // Get the URI fo the dimension label from the dimension label reference.
  const auto& uri = dim_label_ref.uri();
  bool is_relative = true;
  auto dim_label_uri =
      is_relative ? array->array_uri().join_path(uri.to_string()) : uri;

  // Create the dimension label.
  dimension_labels_[dim_label_ref.name()] = tdb_unique_ptr<DimensionLabel>(
      tdb_new(DimensionLabel, dim_label_uri, storage_manager_));
  auto* dim_label = dimension_labels_[dim_label_ref.name()].get();

  // Currently there is no way to open just one of these arrays. This is a
  // placeholder for a single array open is implemented.
  if (open_indexed_array || open_labelled_array) {
    // Open the dimension label. Handling encrypted dimension labels is not yet
    // implemented.
    dim_label->open(
        query_type,
        array->timestamp_start(),
        array->timestamp_end(),
        EncryptionType::NO_ENCRYPTION,
        nullptr,
        0);

    // Check the dimension label schema matches the definition provided in
    // the dimension label reference.
    const auto& label_schema = dim_label->schema();
    if (!label_schema.is_compatible_label(
            array->array_schema_latest().dimension_ptr(
                dim_label_ref.dimension_id()))) {
      throw StatusException(Status_DimensionLabelQueryError(
          "Error opening dimesnion label; Found dimension label does not match "
          "array dimension."));
    }
    if (label_schema.label_order() != dim_label_ref.label_order()) {
      throw StatusException(Status_DimensionLabelQueryError(
          "Error opening dimension label; The label order of the loaded "
          "dimension label is " +
          label_order_str(label_schema.label_order()) +
          ", but the expected label order was " +
          label_order_str(dim_label_ref.label_order()) + "."));
    }
    if (label_schema.label_dimension()->type() != dim_label_ref.label_type()) {
      throw StatusException(Status_DimensionLabelQueryError(
          "Error opening dimension label; The label datatype of the loaded "
          "dimension label is " +
          datatype_str(label_schema.label_dimension()->type()) +
          ", but the expected label datatype was " +
          datatype_str(dim_label_ref.label_type()) + "."));
    }
    if (!(label_schema.label_dimension()->domain() ==
          dim_label_ref.label_domain())) {
      throw StatusException(Status_DimensionLabelQueryError(
          "Error opening dimension label; The label domain of the loaded "
          "dimension label does not match the expected domain."));
    }
    if (label_schema.label_dimension()->cell_val_num() !=
        dim_label_ref.label_cell_val_num()) {
      throw StatusException(Status_DimensionLabelQueryError(
          "Error opening dimension label; The label cell value number of the "
          "loaded "
          "dimension label is " +
          std::to_string(label_schema.label_dimension()->cell_val_num()) +
          ", but the expected label cell value number was " +
          std::to_string(dim_label_ref.label_cell_val_num()) + "."));
    }
  }

  return dim_label;
}

}  // namespace tiledb::sm
