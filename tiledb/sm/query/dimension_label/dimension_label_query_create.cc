/**
 * @file dimension_label_query_create.cc
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
 * Factory for creating dimension label query objects.
 */

#include "tiledb/sm/query/dimension_label/dimension_label_query_create.h"
#include "tiledb/sm/enums/label_order.h"
#include "tiledb/sm/query/query.h"

using namespace tiledb::common;

namespace tiledb::sm {

DimensionLabelDataQuery* DimensionLabelQueryCreate::make_write_query(
    const std::string& label_name,
    LabelOrder label_order,
    StorageManager* storage_manager,
    stats::Stats* parent_stats,
    DimensionLabel* dimension_label,
    const Subarray& parent_subarray,
    const QueryBuffer& label_buffer,
    const QueryBuffer& index_buffer,
    const uint32_t dim_idx,
    optional<std::string> fragment_name) {
  switch (label_order) {
    case (LabelOrder::INCREASING_LABELS):
    case (LabelOrder::DECREASING_LABELS):
      // Create order label writer.
      return tdb_new(
          OrderedWriteDataQuery,
          storage_manager,
          parent_stats->create_child("DimensionLabelQuery"),
          dimension_label,
          parent_subarray,
          label_buffer,
          index_buffer,
          dim_idx,
          fragment_name);

    case (LabelOrder::UNORDERED_LABELS):
      // Create unordered label writer.
      return tdb_new(
          UnorderedWriteDataQuery,
          storage_manager,
          dimension_label,
          parent_subarray,
          label_buffer,
          index_buffer,
          dim_idx,
          fragment_name);

    default:
      // Invalid label order type.
      throw DimensionLabelDataQueryStatusException(
          "Cannot initialize dimension label '" + label_name +
          "'; Dimension label order " + label_order_str(label_order) +
          " not supported.");
  }
}

}  // namespace tiledb::sm
