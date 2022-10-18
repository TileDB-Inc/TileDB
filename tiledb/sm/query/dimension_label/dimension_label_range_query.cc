/**
 * @file dimension_label_range_query.cc
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

#include "tiledb/sm/query/dimension_label/dimension_label_range_query.h"
#include "tiledb/common/common.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/query/dimension_label/index_data.h"

using namespace tiledb::common;

namespace tiledb::sm {

OrderedRangeQuery::OrderedRangeQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    const std::vector<Range>& label_ranges)
    : query_{tdb_unique_ptr<Query>(tdb_new(
          Query, storage_manager, dimension_label->indexed_array(), nullopt))}
    , index_data_{IndexDataCreate::make_index_data(
          dimension_label->index_dimension()->type(),
          2 * label_ranges.size())} {
  // Set the basic query properies.
  throw_if_not_ok(query_->set_layout(Layout::ROW_MAJOR));
  query_->set_dimension_label_ordered_read(
      dimension_label->label_order() == LabelOrder::INCREASING_LABELS);

  // Set the subarray.
  Subarray subarray{*query_->subarray()};
  subarray.set_attribute_ranges(
      dimension_label->label_attribute()->name(), label_ranges);
  throw_if_not_ok(query_->set_subarray(subarray));

  // Set index data buffer that will store the computed ranges.
  throw_if_not_ok(query_->set_data_buffer(
      dimension_label->index_dimension()->name(),
      index_data_->data(),
      index_data_->data_size(),
      true));
}

bool OrderedRangeQuery::completed() const {
  return query_->status() == QueryStatus::COMPLETED;
}

tuple<bool, const void*, storage_size_t> OrderedRangeQuery::computed_ranges()
    const {
  if (!completed()) {
    throw DimensionLabelRangeQueryStatusException(
        "Cannot return computed ranges. Query has not completed.");
  }
  return {false, index_data_->data(), index_data_->count()};
}

void OrderedRangeQuery::process() {
  throw_if_not_ok(query_->init());
  throw_if_not_ok(query_->process());
}

}  // namespace tiledb::sm
