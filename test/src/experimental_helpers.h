/**
 * @file   experimental_helpers.h
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
 * This file declares some test suite helper functions for experimental
 * features.
 */

#ifndef TILEDB_TEST_EXPERIMENTAL_HELPERS_H
#define TILEDB_TEST_EXPERIMENTAL_HELPERS_H

#include "catch.hpp"
#include "helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/experimental/tiledb_dimension_label.h"
#include "tiledb/sm/c_api/experimental/tiledb_struct_def.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "vfs_helpers.h"

namespace tiledb::test {

/**
 * Helper method for adding an internal dimension label to an array schema.
 *
 * @param ctx TileDB context.
 * @param array_schema The array schema to add the dimension label to.
 * @param label_name The name of the dimension label.
 * @param dim_idx The dimension index to add the label on.
 * @param label_order The label order for the dimension label.
 * @param label_domain The dimension label domain.
 * @param label_tile_extent The dimension tile extent for the label.
 * @param index_tile_extent The dimension label tile extent for the index.
 * @param label_cell_val_num Optional label cell vall num.
 * @param label_filters Optional filters for the label attr/dim on the dimension
 *     label.
 * @param index_filters Optional filters for the index attr/dim on the dimension
 *     label.
 * @param capacity Optional capacity size for the dimension label.
 * @param allow_dups Optional allow dups for the dimension label.
 */
void add_dimension_label(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const std::string& label_name,
    const uint32_t dim_idx,
    tiledb_label_order_t label_order,
    tiledb_datatype_t label_datatype,
    const void* label_domain,
    const void* label_tile_extent,
    const void* index_tile_extent,
    optional<uint32_t> label_cell_val_num = nullopt,
    optional<std::pair<tiledb_filter_type_t, int>> label_filters = nullopt,
    optional<std::pair<tiledb_filter_type_t, int>> index_filters = nullopt,
    optional<uint64_t> capacity = nullopt,
    optional<bool> allows_dups = nullopt);

/**
 * Extension of the TemporaryDirectoryFixture that adds helper functions for
 * testing dimension labels.
 */
class DimensionLabelFixture : public TemporaryDirectoryFixture {
 public:
  /**
   * Read data from the indexed array.
   *
   * Temporary hard-coded method for checking array data in the dimension label
   * until update dimension label readers/writers are implemented.
   *
   * @param dim_label_uri URI of the dimension label.
   * @param ncells The number of cells to read.
   * @param start Starting index range value.
   * @param end Ending index range value.
   * @returns Vector of label values read from index array.
   */
  template <typename label_data_type>
  std::vector<label_data_type> read_indexed_array(
      const sm::URI& dim_label_uri,
      const uint64_t ncells,
      void* start,
      void* end) {
    // Define output data.
    std::vector<label_data_type> label_data(ncells);
    uint64_t label_data_size{label_data.size() * sizeof(label_data_type)};

    // Open array.
    tiledb_array_t* array;
    auto uri = dim_label_uri.join_path("indexed");
    require_tiledb_ok(tiledb_array_alloc(ctx, uri.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(
        tiledb_subarray_add_range(ctx, subarray, 0, start, end, nullptr));

    // Create query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_data_buffer(
        ctx, query, "label", label_data.data(), &label_data_size));

    // Submit query.
    require_tiledb_ok(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    require_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
    REQUIRE(query_status == TILEDB_COMPLETED);

    // Clean-up.
    tiledb_query_free(&query);
    tiledb_subarray_free(&subarray);
    tiledb_array_free(&array);

    return label_data;
  }

  /**
   * Read data from the labelled array.
   *
   * Temporary hard-coded method for checking array data in the dimension label
   * until update dimension label readers/writers are implemented.
   *
   * @param dim_label_uri URI of the dimension label.
   * @param ncells The number of cells to read.
   * @param start Starting label range value.
   * @param end Ending label range value.
   * @returns Vector of index values and vector of label values read from the
   *     labelled array.
   */
  template <typename index_data_type, typename label_data_type>
  std::tuple<std::vector<index_data_type>, std::vector<label_data_type>>
  read_labelled_array(
      const sm::URI& dim_label_uri,
      const uint64_t ncells,
      void* start,
      void* end) {
    // Define output data.
    std::vector<index_data_type> index_data(ncells);
    uint64_t index_data_size{index_data.size() * sizeof(index_data_type)};
    std::vector<label_data_type> label_data(ncells);
    uint64_t label_data_size{label_data.size() * sizeof(label_data_type)};

    // Open array.
    tiledb_array_t* array;
    auto uri = dim_label_uri.join_path("labelled");
    require_tiledb_ok(tiledb_array_alloc(ctx, uri.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(
        tiledb_subarray_add_range(ctx, subarray, 0, start, end, nullptr));

    // Create query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_data_buffer(
        ctx, query, "label", label_data.data(), &label_data_size));
    require_tiledb_ok(tiledb_query_set_data_buffer(
        ctx, query, "index", index_data.data(), &index_data_size));

    // Submit query.
    require_tiledb_ok(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    require_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
    REQUIRE(query_status == TILEDB_COMPLETED);

    // Clean-up.
    tiledb_query_free(&query);
    tiledb_subarray_free(&subarray);
    tiledb_array_free(&array);

    return {index_data, label_data};
  }
};

}  // namespace tiledb::test

#endif
