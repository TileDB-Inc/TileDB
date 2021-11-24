/**
 * @file   helpers-sparse_refactored_readers.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file defines some helper functions for the sparse refactored readers
 * tests.
 */

#include "helpers-sparse_refactored_readers.h"
#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

// Name of array.
const char* array_name("sparse_global_order_reader_array");

// namespace std::chrono {
namespace tiledb {

template <typename DIM_T>
void create_array(
    const std::vector<TestDimension<DIM_T>>& test_dims,
    const std::vector<TestAttribute>& test_attrs) {
  // Create domain.
  Context ctx;
  Domain domain(ctx);

  // Create the dimensions.
  for (const auto& test_dim : test_dims) {
    domain.add_dimension(Dimension::create<DIM_T>(
        ctx, test_dim.name_, test_dim.domain_, test_dim.tile_extent_));
  }

  // The array will be sparse.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain)
      .set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
      .set_allows_dups(true);

  // Create the attributes.
  for (const auto& test_attr : test_attrs) {
    Attribute attr = Attribute::create(ctx, test_attr.name_, test_attr.type_);
    if (test_attr.type_ == TILEDB_STRING_ASCII)
      attr.set_cell_val_num(TILEDB_VAR_NUM);
    schema.add_attribute(attr);
  }

  // Check the array schema.
  schema.check();

  // Create the array.
  Array::create(array_name, schema);
}

template <typename DIM_T>
void fill_coords(
    uint64_t i,
    const std::vector<TestDimension<DIM_T>>& test_dims,
    std::array<uint64_t, 2>& coords) {
  const uint64_t domain_min = test_dims[0].domain_[0];
  const auto domain_extent = (test_dims[0].domain_[1] - domain_min + 1);
  const int tile_extent = test_dims[0].tile_extent_;
  const int dim_num = test_dims.size();
  const int tiles_per_row_column = domain_extent / tile_extent;

  auto tile_pos = i / (tile_extent * tile_extent);
  auto cell_pos = i % (tile_extent * tile_extent);

  auto div_tile = (uint64_t)pow(tiles_per_row_column, dim_num - 1);
  auto div_cell = (uint64_t)pow(tile_extent, dim_num - 1);
  for (int64_t dim = 0; dim < dim_num; dim++) {
    coords[dim] += ((tile_pos / div_tile) % tiles_per_row_column) * tile_extent;
    div_tile /= tiles_per_row_column;
    coords[dim] += (cell_pos / div_cell) % tile_extent + domain_min;
    div_cell /= tile_extent;
  }
}

void create_write_query_buffer(
    int min_bound,
    int max_bound,
    std::vector<TestDimension<uint64_t>> dims,
    std::vector<uint64_t>* row_buffer,
    std::vector<uint64_t>* col_buffer,
    std::vector<uint64_t>* data_buffer,
    std::vector<char>* data_var_buffer,
    std::vector<uint64_t>* offset_buffer,
    uint64_t* offset,
    std::vector<QueryBuffer>& buffers) {
  uint64_t length = 1;
  for (int i = min_bound; i < max_bound; i++) {
    std::array<uint64_t, 2> coords = {0, 0};
    fill_coords(i, dims, coords);

    row_buffer->emplace_back(coords[0]);
    col_buffer->emplace_back(coords[1]);
    if (data_buffer != nullptr)
      data_buffer->emplace_back(i);
    if (data_var_buffer != nullptr) {
      for (uint64_t j = 0; j < length; j++)
        data_var_buffer->emplace_back(i % 26 + 65);  // ASCII A-Z

      offset_buffer->emplace_back(*offset);
      *offset = *offset + length;
      length++;

      if (length == 17)
        length = 1;
    }
  }

  if (data_buffer != nullptr)
    buffers.emplace_back("data_buffer", TILEDB_UINT64, data_buffer);
  if (data_var_buffer != nullptr)
    buffers.emplace_back(
        "data_var_buffer", TILEDB_STRING_ASCII, data_var_buffer, offset_buffer);

  buffers.emplace_back("row_buffer", TILEDB_UINT64, row_buffer);
  buffers.emplace_back("col_buffer", TILEDB_UINT64, col_buffer);
}

/*void create_write_query_buffer(
    int min_bound,
    int max_bound,
    std::vector<TestDimension<uint64_t>> dims) {//,
    //uint64_t* offset) {
  std::vector<uint64_t> row_buffer;
  for (int i = min_bound; i < max_bound; i++) {
    std::array<uint64_t, 2> coords = {0, 0};
    fill_coords(i, dims, coords);

    row_buffer.emplace_back(coords[0]);
  }

  std::vector<QueryBuffer> buffers;
  buffers.emplace_back(QueryBuffer());
  buffers[0].name_ = "row_buffer";
  buffers[0].type_ = TILEDB_UINT64;
  buffers[0].data_ = &row_buffer;

}*/

}  // End of namespace tiledb

//}  // End of namespace std::chrono