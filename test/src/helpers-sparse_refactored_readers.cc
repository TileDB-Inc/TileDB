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
#include "tiledb/sm/cpp_api/tiledb"

// Name of array.
const char* array_name("sparse_global_order_reader_array");

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
    buffers.emplace_back("data", TILEDB_UINT64, data_buffer);
  if (data_var_buffer != nullptr)
    buffers.emplace_back(
        "data_var", TILEDB_STRING_ASCII, data_var_buffer, offset_buffer);

  buffers.emplace_back("rows", TILEDB_UINT64, row_buffer);
  buffers.emplace_back("cols", TILEDB_UINT64, col_buffer);
}

bool write(std::vector<QueryBuffer>& test_query_buffers) {
  // Open the array for writing.
  Config config;
  config["sm.use_refactored_readers"] = true;
  Context ctx(config);
  Array array(ctx, array_name, TILEDB_WRITE);

  // Create the queries with unordered layouts.
  Query query(ctx, array);
  query.set_layout(TILEDB_UNORDERED);

  // Set the query buffers.
  for (auto& test_query_buffer : test_query_buffers) {
    if (test_query_buffer.type_ == TILEDB_UINT64) {
      query.set_data_buffer(
          test_query_buffer.name_,
          *(std::vector<uint64_t>*)test_query_buffer.data_);
    } else {
      query.set_data_buffer(
          test_query_buffer.name_,
          *(std::vector<char>*)test_query_buffer.data_);
    }
    if (test_query_buffer.offsets_) {
      query.set_offsets_buffer(
          test_query_buffer.name_, *test_query_buffer.offsets_);
    }
  }

  // Submit and finalize the queries.
  auto s = query.submit();
  query.finalize();

  // Close the array.
  array.close();

  if (s != Query::Status::COMPLETE)  // Error status
    return false;
  else
    return true;
}

bool write_array(
    uint64_t full_domain,
    uint64_t num_fragments,
    std::vector<TestDimension<uint64_t>> dims,
    std::string layout,
    int which_attrs,
    bool perf_analysis) {
  // Create buffers for the fragment write queries
  std::vector<QueryBuffer> write_query_buffers;
  uint64_t min_bound = 0;
  uint64_t max_bound = full_domain / num_fragments;
  uint64_t lower_bound = max_bound / 2;
  uint64_t fragment_size = max_bound;
  bool status;

  auto write_start = std::chrono::high_resolution_clock::now();
  if (layout == "ordered") {
    while (max_bound <= full_domain) {
      std::vector<uint64_t> row_buffer, col_buffer;
      if (which_attrs == 1) {
        std::vector<uint64_t> data_buffer;
        create_write_query_buffer(
            min_bound,
            max_bound,
            dims,
            &row_buffer,
            &col_buffer,
            &data_buffer,
            nullptr,
            nullptr,
            nullptr,
            write_query_buffers);
        status = write(write_query_buffers);
        min_bound = max_bound;
        max_bound += fragment_size;
        if (!status)  // Error status
          return false;
      } else if (which_attrs == 2) {
        std::vector<char> data_var_buffer;
        std::vector<uint64_t> offset_buffer;
        uint64_t offset = 0;
        create_write_query_buffer(
            min_bound,
            max_bound,
            dims,
            &row_buffer,
            &col_buffer,
            nullptr,
            &data_var_buffer,
            &offset_buffer,
            &offset,
            write_query_buffers);
        status = write(write_query_buffers);
        min_bound = max_bound;
        max_bound += fragment_size;
        if (!status)
          return false;
      } else {
        std::vector<uint64_t> data_buffer, offset_buffer;
        std::vector<char> data_var_buffer;
        uint64_t offset = 0;
        create_write_query_buffer(
            min_bound,
            max_bound,
            dims,
            &row_buffer,
            &col_buffer,
            &data_buffer,
            &data_var_buffer,
            &offset_buffer,
            &offset,
            write_query_buffers);
        status = write(write_query_buffers);
        min_bound = max_bound;
        max_bound += fragment_size;
        if (!status)
          return false;
      }
    }

  } else if (layout == "interleaved") {
    std::vector<std::pair<uint64_t, uint64_t>> domains;
    std::vector<std::pair<uint64_t, uint64_t>> fragments;
    uint64_t max_bound = 0;
    uint64_t fragment_multiplier = 3;
    uint64_t fragment_size =
        full_domain / (fragment_multiplier * num_fragments);

    while (max_bound < full_domain) {
      domains.emplace_back(max_bound, max_bound + fragment_size);
      max_bound += fragment_size;
    }

    int count = 1;
    while (!domains.empty()) {
      std::vector<uint64_t> row_buffer, col_buffer, data_buffer, offset_buffer;
      std::vector<char> data_var_buffer;
      for (uint64_t i = 0; i < fragment_multiplier; i++) {
        uint64_t rand_domain_idx = rand() % domains.size();
        std::pair<uint64_t, uint64_t> domain = domains[rand_domain_idx];
        domains.erase(domains.begin() + rand_domain_idx);
        fragments.emplace_back(domain);
      }

      std::sort(fragments.begin(), fragments.end());

      if (which_attrs == 1) {
        while (!fragments.empty()) {
          create_write_query_buffer(
              fragments.begin()->first,
              fragments.begin()->second,
              dims,
              &row_buffer,
              &col_buffer,
              &data_buffer,
              nullptr,
              nullptr,
              nullptr,
              write_query_buffers);
          fragments.erase(fragments.begin());
        }
      } else if (which_attrs == 2) {
        uint64_t offset = 0;
        while (!fragments.empty()) {
          create_write_query_buffer(
              fragments.begin()->first,
              fragments.begin()->second,
              dims,
              &row_buffer,
              &col_buffer,
              nullptr,
              &data_var_buffer,
              &offset_buffer,
              &offset,
              write_query_buffers);
          fragments.erase(fragments.begin());
        }
      } else {
        uint64_t offset = 0;
        while (!fragments.empty()) {
          create_write_query_buffer(
              fragments.begin()->first,
              fragments.begin()->second,
              dims,
              &row_buffer,
              &col_buffer,
              &data_buffer,
              &data_var_buffer,
              &offset_buffer,
              &offset,
              write_query_buffers);
          fragments.erase(fragments.begin());
        }
      }

      status = write(write_query_buffers);
      if (!status)
        return false;
      count++;
    }

  } else if (layout == "duplicated") {
    // We need to write the same query buffer twice.
    // This will result in the first "half" of the data duplicated across
    // the entire domain.
    uint64_t fragment_size = lower_bound;
    while (lower_bound <= full_domain / 2) {
      std::vector<uint64_t> data_buffer, row_buffer, col_buffer;
      create_write_query_buffer(
          min_bound,
          lower_bound,
          dims,
          &row_buffer,
          &col_buffer,
          &data_buffer,
          nullptr,
          nullptr,
          nullptr,
          write_query_buffers);
      create_write_query_buffer(
          min_bound,
          lower_bound,
          dims,
          &row_buffer,
          &col_buffer,
          &data_buffer,
          nullptr,
          nullptr,
          nullptr,
          write_query_buffers);
      status = write(write_query_buffers);
      min_bound = lower_bound;
      lower_bound += fragment_size;
      if (!status)
        return false;
    }

  } else {
    std::cerr << "Error: Invalid fragment layout. "
              << "Must be \"ordered\", \"interleaved\", or \"duplicated\""
              << std::endl;
  }
  auto write_stop = std::chrono::high_resolution_clock::now();
  auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      write_stop - write_start);
  if (perf_analysis)
    std::cerr << "\n[Performance][Write]: " << write_duration.count()
              << " milliseconds." << std::endl;

  return true;
}

void validate_data(
    uint64_t validation_min,
    uint64_t validation_max,
    std::string layout,
    std::vector<uint64_t>* data,
    std::vector<char>* data_var,
    std::vector<uint64_t>* offsets,
    std::vector<uint64_t>& coords_rows,
    std::vector<uint64_t>& coords_cols) {
  if (layout == "ordered" || layout == "interleaved") {
    for (uint64_t i = validation_min; i < (validation_max); i++) {
      if (offsets != nullptr) {
        uint64_t offset = (*offsets)[i - validation_min];
        if ((*data_var)[offset] != (i % 26 + 65)) {
          std::cerr << "Error: Data " << char((*data_var)[offset])
                    << " starting at coordinate {"
                    << coords_rows[i - validation_min] << ","
                    << coords_cols[i - validation_min]
                    << "} is inconsistent with the anticipated value of "
                    << char(i % 26 + 65) << "." << std::endl;
          break;
        }
      } else {
        if ((*data)[i - validation_min] != i) {
          std::cerr << "Error: Data " << (*data)[i - validation_min]
                    << " starting at coordinate {"
                    << coords_rows[i - validation_min] << ","
                    << coords_cols[i - validation_min]
                    << "} is inconsistent with the anticipated value of " << i
                    << "." << std::endl;
          break;
        }
      }
    }
  } else if (layout == "duplicated") {
    uint64_t count = 0;
    for (uint64_t i = validation_min; i < validation_max; i += 2) {
      if ((*data)[i] != count && (*data)[i + 1] != count) {
        std::cerr << "Error: Data at coordinate {" << coords_rows[i] << ","
                  << coords_cols[i]
                  << "} is inconsistent with the anticipated value."
                  << std::endl;
      }
      count++;
    }
  } else {
    std::cerr << "Error: Invalid fragment layout. "
              << "Must be \"ordered\", \"interleaved\", or \"duplicated\""
              << std::endl;
  }
}

bool read_array(
    uint64_t full_domain,
    uint64_t buffer_size,
    bool set_subarray,
    std::string layout,
    int which_attrs,
    bool perf_analysis) {
  Config config;
  config["sm.use_refactored_readers"] = "true";
  Context ctx(config);

  if (perf_analysis)
    std::cerr << "Reading full domain: " << full_domain << std::endl;

  // Set up buffers
  std::vector<QueryBuffer> read_query_buffers;
  std::vector<uint64_t> data(buffer_size), coords_rows(buffer_size),
      coords_cols(buffer_size);
  std::vector<char> data_var(buffer_size);
  std::vector<uint64_t> offsets(buffer_size);
  if (which_attrs == 1) {
    read_query_buffers.emplace_back("data", TILEDB_UINT64, &data);
  } else if (which_attrs == 2) {
    read_query_buffers.emplace_back(
        "data_var", TILEDB_STRING_ASCII, &data_var, &offsets);
  } else {
    read_query_buffers.emplace_back("data", TILEDB_UINT64, &data);
    read_query_buffers.emplace_back(
        "data_var", TILEDB_STRING_ASCII, &data_var, &offsets);
  }
  read_query_buffers.emplace_back("rows", TILEDB_UINT64, &coords_rows);
  read_query_buffers.emplace_back("cols", TILEDB_UINT64, &coords_cols);

  // Open the array for reading and create the read query
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  query.set_layout(TILEDB_GLOBAL_ORDER);

  if (set_subarray)
    query.set_subarray<uint64_t>({1, full_domain, 1, full_domain});

  // Set the query buffers.
  for (auto& test_query_buffer : read_query_buffers) {
    if (test_query_buffer.type_ == TILEDB_UINT64) {
      query.set_data_buffer(
          test_query_buffer.name_,
          *(std::vector<uint64_t>*)test_query_buffer.data_);
    } else {
      query.set_data_buffer(
          test_query_buffer.name_,
          *(std::vector<char>*)test_query_buffer.data_);
    }
    if (test_query_buffer.offsets_) {
      query.set_offsets_buffer(
          test_query_buffer.name_, *test_query_buffer.offsets_);
    }
  }

  uint64_t total_time = 0;
  uint64_t current_offset = 0;
  Query::Status status = Query::Status::UNINITIALIZED;
  while (status != Query::Status::COMPLETE) {
    auto start = std::chrono::high_resolution_clock::now();
    status = query.submit();
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    total_time += duration.count();

    auto result_buffers = query.result_buffer_elements();
    auto result_num = which_attrs == 1 ? result_buffers["data"].second :
                                         result_buffers["data_var"].first;
    bool done = current_offset + result_num == full_domain;
    if (status !=
        (done ? Query::Status::COMPLETE : Query::Status::INCOMPLETE)) {
      std::cerr << "Unexpected status from read query" << std::endl;
      return false;
    }

    // Validate.
    if (which_attrs == 1) {
      validate_data(
          current_offset,
          current_offset + result_num - 1,
          layout,
          &data,
          nullptr,
          nullptr,
          coords_rows,
          coords_cols);
    } else if (which_attrs == 2) {
      validate_data(
          current_offset,
          current_offset + result_num - 1,
          layout,
          nullptr,
          &data_var,
          &offsets,
          coords_rows,
          coords_cols);
    } else {
      validate_data(
          current_offset,
          current_offset + result_num - 1,
          layout,
          &data,
          nullptr,
          nullptr,
          coords_rows,
          coords_cols);
      validate_data(
          current_offset,
          current_offset + result_num - 1,
          layout,
          nullptr,
          &data_var,
          &offsets,
          coords_rows,
          coords_cols);
    }
    if (perf_analysis)
      std::cerr << "Processed offset: " << current_offset << std::endl;
    current_offset += result_num;
  }

  // Performance analysis.
  if (perf_analysis)
    std::cerr << "\n[Performance][Read]: " << total_time << " milliseconds."
              << std::endl;

  // Finalize query and close array.
  query.finalize();
  array.close();

  return true;
}

void sparse_global_test(
    uint64_t full_domain,
    uint64_t num_fragments,
    uint64_t read_buffer_size,
    const std::vector<TestAttribute>& attrs,
    bool set_subarray,
    std::string layout,
    int which_attrs,
    bool perf_analysis) {
  Context ctx;

  // Remove the array if it already exists.
  if (Object::object(ctx, array_name).type() == Object::Type::Array)
    Object::remove(ctx, array_name);

  // Define the dimensions.
  uint64_t domain_max = ceil(sqrt(4 * full_domain));
  uint64_t tile_extent = ceil(0.2 * domain_max);
  std::vector<TestDimension<uint64_t>> dims;
  std::array<uint64_t, 2> rows_domain = {1, domain_max};
  dims.emplace_back("rows", rows_domain, tile_extent);
  std::array<uint64_t, 2> cols_domain = {1, domain_max};
  dims.emplace_back("cols", cols_domain, tile_extent);

  std::vector<TestAttribute> test_attrs;
  if (which_attrs == 1) {
    test_attrs.emplace_back(attrs[0]);
  } else if (which_attrs == 2) {
    test_attrs.emplace_back(attrs[1]);
  } else {
    test_attrs.emplace_back(attrs[0]);
    test_attrs.emplace_back(attrs[1]);
  }

  // Create the array only if it does not exist.
  if (Object::object(ctx, array_name).type() != Object::Type::Array)
    create_array(dims, test_attrs);

  if (write_array(
          full_domain, num_fragments, dims, layout, which_attrs, perf_analysis))
    read_array(
        full_domain,
        read_buffer_size,
        set_subarray,
        layout,
        which_attrs,
        perf_analysis);

  // Remove the array if it exists.
  if (Object::object(ctx, array_name).type() == Object::Type::Array)
    Object::remove(ctx, array_name);
}

}  // End of namespace tiledb