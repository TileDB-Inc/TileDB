/**
 * @file   aggregates_string.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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
 * When run, this program will create a 2D sparse array with on dimension a
 * string type, and the other an integer. The program will write some data to
 * it, and run a query to select coordinates and compute the min and max values
 * of the string dimension using aggregates.
 */

#include <iostream>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

// Name of array
std::string array_name("aggregates_string_array");

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // The array will be 2d array with dimensions "rows" and "cols"
  // "rows" is a string dimension type, so the domain and extent is null
  Domain domain(ctx);
  domain
      .add_dimension(
          Dimension::create(ctx, "rows", TILEDB_STRING_ASCII, nullptr, nullptr))
      .add_dimension(Dimension::create<int32_t>(ctx, "cols", {{1, 4}}, 4));

  // The array will be sparse.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a single attribute "a" so each (i,j) cell can store an integer.
  schema.add_attribute(Attribute::create<int32_t>(ctx, "a"));

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_array() {
  Context ctx;

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);

  // Global order enables writes in stages to a single fragment
  // but requires input to match global order
  query.set_layout(TILEDB_GLOBAL_ORDER);

  // First write
  char rows_1[] = {"barbazcorgefoo"};
  std::vector<uint64_t> rows_offsets_1 = {0, 3, 6, 11};
  std::vector<int32_t> cols_1 = {1, 2, 3, 4};
  std::vector<int32_t> a_1 = {3, 3, 5, 3};

  query.set_data_buffer("a", a_1)
      .set_data_buffer("rows", &rows_1[0], sizeof(rows_1))
      .set_offsets_buffer("rows", rows_offsets_1)
      .set_data_buffer("cols", cols_1);
  query.submit();

  // Second write
  char rows_2[] = {"garplygraultgubquux"};
  std::vector<uint64_t> rows_offsets_2 = {0, 6, 12, 15};
  std::vector<int32_t> cols_2 = {1, 2, 3, 4};
  std::vector<int32_t> a_2 = {6, 6, 3, 4};

  query.set_data_buffer("a", a_2)
      .set_data_buffer("rows", &rows_2[0], sizeof(rows_2))
      .set_offsets_buffer("rows", rows_offsets_2)
      .set_data_buffer("cols", cols_2);
  query.submit();

  // Finalize the write (IMPORTANT) and close the array.
  query.finalize();
  array.close();
}

void print_cells(
    uint64_t result_num,
    uint64_t* rows_offsets,
    uint64_t rows_data_size,
    char* rows_data,
    int32_t* cols_data,
    int32_t* a_data) {
  for (uint64_t r = 0; r < result_num; r++) {
    // For strings we must compute the length based on the offsets
    uint64_t row_start = rows_offsets[r];
    uint64_t row_end =
        r == result_num - 1 ? rows_data_size : rows_offsets[r + 1];
    const int row_value_size = row_end - row_start;
    const char* row_value = &rows_data[row_start];

    const int32_t col_value = cols_data[r];
    const int32_t a_value = a_data[r];
    printf(
        "Cell (%.*s, %i) has data %d\n",
        row_value_size,
        row_value,
        col_value,
        a_value);
  }
}

void read_array() {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Attribute/dimension buffeers
  // (unknown number of cells, buffer sizes are estimates;
  // query may be read in multiple stages)
  constexpr size_t NUM_CELLS = 2;
  std::vector<char> rows_data(NUM_CELLS * 16);
  std::vector<uint64_t> rows_offsets(NUM_CELLS);
  std::vector<int32_t> cols_data(NUM_CELLS);
  std::vector<int32_t> a_data(NUM_CELLS);

  // Aggregate result buffers (1 cell each of unknown size)
  constexpr size_t MAX_RESULT_LENGTH = 64;
  std::vector<char> min_value(MAX_RESULT_LENGTH);
  std::vector<uint64_t> min_offsets(1);
  std::vector<char> max_value(MAX_RESULT_LENGTH);
  std::vector<uint64_t> max_offsets(1);

  // Create a query
  Query query(ctx, array);

  // Query cells with a >= 4
  QueryCondition qc(ctx);
  int32_t a_lower_bound = 4;
  qc.init("a", &a_lower_bound, sizeof(int32_t), TILEDB_GE);
  query.set_condition(qc);

  // Add aggregates for min(rows) and max(rows) on the default channel.
  QueryChannel default_channel = QueryExperimental::get_default_channel(query);
  ChannelOperation min_rows =
      QueryExperimental::create_unary_aggregate<MinOperator>(query, "rows");
  default_channel.apply_aggregate("Min(rows)", min_rows);
  ChannelOperation max_rows =
      QueryExperimental::create_unary_aggregate<MaxOperator>(query, "rows");
  default_channel.apply_aggregate("Max(rows)", max_rows);

  // Set layout and buffers.
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("rows", rows_data)
      .set_offsets_buffer("rows", rows_offsets)
      .set_data_buffer("cols", cols_data)
      .set_data_buffer("a", a_data)
      .set_data_buffer("Min(rows)", min_value)
      .set_offsets_buffer("Min(rows)", min_offsets)
      .set_data_buffer("Max(rows)", max_value)
      .set_offsets_buffer("Max(rows)", max_offsets);

  auto print_current_cells = [&]() {
    print_cells(
        query.result_buffer_elements()["rows"].first,
        &rows_offsets[0],
        query.result_buffer_elements()["rows"].second,
        &rows_data[0],
        &cols_data[0],
        &a_data[0]);
  };

  // Submit the query and close the array.
  while (query.submit() == Query::Status::INCOMPLETE) {
    const size_t num_results = query.result_buffer_elements()["rows"].first;

    // NB: this is not generically a valid assertion
    // (see reading_incomplete.cc)
    // but is true by construction in this example
    assert(num_results > 0);

    print_current_cells();
  }
  array.close();

  print_current_cells();

  // Print out the results.
  const size_t min_value_size =
      query.result_buffer_elements()["Min(rows)"].second;
  const size_t max_value_size =
      query.result_buffer_elements()["Max(rows)"].second;
  std::cout << "Min(rows) = " << std::string(&min_value[0], min_value_size)
            << std::endl;
  std::cout << "Max(rows) = " << std::string(&max_value[0], max_value_size)
            << std::endl;
}

int main() {
  create_array();
  write_array();
  read_array();
  return 0;
}
