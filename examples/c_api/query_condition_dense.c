/**
 * @file   query_condition_dense.c
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
 * When run, this program will create a dense 1D array with 2 attributes. It
 * will then run queries with different query conditions to demonstrate how
 * query conditions can be used to filter out results in TileDB arrays.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>


// Name of array.
const char *array_name = "query_condition_dense_array";
int32_t num_elems = 10;

/**
 * @brief Function to print the values of all the attributes for one
 * index of this array.
 *
 * @param a Attribute a's value.
 * @param b Attribute b's value.
 */
void print_elem(int *a, char *b) {
  if (a == NULL) {
    printf("{null, %s}\n", b);
  } else {
    printf("{%d, %s}\n", *a, b);
  }
}

/**
 * @brief Function to create the TileDB array used in this example.
 * The array will be 1D with size 1 with dimension "index".
 * The bounds on the index will be 0 through 9, inclusive.
 *
 * The array has two attributes. The two attributes are
 *  - "a" (type int)
 *  - "b" (type std::string)
 *
 * @param ctx The context.
 */
void create_array(tiledb_ctx_t *ctx) {
  // Creating the dimension and the domain.
  tiledb_dimension_t *dimension;
  int dim_domain[] = {0, num_elems - 1};
  int tile_extents[] = {1};
  tiledb_dimension_alloc(ctx, "index", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &dimension);

  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx, &domain);
  tiledb_domain_add_dimension(ctx, domain, dimension);

  tiledb_array_schema_t *schema;
  tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &schema);

  // The array will be dense.
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR}});

  // Adding the attributes of the array to the array schema.
  Attribute a = Attribute::create<int>(ctx, "a").set_nullable(true);
  Attribute b = Attribute::create<std::string>(ctx, "b");
  Attribute c = Attribute::create<int32_t>(ctx, "c").set_fill_value(
      &c_fill_value, sizeof(int32_t));
  Attribute d = Attribute::create<float>(ctx, "d").set_fill_value(
      &d_fill_value, sizeof(float));
  schema.add_attribute(a).add_attribute(b).add_attribute(c).add_attribute(d);

  // Create the (empty) array.
  Array::create(array_name, schema);
}

/**
 * @brief Execute a write on array query_condition_sparse array
 * which then stores the following data in the array. The table
 * is organized by dimension/attribute.
 *
 * index |  a   |   b   
 * ---------------------
 *   0   | null | alice
 *   1   | 2    | bob 
 *   2   | null | craig
 *   3   | 4    | dave
 *   4   | null | erin
 *   5   | 6    | frank
 *   6   | null | grace
 *   7   | 8    | heidi
 *   8   | null | ivan
 *   9   | 10   | judy
 *
 * @param ctx The context.
 */
void write_array(tiledb_ctx_t *ctx) {
  /*
  // Create data buffers that store the values to be written in.
  std::vector<int32_t> a_data = {0, 2, 0, 4, 0, 6, 0, 8, 0, 10};
  std::vector<uint8_t> a_data_validity = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
  std::vector<std::string> b_strs = {"alice",
                                     "bob",
                                     "craig",
                                     "dave",
                                     "erin",
                                     "frank",
                                     "grace",
                                     "heidi",
                                     "ivan",
                                     "judy"};
  std::string b_data = "";
  std::vector<uint64_t> b_data_offsets;
  for (const auto& elem : b_strs) {
    b_data_offsets.push_back(b_data.size());
    b_data += elem;
  }
  std::vector<int32_t> c_data = {0, 0, 0, 0, 0, 0, 1, 2, 3, 4};
  std::vector<float> d_data = {
      4.1, 3.4, 5.6, 3.7, 2.3, 1.7, 3.8, 4.9, 3.2, 3.1};

  // Execute the write query.
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_data)
      .set_validity_buffer("a", a_data_validity)
      .set_data_buffer("b", b_data)
      .set_offsets_buffer("b", b_data_offsets)
      .set_data_buffer("c", c_data)
      .set_data_buffer("d", d_data);

  query_w.submit();
  query_w.finalize();
  array_w.close();
  */
}

/**
 * @brief Executes the read query for the array created in write_array.
 *
 * @param ctx The context.
 * @param qc The query condition to execute the query with.
 */
void read_array_with_qc(tiledb_ctx_t *ctx, tiledb_query_condition_t *qc) {
  /*
  // Create data buffers to read the values into.
  std::vector<int> a_data(num_elems);
  std::vector<uint8_t> a_data_validity(num_elems);

  // We initialize the string b_data to contain 44 characters because
  // that is the combined size of all strings in attribute b.
  std::string b_data;
  b_data.resize(44);

  std::vector<uint64_t> b_data_offsets(num_elems);
  std::vector<int32_t> c_data(num_elems);
  std::vector<float> d_data(num_elems);

  // Execute the read query.
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_data)
      .set_validity_buffer("a", a_data_validity)
      .set_data_buffer("b", b_data)
      .set_offsets_buffer("b", b_data_offsets)
      .set_data_buffer("c", c_data)
      .set_data_buffer("d", d_data)
      .add_range("index", 0, num_elems - 1);
  if (qc) {
    query.set_condition(qc.value());
  }

  query.submit();

  // Collect the results of the read query. The number of elements
  // the filtered array contains is in num_elements_result.
  // The length of the filtered substring of all the data is in
  // b_data, and all the offsets for filtered individual elements
  // are in  b_data_offsets.
  auto table = query.result_buffer_elements_nullable();
  size_t num_elements_result = std::get<1>(table["c"]);
  uint64_t b_str_length = std::get<1>(table["b"]);
  b_data_offsets.push_back(b_str_length);

  // Here we print all the elements that are returned by the query.
  for (size_t i = 0; i < num_elements_result; ++i) {
    // We can filter results based on whether the fill value is used in the
    // result buffers.
    if (c_data[i] != c_fill_value) {
      // We pass in nullopt if the data is invalid, per the validity buffer.
      std::optional<int> a_val;
      if (a_data_validity[i]) {
        a_val = a_data[i];
      } else {
        a_val = std::nullopt;
      }
      print_elem(
          a_val,
          b_data.substr(
              b_data_offsets[i], b_data_offsets[i + 1] - b_data_offsets[i]),
          c_data[i],
          d_data[i]);
    }
  }

  query.finalize();
  array.close();
  */
}

int main() {
  // Create the context.
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, NULL, &vfs);

  int32_t is_dir = 0;
  tiledb_vfs_is_dir(ctx, vfs, array_name, &is_dir);
  if (is_dir) {
    tiledb_vfs_remove_dir(ctx, vfs, array_name);
  }

  // Create and write data to the array.
  create_array(ctx);
  write_array(ctx);

  // Printing the entire array.
  printf("Printing the entire array...\n");
  read_array_with_qc(ctx, NULL);
  printf("\n");

  // Execute a read query with query condition `a = null`.
  printf("Running read query with query condition `a = null`...\n");
  tiledb_query_condition_t *qc;
  tiledb_query_condition_alloc(ctx, &qc);
  tiledb_query_condition_init(ctx, qc, "a", NULL, 0, TILEDB_EQ);
  read_array_with_qc(ctx, qc);
  printf("\n");

  // Execute a read query with query condition `b < "eve"`.
  printf("Running read query with query condition `b < \"eve\"`...\n");
  tiledb_query_condition_t *qc1;
  tiledb_query_condition_alloc(ctx, &qc1);
  const char *eve = "eve";
  tiledb_query_condition_init(ctx, qc1, "b", eve, strlen(eve), TILEDB_LT);
  read_array_with_qc(ctx, qc1);
  printf("\n");

  // Execute a read query with query condition `a != null AND b < \"eve\"`.
  printf("Running read query with query condition a != null AND b < \"eve\"`...\n");
  tiledb_query_condition_t *qc2;
  tiledb_query_condition_alloc(ctx, &qc2);
  tiledb_query_condition_combine(ctx, qc, qc1, TILEDB_AND, &qc2);
  read_array_with_qc(ctx, qc2);
  printf("\n");

  is_dir = 0;
  tiledb_vfs_is_dir(ctx, vfs, array_name, &is_dir);
  if (is_dir) {
    tiledb_vfs_remove_dir(ctx, vfs, array_name);
  }

  return 0;
}