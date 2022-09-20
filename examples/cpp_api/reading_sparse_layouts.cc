/**
 * @file   reading_sparse_layouts.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
 * When run, this program will create a simple 2D sparse array, write some data
 * to it, and read a slice of the data back in the layout of the user's choice
 * (passed as an argument to the program: "row", "col", or "global").
 */

#include <iostream>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

template <typename T>
void print_buffer(const std::string & name, const std::vector<T> & buffer) {
  std::cout << name << ": ";
  for (size_t i = 0; i < buffer.size(); i++) {
    std::cout << buffer[i];
    if (i < buffer.size() - 1) std::cout << ", ";
    else std::cout << std::endl;
  }
}

void read_array(const Context& ctx, const std::string& array_name) {
  std::vector<int32_t> a1_read(16);

  Array array_read(ctx, array_name, TILEDB_READ);
  Subarray subarray(ctx, array_read);
  subarray.add_range("rows", 1, 4);
  subarray.add_range("cols", 1, 4);

  Query query_read(ctx, array_read);
  query_read.set_layout(TILEDB_UNORDERED);
  query_read.set_data_buffer("a1", a1_read)
      .set_subarray(subarray);
  query_read.submit();

  assert(query_read.query_status() == tiledb::Query::Status::COMPLETE);
  print_buffer("a1", a1_read);
}

int main() {
  std::string array_name = "array-schema-evolution";
  Context ctx;

  VFS vfs(ctx);
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int32_t>(ctx, "rows", {{1, 4}}));
  domain.add_dimension(Dimension::create<int32_t>(ctx, "cols", {{1, 4}}));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  Attribute a1 = Attribute::create<int32_t>(ctx, "a1");
  schema.add_attribute(a1);
  schema.check();
  Array::create(array_name, schema);

  // Write to array
  {
    std::vector<int32_t> rows_data = { 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4 };
    std::vector<int32_t> cols_data = { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 };
    std::vector<int32_t> a1_data = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };

    Array array_write(ctx, array_name, TILEDB_WRITE);
    Query query_write(ctx, array_write);
    query_write.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a1", a1_data)
        .set_data_buffer("rows", rows_data)
        .set_data_buffer("cols", cols_data);
    query_write.submit();

    assert(query_write.query_status() == tiledb::Query::Status::COMPLETE);

    // Read array
    read_array(ctx, array_name);

    // Evolve array
    ArraySchemaEvolution se(ctx);
    double val = -1.0;
    auto attr = Attribute::create<double>(ctx, "a2")
        .set_fill_value(&val, sizeof(val));
    se.add_attribute(attr);
    se.array_evolve(array_name);
  }


  // Write to array at (1, 1)
  {
    std::vector<int32_t> rows_data = { 1 };
    std::vector<int32_t> cols_data = { 1 };
    std::vector<int32_t> a1_data = { 1 };
    std::vector<double> a2_data = { 1.0 };

    Array array_write(ctx, array_name, TILEDB_WRITE);
    Query query_write(ctx, array_write);
    query_write.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a1", a1_data)
        .set_data_buffer("a2", a2_data)
        .set_data_buffer("rows", rows_data)
        .set_data_buffer("cols", cols_data);
    query_write.submit();

    assert(query_write.query_status() == tiledb::Query::Status::COMPLETE);
  }

  {
    Array array_read(ctx, array_name, TILEDB_READ);
    ArraySchema s(ctx, array_name);

    std::vector<int32_t> a1_read(16);
    std::vector<double> a2_read(16);

    Subarray subarray(ctx, array_read);
    subarray.add_range("rows", 1, 4);
    subarray.add_range("cols", 1, 4);

    Query query_read(ctx, array_read);
    query_read.set_layout(TILEDB_UNORDERED);
    query_read.set_subarray(subarray)
        .set_data_buffer("a1", a1_read)
        .set_data_buffer("a2", a2_read);
    query_read.submit();

    assert(tiledb::Query::Status::COMPLETE == query_read.query_status());

    print_buffer("a1", a1_read);
    print_buffer("a2", a2_read);
  }
}
