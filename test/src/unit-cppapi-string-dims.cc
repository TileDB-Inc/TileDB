/**
 * @file   unit-cppapi-string-dims.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB Inc.
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
 * Tests the C++ API with string dimensions.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

TEST_CASE(
    "C++ API: Test infinite string splits",
    "[cppapi][string-dim][infinite-split]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Define a sparse, 2D array where the first dimension is a string. We will
  // test a read query that would cause an infinite loop splitting string
  // dimensions if not for our fixed limit
  // `constants::max_string_dim_split_depth`.
  Domain domain(ctx);
  domain
      .add_dimension(
          Dimension::create(ctx, "dim1", TILEDB_STRING_ASCII, nullptr, nullptr))
      .add_dimension(Dimension::create<int32_t>(ctx, "dim2", {{0, 9}}, 10));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int32_t>(ctx, "a1"));
  Array::create(array_name, schema);

  // Write data to the array.
  std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'a', 'b', 'b', 'c'};
  std::vector<uint64_t> dim1_offsets = {0, 1, 3, 4, 5, 7};
  std::vector<int32_t> dim2 = {1, 1, 1, 2, 2, 2};
  std::vector<int32_t> a1_data = {1, 2, 3, 4, 5, 6};
  Array array_write(ctx, array_name, TILEDB_WRITE);
  Query query_write(ctx, array_write, TILEDB_WRITE);
  query_write.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a1", a1_data)
      .set_data_buffer("dim1", dim1)
      .set_offsets_buffer("dim1", dim1_offsets)
      .set_data_buffer("dim2", dim2);

  // Perform the write and close the array.
  query_write.submit();
  array_write.close();

  // Prepare a read query.
  Array array_read(ctx, array_name, TILEDB_READ);
  Query query_read(ctx, array_read, TILEDB_READ);
  auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
  auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);
  query_read.add_range(
      0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
  query_read.add_range<int32_t>(
      1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);

  // Prepare buffers with small enough sizes to ensure the string dimension
  // must split.
  a1_data = std::vector<int32_t>(2);
  dim1 = std::vector<char>(8);
  dim1_offsets = std::vector<uint64_t>(1);
  dim2 = std::vector<int32_t>(1);

  query_read.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", a1_data)
      .set_data_buffer("dim1", dim1)
      .set_offsets_buffer("dim1", dim1_offsets)
      .set_data_buffer("dim2", dim2);

  // Submit the query and ensure it does not hang on an infinite loop.
  tiledb::Query::Status status = tiledb::Query::Status::UNINITIALIZED;
  do {
    status = query_read.submit();
  } while (status == tiledb::Query::Status::INCOMPLETE);

  array_read.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test default string dimensions",
    "[cppapi][string-dim][default]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Define a sparse, 3D array where the first and third dimension is a string.
  Domain domain(ctx);
  domain
      .add_dimension(
          Dimension::create(ctx, "dim1", TILEDB_STRING_ASCII, nullptr, nullptr))
      .add_dimension(Dimension::create<int32_t>(ctx, "dim2", {{0, 9}}, 10))
      .add_dimension(Dimension::create(
          ctx, "dim3", TILEDB_STRING_ASCII, nullptr, nullptr));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int32_t>(ctx, "a1"));
  Array::create(array_name, schema);

  // Write data to the array.
  std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'a', 'b', 'b', 'c'};
  std::vector<uint64_t> dim1_offsets = {0, 1, 3, 4, 5, 7};
  std::vector<int32_t> dim2 = {1, 1, 1, 2, 2, 2};
  std::vector<char> dim3 = {'g', 'h', 'h', 'i', 'j', 'k', 'k', 'l'};
  std::vector<uint64_t> dim3_offsets = {0, 1, 3, 4, 5, 7};
  std::vector<int32_t> a1_data = {1, 2, 3, 4, 5, 6};
  Array array_write(ctx, array_name, TILEDB_WRITE);
  Query query_write(ctx, array_write, TILEDB_WRITE);
  query_write.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a1", a1_data)
      .set_data_buffer("dim1", dim1)
      .set_offsets_buffer("dim1", dim1_offsets)
      .set_data_buffer("dim2", dim2)
      .set_data_buffer("dim3", dim3)
      .set_offsets_buffer("dim3", dim3_offsets);

  // Perform the write and close the array.
  query_write.submit();
  array_write.close();

  // Prepare a read query. Do not set a range for the last string dimension.
  Array array_read(ctx, array_name, TILEDB_READ);
  Query query_read(ctx, array_read, TILEDB_READ);
  auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
  auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);
  query_read.add_range(
      0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
  query_read.add_range<int32_t>(
      1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);

  // Prepare buffers.
  a1_data = std::vector<int32_t>(10);
  dim1 = std::vector<char>(10);
  dim1_offsets = std::vector<uint64_t>(10);
  dim2 = std::vector<int32_t>(10);
  dim3 = std::vector<char>(10);
  dim3_offsets = std::vector<uint64_t>(10);

  query_read.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", a1_data)
      .set_data_buffer("dim1", dim1)
      .set_offsets_buffer("dim1", dim1_offsets)
      .set_data_buffer("dim2", dim2)
      .set_data_buffer("dim3", dim3)
      .set_offsets_buffer("dim3", dim3_offsets);

  query_read.submit();
  REQUIRE(query_read.query_status() == Query::Status::COMPLETE);

  auto result_buffers = query_read.result_buffer_elements();
  auto result_num = result_buffers["a1"].second;

  REQUIRE(result_num == 6);

  array_read.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test default string dimensions with partitioning",
    "[cppapi][string-dim][default][partitioning]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Define a sparse, 3D array where the first and third dimension is a string.
  Domain domain(ctx);
  domain
      .add_dimension(
          Dimension::create(ctx, "dim1", TILEDB_STRING_ASCII, nullptr, nullptr))
      .add_dimension(Dimension::create<int32_t>(ctx, "dim2", {{0, 9}}, 10))
      .add_dimension(Dimension::create(
          ctx, "dim3", TILEDB_STRING_ASCII, nullptr, nullptr));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int32_t>(ctx, "a1"));
  Array::create(array_name, schema);

  // Write data to the array.
  std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'a', 'b', 'b', 'c'};
  std::vector<uint64_t> dim1_offsets = {0, 1, 3, 4, 5, 7};
  std::vector<int32_t> dim2 = {1, 1, 1, 2, 2, 2};
  std::vector<char> dim3 = {'g', 'h', 'h', 'i', 'j', 'k', 'k', 'l'};
  std::vector<uint64_t> dim3_offsets = {0, 1, 3, 4, 5, 7};
  std::vector<int32_t> a1_data = {1, 2, 3, 4, 5, 6};
  Array array_write(ctx, array_name, TILEDB_WRITE);
  Query query_write(ctx, array_write, TILEDB_WRITE);
  query_write.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a1", a1_data)
      .set_data_buffer("dim1", dim1)
      .set_offsets_buffer("dim1", dim1_offsets)
      .set_data_buffer("dim2", dim2)
      .set_data_buffer("dim3", dim3)
      .set_offsets_buffer("dim3", dim3_offsets);

  // Perform the write and close the array.
  query_write.submit();
  array_write.close();

  // Prepare a read query. Do not set a range for the last string dimension.
  Array array_read(ctx, array_name, TILEDB_READ);
  Query query_read(ctx, array_read, TILEDB_READ);
  auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
  auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);
  query_read.add_range(
      0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
  query_read.add_range<int32_t>(
      1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);

  // Prepare buffers with small enough sizes to ensure the query must split.
  a1_data = std::vector<int32_t>(3);
  dim1 = std::vector<char>(10);
  dim1_offsets = std::vector<uint64_t>(10);
  dim2 = std::vector<int32_t>(10);
  dim3 = std::vector<char>(10);
  dim3_offsets = std::vector<uint64_t>(10);

  query_read.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", a1_data)
      .set_data_buffer("dim1", dim1)
      .set_offsets_buffer("dim1", dim1_offsets)
      .set_data_buffer("dim2", dim2)
      .set_data_buffer("dim3", dim3)
      .set_offsets_buffer("dim3", dim3_offsets);

  query_read.submit();
  REQUIRE(query_read.query_status() == Query::Status::INCOMPLETE);

  auto result_buffers = query_read.result_buffer_elements();
  auto result_num = result_buffers["a1"].second;

  REQUIRE(result_num == 2);

  array_read.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}