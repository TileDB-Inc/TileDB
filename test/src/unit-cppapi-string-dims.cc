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

#pragma optimize("",off)
std::vector<std::string> dataAndOffsetToStrings(
    Query query,
    std::string col,
    std::vector<uint64_t> offsets,
    std::string data) {
  // Get the string sizes
  auto result_el_map = query.result_buffer_elements();
  auto result_el_off = result_el_map[col].first;
  std::vector<uint64_t> str_sizes;
#if 0
  auto nitems = result_el_off;
  for (uint64_t pos = 0; pos <= result_el_off; ++pos) {
    auto c_offset = offsets[pos];
    auto c_size = (pos < nitems - 1) ? offsets[pos + 1] - c_offset :
                                      data.size() - c_offset;
    str_sizes.push_back(c_size);
  }
#else
  if (result_el_off > 1) {
    for (size_t i = 0; i < result_el_off - 1; ++i) {
      if (i >= offsets.size()) {
        std::cout << "i " << i << ", offsets.size() " << offsets.size();
        __debugbreak();
      }
      str_sizes.push_back(offsets[i + 1] - offsets[i]);
      if (str_sizes.size() > data.size()) {
        std::cout << "str_sizes.size() " << str_sizes.size() << ", data.size() " << data.size();
        __debugbreak();
      }
    }
  }
  auto result_data_size = result_el_map[col].second * sizeof(char);
  str_sizes.push_back(result_data_size - offsets[result_el_off - 1]);
#endif

  if (offsets.size() > data.size()) {
    std::cout << "result_el_off " << result_el_off << ", data.size() "
              << data.size() << std::endl;
    __debugbreak();
  }

  // Get the strings
  std::vector<std::string> str;
  for (size_t i = 0; i < result_el_off; ++i)
    str.push_back(std::string(&data[offsets[i]], str_sizes[i]));
  return str;
}

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
  // std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'a', 'b', 'b', 'c'};
  std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'd', 'e', 'e', 'f'};
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

  auto perform_read = [&](int option=5) {
    // Prepare a read query. Do not set a range for the last string dimension.
    Array array_read(ctx, array_name, TILEDB_READ);
    Query query_read(ctx, array_read, TILEDB_READ);
    auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
    auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);
    auto dim3_non_empty_domain = array_read.non_empty_domain_var(2);

    int expected_result_num = -1;

    // Each case section has the same .add_range possibilities,
    // some active some inactive, to make visual comparison of what's
    // different between them a bit easier, at least with some colorizing
    // editors.
    switch (option) {
      case 1:  // reported to have failed
        expected_result_num = 4;
        // query_read.add_range(0, dim1_non_empty_domain.first,
        //   dim1_non_empty_domain.second); 
        // query_read.add_range(0,
        //   std::string("c"), std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(1, dim2_non_empty_domain.first,
        //   dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"), std::string("i"),
        //   std::string("kl"));
        break;
      case 2:  // reported to have succeeded
        expected_result_num = 2;
        // query_read.add_range(0, dim1_non_empty_domain.first,
        //   dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"),
        //   std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
      case 3:  // reported to have succeeded
        expected_result_num = 6;
        query_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"),
        //    std::string("d"));
        // query_read.add_range(
        //    std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //   std::string("i"), std::string("kl"));
        break;
      case 4:
        expected_result_num = 2;
        // reported to have failed - 'cept seems to be same as case 3 that
        // succeeded
        // query_read.add_range(0, dim1_non_empty_domain.first,
        //  dim1_non_empty_domain.second);
        query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //   std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(1, dim2_non_empty_domain.first,
        //  dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //  std::string("i"), std::string("kl"));
        break;
      case 5:  // maybe intended... reported to have failed
        expected_result_num = 2;
        // query_read.add_range(
        //     0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //   std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //    std::string("i"), std::string("kl"));
        break;
      case 6:  // maybe intended... reported to have failed
        expected_result_num = 4;
        // query_read.add_range(
        //    0, dim1_non_empty_domain.first,
        //    dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"),
        //  std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //  std::string("i"), std::string("kl"));
        break;
      case 7:  // maybe intended... reported to have failed
        expected_result_num = 2;
        // query_read.add_range(
        //    0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"), std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(
        //    1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
      case 8:  // maybe intended... reported to have failed
        expected_result_num = 1;
        // query_read.add_range(
        //    0, dim1_non_empty_domain.first,
        //    dim1_non_empty_domain.second);
        query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //    std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(
        //    1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
      case 9:  // maybe intended... reported to have failed
        expected_result_num = 3;
        query_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //    std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(
        //    1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
      case 10:  // maybe intended... reported to have failed
        expected_result_num = 3;
        // query_read.add_range(
        //     0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"), std::string("d"));
        query_read.add_range(0, std::string("c"), std::string("ee"));
        // query_read.add_range(
        //    std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(
        //    1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kk"));
        break;
    }

      // Prepare buffers.
      a1_data = std::vector<int32_t>(10);
      dim1 = std::vector<char>(10);
      //std::string dim1;
      //dim1.resize(10);
      dim1_offsets = std::vector<uint64_t>(10);
      dim2 = std::vector<int32_t>(10);
      dim3 = std::vector<char>(10);
      //std::string dim3;
      //dim3.resize(10);
      dim3_offsets = std::vector<uint64_t>(10);

      query_read.set_layout(TILEDB_ROW_MAJOR)
          .set_data_buffer("a1", a1_data)
          .set_data_buffer("dim1", dim1)
          .set_offsets_buffer("dim1", dim1_offsets)
          //.set_buffer("dim1", dim1_offsets, dim1)
          .set_data_buffer("dim2", dim2)
          .set_data_buffer("dim3", dim3)
          .set_offsets_buffer("dim3", dim3_offsets);
          //.set_buffer("dim3", dim3_offsets, dim3);

      query_read.submit();
      REQUIRE(query_read.query_status() == Query::Status::COMPLETE);

      auto result_buffers = query_read.result_buffer_elements();
      auto result_num = result_buffers["a1"].second;

//      REQUIRE(result_num == expected_result_num);

    auto show_data = [&]() {
        std::string dim1s(&dim1[0], dim1.size());
        std::string dim3s(&dim3[0], dim3.size());
        std::vector<std::string> d1 =
            dataAndOffsetToStrings(query_read, "dim1", dim1_offsets, dim1s);
        std::vector<std::string> d3 =
            dataAndOffsetToStrings(query_read, "dim3", dim3_offsets, dim3s);
        for (auto i = 0; i < result_num; i++)
          std::cout << d1[i] << "\t" << dim2[i] << "\t" << d3[i] << "\t"
                    << a1_data[i] << std::endl;
      };
    std::cout << "option " << option << ", num results " << result_num << std::endl;
    show_data();

    array_read.close();
  } ;
  for (auto i = 0 ; i < 10; ++i)
    perform_read(i+1);

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

  auto perform_read = [&](int option) {
    // Prepare a read query. Do not set a range for the last string dimension.
    Array array_read(ctx, array_name, TILEDB_READ);
    Query query_read(ctx, array_read, TILEDB_READ);
    auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
    auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);

    decltype(query_read.query_status()) initial_expected_read_status =
        tiledb::Query::Status::UNINITIALIZED;
    int expected_result_num = -1;
    int initial_result_num = 2;

    // Each case section has the same .add_range possibilities,
    // some active some inactive, to make visual comparison of what's
    // different between them a bit easier, at least with some colorizing editors.
    switch (option) {
      case 1:  // reported to have failed
        expected_result_num = 4;
        initial_expected_read_status = Query::Status::INCOMPLETE;
        // query_read.add_range(0, dim1_non_empty_domain.first,
        //   dim1_non_empty_domain.second); 
        // query_read.add_range(0,
        //   std::string("c"), std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(1, dim2_non_empty_domain.first,
        //   dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"), std::string("i"),
        //   std::string("kl"));
        break;
      case 2:  // reported to have succeeded
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        // query_read.add_range(0, dim1_non_empty_domain.first,
        //   dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"),
        //   std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
      case 3:  // reported to have succeeded
        expected_result_num = 6;
        initial_expected_read_status = Query::Status::INCOMPLETE;
        query_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"),
        //    std::string("d"));
        // query_read.add_range(
        //    std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //   std::string("i"), std::string("kl"));
        break;
      case 4:
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        // reported to have failed - 'cept seems to be same as case 3 that
        // succeeded
        // query_read.add_range(0, dim1_non_empty_domain.first,
        //  dim1_non_empty_domain.second);
        query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //   std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(1, dim2_non_empty_domain.first,
        //  dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //  std::string("i"), std::string("kl"));
        break;
      case 5:  // maybe intended... reported to have failed
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        // query_read.add_range(
        //     0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //   std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //    std::string("i"), std::string("kl"));
        break;
      case 6:  // maybe intended... reported to have failed
        expected_result_num = 4;
        initial_expected_read_status = Query::Status::INCOMPLETE;
        // query_read.add_range(
        //    0, dim1_non_empty_domain.first,
        //    dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"),
        //   std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //   std::string("i"), std::string("kl"));
        break;
      case 7:  // maybe intended... reported to have failed
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        // query_read.add_range(
        //    0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"), std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(
        //    1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
      case 8:  // maybe intended... reported to have failed
        expected_result_num = 1;
        initial_result_num = 1;
        initial_expected_read_status = Query::Status::COMPLETE;
        // query_read.add_range(
        //    0, dim1_non_empty_domain.first,
        //    dim1_non_empty_domain.second);
        query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //    std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(
        //    1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
      case 9:  // maybe intended... reported to have failed
        expected_result_num = 3;
        initial_result_num = 3;
        initial_expected_read_status = Query::Status::COMPLETE;
        query_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //    std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(
        //    1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
    }

    // try for bufcnt expected to force split.
    // initial_result_num is initial expected number of results to be returned
    // if bufcnt == 3.
    int bufcnt = initial_result_num > 1 ? initial_result_num - 1 : 1;
    a1_data = std::vector<int32_t>(bufcnt);
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

    auto result_buffers = query_read.result_buffer_elements();
    auto result_num = result_buffers["a1"].second;
    std::cout << "option " << option << ", expected status "
              << initial_expected_read_status 
              << " current read_status() "
              << query_read.query_status()
              << ", (initial) result_num " << result_num 
              << ", final expected_result_num " << expected_result_num << std::endl;

    decltype(query_read.query_status()) what_status =
        bufcnt < initial_result_num ? Query::Status::INCOMPLETE :
        initial_expected_read_status;
    REQUIRE(query_read.query_status() == what_status);

#if 01
    int expect_what =
        bufcnt < initial_result_num ? bufcnt : initial_result_num;
    REQUIRE(result_num == expect_what);
#else
    //TBD: is "a1" always in any returned results?
//    auto result_buffers = query_read.result_buffer_elements();
//    auto result_num = result_buffers["a1"].second;

    REQUIRE(result_num == initial_result_num);
#endif

    int tot_result_num = 0;
    while (query_read.query_status() == Query::Status::INCOMPLETE) {
      tot_result_num += result_num;
      query_read.submit();
      auto result_buffers = query_read.result_buffer_elements();
      result_num = result_buffers["a1"].second;
    }
    tot_result_num += result_num;
    std::cout << "tot_result_num " << tot_result_num << std::endl;
    REQUIRE(query_read.query_status() == Query::Status::COMPLETE);
    REQUIRE(tot_result_num == expected_result_num);

    array_read.close();
  };
  for (auto i = 0; i < 9; ++i)
    perform_read(i + 1);


  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test default string dimensions with partitioning - dups",
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
//  schema.set_allows_dups(true); //TBD: is default 'false'?
  Array::create(array_name, schema);

  // Write data to the array.
  //succeeds with following with and without dups
  std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'a', 'b', 'b', 'c'};
  // fails with 'dups', fails without dups... 
  //well ok, produces different number of results...
  //std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'd', 'e', 'e', 'f'};
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

  auto perform_read = [&](int option) {
    // Prepare a read query. Do not set a range for the last string dimension.
    Array array_read(ctx, array_name, TILEDB_READ);
    Query query_read(ctx, array_read, TILEDB_READ);
    auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
    auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);

    decltype(query_read.query_status()) initial_expected_read_status =
        tiledb::Query::Status::UNINITIALIZED;
    int expected_result_num = -1;
    int initial_result_num = 2;

    // Each case section has the same .add_range possibilities, 
    // some active some inactive, to make visual comparison of what's
    // different between them a bit easier, at least with some colorizing editors.
    switch (option) {
      case 1:  // reported to have failed
        expected_result_num = 4;
        initial_expected_read_status = Query::Status::INCOMPLETE;
        // query_read.add_range(0, dim1_non_empty_domain.first,
        //   dim1_non_empty_domain.second); 
        // query_read.add_range(0,
        //   std::string("c"), std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(1, dim2_non_empty_domain.first,
        //   dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"), std::string("i"),
        //   std::string("kl"));
        break;
      case 2:  // reported to have succeeded
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        // query_read.add_range(0, dim1_non_empty_domain.first,
        //   dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"),
        //   std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
      case 3:  // reported to have succeeded
        expected_result_num = 6;
        initial_expected_read_status = Query::Status::INCOMPLETE;
        query_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"),
        //    std::string("d"));
        // query_read.add_range(
        //    std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //   std::string("i"), std::string("kl"));
        break;
      case 4:
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        // reported to have failed - 'cept seems to be same as case 3 that
        // succeeded
        // query_read.add_range(0, dim1_non_empty_domain.first,
        //   dim1_non_empty_domain.second);
        query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //   std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(1, dim2_non_empty_domain.first,
        //   dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //   std::string("i"), std::string("kl"));
        break;
      case 5:  // maybe intended... reported to have failed
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        // query_read.add_range(
        //     0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //   std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //    std::string("i"), std::string("kl"));
        break;
      case 6:  // maybe intended... reported to have failed
        expected_result_num = 4;
        initial_expected_read_status = Query::Status::INCOMPLETE;
        // query_read.add_range(
        //    0, dim1_non_empty_domain.first,
        //    dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"),
        //  std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        query_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        // query_read.add_range(std::string("dim3"),
        //  std::string("i"), std::string("kl"));
        break;
      case 7:  // maybe intended... reported to have failed
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        // query_read.add_range(
        //    0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"), std::string("d"));
        query_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(
        //    1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
      case 8:  // maybe intended... reported to have failed
        expected_result_num = 1;
        initial_result_num = 1;
        initial_expected_read_status = Query::Status::COMPLETE;
        // query_read.add_range(
        //    0, dim1_non_empty_domain.first,
        //    dim1_non_empty_domain.second);
        query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //    std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(
        //    1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
      case 9:  // maybe intended... reported to have failed
        expected_result_num = 3;
        initial_result_num = 3;
        initial_expected_read_status = Query::Status::COMPLETE;
        query_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        // query_read.add_range(0, std::string("c"), std::string("d"));
        // query_read.add_range(
        //    std::string("dim1"), std::string("az"), std::string("de"));
        // query_read.add_range<int32_t>(
        //    1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        query_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        break;
    }

    // try for bufcnt expected to force split.
    // initial_result_num is initial expected number of results to be returned
    // if bufcnt == 3.
    int bufcnt = initial_result_num > 1 ? initial_result_num - 1 : 1;
    a1_data = std::vector<int32_t>(bufcnt);
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

    auto result_buffers = query_read.result_buffer_elements();
    auto result_num = result_buffers["a1"].second;
    std::cout << "option " << option << ", expected status "
              << initial_expected_read_status << " current read_status() "
              << query_read.query_status() << ", (initial) result_num "
              << result_num << ", final expected_result_num "
              << expected_result_num << std::endl;

    decltype(query_read.query_status()) what_status =
        bufcnt < initial_result_num ? Query::Status::INCOMPLETE :
                                      initial_expected_read_status;
    REQUIRE(
        query_read.query_status() == what_status);

    auto show_data =
        [&]() {
      std::string dim1s(&dim1[0], dim1.size());
      std::string dim3s(&dim3[0], dim3.size());
      std::vector<std::string> d1 =
          dataAndOffsetToStrings(query_read, "dim1", dim1_offsets, dim1s);
      std::vector<std::string> d3 =
          dataAndOffsetToStrings(query_read, "dim3", dim3_offsets, dim3s);
      for (auto i = 0; i < result_num; i++)
        std::cout << d1[i] << "\t" << dim2[i] << "\t" << d3[i] << "\t"
                  << a1_data[i] << std::endl;
    };
    show_data();

#if 01
    int expect_what = bufcnt < initial_result_num ? bufcnt : initial_result_num;
    REQUIRE(result_num == expect_what);
#else
    // TBD: is "a1" always in any returned results?
    //    auto result_buffers = query_read.result_buffer_elements();
    //    auto result_num = result_buffers["a1"].second;

    REQUIRE(result_num == initial_result_num);
#endif

    int tot_result_num = 0;
    while (query_read.query_status() == Query::Status::INCOMPLETE) {
      tot_result_num += result_num;
      query_read.submit();
      auto result_buffers = query_read.result_buffer_elements();
      result_num = result_buffers["a1"].second;
      show_data();
    }
    tot_result_num += result_num;
    std::cout << "tot_result_num " << tot_result_num << std::endl;
    REQUIRE(query_read.query_status() == Query::Status::COMPLETE);
    REQUIRE(tot_result_num == expected_result_num);

    array_read.close();
  };
  for (auto i = 0; i < 9; ++i)
    perform_read(i + 1);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}
