/**
 * @file   unit-cppapi-string-dims.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB Inc.
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

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

#include "tiledb/common/logger_public.h"

using namespace tiledb;

std::vector<std::string> dataAndOffsetToStrings(
    Query query,
    std::string col,
    std::vector<uint64_t> offsets,
    std::string data) {
  // Get the string sizes
  auto result_el_map = query.result_buffer_elements();
  auto result_el_off = result_el_map[col].first;
  std::vector<uint64_t> str_sizes;
  if (result_el_off > 1) {
    for (size_t i = 0; i < result_el_off - 1; ++i) {
      if (i >= offsets.size()) {
        std::stringstream msg;
        msg << "i " << i << ", offsets.size() " << offsets.size();
        LOG_TRACE(msg.str());
      }
      str_sizes.push_back(offsets[i + 1] - offsets[i]);
      if (str_sizes.size() > data.size()) {
        std::stringstream msg;
        msg << "str_sizes.size() " << str_sizes.size() << ", data.size() "
            << data.size();
        LOG_TRACE(msg.str());
      }
    }
  }
  if (result_el_off >= 1) {
    auto result_data_size = result_el_map[col].second * sizeof(char);
    str_sizes.push_back(result_data_size - offsets[result_el_off - 1]);
  }

  // Get the strings
  std::vector<std::string> vstr;
  for (size_t i = 0; i < result_el_off; ++i)
    vstr.push_back(std::string(&data[offsets[i]], str_sizes[i]));
  return vstr;
}

void write_array_1(
    const Context& ctx,
    const std::string& array_name,
    std::vector<char>& dim1,
    bool allow_dups = false) {
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
  if (allow_dups)
    schema.set_allows_dups(true);
  Array::create(array_name, schema);

  // Write data to the array.
  // one of the following provided by parameter.
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
}

TEST_CASE(
    "C++ API: Test infinite string splits",
    "[cppapi][string-dims][infinite-split]") {
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
  Subarray subarray_read(ctx, array_read);
  auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
  auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);
  subarray_read.add_range(
      0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
  subarray_read.add_range<int32_t>(
      1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
  query_read.set_subarray(subarray_read);

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
    "[cppapi][string-dims][default]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;

  std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'd', 'e', 'e', 'f'};
  write_array_1(ctx, array_name, dim1, false);

  std::vector<std::string> expected_dim1, collect_results_dim1;
  std::vector<int32_t> expected_dim2, collect_results_dim2;
  std::vector<std::string> expected_dim3, collect_results_dim3;
  std::vector<int32_t> expected_a1_data, collect_results_a1;

  int which_option = -1;
  SECTION("1") {
    which_option = 1;
  }
  SECTION("2") {
    which_option = 2;
  }
  SECTION("3") {
    which_option = 3;
  }
  SECTION("4") {
    which_option = 4;
  }
  SECTION("5") {
    which_option = 5;
  }
  SECTION("6") {
    which_option = 6;
  }
  SECTION("7") {
    which_option = 7;
  }
  SECTION("8") {
    which_option = 8;
  }
  SECTION("9") {
    which_option = 9;
  }
  SECTION("10") {
    which_option = 10;
  }

  auto perform_read = [&](int option) {
    collect_results_dim1.clear();
    collect_results_dim2.clear();
    collect_results_dim3.clear();
    collect_results_a1.clear();

    // Prepare a read query.
    Array array_read(ctx, array_name, TILEDB_READ);
    Query query_read(ctx, array_read, TILEDB_READ);
    Subarray subarray_read(ctx, array_read);
    auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
    auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);
    auto dim3_non_empty_domain = array_read.non_empty_domain_var(2);

    // Each case section has the same .add_range possibilities,
    // some active some inactive, to make visual comparison of what's
    // different between them a bit easier, at least with some colorizing
    // editors.
    switch (option) {
      case 1:
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        expected_dim1 = {"bb", "c", "d"};
        expected_dim2 = {1, 1, 2};
        expected_dim3 = {"hh", "i", "j"};
        expected_a1_data = {2, 3, 4};
        break;
      case 2:
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"c", "d"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "j"};
        expected_a1_data = {3, 4};
        break;
      case 3:
        subarray_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"a", "bb", "c", "d", "ee", "f"};
        expected_dim2 = {1, 1, 1, 2, 2, 2};
        expected_dim3 = {"g", "hh", "i", "j", "kk", "l"};
        expected_a1_data = {1, 2, 3, 4, 5, 6};
        break;
      case 4:
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        expected_dim1 = {"c", "d"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "j"};
        expected_a1_data = {3, 4};
        break;
      case 5:
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"c", "d"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "j"};
        expected_a1_data = {3, 4};
        break;
      case 6:
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"bb", "c", "d"};
        expected_dim2 = {1, 1, 2};
        expected_dim3 = {"hh", "i", "j"};
        expected_a1_data = {2, 3, 4};
        break;
      case 7:
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"c", "d"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "j"};
        expected_a1_data = {3, 4};
        break;
      case 8:
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"c", "d"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "j"};
        expected_a1_data = {3, 4};
        break;
      case 9:
        subarray_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"c", "d", "ee"};
        expected_dim2 = {1, 2, 2};
        expected_dim3 = {"i", "j", "kk"};
        expected_a1_data = {3, 4, 5};
        break;
      case 10:
        subarray_read.add_range(0, std::string("c"), std::string("ee"));
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kk"));
        expected_dim1 = {"c", "d", "ee"};
        expected_dim2 = {1, 2, 2};
        expected_dim3 = {"i", "j", "kk"};
        expected_a1_data = {3, 4, 5};
        break;
    }
    query_read.set_subarray(subarray_read);

    std::vector<char> dim1;
    std::vector<uint64_t> dim1_offsets;
    std::vector<int32_t> dim2;
    std::vector<char> dim3;
    std::vector<uint64_t> dim3_offsets;
    std::vector<int32_t> a1_data;

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

    auto collect_data = [&]() {
      std::string dim1s(&dim1[0], dim1.size());
      std::string dim3s(&dim3[0], dim3.size());
      std::vector<std::string> d1 =
          dataAndOffsetToStrings(query_read, "dim1", dim1_offsets, dim1s);
      std::vector<std::string> d3 =
          dataAndOffsetToStrings(query_read, "dim3", dim3_offsets, dim3s);
      for (auto i = 0u; i < result_num; i++) {
        {
          std::stringstream msg;
          msg << d1[i] << "\t" << dim2[i] << "\t" << d3[i] << "\t" << a1_data[i]
              << std::endl;
          LOG_TRACE(msg.str());
        }
        collect_results_dim1.emplace_back(d1[i]);
        collect_results_dim2.emplace_back(dim2[i]);
        collect_results_dim3.emplace_back(d3[i]);
        collect_results_a1.emplace_back(a1_data[i]);
      }
    };
    {
      std::stringstream msg;
      msg << "option " << option << ", num results " << result_num << std::endl;
      LOG_TRACE(msg.str());
    }
    collect_data();
    REQUIRE(expected_dim1 == collect_results_dim1);
    REQUIRE(expected_dim2 == collect_results_dim2);
    REQUIRE(expected_dim3 == collect_results_dim3);
    REQUIRE(expected_a1_data == collect_results_a1);

    array_read.close();
  };  // perform_read()

  perform_read(which_option);

  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test default string dimensions with partitioning",
    "[cppapi][string-dims][default][partitioning]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;

  std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'a', 'b', 'b', 'c'};
  write_array_1(ctx, array_name, dim1, false);

  std::vector<std::string> expected_dim1, collect_results_dim1;
  std::vector<int32_t> expected_dim2, collect_results_dim2;
  std::vector<std::string> expected_dim3, collect_results_dim3;
  std::vector<int32_t> expected_a1_data, collect_results_a1;

  int which_option = -1;
  SECTION("1") {
    which_option = 1;
  }
  SECTION("2") {
    which_option = 2;
  }
  SECTION("3") {
    which_option = 3;
  }
  SECTION("4") {
    which_option = 4;
  }
  SECTION("5") {
    which_option = 5;
  }
  SECTION("6") {
    which_option = 6;
  }
  SECTION("7") {
    which_option = 7;
  }
  SECTION("8") {
    which_option = 8;
  }
  SECTION("9") {
    which_option = 9;
  }

  auto perform_read = [&](int option) {
    collect_results_dim1.clear();
    collect_results_dim2.clear();
    collect_results_dim3.clear();
    collect_results_a1.clear();

    // Prepare a read query. Do not set a range for the last string dimension.
    Array array_read(ctx, array_name, TILEDB_READ);
    Query query_read(ctx, array_read, TILEDB_READ);
    Subarray subarray_read(ctx, array_read);
    auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
    auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);

    decltype(query_read.query_status()) initial_expected_read_status =
        tiledb::Query::Status::UNINITIALIZED;
    unsigned expected_result_num = std::numeric_limits<unsigned>::max();
    unsigned initial_result_num = 2;

    // Each case section has the same .add_range possibilities,
    // some active some inactive, to make visual comparison of what's
    // different between them a bit easier, at least with some colorizing
    // editors.
    switch (option) {
      case 1:
        expected_result_num = 4;
        initial_expected_read_status = Query::Status::INCOMPLETE;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        expected_dim1 = {"bb", "bb", "c", "c"};
        expected_dim2 = {1, 2, 1, 2};
        expected_dim3 = {"hh", "kk", "i", "l"};
        expected_a1_data = {2, 5, 3, 6};
        break;
      case 2:
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"bb", "c"};
        expected_dim2 = {2, 1};
        expected_dim3 = {"kk", "i"};
        expected_a1_data = {5, 3};
        break;
      case 3:
        expected_result_num = 6;
        initial_expected_read_status = Query::Status::INCOMPLETE;
        subarray_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"a", "a", "bb", "bb", "c", "c"};
        expected_dim2 = {1, 2, 1, 2, 1, 2};
        expected_dim3 = {"g", "j", "hh", "kk", "i", "l"};
        expected_a1_data = {1, 4, 2, 5, 3, 6};
        break;
      case 4:
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        expected_dim1 = {"c", "c"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "l"};
        expected_a1_data = {3, 6};
        break;
      case 5:
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"c", "c"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "l"};
        expected_a1_data = {3, 6};
        break;
      case 6:
        expected_result_num = 4;
        initial_expected_read_status = Query::Status::INCOMPLETE;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"bb", "bb", "c", "c"};
        expected_dim2 = {1, 2, 1, 2};
        expected_dim3 = {"hh", "kk", "i", "l"};
        expected_a1_data = {2, 5, 3, 6};
        break;
      case 7:
        expected_result_num = 2;
        initial_expected_read_status = Query::Status::COMPLETE;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"bb", "c"};
        expected_dim2 = {2, 1};
        expected_dim3 = {"kk", "i"};
        expected_a1_data = {5, 3};
        break;
      case 8:
        expected_result_num = 1;
        initial_result_num = 1;
        initial_expected_read_status = Query::Status::COMPLETE;
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"c"};
        expected_dim2 = {1};
        expected_dim3 = {"i"};
        expected_a1_data = {3};
        break;
      case 9:
        expected_result_num = 3;
        initial_result_num = 3;
        initial_expected_read_status = Query::Status::COMPLETE;
        subarray_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"a", "bb", "c"};
        expected_dim2 = {2, 2, 1};
        expected_dim3 = {"j", "kk", "i"};
        expected_a1_data = {4, 5, 3};
        break;
    }
    query_read.set_subarray(subarray_read);

    std::vector<char> dim1;
    std::vector<uint64_t> dim1_offsets;
    std::vector<int32_t> dim2;
    std::vector<char> dim3;
    std::vector<uint64_t> dim3_offsets;
    std::vector<int32_t> a1_data;

    // try for bufcnt expected to force split.
    // initial_result_num is initial expected number of results to be returned
    unsigned bufcnt = 3;
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
    {
      std::stringstream msg;
      msg << "option " << option << ", expected status "
          << initial_expected_read_status << " current read_status() "
          << query_read.query_status() << ", (initial) result_num "
          << result_num << ", final expected_result_num " << expected_result_num
          << std::endl;
      LOG_TRACE(msg.str());
    }

    decltype(query_read.query_status()) what_status =
        bufcnt < initial_result_num ? Query::Status::INCOMPLETE :
                                      initial_expected_read_status;
    REQUIRE(query_read.query_status() == what_status);
    auto collect_data = [&]() {
      std::string dim1s(&dim1[0], dim1.size());
      std::string dim3s(&dim3[0], dim3.size());
      std::vector<std::string> d1 =
          dataAndOffsetToStrings(query_read, "dim1", dim1_offsets, dim1s);
      std::vector<std::string> d3 =
          dataAndOffsetToStrings(query_read, "dim3", dim3_offsets, dim3s);
      for (auto i = 0u; i < result_num; i++) {
        {
          std::stringstream msg;
          msg << d1[i] << "\t" << dim2[i] << "\t" << d3[i] << "\t" << a1_data[i]
              << std::endl;
          LOG_TRACE(msg.str());
        }
        collect_results_dim1.emplace_back(d1[i]);
        collect_results_dim2.emplace_back(dim2[i]);
        collect_results_dim3.emplace_back(d3[i]);
        collect_results_a1.emplace_back(a1_data[i]);
      }
    };
    collect_data();

    REQUIRE(result_num == initial_result_num);

    unsigned tot_result_num = 0;
    while (query_read.query_status() == Query::Status::INCOMPLETE) {
      tot_result_num += result_num;
      query_read.submit();
      auto result_buffers = query_read.result_buffer_elements();
      result_num = result_buffers["a1"].second;
      collect_data();
    }
    tot_result_num += result_num;
    {
      std::stringstream msg;
      msg << "tot_result_num " << tot_result_num << std::endl;
      LOG_TRACE(msg.str());
    }
    REQUIRE(query_read.query_status() == Query::Status::COMPLETE);
    REQUIRE(tot_result_num == expected_result_num);
    REQUIRE(expected_dim1 == collect_results_dim1);
    REQUIRE(expected_dim2 == collect_results_dim2);
    REQUIRE(expected_dim3 == collect_results_dim3);
    REQUIRE(expected_a1_data == collect_results_a1);

    array_read.close();
  };  // perform_read()

  perform_read(which_option);

  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test default string dimensions with partitioning - with SECTIONs",
    "[cppapi][string-dims][default][partitioning]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;

  std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'a', 'b', 'b', 'c'};
  write_array_1(ctx, array_name, dim1, false);

  std::vector<std::string> expected_dim1, collect_results_dim1;
  std::vector<int32_t> expected_dim2, collect_results_dim2;
  std::vector<std::string> expected_dim3, collect_results_dim3;
  std::vector<int32_t> expected_a1_data, collect_results_a1;

  collect_results_dim1.clear();
  collect_results_dim2.clear();
  collect_results_dim3.clear();
  collect_results_a1.clear();

  // Prepare a read query. Do not set a range for the last string dimension.
  Array array_read(ctx, array_name, TILEDB_READ);
  Query query_read(ctx, array_read, TILEDB_READ);
  Subarray subarray_read(ctx, array_read);
  auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
  auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);

  decltype(query_read.query_status()) initial_expected_read_status =
      tiledb::Query::Status::UNINITIALIZED;
  unsigned expected_result_num = std::numeric_limits<unsigned>::max();
  unsigned initial_result_num = 2;
  int which_option = -1;

  // Each case section has the same .add_range possibilities,
  // some active some inactive, to make visual comparison of what's
  // different between them a bit easier, at least with some colorizing
  // editors.
  SECTION("1") {
    which_option = 1;
    expected_result_num = 4;
    initial_expected_read_status = Query::Status::INCOMPLETE;
    subarray_read.add_range(
        std::string("dim1"), std::string("az"), std::string("de"));
    expected_dim1 = {"bb", "bb", "c", "c"};
    expected_dim2 = {1, 2, 1, 2};
    expected_dim3 = {"hh", "kk", "i", "l"};
    expected_a1_data = {2, 5, 3, 6};
  }
  SECTION("2") {
    which_option = 2;
    expected_result_num = 2;
    initial_expected_read_status = Query::Status::COMPLETE;
    subarray_read.add_range(
        std::string("dim1"), std::string("az"), std::string("de"));
    subarray_read.add_range<int32_t>(
        1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
    subarray_read.add_range(
        std::string("dim3"), std::string("i"), std::string("kl"));
    expected_dim1 = {"bb", "c"};
    expected_dim2 = {2, 1};
    expected_dim3 = {"kk", "i"};
    expected_a1_data = {5, 3};
  }
  SECTION("3") {
    which_option = 3;
    expected_result_num = 6;
    initial_expected_read_status = Query::Status::INCOMPLETE;
    subarray_read.add_range(
        0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
    subarray_read.add_range<int32_t>(
        1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
    expected_dim1 = {"a", "a", "bb", "bb", "c", "c"};
    expected_dim2 = {1, 2, 1, 2, 1, 2};
    expected_dim3 = {"g", "j", "hh", "kk", "i", "l"};
    expected_a1_data = {1, 4, 2, 5, 3, 6};
  }
  SECTION("4") {
    which_option = 4;
    expected_result_num = 2;
    initial_expected_read_status = Query::Status::COMPLETE;
    subarray_read.add_range(0, std::string("c"), std::string("d"));
    expected_dim1 = {"c", "c"};
    expected_dim2 = {1, 2};
    expected_dim3 = {"i", "l"};
    expected_a1_data = {3, 6};
  }
  SECTION("5") {
    which_option = 5;
    expected_result_num = 2;
    initial_expected_read_status = Query::Status::COMPLETE;
    subarray_read.add_range(0, std::string("c"), std::string("d"));
    subarray_read.add_range<int32_t>(
        1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
    expected_dim1 = {"c", "c"};
    expected_dim2 = {1, 2};
    expected_dim3 = {"i", "l"};
    expected_a1_data = {3, 6};
  }
  SECTION("6") {
    which_option = 6;
    expected_result_num = 4;
    initial_expected_read_status = Query::Status::INCOMPLETE;
    subarray_read.add_range(
        std::string("dim1"), std::string("az"), std::string("de"));
    subarray_read.add_range<int32_t>(
        1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
    expected_dim1 = {"bb", "bb", "c", "c"};
    expected_dim2 = {1, 2, 1, 2};
    expected_dim3 = {"hh", "kk", "i", "l"};
    expected_a1_data = {2, 5, 3, 6};
  }
  SECTION("7") {
    which_option = 7;
    expected_result_num = 2;
    initial_expected_read_status = Query::Status::COMPLETE;
    subarray_read.add_range(
        std::string("dim1"), std::string("az"), std::string("de"));
    subarray_read.add_range(
        std::string("dim3"), std::string("i"), std::string("kl"));
    expected_dim1 = {"bb", "c"};
    expected_dim2 = {2, 1};
    expected_dim3 = {"kk", "i"};
    expected_a1_data = {5, 3};
  }
  SECTION("8") {
    which_option = 8;
    expected_result_num = 1;
    initial_result_num = 1;
    initial_expected_read_status = Query::Status::COMPLETE;
    subarray_read.add_range(0, std::string("c"), std::string("d"));
    subarray_read.add_range(
        std::string("dim3"), std::string("i"), std::string("kl"));
    expected_dim1 = {"c"};
    expected_dim2 = {1};
    expected_dim3 = {"i"};
    expected_a1_data = {3};
  }
  SECTION("9") {
    which_option = 9;
    expected_result_num = 3;
    initial_result_num = 3;
    initial_expected_read_status = Query::Status::COMPLETE;
    subarray_read.add_range(
        0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
    subarray_read.add_range(
        std::string("dim3"), std::string("i"), std::string("kl"));
    expected_dim1 = {"a", "bb", "c"};
    expected_dim2 = {2, 2, 1};
    expected_dim3 = {"j", "kk", "i"};
    expected_a1_data = {4, 5, 3};
  }
  query_read.set_subarray(subarray_read);

  std::vector<uint64_t> dim1_offsets;
  std::vector<int32_t> dim2;
  std::vector<char> dim3;
  std::vector<uint64_t> dim3_offsets;
  std::vector<int32_t> a1_data;

  // try for bufcnt expected to force split.
  // initial_result_num is initial expected number of results to be returned
  unsigned bufcnt = 3;
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
  {
    std::stringstream msg;
    msg << "option " << which_option << ", expected status "
        << initial_expected_read_status << " current read_status() "
        << query_read.query_status() << ", (initial) result_num " << result_num
        << ", final expected_result_num " << expected_result_num << std::endl;
    LOG_TRACE(msg.str());
  }

  decltype(query_read.query_status()) what_status =
      bufcnt < initial_result_num ? Query::Status::INCOMPLETE :
                                    initial_expected_read_status;
  REQUIRE(query_read.query_status() == what_status);
  auto collect_data = [&]() {
    std::string dim1s(&dim1[0], dim1.size());
    std::string dim3s(&dim3[0], dim3.size());
    std::vector<std::string> d1 =
        dataAndOffsetToStrings(query_read, "dim1", dim1_offsets, dim1s);
    std::vector<std::string> d3 =
        dataAndOffsetToStrings(query_read, "dim3", dim3_offsets, dim3s);
    for (auto i = 0u; i < result_num; i++) {
      {
        std::stringstream msg;
        msg << d1[i] << "\t" << dim2[i] << "\t" << d3[i] << "\t" << a1_data[i]
            << std::endl;
        LOG_TRACE(msg.str());
      }
      collect_results_dim1.emplace_back(d1[i]);
      collect_results_dim2.emplace_back(dim2[i]);
      collect_results_dim3.emplace_back(d3[i]);
      collect_results_a1.emplace_back(a1_data[i]);
    }
  };
  collect_data();

  REQUIRE(result_num == initial_result_num);

  unsigned tot_result_num = 0;
  while (query_read.query_status() == Query::Status::INCOMPLETE) {
    tot_result_num += result_num;
    query_read.submit();
    auto result_buffers = query_read.result_buffer_elements();
    result_num = result_buffers["a1"].second;
    collect_data();
  }
  tot_result_num += result_num;
  {
    std::stringstream msg;
    msg << "tot_result_num " << tot_result_num << std::endl;
    LOG_TRACE(msg.str());
  }
  REQUIRE(query_read.query_status() == Query::Status::COMPLETE);
  REQUIRE(tot_result_num == expected_result_num);
  REQUIRE(expected_dim1 == collect_results_dim1);
  REQUIRE(expected_dim2 == collect_results_dim2);
  REQUIRE(expected_dim3 == collect_results_dim3);
  REQUIRE(expected_a1_data == collect_results_a1);

  array_read.close();

  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test default string dimensions with partitioning - dupsallowed, "
    "varying bufcnt",
    "[cppapi][string-dims][default][partitioning]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;

  std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'a', 'b', 'b', 'c'};
  write_array_1(ctx, array_name, dim1, true);

  std::vector<std::string> expected_dim1, collect_results_dim1;
  std::vector<int32_t> expected_dim2, collect_results_dim2;
  std::vector<std::string> expected_dim3, collect_results_dim3;
  std::vector<int32_t> expected_a1_data, collect_results_a1;

  int which_option = -1;
  SECTION("1") {
    which_option = 1;
  }
  SECTION("2") {
    which_option = 2;
  }
  SECTION("3") {
    which_option = 3;
  }
  SECTION("4") {
    which_option = 4;
  }
  SECTION("5") {
    which_option = 5;
  }
  SECTION("6") {
    which_option = 6;
  }
  SECTION("7") {
    which_option = 7;
  }
  SECTION("8") {
    which_option = 8;
  }
  SECTION("9") {
    which_option = 9;
  }

  auto perform_read = [&](int option, int bufcnt = 3) {
    collect_results_dim1.clear();
    collect_results_dim2.clear();
    collect_results_dim3.clear();
    collect_results_a1.clear();

    // Prepare a read query. Ranges set vary in the different switch cases.
    Array array_read(ctx, array_name, TILEDB_READ);
    Query query_read(ctx, array_read, TILEDB_READ);
    Subarray subarray_read(ctx, array_read);
    auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
    auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);

    unsigned expected_result_num = std::numeric_limits<unsigned>::max();

    // Each case section has the same .add_range possibilities,
    // some active some inactive, to make visual comparison of what's
    // different between them a bit easier, at least with some colorizing
    // editors.
    switch (option) {
      case 1:
        expected_result_num = 4;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        expected_dim1 = {"bb", "bb", "c", "c"};
        expected_dim2 = {1, 2, 1, 2};
        expected_dim3 = {"hh", "kk", "i", "l"};
        expected_a1_data = {2, 5, 3, 6};

        break;
      case 2:
        expected_result_num = 2;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"bb", "c"};
        expected_dim2 = {2, 1};
        expected_dim3 = {"kk", "i"};
        expected_a1_data = {5, 3};
        break;
      case 3:
        expected_result_num = 6;
        subarray_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"a", "a", "bb", "bb", "c", "c"};
        expected_dim2 = {1, 2, 1, 2, 1, 2};
        expected_dim3 = {"g", "j", "hh", "kk", "i", "l"};
        expected_a1_data = {1, 4, 2, 5, 3, 6};
        break;
      case 4:
        expected_result_num = 2;
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        expected_dim1 = {"c", "c"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "l"};
        expected_a1_data = {3, 6};
        break;
      case 5:
        expected_result_num = 2;
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"c", "c"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "l"};
        expected_a1_data = {3, 6};
        break;
      case 6:
        expected_result_num = 4;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"bb", "bb", "c", "c"};
        expected_dim2 = {1, 2, 1, 2};
        expected_dim3 = {"hh", "kk", "i", "l"};
        expected_a1_data = {2, 5, 3, 6};
        break;
      case 7:
        expected_result_num = 2;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"bb", "c"};
        expected_dim2 = {2, 1};
        expected_dim3 = {"kk", "i"};
        expected_a1_data = {5, 3};
        break;
      case 8:
        expected_result_num = 1;
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"c"};
        expected_dim2 = {1};
        expected_dim3 = {"i"};
        expected_a1_data = {3};
        break;
      case 9:
        expected_result_num = 3;
        subarray_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"a", "bb", "c"};
        expected_dim2 = {2, 2, 1};
        expected_dim3 = {"j", "kk", "i"};
        expected_a1_data = {4, 5, 3};
        break;
    }
    query_read.set_subarray(subarray_read);

    std::vector<char> dim1;
    std::vector<uint64_t> dim1_offsets;
    std::vector<int32_t> dim2;
    std::vector<char> dim3;
    std::vector<uint64_t> dim3_offsets;
    std::vector<int32_t> a1_data;

    // bufcnt specified by caller
    // initial_result_num is initial expected number of results to be returned
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
    {
      std::stringstream msg;
      msg << "option " << option << " current read_status() "
          << query_read.query_status() << ", (initial) result_num "
          << result_num << ", final expected_result_num " << expected_result_num
          << ", bufcnt " << bufcnt << std::endl;
      LOG_TRACE(msg.str());
    }

    auto collect_data = [&]() {
      std::string dim1s(&dim1[0], dim1.size());
      std::string dim3s(&dim3[0], dim3.size());
      std::vector<std::string> d1 =
          dataAndOffsetToStrings(query_read, "dim1", dim1_offsets, dim1s);
      std::vector<std::string> d3 =
          dataAndOffsetToStrings(query_read, "dim3", dim3_offsets, dim3s);
      for (auto i = 0u; i < result_num; i++) {
        {
          std::stringstream msg;
          msg << d1[i] << "\t" << dim2[i] << "\t" << d3[i] << "\t" << a1_data[i]
              << std::endl;
          LOG_TRACE(msg.str());
        }
        collect_results_dim1.emplace_back(d1[i]);
        collect_results_dim2.emplace_back(dim2[i]);
        collect_results_dim3.emplace_back(d3[i]);
        collect_results_a1.emplace_back(a1_data[i]);
      }
    };
    collect_data();

    unsigned tot_result_num = 0;
    while (query_read.query_status() == Query::Status::INCOMPLETE) {
      tot_result_num += result_num;
      query_read.submit();
      auto result_buffers = query_read.result_buffer_elements();
      result_num = result_buffers["a1"].second;
      collect_data();
    }
    tot_result_num += result_num;
    {
      std::stringstream msg;
      msg << "tot_result_num " << tot_result_num << std::endl;
      LOG_TRACE(msg.str());
    }
    REQUIRE(query_read.query_status() == Query::Status::COMPLETE);
    REQUIRE(tot_result_num == expected_result_num);

    REQUIRE(expected_dim1 == collect_results_dim1);
    REQUIRE(expected_dim2 == collect_results_dim2);
    REQUIRE(expected_dim3 == collect_results_dim3);
    REQUIRE(expected_a1_data == collect_results_a1);

    array_read.close();
  };  // perform_read()

  for (auto bufcnt = 0; bufcnt < 6; ++bufcnt) {
    perform_read(which_option, bufcnt + 1);
  }

  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test default string dimensions with partitioning - dupsallowed - "
    "withsections",
    "[cppapi][string-dims][default][partitioning]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;

  std::vector<char> dim1 = {'a', 'b', 'b', 'c', 'a', 'b', 'b', 'c'};
  write_array_1(ctx, array_name, dim1, true);

  int which_option = -1;
  SECTION("1") {
    which_option = 1;
  }
  SECTION("2") {
    which_option = 2;
  }
  SECTION("3") {
    which_option = 3;
  }
  SECTION("4") {
    which_option = 4;
  }
  SECTION("5") {
    which_option = 5;
  }
  SECTION("6") {
    which_option = 6;
  }
  SECTION("7") {
    which_option = 7;
  }
  SECTION("8") {
    which_option = 8;
  }
  SECTION("9") {
    which_option = 9;
  }

  auto read_bufcnt_section = [&](int option, int bufcnt = 3) {
    std::vector<std::string> expected_dim1, collect_results_dim1;
    std::vector<int32_t> expected_dim2, collect_results_dim2;
    std::vector<std::string> expected_dim3, collect_results_dim3;
    std::vector<int32_t> expected_a1_data, collect_results_a1;

    collect_results_dim1.clear();
    collect_results_dim2.clear();
    collect_results_dim3.clear();
    collect_results_a1.clear();
    // Prepare a read query. Ranges set vary in the different switch cases.
    Array array_read(ctx, array_name, TILEDB_READ);
    Query query_read(ctx, array_read, TILEDB_READ);
    Subarray subarray_read(ctx, array_read);
    auto dim1_non_empty_domain = array_read.non_empty_domain_var(0);
    auto dim2_non_empty_domain = array_read.non_empty_domain<int32_t>(1);

    unsigned expected_result_num = std::numeric_limits<unsigned>::max();

    // Each case section has the same .add_range possibilities,
    // some active some inactive, to make visual comparison of what's
    // different between them a bit easier, at least with some colorizing
    // editors.
    switch (option) {
      case 1:
        expected_result_num = 4;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        expected_dim1 = {"bb", "bb", "c", "c"};
        expected_dim2 = {1, 2, 1, 2};
        expected_dim3 = {"hh", "kk", "i", "l"};
        expected_a1_data = {2, 5, 3, 6};

        break;
      case 2:
        expected_result_num = 2;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"bb", "c"};
        expected_dim2 = {2, 1};
        expected_dim3 = {"kk", "i"};
        expected_a1_data = {5, 3};
        break;
      case 3:
        expected_result_num = 6;
        subarray_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"a", "a", "bb", "bb", "c", "c"};
        expected_dim2 = {1, 2, 1, 2, 1, 2};
        expected_dim3 = {"g", "j", "hh", "kk", "i", "l"};
        expected_a1_data = {1, 4, 2, 5, 3, 6};
        break;
      case 4:
        expected_result_num = 2;
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        expected_dim1 = {"c", "c"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "l"};
        expected_a1_data = {3, 6};
        break;
      case 5:
        expected_result_num = 2;
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"c", "c"};
        expected_dim2 = {1, 2};
        expected_dim3 = {"i", "l"};
        expected_a1_data = {3, 6};
        break;
      case 6:
        expected_result_num = 4;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range<int32_t>(
            1, dim2_non_empty_domain.first, dim2_non_empty_domain.second);
        expected_dim1 = {"bb", "bb", "c", "c"};
        expected_dim2 = {1, 2, 1, 2};
        expected_dim3 = {"hh", "kk", "i", "l"};
        expected_a1_data = {2, 5, 3, 6};
        break;
      case 7:
        expected_result_num = 2;
        subarray_read.add_range(
            std::string("dim1"), std::string("az"), std::string("de"));
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"bb", "c"};
        expected_dim2 = {2, 1};
        expected_dim3 = {"kk", "i"};
        expected_a1_data = {5, 3};
        break;
      case 8:
        expected_result_num = 1;
        subarray_read.add_range(0, std::string("c"), std::string("d"));
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"c"};
        expected_dim2 = {1};
        expected_dim3 = {"i"};
        expected_a1_data = {3};
        break;
      case 9:
        expected_result_num = 3;
        subarray_read.add_range(
            0, dim1_non_empty_domain.first, dim1_non_empty_domain.second);
        subarray_read.add_range(
            std::string("dim3"), std::string("i"), std::string("kl"));
        expected_dim1 = {"a", "bb", "c"};
        expected_dim2 = {2, 2, 1};
        expected_dim3 = {"j", "kk", "i"};
        expected_a1_data = {4, 5, 3};
        break;
    }
    query_read.set_subarray(subarray_read);

    std::vector<char> dim1;
    std::vector<uint64_t> dim1_offsets;
    std::vector<int32_t> dim2;
    std::vector<char> dim3;
    std::vector<uint64_t> dim3_offsets;
    std::vector<int32_t> a1_data;

    // bufcnt specified by caller
    // initial_result_num is initial expected number of results to be returned
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
    {
      std::stringstream msg;
      msg << "option " << option << " current read_status() "
          << query_read.query_status() << ", (initial) result_num "
          << result_num << ", final expected_result_num " << expected_result_num
          << ", bufcnt " << bufcnt << std::endl;
      LOG_TRACE(msg.str());
    }

    auto collect_data = [&]() {
      std::string dim1s(&dim1[0], dim1.size());
      std::string dim3s(&dim3[0], dim3.size());
      std::vector<std::string> d1 =
          dataAndOffsetToStrings(query_read, "dim1", dim1_offsets, dim1s);
      std::vector<std::string> d3 =
          dataAndOffsetToStrings(query_read, "dim3", dim3_offsets, dim3s);
      for (auto i = 0u; i < result_num; i++) {
        {
          std::stringstream msg;
          msg << d1[i] << "\t" << dim2[i] << "\t" << d3[i] << "\t" << a1_data[i]
              << std::endl;
          LOG_TRACE(msg.str());
        }
        collect_results_dim1.emplace_back(d1[i]);
        collect_results_dim2.emplace_back(dim2[i]);
        collect_results_dim3.emplace_back(d3[i]);
        collect_results_a1.emplace_back(a1_data[i]);
      }
    };
    collect_data();

    unsigned tot_result_num = 0;
    while (query_read.query_status() == Query::Status::INCOMPLETE) {
      tot_result_num += result_num;
      query_read.submit();
      auto result_buffers = query_read.result_buffer_elements();
      result_num = result_buffers["a1"].second;
      collect_data();
    }
    tot_result_num += result_num;
    {
      std::stringstream msg;
      msg << "tot_result_num " << tot_result_num << std::endl;
      LOG_TRACE(msg.str());
    }
    REQUIRE(query_read.query_status() == Query::Status::COMPLETE);
    REQUIRE(tot_result_num == expected_result_num);

    REQUIRE(expected_dim1 == collect_results_dim1);
    REQUIRE(expected_dim2 == collect_results_dim2);
    REQUIRE(expected_dim3 == collect_results_dim3);
    REQUIRE(expected_a1_data == collect_results_a1);

    array_read.close();
  };  // perform_read()

  for (auto bufcnt = 0; bufcnt < 6; ++bufcnt) {
    read_bufcnt_section(which_option, bufcnt + 1);
  }

  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

void write_sparse_array_string_dim(
    Context ctx,
    const std::string& array_name,
    std::string& data,
    std::vector<uint64_t>& data_offsets,
    tiledb_layout_t layout) {
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(layout);
  query.set_data_buffer("dim1", (char*)data.data(), data.size());
  query.set_offsets_buffer("dim1", data_offsets.data(), data_offsets.size());
  if (layout != TILEDB_GLOBAL_ORDER) {
    query.submit();
  } else {
    query.submit_and_finalize();
  }

  array.close();
}

void read_and_check_sparse_array_string_dim(
    Context ctx,
    const std::string& array_name,
    std::string& expected_data,
    std::vector<uint64_t>& expected_offsets,
    tiledb_layout_t layout) {
  Array array(ctx, array_name, TILEDB_READ);

  std::vector<uint64_t> offsets_back(expected_offsets.size());
  std::string data_back;
  data_back.resize(expected_data.size());

  Subarray subarray(ctx, array);
  subarray.add_range(
      "dim1", std::string("ATSD987JIO"), std::string("TGSD987JPO"));

  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(subarray);
  query.set_data_buffer("dim1", (char*)data_back.data(), data_back.size());
  query.set_offsets_buffer("dim1", offsets_back.data(), offsets_back.size());
  query.set_layout(layout);

  query.submit();

  // Check the element data and offsets are properly returned
  CHECK(data_back == expected_data);
  CHECK(offsets_back == expected_offsets);

  array.close();
}

TEST_CASE(
    "C++ API: Test filtering of string dimensions on sparse arrays",
    "[cppapi][string-dims][rle-strings][dict-strings][sparse][rest]") {
  auto f = GENERATE(TILEDB_FILTER_RLE, TILEDB_FILTER_DICTIONARY);
  // Create data buffer to use
  std::stringstream repetitions;
  size_t repetition_num = 100;
  for (size_t i = 0; i < repetition_num; i++)
    repetitions << "GLSD987JHY";
  std::string data =
      "ATSD987JIO" + std::string(repetitions.str()) + "TGSD987JPO";
  // Create the corresponding offsets buffer
  std::vector<uint64_t> data_elem_offsets(repetition_num + 2);
  int start = -10;
  std::generate(data_elem_offsets.begin(), data_elem_offsets.end(), [&] {
    return start += 10;
  });

  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_name{vfs_test_setup.array_uri("test_rle_string_dim")};

  Domain domain(ctx);
  auto dim =
      Dimension::create(ctx, "dim1", TILEDB_STRING_ASCII, nullptr, nullptr);

  // Create compressor as a filter
  Filter filter(ctx, f);
  // Create filter list
  FilterList filter_list(ctx);
  // Add compressor to filter list
  filter_list.add_filter(filter);
  dim.set_filter_list(filter_list);

  domain.add_dimension(dim);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.set_allows_dups(true);
  tiledb::Array::create(array_name, schema);

  SECTION("Unordered write") {
    write_sparse_array_string_dim(
        ctx, array_name, data, data_elem_offsets, TILEDB_UNORDERED);
    SECTION("Row major read") {
      read_and_check_sparse_array_string_dim(
          ctx, array_name, data, data_elem_offsets, TILEDB_ROW_MAJOR);
    }
    SECTION("Global order read") {
      read_and_check_sparse_array_string_dim(
          ctx, array_name, data, data_elem_offsets, TILEDB_GLOBAL_ORDER);
    }
    SECTION("Unordered read") {
      read_and_check_sparse_array_string_dim(
          ctx, array_name, data, data_elem_offsets, TILEDB_UNORDERED);
    }
  }
  SECTION("Global order write") {
    write_sparse_array_string_dim(
        ctx, array_name, data, data_elem_offsets, TILEDB_GLOBAL_ORDER);
    SECTION("Row major read") {
      read_and_check_sparse_array_string_dim(
          ctx, array_name, data, data_elem_offsets, TILEDB_ROW_MAJOR);
    }
    SECTION("Global order read") {
      read_and_check_sparse_array_string_dim(
          ctx, array_name, data, data_elem_offsets, TILEDB_GLOBAL_ORDER);
    }
    SECTION("Unordered read") {
      read_and_check_sparse_array_string_dim(
          ctx, array_name, data, data_elem_offsets, TILEDB_UNORDERED);
    }
  }
}

TEST_CASE(
    "C++ API: Test adding RLE/Dictionary filter of string dimensions",
    "[cppapi][string-dims][rle-strings][dict-strings][sparse]") {
  std::string array_name = "test_rle_string_dim";

  Context ctx;
  Domain domain(ctx);
  // Create var-length string dimension
  auto dim_var_string =
      Dimension::create(ctx, "dim1", TILEDB_STRING_ASCII, nullptr, nullptr);
  auto dim_not_var_string =
      tiledb::Dimension::create<int32_t>(ctx, "id", {{1, 100}}, 10);

  // Create filters
  auto f = GENERATE(TILEDB_FILTER_RLE, TILEDB_FILTER_DICTIONARY);
  Filter string_filter(ctx, f);
  Filter another_filter(ctx, TILEDB_FILTER_CHECKSUM_MD5);

  // Create filter list with RLE/Dict only
  FilterList filter_list_rle_only(ctx);
  filter_list_rle_only.add_filter(string_filter);

  // Create filter list with other filters also where RLE/Dict is not first
  FilterList filter_list_with_others_not_first(ctx);
  filter_list_with_others_not_first.add_filter(another_filter);
  filter_list_with_others_not_first.add_filter(string_filter);

  // Create filter list with other filters also where RLE/Dict is first
  FilterList filter_list_with_others_first(ctx);
  filter_list_with_others_first.add_filter(string_filter);
  filter_list_with_others_first.add_filter(another_filter);

  // Create filter list with both RLE and Dict, shouldn't be allowed
  // as one of them won't be first
  FilterList filter_list_with_rle_and_dict(ctx);
  Filter rle_filter(ctx, TILEDB_FILTER_RLE);
  Filter dict_filter(ctx, TILEDB_FILTER_DICTIONARY);
  filter_list_with_rle_and_dict.add_filter(dict_filter);
  filter_list_with_rle_and_dict.add_filter(rle_filter);

  {
    // Add dimension that is not var length string
    domain.add_dimension(dim_not_var_string);
    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain);

    CHECK_NOTHROW(schema.set_coords_filter_list(filter_list_rle_only));
    CHECK_NOTHROW(
        schema.set_coords_filter_list(filter_list_with_others_not_first));

    // Add var length string dimension
    domain.add_dimension(dim_var_string);
    schema.set_domain(domain);

    // Test set_coords_filter_list
    {
      // Case 1: There is no more specific filter list for this dimension
      // Adding RLE after other filters to var-length string dimension is not
      // allowed
      CHECK_THROWS(
          schema.set_coords_filter_list(filter_list_with_others_not_first));
      // Should throw as even RLE is first, dictionary isn't
      CHECK_THROWS(
          schema.set_coords_filter_list(filter_list_with_rle_and_dict));
      // If RLE/Dict is first, it's allowed
      CHECK_NOTHROW(
          schema.set_coords_filter_list(filter_list_with_others_first));
      // If only RLE/Dict is used, it's allowed
      CHECK_NOTHROW(schema.set_coords_filter_list(filter_list_rle_only));
    }

    {
      // set coords Case 2: There is a more specific filter, so whatever we set
      // with set coords should not matter
      CHECK_NOTHROW(dim_var_string.set_filter_list(filter_list_rle_only));

      // We need to use another domain, as adding a dimension with the same name
      // doesn't replace the old one
      Domain domain2(ctx);
      domain2.add_dimension(dim_var_string);
      schema.set_domain(domain2);
      CHECK_NOTHROW(
          schema.set_coords_filter_list(filter_list_with_others_not_first));
    }

    // Test set_filter_list
    {
      // Adding RLE/Dict after other filters to var-length string dimension is
      // not allowed
      CHECK_THROWS(
          dim_var_string.set_filter_list(filter_list_with_others_not_first));
      // Should throw as even RLE is first, dictionary isn't
      CHECK_THROWS(
          dim_var_string.set_filter_list(filter_list_with_rle_and_dict));
      // If RLE/Dict is first, it's allowed
      CHECK_NOTHROW(
          dim_var_string.set_filter_list(filter_list_with_others_first));

      // The rest of the cases are allowed
      CHECK_NOTHROW(dim_var_string.set_filter_list(filter_list_rle_only));
      CHECK_NOTHROW(dim_not_var_string.set_filter_list(filter_list_rle_only));
      CHECK_NOTHROW(dim_not_var_string.set_filter_list(
          filter_list_with_others_not_first));
    }
  }
}
