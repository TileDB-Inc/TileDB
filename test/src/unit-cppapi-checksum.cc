/**
 * @file   unit-cppapi-checksum.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
 * Tests the C++ API for checksum validation.
 */

#include <test/support/tdb_catch.h>
#include <fstream>
#include "test/support/src/coords_workaround.h"
#include "test/support/src/helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb::test;

static void check_filters(
    const tiledb::FilterList& answer, const tiledb::FilterList& check) {
  REQUIRE(check.nfilters() == answer.nfilters());
  REQUIRE(check.max_chunk_size() == answer.max_chunk_size());
  for (uint32_t i = 0; i < check.nfilters(); i++) {
    auto f_answer = answer.filter(i), f_check = check.filter(i);
    REQUIRE(f_check.filter_type() == f_answer.filter_type());
  }
}

static void run_checksum_test(tiledb_filter_type_t filter_type) {
  using namespace tiledb;
  Context& ctx = vanilla_context_cpp();
  Context ctx2;
  VFS vfs(ctx);
  std::string array_name = "cpp_unit_array";

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create schema with filter lists
  FilterList a1_filters(ctx);
  a1_filters.add_filter({ctx, filter_type});

  FilterList a2_filters(ctx);
  a2_filters.add_filter({ctx, filter_type});

  auto a1 = Attribute::create<int>(ctx, "a1");
  auto a2 = Attribute::create<std::string>(ctx, "a2");
  a1.set_filter_list(a1_filters);
  a2.set_filter_list(a2_filters);

  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "d1", {{0, 100}}, 10);
  auto d2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 10);
  domain.add_dimensions(d1, d2);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attributes(a1, a2);

  FilterList offsets_filters(ctx);
  offsets_filters.add_filter({ctx, filter_type});
  schema.set_coords_filter_list(a1_filters)
      .set_offsets_filter_list(offsets_filters);

  // Create array
  Array::create(array_name, schema);

  // Write to array
  std::vector<int> a1_data = {1, 2};
  std::vector<std::string> a2_data = {"abc", "defg"};
  auto a2buf = ungroup_var_buffer(a2_data);
  std::vector<int> coords = {0, 0, 10, 10};
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_data_buffer("a1", a1_data)
      .set_data_buffer("a2", a2buf.second)
      .set_offsets_buffer("a2", a2buf.first)
      .set_data_buffer("__coords", coords)
      .set_layout(TILEDB_UNORDERED);
  REQUIRE(query.submit() == Query::Status::COMPLETE);

  REQUIRE(query.fragment_num() == 1);
  std::string fragment_uri = query.fragment_uri(0);
  array.close();

  // Sanity check reading before corrupting data
  array.open(TILEDB_READ);
  std::vector<int> subarray = {0, 10, 0, 10};
  std::vector<int> coords_read(4);
  std::vector<int> a1_read(2);
  std::vector<uint64_t> a2_read_off(2);
  std::string a2_read_data;
  a2_read_data.resize(7);
  Query query_r(ctx2, array);
  Subarray sub(ctx, array);
  sub.set_subarray(subarray);
  query_r.set_subarray(sub)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer(tiledb::test::TILEDB_COORDS, coords_read)
      .set_data_buffer("a1", a1_read)
      .set_data_buffer("a2", a2_read_data)
      .set_offsets_buffer("a2", a2_read_off);
  REQUIRE(query_r.submit() == Query::Status::COMPLETE);
  array.close();
  auto ret = query_r.result_buffer_elements();
  REQUIRE(ret.size() == 3);
  REQUIRE(ret["a1"].first == 0);
  REQUIRE(ret["a1"].second == 2);
  REQUIRE(ret["a2"].first == 2);
  REQUIRE(ret["a2"].second == 7);
  REQUIRE(coords_read[0] == 0);
  REQUIRE(coords_read[1] == 0);
  REQUIRE(coords_read[2] == 10);
  REQUIRE(coords_read[3] == 10);
  REQUIRE(a1_read[0] == 1);
  REQUIRE(a1_read[1] == 2);
  REQUIRE(a2_read_off[0] == 0);
  REQUIRE(a2_read_off[1] == 3);
  REQUIRE(a2_read_data.substr(0, 7) == "abcdefg");

  // Check reading filter lists.
  array.open(TILEDB_READ);
  auto schema_r = array.schema();
  check_filters(a1_filters, schema_r.coords_filter_list());
  check_filters(offsets_filters, schema_r.offsets_filter_list());
  check_filters(a1_filters, schema_r.attribute("a1").filter_list());
  check_filters(a2_filters, schema_r.attribute("a2").filter_list());
  array.close();

  // Corrupt a byte of a1, we should then get a checksum failure
  std::string file_a;
  file_a = fragment_uri + "/a1.tdb";

  auto file_size = vfs.file_size(file_a);

#ifdef _WIN32
  std::string prefix = "file:///";
#else
  std::string prefix = "file://";
#endif
  file_a = file_a.substr(prefix.length(), file_a.length() - prefix.length());
  // Open A
  std::fstream fbuf;
  fbuf.open(file_a, std::ios::in | std::ios::out | std::ios::binary);
  REQUIRE(fbuf.is_open());

  // We will change 2 to 3
  uint32_t corruption = 3;
  // Seek to the start of the last value
  fbuf.seekp(file_size - sizeof(corruption), std::ios_base::beg);
  fbuf.write(reinterpret_cast<const char*>(&corruption), sizeof(corruption));
  REQUIRE(fbuf.fail() != true);

  // Flush and close
  fbuf.flush();
  fbuf.close();

  // Reading will now fail because we have corrupted a1 to be {1, 3} instead of
  // {1, 2}
  array.open(TILEDB_READ);
  subarray = {0, 10, 0, 10};
  std::vector<int32_t> coords_read2(4);
  std::vector<int32_t> a1_read2(2);
  std::vector<uint64_t> a2_read_off2(2);
  std::string a2_read_data2;
  a2_read_data2.resize(7);
  Query query_r2(ctx, array);
  Subarray sub2(ctx, array);
  sub2.set_subarray(subarray);
  query_r2.set_subarray(sub2)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer(tiledb::test::TILEDB_COORDS, coords_read2)
      .set_data_buffer("a1", a1_read2)
      .set_data_buffer("a2", a2_read_data2)
      .set_offsets_buffer("a2", a2_read_off2);
  // Will throw checksum mismatch for filter error
  REQUIRE_THROWS_AS(query_r2.submit(), TileDBError);
  array.close();

  // Clean up
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE("C++ API: MD5 checksum on array", "[cppapi][checksum][md5]") {
  run_checksum_test(TILEDB_FILTER_CHECKSUM_MD5);
}

TEST_CASE("C++ API: SHA256 checksum on array", "[cppapi][checksum][sha256]") {
  run_checksum_test(TILEDB_FILTER_CHECKSUM_SHA256);
}
