/**
 * @file unit-cppapi-fragment_info.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
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
 * Tests the C++ API functions for manipulating fragment information.
 */

#include "test/src/helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

#include <catch.hpp>
#include <iostream>

using namespace tiledb;
using namespace tiledb::test;

const std::string array_name = "fragment_info_array";

TEST_CASE(
    "C++ API: Test fragment info, errors", "[cppapi][fragment_info][errors]") {
  // Create TileDB context
  Context ctx;
  VFS vfs(ctx);
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());

  // Create fragment info object
  FragmentInfo fragment_info(ctx, array_name);

  // Error if array does not exist
  CHECK_THROWS(fragment_info.load());

  // Create array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx.ptr().get(),
      array_name,
      TILEDB_DENSE,
      {"d"},
      {TILEDB_UINT64},
      {domain},
      {&tile_extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  // Array is not encrypted
  std::string key = "12345678901234567890123456789012";
  CHECK_THROWS(fragment_info.load(TILEDB_AES_256_GCM, key));

  // Write a dense fragment
  QueryBuffers buffers;
  uint64_t subarray[] = {1, 6};
  std::vector<int32_t> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(
      ctx.ptr().get(), array_name, 1, subarray, TILEDB_ROW_MAJOR, buffers);

  // Load again
  fragment_info.load();

  // Get fragment URI
  std::string uri;
  CHECK_THROWS(uri = fragment_info.fragment_uri(1));

  // Get non-empty domain, invalid index and name
  std::vector<uint64_t> non_empty_dom(2);
  CHECK_THROWS(fragment_info.get_non_empty_domain(0, 1, &non_empty_dom[0]));
  CHECK_THROWS(fragment_info.get_non_empty_domain(0, "foo", &non_empty_dom[0]));

  // Var-sized non-empty domain getters should error out
  std::pair<std::string, std::string> non_empty_domain_str;
  CHECK_THROWS(non_empty_domain_str = fragment_info.non_empty_domain_var(0, 0));
  CHECK_THROWS(
      non_empty_domain_str = fragment_info.non_empty_domain_var(0, "d"));

  // Clean up
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
}

TEST_CASE(
    "C++ API: Test fragment info, load and getters",
    "[cppapi][fragment_info][load][getters]") {
  // Create TileDB context
  Context ctx;
  VFS vfs(ctx);

  // Create array
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx.ptr().get(),
      array_name,
      TILEDB_DENSE,
      {"d"},
      {TILEDB_UINT64},
      {domain},
      {&tile_extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  // Create fragment info object
  FragmentInfo fragment_info(ctx, array_name);

  // Load fragment info
  fragment_info.load();

  // No fragments yet
  auto fragment_num = fragment_info.fragment_num();
  CHECK(fragment_num == 0);

  // Write a dense fragment
  QueryBuffers buffers;
  uint64_t subarray[] = {1, 6};
  std::vector<int32_t> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(
      ctx.ptr().get(), array_name, 1, subarray, TILEDB_ROW_MAJOR, buffers);

  // Load fragment info again
  fragment_info.load();

  // Get fragment num again
  fragment_num = fragment_info.fragment_num();
  CHECK(fragment_num == 1);

  // Write a sparse fragment
  a = {11, 12, 13, 14};
  a_size = a.size() * sizeof(int32_t);
  std::vector<uint64_t> d = {1, 3, 5, 7};
  uint64_t d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(
      ctx.ptr().get(),
      array_name,
      2,
      TILEDB_UNORDERED,
      buffers,
      &written_frag_uri);

  // Write another sparse fragment
  a = {21, 22, 23};
  a_size = a.size() * sizeof(int32_t);
  d = {2, 4, 9};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  write_array(ctx.ptr().get(), array_name, 3, TILEDB_UNORDERED, buffers);

  // Load fragment info again
  fragment_info.load();

  // Get fragment num again
  fragment_num = fragment_info.fragment_num();
  CHECK(fragment_num == 3);

  // Get fragment URI
  auto uri = fragment_info.fragment_uri(1);
  CHECK(uri == written_frag_uri);

  // Get fragment size
  auto size = fragment_info.fragment_size(1);
  CHECK(size == 1708);

  // Get dense / sparse
  auto dense = fragment_info.dense(0);
  CHECK(dense == true);
  auto sparse = fragment_info.sparse(0);
  CHECK(sparse == false);
  dense = fragment_info.dense(1);
  CHECK(dense == false);
  sparse = fragment_info.sparse(1);
  CHECK(sparse == true);

  // Get timestamp range
  auto range = fragment_info.timestamp_range(1);
  CHECK(range.first == 2);
  CHECK(range.second == 2);

  // Get non-empty domain
  std::vector<uint64_t> non_empty_dom(2);
  fragment_info.get_non_empty_domain(0, 0, &non_empty_dom[0]);
  CHECK(non_empty_dom == std::vector<uint64_t>{1, 6});
  fragment_info.get_non_empty_domain(1, 0, &non_empty_dom[0]);
  CHECK(non_empty_dom == std::vector<uint64_t>{1, 7});
  fragment_info.get_non_empty_domain(2, 0, &non_empty_dom[0]);
  CHECK(non_empty_dom == std::vector<uint64_t>{2, 9});
  fragment_info.get_non_empty_domain(1, "d", &non_empty_dom[0]);
  CHECK(non_empty_dom == std::vector<uint64_t>{1, 7});

  // Get number of cells
  auto cell_num = fragment_info.cell_num(0);
  CHECK(cell_num == 10);
  cell_num = fragment_info.cell_num(1);
  CHECK(cell_num == 4);
  cell_num = fragment_info.cell_num(2);
  CHECK(cell_num == 3);

  // Get number of MBRs
  auto mbr_num = fragment_info.mbr_num(0);
  CHECK(mbr_num == 0);
  mbr_num = fragment_info.mbr_num(1);
  CHECK(mbr_num == 2);
  mbr_num = fragment_info.mbr_num(2);
  CHECK(mbr_num == 2);

  // Get MBR from index
  std::vector<uint64_t> mbr(2);
  fragment_info.get_mbr(1, 0, 0, &mbr[0]);
  CHECK(mbr == std::vector<uint64_t>{1, 3});
  fragment_info.get_mbr(1, 1, 0, &mbr[0]);
  CHECK(mbr == std::vector<uint64_t>{5, 7});
  fragment_info.get_mbr(2, 0, 0, &mbr[0]);
  CHECK(mbr == std::vector<uint64_t>{2, 4});
  fragment_info.get_mbr(2, 1, 0, &mbr[0]);
  CHECK(mbr == std::vector<uint64_t>{9, 9});

  // Get MBR from name
  fragment_info.get_mbr(1, 0, "d", &mbr[0]);
  CHECK(mbr == std::vector<uint64_t>{1, 3});
  fragment_info.get_mbr(1, 1, "d", &mbr[0]);
  CHECK(mbr == std::vector<uint64_t>{5, 7});
  fragment_info.get_mbr(2, 0, "d", &mbr[0]);
  CHECK(mbr == std::vector<uint64_t>{2, 4});
  fragment_info.get_mbr(2, 1, "d", &mbr[0]);
  CHECK(mbr == std::vector<uint64_t>{9, 9});

  // Get version
  auto version = fragment_info.version(0);
  CHECK(version == tiledb::sm::constants::format_version);

  // Clean up
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
}

TEST_CASE(
    "C++ API: Test fragment info, load from array with string dimension",
    "[cppapi][fragment_info][load][string-dim]") {
  // Create TileDB context
  Context ctx;
  VFS vfs(ctx);
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());

  // Create array
  create_array(
      ctx.ptr().get(),
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  // Write a sparse fragment
  QueryBuffers buffers;
  std::vector<int32_t> a = {11, 12, 13, 14};
  uint64_t a_size = a.size() * sizeof(int32_t);
  std::string d_val("abbcddd");
  uint64_t d_val_size = d_val.size();
  std::vector<uint64_t> d_off = {0, 1, 3, 4};
  uint64_t d_off_size = d_off.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] =
      tiledb::test::QueryBuffer({&d_off[0], d_off_size, &d_val[0], d_val_size});
  std::string written_frag_uri;
  write_array(
      ctx.ptr().get(),
      array_name,
      1,
      TILEDB_UNORDERED,
      buffers,
      &written_frag_uri);

  // Create fragment info object
  FragmentInfo fragment_info(ctx, array_name);

  // Load fragment info
  fragment_info.load();

  // Get non-empty domain
  auto non_empty_domain_str = fragment_info.non_empty_domain_var(0, 0);
  CHECK(std::string("a") == non_empty_domain_str.first);
  CHECK(std::string("ddd") == non_empty_domain_str.second);
  non_empty_domain_str = fragment_info.non_empty_domain_var(0, "d");
  CHECK(std::string("a") == non_empty_domain_str.first);
  CHECK(std::string("ddd") == non_empty_domain_str.second);

  // Get number of MBRs
  auto mbr_num = fragment_info.mbr_num(0);
  CHECK(mbr_num == 2);

  // Get MBR
  auto mbr_str = fragment_info.mbr_var(0, 0, 0);
  CHECK(std::string("a") == mbr_str.first);
  CHECK(std::string("bb") == mbr_str.second);
  mbr_str = fragment_info.mbr_var(0, 1, "d");
  CHECK(std::string("c") == mbr_str.first);
  CHECK(std::string("ddd") == mbr_str.second);

  // Clean up
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
}

TEST_CASE(
    "C++ API: Test fragment info, consolidated fragment metadata",
    "[cppapi][fragment_info][consolidated-metadata]") {
  // Create TileDB context
  Context ctx;
  VFS vfs(ctx);

  // Create array
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx.ptr().get(),
      array_name,
      TILEDB_DENSE,
      {"d"},
      {TILEDB_UINT64},
      {domain},
      {&tile_extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  // Write a sparse fragment
  QueryBuffers buffers;
  std::vector<int32_t> a = {11, 12, 13, 14};
  uint64_t a_size = a.size() * sizeof(int32_t);
  std::vector<uint64_t> d = {1, 3, 5, 7};
  uint64_t d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(
      ctx.ptr().get(),
      array_name,
      1,
      TILEDB_UNORDERED,
      buffers,
      &written_frag_uri);

  // Write another sparse fragment
  a = {21, 22, 23};
  a_size = a.size() * sizeof(int32_t);
  d = {2, 4, 9};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  write_array(ctx.ptr().get(), array_name, 2, TILEDB_UNORDERED, buffers);

  // Create fragment info object
  FragmentInfo fragment_info(ctx, array_name);

  // Load fragment info
  fragment_info.load();

  // Check for consolidated metadata
  auto has = fragment_info.has_consolidated_metadata(0);
  CHECK(has == false);
  has = fragment_info.has_consolidated_metadata(1);
  CHECK(has == false);
  CHECK_THROWS(has = fragment_info.has_consolidated_metadata(2));

  // Get number of unconsolidated fragment metadata
  auto unconsolidated = fragment_info.unconsolidated_metadata_num();
  CHECK(unconsolidated == 2);

  // Consolidate fragment metadata
  Config config;
  config["sm.consolidation.mode"] = "fragment_meta";
  Array::consolidate(ctx, array_name, &config);

  // Load fragment info
  fragment_info.load();

  // Check for consolidated metadata
  has = fragment_info.has_consolidated_metadata(0);
  CHECK(has == true);
  has = fragment_info.has_consolidated_metadata(1);
  CHECK(has == true);

  // Get number of unconsolidated fragment metadata
  unconsolidated = fragment_info.unconsolidated_metadata_num();
  CHECK(unconsolidated == 0);

  // Write another sparse fragment
  a = {31, 32, 33};
  a_size = a.size() * sizeof(int32_t);
  d = {1, 3, 5};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  write_array(ctx.ptr().get(), array_name, 3, TILEDB_UNORDERED, buffers);

  // Load fragment info
  fragment_info.load();

  // Check for consolidated metadata
  has = fragment_info.has_consolidated_metadata(0);
  CHECK(has == true);
  has = fragment_info.has_consolidated_metadata(1);
  CHECK(has == true);
  has = fragment_info.has_consolidated_metadata(2);
  CHECK(has == false);

  // Get number of unconsolidated fragment metadata
  unconsolidated = fragment_info.unconsolidated_metadata_num();
  CHECK(unconsolidated == 1);

  // Clean up
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
}

TEST_CASE(
    "C++ API: Test fragment info, to vacuum",
    "[cppapi][fragment_info][to-vacuum]") {
  // Create TileDB context
  Context ctx;
  VFS vfs(ctx);

  // Create array
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx.ptr().get(),
      array_name,
      TILEDB_DENSE,
      {"d"},
      {TILEDB_UINT64},
      {domain},
      {&tile_extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  // Write a sparse fragment
  QueryBuffers buffers;
  std::vector<int32_t> a = {11, 12, 13, 14};
  uint64_t a_size = a.size() * sizeof(int32_t);
  std::vector<uint64_t> d = {1, 3, 5, 7};
  uint64_t d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(
      ctx.ptr().get(),
      array_name,
      1,
      TILEDB_UNORDERED,
      buffers,
      &written_frag_uri);

  // Write another sparse fragment
  a = {21, 22, 23};
  a_size = a.size() * sizeof(int32_t);
  d = {2, 4, 9};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  write_array(ctx.ptr().get(), array_name, 2, TILEDB_UNORDERED, buffers);

  // Create fragment info object
  FragmentInfo fragment_info(ctx, array_name);

  // Load fragment info
  fragment_info.load();

  // Get number of fragments to vacuum
  auto to_vacuum_num = fragment_info.to_vacuum_num();
  CHECK(to_vacuum_num == 0);

  // Get to vacuum fragment URI - should error out
  std::string to_vacuum_uri;
  CHECK_THROWS(to_vacuum_uri = fragment_info.to_vacuum_uri(0));

  // Consolidate fragments
  Config config;
  config["sm.consolidation.mode"] = "fragments";
  Array::consolidate(ctx, array_name, &config);

  // Load fragment info
  fragment_info.load();

  // Get consolidated fragment URI
  auto uri = fragment_info.fragment_uri(0);
  CHECK(uri.find_last_of("__1_2") != std::string::npos);

  // Get number of fragments to vacuum
  to_vacuum_num = fragment_info.to_vacuum_num();
  CHECK(to_vacuum_num == 2);

  // Get to vacuum fragment URI
  to_vacuum_uri = fragment_info.to_vacuum_uri(0);
  CHECK(to_vacuum_uri == written_frag_uri);

  // Write another sparse fragment
  a = {31, 32, 33};
  a_size = a.size() * sizeof(int32_t);
  d = {1, 3, 5};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  write_array(ctx.ptr().get(), array_name, 3, TILEDB_UNORDERED, buffers);

  // Load fragment info
  fragment_info.load();

  // Get number of fragments to vacuum
  to_vacuum_num = fragment_info.to_vacuum_num();
  CHECK(to_vacuum_num == 2);

  // Vacuum
  Array::vacuum(ctx, array_name);

  // Load fragment info
  fragment_info.load();

  // Get number of fragments to vacuum
  to_vacuum_num = fragment_info.to_vacuum_num();
  CHECK(to_vacuum_num == 0);

  // Clean up
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
}

TEST_CASE(
    "C++ API: Test fragment info, dump", "[cppapi][fragment_info][dump]") {
  // Create TileDB context
  Context ctx;
  VFS vfs(ctx);

  // Create array
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx.ptr().get(),
      array_name,
      TILEDB_DENSE,
      {"d"},
      {TILEDB_UINT64},
      {domain},
      {&tile_extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  // Write a dense fragment
  QueryBuffers buffers;
  uint64_t subarray[] = {1, 6};
  std::vector<int32_t> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  std::string written_frag_uri_1;
  write_array(
      ctx.ptr().get(),
      array_name,
      1,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri_1);

  // Write a sparse fragment
  a = {11, 12, 13, 14};
  a_size = a.size() * sizeof(int32_t);
  std::vector<uint64_t> d = {1, 3, 5, 7};
  uint64_t d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri_2;
  write_array(
      ctx.ptr().get(),
      array_name,
      2,
      TILEDB_UNORDERED,
      buffers,
      &written_frag_uri_2);

  // Write another sparse fragment
  a = {21, 22, 23};
  a_size = a.size() * sizeof(int32_t);
  d = {2, 4, 9};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri_3;
  write_array(
      ctx.ptr().get(),
      array_name,
      3,
      TILEDB_UNORDERED,
      buffers,
      &written_frag_uri_3);

  // Create fragment info object
  FragmentInfo fragment_info(ctx, array_name);

  // Load fragment info
  fragment_info.load();

  // Check dump
  std::string dump_str =
      std::string("- Fragment num: 3\n") +
      "- Unconsolidated metadata num: 3\n" + "- To vacuum num: 0\n" +
      "- Fragment #1:\n" + "  > URI: " + written_frag_uri_1 + "\n" +
      "  > Type: dense\n" + "  > Non-empty domain: [1, 6]\n" +
      "  > Size: 1584\n" + "  > Cell num: 10\n" +
      "  > Timestamp range: [1, 1]\n" + "  > Format version: 9\n" +
      "  > Has consolidated metadata: no\n" + "- Fragment #2:\n" +
      "  > URI: " + written_frag_uri_2 + "\n" + "  > Type: sparse\n" +
      "  > Non-empty domain: [1, 7]\n" + "  > Size: 1708\n" +
      "  > Cell num: 4\n" + "  > Timestamp range: [2, 2]\n" +
      "  > Format version: 9\n" + "  > Has consolidated metadata: no\n" +
      "- Fragment #3:\n" + "  > URI: " + written_frag_uri_3 + "\n" +
      "  > Type: sparse\n" + "  > Non-empty domain: [2, 9]\n" +
      "  > Size: 1696\n" + "  > Cell num: 3\n" +
      "  > Timestamp range: [3, 3]\n" + "  > Format version: 9\n" +
      "  > Has consolidated metadata: no\n";
  FILE* gold_fout = fopen("gold_fout.txt", "w");
  const char* dump = dump_str.c_str();
  fwrite(dump, sizeof(char), strlen(dump), gold_fout);
  fclose(gold_fout);
  FILE* fout = fopen("fout.txt", "w");
  fragment_info.dump(fout);
  fclose(fout);
#ifdef _WIN32
  CHECK(!system("FC gold_fout.txt fout.txt > nul"));
#else
  CHECK(!system("diff gold_fout.txt fout.txt"));
#endif
  CHECK_NOTHROW(vfs.remove_file("gold_fout.txt"));
  CHECK_NOTHROW(vfs.remove_file("fout.txt"));

  // Clean up
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
}
