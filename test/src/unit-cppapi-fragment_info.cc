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
#include "test/src/serialization_wrappers.h"
#include "tiledb/sm/cpp_api/tiledb"

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb;
using namespace tiledb::test;

const std::string array_name = "fragment_info_array_cpp";

TEST_CASE(
    "C++ API: Test fragment info, errors", "[cppapi][fragment_info][errors]") {
  // Create TileDB context
  std::string key = "12345678901234567890123456789012";
  tiledb::Config cfg;
  cfg["sm.encryption_type"] = "AES_256_GCM";
  cfg["sm.encryption_key"] = key.c_str();
  Context ctx(cfg);
  VFS vfs(ctx);

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Error if array does not exist
    CHECK_THROWS(fragment_info.load());
  }

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

  // Write a dense fragment
  QueryBuffers buffers;
  uint64_t subarray[] = {1, 6};
  std::vector<int32_t> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(
      ctx.ptr().get(), array_name, 1, subarray, TILEDB_ROW_MAJOR, buffers);
  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info, array is encrypted
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Get fragment URI
    std::string uri;
    CHECK_THROWS(uri = fragment_info.fragment_uri(1));

    // Get fragment name
    std::string name;
    CHECK_THROWS(name = fragment_info.fragment_name(1));

    // Get non-empty domain, invalid index and name
    std::vector<uint64_t> non_empty_dom(2);
    CHECK_THROWS(fragment_info.get_non_empty_domain(0, 1, &non_empty_dom[0]));
    CHECK_THROWS(
        fragment_info.get_non_empty_domain(0, "foo", &non_empty_dom[0]));

    // Var-sized non-empty domain getters should error out
    std::pair<std::string, std::string> non_empty_domain_str;
    CHECK_THROWS(
        non_empty_domain_str = fragment_info.non_empty_domain_var(0, 0));
    CHECK_THROWS(
        non_empty_domain_str = fragment_info.non_empty_domain_var(0, "d"));
  }

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

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // No fragments yet
    auto fragment_num = fragment_info.fragment_num();
    CHECK(fragment_num == 0);
  }

  // Write a dense fragment
  QueryBuffers buffers;
  uint64_t subarray[] = {1, 6};
  std::vector<int32_t> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(
      ctx.ptr().get(), array_name, 1, subarray, TILEDB_ROW_MAJOR, buffers);

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info again
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Get fragment num again
    auto fragment_num = fragment_info.fragment_num();
    CHECK(fragment_num == 1);

    auto total_cell_num = fragment_info.total_cell_num();
    CHECK(total_cell_num == 10);
  }

  // Write another dense fragment
  subarray[0] = 1;
  subarray[1] = 7;
  a = {7, 1, 2, 3, 4, 5, 6};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(
      ctx.ptr().get(),
      array_name,
      2,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri);

  // Write another dense fragment
  subarray[0] = 2;
  subarray[1] = 9;
  a = {6, 7, 1, 2, 3, 4, 5, 6};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(
      ctx.ptr().get(), array_name, 3, subarray, TILEDB_ROW_MAJOR, buffers);

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info again
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Get fragment num again
    auto fragment_num = fragment_info.fragment_num();
    CHECK(fragment_num == 3);

    // Get fragment URI
    auto uri = fragment_info.fragment_uri(1);
    CHECK(uri == written_frag_uri);

    // Get fragment schema name
    auto schema_name = fragment_info.array_schema_name(1);
    CHECK(schema_name.size() == 62);

    // Get fragment size
    auto size = fragment_info.fragment_size(1);
    CHECK(size == 3202);

    // Get dense / sparse
    auto dense = fragment_info.dense(0);
    CHECK(dense == true);
    auto sparse = fragment_info.sparse(0);
    CHECK(sparse == false);
    dense = fragment_info.dense(1);
    CHECK(dense == true);
    sparse = fragment_info.sparse(1);
    CHECK(sparse == false);

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
    auto frag0_cell_num = fragment_info.cell_num(0);
    CHECK(frag0_cell_num == 10);
    auto frag1_cell_num = fragment_info.cell_num(1);
    CHECK(frag1_cell_num == 10);
    auto frag2_cell_num = fragment_info.cell_num(2);
    CHECK(frag2_cell_num == 10);

    auto total_cell_num = fragment_info.total_cell_num();
    CHECK(total_cell_num == frag0_cell_num + frag1_cell_num + frag2_cell_num);

    // Get number of MBRs - should always be 0 since it's a dense array
    auto mbr_num = fragment_info.mbr_num(0);
    CHECK(mbr_num == 0);
    mbr_num = fragment_info.mbr_num(1);
    CHECK(mbr_num == 0);
    mbr_num = fragment_info.mbr_num(2);
    CHECK(mbr_num == 0);

    // Get MBR from index - should fail since it's a dense array
    std::vector<uint64_t> mbr(2);
    CHECK_THROWS(fragment_info.get_mbr(1, 0, 0, &mbr[0]));
    CHECK_THROWS(fragment_info.get_mbr(1, 1, 0, &mbr[0]));
    CHECK_THROWS(fragment_info.get_mbr(2, 0, 0, &mbr[0]));
    CHECK_THROWS(fragment_info.get_mbr(2, 1, 0, &mbr[0]));

    // Get MBR from name - should fail since it's a dense array
    CHECK_THROWS(fragment_info.get_mbr(1, 0, "d", &mbr[0]));
    CHECK_THROWS(fragment_info.get_mbr(1, 1, "d", &mbr[0]));
    CHECK_THROWS(fragment_info.get_mbr(2, 0, "d", &mbr[0]));
    CHECK_THROWS(fragment_info.get_mbr(2, 1, "d", &mbr[0]));

    // Get version
    auto version = fragment_info.version(0);
    CHECK(version == tiledb::sm::constants::format_version);
  }

  // Clean up
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
}

TEST_CASE("C++ API: Test MBR fragment info", "[cppapi][fragment_info][mbr]") {
  // Create TileDB context
  Context ctx;
  VFS vfs(ctx);

  // Create sparse array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx.ptr().get(),
      array_name,
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {domain, domain},
      {&tile_extent, &tile_extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  // Write a sparse fragment
  QueryBuffers buffers;
  std::vector<int32_t> a = {1, 2};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  std::vector<int64_t> d1 = {1, 2};
  uint64_t d1_size = d1.size() * sizeof(uint64_t);
  buffers["d1"] = tiledb::test::QueryBuffer({&d1[0], d1_size, nullptr, 0});
  std::vector<int64_t> d2 = {1, 2};
  uint64_t d2_size = d2.size() * sizeof(uint64_t);
  buffers["d2"] = tiledb::test::QueryBuffer({&d2[0], d2_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(
      ctx.ptr().get(),
      array_name,
      1,
      TILEDB_UNORDERED,
      buffers,
      &written_frag_uri);

  // Write a second sparse fragment
  std::vector<int64_t> a2 = {9, 10, 11, 12};
  a_size = a2.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a2[0], a_size, nullptr, 0});
  std::vector<int64_t> d1b = {1, 2, 7, 8};
  uint64_t d1b_size = d1b.size() * sizeof(uint64_t);
  buffers["d1"] = tiledb::test::QueryBuffer({&d1b[0], d1b_size, nullptr, 0});
  std::vector<int64_t> d2b = {1, 2, 7, 8};
  uint64_t d2b_size = d2b.size() * sizeof(uint64_t);
  buffers["d2"] = tiledb::test::QueryBuffer({&d2b[0], d2b_size, nullptr, 0});
  write_array(
      ctx.ptr().get(),
      array_name,
      2,
      TILEDB_UNORDERED,
      buffers,
      &written_frag_uri);

  // Write a third sparse fragment
  std::vector<int64_t> a3 = {5, 6, 7, 8};
  a_size = a3.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a3[0], a_size, nullptr, 0});
  std::vector<int64_t> d1c = {1, 2, 7, 1};
  uint64_t d1c_size = d1c.size() * sizeof(uint64_t);
  buffers["d1"] = tiledb::test::QueryBuffer({&d1c[0], d1c_size, nullptr, 0});
  std::vector<int64_t> d2c = {1, 2, 7, 8};
  uint64_t d2c_size = d2c.size() * sizeof(uint64_t);
  buffers["d2"] = tiledb::test::QueryBuffer({&d2c[0], d2c_size, nullptr, 0});
  write_array(
      ctx.ptr().get(),
      array_name,
      3,
      TILEDB_UNORDERED,
      buffers,
      &written_frag_uri);

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);
    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }

    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
      for (uint32_t fid = 0; fid < fragment_info.fragment_num(); ++fid) {
        fragment_info.mbr_num(fid);
      }
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    auto fragment_num = fragment_info.fragment_num();
    CHECK(fragment_num == 3);

    // Test get number of MBRs API

    auto mbr_num = fragment_info.mbr_num(0);
    CHECK(mbr_num == 1);
    mbr_num = fragment_info.mbr_num(1);
    CHECK(mbr_num == 2);
    mbr_num = fragment_info.mbr_num(2);
    CHECK(mbr_num == 2);
    // 3 is out of fragment_info bounds
    CHECK_THROWS(fragment_info.mbr_num(3));

    // Test get MBR from index API
    std::vector<uint64_t> mbr(2);
    fragment_info.get_mbr(0, 0, 0, &mbr[0]);
    CHECK(mbr == std::vector<uint64_t>{1, 2});

    // Test get MBR from name API
    fragment_info.get_mbr(1, 1, "d1", &mbr[0]);
    CHECK(mbr == std::vector<uint64_t>{7, 8});
  }

  // Clean up
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
}

TEST_CASE(
    "C++ API: Test fragment info, load from array with string dimension",
    "[cppapi][fragment_info][load][string-dims][mbr]") {
  // Create TileDB context
  Context ctx;
  Config cfg;
  VFS vfs(ctx);

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

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;

    SECTION("no serialization") {
      serialized_load = false;
    }

    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
      for (uint32_t fid = 0; fid < fragment_info.fragment_num(); ++fid) {
        fragment_info.mbr_num(fid);
      }
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

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
  }

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
  uint64_t subarray[] = {1, 2};
  std::vector<int32_t> a = {1, 2};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(
      ctx.ptr().get(),
      array_name,
      1,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri);

  // Write another dense fragment
  subarray[0] = 3;
  subarray[1] = 4;
  a = {4, 5};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(
      ctx.ptr().get(), array_name, 2, subarray, TILEDB_ROW_MAJOR, buffers);

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Check for consolidated metadata
    auto has = fragment_info.has_consolidated_metadata(0);
    CHECK(has == false);
    has = fragment_info.has_consolidated_metadata(1);
    CHECK(has == false);
    CHECK_THROWS(has = fragment_info.has_consolidated_metadata(2));

    // Get number of unconsolidated fragment metadata
    auto unconsolidated = fragment_info.unconsolidated_metadata_num();
    CHECK(unconsolidated == 2);
  }

  // Consolidate fragment metadata
  Config config;
  config["sm.consolidation.mode"] = "fragment_meta";
  Array::consolidate(ctx, array_name, &config);

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Check for consolidated metadata
    auto has = fragment_info.has_consolidated_metadata(0);
    CHECK(has == true);
    has = fragment_info.has_consolidated_metadata(1);
    CHECK(has == true);

    // Get number of unconsolidated fragment metadata
    auto unconsolidated = fragment_info.unconsolidated_metadata_num();
    CHECK(unconsolidated == 0);
  }

  // Write another dense fragment
  subarray[0] = 3;
  subarray[1] = 4;
  a = {4, 7};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(
      ctx.ptr().get(), array_name, 3, subarray, TILEDB_ROW_MAJOR, buffers);

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Check for consolidated metadata
    auto has = fragment_info.has_consolidated_metadata(0);
    CHECK(has == true);
    has = fragment_info.has_consolidated_metadata(1);
    CHECK(has == true);
    has = fragment_info.has_consolidated_metadata(2);
    CHECK(has == false);

    // Get number of unconsolidated fragment metadata
    auto unconsolidated = fragment_info.unconsolidated_metadata_num();
    CHECK(unconsolidated == 1);
  }

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
  uint64_t subarray[] = {1, 4};
  std::vector<int32_t> a = {11, 12, 13, 14};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(
      ctx.ptr().get(),
      array_name,
      1,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri);

  // Write another dense fragment
  subarray[0] = 5;
  subarray[1] = 7;
  a = {21, 22, 23};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(
      ctx.ptr().get(), array_name, 2, subarray, TILEDB_ROW_MAJOR, buffers);

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Get number of fragments to vacuum
    auto to_vacuum_num = fragment_info.to_vacuum_num();
    CHECK(to_vacuum_num == 0);

    // Get to vacuum fragment URI - should error out
    std::string to_vacuum_uri;
    CHECK_THROWS(to_vacuum_uri = fragment_info.to_vacuum_uri(0));
  }

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Consolidate fragments
    Config config;
    config["sm.consolidation.mode"] = "fragments";
    Array::consolidate(ctx, array_name, &config);

    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Get consolidated fragment URI
    auto uri = fragment_info.fragment_uri(0);
    CHECK(uri.find_last_of("__1_2") != std::string::npos);

    // Get number of fragments to vacuum
    auto to_vacuum_num = fragment_info.to_vacuum_num();
    CHECK(to_vacuum_num == 2);

    // Get to vacuum fragment URI
    auto to_vacuum_uri = fragment_info.to_vacuum_uri(0);
    CHECK(to_vacuum_uri == written_frag_uri);
  }

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Write another dense fragment
    subarray[0] = 1;
    subarray[1] = 3;
    a = {31, 32, 33};
    a_size = a.size() * sizeof(int32_t);
    buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
    write_array(
        ctx.ptr().get(), array_name, 3, subarray, TILEDB_ROW_MAJOR, buffers);

    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Get number of fragments to vacuum
    auto to_vacuum_num = fragment_info.to_vacuum_num();
    CHECK(to_vacuum_num == 2);
  }

  // Vacuum
  Array::vacuum(ctx, array_name);

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Get number of fragments to vacuum
    auto to_vacuum_num = fragment_info.to_vacuum_num();
    CHECK(to_vacuum_num == 0);
  }

  // Clean up
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
}

TEST_CASE(
    "C++ API: Test fragment info, dump", "[cppapi][fragment_info][dump]") {
  // Create TileDB context
  Context ctx;
  VFS vfs(ctx);

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

  // Write another dense fragment
  subarray[0] = 1;
  subarray[1] = 4;
  a = {11, 12, 13, 14};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  std::string written_frag_uri_2;
  write_array(
      ctx.ptr().get(),
      array_name,
      2,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri_2);

  // Write another dense fragment
  subarray[0] = 5;
  subarray[1] = 6;
  a = {11, 12};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  std::string written_frag_uri_3;
  write_array(
      ctx.ptr().get(),
      array_name,
      3,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri_3);

  {
    // Create fragment info object
    FragmentInfo fragment_info(ctx, array_name);

    // Load fragment info
    fragment_info.load();

    bool serialized_load = false;
    SECTION("no serialization") {
      serialized_load = false;
    }
    SECTION("serialization enabled fragment info load") {
#ifdef TILEDB_SERIALIZATION
      serialized_load = true;
#endif
    }

    if (serialized_load) {
      FragmentInfo deserialized_fragment_info(ctx, array_name);
      tiledb_fragment_info_serialize(
          ctx.ptr().get(),
          array_name.c_str(),
          fragment_info.ptr().get(),
          deserialized_fragment_info.ptr().get(),
          tiledb_serialization_type_t(0));
      fragment_info = deserialized_fragment_info;
    }

    // Get Array Schemas for fragments
    ArraySchema frag1_schema = fragment_info.array_schema(0);
    ArraySchema frag2_schema = fragment_info.array_schema(1);
    ArraySchema frag3_schema = fragment_info.array_schema(2);

    // The three fragments use the same schema
    std::stringstream frag1_schema_ss;
    std::stringstream frag2_schema_ss;
    std::stringstream frag3_schema_ss;

    frag1_schema_ss << frag1_schema;
    frag2_schema_ss << frag2_schema;
    frag3_schema_ss << frag3_schema;

    std::string frag1_schema_str = frag1_schema_ss.str();
    std::string frag2_schema_str = frag2_schema_ss.str();
    std::string frag3_schema_str = frag3_schema_ss.str();
    CHECK(frag1_schema_str == frag2_schema_str);
    CHECK(frag1_schema_str == frag3_schema_str);

    // Check dump
    const auto ver = std::to_string(tiledb::sm::constants::format_version);
    std::string dump_str =
        std::string("- Fragment num: 3\n") +
        "- Unconsolidated metadata num: 3\n" + "- To vacuum num: 0\n" +
        "- Fragment #1:\n" + "  > URI: " + written_frag_uri_1 + "\n" +
        "  > Type: dense\n" + "  > Non-empty domain: [1, 6]\n" +
        "  > Size: 3202\n" + "  > Cell num: 10\n" +
        "  > Timestamp range: [1, 1]\n" + "  > Format version: " + ver + "\n" +
        "  > Has consolidated metadata: no\n" + "- Fragment #2:\n" +
        "  > URI: " + written_frag_uri_2 + "\n" + "  > Type: dense\n" +
        "  > Non-empty domain: [1, 4]\n" + "  > Size: 3151\n" +
        "  > Cell num: 5\n" + "  > Timestamp range: [2, 2]\n" +
        "  > Format version: " + ver + "\n" +
        "  > Has consolidated metadata: no\n" + "- Fragment #3:\n" +
        "  > URI: " + written_frag_uri_3 + "\n" + "  > Type: dense\n" +
        "  > Non-empty domain: [5, 6]\n" + "  > Size: 3202\n" +
        "  > Cell num: 10\n" + "  > Timestamp range: [3, 3]\n" +
        "  > Format version: " + ver + "\n" +
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
  }

  // Clean up
  remove_dir(array_name, ctx.ptr().get(), vfs.ptr().get());
}
