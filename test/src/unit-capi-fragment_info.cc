/**
 * @file unit-capi-fragment_info.cc
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
 * Tests the C API functions for manipulating fragment information.
 */

#include "test/src/helpers.h"
#include "test/src/serialization_wrappers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/global_state/unit_test_config.h"

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb::test;

const std::string array_name = "fragment_info_array_c";

TEST_CASE(
    "C API: Test fragment info, errors", "[capi][fragment_info][errors]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Error if array does not exist
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_ERR);

  // Create array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx,
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
  const char* key = "12345678901234567890123456789012";
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  rc = tiledb_config_alloc(&cfg, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  tiledb_ctx_t* ctx_array_not_encrypted;
  REQUIRE(tiledb_ctx_alloc(cfg, &ctx_array_not_encrypted) == TILEDB_OK);
  rc = tiledb_fragment_info_set_config(ctx, fragment_info, cfg);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_fragment_info_load(ctx_array_not_encrypted, fragment_info);
  CHECK(rc == TILEDB_ERR);
  tiledb_fragment_info_free(&fragment_info);

  tiledb_config_free(&cfg);
  tiledb_ctx_free(&ctx_array_not_encrypted);

  // Write a dense fragment
  QueryBuffers buffers;
  uint64_t subarray[] = {1, 6};
  std::vector<int32_t> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(ctx, array_name, 1, subarray, TILEDB_ROW_MAJOR, buffers);

  // Load again
  // We need to allocate the fragment info again with `ctx`, which does not
  // contain a key.
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
  }
#endif

  if (serialized_load) {
    tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get fragment URI
  const char* uri;
  rc = tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 1, &uri);
  CHECK(rc == TILEDB_ERR);

  // Get non-empty domain, invalid index and name
  std::vector<uint64_t> non_empty_dom(2);
  rc = tiledb_fragment_info_get_non_empty_domain_from_index(
      ctx, fragment_info, 0, 1, &non_empty_dom[0]);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_fragment_info_get_non_empty_domain_from_name(
      ctx, fragment_info, 0, "foo", &non_empty_dom[0]);
  CHECK(rc == TILEDB_ERR);

  // Var-sized non-empty domain getters should error out
  uint64_t start_size, end_size;
  char start[10];
  char end[10];
  rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
      ctx, fragment_info, 0, 0, &start_size, &end_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
      ctx, fragment_info, 0, "d", &start_size, &end_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(
      ctx, fragment_info, 0, 0, start, end);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
      ctx, fragment_info, 0, "d", start, end);
  CHECK(rc == TILEDB_ERR);

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
}

TEST_CASE(
    "C API: Test fragment info, load and getters",
    "[capi][fragment_info][load][getters]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx,
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
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // No fragments yet
  uint32_t fragment_num;
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
  CHECK(fragment_num == 0);

  // Write a dense fragment
  QueryBuffers buffers;
  uint64_t subarray[] = {1, 6};
  std::vector<int32_t> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(ctx, array_name, 1, subarray, TILEDB_ROW_MAJOR, buffers);

  // Load fragment info again
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get fragment num again
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
  CHECK(fragment_num == 1);

  uint64_t frag0_cell_num_after_first_write;
  rc = tiledb_fragment_info_get_cell_num(
      ctx, fragment_info, 0, &frag0_cell_num_after_first_write);
  CHECK(rc == TILEDB_OK);
  CHECK(frag0_cell_num_after_first_write == 10);
  uint64_t total_cell_num_after_first_write;
  rc = tiledb_fragment_info_get_total_cell_num(
      ctx, fragment_info, &total_cell_num_after_first_write);
  CHECK(rc == TILEDB_OK);
  CHECK(total_cell_num_after_first_write == 10);

  // Write another dense fragment
  subarray[0] = 1;
  subarray[1] = 7;
  a = {7, 1, 2, 3, 4, 5, 6};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(
      ctx,
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
  write_array(ctx, array_name, 3, subarray, TILEDB_ROW_MAJOR, buffers);

  // Load fragment info again
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get fragment num again
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
  CHECK(fragment_num == 3);

  // Get fragment URI
  const char* uri;
  rc = tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 1, &uri);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string(uri) == written_frag_uri);

  // Get fragment name
  const char* name;
  rc = tiledb_fragment_info_get_fragment_name(ctx, fragment_info, 1, &name);
  CHECK(rc == TILEDB_OK);

  // Get schema name
  const char* schema_name;
  rc = tiledb_fragment_info_get_array_schema_name(
      ctx, fragment_info, 0, &schema_name);
  CHECK(rc == TILEDB_OK);

  // Check schema name
  // TODO: currently this is hard-coded with the expected result
  //       ideally we would check the as-written schema timestamp
  std::string schema_name_str(schema_name != nullptr ? schema_name : "");
  CHECK(schema_name_str.size() == 62);

  // Get fragment size
  uint64_t size;
  rc = tiledb_fragment_info_get_fragment_size(ctx, fragment_info, 1, &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 3202);

  // Get dense / sparse
  int32_t dense;
  rc = tiledb_fragment_info_get_dense(ctx, fragment_info, 0, &dense);
  CHECK(rc == TILEDB_OK);
  CHECK(dense == 1);
  rc = tiledb_fragment_info_get_sparse(ctx, fragment_info, 0, &dense);
  CHECK(rc == TILEDB_OK);
  CHECK(dense == 0);
  rc = tiledb_fragment_info_get_dense(ctx, fragment_info, 1, &dense);
  CHECK(rc == TILEDB_OK);
  CHECK(dense == 1);
  rc = tiledb_fragment_info_get_sparse(ctx, fragment_info, 1, &dense);
  CHECK(rc == TILEDB_OK);
  CHECK(dense == 0);

  // Get timestamp range
  uint64_t start, end;
  rc = tiledb_fragment_info_get_timestamp_range(
      ctx, fragment_info, 1, &start, &end);
  CHECK(rc == TILEDB_OK);
  CHECK(start == 2);
  CHECK(end == 2);

  // Get non-empty domain
  std::vector<uint64_t> non_empty_dom(2);
  rc = tiledb_fragment_info_get_non_empty_domain_from_index(
      ctx, fragment_info, 0, 0, &non_empty_dom[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(non_empty_dom == std::vector<uint64_t>{1, 6});
  rc = tiledb_fragment_info_get_non_empty_domain_from_index(
      ctx, fragment_info, 1, 0, &non_empty_dom[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(non_empty_dom == std::vector<uint64_t>{1, 7});
  rc = tiledb_fragment_info_get_non_empty_domain_from_index(
      ctx, fragment_info, 2, 0, &non_empty_dom[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(non_empty_dom == std::vector<uint64_t>{2, 9});
  rc = tiledb_fragment_info_get_non_empty_domain_from_name(
      ctx, fragment_info, 1, "d", &non_empty_dom[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(non_empty_dom == std::vector<uint64_t>{1, 7});

  // Get number of cells
  uint64_t frag0_cell_num;
  rc =
      tiledb_fragment_info_get_cell_num(ctx, fragment_info, 0, &frag0_cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(frag0_cell_num == 10);
  CHECK(frag0_cell_num == frag0_cell_num_after_first_write);
  uint64_t frag1_cell_num;
  rc =
      tiledb_fragment_info_get_cell_num(ctx, fragment_info, 1, &frag1_cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(frag1_cell_num == 10);
  uint64_t frag2_cell_num;
  rc =
      tiledb_fragment_info_get_cell_num(ctx, fragment_info, 2, &frag2_cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(frag2_cell_num == 10);

  uint64_t total_cell_num_after_third_write;
  rc = tiledb_fragment_info_get_total_cell_num(
      ctx, fragment_info, &total_cell_num_after_third_write);
  CHECK(rc == TILEDB_OK);
  CHECK(
      total_cell_num_after_third_write ==
      frag0_cell_num + frag1_cell_num + frag2_cell_num);

  // Get version
  uint32_t version;
  rc = tiledb_fragment_info_get_version(ctx, fragment_info, 0, &version);
  CHECK(rc == TILEDB_OK);
  CHECK(version == tiledb::sm::constants::format_version);

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
}

TEST_CASE(
    "C API: Test fragment info, load from encrypted array",
    "[capi][fragment_info][load][encryption]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Key
  const char* key = "12345678901234567890123456789012";

  // Create array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx,
      array_name,
      TILEDB_AES_256_GCM,
      key,
      32,
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
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Array is encrypted
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_ERR);

  // Test with wrong key
  const char* wrong_key = "12345678901234567890123456789013";
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  rc = tiledb_config_alloc(&cfg, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(cfg, "sm.encryption_key", wrong_key, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  tiledb_ctx_t* ctx_wrong_key;
  REQUIRE(tiledb_ctx_alloc(cfg, &ctx_wrong_key) == TILEDB_OK);
  rc = tiledb_fragment_info_load(ctx_wrong_key, fragment_info);
  CHECK(rc == TILEDB_ERR);
  tiledb_ctx_free(&ctx_wrong_key);

  // Load fragment info
  rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  tiledb_ctx_t* ctx_correct_key;
  REQUIRE(tiledb_ctx_alloc(cfg, &ctx_correct_key) == TILEDB_OK);
  rc = tiledb_fragment_info_set_config(ctx, fragment_info, cfg);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_fragment_info_load(ctx_correct_key, fragment_info);
  CHECK(rc == TILEDB_OK);
  // tiledb_config_free(&cfg);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
    // force load rtrees so that they are included in serialized
    // fragment info (by default rtree loading is lazy).
    uint64_t mbr_num = 0;
    uint32_t fragment_num = 0;
    rc = tiledb_fragment_info_get_fragment_num(
        ctx, fragment_info, &fragment_num);
    CHECK(rc == TILEDB_OK);
    for (uint32_t fid = 0; fid < fragment_num; ++fid) {
      rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, fid, &mbr_num);
      CHECK(rc == TILEDB_OK);
    }
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_fragment_info_set_config(ctx, deserialized_fragment_info, cfg);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // No fragments yet
  uint32_t fragment_num;
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
  CHECK(fragment_num == 0);

  // Write a dense fragment
  QueryBuffers buffers;
  uint64_t subarray[] = {1, 6};
  std::vector<int32_t> a = {1, 2, 3, 4, 5, 6};
  uint64_t a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(
      ctx,
      array_name,
      TILEDB_AES_256_GCM,
      key,
      32,
      1,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers);

  // Load fragment info again
  rc = tiledb_fragment_info_load(ctx_correct_key, fragment_info);
  CHECK(rc == TILEDB_OK);

  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_fragment_info_set_config(ctx, deserialized_fragment_info, cfg);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get fragment num again
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
  CHECK(fragment_num == 1);

  // Write another dense fragment
  subarray[0] = 1;
  subarray[1] = 7;
  a = {7, 1, 2, 3, 4, 5, 6};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(
      ctx,
      array_name,
      TILEDB_AES_256_GCM,
      key,
      32,
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
      ctx,
      array_name,
      TILEDB_AES_256_GCM,
      key,
      32,
      3,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers);

  // Load fragment info again
  rc = tiledb_fragment_info_load(ctx_correct_key, fragment_info);
  CHECK(rc == TILEDB_OK);
  // tiledb_ctx_free(&ctx_correct_key);

  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_fragment_info_set_config(ctx, deserialized_fragment_info, cfg);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get fragment num again
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
  CHECK(fragment_num == 3);

  // Get fragment URI
  const char* uri;
  rc = tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 1, &uri);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string(uri) == written_frag_uri);

  // Get fragment name
  const char* name;
  rc = tiledb_fragment_info_get_fragment_name(ctx, fragment_info, 1, &name);
  CHECK(rc == TILEDB_OK);

  // Get fragment size
  uint64_t size;
  rc = tiledb_fragment_info_get_fragment_size(ctx, fragment_info, 1, &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 5585);

  // Get dense / sparse
  int32_t dense;
  rc = tiledb_fragment_info_get_dense(ctx, fragment_info, 0, &dense);
  CHECK(rc == TILEDB_OK);
  CHECK(dense == 1);
  rc = tiledb_fragment_info_get_sparse(ctx, fragment_info, 0, &dense);
  CHECK(rc == TILEDB_OK);
  CHECK(dense == 0);
  rc = tiledb_fragment_info_get_dense(ctx, fragment_info, 1, &dense);
  CHECK(rc == TILEDB_OK);
  CHECK(dense == 1);
  rc = tiledb_fragment_info_get_sparse(ctx, fragment_info, 1, &dense);
  CHECK(rc == TILEDB_OK);
  CHECK(dense == 0);

  // Get timestamp range
  uint64_t start, end;
  rc = tiledb_fragment_info_get_timestamp_range(
      ctx, fragment_info, 1, &start, &end);
  CHECK(rc == TILEDB_OK);
  CHECK(start == 2);
  CHECK(end == 2);

  // Get non-empty domain
  std::vector<uint64_t> non_empty_dom(2);
  rc = tiledb_fragment_info_get_non_empty_domain_from_index(
      ctx, fragment_info, 0, 0, &non_empty_dom[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(non_empty_dom == std::vector<uint64_t>{1, 6});
  rc = tiledb_fragment_info_get_non_empty_domain_from_index(
      ctx, fragment_info, 1, 0, &non_empty_dom[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(non_empty_dom == std::vector<uint64_t>{1, 7});
  rc = tiledb_fragment_info_get_non_empty_domain_from_index(
      ctx, fragment_info, 2, 0, &non_empty_dom[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(non_empty_dom == std::vector<uint64_t>{2, 9});
  rc = tiledb_fragment_info_get_non_empty_domain_from_name(
      ctx, fragment_info, 1, "d", &non_empty_dom[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(non_empty_dom == std::vector<uint64_t>{1, 7});

  // Get number of MBRs - should always be 0 since it's a dense array
  uint64_t mbr_num;
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 0, &mbr_num);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_num == 0);
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 1, &mbr_num);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_num == 0);
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 2, &mbr_num);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_num == 0);

  // Get MBR from index - should fail since it's a dense array
  std::vector<uint64_t> mbr(2);
  rc = tiledb_fragment_info_get_mbr_from_index(
      ctx, fragment_info, 1, 0, 0, &mbr[0]);
  CHECK(rc == TILEDB_ERR);

  // Get MBR from name - should fail since it's a dense array
  rc = tiledb_fragment_info_get_mbr_from_name(
      ctx, fragment_info, 1, 0, "d", &mbr[0]);
  CHECK(rc == TILEDB_ERR);

  // Get number of cells
  uint64_t cell_num;
  rc = tiledb_fragment_info_get_cell_num(ctx, fragment_info, 0, &cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(cell_num == 10);
  rc = tiledb_fragment_info_get_cell_num(ctx, fragment_info, 1, &cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(cell_num == 10);
  rc = tiledb_fragment_info_get_cell_num(ctx, fragment_info, 2, &cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(cell_num == 10);

  // Get version
  uint32_t version;
  rc = tiledb_fragment_info_get_version(ctx, fragment_info, 0, &version);
  CHECK(rc == TILEDB_OK);
  CHECK(version == tiledb::sm::constants::format_version);

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
}

TEST_CASE("C API: Test MBR fragment info", "[capi][fragment_info][mbr]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Key
  const char* key = "12345678901234567890123456789012";

  // Create sparse array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx,
      array_name,
      TILEDB_AES_256_GCM,
      key,
      32,
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

  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  rc = tiledb_config_alloc(&cfg, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);

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
      ctx,
      array_name,
      TILEDB_AES_256_GCM,
      key,
      32,
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
      ctx,
      array_name,
      TILEDB_AES_256_GCM,
      key,
      32,
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
      ctx,
      array_name,
      TILEDB_AES_256_GCM,
      key,
      32,
      3,
      TILEDB_UNORDERED,
      buffers,
      &written_frag_uri);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  REQUIRE(tiledb_ctx_alloc(cfg, &ctx) == TILEDB_OK);
  rc = tiledb_fragment_info_set_config(ctx, fragment_info, cfg);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);
  tiledb_config_free(&cfg);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
    // force load rtrees so that they are included in serialized
    // fragment info (by default rtree loading is lazy).
    uint64_t mbr_num = 0;
    uint32_t fragment_num = 0;
    rc = tiledb_fragment_info_get_fragment_num(
        ctx, fragment_info, &fragment_num);
    CHECK(rc == TILEDB_OK);
    for (uint32_t fid = 0; fid < fragment_num; ++fid) {
      rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, fid, &mbr_num);
      CHECK(rc == TILEDB_OK);
    }
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get fragment num
  uint32_t fragment_num;
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
  CHECK(fragment_num == 3);

  // Get number of MBRs
  uint64_t mbr_num;
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 0, &mbr_num);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_num == 1);
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 1, &mbr_num);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_num == 2);
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 2, &mbr_num);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_num == 2);
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 3, &mbr_num);
  CHECK(rc == TILEDB_ERR);

  // Get MBR from index
  std::vector<uint64_t> mbr(2);
  rc = tiledb_fragment_info_get_mbr_from_index(
      ctx, fragment_info, 0, 0, 0, &mbr[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr == std::vector<uint64_t>{1, 2});

  // Get MBR from name
  rc = tiledb_fragment_info_get_mbr_from_name(
      ctx, fragment_info, 1, 1, "d1", &mbr[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr == std::vector<uint64_t>{7, 8});

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_vfs_free(&vfs);
  tiledb_ctx_free(&ctx);
}

TEST_CASE(
    "C API: Test fragment info, load from array with string dimension",
    "[capi][fragment_info][load][string-dims][mbr]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  remove_dir(array_name, ctx, vfs);

  // Create array
  create_array(
      ctx,
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
  write_array(ctx, array_name, 1, TILEDB_UNORDERED, buffers, &written_frag_uri);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
    // force load rtrees so that they are included in serialized
    // fragment info (by default rtree loading is lazy).
    uint64_t mbr_num = 0;
    uint32_t fragment_num = 0;
    rc = tiledb_fragment_info_get_fragment_num(
        ctx, fragment_info, &fragment_num);
    CHECK(rc == TILEDB_OK);
    for (uint32_t fid = 0; fid < fragment_num; ++fid) {
      rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, fid, &mbr_num);
      CHECK(rc == TILEDB_OK);
    }
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Load non-empty domain var size - error
  uint64_t domain[2];
  rc = tiledb_fragment_info_get_non_empty_domain_from_index(
      ctx, fragment_info, 0, 0, &domain[0]);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_fragment_info_get_non_empty_domain_from_name(
      ctx, fragment_info, 0, "d", &domain[0]);
  CHECK(rc == TILEDB_ERR);

  // Load non-empty domain sizes - correct
  uint64_t start_size, end_size;
  rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
      ctx, fragment_info, 0, 0, &start_size, &end_size);
  CHECK(rc == TILEDB_OK);
  CHECK(start_size == 1);
  rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
      ctx, fragment_info, 0, "d", &start_size, &end_size);
  CHECK(rc == TILEDB_OK);
  CHECK(end_size == 3);
  char start[1];
  char end[3];
  rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(
      ctx, fragment_info, 0, 0, start, end);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string("a") == std::string(start, 1));
  CHECK(std::string("ddd") == std::string(end, 3));
  rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
      ctx, fragment_info, 0, "d", start, end);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string("a") == std::string(start, 1));
  CHECK(std::string("ddd") == std::string(end, 3));

  // Incorrect dimension index and name
  rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
      ctx, fragment_info, 0, 2, &start_size, &end_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(
      ctx, fragment_info, 0, 2, start, end);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
      ctx, fragment_info, 0, "foo", &start_size, &end_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
      ctx, fragment_info, 0, "foo", start, end);
  CHECK(rc == TILEDB_ERR);

  // Get number of MBRs
  uint64_t mbr_num;
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 0, &mbr_num);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_num == 2);

  // Get MBR size
  uint64_t mbr_start_size, mbr_end_size;
  rc = tiledb_fragment_info_get_mbr_var_size_from_index(
      ctx, fragment_info, 0, 0, 0, &mbr_start_size, &mbr_end_size);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_start_size == 1);
  CHECK(mbr_end_size == 2);
  rc = tiledb_fragment_info_get_mbr_var_size_from_name(
      ctx, fragment_info, 0, 1, "d", &mbr_start_size, &mbr_end_size);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_start_size == 1);
  CHECK(mbr_end_size == 3);

  // Get MBR
  char mbr0_start[1];
  char mbr0_end[2];
  rc = tiledb_fragment_info_get_mbr_var_from_index(
      ctx, fragment_info, 0, 0, 0, mbr0_start, mbr0_end);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string("a") == std::string(mbr0_start, 1));
  CHECK(std::string("bb") == std::string(mbr0_end, 2));

  char mbr1_start[1];
  char mbr1_end[3];
  rc = tiledb_fragment_info_get_mbr_var_from_name(
      ctx, fragment_info, 0, 1, "d", mbr1_start, mbr1_end);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string("c") == std::string(mbr1_start, 1));
  CHECK(std::string("ddd") == std::string(mbr1_end, 3));

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
}

TEST_CASE(
    "C API: Test fragment info, consolidated fragment metadata",
    "[capi][fragment_info][consolidated-metadata]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx,
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
  std::string written_frag_uri;
  write_array(
      ctx,
      array_name,
      1,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri);

  // Write another dense fragment
  subarray[0] = 1;
  subarray[1] = 7;
  a = {7, 1, 2, 3, 4, 5, 6};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(ctx, array_name, 2, subarray, TILEDB_ROW_MAJOR, buffers);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Check for consolidated metadata
  int32_t has;
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 0, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 0);
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 1, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 0);
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 2, &has);
  CHECK(rc == TILEDB_ERR);

  // Get number of unconsolidated fragment metadata
  uint32_t unconsolidated = 0;
  rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
      ctx, fragment_info, &unconsolidated);
  CHECK(rc == TILEDB_OK);
  CHECK(unconsolidated == 2);

  // Consolidate fragment metadata
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.mode", "fragment_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate - this will consolidate only the fragment metadata
  rc = tiledb_array_consolidate(ctx, array_name.c_str(), config);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Check for consolidated metadata
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 0, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 1);
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 1, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 1);

  // Get number of unconsolidated fragment metadata
  rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
      ctx, fragment_info, &unconsolidated);
  CHECK(rc == TILEDB_OK);
  CHECK(unconsolidated == 0);

  // Write another dense fragment
  subarray[0] = 2;
  subarray[1] = 9;
  a = {6, 7, 1, 2, 3, 4, 5, 6};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(ctx, array_name, 3, subarray, TILEDB_ROW_MAJOR, buffers);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Check for consolidated metadata
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 0, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 1);
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 1, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 1);
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 2, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 0);

  // Get number of unconsolidated fragment metadata
  rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
      ctx, fragment_info, &unconsolidated);
  CHECK(rc == TILEDB_OK);
  CHECK(unconsolidated == 1);

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_error_free(&error);
  tiledb_config_free(&config);
}

TEST_CASE(
    "C API: Test fragment info, to vacuum",
    "[capi][fragment_info][to-vacuum]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx,
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
      ctx,
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
  write_array(ctx, array_name, 2, subarray, TILEDB_ROW_MAJOR, buffers);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get number of fragments to vacuum
  uint32_t to_vacuum_num;
  rc = tiledb_fragment_info_get_to_vacuum_num(
      ctx, fragment_info, &to_vacuum_num);
  CHECK(rc == TILEDB_OK);
  CHECK(to_vacuum_num == 0);

  // Get to vacuum fragment URI - should error out
  const char* to_vacuum_uri;
  rc = tiledb_fragment_info_get_to_vacuum_uri(
      ctx, fragment_info, 0, &to_vacuum_uri);
  CHECK(rc == TILEDB_ERR);

  // Consolidate fragments
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.consolidation.mode", "fragments", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx, array_name.c_str(), config);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get consolidated fragment URI
  const char* uri;
  rc = tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 0, &uri);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string(uri).find_last_of("__1_2") != std::string::npos);

  // Get number of fragments to vacuum
  rc = tiledb_fragment_info_get_to_vacuum_num(
      ctx, fragment_info, &to_vacuum_num);
  CHECK(rc == TILEDB_OK);
  CHECK(to_vacuum_num == 2);

  // Get to vacuum fragment URI
  rc = tiledb_fragment_info_get_to_vacuum_uri(
      ctx, fragment_info, 0, &to_vacuum_uri);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string(to_vacuum_uri) == written_frag_uri);

  // Write another dense fragment
  subarray[0] = 1;
  subarray[1] = 3;
  a = {31, 32, 33};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(ctx, array_name, 3, subarray, TILEDB_ROW_MAJOR, buffers);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get number of fragments to vacuum
  rc = tiledb_fragment_info_get_to_vacuum_num(
      ctx, fragment_info, &to_vacuum_num);
  CHECK(rc == TILEDB_OK);
  CHECK(to_vacuum_num == 2);

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_error_free(&error);
  tiledb_config_free(&config);
}

TEST_CASE("C API: Test fragment info, dump", "[capi][fragment_info][dump]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx,
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
      ctx,
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
      ctx,
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
      ctx,
      array_name,
      3,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri_3);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get fragment array schemas
  tiledb_array_schema_t* frag1_array_schema = nullptr;
  tiledb_array_schema_t* frag2_array_schema = nullptr;
  tiledb_array_schema_t* frag3_array_schema = nullptr;

  rc = tiledb_fragment_info_get_array_schema(
      ctx, fragment_info, 0, &frag1_array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_fragment_info_get_array_schema(
      ctx, fragment_info, 1, &frag2_array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_fragment_info_get_array_schema(
      ctx, fragment_info, 2, &frag3_array_schema);
  CHECK(rc == TILEDB_OK);

  FILE* frag1_schema_file = fopen("frag1_schema.txt", "w");
  FILE* frag2_schema_file = fopen("frag2_schema.txt", "w");
  FILE* frag3_schema_file = fopen("frag3_schema.txt", "w");

  rc = tiledb_array_schema_dump(ctx, frag1_array_schema, frag1_schema_file);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_dump(ctx, frag2_array_schema, frag2_schema_file);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_dump(ctx, frag3_array_schema, frag3_schema_file);
  CHECK(rc == TILEDB_OK);

  fclose(frag1_schema_file);
  fclose(frag2_schema_file);
  fclose(frag3_schema_file);

#ifdef _WIN32
  CHECK(!system("FC frag1_schema.txt frag2_schema.txt > nul"));
  CHECK(!system("FC frag1_schema.txt frag3_schema.txt > nul"));
#else
  CHECK(!system("diff frag1_schema.txt frag2_schema.txt"));
  CHECK(!system("diff frag1_schema.txt frag3_schema.txt"));
#endif

  // Clean up fragement array schemas
  tiledb_array_schema_free(&frag1_array_schema);
  tiledb_array_schema_free(&frag2_array_schema);
  tiledb_array_schema_free(&frag3_array_schema);

  // Remove fragment schema files
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "frag1_schema.txt") == TILEDB_OK);
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "frag2_schema.txt") == TILEDB_OK);
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "frag3_schema.txt") == TILEDB_OK);

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
  tiledb_fragment_info_dump(ctx, fragment_info, fout);
  fclose(fout);
#ifdef _WIN32
  CHECK(!system("FC gold_fout.txt fout.txt > nul"));
#else
  CHECK(!system("diff gold_fout.txt fout.txt"));
#endif
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "gold_fout.txt") == TILEDB_OK);
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "fout.txt") == TILEDB_OK);

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_fragment_info_free(&fragment_info);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
}

TEST_CASE(
    "C API: Test fragment info, dump after consolidation",
    "[capi][fragment_info][dump][to_vacuum]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx,
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
      ctx,
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
      ctx,
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
      ctx,
      array_name,
      3,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri_3);

  // Consolidate
  rc = tiledb_array_consolidate(ctx, array_name.c_str(), nullptr);
  CHECK(rc == TILEDB_OK);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get consolidated fragment URI
  const char* uri;
  rc = tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 0, &uri);
  CHECK(rc == TILEDB_OK);

  // Check dump
  const auto ver = std::to_string(tiledb::sm::constants::format_version);
  std::string dump_str =
      std::string("- Fragment num: 1\n") +
      "- Unconsolidated metadata num: 1\n" + "- To vacuum num: 3\n" +
      "- To vacuum URIs:\n" + "  > " + written_frag_uri_1 + "\n  > " +
      written_frag_uri_2 + "\n  > " + written_frag_uri_3 + "\n" +
      "- Fragment #1:\n" + "  > URI: " + uri + "\n" + "  > Type: dense\n" +
      "  > Non-empty domain: [1, 10]\n" + "  > Size: 3208\n" +
      "  > Cell num: 10\n" + "  > Timestamp range: [1, 3]\n" +
      "  > Format version: " + ver + "\n" +
      "  > Has consolidated metadata: no\n";
  FILE* gold_fout = fopen("gold_fout.txt", "w");
  const char* dump = dump_str.c_str();
  fwrite(dump, sizeof(char), strlen(dump), gold_fout);
  fclose(gold_fout);
  FILE* fout = fopen("fout.txt", "w");
  tiledb_fragment_info_dump(ctx, fragment_info, fout);
  fclose(fout);
#ifdef _WIN32
  CHECK(!system("FC gold_fout.txt fout.txt > nul"));
#else
  CHECK(!system("diff gold_fout.txt fout.txt"));
#endif
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "gold_fout.txt") == TILEDB_OK);
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "fout.txt") == TILEDB_OK);

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
}

TEST_CASE(
    "C API: Test fragment info, dump with string dimension",
    "[capi][fragment_info][dump][string-dims]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  create_array(
      ctx,
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
  write_array(ctx, array_name, 1, TILEDB_UNORDERED, buffers, &written_frag_uri);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Check dump
  const auto ver = std::to_string(tiledb::sm::constants::format_version);
  std::string dump_str =
      std::string("- Fragment num: 1\n") +
      "- Unconsolidated metadata num: 1\n" + "- To vacuum num: 0\n" +
      "- Fragment #1:\n" + "  > URI: " + written_frag_uri + "\n" +
      "  > Type: sparse\n" + "  > Non-empty domain: [a, ddd]\n" +
      "  > Size: 3439\n" + "  > Cell num: 4\n" +
      "  > Timestamp range: [1, 1]\n" + "  > Format version: " + ver + "\n" +
      "  > Has consolidated metadata: no\n";
  FILE* gold_fout = fopen("gold_fout.txt", "w");
  const char* dump = dump_str.c_str();
  fwrite(dump, sizeof(char), strlen(dump), gold_fout);
  fclose(gold_fout);
  FILE* fout = fopen("fout.txt", "w");
  tiledb_fragment_info_dump(ctx, fragment_info, fout);
  fclose(fout);
#ifdef _WIN32
  CHECK(!system("FC gold_fout.txt fout.txt > nul"));
#else
  CHECK(!system("diff gold_fout.txt fout.txt"));
#endif
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "gold_fout.txt") == TILEDB_OK);
  CHECK(tiledb_vfs_remove_file(ctx, vfs, "fout.txt") == TILEDB_OK);

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
}

TEST_CASE(
    "C API: Test fragment info, naming by index",
    "[capi][fragment_info][index-naming]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  uint64_t domain[] = {1, 4, 1, 4};
  uint64_t tile_extent = 1;
  create_array(
      ctx,
      array_name,
      TILEDB_DENSE,
      {"dimOne", "dimTwo"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {&domain[0], &domain[2]},
      {&tile_extent, &tile_extent},
      {"foo", "bar"},
      {TILEDB_INT32, TILEDB_INT32},
      {1, 1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1),
       tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);

  // Write a sparse fragment
  QueryBuffers buffers;
  std::vector<int32_t> foo = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  uint64_t foo_size = foo.size() * sizeof(int32_t);
  buffers["foo"] = tiledb::test::QueryBuffer({&foo[0], foo_size, nullptr, 0});
  std::vector<int32_t> bar = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  uint64_t bar_size = foo.size() * sizeof(int32_t);
  buffers["bar"] = tiledb::test::QueryBuffer({&bar[0], bar_size, nullptr, 0});

  uint64_t subarray[] = {1, 4, 1, 4};

  std::string written_frag_uri_1;
  write_array(
      ctx,
      array_name,
      2,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri_1);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Get fragment URI
  const char* frag_uri;
  rc = tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 0, &frag_uri);
  CHECK(rc == TILEDB_OK);

  // Ensure that the fragment files are properly named after encoding
  int is_dir = 0;
  rc = tiledb_vfs_is_dir(ctx, vfs, frag_uri, &is_dir);
  REQUIRE(rc == TILEDB_OK);

  std::vector<std::string> expected_files = {"/a0.tdb", "/a1.tdb"};

  for (const std::string& expected_file : expected_files) {
    std::string file_name = frag_uri + expected_file;
    int is_file = 0;
    rc = tiledb_vfs_is_file(ctx, vfs, file_name.c_str(), &is_file);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_file > 0);
  }

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
}

TEST_CASE(
    "C API: Test fragment info, consolidated fragment metadata multiple",
    "[capi][fragment_info][consolidated-metadata][multiple]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx,
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
  std::string written_frag_uri;
  write_array(
      ctx,
      array_name,
      1,
      subarray,
      TILEDB_ROW_MAJOR,
      buffers,
      &written_frag_uri);

  // Write another dense fragment
  subarray[0] = 1;
  subarray[1] = 7;
  a = {7, 1, 2, 3, 4, 5, 6};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(ctx, array_name, 3, subarray, TILEDB_ROW_MAJOR, buffers);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  bool serialized_load = false;
  SECTION("no serialization") {
    serialized_load = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled fragment info load") {
    serialized_load = true;
  }
#endif

  tiledb_fragment_info_t* deserialized_fragment_info = nullptr;
  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Check for consolidated metadata
  int32_t has;
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 0, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 0);
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 1, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 0);
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 2, &has);
  CHECK(rc == TILEDB_ERR);

  // Get number of unconsolidated fragment metadata
  uint32_t unconsolidated = 0;
  rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
      ctx, fragment_info, &unconsolidated);
  CHECK(rc == TILEDB_OK);
  CHECK(unconsolidated == 2);

  // Consolidate fragment metadata
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.mode", "fragment_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate - this will consolidate only the fragment metadata
  rc = tiledb_array_consolidate(ctx, array_name.c_str(), config);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  // Check for consolidated metadata
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 0, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 1);
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 1, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 1);

  // Get number of unconsolidated fragment metadata
  rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
      ctx, fragment_info, &unconsolidated);
  CHECK(rc == TILEDB_OK);
  CHECK(unconsolidated == 0);

  // Write another dense fragment in between the existing 2
  subarray[0] = 2;
  subarray[1] = 9;
  a = {6, 7, 1, 2, 3, 4, 5, 6};
  a_size = a.size() * sizeof(int32_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  write_array(ctx, array_name, 2, subarray, TILEDB_ROW_MAJOR, buffers);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Check for consolidated metadata
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 0, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 1);
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 1, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 0);
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 2, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 1);

  // Get number of unconsolidated fragment metadata
  rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
      ctx, fragment_info, &unconsolidated);
  CHECK(rc == TILEDB_OK);
  CHECK(unconsolidated == 1);

  // Consolidate - this will consolidate only the fragment metadata
  rc = tiledb_array_consolidate(ctx, array_name.c_str(), config);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  if (serialized_load) {
    rc = tiledb_fragment_info_alloc(
        ctx, array_name.c_str(), &deserialized_fragment_info);
    CHECK(rc == TILEDB_OK);
    tiledb_fragment_info_serialize(
        ctx,
        array_name.c_str(),
        fragment_info,
        deserialized_fragment_info,
        tiledb_serialization_type_t(0));
    tiledb_fragment_info_free(&fragment_info);
    fragment_info = deserialized_fragment_info;
  }

  // Check again
  rc = tiledb_fragment_info_has_consolidated_metadata(
      ctx, fragment_info, 1, &has);
  CHECK(rc == TILEDB_OK);
  CHECK(has == 1);

  // Get number of unconsolidated fragment metadata
  rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
      ctx, fragment_info, &unconsolidated);
  CHECK(rc == TILEDB_OK);
  CHECK(unconsolidated == 0);

  // Clean up
  tiledb_fragment_info_free(&fragment_info);
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_error_free(&error);
  tiledb_config_free(&config);
}