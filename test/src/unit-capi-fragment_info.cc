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
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/global_state/unit_test_config.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::test;

const std::string array_name = "fragment_info_array";

TEST_CASE(
    "C API: Test fragment info, errors", "[capi][fragment_info][errors]") {
  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  tiledb_vfs_t* vfs = nullptr;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);
  remove_dir(array_name, ctx, vfs);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Error if array does not exist
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_ERR);

  // Create array
  remove_dir(array_name, ctx, vfs);
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
  rc = tiledb_fragment_info_load(ctx_array_not_encrypted, fragment_info);
  CHECK(rc == TILEDB_ERR);
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
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

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
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_fragment_info_free(&fragment_info);
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
  remove_dir(array_name, ctx, vfs);
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

  // Get fragment num again
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
  CHECK(fragment_num == 1);

  // Write a sparse fragment
  a = {11, 12, 13, 14};
  a_size = a.size() * sizeof(int32_t);
  std::vector<uint64_t> d = {1, 3, 5, 7};
  uint64_t d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(ctx, array_name, 2, TILEDB_UNORDERED, buffers, &written_frag_uri);

  // Write another sparse fragment
  a = {21, 22, 23};
  a_size = a.size() * sizeof(int32_t);
  d = {2, 4, 9};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  write_array(ctx, array_name, 3, TILEDB_UNORDERED, buffers);

  // Load fragment info again
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  // Get fragment num again
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
  CHECK(fragment_num == 3);

  // Get fragment URI
  const char* uri;
  rc = tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 1, &uri);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string(uri) == written_frag_uri);

  // Get fragment size
  uint64_t size;
  rc = tiledb_fragment_info_get_fragment_size(ctx, fragment_info, 1, &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 1708);

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
  CHECK(dense == 0);
  rc = tiledb_fragment_info_get_sparse(ctx, fragment_info, 1, &dense);
  CHECK(rc == TILEDB_OK);
  CHECK(dense == 1);

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
  uint64_t cell_num;
  rc = tiledb_fragment_info_get_cell_num(ctx, fragment_info, 0, &cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(cell_num == 10);
  rc = tiledb_fragment_info_get_cell_num(ctx, fragment_info, 1, &cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(cell_num == 4);
  rc = tiledb_fragment_info_get_cell_num(ctx, fragment_info, 2, &cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(cell_num == 3);

  // Get version
  uint32_t version;
  rc = tiledb_fragment_info_get_version(ctx, fragment_info, 0, &version);
  CHECK(rc == TILEDB_OK);
  CHECK(version == tiledb::sm::constants::format_version);

  // Clean up
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_fragment_info_free(&fragment_info);
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
  remove_dir(array_name, ctx, vfs);
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
  rc = tiledb_fragment_info_load(ctx_correct_key, fragment_info);
  CHECK(rc == TILEDB_OK);
  tiledb_config_free(&cfg);

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

  // Get fragment num again
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
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
      ctx,
      array_name,
      TILEDB_AES_256_GCM,
      key,
      32,
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
  write_array(
      ctx,
      array_name,
      TILEDB_AES_256_GCM,
      key,
      32,
      3,
      TILEDB_UNORDERED,
      buffers);

  // Load fragment info again
  rc = tiledb_fragment_info_load(ctx_correct_key, fragment_info);
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_free(&ctx_correct_key);

  // Get fragment num again
  rc = tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
  CHECK(rc == TILEDB_OK);
  CHECK(fragment_num == 3);

  // Get fragment URI
  const char* uri;
  rc = tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 1, &uri);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string(uri) == written_frag_uri);

  // Get fragment size
  uint64_t size;
  rc = tiledb_fragment_info_get_fragment_size(ctx, fragment_info, 1, &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 3061);

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
  CHECK(dense == 0);
  rc = tiledb_fragment_info_get_sparse(ctx, fragment_info, 1, &dense);
  CHECK(rc == TILEDB_OK);
  CHECK(dense == 1);

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

  // Get number of MBRs
  uint64_t mbr_num;
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 0, &mbr_num);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_num == 0);
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 1, &mbr_num);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_num == 2);
  rc = tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 2, &mbr_num);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr_num == 2);

  // Get MBR from index
  std::vector<uint64_t> mbr(2);
  rc = tiledb_fragment_info_get_mbr_from_index(
      ctx, fragment_info, 1, 0, 0, &mbr[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr == std::vector<uint64_t>{1, 3});
  rc = tiledb_fragment_info_get_mbr_from_index(
      ctx, fragment_info, 1, 1, 0, &mbr[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr == std::vector<uint64_t>{5, 7});
  rc = tiledb_fragment_info_get_mbr_from_index(
      ctx, fragment_info, 2, 0, 0, &mbr[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr == std::vector<uint64_t>{2, 4});
  rc = tiledb_fragment_info_get_mbr_from_index(
      ctx, fragment_info, 2, 1, 0, &mbr[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr == std::vector<uint64_t>{9, 9});

  // Get MBR from name
  rc = tiledb_fragment_info_get_mbr_from_name(
      ctx, fragment_info, 1, 0, "d", &mbr[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr == std::vector<uint64_t>{1, 3});
  rc = tiledb_fragment_info_get_mbr_from_name(
      ctx, fragment_info, 1, 1, "d", &mbr[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr == std::vector<uint64_t>{5, 7});
  rc = tiledb_fragment_info_get_mbr_from_name(
      ctx, fragment_info, 2, 0, "d", &mbr[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr == std::vector<uint64_t>{2, 4});
  rc = tiledb_fragment_info_get_mbr_from_name(
      ctx, fragment_info, 2, 1, "d", &mbr[0]);
  CHECK(rc == TILEDB_OK);
  CHECK(mbr == std::vector<uint64_t>{9, 9});

  // Get number of cells
  uint64_t cell_num;
  rc = tiledb_fragment_info_get_cell_num(ctx, fragment_info, 0, &cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(cell_num == 10);
  rc = tiledb_fragment_info_get_cell_num(ctx, fragment_info, 1, &cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(cell_num == 4);
  rc = tiledb_fragment_info_get_cell_num(ctx, fragment_info, 2, &cell_num);
  CHECK(rc == TILEDB_OK);
  CHECK(cell_num == 3);

  // Get version
  uint32_t version;
  rc = tiledb_fragment_info_get_version(ctx, fragment_info, 0, &version);
  CHECK(rc == TILEDB_OK);
  CHECK(version == tiledb::sm::constants::format_version);

  // Clean up
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_fragment_info_free(&fragment_info);
}

TEST_CASE(
    "C API: Test fragment info, load from array with string dimension",
    "[capi][fragment_info][load][string-dim]") {
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
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_fragment_info_free(&fragment_info);
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
  remove_dir(array_name, ctx, vfs);
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

  // Write a sparse fragment
  QueryBuffers buffers;
  std::vector<int32_t> a = {11, 12, 13, 14};
  uint64_t a_size = a.size() * sizeof(int32_t);
  std::vector<uint64_t> d = {1, 3, 5, 7};
  uint64_t d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(ctx, array_name, 1, TILEDB_UNORDERED, buffers, &written_frag_uri);

  // Write another sparse fragment
  a = {21, 22, 23};
  a_size = a.size() * sizeof(int32_t);
  d = {2, 4, 9};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  write_array(ctx, array_name, 2, TILEDB_UNORDERED, buffers);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

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

  // Write another sparse fragment
  a = {31, 32, 33};
  a_size = a.size() * sizeof(int32_t);
  d = {1, 3, 5};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  write_array(ctx, array_name, 3, TILEDB_UNORDERED, buffers);

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
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_error_free(&error);
  tiledb_config_free(&config);
  tiledb_fragment_info_free(&fragment_info);
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
  remove_dir(array_name, ctx, vfs);
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

  // Write a sparse fragment
  QueryBuffers buffers;
  std::vector<int32_t> a = {11, 12, 13, 14};
  uint64_t a_size = a.size() * sizeof(int32_t);
  std::vector<uint64_t> d = {1, 3, 5, 7};
  uint64_t d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri;
  write_array(ctx, array_name, 1, TILEDB_UNORDERED, buffers, &written_frag_uri);

  // Write another sparse fragment
  a = {21, 22, 23};
  a_size = a.size() * sizeof(int32_t);
  d = {2, 4, 9};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  write_array(ctx, array_name, 2, TILEDB_UNORDERED, buffers);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

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

  // Write another sparse fragment
  a = {31, 32, 33};
  a_size = a.size() * sizeof(int32_t);
  d = {1, 3, 5};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  write_array(ctx, array_name, 3, TILEDB_UNORDERED, buffers);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  // Get number of fragments to vacuum
  rc = tiledb_fragment_info_get_to_vacuum_num(
      ctx, fragment_info, &to_vacuum_num);
  CHECK(rc == TILEDB_OK);
  CHECK(to_vacuum_num == 2);

  // Clean up
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_error_free(&error);
  tiledb_config_free(&config);
  tiledb_fragment_info_free(&fragment_info);
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
  remove_dir(array_name, ctx, vfs);
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

  // Write a sparse fragment
  a = {11, 12, 13, 14};
  a_size = a.size() * sizeof(int32_t);
  std::vector<uint64_t> d = {1, 3, 5, 7};
  uint64_t d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri_2;
  write_array(
      ctx, array_name, 2, TILEDB_UNORDERED, buffers, &written_frag_uri_2);

  // Write another sparse fragment
  a = {21, 22, 23};
  a_size = a.size() * sizeof(int32_t);
  d = {2, 4, 9};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri_3;
  write_array(
      ctx, array_name, 3, TILEDB_UNORDERED, buffers, &written_frag_uri_3);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

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
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_fragment_info_free(&fragment_info);
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
  remove_dir(array_name, ctx, vfs);
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

  // Write a sparse fragment
  a = {11, 12, 13, 14};
  a_size = a.size() * sizeof(int32_t);
  std::vector<uint64_t> d = {1, 3, 5, 7};
  uint64_t d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri_2;
  write_array(
      ctx, array_name, 2, TILEDB_UNORDERED, buffers, &written_frag_uri_2);

  // Write another sparse fragment
  a = {21, 22, 23};
  a_size = a.size() * sizeof(int32_t);
  d = {2, 4, 9};
  d_size = d.size() * sizeof(uint64_t);
  buffers["a"] = tiledb::test::QueryBuffer({&a[0], a_size, nullptr, 0});
  buffers["d"] = tiledb::test::QueryBuffer({&d[0], d_size, nullptr, 0});
  std::string written_frag_uri_3;
  write_array(
      ctx, array_name, 3, TILEDB_UNORDERED, buffers, &written_frag_uri_3);

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

  // Get consolidated fragment URI
  const char* uri;
  rc = tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 0, &uri);
  CHECK(rc == TILEDB_OK);

  // Check dump
  std::string dump_str =
      std::string("- Fragment num: 1\n") +
      "- Unconsolidated metadata num: 1\n" + "- To vacuum num: 3\n" +
      "- To vacuum URIs:\n" + "  > " + written_frag_uri_1 + "\n  > " +
      written_frag_uri_2 + "\n  > " + written_frag_uri_3 + "\n" +
      "- Fragment #1:\n" + "  > URI: " + uri + "\n" + "  > Type: dense\n" +
      "  > Non-empty domain: [1, 10]\n" + "  > Size: 1584\n" +
      "  > Cell num: 10\n" + "  > Timestamp range: [1, 3]\n" +
      "  > Format version: 9\n" + "  > Has consolidated metadata: no\n";
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
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_fragment_info_free(&fragment_info);
}

TEST_CASE(
    "C API: Test fragment info, dump with string dimension",
    "[capi][fragment_info][dump][string-dim]") {
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

  // Check dump
  std::string dump_str =
      std::string("- Fragment num: 1\n") +
      "- Unconsolidated metadata num: 1\n" + "- To vacuum num: 0\n" +
      "- Fragment #1:\n" + "  > URI: " + written_frag_uri + "\n" +
      "  > Type: sparse\n" + "  > Non-empty domain: [a, ddd]\n" +
      "  > Size: 1833\n" + "  > Cell num: 4\n" +
      "  > Timestamp range: [1, 1]\n" + "  > Format version: 9\n" +
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
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_fragment_info_free(&fragment_info);
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
  remove_dir(array_name, ctx, vfs);
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

  std::vector<uint64_t> dimOne = {1, 1, 2, 3};
  uint64_t dimOne_size = dimOne.size() * sizeof(uint64_t);
  std::vector<uint64_t> dimTwo = {2, 3, 1, 1};
  uint64_t dimTwo_size = dimTwo.size() * sizeof(uint64_t);
  buffers["dimOne"] =
      tiledb::test::QueryBuffer({&dimOne[0], dimOne_size, nullptr, 0});
  buffers["dimTwo"] =
      tiledb::test::QueryBuffer({&dimTwo[0], dimTwo_size, nullptr, 0});

  std::string written_frag_uri_1;
  write_array(
      ctx, array_name, 2, TILEDB_UNORDERED, buffers, &written_frag_uri_1);

  // Create fragment info object
  tiledb_fragment_info_t* fragment_info = nullptr;
  rc = tiledb_fragment_info_alloc(ctx, array_name.c_str(), &fragment_info);
  CHECK(rc == TILEDB_OK);

  // Load fragment info
  rc = tiledb_fragment_info_load(ctx, fragment_info);
  CHECK(rc == TILEDB_OK);

  // Get fragment URI
  const char* frag_uri;
  rc = tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 0, &frag_uri);
  CHECK(rc == TILEDB_OK);

  // Ensure that the fragment files are properly named after encoding
  int is_dir = 0;
  rc = tiledb_vfs_is_dir(ctx, vfs, frag_uri, &is_dir);
  REQUIRE(rc == TILEDB_OK);

  std::vector<std::string> expected_files = {
      "/a0.tdb", "/a1.tdb", "/d0.tdb", "/d1.tdb"};

  for (const std::string& expected_file : expected_files) {
    std::string file_name = frag_uri + expected_file;
    int is_file = 0;
    rc = tiledb_vfs_is_file(ctx, vfs, file_name.c_str(), &is_file);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_file > 0);
  }

  // Clean up
  remove_dir(array_name, ctx, vfs);
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_fragment_info_free(&fragment_info);
}