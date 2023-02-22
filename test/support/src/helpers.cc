/**
 * @file   helpers.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file defines some test suite helper functions.
 */

#include <filesystem>
#if _WIN32
#include <process.h>
#define getpid _getpid
#else
#include <unistd.h>
#endif

#include <test/support/tdb_catch.h>
#include "helpers.h"
#include "serialization_wrappers.h"
#include "tiledb/api/c_api/buffer/buffer_api_internal.h"
#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/tile_overlap.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/query.h"

std::mutex catch2_macro_mutex;

namespace tiledb {
namespace test {

const std::string& get_temp_path() {
  // Ensure the path has a trailing delimiter.
  static std::string temp_path =
      (std::filesystem::temp_directory_path() /
       ("tiledb_test_" + std::to_string(getpid())) / "")
          .string();

  return temp_path;
}

// Command line arguments.
std::string g_vfs;

void check_tiledb_error_with(
    tiledb_ctx_t* ctx, int rc, const std::string& expected_msg) {
  CHECK(rc == TILEDB_ERR);
  if (rc != TILEDB_ERR) {
    return;
  }
  tiledb_error_t* err{nullptr};
  tiledb_ctx_get_last_error(ctx, &err);
  if (err == nullptr) {
    INFO("No message returned. Expected message: " + expected_msg);
    CHECK(false);
  } else {
    const char* raw_msg;
    tiledb_error_message(err, &raw_msg);
    if (raw_msg == nullptr) {
      INFO("No message returned. Expected message: " + expected_msg);
      CHECK(false);
    } else {
      std::string err_msg{raw_msg};
      CHECK(err_msg == expected_msg);
    }
  }
  tiledb_error_free(&err);
}

void check_tiledb_ok(tiledb_ctx_t* ctx, int rc) {
  if (rc != TILEDB_OK) {
    tiledb_error_t* err{nullptr};
    tiledb_ctx_get_last_error(ctx, &err);
    if (err != nullptr) {
      const char* msg;
      tiledb_error_message(err, &msg);
      if (msg != nullptr) {
        UNSCOPED_INFO(msg);
      }
    }
    tiledb_error_free(&err);
  }
  CHECK(rc == TILEDB_OK);
}

void require_tiledb_error_with(
    tiledb_ctx_t* ctx, int rc, const std::string& expected_msg) {
  REQUIRE(rc == TILEDB_ERR);
  tiledb_error_t* err{nullptr};
  tiledb_ctx_get_last_error(ctx, &err);
  if (err == nullptr) {
    INFO("No message returned. Expected message: " + expected_msg);
    REQUIRE(false);
  }
  const char* raw_msg;
  tiledb_error_message(err, &raw_msg);
  if (raw_msg == nullptr) {
    INFO("No message returned. Expected message: " + expected_msg);
    tiledb_error_free(&err);
    REQUIRE(false);
  }
  std::string err_msg{raw_msg};
  REQUIRE(err_msg == expected_msg);
  tiledb_error_free(&err);
}

void require_tiledb_ok(tiledb_ctx_t* ctx, int rc) {
  if (rc != TILEDB_OK) {
    tiledb_error_t* err{nullptr};
    tiledb_ctx_get_last_error(ctx, &err);
    if (err != nullptr) {
      const char* msg;
      tiledb_error_message(err, &msg);
      if (msg != nullptr) {
        UNSCOPED_INFO(msg);
      }
    }
    tiledb_error_free(&err);
  }
  REQUIRE(rc == TILEDB_OK);
}

int store_g_vfs(std::string&& vfs, std::vector<std::string> vfs_fs) {
  if (!vfs.empty()) {
    if (std::find(vfs_fs.begin(), vfs_fs.end(), vfs) == vfs_fs.end()) {
      std::cerr << "Unknown --vfs argument: \"" << vfs << "\"";
      return 1;
    }

    tiledb::test::g_vfs = std::move(vfs);
  }

  return 0;
}

bool use_refactored_dense_reader() {
  const char* value = nullptr;
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  auto rc = tiledb_config_alloc(&cfg, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);

  rc = tiledb_config_get(cfg, "sm.query.dense.reader", &value, &err);
  CHECK(rc == TILEDB_OK);
  CHECK(err == nullptr);

  bool use_refactored_readers = strcmp(value, "refactored") == 0;

  tiledb_config_free(&cfg);

  return use_refactored_readers;
}

bool use_refactored_sparse_global_order_reader() {
  const char* value = nullptr;
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  auto rc = tiledb_config_alloc(&cfg, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);

  rc = tiledb_config_get(
      cfg, "sm.query.sparse_global_order.reader", &value, &err);
  CHECK(rc == TILEDB_OK);
  CHECK(err == nullptr);

  bool use_refactored_readers = strcmp(value, "refactored") == 0;

  tiledb_config_free(&cfg);

  return use_refactored_readers;
}

bool use_refactored_sparse_unordered_with_dups_reader() {
  const char* value = nullptr;
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  auto rc = tiledb_config_alloc(&cfg, &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);

  rc = tiledb_config_get(
      cfg, "sm.query.sparse_unordered_with_dups.reader", &value, &err);
  CHECK(rc == TILEDB_OK);
  CHECK(err == nullptr);

  bool use_refactored_readers = strcmp(value, "refactored") == 0;

  tiledb_config_free(&cfg);

  return use_refactored_readers;
}

shared_ptr<Logger> g_helper_logger(void) {
  static shared_ptr<Logger> g_helper_logger = make_shared<Logger>(HERE(), "");
  return g_helper_logger;
}

template <class T>
void check_partitions(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<T>>& partitions,
    bool last_unsplittable) {
  bool unsplittable = false;

  // Special case for empty partitions
  if (partitions.empty()) {
    CHECK(partitioner.next(&unsplittable).ok());
    if (last_unsplittable) {
      CHECK(unsplittable);
    } else {
      CHECK(!unsplittable);
      CHECK(partitioner.done());
    }
    return;
  }

  // Non-empty partitions
  for (const auto& p : partitions) {
    CHECK(!partitioner.done());
    CHECK(!unsplittable);
    CHECK(partitioner.next(&unsplittable).ok());
    auto partition = partitioner.current();
    check_subarray<T>(partition, p);
  }

  // Check last unsplittable
  if (last_unsplittable) {
    CHECK(unsplittable);
  } else {
    CHECK(!unsplittable);
    CHECK(partitioner.next(&unsplittable).ok());
    CHECK(!unsplittable);
    CHECK(partitioner.done());
  }
}

template <class T>
void check_subarray(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<T>& ranges) {
  // Check empty subarray
  auto subarray_range_num = subarray.range_num();
  if (ranges.empty()) {
    CHECK(subarray_range_num == 0);
    return;
  }
  uint64_t range_num = 1;
  for (const auto& dim_ranges : ranges)
    range_num *= dim_ranges.size() / 2;
  CHECK(subarray_range_num == range_num);

  // Check dim num
  auto dim_num = subarray.dim_num();
  CHECK(dim_num == ranges.size());

  // Check ranges
  uint64_t dim_range_num = 0;
  const type::Range* range;
  for (unsigned i = 0; i < dim_num; ++i) {
    CHECK(subarray.get_range_num(i, &dim_range_num).ok());
    CHECK(dim_range_num == ranges[i].size() / 2);
    for (uint64_t j = 0; j < dim_range_num; ++j) {
      CHECK(subarray.get_range(i, j, &range).ok());
      auto r = (const T*)range->data();

      CHECK(r[0] == ranges[i][2 * j]);
      CHECK(r[1] == ranges[i][2 * j + 1]);
    }
  }
}

template <class T>
void check_subarray(
    tiledb::Subarray& subarray, const SubarrayRanges<T>& ranges) {
  auto as = subarray.array().schema();
  auto ndims = as.domain().ndim();
  uint64_t nranges = 1;
  for (auto ui = 0u; ui < ndims; ++ui) {
    auto range_num_dim = subarray.range_num(ui);
    nranges *= range_num_dim;
  }
  // Check empty subarray
  auto subarray_range_num = nranges;
  if (ranges.empty()) {
    CHECK(subarray_range_num == 0);
    return;
  }
  uint64_t range_num = 1;
  for (const auto& dim_ranges : ranges)
    range_num *= dim_ranges.size() / 2;
  CHECK(subarray_range_num == range_num);

  // Check dim num
  auto dim_num = ndims;
  CHECK(dim_num == ranges.size());

  // Check ranges
  uint64_t dim_range_num = 0;
  for (unsigned di = 0; di < dim_num; ++di) {
    dim_range_num = subarray.range_num(di);
    CHECK(dim_range_num == ranges[di].size() / 2);
    for (uint64_t ri = 0; ri < dim_range_num; ++ri) {
      auto r = subarray.range<T>(di, ri);

      CHECK(r[0] == ranges[di][2 * ri]);
      CHECK(r[1] == ranges[di][2 * ri + 1]);
    }
  }
}

template <class T>
void check_subarray_equiv(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2) {
  CHECK(subarray1.range_num() == subarray2.range_num());
  // Check dim num
  auto dim_num1 = subarray1.dim_num();
  auto dim_num2 = subarray2.dim_num();
  CHECK(dim_num1 == dim_num2);

  tiledb::sm::ByteVec sa1bytes, sa2bytes;
  //.to_byte_vect() only valid when range_num() == 1, but should be same for
  // both and resulting bytes, empty or otherwise, should be the same as well.
  CHECK(
      subarray1.to_byte_vec(&sa1bytes).ok() ==
      subarray2.to_byte_vec(&sa2bytes).ok());
  CHECK(sa1bytes == sa2bytes);

  const std::vector<std::vector<uint8_t>> sa1tilecoords =
      subarray1.tile_coords();
  const std::vector<std::vector<uint8_t>> sa2tilecoords =
      subarray2.tile_coords();
  CHECK(sa1tilecoords == sa2tilecoords);

  // Check ranges
  uint64_t dim_range_num1 = 0;
  const type::Range* range1;
  uint64_t dim_range_num2 = 0;
  const type::Range* range2;
  if (dim_num1 == dim_num2) {
    for (unsigned i = 0; i < dim_num1; ++i) {
      CHECK(subarray1.get_range_num(i, &dim_range_num1).ok());
      CHECK(subarray2.get_range_num(i, &dim_range_num2).ok());
      CHECK(dim_range_num1 == dim_range_num2);
      if (dim_range_num1 == dim_range_num2) {
        for (uint64_t j = 0; j < dim_range_num1; ++j) {
          CHECK(subarray1.get_range(i, j, &range1).ok());
          CHECK(subarray2.get_range(i, j, &range2).ok());
          auto r1 = (const T*)range1->data();
          auto r2 = (const T*)range2->data();
          CHECK(r1[0] == r2[0]);
          CHECK(r1[1] == r2[1]);
        }
      }
    }
  }
}

template <class T>
bool subarray_equiv(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2) {
  bool equiv_state = 1;  // assume true

  equiv_state &= (subarray1.range_num() == subarray2.range_num());
  // Check dim num
  auto dim_num1 = subarray1.dim_num();
  auto dim_num2 = subarray2.dim_num();
  equiv_state &= (dim_num1 == dim_num2);

  tiledb::sm::ByteVec sa1bytes, sa2bytes;
  //.to_byte_vect() only valid when range_num() == 1, but should be same for
  // both and resulting bytes, empty or otherwise, should be the same as well.
  equiv_state &=
      (subarray1.to_byte_vec(&sa1bytes).ok() ==
       subarray2.to_byte_vec(&sa2bytes).ok());
  equiv_state &= (sa1bytes == sa2bytes);

  const std::vector<std::vector<uint8_t>> sa1tilecoords =
      subarray1.tile_coords();
  const std::vector<std::vector<uint8_t>> sa2tilecoords =
      subarray2.tile_coords();
  CHECK(sa1tilecoords == sa2tilecoords);

  // Check ranges
  uint64_t dim_range_num1 = 0;
  const type::Range* range1;
  uint64_t dim_range_num2 = 0;
  const type::Range* range2;
  if (dim_num1 == dim_num2) {
    for (unsigned i = 0; i < dim_num1; ++i) {
      equiv_state &= (subarray1.get_range_num(i, &dim_range_num1).ok());
      equiv_state &= (subarray2.get_range_num(i, &dim_range_num2).ok());
      equiv_state &= (dim_range_num1 == dim_range_num2);
      if (dim_range_num1 == dim_range_num2) {
        for (uint64_t j = 0; j < dim_range_num1; ++j) {
          CHECK(subarray1.get_range(i, j, &range1).ok());
          CHECK(subarray2.get_range(i, j, &range2).ok());
          auto r1 = (const T*)range1->data();
          auto r2 = (const T*)range2->data();
          equiv_state &= (r1[0] == r2[0]);
          equiv_state &= (r1[1] == r2[1]);
        }
      }
    }
  }

  return equiv_state;
}

void close_array(tiledb_ctx_t* ctx, tiledb_array_t* array) {
  int rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);
}

void create_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_array_type_t array_type,
    const std::vector<std::string>& dim_names,
    const std::vector<tiledb_datatype_t>& dim_types,
    const std::vector<void*>& dim_domains,
    const std::vector<void*>& tile_extents,
    const std::vector<std::string>& attr_names,
    const std::vector<tiledb_datatype_t>& attr_types,
    const std::vector<uint32_t>& cell_val_num,
    const std::vector<std::pair<tiledb_filter_type_t, int>>& compressors,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order,
    uint64_t capacity,
    bool allows_dups,
    bool serialize_array_schema) {
  // For easy reference
  auto dim_num = dim_names.size();
  auto attr_num = attr_names.size();

  // Sanity checks
  assert(dim_types.size() == dim_num);
  assert(dim_domains.size() == dim_num);
  assert(tile_extents.size() == dim_num);
  assert(attr_types.size() == attr_num);
  assert(cell_val_num.size() == attr_num);
  assert(compressors.size() == attr_num);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx, array_type, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx, array_schema, tile_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx, array_schema, capacity);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_allows_dups(ctx, array_schema, (int)allows_dups);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions and domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  REQUIRE(rc == TILEDB_OK);
  for (size_t i = 0; i < dim_num; ++i) {
    tiledb_dimension_t* d;
    rc = tiledb_dimension_alloc(
        ctx,
        dim_names[i].c_str(),
        dim_types[i],
        dim_domains[i],
        tile_extents[i],
        &d);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_domain_add_dimension(ctx, domain, d);
    REQUIRE(rc == TILEDB_OK);
    tiledb_dimension_free(&d);
  }

  // Set domain to schema
  rc = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_free(&domain);

  // Create attributes
  for (size_t i = 0; i < attr_num; ++i) {
    tiledb_attribute_t* a;
    rc = tiledb_attribute_alloc(ctx, attr_names[i].c_str(), attr_types[i], &a);
    REQUIRE(rc == TILEDB_OK);
    rc = set_attribute_compression_filter(
        ctx, a, compressors[i].first, compressors[i].second);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_attribute_set_cell_val_num(ctx, a, cell_val_num[i]);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_schema_add_attribute(ctx, array_schema, a);
    REQUIRE(rc == TILEDB_OK);
    tiledb_attribute_free(&a);
  }

  // Check array schema
  rc = tiledb_array_schema_check(ctx, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create_serialization_wrapper(
      ctx, array_name, array_schema, serialize_array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_array_schema_free(&array_schema);
}

void create_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t enc_type,
    const char* key,
    uint32_t key_len,
    tiledb_array_type_t array_type,
    const std::vector<std::string>& dim_names,
    const std::vector<tiledb_datatype_t>& dim_types,
    const std::vector<void*>& dim_domains,
    const std::vector<void*>& tile_extents,
    const std::vector<std::string>& attr_names,
    const std::vector<tiledb_datatype_t>& attr_types,
    const std::vector<uint32_t>& cell_val_num,
    const std::vector<std::pair<tiledb_filter_type_t, int>>& compressors,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order,
    uint64_t capacity) {
  // For easy reference
  auto dim_num = dim_names.size();
  auto attr_num = attr_names.size();

  // Sanity checks
  assert(dim_types.size() == dim_num);
  assert(dim_domains.size() == dim_num);
  assert(tile_extents.size() == dim_num);
  assert(attr_types.size() == attr_num);
  assert(cell_val_num.size() == attr_num);
  assert(compressors.size() == attr_num);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx, array_type, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx, array_schema, tile_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx, array_schema, capacity);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions and domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  REQUIRE(rc == TILEDB_OK);
  for (size_t i = 0; i < dim_num; ++i) {
    tiledb_dimension_t* d;
    rc = tiledb_dimension_alloc(
        ctx,
        dim_names[i].c_str(),
        dim_types[i],
        dim_domains[i],
        tile_extents[i],
        &d);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_domain_add_dimension(ctx, domain, d);
    REQUIRE(rc == TILEDB_OK);
    tiledb_dimension_free(&d);
  }

  // Set domain to schema
  rc = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_free(&domain);

  // Create attributes
  for (size_t i = 0; i < attr_num; ++i) {
    tiledb_attribute_t* a;
    rc = tiledb_attribute_alloc(ctx, attr_names[i].c_str(), attr_types[i], &a);
    REQUIRE(rc == TILEDB_OK);
    rc = set_attribute_compression_filter(
        ctx, a, compressors[i].first, compressors[i].second);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_attribute_set_cell_val_num(ctx, a, cell_val_num[i]);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_schema_add_attribute(ctx, array_schema, a);
    REQUIRE(rc == TILEDB_OK);
    tiledb_attribute_free(&a);
  }

  // Check array schema
  rc = tiledb_array_schema_check(ctx, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  std::string encryption_type_string =
      encryption_type_str((tiledb::sm::EncryptionType)enc_type);
  rc = tiledb_config_set(
      config, "sm.encryption_type", encryption_type_string.c_str(), &error);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.encryption_key", key, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
      key_len);
  tiledb_ctx_t* ctx_array;
  REQUIRE(tiledb_ctx_alloc(config, &ctx_array) == TILEDB_OK);
  rc = tiledb_array_create(ctx_array, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_array_schema_free(&array_schema);
  tiledb_ctx_free(&ctx_array);
}

tiledb_array_schema_t* create_array_schema(
    tiledb_ctx_t* ctx,
    tiledb_array_type_t array_type,
    const std::vector<std::string>& dim_names,
    const std::vector<tiledb_datatype_t>& dim_types,
    const std::vector<void*>& dim_domains,
    const std::vector<void*>& tile_extents,
    const std::vector<std::string>& attr_names,
    const std::vector<tiledb_datatype_t>& attr_types,
    const std::vector<uint32_t>& cell_val_num,
    const std::vector<std::pair<tiledb_filter_type_t, int>>& compressors,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order,
    uint64_t capacity,
    bool allows_dups) {
  // For easy reference
  auto dim_num = dim_names.size();
  auto attr_num = attr_names.size();

  // Sanity checks
  assert(dim_types.size() == dim_num);
  assert(dim_domains.size() == dim_num);
  assert(tile_extents.size() == dim_num);
  assert(attr_types.size() == attr_num);
  assert(cell_val_num.size() == attr_num);
  assert(compressors.size() == attr_num);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx, array_type, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx, array_schema, tile_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx, array_schema, capacity);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_allows_dups(ctx, array_schema, (int)allows_dups);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions and domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  REQUIRE(rc == TILEDB_OK);
  for (size_t i = 0; i < dim_num; ++i) {
    tiledb_dimension_t* d;
    rc = tiledb_dimension_alloc(
        ctx,
        dim_names[i].c_str(),
        dim_types[i],
        dim_domains[i],
        tile_extents[i],
        &d);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_domain_add_dimension(ctx, domain, d);
    REQUIRE(rc == TILEDB_OK);
    tiledb_dimension_free(&d);
  }

  // Set domain to schema
  rc = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_free(&domain);

  // Create attributes
  for (size_t i = 0; i < attr_num; ++i) {
    tiledb_attribute_t* a;
    rc = tiledb_attribute_alloc(ctx, attr_names[i].c_str(), attr_types[i], &a);
    REQUIRE(rc == TILEDB_OK);
    rc = set_attribute_compression_filter(
        ctx, a, compressors[i].first, compressors[i].second);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_attribute_set_cell_val_num(ctx, a, cell_val_num[i]);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_schema_add_attribute(ctx, array_schema, a);
    REQUIRE(rc == TILEDB_OK);
    tiledb_attribute_free(&a);
  }

  // Check array schema
  rc = tiledb_array_schema_check(ctx, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  return array_schema;
}

void create_s3_bucket(
    const std::string& bucket_name,
    bool s3_supported,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs) {
  if (s3_supported) {
    // Create bucket if it does not exist
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx, vfs, bucket_name.c_str(), &is_bucket);
    REQUIRE(rc == TILEDB_OK);
    if (!is_bucket) {
      rc = tiledb_vfs_create_bucket(ctx, vfs, bucket_name.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }
}

void create_azure_container(
    const std::string& container_name,
    bool azure_supported,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs) {
  if (azure_supported) {
    // Create container if it does not exist
    int is_container = 0;
    int rc =
        tiledb_vfs_is_bucket(ctx, vfs, container_name.c_str(), &is_container);
    REQUIRE(rc == TILEDB_OK);
    if (!is_container) {
      rc = tiledb_vfs_create_bucket(ctx, vfs, container_name.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }
}

void create_ctx_and_vfs(
    bool s3_supported,
    bool azure_supported,
    tiledb_ctx_t** ctx,
    tiledb_vfs_t** vfs) {
  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  if (s3_supported) {
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.endpoint_override", "localhost:9999", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.s3.scheme", "https", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.use_virtual_addressing", "false", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.s3.verify_ssl", "false", &error) ==
        TILEDB_OK);
    REQUIRE(error == nullptr);
#endif
  }
  if (azure_supported) {
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.storage_account_name",
            "devstoreaccount1",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.storage_account_key",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
            "K1SZFPTOtr/KBHBeksoGMGw==",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.blob_endpoint",
            "http://127.0.0.1:10000/devstoreaccount1",
            &error) == TILEDB_OK);
  }
  REQUIRE(tiledb_ctx_alloc(config, ctx) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Create VFS
  *vfs = nullptr;
  REQUIRE(tiledb_vfs_alloc(*ctx, config, vfs) == TILEDB_OK);
  tiledb_config_free(&config);
}

void create_dir(const std::string& path, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  remove_dir(path, ctx, vfs);
  REQUIRE(tiledb_vfs_create_dir(ctx, vfs, path.c_str()) == TILEDB_OK);
}

template <class T>
void create_subarray(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<T>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges) {
  tiledb::sm::Subarray ret(
      array.get(), layout, &g_helper_stats, g_helper_logger(), coalesce_ranges);

  auto dim_num = (unsigned)ranges.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_range_num = ranges[d].size() / 2;
    for (size_t j = 0; j < dim_range_num; ++j) {
      type::Range range(&ranges[d][2 * j], 2 * sizeof(T));
      CHECK(ret.add_range(d, std::move(range), true).ok());
    }
  }

  *subarray = ret;
}

template <class T>
void create_subarray(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<T>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges) {
  (void)layout;
  int32_t rc;
  tiledb_array_t tdb_array;
  tdb_array.array_ = array;
  rc = tiledb_subarray_alloc(ctx, &tdb_array, subarray);
  REQUIRE(rc == TILEDB_OK);
  if (rc == TILEDB_OK) {
    rc = tiledb_subarray_set_coalesce_ranges(ctx, *subarray, coalesce_ranges);
    REQUIRE(rc == TILEDB_OK);

    auto dim_num = (unsigned)ranges.size();
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim_range_num = ranges[d].size() / 2;
      for (size_t j = 0; j < dim_range_num; ++j) {
        rc = tiledb_subarray_add_range(
            ctx, *subarray, d, &ranges[d][2 * j], &ranges[d][2 * j + 1], 0);
        REQUIRE(rc == TILEDB_OK);
      }
    }
  }
}

template <class T>
void create_subarray(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<T>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** returned_subarray,
    bool coalesce_ranges) {
  tiledb::Subarray* psubarray =
      new tiledb::Subarray(*ctx, *array, coalesce_ranges);
  tiledb::Subarray& subarray = *psubarray;

  (void)layout;
  subarray.set_coalesce_ranges(coalesce_ranges);

  auto dim_num = (unsigned)ranges.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_range_num = ranges[d].size() / 2;
    for (size_t j = 0; j < dim_range_num; ++j) {
      subarray.add_range<T>(d, ranges[d][2 * j], ranges[d][2 * j + 1], 0);
    }
  }
  *returned_subarray = psubarray;
}

void get_supported_fs(
    bool* s3_supported,
    bool* hdfs_supported,
    bool* azure_supported,
    bool* gcs_supported) {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  int is_supported = 0;
  int rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_S3, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  *s3_supported = (bool)is_supported;
  rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_HDFS, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  *hdfs_supported = (bool)is_supported;
  rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_AZURE, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  *azure_supported = (bool)is_supported;
  rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_GCS, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  *gcs_supported = (bool)is_supported;

  // Override VFS support if the user used the '--vfs' command line argument.
  if (!g_vfs.empty()) {
    REQUIRE(
        (g_vfs == "native" || g_vfs == "s3" || g_vfs == "hdfs" ||
         g_vfs == "azure" || g_vfs == "gcs"));

    if (g_vfs == "native") {
      *s3_supported = false;
      *hdfs_supported = false;
      *azure_supported = false;
      *gcs_supported = false;
    }

    if (g_vfs == "s3") {
      *s3_supported = true;
      *hdfs_supported = false;
      *azure_supported = false;
      *gcs_supported = false;
    }

    if (g_vfs == "hdfs") {
      *s3_supported = false;
      *hdfs_supported = true;
      *azure_supported = false;
      *gcs_supported = false;
    }

    if (g_vfs == "azure") {
      *s3_supported = false;
      *hdfs_supported = false;
      *azure_supported = true;
      *gcs_supported = false;
    }

    if (g_vfs == "gcs") {
      *s3_supported = false;
      *hdfs_supported = false;
      *azure_supported = false;
      *gcs_supported = true;
    }
  }

  tiledb_ctx_free(&ctx);
}

void open_array(
    tiledb_ctx_t* ctx, tiledb_array_t* array, tiledb_query_type_t query_type) {
  int rc = tiledb_array_open(ctx, array, query_type);
  CHECK(rc == TILEDB_OK);
}

std::string random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

void remove_dir(const std::string& path, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx, vfs, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx, vfs, path.c_str()) == TILEDB_OK);
}

void remove_s3_bucket(
    const std::string& bucket_name,
    bool s3_supported,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs) {
  if (s3_supported) {
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx, vfs, bucket_name.c_str(), &is_bucket);
    CHECK(rc == TILEDB_OK);
    if (is_bucket) {
      rc = tiledb_vfs_remove_bucket(ctx, vfs, bucket_name.c_str());
      CHECK(rc == TILEDB_OK);
    }
  }
}

int set_attribute_compression_filter(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_filter_type_t compressor,
    int32_t level) {
  if (compressor == TILEDB_FILTER_NONE)
    return TILEDB_OK;

  tiledb_filter_t* filter;
  int rc = tiledb_filter_alloc(ctx, compressor, &filter);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_list_t* list;
  rc = tiledb_filter_list_alloc(ctx, &list);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx, list, filter);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_filter_list(ctx, attr, list);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&list);

  return TILEDB_OK;
}

void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_layout_t layout,
    const QueryBuffers& buffers) {
  write_array(
      ctx, array_name, TILEDB_TIMESTAMP_NOW_MS, nullptr, layout, buffers);
}

void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    uint64_t timestamp,
    tiledb_layout_t layout,
    const QueryBuffers& buffers) {
  write_array(ctx, array_name, timestamp, nullptr, layout, buffers);
}

void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t encryption_type,
    const char* key,
    uint64_t key_len,
    uint64_t timestamp,
    tiledb_layout_t layout,
    const QueryBuffers& buffers) {
  write_array(
      ctx,
      array_name,
      encryption_type,
      key,
      key_len,
      timestamp,
      nullptr,
      layout,
      buffers);
}

void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    const void* subarray,
    tiledb_layout_t layout,
    const QueryBuffers& buffers) {
  write_array(
      ctx, array_name, TILEDB_TIMESTAMP_NOW_MS, subarray, layout, buffers);
}

void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    uint64_t timestamp,
    const void* subarray,
    tiledb_layout_t layout,
    const QueryBuffers& buffers) {
  std::string uri;
  write_array(ctx, array_name, timestamp, subarray, layout, buffers, &uri);
  (void)uri;
}

void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t encryption_type,
    const char* key,
    uint64_t key_len,
    uint64_t timestamp,
    const void* subarray,
    tiledb_layout_t layout,
    const QueryBuffers& buffers) {
  std::string uri;
  write_array(
      ctx,
      array_name,
      encryption_type,
      key,
      key_len,
      timestamp,
      subarray,
      layout,
      buffers,
      &uri);
  (void)uri;
}

void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    uint64_t timestamp,
    tiledb_layout_t layout,
    const QueryBuffers& buffers,
    std::string* uri) {
  write_array(ctx, array_name, timestamp, nullptr, layout, buffers, uri);
}

void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t encryption_type,
    const char* key,
    uint64_t key_len,
    uint64_t timestamp,
    tiledb_layout_t layout,
    const QueryBuffers& buffers,
    std::string* uri) {
  write_array(
      ctx,
      array_name,
      encryption_type,
      key,
      key_len,
      timestamp,
      nullptr,
      layout,
      buffers,
      uri);
}

void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    uint64_t timestamp,
    const void* subarray,
    tiledb_layout_t layout,
    const QueryBuffers& buffers,
    std::string* uri) {
  write_array(
      ctx,
      array_name,
      TILEDB_NO_ENCRYPTION,
      nullptr,
      0,
      timestamp,
      subarray,
      layout,
      buffers,
      uri);
}

void write_array(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_encryption_type_t encryption_type,
    const char* key,
    uint64_t key_len,
    uint64_t timestamp,
    const void* sub,
    tiledb_layout_t layout,
    const QueryBuffers& buffers,
    std::string* uri) {
  // Set array configuration
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);

  rc = tiledb_array_set_open_timestamp_end(ctx, array, timestamp);
  REQUIRE(rc == TILEDB_OK);

  // Open array
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
  }
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  if (sub != nullptr) {
    rc = tiledb_subarray_alloc(ctx, array, &subarray);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_subarray_set_subarray(ctx, subarray, sub);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_subarray_t(ctx, query, subarray);
    REQUIRE(rc == TILEDB_OK);
  }
  rc = tiledb_query_set_layout(ctx, query, layout);
  CHECK(rc == TILEDB_OK);

  // Set buffers
  for (const auto& b : buffers) {
    if (b.second.var_ == nullptr) {  // Fixed-sized
      rc = tiledb_query_set_data_buffer(
          ctx,
          query,
          b.first.c_str(),
          b.second.fixed_,
          (uint64_t*)&(b.second.fixed_size_));
      CHECK(rc == TILEDB_OK);
    } else {  // Var-sized
      rc = tiledb_query_set_data_buffer(
          ctx,
          query,
          b.first.c_str(),
          b.second.var_,
          (uint64_t*)&(b.second.var_size_));
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_offsets_buffer(
          ctx,
          query,
          b.first.c_str(),
          (uint64_t*)b.second.fixed_,
          (uint64_t*)&(b.second.fixed_size_));
      CHECK(rc == TILEDB_OK);
    }
  }

  // Submit query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Get fragment uri
  const char* temp_uri;
  rc = tiledb_query_get_fragment_uri(ctx, query, 0, &temp_uri);
  REQUIRE(rc == TILEDB_OK);
  *uri = std::string(temp_uri);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  if (sub != nullptr) {
    tiledb_subarray_free(&subarray);
  }
  tiledb_config_free(&cfg);
}

template <class T>
void read_array(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<T>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers) {
  // Create query
  tiledb_query_t* query;
  int rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, layout);
  CHECK(rc == TILEDB_OK);

  auto dim_num = (unsigned)ranges.size();
  for (unsigned i = 0; i < dim_num; ++i) {
    auto dim_range_num = ranges[i].size() / 2;
    for (size_t j = 0; j < dim_range_num; ++j) {
      rc = tiledb_query_add_range(
          ctx, query, i, &ranges[i][2 * j], &ranges[i][2 * j + 1], nullptr);
      CHECK(rc == TILEDB_OK);
    }
  }

  // Set buffers
  for (const auto& b : buffers) {
    if (b.second.var_ == nullptr) {  // Fixed-sized
      rc = tiledb_query_set_data_buffer(
          ctx,
          query,
          b.first.c_str(),
          b.second.fixed_,
          (uint64_t*)&(b.second.fixed_size_));
      CHECK(rc == TILEDB_OK);
    } else {  // Var-sized
      rc = tiledb_query_set_data_buffer(
          ctx,
          query,
          b.first.c_str(),
          b.second.var_,
          (uint64_t*)&(b.second.var_size_));
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_offsets_buffer(
          ctx,
          query,
          b.first.c_str(),
          (uint64_t*)b.second.fixed_,
          (uint64_t*)&(b.second.fixed_size_));
      CHECK(rc == TILEDB_OK);
    }
  }

  // Submit query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Check status
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  // Clean up
  tiledb_query_free(&query);
}

int32_t num_commits(const std::string& array_name) {
  Context ctx;
  VFS vfs(ctx);

  // Get the number of write files in the commit directory
  CommitsDirectory commits_dir(vfs, array_name);
  return commits_dir.file_count(sm::constants::write_file_suffix);
}

int32_t num_fragments(const std::string& array_name) {
  Context ctx;
  VFS vfs(ctx);

  // Get all URIs in the array directory
  auto uris = vfs.ls(
      array_name + "/" + tiledb::sm::constants::array_fragments_dir_name);
  return static_cast<uint32_t>(uris.size());
}

std::string random_string(const uint64_t l) {
  static const char char_set[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string s;
  s.reserve(l);

  for (uint64_t i = 0; i < l; ++i) {
    s += char_set[rand() % (sizeof(char_set) - 1)];
  }

  return s;
}

std::string get_fragment_dir(std::string array_dir) {
  return array_dir + "/" + tiledb::sm::constants::array_fragments_dir_name;
}

std::string get_commit_dir(std::string array_dir) {
  return array_dir + "/" + tiledb::sm::constants::array_commits_dir_name;
}

FileCount::FileCount(
    VFS vfs, std::string path, std::vector<std::string> expected_extensions) {
  auto files = vfs.ls(path);
  dir_size_ = files.size();

  for (auto file : files) {
    auto file_ext = file.substr(file.find_last_of("."));
    if (std::find(
            expected_extensions.begin(), expected_extensions.end(), file_ext) ==
        expected_extensions.end()) {
      throw std::logic_error(
          "[FileCount::FileCount] Expected extension " + file_ext +
          " is not in the given path.");
    }
    file_count_[file_ext]++;
  }
}

const std::map<std::string, uint64_t>& FileCount::file_count() const {
  return file_count_;
}

uint64_t FileCount::file_count(std::string extension) {
  return file_count_[extension];
}

uint64_t FileCount::dir_size() {
  return dir_size_;
}

template <class T>
void check_counts(span<T> vals, std::vector<uint64_t> expected) {
  auto expected_size = static_cast<T>(expected.size());
  std::vector<uint64_t> counts(expected.size());
  for (uint64_t i = 0; i < vals.size(); i++) {
    CHECK(vals[i] >= 0);
    CHECK(vals[i] < expected_size);

    if (vals[i] >= 0 && vals[i] < expected_size) {
      counts[vals[i]]++;
    }
  }

  for (uint64_t i = 0; i < expected.size(); i++) {
    CHECK(counts[i] == expected[i]);
  }
}
void serialize_query(
    const Context& ctx,
    Query& query,
    std::vector<uint8_t>* serialized,
    bool clientside) {
  ctx.handle_error(serialize_query(
      ctx.ptr().get(), query.ptr().get(), serialized, clientside));
}

int serialize_query(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    std::vector<uint8_t>* serialized,
    bool clientside) {
  // Serialize
  tiledb_buffer_list_t* buff_list;

  if (tiledb_serialize_query(
          ctx, query, TILEDB_CAPNP, clientside ? 1 : 0, &buff_list) !=
      TILEDB_OK) {
    return TILEDB_ERR;
  }

  // Flatten
  tiledb_buffer_t* c_buff;
  if (tiledb_buffer_list_flatten(ctx, buff_list, &c_buff) != TILEDB_OK) {
    return TILEDB_ERR;
  }

  // Wrap in a safe pointer
  auto deleter = [](tiledb_buffer_t* b) { tiledb_buffer_free(&b); };
  std::unique_ptr<tiledb_buffer_t, decltype(deleter)> buff_ptr(c_buff, deleter);

  // Copy into user vector
  void* data;
  uint64_t num_bytes;
  if (tiledb_buffer_get_data(ctx, c_buff, &data, &num_bytes) != TILEDB_OK) {
    return TILEDB_ERR;
  }
  serialized->clear();
  serialized->insert(
      serialized->end(),
      static_cast<const uint8_t*>(data),
      static_cast<const uint8_t*>(data) + num_bytes);

  // Free buffer list
  tiledb_buffer_list_free(&buff_list);

  return TILEDB_OK;
}

void deserialize_query(
    const Context& ctx,
    std::vector<uint8_t>& serialized,
    Query* query,
    bool clientside) {
  ctx.handle_error(deserialize_query(
      ctx.ptr().get(), serialized, query->ptr().get(), clientside));
}

int deserialize_query(
    tiledb_ctx_t* ctx,
    std::vector<uint8_t>& serialized,
    tiledb_query_t* query,
    bool clientside) {
  tiledb_buffer_t* c_buff;
  if (tiledb_buffer_alloc(ctx, &c_buff) != TILEDB_OK) {
    return TILEDB_ERR;
  }

  // Wrap in a safe pointer
  auto deleter = [](tiledb_buffer_t* b) { tiledb_buffer_free(&b); };
  std::unique_ptr<tiledb_buffer_t, decltype(deleter)> buff_ptr(c_buff, deleter);

  if (tiledb_buffer_set_data(
          ctx,
          c_buff,
          reinterpret_cast<void*>(&serialized[0]),
          static_cast<uint64_t>(serialized.size())) != TILEDB_OK) {
    return TILEDB_ERR;
  }

  // Deserialize
  return tiledb_deserialize_query(
      ctx, c_buff, TILEDB_CAPNP, clientside ? 1 : 0, query);
}

int deserialize_array_and_query(
    tiledb_ctx_t* ctx,
    std::vector<uint8_t>& serialized,
    tiledb_query_t** query,
    const char* array_uri,
    bool clientside) {
  tiledb_buffer_t* c_buff;
  if (tiledb_buffer_alloc(ctx, &c_buff) != TILEDB_OK) {
    return TILEDB_ERR;
  }

  // Wrap in a safe pointer
  auto deleter = [](tiledb_buffer_t* b) { tiledb_buffer_free(&b); };
  std::unique_ptr<tiledb_buffer_t, decltype(deleter)> buff_ptr(c_buff, deleter);

  if (tiledb_buffer_set_data(
          ctx,
          c_buff,
          reinterpret_cast<void*>(&serialized[0]),
          static_cast<uint64_t>(serialized.size())) != TILEDB_OK) {
    return TILEDB_ERR;
  }

  // Deserialize
  tiledb_array_t* array;
  return tiledb_deserialize_query_and_array(
      ctx, c_buff, TILEDB_CAPNP, clientside ? 1 : 0, array_uri, query, &array);
}

int array_open_wrapper(
    tiledb_ctx_t* client_ctx,
    tiledb_query_type_t query_type,
    bool serialize,
    tiledb_array_t** open_array) {
  // Open array
#ifndef TILEDB_SERIALIZATION
  return tiledb_array_open(client_ctx, *open_array, query_type);
#endif

  if (!serialize) {
    return tiledb_array_open(client_ctx, *open_array, query_type);
  }

  // Get the config to use it for creating fresh contexts
  tiledb_config_t* config;
  int rc = tiledb_ctx_get_config(client_ctx, &config);
  REQUIRE(rc == TILEDB_OK);

  // this helper only applies to refactored array open, so set it in the config
  // Note: we actually set use_refactored_array_open_and_query_submit instead of
  // simple use_refactored_array_open here, because we want array_open_wrapper
  // to be usable in query_v3 tests that require that flag to be set right
  // from the beginning for full Array objects to be retrieved on array open.
  // There are dedicated tests in unit-capi-array.cc that are testing array v2
  // feature with just setting use_refactored_array_open config variable.
  tiledb_error_t* error = nullptr;
  rc = tiledb_config_set(
      config,
      "rest.use_refactored_array_open_and_query_submit",
      "true",
      &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_array_set_config(client_ctx, *open_array, config) == TILEDB_OK);

  auto serialization_type =
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP;

  // 1. Client: Serialize array_open request to Server. First set the query_type
  // that will be serialized
  auto type = static_cast<sm::QueryType>(query_type);
  (*open_array)->array_->set_query_type(type);

  tiledb_buffer_t* buffer;
  rc = tiledb_serialize_array_open(
      client_ctx, *open_array, serialization_type, 1, &buffer);
  REQUIRE(rc == TILEDB_OK);

  // 2. Server : Receive and deserialize array_open_request
  // Allocate new context for server
  tiledb_ctx_t* server_ctx;
  tiledb_ctx_alloc(config, &server_ctx);
  tiledb_array_t* deserialized_array_server = nullptr;
  rc = tiledb_deserialize_array_open(
      server_ctx, buffer, serialization_type, 0, &deserialized_array_server);
  REQUIRE(rc == TILEDB_OK);
  tiledb_buffer_free(&buffer);

  // Check that the original and de-serialized array have the same query type
  REQUIRE(deserialized_array_server->array_->get_query_type() == type);

  // 3. Server: Open the array the request was received for in the requested
  // mode
  // This is needed in test, as the deserialized array has a dummy array_uri of
  // "deserialized_array" set instead of the original one. The Cloud side
  // already knows the array URI so it's not a problem in the real life scenario
  deserialized_array_server->array_->set_array_uri(
      (*open_array)->array_->array_uri());

  // if we need to add start and end timestamps uncomment this code
  // rc = tiledb_array_set_open_timestamp_start(server_ctx,
  // deserialized_array_server, array->array_->timestamp_start());
  // REQUIRE_SAFE(rc == TILEDB_OK);
  // rc = tiledb_array_set_open_timestamp_end(server_ctx,
  // deserialized_array_server, array->array_->timestamp_end()); REQUIRE_SAFE(rc
  // == TILEDB_OK);
  rc = tiledb_array_open(server_ctx, deserialized_array_server, query_type);
  REQUIRE(rc == TILEDB_OK);

  // 4. Server -> Client: Send opened Array (serialize)
  tiledb_buffer_t* buff;
  rc = tiledb_serialize_array(
      server_ctx,
      deserialized_array_server,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      1,
      &buff);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_close(server_ctx, deserialized_array_server);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&deserialized_array_server);
  tiledb_ctx_free(&server_ctx);
  CHECK(server_ctx == nullptr);

  // 5. Client: Receive and deserialize Array (into deserialized_array_client),
  // in the same way that rest_client does.
  auto st = tiledb::sm::serialization::array_deserialize(
      (*open_array)->array_.get(),
      tiledb::sm::SerializationType::CAPNP,
      buff->buffer(),
      client_ctx->storage_manager());
  REQUIRE(st.ok());

  // 6. Server: Close array and clean up
  tiledb_buffer_free(&buff);

  return rc;
}

int submit_query_wrapper(
    const Context& client_ctx,
    const std::string& array_uri,
    Query* query,
    ServerQueryBuffers& buffers,
    bool serialize_query,
    bool refactored_query_v2,
    bool finalize) {
  auto query_c = query->ptr().get();
  return submit_query_wrapper(
      client_ctx.ptr().get(),
      array_uri,
      &query_c,
      buffers,
      serialize_query,
      refactored_query_v2,
      finalize);
}

int submit_query_wrapper(
    tiledb_ctx_t* client_ctx,
    const std::string& array_uri,
    tiledb_query_t** query,
    ServerQueryBuffers& buffers,
    bool serialize,
    bool refactored_query_v2,
    bool finalize) {
  int rc = 0;
#ifndef TILEDB_SERIALIZATION
  rc = tiledb_query_submit(client_ctx, *query);
  if (rc != TILEDB_OK) {
    return rc;
  }

  if (finalize) {
    // Finalize query
    rc = tiledb_query_finalize(client_ctx, *query);
  }

  return rc;
#endif

  if (!serialize) {
    rc = tiledb_query_submit(client_ctx, *query);
    if (rc != TILEDB_OK) {
      return rc;
    }

    if (finalize) {
      // Finalize query
      rc = tiledb_query_finalize(client_ctx, *query);
    }

    return rc;
  }

  // Get the config to use it for creating fresh contexts
  tiledb_config_t* config;
  rc = tiledb_query_get_config(client_ctx, *query, &config);
  CHECK(rc == TILEDB_OK);

  // Get the array of that query
  tiledb_array_t* array;
  REQUIRE(tiledb_query_get_array(client_ctx, *query, &array) == TILEDB_OK);

  // if refactored query v2 set it in the config
  if (refactored_query_v2) {
    tiledb_error_t* error = nullptr;
    rc = tiledb_config_set(
        config,
        "rest.use_refactored_array_open_and_query_submit",
        "true",
        &error);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(error == nullptr);

    REQUIRE(tiledb_array_close(client_ctx, array) == TILEDB_OK);
    REQUIRE(tiledb_array_set_config(client_ctx, array, config) == TILEDB_OK);
    tiledb_query_type_t query_type;
    REQUIRE_SAFE(
        tiledb_query_get_type(client_ctx, *query, &query_type) == TILEDB_OK);
    REQUIRE(tiledb_array_open(client_ctx, array, query_type) == TILEDB_OK);
  }

  // Get the query type
  tiledb_query_type_t query_type;
  REQUIRE_SAFE(
      tiledb_query_get_type(client_ctx, *query, &query_type) == TILEDB_OK);

  // 1. Client -> Server : Serialize and send query request
  tiledb_query_t* server_deser_query;
  std::vector<uint8_t> serialized;

  rc = serialize_query(client_ctx, *query, &serialized, 1);
  REQUIRE(rc == TILEDB_OK);

  // 2. Server: Deserialize query request
  // Allocate new context for server
  tiledb_ctx_t* server_ctx;
  tiledb_ctx_alloc(config, &server_ctx);
  tiledb_array_t* array_server = nullptr;
  if (refactored_query_v2) {
    rc = deserialize_array_and_query(
        server_ctx, serialized, &server_deser_query, array_uri.c_str(), 0);
    REQUIRE_SAFE(rc == TILEDB_OK);
  } else {
    // Open array
    int rc = tiledb_array_alloc(server_ctx, array_uri.c_str(), &array_server);
    REQUIRE_SAFE(rc == TILEDB_OK);

    rc = tiledb_array_set_open_timestamp_start(
        server_ctx, array_server, array->array_->timestamp_start());
    REQUIRE_SAFE(rc == TILEDB_OK);
    rc = tiledb_array_set_open_timestamp_end(
        server_ctx, array_server, array->array_->timestamp_end_opened_at());
    REQUIRE_SAFE(rc == TILEDB_OK);

    rc = tiledb_array_open(server_ctx, array_server, query_type);
    REQUIRE_SAFE(rc == TILEDB_OK);

    rc = tiledb_query_alloc(
        server_ctx, array_server, query_type, &server_deser_query);
    REQUIRE_SAFE(rc == TILEDB_OK);
    rc = tiledb::test::deserialize_query(
        server_ctx, serialized, server_deser_query, 0);
    REQUIRE_SAFE(rc == TILEDB_OK);
  }

  // This is a feature of the server, not a bug, quoting from query_from_capnp:
  // "On reads, just set null pointers with accurate size so that the
  // server can introspect and allocate properly sized buffers separately."
  // Empty buffers will naturally break query_submit so to go on in test we
  // need to allocate here as if we were the server.
  if (query_type == TILEDB_READ) {
    allocate_query_buffers_server_side(server_ctx, server_deser_query, buffers);
  }

  // 3. Server: Submit query WITHOUT re-opening the array, using under the hood
  // the array found in the deserialized query
  rc = tiledb_query_submit(server_ctx, server_deser_query);
  if (rc != TILEDB_OK) {
    return rc;
  }

  if (finalize) {
    tiledb_query_status_t status;
    rc = tiledb_query_get_status(server_ctx, server_deser_query, &status);
    CHECK(status == TILEDB_COMPLETED);

    rc = tiledb_query_finalize(server_ctx, server_deser_query);
    if (rc != TILEDB_OK) {
      return rc;
    }
  }

  // Serialize the new query and "send it over the network" (server-side)
  // 4. Server -> Client : Send query response
  std::vector<uint8_t> serialized2;
  rc = serialize_query(server_ctx, server_deser_query, &serialized2, 0);
  REQUIRE(rc == TILEDB_OK);

  if (!refactored_query_v2) {
    // Close array and clean up
    rc = tiledb_array_close(server_ctx, array_server);
    CHECK(rc == TILEDB_OK);
    tiledb_array_free(&array_server);
  }

  tiledb_ctx_free(&server_ctx);
  CHECK(server_ctx == nullptr);

  // 5. Client: Deserialize query
  rc = deserialize_query(client_ctx, serialized2, *query, 1);
  REQUIRE(rc == TILEDB_OK);

  // Clean up.
  tiledb_query_free(&server_deser_query);

  return rc;
}

int finalize_query_wrapper(
    const Context& client_ctx,
    const std::string& array_uri,
    Query* query,
    bool serialize_query) {
  auto query_c = query->ptr().get();
  return finalize_query_wrapper(
      client_ctx.ptr().get(), array_uri, &query_c, serialize_query);
}

int finalize_query_wrapper(
    tiledb_ctx_t* client_ctx,
    const std::string& array_uri,
    tiledb_query_t** query,
    bool serialize) {
#ifndef TILEDB_SERIALIZATION
  return tiledb_query_finalize(client_ctx, *query);
#endif

  if (!serialize) {
    return tiledb_query_finalize(client_ctx, *query);
  }

  // Get the config to use it for creating fresh contexts
  tiledb_config_t* config;
  auto rc = tiledb_query_get_config(client_ctx, *query, &config);
  CHECK(rc == TILEDB_OK);

  // Get the query type
  tiledb_query_type_t query_type;
  REQUIRE_SAFE(
      tiledb_query_get_type(client_ctx, *query, &query_type) == TILEDB_OK);

  // 1. Client -> Server : Send query request
  std::vector<uint8_t> serialized;

  rc = serialize_query(client_ctx, *query, &serialized, 1);
  REQUIRE(rc == TILEDB_OK);

  // 2. Server: Deserialize query request
  // Allocate new context for server
  tiledb_ctx_t* server_ctx;
  tiledb_ctx_alloc(config, &server_ctx);
  tiledb_query_t* server_deser_query;
  rc = deserialize_array_and_query(
      server_ctx, serialized, &server_deser_query, array_uri.c_str(), 0);

  // 3. Server: Finalize query
  rc = tiledb_query_finalize(server_ctx, server_deser_query);
  if (rc != TILEDB_OK) {
    return rc;
  }

  /*
   * TODO: This is disabled because it fails today as the previous serialization
   * step before finalize, sets the query buffers to NULL for READ queries, so
   * the following serialization step will hit an assert because of empty
   * buffers, as query_finalize is a NOOP for READ queries.
   *
   * // Serialize the new query and "send it over the network" (server-side)
   * 3. Server -> Client : Send query response
   * std::vector<uint8_t> serialized2;
   * rc = tiledb_query_v2_serialize(
   *     ctx, array_uri.c_str(), serialized, false, server_deser_query, query);
   * REQUIRE_SAFE(rc == TILEDB_OK);
   */

  // Clean up.
  tiledb_query_free(&server_deser_query);

  return rc;
}

void allocate_query_buffers_server_side(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    ServerQueryBuffers& query_buffers) {
  int rc = 0;
  const auto buffer_names = query->query_->buffer_names();
  for (uint64_t i = 0; i < buffer_names.size(); i++) {
    const auto& name = buffer_names[i];
    const auto& buff = query->query_->buffer(name);
    const auto& schema = query->query_->array_schema();
    auto var_size = schema.var_size(name);
    auto nullable = schema.is_nullable(name);
    if (var_size && buff.buffer_var_ == nullptr) {
      // Variable-sized buffer
      query_buffers.attr_or_dim_data.emplace_back(*buff.buffer_var_size_);
      query_buffers.attr_or_dim_off.emplace_back(
          (*buff.buffer_size_) / sizeof(sm::constants::cell_var_offset_size));
      rc = tiledb_query_set_data_buffer(
          ctx,
          query,
          name.c_str(),
          query_buffers.attr_or_dim_data.back().data(),
          buff.buffer_var_size_);
      REQUIRE_SAFE(rc == TILEDB_OK);
      rc = tiledb_query_set_offsets_buffer(
          ctx,
          query,
          name.c_str(),
          query_buffers.attr_or_dim_off.back().data(),
          buff.buffer_size_);
      REQUIRE_SAFE(rc == TILEDB_OK);
    }

    if (name == TILEDB_COORDS || (!var_size && buff.buffer_ == nullptr)) {
      // Fixed-length buffer or Coords
      query_buffers.attr_or_dim.emplace_back(*buff.buffer_size_);
      rc = tiledb_query_set_data_buffer(
          ctx,
          query,
          name.c_str(),
          query_buffers.attr_or_dim.back().data(),
          buff.buffer_size_);
      REQUIRE_SAFE(rc == TILEDB_OK);
    }

    if (nullable) {
      // nullable
      query_buffers.attr_or_dim_nullable.emplace_back(
          *buff.validity_vector_.buffer_size());
      rc = tiledb_query_set_validity_buffer(
          ctx,
          query,
          name.c_str(),
          query_buffers.attr_or_dim_nullable.back().data(),
          buff.validity_vector_.buffer_size());
    }
  }
}

template void check_subarray<int8_t>(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<int8_t>& ranges);
template void check_subarray<uint8_t>(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<uint8_t>& ranges);
template void check_subarray<int16_t>(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<int16_t>& ranges);
template void check_subarray<uint16_t>(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<uint16_t>& ranges);
template void check_subarray<int32_t>(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<int32_t>& ranges);
template void check_subarray<uint32_t>(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<uint32_t>& ranges);
template void check_subarray<int64_t>(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<int64_t>& ranges);
template void check_subarray<uint64_t>(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<uint64_t>& ranges);
template void check_subarray<float>(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<float>& ranges);
template void check_subarray<double>(
    tiledb::sm::Subarray& subarray, const SubarrayRanges<double>& ranges);

template void check_subarray_equiv<int8_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template void check_subarray_equiv<uint8_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template void check_subarray_equiv<int16_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template void check_subarray_equiv<uint16_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template void check_subarray_equiv<int32_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template void check_subarray_equiv<uint32_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template void check_subarray_equiv<int64_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template void check_subarray_equiv<uint64_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template void check_subarray_equiv<float>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template void check_subarray_equiv<double>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);

template bool subarray_equiv<int8_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template bool subarray_equiv<uint8_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template bool subarray_equiv<int16_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template bool subarray_equiv<uint16_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template bool subarray_equiv<int32_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template bool subarray_equiv<uint32_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template bool subarray_equiv<int64_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template bool subarray_equiv<uint64_t>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template bool subarray_equiv<float>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);
template bool subarray_equiv<double>(
    tiledb::sm::Subarray& subarray1, tiledb::sm::Subarray& subarray2);

template void create_subarray<int8_t>(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<int8_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges);
template void create_subarray<uint8_t>(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<uint8_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges);
template void create_subarray<int16_t>(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<int16_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges);
template void create_subarray<uint16_t>(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<uint16_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges);
template void create_subarray<int32_t>(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<int32_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges);
template void create_subarray<uint32_t>(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<uint32_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges);
template void create_subarray<int64_t>(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<int64_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges);
template void create_subarray<uint64_t>(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<uint64_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges);
template void create_subarray<float>(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<float>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges);
template void create_subarray<double>(
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<double>& ranges,
    tiledb::sm::Layout layout,
    tiledb::sm::Subarray* subarray,
    bool coalesce_ranges);

template void create_subarray<int8_t>(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<int8_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges);
template void create_subarray<uint8_t>(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<uint8_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges);
template void create_subarray<int16_t>(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<int16_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges);
template void create_subarray<uint16_t>(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<uint16_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges);
template void create_subarray<int32_t>(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<int32_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges);
template void create_subarray<uint32_t>(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<uint32_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges);
template void create_subarray<int64_t>(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<int64_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges);
template void create_subarray<uint64_t>(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<uint64_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges);
template void create_subarray<float>(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<float>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges);
template void create_subarray<double>(
    tiledb_ctx_t* ctx,
    shared_ptr<tiledb::sm::Array> array,
    const SubarrayRanges<double>& ranges,
    tiledb::sm::Layout layout,
    tiledb_subarray_t** subarray,
    bool coalesce_ranges);

template void create_subarray<int8_t>(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<int8_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges);
template void create_subarray<uint8_t>(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<uint8_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges);
template void create_subarray<int16_t>(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<int16_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges);
template void create_subarray<uint16_t>(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<uint16_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges);
template void create_subarray<int32_t>(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<int32_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges);
template void create_subarray<uint32_t>(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<uint32_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges);
template void create_subarray<int64_t>(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<int64_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges);
template void create_subarray<uint64_t>(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<uint64_t>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges);
template void create_subarray<float>(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<float>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges);
template void create_subarray<double>(
    const tiledb::Context* ctx,
    const tiledb::Array* array,
    const SubarrayRanges<double>& ranges,
    tiledb::sm::Layout layout,
    tiledb::Subarray** subarray,
    bool coalesce_ranges);

template void check_partitions<int8_t>(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<int8_t>>& partitions,
    bool last_unsplittable);
template void check_partitions<uint8_t>(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<uint8_t>>& partitions,
    bool last_unsplittable);
template void check_partitions<int16_t>(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<int16_t>>& partitions,
    bool last_unsplittable);
template void check_partitions<uint16_t>(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<uint16_t>>& partitions,
    bool last_unsplittable);
template void check_partitions<int32_t>(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<int32_t>>& partitions,
    bool last_unsplittable);
template void check_partitions<uint32_t>(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<uint32_t>>& partitions,
    bool last_unsplittable);
template void check_partitions<int64_t>(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<int64_t>>& partitions,
    bool last_unsplittable);
template void check_partitions<uint64_t>(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<uint64_t>>& partitions,
    bool last_unsplittable);
template void check_partitions<float>(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<float>>& partitions,
    bool last_unsplittable);
template void check_partitions<double>(
    tiledb::sm::SubarrayPartitioner& partitioner,
    const std::vector<SubarrayRanges<double>>& partitions,
    bool last_unsplittable);

template void read_array<int8_t>(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<int8_t>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);
template void read_array<uint8_t>(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<uint8_t>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);
template void read_array<int16_t>(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<int16_t>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);
template void read_array<uint16_t>(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<uint16_t>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);
template void read_array<int32_t>(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<int32_t>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);
template void read_array<uint32_t>(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<uint32_t>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);
template void read_array<int64_t>(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<int64_t>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);
template void read_array<uint64_t>(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<uint64_t>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);
template void read_array<float>(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<float>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);
template void read_array<double>(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const SubarrayRanges<double>& ranges,
    tiledb_layout_t layout,
    const QueryBuffers& buffers);

template void check_counts<int32_t>(
    span<int32_t> vals, std::vector<uint64_t> expected);
template void check_counts<uint64_t>(
    span<uint64_t> vals, std::vector<uint64_t> expected);

}  // End of namespace test

}  // End of namespace tiledb
