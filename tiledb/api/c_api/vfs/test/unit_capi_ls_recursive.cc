/**
 * @file tiledb/api/c_api/vfs/test/unit_capi_ls_recursive.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Tests the C API for tiledb_vfs_ls_recursive.
 *
 * TODO: This test is built and ran as part of tiledb_unit. Once we're able to
 *      execute these tests in CI, we should build this test as a separate unit.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/vfs_helpers.h"

using namespace tiledb::test;

TEST_CASE("C API: ls_recursive callback", "[vfs][ls-recursive]") {
  using tiledb::sm::LsObjects;
  S3Test s3_test({10, 50});
  if (!s3_test.is_supported()) {
    return;
  }
  auto expected = s3_test.expected_results();

  tiledb_error_t* err = nullptr;
  tiledb_config_t* config;
  tiledb_config_alloc(&config, &err);
  if constexpr (!tiledb::test::aws_s3_config) {
    tiledb_config_set(
        config, "vfs.s3.endpoint_override", "localhost:9999", &err);
    tiledb_config_set(config, "vfs.s3.scheme", "https", &err);
    tiledb_config_set(config, "vfs.s3.use_virtual_addressing", "false", &err);
    tiledb_config_set(config, "vfs.s3.verify_ssl", "false", &err);
  }
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(config, &ctx);

  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, config, &vfs);

  LsObjects data;
  tiledb_ls_callback_t cb = [](const char* path,
                               size_t path_len,
                               uint64_t object_size,
                               void* data) -> int32_t {
    auto* ls_data = static_cast<LsObjects*>(data);
    ls_data->push_back({{path, path_len}, object_size});
    return 1;
  };

  // This callback will return 0 exactly once. Traversal should stop immediately
  // and not continue to the next object.
  SECTION("callback stops traversal") {
    cb = [](const char* path,
            size_t path_len,
            uint64_t object_size,
            void* data) -> int32_t {
      // There's no precheck here to push_back, so the vector size will match
      // the number of times the callback was executed.
      auto* ls_data = static_cast<LsObjects*>(data);
      ls_data->push_back({{path, path_len}, object_size});
      // Stop traversal after we collect 10 results.
      return ls_data->size() != 10;
    };
    expected.resize(10);
  }

  CHECK(
      tiledb_vfs_ls_recursive(ctx, vfs, s3_test.temp_dir_.c_str(), cb, &data) ==
      TILEDB_OK);
  CHECK(data.size() == expected.size());
  CHECK(data == expected);
}

TEST_CASE("C API: ls_recursive throwing callback", "[vfs][ls-recursive]") {
  using tiledb::sm::LsObjects;
  S3Test s3_test({10, 50});
  if (!s3_test.is_supported()) {
    return;
  }
  auto expected = s3_test.expected_results();

  tiledb_error_t* err = nullptr;
  tiledb_config_t* config;
  tiledb_config_alloc(&config, &err);
  if constexpr (!tiledb::test::aws_s3_config) {
    tiledb_config_set(
        config, "vfs.s3.endpoint_override", "localhost:9999", &err);
    tiledb_config_set(config, "vfs.s3.scheme", "https", &err);
    tiledb_config_set(config, "vfs.s3.use_virtual_addressing", "false", &err);
    tiledb_config_set(config, "vfs.s3.verify_ssl", "false", &err);
  }
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(config, &ctx);

  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, config, &vfs);

  LsObjects data;
  tiledb_ls_callback_t cb =
      [](const char*, size_t, uint64_t, void*) -> int32_t {
    throw std::runtime_error("Throwing callback");
  };

  CHECK(
      tiledb_vfs_ls_recursive(ctx, vfs, s3_test.temp_dir_.c_str(), cb, &data) ==
      TILEDB_ERR);
  CHECK(data.empty());
}
