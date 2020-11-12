/**
 * @file   helpers.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file defines some vfs-specific test suite helper functions.
 */

#include "test/src/vfs_test_helper.h"
#include "catch.hpp"
#include "test/src/helpers.h"

namespace tiledb {
namespace test {

Status SupportedFsS3::prepare_config(
    tiledb_config_t* config, tiledb_error_t* error) {
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
  REQUIRE(
      tiledb_config_set(
          config, "vfs.s3.endpoint_override", "localhost:9999", &error) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_config_set(config, "vfs.s3.scheme", "https", &error) == TILEDB_OK);
  REQUIRE(
      tiledb_config_set(
          config, "vfs.s3.use_virtual_addressing", "false", &error) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_config_set(config, "vfs.s3.verify_ssl", "false", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);
#endif

  return Status::Ok();
}

Status SupportedFsS3::init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  int is_bucket = 0;
  int rc = tiledb_vfs_is_bucket(ctx, vfs, S3_BUCKET.c_str(), &is_bucket);
  REQUIRE(rc == TILEDB_OK);
  if (!is_bucket) {
    rc = tiledb_vfs_create_bucket(ctx, vfs, S3_BUCKET.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
  return Status::Ok();
}

Status SupportedFsS3::close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  int is_bucket = 0;
  int rc = tiledb_vfs_is_bucket(ctx, vfs, S3_BUCKET.c_str(), &is_bucket);
  CHECK(rc == TILEDB_OK);
  if (is_bucket) {
    CHECK(tiledb_vfs_remove_bucket(ctx, vfs, S3_BUCKET.c_str()) == TILEDB_OK);
  }
  return Status::Ok();
}

Status SupportedFsHDFS::prepare_config(
    tiledb_config_t* config, tiledb_error_t* error) {
  (void)config;
  (void)error;
  return Status::Ok();
}

Status SupportedFsHDFS::init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  (void)ctx;
  (void)vfs;
  return Status::Ok();
}

Status SupportedFsHDFS::close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  (void)ctx;
  (void)vfs;
  return Status::Ok();
}

Status SupportedFsAzure::prepare_config(
    tiledb_config_t* config, tiledb_error_t* error) {
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
          "127.0.0.1:10000/devstoreaccount1",
          &error) == TILEDB_OK);
  REQUIRE(
      tiledb_config_set(config, "vfs.azure.use_https", "false", &error) ==
      TILEDB_OK);
  return Status::Ok();
}

Status SupportedFsAzure::init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  int is_container = 0;
  int rc = tiledb_vfs_is_bucket(ctx, vfs, container.c_str(), &is_container);
  REQUIRE(rc == TILEDB_OK);
  if (!is_container) {
    rc = tiledb_vfs_create_bucket(ctx, vfs, container.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
  return Status::Ok();
}

Status SupportedFsAzure::close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  int is_container = 0;
  int rc = tiledb_vfs_is_bucket(ctx, vfs, container.c_str(), &is_container);
  CHECK(rc == TILEDB_OK);
  if (is_container) {
    CHECK(tiledb_vfs_remove_bucket(ctx, vfs, container.c_str()) == TILEDB_OK);
  }
  return Status::Ok();
}

Status SupportedFsWindows::prepare_config(
    tiledb_config_t* config, tiledb_error_t* error) {
  (void)config;
  (void)error;
  return Status::Ok();
}

Status SupportedFsWindows::init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  (void)ctx;
  (void)vfs;
  return Status::Ok();
}

Status SupportedFsWindows::close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  (void)ctx;
  (void)vfs;
  return Status::Ok();
}

Status SupportedFsPosix::prepare_config(
    tiledb_config_t* config, tiledb_error_t* error) {
  (void)config;
  (void)error;
  return Status::Ok();
}

Status SupportedFsPosix::init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  (void)ctx;
  (void)vfs;
  return Status::Ok();
}

Status SupportedFsPosix::close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  (void)ctx;
  (void)vfs;
  return Status::Ok();
}

std::vector<SupportedFs*> vfs_test_get_fs_vec() {
  std::vector<SupportedFs*> fs_vec;
  bool supports_s3_ = false;
  bool supports_hdfs_ = false;
  bool supports_azure_ = false;
  get_supported_fs(&supports_s3_, &supports_hdfs_, &supports_azure_);
  if (supports_s3_) {
    SupportedFsS3* s3_fs;
    fs_vec.emplace_back(s3_fs);
  }
  if (supports_hdfs_) {
    SupportedFsHDFS* hdfs_fs;
    fs_vec.emplace_back(hdfs_fs);
  }
  if (supports_azure_) {
    SupportedFsAzure* azure_fs;
    fs_vec.emplace_back(azure_fs);
  }
#ifdef _WIN32
  SupportedFsWindows* windows_fs;
  fs_vec.emplace_back(windows_fs);
#else
  SupportedFsPosix* posix_fs;
  fs_vec.emplace_back(posix_fs);
#endif

  return fs_vec;
}

void vfs_test_init(tiledb_ctx_t** ctx, tiledb_vfs_t** vfs) {
  std::vector<SupportedFs*> fs_vec = vfs_test_get_fs_vec();
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  for (auto& supported_fs : fs_vec) {
    std::cout << "supported_fs address: " << supported_fs << std::endl;
    REQUIRE(supported_fs->prepare_config(config, error).ok());
  }
  REQUIRE(tiledb_ctx_alloc(config, ctx) == TILEDB_OK);
  REQUIRE(error == nullptr);
  vfs = nullptr;
  REQUIRE(tiledb_vfs_alloc(*ctx, config, vfs) == TILEDB_OK);
  tiledb_config_free(&config);
  for (auto& supported_fs : fs_vec) {
    REQUIRE(supported_fs->init(*ctx, *vfs).ok());
  }
}

Status vfs_test_close(
    std::vector<SupportedFs*> fs_vec, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  for (auto& fs : fs_vec) {
    RETURN_NOT_OK(fs->close(ctx, vfs));
  }
  return Status::Ok();
}

}  // End of namespace test

}  // End of namespace tiledb