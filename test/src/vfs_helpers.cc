/**
 * @file   vfs_helpers.cc
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
 * This file defines some vfs-specific test suite helper functions.
 */
#include "catch.hpp"

#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"

namespace tiledb {
namespace test {

std::vector<std::unique_ptr<SupportedFs>> vfs_test_get_fs_vec() {
  std::vector<std::unique_ptr<SupportedFs>> fs_vec;
  bool supports_s3 = false;
  bool supports_hdfs = false;
  bool supports_azure = false;
  bool supports_gcs = false;
  get_supported_fs(
      &supports_s3, &supports_hdfs, &supports_azure, &supports_gcs);
  if (supports_s3) {
    SupportedFsS3* s3_fs = new SupportedFsS3();
    fs_vec.emplace_back(s3_fs);
  }

  if (supports_hdfs) {
    SupportedFsHDFS* hdfs_fs = new SupportedFsHDFS();
    fs_vec.emplace_back(hdfs_fs);
  }

  if (supports_azure) {
    SupportedFsAzure* azure_fs = new SupportedFsAzure();
    fs_vec.emplace_back(azure_fs);
  }

  if (supports_gcs) {
    SupportedFsGCS* gcs_fs = new SupportedFsGCS();
    fs_vec.emplace_back(gcs_fs);
    SupportedFsGCS* gs_fs = new SupportedFsGCS("gs://");
    fs_vec.emplace_back(gs_fs);
  }

  SupportedFsLocal* local_fs = new SupportedFsLocal();
  fs_vec.emplace_back(local_fs);

  SupportedFsMem* mem_fs = new SupportedFsMem();
  fs_vec.emplace_back(mem_fs);

  return fs_vec;
}

Status vfs_test_init(
    const std::vector<std::unique_ptr<SupportedFs>>& fs_vec,
    tiledb_ctx_t** ctx,
    tiledb_vfs_t** vfs,
    tiledb_config_t* config) {
  tiledb_error_t* error = nullptr;
  tiledb_config_t* config_tmp = config;
  if (config_tmp == nullptr) {
    REQUIRE(tiledb_config_alloc(&config_tmp, &error) == TILEDB_OK);
    REQUIRE(error == nullptr);
  }

  for (auto& supported_fs : fs_vec) {
    REQUIRE(supported_fs->prepare_config(config_tmp, error).ok());
    REQUIRE(error == nullptr);
  }

  REQUIRE(tiledb_ctx_alloc(config_tmp, ctx) == TILEDB_OK);
  REQUIRE(tiledb_vfs_alloc(*ctx, config_tmp, vfs) == TILEDB_OK);
  if (config == nullptr) {
    tiledb_config_free(&config_tmp);
  }

  for (auto& supported_fs : fs_vec) {
    REQUIRE(supported_fs->init(*ctx, *vfs).ok());
  }

  return Status::Ok();
}

Status vfs_test_close(
    const std::vector<std::unique_ptr<SupportedFs>>& fs_vec,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs) {
  for (auto& fs : fs_vec) {
    RETURN_NOT_OK(fs->close(ctx, vfs));
  }

  return Status::Ok();
}

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
  int rc = tiledb_vfs_is_bucket(ctx, vfs, s3_bucket_.c_str(), &is_bucket);
  REQUIRE(rc == TILEDB_OK);
  if (!is_bucket) {
    // In the CI, we've seen issues where the bucket create fails due to
    // `BucketAlreadyOwnedByYou`. We will retry 5 times, sleeping 1 second
    // between each retry if the bucket create fails here.
    for (int i = 0; i < 5; ++i) {
      rc = tiledb_vfs_create_bucket(ctx, vfs, s3_bucket_.c_str());
      if (rc == TILEDB_OK)
        break;
      else
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    REQUIRE(rc == TILEDB_OK);
  }

  rc = tiledb_vfs_is_bucket(ctx, vfs, s3_bucket_.c_str(), &is_bucket);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_bucket);

  return Status::Ok();
}

Status SupportedFsS3::close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  int is_bucket = 0;
  int rc = tiledb_vfs_is_bucket(ctx, vfs, s3_bucket_.c_str(), &is_bucket);
  CHECK(rc == TILEDB_OK);
  if (is_bucket) {
    CHECK(tiledb_vfs_remove_bucket(ctx, vfs, s3_bucket_.c_str()) == TILEDB_OK);
  }

  rc = tiledb_vfs_is_bucket(ctx, vfs, s3_bucket_.c_str(), &is_bucket);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_bucket);

  return Status::Ok();
}

std::string SupportedFsS3::temp_dir() {
  return temp_dir_;
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

std::string SupportedFsHDFS::temp_dir() {
  return temp_dir_;
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
  int rc = tiledb_vfs_is_bucket(ctx, vfs, container_.c_str(), &is_container);
  REQUIRE(rc == TILEDB_OK);
  if (!is_container) {
    rc = tiledb_vfs_create_bucket(ctx, vfs, container_.c_str());
    REQUIRE(rc == TILEDB_OK);
  }

  return Status::Ok();
}

Status SupportedFsAzure::close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  int is_container = 0;
  int rc = tiledb_vfs_is_bucket(ctx, vfs, container_.c_str(), &is_container);
  CHECK(rc == TILEDB_OK);
  if (is_container) {
    CHECK(tiledb_vfs_remove_bucket(ctx, vfs, container_.c_str()) == TILEDB_OK);
  }

  return Status::Ok();
}

std::string SupportedFsAzure::temp_dir() {
  return temp_dir_;
}

Status SupportedFsGCS::prepare_config(
    tiledb_config_t* config, tiledb_error_t* error) {
  REQUIRE(
      tiledb_config_set(config, "vfs.gcs.project_id", "TODO", &error) ==
      TILEDB_OK);

  return Status::Ok();
}

Status SupportedFsGCS::init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  int is_bucket = 0;
  int rc = tiledb_vfs_is_bucket(ctx, vfs, bucket_.c_str(), &is_bucket);
  REQUIRE(rc == TILEDB_OK);
  if (!is_bucket) {
    rc = tiledb_vfs_create_bucket(ctx, vfs, bucket_.c_str());
    REQUIRE(rc == TILEDB_OK);
  }

  return Status::Ok();
}

Status SupportedFsGCS::close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  int is_bucket = 0;
  int rc = tiledb_vfs_is_bucket(ctx, vfs, bucket_.c_str(), &is_bucket);
  CHECK(rc == TILEDB_OK);
  if (is_bucket) {
    CHECK(tiledb_vfs_remove_bucket(ctx, vfs, bucket_.c_str()) == TILEDB_OK);
  }

  return Status::Ok();
}

std::string SupportedFsGCS::temp_dir() {
  return temp_dir_;
}

Status SupportedFsLocal::prepare_config(
    tiledb_config_t* config, tiledb_error_t* error) {
  (void)config;
  (void)error;
  return Status::Ok();
}

Status SupportedFsLocal::init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  (void)ctx;
  (void)vfs;
  return Status::Ok();
}

Status SupportedFsLocal::close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  (void)ctx;
  (void)vfs;
  return Status::Ok();
}

#ifdef _WIN32
// Windows local filesystem
std::string SupportedFsLocal::temp_dir() {
  return temp_dir_;
}

std::string SupportedFsLocal::file_prefix() {
  return file_prefix_;
}

#else
std::string SupportedFsLocal::temp_dir() {
  return temp_dir_;
}

// Posix local filesystem
std::string SupportedFsLocal::file_prefix() {
  return file_prefix_;
}

#endif

Status SupportedFsMem::prepare_config(
    tiledb_config_t* config, tiledb_error_t* error) {
  (void)config;
  (void)error;
  return Status::Ok();
}

Status SupportedFsMem::init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  (void)ctx;
  (void)vfs;
  return Status::Ok();
}

Status SupportedFsMem::close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) {
  (void)ctx;
  (void)vfs;
  return Status::Ok();
}

std::string SupportedFsMem::temp_dir() {
  return temp_dir_;
}

}  // End of namespace test

}  // End of namespace tiledb
