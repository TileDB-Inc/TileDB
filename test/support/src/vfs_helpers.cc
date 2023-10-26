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

// The order of header includes is important here. serialization_wrappers.h
// must be included before tdb_catch.h or anything that includes tdb_catch.h.
// This is due to catch2 including Windows.h which defines things that break
// compiling TileDB.
//
// The blank line between serialization_wrappers.h and the other three headers
// is also important because clang-format will treat these as separate blocks
// of includes. If they were one block, then clang-format would insist they
// were sorted which means that serialization_wrappers.h would be included
// after tdb_catch.h.
#include "test/support/src/serialization_wrappers.h"

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"

namespace tiledb {
namespace test {

tiledb::sm::URI test_dir(const std::string& prefix) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> dist(0);
  return tiledb::sm::URI(prefix + "tiledb-" + std::to_string(dist(gen)));
}

tiledb::sm::Config create_test_config() {
  tiledb::sm::Config cfg;
  if constexpr (!tiledb::test::aws_s3_config) {
    // Set up connection to minio backend emulator.
    REQUIRE_NOTHROW(cfg.set("vfs.s3.endpoint_override", "localhost:9999"));
    REQUIRE_NOTHROW(cfg.set("vfs.s3.scheme", "https"));
    REQUIRE_NOTHROW(cfg.set("vfs.s3.use_virtual_addressing", "false"));
    REQUIRE_NOTHROW(cfg.set("vfs.s3.verify_ssl", "false"));
  }
  return cfg;
}

std::vector<std::unique_ptr<SupportedFs>> vfs_test_get_fs_vec() {
  std::vector<std::unique_ptr<SupportedFs>> fs_vec;

  bool supports_s3 = false;
  bool supports_hdfs = false;
  bool supports_azure = false;
  bool supports_gcs = false;
  get_supported_fs(
      &supports_s3, &supports_hdfs, &supports_azure, &supports_gcs);
  if (supports_s3) {
    fs_vec.emplace_back(std::make_unique<SupportedFsS3>());
  }

  if (supports_hdfs) {
    fs_vec.emplace_back(std::make_unique<SupportedFsHDFS>());
  }

  if (supports_azure) {
    fs_vec.emplace_back(std::make_unique<SupportedFsAzure>());
  }

  if (supports_gcs) {
    fs_vec.emplace_back(std::make_unique<SupportedFsGCS>());
    fs_vec.emplace_back(std::make_unique<SupportedFsGCS>("gs://"));
  }

  fs_vec.emplace_back(std::make_unique<SupportedFsLocal>());

  fs_vec.emplace_back(std::make_unique<SupportedFsMem>());

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

void vfs_test_remove_temp_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx, vfs, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir) {
    REQUIRE(tiledb_vfs_remove_dir(ctx, vfs, path.c_str()) == TILEDB_OK);
  }
}

void vfs_test_create_temp_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const std::string& path) {
  vfs_test_remove_temp_dir(ctx, vfs, path);
  REQUIRE(tiledb_vfs_create_dir(ctx, vfs, path.c_str()) == TILEDB_OK);
}

Status SupportedFsS3::prepare_config(
    [[maybe_unused]] tiledb_config_t* config,
    [[maybe_unused]] tiledb_error_t* error) {
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
      if (rc == TILEDB_OK) {
        break;
      }
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
          "http://127.0.0.1:10000/devstoreaccount1",
          &error) == TILEDB_OK);
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

void TemporaryDirectoryFixture::alloc_encrypted_ctx(
    const std::string& encryption_type,
    const std::string& encryption_key,
    tiledb_ctx_t** ctx_with_encrypt) const {
  // Get the configuration settings for the fixture's context.
  tiledb_config_t* config;
  require_tiledb_ok(tiledb_ctx_get_config(ctx, &config));

  // Change the configuration to match the desired encryption settings.
  tiledb_error_t* error;
  require_tiledb_ok(tiledb_config_set(
      config, "sm.encryption_type", encryption_type.c_str(), &error));
  REQUIRE(error == nullptr);
  require_tiledb_ok(tiledb_config_set(
      config, "sm.encryption_key", encryption_key.c_str(), &error));
  REQUIRE(error == nullptr);

  // Allocate the context with the updated configuration.
  require_tiledb_ok(tiledb_ctx_alloc(config, ctx_with_encrypt));

  // Free resources.
  tiledb_config_free(&config);
  tiledb_error_free(&error);
}

std::string TemporaryDirectoryFixture::create_temporary_array(
    std::string&& name,
    tiledb_array_schema_t* array_schema,
    const bool serialize) {
  auto array_uri = fullpath(std::move(name));
  require_tiledb_ok(tiledb_array_schema_check(ctx, array_schema));
  require_tiledb_ok(tiledb_array_create_serialization_wrapper(
      ctx, array_uri, array_schema, serialize));
  return array_uri;
}

VFSTest::VFSTest(
    const std::vector<size_t>& test_tree, const std::string& prefix)
    : test_tree_(test_tree)
    , compute_(4)
    , io_(4)
    , vfs_(
          &tiledb::test::g_helper_stats,
          &io_,
          &compute_,
          tiledb::test::create_test_config())
    , temp_dir_(tiledb::test::test_dir(prefix)) {
  if (prefix == "file://") {
#ifdef _WIN32
    temp_dir_ =
        tiledb::test::test_dir(prefix + tiledb::sm::Win::current_dir() + "/");
#else
    temp_dir_ =
        tiledb::test::test_dir(prefix + tiledb::sm::Posix::current_dir() + "/");
#endif
  }

  if (temp_dir_.is_file() || temp_dir_.is_memfs()) {
    vfs_.create_dir(temp_dir_).ok();
  } else if (vfs_.supports_uri_scheme(temp_dir_)) {
    vfs_.create_bucket(temp_dir_).ok();
  }
}

VFSTest::~VFSTest() {
  if (vfs_.supports_uri_scheme(temp_dir_)) {
    bool is_dir = false;
    vfs_.is_dir(temp_dir_, &is_dir).ok();
    if (is_dir) {
      vfs_.remove_dir(temp_dir_).ok();
    }
  }
}

void VFSTest::create_objects(
    const sm::URI& uri, size_t count, const std::string& prefix) {
  for (size_t i = 1; i <= count; i++) {
    auto object_uri = uri.join_path(prefix + std::to_string(i));
    vfs_.touch(object_uri).ok();
    std::string data(i * 10, 'a');
    vfs_.open_file(object_uri, sm::VFSMode::VFS_WRITE).ok();
    vfs_.write(object_uri, data.data(), data.size()).ok();
    vfs_.close_file(object_uri).ok();
    expected_results_.emplace_back(object_uri.to_string(), data.size());
  }
}

void VFSTest::setup_test() {
  for (size_t i = 1; i <= test_tree_.size(); i++) {
    sm::URI path = temp_dir_.join_path("subdir_" + std::to_string(i));
    // VFS::create_dir is a no-op for S3.
    vfs_.create_dir(path).ok();
    create_objects(path, test_tree_[i - 1], "test_file_");
  }
}

void VFSTest::test_ls_recursive(
    tiledb::sm::LsCallback cb, size_t expected_count) {
  LsObjects ls_objects;
  CHECK_NOTHROW(vfs_.ls_recursive(temp_dir_, cb, &ls_objects));

  std::sort(expected_results_.begin(), expected_results_.end());
  if (expected_count != 0) {
    expected_results_.resize(expected_count);
  }
  CHECK(ls_objects.size() == expected_results_.size());
  CHECK(ls_objects == expected_results_);
}

#ifdef HAVE_S3
S3Test::S3Test(const std::vector<size_t>& test_tree)
    : VFSTest(test_tree, "s3://")
    , s3_(&tiledb::test::g_helper_stats, &io_, vfs_.config()) {
}

void S3Test::create_objects(
    const sm::URI& uri, size_t count, const std::string& prefix) {
  for (size_t i = 1; i <= count; i++) {
    auto object_uri = uri.join_path(prefix + std::to_string(i));
    s3_.touch(object_uri).ok();
    std::string data(i * 10, 'a');
    s3_.write(object_uri, data.data(), data.size()).ok();
    s3_.flush_object(object_uri).ok();
    expected_results_.emplace_back(object_uri.to_string(), data.size());
  }
}

void S3Test::setup_test() {
  for (size_t i = 1; i <= test_tree_.size(); i++) {
    sm::URI path = temp_dir_.join_path("subdir_" + std::to_string(i));
    // VFS::create_dir is a no-op for S3; Just create objects.
    create_objects(path, test_tree_[i - 1], "test_file_");
  }
}

void S3Test::test_ls_cb(tiledb::sm::LsCallback cb, bool recursive) {
  S3Test::LsObjects ls_objects;
  // If testing with recursion use the root directory, otherwise use a subdir.
  auto path = recursive ? temp_dir_ : temp_dir_.join_path("subdir_1");
  if (recursive) {
    CHECK_NOTHROW(s3_.ls_cb(path, cb, &ls_objects, ""));
  } else {
    CHECK_NOTHROW(s3_.ls_cb(path, cb, &ls_objects));
  }

  if (!recursive) {
    // If non-recursive, all objects in the first directory should be
    // returned.
    expected_results_.resize(test_tree_[0]);
  }
  std::sort(expected_results_.begin(), expected_results_.end());
  CHECK(ls_objects.size() == expected_results_.size());
  CHECK(ls_objects == expected_results_);
}
#else
S3Test::S3Test(const std::vector<size_t>& test_tree)
    : VFSTest(test_tree, "s3://") {
}

void S3Test::create_objects(const sm::URI&, size_t, const std::string&) {
}

void S3Test::setup_test() {
}

void S3Test::test_ls_cb(tiledb::sm::LsCallback, bool) {
}
#endif

}  // End of namespace test
}  // End of namespace tiledb
