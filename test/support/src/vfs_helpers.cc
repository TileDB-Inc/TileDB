/**
 * @file   vfs_helpers.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2023 TileDB, Inc.
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

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"

#include "tiledb/sm/rest/rest_client.h"

namespace tiledb::test {

tiledb::sm::URI test_dir(const std::string& prefix) {
  return tiledb::sm::URI(prefix + "tiledb-" + std::to_string(PRNG::get()()));
}

std::vector<std::unique_ptr<SupportedFs>> vfs_test_get_fs_vec() {
  std::vector<std::unique_ptr<SupportedFs>> fs_vec;

  bool supports_s3 = false;
  bool supports_azure = false;
  bool supports_gcs = false;
  bool supports_rest_s3 = false;
  get_supported_fs(
      &supports_s3, &supports_azure, &supports_gcs, &supports_rest_s3);

  if (supports_s3) {
    fs_vec.emplace_back(std::make_unique<SupportedFsS3>());
  }

  if (supports_azure) {
    fs_vec.emplace_back(std::make_unique<SupportedFsAzure>());
  }

  if (supports_gcs) {
    fs_vec.emplace_back(std::make_unique<SupportedFsGCS>());
    fs_vec.emplace_back(std::make_unique<SupportedFsGCS>("gs://"));
  }

  if (supports_rest_s3) {
    if (tiledb::sm::filesystem::s3_enabled) {
      fs_vec.emplace_back(std::make_unique<SupportedFsS3>(true));
    } else {
      throw tiledb::sm::filesystem::BuiltWithout("S3");
    }
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
  CHECK(tiledb_vfs_is_dir(ctx, vfs, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir) {
    CHECK(tiledb_vfs_remove_dir(ctx, vfs, path.c_str()) == TILEDB_OK);
  }
}

void vfs_test_create_temp_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const std::string& path) {
  vfs_test_remove_temp_dir(ctx, vfs, path);
  CHECK(tiledb_vfs_create_dir(ctx, vfs, path.c_str()) == TILEDB_OK);
}

std::string vfs_array_uri(
    const std::unique_ptr<SupportedFs>& fs,
    const std::string& array_name,
    tiledb_ctx_t* ctx) {
  bool legacy = fs->is_rest() ? ctx->rest_client().rest_legacy() : false;
  if (fs->is_rest() && legacy) {
    return "tiledb://unit/" + array_name;
  } else if (fs->is_rest() && !legacy) {
    // Include a space in the URI to test URL encoding.
    return "tiledb://unit workspace/unit teamspace/" + array_name;
  } else {
    return array_name;
  }
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
      tiledb_config_set(config, "ssl.verify", "false", &error) == TILEDB_OK);
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

bool SupportedFsS3::is_rest() {
  return rest_;
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

VFSTestBase::VFSTestBase(
    const std::vector<size_t>& test_tree, const std::string& prefix)
    : test_tree_(test_tree)
    , compute_(4)
    , io_(4)
    , vfs_(
          &tiledb::test::g_helper_stats,
          tiledb::test::g_helper_logger().get(),
          &io_,
          &compute_,
          create_test_config())
    , prefix_(prefix)
    , temp_dir_(tiledb::test::test_dir(prefix_))
    , is_supported_(vfs_.supports_uri_scheme(temp_dir_)) {
  // TODO: Throw when we can provide a list of supported filesystems to Catch2.
}

VFSTestBase::~VFSTestBase() {
  try {
    if (vfs_.supports_uri_scheme(temp_dir_)) {
      if (vfs_.is_dir(temp_dir_)) {
        REQUIRE_NOTHROW(vfs_.remove_dir(temp_dir_));
      }
    }
  } catch (const std::exception& e) {
    // Suppress exceptions in destructors.
  }
}

tiledb::sm::Config VFSTestBase::create_test_config() {
  tiledb::sm::Config cfg;
  if constexpr (!tiledb::test::aws_s3_config) {
    // Set up connection to minio backend emulator.
    cfg.set("vfs.s3.endpoint_override", "localhost:9999").ok();
    cfg.set("vfs.s3.scheme", "https").ok();
    cfg.set("vfs.s3.use_virtual_addressing", "false").ok();
    cfg.set("ssl.verify", "false").ok();
  }
  cfg.set("vfs.azure.storage_account_name", "devstoreaccount1").ok();
  cfg.set(
         "vfs.azure.storage_account_key",
         "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
         "K1SZFPTOtr/KBHBeksoGMGw==")
      .ok();
  cfg.set("vfs.azure.blob_endpoint", "http://127.0.0.1:10000/devstoreaccount1")
      .ok();
  return cfg;
}

VFSTest::VFSTest(
    const std::vector<size_t>& test_tree, const std::string& prefix)
    : VFSTestBase(test_tree, prefix) {
  if (!is_supported()) {
    return;
  }

  if (temp_dir_.is_file() || temp_dir_.is_memfs()) {
    REQUIRE_NOTHROW(vfs_.create_dir(temp_dir_));
  } else {
    REQUIRE_NOTHROW(vfs_.create_bucket(temp_dir_));
  }
  for (size_t i = 1; i <= test_tree_.size(); i++) {
    sm::URI path = temp_dir_.join_path("subdir_" + std::to_string(i));
    // VFS::create_dir is a no-op for S3.
    REQUIRE_NOTHROW(vfs_.create_dir(path));
    for (size_t j = 1; j <= test_tree_[i - 1]; j++) {
      auto object_uri = path.join_path("test_file_" + std::to_string(j));
      REQUIRE_NOTHROW(vfs_.touch(object_uri));
      std::string data(j * 10, 'a');
      vfs_.open_file(object_uri, sm::VFSMode::VFS_WRITE).ok();
      REQUIRE_NOTHROW(vfs_.write(object_uri, data.data(), data.size()));
      vfs_.close_file(object_uri).ok();
      expected_results().emplace_back(object_uri.to_string(), data.size());
    }
  }
  std::sort(expected_results().begin(), expected_results().end());
}

LocalFsTest::LocalFsTest(const std::vector<size_t>& test_tree)
    : VFSTestBase(test_tree, "file://") {
#ifdef _WIN32
  temp_dir_ =
      tiledb::test::test_dir(prefix_ + tiledb::sm::Win::current_dir() + "/");
#else
  temp_dir_ =
      tiledb::test::test_dir(prefix_ + tiledb::sm::Posix::current_dir() + "/");
#endif

  REQUIRE_NOTHROW(vfs_.create_dir(temp_dir_));
  // TODO: We could refactor to remove duplication with S3Test()
  for (size_t i = 1; i <= test_tree_.size(); i++) {
    sm::URI path = temp_dir_.join_path("subdir_" + std::to_string(i));
    REQUIRE_NOTHROW(vfs_.create_dir(path));
    expected_results().emplace_back(path.to_string(), 0);
    for (size_t j = 1; j <= test_tree_[i - 1]; j++) {
      auto object_uri = path.join_path("test_file_" + std::to_string(j));
      REQUIRE_NOTHROW(vfs_.touch(object_uri));
      std::string data(j * 10, 'a');
      vfs_.open_file(object_uri, sm::VFSMode::VFS_WRITE).ok();
      REQUIRE_NOTHROW(vfs_.write(object_uri, data.data(), data.size()));
      vfs_.close_file(object_uri).ok();
      expected_results().emplace_back(object_uri.to_string(), data.size());
    }
  }
  std::sort(expected_results().begin(), expected_results().end());
}

bool VFSTestSetup::is_legacy_rest() {
  return ctx_c->rest_client().rest_legacy();
}

}  // namespace tiledb::test
