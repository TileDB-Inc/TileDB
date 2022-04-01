/**
 * @file   unit-capi-file.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests the TileDB File type with the C API.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/filesystem/path_win.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/global_state/unit_test_config.h"

#include <iostream>

using namespace tiledb::test;

static const std::string files_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/files";

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

struct FileFx {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  tiledb_config_t* config_;

  // Vector of supported filesystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  // Encryption parameters
  tiledb_encryption_type_t encryption_type_ = TILEDB_NO_ENCRYPTION;
  const char* encryption_key_ = nullptr;

  // Functions
  FileFx();
  ~FileFx();
  void create_temp_dir(const std::string& path) const;
  void remove_temp_dir(const std::string& path) const;
  static std::string random_name(const std::string& prefix);

  std::string localfs_temp_dir_;
};

FileFx::FileFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config_, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, config_).ok());

// Create temporary directory based on the supported filesystem
#ifdef _WIN32
  SupportedFsLocal windows_fs;
  localfs_temp_dir_ = windows_fs.temp_dir();
#else
  SupportedFsLocal posix_fs;
  localfs_temp_dir_ = posix_fs.temp_dir();
#endif

  create_dir(localfs_temp_dir_, ctx_, vfs_);
}

FileFx::~FileFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_config_free(&config_);
}

void FileFx::create_temp_dir(const std::string& path) const {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void FileFx::remove_temp_dir(const std::string& path) const {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
  else {
    int is_file = 0;
    REQUIRE(
        tiledb_vfs_is_file(ctx_, vfs_, path.c_str(), &is_file) == TILEDB_OK);
    if (is_file)
      REQUIRE(tiledb_vfs_remove_file(ctx_, vfs_, path.c_str()) == TILEDB_OK);
  }
}

std::string FileFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

TEST_CASE_METHOD(
    FileFx,
    "C API: Test blob_array create default",
    "[capi][tiledb_array_file][basic]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string base_array_name = "blob_array_test_create";
  std::string array_name = temp_dir + base_array_name;

  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }

  SECTION("- with encryption") {
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
  }

  remove_temp_dir(array_name);
  create_temp_dir(temp_dir);

  tiledb_config_t* cfg = nullptr;
  tiledb_error_t* err = nullptr;
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    int32_t rc = tiledb_config_alloc(&cfg, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    uint32_t key_len = (uint32_t)strlen(encryption_key_);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
  }

  tiledb_array_t* array;
  CHECK(
      tiledb_array_as_file_obtain(ctx_, &array, array_name.c_str(), cfg) ==
      TILEDB_OK);

  // Clean up
  if (cfg) {
    tiledb_config_free(&cfg);
  }
  remove_temp_dir(array_name);
}

TEST_CASE_METHOD(
    FileFx,
    "C API: Test blob_array create with import from uri",
    "[capi][tiledb_array_file][basic]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string base_array_name = "blob_array_test_create";
  std::string array_name = temp_dir + base_array_name;

  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }

  SECTION("- with encryption") {
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
  }

  remove_temp_dir(array_name);
  create_temp_dir(temp_dir);

  tiledb_config_t* cfg = nullptr;
  tiledb_error_t* err = nullptr;
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    int32_t rc = tiledb_config_alloc(&cfg, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    uint32_t key_len = (uint32_t)strlen(encryption_key_);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
  }

  const std::string csv_path = files_dir + "/" + "quickstart_dense.csv";
  tiledb_array_t* array;
  CHECK(
      tiledb_array_as_file_obtain(ctx_, &array, array_name.c_str(), cfg) ==
      TILEDB_OK);
  array->array_->set_timestamp_end(array->array_->timestamp_end() + 1);
  CHECK(
      tiledb_array_as_file_import(ctx_, array, csv_path.c_str()) == TILEDB_OK);

  // Clean up, ctx_/vfs_ are freed on test destructor
  if (cfg) {
    tiledb_config_free(&cfg);
  }
  remove_temp_dir(array_name);
}

TEST_CASE_METHOD(
    FileFx,
    "C API: Test blob_array save and export from uri",
    "[capi][tiledb_array_file][basic]") {
  std::string remote_temp_dir = fs_vec_[0]->temp_dir();
  std::string temp_dir = localfs_temp_dir_;

  std::cout << "C API: Test blob_array save and export from uri" << std::endl;

  std::cout << "temp_dir " << temp_dir << std::endl;
  std::cout << "localfs_temp_dir_" << localfs_temp_dir_ << std::endl;

  std::string base_array_name = "blob_array_test_create";
  std::string array_name = remote_temp_dir + base_array_name;
  std::string output_pathA = localfs_temp_dir_ + "outA";
  std::string output_pathB = localfs_temp_dir_ + "outB";
  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }

  SECTION("- with encryption") {
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
  }

  tiledb_config_t* cfg = nullptr;
  tiledb_error_t* err = nullptr;
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    int32_t rc = tiledb_config_alloc(&cfg, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    uint32_t key_len = (uint32_t)strlen(encryption_key_);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
  }

  tiledb_array_t* array = nullptr;
  tiledb_array_t* array2 = nullptr;
  int32_t is_array_open, is_array2_open;

  auto prep_clean_data = [&](bool init_timestamp = true) -> void {
    if (array) {
      CHECK(tiledb_array_is_open(ctx_, array, &is_array_open) == TILEDB_OK);
      CHECK(is_array_open == 0);
      tiledb_array_free(&array);
    }
    if (array2) {
      CHECK(tiledb_array_is_open(ctx_, array2, &is_array2_open) == TILEDB_OK);
      CHECK(is_array2_open == 0);
      tiledb_array_free(&array2);
    }

    remove_temp_dir(array_name);
    remove_temp_dir(temp_dir);
    create_temp_dir(temp_dir);

    REQUIRE(
        tiledb_array_as_file_obtain(ctx_, &array, array_name.c_str(), cfg) ==
        TILEDB_OK);

    if (init_timestamp) {
      array->array_->set_timestamp_end(1);
    }
    REQUIRE(
        tiledb_array_as_file_obtain(ctx_, &array2, array_name.c_str(), cfg) ==
        TILEDB_OK);
    array2->array_->set_timestamp_end(1);
  };

  // one export withOUT having set timestamp, to mimic basic functionality
  // for real-world usage.
  prep_clean_data(false);

  const std::string csv_name = "quickstart_dense.csv";
  const std::string csv_path = files_dir + "/" + csv_name;
  // array->array_->set_timestamp_end(array->array_->timestamp_end() + 1);
  array2->array_->set_timestamp_end(array->array_->timestamp_end());
  CHECK(
      tiledb_array_as_file_import(ctx_, array, csv_path.c_str()) == TILEDB_OK);

  array2->array_->set_timestamp_end(array->array_->timestamp_end());
  CHECK(
      tiledb_array_as_file_export(ctx_, array2, output_pathA.c_str()) ==
      TILEDB_OK);

  uint64_t original_file_size = 0;
  CHECK(
      tiledb_vfs_file_size(ctx_, vfs_, csv_path.c_str(), &original_file_size) ==
      TILEDB_OK);
  uint64_t exported_file_size = 0;
  CHECK(
      tiledb_vfs_file_size(
          ctx_, vfs_, output_pathA.c_str(), &exported_file_size) == TILEDB_OK);

  REQUIRE(exported_file_size == original_file_size);

  auto show_dir = [&](std::string path) {
    std::vector<tiledb::sm::URI> filelist;
    tiledb::sm::URI uri_path(path);
    vfs_->vfs_->ls(uri_path, &filelist);
    std::cout << "path " << path << ", nitems " << filelist.size() << std::endl;
    for (auto afile : filelist) {
      uint64_t fsize = 0;
      vfs_->vfs_->file_size(afile, &fsize);
      std::cout << afile.to_path() << " " << fsize << std::endl;
    }
  };
  auto show_dirs = [&]() -> void {
    show_dir(temp_dir);
    show_dir(localfs_temp_dir_);
    show_dir(array_name);
#if _WIN32
    auto separator = "\\";
#else
    auto separator = "/";
#endif
    std::cout << "...__fragments..." << std::endl;
    show_dir(array_name + separator + "__fragments");
    std::cout << "...__meta..." << std::endl;
    show_dir(array_name + separator + "__meta");
  };
  // compare actual contents for equality
  auto cmp_files_check = [&](const std::string& file1,
                             const std::string& file2) -> bool {
    std::stringstream cmp_files_cmd;
#ifdef _WIN32
    // 'FC' does not like forward slashes
    cmp_files_cmd << "FC "
                  << tiledb::sm::path_win::slashes_to_backslashes(
                         // tiledb::sm::path_win::path_from_uri(csv_path))
                         tiledb::sm::path_win::path_from_uri(file1))
                  << " "
                  << tiledb::sm::path_win::slashes_to_backslashes(
                         tiledb::sm::path_win::path_from_uri(file2))
                  << " > nul";
#else
    // tiledb::sm::VFS::abs_path(output_path) << " > nul";
    cmp_files_cmd << "diff " << file1 << " " << file2 << " > nul";
#endif
    // auto result = !system(cmpfilescmd.str().c_str());
    auto result = system(cmp_files_cmd.str().c_str()) == 0;
    CHECK(result);

    if (!result) {
      std::cout << "cmp " << file1 << "," << file2 << "different." << std::endl;
      show_dirs();
    }
    return result;
  };
  cmp_files_check(csv_path, output_pathA);

  // try multiple store rapidly
  std::string infiles[] = {
      // file sizes - each file is slightly larger than previous file
      files_dir + "/" + "fileapi0.csv",
      files_dir + "/" + "fileapi1.csv",
      files_dir + "/" + "fileapi2.csv",
      files_dir + "/" + "fileapi3.csv",
      files_dir + "/" + "fileapi4.csv",
      files_dir + "/" + "fileapi5.csv",
      files_dir + "/" + "fileapi6.csv",
      files_dir + "/" + "fileapi7.csv",
      files_dir + "/" + "fileapi8.csv",
      files_dir + "/" + "fileapi9.csv",
  };
  int n_infiles = sizeof(infiles) / sizeof(infiles[0]);
  std::string outfiles[] = {
      temp_dir + "out0",
      temp_dir + "out1",
      temp_dir + "out2",
      temp_dir + "out3",
      temp_dir + "out4",
      temp_dir + "out5",
      temp_dir + "out6",
      temp_dir + "out7",
      temp_dir + "out8",
      temp_dir + "out9",
  };
  int n_outfiles = sizeof(outfiles) / sizeof(outfiles[0]);

  assert(n_infiles == n_outfiles);

  {
    // process files in order of increasing size

    prep_clean_data();

    // be sure output_path not present
    tiledb_vfs_remove_file(ctx_, vfs_, output_pathA.c_str());

    // stores only, then export (last)
    for (auto i = 0; i < n_infiles; ++i) {
      array->array_->set_timestamp_end(array->array_->timestamp_end() + 1);
      array2->array_->set_timestamp_end(array->array_->timestamp_end());
      CHECK(
          tiledb_array_as_file_import(ctx_, array, infiles[i].c_str()) ==
          TILEDB_OK);
      CHECK(tiledb_array_is_open(ctx_, array, &is_array_open) == TILEDB_OK);
      CHECK(is_array_open == 0);
    }

    CHECK(tiledb_array_is_open(ctx_, array2, &is_array2_open) == TILEDB_OK);
    CHECK(is_array2_open == 0);
    CHECK(
        tiledb_array_as_file_export(ctx_, array, output_pathB.c_str()) ==
        TILEDB_OK);
    cmp_files_check(infiles[n_infiles - 1], output_pathB);
    CHECK(
        tiledb_array_as_file_export(ctx_, array2, output_pathA.c_str()) ==
        TILEDB_OK);
    cmp_files_check(infiles[n_infiles - 1], output_pathA);

    CHECK(tiledb_array_is_open(ctx_, array, &is_array_open) == TILEDB_OK);
    CHECK(is_array_open == 0);
    CHECK(tiledb_array_is_open(ctx_, array2, &is_array2_open) == TILEDB_OK);
    CHECK(is_array2_open == 0);

    // stores intermixed with exports
    for (auto i = 0; i < n_infiles; ++i) {
      CHECK(tiledb_array_is_open(ctx_, array, &is_array_open) == TILEDB_OK);
      CHECK(is_array_open == 0);
      array->array_->set_timestamp_end(array->array_->timestamp_end() + 1);
      array2->array_->set_timestamp_end(array->array_->timestamp_end());
      CHECK(
          tiledb_array_as_file_import(ctx_, array, infiles[i].c_str()) ==
          TILEDB_OK);
      CHECK(tiledb_array_is_open(ctx_, array, &is_array_open) == TILEDB_OK);
      CHECK(is_array_open == 0);
      CHECK(tiledb_array_is_open(ctx_, array2, &is_array2_open) == TILEDB_OK);
      CHECK(is_array2_open == 0);
      CHECK(
          tiledb_array_as_file_export(ctx_, array2, outfiles[i].c_str()) ==
          TILEDB_OK);
      CHECK(tiledb_array_is_open(ctx_, array2, &is_array2_open) == TILEDB_OK);
      CHECK(is_array2_open == 0);
    }

    // compare all exports above to original source files
    for (auto i = 0; i < n_outfiles; ++i) {
      cmp_files_check(infiles[i], outfiles[i]);
    }
  }

  {
    // process files in order of decreasing size
    prep_clean_data();

    // be sure output_path not present
    tiledb_vfs_remove_file(ctx_, vfs_, output_pathA.c_str());

    // stores only, then export (last)
    for (auto i = n_infiles - 1; i >= 0; --i) {
      array->array_->set_timestamp_end(array->array_->timestamp_end() + 1);
      array2->array_->set_timestamp_end(array->array_->timestamp_end());
      CHECK(
          tiledb_array_as_file_import(ctx_, array, infiles[i].c_str()) ==
          TILEDB_OK);
      CHECK(tiledb_array_is_open(ctx_, array, &is_array_open) == TILEDB_OK);
      CHECK(is_array_open == 0);
    }

    CHECK(tiledb_array_is_open(ctx_, array2, &is_array2_open) == TILEDB_OK);
    CHECK(is_array2_open == 0);
    CHECK(
        tiledb_array_as_file_export(ctx_, array2, output_pathA.c_str()) ==
        TILEDB_OK);
    cmp_files_check(infiles[0], output_pathA);

    CHECK(tiledb_array_is_open(ctx_, array, &is_array_open) == TILEDB_OK);
    CHECK(is_array_open == 0);
    CHECK(tiledb_array_is_open(ctx_, array2, &is_array2_open) == TILEDB_OK);
    CHECK(is_array2_open == 0);

    // stores intermixed with exports
    for (auto i = n_infiles - 1; i >= 0; --i) {
      CHECK(tiledb_array_is_open(ctx_, array, &is_array_open) == TILEDB_OK);
      CHECK(is_array_open == 0);
      array->array_->set_timestamp_end(array->array_->timestamp_end() + 1);
      array2->array_->set_timestamp_end(array->array_->timestamp_end());
      CHECK(
          tiledb_array_as_file_import(ctx_, array, infiles[i].c_str()) ==
          TILEDB_OK);
      CHECK(tiledb_array_is_open(ctx_, array, &is_array_open) == TILEDB_OK);
      CHECK(is_array_open == 0);
      CHECK(tiledb_array_is_open(ctx_, array2, &is_array2_open) == TILEDB_OK);
      CHECK(is_array2_open == 0);
      CHECK(
          tiledb_array_as_file_export(ctx_, array2, outfiles[i].c_str()) ==
          TILEDB_OK);
      CHECK(tiledb_array_is_open(ctx_, array2, &is_array2_open) == TILEDB_OK);
      CHECK(is_array2_open == 0);
    }

    // compare all exports above to original source files
    // (direction irrelevant for this)
    for (auto i = 0; i < n_outfiles; ++i) {
      cmp_files_check(infiles[i], outfiles[i]);
    }
  }

  tiledb_array_free(&array);
  tiledb_array_free(&array2);

  remove_temp_dir(array_name);
  remove_temp_dir(output_pathA);
  remove_temp_dir(output_pathB);
}
