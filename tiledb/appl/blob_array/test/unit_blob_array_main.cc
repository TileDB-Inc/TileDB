/**
 * @file main.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines a test `main()` for BlobArray/BlobArraySchema tests, for
 * independent compilation away from any larger test suite.
 */

#define TILEDB_NO_API_DEPRECATION_WARNINGS 1
#define CATCH_CONFIG_MAIN
#include "unit_blob_array.h"  // covers blob_array_schema as well

#include "tiledb/appl/blob_array/blob_array.h"
#include "tiledb/appl/blob_array/blob_array_schema.h"

#include "tiledb/common/common-std.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/filesystem/path_win.h"
#include "tiledb/sm/storage_manager/context.h"

#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/vfs.h"

#include "tiledb/sm/global_state/unit_test_config.h"

#include "test/src/vfs_helpers.h"

using namespace tiledb::common;
using namespace tiledb::test;

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

static const std::string files_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/files";
// std::string(TILEDB_MODULE_CMAKE_LISTS_SOURCE_DIR);
static const std::string files_dir2 =
    // std::string(TILEDB_TEST_INPUTS_DIR) + "/files";
    std::string(TILEDB_MODULE_CMAKE_LISTS_SOURCE_DIR);

struct BlobArrayFx {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  tiledb_config_t* config_;

  // Vector of supported filesystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  // Encryption parameters
  // tiledb_encryption_type_t encryption_type_ = TILEDB_NO_ENCRYPTION;
  // const char* encryption_key_ = nullptr;

  // Functions
  BlobArrayFx();
  ~BlobArrayFx();
  void create_temp_dir(const std::string& path) const;
  void remove_temp_dir(const std::string& path) const;
  static std::string random_name(const std::string& prefix);

  std::string localfs_temp_dir_;

  //  tiledb::common::ThreadPool comp_tp_;
  //  tiledb::common::ThreadPool io_tp_;
  //  tiledb::sm::stats::Stats stats_("unit_blob_array");
};

BlobArrayFx::BlobArrayFx() {
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config_, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, config_).ok());

// Create temporary directory based on the supported filesystem
#ifdef _WIN32
  localfs_temp_dir_ = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
  tiledb::sm::Win win_fs;
  win_fs.create_dir(localfs_temp_dir_);
#else
  localfs_temp_dir_ = tiledb::sm::Posix::current_dir() + "/tiledb_test/";
  tiledb::sm::Posix posix_fs;
  posix_fs.create_dir(localfs_temp_dir_);
#endif

  remove_temp_dir(localfs_temp_dir_);  // remove any pre-existing instance
  create_dir(localfs_temp_dir_, ctx_, vfs_);
}

BlobArrayFx::~BlobArrayFx() {
  // need to remove -before- freeing below items
  remove_temp_dir(localfs_temp_dir_);

  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_config_free(&config_);
}

#if 01
void BlobArrayFx::create_temp_dir(const std::string& path) const {
  remove_temp_dir(path);
//  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
#ifdef _WIN32
  tiledb::sm::Win win_fs;
  win_fs.create_dir(path);
#else
  tiledb::sm::Posix posix_fs;
  posix_fs.create_dir(path);
#endif
}

void BlobArrayFx::remove_temp_dir(const std::string& path) const {
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
#endif

std::string BlobArrayFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

TEST_CASE_METHOD(BlobArrayFx, "blob_array basic functionality", "") {
  tdb_shared_ptr<Logger> logger;
  std::string logger_name("unit_blob_arry");
  logger = tdb::make_shared<Logger>(HERE(), logger_name);

  // Encryption parameters
  tiledb::sm::EncryptionType encryption_type =
      tiledb::sm::EncryptionType::NO_ENCRYPTION;
  const char* encryption_key = nullptr;

  SECTION("- without encryption") {
    encryption_type = tiledb::sm::EncryptionType::NO_ENCRYPTION;
    encryption_key = "";
  }

  SECTION("- with encryption") {
    encryption_type = tiledb::sm::EncryptionType::AES_256_GCM;
    encryption_key = "0123456789abcdeF0123456789abcdeF";
  }

  tiledb_config_t* cfg = nullptr;
  tiledb_error_t* err = nullptr;
  if (encryption_type != tiledb::sm::EncryptionType::NO_ENCRYPTION) {
    int32_t rc = tiledb_config_alloc(&cfg, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    uint32_t key_len = (uint32_t)strlen(encryption_key);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
  }

  tiledb::appl::BlobArray* blob_array;
  std::string test_array_name = localfs_temp_dir_ + "/" + "test_blob_array";

  auto uri_array = tiledb::sm::URI(test_array_name);

  blob_array = new (std::nothrow)
      tiledb::appl::BlobArray(uri_array, ctx_->ctx_->storage_manager());

  REQUIRE(blob_array != nullptr);

  remove_temp_dir(test_array_name);

  auto config = cfg ? cfg->config_ : config_->config_;
  REQUIRE(blob_array->create(config).ok() == true);
  REQUIRE(blob_array->is_open() == false);

  auto open_for_write = [&]() -> void {
    if (blob_array->is_open())
      REQUIRE(blob_array->close().ok() == true);
    auto open_res = blob_array->open(
        tiledb::sm::QueryType::WRITE,
        encryption_type,
        encryption_key,
        static_cast<uint32_t>(strlen(encryption_key)));
    REQUIRE(blob_array->is_open() == true);
  };
  auto open_for_read = [&]() -> void {
    if (blob_array->is_open())
      REQUIRE(blob_array->close().ok() == true);
    auto open_res = blob_array->open(
        tiledb::sm::QueryType::READ,
        encryption_type,
        encryption_key,
        static_cast<uint32_t>(strlen(encryption_key)));
    REQUIRE(blob_array->is_open() == true);
  };
  open_for_write();
  REQUIRE(blob_array->close().ok() == true);
  REQUIRE(blob_array->is_open() == false);

  open_for_read();
  REQUIRE(blob_array->close().ok() == true);
  REQUIRE(blob_array->is_open() == false);

  // mix/match states to try...
  // closed read, closed write, open read, open write

  // array is currently closed.

  Status stat;

  const std::string csv_path = files_dir + "/" + "quickstart_dense.csv";
  tiledb::sm::URI inp_uri(csv_path);
  std::vector<int> bufdata{1, 2, 3};
  std::string output_path1 = localfs_temp_dir_ + "outfile1.dat";
  std::string output_path2 = localfs_temp_dir_ + "outfile2.dat";
  tiledb::sm::URI out1_uri(output_path1);
  tiledb::sm::URI out2_uri(output_path2);

  // setting timeswtamp has no effect if blob_array itself
  // sets an initial end_timestamp which is then apparently
  // propogated across all further opens of same array object.
  // TBD:
  // With no setting of end_timestamp in blob_array constructor,
  // and setting the timestamps locally here, failues occur in the
  // tests due to a failure to retrieve sizes metadata or a failure
  // for query to complete.
  // When timestamps are not set, those failures have not been
  // occurring.
  auto basic_uri_to_array = [&](bool expected_result = false) {
    // blob_array->set_timestamp_end(blob_array->timestamp_end() + 1);
    CHECK(
        blob_array->to_array_from_uri(inp_uri, config).ok() == expected_result);
  };
  auto basic_buf_to_array = [&](bool expected_result = false) {
    // blob_array->set_timestamp_end(blob_array->timestamp_end() + 1);
    CHECK(
        blob_array
            ->to_array_from_buffer(
                bufdata.data(), bufdata.size() * sizeof(bufdata[0]), config)
            .ok() == expected_result);
  };

  auto basic_to_array = [&](bool expected_result = false) {
    basic_uri_to_array(expected_result);
    basic_buf_to_array(expected_result);
  };

  auto basic_export_vfs_fh = [&](bool expected_result = false) -> void {
    try {
      // Initialize VFS object
      auto storage_manager_ = ctx_->ctx_->storage_manager();
      auto stats = storage_manager_->stats();
      auto compute_tp = storage_manager_->compute_tp();
      auto io_tp = storage_manager_->io_tp();
      const tiledb::sm::Config* vfs_config = nullptr;
      auto ctx_config = storage_manager_->config();
      tiledb::sm::VFS vfs;
      REQUIRE(vfs.init(stats, compute_tp, io_tp, &ctx_config, vfs_config).ok());

      tiledb::sm::VFSFileHandle vfsfh(
          out2_uri, &vfs, tiledb::sm::VFSMode::VFS_WRITE);
      REQUIRE(vfsfh.is_open());

      stat = blob_array->export_to_vfs_fh(&vfsfh, nullptr);
      CHECK(stat.ok() == expected_result);

      CHECK(vfsfh.close().ok());
      CHECK(vfs.terminate().ok());
    } catch (const std::exception& e) {
      CHECK(!"exception surrounding export_to_vfs_fh() attempt");
    }
  };
  auto basic_export_to_uri = [&](bool expected_result) -> void {
    stat = blob_array->export_to_uri(out1_uri, config);
    CHECK(stat.ok() == expected_result);
  };
  auto basic_export = [&](bool expected_result) -> void {
    basic_export_vfs_fh(expected_result);
    basic_export_to_uri(expected_result);
  };

#if 0
  //basic operations that will succeed to allow dev diag stepping thru,
  //but winds up leaving array populated which causes later tests
  //to fail as they expect the array to be empty.
  open_for_write();
  basic_uri_to_array(true);
  open_for_read();
  basic_export_to_uri(true);

  REQUIRE(blob_array->close().ok() == true);
#endif

  basic_to_array(false);
  basic_export(false);

  open_for_read();

  // array open in READ mode,
  // but array is EMPTY as the write's
  // prior (to_array_... calls) were done with invalid closed state.

  basic_to_array(false);

  // array open in READ mode, but EMPTY
  // And, array is empty since any array writes above (should have) failed.
  basic_export(false);

  REQUIRE(blob_array->close().ok() == true);

  open_for_write();

  // since empty, nothing to export
  basic_export(false);

#if 0 
  //'extra' diagnostic section added to see what might happen if the two writes had 
  // file closed in between them... failures do still seem to potentially occur (on
  // the subsequent reads) tho maybe not quite as often as when both are written 
  // within same open as done further below...
  // 
  //(already) opened for write ...
  basic_uri_to_array(true); //quickstart_dense.csv 115 bytes

  open_for_read();
  stat = blob_array->export_to_uri(out1_uri, config);
  CHECK(stat.ok() == true);

  open_for_write();
  basic_buf_to_array(true); //buf 3 ints, 12 bytes


  open_for_read();
  stat = blob_array->export_to_uri(out2_uri, config);
  CHECK(stat.ok() == true);

  // leave open state as we found, open WRITE, ALTHO NOT EMPTY
  open_for_write();

#endif

  // array open WRITE but empty (unless #ifdef code above active)
  basic_to_array(true);

  REQUIRE(blob_array->close().ok() == true);

  open_for_read();

  // array open for read, has something in it
  basic_to_array(false);  // open read, unable to add

  {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    std::cout << "b4 export, encryption_type " << encryption_type_string
              << std::endl;
  }
  basic_export(true);  // open for read and non-empty, should succeed

  REQUIRE(blob_array->close().ok() == true);

  tiledb::appl::WhiteboxBlobArray wb_ba(*blob_array);
  auto open_wbba_for_write = [&]() -> void {
    if (wb_ba.is_open())
      REQUIRE(wb_ba.close().ok() == true);
    //    blob_array->set_timestamp_end(blob_array->timestamp_end() + 1);
    auto open_res = wb_ba.open(
        tiledb::sm::QueryType::WRITE,
        encryption_type,  // tiledb::sm::EncryptionType::NO_ENCRYPTION,
        encryption_key,   //"",
        static_cast<uint32_t>(
            strlen(encryption_key)));  // inherited from parent
    REQUIRE(wb_ba.is_open() == true);
  };
  auto open_wbba_for_read = [&]() -> void {
    if (wb_ba.is_open())
      REQUIRE(wb_ba.close().ok() == true);
    //    blob_array->set_timestamp_end(blob_array->timestamp_end() + 1);
    auto open_res = wb_ba.open(
        tiledb::sm::QueryType::READ,
        encryption_type,  // tiledb::sm::EncryptionType::NO_ENCRYPTION,
        encryption_key,   //"",
        static_cast<uint32_t>(
            strlen(encryption_key)));  // inherited from parent
    REQUIRE(wb_ba.is_open() == true);
  };

  tiledb::sm::Datatype value_type;
  uint32_t value_size;
  const void* ptr_value;
  CHECK(wb_ba.get_file_ext(&value_type, &value_size, &ptr_value).ok() == false);
  CHECK(
      wb_ba.get_mime_type(&value_type, &value_size, &ptr_value).ok() == false);
  CHECK(
      wb_ba.get_mime_encoding(&value_type, &value_size, &ptr_value).ok() ==
      false);
  open_wbba_for_write();
  CHECK(wb_ba.get_file_ext(&value_type, &value_size, &ptr_value).ok() == false);
  CHECK(
      wb_ba.get_mime_type(&value_type, &value_size, &ptr_value).ok() == false);
  CHECK(
      wb_ba.get_mime_encoding(&value_type, &value_size, &ptr_value).ok() ==
      false);
  open_wbba_for_read();
  CHECK(wb_ba.get_file_ext(&value_type, &value_size, &ptr_value).ok() == true);
  CHECK(wb_ba.get_mime_type(&value_type, &value_size, &ptr_value).ok() == true);
  CHECK(
      wb_ba.get_mime_encoding(&value_type, &value_size, &ptr_value).ok() ==
      true);

  REQUIRE(blob_array->close().ok() == true);

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
    show_dir(localfs_temp_dir_);
    show_dir(test_array_name);
#if _WIN32
    auto separator = "\\";
#else
    auto separator = "/";
#endif
    std::cout << "...__fragments..." << std::endl;
    show_dir(test_array_name + separator + "__fragments");
    std::cout << "...__meta..." << std::endl;
    show_dir(test_array_name + separator + "__meta");
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
      localfs_temp_dir_ + "out0",
      localfs_temp_dir_ + "out1",
      localfs_temp_dir_ + "out2",
      localfs_temp_dir_ + "out3",
      localfs_temp_dir_ + "out4",
      localfs_temp_dir_ + "out5",
      localfs_temp_dir_ + "out6",
      localfs_temp_dir_ + "out7",
      localfs_temp_dir_ + "out8",
      localfs_temp_dir_ + "out9",
  };
  int n_outfiles = sizeof(outfiles) / sizeof(outfiles[0]);

  assert(n_infiles == n_outfiles);

  REQUIRE(blob_array->close().ok() == true);
  remove_temp_dir(test_array_name);
  remove_temp_dir(localfs_temp_dir_);
  create_dir(localfs_temp_dir_, ctx_, vfs_);
  REQUIRE(blob_array->create(config).ok() == true);
  REQUIRE(blob_array->is_open() == false);

  open_for_write();
  bool expected_result = true;
  for (auto i = 0; i < n_infiles; ++i) {
    tiledb::sm::URI inp_uri(infiles[i]);
    CHECK(
        blob_array->to_array_from_uri(inp_uri, config).ok() == expected_result);
  }
  REQUIRE(blob_array->close().ok() == true);
  open_for_read();
  stat = blob_array->export_to_uri(out1_uri, config);
  CHECK(stat.ok() == true);

  open_for_read();
  stat = blob_array->export_to_uri(out2_uri, config);
  CHECK(stat.ok() == true);
  cmp_files_check(out1_uri.to_path(), out2_uri.to_path());

  for (auto i = 0; i < n_infiles; ++i) {
    tiledb::sm::URI inp_uri(infiles[i]);
    // blob_array->set_timestamp_end(blob_array->timestamp_end() + 1);
    open_for_write();
    CHECK(
        blob_array->to_array_from_uri(inp_uri, config).ok() == expected_result);

    tiledb::sm::URI out_uri(outfiles[i]);
    open_for_read();
    stat = blob_array->export_to_uri(out_uri, config);
    CHECK(stat.ok() == true);
  }

  // compare all exports above to original source files
  for (auto i = 0; i < n_outfiles; ++i) {
    cmp_files_check(infiles[i], outfiles[i]);
  }

  REQUIRE(blob_array->close().ok() == true);
  remove_temp_dir(test_array_name);
  remove_temp_dir(localfs_temp_dir_);
  create_dir(localfs_temp_dir_, ctx_, vfs_);
  REQUIRE(blob_array->create(config).ok() == true);
  REQUIRE(blob_array->is_open() == false);

  open_for_write();

  for (auto i = n_infiles - 1; i >= 0; --i) {
    tiledb::sm::URI inp_uri(infiles[i]);
    // blob_array->set_timestamp_end(blob_array->timestamp_end() + 1);
    CHECK(
        blob_array->to_array_from_uri(inp_uri, config).ok() == expected_result);
  }
  REQUIRE(blob_array->close().ok() == true);
  open_for_read();
  stat = blob_array->export_to_uri(out1_uri, config);
  CHECK(stat.ok() == true);
  open_for_read();
  stat = blob_array->export_to_uri(out2_uri, config);
  CHECK(stat.ok() == true);
  cmp_files_check(out1_uri.to_path(), out2_uri.to_path());

  REQUIRE(blob_array->close().ok() == true);

  remove_temp_dir(test_array_name);
  remove_temp_dir(localfs_temp_dir_);
  create_dir(localfs_temp_dir_, ctx_, vfs_);
  REQUIRE(blob_array->create(config).ok() == true);
  REQUIRE(blob_array->is_open() == false);

  for (auto i = n_infiles - 1; i >= 0; --i) {
    tiledb::sm::URI inp_uri(infiles[i]);
    open_for_write();
    // blob_array->set_timestamp_end(blob_array->timestamp_end() + 1);
    CHECK(
        blob_array->to_array_from_uri(inp_uri, config).ok() == expected_result);

    tiledb::sm::URI out_uri(outfiles[i]);
    open_for_read();
    stat = blob_array->export_to_uri(out_uri, config);
    CHECK(stat.ok() == true);
  }
  for (auto i = 0; i < n_outfiles; ++i) {
    cmp_files_check(infiles[i], outfiles[i]);
  }

  REQUIRE(blob_array->close().ok() == true);

  delete blob_array;
  if (cfg) {
    tiledb_config_free(&cfg);
  }
}
