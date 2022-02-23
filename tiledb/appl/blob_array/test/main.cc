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

#define CATCH_CONFIG_MAIN
#include "unit_blob_array.h"  // covers blob_array_schema as well

#include "tiledb/appl/blob_array/blob_array.h"
#include "tiledb/appl/blob_array/blob_array_schema.h"

#include "tiledb/common/common-std.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/storage_manager/context.h"
//#include "tiledb/common/logger_public.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_type.h"

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
  posix.create_dir(localfs_temp_dir_);
#endif

  remove_temp_dir(localfs_temp_dir_); // remove any pre-existing instance
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
  //localfs_temp_dir_ = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
  tiledb::sm::Win win_fs;
  win_fs.create_dir(path);
#else
  //localfs_temp_dir_ = tiledb::sm::Posix::current_dir() + "/tiledb_test/";
  tiledb::sm::Posix posix_fs;
  posix.create_dir(path);
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
  //tiledb::sm::Context ctx(*ctx_->ctx_);
  //tiledb::sm::Config cfg(*config_->config_);
  // tiledb::common::ThreadPool comp_tp;
  // tiledb::common::ThreadPool io_tp;
  // tiledb::sm::stats::Stats stats("unit_blob_array");
  tdb_shared_ptr<Logger> logger;
  std::string logger_name("unit_blob_arry");
  // logger = tiledb::common::make_shared<Logger>(logger_name);
  logger = std::make_shared<Logger>(logger_name);
  // logger = tiledb::common::make_shared<Logger>("unit_blob_arry", , true);
  // tiledb::sm::StorageManager stg_mgr(ctx.compute_tp(), ctx.io_tp(),
  // ctx.stats(), logger);

  // Encryption parameters
  tiledb::sm::EncryptionType encryption_type =
      tiledb::sm::EncryptionType::NO_ENCRYPTION;
  const char* encryption_key = nullptr;

  bool repeating = true;
#if 01
  SECTION("- without encryption") {
    encryption_type = tiledb::sm::EncryptionType::NO_ENCRYPTION;
    encryption_key = "";
  }
#endif
  #if 0
  SECTION("- with encryption") {
    encryption_type = tiledb::sm::EncryptionType::AES_256_GCM;
    encryption_key = "0123456789abcdeF0123456789abcdeF";
  }
  #endif
  #if 01
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
  #endif

  tiledb::appl::BlobArraySchema* blob_array_schema;
  tiledb::appl::BlobArray* blob_array;
  std::string test_array_name = localfs_temp_dir_ + "/" + "test_blob_array";

  auto uri_array = tiledb::sm::URI(test_array_name);

  blob_array = new (std::nothrow)
      tiledb::appl::BlobArray(uri_array, ctx_->ctx_->storage_manager());

  REQUIRE(blob_array != nullptr);

  remove_temp_dir(test_array_name);

//  REQUIRE(blob_array->create(config_->config_).ok() == true);
  auto config = cfg ? cfg->config_ : config_->config_;
  REQUIRE(blob_array->create(config).ok() == true);
  REQUIRE(blob_array->is_open() == false);

  auto open_for_write = [&]() -> void { 
    if (blob_array->is_open())
      REQUIRE(blob_array->close().ok() == true);
    auto open_res = blob_array->open(
        tiledb::sm::QueryType::WRITE,
        encryption_type,  // tiledb::sm::EncryptionType::NO_ENCRYPTION,
        encryption_key,   //"",
        static_cast<uint32_t>(
            strlen(encryption_key)));  // inherited from parent
    REQUIRE(blob_array->is_open() == true);
  };
  auto open_for_read = [&]() -> void {
    if (blob_array->is_open())
      REQUIRE(blob_array->close().ok() == true);
    auto open_res = blob_array->open(
        tiledb::sm::QueryType::READ,
        encryption_type,  // tiledb::sm::EncryptionType::NO_ENCRYPTION,
        encryption_key,   //"",
        static_cast<uint32_t>(
            strlen(encryption_key)));  // inherited from parent
    REQUIRE(blob_array->is_open() == true);
  };
#if 1
  open_for_write();
#else
  auto open_res = blob_array->open(
      tiledb::sm::QueryType::WRITE,
      encryption_type, //tiledb::sm::EncryptionType::NO_ENCRYPTION,
      encryption_key, //"",
      static_cast<uint32_t>(strlen(encryption_key)));  // inherited from parent
  REQUIRE(open_res.ok() == true);
  REQUIRE(blob_array->is_open() == true);
#endif
  REQUIRE(blob_array->close().ok() == true);
  REQUIRE(blob_array->is_open() == false);

  #if 1
  open_for_read();
  #else
  open_res = blob_array->open(
      tiledb::sm::QueryType::READ,
      encryption_type, //tiledb::sm::EncryptionType::NO_ENCRYPTION,
      encryption_key, //"",
      static_cast<uint32_t>(strlen(encryption_key)));  // inherited from parent
  REQUIRE(open_res.ok() == true);
  REQUIRE(blob_array->is_open() == true);
  #endif
  REQUIRE(blob_array->close().ok() == true);
  REQUIRE(blob_array->is_open() == false);

  // mix/match states to try...
  // closed read, closed write, open read, open write

  // array is currently closed.

  Status stat;

  const std::string csv_path = files_dir + "/" + "quickstart_dense.csv";
  tiledb::sm::URI inp_uri(csv_path);
  std::vector<int> bufdata{1, 2, 3};
  //std::string output_path = localfs_temp_dir_ + "outfile.dat";
  std::string output_path1 = localfs_temp_dir_ + "outfile1.dat";
  std::string output_path2 = localfs_temp_dir_ + "outfile2.dat";
  //tiledb::sm::URI out_uri(output_path);
  tiledb::sm::URI out1_uri(output_path1);
  tiledb::sm::URI out2_uri(output_path2);

  auto basic_uri_to_array = [&](bool expected_result = false) {
    CHECK(
        // blob_array->to_array_from_uri(inp_uri, config_->config_).ok() ==
        // expected_result);
        blob_array->to_array_from_uri(inp_uri, config).ok() == expected_result);
  };
  auto basic_buf_to_array = [&](bool expected_result = false) {
    CHECK(
        blob_array
            ->to_array_from_buffer(
                bufdata.data(),
                bufdata.size() * sizeof(bufdata[0]),
                // config_->config_)
                config)
            .ok() == expected_result);
  };
  auto basic_to_array = [&](bool expected_result = false) {
    basic_uri_to_array(expected_result);
    basic_buf_to_array(expected_result);
  };
  basic_to_array(false);

  auto basic_export = [&](bool expected_result) -> void {
    //CHECK(blob_array->export_to_uri(out_uri, config_->config_).ok() == expected_result);
#if 0
    std::cout << "b4 export_to_uri" << std::endl;
    //CHECK(
    //    blob_array->export_to_uri(out_uri, config).ok() ==
    //    expected_result);
    stat = blob_array->export_to_uri(out1_uri, config);
    CHECK(stat.ok( == expected_result));
    std::cout << "aftr export_to_uri" << std::endl;
#endif
    auto try_export_vfs_fh = [&](bool expected_result = false) -> void {
      try {
        tiledb::sm::VFS vfs;
        // Initialize VFS object
        auto storage_manager_ = ctx_->ctx_->storage_manager();
        auto stats = storage_manager_->stats();
        auto compute_tp = storage_manager_->compute_tp();
        auto io_tp = storage_manager_->io_tp();
        const tiledb::sm::Config* vfs_config = nullptr;
        auto ctx_config = storage_manager_->config();
        REQUIRE(
            vfs.init(stats, compute_tp, io_tp, &ctx_config, vfs_config).ok());

        tiledb::sm::VFSFileHandle vfsfh(
            out2_uri, &vfs, tiledb::sm::VFSMode::VFS_WRITE);
        REQUIRE(vfsfh.is_open());

        //CHECK(
        //    blob_array->export_to_vfs_fh(&vfsfh, nullptr).ok() ==
        //    expected_result);
        stat = blob_array->export_to_vfs_fh(&vfsfh, nullptr);
        CHECK(stat.ok() == expected_result);

        CHECK(vfsfh.close().ok());
        CHECK(vfs.terminate().ok());
      } catch (const std::exception& e) {
        CHECK(!"exception surrounding export_to_vfs_fh() attempt");
      }
    };
    std::cout << "b4 try_export_vfs_fh" << std::endl;
    try_export_vfs_fh(expected_result);
    std::cout << "aftr try_export_vfs_fh" << std::endl;
#if 01
    std::cout << "b4 export_to_uri" << std::endl;
    //CHECK(blob_array->export_to_uri(out_uri, config).ok() == expected_result);
    stat = blob_array->export_to_uri(out1_uri, config);
    CHECK(stat.ok() == expected_result);
    std::cout << "aftr export_to_uri" << std::endl;
#endif
  };
  basic_export(false);

  #if 1
  open_for_read();
  #else
  open_res = blob_array->open(
      tiledb::sm::QueryType::READ,
      encryption_type, //tiledb::sm::EncryptionType::NO_ENCRYPTION,
      encryption_key, //"",
      static_cast<uint32_t>(strlen(encryption_key)));  // inherited from parent
  REQUIRE(open_res.ok() == true);
  #endif

  // array open in READ mode, 
  // but array is EMPTY as the write's
  // prior (to_array_... calls) were done with invalid closed state.

  #if 1
  basic_to_array(false);
  #else
  //CHECK(blob_array->to_array_from_uri(inp_uri, config_->config_).ok() == false);
  CHECK(blob_array->to_array_from_uri(inp_uri, config).ok() == false);
  CHECK(
      blob_array
             ->to_array_from_buffer(
                 bufdata.data(),
                 bufdata.size() * sizeof(bufdata[0]),
                 //config_->config_)
                 config)
          .ok() == false);
  #endif

  // array open in READ mode, but EMPTY
  // And, array is empty since any array writes above (should have) failed.
  #if 1
  basic_export(false);
  #else
  //CHECK(blob_array->export_to_uri(out_uri, config_->config_).ok() == false);
  CHECK(blob_array->export_to_uri(out_uri, config).ok() == false);

  //...->export_to_uri() invokes ... blob_array->export_to_vfs_fh();

  try_export_vfs_fh(false);
  #endif

  REQUIRE(blob_array->close().ok() == true);

  #if 1
  open_for_write();
#else
  open_res = blob_array->open(
      tiledb::sm::QueryType::WRITE,
      encryption_type,  // tiledb::sm::EncryptionType::NO_ENCRYPTION,
      encryption_key,   //"",
      static_cast<uint32_t>(strlen(encryption_key)));  // inherited from parent
  REQUIRE(open_res.ok() == true);
  REQUIRE(blob_array->is_open() == true);
  #endif
  //REQUIRE(blob_array->close().ok() == true);
  //REQUIRE(blob_array->is_open() == false);

  //since empty, nothing to export
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

  #if 1
  open_for_read();
#else
  open_res = blob_array->open(
      tiledb::sm::QueryType::READ,
      encryption_type,  // tiledb::sm::EncryptionType::NO_ENCRYPTION,
      encryption_key,   //"",
      static_cast<uint32_t>(strlen(encryption_key)));  // inherited from parent
  REQUIRE(open_res.ok() == true);
  REQUIRE(blob_array->is_open() == true);
  #endif

  // array open for read, has something in it
  basic_to_array(false); // open read, unable to add

  {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    std::cout << "b4 export, encryption_type " << encryption_type_string << std::endl;
  }
  basic_export(true); // open for read and non-empty, should succeed
  if (encryption_type == tiledb::sm::EncryptionType::AES_256_GCM) {
    // cycle giving oppty to debug...
    repeating = false;
    do {
      basic_export(true);  // open for read and non-empty, should succeed
    } while (repeating);
  }

  // blob_array->libmagic_get_mime();
  // blob_array->libmagic_get_mime_encoding();
  // blob_array->store_mime_type();
  // blob_array->store_mime_encoding();
  tiledb::appl::WhiteboxBlobArray wb_ba(*blob_array);
  // wb_ba->

  delete blob_array;
  if (cfg) {
    tiledb_config_free(&cfg);
  }
}

#if 0
/*
 * The type lists need to be in sorted order for the tests to be valid.
 *
 * When you're not careful about rounding, you can inadvertently create values
 * that look distinct but actually are not.
 */
TEMPLATE_LIST_TEST_CASE(
    "Interval/Test - Sorted, distinct values in test lists",
    "[interval]",
    TypesUnderTest) {
  typedef TestType T;
  typedef TestTypeTraits<T> Tr;

  SECTION("outer") {
    std::vector v(Tr::outer);
    REQUIRE(v.size() >= 1);
    size_t n = v.size() - 1;
    for (unsigned int j = 0; j < n; ++j) {
      CHECK(v[j] != v[j + 1]);
      if (v[j] != v[j + 1]) {
        CHECK(v[j] < v[j + 1]);
      }
    }
  }
  SECTION("inner") {
    std::vector v(Tr::inner);
    REQUIRE(v.size() >= 1);
    size_t n = v.size() - 1;
    for (unsigned int j = 0; j < n; ++j) {
      CHECK(v[j] != v[j + 1]);
      if (v[j] != v[j + 1]) {
        CHECK(v[j] < v[j + 1]);
      }
    }
  }
}

TEMPLATE_LIST_TEST_CASE(
    "Interval/TestTypeTraits - Floating-point not-finite elements",
    "[interval]",
    TypesUnderTest) {
  typedef TestType T;
  typedef TestTypeTraits<T> Tr;

  if constexpr (std::is_floating_point_v<T>) {
    // Verify that we've defined non-numeric traits correctly
    CHECK(std::isinf(T(Tr::positive_infinity)));
    CHECK(Tr::positive_infinity > 0);
    CHECK(std::isinf(Tr::negative_infinity));
    CHECK(Tr::negative_infinity < 0);
    CHECK(std::isnan(Tr::NaN));
  }
}
#endif  // 0
