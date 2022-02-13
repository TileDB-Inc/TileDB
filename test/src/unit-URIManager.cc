/**
 * @file unit-URIManager.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Tests the `URIManager` class.
 */

#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb/sm/array/uri_manager.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;
using namespace tiledb::test;

struct URIManagerFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  std::string temp_dir_;
  std::string array_name_;
  StorageManager* storage_manager_;

  URIManagerFx();
  ~URIManagerFx();
};

URIManagerFx::URIManagerFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());

  // Create temporary directory
  temp_dir_ = fs_vec_[0]->temp_dir();
  create_dir(temp_dir_, ctx_, vfs_);

  // Set array name
  array_name_ = temp_dir_ + "uri_manager_array";

  // Set storage manager
  storage_manager_ = ctx_->ctx_->storage_manager();
}

URIManagerFx::~URIManagerFx() {
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

TEST_CASE_METHOD(
    URIManagerFx, "URIManager: Basic tests", "[URIManager][basic]") {
  URIManager(storage_manager_, URI(array_name_), 0, UINT64_MAX);
}
