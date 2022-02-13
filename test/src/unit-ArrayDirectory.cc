/**
 * @file unit-ArrayDirectory.cc
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
 * Tests the `ArrayDirectory` class.
 */

#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb/sm/array/array_directory.h"
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

struct ArrayDirectoryFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_t_;
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  std::string temp_dir_;
  std::string array_name_;
  StorageManager* storage_manager_;
  VFS* vfs_;
  ThreadPool* tp_;

  ArrayDirectoryFx();
  ~ArrayDirectoryFx();
};

ArrayDirectoryFx::ArrayDirectoryFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_t_).ok());

  // Create temporary directory
  temp_dir_ = fs_vec_[0]->temp_dir();
  create_dir(temp_dir_, ctx_, vfs_t_);

  // Set array name
  array_name_ = temp_dir_ + "uri_manager_array";

  // Set storage manager
  storage_manager_ = ctx_->ctx_->storage_manager();
  vfs_ = storage_manager_->vfs();
  tp_ = storage_manager_->compute_tp();
}

ArrayDirectoryFx::~ArrayDirectoryFx() {
  remove_dir(temp_dir_, ctx_, vfs_t_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_t_);
}

TEST_CASE_METHOD(
    ArrayDirectoryFx,
    "ArrayDirectory: Basic tests",
    "[ArrayDirectory][basic]") {
  ArrayDirectory array_dir;
}
