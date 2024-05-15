/**
 * @file tiledb/api/c_api_test_support/testsupport_capi_vfs.h
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
 * This file defines test support classes for the vfs section of the C API.
 */

#ifndef TILEDB_TESTSUPPORT_CAPI_VFS_H
#define TILEDB_TESTSUPPORT_CAPI_VFS_H

#include "tiledb/api/c_api/vfs/vfs_api_external.h"
#include "tiledb/api/c_api/vfs/vfs_api_internal.h"

namespace tiledb::api::test_support {

struct ordinary_vfs {
  tiledb_ctx_handle_t* ctx{nullptr};
  tiledb_vfs_handle_t* vfs{nullptr};
  ordinary_vfs(tiledb_config_t* config = nullptr) {
    auto rc = tiledb_ctx_alloc(nullptr, &ctx);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test context");
    }
    rc = tiledb_vfs_alloc(ctx, config, &vfs);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test vfs");
    }
    if (vfs == nullptr) {
      throw std::logic_error("tiledb_vfs_alloc returned OK but without vfs");
    }
  }
  ~ordinary_vfs() {
    tiledb_vfs_free(&vfs);
    tiledb_ctx_free(&ctx);
  }
};

struct ordinary_vfs_fh {
  ordinary_vfs vfs{ordinary_vfs()};
  tiledb_ctx_handle_t* ctx{vfs.ctx};  // allow access to ordinary_vfs's ctx
  tiledb_vfs_fh_handle_t* vfs_fh{nullptr};
  ordinary_vfs_fh() {
    auto rc = tiledb_vfs_open(
        vfs.ctx, vfs.vfs, "test.txt", TILEDB_VFS_WRITE, &vfs_fh);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test vfs file handle");
    }
    if (vfs_fh == nullptr) {
      throw std::logic_error(
          "tiledb_vfs_alloc returned OK but without vfs file handle");
    }
  }
  ~ordinary_vfs_fh() {
    tiledb_vfs_fh_free(&vfs_fh);
  }
};

}  // namespace tiledb::api::test_support
#endif  // TILEDB_TESTSUPPORT_CAPI_VFS_H
