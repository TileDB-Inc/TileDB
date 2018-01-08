/**
 * @file   tiledb_cpp_api_vfs.cc
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines the C++ API for the TileDB Config object.
 */

#include "tiledb_cpp_api_vfs.h"

namespace tdb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS(const Context& ctx)
    : ctx_(ctx)
    , deleter_(ctx) {
  create_vfs(nullptr);
}

VFS::VFS(const Context& ctx, const Config& config)
    : ctx_(ctx)
    , deleter_(ctx) {
  create_vfs(config.ptr());
}

/* ********************************* */
/*                API                */
/* ********************************* */

void VFS::create_bucket(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_vfs_create_bucket(ctx.ptr(), vfs_.get(), uri.c_str()));
}

void VFS::remove_bucket(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_vfs_remove_bucket(ctx.ptr(), vfs_.get(), uri.c_str()));
}

bool VFS::is_bucket(const std::string& uri) const {
  auto& ctx = ctx_.get();
  int ret;
  ctx.handle_error(
      tiledb_vfs_is_bucket(ctx.ptr(), vfs_.get(), uri.c_str(), &ret));
  return (bool)ret;
}

void VFS::create_dir(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_create_dir(ctx.ptr(), vfs_.get(), uri.c_str()));
}

bool VFS::is_dir(const std::string& uri) const {
  auto& ctx = ctx_.get();
  int ret;
  ctx.handle_error(tiledb_vfs_is_dir(ctx.ptr(), vfs_.get(), uri.c_str(), &ret));
  return (bool)ret;
}

void VFS::remove_dir(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_remove_dir(ctx.ptr(), vfs_.get(), uri.c_str()));
}

bool VFS::is_file(const std::string& uri) const {
  auto& ctx = ctx_.get();
  int ret;
  ctx.handle_error(
      tiledb_vfs_is_file(ctx.ptr(), vfs_.get(), uri.c_str(), &ret));
  return (bool)ret;
}

void VFS::remove_file(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_remove_file(ctx.ptr(), vfs_.get(), uri.c_str()));
}

uint64_t VFS::file_size(const std::string& uri) const {
  uint64_t ret;
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_vfs_file_size(ctx.ptr(), vfs_.get(), uri.c_str(), &ret));
  return ret;
}

void VFS::move(const std::string& old_uri, const std::string& new_uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_vfs_move(ctx.ptr(), vfs_.get(), old_uri.c_str(), new_uri.c_str()));
}

void VFS::read(
    const std::string& uri,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_read(
      ctx.ptr(), vfs_.get(), uri.c_str(), offset, buffer, nbytes));
}

void VFS::write(
    const std::string& uri, const void* buffer, uint64_t nbytes) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_vfs_write(ctx.ptr(), vfs_.get(), uri.c_str(), buffer, nbytes));
}

void VFS::sync(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_sync(ctx.ptr(), vfs_.get(), uri.c_str()));
}

bool VFS::supports_fs(tiledb_filesystem_t fs) const {
  auto& ctx = ctx_.get();
  int ret;
  ctx.handle_error(tiledb_vfs_supports_fs(ctx.ptr(), vfs_.get(), fs, &ret));
  return (bool)ret;
}

void VFS::touch(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_touch(ctx.ptr(), vfs_.get(), uri.c_str()));
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

void VFS::create_vfs(tiledb_config_t* config) {
  tiledb_vfs_t* vfs;
  int rc = tiledb_vfs_create(ctx_.get().ptr(), &vfs, config);
  if (rc != TILEDB_OK)
    throw std::runtime_error(
        "[TileDB::C++API] Error: Failed to create VFS object");

  vfs_ = std::shared_ptr<tiledb_vfs_t>(vfs, deleter_);
}

}  // namespace tdb
