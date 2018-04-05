/**
 * @file   tiledb_cpp_api_vfs.cc
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "vfs.h"

namespace tiledb {

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
    , config_(config)
    , deleter_(ctx) {
  create_vfs(config.ptr().get());
}

/* ********************************* */
/*                API                */
/* ********************************* */

void VFS::create_bucket(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_create_bucket(ctx, vfs_.get(), uri.c_str()));
}

void VFS::remove_bucket(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_remove_bucket(ctx, vfs_.get(), uri.c_str()));
}

bool VFS::is_bucket(const std::string& uri) const {
  auto& ctx = ctx_.get();
  int ret;
  ctx.handle_error(tiledb_vfs_is_bucket(ctx, vfs_.get(), uri.c_str(), &ret));
  return (bool)ret;
}

void VFS::empty_bucket(const std::string& bucket) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_empty_bucket(ctx, vfs_.get(), bucket.c_str()));
}

bool VFS::is_empty_bucket(const std::string& bucket) const {
  auto& ctx = ctx_.get();
  int empty;
  ctx.handle_error(
      tiledb_vfs_is_empty_bucket(ctx, vfs_.get(), bucket.c_str(), &empty));
  return empty == 0;
}

void VFS::create_dir(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_create_dir(ctx, vfs_.get(), uri.c_str()));
}

bool VFS::is_dir(const std::string& uri) const {
  auto& ctx = ctx_.get();
  int ret;
  ctx.handle_error(tiledb_vfs_is_dir(ctx, vfs_.get(), uri.c_str(), &ret));
  return (bool)ret;
}

void VFS::remove_dir(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_remove_dir(ctx, vfs_.get(), uri.c_str()));
}

bool VFS::is_file(const std::string& uri) const {
  auto& ctx = ctx_.get();
  int ret;
  ctx.handle_error(tiledb_vfs_is_file(ctx, vfs_.get(), uri.c_str(), &ret));
  return (bool)ret;
}

void VFS::remove_file(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_remove_file(ctx, vfs_.get(), uri.c_str()));
}

uint64_t VFS::file_size(const std::string& uri) const {
  uint64_t ret;
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_file_size(ctx, vfs_.get(), uri.c_str(), &ret));
  return ret;
}

void VFS::move_file(
    const std::string& old_uri, const std::string& new_uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_vfs_move_file(ctx, vfs_.get(), old_uri.c_str(), new_uri.c_str()));
}

void VFS::move_dir(
    const std::string& old_uri, const std::string& new_uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_vfs_move_dir(ctx, vfs_.get(), old_uri.c_str(), new_uri.c_str()));
}

void VFS::touch(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_touch(ctx, vfs_.get(), uri.c_str()));
}

const Context& VFS::context() const {
  return ctx_.get();
}

std::shared_ptr<tiledb_vfs_t> VFS::ptr() const {
  return vfs_;
}

Config VFS::config() const {
  return config_;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

void VFS::create_vfs(tiledb_config_t* config) {
  tiledb_vfs_t* vfs;
  int rc = tiledb_vfs_create(ctx_.get(), &vfs, config);
  if (rc != TILEDB_OK)
    throw std::runtime_error(
        "[TileDB::C++API] Error: Failed to create VFS object");

  vfs_ = std::shared_ptr<tiledb_vfs_t>(vfs, deleter_);
}

}  // namespace tiledb
