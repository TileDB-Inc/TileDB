/**
 * @file tiledb/api/c_api/vfs/vfs_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2025 TileDB, Inc.
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
 * This file declares the internals of the vfs section of the C API.
 */

#ifndef TILEDB_CAPI_VFS_INTERNAL_H
#define TILEDB_CAPI_VFS_INTERNAL_H

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filesystem/vfs_file_handle.h"
#include "vfs_api_experimental.h"
#include "vfs_api_external.h"

/** Handle `struct` for API VFS objects. */
struct tiledb_vfs_handle_t
    : public tiledb::api::CAPIHandle<tiledb_vfs_handle_t> {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"vfs"};

 private:
  using vfs_type = tiledb::sm::VFS;
  vfs_type vfs_;

 public:
  explicit tiledb_vfs_handle_t(
      tiledb::sm::stats::Stats* parent_stats,
      Logger* logger,
      ThreadPool* compute_tp,
      ThreadPool* io_tp,
      const tiledb::sm::Config& config)
      : vfs_{parent_stats, logger, compute_tp, io_tp, config} {
  }

  vfs_type* vfs() {
    return &vfs_;
  }

  tiledb::sm::Config config() const {
    return vfs_.config();
  }

  void create_bucket(const tiledb::sm::URI& uri) const {
    vfs_.create_bucket(uri);
  }

  void remove_bucket(const tiledb::sm::URI& uri) const {
    vfs_.remove_bucket(uri);
  }

  void empty_bucket(const tiledb::sm::URI& uri) const {
    vfs_.empty_bucket(uri);
  }

  bool is_empty_bucket(const tiledb::sm::URI& uri) const {
    return vfs_.is_empty_bucket(uri);
  }

  bool is_bucket(const tiledb::sm::URI& uri) const {
    return vfs_.is_bucket(uri);
  }

  void create_dir(const tiledb::sm::URI& uri) const {
    vfs_.create_dir(uri);
  }

  bool is_dir(const tiledb::sm::URI& uri) const {
    return vfs_.is_dir(uri);
  }

  void remove_dir(const tiledb::sm::URI& uri) const {
    vfs_.remove_dir(uri);
  }

  bool is_file(const tiledb::sm::URI& uri) const {
    return vfs_.is_file(uri);
  }

  void remove_file(const tiledb::sm::URI& uri) const {
    vfs_.remove_file(uri);
  }

  uint64_t dir_size(const tiledb::sm::URI& dir_name) const {
    return vfs_.dir_size(dir_name);
  }

  uint64_t file_size(const tiledb::sm::URI& uri) const {
    return vfs_.file_size(uri);
  }

  void move_file(
      const tiledb::sm::URI& old_uri, const tiledb::sm::URI& new_uri) {
    vfs_.move_file(old_uri, new_uri);
  }

  void move_dir(
      const tiledb::sm::URI& old_uri, const tiledb::sm::URI& new_uri) {
    vfs_.move_dir(old_uri, new_uri);
  }

  void copy_file(
      const tiledb::sm::URI& old_uri, const tiledb::sm::URI& new_uri) {
    vfs_.copy_file(old_uri, new_uri);
  }

  void copy_dir(
      const tiledb::sm::URI& old_uri, const tiledb::sm::URI& new_uri) {
    vfs_.copy_dir(old_uri, new_uri);
  }

  Status ls(
      const tiledb::sm::URI& parent, std::vector<tiledb::sm::URI>* uris) const {
    return vfs_.ls(parent, uris);
  }

  void touch(const tiledb::sm::URI& uri) const {
    vfs_.touch(uri);
  }

  void ls_recursive(
      const tiledb::sm::URI& parent,
      tiledb_ls_callback_t cb,
      void* data) const {
    tiledb::sm::CallbackWrapperCAPI wrapper(cb, data);
    vfs_.ls_recursive(parent, wrapper);
  }
};

/** Handle `struct` for API VFS file handle objects. */
struct tiledb_vfs_fh_handle_t
    : public tiledb::api::CAPIHandle<tiledb_vfs_fh_handle_t> {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"vfs file handle"};

 private:
  using vfs_fh_type = tiledb::sm::VFSFileHandle;
  vfs_fh_type vfs_fh_;

 public:
  explicit tiledb_vfs_fh_handle_t(
      const tiledb::sm::URI& uri,
      tiledb::sm::VFS* vfs,
      tiledb::sm::VFSMode mode)
      : vfs_fh_{uri, vfs, mode} {
    throw_if_not_ok(vfs_fh_.open());
  }

  Status open() {
    return vfs_fh_.open();
  }

  Status close() {
    return vfs_fh_.close();
  }

  Status read(uint64_t offset, void* buffer, uint64_t nbytes) {
    return vfs_fh_.read(offset, buffer, nbytes);
  }

  Status write(const void* buffer, uint64_t nbytes) {
    return vfs_fh_.write(buffer, nbytes);
  }

  Status sync() {
    return vfs_fh_.sync();
  }

  bool is_open() const {
    return vfs_fh_.is_open();
  }
};

namespace tiledb::api {
/**
 * Returns after successfully validating a VFS.
 *
 * @param vfs Possibly-valid pointer to a virtual filesystem
 */
inline void ensure_vfs_is_valid(const tiledb_vfs_t* vfs) {
  ensure_handle_is_valid(vfs);
}

/**
 * Returns after successfully validating a VFS file handle.
 *
 * @param vfs_fh Possibly-valid pointer to a virtual filesystem file handle
 */
inline void ensure_vfs_fh_is_valid(const tiledb_vfs_fh_t* vfs_fh) {
  ensure_handle_is_valid(vfs_fh);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_VFS_INTERNAL_H
