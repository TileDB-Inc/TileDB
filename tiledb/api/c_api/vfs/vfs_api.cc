/**
 * @file tiledb/api/c_api/vfs/vfs_api.cc
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
 * This file defines C API functions for the virtual filesystem section.
 */

#include "tiledb/api/c_api_support/c_api_support.h"
#include "vfs_api_experimental.h"
#include "vfs_api_internal.h"

namespace tiledb::api {

capi_return_t tiledb_vfs_mode_to_str(
    tiledb_vfs_mode_t vfs_mode, const char** str) {
  const auto& strval = tiledb::sm::vfsmode_str((tiledb::sm::VFSMode)vfs_mode);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_vfs_mode_from_str(
    const char* str, tiledb_vfs_mode_t* vfs_mode) {
  tiledb::sm::VFSMode val = tiledb::sm::VFSMode::VFS_READ;
  if (!tiledb::sm::vfsmode_enum(str, &val).ok())
    return TILEDB_ERR;
  *vfs_mode = (tiledb_vfs_mode_t)val;
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_alloc(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_vfs_t** vfs) {
  ensure_output_pointer_is_valid(vfs);

  // Create VFS object
  auto& resources{ctx->resources()};
  auto logger{resources.logger().get()};
  auto& stats{resources.stats()};
  auto& compute_tp{resources.compute_tp()};
  auto& io_tp{resources.io_tp()};
  auto& ctx_config{resources.config()};
  if (config) {
    ctx_config.inherit((config->config()));
  }
  *vfs = tiledb_vfs_t::make_handle(
      &stats, logger, &compute_tp, &io_tp, ctx_config);

  return TILEDB_OK;
}

void tiledb_vfs_free(tiledb_vfs_t** vfs) {
  ensure_output_pointer_is_valid(vfs);
  ensure_vfs_is_valid(*vfs);
  tiledb_vfs_t::break_handle(*vfs);
}

capi_return_t tiledb_vfs_get_config(
    tiledb_vfs_t* vfs, tiledb_config_t** config) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(config);

  *config = tiledb_config_handle_t::make_handle(vfs->config());
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_create_bucket(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  vfs->create_bucket(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_remove_bucket(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  vfs->remove_bucket(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_empty_bucket(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  vfs->empty_bucket(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_empty_bucket(
    tiledb_vfs_t* vfs, const char* uri, int32_t* is_empty) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(is_empty);
  *is_empty = (int32_t)vfs->is_empty_bucket(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_bucket(
    tiledb_vfs_t* vfs, const char* uri, int32_t* is_bucket) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(is_bucket);
  *is_bucket = (int32_t)vfs->is_bucket(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_create_dir(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  vfs->create_dir(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_dir(
    tiledb_vfs_t* vfs, const char* uri, int32_t* is_dir) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(is_dir);
  *is_dir = (int32_t)vfs->is_dir(tiledb::sm::URI(uri));

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_remove_dir(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  vfs->remove_dir(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_file(
    tiledb_vfs_t* vfs, const char* uri, int32_t* is_file) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(is_file);
  *is_file = (int32_t)vfs->is_file(tiledb::sm::URI(uri));

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_remove_file(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  vfs->remove_file(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_dir_size(
    tiledb_vfs_t* vfs, const char* uri, uint64_t* size) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(size);
  *size = vfs->dir_size(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_file_size(
    tiledb_vfs_t* vfs, const char* uri, uint64_t* size) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(size);
  *size = vfs->file_size(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_move_file(
    tiledb_vfs_t* vfs, const char* old_uri, const char* new_uri) {
  ensure_vfs_is_valid(vfs);
  vfs->move_file(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_move_dir(
    tiledb_vfs_t* vfs, const char* old_uri, const char* new_uri) {
  ensure_vfs_is_valid(vfs);
  vfs->move_dir(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_copy_file(
    tiledb_vfs_t* vfs, const char* old_uri, const char* new_uri) {
  ensure_vfs_is_valid(vfs);
  vfs->copy_file(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_copy_dir(
    tiledb_vfs_t* vfs, const char* old_uri, const char* new_uri) {
  ensure_vfs_is_valid(vfs);
  vfs->copy_dir(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_open(
    tiledb_vfs_t* vfs,
    const char* uri,
    tiledb_vfs_mode_t mode,
    tiledb_vfs_fh_t** fh) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(fh);

  // Create VFS file handle
  auto fh_uri = tiledb::sm::URI(uri);
  if (fh_uri.is_invalid()) {
    throw CAPIStatusException(std::string("Invalid TileDB object: ") + "uri");
  }
  auto vfs_mode = static_cast<tiledb::sm::VFSMode>(mode);

  // Throws if opening the uri is unsuccessful
  *fh = tiledb_vfs_fh_t::make_handle(fh_uri, vfs->vfs(), vfs_mode);

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_close(tiledb_vfs_fh_t* fh) {
  ensure_output_pointer_is_valid(fh);
  ensure_vfs_fh_is_valid(fh);
  throw_if_not_ok(fh->close());
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_read(
    tiledb_vfs_fh_t* fh, uint64_t offset, void* buffer, uint64_t nbytes) {
  ensure_vfs_fh_is_valid(fh);
  ensure_output_pointer_is_valid(buffer);
  throw_if_not_ok(fh->read(offset, buffer, nbytes));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_write(
    tiledb_vfs_fh_t* fh, const void* buffer, uint64_t nbytes) {
  ensure_vfs_fh_is_valid(fh);
  throw_if_not_ok(fh->write(buffer, nbytes));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_sync(tiledb_vfs_fh_t* fh) {
  ensure_vfs_fh_is_valid(fh);
  throw_if_not_ok(fh->sync());
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_ls(
    tiledb_vfs_t* vfs,
    const char* path,
    int32_t (*callback)(const char*, void*),
    void* data) {
  // Sanity checks
  if (callback == nullptr) {
    throw CAPIStatusException(
        std::string("Invalid TileDB object: ") + "callback function");
  }

  // Get children
  std::vector<tiledb::sm::URI> children;
  throw_if_not_ok(vfs->ls(tiledb::sm::URI(path), &children));

  // Apply the callback to every child
  int rc = 1;
  for (const auto& uri : children) {
    rc = callback(uri.to_string().c_str(), data);
    if (rc != 1)
      break;
  }

  if (rc == -1)
    return TILEDB_ERR;
  return TILEDB_OK;
}

void tiledb_vfs_fh_free(tiledb_vfs_fh_t** fh) {
  ensure_output_pointer_is_valid(fh);
  ensure_vfs_fh_is_valid(*fh);
  tiledb_vfs_fh_t::break_handle(*fh);
}

capi_return_t tiledb_vfs_fh_is_closed(tiledb_vfs_fh_t* fh, int32_t* is_closed) {
  ensure_vfs_fh_is_valid(fh);
  ensure_output_pointer_is_valid(is_closed);
  *is_closed = !fh->is_open();
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_touch(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  vfs->touch(tiledb::sm::URI(uri));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_ls_recursive(
    tiledb_vfs_t* vfs,
    const char* path,
    tiledb_ls_callback_t callback,
    void* data) {
  ensure_vfs_is_valid(vfs);
  if (path == nullptr) {
    throw CAPIStatusException("Invalid TileDB object: VFS passed a null path.");
  } else if (callback == nullptr) {
    throw CAPIStatusException(
        "Invalid TileDB object: Callback function is null.");
  }
  ensure_output_pointer_is_valid(data);
  vfs->ls_recursive(tiledb::sm::URI(path), callback, data);
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_plain;

CAPI_INTERFACE(vfs_mode_to_str, tiledb_vfs_mode_t vfs_mode, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_vfs_mode_to_str>(vfs_mode, str);
}

CAPI_INTERFACE(
    vfs_mode_from_str, const char* str, tiledb_vfs_mode_t* vfs_mode) {
  return api_entry_plain<tiledb::api::tiledb_vfs_mode_from_str>(str, vfs_mode);
}

CAPI_INTERFACE(
    vfs_alloc, tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_vfs_t** vfs) {
  return tiledb::api::api_entry_with_context<tiledb::api::tiledb_vfs_alloc>(
      ctx, config, vfs);
}

CAPI_INTERFACE_VOID(vfs_free, tiledb_vfs_t** vfs) {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_vfs_free>(vfs);
}

CAPI_INTERFACE(
    vfs_get_config,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    tiledb_config_t** config) {
  return api_entry_context<tiledb::api::tiledb_vfs_get_config>(
      ctx, vfs, config);
}

CAPI_INTERFACE(
    vfs_create_bucket, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_create_bucket>(
      ctx, vfs, uri);
}

CAPI_INTERFACE(
    vfs_remove_bucket, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_remove_bucket>(
      ctx, vfs, uri);
}

CAPI_INTERFACE(
    vfs_empty_bucket, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_empty_bucket>(ctx, vfs, uri);
}

CAPI_INTERFACE(
    vfs_is_empty_bucket,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_empty) {
  return api_entry_context<tiledb::api::tiledb_vfs_is_empty_bucket>(
      ctx, vfs, uri, is_empty);
}

CAPI_INTERFACE(
    vfs_is_bucket,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_bucket) {
  return api_entry_context<tiledb::api::tiledb_vfs_is_bucket>(
      ctx, vfs, uri, is_bucket);
}

CAPI_INTERFACE(
    vfs_create_dir, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_create_dir>(ctx, vfs, uri);
}

CAPI_INTERFACE(
    vfs_is_dir,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_dir) {
  return api_entry_context<tiledb::api::tiledb_vfs_is_dir>(
      ctx, vfs, uri, is_dir);
}

CAPI_INTERFACE(
    vfs_remove_dir, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_remove_dir>(ctx, vfs, uri);
}

CAPI_INTERFACE(
    vfs_is_file,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_file) {
  return api_entry_context<tiledb::api::tiledb_vfs_is_file>(
      ctx, vfs, uri, is_file);
}

CAPI_INTERFACE(
    vfs_remove_file, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_remove_file>(ctx, vfs, uri);
}

CAPI_INTERFACE(
    vfs_dir_size,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    uint64_t* size) {
  return api_entry_context<tiledb::api::tiledb_vfs_dir_size>(
      ctx, vfs, uri, size);
}

CAPI_INTERFACE(
    vfs_file_size,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    uint64_t* size) {
  return api_entry_context<tiledb::api::tiledb_vfs_file_size>(
      ctx, vfs, uri, size);
}

CAPI_INTERFACE(
    vfs_move_file,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_move_file>(
      ctx, vfs, old_uri, new_uri);
}

CAPI_INTERFACE(
    vfs_move_dir,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_move_dir>(
      ctx, vfs, old_uri, new_uri);
}

CAPI_INTERFACE(
    vfs_copy_file,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_copy_file>(
      ctx, vfs, old_uri, new_uri);
}

CAPI_INTERFACE(
    vfs_copy_dir,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_copy_dir>(
      ctx, vfs, old_uri, new_uri);
}

CAPI_INTERFACE(
    vfs_open,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    tiledb_vfs_mode_t mode,
    tiledb_vfs_fh_t** fh) {
  return api_entry_context<tiledb::api::tiledb_vfs_open>(
      ctx, vfs, uri, mode, fh);
}

CAPI_INTERFACE(vfs_close, tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) {
  return api_entry_context<tiledb::api::tiledb_vfs_close>(ctx, fh);
}

CAPI_INTERFACE(
    vfs_read,
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes) {
  return api_entry_context<tiledb::api::tiledb_vfs_read>(
      ctx, fh, offset, buffer, nbytes);
}

CAPI_INTERFACE(
    vfs_write,
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    const void* buffer,
    uint64_t nbytes) {
  return api_entry_context<tiledb::api::tiledb_vfs_write>(
      ctx, fh, buffer, nbytes);
}

CAPI_INTERFACE(vfs_sync, tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) {
  return api_entry_context<tiledb::api::tiledb_vfs_sync>(ctx, fh);
}

CAPI_INTERFACE(
    vfs_ls,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    int32_t (*callback)(const char*, void*),
    void* data) {
  return api_entry_context<tiledb::api::tiledb_vfs_ls>(
      ctx, vfs, path, callback, data);
}

CAPI_INTERFACE_VOID(vfs_fh_free, tiledb_vfs_fh_t** fh) {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_vfs_fh_free>(fh);
}

CAPI_INTERFACE(
    vfs_fh_is_closed,
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    int32_t* is_closed) {
  return api_entry_context<tiledb::api::tiledb_vfs_fh_is_closed>(
      ctx, fh, is_closed);
}

CAPI_INTERFACE(
    vfs_touch, tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  return api_entry_context<tiledb::api::tiledb_vfs_touch>(ctx, vfs, uri);
}

CAPI_INTERFACE(
    vfs_ls_recursive,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    tiledb_ls_callback_t callback,
    void* data) {
  return api_entry_context<tiledb::api::tiledb_vfs_ls_recursive>(
      ctx, vfs, path, callback, data);
}
