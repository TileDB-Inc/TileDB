/**
 * @file tiledb/api/c_api/vfs/vfs_api.cc
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
 * This file defines C API functions for the virtual filesystem section.
 */

#include "tiledb/api/c_api_support/c_api_support.h"
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
  auto stats = ctx->storage_manager()->stats();
  auto compute_tp = ctx->storage_manager()->compute_tp();
  auto io_tp = ctx->storage_manager()->io_tp();
  auto ctx_config = ctx->storage_manager()->config();
  if (config) {
    ctx_config.inherit((config->config()));
  }
  *vfs = tiledb_vfs_t::make_handle(stats, compute_tp, io_tp, ctx_config);

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
  throw_if_not_ok(vfs->create_bucket(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_remove_bucket(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->remove_bucket(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_empty_bucket(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->empty_bucket(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_empty_bucket(
    tiledb_vfs_t* vfs, const char* uri, int32_t* is_empty) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(is_empty);

  bool b;
  throw_if_not_ok(vfs->is_empty_bucket(tiledb::sm::URI(uri), &b));
  *is_empty = (int32_t)b;

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_bucket(
    tiledb_vfs_t* vfs, const char* uri, int32_t* is_bucket) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(is_bucket);

  bool exists;
  throw_if_not_ok(vfs->is_bucket(tiledb::sm::URI(uri), &exists));
  *is_bucket = (int32_t)exists;

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_create_dir(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->create_dir(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_dir(
    tiledb_vfs_t* vfs, const char* uri, int32_t* is_dir) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(is_dir);

  bool exists;
  throw_if_not_ok(vfs->is_dir(tiledb::sm::URI(uri), &exists));
  *is_dir = (int32_t)exists;

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_remove_dir(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->remove_dir(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_file(
    tiledb_vfs_t* vfs, const char* uri, int32_t* is_file) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(is_file);

  bool exists;
  throw_if_not_ok(vfs->is_file(tiledb::sm::URI(uri), &exists));
  *is_file = (int32_t)exists;

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_remove_file(tiledb_vfs_t* vfs, const char* uri) {
  ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->remove_file(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_dir_size(
    tiledb_vfs_t* vfs, const char* uri, uint64_t* size) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(size);
  throw_if_not_ok(vfs->dir_size(tiledb::sm::URI(uri), size));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_file_size(
    tiledb_vfs_t* vfs, const char* uri, uint64_t* size) {
  ensure_vfs_is_valid(vfs);
  ensure_output_pointer_is_valid(size);
  throw_if_not_ok(vfs->file_size(tiledb::sm::URI(uri), size));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_move_file(
    tiledb_vfs_t* vfs, const char* old_uri, const char* new_uri) {
  ensure_vfs_is_valid(vfs);
  throw_if_not_ok(
      vfs->move_file(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_move_dir(
    tiledb_vfs_t* vfs, const char* old_uri, const char* new_uri) {
  ensure_vfs_is_valid(vfs);
  throw_if_not_ok(
      vfs->move_dir(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_copy_file(
    tiledb_vfs_t* vfs, const char* old_uri, const char* new_uri) {
  ensure_vfs_is_valid(vfs);
  throw_if_not_ok(
      vfs->copy_file(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_copy_dir(
    tiledb_vfs_t* vfs, const char* old_uri, const char* new_uri) {
  ensure_vfs_is_valid(vfs);
  throw_if_not_ok(
      vfs->copy_dir(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri)));
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

capi_return_t tiledb_vfs_ls_recursive(
    tiledb_vfs_t* vfs,
    const char* path,
    int32_t (*callback)(const char*, void*, void*),
    void* data,
    void* data_offsets,
    int64_t max_paths) {
  ensure_output_pointer_is_valid(data);
  ensure_output_pointer_is_valid(data_offsets);
  std::vector<tiledb::sm::URI> children;
  vfs->ls_recursive(tiledb::sm::URI(path), &children, max_paths);

  // Apply first callback with empty path to set first offset of 0.
  int rc = callback("", data, data_offsets);
  while (rc == 1 && !children.empty()) {
    rc = callback(children.back().c_str(), data, data_offsets);
    children.pop_back();
  }

  if (rc == -1) {
    return TILEDB_ERR;
  }
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
  throw_if_not_ok(vfs->touch(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_plain;

capi_return_t tiledb_vfs_mode_to_str(
    tiledb_vfs_mode_t vfs_mode, const char** str) noexcept {
  return api_entry_plain<tiledb::api::tiledb_vfs_mode_to_str>(vfs_mode, str);
}

capi_return_t tiledb_vfs_mode_from_str(
    const char* str, tiledb_vfs_mode_t* vfs_mode) noexcept {
  return api_entry_plain<tiledb::api::tiledb_vfs_mode_from_str>(str, vfs_mode);
}

capi_return_t tiledb_vfs_alloc(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_vfs_t** vfs) noexcept {
  return tiledb::api::api_entry_with_context<tiledb::api::tiledb_vfs_alloc>(
      ctx, config, vfs);
}

void tiledb_vfs_free(tiledb_vfs_t** vfs) noexcept {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_vfs_free>(vfs);
}

capi_return_t tiledb_vfs_get_config(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, tiledb_config_t** config) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_get_config>(
      ctx, vfs, config);
}

capi_return_t tiledb_vfs_create_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_create_bucket>(
      ctx, vfs, uri);
}

capi_return_t tiledb_vfs_remove_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_remove_bucket>(
      ctx, vfs, uri);
}

capi_return_t tiledb_vfs_empty_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_empty_bucket>(ctx, vfs, uri);
}

capi_return_t tiledb_vfs_is_empty_bucket(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_empty) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_is_empty_bucket>(
      ctx, vfs, uri, is_empty);
}

capi_return_t tiledb_vfs_is_bucket(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_bucket) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_is_bucket>(
      ctx, vfs, uri, is_bucket);
}

capi_return_t tiledb_vfs_create_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_create_dir>(ctx, vfs, uri);
}

capi_return_t tiledb_vfs_is_dir(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_dir) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_is_dir>(
      ctx, vfs, uri, is_dir);
}

capi_return_t tiledb_vfs_remove_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_remove_dir>(ctx, vfs, uri);
}

capi_return_t tiledb_vfs_is_file(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_file) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_is_file>(
      ctx, vfs, uri, is_file);
}

capi_return_t tiledb_vfs_remove_file(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_remove_file>(ctx, vfs, uri);
}

capi_return_t tiledb_vfs_dir_size(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    uint64_t* size) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_dir_size>(
      ctx, vfs, uri, size);
}

capi_return_t tiledb_vfs_file_size(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    uint64_t* size) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_file_size>(
      ctx, vfs, uri, size);
}

capi_return_t tiledb_vfs_move_file(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_move_file>(
      ctx, vfs, old_uri, new_uri);
}

capi_return_t tiledb_vfs_move_dir(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_move_dir>(
      ctx, vfs, old_uri, new_uri);
}

capi_return_t tiledb_vfs_copy_file(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_copy_file>(
      ctx, vfs, old_uri, new_uri);
}

capi_return_t tiledb_vfs_copy_dir(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_copy_dir>(
      ctx, vfs, old_uri, new_uri);
}

capi_return_t tiledb_vfs_open(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    tiledb_vfs_mode_t mode,
    tiledb_vfs_fh_t** fh) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_open>(
      ctx, vfs, uri, mode, fh);
}

capi_return_t tiledb_vfs_close(
    tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_close>(ctx, fh);
}

capi_return_t tiledb_vfs_read(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_read>(
      ctx, fh, offset, buffer, nbytes);
}

capi_return_t tiledb_vfs_write(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    const void* buffer,
    uint64_t nbytes) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_write>(
      ctx, fh, buffer, nbytes);
}

capi_return_t tiledb_vfs_sync(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_sync>(ctx, fh);
}

capi_return_t tiledb_vfs_ls(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    int32_t (*callback)(const char*, void*),
    void* data) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_ls>(
      ctx, vfs, path, callback, data);
}

capi_return_t tiledb_vfs_ls_recursive(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    int32_t (*callback)(const char*, void*, void*),
    void* data,
    void* data_offsets,
    int64_t max_paths) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_ls_recursive>(
      ctx, vfs, path, callback, data, data_offsets, max_paths);
}

void tiledb_vfs_fh_free(tiledb_vfs_fh_t** fh) noexcept {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_vfs_fh_free>(fh);
}

capi_return_t tiledb_vfs_fh_is_closed(
    tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh, int32_t* is_closed) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_fh_is_closed>(
      ctx, fh, is_closed);
}

capi_return_t tiledb_vfs_touch(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<tiledb::api::tiledb_vfs_touch>(ctx, vfs, uri);
}
