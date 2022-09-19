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

#include "tiledb/sm/c_api/api_exception_safety.h"
#include "vfs_api_external.h"
#include "vfs_api_internal.h"

/* ****************************** */
/*        VIRTUAL FILESYSTEM      */
/* ****************************** */

namespace tiledb::common::detail {

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
  api::ensure_output_pointer_is_valid(vfs);
  if (config != nullptr && config->config_ == nullptr) {
    api::action_invalid_object("config");
  }

  // Create VFS object
  auto stats = ctx->storage_manager()->stats();
  auto compute_tp = ctx->storage_manager()->compute_tp();
  auto io_tp = ctx->storage_manager()->io_tp();
  auto ctx_config = ctx->storage_manager()->config();
  if (config) {
    ctx_config.inherit(*(config->config_));
  }
  *vfs = tiledb_vfs_t::make_handle(stats, compute_tp, io_tp, ctx_config);

  return TILEDB_OK;
}

void tiledb_vfs_free(tiledb_vfs_t** vfs) {
  api::ensure_output_pointer_is_valid(vfs);
  api::ensure_vfs_is_valid(*vfs);
  tiledb_vfs_t::break_handle(*vfs);
}

capi_return_t tiledb_vfs_get_config(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, tiledb_config_t** config) {
  api::ensure_vfs_is_valid(vfs);
  api::ensure_output_pointer_is_valid(config);

  // Create a new config struct
  *config = new (std::nothrow) tiledb_config_t;
  if (*config == nullptr)
    return TILEDB_OOM;

  // Create a new config
  (*config)->config_ = new (std::nothrow) tiledb::sm::Config();
  if ((*config)->config_ == nullptr) {
    delete (*config);
    *config = nullptr;
    return TILEDB_OOM;
  }
  *((*config)->config_) = vfs->config();

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_create_bucket(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->create_bucket(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_remove_bucket(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->remove_bucket(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_empty_bucket(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->empty_bucket(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_empty_bucket(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri, int32_t* is_empty) {
  api::ensure_vfs_is_valid(vfs);
  api::ensure_output_pointer_is_valid(is_empty);

  bool b;
  throw_if_not_ok(vfs->is_empty_bucket(tiledb::sm::URI(uri), &b));
  *is_empty = (int32_t)b;

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_bucket(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri, int32_t* is_bucket) {
  api::ensure_vfs_is_valid(vfs);
  api::ensure_output_pointer_is_valid(is_bucket);

  bool exists;
  throw_if_not_ok(vfs->is_bucket(tiledb::sm::URI(uri), &exists));
  *is_bucket = (int32_t)exists;

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_create_dir(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->create_dir(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_dir(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri, int32_t* is_dir) {
  api::ensure_vfs_is_valid(vfs);

  bool exists;
  throw_if_not_ok(vfs->is_dir(tiledb::sm::URI(uri), &exists));
  *is_dir = (int32_t)exists;

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_remove_dir(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->remove_dir(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_is_file(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri, int32_t* is_file) {
  api::ensure_vfs_is_valid(vfs);
  api::ensure_output_pointer_is_valid(is_file);

  bool exists;
  throw_if_not_ok(vfs->is_file(tiledb::sm::URI(uri), &exists));
  *is_file = (int32_t)exists;

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_remove_file(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->remove_file(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_dir_size(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri, uint64_t* size) {
  api::ensure_vfs_is_valid(vfs);
  api::ensure_output_pointer_is_valid(size);
  throw_if_not_ok(vfs->dir_size(tiledb::sm::URI(uri), size));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_file_size(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri, uint64_t* size) {
  api::ensure_vfs_is_valid(vfs);
  api::ensure_output_pointer_is_valid(size);
  throw_if_not_ok(vfs->file_size(tiledb::sm::URI(uri), size));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_move_file(
    tiledb_ctx_t*,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(
      vfs->move_file(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_move_dir(
    tiledb_ctx_t*,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(
      vfs->move_dir(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_copy_file(
    tiledb_ctx_t*,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(
      vfs->copy_file(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_copy_dir(
    tiledb_ctx_t*,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(
      vfs->copy_dir(tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri)));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_open(
    tiledb_ctx_t*,
    tiledb_vfs_t* vfs,
    const char* uri,
    tiledb_vfs_mode_t mode,
    tiledb_vfs_fh_t** fh) {
  api::ensure_vfs_is_valid(vfs);
  api::ensure_output_pointer_is_valid(fh);

  // Create VFS file handle
  auto fh_uri = tiledb::sm::URI(uri);
  if (fh_uri.is_invalid()) {
    api::action_invalid_object("uri");
  }
  auto vfs_mode = static_cast<tiledb::sm::VFSMode>(mode);
  *fh = tiledb_vfs_fh_t::make_handle(fh_uri, vfs->vfs(), vfs_mode);

  // Open VFS file
  auto st{(*fh)->open()};
  if (!st.ok()) {
    tiledb_vfs_fh_t::break_handle(*fh);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

capi_return_t tiledb_vfs_close(tiledb_ctx_t*, tiledb_vfs_fh_t* fh) {
  api::ensure_output_pointer_is_valid(fh);
  api::ensure_vfs_fh_is_valid(fh);
  throw_if_not_ok(fh->close());
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_read(
    tiledb_ctx_t*,
    tiledb_vfs_fh_t* fh,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes) {
  api::ensure_vfs_fh_is_valid(fh);
  api::ensure_output_pointer_is_valid(buffer);
  throw_if_not_ok(fh->read(offset, buffer, nbytes));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_write(
    tiledb_ctx_t*, tiledb_vfs_fh_t* fh, const void* buffer, uint64_t nbytes) {
  api::ensure_vfs_fh_is_valid(fh);
  throw_if_not_ok(fh->write(buffer, nbytes));
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_sync(tiledb_ctx_t*, tiledb_vfs_fh_t* fh) {
  api::ensure_vfs_fh_is_valid(fh);
  throw_if_not_ok(fh->sync());
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_ls(
    tiledb_ctx_t*,
    tiledb_vfs_t* vfs,
    const char* path,
    int32_t (*callback)(const char*, void*),
    void* data) {
  // Sanity checks
  if (callback == nullptr) {
    api::action_invalid_object("callback function");
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
  api::ensure_output_pointer_is_valid(fh);
  api::ensure_vfs_fh_is_valid(*fh);
  tiledb_vfs_fh_t::break_handle(*fh);
}

capi_return_t tiledb_vfs_fh_is_closed(
    tiledb_ctx_t*, tiledb_vfs_fh_t* fh, int32_t* is_closed) {
  api::ensure_vfs_fh_is_valid(fh);
  api::ensure_output_pointer_is_valid(is_closed);
  *is_closed = !fh->is_open();
  return TILEDB_OK;
}

capi_return_t tiledb_vfs_touch(
    tiledb_ctx_t*, tiledb_vfs_t* vfs, const char* uri) {
  api::ensure_vfs_is_valid(vfs);
  throw_if_not_ok(vfs->touch(tiledb::sm::URI(uri)));
  return TILEDB_OK;
}

}  // namespace tiledb::common::detail

/* ****************************** */
/*        VIRTUAL FILESYSTEM      */
/* ****************************** */

int32_t tiledb_vfs_mode_to_str(
    tiledb_vfs_mode_t vfs_mode, const char** str) noexcept {
  return api_entry<detail::tiledb_vfs_mode_to_str>(vfs_mode, str);
}

int32_t tiledb_vfs_mode_from_str(
    const char* str, tiledb_vfs_mode_t* vfs_mode) noexcept {
  return api_entry<detail::tiledb_vfs_mode_from_str>(str, vfs_mode);
}

int32_t tiledb_vfs_alloc(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_vfs_t** vfs) noexcept {
  return api_entry_context<detail::tiledb_vfs_alloc>(ctx, config, vfs);
}

void tiledb_vfs_free(tiledb_vfs_t** vfs) noexcept {
  return api_entry_void<detail::tiledb_vfs_free>(vfs);
}

int32_t tiledb_vfs_get_config(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, tiledb_config_t** config) noexcept {
  return api_entry_context<detail::tiledb_vfs_get_config>(ctx, vfs, config);
}

int32_t tiledb_vfs_create_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_create_bucket>(ctx, vfs, uri);
}

int32_t tiledb_vfs_remove_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_remove_bucket>(ctx, vfs, uri);
}

int32_t tiledb_vfs_empty_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_empty_bucket>(ctx, vfs, uri);
}

int32_t tiledb_vfs_is_empty_bucket(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_empty) noexcept {
  return api_entry_context<detail::tiledb_vfs_is_empty_bucket>(
      ctx, vfs, uri, is_empty);
}

int32_t tiledb_vfs_is_bucket(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_bucket) noexcept {
  return api_entry_context<detail::tiledb_vfs_is_bucket>(
      ctx, vfs, uri, is_bucket);
}

int32_t tiledb_vfs_create_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_create_dir>(ctx, vfs, uri);
}

int32_t tiledb_vfs_is_dir(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_dir) noexcept {
  return api_entry_context<detail::tiledb_vfs_is_dir>(ctx, vfs, uri, is_dir);
}

int32_t tiledb_vfs_remove_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_remove_dir>(ctx, vfs, uri);
}

int32_t tiledb_vfs_is_file(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    int32_t* is_file) noexcept {
  return api_entry_context<detail::tiledb_vfs_is_file>(ctx, vfs, uri, is_file);
}

int32_t tiledb_vfs_remove_file(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_remove_file>(ctx, vfs, uri);
}

int32_t tiledb_vfs_dir_size(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    uint64_t* size) noexcept {
  return api_entry_context<detail::tiledb_vfs_dir_size>(ctx, vfs, uri, size);
}

int32_t tiledb_vfs_file_size(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    uint64_t* size) noexcept {
  return api_entry_context<detail::tiledb_vfs_file_size>(ctx, vfs, uri, size);
}

int32_t tiledb_vfs_move_file(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_move_file>(
      ctx, vfs, old_uri, new_uri);
}

int32_t tiledb_vfs_move_dir(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_move_dir>(
      ctx, vfs, old_uri, new_uri);
}

int32_t tiledb_vfs_copy_file(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_copy_file>(
      ctx, vfs, old_uri, new_uri);
}

int32_t tiledb_vfs_copy_dir(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_copy_dir>(
      ctx, vfs, old_uri, new_uri);
}

int32_t tiledb_vfs_open(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    tiledb_vfs_mode_t mode,
    tiledb_vfs_fh_t** fh) noexcept {
  return api_entry_context<detail::tiledb_vfs_open>(ctx, vfs, uri, mode, fh);
}

int32_t tiledb_vfs_close(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) noexcept {
  return api_entry_context<detail::tiledb_vfs_close>(ctx, fh);
}

int32_t tiledb_vfs_read(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes) noexcept {
  return api_entry_context<detail::tiledb_vfs_read>(
      ctx, fh, offset, buffer, nbytes);
}

int32_t tiledb_vfs_write(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    const void* buffer,
    uint64_t nbytes) noexcept {
  return api_entry_context<detail::tiledb_vfs_write>(ctx, fh, buffer, nbytes);
}

int32_t tiledb_vfs_sync(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) noexcept {
  return api_entry_context<detail::tiledb_vfs_sync>(ctx, fh);
}

int32_t tiledb_vfs_ls(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    int32_t (*callback)(const char*, void*),
    void* data) noexcept {
  return api_entry_context<detail::tiledb_vfs_ls>(
      ctx, vfs, path, callback, data);
}

void tiledb_vfs_fh_free(tiledb_vfs_fh_t** fh) noexcept {
  return api_entry_void<detail::tiledb_vfs_fh_free>(fh);
}

int32_t tiledb_vfs_fh_is_closed(
    tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh, int32_t* is_closed) noexcept {
  return api_entry_context<detail::tiledb_vfs_fh_is_closed>(ctx, fh, is_closed);
}

int32_t tiledb_vfs_touch(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) noexcept {
  return api_entry_context<detail::tiledb_vfs_touch>(ctx, vfs, uri);
}
