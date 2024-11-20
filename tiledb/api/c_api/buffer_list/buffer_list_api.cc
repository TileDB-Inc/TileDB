/**
 * @file tiledb/api/c_api/buffer_list/buffer_list_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines the buffer list C API of TileDB.
 */

#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/common/memory_tracker.h"

#include "../buffer/buffer_api_internal.h"
#include "buffer_list_api_external.h"
#include "buffer_list_api_internal.h"

namespace tiledb::api {

capi_return_t tiledb_buffer_list_alloc(
    tiledb_ctx_handle_t* ctx, tiledb_buffer_list_t** buffer_list) {
  ensure_context_is_valid(ctx);
  ensure_output_pointer_is_valid(buffer_list);
  *buffer_list = make_handle<tiledb_buffer_list_handle_t>(
      ctx->resources().serialization_memory_tracker());
  return TILEDB_OK;
}

void tiledb_buffer_list_free(tiledb_buffer_list_t** buffer_list) {
  ensure_output_pointer_is_valid(buffer_list);
  ensure_buffer_list_is_valid(*buffer_list);
  break_handle(*buffer_list);
}

capi_return_t tiledb_buffer_list_get_num_buffers(
    const tiledb_buffer_list_t* buffer_list, uint64_t* num_buffers) {
  ensure_buffer_list_is_valid(buffer_list);
  ensure_output_pointer_is_valid(num_buffers);
  *num_buffers = buffer_list->buffer_list().num_buffers();
  return TILEDB_OK;
}

capi_return_t tiledb_buffer_list_get_buffer(
    const tiledb_buffer_list_t* buffer_list,
    uint64_t buffer_idx,
    tiledb_buffer_t** buffer) {
  ensure_buffer_list_is_valid(buffer_list);
  ensure_output_pointer_is_valid(buffer);

  // Get the underlying buffer
  span<const char> b = buffer_list->buffer_list().get_buffer(buffer_idx);

  // Create a non-owning wrapper of the underlying buffer
  *buffer = make_handle<tiledb_buffer_handle_t>(
      b.data(), b.size(), buffer_list->memory_tracker());

  return TILEDB_OK;
}

capi_return_t tiledb_buffer_list_get_total_size(
    const tiledb_buffer_list_t* buffer_list, uint64_t* total_size) {
  ensure_buffer_list_is_valid(buffer_list);
  ensure_output_pointer_is_valid(total_size);
  *total_size = buffer_list->buffer_list().total_size();
  return TILEDB_OK;
}

capi_return_t tiledb_buffer_list_flatten(
    tiledb_buffer_list_t* buffer_list, tiledb_buffer_t** buffer) {
  ensure_buffer_list_is_valid(buffer_list);
  ensure_output_pointer_is_valid(buffer);

  // Create a serialization buffer
  const auto nbytes = buffer_list->buffer_list().total_size();
  *buffer = make_handle<tiledb_buffer_t>(
      buffer_list->buffer_list().total_size(), buffer_list->memory_tracker());

  try {
    // Read all into the dest buffer
    buffer_list->buffer_list().reset_offset();
    buffer_list->buffer_list().read(
        (*buffer)->buffer().owned_mutable_span().data(), nbytes);
  } catch (...) {
    break_handle(*buffer);
    throw;
  }

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_void;
using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    buffer_list_alloc, tiledb_ctx_t* ctx, tiledb_buffer_list_t** buffer_list) {
  return api_entry_with_context<tiledb::api::tiledb_buffer_list_alloc>(
      ctx, buffer_list);
}

CAPI_INTERFACE_VOID(buffer_list_free, tiledb_buffer_list_t** buffer_list) {
  return api_entry_void<tiledb::api::tiledb_buffer_list_free>(buffer_list);
}

CAPI_INTERFACE(
    buffer_list_get_num_buffers,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_list_t* buffer_list,
    uint64_t* num_buffers) {
  return api_entry_context<tiledb::api::tiledb_buffer_list_get_num_buffers>(
      ctx, buffer_list, num_buffers);
}

CAPI_INTERFACE(
    buffer_list_get_buffer,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_list_t* buffer_list,
    uint64_t buffer_idx,
    tiledb_buffer_t** buffer) {
  return api_entry_context<tiledb::api::tiledb_buffer_list_get_buffer>(
      ctx, buffer_list, buffer_idx, buffer);
}

CAPI_INTERFACE(
    buffer_list_get_total_size,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_list_t* buffer_list,
    uint64_t* total_size) {
  return api_entry_context<tiledb::api::tiledb_buffer_list_get_total_size>(
      ctx, buffer_list, total_size);
}

CAPI_INTERFACE(
    buffer_list_flatten,
    tiledb_ctx_t* ctx,
    tiledb_buffer_list_t* buffer_list,
    tiledb_buffer_t** buffer) {
  return api_entry_context<tiledb::api::tiledb_buffer_list_flatten>(
      ctx, buffer_list, buffer);
}
