/**
 * @file tiledb/api/c_api/buffer/buffer_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines the buffer C API of TileDB.
 **/

#include "tiledb/api/c_api_support/c_api_support.h"

#include "buffer_api_external.h"
#include "buffer_api_internal.h"

namespace tiledb::api {

capi_return_t tiledb_buffer_alloc(tiledb_buffer_handle_t** buffer) {
  ensure_output_pointer_is_valid(buffer);
  *buffer = tiledb_buffer_handle_t::make_handle();
  return TILEDB_OK;
}

void tiledb_buffer_free(tiledb_buffer_handle_t** buffer) {
  ensure_output_pointer_is_valid(buffer);
  ensure_buffer_is_valid(*buffer);
  tiledb_buffer_handle_t::break_handle(*buffer);
}

capi_return_t tiledb_buffer_set_type(
    tiledb_buffer_handle_t* buffer, tiledb_datatype_t datatype) {
  ensure_buffer_is_valid(buffer);
  buffer->set_datatype(static_cast<tiledb::sm::Datatype>(datatype));
  return TILEDB_OK;
}

capi_return_t tiledb_buffer_get_type(
    const tiledb_buffer_handle_t* buffer, tiledb_datatype_t* datatype) {
  ensure_buffer_is_valid(buffer);
  ensure_output_pointer_is_valid(datatype);
  *datatype = static_cast<tiledb_datatype_t>(buffer->datatype());
  return TILEDB_OK;
}

capi_return_t tiledb_buffer_get_data(
    const tiledb_buffer_t* buffer, void** data, uint64_t* num_bytes) {
  ensure_buffer_is_valid(buffer);
  ensure_output_pointer_is_valid(data);
  ensure_output_pointer_is_valid(num_bytes);

  *data = buffer->buffer().data();
  *num_bytes = buffer->buffer().size();

  return TILEDB_OK;
}

capi_return_t tiledb_buffer_set_data(
    tiledb_buffer_t* buffer, void* data, uint64_t size) {
  ensure_buffer_is_valid(buffer);

  // Create a temporary Buffer object as a wrapper.
  tiledb::sm::Buffer tmp_buffer(data, size);

  // Swap with the given buffer.
  buffer->buffer().swap(tmp_buffer);

  // 'tmp_buffer' now destructs, freeing the old allocation (if any) of the
  // given buffer.

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_void;

capi_return_t tiledb_buffer_alloc(
    tiledb_ctx_t* ctx, tiledb_buffer_t** buffer) noexcept {
  return api_entry_context<tiledb::api::tiledb_buffer_alloc>(ctx, buffer);
}

void tiledb_buffer_free(tiledb_buffer_t** buffer) noexcept {
  return api_entry_void<tiledb::api::tiledb_buffer_free>(buffer);
}

capi_return_t tiledb_buffer_set_type(
    tiledb_ctx_t* ctx,
    tiledb_buffer_t* buffer,
    tiledb_datatype_t datatype) noexcept {
  return api_entry_context<tiledb::api::tiledb_buffer_set_type>(
      ctx, buffer, datatype);
}

capi_return_t tiledb_buffer_get_type(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_datatype_t* datatype) noexcept {
  return api_entry_context<tiledb::api::tiledb_buffer_get_type>(
      ctx, buffer, datatype);
}

capi_return_t tiledb_buffer_get_data(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    void** data,
    uint64_t* size) noexcept {
  return api_entry_context<tiledb::api::tiledb_buffer_get_data>(
      ctx, buffer, data, size);
}

capi_return_t tiledb_buffer_set_data(
    tiledb_ctx_t* ctx, tiledb_buffer_t* buffer, void* data, uint64_t size) {
  return api_entry_context<tiledb::api::tiledb_buffer_set_data>(
      ctx, buffer, data, size);
}
