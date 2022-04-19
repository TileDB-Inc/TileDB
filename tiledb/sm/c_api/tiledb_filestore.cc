/**
 * @file   tiledb_filestore.cc
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
 * This file defines the C API of TileDB for filestore.
 **/

#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/c_api/api_exception_safety.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/enums/mime_type.h"

namespace tiledb::common::detail {

TILEDB_EXPORT int32_t tiledb_filestore_schema_create(
    tiledb_ctx_t* ctx,
    const char* uri,
    tiledb_mime_type_t mime_type,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT {
  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_filestore_uri_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    const char* file_uri) TILEDB_NOEXCEPT {
  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_filestore_uri_export(
    tiledb_ctx_t* ctx,
    const char* file_uri,
    const char* filestore_array_uri) TILEDB_NOEXCEPT {
  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_filestore_buffer_import(
    tiledb_ctx_t* ctx, const char* filestore_array_uri, void* buf, size_t size)
    TILEDB_NOEXCEPT {
  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_filestore_buffer_export(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    void** buf,
    size_t alloc_size) TILEDB_NOEXCEPT {
  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_mime_type_to_str(
    tiledb_mime_type_t mime_type, const char** str) TILEDB_NOEXCEPT {
  const auto& strval =
      tiledb::sm::mime_type_str((tiledb::sm::MimeType)mime_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_mime_type_from_str(
    const char* str, tiledb_mime_type_t* mime_type) TILEDB_NOEXCEPT {
  tiledb::sm::MimeType val = tiledb::sm::MimeType::MIME_NONE;
  if (!tiledb::sm::mime_type_enum(str, &val).ok())
    return TILEDB_ERR;
  *mime_type = (tiledb_mime_type_t)val;
  return TILEDB_OK;
}

}  // namespace tiledb::common::detail

TILEDB_EXPORT int32_t tiledb_filestore_schema_create(
    tiledb_ctx_t* ctx,
    const char* uri,
    tiledb_mime_type_t mime_type,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_schema_create>(
      ctx, uri, mime_type, array_schema);
}

TILEDB_EXPORT int32_t tiledb_filestore_uri_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    const char* file_uri) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_uri_import>(
      ctx, filestore_array_uri, file_uri);
}

TILEDB_EXPORT int32_t tiledb_filestore_uri_export(
    tiledb_ctx_t* ctx,
    const char* file_uri,
    const char* filestore_array_uri) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_uri_export>(
      ctx, file_uri, filestore_array_uri);
}

TILEDB_EXPORT int32_t tiledb_filestore_buffer_import(
    tiledb_ctx_t* ctx, const char* filestore_array_uri, void* buf, size_t size)
    TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_buffer_import>(
      ctx, filestore_array_uri, buf, size);
}

TILEDB_EXPORT int32_t tiledb_filestore_buffer_export(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    void** buf,
    size_t alloc_size) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_buffer_export>(
      ctx, filestore_array_uri, buf, alloc_size);
}

TILEDB_EXPORT int32_t tiledb_mime_type_to_str(
    tiledb_mime_type_t mime_type, const char** str) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_mime_type_to_str>(mime_type, str);
}

TILEDB_EXPORT int32_t tiledb_mime_type_from_str(
    const char* str, tiledb_mime_type_t* mime_type) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_mime_type_from_str>(str, mime_type);
}
