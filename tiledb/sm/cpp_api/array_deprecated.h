/**
 * @file   array_deprecated.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * C++ Deprecated API for TileDB Array.
 */

#include "tiledb.h"

// clang-format off
/**
 * @copybrief Array::Array(const Context&,const std::string&,tiledb_query_type_t,TemporalPolicy,EncryptionAlgorithm)
 *
 * See @ref Array::Array(const Context&,const std::string&,tiledb_query_type_t,TemporalPolicy,EncryptionAlgorithm) "Array::Array"
 * 
 * @deprecated Release 2.15
 * @note Slated for removal in release 2.17
 *
 * @param ctx TileDB context.
 * @param array_uri The array URI.
 * @param query_type Query type to open the array for.
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 */
// clang-format on
TILEDB_DEPRECATED
Array(
    const Context& ctx,
    const std::string& array_uri,
    tiledb_query_type_t query_type,
    tiledb_encryption_type_t encryption_type,
    const std::string& encryption_key)
    : Array(
          ctx,
          array_uri,
          query_type,
          TemporalPolicy(),
          EncryptionAlgorithm(encryption_type, encryption_key.c_str())) {
}

// clang-format off
/**
 * @copybrief Array::Array(const Context&,const std::string&,tiledb_query_type_t,TemporalPolicy,EncryptionAlgorithm)
 *
 * See @ref Array::Array(const Context&,const std::string&,tiledb_query_type_t,TemporalPolicy,EncryptionAlgorithm) "Array::Array"
 * 
 * @deprecated Release 2.15
 * @note Slated for removal in release 2.17
 *
 * @param ctx TileDB context.
 * @param array_uri The array URI.
 * @param query_type Query type to open the array for.
 * @param timestamp The timestamp to open the array at.
 */
// clang-format on
TILEDB_DEPRECATED
Array(
    const Context& ctx,
    const std::string& array_uri,
    tiledb_query_type_t query_type,
    uint64_t timestamp)
    : Array(ctx, array_uri, query_type, TemporalPolicy(TimeTravel, timestamp)) {
}

// clang-format off
/**
 * @copybrief Array::Array(const Context&,const std::string&,tiledb_query_type_t,TemporalPolicy,EncryptionAlgorithm)
 *
 * See @ref Array::Array(const Context&,const std::string&,tiledb_query_type_t,TemporalPolicy,EncryptionAlgorithm) "Array::Array"
 * 
 * @deprecated Release 2.15
 * @note Slated for removal in release 2.17
 *
 * @param ctx TileDB context.
 * @param array_uri The array URI.
 * @param query_type Query type to open the array for.
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 * @param key_length Length in bytes of the encryption key.
 * @param timestamp The timestamp to open the array at.
 */
// clang-format on
TILEDB_DEPRECATED
Array(
    const Context& ctx,
    const std::string& array_uri,
    tiledb_query_type_t query_type,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    uint64_t timestamp)
    : Array(
          ctx,
          array_uri,
          query_type,
          TemporalPolicy(TimeTravel, timestamp),
          EncryptionAlgorithm(
              encryption_type,
              std::string((const char*)encryption_key, key_length).c_str())) {
}

// clang-format off
/**
 * @copybrief Array::Array(const Context&,const std::string&,tiledb_query_type_t,TemporalPolicy,EncryptionAlgorithm)
 *
 * See @ref Array::Array(const Context&,const std::string&,tiledb_query_type_t,TemporalPolicy,EncryptionAlgorithm) "Array::Array"
 * 
 * @deprecated Release 2.15
 * @note Slated for removal in release 2.17
 */
// clang-format on
TILEDB_DEPRECATED
Array(
    const Context& ctx,
    const std::string& array_uri,
    tiledb_query_type_t query_type,
    tiledb_encryption_type_t encryption_type,
    const std::string& encryption_key,
    uint64_t timestamp)
    : Array(
          ctx,
          array_uri,
          query_type,
          TemporalPolicy(TimeTravel, timestamp),
          EncryptionAlgorithm(encryption_type, encryption_key.c_str())) {
}

/**
 * Note: This API is deprecated and replaced with a static method.
 *
 * @deprecated Release 2.15
 * @note Slated for removal in release 2.17
 */
TILEDB_DEPRECATED
void delete_array(const std::string& uri) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_array_delete_array(ctx.ptr().get(), array_.get(), uri.c_str()));
}

// clang-format off
/**
 * @copybrief Array::open(tiledb_query_type_t)
 *
 * See @ref Array::open(tiledb_query_type_t) "Array::open"
 * 
 * @deprecated Release 2.15
 * @note Slated for removal in release 2.17
 *
 * @param query_type The type of queries the array object will be receiving.
 * @param timestamp The timestamp to open the array at.
 * @throws TileDBError if the array is already open or other error occurred.
 */
// clang-format on
TILEDB_DEPRECATED
void open(tiledb_query_type_t query_type, uint64_t timestamp) {
  auto& ctx = ctx_.get();
  tiledb_ctx_t* c_ctx = ctx.ptr().get();
  ctx.handle_error(
      tiledb_array_set_open_timestamp_end(c_ctx, array_.get(), timestamp));
  open(query_type);
}

// clang-format off
/**
 * @copybrief Array::open(tiledb_query_type_t,tiledb_encryption_type_t,const std::string&)
 *
 * See @ref Array::open(tiledb_query_type_t,tiledb_encryption_type_t,const std::string&) "Array::open"
 * 
 * @deprecated Release 2.15
 * @note Slated for removal in release 2.17
 *
 * @param query_type The type of queries the array object will be receiving.
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 * @param key_length Length in bytes of the encryption key.
 * @param timestamp The timestamp to open the array at.
 */
// clang-format on
TILEDB_DEPRECATED
void open(
    tiledb_query_type_t query_type,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    uint64_t timestamp) {
  auto& ctx = ctx_.get();
  tiledb_ctx_t* c_ctx = ctx.ptr().get();

  ctx.handle_error(
      tiledb_array_set_open_timestamp_end(c_ctx, array_.get(), timestamp));

  std::string enc_key_str((const char*)encryption_key, key_length);
  open(query_type, encryption_type, enc_key_str);
}
