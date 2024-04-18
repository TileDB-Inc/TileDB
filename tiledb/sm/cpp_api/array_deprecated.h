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

/**
 * Deletes the fragments written between the input timestamps of an array
 * with the input uri.
 *
 * @param uri The URI of the fragments' parent Array.
 * @param timestamp_start The epoch start timestamp in milliseconds.
 * @param timestamp_end The epoch end timestamp in milliseconds. Use
 * UINT64_MAX for the current timestamp.
 */
TILEDB_DEPRECATED
void delete_fragments(
    const std::string& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end) const {
  throw std::logic_error(
      "This method is deprecated. Please use "
      "Array::delete_fragments(ctx, uri, timestamp_start, timestamp_end)");
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_array_delete_fragments_v2(
      ctx.ptr().get(), uri.c_str(), timestamp_start, timestamp_end));
}

/**
 * @brief Consolidates the fragments of an encrypted array into a single
 * fragment.
 *
 * You must first finalize all queries to the array before consolidation can
 * begin (as consolidation temporarily acquires an exclusive lock on the
 * array).
 *
 * **Example:**
 * @code{.cpp}
 * // Load AES-256 key from disk, environment variable, etc.
 * uint8_t key[32] = ...;
 * tiledb::Array::consolidate(
 *     ctx,
 *     "s3://bucket-name/array-name",
 *     TILEDB_AES_256_GCM,
 *     key,
 *     sizeof(key));
 * @endcode
 *
 * @param ctx TileDB context
 * @param array_uri The URI of the TileDB array to be consolidated.
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 * @param key_length Length in bytes of the encryption key.
 * @param config Configuration parameters for the consolidation.
 */
TILEDB_DEPRECATED
static void consolidate(
    const Context& ctx,
    const std::string& uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    Config* const config = nullptr) {
  ctx.handle_error(tiledb_array_consolidate_with_key(
      ctx.ptr().get(),
      uri.c_str(),
      encryption_type,
      encryption_key,
      key_length,
      config ? config->ptr().get() : nullptr));
}

// clang-format off
  /**
   * @copybrief Array::consolidate(const Context&,const std::string&,tiledb_encryption_type_t,const void*,uint32_t,const Config&)
   *
   * See @ref Array::consolidate(
   *     const Context&,
   *     const std::string&,
   *     tiledb_encryption_type_t,
   *     const void*,
   *     uint32_t,const Config&) "Array::consolidate"
   *
   * @param ctx TileDB context
   * @param array_uri The URI of the TileDB array to be consolidated.
   * @param encryption_type The encryption type to use.
   * @param encryption_key The encryption key to use.
   * @param config Configuration parameters for the consolidation.
   */
// clang-format on
TILEDB_DEPRECATED
static void consolidate(
    const Context& ctx,
    const std::string& uri,
    tiledb_encryption_type_t encryption_type,
    const std::string& encryption_key,
    Config* const config = nullptr) {
  return consolidate(
      ctx,
      uri,
      encryption_type,
      encryption_key.data(),
      (uint32_t)encryption_key.size(),
      config);
}

/**
 * Loads the array schema from an encrypted array.
 *
 * **Example:**
 * @code{.cpp}
 * auto schema = tiledb::Array::load_schema(ctx,
 * "s3://bucket-name/array-name", key_type, key, key_len);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param uri The array URI.
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 * @param key_length Length in bytes of the encryption key.
 * @return The loaded ArraySchema object.
 */
TILEDB_DEPRECATED
static ArraySchema load_schema(
    const Context& ctx,
    const std::string& uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  tiledb_array_schema_t* schema;
  ctx.handle_error(tiledb_array_schema_load_with_key(
      ctx.ptr().get(),
      uri.c_str(),
      encryption_type,
      encryption_key,
      key_length,
      &schema));
  return ArraySchema(ctx, schema);
}

/**
 * @brief Creates a new encrypted TileDB array given an input schema.
 *
 * **Example:**
 * @code{.cpp}
 * // Load AES-256 key from disk, environment variable, etc.
 * uint8_t key[32] = ...;
 * tiledb::Array::create("s3://bucket-name/array-name", schema,
 *    TILEDB_AES_256_GCM, key, sizeof(key));
 * @endcode
 *
 * @param uri URI where array will be created.
 * @param schema The array schema.
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 * @param key_length Length in bytes of the encryption key.
 */
TILEDB_DEPRECATED
static void create(
    const std::string& uri,
    const ArraySchema& schema,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto& ctx = schema.context();
  tiledb_ctx_t* c_ctx = ctx.ptr().get();
  ctx.handle_error(tiledb_array_schema_check(c_ctx, schema.ptr().get()));
  ctx.handle_error(tiledb_array_create_with_key(
      c_ctx,
      uri.c_str(),
      schema.ptr().get(),
      encryption_type,
      encryption_key,
      key_length));
}

// clang-format off
  /**
   * @copybrief Array::create(const std::string&,const ArraySchema&,tiledb_encryption_type_t,const void*,uint32_t)
   *
   * See @ref Array::create(const std::string&,const ArraySchema&,tiledb_encryption_type_t,const void*,uint32_t) "Array::create"
   *
   * @param uri URI where array will be created.
   * @param schema The array schema.
   * @param encryption_type The encryption type to use.
   * @param encryption_key The encryption key to use.
   */
// clang-format on
TILEDB_DEPRECATED
static void create(
    const std::string& uri,
    const ArraySchema& schema,
    tiledb_encryption_type_t encryption_type,
    const std::string& encryption_key) {
  return create(
      uri,
      schema,
      encryption_type,
      encryption_key.data(),
      (uint32_t)encryption_key.size());
}

/**
 * @brief Consolidates the metadata of an encrypted array.
 *
 * You must first finalize all queries to the array before consolidation can
 * begin (as consolidation temporarily acquires an exclusive lock on the
 * array).
 *
 * **Example:**
 * @code{.cpp}
 * // Load AES-256 key from disk, environment variable, etc.
 * uint8_t key[32] = ...;
 * tiledb::Array::consolidate_metadata(
 *     ctx,
 *     "s3://bucket-name/array-name",
 *     TILEDB_AES_256_GCM,
 *     key,
 *     sizeof(key));
 * @endcode
 *
 * @param ctx TileDB context
 * @param array_uri The URI of the TileDB array whose
 *     metadata will be consolidated.
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 * @param key_length Length in bytes of the encryption key.
 * @param config Configuration parameters for the consolidation.
 */
TILEDB_DEPRECATED
static void consolidate_metadata(
    const Context& ctx,
    const std::string& uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    Config* const config = nullptr) {
  Config local_cfg;
  Config* config_aux = config;
  if (!config_aux) {
    config_aux = &local_cfg;
  }

  (*config_aux)["sm.consolidation.mode"] = "array_meta";
  consolidate(
      ctx, uri, encryption_type, encryption_key, key_length, config_aux);
}

// clang-format off
  /**
   * @copybrief Array::consolidate_metadata(const Context&, const std::string&, tiledb_encryption_type_t, const void*,uint32_t, const Config&)
   *
   * See @ref Array::consolidate_metadata(
   *     const Context&,
   *     const std::string&,
   *     tiledb_encryption_type_t,
   *     const void*,
   *     uint32_t,const Config&) "Array::consolidate_metadata"
   *
   * @param ctx TileDB context
   * @param array_uri The URI of the TileDB array whose
   *     metadata will be consolidated.
   * @param encryption_type The encryption type to use.
   * @param encryption_key The encryption key to use.
   * @param config Configuration parameters for the consolidation.
   */
// clang-format on
TILEDB_DEPRECATED
static void consolidate_metadata(
    const Context& ctx,
    const std::string& uri,
    tiledb_encryption_type_t encryption_type,
    const std::string& encryption_key,
    Config* const config = nullptr) {
  Config local_cfg;
  Config* config_aux = config;
  if (!config_aux) {
    config_aux = &local_cfg;
  }

  (*config_aux)["sm.consolidation.mode"] = "array_meta";
  consolidate(
      ctx,
      uri,
      encryption_type,
      encryption_key.data(),
      (uint32_t)encryption_key.size(),
      config_aux);
}
