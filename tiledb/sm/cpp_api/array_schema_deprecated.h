/**
 * @file   array_schema_deprecated.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * C++ Deprecated API for TileDB ArraySchema.
 */

#include "tiledb.h"

/**
 * Loads the schema of an existing encrypted array.
 *
 * **Example:**
 * @code{.cpp}
 * // Load AES-256 key from disk, environment variable, etc.
 * uint8_t key[32] = ...;
 * tiledb::Context ctx;
 * tiledb::ArraySchema schema(ctx, "s3://bucket-name/array-name",
 *    TILEDB_AES_256_GCM, key, sizeof(key));
 * @endcode
 *
 * @param ctx TileDB context
 * @param uri URI of array
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 * @param key_length Length in bytes of the encryption key.
 */
TILEDB_DEPRECATED
ArraySchema(
    const Context& ctx,
    const std::string& uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length)
    : Schema(ctx) {
  tiledb_ctx_t* c_ctx = ctx.ptr().get();
  tiledb_array_schema_t* schema;
  ctx.handle_error(tiledb_array_schema_load_with_key(
      c_ctx,
      uri.c_str(),
      encryption_type,
      encryption_key,
      key_length,
      &schema));
  schema_ = std::shared_ptr<tiledb_array_schema_t>(schema, deleter_);
}

/**
 * Loads the schema of an existing encrypted array.
 *
 * @param ctx TileDB context
 * @param uri URI of array
 * @param encryption_type The encryption type to use.
 * @param encryption_key The encryption key to use.
 */
TILEDB_DEPRECATED
ArraySchema(
    const Context& ctx,
    const std::string& uri,
    tiledb_encryption_type_t encryption_type,
    const std::string& encryption_key)
    : ArraySchema(
          ctx,
          uri,
          encryption_type,
          encryption_key.data(),
          (uint32_t)encryption_key.size()) {
}
