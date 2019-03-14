/**
 * @file   tiledb_cpp_api_array.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file declares the C++ API for TileDB array operations.
 */

#ifndef TILEDB_CPP_API_ARRAY_H
#define TILEDB_CPP_API_ARRAY_H

#include "array_schema.h"
#include "context.h"
#include "tiledb.h"
#include "type.h"

#include <unordered_map>
#include <vector>

namespace tiledb {

/**
 * Class representing a TileDB array object.
 *
 * @details
 * An Array object represents array data in TileDB at some persisted location,
 * e.g. on disk, in an S3 bucket, etc. Once an array has been opened for reading
 * or writing, interact with the data through Query objects.
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 *
 * // Create an ArraySchema, add attributes, domain, etc.
 * tiledb::ArraySchema schema(...);
 *
 * // Create empty array named "my_array" on persistent storage.
 * tiledb::Array::create("my_array", schema);
 * @endcode
 */
class Array {
 public:
  /**
   * @brief Constructor. This opens the array for the given query type. The
   * destructor calls the `close()` method.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Open the array for reading
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * @endcode
   *
   * @param ctx TileDB context.
   * @param array_uri The array URI.
   * @param query_type Query type to open the array for.
   */
  Array(
      const Context& ctx,
      const std::string& array_uri,
      tiledb_query_type_t query_type)
      : Array(ctx, array_uri, query_type, TILEDB_NO_ENCRYPTION, nullptr, 0) {
  }

  /**
   * @brief Constructor. This opens an encrypted array for the given query type.
   * The destructor calls the `close()` method.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Open the encrypted array for reading
   * tiledb::Context ctx;
   * // Load AES-256 key from disk, environment variable, etc.
   * uint8_t key[32] = ...;
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ,
   *    TILEDB_AES_256_GCM, key, sizeof(key));
   * @endcode
   *
   * @param ctx TileDB context.
   * @param array_uri The array URI.
   * @param query_type Query type to open the array for.
   * @param encryption_type The encryption type to use.
   * @param encryption_key The encryption key to use.
   * @param key_length Length in bytes of the encryption key.
   */
  Array(
      const Context& ctx,
      const std::string& array_uri,
      tiledb_query_type_t query_type,
      tiledb_encryption_type_t encryption_type,
      const void* encryption_key,
      uint32_t key_length)
      : ctx_(ctx)
      , schema_(ArraySchema(ctx, (tiledb_array_schema_t*)nullptr)) {
    tiledb_array_t* array;
    ctx.handle_error(tiledb_array_alloc(ctx, array_uri.c_str(), &array));
    array_ = std::shared_ptr<tiledb_array_t>(array, deleter_);
    ctx.handle_error(tiledb_array_open_with_key(
        ctx, array, query_type, encryption_type, encryption_key, key_length));

    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array, &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  // clang-format off
  /**
   * @copybrief Array::Array(const Context&,const std::string&,tiledb_query_type_t,tiledb_encryption_type_t,const void*,uint32_t)
   *
   * See @ref Array::Array(const Context&,const std::string&,tiledb_query_type_t,tiledb_encryption_type_t,const void*,uint32_t) "Array::Array"
   *
   * @param ctx TileDB context.
   * @param array_uri The array URI.
   * @param query_type Query type to open the array for.
   * @param encryption_type The encryption type to use.
   * @param encryption_key The encryption key to use.
   */
  // clang-format on
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
            encryption_type,
            encryption_key.data(),
            (uint32_t)encryption_key.size()) {
  }

  /**
   * @brief Constructor. This opens the array for the given query type at the
   * given timestamp. The destructor calls the `close()` method.
   *
   * This constructor takes as input a
   * timestamp, representing time in milliseconds ellapsed since
   * 1970-01-01 00:00:00 +0000 (UTC). Opening the array at a
   * timestamp provides a view of the array with all writes/updates that
   * happened at or before `timestamp` (i.e., excluding those that
   * occurred after `timestamp`). This is useful to ensure
   * consistency at a potential distributed setting, where machines
   * need to operate on the same view of the array.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Open the array for reading
   * tiledb::Context ctx;
   * // Get some `timestamp` here in milliseconds
   * tiledb::Array array(
   *     ctx, "s3://bucket-name/array-name", TILEDB_READ, timestamp);
   * @endcode
   *
   * @param ctx TileDB context.
   * @param array_uri The array URI.
   * @param query_type Query type to open the array for.
   * @param timestamp The timestamp to open the array at.
   */
  Array(
      const Context& ctx,
      const std::string& array_uri,
      tiledb_query_type_t query_type,
      uint64_t timestamp)
      : Array(
            ctx,
            array_uri,
            query_type,
            TILEDB_NO_ENCRYPTION,
            nullptr,
            0,
            timestamp) {
  }

  // clang-format off
  /**
   * @copybrief Array::Array(const Context&,const std::string&,tiledb_query_type_t,uint64_t)
   *
   * Same as @ref Array::Array(const Context&,const std::string&,tiledb_query_type_t,uint64_t) "Array::Array"
   * but for encrypted arrays.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Open the encrypted array for reading
   * tiledb::Context ctx;
   * // Load AES-256 key from disk, environment variable, etc.
   * uint8_t key[32] = ...;
   * // Get some `timestamp` here in milliseconds
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ,
   *    TILEDB_AES_256_GCM, key, sizeof(key), timestamp);
   * @endcode
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
  Array(
      const Context& ctx,
      const std::string& array_uri,
      tiledb_query_type_t query_type,
      tiledb_encryption_type_t encryption_type,
      const void* encryption_key,
      uint32_t key_length,
      uint64_t timestamp)
      : ctx_(ctx)
      , schema_(ArraySchema(ctx, (tiledb_array_schema_t*)nullptr)) {
    tiledb_array_t* array;
    ctx.handle_error(tiledb_array_alloc(ctx, array_uri.c_str(), &array));
    array_ = std::shared_ptr<tiledb_array_t>(array, deleter_);
    ctx.handle_error(tiledb_array_open_at_with_key(
        ctx,
        array,
        query_type,
        encryption_type,
        encryption_key,
        key_length,
        timestamp));

    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array, &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  // clang-format off
  /**
   * @copybrief Array::Array(const Context&,const std::string&,tiledb_query_type_t,tiledb_encryption_type_t,const void*,uint32_t,uint64_t)
   *
   * See @ref Array::Array(const Context&,const std::string&,tiledb_query_type_t,tiledb_encryption_type_t,const void*,uint32_t,uint64_t) "Array::Array"
   */
  // clang-format on
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
            encryption_type,
            encryption_key.data(),
            (uint32_t)encryption_key.size(),
            timestamp) {
  }

  Array(const Array&) = default;
  Array(Array&&) = default;
  Array& operator=(const Array&) = default;
  Array& operator=(Array&&) = default;

  /** Destructor; calls `close()`. */
  ~Array() {
    close();
  }

  /** Checks if the array is open. */
  bool is_open() const {
    auto& ctx = ctx_.get();
    int open = 0;
    ctx.handle_error(tiledb_array_is_open(ctx, array_.get(), &open));
    return bool(open);
  }

  /** Returns the array URI. */
  std::string uri() const {
    auto& ctx = ctx_.get();
    const char* uri = nullptr;
    ctx.handle_error(tiledb_array_get_uri(ctx, array_.get(), &uri));
    return std::string(uri);
  }

  /** Get the ArraySchema for the array. **/
  ArraySchema schema() const {
    auto& ctx = ctx_.get();
    tiledb_array_schema_t* schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array_.get(), &schema));
    return ArraySchema(ctx, schema);
  }

  /** Returns a shared pointer to the C TileDB array object. */
  std::shared_ptr<tiledb_array_t> ptr() const {
    return array_;
  }

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_array_t*() const {
    return array_.get();
  }

  /**
   * @brief Opens the array. The array is opened using a query type as input.
   *
   * This is to indicate that queries created for this `Array`
   * object will inherit the query type. In other words, `Array`
   * objects are opened to receive only one type of queries.
   * They can always be closed and be re-opened with another query type.
   * Also there may be many different `Array`
   * objects created and opened with different query types. For
   * instance, one may create and open an array object `array_read` for
   * reads and another one `array_write` for writes, and interleave
   * creation and submission of queries for both these array objects.
   *
   * **Example:**
   * @code{.cpp}
   * // Open the array for writing
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_WRITE);
   * // Close and open again for reading.
   * array.close();
   * array.open(TILEDB_READ);
   * @endcode
   *
   * @param query_type The type of queries the array object will be receiving.
   * @throws TileDBError if the array is already open or other error occurred.
   */
  void open(tiledb_query_type_t query_type) {
    open(query_type, TILEDB_NO_ENCRYPTION, nullptr, 0);
  }

  /**
   * @brief Opens the array, for encrypted arrays.
   *
   * **Example:**
   * @code{.cpp}
   * // Load AES-256 key from disk, environment variable, etc.
   * uint8_t key[32] = ...;
   * // Open the encrypted array for writing
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_WRITE,
   *    TILEDB_AES_256_GCM, key, sizeof(key));
   * // Close and open again for reading.
   * array.close();
   * array.open(TILEDB_READ, TILEDB_AES_256_GCM, key, sizeof(key));
   * @endcode
   *
   * @param query_type The type of queries the array object will be receiving.
   * @param encryption_type The encryption type to use.
   * @param encryption_key The encryption key to use.
   * @param key_length Length in bytes of the encryption key.
   */
  void open(
      tiledb_query_type_t query_type,
      tiledb_encryption_type_t encryption_type,
      const void* encryption_key,
      uint32_t key_length) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_open_with_key(
        ctx,
        array_.get(),
        query_type,
        encryption_type,
        encryption_key,
        key_length));
    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array_.get(), &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  // clang-format off
  /**
   * @copybrief Array::open(tiledb_query_type_t,tiledb_encryption_type_t,const void*,uint32_t)
   *
   * See @ref Array::open(tiledb_query_type_t,tiledb_encryption_type_t,const void*,uint32_t) "Array::open"
   */
  // clang-format on
  void open(
      tiledb_query_type_t query_type,
      tiledb_encryption_type_t encryption_type,
      const std::string& encryption_key) {
    open(
        query_type,
        encryption_type,
        encryption_key.data(),
        (uint32_t)encryption_key.size());
  }

  /**
   * @brief Opens the array for a query type, at the given timestamp.
   *
   * This function takes as input a
   * timestamp, representing time in milliseconds ellapsed since
   * 1970-01-01 00:00:00 +0000 (UTC). Opening the array at a
   * timestamp provides a view of the array with all writes/updates that
   * happened at or before `timestamp` (i.e., excluding those that
   * occurred after `timestamp`). This is useful to ensure
   * consistency at a potential distributed setting, where machines
   * need to operate on the same view of the array.
   *
   * **Example:**
   * @code{.cpp}
   * // Open the array for writing
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_WRITE);
   * // Close and open again for reading.
   * array.close();
   * // Get some `timestamp` in milliseconds here
   * array.open(TILEDB_READ, timestamp);
   * @endcode
   *
   * @param query_type The type of queries the array object will be receiving.
   * @param timestamp The timestamp to open the array at.
   * @throws TileDBError if the array is already open or other error occurred.
   */
  void open(tiledb_query_type_t query_type, uint64_t timestamp) {
    open(query_type, TILEDB_NO_ENCRYPTION, nullptr, 0, timestamp);
  }

  /**
   * @copybrief Array::open(tiledb_query_type_t,uint64_t)
   *
   * Same as @ref Array::open(tiledb_query_type_t,uint64_t) "Array::open"
   * but for encrypted arrays.
   *
   * **Example:**
   * @code{.cpp}
   * // Load AES-256 key from disk, environment variable, etc.
   * uint8_t key[32] = ...;
   * // Open the encrypted array for writing
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_WRITE,
   *    TILEDB_AES_256_GCM, key, sizeof(key));
   * // Close and open again for reading.
   * array.close();
   * // Get some `timestamp` in milliseconds here
   * array.open(TILEDB_READ, TILEDB_AES_256_GCM, key, sizeof(key), timestamp);
   * @endcode
   *
   * @param query_type The type of queries the array object will be receiving.
   * @param encryption_type The encryption type to use.
   * @param encryption_key The encryption key to use.
   * @param key_length Length in bytes of the encryption key.
   * @param timestamp The timestamp to open the array at.
   */
  void open(
      tiledb_query_type_t query_type,
      tiledb_encryption_type_t encryption_type,
      const void* encryption_key,
      uint32_t key_length,
      uint64_t timestamp) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_open_at_with_key(
        ctx,
        array_.get(),
        query_type,
        encryption_type,
        encryption_key,
        key_length,
        timestamp));
    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array_.get(), &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  // clang-format off
  /**
   * @copybrief Array::open(tiledb_query_type_t,tiledb_encryption_type_t,const void*,uint32_t,uint64_t)
   *
   * See @ref Array::open(tiledb_query_type_t,tiledb_encryption_type_t,const void*,uint32_t,uint64_t) "Array::open"
   */
  // clang-format on
  void open(
      tiledb_query_type_t query_type,
      tiledb_encryption_type_t encryption_type,
      const std::string& encryption_key,
      uint64_t timestamp) {
    open(
        query_type,
        encryption_type,
        encryption_key.data(),
        (uint32_t)encryption_key.size(),
        timestamp);
  }

  /**
   * Reopens the array (the array must be already open). This is useful
   * when the array got updated after it got opened and the `Array`
   * object got created. To sync-up with the updates, the user must either
   * close the array and open with `open()`, or just use
   * `reopen()` without closing. This function will be generally
   * faster than the former alternative.
   *
   * Note: reopening encrypted arrays does not require the encryption key.
   *
   * **Example:**
   * @code{.cpp}
   * // Open the array for reading
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * array.reopen();
   * @endcode
   *
   * @throws TileDBError if the array was not already open or other error
   * occurred.
   */
  void reopen() {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_reopen(ctx, array_.get()));
    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array_.get(), &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  /**
   * Reopens the array at a specific timestamp.
   *
   * **Example:**
   * @code{.cpp}
   * // Open the array for reading
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * uint64_t timestamp = tiledb_timestamp_now_ms();
   * array.reopen_at(timestamp);
   * @endcode
   *
   * @throws TileDBError if the array was not already open or other error
   * occurred.
   */
  void reopen_at(uint64_t timestamp) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_reopen_at(ctx, array_.get(), timestamp));
    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array_.get(), &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  /** Returns the timestamp at which the array was opened. */
  uint64_t timestamp() const {
    auto& ctx = ctx_.get();
    uint64_t timestamp;
    ctx.handle_error(tiledb_array_get_timestamp(ctx, array_.get(), &timestamp));
    return timestamp;
  }

  /**
   * Closes the array. The destructor calls this automatically.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * array.close();
   * @endcode
   */
  void close() {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_close(ctx, array_.get()));
  }

  /**
   * @brief Consolidates the fragments of an array into a single fragment.
   *
   * You must first finalize all queries to the array before consolidation can
   * begin (as consolidation temporarily acquires an exclusive lock on the
   * array).
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Array::consolidate(ctx, "s3://bucket-name/array-name");
   * @endcode
   *
   * @param ctx TileDB context
   * @param array_uri The URI of the TileDB array to be consolidated.
   * @param config Configuration parameters for the consolidation.
   */
  static void consolidate(
      const Context& ctx,
      const std::string& uri,
      const Config& config = Config()) {
    consolidate(ctx, uri, TILEDB_NO_ENCRYPTION, nullptr, 0, config);
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
   * tiledb::Array::consolidate(ctx, "s3://bucket-name/array-name",
   *    TILEDB_AES_256_GCM, key, sizeof(key));
   * @endcode
   *
   * @param ctx TileDB context
   * @param array_uri The URI of the TileDB array to be consolidated.
   * @param encryption_type The encryption type to use.
   * @param encryption_key The encryption key to use.
   * @param key_length Length in bytes of the encryption key.
   * @param config Configuration parameters for the consolidation.
   */
  static void consolidate(
      const Context& ctx,
      const std::string& uri,
      tiledb_encryption_type_t encryption_type,
      const void* encryption_key,
      uint32_t key_length,
      const Config& config = Config()) {
    ctx.handle_error(tiledb_array_consolidate_with_key(
        ctx,
        uri.c_str(),
        encryption_type,
        encryption_key,
        key_length,
        config.ptr().get()));
  }

  // clang-format off
  /**
   * @copybrief Array::consolidate(const Context&,const std::string&,tiledb_encryption_type_t,const void*,uint32_t,const Config&)
   *
   * See @ref Array::consolidate(const Context&,const std::string&,tiledb_encryption_type_t,const void*,uint32_t,const Config&) "Array::consolidate"
   *
   * @param ctx TileDB context
   * @param array_uri The URI of the TileDB array to be consolidated.
   * @param encryption_type The encryption type to use.
   * @param encryption_key The encryption key to use.
   * @param config Configuration parameters for the consolidation.
   */
  // clang-format on
  static void consolidate(
      const Context& ctx,
      const std::string& uri,
      tiledb_encryption_type_t encryption_type,
      const std::string& encryption_key,
      const Config& config = Config()) {
    return consolidate(
        ctx,
        uri,
        encryption_type,
        encryption_key.data(),
        (uint32_t)encryption_key.size(),
        config);
  }

  /**
   * Creates a new TileDB array given an input schema.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Array::create("s3://bucket-name/array-name", schema);
   * @endcode
   *
   * @param uri URI where array will be created.
   * @param schema The array schema.
   */
  static void create(const std::string& uri, const ArraySchema& schema) {
    create(uri, schema, TILEDB_NO_ENCRYPTION, nullptr, 0);
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
  static void create(
      const std::string& uri,
      const ArraySchema& schema,
      tiledb_encryption_type_t encryption_type,
      const void* encryption_key,
      uint32_t key_length) {
    auto& ctx = schema.context();
    ctx.handle_error(tiledb_array_schema_check(ctx, schema));
    ctx.handle_error(tiledb_array_create_with_key(
        ctx, uri.c_str(), schema, encryption_type, encryption_key, key_length));
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
   * Gets the encryption type the given array was created with.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb_encryption_type_t enc_type;
   * tiledb::Array::encryption_type(ctx, "s3://bucket-name/array-name",
   *    &enc_type);
   * @endcode
   *
   * @param ctx TileDB context
   * @param array_uri The URI of the TileDB array to be consolidated.
   * @param encryption_type Set to the encryption type of the array.
   */
  static tiledb_encryption_type_t encryption_type(
      const Context& ctx, const std::string& array_uri) {
    tiledb_encryption_type_t encryption_type;
    ctx.handle_error(
        tiledb_array_encryption_type(ctx, array_uri.c_str(), &encryption_type));
    return encryption_type;
  }

  /**
   * Retrieves the non-empty domain from the array. This is the union of the
   * non-empty domains of the array fragments.
   *
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * // Specify the domain type (example uint32_t)
   * auto non_empty = array.non_empty_domain<uint32_t>();
   * std::cout << "Dimension named " << non_empty[0].first << " has cells in ["
   *           << non_empty[0].second.first << ", " non_empty[0].second.second
   *           << "]" << std::endl;
   * @endcode
   *
   * @tparam T Domain datatype
   * @return Vector of dim names with a {lower, upper} pair. Inclusive.
   *         Empty vector if the array has no data.
   */
  template <typename T>
  std::vector<std::pair<std::string, std::pair<T, T>>> non_empty_domain() {
    impl::type_check<T>(schema_.domain().type());
    std::vector<std::pair<std::string, std::pair<T, T>>> ret;

    auto dims = schema_.domain().dimensions();
    std::vector<T> buf(dims.size() * 2);
    int empty;

    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_get_non_empty_domain(
        ctx, array_.get(), buf.data(), &empty));

    if (empty)
      return ret;

    for (size_t i = 0; i < dims.size(); ++i) {
      auto domain = std::pair<T, T>(buf[i * 2], buf[(i * 2) + 1]);
      ret.push_back(
          std::pair<std::string, std::pair<T, T>>(dims[i].name(), domain));
    }

    return ret;
  }

  /**
   * Compute an upper bound on the buffer elements needed to read a subarray.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * std::vector<int> subarray = {0, 2, 0, 2};
   * auto max_elements = array.max_buffer_elements(subarray);
   *
   * // For fixed-sized attributes, `.second` is the max number of elements
   * // that can be read for the attribute. Use it to size the vector.
   * std::vector<int> data_a1(max_elements["a1"].second);
   *
   * // In sparse reads, coords are also fixed-sized attributes.
   * std::vector<int> coords(max_elements[TILEDB_COORDS].second);
   *
   * // In variable attributes, e.g. std::string type, need two buffers,
   * // one for offsets and one for cell data.
   * std::vector<uint64_t> offsets_a1(max_elements["a2"].first);
   * std::vector<char> data_a1(max_elements["a2"].second);
   * @endcode
   *
   * @tparam T The domain datatype
   * @param subarray Targeted subarray.
   * @return A map of attribute name (including `TILEDB_COORDS`) to
   *     the maximum number of elements that can be read in the given subarray.
   *     For each attribute, a pair of numbers are returned. The first,
   *     for variable-length attributes, is the maximum number of offsets
   *     for that attribute in the given subarray. For fixed-length attributes
   *     and coordinates, the first is always 0. The second is the maximum
   *     number of elements for that attribute in the given subarray.
   */
  template <typename T>
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
  max_buffer_elements(const std::vector<T>& subarray) {
    auto ctx = ctx_.get();
    impl::type_check<T>(schema_.domain().type(), 1);

    // Handle attributes
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> ret;
    auto schema_attrs = schema_.attributes();
    uint64_t attr_size, type_size;

    for (const auto& a : schema_attrs) {
      auto var = a.second.cell_val_num() == TILEDB_VAR_NUM;
      auto name = a.second.name();
      type_size = tiledb_datatype_size(a.second.type());

      if (var) {
        uint64_t size_off, size_val;
        ctx.handle_error(tiledb_array_max_buffer_size_var(
            ctx,
            array_.get(),
            name.c_str(),
            subarray.data(),
            &size_off,
            &size_val));
        ret[a.first] = std::pair<uint64_t, uint64_t>(
            size_off / TILEDB_OFFSET_SIZE, size_val / type_size);
      } else {
        ctx.handle_error(tiledb_array_max_buffer_size(
            ctx, array_.get(), name.c_str(), subarray.data(), &attr_size));
        ret[a.first] = std::pair<uint64_t, uint64_t>(0, attr_size / type_size);
      }
    }

    // Handle coordinates
    type_size = tiledb_datatype_size(schema_.domain().type());
    ctx.handle_error(tiledb_array_max_buffer_size(
        ctx, array_.get(), TILEDB_COORDS, subarray.data(), &attr_size));
    ret[TILEDB_COORDS] =
        std::pair<uint64_t, uint64_t>(0, attr_size / type_size);

    return ret;
  }

  /** Returns the query type the array was opened with. */
  tiledb_query_type_t query_type() const {
    auto& ctx = ctx_.get();
    tiledb_query_type_t query_type;
    ctx.handle_error(
        tiledb_array_get_query_type(ctx, array_.get(), &query_type));
    return query_type;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the TileDB C array object. */
  std::shared_ptr<tiledb_array_t> array_;

  /** The array schema. */
  ArraySchema schema_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_ARRAY_H
