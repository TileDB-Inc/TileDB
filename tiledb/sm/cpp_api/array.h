/**
 * @file   array.h
 *
 * @author Ravi Gaddipati
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
 * This file declares the C++ API for TileDB array operations.
 */

#ifndef TILEDB_CPP_API_ARRAY_H
#define TILEDB_CPP_API_ARRAY_H

#include "array_schema.h"
#include "context.h"
#include "tiledb.h"
#include "tiledb_experimental.h"
#include "type.h"

#include <unordered_map>
#include <vector>

namespace tiledb {

/** Marker class to enforce a TimeTravel TemporalPolicy. */
class TimeTravelMarker {};
static constexpr TimeTravelMarker TimeTravel{};

/** Marker class to enforce a TimestampStartEnd TemporalPolicy. */
class TimestampStartEndMarker {};
static constexpr TimestampStartEndMarker TimestampStartEnd{};

/**
 * Class representing a TileDB temporal policy object.
 *
 * @details
 * A TemporalPolicy object dictates the timestamp usage with which an Array is
 * opened. TimeTravel represents opening at a single point in time, while
 * TimestampStartEnd will open between the two specified times.
 * Note that this class was created specifically to maintain timestamps for the
 * Array class, but its semantics are not specific to an Array.
 *
 * **Example:**
 *
 * @code{.cpp}
 * // Open an array with TimeTravel
 * tiledb::Context ctx;
 * tiledb::Array array_time_travel(
 *    ctx, "s3://bucket/array", TILEDB_WRITE, TemporalPolicy(TimeTravel, 1));
 *
 * // Open an array with TimestampStartEnd
 * TemporalPolicy tp_time_range(TimestampStartEnd, 0, 1);
 * tiledb::Context ctx;
 * tiledb::Array array_time_range(
 *    ctx, "s3://bucket/array", TILEDB_WRITE, tp_time_range);
 * @endcode
 */
class TemporalPolicy {
  /**
   * Make available the internals of TemporalPolicy to Array.
   */
  friend class Array;

 public:
  TemporalPolicy()
      : timestamp_start_(0)
      , timestamp_end_(UINT64_MAX){};

  TemporalPolicy(const TimeTravelMarker&, uint64_t timestamp)
      : timestamp_start_(0)
      , timestamp_end_(timestamp){};

  TemporalPolicy(
      const TimestampStartEndMarker&,
      uint64_t timestamp_start,
      uint64_t timestamp_end)
      : timestamp_start_(timestamp_start)
      , timestamp_end_(timestamp_end){};

 private:
  inline uint64_t timestamp_start() const {
    return timestamp_start_;
  }

  inline uint64_t timestamp_end() const {
    return timestamp_end_;
  }

  const uint64_t timestamp_start_;
  const uint64_t timestamp_end_;
};

/** Marker class to enforce an AES_256_GCM encryption type. */
class AESGCMEncryptionTypeMarker {};
static constexpr AESGCMEncryptionTypeMarker AESGCM{};

/**
 * Class representing a TileDB encryption algorithm object.
 *
 * @details
 * An EncryptionAlgorithm represents a pair of encryption type and key.
 *
 * @code{.cpp}
 * // Open an array with an EncryptionAlgorithm
 * tiledb::Context ctx;
 * std::string key = "0123456789";
 * tiledb::Array array(
 *    ctx,
 *    "s3://bucket/array",
 *    TILEDB_WRITE,
 *    TemporalPolicy(),
 *    EncryptionAlgorithm(AESGCM, key));
 * @endcode
 */
class EncryptionAlgorithm {
  /**
   * Make available the internals of EncryptionAlgorithm to Array.
   */
  friend class Array;

 public:
  EncryptionAlgorithm()
      : type_(TILEDB_NO_ENCRYPTION)
      , key_(nullptr){};

  EncryptionAlgorithm(tiledb_encryption_type_t type, const char* key)
      : type_(type)
      , key_(key){};

  EncryptionAlgorithm(const AESGCMEncryptionTypeMarker&, const char* key)
      : type_(TILEDB_AES_256_GCM)
      , key_(key){};

 private:
  inline tiledb_encryption_type_t type() const {
    return type_;
  }

  inline const char* key() const {
    return key_;
  }

  const tiledb_encryption_type_t type_;
  const char* key_;
};

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
   * @param temporal_policy The TemporalPolicy with which to open the array.
   * @param encryption_algorithm The EncryptionAlgorithm to set on the array.
   */
  Array(
      const Context& ctx,
      const std::string& array_uri,
      tiledb_query_type_t query_type,
      const TemporalPolicy temporal_policy = {},
      const EncryptionAlgorithm encryption_algorithm = {})
      : ctx_(ctx)
      , schema_(ArraySchema(ctx, (tiledb_array_schema_t*)nullptr)) {
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    tiledb_array_t* array;
    ctx.handle_error(tiledb_array_alloc(c_ctx, array_uri.c_str(), &array));
    array_ = std::shared_ptr<tiledb_array_t>(array, deleter_);

    ctx.handle_error(tiledb_array_set_open_timestamp_start(
        c_ctx, array, temporal_policy.timestamp_start()));
    ctx.handle_error(tiledb_array_set_open_timestamp_end(
        c_ctx, array, temporal_policy.timestamp_end()));

    if (encryption_algorithm.key()) {
      auto config = ctx.config();
      const char* enc_type_str;
      tiledb_encryption_type_to_str(encryption_algorithm.type(), &enc_type_str);
      config.set("sm.encryption_type", enc_type_str);
      config.set("sm.encryption_key", encryption_algorithm.key());
      ctx.handle_error(
          tiledb_array_set_config(c_ctx, array, config.ptr().get()));
    }

    ctx.handle_error(tiledb_array_open(c_ctx, array, query_type));
    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(c_ctx, array, &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  /**
   * @brief Constructor. This sets the array config.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb_config_t* config;
   * @endcode
   *
   * @param ctx TileDB context.
   * @param carray The array.
   * @param config The array's config.
   */
  Array(const Context& ctx, tiledb_array_t* carray, tiledb_config_t* config)
      : ctx_(ctx)
      , schema_(ArraySchema(ctx, (tiledb_array_schema_t*)nullptr)) {
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_array_set_config(c_ctx, carray, config));
  }

  /**
   * Constructor. Creates a TileDB Array instance wrapping the given pointer.
   * @param ctx tiledb::Context
   * @param own=true If false, disables underlying cleanup upon destruction.
   * @throws TileDBError if construction fails
   */
  Array(const Context& ctx, tiledb_array_t* carray, bool own = true)
      : ctx_(ctx)
      , schema_(ArraySchema(ctx, (tiledb_array_schema_t*)nullptr)) {
    if (carray == nullptr)
      throw TileDBError(
          "[TileDB::C++API] Error: Failed to create Array from null pointer");

    tiledb_ctx_t* c_ctx = ctx.ptr().get();

    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(c_ctx, carray, &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
    owns_c_ptr_ = own;

    array_ = std::shared_ptr<tiledb_array_t>(carray, [own](tiledb_array_t* p) {
      if (own) {
        tiledb_array_free(&p);
      }
    });
  }

  Array(const Array&) = default;
  Array(Array&&) = default;
  Array& operator=(const Array&) = default;
  Array& operator=(Array&&) = default;

  /** Destructor; calls `close()`. */
  ~Array() {
    if (owns_c_ptr_ && is_open()) {
      close();
    }
  }

  /** Checks if the array is open. */
  bool is_open() const {
    auto& ctx = ctx_.get();
    int open = 0;
    ctx.handle_error(
        tiledb_array_is_open(ctx.ptr().get(), array_.get(), &open));
    return bool(open);
  }

  /** Returns the array URI. */
  std::string uri() const {
    auto& ctx = ctx_.get();
    const char* uri = nullptr;
    ctx.handle_error(tiledb_array_get_uri(ctx.ptr().get(), array_.get(), &uri));
    return std::string(uri);
  }

  /** Get the ArraySchema for the array. **/
  ArraySchema schema() const {
    auto& ctx = ctx_.get();
    tiledb_array_schema_t* schema;
    ctx.handle_error(
        tiledb_array_get_schema(ctx.ptr().get(), array_.get(), &schema));
    return ArraySchema(ctx, schema);
  }

  /** Returns a shared pointer to the C TileDB array object. */
  std::shared_ptr<tiledb_array_t> ptr() const {
    return array_;
  }

  /**
   * Deletes all data written to the array with the input uri.
   *
   * @param ctx TileDB context
   * @param uri The Array's URI
   *
   * @post This is destructive; the array may not be reopened after delete.
   */
  static void delete_array(const Context& ctx, const std::string& uri) {
    ctx.handle_error(tiledb_array_delete(ctx.ptr().get(), uri.c_str()));
  }

  /**
   * Deletes the fragments written between the input timestamps of an array
   * with the input uri.
   *
   * @param ctx TileDB context
   * @param uri The URI of the fragments' parent Array.
   * @param timestamp_start The epoch start timestamp in milliseconds.
   * @param timestamp_end The epoch end timestamp in milliseconds. Use
   * UINT64_MAX for the current timestamp.
   */
  static void delete_fragments(
      const Context& ctx,
      const std::string& uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end) {
    ctx.handle_error(tiledb_array_delete_fragments_v2(
        ctx.ptr().get(), uri.c_str(), timestamp_start, timestamp_end));
  }

  /**
   * Deletes the fragments with the input uris on an array with the input uri.
   */
  static void delete_fragments_list(
      const Context& ctx,
      const std::string& uri,
      const char* fragment_uris[],
      const size_t num_fragments) {
    ctx.handle_error(tiledb_array_delete_fragments_list(
        ctx.ptr().get(), uri.c_str(), fragment_uris, num_fragments));
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
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_array_open(c_ctx, array_.get(), query_type));
    tiledb_array_schema_t* array_schema;
    ctx.handle_error(
        tiledb_array_get_schema(c_ctx, array_.get(), &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  // clang-format off
  /**
   * @copybrief Array::open(tiledb_query_type_t)
   *
   * See @ref Array::open(tiledb_query_type_t) "Array::open"
   */
  // clang-format on
  void open(tiledb_query_type_t query_type, uint64_t timestamp) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();

    ctx.handle_error(
        tiledb_array_set_open_timestamp_end(c_ctx, array_.get(), timestamp));
    open(query_type);
  }

  // clang-format off
  /**
   * @copybrief Array::open(tiledb_query_type_t)
   *
   * See @ref Array::open(tiledb_query_type_t) "Array::open"
   */
  // clang-format on
  void open(
      tiledb_query_type_t query_type,
      tiledb_encryption_type_t encryption_type,
      const std::string& encryption_key) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    auto config = ctx.config();

    const char* enc_type_str;
    tiledb_encryption_type_to_str(encryption_type, &enc_type_str);
    config.set("sm.encryption_type", enc_type_str);
    config.set("sm.encryption_key", encryption_key);
    ctx.handle_error(
        tiledb_array_set_config(c_ctx, array_.get(), config.ptr().get()));

    open(query_type);
  }

  // clang-format off
  /**
   * @copybrief Array::open(tiledb_query_type_t,tiledb_encryption_type_t,const std::string&)
   *
   * See @ref Array::open(tiledb_query_type_t,tiledb_encryption_type_t,const std::string&) "Array::open"
   */
  // clang-format on
  void open(
      tiledb_query_type_t query_type,
      tiledb_encryption_type_t encryption_type,
      const std::string& encryption_key,
      uint64_t timestamp) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();

    ctx.handle_error(
        tiledb_array_set_open_timestamp_end(c_ctx, array_.get(), timestamp));
    open(query_type, encryption_type, encryption_key);
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
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_array_reopen(c_ctx, array_.get()));
    tiledb_array_schema_t* array_schema;
    ctx.handle_error(
        tiledb_array_get_schema(c_ctx, array_.get(), &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  /** Sets the inclusive starting timestamp when opening this array. */
  void set_open_timestamp_start(uint64_t timestamp_start) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_set_open_timestamp_start(
        ctx.ptr().get(), array_.get(), timestamp_start));
  }

  /** Sets the inclusive ending timestamp when opening this array. */
  void set_open_timestamp_end(uint64_t timestamp_end) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_set_open_timestamp_end(
        ctx.ptr().get(), array_.get(), timestamp_end));
  }

  /** Retrieves the inclusive starting timestamp. */
  uint64_t open_timestamp_start() const {
    auto& ctx = ctx_.get();
    uint64_t timestamp_start;
    ctx.handle_error(tiledb_array_get_open_timestamp_start(
        ctx.ptr().get(), array_.get(), &timestamp_start));
    return timestamp_start;
  }

  /** Retrieves the inclusive ending timestamp. */
  uint64_t open_timestamp_end() const {
    auto& ctx = ctx_.get();
    uint64_t timestamp_end;
    ctx.handle_error(tiledb_array_get_open_timestamp_end(
        ctx.ptr().get(), array_.get(), &timestamp_end));
    return timestamp_end;
  }

  /**
   * Sets the array config.
   *
   * @pre The array must be closed.
   */
  void set_config(const Config& config) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_set_config(
        ctx.ptr().get(), array_.get(), config.ptr().get()));
  }

  /** Retrieves the array config. */
  Config config() const {
    auto& ctx = ctx_.get();
    tiledb_config_t* config = nullptr;
    ctx.handle_error(
        tiledb_array_get_config(ctx.ptr().get(), array_.get(), &config));
    return Config(&config);
  }

  /**
   * Closes the array. The destructor calls this automatically
   * if the underlying pointer is owned.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * array.close();
   * @endcode
   */
  void close() {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_close(ctx.ptr().get(), array_.get()));
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
      Config* const config = nullptr) {
    ctx.handle_error(tiledb_array_consolidate(
        ctx.ptr().get(), uri.c_str(), config ? config->ptr().get() : nullptr));
  }

  /**
   * @brief Consolidates the fragments with the input uris into a single
   * fragment.
   *
   * You must first finalize all queries to the array before consolidation can
   * begin (as consolidation temporarily acquires an exclusive lock on the
   * array).
   *
   * **Example:**
   * @code{.cpp}
   * const char* fragment_uris[2] = {
   * "__1712657401931_1712657401931_285cf8a0eff4df875a04cfbea96d5c00_21",
   * "__1712657401948_1712657401948_285cf8a0efdsafas6a5a04cfbesajads_21"};
   *
   * tiledb::Array::consolidate(
   *     ctx,
   *     "s3://bucket-name/array-name",
   *      fragment_uris,
   *      2,
   *      config);
   * @endcode
   *
   * @param ctx TileDB context
   * @param array_uri The URI of the TileDB array to be consolidated.
   * @param fragment_uris Fragment names of the fragments to consolidate. The
   *     names can be recovered using tiledb_fragment_info_get_fragment_name_v2.
   * @param num_fragments The number of fragments to consolidate.
   * @param config Configuration parameters for the consolidation.
   */
  static void consolidate(
      const Context& ctx,
      const std::string& array_uri,
      const char* fragment_uris[],
      const size_t num_fragments,
      Config* const config = nullptr) {
    ctx.handle_error(tiledb_array_consolidate_fragments(
        ctx.ptr().get(),
        array_uri.c_str(),
        fragment_uris,
        num_fragments,
        config ? config->ptr().get() : nullptr));
  }

  /**
   * Cleans up the array, such as consolidated fragments and array metadata.
   * Note that this will coarsen the granularity of time traveling (see docs
   * for more information).
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Array::vacuum(ctx, "s3://bucket-name/array-name");
   * @endcode
   *
   * @param ctx TileDB context
   * @param array_uri The URI of the TileDB array to be vacuumed.
   * @param config Configuration parameters for the vacuuming.
   */
  static void vacuum(
      const Context& ctx,
      const std::string& uri,
      Config* const config = nullptr) {
    ctx.handle_error(tiledb_array_vacuum(
        ctx.ptr().get(), uri.c_str(), config ? config->ptr().get() : nullptr));
  }

  /**
   * Creates a new TileDB array given an input schema.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Array::create(ctx, "s3://bucket-name/array-name", schema);
   * @endcode
   *
   * @param ctx The TileDB context.
   * @param uri URI where array will be created.
   * @param schema The array schema.
   */
  static void create(
      const Context& ctx, const std::string& uri, const ArraySchema& schema) {
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_array_schema_check(c_ctx, schema.ptr().get()));
    ctx.handle_error(
        tiledb_array_create(c_ctx, uri.c_str(), schema.ptr().get()));
  }

  /**
   * Creates a new TileDB array given an input schema.
   *
   * To create the array, this function uses the context that was used to
   * instantiate the schema. You are recommended to explicitly pass it with the
   * overload that takes a context.
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
    create(schema.context(), uri, schema);
  }

  /**
   * Loads the array schema from an array.
   *
   * **Example:**
   * @code{.cpp}
   * auto schema = tiledb::Array::load_schema(ctx,
   * "s3://bucket-name/array-name");
   * @endcode
   *
   * @param ctx The TileDB context.
   * @param uri The array URI.
   * @return The loaded ArraySchema object.
   */
  static ArraySchema load_schema(const Context& ctx, const std::string& uri) {
    tiledb_array_schema_t* schema;
    ctx.handle_error(
        tiledb_array_schema_load(ctx.ptr().get(), uri.c_str(), &schema));
    return ArraySchema(ctx, schema);
  }

  /**
   * Loads the array schema from an array.
   * Options to load additional features are read from the optionally-provided
   * `config`. See `tiledb_array_schema_load_with_config`.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Config config;
   * config["rest.load_enumerations_on_array_open"] = "true";
   * auto schema = tiledb::Array::load_schema_with_config(ctx, config,
   * "s3://bucket-name/array-name");
   * @endcode
   *
   * @param ctx The TileDB context.
   * @param config The request for additional features.
   * @param uri The array URI.
   * @return The loaded ArraySchema object.
   */
  static ArraySchema load_schema_with_config(
      const Context& ctx, const Config& config, const std::string& uri) {
    tiledb_array_schema_t* schema;
    ctx.handle_error(tiledb_array_schema_load_with_config(
        ctx.ptr().get(), config.ptr().get(), uri.c_str(), &schema));
    return ArraySchema(ctx, schema);
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
    ctx.handle_error(tiledb_array_encryption_type(
        ctx.ptr().get(), array_uri.c_str(), &encryption_type));
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
        ctx.ptr().get(), array_.get(), buf.data(), &empty));

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
   * Retrieves the non-empty domain from the array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   *
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * // Specify the dimension type (example uint32_t)
   * auto non_empty = array.non_empty_domain<uint32_t>(0);
   * @endcode
   *
   * @tparam T Dimension datatype
   * @param idx The dimension index.
   * @return The {lower, upper} pair of the non-empty domain (inclusive)
   *         on the input dimension.
   */
  template <typename T>
  std::pair<T, T> non_empty_domain(unsigned idx) {
    auto dim = schema_.domain().dimension(idx);
    impl::type_check<T>(dim.type());
    std::pair<T, T> ret;
    std::vector<T> buf(2);
    int empty;

    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_get_non_empty_domain_from_index(
        ctx.ptr().get(), array_.get(), idx, buf.data(), &empty));

    if (empty)
      return ret;

    ret = std::pair<T, T>(buf[0], buf[1]);
    return ret;
  }

  /**
   * Retrieves the non-empty domain from the array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   *
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * // Specify the dimension type (example uint32_t)
   * auto non_empty = array.non_empty_domain<uint32_t>("d1");
   * @endcode
   *
   * @tparam T Dimension datatype
   * @param name The dimension name.
   * @return The {lower, upper} pair of the non-empty domain (inclusive)
   *         on the input dimension.
   */
  template <typename T>
  std::pair<T, T> non_empty_domain(const std::string& name) {
    auto dim = schema_.domain().dimension(name);
    impl::type_check<T>(dim.type());
    std::pair<T, T> ret;
    std::vector<T> buf(2);
    int empty;

    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_get_non_empty_domain_from_name(
        ctx.ptr().get(), array_.get(), name.c_str(), buf.data(), &empty));

    if (empty)
      return ret;

    ret = std::pair<T, T>(buf[0], buf[1]);
    return ret;
  }

  /**
   * Retrieves the non-empty domain from the array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   * Applicable only to var-sized dimensions.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * // Specify the dimension type (example uint32_t)
   * auto non_empty = array.non_empty_domain_var(0);
   * @endcode
   *
   * @param idx The dimension index.
   * @return The {lower, upper} pair of the non-empty domain (inclusive)
   *         on the input dimension.
   */
  std::pair<std::string, std::string> non_empty_domain_var(unsigned idx) {
    auto dim = schema_.domain().dimension(idx);
    impl::type_check<char>(dim.type());
    std::pair<std::string, std::string> ret;

    // Get range sizes
    uint64_t start_size, end_size;
    int empty;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_get_non_empty_domain_var_size_from_index(
        ctx.ptr().get(), array_.get(), idx, &start_size, &end_size, &empty));

    if (empty)
      return ret;

    // Get ranges
    ret.first.resize(start_size);
    ret.second.resize(end_size);
    ctx.handle_error(tiledb_array_get_non_empty_domain_var_from_index(
        ctx.ptr().get(),
        array_.get(),
        idx,
        &(ret.first[0]),
        &(ret.second[0]),
        &empty));

    return ret;
  }

  /**
   * Retrieves the non-empty domain from the array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   * Applicable only to var-sized dimensions.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, "s3://bucket-name/array-name", TILEDB_READ);
   * // Specify the dimension type (example uint32_t)
   * auto non_empty = array.non_empty_domain_var("d1");
   * @endcode
   *
   * @param name The dimension name.
   * @return The {lower, upper} pair of the non-empty domain (inclusive)
   *         on the input dimension.
   */
  std::pair<std::string, std::string> non_empty_domain_var(
      const std::string& name) {
    auto dim = schema_.domain().dimension(name);
    impl::type_check<char>(dim.type());
    std::pair<std::string, std::string> ret;

    // Get range sizes
    uint64_t start_size, end_size;
    int empty;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_get_non_empty_domain_var_size_from_name(
        ctx.ptr().get(),
        array_.get(),
        name.c_str(),
        &start_size,
        &end_size,
        &empty));

    if (empty)
      return ret;

    // Get ranges
    ret.first.resize(start_size);
    ret.second.resize(end_size);
    ctx.handle_error(tiledb_array_get_non_empty_domain_var_from_name(
        ctx.ptr().get(),
        array_.get(),
        name.c_str(),
        &(ret.first[0]),
        &(ret.second[0]),
        &empty));

    return ret;
  }

  /** Returns the query type the array was opened with. */
  tiledb_query_type_t query_type() const {
    auto& ctx = ctx_.get();
    tiledb_query_type_t query_type;
    ctx.handle_error(tiledb_array_get_query_type(
        ctx.ptr().get(), array_.get(), &query_type));
    return query_type;
  }

  /**
   * @brief Consolidates the metadata of an array.
   *
   * You must first finalize all queries to the array before consolidation can
   * begin (as consolidation temporarily acquires an exclusive lock on the
   * array).
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Array::consolidate_metadata(ctx, "s3://bucket-name/array-name");
   * @endcode
   *
   * @param ctx TileDB context
   * @param array_uri The URI of the TileDB array whose
   *     metadata will be consolidated.
   * @param config Configuration parameters for the consolidation.
   */
  static void consolidate_metadata(
      const Context& ctx,
      const std::string& uri,
      Config* const config = nullptr) {
    Config local_cfg;
    Config* config_aux = config;
    if (!config_aux) {
      config_aux = &local_cfg;
    }

    (*config_aux)["sm.consolidation.mode"] = "array_meta";
    consolidate(ctx, uri, config_aux);
  }

  /**
   * @brief Upgrades an array to the latest format version.
   *
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Array::upgrade_version(ctx, "array_name");
   * @endcode
   *
   * @param ctx TileDB context
   * @param array_uri The URI of the TileDB array to be upgraded.
   * @param config Configuration parameters for the upgrade.
   */
  static void upgrade_version(
      const Context& ctx,
      const std::string& array_uri,
      Config* const config = nullptr) {
    ctx.handle_error(tiledb_array_upgrade_version(
        ctx.ptr().get(),
        array_uri.c_str(),
        config ? config->ptr().get() : nullptr));
  }

  /**
   * It puts a metadata key-value item to an open array. The array must
   * be opened in WRITE mode, otherwise the function will error out.
   *
   * @param key The key of the metadata item to be added. UTF-8 encodings
   *     are acceptable.
   * @param value_type The datatype of the value.
   * @param value_num The value may consist of more than one items of the
   *     same datatype. This argument indicates the number of items in the
   *     value component of the metadata.
   * @param value The metadata value in binary form.
   *
   * @note The writes will take effect only upon closing the array.
   */
  void put_metadata(
      const std::string& key,
      tiledb_datatype_t value_type,
      uint32_t value_num,
      const void* value) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_array_put_metadata(
        c_ctx, array_.get(), key.c_str(), value_type, value_num, value));
  }

  /**
   * It deletes a metadata key-value item from an open array. The array must
   * be opened in WRITE mode, otherwise the function will error out.
   *
   * @param key The key of the metadata item to be deleted.
   *
   * @note The writes will take effect only upon closing the array.
   *
   * @note If the key does not exist, this will take no effect
   *     (i.e., the function will not error out).
   */
  void delete_metadata(const std::string& key) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(
        tiledb_array_delete_metadata(c_ctx, array_.get(), key.c_str()));
  }

  /**
   * It gets a metadata key-value item from an open array. The array must
   * be opened in READ mode, otherwise the function will error out.
   *
   * @param key The key of the metadata item to be retrieved. UTF-8 encodings
   *     are acceptable.
   * @param value_type The datatype of the value.
   * @param value_num The value may consist of more than one items of the
   *     same datatype. This argument indicates the number of items in the
   *     value component of the metadata. Keys with empty values are indicated
   *     by value_num == 1 and value == NULL.
   * @param value The metadata value in binary form.
   *
   * @note If the key does not exist, then `value` will be NULL.
   */
  void get_metadata(
      const std::string& key,
      tiledb_datatype_t* value_type,
      uint32_t* value_num,
      const void** value) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_array_get_metadata(
        c_ctx, array_.get(), key.c_str(), value_type, value_num, value));
  }

  /**
   * Checks if key exists in metadata from an open array. The array must
   * be opened in READ mode, otherwise the function will error out.
   *
   * @param key The key of the metadata item to be retrieved. UTF-8 encodings
   *     are acceptable.
   * @param value_type The datatype of the value associated with the key (if
   * any).
   * @return true if the key exists, else false.
   * @note If the key does not exist, then `value_type` will not be modified.
   */
  bool has_metadata(const std::string& key, tiledb_datatype_t* value_type) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    int32_t has_key;
    ctx.handle_error(tiledb_array_has_metadata_key(
        c_ctx, array_.get(), key.c_str(), value_type, &has_key));
    return has_key == 1;
  }

  /**
   * Returns then number of metadata items in an open array. The array must
   * be opened in READ mode, otherwise the function will error out.
   */
  uint64_t metadata_num() const {
    uint64_t num;
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_array_get_metadata_num(c_ctx, array_.get(), &num));
    return num;
  }

  /**
   * It gets a metadata item from an open array using an index.
   * The array must be opened in READ mode, otherwise the function will
   * error out.
   *
   * @param index The index used to get the metadata.
   * @param key The metadata key.
   * @param value_type The datatype of the value.
   * @param value_num The value may consist of more than one items of the
   *     same datatype. This argument indicates the number of items in the
   *     value component of the metadata. Keys with empty values are indicated
   *     by value_num == 1 and value == NULL.
   * @param value The metadata value in binary form.
   */
  void get_metadata_from_index(
      uint64_t index,
      std::string* key,
      tiledb_datatype_t* value_type,
      uint32_t* value_num,
      const void** value) {
    const char* key_c;
    uint32_t key_len;
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_array_get_metadata_from_index(
        c_ctx,
        array_.get(),
        index,
        &key_c,
        &key_len,
        value_type,
        value_num,
        value));
    key->resize(key_len);
    std::memcpy((void*)key->data(), key_c, key_len);
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

  /** Flag indicating ownership of the TileDB C array object */
  bool owns_c_ptr_ = true;

  /** The array schema. */
  ArraySchema schema_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_ARRAY_H
