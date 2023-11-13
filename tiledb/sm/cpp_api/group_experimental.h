/**
 * @file   group_experimental.h
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
 * This file declares the C++ API for the TileDB groups.
 */

#ifndef TILEDB_CPP_API_GROUP_EXPERIMENTAL_H
#define TILEDB_CPP_API_GROUP_EXPERIMENTAL_H

#include "context.h"
#include "object.h"
#include "tiledb.h"

namespace tiledb {
class Group {
 public:
  /**
   * @brief Constructor. Opens the group for the given query type. The
   * destructor calls the `close()` method.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Open the group for reading
   * tiledb::Context ctx;
   * tiledb::Group group(ctx, "s3://bucket-name/group-name", TILEDB_READ);
   * @endcode
   *
   * @param ctx TileDB context.
   * @param group_uri The group URI.
   * @param query_type Query type to open the group for.
   */
  Group(
      const Context& ctx,
      const std::string& group_uri,
      tiledb_query_type_t query_type)
      : Group(ctx, group_uri, query_type, nullptr) {
  }

  /**
   * @brief Constructor. Sets a config to the group and opens it for the given
   * query type. The destructor calls the `close()` method.
   *
   * **Example:**
   *
   * @code{.cpp}
   * // Open the group for reading
   * tiledb::Context ctx;
   * tiledb::Config cfg;
   * cfg["rest.username"] = "user";
   * cfg["rest.password"] = "pass";
   * tiledb::Group group(ctx, "s3://bucket-name/group-name", TILEDB_READ, cfg);
   * @endcode
   *
   * @param ctx TileDB context.
   * @param group_uri The group URI.
   * @param query_type Query type to open the group for.
   * @param config COnfiguration parameters
   */
  Group(
      const Context& ctx,
      const std::string& group_uri,
      tiledb_query_type_t query_type,
      const Config& config)
      : Group(ctx, group_uri, query_type, config.ptr().get()) {
  }

  Group(const Group&) = default;
  Group(Group&&) = default;
  Group& operator=(const Group&) = default;
  Group& operator=(Group&&) = default;

  /** Destructor; calls `close()`. */
  ~Group() {
    if (owns_c_ptr_ && is_open()) {
      close(false);
    }
  }

  /**
   * @brief Opens the group using a query type as input.
   *
   * This is to indicate that queries created for this `Group`
   * object will inherit the query type. In other words, `Group`
   * objects are opened to receive only one type of queries.
   * They can always be closed and be re-opened with another query type.
   * Also there may be many different `Group`
   * objects created and opened with different query types. For
   * instance, one may create and open an group object `group_read` for
   * reads and another one `group_write` for writes, and interleave
   * creation and submission of queries for both these group objects.
   *
   * **Example:**
   * @code{.cpp}
   * // Open the group for writing
   * tiledb::Group group(ctx, "s3://bucket-name/group-name", TILEDB_WRITE);
   * // Close and open again for reading.
   * group.close();
   * group.open(TILEDB_READ);
   * @endcode
   *
   * @param query_type The type of queries the group object will be receiving.
   * @throws TileDBError if the group is already open or other error occurred.
   */
  void open(tiledb_query_type_t query_type) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_group_open(c_ctx, group_.get(), query_type));
  }

  /**
   * Sets the group config.
   *
   * @pre The group must be closed.
   */
  void set_config(const Config& config) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_group_set_config(
        ctx.ptr().get(), group_.get(), config.ptr().get()));
  }

  /** Retrieves the group config. */
  Config config() const {
    auto& ctx = ctx_.get();
    tiledb_config_t* config = nullptr;
    ctx.handle_error(
        tiledb_group_get_config(ctx.ptr().get(), group_.get(), &config));
    return Config(&config);
  }

  /**
   * Closes the group. This must be called directly if you wish to check that
   * any changes to the group were committed. This is automatically called
   * by the destructor but any errors encountered are logged instead of throwing
   * an exception from a destructor.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Group group(ctx, "s3://bucket-name/group-name", TILEDB_READ);
   * group.close();
   * @endcode
   */
  void close(bool should_throw = true) {
    auto& ctx = ctx_.get();
    auto rc = tiledb_group_close(ctx.ptr().get(), group_.get());
    if (rc != TILEDB_OK && should_throw) {
      ctx.handle_error(rc);
    } else if (rc != TILEDB_OK) {
      auto msg = ctx.get_last_error_message();
      tiledb_log_warn(ctx.ptr().get(), msg.c_str());
    }
  }

  /**
   *
   * Create a TileDB Group
   *
   * * **Example:**
   * @code{.cpp}
   * tiledb::Group::create(ctx, "s3://bucket-name/group-name");
   * @endcode
   *
   * @param ctx tiledb context
   * @param uri URI where group will be created.
   */
  // clang-format on
  static void create(const tiledb::Context& ctx, const std::string& uri) {
    ctx.handle_error(tiledb_group_create(ctx.ptr().get(), uri.c_str()));
  }

  /** Checks if the group is open. */
  bool is_open() const {
    auto& ctx = ctx_.get();
    int open = 0;
    ctx.handle_error(
        tiledb_group_is_open(ctx.ptr().get(), group_.get(), &open));
    return bool(open);
  }

  /** Returns the group URI. */
  std::string uri() const {
    auto& ctx = ctx_.get();
    const char* uri = nullptr;
    ctx.handle_error(tiledb_group_get_uri(ctx.ptr().get(), group_.get(), &uri));
    return std::string(uri);
  }

  /** Returns the query type the group was opened with. */
  tiledb_query_type_t query_type() const {
    auto& ctx = ctx_.get();
    tiledb_query_type_t query_type;
    ctx.handle_error(tiledb_group_get_query_type(
        ctx.ptr().get(), group_.get(), &query_type));
    return query_type;
  }

  /**
   * Puts a metadata key-value item to an open group. The group must
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
   * @note The writes will take effect only upon closing the group.
   */
  void put_metadata(
      const std::string& key,
      tiledb_datatype_t value_type,
      uint32_t value_num,
      const void* value) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_group_put_metadata(
        c_ctx, group_.get(), key.c_str(), value_type, value_num, value));
  }

  /**
   * Deletes all written data from an open group. The group must
   * be opened in MODIFY_EXCLUSIVE mode, otherwise the function will error out.
   *
   * @param uri The address of the group item to be deleted.
   * @param recursive True if all data inside the group is to be deleted.
   *
   * @note if recursive == false, data added to the group will be left as-is.
   * @post This is destructive; the group may not be reopened after delete.
   */
  void delete_group(const std::string& uri, bool recursive = false) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(
        tiledb_group_delete_group(c_ctx, group_.get(), uri.c_str(), recursive));
  }

  /**
   * Deletes a metadata key-value item from an open group. The group must
   * be opened in WRITE mode, otherwise the function will error out.
   *
   * @param key The key of the metadata item to be deleted.
   *
   * @note The writes will take effect only upon closing the group.
   *
   * @note If the key does not exist, this will take no effect
   *     (i.e., the function will not error out).
   */
  void delete_metadata(const std::string& key) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(
        tiledb_group_delete_metadata(c_ctx, group_.get(), key.c_str()));
  }

  /**
   * Gets a metadata key-value item from an open group. The group must
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
    ctx.handle_error(tiledb_group_get_metadata(
        c_ctx, group_.get(), key.c_str(), value_type, value_num, value));
  }

  /**
   * Checks if key exists in metadata from an open group. The group must
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
    ctx.handle_error(tiledb_group_has_metadata_key(
        c_ctx, group_.get(), key.c_str(), value_type, &has_key));
    return has_key == 1;
  }

  /**
   * Returns then number of metadata items in an open group. The group must
   * be opened in READ mode, otherwise the function will error out.
   */
  uint64_t metadata_num() const {
    uint64_t num;
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_group_get_metadata_num(c_ctx, group_.get(), &num));
    return num;
  }

  /**
   * Gets a metadata item from an open group using an index.
   * The group must be opened in READ mode, otherwise the function will
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
    ctx.handle_error(tiledb_group_get_metadata_from_index(
        c_ctx,
        group_.get(),
        index,
        &key_c,
        &key_len,
        value_type,
        value_num,
        value));
    key->resize(key_len);
    std::memcpy((void*)key->data(), key_c, key_len);
  }

  /**
   *
   * Add a member to a group
   *
   * @param uri of member to add
   * @param relative is the URI relative to the group location
   */
  void add_member(
      const std::string& uri,
      const bool& relative,
      std::optional<std::string> name = std::nullopt) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    const char* name_cstr = nullptr;
    if (name.has_value()) {
      name_cstr = name->c_str();
    }
    ctx.handle_error(tiledb_group_add_member(
        c_ctx, group_.get(), uri.c_str(), relative, name_cstr));
  }

  /**
   * Remove a member from a group
   *
   * @param name_or_uri Name or URI of member to remove. If the URI is
   * registered multiple times in the group, the name needs to be specified so
   * that the correct one can be removed. Note that if a URI is registered as
   * both a named and unnamed member, the unnamed member will be removed
   * successfully using the URI.
   */
  void remove_member(const std::string& name_or_uri) {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(
        tiledb_group_remove_member(c_ctx, group_.get(), name_or_uri.c_str()));
  }

  uint64_t member_count() const {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    uint64_t count = 0;
    ctx.handle_error(
        tiledb_group_get_member_count(c_ctx, group_.get(), &count));
    return count;
  }

  tiledb::Object member(uint64_t index) const {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    char* uri;
    tiledb_object_t type;
    char* name;
    ctx.handle_error(tiledb_group_get_member_by_index(
        c_ctx, group_.get(), index, &uri, &type, &name));
    std::string uri_str(uri);
    std::free(uri);
    std::optional<std::string> name_opt = std::nullopt;
    if (name != nullptr) {
      name_opt = name;
      std::free(name);
    }
    return tiledb::Object(type, uri_str, name_opt);
  }

  tiledb::Object member(std::string name) const {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    char* uri;
    tiledb_object_t type;
    ctx.handle_error(tiledb_group_get_member_by_name(
        c_ctx, group_.get(), name.c_str(), &uri, &type));
    std::string uri_str(uri);
    std::free(uri);
    return tiledb::Object(type, uri_str, name);
  }

  /**
   * retrieve the relative attribute for a named member
   *
   * @param name of member to retrieve associated relative indicator.
   */
  bool is_relative(std::string name) const {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    uint8_t is_relative;
    ctx.handle_error(tiledb_group_get_is_relative_uri_by_name(
        c_ctx, group_.get(), name.c_str(), &is_relative));
    return is_relative != 0;
  }

  std::string dump(const bool recursive) const {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    char* str;
    ctx.handle_error(
        tiledb_group_dump_str(c_ctx, group_.get(), &str, recursive));

    std::string ret(str);
    free(str);
    return ret;
  }

  /**
   * Consolidates the group metadata into a single group metadata file.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Group::consolidate_metadata(ctx, "s3://bucket-name/group-name");
   * @endcode
   *
   * @param ctx TileDB context
   * @param uri The URI of the TileDB group to be consolidated.
   * @param config Configuration parameters for the consolidation.
   */
  static void consolidate_metadata(
      const Context& ctx,
      const std::string& uri,
      Config* const config = nullptr) {
    ctx.handle_error(tiledb_group_consolidate_metadata(
        ctx.ptr().get(), uri.c_str(), config ? config->ptr().get() : nullptr));
  }

  /**
   * Cleans up the group metadata.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Group::vacuum_metadata(ctx, "s3://bucket-name/group-name");
   * @endcode
   *
   * @param ctx TileDB context
   * @param uri The URI of the TileDB group to vacuum.
   * @param config Configuration parameters for the vacuuming.
   */
  static void vacuum_metadata(
      const Context& ctx,
      const std::string& uri,
      Config* const config = nullptr) {
    ctx.handle_error(tiledb_group_vacuum_metadata(
        ctx.ptr().get(), uri.c_str(), config ? config->ptr().get() : nullptr));
  }

 private:
  Group(
      const Context& ctx,
      const std::string& group_uri,
      tiledb_query_type_t query_type,
      tiledb_config_t* config)
      : ctx_(ctx) {
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    tiledb_group_t* group;
    ctx.handle_error(tiledb_group_alloc(c_ctx, group_uri.c_str(), &group));
    group_ = std::shared_ptr<tiledb_group_t>(group, deleter_);
    if (config) {
      ctx.handle_error(tiledb_group_set_config(c_ctx, group, config));
    }
    ctx.handle_error(tiledb_group_open(c_ctx, group, query_type));
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Flag indicating ownership of the TileDB C group object */
  bool owns_c_ptr_ = true;

  /** Pointer to the TileDB C group object. */
  std::shared_ptr<tiledb_group_t> group_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_GROUP_EXPERIMENTAL_H
