/**
 * @file tiledb/api/c_api/array/array_api_internal.h
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
 * This file declares the internals of the array section of the C API.
 */

#ifndef TILEDB_CAPI_ARRAY_INTERNAL_H
#define TILEDB_CAPI_ARRAY_INTERNAL_H

#include "array_api_external.h"

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array/array.h"

// using namespace tiledb::sm;

/** Handle `struct` for API Array objects. */
struct tiledb_array_handle_t
    : public tiledb::api::CAPIHandle<tiledb_array_handle_t> {
  /** Type name */
  static constexpr std::string_view object_type_name{"array"};

 private:
  using array_type = shared_ptr<tiledb::sm::Array>;
  array_type array_;

 public:
  explicit tiledb_array_handle_t(
      tiledb::sm::ContextResources& resources,
      const tiledb::sm::URI& array_uri,
      tiledb::sm::ConsistencyController& cc = tiledb::sm::controller())
      : array_{
            make_shared<tiledb::sm::Array>(HERE(), resources, array_uri, cc)} {
  }

  /**
   * Constructor from `shared_ptr<Array>` copies the shared pointer.
   */
  explicit tiledb_array_handle_t(const array_type& x)
      : array_(x) {
  }

  array_type array() const {
    return array_;
  }

  const tiledb::sm::ArraySchema& array_schema_latest() const {
    return array_->array_schema_latest();
  }

  shared_ptr<const tiledb::sm::ArraySchema> array_schema_latest_ptr() const {
    return array_->array_schema_latest_ptr();
  }

  const std::unordered_map<std::string, shared_ptr<tiledb::sm::ArraySchema>>&
  array_schemas_all() const {
    return array_->array_schemas_all();
  }

  const tiledb::sm::URI& array_uri() const {
    return array_->array_uri();
  }

  Status close() {
    return array_->close();
  }

  tiledb::sm::Config config() const {
    return array_->config();
  }

  void delete_array(const tiledb::sm::URI& uri) {
    array_->delete_array(uri);
  }

  void delete_fragments(
      tiledb::sm::ContextResources& resources,
      const tiledb::sm::URI& uri,
      uint64_t ts_start,
      uint64_t ts_end,
      std::optional<tiledb::sm::ArrayDirectory> array_dir = std::nullopt) {
    array_->delete_fragments(resources, uri, ts_start, ts_end, array_dir);
  }

  void delete_fragments(
      const tiledb::sm::URI& uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end) {
    array_->delete_fragments(uri, timestamp_start, timestamp_end);
  }

  void delete_fragments_list(
      const std::vector<tiledb::sm::URI>& fragment_uris) {
    array_->delete_fragments_list(fragment_uris);
  }

  void delete_metadata(const char* key) {
    array_->delete_metadata(key);
  }

  std::vector<shared_ptr<tiledb::sm::FragmentMetadata>>& fragment_metadata() {
    return array_->fragment_metadata();
  }

  tuple<Status, optional<shared_ptr<tiledb::sm::ArraySchema>>>
  get_array_schema() const {
    return array_->get_array_schema();
  }

  std::unordered_map<std::string, uint64_t> get_average_var_cell_sizes() const {
    return array_->get_average_var_cell_sizes();
  }

  const tiledb::sm::EncryptionKey& get_encryption_key() const {
    return array_->get_encryption_key();
  }

  shared_ptr<const tiledb::sm::Enumeration> get_enumeration(
      const std::string& enumeration_name) const {
    return array_->get_enumeration(enumeration_name);
  }

  std::unordered_map<
      std::string,
      std::vector<shared_ptr<const tiledb::sm::Enumeration>>>
  get_all_enumerations() {
    return array_->get_all_enumerations();
  }

  std::vector<shared_ptr<const tiledb::sm::Enumeration>> get_enumerations(
      const std::vector<std::string>& enumeration_names) {
    return array_->get_enumerations(enumeration_names);
  }

  void get_metadata(
      const char* key,
      tiledb::sm::Datatype* value_type,
      uint32_t* value_num,
      const void** value) const {
    array_->get_metadata(key, value_type, value_num, value);
  }

  void get_metadata(
      uint64_t index,
      const char** key,
      uint32_t* key_len,
      tiledb::sm::Datatype* value_type,
      uint32_t* value_num,
      const void** value) const {
    array_->get_metadata(index, key, key_len, value_type, value_num, value);
  }

  tiledb::sm::QueryType get_query_type() const {
    return array_->get_query_type();
  }

  bool is_open() const {
    return array_->is_open();
  }

  void load_all_enumerations(bool all_schemas = false) const {
    array_->load_all_enumerations(all_schemas);
  }

  tiledb::sm::NDRange& loaded_non_empty_domain() {
    return array_->loaded_non_empty_domain();
  }

  tiledb::sm::Metadata& metadata() const {
    return array_->metadata();
  }

  uint64_t metadata_num() {
    return array_->metadata_num();
  }

  std::optional<tiledb::sm::Datatype> metadata_type(const char* key) {
    return array_->metadata_type(key);
  }

  const tiledb::sm::NDRange non_empty_domain() {
    return array_->non_empty_domain();
  }

  void non_empty_domain(tiledb::sm::NDRange* domain, bool* is_empty) {
    array_->non_empty_domain(domain, is_empty);
  }

  void non_empty_domain(void* domain, bool* is_empty) {
    array_->non_empty_domain(domain, is_empty);
  }

  void non_empty_domain_from_index(unsigned idx, void* domain, bool* is_empty) {
    array_->non_empty_domain_from_index(idx, domain, is_empty);
  }

  void non_empty_domain_from_name(
      std::string_view field_name, void* domain, bool* is_empty) {
    array_->non_empty_domain_from_name(field_name, domain, is_empty);
  }

  void non_empty_domain_var_from_index(
      unsigned idx, void* start, void* end, bool* is_empty) {
    array_->non_empty_domain_var_from_index(idx, start, end, is_empty);
  }

  void non_empty_domain_var_from_name(
      std::string_view field_name, void* start, void* end, bool* is_empty) {
    array_->non_empty_domain_var_from_name(field_name, start, end, is_empty);
  }

  void non_empty_domain_var_size_from_index(
      unsigned idx, uint64_t* start_size, uint64_t* end_size, bool* is_empty) {
    return array_->non_empty_domain_var_size_from_index(
        idx, start_size, end_size, is_empty);
  }

  void non_empty_domain_var_size_from_name(
      std::string_view field_name,
      uint64_t* start_size,
      uint64_t* end_size,
      bool* is_empty) {
    array_->non_empty_domain_var_size_from_name(
        field_name, start_size, end_size, is_empty);
  }

  Status open(
      tiledb::sm::QueryType query_type,
      tiledb::sm::EncryptionType enc_type,
      const void* enc_key,
      uint32_t key_length) {
    return array_->open(query_type, enc_type, enc_key, key_length);
  }

  const shared_ptr<tiledb::sm::OpenedArray> opened_array() const {
    return array_->opened_array();
  }

  void put_metadata(
      const char* key,
      tiledb::sm::Datatype value_type,
      uint32_t value_num,
      const void* value) {
    array_->put_metadata(key, value_type, value_num, value);
  }

  Status reopen() {
    return array_->reopen();
  }

  void set_array_uri(const tiledb::sm::URI& array_uri) {
    array_->set_array_uri(array_uri);
  }

  void set_config(tiledb::sm::Config config) {
    array_->set_config(config);
  }

  void set_query_type(tiledb::sm::QueryType query_type) {
    array_->set_query_type(query_type);
  }

  void set_timestamp_start(uint64_t timestamp_start) {
    array_->set_timestamp_start(timestamp_start);
  }

  void set_timestamp_end(uint64_t timestamp_end) {
    array_->set_timestamp_end(timestamp_end);
  }

  uint64_t timestamp_start() const {
    return array_->timestamp_start();
  }

  uint64_t timestamp_end_opened_at() const {
    return array_->timestamp_end_opened_at();
  }

  tiledb::sm::Metadata* unsafe_metadata() {
    return array_->unsafe_metadata();
  }
};

namespace tiledb::api {

/**
 * Returns after successfully validating an array.
 *
 * @param array Possibly-valid pointer to an array
 */
inline void ensure_array_is_valid(const tiledb_array_t* array) {
  ensure_handle_is_valid(array);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_ARRAY_INTERNAL_H
