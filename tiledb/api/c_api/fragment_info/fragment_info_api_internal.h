/**
 * @file tiledb/api/c_api/fragment_info/fragment_info_api_internal.h
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
 * This file declares the internals of the fragment_info section of the C API.
 */

#ifndef TILEDB_CAPI_FRAGMENT_INFO_INTERNAL_H
#define TILEDB_CAPI_FRAGMENT_INFO_INTERNAL_H

#include "fragment_info_api_external.h"

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/fragment/fragment_info.h"

/** Handle `struct` for API FragmentInfo objects. */
struct tiledb_fragment_info_handle_t
    : public tiledb::api::CAPIHandle<tiledb_fragment_info_handle_t> {
  /** Type name */
  static constexpr std::string_view object_type_name{"fragment_info"};

 private:
  using fragment_info_type = shared_ptr<tiledb::sm::FragmentInfo>;
  fragment_info_type fragment_info_;

 public:
  explicit tiledb_fragment_info_handle_t() = delete;

  explicit tiledb_fragment_info_handle_t(
      const tiledb::sm::URI& array_uri, tiledb::sm::ContextResources& resources)
      : fragment_info_{make_shared<tiledb::sm::FragmentInfo>(
            HERE(), array_uri, resources)} {
  }

  /**
   * Constructor from `shared_ptr<FragmentInfo>` copies the shared pointer.
   */
  explicit tiledb_fragment_info_handle_t(const fragment_info_type& x)
      : fragment_info_(x) {
  }

  fragment_info_type fragment_info() const {
    return fragment_info_;
  }

  const Config& config() const {
    return fragment_info_->config();
  }

  void dump(FILE* out) const {
    fragment_info_->dump(out);
  }

  const std::string& fragment_name(uint32_t fid) const {
    return fragment_info_->fragment_name(fid);
  }

  uint32_t fragment_num() const {
    return fragment_info_->fragment_num();
  }

  shared_ptr<tiledb::sm::ArraySchema> get_array_schema(uint32_t fid) {
    return fragment_info_->get_array_schema(fid);
  }

  Status get_array_schema_name(uint32_t fid, const char** schema_name) {
    return fragment_info_->get_array_schema_name(fid, schema_name);
  }

  Status get_cell_num(uint32_t fid, uint64_t* cell_num) const {
    return fragment_info_->get_cell_num(fid, cell_num);
  }

  Status get_dense(uint32_t fid, int32_t* dense) const {
    return fragment_info_->get_dense(fid, dense);
  }

  Status get_fragment_size(uint32_t fid, uint64_t* size) const {
    return fragment_info_->get_fragment_size(fid, size);
  }

  Status get_fragment_uri(uint32_t fid, const char** uri) const {
    return fragment_info_->get_fragment_uri(fid, uri);
  }

  Status get_mbr(uint32_t fid, uint32_t mid, uint32_t did, void* mbr) {
    return fragment_info_->get_mbr(fid, mid, did, mbr);
  }

  Status get_mbr(uint32_t fid, uint32_t mid, const char* dim_name, void* mbr) {
    return fragment_info_->get_mbr(fid, mid, dim_name, mbr);
  }

  Status get_mbr_num(uint32_t fid, uint64_t* mbr_num) {
    return fragment_info_->get_mbr_num(fid, mbr_num);
  }

  Status get_mbr_var(
      uint32_t fid, uint32_t mid, uint32_t did, void* start, void* end) {
    return fragment_info_->get_mbr_var(fid, mid, did, start, end);
  }

  Status get_mbr_var(
      uint32_t fid,
      uint32_t mid,
      const char* dim_name,
      void* start,
      void* end) {
    return fragment_info_->get_mbr_var(fid, mid, dim_name, start, end);
  }

  Status get_mbr_var_size(
      uint32_t fid,
      uint32_t mid,
      uint32_t did,
      uint64_t* start_size,
      uint64_t* end_size) {
    return fragment_info_->get_mbr_var_size(
        fid, mid, did, start_size, end_size);
  }

  Status get_mbr_var_size(
      uint32_t fid,
      uint32_t mid,
      const char* dim_name,
      uint64_t* start_size,
      uint64_t* end_size) {
    return fragment_info_->get_mbr_var_size(
        fid, mid, dim_name, start_size, end_size);
  }

  Status get_non_empty_domain(uint32_t fid, uint32_t did, void* domain) const {
    return fragment_info_->get_non_empty_domain(fid, did, domain);
  }

  Status get_non_empty_domain(
      uint32_t fid, const char* dim_name, void* domain) const {
    return fragment_info_->get_non_empty_domain(fid, dim_name, domain);
  }

  Status get_non_empty_domain_var_size(
      uint32_t fid,
      uint32_t did,
      uint64_t* start_size,
      uint64_t* end_size) const {
    return fragment_info_->get_non_empty_domain_var_size(
        fid, did, start_size, end_size);
  }

  Status get_non_empty_domain_var_size(
      uint32_t fid,
      const char* dim_name,
      uint64_t* start_size,
      uint64_t* end_size) const {
    return fragment_info_->get_non_empty_domain_var_size(
        fid, dim_name, start_size, end_size);
  }

  Status get_non_empty_domain_var(
      uint32_t fid, uint32_t did, void* start, void* end) const {
    return fragment_info_->get_non_empty_domain_var(fid, did, start, end);
  }

  Status get_non_empty_domain_var(
      uint32_t fid, const char* dim_name, void* start, void* end) const {
    return fragment_info_->get_non_empty_domain_var(fid, dim_name, start, end);
  }

  Status get_sparse(uint32_t fid, int32_t* sparse) const {
    return fragment_info_->get_sparse(fid, sparse);
  }

  Status get_timestamp_range(
      uint32_t fid, uint64_t* start, uint64_t* end) const {
    return fragment_info_->get_timestamp_range(fid, start, end);
  }

  Status get_total_cell_num(uint64_t* cell_num) const {
    return fragment_info_->get_total_cell_num(cell_num);
  }

  Status get_to_vacuum_uri(uint32_t fid, const char** uri) const {
    return fragment_info_->get_to_vacuum_uri(fid, uri);
  }

  Status get_version(uint32_t fid, uint32_t* version) const {
    return fragment_info_->get_version(fid, version);
  }

  Status has_consolidated_metadata(uint32_t fid, int32_t* has) const {
    return fragment_info_->has_consolidated_metadata(fid, has);
  }

  Status load() {
    return fragment_info_->load();
  }

  void set_config(const tiledb::sm::Config& config) {
    fragment_info_->set_config(config);
  }

  uint32_t to_vacuum_num() const {
    return fragment_info_->to_vacuum_num();
  }

  uint32_t unconsolidated_metadata_num() const {
    return fragment_info_->unconsolidated_metadata_num();
  }
};

namespace tiledb::api {

/**
 * Returns after successfully validating a fragment info object.
 *
 * @param fragment_info Possibly-valid pointer to a fragment info object.
 */
inline void ensure_fragment_info_is_valid(
    const tiledb_fragment_info_t* fragment_info) {
  ensure_handle_is_valid(fragment_info);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_FRAGMENT_INFO_INTERNAL_H
