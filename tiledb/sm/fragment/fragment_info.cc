/**
 * @file   fragment_info.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2024 TileDB, Inc.
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
 * This file implements the FragmentInfo class.
 */

#include "tiledb/sm/fragment/fragment_info.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/tile/generic_tile_io.h"

namespace tiledb::sm {

class FragmentInfoException : public StatusException {
 public:
  explicit FragmentInfoException(const std::string& message)
      : StatusException("FragmentInfo", message) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

FragmentInfo::FragmentInfo(const URI& array_uri, ContextResources& resources)
    : array_uri_(array_uri)
    , config_(resources.config())
    , resources_(&resources)
    , unconsolidated_metadata_num_(0) {
}

FragmentInfo::~FragmentInfo() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

void FragmentInfo::set_config(const Config& config) {
  if (loaded_) {
    throw FragmentInfoException("[set_config] Cannot set config after load");
  }
  config_.inherit(config);
}

void FragmentInfo::expand_anterior_ndrange(
    const Domain& domain, const NDRange& range) {
  domain.expand_ndrange(range, &anterior_ndrange_);
}

Status FragmentInfo::get_dense(uint32_t fid, int32_t* dense) const {
  ensure_loaded();
  if (dense == nullptr) {
    throw FragmentInfoException(
        "Cannot check if fragment is dense; Dense argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot check if fragment is dense; Invalid fragment index");
  }

  *dense = (int32_t)!single_fragment_info_vec_[fid].sparse();

  return Status::Ok();
}

Status FragmentInfo::get_sparse(uint32_t fid, int32_t* sparse) const {
  ensure_loaded();
  if (sparse == nullptr) {
    throw FragmentInfoException(
        "Cannot check if fragment is sparse; Sparse argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot check if fragment is sparse; Invalid fragment index");
  }

  *sparse = (int32_t)single_fragment_info_vec_[fid].sparse();

  return Status::Ok();
}

uint32_t FragmentInfo::fragment_num() const {
  ensure_loaded();
  return (uint32_t)single_fragment_info_vec_.size();
}

Status FragmentInfo::get_cell_num(uint32_t fid, uint64_t* cell_num) const {
  ensure_loaded();
  if (cell_num == nullptr) {
    throw FragmentInfoException(
        "Cannot get fragment URI; Cell number argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get fragment URI; Invalid fragment index");
  }

  *cell_num = single_fragment_info_vec_[fid].cell_num();

  return Status::Ok();
}

Status FragmentInfo::get_total_cell_num(uint64_t* cell_num) const {
  ensure_loaded();
  if (cell_num == nullptr) {
    throw FragmentInfoException("Cell number argument cannot be null");
  }

  // Return simple summation of cell counts in each fragment present without
  // any consideration of cells that may be overlapping, i.e. the count
  // returned will be >= the actual unique number of cells represented within
  // the fragments.

  uint64_t total_cell_num = 0;
  uint64_t endi = single_fragment_info_vec_.size();
  for (auto fid = 0ul; fid < endi; ++fid) {
    total_cell_num += single_fragment_info_vec_[fid].cell_num();
  }

  *cell_num = total_cell_num;

  return Status::Ok();
}

const std::string& FragmentInfo::fragment_name(uint32_t fid) const {
  ensure_loaded();
  if (fid >= fragment_num())
    throw FragmentInfoException(
        "Cannot get fragment URI; Invalid fragment index");

  return single_fragment_info_vec_[fid].name();
}

Status FragmentInfo::get_fragment_size(uint32_t fid, uint64_t* size) const {
  ensure_loaded();
  if (size == nullptr) {
    throw FragmentInfoException(
        "Cannot get fragment URI; Size argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get fragment URI; Invalid fragment index");
  }

  *size = single_fragment_info_vec_[fid].fragment_size();

  return Status::Ok();
}

Status FragmentInfo::get_fragment_uri(uint32_t fid, const char** uri) const {
  ensure_loaded();
  if (uri == nullptr) {
    throw FragmentInfoException(
        "Cannot get fragment URI; URI argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get fragment URI; Invalid fragment index");
  }

  *uri = single_fragment_info_vec_[fid].uri().c_str();

  return Status::Ok();
}

Status FragmentInfo::get_to_vacuum_uri(uint32_t fid, const char** uri) const {
  ensure_loaded();
  if (uri == nullptr) {
    throw FragmentInfoException(
        "Cannot get URI of fragment to vacuum; URI argument cannot be null");
  }

  if (fid >= to_vacuum_.size()) {
    throw FragmentInfoException(
        "Cannot get URI of fragment to vacuum; Invalid fragment index");
  }

  *uri = to_vacuum_[fid].c_str();

  return Status::Ok();
}

Status FragmentInfo::get_timestamp_range(
    uint32_t fid, uint64_t* start, uint64_t* end) const {
  ensure_loaded();
  if (start == nullptr) {
    throw FragmentInfoException(
        "Cannot get timestamp range; Start argument cannot be null");
  }

  if (end == nullptr) {
    throw FragmentInfoException(
        "Cannot get timestamp range; End argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get fragment URI; Invalid fragment index");
  }

  auto range = single_fragment_info_vec_[fid].timestamp_range();
  *start = range.first;
  *end = range.second;

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain(
    uint32_t fid, uint32_t did, void* domain) const {
  ensure_loaded();
  if (domain == nullptr) {
    throw FragmentInfoException(
        "Cannot get non-empty domain; Domain argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain; Invalid fragment index");
  }

  const auto& non_empty_domain =
      single_fragment_info_vec_[fid].non_empty_domain();

  if (did >= non_empty_domain.size()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain; Invalid dimension index");
  }

  if (non_empty_domain[did].var_size()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain; Dimension is variable-sized");
  }

  assert(!non_empty_domain[did].empty());
  std::memcpy(
      domain, non_empty_domain[did].data(), non_empty_domain[did].size());

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain(
    uint32_t fid, const char* dim_name, void* domain) const {
  ensure_loaded();
  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain; Invalid fragment index");
  }
  if (dim_name == nullptr) {
    throw FragmentInfoException(
        "Cannot get non-empty domain; Dimension name argument cannot be null");
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  const auto& array_schema = meta->array_schema();
  auto dim_num = array_schema->dim_num();
  uint32_t did;
  for (did = 0; did < dim_num; ++did) {
    if (dim_name == array_schema->dimension_ptr(did)->name()) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_num) {
    throw FragmentInfoException(
        "Cannot get non-empty domain; Invalid dimension name '" +
        std::string(dim_name) + "'");
  }

  return get_non_empty_domain(fid, did, domain);
}

Status FragmentInfo::get_non_empty_domain_var_size(
    uint32_t fid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) const {
  ensure_loaded();
  if (start_size == nullptr) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var size; Start size cannot be null");
  }

  if (end_size == nullptr) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var size; End size cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var size; Invalid fragment index");
  }

  const auto& non_empty_domain =
      single_fragment_info_vec_[fid].non_empty_domain();

  if (did >= non_empty_domain.size()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var size; Invalid dimension index");
  }

  if (!non_empty_domain[did].var_size()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var size; Dimension is fixed sized");
  }

  *start_size = non_empty_domain[did].start_size();
  *end_size = non_empty_domain[did].end_size();

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain_var_size(
    uint32_t fid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) const {
  ensure_loaded();
  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get var-sized non-empty domain; Invalid fragment index");
  }
  if (dim_name == nullptr) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var size; "
        "Dimension name argument cannot be null");
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  const auto& array_schema = meta->array_schema();
  auto dim_num = array_schema->dim_num();
  uint32_t did;
  for (did = 0; did < dim_num; ++did) {
    if (dim_name == array_schema->dimension_ptr(did)->name()) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_num) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var size; Invalid dimension name '" +
        std::string(dim_name) + "'");
  }

  return get_non_empty_domain_var_size(fid, did, start_size, end_size);
}

Status FragmentInfo::get_non_empty_domain_var(
    uint32_t fid, uint32_t did, void* start, void* end) const {
  ensure_loaded();
  if (start == nullptr) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var; Domain start argument cannot be "
        "null");
  }

  if (end == nullptr) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var; Domain end argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var; Invalid fragment index");
  }

  const auto& non_empty_domain =
      single_fragment_info_vec_[fid].non_empty_domain();

  if (did >= non_empty_domain.size()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var; Invalid dimension index");
  }

  if (!non_empty_domain[did].var_size()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var; Dimension is fixed-sized");
  }

  auto start_str = non_empty_domain[did].start_str();
  std::memcpy(start, start_str.data(), start_str.size());
  auto end_str = non_empty_domain[did].end_str();
  std::memcpy(end, end_str.data(), end_str.size());

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain_var(
    uint32_t fid, const char* dim_name, void* start, void* end) const {
  ensure_loaded();
  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var; Invalid fragment index");
  }
  if (dim_name == nullptr) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var; Dimension "
        "name argument cannot be null");
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  const auto& array_schema = meta->array_schema();
  auto dim_num = array_schema->dim_num();
  uint32_t did;
  for (did = 0; did < dim_num; ++did) {
    if (dim_name == array_schema->dimension_ptr(did)->name()) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_num) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var; Invalid dimension name '" +
        std::string(dim_name) + "'");
  }

  return get_non_empty_domain_var(fid, did, start, end);
}

Status FragmentInfo::get_mbr_num(uint32_t fid, uint64_t* mbr_num) {
  ensure_loaded();
  if (mbr_num == nullptr) {
    throw FragmentInfoException(
        "Cannot get fragment URI; MBR number argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get fragment URI; Invalid fragment index");
  }

  if (!single_fragment_info_vec_[fid].sparse()) {
    *mbr_num = 0;
    return Status::Ok();
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  meta->loaded_metadata()->load_rtree(enc_key_);
  *mbr_num = meta->mbrs().size();

  return Status::Ok();
}

Status FragmentInfo::get_mbr(
    uint32_t fid, uint32_t mid, uint32_t did, void* mbr) {
  ensure_loaded();
  if (mbr == nullptr) {
    throw FragmentInfoException("Cannot get MBR; mbr argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException("Cannot get MBR; Invalid fragment index");
  }

  if (!single_fragment_info_vec_[fid].sparse()) {
    throw FragmentInfoException("Cannot get MBR; Fragment is not sparse");
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  meta->loaded_metadata()->load_rtree(enc_key_);
  const auto& mbrs = meta->mbrs();

  if (mid >= mbrs.size()) {
    throw FragmentInfoException("Cannot get MBR; Invalid MBR index");
  }

  const auto& minimum_bounding_rectangle = mbrs[mid];
  if (did >= minimum_bounding_rectangle.size()) {
    throw FragmentInfoException("Cannot get MBR; Invalid dimension index");
  }

  if (minimum_bounding_rectangle[did].var_size()) {
    throw FragmentInfoException("Cannot get MBR; Dimension is variable-sized");
  }

  std::memcpy(
      mbr,
      minimum_bounding_rectangle[did].data(),
      minimum_bounding_rectangle[did].size());

  return Status::Ok();
}

Status FragmentInfo::get_mbr(
    uint32_t fid, uint32_t mid, const char* dim_name, void* mbr) {
  ensure_loaded();
  if (fid >= fragment_num()) {
    throw FragmentInfoException("Cannot get MBR; Invalid fragment index");
  }
  if (dim_name == nullptr) {
    throw FragmentInfoException(
        "Cannot get non-empty domain; Dimension name argument cannot be null");
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  const auto& array_schema = meta->array_schema();
  auto dim_num = array_schema->dim_num();
  uint32_t did;
  for (did = 0; did < dim_num; ++did) {
    if (dim_name == array_schema->dimension_ptr(did)->name()) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_num) {
    throw FragmentInfoException(
        "Cannot get non-empty domain; Invalid dimension name '" +
        std::string(dim_name) + "'");
  }

  return get_mbr(fid, mid, did, mbr);
}

Status FragmentInfo::get_mbr_var_size(
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) {
  ensure_loaded();
  if (start_size == nullptr) {
    throw FragmentInfoException(
        "Cannot get MBR var size; Start size argument cannot be null");
  }

  if (end_size == nullptr) {
    throw FragmentInfoException(
        "Cannot get MBR var size; End size argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get MBR var size; Invalid fragment index");
  }

  if (!single_fragment_info_vec_[fid].sparse()) {
    throw FragmentInfoException("Cannot get MBR; Fragment is not sparse");
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  meta->loaded_metadata()->load_rtree(enc_key_);
  const auto& mbrs = meta->mbrs();

  if (mid >= mbrs.size()) {
    throw FragmentInfoException("Cannot get MBR; Invalid mbr index");
  }

  const auto& minimum_bounding_rectangle = mbrs[mid];
  if (did >= minimum_bounding_rectangle.size()) {
    throw FragmentInfoException(
        "Cannot get MBR var size; Invalid dimension index");
  }

  if (!minimum_bounding_rectangle[did].var_size()) {
    throw FragmentInfoException(
        "Cannot get MBR var size; Dimension is fixed sized");
  }

  *start_size = minimum_bounding_rectangle[did].start_size();
  *end_size = minimum_bounding_rectangle[did].end_size();

  return Status::Ok();
}

Status FragmentInfo::get_mbr_var_size(
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) {
  ensure_loaded();
  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get MBR var size; Invalid fragment index");
  }
  if (dim_name == nullptr) {
    throw FragmentInfoException(
        "Cannot get MBR var size; "
        "Dimension name argument cannot be null");
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  const auto& array_schema = meta->array_schema();
  auto dim_num = array_schema->dim_num();
  uint32_t did;
  for (did = 0; did < dim_num; ++did) {
    if (dim_name == array_schema->dimension_ptr(did)->name()) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_num) {
    throw FragmentInfoException(
        "Cannot get MBR var size; Invalid dimension name '" +
        std::string(dim_name) + "'");
  }

  return get_mbr_var_size(fid, mid, did, start_size, end_size);
}

Status FragmentInfo::get_mbr_var(
    uint32_t fid, uint32_t mid, uint32_t did, void* start, void* end) {
  ensure_loaded();
  if (start == nullptr) {
    throw FragmentInfoException(
        "Cannot get MBR var; Start argument cannot be null");
  }

  if (end == nullptr) {
    throw FragmentInfoException(
        "Cannot get MBR var; End argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException("Cannot get MBR var; Invalid fragment index");
  }

  if (!single_fragment_info_vec_[fid].sparse()) {
    throw FragmentInfoException("Cannot get MBR var; Fragment is not sparse");
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  meta->loaded_metadata()->load_rtree(enc_key_);
  const auto& mbrs = meta->mbrs();

  if (mid >= mbrs.size()) {
    throw FragmentInfoException("Cannot get MBR var; Invalid mbr index");
  }

  const auto& minimum_bounding_rectangle = mbrs[mid];
  if (did >= minimum_bounding_rectangle.size()) {
    throw FragmentInfoException("Cannot get MBR var; Invalid dimension index");
  }

  if (!minimum_bounding_rectangle[did].var_size()) {
    throw FragmentInfoException("Cannot get MBR var; Dimension is fixed-sized");
  }

  auto start_str = minimum_bounding_rectangle[did].start_str();
  std::memcpy(start, start_str.data(), start_str.size());
  auto end_str = minimum_bounding_rectangle[did].end_str();
  std::memcpy(end, end_str.data(), end_str.size());

  return Status::Ok();
}

Status FragmentInfo::get_mbr_var(
    uint32_t fid, uint32_t mid, const char* dim_name, void* start, void* end) {
  ensure_loaded();
  if (fid >= fragment_num()) {
    throw FragmentInfoException("Cannot get MBR var; Invalid fragment index");
  }
  if (dim_name == nullptr) {
    throw FragmentInfoException(
        "Cannot get MBR var; Dimension "
        "name argument cannot be null");
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  const auto& array_schema = meta->array_schema();
  auto dim_num = array_schema->dim_num();
  uint32_t did;
  for (did = 0; did < dim_num; ++did) {
    if (dim_name == array_schema->dimension_ptr(did)->name()) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_num) {
    throw FragmentInfoException(
        "Cannot get non-empty domain var; Invalid dimension name '" +
        std::string(dim_name) + "'");
  }

  return get_mbr_var(fid, mid, did, start, end);
}

Status FragmentInfo::get_version(uint32_t fid, uint32_t* version) const {
  ensure_loaded();
  if (version == nullptr) {
    throw FragmentInfoException(
        "Cannot get version; Version argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException("Cannot get version; Invalid fragment index");
  }

  *version = single_fragment_info_vec_[fid].format_version();

  return Status::Ok();
}

shared_ptr<ArraySchema> FragmentInfo::get_array_schema(uint32_t fid) {
  ensure_loaded();
  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get array schema; Invalid fragment index");
  }
  URI schema_uri;
  uint32_t version = single_fragment_info_vec_[fid].format_version();
  if (version >= 10) {
    schema_uri =
        array_uri_.join_path(constants::array_schema_dir_name)
            .join_path(single_fragment_info_vec_[fid].array_schema_name());
  } else {
    schema_uri = array_uri_.join_path(constants::array_schema_filename);
  }

  EncryptionKey encryption_key;
  auto tracker = resources_->ephemeral_memory_tracker();
  return ArrayDirectory::load_array_schema_from_uri(
      *resources_, schema_uri, encryption_key, tracker);
}

Status FragmentInfo::get_array_schema_name(
    uint32_t fid, const char** schema_name) {
  ensure_loaded();
  if (schema_name == nullptr) {
    throw FragmentInfoException(
        "Cannot get array schema URI; schema name argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot get array schema name; Invalid fragment index");
  }

  uint32_t version = single_fragment_info_vec_[fid].format_version();
  if (version >= 10) {
    *schema_name = single_fragment_info_vec_[fid].array_schema_name().c_str();
  } else {
    *schema_name = constants::array_schema_filename.c_str();
  }

  return Status::Ok();
}

Status FragmentInfo::has_consolidated_metadata(
    uint32_t fid, int32_t* has) const {
  ensure_loaded();
  if (has == nullptr) {
    throw FragmentInfoException(
        "Cannot check if fragment has consolidated "
        "metadata; Has argument cannot be null");
  }

  if (fid >= fragment_num()) {
    throw FragmentInfoException(
        "Cannot check if fragment has consolidated "
        "metadata; Invalid fragment index");
  }

  *has = single_fragment_info_vec_[fid].has_consolidated_footer();

  return Status::Ok();
}

Status FragmentInfo::load() {
  RETURN_NOT_OK(set_enc_key_from_config());
  RETURN_NOT_OK(set_default_timestamp_range());

  if (array_uri_.is_tiledb()) {
    auto rest_client = resources_->rest_client();
    if (rest_client == nullptr) {
      throw FragmentInfoException(
          "Cannot load fragment info; remote array with no REST client.");
    }

    // Overriding this config parameter is necessary to enable Cloud to load
    // MBRs at the same time as the rest of fragment info and not lazily
    // as it's the case for local fragment info load requests.
    throw_if_not_ok(config_.set("sm.fragment_info.preload_mbrs", "true"));

    return rest_client->post_fragment_info_from_rest(array_uri_, this);
  }

  // Create an ArrayDirectory object and load
  ArrayDirectory array_dir(
      *resources_, array_uri_, timestamp_start_, timestamp_end_);

  return load(array_dir);
}

Status FragmentInfo::load(
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  RETURN_NOT_OK(enc_key_.set_key(encryption_type, encryption_key, key_length));
  RETURN_NOT_OK(set_default_timestamp_range());

  // Create an ArrayDirectory object and load
  ArrayDirectory array_dir(
      *resources_, array_uri_, timestamp_start_, timestamp_end_);
  return load(array_dir);
}

Status FragmentInfo::load(
    const ArrayDirectory& array_dir,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  timestamp_start_ = timestamp_start;
  timestamp_end_ = timestamp_end;

  RETURN_NOT_OK(enc_key_.set_key(encryption_type, encryption_key, key_length));
  return load(array_dir);
}

Status FragmentInfo::load(const ArrayDirectory& array_dir) {
  // Check if we need to preload MBRs or not based on config
  bool found = false, preload_rtrees = false;
  auto status = config_.get<bool>(
      "sm.fragment_info.preload_mbrs", &preload_rtrees, &found);
  if (!status.ok() || !found) {
    throw std::runtime_error("Cannot get fragment info config setting");
  }

  // Get the array schemas and fragment metadata.
  auto memory_tracker = resources_->create_memory_tracker();
  std::vector<std::shared_ptr<FragmentMetadata>> fragment_metadata;
  std::tie(array_schema_latest_, array_schemas_all_, fragment_metadata) =
      load_array_schemas_and_fragment_metadata(
          *resources_, array_dir, memory_tracker, enc_key_);
  auto fragment_num = (uint32_t)fragment_metadata.size();

  // Get fragment sizes
  std::vector<uint64_t> sizes(fragment_num, 0);
  throw_if_not_ok(parallel_for(
      &resources_->compute_tp(),
      0,
      fragment_num,
      [this, &fragment_metadata, &sizes, preload_rtrees](uint64_t i) {
        // Get fragment size. Applicable only to relevant fragments, including
        // fragments that are in the range [timestamp_start_, timestamp_end_].
        auto meta = fragment_metadata[i];
        if (meta->timestamp_range().first >= timestamp_start_ &&
            meta->timestamp_range().second <= timestamp_end_) {
          sizes[i] = meta->fragment_size();
        }

        if (preload_rtrees & !meta->dense()) {
          meta->loaded_metadata()->load_rtree(enc_key_);
        }

        return Status::Ok();
      }));

  // Clear single fragment info vec and anterior range
  single_fragment_info_vec_.clear();
  anterior_ndrange_.clear();

  // Create the vector that will store the SingleFragmentInfo objects
  for (uint64_t fid = 0; fid < fragment_num; fid++) {
    const auto meta = fragment_metadata[fid];
    const auto& array_schema = meta->array_schema();
    const auto& non_empty_domain = meta->non_empty_domain();

    if (meta->timestamp_range().first < timestamp_start_) {
      expand_anterior_ndrange(array_schema->domain(), non_empty_domain);
    } else if (meta->timestamp_range().second <= timestamp_end_) {
      const auto& uri = meta->fragment_uri();
      bool sparse = !meta->dense();

      // compute expanded non-empty domain (only for dense fragments)
      auto expanded_non_empty_domain = non_empty_domain;
      if (!sparse) {
        expand_tiles_respecting_current_domain(
            array_schema->domain(),
            array_schema->current_domain(),
            &expanded_non_empty_domain);
      }

      // Push new fragment info
      single_fragment_info_vec_.emplace_back(SingleFragmentInfo(
          uri,
          sparse,
          meta->timestamp_range(),
          sizes[fid],
          non_empty_domain,
          expanded_non_empty_domain,
          meta));
    }
  }

  // Get the URIs to vacuum
  auto filtered_fragment_uris = array_dir.filtered_fragment_uris(true);
  to_vacuum_ = filtered_fragment_uris.fragment_uris_to_vacuum();

  // Get number of unconsolidated fragment metadata
  unconsolidated_metadata_num_ = 0;
  for (const auto& f : single_fragment_info_vec_) {
    unconsolidated_metadata_num_ += (uint32_t)!f.has_consolidated_footer();
  }

  loaded_ = true;
  return Status::Ok();
}

void FragmentInfo::ensure_loaded() const {
  if (!loaded_) {
    throw FragmentInfoException("Fragment info has not been loaded.");
  }
}

Status FragmentInfo::load_and_replace(
    const URI& new_fragment_uri,
    const std::vector<TimestampedURI>& to_replace) {
  // Load the new single fragment info
  auto&& [st, new_single_fragment_info] = load(new_fragment_uri);
  RETURN_NOT_OK(st);

  // Replace single fragment info elements with the new
  // single fragment info
  RETURN_NOT_OK(replace(new_single_fragment_info.value(), to_replace));

  return Status::Ok();
}

tuple<shared_ptr<Tile>, std::vector<std::pair<std::string, uint64_t>>>
load_consolidated_fragment_meta(
    ContextResources& resources,
    const URI& uri,
    const EncryptionKey& enc_key,
    shared_ptr<MemoryTracker> memory_tracker) {
  auto timer_se =
      resources.stats().start_timer("sm_read_load_consolidated_frag_meta");

  // No consolidated fragment metadata file
  if (uri.to_string().empty()) {
    throw FragmentInfoException(
        "Cannot load consolidated fragment metadata; URI is empty.");
  }

  auto tile = GenericTileIO::load(resources, uri, 0, enc_key, memory_tracker);

  resources.stats().add_counter("consolidated_frag_meta_size", tile->size());

  uint32_t fragment_num;
  Deserializer deserializer(tile->data(), tile->size());
  fragment_num = deserializer.read<uint32_t>();

  uint64_t name_size, offset;
  std::string name;
  std::vector<std::pair<std::string, uint64_t>> ret;
  ret.reserve(fragment_num);
  for (uint32_t f = 0; f < fragment_num; ++f) {
    name_size = deserializer.read<uint64_t>();
    name.resize(name_size);
    deserializer.read(&name[0], name_size);
    offset = deserializer.read<uint64_t>();
    ret.emplace_back(name, offset);
  }

  return {tile, std::move(ret)};
}

std::tuple<
    shared_ptr<ArraySchema>,
    std::unordered_map<std::string, shared_ptr<ArraySchema>>,
    std::vector<shared_ptr<FragmentMetadata>>>
FragmentInfo::load_array_schemas_and_fragment_metadata(
    ContextResources& resources,
    const ArrayDirectory& array_dir,
    shared_ptr<MemoryTracker> memory_tracker,
    const EncryptionKey& enc_key) {
  auto timer_se = resources.stats().start_timer(
      "sm_load_array_schemas_and_fragment_metadata");

  // Load array schemas
  auto tracker = resources.ephemeral_memory_tracker();
  std::shared_ptr<ArraySchema> array_schema_latest;
  std::unordered_map<std::string, std::shared_ptr<ArraySchema>>
      array_schemas_all;
  std::tie(array_schema_latest, array_schemas_all) =
      array_dir.load_array_schemas(enc_key, tracker);

  const auto filtered_fragment_uris = [&]() {
    auto timer_se =
        resources.stats().start_timer("sm_load_filtered_fragment_uris");
    return array_dir.filtered_fragment_uris(array_schema_latest->dense());
  }();
  const auto& meta_uris = array_dir.fragment_meta_uris();
  const auto& fragments_to_load = filtered_fragment_uris.fragment_uris();

  // Get the consolidated fragment metadatas
  std::vector<shared_ptr<Tile>> fragment_metadata_tiles(meta_uris.size());
  std::vector<std::vector<std::pair<std::string, uint64_t>>> offsets_vectors(
      meta_uris.size());
  throw_if_not_ok(
      parallel_for(&resources.compute_tp(), 0, meta_uris.size(), [&](size_t i) {
        auto&& [tile_opt, offsets] = load_consolidated_fragment_meta(
            resources, meta_uris[i], enc_key, memory_tracker);
        fragment_metadata_tiles[i] = tile_opt;
        offsets_vectors[i] = std::move(offsets);
        return Status::Ok();
      }));

  // Get the unique fragment metadatas into a map.
  std::unordered_map<std::string, std::pair<Tile*, uint64_t>> offsets;
  for (uint64_t i = 0; i < offsets_vectors.size(); i++) {
    for (auto& offset : offsets_vectors[i]) {
      if (offsets.count(offset.first) == 0) {
        offsets.emplace(
            offset.first,
            std::make_pair(fragment_metadata_tiles[i].get(), offset.second));
      }
    }
  }

  // Load the fragment metadata
  auto&& fragment_metadata = FragmentMetadata::load(
      resources,
      memory_tracker,
      array_schema_latest,
      array_schemas_all,
      enc_key,
      fragments_to_load,
      offsets);

  return {array_schema_latest, array_schemas_all, fragment_metadata};
}

const std::vector<SingleFragmentInfo>& FragmentInfo::single_fragment_info_vec()
    const {
  return single_fragment_info_vec_;
}

const NDRange& FragmentInfo::anterior_ndrange() const {
  return anterior_ndrange_;
}

uint32_t FragmentInfo::to_vacuum_num() const {
  return (uint32_t)to_vacuum_.size();
}

uint32_t FragmentInfo::unconsolidated_metadata_num() const {
  return unconsolidated_metadata_num_;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

Status FragmentInfo::set_enc_key_from_config() {
  std::string enc_key_str, enc_type_str;
  bool found = false;
  enc_key_str = config_.get("sm.encryption_key", &found);
  enc_type_str = config_.get("sm.encryption_type", &found);
  auto [st, et] = encryption_type_enum(enc_type_str);
  RETURN_NOT_OK(st);
  auto enc_type = et.value();
  return enc_key_.set_key(
      enc_type, enc_key_str.c_str(), static_cast<uint32_t>(enc_key_str.size()));
}

Status FragmentInfo::set_default_timestamp_range() {
  timestamp_start_ = 0;
  timestamp_end_ = utils::time::timestamp_now_ms();

  // TODO: get the timestamp range from the config

  return Status::Ok();
}

tuple<Status, optional<SingleFragmentInfo>> FragmentInfo::load(
    const URI& new_fragment_uri) const {
  SingleFragmentInfo ret;
  auto& vfs = resources_->vfs();
  const auto& array_schema_latest =
      single_fragment_info_vec_.back().meta()->array_schema();

  // Get timestamp range
  FragmentID fragment_id{new_fragment_uri};
  auto timestamp_range{fragment_id.timestamp_range()};

  // Check if fragment is sparse
  bool sparse = false;
  if (fragment_id.array_format_version() <= 2) {
    URI coords_uri =
        new_fragment_uri.join_path(constants::coords + constants::file_suffix);
    RETURN_NOT_OK_TUPLE(vfs.is_file(coords_uri, &sparse), nullopt);
  } else {
    // Do nothing. It does not matter what the `sparse` value
    // is, since the FragmentMetadata object will load the correct
    // value from the metadata file.

    // Also `sparse` is updated below after loading the metadata
  }

  // Get fragment non-empty domain
  auto meta = make_shared<FragmentMetadata>(
      HERE(),
      resources_,
      array_schema_latest,
      new_fragment_uri,
      timestamp_range,
      resources_->create_memory_tracker(),
      !sparse);
  meta->load(enc_key_, nullptr, 0, array_schemas_all_);

  // This is important for format version > 2
  sparse = !meta->dense();

  // Get fragment size
  uint64_t size = meta->fragment_size();

  // Compute expanded non-empty domain only for dense fragments
  // Get non-empty domain, and compute expanded non-empty domain
  // (only for dense fragments)
  const auto& non_empty_domain = meta->non_empty_domain();
  auto expanded_non_empty_domain = non_empty_domain;
  if (!sparse) {
    expand_tiles_respecting_current_domain(
        meta->array_schema()->domain(),
        meta->array_schema()->current_domain(),
        &expanded_non_empty_domain);
  }

  // Set fragment info
  ret = SingleFragmentInfo(
      new_fragment_uri,
      sparse,
      timestamp_range,
      size,
      non_empty_domain,
      expanded_non_empty_domain,
      meta);

  return {Status::Ok(), ret};
}

Status FragmentInfo::replace(
    const SingleFragmentInfo& new_single_fragment_info,
    const std::vector<TimestampedURI>& to_replace) {
  auto to_replace_it = to_replace.begin();
  auto single_fragment_info_it = single_fragment_info_vec_.begin();
  std::vector<SingleFragmentInfo> updated_single_fragment_info_vec;
  bool new_fragment_added = false;
  auto old_fragment_num = fragment_num();

  while (single_fragment_info_it != single_fragment_info_vec_.end()) {
    // No match - add the fragment info and advance `fragment_it`
    if (to_replace_it == to_replace.end() ||
        single_fragment_info_it->uri().to_string() !=
            to_replace_it->uri_.to_string()) {
      updated_single_fragment_info_vec.emplace_back(*single_fragment_info_it);
      ++single_fragment_info_it;
    } else {  // Match - add new fragment only once and advance both iterators
      if (!new_fragment_added) {
        updated_single_fragment_info_vec.emplace_back(new_single_fragment_info);
        new_fragment_added = true;
      }
      ++single_fragment_info_it;
      ++to_replace_it;
    }
  }

  single_fragment_info_vec_ = std::move(updated_single_fragment_info_vec);

  assert(fragment_num() == old_fragment_num - to_replace.size() + 1);
  (void)old_fragment_num;  // When running in release mode, this is not used

  return Status::Ok();
}

}  // namespace tiledb::sm

std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::FragmentInfo& fragment_info) {
  auto fragment_num = fragment_info.fragment_num();

  os << "- Fragment num: " << fragment_num << "\n";
  os << "- Unconsolidated metadata num: "
     << fragment_info.unconsolidated_metadata_num() << "\n";

  auto to_vacuum = fragment_info.to_vacuum();
  os << "- To vacuum num: " << to_vacuum.size() << "\n";

  if (!to_vacuum.empty()) {
    os << "- To vacuum URIs:\n";
    for (const auto& v : to_vacuum)
      os << "  > " << v.c_str() << "\n";
  }

  auto single_fragment_info_vec = fragment_info.single_fragment_info_vec();
  for (uint32_t fid = 0; fid < fragment_num; ++fid) {
    auto meta = single_fragment_info_vec[fid].meta();
    auto dim_types = meta->dim_types();
    os << "- Fragment #" << fid + 1 << ":\n";
    single_fragment_info_vec[fid].dump_single_fragment_info(os, dim_types);
  }

  return os;
}
