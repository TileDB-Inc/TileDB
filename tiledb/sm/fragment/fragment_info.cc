/**
 * @file   fragment_info.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2022 TileDB, Inc.
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
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/storage_format/uri/parse_uri.h"

using namespace tiledb::sm;
using namespace tiledb::common;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

FragmentInfo::FragmentInfo()
    : storage_manager_(nullptr)
    , unconsolidated_metadata_num_(0) {
}

FragmentInfo::FragmentInfo(
    const URI& array_uri, StorageManager* storage_manager)
    : array_uri_(array_uri)
    , config_(storage_manager->config())
    , storage_manager_(storage_manager)
    , unconsolidated_metadata_num_(0) {
}

FragmentInfo::~FragmentInfo() = default;

FragmentInfo::FragmentInfo(const FragmentInfo& fragment_info)
    : FragmentInfo() {
  auto clone = fragment_info.clone();
  swap(clone);
}

FragmentInfo::FragmentInfo(FragmentInfo&& fragment_info)
    : FragmentInfo() {
  swap(fragment_info);
}

FragmentInfo& FragmentInfo::operator=(const FragmentInfo& fragment_info) {
  auto clone = fragment_info.clone();
  swap(clone);
  return *this;
}

FragmentInfo& FragmentInfo::operator=(FragmentInfo&& fragment_info) {
  swap(fragment_info);
  return *this;
}

/* ********************************* */
/*                API                */
/* ********************************* */

Status FragmentInfo::set_config(const Config& config) {
  config_ = config;
  return Status::Ok();
}

void FragmentInfo::expand_anterior_ndrange(
    const Domain& domain, const NDRange& range) {
  domain.expand_ndrange(range, &anterior_ndrange_);
}

void FragmentInfo::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  auto fragment_num = this->fragment_num();

  std::stringstream ss;
  ss << "- Fragment num: " << fragment_num << "\n";
  ss << "- Unconsolidated metadata num: " << unconsolidated_metadata_num_
     << "\n";
  ss << "- To vacuum num: " << to_vacuum_.size() << "\n";

  if (!to_vacuum_.empty()) {
    ss << "- To vacuum URIs:\n";
    for (const auto& v : to_vacuum_)
      ss << "  > " << v.c_str() << "\n";
  }

  fprintf(out, "%s", ss.str().c_str());

  for (uint32_t fid = 0; fid < fragment_num; ++fid) {
    auto meta = single_fragment_info_vec_[fid].meta();
    auto dim_types = meta->dim_types();
    fprintf(out, "- Fragment #%u:\n", fid + 1);
    single_fragment_info_vec_[fid].dump(dim_types, out);
  }
}

Status FragmentInfo::get_dense(uint32_t fid, int32_t* dense) const {
  if (dense == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot check if fragment is dense; Dense argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot check if fragment is dense; Invalid fragment index"));

  *dense = (int32_t)!single_fragment_info_vec_[fid].sparse();

  return Status::Ok();
}

Status FragmentInfo::get_sparse(uint32_t fid, int32_t* sparse) const {
  if (sparse == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot check if fragment is sparse; Sparse argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot check if fragment is sparse; Invalid fragment index"));

  *sparse = (int32_t)single_fragment_info_vec_[fid].sparse();

  return Status::Ok();
}

uint32_t FragmentInfo::fragment_num() const {
  return (uint32_t)single_fragment_info_vec_.size();
}

Status FragmentInfo::get_cell_num(uint32_t fid, uint64_t* cell_num) const {
  if (cell_num == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment URI; Cell number argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  *cell_num = single_fragment_info_vec_[fid].cell_num();

  return Status::Ok();
}

Status FragmentInfo::get_total_cell_num(uint64_t* cell_num) const {
  if (cell_num == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cell number argument cannot be null"));

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

Status FragmentInfo::get_fragment_name(uint32_t fid, const char** name) const {
  if (name == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment name; Name argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  auto meta = single_fragment_info_vec_[fid].meta();
  auto meta_name =
      meta->fragment_uri().remove_trailing_slash().last_path_part();
  *name = meta_name.c_str();

  return Status::Ok();
}

Status FragmentInfo::get_fragment_size(uint32_t fid, uint64_t* size) const {
  if (size == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment URI; Size argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  *size = single_fragment_info_vec_[fid].fragment_size();

  return Status::Ok();
}

Status FragmentInfo::get_fragment_uri(uint32_t fid, const char** uri) const {
  if (uri == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment URI; URI argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  *uri = single_fragment_info_vec_[fid].uri().c_str();

  return Status::Ok();
}

Status FragmentInfo::get_to_vacuum_uri(uint32_t fid, const char** uri) const {
  if (uri == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get URI of fragment to vacuum; URI argument cannot be null"));

  if (fid >= to_vacuum_.size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get URI of fragment to vacuum; Invalid fragment index"));

  *uri = to_vacuum_[fid].c_str();

  return Status::Ok();
}

Status FragmentInfo::get_timestamp_range(
    uint32_t fid, uint64_t* start, uint64_t* end) const {
  if (start == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get timestamp range; Start argument cannot be null"));

  if (end == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get timestamp range; End argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  auto range = single_fragment_info_vec_[fid].timestamp_range();
  *start = range.first;
  *end = range.second;

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain(
    uint32_t fid, uint32_t did, void* domain) const {
  if (domain == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain; Domain argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain; Invalid fragment index"));

  const auto& non_empty_domain =
      single_fragment_info_vec_[fid].non_empty_domain();

  if (did >= non_empty_domain.size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain; Invalid dimension index"));

  if (non_empty_domain[did].var_size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain; Dimension is variable-sized"));

  assert(!non_empty_domain[did].empty());
  std::memcpy(
      domain, non_empty_domain[did].data(), non_empty_domain[did].size());

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain(
    uint32_t fid, const char* dim_name, void* domain) const {
  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain; Invalid fragment index"));
  if (dim_name == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain; Dimension name argument cannot be null"));

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
    auto msg =
        std::string("Cannot get non-empty domain; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status_FragmentInfoError(msg));
  }

  return get_non_empty_domain(fid, did, domain);
}

Status FragmentInfo::get_non_empty_domain_var_size(
    uint32_t fid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) const {
  if (start_size == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get non-empty domain var size; Start "
                                 "size argument cannot be null"));

  if (end_size == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get non-empty domain var size; End "
                                 "size argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain var size; Invalid fragment index"));

  const auto& non_empty_domain =
      single_fragment_info_vec_[fid].non_empty_domain();

  if (did >= non_empty_domain.size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain var size; Invalid dimension index"));

  if (!non_empty_domain[did].var_size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain var size; Dimension is fixed sized"));

  assert(!non_empty_domain[did].empty());
  *start_size = non_empty_domain[did].start_size();
  *end_size = non_empty_domain[did].end_size();

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain_var_size(
    uint32_t fid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) const {
  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get var-sized non-empty domain; Invalid fragment index"));
  if (dim_name == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get non-empty domain var size; "
                                 "Dimension name argument cannot be null"));

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
    auto msg =
        std::string(
            "Cannot get non-empty domain var size; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status_FragmentInfoError(msg));
  }

  return get_non_empty_domain_var_size(fid, did, start_size, end_size);
}

Status FragmentInfo::get_non_empty_domain_var(
    uint32_t fid, uint32_t did, void* start, void* end) const {
  if (start == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get non-empty domain var; Domain "
                                 "start argument cannot be null"));

  if (end == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain var; Domain end argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain var; Invalid fragment index"));

  const auto& non_empty_domain =
      single_fragment_info_vec_[fid].non_empty_domain();

  if (did >= non_empty_domain.size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain var; Invalid dimension index"));

  if (!non_empty_domain[did].var_size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain var; Dimension is fixed-sized"));

  assert(!non_empty_domain[did].empty());
  auto start_str = non_empty_domain[did].start_str();
  std::memcpy(start, start_str.data(), start_str.size());
  auto end_str = non_empty_domain[did].end_str();
  std::memcpy(end, end_str.data(), end_str.size());

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain_var(
    uint32_t fid, const char* dim_name, void* start, void* end) const {
  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain var; Invalid fragment index"));
  if (dim_name == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get non-empty domain var; Dimension "
                                 "name argument cannot be null"));

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
    auto msg =
        std::string(
            "Cannot get non-empty domain var; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status_FragmentInfoError(msg));
  }

  return get_non_empty_domain_var(fid, did, start, end);
}

Status FragmentInfo::get_mbr_num(uint32_t fid, uint64_t* mbr_num) {
  if (mbr_num == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment URI; MBR number argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  if (!single_fragment_info_vec_[fid].sparse()) {
    *mbr_num = 0;
    return Status::Ok();
  }

  auto meta = single_fragment_info_vec_[fid].meta();
  RETURN_NOT_OK(meta->load_rtree(enc_key_));
  *mbr_num = meta->mbrs().size();

  return Status::Ok();
}

Status FragmentInfo::get_mbr(
    uint32_t fid, uint32_t mid, uint32_t did, void* mbr) {
  if (mbr == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get MBR; mbr argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR; Invalid fragment index"));

  if (!single_fragment_info_vec_[fid].sparse())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR; Fragment is not sparse"));

  auto meta = single_fragment_info_vec_[fid].meta();
  RETURN_NOT_OK(meta->load_rtree(enc_key_));
  const auto& mbrs = meta->mbrs();

  if (mid >= mbrs.size())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR; Invalid MBR index"));

  const auto& minimum_bounding_rectangle = mbrs[mid];
  if (did >= minimum_bounding_rectangle.size())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR; Invalid dimension index"));

  if (minimum_bounding_rectangle[did].var_size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get MBR; Dimension is variable-sized"));

  assert(!minimum_bounding_rectangle[did].empty());
  std::memcpy(
      mbr,
      minimum_bounding_rectangle[did].data(),
      minimum_bounding_rectangle[did].size());

  return Status::Ok();
}

Status FragmentInfo::get_mbr(
    uint32_t fid, uint32_t mid, const char* dim_name, void* mbr) {
  if (fid >= fragment_num())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR; Invalid fragment index"));
  if (dim_name == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get non-empty domain; Dimension name argument cannot be null"));

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
    auto msg =
        std::string("Cannot get non-empty domain; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status_FragmentInfoError(msg));
  }

  return get_mbr(fid, mid, did, mbr);
}

Status FragmentInfo::get_mbr_var_size(
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (start_size == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR var size; Start "
                                 "size argument cannot be null"));

  if (end_size == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR var size; End "
                                 "size argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get MBR var size; Invalid fragment index"));

  if (!single_fragment_info_vec_[fid].sparse())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR; Fragment is not sparse"));

  auto meta = single_fragment_info_vec_[fid].meta();
  RETURN_NOT_OK(meta->load_rtree(enc_key_));
  const auto& mbrs = meta->mbrs();

  if (mid >= mbrs.size())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR; Invalid mbr index"));

  const auto& minimum_bounding_rectangle = mbrs[mid];

  if (did >= minimum_bounding_rectangle.size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get MBR var size; Invalid dimension index"));

  if (!minimum_bounding_rectangle[did].var_size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get MBR var size; Dimension is fixed sized"));

  assert(!minimum_bounding_rectangle[did].empty());
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
  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get MBR var size; Invalid fragment index"));
  if (dim_name == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR var size; "
                                 "Dimension name argument cannot be null"));

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
    auto msg =
        std::string("Cannot get MBR var size; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status_FragmentInfoError(msg));
  }

  return get_mbr_var_size(fid, mid, did, start_size, end_size);
}

Status FragmentInfo::get_mbr_var(
    uint32_t fid, uint32_t mid, uint32_t did, void* start, void* end) {
  if (start == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get MBR var; Start argument cannot be null"));

  if (end == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get MBR var; End argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR var; Invalid fragment index"));

  if (!single_fragment_info_vec_[fid].sparse())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR var; Fragment is not sparse"));

  auto meta = single_fragment_info_vec_[fid].meta();
  RETURN_NOT_OK(meta->load_rtree(enc_key_));
  const auto& mbrs = meta->mbrs();

  if (mid >= mbrs.size())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR var; Invalid mbr index"));

  const auto& minimum_bounding_rectangle = mbrs[mid];

  if (did >= minimum_bounding_rectangle.size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get MBR var; Invalid dimension index"));

  if (!minimum_bounding_rectangle[did].var_size())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get MBR var; Dimension is fixed-sized"));

  assert(!minimum_bounding_rectangle[did].empty());
  auto start_str = minimum_bounding_rectangle[did].start_str();
  std::memcpy(start, start_str.data(), start_str.size());
  auto end_str = minimum_bounding_rectangle[did].end_str();
  std::memcpy(end, end_str.data(), end_str.size());

  return Status::Ok();
}

Status FragmentInfo::get_mbr_var(
    uint32_t fid, uint32_t mid, const char* dim_name, void* start, void* end) {
  if (fid >= fragment_num())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR var; Invalid fragment index"));
  if (dim_name == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get MBR var; Dimension "
                                 "name argument cannot be null"));

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
    auto msg =
        std::string(
            "Cannot get non-empty domain var; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status_FragmentInfoError(msg));
  }

  return get_mbr_var(fid, mid, did, start, end);
}

Status FragmentInfo::get_version(uint32_t fid, uint32_t* version) const {
  if (version == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get version; Version argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot get version; Invalid fragment index"));

  *version = single_fragment_info_vec_[fid].format_version();

  return Status::Ok();
}

tuple<Status, optional<shared_ptr<ArraySchema>>> FragmentInfo::get_array_schema(
    uint32_t fid) {
  if (fid >= fragment_num())
    return {LOG_STATUS(Status_FragmentInfoError(
                "Cannot get array schema; Invalid fragment index")),
            nullopt};
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
  return storage_manager_->load_array_schema_from_uri(
      schema_uri, encryption_key);
}

Status FragmentInfo::get_array_schema_name(
    uint32_t fid, const char** schema_name) {
  if (schema_name == nullptr)
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get array schema URI; schema name argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(Status_FragmentInfoError(
        "Cannot get array schema name; Invalid fragment index"));

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
  if (has == nullptr)
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot check if fragment has consolidated "
                                 "metadata; Has argument cannot be null"));

  if (fid >= fragment_num())
    return LOG_STATUS(
        Status_FragmentInfoError("Cannot check if fragment has consolidated "
                                 "metadata; Invalid fragment index"));

  *has = single_fragment_info_vec_[fid].has_consolidated_footer();

  return Status::Ok();
}

Status FragmentInfo::load() {
  RETURN_NOT_OK(set_enc_key_from_config());
  RETURN_NOT_OK(set_default_timestamp_range());

  // Create an ArrayDirectory object and load
  auto array_dir = ArrayDirectory(
      storage_manager_->vfs(),
      storage_manager_->compute_tp(),
      array_uri_,
      timestamp_start_,
      timestamp_end_);

  return load(array_dir);
}

Status FragmentInfo::load(
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  RETURN_NOT_OK(enc_key_.set_key(encryption_type, encryption_key, key_length));
  RETURN_NOT_OK(set_default_timestamp_range());

  // Create an ArrayDirectory object and load
  auto array_dir = ArrayDirectory(
      storage_manager_->vfs(),
      storage_manager_->compute_tp(),
      array_uri_,
      timestamp_start_,
      timestamp_end_);
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
  if (array_uri_.is_tiledb()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr) {
      return LOG_STATUS(Status_ArrayError(
          "Cannot load fragment info; remote array with no REST client."));
    }

    return rest_client->post_fragment_info_from_rest(array_uri_, this);
  }

  // Get the array schemas and fragment metadata.
  auto&& [st_schemas, array_schema_latest, array_schemas_all, fragment_metadata] =
      storage_manager_->load_array_schemas_and_fragment_metadata(
          array_dir, nullptr, enc_key_);
  RETURN_NOT_OK(st_schemas);
  const auto& fragment_metadata_value = fragment_metadata.value();
  array_schemas_all_ = std::move(array_schemas_all.value());
  auto fragment_num = (uint32_t)fragment_metadata_value.size();

  // Get fragment sizes
  std::vector<uint64_t> sizes(fragment_num, 0);
  RETURN_NOT_OK(parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_num,
      [this, &fragment_metadata_value, &sizes](uint64_t i) {
        // Get fragment size. Applicable only to relevant fragments, including
        // fragments that are in the range [timestamp_start_, timestamp_end_].
        auto meta = fragment_metadata_value[i];
        if (meta->timestamp_range().first >= timestamp_start_ &&
            meta->timestamp_range().second <= timestamp_end_) {
          uint64_t size;
          RETURN_NOT_OK(meta->fragment_size(&size));
          sizes[i] = size;
        }

        return Status::Ok();
      }));

  // Clear single fragment info vec and anterior range
  single_fragment_info_vec_.clear();
  anterior_ndrange_.clear();

  // Create the vector that will store the SingleFragmentInfo objects
  for (uint64_t fid = 0; fid < fragment_num; fid++) {
    const auto meta = fragment_metadata_value[fid];
    const auto& array_schema = meta->array_schema();
    const auto& non_empty_domain = meta->non_empty_domain();

    if (meta->timestamp_range().first < timestamp_start_) {
      expand_anterior_ndrange(array_schema->domain(), non_empty_domain);
    } else if (meta->timestamp_range().second <= timestamp_end_) {
      const auto& uri = meta->fragment_uri();
      bool sparse = !meta->dense();

      // compute expanded non-empty domain (only for dense fragments)
      auto expanded_non_empty_domain = non_empty_domain;
      if (!sparse)
        array_schema->domain().expand_to_tiles(&expanded_non_empty_domain);

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

  return Status::Ok();
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
  auto vfs = storage_manager_->vfs();
  const auto& array_schema_latest =
      single_fragment_info_vec_.back().meta()->array_schema();

  // Get timestamp range
  std::pair<uint64_t, uint64_t> timestamp_range;
  RETURN_NOT_OK_TUPLE(
      utils::parse::get_timestamp_range(new_fragment_uri, &timestamp_range),
      nullopt);
  uint32_t version;
  auto name = new_fragment_uri.remove_trailing_slash().last_path_part();
  RETURN_NOT_OK_TUPLE(
      utils::parse::get_fragment_name_version(name, &version), nullopt);

  // Check if fragment is sparse
  bool sparse = false;
  if (version == 1) {  // This corresponds to format version <=2
    URI coords_uri =
        new_fragment_uri.join_path(constants::coords + constants::file_suffix);
    RETURN_NOT_OK_TUPLE(vfs->is_file(coords_uri, &sparse), nullopt);
  } else {
    // Do nothing. It does not matter what the `sparse` value
    // is, since the FragmentMetadata object will load the correct
    // value from the metadata file.

    // Also `sparse` is updated below after loading the metadata
  }

  // Get fragment non-empty domain
  auto meta = make_shared<FragmentMetadata>(
      HERE(),
      storage_manager_,
      nullptr,
      array_schema_latest,
      new_fragment_uri,
      timestamp_range,
      !sparse);
  RETURN_NOT_OK_TUPLE(
      meta->load(enc_key_, nullptr, 0, array_schemas_all_), nullopt);

  // This is important for format version > 2
  sparse = !meta->dense();

  // Get fragment size
  uint64_t size;
  RETURN_NOT_OK_TUPLE(meta->fragment_size(&size), nullopt);

  // Compute expanded non-empty domain only for dense fragments
  // Get non-empty domain, and compute expanded non-empty domain
  // (only for dense fragments)
  const auto& non_empty_domain = meta->non_empty_domain();
  auto expanded_non_empty_domain = non_empty_domain;
  if (!sparse)
    meta->array_schema()->domain().expand_to_tiles(&expanded_non_empty_domain);

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

FragmentInfo FragmentInfo::clone() const {
  FragmentInfo clone;
  clone.array_uri_ = array_uri_;
  clone.array_schemas_all_ = array_schemas_all_;
  clone.config_ = config_;
  clone.single_fragment_info_vec_ = single_fragment_info_vec_;
  clone.storage_manager_ = storage_manager_;
  clone.to_vacuum_ = to_vacuum_;
  clone.unconsolidated_metadata_num_ = unconsolidated_metadata_num_;
  clone.anterior_ndrange_ = anterior_ndrange_;
  clone.timestamp_start_ = timestamp_start_;
  clone.timestamp_end_ = timestamp_end_;

  return clone;
}

void FragmentInfo::swap(FragmentInfo& fragment_info) {
  std::swap(array_uri_, fragment_info.array_uri_);
  std::swap(array_schemas_all_, fragment_info.array_schemas_all_);
  std::swap(config_, fragment_info.config_);
  std::swap(single_fragment_info_vec_, fragment_info.single_fragment_info_vec_);
  std::swap(storage_manager_, fragment_info.storage_manager_);
  std::swap(to_vacuum_, fragment_info.to_vacuum_);
  std::swap(
      unconsolidated_metadata_num_, fragment_info.unconsolidated_metadata_num_);
  std::swap(anterior_ndrange_, fragment_info.anterior_ndrange_);
  std::swap(timestamp_start_, fragment_info.timestamp_start_);
  std::swap(timestamp_end_, fragment_info.timestamp_end_);
}
