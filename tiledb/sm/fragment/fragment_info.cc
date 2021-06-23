/**
 * @file   fragment_info.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
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
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb::sm;
using namespace tiledb::common;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

FragmentInfo::FragmentInfo()
    : storage_manager_(nullptr)
    , array_(nullptr)
    , unconsolidated_metadata_num_(0) {
}

FragmentInfo::FragmentInfo(
    const URI& array_uri, StorageManager* storage_manager)
    : array_uri_(array_uri)
    , storage_manager_(storage_manager)
    , array_(tdb_new(Array, array_uri_, storage_manager_))
    , unconsolidated_metadata_num_(0) {
}

FragmentInfo::~FragmentInfo() {
  if (array_->is_open())
    array_->close();
  tdb_delete(array_);
}

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

void FragmentInfo::append(const SingleFragmentInfo& fragment) {
  fragments_.emplace_back(fragment);
}

void FragmentInfo::expand_anterior_ndrange(
    const Domain* domain, const NDRange& range) {
  domain->expand_ndrange(range, &anterior_ndrange_);
}

void FragmentInfo::clear() {
  fragments_.clear();
  anterior_ndrange_.clear();
}

void FragmentInfo::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  std::stringstream ss;
  ss << "- Fragment num: " << fragments_.size() << "\n";
  ss << "- Unconsolidated metadata num: " << unconsolidated_metadata_num_
     << "\n";
  ss << "- To vacuum num: " << to_vacuum_.size() << "\n";

  if (!to_vacuum_.empty()) {
    ss << "- To vacuum URIs:\n";
    for (const auto& v : to_vacuum_)
      ss << "  > " << v.c_str() << "\n";
  }

  fprintf(out, "%s", ss.str().c_str());

  for (uint32_t i = 0; i < (uint32_t)fragments_.size(); ++i) {
    fprintf(out, "- Fragment #%u:\n", i + 1);
    fragments_[i].dump(dim_types_, out);
  }
}

Status FragmentInfo::get_dense(uint32_t fid, int32_t* dense) const {
  if (dense == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot check if fragment is dense; Dense argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot check if fragment is dense; Invalid fragment index"));

  *dense = (int32_t)!fragments_[fid].sparse();

  return Status::Ok();
}

Status FragmentInfo::get_sparse(uint32_t fid, int32_t* sparse) const {
  if (sparse == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot check if fragment is sparse; Sparse argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot check if fragment is sparse; Invalid fragment index"));

  *sparse = (int32_t)fragments_[fid].sparse();

  return Status::Ok();
}

uint32_t FragmentInfo::fragment_num() const {
  return (uint32_t)fragments_.size();
}

Status FragmentInfo::get_cell_num(uint32_t fid, uint64_t* cell_num) const {
  if (cell_num == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get fragment URI; Cell number argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  *cell_num = fragments_[fid].cell_num();

  return Status::Ok();
}

Status FragmentInfo::get_fragment_size(uint32_t fid, uint64_t* size) const {
  if (size == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get fragment URI; Size argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  *size = fragments_[fid].fragment_size();

  return Status::Ok();
}

Status FragmentInfo::get_fragment_uri(uint32_t fid, const char** uri) const {
  if (uri == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get fragment URI; URI argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  *uri = fragments_[fid].uri().c_str();

  return Status::Ok();
}

Status FragmentInfo::get_to_vacuum_uri(uint32_t fid, const char** uri) const {
  if (uri == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get URI of fragment to vacuum; URI argument cannot be null"));

  if (fid >= to_vacuum_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get URI of fragment to vacuum; Invalid fragment index"));

  *uri = to_vacuum_[fid].c_str();

  return Status::Ok();
}

Status FragmentInfo::get_timestamp_range(
    uint32_t fid, uint64_t* start, uint64_t* end) const {
  if (start == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get timestamp range; Start argument cannot be null"));

  if (end == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get timestamp range; End argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  auto range = fragments_[fid].timestamp_range();
  *start = range.first;
  *end = range.second;

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain(
    uint32_t fid, uint32_t did, void* domain) const {
  if (domain == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain; Domain argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain; Invalid fragment index"));

  const auto& non_empty_domain = fragments_[fid].non_empty_domain();

  if (did >= non_empty_domain.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain; Invalid dimension index"));

  if (non_empty_domain[did].var_size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain; Dimension is variable-sized"));

  assert(!non_empty_domain[did].empty());
  std::memcpy(
      domain, non_empty_domain[did].data(), non_empty_domain[did].size());

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain(
    uint32_t fid, const char* dim_name, void* domain) const {
  if (dim_name == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain; Dimension name argument cannot be null"));

  uint32_t did;
  for (did = 0; did < dim_names_.size(); ++did) {
    if (dim_name == dim_names_[did]) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_names_.size()) {
    auto msg =
        std::string("Cannot get non-empty domain; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status::FragmentInfoError(msg));
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
        Status::FragmentInfoError("Cannot get non-empty domain var size; Start "
                                  "size argument cannot be null"));

  if (end_size == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get non-empty domain var size; End "
                                  "size argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain var size; Invalid fragment index"));

  const auto& non_empty_domain = fragments_[fid].non_empty_domain();

  if (did >= non_empty_domain.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain var size; Invalid dimension index"));

  if (!non_empty_domain[did].var_size())
    return LOG_STATUS(Status::FragmentInfoError(
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
  if (dim_name == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get non-empty domain var size; "
                                  "Dimension name argument cannot be null"));

  uint32_t did;
  for (did = 0; did < dim_names_.size(); ++did) {
    if (dim_name == dim_names_[did]) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_names_.size()) {
    auto msg =
        std::string(
            "Cannot get non-empty domain var size; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status::FragmentInfoError(msg));
  }

  return get_non_empty_domain_var_size(fid, did, start_size, end_size);
}

Status FragmentInfo::get_non_empty_domain_var(
    uint32_t fid, uint32_t did, void* start, void* end) const {
  if (start == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get non-empty domain var; Domain "
                                  "start argument cannot be null"));

  if (end == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain var; Domain end argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain var; Invalid fragment index"));

  const auto& non_empty_domain = fragments_[fid].non_empty_domain();

  if (did >= non_empty_domain.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain var; Invalid dimension index"));

  if (!non_empty_domain[did].var_size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain var; Dimension is fixed-sized"));

  assert(!non_empty_domain[did].empty());
  std::memcpy(
      start, non_empty_domain[did].start(), non_empty_domain[did].start_size());
  std::memcpy(
      end, non_empty_domain[did].end(), non_empty_domain[did].end_size());

  return Status::Ok();
}

Status FragmentInfo::get_non_empty_domain_var(
    uint32_t fid, const char* dim_name, void* start, void* end) const {
  if (dim_name == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get non-empty domain var; Dimension "
                                  "name argument cannot be null"));

  uint32_t did;
  for (did = 0; did < dim_names_.size(); ++did) {
    if (dim_name == dim_names_[did]) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_names_.size()) {
    auto msg =
        std::string(
            "Cannot get non-empty domain var; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status::FragmentInfoError(msg));
  }

  return get_non_empty_domain_var(fid, did, start, end);
}

Status FragmentInfo::get_mbr_num(uint32_t fid, uint64_t* mbr_num) const {
  if (mbr_num == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get fragment URI; mbr number argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get fragment URI; Invalid fragment index"));

  auto meta = fragments_[fid].meta();

  if (!fragments_[fid].sparse()) {
    *mbr_num = 0;
    return Status::Ok();
  }

  auto key = fragments_[fid].encryption_key();
  RETURN_NOT_OK(meta->load_rtree(*key));
  *mbr_num = meta->mbrs().size();

  return Status::Ok();
}

Status FragmentInfo::get_mbr(
    uint32_t fid, uint32_t mid, uint32_t did, void* mbr) const {
  if (mbr == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get mbr; mbr cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR; Invalid fragment index"));

  if (!fragments_[fid].sparse())
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR; Fragment is not sparse"));

  auto meta = fragments_[fid].meta();

  auto key = fragments_[fid].encryption_key();
  RETURN_NOT_OK(meta->load_rtree(*key));
  const auto& mbrs = meta->mbrs();

  if (mid >= mbrs.size())
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR; Invalid mbr index"));

  const auto& minimum_bounding_rectangle = mbrs[mid];

  if (did >= minimum_bounding_rectangle.size())
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR; Invalid dimension index"));

  if (minimum_bounding_rectangle[did].var_size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get MBR; Dimension is variable-sized"));

  assert(!minimum_bounding_rectangle[did].empty());
  std::memcpy(
      mbr,
      minimum_bounding_rectangle[did].data(),
      minimum_bounding_rectangle[did].size());

  return Status::Ok();
}

Status FragmentInfo::get_mbr(
    uint32_t fid, uint32_t mid, const char* dim_name, void* mbr) const {
  if (dim_name == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain; Dimension name argument cannot be null"));

  uint32_t did;
  for (did = 0; did < dim_names_.size(); ++did) {
    if (dim_name == dim_names_[did]) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_names_.size()) {
    auto msg =
        std::string("Cannot get non-empty domain; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status::FragmentInfoError(msg));
  }

  return get_mbr(fid, mid, did, mbr);
}

Status FragmentInfo::get_mbr_var_size(
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) const {
  if (start_size == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR var size; Start "
                                  "size argument cannot be null"));

  if (end_size == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR var size; End "
                                  "size argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get MBR var size; Invalid fragment index"));

  if (!fragments_[fid].sparse())
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR; Fragment is not sparse"));

  auto meta = fragments_[fid].meta();
  auto key = fragments_[fid].encryption_key();
  RETURN_NOT_OK(meta->load_rtree(*key));
  const auto& mbrs = meta->mbrs();

  if (mid >= mbrs.size())
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR; Invalid mbr index"));

  const auto& minimum_bounding_rectangle = mbrs[mid];

  if (did >= minimum_bounding_rectangle.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get MBR var size; Invalid dimension index"));

  if (!minimum_bounding_rectangle[did].var_size())
    return LOG_STATUS(Status::FragmentInfoError(
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
    uint64_t* end_size) const {
  if (dim_name == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR var size; "
                                  "Dimension name argument cannot be null"));

  uint32_t did;
  for (did = 0; did < dim_names_.size(); ++did) {
    if (dim_name == dim_names_[did]) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_names_.size()) {
    auto msg =
        std::string("Cannot get MBR var size; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status::FragmentInfoError(msg));
  }

  return get_mbr_var_size(fid, mid, did, start_size, end_size);
}

Status FragmentInfo::get_mbr_var(
    uint32_t fid, uint32_t mid, uint32_t did, void* start, void* end) const {
  if (start == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get non-empty domain var; Domain "
                                  "start argument cannot be null"));

  if (end == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain var; Domain end argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain var; Invalid fragment index"));

  if (!fragments_[fid].sparse())
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR; Fragment is not sparse"));

  auto meta = fragments_[fid].meta();
  auto key = fragments_[fid].encryption_key();
  RETURN_NOT_OK(meta->load_rtree(*key));
  const auto& mbrs = meta->mbrs();

  if (mid >= mbrs.size())
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get MBR; Invalid mbr index"));

  const auto& minimum_bounding_rectangle = mbrs[mid];

  if (did >= minimum_bounding_rectangle.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain var; Invalid dimension index"));

  if (!minimum_bounding_rectangle[did].var_size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get non-empty domain var; Dimension is fixed-sized"));

  assert(!minimum_bounding_rectangle[did].empty());
  std::memcpy(
      start,
      minimum_bounding_rectangle[did].start(),
      minimum_bounding_rectangle[did].start_size());
  std::memcpy(
      end,
      minimum_bounding_rectangle[did].end(),
      minimum_bounding_rectangle[did].end_size());

  return Status::Ok();
}

Status FragmentInfo::get_mbr_var(
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* start,
    void* end) const {
  if (dim_name == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot get non-empty domain var; Dimension "
                                  "name argument cannot be null"));

  uint32_t did;
  for (did = 0; did < dim_names_.size(); ++did) {
    if (dim_name == dim_names_[did]) {
      break;
    }
  }

  // Dimension name not found
  if (did == dim_names_.size()) {
    auto msg =
        std::string(
            "Cannot get non-empty domain var; Invalid dimension name '") +
        dim_name + "'";
    return LOG_STATUS(Status::FragmentInfoError(msg));
  }

  return get_mbr_var(fid, mid, did, start, end);
}

Status FragmentInfo::get_version(uint32_t fid, uint32_t* version) const {
  if (version == nullptr)
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get version; Version argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(Status::FragmentInfoError(
        "Cannot get version; Invalid fragment index"));

  *version = fragments_[fid].format_version();

  return Status::Ok();
}

Status FragmentInfo::has_consolidated_metadata(
    uint32_t fid, int32_t* has) const {
  if (has == nullptr)
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot check if fragment has consolidated "
                                  "metadata; Has argument cannot be null"));

  if (fid >= fragments_.size())
    return LOG_STATUS(
        Status::FragmentInfoError("Cannot check if fragment has consolidated "
                                  "metadata; Invalid fragment index"));

  *has = fragments_[fid].has_consolidated_footer();

  return Status::Ok();
}

Status FragmentInfo::load(
    const Config config,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  bool is_array;
  RETURN_NOT_OK(storage_manager_->is_array(array_uri_, &is_array));
  if (!is_array) {
    auto msg = std::string("Cannot load fragment info; Array '") +
               array_uri_.to_string() + "' does not exist";
    return LOG_STATUS(Status::FragmentInfoError(msg));
  }

  if (array_->is_open())
    array_->close();

  if (encryption_type == EncryptionType::NO_ENCRYPTION) {
    bool found = false;
    std::string encryption_key_from_cfg =
        config.get("sm.encryption_key", &found);
    assert(found);
    std::string encryption_type_from_cfg =
        config.get("sm.encryption_type", &found);
    assert(found);
    RETURN_NOT_OK(
        encryption_type_enum(encryption_type_from_cfg, &encryption_type));
    EncryptionKey encryption_key_cfg;
    uint32_t key_length = 0;

    if (encryption_key_from_cfg.empty()) {
      RETURN_NOT_OK(encryption_key_cfg.set_key(encryption_type, nullptr, 0));
    } else {
      if (EncryptionKey::is_valid_key_length(
              encryption_type,
              static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
        const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
        if (unit_test_cfg.array_encryption_key_length.is_set()) {
          key_length = unit_test_cfg.array_encryption_key_length.get();
        } else {
          key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
        }
      }
    }
    RETURN_NOT_OK(array_->open_without_fragments(
        encryption_type,
        (const void*)encryption_key_from_cfg.c_str(),
        key_length));
  } else {
    RETURN_NOT_OK(array_->open_without_fragments(
        encryption_type, encryption_key, key_length));
  }

  auto timestamp = utils::time::timestamp_now_ms();
  RETURN_NOT_OK_ELSE(
      array_->get_fragment_info(0, timestamp, this, true), array_->close());

  unconsolidated_metadata_num_ = 0;
  for (const auto& f : fragments_)
    unconsolidated_metadata_num_ += (uint32_t)!f.has_consolidated_footer();

  return Status::Ok();
}

void FragmentInfo::set_dim_info(
    const std::vector<std::string>& dim_names,
    const std::vector<Datatype>& dim_types) {
  dim_names_ = dim_names;
  dim_types_ = dim_types;
}

void FragmentInfo::set_to_vacuum(const std::vector<URI>& to_vacuum) {
  to_vacuum_ = to_vacuum;
}

const std::vector<SingleFragmentInfo>& FragmentInfo::fragments() const {
  return fragments_;
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

FragmentInfo FragmentInfo::clone() const {
  FragmentInfo clone;
  clone.array_uri_ = array_uri_;
  clone.dim_names_ = dim_names_;
  clone.dim_types_ = dim_types_;
  clone.fragments_ = fragments_;
  clone.storage_manager_ = storage_manager_;
  clone.to_vacuum_ = to_vacuum_;
  clone.unconsolidated_metadata_num_ = unconsolidated_metadata_num_;
  clone.anterior_ndrange_ = anterior_ndrange_;

  return clone;
}

void FragmentInfo::swap(FragmentInfo& fragment_info) {
  std::swap(array_uri_, fragment_info.array_uri_);
  std::swap(dim_names_, fragment_info.dim_names_);
  std::swap(dim_types_, fragment_info.dim_types_);
  std::swap(fragments_, fragment_info.fragments_);
  std::swap(storage_manager_, fragment_info.storage_manager_);
  std::swap(to_vacuum_, fragment_info.to_vacuum_);
  std::swap(
      unconsolidated_metadata_num_, fragment_info.unconsolidated_metadata_num_);
  std::swap(anterior_ndrange_, fragment_info.anterior_ndrange_);
}
