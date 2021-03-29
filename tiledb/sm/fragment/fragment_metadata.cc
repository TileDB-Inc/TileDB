/**
 * @file   fragment_metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements the FragmentMetadata class.
 */

#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"

#include <cassert>
#include <iostream>
#include <string>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

FragmentMetadata::FragmentMetadata(
    StorageManager* storage_manager,
    const ArraySchema* array_schema,
    const URI& fragment_uri,
    const std::pair<uint64_t, uint64_t>& timestamp_range,
    bool dense)
    : storage_manager_(storage_manager)
    , array_schema_(array_schema)
    , dense_(dense)
    , fragment_uri_(fragment_uri)
    , timestamp_range_(timestamp_range) {
  has_consolidated_footer_ = false;
  rtree_ = RTree(array_schema_->domain(), constants::rtree_fanout);
  meta_file_size_ = 0;
  version_ = constants::format_version;
  tile_index_base_ = 0;
  sparse_tile_num_ = 0;
  footer_size_ = 0;
  footer_offset_ = 0;
  auto attributes = array_schema_->attributes();
  for (unsigned i = 0; i < attributes.size(); ++i) {
    auto attr_name = attributes[i]->name();
    idx_map_[attr_name] = i;
  }
  idx_map_[constants::coords] = array_schema_->attribute_num();
  for (unsigned i = 0; i < array_schema_->dim_num(); ++i) {
    auto dim_name = array_schema_->dimension(i)->name();
    idx_map_[dim_name] = array_schema_->attribute_num() + 1 + i;
  }
}

FragmentMetadata::~FragmentMetadata() = default;

/* ****************************** */
/*                API             */
/* ****************************** */

const URI& FragmentMetadata::array_uri() const {
  return array_schema_->array_uri();
}

Status FragmentMetadata::set_mbr(uint64_t tile, const NDRange& mbr) {
  // For easy reference
  tile += tile_index_base_;
  RETURN_NOT_OK(rtree_.set_leaf(tile, mbr));
  return expand_non_empty_domain(mbr);
}

void FragmentMetadata::set_tile_index_base(uint64_t tile_base) {
  tile_index_base_ = tile_base;
}

void FragmentMetadata::set_tile_offset(
    const std::string& name, uint64_t tid, uint64_t step) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  assert(tid < tile_offsets_[idx].size());
  tile_offsets_[idx][tid] = file_sizes_[idx];
  file_sizes_[idx] += step;
}

void FragmentMetadata::set_tile_var_offset(
    const std::string& name, uint64_t tid, uint64_t step) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  assert(tid < tile_var_offsets_[idx].size());
  tile_var_offsets_[idx][tid] = file_var_sizes_[idx];
  file_var_sizes_[idx] += step;
}

void FragmentMetadata::set_tile_var_size(
    const std::string& name, uint64_t tid, uint64_t size) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  assert(tid < tile_var_sizes_[idx].size());
  tile_var_sizes_[idx][tid] = size;
}

void FragmentMetadata::set_tile_validity_offset(
    const std::string& name, uint64_t tid, uint64_t step) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  assert(tid < tile_validity_offsets_[idx].size());
  tile_validity_offsets_[idx][tid] = file_validity_sizes_[idx];
  file_validity_sizes_[idx] += step;
}

uint64_t FragmentMetadata::cell_num() const {
  auto tile_num = this->tile_num();
  assert(tile_num != 0);
  if (dense_) {  // Dense fragment
    return tile_num * array_schema_->domain()->cell_num_per_tile();
  } else {  // Sparse fragment
    return (tile_num - 1) * array_schema_->capacity() + last_tile_cell_num();
  }
}

uint64_t FragmentMetadata::cell_num(uint64_t tile_pos) const {
  if (dense_)
    return array_schema_->domain()->cell_num_per_tile();

  uint64_t tile_num = this->tile_num();
  if (tile_pos != tile_num - 1)
    return array_schema_->capacity();

  return last_tile_cell_num();
}

Status FragmentMetadata::add_max_buffer_sizes(
    const EncryptionKey& encryption_key,
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Dense case
  if (dense_)
    return add_max_buffer_sizes_dense(encryption_key, subarray, buffer_sizes);

  // Convert subarray to NDRange
  auto dim_num = array_schema_->dim_num();
  auto sub_ptr = (const unsigned char*)subarray;
  NDRange sub_nd(dim_num);
  uint64_t offset = 0;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto r_size = 2 * array_schema_->dimension(d)->coord_size();
    sub_nd[d].set_range(&sub_ptr[offset], r_size);
    offset += r_size;
  }

  // Sparse case
  return add_max_buffer_sizes_sparse(encryption_key, sub_nd, buffer_sizes);
}

Status FragmentMetadata::add_max_buffer_sizes_dense(
    const EncryptionKey& encryption_key,
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Note: applicable only to the dense case where all dimensions
  // have the same type
  auto type = array_schema_->dimension(0)->type();
  switch (type) {
    case Datatype::INT32:
      return add_max_buffer_sizes_dense<int32_t>(
          encryption_key, static_cast<const int32_t*>(subarray), buffer_sizes);
    case Datatype::INT64:
      return add_max_buffer_sizes_dense<int64_t>(
          encryption_key, static_cast<const int64_t*>(subarray), buffer_sizes);
    case Datatype::FLOAT32:
      return add_max_buffer_sizes_dense<float>(
          encryption_key, static_cast<const float*>(subarray), buffer_sizes);
    case Datatype::FLOAT64:
      return add_max_buffer_sizes_dense<double>(
          encryption_key, static_cast<const double*>(subarray), buffer_sizes);
    case Datatype::INT8:
      return add_max_buffer_sizes_dense<int8_t>(
          encryption_key, static_cast<const int8_t*>(subarray), buffer_sizes);
    case Datatype::UINT8:
      return add_max_buffer_sizes_dense<uint8_t>(
          encryption_key, static_cast<const uint8_t*>(subarray), buffer_sizes);
    case Datatype::INT16:
      return add_max_buffer_sizes_dense<int16_t>(
          encryption_key, static_cast<const int16_t*>(subarray), buffer_sizes);
    case Datatype::UINT16:
      return add_max_buffer_sizes_dense<uint16_t>(
          encryption_key, static_cast<const uint16_t*>(subarray), buffer_sizes);
    case Datatype::UINT32:
      return add_max_buffer_sizes_dense<uint32_t>(
          encryption_key, static_cast<const uint32_t*>(subarray), buffer_sizes);
    case Datatype::UINT64:
      return add_max_buffer_sizes_dense<uint64_t>(
          encryption_key, static_cast<const uint64_t*>(subarray), buffer_sizes);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      return add_max_buffer_sizes_dense<int64_t>(
          encryption_key, static_cast<const int64_t*>(subarray), buffer_sizes);
    default:
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot compute add read buffer sizes for dense array; Unsupported "
          "domain type"));
  }

  return Status::Ok();
}

template <class T>
Status FragmentMetadata::add_max_buffer_sizes_dense(
    const EncryptionKey& encryption_key,
    const T* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Calculate the ids of all tiles overlapping with subarray
  auto tids = compute_overlapping_tile_ids(subarray);
  uint64_t size = 0;

  // Compute buffer sizes
  for (auto& tid : tids) {
    for (auto& it : *buffer_sizes) {
      if (array_schema_->var_size(it.first)) {
        auto cell_num = this->cell_num(tid);
        it.second.first += cell_num * constants::cell_var_offset_size;
        RETURN_NOT_OK(tile_var_size(encryption_key, it.first, tid, &size));
        it.second.second += size;
      } else {
        it.second.first += cell_num(tid) * array_schema_->cell_size(it.first);
      }
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::add_max_buffer_sizes_sparse(
    const EncryptionKey& encryption_key,
    const NDRange& subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  RETURN_NOT_OK(load_rtree(encryption_key));

  // Get tile overlap
  auto tile_overlap = rtree_.get_tile_overlap(subarray);
  uint64_t size = 0;

  // Handle tile ranges
  for (const auto& tr : tile_overlap.tile_ranges_) {
    for (uint64_t tid = tr.first; tid <= tr.second; ++tid) {
      for (auto& it : *buffer_sizes) {
        if (array_schema_->var_size(it.first)) {
          auto cell_num = this->cell_num(tid);
          it.second.first += cell_num * constants::cell_var_offset_size;
          RETURN_NOT_OK(tile_var_size(encryption_key, it.first, tid, &size));
          it.second.second += size;
        } else {
          it.second.first += cell_num(tid) * array_schema_->cell_size(it.first);
        }
      }
    }
  }

  // Handle individual tiles
  for (const auto& t : tile_overlap.tiles_) {
    auto tid = t.first;
    for (auto& it : *buffer_sizes) {
      if (array_schema_->var_size(it.first)) {
        auto cell_num = this->cell_num(tid);
        it.second.first += cell_num * constants::cell_var_offset_size;
        RETURN_NOT_OK(tile_var_size(encryption_key, it.first, tid, &size));
        it.second.second += size;
      } else {
        it.second.first += cell_num(tid) * array_schema_->cell_size(it.first);
      }
    }
  }

  return Status::Ok();
}

bool FragmentMetadata::dense() const {
  return dense_;
}

const NDRange& FragmentMetadata::domain() const {
  return domain_;
}

uint32_t FragmentMetadata::format_version() const {
  return version_;
}

Status FragmentMetadata::fragment_size(uint64_t* size) const {
  // Add file sizes
  *size = 0;
  for (const auto& file_size : file_sizes_)
    *size += file_size;
  for (const auto& file_var_size : file_var_sizes_)
    *size += file_var_size;
  for (const auto& file_validity_size : file_validity_sizes_)
    *size += file_validity_size;

  // The fragment metadata file size can be empty when we've loaded consolidated
  // metadata
  uint64_t meta_file_size = meta_file_size_;
  if (meta_file_size == 0) {
    auto meta_uri = fragment_uri_.join_path(
        std::string(constants::fragment_metadata_filename));
    RETURN_NOT_OK(
        storage_manager_->vfs()->file_size(meta_uri, &meta_file_size));
  }
  // Validate that the meta_file_size is not zero, either preloaded or fetched
  // above
  assert(meta_file_size != 0);

  // Add fragment metadata file size
  *size += meta_file_size;

  return Status::Ok();
}

const URI& FragmentMetadata::fragment_uri() const {
  return fragment_uri_;
}

bool FragmentMetadata::has_consolidated_footer() const {
  return has_consolidated_footer_;
}

bool FragmentMetadata::overlaps_non_empty_domain(const NDRange& range) const {
  return array_schema_->domain()->overlap(range, non_empty_domain_);
}

Status FragmentMetadata::get_tile_overlap(
    const NDRange& range, TileOverlap* tile_overlap) {
  assert(version_ <= 2 || loaded_metadata_.rtree_);
  *tile_overlap = rtree_.get_tile_overlap(range);
  return Status::Ok();
}

Status FragmentMetadata::init(const NDRange& non_empty_domain) {
  // For easy reference
  auto dim_num = array_schema_->dim_num();
  auto num = array_schema_->attribute_num() + dim_num + 1;
  auto domain = array_schema_->domain();

  // Sanity check
  assert(!non_empty_domain.empty());
  assert(non_empty_domain_.empty());
  assert(domain_.empty());

  // Set non-empty domain for dense arrays (for sparse it will be calculated
  // via the MBRs)
  if (dense_) {
    non_empty_domain_ = non_empty_domain;

    // The following is needed in case the fragment is a result of
    // dense consolidation, as the consolidator may have expanded
    // the fragment domain beyond the array domain to include
    // integral space tiles
    domain->crop_ndrange(&non_empty_domain_);

    // Set expanded domain
    domain_ = non_empty_domain_;
    domain->expand_to_tiles(&domain_);
  }

  // Set last tile cell number
  last_tile_cell_num_ = 0;

  // Initialize tile offsets
  tile_offsets_.resize(num);
  tile_offsets_mtx_.resize(num);
  file_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i)
    file_sizes_[i] = 0;

  // Initialize variable tile offsets
  tile_var_offsets_.resize(num);
  tile_var_offsets_mtx_.resize(num);
  file_var_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i)
    file_var_sizes_[i] = 0;

  // Initialize variable tile sizes
  tile_var_sizes_.resize(num);

  // Initialize validity tile offsets
  tile_validity_offsets_.resize(num);
  file_validity_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i)
    file_validity_sizes_[i] = 0;

  return Status::Ok();
}

uint64_t FragmentMetadata::last_tile_cell_num() const {
  return last_tile_cell_num_;
}

Status FragmentMetadata::load(
    const EncryptionKey& encryption_key, Buffer* f_buff, uint64_t offset) {
  auto meta_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));
  // Load the metadata file size when we are not reading from consolidated
  // buffer
  if (f_buff == nullptr)
    RETURN_NOT_OK(
        storage_manager_->vfs()->file_size(meta_uri, &meta_file_size_));

  // Get fragment name version
  uint32_t f_version;
  auto name = fragment_uri_.remove_trailing_slash().last_path_part();
  RETURN_NOT_OK(utils::parse::get_fragment_name_version(name, &f_version));

  // Note: The fragment name version is different from the fragment format
  // version.
  //  - Version 1 corresponds to format versions 1 and 2
  //    * __uuid_<t1>{_t2}
  //  - Version 2 corresponds to version 3 and 4
  //    * __t1_t2_uuid
  //  - Version 3 corresponds to version 5 or higher
  //    * __t1_t2_uuid_version
  if (f_version == 1)
    return load_v1_v2(encryption_key);
  return load_v3_or_higher(encryption_key, f_buff, offset);
}

Status FragmentMetadata::store(const EncryptionKey& encryption_key) {
  STATS_START_TIMER(stats::Stats::TimerType::WRITE_STORE_FRAG_META)

  auto array_uri = this->array_uri();
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  uint64_t offset = 0, nbytes;

  // Store R-Tree
  gt_offsets_.rtree_ = offset;
  RETURN_NOT_OK_ELSE(store_rtree(encryption_key, &nbytes), clean_up());
  offset += nbytes;

  // Store tile offsets
  gt_offsets_.tile_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_offsets_[i] = offset;
    RETURN_NOT_OK_ELSE(
        store_tile_offsets(i, encryption_key, &nbytes), clean_up());
    offset += nbytes;
  }

  // Store tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_var_offsets_[i] = offset;
    RETURN_NOT_OK_ELSE(
        store_tile_var_offsets(i, encryption_key, &nbytes), clean_up());
    offset += nbytes;
  }

  // Store tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_var_sizes_[i] = offset;
    RETURN_NOT_OK_ELSE(
        store_tile_var_sizes(i, encryption_key, &nbytes), clean_up());
    offset += nbytes;
  }

  // Store validity tile offsets
  if (version_ >= 7) {
    gt_offsets_.tile_validity_offsets_.resize(num);
    for (unsigned int i = 0; i < num; ++i) {
      gt_offsets_.tile_validity_offsets_[i] = offset;
      RETURN_NOT_OK_ELSE(
          store_tile_validity_offsets(i, encryption_key, &nbytes), clean_up());
      offset += nbytes;
    }
  }

  // Store footer
  RETURN_NOT_OK_ELSE(store_footer(encryption_key), clean_up());

  // Close file
  return storage_manager_->close_file(fragment_metadata_uri);

  STATS_END_TIMER(stats::Stats::TimerType::WRITE_STORE_FRAG_META)
}

const NDRange& FragmentMetadata::non_empty_domain() {
  return non_empty_domain_;
}

Status FragmentMetadata::set_num_tiles(uint64_t num_tiles) {
  auto num = array_schema_->attribute_num() + 1 + array_schema_->dim_num();

  for (unsigned i = 0; i < num; i++) {
    assert(num_tiles >= tile_offsets_[i].size());
    tile_offsets_[i].resize(num_tiles, 0);
    tile_var_offsets_[i].resize(num_tiles, 0);
    tile_var_sizes_[i].resize(num_tiles, 0);
    tile_validity_offsets_[i].resize(num_tiles, 0);
  }

  if (!dense_) {
    rtree_.set_leaf_num(num_tiles);
    sparse_tile_num_ = num_tiles;
  }

  return Status::Ok();
}

void FragmentMetadata::set_last_tile_cell_num(uint64_t cell_num) {
  last_tile_cell_num_ = cell_num;
}

uint64_t FragmentMetadata::tile_index_base() const {
  return tile_index_base_;
}

uint64_t FragmentMetadata::tile_num() const {
  if (dense_)
    return array_schema_->domain()->tile_num(domain_);

  return sparse_tile_num_;
}

std::string FragmentMetadata::encode_name(const std::string& name) const {
  if (version_ <= 7)
    return name;

  if (version_ == 8) {
    static const std::unordered_map<char, std::string> percent_encoding{
        // RFC 3986
        {'!', "%21"},
        {'#', "%23"},
        {'$', "%24"},
        {'%', "%25"},
        {'&', "%26"},
        {'\'', "%27"},
        {'(', "%28"},
        {')', "%29"},
        {'*', "%2A"},
        {'+', "%2B"},
        {',', "%2C"},
        {'/', "%2F"},
        {':', "%3A"},
        {';', "%3B"},
        {'=', "%3D"},
        {'?', "%3F"},
        {'@', "%40"},
        {'[', "%5B"},
        {']', "%5D"},
        // Extra encodings to cover illegal characters on Windows
        {'\"', "%22"},
        {'<', "%20"},
        {'>', "%2D"},
        {'\\', "%30"},
        {'|', "%3C"}};

    std::stringstream percent_encoded_name;
    for (const char c : name) {
      if (percent_encoding.count(c) == 0)
        percent_encoded_name << c;
      else
        percent_encoded_name << percent_encoding.at(c);
    }

    return percent_encoded_name.str();
  }

  assert(version_ > 8);
  const auto iter = idx_map_.find(name);
  if (iter == idx_map_.end())
    LOG_FATAL("Name " + name + " not in idx_map_");
  const unsigned idx = iter->second;

  const std::vector<tiledb::sm::Attribute*> attributes =
      array_schema_->attributes();
  for (unsigned i = 0; i < attributes.size(); ++i) {
    const std::string attr_name = attributes[i]->name();
    if (attr_name == name) {
      return "a" + std::to_string(idx);
    }
  }

  for (unsigned i = 0; i < array_schema_->dim_num(); ++i) {
    const std::string dim_name = array_schema_->dimension(i)->name();
    if (dim_name == name) {
      const unsigned dim_idx = idx - array_schema_->attribute_num() - 1;
      return "d" + std::to_string(dim_idx);
    }
  }

  if (name == constants::coords) {
    return name;
  }

  LOG_FATAL("Unable to locate dimension/attribute " + name);
  return "";
}

URI FragmentMetadata::uri(const std::string& name) const {
  return fragment_uri_.join_path(encode_name(name) + constants::file_suffix);
}

URI FragmentMetadata::var_uri(const std::string& name) const {
  return fragment_uri_.join_path(
      encode_name(name) + "_var" + constants::file_suffix);
}

URI FragmentMetadata::validity_uri(const std::string& name) const {
  return fragment_uri_.join_path(
      encode_name(name) + "_validity" + constants::file_suffix);
}

Status FragmentMetadata::load_tile_offsets(
    const EncryptionKey& encryption_key, std::vector<std::string>&& names) {
  // Sort 'names' in ascending order of their index. The
  // motivation is to load the offsets in order of their
  // layout for sequential reads to the file.
  std::sort(
      names.begin(),
      names.end(),
      [&](const std::string& lhs, const std::string& rhs) {
        assert(idx_map_.count(lhs) > 0);
        assert(idx_map_.count(rhs) > 0);
        return idx_map_[lhs] < idx_map_[rhs];
      });

  // The fixed offsets are located before the
  // var offsets. Load all of the fixed offsets
  // first.
  for (const auto& name : names) {
    RETURN_NOT_OK(load_tile_offsets(encryption_key, idx_map_[name]));
  }

  // Load all of the var offsets.
  for (const auto& name : names) {
    if (array_schema_->var_size(name))
      RETURN_NOT_OK(load_tile_var_offsets(encryption_key, idx_map_[name]));
  }

  // Load all of the var offsets.
  for (const auto& name : names) {
    if (array_schema_->is_nullable(name))
      RETURN_NOT_OK(load_tile_validity_offsets(encryption_key, idx_map_[name]));
  }

  return Status::Ok();
}

Status FragmentMetadata::file_offset(
    const EncryptionKey& encryption_key,
    const std::string& name,
    uint64_t tile_idx,
    uint64_t* offset) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  RETURN_NOT_OK(load_tile_offsets(encryption_key, idx));
  *offset = tile_offsets_[idx][tile_idx];
  return Status::Ok();
}

Status FragmentMetadata::file_var_offset(
    const EncryptionKey& encryption_key,
    const std::string& name,
    uint64_t tile_idx,
    uint64_t* offset) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  RETURN_NOT_OK(load_tile_var_offsets(encryption_key, idx));
  *offset = tile_var_offsets_[idx][tile_idx];
  return Status::Ok();
}

Status FragmentMetadata::file_validity_offset(
    const EncryptionKey& encryption_key,
    const std::string& name,
    uint64_t tile_idx,
    uint64_t* offset) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  RETURN_NOT_OK(load_tile_validity_offsets(encryption_key, idx));
  *offset = tile_validity_offsets_[idx][tile_idx];
  return Status::Ok();
}

const NDRange& FragmentMetadata::mbr(uint64_t tile_idx) const {
  return rtree_.leaf(tile_idx);
}

const std::vector<NDRange>& FragmentMetadata::mbrs() const {
  return rtree_.leaves();
}

Status FragmentMetadata::persisted_tile_size(
    const EncryptionKey& encryption_key,
    const std::string& name,
    uint64_t tile_idx,
    uint64_t* tile_size) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  RETURN_NOT_OK(load_tile_offsets(encryption_key, idx));

  auto tile_num = this->tile_num();

  *tile_size =
      (tile_idx != tile_num - 1) ?
          tile_offsets_[idx][tile_idx + 1] - tile_offsets_[idx][tile_idx] :
          file_sizes_[idx] - tile_offsets_[idx][tile_idx];

  return Status::Ok();
}

Status FragmentMetadata::persisted_tile_var_size(
    const EncryptionKey& encryption_key,
    const std::string& name,
    uint64_t tile_idx,
    uint64_t* tile_size) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  RETURN_NOT_OK(load_tile_var_offsets(encryption_key, idx));

  auto tile_num = this->tile_num();

  *tile_size = (tile_idx != tile_num - 1) ?
                   tile_var_offsets_[idx][tile_idx + 1] -
                       tile_var_offsets_[idx][tile_idx] :
                   file_var_sizes_[idx] - tile_var_offsets_[idx][tile_idx];

  return Status::Ok();
}

Status FragmentMetadata::persisted_tile_validity_size(
    const EncryptionKey& encryption_key,
    const std::string& name,
    uint64_t tile_idx,
    uint64_t* tile_size) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  RETURN_NOT_OK(load_tile_validity_offsets(encryption_key, idx));

  auto tile_num = this->tile_num();

  *tile_size =
      (tile_idx != tile_num - 1) ?
          tile_validity_offsets_[idx][tile_idx + 1] -
              tile_validity_offsets_[idx][tile_idx] :
          file_validity_sizes_[idx] - tile_validity_offsets_[idx][tile_idx];

  return Status::Ok();
}

uint64_t FragmentMetadata::tile_size(
    const std::string& name, uint64_t tile_idx) const {
  auto var_size = array_schema_->var_size(name);
  auto cell_num = this->cell_num(tile_idx);
  return (var_size) ? cell_num * constants::cell_var_offset_size :
                      cell_num * array_schema_->cell_size(name);
}

Status FragmentMetadata::tile_var_size(
    const EncryptionKey& encryption_key,
    const std::string& name,
    uint64_t tile_idx,
    uint64_t* tile_size) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  RETURN_NOT_OK(load_tile_var_sizes(encryption_key, idx));
  *tile_size = tile_var_sizes_[idx][tile_idx];

  return Status::Ok();
}

uint64_t FragmentMetadata::first_timestamp() const {
  return timestamp_range_.first;
}

const std::pair<uint64_t, uint64_t>& FragmentMetadata::timestamp_range() const {
  return timestamp_range_;
}

bool FragmentMetadata::operator<(const FragmentMetadata& metadata) const {
  return (timestamp_range_.first < metadata.timestamp_range_.first) ||
         (timestamp_range_.first == metadata.timestamp_range_.first &&
          fragment_uri_ < metadata.fragment_uri_);
}

Status FragmentMetadata::write_footer(Buffer* buff) const {
  RETURN_NOT_OK(write_version(buff));
  RETURN_NOT_OK(write_dense(buff));
  RETURN_NOT_OK(write_non_empty_domain(buff));
  RETURN_NOT_OK(write_sparse_tile_num(buff));
  RETURN_NOT_OK(write_last_tile_cell_num(buff));
  RETURN_NOT_OK(write_file_sizes(buff));
  RETURN_NOT_OK(write_file_var_sizes(buff));
  RETURN_NOT_OK(write_file_validity_sizes(buff));
  RETURN_NOT_OK(write_generic_tile_offsets(buff));

  return Status::Ok();
}

Status FragmentMetadata::load_rtree(const EncryptionKey& encryption_key) {
  if (version_ <= 2)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.rtree_)
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(
      read_generic_tile_from_file(encryption_key, gt_offsets_.rtree_, &buff));

  STATS_ADD_COUNTER(stats::Stats::CounterType::READ_RTREE_SIZE, buff.size());

  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(rtree_.deserialize(&cbuff, array_schema_->domain(), version_));

  loaded_metadata_.rtree_ = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_var_sizes(
    const EncryptionKey& encryption_key, const std::string& name) {
  if (version_ <= 2)
    return Status::Ok();

  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  return (load_tile_var_sizes(encryption_key, idx));
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status FragmentMetadata::get_footer_size(
    uint32_t version, uint64_t* size) const {
  if (version < 3) {
    *size = footer_size_v3_v4();
  } else if (version < 4) {
    *size = footer_size_v5_v6();
  } else {
    *size = footer_size_v7_or_higher();
  }

  return Status::Ok();
}

uint64_t FragmentMetadata::footer_size() const {
  return footer_size_;
}

Status FragmentMetadata::get_footer_offset_and_size(
    uint64_t* offset, uint64_t* size) const {
  if (array_schema_->domain()->all_dims_fixed()) {
    uint32_t f_version;
    auto name = fragment_uri_.remove_trailing_slash().last_path_part();
    RETURN_NOT_OK(utils::parse::get_fragment_name_version(name, &f_version));
    RETURN_NOT_OK(get_footer_size(f_version, size));
    *offset = meta_file_size_ - *size;
  } else {
    URI fragment_metadata_uri = fragment_uri_.join_path(
        std::string(constants::fragment_metadata_filename));
    uint64_t size_offset = meta_file_size_ - sizeof(uint64_t);
    Buffer buff;
    RETURN_NOT_OK(storage_manager_->read(
        fragment_metadata_uri, size_offset, &buff, sizeof(uint64_t)));
    buff.reset_offset();
    RETURN_NOT_OK(buff.read(size, sizeof(uint64_t)));
    *offset = meta_file_size_ - *size - sizeof(uint64_t);
    STATS_ADD_COUNTER(
        stats::Stats::CounterType::READ_FRAG_META_SIZE, sizeof(uint64_t));
  }

  return Status::Ok();
}

uint64_t FragmentMetadata::footer_size_v3_v4() const {
  auto attribute_num = array_schema_->attribute_num();
  auto dim_num = array_schema_->dim_num();
  // v3 and v4 support only arrays where all dimensions have the same type
  auto domain_size = 2 * dim_num * array_schema_->dimension(0)->coord_size();

  // Get footer size
  uint64_t size = 0;
  size += sizeof(uint32_t);                        // version
  size += sizeof(char);                            // dense
  size += sizeof(char);                            // null non-empty domain
  size += domain_size;                             // non-empty domain
  size += sizeof(uint64_t);                        // sparse tile num
  size += sizeof(uint64_t);                        // last tile cell num
  size += (attribute_num + 1) * sizeof(uint64_t);  // file sizes
  size += attribute_num * sizeof(uint64_t);        // file var sizes
  size += sizeof(uint64_t);                        // R-Tree offset
  size += (attribute_num + 1) * sizeof(uint64_t);  // tile offsets
  size += attribute_num * sizeof(uint64_t);        // tile var offsets
  size += attribute_num * sizeof(uint64_t);        // tile var sizes

  return size;
}

uint64_t FragmentMetadata::footer_size_v5_v6() const {
  auto dim_num = array_schema_->dim_num();
  auto num = array_schema_->attribute_num() + dim_num + 1;
  uint64_t domain_size = 0;

  if (non_empty_domain_.empty()) {
    // For var-sized dimensions, this function would be called only upon
    // writing the footer to storage, in which case the non-empty domain
    // would not be empty. For reading the footer from storage, the footer
    // size is explicitly stored to and retrieved from storage, so this
    // function is not called then.
    assert(array_schema_->domain()->all_dims_fixed());
    for (unsigned d = 0; d < dim_num; ++d)
      domain_size += 2 * array_schema_->domain()->dimension(d)->coord_size();
  } else {
    for (unsigned d = 0; d < dim_num; ++d) {
      domain_size += non_empty_domain_[d].size();
      if (array_schema_->dimension(d)->var_size())
        domain_size += 2 * sizeof(uint64_t);  // Two more sizes get serialized
    }
  }

  // Get footer size
  uint64_t size = 0;
  size += sizeof(uint32_t);        // version
  size += sizeof(char);            // dense
  size += sizeof(char);            // null non-empty domain
  size += domain_size;             // non-empty domain
  size += sizeof(uint64_t);        // sparse tile num
  size += sizeof(uint64_t);        // last tile cell num
  size += num * sizeof(uint64_t);  // file sizes
  size += num * sizeof(uint64_t);  // file var sizes
  size += sizeof(uint64_t);        // R-Tree offset
  size += num * sizeof(uint64_t);  // tile offsets
  size += num * sizeof(uint64_t);  // tile var offsets
  size += num * sizeof(uint64_t);  // tile var sizes

  return size;
}

uint64_t FragmentMetadata::footer_size_v7_or_higher() const {
  auto dim_num = array_schema_->dim_num();
  auto num = array_schema_->attribute_num() + dim_num + 1;
  uint64_t domain_size = 0;

  if (non_empty_domain_.empty()) {
    // For var-sized dimensions, this function would be called only upon
    // writing the footer to storage, in which case the non-empty domain
    // would not be empty. For reading the footer from storage, the footer
    // size is explicitly stored to and retrieved from storage, so this
    // function is not called then.
    assert(array_schema_->domain()->all_dims_fixed());
    for (unsigned d = 0; d < dim_num; ++d)
      domain_size += 2 * array_schema_->domain()->dimension(d)->coord_size();
  } else {
    for (unsigned d = 0; d < dim_num; ++d) {
      domain_size += non_empty_domain_[d].size();
      if (array_schema_->dimension(d)->var_size())
        domain_size += 2 * sizeof(uint64_t);  // Two more sizes get serialized
    }
  }

  // Get footer size
  uint64_t size = 0;
  size += sizeof(uint32_t);        // version
  size += sizeof(char);            // dense
  size += sizeof(char);            // null non-empty domain
  size += domain_size;             // non-empty domain
  size += sizeof(uint64_t);        // sparse tile num
  size += sizeof(uint64_t);        // last tile cell num
  size += num * sizeof(uint64_t);  // file sizes
  size += num * sizeof(uint64_t);  // file var sizes
  size += num * sizeof(uint64_t);  // file validity sizes
  size += sizeof(uint64_t);        // R-Tree offset
  size += num * sizeof(uint64_t);  // tile offsets
  size += num * sizeof(uint64_t);  // tile var offsets
  size += num * sizeof(uint64_t);  // tile var sizes
  size += num * sizeof(uint64_t);  // tile validity sizes

  return size;
}

template <class T>
std::vector<uint64_t> FragmentMetadata::compute_overlapping_tile_ids(
    const T* subarray) const {
  assert(dense_);
  std::vector<uint64_t> tids;
  auto dim_num = array_schema_->dim_num();

  // Temporary domain vector
  auto coord_size = array_schema_->domain()->dimension(0)->coord_size();
  auto temp_size = 2 * dim_num * coord_size;
  std::vector<uint8_t> temp(temp_size);
  uint8_t offset = 0;
  for (unsigned d = 0; d < dim_num; ++d) {
    std::memcpy(&temp[offset], domain_[d].data(), domain_[d].size());
    offset += domain_[d].size();
  }
  auto metadata_domain = (const T*)&temp[0];

  // Check if there is any overlap
  if (!utils::geometry::overlap(subarray, metadata_domain, dim_num))
    return tids;

  // Initialize subarray tile domain
  auto subarray_tile_domain = tdb_new_array(T, 2 * dim_num);
  get_subarray_tile_domain(subarray, subarray_tile_domain);

  // Initialize tile coordinates
  auto tile_coords = tdb_new_array(T, dim_num);
  for (unsigned int i = 0; i < dim_num; ++i)
    tile_coords[i] = subarray_tile_domain[2 * i];

  // Walk through all tiles in subarray tile domain
  auto domain = array_schema_->domain();
  uint64_t tile_pos;
  do {
    tile_pos = domain->get_tile_pos(metadata_domain, tile_coords);
    tids.emplace_back(tile_pos);
    domain->get_next_tile_coords(subarray_tile_domain, tile_coords);
  } while (utils::geometry::coords_in_rect(
      tile_coords, subarray_tile_domain, dim_num));

  // Clean up
  tdb_delete_array(subarray_tile_domain);
  tdb_delete_array(tile_coords);

  return tids;
}

template <class T>
std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov(const T* subarray) const {
  assert(dense_);
  std::vector<std::pair<uint64_t, double>> tids;
  auto dim_num = array_schema_->dim_num();

  // Temporary domain vector
  auto coord_size = array_schema_->domain()->dimension(0)->coord_size();
  auto temp_size = 2 * dim_num * coord_size;
  std::vector<uint8_t> temp(temp_size);
  uint8_t offset = 0;
  for (unsigned d = 0; d < dim_num; ++d) {
    std::memcpy(&temp[offset], domain_[d].data(), domain_[d].size());
    offset += domain_[d].size();
  }
  auto metadata_domain = (const T*)&temp[0];

  // Check if there is any overlap
  if (!utils::geometry::overlap(subarray, metadata_domain, dim_num))
    return tids;

  // Initialize subarray tile domain
  auto subarray_tile_domain = tdb_new_array(T, 2 * dim_num);
  get_subarray_tile_domain(subarray, subarray_tile_domain);

  auto tile_subarray = tdb_new_array(T, 2 * dim_num);
  auto tile_overlap = tdb_new_array(T, 2 * dim_num);
  bool overlap;
  double cov;

  // Initialize tile coordinates
  auto tile_coords = tdb_new_array(T, dim_num);
  for (unsigned int i = 0; i < dim_num; ++i)
    tile_coords[i] = subarray_tile_domain[2 * i];

  // Walk through all tiles in subarray tile domain
  auto domain = array_schema_->domain();
  uint64_t tile_pos;
  do {
    domain->get_tile_subarray(metadata_domain, tile_coords, tile_subarray);
    utils::geometry::overlap(
        subarray, tile_subarray, dim_num, tile_overlap, &overlap);
    assert(overlap);
    cov = utils::geometry::coverage(tile_overlap, tile_subarray, dim_num);
    tile_pos = domain->get_tile_pos(metadata_domain, tile_coords);
    tids.emplace_back(tile_pos, cov);
    domain->get_next_tile_coords(subarray_tile_domain, tile_coords);
  } while (utils::geometry::coords_in_rect(
      tile_coords, subarray_tile_domain, dim_num));

  // Clean up
  tdb_delete_array(subarray_tile_domain);
  tdb_delete_array(tile_coords);
  tdb_delete_array(tile_subarray);
  tdb_delete_array(tile_overlap);

  return tids;
}

template <class T>
void FragmentMetadata::get_subarray_tile_domain(
    const T* subarray, T* subarray_tile_domain) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();

  // Calculate subarray in tile domain
  for (unsigned d = 0; d < dim_num; ++d) {
    auto domain = (const T*)domain_[d].data();
    auto tile_extent =
        *(const T*)array_schema_->domain()->tile_extent(d).data();
    auto overlap = std::max(subarray[2 * d], domain[0]);
    subarray_tile_domain[2 * d] = (overlap - domain[0]) / tile_extent;

    overlap = std::min(subarray[2 * d + 1], domain[1]);
    subarray_tile_domain[2 * d + 1] = (overlap - domain[0]) / tile_extent;
  }
}

Status FragmentMetadata::expand_non_empty_domain(const NDRange& mbr) {
  std::lock_guard<std::mutex> lock(mtx_);

  // Case the non-empty domain is not initialized yet
  if (non_empty_domain_.empty()) {
    non_empty_domain_ = mbr;
    return Status::Ok();
  }

  // Expand existing non-empty domain
  array_schema_->domain()->expand_ndrange(mbr, &non_empty_domain_);

  return Status::Ok();
}

Status FragmentMetadata::load_tile_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (version_ <= 2)
    return Status::Ok();

  // If the tile offset is already loaded, exit early to avoid the lock
  if (loaded_metadata_.tile_offsets_[idx])
    return Status::Ok();

  std::lock_guard<std::mutex> lock(tile_offsets_mtx_[idx]);

  if (loaded_metadata_.tile_offsets_[idx])
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_offsets_[idx], &buff));

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::READ_TILE_OFFSETS_SIZE, buff.size());

  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_tile_offsets(idx, &cbuff));

  loaded_metadata_.tile_offsets_[idx] = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_var_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (version_ <= 2)
    return Status::Ok();

  // If the tile var offset is already loaded, exit early to avoid the lock
  if (loaded_metadata_.tile_var_offsets_[idx])
    return Status::Ok();

  std::lock_guard<std::mutex> lock(tile_var_offsets_mtx_[idx]);

  if (loaded_metadata_.tile_var_offsets_[idx])
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_var_offsets_[idx], &buff));

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::READ_TILE_VAR_OFFSETS_SIZE, buff.size());

  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_tile_var_offsets(idx, &cbuff));

  loaded_metadata_.tile_var_offsets_[idx] = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_var_sizes(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (version_ <= 2)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_var_sizes_[idx])
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_var_sizes_[idx], &buff));

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::READ_TILE_VAR_SIZES_SIZE, buff.size());

  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_tile_var_sizes(idx, &cbuff));

  loaded_metadata_.tile_var_sizes_[idx] = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_validity_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (version_ <= 6)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_validity_offsets_[idx])
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_validity_offsets_[idx], &buff));

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::READ_TILE_VALIDITY_OFFSETS_SIZE, buff.size());

  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_tile_validity_offsets(idx, &cbuff));

  loaded_metadata_.tile_validity_offsets_[idx] = true;

  return Status::Ok();
}

// ===== FORMAT =====
//  bounding_coords_num (uint64_t)
//  bounding_coords_#1 (void*) bounding_coords_#2 (void*) ...
Status FragmentMetadata::load_bounding_coords(ConstBuffer* buff) {
  // Get number of bounding coordinates
  uint64_t bounding_coords_num = 0;
  RETURN_NOT_OK(buff->read(&bounding_coords_num, sizeof(uint64_t)));

  // Get bounding coordinates
  // Note: This version supports only dimensions domains with the same type
  auto coord_size = array_schema_->domain()->dimension(0)->coord_size();
  auto dim_num = array_schema_->domain()->dim_num();
  uint64_t bounding_coords_size = 2 * dim_num * coord_size;
  bounding_coords_.resize(bounding_coords_num);
  for (uint64_t i = 0; i < bounding_coords_num; ++i) {
    bounding_coords_[i].resize(bounding_coords_size);
    RETURN_NOT_OK(buff->read(&bounding_coords_[i][0], bounding_coords_size));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_file_sizes(ConstBuffer* buff) {
  if (version_ < 5)
    return load_file_sizes_v1_v4(buff);
  else
    return load_file_sizes_v5_or_higher(buff);
}

// ===== FORMAT =====
// file_sizes#0 (uint64_t)
// ...
// file_sizes#attribute_num (uint64_t)
Status FragmentMetadata::load_file_sizes_v1_v4(ConstBuffer* buff) {
  auto attribute_num = array_schema_->attribute_num();
  file_sizes_.resize(attribute_num + 1);
  Status st =
      buff->read(&file_sizes_[0], (attribute_num + 1) * sizeof(uint64_t));

  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading tile offsets failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// file_sizes#0 (uint64_t)
// ...
// file_sizes#{attribute_num+dim_num} (uint64_t)
Status FragmentMetadata::load_file_sizes_v5_or_higher(ConstBuffer* buff) {
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  file_sizes_.resize(num);
  Status st = buff->read(&file_sizes_[0], num * sizeof(uint64_t));

  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading tile offsets failed"));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_file_var_sizes(ConstBuffer* buff) {
  if (version_ < 5)
    return load_file_var_sizes_v1_v4(buff);
  else
    return load_file_var_sizes_v5_or_higher(buff);
}

// ===== FORMAT =====
// file_var_sizes#0 (uint64_t)
// ...
// file_var_sizes#attribute_num (uint64_t)
Status FragmentMetadata::load_file_var_sizes_v1_v4(ConstBuffer* buff) {
  auto attribute_num = array_schema_->attribute_num();
  file_var_sizes_.resize(attribute_num);
  Status st = buff->read(&file_var_sizes_[0], attribute_num * sizeof(uint64_t));

  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading tile offsets failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// file_var_sizes#0 (uint64_t)
// ...
// file_var_sizes#{attribute_num+dim_num} (uint64_t)
Status FragmentMetadata::load_file_var_sizes_v5_or_higher(ConstBuffer* buff) {
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  file_var_sizes_.resize(num);
  Status st = buff->read(&file_var_sizes_[0], num * sizeof(uint64_t));

  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading tile offsets failed"));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_file_validity_sizes(ConstBuffer* buff) {
  if (version_ <= 6)
    return Status::Ok();

  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  file_validity_sizes_.resize(num);
  Status st = buff->read(&file_validity_sizes_[0], num * sizeof(uint64_t));

  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading tile offsets failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// last_tile_cell_num (uint64_t)
Status FragmentMetadata::load_last_tile_cell_num(ConstBuffer* buff) {
  // Get last tile cell number
  Status st = buff->read(&last_tile_cell_num_, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading last tile cell number "
        "failed"));
  }
  return Status::Ok();
}

// ===== FORMAT =====
// mbr_num (uint64_t)
// mbr_#1 (void*)
// mbr_#2 (void*)
// ...
Status FragmentMetadata::load_mbrs(ConstBuffer* buff) {
  // Get number of MBRs
  uint64_t mbr_num = 0;
  RETURN_NOT_OK(buff->read(&mbr_num, sizeof(uint64_t)));

  // Set leaf level
  rtree_.set_leaf_num(mbr_num);
  auto domain = array_schema_->domain();
  auto dim_num = domain->dim_num();
  for (uint64_t m = 0; m < mbr_num; ++m) {
    NDRange mbr(dim_num);
    for (unsigned d = 0; d < dim_num; ++d) {
      auto r_size = 2 * domain->dimension(d)->coord_size();
      mbr[d].set_range(buff->cur_data(), r_size);
      buff->advance_offset(r_size);
    }
    rtree_.set_leaf(m, mbr);
  }

  // Build R-tree bottom-up
  if (mbr_num > 0) {
    rtree_.build_tree();
  }

  sparse_tile_num_ = mbr_num;

  return Status::Ok();
}

Status FragmentMetadata::load_non_empty_domain(ConstBuffer* buff) {
  if (version_ <= 2)
    return load_non_empty_domain_v1_v2(buff);
  else if (version_ == 3 || version_ == 4)
    return load_non_empty_domain_v3_v4(buff);
  return load_non_empty_domain_v5_or_higher(buff);
}

// ===== FORMAT =====
// non_empty_domain_size (uint64_t)
// non_empty_domain (void*)
Status FragmentMetadata::load_non_empty_domain_v1_v2(ConstBuffer* buff) {
  // Get domain size
  uint64_t domain_size = 0;
  RETURN_NOT_OK(buff->read(&domain_size, sizeof(uint64_t)));

  // Get non-empty domain
  if (domain_size != 0) {
    auto dim_num = array_schema_->dim_num();
    std::vector<uint8_t> temp(domain_size);
    RETURN_NOT_OK(buff->read(&temp[0], domain_size));
    non_empty_domain_.resize(dim_num);
    uint64_t offset = 0;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto coord_size = array_schema_->dimension(d)->coord_size();
      Range r(&temp[offset], 2 * coord_size);
      non_empty_domain_[d] = std::move(r);
      offset += 2 * coord_size;
    }
  }

  // Get expanded domain
  if (!non_empty_domain_.empty()) {
    domain_ = non_empty_domain_;
    array_schema_->domain()->expand_to_tiles(&domain_);
  }

  return Status::Ok();
}

// ===== FORMAT =====
// null non_empty_domain (char)
// non_empty_domain (domain_size)
Status FragmentMetadata::load_non_empty_domain_v3_v4(ConstBuffer* buff) {
  // Get null non-empty domain
  bool null_non_empty_domain = false;
  RETURN_NOT_OK(buff->read(&null_non_empty_domain, sizeof(char)));

  // Get non-empty domain
  if (!null_non_empty_domain) {
    auto dim_num = array_schema_->dim_num();
    // Note: These versions supports only dimensions domains with the same type
    auto coord_size = array_schema_->domain()->dimension(0)->coord_size();
    auto domain_size = 2 * dim_num * coord_size;
    std::vector<uint8_t> temp(domain_size);
    RETURN_NOT_OK(buff->read(&temp[0], domain_size));
    non_empty_domain_.resize(dim_num);
    uint64_t offset = 0;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto coord_size = array_schema_->dimension(d)->coord_size();
      Range r(&temp[offset], 2 * coord_size);
      non_empty_domain_[d] = std::move(r);
      offset += 2 * coord_size;
    }
  }

  // Get expanded domain
  if (!non_empty_domain_.empty()) {
    domain_ = non_empty_domain_;
    array_schema_->domain()->expand_to_tiles(&domain_);
  }

  return Status::Ok();
}

// ===== FORMAT =====
// null_non_empty_domain
// fix-sized: range(void*)
// var-sized: range_size(uint64_t) | start_range_size(uint64_t) | range(void*)
Status FragmentMetadata::load_non_empty_domain_v5_or_higher(ConstBuffer* buff) {
  // Get null non-empty domain
  char null_non_empty_domain = 0;
  RETURN_NOT_OK(buff->read(&null_non_empty_domain, sizeof(char)));

  auto domain = array_schema_->domain();
  if (null_non_empty_domain == 0) {
    auto dim_num = array_schema_->dim_num();
    non_empty_domain_.resize(dim_num);
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = domain->dimension(d);
      if (!dim->var_size()) {  // Fixed-sized
        auto r_size = 2 * dim->coord_size();
        non_empty_domain_[d].set_range(buff->cur_data(), r_size);
        buff->advance_offset(r_size);
      } else {  // Var-sized
        uint64_t r_size, start_size;
        RETURN_NOT_OK(buff->read(&r_size, sizeof(uint64_t)));
        RETURN_NOT_OK(buff->read(&start_size, sizeof(uint64_t)));
        non_empty_domain_[d].set_range(buff->cur_data(), r_size, start_size);
        buff->advance_offset(r_size);
      }
    }
  }

  // Get expanded domain
  if (!non_empty_domain_.empty()) {
    domain_ = non_empty_domain_;
    array_schema_->domain()->expand_to_tiles(&domain_);
  }

  return Status::Ok();
}

// Applicable only to versions 1 and 2
Status FragmentMetadata::load_tile_offsets(ConstBuffer* buff) {
  Status st;
  uint64_t tile_offsets_num = 0;
  unsigned int attribute_num = array_schema_->attribute_num();

  // Allocate tile offsets
  tile_offsets_.resize(attribute_num + 1);
  tile_offsets_mtx_.resize(attribute_num + 1);

  // For all attributes, get the tile offsets
  for (unsigned int i = 0; i < attribute_num + 1; ++i) {
    // Get number of tile offsets
    st = buff->read(&tile_offsets_num, sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading number of tile offsets "
          "failed"));
    }

    if (tile_offsets_num == 0)
      continue;

    // Get tile offsets
    tile_offsets_[i].resize(tile_offsets_num);
    st = buff->read(&tile_offsets_[i][0], tile_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading tile offsets failed"));
    }
  }

  loaded_metadata_.tile_offsets_.resize(
      array_schema_->attribute_num() + 1, true);

  return Status::Ok();
}

Status FragmentMetadata::load_tile_offsets(unsigned idx, ConstBuffer* buff) {
  Status st;
  uint64_t tile_offsets_num = 0;

  // Get number of tile offsets
  st = buff->read(&tile_offsets_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading number of tile offsets "
        "failed"));
  }

  // Get tile offsets
  if (tile_offsets_num != 0) {
    tile_offsets_[idx].resize(tile_offsets_num);
    st =
        buff->read(&tile_offsets_[idx][0], tile_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading tile offsets failed"));
    }
  }

  return Status::Ok();
}

// ===== FORMAT =====
// tile_var_offsets_attr#0_num (uint64_t)
// tile_var_offsets_attr#0_#1 (uint64_t) tile_var_offsets_attr#0_#2 (uint64_t)
// ...
// ...
// tile_var_offsets_attr#<attribute_num-1>_num(uint64_t)
// tile_var_offsets_attr#<attribute_num-1>_#1 (uint64_t)
//     tile_ver_offsets_attr#<attribute_num-1>_#2 (uint64_t) ...
Status FragmentMetadata::load_tile_var_offsets(ConstBuffer* buff) {
  Status st;
  unsigned int attribute_num = array_schema_->attribute_num();
  uint64_t tile_var_offsets_num = 0;

  // Allocate tile offsets
  tile_var_offsets_.resize(attribute_num);
  tile_var_offsets_mtx_.resize(attribute_num);

  // For all attributes, get the variable tile offsets
  for (unsigned int i = 0; i < attribute_num; ++i) {
    // Get number of tile offsets
    st = buff->read(&tile_var_offsets_num, sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading number of variable tile "
          "offsets failed"));
    }

    if (tile_var_offsets_num == 0)
      continue;

    // Get variable tile offsets
    tile_var_offsets_[i].resize(tile_var_offsets_num);
    st = buff->read(
        &tile_var_offsets_[i][0], tile_var_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading variable tile offsets "
          "failed"));
    }
  }

  loaded_metadata_.tile_var_offsets_.resize(
      array_schema_->attribute_num(), true);

  return Status::Ok();
}

Status FragmentMetadata::load_tile_var_offsets(
    unsigned idx, ConstBuffer* buff) {
  Status st;
  uint64_t tile_var_offsets_num = 0;

  // Get number of tile offsets
  st = buff->read(&tile_var_offsets_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading number of variable tile "
        "offsets failed"));
  }

  // Get variable tile offsets
  if (tile_var_offsets_num != 0) {
    tile_var_offsets_[idx].resize(tile_var_offsets_num);
    st = buff->read(
        &tile_var_offsets_[idx][0], tile_var_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading variable tile offsets "
          "failed"));
    }
  }

  return Status::Ok();
}

// ===== FORMAT =====
// tile_var_sizes_attr#0_num (uint64_t)
// tile_var_sizes_attr#0_#1 (uint64_t) tile_sizes_attr#0_#2 (uint64_t) ...
// ...
// tile_var_sizes_attr#<attribute_num-1>_num(uint64_t)
// tile_var_sizes__attr#<attribute_num-1>_#1 (uint64_t)
//     tile_var_sizes_attr#<attribute_num-1>_#2 (uint64_t) ...
Status FragmentMetadata::load_tile_var_sizes(ConstBuffer* buff) {
  Status st;
  unsigned int attribute_num = array_schema_->attribute_num();
  uint64_t tile_var_sizes_num = 0;

  // Allocate tile sizes
  tile_var_sizes_.resize(attribute_num);

  // For all attributes, get the variable tile sizes
  for (unsigned int i = 0; i < attribute_num; ++i) {
    // Get number of tile sizes
    st = buff->read(&tile_var_sizes_num, sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading number of variable tile "
          "sizes failed"));
    }

    if (tile_var_sizes_num == 0)
      continue;

    // Get variable tile sizes
    tile_var_sizes_[i].resize(tile_var_sizes_num);
    st = buff->read(
        &tile_var_sizes_[i][0], tile_var_sizes_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading variable tile sizes "
          "failed"));
    }
  }

  loaded_metadata_.tile_var_sizes_.resize(array_schema_->attribute_num(), true);

  return Status::Ok();
}

Status FragmentMetadata::load_tile_var_sizes(unsigned idx, ConstBuffer* buff) {
  Status st;
  uint64_t tile_var_sizes_num = 0;

  // Get number of tile sizes
  st = buff->read(&tile_var_sizes_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading number of variable tile "
        "sizes failed"));
  }

  // Get variable tile sizes
  if (tile_var_sizes_num != 0) {
    tile_var_sizes_[idx].resize(tile_var_sizes_num);
    st = buff->read(
        &tile_var_sizes_[idx][0], tile_var_sizes_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading variable tile sizes "
          "failed"));
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::load_tile_validity_offsets(
    unsigned idx, ConstBuffer* buff) {
  Status st;
  uint64_t tile_validity_offsets_num = 0;

  // Get number of tile offsets
  st = buff->read(&tile_validity_offsets_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(
        Status::FragmentMetadataError("Cannot load fragment metadata; Reading "
                                      "number of validity tile offsets "
                                      "failed"));
  }

  // Get tile offsets
  if (tile_validity_offsets_num != 0) {
    tile_validity_offsets_[idx].resize(tile_validity_offsets_num);
    st = buff->read(
        &tile_validity_offsets_[idx][0],
        tile_validity_offsets_num * sizeof(uint64_t));

    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading validity tile offsets "
          "failed"));
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::load_version(ConstBuffer* buff) {
  RETURN_NOT_OK(buff->read(&version_, sizeof(uint32_t)));
  return Status::Ok();
}

Status FragmentMetadata::load_dense(ConstBuffer* buff) {
  RETURN_NOT_OK(buff->read(&dense_, sizeof(char)));
  return Status::Ok();
}

Status FragmentMetadata::load_sparse_tile_num(ConstBuffer* buff) {
  RETURN_NOT_OK(buff->read(&sparse_tile_num_, sizeof(uint64_t)));
  return Status::Ok();
}

Status FragmentMetadata::load_generic_tile_offsets(ConstBuffer* buff) {
  if (version_ == 3 || version_ == 4)
    return load_generic_tile_offsets_v3_v4(buff);
  else if (version_ >= 5 && version_ < 7)
    return load_generic_tile_offsets_v5_v6(buff);
  else if (version_ >= 7)
    return load_generic_tile_offsets_v7_or_higher(buff);

  assert(false);
  return Status::Ok();
}

Status FragmentMetadata::load_generic_tile_offsets_v3_v4(ConstBuffer* buff) {
  // Load R-Tree offset
  RETURN_NOT_OK(buff->read(&gt_offsets_.rtree_, sizeof(uint64_t)));

  // Load offsets for tile offsets
  unsigned int attribute_num = array_schema_->attribute_num();
  gt_offsets_.tile_offsets_.resize(attribute_num + 1);
  for (unsigned i = 0; i < attribute_num + 1; ++i) {
    RETURN_NOT_OK(buff->read(&gt_offsets_.tile_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile var offsets
  gt_offsets_.tile_var_offsets_.resize(attribute_num);
  for (unsigned i = 0; i < attribute_num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_var_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile var sizes
  gt_offsets_.tile_var_sizes_.resize(attribute_num);
  for (unsigned i = 0; i < attribute_num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_var_sizes_[i], sizeof(uint64_t)));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_generic_tile_offsets_v5_v6(ConstBuffer* buff) {
  // Load R-Tree offset
  RETURN_NOT_OK(buff->read(&gt_offsets_.rtree_, sizeof(uint64_t)));

  // Load offsets for tile offsets
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  gt_offsets_.tile_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(buff->read(&gt_offsets_.tile_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_var_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_var_sizes_[i], sizeof(uint64_t)));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_generic_tile_offsets_v7_or_higher(
    ConstBuffer* buff) {
  // Load R-Tree offset
  RETURN_NOT_OK(buff->read(&gt_offsets_.rtree_, sizeof(uint64_t)));

  // Load offsets for tile offsets
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  gt_offsets_.tile_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(buff->read(&gt_offsets_.tile_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_var_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_var_sizes_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile validity offsets
  if (version_ >= 7) {
    gt_offsets_.tile_validity_offsets_.resize(num);
    for (unsigned i = 0; i < num; ++i) {
      RETURN_NOT_OK(
          buff->read(&gt_offsets_.tile_validity_offsets_[i], sizeof(uint64_t)));
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::load_v1_v2(const EncryptionKey& encryption_key) {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));
  // Read metadata
  GenericTileIO tile_io(storage_manager_, fragment_metadata_uri);
  auto tile = (Tile*)nullptr;
  RETURN_NOT_OK(tile_io.read_generic(
      &tile, 0, encryption_key, storage_manager_->config()));

  auto chunked_buffer = tile->chunked_buffer();
  Buffer buff;
  RETURN_NOT_OK_ELSE(buff.realloc(chunked_buffer->size()), tdb_delete(tile));
  buff.set_size(chunked_buffer->size());
  RETURN_NOT_OK_ELSE(
      chunked_buffer->read(buff.data(), buff.size(), 0), tdb_delete(tile));
  tdb_delete(tile);

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::READ_FRAG_META_SIZE, buff.size());

  // Deserialize
  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_version(&cbuff));
  RETURN_NOT_OK(load_non_empty_domain(&cbuff));
  RETURN_NOT_OK(load_mbrs(&cbuff));
  RETURN_NOT_OK(load_bounding_coords(&cbuff));
  RETURN_NOT_OK(load_tile_offsets(&cbuff));
  RETURN_NOT_OK(load_tile_var_offsets(&cbuff));
  RETURN_NOT_OK(load_tile_var_sizes(&cbuff));
  RETURN_NOT_OK(load_last_tile_cell_num(&cbuff));
  RETURN_NOT_OK(load_file_sizes(&cbuff));
  RETURN_NOT_OK(load_file_var_sizes(&cbuff));
  RETURN_NOT_OK(load_file_validity_sizes(&cbuff));

  return Status::Ok();
}

Status FragmentMetadata::load_v3_or_higher(
    const EncryptionKey& encryption_key, Buffer* f_buff, uint64_t offset) {
  RETURN_NOT_OK(load_footer(encryption_key, f_buff, offset));
  return Status::Ok();
}

Status FragmentMetadata::load_footer(
    const EncryptionKey& encryption_key, Buffer* f_buff, uint64_t offset) {
  (void)encryption_key;  // Not used for now, perhaps in the future
  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.footer_)
    return Status::Ok();

  Buffer buff;
  tdb_shared_ptr<ConstBuffer> cbuff = nullptr;
  if (f_buff == nullptr) {
    has_consolidated_footer_ = false;
    RETURN_NOT_OK(read_file_footer(&buff, &footer_offset_, &footer_size_));
    cbuff = tdb_make_shared(ConstBuffer, &buff);
  } else {
    footer_size_ = 0;
    footer_offset_ = offset;
    has_consolidated_footer_ = true;
    cbuff = tdb_make_shared(ConstBuffer, f_buff);
    cbuff->set_offset(offset);
  }

  RETURN_NOT_OK(load_version(cbuff.get()));
  RETURN_NOT_OK(load_dense(cbuff.get()));
  RETURN_NOT_OK(load_non_empty_domain(cbuff.get()));
  RETURN_NOT_OK(load_sparse_tile_num(cbuff.get()));
  RETURN_NOT_OK(load_last_tile_cell_num(cbuff.get()));
  RETURN_NOT_OK(load_file_sizes(cbuff.get()));
  RETURN_NOT_OK(load_file_var_sizes(cbuff.get()));
  RETURN_NOT_OK(load_file_validity_sizes(cbuff.get()));

  unsigned num = array_schema_->attribute_num() + 1;
  num += (version_ >= 5) ? array_schema_->dim_num() : 0;

  tile_offsets_.resize(num);
  tile_offsets_mtx_.resize(num);
  tile_var_offsets_.resize(num);
  tile_var_offsets_mtx_.resize(num);
  tile_var_sizes_.resize(num);
  tile_validity_offsets_.resize(num);

  loaded_metadata_.tile_offsets_.resize(num, false);
  loaded_metadata_.tile_var_offsets_.resize(num, false);
  loaded_metadata_.tile_var_sizes_.resize(num, false);
  loaded_metadata_.tile_validity_offsets_.resize(num, false);

  RETURN_NOT_OK(load_generic_tile_offsets(cbuff.get()));

  loaded_metadata_.footer_ = true;

  // If the footer_size is not set lets calculate from how much of the buffer we
  // read
  if (footer_size_ == 0)
    footer_size_ = cbuff->offset() - offset;

  return Status::Ok();
}

// ===== FORMAT =====
// file_sizes#0 (uint64_t)
// ...
// file_sizes#{attribute_num+dim_num} (uint64_t)
Status FragmentMetadata::write_file_sizes(Buffer* buff) const {
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  Status st = buff->write(&file_sizes_[0], num * sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing file sizes failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// file_var_sizes#0 (uint64_t)
// ...
// file_var_sizes#{attribute_num+dim_num} (uint64_t)
Status FragmentMetadata::write_file_var_sizes(Buffer* buff) const {
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  Status st = buff->write(&file_var_sizes_[0], num * sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing file sizes failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// file_validity_sizes#0 (uint64_t)
// ...
// file_validity_sizes#{attribute_num+dim_num} (uint64_t)
Status FragmentMetadata::write_file_validity_sizes(Buffer* buff) const {
  if (version_ <= 6)
    return Status::Ok();

  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  Status st = buff->write(&file_validity_sizes_[0], num * sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing file sizes failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// rtree_offset(uint64_t)
// tile_offsets_offset_0(uint64_t)
// ...
// tile_offsets_offset_{attr_num+dim_num}(uint64_t)
// tile_var_offsets_0(uint64_t)
// ...
// tile_var_offsets_{attr_num+dim_num}(uint64_t)
// tile_var_sizes_0(uint64_t)
// ...
// tile_var_sizes_{attr_num+dim_num}(uint64_t)
Status FragmentMetadata::write_generic_tile_offsets(Buffer* buff) const {
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;

  // Write R-Tree offset
  auto st = buff->write(&gt_offsets_.rtree_, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing R-Tree offset failed"));
  }

  // Write tile offsets
  for (unsigned i = 0; i < num; ++i) {
    st = buff->write(&gt_offsets_.tile_offsets_[i], sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing tile offsets failed"));
    }
  }

  // Write tile var offsets
  for (unsigned i = 0; i < num; ++i) {
    st = buff->write(&gt_offsets_.tile_var_offsets_[i], sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(
          Status::FragmentMetadataError("Cannot serialize fragment metadata; "
                                        "Writing tile var offsets failed"));
    }
  }

  // Write tile var sizes
  for (unsigned i = 0; i < num; ++i) {
    st = buff->write(&gt_offsets_.tile_var_sizes_[i], sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing tile var sizes failed"));
    }
  }

  // Write tile validity offsets
  if (version_ >= 7) {
    for (unsigned i = 0; i < num; ++i) {
      st =
          buff->write(&gt_offsets_.tile_validity_offsets_[i], sizeof(uint64_t));
      if (!st.ok()) {
        return LOG_STATUS(Status::FragmentMetadataError(
            "Cannot serialize fragment metadata; Writing tile offsets failed"));
      }
    }
  }

  return Status::Ok();
}

// ===== FORMAT =====
// last_tile_cell_num(uint64_t)
Status FragmentMetadata::write_last_tile_cell_num(Buffer* buff) const {
  uint64_t cell_num_per_tile =
      dense_ ? array_schema_->domain()->cell_num_per_tile() :
               array_schema_->capacity();

  // Handle the case of zero
  uint64_t last_tile_cell_num =
      (last_tile_cell_num_ == 0) ? cell_num_per_tile : last_tile_cell_num_;

  Status st = buff->write(&last_tile_cell_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(
        Status::FragmentMetadataError("Cannot serialize fragment metadata; "
                                      "Writing last tile cell number failed"));
  }
  return Status::Ok();
}

Status FragmentMetadata::store_rtree(
    const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;
  RETURN_NOT_OK(write_rtree(&buff));

  RETURN_NOT_OK(
      write_generic_tile_to_file(encryption_key, std::move(buff), nbytes));
  STATS_ADD_COUNTER(stats::Stats::CounterType::WRITE_RTREE_SIZE, *nbytes);

  return Status::Ok();
}

Status FragmentMetadata::write_rtree(Buffer* buff) {
  RETURN_NOT_OK(rtree_.build_tree());
  RETURN_NOT_OK(rtree_.serialize(buff));
  return Status::Ok();
}

// ===== FORMAT =====
// null_non_empty_domain(char)
// fix-sized: range(void*)
// var-sized: range_size(uint64_t) | start_range_size(uint64_t) | range(void*)
// ...
Status FragmentMetadata::write_non_empty_domain(Buffer* buff) const {
  // Write null_non_empty_domain
  auto null_non_empty_domain = (char)non_empty_domain_.empty();
  RETURN_NOT_OK(buff->write(&null_non_empty_domain, sizeof(char)));

  // Write domain size
  uint64_t domain_size = 0;
  auto domain = array_schema_->domain();
  auto dim_num = domain->dim_num();
  if (non_empty_domain_.empty()) {
    // Applicable only to homogeneous domains with fixed-sized types
    assert(domain->all_dims_fixed());
    assert(domain->all_dims_same_type());
    domain_size = 2 * dim_num * domain->dimension(0)->coord_size();

    // Write domain (dummy values)
    std::vector<uint8_t> d(domain_size, 0);
    RETURN_NOT_OK(buff->write(&d[0], domain_size));
  } else {
    // Write non-empty domain
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = domain->dimension(d);
      const auto& r = non_empty_domain_[d];
      if (!dim->var_size()) {  // Fixed-sized
        RETURN_NOT_OK(buff->write(r.data(), r.size()));
      } else {  // Var-sized
        auto r_size = r.size();
        auto r_start_size = r.start_size();
        RETURN_NOT_OK(buff->write(&r_size, sizeof(uint64_t)));
        RETURN_NOT_OK(buff->write(&r_start_size, sizeof(uint64_t)));
        RETURN_NOT_OK(buff->write(r.data(), r_size));
      }
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::read_generic_tile_from_file(
    const EncryptionKey& encryption_key, uint64_t offset, Buffer* buff) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  // Read metadata
  GenericTileIO tile_io(storage_manager_, fragment_metadata_uri);
  Tile* tile = nullptr;
  RETURN_NOT_OK(tile_io.read_generic(
      &tile, offset, encryption_key, storage_manager_->config()));

  const auto chunked_buffer = tile->chunked_buffer();
  buff->realloc(chunked_buffer->size());
  buff->set_size(chunked_buffer->size());
  RETURN_NOT_OK_ELSE(
      chunked_buffer->read(buff->data(), buff->size(), 0), tdb_delete(tile));
  tdb_delete(tile);

  return Status::Ok();
}

Status FragmentMetadata::read_file_footer(
    Buffer* buff, uint64_t* footer_offset, uint64_t* footer_size) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  // Get footer offset
  RETURN_NOT_OK(get_footer_offset_and_size(footer_offset, footer_size));

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::READ_FRAG_META_SIZE, *footer_size);

  // Read footer
  return storage_manager_->read(
      fragment_metadata_uri, *footer_offset, buff, *footer_size);
}

Status FragmentMetadata::write_generic_tile_to_file(
    const EncryptionKey& encryption_key,
    Buffer&& buff,
    uint64_t* nbytes) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  ChunkedBuffer* const chunked_buffer = tdb_new(ChunkedBuffer);
  RETURN_NOT_OK_ELSE(
      Tile::buffer_to_contiguous_fixed_chunks(
          buff, 0, constants::generic_tile_cell_size, chunked_buffer),
      tdb_delete(chunked_buffer));
  buff.disown_data();

  Tile tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      chunked_buffer,
      true);

  GenericTileIO tile_io(storage_manager_, fragment_metadata_uri);
  RETURN_NOT_OK(tile_io.write_generic(&tile, encryption_key, nbytes));

  return Status::Ok();
}

Status FragmentMetadata::write_footer_to_file(Buffer* buff) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  auto size = buff->size();
  RETURN_NOT_OK(storage_manager_->write(
      fragment_metadata_uri, buff->data(), buff->size()));

  // Write the size in the end if there is at least one var-sized dimension
  if (!array_schema_->domain()->all_dims_fixed())
    return storage_manager_->write(fragment_metadata_uri, &size, sizeof(size));
  return Status::Ok();
}

Status FragmentMetadata::store_tile_offsets(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;
  RETURN_NOT_OK(write_tile_offsets(idx, &buff));
  RETURN_NOT_OK(
      write_generic_tile_to_file(encryption_key, std::move(buff), nbytes));

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::WRITE_TILE_OFFSETS_SIZE, *nbytes);

  return Status::Ok();
}

Status FragmentMetadata::write_tile_offsets(unsigned idx, Buffer* buff) {
  Status st;

  // Write number of tile offsets
  uint64_t tile_offsets_num = tile_offsets_[idx].size();
  st = buff->write(&tile_offsets_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing number of tile offsets "
        "failed"));
  }

  // Write tile offsets
  if (tile_offsets_num != 0) {
    st = buff->write(
        &tile_offsets_[idx][0], tile_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing tile offsets failed"));
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::store_tile_var_offsets(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;
  RETURN_NOT_OK(write_tile_var_offsets(idx, &buff));
  RETURN_NOT_OK(
      write_generic_tile_to_file(encryption_key, std::move(buff), nbytes));

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::WRITE_TILE_VAR_OFFSETS_SIZE, *nbytes);

  return Status::Ok();
}

Status FragmentMetadata::write_tile_var_offsets(unsigned idx, Buffer* buff) {
  Status st;

  // Write tile offsets for each attribute
  // Write number of offsets
  uint64_t tile_var_offsets_num = tile_var_offsets_[idx].size();
  st = buff->write(&tile_var_offsets_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing number of "
        "variable tile offsets failed"));
  }

  // Write tile offsets
  if (tile_var_offsets_num != 0) {
    st = buff->write(
        &tile_var_offsets_[idx][0], tile_var_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing "
          "variable tile offsets failed"));
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::store_tile_var_sizes(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;
  RETURN_NOT_OK(write_tile_var_sizes(idx, &buff));
  RETURN_NOT_OK(
      write_generic_tile_to_file(encryption_key, std::move(buff), nbytes));

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::WRITE_TILE_VAR_SIZES_SIZE, *nbytes);

  return Status::Ok();
}

Status FragmentMetadata::write_tile_var_sizes(unsigned idx, Buffer* buff) {
  Status st;

  // Write number of sizes
  uint64_t tile_var_sizes_num = tile_var_sizes_[idx].size();
  st = buff->write(&tile_var_sizes_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing number of "
        "variable tile sizes failed"));
  }

  // Write tile sizes
  if (tile_var_sizes_num != 0) {
    st = buff->write(
        &tile_var_sizes_[idx][0], tile_var_sizes_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(
          Status::FragmentMetadataError("Cannot serialize fragment metadata; "
                                        "Writing variable tile sizes failed"));
    }
  }
  return Status::Ok();
}

Status FragmentMetadata::store_tile_validity_offsets(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;
  RETURN_NOT_OK(write_tile_validity_offsets(idx, &buff));
  RETURN_NOT_OK(
      write_generic_tile_to_file(encryption_key, std::move(buff), nbytes));

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::WRITE_TILE_VALIDITY_OFFSETS_SIZE, *nbytes);

  return Status::Ok();
}

Status FragmentMetadata::write_tile_validity_offsets(
    unsigned idx, Buffer* buff) {
  Status st;

  // Write number of tile offsets
  uint64_t tile_validity_offsets_num = tile_validity_offsets_[idx].size();
  st = buff->write(&tile_validity_offsets_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(
        Status::FragmentMetadataError("Cannot serialize fragment metadata; "
                                      "Writing number of validity tile offsets "
                                      "failed"));
  }

  // Write tile offsets
  if (tile_validity_offsets_num != 0) {
    st = buff->write(
        &tile_validity_offsets_[idx][0],
        tile_validity_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing tile offsets failed"));
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::write_version(Buffer* buff) const {
  RETURN_NOT_OK(buff->write(&version_, sizeof(uint32_t)));
  return Status::Ok();
}

Status FragmentMetadata::write_dense(Buffer* buff) const {
  RETURN_NOT_OK(buff->write(&dense_, sizeof(char)));
  return Status::Ok();
}

Status FragmentMetadata::write_sparse_tile_num(Buffer* buff) const {
  RETURN_NOT_OK(buff->write(&sparse_tile_num_, sizeof(uint64_t)));
  return Status::Ok();
}

Status FragmentMetadata::store_footer(const EncryptionKey& encryption_key) {
  (void)encryption_key;  // Not used for now, maybe in the future

  Buffer buff;
  RETURN_NOT_OK(write_footer(&buff));
  RETURN_NOT_OK(write_footer_to_file(&buff));

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::WRITE_FRAG_META_FOOTER_SIZE, buff.size());

  return Status::Ok();
}

void FragmentMetadata::clean_up() {
  auto array_uri = this->array_uri();
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);

  storage_manager_->close_file(fragment_metadata_uri);
  storage_manager_->vfs()->remove_file(fragment_metadata_uri);
  storage_manager_->array_xunlock(array_uri);
}

// Explicit template instantiations
template std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov<int8_t>(
    const int8_t* subarray) const;
template std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov<uint8_t>(
    const uint8_t* subarray) const;
template std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov<int16_t>(
    const int16_t* subarray) const;
template std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov<uint16_t>(
    const uint16_t* subarray) const;
template std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov<int32_t>(
    const int32_t* subarray) const;
template std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov<uint32_t>(
    const uint32_t* subarray) const;
template std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov<int64_t>(
    const int64_t* subarray) const;
template std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov<uint64_t>(
    const uint64_t* subarray) const;
template std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov<float>(
    const float* subarray) const;
template std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov<double>(
    const double* subarray) const;

}  // namespace sm
}  // namespace tiledb
