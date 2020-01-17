/**
 * @file  fragment_metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/tile/tile_io.h"

#include <cassert>
#include <iostream>

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
  meta_file_size_ = 0;
  version_ = constants::format_version;
  tile_index_base_ = 0;
  sparse_tile_num_ = 0;
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

/* ****************************** */
/*                API             */
/* ****************************** */

const URI& FragmentMetadata::array_uri() const {
  return array_schema_->array_uri();
}

Status FragmentMetadata::set_mbr(uint64_t tile, const NDRange& mbr) {
  tile += tile_index_base_;
  mbrs_[tile] = mbr;

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
  tile_offsets_[idx][tid] = next_tile_offsets_[idx];
  next_tile_offsets_[idx] += step;
}

void FragmentMetadata::set_tile_var_offset(
    const std::string& name, uint64_t tid, uint64_t step) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  assert(tid < tile_var_offsets_[idx].size());
  tile_var_offsets_[idx][tid] = next_tile_var_offsets_[idx];
  next_tile_var_offsets_[idx] += step;
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
    const NDRange& subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  if (dense_)
    return add_max_buffer_sizes_dense(encryption_key, subarray, buffer_sizes);
  return add_max_buffer_sizes_sparse(encryption_key, subarray, buffer_sizes);
}

Status FragmentMetadata::add_max_buffer_sizes_dense(
    const EncryptionKey& encryption_key,
    const NDRange& subarray,
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

  // Add fragment metadata file size
  assert(meta_file_size_ != 0);  // The file size should be loaded
  *size += meta_file_size_;

  return Status::Ok();
}

const URI& FragmentMetadata::fragment_uri() const {
  return fragment_uri_;
}

Status FragmentMetadata::get_tile_overlap(
    const EncryptionKey& encryption_key,
    const NDRange& range,
    TileOverlap* tile_overlap) {
  // Return if the range does not overlap the non-empty domain of the fragment
  auto domain = array_schema_->domain();
  if (!domain->overlap(range, non_empty_domain_))
    return Status::Ok();

  // Handle version > 2
  if (version_ > 2) {
    RETURN_NOT_OK(load_rtree(encryption_key));
    *tile_overlap = rtree_.get_tile_overlap(range);
    return Status::Ok();
  }

  // Handle version <= 2
  auto mbr_num = (uint64_t)mbrs_.size();
  for (uint64_t t = 0; t < mbr_num; ++t) {
    auto overlap = domain->coverage(range, mbrs_[t]);
    if (overlap > 0.0) {
      auto to = std::pair<uint64_t, double>(t, overlap);
      tile_overlap->tiles_.push_back(to);
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::init(const NDRange& non_empty_domain) {
  // For easy reference
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  auto domain = array_schema_->domain();

  // Sanity check
  assert(!non_empty_domain.empty());

  // Set non-empty domain for dense arrays (for sparse it will be calculated
  // via the MBRs)
  if (dense_) {
    // Set non-empty domain
    non_empty_domain_ = non_empty_domain;

    // The following is needed in case the fragment is a result of
    // dense consolidation, as the consolidator may have expanded
    // the fragment domain beyond the array domain to include
    // integral space tiles
    domain->crop_domain(&non_empty_domain_);

    // Set expanded domain
    domain_ = non_empty_domain_;
    domain->expand_domain(&domain_);
  }

  // Set last tile cell number
  last_tile_cell_num_ = 0;

  // Initialize tile offsets
  tile_offsets_.resize(num);
  next_tile_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i)
    next_tile_offsets_[i] = 0;

  // Initialize variable tile offsets
  tile_var_offsets_.resize(num);
  next_tile_var_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i)
    next_tile_var_offsets_[i] = 0;

  // Initialize variable tile sizes
  tile_var_sizes_.resize(num);

  return Status::Ok();
}

uint64_t FragmentMetadata::last_tile_cell_num() const {
  return last_tile_cell_num_;
}

Status FragmentMetadata::load(const EncryptionKey& encryption_key) {
  auto meta_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));
  RETURN_NOT_OK(storage_manager_->vfs()->file_size(meta_uri, &meta_file_size_));

  // Get fragment name version
  uint32_t f_version;
  RETURN_NOT_OK(
      utils::parse::get_fragment_name_version(fragment_uri_, &f_version));

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
  return load_v3_or_higher(encryption_key);
}

const std::vector<NDRange>& FragmentMetadata::mbrs() const {
  return mbrs_;
}

Status FragmentMetadata::store(const EncryptionKey& encryption_key) {
  auto array_uri = this->array_uri();
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  uint64_t offset = 0, nbytes;

  // Do nothing if fragment directory does not exist. The fragment directory
  // is created only when some attribute file is written
  bool is_dir = false;
  RETURN_NOT_OK(storage_manager_->is_dir(fragment_uri_, &is_dir));
  if (!is_dir)
    return Status::Ok();

  // Exclusively lock the array
  RETURN_NOT_OK(storage_manager_->array_xlock(array_uri));

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

  // Store footer
  RETURN_NOT_OK_ELSE(store_footer(encryption_key), clean_up());

  // Close file
  auto st = storage_manager_->close_file(fragment_metadata_uri);

  // Unlock array
  auto st2 = storage_manager_->array_xunlock(array_uri);

  return !st.ok() ? st : st2;
}

const NDRange& FragmentMetadata::non_empty_domain() const {
  return non_empty_domain_;
}

Status FragmentMetadata::set_num_tiles(uint64_t num_tiles) {
  auto num = array_schema_->attribute_num() + 1 + array_schema_->dim_num();

  for (unsigned i = 0; i < num; i++) {
    assert(num_tiles >= tile_offsets_[i].size());
    tile_offsets_[i].resize(num_tiles, 0);
    tile_var_offsets_[i].resize(num_tiles, 0);
    tile_var_sizes_[i].resize(num_tiles, 0);
  }

  if (!dense_) {
    mbrs_.resize(num_tiles);
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

URI FragmentMetadata::uri(const std::string& name) const {
  return fragment_uri_.join_path(name + constants::file_suffix);
}

URI FragmentMetadata::var_uri(const std::string& name) const {
  return fragment_uri_.join_path(name + "_var" + constants::file_suffix);
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

const NDRange& FragmentMetadata::mbr(uint64_t tile_idx) const {
  return rtree_.leaf(tile_idx);
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

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status FragmentMetadata::get_footer_offset_and_size(
    uint64_t* offset, uint64_t* size) const {
  uint32_t f_version;
  RETURN_NOT_OK(
      utils::parse::get_fragment_name_version(fragment_uri_, &f_version));

  if (f_version < 3)
    return get_footer_offset_and_size_v3_v4(offset, size);
  return get_footer_offset_and_size_v5_or_higher(offset, size);
}

Status FragmentMetadata::get_footer_offset_and_size_v3_v4(
    uint64_t* offset, uint64_t* size) const {
  auto attribute_num = array_schema_->attribute_num();
  auto domain_size = 2 * array_schema_->coords_size();

  // Get footer size
  *size = 0;
  *size += sizeof(uint32_t);                        // version
  *size += sizeof(char);                            // dense
  *size += sizeof(char);                            // null non-empty domain
  *size += domain_size;                             // non-empty domain
  *size += sizeof(uint64_t);                        // sparse tile num
  *size += sizeof(uint64_t);                        // last tile cell num
  *size += (attribute_num + 1) * sizeof(uint64_t);  // file sizes
  *size += attribute_num * sizeof(uint64_t);        // file var sizes
  *size += sizeof(uint64_t);                        // R-Tree offset
  *size += (attribute_num + 1) * sizeof(uint64_t);  // tile offsets
  *size += attribute_num * sizeof(uint64_t);        // tile var offsets
  *size += attribute_num * sizeof(uint64_t);        // tile var sizes

  // Get footer offset
  *offset = meta_file_size_ - *size;

  return Status::Ok();
}

Status FragmentMetadata::get_footer_offset_and_size_v5_or_higher(
    uint64_t* offset, uint64_t* size) const {
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  auto domain_size = 2 * array_schema_->coords_size();

  // Get footer size
  *size = 0;
  *size += sizeof(uint32_t);        // version
  *size += sizeof(char);            // dense
  *size += sizeof(char);            // null non-empty domain
  *size += domain_size;             // non-empty domain
  *size += sizeof(uint64_t);        // sparse tile num
  *size += sizeof(uint64_t);        // last tile cell num
  *size += num * sizeof(uint64_t);  // file sizes
  *size += num * sizeof(uint64_t);  // file var sizes
  *size += sizeof(uint64_t);        // R-Tree offset
  *size += num * sizeof(uint64_t);  // tile offsets
  *size += num * sizeof(uint64_t);  // tile var offsets
  *size += num * sizeof(uint64_t);  // tile var sizes

  // Get footer offset
  *offset = meta_file_size_ - *size;

  return Status::Ok();
}

std::vector<uint64_t> FragmentMetadata::compute_overlapping_tile_ids(
    const NDRange& subarray) const {
  assert(dense_);
  std::vector<uint64_t> tids;
  auto domain = array_schema_->domain();

  // Check if there is any overlap
  if (!domain->overlap(subarray, domain_))
    return tids;

  auto subarray_tile_domain = domain->tile_domain(subarray, domain_);
  auto tile_coords = domain->start_coords(subarray_tile_domain);

  // Walk through all tiles in subarray tile domain
  uint64_t tile_pos;
  do {
    tile_pos = domain->tile_pos(domain_, tile_coords);
    tids.emplace_back(tile_pos);
    domain->next_coords(subarray_tile_domain, &tile_coords);
  } while (domain->point_in_range(tile_coords, subarray_tile_domain));

  return tids;
}

std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov(
    const NDRange& subarray) const {
  assert(dense_);
  std::vector<std::pair<uint64_t, double>> tids;
  auto domain = array_schema_->domain();

  // Check if there is any overlap
  if (!domain->overlap(subarray, domain_))
    return tids;

  auto subarray_tile_domain = domain->tile_domain(subarray, domain_);
  auto tile_coords = domain->start_coords(subarray_tile_domain);

  // Walk through all tiles in subarray tile domain
  uint64_t tile_pos;
  double cov;
  do {
    cov = domain->tile_coverage(domain_, tile_coords, subarray);
    tile_pos = domain->tile_pos(domain_, tile_coords);
    tids.emplace_back(tile_pos, cov);
    domain->next_coords(subarray_tile_domain, &tile_coords);
  } while (domain->point_in_range(tile_coords, subarray_tile_domain));

  return tids;
}

Status FragmentMetadata::expand_non_empty_domain(const NDRange& mbr) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (non_empty_domain_.empty()) {
    non_empty_domain_ = mbr;
    return Status::Ok();
  }

  array_schema_->domain()->expand_mbr(&non_empty_domain_, mbr);
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

  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(rtree_.deserialize(&cbuff));

  loaded_metadata_.rtree_ = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (version_ <= 2)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_offsets_[idx])
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_offsets_[idx], &buff));
  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_tile_offsets(idx, &cbuff));

  loaded_metadata_.tile_offsets_[idx] = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_var_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (version_ <= 2)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_var_offsets_[idx])
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_var_offsets_[idx], &buff));

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

  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_tile_var_sizes(idx, &cbuff));

  loaded_metadata_.tile_var_sizes_[idx] = true;

  return Status::Ok();
}

// ===== FORMAT =====
//  bounding_coords_num (uint64_t)
//  bounding_coords_#1 (void*) bounding_coords_#2 (void*) ...
Status FragmentMetadata::load_bounding_coords(ConstBuffer* buff) {
  // Get number of bounding coordinates
  uint64_t num = 0;
  RETURN_NOT_OK(buff->read(&num, sizeof(uint64_t)));

  // Get bounding coordinates
  uint64_t size = 2 * array_schema_->coords_size() * num;
  std::vector<uint8_t> bounding_coords(size);
  RETURN_NOT_OK(buff->read(&bounding_coords[0], size));

  // Note: this is only for backwards compatibility. FragmentMetadata
  // does not make use of bounding coordinates any more

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

  // Get MBRs
  auto dim_num = array_schema_->dim_num();
  NDRange mbr(dim_num);
  mbrs_.resize(mbr_num);
  for (uint64_t i = 0; i < mbr_num; ++i) {
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = array_schema_->dimension(d);
      // Note: we will have to revisit the range size when we add support
      // for heterogeneous and string dimensions
      auto range_size = 2 * dim->coord_size();
      mbr[d].resize(range_size);
      RETURN_NOT_OK(buff->read(&mbr[d][0], range_size));
    }
    mbrs_[i] = mbr;
  }

  sparse_tile_num_ = mbrs_.size();

  return Status::Ok();
}

Status FragmentMetadata::load_non_empty_domain(ConstBuffer* buff) {
  if (version_ <= 2)
    return load_non_empty_domain_v2(buff);
  return load_non_empty_domain_v3(buff);
}

// ===== FORMAT =====
// non_empty_domain_size (uint64_t)
// non_empty_domain (void*)
Status FragmentMetadata::load_non_empty_domain_v2(ConstBuffer* buff) {
  // Get domain size
  uint64_t domain_size = 0;
  RETURN_NOT_OK(buff->read(&domain_size, sizeof(uint64_t)));
  (void)domain_size;  // This is not really needed

  // Get non-empty domain
  auto dim_num = array_schema_->dim_num();
  if (domain_size == 0) {
    non_empty_domain_.clear();
  } else {
    non_empty_domain_.resize(dim_num);
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = array_schema_->dimension(d);
      // Note: we will have to revisit the range size when we add support
      // for heterogeneous and string dimensions
      auto range_size = 2 * dim->coord_size();
      non_empty_domain_[d].resize(range_size);
      RETURN_NOT_OK_ELSE(
          buff->read(&non_empty_domain_[d][0], range_size),
          non_empty_domain_.clear());
    }
  }

  // Get expanded domain
  if (non_empty_domain_.empty()) {
    domain_.clear();
  } else {
    domain_ = non_empty_domain_;
    array_schema_->domain()->expand_domain(&domain_);
  }

  return Status::Ok();
}

// ===== FORMAT =====
// null non_empty_domain (char)
// non_empty_domain (domain_size)
Status FragmentMetadata::load_non_empty_domain_v3(ConstBuffer* buff) {
  // Get null non-empty domain
  bool null_non_empty_domain = false;
  RETURN_NOT_OK(buff->read(&null_non_empty_domain, sizeof(char)));

  // Get non-empty domain
  auto dim_num = array_schema_->dim_num();
  non_empty_domain_.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema_->dimension(d);
    // Note: we will have to revisit the range size when we add support
    // for heterogeneous and string dimensions
    auto range_size = 2 * dim->coord_size();
    non_empty_domain_[d].resize(range_size);
    RETURN_NOT_OK_ELSE(
        buff->read(&non_empty_domain_[d][0], range_size),
        non_empty_domain_.clear());
  }

  if (null_non_empty_domain) {
    non_empty_domain_.clear();
  }

  // Get expanded domain
  if (non_empty_domain_.empty()) {
    domain_.clear();
  } else {
    domain_ = non_empty_domain_;
    array_schema_->domain()->expand_domain(&domain_);
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

Status FragmentMetadata::create_rtree() {
  auto domain = array_schema_->domain();
  auto rtree = RTree(domain, constants::rtree_fanout, mbrs_);
  rtree_ = std::move(rtree);
  return Status::Ok();
}

Status FragmentMetadata::load_generic_tile_offsets(ConstBuffer* buff) {
  if (version_ == 3 || version_ == 4)
    return load_generic_tile_offsets_v3_v4(buff);
  else if (version_ > 4)
    return load_generic_tile_offsets_v5_or_higher(buff);

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

Status FragmentMetadata::load_generic_tile_offsets_v5_or_higher(
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

  return Status::Ok();
}

Status FragmentMetadata::load_v1_v2(const EncryptionKey& encryption_key) {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));
  // Read metadata
  TileIO tile_io(storage_manager_, fragment_metadata_uri);
  auto tile = (Tile*)nullptr;
  RETURN_NOT_OK(tile_io.read_generic(&tile, 0, encryption_key));
  tile->disown_buff();
  auto buff = tile->buffer();
  STATS_COUNTER_ADD(fragment_metadata_bytes_read, tile_io.file_size())
  delete tile;

  // Deserialize
  ConstBuffer cbuff(buff);
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
  RETURN_NOT_OK(create_rtree());

  delete buff;

  return Status::Ok();
}

Status FragmentMetadata::load_v3_or_higher(
    const EncryptionKey& encryption_key) {
  RETURN_NOT_OK(load_footer(encryption_key));
  return Status::Ok();
}

Status FragmentMetadata::load_footer(const EncryptionKey& encryption_key) {
  (void)encryption_key;  // Not used for now, perhaps in the future

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.footer_)
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(read_file_footer(&buff));

  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_version(&cbuff));
  RETURN_NOT_OK(load_dense(&cbuff));
  RETURN_NOT_OK(load_non_empty_domain(&cbuff));
  RETURN_NOT_OK(load_sparse_tile_num(&cbuff));
  RETURN_NOT_OK(load_last_tile_cell_num(&cbuff));
  RETURN_NOT_OK(load_file_sizes(&cbuff));
  RETURN_NOT_OK(load_file_var_sizes(&cbuff));

  unsigned num = array_schema_->attribute_num() + 1;
  num += (version_ >= 5) ? array_schema_->dim_num() : 0;

  tile_offsets_.resize(num);
  tile_var_offsets_.resize(num);
  tile_var_sizes_.resize(num);

  loaded_metadata_.tile_offsets_.resize(num, false);
  loaded_metadata_.tile_var_offsets_.resize(num, false);
  loaded_metadata_.tile_var_sizes_.resize(num, false);

  RETURN_NOT_OK(load_generic_tile_offsets(&cbuff));

  loaded_metadata_.footer_ = true;

  return Status::Ok();
}

// ===== FORMAT =====
// file_sizes#0 (uint64_t)
// ...
// file_sizes#{attribute_num+dim_num} (uint64_t)
Status FragmentMetadata::write_file_sizes(Buffer* buff) {
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  Status st = buff->write(&next_tile_offsets_[0], num * sizeof(uint64_t));
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
Status FragmentMetadata::write_file_var_sizes(Buffer* buff) {
  auto num = array_schema_->attribute_num() + array_schema_->dim_num() + 1;
  Status st = buff->write(&next_tile_var_offsets_[0], num * sizeof(uint64_t));
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
Status FragmentMetadata::write_generic_tile_offsets(Buffer* buff) {
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

  return Status::Ok();
}

// ===== FORMAT =====
// last_tile_cell_num(uint64_t)
Status FragmentMetadata::write_last_tile_cell_num(Buffer* buff) {
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
  RETURN_NOT_OK(write_generic_tile_to_file(encryption_key, &buff, nbytes));
  return Status::Ok();
}

Status FragmentMetadata::write_rtree(Buffer* buff) {
  RETURN_NOT_OK(create_rtree());
  RETURN_NOT_OK(rtree_.serialize(buff));
  return Status::Ok();
}

// ===== FORMAT =====
// non_empty_domain_size(uint64_t) non_empty_domain(void*)
Status FragmentMetadata::write_non_empty_domain(Buffer* buff) {
  bool null_non_empty_domain = (non_empty_domain_.empty());

  // Write null non-empty domain
  RETURN_NOT_OK(buff->write(&null_non_empty_domain, sizeof(char)));

  // Write non-empty domain
  if (!non_empty_domain_.empty()) {
    for (const auto& r : non_empty_domain_)
      RETURN_NOT_OK(buff->write(&r[0], r.size()));
  } else {  // Write some dummy values
    // Note: we will have to revisit this when we add support for
    // heterogeneous and string dimensions
    auto size = 2 * array_schema_->coords_size();
    std::vector<uint8_t> d(size, 0);
    RETURN_NOT_OK(buff->write(&d[0], size));
  }

  return Status::Ok();
}

Status FragmentMetadata::read_generic_tile_from_file(
    const EncryptionKey& encryption_key, uint64_t offset, Buffer* buff) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  // Read metadata
  TileIO tile_io(storage_manager_, fragment_metadata_uri);
  auto tile = (Tile*)nullptr;
  RETURN_NOT_OK(tile_io.read_generic(&tile, offset, encryption_key));
  tile->buffer()->swap(*buff);
  delete tile;

  return Status::Ok();
}

Status FragmentMetadata::read_file_footer(Buffer* buff) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  // Get footer offset
  uint64_t footer_offset = 0, footer_size = 0;
  RETURN_NOT_OK(get_footer_offset_and_size(&footer_offset, &footer_size));

  // Read footer
  return storage_manager_->read(
      fragment_metadata_uri, footer_offset, buff, footer_size);
}

Status FragmentMetadata::write_generic_tile_to_file(
    const EncryptionKey& encryption_key, Buffer* buff, uint64_t* nbytes) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));
  buff->reset_offset();
  Tile tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      buff,
      false);
  TileIO tile_io(storage_manager_, fragment_metadata_uri);
  RETURN_NOT_OK(tile_io.write_generic(&tile, encryption_key, nbytes));

  return Status::Ok();
}

Status FragmentMetadata::write_file_footer(Buffer* buff) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  return storage_manager_->write(
      fragment_metadata_uri, buff->data(), buff->size());
}

Status FragmentMetadata::store_tile_offsets(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;
  RETURN_NOT_OK(write_tile_offsets(idx, &buff));
  RETURN_NOT_OK(write_generic_tile_to_file(encryption_key, &buff, nbytes));

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
  RETURN_NOT_OK(write_generic_tile_to_file(encryption_key, &buff, nbytes));

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
  RETURN_NOT_OK(write_generic_tile_to_file(encryption_key, &buff, nbytes));

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

Status FragmentMetadata::write_version(Buffer* buff) {
  RETURN_NOT_OK(buff->write(&version_, sizeof(uint32_t)));
  return Status::Ok();
}

Status FragmentMetadata::write_dense(Buffer* buff) {
  RETURN_NOT_OK(buff->write(&dense_, sizeof(char)));
  return Status::Ok();
}

Status FragmentMetadata::write_sparse_tile_num(Buffer* buff) {
  RETURN_NOT_OK(buff->write(&sparse_tile_num_, sizeof(uint64_t)));
  return Status::Ok();
}

Status FragmentMetadata::store_footer(const EncryptionKey& encryption_key) {
  (void)encryption_key;  // Not used for now, maybe in the future

  Buffer buff;
  RETURN_NOT_OK(write_version(&buff));
  RETURN_NOT_OK(write_dense(&buff));
  RETURN_NOT_OK(write_non_empty_domain(&buff));
  RETURN_NOT_OK(write_sparse_tile_num(&buff));
  RETURN_NOT_OK(write_last_tile_cell_num(&buff));
  RETURN_NOT_OK(write_file_sizes(&buff));
  RETURN_NOT_OK(write_file_var_sizes(&buff));
  RETURN_NOT_OK(write_generic_tile_offsets(&buff));
  RETURN_NOT_OK(write_file_footer(&buff));

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

}  // namespace sm
}  // namespace tiledb
