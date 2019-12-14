/**
 * @file   book_keeping.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
 * This file implements the BookKeeping class.
 */

#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/tile/tile_io.h"

#include <cassert>
#include <iostream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

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

  std::cerr << "JOE FragmentMetadata 1" << std::endl;

  domain_ = nullptr;
  meta_file_size_ = 0;
  non_empty_domain_ = nullptr;
  version_ = constants::format_version;
  tile_index_base_ = 0;
  sparse_tile_num_ = 0;
  std::cerr << "JOE FragmentMetadata 2" << std::endl;
  auto attributes = array_schema_->attributes();
  std::cerr << "JOE FragmentMetadata 3" << std::endl;
  for (unsigned i = 0; i < attributes.size(); ++i) {
    std::cerr << "JOE FragmentMetadata 4" << std::endl;
    auto attr_name = attributes[i]->name();
    std::cerr << "JOE FragmentMetadata 5" << std::endl;
    attribute_idx_map_[attr_name] = i;
    std::cerr << "JOE FragmentMetadata 5.1" << std::endl;
    attribute_uri_map_[attr_name] =
        fragment_uri_.join_path(attr_name + constants::file_suffix);
    std::cerr << "JOE FragmentMetadata 6" << std::endl;
    if (attributes[i]->var_size()) {
      std::cerr << "JOE FragmentMetadata 7" << std::endl;
      attribute_var_uri_map_[attr_name] =
          fragment_uri_.join_path(attr_name + "_var" + constants::file_suffix);
      std::cerr << "JOE FragmentMetadata 8" << std::endl;
    }
  }
  std::cerr << "JOE FragmentMetadata 4" << std::endl;
  attribute_idx_map_[constants::coords] = array_schema_->attribute_num();
  attribute_uri_map_[constants::coords] =
      fragment_uri_.join_path(constants::coords + constants::file_suffix);
}

FragmentMetadata::~FragmentMetadata() {
  std::free(domain_);
  std::free(non_empty_domain_);

  auto mbr_num = (uint64_t)mbrs_.size();
  for (uint64_t i = 0; i < mbr_num; ++i)
    std::free(mbrs_[i]);

  auto bounding_coords_num = (uint64_t)bounding_coords_.size();
  for (uint64_t i = 0; i < bounding_coords_num; ++i)
    std::free(bounding_coords_[i]);
}

/* ****************************** */
/*                API             */
/* ****************************** */

const URI& FragmentMetadata::array_uri() const {
  return array_schema_->array_uri();
}

Status FragmentMetadata::set_mbr(uint64_t tile, const void* mbr) {
  switch (array_schema_->coords_type()) {
    case Datatype::INT8:
      return set_mbr<int8_t>(tile, static_cast<const int8_t*>(mbr));
    case Datatype::UINT8:
      return set_mbr<uint8_t>(tile, static_cast<const uint8_t*>(mbr));
    case Datatype::INT16:
      return set_mbr<int16_t>(tile, static_cast<const int16_t*>(mbr));
    case Datatype::UINT16:
      return set_mbr<uint16_t>(tile, static_cast<const uint16_t*>(mbr));
    case Datatype::INT32:
      return set_mbr<int>(tile, static_cast<const int*>(mbr));
    case Datatype::UINT32:
      return set_mbr<unsigned>(tile, static_cast<const unsigned*>(mbr));
    case Datatype::INT64:
      return set_mbr<int64_t>(tile, static_cast<const int64_t*>(mbr));
    case Datatype::UINT64:
      return set_mbr<uint64_t>(tile, static_cast<const uint64_t*>(mbr));
    case Datatype::FLOAT32:
      return set_mbr<float>(tile, static_cast<const float*>(mbr));
    case Datatype::FLOAT64:
      return set_mbr<double>(tile, static_cast<const double*>(mbr));
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
      return set_mbr<int64_t>(tile, static_cast<const int64_t*>(mbr));
    default:
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot append mbr; Unsupported coordinates type"));
  }
}

template <class T>
Status FragmentMetadata::set_mbr(uint64_t tile, const void* mbr) {
  // For easy reference
  uint64_t mbr_size = 2 * array_schema_->coords_size();
  tile += tile_index_base_;

  // Copy and set MBR
  void* new_mbr = std::malloc(mbr_size);
  std::memcpy(new_mbr, mbr, mbr_size);
  assert(tile < mbrs_.size());
  mbrs_[tile] = new_mbr;

  return expand_non_empty_domain(static_cast<const T*>(mbr));
}

void FragmentMetadata::set_tile_index_base(uint64_t tile_base) {
  tile_index_base_ = tile_base;
}

void FragmentMetadata::set_tile_offset(
    const std::string& attribute, uint64_t tile, uint64_t tile_size) {
  auto attribute_id = attribute_idx_map_[attribute];
  tile += tile_index_base_;
  assert(tile < tile_offsets_[attribute_id].size());
  tile_offsets_[attribute_id][tile] = next_tile_offsets_[attribute_id];
  next_tile_offsets_[attribute_id] += tile_size;
}

void FragmentMetadata::set_tile_var_offset(
    const std::string& attribute, uint64_t tile, uint64_t step) {
  auto attribute_id = attribute_idx_map_[attribute];
  tile += tile_index_base_;
  assert(tile < tile_var_offsets_[attribute_id].size());
  tile_var_offsets_[attribute_id][tile] = next_tile_var_offsets_[attribute_id];
  next_tile_var_offsets_[attribute_id] += step;
}

void FragmentMetadata::set_tile_var_size(
    const std::string& attribute, uint64_t tile, uint64_t size) {
  auto attribute_id = attribute_idx_map_[attribute];
  tile += tile_index_base_;
  assert(tile < tile_var_sizes_[attribute_id].size());
  tile_var_sizes_[attribute_id][tile] = size;
}

uint64_t FragmentMetadata::cell_num(uint64_t tile_pos) const {
  if (dense_)
    return array_schema_->domain()->cell_num_per_tile();

  uint64_t tile_num = this->tile_num();
  if (tile_pos != tile_num - 1)
    return array_schema_->capacity();

  return last_tile_cell_num();
}

template <class T>
Status FragmentMetadata::add_max_buffer_sizes(
    const EncryptionKey& encryption_key,
    const T* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  if (dense_)
    return add_max_buffer_sizes_dense(encryption_key, subarray, buffer_sizes);
  return add_max_buffer_sizes_sparse(encryption_key, subarray, buffer_sizes);
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

// TODO (sp): remove in 1.7
template <class T>
Status FragmentMetadata::add_max_buffer_sizes_sparse(
    const EncryptionKey& encryption_key,
    const T* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  RETURN_NOT_OK(load_rtree(encryption_key));

  // Get tile overlap
  std::vector<const T*> range;
  auto dim_num = array_schema_->dim_num();
  range.resize(dim_num);
  for (unsigned i = 0; i < dim_num; ++i)
    range[i] = &subarray[2 * i];
  auto tile_overlap = rtree_.get_tile_overlap<T>(range);
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

const void* FragmentMetadata::domain() const {
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

template <class T>
Status FragmentMetadata::get_tile_overlap(
    const EncryptionKey& encryption_key,
    const std::vector<const T*>& range,
    TileOverlap* tile_overlap) {
  // Return if the range does not overlap the non-empty domain of the fragment
  if (!utils::geometry::overlap(range, (const T*)non_empty_domain_))
    return Status::Ok();

  // Handle version > 2
  if (version_ > 2) {
    RETURN_NOT_OK(load_rtree(encryption_key));
    *tile_overlap = rtree_.get_tile_overlap<T>(range);
    return Status::Ok();
  }

  // Handle version <= 2
  auto mbr_num = mbrs_.size();
  for (size_t t = 0; t < mbr_num; ++t) {
    auto m = (const T*)mbrs_[t];
    auto overlap = RTree::range_overlap<T>(range, m);
    if (overlap > 0.0) {
      auto to = std::pair<uint64_t, double>(t, overlap);
      tile_overlap->tiles_.push_back(to);
    }
  }

  return Status::Ok();
}

template <class T>
uint64_t FragmentMetadata::get_tile_pos(const T* tile_coords) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();

  // Get tile subarray of the expanded non-empty domain
  std::vector<T> tile_subarray;
  tile_subarray.resize(2 * dim_num);
  array_schema_->domain()->get_tile_domain((T*)domain_, &tile_subarray[0]);

  // Normalize tile coords such in tile subarray
  std::vector<T> norm_tile_coords;
  norm_tile_coords.resize(dim_num);
  for (unsigned i = 0; i < dim_num; ++i)
    norm_tile_coords[i] = tile_coords[i] - tile_subarray[2 * i];

  // Return tile pos in tile subarray
  return array_schema_->domain()->get_tile_pos(
      (T*)domain_, &norm_tile_coords[0]);
}

Status FragmentMetadata::init(const void* non_empty_domain) {
  // For easy reference
  unsigned int attribute_num = array_schema_->attribute_num();
  auto domain = array_schema_->domain();

  // Sanity check
  assert(non_empty_domain != nullptr);
  assert(non_empty_domain_ == nullptr);
  assert(domain_ == nullptr);

  // Set non-empty domain for dense arrays (for sparse it will be calculated
  // via the MBRs)
  uint64_t domain_size = 2 * array_schema_->coords_size();
  if (dense_) {
    // Set non-empty domain
    non_empty_domain_ = std::malloc(domain_size);
    std::memcpy(non_empty_domain_, non_empty_domain, domain_size);
    // The following is needed in case the fragment is a result of
    // dense consolidation, as the consolidator may have expanded
    // the fragment domain beyond the array domain to include
    // integral space tiles
    domain->crop_domain(non_empty_domain_);

    // Set expanded domain
    domain_ = std::malloc(domain_size);
    std::memcpy(domain_, non_empty_domain_, domain_size);
    domain->expand_domain(domain_);
  }

  // Set last tile cell number
  last_tile_cell_num_ = 0;

  // Initialize tile offsets
  tile_offsets_.resize(attribute_num + 1);
  next_tile_offsets_.resize(attribute_num + 1);
  for (unsigned int i = 0; i < attribute_num + 1; ++i)
    next_tile_offsets_[i] = 0;

  // Initialize variable tile offsets
  tile_var_offsets_.resize(attribute_num);
  next_tile_var_offsets_.resize(attribute_num);
  for (unsigned int i = 0; i < attribute_num; ++i)
    next_tile_var_offsets_[i] = 0;

  // Initialize variable tile sizes
  tile_var_sizes_.resize(attribute_num);

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
  std::string uri_str = fragment_uri_.c_str();
  if (uri_str.back() == '/')
    uri_str.pop_back();
  std::string name = URI(uri_str).last_path_part();
  uint32_t f_version;
  RETURN_NOT_OK(utils::parse::get_fragment_name_version(name, &f_version));

  if (f_version == 1)
    return load_v2(encryption_key);
  return load_v3(encryption_key);
}

Status FragmentMetadata::store(const EncryptionKey& encryption_key) {
  auto array_uri = this->array_uri();
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  unsigned int attribute_num = array_schema_->attribute_num();
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
  auto st = store_rtree(encryption_key, &nbytes);
  offset += nbytes;
  if (!st.ok()) {
    storage_manager_->close_file(fragment_metadata_uri);
    storage_manager_->vfs()->remove_file(fragment_metadata_uri);
    storage_manager_->array_xunlock(array_uri);
    return st;
  }

  // Store tile offsets
  gt_offsets_.tile_offsets_.resize(attribute_num + 1);
  for (unsigned int i = 0; i < attribute_num + 1; ++i) {
    gt_offsets_.tile_offsets_[i] = offset;
    st = store_tile_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
    if (!st.ok()) {
      storage_manager_->close_file(fragment_metadata_uri);
      storage_manager_->vfs()->remove_file(fragment_metadata_uri);
      storage_manager_->array_xunlock(array_uri);
      return st;
    }
  }

  // Store tile var offsets
  gt_offsets_.tile_var_offsets_.resize(attribute_num);
  for (unsigned int i = 0; i < attribute_num; ++i) {
    gt_offsets_.tile_var_offsets_[i] = offset;
    st = store_tile_var_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
    if (!st.ok()) {
      storage_manager_->close_file(fragment_metadata_uri);
      storage_manager_->vfs()->remove_file(fragment_metadata_uri);
      storage_manager_->array_xunlock(array_uri);
      return st;
    }
  }

  // Store tile var sizes
  gt_offsets_.tile_var_sizes_.resize(attribute_num);
  for (unsigned int i = 0; i < attribute_num; ++i) {
    gt_offsets_.tile_var_sizes_[i] = offset;
    st = store_tile_var_sizes(i, encryption_key, &nbytes);
    offset += nbytes;
    if (!st.ok()) {
      storage_manager_->close_file(fragment_metadata_uri);
      storage_manager_->vfs()->remove_file(fragment_metadata_uri);
      storage_manager_->array_xunlock(array_uri);
      return st;
    }
  }

  // Store footer
  st = store_footer(encryption_key);
  if (!st.ok()) {
    storage_manager_->close_file(fragment_metadata_uri);
    storage_manager_->vfs()->remove_file(fragment_metadata_uri);
    storage_manager_->array_xunlock(array_uri);
    return st;
  }

  // Close file
  st = storage_manager_->close_file(fragment_metadata_uri);

  // Unlock array
  auto st2 = storage_manager_->array_xunlock(array_uri);

  return !st.ok() ? st : st2;
}

const std::vector<void*> FragmentMetadata::mbrs() const {
  return mbrs_;
}

const void* FragmentMetadata::non_empty_domain() const {
  return non_empty_domain_;
}

Status FragmentMetadata::set_num_tiles(uint64_t num_tiles) {
  auto num_attributes = array_schema_->attribute_num();

  for (unsigned i = 0; i < num_attributes + 1; i++) {
    assert(num_tiles >= tile_offsets_[i].size());
    tile_offsets_[i].resize(num_tiles, 0);
    if (i < num_attributes) {
      tile_var_offsets_[i].resize(num_tiles, 0);
      tile_var_sizes_[i].resize(num_tiles, 0);
    }
  }

  if (!dense_) {
    mbrs_.resize(num_tiles, nullptr);
    sparse_tile_num_ = num_tiles;
    bounding_coords_.resize(num_tiles, nullptr);
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

URI FragmentMetadata::attr_uri(const std::string& attribute) const {
  return attribute_uri_map_.at(attribute);
}

URI FragmentMetadata::attr_var_uri(const std::string& attribute) const {
  return attribute_var_uri_map_.at(attribute);
}

Status FragmentMetadata::file_offset(
    const EncryptionKey& encryption_key,
    const std::string& attribute,
    uint64_t tile_idx,
    uint64_t* offset) {
  auto it = attribute_idx_map_.find(attribute);
  auto attribute_id = it->second;
  RETURN_NOT_OK(load_tile_offsets(encryption_key, attribute_id));
  *offset = tile_offsets_[attribute_id][tile_idx];
  return Status::Ok();
}

Status FragmentMetadata::file_var_offset(
    const EncryptionKey& encryption_key,
    const std::string& attribute,
    uint64_t tile_idx,
    uint64_t* offset) {
  auto it = attribute_idx_map_.find(attribute);
  auto attribute_id = it->second;
  RETURN_NOT_OK(load_tile_var_offsets(encryption_key, attribute_id));
  *offset = tile_var_offsets_[attribute_id][tile_idx];
  return Status::Ok();
}

const void* FragmentMetadata::mbr(uint64_t tile_idx) const {
  return rtree_.leaf(tile_idx);
}

Status FragmentMetadata::persisted_tile_size(
    const EncryptionKey& encryption_key,
    const std::string& attribute,
    uint64_t tile_idx,
    uint64_t* tile_size) {
  auto it = attribute_idx_map_.find(attribute);
  auto attribute_id = it->second;
  RETURN_NOT_OK(load_tile_offsets(encryption_key, attribute_id));

  auto tile_num = this->tile_num();

  *tile_size =
      (tile_idx != tile_num - 1) ?
          tile_offsets_[attribute_id][tile_idx + 1] -
              tile_offsets_[attribute_id][tile_idx] :
          file_sizes_[attribute_id] - tile_offsets_[attribute_id][tile_idx];

  return Status::Ok();
}

Status FragmentMetadata::persisted_tile_var_size(
    const EncryptionKey& encryption_key,
    const std::string& attribute,
    uint64_t tile_idx,
    uint64_t* tile_size) {
  auto it = attribute_idx_map_.find(attribute);
  auto attribute_id = it->second;
  RETURN_NOT_OK(load_tile_var_offsets(encryption_key, attribute_id));
  auto tile_num = this->tile_num();

  *tile_size = (tile_idx != tile_num - 1) ?
                   tile_var_offsets_[attribute_id][tile_idx + 1] -
                       tile_var_offsets_[attribute_id][tile_idx] :
                   file_var_sizes_[attribute_id] -
                       tile_var_offsets_[attribute_id][tile_idx];

  return Status::Ok();
}

Status FragmentMetadata::rtree(
    const EncryptionKey& encryption_key, const RTree** rtree) {
  RETURN_NOT_OK(load_rtree(encryption_key));
  *rtree = (version_ <= 2) ? nullptr : &rtree_;
  return Status::Ok();
}

uint64_t FragmentMetadata::tile_size(
    const std::string& attribute, uint64_t tile_idx) const {
  auto var_size = array_schema_->var_size(attribute);
  auto cell_num = this->cell_num(tile_idx);
  return (var_size) ? cell_num * constants::cell_var_offset_size :
                      cell_num * array_schema_->cell_size(attribute);
}

Status FragmentMetadata::tile_var_size(
    const EncryptionKey& encryption_key,
    const std::string& attribute,
    uint64_t tile_idx,
    uint64_t* tile_size) {
  auto it = attribute_idx_map_.find(attribute);
  auto attribute_id = it->second;
  RETURN_NOT_OK(load_tile_var_sizes(encryption_key, attribute_id));
  *tile_size = tile_var_sizes_[attribute_id][tile_idx];
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

template <class T>
std::vector<uint64_t> FragmentMetadata::compute_overlapping_tile_ids(
    const T* subarray) const {
  assert(dense_);
  std::vector<uint64_t> tids;
  auto dim_num = array_schema_->dim_num();
  auto metadata_domain = static_cast<const T*>(domain_);

  // Check if there is any overlap
  if (!utils::geometry::overlap(subarray, metadata_domain, dim_num))
    return tids;

  // Initialize subarray tile domain
  auto subarray_tile_domain = new T[2 * dim_num];
  get_subarray_tile_domain(subarray, subarray_tile_domain);

  // Initialize tile coordinates
  auto tile_coords = new T[dim_num];
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
  delete[] subarray_tile_domain;
  delete[] tile_coords;

  return tids;
}

template <class T>
std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov(const T* subarray) const {
  assert(dense_);
  std::vector<std::pair<uint64_t, double>> tids;
  auto dim_num = array_schema_->dim_num();
  auto metadata_domain = static_cast<const T*>(domain_);

  // Check if there is any overlap
  if (!utils::geometry::overlap(subarray, metadata_domain, dim_num))
    return tids;

  // Initialize subarray tile domain
  auto subarray_tile_domain = new T[2 * dim_num];
  get_subarray_tile_domain(subarray, subarray_tile_domain);

  auto tile_subarray = new T[2 * dim_num];
  auto tile_overlap = new T[2 * dim_num];
  bool overlap;
  double cov;

  // Initialize tile coordinates
  auto tile_coords = new T[dim_num];
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
  delete[] subarray_tile_domain;
  delete[] tile_coords;
  delete[] tile_subarray;
  delete[] tile_overlap;

  return tids;
}

template <class T>
void FragmentMetadata::get_subarray_tile_domain(
    const T* subarray, T* subarray_tile_domain) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents =
      static_cast<const T*>(array_schema_->domain()->tile_extents());

  // Calculate subarray in tile domain
  for (unsigned int i = 0; i < dim_num; ++i) {
    auto overlap = MAX(subarray[2 * i], domain[2 * i]);
    subarray_tile_domain[2 * i] = (overlap - domain[2 * i]) / tile_extents[i];

    overlap = MIN(subarray[2 * i + 1], domain[2 * i + 1]);
    subarray_tile_domain[2 * i + 1] =
        (overlap - domain[2 * i]) / tile_extents[i];
  }
}

template <class T>
Status FragmentMetadata::expand_non_empty_domain(const T* mbr) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (non_empty_domain_ == nullptr) {
    auto domain_size = 2 * array_schema_->coords_size();
    non_empty_domain_ = std::malloc(domain_size);
    if (non_empty_domain_ == nullptr)
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot expand non-empty domain; Memory allocation failed"));

    std::memcpy(non_empty_domain_, mbr, domain_size);
    return Status::Ok();
  }

  auto dim_num = array_schema_->dim_num();
  auto coords = new T[dim_num];
  for (unsigned i = 0; i < dim_num; ++i)
    coords[i] = mbr[2 * i];
  auto non_empty_domain = static_cast<T*>(non_empty_domain_);
  utils::geometry::expand_mbr(non_empty_domain, coords, dim_num);
  for (unsigned i = 0; i < dim_num; ++i)
    coords[i] = mbr[2 * i + 1];
  utils::geometry::expand_mbr(non_empty_domain, coords, dim_num);
  delete[] coords;

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
    const EncryptionKey& encryption_key, unsigned attr_id) {
  if (version_ <= 2)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_offsets_[attr_id])
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_offsets_[attr_id], &buff));
  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_tile_offsets(attr_id, &cbuff));

  loaded_metadata_.tile_offsets_[attr_id] = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_var_offsets(
    const EncryptionKey& encryption_key, unsigned attr_id) {
  if (version_ <= 2)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_var_offsets_[attr_id])
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_var_offsets_[attr_id], &buff));

  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_tile_var_offsets(attr_id, &cbuff));

  loaded_metadata_.tile_var_offsets_[attr_id] = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_var_sizes(
    const EncryptionKey& encryption_key, unsigned attr_id) {
  if (version_ <= 2)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_var_sizes_[attr_id])
    return Status::Ok();

  Buffer buff;
  RETURN_NOT_OK(read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_var_sizes_[attr_id], &buff));

  ConstBuffer cbuff(&buff);
  RETURN_NOT_OK(load_tile_var_sizes(attr_id, &cbuff));

  loaded_metadata_.tile_var_sizes_[attr_id] = true;

  return Status::Ok();
}

// ===== FORMAT =====
//  bounding_coords_num (uint64_t)
//  bounding_coords_#1 (void*) bounding_coords_#2 (void*) ...
Status FragmentMetadata::load_bounding_coords(ConstBuffer* buff) {
  uint64_t bounding_coords_size = 2 * array_schema_->coords_size();

  // Get number of bounding coordinates
  uint64_t bounding_coords_num = 0;
  Status st = buff->read(&bounding_coords_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading number of "
        "bounding coordinates failed"));
  }
  // Get bounding coordinates
  void* bounding_coords;
  bounding_coords_.resize(bounding_coords_num);
  for (uint64_t i = 0; i < bounding_coords_num; ++i) {
    bounding_coords = std::malloc(bounding_coords_size);
    st = buff->read(bounding_coords, bounding_coords_size);
    if (!st.ok()) {
      std::free(bounding_coords);
      return LOG_STATUS(
          Status::FragmentMetadataError("Cannot load fragment metadata; "
                                        "Reading bounding coordinates failed"));
    }
    bounding_coords_[i] = bounding_coords;
  }
  return Status::Ok();
}

// ===== FORMAT =====
// file_sizes_attr#0 (uint64_t)
// ...
// file_sizes_attr#attribute_num (uint64_t)
Status FragmentMetadata::load_file_sizes(ConstBuffer* buff) {
  unsigned int attribute_num = array_schema_->attribute_num();
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
// file_sizes_attr#0 (uint64_t)
// ...
// file_sizes_attr#attribute_num (uint64_t)
Status FragmentMetadata::load_file_var_sizes(ConstBuffer* buff) {
  unsigned int attribute_num = array_schema_->attribute_num();
  file_var_sizes_.resize(attribute_num);
  Status st = buff->read(&file_var_sizes_[0], attribute_num * sizeof(uint64_t));

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
  Status st = buff->read(&mbr_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading number of MBRs failed"));
  }

  // Get MBRs
  uint64_t mbr_size = 2 * array_schema_->coords_size();
  void* mbr = nullptr;
  mbrs_.resize(mbr_num);
  for (uint64_t i = 0; i < mbr_num; ++i) {
    mbr = std::malloc(mbr_size);
    st = buff->read(mbr, mbr_size);
    if (!st.ok()) {
      std::free(mbr);
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading MBR failed"));
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
  Status st = buff->read(&domain_size, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading domain size failed"));
  }

  // Get non-empty domain
  if (domain_size == 0) {
    non_empty_domain_ = nullptr;
  } else {
    non_empty_domain_ = std::malloc(domain_size);
    st = buff->read(non_empty_domain_, domain_size);
    if (!st.ok()) {
      std::free(non_empty_domain_);
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading domain failed"));
    }
  }

  // Get expanded domain
  if (non_empty_domain_ == nullptr) {
    domain_ = nullptr;
  } else {
    domain_ = std::malloc(domain_size);
    std::memcpy(domain_, non_empty_domain_, domain_size);
    array_schema_->domain()->expand_domain(domain_);
  }

  return Status::Ok();
}

// ===== FORMAT =====
// null non_empty_domain (char)
// non_empty_domain (domain_size)
Status FragmentMetadata::load_non_empty_domain_v3(ConstBuffer* buff) {
  // Get null non-empty domain
  bool null_non_empty_domain = false;
  Status st = buff->read(&null_non_empty_domain, sizeof(char));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading domain size failed"));
  }

  // Get non-empty domain
  auto domain_size = 2 * array_schema_->coords_size();
  non_empty_domain_ = std::malloc(domain_size);
  st = buff->read(non_empty_domain_, domain_size);
  if (!st.ok()) {
    std::free(non_empty_domain_);
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading domain failed"));
  }
  if (null_non_empty_domain) {
    std::free(non_empty_domain_);
    non_empty_domain_ = nullptr;
  }

  // Get expanded domain
  if (non_empty_domain_ == nullptr) {
    domain_ = nullptr;
  } else {
    domain_ = std::malloc(domain_size);
    std::memcpy(domain_, non_empty_domain_, domain_size);
    array_schema_->domain()->expand_domain(domain_);
  }

  return Status::Ok();
}

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

Status FragmentMetadata::load_tile_offsets(
    unsigned attr_id, ConstBuffer* buff) {
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
    tile_offsets_[attr_id].resize(tile_offsets_num);
    st = buff->read(
        &tile_offsets_[attr_id][0], tile_offsets_num * sizeof(uint64_t));
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
    unsigned attr_id, ConstBuffer* buff) {
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
    tile_var_offsets_[attr_id].resize(tile_var_offsets_num);
    st = buff->read(
        &tile_var_offsets_[attr_id][0],
        tile_var_offsets_num * sizeof(uint64_t));
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

Status FragmentMetadata::load_tile_var_sizes(
    unsigned attr_id, ConstBuffer* buff) {
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
    tile_var_sizes_[attr_id].resize(tile_var_sizes_num);
    st = buff->read(
        &tile_var_sizes_[attr_id][0], tile_var_sizes_num * sizeof(uint64_t));
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
  auto dim_num = array_schema_->dim_num();
  auto type = array_schema_->domain()->type();
  auto rtree = RTree(type, dim_num, constants::rtree_fanout, mbrs_);
  rtree_ = std::move(rtree);
  return Status::Ok();
}

Status FragmentMetadata::get_generic_tile_size(
    uint64_t offset, uint64_t* size) {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));
  TileIO tile_io(storage_manager_, fragment_metadata_uri);
  TileIO::GenericTileHeader header;
  RETURN_NOT_OK(tile_io.read_generic_tile_header(
      storage_manager_, fragment_metadata_uri, offset, &header));

  *size = TileIO::GenericTileHeader::BASE_SIZE + header.filter_pipeline_size +
          header.persisted_size;

  return Status::Ok();
}

Status FragmentMetadata::load_generic_tile_offsets(ConstBuffer* buff) {
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

Status FragmentMetadata::load_v2(const EncryptionKey& encryption_key) {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  // Read metadata
  TileIO tile_io(storage_manager_, fragment_metadata_uri);
  auto tile = (Tile*)nullptr;
  RETURN_NOT_OK(tile_io.read_generic(&tile, 0, encryption_key));
  //tile->disown_buff();
  Buffer buff(*tile->buffer2());
  STATS_COUNTER_ADD(fragment_metadata_bytes_read, tile_io.file_size());
  delete tile;

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
  RETURN_NOT_OK(create_rtree());

  return Status::Ok();
}

Status FragmentMetadata::load_v3(const EncryptionKey& encryption_key) {
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

  tile_offsets_.resize(array_schema_->attribute_num() + 1);
  tile_var_offsets_.resize(array_schema_->attribute_num());
  tile_var_sizes_.resize(array_schema_->attribute_num());

  loaded_metadata_.tile_offsets_.resize(
      array_schema_->attribute_num() + 1, false);
  loaded_metadata_.tile_var_offsets_.resize(
      array_schema_->attribute_num(), false);
  loaded_metadata_.tile_var_sizes_.resize(
      array_schema_->attribute_num(), false);

  RETURN_NOT_OK(load_generic_tile_offsets(&cbuff));

  loaded_metadata_.footer_ = true;

  return Status::Ok();
}

// ===== FORMAT =====
// file_sizes_attr#0 (uint64_t)
// ...
// file_sizes_attr#attribute_num (uint64_t)
Status FragmentMetadata::write_file_sizes(Buffer* buff) {
  unsigned int attribute_num = array_schema_->attribute_num();
  Status st = buff->write(
      &next_tile_offsets_[0], (attribute_num + 1) * sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing file sizes failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// file_var_sizes_attr#0 (uint64_t)
// ...
// file_var_sizes_attr#attribute_num (uint64_t)
Status FragmentMetadata::write_file_var_sizes(Buffer* buff) {
  unsigned int attribute_num = array_schema_->attribute_num();
  Status st =
      buff->write(&next_tile_var_offsets_[0], attribute_num * sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing file sizes failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// rtree_offset(uint64_t)
// tile_offsets_offset_attr1(uint64_t)
// ...
// tile_offsets_offset_attr_{attr_num+1}(uint64_t)
// tile_var_offsets_offset_attr1(uint64_t)
// ...
// tile_var_offsets_offset_attr_{attr_num}(uint64_t)
// tile_var_sizes_offset_attr1(uint64_t)
// ...
// tile_var_sizes_offset_attr_{attr_num}(uint64_t)
Status FragmentMetadata::write_generic_tile_offsets(Buffer* buff) {
  unsigned int attribute_num = array_schema_->attribute_num();

  // Write R-Tree offset
  auto st = buff->write(&gt_offsets_.rtree_, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing R-Tree offset failed"));
  }

  // Write tile offsets
  for (unsigned i = 0; i < attribute_num + 1; ++i) {
    st = buff->write(&gt_offsets_.tile_offsets_[i], sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing tile offsets failed"));
    }
  }

  // Write tile var offsets
  for (unsigned i = 0; i < attribute_num; ++i) {
    st = buff->write(&gt_offsets_.tile_var_offsets_[i], sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(
          Status::FragmentMetadataError("Cannot serialize fragment metadata; "
                                        "Writing tile var offsets failed"));
    }
  }

  // Write tile var sizes
  for (unsigned i = 0; i < attribute_num; ++i) {
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
// mbr_num(uint64_t)
// mbr_#1(void*) mbr_#2(void*) ...
Status FragmentMetadata::write_mbrs(Buffer* buff) {
  Status st;
  uint64_t mbr_size = 2 * array_schema_->coords_size();
  uint64_t mbr_num = mbrs_.size();

  // Write number of MBRs
  st = buff->write(&mbr_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing number of MBRs failed"));
  }

  // Write MBRs
  for (uint64_t i = 0; i < mbr_num; ++i) {
    st = buff->write(mbrs_[i], mbr_size);
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing MBR failed"));
    }
  }

  return Status::Ok();
}

// ===== FORMAT =====
// non_empty_domain_size(uint64_t) non_empty_domain(void*)
Status FragmentMetadata::write_non_empty_domain(Buffer* buff) {
  uint64_t domain_size = 2 * array_schema_->coords_size();
  bool null_non_empty_domain = (non_empty_domain_ == nullptr);

  // Write null non-empty domain
  Status st = buff->write(&null_non_empty_domain, sizeof(char));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing non-empty domain failed"));
  }

  // Write non-empty domain
  if (non_empty_domain_ != nullptr) {
    st = buff->write(non_empty_domain_, domain_size);
    if (!st.ok()) {
      return LOG_STATUS(
          Status::FragmentMetadataError("Cannot serialize fragment metadata; "
                                        "Writing non-empty domain failed"));
    }
  } else {  // Write some dummy values
    std::vector<uint8_t> d(domain_size, 0);
    st = buff->write(&d[0], domain_size);
    if (!st.ok()) {
      return LOG_STATUS(
          Status::FragmentMetadataError("Cannot serialize fragment metadata; "
                                        "Writing non-empty domain failed"));
    }
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
  tile->buffer2()->swap(*buff);
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
    unsigned attr_id, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;
  RETURN_NOT_OK(write_tile_offsets(attr_id, &buff));
  RETURN_NOT_OK(write_generic_tile_to_file(encryption_key, &buff, nbytes));

  return Status::Ok();
}

Status FragmentMetadata::write_tile_offsets(unsigned attr_id, Buffer* buff) {
  Status st;

  // Write number of tile offsets
  uint64_t tile_offsets_num = tile_offsets_[attr_id].size();
  st = buff->write(&tile_offsets_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing number of tile offsets "
        "failed"));
  }

  // Write tile offsets
  if (tile_offsets_num != 0) {
    st = buff->write(
        &tile_offsets_[attr_id][0], tile_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing tile offsets failed"));
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::store_tile_var_offsets(
    unsigned attr_id, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;
  RETURN_NOT_OK(write_tile_var_offsets(attr_id, &buff));
  RETURN_NOT_OK(write_generic_tile_to_file(encryption_key, &buff, nbytes));

  return Status::Ok();
}

Status FragmentMetadata::write_tile_var_offsets(
    unsigned attr_id, Buffer* buff) {
  Status st;

  // Write tile offsets for each attribute
  // Write number of offsets
  uint64_t tile_var_offsets_num = tile_var_offsets_[attr_id].size();
  st = buff->write(&tile_var_offsets_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing number of "
        "variable tile offsets failed"));
  }

  // Write tile offsets
  if (tile_var_offsets_num != 0) {
    st = buff->write(
        &tile_var_offsets_[attr_id][0],
        tile_var_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing "
          "variable tile offsets failed"));
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::store_tile_var_sizes(
    unsigned attr_id, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;
  RETURN_NOT_OK(write_tile_var_sizes(attr_id, &buff));
  RETURN_NOT_OK(write_generic_tile_to_file(encryption_key, &buff, nbytes));

  return Status::Ok();
}

Status FragmentMetadata::write_tile_var_sizes(unsigned attr_id, Buffer* buff) {
  Status st;

  // Write number of sizes
  uint64_t tile_var_sizes_num = tile_var_sizes_[attr_id].size();
  st = buff->write(&tile_var_sizes_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing number of "
        "variable tile sizes failed"));
  }

  // Write tile sizes
  if (tile_var_sizes_num != 0) {
    st = buff->write(
        &tile_var_sizes_[attr_id][0], tile_var_sizes_num * sizeof(uint64_t));
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

// Explicit template instantiations
template Status FragmentMetadata::set_mbr<int8_t>(
    uint64_t tile, const void* mbr);
template Status FragmentMetadata::set_mbr<uint8_t>(
    uint64_t tile, const void* mbr);
template Status FragmentMetadata::set_mbr<int16_t>(
    uint64_t tile, const void* mbr);
template Status FragmentMetadata::set_mbr<uint16_t>(
    uint64_t tile, const void* mbr);
template Status FragmentMetadata::set_mbr<int32_t>(
    uint64_t tile, const void* mbr);
template Status FragmentMetadata::set_mbr<uint32_t>(
    uint64_t tile, const void* mbr);
template Status FragmentMetadata::set_mbr<int64_t>(
    uint64_t tile, const void* mbr);
template Status FragmentMetadata::set_mbr<uint64_t>(
    uint64_t tile, const void* mbr);
template Status FragmentMetadata::set_mbr<float>(
    uint64_t tile, const void* mbr);
template Status FragmentMetadata::set_mbr<double>(
    uint64_t tile, const void* mbr);

template Status FragmentMetadata::add_max_buffer_sizes<int8_t>(
    const EncryptionKey& encryption_key,
    const int8_t* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);
template Status FragmentMetadata::add_max_buffer_sizes<uint8_t>(
    const EncryptionKey& encryption_key,
    const uint8_t* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);
template Status FragmentMetadata::add_max_buffer_sizes<int16_t>(
    const EncryptionKey& encryption_key,
    const int16_t* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);
template Status FragmentMetadata::add_max_buffer_sizes<uint16_t>(
    const EncryptionKey& encryption_key,
    const uint16_t* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);
template Status FragmentMetadata::add_max_buffer_sizes<int>(
    const EncryptionKey& encryption_key,
    const int* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);
template Status FragmentMetadata::add_max_buffer_sizes<unsigned>(
    const EncryptionKey& encryption_key,
    const unsigned* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);
template Status FragmentMetadata::add_max_buffer_sizes<int64_t>(
    const EncryptionKey& encryption_key,
    const int64_t* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);
template Status FragmentMetadata::add_max_buffer_sizes<uint64_t>(
    const EncryptionKey& encryption_key,
    const uint64_t* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);
template Status FragmentMetadata::add_max_buffer_sizes<float>(
    const EncryptionKey& encryption_key,
    const float* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);
template Status FragmentMetadata::add_max_buffer_sizes<double>(
    const EncryptionKey& encryption_key,
    const double* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);

template uint64_t FragmentMetadata::get_tile_pos<int8_t>(
    const int8_t* tile_coords) const;
template uint64_t FragmentMetadata::get_tile_pos<uint8_t>(
    const uint8_t* tile_coords) const;
template uint64_t FragmentMetadata::get_tile_pos<int16_t>(
    const int16_t* tile_coords) const;
template uint64_t FragmentMetadata::get_tile_pos<uint16_t>(
    const uint16_t* tile_coords) const;
template uint64_t FragmentMetadata::get_tile_pos<int>(
    const int* tile_coords) const;
template uint64_t FragmentMetadata::get_tile_pos<unsigned>(
    const unsigned* tile_coords) const;
template uint64_t FragmentMetadata::get_tile_pos<int64_t>(
    const int64_t* tile_coords) const;
template uint64_t FragmentMetadata::get_tile_pos<uint64_t>(
    const uint64_t* tile_coords) const;

template Status FragmentMetadata::get_tile_overlap<int8_t>(
    const EncryptionKey& encryption_key,
    const std::vector<const int8_t*>& range,
    TileOverlap* tile_overlap);
template Status FragmentMetadata::get_tile_overlap<uint8_t>(
    const EncryptionKey& encryption_key,
    const std::vector<const uint8_t*>& range,
    TileOverlap* tile_overlap);
template Status FragmentMetadata::get_tile_overlap<int16_t>(
    const EncryptionKey& encryption_key,
    const std::vector<const int16_t*>& range,
    TileOverlap* tile_overlap);
template Status FragmentMetadata::get_tile_overlap<uint16_t>(
    const EncryptionKey& encryption_key,
    const std::vector<const uint16_t*>& range,
    TileOverlap* tile_overlap);
template Status FragmentMetadata::get_tile_overlap<int32_t>(
    const EncryptionKey& encryption_key,
    const std::vector<const int32_t*>& range,
    TileOverlap* tile_overlap);
template Status FragmentMetadata::get_tile_overlap<uint32_t>(
    const EncryptionKey& encryption_key,
    const std::vector<const uint32_t*>& range,
    TileOverlap* tile_overlap);
template Status FragmentMetadata::get_tile_overlap<int64_t>(
    const EncryptionKey& encryption_key,
    const std::vector<const int64_t*>& range,
    TileOverlap* tile_overlap);
template Status FragmentMetadata::get_tile_overlap<uint64_t>(
    const EncryptionKey& encryption_key,
    const std::vector<const uint64_t*>& range,
    TileOverlap* tile_overlap);
template Status FragmentMetadata::get_tile_overlap<float>(
    const EncryptionKey& encryption_key,
    const std::vector<const float*>& range,
    TileOverlap* tile_overlap);
template Status FragmentMetadata::get_tile_overlap<double>(
    const EncryptionKey& encryption_key,
    const std::vector<const double*>& range,
    TileOverlap* tile_overlap);

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
