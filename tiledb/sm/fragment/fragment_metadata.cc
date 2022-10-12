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

#include "tiledb/common/common.h"

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"
#include "tiledb/storage_format/serialization/serializers.h"
#include "tiledb/storage_format/uri/parse_uri.h"
#include "tiledb/type/range/range.h"

#include <cassert>
#include <iostream>
#include <numeric>
#include <string>

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb {
namespace sm {

class FragmentMetadataStatusException : public StatusException {
 public:
  explicit FragmentMetadataStatusException(const std::string& message)
      : StatusException("FragmentMetadata", message) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

FragmentMetadata::FragmentMetadata() {
}

FragmentMetadata::FragmentMetadata(
    StorageManager* storage_manager,
    MemoryTracker* memory_tracker,
    const shared_ptr<const ArraySchema>& array_schema,
    const URI& fragment_uri,
    const std::pair<uint64_t, uint64_t>& timestamp_range,
    bool dense,
    bool has_timestamps,
    bool has_deletes_meta)
    : storage_manager_(storage_manager)
    , memory_tracker_(memory_tracker)
    , array_schema_(array_schema)
    , dense_(dense)
    , footer_size_(0)
    , footer_offset_(0)
    , fragment_uri_(fragment_uri)
    , has_consolidated_footer_(false)
    , last_tile_cell_num_(0)
    , has_timestamps_(has_timestamps)
    , has_delete_meta_(has_deletes_meta)
    , sparse_tile_num_(0)
    , meta_file_size_(0)
    , rtree_(RTree(&array_schema_->domain(), constants::rtree_fanout))
    , tile_index_base_(0)
    , version_(array_schema_->write_version())
    , timestamp_range_(timestamp_range)
    , array_uri_(array_schema_->array_uri()) {
  build_idx_map();
  array_schema_name_ = array_schema_->name();
}

FragmentMetadata::~FragmentMetadata() = default;

// Copy initialization
FragmentMetadata::FragmentMetadata(const FragmentMetadata& other) {
  storage_manager_ = other.storage_manager_;
  array_schema_ = other.array_schema_;
  dense_ = other.dense_;
  fragment_uri_ = other.fragment_uri_;
  timestamp_range_ = other.timestamp_range_;
  has_consolidated_footer_ = other.has_consolidated_footer_;
  rtree_ = other.rtree_;
  meta_file_size_ = other.meta_file_size_;
  version_ = other.version_;
  tile_index_base_ = other.tile_index_base_;
  has_timestamps_ = other.has_timestamps_;
  has_delete_meta_ = other.has_delete_meta_;
  sparse_tile_num_ = other.sparse_tile_num_;
  footer_size_ = other.footer_size_;
  footer_offset_ = other.footer_offset_;
  idx_map_ = other.idx_map_;
  array_schema_name_ = other.array_schema_name_;
  array_uri_ = other.array_uri_;
}

FragmentMetadata& FragmentMetadata::operator=(const FragmentMetadata& other) {
  storage_manager_ = other.storage_manager_;
  array_schema_ = other.array_schema_;
  dense_ = other.dense_;
  fragment_uri_ = other.fragment_uri_;
  timestamp_range_ = other.timestamp_range_;
  has_consolidated_footer_ = other.has_consolidated_footer_;
  rtree_ = other.rtree_;
  meta_file_size_ = other.meta_file_size_;
  version_ = other.version_;
  tile_index_base_ = other.tile_index_base_;
  has_timestamps_ = other.has_timestamps_;
  has_delete_meta_ = other.has_delete_meta_;
  sparse_tile_num_ = other.sparse_tile_num_;
  footer_size_ = other.footer_size_;
  footer_offset_ = other.footer_offset_;
  idx_map_ = other.idx_map_;
  array_schema_name_ = other.array_schema_name_;
  array_uri_ = other.array_uri_;

  return *this;
}

/* ****************************** */
/*                API             */
/* ****************************** */

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

void FragmentMetadata::set_tile_min(
    const std::string& name, uint64_t tid, const ByteVec& min) {
  const auto size = min.size();
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * size;
  assert(tid < tile_min_buffer_[idx].size() / size);
  memcpy(&tile_min_buffer_[idx][buff_offset], min.data(), size);
}

void FragmentMetadata::set_tile_min_var_size(
    const std::string& name, uint64_t tid, uint64_t size) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * sizeof(uint64_t);
  assert(tid < tile_min_buffer_[idx].size() / sizeof(uint64_t));

  auto offset = (uint64_t*)&tile_min_buffer_[idx][buff_offset];
  *offset = size;
}

void FragmentMetadata::set_tile_min_var(
    const std::string& name, uint64_t tid, const ByteVec& min) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * sizeof(uint64_t);
  assert(tid < tile_min_buffer_[idx].size() / sizeof(uint64_t));

  auto offset = (uint64_t*)&tile_min_buffer_[idx][buff_offset];
  auto size = buff_offset != tile_min_buffer_[idx].size() - sizeof(uint64_t) ?
                  offset[1] - offset[0] :
                  tile_min_var_buffer_[idx].size() - offset[0];

  // Copy var data
  if (size) {  // avoid (potentially) illegal index ref's when size is zero
    memcpy(&tile_min_var_buffer_[idx][offset[0]], min.data(), size);
  }
}

void FragmentMetadata::set_tile_max(
    const std::string& name, uint64_t tid, const ByteVec& max) {
  const auto size = max.size();
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * size;
  assert(tid < tile_max_buffer_[idx].size() / size);
  memcpy(&tile_max_buffer_[idx][buff_offset], max.data(), size);
}

void FragmentMetadata::set_tile_max_var_size(
    const std::string& name, uint64_t tid, uint64_t size) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * sizeof(uint64_t);
  assert(tid < tile_max_buffer_[idx].size() / sizeof(uint64_t));

  auto offset = (uint64_t*)&tile_max_buffer_[idx][buff_offset];
  *offset = size;
}

void FragmentMetadata::set_tile_max_var(
    const std::string& name, uint64_t tid, const ByteVec& max) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * sizeof(uint64_t);
  assert(tid < tile_max_buffer_[idx].size() / sizeof(uint64_t));

  auto offset = (uint64_t*)&tile_max_buffer_[idx][buff_offset];
  auto size = buff_offset != tile_max_buffer_[idx].size() - sizeof(uint64_t) ?
                  offset[1] - offset[0] :
                  tile_max_var_buffer_[idx].size() - offset[0];

  // Copy var data
  if (size) {  // avoid (potentially) illegal index ref's when size is zero
    memcpy(&tile_max_var_buffer_[idx][offset[0]], max.data(), size);
  }
}

void FragmentMetadata::convert_tile_min_max_var_sizes_to_offsets(
    const std::string& name) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;

  // Fix the min offsets.
  uint64_t offset = tile_min_var_buffer_[idx].size();
  auto offsets = (uint64_t*)tile_min_buffer_[idx].data() + tile_index_base_;
  for (uint64_t i = tile_index_base_;
       i < tile_min_buffer_[idx].size() / sizeof(uint64_t);
       i++) {
    auto size = *offsets;
    *offsets = offset;
    offsets++;
    offset += size;
  }

  // Allocate min var data buffer.
  tile_min_var_buffer_[idx].resize(offset);

  // Fix the max offsets.
  offset = tile_max_var_buffer_[idx].size();
  offsets = (uint64_t*)tile_max_buffer_[idx].data() + tile_index_base_;
  for (uint64_t i = tile_index_base_;
       i < tile_max_buffer_[idx].size() / sizeof(uint64_t);
       i++) {
    auto size = *offsets;
    *offsets = offset;
    offsets++;
    offset += size;
  }

  // Allocate min var data buffer.
  tile_max_var_buffer_[idx].resize(offset);
}

void FragmentMetadata::set_tile_sum(
    const std::string& name, uint64_t tid, const ByteVec& sum) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  assert(tid * sizeof(uint64_t) < tile_sums_[idx].size());
  memcpy(
      &tile_sums_[idx][tid * sizeof(uint64_t)], sum.data(), sizeof(uint64_t));
}

void FragmentMetadata::set_tile_null_count(
    const std::string& name, uint64_t tid, uint64_t null_count) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  assert(tid < tile_null_counts_[idx].size());
  tile_null_counts_[idx][tid] = null_count;
}

template <>
void FragmentMetadata::compute_fragment_min_max_sum<char>(
    const std::string& name);

Status FragmentMetadata::compute_fragment_min_max_sum_null_count() {
  std::vector<std::string> names;
  names.reserve(idx_map_.size());
  for (auto& it : idx_map_) {
    names.emplace_back(it.first);
  }

  // Process all attributes in parallel.
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, idx_map_.size(), [&](uint64_t n) {
        // For easy reference.
        const auto& name = names[n];
        const auto& idx = idx_map_[name];
        const auto var_size = array_schema_->var_size(name);
        const auto type = array_schema_->type(name);

        // Compute null count.
        fragment_null_counts_[idx] = std::accumulate(
            tile_null_counts_[idx].begin(), tile_null_counts_[idx].end(), 0);

        if (var_size) {
          min_max_var(name);
        } else {
          // Switch depending on datatype.
          switch (type) {
            case Datatype::INT8:
              compute_fragment_min_max_sum<int8_t>(name);
              break;
            case Datatype::INT16:
              compute_fragment_min_max_sum<int16_t>(name);
              break;
            case Datatype::INT32:
              compute_fragment_min_max_sum<int32_t>(name);
              break;
            case Datatype::INT64:
              compute_fragment_min_max_sum<int64_t>(name);
              break;
            case Datatype::BOOL:
            case Datatype::UINT8:
              compute_fragment_min_max_sum<uint8_t>(name);
              break;
            case Datatype::UINT16:
              compute_fragment_min_max_sum<uint16_t>(name);
              break;
            case Datatype::UINT32:
              compute_fragment_min_max_sum<uint32_t>(name);
              break;
            case Datatype::UINT64:
              compute_fragment_min_max_sum<uint64_t>(name);
              break;
            case Datatype::FLOAT32:
              compute_fragment_min_max_sum<float>(name);
              break;
            case Datatype::FLOAT64:
              compute_fragment_min_max_sum<double>(name);
              break;
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
            case Datatype::TIME_HR:
            case Datatype::TIME_MIN:
            case Datatype::TIME_SEC:
            case Datatype::TIME_MS:
            case Datatype::TIME_US:
            case Datatype::TIME_NS:
            case Datatype::TIME_PS:
            case Datatype::TIME_FS:
            case Datatype::TIME_AS:
              compute_fragment_min_max_sum<int64_t>(name);
              break;
            case Datatype::STRING_ASCII:
            case Datatype::CHAR:
              compute_fragment_min_max_sum<char>(name);
              break;
            case Datatype::BLOB:
              compute_fragment_min_max_sum<std::byte>(name);
              break;
            default:
              break;
          }
        }

        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  return Status::Ok();
}

void FragmentMetadata::set_array_schema(
    const shared_ptr<const ArraySchema>& array_schema) {
  array_schema_ = array_schema;

  // Rebuild index mapping
  build_idx_map();
}

uint64_t FragmentMetadata::cell_num() const {
  auto tile_num = this->tile_num();
  assert(tile_num != 0);
  if (dense_) {  // Dense fragment
    return tile_num * array_schema_->domain().cell_num_per_tile();
  } else {  // Sparse fragment
    return (tile_num - 1) * array_schema_->capacity() + last_tile_cell_num();
  }
}

uint64_t FragmentMetadata::cell_num(uint64_t tile_pos) const {
  if (dense_)
    return array_schema_->domain().cell_num_per_tile();

  uint64_t tile_num = this->tile_num();
  if (tile_pos != tile_num - 1)
    return array_schema_->capacity();

  return last_tile_cell_num();
}

std::vector<Datatype> FragmentMetadata::dim_types() const {
  std::vector<Datatype> ret;
  for (uint32_t d = 0; d < array_schema_->dim_num(); d++) {
    ret.emplace_back(array_schema_->dimension_ptr(d)->type());
  }

  return ret;
}

Status FragmentMetadata::add_max_buffer_sizes(
    const EncryptionKey& encryption_key,
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Dense case
  if (dense_)
    return add_max_buffer_sizes_dense(subarray, buffer_sizes);

  // Convert subarray to NDRange
  auto dim_num = array_schema_->dim_num();
  auto sub_ptr = (const unsigned char*)subarray;
  NDRange sub_nd(dim_num);
  uint64_t offset = 0;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto r_size{2 * array_schema_->dimension_ptr(d)->coord_size()};
    sub_nd[d].set_range(&sub_ptr[offset], r_size);
    offset += r_size;
  }

  // Sparse case
  return add_max_buffer_sizes_sparse(encryption_key, sub_nd, buffer_sizes);
}

Status FragmentMetadata::add_max_buffer_sizes_dense(
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Note: applicable only to the dense case where all dimensions
  // have the same type
  auto type{array_schema_->dimension_ptr(0)->type()};
  switch (type) {
    case Datatype::INT32:
      return add_max_buffer_sizes_dense<int32_t>(
          static_cast<const int32_t*>(subarray), buffer_sizes);
    case Datatype::INT64:
      return add_max_buffer_sizes_dense<int64_t>(
          static_cast<const int64_t*>(subarray), buffer_sizes);
    case Datatype::FLOAT32:
      return add_max_buffer_sizes_dense<float>(
          static_cast<const float*>(subarray), buffer_sizes);
    case Datatype::FLOAT64:
      return add_max_buffer_sizes_dense<double>(
          static_cast<const double*>(subarray), buffer_sizes);
    case Datatype::INT8:
      return add_max_buffer_sizes_dense<int8_t>(
          static_cast<const int8_t*>(subarray), buffer_sizes);
    case Datatype::UINT8:
      return add_max_buffer_sizes_dense<uint8_t>(
          static_cast<const uint8_t*>(subarray), buffer_sizes);
    case Datatype::INT16:
      return add_max_buffer_sizes_dense<int16_t>(
          static_cast<const int16_t*>(subarray), buffer_sizes);
    case Datatype::UINT16:
      return add_max_buffer_sizes_dense<uint16_t>(
          static_cast<const uint16_t*>(subarray), buffer_sizes);
    case Datatype::UINT32:
      return add_max_buffer_sizes_dense<uint32_t>(
          static_cast<const uint32_t*>(subarray), buffer_sizes);
    case Datatype::UINT64:
      return add_max_buffer_sizes_dense<uint64_t>(
          static_cast<const uint64_t*>(subarray), buffer_sizes);
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return add_max_buffer_sizes_dense<int64_t>(
          static_cast<const int64_t*>(subarray), buffer_sizes);
    default:
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot compute add read buffer sizes for dense array; Unsupported "
          "domain type"));
  }

  return Status::Ok();
}

template <class T>
Status FragmentMetadata::add_max_buffer_sizes_dense(
    const T* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Calculate the ids of all tiles overlapping with subarray
  auto tids = compute_overlapping_tile_ids(subarray);

  // Compute buffer sizes
  for (auto& tid : tids) {
    for (auto& it : *buffer_sizes) {
      if (array_schema_->var_size(it.first)) {
        auto cell_num = this->cell_num(tid);
        it.second.first += cell_num * constants::cell_var_offset_size;
        auto&& [st, size] = tile_var_size(it.first, tid);
        RETURN_NOT_OK(st);
        it.second.second += *size;
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
  std::vector<bool> is_default(subarray.size(), false);
  auto tile_overlap = rtree_.get_tile_overlap(subarray, is_default);

  // Handle tile ranges
  for (const auto& tr : tile_overlap.tile_ranges_) {
    for (uint64_t tid = tr.first; tid <= tr.second; ++tid) {
      for (auto& it : *buffer_sizes) {
        if (array_schema_->var_size(it.first)) {
          auto cell_num = this->cell_num(tid);
          it.second.first += cell_num * constants::cell_var_offset_size;
          auto&& [st, size] = tile_var_size(it.first, tid);
          RETURN_NOT_OK(st);
          it.second.second += *size;
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
        auto&& [st, size] = tile_var_size(it.first, tid);
        it.second.second += *size;
        RETURN_NOT_OK(st);
      } else {
        it.second.first += cell_num(tid) * array_schema_->cell_size(it.first);
      }
    }
  }

  return Status::Ok();
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

Status FragmentMetadata::get_tile_overlap(
    const NDRange& range,
    std::vector<bool>& is_default,
    TileOverlap* tile_overlap) {
  assert(version_ <= 2 || loaded_metadata_.rtree_);
  *tile_overlap = rtree_.get_tile_overlap(range, is_default);
  return Status::Ok();
}

void FragmentMetadata::compute_tile_bitmap(
    const Range& range, unsigned d, std::vector<uint8_t>* tile_bitmap) {
  assert(version_ <= 2 || loaded_metadata_.rtree_);
  rtree_.compute_tile_bitmap(range, d, tile_bitmap);
}

Status FragmentMetadata::init_domain(const NDRange& non_empty_domain) {
  auto& domain{array_schema_->domain()};

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
    domain.crop_ndrange(&non_empty_domain_);

    // Set expanded domain
    domain_ = non_empty_domain_;
    domain.expand_to_tiles(&domain_);
  }

  return Status::Ok();
}

Status FragmentMetadata::init(const NDRange& non_empty_domain) {
  // For easy reference
  auto num = num_dims_and_attrs();

  RETURN_NOT_OK(init_domain(non_empty_domain));

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

  // Initialize tile min/max/sum/null count
  tile_min_buffer_.resize(num);
  tile_min_var_buffer_.resize(num);
  tile_max_buffer_.resize(num);
  tile_max_var_buffer_.resize(num);
  tile_sums_.resize(num);
  tile_null_counts_.resize(num);

  // Initialize fragment min/max/sum/null count
  fragment_mins_.resize(num);
  fragment_maxs_.resize(num);
  fragment_sums_.resize(num);
  fragment_null_counts_.resize(num);

  return Status::Ok();
}

Status FragmentMetadata::load(
    const EncryptionKey& encryption_key,
    Buffer* f_buff,
    uint64_t offset,
    std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas) {
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
    return load_v1_v2(encryption_key, array_schemas);
  return load_v3_or_higher(encryption_key, f_buff, offset, array_schemas);
}

void FragmentMetadata::store(const EncryptionKey& encryption_key) {
  auto timer_se =
      storage_manager_->stats()->start_timer("write_store_frag_meta");

  if (version_ < 7) {
    auto fragment_metadata_uri =
        fragment_uri_.join_path(constants::fragment_metadata_filename);
    throw std::logic_error(
        "FragmentMetadata::store(), unexpected version_ " +
        std::to_string(version_) + " storing " +
        fragment_metadata_uri.to_string());
  }
  try {
    if (version_ <= 10) {
      throw_if_not_ok(store_v7_v10(encryption_key));
    } else if (version_ == 11) {
      throw_if_not_ok(store_v11(encryption_key));
    } else if (version_ <= 14) {
      throw_if_not_ok(store_v12_v14(encryption_key));
    } else {
      throw_if_not_ok(store_v15_or_higher(encryption_key));
    }
    return;
  } catch (...) {
    clean_up();
    auto fragment_metadata_uri =
        fragment_uri_.join_path(constants::fragment_metadata_filename);
    std::throw_with_nested(FragmentMetadataStatusException(
        "FragmentMetadata::store() failed on " +
        fragment_metadata_uri.to_string()));
  }
}

Status FragmentMetadata::store_v7_v10(const EncryptionKey& encryption_key) {
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  auto num = num_dims_and_attrs();
  uint64_t offset = 0, nbytes;

  // Store R-Tree
  gt_offsets_.rtree_ = offset;
  throw_if_not_ok(store_rtree(encryption_key, &nbytes));
  offset += nbytes;

  // Store tile offsets
  gt_offsets_.tile_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_offsets_[i] = offset;
    store_tile_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_var_offsets_[i] = offset;
    store_tile_var_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_var_sizes_[i] = offset;
    store_tile_var_sizes(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store validity tile offsets
  gt_offsets_.tile_validity_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_validity_offsets_[i] = offset;
    throw_if_not_ok(store_tile_validity_offsets(i, encryption_key, &nbytes));
    offset += nbytes;
  }

  // Store footer
  throw_if_not_ok(store_footer(encryption_key));

  // Close file
  return storage_manager_->close_file(fragment_metadata_uri);
}

Status FragmentMetadata::store_v11(const EncryptionKey& encryption_key) {
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  auto num = num_dims_and_attrs();
  uint64_t offset = 0, nbytes;

  // Store R-Tree
  gt_offsets_.rtree_ = offset;
  RETURN_NOT_OK_ELSE(store_rtree(encryption_key, &nbytes), clean_up());
  offset += nbytes;

  // Store tile offsets
  gt_offsets_.tile_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_offsets_[i] = offset;
    store_tile_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_var_offsets_[i] = offset;
    store_tile_var_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_var_sizes_[i] = offset;
    store_tile_var_sizes(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store validity tile offsets
  gt_offsets_.tile_validity_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_validity_offsets_[i] = offset;
    RETURN_NOT_OK_ELSE(
        store_tile_validity_offsets(i, encryption_key, &nbytes), clean_up());
    offset += nbytes;
  }

  // Store mins
  gt_offsets_.tile_min_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_min_offsets_[i] = offset;
    store_tile_mins(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store maxs
  gt_offsets_.tile_max_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_max_offsets_[i] = offset;
    store_tile_maxs(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store sums
  gt_offsets_.tile_sum_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_sum_offsets_[i] = offset;
    store_tile_sums(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store null counts
  gt_offsets_.tile_null_count_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_null_count_offsets_[i] = offset;
    store_tile_null_counts(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store footer
  RETURN_NOT_OK_ELSE(store_footer(encryption_key), clean_up());

  // Close file
  return storage_manager_->close_file(fragment_metadata_uri);
}

Status FragmentMetadata::store_v12_v14(const EncryptionKey& encryption_key) {
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  auto num = num_dims_and_attrs();
  uint64_t offset = 0, nbytes;

  // Store R-Tree
  gt_offsets_.rtree_ = offset;
  throw_if_not_ok(store_rtree(encryption_key, &nbytes));
  offset += nbytes;

  // Store tile offsets
  gt_offsets_.tile_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_offsets_[i] = offset;
    store_tile_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_var_offsets_[i] = offset;
    store_tile_var_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_var_sizes_[i] = offset;
    store_tile_var_sizes(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store validity tile offsets
  gt_offsets_.tile_validity_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_validity_offsets_[i] = offset;
    throw_if_not_ok(store_tile_validity_offsets(i, encryption_key, &nbytes));
    offset += nbytes;
  }

  // Store mins
  gt_offsets_.tile_min_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_min_offsets_[i] = offset;
    store_tile_mins(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store maxs
  gt_offsets_.tile_max_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_max_offsets_[i] = offset;
    store_tile_maxs(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store sums
  gt_offsets_.tile_sum_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_sum_offsets_[i] = offset;
    store_tile_sums(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store null counts
  gt_offsets_.tile_null_count_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_null_count_offsets_[i] = offset;
    store_tile_null_counts(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store fragment min, max, sum and null count
  gt_offsets_.fragment_min_max_sum_null_count_offset_ = offset;
  store_fragment_min_max_sum_null_count(num, encryption_key, &nbytes);
  offset += nbytes;

  // Store footer
  throw_if_not_ok(store_footer(encryption_key));

  // Close file
  return storage_manager_->close_file(fragment_metadata_uri);
}

Status FragmentMetadata::store_v15_or_higher(
    const EncryptionKey& encryption_key) {
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  auto num = num_dims_and_attrs();
  uint64_t offset = 0, nbytes;

  // Store R-Tree
  gt_offsets_.rtree_ = offset;
  throw_if_not_ok(store_rtree(encryption_key, &nbytes));
  offset += nbytes;

  // Store tile offsets
  gt_offsets_.tile_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_offsets_[i] = offset;
    store_tile_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_var_offsets_[i] = offset;
    store_tile_var_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_var_sizes_[i] = offset;
    store_tile_var_sizes(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store validity tile offsets
  gt_offsets_.tile_validity_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_validity_offsets_[i] = offset;
    throw_if_not_ok(store_tile_validity_offsets(i, encryption_key, &nbytes));
    offset += nbytes;
  }

  // Store mins
  gt_offsets_.tile_min_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_min_offsets_[i] = offset;
    store_tile_mins(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store maxs
  gt_offsets_.tile_max_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_max_offsets_[i] = offset;
    store_tile_maxs(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store sums
  gt_offsets_.tile_sum_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_sum_offsets_[i] = offset;
    store_tile_sums(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store null counts
  gt_offsets_.tile_null_count_offsets_.resize(num);
  for (unsigned int i = 0; i < num; ++i) {
    gt_offsets_.tile_null_count_offsets_[i] = offset;
    store_tile_null_counts(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store fragment min, max, sum and null count
  gt_offsets_.fragment_min_max_sum_null_count_offset_ = offset;
  store_fragment_min_max_sum_null_count(num, encryption_key, &nbytes);
  offset += nbytes;

  // Store processed condition
  gt_offsets_.processed_conditions_offsets_ = offset;
  store_processed_conditions(encryption_key, &nbytes);
  offset += nbytes;

  // Store footer
  throw_if_not_ok(store_footer(encryption_key));

  // Close file
  return storage_manager_->close_file(fragment_metadata_uri);
}

Status FragmentMetadata::set_num_tiles(uint64_t num_tiles) {
  for (auto& it : idx_map_) {
    auto i = it.second;
    assert(num_tiles >= tile_offsets_[i].size());

    // Get the fixed cell size
    const auto is_dim = array_schema_->is_dim(it.first);
    const auto var_size = array_schema_->var_size(it.first);
    const auto cell_size = var_size ? constants::cell_var_offset_size :
                                      array_schema_->cell_size(it.first);

    tile_offsets_[i].resize(num_tiles, 0);
    tile_var_offsets_[i].resize(num_tiles, 0);
    tile_var_sizes_[i].resize(num_tiles, 0);
    tile_validity_offsets_[i].resize(num_tiles, 0);

    // No metadata for dense coords
    if (!array_schema_->dense() || !is_dim) {
      const auto type = array_schema_->type(it.first);
      const auto cell_val_num = array_schema_->cell_val_num(it.first);

      if (TileMetadataGenerator::has_min_max_metadata(
              type, is_dim, var_size, cell_val_num)) {
        tile_min_buffer_[i].resize(num_tiles * cell_size, 0);
        tile_max_buffer_[i].resize(num_tiles * cell_size, 0);
      }

      if (TileMetadataGenerator::has_sum_metadata(
              type, var_size, cell_val_num)) {
        if (!var_size)
          tile_sums_[i].resize(num_tiles * sizeof(uint64_t), 0);
      }

      if (array_schema_->is_nullable(it.first))
        tile_null_counts_[i].resize(num_tiles, 0);
    }
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

uint64_t FragmentMetadata::tile_num() const {
  if (dense_) {
    return array_schema_->domain().tile_num(domain_);
  }

  return sparse_tile_num_;
}

tuple<Status, optional<std::string>> FragmentMetadata::encode_name(
    const std::string& name) const {
  if (version_ <= 7)
    return {Status::Ok(), name};

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

    return {Status::Ok(), percent_encoded_name.str()};
  }

  assert(version_ > 8);
  const auto iter = idx_map_.find(name);
  if (iter == idx_map_.end())
    return {Status_FragmentMetadataError("Name " + name + " not in idx_map_"),
            std::nullopt};

  const unsigned idx = iter->second;

  auto attributes = array_schema_->attributes();
  for (unsigned i = 0; i < attributes.size(); ++i) {
    const std::string attr_name = attributes[i]->name();
    if (attr_name == name) {
      return {Status::Ok(), "a" + std::to_string(idx)};
    }
  }

  for (unsigned i = 0; i < array_schema_->dim_num(); ++i) {
    const auto& dim_name{array_schema_->dimension_ptr(i)->name()};
    if (dim_name == name) {
      const unsigned dim_idx = idx - array_schema_->attribute_num() - 1;
      return {Status::Ok(), "d" + std::to_string(dim_idx)};
    }
  }

  if (name == constants::coords) {
    return {Status::Ok(), name};
  }

  if (name == constants::timestamps) {
    return {Status::Ok(), "t"};
  }

  if (name == constants::delete_timestamps) {
    return {Status::Ok(), "dt"};
  }

  if (name == constants::delete_condition_index) {
    return {Status::Ok(), "dci"};
  }

  auto err = "Unable to locate dimension/attribute " + name;
  return {Status_FragmentMetadataError(err), std::nullopt};
}

tuple<Status, optional<URI>> FragmentMetadata::uri(
    const std::string& name) const {
  auto&& [st, encoded_name] = encode_name(name);
  if (!st.ok())
    return {st, std::nullopt};

  return {st, fragment_uri_.join_path(*encoded_name + constants::file_suffix)};
}

tuple<Status, optional<URI>> FragmentMetadata::var_uri(
    const std::string& name) const {
  auto&& [st, encoded_name] = encode_name(name);
  if (!st.ok())
    return {st, std::nullopt};

  return {
      st,
      fragment_uri_.join_path(*encoded_name + "_var" + constants::file_suffix)};
}

tuple<Status, optional<URI>> FragmentMetadata::validity_uri(
    const std::string& name) const {
  auto&& [st, encoded_name] = encode_name(name);
  if (!st.ok())
    return {st, std::nullopt};

  return {st,
          fragment_uri_.join_path(
              *encoded_name + "_validity" + constants::file_suffix)};
}

const std::string& FragmentMetadata::array_schema_name() {
  return array_schema_name_;
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

Status FragmentMetadata::load_tile_min_values(
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

  // Load all the min values.
  for (const auto& name : names) {
    RETURN_NOT_OK(load_tile_min_values(encryption_key, idx_map_[name]));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_tile_max_values(
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

  // Load all the max values.
  for (const auto& name : names) {
    RETURN_NOT_OK(load_tile_max_values(encryption_key, idx_map_[name]));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_tile_sum_values(
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

  // Load all the sum values.
  for (const auto& name : names) {
    RETURN_NOT_OK(load_tile_sum_values(encryption_key, idx_map_[name]));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_tile_null_count_values(
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

  // Load all the null count values.
  for (const auto& name : names) {
    RETURN_NOT_OK(load_tile_null_count_values(encryption_key, idx_map_[name]));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_fragment_min_max_sum_null_count(
    const EncryptionKey& encryption_key) {
  if (loaded_metadata_.fragment_min_max_sum_null_count_)
    return Status::Ok();

  if (version_ <= 11)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  auto&& [st, tile_opt] = read_generic_tile_from_file(
      encryption_key, gt_offsets_.fragment_min_max_sum_null_count_offset_);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter(
      "read_fragment_min_max_sum_null_count_size", tile.size());

  Deserializer deserializer(tile.data(), tile.size());
  load_fragment_min_max_sum_null_count(deserializer);

  loaded_metadata_.fragment_min_max_sum_null_count_ = true;

  return Status::Ok();
}

Status FragmentMetadata::load_processed_conditions(
    const EncryptionKey& encryption_key) {
  if (loaded_metadata_.processed_conditions_) {
    return Status::Ok();
  }

  if (version_ <= 15) {
    return Status::Ok();
  }

  std::lock_guard<std::mutex> lock(mtx_);

  auto&& [st, tile_opt] = read_generic_tile_from_file(
      encryption_key, gt_offsets_.processed_conditions_offsets_);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter(
      "read_processed_conditions_size", tile.size());

  Deserializer deserializer(tile.data(), tile.size());
  load_processed_conditions(deserializer);

  loaded_metadata_.processed_conditions_ = true;

  return Status::Ok();
}

Status FragmentMetadata::file_offset(
    const std::string& name, uint64_t tile_idx, uint64_t* offset) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_offsets_[idx]) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Trying to access metadata that's not loaded"));
  }

  *offset = tile_offsets_[idx][tile_idx];
  return Status::Ok();
}

Status FragmentMetadata::file_var_offset(
    const std::string& name, uint64_t tile_idx, uint64_t* offset) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_var_offsets_[idx]) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Trying to access metadata that's not loaded"));
  }

  *offset = tile_var_offsets_[idx][tile_idx];
  return Status::Ok();
}

Status FragmentMetadata::file_validity_offset(
    const std::string& name, uint64_t tile_idx, uint64_t* offset) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_validity_offsets_[idx]) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Trying to access metadata that's not loaded"));
  }

  *offset = tile_validity_offsets_[idx][tile_idx];
  return Status::Ok();
}

const NDRange& FragmentMetadata::mbr(uint64_t tile_idx) const {
  return rtree_.leaf(tile_idx);
}

const std::vector<NDRange>& FragmentMetadata::mbrs() const {
  return rtree_.leaves();
}

tuple<Status, optional<uint64_t>> FragmentMetadata::persisted_tile_size(
    const std::string& name, uint64_t tile_idx) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_offsets_[idx]) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt};
  }

  auto tile_num = this->tile_num();

  auto tile_size =
      (tile_idx != tile_num - 1) ?
          tile_offsets_[idx][tile_idx + 1] - tile_offsets_[idx][tile_idx] :
          file_sizes_[idx] - tile_offsets_[idx][tile_idx];

  return {Status::Ok(), tile_size};
}

tuple<Status, optional<uint64_t>> FragmentMetadata::persisted_tile_var_size(
    const std::string& name, uint64_t tile_idx) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;

  if (!loaded_metadata_.tile_var_offsets_[idx]) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt};
  }

  auto tile_num = this->tile_num();

  auto tile_size = (tile_idx != tile_num - 1) ?
                       tile_var_offsets_[idx][tile_idx + 1] -
                           tile_var_offsets_[idx][tile_idx] :
                       file_var_sizes_[idx] - tile_var_offsets_[idx][tile_idx];

  return {Status::Ok(), tile_size};
}

tuple<Status, optional<uint64_t>>
FragmentMetadata::persisted_tile_validity_size(
    const std::string& name, uint64_t tile_idx) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_validity_offsets_[idx]) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt};
  }

  auto tile_num = this->tile_num();

  auto tile_size =
      (tile_idx != tile_num - 1) ?
          tile_validity_offsets_[idx][tile_idx + 1] -
              tile_validity_offsets_[idx][tile_idx] :
          file_validity_sizes_[idx] - tile_validity_offsets_[idx][tile_idx];

  return {Status::Ok(), tile_size};
}

uint64_t FragmentMetadata::tile_size(
    const std::string& name, uint64_t tile_idx) const {
  auto var_size = array_schema_->var_size(name);
  auto cell_num = this->cell_num(tile_idx);
  return (var_size) ? cell_num * constants::cell_var_offset_size :
                      cell_num * array_schema_->cell_size(name);
}

tuple<Status, optional<uint64_t>> FragmentMetadata::tile_var_size(
    const std::string& name, uint64_t tile_idx) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_var_sizes_[idx]) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt};
  }

  auto tile_size = tile_var_sizes_[idx][tile_idx];
  return {Status::Ok(), tile_size};
}

tuple<Status, optional<void*>, optional<uint64_t>>
FragmentMetadata::get_tile_min(const std::string& name, uint64_t tile_idx) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_min_[idx]) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt,
            nullopt};
  }

  const auto type = array_schema_->type(name);
  const auto is_dim = array_schema_->is_dim(name);
  const auto var_size = array_schema_->var_size(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    return {Status_FragmentMetadataError(
                "Trying to access metadata that's not present"),
            nullopt,
            nullopt};
  }

  if (var_size) {
    auto tile_num = this->tile_num();
    auto offsets = (uint64_t*)tile_min_buffer_[idx].data();
    auto min_offset = offsets[tile_idx];
    auto size = tile_idx == tile_num - 1 ?
                    tile_min_var_buffer_[idx].size() - min_offset :
                    offsets[tile_idx + 1] - min_offset;
    void* min = &tile_min_var_buffer_[idx][min_offset];
    return {Status::Ok(), min, size};
  } else {
    auto size = array_schema_->cell_size(name);
    void* min = &tile_min_buffer_[idx][tile_idx * size];
    return {Status::Ok(), min, size};
  }
}

tuple<Status, optional<void*>, optional<uint64_t>>
FragmentMetadata::get_tile_max(const std::string& name, uint64_t tile_idx) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_max_[idx]) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt,
            nullopt};
  }

  const auto type = array_schema_->type(name);
  const auto is_dim = array_schema_->is_dim(name);
  const auto var_size = array_schema_->var_size(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    return {Status_FragmentMetadataError(
                "Trying to access metadata that's not present"),
            nullopt,
            nullopt};
  }

  if (var_size) {
    auto tile_num = this->tile_num();
    auto offsets = (uint64_t*)tile_max_buffer_[idx].data();
    auto max_offset = offsets[tile_idx];
    auto size = tile_idx == tile_num - 1 ?
                    tile_max_var_buffer_[idx].size() - max_offset :
                    offsets[tile_idx + 1] - max_offset;
    void* max = &tile_max_var_buffer_[idx][max_offset];
    return {Status::Ok(), max, size};
  } else {
    auto size = array_schema_->cell_size(name);
    void* max = &tile_max_buffer_[idx][tile_idx * size];
    return {Status::Ok(), max, size};
  }
}

tuple<Status, optional<void*>> FragmentMetadata::get_tile_sum(
    const std::string& name, uint64_t tile_idx) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_sum_[idx]) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt};
  }

  auto type = array_schema_->type(name);
  auto var_size = array_schema_->var_size(name);
  auto cell_val_num = array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_sum_metadata(type, var_size, cell_val_num)) {
    return {Status_FragmentMetadataError(
                "Trying to access metadata that's not present"),
            nullopt};
  }

  void* sum = &tile_sums_[idx][tile_idx * sizeof(uint64_t)];
  return {Status::Ok(), sum};
}

tuple<Status, optional<uint64_t>> FragmentMetadata::get_tile_null_count(
    const std::string& name, uint64_t tile_idx) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_null_count_[idx]) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt};
  }

  if (!array_schema_->is_nullable(name)) {
    return {Status_FragmentMetadataError(
                "Trying to access metadata that's not present"),
            nullopt};
  }

  uint64_t null_count = tile_null_counts_[idx][tile_idx];
  return {Status::Ok(), null_count};
}

tuple<Status, optional<std::vector<uint8_t>>> FragmentMetadata::get_min(
    const std::string& name) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.fragment_min_max_sum_null_count_) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt};
  }

  const auto type = array_schema_->type(name);
  const auto is_dim = array_schema_->is_dim(name);
  const auto var_size = array_schema_->var_size(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    return {Status_FragmentMetadataError(
                "Trying to access metadata that's not present"),
            nullopt};
  }

  return {Status::Ok(), fragment_mins_[idx]};
}

tuple<Status, optional<std::vector<uint8_t>>> FragmentMetadata::get_max(
    const std::string& name) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.fragment_min_max_sum_null_count_) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt};
  }

  const auto type = array_schema_->type(name);
  const auto is_dim = array_schema_->is_dim(name);
  const auto var_size = array_schema_->var_size(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    return {Status_FragmentMetadataError(
                "Trying to access metadata that's not present"),
            nullopt};
  }

  return {Status::Ok(), fragment_maxs_[idx]};
}

tuple<Status, optional<void*>> FragmentMetadata::get_sum(
    const std::string& name) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.fragment_min_max_sum_null_count_) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt};
  }

  const auto type = array_schema_->type(name);
  const auto var_size = array_schema_->var_size(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_sum_metadata(type, var_size, cell_val_num)) {
    return {Status_FragmentMetadataError(
                "Trying to access metadata that's not present"),
            nullopt};
  }

  return {Status::Ok(), &fragment_sums_[idx]};
}

tuple<Status, optional<uint64_t>> FragmentMetadata::get_null_count(
    const std::string& name) {
  auto it = idx_map_.find(name);
  assert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.fragment_min_max_sum_null_count_) {
    return {LOG_STATUS(Status_FragmentMetadataError(
                "Trying to access metadata that's not loaded")),
            nullopt};
  }

  if (!array_schema_->is_nullable(name)) {
    return {Status_FragmentMetadataError(
                "Trying to access metadata that's not present"),
            nullopt};
  }

  return {Status::Ok(), fragment_null_counts_[idx]};
}

void FragmentMetadata::set_processed_conditions(
    std::vector<std::string>& processed_conditions) {
  processed_conditions_ = processed_conditions;
  processed_conditions_set_ = std::unordered_set<std::string>(
      processed_conditions.begin(), processed_conditions.end());
}

std::vector<std::string>& FragmentMetadata::get_processed_conditions() {
  if (!loaded_metadata_.processed_conditions_) {
    throw std::logic_error("Trying to access metadata that's not present");
  }

  return processed_conditions_;
}

std::unordered_set<std::string>&
FragmentMetadata::get_processed_conditions_set() {
  if (!loaded_metadata_.processed_conditions_) {
    throw std::logic_error("Trying to access metadata that's not present");
  }

  return processed_conditions_set_;
}

uint64_t FragmentMetadata::first_timestamp() const {
  return timestamp_range_.first;
}

bool FragmentMetadata::operator<(const FragmentMetadata& metadata) const {
  return (timestamp_range_.first < metadata.timestamp_range_.first) ||
         (timestamp_range_.first == metadata.timestamp_range_.first &&
          fragment_uri_ < metadata.fragment_uri_);
}

Status FragmentMetadata::write_footer(Buffer* buff) const {
  RETURN_NOT_OK(write_version(buff));
  if (version_ >= 10) {
    RETURN_NOT_OK(write_array_schema_name(buff));
  }
  RETURN_NOT_OK(write_dense(buff));
  RETURN_NOT_OK(write_non_empty_domain(buff));
  RETURN_NOT_OK(write_sparse_tile_num(buff));
  RETURN_NOT_OK(write_last_tile_cell_num(buff));

  if (version_ >= 14) {
    RETURN_NOT_OK(write_has_timestamps(buff));
  }

  if (version_ >= 15) {
    RETURN_NOT_OK(write_has_delete_meta(buff));
  }

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

  auto&& [st, tile_opt] =
      read_generic_tile_from_file(encryption_key, gt_offsets_.rtree_);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter("read_rtree_size", tile.size());

  // Use the serialized buffer size to approximate memory usage of the rtree.
  if (memory_tracker_ != nullptr &&
      !memory_tracker_->take_memory(tile.size())) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot load R-tree; Insufficient memory budget; Needed " +
        std::to_string(tile.size()) + " but only had " +
        std::to_string(memory_tracker_->get_memory_available()) +
        " from budget " +
        std::to_string(memory_tracker_->get_memory_budget())));
  }

  Deserializer deserializer(tile.data(), tile.size());
  rtree_.deserialize(deserializer, &array_schema_->domain(), version_);

  loaded_metadata_.rtree_ = true;

  return Status::Ok();
}

void FragmentMetadata::free_rtree() {
  auto freed = rtree_.free_memory();
  if (memory_tracker_ != nullptr)
    memory_tracker_->release_memory(freed);
  loaded_metadata_.rtree_ = false;
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
  } else if (version < 11) {
    *size = footer_size_v7_v10();
  } else {
    *size = footer_size_v11_or_higher();
  }

  return Status::Ok();
}

uint64_t FragmentMetadata::footer_size() const {
  return footer_size_;
}

Status FragmentMetadata::get_footer_offset_and_size(
    uint64_t* offset, uint64_t* size) const {
  uint32_t f_version;
  auto name = fragment_uri_.remove_trailing_slash().last_path_part();
  RETURN_NOT_OK(utils::parse::get_fragment_name_version(name, &f_version));
  if (array_schema_->domain().all_dims_fixed() && f_version < 5) {
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
    storage_manager_->stats()->add_counter(
        "read_frag_meta_size", sizeof(uint64_t));
  }

  return Status::Ok();
}

uint64_t FragmentMetadata::footer_size_v3_v4() const {
  auto attribute_num = array_schema_->attribute_num();
  auto dim_num = array_schema_->dim_num();
  // v3 and v4 support only arrays where all dimensions have the same type
  auto domain_size{2 * dim_num * array_schema_->dimension_ptr(0)->coord_size()};

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
  auto num = num_dims_and_attrs();
  size_t domain_size = 0;

  if (non_empty_domain_.empty()) {
    // For var-sized dimensions, this function would be called only upon
    // writing the footer to storage, in which case the non-empty domain
    // would not be empty. For reading the footer from storage, the footer
    // size is explicitly stored to and retrieved from storage, so this
    // function is not called then.
    assert(array_schema_->domain().all_dims_fixed());
    for (unsigned d = 0; d < dim_num; ++d)
      domain_size += 2 * array_schema_->domain().dimension_ptr(d)->coord_size();
  } else {
    for (unsigned d = 0; d < dim_num; ++d) {
      domain_size += non_empty_domain_[d].size();
      if (array_schema_->dimension_ptr(d)->var_size()) {
        domain_size += 2 * sizeof(uint64_t);  // Two more sizes get serialized}
      }
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

uint64_t FragmentMetadata::footer_size_v7_v10() const {
  auto dim_num = array_schema_->dim_num();
  auto num = num_dims_and_attrs();
  uint64_t domain_size = 0;

  if (non_empty_domain_.empty()) {
    // For var-sized dimensions, this function would be called only upon
    // writing the footer to storage, in which case the non-empty domain
    // would not be empty. For reading the footer from storage, the footer
    // size is explicitly stored to and retrieved from storage, so this
    // function is not called then.
    assert(array_schema_->domain().all_dims_fixed());
    for (unsigned d = 0; d < dim_num; ++d)
      domain_size += 2 * array_schema_->domain().dimension_ptr(d)->coord_size();
  } else {
    for (unsigned d = 0; d < dim_num; ++d) {
      domain_size += non_empty_domain_[d].size();
      if (array_schema_->dimension_ptr(d)->var_size()) {
        domain_size += 2 * sizeof(uint64_t);  // Two more sizes get serialized}
      }
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

uint64_t FragmentMetadata::footer_size_v11_or_higher() const {
  auto dim_num = array_schema_->dim_num();
  auto num = num_dims_and_attrs();
  uint64_t domain_size = 0;

  if (non_empty_domain_.empty()) {
    // For var-sized dimensions, this function would be called only upon
    // writing the footer to storage, in which case the non-empty domain
    // would not be empty. For reading the footer from storage, the footer
    // size is explicitly stored to and retrieved from storage, so this
    // function is not called then.
    assert(array_schema_->domain().all_dims_fixed());
    for (unsigned d = 0; d < dim_num; ++d)
      domain_size += 2 * array_schema_->domain().dimension_ptr(d)->coord_size();
  } else {
    for (unsigned d = 0; d < dim_num; ++d) {
      domain_size += non_empty_domain_[d].size();
      if (array_schema_->dimension_ptr(d)->var_size()) {
        domain_size += 2 * sizeof(uint64_t);  // Two more sizes get serialized
      }
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
  size += num * sizeof(uint64_t);  // tile mins sizes
  size += num * sizeof(uint64_t);  // tile maxs sizes
  size += num * sizeof(uint64_t);  // tile sums sizes
  size += num * sizeof(uint64_t);  // tile null count sizes

  return size;
}

template <class T>
std::vector<uint64_t> FragmentMetadata::compute_overlapping_tile_ids(
    const T* subarray) const {
  assert(dense_);
  std::vector<uint64_t> tids;
  auto dim_num = array_schema_->dim_num();

  // Temporary domain vector
  auto coord_size{array_schema_->domain().dimension_ptr(0)->coord_size()};
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
  auto& domain{array_schema_->domain()};
  uint64_t tile_pos;
  do {
    tile_pos = domain.get_tile_pos(metadata_domain, tile_coords);
    tids.emplace_back(tile_pos);
    domain.get_next_tile_coords(subarray_tile_domain, tile_coords);
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
  auto coord_size{array_schema_->domain().dimension_ptr(0)->coord_size()};
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
  auto& domain{array_schema_->domain()};
  uint64_t tile_pos;
  do {
    domain.get_tile_subarray(metadata_domain, tile_coords, tile_subarray);
    utils::geometry::overlap(
        subarray, tile_subarray, dim_num, tile_overlap, &overlap);
    assert(overlap);
    cov = utils::geometry::coverage(tile_overlap, tile_subarray, dim_num);
    tile_pos = domain.get_tile_pos(metadata_domain, tile_coords);
    tids.emplace_back(tile_pos, cov);
    domain.get_next_tile_coords(subarray_tile_domain, tile_coords);
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
    auto tile_extent = *(const T*)array_schema_->domain().tile_extent(d).data();
    auto overlap = std::max(subarray[2 * d], domain[0]);
    subarray_tile_domain[2 * d] =
        Dimension::tile_idx(overlap, domain[0], tile_extent);

    overlap = std::min(subarray[2 * d + 1], domain[1]);
    subarray_tile_domain[2 * d + 1] =
        Dimension::tile_idx(overlap, domain[0], tile_extent);
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
  array_schema_->domain().expand_ndrange(mbr, &non_empty_domain_);

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

  auto&& [st, tile_opt] = read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_offsets_[idx]);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter("read_tile_offsets_size", tile.size());

  Deserializer deserializer(tile.data(), tile.size());
  load_tile_offsets(idx, deserializer);

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

  auto&& [st, tile_opt] = read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_var_offsets_[idx]);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter(
      "read_tile_var_offsets_size", tile.size());

  Deserializer deserializer(tile.data(), tile.size());
  load_tile_var_offsets(idx, deserializer);

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

  auto&& [st, tile_opt] = read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_var_sizes_[idx]);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter(
      "read_tile_var_sizes_size", tile.size());

  Deserializer deserializer(tile.data(), tile.size());
  load_tile_var_sizes(idx, deserializer);

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

  auto&& [st, tile_opt] = read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_validity_offsets_[idx]);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter(
      "read_tile_validity_offsets_size", tile.size());

  ConstBuffer cbuff(tile.data(), tile.size());
  RETURN_NOT_OK(load_tile_validity_offsets(idx, &cbuff));

  loaded_metadata_.tile_validity_offsets_[idx] = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_min_values(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (version_ <= 10)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_min_[idx])
    return Status::Ok();

  auto&& [st, tile_opt] = read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_min_offsets_[idx]);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter("read_tile_min_size", tile.size());

  Deserializer deserializer(tile.data(), tile.size());
  load_tile_min_values(idx, deserializer);

  loaded_metadata_.tile_min_[idx] = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_max_values(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (version_ <= 10)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_max_[idx])
    return Status::Ok();

  auto&& [st, tile_opt] = read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_max_offsets_[idx]);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter("read_tile_max_size", tile.size());

  Deserializer deserializer(tile.data(), tile.size());
  load_tile_max_values(idx, deserializer);

  loaded_metadata_.tile_max_[idx] = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_sum_values(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (version_ <= 10)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_sum_[idx])
    return Status::Ok();

  auto&& [st, tile_opt] = read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_sum_offsets_[idx]);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter("read_tile_sum_size", tile.size());

  Deserializer deserializer(tile.data(), tile.size());
  load_tile_sum_values(idx, deserializer);

  loaded_metadata_.tile_sum_[idx] = true;

  return Status::Ok();
}

Status FragmentMetadata::load_tile_null_count_values(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (version_ <= 10)
    return Status::Ok();

  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.tile_null_count_[idx])
    return Status::Ok();

  auto&& [st, tile_opt] = read_generic_tile_from_file(
      encryption_key, gt_offsets_.tile_null_count_offsets_[idx]);
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter(
      "read_tile_null_count_size", tile.size());

  Deserializer deserializer(tile.data(), tile.size());
  load_tile_null_count_values(idx, deserializer);

  loaded_metadata_.tile_null_count_[idx] = true;

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
  auto coord_size{array_schema_->domain().dimension_ptr(0)->coord_size()};
  auto dim_num = array_schema_->domain().dim_num();
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
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot load fragment metadata; Reading tile offsets failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// file_sizes#0 (uint64_t)
// ...
// file_sizes#{attribute_num+dim_num} (uint64_t)
Status FragmentMetadata::load_file_sizes_v5_or_higher(ConstBuffer* buff) {
  auto num = num_dims_and_attrs();
  file_sizes_.resize(num);
  Status st = buff->read(&file_sizes_[0], num * sizeof(uint64_t));

  if (!st.ok()) {
    return LOG_STATUS(Status_FragmentMetadataError(
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
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot load fragment metadata; Reading tile offsets failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// file_var_sizes#0 (uint64_t)
// ...
// file_var_sizes#{attribute_num+dim_num} (uint64_t)
Status FragmentMetadata::load_file_var_sizes_v5_or_higher(ConstBuffer* buff) {
  auto num = num_dims_and_attrs();
  file_var_sizes_.resize(num);
  Status st = buff->read(&file_var_sizes_[0], num * sizeof(uint64_t));

  if (!st.ok()) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot load fragment metadata; Reading tile offsets failed"));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_file_validity_sizes(ConstBuffer* buff) {
  if (version_ <= 6)
    return Status::Ok();

  auto num = num_dims_and_attrs();
  file_validity_sizes_.resize(num);
  Status st = buff->read(&file_validity_sizes_[0], num * sizeof(uint64_t));

  if (!st.ok()) {
    return LOG_STATUS(Status_FragmentMetadataError(
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
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot load fragment metadata; Reading last tile cell number "
        "failed"));
  }
  return Status::Ok();
}

// ===== FORMAT =====
// has_timestamps (char)
Status FragmentMetadata::load_has_timestamps(ConstBuffer* buff) {
  // Get includes timestamps
  Status st = buff->read(&has_timestamps_, sizeof(char));
  if (!st.ok()) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot load fragment metadata; Reading include timestamps failed"));
  }

  // Rebuild index map
  if (has_timestamps_) {
    build_idx_map();
  }
  return Status::Ok();
}

// ===== FORMAT =====
// has_delete_meta (char)
Status FragmentMetadata::load_has_delete_meta(ConstBuffer* buff) {
  // Get includes timestamps
  Status st = buff->read(&has_delete_meta_, sizeof(char));
  if (!st.ok()) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot load fragment metadata; Reading include delete meta failed"));
  }

  // Rebuild index map
  if (has_delete_meta_) {
    build_idx_map();
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
  auto& domain{array_schema_->domain()};
  auto dim_num = domain.dim_num();
  for (uint64_t m = 0; m < mbr_num; ++m) {
    NDRange mbr(dim_num);
    for (unsigned d = 0; d < dim_num; ++d) {
      auto r_size{2 * domain.dimension_ptr(d)->coord_size()};
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
      auto coord_size{array_schema_->dimension_ptr(d)->coord_size()};
      Range r(&temp[offset], 2 * coord_size);
      non_empty_domain_[d] = std::move(r);
      offset += 2 * coord_size;
    }
  }

  // Get expanded domain
  if (!non_empty_domain_.empty()) {
    domain_ = non_empty_domain_;
    array_schema_->domain().expand_to_tiles(&domain_);
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
    auto coord_size_0{array_schema_->domain().dimension_ptr(0)->coord_size()};
    auto domain_size = 2 * dim_num * coord_size_0;
    std::vector<uint8_t> temp(domain_size);
    RETURN_NOT_OK(buff->read(&temp[0], domain_size));
    non_empty_domain_.resize(dim_num);
    uint64_t offset = 0;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto coord_size{array_schema_->dimension_ptr(d)->coord_size()};
      Range r(&temp[offset], 2 * coord_size);
      non_empty_domain_[d] = std::move(r);
      offset += 2 * coord_size;
    }
  }

  // Get expanded domain
  if (!non_empty_domain_.empty()) {
    domain_ = non_empty_domain_;
    array_schema_->domain().expand_to_tiles(&domain_);
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

  auto& domain{array_schema_->domain()};
  if (null_non_empty_domain == 0) {
    auto dim_num = array_schema_->dim_num();
    non_empty_domain_.resize(dim_num);
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim{domain.dimension_ptr(d)};
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
    array_schema_->domain().expand_to_tiles(&domain_);
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
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load fragment metadata; Reading number of tile offsets "
          "failed"));
    }

    if (tile_offsets_num == 0)
      continue;

    auto size = tile_offsets_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load tile offsets; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget())));
    }

    // Get tile offsets
    tile_offsets_[i].resize(tile_offsets_num);
    st = buff->read(&tile_offsets_[i][0], size);
    if (!st.ok()) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load fragment metadata; Reading tile offsets failed"));
    }
  }

  loaded_metadata_.tile_offsets_.resize(
      array_schema_->attribute_num() + 1, true);

  return Status::Ok();
}

void FragmentMetadata::load_tile_offsets(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_offsets_num = 0;

  // Get number of tile offsets
  tile_offsets_num = deserializer.read<uint64_t>();

  // Get tile offsets
  if (tile_offsets_num != 0) {
    auto size = tile_offsets_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      throw FragmentMetadataStatusException(
          "Cannot load tile offsets; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget()));
    }

    tile_offsets_[idx].resize(tile_offsets_num);
    deserializer.read(&tile_offsets_[idx][0], size);
  }
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
      LOG_STATUS(st);
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load fragment metadata; Reading number of variable tile "
          "offsets failed"));
    }

    if (tile_var_offsets_num == 0)
      continue;

    auto size = tile_var_offsets_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load tile var offsets; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget())));
    }

    // Get variable tile offsets
    tile_var_offsets_[i].resize(tile_var_offsets_num);
    st = buff->read(&tile_var_offsets_[i][0], size);
    if (!st.ok()) {
      LOG_STATUS(st);
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load fragment metadata; Reading variable tile offsets "
          "failed"));
    }
  }

  loaded_metadata_.tile_var_offsets_.resize(
      array_schema_->attribute_num(), true);

  return Status::Ok();
}

void FragmentMetadata::load_tile_var_offsets(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_var_offsets_num = 0;

  // Get number of tile offsets
  tile_var_offsets_num = deserializer.read<uint64_t>();

  // Get variable tile offsets
  if (tile_var_offsets_num != 0) {
    auto size = tile_var_offsets_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      throw FragmentMetadataStatusException(
          "Cannot load tile var offsets; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget()));
    }

    tile_var_offsets_[idx].resize(tile_var_offsets_num);
    deserializer.read(&tile_var_offsets_[idx][0], size);
  }
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
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load fragment metadata; Reading number of variable tile "
          "sizes failed"));
    }

    if (tile_var_sizes_num == 0)
      continue;

    auto size = tile_var_sizes_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load tile var sizes; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget())));
    }

    // Get variable tile sizes
    tile_var_sizes_[i].resize(tile_var_sizes_num);
    st = buff->read(&tile_var_sizes_[i][0], size);
    if (!st.ok()) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load fragment metadata; Reading variable tile sizes "
          "failed"));
    }
  }

  loaded_metadata_.tile_var_sizes_.resize(array_schema_->attribute_num(), true);

  return Status::Ok();
}

void FragmentMetadata::load_tile_var_sizes(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_var_sizes_num = 0;

  // Get number of tile sizes
  tile_var_sizes_num = deserializer.read<uint64_t>();

  // Get variable tile sizes
  if (tile_var_sizes_num != 0) {
    auto size = tile_var_sizes_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      throw FragmentMetadataStatusException(
          "Cannot load tile var sizes; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget()));
    }

    tile_var_sizes_[idx].resize(tile_var_sizes_num);
    deserializer.read(&tile_var_sizes_[idx][0], size);
  }
}

Status FragmentMetadata::load_tile_validity_offsets(
    unsigned idx, ConstBuffer* buff) {
  Status st;
  uint64_t tile_validity_offsets_num = 0;

  // Get number of tile offsets
  st = buff->read(&tile_validity_offsets_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(
        Status_FragmentMetadataError("Cannot load fragment metadata; Reading "
                                     "number of validity tile offsets "
                                     "failed"));
  }

  // Get tile offsets
  if (tile_validity_offsets_num != 0) {
    auto size = tile_validity_offsets_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load tile validity offsets; Insufficient memory budget; "
          "Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget())));
    }

    tile_validity_offsets_[idx].resize(tile_validity_offsets_num);
    st = buff->read(&tile_validity_offsets_[idx][0], size);

    if (!st.ok()) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot load fragment metadata; Reading validity tile offsets "
          "failed"));
    }
  }

  return Status::Ok();
}

// ===== FORMAT =====
// tile_min_values#0_size_buffer (uint64_t)
// tile_min_values#0_size_buffer_var (uint64_t)
// tile_min_values#0_buffer
// tile_min_values#0_buffer_var
// ...
// tile_min_values#<attribute_num-1>_size_buffer (uint64_t)
// tile_min_values#<attribute_num-1>_size_buffer_var (uint64_t)
// tile_min_values#<attribute_num-1>_buffer
// tile_min_values#<attribute_num-1>_buffer_var
void FragmentMetadata::load_tile_min_values(
    unsigned idx, Deserializer& deserializer) {
  Status st;
  uint64_t buffer_size = 0;
  uint64_t var_buffer_size = 0;

  // Get buffer size
  buffer_size = deserializer.read<uint64_t>();

  // Get var buffer size
  var_buffer_size = deserializer.read<uint64_t>();

  // Get tile mins
  if (buffer_size != 0) {
    auto size = buffer_size + var_buffer_size;
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      throw StatusException(Status_FragmentMetadataError(
          "Cannot load min values; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget())));
    }

    tile_min_buffer_[idx].resize(buffer_size);
    deserializer.read(&tile_min_buffer_[idx][0], buffer_size);

    if (var_buffer_size) {
      tile_min_var_buffer_[idx].resize(var_buffer_size);
      deserializer.read(&tile_min_var_buffer_[idx][0], var_buffer_size);
    }
  }
}

// ===== FORMAT =====
// tile_max_values#0_size_buffer (uint64_t)
// tile_max_values#0_size_buffer_var (uint64_t)
// tile_max_values#0_buffer
// tile_max_values#0_buffer_var
// ...
// tile_max_values#<attribute_num-1>_size_buffer (uint64_t)
// tile_max_values#<attribute_num-1>_size_buffer_var (uint64_t)
// tile_max_values#<attribute_num-1>_buffer
// tile_max_values#<attribute_num-1>_buffer_var
void FragmentMetadata::load_tile_max_values(
    unsigned idx, Deserializer& deserializer) {
  Status st;
  uint64_t buffer_size = 0;
  uint64_t var_buffer_size = 0;

  // Get buffer size
  buffer_size = deserializer.read<uint64_t>();

  // Get var buffer size
  var_buffer_size = deserializer.read<uint64_t>();

  // Get tile maxs
  if (buffer_size != 0) {
    auto size = buffer_size + var_buffer_size;
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      throw StatusException(Status_FragmentMetadataError(
          "Cannot load max values; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget())));
    }

    tile_max_buffer_[idx].resize(buffer_size);
    deserializer.read(&tile_max_buffer_[idx][0], buffer_size);

    if (var_buffer_size) {
      tile_max_var_buffer_[idx].resize(var_buffer_size);
      deserializer.read(&tile_max_var_buffer_[idx][0], var_buffer_size);
    }
  }
}

// ===== FORMAT =====
// tile_sum_values_attr#0_num (uint64_t)
// tile_sum_value_attr#0_#1 (uint64_t) tile_sum_value_attr#0_#2 (uint64_t) ...
// ...
// tile_sum_values_attr#<attribute_num-1>_num (uint64_t)
// tile_sum_value_attr#<attribute_num-1>_#1 (uint64_t)
//     tile_sum_value_attr#<attribute_num-1>_#2 (uint64_t) ...
void FragmentMetadata::load_tile_sum_values(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_sum_num = 0;

  // Get number of tile sums
  tile_sum_num = deserializer.read<uint64_t>();

  // Get tile sums
  if (tile_sum_num != 0) {
    auto size = tile_sum_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      throw StatusException(Status_FragmentMetadataError(
          "Cannot load sum values; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget())));
    }

    tile_sums_[idx].resize(size);
    deserializer.read(tile_sums_[idx].data(), size);
  }
}

// ===== FORMAT =====
// tile_nc_values_attr#0_num (uint64_t)
// tile_nc_value_attr#0_#1 (uint64_t) tile_nc_value_attr#0_#2 (uint64_t) ...
// ...
// tile_nc_values_attr#<attribute_num-1>_num (uint64_t)
// tile_nc_value_attr#<attribute_num-1>_#1 (uint64_t)
//     tile_nc_value_attr#<attribute_num-1>_#2 (uint64_t) ...
void FragmentMetadata::load_tile_null_count_values(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_null_count_num = 0;

  // Get number of tile null counts
  tile_null_count_num = deserializer.read<uint64_t>();

  // Get tile null count
  if (tile_null_count_num != 0) {
    auto size = tile_null_count_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr && !memory_tracker_->take_memory(size)) {
      throw StatusException(Status_FragmentMetadataError(
          "Cannot load null count values; Insufficient memory budget; "
          "Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget())));
    }

    tile_null_counts_[idx].resize(tile_null_count_num);
    deserializer.read(&tile_null_counts_[idx][0], size);
  }
}

// ===== FORMAT =====
// fragment_min_size_attr#0 (uint64_t)
// fragment_min_attr#0 (min_size)
// fragment_max_size_attr#0 (uint64_t)
// fragment_max_attr#0 (max_size)
// fragment_sum_attr#0 (uint64_t)
// fragment_null_count_attr#0 (uint64_t)
// ...
// fragment_min_size_attr#<attribute_num-1> (uint64_t)
// fragment_min_attr#<attribute_num-1> (min_size)
// fragment_max_size_attr#<attribute_num-1> (uint64_t)
// fragment_max_attr#<attribute_num-1> (max_size)
// fragment_sum_attr#<attribute_num-1> (uint64_t)
// fragment_null_count_attr#<attribute_num-1> (uint64_t)
void FragmentMetadata::load_fragment_min_max_sum_null_count(
    Deserializer& deserializer) {
  auto num = num_dims_and_attrs();

  for (unsigned int i = 0; i < num; ++i) {
    // Get min.
    uint64_t min_size;
    min_size = deserializer.read<uint64_t>();

    fragment_mins_[i].resize(min_size);
    deserializer.read(fragment_mins_[i].data(), min_size);

    // Get max.
    uint64_t max_size;
    max_size = deserializer.read<uint64_t>();

    fragment_maxs_[i].resize(max_size);
    deserializer.read(fragment_maxs_[i].data(), max_size);

    // Get sum.
    fragment_sums_[i] = deserializer.read<uint64_t>();

    // Get null count.
    fragment_null_counts_[i] = deserializer.read<uint64_t>();
  }
}

// ===== FORMAT =====
// condition_num (uint64_t)
// processed_condition_size#0 (uint64_t)
// processed_condition#0
// ...
// processed_condition_size#<condition_num-1> (uint64_t)
// processed_condition#<condition_num-1>
void FragmentMetadata::load_processed_conditions(Deserializer& deserializer) {
  // Get num conditions.
  uint64_t num;
  num = deserializer.read<uint64_t>();

  processed_conditions_.reserve(num);
  for (uint64_t i = 0; i < num; i++) {
    uint64_t size;
    size = deserializer.read<uint64_t>();

    std::string condition;
    condition.resize(size);
    deserializer.read(condition.data(), size);

    processed_conditions_.emplace_back(condition);
  }

  processed_conditions_set_ = std::unordered_set<std::string>(
      processed_conditions_.begin(), processed_conditions_.end());
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
  if (version_ == 3 || version_ == 4) {
    return load_generic_tile_offsets_v3_v4(buff);
  } else if (version_ >= 5 && version_ < 7) {
    return load_generic_tile_offsets_v5_v6(buff);
  } else if (version_ >= 7 && version_ < 11) {
    return load_generic_tile_offsets_v7_v10(buff);
  } else if (version_ == 11) {
    return load_generic_tile_offsets_v11(buff);
  } else if (version_ >= 12 && version_ < 16) {
    return load_generic_tile_offsets_v12_v15(buff);
  } else {
    return load_generic_tile_offsets_v16_or_higher(buff);
  }

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
  auto num = num_dims_and_attrs();
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

Status FragmentMetadata::load_generic_tile_offsets_v7_v10(ConstBuffer* buff) {
  // Load R-Tree offset
  RETURN_NOT_OK(buff->read(&gt_offsets_.rtree_, sizeof(uint64_t)));

  // Load offsets for tile offsets
  auto num = num_dims_and_attrs();
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
  gt_offsets_.tile_validity_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_validity_offsets_[i], sizeof(uint64_t)));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_generic_tile_offsets_v11(ConstBuffer* buff) {
  // Load R-Tree offset
  RETURN_NOT_OK(buff->read(&gt_offsets_.rtree_, sizeof(uint64_t)));

  // Load offsets for tile offsets
  auto num = num_dims_and_attrs();
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
  gt_offsets_.tile_validity_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_validity_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile min offsets
  gt_offsets_.tile_min_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_min_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile max offsets
  gt_offsets_.tile_max_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_max_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile sum offsets
  gt_offsets_.tile_sum_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_sum_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile null count offsets
  gt_offsets_.tile_null_count_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_null_count_offsets_[i], sizeof(uint64_t)));
  }

  return Status::Ok();
}

Status FragmentMetadata::load_generic_tile_offsets_v12_v15(ConstBuffer* buff) {
  // Load R-Tree offset
  RETURN_NOT_OK(buff->read(&gt_offsets_.rtree_, sizeof(uint64_t)));

  // Load offsets for tile offsets
  auto num = num_dims_and_attrs();
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
  gt_offsets_.tile_validity_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_validity_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile min offsets
  gt_offsets_.tile_min_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_min_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile max offsets
  gt_offsets_.tile_max_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_max_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile sum offsets
  gt_offsets_.tile_sum_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_sum_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile null count offsets
  gt_offsets_.tile_null_count_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_null_count_offsets_[i], sizeof(uint64_t)));
  }

  RETURN_NOT_OK(buff->read(
      &gt_offsets_.fragment_min_max_sum_null_count_offset_, sizeof(uint64_t)));

  return Status::Ok();
}

Status FragmentMetadata::load_generic_tile_offsets_v16_or_higher(
    ConstBuffer* buff) {
  // Load R-Tree offset
  RETURN_NOT_OK(buff->read(&gt_offsets_.rtree_, sizeof(uint64_t)));

  // Load offsets for tile offsets
  auto num = num_dims_and_attrs();
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
  gt_offsets_.tile_validity_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_validity_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile min offsets
  gt_offsets_.tile_min_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_min_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile max offsets
  gt_offsets_.tile_max_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_max_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile sum offsets
  gt_offsets_.tile_sum_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_sum_offsets_[i], sizeof(uint64_t)));
  }

  // Load offsets for tile null count offsets
  gt_offsets_.tile_null_count_offsets_.resize(num);
  for (unsigned i = 0; i < num; ++i) {
    RETURN_NOT_OK(
        buff->read(&gt_offsets_.tile_null_count_offsets_[i], sizeof(uint64_t)));
  }

  RETURN_NOT_OK(buff->read(
      &gt_offsets_.fragment_min_max_sum_null_count_offset_, sizeof(uint64_t)));

  RETURN_NOT_OK(
      buff->read(&gt_offsets_.processed_conditions_offsets_, sizeof(uint64_t)));

  return Status::Ok();
}

Status FragmentMetadata::load_array_schema_name(ConstBuffer* buff) {
  uint64_t size = 0;
  RETURN_NOT_OK(buff->read(&size, sizeof(uint64_t)));
  if (size == 0) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot load array schema name; Size of schema name is zero"));
  }
  array_schema_name_.resize(size);

  RETURN_NOT_OK(buff->read(&array_schema_name_[0], size));

  return Status::Ok();
}

Status FragmentMetadata::load_v1_v2(
    const EncryptionKey& encryption_key,
    const std::unordered_map<std::string, shared_ptr<ArraySchema>>&
        array_schemas) {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));
  // Read metadata
  GenericTileIO tile_io(storage_manager_, fragment_metadata_uri);
  auto&& [st, tile_opt] =
      tile_io.read_generic(0, encryption_key, storage_manager_->config());
  RETURN_NOT_OK(st);
  auto& tile = *tile_opt;

  storage_manager_->stats()->add_counter("read_frag_meta_size", tile.size());

  // Pre-v10 format fragments we need to set the schema and schema name to
  // the "old" schema. This way "old" fragments are still loaded fine
  array_schema_name_ = tiledb::sm::constants::array_schema_filename;
  auto schema = array_schemas.find(array_schema_name_);
  if (schema != array_schemas.end()) {
    set_array_schema(schema->second);
  } else {
    return Status_FragmentMetadataError(
        "Could not find schema" + array_schema_name_ +
        " in map of schemas loaded.\n" +
        "Consider reloading the array to check for new array schemas.");
  }

  // Deserialize
  ConstBuffer cbuff(tile.data(), tile.size());
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
    const EncryptionKey& encryption_key,
    Buffer* f_buff,
    uint64_t offset,
    std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas) {
  RETURN_NOT_OK(load_footer(encryption_key, f_buff, offset, array_schemas));
  return Status::Ok();
}

Status FragmentMetadata::load_footer(
    const EncryptionKey& encryption_key,
    Buffer* f_buff,
    uint64_t offset,
    std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas) {
  (void)encryption_key;  // Not used for now, perhaps in the future
  std::lock_guard<std::mutex> lock(mtx_);

  if (loaded_metadata_.footer_)
    return Status::Ok();

  Buffer buff;
  shared_ptr<ConstBuffer> cbuff = nullptr;
  if (f_buff == nullptr) {
    has_consolidated_footer_ = false;
    RETURN_NOT_OK(read_file_footer(&buff, &footer_offset_, &footer_size_));
    cbuff = make_shared<ConstBuffer>(HERE(), &buff);
  } else {
    footer_size_ = 0;
    footer_offset_ = offset;
    has_consolidated_footer_ = true;
    cbuff = make_shared<ConstBuffer>(HERE(), f_buff);
    cbuff->set_offset(offset);
  }

  RETURN_NOT_OK(load_version(cbuff.get()));
  if (version_ >= 10) {
    RETURN_NOT_OK(load_array_schema_name(cbuff.get()));
    auto schema = array_schemas.find(array_schema_name_);
    if (schema != array_schemas.end()) {
      set_array_schema(schema->second);
    } else {
      return Status_FragmentMetadataError(
          "Could not find schema" + array_schema_name_ +
          " in map of schemas loaded.\n" +
          "Consider reloading the array to check for new array schemas.");
    }
  } else {
    // Pre-v10 format fragments we need to set the schema and schema name to
    // the "old" schema. This way "old" fragments are still loaded fine
    array_schema_name_ = tiledb::sm::constants::array_schema_filename;
    auto schema = array_schemas.find(array_schema_name_);
    if (schema != array_schemas.end()) {
      set_array_schema(schema->second);
    } else {
      return Status_FragmentMetadataError(
          "Could not find schema" + array_schema_name_ +
          " in map of schemas loaded.\n" +
          "Consider reloading the array to check for new array schemas.");
    }
  }
  RETURN_NOT_OK(load_dense(cbuff.get()));
  RETURN_NOT_OK(load_non_empty_domain(cbuff.get()));
  RETURN_NOT_OK(load_sparse_tile_num(cbuff.get()));
  RETURN_NOT_OK(load_last_tile_cell_num(cbuff.get()));

  if (version_ >= 14) {
    RETURN_NOT_OK(load_has_timestamps(cbuff.get()));
  }

  if (version_ >= 15) {
    RETURN_NOT_OK(load_has_delete_meta(cbuff.get()));
  }

  RETURN_NOT_OK(load_file_sizes(cbuff.get()));
  RETURN_NOT_OK(load_file_var_sizes(cbuff.get()));
  RETURN_NOT_OK(load_file_validity_sizes(cbuff.get()));

  unsigned num = array_schema_->attribute_num() + 1 + has_timestamps_ +
                 has_delete_meta_ * 2;
  num += (version_ >= 5) ? array_schema_->dim_num() : 0;

  tile_offsets_.resize(num);
  tile_offsets_mtx_.resize(num);
  tile_var_offsets_.resize(num);
  tile_var_offsets_mtx_.resize(num);
  tile_var_sizes_.resize(num);
  tile_validity_offsets_.resize(num);
  tile_min_buffer_.resize(num);
  tile_min_var_buffer_.resize(num);
  tile_max_buffer_.resize(num);
  tile_max_var_buffer_.resize(num);
  tile_sums_.resize(num);
  tile_null_counts_.resize(num);

  fragment_mins_.resize(num);
  fragment_maxs_.resize(num);
  fragment_sums_.resize(num);
  fragment_null_counts_.resize(num);

  loaded_metadata_.tile_offsets_.resize(num, false);
  loaded_metadata_.tile_var_offsets_.resize(num, false);
  loaded_metadata_.tile_var_sizes_.resize(num, false);
  loaded_metadata_.tile_validity_offsets_.resize(num, false);
  loaded_metadata_.tile_min_.resize(num, false);
  loaded_metadata_.tile_max_.resize(num, false);
  loaded_metadata_.tile_sum_.resize(num, false);
  loaded_metadata_.tile_null_count_.resize(num, false);

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
  auto num = num_dims_and_attrs();
  Status st = buff->write(&file_sizes_[0], num * sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing file sizes failed"));
  }

  return Status::Ok();
}

// ===== FORMAT =====
// file_var_sizes#0 (uint64_t)
// ...
// file_var_sizes#{attribute_num+dim_num} (uint64_t)
Status FragmentMetadata::write_file_var_sizes(Buffer* buff) const {
  auto num = num_dims_and_attrs();
  Status st = buff->write(&file_var_sizes_[0], num * sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status_FragmentMetadataError(
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

  auto num = num_dims_and_attrs();
  Status st = buff->write(&file_validity_sizes_[0], num * sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status_FragmentMetadataError(
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
  auto num = num_dims_and_attrs();

  // Write R-Tree offset
  auto st = buff->write(&gt_offsets_.rtree_, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing R-Tree offset failed"));
  }

  // Write tile offsets
  for (unsigned i = 0; i < num; ++i) {
    st = buff->write(&gt_offsets_.tile_offsets_[i], sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing tile offsets failed"));
    }
  }

  // Write tile var offsets
  for (unsigned i = 0; i < num; ++i) {
    st = buff->write(&gt_offsets_.tile_var_offsets_[i], sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(
          Status_FragmentMetadataError("Cannot serialize fragment metadata; "
                                       "Writing tile var offsets failed"));
    }
  }

  // Write tile var sizes
  for (unsigned i = 0; i < num; ++i) {
    st = buff->write(&gt_offsets_.tile_var_sizes_[i], sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing tile var sizes failed"));
    }
  }

  // Write tile validity offsets
  if (version_ >= 7) {
    for (unsigned i = 0; i < num; ++i) {
      st =
          buff->write(&gt_offsets_.tile_validity_offsets_[i], sizeof(uint64_t));
      if (!st.ok()) {
        return LOG_STATUS(Status_FragmentMetadataError(
            "Cannot serialize fragment metadata; Writing tile offsets failed"));
      }
    }
  }

  // Write tile min offsets
  if (version_ >= 11) {
    for (unsigned i = 0; i < num; ++i) {
      st = buff->write(&gt_offsets_.tile_min_offsets_[i], sizeof(uint64_t));
      if (!st.ok()) {
        return LOG_STATUS(Status_FragmentMetadataError(
            "Cannot serialize fragment metadata; Writing tile mins failed"));
      }
    }
  }

  // Write tile max offsets
  if (version_ >= 11) {
    for (unsigned i = 0; i < num; ++i) {
      st = buff->write(&gt_offsets_.tile_max_offsets_[i], sizeof(uint64_t));
      if (!st.ok()) {
        return LOG_STATUS(Status_FragmentMetadataError(
            "Cannot serialize fragment metadata; Writing tile maxs failed"));
      }
    }
  }

  // Write tile sum offsets
  if (version_ >= 11) {
    for (unsigned i = 0; i < num; ++i) {
      st = buff->write(&gt_offsets_.tile_sum_offsets_[i], sizeof(uint64_t));
      if (!st.ok()) {
        return LOG_STATUS(Status_FragmentMetadataError(
            "Cannot serialize fragment metadata; Writing tile sums failed"));
      }
    }
  }

  // Write tile null count offsets
  if (version_ >= 11) {
    for (unsigned i = 0; i < num; ++i) {
      st = buff->write(
          &gt_offsets_.tile_null_count_offsets_[i], sizeof(uint64_t));
      if (!st.ok()) {
        return LOG_STATUS(Status_FragmentMetadataError(
            "Cannot serialize fragment metadata; Writing tile null counts "
            "failed"));
      }
    }
  }

  if (version_ >= 11) {
    st = buff->write(
        &gt_offsets_.fragment_min_max_sum_null_count_offset_, sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing fragment min max sum "
          "null counts failed"));
    }
  }

  if (version_ >= 16) {
    st = buff->write(
        &gt_offsets_.processed_conditions_offsets_, sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing processed conditions"
          "failed"));
    }
  }

  return Status::Ok();
}

Status FragmentMetadata::write_array_schema_name(Buffer* buff) const {
  uint64_t size = array_schema_name_.size();
  if (size == 0) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot write array schema name; Size of schema name is zero"));
  }
  RETURN_NOT_OK(buff->write(&size, sizeof(uint64_t)));
  return buff->write(array_schema_name_.c_str(), size);
}

// ===== FORMAT =====
// last_tile_cell_num(uint64_t)
Status FragmentMetadata::write_last_tile_cell_num(Buffer* buff) const {
  uint64_t cell_num_per_tile = dense_ ?
                                   array_schema_->domain().cell_num_per_tile() :
                                   array_schema_->capacity();

  // Handle the case of zero
  uint64_t last_tile_cell_num =
      (last_tile_cell_num_ == 0) ? cell_num_per_tile : last_tile_cell_num_;

  Status st = buff->write(&last_tile_cell_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(
        Status_FragmentMetadataError("Cannot serialize fragment metadata; "
                                     "Writing last tile cell number failed"));
  }
  return Status::Ok();
}

Status FragmentMetadata::store_rtree(
    const EncryptionKey& encryption_key, uint64_t* nbytes) {
  auto rtree_tile = write_rtree();
  RETURN_NOT_OK(write_generic_tile_to_file(encryption_key, rtree_tile, nbytes));
  storage_manager_->stats()->add_counter("write_rtree_size", *nbytes);

  return Status::Ok();
}

Tile FragmentMetadata::write_rtree() {
  rtree_.build_tree();
  SizeComputationSerializer size_computation_serializer;
  rtree_.serialize(size_computation_serializer);

  Tile tile{Tile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  rtree_.serialize(serializer);

  return tile;
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
  auto& domain = array_schema_->domain();
  auto dim_num = domain.dim_num();
  if (non_empty_domain_.empty()) {
    // Applicable only to homogeneous domains with fixed-sized types
    assert(domain.all_dims_fixed());
    assert(domain.all_dims_same_type());
    auto domain_size{2 * dim_num * domain.dimension_ptr(0)->coord_size()};

    // Write domain (dummy values)
    std::vector<uint8_t> d(domain_size, 0);
    RETURN_NOT_OK(buff->write(&d[0], domain_size));
  } else {
    // Write non-empty domain
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim{domain.dimension_ptr(d)};
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

tuple<Status, optional<Tile>> FragmentMetadata::read_generic_tile_from_file(
    const EncryptionKey& encryption_key, uint64_t offset) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  // Read metadata
  GenericTileIO tile_io(storage_manager_, fragment_metadata_uri);
  auto&& [st, tile_opt] =
      tile_io.read_generic(offset, encryption_key, storage_manager_->config());
  RETURN_NOT_OK_TUPLE(st, nullopt);

  return {Status::Ok(), std::move(*tile_opt)};
}

Status FragmentMetadata::read_file_footer(
    Buffer* buff, uint64_t* footer_offset, uint64_t* footer_size) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  // Get footer offset
  RETURN_NOT_OK(get_footer_offset_and_size(footer_offset, footer_size));

  storage_manager_->stats()->add_counter("read_frag_meta_size", *footer_size);

  if (memory_tracker_ != nullptr &&
      !memory_tracker_->take_memory(*footer_size)) {
    return LOG_STATUS(Status_FragmentMetadataError(
        "Cannot load file footer; Insufficient memory budget; Needed " +
        std::to_string(*footer_size) + " but only had " +
        std::to_string(memory_tracker_->get_memory_available()) +
        " from budget " +
        std::to_string(memory_tracker_->get_memory_budget())));
  }

  // Read footer
  return storage_manager_->read(
      fragment_metadata_uri, *footer_offset, buff, *footer_size);
}

Status FragmentMetadata::write_generic_tile_to_file(
    const EncryptionKey& encryption_key, Buffer& buff, uint64_t* nbytes) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  Tile tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      buff.data(),
      buff.size());

  GenericTileIO tile_io(storage_manager_, fragment_metadata_uri);
  RETURN_NOT_OK(tile_io.write_generic(&tile, encryption_key, nbytes));

  return Status::Ok();
}

Status FragmentMetadata::write_generic_tile_to_file(
    const EncryptionKey& encryption_key, Tile& tile, uint64_t* nbytes) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

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
  if (!array_schema_->domain().all_dims_fixed() || version_ >= 10)
    return storage_manager_->write(fragment_metadata_uri, &size, sizeof(size));
  return Status::Ok();
}

void FragmentMetadata::store_tile_offsets(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_offsets(idx, size_computation_serializer);

  Tile tile{Tile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  write_tile_offsets(idx, serializer);

  throw_if_not_ok(write_generic_tile_to_file(encryption_key, tile, nbytes));

  storage_manager_->stats()->add_counter("write_tile_offsets_size", *nbytes);
}

void FragmentMetadata::write_tile_offsets(
    unsigned idx, Serializer& serializer) {
  // Write number of tile offsets
  uint64_t tile_offsets_num = tile_offsets_[idx].size();
  serializer.write<uint64_t>(tile_offsets_num);

  // Write tile offsets
  if (tile_offsets_num != 0) {
    serializer.write(
        &tile_offsets_[idx][0], tile_offsets_num * sizeof(uint64_t));
  }
}

void FragmentMetadata::store_tile_var_offsets(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_var_offsets(idx, size_computation_serializer);

  Tile tile{Tile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  write_tile_var_offsets(idx, serializer);

  throw_if_not_ok(write_generic_tile_to_file(encryption_key, tile, nbytes));

  storage_manager_->stats()->add_counter(
      "write_tile_var_offsets_size", *nbytes);
}

void FragmentMetadata::write_tile_var_offsets(
    unsigned idx, Serializer& serializer) {
  // Write tile offsets for each attribute
  // Write number of offsets
  uint64_t tile_var_offsets_num = tile_var_offsets_[idx].size();
  serializer.write<uint64_t>(tile_var_offsets_num);

  // Write tile offsets
  if (tile_var_offsets_num != 0) {
    serializer.write(
        &tile_var_offsets_[idx][0], tile_var_offsets_num * sizeof(uint64_t));
  }
}

void FragmentMetadata::store_tile_var_sizes(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_var_sizes(idx, size_computation_serializer);

  Tile tile{Tile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  write_tile_var_sizes(idx, serializer);

  throw_if_not_ok(write_generic_tile_to_file(encryption_key, tile, nbytes));

  storage_manager_->stats()->add_counter("write_tile_var_sizes_size", *nbytes);
}

void FragmentMetadata::write_tile_var_sizes(
    unsigned idx, Serializer& serializer) {
  // Write number of sizes
  uint64_t tile_var_sizes_num = tile_var_sizes_[idx].size();
  serializer.write<uint64_t>(tile_var_sizes_num);

  // Write tile sizes
  if (tile_var_sizes_num != 0) {
    serializer.write(
        &tile_var_sizes_[idx][0], tile_var_sizes_num * sizeof(uint64_t));
  }
}

Status FragmentMetadata::store_tile_validity_offsets(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;
  RETURN_NOT_OK(write_tile_validity_offsets(idx, &buff));
  RETURN_NOT_OK(write_generic_tile_to_file(encryption_key, buff, nbytes));

  storage_manager_->stats()->add_counter(
      "write_tile_validity_offsets_size", *nbytes);

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
        Status_FragmentMetadataError("Cannot serialize fragment metadata; "
                                     "Writing number of validity tile offsets "
                                     "failed"));
  }

  // Write tile offsets
  if (tile_validity_offsets_num != 0) {
    st = buff->write(
        &tile_validity_offsets_[idx][0],
        tile_validity_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status_FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing tile offsets failed"));
    }
  }

  return Status::Ok();
}

void FragmentMetadata::store_tile_mins(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_mins(idx, size_computation_serializer);

  Tile tile{Tile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  write_tile_mins(idx, serializer);

  throw_if_not_ok(write_generic_tile_to_file(encryption_key, tile, nbytes));

  storage_manager_->stats()->add_counter("write_mins_size", *nbytes);
}

void FragmentMetadata::write_tile_mins(unsigned idx, Serializer& serializer) {
  Status st;

  // Write size of buffer
  uint64_t tile_mins_buffer_size = tile_min_buffer_[idx].size();
  serializer.write<uint64_t>(tile_mins_buffer_size);

  // Write size of buffer var
  uint64_t tile_mins_var_buffer_size = tile_min_var_buffer_[idx].size();
  serializer.write<uint64_t>(tile_mins_var_buffer_size);

  // Write tile buffer
  if (tile_mins_buffer_size != 0) {
    serializer.write(&tile_min_buffer_[idx][0], tile_mins_buffer_size);
  }

  // Write tile var buffer
  if (tile_mins_var_buffer_size != 0) {
    serializer.write(&tile_min_var_buffer_[idx][0], tile_mins_var_buffer_size);
  }
}

void FragmentMetadata::store_tile_maxs(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_maxs(idx, size_computation_serializer);

  Tile tile{Tile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  write_tile_maxs(idx, serializer);

  throw_if_not_ok(write_generic_tile_to_file(encryption_key, tile, nbytes));

  storage_manager_->stats()->add_counter("write_maxs_size", *nbytes);
}

void FragmentMetadata::write_tile_maxs(unsigned idx, Serializer& serializer) {
  Status st;

  // Write size of buffer
  uint64_t tile_maxs_buffer_size = tile_max_buffer_[idx].size();
  serializer.write<uint64_t>(tile_maxs_buffer_size);

  // Write size of buffer var
  uint64_t tile_maxs_var_buffer_size = tile_max_var_buffer_[idx].size();
  serializer.write<uint64_t>(tile_maxs_var_buffer_size);

  // Write tile buffer
  if (tile_maxs_buffer_size != 0) {
    serializer.write(&tile_max_buffer_[idx][0], tile_maxs_buffer_size);
  }

  // Write tile var buffer
  if (tile_maxs_var_buffer_size != 0) {
    serializer.write(&tile_max_var_buffer_[idx][0], tile_maxs_var_buffer_size);
  }
}

void FragmentMetadata::store_tile_sums(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_sums(idx, size_computation_serializer);

  Tile tile{Tile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  write_tile_sums(idx, serializer);

  throw_if_not_ok(write_generic_tile_to_file(encryption_key, tile, nbytes));

  storage_manager_->stats()->add_counter("write_sums_size", *nbytes);
}

void FragmentMetadata::write_tile_sums(unsigned idx, Serializer& serializer) {
  // Write number of tile sums
  uint64_t tile_sums_num = tile_sums_[idx].size() / sizeof(uint64_t);
  serializer.write<uint64_t>(tile_sums_num);

  // Write tile sums
  if (tile_sums_num != 0) {
    serializer.write(tile_sums_[idx].data(), tile_sums_num * sizeof(uint64_t));
  }
}

void FragmentMetadata::store_tile_null_counts(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_null_counts(idx, size_computation_serializer);

  Tile tile{Tile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  write_tile_null_counts(idx, serializer);

  throw_if_not_ok(write_generic_tile_to_file(encryption_key, tile, nbytes));

  storage_manager_->stats()->add_counter("write_null_counts_size", *nbytes);
}

void FragmentMetadata::write_tile_null_counts(
    unsigned idx, Serializer& serializer) {
  // Write number of tile null counts
  uint64_t tile_null_counts_num = tile_null_counts_[idx].size();
  serializer.write<uint64_t>(tile_null_counts_num);

  // Write tile null counts
  if (tile_null_counts_num != 0) {
    serializer.write(
        &tile_null_counts_[idx][0], tile_null_counts_num * sizeof(uint64_t));
  }
}

void FragmentMetadata::store_fragment_min_max_sum_null_count(
    uint64_t num, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Status st;
  Buffer buff;

  auto serialize_data = [&](Serializer& serializer) {
    // Store all attributes.
    for (unsigned int i = 0; i < num; ++i) {
      // Store min.
      uint64_t min_size = fragment_mins_[i].size();
      serializer.write<uint64_t>(min_size);

      serializer.write(fragment_mins_[i].data(), min_size);

      // Store max.
      uint64_t max_size = fragment_maxs_[i].size();
      serializer.write<uint64_t>(max_size);

      serializer.write(fragment_maxs_[i].data(), max_size);

      // Store sum.
      serializer.write<uint64_t>(fragment_sums_[i]);

      // Store null count.
      serializer.write<uint64_t>(fragment_null_counts_[i]);
    }
  };

  SizeComputationSerializer size_computation_serializer;
  serialize_data(size_computation_serializer);

  Tile tile{Tile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  serialize_data(serializer);

  throw_if_not_ok(write_generic_tile_to_file(encryption_key, tile, nbytes));

  storage_manager_->stats()->add_counter("write_null_counts_size", *nbytes);
}

void FragmentMetadata::store_processed_conditions(
    const EncryptionKey& encryption_key, uint64_t* nbytes) {
  auto serialize_processed_conditions = [this](Serializer& serializer) {
    // Store num conditions.
    uint64_t num = processed_conditions_.size();
    serializer.write<uint64_t>(num);

    for (auto& processed_condition : processed_conditions_) {
      uint64_t size = processed_condition.size();
      serializer.write<uint64_t>(size);

      serializer.write(processed_condition.data(), processed_condition.size());
    }
  };
  SizeComputationSerializer size_computation_serializer;
  serialize_processed_conditions(size_computation_serializer);

  Tile tile{Tile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  serialize_processed_conditions(serializer);

  throw_if_not_ok(write_generic_tile_to_file(encryption_key, tile, nbytes));

  storage_manager_->stats()->add_counter(
      "write_processed_conditions_size", *nbytes);
}

template <class T>
void FragmentMetadata::compute_fragment_min_max_sum(const std::string& name) {
  // For easy reference.
  const auto& idx = idx_map_[name];
  const auto nullable = array_schema_->is_nullable(name);
  const auto is_dim = array_schema_->is_dim(name);
  const auto type = array_schema_->type(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);

  // No metadata for dense coords
  if (!array_schema_->dense() || !is_dim) {
    const auto has_min_max = TileMetadataGenerator::has_min_max_metadata(
        type, is_dim, false, cell_val_num);
    const auto has_sum =
        TileMetadataGenerator::has_sum_metadata(type, false, cell_val_num);

    if (has_min_max) {
      // Initialize defaults.
      T min = metadata_generator_type_data<T>::min;
      T max = metadata_generator_type_data<T>::max;

      // Get data and tile num.
      auto min_values =
          static_cast<T*>(static_cast<void*>(tile_min_buffer_[idx].data()));
      auto max_values =
          static_cast<T*>(static_cast<void*>(tile_max_buffer_[idx].data()));
      auto& null_count_values = tile_null_counts_[idx];
      auto tile_num = this->tile_num();

      // Process tile by tile.
      for (uint64_t t = 0; t < tile_num; t++) {
        const bool is_null = nullable && null_count_values[t] == cell_num(t);
        if (!is_null) {
          min = min < min_values[t] ? min : min_values[t];
          max = max > max_values[t] ? max : max_values[t];
        }
      }

      // Copy min max values.
      fragment_mins_[idx].resize(sizeof(T));
      fragment_maxs_[idx].resize(sizeof(T));
      memcpy(fragment_mins_[idx].data(), &min, sizeof(T));
      memcpy(fragment_maxs_[idx].data(), &max, sizeof(T));
    }

    if (has_sum) {
      compute_fragment_sum<typename metadata_generator_type_data<T>::sum_type>(
          idx, nullable);
    }
  }
}

template <>
void FragmentMetadata::compute_fragment_min_max_sum<char>(
    const std::string& name) {
  // For easy reference.
  const auto idx = idx_map_[name];
  const auto nullable = array_schema_->is_nullable(name);
  const auto is_dim = array_schema_->is_dim(name);
  const auto type = array_schema_->type(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);

  // Return if there's no min/max.
  const auto has_min_max = TileMetadataGenerator::has_min_max_metadata(
      type, is_dim, false, cell_val_num);
  if (!has_min_max)
    return;

  // Initialize to null.
  void* min = nullptr;
  void* max = nullptr;

  // Get data and tile num.
  auto min_values = tile_min_buffer_[idx].data();
  auto max_values = tile_max_buffer_[idx].data();
  auto& null_count_values = tile_null_counts_[idx];
  auto tile_num = this->tile_num();

  // Process tile by tile.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      min = (min == nullptr ||
             strncmp((const char*)min, (const char*)min_values, cell_val_num) >
                 0) ?
                min_values :
                min;
      min_values += cell_val_num;
      max = (max == nullptr ||
             strncmp((const char*)max, (const char*)max_values, cell_val_num) <
                 0) ?
                max_values :
                max;
      max_values += cell_val_num;
    }
  }

  // Copy values.
  if (min != nullptr) {
    fragment_mins_[idx].resize(cell_val_num);
    memcpy(fragment_mins_[idx].data(), min, cell_val_num);
  }

  if (max != nullptr) {
    fragment_maxs_[idx].resize(cell_val_num);
    memcpy(fragment_maxs_[idx].data(), max, cell_val_num);
  }
}

template <>
void FragmentMetadata::compute_fragment_sum<int64_t>(
    const uint64_t idx, const bool nullable) {
  // Zero sum.
  int64_t sum_data = 0;

  // Get data and tile num.
  auto values =
      static_cast<int64_t*>(static_cast<void*>(tile_sums_[idx].data()));
  auto& null_count_values = tile_null_counts_[idx];
  auto tile_num = this->tile_num();

  // Process tile by tile, swallowing overflow exception.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      if (sum_data > 0 && values[t] > 0 &&
          (sum_data > std::numeric_limits<int64_t>::max() - values[t])) {
        sum_data = std::numeric_limits<int64_t>::max();
        break;
      }

      if (sum_data < 0 && values[t] < 0 &&
          (sum_data < std::numeric_limits<int64_t>::min() - values[t])) {
        sum_data = std::numeric_limits<int64_t>::min();
        break;
      }

      sum_data += values[t];
    }
  }

  // Copy value.
  memcpy(&fragment_sums_[idx], &sum_data, sizeof(int64_t));
}

template <>
void FragmentMetadata::compute_fragment_sum<uint64_t>(
    const uint64_t idx, const bool nullable) {
  // Zero sum.
  uint64_t sum_data = 0;

  // Get data and tile num.
  auto values =
      static_cast<uint64_t*>(static_cast<void*>(tile_sums_[idx].data()));
  auto& null_count_values = tile_null_counts_[idx];
  auto tile_num = this->tile_num();

  // Process tile by tile, swallowing overflow exception.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      if (sum_data > std::numeric_limits<uint64_t>::max() - values[t]) {
        sum_data = std::numeric_limits<uint64_t>::max();
        break;
      }

      sum_data += values[t];
    }
  }

  // Copy value.
  memcpy(&fragment_sums_[idx], &sum_data, sizeof(uint64_t));
}

template <>
void FragmentMetadata::compute_fragment_sum<double>(
    const uint64_t idx, const bool nullable) {
  // Zero sum.
  double sum_data = 0;

  // Get data and tile num.
  auto values =
      static_cast<double*>(static_cast<void*>(tile_sums_[idx].data()));
  auto& null_count_values = tile_null_counts_[idx];
  auto tile_num = this->tile_num();

  // Process tile by tile, swallowing overflow exception.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      if ((sum_data < 0.0) == (values[t] < 0.0) &&
          std::abs(sum_data) >
              std::numeric_limits<double>::max() - std::abs(values[t])) {
        sum_data = sum_data < 0.0 ? std::numeric_limits<double>::lowest() :
                                    std::numeric_limits<double>::max();
        break;
      }

      sum_data += values[t];
    }
  }

  // Copy value.
  memcpy(&fragment_sums_[idx], &sum_data, sizeof(double));
}

void FragmentMetadata::min_max_var(const std::string& name) {
  // For easy reference.
  const auto nullable = array_schema_->is_nullable(name);
  const auto is_dim = array_schema_->is_dim(name);
  const auto type = array_schema_->type(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  const auto idx = idx_map_[name];

  // Return if there's no min/max.
  const auto has_min_max = TileMetadataGenerator::has_min_max_metadata(
      type, is_dim, true, cell_val_num);
  if (!has_min_max)
    return;

  // Initialize to null.
  void* min = nullptr;
  void* max = nullptr;
  uint64_t min_size = 0;
  uint64_t max_size = 0;

  // Get data and tile num.
  auto min_offsets =
      static_cast<uint64_t*>(static_cast<void*>(tile_min_buffer_[idx].data()));
  auto max_offsets =
      static_cast<uint64_t*>(static_cast<void*>(tile_max_buffer_[idx].data()));
  auto min_values = tile_min_var_buffer_[idx].data();
  auto max_values = tile_max_var_buffer_[idx].data();
  auto& null_count_values = tile_null_counts_[idx];
  auto tile_num = this->tile_num();

  // Process tile by tile.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      auto min_value = min_values + min_offsets[t];
      auto min_value_size =
          t == tile_num - 1 ?
              tile_min_var_buffer_[idx].size() - min_offsets[t] :
              min_offsets[t + 1] - min_offsets[t];
      auto max_value = max_values + max_offsets[t];
      auto max_value_size =
          t == tile_num - 1 ?
              tile_max_var_buffer_[idx].size() - max_offsets[t] :
              max_offsets[t + 1] - max_offsets[t];
      if (min == nullptr && max == nullptr) {
        min = min_value;
        min_size = min_value_size;
        max = max_value;
        max_size = max_value_size;
      } else {
        // Process min.
        size_t min_cmp_size = std::min<size_t>(min_size, min_value_size);
        int cmp =
            strncmp(static_cast<const char*>(min), min_value, min_cmp_size);
        if (cmp != 0) {
          if (cmp > 0) {
            min = min_value;
            min_size = min_value_size;
          }
        } else {
          if (min_value_size < min_size) {
            min = min_value;
            min_size = min_value_size;
          }
        }

        // Process max.
        size_t max_cmp_size = std::min<size_t>(max_size, max_value_size);
        cmp = strncmp(static_cast<const char*>(max), max_value, max_cmp_size);
        if (cmp != 0) {
          if (cmp < 0) {
            max = max_value;
            max_size = max_value_size;
          }
        } else {
          if (max_value_size > max_size) {
            max = max_value;
            max_size = max_value_size;
          }
        }
      }
    }
  }

  // Copy values.
  if (min != nullptr) {
    fragment_mins_[idx].resize(min_size);
    memcpy(fragment_mins_[idx].data(), min, min_size);
  }

  if (max != nullptr) {
    fragment_maxs_[idx].resize(max_size);
    memcpy(fragment_maxs_[idx].data(), max, max_size);
  }
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

Status FragmentMetadata::write_has_timestamps(Buffer* buff) const {
  RETURN_NOT_OK(buff->write(&has_timestamps_, sizeof(char)));
  return Status::Ok();
}

Status FragmentMetadata::write_has_delete_meta(Buffer* buff) const {
  RETURN_NOT_OK(buff->write(&has_delete_meta_, sizeof(char)));
  return Status::Ok();
}

Status FragmentMetadata::store_footer(const EncryptionKey& encryption_key) {
  (void)encryption_key;  // Not used for now, maybe in the future

  Buffer buff;
  RETURN_NOT_OK(write_footer(&buff));
  RETURN_NOT_OK(write_footer_to_file(&buff));

  storage_manager_->stats()->add_counter(
      "write_frag_meta_footer_size", buff.size());

  return Status::Ok();
}

void FragmentMetadata::clean_up() {
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);

  storage_manager_->close_file(fragment_metadata_uri);
  storage_manager_->vfs()->remove_file(fragment_metadata_uri);
}

const shared_ptr<const ArraySchema>& FragmentMetadata::array_schema() const {
  return array_schema_;
}

void FragmentMetadata::build_idx_map() {
  idx_map_.clear();

  auto attributes = array_schema_->attributes();
  for (unsigned i = 0; i < attributes.size(); ++i) {
    auto attr_name = attributes[i]->name();
    idx_map_[attr_name] = i;
  }
  idx_map_[constants::coords] = array_schema_->attribute_num();
  for (unsigned i = 0; i < array_schema_->dim_num(); ++i) {
    const auto& dim_name{array_schema_->dimension_ptr(i)->name()};
    idx_map_[dim_name] = array_schema_->attribute_num() + 1 + i;
  }

  auto idx = array_schema_->attribute_num() + 1 + array_schema_->dim_num();
  if (has_timestamps_) {
    idx_map_[constants::timestamps] = idx++;
  }
  if (has_delete_meta_) {
    idx_map_[constants::delete_timestamps] = idx++;
    idx_map_[constants::delete_condition_index] = idx++;
  }
}

void FragmentMetadata::set_schema_name(const std::string& name) {
  array_schema_name_ = name;
}

void FragmentMetadata::set_dense(bool dense) {
  dense_ = dense;
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
