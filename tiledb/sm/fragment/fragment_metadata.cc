/**
 * @file   fragment_metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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

#include "tiledb/common/assert.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/current_domain.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/ndrectangle.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/fragment/ondemand_fragment_metadata.h"
#include "tiledb/sm/fragment/v1v2preloaded_fragment_metadata.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/rectangle.h"
#include "tiledb/sm/query/readers/aggregators/tile_metadata.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"
#include "tiledb/storage_format/serialization/serializers.h"
#include "tiledb/type/range/range.h"

#include <cassert>
#include <iostream>
#include <numeric>
#include <string>

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

FragmentMetadata::FragmentMetadata(
    ContextResources* resources,
    shared_ptr<MemoryTracker> memory_tracker,
    format_version_t version)
    : resources_(resources)
    , memory_tracker_(memory_tracker)
    , version_(version)
    , loaded_metadata_(
          LoadedFragmentMetadata::create(*this, memory_tracker, version_))
    , loaded_metadata_ptr_(loaded_metadata_.get()) {
}

FragmentMetadata::FragmentMetadata(
    ContextResources* resources,
    const shared_ptr<const ArraySchema>& array_schema,
    const URI& fragment_uri,
    const std::pair<uint64_t, uint64_t>& timestamp_range,
    shared_ptr<MemoryTracker> memory_tracker,
    bool dense,
    bool has_timestamps,
    bool has_deletes_meta)
    : resources_(resources)
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
    , tile_index_base_(0)
    , version_(FragmentID{fragment_uri}.array_format_version())
    , timestamp_range_(timestamp_range)
    , array_uri_(array_schema_->array_uri())
    , loaded_metadata_(
          LoadedFragmentMetadata::create(*this, memory_tracker, version_))
    , loaded_metadata_ptr_(loaded_metadata_.get()) {
  build_idx_map();
  array_schema_name_ = array_schema_->name();
}

/* ****************************** */
/*                API             */
/* ****************************** */

void FragmentMetadata::set_mbr(uint64_t tile, const NDRange& mbr) {
  // For easy reference
  tile += tile_index_base_;
  loaded_metadata_ptr_->rtree().set_leaf(tile, mbr);
  return expand_non_empty_domain(mbr);
}

void FragmentMetadata::set_tile_index_base(uint64_t tile_base) {
  tile_index_base_ = tile_base;
}

void FragmentMetadata::set_tile_offset(
    const std::string& name, uint64_t tid, uint64_t step) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  iassert(tid < loaded_metadata_ptr_->tile_offsets()[idx].size());
  loaded_metadata_ptr_->tile_offsets()[idx][tid] = file_sizes_[idx];
  file_sizes_[idx] += step;
}

void FragmentMetadata::set_tile_var_offset(
    const std::string& name, uint64_t tid, uint64_t step) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  iassert(tid < loaded_metadata_ptr_->tile_var_offsets()[idx].size());
  loaded_metadata_ptr_->tile_var_offsets()[idx][tid] = file_var_sizes_[idx];
  file_var_sizes_[idx] += step;
}

void FragmentMetadata::set_tile_var_size(
    const std::string& name, uint64_t tid, uint64_t size) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  iassert(tid < loaded_metadata_ptr_->tile_var_sizes()[idx].size());
  loaded_metadata_ptr_->tile_var_sizes()[idx][tid] = size;
}

void FragmentMetadata::set_tile_validity_offset(
    const std::string& name, uint64_t tid, uint64_t step) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  iassert(tid < loaded_metadata_ptr_->tile_validity_offsets()[idx].size());
  loaded_metadata_ptr_->tile_validity_offsets()[idx][tid] =
      file_validity_sizes_[idx];
  file_validity_sizes_[idx] += step;
}

void FragmentMetadata::set_tile_min(
    const std::string& name, uint64_t tid, const ByteVec& min) {
  const auto size = min.size();
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * size;
  iassert(tid < loaded_metadata_ptr_->tile_min_buffer()[idx].size() / size);
  memcpy(
      &loaded_metadata_ptr_->tile_min_buffer()[idx][buff_offset],
      min.data(),
      size);
}

void FragmentMetadata::set_tile_min_var_size(
    const std::string& name, uint64_t tid, uint64_t size) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * sizeof(uint64_t);
  iassert(
      tid <
      loaded_metadata_ptr_->tile_min_buffer()[idx].size() / sizeof(uint64_t));

  auto offset =
      (uint64_t*)&loaded_metadata_ptr_->tile_min_buffer()[idx][buff_offset];
  *offset = size;
}

void FragmentMetadata::set_tile_min_var(
    const std::string& name, uint64_t tid, const ByteVec& min) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * sizeof(uint64_t);
  iassert(
      tid <
      loaded_metadata_ptr_->tile_min_buffer()[idx].size() / sizeof(uint64_t));

  auto offset =
      (uint64_t*)&loaded_metadata_ptr_->tile_min_buffer()[idx][buff_offset];
  auto size =
      buff_offset != loaded_metadata_ptr_->tile_min_buffer()[idx].size() -
                         sizeof(uint64_t) ?
          offset[1] - offset[0] :
          loaded_metadata_ptr_->tile_min_var_buffer()[idx].size() - offset[0];

  // Copy var data
  if (size) {  // avoid (potentially) illegal index ref's when size is zero
    memcpy(
        &loaded_metadata_ptr_->tile_min_var_buffer()[idx][offset[0]],
        min.data(),
        size);
  }
}

void FragmentMetadata::set_tile_max(
    const std::string& name, uint64_t tid, const ByteVec& max) {
  const auto size = max.size();
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * size;
  iassert(tid < loaded_metadata_ptr_->tile_max_buffer()[idx].size() / size);
  memcpy(
      &loaded_metadata_ptr_->tile_max_buffer()[idx][buff_offset],
      max.data(),
      size);
}

void FragmentMetadata::set_tile_max_var_size(
    const std::string& name, uint64_t tid, uint64_t size) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * sizeof(uint64_t);
  iassert(
      tid <
      loaded_metadata_ptr_->tile_max_buffer()[idx].size() / sizeof(uint64_t));

  auto offset =
      (uint64_t*)&loaded_metadata_ptr_->tile_max_buffer()[idx][buff_offset];
  *offset = size;
}

void FragmentMetadata::set_tile_max_var(
    const std::string& name, uint64_t tid, const ByteVec& max) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  auto buff_offset = tid * sizeof(uint64_t);
  iassert(
      tid <
      loaded_metadata_ptr_->tile_max_buffer()[idx].size() / sizeof(uint64_t));

  auto offset =
      (uint64_t*)&loaded_metadata_ptr_->tile_max_buffer()[idx][buff_offset];
  auto size =
      buff_offset != loaded_metadata_ptr_->tile_max_buffer()[idx].size() -
                         sizeof(uint64_t) ?
          offset[1] - offset[0] :
          loaded_metadata_ptr_->tile_max_var_buffer()[idx].size() - offset[0];

  // Copy var data
  if (size) {  // avoid (potentially) illegal index ref's when size is zero
    memcpy(
        &loaded_metadata_ptr_->tile_max_var_buffer()[idx][offset[0]],
        max.data(),
        size);
  }
}

void FragmentMetadata::convert_tile_min_max_var_sizes_to_offsets(
    const std::string& name) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;

  // Fix the min offsets.
  uint64_t offset = loaded_metadata_ptr_->tile_min_var_buffer()[idx].size();
  auto offsets =
      (uint64_t*)loaded_metadata_ptr_->tile_min_buffer()[idx].data() +
      tile_index_base_;
  for (uint64_t i = tile_index_base_;
       i <
       loaded_metadata_ptr_->tile_min_buffer()[idx].size() / sizeof(uint64_t);
       i++) {
    auto size = *offsets;
    *offsets = offset;
    offsets++;
    offset += size;
  }

  // Allocate min var data buffer.
  loaded_metadata_ptr_->tile_min_var_buffer()[idx].resize(offset);

  // Fix the max offsets.
  offset = loaded_metadata_ptr_->tile_max_var_buffer()[idx].size();
  offsets = (uint64_t*)loaded_metadata_ptr_->tile_max_buffer()[idx].data() +
            tile_index_base_;
  for (uint64_t i = tile_index_base_;
       i <
       loaded_metadata_ptr_->tile_max_buffer()[idx].size() / sizeof(uint64_t);
       i++) {
    auto size = *offsets;
    *offsets = offset;
    offsets++;
    offset += size;
  }

  // Allocate min var data buffer.
  loaded_metadata_ptr_->tile_max_var_buffer()[idx].resize(offset);
}

void FragmentMetadata::set_tile_sum(
    const std::string& name, uint64_t tid, const ByteVec& sum) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  iassert(
      tid * sizeof(uint64_t) < loaded_metadata_ptr_->tile_sums()[idx].size());
  memcpy(
      &loaded_metadata_ptr_->tile_sums()[idx][tid * sizeof(uint64_t)],
      sum.data(),
      sizeof(uint64_t));
}

void FragmentMetadata::set_tile_null_count(
    const std::string& name, uint64_t tid, uint64_t null_count) {
  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  tid += tile_index_base_;
  iassert(tid < loaded_metadata_ptr_->tile_null_counts()[idx].size());
  loaded_metadata_ptr_->tile_null_counts()[idx][tid] = null_count;
}

template <>
void FragmentMetadata::compute_fragment_min_max_sum<char>(
    const std::string& name);

void FragmentMetadata::compute_fragment_min_max_sum_null_count() {
  std::vector<std::string> names;
  names.reserve(idx_map_.size());
  for (auto& it : idx_map_) {
    names.emplace_back(it.first);
  }

  // Process all attributes in parallel.
  throw_if_not_ok(parallel_for(
      &resources_->compute_tp(), 0, idx_map_.size(), [&](uint64_t n) {
        // For easy reference.
        const auto& name = names[n];
        const auto& idx = idx_map_[name];
        const auto var_size = array_schema_->var_size(name);
        const auto type = array_schema_->type(name);

        // Compute null count.
        loaded_metadata_ptr_->fragment_null_counts()[idx] = std::accumulate(
            loaded_metadata_ptr_->tile_null_counts()[idx].begin(),
            loaded_metadata_ptr_->tile_null_counts()[idx].end(),
            0ull);

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
            case Datatype::GEOM_WKB:
            case Datatype::GEOM_WKT:
              compute_fragment_min_max_sum<std::byte>(name);
              break;
            default:
              break;
          }
        }

        return Status::Ok();
      }));
}

void FragmentMetadata::set_array_schema(
    const shared_ptr<const ArraySchema>& array_schema) {
  array_schema_ = array_schema;

  // Rebuild index mapping
  build_idx_map();
}

uint64_t FragmentMetadata::cell_num() const {
  auto tile_num = this->tile_num();
  iassert(tile_num != 0);
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

void FragmentMetadata::add_max_buffer_sizes(
    const EncryptionKey& encryption_key,
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Dense case
  if (dense_) {
    add_max_buffer_sizes_dense(subarray, buffer_sizes);
  } else {
    // Convert subarray to NDRange
    auto dim_num = array_schema_->dim_num();
    auto sub_ptr = (const unsigned char*)subarray;
    NDRange sub_nd(dim_num);
    uint64_t offset = 0;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto r_size{2 * array_schema_->dimension_ptr(d)->coord_size()};
      sub_nd[d] = Range(&sub_ptr[offset], r_size);
      offset += r_size;
    }

    // Sparse case
    add_max_buffer_sizes_sparse(encryption_key, sub_nd, buffer_sizes);
  }
}

void FragmentMetadata::add_max_buffer_sizes_dense(
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
      throw FragmentMetadataStatusException(
          "Cannot compute add read buffer sizes for dense array; Unsupported "
          "domain type");
  }
}

template <class T>
void FragmentMetadata::add_max_buffer_sizes_dense(
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
        it.second.second += loaded_metadata_ptr_->tile_var_size(it.first, tid);
      } else {
        it.second.first += cell_num(tid) * array_schema_->cell_size(it.first);
      }
    }
  }
}

void FragmentMetadata::add_max_buffer_sizes_sparse(
    const EncryptionKey& encryption_key,
    const NDRange& subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  loaded_metadata_ptr_->load_rtree(encryption_key);

  // Get tile overlap
  std::vector<bool> is_default(subarray.size(), false);
  auto tile_overlap =
      loaded_metadata_ptr_->rtree().get_tile_overlap(subarray, is_default);

  // Handle tile ranges
  for (const auto& tr : tile_overlap.tile_ranges_) {
    for (uint64_t tid = tr.first; tid <= tr.second; ++tid) {
      for (auto& it : *buffer_sizes) {
        if (array_schema_->var_size(it.first)) {
          auto cell_num = this->cell_num(tid);
          it.second.first += cell_num * constants::cell_var_offset_size;
          it.second.second +=
              loaded_metadata_ptr_->tile_var_size(it.first, tid);
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
        it.second.second += loaded_metadata_ptr_->tile_var_size(it.first, tid);
      } else {
        it.second.first += cell_num(tid) * array_schema_->cell_size(it.first);
      }
    }
  }
}

uint64_t FragmentMetadata::fragment_size() const {
  // Add file sizes
  uint64_t size = 0;
  for (const auto& file_size : file_sizes_)
    size += file_size;
  for (const auto& file_var_size : file_var_sizes_)
    size += file_var_size;
  for (const auto& file_validity_size : file_validity_sizes_)
    size += file_validity_size;

  const uint64_t meta_file_size = fragment_meta_size();
  // Validate that the meta_file_size is not zero, either preloaded or fetched
  // above
  iassert(meta_file_size != 0);

  // Add fragment metadata file size
  size += meta_file_size;

  return size;
}

uint64_t FragmentMetadata::fragment_meta_size() const {
  // The fragment metadata file size can be empty when we've loaded consolidated
  // metadata
  if (meta_file_size_ == 0) {
    auto meta_uri = fragment_uri_.join_path(
        std::string(constants::fragment_metadata_filename));
    meta_file_size_ = resources_->vfs().file_size(meta_uri);
  }
  return meta_file_size_;
}

void FragmentMetadata::init_domain(const NDRange& non_empty_domain) {
  // Sanity check
  iassert(!non_empty_domain.empty());
  iassert(non_empty_domain_.empty());
  iassert(domain_.empty());

  set_domain(non_empty_domain);
}

void FragmentMetadata::set_domain(const NDRange& non_empty_domain) {
  auto& domain{array_schema_->domain()};

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
    array_schema_->current_domain().expand_to_tiles(domain, domain_);
  }
}

void FragmentMetadata::init(const NDRange& non_empty_domain) {
  // For easy reference
  auto num = num_dims_and_attrs();

  init_domain(non_empty_domain);

  // Set last tile cell number
  last_tile_cell_num_ = 0;

  // Initialize tile offsets
  loaded_metadata_ptr_->resize_offsets(num);

  file_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i)
    file_sizes_[i] = 0;

  file_var_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i)
    file_var_sizes_[i] = 0;

  // Initialize validity tile offsets
  file_validity_sizes_.resize(num);
  for (unsigned int i = 0; i < num; ++i)
    file_validity_sizes_[i] = 0;
}

std::vector<shared_ptr<FragmentMetadata>> FragmentMetadata::load(
    ContextResources& resources,
    shared_ptr<MemoryTracker> memory_tracker,
    const shared_ptr<const ArraySchema> array_schema_latest,
    const std::unordered_map<std::string, shared_ptr<ArraySchema>>&
        array_schemas_all,
    const EncryptionKey& encryption_key,
    const std::vector<TimestampedURI>& fragments_to_load,
    const std::unordered_map<std::string, std::pair<Tile*, uint64_t>>&
        offsets) {
  auto timer_se = resources.stats().start_timer("sm_load_fragment_metadata");

  // Load the metadata for each fragment
  auto fragment_num = fragments_to_load.size();
  std::vector<shared_ptr<FragmentMetadata>> fragment_metadata;
  fragment_metadata.resize(fragment_num);
  auto status =
      parallel_for(&resources.compute_tp(), 0, fragment_num, [&](size_t f) {
        const auto& sf = fragments_to_load[f];
        URI coords_uri =
            sf.uri_.join_path(constants::coords + constants::file_suffix);

        // Note that the fragment metadata version is >= the array schema
        // version. Therefore, the check below is defensive and will always
        // ensure backwards compatibility.
        shared_ptr<FragmentMetadata> metadata;
        FragmentID fragment_id{sf.uri_};
        if (fragment_id.array_format_version() <= 2) {
          metadata = make_shared<FragmentMetadata>(
              HERE(),
              &resources,
              array_schema_latest,
              sf.uri_,
              sf.timestamp_range_,
              memory_tracker,
              !resources.vfs().is_file(coords_uri));
        } else {
          // Fragment format version > 2
          metadata = make_shared<FragmentMetadata>(
              HERE(),
              &resources,
              array_schema_latest,
              sf.uri_,
              sf.timestamp_range_,
              memory_tracker);
        }

        // Potentially find the basic fragment metadata in the consolidated
        // metadata buffer
        Tile* fragment_metadata_tile = nullptr;
        uint64_t offset = 0;

        auto it = offsets.end();
        if (metadata->format_version() >= 9) {
          it = offsets.find(fragment_id.name());
        } else {
          it = offsets.find(sf.uri_.to_string());
        }
        if (it != offsets.end()) {
          fragment_metadata_tile = it->second.first;
          offset = it->second.second;
        }

        // Load fragment metadata
        metadata->load(
            encryption_key, fragment_metadata_tile, offset, array_schemas_all);

        fragment_metadata[f] = metadata;
        return Status::Ok();
      });
  throw_if_not_ok(status);

  return fragment_metadata;
}

void FragmentMetadata::load(
    const EncryptionKey& encryption_key,
    Tile* fragment_metadata_tile,
    uint64_t offset,
    std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas) {
  auto meta_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));
  // Load the metadata file size when we are not reading from consolidated
  // buffer
  if (fragment_metadata_tile == nullptr) {
    meta_file_size_ = resources_->vfs().file_size(meta_uri);
  }

  // Get fragment name version
  FragmentID fragment_id{fragment_uri_};
  if (fragment_id.array_format_version() <= 2) {
    return load_v1_v2(encryption_key, array_schemas);
  } else {
    return load_v3_or_higher(
        encryption_key, fragment_metadata_tile, offset, array_schemas);
  }
}

void FragmentMetadata::store(const EncryptionKey& encryption_key) {
  // integrity checks
  if (dense_) {
    const uint64_t dense_tile_num = tile_num();

    for (const auto& tile_offsets : loaded_metadata_ptr_->tile_offsets()) {
      iassert(tile_offsets.size() == dense_tile_num);
    }
    for (const auto& tile_var_offsets :
         loaded_metadata_ptr_->tile_var_offsets()) {
      iassert(tile_var_offsets.size() == dense_tile_num);
    }
    for (const auto& tile_var_sizes : loaded_metadata_ptr_->tile_var_sizes()) {
      iassert(tile_var_sizes.size() == dense_tile_num);
    }
    for (const auto& tile_validity_offsets :
         loaded_metadata_ptr_->tile_validity_offsets()) {
      iassert(tile_validity_offsets.size() == dense_tile_num);
    }
    for (const auto& tile_null_counts :
         loaded_metadata_ptr_->tile_null_counts()) {
      if (!tile_null_counts.empty()) {
        iassert(tile_null_counts.size() == dense_tile_num);
      }
    }

    // what about min, max, sum?
    // requires iteration in stride with schema fields to get cell size
    // probably a good idea, ask about it in code review
  }

  auto timer_se = resources_->stats().start_timer("write_store_frag_meta");

  // Make sure the data fits in the current domain before we commit to disk.
  auto cd = array_schema_->get_current_domain();
  if (!cd->empty()) {
    if (!cd->includes(non_empty_domain_)) {
      throw FragmentMetadataStatusException(
          "Cells are written outside of the defined current domain.");
    }
  }

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
      store_v7_v10(encryption_key);
    } else if (version_ == 11) {
      store_v11(encryption_key);
    } else if (version_ <= 14) {
      store_v12_v14(encryption_key);
    } else {
      store_v15_or_higher(encryption_key);
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

void FragmentMetadata::store_v7_v10(const EncryptionKey& encryption_key) {
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  auto num = num_dims_and_attrs();
  uint64_t offset = 0, nbytes;

  // Store R-Tree
  gt_offsets_.rtree_ = offset;
  store_rtree(encryption_key, &nbytes);
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
    store_tile_validity_offsets(i, encryption_key, &nbytes);
    offset += nbytes;
  }

  // Store footer
  store_footer(encryption_key);

  // Close file
  throw_if_not_ok(resources_->vfs().close_file(fragment_metadata_uri));
}

void FragmentMetadata::store_v11(const EncryptionKey& encryption_key) {
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  auto num = num_dims_and_attrs();
  uint64_t offset = 0, nbytes;

  // Store R-Tree
  gt_offsets_.rtree_ = offset;
  store_rtree(encryption_key, &nbytes);
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
    store_tile_validity_offsets(i, encryption_key, &nbytes);
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
  store_footer(encryption_key);

  // Close file
  throw_if_not_ok(resources_->vfs().close_file(fragment_metadata_uri));
}

void FragmentMetadata::store_v12_v14(const EncryptionKey& encryption_key) {
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  auto num = num_dims_and_attrs();
  uint64_t offset = 0, nbytes;

  // Store R-Tree
  gt_offsets_.rtree_ = offset;
  store_rtree(encryption_key, &nbytes);
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
    store_tile_validity_offsets(i, encryption_key, &nbytes);
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
  store_footer(encryption_key);

  // Close file
  throw_if_not_ok(resources_->vfs().close_file(fragment_metadata_uri));
}

void FragmentMetadata::store_v15_or_higher(
    const EncryptionKey& encryption_key) {
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);
  auto num = num_dims_and_attrs();
  uint64_t offset = 0, nbytes;

  // Store R-Tree
  gt_offsets_.rtree_ = offset;
  store_rtree(encryption_key, &nbytes);
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
    store_tile_validity_offsets(i, encryption_key, &nbytes);
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
  store_footer(encryption_key);

  // Close file
  throw_if_not_ok(resources_->vfs().close_file(fragment_metadata_uri));
}

void FragmentMetadata::set_num_tiles(uint64_t num_tiles) {
  if (dense_) {
    const uint64_t dense_tile_num = tile_num();
    iassert(num_tiles <= dense_tile_num);
  }

  for (auto& it : idx_map_) {
    auto i = it.second;
    iassert(num_tiles >= loaded_metadata_ptr_->tile_offsets()[i].size());

    // Get the fixed cell size
    const auto is_dim = array_schema_->is_dim(it.first);
    const auto var_size = array_schema_->var_size(it.first);
    const auto cell_size = var_size ? constants::cell_var_offset_size :
                                      array_schema_->cell_size(it.first);

    loaded_metadata_ptr_->tile_offsets()[i].resize(num_tiles, 0);
    loaded_metadata_ptr_->tile_var_offsets()[i].resize(num_tiles, 0);
    loaded_metadata_ptr_->tile_var_sizes()[i].resize(num_tiles, 0);
    loaded_metadata_ptr_->tile_validity_offsets()[i].resize(num_tiles, 0);

    // No metadata for dense coords
    if (!array_schema_->dense() || !is_dim) {
      const auto type = array_schema_->type(it.first);
      const auto cell_val_num = array_schema_->cell_val_num(it.first);

      if (TileMetadataGenerator::has_min_max_metadata(
              type, is_dim, var_size, cell_val_num)) {
        loaded_metadata_ptr_->tile_min_buffer()[i].resize(
            num_tiles * cell_size, 0);
        loaded_metadata_ptr_->tile_max_buffer()[i].resize(
            num_tiles * cell_size, 0);
      }

      if (TileMetadataGenerator::has_sum_metadata(
              type, var_size, cell_val_num)) {
        if (!var_size)
          loaded_metadata_ptr_->tile_sums()[i].resize(
              num_tiles * sizeof(uint64_t), 0);
      }

      if (array_schema_->is_nullable(it.first))
        loaded_metadata_ptr_->tile_null_counts()[i].resize(num_tiles, 0);
    }
  }

  if (!dense_) {
    loaded_metadata_ptr_->rtree().set_leaf_num(num_tiles);
    sparse_tile_num_ = num_tiles;
  }
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

std::string FragmentMetadata::encode_name(const std::string& name) const {
  if (version_ <= 7) {
    return name;
  }

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

  iassert(version_ > 8);
  const auto iter = idx_map_.find(name);
  if (iter == idx_map_.end()) {
    throw FragmentMetadataStatusException("Name " + name + " not in idx_map_");
  }

  const unsigned idx = iter->second;

  auto& attributes = array_schema_->attributes();
  for (unsigned i = 0; i < attributes.size(); ++i) {
    const std::string attr_name = attributes[i]->name();
    if (attr_name == name) {
      return "a" + std::to_string(idx);
    }
  }

  for (unsigned i = 0; i < array_schema_->dim_num(); ++i) {
    const auto& dim_name{array_schema_->dimension_ptr(i)->name()};
    if (dim_name == name) {
      const unsigned dim_idx = idx - array_schema_->attribute_num() - 1;
      return "d" + std::to_string(dim_idx);
    }
  }

  if (name == constants::coords) {
    return name;
  }

  if (name == constants::timestamps) {
    return "t";
  }

  if (name == constants::delete_timestamps) {
    return "dt";
  }

  if (name == constants::delete_condition_index) {
    return "dci";
  }

  throw FragmentMetadataStatusException(
      "Unable to locate dimension/attribute " + name);
}

URI FragmentMetadata::uri(const std::string& name) const {
  auto encoded_name = encode_name(name);
  return fragment_uri_.join_path(encoded_name + constants::file_suffix);
}

URI FragmentMetadata::var_uri(const std::string& name) const {
  auto encoded_name = encode_name(name);
  return fragment_uri_.join_path(
      encoded_name + "_var" + constants::file_suffix);
}

URI FragmentMetadata::validity_uri(const std::string& name) const {
  auto encoded_name = encode_name(name);
  return fragment_uri_.join_path(
      encoded_name + "_validity" + constants::file_suffix);
}

const std::string& FragmentMetadata::array_schema_name() const {
  return array_schema_name_;
}

const NDRange& FragmentMetadata::mbr(uint64_t tile_idx) const {
  return loaded_metadata_ptr_->rtree().leaf(tile_idx);
}

const tdb::pmr::vector<NDRange>& FragmentMetadata::mbrs() const {
  return loaded_metadata_ptr_->rtree().leaves();
}

uint64_t FragmentMetadata::tile_size(
    const std::string& name, uint64_t tile_idx) const {
  auto var_size = array_schema_->var_size(name);
  auto cell_num = this->cell_num(tile_idx);
  return (var_size) ? (cell_num + 1) * constants::cell_var_offset_size :
                      cell_num * array_schema_->cell_size(name);
}

template <typename T>
T FragmentMetadata::get_tile_min_as(
    const std::string& name, uint64_t tile_idx) const {
  const auto var_size = array_schema_->var_size(name);
  if (var_size) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata as wrong type");
  }

  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_ptr_->loaded_metadata().tile_min_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata that's not loaded");
  }

  const auto type = array_schema_->type(name);
  const auto is_dim = array_schema_->is_dim(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata that's not present");
  }

  auto size = array_schema_->cell_size(name);
  const void* min =
      &loaded_metadata_ptr_->tile_min_buffer()[idx][tile_idx * size];
  if constexpr (std::is_same_v<T, const void*>) {
    return min;
  } else {
    return *static_cast<const T*>(min);
  }
}

template <>
std::string_view FragmentMetadata::get_tile_min_as<std::string_view>(
    const std::string& name, uint64_t tile_idx) const {
  const auto type = array_schema_->type(name);
  const auto var_size = array_schema_->var_size(name);
  if (!var_size && type != Datatype::STRING_ASCII && type != Datatype::CHAR) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata as wrong type");
  }

  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_ptr_->loaded_metadata().tile_min_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata that's not loaded");
  }

  const auto is_dim = array_schema_->is_dim(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata that's not present");
  }

  using sv_size_cast = std::string_view::size_type;
  if (var_size) {
    auto tile_num = this->tile_num();
    auto offsets =
        (uint64_t*)loaded_metadata_ptr_->tile_min_buffer()[idx].data();
    auto min_offset = offsets[tile_idx];
    auto size =
        tile_idx == tile_num - 1 ?
            static_cast<sv_size_cast>(
                loaded_metadata_ptr_->tile_min_var_buffer()[idx].size() -
                min_offset) :
            static_cast<sv_size_cast>(offsets[tile_idx + 1] - min_offset);
    if (size == 0) {
      return {};
    }

    const char* min =
        &loaded_metadata_ptr_->tile_min_var_buffer()[idx][min_offset];
    return {min, size};
  } else {
    auto size = static_cast<sv_size_cast>(array_schema_->cell_size(name));
    const void* min =
        &loaded_metadata_ptr_->tile_min_buffer()[idx][tile_idx * size];
    return {static_cast<const char*>(min), size};
  }
}

template <typename T>
T FragmentMetadata::get_tile_max_as(
    const std::string& name, uint64_t tile_idx) const {
  const auto var_size = array_schema_->var_size(name);
  if (var_size) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata as wrong type");
  }

  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_ptr_->loaded_metadata().tile_max_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata that's not loaded");
  }

  const auto type = array_schema_->type(name);
  const auto is_dim = array_schema_->is_dim(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata that's not present");
  }

  auto size = array_schema_->cell_size(name);
  const void* max =
      &loaded_metadata_ptr_->tile_max_buffer()[idx][tile_idx * size];
  if constexpr (std::is_same_v<T, const void*>) {
    return max;
  } else {
    return *static_cast<const T*>(max);
  }
}

template <>
std::string_view FragmentMetadata::get_tile_max_as<std::string_view>(
    const std::string& name, uint64_t tile_idx) const {
  const auto type = array_schema_->type(name);
  const auto var_size = array_schema_->var_size(name);
  if (!var_size && type != Datatype::STRING_ASCII && type != Datatype::CHAR) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata as wrong type");
  }

  auto it = idx_map_.find(name);
  iassert(it != idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_ptr_->loaded_metadata().tile_max_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata that's not loaded");
  }

  const auto is_dim = array_schema_->is_dim(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata that's not present");
  }

  using sv_size_cast = std::string_view::size_type;
  if (var_size) {
    auto tile_num = this->tile_num();
    auto offsets =
        (uint64_t*)loaded_metadata_ptr_->tile_max_buffer()[idx].data();
    auto max_offset = offsets[tile_idx];
    auto size =
        tile_idx == tile_num - 1 ?
            static_cast<sv_size_cast>(
                loaded_metadata_ptr_->tile_max_var_buffer()[idx].size() -
                max_offset) :
            static_cast<sv_size_cast>(offsets[tile_idx + 1] - max_offset);
    if (size == 0) {
      return {};
    }

    const char* max =
        &loaded_metadata_ptr_->tile_max_var_buffer()[idx][max_offset];
    return {max, size};
  } else {
    auto size = static_cast<sv_size_cast>(array_schema_->cell_size(name));
    const void* max =
        &loaded_metadata_ptr_->tile_max_buffer()[idx][tile_idx * size];
    return {static_cast<const char*>(max), size};
  }
}

TileMetadata FragmentMetadata::get_tile_metadata(
    const std::string& name, const uint64_t tile_idx) const {
  auto var_size = array_schema_->var_size(name);
  auto is_dim = array_schema_->is_dim(name);
  auto count = cell_num(tile_idx);

  if (name == constants::count_of_rows) {
    return {count, 0, nullptr, 0, nullptr, 0, nullptr};
  }

  uint64_t null_count = 0;
  if (array_schema_->is_nullable(name)) {
    null_count = loaded_metadata_ptr_->get_tile_null_count(name, tile_idx);
  }

  unsigned dim_idx = 0;
  const NDRange* mbr = nullptr;
  if (is_dim) {
    dim_idx = array_schema_->domain().get_dimension_index(name);
    mbr = &loaded_metadata_ptr_->rtree().leaf(tile_idx);
  }

  if (var_size) {
    std::string_view min =
        is_dim ? mbr->at(dim_idx).start_str() :
                 get_tile_min_as<std::string_view>(name, tile_idx);
    std::string_view max =
        is_dim ? mbr->at(dim_idx).end_str() :
                 get_tile_max_as<std::string_view>(name, tile_idx);
    return {
        count,
        null_count,
        min.data(),
        min.size(),
        max.data(),
        max.size(),
        nullptr};
  } else {
    auto cell_size = array_schema_->cell_size(name);
    const void* min = is_dim ? mbr->at(dim_idx).start_fixed() :
                               get_tile_min_as<const void*>(name, tile_idx);
    const void* max = is_dim ? mbr->at(dim_idx).end_fixed() :
                               get_tile_max_as<const void*>(name, tile_idx);

    const auto type = array_schema_->type(name);
    const auto cell_val_num = array_schema_->cell_val_num(name);
    const void* sum = nullptr;
    if (TileMetadataGenerator::has_sum_metadata(type, false, cell_val_num)) {
      sum = loaded_metadata_ptr_->get_tile_sum(name, tile_idx);
    }

    return {count, null_count, min, cell_size, max, cell_size, sum};
  }
}

void FragmentMetadata::set_processed_conditions(
    std::vector<std::string>& processed_conditions) {
  loaded_metadata_ptr_->processed_conditions() = processed_conditions;
  loaded_metadata_ptr_->processed_conditions_set() =
      std::unordered_set<std::string>(
          processed_conditions.begin(), processed_conditions.end());
}

uint64_t FragmentMetadata::first_timestamp() const {
  return timestamp_range_.first;
}

bool FragmentMetadata::operator<(const FragmentMetadata& metadata) const {
  return (timestamp_range_.first < metadata.timestamp_range_.first) ||
         (timestamp_range_.first == metadata.timestamp_range_.first &&
          fragment_uri_ < metadata.fragment_uri_);
}

void FragmentMetadata::write_footer(Serializer& serializer) const {
  write_version(serializer);
  if (version_ >= 10) {
    write_array_schema_name(serializer);
  }
  write_dense(serializer);
  write_non_empty_domain(serializer);
  write_sparse_tile_num(serializer);
  write_last_tile_cell_num(serializer);

  if (version_ >= 14) {
    write_has_timestamps(serializer);
  }

  if (version_ >= 15) {
    write_has_delete_meta(serializer);
  }

  write_file_sizes(serializer);
  write_file_var_sizes(serializer);
  write_file_validity_sizes(serializer);
  write_generic_tile_offsets(serializer);
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

uint64_t FragmentMetadata::footer_size() const {
  return footer_size_;
}

void FragmentMetadata::get_footer_offset_and_size(
    uint64_t* offset, uint64_t* size) const {
  FragmentID fragment_id{fragment_uri_};
  auto fragment_format_version{fragment_id.array_format_version()};
  auto all_fixed = array_schema_->domain().all_dims_fixed();
  if (all_fixed && fragment_format_version < 5) {
    *size = footer_size_v3_v4();
    *offset = meta_file_size_ - *size;
  } else if (all_fixed && fragment_format_version < 7) {
    *size = footer_size_v5_v6();
    *offset = meta_file_size_ - *size;
  } else if (all_fixed && fragment_format_version < 10) {
    *size = footer_size_v7_v9();
    *offset = meta_file_size_ - *size;
  } else {
    URI fragment_metadata_uri = fragment_uri_.join_path(
        std::string(constants::fragment_metadata_filename));
    uint64_t size_offset = meta_file_size_ - sizeof(uint64_t);
    throw_if_not_ok(resources_->vfs().read_exactly(
        fragment_metadata_uri, size_offset, size, sizeof(uint64_t)));
    *offset = meta_file_size_ - *size - sizeof(uint64_t);
    resources_->stats().add_counter("read_frag_meta_size", sizeof(uint64_t));
  }
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
    iassert(array_schema_->domain().all_dims_fixed());
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

uint64_t FragmentMetadata::footer_size_v7_v9() const {
  auto dim_num = array_schema_->dim_num();
  auto num = num_dims_and_attrs();
  uint64_t domain_size = 0;

  if (non_empty_domain_.empty()) {
    // For var-sized dimensions, this function would be called only upon
    // writing the footer to storage, in which case the non-empty domain
    // would not be empty. For reading the footer from storage, the footer
    // size is explicitly stored to and retrieved from storage, so this
    // function is not called then.
    iassert(array_schema_->domain().all_dims_fixed());
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

template <class T>
std::vector<uint64_t> FragmentMetadata::compute_overlapping_tile_ids(
    const T* subarray) const {
  iassert(dense_);
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
  if (!rectangle::overlap(subarray, metadata_domain, dim_num))
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
  } while (
      rectangle::coords_in_rect(tile_coords, subarray_tile_domain, dim_num));

  // Clean up
  tdb_delete_array(subarray_tile_domain);
  tdb_delete_array(tile_coords);

  return tids;
}

template <class T>
std::vector<std::pair<uint64_t, double>>
FragmentMetadata::compute_overlapping_tile_ids_cov(const T* subarray) const {
  iassert(dense_);
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
  if (!rectangle::overlap(subarray, metadata_domain, dim_num))
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
    rectangle::overlap(
        subarray, tile_subarray, dim_num, tile_overlap, &overlap);
    iassert(overlap);
    cov = rectangle::coverage(tile_overlap, tile_subarray, dim_num);
    tile_pos = domain.get_tile_pos(metadata_domain, tile_coords);
    tids.emplace_back(tile_pos, cov);
    domain.get_next_tile_coords(subarray_tile_domain, tile_coords);
  } while (
      rectangle::coords_in_rect(tile_coords, subarray_tile_domain, dim_num));

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

void FragmentMetadata::expand_non_empty_domain(const NDRange& mbr) {
  std::lock_guard<std::mutex> lock(mtx_);

  // Case the non-empty domain is not initialized yet
  if (non_empty_domain_.empty()) {
    non_empty_domain_ = mbr;
    return;
  }

  // Expand existing non-empty domain
  array_schema_->domain().expand_ndrange(mbr, &non_empty_domain_);
}

// ===== FORMAT =====
//  bounding_coords_num (uint64_t)
//  bounding_coords_#1 (dim_type[2][n_dims])
//  bounding_coords_#2 (dim_type[2][n_dims])
//  ...
// Note: This version supports only dimensions domains with the same type
void FragmentMetadata::load_bounding_coords(Deserializer& deserializer) {
  // Get number of bounding coordinates
  uint64_t bounding_coords_num = deserializer.read<uint64_t>();

  auto coord_size{array_schema_->domain().dimension_ptr(0)->coord_size()};
  auto dim_num = array_schema_->domain().dim_num();
  uint64_t bounding_coords_size = 2 * dim_num * coord_size;
  // Skip bounding coordinates. They are not actually being used.
  deserializer.skip(bounding_coords_num * bounding_coords_size);
}

void FragmentMetadata::load_file_sizes(Deserializer& deserializer) {
  if (version_ < 5)
    load_file_sizes_v1_v4(deserializer);
  else
    load_file_sizes_v5_or_higher(deserializer);
}

// ===== FORMAT =====
// file_sizes#0 (uint64_t)
// ...
// file_sizes#attribute_num (uint64_t)
void FragmentMetadata::load_file_sizes_v1_v4(Deserializer& deserializer) {
  auto attribute_num = array_schema_->attribute_num();
  file_sizes_.resize(attribute_num + 1);
  deserializer.read(&file_sizes_[0], (attribute_num + 1) * sizeof(uint64_t));
}

// ===== FORMAT =====
// file_sizes#0 (uint64_t)
// ...
// file_sizes#{attribute_num+dim_num} (uint64_t)
void FragmentMetadata::load_file_sizes_v5_or_higher(
    Deserializer& deserializer) {
  auto num = num_dims_and_attrs();
  file_sizes_.resize(num);
  deserializer.read(&file_sizes_[0], num * sizeof(uint64_t));
}

void FragmentMetadata::load_file_var_sizes(Deserializer& deserializer) {
  if (version_ < 5)
    load_file_var_sizes_v1_v4(deserializer);
  else
    load_file_var_sizes_v5_or_higher(deserializer);
}

// ===== FORMAT =====
// file_var_sizes#0 (uint64_t)
// ...
// file_var_sizes#attribute_num (uint64_t)
void FragmentMetadata::load_file_var_sizes_v1_v4(Deserializer& deserializer) {
  auto attribute_num = array_schema_->attribute_num();
  file_var_sizes_.resize(attribute_num);
  deserializer.read(&file_var_sizes_[0], attribute_num * sizeof(uint64_t));
}

// ===== FORMAT =====
// file_var_sizes#0 (uint64_t)
// ...
// file_var_sizes#{attribute_num+dim_num} (uint64_t)
void FragmentMetadata::load_file_var_sizes_v5_or_higher(
    Deserializer& deserializer) {
  auto num = num_dims_and_attrs();
  file_var_sizes_.resize(num);
  deserializer.read(&file_var_sizes_[0], num * sizeof(uint64_t));
}

void FragmentMetadata::load_file_validity_sizes(Deserializer& deserializer) {
  if (version_ <= 6)
    return;

  auto num = num_dims_and_attrs();
  file_validity_sizes_.resize(num);
  deserializer.read(&file_validity_sizes_[0], num * sizeof(uint64_t));
}

// ===== FORMAT =====
// last_tile_cell_num (uint64_t)
void FragmentMetadata::load_last_tile_cell_num(Deserializer& deserializer) {
  // Get last tile cell number
  last_tile_cell_num_ = deserializer.read<uint64_t>();
}

// ===== FORMAT =====
// has_timestamps (char)
void FragmentMetadata::load_has_timestamps(Deserializer& deserializer) {
  // Get includes timestamps
  has_timestamps_ = deserializer.read<char>();

  // Rebuild index map
  if (has_timestamps_) {
    build_idx_map();
  }
}

// ===== FORMAT =====
// has_delete_meta (char)
void FragmentMetadata::load_has_delete_meta(Deserializer& deserializer) {
  // Get includes delete metadata
  has_delete_meta_ = deserializer.read<char>();

  // Rebuild index map
  if (has_delete_meta_) {
    build_idx_map();
  }
}

// ===== FORMAT =====
// mbr_num (uint64_t)
// mbr_#1 (void*)
// mbr_#2 (void*)
// ...
void FragmentMetadata::load_mbrs(Deserializer& deserializer) {
  // Get number of MBRs
  uint64_t mbr_num = 0;
  mbr_num = deserializer.read<uint64_t>();

  // Set leaf level
  loaded_metadata_ptr_->rtree().set_leaf_num(mbr_num);
  auto& domain{array_schema_->domain()};
  auto dim_num = domain.dim_num();
  for (uint64_t m = 0; m < mbr_num; ++m) {
    NDRange mbr(dim_num);
    for (unsigned d = 0; d < dim_num; ++d) {
      uint64_t r_size{2 * domain.dimension_ptr(d)->coord_size()};
      mbr[d] = Range(deserializer.get_ptr<char>(r_size), r_size);
    }
    loaded_metadata_ptr_->rtree().set_leaf(m, mbr);
  }

  // Build R-tree bottom-up
  if (mbr_num > 0) {
    loaded_metadata_ptr_->rtree().build_tree();
  }

  sparse_tile_num_ = mbr_num;
}

void FragmentMetadata::load_non_empty_domain(Deserializer& deserializer) {
  if (version_ <= 2) {
    load_non_empty_domain_v1_v2(deserializer);
  } else if (version_ == 3 || version_ == 4) {
    load_non_empty_domain_v3_v4(deserializer);
  } else {
    load_non_empty_domain_v5_or_higher(deserializer);
  }
}

// ===== FORMAT =====
// non_empty_domain_size (uint64_t)
// non_empty_domain (void*)
void FragmentMetadata::load_non_empty_domain_v1_v2(Deserializer& deserializer) {
  // Get domain size
  uint64_t domain_size = 0;
  domain_size = deserializer.read<uint64_t>();

  // Get non-empty domain
  if (domain_size != 0) {
    auto dim_num = array_schema_->dim_num();
    std::vector<uint8_t> temp(domain_size);
    deserializer.read(&temp[0], domain_size);
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
    array_schema_->domain().expand_to_tiles_when_no_current_domain(domain_);
  }
}

// ===== FORMAT =====
// null non_empty_domain (char)
// non_empty_domain (domain_size)
void FragmentMetadata::load_non_empty_domain_v3_v4(Deserializer& deserializer) {
  // Get null non-empty domain
  bool null_non_empty_domain = false;
  null_non_empty_domain = deserializer.read<char>();

  // Get non-empty domain
  if (!null_non_empty_domain) {
    auto dim_num = array_schema_->dim_num();
    // Note: These versions supports only dimensions domains with the same
    // type
    auto coord_size_0{array_schema_->domain().dimension_ptr(0)->coord_size()};
    auto domain_size = 2 * dim_num * coord_size_0;
    std::vector<uint8_t> temp(domain_size);
    deserializer.read(&temp[0], domain_size);
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
    array_schema_->domain().expand_to_tiles_when_no_current_domain(domain_);
  }
}

// ===== FORMAT =====
// null_non_empty_domain
// fix-sized: range(void*)
// var-sized: range_size(uint64_t) | start_range_size(uint64_t) |
// range(void*)
void FragmentMetadata::load_non_empty_domain_v5_or_higher(
    Deserializer& deserializer) {
  // Get null non-empty domain
  char null_non_empty_domain = 0;
  null_non_empty_domain = deserializer.read<char>();

  if (null_non_empty_domain == 0) {
    non_empty_domain_ = NDRectangle::deserialize_ndranges(
        deserializer, array_schema_->shared_domain());
  }

  // Get expanded domain
  if (!non_empty_domain_.empty()) {
    domain_ = non_empty_domain_;
    array_schema_->current_domain().expand_to_tiles(
        array_schema_->domain(), domain_);
  }
}

void FragmentMetadata::load_version(Deserializer& deserializer) {
  version_ = deserializer.read<uint32_t>();
}

void FragmentMetadata::load_dense(Deserializer& deserializer) {
  dense_ = deserializer.read<char>();
}

void FragmentMetadata::load_sparse_tile_num(Deserializer& deserializer) {
  sparse_tile_num_ = deserializer.read<uint64_t>();
}

void FragmentMetadata::load_generic_tile_offsets(Deserializer& deserializer) {
  if (version_ == 3 || version_ == 4) {
    load_generic_tile_offsets_v3_v4(deserializer);
  } else if (version_ >= 5 && version_ < 7) {
    load_generic_tile_offsets_v5_v6(deserializer);
  } else if (version_ >= 7 && version_ < 11) {
    load_generic_tile_offsets_v7_v10(deserializer);
  } else if (version_ == 11) {
    load_generic_tile_offsets_v11(deserializer);
  } else if (version_ >= 12 && version_ < 16) {
    load_generic_tile_offsets_v12_v15(deserializer);
  } else {
    load_generic_tile_offsets_v16_or_higher(deserializer);
  }
}

void FragmentMetadata::load_generic_tile_offsets_v3_v4(
    Deserializer& deserializer) {
  // Load R-Tree offset
  gt_offsets_.rtree_ = deserializer.read<uint64_t>();

  // Load offsets for tile offsets
  unsigned int attribute_num = array_schema_->attribute_num();
  gt_offsets_.tile_offsets_.resize(attribute_num + 1);
  deserializer.read(
      &gt_offsets_.tile_offsets_[0], (attribute_num + 1) * sizeof(uint64_t));

  // Load offsets for tile var offsets
  gt_offsets_.tile_var_offsets_.resize(attribute_num);
  deserializer.read(
      &gt_offsets_.tile_var_offsets_[0], attribute_num * sizeof(uint64_t));

  // Load offsets for tile var sizes
  gt_offsets_.tile_var_sizes_.resize(attribute_num);
  deserializer.read(
      &gt_offsets_.tile_var_sizes_[0], attribute_num * sizeof(uint64_t));
}

void FragmentMetadata::load_generic_tile_offsets_v5_v6(
    Deserializer& deserializer) {
  // Load R-Tree offset
  gt_offsets_.rtree_ = deserializer.read<uint64_t>();

  // Load offsets for tile offsets
  auto num = num_dims_and_attrs();
  gt_offsets_.tile_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_var_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  deserializer.read(&gt_offsets_.tile_var_sizes_[0], num * sizeof(uint64_t));
}

void FragmentMetadata::load_generic_tile_offsets_v7_v10(
    Deserializer& deserializer) {
  // Load R-Tree offset
  gt_offsets_.rtree_ = deserializer.read<uint64_t>();

  // Load offsets for tile offsets
  auto num = num_dims_and_attrs();
  gt_offsets_.tile_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_var_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  deserializer.read(&gt_offsets_.tile_var_sizes_[0], num * sizeof(uint64_t));

  // Load offsets for tile validity offsets
  gt_offsets_.tile_validity_offsets_.resize(num);
  deserializer.read(
      &gt_offsets_.tile_validity_offsets_[0], num * sizeof(uint64_t));
}

void FragmentMetadata::load_generic_tile_offsets_v11(
    Deserializer& deserializer) {
  // Load R-Tree offset
  gt_offsets_.rtree_ = deserializer.read<uint64_t>();

  // Load offsets for tile offsets
  auto num = num_dims_and_attrs();
  gt_offsets_.tile_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_var_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  deserializer.read(&gt_offsets_.tile_var_sizes_[0], num * sizeof(uint64_t));

  // Load offsets for tile validity offsets
  gt_offsets_.tile_validity_offsets_.resize(num);
  deserializer.read(
      &gt_offsets_.tile_validity_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile min offsets
  gt_offsets_.tile_min_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_min_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile max offsets
  gt_offsets_.tile_max_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_max_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile sum offsets
  gt_offsets_.tile_sum_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_sum_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile null count offsets
  gt_offsets_.tile_null_count_offsets_.resize(num);
  deserializer.read(
      &gt_offsets_.tile_null_count_offsets_[0], num * sizeof(uint64_t));
}

void FragmentMetadata::load_generic_tile_offsets_v12_v15(
    Deserializer& deserializer) {
  // Load R-Tree offset
  gt_offsets_.rtree_ = deserializer.read<uint64_t>();

  // Load offsets for tile offsets
  auto num = num_dims_and_attrs();
  gt_offsets_.tile_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_var_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  deserializer.read(&gt_offsets_.tile_var_sizes_[0], num * sizeof(uint64_t));

  // Load offsets for tile validity offsets
  gt_offsets_.tile_validity_offsets_.resize(num);
  deserializer.read(
      &gt_offsets_.tile_validity_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile min offsets
  gt_offsets_.tile_min_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_min_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile max offsets
  gt_offsets_.tile_max_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_max_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile sum offsets
  gt_offsets_.tile_sum_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_sum_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile null count offsets
  gt_offsets_.tile_null_count_offsets_.resize(num);
  deserializer.read(
      &gt_offsets_.tile_null_count_offsets_[0], num * sizeof(uint64_t));

  gt_offsets_.fragment_min_max_sum_null_count_offset_ =
      deserializer.read<uint64_t>();
}

void FragmentMetadata::load_generic_tile_offsets_v16_or_higher(
    Deserializer& deserializer) {
  // Load R-Tree offset
  gt_offsets_.rtree_ = deserializer.read<uint64_t>();

  // Load offsets for tile offsets
  auto num = num_dims_and_attrs();
  gt_offsets_.tile_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile var offsets
  gt_offsets_.tile_var_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_var_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile var sizes
  gt_offsets_.tile_var_sizes_.resize(num);
  deserializer.read(&gt_offsets_.tile_var_sizes_[0], num * sizeof(uint64_t));

  // Load offsets for tile validity offsets
  gt_offsets_.tile_validity_offsets_.resize(num);
  deserializer.read(
      &gt_offsets_.tile_validity_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile min offsets
  gt_offsets_.tile_min_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_min_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile max offsets
  gt_offsets_.tile_max_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_max_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile sum offsets
  gt_offsets_.tile_sum_offsets_.resize(num);
  deserializer.read(&gt_offsets_.tile_sum_offsets_[0], num * sizeof(uint64_t));

  // Load offsets for tile null count offsets
  gt_offsets_.tile_null_count_offsets_.resize(num);
  deserializer.read(
      &gt_offsets_.tile_null_count_offsets_[0], num * sizeof(uint64_t));

  gt_offsets_.fragment_min_max_sum_null_count_offset_ =
      deserializer.read<uint64_t>();

  gt_offsets_.processed_conditions_offsets_ = deserializer.read<uint64_t>();
}

void FragmentMetadata::load_array_schema_name(Deserializer& deserializer) {
  uint64_t size = 0;
  size = deserializer.read<uint64_t>();
  if (size == 0) {
    throw FragmentMetadataStatusException(
        "Cannot load array schema name; Size of schema name is zero");
  }
  array_schema_name_.resize(size);

  deserializer.read(&array_schema_name_[0], size);
}

void FragmentMetadata::load_v1_v2(
    const EncryptionKey& encryption_key,
    const std::unordered_map<std::string, shared_ptr<ArraySchema>>&
        array_schemas) {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));
  // Read metadata
  GenericTileIO tile_io(*resources_, fragment_metadata_uri);
  auto tile = tile_io.read_generic(
      0, encryption_key, resources_->config(), memory_tracker_);

  resources_->stats().add_counter("read_frag_meta_size", tile->size());

  // Pre-v10 format fragments we need to set the schema and schema name to
  // the "old" schema. This way "old" fragments are still loaded fine
  array_schema_name_ = tiledb::sm::constants::array_schema_filename;
  auto schema = array_schemas.find(array_schema_name_);
  if (schema != array_schemas.end()) {
    set_array_schema(schema->second);
  } else {
    throw FragmentMetadataStatusException(
        "Could not find schema" + array_schema_name_ +
        " in map of schemas loaded.\n" +
        "Consider reloading the array to check for new array schemas.");
  }

  // Deserialize
  Deserializer deserializer(tile->data(), tile->size());
  load_version(deserializer);
  load_non_empty_domain(deserializer);
  load_mbrs(deserializer);
  load_bounding_coords(deserializer);
  auto v1v2_meta =
      dynamic_cast<V1V2PreloadedFragmentMetadata*>(loaded_metadata_ptr_);
  if (v1v2_meta == nullptr) {
    throw std::logic_error(
        "FragmentMetadata::load_v1_v2 unable to dynamic_cast "
        "loaded_metadata_ptr_");
  }
  v1v2_meta->load_tile_offsets(deserializer);
  v1v2_meta->load_tile_var_offsets(deserializer);
  v1v2_meta->load_tile_var_sizes(deserializer);
  load_last_tile_cell_num(deserializer);
  load_file_sizes(deserializer);
  load_file_var_sizes(deserializer);
  load_file_validity_sizes(deserializer);
}

void FragmentMetadata::load_v3_or_higher(
    const EncryptionKey& encryption_key,
    Tile* fragment_metadata_tile,
    uint64_t offset,
    std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas) {
  load_footer(encryption_key, fragment_metadata_tile, offset, array_schemas);
}

void FragmentMetadata::load_footer(
    const EncryptionKey&,
    Tile* fragment_metadata_tile,
    uint64_t offset,
    std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas) {
  std::lock_guard<std::mutex> lock(mtx_);

  std::shared_ptr<Tile> tile;
  if (fragment_metadata_tile == nullptr) {
    has_consolidated_footer_ = false;
    read_file_footer(tile, &footer_offset_, &footer_size_);

    fragment_metadata_tile = tile.get();
    offset = 0;
  } else {
    footer_size_ = 0;  // adjusted at end of routine based on buffer consumed
    footer_offset_ = offset;
    has_consolidated_footer_ = true;
  }
  Deserializer deserializer(
      fragment_metadata_tile->data_as<uint8_t>() + offset,
      fragment_metadata_tile->size() - offset);
  auto starting_deserializer_size = deserializer.size();

  load_version(deserializer);

  if (version_ >= 10) {
    load_array_schema_name(deserializer);
    auto schema = array_schemas.find(array_schema_name_);
    if (schema != array_schemas.end()) {
      set_array_schema(schema->second);
    } else {
      throw FragmentMetadataStatusException(
          "Could not find schema " + array_schema_name_ +
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
      throw FragmentMetadataStatusException(
          "Could not find schema " + array_schema_name_ +
          " in map of schemas loaded.\n" +
          "Consider reloading the array to check for new array schemas.");
    }
  }
  load_dense(deserializer);
  load_non_empty_domain(deserializer);
  load_sparse_tile_num(deserializer);
  load_last_tile_cell_num(deserializer);

  if (version_ >= 14) {
    load_has_timestamps(deserializer);
  }

  if (version_ >= 15) {
    load_has_delete_meta(deserializer);
  }

  load_file_sizes(deserializer);
  load_file_var_sizes(deserializer);
  load_file_validity_sizes(deserializer);

  unsigned num = array_schema_->attribute_num() + 1 + has_timestamps_ +
                 has_delete_meta_ * 2;

  // If version < 5 we use zipped coordinates, otherwise separate
  num += (version_ >= 5) ? array_schema_->dim_num() : 0;

  loaded_metadata_ptr_->resize_offsets(num);

  load_generic_tile_offsets(deserializer);

  // If the footer_size is not set lets calculate from how much of the
  // buffer we read
  if (footer_size_ == 0) {
    footer_size_ = starting_deserializer_size - deserializer.size();
  }
}

// ===== FORMAT =====
// file_sizes#0 (uint64_t)
// ...
// file_sizes#{attribute_num+dim_num} (uint64_t)
void FragmentMetadata::write_file_sizes(Serializer& serializer) const {
  auto num = num_dims_and_attrs();
  serializer.write(&file_sizes_[0], num * sizeof(uint64_t));
}

// ===== FORMAT =====
// file_var_sizes#0 (uint64_t)
// ...
// file_var_sizes#{attribute_num+dim_num} (uint64_t)
void FragmentMetadata::write_file_var_sizes(Serializer& serializer) const {
  auto num = num_dims_and_attrs();
  serializer.write(&file_var_sizes_[0], num * sizeof(uint64_t));
}

// ===== FORMAT =====
// file_validity_sizes#0 (uint64_t)
// ...
// file_validity_sizes#{attribute_num+dim_num} (uint64_t)
void FragmentMetadata::write_file_validity_sizes(Serializer& serializer) const {
  if (version_ <= 6)
    return;

  auto num = num_dims_and_attrs();
  serializer.write(&file_validity_sizes_[0], num * sizeof(uint64_t));
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
void FragmentMetadata::write_generic_tile_offsets(
    Serializer& serializer) const {
  auto num = num_dims_and_attrs();

  // Write R-Tree offset
  serializer.write<uint64_t>(gt_offsets_.rtree_);

  // Write tile offsets
  serializer.write(&gt_offsets_.tile_offsets_[0], num * sizeof(uint64_t));

  // Write tile var offsets
  serializer.write(&gt_offsets_.tile_var_offsets_[0], num * sizeof(uint64_t));

  // Write tile var sizes
  serializer.write(&gt_offsets_.tile_var_sizes_[0], num * sizeof(uint64_t));

  // Write tile validity offsets
  if (version_ >= 7) {
    serializer.write(
        &gt_offsets_.tile_validity_offsets_[0], num * sizeof(uint64_t));
  }

  // Write tile min offsets
  if (version_ >= 11) {
    serializer.write(&gt_offsets_.tile_min_offsets_[0], num * sizeof(uint64_t));
  }

  // Write tile max offsets
  if (version_ >= 11) {
    serializer.write(&gt_offsets_.tile_max_offsets_[0], num * sizeof(uint64_t));
  }

  // Write tile sum offsets
  if (version_ >= 11) {
    serializer.write(&gt_offsets_.tile_sum_offsets_[0], num * sizeof(uint64_t));
  }

  // Write tile null count offsets
  if (version_ >= 11) {
    serializer.write(
        &gt_offsets_.tile_null_count_offsets_[0], num * sizeof(uint64_t));
  }

  if (version_ >= 11) {
    serializer.write<uint64_t>(
        gt_offsets_.fragment_min_max_sum_null_count_offset_);
  }

  if (version_ >= 16) {
    serializer.write<uint64_t>(gt_offsets_.processed_conditions_offsets_);
  }
}

void FragmentMetadata::write_array_schema_name(Serializer& serializer) const {
  uint64_t size = array_schema_name_.size();
  if (size == 0) {
    throw FragmentMetadataStatusException(
        "Cannot write array schema name; Size of schema name is zero");
  }
  serializer.write<uint64_t>(size);
  serializer.write(array_schema_name_.c_str(), size);
}

// ===== FORMAT =====
// last_tile_cell_num(uint64_t)
void FragmentMetadata::write_last_tile_cell_num(Serializer& serializer) const {
  uint64_t cell_num_per_tile = dense_ ?
                                   array_schema_->domain().cell_num_per_tile() :
                                   array_schema_->capacity();

  // Handle the case of zero
  uint64_t last_tile_cell_num =
      (last_tile_cell_num_ == 0) ? cell_num_per_tile : last_tile_cell_num_;

  serializer.write<uint64_t>(last_tile_cell_num);
}

void FragmentMetadata::store_rtree(
    const EncryptionKey& encryption_key, uint64_t* nbytes) {
  auto rtree_tile = write_rtree();
  write_generic_tile_to_file(encryption_key, rtree_tile, nbytes);
  resources_->stats().add_counter("write_rtree_size", *nbytes);
}

shared_ptr<WriterTile> FragmentMetadata::write_rtree() {
  loaded_metadata_ptr_->rtree().build_tree();
  SizeComputationSerializer size_computation_serializer;
  loaded_metadata_ptr_->rtree().serialize(size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  loaded_metadata_ptr_->rtree().serialize(serializer);

  return tile;
}

// ===== FORMAT =====
// null_non_empty_domain(char)
// fix-sized: range(void*)
// var-sized: range_size(uint64_t) | start_range_size(uint64_t) |
// range(void*)
// ...
void FragmentMetadata::write_non_empty_domain(Serializer& serializer) const {
  // Write null_non_empty_domain
  auto null_non_empty_domain = (char)non_empty_domain_.empty();
  serializer.write<char>(null_non_empty_domain);

  // Write domain size
  auto& domain = array_schema_->domain();
  auto dim_num = domain.dim_num();
  if (non_empty_domain_.empty()) {
    // Applicable only to homogeneous domains with fixed-sized types
    iassert(domain.all_dims_fixed());
    iassert(domain.all_dims_same_type());
    auto domain_size{2 * dim_num * domain.dimension_ptr(0)->coord_size()};

    // Write domain (dummy values)
    std::vector<uint8_t> d(domain_size, 0);
    serializer.write(&d[0], domain_size);
  } else {
    NDRectangle(
        memory_tracker_, array_schema_->shared_domain(), non_empty_domain_)
        .serialize(serializer);
  }
}

shared_ptr<Tile> FragmentMetadata::read_generic_tile_from_file(
    const EncryptionKey& encryption_key, uint64_t offset) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  // Read metadata
  GenericTileIO tile_io(*resources_, fragment_metadata_uri);
  return tile_io.read_generic(
      offset, encryption_key, resources_->config(), memory_tracker_);
}

void FragmentMetadata::read_file_footer(
    std::shared_ptr<Tile>& tile,
    uint64_t* footer_offset,
    uint64_t* footer_size) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  // Get footer offset
  get_footer_offset_and_size(footer_offset, footer_size);

  tile = Tile::from_generic(*footer_size, memory_tracker_);

  resources_->stats().add_counter("read_frag_meta_size", *footer_size);

  if (memory_tracker_ != nullptr &&
      !memory_tracker_->take_memory(*footer_size, MemoryType::FOOTER)) {
    throw FragmentMetadataStatusException(
        "Cannot load file footer; Insufficient memory budget; Needed " +
        std::to_string(*footer_size) + " but only had " +
        std::to_string(memory_tracker_->get_memory_available()) +
        " from budget " + std::to_string(memory_tracker_->get_memory_budget()));
  }

  // Read footer
  throw_if_not_ok(resources_->vfs().read_exactly(
      fragment_metadata_uri,
      *footer_offset,
      tile->data_as<uint8_t>(),
      *footer_size));
}

void FragmentMetadata::write_generic_tile_to_file(
    const EncryptionKey& encryption_key,
    shared_ptr<WriterTile> tile,
    uint64_t* nbytes) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  GenericTileIO tile_io(*resources_, fragment_metadata_uri);
  tile_io.write_generic(tile, encryption_key, nbytes);
}

void FragmentMetadata::write_footer_to_file(shared_ptr<WriterTile> tile) const {
  URI fragment_metadata_uri = fragment_uri_.join_path(
      std::string(constants::fragment_metadata_filename));

  uint64_t size = tile->size();
  resources_->vfs().write(fragment_metadata_uri, tile->data(), tile->size());

  // Write the size in the end if there is at least one var-sized dimension
  if (!array_schema_->domain().all_dims_fixed() || version_ >= 10) {
    resources_->vfs().write(fragment_metadata_uri, &size, sizeof(uint64_t));
  }
}

void FragmentMetadata::store_tile_offsets(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_offsets(idx, size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  write_tile_offsets(idx, serializer);
  write_generic_tile_to_file(encryption_key, tile, nbytes);

  resources_->stats().add_counter("write_tile_offsets_size", *nbytes);
}

void FragmentMetadata::write_tile_offsets(
    unsigned idx, Serializer& serializer) {
  // Write number of tile offsets
  uint64_t tile_offsets_num = loaded_metadata_ptr_->tile_offsets()[idx].size();
  serializer.write<uint64_t>(tile_offsets_num);

  // Write tile offsets
  if (tile_offsets_num != 0) {
    serializer.write(
        &loaded_metadata_ptr_->tile_offsets()[idx][0],
        tile_offsets_num * sizeof(uint64_t));
  }
}

void FragmentMetadata::store_tile_var_offsets(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_var_offsets(idx, size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  write_tile_var_offsets(idx, serializer);
  write_generic_tile_to_file(encryption_key, tile, nbytes);

  resources_->stats().add_counter("write_tile_var_offsets_size", *nbytes);
}

void FragmentMetadata::write_tile_var_offsets(
    unsigned idx, Serializer& serializer) {
  // Write tile offsets for each attribute
  // Write number of offsets
  uint64_t tile_var_offsets_num =
      loaded_metadata_ptr_->tile_var_offsets()[idx].size();
  serializer.write<uint64_t>(tile_var_offsets_num);

  // Write tile offsets
  if (tile_var_offsets_num != 0) {
    serializer.write(
        &loaded_metadata_ptr_->tile_var_offsets()[idx][0],
        tile_var_offsets_num * sizeof(uint64_t));
  }
}

void FragmentMetadata::store_tile_var_sizes(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_var_sizes(idx, size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  write_tile_var_sizes(idx, serializer);
  write_generic_tile_to_file(encryption_key, tile, nbytes);

  resources_->stats().add_counter("write_tile_var_sizes_size", *nbytes);
}

void FragmentMetadata::write_tile_var_sizes(
    unsigned idx, Serializer& serializer) {
  // Write number of sizes
  uint64_t tile_var_sizes_num =
      loaded_metadata_ptr_->tile_var_sizes()[idx].size();
  serializer.write<uint64_t>(tile_var_sizes_num);

  // Write tile sizes
  if (tile_var_sizes_num != 0) {
    serializer.write(
        &loaded_metadata_ptr_->tile_var_sizes()[idx][0],
        tile_var_sizes_num * sizeof(uint64_t));
  }
}

void FragmentMetadata::store_tile_validity_offsets(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_validity_offsets(idx, size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  write_tile_validity_offsets(idx, serializer);
  write_generic_tile_to_file(encryption_key, tile, nbytes);

  resources_->stats().add_counter("write_tile_validity_offsets_size", *nbytes);
}

void FragmentMetadata::write_tile_validity_offsets(
    unsigned idx, Serializer& serializer) {
  // Write number of tile offsets
  uint64_t tile_validity_offsets_num =
      loaded_metadata_ptr_->tile_validity_offsets()[idx].size();
  serializer.write<uint64_t>(tile_validity_offsets_num);

  // Write tile validity offsets
  if (tile_validity_offsets_num != 0) {
    serializer.write(
        &loaded_metadata_ptr_->tile_validity_offsets()[idx][0],
        tile_validity_offsets_num * sizeof(uint64_t));
  }
}

void FragmentMetadata::store_tile_mins(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_mins(idx, size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  write_tile_mins(idx, serializer);
  write_generic_tile_to_file(encryption_key, tile, nbytes);

  resources_->stats().add_counter("write_mins_size", *nbytes);
}

void FragmentMetadata::write_tile_mins(unsigned idx, Serializer& serializer) {
  // Write size of buffer
  uint64_t tile_mins_buffer_size =
      loaded_metadata_ptr_->tile_min_buffer()[idx].size();
  serializer.write<uint64_t>(tile_mins_buffer_size);

  // Write size of buffer var
  uint64_t tile_mins_var_buffer_size =
      loaded_metadata_ptr_->tile_min_var_buffer()[idx].size();
  serializer.write<uint64_t>(tile_mins_var_buffer_size);

  // Write tile buffer
  if (tile_mins_buffer_size != 0) {
    serializer.write(
        &loaded_metadata_ptr_->tile_min_buffer()[idx][0],
        tile_mins_buffer_size);
  }

  // Write tile var buffer
  if (tile_mins_var_buffer_size != 0) {
    serializer.write(
        &loaded_metadata_ptr_->tile_min_var_buffer()[idx][0],
        tile_mins_var_buffer_size);
  }
}

void FragmentMetadata::store_tile_maxs(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_maxs(idx, size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  write_tile_maxs(idx, serializer);
  write_generic_tile_to_file(encryption_key, tile, nbytes);

  resources_->stats().add_counter("write_maxs_size", *nbytes);
}

void FragmentMetadata::write_tile_maxs(unsigned idx, Serializer& serializer) {
  // Write size of buffer
  uint64_t tile_maxs_buffer_size =
      loaded_metadata_ptr_->tile_max_buffer()[idx].size();
  serializer.write<uint64_t>(tile_maxs_buffer_size);

  // Write size of buffer var
  uint64_t tile_maxs_var_buffer_size =
      loaded_metadata_ptr_->tile_max_var_buffer()[idx].size();
  serializer.write<uint64_t>(tile_maxs_var_buffer_size);

  // Write tile buffer
  if (tile_maxs_buffer_size != 0) {
    serializer.write(
        &loaded_metadata_ptr_->tile_max_buffer()[idx][0],
        tile_maxs_buffer_size);
  }

  // Write tile var buffer
  if (tile_maxs_var_buffer_size != 0) {
    serializer.write(
        &loaded_metadata_ptr_->tile_max_var_buffer()[idx][0],
        tile_maxs_var_buffer_size);
  }
}

void FragmentMetadata::store_tile_sums(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_sums(idx, size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  write_tile_sums(idx, serializer);
  write_generic_tile_to_file(encryption_key, tile, nbytes);

  resources_->stats().add_counter("write_sums_size", *nbytes);
}

void FragmentMetadata::write_tile_sums(unsigned idx, Serializer& serializer) {
  // Write number of tile sums
  uint64_t tile_sums_num =
      loaded_metadata_ptr_->tile_sums()[idx].size() / sizeof(uint64_t);
  serializer.write<uint64_t>(tile_sums_num);

  // Write tile sums
  if (tile_sums_num != 0) {
    serializer.write(
        loaded_metadata_ptr_->tile_sums()[idx].data(),
        tile_sums_num * sizeof(uint64_t));
  }
}

void FragmentMetadata::store_tile_null_counts(
    unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  SizeComputationSerializer size_computation_serializer;
  write_tile_null_counts(idx, size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  write_tile_null_counts(idx, serializer);
  write_generic_tile_to_file(encryption_key, tile, nbytes);

  resources_->stats().add_counter("write_null_counts_size", *nbytes);
}

void FragmentMetadata::write_tile_null_counts(
    unsigned idx, Serializer& serializer) {
  // Write number of tile null counts
  uint64_t tile_null_counts_num =
      loaded_metadata_ptr_->tile_null_counts()[idx].size();
  serializer.write<uint64_t>(tile_null_counts_num);

  // Write tile null counts
  if (tile_null_counts_num != 0) {
    serializer.write(
        &loaded_metadata_ptr_->tile_null_counts()[idx][0],
        tile_null_counts_num * sizeof(uint64_t));
  }
}

void FragmentMetadata::store_fragment_min_max_sum_null_count(
    uint64_t num, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  Buffer buff;

  auto serialize_data = [&](Serializer& serializer) {
    // Store all attributes.
    for (unsigned int i = 0; i < num; ++i) {
      // Store min.
      uint64_t min_size = loaded_metadata_ptr_->fragment_mins()[i].size();
      serializer.write<uint64_t>(min_size);

      serializer.write(
          loaded_metadata_ptr_->fragment_mins()[i].data(), min_size);

      // Store max.
      uint64_t max_size = loaded_metadata_ptr_->fragment_maxs()[i].size();
      serializer.write<uint64_t>(max_size);

      serializer.write(
          loaded_metadata_ptr_->fragment_maxs()[i].data(), max_size);

      // Store sum.
      serializer.write<uint64_t>(loaded_metadata_ptr_->fragment_sums()[i]);

      // Store null count.
      serializer.write<uint64_t>(
          loaded_metadata_ptr_->fragment_null_counts()[i]);
    }
  };

  SizeComputationSerializer size_computation_serializer;
  serialize_data(size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  serialize_data(serializer);
  write_generic_tile_to_file(encryption_key, tile, nbytes);

  resources_->stats().add_counter("write_null_counts_size", *nbytes);
}

void FragmentMetadata::store_processed_conditions(
    const EncryptionKey& encryption_key, uint64_t* nbytes) {
  auto serialize_processed_conditions = [this](Serializer& serializer) {
    // Store num conditions.
    uint64_t num = loaded_metadata_ptr_->processed_conditions().size();
    serializer.write<uint64_t>(num);

    for (auto& processed_condition :
         loaded_metadata_ptr_->processed_conditions()) {
      uint64_t size = processed_condition.size();
      serializer.write<uint64_t>(size);

      serializer.write(processed_condition.data(), processed_condition.size());
    }
  };
  SizeComputationSerializer size_computation_serializer;
  serialize_processed_conditions(size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  serialize_processed_conditions(serializer);
  write_generic_tile_to_file(encryption_key, tile, nbytes);

  resources_->stats().add_counter("write_processed_conditions_size", *nbytes);
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
      auto min_values = static_cast<T*>(static_cast<void*>(
          loaded_metadata_ptr_->tile_min_buffer()[idx].data()));
      auto max_values = static_cast<T*>(static_cast<void*>(
          loaded_metadata_ptr_->tile_max_buffer()[idx].data()));
      auto& null_count_values = loaded_metadata_ptr_->tile_null_counts()[idx];
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
      loaded_metadata_ptr_->fragment_mins()[idx].resize(sizeof(T));
      loaded_metadata_ptr_->fragment_maxs()[idx].resize(sizeof(T));
      memcpy(
          loaded_metadata_ptr_->fragment_mins()[idx].data(), &min, sizeof(T));
      memcpy(
          loaded_metadata_ptr_->fragment_maxs()[idx].data(), &max, sizeof(T));
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
  auto min_values = loaded_metadata_ptr_->tile_min_buffer()[idx].data();
  auto max_values = loaded_metadata_ptr_->tile_max_buffer()[idx].data();
  auto& null_count_values = loaded_metadata_ptr_->tile_null_counts()[idx];
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
    loaded_metadata_ptr_->fragment_mins()[idx].resize(cell_val_num);
    memcpy(
        loaded_metadata_ptr_->fragment_mins()[idx].data(), min, cell_val_num);
  }

  if (max != nullptr) {
    loaded_metadata_ptr_->fragment_maxs()[idx].resize(cell_val_num);
    memcpy(
        loaded_metadata_ptr_->fragment_maxs()[idx].data(), max, cell_val_num);
  }
}

template <>
void FragmentMetadata::compute_fragment_sum<int64_t>(
    const uint64_t idx, const bool nullable) {
  // Zero sum.
  int64_t sum_data = 0;

  // Get data and tile num.
  auto values = static_cast<int64_t*>(
      static_cast<void*>(loaded_metadata_ptr_->tile_sums()[idx].data()));
  auto& null_count_values = loaded_metadata_ptr_->tile_null_counts()[idx];
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
  memcpy(
      &loaded_metadata_ptr_->fragment_sums()[idx], &sum_data, sizeof(int64_t));
}

template <>
void FragmentMetadata::compute_fragment_sum<uint64_t>(
    const uint64_t idx, const bool nullable) {
  // Zero sum.
  uint64_t sum_data = 0;

  // Get data and tile num.
  auto values = static_cast<uint64_t*>(
      static_cast<void*>(loaded_metadata_ptr_->tile_sums()[idx].data()));
  auto& null_count_values = loaded_metadata_ptr_->tile_null_counts()[idx];
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
  memcpy(
      &loaded_metadata_ptr_->fragment_sums()[idx], &sum_data, sizeof(uint64_t));
}

template <>
void FragmentMetadata::compute_fragment_sum<double>(
    const uint64_t idx, const bool nullable) {
  // Zero sum.
  double sum_data = 0;

  // Get data and tile num.
  auto values = static_cast<double*>(
      static_cast<void*>(loaded_metadata_ptr_->tile_sums()[idx].data()));
  auto& null_count_values = loaded_metadata_ptr_->tile_null_counts()[idx];
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
  memcpy(
      &loaded_metadata_ptr_->fragment_sums()[idx], &sum_data, sizeof(double));
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
  auto min_offsets = static_cast<uint64_t*>(
      static_cast<void*>(loaded_metadata_ptr_->tile_min_buffer()[idx].data()));
  auto max_offsets = static_cast<uint64_t*>(
      static_cast<void*>(loaded_metadata_ptr_->tile_max_buffer()[idx].data()));
  auto min_values = loaded_metadata_ptr_->tile_min_var_buffer()[idx].data();
  auto max_values = loaded_metadata_ptr_->tile_max_var_buffer()[idx].data();
  auto& null_count_values = loaded_metadata_ptr_->tile_null_counts()[idx];
  auto tile_num = this->tile_num();

  // Process tile by tile.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      auto min_value = min_values + min_offsets[t];
      auto min_value_size =
          t == tile_num - 1 ?
              loaded_metadata_ptr_->tile_min_var_buffer()[idx].size() -
                  min_offsets[t] :
              min_offsets[t + 1] - min_offsets[t];
      auto max_value = max_values + max_offsets[t];
      auto max_value_size =
          t == tile_num - 1 ?
              loaded_metadata_ptr_->tile_max_var_buffer()[idx].size() -
                  max_offsets[t] :
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
    loaded_metadata_ptr_->fragment_mins()[idx].resize(min_size);
    memcpy(loaded_metadata_ptr_->fragment_mins()[idx].data(), min, min_size);
  }

  if (max != nullptr) {
    loaded_metadata_ptr_->fragment_maxs()[idx].resize(max_size);
    memcpy(loaded_metadata_ptr_->fragment_maxs()[idx].data(), max, max_size);
  }
}

void FragmentMetadata::write_version(Serializer& serializer) const {
  serializer.write<uint32_t>(version_);
}

void FragmentMetadata::write_dense(Serializer& serializer) const {
  serializer.write<char>(dense_);
}

void FragmentMetadata::write_sparse_tile_num(Serializer& serializer) const {
  serializer.write<uint64_t>(sparse_tile_num_);
}

void FragmentMetadata::write_has_timestamps(Serializer& serializer) const {
  serializer.write<char>(has_timestamps_);
}

void FragmentMetadata::write_has_delete_meta(Serializer& serializer) const {
  serializer.write<char>(has_delete_meta_);
}

void FragmentMetadata::store_footer(const EncryptionKey&) {
  SizeComputationSerializer size_computation_serializer;
  write_footer(size_computation_serializer);
  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  write_footer(serializer);
  write_footer_to_file(tile);

  resources_->stats().add_counter("write_frag_meta_footer_size", tile->size());
}

void FragmentMetadata::clean_up() {
  auto fragment_metadata_uri =
      fragment_uri_.join_path(constants::fragment_metadata_filename);

  throw_if_not_ok(resources_->vfs().close_file(fragment_metadata_uri));
  resources_->vfs().remove_file(fragment_metadata_uri);
}

const shared_ptr<const ArraySchema>& FragmentMetadata::array_schema() const {
  return array_schema_;
}

void FragmentMetadata::build_idx_map() {
  idx_map_.clear();

  auto& attributes = array_schema_->attributes();
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
template int8_t FragmentMetadata::get_tile_min_as<int8_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint8_t FragmentMetadata::get_tile_min_as<uint8_t>(
    const std::string& name, uint64_t tile_idx) const;
template int16_t FragmentMetadata::get_tile_min_as<int16_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint16_t FragmentMetadata::get_tile_min_as<uint16_t>(
    const std::string& name, uint64_t tile_idx) const;
template int32_t FragmentMetadata::get_tile_min_as<int32_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint32_t FragmentMetadata::get_tile_min_as<uint32_t>(
    const std::string& name, uint64_t tile_idx) const;
template int64_t FragmentMetadata::get_tile_min_as<int64_t>(
    const std::string& name, uint64_t tile_idx) const;
template char FragmentMetadata::get_tile_min_as<char>(
    const std::string& name, uint64_t tile_idx) const;
template uint64_t FragmentMetadata::get_tile_min_as<uint64_t>(
    const std::string& name, uint64_t tile_idx) const;
template float FragmentMetadata::get_tile_min_as<float>(
    const std::string& name, uint64_t tile_idx) const;
template double FragmentMetadata::get_tile_min_as<double>(
    const std::string& name, uint64_t tile_idx) const;
template std::byte FragmentMetadata::get_tile_min_as<std::byte>(
    const std::string& name, uint64_t tile_idx) const;
template int8_t FragmentMetadata::get_tile_max_as<int8_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint8_t FragmentMetadata::get_tile_max_as<uint8_t>(
    const std::string& name, uint64_t tile_idx) const;
template int16_t FragmentMetadata::get_tile_max_as<int16_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint16_t FragmentMetadata::get_tile_max_as<uint16_t>(
    const std::string& name, uint64_t tile_idx) const;
template int32_t FragmentMetadata::get_tile_max_as<int32_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint32_t FragmentMetadata::get_tile_max_as<uint32_t>(
    const std::string& name, uint64_t tile_idx) const;
template int64_t FragmentMetadata::get_tile_max_as<int64_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint64_t FragmentMetadata::get_tile_max_as<uint64_t>(
    const std::string& name, uint64_t tile_idx) const;
template float FragmentMetadata::get_tile_max_as<float>(
    const std::string& name, uint64_t tile_idx) const;
template double FragmentMetadata::get_tile_max_as<double>(
    const std::string& name, uint64_t tile_idx) const;
template std::byte FragmentMetadata::get_tile_max_as<std::byte>(
    const std::string& name, uint64_t tile_idx) const;
template char FragmentMetadata::get_tile_max_as<char>(
    const std::string& name, uint64_t tile_idx) const;

}  // namespace tiledb::sm
