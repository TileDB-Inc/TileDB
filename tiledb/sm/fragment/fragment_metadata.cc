/**
 * @file   book_keeping.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

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
    const ArraySchema* array_schema, bool dense, const URI& fragment_uri)
    : array_schema_(array_schema)
    , dense_(dense)
    , fragment_uri_(fragment_uri) {
  cell_num_in_domain_ = 0;
  domain_ = nullptr;
  non_empty_domain_ = nullptr;
  std::memcpy(version_, constants::version, sizeof(version_));
}

FragmentMetadata::~FragmentMetadata() {
  if (domain_ != nullptr)
    std::free(domain_);

  if (non_empty_domain_ != nullptr)
    std::free(non_empty_domain_);

  auto mbr_num = (uint64_t)mbrs_.size();
  for (uint64_t i = 0; i < mbr_num; ++i)
    if (mbrs_[i] != nullptr)
      std::free(mbrs_[i]);

  auto bounding_coords_num = (uint64_t)bounding_coords_.size();
  for (uint64_t i = 0; i < bounding_coords_num; ++i)
    if (bounding_coords_[i] != nullptr)
      std::free(bounding_coords_[i]);
}

/* ****************************** */
/*             ACCESSORS          */
/* ****************************** */

void FragmentMetadata::append_bounding_coords(const void* bounding_coords) {
  // For easy reference
  uint64_t bounding_coords_size = 2 * array_schema_->coords_size();

  // Copy and append MBR
  void* new_bounding_coords = std::malloc(bounding_coords_size);
  std::memcpy(new_bounding_coords, bounding_coords, bounding_coords_size);
  bounding_coords_.push_back(new_bounding_coords);
}

Status FragmentMetadata::append_mbr(const void* mbr) {
  switch (array_schema_->coords_type()) {
    case Datatype::INT8:
      return append_mbr<int8_t>(static_cast<const int8_t*>(mbr));
    case Datatype::UINT8:
      return append_mbr<uint8_t>(static_cast<const uint8_t*>(mbr));
    case Datatype::INT16:
      return append_mbr<int16_t>(static_cast<const int16_t*>(mbr));
    case Datatype::UINT16:
      return append_mbr<uint16_t>(static_cast<const uint16_t*>(mbr));
    case Datatype::INT32:
      return append_mbr<int>(static_cast<const int*>(mbr));
    case Datatype::UINT32:
      return append_mbr<unsigned>(static_cast<const unsigned*>(mbr));
    case Datatype::INT64:
      return append_mbr<int64_t>(static_cast<const int64_t*>(mbr));
    case Datatype::UINT64:
      return append_mbr<uint64_t>(static_cast<const uint64_t*>(mbr));
    case Datatype::FLOAT32:
      return append_mbr<float>(static_cast<const float*>(mbr));
    case Datatype::FLOAT64:
      return append_mbr<double>(static_cast<const double*>(mbr));
    default:
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot append mbr; Unsupported coordinates type"));
  }
}

template <class T>
Status FragmentMetadata::append_mbr(const void* mbr) {
  // For easy reference
  uint64_t mbr_size = 2 * array_schema_->coords_size();

  // Copy and append MBR
  void* new_mbr = std::malloc(mbr_size);
  std::memcpy(new_mbr, mbr, mbr_size);
  mbrs_.push_back(new_mbr);

  return expand_non_empty_domain(static_cast<const T*>(mbr));
}

void FragmentMetadata::append_tile_offset(
    unsigned int attribute_id, uint64_t step) {
  tile_offsets_[attribute_id].push_back(next_tile_offsets_[attribute_id]);
  uint64_t new_offset = tile_offsets_[attribute_id].back() + step;
  next_tile_offsets_[attribute_id] = new_offset;
}

void FragmentMetadata::append_tile_var_offset(
    unsigned int attribute_id, uint64_t step) {
  tile_var_offsets_[attribute_id].push_back(
      next_tile_var_offsets_[attribute_id]);
  uint64_t new_offset = tile_var_offsets_[attribute_id].back() + step;
  next_tile_var_offsets_[attribute_id] = new_offset;
}

void FragmentMetadata::append_tile_var_size(
    unsigned int attribute_id, uint64_t size) {
  tile_var_sizes_[attribute_id].push_back(size);
}

const std::vector<void*>& FragmentMetadata::bounding_coords() const {
  return bounding_coords_;
}

uint64_t FragmentMetadata::cell_num(uint64_t tile_pos) const {
  if (dense_)
    return array_schema_->domain()->cell_num_per_tile();

  uint64_t tile_num = this->tile_num();
  if (tile_pos != tile_num - 1)
    return array_schema_->capacity();

  return last_tile_cell_num();
}

uint64_t FragmentMetadata::cell_num_in_domain() const {
  return cell_num_in_domain_;
}

template <class T>
Status FragmentMetadata::compute_max_read_buffer_sizes(
    const T* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const {
  if (dense_)
    return compute_max_read_buffer_sizes_dense(
        subarray, attributes, attribute_num, buffer_num, buffer_sizes);
  return compute_max_read_buffer_sizes_sparse(
      subarray, attributes, attribute_num, buffer_num, buffer_sizes);
}

template <class T>
Status FragmentMetadata::compute_max_read_buffer_sizes_dense(
    const T* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const {
  // Zero out all buffer sizes
  for (unsigned i = 0; i < buffer_num; ++i)
    buffer_sizes[i] = 0;

  // Calculate the ids of all tiles overlapping with subarray
  auto tids = compute_overlapping_tile_ids(subarray);

  // Calculate attribute ids and var sizes
  std::vector<unsigned> attribute_ids;
  std::vector<bool> var_sizes;
  unsigned aid;
  for (unsigned i = 0; i < attribute_num; ++i) {
    RETURN_NOT_OK(array_schema_->attribute_id(attributes[i], &aid));
    attribute_ids.emplace_back(aid);
    var_sizes.push_back(array_schema_->var_size(aid));
  }

  // Compute buffer sizes
  unsigned bid;
  for (auto& tid : tids) {
    bid = 0;
    for (unsigned i = 0; i < attribute_num; ++i) {
      if (var_sizes[i]) {
        auto cell_num = this->cell_num(tid);
        buffer_sizes[bid++] += cell_num * constants::cell_var_offset_size;
        buffer_sizes[bid++] += tile_var_sizes_[attribute_ids[i]][tid];
      } else {
        buffer_sizes[bid++] +=
            cell_num(tid) * array_schema_->cell_size(attribute_ids[i]);
      }
    }
    assert(bid == buffer_num);
  }

  return Status::Ok();
}

template <class T>
Status FragmentMetadata::compute_max_read_buffer_sizes_sparse(
    const T* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const {
  for (unsigned i = 0; i < buffer_num; ++i)
    buffer_sizes[i] = 0;

  // Calculate attribute ids and var sizes
  std::vector<unsigned> attribute_ids;
  std::vector<bool> var_sizes;
  unsigned aid;
  for (unsigned i = 0; i < attribute_num; ++i) {
    RETURN_NOT_OK(array_schema_->attribute_id(attributes[i], &aid));
    attribute_ids.emplace_back(aid);
    var_sizes.push_back(array_schema_->var_size(aid));
  }

  unsigned bid, tid = 0;
  auto dim_num = array_schema_->dim_num();
  for (auto& mbr : mbrs_) {
    bid = 0;
    if (utils::overlap(static_cast<T*>(mbr), subarray, dim_num)) {
      for (unsigned i = 0; i < attribute_num; ++i) {
        if (var_sizes[i]) {
          auto cell_num = this->cell_num(tid);
          buffer_sizes[bid++] += cell_num * constants::cell_var_offset_size;
          buffer_sizes[bid++] += tile_var_sizes_[attribute_ids[i]][tid];
        } else {
          buffer_sizes[bid++] +=
              cell_num(tid) * array_schema_->cell_size(attribute_ids[i]);
        }
      }
      assert(bid == buffer_num);
    }
    tid++;
  }

  return Status::Ok();
}

bool FragmentMetadata::dense() const {
  return dense_;
}

Status FragmentMetadata::deserialize(ConstBuffer* buf) {
  RETURN_NOT_OK(load_version(buf));
  RETURN_NOT_OK(load_non_empty_domain(buf));
  RETURN_NOT_OK(load_mbrs(buf));
  RETURN_NOT_OK(load_bounding_coords(buf));
  RETURN_NOT_OK(load_tile_offsets(buf));
  RETURN_NOT_OK(load_tile_var_offsets(buf));
  RETURN_NOT_OK(load_tile_var_sizes(buf));
  RETURN_NOT_OK(load_last_tile_cell_num(buf));
  RETURN_NOT_OK(load_file_sizes(buf));
  RETURN_NOT_OK(load_file_var_sizes(buf));

  return Status::Ok();
}

const void* FragmentMetadata::domain() const {
  return domain_;
}

uint64_t FragmentMetadata::file_sizes(unsigned int attribute_id) const {
  assert(attribute_id < file_sizes_.size());
  return file_sizes_[attribute_id];
}

uint64_t FragmentMetadata::file_var_sizes(unsigned int attribute_id) const {
  assert(attribute_id < file_var_sizes_.size());
  return file_var_sizes_[attribute_id];
}

const URI& FragmentMetadata::fragment_uri() const {
  return fragment_uri_;
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

    // Set expanded domain
    domain_ = std::malloc(domain_size);
    std::memcpy(domain_, non_empty_domain_, domain_size);
    domain->expand_domain(domain_);

    // Compute cell num in expanded domain
    cell_num_in_domain_ = domain->cell_num(domain_);
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

const std::vector<void*>& FragmentMetadata::mbrs() const {
  return mbrs_;
}

const void* FragmentMetadata::non_empty_domain() const {
  return non_empty_domain_;
}

Status FragmentMetadata::serialize(Buffer* buf) {
  RETURN_NOT_OK(write_version(buf));
  RETURN_NOT_OK(write_non_empty_domain(buf));
  RETURN_NOT_OK(write_mbrs(buf));
  RETURN_NOT_OK(write_bounding_coords(buf));
  RETURN_NOT_OK(write_tile_offsets(buf));
  RETURN_NOT_OK(write_tile_var_offsets(buf));
  RETURN_NOT_OK(write_tile_var_sizes(buf));
  RETURN_NOT_OK(write_last_tile_cell_num(buf));
  RETURN_NOT_OK(write_file_sizes(buf));
  RETURN_NOT_OK(write_file_var_sizes(buf));

  return Status::Ok();
}

void FragmentMetadata::set_last_tile_cell_num(uint64_t cell_num) {
  last_tile_cell_num_ = cell_num;
}

uint64_t FragmentMetadata::tile_num() const {
  if (dense_)
    return array_schema_->domain()->tile_num(domain_);

  return (uint64_t)mbrs_.size();
}

const std::vector<std::vector<uint64_t>>& FragmentMetadata::tile_offsets()
    const {
  return tile_offsets_;
}

const std::vector<std::vector<uint64_t>>& FragmentMetadata::tile_var_offsets()
    const {
  return tile_var_offsets_;
}

const std::vector<std::vector<uint64_t>>& FragmentMetadata::tile_var_sizes()
    const {
  return tile_var_sizes_;
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

template <class T>
std::vector<uint64_t> FragmentMetadata::compute_overlapping_tile_ids(
    const T* subarray) const {
  assert(dense_);
  std::vector<uint64_t> tids;
  auto dim_num = array_schema_->dim_num();
  auto metadata_domain = static_cast<const T*>(domain_);

  // Check if there is any overlap
  if (!utils::overlap(subarray, metadata_domain, dim_num))
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
  } while (utils::coords_in_rect(tile_coords, subarray_tile_domain, dim_num));

  // Clean up
  delete[] subarray_tile_domain;
  delete[] tile_coords;

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

  // Get tile domain
  auto tile_domain = new T[2 * dim_num];
  T tile_num;  // Per dimension
  for (unsigned int i = 0; i < dim_num; ++i) {
    if (&typeid(T) != &typeid(float) && &typeid(T) != &typeid(double))
      tile_num =
          ceil(double(domain[2 * i + 1] - domain[2 * i] + 1) / tile_extents[i]);
    else
      tile_num =
          ceil(double(domain[2 * i + 1] - domain[2 * i]) / tile_extents[i]);
    tile_domain[2 * i] = 0;
    tile_domain[2 * i + 1] = tile_num - 1;
  }

  // Calculate subarray in tile domain
  for (unsigned int i = 0; i < dim_num; ++i) {
    auto overlap = MAX(subarray[2 * i], domain[2 * i]);
    subarray_tile_domain[2 * i] = (overlap - domain[2 * i]) / tile_extents[i];

    overlap = MIN(subarray[2 * i + 1], domain[2 * i + 1]);
    subarray_tile_domain[2 * i + 1] =
        (overlap - domain[2 * i]) / tile_extents[i];
  }

  delete[] tile_domain;
}

template <class T>
Status FragmentMetadata::expand_non_empty_domain(const T* mbr) {
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
  utils::expand_mbr(non_empty_domain, coords, dim_num);
  for (unsigned i = 0; i < dim_num; ++i)
    coords[i] = mbr[2 * i + 1];
  utils::expand_mbr(non_empty_domain, coords, dim_num);
  delete[] coords;

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
  file_var_sizes_.resize(attribute_num + 1);
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
        "Cannot load fragment metadata; Reading last tile cell number failed"));
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
  return Status::Ok();
}

// ===== FORMAT =====
// non_empty_domain_size (uint64_t)
// non_empty_domain (void*)
Status FragmentMetadata::load_non_empty_domain(ConstBuffer* buff) {
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
// tile_offsets_attr#0_num (uint64_t)
// tile_offsets_attr#0_#1 (uint64_t) tile_offsets_attr#0_#2 (uint64_t) ...
// ...
// tile_offsets_attr#<attribute_num>_num (uint64_t)
// tile_offsets_attr#<attribute_num>_#1 (uint64_t)
// tile_offsets_attr#<attribute_num>_#2 (uint64_t) ...
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
          "Cannot load fragment metadata; Reading variable tile sizes failed"));
    }
  }
  return Status::Ok();
}

// ===== FORMAT =====
// version (int[3])
Status FragmentMetadata::load_version(ConstBuffer* buff) {
  RETURN_NOT_OK(buff->read(version_, sizeof(version_)));
  return Status::Ok();
}

// ===== FORMAT =====
// bounding_coords_num(uint64_t)
// bounding_coords_#1(void*) bounding_coords_#2(void*) ...
Status FragmentMetadata::write_bounding_coords(Buffer* buff) {
  Status st;
  uint64_t bounding_coords_size = 2 * array_schema_->coords_size();
  auto bounding_coords_num = (uint64_t)bounding_coords_.size();
  // Write number of bounding coordinates
  st = buff->write(&bounding_coords_num, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing number of bounding "
        "coordinates failed"));
  }

  // Write bounding coordinates
  for (uint64_t i = 0; i < bounding_coords_num; ++i) {
    st = buff->write(bounding_coords_[i], bounding_coords_size);
    if (!st.ok()) {
      return LOG_STATUS(
          Status::FragmentMetadataError("Cannot serialize fragment metadata; "
                                        "Writing bounding coordinates failed"));
    }
  }
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
  uint64_t domain_size =
      (non_empty_domain_ == nullptr) ? 0 : array_schema_->coords_size() * 2;

  // Write non-empty domain size
  Status st = buff->write(&domain_size, sizeof(uint64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot serialize fragment metadata; Writing domain size failed"));
  }

  // Write non-empty domain
  if (non_empty_domain_ != nullptr) {
    st = buff->write(non_empty_domain_, domain_size);
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing domain failed"));
    }
  }

  return Status::Ok();
}

// ===== FORMAT =====
// tile_offsets_attr#0_num(uint64_t)
// tile_offsets_attr#0_#1 (uint64_t) tile_offsets_attr#0_#2 (uint64_t) ...
// ...
// tile_offsets_attr#<attribute_num>_num(uint64_t)
// tile_offsets_attr#<attribute_num>_#1 (uint64_t)
// tile_offsets_attr#<attribute_num>_#2 (uint64_t) ...
Status FragmentMetadata::write_tile_offsets(Buffer* buff) {
  Status st;
  unsigned int attribute_num = array_schema_->attribute_num();

  // Write tile offsets for each attribute
  for (unsigned int i = 0; i < attribute_num + 1; ++i) {
    // Write number of tile offsets
    uint64_t tile_offsets_num = tile_offsets_[i].size();
    st = buff->write(&tile_offsets_num, sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing number of tile offsets "
          "failed"));
    }

    if (tile_offsets_num == 0)
      continue;

    // Write tile offsets
    st = buff->write(&tile_offsets_[i][0], tile_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing tile offsets failed"));
    }
  }

  return Status::Ok();
}

// ===== FORMAT =====
// tile_var_offsets_attr#0_num(uint64_t)
// tile_var_offsets_attr#0_#1 (uint64_t) tile_var_offsets_attr#0_#2 (uint64_t)
// ...
// ...
// tile_var_offsets_attr#<attribute_num-1>_num(uint64_t)
// tile_var_offsets_attr#<attribute_num-1>_#1 (uint64_t)
//     tile_var_offsets_attr#<attribute_num-1>_#2 (uint64_t) ...
Status FragmentMetadata::write_tile_var_offsets(Buffer* buff) {
  Status st;
  unsigned int attribute_num = array_schema_->attribute_num();

  // Write tile offsets for each attribute
  for (unsigned int i = 0; i < attribute_num; ++i) {
    // Write number of offsets
    uint64_t tile_var_offsets_num = tile_var_offsets_[i].size();
    st = buff->write(&tile_var_offsets_num, sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing number of "
          "variable tile offsets failed"));
    }

    if (tile_var_offsets_num == 0)
      continue;

    // Write tile offsets
    st = buff->write(
        &tile_var_offsets_[i][0], tile_var_offsets_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing "
          "variable tile offsets failed"));
    }
  }

  return Status::Ok();
}

// ===== FORMAT =====
// tile_var_sizes_attr#0_num(uint64_t)
// tile_var_sizes_attr#0_#1 (uint64_t) tile_sizes_attr#0_#2 (uint64_t) ...
// ...
// tile_var_sizes_attr#<attribute_num-1>_num(uint64_t)
// tile_var_sizes_attr#<attribute_num-1>_#1(uint64_t)
//     tile_var_sizes_attr#<attribute_num-1>_#2 (uint64_t) ...
Status FragmentMetadata::write_tile_var_sizes(Buffer* buff) {
  Status st;
  unsigned int attribute_num = array_schema_->attribute_num();

  // Write tile sizes for each attribute
  for (unsigned int i = 0; i < attribute_num; ++i) {
    // Write number of sizes
    uint64_t tile_var_sizes_num = tile_var_sizes_[i].size();
    st = buff->write(&tile_var_sizes_num, sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot serialize fragment metadata; Writing number of "
          "variable tile sizes failed"));
    }

    if (tile_var_sizes_num == 0)
      continue;

    // Write tile sizes
    st = buff->write(
        &tile_var_sizes_[i][0], tile_var_sizes_num * sizeof(uint64_t));
    if (!st.ok()) {
      return LOG_STATUS(
          Status::FragmentMetadataError("Cannot serialize fragment metadata; "
                                        "Writing variable tile sizes failed"));
    }
  }
  return Status::Ok();
}

// ===== FORMAT =====
// version (int[3])
Status FragmentMetadata::write_version(Buffer* buff) {
  RETURN_NOT_OK(buff->write(constants::version, sizeof(constants::version)));
  return Status::Ok();
}

// Explicit template instantiations
template Status FragmentMetadata::append_mbr<int8_t>(const void* mbr);
template Status FragmentMetadata::append_mbr<uint8_t>(const void* mbr);
template Status FragmentMetadata::append_mbr<int16_t>(const void* mbr);
template Status FragmentMetadata::append_mbr<uint16_t>(const void* mbr);
template Status FragmentMetadata::append_mbr<int32_t>(const void* mbr);
template Status FragmentMetadata::append_mbr<uint32_t>(const void* mbr);
template Status FragmentMetadata::append_mbr<int64_t>(const void* mbr);
template Status FragmentMetadata::append_mbr<uint64_t>(const void* mbr);
template Status FragmentMetadata::append_mbr<float>(const void* mbr);
template Status FragmentMetadata::append_mbr<double>(const void* mbr);

template Status FragmentMetadata::compute_max_read_buffer_sizes<int8_t>(
    const int8_t* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const;
template Status FragmentMetadata::compute_max_read_buffer_sizes<uint8_t>(
    const uint8_t* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const;
template Status FragmentMetadata::compute_max_read_buffer_sizes<int16_t>(
    const int16_t* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const;
template Status FragmentMetadata::compute_max_read_buffer_sizes<uint16_t>(
    const uint16_t* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const;
template Status FragmentMetadata::compute_max_read_buffer_sizes<int>(
    const int* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const;
template Status FragmentMetadata::compute_max_read_buffer_sizes<unsigned>(
    const unsigned* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const;
template Status FragmentMetadata::compute_max_read_buffer_sizes<int64_t>(
    const int64_t* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const;
template Status FragmentMetadata::compute_max_read_buffer_sizes<uint64_t>(
    const uint64_t* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const;
template Status FragmentMetadata::compute_max_read_buffer_sizes<float>(
    const float* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const;
template Status FragmentMetadata::compute_max_read_buffer_sizes<double>(
    const double* subarray,
    const char** attributes,
    unsigned attribute_num,
    unsigned buffer_num,
    uint64_t* buffer_sizes) const;

}  // namespace sm
}  // namespace tiledb
