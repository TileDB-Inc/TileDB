/**
 * @file   book_keeping.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "fragment_metadata.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include "config.h"
#include "filesystem.h"
#include "logger.h"
#include "utils.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

FragmentMetadata::FragmentMetadata(
    const ArraySchema* array_schema, bool dense, const uri::URI& fragment_uri)
    : array_schema_(array_schema)
    , dense_(dense)
    , fragment_uri_(fragment_uri) {
  domain_ = nullptr;
  non_empty_domain_ = nullptr;
}

FragmentMetadata::~FragmentMetadata() {
  if (domain_ != nullptr)
    free(domain_);

  if (non_empty_domain_ != nullptr)
    free(non_empty_domain_);

  int64_t mbr_num = mbrs_.size();
  for (int64_t i = 0; i < mbr_num; ++i)
    if (mbrs_[i] != nullptr)
      free(mbrs_[i]);

  int64_t bounding_coords_num = bounding_coords_.size();
  for (int64_t i = 0; i < bounding_coords_num; ++i)
    if (bounding_coords_[i] != nullptr)
      free(bounding_coords_[i]);
}

/* ****************************** */
/*             ACCESSORS          */
/* ****************************** */

const std::vector<void*>& FragmentMetadata::bounding_coords() const {
  return bounding_coords_;
}

int64_t FragmentMetadata::cell_num(int64_t tile_pos) const {
  if (dense_) {
    return array_schema_->cell_num_per_tile();
  } else {
    int64_t tile_num = this->tile_num();
    if (tile_pos != tile_num - 1)
      return array_schema_->capacity();
    else
      return last_tile_cell_num();
  }
}

bool FragmentMetadata::dense() const {
  return dense_;
}

const void* FragmentMetadata::domain() const {
  return domain_;
}

int64_t FragmentMetadata::last_tile_cell_num() const {
  return last_tile_cell_num_;
}

const std::vector<void*>& FragmentMetadata::mbrs() const {
  return mbrs_;
}

const void* FragmentMetadata::non_empty_domain() const {
  return non_empty_domain_;
}

int64_t FragmentMetadata::tile_num() const {
  if (dense_) {
    return array_schema_->tile_num(domain_);
  } else {
    return mbrs_.size();
  }
}

const std::vector<std::vector<off_t>>& FragmentMetadata::tile_offsets() const {
  return tile_offsets_;
}

const std::vector<std::vector<off_t>>& FragmentMetadata::tile_var_offsets()
    const {
  return tile_var_offsets_;
}

const std::vector<std::vector<size_t>>& FragmentMetadata::tile_var_sizes()
    const {
  return tile_var_sizes_;
}

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

void FragmentMetadata::append_bounding_coords(const void* bounding_coords) {
  // For easy reference
  size_t bounding_coords_size = 2 * array_schema_->coords_size();

  // Copy and append MBR
  void* new_bounding_coords = malloc(bounding_coords_size);
  std::memcpy(new_bounding_coords, bounding_coords, bounding_coords_size);
  bounding_coords_.push_back(new_bounding_coords);
}

void FragmentMetadata::append_mbr(const void* mbr) {
  // For easy reference
  size_t mbr_size = 2 * array_schema_->coords_size();

  // Copy and append MBR
  void* new_mbr = malloc(mbr_size);
  std::memcpy(new_mbr, mbr, mbr_size);
  mbrs_.push_back(new_mbr);
}

void FragmentMetadata::append_tile_offset(int attribute_id, size_t step) {
  tile_offsets_[attribute_id].push_back(next_tile_offsets_[attribute_id]);
  size_t new_offset = tile_offsets_[attribute_id].back() + step;
  next_tile_offsets_[attribute_id] = new_offset;
}

void FragmentMetadata::append_tile_var_offset(int attribute_id, size_t step) {
  tile_var_offsets_[attribute_id].push_back(
      next_tile_var_offsets_[attribute_id]);
  size_t new_offset = tile_var_offsets_[attribute_id].back() + step;
  next_tile_var_offsets_[attribute_id] = new_offset;
}

void FragmentMetadata::append_tile_var_size(int attribute_id, size_t size) {
  tile_var_sizes_[attribute_id].push_back(size);
}

/* FORMAT:
 * non_empty_domain_size(size_t) non_empty_domain(void*)
 * mbr_num(int64_t)
 * mbr_#1(void*) mbr_#2(void*) ...
 * bounding_coords_num(int64_t)
 * bounding_coords_#1(void*) bounding_coords_#2(void*) ...
 * tile_offsets_attr#0_num(int64_t)
 * tile_offsets_attr#0_#1 (off_t) tile_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_offsets_attr#<attribute_num>_num(int64_t)
 * tile_offsets_attr#<attribute_num>_#1(off_t)
 *     tile_offsets_attr#<attribute_num>_#2 (off_t) ...
 * tile_var_offsets_attr#0_num(int64_t)
 * tile_var_offsets_attr#0_#1 (off_t) tile_var_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_var_offsets_attr#<attribute_num-1>_num(int64_t)
 * tile_var_offsets_attr#<attribute_num-1>_#1 (off_t)
 *     tile_var_offsets_attr#<attribute_num-1>_#2 (off_t) ...
 * tile_var_sizes_attr#0_num(int64_t)
 * tile_var_sizes_attr#0_#1(size_t) tile_sizes_attr#0_#2 (size_t) ...
 * ...
 * tile_var_sizes_attr#<attribute_num-1>_num(int64_t)
 * tile_var_sizes__attr#<attribute_num-1>_#1(size_t)
 *     tile_var_sizes_attr#<attribute_num-1>_#2 (size_t) ...
 * last_tile_cell_num(int64_t)
 */
Status FragmentMetadata::flush() {
  // Do nothing if the fragment directory does not exist (fragment empty)
  if (!utils::fragment_exists(fragment_uri_))
    return Status::Ok();

  // Prepare file name
  std::string filename = fragment_uri_.to_posix_path();
  filename = filename + "/" + constants::fragment_metadata_filename +
             constants::file_suffix;

  Buffer* buff = new Buffer();

  // Write non-empty domain
  RETURN_NOT_OK_ELSE(write_non_empty_domain(buff), delete buff);

  // Write MBRs
  RETURN_NOT_OK_ELSE(write_mbrs(buff), delete buff);

  // Write bounding coordinates
  RETURN_NOT_OK_ELSE(write_bounding_coords(buff), delete buff);

  // Write tile offsets
  RETURN_NOT_OK_ELSE(write_tile_offsets(buff), delete buff);

  // Write variable tile offsets
  RETURN_NOT_OK_ELSE(write_tile_var_offsets(buff), delete buff);

  // Write variable tile sizes
  RETURN_NOT_OK_ELSE(write_tile_var_sizes(buff), delete buff);

  // Write cell number of the last tile
  RETURN_NOT_OK_ELSE(write_last_tile_cell_num(buff), delete buff);

  // Save metadata to disk
  Status st = vfs::write_to_file(filename, buff->data(), buff->size());
  delete buff;
  if (!st.ok()) {
    return LOG_STATUS(
        Status::FragmentMetadataError("Cannot write fragment metadata file"));
  }
  return Status::Ok();
}

Status FragmentMetadata::init(const void* non_empty_domain) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  // Sanity check
  assert(non_empty_domain_ == NULL);
  assert(domain_ == NULL);

  // Set non-empty domain
  size_t domain_size = 2 * array_schema_->coords_size();
  non_empty_domain_ = malloc(domain_size);
  if (non_empty_domain == nullptr)
    std::memcpy(non_empty_domain_, array_schema_->domain(), domain_size);
  else
    std::memcpy(non_empty_domain_, non_empty_domain, domain_size);

  // Set expanded domain
  domain_ = malloc(domain_size);
  std::memcpy(domain_, non_empty_domain_, domain_size);
  array_schema_->expand_domain(domain_);

  // Set last tile cell number
  last_tile_cell_num_ = 0;

  // Initialize tile offsets
  tile_offsets_.resize(attribute_num + 1);
  next_tile_offsets_.resize(attribute_num + 1);
  for (int i = 0; i < attribute_num + 1; ++i)
    next_tile_offsets_[i] = 0;

  // Initialize variable tile offsets
  tile_var_offsets_.resize(attribute_num);
  next_tile_var_offsets_.resize(attribute_num);
  for (int i = 0; i < attribute_num; ++i)
    next_tile_var_offsets_[i] = 0;

  // Initialize variable tile sizes
  tile_var_sizes_.resize(attribute_num);

  return Status::Ok();
}

/* FORMAT:
 * non_empty_domain_size(size_t) non_empty_domain(void*)
 * mbr_num(int64_t)
 * mbr_#1(void*) mbr_#2(void*) ...
 * bounding_coords_num(int64_t)
 * bounding_coords_#1(void*) bounding_coords_#2(void*) ...
 * tile_offsets_attr#0_num(int64_t)
 * tile_offsets_attr#0_#1 (off_t) tile_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_offsets_attr#<attribute_num>_num(int64_t)
 * tile_offsets_attr#<attribute_num>_#1(off_t)
 *     tile_offsets_attr#<attribute_num>_#2 (off_t) ...
 * tile_var_offsets_attr#0_num(int64_t)
 * tile_var_offsets_attr#0_#1 (off_t) tile_var_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_var_offsets_attr#<attribute_num-1>_num(int64_t)
 * tile_var_offsets_attr#<attribute_num-1>_#1 (off_t)
 *     tile_var_offsets_attr#<attribute_num-1>_#2 (off_t) ...
 * tile_var_sizes_attr#0_num(int64_t)
 * tile_var_sizes_attr#0_#1(size_t) tile_sizes_attr#0_#2 (size_t) ...
 * ...
 * tile_var_sizes_attr#<attribute_num-1>_num(int64_t)
 * tile_var_sizes__attr#<attribute_num-1>_#1(size_t)
 *     tile_var_sizes_attr#<attribute_num-1>_#2 (size_t) ...
 * last_tile_cell_num(int64_t)
 */
Status FragmentMetadata::load() {
  // Prepare file name
  std::string filename = fragment_uri_.to_posix_path();
  filename = filename + "/" + constants::fragment_metadata_filename +
             constants::file_suffix;
  Buffer* buff = nullptr;
  Status st = vfs::read_from_file(filename, &buff);
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Cannot open file"));
  }

  // Load non-empty domain
  RETURN_NOT_OK_ELSE(load_non_empty_domain(buff), delete buff);

  // Load MBRs
  RETURN_NOT_OK_ELSE(load_mbrs(buff), delete buff);

  // Load bounding coordinates
  RETURN_NOT_OK_ELSE(load_bounding_coords(buff), delete buff);

  // Load tile offsets
  RETURN_NOT_OK_ELSE(load_tile_offsets(buff), delete buff);

  // Load variable tile offsets
  RETURN_NOT_OK_ELSE(load_tile_var_offsets(buff), delete buff);

  // Load variable tile sizes
  RETURN_NOT_OK_ELSE(load_tile_var_sizes(buff), delete buff);

  // Load cell number of last tile
  RETURN_NOT_OK_ELSE(load_last_tile_cell_num(buff), delete buff);

  delete buff;

  return Status::Ok();
}

void FragmentMetadata::set_last_tile_cell_num(int64_t cell_num) {
  last_tile_cell_num_ = cell_num;
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

/* FORMAT:
 * bounding_coords_num(int64_t)
 * bounding_coords_#1(void*) bounding_coords_#2(void*) ...
 */
Status FragmentMetadata::write_bounding_coords(Buffer* buff) {
  Status st;
  size_t bounding_coords_size = 2 * array_schema_->coords_size();
  int64_t bounding_coords_num = bounding_coords_.size();
  // Write number of bounding coordinates
  st = buff->write(&bounding_coords_num, sizeof(int64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot finalize fragment metadata; Writing number of bounding "
        "coordinates failed"));
  }
  // Write bounding coordinates
  for (int64_t i = 0; i < bounding_coords_num; ++i) {
    st = buff->write(bounding_coords_[i], bounding_coords_size);
    if (!st.ok()) {
      return LOG_STATUS(
          Status::FragmentMetadataError("Cannot finalize fragment metadata; "
                                        "Writing bounding coordinates failed"));
    }
  }
  return Status::Ok();
}

/* FORMAT:
 * last_tile_cell_num(int64_t)
 */
Status FragmentMetadata::write_last_tile_cell_num(Buffer* buff) {
  int64_t cell_num_per_tile =
      dense_ ? array_schema_->cell_num_per_tile() : array_schema_->capacity();
  // Handle the case of zero
  int64_t last_tile_cell_num =
      (last_tile_cell_num_ == 0) ? cell_num_per_tile : last_tile_cell_num_;
  Status st = buff->write(&last_tile_cell_num, sizeof(int64_t));
  if (!st.ok()) {
    return LOG_STATUS(
        Status::FragmentMetadataError("Cannot finalize fragment metadata; "
                                      "Writing last tile cell number failed"));
  }
  return Status::Ok();
}

/* FORMAT:
 * mbr_num(int64_t)
 * mbr_#1(void*) mbr_#2(void*) ...
 */
Status FragmentMetadata::write_mbrs(Buffer* buff) {
  Status st;
  size_t mbr_size = 2 * array_schema_->coords_size();
  int64_t mbr_num = mbrs_.size();
  // Write number of MBRs
  st = buff->write(&mbr_num, sizeof(int64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot finalize fragment metadata; Writing number of MBRs failed"));
  }
  // Write MBRs
  for (int64_t i = 0; i < mbr_num; ++i) {
    st = buff->write(mbrs_[i], mbr_size);
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot finalize fragment metadata; Writing MBR failed"));
    }
  }
  return Status::Ok();
}

/* FORMAT:
 * non_empty_domain_size(size_t) non_empty_domain(void*)
 */
Status FragmentMetadata::write_non_empty_domain(Buffer* buff) {
  size_t domain_size =
      (non_empty_domain_ == nullptr) ? 0 : array_schema_->coords_size() * 2;
  // Write non-empty domain size
  Status st = buff->write(&domain_size, sizeof(size_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot finalize fragment metadata; Writing domain size failed"));
  }
  // Write non-empty domain
  if (non_empty_domain_ != nullptr) {
    st = buff->write(non_empty_domain_, domain_size);
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot finalize fragment metadata; Writing domain failed"));
    }
  }
  return Status::Ok();
}

/* FORMAT:
 * tile_offsets_attr#0_num(int64_t)
 * tile_offsets_attr#0_#1 (off_t) tile_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_offsets_attr#<attribute_num>_num(int64_t)
 * tile_offsets_attr#<attribute_num>_#1 (off_t)
 * tile_offsets_attr#<attribute_num>_#2 (off_t) ...
 */
Status FragmentMetadata::write_tile_offsets(Buffer* buff) {
  Status st;
  int attribute_num = array_schema_->attribute_num();
  // Write tile offsets for each attribute
  for (int i = 0; i < attribute_num + 1; ++i) {
    // Write number of tile offsets
    int64_t tile_offsets_num = tile_offsets_[i].size();
    st = buff->write(&tile_offsets_num, sizeof(int64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot finalize fragment metadata; Writing number of tile offsets "
          "failed"));
    }
    if (tile_offsets_num == 0)
      continue;
    // Write tile offsets
    st = buff->write(&tile_offsets_[i][0], tile_offsets_num * sizeof(off_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot finalize fragment metadata; Writing tile offsets failed"));
    }
  }
  return Status::Ok();
}

/* FORMAT:
 * tile_var_offsets_attr#0_num(int64_t)
 * tile_var_offsets_attr#0_#1 (off_t) tile_var_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_var_offsets_attr#<attribute_num-1>_num(int64_t)
 * tile_var_offsets_attr#<attribute_num-1>_#1 (off_t)
 *     tile_var_offsets_attr#<attribute_num-1>_#2 (off_t) ...
 */
Status FragmentMetadata::write_tile_var_offsets(Buffer* buff) {
  Status st;
  int attribute_num = array_schema_->attribute_num();

  // Write tile offsets for each attribute
  for (int i = 0; i < attribute_num; ++i) {
    // Write number of offsets
    int64_t tile_var_offsets_num = tile_var_offsets_[i].size();
    st = buff->write(&tile_var_offsets_num, sizeof(int64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot finalize fragment metadata; Writing number of "
          "variable tile offsets failed"));
    }
    if (tile_var_offsets_num == 0)
      continue;
    // Write tile offsets
    st = buff->write(
        &tile_var_offsets_[i][0], tile_var_offsets_num * sizeof(off_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot finalize fragment metadata; Writing "
          "variable tile offsets failed"));
    }
  }
  return Status::Ok();
}

/* FORMAT:
 * tile_var_sizes_attr#0_num(int64_t)
 * tile_var_sizes_attr#0_#1 (size_t) tile_sizes_attr#0_#2 (size_t) ...
 * ...
 * tile_var_sizes_attr#<attribute_num-1>_num(int64_t)
 * tile_var_sizes__attr#<attribute_num-1>_#1(size_t)
 *     tile_var_sizes_attr#<attribute_num-1>_#2 (size_t) ...
 */
Status FragmentMetadata::write_tile_var_sizes(Buffer* buff) {
  Status st;
  int attribute_num = array_schema_->attribute_num();

  // Write tile sizes for each attribute
  for (int i = 0; i < attribute_num; ++i) {
    // Write number of sizes
    int64_t tile_var_sizes_num = tile_var_sizes_[i].size();
    st = buff->write(&tile_var_sizes_num, sizeof(int64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot finalize fragment metadata; Writing number of "
          "variable tile sizes failed"));
    }
    if (tile_var_sizes_num == 0)
      continue;
    // Write tile sizes
    st = buff->write(
        &tile_var_sizes_[i][0], tile_var_sizes_num * sizeof(size_t));
    if (!st.ok()) {
      return LOG_STATUS(
          Status::FragmentMetadataError("Cannot finalize fragment metadata; "
                                        "Writing variable tile sizes failed"));
    }
  }
  return Status::Ok();
}

/* FORMAT:
 * bounding_coords_num (int64_t)
 * bounding_coords_#1 (void*) bounding_coords_#2 (void*) ...
 */
Status FragmentMetadata::load_bounding_coords(Buffer* buff) {
  size_t bounding_coords_size = 2 * array_schema_->coords_size();

  // Get number of bounding coordinates
  int64_t bounding_coords_num = 0;
  Status st = buff->read(&bounding_coords_num, sizeof(int64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading number of "
        "bounding coordinates failed"));
  }
  // Get bounding coordinates
  void* bounding_coords;
  bounding_coords_.resize(bounding_coords_num);
  for (int64_t i = 0; i < bounding_coords_num; ++i) {
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

/* FORMAT:
 * last_tile_cell_num (int64_t)
 */
Status FragmentMetadata::load_last_tile_cell_num(Buffer* buff) {
  // Get last tile cell number
  Status st = buff->read(&last_tile_cell_num_, sizeof(int64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading last tile cell number failed"));
  }
  return Status::Ok();
}

/* FORMAT:
 * mbr_num (int64_t)
 * mbr_#1 (void*) mbr_#2 (void*) ... mbr_#<mbr_num> (void*)
 */
Status FragmentMetadata::load_mbrs(Buffer* buff) {
  size_t mbr_size = 2 * array_schema_->coords_size();
  // Get number of MBRs
  int64_t mbr_num = 0;
  Status st = buff->read(&mbr_num, sizeof(int64_t));
  if (!st.ok()) {
    return LOG_STATUS(Status::FragmentMetadataError(
        "Cannot load fragment metadata; Reading number of MBRs failed"));
  }
  // Get MBRs
  void* mbr = nullptr;
  mbrs_.resize(mbr_num);
  for (int64_t i = 0; i < mbr_num; ++i) {
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

/* FORMAT:
 * non_empty_domain_size (size_t) non_empty_domain (void*)
 */
Status FragmentMetadata::load_non_empty_domain(Buffer* buff) {
  // Get domain size
  size_t domain_size = 0;
  Status st = buff->read(&domain_size, sizeof(size_t));
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
    array_schema_->expand_domain(domain_);
  }
  return Status::Ok();
}

/* FORMAT:
 * tile_offsets_attr#0_num (int64_t)
 * tile_offsets_attr#0_#1 (off_t) tile_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_offsets_attr#<attribute_num>_num (int64_t)
 * tile_offsets_attr#<attribute_num>_#1 (off_t)
 * tile_offsets_attr#<attribute_num>_#2 (off_t) ...
 */
Status FragmentMetadata::load_tile_offsets(Buffer* buff) {
  Status st;
  int64_t tile_offsets_num = 0;
  int attribute_num = array_schema_->attribute_num();

  // Allocate tile offsets
  tile_offsets_.resize(attribute_num + 1);

  // For all attributes, get the tile offsets
  for (int i = 0; i < attribute_num + 1; ++i) {
    // Get number of tile offsets
    st = buff->read(&tile_offsets_num, sizeof(int64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading number of tile offsets "
          "failed"));
    }
    if (tile_offsets_num == 0)
      continue;
    // Get tile offsets
    tile_offsets_[i].resize(tile_offsets_num);
    st = buff->read(&tile_offsets_[i][0], tile_offsets_num * sizeof(off_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading tile offsets failed"));
    }
  }
  return Status::Ok();
}

/* FORMAT:
 * tile_var_offsets_attr#0_num (int64_t)
 * tile_var_offsets_attr#0_#1 (off_t) tile_var_offsets_attr#0_#2 (off_t) ...
 * ...
 * tile_var_offsets_attr#<attribute_num-1>_num(int64_t)
 * tile_var_offsets_attr#<attribute_num-1>_#1 (off_t)
 *     tile_ver_offsets_attr#<attribute_num-1>_#2 (off_t) ...
 */
Status FragmentMetadata::load_tile_var_offsets(Buffer* buff) {
  Status st;
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_var_offsets_num = 0;

  // Allocate tile offsets
  tile_var_offsets_.resize(attribute_num);

  // For all attributes, get the variable tile offsets
  for (int i = 0; i < attribute_num; ++i) {
    // Get number of tile offsets
    st = buff->read(&tile_var_offsets_num, sizeof(int64_t));
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
        &tile_var_offsets_[i][0], tile_var_offsets_num * sizeof(off_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading variable tile offsets "
          "failed"));
    }
  }
  return Status::Ok();
}

/* FORMAT:
 * tile_var_sizes_attr#0_num (int64_t)
 * tile_var_sizes_attr#0_#1 (size_t) tile_sizes_attr#0_#2 (size_t) ...
 * ...
 * tile_var_sizes_attr#<attribute_num-1>_num( int64_t)
 * tile_var_sizes__attr#<attribute_num-1>_#1 (size_t)
 *     tile_var_sizes_attr#<attribute_num-1>_#2 (size_t) ...
 */
Status FragmentMetadata::load_tile_var_sizes(Buffer* buff) {
  Status st;
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_var_sizes_num = 0;

  // Allocate tile sizes
  tile_var_sizes_.resize(attribute_num);

  // For all attributes, get the variable tile sizes
  for (int i = 0; i < attribute_num; ++i) {
    // Get number of tile sizes
    st = buff->read(&tile_var_sizes_num, sizeof(int64_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading number of variable tile "
          "sizes failed"));
    }
    if (tile_var_sizes_num == 0)
      continue;
    // Get variable tile sizes
    tile_var_sizes_[i].resize(tile_var_sizes_num);
    st =
        buff->read(&tile_var_sizes_[i][0], tile_var_sizes_num * sizeof(size_t));
    if (!st.ok()) {
      return LOG_STATUS(Status::FragmentMetadataError(
          "Cannot load fragment metadata; Reading variable tile sizes failed"));
    }
  }
  return Status::Ok();
}

}  // namespace tiledb
